use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Result,
};
use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    message::{Headers, Message},
    topic_partition_list::{Offset, TopicPartitionList},
};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Mutex,
    time::Duration,
};

const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_TIMEOUT: Duration = Duration::from_millis(250);

// Tributary exposes these Kafka/librdkafka configuration keys as named
// parameters. Keep this list aligned with TributaryConfigKeys().
const TRIBUTARY_CONFIG_KEYS: &[&str] = &[
    "bootstrap.servers",
    "security.protocol",
    "sasl.mechanism",
    "sasl.username",
    "sasl.password",
    "ssl.ca.location",
    "ssl.certificate.location",
    "ssl.key.location",
    "ssl.key.password",
    "group.id",
    "client.id",
    "transactional.id",
    "schema.registry.url",
    "schema.registry.basic.auth.user.info",
    "debug",
    "sasl.oauthbearer.client.id",
    "sasl.oauthbearer.client.secret",
    "sasl.oauthbearer.method",
    "sasl.oauthbearer.token.endpoint.url",
    "auto.offset.reset",
    "enable.auto.commit",
    "enable.auto.offset.store",
    "enable.partition.eof",
    "fetch.min.bytes",
    "fetch.wait.max.ms",
    "fetch.max.bytes",
    "max.partition.fetch.bytes",
    "max.poll.interval.ms",
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "socket.timeout.ms",
    "socket.connection.setup.timeout.ms",
    "socket.keepalive.enable",
    "connections.max.idle.ms",
    "receive.message.max.bytes",
    "queued.min.messages",
    "queued.max.messages.kbytes",
    "fetch.error.backoff.ms",
    "retry.backoff.ms",
    "retry.backoff.max.ms",
    "reconnect.backoff.ms",
    "reconnect.backoff.max.ms",
    "allow.auto.create.topics",
    "partition.assignment.strategy",
    "check.crcs",
    "isolation.level",
];

#[derive(Debug)]
struct PartitionSnapshot {
    high: i64,
}

#[derive(Debug)]
pub struct KafkaScanBind {
    topic: String,
    config: HashMap<String, String>,
}

pub struct KafkaScanInit {
    state: Mutex<Option<KafkaScanState>>,
}

struct KafkaScanState {
    consumer: BaseConsumer,
    snapshots: HashMap<i32, PartitionSnapshot>,
    finished_partitions: HashSet<i32>,
    assigned: bool,
}

#[derive(Debug)]
struct Row {
    topic: String,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
    message: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
}

pub struct KafkaScan;

impl VTab for KafkaScan {
    type BindData = KafkaScanBind;
    type InitData = KafkaScanInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("topic", LogicalTypeId::Varchar.into());
        bind.add_result_column("partition", LogicalTypeId::Integer.into());
        bind.add_result_column("offset", LogicalTypeId::Bigint.into());
        bind.add_result_column("key", LogicalTypeId::Blob.into());
        bind.add_result_column("message", LogicalTypeId::Blob.into());

        let header_struct = LogicalTypeHandle::struct_type(&[
            ("key", LogicalTypeId::Varchar.into()),
            ("value", LogicalTypeId::Blob.into()),
        ]);
        bind.add_result_column("headers", LogicalTypeHandle::list(&header_struct));

        if bind.get_parameter_count() != 1 {
            return Err(
                "tributary_scan_topic requires one positional argument: topic; Kafka settings use named parameters"
                    .into(),
            );
        }

        let topic = bind.get_parameter(0).to_string();
        if topic.is_empty() {
            return Err("topic must not be empty".into());
        }

        let mut config = HashMap::new();
        for &key in TRIBUTARY_CONFIG_KEYS {
            if let Some(value) = bind.get_named_parameter(key) {
                config.insert(key.to_owned(), value.to_string());
            }
        }

        if !config.contains_key("bootstrap.servers") {
            return Err("tributary_scan_topic requires named parameter \"bootstrap.servers\"".into());
        }

        if config
            .get("bootstrap.servers")
            .map(String::is_empty)
            .unwrap_or(true)
        {
            return Err("bootstrap.servers must not be empty".into());
        }

        Ok(KafkaScanBind { topic, config })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        init.set_max_threads(1);
        Ok(KafkaScanInit {
            state: Mutex::new(None),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind = func.get_bind_data();
        let init = func.get_init_data();
        let mut guard = init
            .state
            .lock()
            .map_err(|_| "Kafka scanner state mutex was poisoned")?;

        if guard.is_none() {
            *guard = Some(KafkaScanState::new(&bind.topic, &bind.config)?);
        }

        let state = guard.as_mut().expect("state initialized above");
        let capacity = output.flat_vector(0).capacity();
        let rows = state.next_rows(&bind.topic, capacity)?;

        if rows.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        write_rows(output, &rows)?;
        output.set_len(rows.len());
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeId::Varchar.into()])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(
            TRIBUTARY_CONFIG_KEYS
                .iter()
                .map(|key| ((*key).to_owned(), LogicalTypeId::Varchar.into()))
                .collect(),
        )
    }
}

impl KafkaScanState {
    fn new(topic: &str, config: &HashMap<String, String>) -> Result<Self, Box<dyn Error>> {
        let mut client_config = ClientConfig::new();
        for (key, value) in config {
            // Tributary consumes these settings itself for Schema Registry;
            // they are not librdkafka properties and must not be passed to the
            // Kafka client configuration.
            if key.starts_with("schema.registry.") {
                continue;
            }
            client_config.set(key, value);
        }

        // This scanner uses explicit partition assignment rather than Kafka
        // consumer-group subscription. Do not invent a group.id: librdkafka
        // will otherwise initialize group coordination for a manually-assigned
        // consumer, which can produce LOCAL__UNKNOWN_GROUP on brokers such as
        // the Redpanda version used by CI. If a caller supplies group.id, keep
        // it for Tributary-compatible configuration semantics.
        client_config
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .set("auto.offset.reset", "earliest");

        let consumer: BaseConsumer = client_config.create()?;

        let metadata = consumer.fetch_metadata(Some(topic), KAFKA_TIMEOUT)?;
        let metadata_topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| format!("Kafka topic not found: {topic}"))?;

        let mut snapshots = HashMap::new();
        let mut assignment = TopicPartitionList::new();

        for partition in metadata_topic.partitions() {
            let partition_id = partition.id();
            let (low, high) = consumer.fetch_watermarks(topic, partition_id, KAFKA_TIMEOUT)?;
            if low < high {
                snapshots.insert(partition_id, PartitionSnapshot { high });
                assignment.add_partition_offset(topic, partition_id, Offset::Offset(low))?;
            }
        }

        let assigned = !snapshots.is_empty();
        if assigned {
            consumer.assign(&assignment)?;
        }

        Ok(Self {
            consumer,
            snapshots,
            finished_partitions: HashSet::new(),
            assigned,
        })
    }

    fn next_rows(&mut self, topic: &str, capacity: usize) -> Result<Vec<Row>, Box<dyn Error>> {
        if !self.assigned || self.finished_partitions.len() == self.snapshots.len() {
            return Ok(Vec::new());
        }

        let mut rows = Vec::with_capacity(capacity);

        while rows.len() < capacity && self.finished_partitions.len() < self.snapshots.len() {
            match self.consumer.poll(POLL_TIMEOUT) {
                None => continue,
                Some(Ok(message)) => {
                    let partition = message.partition();
                    let Some(snapshot) = self.snapshots.get(&partition) else {
                        continue;
                    };

                    if message.offset() >= snapshot.high {
                        self.finished_partitions.insert(partition);
                        continue;
                    }

                    let headers = message
                        .headers()
                        .map(|headers| {
                            headers
                                .iter()
                                .map(|header| {
                                    (header.key.to_owned(), header.value.map(|v| v.to_vec()))
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    rows.push(Row {
                        topic: topic.to_owned(),
                        partition,
                        offset: message.offset(),
                        key: message.key().map(ToOwned::to_owned),
                        message: message.payload().map(ToOwned::to_owned),
                        headers,
                    });
                }
                Some(Err(KafkaError::PartitionEOF(partition))) => {
                    self.finished_partitions.insert(partition);
                }
                Some(Err(error)) => return Err(format!("Kafka consume error: {error}").into()),
            }
        }

        Ok(rows)
    }
}

fn write_rows(output: &mut DataChunkHandle, rows: &[Row]) -> Result<(), Box<dyn Error>> {
    let count = rows.len();

    {
        let vector = output.flat_vector(0);
        for (i, row) in rows.iter().enumerate() {
            vector.insert(i, row.topic.as_str());
        }
    }

    {
        let mut vector = output.flat_vector(1);
        let values = unsafe { vector.as_mut_slice_with_len::<i32>(count) };
        for (slot, row) in values.iter_mut().zip(rows) {
            *slot = row.partition;
        }
    }

    {
        let mut vector = output.flat_vector(2);
        let values = unsafe { vector.as_mut_slice_with_len::<i64>(count) };
        for (slot, row) in values.iter_mut().zip(rows) {
            *slot = row.offset;
        }
    }

    {
        let mut vector = output.flat_vector(3);
        for (i, row) in rows.iter().enumerate() {
            match &row.key {
                Some(value) => vector.insert(i, value.as_slice()),
                None => vector.set_null(i),
            }
        }
    }

    {
        let mut vector = output.flat_vector(4);
        for (i, row) in rows.iter().enumerate() {
            match &row.message {
                Some(value) => vector.insert(i, value.as_slice()),
                None => vector.set_null(i),
            }
        }
    }

    {
        let mut list = output.list_vector(5);
        let total_headers: usize = rows.iter().map(|row| row.headers.len()).sum();
        let mut offset = 0usize;
        for (i, row) in rows.iter().enumerate() {
            list.set_entry(i, offset, row.headers.len());
            offset += row.headers.len();
        }

        let child = list.struct_child(total_headers);
        {
            let keys = child.child(0, total_headers);
            for (i, (key, _)) in rows.iter().flat_map(|r| r.headers.iter()).enumerate() {
                keys.insert(i, key.as_str());
            }
        }
        {
            let mut values = child.child(1, total_headers);
            let mut child_index = 0usize;
            for row in rows {
                for (_, value) in &row.headers {
                    match value {
                        Some(bytes) => values.insert(child_index, bytes.as_slice()),
                        None => values.set_null(child_index),
                    }
                    child_index += 1;
                }
            }
        }
        list.set_len(count);
    }

    Ok(())
}
