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

#[derive(Debug)]
struct PartitionSnapshot {
    high: i64,
}

#[derive(Debug)]
pub struct KafkaScanBind {
    topic: String,
    bootstrap_servers: String,
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

        if bind.get_parameter_count() != 2 {
            return Err("tributary_scan_topic requires exactly 2 arguments: topic, bootstrap_servers".into());
        }

        let topic = bind.get_parameter(0).to_string();
        let bootstrap_servers = bind.get_parameter(1).to_string();
        if topic.is_empty() {
            return Err("topic must not be empty".into());
        }
        if bootstrap_servers.is_empty() {
            return Err("bootstrap_servers must not be empty".into());
        }

        Ok(KafkaScanBind {
            topic,
            bootstrap_servers,
        })
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
            *guard = Some(KafkaScanState::new(&bind.topic, &bind.bootstrap_servers)?);
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
        Some(vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Varchar.into()])
    }
}

impl KafkaScanState {
    fn new(topic: &str, bootstrap_servers: &str) -> Result<Self, Box<dyn Error>> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", "tributary-rs-scan")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .set("auto.offset.reset", "earliest")
            .create()?;

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
        let list = output.list_vector(5);
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
