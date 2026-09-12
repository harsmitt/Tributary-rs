use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Result,
};
use rdkafka::{config::ClientConfig, consumer::{BaseConsumer, Consumer}};
use std::{collections::HashMap, error::Error, sync::Mutex, time::Duration};

const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);
const TRIBUTARY_CONFIG_KEYS: &[&str] = &[
    "bootstrap.servers", "security.protocol", "sasl.mechanism", "sasl.username", "sasl.password",
    "ssl.ca.location", "ssl.certificate.location", "ssl.key.location", "ssl.key.password",
    "group.id", "client.id", "transactional.id", "schema.registry.url",
    "schema.registry.basic.auth.user.info", "debug", "sasl.oauthbearer.client.id",
    "sasl.oauthbearer.client.secret", "sasl.oauthbearer.method", "sasl.oauthbearer.token.endpoint.url",
    "auto.offset.reset", "enable.auto.commit", "enable.auto.offset.store", "enable.partition.eof",
    "fetch.min.bytes", "fetch.wait.max.ms", "fetch.max.bytes", "max.partition.fetch.bytes",
    "max.poll.interval.ms", "session.timeout.ms", "heartbeat.interval.ms", "socket.timeout.ms",
    "socket.connection.setup.timeout.ms", "socket.keepalive.enable", "connections.max.idle.ms",
    "receive.message.max.bytes", "queued.min.messages", "queued.max.messages.kbytes",
    "fetch.error.backoff.ms", "retry.backoff.ms", "retry.backoff.max.ms", "reconnect.backoff.ms",
    "reconnect.backoff.max.ms", "allow.auto.create.topics", "partition.assignment.strategy",
    "check.crcs", "isolation.level",
];

#[derive(Debug)]
pub struct MetadataBind { config: HashMap<String, String> }
pub struct MetadataInit { state: Mutex<Option<MetadataState>> }
struct MetadataState { brokers: Vec<BrokerRow>, topics: Vec<TopicRow>, emitted: bool }
#[derive(Debug)] struct BrokerRow { id: i32, host: String, port: i32 }
#[derive(Debug)] struct TopicRow { name: String, error: Option<String>, partitions: Vec<PartitionRow> }
#[derive(Debug)] struct PartitionRow { id: i32, leader: i32 }
pub struct KafkaMetadata;

impl VTab for KafkaMetadata {
    type BindData = MetadataBind;
    type InitData = MetadataInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let partition_type = LogicalTypeHandle::struct_type(&[
            ("id", LogicalTypeId::Integer.into()),
            ("leader", LogicalTypeId::Integer.into()),
        ]);
        let topic_type = LogicalTypeHandle::struct_type(&[
            ("name", LogicalTypeId::Varchar.into()),
            ("error", LogicalTypeId::Varchar.into()),
            ("partitions", LogicalTypeHandle::list(&partition_type)),
        ]);
        let broker_type = LogicalTypeHandle::struct_type(&[
            ("id", LogicalTypeId::Integer.into()),
            ("host", LogicalTypeId::Varchar.into()),
            ("port", LogicalTypeId::Integer.into()),
        ]);
        bind.add_result_column("brokers", LogicalTypeHandle::list(&broker_type));
        bind.add_result_column("topics", LogicalTypeHandle::list(&topic_type));

        if bind.get_parameter_count() != 0 {
            return Err("tributary_metadata does not accept positional arguments; Kafka settings use named parameters".into());
        }
        let mut config = HashMap::new();
        for &key in TRIBUTARY_CONFIG_KEYS {
            if let Some(value) = bind.get_named_parameter(key) { config.insert(key.to_owned(), value.to_string()); }
        }
        if !config.contains_key("bootstrap.servers") {
            return Err("tributary_metadata requires named parameter \"bootstrap.servers\"".into());
        }
        if config.get("bootstrap.servers").map(String::is_empty).unwrap_or(true) {
            return Err("bootstrap.servers must not be empty".into());
        }
        Ok(MetadataBind { config })
    }

    fn init(_init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(MetadataInit { state: Mutex::new(None) })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let bind = func.get_bind_data();
        let init = func.get_init_data();
        let mut guard = init.state.lock().map_err(|_| "Kafka metadata state mutex was poisoned")?;
        if guard.is_none() { *guard = Some(MetadataState::fetch(&bind.config)?); }
        let state = guard.as_mut().expect("metadata state initialized above");
        if state.emitted { output.set_len(0); return Ok(()); }
        write_metadata(output, &state.brokers, &state.topics)?;
        output.set_len(1);
        state.emitted = true;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> { Some(Vec::new()) }
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(TRIBUTARY_CONFIG_KEYS.iter().map(|key| ((*key).to_owned(), LogicalTypeId::Varchar.into())).collect())
    }
}

impl MetadataState {
    fn fetch(config: &HashMap<String, String>) -> Result<Self, Box<dyn Error>> {
        let mut client_config = ClientConfig::new();
        for (key, value) in config {
            if key.starts_with("schema.registry.") { continue; }
            client_config.set(key, value);
        }
        if !config.contains_key("group.id") {
            client_config.set("group.id", "tributary-rs-metadata");
        }
        let consumer: BaseConsumer = client_config.create()?;
        let metadata = consumer.fetch_metadata(None, KAFKA_TIMEOUT)?;
        let brokers = metadata.brokers().iter().map(|broker| BrokerRow {
            id: broker.id(), host: broker.host().to_owned(), port: broker.port(),
        }).collect();
        let topics = metadata.topics().iter().map(|topic| TopicRow {
            name: topic.name().to_owned(),
            error: topic.error().map(|error| format!("{:?}", error)),
            partitions: topic.partitions().iter().map(|partition| PartitionRow {
                id: partition.id(), leader: partition.leader(),
            }).collect(),
        }).collect();
        Ok(Self { brokers, topics, emitted: false })
    }
}

fn write_metadata(output: &mut DataChunkHandle, brokers: &[BrokerRow], topics: &[TopicRow]) -> Result<(), Box<dyn Error>> {
    {
        let mut list = output.list_vector(0);
        let child = list.struct_child(brokers.len());
        {
            let mut ids = child.child(0, brokers.len());
            let values = unsafe { ids.as_mut_slice_with_len::<i32>(brokers.len()) };
            for (slot, broker) in values.iter_mut().zip(brokers) { *slot = broker.id; }
        }
        {
            let hosts = child.child(1, brokers.len());
            for (i, broker) in brokers.iter().enumerate() { hosts.insert(i, broker.host.as_str()); }
        }
        {
            let mut ports = child.child(2, brokers.len());
            let values = unsafe { ports.as_mut_slice_with_len::<i32>(brokers.len()) };
            for (slot, broker) in values.iter_mut().zip(brokers) { *slot = broker.port; }
        }
        for (i, _) in brokers.iter().enumerate() { list.set_entry(i, i, 1); }
        list.set_len(brokers.len());
    }
    {
        let mut list = output.list_vector(1);
        let total_partitions: usize = topics.iter().map(|topic| topic.partitions.len()).sum();
        let child = list.struct_child(topics.len());
        let names = child.child(0, topics.len());
        let mut errors = child.child(1, topics.len());
        let mut partition_lists = child.list_vector_child(2);
        let partition_child = partition_lists.struct_child(total_partitions);
        {
            let mut partition_ids = partition_child.child(0, total_partitions);
            let values = unsafe { partition_ids.as_mut_slice_with_len::<i32>(total_partitions) };
            let mut index = 0usize;
            for topic in topics {
                for partition in &topic.partitions {
                    values[index] = partition.id;
                    index += 1;
                }
            }
        }
        {
            let mut partition_leaders = partition_child.child(1, total_partitions);
            let values = unsafe { partition_leaders.as_mut_slice_with_len::<i32>(total_partitions) };
            let mut index = 0usize;
            for topic in topics {
                for partition in &topic.partitions {
                    values[index] = partition.leader;
                    index += 1;
                }
            }
        }
        let mut partition_offset = 0usize;
        for (i, topic) in topics.iter().enumerate() {
            names.insert(i, topic.name.as_str());
            match &topic.error { Some(error) => errors.insert(i, error.as_str()), None => errors.set_null(i) }
            let length = topic.partitions.len();
            partition_lists.set_entry(i, partition_offset, length);
            partition_offset += length;
        }
        partition_lists.set_len(total_partitions);
        list.set_len(topics.len());
    }
    Ok(())
}