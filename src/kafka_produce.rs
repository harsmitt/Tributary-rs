use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Result,
};
use rdkafka::{
    client::ClientContext,
    config::ClientConfig,
    message::{DeliveryResult, Header, Message, OwnedHeaders},
    producer::{BaseProducer, BaseRecord, Producer, ProducerContext},
};
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

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
pub struct ProduceBind {
    topic: String,
    message: String,
    key: Option<String>,
    headers: Option<HashMap<String, String>>,
    config: HashMap<String, String>,
}

pub struct ProduceInit {
    state: Mutex<Option<ProduceState>>,
}

struct ProduceState {
    result: Option<DeliveryState>,
    emitted: bool,
}

#[derive(Debug, Clone)]
enum DeliveryState {
    Delivered { partition: i32, offset: i64 },
    Failed(String),
}

#[derive(Clone, Default)]
struct DeliveryContext {
    result: Arc<Mutex<Option<DeliveryState>>>,
}

impl ClientContext for DeliveryContext {}

impl ProducerContext for DeliveryContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {
        let state = match result {
            Ok(message) => DeliveryState::Delivered {
                partition: message.partition(),
                offset: message.offset(),
            },
            Err((error, _)) => DeliveryState::Failed(error.to_string()),
        };
        if let Ok(mut guard) = self.result.lock() {
            *guard = Some(state);
        }
    }
}

pub struct KafkaProduce;

impl VTab for KafkaProduce {
    type BindData = ProduceBind;
    type InitData = ProduceInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("topic", LogicalTypeId::Varchar.into());
        bind.add_result_column("partition", LogicalTypeId::Integer.into());
        bind.add_result_column("offset", LogicalTypeId::Bigint.into());

        if bind.get_parameter_count() != 2 {
            return Err(
                "tributary_produce requires two positional arguments: topic and message; Kafka settings, key, and headers use named parameters".into(),
            );
        }

        let topic = bind.get_parameter(0).to_string();
        if topic.is_empty() {
            return Err("topic must not be empty".into());
        }
        let message = bind.get_parameter(1).to_string();
        let key = bind.get_named_parameter("key").map(|value| value.to_string());
        let headers = match bind.get_named_parameter("headers") {
            Some(value) if value.is_null() => None,
            Some(value) => {
                let text = value.to_string();
                let parsed: HashMap<String, String> = serde_json::from_str(&text)
                    .map_err(|error| format!("headers must be a JSON object with string values: {error}"))?;
                Some(parsed)
            }
            None => None,
        };

        let mut config = HashMap::new();
        for &name in TRIBUTARY_CONFIG_KEYS {
            if let Some(value) = bind.get_named_parameter(name) {
                config.insert(name.to_owned(), value.to_string());
            }
        }
        if !config.contains_key("bootstrap.servers") {
            return Err("tributary_produce requires named parameter \"bootstrap.servers\"".into());
        }
        if config.get("bootstrap.servers").map(String::is_empty).unwrap_or(true) {
            return Err("bootstrap.servers must not be empty".into());
        }

        Ok(ProduceBind { topic, message, key, headers, config })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(ProduceInit { state: Mutex::new(None) })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind = func.get_bind_data();
        let init = func.get_init_data();
        let mut guard = init.state.lock().map_err(|_| "Kafka producer state mutex was poisoned")?;

        if guard.is_none() {
            let delivery_result = produce(
                &bind.topic,
                &bind.message,
                bind.key.as_deref(),
                bind.headers.as_ref(),
                &bind.config,
            )?;
            *guard = Some(ProduceState { result: Some(delivery_result), emitted: false });
        }

        let state = guard.as_mut().expect("producer state initialized above");
        if state.emitted {
            output.set_len(0);
            return Ok(());
        }
        state.emitted = true;

        match state.result.take().expect("producer result initialized above") {
            DeliveryState::Delivered { partition, offset } => {
                let mut topic = output.flat_vector(0);
                topic.insert(0, bind.topic.as_str());
                unsafe {
                    output.flat_vector(1).as_mut_slice_with_len::<i32>(1)[0] = partition;
                    output.flat_vector(2).as_mut_slice_with_len::<i64>(1)[0] = offset;
                }
                output.set_len(1);
                Ok(())
            }
            DeliveryState::Failed(error) => Err(format!("Kafka message delivery failed: {error}").into()),
        }
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Varchar.into()])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let mut parameters = vec![
            ("key".to_owned(), LogicalTypeId::Varchar.into()),
            ("headers".to_owned(), LogicalTypeId::Varchar.into()),
        ];
        parameters.extend(
            TRIBUTARY_CONFIG_KEYS.iter().map(|key| ((*key).to_owned(), LogicalTypeId::Varchar.into())),
        );
        Some(parameters)
    }
}

fn produce(
    topic: &str,
    message: &str,
    key: Option<&str>,
    headers: Option<&HashMap<String, String>>,
    config: &HashMap<String, String>,
) -> Result<DeliveryState, Box<dyn Error>> {
    let mut client_config = ClientConfig::new();
    for (name, value) in config {
        if name.starts_with("schema.registry.") {
            continue;
        }
        client_config.set(name, value);
    }

    let delivery_context = DeliveryContext::default();
    let result_handle = Arc::clone(&delivery_context.result);
    let producer: BaseProducer<DeliveryContext> = client_config.create_with_context(delivery_context)?;

    let owned_headers = headers.map(|values| {
        values.iter().fold(OwnedHeaders::new(), |headers, (key, value)| {
            headers.insert(Header { key, value: Some(value.as_bytes()) })
        })
    });

    let record = match (key, owned_headers) {
        (Some(key), Some(headers)) => BaseRecord::to(topic).payload(message).key(key).headers(headers),
        (Some(key), None) => BaseRecord::to(topic).payload(message).key(key),
        (None, Some(headers)) => BaseRecord::to(topic).payload(message).headers(headers),
        (None, None) => BaseRecord::to(topic).payload(message),
    };

    producer.send(record).map_err(|(error, _)| format!("failed to enqueue Kafka message: {error}"))?;
    producer.flush(KAFKA_TIMEOUT);

    result_handle
        .lock()
        .map_err(|_| "Kafka delivery state mutex was poisoned")?
        .clone()
        .ok_or_else(|| "Kafka producer completed without a delivery report".into())
}
