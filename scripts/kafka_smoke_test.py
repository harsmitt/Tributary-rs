import os
import time

from kafka.admin import KafkaAdminClient, NewTopic
from confluent_kafka import Producer
import duckdb

BROKERS = "127.0.0.1:9092"
TOPIC = "tributary_rs_smoke"
PRODUCE_TOPIC = "tributary_rs_produce_smoke"
METADATA_TOPIC = "tributary_rs_metadata_smoke"
EXTENSION = os.environ["DUCKDB_EXTENSION"]


def wait_for_kafka():
    for _ in range(60):
        try:
            admin = KafkaAdminClient(bootstrap_servers=BROKERS, request_timeout_ms=1000)
            admin.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Kafka did not become ready")


wait_for_kafka()

admin = KafkaAdminClient(bootstrap_servers=BROKERS)
try:
    admin.create_topics([
        NewTopic(TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(PRODUCE_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(METADATA_TOPIC, num_partitions=2, replication_factor=1),
    ])
except Exception as exc:
    if "TopicAlreadyExists" not in str(exc):
        raise
finally:
    admin.close()

producer = Producer({"bootstrap.servers": BROKERS})
for i in range(3):
    producer.produce(
        TOPIC,
        key=f"key-{i}".encode(),
        value=f"payload-{i}".encode(),
        headers=[("trace-id", f"trace-{i}".encode()), ("trace-id", None)],
    )
    producer.poll(0)
producer.flush(10)

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"LOAD '{EXTENSION}'")

produce_result = con.execute(
    'SELECT * FROM tributary_produce('
    f"'{PRODUCE_TOPIC}', 'producer-payload', \"bootstrap.servers\" := '{BROKERS}', key := 'producer-key')"
).fetchone()
assert produce_result[0] == PRODUCE_TOPIC, produce_result
assert produce_result[1] == 0, produce_result
assert produce_result[2] >= 0, produce_result

scan = f"tributary_scan_topic('{TOPIC}', \"bootstrap.servers\" := '{BROKERS}')"

rows = con.execute(
    f"""
    SELECT
        count(*) AS row_count,
        count(*) FILTER (WHERE len(headers) = 2) AS two_header_rows,
        count(*) FILTER (WHERE headers[1].key = 'trace-id' AND headers[2].key = 'trace-id') AS duplicate_key_rows,
        count(*) FILTER (WHERE headers[2].value IS NULL) AS null_value_rows
    FROM {scan}
    """
).fetchone()

assert rows == (3, 3, 3, 3), rows

payloads = con.execute(
    f'SELECT key, message FROM {scan} ORDER BY "offset"'
).fetchall()
assert payloads == [
    (b"key-0", b"payload-0"),
    (b"key-1", b"payload-1"),
    (b"key-2", b"payload-2"),
], payloads

produced = con.execute(
    f'SELECT key, message FROM {"tributary_scan_topic"}(\'{PRODUCE_TOPIC}\', "bootstrap.servers" := \'{BROKERS}\')'
).fetchall()
assert produced == [(b"producer-key", b"producer-payload")], produced

metadata = con.execute(
    'SELECT brokers, topics FROM tributary_metadata('
    f'"bootstrap.servers" := \'{BROKERS}\')'
).fetchone()

brokers, topics = metadata
assert len(brokers) == 1, brokers
assert brokers[0]["host"] == "127.0.0.1", brokers
assert brokers[0]["port"] == 9092, brokers

topic_map = {topic["name"]: topic for topic in topics}
assert TOPIC in topic_map, topic_map
assert PRODUCE_TOPIC in topic_map, topic_map
assert METADATA_TOPIC in topic_map, topic_map
assert topic_map[TOPIC]["error"] is None, topic_map[TOPIC]
assert topic_map[PRODUCE_TOPIC]["error"] is None, topic_map[PRODUCE_TOPIC]
assert topic_map[METADATA_TOPIC]["error"] is None, topic_map[METADATA_TOPIC]
assert [p["id"] for p in topic_map[METADATA_TOPIC]["partitions"]] == [0, 1], topic_map[METADATA_TOPIC]
assert all(p["leader"] >= 0 for p in topic_map[METADATA_TOPIC]["partitions"]), topic_map[METADATA_TOPIC]

print("SUCCESS: Tributary Rust extension consumed 3 Kafka records")
print("SUCCESS: duplicate header keys and NULL header values preserved")
print("SUCCESS: raw key/message bytes preserved")
print("SUCCESS: tributary_produce delivered a keyed Kafka message")
print("SUCCESS: Tributary-compatible bootstrap.servers named parameter works")
print("SUCCESS: tributary_metadata returned brokers, topics, and partitions")
