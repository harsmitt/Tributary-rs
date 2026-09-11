import os
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import duckdb

BROKERS = "127.0.0.1:9092"
TOPIC = "tributary_rs_smoke"
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
    admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
except Exception as exc:
    if "TopicAlreadyExists" not in str(exc):
        raise
finally:
    admin.close()

producer = KafkaProducer(bootstrap_servers=BROKERS)
for i in range(3):
    future = producer.send(
        TOPIC,
        key=f"key-{i}".encode(),
        value=f"payload-{i}".encode(),
        headers=[("trace-id", f"trace-{i}".encode()), ("trace-id", None)],
    )
    future.get(timeout=10)
producer.flush()
producer.close()

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"LOAD '{EXTENSION}'")

rows = con.execute(
    f"""
    SELECT
        count(*) AS row_count,
        count(*) FILTER (WHERE len(headers) = 2) AS two_header_rows,
        count(*) FILTER (WHERE headers[1].key = 'trace-id' AND headers[2].key = 'trace-id') AS duplicate_key_rows,
        count(*) FILTER (WHERE headers[2].value IS NULL) AS null_value_rows
    FROM tributary_scan_topic('{TOPIC}', '{BROKERS}')
    """
).fetchone()

assert rows == (3, 3, 3, 3), rows

payloads = con.execute(
    f"SELECT key, message FROM tributary_scan_topic('{TOPIC}', '{BROKERS}') ORDER BY offset"
).fetchall()
assert payloads == [
    (b"key-0", b"payload-0"),
    (b"key-1", b"payload-1"),
    (b"key-2", b"payload-2"),
], payloads

print("SUCCESS: Tributary Rust extension consumed 3 Kafka records")
print("SUCCESS: duplicate header keys and NULL header values preserved")
print("SUCCESS: raw key/message bytes preserved")
