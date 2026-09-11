# Tributary-rs

Rust implementation of a DuckDB 1.5.5 Kafka snapshot table function, initially targeting the core Tributary scan behavior plus Kafka headers.

## Target

- DuckDB 1.5.5
- Rust
- `duckdb-rs` 1.10505.x
- `rdkafka`

## Planned SQL API

```sql
LOAD 'tributary_rs.duckdb_extension';

SELECT *
FROM tributary_scan_topic('events', 'localhost:9092');
```

Output:

```text
topic       VARCHAR
partition   INTEGER
offset      BIGINT
key         BLOB
message     BLOB
headers     LIST<STRUCT(key VARCHAR, value BLOB)>
```

The scanner will preserve Tributary's snapshot semantics: capture partition watermarks when the query starts, then consume only records up to those captured high watermarks. Messages arriving after the snapshot boundary are not included.

## Build

GitHub Actions will build and test the extension against DuckDB 1.5.5. Local builds should use the official DuckDB Rust extension template build infrastructure.
