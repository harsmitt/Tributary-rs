# Tributary-rs development guide

This repository is a pure-Rust reimplementation of the core Kafka scan path from Query.Farm Tributary. The first milestone targets DuckDB 1.5.5 and keeps the implementation intentionally small: one table function, synchronous librdkafka polling, snapshot watermarks, and Kafka headers.

## Exact toolchain target

| Component | Version |
|---|---|
| DuckDB | v1.5.5 |
| duckdb-rs | ~1.10505.x |
| Rust | stable |
| rdkafka | 0.39.0 |
| librdkafka | built from the rdkafka crate with `cmake-build` |

The Rust DuckDB template uses the unstable DuckDB C API because duckdb-rs currently needs it for loadable extensions. The resulting extension is therefore tied to the targeted DuckDB release and must be rebuilt when the DuckDB target changes.

## Clone and reproduce

```bash
git clone --recurse-submodules https://github.com/harsmitt/Tributary-rs.git
cd Tributary-rs
```

Required local tools:

- Git
- Make
- Python 3 + `venv`
- Rust/Cargo
- CMake and a native C/C++ toolchain for librdkafka

On Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y git make cmake build-essential python3 python3-venv
```

## Build

```bash
make configure
make debug
```

For an optimized extension:

```bash
make clean_all
make configure
make release
```

The loadable artifact is produced at:

```text
build/release/extension/tributary_rs/tributary_rs.duckdb_extension
```

The official Rust extension template appends DuckDB extension metadata after Cargo builds the shared library; do not treat `cargo build` alone as the final `.duckdb_extension` artifact.

## Test

Run the DuckDB SQLLogicTests:

```bash
make test_debug
make test_release
```

The GitHub Actions workflow additionally starts a disposable Redpanda broker, produces Kafka records containing duplicate headers and a NULL header value, loads the built extension with the Python DuckDB client, and verifies the returned rows and bytes.

## Manual smoke test

Start a Kafka-compatible broker at `localhost:9092`, create a topic named `events`, and produce records. Then:

```bash
duckdb -unsigned
```

```sql
LOAD './build/release/extension/tributary_rs/tributary_rs.duckdb_extension';

SELECT *
FROM tributary_scan_topic('events', 'localhost:9092');
```

## Current semantics

`tributary_scan_topic(topic, bootstrap_servers)`:

1. Fetches topic metadata during scan initialization.
2. Reads the low/high watermark for every partition.
3. Assigns each non-empty partition at its captured low watermark.
4. Returns records while their offsets are below the captured high watermark.
5. Does not commit consumer offsets.
6. Uses one scanner thread initially (`set_max_threads(1)`).
7. Copies key and payload as raw bytes.
8. Preserves Kafka header order and duplicate header names.
9. Preserves NULL Kafka header values as SQL NULL BLOBs.

This deliberately mirrors the snapshot behavior before adding parallel partition workers or Schema Registry decoding.

## Architecture mapping from C++ Tributary

| C++ Tributary | Tributary-rs |
|---|---|
| `TributaryScanTopicBind` | `KafkaScanBind` |
| `TributaryScanTopicLocalState` / global scan state | `KafkaScanState` inside `KafkaScanInit` |
| `RdKafka::Consumer` | `rdkafka::consumer::BaseConsumer` |
| `RdKafka::Message` | `BorrowedMessage` |
| DuckDB DataChunk/vector writes | `DataChunkHandle` and DuckDB Rust vector APIs |

## Changing DuckDB versions

1. Change `TARGET_DUCKDB_VERSION` in `Makefile`.
2. Change the `duckdb` dependency to the matching encoded duckdb-rs version. For example, DuckDB v1.5.5 maps to `1.10505.x`.
3. Update the `extension-ci-tools` submodule to the branch/commit appropriate for the target.
4. Run:

```bash
make clean_all
make configure
make release
make test_release
```

Do not mix DuckDB target versions, duckdb-rs versions, and CI-tool versions. The unstable C API build is intentionally version-specific.

## Why `rdkafka` uses `cmake-build`

The extension builds librdkafka from the crate's pinned source rather than requiring a matching system `librdkafka` shared library. This makes CI and release artifacts more reproducible and avoids a common corporate-machine failure mode where the installed librdkafka version differs from the Rust crate's expected ABI.

If your environment requires a system librdkafka, change the Cargo feature to `dynamic-linking` and ensure the installed library exactly matches the version expected by `rdkafka-sys`.

## Corporate proxy/build troubleshooting

If dependency downloads fail behind a proxy, configure Cargo and Git to use the corporate proxy/CA in the normal way before running `make configure`.

For DuckDB itself, duckdb-rs can also use a pre-existing DuckDB library when configured appropriately; do not add the `bundled` feature to this loadable-extension target unless you intentionally want to build another copy of DuckDB.

For librdkafka, the current configuration is self-contained through `cmake-build`, so `pkg-config` is not required for librdkafka discovery.

## Porting checklist

- [x] Pure-Rust DuckDB C API entry point
- [x] `tributary_scan_topic(topic, bootstrap_servers)` registration
- [x] Kafka metadata discovery
- [x] Per-partition low/high watermark snapshot
- [x] Raw key and message bytes
- [x] Kafka headers as `LIST<STRUCT(key VARCHAR, value BLOB)>`
- [x] Duplicate header names preserved
- [x] NULL header values preserved
- [x] Single-threaded correctness-first execution
- [x] SQLLogicTest load coverage
- [x] CI release build
- [x] Kafka/Redpanda integration smoke test
- [ ] Schema Registry decoding
- [ ] Kafka producer functions
- [ ] Tributary metadata/secrets parity
- [ ] Parallel partition execution
- [ ] Cross-platform release validation

## Release process

The CI workflow produces the Linux amd64 extension as a GitHub Actions artifact. A tagged release should only be cut after the build, SQL tests, and Kafka integration smoke test are green.
