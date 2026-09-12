mod kafka_metadata;
mod kafka_produce;
mod kafka_scan;

use duckdb::{duckdb_entrypoint_c_api, Connection, Result};
use std::error::Error;

#[duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<kafka_scan::KafkaScan>("tributary_scan_topic")?;
    con.register_table_function::<kafka_metadata::KafkaMetadata>("tributary_metadata")?;
    con.register_table_function::<kafka_produce::KafkaProduce>("tributary_produce")?;
    Ok(())
}
