use std::{error::Error, path::PathBuf};

use chrono::Utc;
use mqtt_to_delta::{
    batcher::MessageBatcher,
    pipeline::{
        build_record_batch, generate_messages_for_next_days, telemetry_schema, write_to_delete,
        write_to_delta,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut batcher = MessageBatcher::default();
    let schema = telemetry_schema();
    let table_root = PathBuf::from("table");
    // let mut written_files = Vec::new();
    let mut batch_id = 0usize;

    for message in generate_messages_for_next_days(Utc::now(), 7, 100)? {
        if let Some(batch) = batcher.push(message) {
            let record_batch = build_record_batch(schema.clone(), &batch)?;
            write_to_delta("delta-table", record_batch).await?;
        }
    }

    // for path in &written_files {
    //     print_parquet_metadata(path)?;
    // }

    Ok(())
}
