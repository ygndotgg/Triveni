use std::error::Error;

use chrono::Utc;
use mqtt_to_delta::{
    batcher::MessageBatcher,
    pipeline::{
        build_record_batch, generate_messages_for_next_days, telemetry_schema,
        write_to_delta,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut batcher = MessageBatcher::default();
    let schema = telemetry_schema();

    for message in generate_messages_for_next_days(Utc::now(), 7, 100)? {
        if let Some(batch) = batcher.push(message) {
            let record_batch = build_record_batch(schema.clone(), &batch)?;
            write_to_delta("delta-table", record_batch).await?;
        }
    }

    if let Some(batch) = batcher.flush() {
        let record_batch = build_record_batch(schema, &batch)?;
        write_to_delta("delta-table", record_batch).await?;
    }

    Ok(())
}
