use std::{error::Error, sync::Arc};

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use deltalake::{
    DeltaTableBuilder, DeltaTableError, ensure_table_uri,
    arrow::{
        array::{ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder},
        datatypes::{DataType, Field, Schema},
    },
    kernel::{DataType as DeltaDataType, PrimitiveType, StructField},
};

use crate::message::{TelemetryMessage, event_date_from_ts_ms};
pub use crate::parquet_man::{print_parquet_metadata, write_parquet, write_partitioned_batch};

pub fn telemetry_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("device_id", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("humidity", DataType::Float64, false),
        Field::new("event_date", DataType::Utf8, false),
    ]))
}

pub fn generate_messages_for_next_days(
    start: DateTime<Utc>,
    num_days: usize,
    messages_per_day: usize,
) -> Result<Vec<TelemetryMessage>, Box<dyn Error>> {
    let start_of_day = Utc
        .with_ymd_and_hms(start.year(), start.month(), start.day(), 0, 0, 0)
        .single()
        .ok_or("failed to build UTC start-of-day timestamp")?;

    let mut messages = Vec::with_capacity(num_days * messages_per_day);
    for day in 0..num_days {
        let day_start = start_of_day + Duration::days(day as i64);
        for idx in 0..messages_per_day {
            let ts_ms = (day_start + Duration::minutes((idx * 10) as i64)).timestamp_millis();
            messages.push(TelemetryMessage {
                device_id: format!("device-{day:02}-{idx:03}"),
                ts_ms,
                temperature: 20.0 + day as f64 + idx as f64 * 0.1,
                humidity: 40.0 + day as f64 + idx as f64 * 0.2,
                event_date: event_date_from_ts_ms(ts_ms)?,
            });
        }
    }

    Ok(messages)
}

pub fn build_record_batch(
    schema: Arc<Schema>,
    messages: &[TelemetryMessage],
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut device_id_builder = StringBuilder::new();
    let mut ts_ms_builder = Int64Builder::new();
    let mut temperature_builder = Float64Builder::new();
    let mut humidity_builder = Float64Builder::new();
    let mut event_date_builder = StringBuilder::new();

    for msg in messages {
        device_id_builder.append_value(&msg.device_id);
        ts_ms_builder.append_value(msg.ts_ms);
        temperature_builder.append_value(msg.temperature);
        humidity_builder.append_value(msg.humidity);
        event_date_builder.append_value(&msg.event_date);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(device_id_builder.finish()),
        Arc::new(ts_ms_builder.finish()),
        Arc::new(temperature_builder.finish()),
        Arc::new(humidity_builder.finish()),
        Arc::new(event_date_builder.finish()),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

pub async fn write_to_delta(table_path: &str, batch: RecordBatch) -> Result<(), Box<dyn Error>> {
    let table_uri = ensure_table_uri(table_path)?;
    let maybe_table = deltalake::open_table(table_uri.clone()).await;

    let table = match maybe_table {
        Ok(table) => table,
        Err(DeltaTableError::NotATable(_)) => {
            let table = DeltaTableBuilder::from_url(table_uri)?.build()?;
            table
                .create()
                .with_columns(delta_columns())
                .with_partition_columns(["event_date"])
                .await?
        }
        Err(err) => return Err(Box::new(err)),
    };

    table.write(vec![batch]).await?;
    Ok(())
}

fn delta_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            "device_id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "ts_ms".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        StructField::new(
            "temperature".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Double),
            false,
        ),
        StructField::new(
            "humidity".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Double),
            false,
        ),
        StructField::new(
            "event_date".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
    ]
}
