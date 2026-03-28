use std::{
    collections::BTreeMap,
    error::Error,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
};
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
    },
};

use crate::message::{TelemetryMessage, event_date_from_ts_ms};

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
    Ok(())
}

// pub fn write_partitioned_batch(
//     schema: Arc<Schema>,
//     batch_id: usize,
//     batch: Vec<TelemetryMessage>,
//     table_root: &Path,
// ) -> Result<Vec<PathBuf>, Box<dyn Error>> {
//     let mut grouped: BTreeMap<String, Vec<TelemetryMessage>> = BTreeMap::new();
//     for msg in batch {
//         grouped.entry(msg.event_date.clone()).or_default().push(msg);
//     }

//     let mut written_paths = Vec::with_capacity(grouped.len());
//     for (event_date, rows) in grouped {
//         let record_batch = build_record_batch(schema.clone(), &rows)?;
//         let partition_dir = table_root.join(format!("event_date={event_date}"));
//         let output_path = partition_dir.join(format!("part-{batch_id:05}.parquet"));
//         write_parquet(schema.clone(), &record_batch, &output_path)?;
//         written_paths.push(output_path);
//     }

//     Ok(written_paths)
// }

// pub fn write_parquet(
//     schema: Arc<Schema>,
//     batch: &RecordBatch,
//     path: &Path,
// ) -> Result<(), Box<dyn Error>> {
//     if let Some(parent) = path.parent() {
//         fs::create_dir_all(parent)?;
//     }

//     let file = File::create(path)?;
//     let props = WriterProperties::builder()
//         .set_compression(Compression::SNAPPY)
//         .build();
//     let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
//     writer.write(batch)?;
//     writer.close()?;

//     Ok(())
// }

// pub fn print_parquet_metadata(path: &Path) -> Result<(), Box<dyn Error>> {
//     let file = File::open(path)?;
//     let parquet_reader = SerializedFileReader::new(file)?;
//     let metadata = parquet_reader.metadata();

//     println!("Parquet file: {}", path.display());
//     println!("Parquet row groups: {}", metadata.num_row_groups());
//     for i in 0..metadata.num_row_groups() {
//         let row_group = metadata.row_group(i);
//         println!("Row group {i}: {} rows", row_group.num_rows());
//         for j in 0..row_group.num_columns() {
//             let column = row_group.column(j);
//             println!(
//                 "  column {}: {}, compressed={} bytes",
//                 j,
//                 column.column_path().string(),
//                 column.compressed_size()
//             );
//         }
//     }

//     Ok(())
// }
