use std::{
    collections::BTreeMap,
    error::Error,
    fs::{File, OpenOptions},
    path::Path,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{Field, Schema},
};
use mqtt_to_delta::{
    batcher::MessageBatcher,
    message::{TelemetryMessage, event_date_from_ts_ms},
};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
    },
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut outputpath = Vec::new();
    let mut batcher = MessageBatcher::default();
    let schema = Arc::new(Schema::new(vec![
        Field::new("device_id", arrow::datatypes::DataType::Utf8, false),
        Field::new("ts_ms", arrow::datatypes::DataType::Int64, false),
        Field::new("temperature", arrow::datatypes::DataType::Float64, false),
        Field::new("humidity", arrow::datatypes::DataType::Float64, false),
        Field::new("event_date", arrow::datatypes::DataType::Utf8, false),
    ]));
    for i in 0..1000 {
        let ts_ms = 1_711_600_000_123_i64 + i * 1_0000;
        let k = TelemetryMessage {
            device_id: format!("id-{i}"),
            ts_ms,
            event_date: event_date_from_ts_ms(ts_ms)?,
            temperature: (i as f64 + 32.942) as f64,
            humidity: (i as f64 + 2331.21) as f64,
        };
        if let Some(batch) = batcher.push(k) {
            let mut grouped: BTreeMap<String, Vec<TelemetryMessage>> = BTreeMap::new();
            for msg in batch {
                grouped.entry(msg.event_date.clone()).or_default().push(msg);
            }
            for (event_date, rows) in grouped {
                let record_batch = build_record_batch(
                    schema.clone(),
                    rows.iter()
                        .map(|f| {
                            (
                                f.device_id.clone(),
                                f.ts_ms,
                                f.temperature,
                                f.humidity,
                                f.event_date.clone(),
                            )
                        })
                        .collect(),
                )?;
                let output_path = format!("table/event_date={event_date}/part-00001.parquet");

                write_parquet(schema.clone(), &record_batch, &output_path)?;

                outputpath.push(output_path);
            }
        }
    }
    for i in outputpath {
        print_parquet_metadata(&i)?;
    }

    Ok(())
}

fn print_parquet_metadata(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let parquet_reader = SerializedFileReader::new(file)?;
    let metadata = parquet_reader.metadata();

    println!("Parquet row groups: {}", metadata.num_row_groups());
    for i in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(i);
        println!("Row group {i}: {} rows", row_group.num_rows());
        for j in 0..row_group.num_columns() {
            let column = row_group.column(j);
            println!(
                "  column {}: {}, compressed={} bytes",
                j,
                column.column_path().string(),
                column.compressed_size()
            );
        }
    }

    Ok(())
}

fn build_record_batch(
    schema: Arc<Schema>,
    messages: Vec<(String, i64, f64, f64, String)>,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut deviceid_builder = StringBuilder::new();
    let mut temp_builder = Float64Builder::new();
    let mut tms_builder = Int64Builder::new();

    let mut humidity_builder = Float64Builder::new();
    let mut date_builder = StringBuilder::new();

    for (d_id, tms, temp, hmd, date) in messages {
        deviceid_builder.append_value(d_id);
        temp_builder.append_value(temp);
        tms_builder.append_value(tms);
        humidity_builder.append_value(hmd);
        date_builder.append_value(date);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(deviceid_builder.finish()),
        Arc::new(tms_builder.finish()),
        Arc::new(temp_builder.finish()),
        Arc::new(humidity_builder.finish()),
        Arc::new(date_builder.finish()),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn write_parquet(
    schema: Arc<Schema>,
    batch: &RecordBatch,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    Ok(())
}
