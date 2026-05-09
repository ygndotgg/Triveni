use std::{error::Error, fs, num::NonZeroU64, path::PathBuf};

use deltalake::{
    DeltaTable, DeltaTableBuilder,
    arrow::record_batch::RecordBatch,
    ensure_table_uri,
    kernel::{DataType as DeltaDataType, PrimitiveType, StructField},
    operations::optimize::OptimizeType,
    parquet::{basic::Compression, file::properties::WriterProperties},
};

const DEFAULT_PARTITION_COLUMN: &str = "event_date";
const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaWriteOptions {
    pub partition_columns: Vec<String>,
    pub target_file_size_bytes: Option<u64>,
    pub write_batch_size: Option<usize>,
}

impl Default for DeltaWriteOptions {
    fn default() -> Self {
        Self {
            partition_columns: vec![DEFAULT_PARTITION_COLUMN.to_string()],
            target_file_size_bytes: Some(DEFAULT_TARGET_FILE_SIZE_BYTES),
            write_batch_size: Some(DEFAULT_WRITE_BATCH_SIZE),
        }
    }
}

impl DeltaWriteOptions {
    fn target_file_size(&self) -> Result<Option<NonZeroU64>, Box<dyn Error>> {
        match self.target_file_size_bytes {
            Some(0) => Err("target_file_size_bytes must be greater than zero".into()),
            Some(size) => Ok(NonZeroU64::new(size)),
            None => Ok(None),
        }
    }

    fn validated_write_batch_size(&self) -> Result<Option<usize>, Box<dyn Error>> {
        match self.write_batch_size {
            Some(0) => Err("write_batch_size must be greater than zero".into()),
            value => Ok(value),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaTableSummary {
    pub table_uri: String,
    pub version: i64,
    pub active_files: usize,
    pub partition_columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaActiveFile {
    pub relative_path: String,
    pub absolute_path: PathBuf,
    pub size_bytes: u64,
    pub event_date: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDeltaSnapshot {
    pub summary: DeltaTableSummary,
    pub active_files: Vec<DeltaActiveFile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaWriteOutcome {
    pub table_uri: String,
    pub version: i64,
    pub active_files: usize,
    pub rows_written: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaOptimizeOutcome {
    pub table_uri: String,
    pub version: i64,
    pub active_files: usize,
    pub num_files_added: u64,
    pub num_files_removed: u64,
    pub total_files_skipped: usize,
}

pub async fn ensure_telemetry_table(
    table_path: &str,
    options: &DeltaWriteOptions,
) -> Result<DeltaTable, Box<dyn Error>> {
    let table_uri = ensure_table_uri(table_path)?;
    match deltalake::open_table(table_uri.clone()).await {
        Ok(table) => Ok(table),
        Err(deltalake::DeltaTableError::NotATable(_)) => {
            let create = DeltaTableBuilder::from_url(table_uri)?.build()?.create();

            let create = create
                .with_columns(telemetry_delta_columns())
                .with_partition_columns(options.partition_columns.clone())
                .with_configuration(table_configuration(options));
            Ok(create.await?)
        }
        Err(err) => Err(Box::new(err)),
    }
}

pub async fn write_telemetry_batch(
    table_path: &str,
    batch: RecordBatch,
    options: &DeltaWriteOptions,
) -> Result<DeltaWriteOptions, Box<dyn Error>> {
    let rows_writeen = batch.num_rows();
    let table = ensure
    unimplemented!()
}

fn ensure_local_table_path(table_path: &str) -> Result<(), Box<dyn Error>> {
    if !table_path.contains("://") {
        fs::create_dir_all(table_path)?;
    }
    Ok(())
}

fn telemetry_delta_columns() -> Vec<StructField> {
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

fn table_configuration(options: &DeltaWriteOptions) -> Vec<(String, Option<String>)> {
    let mut configuration = Vec::new();
    if let Some(target_file_size_bytes) = options.target_file_size_bytes {
        configuration.push((
            "delta.targetFileSize".to_string(),
            Some(target_file_size_bytes.to_string()),
        ));
    }
    configuration
}
