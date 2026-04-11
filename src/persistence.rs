use std::{error::Error, fs, num::NonZeroU64};

use deltalake::{
    DeltaTable, DeltaTableBuilder, ensure_table_uri,
    arrow::record_batch::RecordBatch,
    kernel::{DataType as DeltaDataType, PrimitiveType, StructField},
    operations::optimize::OptimizeType,
    parquet::{
        basic::Compression,
        file::properties::WriterProperties,
    },
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
    ensure_local_table_path(table_path)?;
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
) -> Result<DeltaWriteOutcome, Box<dyn Error>> {
    let rows_written = batch.num_rows();
    let table = ensure_telemetry_table(table_path, options).await?;

    let mut write = table
        .write(vec![batch])
        .with_partition_columns(options.partition_columns.clone())
        .with_writer_properties(default_writer_properties());

    if let Some(target_file_size) = options.target_file_size()? {
        write = write.with_target_file_size(Some(target_file_size));
    }

    if let Some(write_batch_size) = options.validated_write_batch_size()? {
        write = write.with_write_batch_size(write_batch_size);
    }

    let table = write.await?;
    let summary = summarize_loaded_table(&table)?;

    Ok(DeltaWriteOutcome {
        table_uri: summary.table_uri,
        version: summary.version,
        active_files: summary.active_files,
        rows_written,
    })
}

pub async fn describe_telemetry_table(
    table_path: &str,
) -> Result<DeltaTableSummary, Box<dyn Error>> {
    let table = open_existing_table(table_path).await?;
    Ok(summarize_loaded_table(&table)?)
}

pub async fn list_active_file_uris(table_path: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let table = open_existing_table(table_path).await?;
    Ok(table.get_file_uris()?.collect())
}

pub async fn compact_telemetry_table(
    table_path: &str,
    target_size_bytes: u64,
) -> Result<DeltaOptimizeOutcome, Box<dyn Error>> {
    let target_size =
        NonZeroU64::new(target_size_bytes).ok_or("target_size_bytes must be greater than zero")?;
    let table = open_existing_table(table_path).await?;

    let (table, metrics) = table
        .optimize()
        .with_type(OptimizeType::Compact)
        .with_target_size(target_size)
        .await?;

    let summary = summarize_loaded_table(&table)?;

    Ok(DeltaOptimizeOutcome {
        table_uri: summary.table_uri,
        version: summary.version,
        active_files: summary.active_files,
        num_files_added: metrics.num_files_added,
        num_files_removed: metrics.num_files_removed,
        total_files_skipped: metrics.total_files_skipped,
    })
}

fn summarize_loaded_table(table: &DeltaTable) -> Result<DeltaTableSummary, deltalake::DeltaTableError> {
    let state = table.snapshot()?;
    Ok(DeltaTableSummary {
        table_uri: table.table_url().to_string(),
        version: state.version(),
        active_files: state.log_data().num_files(),
        partition_columns: state.metadata().partition_columns().to_vec(),
    })
}

async fn open_existing_table(table_path: &str) -> Result<DeltaTable, Box<dyn Error>> {
    let table_uri = ensure_table_uri(table_path)?;
    Ok(deltalake::open_table(table_uri).await?)
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

fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

fn ensure_local_table_path(table_path: &str) -> Result<(), Box<dyn Error>> {
    if !table_path.contains("://") {
        fs::create_dir_all(table_path)?;
    }
    Ok(())
}
