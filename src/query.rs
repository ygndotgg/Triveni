use std::{error::Error, future::Future, pin::Pin};

use deltalake::{
    DeltaTable, ensure_table_uri, open_table_with_version,
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    datafusion::prelude::SessionContext,
};

use crate::{
    cache::{CacheTier, HybridTelemetryCache},
    persistence::{DeltaTableSummary, describe_telemetry_table},
};

type QueryFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, Box<dyn Error>>> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuerySnapshotMode {
    Latest,
    Cached,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryRequest {
    pub sql: String,
    pub table_name: String,
    pub max_rows: Option<usize>,
    pub snapshot_mode: QuerySnapshotMode,
}

impl QueryRequest {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            table_name: "telemetry".to_string(),
            max_rows: Some(10_000),
            snapshot_mode: QuerySnapshotMode::Latest,
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryResponse {
    pub engine: &'static str,
    pub sql: String,
    pub snapshot_version: i64,
    pub row_count: usize,
    pub column_names: Vec<String>,
    pub metadata_tier: Option<CacheTier>,
    pub table_summary: DeltaTableSummary,
    pub batches: Vec<RecordBatch>,
}

pub struct QueryService {
    engine: Box<dyn QueryEngineAdapter>,
    cache: Option<HybridTelemetryCache>,
}

impl QueryService {
    pub fn datafusion() -> Self {
        Self {
            engine: Box::new(DataFusionQueryEngine),
            cache: None,
        }
    }

    pub fn with_cache(mut self, cache: HybridTelemetryCache) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn cache_mut(&mut self) -> Option<&mut HybridTelemetryCache> {
        self.cache.as_mut()
    }

    pub async fn execute(
        &mut self,
        table_path: &str,
        request: QueryRequest,
    ) -> Result<QueryResponse, Box<dyn Error>> {
        let normalized_sql = normalize_read_only_sql(&request.sql)?;
        let snapshot = self.resolve_snapshot(table_path, &request).await?;
        let batches = self
            .engine
            .execute(table_path, snapshot.summary.version, &request.table_name, &normalized_sql)
            .await?;

        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        if let Some(max_rows) = request.max_rows {
            if row_count > max_rows {
                return Err(format!(
                    "query returned {row_count} rows which exceeds the configured max_rows={max_rows}"
                )
                .into());
            }
        }

        let column_names = batches
            .first()
            .map(|batch| schema_columns(&batch.schema()))
            .unwrap_or_default();

        Ok(QueryResponse {
            engine: self.engine.name(),
            sql: normalized_sql,
            snapshot_version: snapshot.summary.version,
            row_count,
            column_names,
            metadata_tier: snapshot.metadata_tier,
            table_summary: snapshot.summary,
            batches,
        })
    }

    async fn resolve_snapshot(
        &mut self,
        table_path: &str,
        request: &QueryRequest,
    ) -> Result<ResolvedSnapshot, Box<dyn Error>> {
        if let Some(cache) = self.cache.as_mut() {
            let cached = match request.snapshot_mode {
                QuerySnapshotMode::Latest => cache.refresh_snapshot(table_path).await?,
                QuerySnapshotMode::Cached => cache.load_snapshot(table_path).await?,
            };
            return Ok(ResolvedSnapshot {
                summary: cached.summary,
                metadata_tier: Some(cached.metadata_tier),
            });
        }

        let summary = describe_telemetry_table(table_path).await?;
        Ok(ResolvedSnapshot {
            summary,
            metadata_tier: None,
        })
    }
}

struct ResolvedSnapshot {
    summary: DeltaTableSummary,
    metadata_tier: Option<CacheTier>,
}

trait QueryEngineAdapter: Send + Sync {
    fn name(&self) -> &'static str;

    fn execute<'a>(
        &'a self,
        table_path: &'a str,
        version: i64,
        table_name: &'a str,
        sql: &'a str,
    ) -> QueryFuture<'a>;
}

struct DataFusionQueryEngine;

impl QueryEngineAdapter for DataFusionQueryEngine {
    fn name(&self) -> &'static str {
        "datafusion"
    }

    fn execute<'a>(
        &'a self,
        table_path: &'a str,
        version: i64,
        table_name: &'a str,
        sql: &'a str,
    ) -> QueryFuture<'a> {
        Box::pin(async move {
            let table = open_snapshot(table_path, version).await?;
            let provider = table.table_provider().await?;
            let ctx = SessionContext::new();
            ctx.register_table(table_name, provider)?;
            let dataframe = ctx.sql(sql).await?;
            Ok(dataframe.collect().await?)
        })
    }
}

async fn open_snapshot(table_path: &str, version: i64) -> Result<DeltaTable, Box<dyn Error>> {
    let table_uri = ensure_table_uri(table_path)?;
    Ok(open_table_with_version(table_uri, version).await?)
}

fn normalize_read_only_sql(sql: &str) -> Result<String, Box<dyn Error>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err("sql query must not be empty".into());
    }

    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
    if trimmed.contains(';') {
        return Err("multiple SQL statements are not supported".into());
    }

    let lowercase = trimmed.to_ascii_lowercase();
    if !(lowercase.starts_with("select") || lowercase.starts_with("with")) {
        return Err("only read-only SELECT statements are supported".into());
    }

    Ok(trimmed.to_string())
}

fn schema_columns(schema: &SchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}
