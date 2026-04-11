use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{Duration, TimeZone, Utc};
use deltalake::arrow::array::Int64Array;
use mqtt_to_delta::{
    cache::{CacheTier, HybridCacheConfig, HybridTelemetryCache},
    message::{TelemetryMessage, event_date_from_ts_ms},
    persistence::{DeltaWriteOptions, write_telemetry_batch},
    pipeline::{build_record_batch, telemetry_schema},
    query::{QueryRequest, QueryService, QuerySnapshotMode},
};

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("mqtt_to_delta_{name}_{nanos}"))
}

fn message(ts: chrono::DateTime<Utc>, idx: usize, temperature: f64) -> TelemetryMessage {
    let ts_ms = ts.timestamp_millis();
    TelemetryMessage {
        device_id: format!("device-{idx:02}"),
        ts_ms,
        temperature,
        humidity: 40.0 + idx as f64,
        event_date: event_date_from_ts_ms(ts_ms).unwrap(),
    }
}

async fn write_messages(table_root: &PathBuf, messages: Vec<TelemetryMessage>) {
    let batch = build_record_batch(telemetry_schema(), &messages).unwrap();
    write_telemetry_batch(table_root.to_str().unwrap(), batch, &DeltaWriteOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn datafusion_query_service_runs_aggregate_sql() {
    let table_root = unique_test_dir("query_aggregate");
    let first_day = Utc.with_ymd_and_hms(2026, 4, 11, 0, 0, 0).single().unwrap();
    let second_day = first_day + Duration::days(1);

    write_messages(
        &table_root,
        vec![
            message(first_day, 0, 21.0),
            message(first_day + Duration::minutes(5), 1, 22.0),
            message(second_day, 2, 24.0),
        ],
    )
    .await;

    let mut service = QueryService::datafusion();
    let response = service
        .execute(
            table_root.to_str().unwrap(),
            QueryRequest::new(
                "SELECT COUNT(*) AS total_rows \
                 FROM telemetry \
                 WHERE event_date = '2026-04-11'",
            ),
        )
        .await
        .unwrap();

    assert_eq!(response.engine, "datafusion");
    assert_eq!(response.snapshot_version, 1);
    assert_eq!(response.row_count, 1);
    assert_eq!(response.column_names, vec!["total_rows"]);
    assert_eq!(response.metadata_tier, None);
    assert_eq!(scalar_count(&response), 2);

    fs::remove_dir_all(&table_root).unwrap();
}

#[tokio::test]
async fn query_service_rejects_non_select_sql() {
    let table_root = unique_test_dir("query_reject");
    let mut service = QueryService::datafusion();

    let error = service
        .execute(
            table_root.to_str().unwrap(),
            QueryRequest::new("DELETE FROM telemetry"),
        )
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("only read-only SELECT statements are supported")
    );
}

#[tokio::test]
async fn cached_and_latest_snapshot_modes_query_different_versions() {
    let table_root = unique_test_dir("query_snapshot_modes");
    let cache_root = unique_test_dir("query_snapshot_modes_cache");
    let base = Utc.with_ymd_and_hms(2026, 4, 11, 0, 0, 0).single().unwrap();

    write_messages(&table_root, vec![message(base, 0, 21.0)]).await;

    let cache = HybridTelemetryCache::new(cache_root.as_path(), HybridCacheConfig::default())
        .unwrap();
    let mut service = QueryService::datafusion().with_cache(cache);

    let mut cached_request = QueryRequest::new("SELECT COUNT(*) AS total_rows FROM telemetry");
    cached_request.snapshot_mode = QuerySnapshotMode::Cached;

    let first = service
        .execute(table_root.to_str().unwrap(), cached_request.clone())
        .await
        .unwrap();
    let second = service
        .execute(table_root.to_str().unwrap(), cached_request.clone())
        .await
        .unwrap();

    write_messages(&table_root, vec![message(base + Duration::minutes(1), 1, 22.0)]).await;

    let cached_after_write = service
        .execute(table_root.to_str().unwrap(), cached_request)
        .await
        .unwrap();

    let latest_after_write = service
        .execute(
            table_root.to_str().unwrap(),
            QueryRequest::new("SELECT COUNT(*) AS total_rows FROM telemetry"),
        )
        .await
        .unwrap();

    assert_eq!(first.metadata_tier, Some(CacheTier::Source));
    assert_eq!(second.metadata_tier, Some(CacheTier::Memory));
    assert_eq!(first.snapshot_version, 1);
    assert_eq!(second.snapshot_version, 1);
    assert_eq!(cached_after_write.snapshot_version, 1);
    assert_eq!(latest_after_write.snapshot_version, 2);

    let cached_count = scalar_count(&cached_after_write);
    let latest_count = scalar_count(&latest_after_write);
    assert_eq!(cached_count, 1);
    assert_eq!(latest_count, 2);

    fs::remove_dir_all(&table_root).unwrap();
    fs::remove_dir_all(&cache_root).unwrap();
}

fn scalar_count(response: &mqtt_to_delta::query::QueryResponse) -> i64 {
    response.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
