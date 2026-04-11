use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{Duration, TimeZone, Utc};
use deltalake::{ensure_table_uri, open_table, operations::collect_sendable_stream};
use mqtt_to_delta::{
    message::{TelemetryMessage, event_date_from_ts_ms},
    persistence::{
        DeltaWriteOptions, compact_telemetry_table, describe_telemetry_table,
        list_active_file_uris, write_telemetry_batch,
    },
    pipeline::{build_record_batch, generate_messages_for_next_days, telemetry_schema},
};

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("mqtt_to_delta_{name}_{nanos}"))
}

async fn scan_row_count(table_root: &PathBuf) -> usize {
    let table_uri = ensure_table_uri(table_root.to_str().unwrap()).unwrap();
    let table = open_table(table_uri).await.unwrap();
    let (_table, stream) = table.scan_table().await.unwrap();
    let batches = collect_sendable_stream(stream).await.unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[tokio::test]
async fn delta_write_exposes_latest_snapshot_metadata() {
    let table_root = unique_test_dir("delta_summary");
    let schema = telemetry_schema();
    let options = DeltaWriteOptions {
        target_file_size_bytes: Some(256 * 1024),
        write_batch_size: Some(16),
        ..Default::default()
    };

    let start = Utc.with_ymd_and_hms(2026, 4, 11, 0, 0, 0).single().unwrap();
    let messages = generate_messages_for_next_days(start, 1, 4).unwrap();
    let batch = build_record_batch(schema, &messages).unwrap();

    let write = write_telemetry_batch(table_root.to_str().unwrap(), batch, &options)
        .await
        .unwrap();
    let summary = describe_telemetry_table(table_root.to_str().unwrap())
        .await
        .unwrap();
    let active_files = list_active_file_uris(table_root.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(write.rows_written, 4);
    assert_eq!(write.version, 1, "create is version 0, first write is version 1");
    assert_eq!(summary.version, 1);
    assert_eq!(summary.active_files, 1);
    assert_eq!(summary.partition_columns, vec!["event_date".to_string()]);
    assert_eq!(active_files.len(), 1);
    assert!(active_files[0].contains("event_date=2026-04-11"));
    assert_eq!(scan_row_count(&table_root).await, 4);

    fs::remove_dir_all(&table_root).unwrap();
}

#[tokio::test]
async fn compaction_reduces_small_committed_files_without_losing_rows() {
    let table_root = unique_test_dir("delta_compact");
    let schema = telemetry_schema();
    let options = DeltaWriteOptions {
        target_file_size_bytes: Some(16 * 1024),
        write_batch_size: Some(1),
        ..Default::default()
    };

    let base_ts = Utc
        .with_ymd_and_hms(2026, 4, 11, 0, 0, 0)
        .single()
        .unwrap();

    for idx in 0..8 {
        let ts_ms = (base_ts + Duration::minutes(idx)).timestamp_millis();
        let batch = build_record_batch(
            schema.clone(),
            &[TelemetryMessage {
                device_id: format!("device-{idx:02}"),
                ts_ms,
                temperature: 20.0 + idx as f64,
                humidity: 40.0 + idx as f64,
                event_date: event_date_from_ts_ms(ts_ms).unwrap(),
            }],
        )
        .unwrap();

        write_telemetry_batch(table_root.to_str().unwrap(), batch, &options)
            .await
            .unwrap();
    }

    let before = describe_telemetry_table(table_root.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(before.active_files, 8);
    assert_eq!(scan_row_count(&table_root).await, 8);

    let optimized = compact_telemetry_table(table_root.to_str().unwrap(), 1024 * 1024)
        .await
        .unwrap();
    let after = describe_telemetry_table(table_root.to_str().unwrap())
        .await
        .unwrap();

    assert!(optimized.num_files_removed > 0);
    assert!(optimized.num_files_added > 0);
    assert!(after.active_files < before.active_files);
    assert_eq!(optimized.version, after.version);
    assert_eq!(optimized.active_files, after.active_files);
    assert_eq!(scan_row_count(&table_root).await, 8);

    fs::remove_dir_all(&table_root).unwrap();
}
