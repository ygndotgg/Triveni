use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{Duration, TimeZone, Utc};
use mqtt_to_delta::{
    cache::{CacheTier, CacheValueClass, HybridCacheConfig, HybridTelemetryCache},
    message::{TelemetryMessage, event_date_from_ts_ms},
    persistence::{DeltaWriteOptions, compact_telemetry_table, write_telemetry_batch},
    pipeline::{build_record_batch, telemetry_schema},
};

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("mqtt_to_delta_{name}_{nanos}"))
}

fn telemetry_message(ts: chrono::DateTime<Utc>, idx: usize) -> TelemetryMessage {
    let ts_ms = ts.timestamp_millis();
    TelemetryMessage {
        device_id: format!("device-{idx:02}"),
        ts_ms,
        temperature: 20.0 + idx as f64,
        humidity: 40.0 + idx as f64,
        event_date: event_date_from_ts_ms(ts_ms).unwrap(),
    }
}

async fn write_single_row(
    table_root: &PathBuf,
    ts: chrono::DateTime<Utc>,
    idx: usize,
) {
    let batch = build_record_batch(telemetry_schema(), &[telemetry_message(ts, idx)]).unwrap();
    let options = DeltaWriteOptions {
        target_file_size_bytes: Some(16 * 1024),
        write_batch_size: Some(1),
        ..Default::default()
    };

    write_telemetry_batch(table_root.to_str().unwrap(), batch, &options)
        .await
        .unwrap();
}

#[tokio::test]
async fn metadata_uses_memory_after_first_snapshot_load() {
    let table_root = unique_test_dir("cache_metadata");
    let cache_root = unique_test_dir("cache_metadata_store");

    write_single_row(
        &table_root,
        Utc.with_ymd_and_hms(2026, 4, 11, 0, 0, 0).single().unwrap(),
        0,
    )
    .await;

    let mut cache = HybridTelemetryCache::new(cache_root.as_path(), HybridCacheConfig::default())
        .unwrap();

    let first = cache.load_snapshot(table_root.to_str().unwrap()).await.unwrap();
    let second = cache.load_snapshot(table_root.to_str().unwrap()).await.unwrap();
    let metrics = cache.metrics();

    assert_eq!(first.metadata_tier, CacheTier::Source);
    assert_eq!(second.metadata_tier, CacheTier::Memory);
    assert_eq!(metrics.metadata_misses, 1);
    assert_eq!(metrics.metadata_hits, 1);

    fs::remove_dir_all(&table_root).unwrap();
    fs::remove_dir_all(&cache_root).unwrap();
}

#[tokio::test]
async fn recent_data_promotes_to_memory_while_historical_data_stays_on_disk() {
    let table_root = unique_test_dir("cache_tiers");
    let cache_root = unique_test_dir("cache_tiers_store");

    let newest = Utc.with_ymd_and_hms(2026, 4, 11, 12, 0, 0).single().unwrap();
    let older = newest - Duration::days(20);
    write_single_row(&table_root, newest, 0).await;
    write_single_row(&table_root, older, 1).await;

    let mut cache = HybridTelemetryCache::new(
        cache_root.as_path(),
        HybridCacheConfig {
            memory_capacity_bytes: 1024 * 1024,
            disk_capacity_bytes: 1024 * 1024,
            memory_entry_max_bytes: 64 * 1024,
            disk_entry_max_bytes: 256 * 1024,
            recent_partition_days: 1,
            promote_after_accesses: 2,
        },
    )
    .unwrap();

    let snapshot = cache.refresh_snapshot(table_root.to_str().unwrap()).await.unwrap();
    let recent_file = snapshot
        .active_files
        .iter()
        .find(|file| file.class == CacheValueClass::RecentData)
        .unwrap()
        .relative_path
        .clone();
    let historical_file = snapshot
        .active_files
        .iter()
        .find(|file| file.class == CacheValueClass::HistoricalData)
        .unwrap()
        .relative_path
        .clone();

    let recent_first = cache
        .read_file(table_root.to_str().unwrap(), &recent_file)
        .await
        .unwrap();
    let recent_second = cache
        .read_file(table_root.to_str().unwrap(), &recent_file)
        .await
        .unwrap();
    let recent_third = cache
        .read_file(table_root.to_str().unwrap(), &recent_file)
        .await
        .unwrap();

    let historical_first = cache
        .read_file(table_root.to_str().unwrap(), &historical_file)
        .await
        .unwrap();
    let historical_second = cache
        .read_file(table_root.to_str().unwrap(), &historical_file)
        .await
        .unwrap();

    assert_eq!(recent_first.tier, CacheTier::Source);
    assert_eq!(recent_second.tier, CacheTier::Disk);
    assert_eq!(recent_third.tier, CacheTier::Memory);
    assert!(cache.contains_in_memory(table_root.to_str().unwrap(), &recent_file));

    assert_eq!(historical_first.tier, CacheTier::Source);
    assert_eq!(historical_second.tier, CacheTier::Disk);
    assert!(!cache.contains_in_memory(table_root.to_str().unwrap(), &historical_file));
    assert!(cache.contains_on_disk(table_root.to_str().unwrap(), &historical_file));

    fs::remove_dir_all(&table_root).unwrap();
    fs::remove_dir_all(&cache_root).unwrap();
}

#[tokio::test]
async fn snapshot_refresh_invalidates_removed_files_after_compaction() {
    let table_root = unique_test_dir("cache_invalidation");
    let cache_root = unique_test_dir("cache_invalidation_store");

    let base = Utc.with_ymd_and_hms(2026, 4, 11, 0, 0, 0).single().unwrap();
    for idx in 0..6 {
        write_single_row(&table_root, base + Duration::minutes(idx as i64), idx).await;
    }

    let mut cache = HybridTelemetryCache::new(
        cache_root.as_path(),
        HybridCacheConfig {
            promote_after_accesses: 2,
            ..Default::default()
        },
    )
    .unwrap();

    let before = cache.refresh_snapshot(table_root.to_str().unwrap()).await.unwrap();
    let old_file = before.active_files[0].relative_path.clone();

    cache
        .read_file(table_root.to_str().unwrap(), &old_file)
        .await
        .unwrap();
    cache
        .read_file(table_root.to_str().unwrap(), &old_file)
        .await
        .unwrap();
    cache
        .read_file(table_root.to_str().unwrap(), &old_file)
        .await
        .unwrap();

    assert!(cache.contains_on_disk(table_root.to_str().unwrap(), &old_file));
    assert!(cache.contains_in_memory(table_root.to_str().unwrap(), &old_file));

    compact_telemetry_table(table_root.to_str().unwrap(), 1024 * 1024)
        .await
        .unwrap();

    let after = cache.refresh_snapshot(table_root.to_str().unwrap()).await.unwrap();
    let metrics = cache.metrics();

    assert!(after.summary.active_files < before.summary.active_files);
    assert!(!cache.contains_on_disk(table_root.to_str().unwrap(), &old_file));
    assert!(!cache.contains_in_memory(table_root.to_str().unwrap(), &old_file));
    assert!(metrics.invalidations > 0);
    assert!(cache
        .read_file(table_root.to_str().unwrap(), &old_file)
        .await
        .is_err());

    fs::remove_dir_all(&table_root).unwrap();
    fs::remove_dir_all(&cache_root).unwrap();
}
