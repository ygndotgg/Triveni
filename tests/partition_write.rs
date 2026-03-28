use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{TimeZone, Utc};
use mqtt_to_delta::{
    batcher::MessageBatcher,
    pipeline::{generate_messages_for_next_days, telemetry_schema, write_partitioned_batch},
};

fn unique_test_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("mqtt_to_delta_partition_test_{nanos}"))
}

#[test]
fn writes_unique_parquet_files_across_seven_days() {
    let schema = telemetry_schema();
    let table_root = unique_test_dir();
    let mut batcher = MessageBatcher::default();
    let mut batch_id = 0usize;
    let mut written_paths = Vec::new();

    let start = Utc.with_ymd_and_hms(2026, 3, 28, 0, 0, 0).single().unwrap();
    let messages = generate_messages_for_next_days(start, 7, 100).unwrap();

    for message in messages {
        if let Some(batch) = batcher.push(message) {
            let paths =
                write_partitioned_batch(schema.clone(), batch_id, batch, &table_root).unwrap();
            written_paths.extend(paths);
            batch_id += 1;
        }
    }

    let mut files_by_partition: BTreeMap<String, usize> = BTreeMap::new();
    for path in &written_paths {
        let partition = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|name| name.to_str())
            .unwrap()
            .to_string();
        *files_by_partition.entry(partition).or_default() += 1;
        assert!(path.exists(), "expected parquet file to exist: {}", path.display());
    }

    assert_eq!(written_paths.len(), 7, "expected one unique file per flush");
    assert_eq!(files_by_partition.len(), 7, "expected seven date partitions");

    for day in 28..=31 {
        let key = format!("event_date=2026-03-{day:02}");
        assert_eq!(files_by_partition.get(&key), Some(&1));
    }
    for day in 1..=3 {
        let key = format!("event_date=2026-04-{day:02}");
        assert_eq!(files_by_partition.get(&key), Some(&1));
    }

    fs::remove_dir_all(&table_root).unwrap();
}
