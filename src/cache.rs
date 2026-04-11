use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;

use crate::persistence::{DeltaTableSummary, LocalDeltaSnapshot, load_local_delta_snapshot};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HybridCacheConfig {
    pub memory_capacity_bytes: usize,
    pub disk_capacity_bytes: usize,
    pub memory_entry_max_bytes: usize,
    pub disk_entry_max_bytes: usize,
    pub recent_partition_days: i64,
    pub promote_after_accesses: usize,
}

impl Default for HybridCacheConfig {
    fn default() -> Self {
        Self {
            memory_capacity_bytes: 8 * 1024 * 1024,
            disk_capacity_bytes: 64 * 1024 * 1024,
            memory_entry_max_bytes: 512 * 1024,
            disk_entry_max_bytes: 8 * 1024 * 1024,
            recent_partition_days: 2,
            promote_after_accesses: 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheTier {
    Memory,
    Disk,
    Source,
    Bypassed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheValueClass {
    Metadata,
    RecentData,
    HistoricalData,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheMetrics {
    pub metadata_hits: usize,
    pub metadata_misses: usize,
    pub memory_hits: usize,
    pub disk_hits: usize,
    pub source_reads: usize,
    pub bypass_reads: usize,
    pub invalidations: usize,
    pub memory_entries: usize,
    pub disk_entries: usize,
    pub memory_usage_bytes: usize,
    pub disk_usage_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotFileInfo {
    pub relative_path: String,
    pub size_bytes: u64,
    pub event_date: Option<String>,
    pub class: CacheValueClass,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedSnapshotView {
    pub summary: DeltaTableSummary,
    pub active_files: Vec<SnapshotFileInfo>,
    pub metadata_tier: CacheTier,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedFileRead {
    pub relative_path: String,
    pub size_bytes: usize,
    pub tier: CacheTier,
    pub class: CacheValueClass,
}

#[derive(Clone)]
struct SnapshotCacheEntry {
    snapshot: LocalDeltaSnapshot,
    classes: HashMap<String, CacheValueClass>,
}

#[derive(Clone)]
struct MemoryEntry {
    bytes: Vec<u8>,
    size_bytes: usize,
    last_access: u64,
    class: CacheValueClass,
}

#[derive(Clone)]
struct DiskEntry {
    path: PathBuf,
    size_bytes: usize,
    last_access: u64,
    class: CacheValueClass,
}

pub struct HybridTelemetryCache {
    config: HybridCacheConfig,
    cache_root: PathBuf,
    metadata: HashMap<String, SnapshotCacheEntry>,
    memory: HashMap<String, MemoryEntry>,
    memory_lru: VecDeque<String>,
    disk: HashMap<String, DiskEntry>,
    disk_lru: VecDeque<String>,
    observation_counts: HashMap<String, usize>,
    metrics: CacheMetrics,
    tick: u64,
}

impl HybridTelemetryCache {
    pub fn new(cache_root: impl AsRef<Path>, config: HybridCacheConfig) -> Result<Self, Box<dyn Error>> {
        fs::create_dir_all(cache_root.as_ref())?;
        Ok(Self {
            config,
            cache_root: cache_root.as_ref().to_path_buf(),
            metadata: HashMap::new(),
            memory: HashMap::new(),
            memory_lru: VecDeque::new(),
            disk: HashMap::new(),
            disk_lru: VecDeque::new(),
            observation_counts: HashMap::new(),
            metrics: CacheMetrics::default(),
            tick: 0,
        })
    }

    pub async fn load_snapshot(
        &mut self,
        table_path: &str,
    ) -> Result<CachedSnapshotView, Box<dyn Error>> {
        if let Some(entry) = self.metadata.get(table_path).cloned() {
            self.metrics.metadata_hits += 1;
            return Ok(snapshot_view_from_entry(&entry, CacheTier::Memory));
        }

        self.metrics.metadata_misses += 1;
        self.load_snapshot_from_source(table_path).await
    }

    pub async fn refresh_snapshot(
        &mut self,
        table_path: &str,
    ) -> Result<CachedSnapshotView, Box<dyn Error>> {
        self.load_snapshot_from_source(table_path).await
    }

    async fn load_snapshot_from_source(
        &mut self,
        table_path: &str,
    ) -> Result<CachedSnapshotView, Box<dyn Error>> {
        let snapshot = load_local_delta_snapshot(table_path).await?;
        let classes = classify_files(&snapshot, self.config.recent_partition_days);
        self.invalidate_stale_entries(table_path, &snapshot);
        self.metadata.insert(
            table_path.to_string(),
            SnapshotCacheEntry {
                snapshot: snapshot.clone(),
                classes: classes.clone(),
            },
        );

        Ok(snapshot_view_from_snapshot(snapshot, &classes, CacheTier::Source))
    }

    pub async fn read_file(
        &mut self,
        table_path: &str,
        relative_path: &str,
    ) -> Result<CachedFileRead, Box<dyn Error>> {
        let snapshot = self.load_snapshot(table_path).await?;
        let file = self
            .metadata
            .get(table_path)
            .and_then(|entry| {
                entry
                    .snapshot
                    .active_files
                    .iter()
                    .find(|file| file.relative_path == relative_path)
                    .cloned()
            })
            .ok_or_else(|| format!("file `{relative_path}` is not active in the latest snapshot"))?;

        let class = snapshot
            .active_files
            .iter()
            .find(|entry| entry.relative_path == relative_path)
            .map(|entry| entry.class)
            .unwrap_or(CacheValueClass::HistoricalData);

        let cache_key = cache_key(table_path, relative_path);
        let size_bytes = file.size_bytes as usize;
        self.bump_observation(&cache_key);

        let access_tick = self.next_tick();
        if let Some(entry) = self.memory.get_mut(&cache_key) {
            self.metrics.memory_hits += 1;
            entry.last_access = access_tick;
            touch_key(&mut self.memory_lru, &cache_key);
            return Ok(CachedFileRead {
                relative_path: relative_path.to_string(),
                size_bytes: entry.bytes.len(),
                tier: CacheTier::Memory,
                class: entry.class,
            });
        }

        let access_tick = self.next_tick();
        if let Some(entry) = self.disk.get_mut(&cache_key) {
            self.metrics.disk_hits += 1;
            entry.last_access = access_tick;
            touch_key(&mut self.disk_lru, &cache_key);
        }

        if self.disk.contains_key(&cache_key) {
            let (disk_path, disk_size, disk_class) = {
                let entry = self.disk.get(&cache_key).expect("disk entry disappeared");
                (entry.path.clone(), entry.size_bytes, entry.class)
            };
            let bytes = fs::read(&disk_path)?;
            if self.should_promote_to_memory(class, size_bytes, &cache_key) {
                self.insert_memory(cache_key.clone(), bytes, class)?;
            }

            return Ok(CachedFileRead {
                relative_path: relative_path.to_string(),
                size_bytes: disk_size,
                tier: CacheTier::Disk,
                class: disk_class,
            });
        }

        if !self.should_admit_to_disk(class, size_bytes) && !self.should_admit_to_memory(class, size_bytes)
        {
            self.metrics.bypass_reads += 1;
            let size_bytes = fs::read(&file.absolute_path)?.len();
            return Ok(CachedFileRead {
                relative_path: relative_path.to_string(),
                size_bytes,
                tier: CacheTier::Bypassed,
                class,
            });
        }

        self.metrics.source_reads += 1;
        let bytes = fs::read(&file.absolute_path)?;

        if self.should_admit_to_disk(class, bytes.len()) {
            self.insert_disk(cache_key.clone(), &bytes, class)?;
        }

        if self.should_promote_to_memory(class, bytes.len(), &cache_key) {
            self.insert_memory(cache_key, bytes, class)?;
        }

        Ok(CachedFileRead {
            relative_path: relative_path.to_string(),
            size_bytes,
            tier: CacheTier::Source,
            class,
        })
    }

    pub fn metrics(&self) -> CacheMetrics {
        let mut metrics = self.metrics.clone();
        metrics.memory_entries = self.memory.len();
        metrics.disk_entries = self.disk.len();
        metrics.memory_usage_bytes = self.memory.values().map(|entry| entry.size_bytes).sum();
        metrics.disk_usage_bytes = self.disk.values().map(|entry| entry.size_bytes).sum();
        metrics
    }

    pub fn contains_in_memory(&self, table_path: &str, relative_path: &str) -> bool {
        self.memory.contains_key(&cache_key(table_path, relative_path))
    }

    pub fn contains_on_disk(&self, table_path: &str, relative_path: &str) -> bool {
        self.disk.contains_key(&cache_key(table_path, relative_path))
    }

    fn invalidate_stale_entries(
        &mut self,
        table_path: &str,
        snapshot: &LocalDeltaSnapshot,
    ) {
        let active_keys: HashSet<String> = snapshot
            .active_files
            .iter()
            .map(|file| cache_key(table_path, &file.relative_path))
            .collect();

        let stale_memory: Vec<String> = self
            .memory
            .keys()
            .filter(|key| key.starts_with(table_path) && !active_keys.contains(*key))
            .cloned()
            .collect();
        let stale_disk: Vec<String> = self
            .disk
            .keys()
            .filter(|key| key.starts_with(table_path) && !active_keys.contains(*key))
            .cloned()
            .collect();

        for key in stale_memory {
            if self.memory.remove(&key).is_some() {
                remove_key(&mut self.memory_lru, &key);
                self.metrics.invalidations += 1;
            }
        }

        for key in stale_disk {
            if let Some(entry) = self.disk.remove(&key) {
                let _ = fs::remove_file(entry.path);
                remove_key(&mut self.disk_lru, &key);
                self.metrics.invalidations += 1;
            }
        }
    }

    fn should_admit_to_memory(&self, class: CacheValueClass, size_bytes: usize) -> bool {
        matches!(class, CacheValueClass::RecentData) && size_bytes <= self.config.memory_entry_max_bytes
    }

    fn should_promote_to_memory(
        &self,
        class: CacheValueClass,
        size_bytes: usize,
        cache_key: &str,
    ) -> bool {
        self.should_admit_to_memory(class, size_bytes)
            && self
                .observation_counts
                .get(cache_key)
                .copied()
                .unwrap_or_default()
                >= self.config.promote_after_accesses
    }

    fn should_admit_to_disk(&self, class: CacheValueClass, size_bytes: usize) -> bool {
        matches!(class, CacheValueClass::RecentData | CacheValueClass::HistoricalData)
            && size_bytes <= self.config.disk_entry_max_bytes
    }

    fn insert_memory(
        &mut self,
        cache_key: String,
        bytes: Vec<u8>,
        class: CacheValueClass,
    ) -> Result<(), Box<dyn Error>> {
        let size_bytes = bytes.len();
        if size_bytes > self.config.memory_entry_max_bytes {
            return Ok(());
        }

        while current_memory_usage(&self.memory) + size_bytes > self.config.memory_capacity_bytes {
            let Some(oldest) = self.memory_lru.pop_front() else {
                break;
            };
            self.memory.remove(&oldest);
        }

        let entry = MemoryEntry {
            bytes,
            size_bytes,
            last_access: self.next_tick(),
            class,
        };
        touch_key(&mut self.memory_lru, &cache_key);
        self.memory.insert(cache_key, entry);
        Ok(())
    }

    fn insert_disk(
        &mut self,
        cache_key: String,
        bytes: &[u8],
        class: CacheValueClass,
    ) -> Result<(), Box<dyn Error>> {
        let size_bytes = bytes.len();
        if size_bytes > self.config.disk_entry_max_bytes {
            return Ok(());
        }

        while current_disk_usage(&self.disk) + size_bytes > self.config.disk_capacity_bytes {
            let Some(oldest) = self.disk_lru.pop_front() else {
                break;
            };
            if let Some(entry) = self.disk.remove(&oldest) {
                let _ = fs::remove_file(entry.path);
            }
        }

        let path = self.cache_root.join(disk_filename(&cache_key));
        fs::write(&path, bytes)?;
        let entry = DiskEntry {
            path,
            size_bytes,
            last_access: self.next_tick(),
            class,
        };
        touch_key(&mut self.disk_lru, &cache_key);
        self.disk.insert(cache_key, entry);
        Ok(())
    }

    fn bump_observation(&mut self, cache_key: &str) {
        *self
            .observation_counts
            .entry(cache_key.to_string())
            .or_default() += 1;
    }

    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }
}

fn classify_files(
    snapshot: &LocalDeltaSnapshot,
    recent_partition_days: i64,
) -> HashMap<String, CacheValueClass> {
    let newest_partition = snapshot
        .active_files
        .iter()
        .filter_map(|file| file.event_date.as_deref().and_then(parse_event_date))
        .max();

    snapshot
        .active_files
        .iter()
        .map(|file| {
            let class = newest_partition
                .and_then(|newest| {
                    file.event_date
                        .as_deref()
                        .and_then(parse_event_date)
                        .map(|date| (newest - date).num_days())
                })
                .filter(|days| *days <= recent_partition_days)
                .map(|_| CacheValueClass::RecentData)
                .unwrap_or(CacheValueClass::HistoricalData);
            (file.relative_path.clone(), class)
        })
        .collect()
}

fn snapshot_view_from_entry(entry: &SnapshotCacheEntry, metadata_tier: CacheTier) -> CachedSnapshotView {
    snapshot_view_from_snapshot(entry.snapshot.clone(), &entry.classes, metadata_tier)
}

fn snapshot_view_from_snapshot(
    snapshot: LocalDeltaSnapshot,
    classes: &HashMap<String, CacheValueClass>,
    metadata_tier: CacheTier,
) -> CachedSnapshotView {
    let active_files = snapshot
        .active_files
        .iter()
        .map(|file| SnapshotFileInfo {
            relative_path: file.relative_path.clone(),
            size_bytes: file.size_bytes,
            event_date: file.event_date.clone(),
            class: *classes
                .get(&file.relative_path)
                .unwrap_or(&CacheValueClass::HistoricalData),
        })
        .collect();

    CachedSnapshotView {
        summary: snapshot.summary,
        active_files,
        metadata_tier,
    }
}

fn cache_key(table_path: &str, relative_path: &str) -> String {
    format!("{table_path}::{relative_path}")
}

fn disk_filename(cache_key: &str) -> String {
    let mut sanitized = String::with_capacity(cache_key.len());
    for ch in cache_key.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    format!("{sanitized}.cache")
}

fn parse_event_date(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

fn current_memory_usage(memory: &HashMap<String, MemoryEntry>) -> usize {
    memory.values().map(|entry| entry.size_bytes).sum()
}

fn current_disk_usage(disk: &HashMap<String, DiskEntry>) -> usize {
    disk.values().map(|entry| entry.size_bytes).sum()
}

fn touch_key(lru: &mut VecDeque<String>, key: &str) {
    remove_key(lru, key);
    lru.push_back(key.to_string());
}

fn remove_key(lru: &mut VecDeque<String>, key: &str) {
    if let Some(index) = lru.iter().position(|candidate| candidate == key) {
        lru.remove(index);
    }
}
