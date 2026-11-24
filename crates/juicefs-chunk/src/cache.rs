//! In-memory and local disk block caches for `juicefs-chunk`.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::fs;

use crate::ChunkError;

/// Default maximum disk cache size (100 MiB, matching `ChunkConfig::default()`).
const DEFAULT_DISK_CACHE_BYTES: u64 = 100 << 20;

/// Eviction policy for [`DiskBlockCache`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// No automatic eviction; blocks remain until explicitly removed.
    None,
    /// Evict the older of two randomly sampled blocks (`JuiceFS` default).
    #[default]
    TwoRandom,
    /// Evict the least recently accessed block.
    Lru,
}

/// Simple FIFO-bounded in-memory block cache.
#[derive(Debug)]
pub struct MemoryBlockCache {
    max_bytes: usize,
    inner: Mutex<MemCacheInner>,
}

#[derive(Debug, Default)]
struct MemCacheInner {
    used_bytes: usize,
    entries: HashMap<String, Bytes>,
    order: VecDeque<String>,
}

impl MemoryBlockCache {
    /// Creates a new in-memory cache with capacity `max_bytes`.
    #[must_use]
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(MemCacheInner::default()),
        }
    }

    /// Returns `true` if `key` is currently cached in memory.
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.inner.lock().entries.contains_key(key)
    }

    /// Returns the number of blocks currently held in the memory cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Returns `true` if the memory cache holds no blocks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().entries.is_empty()
    }

    /// Retrieves a cached uncompressed block if present.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.inner.lock().entries.get(key).cloned()
    }

    /// Inserts an uncompressed block into the cache, evicting oldest entries if needed.
    pub fn insert(&self, key: String, data: Bytes) {
        let data_len = data.len();
        if self.max_bytes == 0 || data_len > self.max_bytes {
            return;
        }
        let mut guard = self.inner.lock();
        if let Some(prev) = guard.entries.insert(key.clone(), data) {
            guard.used_bytes = guard.used_bytes.saturating_sub(prev.len());
        } else {
            guard.order.push_back(key);
        }
        guard.used_bytes += data_len;

        while guard.used_bytes > self.max_bytes {
            let Some(oldest) = guard.order.pop_front() else {
                break;
            };
            if let Some(removed) = guard.entries.remove(&oldest) {
                guard.used_bytes = guard.used_bytes.saturating_sub(removed.len());
            }
        }
    }

    /// Removes a block from the in-memory cache.
    pub fn remove(&self, key: &str) {
        let mut guard = self.inner.lock();
        if let Some(removed) = guard.entries.remove(key) {
            guard.used_bytes = guard.used_bytes.saturating_sub(removed.len());
        }
    }

    /// Returns total bytes currently held in the memory cache.
    #[must_use]
    pub fn used_bytes(&self) -> usize {
        self.inner.lock().used_bytes
    }
}

#[derive(Debug)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn from_entropy() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0x9E37_79B9_7F4A_7C15, |d| d.as_nanos() as u64);
        let seed = nanos ^ 0x517C_C1B7_2722_0A95;
        Self {
            state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn gen_index(&mut self, len: usize) -> usize {
        (self.next_u64() as usize) % len
    }
}

#[derive(Debug, Clone, Copy)]
struct CacheEntry {
    size: u64,
    seq: u64,
    vec_idx: usize,
}

#[derive(Debug)]
struct DiskCacheState {
    used_bytes: u64,
    seq: u64,
    entries: HashMap<String, CacheEntry>,
    lru_order: BTreeMap<u64, String>,
    keys_vec: Vec<String>,
    rng: XorShift64,
}

impl DiskCacheState {
    fn new() -> Self {
        Self {
            used_bytes: 0,
            seq: 0,
            entries: HashMap::new(),
            lru_order: BTreeMap::new(),
            keys_vec: Vec::new(),
            rng: XorShift64::from_entropy(),
        }
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn insert(&mut self, key: String, size: u64) {
        let new_seq = self.next_seq();
        if let Some(existing) = self.entries.get_mut(&key) {
            let old_seq = existing.seq;
            let old_size = existing.size;
            self.lru_order.remove(&old_seq);
            self.used_bytes = self.used_bytes.saturating_sub(old_size);
            existing.size = size;
            existing.seq = new_seq;
            self.lru_order.insert(new_seq, key);
            self.used_bytes += size;
        } else {
            let vec_idx = self.keys_vec.len();
            self.keys_vec.push(key.clone());
            self.entries.insert(
                key.clone(),
                CacheEntry {
                    size,
                    seq: new_seq,
                    vec_idx,
                },
            );
            self.lru_order.insert(new_seq, key);
            self.used_bytes += size;
        }
    }

    fn touch(&mut self, key: &str) {
        let Some(old_seq) = self.entries.get(key).map(|e| e.seq) else {
            return;
        };
        self.lru_order.remove(&old_seq);
        let new_seq = self.next_seq();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.seq = new_seq;
        }
        self.lru_order.insert(new_seq, key.to_owned());
    }

    fn remove(&mut self, key: &str) -> Option<u64> {
        let entry = self.entries.remove(key)?;
        self.lru_order.remove(&entry.seq);
        self.used_bytes = self.used_bytes.saturating_sub(entry.size);

        let last_idx = self.keys_vec.len() - 1;
        self.keys_vec.swap_remove(entry.vec_idx);
        if entry.vec_idx < last_idx {
            let swapped_key = &self.keys_vec[entry.vec_idx];
            if let Some(swapped_entry) = self.entries.get_mut(swapped_key) {
                swapped_entry.vec_idx = entry.vec_idx;
            }
        }
        Some(entry.size)
    }

    fn sample_candidate(&mut self, protect_key: Option<&str>) -> Option<(String, u64)> {
        let len = self.keys_vec.len();
        if len == 0 {
            return None;
        }
        let start = self.rng.gen_index(len);
        for offset in 0..len {
            let idx = (start + offset) % len;
            let key = &self.keys_vec[idx];
            if protect_key == Some(key.as_str()) {
                continue;
            }
            if let Some(entry) = self.entries.get(key) {
                return Some((key.clone(), entry.seq));
            }
        }
        None
    }

    fn evict_one(&mut self, policy: EvictionPolicy, protect_key: Option<&str>) -> Option<String> {
        let victim = match policy {
            EvictionPolicy::None => return None,
            EvictionPolicy::Lru => self
                .lru_order
                .values()
                .find(|k| protect_key != Some(k.as_str()))
                .cloned(),
            EvictionPolicy::TwoRandom => {
                let first = self.sample_candidate(protect_key)?;
                let second = self.sample_candidate(protect_key)?;
                if second.1 < first.1 {
                    Some(second.0)
                } else {
                    Some(first.0)
                }
            }
        }?;
        self.remove(&victim);
        Some(victim)
    }
}

#[derive(Debug)]
struct DiskCacheInner {
    root: PathBuf,
    max_bytes: u64,
    policy: EvictionPolicy,
    state: Mutex<DiskCacheState>,
    tmp_seq: AtomicU64,
}

/// Local disk cache storing uncompressed blocks under `<root>/raw/<key>` with capacity
/// tracking and configurable eviction (`2-random` or `LRU`).
#[derive(Debug, Clone)]
pub struct DiskBlockCache {
    inner: Arc<DiskCacheInner>,
}

impl DiskBlockCache {
    /// Creates a disk cache rooted at `root` with the default 100 MiB capacity and
    /// [`EvictionPolicy::TwoRandom`].
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self::with_eviction(root, DEFAULT_DISK_CACHE_BYTES, EvictionPolicy::TwoRandom)
    }

    /// Creates a disk cache rooted at `root` bounded by `max_bytes` using
    /// [`EvictionPolicy::TwoRandom`].
    #[must_use]
    pub fn with_capacity(root: impl AsRef<Path>, max_bytes: u64) -> Self {
        Self::with_eviction(root, max_bytes, EvictionPolicy::TwoRandom)
    }

    /// Creates a disk cache rooted at `root` bounded by `max_bytes` using `policy`.
    #[must_use]
    pub fn with_eviction(root: impl AsRef<Path>, max_bytes: u64, policy: EvictionPolicy) -> Self {
        let root_path = root.as_ref().to_path_buf();
        let mut state = DiskCacheState::new();
        let raw_root = root_path.join("raw");
        if raw_root.exists() {
            scan_dir_sync(&raw_root, &raw_root, &mut state);
        }

        Self {
            inner: Arc::new(DiskCacheInner {
                root: root_path,
                max_bytes,
                policy,
                state: Mutex::new(state),
                tmp_seq: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the configured eviction policy.
    #[must_use]
    pub fn policy(&self) -> EvictionPolicy {
        self.inner.policy
    }

    /// Returns the maximum capacity in bytes.
    #[must_use]
    pub fn max_bytes(&self) -> u64 {
        self.inner.max_bytes
    }

    /// Returns total cached bytes currently tracked on disk.
    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.inner.state.lock().used_bytes
    }

    /// Returns the number of cached block files currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.state.lock().entries.len()
    }

    /// Returns `true` if no blocks are currently tracked in the disk cache.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.state.lock().entries.is_empty()
    }

    /// Returns `true` if `key` is currently tracked in the disk cache index.
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.inner.state.lock().entries.contains_key(key)
    }

    fn block_path(&self, key: &str) -> PathBuf {
        self.inner.root.join("raw").join(key)
    }

    /// Attempts to read a cached block from local disk, updating access recency on hit.
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>, ChunkError> {
        let path = self.block_path(key);
        match fs::read(&path).await {
            Ok(bytes) => {
                let len = bytes.len() as u64;
                let evicted = {
                    let mut state = self.inner.state.lock();
                    if state.entries.contains_key(key) {
                        state.touch(key);
                        Vec::new()
                    } else if self.inner.max_bytes > 0 && len <= self.inner.max_bytes {
                        state.insert(key.to_owned(), len);
                        self.evict_excess_locked(&mut state, Some(key))
                    } else {
                        Vec::new()
                    }
                };
                self.delete_evicted_files(evicted).await?;
                Ok(Some(Bytes::from(bytes)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.inner.state.lock().remove(key);
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Persists an uncompressed block to local disk atomically, evicting older entries
    /// when `used_bytes` exceeds `max_bytes`.
    pub async fn put(&self, key: &str, data: &[u8]) -> Result<(), ChunkError> {
        let data_len = data.len() as u64;
        if self.inner.max_bytes == 0 || data_len > self.inner.max_bytes {
            return Ok(());
        }

        let path = self.block_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let seq = self.inner.tmp_seq.fetch_add(1, Ordering::Relaxed);
        let tmp = path.with_extension(format!("tmp.{}.{seq}", std::process::id()));
        fs::write(&tmp, data).await?;
        fs::rename(&tmp, &path).await?;

        let evicted = {
            let mut state = self.inner.state.lock();
            state.insert(key.to_owned(), data_len);
            self.evict_excess_locked(&mut state, Some(key))
        };
        self.delete_evicted_files(evicted).await?;
        Ok(())
    }

    /// Removes a cached block from disk and capacity tracking if it exists.
    pub async fn remove(&self, key: &str) -> Result<(), ChunkError> {
        self.inner.state.lock().remove(key);
        let path = self.block_path(key);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn evict_excess_locked(
        &self,
        state: &mut DiskCacheState,
        protect_key: Option<&str>,
    ) -> Vec<PathBuf> {
        let mut to_delete = Vec::new();
        while state.used_bytes > self.inner.max_bytes {
            let Some(victim) = state.evict_one(self.inner.policy, protect_key) else {
                break;
            };
            to_delete.push(self.block_path(&victim));
        }
        to_delete
    }

    async fn delete_evicted_files(&self, paths: Vec<PathBuf>) -> Result<(), ChunkError> {
        for path in paths {
            match fs::remove_file(&path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

fn scan_dir_sync(raw_root: &Path, dir: &Path, state: &mut DiskCacheState) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if ft.is_dir() {
            scan_dir_sync(raw_root, &path, state);
        } else if ft.is_file() {
            let name = entry.file_name();
            if name.to_string_lossy().contains(".tmp.") {
                continue;
            }
            let Ok(rel) = path.strip_prefix(raw_root) else {
                continue;
            };
            let key = rel
                .iter()
                .map(|c| c.to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if let Ok(meta) = entry.metadata() {
                state.insert(key, meta.len());
            }
        }
    }
}
