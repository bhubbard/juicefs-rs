//! Chunk and block storage engine with compression, deduplication, prefetching,
//! rate limiting, and local caching.

pub mod cache;
pub mod error;
pub mod prefetch;
pub mod ratelimit;
pub mod singleflight;
pub mod writeback;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use juicefs_core::{ChunkConfig, CompressionAlgo};
use juicefs_object::{ObjectError, ObjectStorage};
use tokio::task::JoinSet;

pub use cache::{DiskBlockCache, EvictionPolicy, MemoryBlockCache};
pub use error::ChunkError;
pub use prefetch::Prefetcher;
pub use ratelimit::TokenBucket;
pub use singleflight::SingleFlight;
pub use writeback::WritebackQueue;

/// Computes the canonical `JuiceFS` object key for block `index` of `slice_id`.
#[must_use]
pub fn block_key(slice_id: u64, index: usize, block_size: usize, hash_prefix: bool) -> String {
    if hash_prefix {
        format!(
            "chunks/{:02X}/{}/{}_{}_{}",
            slice_id % 256,
            slice_id / 1_000_000,
            slice_id,
            index,
            block_size
        )
    } else {
        format!(
            "chunks/{}/{}/{}_{}_{}",
            slice_id / 1_000_000,
            slice_id / 1_000,
            slice_id,
            index,
            block_size
        )
    }
}

/// Computes the size of block `index` for a slice of total length `slice_len`.
#[must_use]
pub fn block_len(slice_len: usize, index: usize, block_size: usize) -> usize {
    let start = index.saturating_mul(block_size);
    if start >= slice_len {
        0
    } else {
        (slice_len - start).min(block_size)
    }
}

fn parse_block_orig_size(key: &str) -> Option<usize> {
    let (_, tail) = key.rsplit_once('_')?;
    tail.parse::<usize>().ok()
}

/// Chunk store backed by an [`ObjectStorage`], compression, `SingleFlight` deduplication,
/// background [`Prefetcher`], optional [`TokenBucket`] rate limits, and multi-tier caches.
#[derive(Debug, Clone)]
pub struct CachedChunkStore {
    inner: Arc<ChunkStoreInner>,
}

#[derive(Debug)]
struct ChunkStoreInner {
    store: Arc<dyn ObjectStorage>,
    config: ChunkConfig,
    algo: CompressionAlgo,
    mem_cache: MemoryBlockCache,
    disk_cache: Option<DiskBlockCache>,
    single_flight: SingleFlight<Bytes, ChunkError>,
    prefetcher: RwLock<Prefetcher>,
    prefetch_window: AtomicUsize,
    upload_limit: RwLock<Option<TokenBucket>>,
    download_limit: RwLock<Option<TokenBucket>>,
    writeback: WritebackQueue,
}

impl ChunkStoreInner {
    async fn load_by_key(&self, key: &str, blen: usize) -> Result<Bytes, ChunkError> {
        if let Some(hit) = self.mem_cache.get(key) {
            return Ok(hit);
        }
        if let Some(dc) = &self.disk_cache
            && let Some(hit) = dc.get(key).await?
        {
            self.mem_cache.insert(key.to_owned(), hit.clone());
            return Ok(hit);
        }

        let res = self
            .single_flight
            .execute(key, || async {
                if let Some(hit) = self.mem_cache.get(key) {
                    return Ok(hit);
                }

                let down_limiter = self.download_limit.read().expect("lock poisoned").clone();
                let is_compressed = !matches!(self.algo, CompressionAlgo::None);
                if !is_compressed && let Some(limiter) = &down_limiter {
                    limiter.acquire(blen as u64).await;
                }

                let compressed = self.store.get(key, 0, 0).await?;
                if is_compressed && let Some(limiter) = &down_limiter {
                    limiter.acquire(compressed.len() as u64).await;
                }

                let raw = Bytes::from(self.algo.decompress(&compressed, blen)?);
                self.mem_cache.insert(key.to_owned(), raw.clone());
                if let Some(dc) = &self.disk_cache {
                    dc.put(key, &raw).await?;
                }
                Ok(raw)
            })
            .await;

        res.map_err(|arc_err| Arc::try_unwrap(arc_err).unwrap_or_else(ChunkError::Shared))
    }
}

impl CachedChunkStore {
    /// Creates a new [`CachedChunkStore`] wrapping `store`.
    pub fn new(store: Arc<dyn ObjectStorage>, config: ChunkConfig) -> Result<Self, ChunkError> {
        let algo: CompressionAlgo = config.compression.parse()?;
        let mem_cache = MemoryBlockCache::new(config.buffer_size);
        let disk_cache = config
            .cache_dir
            .as_ref()
            .filter(|_| config.cache_size > 0)
            .map(|dir| DiskBlockCache::with_capacity(dir, config.cache_size));

        let cache_enabled = config.buffer_size > 0 || disk_cache.is_some();
        let default_prefetch = usize::from(cache_enabled);

        let inner = Arc::new_cyclic(|weak| {
            let prefetcher = Self::build_prefetcher(weak.clone(), default_prefetch);
            ChunkStoreInner {
                store,
                config,
                algo,
                mem_cache,
                disk_cache,
                single_flight: SingleFlight::new(),
                prefetcher: RwLock::new(prefetcher),
                prefetch_window: AtomicUsize::new(default_prefetch),
                upload_limit: RwLock::new(None),
                download_limit: RwLock::new(None),
                writeback: WritebackQueue::new(16),
            }
        });

        Ok(Self { inner })
    }

    fn build_prefetcher(weak: std::sync::Weak<ChunkStoreInner>, parallel: usize) -> Prefetcher {
        Prefetcher::new(parallel, move |key| {
            let weak = weak.clone();
            async move {
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let Some(blen) = parse_block_orig_size(&key) else {
                    return;
                };
                if blen == 0 || blen > inner.config.block_size {
                    return;
                }
                if inner.mem_cache.contains(&key) {
                    return;
                }
                if let Some(dc) = &inner.disk_cache
                    && dc.contains(&key)
                {
                    return;
                }
                if let Err(err) = inner.load_by_key(&key, blen).await {
                    tracing::debug!(key = %key, %err, "background prefetch failed");
                }
            }
        })
    }

    /// Configures the background prefetch parallelism and lookahead block window.
    ///
    /// Passing `0` disables automatic background prefetching.
    #[must_use]
    pub fn with_prefetch(self, parallel: usize) -> Self {
        let weak = Arc::downgrade(&self.inner);
        let prefetcher = Self::build_prefetcher(weak, parallel);
        *self.inner.prefetcher.write().expect("lock poisoned") = prefetcher;
        self.inner
            .prefetch_window
            .store(parallel, Ordering::Relaxed);
        self
    }

    /// Configures an upload bandwidth limit in bytes per second (`0` disables the limit).
    #[must_use]
    pub fn with_upload_limit(self, bytes_per_sec: u64) -> Self {
        let limiter = (bytes_per_sec > 0).then(|| TokenBucket::from_bytes_per_sec(bytes_per_sec));
        *self.inner.upload_limit.write().expect("lock poisoned") = limiter;
        self
    }

    /// Configures a download bandwidth limit in bytes per second (`0` disables the limit).
    #[must_use]
    pub fn with_download_limit(self, bytes_per_sec: u64) -> Self {
        let limiter = (bytes_per_sec > 0).then(|| TokenBucket::from_bytes_per_sec(bytes_per_sec));
        *self.inner.download_limit.write().expect("lock poisoned") = limiter;
        self
    }

    /// Configures a custom [`TokenBucket`] for upload rate limiting.
    #[must_use]
    pub fn with_upload_limiter(self, limiter: Option<TokenBucket>) -> Self {
        *self.inner.upload_limit.write().expect("lock poisoned") = limiter;
        self
    }

    /// Configures a custom [`TokenBucket`] for download rate limiting.
    #[must_use]
    pub fn with_download_limiter(self, limiter: Option<TokenBucket>) -> Self {
        *self.inner.download_limit.write().expect("lock poisoned") = limiter;
        self
    }

    /// Updates upload and download bandwidth limits (in bytes per second) at runtime.
    ///
    /// Passing `Some(0)` or `None` disables the corresponding limit.
    pub fn update_limits(
        &self,
        upload_bytes_per_sec: Option<u64>,
        download_bytes_per_sec: Option<u64>,
    ) {
        *self.inner.upload_limit.write().expect("lock poisoned") = upload_bytes_per_sec
            .filter(|&bps| bps > 0)
            .map(TokenBucket::from_bytes_per_sec);
        *self.inner.download_limit.write().expect("lock poisoned") = download_bytes_per_sec
            .filter(|&bps| bps > 0)
            .map(TokenBucket::from_bytes_per_sec);
    }

    /// Returns a reference to the underlying [`ObjectStorage`].
    #[must_use]
    pub fn blob_storage(&self) -> &Arc<dyn ObjectStorage> {
        &self.inner.store
    }

    /// Returns a reference to the in-memory block cache.
    #[must_use]
    pub fn memory_cache(&self) -> &MemoryBlockCache {
        &self.inner.mem_cache
    }

    /// Returns a reference to the optional local disk block cache.
    #[must_use]
    pub fn disk_cache(&self) -> Option<&DiskBlockCache> {
        self.inner.disk_cache.as_ref()
    }

    /// Returns the configured block size in bytes.
    #[must_use]
    pub fn block_size(&self) -> usize {
        self.inner.config.block_size
    }

    /// Returns total bytes currently used in the in-memory block cache.
    #[must_use]
    pub fn used_memory(&self) -> usize {
        self.inner.mem_cache.used_bytes()
    }

    /// Schedules a specific block key for asynchronous background prefetching.
    #[must_use]
    pub fn prefetch_key(&self, key: &str) -> bool {
        self.inner
            .prefetcher
            .read()
            .expect("lock poisoned")
            .fetch(key)
    }

    /// Schedules upcoming blocks `[start_idx .. start_idx + count)` of `slice_id` for prefetching.
    pub fn prefetch_blocks(&self, slice_id: u64, slice_len: usize, start_idx: usize, count: usize) {
        let bsize = self.inner.config.block_size;
        for offset in 0..count {
            let idx = start_idx + offset;
            let blen = block_len(slice_len, idx, bsize);
            if blen == 0 {
                break;
            }
            let key = block_key(slice_id, idx, blen, self.inner.config.hash_prefix);
            if !self.inner.mem_cache.contains(&key) {
                self.inner
                    .prefetcher
                    .read()
                    .expect("lock poisoned")
                    .fetch(&key);
            }
        }
    }

    /// Flushes all pending asynchronous write-back uploads to object storage.
    pub async fn flush(&self) -> Result<(), ChunkError> {
        self.inner.writeback.flush().await
    }

    /// Returns the number of pending background write-back upload tasks.
    #[must_use]
    pub fn pending_writebacks(&self) -> usize {
        self.inner.writeback.pending()
    }

    /// Writes a complete slice (`slice_id`) split into `block_size` blocks.
    pub async fn write_slice(&self, slice_id: u64, data: &[u8]) -> Result<(), ChunkError> {
        if data.is_empty() {
            return Ok(());
        }
        let bsize = self.inner.config.block_size;
        if self.inner.config.writeback {
            for (idx, block) in data.chunks(bsize).enumerate() {
                let key = block_key(slice_id, idx, block.len(), self.inner.config.hash_prefix);
                let raw = Bytes::copy_from_slice(block);
                self.inner.mem_cache.insert(key.clone(), raw.clone());
                if let Some(dc) = &self.inner.disk_cache {
                    dc.put(&key, &raw).await?;
                }
                let inner = Arc::clone(&self.inner);
                self.inner.writeback.submit(async move {
                    let compressed = inner.algo.compress(&raw)?;
                    let up_limiter = inner.upload_limit.read().expect("lock poisoned").clone();
                    if let Some(limiter) = up_limiter {
                        limiter.acquire(compressed.len() as u64).await;
                    }
                    inner.store.put(&key, Bytes::from(compressed)).await?;
                    Ok(())
                });
            }
            return Ok(());
        }
        let mut join_set = JoinSet::new();
        for (idx, block) in data.chunks(bsize).enumerate() {
            let key = block_key(slice_id, idx, block.len(), self.inner.config.hash_prefix);
            let inner = Arc::clone(&self.inner);
            let raw = Bytes::copy_from_slice(block);
            join_set.spawn(async move {
                let compressed = inner.algo.compress(&raw)?;
                let up_limiter = inner.upload_limit.read().expect("lock poisoned").clone();
                if let Some(limiter) = up_limiter {
                    limiter.acquire(compressed.len() as u64).await;
                }
                inner.store.put(&key, Bytes::from(compressed)).await?;
                inner.mem_cache.insert(key.clone(), raw.clone());
                if let Some(dc) = &inner.disk_cache {
                    dc.put(&key, &raw).await?;
                }
                Ok::<(), ChunkError>(())
            });
        }
        while let Some(res) = join_set.join_next().await {
            res.map_err(|e| ChunkError::Io(std::io::Error::other(e.to_string())))??;
        }
        Ok(())
    }

    /// Reads `buf.len()` bytes from `slice_id` (total `slice_len`) starting at `offset`.
    ///
    /// Returns the number of bytes actually read.
    pub async fn read_slice(
        &self,
        slice_id: u64,
        slice_len: usize,
        offset: usize,
        buf: &mut [u8],
    ) -> Result<usize, ChunkError> {
        if buf.is_empty() || offset >= slice_len {
            return Ok(0);
        }
        let to_read = buf.len().min(slice_len - offset);
        let bsize = self.inner.config.block_size;
        let window = self.inner.prefetch_window.load(Ordering::Relaxed);
        let mut copied = 0;

        while copied < to_read {
            let cur_off = offset + copied;
            let idx = cur_off / bsize;
            let boff = cur_off % bsize;
            let blen = block_len(slice_len, idx, bsize);
            if blen == 0 || boff >= blen {
                break;
            }
            if window > 0 {
                self.prefetch_blocks(slice_id, slice_len, idx + 1, window);
            }
            let take = (to_read - copied).min(blen - boff);
            let block = self.load_block(slice_id, idx, blen).await?;
            buf[copied..copied + take].copy_from_slice(&block[boff..boff + take]);
            copied += take;
        }
        Ok(copied)
    }

    /// Loads block `idx` of `slice_id` (expected uncompressed size `blen`), checking
    /// memory/disk caches first and deduplicating concurrent object-store fetches via
    /// `SingleFlight`.
    pub async fn load_block(
        &self,
        slice_id: u64,
        idx: usize,
        blen: usize,
    ) -> Result<Bytes, ChunkError> {
        let key = block_key(slice_id, idx, blen, self.inner.config.hash_prefix);
        self.inner.load_by_key(&key, blen).await
    }

    /// Removes all blocks belonging to `slice_id` from object storage and local caches.
    pub async fn remove_slice(&self, slice_id: u64, slice_len: usize) -> Result<(), ChunkError> {
        if slice_len == 0 {
            return Ok(());
        }
        let bsize = self.inner.config.block_size;
        let count = slice_len.div_ceil(bsize);
        for idx in 0..count {
            let blen = block_len(slice_len, idx, bsize);
            let key = block_key(slice_id, idx, blen, self.inner.config.hash_prefix);
            self.inner.mem_cache.remove(&key);
            if let Some(dc) = &self.inner.disk_cache {
                dc.remove(&key).await?;
            }
            match self.inner.store.delete(&key).await {
                Ok(()) | Err(ObjectError::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use juicefs_object::MemoryStore;

    use super::*;

    #[tokio::test]
    async fn write_and_read_multi_block_slice() {
        let store = Arc::new(MemoryStore::new());
        let cfg = ChunkConfig {
            block_size: 1024,
            compression: "zstd".to_owned(),
            ..Default::default()
        };
        let chunk_store = CachedChunkStore::new(store, cfg).expect("chunk store");
        let payload: Vec<u8> = (0..2500).map(|i| (i % 251) as u8).collect();

        chunk_store.write_slice(1, &payload).await.expect("write");

        let mut out = vec![0u8; 1500];
        let n = chunk_store
            .read_slice(1, payload.len(), 500, &mut out)
            .await
            .expect("read");
        assert_eq!(n, 1500);
        assert_eq!(&out[..], &payload[500..2000]);

        chunk_store
            .remove_slice(1, payload.len())
            .await
            .expect("remove");
    }
}
