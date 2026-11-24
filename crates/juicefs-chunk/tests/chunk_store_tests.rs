//! Integration tests for `juicefs-chunk`: `CachedChunkStore`, `SingleFlight`,
//! `Prefetcher`, `DiskBlockCache` capacity eviction, and `TokenBucket` rate limiting.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use juicefs_chunk::{
    CachedChunkStore, ChunkError, DiskBlockCache, EvictionPolicy, Prefetcher, SingleFlight,
    TokenBucket, block_key, block_len,
};
use juicefs_core::ChunkConfig;
use juicefs_object::{MemoryStore, ObjectError, ObjectMeta, ObjectStorage};
use parking_lot::Mutex;
use tempfile::tempdir;
use tokio::sync::Barrier;
use tokio::time::Instant;

#[derive(Debug)]
struct CountingStore {
    inner: MemoryStore,
    get_calls: AtomicUsize,
    put_calls: AtomicUsize,
    key_gets: Mutex<HashMap<String, usize>>,
    get_delay: Duration,
}

impl CountingStore {
    fn new(get_delay: Duration) -> Self {
        Self {
            inner: MemoryStore::new(),
            get_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            key_gets: Mutex::new(HashMap::new()),
            get_delay,
        }
    }

    fn total_gets(&self) -> usize {
        self.get_calls.load(Ordering::SeqCst)
    }

    fn gets_for_key(&self, key: &str) -> usize {
        self.key_gets.lock().get(key).copied().unwrap_or(0)
    }
}

impl ObjectStorage for CountingStore {
    fn description(&self) -> String {
        "counting-memory".to_owned()
    }

    fn create<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.create().await })
    }

    fn get<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
        offset: u64,
        limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, ObjectError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            *self.key_gets.lock().entry(key.to_owned()).or_insert(0) += 1;
            if !self.get_delay.is_zero() {
                tokio::time::sleep(self.get_delay).await;
            }
            self.inner.get(key, offset, limit).await
        })
    }

    fn put<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.put(key, data).await
        })
    }

    fn head<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = Result<ObjectMeta, ObjectError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.head(key).await })
    }

    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.delete(key).await })
    }

    fn copy<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        dst: &'life1 str,
        src: &'life2 str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.copy(dst, src).await })
    }

    fn list<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, ObjectError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.list(prefix).await })
    }
}

#[test]
fn test_block_indexes_and_keys() {
    assert_eq!(block_key(42, 0, 1024, false), "chunks/0/0/42_0_1024");
    assert_eq!(
        block_key(1_234_567, 3, 4096, false),
        "chunks/1/1234/1234567_3_4096"
    );
    assert_eq!(block_key(257, 1, 512, true), "chunks/01/0/257_1_512");
    assert_eq!(block_len(2500, 0, 1024), 1024);
    assert_eq!(block_len(2500, 2, 1024), 452);
    assert_eq!(block_len(2500, 3, 1024), 0);
}

#[tokio::test]
async fn test_store_compressed_and_small_buffer_eviction() {
    for algo in ["none", "lz4", "zstd"] {
        let obj = Arc::new(MemoryStore::new());
        let cfg = ChunkConfig {
            block_size: 256,
            compression: algo.to_owned(),
            buffer_size: 512, // smaller than total written data -> forces eviction
            ..Default::default()
        };
        let store = CachedChunkStore::new(obj, cfg).expect("chunk store");
        let data: Vec<u8> = (0..1024).map(|i| (i * 7 % 256) as u8).collect();
        store.write_slice(1, &data).await.expect("write_slice");

        assert!(store.used_memory() <= 512, "memory cache exceeded limit");

        let mut buf = vec![0u8; 1024];
        let n = store
            .read_slice(1, data.len(), 0, &mut buf)
            .await
            .expect("read_slice");
        assert_eq!(n, 1024);
        assert_eq!(buf, data);
    }
}

#[tokio::test]
async fn test_disk_cache_survives_memory_miss() {
    let cache_dir = tempdir().expect("tempdir");
    let obj = Arc::new(MemoryStore::new());
    let cfg = ChunkConfig {
        block_size: 512,
        compression: "zstd".to_owned(),
        cache_dir: Some(cache_dir.path().to_path_buf()),
        buffer_size: 0, // disable memory cache so disk cache is exercised
        ..Default::default()
    };
    let store = CachedChunkStore::new(Arc::clone(&obj) as Arc<_>, cfg).expect("chunk store");
    let payload = b"disk-cache-persistence-test-".repeat(40);
    store.write_slice(7, &payload).await.expect("write");

    // Delete from underlying object store; read should still succeed from disk cache
    let blen0 = block_len(payload.len(), 0, 512);
    obj.delete(&block_key(7, 0, blen0, false))
        .await
        .expect("delete obj");

    let mut out = vec![0u8; 200];
    let n = store
        .read_slice(7, payload.len(), 0, &mut out)
        .await
        .expect("read from disk cache");
    assert_eq!(n, 200);
    assert_eq!(&out[..], &payload[..200]);

    store
        .remove_slice(7, payload.len())
        .await
        .expect("remove_slice");
}

#[tokio::test]
async fn test_singleflight_deduplicates_concurrent_load_block() {
    let obj = Arc::new(CountingStore::new(Duration::from_millis(30)));
    let writer_cfg = ChunkConfig {
        block_size: 512,
        compression: "zstd".to_owned(),
        buffer_size: 0,
        cache_size: 0,
        ..Default::default()
    };
    let writer =
        CachedChunkStore::new(Arc::clone(&obj) as Arc<_>, writer_cfg).expect("writer store");
    let payload: Vec<u8> = (0..512).map(|i| (i % 199) as u8).collect();
    writer.write_slice(99, &payload).await.expect("write");

    // Fresh reader store with cold caches so concurrent load_block calls hit object storage.
    let reader_cfg = ChunkConfig {
        block_size: 512,
        compression: "zstd".to_owned(),
        buffer_size: 1024 * 64,
        ..Default::default()
    };
    let reader = CachedChunkStore::new(Arc::clone(&obj) as Arc<_>, reader_cfg)
        .expect("reader store")
        .with_prefetch(0);

    let concurrency = 24;
    let barrier = Arc::new(Barrier::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let store = reader.clone();
        let bar = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            store.load_block(99, 0, 512).await
        }));
    }

    for handle in handles {
        let block = handle.await.expect("join").expect("load_block");
        assert_eq!(&block[..], &payload[..]);
    }

    let key = block_key(99, 0, 512, false);
    assert_eq!(
        obj.gets_for_key(&key),
        1,
        "expected exactly 1 ObjectStorage::get call for concurrent load_block"
    );
    assert_eq!(obj.total_gets(), 1);
}

#[tokio::test]
async fn test_singleflight_error_and_retry() {
    let sf: SingleFlight<u32, ChunkError> = SingleFlight::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(8));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let sf = &sf;
        let calls = Arc::clone(&calls);
        let barrier = Arc::clone(&barrier);
        handles.push(async move {
            barrier.wait().await;
            sf.execute("err-key", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(15)).await;
                Err(ChunkError::OutOfBounds {
                    offset: 10,
                    length: 5,
                })
            })
            .await
        });
    }
    let results = futures::future::join_all(handles).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(results.iter().all(Result::is_err));
    assert_eq!(sf.in_flight_count(), 0);

    // Subsequent call after completion should execute a fresh flight and succeed.
    let ok = sf
        .execute("err-key", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(42)
        })
        .await
        .expect("retry succeeds");
    assert_eq!(ok, 42);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_disk_cache_capacity_eviction_2random_and_lru() {
    // 1. LRU eviction: verify exact eviction of least-recently-used entry.
    let dir_lru = tempdir().expect("tempdir lru");
    let lru_cache = DiskBlockCache::with_eviction(dir_lru.path(), 1024, EvictionPolicy::Lru);

    let b1 = vec![1u8; 512];
    let b2 = vec![2u8; 512];
    let b3 = vec![3u8; 512];

    lru_cache
        .put("chunks/0/0/1_0_512", &b1)
        .await
        .expect("put 1");
    lru_cache
        .put("chunks/0/0/2_0_512", &b2)
        .await
        .expect("put 2");
    assert_eq!(lru_cache.used_bytes(), 1024);
    assert_eq!(lru_cache.len(), 2);

    // Touch block 1 so block 2 becomes the least recently used entry.
    let hit = lru_cache
        .get("chunks/0/0/1_0_512")
        .await
        .expect("get 1")
        .expect("block 1 cached");
    assert_eq!(&hit[..], &b1[..]);

    // Inserting block 3 (512B) exceeds 1024B -> must evict block 2.
    lru_cache
        .put("chunks/0/0/3_0_512", &b3)
        .await
        .expect("put 3");
    assert_eq!(lru_cache.used_bytes(), 1024);
    assert_eq!(lru_cache.len(), 2);
    assert!(lru_cache.contains("chunks/0/0/1_0_512"));
    assert!(!lru_cache.contains("chunks/0/0/2_0_512"));
    assert!(lru_cache.contains("chunks/0/0/3_0_512"));
    assert!(
        lru_cache
            .get("chunks/0/0/2_0_512")
            .await
            .expect("get 2")
            .is_none(),
        "evicted file should be deleted from disk"
    );

    // 2. TwoRandom eviction: enforce max_bytes strictly under many insertions.
    let dir_rand = tempdir().expect("tempdir 2random");
    let rand_cache =
        DiskBlockCache::with_eviction(dir_rand.path(), 1024, EvictionPolicy::TwoRandom);
    for i in 0..10u64 {
        let key = block_key(i + 1, 0, 512, false);
        let data = vec![(i % 255) as u8; 512];
        rand_cache.put(&key, &data).await.expect("put");
        assert!(
            rand_cache.used_bytes() <= 1024,
            "used_bytes {} exceeded max_bytes 1024",
            rand_cache.used_bytes()
        );
        assert!(rand_cache.len() <= 2);
        assert!(!rand_cache.is_empty());
    }

    // 3. CachedChunkStore honors ChunkConfig::cache_size on DiskBlockCache.
    let dir_store = tempdir().expect("tempdir store");
    let obj = Arc::new(MemoryStore::new());
    let cfg = ChunkConfig {
        block_size: 256,
        compression: "none".to_owned(),
        cache_dir: Some(dir_store.path().to_path_buf()),
        cache_size: 512,
        buffer_size: 0,
        ..Default::default()
    };
    let store = CachedChunkStore::new(obj, cfg)
        .expect("store")
        .with_prefetch(0);
    let data = vec![0xABu8; 1024]; // 4 x 256B blocks
    store.write_slice(10, &data).await.expect("write");

    let dc = store.disk_cache().expect("disk cache enabled");
    assert!(
        dc.used_bytes() <= 512,
        "disk cache exceeded configured cache_size: {}",
        dc.used_bytes()
    );
    assert!(dc.len() <= 2);
}

#[tokio::test]
async fn test_prefetcher_warms_subsequent_blocks() {
    // 1. Direct Prefetcher behavior: disabled for parallel=0, deduplicates busy keys.
    let disabled = Prefetcher::new(0, |_| async {});
    assert!(!disabled.is_enabled());
    assert!(!disabled.fetch("k1"));

    let fetch_count = Arc::new(AtomicUsize::new(0));
    let count_clone = Arc::clone(&fetch_count);
    let prefetcher = Prefetcher::new(2, move |_key| {
        let count = Arc::clone(&count_clone);
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });
    assert!(prefetcher.fetch("dup-key"));
    assert!(
        !prefetcher.fetch("dup-key"),
        "duplicate key should not be re-queued while busy"
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

    // 2. CachedChunkStore integration: reading block 0 warms blocks 1 and 2 in background.
    let obj = Arc::new(CountingStore::new(Duration::ZERO));
    let writer_cfg = ChunkConfig {
        block_size: 256,
        compression: "lz4".to_owned(),
        buffer_size: 0,
        cache_size: 0,
        ..Default::default()
    };
    let writer = CachedChunkStore::new(Arc::clone(&obj) as Arc<_>, writer_cfg).expect("writer");
    let payload: Vec<u8> = (0..1024).map(|i| (i * 3 % 251) as u8).collect();
    writer.write_slice(55, &payload).await.expect("write");

    let reader_cfg = ChunkConfig {
        block_size: 256,
        compression: "lz4".to_owned(),
        buffer_size: 64 * 1024,
        ..Default::default()
    };
    let reader = CachedChunkStore::new(Arc::clone(&obj) as Arc<_>, reader_cfg)
        .expect("reader")
        .with_prefetch(2);

    // Read only the first 64 bytes (block 0). Prefetcher should warm blocks 1 and 2.
    let mut head = vec![0u8; 64];
    let n = reader
        .read_slice(55, payload.len(), 0, &mut head)
        .await
        .expect("read block 0");
    assert_eq!(n, 64);
    assert_eq!(&head[..], &payload[..64]);

    let key1 = block_key(55, 1, 256, false);
    let key2 = block_key(55, 2, 256, false);

    let warmed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if reader.memory_cache().contains(&key1) && reader.memory_cache().contains(&key2) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        warmed.is_ok(),
        "expected prefetcher to warm subsequent blocks 1 and 2"
    );

    let gets_before = obj.total_gets();
    let mut tail = vec![0u8; 512];
    let n2 = reader
        .read_slice(55, payload.len(), 256, &mut tail)
        .await
        .expect("read blocks 1..2");
    assert_eq!(n2, 512);
    assert_eq!(&tail[..], &payload[256..768]);
    assert_eq!(
        obj.gets_for_key(&key1),
        1,
        "block 1 should only have been fetched once (by prefetcher)"
    );
    assert_eq!(
        obj.gets_for_key(&key2),
        1,
        "block 2 should only have been fetched once (by prefetcher)"
    );
    // Reading blocks 1 and 2 did not need any extra object-store fetch for block 1/2
    // (block 3 may optionally be prefetched when reading block 1/2).
    assert!(gets_before >= 3);
}

#[tokio::test]
async fn test_token_bucket_rate_limiter() {
    // 1. Direct TokenBucket burst & pacing test.
    let bucket = TokenBucket::new(10_000.0, 1_000);
    assert!(bucket.try_acquire(1_000));
    assert!(!bucket.try_acquire(500));

    let start = Instant::now();
    // Acquiring 2_000 tokens at 10_000 tokens/sec from empty bucket takes ~200ms.
    bucket.acquire(2_000).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(170),
        "expected >= 170ms wait, got {elapsed:?}"
    );

    // 2. CachedChunkStore upload & download rate limiting.
    let obj = Arc::new(MemoryStore::new());
    let writer_cfg = ChunkConfig {
        block_size: 1024,
        compression: "none".to_owned(),
        buffer_size: 0,
        cache_size: 0,
        ..Default::default()
    };
    let writer = CachedChunkStore::new(Arc::clone(&obj) as Arc<_>, writer_cfg)
        .expect("writer")
        .with_upload_limiter(Some(TokenBucket::new(20_480.0, 512)));

    let payload = vec![0x5Au8; 4096];
    let up_start = Instant::now();
    writer.write_slice(1, &payload).await.expect("write");
    let up_elapsed = up_start.elapsed();
    // 4096 bytes with 512B initial burst at 20480 B/s => (4096 - 512) / 20480 = 175ms
    assert!(
        up_elapsed >= Duration::from_millis(150),
        "upload finished too fast for rate limit: {up_elapsed:?}"
    );

    let reader_cfg = ChunkConfig {
        block_size: 1024,
        compression: "none".to_owned(),
        buffer_size: 0,
        cache_size: 0,
        ..Default::default()
    };
    let reader = CachedChunkStore::new(obj, reader_cfg)
        .expect("reader")
        .with_prefetch(0)
        .with_download_limiter(Some(TokenBucket::new(20_480.0, 512)));

    let mut out = vec![0u8; 4096];
    let down_start = Instant::now();
    let n = reader
        .read_slice(1, payload.len(), 0, &mut out)
        .await
        .expect("read");
    let down_elapsed = down_start.elapsed();
    assert_eq!(n, 4096);
    assert_eq!(out, payload);
    assert!(
        down_elapsed >= Duration::from_millis(150),
        "download finished too fast for rate limit: {down_elapsed:?}"
    );
}
