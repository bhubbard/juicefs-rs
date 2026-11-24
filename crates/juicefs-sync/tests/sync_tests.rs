//! Integration tests for `juicefs-sync`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use juicefs_object::{FileStore, MemoryStore, ObjectError, ObjectMeta, ObjectStorage};
use juicefs_sync::{SyncConfig, SyncStats, sync_objects};

#[derive(Debug)]
struct TrackingStore {
    inner: Arc<MemoryStore>,
    delay: Duration,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    fail_key: Option<String>,
}

impl TrackingStore {
    fn new(inner: Arc<MemoryStore>, delay: Duration, fail_key: Option<&str>) -> Self {
        Self {
            inner,
            delay,
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            fail_key: fail_key.map(str::to_owned),
        }
    }

    fn record_enter(&self) {
        let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        let mut prev = self.max_in_flight.load(Ordering::SeqCst);
        while cur > prev {
            match self.max_in_flight.compare_exchange_weak(
                prev,
                cur,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
    }

    fn record_exit(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    fn max_observed(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStorage for TrackingStore {
    fn description(&self) -> String {
        "tracking://".to_owned()
    }

    async fn create(&self) -> Result<(), ObjectError> {
        self.inner.create().await
    }

    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        self.record_enter();
        tokio::time::sleep(self.delay).await;
        let res = if self.fail_key.as_deref() == Some(key) {
            Err(ObjectError::Backend(format!("injected failure for {key}")))
        } else {
            self.inner.get(key, offset, limit).await
        };
        self.record_exit();
        res
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
        self.record_enter();
        tokio::time::sleep(self.delay).await;
        let res = if self.fail_key.as_deref() == Some(key) {
            Err(ObjectError::Backend(format!("injected failure for {key}")))
        } else {
            self.inner.put(key, data).await
        };
        self.record_exit();
        res
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
        self.inner.head(key).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        self.inner.delete(key).await
    }

    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
        self.inner.copy(dst, src).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
        self.inner.list(prefix).await
    }
}

#[tokio::test]
async fn sync_copies_all_objects_to_empty_dst() {
    let src = Arc::new(MemoryStore::new());
    let dst = Arc::new(MemoryStore::new());

    src.put("a.txt", Bytes::from_static(b"alpha"))
        .await
        .unwrap();
    src.put("dir/b.bin", Bytes::from_static(b"bravo-data"))
        .await
        .unwrap();
    src.put("dir/sub/c.txt", Bytes::new()).await.unwrap();

    let stats = sync_objects(src.clone(), dst.clone(), SyncConfig::default())
        .await
        .unwrap();

    assert_eq!(
        stats,
        SyncStats {
            copied: 3,
            skipped: 0,
            deleted: 0,
            failed: 0,
            bytes_copied: 15,
        }
    );
    assert_eq!(
        dst.get("a.txt", 0, 0).await.unwrap(),
        Bytes::from_static(b"alpha")
    );
    assert_eq!(
        dst.get("dir/b.bin", 0, 0).await.unwrap(),
        Bytes::from_static(b"bravo-data")
    );
    assert_eq!(dst.get("dir/sub/c.txt", 0, 0).await.unwrap(), Bytes::new());
}

#[tokio::test]
async fn sync_respects_prefix_and_preserves_other_keys() {
    let src = Arc::new(MemoryStore::new());
    let dst = Arc::new(MemoryStore::new());

    src.put("logs/2026/a.log", Bytes::from_static(b"log-a"))
        .await
        .unwrap();
    src.put("other/keep.txt", Bytes::from_static(b"outside"))
        .await
        .unwrap();

    dst.put("logs/2026/stale.log", Bytes::from_static(b"stale"))
        .await
        .unwrap();
    dst.put("keep/untouched.txt", Bytes::from_static(b"keep-me"))
        .await
        .unwrap();

    let cfg = SyncConfig {
        prefix: "logs/".to_owned(),
        delete_dst: true,
        ..SyncConfig::default()
    };

    let stats = sync_objects(src.clone(), dst.clone(), cfg).await.unwrap();

    assert_eq!(stats.copied, 1);
    assert_eq!(stats.deleted, 1);
    assert_eq!(stats.failed, 0);
    assert!(dst.head("logs/2026/a.log").await.is_ok());
    assert!(dst.head("logs/2026/stale.log").await.is_err());
    assert!(dst.head("other/keep.txt").await.is_err());
    assert!(dst.head("keep/untouched.txt").await.is_ok());
}

#[tokio::test]
async fn sync_update_skips_unchanged_and_copies_newer_or_resized() {
    let src = Arc::new(MemoryStore::new());
    let dst = Arc::new(MemoryStore::new());

    src.put("same.txt", Bytes::from_static(b"12345"))
        .await
        .unwrap();
    src.put("same_len_updated.txt", Bytes::from_static(b"aaaaa"))
        .await
        .unwrap();
    src.put("resized.txt", Bytes::from_static(b"short"))
        .await
        .unwrap();

    let first = sync_objects(src.clone(), dst.clone(), SyncConfig::default())
        .await
        .unwrap();
    assert_eq!(first.copied, 3);

    // Second pass with update=true should skip all three.
    let cfg = SyncConfig {
        update: true,
        ..SyncConfig::default()
    };
    let second = sync_objects(src.clone(), dst.clone(), cfg.clone())
        .await
        .unwrap();
    assert_eq!(second.copied, 0);
    assert_eq!(second.skipped, 3);

    // Sleep briefly so mtime advances strictly beyond dst's mtime.
    tokio::time::sleep(Duration::from_millis(10)).await;
    src.put("same_len_updated.txt", Bytes::from_static(b"bbbbb"))
        .await
        .unwrap();
    src.put("resized.txt", Bytes::from_static(b"much-longer"))
        .await
        .unwrap();

    let third = sync_objects(src.clone(), dst.clone(), cfg).await.unwrap();
    assert_eq!(third.copied, 2);
    assert_eq!(third.skipped, 1);
    assert_eq!(third.bytes_copied, 5 + 11);
    assert_eq!(
        dst.get("same_len_updated.txt", 0, 0).await.unwrap(),
        Bytes::from_static(b"bbbbb")
    );
    assert_eq!(
        dst.get("resized.txt", 0, 0).await.unwrap(),
        Bytes::from_static(b"much-longer")
    );
}

#[tokio::test]
async fn sync_without_update_overwrites_existing() {
    let src = Arc::new(MemoryStore::new());
    let dst = Arc::new(MemoryStore::new());

    src.put("file.txt", Bytes::from_static(b"new12"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    dst.put("file.txt", Bytes::from_static(b"old12"))
        .await
        .unwrap();

    let stats = sync_objects(
        src.clone(),
        dst.clone(),
        SyncConfig {
            update: false,
            ..SyncConfig::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(stats.copied, 1);
    assert_eq!(stats.skipped, 0);
    assert_eq!(
        dst.get("file.txt", 0, 0).await.unwrap(),
        Bytes::from_static(b"new12")
    );
}

#[tokio::test]
async fn sync_dry_run_does_not_mutate_dst() {
    let src = Arc::new(MemoryStore::new());
    let dst = Arc::new(MemoryStore::new());

    src.put("new.txt", Bytes::from_static(b"payload"))
        .await
        .unwrap();
    dst.put("extra.txt", Bytes::from_static(b"keep"))
        .await
        .unwrap();

    let cfg = SyncConfig {
        delete_dst: true,
        dry_run: true,
        ..SyncConfig::default()
    };

    let stats = sync_objects(src.clone(), dst.clone(), cfg).await.unwrap();
    assert_eq!(stats.copied, 1);
    assert_eq!(stats.deleted, 1);
    assert_eq!(stats.bytes_copied, 7);

    // Destination remains untouched.
    assert!(dst.head("new.txt").await.is_err());
    assert!(dst.head("extra.txt").await.is_ok());
}

#[tokio::test]
async fn sync_concurrency_is_bounded_by_semaphore() {
    let src_inner = Arc::new(MemoryStore::new());
    for i in 0..12 {
        src_inner
            .put(&format!("obj-{i:02}.dat"), Bytes::from(vec![b'x'; 64]))
            .await
            .unwrap();
    }

    let src = Arc::new(TrackingStore::new(
        src_inner,
        Duration::from_millis(15),
        None,
    ));
    let dst = Arc::new(MemoryStore::new());

    let cfg = SyncConfig {
        concurrency: 3,
        ..SyncConfig::default()
    };

    let stats = sync_objects(src.clone(), dst.clone(), cfg).await.unwrap();
    assert_eq!(stats.copied, 12);
    assert_eq!(stats.failed, 0);
    assert!(
        src.max_observed() <= 3,
        "observed concurrency {} exceeded limit 3",
        src.max_observed()
    );
    assert!(
        src.max_observed() >= 2,
        "expected concurrent execution, max observed {}",
        src.max_observed()
    );
}

#[tokio::test]
async fn sync_records_individual_failures_without_aborting_other_keys() {
    let src_inner = Arc::new(MemoryStore::new());
    src_inner
        .put("ok1.txt", Bytes::from_static(b"one"))
        .await
        .unwrap();
    src_inner
        .put("bad.txt", Bytes::from_static(b"boom"))
        .await
        .unwrap();
    src_inner
        .put("ok2.txt", Bytes::from_static(b"two"))
        .await
        .unwrap();

    let src = Arc::new(TrackingStore::new(
        src_inner,
        Duration::from_millis(1),
        Some("bad.txt"),
    ));
    let dst = Arc::new(MemoryStore::new());

    let stats = sync_objects(src, dst.clone(), SyncConfig::default())
        .await
        .unwrap();

    assert_eq!(stats.copied, 2);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.bytes_copied, 6);
    assert!(dst.head("ok1.txt").await.is_ok());
    assert!(dst.head("ok2.txt").await.is_ok());
    assert!(dst.head("bad.txt").await.is_err());
}

#[tokio::test]
async fn sync_file_store_to_file_store_end_to_end() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();

    let src = Arc::new(FileStore::new(src_dir.path()).unwrap());
    let dst = Arc::new(FileStore::new(dst_dir.path()).unwrap());
    src.create().await.unwrap();

    src.put("nested/hello.txt", Bytes::from_static(b"file-sync"))
        .await
        .unwrap();
    dst.create().await.unwrap();
    dst.put("nested/old.txt", Bytes::from_static(b"remove-me"))
        .await
        .unwrap();

    let stats = sync_objects(
        src,
        dst.clone(),
        SyncConfig {
            concurrency: 4,
            delete_dst: true,
            ..SyncConfig::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(stats.copied, 1);
    assert_eq!(stats.deleted, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(
        dst.get("nested/hello.txt", 0, 0).await.unwrap(),
        Bytes::from_static(b"file-sync")
    );
    assert!(dst.head("nested/old.txt").await.is_err());
}
