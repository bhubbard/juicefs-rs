//! Concurrent object storage synchronization engine modeled after `JuiceFS` `pkg/sync`.

pub mod error;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use juicefs_object::{ObjectError, ObjectMeta, ObjectStorage};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, warn};

pub use error::SyncError;

const DEFAULT_CONCURRENCY: usize = 16;

/// Configuration controlling [`sync_objects`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncConfig {
    /// Maximum number of concurrent copy/delete tasks (values of `0` are clamped to `1`).
    pub concurrency: usize,
    /// When `true`, delete keys in `dst` (matching `prefix`) that do not exist in `src`.
    pub delete_dst: bool,
    /// When `true`, compare size and modification time and skip up-to-date destination objects.
    pub update: bool,
    /// When `true`, compute stats without writing to or deleting from `dst`.
    pub dry_run: bool,
    /// Restrict synchronization to keys starting with this prefix.
    pub prefix: String,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY,
            delete_dst: false,
            update: false,
            dry_run: false,
            prefix: String::new(),
        }
    }
}

/// Summary statistics produced by [`sync_objects`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Number of objects copied (or that would be copied when `dry_run` is `true`).
    pub copied: u64,
    /// Number of objects skipped because destination is already up to date.
    pub skipped: u64,
    /// Number of extra objects deleted from destination (or that would be deleted when `dry_run` is `true`).
    pub deleted: u64,
    /// Number of individual copy or delete operations that failed.
    pub failed: u64,
    /// Total payload bytes copied (or that would be copied when `dry_run` is `true`).
    pub bytes_copied: u64,
}

enum TaskOutcome {
    Copy {
        key: String,
        result: Result<u64, ObjectError>,
    },
    Delete {
        key: String,
        result: Result<(), ObjectError>,
    },
}

/// Synchronizes objects matching `cfg.prefix` from `src` to `dst` concurrently.
///
/// Tasks are bounded by a Tokio [`Semaphore`] sized to `cfg.concurrency.max(1)`.
/// When `cfg.update` is `true`, an existing destination object is skipped if
/// `dst.size == src.size && src.mtime <= dst.mtime`.
/// When `cfg.delete_dst` is `true`, extra destination keys matching `cfg.prefix`
/// that are absent from `src` are removed.
pub async fn sync_objects(
    src: Arc<dyn ObjectStorage>,
    dst: Arc<dyn ObjectStorage>,
    cfg: SyncConfig,
) -> Result<SyncStats, SyncError> {
    if !cfg.dry_run {
        dst.create().await?;
    }

    let (src_objects, dst_objects) =
        tokio::try_join!(src.list(&cfg.prefix), dst.list(&cfg.prefix))?;

    let mut dst_map: HashMap<String, ObjectMeta> = HashMap::with_capacity(dst_objects.len());
    for obj in dst_objects {
        dst_map.insert(obj.key.clone(), obj);
    }

    let mut src_keys: HashSet<String> = HashSet::with_capacity(src_objects.len());
    let mut to_copy: Vec<ObjectMeta> = Vec::new();
    let mut stats = SyncStats::default();

    for src_obj in src_objects {
        src_keys.insert(src_obj.key.clone());

        if cfg.update
            && let Some(dst_obj) = dst_map.get(&src_obj.key)
            && is_up_to_date(&src_obj, dst_obj)
        {
            debug!(key = %src_obj.key, "skipping up-to-date object");
            stats.skipped += 1;
            continue;
        }
        to_copy.push(src_obj);
    }

    let mut to_delete: Vec<String> = Vec::new();
    if cfg.delete_dst {
        for key in dst_map.into_keys() {
            if !src_keys.contains(&key) {
                to_delete.push(key);
            }
        }
    }

    if cfg.dry_run {
        for obj in to_copy {
            debug!(key = %obj.key, size = obj.size, "dry-run: would copy object");
            stats.copied += 1;
            stats.bytes_copied += obj.size;
        }
        for key in to_delete {
            debug!(key = %key, "dry-run: would delete object");
            stats.deleted += 1;
        }
        return Ok(stats);
    }

    let concurrency = cfg.concurrency.max(1);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut join_set: JoinSet<TaskOutcome> = JoinSet::new();

    for obj in to_copy {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| SyncError::Task(e.to_string()))?;
        let src = Arc::clone(&src);
        let dst = Arc::clone(&dst);
        let key = obj.key;

        join_set.spawn(async move {
            let _permit = permit;
            let result = copy_one(src.as_ref(), dst.as_ref(), &key).await;
            TaskOutcome::Copy { key, result }
        });

        while let Some(res) = join_set.try_join_next() {
            record_outcome(&mut stats, res.map_err(|e| SyncError::Task(e.to_string()))?);
        }
    }

    for key in to_delete {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| SyncError::Task(e.to_string()))?;
        let dst = Arc::clone(&dst);

        join_set.spawn(async move {
            let _permit = permit;
            let result = dst.delete(&key).await;
            TaskOutcome::Delete { key, result }
        });

        while let Some(res) = join_set.try_join_next() {
            record_outcome(&mut stats, res.map_err(|e| SyncError::Task(e.to_string()))?);
        }
    }

    while let Some(res) = join_set.join_next().await {
        record_outcome(&mut stats, res.map_err(|e| SyncError::Task(e.to_string()))?);
    }

    Ok(stats)
}

fn is_up_to_date(src: &ObjectMeta, dst: &ObjectMeta) -> bool {
    src.size == dst.size && src.mtime <= dst.mtime
}

async fn copy_one(
    src: &dyn ObjectStorage,
    dst: &dyn ObjectStorage,
    key: &str,
) -> Result<u64, ObjectError> {
    let data = src.get(key, 0, 0).await?;
    let len = data.len() as u64;
    dst.put(key, data).await?;
    Ok(len)
}

fn record_outcome(stats: &mut SyncStats, outcome: TaskOutcome) {
    match outcome {
        TaskOutcome::Copy { key, result } => match result {
            Ok(bytes) => {
                stats.copied += 1;
                stats.bytes_copied += bytes;
            }
            Err(err) => {
                warn!(key = %key, error = %err, "failed to copy object");
                stats.failed += 1;
            }
        },
        TaskOutcome::Delete { key, result } => match result {
            Ok(()) => {
                stats.deleted += 1;
            }
            Err(err) => {
                warn!(key = %key, error = %err, "failed to delete object");
                stats.failed += 1;
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::*;

    fn meta(key: &str, size: u64, mtime: SystemTime) -> ObjectMeta {
        ObjectMeta {
            key: key.to_owned(),
            size,
            mtime,
            is_dir: false,
        }
    }

    #[test]
    fn default_config_uses_sane_concurrency() {
        let cfg = SyncConfig::default();
        assert_eq!(cfg.concurrency, DEFAULT_CONCURRENCY);
        assert!(!cfg.delete_dst);
        assert!(!cfg.update);
        assert!(!cfg.dry_run);
        assert_eq!(cfg.prefix, "");
    }

    #[test]
    fn is_up_to_date_checks_size_and_mtime() {
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let t1 = SystemTime::UNIX_EPOCH + Duration::from_secs(200);

        assert!(is_up_to_date(&meta("a", 10, t0), &meta("a", 10, t0)));
        assert!(is_up_to_date(&meta("a", 10, t0), &meta("a", 10, t1)));
        assert!(!is_up_to_date(&meta("a", 10, t1), &meta("a", 10, t0)));
        assert!(!is_up_to_date(&meta("a", 11, t0), &meta("a", 10, t1)));
    }
}
