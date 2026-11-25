//! Metadata engine trait, `SQLite` implementation, and optional `Redis` implementation for `JuiceFS`.

mod dump;
pub mod error;
#[cfg(feature = "redis")]
pub mod redis_meta;
pub mod sqlite;
mod util;

use std::sync::Arc;

use async_trait::async_trait;
use juicefs_core::{Attr, Entry, Format, Ino, NodeType, SetAttrFlags, Slice, StatFs};
use serde::{Deserialize, Serialize};

pub use error::MetaError;
#[cfg(feature = "redis")]
pub use redis_meta::RedisMeta;
pub use sqlite::SqliteMeta;

/// Lock type for BSD (`flock`) and `POSIX` (`fcntl`) file locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum FlockType {
    /// Release an existing lock (`F_UNLCK`).
    Unlock = 0,
    /// Shared read lock (`F_RDLCK`).
    Shared = 1,
    /// Exclusive write lock (`F_WRLCK`).
    Exclusive = 2,
}

impl TryFrom<u8> for FlockType {
    type Error = MetaError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unlock),
            1 => Ok(Self::Shared),
            2 => Ok(Self::Exclusive),
            other => Err(MetaError::InvalidInput(format!(
                "invalid lock type: {other}"
            ))),
        }
    }
}

/// Byte-range `POSIX` lock record (`fcntl` `F_GETLK`/`F_SETLK`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlockRecord {
    /// Lock type (`Unlock`, `Shared`, or `Exclusive`).
    pub typ: FlockType,
    /// Process ID of the lock owner.
    pub pid: u32,
    /// Inclusive start byte offset.
    pub start: u64,
    /// Inclusive end byte offset.
    pub end: u64,
}

/// Directory space and inode quota limits and current usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Quota {
    /// Maximum allowed bytes (`<= 0` means unlimited).
    pub max_space: i64,
    /// Maximum allowed inodes (`<= 0` means unlimited).
    pub max_inodes: i64,
    /// Currently used bytes under the directory subtree.
    pub used_space: i64,
    /// Currently used inodes under the directory subtree.
    pub used_inodes: i64,
}

/// Incremental directory statistics (`dirStats`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DirStat {
    /// Total logical file data length in bytes under the directory.
    pub data_length: i64,
    /// Total 4 KiB-aligned used space in bytes under the directory.
    pub used_space: i64,
    /// Total number of inodes under the directory.
    pub used_inodes: i64,
}

/// Target entity for per-user or per-group quotas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum QuotaTarget {
    /// Quota for a specific Unix user ID (`uid`).
    User(u32),
    /// Quota for a specific Unix group ID (`gid`).
    Group(u32),
}

/// Recorded metadata mutation changelog entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeLogEntry {
    /// Monotonically increasing changelog sequence identifier.
    pub id: i64,
    /// Serialized mutation entry payload.
    pub entry: String,
}

/// Client mount session metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInfo {
    /// Unique session identifier.
    pub sid: u64,
    /// Expiration timestamp in Unix seconds.
    pub expire: i64,
    /// Client mount point or label.
    pub mount_point: String,
}

/// Mismatch between stored `nlink` and the link count observed in directory edges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NlinkMismatch {
    /// Inode with an inconsistent link count.
    pub inode: Ino,
    /// `nlink` value stored on the node.
    pub stored_nlink: u32,
    /// Expected `nlink` computed from directory edges.
    pub expected_nlink: u32,
}

/// Filesystem metadata consistency report produced by [`Meta::fsck`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FsckReport {
    /// Edges whose parent node does not exist or is not a directory.
    pub orphaned_edges: Vec<String>,
    /// Target inodes referenced by edges that are missing from the node table.
    pub missing_nodes: Vec<u64>,
    /// Normal nodes (excluding root and trash) with no incoming directory edge.
    pub unreferenced_nodes: Vec<u64>,
    /// Nodes whose stored `nlink` disagrees with directory edges.
    pub nlink_mismatches: Vec<NlinkMismatch>,
    /// Total number of nodes inspected.
    pub checked_nodes: usize,
    /// Total number of edges inspected.
    pub checked_edges: usize,
}

impl FsckReport {
    /// Returns `true` if no metadata inconsistencies were found.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.orphaned_edges.is_empty()
            && self.missing_nodes.is_empty()
            && self.unreferenced_nodes.is_empty()
            && self.nlink_mismatches.is_empty()
    }
}

/// Async metadata engine interface modeled after `JuiceFS` `pkg/meta.Meta`.
#[async_trait]
pub trait Meta: Send + Sync + std::fmt::Debug {
    /// Formats a new volume and initializes the root inode (`Ino(1)`).
    async fn init(&self, format: &Format, force: bool) -> Result<(), MetaError>;

    /// Loads the persisted volume [`Format`].
    async fn load(&self) -> Result<Format, MetaError>;

    /// Returns filesystem capacity and usage statistics.
    async fn statfs(&self) -> Result<StatFs, MetaError>;

    /// Looks up `name` inside directory `parent`.
    async fn lookup(&self, parent: Ino, name: &str) -> Result<(Ino, Attr), MetaError>;

    /// Returns attributes for `inode`.
    async fn getattr(&self, inode: Ino) -> Result<Attr, MetaError>;

    /// Updates attributes for `inode` according to `mask`.
    async fn setattr(&self, inode: Ino, attr: &Attr, mask: SetAttrFlags)
    -> Result<Attr, MetaError>;

    /// Creates a new regular file or special node in `parent`.
    async fn mknod(
        &self,
        parent: Ino,
        name: &str,
        typ: NodeType,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<(Ino, Attr), MetaError>;

    /// Creates a new directory in `parent`.
    async fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<(Ino, Attr), MetaError>;

    /// Creates a symbolic link in `parent` pointing to `target`.
    async fn symlink(
        &self,
        parent: Ino,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<(Ino, Attr), MetaError>;

    /// Reads the target of symbolic link `inode`.
    async fn readlink(&self, inode: Ino) -> Result<String, MetaError>;

    /// Creates a hard link `new_name` in `new_parent` pointing to `inode`.
    async fn link(&self, inode: Ino, new_parent: Ino, new_name: &str) -> Result<Attr, MetaError>;

    /// Removes directory entry `name` from `parent`.
    async fn unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError>;

    /// Removes an empty directory `name` from `parent`.
    async fn rmdir(&self, parent: Ino, name: &str) -> Result<(), MetaError>;

    /// Renames `src_name` in `src_parent` to `dst_name` in `dst_parent`.
    async fn rename(
        &self,
        src_parent: Ino,
        src_name: &str,
        dst_parent: Ino,
        dst_name: &str,
    ) -> Result<(), MetaError>;

    /// Lists all entries in directory `inode`.
    async fn readdir(&self, inode: Ino) -> Result<Vec<Entry>, MetaError>;

    /// Allocates a new globally unique slice ID.
    async fn new_slice(&self) -> Result<u64, MetaError>;

    /// Returns the resolved non-overlapping slices for chunk `indx` of `inode`.
    async fn read_chunk(&self, inode: Ino, indx: u32) -> Result<Vec<Slice>, MetaError>;

    /// Appends a written slice to chunk `indx` of `inode` and updates file length.
    async fn write_chunk(
        &self,
        inode: Ino,
        indx: u32,
        pos: u32,
        slice: Slice,
        new_file_length: u64,
    ) -> Result<Attr, MetaError>;

    /// Truncates or extends `inode` to `length` bytes.
    async fn truncate(&self, inode: Ino, length: u64) -> Result<Attr, MetaError>;

    /// Sets an extended attribute on `inode`.
    async fn set_xattr(&self, inode: Ino, name: &str, value: &[u8]) -> Result<(), MetaError>;

    /// Gets an extended attribute from `inode`.
    async fn get_xattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>, MetaError>;

    /// Lists extended attribute names on `inode`.
    async fn list_xattr(&self, inode: Ino) -> Result<Vec<String>, MetaError>;

    /// Removes an extended attribute from `inode`.
    async fn remove_xattr(&self, inode: Ino, name: &str) -> Result<(), MetaError>;

    /// Lists `(slice_id, size)` pairs whose reference count has dropped to zero.
    async fn list_orphan_slices(&self) -> Result<Vec<(u64, u32)>, MetaError>;

    /// Removes an orphan slice reference record after its object data has been deleted.
    async fn purge_orphan_slice(&self, slice_id: u64) -> Result<(), MetaError>;

    /// Acquires, upgrades, downgrades, or releases a whole-file `flock` lock.
    async fn flock(&self, inode: Ino, owner: u64, ltype: FlockType) -> Result<(), MetaError>;

    /// Queries whether `lock` on `inode` would conflict with locks held by other owners.
    async fn get_lk(
        &self,
        inode: Ino,
        owner: u64,
        lock: PlockRecord,
    ) -> Result<PlockRecord, MetaError>;

    /// Acquires, updates, or releases a byte-range `POSIX` lock on `inode`.
    async fn set_lk(&self, inode: Ino, owner: u64, lock: PlockRecord) -> Result<(), MetaError>;

    /// Sets or updates space and inode quotas on directory `inode`.
    async fn set_quota(
        &self,
        inode: Ino,
        max_space: i64,
        max_inodes: i64,
    ) -> Result<Quota, MetaError>;

    /// Returns the quota configured on directory `inode`.
    async fn get_quota(&self, inode: Ino) -> Result<Quota, MetaError>;

    /// Removes the quota configured on directory `inode`.
    async fn del_quota(&self, inode: Ino) -> Result<(), MetaError>;

    /// Lists all configured directory quotas sorted by inode.
    async fn list_quotas(&self) -> Result<Vec<(Ino, Quota)>, MetaError>;

    /// Moves entry `name` in `parent` into `.trash`, returning its trash entry name.
    async fn move_to_trash(&self, parent: Ino, name: &str) -> Result<String, MetaError>;

    /// Permanently deletes all entries currently stored under `.trash`, returning removed node count.
    async fn empty_trash(&self) -> Result<u64, MetaError>;

    /// Restores `trash_name` from `.trash` back to its original parent directory and name.
    async fn restore_from_trash(&self, trash_name: &str) -> Result<(Ino, Attr), MetaError>;

    /// Serializes the full metadata state to a `JSON` string.
    async fn dump_json(&self) -> Result<String, MetaError>;

    /// Replaces the metadata state by loading a `JSON` snapshot produced by [`Meta::dump_json`].
    async fn load_json(&self, json: &str) -> Result<(), MetaError>;

    /// Checks metadata integrity (orphaned edges, missing nodes, `nlink` consistency).
    async fn fsck(&self) -> Result<FsckReport, MetaError>;

    /// Atomically replaces the first `old_count` slice records of `(inode, indx)` with `new_slice`.
    async fn compact_chunk(
        &self,
        inode: Ino,
        indx: u32,
        old_count: usize,
        new_slice: Slice,
    ) -> Result<(), MetaError>;

    /// Updates the persisted volume [`Format`] in metadata.
    async fn update_format(&self, format: &Format) -> Result<(), MetaError>;

    /// Zero-copy clones a file or directory tree from `src_parent/src_name` to `dst_parent/dst_name`.
    async fn clone_entry(
        &self,
        src_parent: Ino,
        src_name: &str,
        dst_parent: Ino,
        dst_name: &str,
    ) -> Result<(Ino, Attr), MetaError>;

    /// Returns incremental directory statistics (`dirStats`) for directory `inode`.
    async fn get_dir_stat(&self, inode: Ino) -> Result<DirStat, MetaError>;

    /// Sets or updates a user or group quota.
    async fn set_ug_quota(
        &self,
        target: QuotaTarget,
        max_space: i64,
        max_inodes: i64,
    ) -> Result<Quota, MetaError>;

    /// Returns the configured quota for a user or group target.
    async fn get_ug_quota(&self, target: QuotaTarget) -> Result<Quota, MetaError>;

    /// Removes the configured quota for a user or group target.
    async fn del_ug_quota(&self, target: QuotaTarget) -> Result<(), MetaError>;

    /// Lists all configured user and group quotas.
    async fn list_ug_quotas(&self) -> Result<Vec<(QuotaTarget, Quota)>, MetaError>;

    /// Reads up to `limit` metadata changelog entries with `id > after_id`.
    async fn read_changelog(
        &self,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<ChangeLogEntry>, MetaError>;

    /// Creates a new client session with expiration `ttl_secs` seconds from now.
    async fn new_session(&self, mount_point: &str, ttl_secs: i64)
    -> Result<SessionInfo, MetaError>;

    /// Refreshes expiration for active session `sid` by `ttl_secs` seconds from now.
    async fn heartbeat(&self, sid: u64, ttl_secs: i64) -> Result<(), MetaError>;

    /// Closes session `sid` and releases any locks associated with it.
    async fn close_session(&self, sid: u64) -> Result<(), MetaError>;

    /// Lists all recorded sessions ordered by session ID.
    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, MetaError>;

    /// Deletes all expired sessions and returns the number of removed sessions.
    async fn cleanup_stale_sessions(&self) -> Result<u64, MetaError>;
}

/// Opens a metadata engine from a `JuiceFS` metadata URL (e.g. `sqlite3:///path/to/db`).
pub fn open_meta(meta_url: &str) -> Result<SqliteMeta, MetaError> {
    if let Some(rest) = meta_url.strip_prefix("sqlite3://") {
        let path = if rest == ":memory:" || rest.is_empty() {
            ":memory:".to_owned()
        } else {
            rest.to_owned()
        };
        return SqliteMeta::open(&path);
    }
    if std::path::Path::new(meta_url)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("db"))
        || meta_url == ":memory:"
        || !meta_url.contains("://")
    {
        return SqliteMeta::open(meta_url);
    }
    Err(MetaError::InvalidInput(format!(
        "unsupported metadata URL scheme (expected sqlite3://): {meta_url}"
    )))
}

/// Opens any supported metadata backend (`sqlite3://` or `redis://` / `rediss://`) as `Arc<dyn Meta>`.
#[cfg_attr(
    not(feature = "redis"),
    expect(
        clippy::unused_async,
        reason = "await is used when the redis feature is enabled"
    )
)]
pub async fn open_any_meta(meta_url: &str) -> Result<Arc<dyn Meta>, MetaError> {
    if meta_url.starts_with("redis://") || meta_url.starts_with("rediss://") {
        #[cfg(feature = "redis")]
        {
            let meta = RedisMeta::connect(meta_url).await?;
            return Ok(Arc::new(meta));
        }
        #[cfg(not(feature = "redis"))]
        {
            return Err(MetaError::InvalidInput(
                "redis metadata backend requires the `redis` feature".to_owned(),
            ));
        }
    }
    Ok(Arc::new(open_meta(meta_url)?))
}
