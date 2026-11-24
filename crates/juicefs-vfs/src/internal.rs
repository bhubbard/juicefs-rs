//! Virtual internal files (`.stats`, `.config`, `.control`, `.accesslog`) for `JuiceFS`.

use juicefs_core::{Format, Ino, StatFs};
use serde::{Deserialize, Serialize};

use crate::error::VfsError;

/// Base inode number reserved for virtual internal nodes.
pub const MIN_INTERNAL_NODE: u64 = 0x7FFF_FFFF_0000_0000;

/// Virtual inode for `.accesslog`.
pub const LOG_INODE: Ino = Ino(MIN_INTERNAL_NODE + 1);

/// Virtual inode for `.control`.
pub const CONTROL_INODE: Ino = Ino(MIN_INTERNAL_NODE + 2);

/// Virtual inode for `.stats`.
pub const STATS_INODE: Ino = Ino(MIN_INTERNAL_NODE + 3);

/// Virtual inode for `.config`.
pub const CONFIG_INODE: Ino = Ino(MIN_INTERNAL_NODE + 4);

/// Virtual file name for volume metrics/statistics.
pub const STATS_NAME: &str = ".stats";

/// Virtual file name for volume configuration JSON.
pub const CONFIG_NAME: &str = ".config";

/// Virtual file name for control channel.
pub const CONTROL_NAME: &str = ".control";

/// Virtual file name for access log stream.
pub const ACCESSLOG_NAME: &str = ".accesslog";

/// Volume runtime and capacity statistics exposed via `.stats`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeStats {
    /// Volume name.
    pub volume: String,
    /// Volume UUID.
    pub uuid: String,
    /// Object storage backend name.
    pub storage: String,
    /// Configured block size in bytes.
    pub block_size: usize,
    /// Compression algorithm.
    pub compression: String,
    /// Total filesystem space in bytes.
    pub total_space: u64,
    /// Available filesystem space in bytes.
    pub avail_space: u64,
    /// Used filesystem space in bytes.
    pub used_space: u64,
    /// Number of allocated inodes.
    pub used_inodes: u64,
    /// Number of remaining available inodes.
    pub avail_inodes: u64,
    /// Bytes currently held in the in-memory chunk cache.
    pub chunk_cache_bytes: usize,
}

/// Returns `true` if `ino` is a virtual internal node.
#[must_use]
pub const fn is_special_node(ino: Ino) -> bool {
    ino.as_u64() >= MIN_INTERNAL_NODE
}

/// Returns `true` if `name` is a reserved virtual internal filename.
#[must_use]
pub fn is_special_name(name: &str) -> bool {
    matches!(
        name.trim_start_matches('/'),
        STATS_NAME | CONFIG_NAME | CONTROL_NAME | ACCESSLOG_NAME | ".trash"
    )
}

pub(crate) fn render_stats_json(
    format: &Format,
    fs_stat: StatFs,
    chunk_cache_bytes: usize,
) -> Result<Vec<u8>, VfsError> {
    let volume_stats = VolumeStats {
        volume: format.name.clone(),
        uuid: format.uuid.clone(),
        storage: format.storage.clone(),
        block_size: format.block_size,
        compression: format.compression.clone(),
        total_space: fs_stat.total_space,
        avail_space: fs_stat.avail_space,
        used_space: fs_stat.total_space.saturating_sub(fs_stat.avail_space),
        used_inodes: fs_stat.used_inodes,
        avail_inodes: fs_stat.avail_inodes,
        chunk_cache_bytes,
    };
    Ok(serde_json::to_vec_pretty(&volume_stats)?)
}

pub(crate) fn render_config_json(format: &Format) -> Result<Vec<u8>, VfsError> {
    let mut sanitized = format.clone();
    if !sanitized.secret_key.is_empty() {
        "removed".clone_into(&mut sanitized.secret_key);
    }
    if !sanitized.session_token.is_empty() {
        "removed".clone_into(&mut sanitized.session_token);
    }
    Ok(serde_json::to_vec_pretty(&sanitized)?)
}
