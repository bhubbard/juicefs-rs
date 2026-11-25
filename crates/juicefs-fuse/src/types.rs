//! Pure-Rust FUSE protocol wire types and POSIX mode conversions.

use std::time::Duration;

use juicefs_core::{Attr, Entry, Ino, MAX_NAME, NodeType, PAGE_SIZE, StatFs};

use crate::error::FuseError;

/// FUSE file attribute record (`struct fuse_attr`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseAttr {
    /// Inode number.
    pub ino: u64,
    /// Logical file size in bytes.
    pub size: u64,
    /// Number of allocated 512-byte blocks.
    pub blocks: u64,
    /// Last access time (seconds since Unix epoch).
    pub atime: i64,
    /// Last access time nanoseconds.
    pub atime_nsec: u32,
    /// Last modification time (seconds since Unix epoch).
    pub mtime: i64,
    /// Last modification time nanoseconds.
    pub mtime_nsec: u32,
    /// Last status change time (seconds since Unix epoch).
    pub ctime: i64,
    /// Last status change time nanoseconds.
    pub ctime_nsec: u32,
    /// POSIX file mode including file type (`S_IFMT | perm`).
    pub mode: u32,
    /// Hard link count.
    pub nlink: u32,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// Device ID for character and block special files.
    pub rdev: u32,
    /// Preferred I/O block size in bytes (`65536`).
    pub blksize: u32,
}

impl FuseAttr {
    /// Converts a `JuiceFS` `(Ino, Attr)` pair into a [`FuseAttr`].
    #[must_use]
    pub fn from_vfs_attr(inode: Ino, attr: &Attr) -> Self {
        let (size, blocks) = match attr.typ {
            NodeType::File | NodeType::Directory | NodeType::Symlink => {
                (attr.length, attr.length.div_ceil(512))
            }
            NodeType::Fifo | NodeType::BlockDev | NodeType::CharDev | NodeType::Socket => (0, 0),
        };
        let rdev = match attr.typ {
            NodeType::BlockDev | NodeType::CharDev => attr.rdev,
            _ => 0,
        };
        Self {
            ino: inode.as_u64(),
            size,
            blocks,
            atime: attr.atime,
            atime_nsec: attr.atime_nsec,
            mtime: attr.mtime,
            mtime_nsec: attr.mtime_nsec,
            ctime: attr.ctime,
            ctime_nsec: attr.ctime_nsec,
            mode: node_type_to_mode(attr.typ, attr.mode),
            nlink: attr.nlink,
            uid: attr.uid,
            gid: attr.gid,
            rdev,
            blksize: PAGE_SIZE as u32,
        }
    }
}

/// FUSE dentry reply (`struct fuse_entry_out`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseEntry {
    /// Inode number (`nodeid`).
    pub ino: u64,
    /// Inode generation number.
    pub generation: u64,
    /// Cache validity duration for this name-to-inode mapping.
    pub entry_ttl: Duration,
    /// Cache validity duration for [`Self::attr`].
    pub attr_ttl: Duration,
    /// Node attributes.
    pub attr: FuseAttr,
}

impl FuseEntry {
    /// Builds a [`FuseEntry`] from `(Ino, Attr)` using the configured cache TTLs.
    #[must_use]
    pub fn from_vfs_entry(
        inode: Ino,
        attr: &Attr,
        entry_ttl: Duration,
        attr_ttl: Duration,
    ) -> Self {
        Self {
            ino: inode.as_u64(),
            generation: 1,
            entry_ttl,
            attr_ttl,
            attr: FuseAttr::from_vfs_attr(inode, attr),
        }
    }
}

/// FUSE directory entry returned by `readdir`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseDirEntry {
    /// Target inode number.
    pub ino: u64,
    /// Monotonic directory stream offset for resuming `readdir`.
    pub offset: u64,
    /// Entry name.
    pub name: String,
    /// Node type.
    pub typ: NodeType,
    /// Full POSIX mode (`S_IFMT | perm`).
    pub mode: u32,
}

impl FuseDirEntry {
    /// Converts a `JuiceFS` metadata [`Entry`] and stream offset into a [`FuseDirEntry`].
    #[must_use]
    pub fn from_vfs_entry(entry: &Entry, offset: u64) -> Self {
        Self {
            ino: entry.inode.as_u64(),
            offset,
            name: entry.name.clone(),
            typ: entry.attr.typ,
            mode: node_type_to_mode(entry.attr.typ, entry.attr.mode),
        }
    }
}

/// FUSE filesystem capacity statistics (`struct fuse_kstatfs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseStatFs {
    /// Total data blocks in filesystem.
    pub blocks: u64,
    /// Free blocks in filesystem.
    pub bfree: u64,
    /// Free blocks available to unprivileged user.
    pub bavail: u64,
    /// Total file nodes (inodes) in filesystem.
    pub files: u64,
    /// Free file nodes in filesystem.
    pub ffree: u64,
    /// Optimal transfer block size in bytes.
    pub bsize: u32,
    /// Fragment size in bytes.
    pub frsize: u32,
    /// Maximum length of filenames.
    pub namelen: u32,
}

impl From<StatFs> for FuseStatFs {
    fn from(stat: StatFs) -> Self {
        const BLOCK_SIZE: u32 = 4096;
        let bsize_u64 = u64::from(BLOCK_SIZE);
        let blocks = (stat.total_space / bsize_u64).max(1);
        let bavail = stat.avail_space / bsize_u64;
        Self {
            blocks,
            bfree: bavail,
            bavail,
            files: stat.used_inodes.saturating_add(stat.avail_inodes),
            ffree: stat.avail_inodes,
            bsize: BLOCK_SIZE,
            frsize: BLOCK_SIZE,
            namelen: MAX_NAME as u32,
        }
    }
}

/// Encodes a [`NodeType`] and 12-bit permission bits into a full POSIX `st_mode`.
#[must_use]
pub const fn node_type_to_mode(typ: NodeType, perm: u16) -> u32 {
    let type_bits = match typ {
        NodeType::File => libc::S_IFREG as u32,
        NodeType::Directory => libc::S_IFDIR as u32,
        NodeType::Symlink => libc::S_IFLNK as u32,
        NodeType::Fifo => libc::S_IFIFO as u32,
        NodeType::BlockDev => libc::S_IFBLK as u32,
        NodeType::CharDev => libc::S_IFCHR as u32,
        NodeType::Socket => libc::S_IFSOCK as u32,
    };
    type_bits | ((perm as u32) & 0o7777)
}

/// Extracts [`NodeType`] and permission bits from a POSIX `st_mode` value.
///
/// If no `S_IFMT` file-type bits are set, defaults to [`NodeType::File`].
pub fn mode_to_node_type(mode: u32) -> Result<(NodeType, u16), FuseError> {
    let perm = (mode & 0o7777) as u16;
    let ifmt = mode & u32::from(libc::S_IFMT);
    let typ = match ifmt {
        0 => NodeType::File,
        x if x == u32::from(libc::S_IFREG) => NodeType::File,
        x if x == u32::from(libc::S_IFDIR) => NodeType::Directory,
        x if x == u32::from(libc::S_IFLNK) => NodeType::Symlink,
        x if x == u32::from(libc::S_IFIFO) => NodeType::Fifo,
        x if x == u32::from(libc::S_IFBLK) => NodeType::BlockDev,
        x if x == u32::from(libc::S_IFCHR) => NodeType::CharDev,
        x if x == u32::from(libc::S_IFSOCK) => NodeType::Socket,
        other => {
            return Err(FuseError::invalid(format!(
                "unsupported POSIX file type bits: {other:#o}"
            )));
        }
    };
    Ok((typ, perm))
}
