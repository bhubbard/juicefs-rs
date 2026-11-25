//! POSIX filesystem types, inode identifiers, and node attributes.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::CoreError;

/// Bit shift for a 64 MiB chunk.
pub const CHUNK_BITS: u32 = 26;

/// Size of a single `JuiceFS` chunk (64 MiB).
pub const CHUNK_SIZE: u64 = 1 << CHUNK_BITS;

/// Default size of a single block within a chunk (4 MiB).
pub const DEFAULT_BLOCK_SIZE: usize = 4 << 20;

/// Page size used for read/write buffering (64 KiB).
pub const PAGE_SIZE: usize = 1 << 16;

/// Maximum filename length in bytes.
pub const MAX_NAME: usize = 255;

/// Maximum symlink target length in bytes.
pub const MAX_SYMLINK: usize = 4096;

/// Root directory inode number.
pub const ROOT_INODE: Ino = Ino(1);

/// Trash directory inode number.
pub const TRASH_INODE: Ino = Ino(0x7FFF_FFFF_1000_0000);

/// Inode number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Ino(pub u64);

impl Ino {
    /// Returns the raw `u64` inode value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns `true` if the inode is valid (`>= 1`).
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 >= ROOT_INODE.0
    }

    /// Returns `true` if this inode belongs to the `.trash` tree.
    #[must_use]
    pub const fn is_trash(self) -> bool {
        self.0 >= TRASH_INODE.0
    }

    /// Returns `true` if this is a normal user-visible inode.
    #[must_use]
    pub const fn is_normal(self) -> bool {
        self.0 >= ROOT_INODE.0 && self.0 < TRASH_INODE.0
    }
}

impl fmt::Display for Ino {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Ino {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Ino> for u64 {
    fn from(value: Ino) -> Self {
        value.0
    }
}

/// Filesystem node type, matching `JuiceFS` metadata constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeType {
    /// Regular file.
    File = 1,
    /// Directory.
    Directory = 2,
    /// Symbolic link.
    Symlink = 3,
    /// Named pipe (FIFO).
    Fifo = 4,
    /// Block device.
    BlockDev = 5,
    /// Character device.
    CharDev = 6,
    /// Unix domain socket.
    Socket = 7,
}

impl TryFrom<u8> for NodeType {
    type Error = CoreError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::File),
            2 => Ok(Self::Directory),
            3 => Ok(Self::Symlink),
            4 => Ok(Self::Fifo),
            5 => Ok(Self::BlockDev),
            6 => Ok(Self::CharDev),
            7 => Ok(Self::Socket),
            other => Err(CoreError::InvalidNodeType(other)),
        }
    }
}

/// POSIX node attributes stored in metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attr {
    /// Node flags (immutable, append, etc.).
    pub flags: u8,
    /// Node type.
    pub typ: NodeType,
    /// POSIX permission mode bits (without file type bits).
    pub mode: u16,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// Device ID for special files.
    pub rdev: u32,
    /// Access time (seconds since Unix epoch).
    pub atime: i64,
    /// Modification time (seconds since Unix epoch).
    pub mtime: i64,
    /// Status change time (seconds since Unix epoch).
    pub ctime: i64,
    /// Access time nanoseconds.
    pub atime_nsec: u32,
    /// Modification time nanoseconds.
    pub mtime_nsec: u32,
    /// Status change time nanoseconds.
    pub ctime_nsec: u32,
    /// Hard link count (or subdirectory count + 2 for directories).
    pub nlink: u32,
    /// File length in bytes.
    pub length: u64,
    /// Parent inode (0 for hardlinked files tracked separately).
    pub parent: Ino,
    /// Storage tier ID.
    pub tier: u8,
}

impl Default for Attr {
    fn default() -> Self {
        Self {
            flags: 0,
            typ: NodeType::File,
            mode: 0o644,
            uid: 0,
            gid: 0,
            rdev: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            atime_nsec: 0,
            mtime_nsec: 0,
            ctime_nsec: 0,
            nlink: 1,
            length: 0,
            parent: ROOT_INODE,
            tier: 0,
        }
    }
}

impl Attr {
    /// Returns `true` if this node is a directory.
    #[must_use]
    pub const fn is_dir(&self) -> bool {
        matches!(self.typ, NodeType::Directory)
    }

    /// Returns `true` if this node is a regular file.
    #[must_use]
    pub const fn is_file(&self) -> bool {
        matches!(self.typ, NodeType::File)
    }

    /// Returns `true` if this node is a symbolic link.
    #[must_use]
    pub const fn is_symlink(&self) -> bool {
        matches!(self.typ, NodeType::Symlink)
    }
}

/// Directory entry returned by `readdir`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Inode number of the entry.
    pub inode: Ino,
    /// Entry name (valid UTF-8 or lossy-decoded).
    pub name: String,
    /// Node attributes.
    pub attr: Attr,
}

/// Bitmask controlling which fields `setattr` updates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SetAttrFlags(u32);

impl SetAttrFlags {
    /// Update `mode`.
    pub const MODE: Self = Self(1 << 0);
    /// Update `uid`.
    pub const UID: Self = Self(1 << 1);
    /// Update `gid`.
    pub const GID: Self = Self(1 << 2);
    /// Update `length` (truncate/extend).
    pub const SIZE: Self = Self(1 << 3);
    /// Update `atime` from `attr.atime`.
    pub const ATIME: Self = Self(1 << 4);
    /// Update `mtime` from `attr.mtime`.
    pub const MTIME: Self = Self(1 << 5);
    /// Update `ctime` from `attr.ctime`.
    pub const CTIME: Self = Self(1 << 6);
    /// Set `atime` to current time.
    pub const ATIME_NOW: Self = Self(1 << 7);
    /// Set `mtime` to current time.
    pub const MTIME_NOW: Self = Self(1 << 8);
    /// Set `ctime` to current time.
    pub const CTIME_NOW: Self = Self(1 << 9);
    /// Update storage `tier`.
    pub const TIER: Self = Self(1 << 10);

    /// Combines two flag sets.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns `true` if all bits in `other` are set.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }
}

/// Filesystem capacity and inode usage summary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatFs {
    /// Total space in bytes.
    pub total_space: u64,
    /// Available space in bytes.
    pub avail_space: u64,
    /// Used inodes.
    pub used_inodes: u64,
    /// Available inodes.
    pub avail_inodes: u64,
}
