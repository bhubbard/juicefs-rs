//! Core domain types, slice interval algebra, configuration, and compression for `JuiceFS`.

pub mod compress;
pub mod config;
pub mod error;
pub mod slice;
pub mod types;

pub use compress::CompressionAlgo;
pub use config::{ChunkConfig, ClientConfig, Format, TierConfig};
pub use error::CoreError;
pub use slice::{
    SLICE_BYTES, Slice, SliceRecord, build_slice, compact_chunk, decode_slices, encode_slice,
};
pub use types::{
    Attr, CHUNK_BITS, CHUNK_SIZE, DEFAULT_BLOCK_SIZE, Entry, Ino, MAX_NAME, MAX_SYMLINK, NodeType,
    PAGE_SIZE, ROOT_INODE, SetAttrFlags, StatFs, TRASH_INODE,
};
