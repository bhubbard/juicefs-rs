//! Slice binary encoding and interval-tree resolution for overlapping writes.
//!
//! Each slice record in `JuiceFS` metadata is 24 bytes in big-endian order:
//! `pos (u32) | id (u64) | size (u32) | off (u32) | len (u32)`.

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

use crate::error::CoreError;

/// Serialized size of a single slice record in bytes.
pub const SLICE_BYTES: usize = 24;

/// A resolved contiguous slice within a 64 MiB chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slice {
    /// Slice ID (`0` represents a sparse zero-filled hole).
    pub id: u64,
    /// Total length of the underlying slice object.
    pub size: u32,
    /// Byte offset within the underlying slice object.
    pub off: u32,
    /// Length of this slice segment in bytes.
    pub len: u32,
}

/// A raw slice record stored in chunk metadata, including its chunk offset `pos`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceRecord {
    /// Offset within the 64 MiB chunk.
    pub pos: u32,
    /// Slice ID (`0` for zeroed/sparse regions).
    pub id: u64,
    /// Total size of the slice.
    pub size: u32,
    /// Offset inside the slice.
    pub off: u32,
    /// Length of this write inside the chunk.
    pub len: u32,
}

/// Encodes a [`SliceRecord`] into the 24-byte big-endian `JuiceFS` wire format.
#[must_use]
pub fn encode_slice(rec: SliceRecord) -> [u8; SLICE_BYTES] {
    let mut buf = BytesMut::with_capacity(SLICE_BYTES);
    buf.put_u32(rec.pos);
    buf.put_u64(rec.id);
    buf.put_u32(rec.size);
    buf.put_u32(rec.off);
    buf.put_u32(rec.len);
    let mut out = [0u8; SLICE_BYTES];
    out.copy_from_slice(&buf);
    out
}

/// Decodes a sequence of 24-byte big-endian slice records from `buf`.
pub fn decode_slices(buf: &[u8]) -> Result<Vec<SliceRecord>, CoreError> {
    if !buf.len().is_multiple_of(SLICE_BYTES) {
        return Err(CoreError::CorruptSliceBuffer(buf.len()));
    }
    let count = buf.len() / SLICE_BYTES;
    let mut out = Vec::with_capacity(count);
    let mut cursor = buf;
    for _ in 0..count {
        let pos = cursor.get_u32();
        let id = cursor.get_u64();
        let size = cursor.get_u32();
        let off = cursor.get_u32();
        let len = cursor.get_u32();
        out.push(SliceRecord {
            pos,
            id,
            size,
            off,
            len,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone)]
struct TreeNode {
    pos: u32,
    id: u64,
    size: u32,
    off: u32,
    len: u32,
    left: Option<Box<TreeNode>>,
    right: Option<Box<TreeNode>>,
}

impl TreeNode {
    fn new(pos: u32, id: u64, size: u32, off: u32, len: u32) -> Option<Box<Self>> {
        if len == 0 {
            return None;
        }
        Some(Box::new(Self {
            pos,
            id,
            size,
            off,
            len,
            left: None,
            right: None,
        }))
    }
}

fn cut(node: Option<Box<TreeNode>>, pos: u32) -> (Option<Box<TreeNode>>, Option<Box<TreeNode>>) {
    let Some(mut s) = node else {
        return (None, None);
    };
    if pos <= s.pos {
        let left_child = s
            .left
            .take()
            .or_else(|| TreeNode::new(pos, 0, 0, 0, s.pos - pos));
        let (left, new_s_left) = cut(left_child, pos);
        s.left = new_s_left;
        (left, Some(s))
    } else if pos < s.pos + s.len {
        let l = pos - s.pos;
        let mut right = TreeNode {
            pos,
            id: s.id,
            size: s.size,
            off: s.off + l,
            len: s.len - l,
            left: None,
            right: s.right.take(),
        };
        s.len = l;
        right.left = None;
        (Some(s), Some(Box::new(right)))
    } else {
        let right_child = s
            .right
            .take()
            .or_else(|| TreeNode::new(s.pos + s.len, 0, 0, 0, pos - s.pos - s.len));
        let (new_s_right, right) = cut(right_child, pos);
        s.right = new_s_right;
        (Some(s), right)
    }
}

fn visit(node: Option<&TreeNode>, pos: &mut u32, out: &mut Vec<Slice>) {
    let Some(s) = node else {
        return;
    };
    visit(s.left.as_deref(), pos, out);
    if s.pos > *pos {
        let gap = s.pos - *pos;
        out.push(Slice {
            id: 0,
            size: gap,
            off: 0,
            len: gap,
        });
        *pos = s.pos;
    }
    out.push(Slice {
        id: s.id,
        size: s.size,
        off: s.off,
        len: s.len,
    });
    *pos += s.len;
    visit(s.right.as_deref(), pos, out);
}

/// Resolves an ordered list of slice writes into non-overlapping contiguous [`Slice`] segments.
///
/// Later records overwrite earlier ones at overlapping byte ranges, and gaps are
/// represented as sparse slices with `id == 0`.
#[must_use]
pub fn build_slice(records: &[SliceRecord]) -> Vec<Slice> {
    let mut root: Option<Box<TreeNode>> = None;
    for &rec in records {
        if rec.len == 0 {
            continue;
        }
        let (left, right_tree) = cut(root, rec.pos);
        let (_, right) = cut(right_tree, rec.pos + rec.len);
        root = Some(Box::new(TreeNode {
            pos: rec.pos,
            id: rec.id,
            size: rec.size,
            off: rec.off,
            len: rec.len,
            left,
            right,
        }));
    }
    let mut pos = 0u32;
    let mut out = Vec::new();
    visit(root.as_deref(), &mut pos, &mut out);
    out
}

/// Compacts slice records by trimming leading and trailing sparse (`id == 0`) regions.
///
/// Returns `(start_pos, total_len, slices)`.
#[must_use]
pub fn compact_chunk(records: &[SliceRecord]) -> (u32, u32, Vec<Slice>) {
    let mut chunk = build_slice(records);
    let mut pos = 0u32;
    while chunk.len() > 1 {
        if chunk[0].id == 0 {
            pos += chunk[0].len;
            chunk.remove(0);
        } else if chunk[chunk.len() - 1].id == 0 {
            chunk.pop();
        } else {
            break;
        }
    }
    if chunk.len() == 1 && chunk[0].id == 0 {
        chunk[0].len = 1;
    }
    let size: u32 = chunk.iter().map(|s| s.len).sum();
    (pos, size, chunk)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_slice_encoding() {
        let rec = SliceRecord {
            pos: 4096,
            id: 42,
            size: 8192,
            off: 1024,
            len: 4096,
        };
        let encoded = encode_slice(rec);
        let decoded = decode_slices(&encoded).expect("valid 24-byte buffer");
        assert_eq!(decoded, vec![rec]);
    }

    #[test]
    fn build_slice_resolves_overlaps_and_holes() {
        let records = vec![
            SliceRecord {
                pos: 10,
                id: 1,
                size: 20,
                off: 0,
                len: 20,
            },
            SliceRecord {
                pos: 15,
                id: 2,
                size: 10,
                off: 0,
                len: 10,
            },
        ];
        let slices = build_slice(&records);
        assert_eq!(
            slices,
            vec![
                Slice {
                    id: 0,
                    size: 10,
                    off: 0,
                    len: 10
                },
                Slice {
                    id: 1,
                    size: 20,
                    off: 0,
                    len: 5
                },
                Slice {
                    id: 2,
                    size: 10,
                    off: 0,
                    len: 10
                },
                Slice {
                    id: 1,
                    size: 20,
                    off: 15,
                    len: 5
                },
            ]
        );
    }
}
