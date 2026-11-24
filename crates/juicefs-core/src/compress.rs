//! Compression algorithms supported by `JuiceFS` blocks.

use std::str::FromStr;

use crate::error::CoreError;

/// Compression algorithm for object storage blocks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum CompressionAlgo {
    /// No compression.
    #[default]
    None,
    /// LZ4 block compression.
    Lz4,
    /// Zstandard compression (level 1).
    Zstd,
}

impl CompressionAlgo {
    /// Returns the canonical lowercase name stored in `Format`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    /// Compresses `src` according to the configured algorithm.
    pub fn compress(self, src: &[u8]) -> Result<Vec<u8>, CoreError> {
        match self {
            Self::None => Ok(src.to_vec()),
            Self::Lz4 => Ok(lz4_flex::block::compress(src)),
            Self::Zstd => {
                zstd::bulk::compress(src, 1).map_err(|e| CoreError::Compression(e.to_string()))
            }
        }
    }

    /// Decompresses `src` into a buffer of at most `max_uncompressed_len` bytes.
    pub fn decompress(self, src: &[u8], max_uncompressed_len: usize) -> Result<Vec<u8>, CoreError> {
        match self {
            Self::None => Ok(src.to_vec()),
            Self::Lz4 => {
                if src.is_empty() {
                    return Err(CoreError::Compression(
                        "decompress an empty input".to_owned(),
                    ));
                }
                lz4_flex::block::decompress(src, max_uncompressed_len)
                    .map_err(|e| CoreError::Compression(e.to_string()))
            }
            Self::Zstd => {
                if src.is_empty() {
                    return Err(CoreError::Compression(
                        "decompress an empty input".to_owned(),
                    ));
                }
                zstd::bulk::decompress(src, max_uncompressed_len)
                    .map_err(|e| CoreError::Compression(e.to_string()))
            }
        }
    }
}

impl FromStr for CompressionAlgo {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "" | "none" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            other => Err(CoreError::UnsupportedCompression(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_roundtrip_all_algos() {
        let payload = b"juicefs-rust-block-payload-".repeat(64);
        for algo in [
            CompressionAlgo::None,
            CompressionAlgo::Lz4,
            CompressionAlgo::Zstd,
        ] {
            let compressed = algo.compress(&payload).expect("compress");
            let restored = algo
                .decompress(&compressed, payload.len())
                .expect("decompress");
            assert_eq!(restored, payload, "algo {algo:?}");
        }
    }
}
