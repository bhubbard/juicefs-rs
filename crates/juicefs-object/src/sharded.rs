//! Consistent key-sharding wrapper across multiple [`ObjectStorage`] backends.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::try_join_all;

use crate::{Limits, MultipartUpload, ObjectError, ObjectMeta, ObjectStorage, Part, validate_key};

const FNV1A_32_OFFSET_BASIS: u32 = 0x811c_9dc5;
const FNV1A_32_PRIME: u32 = 0x0100_0193;

/// Computes the 32-bit FNV-1a hash matching Go's `hash/fnv.New32a`.
#[must_use]
pub fn fnv1a_32(bytes: &[u8]) -> u32 {
    let mut hash = FNV1A_32_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV1A_32_PRIME);
    }
    hash
}

/// Distributes objects across multiple [`ObjectStorage`] shards deterministically
/// using 32-bit FNV-1a hashing (`hash % shards.len()`), and fans out `list`
/// across all shards with merge-sorted output.
#[derive(Debug, Clone)]
pub struct ShardedStore {
    shards: Vec<Arc<dyn ObjectStorage>>,
}

impl ShardedStore {
    /// Creates a [`ShardedStore`] over `shards`.
    ///
    /// Returns [`ObjectError::Backend`] if `shards` is empty.
    pub fn new(shards: Vec<Arc<dyn ObjectStorage>>) -> Result<Self, ObjectError> {
        if shards.is_empty() {
            return Err(ObjectError::Backend(
                "sharded store requires at least one shard".to_owned(),
            ));
        }
        Ok(Self { shards })
    }

    /// Returns the number of configured shards.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns a slice of all underlying shard stores.
    #[must_use]
    pub fn shards(&self) -> &[Arc<dyn ObjectStorage>] {
        &self.shards
    }

    /// Computes the deterministic shard index for `key`.
    #[must_use]
    pub fn shard_index(&self, key: &str) -> usize {
        (fnv1a_32(key.as_bytes()) as usize) % self.shards.len()
    }

    fn pick(&self, key: &str) -> &Arc<dyn ObjectStorage> {
        &self.shards[self.shard_index(key)]
    }
}

#[async_trait]
impl ObjectStorage for ShardedStore {
    fn description(&self) -> String {
        format!(
            "shard{}://{}",
            self.shards.len(),
            self.shards[0].description()
        )
    }

    fn limits(&self) -> Limits {
        let mut limits = self.shards[0].limits();
        limits.is_support_upload_part_copy = false;
        limits
    }

    async fn create(&self) -> Result<(), ObjectError> {
        try_join_all(self.shards.iter().map(|shard| shard.create())).await?;
        Ok(())
    }

    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        validate_key(key)?;
        self.pick(key).get(key, offset, limit).await
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
        validate_key(key)?;
        self.pick(key).put(key, data).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
        validate_key(key)?;
        self.pick(key).head(key).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        validate_key(key)?;
        self.pick(key).delete(key).await
    }

    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
        validate_key(dst)?;
        validate_key(src)?;
        Err(ObjectError::Unsupported(
            "copy is not supported on sharded store".to_owned(),
        ))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
        let shard_lists = try_join_all(self.shards.iter().map(|shard| shard.list(prefix))).await?;
        let total_len = shard_lists.iter().map(Vec::len).sum();
        let mut merged = Vec::with_capacity(total_len);
        for items in shard_lists {
            merged.extend(items);
        }
        merged.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(merged)
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<MultipartUpload, ObjectError> {
        validate_key(key)?;
        self.pick(key).create_multipart_upload(key).await
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        num: usize,
        data: Bytes,
    ) -> Result<Part, ObjectError> {
        validate_key(key)?;
        self.pick(key).upload_part(key, upload_id, num, data).await
    }

    async fn complete_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[Part],
    ) -> Result<(), ObjectError> {
        validate_key(key)?;
        self.pick(key).complete_upload(key, upload_id, parts).await
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), ObjectError> {
        validate_key(key)?;
        self.pick(key).abort_upload(key, upload_id).await
    }
}
