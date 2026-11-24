//! Key-prefixing wrapper for [`ObjectStorage`].

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Limits, MultipartUpload, ObjectError, ObjectMeta, ObjectStorage, Part, validate_key};

/// Wraps an [`ObjectStorage`] and prepends `<prefix>/` to every key.
#[derive(Debug)]
pub struct PrefixedStore {
    inner: Arc<dyn ObjectStorage>,
    prefix: String,
}

impl PrefixedStore {
    /// Creates a new [`PrefixedStore`] with the given `prefix`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStorage>, prefix: impl AsRef<str>) -> Self {
        let trimmed = prefix.as_ref().trim_matches('/');
        let prefix = if trimmed.is_empty() {
            String::new()
        } else {
            format!("{trimmed}/")
        };
        Self { inner, prefix }
    }

    fn full_key(&self, key: &str) -> Result<String, ObjectError> {
        validate_key(key)?;
        Ok(format!("{}{}", self.prefix, key))
    }
}

#[async_trait]
impl ObjectStorage for PrefixedStore {
    fn description(&self) -> String {
        format!("{}{}", self.inner.description(), self.prefix)
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }

    async fn create(&self) -> Result<(), ObjectError> {
        self.inner.create().await
    }

    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        let fk = self.full_key(key)?;
        self.inner.get(&fk, offset, limit).await
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
        let fk = self.full_key(key)?;
        self.inner.put(&fk, data).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
        let fk = self.full_key(key)?;
        let mut meta = self.inner.head(&fk).await?;
        if let Some(stripped) = meta.key.strip_prefix(&self.prefix) {
            meta.key = stripped.to_owned();
        }
        Ok(meta)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        let fk = self.full_key(key)?;
        self.inner.delete(&fk).await
    }

    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
        let fdst = self.full_key(dst)?;
        let fsrc = self.full_key(src)?;
        self.inner.copy(&fdst, &fsrc).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
        let fp = format!("{}{}", self.prefix, prefix);
        let mut items = self.inner.list(&fp).await?;
        for item in &mut items {
            if let Some(stripped) = item.key.strip_prefix(&self.prefix) {
                item.key = stripped.to_owned();
            }
        }
        Ok(items)
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<MultipartUpload, ObjectError> {
        let fk = self.full_key(key)?;
        self.inner.create_multipart_upload(&fk).await
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        num: usize,
        data: Bytes,
    ) -> Result<Part, ObjectError> {
        let fk = self.full_key(key)?;
        self.inner.upload_part(&fk, upload_id, num, data).await
    }

    async fn complete_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[Part],
    ) -> Result<(), ObjectError> {
        let fk = self.full_key(key)?;
        self.inner.complete_upload(&fk, upload_id, parts).await
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), ObjectError> {
        let fk = self.full_key(key)?;
        self.inner.abort_upload(&fk, upload_id).await
    }
}
