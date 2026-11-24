//! In-memory [`ObjectStorage`] implementation for testing and ephemeral volumes.

use std::collections::BTreeMap;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::{
    Limits, MultipartUpload, ObjectError, ObjectMeta, ObjectStorage, Part, sha256_hex,
    validate_key, validate_parts_order,
};

const MAX_PART_SIZE: u64 = 5 << 30;
const MAX_PART_COUNT: u64 = 10_000;

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    mtime: SystemTime,
}

#[derive(Debug, Default)]
struct MemoryMultipartUpload {
    key: String,
    parts: BTreeMap<usize, (Bytes, String)>,
}

/// Thread-safe in-memory object store.
#[derive(Debug, Default)]
pub struct MemoryStore {
    objects: RwLock<BTreeMap<String, StoredObject>>,
    uploads: RwLock<BTreeMap<String, MemoryMultipartUpload>>,
}

impl MemoryStore {
    /// Creates an empty in-memory object store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ObjectStorage for MemoryStore {
    fn description(&self) -> String {
        "mem://".to_owned()
    }

    fn limits(&self) -> Limits {
        Limits {
            is_support_multipart_upload: true,
            is_support_upload_part_copy: false,
            min_part_size: 1,
            max_part_size: MAX_PART_SIZE,
            max_part_count: MAX_PART_COUNT,
        }
    }

    async fn create(&self) -> Result<(), ObjectError> {
        Ok(())
    }

    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        validate_key(key)?;
        let guard = self.objects.read();
        let obj = guard
            .get(key)
            .ok_or_else(|| ObjectError::NotFound(key.to_owned()))?;
        let total = obj.data.len();
        let start = usize::try_from(offset).map_err(|_| ObjectError::InvalidRange {
            offset,
            size: total as u64,
        })?;
        if start > total {
            return Err(ObjectError::InvalidRange {
                offset,
                size: total as u64,
            });
        }
        let end = if limit == 0 {
            total
        } else {
            start.saturating_add(limit).min(total)
        };
        Ok(obj.data.slice(start..end))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
        validate_key(key)?;
        self.objects.write().insert(
            key.to_owned(),
            StoredObject {
                data,
                mtime: SystemTime::now(),
            },
        );
        Ok(())
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
        validate_key(key)?;
        let guard = self.objects.read();
        let obj = guard
            .get(key)
            .ok_or_else(|| ObjectError::NotFound(key.to_owned()))?;
        Ok(ObjectMeta {
            key: key.to_owned(),
            size: obj.data.len() as u64,
            mtime: obj.mtime,
            is_dir: key.ends_with('/'),
        })
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        validate_key(key)?;
        self.objects.write().remove(key);
        Ok(())
    }

    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
        validate_key(dst)?;
        validate_key(src)?;
        let mut guard = self.objects.write();
        let src_obj = guard
            .get(src)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound(src.to_owned()))?;
        guard.insert(
            dst.to_owned(),
            StoredObject {
                data: src_obj.data,
                mtime: SystemTime::now(),
            },
        );
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
        let guard = self.objects.read();
        let out = guard
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectMeta {
                key: k.clone(),
                size: v.data.len() as u64,
                mtime: v.mtime,
                is_dir: k.ends_with('/'),
            })
            .collect();
        Ok(out)
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<MultipartUpload, ObjectError> {
        validate_key(key)?;
        let upload_id = Uuid::new_v4().to_string();
        self.uploads.write().insert(
            upload_id.clone(),
            MemoryMultipartUpload {
                key: key.to_owned(),
                parts: BTreeMap::new(),
            },
        );
        Ok(MultipartUpload {
            upload_id,
            min_part_size: 1,
            max_count: MAX_PART_COUNT,
        })
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        num: usize,
        data: Bytes,
    ) -> Result<Part, ObjectError> {
        validate_key(key)?;
        if num == 0 {
            return Err(ObjectError::Backend(
                "multipart part number must be >= 1".to_owned(),
            ));
        }
        let mut uploads = self.uploads.write();
        let upload = uploads
            .get_mut(upload_id)
            .ok_or_else(|| ObjectError::NotFound(format!("upload {upload_id}")))?;
        if upload.key != key {
            return Err(ObjectError::Backend(format!(
                "upload {upload_id} belongs to key '{}', not '{key}'",
                upload.key
            )));
        }
        let etag = sha256_hex(&data);
        let size = data.len();
        upload.parts.insert(num, (data, etag.clone()));
        Ok(Part { num, size, etag })
    }

    async fn complete_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[Part],
    ) -> Result<(), ObjectError> {
        validate_key(key)?;
        validate_parts_order(parts)?;

        let assembled = {
            let mut uploads = self.uploads.write();
            let upload = uploads
                .get(upload_id)
                .ok_or_else(|| ObjectError::NotFound(format!("upload {upload_id}")))?;
            if upload.key != key {
                return Err(ObjectError::Backend(format!(
                    "upload {upload_id} belongs to key '{}', not '{key}'",
                    upload.key
                )));
            }

            let mut total_size = 0usize;
            for part in parts {
                let (data, etag) = upload.parts.get(&part.num).ok_or_else(|| {
                    ObjectError::NotFound(format!("upload {upload_id} part {}", part.num))
                })?;
                if etag != &part.etag || data.len() != part.size {
                    return Err(ObjectError::Backend(format!(
                        "part {} metadata mismatch for upload {upload_id}",
                        part.num
                    )));
                }
                total_size = total_size.saturating_add(data.len());
            }

            let mut buf = BytesMut::with_capacity(total_size);
            for part in parts {
                if let Some((data, _)) = upload.parts.get(&part.num) {
                    buf.extend_from_slice(data);
                }
            }
            uploads.remove(upload_id);
            buf.freeze()
        };

        self.objects.write().insert(
            key.to_owned(),
            StoredObject {
                data: assembled,
                mtime: SystemTime::now(),
            },
        );
        Ok(())
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), ObjectError> {
        validate_key(key)?;
        let mut uploads = self.uploads.write();
        if let Some(upload) = uploads.get(upload_id) {
            if upload.key != key {
                return Err(ObjectError::Backend(format!(
                    "upload {upload_id} belongs to key '{}', not '{key}'",
                    upload.key
                )));
            }
            uploads.remove(upload_id);
        }
        Ok(())
    }
}
