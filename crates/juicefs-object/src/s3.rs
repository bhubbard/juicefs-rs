//! Amazon S3 and S3-compatible [`ObjectStorage`] backend powered by `object_store`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use juicefs_core::Format;
use object_store::ObjectStore as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use parking_lot::Mutex;
use url::Url;
use uuid::Uuid;

use crate::{
    Limits, MultipartUpload, ObjectError, ObjectMeta, ObjectStorage, Part, sha256_hex,
    validate_key, validate_parts_order,
};

const S3_MIN_PART_SIZE: u64 = 5 << 20;
const S3_MAX_PART_SIZE: u64 = 5 << 30;
const S3_MAX_PART_COUNT: u64 = 10_000;

#[derive(Debug)]
struct S3MultipartSession {
    key: String,
    upload: Box<dyn object_store::MultipartUpload>,
    parts: BTreeMap<usize, (Bytes, String)>,
}

/// S3-compatible object storage backend.
#[derive(Debug)]
pub struct S3Store {
    inner: Arc<dyn object_store::ObjectStore>,
    bucket_url: String,
    base_prefix: String,
    uploads: Mutex<HashMap<String, Arc<tokio::sync::Mutex<S3MultipartSession>>>>,
}

impl S3Store {
    /// Builds an [`S3Store`] from a `JuiceFS` volume [`Format`].
    pub fn from_format(format: &Format) -> Result<Self, ObjectError> {
        let parsed = Url::parse(&format.bucket)
            .map_err(|e| ObjectError::Backend(format!("invalid s3 bucket url: {e}")))?;

        let bucket_name = parsed
            .host_str()
            .ok_or_else(|| ObjectError::Backend("missing bucket name in s3 url".to_owned()))?;

        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);

        if !format.access_key.is_empty() {
            builder = builder.with_access_key_id(&format.access_key);
        }
        if !format.secret_key.is_empty() {
            builder = builder.with_secret_access_key(&format.secret_key);
        }
        if !format.session_token.is_empty() {
            builder = builder.with_token(&format.session_token);
        }
        if let Some(port) = parsed.port() {
            let scheme = if parsed.scheme() == "s3" {
                "https"
            } else {
                parsed.scheme()
            };
            let endpoint = format!("{scheme}://{bucket_name}:{port}");
            builder = builder.with_endpoint(endpoint);
        }

        let store = builder
            .build()
            .map_err(|e| ObjectError::Backend(e.to_string()))?;

        let base_prefix = parsed.path().trim_matches('/').to_owned();
        Ok(Self {
            inner: Arc::new(store),
            bucket_url: format.bucket.clone(),
            base_prefix,
            uploads: Mutex::new(HashMap::new()),
        })
    }

    fn obj_path(&self, key: &str) -> Result<ObjPath, ObjectError> {
        validate_key(key)?;
        let full = if self.base_prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.base_prefix)
        };
        ObjPath::parse(&full).map_err(|e| ObjectError::InvalidKey(e.to_string()))
    }
}

fn map_err(err: object_store::Error, key: &str) -> ObjectError {
    match err {
        object_store::Error::NotFound { .. } => ObjectError::NotFound(key.to_owned()),
        other => ObjectError::Backend(other.to_string()),
    }
}

#[async_trait]
impl ObjectStorage for S3Store {
    fn description(&self) -> String {
        self.bucket_url.clone()
    }

    fn limits(&self) -> Limits {
        Limits {
            is_support_multipart_upload: true,
            is_support_upload_part_copy: false,
            min_part_size: S3_MIN_PART_SIZE,
            max_part_size: S3_MAX_PART_SIZE,
            max_part_count: S3_MAX_PART_COUNT,
        }
    }

    async fn create(&self) -> Result<(), ObjectError> {
        Ok(())
    }

    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        let path = self.obj_path(key)?;
        if offset == 0 && limit == 0 {
            let res = self.inner.get(&path).await.map_err(|e| map_err(e, key))?;
            res.bytes().await.map_err(|e| map_err(e, key))
        } else if limit == 0 {
            let meta = self.head(key).await?;
            if offset >= meta.size {
                return Ok(Bytes::new());
            }
            self.inner
                .get_range(&path, offset..meta.size)
                .await
                .map_err(|e| map_err(e, key))
        } else {
            let end = offset.saturating_add(limit as u64);
            self.inner
                .get_range(&path, offset..end)
                .await
                .map_err(|e| map_err(e, key))
        }
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
        let path = self.obj_path(key)?;
        self.inner
            .put(&path, data.into())
            .await
            .map_err(|e| map_err(e, key))?;
        Ok(())
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
        let path = self.obj_path(key)?;
        let meta = self.inner.head(&path).await.map_err(|e| map_err(e, key))?;
        Ok(ObjectMeta {
            key: key.to_owned(),
            size: meta.size,
            mtime: SystemTime::from(meta.last_modified),
            is_dir: false,
        })
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        let path = self.obj_path(key)?;
        match self.inner.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_err(e, key)),
        }
    }

    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
        let src_path = self.obj_path(src)?;
        let dst_path = self.obj_path(dst)?;
        self.inner
            .copy(&src_path, &dst_path)
            .await
            .map_err(|e| map_err(e, src))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
        let full_prefix = if self.base_prefix.is_empty() {
            prefix.to_owned()
        } else if prefix.is_empty() {
            self.base_prefix.clone()
        } else {
            format!("{}/{prefix}", self.base_prefix)
        };
        let obj_prefix =
            ObjPath::parse(&full_prefix).map_err(|e| ObjectError::InvalidKey(e.to_string()))?;
        let items: Vec<object_store::ObjectMeta> = self
            .inner
            .list(Some(&obj_prefix))
            .try_collect()
            .await
            .map_err(|e| ObjectError::Backend(e.to_string()))?;

        let strip_prefix = if self.base_prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.base_prefix)
        };

        Ok(items
            .into_iter()
            .map(|m| {
                let raw = m.location.to_string();
                let key = raw.strip_prefix(&strip_prefix).unwrap_or(&raw).to_owned();
                ObjectMeta {
                    key,
                    size: m.size,
                    mtime: SystemTime::from(m.last_modified),
                    is_dir: false,
                }
            })
            .collect())
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<MultipartUpload, ObjectError> {
        let path = self.obj_path(key)?;
        let upload = self
            .inner
            .put_multipart(&path)
            .await
            .map_err(|e| map_err(e, key))?;
        let upload_id = Uuid::new_v4().to_string();
        let session = Arc::new(tokio::sync::Mutex::new(S3MultipartSession {
            key: key.to_owned(),
            upload,
            parts: BTreeMap::new(),
        }));
        self.uploads.lock().insert(upload_id.clone(), session);
        Ok(MultipartUpload {
            upload_id,
            min_part_size: S3_MIN_PART_SIZE,
            max_count: S3_MAX_PART_COUNT,
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
        let session = self
            .uploads
            .lock()
            .get(upload_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound(format!("upload {upload_id}")))?;
        let mut guard = session.lock().await;
        if guard.key != key {
            return Err(ObjectError::Backend(format!(
                "upload {upload_id} belongs to key '{}', not '{key}'",
                guard.key
            )));
        }
        let etag = sha256_hex(&data);
        let size = data.len();
        guard.parts.insert(num, (data, etag.clone()));
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
        let session = self
            .uploads
            .lock()
            .get(upload_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound(format!("upload {upload_id}")))?;
        let mut guard = session.lock().await;
        if guard.key != key {
            return Err(ObjectError::Backend(format!(
                "upload {upload_id} belongs to key '{}', not '{key}'",
                guard.key
            )));
        }

        let mut payloads = Vec::with_capacity(parts.len());
        for part in parts {
            let (data, etag) = guard.parts.get(&part.num).ok_or_else(|| {
                ObjectError::NotFound(format!("upload {upload_id} part {}", part.num))
            })?;
            if etag != &part.etag || data.len() != part.size {
                return Err(ObjectError::Backend(format!(
                    "part {} metadata mismatch for upload {upload_id}",
                    part.num
                )));
            }
            payloads.push(data.clone());
        }

        for data in payloads {
            guard
                .upload
                .put_part(data.into())
                .await
                .map_err(|e| map_err(e, key))?;
        }
        guard.upload.complete().await.map_err(|e| map_err(e, key))?;
        drop(guard);
        self.uploads.lock().remove(upload_id);
        Ok(())
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), ObjectError> {
        validate_key(key)?;
        let session = self.uploads.lock().get(upload_id).cloned();
        let Some(session) = session else {
            return Ok(());
        };
        let mut guard = session.lock().await;
        if guard.key != key {
            return Err(ObjectError::Backend(format!(
                "upload {upload_id} belongs to key '{}', not '{key}'",
                guard.key
            )));
        }
        let res = guard.upload.abort().await.map_err(|e| map_err(e, key));
        drop(guard);
        self.uploads.lock().remove(upload_id);
        res
    }
}
