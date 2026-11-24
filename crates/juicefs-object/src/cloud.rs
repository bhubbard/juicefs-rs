//! Shared `object_store`-backed cloud storage adapter plus GCS and Azure Blob backends.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use juicefs_core::Format;
use object_store::ObjectStore as _;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjPath;
use parking_lot::Mutex;
use url::Url;
use uuid::Uuid;

use crate::{
    Limits, MultipartUpload, ObjectError, ObjectMeta, ObjectStorage, Part, sha256_hex,
    validate_key, validate_parts_order,
};

const CLOUD_MIN_PART_SIZE: u64 = 5 << 20;
const CLOUD_MAX_PART_SIZE: u64 = 5 << 30;
const GCS_MAX_PART_COUNT: u64 = 10_000;
const AZURE_MAX_PART_COUNT: u64 = 50_000;

#[derive(Debug)]
struct CloudMultipartSession {
    key: String,
    upload: Box<dyn object_store::MultipartUpload>,
    parts: BTreeMap<usize, (Bytes, String)>,
}

/// Shared adapter implementing [`ObjectStorage`] on top of any `object_store::ObjectStore`.
#[derive(Debug)]
pub(crate) struct CloudStore {
    inner: Arc<dyn object_store::ObjectStore>,
    bucket_url: String,
    base_prefix: String,
    limits: Limits,
    uploads: Mutex<HashMap<String, Arc<tokio::sync::Mutex<CloudMultipartSession>>>>,
}

impl CloudStore {
    pub(crate) fn new(
        inner: Arc<dyn object_store::ObjectStore>,
        bucket_url: String,
        base_prefix: String,
        limits: Limits,
    ) -> Self {
        Self {
            inner,
            bucket_url,
            base_prefix,
            limits,
            uploads: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn base_prefix(&self) -> &str {
        &self.base_prefix
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
impl ObjectStorage for CloudStore {
    fn description(&self) -> String {
        self.bucket_url.clone()
    }

    fn limits(&self) -> Limits {
        self.limits
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
        let session = Arc::new(tokio::sync::Mutex::new(CloudMultipartSession {
            key: key.to_owned(),
            upload,
            parts: BTreeMap::new(),
        }));
        self.uploads.lock().insert(upload_id.clone(), session);
        Ok(MultipartUpload {
            upload_id,
            min_part_size: self.limits.min_part_size,
            max_count: self.limits.max_part_count,
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

/// Forwards [`ObjectStorage`] methods to an inner `CloudStore` field (`self.0`).
macro_rules! delegate_cloud_storage {
    ($ty:ty) => {
        #[async_trait]
        impl ObjectStorage for $ty {
            fn description(&self) -> String {
                self.0.description()
            }

            fn limits(&self) -> Limits {
                self.0.limits()
            }

            async fn create(&self) -> Result<(), ObjectError> {
                self.0.create().await
            }

            async fn get(
                &self,
                key: &str,
                offset: u64,
                limit: usize,
            ) -> Result<Bytes, ObjectError> {
                self.0.get(key, offset, limit).await
            }

            async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
                self.0.put(key, data).await
            }

            async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
                self.0.head(key).await
            }

            async fn delete(&self, key: &str) -> Result<(), ObjectError> {
                self.0.delete(key).await
            }

            async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
                self.0.copy(dst, src).await
            }

            async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
                self.0.list(prefix).await
            }

            async fn create_multipart_upload(
                &self,
                key: &str,
            ) -> Result<MultipartUpload, ObjectError> {
                self.0.create_multipart_upload(key).await
            }

            async fn upload_part(
                &self,
                key: &str,
                upload_id: &str,
                num: usize,
                data: Bytes,
            ) -> Result<Part, ObjectError> {
                self.0.upload_part(key, upload_id, num, data).await
            }

            async fn complete_upload(
                &self,
                key: &str,
                upload_id: &str,
                parts: &[Part],
            ) -> Result<(), ObjectError> {
                self.0.complete_upload(key, upload_id, parts).await
            }

            async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), ObjectError> {
                self.0.abort_upload(key, upload_id).await
            }
        }
    };
}

pub(crate) use delegate_cloud_storage;

/// Google Cloud Storage (`GCS`) object storage backend.
#[derive(Debug)]
pub struct GcsStore(CloudStore);

impl GcsStore {
    /// Builds a [`GcsStore`] from a `JuiceFS` volume [`Format`].
    pub fn from_format(format: &Format) -> Result<Self, ObjectError> {
        let parsed = Url::parse(&format.bucket)
            .map_err(|e| ObjectError::Backend(format!("invalid gcs bucket url: {e}")))?;

        match parsed.scheme() {
            "gs" | "gcs" => {}
            other => {
                return Err(ObjectError::Backend(format!(
                    "unsupported gcs url scheme '{other}': expected gs:// or gcs://"
                )));
            }
        }

        let bucket_name = parsed
            .host_str()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| ObjectError::Backend("missing bucket name in gcs url".to_owned()))?;

        let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket_name);
        if format.secret_key.trim_start().starts_with('{') {
            builder = builder.with_service_account_key(&format.secret_key);
        }

        let store = builder
            .build()
            .map_err(|e| ObjectError::Backend(e.to_string()))?;

        let base_prefix = parsed.path().trim_matches('/').to_owned();
        let limits = Limits {
            is_support_multipart_upload: true,
            is_support_upload_part_copy: false,
            min_part_size: CLOUD_MIN_PART_SIZE,
            max_part_size: CLOUD_MAX_PART_SIZE,
            max_part_count: GCS_MAX_PART_COUNT,
        };

        Ok(Self(CloudStore::new(
            Arc::new(store),
            format.bucket.clone(),
            base_prefix,
            limits,
        )))
    }

    /// Returns the base object key prefix parsed from the bucket URL.
    #[must_use]
    pub fn base_prefix(&self) -> &str {
        self.0.base_prefix()
    }
}

delegate_cloud_storage!(GcsStore);

/// Microsoft Azure Blob Storage object storage backend.
#[derive(Debug)]
pub struct AzureStore(CloudStore);

impl AzureStore {
    /// Builds an [`AzureStore`] from a `JuiceFS` volume [`Format`].
    pub fn from_format(format: &Format) -> Result<Self, ObjectError> {
        let parsed = Url::parse(&format.bucket)
            .map_err(|e| ObjectError::Backend(format!("invalid azure bucket url: {e}")))?;

        let (container_name, url_account, base_prefix) = match parsed.scheme() {
            "azblob" | "azure" | "wasb" | "wasbs" => {
                let host = parsed.host_str().filter(|h| !h.is_empty()).ok_or_else(|| {
                    ObjectError::Backend("missing container name in azure url".to_owned())
                })?;
                let prefix = parsed.path().trim_matches('/').to_owned();
                if parsed.username().is_empty() {
                    (host.to_owned(), None, prefix)
                } else {
                    let account = host
                        .split('.')
                        .next()
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            ObjectError::Backend("missing account name in azure url".to_owned())
                        })?;
                    (
                        parsed.username().to_owned(),
                        Some(account.to_owned()),
                        prefix,
                    )
                }
            }
            "http" | "https" => {
                let host = parsed
                    .host_str()
                    .filter(|h| !h.is_empty())
                    .ok_or_else(|| ObjectError::Backend("missing host in azure url".to_owned()))?;
                let account = host
                    .split('.')
                    .next()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        ObjectError::Backend("missing account name in azure url".to_owned())
                    })?;
                let mut segments = parsed
                    .path_segments()
                    .into_iter()
                    .flatten()
                    .filter(|s| !s.is_empty());
                let container = segments
                    .next()
                    .ok_or_else(|| {
                        ObjectError::Backend("missing container name in azure url".to_owned())
                    })?
                    .to_owned();
                let prefix = segments.collect::<Vec<_>>().join("/");
                (container, Some(account.to_owned()), prefix)
            }
            other => {
                return Err(ObjectError::Backend(format!(
                    "unsupported azure url scheme '{other}': expected azblob://, wasb://, or azure://"
                )));
            }
        };

        let mut builder = MicrosoftAzureBuilder::from_env().with_container_name(container_name);
        if !format.access_key.is_empty() {
            builder = builder.with_account(&format.access_key);
        } else if let Some(account) = url_account {
            builder = builder.with_account(account);
        }
        if !format.secret_key.is_empty() {
            builder = builder.with_access_key(&format.secret_key);
        }
        if !format.session_token.is_empty() {
            builder = builder.with_bearer_token_authorization(&format.session_token);
        }
        if parsed.scheme() == "http" {
            builder = builder.with_allow_http(true);
        }

        let store = builder
            .build()
            .map_err(|e| ObjectError::Backend(e.to_string()))?;

        let limits = Limits {
            is_support_multipart_upload: true,
            is_support_upload_part_copy: false,
            min_part_size: CLOUD_MIN_PART_SIZE,
            max_part_size: CLOUD_MAX_PART_SIZE,
            max_part_count: AZURE_MAX_PART_COUNT,
        };

        Ok(Self(CloudStore::new(
            Arc::new(store),
            format.bucket.clone(),
            base_prefix,
            limits,
        )))
    }

    /// Returns the base object key prefix parsed from the container URL.
    #[must_use]
    pub fn base_prefix(&self) -> &str {
        self.0.base_prefix()
    }
}

delegate_cloud_storage!(AzureStore);
