//! Amazon S3 and S3-compatible [`ObjectStorage`] backend powered by `object_store`.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use juicefs_core::Format;
use object_store::aws::AmazonS3Builder;
use url::Url;

use crate::cloud::{CloudStore, delegate_cloud_storage};
use crate::{Limits, MultipartUpload, ObjectError, ObjectMeta, ObjectStorage, Part};

const S3_MIN_PART_SIZE: u64 = 5 << 20;
const S3_MAX_PART_SIZE: u64 = 5 << 30;
const S3_MAX_PART_COUNT: u64 = 10_000;

/// S3-compatible object storage backend.
#[derive(Debug)]
pub struct S3Store(CloudStore);

impl S3Store {
    /// Builds an [`S3Store`] from a `JuiceFS` volume [`Format`].
    pub fn from_format(format: &Format) -> Result<Self, ObjectError> {
        let parsed = Url::parse(&format.bucket)
            .map_err(|e| ObjectError::Backend(format!("invalid s3 bucket url: {e}")))?;

        let bucket_name = parsed
            .host_str()
            .filter(|h| !h.is_empty())
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
        if parsed.scheme() == "http" {
            builder = builder.with_allow_http(true);
        }

        let store = builder
            .build()
            .map_err(|e| ObjectError::Backend(e.to_string()))?;

        let base_prefix = parsed.path().trim_matches('/').to_owned();
        let limits = Limits {
            is_support_multipart_upload: true,
            is_support_upload_part_copy: false,
            min_part_size: S3_MIN_PART_SIZE,
            max_part_size: S3_MAX_PART_SIZE,
            max_part_count: S3_MAX_PART_COUNT,
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

delegate_cloud_storage!(S3Store);
