//! Local filesystem [`ObjectStorage`] backend.

use std::fmt::Write as _;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

use crate::{
    Limits, MultipartUpload, ObjectError, ObjectMeta, ObjectStorage, Part, sha256_hex,
    validate_key, validate_parts_order,
};

const MULTIPART_DIR: &str = ".juicefs-multipart";
const MAX_PART_SIZE: u64 = 5 << 30;
const MAX_PART_COUNT: u64 = 10_000;

/// Local directory-backed object store.
#[derive(Debug, Clone)]
pub struct FileStore {
    root: PathBuf,
    inplace: bool,
}

impl FileStore {
    /// Creates a [`FileStore`] rooted at `root`.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ObjectError> {
        let root = root.as_ref().to_path_buf();
        Ok(Self {
            root,
            inplace: false,
        })
    }

    /// Enables or disables direct in-place writes (skipping temp-file + `fsync` + rename).
    #[must_use]
    pub const fn with_inplace(mut self, inplace: bool) -> Self {
        self.inplace = inplace;
        self
    }

    fn resolve_path(&self, key: &str) -> Result<PathBuf, ObjectError> {
        validate_key(key)?;
        Ok(self.root.join(key))
    }

    fn upload_dir(&self, upload_id: &str) -> Result<PathBuf, ObjectError> {
        validate_upload_id(upload_id)?;
        Ok(self.root.join(MULTIPART_DIR).join(upload_id))
    }
}

fn validate_upload_id(upload_id: &str) -> Result<(), ObjectError> {
    if upload_id.is_empty()
        || upload_id.contains('/')
        || upload_id.contains('\\')
        || upload_id.contains("..")
    {
        return Err(ObjectError::InvalidKey(upload_id.to_owned()));
    }
    Ok(())
}

async fn verify_upload_key(
    upload_dir: &Path,
    upload_id: &str,
    key: &str,
) -> Result<(), ObjectError> {
    let key_path = upload_dir.join("key");
    let recorded = match fs::read_to_string(&key_path).await {
        Ok(k) => k,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ObjectError::NotFound(format!("upload {upload_id}")));
        }
        Err(e) => return Err(e.into()),
    };
    if recorded != key {
        return Err(ObjectError::Backend(format!(
            "upload {upload_id} belongs to key '{recorded}', not '{key}'"
        )));
    }
    Ok(())
}

async fn write_atomic(path: &Path, data: &[u8]) -> Result<(), ObjectError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let tmp_name = format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("obj"),
        Uuid::new_v4()
    );
    let tmp_path = path.with_file_name(tmp_name);
    let mut file = fs::File::create(&tmp_path).await?;
    if let Err(e) = async {
        file.write_all(data).await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(&tmp_path, path).await
    }
    .await
    {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(e.into());
    }
    Ok(())
}

#[async_trait]
impl ObjectStorage for FileStore {
    fn description(&self) -> String {
        format!("file://{}", self.root.display())
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
        fs::create_dir_all(&self.root).await?;
        Ok(())
    }

    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        let path = self.resolve_path(key)?;
        let mut file = match fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectError::NotFound(key.to_owned()));
            }
            Err(e) => return Err(e.into()),
        };
        let meta = file.metadata().await?;
        let total = meta.len();
        if offset > total {
            return Err(ObjectError::InvalidRange {
                offset,
                size: total,
            });
        }
        file.seek(SeekFrom::Start(offset)).await?;
        if limit == 0 {
            let mut buf = Vec::with_capacity((total - offset) as usize);
            file.read_to_end(&mut buf).await?;
            Ok(Bytes::from(buf))
        } else {
            let to_read = limit.min((total - offset) as usize);
            let mut buf = vec![0u8; to_read];
            file.read_exact(&mut buf).await?;
            Ok(Bytes::from(buf))
        }
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
        let path = self.resolve_path(key)?;
        if self.inplace {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await?;
            }
            fs::write(&path, &data).await?;
            return Ok(());
        }
        write_atomic(&path, &data).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
        let path = self.resolve_path(key)?;
        let meta = match fs::metadata(&path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectError::NotFound(key.to_owned()));
            }
            Err(e) => return Err(e.into()),
        };
        Ok(ObjectMeta {
            key: key.to_owned(),
            size: meta.len(),
            mtime: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            is_dir: meta.is_dir(),
        })
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        let path = self.resolve_path(key)?;
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
        let src_path = self.resolve_path(src)?;
        let dst_path = self.resolve_path(dst)?;
        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        match fs::copy(&src_path, &dst_path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ObjectError::NotFound(src.to_owned()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
        let mut results = Vec::new();
        if !self.root.exists() {
            return Ok(results);
        }
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let mut entries = fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if dir == self.root && entry.file_name() == MULTIPART_DIR {
                    continue;
                }
                let meta = entry.metadata().await?;
                if meta.is_dir() {
                    stack.push(path);
                    continue;
                }
                let rel = path
                    .strip_prefix(&self.root)
                    .map_err(|e| ObjectError::Backend(e.to_string()))?;
                let key = rel.to_string_lossy().replace('\\', "/");
                if key.starts_with(prefix) {
                    results.push(ObjectMeta {
                        key,
                        size: meta.len(),
                        mtime: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                        is_dir: false,
                    });
                }
            }
        }
        results.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(results)
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<MultipartUpload, ObjectError> {
        validate_key(key)?;
        let upload_id = Uuid::new_v4().to_string();
        let upload_dir = self.upload_dir(&upload_id)?;
        fs::create_dir_all(&upload_dir).await?;
        write_atomic(&upload_dir.join("key"), key.as_bytes()).await?;
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
        let upload_dir = self.upload_dir(upload_id)?;
        verify_upload_key(&upload_dir, upload_id, key).await?;

        let part_path = upload_dir.join(format!("part-{num}"));
        write_atomic(&part_path, &data).await?;
        let etag = sha256_hex(&data);
        Ok(Part {
            num,
            size: data.len(),
            etag,
        })
    }

    async fn complete_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[Part],
    ) -> Result<(), ObjectError> {
        let dst_path = self.resolve_path(key)?;
        validate_parts_order(parts)?;
        let upload_dir = self.upload_dir(upload_id)?;
        verify_upload_key(&upload_dir, upload_id, key).await?;

        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let tmp_name = format!(
            ".{}.tmp.{}",
            dst_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("obj"),
            Uuid::new_v4()
        );
        let tmp_path = dst_path.with_file_name(tmp_name);
        let mut dst_file = fs::File::create(&tmp_path).await?;

        let assemble_res: Result<(), ObjectError> = async {
            let mut copy_buf = vec![0u8; 64 * 1024];
            for part in parts {
                let part_path = upload_dir.join(format!("part-{}", part.num));
                let mut part_file = match fs::File::open(&part_path).await {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Err(ObjectError::NotFound(format!(
                            "upload {upload_id} part {}",
                            part.num
                        )));
                    }
                    Err(e) => return Err(e.into()),
                };
                let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
                let mut part_size = 0usize;
                loop {
                    let n = part_file.read(&mut copy_buf).await?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&copy_buf[..n]);
                    dst_file.write_all(&copy_buf[..n]).await?;
                    part_size = part_size.saturating_add(n);
                }
                let digest = hasher.finish();
                let mut etag = String::with_capacity(digest.as_ref().len() * 2);
                for byte in digest.as_ref() {
                    let _ = write!(etag, "{byte:02x}");
                }
                if part_size != part.size || etag != part.etag {
                    return Err(ObjectError::Backend(format!(
                        "part {} metadata mismatch for upload {upload_id}",
                        part.num
                    )));
                }
            }
            dst_file.sync_all().await?;
            drop(dst_file);
            fs::rename(&tmp_path, &dst_path).await?;
            Ok(())
        }
        .await;

        if let Err(e) = assemble_res {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(e);
        }

        let _ = fs::remove_dir_all(&upload_dir).await;
        Ok(())
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), ObjectError> {
        validate_key(key)?;
        let upload_dir = self.upload_dir(upload_id)?;
        match verify_upload_key(&upload_dir, upload_id, key).await {
            Ok(()) => {}
            Err(ObjectError::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        }
        match fs::remove_dir_all(&upload_dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn restore(&self, key: &str, _days: i32) -> Result<(), ObjectError> {
        let path = self.resolve_path(key)?;
        match fs::metadata(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ObjectError::NotFound(key.to_owned()))
            }
            Err(e) => Err(e.into()),
        }
    }
}
