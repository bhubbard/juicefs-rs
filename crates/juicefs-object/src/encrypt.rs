//! Client-side AES-256-GCM encryption wrapper for [`ObjectStorage`].

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::digest::{SHA256, digest};
use ring::rand::{SecureRandom, SystemRandom};

use crate::{Limits, ObjectError, ObjectMeta, ObjectStorage, validate_key};

/// Length in bytes of an AES-256 key.
pub const AES_256_KEY_LEN: usize = 32;

/// Wraps an [`ObjectStorage`] and transparently encrypts objects on `put` and
/// decrypts them on `get` using AES-256-GCM with a fresh random 96-bit nonce
/// per write.
///
/// Ciphertext layout: `[12-byte nonce][encrypted payload][16-byte GCM tag]`.
pub struct EncryptedStore {
    inner: Arc<dyn ObjectStorage>,
    key: LessSafeKey,
    rng: SystemRandom,
}

impl fmt::Debug for EncryptedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedStore")
            .field("inner", &self.inner)
            .field("key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl EncryptedStore {
    /// Creates an [`EncryptedStore`] using a 256-bit AES key.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStorage>, key: [u8; AES_256_KEY_LEN]) -> Self {
        let unbound = UnboundKey::new(&AES_256_GCM, &key)
            .expect("32-byte key is always valid for AES-256-GCM");
        Self {
            inner,
            key: LessSafeKey::new(unbound),
            rng: SystemRandom::new(),
        }
    }

    /// Creates an [`EncryptedStore`] from a raw key slice (must be 32 bytes).
    pub fn from_key_slice(inner: Arc<dyn ObjectStorage>, key: &[u8]) -> Result<Self, ObjectError> {
        let key_arr: [u8; AES_256_KEY_LEN] = key.try_into().map_err(|_| {
            ObjectError::Crypto(format!(
                "invalid AES-256 key length: expected {AES_256_KEY_LEN} bytes, got {}",
                key.len()
            ))
        })?;
        Ok(Self::new(inner, key_arr))
    }

    /// Creates an [`EncryptedStore`] by deriving a 256-bit key from `passphrase` via SHA-256.
    #[must_use]
    pub fn from_passphrase(inner: Arc<dyn ObjectStorage>, passphrase: impl AsRef<[u8]>) -> Self {
        let hash = digest(&SHA256, passphrase.as_ref());
        let mut key = [0u8; AES_256_KEY_LEN];
        key.copy_from_slice(hash.as_ref());
        Self::new(inner, key)
    }

    /// Returns a reference to the wrapped [`ObjectStorage`].
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn ObjectStorage> {
        &self.inner
    }

    fn encrypt(&self, plaintext: &[u8]) -> Result<Bytes, ObjectError> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        self.rng
            .fill(&mut nonce_bytes)
            .map_err(|_| ObjectError::Crypto("failed to generate random nonce".to_owned()))?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let tag_len = AES_256_GCM.tag_len();
        let mut buf = Vec::with_capacity(NONCE_LEN + plaintext.len() + tag_len);
        buf.extend_from_slice(&nonce_bytes);
        buf.extend_from_slice(plaintext);

        let tag = self
            .key
            .seal_in_place_separate_tag(nonce, Aad::empty(), &mut buf[NONCE_LEN..])
            .map_err(|_| ObjectError::Crypto("AES-256-GCM encryption failed".to_owned()))?;
        buf.extend_from_slice(tag.as_ref());

        Ok(Bytes::from(buf))
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Bytes, ObjectError> {
        let tag_len = AES_256_GCM.tag_len();
        if ciphertext.len() < NONCE_LEN + tag_len {
            return Err(ObjectError::Crypto(format!(
                "ciphertext too short: {} bytes (minimum {})",
                ciphertext.len(),
                NONCE_LEN + tag_len
            )));
        }

        let nonce = Nonce::try_assume_unique_for_key(&ciphertext[..NONCE_LEN])
            .map_err(|_| ObjectError::Crypto("invalid nonce length".to_owned()))?;
        let mut buf = ciphertext[NONCE_LEN..].to_vec();
        let plain_len = self
            .key
            .open_in_place(nonce, Aad::empty(), &mut buf)
            .map_err(|_| {
                ObjectError::Crypto(
                    "decryption failed: invalid key or corrupted ciphertext".to_owned(),
                )
            })?
            .len();
        buf.truncate(plain_len);
        Ok(Bytes::from(buf))
    }
}

#[async_trait]
impl ObjectStorage for EncryptedStore {
    fn description(&self) -> String {
        format!("{}(encrypted)", self.inner.description())
    }

    fn limits(&self) -> Limits {
        // Multipart parts cannot be independently authenticated/decrypted under whole-object AEAD.
        Limits::default()
    }

    async fn create(&self) -> Result<(), ObjectError> {
        self.inner.create().await
    }

    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        validate_key(key)?;
        let ciphertext = self.inner.get(key, 0, 0).await?;
        let plaintext = self.decrypt(&ciphertext)?;
        let total = plaintext.len();
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
        Ok(plaintext.slice(start..end))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
        validate_key(key)?;
        let ciphertext = self.encrypt(&data)?;
        self.inner.put(key, ciphertext).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
        self.inner.head(key).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        self.inner.delete(key).await
    }

    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
        self.inner.copy(dst, src).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
        self.inner.list(prefix).await
    }
}
