//! Chunked client-side AEAD encryption wrapper for [`ObjectStorage`].
//!
//! Splits objects into fixed-size plaintext chunks (`CHUNK_SIZE = 64 KiB`),
//! encrypting each chunk independently with a fresh random 96-bit nonce and
//! AEAD authentication tag so range reads only fetch and decrypt overlapping
//! chunks.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use ring::aead::{
    AES_256_GCM, Aad, Algorithm, CHACHA20_POLY1305, LessSafeKey, NONCE_LEN, Nonce, UnboundKey,
};
use ring::digest::{SHA256, digest};
use ring::rand::{SecureRandom, SystemRandom};

use crate::{Limits, ObjectError, ObjectMeta, ObjectStorage, validate_key};

/// Plaintext chunk size in bytes (64 KiB).
pub const CHUNK_SIZE: usize = 64 * 1024;

/// Required key length in bytes for both AES-256-GCM and ChaCha20-Poly1305.
pub const CHUNK_KEY_LEN: usize = 32;

/// AEAD authentication tag length in bytes (16 bytes for both supported ciphers).
const TAG_LEN: usize = 16;

/// Per-chunk encryption overhead in bytes (`12`-byte nonce + `16`-byte tag).
const CHUNK_OVERHEAD: usize = NONCE_LEN + TAG_LEN;

/// Magic prefix identifying chunked-encrypted objects (`"JFSC"`).
const HEADER_MAGIC: [u8; 4] = *b"JFSC";

/// Encoded header length in bytes: `magic (4) + algo (1) + chunk_size (4) + total_plaintext_len (8)`.
pub const HEADER_SIZE: usize = 17;

/// AEAD cipher algorithm supported by [`ChunkedEncryptedStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkedCipher {
    /// AES-256-GCM with 96-bit nonces and 128-bit tags.
    Aes256Gcm,
    /// ChaCha20-Poly1305 with 96-bit nonces and 128-bit tags.
    ChaCha20Poly1305,
}

impl ChunkedCipher {
    const fn algorithm(self) -> &'static Algorithm {
        match self {
            Self::Aes256Gcm => &AES_256_GCM,
            Self::ChaCha20Poly1305 => &CHACHA20_POLY1305,
        }
    }

    const fn id(self) -> u8 {
        match self {
            Self::Aes256Gcm => 1,
            Self::ChaCha20Poly1305 => 2,
        }
    }

    fn from_id(id: u8) -> Result<Self, ObjectError> {
        match id {
            1 => Ok(Self::Aes256Gcm),
            2 => Ok(Self::ChaCha20Poly1305),
            _ => Err(ObjectError::Crypto(format!(
                "unknown chunked cipher algorithm id: {id}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChunkHeader {
    cipher: ChunkedCipher,
    chunk_size: u32,
    total_plaintext_len: u64,
}

impl ChunkHeader {
    fn encode(self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&HEADER_MAGIC);
        buf[4] = self.cipher.id();
        buf[5..9].copy_from_slice(&self.chunk_size.to_be_bytes());
        buf[9..17].copy_from_slice(&self.total_plaintext_len.to_be_bytes());
        buf
    }

    fn decode(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() < HEADER_SIZE {
            return Err(ObjectError::Crypto(format!(
                "truncated chunked header: expected {HEADER_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        if bytes[0..4] != HEADER_MAGIC {
            return Err(ObjectError::Crypto(
                "invalid chunked encryption header magic".to_owned(),
            ));
        }
        let cipher = ChunkedCipher::from_id(bytes[4])?;
        let chunk_size = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if chunk_size == 0 {
            return Err(ObjectError::Crypto(
                "invalid chunked encryption header: chunk_size must be > 0".to_owned(),
            ));
        }
        let total_plaintext_len = u64::from_be_bytes([
            bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15], bytes[16],
        ]);
        Ok(Self {
            cipher,
            chunk_size,
            total_plaintext_len,
        })
    }
}

fn chunk_aad(header_bytes: &[u8; HEADER_SIZE], chunk_index: u64) -> [u8; HEADER_SIZE + 8] {
    let mut aad = [0u8; HEADER_SIZE + 8];
    aad[..HEADER_SIZE].copy_from_slice(header_bytes);
    aad[HEADER_SIZE..].copy_from_slice(&chunk_index.to_be_bytes());
    aad
}

fn expected_encrypted_len(total_plaintext_len: u64, chunk_size: u32) -> Option<u64> {
    if total_plaintext_len == 0 {
        return Some(HEADER_SIZE as u64);
    }
    let cs = u64::from(chunk_size);
    let num_chunks = total_plaintext_len.div_ceil(cs);
    let overhead = u64::try_from(CHUNK_OVERHEAD).ok()?;
    let header_len = u64::try_from(HEADER_SIZE).ok()?;
    header_len
        .checked_add(total_plaintext_len)?
        .checked_add(num_chunks.checked_mul(overhead)?)
}

/// Wraps an [`ObjectStorage`] and transparently encrypts objects in 64 KiB
/// plaintext chunks using [`ChunkedCipher`] (`AES-256-GCM` or `ChaCha20-Poly1305`).
///
/// Object layout:
/// `[17-byte header: magic + algo + chunk_size + total_plaintext_len][chunk 0][chunk 1]...`
///
/// Each chunk has layout:
/// `[12-byte random nonce][ciphertext][16-byte AEAD tag]`.
pub struct ChunkedEncryptedStore {
    inner: Arc<dyn ObjectStorage>,
    cipher: ChunkedCipher,
    key: LessSafeKey,
    rng: SystemRandom,
    chunk_size: usize,
}

impl fmt::Debug for ChunkedEncryptedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkedEncryptedStore")
            .field("inner", &self.inner)
            .field("cipher", &self.cipher)
            .field("chunk_size", &self.chunk_size)
            .field("key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl ChunkedEncryptedStore {
    /// Creates a [`ChunkedEncryptedStore`] using `cipher` and a 256-bit key.
    #[must_use]
    pub fn new(
        inner: Arc<dyn ObjectStorage>,
        cipher: ChunkedCipher,
        key: [u8; CHUNK_KEY_LEN],
    ) -> Self {
        let unbound = UnboundKey::new(cipher.algorithm(), &key)
            .expect("32-byte key is always valid for 256-bit AEAD ciphers");
        Self {
            inner,
            cipher,
            key: LessSafeKey::new(unbound),
            rng: SystemRandom::new(),
            chunk_size: CHUNK_SIZE,
        }
    }

    /// Creates a [`ChunkedEncryptedStore`] from a raw key slice (must be 32 bytes).
    pub fn from_key_slice(
        inner: Arc<dyn ObjectStorage>,
        cipher: ChunkedCipher,
        key: &[u8],
    ) -> Result<Self, ObjectError> {
        let key_arr: [u8; CHUNK_KEY_LEN] = key.try_into().map_err(|_| {
            ObjectError::Crypto(format!(
                "invalid key length: expected {CHUNK_KEY_LEN} bytes, got {}",
                key.len()
            ))
        })?;
        Ok(Self::new(inner, cipher, key_arr))
    }

    /// Creates a [`ChunkedEncryptedStore`] by deriving a 256-bit key from `passphrase` via SHA-256.
    #[must_use]
    pub fn from_passphrase(
        inner: Arc<dyn ObjectStorage>,
        cipher: ChunkedCipher,
        passphrase: impl AsRef<[u8]>,
    ) -> Self {
        let hash = digest(&SHA256, passphrase.as_ref());
        let mut key = [0u8; CHUNK_KEY_LEN];
        key.copy_from_slice(hash.as_ref());
        Self::new(inner, cipher, key)
    }

    /// Returns the configured [`ChunkedCipher`].
    #[must_use]
    pub const fn cipher(&self) -> ChunkedCipher {
        self.cipher
    }

    /// Returns the plaintext chunk size in bytes.
    #[must_use]
    pub const fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Returns a reference to the wrapped [`ObjectStorage`].
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn ObjectStorage> {
        &self.inner
    }

    fn calc_plain_size(&self, enc_size: u64) -> u64 {
        let header_len = HEADER_SIZE as u64;
        if enc_size <= header_len {
            return 0;
        }
        let body = enc_size - header_len;
        let cs = self.chunk_size as u64;
        let overhead = CHUNK_OVERHEAD as u64;
        let stride = cs + overhead;
        let full_chunks = body / stride;
        let rem = body % stride;
        let mut plain = full_chunks * cs;
        if rem > overhead {
            plain += rem - overhead;
        }
        plain
    }

    fn encrypt_object(&self, plaintext: &[u8]) -> Result<Bytes, ObjectError> {
        let total_plaintext_len = u64::try_from(plaintext.len())
            .map_err(|_| ObjectError::Crypto("plaintext size exceeds u64".to_owned()))?;
        let chunk_size_u32 = u32::try_from(self.chunk_size)
            .map_err(|_| ObjectError::Crypto("chunk size exceeds u32".to_owned()))?;

        let header = ChunkHeader {
            cipher: self.cipher,
            chunk_size: chunk_size_u32,
            total_plaintext_len,
        };
        let header_bytes = header.encode();

        if plaintext.is_empty() {
            return Ok(Bytes::copy_from_slice(&header_bytes));
        }

        let num_chunks = plaintext.len().div_ceil(self.chunk_size);
        let total_enc_len = HEADER_SIZE + plaintext.len() + num_chunks * CHUNK_OVERHEAD;
        let mut out = Vec::with_capacity(total_enc_len);
        out.extend_from_slice(&header_bytes);

        for (idx, chunk) in plaintext.chunks(self.chunk_size).enumerate() {
            let mut nonce_bytes = [0u8; NONCE_LEN];
            self.rng
                .fill(&mut nonce_bytes)
                .map_err(|_| ObjectError::Crypto("failed to generate random nonce".to_owned()))?;
            let nonce = Nonce::assume_unique_for_key(nonce_bytes);
            let aad_bytes = chunk_aad(&header_bytes, idx as u64);

            out.extend_from_slice(&nonce_bytes);
            let ct_start = out.len();
            out.extend_from_slice(chunk);

            let tag = self
                .key
                .seal_in_place_separate_tag(nonce, Aad::from(&aad_bytes), &mut out[ct_start..])
                .map_err(|_| ObjectError::Crypto("chunk AEAD encryption failed".to_owned()))?;
            out.extend_from_slice(tag.as_ref());
        }

        Ok(Bytes::from(out))
    }

    fn decrypt_chunk(
        &self,
        enc_chunk: &[u8],
        header_bytes: &[u8; HEADER_SIZE],
        chunk_index: u64,
    ) -> Result<Vec<u8>, ObjectError> {
        if enc_chunk.len() < CHUNK_OVERHEAD {
            return Err(ObjectError::Crypto(format!(
                "encrypted chunk {chunk_index} too short: {} bytes (minimum {CHUNK_OVERHEAD})",
                enc_chunk.len()
            )));
        }

        let nonce = Nonce::try_assume_unique_for_key(&enc_chunk[..NONCE_LEN])
            .map_err(|_| ObjectError::Crypto("invalid nonce length".to_owned()))?;
        let aad_bytes = chunk_aad(header_bytes, chunk_index);
        let mut buf = enc_chunk[NONCE_LEN..].to_vec();
        let plain_len = self
            .key
            .open_in_place(nonce, Aad::from(&aad_bytes), &mut buf)
            .map_err(|_| {
                ObjectError::Crypto(format!(
                    "decryption failed for chunk {chunk_index}: invalid key or corrupted ciphertext"
                ))
            })?
            .len();
        buf.truncate(plain_len);
        Ok(buf)
    }

    fn parse_and_check_header(
        &self,
        raw_header: &[u8],
    ) -> Result<([u8; HEADER_SIZE], ChunkHeader), ObjectError> {
        let header = ChunkHeader::decode(raw_header)?;
        if header.cipher != self.cipher {
            return Err(ObjectError::Crypto(format!(
                "cipher mismatch: object uses {:?}, store configured for {:?}",
                header.cipher, self.cipher
            )));
        }
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&raw_header[..HEADER_SIZE]);
        Ok((header_bytes, header))
    }

    async fn get_full(&self, key: &str) -> Result<Bytes, ObjectError> {
        let raw = self.inner.get(key, 0, 0).await?;
        let (header_bytes, header) = self.parse_and_check_header(&raw)?;
        let expected_len = expected_encrypted_len(header.total_plaintext_len, header.chunk_size)
            .ok_or_else(|| ObjectError::Crypto("encrypted object length overflow".to_owned()))?;
        if raw.len() as u64 != expected_len {
            return Err(ObjectError::Crypto(format!(
                "encrypted object length mismatch: expected {expected_len} bytes, got {}",
                raw.len()
            )));
        }
        if header.total_plaintext_len == 0 {
            return Ok(Bytes::new());
        }

        let total_plain = usize::try_from(header.total_plaintext_len)
            .map_err(|_| ObjectError::Backend("object exceeds addressable memory".to_owned()))?;
        let stride = header.chunk_size as usize + CHUNK_OVERHEAD;
        let mut out = Vec::with_capacity(total_plain);

        let mut pos = HEADER_SIZE;
        let mut chunk_idx = 0u64;
        while pos < raw.len() {
            let end = (pos + stride).min(raw.len());
            let plain = self.decrypt_chunk(&raw[pos..end], &header_bytes, chunk_idx)?;
            out.extend_from_slice(&plain);
            pos = end;
            chunk_idx += 1;
        }
        Ok(Bytes::from(out))
    }

    async fn get_range(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        let raw_header = self.inner.get(key, 0, HEADER_SIZE).await?;
        let (header_bytes, header) = self.parse_and_check_header(&raw_header)?;

        if offset > header.total_plaintext_len {
            return Err(ObjectError::InvalidRange {
                offset,
                size: header.total_plaintext_len,
            });
        }

        let end = if limit == 0 {
            header.total_plaintext_len
        } else {
            offset
                .saturating_add(limit as u64)
                .min(header.total_plaintext_len)
        };
        if offset == end {
            return Ok(Bytes::new());
        }

        let cs = u64::from(header.chunk_size);
        let start_chunk = offset / cs;
        let end_chunk = (end - 1) / cs;

        let enc_stride = cs + CHUNK_OVERHEAD as u64;
        let raw_start = HEADER_SIZE as u64 + start_chunk * enc_stride;
        let last_chunk_plain_len = (header.total_plaintext_len - end_chunk * cs).min(cs);
        let raw_end = HEADER_SIZE as u64
            + end_chunk * enc_stride
            + last_chunk_plain_len
            + CHUNK_OVERHEAD as u64;
        let raw_len = usize::try_from(raw_end - raw_start)
            .map_err(|_| ObjectError::Backend("range exceeds addressable memory".to_owned()))?;

        let raw_chunks = match self.inner.get(key, raw_start, raw_len).await {
            Ok(b) => b,
            Err(ObjectError::InvalidRange { .. }) => {
                return Err(ObjectError::Crypto(
                    "encrypted object truncated before requested chunk".to_owned(),
                ));
            }
            Err(e) => return Err(e),
        };
        if raw_chunks.len() != raw_len {
            return Err(ObjectError::Crypto(format!(
                "truncated encrypted chunk data: expected {raw_len} bytes, got {}",
                raw_chunks.len()
            )));
        }

        let out_cap = usize::try_from(end - offset)
            .map_err(|_| ObjectError::Backend("range exceeds addressable memory".to_owned()))?;
        let mut out = Vec::with_capacity(out_cap);

        for chunk_idx in start_chunk..=end_chunk {
            let rel_idx = usize::try_from(chunk_idx - start_chunk)
                .map_err(|_| ObjectError::Backend("chunk index overflow".to_owned()))?;
            let chunk_plain_len = (header.total_plaintext_len - chunk_idx * cs).min(cs) as usize;
            let chunk_enc_len = chunk_plain_len + CHUNK_OVERHEAD;
            let enc_offset = rel_idx * (header.chunk_size as usize + CHUNK_OVERHEAD);
            let enc_slice = &raw_chunks[enc_offset..enc_offset + chunk_enc_len];

            let plain_chunk = self.decrypt_chunk(enc_slice, &header_bytes, chunk_idx)?;
            if plain_chunk.len() != chunk_plain_len {
                return Err(ObjectError::Crypto(format!(
                    "chunk {chunk_idx} plaintext length mismatch: expected {chunk_plain_len}, got {}",
                    plain_chunk.len()
                )));
            }

            let chunk_global_start = chunk_idx * cs;
            let take_start = offset.saturating_sub(chunk_global_start) as usize;
            let take_end = (end - chunk_global_start).min(chunk_plain_len as u64) as usize;
            out.extend_from_slice(&plain_chunk[take_start..take_end]);
        }

        Ok(Bytes::from(out))
    }
}

#[async_trait]
impl ObjectStorage for ChunkedEncryptedStore {
    fn description(&self) -> String {
        format!("{}(encrypted-chunked)", self.inner.description())
    }

    fn limits(&self) -> Limits {
        Limits::default()
    }

    async fn create(&self) -> Result<(), ObjectError> {
        self.inner.create().await
    }

    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError> {
        validate_key(key)?;
        if offset == 0 && limit == 0 {
            self.get_full(key).await
        } else {
            self.get_range(key, offset, limit).await
        }
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError> {
        validate_key(key)?;
        let ciphertext = self.encrypt_object(&data)?;
        self.inner.put(key, ciphertext).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError> {
        validate_key(key)?;
        let mut meta = self.inner.head(key).await?;
        if !meta.is_dir {
            meta.size = self.calc_plain_size(meta.size);
        }
        Ok(meta)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        validate_key(key)?;
        self.inner.delete(key).await
    }

    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError> {
        validate_key(dst)?;
        validate_key(src)?;
        self.inner.copy(dst, src).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError> {
        let mut items = self.inner.list(prefix).await?;
        for item in &mut items {
            if !item.is_dir {
                item.size = self.calc_plain_size(item.size);
            }
        }
        Ok(items)
    }

    async fn restore(&self, key: &str, days: i32) -> Result<(), ObjectError> {
        validate_key(key)?;
        self.inner.restore(key, days).await
    }
}
