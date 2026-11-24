//! Ported tests from `pkg/object/object_storage_test.go`.

use std::sync::Arc;

use bytes::Bytes;
use juicefs_core::Format;
use juicefs_object::{
    AzureStore, EncryptedStore, FileStore, GcsStore, MemoryStore, ObjectError, ObjectStorage, Part,
    PrefixedStore, S3Store, ShardedStore, open_from_format,
};
use tempfile::tempdir;

async fn run_object_storage_suite(store: &dyn ObjectStorage) {
    store.create().await.expect("create");

    // Put, Head, Get
    let data = Bytes::from_static(b"hello juicefs object storage");
    store.put("dir/file.txt", data.clone()).await.expect("put");
    let meta = store.head("dir/file.txt").await.expect("head");
    assert_eq!(meta.key, "dir/file.txt");
    assert_eq!(meta.size, data.len() as u64);

    let full = store.get("dir/file.txt", 0, 0).await.expect("get full");
    assert_eq!(full, data);

    let slice = store.get("dir/file.txt", 6, 7).await.expect("get range");
    assert_eq!(&slice[..], b"juicefs");

    let tail = store.get("dir/file.txt", 21, 0).await.expect("get tail");
    assert_eq!(&tail[..], b"storage");

    // Out-of-bounds offset
    assert!(matches!(
        store.get("dir/file.txt", 999, 10).await,
        Err(ObjectError::InvalidRange { .. })
    ));

    // Overwrite
    store
        .put("dir/file.txt", Bytes::from_static(b"updated"))
        .await
        .expect("overwrite");
    let updated = store.get("dir/file.txt", 0, 0).await.expect("get updated");
    assert_eq!(&updated[..], b"updated");

    // Copy
    store
        .copy("dir/copy.txt", "dir/file.txt")
        .await
        .expect("copy");
    let copied = store.get("dir/copy.txt", 0, 0).await.expect("get copy");
    assert_eq!(&copied[..], b"updated");

    // Delete is idempotent
    store.delete("dir/file.txt").await.expect("delete");
    store
        .delete("dir/file.txt")
        .await
        .expect("delete idempotent");
    assert!(matches!(
        store.head("dir/file.txt").await,
        Err(ObjectError::NotFound(_))
    ));
    store.delete("dir/copy.txt").await.expect("cleanup copy");
}

async fn run_list_prefix_boundary(store: Arc<dyn ObjectStorage>) {
    let prefixed = PrefixedStore::new(store, "unit-test/list-prefix-boundary");
    let all_keys = ["a", "b", "p", "p1", "p2", "z", "测试", "测试1"];
    for key in all_keys {
        prefixed
            .put(key, Bytes::from(key.as_bytes().to_vec()))
            .await
            .expect("put");
    }

    let p_items = prefixed.list("p").await.expect("list p");
    let p_keys: Vec<&str> = p_items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(p_keys, vec!["p", "p1", "p2"]);

    let q_items = prefixed.list("q").await.expect("list q");
    assert_eq!(q_items.len(), 0);

    let uni_items = prefixed.list("测试").await.expect("list unicode");
    let uni_keys: Vec<&str> = uni_items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(uni_keys, vec!["测试", "测试1"]);

    let all_items = prefixed.list("").await.expect("list all");
    let mut got_keys: Vec<&str> = all_items.iter().map(|m| m.key.as_str()).collect();
    got_keys.sort_unstable();
    let mut want_keys = all_keys.to_vec();
    want_keys.sort_unstable();
    assert_eq!(got_keys, want_keys);
}

#[tokio::test]
async fn test_memory_store() {
    let store = MemoryStore::new();
    run_object_storage_suite(&store).await;
    run_list_prefix_boundary(Arc::new(store)).await;
}

#[tokio::test]
async fn test_disk_file_store() {
    let dir = tempdir().expect("tempdir");
    let store = FileStore::new(dir.path()).expect("file store");
    run_object_storage_suite(&store).await;
    run_list_prefix_boundary(Arc::new(store)).await;
}

#[tokio::test]
async fn test_rejects_unsafe_keys() {
    let store = MemoryStore::new();
    for bad in ["", "/abs", "../escape", "a/../b"] {
        assert!(matches!(
            store.head(bad).await,
            Err(ObjectError::InvalidKey(_))
        ));
    }
}

async fn run_multipart_suite(store: &dyn ObjectStorage) {
    store.create().await.expect("create");
    assert!(store.limits().is_support_multipart_upload);

    let key = "multipart/large.bin";
    let upload = store
        .create_multipart_upload(key)
        .await
        .expect("create multipart upload");
    assert_ne!(upload.upload_id, "");
    assert!(upload.min_part_size >= 1);
    assert!(upload.max_count >= 1);

    // Upload parts out of order: part 2, then part 1, then part 3.
    let part2_data = Bytes::from_static(b"SECOND-PART-DATA-");
    let part1_orig = Bytes::from_static(b"FIRST-PART-OLD-");
    let part3_data = Bytes::from_static(b"THIRD-PART-END");

    let p2 = store
        .upload_part(key, &upload.upload_id, 2, part2_data.clone())
        .await
        .expect("upload part 2");
    assert_eq!(p2.num, 2);
    assert_eq!(p2.size, part2_data.len());

    let p1_old = store
        .upload_part(key, &upload.upload_id, 1, part1_orig)
        .await
        .expect("upload part 1");
    assert_eq!(p1_old.num, 1);

    let p3 = store
        .upload_part(key, &upload.upload_id, 3, part3_data.clone())
        .await
        .expect("upload part 3");

    // Overwrite part 1 before completing.
    let part1_data = Bytes::from_static(b"FIRST-PART-UPDATED-");
    let p1 = store
        .upload_part(key, &upload.upload_id, 1, part1_data.clone())
        .await
        .expect("overwrite part 1");
    assert_ne!(p1.etag, p1_old.etag);

    run_multipart_validation_checks(store, key, &upload.upload_id, &p1, &p2, &p3).await;

    store
        .complete_upload(key, &upload.upload_id, &[p1, p2, p3])
        .await
        .expect("complete upload");

    let mut expected = Vec::new();
    expected.extend_from_slice(&part1_data);
    expected.extend_from_slice(&part2_data);
    expected.extend_from_slice(&part3_data);

    let meta = store.head(key).await.expect("head completed object");
    assert_eq!(meta.size, expected.len() as u64);

    let got = store.get(key, 0, 0).await.expect("get completed object");
    assert_eq!(&got[..], &expected[..]);

    let range = store.get(key, 11, 15).await.expect("range across parts");
    assert_eq!(&range[..], &expected[11..26]);

    // Staging artifacts must not appear in list output.
    let listed = store.list("multipart/").await.expect("list after complete");
    let keys: Vec<&str> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec![key]);
    let all = store.list("").await.expect("list root");
    assert!(
        !all.iter().any(|m| m.key.contains(".juicefs-multipart")),
        "multipart staging files leaked into list: {all:?}"
    );

    // Subsequent operations on completed upload_id fail.
    assert!(
        store
            .upload_part(key, &upload.upload_id, 4, Bytes::from_static(b"late"))
            .await
            .is_err()
    );
    assert!(
        store
            .complete_upload(
                key,
                &upload.upload_id,
                &[Part {
                    num: 1,
                    size: 0,
                    etag: String::new(),
                }]
            )
            .await
            .is_err()
    );

    run_multipart_abort_check(store).await;
    store.delete(key).await.expect("cleanup");
}

async fn run_multipart_validation_checks(
    store: &dyn ObjectStorage,
    key: &str,
    upload_id: &str,
    p1: &Part,
    p2: &Part,
    p3: &Part,
) {
    assert!(
        store
            .upload_part(key, upload_id, 0, Bytes::from_static(b"bad"))
            .await
            .is_err()
    );
    assert!(store.complete_upload(key, upload_id, &[]).await.is_err());
    assert!(
        store
            .complete_upload(key, upload_id, &[p2.clone(), p1.clone()])
            .await
            .is_err()
    );
    let mut bad_etag = p1.clone();
    "deadbeef".clone_into(&mut bad_etag.etag);
    assert!(
        store
            .complete_upload(key, upload_id, &[bad_etag, p2.clone(), p3.clone()])
            .await
            .is_err()
    );
}

async fn run_multipart_abort_check(store: &dyn ObjectStorage) {
    let abort_key = "multipart/aborted.bin";
    let aborted = store
        .create_multipart_upload(abort_key)
        .await
        .expect("create abort upload");
    let ap1 = store
        .upload_part(
            abort_key,
            &aborted.upload_id,
            1,
            Bytes::from_static(b"temp"),
        )
        .await
        .expect("upload part before abort");
    store
        .abort_upload(abort_key, &aborted.upload_id)
        .await
        .expect("abort upload");
    // Idempotent abort.
    store
        .abort_upload(abort_key, &aborted.upload_id)
        .await
        .expect("idempotent abort");

    assert!(
        store
            .upload_part(abort_key, &aborted.upload_id, 2, Bytes::from_static(b"x"))
            .await
            .is_err()
    );
    assert!(
        store
            .complete_upload(abort_key, &aborted.upload_id, &[ap1])
            .await
            .is_err()
    );
    assert!(matches!(
        store.head(abort_key).await,
        Err(ObjectError::NotFound(_))
    ));
}

#[tokio::test]
async fn test_memory_store_multipart() {
    let store = MemoryStore::new();
    run_multipart_suite(&store).await;
}

#[tokio::test]
async fn test_file_store_multipart() {
    let dir = tempdir().expect("tempdir");
    let store = FileStore::new(dir.path()).expect("file store");
    run_multipart_suite(&store).await;
}

#[tokio::test]
async fn test_prefixed_store_multipart() {
    let inner = Arc::new(MemoryStore::new());
    let prefixed = PrefixedStore::new(inner, "vol-prefix");
    run_multipart_suite(&prefixed).await;
}

#[tokio::test]
async fn test_encrypted_store_roundtrip_and_range() {
    let inner = Arc::new(MemoryStore::new());
    let key = [0x42u8; 32];
    let store = EncryptedStore::new(inner.clone(), key);
    store.create().await.expect("create");
    assert!(store.description().ends_with("(encrypted)"));
    assert!(!store.limits().is_support_multipart_upload);
    assert!(matches!(
        store.create_multipart_upload("a").await,
        Err(ObjectError::Unsupported(_))
    ));

    let plaintext = Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz");
    store
        .put("secret/doc.txt", plaintext.clone())
        .await
        .expect("put encrypted");

    // Underlying object must be encrypted and include nonce (12) + GCM tag (16).
    let raw = inner.get("secret/doc.txt", 0, 0).await.expect("get raw");
    assert_ne!(&raw[..], &plaintext[..]);
    assert_eq!(raw.len(), plaintext.len() + 12 + 16);

    // Full decrypt round-trip.
    let got = store
        .get("secret/doc.txt", 0, 0)
        .await
        .expect("get decrypted");
    assert_eq!(got, plaintext);

    // Range reads on decrypted payload.
    assert_eq!(
        &store.get("secret/doc.txt", 0, 5).await.expect("range head")[..],
        b"01234"
    );
    assert_eq!(
        &store.get("secret/doc.txt", 10, 6).await.expect("range mid")[..],
        b"abcdef"
    );
    assert_eq!(
        &store
            .get("secret/doc.txt", 30, 0)
            .await
            .expect("range tail")[..],
        b"uvwxyz"
    );
    assert_eq!(
        &store.get("secret/doc.txt", 36, 5).await.expect("range eof")[..],
        b""
    );
    assert!(matches!(
        store.get("secret/doc.txt", 37, 5).await,
        Err(ObjectError::InvalidRange { .. })
    ));

    // Empty object round-trip.
    store
        .put("secret/empty.bin", Bytes::new())
        .await
        .expect("put empty");
    assert_eq!(
        store
            .get("secret/empty.bin", 0, 0)
            .await
            .expect("get empty"),
        Bytes::new()
    );

    // Identical plaintext written twice uses distinct random nonces.
    store
        .put("secret/doc2.txt", plaintext.clone())
        .await
        .expect("put second");
    let raw2 = inner.get("secret/doc2.txt", 0, 0).await.expect("get raw2");
    assert_ne!(raw, raw2);

    // Copy, head, list, delete work transparently.
    store
        .copy("secret/doc_copy.txt", "secret/doc.txt")
        .await
        .expect("copy encrypted");
    assert_eq!(
        store
            .get("secret/doc_copy.txt", 0, 0)
            .await
            .expect("get copied"),
        plaintext
    );
    assert!(store.head("secret/doc.txt").await.is_ok());
    assert_eq!(store.list("secret/").await.expect("list").len(), 4);
    store.delete("secret/doc.txt").await.expect("delete");
    assert!(matches!(
        store.head("secret/doc.txt").await,
        Err(ObjectError::NotFound(_))
    ));
}

#[tokio::test]
async fn test_encrypted_store_wrong_key_and_tamper_fail() {
    let inner = Arc::new(MemoryStore::new());
    let writer = EncryptedStore::new(inner.clone(), [0x11u8; 32]);
    let reader_wrong_key = EncryptedStore::new(inner.clone(), [0x22u8; 32]);

    writer
        .put("k", Bytes::from_static(b"top secret payload"))
        .await
        .expect("put");

    assert!(matches!(
        reader_wrong_key.get("k", 0, 0).await,
        Err(ObjectError::Crypto(_))
    ));

    // Passphrase constructor mismatch also fails, matching passphrase succeeds.
    let pass_writer = EncryptedStore::from_passphrase(inner.clone(), "correct-horse");
    let pass_good = EncryptedStore::from_passphrase(inner.clone(), "correct-horse");
    let pass_bad = EncryptedStore::from_passphrase(inner.clone(), "wrong-horse");

    pass_writer
        .put("p", Bytes::from_static(b"passphrase protected"))
        .await
        .expect("put passphrase");
    assert_eq!(
        &pass_good.get("p", 0, 0).await.expect("decrypt good")[..],
        b"passphrase protected"
    );
    assert!(matches!(
        pass_bad.get("p", 0, 0).await,
        Err(ObjectError::Crypto(_))
    ));

    // Tampered ciphertext fails GCM authentication.
    let mut tampered = inner.get("p", 0, 0).await.expect("raw").to_vec();
    let last = tampered.len() - 1;
    tampered[last] ^= 0xFF;
    inner
        .put("p", Bytes::from(tampered))
        .await
        .expect("overwrite tampered");
    assert!(matches!(
        pass_good.get("p", 0, 0).await,
        Err(ObjectError::Crypto(_))
    ));

    // Invalid key length is rejected.
    assert!(matches!(
        EncryptedStore::from_key_slice(inner, &[0u8; 16]),
        Err(ObjectError::Crypto(_))
    ));
}

#[tokio::test]
async fn test_sharded_store_distribution_and_operations() {
    assert!(ShardedStore::new(vec![]).is_err());

    let shards: Vec<Arc<MemoryStore>> = (0..4).map(|_| Arc::new(MemoryStore::new())).collect();
    let dyn_shards: Vec<Arc<dyn ObjectStorage>> = shards
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStorage>)
        .collect();
    let store = ShardedStore::new(dyn_shards).expect("sharded store");
    assert_eq!(store.shard_count(), 4);
    assert!(store.description().starts_with("shard4://"));
    store.create().await.expect("create");

    // Put 40 keys and verify FNV-1a distribution across all 4 shards.
    let mut expected_all = Vec::new();
    for i in 0..40 {
        let key = format!("key-{i:02}");
        let val = format!("value-{i:02}");
        store
            .put(&key, Bytes::from(val.into_bytes()))
            .await
            .expect("put sharded");
        expected_all.push(key);
    }

    let mut total_across_shards = 0usize;
    for (idx, shard) in shards.iter().enumerate() {
        let items = shard.list("").await.expect("shard list");
        assert!(
            !items.is_empty(),
            "shard {idx} unexpectedly received 0 keys out of 40"
        );
        for item in &items {
            assert_eq!(store.shard_index(&item.key), idx);
        }
        total_across_shards += items.len();
    }
    assert_eq!(total_across_shards, 40);

    // Verify get, head, range get.
    let v = store.get("key-17", 0, 0).await.expect("get key-17");
    assert_eq!(&v[..], b"value-17");
    let v_slice = store.get("key-17", 6, 2).await.expect("get range key-17");
    assert_eq!(&v_slice[..], b"17");
    let meta = store.head("key-17").await.expect("head key-17");
    assert_eq!(meta.size, 8);

    // Fan-out list returns merge-sorted keys.
    let all_listed = store.list("").await.expect("list all");
    let got_keys: Vec<String> = all_listed.into_iter().map(|m| m.key).collect();
    expected_all.sort();
    assert_eq!(got_keys, expected_all);

    let prefix_listed = store.list("key-2").await.expect("list key-2");
    let prefix_keys: Vec<String> = prefix_listed.into_iter().map(|m| m.key).collect();
    let expected_prefix: Vec<String> = (20..30).map(|i| format!("key-{i:02}")).collect();
    assert_eq!(prefix_keys, expected_prefix);

    // Copy is unsupported on ShardedStore.
    assert!(matches!(
        store.copy("key-99", "key-17").await,
        Err(ObjectError::Unsupported(_))
    ));

    // Delete removes from target shard and is idempotent.
    store.delete("key-17").await.expect("delete key-17");
    store
        .delete("key-17")
        .await
        .expect("delete key-17 idempotent");
    assert!(matches!(
        store.head("key-17").await,
        Err(ObjectError::NotFound(_))
    ));

    // Verify list-prefix boundary and multipart on ShardedStore.
    run_list_prefix_boundary(Arc::new(store.clone())).await;
    run_multipart_suite(&store).await;
}

#[test]
fn test_gcs_store_from_format_and_invalid_urls() {
    let fmt = Format::new("vol", "gs", "gs://my-gcs-bucket/path/to/prefix");
    let store = GcsStore::from_format(&fmt).expect("valid gs:// url");
    assert_eq!(store.description(), "gs://my-gcs-bucket/path/to/prefix");
    assert_eq!(store.base_prefix(), "path/to/prefix");
    assert!(store.limits().is_support_multipart_upload);

    let root_fmt = Format::new("vol", "gcs", "gs://my-gcs-bucket");
    let root_store = GcsStore::from_format(&root_fmt).expect("gs:// without prefix");
    assert_eq!(root_store.base_prefix(), "");

    let gcs_scheme_fmt = Format::new("vol", "gcs", "gcs://my-gcs-bucket/sub/");
    let gcs_scheme_store = GcsStore::from_format(&gcs_scheme_fmt).expect("gcs:// url");
    assert_eq!(gcs_scheme_store.base_prefix(), "sub");

    for bad_url in [
        "",
        "://bad-url",
        "gs:///missing-bucket",
        "s3://wrong/prefix",
    ] {
        let bad_fmt = Format::new("vol", "gs", bad_url);
        let err = GcsStore::from_format(&bad_fmt).expect_err("expected invalid gcs url error");
        assert!(
            matches!(err, ObjectError::Backend(_)),
            "unexpected error for '{bad_url}': {err:?}"
        );
    }
}

#[test]
fn test_azure_store_from_format_and_invalid_urls() {
    let mut azblob_fmt = Format::new("vol", "azblob", "azblob://my-container/az/prefix");
    azblob_fmt.access_key = "myaccount".to_owned();
    azblob_fmt.secret_key = "dGVzdGtleQ==".to_owned();
    let azblob_store = AzureStore::from_format(&azblob_fmt).expect("valid azblob:// url");
    assert_eq!(
        azblob_store.description(),
        "azblob://my-container/az/prefix"
    );
    assert_eq!(azblob_store.base_prefix(), "az/prefix");
    let limits = azblob_store.limits();
    assert!(limits.is_support_multipart_upload);
    assert_eq!(limits.max_part_count, 50_000);

    let mut wasb_fmt = Format::new("vol", "wasb", "wasb://my-container/wasb/prefix/");
    wasb_fmt.access_key = "myaccount".to_owned();
    let wasb_store = AzureStore::from_format(&wasb_fmt).expect("valid wasb:// url");
    assert_eq!(wasb_store.base_prefix(), "wasb/prefix");

    let wasb_full_fmt = Format::new(
        "vol",
        "wasb",
        "wasb://my-container@myaccount.blob.core.windows.net/nested/prefix",
    );
    let wasb_full_store =
        AzureStore::from_format(&wasb_full_fmt).expect("wasb:// with embedded account");
    assert_eq!(wasb_full_store.base_prefix(), "nested/prefix");

    let https_fmt = Format::new(
        "vol",
        "azure",
        "https://myaccount.blob.core.windows.net/my-container/https/prefix",
    );
    let https_store = AzureStore::from_format(&https_fmt).expect("https azure blob url");
    assert_eq!(https_store.base_prefix(), "https/prefix");

    for bad_url in [
        "",
        "://bad-url",
        "azblob:///missing-container",
        "wasb:///missing-container",
        "s3://wrong-scheme/prefix",
        "https://myaccount.blob.core.windows.net/",
    ] {
        let mut bad_fmt = Format::new("vol", "azblob", bad_url);
        bad_fmt.access_key = "myaccount".to_owned();
        let err = AzureStore::from_format(&bad_fmt).expect_err("expected invalid azure url error");
        assert!(
            matches!(err, ObjectError::Backend(_)),
            "unexpected error for '{bad_url}': {err:?}"
        );
    }

    let mut bad_key_fmt = Format::new("vol", "azblob", "azblob://my-container/prefix");
    bad_key_fmt.access_key = "myaccount".to_owned();
    bad_key_fmt.secret_key = "not-valid-base64!!!".to_owned();
    assert!(matches!(
        AzureStore::from_format(&bad_key_fmt),
        Err(ObjectError::Backend(_))
    ));
}

#[test]
fn test_s3_store_from_format_and_invalid_urls() {
    let fmt = Format::new("vol", "s3", "s3://my-bucket/s3/prefix/");
    let store = S3Store::from_format(&fmt).expect("valid s3:// url");
    assert_eq!(store.description(), "s3://my-bucket/s3/prefix/");
    assert_eq!(store.base_prefix(), "s3/prefix");

    for bad_url in ["", "://bad-url", "s3:///missing-bucket"] {
        let bad_fmt = Format::new("vol", "s3", bad_url);
        assert!(matches!(
            S3Store::from_format(&bad_fmt),
            Err(ObjectError::Backend(_))
        ));
    }
}

#[test]
fn test_open_from_format_cloud_dispatch() {
    for storage in ["s3", "oss", "cos", "minio", "b2", "obs", "bos", "tos"] {
        let url = format!("{storage}://my-bucket/prefix");
        let fmt = Format::new("vol", storage, &url);
        let store = open_from_format(&fmt)
            .unwrap_or_else(|e| panic!("open_from_format failed for '{storage}': {e}"));
        assert_eq!(store.description(), format!("{url}vol/"));
    }

    for (storage, name, expected_suffix) in [("gs", "vol", "vol/"), ("gcs", "", "")] {
        let url = "gs://gcs-bucket/prefix";
        let fmt = Format::new(name, storage, url);
        let store = open_from_format(&fmt)
            .unwrap_or_else(|e| panic!("open_from_format failed for '{storage}': {e}"));
        assert_eq!(store.description(), format!("{url}{expected_suffix}"));
    }

    for storage in ["azblob", "azure", "wasb"] {
        let url = format!("{storage}://my-container/prefix");
        let mut fmt = Format::new("vol", storage, &url);
        fmt.access_key = "myaccount".to_owned();
        let store = open_from_format(&fmt)
            .unwrap_or_else(|e| panic!("open_from_format failed for '{storage}': {e}"));
        assert_eq!(store.description(), format!("{url}vol/"));
    }

    let upper_fmt = Format::new("", "GCS", "gs://gcs-bucket/prefix");
    let upper_store = open_from_format(&upper_fmt).expect("case-insensitive storage dispatch");
    assert_eq!(upper_store.description(), "gs://gcs-bucket/prefix");

    let unsupported = Format::new("vol", "swift", "swift://container/prefix");
    assert!(matches!(
        open_from_format(&unsupported),
        Err(ObjectError::UnsupportedBackend(name)) if name == "swift"
    ));
}
