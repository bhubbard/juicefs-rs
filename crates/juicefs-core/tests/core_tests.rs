//! Ported tests from `pkg/compress/compress_test.go` and `pkg/meta/slice_test`.

use juicefs_core::{
    CompressionAlgo, CoreError, Slice, SliceRecord, build_slice, compact_chunk, decode_slices,
    encode_slice,
};

#[test]
fn compress_empty_and_small_buffers() {
    for algo in [
        CompressionAlgo::None,
        CompressionAlgo::Lz4,
        CompressionAlgo::Zstd,
    ] {
        let compressed = algo.compress(b"").expect("compress empty");
        let restored = algo.decompress(&compressed, 100).expect("decompress empty");
        assert!(restored.is_empty(), "algo {algo:?}");

        let sample = algo.as_str().as_bytes();
        let c = algo.compress(sample).expect("compress name");
        let d = algo.decompress(&c, sample.len()).expect("decompress name");
        assert_eq!(d, sample, "algo {algo:?}");
    }
}

#[test]
fn decompress_rejects_short_destination_or_empty_input() {
    let payload = b"juicefs-compression-boundary-check-payload-0123456789".repeat(8);
    for algo in [CompressionAlgo::Lz4, CompressionAlgo::Zstd] {
        let compressed = algo.compress(&payload).expect("compress");
        let err = algo.decompress(&compressed, 1);
        assert!(err.is_err(), "expected short dst error for {algo:?}");
        assert!(
            algo.decompress(&[], 100).is_err(),
            "expected empty compressed input error for {algo:?}"
        );
    }
}

#[test]
fn compression_algo_parsing() {
    assert_eq!(
        "".parse::<CompressionAlgo>().unwrap(),
        CompressionAlgo::None
    );
    assert_eq!(
        "NONE".parse::<CompressionAlgo>().unwrap(),
        CompressionAlgo::None
    );
    assert_eq!(
        "lz4".parse::<CompressionAlgo>().unwrap(),
        CompressionAlgo::Lz4
    );
    assert_eq!(
        "ZSTD".parse::<CompressionAlgo>().unwrap(),
        CompressionAlgo::Zstd
    );
    assert!(matches!(
        "brotli".parse::<CompressionAlgo>(),
        Err(CoreError::UnsupportedCompression(_))
    ));
}

#[test]
fn slice_decode_rejects_corrupt_length() {
    let err = decode_slices(&[0u8; 23]);
    assert!(matches!(err, Err(CoreError::CorruptSliceBuffer(23))));
}

#[test]
fn slice_encode_decode_multiple() {
    let records = vec![
        SliceRecord {
            pos: 0,
            id: 1,
            size: 4096,
            off: 0,
            len: 4096,
        },
        SliceRecord {
            pos: 8192,
            id: 2,
            size: 2048,
            off: 512,
            len: 1024,
        },
    ];
    let mut buf = Vec::new();
    for &rec in &records {
        buf.extend_from_slice(&encode_slice(rec));
    }
    let decoded = decode_slices(&buf).expect("decode");
    assert_eq!(decoded, records);
}

#[test]
fn build_slice_complete_overwrite_and_compact() {
    let records = vec![
        SliceRecord {
            pos: 100,
            id: 1,
            size: 100,
            off: 0,
            len: 100,
        },
        SliceRecord {
            pos: 100,
            id: 2,
            size: 100,
            off: 0,
            len: 100,
        },
    ];
    let resolved = build_slice(&records);
    assert_eq!(
        resolved,
        vec![
            Slice {
                id: 0,
                size: 100,
                off: 0,
                len: 100
            },
            Slice {
                id: 2,
                size: 100,
                off: 0,
                len: 100
            },
        ]
    );

    let (start_pos, total_len, compacted) = compact_chunk(&records);
    assert_eq!(start_pos, 100);
    assert_eq!(total_len, 100);
    assert_eq!(
        compacted,
        vec![Slice {
            id: 2,
            size: 100,
            off: 0,
            len: 100
        }]
    );
}
