//! Atomic metrics collection and Prometheus text exposition for `juicefs-rs`.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Shared, thread-safe metrics registry backed by atomic counters.
///
/// Cloning `Metrics` is cheap and shares the same underlying counters via [`Arc`].
#[derive(Debug, Clone, Default)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    vfs_reads: AtomicU64,
    vfs_writes: AtomicU64,
    vfs_read_bytes: AtomicU64,
    vfs_write_bytes: AtomicU64,
    object_gets: AtomicU64,
    object_puts: AtomicU64,
    object_deletes: AtomicU64,
    gateway_requests: AtomicU64,
    webdav_requests: AtomicU64,
    errors: AtomicU64,
}

/// Point-in-time snapshot of all counter values in [`Metrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Total number of VFS read operations.
    pub vfs_reads: u64,
    /// Total number of VFS write operations.
    pub vfs_writes: u64,
    /// Total bytes read via VFS.
    pub vfs_read_bytes: u64,
    /// Total bytes written via VFS.
    pub vfs_write_bytes: u64,
    /// Total number of object store `GET` operations.
    pub object_gets: u64,
    /// Total number of object store `PUT` operations.
    pub object_puts: u64,
    /// Total number of object store `DELETE` operations.
    pub object_deletes: u64,
    /// Total number of S3 gateway requests handled.
    pub gateway_requests: u64,
    /// Total number of `WebDAV` requests handled.
    pub webdav_requests: u64,
    /// Total number of recorded errors.
    pub errors: u64,
}

struct CounterSpec {
    name: &'static str,
    help: &'static str,
    value: fn(&MetricsInner) -> u64,
}

const COUNTERS: &[CounterSpec] = &[
    CounterSpec {
        name: "juicefs_vfs_reads_total",
        help: "Total number of VFS read operations.",
        value: |m| m.vfs_reads.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_vfs_writes_total",
        help: "Total number of VFS write operations.",
        value: |m| m.vfs_writes.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_vfs_read_bytes_total",
        help: "Total number of bytes read via VFS.",
        value: |m| m.vfs_read_bytes.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_vfs_write_bytes_total",
        help: "Total number of bytes written via VFS.",
        value: |m| m.vfs_write_bytes.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_object_gets_total",
        help: "Total number of object store GET operations.",
        value: |m| m.object_gets.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_object_puts_total",
        help: "Total number of object store PUT operations.",
        value: |m| m.object_puts.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_object_deletes_total",
        help: "Total number of object store DELETE operations.",
        value: |m| m.object_deletes.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_gateway_requests_total",
        help: "Total number of S3 gateway requests handled.",
        value: |m| m.gateway_requests.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_webdav_requests_total",
        help: "Total number of WebDAV requests handled.",
        value: |m| m.webdav_requests.load(Ordering::Relaxed),
    },
    CounterSpec {
        name: "juicefs_errors_total",
        help: "Total number of errors encountered.",
        value: |m| m.errors.load(Ordering::Relaxed),
    },
];

impl Metrics {
    /// Creates a new zero-initialized [`Metrics`] instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a VFS read operation of `bytes` bytes.
    pub fn inc_vfs_read(&self, bytes: u64) {
        self.inner.vfs_reads.fetch_add(1, Ordering::Relaxed);
        self.inner
            .vfs_read_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a VFS write operation of `bytes` bytes.
    pub fn inc_vfs_write(&self, bytes: u64) {
        self.inner.vfs_writes.fetch_add(1, Ordering::Relaxed);
        self.inner
            .vfs_write_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records an object store `GET` operation.
    pub fn inc_object_get(&self) {
        self.inner.object_gets.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an object store `PUT` operation.
    pub fn inc_object_put(&self) {
        self.inner.object_puts.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an object store `DELETE` operation.
    pub fn inc_object_delete(&self) {
        self.inner.object_deletes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an S3 gateway request.
    pub fn inc_gateway_request(&self) {
        self.inner.gateway_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a `WebDAV` request.
    pub fn inc_webdav_request(&self) {
        self.inner.webdav_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an error occurrence.
    pub fn inc_error(&self) {
        self.inner.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the current value of `vfs_reads`.
    #[must_use]
    pub fn vfs_reads(&self) -> u64 {
        self.inner.vfs_reads.load(Ordering::Relaxed)
    }

    /// Returns the current value of `vfs_writes`.
    #[must_use]
    pub fn vfs_writes(&self) -> u64 {
        self.inner.vfs_writes.load(Ordering::Relaxed)
    }

    /// Returns the current value of `vfs_read_bytes`.
    #[must_use]
    pub fn vfs_read_bytes(&self) -> u64 {
        self.inner.vfs_read_bytes.load(Ordering::Relaxed)
    }

    /// Returns the current value of `vfs_write_bytes`.
    #[must_use]
    pub fn vfs_write_bytes(&self) -> u64 {
        self.inner.vfs_write_bytes.load(Ordering::Relaxed)
    }

    /// Returns the current value of `object_gets`.
    #[must_use]
    pub fn object_gets(&self) -> u64 {
        self.inner.object_gets.load(Ordering::Relaxed)
    }

    /// Returns the current value of `object_puts`.
    #[must_use]
    pub fn object_puts(&self) -> u64 {
        self.inner.object_puts.load(Ordering::Relaxed)
    }

    /// Returns the current value of `object_deletes`.
    #[must_use]
    pub fn object_deletes(&self) -> u64 {
        self.inner.object_deletes.load(Ordering::Relaxed)
    }

    /// Returns the current value of `gateway_requests`.
    #[must_use]
    pub fn gateway_requests(&self) -> u64 {
        self.inner.gateway_requests.load(Ordering::Relaxed)
    }

    /// Returns the current value of `webdav_requests`.
    #[must_use]
    pub fn webdav_requests(&self) -> u64 {
        self.inner.webdav_requests.load(Ordering::Relaxed)
    }

    /// Returns the current value of `errors`.
    #[must_use]
    pub fn errors(&self) -> u64 {
        self.inner.errors.load(Ordering::Relaxed)
    }

    /// Captures a point-in-time [`MetricsSnapshot`] of all counters.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            vfs_reads: self.vfs_reads(),
            vfs_writes: self.vfs_writes(),
            vfs_read_bytes: self.vfs_read_bytes(),
            vfs_write_bytes: self.vfs_write_bytes(),
            object_gets: self.object_gets(),
            object_puts: self.object_puts(),
            object_deletes: self.object_deletes(),
            gateway_requests: self.gateway_requests(),
            webdav_requests: self.webdav_requests(),
            errors: self.errors(),
        }
    }

    /// Renders all metrics in Prometheus text exposition format.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(1024);
        for spec in COUNTERS {
            let value = (spec.value)(&self.inner);
            let _ = writeln!(out, "# HELP {} {}", spec.name, spec.help);
            let _ = writeln!(out, "# TYPE {} counter", spec.name);
            let _ = writeln!(out, "{} {value}", spec.name);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        let metrics = Metrics::new();
        assert_eq!(metrics.snapshot(), MetricsSnapshot::default());
    }

    #[test]
    fn increment_helpers_update_counters_across_clones() {
        let metrics = Metrics::new();
        let cloned = metrics.clone();

        metrics.inc_vfs_read(1024);
        cloned.inc_vfs_read(2048);
        metrics.inc_vfs_write(512);
        cloned.inc_vfs_write(0);
        metrics.inc_object_get();
        cloned.inc_object_get();
        metrics.inc_object_put();
        cloned.inc_object_delete();
        metrics.inc_gateway_request();
        cloned.inc_gateway_request();
        cloned.inc_gateway_request();
        metrics.inc_webdav_request();
        cloned.inc_error();
        metrics.inc_error();

        assert_eq!(metrics.vfs_reads(), 2);
        assert_eq!(metrics.vfs_read_bytes(), 3072);
        assert_eq!(metrics.vfs_writes(), 2);
        assert_eq!(metrics.vfs_write_bytes(), 512);
        assert_eq!(metrics.object_gets(), 2);
        assert_eq!(metrics.object_puts(), 1);
        assert_eq!(metrics.object_deletes(), 1);
        assert_eq!(metrics.gateway_requests(), 3);
        assert_eq!(metrics.webdav_requests(), 1);
        assert_eq!(metrics.errors(), 2);
        assert_eq!(metrics.snapshot(), cloned.snapshot());
    }

    #[test]
    fn concurrent_increments_are_accurate() {
        let metrics = Metrics::new();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let m = metrics.clone();
                scope.spawn(move || {
                    for _ in 0..1_000 {
                        m.inc_vfs_read(16);
                        m.inc_vfs_write(8);
                        m.inc_object_get();
                        m.inc_object_put();
                        m.inc_object_delete();
                        m.inc_gateway_request();
                        m.inc_webdav_request();
                        m.inc_error();
                    }
                });
            }
        });

        assert_eq!(
            metrics.snapshot(),
            MetricsSnapshot {
                vfs_reads: 8_000,
                vfs_writes: 8_000,
                vfs_read_bytes: 128_000,
                vfs_write_bytes: 64_000,
                object_gets: 8_000,
                object_puts: 8_000,
                object_deletes: 8_000,
                gateway_requests: 8_000,
                webdav_requests: 8_000,
                errors: 8_000,
            }
        );
    }

    #[test]
    fn render_prometheus_outputs_valid_exposition_format() {
        let metrics = Metrics::new();
        metrics.inc_vfs_read(4096);
        metrics.inc_vfs_write(1024);
        metrics.inc_object_get();
        metrics.inc_object_put();
        metrics.inc_object_delete();
        metrics.inc_gateway_request();
        metrics.inc_webdav_request();
        metrics.inc_error();

        let rendered = metrics.render_prometheus();
        let expected = "\
# HELP juicefs_vfs_reads_total Total number of VFS read operations.
# TYPE juicefs_vfs_reads_total counter
juicefs_vfs_reads_total 1
# HELP juicefs_vfs_writes_total Total number of VFS write operations.
# TYPE juicefs_vfs_writes_total counter
juicefs_vfs_writes_total 1
# HELP juicefs_vfs_read_bytes_total Total number of bytes read via VFS.
# TYPE juicefs_vfs_read_bytes_total counter
juicefs_vfs_read_bytes_total 4096
# HELP juicefs_vfs_write_bytes_total Total number of bytes written via VFS.
# TYPE juicefs_vfs_write_bytes_total counter
juicefs_vfs_write_bytes_total 1024
# HELP juicefs_object_gets_total Total number of object store GET operations.
# TYPE juicefs_object_gets_total counter
juicefs_object_gets_total 1
# HELP juicefs_object_puts_total Total number of object store PUT operations.
# TYPE juicefs_object_puts_total counter
juicefs_object_puts_total 1
# HELP juicefs_object_deletes_total Total number of object store DELETE operations.
# TYPE juicefs_object_deletes_total counter
juicefs_object_deletes_total 1
# HELP juicefs_gateway_requests_total Total number of S3 gateway requests handled.
# TYPE juicefs_gateway_requests_total counter
juicefs_gateway_requests_total 1
# HELP juicefs_webdav_requests_total Total number of WebDAV requests handled.
# TYPE juicefs_webdav_requests_total counter
juicefs_webdav_requests_total 1
# HELP juicefs_errors_total Total number of errors encountered.
# TYPE juicefs_errors_total counter
juicefs_errors_total 1
";
        assert_eq!(rendered, expected);
    }

    #[test]
    fn render_prometheus_has_consistent_triplet_structure() {
        let metrics = Metrics::new();
        let rendered = metrics.render_prometheus();
        assert!(rendered.ends_with('\n'));
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), COUNTERS.len() * 3);

        let (chunks, remainder) = lines.as_chunks::<3>();
        assert_eq!(remainder, [] as [&str; 0]);
        for [help_line, type_line, sample_line] in chunks {
            assert!(help_line.starts_with("# HELP juicefs_"));
            assert!(type_line.starts_with("# TYPE juicefs_"));
            assert!(type_line.ends_with(" counter"));

            let type_name = type_line
                .strip_prefix("# TYPE ")
                .and_then(|s| s.strip_suffix(" counter"))
                .expect("valid TYPE line");
            let help_name = help_line
                .strip_prefix("# HELP ")
                .and_then(|s| s.split_once(' '))
                .map(|(name, _)| name)
                .expect("valid HELP line");
            let (sample_name, sample_val) = sample_line.split_once(' ').expect("valid sample line");

            assert_eq!(help_name, type_name);
            assert_eq!(sample_name, type_name);
            assert_eq!(sample_val, "0");
        }
    }
}
