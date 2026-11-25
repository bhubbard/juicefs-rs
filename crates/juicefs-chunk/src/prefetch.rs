//! Asynchronous background block prefetcher.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
use tokio::sync::{Mutex as AsyncMutex, mpsc};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type FetchCallback = Arc<dyn Fn(String) -> BoxFuture + Send + Sync>;

/// Non-blocking asynchronous background prefetcher for upcoming block keys.
///
/// Mirrors `JuiceFS` `pkg/chunk/prefetch.go`: queues keys up to a bounded capacity,
/// deduplicates keys that are already queued or in progress, and drops excess requests
/// without blocking foreground reads.
#[derive(Clone)]
pub struct Prefetcher {
    inner: Option<Arc<PrefetcherInner>>,
}

struct PrefetcherInner {
    parallel: usize,
    tx: mpsc::Sender<String>,
    rx: Arc<AsyncMutex<mpsc::Receiver<String>>>,
    busy: Arc<Mutex<HashSet<String>>>,
    fetch_fn: FetchCallback,
    started: AtomicBool,
    start_lock: Mutex<()>,
}

impl std::fmt::Debug for Prefetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Some(inner) => f
                .debug_struct("Prefetcher")
                .field("parallel", &inner.parallel)
                .field("started", &inner.started.load(Ordering::Relaxed))
                .field("busy_len", &inner.busy.lock().expect("lock poisoned").len())
                .finish(),
            None => f.debug_struct("Prefetcher").field("parallel", &0).finish(),
        }
    }
}

struct BusyGuard {
    busy: Arc<Mutex<HashSet<String>>>,
    key: String,
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.busy.lock().expect("lock poisoned").remove(&self.key);
    }
}

impl Prefetcher {
    /// Creates a new [`Prefetcher`] with `parallel` background worker tasks.
    ///
    /// When `parallel == 0`, prefetching is disabled and [`Prefetcher::fetch`] is a no-op.
    /// If called within an active Tokio runtime, worker tasks are spawned immediately;
    /// otherwise they are lazily spawned on the first [`Prefetcher::fetch`] call inside
    /// a Tokio runtime.
    pub fn new<F, Fut>(parallel: usize, fetch: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if parallel == 0 {
            return Self { inner: None };
        }

        let queue_cap = parallel.saturating_mul(4).max(10);
        let (tx, rx) = mpsc::channel(queue_cap);
        let fetch_fn: FetchCallback = Arc::new(move |key| Box::pin(fetch(key)));

        let inner = Arc::new(PrefetcherInner {
            parallel,
            tx,
            rx: Arc::new(AsyncMutex::new(rx)),
            busy: Arc::new(Mutex::new(HashSet::new())),
            fetch_fn,
            started: AtomicBool::new(false),
            start_lock: Mutex::new(()),
        });

        let prefetcher = Self { inner: Some(inner) };
        prefetcher.ensure_started();
        prefetcher
    }

    /// Returns a disabled no-op [`Prefetcher`].
    #[must_use]
    pub const fn disabled() -> Self {
        Self { inner: None }
    }

    /// Returns `true` if background prefetching is enabled (`parallel > 0`).
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Returns the configured worker parallelism (`0` when disabled).
    #[must_use]
    pub fn parallel(&self) -> usize {
        self.inner.as_ref().map_or(0, |i| i.parallel)
    }

    /// Schedules `key` for background prefetching without blocking.
    ///
    /// Returns `true` if `key` was newly queued, or `false` if prefetching is disabled,
    /// `key` is already pending/in-flight, or the pending queue is full.
    pub fn fetch(&self, key: impl AsRef<str>) -> bool {
        let Some(inner) = &self.inner else {
            return false;
        };
        self.ensure_started();

        let key = key.as_ref();
        let mut busy = inner.busy.lock().expect("lock poisoned");
        if busy.contains(key) {
            return false;
        }
        match inner.tx.try_send(key.to_owned()) {
            Ok(()) => {
                busy.insert(key.to_owned());
                true
            }
            Err(_) => false,
        }
    }

    fn ensure_started(&self) {
        let Some(inner) = &self.inner else {
            return;
        };
        if inner.started.load(Ordering::Acquire) {
            return;
        }
        let Ok(handle) = Handle::try_current() else {
            return;
        };
        let _guard = inner.start_lock.lock().expect("lock poisoned");
        if inner.started.load(Ordering::Relaxed) {
            return;
        }

        for _ in 0..inner.parallel {
            let rx = Arc::clone(&inner.rx);
            let busy = Arc::clone(&inner.busy);
            let fetch_fn = Arc::clone(&inner.fetch_fn);
            handle.spawn(async move {
                loop {
                    let key = {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    };
                    let Some(key) = key else {
                        break;
                    };
                    let _busy_guard = BusyGuard {
                        busy: Arc::clone(&busy),
                        key: key.clone(),
                    };
                    fetch_fn(key).await;
                }
            });
        }
        inner.started.store(true, Ordering::Release);
    }
}
