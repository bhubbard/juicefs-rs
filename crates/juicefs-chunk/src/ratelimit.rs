//! Token-bucket rate limiter for object storage upload/download bandwidth control.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

/// Asynchronous token-bucket rate limiter supporting burst capacity and smooth pacing.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    inner: Arc<TokenBucketInner>,
}

#[derive(Debug)]
struct TokenBucketInner {
    rate: f64,
    capacity: u64,
    state: Mutex<BucketState>,
}

#[derive(Debug)]
struct BucketState {
    available: f64,
    last_refill: Instant,
}

impl TokenBucket {
    /// Creates a new [`TokenBucket`] refilling at `rate` tokens/bytes per second
    /// with maximum burst `capacity` (initially full).
    ///
    /// If `rate <= 0.0`, rate limiting is effectively disabled and [`TokenBucket::acquire`]
    /// returns immediately.
    #[must_use]
    pub fn new(rate: f64, capacity: u64) -> Self {
        let rate = if rate.is_finite() && rate > 0.0 {
            rate
        } else {
            0.0
        };
        Self {
            inner: Arc::new(TokenBucketInner {
                rate,
                capacity,
                state: Mutex::new(BucketState {
                    available: capacity as f64,
                    last_refill: Instant::now(),
                }),
            }),
        }
    }

    /// Creates a [`TokenBucket`] sized for `bytes_per_sec` bandwidth with a 100ms burst window.
    #[must_use]
    pub fn from_bytes_per_sec(bytes_per_sec: u64) -> Self {
        let burst = (bytes_per_sec / 10).max(1);
        Self::new(bytes_per_sec as f64, burst)
    }

    /// Returns the configured refill rate in tokens (bytes) per second.
    #[must_use]
    pub fn rate(&self) -> f64 {
        self.inner.rate
    }

    /// Returns the burst capacity in tokens (bytes).
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    /// Returns the currently available (non-negative) token count after refilling.
    #[must_use]
    pub fn available(&self) -> f64 {
        let mut guard = self.inner.state.lock();
        self.refill_locked(&mut guard);
        guard.available.max(0.0)
    }

    /// Attempts to consume `tokens` immediately without waiting.
    ///
    /// Returns `true` if enough tokens were available, or `false` without modifying
    /// the bucket balance otherwise.
    #[must_use]
    pub fn try_acquire(&self, tokens: u64) -> bool {
        if tokens == 0 || self.inner.rate <= 0.0 {
            return true;
        }
        let mut guard = self.inner.state.lock();
        self.refill_locked(&mut guard);
        let needed = tokens as f64;
        if guard.available >= needed {
            guard.available -= needed;
            true
        } else {
            false
        }
    }

    /// Consumes `tokens`, asynchronously sleeping until the reserved bandwidth is available.
    ///
    /// Requests larger than [`TokenBucket::capacity`] are supported via token reservation
    /// without deadlocking.
    pub async fn acquire(&self, tokens: u64) {
        if tokens == 0 || self.inner.rate <= 0.0 {
            return;
        }

        let wait_dur = {
            let mut guard = self.inner.state.lock();
            self.refill_locked(&mut guard);
            guard.available -= tokens as f64;
            if guard.available < 0.0 {
                let secs = -guard.available / self.inner.rate;
                if secs.is_finite() && secs > 0.0 {
                    Some(Duration::from_secs_f64(secs))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(dur) = wait_dur {
            tokio::time::sleep(dur).await;
        }
    }

    fn refill_locked(&self, state: &mut BucketState) {
        let now = Instant::now();
        let elapsed = now
            .saturating_duration_since(state.last_refill)
            .as_secs_f64();
        state.last_refill = now;
        if elapsed > 0.0 && self.inner.rate > 0.0 {
            state.available =
                (state.available + elapsed * self.inner.rate).min(self.inner.capacity as f64);
        }
    }
}
