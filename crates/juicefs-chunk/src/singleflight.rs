//! Request deduplication (`singleflight`) for concurrent block loads.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::watch;

type FlightMap<T, E> = Mutex<HashMap<String, watch::Receiver<Option<Result<T, Arc<E>>>>>>;

/// Deduplicates concurrent asynchronous computations keyed by a string identifier.
///
/// When multiple callers concurrently invoke [`SingleFlight::execute`] for the same key,
/// only a single leader future executes while concurrent callers await its shared result.
/// Errors are wrapped in [`Arc`] so all waiters receive the failure; nothing is cached
/// after a flight finishes.
#[derive(Debug)]
pub struct SingleFlight<T, E> {
    flights: FlightMap<T, E>,
}

impl<T, E> Default for SingleFlight<T, E> {
    fn default() -> Self {
        Self::new()
    }
}

enum FlightRole<T, E> {
    Leader(watch::Sender<Option<Result<T, Arc<E>>>>),
    Waiter(watch::Receiver<Option<Result<T, Arc<E>>>>),
}

struct FlightGuard<'a, T, E> {
    flights: &'a FlightMap<T, E>,
    key: String,
    completed: bool,
}

impl<T, E> Drop for FlightGuard<'_, T, E> {
    fn drop(&mut self) {
        if !self.completed {
            self.flights.lock().remove(&self.key);
        }
    }
}

impl<T, E> SingleFlight<T, E> {
    /// Creates a new [`SingleFlight`] coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            flights: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the number of keys currently in flight.
    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.flights.lock().len()
    }
}

impl<T, E> SingleFlight<T, E>
where
    T: Clone,
{
    /// Executes `f` for `key`, deduplicating any concurrent calls for the same `key`.
    ///
    /// If another task is already executing a flight for `key`, this caller awaits
    /// the shared outcome instead of invoking `f`. If the leader task is cancelled
    /// before producing a result, a waiting caller automatically retries and elects
    /// a new leader.
    pub async fn execute<F, Fut>(&self, key: &str, f: F) -> Result<T, Arc<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut f = Some(f);

        loop {
            let role = {
                let mut guard = self.flights.lock();
                if let Some(rx) = guard.get(key) {
                    FlightRole::Waiter(rx.clone())
                } else {
                    let (tx, rx) = watch::channel(None);
                    guard.insert(key.to_owned(), rx);
                    FlightRole::Leader(tx)
                }
            };

            match role {
                FlightRole::Leader(tx) => {
                    // `guard` is declared after `tx` so on cancellation `guard` drops first
                    // (removing `key` from `flights`) before `tx` closes the channel.
                    let mut guard = FlightGuard {
                        flights: &self.flights,
                        key: key.to_owned(),
                        completed: false,
                    };

                    let fut = (f.take().expect("leader consumes f exactly once"))();
                    let res = fut.await.map_err(Arc::new);

                    {
                        let mut map = self.flights.lock();
                        map.remove(key);
                        guard.completed = true;
                    }
                    let _ = tx.send(Some(res.clone()));
                    return res;
                }
                FlightRole::Waiter(mut rx) => {
                    loop {
                        if let Some(res) = rx.borrow_and_update().as_ref().cloned() {
                            return res;
                        }
                        if rx.changed().await.is_err() {
                            if let Some(res) = rx.borrow().as_ref().cloned() {
                                return res;
                            }
                            // Leader was cancelled before sending; retry to elect a new leader.
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Joins an already in-flight execution for `key` if one exists.
    ///
    /// Returns `None` immediately when no flight for `key` is currently active.
    pub async fn try_piggyback(&self, key: &str) -> Option<Result<T, Arc<E>>> {
        let mut rx = self.flights.lock().get(key).cloned()?;
        loop {
            if let Some(res) = rx.borrow_and_update().as_ref().cloned() {
                return Some(res);
            }
            if rx.changed().await.is_err() {
                return rx.borrow().as_ref().cloned();
            }
        }
    }
}
