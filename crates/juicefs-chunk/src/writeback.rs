//! Asynchronous write-back upload queue for [`crate::CachedChunkStore`].

use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::ChunkError;

/// Bounded asynchronous write-back queue that stages block uploads in the background.
#[derive(Debug)]
pub struct WritebackQueue {
    permits: Arc<Semaphore>,
    tasks: Mutex<Vec<JoinHandle<Result<(), ChunkError>>>>,
}

impl WritebackQueue {
    /// Creates a new [`WritebackQueue`] allowing up to `max_concurrent` parallel uploads.
    #[must_use]
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// Enqueues an upload future to execute asynchronously.
    pub fn submit<F>(&self, fut: F)
    where
        F: Future<Output = Result<(), ChunkError>> + Send + 'static,
    {
        let permits = Arc::clone(&self.permits);
        let handle = tokio::spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .map_err(|_| ChunkError::Io(std::io::Error::other("writeback semaphore closed")))?;
            fut.await
        });
        let mut guard = self.tasks.lock().expect("lock poisoned");
        guard.retain(|h| !h.is_finished());
        guard.push(handle);
    }

    /// Returns the number of currently tracked background upload tasks.
    #[must_use]
    pub fn pending(&self) -> usize {
        let mut guard = self.tasks.lock().expect("lock poisoned");
        guard.retain(|h| !h.is_finished());
        guard.len()
    }

    /// Waits for all queued write-back uploads to complete, returning the first error if any.
    pub async fn flush(&self) -> Result<(), ChunkError> {
        let handles: Vec<_> = {
            let mut guard = self.tasks.lock().expect("lock poisoned");
            std::mem::take(&mut *guard)
        };
        let mut first_err: Option<ChunkError> = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(ChunkError::Io(std::io::Error::other(e.to_string())));
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }
}
