//! Async task handle — a cancellable [`Future`] for a single SQLite job.
//!
//! An [`AsyncTask`] is returned by
//! [`AsyncIsle::spawn_call`](crate::AsyncIsle::spawn_call) and
//! [`AsyncIsle::try_call`](crate::AsyncIsle::try_call).  It implements
//! [`Future`], so it can be `.await`ed directly.
//!
//! # Drop = cancel
//!
//! Dropping an `AsyncTask` **cancels** the underlying job: a queued job
//! is discarded before execution, a running statement is interrupted.
//! This makes "stopped awaiting" and "stopped running" coincide by
//! default — a job whose future was abandoned does not keep burning the
//! SQLite thread.  Opt out with [`detach`](AsyncTask::detach) for
//! fire-and-forget jobs.

use crate::error::IsleError;
use crate::token::CancelToken;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Async handle to a pending SQLite job.
///
/// Implements [`Future`] — `.await` it to get the result.
///
/// # Cancellation
///
/// Three ways to stop the job:
///
/// - **drop** the task (default drop-cancel; see module docs),
/// - call [`cancel`](AsyncTask::cancel) explicitly,
/// - clone the [`cancel_token`](AsyncTask::cancel_token) and fire it
///   from anywhere.
///
/// ```rust
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use rusqlite_isle::{AsyncIsle, IsleError};
/// use std::time::Duration;
///
/// let (isle, driver) = AsyncIsle::open_in_memory(|_conn| Ok(())).await?;
/// let task = isle.spawn_call(|conn| {
///     conn.query_row(
///         "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
///          WHERE x < 1000000000) SELECT count(*) FROM c",
///         [],
///         |r| r.get::<_, i64>(0),
///     )
/// });
/// let token = task.cancel_token().clone();
/// tokio::spawn(async move {
///     tokio::time::sleep(Duration::from_millis(50)).await;
///     token.cancel();
/// });
/// assert!(matches!(task.await, Err(IsleError::Cancelled)));
/// driver.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct AsyncTask<T> {
    rx: tokio::sync::oneshot::Receiver<Result<T, IsleError>>,
    cancel: CancelToken,
    detached: bool,
}

impl<T> AsyncTask<T> {
    pub(crate) fn new(
        rx: tokio::sync::oneshot::Receiver<Result<T, IsleError>>,
        cancel: CancelToken,
    ) -> Self {
        Self {
            rx,
            cancel,
            detached: false,
        }
    }

    /// Cancel the job.
    ///
    /// A queued job is dropped before execution; a running statement is
    /// interrupted via `sqlite3_interrupt`.  The task resolves to
    /// [`IsleError::Cancelled`].
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Access the cancel token (e.g. to clone and share with another task).
    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel
    }

    /// Detach the job from this handle (opt out of drop-cancel).
    ///
    /// The job keeps running on the SQLite thread to completion; its
    /// result is discarded.  Use for fire-and-forget writes.
    pub fn detach(mut self) {
        self.detached = true;
        // self drops here; Drop sees `detached` and skips the cancel.
    }
}

impl<T> Future for AsyncTask<T> {
    type Output = Result<T, IsleError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            // Sender dropped without a result: the SQLite thread shut
            // down or aborted while the job was queued.
            Poll::Ready(Err(_)) => Poll::Ready(Err(IsleError::Closed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for AsyncTask<T> {
    fn drop(&mut self) {
        if !self.detached {
            // Harmless if the job already completed: the interrupt
            // handle was detached when the job finished.
            self.cancel.cancel();
        }
    }
}
