//! Task handle — a cancellable pending result for a single SQLite job.
//!
//! A [`Task`] is returned by [`Isle::spawn_call`](crate::Isle::spawn_call)
//! and [`Isle::try_call`](crate::Isle::try_call).  It provides a
//! [`CancelToken`] for interruption and a blocking
//! [`wait`](Task::wait) method to collect the result.

use crate::error::IsleError;
use crate::token::CancelToken;
use std::sync::mpsc;

/// Handle to a pending SQLite job (sync API).
///
/// The job runs on the SQLite thread.  The caller can:
/// - [`wait`](Task::wait) for the result (blocking),
/// - [`cancel`](Task::cancel) the job (queued jobs are dropped, running
///   statements are interrupted),
/// - [`try_recv`](Task::try_recv) to poll without blocking,
/// - clone the [`cancel_token`](Task::cancel_token) to cancel from
///   another thread.
///
/// Unlike [`AsyncTask`](crate::AsyncTask), dropping a `Task` does **not**
/// cancel the job — the sync API has no implicit drop-cancel.
pub struct Task<T> {
    rx: mpsc::Receiver<Result<T, IsleError>>,
    cancel: CancelToken,
}

impl<T> Task<T> {
    pub(crate) fn new(rx: mpsc::Receiver<Result<T, IsleError>>, cancel: CancelToken) -> Self {
        Self { rx, cancel }
    }

    /// Block until the result is available.
    ///
    /// Returns [`IsleError::Closed`] if the SQLite thread went away
    /// without delivering a result (shutdown or aborted).
    pub fn wait(self) -> Result<T, IsleError> {
        self.rx.recv().map_err(|_| IsleError::Closed)?
    }

    /// Cancel the job.
    ///
    /// A queued job is dropped before execution; a running statement is
    /// interrupted via `sqlite3_interrupt`.  The task will resolve to
    /// [`IsleError::Cancelled`].
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Non-blocking poll for the result.
    ///
    /// Returns `None` while the job is still pending.
    pub fn try_recv(&self) -> Option<Result<T, IsleError>> {
        self.rx.try_recv().ok()
    }

    /// Access the cancel token (e.g. to clone and share with other code).
    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel
    }

    /// Borrow the underlying receiver (crate-internal, for timeout waits).
    pub(crate) fn rx_ref(&self) -> &mpsc::Receiver<Result<T, IsleError>> {
        &self.rx
    }
}
