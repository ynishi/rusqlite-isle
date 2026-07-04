//! Cancellation token wired to SQLite's interrupt mechanism.
//!
//! A [`CancelToken`] covers the two stages a job can be in:
//!
//! 1. **queued** — the job has not started yet.  The SQLite thread checks
//!    the token immediately before running each job and drops the job
//!    without execution when the token has fired.
//! 2. **running** — the job registered the connection's
//!    [`InterruptHandle`](rusqlite::InterruptHandle) on the token.  Firing
//!    the token calls `sqlite3_interrupt`, which aborts the in-flight
//!    statement with `SQLITE_INTERRUPT`; the isle then normalizes that
//!    error to [`IsleError::Cancelled`](crate::IsleError::Cancelled) or
//!    [`IsleError::Timeout`](crate::IsleError::Timeout) depending on which
//!    path fired the token.

use crate::error::IsleError;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

const STATE_NONE: u8 = 0;
const STATE_CANCELLED: u8 = 1;
const STATE_TIMEOUT: u8 = 2;

struct TokenInner {
    /// 0 = not fired, 1 = cancelled, 2 = deadline expired.
    state: AtomicU8,
    /// Interrupt handle of the connection currently running this job.
    ///
    /// `Some` only while the associated job is executing; cleared as soon
    /// as the job completes so a late `cancel()` cannot interrupt an
    /// unrelated subsequent job.
    handle: Mutex<Option<rusqlite::InterruptHandle>>,
}

/// Cancellation signal shared between callers and the SQLite thread.
///
/// Clone is cheap (`Arc`).  `CancelToken` is `Send + Sync` and can be
/// fired from any thread.
///
/// A token is created per job (see
/// [`Isle::spawn_call`](crate::Isle::spawn_call)); firing it affects only
/// that job, in whichever stage it currently is (queued or running).
#[derive(Clone)]
pub struct CancelToken {
    inner: Arc<TokenInner>,
}

impl CancelToken {
    /// Create a new token (not fired).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TokenInner {
                state: AtomicU8::new(STATE_NONE),
                handle: Mutex::new(None),
            }),
        }
    }

    /// Request cancellation.
    ///
    /// If the job is still queued it will be dropped before execution.
    /// If it is running, the registered
    /// [`InterruptHandle`](rusqlite::InterruptHandle) is interrupted and
    /// the resulting `SQLITE_INTERRUPT` is normalized to
    /// [`IsleError::Cancelled`](crate::IsleError::Cancelled).
    ///
    /// Calling `cancel` after the job completed is a no-op (the handle is
    /// already detached).
    pub fn cancel(&self) {
        self.fire(STATE_CANCELLED);
    }

    /// Whether [`cancel`](Self::cancel) has been called.
    pub fn is_cancelled(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == STATE_CANCELLED
    }

    /// Mark the token as expired by a per-call deadline (internal).
    ///
    /// Fires the same interrupt path as `cancel` but the resulting error
    /// is normalized to [`IsleError::Timeout`](crate::IsleError::Timeout).
    pub(crate) fn expire(&self) {
        self.fire(STATE_TIMEOUT);
    }

    fn fire(&self, state: u8) {
        // Keep the first reason: cancel-then-timeout stays Cancelled.
        let _ = self.inner.state.compare_exchange(
            STATE_NONE,
            state,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if let Ok(guard) = self.inner.handle.lock() {
            if let Some(h) = guard.as_ref() {
                h.interrupt();
            }
        }
    }

    /// The error this token maps to, if it has fired.
    pub(crate) fn fired(&self) -> Option<IsleError> {
        match self.inner.state.load(Ordering::Acquire) {
            STATE_CANCELLED => Some(IsleError::Cancelled),
            STATE_TIMEOUT => Some(IsleError::Timeout),
            _ => None,
        }
    }

    /// Register the running connection's interrupt handle.
    ///
    /// Called by the SQLite thread right before executing the job.  If the
    /// token already fired between the queued-stage check and this call,
    /// the interrupt is delivered immediately so the race window is closed.
    pub(crate) fn attach(&self, handle: rusqlite::InterruptHandle) {
        if let Ok(mut guard) = self.inner.handle.lock() {
            *guard = Some(handle);
        }
        if self.inner.state.load(Ordering::Acquire) != STATE_NONE {
            if let Ok(guard) = self.inner.handle.lock() {
                if let Some(h) = guard.as_ref() {
                    h.interrupt();
                }
            }
        }
    }

    /// Detach the interrupt handle after the job finished.
    ///
    /// Guarantees that a late `cancel()` on this token cannot interrupt a
    /// subsequent, unrelated job on the same connection.
    pub(crate) fn detach(&self) {
        if let Ok(mut guard) = self.inner.handle.lock() {
            *guard = None;
        }
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelToken")
            .field("state", &self.inner.state.load(Ordering::Acquire))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_default_not_fired() {
        let token = CancelToken::new();
        assert!(!token.is_cancelled());
        assert!(token.fired().is_none());
    }

    #[test]
    fn token_cancel_sets_state_across_clones() {
        let token = CancelToken::new();
        let clone = token.clone();
        token.cancel();
        assert!(clone.is_cancelled());
        assert!(matches!(clone.fired(), Some(IsleError::Cancelled)));
    }

    #[test]
    fn token_first_reason_wins() {
        let token = CancelToken::new();
        token.expire();
        token.cancel();
        assert!(matches!(token.fired(), Some(IsleError::Timeout)));
    }
}
