//! Error types for rusqlite-isle.

/// Errors returned by isle operations.
///
/// SQLite-level failures are kept in [`Sqlite`](Self::Sqlite) (with the
/// original [`rusqlite::Error`] as source), while isle-level conditions
/// ([`Cancelled`](Self::Cancelled), [`Timeout`](Self::Timeout),
/// [`Closed`](Self::Closed), [`QueueFull`](Self::QueueFull),
/// [`Panicked`](Self::Panicked)) live one level above so callers can
/// decide whether a retry makes sense purely from the variant.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IsleError {
    /// A SQL execution error from rusqlite (including `SQLITE_BUSY`).
    ///
    /// Note that `SQLITE_BUSY` after `busy_timeout` stays in this variant;
    /// it is **not** reported as [`Timeout`](Self::Timeout), which is
    /// reserved for the isle's own per-call deadline.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// The job was cancelled via [`CancelToken`](crate::CancelToken)
    /// (explicit `cancel()`, or drop-cancel of an
    /// [`AsyncTask`](crate::AsyncTask) when the `tokio` feature is on).
    ///
    /// Queued jobs are dropped before execution; running jobs are
    /// interrupted via `sqlite3_interrupt` and normalized to this variant.
    #[error("cancelled")]
    Cancelled,

    /// The per-call deadline elapsed
    /// (see [`Isle::call_timeout`](crate::Isle::call_timeout)).
    ///
    /// Internally the deadline fires the same interrupt path as
    /// cancellation; the token state distinguishes the two.
    #[error("timeout")]
    Timeout,

    /// The isle has shut down (or the SQLite thread is gone) and can no
    /// longer accept or complete jobs.
    #[error("isle closed")]
    Closed,

    /// The bounded request channel is full (backpressure).
    ///
    /// Only returned by `try_call` / `spawn_call` style non-waiting
    /// submissions.  Unlike [`Closed`](Self::Closed) this is transient —
    /// the SQLite thread is alive and retrying may succeed.
    #[error("queue full (backpressure)")]
    QueueFull,

    /// The job closure panicked.
    ///
    /// The panic is caught with `catch_unwind` and converted to this
    /// variant (payload message included).  After a panic the isle runs a
    /// `SELECT 1` health check on the connection; if that fails, the isle
    /// transitions to [`Closed`](Self::Closed) instead of continuing in a
    /// poisoned state.
    #[error("job panicked: {0}")]
    Panicked(String),
}

impl IsleError {
    /// Whether the underlying rusqlite error is `SQLITE_INTERRUPT`.
    pub(crate) fn is_interrupt(e: &rusqlite::Error) -> bool {
        matches!(
            e,
            rusqlite::Error::SqliteFailure(f, _)
                if f.code == rusqlite::ErrorCode::OperationInterrupted
        )
    }
}
