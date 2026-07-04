//! SQLite thread internals — the job loop that owns the `Connection`.
//!
//! This module is internal.  The public entry points are
//! [`Isle`](crate::Isle) and (with the `tokio` feature)
//! [`AsyncIsle`](crate::AsyncIsle).
//!
//! Both the sync and async front-ends funnel into the same
//! [`run_loop`] / [`make_job`] machinery: a job is a boxed closure that
//! receives `&mut Connection`, performs its own cancellation bookkeeping
//! (queued-stage token check, interrupt-handle attach/detach,
//! `catch_unwind`, error normalization) and sends its typed result
//! through a channel it captured at construction time.

use crate::error::IsleError;
use crate::token::CancelToken;
use rusqlite::Connection;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Boxed job executed on the SQLite thread.
///
/// The closure owns its response channel; the loop only needs to know
/// whether the job panicked (to run the post-panic health check).
pub(crate) type Job = Box<dyn FnOnce(&mut Connection) -> JobOutcome + Send>;

/// What the loop needs to know after running a job.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum JobOutcome {
    /// The job ran to completion (success, error, or pre-run cancel).
    Completed,
    /// The job closure panicked (already reported to the caller as
    /// [`IsleError::Panicked`]); the loop must health-check the
    /// connection before serving further jobs.
    Panicked,
}

/// Request sent from handles to the SQLite thread.
pub(crate) enum Request {
    /// Run a job.
    Job(Job),
    /// Graceful shutdown (jobs already queued ahead of this message are
    /// drained first).
    Shutdown,
}

/// Build a job wrapper around a user closure.
///
/// The wrapper performs, in order:
///
/// 1. queued-stage cancel check (token fired before execution → respond
///    with `Cancelled` / `Timeout` without touching the connection);
/// 2. interrupt-handle registration on the token (running-stage cancel);
/// 3. `catch_unwind` around the user closure;
/// 4. interrupt-handle detach (so a late fire cannot hit the next job);
/// 5. error normalization: `SQLITE_INTERRUPT` is mapped back to
///    `Cancelled` / `Timeout` from the token state, or to `Closed` when
///    the isle is aborting (`shutdown_now`); panics become `Panicked`.
pub(crate) fn make_job<T, F, R>(
    f: F,
    token: CancelToken,
    abort: Arc<AtomicBool>,
    respond: R,
) -> Job
where
    T: Send + 'static,
    F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    R: FnOnce(Result<T, IsleError>) + Send + 'static,
{
    Box::new(move |conn: &mut Connection| {
        if let Some(err) = token.fired() {
            respond(Err(err));
            return JobOutcome::Completed;
        }

        token.attach(conn.get_interrupt_handle());
        let result = catch_unwind(AssertUnwindSafe(|| f(conn)));
        token.detach();

        match result {
            Ok(Ok(value)) => {
                respond(Ok(value));
                JobOutcome::Completed
            }
            Ok(Err(e)) => {
                respond(Err(normalize_error(e, &token, &abort)));
                JobOutcome::Completed
            }
            Err(payload) => {
                respond(Err(IsleError::Panicked(panic_message(payload))));
                JobOutcome::Panicked
            }
        }
    })
}

/// Map a rusqlite error to an [`IsleError`], folding `SQLITE_INTERRUPT`
/// back into the isle-level reason that triggered it.
fn normalize_error(e: rusqlite::Error, token: &CancelToken, abort: &AtomicBool) -> IsleError {
    if IsleError::is_interrupt(&e) {
        if let Some(err) = token.fired() {
            return err;
        }
        if abort.load(Ordering::Acquire) {
            return IsleError::Closed;
        }
    }
    IsleError::Sqlite(e)
}

/// Extract a readable message from a panic payload.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Run the job loop on the current thread.
///
/// `next` blocks for the next request and returns `None` when the channel
/// is disconnected.  The loop exits on:
///
/// - `Shutdown` request (graceful — earlier queued jobs already drained),
/// - channel disconnect (all senders dropped),
/// - failed post-panic health check (`SELECT 1`), in which case the isle
///   must not keep serving jobs on a possibly poisoned connection.
///
/// While `abort` is set (`shutdown_now`), remaining jobs are dropped
/// without execution; dropping a job drops its captured response sender,
/// which the caller observes as [`IsleError::Closed`].
pub(crate) fn run_loop(
    mut conn: Connection,
    mut next: impl FnMut() -> Option<Request>,
    abort: &AtomicBool,
) {
    while let Some(req) = next() {
        match req {
            Request::Job(job) => {
                if abort.load(Ordering::Acquire) {
                    drop(job);
                    continue;
                }
                match job(&mut conn) {
                    JobOutcome::Completed => {}
                    JobOutcome::Panicked => {
                        let healthy = conn
                            .query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
                            .is_ok();
                        if !healthy {
                            break;
                        }
                    }
                }
            }
            Request::Shutdown => break,
        }
    }
}
