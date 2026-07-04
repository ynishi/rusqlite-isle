//! Async handle and driver for the thread-isolated SQLite connection.
//!
//! This module follows the **Handle / Driver** separation pattern
//! (tokio `Runtime` / `Handle`, Alice Ryhl's actor pattern):
//!
//! - [`AsyncIsle`] is a **lightweight, cloneable handle** that sends
//!   jobs to the SQLite thread.  It can be shared across tasks without
//!   `Arc`.
//! - [`AsyncIsleDriver`] is the **lifecycle owner** that holds the OS
//!   thread's `JoinHandle` and provides
//!   [`shutdown`](AsyncIsleDriver::shutdown) (drain + join) and
//!   [`shutdown_now`](AsyncIsleDriver::shutdown_now) (abort).
//!
//! Communication uses a bounded `tokio::sync::mpsc` channel so callers
//! get backpressure ([`IsleError::QueueFull`]) rather than unbounded
//! memory growth.  The SQLite thread itself needs no tokio runtime — it
//! drains the channel with `blocking_recv`.
//!
//! # Shutdown semantics
//!
//! - [`shutdown`](AsyncIsleDriver::shutdown): a `Shutdown` message is
//!   queued behind already-submitted jobs, so those **drain to
//!   completion** before the thread exits.  The driver then awaits a
//!   oneshot completion signal (pure async, no `spawn_blocking`) and
//!   joins the already-exited thread.
//! - [`shutdown_now`](AsyncIsleDriver::shutdown_now): an abort flag is
//!   set and the connection is interrupted.  The running statement stops
//!   with `SQLITE_INTERRUPT` (normalized to [`IsleError::Closed`]);
//!   queued jobs are discarded without execution (their callers observe
//!   [`IsleError::Closed`]).
//! - **Drop without shutdown**: the driver does *not* send `Shutdown` on
//!   drop.  The thread exits naturally when all handles and the driver
//!   are dropped and the channel disconnects ("in Rust, cancellation is
//!   drop").

use crate::async_task::AsyncTask;
use crate::error::IsleError;
use crate::thread::{self, Request};
use crate::token::CancelToken;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// Default capacity for the bounded request channel.
const DEFAULT_CHANNEL_CAPACITY: usize = 256;

enum OpenTarget {
    Path(PathBuf),
    InMemory,
}

/// Cloneable async handle to a thread-isolated SQLite connection.
///
/// `AsyncIsle` holds only a channel sender and can be freely cloned to
/// share across tokio tasks — no `Arc` wrapper needed.
///
/// # Lifecycle
///
/// 1. [`AsyncIsle::spawn`] / [`AsyncIsle::open_in_memory`] create the
///    connection and return `(AsyncIsle, AsyncIsleDriver)`.
/// 2. Clone and distribute the handle.
/// 3. Use [`call`](Self::call) / [`call_timeout`](Self::call_timeout) /
///    [`spawn_call`](Self::spawn_call) / [`try_call`](Self::try_call)
///    from any task.
/// 4. Call [`AsyncIsleDriver::shutdown`] for a clean drain + join.
#[derive(Clone)]
pub struct AsyncIsle {
    tx: tokio::sync::mpsc::Sender<Request>,
    abort: Arc<AtomicBool>,
}

/// Lifecycle driver for the SQLite thread (async API).
///
/// Sole owner of the OS thread's [`JoinHandle`]; not `Clone`.
///
/// Shutdown semantics: [`shutdown`](Self::shutdown) drains queued jobs
/// then joins; [`shutdown_now`](Self::shutdown_now) interrupts the
/// running statement and discards queued jobs; dropping the driver does
/// **not** stop the thread while [`AsyncIsle`] handles are still alive.
#[must_use = "call .shutdown().await for a clean thread join"]
pub struct AsyncIsleDriver {
    tx: tokio::sync::mpsc::Sender<Request>,
    abort: Arc<AtomicBool>,
    interrupt: rusqlite::InterruptHandle,
    done_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    join: Option<JoinHandle<()>>,
}

/// Builder for [`AsyncIsle`] with configurable parameters.
///
/// Create via [`AsyncIsle::builder`].
///
/// # Example
///
/// ```rust
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use rusqlite_isle::AsyncIsle;
/// use std::time::Duration;
///
/// let (isle, driver) = AsyncIsle::builder()
///     .channel_capacity(64)
///     .thread_name("sqlite-worker")
///     .busy_timeout(Duration::from_millis(100))
///     .open_in_memory(|_conn| Ok(()))
///     .await?;
/// driver.shutdown().await?;
/// # let _ = isle;
/// # Ok(())
/// # }
/// ```
pub struct AsyncIsleBuilder {
    channel_capacity: usize,
    thread_name: String,
    busy_timeout: Option<Duration>,
    open_flags: Option<rusqlite::OpenFlags>,
}

impl Default for AsyncIsleBuilder {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            thread_name: "rusqlite-isle-async".into(),
            busy_timeout: None,
            open_flags: None,
        }
    }
}

impl AsyncIsleBuilder {
    /// Set the bounded channel capacity (backpressure limit).
    ///
    /// When the channel is full, [`AsyncIsle::try_call`] and
    /// [`AsyncIsle::spawn_call`] report [`IsleError::QueueFull`];
    /// [`AsyncIsle::call`] awaits capacity instead.
    ///
    /// Default: 256.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the OS thread name (visible in debuggers and `top`).
    ///
    /// Default: `"rusqlite-isle-async"`.
    pub fn thread_name(mut self, name: &str) -> Self {
        self.thread_name = name.to_string();
        self
    }

    /// Set `sqlite3_busy_timeout` on the connection after opening.
    ///
    /// Lock contention exceeding this budget surfaces as
    /// [`IsleError::Sqlite`] (`SQLITE_BUSY`), distinct from the isle's
    /// per-call [`IsleError::Timeout`].
    pub fn busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = Some(timeout);
        self
    }

    /// Open the database with explicit [`rusqlite::OpenFlags`]
    /// (e.g. read-only for reader isles in a WAL setup).
    ///
    /// Only applies to [`spawn`](Self::spawn); ignored by
    /// [`open_in_memory`](Self::open_in_memory).
    pub fn open_flags(mut self, flags: rusqlite::OpenFlags) -> Self {
        self.open_flags = Some(flags);
        self
    }

    /// Open a database file with these settings.
    ///
    /// See [`AsyncIsle::spawn`] for details.
    pub async fn spawn<F>(
        self,
        path: impl AsRef<Path>,
        init: F,
    ) -> Result<(AsyncIsle, AsyncIsleDriver), IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        AsyncIsle::spawn_inner(OpenTarget::Path(path.as_ref().to_path_buf()), init, self).await
    }

    /// Open an in-memory database with these settings.
    ///
    /// See [`AsyncIsle::open_in_memory`] for details.
    pub async fn open_in_memory<F>(
        self,
        init: F,
    ) -> Result<(AsyncIsle, AsyncIsleDriver), IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        AsyncIsle::spawn_inner(OpenTarget::InMemory, init, self).await
    }
}

impl AsyncIsle {
    /// Create a builder for configuring the async isle.
    pub fn builder() -> AsyncIsleBuilder {
        AsyncIsleBuilder::default()
    }

    /// Open a database file on a dedicated thread (default settings).
    ///
    /// Returns `(handle, driver)`:
    /// - **handle** ([`AsyncIsle`]) — clone and share freely,
    /// - **driver** ([`AsyncIsleDriver`]) — call
    ///   [`shutdown`](AsyncIsleDriver::shutdown) when done.
    ///
    /// The `init` closure runs on the SQLite thread before any jobs are
    /// processed (pragmas, migrations, etc.).
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::Sqlite`] if opening or `init` fails.
    pub async fn spawn<F>(
        path: impl AsRef<Path>,
        init: F,
    ) -> Result<(Self, AsyncIsleDriver), IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        AsyncIsleBuilder::default().spawn(path, init).await
    }

    /// Open an in-memory database on a dedicated thread (default settings).
    ///
    /// See [`spawn`](Self::spawn) for details.
    pub async fn open_in_memory<F>(init: F) -> Result<(Self, AsyncIsleDriver), IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        AsyncIsleBuilder::default().open_in_memory(init).await
    }

    async fn spawn_inner<F>(
        target: OpenTarget,
        init: F,
        builder: AsyncIsleBuilder,
    ) -> Result<(Self, AsyncIsleDriver), IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Request>(builder.channel_capacity);
        let (init_tx, init_rx) =
            tokio::sync::oneshot::channel::<Result<rusqlite::InterruptHandle, IsleError>>();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let abort = Arc::new(AtomicBool::new(false));
        let abort_thread = Arc::clone(&abort);

        let busy_timeout = builder.busy_timeout;
        let open_flags = builder.open_flags;

        let join = std::thread::Builder::new()
            .name(builder.thread_name)
            .spawn(move || {
                let opened = match target {
                    OpenTarget::Path(path) => match open_flags {
                        Some(flags) => Connection::open_with_flags(path, flags),
                        None => Connection::open(path),
                    },
                    OpenTarget::InMemory => Connection::open_in_memory(),
                };
                let mut conn = match opened {
                    Ok(conn) => conn,
                    Err(e) => {
                        let _ = init_tx.send(Err(IsleError::Sqlite(e)));
                        let _ = done_tx.send(());
                        return;
                    }
                };

                let setup = busy_timeout
                    .map_or(Ok(()), |t| conn.busy_timeout(t))
                    .and_then(|()| init(&mut conn));
                if let Err(e) = setup {
                    let _ = init_tx.send(Err(IsleError::Sqlite(e)));
                    let _ = done_tx.send(());
                    return;
                }

                let _ = init_tx.send(Ok(conn.get_interrupt_handle()));
                thread::run_loop(conn, || rx.blocking_recv(), &abort_thread);
                // Signal completion so shutdown().await never hangs.
                let _ = done_tx.send(());
            })
            .map_err(|_| IsleError::Closed)?;

        let interrupt = match init_rx.await {
            Ok(Ok(handle)) => handle,
            Ok(Err(e)) => {
                let _ = join.join();
                return Err(e);
            }
            Err(_) => {
                let _ = join.join();
                return Err(IsleError::Closed);
            }
        };

        let handle = Self {
            tx: tx.clone(),
            abort: Arc::clone(&abort),
        };
        let driver = AsyncIsleDriver {
            tx,
            abort,
            interrupt,
            done_rx: Some(done_rx),
            join: Some(join),
        };

        Ok((handle, driver))
    }

    /// Run a closure against the connection.
    ///
    /// The closure executes on the SQLite thread with exclusive access;
    /// the caller's tokio task awaits a oneshot channel and is never
    /// blocked.  Open transactions inside the closure and commit before
    /// returning — an uncommitted [`rusqlite::Transaction`] rolls back on
    /// drop.
    ///
    /// When the bounded channel is full, this **awaits capacity**
    /// (backpressure).  Use [`try_call`](Self::try_call) for an
    /// immediate [`IsleError::QueueFull`] instead.
    pub async fn call<T, F>(&self, f: F) -> Result<T, IsleError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let (job, task) = self.make_job_task(f);
        self.tx
            .send(Request::Job(job))
            .await
            .map_err(|_| IsleError::Closed)?;
        task.await
    }

    /// Run a closure with a per-call deadline.
    ///
    /// If the deadline elapses before the job completes, the job is
    /// cancelled through the interrupt path and this returns
    /// [`IsleError::Timeout`].  `SQLITE_BUSY` from lock contention stays
    /// [`IsleError::Sqlite`].
    pub async fn call_timeout<T, F>(&self, timeout: Duration, f: F) -> Result<T, IsleError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let (job, mut task) = self.make_job_task(f);
        self.tx
            .send(Request::Job(job))
            .await
            .map_err(|_| IsleError::Closed)?;
        match tokio::time::timeout(timeout, &mut task).await {
            Ok(result) => result,
            Err(_elapsed) => {
                task.cancel_token().expire();
                // The job observes the interrupt (or the queued-stage
                // check) and still delivers a normalized result.
                task.await
            }
        }
    }

    /// Submit a closure and return a cancellable [`AsyncTask`].
    ///
    /// Non-blocking: enqueues via `try_send`.  When the channel is full
    /// the returned task resolves immediately to
    /// [`IsleError::QueueFull`].
    ///
    /// **Dropping the returned task cancels the job** — hold it or
    /// [`detach`](AsyncTask::detach) it.
    pub fn spawn_call<T, F>(&self, f: F) -> AsyncTask<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        match self.try_call(f) {
            Ok(task) => task,
            Err(e) => make_error_task(e),
        }
    }

    /// Submit a closure without waiting for queue capacity.
    ///
    /// Returns [`IsleError::QueueFull`] immediately when the bounded
    /// channel is full, [`IsleError::Closed`] when the isle has shut
    /// down.  On success returns an [`AsyncTask`] to await.
    pub fn try_call<T, F>(&self, f: F) -> Result<AsyncTask<T>, IsleError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let (job, task) = self.make_job_task(f);
        match self.tx.try_send(Request::Job(job)) {
            Ok(()) => Ok(task),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Err(IsleError::QueueFull),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(IsleError::Closed),
        }
    }

    /// Check if the SQLite thread is still accepting jobs.
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }

    // ── internal ────────────────────────────────────────────────────

    fn make_job_task<T, F>(&self, f: F) -> (thread::Job, AsyncTask<T>)
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let token = CancelToken::new();
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let job = thread::make_job(f, token.clone(), Arc::clone(&self.abort), move |result| {
            let _ = resp_tx.send(result);
        });
        (job, AsyncTask::new(resp_rx, token))
    }
}

impl AsyncIsleDriver {
    /// Graceful shutdown: drain queued jobs, stop the thread, join it.
    ///
    /// The `Shutdown` message queues **behind** already-submitted jobs,
    /// so those run to completion first.  Completion is then awaited via
    /// a oneshot signal (pure async — no `spawn_blocking` thread is
    /// consumed) and the already-exited thread is joined.
    ///
    /// After shutdown, all [`AsyncIsle`] handles report
    /// [`IsleError::Closed`].
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::Panicked`] if the SQLite thread panicked.
    pub async fn shutdown(mut self) -> Result<(), IsleError> {
        // send().await respects backpressure; try_send would silently
        // drop the shutdown signal when the channel is full.
        let _ = self.tx.send(Request::Shutdown).await;
        self.finish().await
    }

    /// Immediate shutdown: abort the running statement and discard
    /// queued jobs.
    ///
    /// - The abort flag stops the loop from executing further jobs;
    ///   discarded jobs resolve to [`IsleError::Closed`] on the caller
    ///   side.
    /// - The in-flight statement is interrupted (`sqlite3_interrupt`)
    ///   and its error is normalized to [`IsleError::Closed`].
    /// - An uncommitted transaction in the interrupted job rolls back
    ///   via [`rusqlite::Transaction`]'s drop guard.
    pub async fn shutdown_now(mut self) -> Result<(), IsleError> {
        self.abort.store(true, Ordering::Release);
        self.interrupt.interrupt();
        // The loop is discarding jobs now, so capacity frees quickly.
        let _ = self.tx.send(Request::Shutdown).await;
        self.finish().await
    }

    async fn finish(&mut self) -> Result<(), IsleError> {
        if let Some(done_rx) = self.done_rx.take() {
            let _ = done_rx.await;
        }
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| IsleError::Panicked("sqlite thread panicked".into()))?;
        }
        Ok(())
    }

    /// Check if the SQLite thread is still alive.
    pub fn is_alive(&self) -> bool {
        self.join.as_ref().is_some_and(|j| !j.is_finished())
    }
}

// No Drop impl for AsyncIsleDriver: dropping the driver does NOT send
// Shutdown.  Other AsyncIsle clones may still be sending jobs; the
// thread exits naturally when every sender is gone.

/// Create an [`AsyncTask`] that resolves to an error immediately.
fn make_error_task<T: Send + 'static>(err: IsleError) -> AsyncTask<T> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = tx.send(Err(err));
    AsyncTask::new(rx, CancelToken::new())
}
