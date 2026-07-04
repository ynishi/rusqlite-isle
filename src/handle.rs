//! `Isle` — the synchronous public handle for the SQLite thread.

use crate::error::IsleError;
use crate::task::Task;
use crate::thread::{self, Request};
use crate::token::CancelToken;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Default capacity for the bounded request channel.
pub(crate) const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// What to open on the SQLite thread.
enum OpenTarget {
    Path(PathBuf),
    InMemory,
}

/// Handle to a thread-isolated SQLite connection (sync API).
///
/// `Isle` owns the request channel and the join handle for the SQLite
/// thread.  All operations are thread-safe (`Isle: Send + Sync`).
///
/// # Lifecycle
///
/// 1. [`Isle::spawn`] / [`Isle::open_in_memory`] open the connection on a
///    dedicated thread (the `init` closure runs there first — pragmas,
///    migrations, etc.).
/// 2. Use [`call`](Isle::call) / [`call_timeout`](Isle::call_timeout) /
///    [`try_call`](Isle::try_call) / [`spawn_call`](Isle::spawn_call) to
///    run closures against the connection.
/// 3. [`shutdown`](Isle::shutdown) drains queued jobs, stops the thread,
///    and joins it.  Afterwards every submission returns
///    [`IsleError::Closed`].
///
/// If the `Isle` is dropped without `shutdown`, a best-effort shutdown
/// signal is sent and the thread exits once the channel disconnects.
#[must_use = "use .shutdown() for a clean thread join"]
pub struct Isle {
    tx: mpsc::SyncSender<Request>,
    join: Mutex<Option<JoinHandle<()>>>,
    abort: Arc<AtomicBool>,
}

/// Builder for [`Isle`] with configurable parameters.
///
/// Create via [`Isle::builder`].
///
/// # Example
///
/// ```rust
/// use rusqlite_isle::Isle;
/// use std::time::Duration;
///
/// let isle = Isle::builder()
///     .channel_capacity(64)
///     .thread_name("sqlite-worker")
///     .busy_timeout(Duration::from_millis(100))
///     .open_in_memory(|_conn| Ok(()))
///     .unwrap();
/// isle.shutdown().unwrap();
/// ```
pub struct IsleBuilder {
    channel_capacity: usize,
    thread_name: String,
    busy_timeout: Option<Duration>,
    open_flags: Option<rusqlite::OpenFlags>,
}

impl Default for IsleBuilder {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            thread_name: "rusqlite-isle".into(),
            busy_timeout: None,
            open_flags: None,
        }
    }
}

impl IsleBuilder {
    /// Set the bounded channel capacity (backpressure limit).
    ///
    /// When the channel is full, [`Isle::try_call`] returns
    /// [`IsleError::QueueFull`]; blocking submissions wait for capacity.
    ///
    /// Default: 256.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the OS thread name (visible in debuggers and `top`).
    ///
    /// Default: `"rusqlite-isle"`.
    pub fn thread_name(mut self, name: &str) -> Self {
        self.thread_name = name.to_string();
        self
    }

    /// Set `sqlite3_busy_timeout` on the connection after opening.
    ///
    /// Lock contention that exceeds this budget surfaces as
    /// [`IsleError::Sqlite`] (`SQLITE_BUSY`) — distinct from the isle's
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

    /// Open a database file on a dedicated thread with these settings.
    ///
    /// See [`Isle::spawn`] for details.
    pub fn spawn<F>(self, path: impl AsRef<Path>, init: F) -> Result<Isle, IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        Isle::spawn_inner(OpenTarget::Path(path.as_ref().to_path_buf()), init, self)
    }

    /// Open an in-memory database on a dedicated thread with these settings.
    ///
    /// See [`Isle::open_in_memory`] for details.
    pub fn open_in_memory<F>(self, init: F) -> Result<Isle, IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        Isle::spawn_inner(OpenTarget::InMemory, init, self)
    }
}

impl Isle {
    /// Create a builder for configuring the isle.
    pub fn builder() -> IsleBuilder {
        IsleBuilder::default()
    }

    /// Open a database file on a dedicated thread (default settings).
    ///
    /// The `init` closure runs on the SQLite thread before any jobs are
    /// processed.  Use it for pragmas (`journal_mode=WAL`), migrations,
    /// and prepared setup.
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::Sqlite`] if opening the database or running
    /// `init` fails.
    pub fn spawn<F>(path: impl AsRef<Path>, init: F) -> Result<Self, IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        IsleBuilder::default().spawn(path, init)
    }

    /// Open an in-memory database on a dedicated thread (default settings).
    ///
    /// See [`spawn`](Self::spawn) for `init` semantics.
    pub fn open_in_memory<F>(init: F) -> Result<Self, IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        IsleBuilder::default().open_in_memory(init)
    }

    fn spawn_inner<F>(target: OpenTarget, init: F, builder: IsleBuilder) -> Result<Self, IsleError>
    where
        F: FnOnce(&mut Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel::<Request>(builder.channel_capacity);
        let (init_tx, init_rx) = mpsc::channel::<Result<(), IsleError>>();
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
                        return;
                    }
                };

                let setup = busy_timeout
                    .map_or(Ok(()), |t| conn.busy_timeout(t))
                    .and_then(|()| init(&mut conn));
                if let Err(e) = setup {
                    let _ = init_tx.send(Err(IsleError::Sqlite(e)));
                    return;
                }

                let _ = init_tx.send(Ok(()));
                thread::run_loop(conn, || rx.recv().ok(), &abort_thread);
            })
            .map_err(|_| IsleError::Closed)?;

        match init_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                tx,
                join: Mutex::new(Some(join)),
                abort,
            }),
            Ok(Err(e)) => {
                let _ = join.join();
                Err(e)
            }
            Err(_) => {
                let _ = join.join();
                Err(IsleError::Closed)
            }
        }
    }

    /// Run a closure against the connection (blocking).
    ///
    /// The closure executes on the SQLite thread with exclusive access to
    /// the connection; results are returned with their concrete type `T`.
    /// Open transactions inside the closure and commit before returning —
    /// an uncommitted [`rusqlite::Transaction`] rolls back on drop.
    ///
    /// Blocks for queue capacity when the bounded channel is full
    /// (backpressure).  Use [`try_call`](Self::try_call) for a
    /// non-waiting submission.
    pub fn call<T, F>(&self, f: F) -> Result<T, IsleError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let (task, _token) = self.submit_blocking(f)?;
        task.wait()
    }

    /// Run a closure with a per-call deadline.
    ///
    /// If the deadline elapses before the job completes, the job is
    /// cancelled through the interrupt path and the call returns
    /// [`IsleError::Timeout`].  `SQLITE_BUSY` from lock contention is
    /// **not** folded into this — it stays [`IsleError::Sqlite`].
    pub fn call_timeout<T, F>(&self, timeout: Duration, f: F) -> Result<T, IsleError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let (task, token) = self.submit_blocking(f)?;
        match task.rx_ref().recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                token.expire();
                // The job observes the interrupt (or the queued-stage
                // check) and still delivers a normalized result.
                task.wait()
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(IsleError::Closed),
        }
    }

    /// Submit a closure without waiting for queue capacity.
    ///
    /// Returns [`IsleError::QueueFull`] immediately when the bounded
    /// channel is full, [`IsleError::Closed`] when the isle has shut
    /// down.  On success returns a [`Task`] to wait on.
    pub fn try_call<T, F>(&self, f: F) -> Result<Task<T>, IsleError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let (job, task, _token) = self.make_job_task(f);
        match self.tx.try_send(Request::Job(job)) {
            Ok(()) => Ok(task),
            Err(mpsc::TrySendError::Full(_)) => Err(IsleError::QueueFull),
            Err(mpsc::TrySendError::Disconnected(_)) => Err(IsleError::Closed),
        }
    }

    /// Submit a closure and return a cancellable [`Task`].
    ///
    /// Blocks for queue capacity when the channel is full (same
    /// backpressure as [`call`](Self::call)).
    pub fn spawn_call<T, F>(&self, f: F) -> Result<Task<T>, IsleError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let (task, _token) = self.submit_blocking(f)?;
        Ok(task)
    }

    /// Graceful shutdown: drain queued jobs, stop the thread, join it.
    ///
    /// Jobs already queued when `shutdown` is called run to completion.
    /// After shutdown every submission returns [`IsleError::Closed`].
    /// Idempotent — a second call is a no-op.
    pub fn shutdown(&self) -> Result<(), IsleError> {
        let _ = self.tx.send(Request::Shutdown);
        let handle = self
            .join
            .lock()
            .map_err(|e| IsleError::Panicked(e.to_string()))?
            .take();
        if let Some(join) = handle {
            join.join()
                .map_err(|_| IsleError::Panicked("sqlite thread panicked".into()))?;
        }
        Ok(())
    }

    /// Check if the SQLite thread is still alive.
    pub fn is_alive(&self) -> bool {
        self.join
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|j| !j.is_finished()))
            .unwrap_or(false)
    }

    // ── internal ────────────────────────────────────────────────────

    fn make_job_task<T, F>(&self, f: F) -> (thread::Job, Task<T>, CancelToken)
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let token = CancelToken::new();
        let (resp_tx, resp_rx) = mpsc::channel();
        let job = thread::make_job(f, token.clone(), Arc::clone(&self.abort), move |result| {
            let _ = resp_tx.send(result);
        });
        (job, Task::new(resp_rx, token.clone()), token)
    }

    fn submit_blocking<T, F>(&self, f: F) -> Result<(Task<T>, CancelToken), IsleError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let (job, task, token) = self.make_job_task(f);
        self.tx
            .send(Request::Job(job))
            .map_err(|_| IsleError::Closed)?;
        Ok((task, token))
    }
}

impl Drop for Isle {
    fn drop(&mut self) {
        // Best-effort shutdown signal; don't block or join on drop.
        let _ = self.tx.try_send(Request::Shutdown);
    }
}
