//! Thread-isolated SQLite with cancellation, async bridge, and connection
//! pool for [rusqlite].
//!
//! # Problem
//!
//! `rusqlite-isle` confines a [`rusqlite::Connection`] to a dedicated OS
//! thread and communicates via channels.  This solves three fundamental
//! problems with using rusqlite from concurrent code:
//!
//! 1. **rusqlite is blocking** — calling it directly inside an async
//!    runtime stalls the executor.  Routing every call through a channel
//!    to a dedicated thread keeps async tasks non-blocking.
//!
//! 2. **`Connection` is not `Sync`** — sharing it requires serialization.
//!    A single-writer thread with a job queue is the most direct way to
//!    structure that serialization (actor pattern).
//!
//! 3. **Cancellation is uncontrolled in plain rusqlite** — there is no
//!    unified mechanism to stop a long-running query from outside.
//!    `rusqlite-isle` wires [`CancelToken`] to
//!    [`InterruptHandle`](rusqlite::InterruptHandle) so queued jobs are
//!    dropped before execution and running statements are interrupted
//!    via `sqlite3_interrupt`.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────┐                ┌────────────────────┐
//! │  tokio tasks      │  tokio mpsc   │  SQLite thread      │
//! │                   │──────────────►│  (owns Connection)  │
//! │  AsyncIsle handle │  (bounded,    │                     │
//! │  (Clone, no Arc)  │  backpressure)│  sequential jobs    │
//! │                   │◄──────────────│  + InterruptHandle  │
//! │                   │   oneshot     │  + catch_unwind     │
//! ├──────────────────┤                │                     │
//! │  AsyncIsleDriver  │───shutdown───►│                     │
//! │  (lifecycle owner)│               └────────────────────┘
//! └──────────────────┘
//! ```
//!
//! - Every job is a typed closure `FnOnce(&mut Connection) -> Result<T,
//!   rusqlite::Error>`; the result is sent back through a oneshot channel
//!   captured by the job itself, so `T` stays generic (no type erasure).
//! - Transactions are opened and committed **inside** a single closure;
//!   there is no API for holding a transaction across `.await` points,
//!   which structurally rules out cross-await deadlocks.
//! - Panics inside a job are caught with `catch_unwind`, reported as
//!   [`IsleError::Panicked`], and followed by a `SELECT 1` health check;
//!   if the check fails the isle transitions to closed.
//!
//! # Example
//!
//! ```rust
//! use rusqlite_isle::Isle;
//!
//! let isle = Isle::open_in_memory(|conn| {
//!     conn.execute_batch("CREATE TABLE t (x INTEGER)")
//! }).unwrap();
//!
//! let n: i64 = isle.call(|conn| {
//!     conn.execute("INSERT INTO t (x) VALUES (1)", [])?;
//!     conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
//! }).unwrap();
//! assert_eq!(n, 1);
//!
//! isle.shutdown().unwrap();
//! ```
//!
//! With the `tokio` feature, [`AsyncIsle`] provides the same API for async
//! callers, plus [`AsyncTask`] futures whose **drop cancels** the underlying
//! job (opt out with [`AsyncTask::detach`]).  With the `pool` feature,
//! [`IslePool`] provides checkout/return semantics for a WAL-style
//! one-writer / N-reader arrangement.
//!
//! [rusqlite]: https://docs.rs/rusqlite

#![warn(missing_docs)]

mod error;
mod handle;
#[cfg(feature = "pool")]
mod pool;
mod task;
mod thread;
mod token;

#[cfg(feature = "tokio")]
mod async_isle;
#[cfg(feature = "tokio")]
mod async_task;

pub use error::IsleError;
pub use handle::{Isle, IsleBuilder};
pub use task::Task;
pub use token::CancelToken;

#[cfg(feature = "pool")]
pub use pool::{IslePool, PoolConfig, PooledIsle};

#[cfg(feature = "tokio")]
pub use async_isle::{AsyncIsle, AsyncIsleBuilder, AsyncIsleDriver};
#[cfg(feature = "tokio")]
pub use async_task::AsyncTask;
