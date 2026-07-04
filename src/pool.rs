//! Connection pool of thread-isolated SQLite isles.
//!
//! [`IslePool`] manages a set of [`Isle`] instances with checkout/return
//! semantics via RAII guards ([`PooledIsle`]).
//!
//! # Intended arrangement (WAL: 1 writer + N readers)
//!
//! The pool itself is topology-agnostic: it grows lazily up to
//! `max_size` using the factory you provide.  For the WAL pattern, keep
//! a single writer [`Isle`] outside the pool and give the pool a factory
//! that opens **read-only** isles:
//!
//! ```rust
//! use rusqlite_isle::{Isle, IslePool, PoolConfig};
//! # let dir = std::env::temp_dir().join(format!("isle-pool-doc-{}", std::process::id()));
//! # std::fs::create_dir_all(&dir).unwrap();
//! # let path = dir.join("doc.db");
//!
//! // writer (owns schema + WAL mode)
//! let writer = Isle::spawn(&path, |conn| {
//!     conn.pragma_update(None, "journal_mode", "WAL")?;
//!     conn.execute_batch("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
//! }).unwrap();
//!
//! // reader pool (read-only open flags)
//! let p = path.clone();
//! let pool = IslePool::new(
//!     move || {
//!         Isle::builder()
//!             .open_flags(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
//!             .spawn(&p, |_conn| Ok(()))
//!     },
//!     PoolConfig { max_size: 4 },
//! ).unwrap();
//!
//! writer.call(|c| c.execute("INSERT INTO t (x) VALUES (1)", [])).unwrap();
//! let reader = pool.checkout().unwrap();
//! let n: i64 = reader.call(|c| c.query_row("SELECT count(*) FROM t", [], |r| r.get(0))).unwrap();
//! assert_eq!(n, 1);
//! drop(reader); // returned to the pool
//!
//! pool.shutdown();
//! writer.shutdown().unwrap();
//! # let _ = std::fs::remove_dir_all(&dir);
//! ```
//!
//! Which calls go to the writer and which to the reader pool is the
//! **consumer's responsibility** — the pool does not route.

use crate::error::IsleError;
use crate::handle::Isle;
use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

/// Factory function type for creating new isles.
type Factory = dyn Fn() -> Result<Isle, IsleError> + Send + Sync;

/// Configuration for [`IslePool`].
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of isles that can exist simultaneously
    /// (checked out + idle).
    pub max_size: usize,
}

/// Shared inner state of the pool.
struct PoolInner {
    /// Idle isles ready for checkout.
    idle: Vec<Isle>,
    /// Number of isles currently checked out.
    active: usize,
    /// Whether the pool has been shut down.
    closed: bool,
}

/// A pool of thread-isolated SQLite isles.
///
/// Thread-safe — share via `Arc` across threads.  Isles are spawned
/// lazily on checkout (up to [`PoolConfig::max_size`]) and returned to
/// the idle list when the [`PooledIsle`] guard drops.
pub struct IslePool {
    inner: Mutex<PoolInner>,
    condvar: Condvar,
    factory: Arc<Factory>,
    config: PoolConfig,
}

impl IslePool {
    /// Create a new pool with the given factory and configuration.
    ///
    /// The factory is called each time the pool grows; it typically opens
    /// an [`Isle`] with read-only [`open_flags`](crate::IsleBuilder::open_flags).
    /// No isles are created eagerly.
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::Closed`] if `max_size` is zero.
    pub fn new<F>(factory: F, config: PoolConfig) -> Result<Self, IsleError>
    where
        F: Fn() -> Result<Isle, IsleError> + Send + Sync + 'static,
    {
        if config.max_size == 0 {
            return Err(IsleError::Closed);
        }
        Ok(Self {
            inner: Mutex::new(PoolInner {
                idle: Vec::with_capacity(config.max_size),
                active: 0,
                closed: false,
            }),
            condvar: Condvar::new(),
            factory: Arc::new(factory),
            config,
        })
    }

    /// Checkout an isle, blocking until one is available.
    ///
    /// 1. An idle isle is returned immediately if present.
    /// 2. Below `max_size`, a new isle is spawned via the factory.
    /// 3. Otherwise this blocks until another thread returns one.
    ///
    /// # Errors
    ///
    /// - [`IsleError::Closed`] if the pool has been shut down.
    /// - Factory errors are propagated.
    /// - [`IsleError::Panicked`] if the internal lock is poisoned.
    pub fn checkout(&self) -> Result<PooledIsle<'_>, IsleError> {
        let mut inner = self.lock_inner()?;
        loop {
            if inner.closed {
                return Err(IsleError::Closed);
            }
            match self.try_acquire(inner)? {
                Acquired::Isle(pooled) => return Ok(pooled),
                Acquired::NeedWait(guard) => {
                    inner = self
                        .condvar
                        .wait(guard)
                        .map_err(|e| IsleError::Panicked(e.to_string()))?;
                }
            }
        }
    }

    /// Try to checkout an isle without blocking.
    ///
    /// Returns `Ok(None)` when no isle is immediately available and the
    /// pool is at capacity.
    ///
    /// # Errors
    ///
    /// Same as [`checkout`](Self::checkout), minus the blocking wait.
    pub fn try_checkout(&self) -> Result<Option<PooledIsle<'_>>, IsleError> {
        let inner = self.lock_inner()?;
        if inner.closed {
            return Err(IsleError::Closed);
        }
        match self.try_acquire(inner)? {
            Acquired::Isle(pooled) => Ok(Some(pooled)),
            Acquired::NeedWait(_) => Ok(None),
        }
    }

    /// Checkout with a timeout.
    ///
    /// Returns [`IsleError::Timeout`] if the timeout expires before an
    /// isle becomes available.
    pub fn checkout_timeout(&self, timeout: Duration) -> Result<PooledIsle<'_>, IsleError> {
        let mut inner = self.lock_inner()?;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if inner.closed {
                return Err(IsleError::Closed);
            }
            match self.try_acquire(inner)? {
                Acquired::Isle(pooled) => return Ok(pooled),
                Acquired::NeedWait(guard) => {
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(IsleError::Timeout);
                    }
                    let (guard, _) = self
                        .condvar
                        .wait_timeout(guard, remaining)
                        .map_err(|e| IsleError::Panicked(e.to_string()))?;
                    inner = guard;
                }
            }
        }
    }

    /// Number of currently checked-out isles.
    pub fn active(&self) -> usize {
        self.inner.lock().map(|g| g.active).unwrap_or(0)
    }

    /// Number of idle isles available for checkout.
    pub fn idle(&self) -> usize {
        self.inner.lock().map(|g| g.idle.len()).unwrap_or(0)
    }

    /// Shut down the pool and all idle isles.
    ///
    /// Checked-out isles are shut down when their [`PooledIsle`] guards
    /// drop.  Subsequent checkouts return [`IsleError::Closed`].
    pub fn shutdown(&self) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        inner.closed = true;
        for isle in inner.idle.drain(..) {
            let _ = isle.shutdown();
        }
        self.condvar.notify_all();
    }

    // ── internal ────────────────────────────────────────────────────

    fn try_acquire<'a>(
        &'a self,
        mut inner: MutexGuard<'a, PoolInner>,
    ) -> Result<Acquired<'a>, IsleError> {
        if let Some(isle) = Self::take_alive_isle(&mut inner) {
            inner.active += 1;
            return Ok(Acquired::Isle(PooledIsle::new(self, isle)));
        }

        if inner.active + inner.idle.len() < self.config.max_size {
            inner.active += 1;
            drop(inner);
            match (self.factory)() {
                Ok(isle) => Ok(Acquired::Isle(PooledIsle::new(self, isle))),
                Err(e) => {
                    self.dec_active();
                    Err(e)
                }
            }
        } else {
            Ok(Acquired::NeedWait(inner))
        }
    }

    /// Return an isle to the pool (called by `PooledIsle::drop`).
    fn return_isle(&self, isle: Isle) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        inner.active = inner.active.saturating_sub(1);

        if inner.closed {
            let _ = isle.shutdown();
        } else if isle.is_alive() {
            inner.idle.push(isle);
        }
        self.condvar.notify_one();
    }

    /// Take an alive isle from the idle list, skipping dead ones.
    fn take_alive_isle(inner: &mut PoolInner) -> Option<Isle> {
        while let Some(isle) = inner.idle.pop() {
            if isle.is_alive() {
                return Some(isle);
            }
        }
        None
    }

    fn lock_inner(&self) -> Result<MutexGuard<'_, PoolInner>, IsleError> {
        self.inner
            .lock()
            .map_err(|e| IsleError::Panicked(e.to_string()))
    }

    fn dec_active(&self) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        inner.active = inner.active.saturating_sub(1);
        self.condvar.notify_one();
    }
}

/// Result of [`IslePool::try_acquire`].
enum Acquired<'pool> {
    Isle(PooledIsle<'pool>),
    NeedWait(MutexGuard<'pool, PoolInner>),
}

/// RAII guard for a checked-out [`Isle`].
///
/// Dereferences to [`Isle`] for direct use of `call`, `call_timeout`,
/// `spawn_call`, etc.  On drop the isle is returned to the pool (or shut
/// down if the pool is closed / the isle's thread died).
pub struct PooledIsle<'pool> {
    pool: &'pool IslePool,
    isle: Option<Isle>,
}

impl std::fmt::Debug for PooledIsle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledIsle")
            .field("alive", &self.isle.as_ref().map(Isle::is_alive))
            .finish()
    }
}

impl<'pool> PooledIsle<'pool> {
    fn new(pool: &'pool IslePool, isle: Isle) -> Self {
        Self {
            pool,
            isle: Some(isle),
        }
    }
}

impl Deref for PooledIsle<'_> {
    type Target = Isle;

    fn deref(&self) -> &Isle {
        self.isle.as_ref().expect("PooledIsle used after drop")
    }
}

impl Drop for PooledIsle<'_> {
    fn drop(&mut self) {
        if let Some(isle) = self.isle.take() {
            self.pool.return_isle(isle);
        }
    }
}
