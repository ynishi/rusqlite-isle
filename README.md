# rusqlite-isle

Thread-isolated SQLite with cancellation, async bridge, and connection pool for [rusqlite](https://docs.rs/rusqlite).

## Problem

Using rusqlite from concurrent code hits three walls:

1. **rusqlite is blocking** — calling it directly inside an async runtime stalls the executor.
2. **`Connection` is not `Sync`** — sharing it requires serialization; a single-writer thread with a job queue is the most direct structure for that.
3. **Cancellation is uncontrolled** — plain rusqlite has no unified way to stop a long-running query from outside.

`rusqlite-isle` confines the `Connection` to a dedicated OS thread (an "isle") and controls execution, errors, and cancellation across the channel boundary.

## Features

- **Typed closure jobs** — `FnOnce(&mut Connection) -> Result<T, rusqlite::Error>`; results come back with their concrete `T` (no type erasure), via the oneshot channel the job captured (tokio-rusqlite style).
- **Transactions stay inside one closure** — there is no API for holding a transaction across `.await`, structurally ruling out cross-await deadlocks; uncommitted transactions roll back on drop.
- **Two-stage cancellation** — a `CancelToken` drops queued jobs before execution and interrupts running statements via `sqlite3_interrupt`; `SQLITE_INTERRUPT` is normalized back to `Cancelled` / `Timeout` from the token state.
- **Drop = cancel (async)** — dropping an `AsyncTask` cancels the job; opt out with `detach()`.
- **Per-call deadlines** — `call_timeout` fires the interrupt path and reports `Timeout`, kept distinct from `SQLITE_BUSY` (which stays `Sqlite`).
- **Panic containment** — job panics are caught (`catch_unwind`), reported as `Panicked`, followed by a `SELECT 1` health check; a failing check closes the isle instead of serving a poisoned connection.
- **Backpressure** — bounded channels; non-waiting submissions report `QueueFull`.
- **Handle / Driver separation (async)** — `AsyncIsle` is a cheap `Clone` handle; `AsyncIsleDriver` owns the lifecycle (`shutdown().await` = drain + join, `shutdown_now()` = abort).
- **Reader pool** (`pool` feature) — checkout/return semantics for a WAL-style 1-writer / N-reader arrangement.

## Architecture

### Sync (`Isle`)

```text
┌─────────────────┐   bounded mpsc   ┌──────────────────────┐
│  caller thread   │─────────────────►│  SQLite thread        │
│                  │                  │  (owns Connection)    │
│  Isle handle     │◄─────────────────│  sequential jobs      │
└─────────────────┘     channel      │  + InterruptHandle    │
                                     │  + catch_unwind       │
                                     └──────────────────────┘
```

### Async (`AsyncIsle`, requires `tokio` feature)

```text
┌──────────────────┐                ┌────────────────────┐
│  tokio tasks      │  tokio mpsc   │  SQLite thread      │
│                   │──────────────►│  (owns Connection)  │
│  AsyncIsle handle │  (bounded,    │                     │
│  (Clone, no Arc)  │  backpressure)│  sequential jobs    │
│                   │◄──────────────│  + InterruptHandle  │
│                   │   oneshot     │  + catch_unwind     │
├──────────────────┤                │                     │
│  AsyncIsleDriver  │───shutdown───►│                     │
│  (lifecycle owner)│               └────────────────────┘
└──────────────────┘
```

## Usage

```toml
[dependencies]
rusqlite-isle = "0.1"

# For async support:
# rusqlite-isle = { version = "0.1", features = ["tokio"] }

# For the reader pool:
# rusqlite-isle = { version = "0.1", features = ["pool"] }

# Both:
# rusqlite-isle = { version = "0.1", features = ["tokio", "pool"] }
```

### Sync API

```rust
use rusqlite_isle::Isle;

let isle = Isle::open_in_memory(|conn| {
    conn.execute_batch("CREATE TABLE t (x INTEGER)")
}).unwrap();

// Transaction opened and committed inside one closure.
let n: i64 = isle.call(|conn| {
    let tx = conn.transaction()?;
    tx.execute("INSERT INTO t (x) VALUES (1)", [])?;
    let n = tx.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?;
    tx.commit()?;
    Ok(n)
}).unwrap();
assert_eq!(n, 1);

isle.shutdown().unwrap();
```

### Async API

```rust
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use rusqlite_isle::AsyncIsle;

let (isle, driver) = AsyncIsle::spawn("app.db", |conn| {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.execute_batch("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
}).await?;

// Clone freely — no Arc needed.
let isle2 = isle.clone();

let n: i64 = isle2.call(|conn| {
    conn.execute("INSERT INTO t (x) VALUES (1)", [])?;
    conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
}).await?;
assert!(n >= 1);

driver.shutdown().await?; // drain queued jobs + join the thread
# std::fs::remove_file("app.db").ok();
# Ok(())
# }
```

### Cancellation

```rust
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use rusqlite_isle::{AsyncIsle, IsleError};
use std::time::Duration;

let (isle, driver) = AsyncIsle::open_in_memory(|_conn| Ok(())).await?;

let task = isle.spawn_call(|conn| {
    conn.query_row(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE x < 1000000000) SELECT count(*) FROM c",
        [],
        |r| r.get::<_, i64>(0),
    )
});
let token = task.cancel_token().clone();
tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel(); // or: drop(task) — drop cancels by default
});
assert!(matches!(task.await, Err(IsleError::Cancelled)));

// Per-call deadline (distinct from SQLITE_BUSY):
let r: Result<i64, _> = isle.call_timeout(Duration::from_millis(100), |conn| {
    conn.query_row(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE x < 1000000000) SELECT count(*) FROM c",
        [],
        |r| r.get(0),
    )
}).await;
assert!(matches!(r, Err(IsleError::Timeout)));

driver.shutdown().await?;
# Ok(())
# }
```

### Reader pool (`pool` feature)

```rust,ignore
use rusqlite_isle::{Isle, IslePool, PoolConfig};

// WAL: one writer isle outside the pool ...
let writer = Isle::spawn("app.db", |conn| {
    conn.pragma_update(None, "journal_mode", "WAL")
})?;

// ... plus a pool of read-only reader isles.
let pool = IslePool::new(
    || Isle::builder()
        .open_flags(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .spawn("app.db", |_conn| Ok(())),
    PoolConfig { max_size: 4 },
)?;

let reader = pool.checkout()?;
let n: i64 = reader.call(|c| c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)))?;
drop(reader); // returned to the pool
```

Routing writes to the writer and reads to the pool is the consumer's responsibility — the pool does not route.

## API

### Sync (`Isle`)

| Method | Description |
|---|---|
| `Isle::spawn(path, init)` | Open a database file on a dedicated thread |
| `Isle::open_in_memory(init)` | Open an in-memory database |
| `Isle::builder()` | Channel capacity / thread name / busy_timeout / open flags |
| `isle.call(f)` | Run a closure (blocking, waits for capacity) |
| `isle.call_timeout(dur, f)` | Per-call deadline → `Timeout` |
| `isle.try_call(f)` | Non-waiting submit → `Task<T>` (or `QueueFull`) |
| `isle.spawn_call(f)` | Cancellable `Task<T>` |
| `task.wait()` / `task.cancel()` / `task.cancel_token()` | Collect / cancel / share the token |
| `isle.shutdown()` | Drain queued jobs, stop, join |

### Async (`AsyncIsle`, `tokio` feature)

| Method | Description |
|---|---|
| `AsyncIsle::spawn(path, init)` | Returns `(AsyncIsle, AsyncIsleDriver)` |
| `AsyncIsle::open_in_memory(init)` | In-memory variant |
| `AsyncIsle::builder()` | Channel capacity / thread name / busy_timeout / open flags |
| `isle.call(f)` | Run a closure (awaits queue capacity) |
| `isle.call_timeout(dur, f)` | Per-call deadline → `Timeout` |
| `isle.spawn_call(f)` | Cancellable `AsyncTask<T>` (a `Future`) |
| `isle.try_call(f)` | Non-waiting submit (or `QueueFull`) |
| `task.cancel()` / `task.cancel_token()` | Explicit cancel / share the token |
| `task.detach()` | Opt out of drop-cancel (fire-and-forget) |
| `driver.shutdown()` | Drain queued jobs + join |
| `driver.shutdown_now()` | Interrupt running job, discard queued jobs, join |

### Pool (`IslePool`, `pool` feature)

| Method | Description |
|---|---|
| `IslePool::new(factory, config)` | Lazy pool of isles (factory typically opens read-only) |
| `pool.checkout()` | Blocking checkout (RAII guard returns on drop) |
| `pool.try_checkout()` | `Ok(None)` when at capacity |
| `pool.checkout_timeout(dur)` | `Timeout` when the wait expires |
| `pool.active()` / `pool.idle()` | Counters |
| `pool.shutdown()` | Close the pool, shut down idle isles |

### Errors (`IsleError`)

| Variant | Meaning |
|---|---|
| `Sqlite(rusqlite::Error)` | SQL error, including `SQLITE_BUSY` |
| `Cancelled` | Token fired / task dropped |
| `Timeout` | Per-call deadline elapsed |
| `Closed` | Isle shut down / thread gone |
| `QueueFull` | Bounded channel full (transient) |
| `Panicked(String)` | Job closure panicked (caught) |

## Minimum Supported Rust Version

Rust 1.77.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
