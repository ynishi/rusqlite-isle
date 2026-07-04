//! Integration tests for the synchronous `Isle` API.

use rusqlite_isle::{Isle, IsleError};
use std::time::Duration;

/// A query that runs for a very long time (bounded, so a broken
/// interrupt path cannot hang the suite forever) but is reliably
/// interruptible via `sqlite3_interrupt`.
const LONG_QUERY: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
     WHERE x < 1000000000) SELECT count(*) FROM c";

fn long_query(conn: &mut rusqlite::Connection) -> Result<i64, rusqlite::Error> {
    conn.query_row(LONG_QUERY, [], |r| r.get(0))
}

fn open_test_isle() -> Isle {
    Isle::open_in_memory(|conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER NOT NULL)")
    })
    .unwrap()
}

#[test]
fn crud_basic() {
    let isle = open_test_isle();

    let inserted: usize = isle
        .call(|conn| conn.execute("INSERT INTO t (x) VALUES (?1)", [42]))
        .unwrap();
    assert_eq!(inserted, 1);

    let x: i64 = isle
        .call(|conn| conn.query_row("SELECT x FROM t WHERE id = 1", [], |r| r.get(0)))
        .unwrap();
    assert_eq!(x, 42);

    let updated: usize = isle
        .call(|conn| conn.execute("UPDATE t SET x = 7 WHERE id = 1", []))
        .unwrap();
    assert_eq!(updated, 1);

    let deleted: usize = isle
        .call(|conn| conn.execute("DELETE FROM t WHERE id = 1", []))
        .unwrap();
    assert_eq!(deleted, 1);

    let n: i64 = isle
        .call(|conn| conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)))
        .unwrap();
    assert_eq!(n, 0);

    isle.shutdown().unwrap();
}

#[test]
fn transaction_commit_inside_closure() {
    let isle = open_test_isle();

    let n: i64 = isle
        .call(|conn| {
            let tx = conn.transaction()?;
            tx.execute("INSERT INTO t (x) VALUES (1)", [])?;
            tx.execute("INSERT INTO t (x) VALUES (2)", [])?;
            let n: i64 = tx.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?;
            tx.commit()?;
            Ok(n)
        })
        .unwrap();
    assert_eq!(n, 2);

    isle.shutdown().unwrap();
}

#[test]
fn transaction_rolls_back_on_error() {
    let isle = open_test_isle();

    let result: Result<(), IsleError> = isle.call(|conn| {
        let tx = conn.transaction()?;
        tx.execute("INSERT INTO t (x) VALUES (1)", [])?;
        // Force an error before commit; tx rolls back on drop.
        tx.query_row("SELECT * FROM missing_table", [], |_| Ok(()))?;
        tx.commit()?;
        Ok(())
    });
    assert!(matches!(result, Err(IsleError::Sqlite(_))));

    let n: i64 = isle
        .call(|conn| conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)))
        .unwrap();
    assert_eq!(n, 0, "uncommitted insert must have rolled back");

    isle.shutdown().unwrap();
}

#[test]
fn cancel_running_query() {
    let isle = open_test_isle();

    let task = isle.spawn_call(long_query).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    task.cancel();
    let result = task.wait();
    assert!(
        matches!(result, Err(IsleError::Cancelled)),
        "expected Cancelled, got {result:?}"
    );

    // The isle keeps serving jobs afterwards.
    let one: i64 = isle
        .call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)))
        .unwrap();
    assert_eq!(one, 1);

    isle.shutdown().unwrap();
}

#[test]
fn cancel_queued_job() {
    let isle = open_test_isle();

    // Occupy the thread, then cancel a job that is still queued.
    let running = isle.spawn_call(long_query).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let queued = isle
        .spawn_call(|conn| conn.query_row("SELECT 1", [], |r| r.get::<_, i64>(0)))
        .unwrap();
    queued.cancel();
    running.cancel();

    assert!(matches!(queued.wait(), Err(IsleError::Cancelled)));
    assert!(matches!(running.wait(), Err(IsleError::Cancelled)));

    isle.shutdown().unwrap();
}

#[test]
fn call_timeout_returns_timeout() {
    let isle = open_test_isle();

    let result: Result<i64, IsleError> =
        isle.call_timeout(Duration::from_millis(100), long_query);
    assert!(
        matches!(result, Err(IsleError::Timeout)),
        "expected Timeout, got {result:?}"
    );

    // Thread is healthy afterwards.
    let one: i64 = isle
        .call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)))
        .unwrap();
    assert_eq!(one, 1);

    isle.shutdown().unwrap();
}

#[test]
fn busy_is_sqlite_error_not_timeout() {
    let dir = std::env::temp_dir().join(format!("rusqlite-isle-busy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("busy.db");

    let isle = Isle::builder()
        .busy_timeout(Duration::from_millis(50))
        .spawn(&path, |conn| {
            conn.execute_batch("CREATE TABLE t (x INTEGER)")
        })
        .unwrap();

    // Another connection holds an exclusive lock.
    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();

    let result: Result<usize, IsleError> =
        isle.call(|conn| conn.execute("INSERT INTO t (x) VALUES (1)", []));
    assert!(
        matches!(result, Err(IsleError::Sqlite(_))),
        "SQLITE_BUSY must stay in the Sqlite variant, got {result:?}"
    );

    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    isle.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn panic_is_reported_and_isle_survives() {
    let isle = open_test_isle();

    let result: Result<(), IsleError> = isle.call(|_conn| panic!("boom in job"));
    match result {
        Err(IsleError::Panicked(msg)) => assert!(msg.contains("boom in job"), "msg: {msg}"),
        other => panic!("expected Panicked, got {other:?}"),
    }

    // Health check passed — subsequent calls continue normally.
    let one: i64 = isle
        .call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)))
        .unwrap();
    assert_eq!(one, 1);

    isle.shutdown().unwrap();
}

#[test]
fn late_cancel_does_not_interrupt_next_job() {
    let isle = open_test_isle();

    for i in 0..30 {
        let task = isle
            .spawn_call(move |conn| conn.execute("INSERT INTO t (x) VALUES (?1)", [i]))
            .unwrap();
        let token = task.cancel_token().clone();
        let inserted = task.wait().unwrap();
        assert_eq!(inserted, 1);
        // Fire the token AFTER completion: must not affect the next job.
        token.cancel();
        let n: i64 = isle
            .call(|conn| conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)))
            .unwrap();
        assert_eq!(n, i64::from(i) + 1);
    }

    isle.shutdown().unwrap();
}

#[test]
fn try_call_queue_full() {
    let isle = Isle::builder()
        .channel_capacity(1)
        .open_in_memory(|_conn| Ok(()))
        .unwrap();

    // Occupy the thread with a sleeping job.
    let running = isle
        .spawn_call(|_conn| {
            std::thread::sleep(Duration::from_millis(500));
            Ok(())
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // Fill the single queue slot.
    let queued = isle.try_call(|_conn| Ok(1_i64)).unwrap();

    // Next non-waiting submission must report backpressure.
    let overflow = isle.try_call(|_conn| Ok(2_i64));
    assert!(
        matches!(overflow, Err(IsleError::QueueFull)),
        "expected QueueFull"
    );

    running.wait().unwrap();
    assert_eq!(queued.wait().unwrap(), 1);
    isle.shutdown().unwrap();
}

#[test]
fn shutdown_drains_queued_jobs_then_closes() {
    let isle = open_test_isle();

    let tasks: Vec<_> = (0..5)
        .map(|i| {
            isle.spawn_call(move |conn| conn.execute("INSERT INTO t (x) VALUES (?1)", [i]))
                .unwrap()
        })
        .collect();

    isle.shutdown().unwrap();

    // All queued jobs ran to completion before the thread exited.
    for task in tasks {
        assert_eq!(task.wait().unwrap(), 1);
    }

    // Submissions after shutdown report Closed.
    let after: Result<i64, IsleError> =
        isle.call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)));
    assert!(
        matches!(after, Err(IsleError::Closed)),
        "expected Closed, got {after:?}"
    );
    let after_try = isle.try_call(|_conn| Ok(()));
    assert!(matches!(after_try, Err(IsleError::Closed)));
}
