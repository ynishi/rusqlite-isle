//! Integration tests for the async `AsyncIsle` API (`tokio` feature).

#![cfg(feature = "tokio")]

use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::time::Duration;

/// A query that runs for a very long time (bounded, so a broken
/// interrupt path cannot hang the suite forever) but is reliably
/// interruptible via `sqlite3_interrupt`.
const LONG_QUERY: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
     WHERE x < 1000000000) SELECT count(*) FROM c";

fn long_query(conn: &mut rusqlite::Connection) -> Result<i64, rusqlite::Error> {
    conn.query_row(LONG_QUERY, [], |r| r.get(0))
}

async fn open_test_isle() -> (AsyncIsle, AsyncIsleDriver) {
    AsyncIsle::open_in_memory(|conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER NOT NULL)")
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn async_crud_and_tx() {
    let (isle, driver) = open_test_isle().await;

    let inserted: usize = isle
        .call(|conn| conn.execute("INSERT INTO t (x) VALUES (?1)", [42]))
        .await
        .unwrap();
    assert_eq!(inserted, 1);

    // Clone the handle freely (no Arc).
    let isle2 = isle.clone();
    let x: i64 = isle2
        .call(|conn| conn.query_row("SELECT x FROM t WHERE id = 1", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(x, 42);

    // Transaction opened and committed inside a single closure.
    let n: i64 = isle
        .call(|conn| {
            let tx = conn.transaction()?;
            tx.execute("INSERT INTO t (x) VALUES (2)", [])?;
            let n: i64 = tx.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?;
            tx.commit()?;
            Ok(n)
        })
        .await
        .unwrap();
    assert_eq!(n, 2);

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_cancel_running_query() {
    let (isle, driver) = open_test_isle().await;

    let task = isle.spawn_call(long_query);
    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        token.cancel();
    });

    let result = task.await;
    assert!(
        matches!(result, Err(IsleError::Cancelled)),
        "expected Cancelled, got {result:?}"
    );

    // The isle keeps serving jobs afterwards.
    let one: i64 = isle
        .call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(one, 1);

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_call_timeout() {
    let (isle, driver) = open_test_isle().await;

    let result: Result<i64, IsleError> = isle
        .call_timeout(Duration::from_millis(100), long_query)
        .await;
    assert!(
        matches!(result, Err(IsleError::Timeout)),
        "expected Timeout, got {result:?}"
    );

    // Thread is healthy afterwards.
    let one: i64 = isle
        .call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(one, 1);

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn drop_cancels_running_job() {
    let (isle, driver) = open_test_isle().await;

    let task = isle.spawn_call(|conn| {
        // Interrupted before the marker insert is reached.
        conn.query_row(LONG_QUERY, [], |r| r.get::<_, i64>(0))?;
        conn.execute("INSERT INTO t (x) VALUES (99)", [])?;
        Ok(())
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(task);

    // The interrupted job must have released the thread: a follow-up
    // call completes well within the deadline and sees no marker row.
    let n: i64 = isle
        .call_timeout(Duration::from_secs(10), |conn| {
            conn.query_row("SELECT count(*) FROM t WHERE x = 99", [], |r| r.get(0))
        })
        .await
        .unwrap();
    assert_eq!(n, 0, "dropped task must not have completed its insert");

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn detach_lets_job_run_to_completion() {
    let (isle, driver) = open_test_isle().await;

    let task = isle.spawn_call(|conn| {
        std::thread::sleep(Duration::from_millis(100));
        conn.execute("INSERT INTO t (x) VALUES (7)", [])?;
        Ok(())
    });
    task.detach();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let n: i64 = isle
        .call(|conn| conn.query_row("SELECT count(*) FROM t WHERE x = 7", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(n, 1, "detached job must have run to completion");

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_queued_jobs() {
    let (isle, driver) = open_test_isle().await;

    let tasks: Vec<_> = (0..5)
        .map(|i| isle.spawn_call(move |conn| conn.execute("INSERT INTO t (x) VALUES (?1)", [i])))
        .collect();

    driver.shutdown().await.unwrap();

    // All queued jobs ran to completion before the thread exited.
    for task in tasks {
        assert_eq!(task.await.unwrap(), 1);
    }

    // Submissions after shutdown report Closed.
    let after: Result<i64, IsleError> = isle
        .call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)))
        .await;
    assert!(
        matches!(after, Err(IsleError::Closed)),
        "expected Closed, got {after:?}"
    );
    let after_try = isle.try_call(|_conn| Ok(()));
    assert!(matches!(after_try, Err(IsleError::Closed)));
}

#[tokio::test]
async fn shutdown_now_aborts_running_and_queued_jobs() {
    let (isle, driver) = open_test_isle().await;

    let running = isle.spawn_call(long_query);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let queued: Vec<_> = (0..3)
        .map(|_| isle.spawn_call(|conn| conn.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))))
        .collect();

    driver.shutdown_now().await.unwrap();

    let r = running.await;
    assert!(
        matches!(r, Err(IsleError::Closed)),
        "running job should abort as Closed, got {r:?}"
    );
    for task in queued {
        let r = task.await;
        assert!(
            matches!(r, Err(IsleError::Closed)),
            "queued job should be discarded as Closed, got {r:?}"
        );
    }
}

#[tokio::test]
async fn try_call_queue_full() {
    let (isle, driver) = AsyncIsle::builder()
        .channel_capacity(1)
        .open_in_memory(|_conn| Ok(()))
        .await
        .unwrap();

    // Occupy the thread with a sleeping job.
    let running = isle.spawn_call(|_conn| {
        std::thread::sleep(Duration::from_millis(500));
        Ok(())
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fill the single queue slot.
    let queued = isle.try_call(|_conn| Ok(1_i64)).unwrap();

    // Next non-waiting submission must report backpressure.
    let overflow = isle.try_call(|_conn| Ok(2_i64));
    assert!(
        matches!(overflow, Err(IsleError::QueueFull)),
        "expected QueueFull"
    );

    // spawn_call surfaces the same condition through the task.
    let overflow_task = isle.spawn_call(|_conn| Ok(3_i64));
    let r = overflow_task.await;
    assert!(matches!(r, Err(IsleError::QueueFull)));

    running.await.unwrap();
    assert_eq!(queued.await.unwrap(), 1);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_panic_is_reported_and_isle_survives() {
    let (isle, driver) = open_test_isle().await;

    let result: Result<(), IsleError> = isle.call(|_conn| panic!("boom async")).await;
    match result {
        Err(IsleError::Panicked(msg)) => assert!(msg.contains("boom async"), "msg: {msg}"),
        other => panic!("expected Panicked, got {other:?}"),
    }

    let one: i64 = isle
        .call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(one, 1);

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn late_cancel_does_not_interrupt_next_job() {
    let (isle, driver) = open_test_isle().await;

    for i in 0..30 {
        let task =
            isle.spawn_call(move |conn| conn.execute("INSERT INTO t (x) VALUES (?1)", [i]));
        let token = task.cancel_token().clone();
        assert_eq!(task.await.unwrap(), 1);
        // Fire the token AFTER completion: must not affect the next job.
        token.cancel();
        let n: i64 = isle
            .call(|conn| conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)))
            .await
            .unwrap();
        assert_eq!(n, i64::from(i) + 1);
    }

    driver.shutdown().await.unwrap();
}
