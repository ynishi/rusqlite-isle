//! Integration tests for `IslePool` (`pool` feature).

#![cfg(feature = "pool")]

use rusqlite_isle::{Isle, IsleError, IslePool, PoolConfig};
use std::time::Duration;

fn test_pool(max_size: usize) -> IslePool {
    IslePool::new(
        || Isle::open_in_memory(|conn| conn.execute_batch("CREATE TABLE t (x INTEGER)")),
        PoolConfig { max_size },
    )
    .unwrap()
}

#[test]
fn checkout_use_and_return() {
    let pool = test_pool(2);
    assert_eq!(pool.active(), 0);
    assert_eq!(pool.idle(), 0);

    {
        let isle = pool.checkout().unwrap();
        assert_eq!(pool.active(), 1);
        let n: i64 = isle
            .call(|conn| {
                conn.execute("INSERT INTO t (x) VALUES (1)", [])?;
                conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            })
            .unwrap();
        assert_eq!(n, 1);
    } // returned to the pool here

    assert_eq!(pool.active(), 0);
    assert_eq!(pool.idle(), 1);

    // Reuse: the same isle (same in-memory DB) comes back.
    let isle = pool.checkout().unwrap();
    let n: i64 = isle
        .call(|conn| conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)))
        .unwrap();
    assert_eq!(n, 1, "warm reuse keeps the same connection state");
    drop(isle);

    pool.shutdown();
}

#[test]
fn try_checkout_none_at_capacity() {
    let pool = test_pool(2);

    let a = pool.checkout().unwrap();
    let b = pool.checkout().unwrap();
    assert_eq!(pool.active(), 2);

    let c = pool.try_checkout().unwrap();
    assert!(c.is_none(), "capacity exceeded must yield None");

    drop(a);
    let c = pool.try_checkout().unwrap();
    assert!(c.is_some(), "freed slot must be checkoutable again");

    drop(b);
    drop(c);
    pool.shutdown();
}

#[test]
fn checkout_timeout_expires() {
    let pool = test_pool(1);

    let held = pool.checkout().unwrap();
    let result = pool.checkout_timeout(Duration::from_millis(100));
    assert!(
        matches!(result, Err(IsleError::Timeout)),
        "expected Timeout, got {:?}",
        result.map(|_| ())
    );

    drop(held);
    let ok = pool.checkout_timeout(Duration::from_millis(100));
    assert!(ok.is_ok());
    drop(ok);
    pool.shutdown();
}

#[test]
fn checkout_unblocks_when_returned() {
    let pool = std::sync::Arc::new(test_pool(1));

    let held = pool.checkout().unwrap();
    let pool2 = std::sync::Arc::clone(&pool);
    let waiter = std::thread::spawn(move || {
        let isle = pool2.checkout().unwrap();
        let one: i64 = isle
            .call(|conn| conn.query_row("SELECT 1", [], |r| r.get(0)))
            .unwrap();
        one
    });

    std::thread::sleep(Duration::from_millis(100));
    drop(held); // wakes the blocked checkout

    assert_eq!(waiter.join().unwrap(), 1);
    pool.shutdown();
}

#[test]
fn shutdown_closes_pool() {
    let pool = test_pool(2);

    {
        let isle = pool.checkout().unwrap();
        let _ = isle
            .call(|conn| conn.query_row("SELECT 1", [], |r| r.get::<_, i64>(0)))
            .unwrap();
    }
    assert_eq!(pool.idle(), 1);

    pool.shutdown();
    assert_eq!(pool.idle(), 0);

    let result = pool.checkout();
    assert!(
        matches!(result, Err(IsleError::Closed)),
        "expected Closed after shutdown"
    );
    let result = pool.try_checkout();
    assert!(matches!(result, Err(IsleError::Closed)));
}
