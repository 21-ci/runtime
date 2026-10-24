//! The lock table behind `std:lock` (`rt_host::LockTable`): one named mutex per (service, key).
//! Waiting is async (never blocks a thread), waiters are granted in FIFO order, a holder's lease
//! expires so a killed or hung task cannot hold a key forever, a grant that nobody takes is passed
//! on, and the per-service and per-key caps answer `lock_limit`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rt_host::{LockHold, LockTable, LOCK_KEYS_PER_SERVICE, LOCK_WAITERS_PER_KEY};
use rt_types::ServiceId;

fn svc(n: u128) -> ServiceId {
    ServiceId::from_u128(n)
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn code_of<T>(r: Result<T, rt_host::HostError>) -> &'static str {
    match r {
        Ok(_) => panic!("expected an error"),
        Err(e) => e.code,
    }
}

async fn hold(t: &Arc<LockTable>, s: ServiceId, key: &str, wait: Duration, lease: Duration) -> LockHold {
    match t.acquire(s, key, wait, lease).await {
        Ok(h) => h,
        Err(e) => panic!("acquire {key}: {} {}", e.code, e.message),
    }
}

#[tokio::test(start_paused = true)]
async fn second_acquire_waits_until_the_first_is_released() {
    let t = LockTable::new();
    let s = svc(1);
    let first = hold(&t, s, "k", secs(1), secs(30)).await;
    let t2 = t.clone();
    let second = tokio::spawn(async move { t2.acquire(s, "k", secs(60), secs(30)).await.map(|h| h.id()) });
    tokio::time::sleep(secs(5)).await;
    assert!(!second.is_finished(), "a waiter must not get a key that is held");
    drop(first);
    assert!(second.await.expect("join").is_ok(), "granted after the release");
}

#[tokio::test(start_paused = true)]
async fn waiters_are_granted_in_arrival_order() {
    let t = LockTable::new();
    let s = svc(1);
    let order = Arc::new(Mutex::new(Vec::new()));
    let first = hold(&t, s, "k", secs(1), secs(300)).await;
    let mut tasks = Vec::new();
    for i in 0..3u32 {
        let (t2, order2) = (t.clone(), order.clone());
        tasks.push(tokio::spawn(async move {
            let h = t2.acquire(s, "k", secs(300), secs(300)).await.expect("granted");
            order2.lock().unwrap().push(i);
            drop(h);
        }));
        // Arrival order is the spawn order: let each waiter enqueue before the next one starts.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(first);
    for t in tasks {
        t.await.expect("join");
    }
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
}

#[tokio::test(start_paused = true)]
async fn wait_timeout_is_lock_timeout_and_leaves_no_waiter_behind() {
    let t = LockTable::new();
    let s = svc(1);
    let holder = hold(&t, s, "k", secs(1), secs(300)).await;
    let started = tokio::time::Instant::now();
    let r = t.acquire(s, "k", secs(2), secs(30)).await;
    assert_eq!(code_of(r), "lock_timeout");
    assert!(started.elapsed() >= secs(2), "waited the full wait before giving up");
    drop(holder);
    // The timed-out waiter must not be granted the key after it gave up.
    let again = t.acquire(s, "k", Duration::ZERO, secs(30)).await;
    assert!(again.is_ok(), "the key is free once the holder released it");
}

#[tokio::test(start_paused = true)]
async fn zero_wait_fails_fast_when_held() {
    let t = LockTable::new();
    let s = svc(1);
    let holder = hold(&t, s, "k", Duration::ZERO, secs(30)).await;
    assert_eq!(
        code_of(t.acquire(s, "k", Duration::ZERO, secs(30)).await),
        "lock_timeout"
    );
    drop(holder);
    assert!(t.acquire(s, "k", Duration::ZERO, secs(30)).await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn lease_expiry_frees_a_holder_that_never_releases() {
    let t = LockTable::new();
    let s = svc(1);
    // A crashed or hung holder: its guard is leaked, so only the lease can free the key.
    #[allow(clippy::mem_forget, reason = "the leaked guard is what this test is about")]
    std::mem::forget(hold(&t, s, "k", secs(1), secs(2)).await);
    let started = tokio::time::Instant::now();
    let h = hold(&t, s, "k", secs(10), secs(2)).await;
    let waited = started.elapsed();
    assert!(waited >= secs(2), "the lease was honored: {waited:?}");
    assert!(
        waited < secs(3),
        "granted at lease expiry, not at the wait limit: {waited:?}"
    );
    drop(h);
}

#[tokio::test(start_paused = true)]
async fn a_killed_holder_releases_its_key() {
    let t = LockTable::new();
    let s = svc(1);
    let t2 = t.clone();
    let task = tokio::spawn(async move {
        let _held = t2.acquire(s, "k", secs(1), secs(300)).await.expect("held");
        std::future::pending::<()>().await;
    });
    tokio::time::sleep(secs(1)).await;
    task.abort();
    let _ = task.await;
    assert!(
        t.acquire(s, "k", Duration::ZERO, secs(30)).await.is_ok(),
        "the aborted task's hold is gone"
    );
}

#[tokio::test(start_paused = true)]
async fn a_grant_nobody_takes_is_passed_on() {
    let t = LockTable::new();
    let s = svc(1);
    let holder = hold(&t, s, "k", secs(1), secs(300)).await;
    let t2 = t.clone();
    let waiter = tokio::spawn(async move {
        let _h = t2.acquire(s, "k", secs(60), secs(300)).await.expect("granted");
    });
    tokio::time::sleep(secs(1)).await;
    // The release grants the waiter, and the waiter is killed before it runs: the grant must not
    // stay with a task that is gone.
    drop(holder);
    waiter.abort();
    let _ = waiter.await;
    assert!(t.acquire(s, "k", Duration::ZERO, secs(30)).await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn release_is_idempotent_and_a_stale_hold_cannot_free_a_newer_one() {
    let t = LockTable::new();
    let s = svc(1);
    // The first holder's lease runs out; the second takes the key. The first's late drop must not
    // release the second.
    let stale = hold(&t, s, "k", secs(1), secs(1)).await;
    let fresh = hold(&t, s, "k", secs(5), secs(300)).await;
    drop(stale);
    assert_eq!(
        code_of(t.acquire(s, "k", Duration::ZERO, secs(30)).await),
        "lock_timeout"
    );
    drop(fresh);
}

#[tokio::test(start_paused = true)]
async fn keys_are_scoped_to_their_service() {
    let t = LockTable::new();
    let _a = hold(&t, svc(1), "k", Duration::ZERO, secs(30)).await;
    let _b = hold(&t, svc(2), "k", Duration::ZERO, secs(30)).await;
}

#[tokio::test(start_paused = true)]
async fn waiters_per_key_are_capped() {
    let t = LockTable::new();
    let s = svc(1);
    let _holder = hold(&t, s, "k", Duration::ZERO, secs(300)).await;
    for _ in 0..LOCK_WAITERS_PER_KEY {
        let t2 = t.clone();
        tokio::spawn(async move {
            let _ = t2.acquire(s, "k", secs(300), secs(300)).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(code_of(t.acquire(s, "k", secs(1), secs(30)).await), "lock_limit");
}

#[tokio::test(start_paused = true)]
async fn keys_per_service_are_capped() {
    let t = LockTable::new();
    let s = svc(1);
    let mut held = Vec::new();
    for i in 0..LOCK_KEYS_PER_SERVICE {
        held.push(hold(&t, s, &format!("k{i}"), Duration::ZERO, secs(300)).await);
    }
    assert_eq!(
        code_of(t.acquire(s, "one-more", Duration::ZERO, secs(30)).await),
        "lock_limit"
    );
    // Another service has its own budget.
    assert!(t.acquire(svc(2), "one-more", Duration::ZERO, secs(30)).await.is_ok());
    // A key already in use is not a new key.
    drop(held.pop());
    assert_eq!(
        code_of(t.acquire(s, "k0", Duration::ZERO, secs(30)).await),
        "lock_timeout"
    );
}

#[tokio::test(start_paused = true)]
async fn arguments_are_validated() {
    let t = LockTable::new();
    let s = svc(1);
    assert_eq!(code_of(t.acquire(s, "", secs(1), secs(1)).await), "validation_failed");
    assert_eq!(
        code_of(t.acquire(s, &"x".repeat(513), secs(1), secs(1)).await),
        "validation_failed"
    );
    assert_eq!(
        code_of(t.acquire(s, "k", secs(1), Duration::ZERO).await),
        "validation_failed",
        "a lease is positive"
    );
    assert_eq!(
        code_of(t.acquire(s, "k", secs(3600), secs(1)).await),
        "validation_failed",
        "wait is capped"
    );
    assert_eq!(
        code_of(t.acquire(s, "k", secs(1), secs(3600)).await),
        "validation_failed",
        "lease is capped"
    );
}

/// LOCK-004: keys are compared byte for byte: case and Unicode variants are distinct locks.
#[tokio::test(start_paused = true)]
async fn lock_004_case_and_unicode_variants_are_distinct() {
    let t = LockTable::new();
    let s = svc(1);
    let _a = hold(&t, s, "key", secs(0), secs(30)).await;
    let _b = hold(&t, s, "Key", secs(0), secs(30)).await;
    let _c = hold(&t, s, "ключ", secs(0), secs(30)).await;
    let _d = hold(&t, s, "КЛЮЧ", secs(0), secs(30)).await;
    // Composed vs decomposed é.
    let _e = hold(&t, s, "caf\u{e9}", secs(0), secs(30)).await;
    let _f = hold(&t, s, "cafe\u{301}", secs(0), secs(30)).await;
    assert_eq!(
        code_of(t.acquire(s, "key", secs(0), secs(30)).await),
        "lock_timeout",
        "the same key is held"
    );
}

/// LOCK-005: a task acquiring a key it already holds is not re-entrant: it waits like anyone
/// else and gets `lock_timeout` after its wait, never a deadlock.
#[tokio::test(start_paused = true)]
async fn lock_005_reacquiring_a_held_key_times_out() {
    let t = LockTable::new();
    let s = svc(1);
    let _h = hold(&t, s, "k", secs(0), secs(30)).await;
    assert_eq!(
        code_of(t.acquire(s, "k", Duration::from_millis(200), secs(30)).await),
        "lock_timeout"
    );
}

/// LOCK-006: entries go when their last holder or waiter does: many distinct keys over time
/// never hit the per-service key cap.
#[tokio::test(start_paused = true)]
async fn lock_006_released_keys_leave_the_table() {
    let t = LockTable::new();
    let s = svc(1);
    for i in 0..(LOCK_KEYS_PER_SERVICE * 4) {
        let h = hold(&t, s, &format!("k{i}"), secs(0), secs(30)).await;
        drop(h);
    }
    // A waiter that gave up leaves nothing behind either.
    let h = hold(&t, s, "busy", secs(0), secs(30)).await;
    for _ in 0..(LOCK_KEYS_PER_SERVICE * 2) {
        let _ = t.acquire(s, "busy", Duration::from_millis(1), secs(30)).await;
    }
    drop(h);
    let mut held = Vec::new();
    for i in 0..LOCK_KEYS_PER_SERVICE {
        held.push(hold(&t, s, &format!("fresh{i}"), secs(0), secs(30)).await);
    }
}
