//! rt-store `queue.db` delayed schedules (`std:schedule`): one pending schedule per
//! (service, id), a put replaces it, cancel removes it, a due schedule is leased to one caller until
//! it is done, a lease that runs out delivers it again (at-least-once), a retry that exhausts
//! `DELAYED_MAX_ATTEMPTS` drops it, a stale generation cannot finish a newer put, the per-service cap,
//! id paging, and persistence across a reopen.

use std::time::Duration;

use bytes::Bytes;
use rt_store::{DelayedStore, NewDelayed, RetryOutcome, SqliteQueueStore, DELAYED_MAX_ATTEMPTS, MAX_DELAYED_PENDING};
use rt_types::ServiceId;

const NOW: i64 = 1_000_000;
const LONG: Duration = Duration::from_secs(60);

fn svc(n: u128) -> ServiceId {
    ServiceId::from_u128(n)
}

fn at(id: &str, run_at_ms: i64) -> NewDelayed {
    NewDelayed {
        id: id.to_string(),
        run_at_ms,
        payload: Bytes::from_static(b"{\"n\":1}"),
    }
}

fn store() -> std::sync::Arc<SqliteQueueStore> {
    SqliteQueueStore::open_in_memory(5).expect("in-memory queue store")
}

fn ids(entries: &[rt_store::DelayedEntry]) -> Vec<&str> {
    entries.iter().map(|e| e.id.as_str()).collect()
}

#[tokio::test]
async fn a_put_replaces_the_pending_id_and_cancel_removes_it() {
    let s = store();
    let g1 = s.put(svc(1), at("a", NOW)).await.unwrap().expect("accepted");
    let g2 = s.put(svc(1), at("a", NOW + 5_000)).await.unwrap().expect("accepted");
    assert_ne!(g1, g2, "a re-put is a new generation");
    let list = s.list(svc(1), None, 10).await.unwrap();
    assert_eq!(ids(&list), vec!["a"]);
    assert_eq!(list[0].run_at_ms, NOW + 5_000, "the new time wins");
    assert!(s.cancel(svc(1), "a").await.unwrap());
    assert!(!s.cancel(svc(1), "a").await.unwrap(), "cancel once");
    assert!(s.list(svc(1), None, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn pending_schedules_are_capped_per_service_and_a_replace_is_not_new() {
    let s = store();
    for i in 0..MAX_DELAYED_PENDING {
        assert!(s.put(svc(1), at(&format!("i{i}"), NOW)).await.unwrap().is_some());
    }
    assert!(
        s.put(svc(1), at("one-more", NOW)).await.unwrap().is_none(),
        "the cap refuses a new id"
    );
    assert!(
        s.put(svc(1), at("i0", NOW + 1)).await.unwrap().is_some(),
        "a pending id can be replaced at the cap"
    );
    assert!(
        s.put(svc(2), at("one-more", NOW)).await.unwrap().is_some(),
        "another service has its own budget"
    );
}

#[tokio::test]
async fn a_due_schedule_is_leased_to_one_caller_until_it_is_done() {
    let s = store();
    s.put(svc(1), at("a", NOW - 1)).await.unwrap();
    s.put(svc(1), at("b", NOW + 60_000)).await.unwrap();
    let got = s.lease(NOW, LONG, 10).await.unwrap();
    assert_eq!(got.len(), 1, "only the due one");
    let (service, item) = &got[0];
    assert_eq!(*service, svc(1));
    assert_eq!((item.id.as_str(), item.attempt), ("a", 1));
    assert_eq!(item.payload, Bytes::from_static(b"{\"n\":1}"));
    assert!(
        s.lease(NOW, LONG, 10).await.unwrap().is_empty(),
        "leased: nobody else gets it"
    );
    assert!(s.done(svc(1), "a", item.gen).await.unwrap());
    assert!(!s.done(svc(1), "a", item.gen).await.unwrap(), "done once");
    assert_eq!(ids(&s.list(svc(1), None, 10).await.unwrap()), vec!["b"]);
}

#[tokio::test]
async fn a_lease_that_runs_out_delivers_the_schedule_again() {
    let s = store();
    s.put(svc(1), at("a", NOW - 1)).await.unwrap();
    let first = s.lease(NOW, LONG, 10).await.unwrap();
    assert_eq!(first[0].1.attempt, 1);
    let again = s.lease(NOW + 61_000, LONG, 10).await.unwrap();
    assert_eq!(again.len(), 1, "the holder never finished: at-least-once");
    assert_eq!((again[0].1.id.as_str(), again[0].1.attempt), ("a", 2));
    assert_eq!(again[0].1.gen, first[0].1.gen, "the same put");
}

#[tokio::test]
async fn a_stale_generation_cannot_finish_a_newer_put() {
    let s = store();
    s.put(svc(1), at("a", NOW - 1)).await.unwrap();
    let old = s.lease(NOW, LONG, 10).await.unwrap().remove(0).1;
    let newer = s.put(svc(1), at("a", NOW - 1)).await.unwrap().expect("accepted");
    assert!(
        !s.done(svc(1), "a", old.gen).await.unwrap(),
        "the old delivery cannot delete the new put"
    );
    let got = s.lease(NOW, LONG, 10).await.unwrap();
    assert_eq!((got.len(), got[0].1.gen), (1, newer));
}

#[tokio::test]
async fn a_retry_requeues_and_the_last_attempt_drops_the_schedule() {
    let s = store();
    s.put(svc(1), at("a", NOW - 1)).await.unwrap();
    for attempt in 1..DELAYED_MAX_ATTEMPTS {
        let item = s.lease(NOW, LONG, 10).await.unwrap().remove(0).1;
        assert_eq!(item.attempt, attempt);
        assert_eq!(
            s.retry(svc(1), "a", item.gen, NOW).await.unwrap(),
            RetryOutcome::Requeued
        );
    }
    let last = s.lease(NOW, LONG, 10).await.unwrap().remove(0).1;
    assert_eq!(last.attempt, DELAYED_MAX_ATTEMPTS);
    assert_eq!(
        s.retry(svc(1), "a", last.gen, NOW).await.unwrap(),
        RetryOutcome::Dropped
    );
    assert!(
        s.list(svc(1), None, 10).await.unwrap().is_empty(),
        "dropped after the last attempt"
    );
    assert_eq!(s.retry(svc(1), "a", last.gen, NOW).await.unwrap(), RetryOutcome::Gone);
}

#[tokio::test]
async fn list_pages_in_id_order() {
    let s = store();
    for id in ["c", "a", "b"] {
        s.put(svc(1), at(id, NOW)).await.unwrap();
    }
    assert_eq!(ids(&s.list(svc(1), None, 2).await.unwrap()), vec!["a", "b"]);
    assert_eq!(ids(&s.list(svc(1), Some("b"), 2).await.unwrap()), vec!["c"]);
}

#[tokio::test]
async fn pending_schedules_survive_a_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("queue.db");
    {
        let s = SqliteQueueStore::open(&path, 5).expect("open");
        s.put(svc(1), at("keep", NOW - 1)).await.unwrap();
    }
    let s = SqliteQueueStore::open(&path, 5).expect("reopen");
    let got = s.lease(NOW, LONG, 10).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1.id, "keep");
}

#[tokio::test]
async fn a_release_gives_the_attempt_back_and_reschedules() {
    let s = store();
    s.put(svc(1), at("a", NOW - 1)).await.unwrap();
    let item = s.lease(NOW, LONG, 10).await.unwrap().remove(0).1;
    assert_eq!(item.attempt, 1);
    assert!(s.release(svc(1), "a", item.gen, NOW + 1_000).await.unwrap(), "released");
    assert!(
        s.lease(NOW, LONG, 10).await.unwrap().is_empty(),
        "due again only at the new time"
    );
    let again = s.lease(NOW + 1_000, LONG, 10).await.unwrap().remove(0).1;
    assert_eq!(
        again.attempt, 1,
        "a release that never started the delivery costs no attempt"
    );
    assert!(
        !s.release(svc(1), "a", item.gen + 1, NOW).await.unwrap(),
        "a generation that is not current is not released"
    );
}

/// DLY-002: at one below the cap, two new ids put at once: exactly one is accepted.
#[tokio::test]
async fn dly_002_two_new_ids_at_the_cap_minus_one_admit_exactly_one() {
    for round in 0..5 {
        let s = store();
        for i in 0..MAX_DELAYED_PENDING - 1 {
            assert!(s.put(svc(1), at(&format!("i{i}"), NOW)).await.unwrap().is_some());
        }
        let (a, b) = tokio::join!(s.put(svc(1), at("x", NOW)), s.put(svc(1), at("y", NOW)));
        let accepted = [a.unwrap(), b.unwrap()].iter().filter(|r| r.is_some()).count();
        assert_eq!(accepted, 1, "round {round}");
        let mut n = 0;
        let mut after = None;
        loop {
            let page = s.list(svc(1), after.as_deref(), 500).await.unwrap();
            if page.is_empty() {
                break;
            }
            n += page.len();
            after = page.last().map(|e| e.id.clone());
        }
        assert_eq!(n, MAX_DELAYED_PENDING, "never over the cap");
    }
}
