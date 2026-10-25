//! rt-store `queue.db` tests (TRG-09..12): durable at-least-once delivery with
//! leases, acks, retry, DLQ after `max_attempts`, redrive and dedup.
//!
//! There is no clock injection in the store, so visibility/delay tests use short real durations.

use crate::common;

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::queue::{MAX_DELAY, MAX_MESSAGE_BYTES};
use rt_store::{NewMessage, QueueStore, RetryResult, SqliteQueueStore, StoreError};
use rt_types::MsgId;

const LONG: Duration = Duration::from_secs(60);

fn msg(body: &str) -> NewMessage {
    NewMessage::new(Bytes::from(body.to_string()))
}

fn queue(max_attempts: u16) -> std::sync::Arc<SqliteQueueStore> {
    SqliteQueueStore::open_in_memory(max_attempts).expect("in-memory queue store")
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[tokio::test]
async fn ac_queue_01_enqueue_then_lease_delivers_body_with_attempt_one() {
    let q = queue(5);
    let s = svc(1);
    let id = q.enqueue(s, msg("hello")).await.expect("enqueue");

    let leased = q.lease(s, 10, LONG).await.expect("lease");
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].id, id);
    assert_eq!(leased[0].body, Bytes::from_static(b"hello"));
    assert_eq!(leased[0].attempt, 1, "first delivery is attempt 1");
    assert!(
        (leased[0].enqueued_at_ms - now_ms()).abs() < 60_000,
        "enqueued_at_ms must be a current unix-ms timestamp"
    );
}

#[tokio::test]
async fn ac_queue_02_leased_messages_are_invisible_until_the_visibility_timeout() {
    let q = queue(5);
    let s = svc(1);
    let id = q.enqueue(s, msg("once")).await.expect("enqueue");

    let first = q.lease(s, 10, Duration::from_millis(300)).await.expect("lease");
    assert_eq!(first.len(), 1);
    assert!(
        q.lease(s, 10, LONG).await.expect("second lease").is_empty(),
        "a leased message is invisible to other consumers"
    );

    sleep_ms(500).await;
    let again = q.lease(s, 10, LONG).await.expect("lease after timeout");
    assert_eq!(
        again.len(),
        1,
        "an unacked message is redelivered after its visibility timeout (at-least-once, TRG-11)"
    );
    assert_eq!(again[0].id, id);
    assert_eq!(again[0].attempt, 2, "redelivery increments the attempt count");
}

#[tokio::test]
async fn ac_queue_03_ack_removes_message_for_good() {
    let q = queue(5);
    let s = svc(1);
    let ids = [
        q.enqueue(s, msg("a")).await.expect("a"),
        q.enqueue(s, msg("b")).await.expect("b"),
        q.enqueue(s, msg("c")).await.expect("c"),
    ];
    let leased = q.lease(s, 10, Duration::from_millis(200)).await.expect("lease");
    assert_eq!(leased.len(), 3);

    q.ack(&[ids[0], ids[1]]).await.expect("ack two");
    let stats = q.stats(s).await.expect("stats");
    assert_eq!((stats.depth, stats.in_flight, stats.dlq), (0, 1, 0));

    // Acked messages never come back; the unacked one does.
    sleep_ms(400).await;
    let redelivered = q.lease(s, 10, LONG).await.expect("lease");
    assert_eq!(redelivered.iter().map(|m| m.id).collect::<Vec<_>>(), vec![ids[2]]);

    q.ack(&[ids[2]]).await.expect("ack last");
    let stats = q.stats(s).await.expect("stats");
    assert_eq!((stats.depth, stats.in_flight, stats.dlq), (0, 0, 0));

    q.ack(&[MsgId::from_u128(404)])
        .await
        .expect("acking an unknown or already-acked id is a no-op");
}

#[tokio::test]
async fn ac_queue_04_lease_respects_max_and_never_hands_out_a_message_twice() {
    let q = queue(5);
    let s = svc(1);
    let mut all = HashSet::new();
    for i in 0..5 {
        all.insert(q.enqueue(s, msg(&format!("m{i}"))).await.expect("enqueue"));
    }

    let first = q.lease(s, 3, LONG).await.expect("lease 3");
    assert_eq!(first.len(), 3);
    let rest = q.lease(s, 10, LONG).await.expect("lease rest");
    assert_eq!(rest.len(), 2);
    let got: HashSet<MsgId> = first.iter().chain(rest.iter()).map(|m| m.id).collect();
    assert_eq!(got, all, "every message delivered exactly once, none lost");
    assert!(q.lease(s, 10, LONG).await.expect("lease empty").is_empty());
    assert!(q.lease(s, 0, LONG).await.expect("lease zero").is_empty());
}

#[tokio::test]
async fn ac_queue_05_delay_hides_the_message_until_due() {
    let q = queue(5);
    let s = svc(1);
    let id = q
        .enqueue(
            s,
            NewMessage {
                body: Bytes::from_static(b"later"),
                delay: Duration::from_millis(400),
                dedup_id: None,
            },
        )
        .await
        .expect("enqueue");

    assert!(q.lease(s, 10, LONG).await.expect("early lease").is_empty());
    let stats = q.stats(s).await.expect("stats");
    assert_eq!(
        (stats.depth, stats.in_flight),
        (1, 0),
        "delayed messages count as waiting"
    );
    assert!(
        q.services_with_ready().await.expect("ready").is_empty(),
        "a delayed message is not ready"
    );

    sleep_ms(600).await;
    assert_eq!(q.services_with_ready().await.expect("ready"), vec![s]);
    let leased = q.lease(s, 10, LONG).await.expect("lease");
    assert_eq!(leased.len(), 1);
    assert_eq!((leased[0].id, leased[0].attempt), (id, 1));
}

#[tokio::test]
async fn ac_queue_06_retry_requeues_with_delay_and_dead_letters_at_max_attempts() {
    // TRG-10: a handler that always fails ends in the DLQ after max_attempts deliveries.
    let q = queue(3);
    let s = svc(1);
    let id = q.enqueue(s, msg("poison")).await.expect("enqueue");

    for expected_attempt in 1..=2u16 {
        let leased = q.lease(s, 1, LONG).await.expect("lease");
        assert_eq!(leased.len(), 1, "attempt {expected_attempt} must be deliverable");
        assert_eq!(leased[0].attempt, expected_attempt);
        let res = q.retry(id, Duration::ZERO).await.expect("retry");
        assert!(
            matches!(res, RetryResult::Requeued { .. }),
            "attempt {expected_attempt}: {res:?}"
        );
        let stats = q.stats(s).await.expect("stats");
        assert_eq!((stats.depth, stats.in_flight, stats.dlq), (1, 0, 0));
    }

    let third = q.lease(s, 1, LONG).await.expect("lease");
    assert_eq!(third[0].attempt, 3);
    assert_eq!(
        q.retry(id, Duration::ZERO).await.expect("retry"),
        RetryResult::DeadLettered
    );

    let stats = q.stats(s).await.expect("stats");
    assert_eq!((stats.depth, stats.in_flight, stats.dlq), (0, 0, 1));
    assert!(
        q.lease(s, 10, LONG).await.expect("lease after DLQ").is_empty(),
        "dead letters are not delivered"
    );
    assert!(q.services_with_ready().await.expect("ready").is_empty());

    let dlq = q.dlq_list(s, 10).await.expect("dlq list");
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].id, id);
    assert_eq!(dlq[0].body, Bytes::from_static(b"poison"));
    assert_eq!(dlq[0].attempts, 3);
    assert!((dlq[0].failed_at_ms - now_ms()).abs() < 60_000);
}

#[tokio::test]
async fn ac_queue_07_retry_delay_postpones_redelivery() {
    let q = queue(5);
    let s = svc(1);
    let id = q.enqueue(s, msg("backoff")).await.expect("enqueue");
    q.lease(s, 1, LONG).await.expect("lease");

    assert!(matches!(
        q.retry(id, Duration::from_millis(400)).await.expect("retry"),
        RetryResult::Requeued { .. }
    ));
    assert!(
        q.lease(s, 1, LONG).await.expect("immediate lease").is_empty(),
        "retry delay must hide the message"
    );
    sleep_ms(600).await;
    let again = q.lease(s, 1, LONG).await.expect("lease after delay");
    assert_eq!((again.len(), again[0].attempt), (1, 2));
}

#[tokio::test]
async fn a_late_ack_or_retry_after_lease_expiry_does_not_touch_the_redelivery() {
    let q = queue(5);
    let s = svc(1);
    let id = q.enqueue(s, msg("slow consumer")).await.expect("enqueue");

    // Consumer A leases attempt 1, then outlives its visibility timeout.
    let a = q.lease(s, 1, Duration::from_millis(200)).await.expect("lease A");
    assert_eq!(a[0].attempt, 1);
    sleep_ms(400).await;
    // Consumer B gets the redelivery (attempt 2) and is still working on it.
    let b = q.lease(s, 1, LONG).await.expect("lease B");
    assert_eq!((b[0].id, b[0].attempt), (id, 2));

    // A finishes late: its ack and its retry are both fenced off by the attempt it leased.
    assert_eq!(
        q.ack_leased(&[(id, 1)]).await.expect("late ack"),
        0,
        "a stale ack deletes nothing"
    );
    let err = q.retry_leased(id, 1, Duration::ZERO).await.expect_err("stale retry");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
    assert_eq!(
        q.stats(s).await.expect("stats").in_flight,
        1,
        "B's delivery is still leased"
    );

    // B's own ack works.
    assert_eq!(q.ack_leased(&[(id, 2)]).await.expect("ack B"), 1);
    assert!(q.lease(s, 1, LONG).await.expect("lease after ack").is_empty());
}

#[tokio::test]
async fn ac_queue_08_retry_of_a_message_that_is_not_leased_is_not_found() {
    let q = queue(5);
    let s = svc(1);
    let err = q
        .retry(MsgId::from_u128(404), Duration::ZERO)
        .await
        .expect_err("unknown id");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
    let id = q.enqueue(s, msg("waiting")).await.expect("enqueue");
    q.lease(s, 1, LONG).await.expect("lease");
    q.ack(&[id]).await.expect("ack");
    assert!(
        q.retry(id, Duration::ZERO).await.is_err(),
        "an acked message cannot be retried"
    );
}

#[tokio::test]
async fn ac_queue_09_dlq_redrive_puts_messages_back_with_attempt_one() {
    let q = queue(1);
    let s = svc(1);
    let other = svc(2);
    let id = q.enqueue(s, msg("redrive me")).await.expect("enqueue");
    let other_id = q.enqueue(other, msg("other service")).await.expect("enqueue other");
    q.lease(s, 1, LONG).await.expect("lease");
    assert_eq!(
        q.retry(id, Duration::ZERO).await.expect("retry"),
        RetryResult::DeadLettered,
        "max_attempts=1 dead-letters the first failure"
    );
    q.lease(other, 1, LONG).await.expect("lease other");
    assert_eq!(
        q.retry(other_id, Duration::ZERO).await.expect("retry other"),
        RetryResult::DeadLettered
    );

    assert_eq!(q.dlq_redrive(s).await.expect("redrive"), 1);
    assert!(q.dlq_list(s, 10).await.expect("dlq").is_empty());
    assert_eq!(
        q.dlq_list(other, 10).await.expect("other dlq").len(),
        1,
        "redrive is per service"
    );

    let stats = q.stats(s).await.expect("stats");
    assert_eq!((stats.depth, stats.dlq), (1, 0));
    let leased = q.lease(s, 1, LONG).await.expect("lease");
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].id, id);
    assert_eq!(leased[0].body, Bytes::from_static(b"redrive me"));
    assert_eq!(leased[0].attempt, 1, "redrive resets the attempt count");

    assert_eq!(q.dlq_redrive(s).await.expect("redrive again"), 0);
}

#[tokio::test]
async fn ac_queue_10_dlq_purge_deletes_only_that_services_dead_letters() {
    let q = queue(1);
    let (a, b) = (svc(1), svc(2));
    for s in [a, b] {
        for i in 0..3 {
            let id = q.enqueue(s, msg(&format!("{i}"))).await.expect("enqueue");
            q.lease(s, 1, LONG).await.expect("lease");
            assert_eq!(
                q.retry(id, Duration::ZERO).await.expect("retry"),
                RetryResult::DeadLettered
            );
        }
    }
    assert_eq!(
        q.dlq_list(a, 2).await.expect("limited list").len(),
        2,
        "dlq_list honours the limit"
    );
    assert_eq!(q.dlq_list(a, 10).await.expect("list").len(), 3);

    assert_eq!(q.dlq_purge(a).await.expect("purge"), 3);
    assert!(q.dlq_list(a, 10).await.expect("list").is_empty());
    assert_eq!(q.stats(a).await.expect("stats").dlq, 0);
    assert_eq!(q.stats(b).await.expect("stats").dlq, 3);
    assert_eq!(q.dlq_purge(a).await.expect("purge twice"), 0);
}

#[tokio::test]
async fn ac_queue_11_stats_report_depth_in_flight_age_and_dlq() {
    let q = queue(5);
    let s = svc(1);
    let empty = q.stats(s).await.expect("stats");
    assert_eq!(
        (empty.depth, empty.in_flight, empty.oldest_age_ms, empty.dlq),
        (0, 0, 0, 0)
    );

    for i in 0..4 {
        q.enqueue(s, msg(&format!("{i}"))).await.expect("enqueue");
    }
    sleep_ms(120).await;
    let stats = q.stats(s).await.expect("stats");
    assert_eq!((stats.depth, stats.in_flight), (4, 0));
    assert!(
        stats.oldest_age_ms >= 100,
        "oldest waiting message is >= 120 ms old, got {}",
        stats.oldest_age_ms
    );
    assert!(stats.oldest_age_ms < 30_000);

    q.lease(s, 3, LONG).await.expect("lease 3");
    let stats = q.stats(s).await.expect("stats");
    assert_eq!((stats.depth, stats.in_flight), (1, 3));
}

#[tokio::test]
async fn ac_queue_12_same_dedup_id_is_delivered_once_per_service() {
    // TRG-12.
    let q = queue(5);
    let (a, b) = (svc(1), svc(2));
    let with = |body: &str, dedup: &str| NewMessage {
        body: Bytes::from(body.to_string()),
        delay: Duration::ZERO,
        dedup_id: Some(dedup.to_string()),
    };

    q.enqueue(a, with("first", "order-42")).await.expect("first");
    q.enqueue(a, with("second", "order-42"))
        .await
        .expect("duplicate enqueue is accepted but not queued twice");
    q.enqueue(a, with("third", "order-43"))
        .await
        .expect("different dedup id");
    q.enqueue(b, with("other service", "order-42"))
        .await
        .expect("same dedup id, other service");

    assert_eq!(q.stats(a).await.expect("stats").depth, 2);
    let mut bodies: Vec<Bytes> = q
        .lease(a, 10, LONG)
        .await
        .expect("lease")
        .into_iter()
        .map(|m| m.body)
        .collect();
    bodies.sort();
    assert_eq!(
        bodies,
        vec![Bytes::from_static(b"first"), Bytes::from_static(b"third")],
        "the first message wins"
    );
    assert_eq!(q.lease(b, 10, LONG).await.expect("lease b").len(), 1);

    // Without a dedup id every enqueue is its own message.
    q.enqueue(a, msg("same")).await.expect("e1");
    q.enqueue(a, msg("same")).await.expect("e2");
    assert_eq!(q.lease(a, 10, LONG).await.expect("lease").len(), 2);
}

#[tokio::test]
async fn ac_queue_13_services_with_ready_lists_only_services_with_visible_messages() {
    let q = queue(5);
    let (a, b, c) = (svc(1), svc(2), svc(3));
    q.enqueue(a, msg("ready")).await.expect("a");
    q.enqueue(
        b,
        NewMessage {
            body: Bytes::from_static(b"later"),
            delay: Duration::from_secs(3600),
            dedup_id: None,
        },
    )
    .await
    .expect("b");
    q.enqueue(c, msg("leased")).await.expect("c");
    q.lease(c, 1, LONG).await.expect("lease c");

    assert_eq!(q.services_with_ready().await.expect("ready"), vec![a]);
    q.lease(a, 1, LONG).await.expect("lease a");
    assert!(q.services_with_ready().await.expect("ready").is_empty());
}

#[tokio::test]
async fn ac_queue_14_services_never_see_each_others_messages() {
    let q = queue(5);
    let (a, b) = (svc(1), svc(2));
    q.enqueue(a, msg("for a")).await.expect("a");
    assert!(q.lease(b, 10, LONG).await.expect("lease b").is_empty());
    assert_eq!(q.stats(b).await.expect("stats b").depth, 0);
    assert_eq!(q.lease(a, 10, LONG).await.expect("lease a").len(), 1);
}

#[tokio::test]
async fn ac_queue_15_message_size_and_delay_limits_are_enforced() {
    let q = queue(5);
    let s = svc(1);

    q.enqueue(s, NewMessage::new(Bytes::from(vec![7u8; MAX_MESSAGE_BYTES])))
        .await
        .expect("exactly 1 MiB is allowed");
    let err = q
        .enqueue(s, NewMessage::new(Bytes::from(vec![7u8; MAX_MESSAGE_BYTES + 1])))
        .await
        .expect_err("over 1 MiB");
    assert!(matches!(err, StoreError::Invalid(_)), "got {err:?}");

    q.enqueue(
        s,
        NewMessage {
            body: Bytes::from_static(b"x"),
            delay: MAX_DELAY,
            dedup_id: None,
        },
    )
    .await
    .expect("7 days is allowed");
    let err = q
        .enqueue(
            s,
            NewMessage {
                body: Bytes::from_static(b"x"),
                delay: MAX_DELAY + Duration::from_secs(1),
                dedup_id: None,
            },
        )
        .await
        .expect_err("delay over 7 days");
    assert!(matches!(err, StoreError::Invalid(_)), "got {err:?}");
    assert_eq!(
        q.stats(s).await.expect("stats").depth,
        2,
        "rejected enqueues leave nothing behind"
    );
}

#[tokio::test]
async fn ac_queue_16_binary_bodies_roundtrip_exactly() {
    let q = queue(5);
    let s = svc(1);
    let body = Bytes::from(noise(10_000, 9));
    q.enqueue(s, NewMessage::new(body.clone())).await.expect("enqueue");
    let leased = q.lease(s, 1, LONG).await.expect("lease");
    assert_eq!(leased[0].body, body);
}

#[tokio::test]
async fn ac_queue_17_concurrent_consumers_never_receive_the_same_message() {
    let q = queue(5);
    let s = svc(1);
    for i in 0..40 {
        q.enqueue(s, msg(&format!("m{i}"))).await.expect("enqueue");
    }

    let mut handles = Vec::new();
    for _ in 0..8 {
        let q = q.clone();
        handles.push(tokio::spawn(async move {
            let mut got = Vec::new();
            loop {
                let batch = q.lease(s, 5, LONG).await.expect("lease");
                if batch.is_empty() {
                    break;
                }
                got.extend(batch.into_iter().map(|m| m.id));
            }
            got
        }));
    }
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.expect("join"));
    }
    let unique: HashSet<_> = all.iter().copied().collect();
    assert_eq!(all.len(), 40, "all messages delivered");
    assert_eq!(unique.len(), 40, "no message leased by two consumers at once");
}

#[tokio::test]
async fn ac_queue_18_messages_and_leases_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("queue.db");
    let s = svc(1);
    let (queued, leased_id) = {
        let q = SqliteQueueStore::open(&path, 5).expect("open");
        let queued = q.enqueue(s, msg("queued")).await.expect("enqueue");
        let leased_id = q.enqueue(s, msg("in flight")).await.expect("enqueue");
        let l = q.lease(s, 10, Duration::from_millis(300)).await.expect("lease");
        assert_eq!(l.len(), 2);
        q.ack(&[queued]).await.expect("ack one");
        (queued, leased_id)
    };

    let q = SqliteQueueStore::open(&path, 5).expect("reopen");
    sleep_ms(500).await;
    let after = q.lease(s, 10, LONG).await.expect("lease after restart (TRG-11)");
    assert_eq!(
        after.iter().map(|m| m.id).collect::<Vec<_>>(),
        vec![leased_id],
        "the unacked message is redelivered, the acked one is gone"
    );
    assert_ne!(after[0].id, queued);
    assert_eq!(after[0].attempt, 2);
}

// ---- B3.7: in-memory "has ready" hint ----------------------------------------------------------

#[tokio::test]
async fn ac_b37_services_with_ready_skips_the_database_while_nothing_can_be_ready() {
    let q = queue(5);
    let (a, b) = (svc(1), svc(2));
    for _ in 0..10 {
        assert!(q.services_with_ready().await.expect("ready").is_empty());
    }
    assert_eq!(q.ready_scans(), 0, "an empty queue is never scanned");

    q.enqueue(
        a,
        NewMessage {
            delay: Duration::from_secs(3600),
            ..msg("later")
        },
    )
    .await
    .expect("delayed");
    for _ in 0..10 {
        assert!(q.services_with_ready().await.expect("ready").is_empty());
    }
    assert_eq!(q.ready_scans(), 0, "a message due in an hour is not scanned for now");

    q.enqueue(b, msg("now")).await.expect("enqueue");
    assert_eq!(q.services_with_ready().await.expect("ready"), vec![b]);
    let scans = q.ready_scans();
    assert!(scans >= 1);

    // Leased: invisible for the lease, so nothing is due until it expires.
    let leased = q.lease(b, 10, LONG).await.expect("lease");
    assert_eq!(leased.len(), 1);
    for _ in 0..10 {
        assert!(q.services_with_ready().await.expect("ready").is_empty());
    }
    assert_eq!(q.ready_scans(), scans, "nothing due: no scans while the lease runs");

    // A retry makes it visible again and the hint follows.
    q.retry_leased(leased[0].id, leased[0].attempt, Duration::ZERO)
        .await
        .expect("retry");
    assert_eq!(q.services_with_ready().await.expect("ready"), vec![b]);
}

#[tokio::test]
async fn ac_b37_the_hint_never_hides_a_ready_message() {
    let q = queue(5);
    let s = svc(1);
    // Lease expiry makes a message visible again without any write.
    q.enqueue(s, msg("m")).await.expect("enqueue");
    let leased = q.lease(s, 10, Duration::from_millis(50)).await.expect("lease");
    assert_eq!(leased.len(), 1);
    assert!(q.services_with_ready().await.expect("ready").is_empty());
    sleep_ms(80).await;
    assert_eq!(
        q.services_with_ready().await.expect("ready"),
        vec![s],
        "visible again after the lease expired"
    );

    // A delayed message becomes ready when its delay passes.
    let d = svc(2);
    q.enqueue(
        d,
        NewMessage {
            delay: Duration::from_millis(50),
            ..msg("soon")
        },
    )
    .await
    .expect("delayed");
    sleep_ms(80).await;
    let ready: HashSet<_> = q.services_with_ready().await.expect("ready").into_iter().collect();
    assert!(ready.contains(&d), "{ready:?}");

    // Acked messages drop out.
    let all = q.lease(s, 10, LONG).await.expect("lease s");
    q.ack(&all.iter().map(|m| m.id).collect::<Vec<_>>()).await.expect("ack");
    let leased = q.lease(d, 10, LONG).await.expect("lease d");
    q.ack(&leased.iter().map(|m| m.id).collect::<Vec<_>>())
        .await
        .expect("ack d");
    assert!(q.services_with_ready().await.expect("ready").is_empty());
}

#[tokio::test]
async fn ac_b37_a_reopened_queue_finds_the_ready_messages_it_left() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("queue.db");
    let s = svc(7);
    {
        let q = SqliteQueueStore::open(&path, 5).expect("open");
        q.enqueue(s, msg("persisted")).await.expect("enqueue");
    }
    let q = SqliteQueueStore::open(&path, 5).expect("reopen");
    assert_eq!(q.services_with_ready().await.expect("ready"), vec![s]);
    // DLQ redrive also wakes the hint.
    let dlq = SqliteQueueStore::open(&dir.path().join("dlq.db"), 1).expect("open");
    dlq.enqueue(s, msg("poison")).await.expect("enqueue");
    let l = dlq.lease(s, 1, LONG).await.expect("lease");
    assert_eq!(
        dlq.retry_leased(l[0].id, l[0].attempt, Duration::ZERO)
            .await
            .expect("retry"),
        RetryResult::DeadLettered
    );
    assert!(dlq.services_with_ready().await.expect("ready").is_empty());
    assert_eq!(dlq.dlq_redrive(s).await.expect("redrive"), 1);
    assert_eq!(dlq.services_with_ready().await.expect("ready"), vec![s]);
}

/// QST-008: a retry or lease with an absurd delay or visibility (`Duration::MAX`) saturates the
/// timestamp instead of overflowing it (an overflow wrapped `visible_at` negative in release
/// builds, making the message visible at once, and panicked the writer thread in debug builds).
#[tokio::test]
async fn qst_008_huge_delays_and_visibilities_saturate() {
    let q = queue(5);
    let s = svc(1);
    q.enqueue(s, NewMessage::new(&b"m"[..])).await.expect("enqueue");
    let leased = q.lease(s, 1, Duration::MAX).await.expect("lease");
    assert_eq!(leased.len(), 1);
    assert!(
        q.lease(s, 1, Duration::from_secs(1)).await.expect("lease").is_empty(),
        "leased forever"
    );
    let id = leased[0].id;
    assert!(matches!(
        q.retry(id, Duration::MAX).await.expect("retry"),
        RetryResult::Requeued { .. }
    ));
    assert!(
        q.lease(s, 1, Duration::from_secs(1)).await.expect("lease").is_empty(),
        "not visible again"
    );
}

/// META-010: deleting a service purges its messages, dead letters, dedup ids and one-off
/// schedules from queue.db (they live outside meta.db's cascade); other services keep theirs.
#[tokio::test]
async fn meta_010_purging_a_service_clears_its_queue_rows() {
    use rt_store::{DelayedStore, NewDelayed};
    let q = queue(1);
    for s in [svc(1), svc(2)] {
        q.enqueue(
            s,
            NewMessage {
                body: Bytes::from_static(b"{}"),
                delay: Duration::ZERO,
                dedup_id: Some("d".into()),
            },
        )
        .await
        .expect("enqueue");
        q.enqueue(s, msg("{}")).await.expect("enqueue");
        DelayedStore::put(
            &*q,
            s,
            NewDelayed {
                id: "later".into(),
                run_at_ms: i64::MAX / 2,
                payload: Bytes::from_static(b"{}"),
            },
        )
        .await
        .expect("delayed");
    }
    // One dead letter for service 1.
    let leased = QueueStore::lease(&*q, svc(1), 1, Duration::from_secs(30))
        .await
        .expect("lease");
    QueueStore::retry(&*q, leased[0].id, Duration::ZERO)
        .await
        .expect("to the dlq");
    assert_eq!(q.dlq_list(svc(1), 10).await.expect("dlq").len(), 1);

    q.purge_service(svc(1)).await.expect("purge");
    let st = q.stats(svc(1)).await.expect("stats");
    assert_eq!(
        (st.depth, q.dlq_list(svc(1), 10).await.expect("dlq").len()),
        (0, 0),
        "{st:?}"
    );
    assert!(DelayedStore::list(&*q, svc(1), None, 10)
        .await
        .expect("delayed")
        .is_empty());
    assert!(!q.services_with_ready().await.expect("ready").contains(&svc(1)));
    // A new message with the old dedup id is a new message.
    q.enqueue(
        svc(1),
        NewMessage {
            body: Bytes::from_static(b"{}"),
            delay: Duration::ZERO,
            dedup_id: Some("d".into()),
        },
    )
    .await
    .expect("enqueue");
    assert_eq!(q.stats(svc(1)).await.expect("stats").depth, 1);

    assert_eq!(q.stats(svc(2)).await.expect("stats").depth, 2, "service 2 untouched");
    assert_eq!(
        DelayedStore::list(&*q, svc(2), None, 10).await.expect("delayed").len(),
        1
    );
}

/// QST-004: a duplicate dedup id returns the first message's id and keeps the first body.
#[tokio::test]
async fn qst_004_a_duplicate_dedup_id_returns_the_first_id() {
    let q = queue(5);
    let with = |body: &'static str| NewMessage {
        body: Bytes::from_static(body.as_bytes()),
        delay: Duration::ZERO,
        dedup_id: Some("x".into()),
    };
    let first = q.enqueue(svc(1), with("first")).await.expect("first");
    let again = q.enqueue(svc(1), with("second")).await.expect("again");
    assert_eq!(first, again);
    let got = QueueStore::lease(&*q, svc(1), 10, Duration::from_secs(30))
        .await
        .expect("lease");
    assert_eq!((got.len(), got[0].body.clone()), (1, Bytes::from_static(b"first")));
}

/// CALL-011: concurrent capped enqueues at depth max-1 admit exactly one; a duplicate dedup id
/// at the cap still answers with its message (it adds nothing).
#[tokio::test]
async fn call_011_capped_enqueues_never_overshoot_the_depth() {
    let dir = tempfile::tempdir().expect("tempdir");
    let q = SqliteQueueStore::open(&dir.path().join("queue.db"), 5).expect("open");
    for i in 0..9 {
        q.enqueue_capped(
            svc(1),
            NewMessage {
                body: Bytes::from(format!("{i}")),
                delay: Duration::ZERO,
                dedup_id: Some(format!("d{i}")),
            },
            10,
        )
        .await
        .expect("under the cap");
    }
    let tries = (0..8).map(|_| q.enqueue_capped(svc(1), msg("x"), 10));
    let results = futures_util::future::join_all(tries).await;
    let ok = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok, 1, "{results:?}");
    assert!(results
        .iter()
        .filter(|r| r.is_err())
        .all(|r| matches!(r, Err(StoreError::Conflict("queue_full")))));
    assert_eq!(q.stats(svc(1)).await.expect("stats").depth, 10);
    let again = q
        .enqueue_capped(
            svc(1),
            NewMessage {
                body: Bytes::from_static(b"0"),
                delay: Duration::ZERO,
                dedup_id: Some("d0".into()),
            },
            10,
        )
        .await;
    assert!(again.is_ok(), "a dedup hit adds nothing: {again:?}");
}

// ---------------------------------------------------------------------------------------------
// Async invocations (queue.db v3)
// ---------------------------------------------------------------------------------------------

mod invocations {
    use super::*;
    use rt_store::queue::{Accepted, InvocationState, InvocationStore, NewInvocation, MIGRATIONS};
    use rt_types::InvocationId;

    fn inv(s: rt_types::ServiceId, key: Option<&str>, hash: &str) -> NewInvocation {
        NewInvocation {
            id: InvocationId::new(),
            service: s,
            method: "POST".into(),
            uri: "/hook?x=1".into(),
            sealed_headers: vec![0, 1, 2, 255],
            sealed_body: b"sealed-body".to_vec(),
            body_bytes: 4,
            max_age: Duration::from_secs(3600),
            max_attempts: 3,
            idempotency_key: key.map(str::to_string),
            request_hash: hash.into(),
        }
    }

    async fn accept_new(q: &SqliteQueueStore, n: NewInvocation) -> InvocationId {
        match q.accept(n, 100_000).await.expect("accept") {
            Accepted::New(id) => id,
            other => panic!("expected a new record, got {other:?}"),
        }
    }

    /// Accept persists the sealed bytes untouched; a lease hands them back with attempt 1;
    /// complete marks the record succeeded and wipes headers and body.
    #[tokio::test]
    async fn ac_async_01_accept_and_lease_and_complete_wipes_headers_and_body() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("queue.db");
        let q = SqliteQueueStore::open(&path, 5).expect("file store");
        let s = svc(1);
        let id = accept_new(&q, inv(s, None, "h")).await;
        assert_eq!(q.services_with_invocations().await.expect("ready"), vec![s]);
        let l = q.lease_invocations(s, 10, LONG).await.expect("lease");
        assert_eq!(l.len(), 1);
        assert_eq!(l[0].id, id);
        assert_eq!(l[0].sealed_headers, vec![0, 1, 2, 255], "opaque header bytes kept");
        assert_eq!(l[0].sealed_body, b"sealed-body".to_vec());
        assert_eq!(
            (l[0].attempt, l[0].method.as_str(), l[0].uri.as_str()),
            (1, "POST", "/hook?x=1")
        );
        assert!(
            q.services_with_invocations().await.expect("ready").is_empty(),
            "leased = invisible"
        );
        assert!(!q.complete(id, 2, 200).await.expect("stale"), "fenced by attempt");
        assert!(q.complete(id, 1, 204).await.expect("complete"));
        assert!(!q.complete(id, 1, 204).await.expect("twice"), "no lease left");
        let r = q.invocation(s, id).await.expect("get").expect("record");
        assert_eq!(r.state, InvocationState::Succeeded);
        assert_eq!((r.status, r.condition.as_deref()), (Some(204), Some("success")));
        assert!(r.expires_at_ms - r.updated_at_ms >= 24 * 3600 * 1000 - 1);
        let (h, b): (Option<Vec<u8>>, Option<Vec<u8>>) = rusqlite::Connection::open(&path)
            .expect("raw")
            .query_row(
                "SELECT headers, body FROM invocations WHERE id = ?1",
                [id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("row");
        assert!(h.is_none() && b.is_none(), "headers and body wiped");
        assert!(
            q.invocation(svc(2), id).await.expect("other").is_none(),
            "scoped to its service"
        );
    }

    /// A failed attempt is retried after its delay; the attempt at max_attempts fails the record.
    #[tokio::test]
    async fn ac_async_02_fail_attempt_retries_then_fails_at_max_attempts() {
        let q = queue(5);
        let s = svc(1);
        let id = accept_new(&q, inv(s, None, "h")).await;
        for attempt in 1..=3u16 {
            let l = q.lease_invocations(s, 10, LONG).await.expect("lease");
            assert_eq!(l[0].attempt, attempt);
            let st = q
                .fail_attempt(id, attempt, "status_5xx", Some(503), Duration::ZERO)
                .await
                .expect("fail");
            let want = if attempt < 3 {
                InvocationState::Pending
            } else {
                InvocationState::Failed
            };
            assert_eq!(st, want);
        }
        let r = q.invocation(s, id).await.expect("get").expect("record");
        assert_eq!(r.condition.as_deref(), Some("retries_exhausted"));
        assert_eq!(
            (r.error.as_deref(), r.status, r.attempts),
            (Some("status_5xx"), Some(503), 3)
        );
        assert!(matches!(
            q.fail_attempt(id, 3, "x", None, Duration::ZERO).await,
            Err(StoreError::NotFound)
        ));
        assert!(q.lease_invocations(s, 10, LONG).await.expect("lease").is_empty());
    }

    /// `fail_now` fails the leased delivery at once (condition `unreadable`, no retry) and is
    /// fenced like `fail_attempt`.
    #[tokio::test]
    async fn ac_async_11_fail_now_fails_at_once_without_retry() {
        let q = queue(5);
        let s = svc(1);
        let id = accept_new(&q, inv(s, None, "h")).await;
        assert!(matches!(
            q.fail_now(id, 1, "sealed_unreadable").await,
            Err(StoreError::NotFound)
        ));
        let l = q.lease_invocations(s, 10, LONG).await.expect("lease");
        assert!(matches!(
            q.fail_now(id, 2, "sealed_unreadable").await,
            Err(StoreError::NotFound)
        ));
        q.fail_now(id, l[0].attempt, "sealed_unreadable")
            .await
            .expect("fail now");
        let r = q.invocation(s, id).await.expect("get").expect("record");
        assert_eq!(r.state, InvocationState::Failed);
        assert_eq!(r.condition.as_deref(), Some("unreadable"));
        assert_eq!((r.error.as_deref(), r.attempts), (Some("sealed_unreadable"), 1));
        assert!(q.lease_invocations(s, 10, LONG).await.expect("lease").is_empty());
    }

    /// A release (throttle) hands the delivery back without spending an attempt.
    #[tokio::test]
    async fn ac_async_03_release_does_not_count_an_attempt() {
        let q = queue(5);
        let s = svc(1);
        let id = accept_new(&q, inv(s, None, "h")).await;
        for _ in 0..10 {
            let l = q.lease_invocations(s, 10, LONG).await.expect("lease");
            assert_eq!(l[0].attempt, 1);
            assert!(!q.release(id, 2, Duration::ZERO).await.expect("stale"));
            assert!(q.release(id, 1, Duration::ZERO).await.expect("release"));
        }
        let l = q.lease_invocations(s, 10, LONG).await.expect("lease");
        q.release(id, l[0].attempt, LONG).await.expect("release later");
        assert!(
            q.lease_invocations(s, 10, LONG).await.expect("lease").is_empty(),
            "delayed"
        );
    }

    /// The service's pending count is capped at accept.
    #[tokio::test]
    async fn ac_async_04_backlog_cap_refuses_with_async_backlog_full() {
        let q = queue(5);
        let s = svc(1);
        q.accept(inv(s, None, "a"), 2).await.expect("1");
        q.accept(inv(s, None, "b"), 2).await.expect("2");
        assert!(matches!(
            q.accept(inv(s, None, "c"), 2).await,
            Err(StoreError::Conflict("async_backlog_full"))
        ));
        q.accept(inv(svc(2), None, "c"), 2)
            .await
            .expect("other service has its own cap");
    }

    /// The same key with the same hash replays the record (with its current state).
    #[tokio::test]
    async fn ac_async_05_idempotency_key_replays_while_the_record_exists() {
        let q = queue(5);
        let s = svc(1);
        let id = accept_new(&q, inv(s, Some("k"), "h")).await;
        assert_eq!(
            q.accept(inv(s, Some("k"), "h"), 100).await.expect("replay"),
            Accepted::Replay {
                id,
                state: InvocationState::Pending
            }
        );
        let l = q.lease_invocations(s, 1, LONG).await.expect("lease");
        q.complete(id, l[0].attempt, 200).await.expect("complete");
        assert_eq!(
            q.accept(inv(s, Some("k"), "h"), 0)
                .await
                .expect("replay ignores the cap"),
            Accepted::Replay {
                id,
                state: InvocationState::Succeeded
            }
        );
        accept_new(&q, inv(svc(2), Some("k"), "h")).await;
        q.expire(i64::MAX - 1).await.expect("sweep");
        accept_new(&q, inv(s, Some("k"), "h")).await;
    }

    /// The sweep fails pending records past their age and deletes finished records past retention.
    #[tokio::test]
    async fn ac_async_06_expire_fails_event_age_exceeded_and_deletes_finished_records() {
        let q = queue(5);
        let s = svc(1);
        let id = accept_new(&q, inv(s, None, "h")).await;
        let done = accept_new(&q, inv(s, None, "d")).await;
        let l = q.lease_invocations(s, 10, LONG).await.expect("lease");
        let d = l.iter().find(|l| l.id == done).expect("leased");
        q.complete(done, d.attempt, 200).await.expect("complete");
        let now = now_ms();
        let st = q.expire(now).await.expect("nothing yet");
        assert_eq!((st.aged_out, st.deleted), (0, 0));
        let st = q.expire(now + 3_600_001).await.expect("age");
        assert_eq!(st.aged_out, 1);
        let r = q.invocation(s, id).await.expect("get").expect("record");
        assert_eq!(
            (r.state, r.condition.as_deref()),
            (InvocationState::Failed, Some("event_age_exceeded"))
        );
        let st = q.expire(now + 25 * 3600 * 1000).await.expect("retention");
        assert_eq!(st.deleted, 1, "the succeeded record goes after 24 h");
        assert!(q.invocation(s, done).await.expect("get").is_none());
        let st = q.expire(i64::MAX - 1).await.expect("all");
        assert_eq!(st.deleted, 1, "the failed record goes after 7 d");
    }

    /// Redrive puts failed records back to pending with attempts 0; purge deletes them.
    #[tokio::test]
    async fn ac_async_07_redrive_and_purge_failed() {
        let q = queue(5);
        let s = svc(1);
        let mut one = inv(s, None, "h");
        one.max_attempts = 1;
        let id = accept_new(&q, one).await;
        let l = q.lease_invocations(s, 1, LONG).await.expect("lease");
        q.fail_attempt(id, l[0].attempt, "guest_error", None, Duration::ZERO)
            .await
            .expect("fail");
        assert_eq!(q.redrive_invocations(s, 100_000).await.expect("redrive"), 1);
        let r = q.invocation(s, id).await.expect("get").expect("record");
        assert_eq!(
            (r.state, r.attempts, r.error.clone()),
            (InvocationState::Pending, 0, None)
        );
        let l = q.lease_invocations(s, 1, LONG).await.expect("lease again");
        assert_eq!(l[0].attempt, 1);
        assert_eq!(
            l[0].sealed_body,
            b"sealed-body".to_vec(),
            "the request survives for redrive"
        );
        q.fail_attempt(id, 1, "guest_error", None, Duration::ZERO)
            .await
            .expect("fail");
        assert_eq!(q.purge_failed(svc(2)).await.expect("other"), 0);
        assert_eq!(q.purge_failed(s).await.expect("purge"), 1);
        assert!(q.invocation(s, id).await.expect("get").is_none());
    }

    /// Lists are newest first, filtered by state, paged with an opaque cursor.
    #[tokio::test]
    async fn ac_async_08_list_by_state_pages_with_a_cursor() {
        let q = queue(5);
        let s = svc(1);
        let mut ids = Vec::new();
        for i in 0..5 {
            ids.push(accept_new(&q, inv(s, None, &i.to_string())).await);
            sleep_ms(2).await;
        }
        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let p = q.list_invocations(s, None, 2, cursor).await.expect("page");
            assert!(p.items.len() <= 2);
            seen.extend(p.items.iter().map(|r| r.id));
            match p.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        ids.reverse();
        assert_eq!(seen, ids, "newest first, every record once");
        let p = q
            .list_invocations(s, Some(InvocationState::Failed), 10, None)
            .await
            .expect("failed");
        assert!(p.items.is_empty());
        assert!(matches!(
            q.list_invocations(s, None, 10, Some("garbage".into())).await,
            Err(StoreError::Invalid(_))
        ));
    }

    /// A v2 database keeps its messages and gains the invocations table on open.
    #[tokio::test]
    async fn ac_async_09_upgrade_from_v2_keeps_messages_and_adds_invocations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let p = dir.path().join("queue.db");
        {
            let c = rusqlite::Connection::open(&p).expect("raw");
            for (v, sql) in MIGRATIONS.iter().filter(|(v, _)| *v <= 2) {
                c.execute_batch(sql).expect("old schema");
                c.pragma_update(None, "user_version", *v).expect("version");
            }
            c.execute(
                "INSERT INTO messages (id, service_id, body, attempt, visible_at, enqueued_at, dedup_id, leased) \
                 VALUES (?1, ?2, x'6869', 0, 0, 0, NULL, 0)",
                rusqlite::params![MsgId::new().to_string(), svc(1).to_string()],
            )
            .expect("v2 message");
        }
        let q = SqliteQueueStore::open(&p, 5).expect("upgrade");
        let l = q.lease(svc(1), 10, LONG).await.expect("lease");
        assert_eq!(l[0].body, Bytes::from_static(b"hi"));
        accept_new(&q, inv(svc(1), None, "h")).await;
    }

    /// Deleting a service removes its invocations too.
    #[tokio::test]
    async fn ac_async_10_purge_service_removes_invocations() {
        let q = queue(5);
        let id = accept_new(&q, inv(svc(1), None, "h")).await;
        let other = accept_new(&q, inv(svc(2), None, "h")).await;
        assert!(q.purge_service(svc(1)).await.expect("purge") >= 1);
        assert!(q.invocation(svc(1), id).await.expect("get").is_none());
        assert!(q.invocation(svc(2), other).await.expect("get").is_some());
    }

    /// Each lease is an attempt (a crash mid-run spends it); a release refunds it.
    #[tokio::test]
    async fn ac_async_11_lease_counts_the_attempt_and_release_refunds_it() {
        let q = queue(5);
        let s = svc(1);
        let id = accept_new(&q, inv(s, None, "h")).await;
        q.lease_invocations(s, 1, Duration::ZERO).await.expect("lease 1");
        let l = q
            .lease_invocations(s, 1, Duration::ZERO)
            .await
            .expect("lease after expiry");
        assert_eq!(l[0].attempt, 2, "the expired lease counted");
        assert!(
            !q.complete(id, 1, 200).await.expect("old"),
            "the first delivery is fenced out"
        );
        assert!(q.release(id, 2, Duration::ZERO).await.expect("release"));
        let r = q.invocation(s, id).await.expect("get").expect("record");
        assert_eq!(r.attempts, 1);
    }

    /// A due record whose every attempt was started without a settle fails as `crashed`.
    #[tokio::test]
    async fn ac_async_12_a_lease_past_max_attempts_fails_crashed() {
        let q = queue(5);
        let s = svc(1);
        let id = accept_new(&q, inv(s, None, "h")).await;
        for _ in 0..3 {
            assert_eq!(q.lease_invocations(s, 1, Duration::ZERO).await.expect("lease").len(), 1);
        }
        assert!(q
            .lease_invocations(s, 1, Duration::ZERO)
            .await
            .expect("lease")
            .is_empty());
        let r = q.invocation(s, id).await.expect("get").expect("record");
        assert_eq!(r.state, InvocationState::Failed);
        assert_eq!(
            (r.condition.as_deref(), r.error.as_deref()),
            (Some("retries_exhausted"), Some("crashed"))
        );
        assert_eq!(q.lease_invocations(s, 0, LONG).await.expect("zero").len(), 0);
    }

    /// At most 1000 failed records are kept per service; the oldest go first.
    #[tokio::test]
    async fn ac_async_13_failed_records_beyond_1000_are_evicted_oldest_first() {
        let q = queue(5);
        let s = svc(1);
        async fn fail_one(q: &SqliteQueueStore, s: rt_types::ServiceId) -> InvocationId {
            let mut n = inv(s, None, "h");
            n.max_attempts = 1;
            let id = accept_new(q, n).await;
            q.lease_invocations(s, 1, LONG).await.expect("lease");
            q.fail_attempt(id, 1, "guest_error", None, Duration::ZERO)
                .await
                .expect("fail");
            id
        }
        let oldest = fail_one(&q, s).await;
        sleep_ms(3).await;
        let mut last = oldest;
        for _ in 0..1000 {
            last = fail_one(&q, s).await;
        }
        assert_eq!(q.take_evicted(), 1);
        assert_eq!(q.take_evicted(), 0, "taken");
        assert!(
            q.invocation(s, oldest).await.expect("get").is_none(),
            "the oldest was evicted"
        );
        assert!(q.invocation(s, last).await.expect("get").is_some());
        let p = q.list_invocations(s, None, 1, None).await.expect("counts");
        assert_eq!(p.counts.failed, 1000);
    }

    /// The same idempotency key with another request hash is a conflict.
    #[tokio::test]
    async fn ac_async_14_same_key_other_hash_is_idempotency_mismatch() {
        let q = queue(5);
        let s = svc(1);
        accept_new(&q, inv(s, Some("k"), "h1")).await;
        assert!(matches!(
            q.accept(inv(s, Some("k"), "h2"), 100).await,
            Err(StoreError::Conflict("idempotency_mismatch"))
        ));
    }

    /// List pages carry the service's pending and failed counts.
    #[tokio::test]
    async fn ac_async_15_counts_report_pending_and_failed() {
        let q = queue(5);
        let s = svc(1);
        accept_new(&q, inv(s, None, "a")).await;
        let mut f = inv(s, None, "b");
        f.max_attempts = 1;
        let id = accept_new(&q, f).await;
        accept_new(&q, inv(svc(2), None, "c")).await;
        let l = q.lease_invocations(s, 10, LONG).await.expect("lease");
        let a = l.iter().find(|l| l.id == id).expect("leased").attempt;
        q.fail_attempt(id, a, "guest_error", None, Duration::ZERO)
            .await
            .expect("fail");
        let p = q.list_invocations(s, None, 10, None).await.expect("list");
        assert_eq!((p.counts.pending, p.counts.failed), (1, 1));
        assert_eq!(p.items.len(), 2);
    }

    /// B1: pending + failed sealed bytes are capped per tenant across its services; succeeded
    /// records (wiped) and other tenants do not count.
    #[tokio::test]
    async fn b1_tenant_byte_cap_spans_services_and_refuses_with_async_backlog_full() {
        use rt_store::queue::AcceptLimits;
        let q = queue(5);
        let t = rt_types::TenantId::from_u128(1);
        let other = rt_types::TenantId::from_u128(2);
        let big = |s| {
            let mut n = inv(s, None, "h");
            n.sealed_body = vec![7; 1000];
            n
        };
        let per = 1000 + 4; // sealed body + sealed headers
        let lim = |tenant| AcceptLimits {
            tenant: Some(tenant),
            max_pending: 100,
            max_tenant_bytes: 3 * per,
        };
        let first = match q.accept_with(big(svc(1)), lim(t)).await.expect("1") {
            Accepted::New(id) => id,
            other => panic!("{other:?}"),
        };
        q.accept_with(big(svc(2)), lim(t)).await.expect("2");
        let mut failing = big(svc(2));
        failing.max_attempts = 1;
        let fid = match q.accept_with(failing, lim(t)).await.expect("3") {
            Accepted::New(id) => id,
            other => panic!("{other:?}"),
        };
        assert!(matches!(
            q.accept_with(big(svc(1)), lim(t)).await,
            Err(StoreError::Conflict("async_backlog_full"))
        ));
        q.accept_with(big(svc(3)), lim(other))
            .await
            .expect("another tenant has its own budget");
        // A failed record still holds its bytes.
        let l = q.lease_invocations(svc(2), 10, LONG).await.expect("lease");
        let a = l.iter().find(|l| l.id == fid).expect("leased").attempt;
        q.fail_attempt(fid, a, "guest_error", None, Duration::ZERO)
            .await
            .expect("fail");
        assert!(matches!(
            q.accept_with(big(svc(1)), lim(t)).await,
            Err(StoreError::Conflict("async_backlog_full"))
        ));
        // A succeeded record is wiped and frees its bytes.
        let l = q.lease_invocations(svc(1), 10, LONG).await.expect("lease");
        assert!(q.complete(first, l[0].attempt, 200).await.expect("complete"));
        q.accept_with(big(svc(1)), lim(t)).await.expect("room again");
    }

    /// MINOR 3: redrive leaves `unreadable` records failed and never pushes the service past
    /// `max_pending`.
    #[tokio::test]
    async fn minor3_redrive_skips_unreadable_and_respects_the_pending_cap() {
        let q = queue(5);
        let s = svc(1);
        let mut ids = Vec::new();
        for h in ["a", "b", "c"] {
            let mut n = inv(s, None, h);
            n.max_attempts = 1;
            ids.push(accept_new(&q, n).await);
        }
        let l = q.lease_invocations(s, 10, LONG).await.expect("lease");
        for x in &l {
            if x.id == ids[0] {
                q.fail_now(x.id, x.attempt, "sealed_unreadable")
                    .await
                    .expect("fail now");
            } else {
                q.fail_attempt(x.id, x.attempt, "guest_error", None, Duration::ZERO)
                    .await
                    .expect("fail");
            }
        }
        accept_new(&q, inv(s, None, "pending")).await;
        // One pending already, cap 2: only one of the two readable failures comes back.
        assert_eq!(q.redrive_invocations(s, 2).await.expect("redrive"), 1);
        let r = q.invocation(s, ids[0]).await.expect("get").expect("record");
        assert_eq!(r.state, InvocationState::Failed, "unreadable stays failed");
        assert_eq!(q.redrive_invocations(s, 2).await.expect("full"), 0);
        assert_eq!(q.redrive_invocations(s, 100).await.expect("rest"), 1);
        let r = q.invocation(s, ids[0]).await.expect("get").expect("record");
        assert_eq!(r.state, InvocationState::Failed);
    }
}
