//! `queue.db` paths the behavioural tests do not reach: a database whose directory cannot be
//! created, a dedup record lost while its message is still queued, a depth cap whose count fails,
//! and a dead letter or DLQ read that fails. A second connection to the same file breaks the
//! table involved, the way a damaged or half-migrated database would.

use crate::common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{DelayedStore, NewDelayed, NewMessage, QueueStore, SqliteQueueStore};

const LONG: Duration = Duration::from_secs(60);

fn file_queue(dir: &Path, max_attempts: u16) -> (std::sync::Arc<SqliteQueueStore>, PathBuf) {
    let p = dir.join("queue.db");
    (
        SqliteQueueStore::open(&p, max_attempts).expect("file queue store opens"),
        p,
    )
}

fn raw(p: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(p).expect("second connection to the same file")
}

#[test]
fn a_queue_whose_directory_cannot_be_created_fails_to_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"a file, not a directory").expect("write");
    assert!(SqliteQueueStore::open(&blocker.join("queue.db"), 5).is_err());
}

#[tokio::test]
async fn a_message_still_queued_after_its_dedup_record_is_gone_is_returned_again_not_duplicated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (q, p) = file_queue(dir.path(), 5);
    let s = svc(7);
    let mut m = NewMessage::new(Bytes::from_static(b"once"));
    m.dedup_id = Some("order-42".into());

    let first = q.enqueue(s, m.clone()).await.expect("first enqueue");
    raw(&p)
        .execute("DELETE FROM dedup", [])
        .expect("dedup window forgotten, message still queued");

    let again = q.enqueue(s, m).await.expect("same dedup id");
    assert_eq!(again, first, "the queued message is the one that answers");
    assert_eq!(
        QueueStore::lease(&*q, s, 10, LONG).await.expect("lease").len(),
        1,
        "delivered once"
    );
    assert_eq!(q.stats(s).await.expect("stats").depth, 0);
}

#[tokio::test]
async fn a_depth_capped_enqueue_fails_when_the_depth_cannot_be_counted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (q, p) = file_queue(dir.path(), 5);
    let s = svc(8);
    raw(&p)
        .execute_batch("ALTER TABLE messages RENAME TO messages_gone")
        .expect("break messages");

    assert!(q
        .enqueue_capped(s, NewMessage::new(Bytes::from_static(b"x")), 3)
        .await
        .is_err());
    assert!(q.stats(s).await.is_err(), "stats counts the same table");
    raw(&p)
        .execute_batch("ALTER TABLE messages_gone RENAME TO messages")
        .expect("repair");
    q.enqueue_capped(s, NewMessage::new(Bytes::from_static(b"x")), 3)
        .await
        .expect("works again");
}

#[tokio::test]
async fn a_poison_message_that_cannot_reach_the_dlq_is_an_error_not_a_silent_drop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (q, p) = file_queue(dir.path(), 1);
    let s = svc(9);
    q.enqueue(s, NewMessage::new(Bytes::from_static(b"poison")))
        .await
        .expect("enqueue");
    let first = QueueStore::lease(&*q, s, 1, Duration::from_millis(100))
        .await
        .expect("first delivery");
    assert_eq!(first.len(), 1);
    tokio::time::sleep(Duration::from_millis(150)).await;

    raw(&p)
        .execute_batch("ALTER TABLE dlq RENAME TO dlq_gone")
        .expect("break dlq");
    assert!(
        QueueStore::lease(&*q, s, 1, LONG).await.is_err(),
        "the exhausted message cannot be dead-lettered"
    );
    raw(&p)
        .execute_batch("ALTER TABLE dlq_gone RENAME TO dlq")
        .expect("repair");
}

#[tokio::test]
async fn dlq_reads_and_redrive_report_a_broken_dlq_as_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (q, p) = file_queue(dir.path(), 5);
    let s = svc(10);
    raw(&p)
        .execute_batch("ALTER TABLE dlq RENAME TO dlq_gone")
        .expect("break dlq");

    assert!(q.dlq_list(s, 10).await.is_err());
    assert!(q.dlq_redrive(s).await.is_err());
    raw(&p)
        .execute_batch("ALTER TABLE dlq_gone RENAME TO dlq")
        .expect("repair");
    assert!(q.dlq_list(s, 10).await.expect("list again").is_empty());
}

#[tokio::test]
async fn delayed_schedules_report_a_broken_table_as_an_error_on_each_operation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (q, p) = file_queue(dir.path(), 5);
    let s = svc(11);
    raw(&p)
        .execute_batch("ALTER TABLE delayed RENAME TO delayed_gone")
        .expect("break delayed");

    let item = NewDelayed {
        id: "job".into(),
        run_at_ms: now_ms(),
        payload: Bytes::from_static(b"{}"),
    };
    assert!(q.put(s, item).await.is_err());
    assert!(q.list(s, None, 10).await.is_err());
    assert!(q.done(s, "job", 1).await.is_err());
    assert!(q.release(s, "job", 1, now_ms()).await.is_err());

    raw(&p)
        .execute_batch("ALTER TABLE delayed_gone RENAME TO delayed")
        .expect("repair");
    assert!(q.list(s, None, 10).await.expect("list again").is_empty());
}

#[tokio::test]
async fn a_queue_opened_without_a_directory_part_is_an_anonymous_database() {
    let q = SqliteQueueStore::open(Path::new(""), 5).expect("anonymous database");
    q.enqueue(svc(12), NewMessage::new(Bytes::from_static(b"x")))
        .await
        .expect("the writer has the schema");
}

/// QENG-003: `release_leased` hands a leased message back with the lease's attempt refunded, and
/// only while the message is still on that leased attempt.
#[tokio::test]
async fn qeng_003_release_leased_restores_the_attempt_and_is_fenced() {
    let q = SqliteQueueStore::open_in_memory(2).expect("queue");
    let s = common::svc(1);
    let id = q.enqueue(s, NewMessage::new("m")).await.expect("enqueue");
    assert_eq!(q.release_leased(&[], Duration::ZERO).await.expect("empty"), 0);
    for _ in 0..5 {
        let leased = QueueStore::lease(&*q, s, 10, LONG).await.expect("lease");
        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].attempt, 1, "a released delivery never used up an attempt");
        assert_eq!(
            q.release_leased(&[(id, 2)], Duration::ZERO).await.expect("stale"),
            0,
            "fenced"
        );
        assert_eq!(q.release_leased(&[(id, 1)], Duration::ZERO).await.expect("release"), 1);
        assert_eq!(
            q.release_leased(&[(id, 1)], Duration::ZERO).await.expect("again"),
            0,
            "no lease left"
        );
    }
    assert_eq!(q.stats(s).await.expect("stats").dlq, 0);
    let ready = q.services_with_ready().await.expect("ready");
    assert_eq!(ready, vec![s], "a release makes the message due again");
    let delayed = QueueStore::lease(&*q, s, 10, LONG).await.expect("lease");
    q.release_leased(&[(delayed[0].id, 1)], Duration::from_secs(60))
        .await
        .expect("release later");
    assert!(
        QueueStore::lease(&*q, s, 10, LONG).await.expect("lease").is_empty(),
        "invisible until the delay ends"
    );
}
