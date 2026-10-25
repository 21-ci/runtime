//! Stream store tests: `streams.db` topics, partitions, offsets, cursors and dead letters.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{
    CursorKey, Fault, Faults, NewRecord, ReadFrom, SqliteStreamStore, StoreError, StreamStore, Topic, TopicSpec,
};
use rt_types::events::{partition_for, Namespace, StartPosition, StreamRecord, DEAD_MAX_PER_CONSUMER};
use rt_types::{ProjectId, ServiceId, TenantId};

const BIG: u64 = u64::MAX;
const HOUR: u64 = 3_600_000;

fn store() -> Arc<SqliteStreamStore> {
    SqliteStreamStore::open_in_memory().expect("in-memory stream store")
}

fn ns(t: u128, p: u128, env: &str) -> Namespace {
    Namespace {
        tenant: TenantId::from_u128(t),
        project: ProjectId::from_u128(p),
        environment: env.to_string(),
    }
}

fn spec(name: &str, partitions: u32) -> TopicSpec {
    TopicSpec {
        name: name.into(),
        partitions,
        retention_ms: HOUR,
        max_bytes: 0,
    }
}

async fn topic(s: &SqliteStreamStore, name: &str, partitions: u32) -> Topic {
    s.create_topic(&ns(1, 1, "prod"), spec(name, partitions), 100)
        .await
        .expect("create topic")
}

fn rec(body: &str) -> NewRecord {
    NewRecord::new(Bytes::from(body.to_string()))
}

fn keyed(key: &str, body: &str) -> NewRecord {
    NewRecord {
        key: Some(key.into()),
        ..rec(body)
    }
}

fn key(t: &Topic, svc: ServiceId, part: u32) -> CursorKey {
    CursorKey {
        topic: t.id,
        service: svc,
        group: "default".into(),
        partition: part,
    }
}

fn record(part: u32, off: u64, body: &str) -> StreamRecord {
    StreamRecord {
        partition: part,
        offset: off,
        key: None,
        headers: vec![],
        body: Bytes::from(body.to_string()),
        ts_ms: now_ms(),
    }
}

/// A topic name is unique per namespace; other envs, projects and tenants may reuse it.
#[tokio::test]
async fn ac_stm_01_create_topic_and_name_is_unique_per_namespace() {
    let s = store();
    let t = topic(&s, "orders", 2).await;
    assert!(t.id.to_string().starts_with("tpc_"));
    assert_eq!(t.partitions, 2);
    let e = s
        .create_topic(&ns(1, 1, "prod"), spec("orders", 1), 100)
        .await
        .unwrap_err();
    assert!(matches!(e, StoreError::Conflict("topic_exists")), "{e:?}");
    for n in [ns(1, 1, "dev"), ns(1, 2, "prod"), ns(2, 1, "prod")] {
        s.create_topic(&n, spec("orders", 1), 100)
            .await
            .expect("other namespace");
    }
    for bad in ["tpc_x", "Bad", "", "-a"] {
        let e = s.create_topic(&ns(1, 1, "prod"), spec(bad, 1), 100).await.unwrap_err();
        assert!(matches!(e, StoreError::Invalid(_)), "{bad}: {e:?}");
    }
    for p in [0, 17] {
        assert!(s.create_topic(&ns(1, 1, "prod"), spec("p", p), 100).await.is_err());
    }
    let mut zero = spec("z", 1);
    zero.retention_ms = 0;
    assert!(s.create_topic(&ns(1, 1, "prod"), zero, 100).await.is_err());
    let got = s.get_topic(&ns(1, 1, "prod"), "orders").await.unwrap().unwrap();
    assert_eq!(got, t);
    assert_eq!(s.topic_by_id(t.id).await.unwrap(), Some(t.clone()));
    assert!(s.get_topic(&ns(1, 1, "prod"), "nope").await.unwrap().is_none());
    assert_eq!(s.list_topics(t.ns.tenant, None, None).await.unwrap().len(), 3);
    assert_eq!(
        s.list_topics(t.ns.tenant, Some(t.ns.project), Some("prod"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(s.count_topics_in(t.ns.tenant, t.ns.project, None).await.unwrap(), 2);
    assert_eq!(
        s.count_topics_in(t.ns.tenant, t.ns.project, Some("dev")).await.unwrap(),
        1
    );
    let u = s.update_topic(t.id, Some(5), Some(9)).await.unwrap();
    assert_eq!((u.retention_ms, u.max_bytes), (5, 9));
    let u = s.update_topic(t.id, None, None).await.unwrap();
    assert_eq!((u.retention_ms, u.max_bytes), (5, 9));
    assert!(matches!(
        s.update_topic(t.id, Some(0), None).await,
        Err(StoreError::Invalid(_))
    ));
    assert!(matches!(
        s.update_topic(rt_types::TopicId::new(), None, None).await,
        Err(StoreError::NotFound)
    ));
}

/// `max_topics` counts the tenant's topics across projects and envs only.
#[tokio::test]
async fn ac_stm_02_topic_limit_counts_only_the_tenant() {
    let s = store();
    s.create_topic(&ns(1, 1, "a"), spec("x", 1), 2).await.unwrap();
    s.create_topic(&ns(1, 2, "b"), spec("y", 1), 2).await.unwrap();
    let e = s.create_topic(&ns(1, 3, "c"), spec("z", 1), 2).await.unwrap_err();
    assert!(matches!(e, StoreError::Conflict("topic_limit")), "{e:?}");
    s.create_topic(&ns(2, 1, "a"), spec("z", 1), 2)
        .await
        .expect("other tenant");
}

/// Offsets are dense per partition; a key always picks `partition_for(key)`.
#[tokio::test]
async fn ac_stm_03_append_assigns_dense_offsets_per_partition_and_key_picks_partition_for() {
    let s = store();
    let t = topic(&s, "k", 4).await;
    let out = s
        .append(t.id, vec![keyed("a", "1"), keyed("a", "2"), keyed("b", "3")], BIG)
        .await
        .unwrap();
    let pa = partition_for("a", 4);
    assert_eq!((out[0].partition, out[0].offset), (pa, 0));
    assert_eq!((out[1].partition, out[1].offset), (pa, 1));
    assert_eq!(out[2].partition, partition_for("b", 4));
    assert!(out.iter().all(|a| !a.duplicate));
    let heads = s.heads(t.id).await.unwrap();
    assert_eq!(heads.len(), 4);
    assert_eq!(heads[pa as usize].next, 2);
    let rs = s.read(t.id, pa, ReadFrom::Earliest, 10, 1 << 20).await.unwrap();
    assert_eq!(rs.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(rs[0].key.as_deref(), Some("a"));
    let t2 = s.topic_by_id(t.id).await.unwrap().unwrap();
    assert_eq!(t2.bytes, 6, "body 1 + key 1 per record");
}

/// Unkeyed appends spread round robin over the partitions.
#[tokio::test]
async fn ac_stm_04_append_without_key_round_robins() {
    let s = store();
    let t = topic(&s, "rr", 3).await;
    let out = s
        .append(t.id, (0..6).map(|n| rec(&n.to_string())).collect(), BIG)
        .await
        .unwrap();
    let parts: Vec<u32> = out.iter().map(|a| a.partition).collect();
    assert_eq!(parts, vec![0, 1, 2, 0, 1, 2]);
    assert!(s.heads(t.id).await.unwrap().iter().all(|h| h.next == 2));
}

/// One invalid record refuses the whole batch.
#[tokio::test]
async fn ac_stm_05_append_batch_is_all_or_nothing() {
    let s = store();
    let t = topic(&s, "b", 1).await;
    let mut bad = rec("x");
    bad.headers = vec![("x-rt-no".into(), "v".into())];
    assert!(matches!(
        s.append(t.id, vec![rec("ok"), bad], BIG).await,
        Err(StoreError::Invalid(_))
    ));
    let long_key = keyed(&"k".repeat(300), "x");
    assert!(s.append(t.id, vec![long_key], BIG).await.is_err());
    let mut dd = rec("x");
    dd.dedup_id = Some(String::new());
    assert!(s.append(t.id, vec![dd], BIG).await.is_err());
    assert!(s
        .append(t.id, vec![NewRecord::new(vec![0u8; 1024 * 1024 + 1])], BIG)
        .await
        .is_err());
    assert!(s.append(t.id, (0..101).map(|_| rec("x")).collect(), BIG).await.is_err());
    assert!(s
        .append(
            t.id,
            (0..5).map(|_| NewRecord::new(vec![0u8; 1024 * 1024])).collect(),
            BIG
        )
        .await
        .is_err());
    assert!(s.append(t.id, vec![], BIG).await.unwrap().is_empty());
    assert!(matches!(
        s.append(rt_types::TopicId::new(), vec![rec("x")], BIG).await,
        Err(StoreError::NotFound)
    ));
    assert_eq!(s.heads(t.id).await.unwrap()[0].next, 0);
    assert_eq!(s.topic_by_id(t.id).await.unwrap().unwrap().bytes, 0);
}

/// Same dedup id within the window returns the original; after expiry it appends again.
#[tokio::test]
async fn ac_stm_06_dedup_within_window_returns_original_offset_and_expires_after() {
    let s = store();
    let t = topic(&s, "d", 1).await;
    let mut r = rec("x");
    r.dedup_id = Some("id1".into());
    let a = s.append(t.id, vec![r.clone(), r.clone()], BIG).await.unwrap();
    assert_eq!((a[0].offset, a[0].duplicate), (0, false));
    assert_eq!((a[1].offset, a[1].duplicate), (0, true));
    let b = s.append(t.id, vec![r.clone()], BIG).await.unwrap();
    assert!(b[0].duplicate);
    let st = s.trim(now_ms() + 3 * 60_000).await.unwrap();
    assert_eq!(st.dedup, 1);
    let c = s.append(t.id, vec![r], BIG).await.unwrap();
    assert!(!c[0].duplicate);
}

/// The tenant cap refuses an append that would exceed it.
#[tokio::test]
async fn ac_stm_07_tenant_cap_refuses_with_stream_full() {
    let s = store();
    let t = topic(&s, "c", 1).await;
    s.append(t.id, vec![rec("12345")], 10).await.unwrap();
    let e = s.append(t.id, vec![rec("123456")], 10).await.unwrap_err();
    assert!(matches!(e, StoreError::Conflict("stream_full")), "{e:?}");
    assert_eq!(e.code(), "stream_full");
    s.append(t.id, vec![rec("12345")], 10)
        .await
        .expect("exactly at the cap");
}

/// Reads by offset, time, earliest and latest; limit and byte budget apply.
#[tokio::test]
async fn ac_stm_08_read_from_offset_time_earliest_latest_respects_limit_and_bytes() {
    let s = store();
    let t = topic(&s, "r", 2).await;
    let mut h = rec("aaaa");
    h.headers = vec![("h".into(), "v".into())];
    s.append(t.id, vec![h], BIG).await.unwrap();
    s.append(t.id, vec![rec("x"), rec("bbbb"), rec("y"), rec("cccc")], BIG)
        .await
        .unwrap();
    // partition 0 holds offsets 0..3: aaaa, bbbb, cccc
    let all = s.read(t.id, 0, ReadFrom::Earliest, 100, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].headers, vec![("h".to_string(), "v".to_string())]);
    assert_eq!(
        s.read(t.id, 0, ReadFrom::Offset(1), 100, 1 << 20).await.unwrap()[0].offset,
        1
    );
    assert_eq!(
        s.read(t.id, 0, ReadFrom::Offset(99), 100, 1 << 20).await.unwrap().len(),
        0
    );
    assert!(s
        .read(t.id, 0, ReadFrom::Latest, 100, 1 << 20)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(s.read(t.id, 0, ReadFrom::Earliest, 2, 1 << 20).await.unwrap().len(), 2);
    assert_eq!(s.read(t.id, 0, ReadFrom::Earliest, 100, 5).await.unwrap().len(), 1);
    assert_eq!(
        s.read(t.id, 0, ReadFrom::Earliest, 100, 1).await.unwrap().len(),
        1,
        "at least one"
    );
    assert_eq!(s.read(t.id, 0, ReadFrom::Time(0), 100, 1 << 20).await.unwrap().len(), 3);
    assert!(s
        .read(t.id, 0, ReadFrom::Time(now_ms() + 60_000), 100, 1 << 20)
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        s.read(t.id, 2, ReadFrom::Earliest, 1, 1).await,
        Err(StoreError::Invalid(_))
    ));
}

/// Timestamps within a partition never decrease.
#[tokio::test]
async fn ac_stm_09_timestamps_never_decrease_within_a_partition() {
    let s = store();
    let t = topic(&s, "ts", 1).await;
    for _ in 0..5 {
        s.append(t.id, vec![rec("a"), rec("b")], BIG).await.unwrap();
    }
    let rs = s.read(t.id, 0, ReadFrom::Earliest, 100, 1 << 20).await.unwrap();
    assert!(rs.windows(2).all(|w| w[0].ts_ms <= w[1].ts_ms));
}

/// A new cursor starts at the first (earliest) or next (latest) offset; later calls return it.
#[tokio::test]
async fn ac_stm_10_cursor_get_or_create_at_earliest_and_latest() {
    let s = store();
    let t = topic(&s, "cur", 1).await;
    s.append(t.id, vec![rec("a"), rec("b")], BIG).await.unwrap();
    let e = s.cursor(&key(&t, svc(1), 0), StartPosition::Earliest).await.unwrap();
    assert_eq!((e.committed, e.attempt, e.gen, e.blocked), (0, 0, 0, false));
    let l = s.cursor(&key(&t, svc(2), 0), StartPosition::Latest).await.unwrap();
    assert_eq!(l.committed, 2);
    let again = s.cursor(&key(&t, svc(1), 0), StartPosition::Latest).await.unwrap();
    assert_eq!(again.committed, 0, "existing cursor is returned unchanged");
    assert!(s.cursor(&key(&t, svc(1), 5), StartPosition::Latest).await.is_err());
    let mut bad = key(&t, svc(1), 0);
    bad.group = "Bad!".into();
    assert!(s.cursor(&bad, StartPosition::Latest).await.is_err());
    assert_eq!(s.cursors_of(svc(1)).await.unwrap().len(), 1);
    assert_eq!(s.cursors_on(t.id).await.unwrap().len(), 2);
}

/// fail at committed adds an attempt; fail at a later offset commits up to it with attempt 1.
#[tokio::test]
async fn ac_stm_11_commit_fail_defer_follow_the_attempt_rules() {
    let s = store();
    let t = topic(&s, "f", 1).await;
    let k = key(&t, svc(1), 0);
    s.cursor(&k, StartPosition::Earliest).await.unwrap();
    let d = Duration::from_secs(60);
    assert_eq!(s.fail(&k, 0, 0, d, "boom").await.unwrap(), Some(1));
    assert_eq!(s.fail(&k, 0, 0, d, "boom").await.unwrap(), Some(2));
    let c = s.cursor(&k, StartPosition::Earliest).await.unwrap();
    assert!(c.next_at_ms > now_ms());
    assert_eq!(c.last_error.as_deref(), Some("boom"));
    assert_eq!(s.fail(&k, 0, 3, d, "late").await.unwrap(), Some(1));
    assert_eq!(s.cursor(&k, StartPosition::Earliest).await.unwrap().committed, 3);
    assert!(s.commit(&k, 0, 5).await.unwrap());
    let c = s.cursor(&k, StartPosition::Earliest).await.unwrap();
    assert_eq!((c.committed, c.attempt, c.next_at_ms, c.last_error), (5, 0, 0, None));
    assert!(s.defer(&k, 0, d).await.unwrap());
    let c = s.cursor(&k, StartPosition::Earliest).await.unwrap();
    assert_eq!(c.attempt, 0);
    assert!(c.next_at_ms > now_ms());
}

/// After a reset, settles carrying the old gen change nothing.
#[tokio::test]
async fn ac_stm_12_gen_fences_settles_after_reset() {
    let s = store();
    let t = topic(&s, "g", 2).await;
    s.append(t.id, vec![rec("a"), rec("b"), rec("c")], BIG).await.unwrap();
    let k = key(&t, svc(1), 0);
    s.cursor(&k, StartPosition::Latest).await.unwrap();
    let cs = s
        .reset(t.id, svc(1), "default", None, ReadFrom::Earliest)
        .await
        .unwrap();
    assert_eq!(cs.len(), 2, "missing partition cursor created");
    assert_eq!(cs[0].gen, 1);
    assert_eq!(cs[0].committed, 0);
    assert!(!s.commit(&k, 0, 9).await.unwrap());
    assert_eq!(s.fail(&k, 0, 0, Duration::ZERO, "x").await.unwrap(), None);
    assert!(!s.defer(&k, 0, Duration::ZERO).await.unwrap());
    assert!(!s.block(&k, 0, "x").await.unwrap());
    assert!(!s
        .dead_letter(&k, 0, &record(0, 0, "a"), 1, "x", u64::MAX)
        .await
        .unwrap());
    assert!(s.commit(&k, 1, 1).await.unwrap());
    let one = s
        .reset(t.id, svc(1), "default", Some(0), ReadFrom::Offset(1))
        .await
        .unwrap();
    assert_eq!((one.len(), one[0].committed, one[0].gen), (1, 1, 2));
    let lat = s
        .reset(t.id, svc(1), "default", Some(0), ReadFrom::Latest)
        .await
        .unwrap();
    assert_eq!(lat[0].committed, 2);
    let tm = s
        .reset(t.id, svc(1), "default", Some(0), ReadFrom::Time(0))
        .await
        .unwrap();
    assert_eq!(tm[0].committed, 0);
    assert!(s
        .reset(t.id, svc(1), "default", Some(7), ReadFrom::Latest)
        .await
        .is_err());
    assert!(s.reset(t.id, svc(1), "BAD", None, ReadFrom::Latest).await.is_err());
}

/// dead_letter copies one record and moves the cursor past it.
#[tokio::test]
async fn ac_stm_13_dead_letter_moves_one_record_and_advances() {
    let s = store();
    let t = topic(&s, "dl", 1).await;
    let k = key(&t, svc(1), 0);
    s.cursor(&k, StartPosition::Earliest).await.unwrap();
    let mut r = record(0, 4, "poison");
    r.key = Some("k".into());
    r.headers = vec![("a".into(), "b".into())];
    assert!(s.dead_letter(&k, 0, &r, 3, "status_500", u64::MAX).await.unwrap());
    let c = s.cursor(&k, StartPosition::Earliest).await.unwrap();
    assert_eq!((c.committed, c.attempt), (5, 0));
    let dead = s.dead_list(t.id, svc(1), "default", 10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].record, r);
    assert_eq!((dead[0].attempts, dead[0].error.as_str()), (3, "status_500"));
    assert_eq!(s.dead_count(t.id, svc(1), "default").await.unwrap(), 1);
    assert_eq!(s.dead_purge(t.id, svc(1), "default").await.unwrap(), 1);
    assert_eq!(s.dead_count(t.id, svc(1), "default").await.unwrap(), 0);
    assert_eq!(s.topic_by_id(t.id).await.unwrap().unwrap().dead_bytes, 0);
}

/// A blocked cursor stays blocked until reset.
#[tokio::test]
async fn ac_stm_14_block_and_reset_unblocks() {
    let s = store();
    let t = topic(&s, "bl", 1).await;
    let k = key(&t, svc(1), 0);
    s.cursor(&k, StartPosition::Earliest).await.unwrap();
    assert!(s.block(&k, 0, "exhausted").await.unwrap());
    let c = s.cursor(&k, StartPosition::Earliest).await.unwrap();
    assert!(c.blocked);
    let r = s
        .reset(t.id, svc(1), "default", Some(0), ReadFrom::Earliest)
        .await
        .unwrap();
    assert!(!r[0].blocked);
    assert_eq!(r[0].last_error, None);
}

/// Trim by age drops old records, moves first_off and bytes, and lifts cursors behind it.
#[tokio::test]
async fn ac_stm_15_trim_by_age_moves_first_offset_and_bytes() {
    let s = store();
    let t = topic(&s, "age", 1).await;
    s.append(t.id, vec![rec("a"), rec("b")], BIG).await.unwrap();
    s.cursor(&key(&t, svc(1), 0), StartPosition::Earliest).await.unwrap();
    let st = s.trim(now_ms()).await.unwrap();
    assert_eq!(st.records, 0, "nothing is old yet");
    let st = s.trim(now_ms() + HOUR as i64 + 1000).await.unwrap();
    assert_eq!((st.records, st.cursors_moved), (2, 1));
    let h = s.heads(t.id).await.unwrap()[0];
    assert_eq!((h.first, h.next), (2, 2));
    assert_eq!(s.topic_by_id(t.id).await.unwrap().unwrap().bytes, 0);
    assert_eq!(
        s.cursor(&key(&t, svc(1), 0), StartPosition::Earliest)
            .await
            .unwrap()
            .committed,
        2
    );
    assert!(s.read(t.id, 0, ReadFrom::Offset(0), 10, 100).await.unwrap().is_empty());
}

/// Trim by max_bytes drops the oldest records first until the topic fits.
#[tokio::test]
async fn ac_stm_16_trim_by_max_bytes_drops_oldest_first() {
    let s = store();
    let mut sp = spec("size", 2);
    sp.max_bytes = 4;
    let t = s.create_topic(&ns(1, 1, "prod"), sp, 10).await.unwrap();
    for b in ["11", "22", "33", "44"] {
        s.append(t.id, vec![rec(b)], BIG).await.unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let st = s.trim(now_ms()).await.unwrap();
    assert_eq!(st.records, 2);
    let t2 = s.topic_by_id(t.id).await.unwrap().unwrap();
    assert_eq!(t2.bytes, 4);
    let mut left: Vec<String> = Vec::new();
    for p in 0..2 {
        for r in s.read(t.id, p, ReadFrom::Earliest, 10, 1 << 20).await.unwrap() {
            left.push(String::from_utf8(r.body.to_vec()).unwrap());
        }
    }
    left.sort();
    assert_eq!(left, vec!["33", "44"]);
}

/// Trim expires dedup entries and dead letters older than 7 days.
#[tokio::test]
async fn ac_stm_17_trim_expires_dedup_and_old_dead_letters() {
    let s = store();
    let t = topic(&s, "exp", 1).await;
    let mut r = rec("x");
    r.dedup_id = Some("d".into());
    s.append(t.id, vec![r], BIG).await.unwrap();
    let k = key(&t, svc(1), 0);
    s.cursor(&k, StartPosition::Earliest).await.unwrap();
    s.dead_letter(&k, 0, &record(0, 0, "x"), 1, "e", u64::MAX)
        .await
        .unwrap();
    let st = s.trim(now_ms() + 8 * 24 * HOUR as i64).await.unwrap();
    assert_eq!((st.dedup, st.dead), (1, 1));
    assert_eq!(s.dead_count(t.id, svc(1), "default").await.unwrap(), 0);
    assert_eq!(s.topic_by_id(t.id).await.unwrap().unwrap().dead_bytes, 0);
}

/// Deleting a topic, purging a tenant or a service removes their rows.
#[tokio::test]
async fn ac_stm_18_delete_topic_and_purge_tenant_and_service_remove_everything() {
    let s = store();
    let t = topic(&s, "del", 1).await;
    let t2 = topic(&s, "keep", 1).await;
    s.append(t.id, vec![rec("a")], BIG).await.unwrap();
    s.cursor(&key(&t, svc(1), 0), StartPosition::Earliest).await.unwrap();
    s.delete_topic(t.id).await.unwrap();
    assert!(s.topic_by_id(t.id).await.unwrap().is_none());
    assert!(s.cursors_of(svc(1)).await.unwrap().is_empty());
    assert!(matches!(s.delete_topic(t.id).await, Err(StoreError::NotFound)));
    assert!(matches!(s.heads(t.id).await, Err(StoreError::NotFound)));

    let k = key(&t2, svc(2), 0);
    s.cursor(&k, StartPosition::Earliest).await.unwrap();
    s.dead_letter(&k, 0, &record(0, 0, "x"), 1, "e", u64::MAX)
        .await
        .unwrap();
    assert_eq!(s.purge_service(svc(2)).await.unwrap(), 2);
    assert!(s.cursors_on(t2.id).await.unwrap().is_empty());
    assert_eq!(s.topic_by_id(t2.id).await.unwrap().unwrap().dead_bytes, 0);

    assert_eq!(s.purge_tenant(t2.ns.tenant).await.unwrap(), 1);
    assert!(s.list_topics(t2.ns.tenant, None, None).await.unwrap().is_empty());
}

/// A file-backed store keeps topics, records and cursors across a reopen.
#[tokio::test]
async fn ac_stm_19_reopen_keeps_topics_records_and_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sub/streams.db");
    let t = {
        let s = SqliteStreamStore::open(&path).unwrap();
        let t = topic(&s, "keep", 1).await;
        s.append(t.id, vec![rec("a")], BIG).await.unwrap();
        let k = key(&t, svc(1), 0);
        s.cursor(&k, StartPosition::Earliest).await.unwrap();
        s.commit(&k, 0, 1).await.unwrap();
        t
    };
    let s = SqliteStreamStore::open(&path).unwrap();
    assert_eq!(s.topic_by_id(t.id).await.unwrap().unwrap().name, "keep");
    assert_eq!(s.read(t.id, 0, ReadFrom::Earliest, 10, 100).await.unwrap().len(), 1);
    assert_eq!(s.cursors_of(svc(1)).await.unwrap()[0].committed, 1);
}

/// A database from a newer binary is refused.
#[tokio::test]
async fn ac_stm_20_db_newer_than_binary_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("streams.db");
    rusqlite::Connection::open(&path)
        .unwrap()
        .pragma_update(None, "user_version", 99)
        .unwrap();
    assert!(matches!(SqliteStreamStore::open(&path), Err(StoreError::Corrupt(_))));
}

/// An injected fault fails the append and leaves no rows.
#[tokio::test]
async fn ac_stm_21_faults_on_append_leave_no_partial_rows() {
    let s = store();
    let t = topic(&s, "fa", 1).await;
    let f = Faults::new();
    s.inject_faults(f.clone());
    f.fail_next(1, Fault::Io);
    assert!(s.append(t.id, vec![rec("a"), rec("b")], BIG).await.is_err());
    assert_eq!(s.heads(t.id).await.unwrap()[0].next, 0);
    assert_eq!(s.topic_by_id(t.id).await.unwrap().unwrap().bytes, 0);
}

/// Dead letters count toward the tenant cap.
#[tokio::test]
async fn ac_stm_22_dead_letters_count_toward_the_tenant_cap() {
    let s = store();
    let t = topic(&s, "cap", 1).await;
    let k = key(&t, svc(1), 0);
    s.cursor(&k, StartPosition::Earliest).await.unwrap();
    s.dead_letter(&k, 0, &record(0, 0, "12345678"), 1, "e", u64::MAX)
        .await
        .unwrap();
    assert_eq!(s.topic_by_id(t.id).await.unwrap().unwrap().dead_bytes, 8);
    let e = s.append(t.id, vec![rec("123")], 10).await.unwrap_err();
    assert!(matches!(e, StoreError::Conflict("stream_full")));
    s.append(t.id, vec![rec("12")], 10)
        .await
        .expect("fits with the dead letter");
}

/// A consumer keeps at most DEAD_MAX_PER_CONSUMER dead letters; the oldest go first.
#[tokio::test]
async fn ac_stm_23_dead_letters_beyond_1000_per_consumer_are_trimmed() {
    let s = store();
    let t = topic(&s, "many", 1).await;
    let k = key(&t, svc(1), 0);
    s.cursor(&k, StartPosition::Earliest).await.unwrap();
    for off in 0..DEAD_MAX_PER_CONSUMER + 5 {
        s.dead_letter(&k, 0, &record(0, off, "x"), 1, "e", u64::MAX)
            .await
            .unwrap();
    }
    assert_eq!(
        s.dead_count(t.id, svc(1), "default").await.unwrap(),
        DEAD_MAX_PER_CONSUMER
    );
    let first = &s.dead_list(t.id, svc(1), "default", 1).await.unwrap()[0];
    assert_eq!(first.record.offset, 5);
    assert_eq!(
        s.topic_by_id(t.id).await.unwrap().unwrap().dead_bytes,
        DEAD_MAX_PER_CONSUMER
    );
    let st = s.trim(now_ms()).await.unwrap();
    assert_eq!(st.dead, 0);
}

/// Appends/s for 1 and 100 records per call.
#[tokio::test]
#[ignore = "perf"]
async fn stm_perf_append_throughput() {
    let dir = tempfile::tempdir().unwrap();
    let s = SqliteStreamStore::open(&dir.path().join("p.db")).unwrap();
    let t = topic(&s, "perf", 4).await;
    for per in [1usize, 100] {
        let start = std::time::Instant::now();
        let calls = 2000 / per;
        for _ in 0..calls {
            s.append(t.id, (0..per).map(|_| rec("payload")).collect(), BIG)
                .await
                .unwrap();
        }
        let secs = start.elapsed().as_secs_f64();
        println!("{per} per call: {:.0} appends/s", (calls * per) as f64 / secs);
    }
}

/// Dead-letter copies cannot push the tenant past its stream cap: once records plus dead letters
/// would exceed it, the copy is kept without key, headers and body (the cursor still moves).
#[tokio::test]
async fn ac_stm_24_dead_letter_copies_respect_the_tenant_cap() {
    let s = store();
    let t = topic(&s, "amp", 1).await;
    let cap = 32;
    s.append(t.id, vec![rec("12345678"), rec("abcdefgh")], cap)
        .await
        .unwrap();
    let a = key(&t, svc(1), 0);
    let b = CursorKey {
        group: "b".into(),
        ..key(&t, svc(1), 0)
    };
    for k in [&a, &b] {
        s.cursor(k, StartPosition::Earliest).await.unwrap();
        let mut r0 = record(0, 0, "12345678");
        r0.key = Some("k".into());
        assert!(s.dead_letter(k, 0, &r0, 1, "e", cap).await.unwrap());
        assert!(s
            .dead_letter(k, 0, &record(0, 1, "abcdefgh"), 1, "e", cap)
            .await
            .unwrap());
        assert_eq!(s.cursor(k, StartPosition::Earliest).await.unwrap().committed, 2);
    }
    let tp = s.topic_by_id(t.id).await.unwrap().unwrap();
    assert!(tp.bytes + tp.dead_bytes <= cap, "{} + {}", tp.bytes, tp.dead_bytes);
    assert_eq!(tp.dead_bytes, 9, "only group a's first copy (key + body) fits");
    let dead_a = s.dead_list(t.id, svc(1), "default", 10).await.unwrap();
    let dead_b = s.dead_list(t.id, svc(1), "b", 10).await.unwrap();
    assert_eq!((dead_a.len(), dead_b.len()), (2, 2));
    assert!(dead_b
        .iter()
        .all(|d| d.record.body.is_empty() && d.record.key.is_none()));
    assert!(dead_b.iter().all(|d| d.error == "e" && d.attempts == 1));
}
