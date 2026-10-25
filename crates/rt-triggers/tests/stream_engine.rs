//! Stream consumer engine (plan-queues 2.2 / 6.4, plan-final 2.5): in-memory `SqliteStreamStore`,
//! `VirtualClock`, scripted `Submitter`.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rt_sched::{Clock, SimClock};
use rt_store::stream::{NewRecord, ReadFrom, SqliteStreamStore, StreamStore, Topic, TopicSpec};
use rt_testkit::VirtualClock;
use rt_triggers::stream_engine::{
    parse_fail_at, stream_backoff, ConsumerDecl, StreamEngine, StreamEngineConfig, StreamTaskFactory,
};
use rt_triggers::Submitter;
use rt_types::events::{Namespace, OnExhausted, StartPosition, StreamBatch, STREAM_FAIL_AT_HEADER};
use rt_types::{
    Digest, KillReason, Outcome, Payload, ProjectId, RejectReason, Reply, ServiceId, ServiceRef, TaskKind, TaskSpec,
    TenantId, Trigger, VersionNo,
};
use tokio::sync::watch;

// ---------------------------------------------------------------- helpers

fn ns() -> Namespace {
    Namespace {
        tenant: TenantId::from_u128(1),
        project: ProjectId::from_u128(1),
        environment: "prod".into(),
    }
}

fn svc(n: u128) -> ServiceId {
    ServiceId::from_u128(n)
}

fn decl(service: u128, topic: &str, group: &str) -> ConsumerDecl {
    ConsumerDecl {
        service: svc(service),
        ns: ns(),
        topic: topic.into(),
        group: group.into(),
        start: StartPosition::Earliest,
        batch_size: 100,
        max_attempts: 10,
        on_exhausted: OnExhausted::Dlq,
    }
}

struct Factory {
    consumers: Mutex<Vec<ConsumerDecl>>,
    cap: Mutex<u64>,
}

impl StreamTaskFactory for Factory {
    fn generation(&self) -> u64 {
        1
    }
    fn consumers(&self) -> Vec<ConsumerDecl> {
        self.consumers.lock().clone()
    }
    fn stream_cap(&self, _tenant: TenantId) -> u64 {
        *self.cap.lock()
    }
    fn task_for(&self, service: ServiceId, batch: StreamBatch) -> Option<TaskSpec> {
        let sref = ServiceRef {
            tenant: TenantId::from_u128(1),
            service,
            version: VersionNo(1),
            digest: Digest::of(b"code"),
        };
        let trig = Trigger::Stream {
            topic: Arc::from(batch.topic.as_str()),
            partition: batch.partition,
            attempt: batch.attempt,
        };
        Some(TaskSpec::new(TaskKind::DENO, sref, trig).with_payload(Payload::Stream(batch)))
    }
}

/// What the scripted scheduler does with a batch.
enum Ans {
    Done(Box<Outcome>),
    Refuse(RejectReason),
    /// Completes with `200` once the gate opens.
    Hold,
}

type Script = Box<dyn FnMut(&StreamBatch) -> Ans + Send>;

struct Delivery {
    service: ServiceId,
    batch: StreamBatch,
    at_ms: i64,
}

struct Sched {
    log: Mutex<Vec<Delivery>>,
    script: Mutex<Script>,
    gate: watch::Sender<bool>,
}

impl Sched {
    fn set(&self, f: impl FnMut(&StreamBatch) -> Ans + Send + 'static) {
        *self.script.lock() = Box::new(f);
    }
    fn open_gate(&self) {
        self.gate.send_replace(true);
    }
    fn count(&self) -> usize {
        self.log.lock().len()
    }
    fn offsets(&self) -> Vec<Vec<u64>> {
        self.log
            .lock()
            .iter()
            .map(|d| d.batch.records.iter().map(|r| r.offset).collect())
            .collect()
    }
    fn attempts(&self) -> Vec<u16> {
        self.log.lock().iter().map(|d| d.batch.attempt).collect()
    }
}

fn done(o: Outcome) -> Ans {
    Ans::Done(Box::new(o))
}

fn ok() -> Outcome {
    Outcome::Ok(Reply::ok())
}

fn fail_at(v: &str) -> Outcome {
    let mut r = Reply::ok();
    r.headers
        .insert(STREAM_FAIL_AT_HEADER, http::HeaderValue::from_str(v).unwrap());
    Outcome::Ok(r)
}

fn submitter(clock: VirtualClock) -> (Arc<Sched>, Submitter) {
    let (tx, _rx) = watch::channel(false);
    let s = Arc::new(Sched {
        log: Mutex::new(Vec::new()),
        script: Mutex::new(Box::new(|_| done(ok()))),
        gate: tx,
    });
    let s2 = s.clone();
    let sub: Submitter = Arc::new(move |spec: TaskSpec| {
        let Payload::Stream(batch) = &spec.payload else {
            panic!("not a stream payload");
        };
        let ans = (s2.script.lock())(batch);
        if let Ans::Refuse(r) = ans {
            return Err(r);
        }
        s2.log.lock().push(Delivery {
            service: spec.service.service,
            batch: batch.clone(),
            at_ms: clock.unix_ms(),
        });
        let mut gate = s2.gate.subscribe();
        Ok(Box::pin(async move {
            match ans {
                Ans::Done(o) => *o,
                Ans::Hold => {
                    let _ = gate.wait_for(|v| *v).await;
                    ok()
                }
                Ans::Refuse(_) => unreachable!(),
            }
        }))
    });
    (s, sub)
}

struct Rig {
    clock: VirtualClock,
    store: Arc<SqliteStreamStore>,
    engine: Arc<StreamEngine>,
    sched: Arc<Sched>,
    metrics: Arc<rt_obs::Metrics>,
    factory: Arc<Factory>,
}

fn cfg() -> StreamEngineConfig {
    StreamEngineConfig::default()
}

async fn topic(store: &SqliteStreamStore, name: &str, partitions: u32, max_bytes: u64) -> Topic {
    store
        .create_topic(
            &ns(),
            TopicSpec {
                name: name.into(),
                partitions,
                retention_ms: 24 * 3600 * 1000,
                max_bytes,
            },
            100,
        )
        .await
        .unwrap()
}

fn rig(cfg: StreamEngineConfig, store: Arc<SqliteStreamStore>, consumers: Vec<ConsumerDecl>) -> Rig {
    let clock = VirtualClock::new();
    let (sched, sub) = submitter(clock.clone());
    let metrics = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let factory = Arc::new(Factory {
        consumers: Mutex::new(consumers),
        cap: Mutex::new(u64::MAX),
    });
    let dyn_clock: Arc<dyn Clock> = Arc::new(clock.clone());
    let engine =
        Arc::new(StreamEngine::new(cfg, store.clone(), factory.clone(), sub, dyn_clock).with_metrics(metrics.clone()));
    Rig {
        clock,
        store,
        engine,
        sched,
        metrics,
        factory,
    }
}

impl Rig {
    fn start(&self) -> tokio::task::JoinHandle<()> {
        let e = self.engine.clone();
        tokio::spawn(async move { e.run().await })
    }

    async fn append(&self, t: &Topic, bodies: &[&str]) {
        for b in bodies {
            self.store
                .append(t.id, vec![NewRecord::new(b.to_string())], u64::MAX)
                .await
                .unwrap();
        }
        self.engine.notify(t.id);
    }

    /// Advance virtual time `n` times by `step_ms`, letting the engine and the store run.
    async fn step(&self, n: usize, step_ms: u64) {
        for _ in 0..n {
            self.clock.advance(Duration::from_millis(step_ms));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn until(&self, what: &str, cond: impl Fn(&Sched) -> bool) {
        for _ in 0..4000 {
            if cond(&self.sched) {
                return;
            }
            self.step(1, 50).await;
        }
        panic!("timed out waiting for {what}: offsets {:?}", self.sched.offsets());
    }

    async fn committed(&self, service: u128, partition: u32) -> u64 {
        self.try_cursor(service, partition).await.map_or(0, |c| c.committed)
    }

    async fn try_cursor(&self, service: u128, partition: u32) -> Option<rt_store::stream::Cursor> {
        self.store
            .cursors_of(svc(service))
            .await
            .unwrap()
            .into_iter()
            .find(|c| c.key.partition == partition)
    }

    async fn cursor(&self, service: u128, partition: u32) -> rt_store::stream::Cursor {
        self.store
            .cursors_of(svc(service))
            .await
            .unwrap()
            .into_iter()
            .find(|c| c.key.partition == partition)
            .expect("cursor")
    }

    async fn until_committed(&self, service: u128, partition: u32, want: u64) {
        for _ in 0..4000 {
            if self.committed(service, partition).await == want {
                return;
            }
            self.step(1, 50).await;
        }
        panic!(
            "cursor never reached {want}: {}; offsets {:?}",
            self.committed(service, partition).await,
            self.sched.offsets()
        );
    }
}

// ---------------------------------------------------------------- unit

/// `1 s × 2^(attempt-1)`, capped at 5 min.
#[test]
fn stream_backoff_doubles_from_one_second_and_caps_at_five_minutes() {
    assert_eq!(stream_backoff(1, 0.0), Duration::from_secs(1));
    assert_eq!(stream_backoff(2, 0.0), Duration::from_secs(2));
    assert_eq!(stream_backoff(4, 0.0), Duration::from_secs(8));
    assert_eq!(stream_backoff(10, 0.0), Duration::from_secs(300));
    assert_eq!(stream_backoff(u16::MAX, 0.0), Duration::from_secs(300));
    assert_eq!(stream_backoff(0, 0.0), Duration::from_secs(1), "attempt 0 behaves as 1");
}

/// Jitter is ±20 % and clamped.
#[test]
fn stream_backoff_jitter_is_clamped_to_twenty_percent() {
    assert_eq!(stream_backoff(2, 0.2), Duration::from_millis(2400));
    assert_eq!(stream_backoff(2, -0.2), Duration::from_millis(1600));
    assert_eq!(stream_backoff(2, 5.0), Duration::from_millis(2400));
    assert_eq!(stream_backoff(2, -5.0), Duration::from_millis(1600));
}

fn h(v: &[u8]) -> http::HeaderMap {
    let mut m = http::HeaderMap::new();
    m.insert(STREAM_FAIL_AT_HEADER, http::HeaderValue::from_bytes(v).unwrap());
    m
}

/// No header = commit everything; a delivered offset is accepted.
#[test]
fn parse_fail_at_accepts_absent_and_delivered_offsets() {
    assert_eq!(parse_fail_at(&http::HeaderMap::new(), 3, 7), Ok(None));
    assert_eq!(parse_fail_at(&h(b"3"), 3, 7), Ok(Some(3)));
    assert_eq!(parse_fail_at(&h(b" 7 "), 3, 7), Ok(Some(7)));
}

/// Garbage, negative, non-UTF-8 or out-of-batch offsets are malformed.
#[test]
fn parse_fail_at_rejects_malformed_and_out_of_range() {
    assert_eq!(parse_fail_at(&h(b"abc"), 3, 7), Err(()));
    assert_eq!(parse_fail_at(&h(b"-1"), 3, 7), Err(()));
    assert_eq!(parse_fail_at(&h(b"2"), 3, 7), Err(()));
    assert_eq!(parse_fail_at(&h(b"8"), 3, 7), Err(()));
    assert_eq!(parse_fail_at(&h(b"\xff"), 3, 7), Err(()));
}

#[test]
fn config_defaults_match_the_spec() {
    let c = StreamEngineConfig::default();
    assert_eq!(c.poll, Duration::from_millis(250));
    assert_eq!(c.head_refresh, Duration::from_secs(5));
    assert_eq!(c.trim_every, Duration::from_secs(60));
    assert_eq!(c.inflight_per_service, 8);
    assert_eq!(c.inflight_node, 256);
}

// ---------------------------------------------------------------- engine

/// Records are delivered in offset order as one batch and a 2xx commits through the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_01_delivers_in_order_and_commits_on_2xx() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let r = rig(cfg(), store, vec![decl(1, "orders", "default")]);
    r.append(&t, &["a", "b", "c", "d", "e"]).await;
    let run = r.start();
    r.until_committed(1, 0, 5).await;
    assert_eq!(r.sched.offsets(), vec![vec![0, 1, 2, 3, 4]]);
    {
        let log = r.sched.log.lock();
        let d = &log[0];
        assert_eq!(d.service, svc(1));
        assert_eq!(
            (d.batch.topic.as_str(), d.batch.group.as_str(), d.batch.attempt),
            ("orders", "default", 1)
        );
    }
    // The counter moves right after the commit returns.
    for _ in 0..200 {
        if r.metrics.stream_delivered_total.get() == 5 {
            break;
        }
        r.step(1, 50).await;
    }
    assert_eq!(r.metrics.stream_delivered_total.get(), 5);
    run.abort();
}

/// A cursor never has two batches in flight: new records wait for the previous settle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_02_one_inflight_batch_per_cursor() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let r = rig(cfg(), store, vec![decl(1, "orders", "default")]);
    r.sched.set(|_| Ans::Hold);
    let run = r.start();
    r.append(&t, &["a", "b", "c"]).await;
    r.until("first batch", |s| s.count() == 1).await;
    r.append(&t, &["d", "e"]).await;
    r.step(100, 50).await;
    assert_eq!(r.sched.count(), 1, "second batch must wait for the first settle");
    r.sched.open_gate();
    r.until_committed(1, 0, 5).await;
    assert_eq!(r.sched.offsets(), vec![vec![0, 1, 2], vec![3, 4]]);
    run.abort();
}

/// A valid fail-at commits the records before it and redelivers that record alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_03_fail_at_commits_before_and_redelivers_that_record_alone() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let r = rig(cfg(), store, vec![decl(1, "orders", "default")]);
    r.append(&t, &["a", "b", "c", "d", "e"]).await;
    let mut n = 0;
    r.sched.set(move |_| {
        n += 1;
        done(if n == 1 { fail_at("2") } else { ok() })
    });
    let run = r.start();
    r.until_committed(1, 0, 5).await;
    assert_eq!(r.sched.offsets(), vec![vec![0, 1, 2, 3, 4], vec![2], vec![3, 4]]);
    assert_eq!(r.sched.attempts(), vec![1, 2, 1]);
    run.abort();
}

/// A throw is one failed attempt for the first record: it is retried alone after
/// `1 s × 2^(attempt-1)` ±20 %, and a success returns to full batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_04_throw_retries_first_record_alone_with_backoff() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let r = rig(cfg(), store, vec![decl(1, "orders", "default")]);
    r.append(&t, &["a", "b", "c"]).await;
    let mut n = 0;
    r.sched.set(move |_| {
        n += 1;
        done(if n <= 2 { Outcome::GuestError } else { ok() })
    });
    let run = r.start();
    r.until_committed(1, 0, 3).await;
    assert_eq!(r.sched.offsets(), vec![vec![0, 1, 2], vec![0], vec![0], vec![1, 2]]);
    assert_eq!(r.sched.attempts(), vec![1, 2, 3, 1]);
    let times: Vec<i64> = r.sched.log.lock().iter().map(|d| d.at_ms).collect();
    let (g1, g2) = (times[1] - times[0], times[2] - times[1]);
    assert!((750..=1_350).contains(&g1), "first backoff ≈1 s, got {g1} ms");
    assert!((1_550..=2_550).contains(&g2), "second backoff ≈2 s, got {g2} ms");
    let c = r.cursor(1, 0).await;
    assert_eq!(c.attempt, 0);
    run.abort();
}

async fn exhaust(mode: OnExhausted) -> (Rig, Topic, tokio::task::JoinHandle<()>) {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let mut d = decl(1, "orders", "default");
    d.max_attempts = 2;
    d.on_exhausted = mode;
    let r = rig(cfg(), store, vec![d]);
    r.append(&t, &["poison", "fine"]).await;
    r.sched.set(|b| {
        if b.records[0].body.as_ref() == b"poison" {
            done(Outcome::GuestError)
        } else {
            done(ok())
        }
    });
    let run = r.start();
    (r, t, run)
}

/// After `max_attempts` deliveries of one record: `dlq` copies it to the dead letters and moves
/// on, `skip` moves on, `block` stops the cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_05_exhausted_dlq_skip_block() {
    // dlq
    let (r, t, run) = exhaust(OnExhausted::Dlq).await;
    r.until_committed(1, 0, 2).await;
    assert_eq!(r.sched.offsets(), vec![vec![0, 1], vec![0], vec![1]]);
    assert_eq!(r.store.dead_count(t.id, svc(1), "default").await.unwrap(), 1);
    let dead = r.store.dead_list(t.id, svc(1), "default", 10).await.unwrap();
    assert_eq!((dead[0].record.offset, dead[0].attempts), (0, 2));
    assert_eq!(dead[0].error, "guest_error");
    assert_eq!(r.metrics.stream_dlq_total.get(), 1);
    run.abort();

    // skip
    let (r, t, run) = exhaust(OnExhausted::Skip).await;
    r.until_committed(1, 0, 2).await;
    assert_eq!(r.sched.offsets(), vec![vec![0, 1], vec![0], vec![1]]);
    assert_eq!(r.store.dead_count(t.id, svc(1), "default").await.unwrap(), 0);
    assert_eq!(
        r.metrics.stream_skipped_total.with_label_values(&["exhausted"]).get(),
        1
    );
    run.abort();

    // block
    let (r, _t, run) = exhaust(OnExhausted::Block).await;
    r.until("two deliveries", |s| s.count() == 2).await;
    r.step(200, 100).await;
    assert_eq!(r.sched.count(), 2, "a blocked cursor gets no more deliveries");
    let c = r.cursor(1, 0).await;
    assert!(c.blocked);
    assert_eq!(c.committed, 0);
    run.abort();
}

/// Admission refusals, a shutdown kill and a service that cannot take the task are not
/// attempts: the record is never dead-lettered and keeps attempt 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_06_rejection_and_shutdown_are_not_attempts() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let mut d = decl(1, "orders", "default");
    d.max_attempts = 2;
    let r = rig(cfg(), store, vec![d]);
    r.append(&t, &["a"]).await;
    let mut n = 0;
    r.sched.set(move |_| {
        n += 1;
        match n {
            1..=4 => Ans::Refuse(RejectReason::Overloaded),
            5..=7 => done(Outcome::Killed(KillReason::Shutdown)),
            _ => done(ok()),
        }
    });
    let run = r.start();
    r.until_committed(1, 0, 1).await;
    // Refusals are not logged as deliveries; the three shutdown kills and the success are.
    assert_eq!(r.sched.attempts(), vec![1, 1, 1, 1]);
    assert_eq!(r.store.dead_count(t.id, svc(1), "default").await.unwrap(), 0);
    run.abort();
}

/// A malformed or out-of-batch fail-at fails the whole batch (one attempt for the first record).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_07_malformed_or_out_of_range_fail_at_is_a_whole_batch_failure() {
    for bad in ["oops", "99"] {
        let store = SqliteStreamStore::open_in_memory().unwrap();
        let t = topic(&store, "orders", 1, 0).await;
        let r = rig(cfg(), store, vec![decl(1, "orders", "default")]);
        r.append(&t, &["a", "b", "c"]).await;
        let mut n = 0;
        r.sched.set(move |_| {
            n += 1;
            done(if n == 1 { fail_at(bad) } else { ok() })
        });
        let run = r.start();
        r.until_committed(1, 0, 3).await;
        assert_eq!(
            r.sched.offsets(),
            vec![vec![0, 1, 2], vec![0], vec![1, 2]],
            "fail-at {bad}"
        );
        assert_eq!(r.sched.attempts(), vec![1, 2, 1]);
        run.abort();
    }
}

/// A batch holds at most `batch_size` records and at most 1 MiB of bodies (but always one).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_09_batch_respects_size_and_1mib() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "small", 1, 0).await;
    let big = topic(&store, "big", 1, 0).await;
    let mut a = decl(1, "small", "default");
    a.batch_size = 3;
    let r = rig(cfg(), store, vec![a, decl(2, "big", "default")]);
    r.append(&t, &["1", "2", "3", "4", "5", "6", "7"]).await;
    let body = "x".repeat(400 * 1024);
    r.append(&big, &[&body, &body, &body]).await;
    let run = r.start();
    r.until_committed(1, 0, 7).await;
    r.until_committed(2, 0, 3).await;
    let log = r.sched.log.lock();
    let small: Vec<usize> = log
        .iter()
        .filter(|d| d.service == svc(1))
        .map(|d| d.batch.records.len())
        .collect();
    let bigs: Vec<usize> = log
        .iter()
        .filter(|d| d.service == svc(2))
        .map(|d| d.batch.records.len())
        .collect();
    assert_eq!(small, vec![3, 3, 1]);
    assert_eq!(bigs, vec![2, 1], "3 × 400 KiB does not fit in 1 MiB");
    drop(log);
    run.abort();
}

/// A new `latest` cursor starts at the partition's next offset: older records are not delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_10_latest_start_skips_older_records() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let mut d = decl(1, "orders", "default");
    d.start = StartPosition::Latest;
    let r = rig(cfg(), store, vec![d]);
    r.append(&t, &["old1", "old2", "old3"]).await;
    let run = r.start();
    // Let the engine see the consumer (and create its cursor) before the new appends.
    for _ in 0..200 {
        if r.store.cursors_of(svc(1)).await.unwrap().len() == 1 {
            break;
        }
        r.step(1, 50).await;
    }
    r.append(&t, &["new1", "new2"]).await;
    r.until_committed(1, 0, 5).await;
    assert_eq!(r.sched.offsets(), vec![vec![3, 4]]);
    run.abort();
}

/// Records trimmed by retention under a lagging cursor are skipped: the cursor jumps to the first
/// retained offset and the skip is counted as `out_of_range`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_11_cursor_behind_retention_jumps_to_first_offset() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 2_000).await;
    let mut c = cfg();
    c.trim_every = Duration::from_secs(1);
    let r = rig(c, store, vec![decl(1, "orders", "default")]);
    r.sched.set(|_| Ans::Refuse(RejectReason::Overloaded));
    let run = r.start();
    let body = "y".repeat(500);
    let bodies: Vec<&str> = (0..10).map(|_| body.as_str()).collect();
    r.append(&t, &bodies).await;
    let mut first = 0;
    for _ in 0..400 {
        first = r.store.heads(t.id).await.unwrap()[0].first;
        if first > 0 {
            break;
        }
        r.step(1, 100).await;
    }
    assert!(first > 0, "size retention trimmed nothing");
    r.sched.set(|_| done(ok()));
    r.until_committed(1, 0, 10).await;
    let offsets = r.sched.offsets();
    assert_eq!(offsets[0][0], first, "delivery restarts at the first retained offset");
    assert!(
        r.metrics
            .stream_skipped_total
            .with_label_values(&["out_of_range"])
            .get()
            >= 1
    );
    assert!(r.metrics.stream_trimmed_total.get() >= first);
    run.abort();
}

/// An API reset bumps the cursor's gen: the in-flight batch's late commit is ignored and delivery
/// continues from the reset position once the engine is invalidated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_12_reset_invalidates_and_late_settle_is_ignored() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let r = rig(cfg(), store, vec![decl(1, "orders", "default")]);
    r.append(&t, &["a", "b", "c"]).await;
    r.sched.set(|_| Ans::Hold);
    let run = r.start();
    r.until("first batch", |s| s.count() == 1).await;
    r.store
        .reset(t.id, svc(1), "default", None, ReadFrom::Offset(1))
        .await
        .unwrap();
    r.engine.invalidate(svc(1));
    r.sched.open_gate();
    r.until("redelivery", |s| s.count() == 2).await;
    r.until_committed(1, 0, 3).await;
    assert_eq!(r.sched.offsets(), vec![vec![0, 1, 2], vec![1, 2]]);
    run.abort();
}

/// At most `inflight_per_service` batches per service and `inflight_node` per node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_13_inflight_caps_per_service_and_node() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 4, 0).await;
    let mut c = cfg();
    c.inflight_per_service = 2;
    c.inflight_node = 3;
    let r = rig(c, store, vec![decl(1, "orders", "a"), decl(2, "orders", "b")]);
    r.sched.set(|_| Ans::Hold);
    let run = r.start();
    // Unkeyed appends go round robin: one record per partition.
    r.append(&t, &["p0", "p1", "p2", "p3"]).await;
    r.until("node cap", |s| s.count() == 3).await;
    r.step(100, 50).await;
    let per = |s: u128| r.sched.log.lock().iter().filter(|d| d.service == svc(s)).count();
    assert_eq!(r.sched.count(), 3, "node cap");
    assert!(per(1) <= 2 && per(2) <= 2, "service cap: {} / {}", per(1), per(2));
    r.sched.open_gate();
    for p in 0..4 {
        r.until_committed(1, p, 1).await;
        r.until_committed(2, p, 1).await;
    }
    assert_eq!(r.sched.count(), 8);
    run.abort();
}

/// After `drain` nothing new is dispatched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_14_drain_stops_dispatching() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let r = rig(cfg(), store, vec![decl(1, "orders", "default")]);
    let run = r.start();
    r.append(&t, &["a"]).await;
    r.until_committed(1, 0, 1).await;
    r.engine.drain();
    r.append(&t, &["b", "c"]).await;
    r.step(200, 100).await;
    assert_eq!(r.sched.count(), 1);
    assert_eq!(r.committed(1, 0).await, 1);
    run.abort();
}

/// Retention runs every `trim_every`, not before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_15_trim_runs_on_its_interval() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 2_000).await;
    let r = rig(cfg(), store, vec![]);
    let run = r.start();
    let body = "z".repeat(500);
    let bodies: Vec<&str> = (0..10).map(|_| body.as_str()).collect();
    r.append(&t, &bodies).await;
    r.step(50, 1_000).await; // 50 s
    assert_eq!(r.store.heads(t.id).await.unwrap()[0].first, 0, "no trim before 60 s");
    assert_eq!(r.metrics.stream_trimmed_total.get(), 0);
    let mut first = 0;
    for _ in 0..40 {
        r.step(1, 1_000).await;
        first = r.store.heads(t.id).await.unwrap()[0].first;
        if first > 0 {
            break;
        }
    }
    assert!(first > 0, "trim ran after 60 s");
    assert_eq!(r.metrics.stream_trimmed_total.get(), first);
    let _ = &r.factory;
    run.abort();
}

/// A dead letter that would push the tenant past its stream cap keeps its offset, attempts and
/// error but no body; the cursor still moves on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_stm_eng_dlq_copy_over_the_tenant_cap_drops_the_body() {
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let t = topic(&store, "orders", 1, 0).await;
    let mut d = decl(1, "orders", "default");
    d.max_attempts = 1;
    let r = rig(cfg(), store, vec![d]);
    *r.factory.cap.lock() = 10; // exactly the two records
    r.append(&t, &["poison", "fine"]).await;
    r.sched.set(|b| {
        if b.records[0].body.as_ref() == b"poison" {
            done(Outcome::GuestError)
        } else {
            done(ok())
        }
    });
    let run = r.start();
    r.until_committed(1, 0, 2).await;
    let dead = r.store.dead_list(t.id, svc(1), "default", 10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!((dead[0].record.offset, dead[0].attempts), (0, 1));
    assert!(dead[0].record.body.is_empty());
    assert_eq!(r.store.topic_by_id(t.id).await.unwrap().unwrap().dead_bytes, 0);
    run.abort();
}
