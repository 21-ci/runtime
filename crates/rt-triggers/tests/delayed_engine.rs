//! One-off schedules (`std:schedule`) delivered by the delayed engine on the
//! virtual clock: a due schedule runs once and is removed, a schedule is not delivered early, a
//! failed delivery is retried with the queue's backoff and dropped after `DELAYED_MAX_ATTEMPTS`,
//! and a service that cannot take the delivery keeps the schedule pending.
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::{drive, dyn_clock, new_sim};
use parking_lot::Mutex;
use rt_sched::Clock;
use rt_store::{
    DelayedEntry, DelayedItem, DelayedStore, NewDelayed, RetryOutcome, SqliteQueueStore, DELAYED_MAX_ATTEMPTS,
};
use rt_testkit::{ids, Script, ScriptBook, SimRuntime, SpecBuilder};
use rt_triggers::{DelayedEngine, DelayedEngineConfig, DelayedTaskFactory};
use rt_types::{DegradationMode, DelayedEvent, Payload, ServiceId, TaskSpec, Trigger};

const SVC: u128 = 1;

fn svc() -> ServiceId {
    ids::service(SVC)
}

#[derive(Clone, Debug)]
struct Delivery {
    id: String,
    attempt: u32,
    at_ms: i64,
    scheduled_at_ms: i64,
}

/// Builds the delivery task of each due schedule; the task succeeds unless `fail` is set.
struct Factory {
    book: Arc<ScriptBook>,
    clock: Arc<rt_testkit::VirtualClock>,
    fail: Mutex<bool>,
    available: Mutex<bool>,
    deliveries: Mutex<Vec<Delivery>>,
}

impl Factory {
    fn new(sim: &SimRuntime) -> Arc<Self> {
        Arc::new(Self {
            book: sim.scripts().clone(),
            clock: sim.clock().clone(),
            fail: Mutex::new(false),
            available: Mutex::new(true),
            deliveries: Mutex::new(Vec::new()),
        })
    }
}

impl DelayedTaskFactory for Factory {
    fn task_for(&self, service: ServiceId, item: &DelayedItem) -> Option<TaskSpec> {
        if !*self.available.lock() {
            return None;
        }
        self.deliveries.lock().push(Delivery {
            id: item.id.clone(),
            attempt: item.attempt,
            at_ms: self.clock.unix_ms(),
            scheduled_at_ms: item.run_at_ms,
        });
        let mut script = Script::new().io_ms(0);
        if *self.fail.lock() {
            script = script.then(rt_testkit::panic());
        }
        self.book.set_for_service(service, script);
        let service_n: u128 = service.ulid().into();
        let payload = serde_json::from_slice(&item.payload).unwrap_or(serde_json::Value::Null);
        Some(
            SpecBuilder::deno()
                .service(service_n)
                .trigger(Trigger::Scheduled { id: item.id.clone() })
                .wall_limit(5_000)
                .payload(Payload::Delayed(DelayedEvent {
                    id: item.id.clone(),
                    scheduled_at_ms: item.run_at_ms,
                    payload,
                }))
                .finish(),
        )
    }
}

struct Rig {
    sim: SimRuntime,
    store: Arc<SqliteQueueStore>,
    factory: Arc<Factory>,
    engine: Arc<DelayedEngine>,
}

fn rig() -> Rig {
    let sim = new_sim();
    let store = SqliteQueueStore::open_in_memory(5).expect("store");
    let factory = Factory::new(&sim);
    let engine = Arc::new(DelayedEngine::new(
        DelayedEngineConfig::default(),
        store.clone() as Arc<dyn DelayedStore>,
        sim.scheduler().clone(),
        dyn_clock(&sim),
        factory.clone(),
    ));
    Rig {
        sim,
        store,
        factory,
        engine,
    }
}

async fn put(r: &Rig, id: &str, delay_ms: i64) {
    let run_at_ms = r.sim.clock().unix_ms() + delay_ms;
    let payload = Bytes::from_static(b"{\"n\":1}");
    r.store
        .put(
            svc(),
            NewDelayed {
                id: id.into(),
                run_at_ms,
                payload,
            },
        )
        .await
        .expect("put")
        .expect("accepted");
}

fn spawn_engine(engine: &Arc<DelayedEngine>) -> tokio::task::JoinHandle<()> {
    let e = engine.clone();
    tokio::task::spawn_local(async move { e.run().await })
}

#[tokio::test(flavor = "current_thread")]
async fn a_due_schedule_is_delivered_once_with_its_event_and_removed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            put(&r, "reminder", 1_000).await;
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(10), Duration::from_millis(50)).await;
            run.abort();

            let d = r.factory.deliveries.lock().clone();
            assert_eq!(d.len(), 1, "delivered once: {d:?}");
            assert_eq!((d[0].id.as_str(), d[0].attempt), ("reminder", 1));
            assert!(d[0].at_ms >= d[0].scheduled_at_ms, "never early");
            assert!(
                d[0].at_ms - d[0].scheduled_at_ms < 2_000,
                "promptly after its time: {d:?}"
            );
            assert!(
                r.store.list(svc(), None, 10).await.expect("list").is_empty(),
                "acked: removed"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_schedule_is_not_delivered_before_its_time() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            put(&r, "later", 20_000).await;
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            run.abort();
            assert!(r.factory.deliveries.lock().is_empty(), "nothing before 20 s");
            assert_eq!(
                r.store.list(svc(), None, 10).await.expect("list").len(),
                1,
                "still pending"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_failing_delivery_is_retried_with_backoff_then_dropped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            *r.factory.fail.lock() = true;
            put(&r, "flaky", 0).await;
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(90), Duration::from_millis(100)).await;
            run.abort();

            let d = r.factory.deliveries.lock().clone();
            let attempts: Vec<u32> = d.iter().map(|x| x.attempt).collect();
            assert_eq!(
                attempts,
                (1..=DELAYED_MAX_ATTEMPTS).collect::<Vec<_>>(),
                "every attempt, no more"
            );
            let gap = |i: usize| (d[i + 1].at_ms - d[i].at_ms) as f64 / 1000.0;
            // 2 s, 4 s, 8 s, 16 s of backoff (+ up to 20 %).
            assert!((1.6..=2.6).contains(&gap(0)), "first backoff ≈ 2 s, got {}", gap(0));
            assert!(
                gap(1) > gap(0) && gap(2) > gap(1),
                "the backoff grows: {} {} {}",
                gap(0),
                gap(1),
                gap(2)
            );
            assert!(
                r.store.list(svc(), None, 10).await.expect("list").is_empty(),
                "dropped after the last attempt"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_service_that_cannot_take_the_delivery_keeps_the_schedule_pending() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            *r.factory.available.lock() = false;
            put(&r, "waiting", 0).await;
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            assert!(r.factory.deliveries.lock().is_empty());
            assert_eq!(
                r.store.list(svc(), None, 10).await.expect("list").len(),
                1,
                "kept, not dropped"
            );

            *r.factory.available.lock() = true;
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            run.abort();
            let d = r.factory.deliveries.lock().clone();
            assert_eq!(d.len(), 1, "delivered once the service can take it: {d:?}");
            assert_eq!(d[0].attempt, 1, "waiting cost no attempt");
        })
        .await;
}

/// The queue store with a switch for each engine call that fails with an internal error.
struct Flaky {
    inner: Arc<SqliteQueueStore>,
    fail_lease: AtomicBool,
    fail_release: AtomicBool,
    fail_done: AtomicBool,
    fail_retry: AtomicBool,
}

fn injected() -> rt_store::StoreError {
    rt_store::StoreError::Internal("injected store failure".into())
}

#[async_trait]
impl DelayedStore for Flaky {
    async fn put(&self, s: ServiceId, m: NewDelayed) -> rt_store::Result<Option<i64>> {
        self.inner.put(s, m).await
    }
    async fn cancel(&self, s: ServiceId, id: &str) -> rt_store::Result<bool> {
        self.inner.cancel(s, id).await
    }
    async fn list(&self, s: ServiceId, after: Option<&str>, limit: usize) -> rt_store::Result<Vec<DelayedEntry>> {
        self.inner.list(s, after, limit).await
    }
    async fn lease(&self, now_ms: i64, lease: Duration, max: usize) -> rt_store::Result<Vec<(ServiceId, DelayedItem)>> {
        if self.fail_lease.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.lease(now_ms, lease, max).await
    }
    async fn done(&self, s: ServiceId, id: &str, gen: i64) -> rt_store::Result<bool> {
        if self.fail_done.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.done(s, id, gen).await
    }
    async fn retry(&self, s: ServiceId, id: &str, gen: i64, run_at_ms: i64) -> rt_store::Result<RetryOutcome> {
        if self.fail_retry.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.retry(s, id, gen, run_at_ms).await
    }
    async fn release(&self, s: ServiceId, id: &str, gen: i64, run_at_ms: i64) -> rt_store::Result<bool> {
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.release(s, id, gen, run_at_ms).await
    }
}

/// A rig whose engine talks to a [`Flaky`] store.
fn flaky_rig() -> (Rig, Arc<Flaky>) {
    let sim = new_sim();
    let inner = SqliteQueueStore::open_in_memory(5).expect("store");
    let flaky = Arc::new(Flaky {
        inner: inner.clone(),
        fail_lease: AtomicBool::new(false),
        fail_release: AtomicBool::new(false),
        fail_done: AtomicBool::new(false),
        fail_retry: AtomicBool::new(false),
    });
    let factory = Factory::new(&sim);
    let engine = Arc::new(DelayedEngine::new(
        DelayedEngineConfig::default(),
        flaky.clone() as Arc<dyn DelayedStore>,
        sim.scheduler().clone(),
        dyn_clock(&sim),
        factory.clone(),
    ));
    (
        Rig {
            sim,
            store: inner,
            factory,
            engine,
        },
        flaky,
    )
}

/// A store that cannot lease delivers nothing and keeps the engine running: when it recovers,
/// the due schedule is delivered.
#[tokio::test(flavor = "current_thread")]
async fn a_store_that_cannot_lease_delivers_nothing_until_it_recovers() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut r, flaky) = flaky_rig();
            flaky.fail_lease.store(true, Ordering::SeqCst);
            put(&r, "due", 0).await;
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            assert!(
                r.factory.deliveries.lock().is_empty(),
                "nothing leased, nothing delivered"
            );
            flaky.fail_lease.store(false, Ordering::SeqCst);
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            run.abort();
            assert_eq!(
                r.factory.deliveries.lock().len(),
                1,
                "delivered after the store recovers"
            );
        })
        .await;
}

/// A delivery whose ack fails is delivered again once its lease runs out (at least once).
#[tokio::test(flavor = "current_thread")]
async fn a_delivery_that_cannot_be_acked_is_delivered_again() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut r, flaky) = flaky_rig();
            flaky.fail_done.store(true, Ordering::SeqCst);
            put(&r, "again", 0).await;
            let run = spawn_engine(&r.engine);
            // The lease is 600 s: the item comes back once it runs out.
            drive(&mut r.sim, Duration::from_secs(700), Duration::from_secs(1)).await;
            run.abort();
            let n = r.factory.deliveries.lock().iter().filter(|d| d.id == "again").count();
            assert!(n >= 2, "an unacked schedule comes back: {n} deliveries");
        })
        .await;
}

/// A failed delivery whose retry cannot be recorded is not lost: its lease ends and it is tried
/// again.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_delivery_whose_retry_cannot_be_recorded_is_tried_again() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut r, flaky) = flaky_rig();
            *r.factory.fail.lock() = true;
            flaky.fail_retry.store(true, Ordering::SeqCst);
            put(&r, "stuck", 0).await;
            let run = spawn_engine(&r.engine);
            // The lease is 600 s: the item comes back once it runs out.
            drive(&mut r.sim, Duration::from_secs(700), Duration::from_secs(1)).await;
            run.abort();
            let n = r.factory.deliveries.lock().iter().filter(|d| d.id == "stuck").count();
            assert!(n >= 2, "the schedule is delivered again: {n} deliveries");
        })
        .await;
}

/// A service that cannot take a delivery makes the engine release the schedule; when the release
/// cannot be recorded the lease keeps it until it ends, and the delivery then happens.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_release_leaves_the_schedule_for_its_lease_to_return() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut r, flaky) = flaky_rig();
            *r.factory.available.lock() = false;
            flaky.fail_release.store(true, Ordering::SeqCst);
            put(&r, "held", 0).await;
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            *r.factory.available.lock() = true;
            // The lease is 600 s: the item comes back once it runs out.
            drive(&mut r.sim, Duration::from_secs(700), Duration::from_secs(1)).await;
            run.abort();
            assert_eq!(
                r.factory.deliveries.lock().iter().filter(|d| d.id == "held").count(),
                1,
                "delivered once"
            );
        })
        .await;
}

/// A draining node refuses the schedule at submit: it is kept for a retry (not dropped), and it
/// is delivered and acknowledged once the node admits work again.
#[tokio::test(flavor = "current_thread")]
async fn a_schedule_the_scheduler_refuses_is_kept_and_delivered_once_admitted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            r.sim.scheduler().set_mode(DegradationMode::Drain);
            put(&r, "refused", 0).await;
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            assert_eq!(
                r.store.list(svc(), None, 10).await.expect("list").len(),
                1,
                "kept while the node drains"
            );
            r.sim.scheduler().set_mode(DegradationMode::Normal);
            drive(&mut r.sim, Duration::from_secs(30), Duration::from_millis(100)).await;
            run.abort();
            assert!(
                r.store.list(svc(), None, 10).await.expect("list").is_empty(),
                "acknowledged once delivered"
            );
            assert!(!r.factory.deliveries.lock().is_empty(), "the factory was asked");
        })
        .await;
}
