//! Queue engine: TRG-09..13.
//!
//! The engine runs against a `FakeQueueStore` on the `VirtualClock` (see `common`) so the
//! backoff / visibility timings of TRG-09..11 are asserted in virtual seconds. The store-level
//! contracts of TRG-10/12 are additionally checked against the real `SqliteQueueStore`.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{drive, dyn_clock, max_parallel, new_sim, AttemptFactory, FakeQueueStore};
use proptest::prelude::*;
use rt_sec::{LocalQuotaEnforcer, QuotaEnforcer, QuotaError, QuotaOp};
use rt_store::{NewMessage, QueueStore, RetryResult, SqliteQueueStore};
use rt_testkit::{ids, Script, SimRuntime};
use rt_triggers::{backoff_delay, QueueEngine, QueueEngineConfig, QueueTaskFactory, RETRY_BASE};
use rt_types::{DegradationMode, Plan, ServiceId};

const SVC: u128 = 1;

fn svc() -> ServiceId {
    ids::service(SVC)
}

struct Rig {
    sim: SimRuntime,
    store: Arc<FakeQueueStore>,
    factory: Arc<AttemptFactory>,
    engine: Arc<QueueEngine>,
}

fn rig(cfg: QueueEngineConfig, succeed_on: Option<u16>, wall_ms: u64) -> Rig {
    let sim = new_sim();
    let store = FakeQueueStore::new(sim.clock().clone(), cfg.max_attempts);
    let factory = AttemptFactory::new(&sim, SVC, succeed_on, wall_ms);
    let engine = Arc::new(QueueEngine::new(
        cfg,
        store.clone(),
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

fn spawn_engine(engine: &Arc<QueueEngine>) -> tokio::task::JoinHandle<()> {
    let e = engine.clone();
    tokio::task::spawn_local(async move { e.run().await })
}

fn secs(ms: i64) -> f64 {
    ms as f64 / 1000.0
}

// ---------------------------------------------------------------------------------------------
// Backoff (pure)
// ---------------------------------------------------------------------------------------------

#[test]
fn backoff_is_two_seconds_times_two_to_the_attempt() {
    assert_eq!(RETRY_BASE, Duration::from_secs(2));
    assert_eq!(backoff_delay(1, 0.0), Duration::from_secs(2));
    assert_eq!(backoff_delay(2, 0.0), Duration::from_secs(4));
    assert_eq!(backoff_delay(3, 0.0), Duration::from_secs(8));
    assert_eq!(backoff_delay(4, 0.0), Duration::from_secs(16));
    assert_eq!(backoff_delay(5, 0.0), Duration::from_secs(32));
}

#[test]
fn backoff_jitter_is_plus_minus_twenty_percent_and_clamped() {
    assert_eq!(backoff_delay(1, 0.2), Duration::from_millis(2_400));
    assert_eq!(backoff_delay(1, -0.2), Duration::from_millis(1_600));
    assert_eq!(backoff_delay(2, 0.2), Duration::from_millis(4_800));
    // Out-of-range jitter is clamped to ±20 %.
    assert_eq!(backoff_delay(1, 5.0), Duration::from_millis(2_400));
    assert_eq!(backoff_delay(1, -5.0), Duration::from_millis(1_600));
}

#[test]
fn backoff_never_overflows_for_huge_attempt_counts() {
    let d = backoff_delay(u16::MAX, 0.2);
    assert!(d >= Duration::from_secs(2));
    assert!(d < Duration::from_secs(10_000_000), "capped, got {d:?}");
}

proptest! {
    #[test]
    fn backoff_without_jitter_is_non_decreasing(a in 1u16..=30) {
        prop_assert!(backoff_delay(a + 1, 0.0) >= backoff_delay(a, 0.0));
    }

    #[test]
    fn backoff_stays_within_jitter_band(a in 1u16..=12, j in -0.2f64..=0.2) {
        let base = RETRY_BASE.as_secs_f64() * f64::from(1u32 << (a - 1));
        let d = backoff_delay(a, j).as_secs_f64();
        prop_assert!(d >= base * 0.8 - 1e-6 && d <= base * 1.2 + 1e-6, "{d} not within ±20% of {base}");
    }
}

#[test]
fn engine_config_defaults_match_the_spec() {
    let c = QueueEngineConfig::default();
    assert_eq!(c.batch_size, 10);
    assert_eq!(c.max_attempts, 5);
    assert_eq!(c.max_concurrency, 4);
}

// ---------------------------------------------------------------------------------------------
// TRG-09: handler fails twice then succeeds
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_09_fail_twice_then_succeed_with_exponential_backoff() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(QueueEngineConfig::default(), Some(3), 30_000);
            r.store.enqueue(svc(), NewMessage::new("job-1")).await.expect("enqueue");
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);

            drive(&mut r.sim, Duration::from_secs(40), Duration::from_millis(50)).await;
            run.abort();

            assert_eq!(
                r.factory.attempts(),
                vec![1, 2, 3],
                "attempts 1, 2, 3 and nothing after the success"
            );
            let t = r.factory.times_ms();
            let gap1 = secs(t[1] - t[0]);
            let gap2 = secs(t[2] - t[1]);
            // ≈ 2 s and ≈ 4 s, ±20 % (+ 0.15 s of simulation step slack).
            assert!(
                (1.6 - 0.15..=2.4 + 0.15).contains(&gap1),
                "first backoff ≈ 2 s ±20 %, got {gap1}"
            );
            assert!(
                (3.2 - 0.15..=4.8 + 0.15).contains(&gap2),
                "second backoff ≈ 4 s ±20 %, got {gap2}"
            );

            let stats = r.store.stats(svc()).await.expect("stats");
            assert_eq!(
                (stats.depth, stats.in_flight, stats.dlq),
                (0, 0, 0),
                "acked after attempt 3"
            );
            assert_eq!(r.store.acked().len(), 1);
            assert_eq!(r.store.stored(), 0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_batch_retries_every_message_of_the_batch() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Form A / WASM semantics: anything but 2xx retries *all* messages of the batch.
            let mut r = rig(QueueEngineConfig::default(), Some(2), 30_000);
            for i in 0..3 {
                r.store
                    .enqueue(svc(), NewMessage::new(format!("m{i}")))
                    .await
                    .expect("enqueue");
            }
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(10), Duration::from_millis(50)).await;
            run.abort();

            let d = r.factory.deliveries.lock().clone();
            assert_eq!(d.len(), 2, "one failing delivery, one successful redelivery: {d:?}");
            assert_eq!((d[0].attempt, d[0].batch_len), (1, 3));
            assert_eq!((d[1].attempt, d[1].batch_len), (2, 3));
            assert_eq!(r.store.retry_log().len(), 3, "all three messages were retried");
            assert_eq!(r.store.acked().len(), 3, "and acked after the successful redelivery");
        })
        .await;
}

// ---------------------------------------------------------------------------------------------
// TRG-10: DLQ after max attempts; redrive
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_10_always_failing_message_lands_in_dlq_after_five_attempts_and_redrive_restarts_at_attempt_1() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let cfg = QueueEngineConfig {
                max_attempts: 5,
                ..QueueEngineConfig::default()
            };
            let mut r = rig(cfg, None, 30_000);
            let id = r
                .store
                .enqueue(svc(), NewMessage::new("poison"))
                .await
                .expect("enqueue");
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);

            // 2 + 4 + 8 + 16 s of backoff (+ up to 20 %) between five attempts.
            drive(&mut r.sim, Duration::from_secs(60), Duration::from_millis(100)).await;

            assert_eq!(
                r.factory.attempts(),
                vec![1, 2, 3, 4, 5],
                "exactly max_attempts deliveries"
            );
            let stats = r.store.stats(svc()).await.expect("stats");
            assert_eq!(
                (stats.depth, stats.in_flight, stats.dlq),
                (0, 0, 1),
                "the message is in the DLQ"
            );
            let dlq = r.store.dlq_list(svc(), 10).await.expect("dlq");
            assert_eq!(dlq.len(), 1);
            assert_eq!(dlq[0].id, id);
            assert_eq!(dlq[0].attempts, 5);
            assert!(r
                .store
                .retry_log()
                .last()
                .is_some_and(|x| x.result == RetryResult::DeadLettered));

            // Redrive: back in the queue; the next delivery is attempt 1 again.
            *r.factory.succeed_on.lock() = Some(1);
            assert_eq!(r.store.dlq_redrive(svc()).await.expect("redrive"), 1);
            r.engine.notify(svc());
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            run.abort();

            let attempts = r.factory.attempts();
            assert_eq!(attempts.len(), 6, "one more delivery after the redrive: {attempts:?}");
            assert_eq!(attempts[5], 1, "redriven message restarts at attempt 1");
            let stats = r.store.stats(svc()).await.expect("stats");
            assert_eq!((stats.depth, stats.in_flight, stats.dlq), (0, 0, 0));
        })
        .await;
}

/// Same contract against the real store (no time travel needed: zero delays).
#[tokio::test]
async fn ac_trg_10_sqlite_store_dead_letters_at_max_attempts_and_redrives_with_attempt_1() {
    let store = SqliteQueueStore::open_in_memory(5).expect("open");
    let s = svc();
    let id = store.enqueue(s, NewMessage::new("poison")).await.expect("enqueue");

    for attempt in 1..=5u16 {
        let leased = store.lease(s, 10, Duration::from_secs(30)).await.expect("lease");
        assert_eq!(leased.len(), 1, "attempt {attempt}");
        assert_eq!(leased[0].id, id);
        assert_eq!(leased[0].attempt, attempt);
        let r = store.retry(id, Duration::ZERO).await.expect("retry");
        if attempt < 5 {
            assert_eq!(r, RetryResult::Requeued { attempt });
        } else {
            assert_eq!(r, RetryResult::DeadLettered);
        }
    }
    assert!(store
        .lease(s, 10, Duration::from_secs(30))
        .await
        .expect("lease")
        .is_empty());
    assert_eq!(store.stats(s).await.expect("stats").dlq, 1);
    assert_eq!(store.dlq_list(s, 10).await.expect("dlq")[0].id, id);

    assert_eq!(store.dlq_redrive(s).await.expect("redrive"), 1);
    let leased = store.lease(s, 10, Duration::from_secs(30)).await.expect("lease");
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].attempt, 1, "redrive resets the attempt counter");
}

// ---------------------------------------------------------------------------------------------
// TRG-11: crash during a queue task → redelivery after the visibility timeout
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_11_message_is_redelivered_after_visibility_timeout_when_the_node_dies_mid_task() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let wall_ms = 5_000u64;
            let visibility = Duration::from_millis(wall_ms) + Duration::from_secs(10);
            let mut r = rig(QueueEngineConfig::default(), Some(1), wall_ms);
            // The first handler hangs "forever" (the process is SIGKILLed while it runs).
            *r.factory.run_ms.lock() = 3_600_000;
            r.store
                .enqueue(svc(), NewMessage::new("survivor"))
                .await
                .expect("enqueue");
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(1), Duration::from_millis(50)).await;
            assert_eq!(r.factory.attempts(), vec![1], "delivered once, handler is running");
            let first_at = r.factory.times_ms()[0];

            // SIGKILL: the engine and everything it owned vanish; nothing is acked or retried.
            run.abort();
            drop(r.engine);

            let lease = r.store.lease_log();
            assert_eq!(lease.len(), 1);
            assert_eq!(
                lease[0].visibility, visibility,
                "visibility timeout = task wall limit + 10 s"
            );

            // Restart: a fresh engine on the same store. The handler of the new life succeeds quickly.
            *r.factory.run_ms.lock() = 0;
            let engine2 = Arc::new(QueueEngine::new(
                QueueEngineConfig::default(),
                r.store.clone(),
                r.sim.scheduler().clone(),
                dyn_clock(&r.sim),
                r.factory.clone(),
            ));
            let run2 = spawn_engine(&engine2);

            // Still invisible half way through the visibility window.
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(100)).await;
            assert_eq!(
                r.factory.attempts(),
                vec![1],
                "not redelivered while the lease is valid"
            );

            drive(&mut r.sim, Duration::from_secs(15), Duration::from_millis(100)).await;
            run2.abort();
            assert_eq!(
                r.factory.attempts(),
                vec![1, 2],
                "at-least-once: redelivered with attempt 2"
            );
            let second_at = r.factory.times_ms()[1];
            assert!(
                second_at - first_at >= visibility.as_millis() as i64,
                "redelivery at +{} ms, before the visibility timeout of {} ms",
                second_at - first_at,
                visibility.as_millis()
            );
            assert_eq!(r.store.stats(svc()).await.expect("stats").depth, 0);
            assert_eq!(r.store.acked().len(), 1);
        })
        .await;
}

/// Real store, real (short) visibility timeout.
#[tokio::test]
async fn ac_trg_11_sqlite_store_leased_message_reappears_after_visibility() {
    let store = SqliteQueueStore::open_in_memory(5).expect("open");
    let s = svc();
    store.enqueue(s, NewMessage::new("x")).await.expect("enqueue");
    let first = store.lease(s, 10, Duration::from_millis(250)).await.expect("lease");
    assert_eq!(first.len(), 1);
    assert!(
        store
            .lease(s, 10, Duration::from_millis(250))
            .await
            .expect("lease")
            .is_empty(),
        "invisible while leased"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    let again = store.lease(s, 10, Duration::from_secs(30)).await.expect("lease");
    assert_eq!(again.len(), 1, "at-least-once: visible again");
    assert_eq!(again[0].id, first[0].id);
    assert_eq!(again[0].attempt, 2);
}

// ---------------------------------------------------------------------------------------------
// TRG-12 / TRG-13: dedup, queue depth
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_trg_12_same_dedup_id_within_the_window_delivers_one_message() {
    let store = SqliteQueueStore::open_in_memory(5).expect("open");
    let s = svc();
    let mk = |body: &str, dedup: Option<&str>| NewMessage {
        body: body.to_string().into(),
        delay: Duration::ZERO,
        dedup_id: dedup.map(str::to_string),
    };
    let a = store.enqueue(s, mk("first", Some("job-1"))).await.expect("enqueue");
    let b = store.enqueue(s, mk("second", Some("job-1"))).await.expect("enqueue");
    store.enqueue(s, mk("other", Some("job-2"))).await.expect("enqueue");
    store.enqueue(s, mk("no-dedup", None)).await.expect("enqueue");
    store.enqueue(s, mk("no-dedup", None)).await.expect("enqueue");
    assert_eq!(a, b, "the duplicate enqueue returns the original message id");

    let leased = store.lease(s, 100, Duration::from_secs(30)).await.expect("lease");
    let bodies: Vec<String> = leased
        .iter()
        .map(|m| String::from_utf8_lossy(&m.body).into_owned())
        .collect();
    assert_eq!(bodies.iter().filter(|b| b.as_str() == "first").count(), 1);
    assert_eq!(
        bodies.iter().filter(|b| b.as_str() == "second").count(),
        0,
        "duplicate dropped: {bodies:?}"
    );
    assert_eq!(
        leased.len(),
        4,
        "job-1 once, job-2 once, two non-dedup messages: {bodies:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_12_dedup_holds_across_the_engine_path() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(QueueEngineConfig::default(), Some(1), 30_000);
            let nm = |b: &str| NewMessage {
                body: b.to_string().into(),
                delay: Duration::ZERO,
                dedup_id: Some("same".into()),
            };
            r.store.enqueue(svc(), nm("a")).await.expect("enqueue");
            r.store.enqueue(svc(), nm("b")).await.expect("enqueue");
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            run.abort();
            let total: usize = r.factory.deliveries.lock().iter().map(|d| d.batch_len).sum();
            assert_eq!(
                total, 1,
                "one message delivered for two enqueues with the same dedup_id"
            );
        })
        .await;
}

#[test]
fn ac_trg_13_queue_at_plan_depth_is_queue_full() {
    let plan = Plan {
        max_queue_depth: 100,
        ..Plan::default()
    };
    let q = LocalQuotaEnforcer::new(common::FixedPlans::new(plan));
    let t = ids::tenant(1);
    assert!(q.check(t, QuotaOp::Enqueue { depth: 99 }).is_ok());
    let err = q
        .check(t, QuotaOp::Enqueue { depth: 100 })
        .expect_err("depth at the plan maximum");
    assert_eq!(err, QuotaError::QueueFull);
    assert_eq!(err.http_status(), 429);
    assert_eq!(err.code(), "queue_full");
    assert_eq!(
        q.check(t, QuotaOp::Enqueue { depth: 5_000 }),
        Err(QuotaError::QueueFull)
    );
}

// ---------------------------------------------------------------------------------------------
// Delivery shape: batches, concurrency, visibility, delays
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn batches_hold_at_most_batch_size_messages_and_everything_is_delivered_once() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let cfg = QueueEngineConfig {
                batch_size: 10,
                max_concurrency: 4,
                ..QueueEngineConfig::default()
            };
            let mut r = rig(cfg, Some(1), 30_000);
            for i in 0..25 {
                r.store
                    .enqueue(svc(), NewMessage::new(format!("m{i}")))
                    .await
                    .expect("enqueue");
            }
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(10), Duration::from_millis(50)).await;
            run.abort();

            let d = r.factory.deliveries.lock().clone();
            assert!(
                d.iter().all(|x| x.batch_len <= 10),
                "batch size cap: {:?}",
                d.iter().map(|x| x.batch_len).collect::<Vec<_>>()
            );
            let total: usize = d.iter().map(|x| x.batch_len).sum();
            assert_eq!(total, 25, "every message delivered exactly once");
            assert!(d.len() >= 3, "25 messages need at least 3 batches of 10");
            assert_eq!(r.store.acked().len(), 25);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn per_service_queue_concurrency_is_capped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let cfg = QueueEngineConfig {
                batch_size: 1,
                max_concurrency: 2,
                ..QueueEngineConfig::default()
            };
            let mut r = rig(cfg, Some(1), 30_000);
            *r.factory.run_ms.lock() = 1_000;
            for i in 0..8 {
                r.store
                    .enqueue(svc(), NewMessage::new(format!("m{i}")))
                    .await
                    .expect("enqueue");
            }
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(15), Duration::from_millis(50)).await;
            run.abort();

            assert_eq!(r.factory.deliveries.lock().len(), 8);
            let peak = max_parallel(r.sim.scripts());
            assert!(peak <= 2, "queue.max_concurrency=2 but {peak} handlers ran at once");
            assert!(
                peak >= 2,
                "with 8 waiting messages the engine should use its concurrency"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn delayed_message_is_not_delivered_before_its_delay() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(QueueEngineConfig::default(), Some(1), 30_000);
            let t0 = r
                .store
                .enqueue(
                    svc(),
                    NewMessage {
                        body: "later".into(),
                        delay: Duration::from_secs(5),
                        dedup_id: None,
                    },
                )
                .await;
            t0.expect("enqueue");
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);

            drive(&mut r.sim, Duration::from_millis(4_500), Duration::from_millis(50)).await;
            assert!(r.factory.attempts().is_empty(), "not before delay_seconds");
            drive(&mut r.sim, Duration::from_secs(3), Duration::from_millis(50)).await;
            run.abort();
            assert_eq!(r.factory.attempts(), vec![1]);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lease_uses_task_wall_limit_plus_ten_seconds_as_visibility() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(QueueEngineConfig::default(), Some(1), 45_000);
            r.store.enqueue(svc(), NewMessage::new("v")).await.expect("enqueue");
            r.engine.notify(svc());
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(2), Duration::from_millis(50)).await;
            run.abort();
            let leases = r.store.lease_log();
            assert!(!leases.is_empty());
            assert_eq!(leases[0].visibility, Duration::from_secs(55), "wall 45 s + 10 s");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn messages_of_other_services_are_delivered_to_their_own_tasks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Two services share the engine; the factory is told which service a batch belongs to.
            let mut r = rig(QueueEngineConfig::default(), Some(1), 30_000);
            let other = ids::service(2);
            r.store.enqueue(svc(), NewMessage::new("mine")).await.expect("enqueue");
            r.store
                .enqueue(other, NewMessage::new("theirs"))
                .await
                .expect("enqueue");
            r.sim.scripts().set_for_service(other, Script::noop());
            r.engine.notify(svc());
            r.engine.notify(other);
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(3), Duration::from_millis(50)).await;
            run.abort();
            // Both services' messages were leased (separately: a batch never mixes services).
            let leases = r.store.lease_log();
            let services: std::collections::BTreeSet<String> = leases.iter().map(|l| l.service.to_string()).collect();
            assert!(services.contains(&svc().to_string()));
            assert!(services.contains(&other.to_string()));
            assert!(leases.iter().all(|l| l.ids.len() == 1), "a batch never mixes services");
        })
        .await;
}

/// The attempt factory, but the service can be "gone" (no task for a batch) for a while.
struct Gone {
    inner: Arc<AttemptFactory>,
    gone: AtomicBool,
}

impl QueueTaskFactory for Gone {
    fn wall_hint(&self, service: ServiceId) -> Option<Duration> {
        self.inner.wall_hint(service)
    }
    fn task_for(&self, service: ServiceId, batch: &[rt_store::Leased]) -> Option<rt_types::TaskSpec> {
        if self.gone.load(Ordering::SeqCst) {
            return None;
        }
        self.inner.task_for(service, batch)
    }
}

/// A batch the service cannot take right now is not dropped: it backs off and is run once the
/// service is back.
#[tokio::test(flavor = "current_thread")]
async fn a_batch_the_service_cannot_take_now_is_retried_not_lost() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut sim = new_sim();
            let cfg = QueueEngineConfig::default();
            let store = FakeQueueStore::new(sim.clock().clone(), cfg.max_attempts);
            let inner = AttemptFactory::new(&sim, SVC, Some(1), 1_000);
            let factory = Arc::new(Gone {
                inner: inner.clone(),
                gone: AtomicBool::new(true),
            });
            let engine = Arc::new(QueueEngine::new(
                cfg,
                store.clone(),
                sim.scheduler().clone(),
                dyn_clock(&sim),
                factory.clone(),
            ));
            store.enqueue(svc(), NewMessage::new("m")).await.expect("enqueue");
            engine.notify(svc());
            let run = spawn_engine(&engine);
            drive(&mut sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            assert!(inner.attempts().is_empty(), "nothing runs while the service is gone");
            factory.gone.store(false, Ordering::SeqCst);
            drive(&mut sim, Duration::from_secs(60), Duration::from_millis(100)).await;
            run.abort();
            assert_eq!(
                inner.deliveries.lock().len(),
                1,
                "the message is run once the service is back"
            );
        })
        .await;
}

/// A draining node refuses the batch at submit: the message is retried, and it runs once the node
/// admits work again.
#[tokio::test(flavor = "current_thread")]
async fn a_batch_the_scheduler_refuses_is_retried_and_runs_once_admitted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut sim = new_sim();
            let cfg = QueueEngineConfig::default();
            let store = FakeQueueStore::new(sim.clock().clone(), cfg.max_attempts);
            let factory = AttemptFactory::new(&sim, SVC, Some(1), 1_000);
            let engine = Arc::new(QueueEngine::new(
                cfg,
                store.clone(),
                sim.scheduler().clone(),
                dyn_clock(&sim),
                factory.clone(),
            ));
            sim.scheduler().set_mode(DegradationMode::Drain);
            store.enqueue(svc(), NewMessage::new("m")).await.expect("enqueue");
            engine.notify(svc());
            let run = spawn_engine(&engine);
            drive(&mut sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            let refused = factory.deliveries.lock().len();
            sim.scheduler().set_mode(DegradationMode::Normal);
            drive(&mut sim, Duration::from_secs(60), Duration::from_millis(100)).await;
            run.abort();
            assert!(
                factory.deliveries.lock().len() > refused,
                "the retried batch runs once admitted"
            );
        })
        .await;
}
