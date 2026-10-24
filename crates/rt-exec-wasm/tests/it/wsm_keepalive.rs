//! WASM keep-alive after the answer (`scaling.min_run_time_ms`, `rt_sched::after_answer`):
//! - unset: a used instance follows the service's `cache.idle_ttl` (today's rule);
//! - `> 0`: a used instance stays that long, whatever `cache.idle_ttl` says;
//! - `0`: dropped right after the answer, unless tasks of the service wait in admission (then
//!   kept until none waits), or the key is below its warm floor (`min_instances`);
//! - a trapped or killed instance is never kept, whatever `min_run_time` says.

use crate::common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{prepared, Req, Rig};
use rt_obs::{Metrics, MetricsOptions};
use rt_sched::{Backlog, WarmKey, WarmTarget, WarmTargets};
use rt_types::{KillReason, Outcome, ServiceId, ServiceRef, TenantId, VersionNo};

const BUDGET: Duration = Duration::from_millis(50);

/// A fixed admission backlog for every service.
struct Waiting(AtomicU32);

impl Backlog for Waiting {
    fn waiting(&self, _service: ServiceId) -> u32 {
        self.0.load(Ordering::Acquire)
    }
}

/// Warm targets with a backlog of `n` waiting tasks (keep the returned `Arc` alive).
fn backlog(n: u32) -> (Arc<WarmTargets>, Arc<Waiting>) {
    let targets = WarmTargets::new();
    let waiting = Arc::new(Waiting(AtomicU32::new(n)));
    let dyn_backlog: Arc<dyn Backlog> = waiting.clone();
    targets.set_backlog(Arc::downgrade(&dyn_backlog));
    (targets, waiting)
}

fn keepalive(m: &Metrics, outcome: &str) -> u64 {
    m.instance_keepalive_total.with_label_values(&["wasm", outcome]).get()
}

/// Unset `min_run_time` inherits: the used instance stays until the list's `cache.idle_ttl`.
#[tokio::test]
async fn mrt_wsm_01_unset_keeps_the_list_idle_ttl() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    let req = || Req::get("/").reuse().service(101).idle_ttl(Duration::from_millis(80));
    assert_eq!(rig.run(&counter, req()).await.text(), "1");
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 1, "kept within the idle TTL");
    tokio::time::sleep(Duration::from_millis(150)).await;
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 0, "dropped after the idle TTL");
}

/// A set `min_run_time` is the used instance's keep-alive, longer or shorter than the idle TTL.
#[tokio::test]
async fn mrt_wsm_02_a_used_instance_expires_after_min_run_time() {
    let metrics = Metrics::new(MetricsOptions::default()).expect("metrics");
    let mut rig = Rig::new().metrics(metrics.clone());
    let counter = prepared("counter");
    // longer than the idle TTL: min_run_time wins
    let req = || {
        Req::get("/")
            .reuse()
            .service(102)
            .idle_ttl(Duration::from_millis(10))
            .min_run_time_ms(300)
    };
    assert_eq!(rig.run(&counter, req()).await.text(), "1");
    tokio::time::sleep(Duration::from_millis(60)).await;
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 1, "idle TTL passed, min_run_time not yet");
    assert_eq!(
        rig.run(&counter, req()).await.text(),
        "2",
        "the kept instance is reused"
    );
    assert_eq!(keepalive(&metrics, "reused"), 1);
    tokio::time::sleep(Duration::from_millis(400)).await;
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 0, "expired after min_run_time");
    assert_eq!(keepalive(&metrics, "kept"), 2);
    assert_eq!(keepalive(&metrics, "expired"), 1);

    // shorter than the idle TTL: it goes before the TTL
    let short = || {
        Req::get("/")
            .reuse()
            .service(103)
            .idle_ttl(Duration::from_secs(600))
            .min_run_time_ms(30)
    };
    assert_eq!(rig.run(&counter, short()).await.text(), "1");
    tokio::time::sleep(Duration::from_millis(80)).await;
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 0, "expired long before the idle TTL");
}

/// `min_run_time = 0` with nothing waiting: the instance is dropped right after the answer.
#[tokio::test]
async fn mrt_wsm_03_zero_drops_the_instance_after_the_answer() {
    let metrics = Metrics::new(MetricsOptions::default()).expect("metrics");
    let (targets, _waiting) = backlog(0);
    let mut rig = Rig::new().metrics(metrics.clone()).warm(targets);
    let counter = prepared("counter");
    let req = || Req::get("/").reuse().service(104).min_run_time_ms(0);
    assert_eq!(rig.run(&counter, req()).await.text(), "1");
    assert_eq!(rig.exec.idle_instances(), 0);
    assert_eq!(rig.run(&counter, req()).await.text(), "1", "a fresh instance");
    assert_eq!(keepalive(&metrics, "disposed"), 2);

    // without a registered scheduler nothing waits either
    let mut bare = Rig::new();
    assert_eq!(bare.run(&counter, req()).await.text(), "1");
    assert_eq!(bare.exec.idle_instances(), 0);
}

/// `min_run_time = 0` while tasks of the service wait: kept for them, dropped at the first
/// housekeeping that sees none waiting.
#[tokio::test]
async fn mrt_wsm_04_zero_with_waiting_work_stashes_until_the_queue_empties() {
    let metrics = Metrics::new(MetricsOptions::default()).expect("metrics");
    let (targets, waiting) = backlog(2);
    let mut rig = Rig::new().metrics(metrics.clone()).warm(targets);
    let counter = prepared("counter");
    let req = || Req::get("/").reuse().service(105).min_run_time_ms(0);
    assert_eq!(rig.run(&counter, req()).await.text(), "1");
    assert_eq!(rig.exec.idle_instances(), 1, "kept for the waiting work");
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 1, "work still waits");
    assert_eq!(rig.run(&counter, req()).await.text(), "2", "handed to the next task");
    assert_eq!(keepalive(&metrics, "handoff"), 2);
    waiting.0.store(0, Ordering::Release);
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 0, "dropped once the queue is empty");
    assert_eq!(keepalive(&metrics, "expired"), 1);
}

/// A buffered answer stashes the instance before the head is sent: when the outcome arrives the
/// instance is already idle, so a task granted then finds it.
#[tokio::test]
async fn mrt_wsm_05_a_buffered_answer_is_idle_before_its_head() {
    let (targets, _waiting) = backlog(1);
    let mut rig = Rig::new().warm(targets);
    let counter = prepared("counter");
    let req = Req::get("/").reuse().service(106).min_run_time_ms(0);
    let (_task, fut) = rig.start(&counter, &req);
    let local = tokio::task::LocalSet::new();
    let outcome = local.run_until(fut).await;
    assert!(matches!(outcome, Outcome::Ok(_)), "{outcome:?}");
    assert_eq!(rig.exec.idle_instances(), 1, "idle when the outcome arrived");
}

/// A guest trap or a kill never leaves the instance idle, however long `min_run_time` is.
#[tokio::test]
async fn mrt_wsm_06_a_trapped_or_killed_instance_is_never_stashed() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    let spin = prepared("spin");
    rig.run(&counter, Req::get("/trap").reuse().service(107).min_run_time_ms(60_000))
        .await
        .expect_guest_error();
    assert_eq!(rig.exec.idle_instances(), 0, "trapped instance kept");

    let req = Req::get("/").reuse().service(108).min_run_time_ms(60_000);
    let (task, fut) = rig.start(&spin, &req);
    let killer = rig.kill_later(task, Duration::from_millis(100), KillReason::WallTimeout);
    let local = tokio::task::LocalSet::new();
    let outcome = local.run_until(fut).await;
    killer.join().expect("killer thread");
    assert!(
        matches!(outcome, Outcome::Killed(KillReason::WallTimeout)),
        "{outcome:?}"
    );
    assert_eq!(rig.exec.idle_instances(), 0, "killed instance kept");
}

/// The warm floor beats `min_run_time = 0`: an instance the key's target still needs stays.
#[tokio::test]
async fn mrt_wsm_07_floor_instances_are_kept_at_zero() {
    let (targets, _waiting) = backlog(0);
    let counter = prepared("counter");
    let svc = ServiceRef {
        tenant: TenantId::from_u128(1),
        service: ServiceId::from_u128(109),
        version: VersionNo(1),
        digest: counter.key.digest,
    };
    targets.publish(HashMap::from([(
        WarmKey {
            service: svc.service,
            key: counter.key,
        },
        WarmTarget {
            service: svc,
            per_proc: 1,
        },
    )]));
    let mut rig = Rig::new().warm(targets);
    let req = || Req::get("/").reuse().service(109).min_run_time_ms(0);
    assert_eq!(rig.run(&counter, req()).await.text(), "1");
    assert_eq!(rig.exec.idle_instances(), 1, "kept for the floor");
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 1, "the floor instance never expires");
    assert_eq!(rig.run(&counter, req()).await.text(), "2");
}
