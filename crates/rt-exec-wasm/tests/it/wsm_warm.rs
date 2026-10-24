//! WSM-16: warm capacity (decisions D-A3/D-A4, `rt_sched::WarmTargets`). The `between_tasks` /
//! `on_idle` hooks converge on the autoscaler's per-P targets by pre-instantiating fresh
//! instances off the request path:
//! - with reuse off a warm instance serves exactly one task (it is fresh: WSM-04 still holds);
//! - with reuse on, idle reusable instances count toward the target;
//! - a key without a target keeps its instances until the service's idle TTL, then drops them;
//! - memory pressure `High` drops them at once; they never cross services or tenants;
//! - the work per hook call is bounded by its budget.

use crate::common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common::{prepared, Req, Rig, MIB};
use rt_exec_wasm::WasmPrepared;
use rt_mem::{MemCategory, MemoryGovernor as _, Owner};
use rt_sched::{WarmKey, WarmTarget, WarmTargets};
use rt_types::{ServiceId, ServiceRef, TenantId, VersionNo};

const BUDGET: Duration = Duration::from_millis(50);

fn target(p: &Arc<WasmPrepared>, tenant: u128, service: u128, per_proc: u32) -> (WarmKey, WarmTarget) {
    let svc = ServiceRef {
        tenant: TenantId::from_u128(tenant),
        service: ServiceId::from_u128(service),
        version: VersionNo(1),
        digest: p.key.digest,
    };
    (
        WarmKey {
            service: svc.service,
            key: p.key,
        },
        WarmTarget { service: svc, per_proc },
    )
}

/// Hook calls until `n` warm instances are held (pre-instantiation may yield across calls).
async fn converge(rig: &mut Rig, n: usize) {
    for _ in 0..50 {
        rig.between_tasks(BUDGET).await;
        if rig.exec.warm_instances() == n {
            return;
        }
    }
    assert_eq!(rig.exec.warm_instances(), n, "warm instances did not converge");
}

/// Warm tests run on an engine of their own: a failed pre-instantiation rests its key for a
/// second, so they must not depend on the free slots of the pool the rest of the binary shares.
fn private_engine() -> (wasmtime::Engine, rt_exec_wasm::EpochTicker) {
    common::small_engine(16)
}

fn warm_rig() -> (Rig, Arc<WarmTargets>, Holder) {
    let (engine, ticker) = private_engine();
    let targets = WarmTargets::new();
    let rig = Rig::on_engine(engine.clone(), 512, Duration::from_millis(25)).warm(targets.clone());
    let counter = common::prepared_unique(&engine, "counter");
    (
        rig,
        targets,
        Holder {
            counter,
            _ticker: ticker,
        },
    )
}

/// What a test keeps alive next to its rig: the fixture on the private engine and its ticker.
struct Holder {
    counter: Arc<WasmPrepared>,
    _ticker: rt_exec_wasm::EpochTicker,
}

#[tokio::test]
async fn ac_wsm_16_targets_are_met_with_fresh_instances_each_used_once() {
    let (mut rig, targets, held) = warm_rig();
    let counter = held.counter.clone();
    // A first task makes the artifact and this Machine's P known.
    assert_eq!(rig.run(&counter, Req::get("/")).await.text(), "1");
    targets.publish(HashMap::from([target(&counter, 1, 1, 2)]));
    converge(&mut rig, 2).await;
    let reserved = rig
        .governor
        .stats()
        .by_category
        .get(&MemCategory::WasmReuse)
        .copied()
        .unwrap_or(0);
    assert!(reserved > 0, "warm instances are charged to wasm_reuse (evictable)");

    // reuse off: a task takes a warm instance, which is fresh, and does not give it back
    assert_eq!(rig.run(&counter, Req::get("/")).await.text(), "1");
    assert_eq!(rig.exec.warm_instances(), 1, "the warm instance was consumed");
    assert_eq!(rig.run(&counter, Req::get("/")).await.text(), "1");
    assert_eq!(rig.exec.idle_instances(), 0);
    converge(&mut rig, 2).await;
}

#[tokio::test]
async fn ac_wsm_16_with_reuse_idle_instances_count_toward_the_target() {
    let (mut rig, targets, held) = warm_rig();
    let counter = held.counter.clone();
    assert_eq!(rig.run(&counter, Req::get("/").reuse()).await.text(), "1");
    targets.publish(HashMap::from([target(&counter, 1, 1, 1)]));
    for _ in 0..5 {
        rig.between_tasks(BUDGET).await;
    }
    assert_eq!(
        (rig.exec.idle_instances(), rig.exec.warm_instances()),
        (1, 0),
        "the reusable instance meets the target"
    );
    assert_eq!(rig.run(&counter, Req::get("/").reuse()).await.text(), "2");

    // a higher target is topped up with fresh instances; the reused one is still preferred
    targets.publish(HashMap::from([target(&counter, 1, 1, 3)]));
    converge(&mut rig, 2).await;
    assert_eq!(rig.run(&counter, Req::get("/").reuse()).await.text(), "3");
}

#[tokio::test]
async fn ac_wsm_16_without_a_target_warm_instances_go_after_the_idle_ttl() {
    let (mut rig, targets, held) = warm_rig();
    let counter = held.counter.clone();
    let baseline = rig.governor.committed();
    rig.run(&counter, Req::get("/")).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 1, 2)]));
    converge(&mut rig, 2).await;
    // a task of the service records its idle TTL on the list
    rig.run(&counter, Req::get("/").idle_ttl(Duration::from_millis(100)))
        .await
        .text();
    converge(&mut rig, 2).await;

    targets.publish(HashMap::new()); // scale to zero
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.warm_instances(), 2, "kept until the idle TTL passed");
    tokio::time::sleep(Duration::from_millis(150)).await;
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 0, "dropped after the idle TTL");
    assert_eq!(
        rig.governor.committed(),
        baseline,
        "their memory went back to the governor"
    );
}

#[tokio::test]
async fn ac_wsm_16_memory_pressure_drops_warm_instances() {
    let (engine, _ticker) = private_engine();
    let targets = WarmTargets::new();
    let mut rig = Rig::on_engine(engine.clone(), 256, Duration::from_millis(25)).warm(targets.clone());
    let counter = common::prepared_unique(&engine, "counter");
    rig.run(&counter, Req::get("/")).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 1, 2)]));
    converge(&mut rig, 2).await;

    // push the node past the high watermark (80 %)
    let hog = rig
        .governor
        .reserve(MemCategory::Other, 220 * MIB, Owner::NONE)
        .expect("reserve");
    rt_sched::Executor::on_idle(&mut rig.exec, BUDGET);
    assert_eq!(
        rig.exec.idle_instances(),
        0,
        "evictable warm instances are dropped under pressure"
    );
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 0, "and not rebuilt while the pressure lasts");
    drop(hog);
    converge(&mut rig, 2).await;
}

#[tokio::test]
async fn ac_wsm_16_warm_instances_never_cross_services_or_tenants() {
    let (mut rig, targets, held) = warm_rig();
    let counter = held.counter.clone(); // the same artifact backs every service below
    rig.run(&counter, Req::get("/").service(2)).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 2, 2)]));
    converge(&mut rig, 2).await;

    rig.run(&counter, Req::get("/").service(1)).await.text();
    rig.run(&counter, Req::get("/").tenant(2).service(2)).await.text();
    assert_eq!(
        rig.exec.warm_instances(),
        2,
        "another service or tenant took service 2's warm instance"
    );
    rig.run(&counter, Req::get("/").service(2)).await.text();
    assert_eq!(rig.exec.warm_instances(), 1);
}

#[tokio::test]
async fn ac_wsm_16_hook_work_is_bounded_by_its_budget() {
    let (mut rig, targets, held) = warm_rig();
    let counter = held.counter.clone();
    rig.run(&counter, Req::get("/")).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 1, 4)]));
    rig.between_tasks(Duration::ZERO).await;
    assert_eq!(rig.exec.warm_instances(), 0, "a zero budget does no work");
    converge(&mut rig, 4).await;
}

#[tokio::test]
async fn ac_wsm_16_no_targets_no_warm_instances() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    rig.run(&counter, Req::get("/")).await.text();
    rig.between_tasks(BUDGET).await;
    assert_eq!(rig.exec.idle_instances(), 0);
}

#[tokio::test]
async fn ac_wsm_16_two_tenants_with_identical_code_never_share_warm_instances() {
    let (mut rig, targets, held) = warm_rig();
    let counter = held.counter.clone(); // one PreparedKey for both tenants' services
    rig.run(&counter, Req::get("/").tenant(1).service(11)).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 11, 1), target(&counter, 2, 22, 1)]));
    converge(&mut rig, 2).await;
    // Tenant 2's task takes tenant 2's warm instance only; tenant 1's stays.
    rig.run(&counter, Req::get("/").tenant(2).service(22)).await.text();
    assert_eq!(rig.exec.warm_instances(), 1);
    rig.run(&counter, Req::get("/").tenant(2).service(22)).await.text();
    assert_eq!(rig.exec.warm_instances(), 1, "tenant 2 took tenant 1's warm instance");
    rig.run(&counter, Req::get("/").tenant(1).service(11)).await.text();
    assert_eq!(rig.exec.warm_instances(), 0);
}

#[tokio::test]
async fn ac_wsm_16_hook_calls_rotate_over_keys() {
    // One unit of work per call (a 1 ns budget): with two targets both keys get an instance within
    // two calls, instead of the first key taking every call.
    // On an engine nobody ticks no pre-instantiation ever yields, so a call is exactly one unit.
    let engine = common::untick_engine(8);
    let targets = WarmTargets::new();
    let mut rig = Rig::on_engine(engine.clone(), 512, Duration::from_millis(25)).warm(targets.clone());
    let counter = common::prepared_unique(&engine, "counter");
    rig.run(&counter, Req::get("/").service(31)).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 31, 2), target(&counter, 1, 32, 2)]));
    rig.between_tasks(Duration::from_nanos(1)).await;
    rig.between_tasks(Duration::from_nanos(1)).await;
    assert_eq!(rig.exec.warm_instances(), 2);
    let a = rig.run(&counter, Req::get("/").service(31)).await;
    let b = rig.run(&counter, Req::get("/").service(32)).await;
    a.reply();
    b.reply();
    assert_eq!(rig.exec.warm_instances(), 0, "each key had one warm instance");
}

#[tokio::test]
async fn ac_wsm_16_a_lowered_target_drops_the_fresh_instances_above_it() {
    let (mut rig, targets, held) = warm_rig();
    let counter = held.counter.clone();
    rig.run(&counter, Req::get("/")).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 1, 3)]));
    converge(&mut rig, 3).await;
    targets.publish(HashMap::from([target(&counter, 1, 1, 1)]));
    rig.between_tasks(BUDGET).await;
    assert_eq!(
        rig.exec.warm_instances(),
        1,
        "scaled down to the new target at the next hook call"
    );
}

#[tokio::test]
async fn ac_wsm_16_a_key_whose_preinstantiation_failed_is_not_retried_for_a_second() {
    // A pool of two slots: the third warm instance cannot be built.
    let engine = common::untick_engine(2);
    let targets = WarmTargets::new();
    let mut rig = Rig::on_engine(engine.clone(), 512, Duration::from_millis(25)).warm(targets.clone());
    let counter = common::prepared_unique(&engine, "counter");
    rig.run(&counter, Req::get("/")).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 1, 3)]));

    // Every attempt reads the service's environment: two that succeed and the one that fails, then
    // the key rests, however much of the call's budget is left.
    let before = rig.control.env_reads.load(Ordering::SeqCst);
    rig.between_tasks(BUDGET).await;
    assert_eq!(
        rig.control.env_reads.load(Ordering::SeqCst) - before,
        3,
        "the failed key was retried within the call"
    );
    assert_eq!(rig.exec.warm_instances(), 2, "the pool held two");

    // A task takes one and its slot frees at its end; the key is still resting.
    rig.run(&counter, Req::get("/")).await.text();
    let before = rig.control.env_reads.load(Ordering::SeqCst);
    rig.between_tasks(BUDGET).await;
    assert_eq!(
        rig.control.env_reads.load(Ordering::SeqCst),
        before,
        "no attempt inside the backoff"
    );
    assert_eq!(rig.exec.warm_instances(), 1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    converge(&mut rig, 2).await;
}

#[tokio::test]
async fn ac_wsm_16_pre_instantiations_that_yield_are_finished_by_later_hook_calls() {
    let (engine, _ticker) = common::small_engine(8);
    let hot = {
        // A hot epoch: every pre-instantiation is asked to yield again and again.
        let (stop, engine) = (Arc::new(std::sync::atomic::AtomicBool::new(false)), engine.clone());
        let flag = stop.clone();
        let thread = std::thread::spawn(move || {
            while !flag.load(std::sync::atomic::Ordering::Relaxed) {
                engine.increment_epoch();
                std::thread::sleep(Duration::from_micros(20));
            }
        });
        (stop, thread)
    };
    let targets = WarmTargets::new();
    let mut rig = Rig::on_engine(engine.clone(), 512, Duration::from_millis(25)).warm(targets.clone());
    let counter = common::prepared_unique(&engine, "counter");
    rig.run(&counter, Req::get("/")).await.text();
    targets.publish(HashMap::from([target(&counter, 1, 1, 4)]));
    rig.between_tasks(BUDGET).await; // starts several jobs, most of them still pending
    rig.between_tasks(Duration::from_nanos(1)).await; // finishes one and is out of budget for the rest
    converge(&mut rig, 4).await;
    hot.0.store(true, std::sync::atomic::Ordering::Relaxed);
    hot.1.join().expect("ticker thread");
    assert_eq!(
        rig.run(&counter, Req::get("/")).await.text(),
        "1",
        "a finished warm instance serves a task"
    );
}
