//! WSM-05: instance reuse (`wasm.reuse_instances`, on only in `min_latency`).
//!
//! Reuse: after a **clean** completion the instance returns to a per-service, per-P idle
//! list. Dropped on trap/kill/timeout. Never crosses services.

use crate::common;

use std::time::Duration;

use common::{prepared, Req, Rig};
use rt_mem::MemoryGovernor as _;
use rt_types::KillReason;

async fn counts(rig: &mut Rig, p: &std::sync::Arc<rt_exec_wasm::WasmPrepared>, reqs: Vec<Req>) -> Vec<String> {
    let mut out = Vec::new();
    for r in reqs {
        out.push(rig.run(p, r).await.text());
    }
    out
}

#[tokio::test]
async fn ac_wsm_05_reuse_returns_1_2_3_on_the_same_p() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    let got = counts(
        &mut rig,
        &counter,
        (0..3).map(|_| Req::get("/").reuse().proc(0)).collect(),
    )
    .await;
    assert_eq!(got, vec!["1", "2", "3"]);
}

#[tokio::test]
async fn ac_wsm_05_reused_instance_never_crosses_services() {
    let mut rig = Rig::new();
    let counter = prepared("counter"); // the very same compiled component backs both services
    let a1 = rig.run(&counter, Req::get("/").reuse().service(1)).await.text();
    let a2 = rig.run(&counter, Req::get("/").reuse().service(1)).await.text();
    assert_eq!((a1.as_str(), a2.as_str()), ("1", "2"));

    // another service's component never sees service 1's instance
    let b1 = rig.run(&counter, Req::get("/").reuse().service(2)).await.text();
    assert_eq!(
        b1, "1",
        "service 2 got service 1's instance (state leaked across services)"
    );
    let b2 = rig.run(&counter, Req::get("/").reuse().service(2)).await.text();
    assert_eq!(b2, "2");

    // and service 1's idle instance is still its own
    let a3 = rig.run(&counter, Req::get("/").reuse().service(1)).await.text();
    assert_eq!(a3, "3");

    // another tenant with the same service number is yet another service
    let c1 = rig
        .run(&counter, Req::get("/").reuse().tenant(2).service(1))
        .await
        .text();
    assert_eq!(c1, "1", "tenant 2 got tenant 1's instance");
}

#[tokio::test]
async fn ac_wsm_05_counter_restarts_at_1_after_a_trap() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    let got = counts(&mut rig, &counter, (0..2).map(|_| Req::get("/").reuse()).collect()).await;
    assert_eq!(got, vec!["1", "2"]);

    // the guest traps on the third request: the instance must not return to the idle list
    rig.run(&counter, Req::get("/trap").reuse()).await.expect_guest_error();

    let after = rig.run(&counter, Req::get("/").reuse()).await.text();
    assert_eq!(after, "1", "an instance that trapped was reused");
    let next = rig.run(&counter, Req::get("/").reuse()).await.text();
    assert_eq!(next, "2");
}

#[tokio::test]
async fn ac_wsm_05_killed_instance_is_dropped_not_reused() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    let spin = prepared("spin");
    assert_eq!(rig.run(&counter, Req::get("/").reuse().service(5)).await.text(), "1");

    // kill a task of the same service mid-run (the spin guest has its own state; the point is
    // that a kill never leaves a half-run instance on service 5's idle list)
    let (task, fut) = rig.start(&spin, &Req::get("/").reuse().service(5));
    let killer = rig.kill_later(task, Duration::from_millis(100), KillReason::WallTimeout);
    let local = tokio::task::LocalSet::new();
    let outcome = local.run_until(fut).await;
    killer.join().expect("killer thread");
    assert!(
        matches!(outcome, rt_types::Outcome::Killed(KillReason::WallTimeout)),
        "{outcome:?}"
    );

    // the clean instance from before is still reusable
    assert_eq!(rig.run(&counter, Req::get("/").reuse().service(5)).await.text(), "2");
}

#[tokio::test]
async fn ac_wsm_05_idle_list_is_per_p() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    assert_eq!(rig.run(&counter, Req::get("/").reuse().proc(0)).await.text(), "1");
    assert_eq!(rig.run(&counter, Req::get("/").reuse().proc(0)).await.text(), "2");
    // P1 has no idle instance of this service yet
    assert_eq!(rig.run(&counter, Req::get("/").reuse().proc(1)).await.text(), "1");
    assert_eq!(rig.run(&counter, Req::get("/").reuse().proc(0)).await.text(), "3");
}

#[tokio::test]
async fn ac_wsm_05_reuse_is_off_unless_the_service_asks_for_it() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    let got = counts(&mut rig, &counter, (0..3).map(|_| Req::get("/")).collect()).await;
    assert_eq!(got, vec!["1", "1", "1"]);
    // a reusing service and a non-reusing one do not interfere
    assert_eq!(rig.run(&counter, Req::get("/").reuse().service(7)).await.text(), "1");
    assert_eq!(rig.run(&counter, Req::get("/").service(8)).await.text(), "1");
    assert_eq!(rig.run(&counter, Req::get("/").reuse().service(7)).await.text(), "2");
}

#[tokio::test]
async fn ac_wsm_05_idle_instances_count_against_the_governor_as_evictable_memory() {
    // Idle reusable instances keep their memory reserved under `WasmReuse`, and give it
    // back when the executor is told to shed idle state.
    let mut rig = Rig::new();
    let counter = prepared("counter");
    let baseline = rig.governor.committed();
    rig.run(&counter, Req::get("/").reuse()).await.text();
    let held = rig.governor.committed();
    assert!(
        held > baseline,
        "an idle reusable instance must stay accounted (baseline {baseline}, now {held})"
    );
    let reuse_bytes = rig
        .governor
        .stats()
        .by_category
        .get(&rt_mem::MemCategory::WasmReuse)
        .copied()
        .unwrap_or(0);
    assert!(
        reuse_bytes > 0,
        "idle instance memory must be charged to category wasm_reuse: {:?}",
        rig.governor.stats().by_category
    );

    rt_sched::Executor::on_idle(&mut rig.exec, Duration::from_millis(5));
    // `on_idle` is best-effort: dropping the executor must release everything.
    drop(rig.exec);
    assert_eq!(
        rig.governor.committed(),
        baseline,
        "idle instances leaked governor memory"
    );
}

// ---------------------------------------------------------------------------------------------
// WSM-13: the clocks of a reused instance
// ---------------------------------------------------------------------------------------------

fn unix_now_ns() -> i128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i128)
        .unwrap_or(0)
}

#[tokio::test]
async fn ac_wsm_13_reused_instance_wall_clock_tracks_real_time_and_monotonic_skips_idle_time() {
    let mut rig = Rig::new();
    let clock = prepared("clock");
    let first = rig.run(&clock, Req::get("/?iters=0").reuse()).await.reply();
    assert_eq!(rig.exec.idle_instances(), 1, "the instance is kept for the next task");

    // The instance sits idle between two tasks.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = unix_now_ns();
    let second = rig.run(&clock, Req::get("/?iters=0").reuse()).await.reply();
    let after = unix_now_ns();
    let num = |r: &common::Reply, k: &str| r.kv(k).parse::<i128>().unwrap_or_else(|e| panic!("{k}: {e}"));

    // wall: re-anchored when the task starts, so it reads real time (not 300 ms behind)
    let wall = num(&second, "start_wall_ns");
    let slack = 5_000_000; // 5 ms of clock granularity
    assert!(
        wall + slack >= before && wall <= after + slack,
        "wall {wall} not within [{before}, {after}]"
    );
    assert!(
        wall > num(&first, "start_wall_ns"),
        "the wall clock never moves backwards"
    );

    // monotonic: frozen over the idle gap
    let mono_gap = num(&second, "start_mono_ns") - num(&first, "start_mono_ns");
    assert!(
        (0..100_000_000).contains(&mono_gap),
        "idle time leaked into the monotonic clock: {mono_gap} ns"
    );
}

// ---------------------------------------------------------------------------------------------
// The idle key: version and environment
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_05_a_changed_environment_discards_the_idle_instance() {
    // wasi-libc caches the environment on first read: a rotated secret must not live on in a
    // reused instance.
    let mut rig = Rig::new();
    let counter = prepared("counter");
    assert_eq!(
        counts(&mut rig, &counter, (0..2).map(|_| Req::get("/").reuse()).collect()).await,
        vec!["1", "2"]
    );
    rig.control
        .env
        .lock()
        .secrets
        .push(("API_KEY".into(), "rotated".into()));
    assert_eq!(
        rig.run(&counter, Req::get("/").reuse()).await.text(),
        "1",
        "an instance built with the old secrets was reused"
    );
    assert_eq!(rig.run(&counter, Req::get("/").reuse()).await.text(), "2");
}

#[tokio::test]
async fn ac_wsm_05_reuse_never_crosses_versions() {
    let mut rig = Rig::new();
    let counter = prepared("counter"); // the same code deployed as version 1 and version 2
    assert_eq!(rig.run(&counter, Req::get("/").reuse().version(1)).await.text(), "1");
    assert_eq!(rig.run(&counter, Req::get("/").reuse().version(2)).await.text(), "1");
    assert_eq!(rig.run(&counter, Req::get("/").reuse().version(1)).await.text(), "2");
}

#[tokio::test]
async fn ac_wsm_05_with_no_idle_room_configured_nothing_is_parked() {
    let mut rig = Rig::new().reuse_max_idle(0);
    let counter = prepared("counter");
    let got = counts(&mut rig, &counter, (0..2).map(|_| Req::get("/").reuse()).collect()).await;
    assert_eq!(got, vec!["1", "1"]);
    assert_eq!(rig.exec.idle_instances(), 0);
}

#[tokio::test]
async fn ac_wsm_05_the_idle_list_of_a_service_holds_at_most_reuse_max_idle_instances() {
    let mut rig = Rig::new().reuse_max_idle(1);
    let stream = prepared("stream");
    // Two tasks overlap, so each starts a fresh instance; both finish clean, only one is kept.
    let req = || Req::get("/?chunks=2&delay_ms=50").reuse();
    let both = rig
        .run_all(vec![(stream.clone(), req()), (stream.clone(), req())])
        .await;
    assert_eq!(both.len(), 2);
    assert_eq!(rig.exec.idle_instances(), 1);
}
