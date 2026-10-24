//! The Machine's idle hooks (`on_idle`, `between_tasks`): how much one call does, what a refused
//! reservation or an empty budget stops, and a hook that runs where there is no Machine.

use crate::common;

use std::time::Duration;

use common::*;
use rt_sched::Executor;

const HELLO: &str = "export default () => new Response('hi');";

#[test]
fn an_empty_budget_still_creates_one_pooled_isolate_per_call() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(3));
        assert_eq!(h.exec.pool_len(), 0);
        h.exec.on_idle(Duration::ZERO);
        assert_eq!(h.exec.pool_len(), 1, "an idle P converges even with a tiny budget");
        h.exec.on_idle(Duration::from_secs(5));
        assert_eq!(h.exec.pool_len(), 3, "a real budget fills the pool to its size");
    });
}

#[test]
fn a_governor_that_refuses_the_pooled_isolates_reservation_leaves_the_pool_empty() {
    run_local(async {
        // An isolate bigger than the whole budget: the reservation is refused (the node is not under pressure).
        let mut h = Harness::new(rt_exec_deno::DenoExecutorConfig {
            isolate_base_mb: 600,
            ..cfg_with_pool(2)
        });
        h.exec.on_idle(Duration::from_secs(1));
        assert_eq!(h.exec.pool_len(), 0);
        h.exec.between_tasks(Duration::from_secs(1));
        assert_eq!(h.exec.pool_len(), 0);
    });
}

#[test]
fn a_call_with_no_budget_left_disposes_one_finished_isolate_and_leaves_the_rest_for_later() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(2));
        let p = h.prepare(&ts_upload(HELLO)).await;
        for _ in 0..3 {
            let (got, _) = h.get("/", &p).await;
            assert_eq!(got.status, 200);
        }
        h.settle(Duration::from_millis(30)).await;
        let queued = h.exec.dispose_backlog_len();
        assert!(queued >= 2, "finished isolates wait for an idle moment: {queued}");
        h.exec.on_idle(Duration::ZERO);
        assert_eq!(
            h.exec.dispose_backlog_len(),
            queued - 1,
            "one unit, then the budget is spent"
        );
        assert_eq!(h.exec.pool_len(), 0, "nothing was refilled in that call");
        h.exec.on_idle(Duration::from_secs(5));
        assert_eq!(h.exec.dispose_backlog_len(), 0);
        assert_eq!(h.exec.pool_len(), 2);
    });
}

#[test]
fn between_tasks_disposes_before_it_refills_and_stops_when_its_budget_is_spent() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(2));
        let p = h.prepare(&ts_upload(HELLO)).await;
        for _ in 0..2 {
            h.get("/", &p).await;
        }
        h.settle(Duration::from_millis(30)).await;
        let queued = h.exec.dispose_backlog_len();
        h.exec.between_tasks(Duration::ZERO);
        assert!(
            h.exec.dispose_backlog_len() < queued || h.exec.pool_len() > 0,
            "one unit of work was done"
        );
    });
}

#[test]
fn a_warm_up_requested_where_there_is_no_local_set_is_dropped_with_a_warning() {
    // `warm_floor` publishes targets inside a `LocalSet`; here the hook runs on a bare runtime, as
    // it would while a Machine shuts down: starting the warm-up task must not take the node down.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let local = tokio::task::LocalSet::new();
    let (mut h, targets, p) = local.block_on(&rt, async {
        let targets = rt_sched::WarmTargets::new();
        let h = Harness::new(cfg0().with_warm_targets(targets.clone()));
        let p = h.prepare(&ts_upload(HELLO)).await;
        (h, targets, p)
    });
    let knobs = rt_types::ServiceKnobs {
        reuse_isolates: true,
        ..rt_types::ServiceKnobs::default()
    };
    local.block_on(&rt, async {
        // The target is published first; a task of the service then makes the Machine know it.
        let s = spec().knobs(knobs).http_get("/").finish();
        let service = s.service;
        targets.publish(std::collections::HashMap::from([(
            rt_sched::WarmKey {
                service: service.service,
                key: p.key,
            },
            rt_sched::WarmTarget { service, per_proc: 3 },
        )]));
        let (_task, fut) = h.start(s, &p);
        read(fut.await).await;
        h.settle(Duration::from_millis(20)).await;
    });
    // No LocalSet is entered for this call.
    rt.block_on(async { h.exec.between_tasks(Duration::from_millis(50)) });
    assert_eq!(
        h.exec.warm_len(),
        1,
        "the earlier task's isolate is untouched; the warm-up was not started"
    );
}

#[test]
fn a_task_future_dropped_before_its_head_outside_a_local_set_does_not_take_the_node_down() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let local = tokio::task::LocalSet::new();
    let (mut h, p) = local.block_on(&rt, async {
        let h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload("export default async () => { await new Promise((r) => setTimeout(r, 400)); return new Response('x'); };")).await;
        (h, p)
    });
    // Polled on a bare runtime and dropped while the handler still waits: the driver cannot be
    // handed to a local task, so it is dropped (the Machine's set is gone).
    rt.block_on(async {
        let (_task, fut) = h.start(get("/"), &p);
        assert!(
            tokio::time::timeout(Duration::from_millis(60), fut).await.is_err(),
            "no head yet"
        );
    });
}
