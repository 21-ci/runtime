//! Placement (`submit`): power-of-two-choices, GRQ fallback, LRQ overflow,
//! `runnext` for children, `wakep`, fairness against the GRQ - plus SCH-23 (throughput) and
//! SCH-25 (builds never disturb guest processors).
//!
//! ACs: SCH-01, SCH-02, SCH-23 (perf, ignored), SCH-24, SCH-25.

mod common;

use std::time::{Duration, Instant};

use common::*;
use rt_sched::trace::{PickSource, TraceEvent};
use rt_sched::SchedConfig;
use rt_testkit::{call, cpu, io, script, service_ref, Script, SimRuntime, SimTask};
use rt_types::{Outcome, ProcId, ServiceKnobs, TaskId, TaskKind, TaskSpec, Trigger};

// ---------------------------------------------------------------------------------------------
// SCH-01: LRQ overflow
// ---------------------------------------------------------------------------------------------

/// SCH-01: P0's LRQ is full (256); another task is placed on P0 -> 128 LRQ tasks + the new one
/// move to the GRQ; FIFO order within the moved batch is preserved.
#[test]
fn ac_sch_01_lrq_overflow_moves_half_plus_new_to_grq() {
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];

    let mut tasks: Vec<SimTask> = Vec::new();
    for _ in 0..256 {
        tasks.push(admitted(sim.submit_existing_to_proc(p0, d(2).finish())));
    }
    let snap = sim.snapshot();
    assert_eq!(snap.lrq(p0), 256, "P0's LRQ is full");
    assert_eq!(snap.grq_len(TaskKind::DENO), 0);

    let mut newcomer = admitted(sim.submit_existing_to_proc(p0, d(2).finish()));
    let snap = sim.snapshot();
    assert_eq!(snap.lrq(p0), 128, "128 tasks stay in the LRQ");
    assert_eq!(
        snap.grq_len(TaskKind::DENO),
        129,
        "128 LRQ tasks + the new one are on the GRQ"
    );
    let trace = sim.trace();
    assert_eq!(
        count_events(
            &trace,
            |e| matches!(e, TraceEvent::Overflow { proc, moved } if *proc == p0 && *moved == 129)
        ),
        1,
        "exactly one overflow event moving 129 tasks (128 + the new one)"
    );

    sim.run_until_idle();
    for t in tasks.iter_mut().chain(std::iter::once(&mut newcomer)) {
        assert_ok(sim.outcome(t));
    }

    // FIFO inside the moved batch: the 128 oldest tasks, then the newcomer; the 128 tasks that
    // stayed in the LRQ also keep their order.
    let order = sim.scripts().start_order();
    assert_eq!(order.len(), 257);
    let ids: Vec<TaskId> = tasks.iter().map(|t| t.id).collect();
    let mut moved: Vec<TaskId> = ids[..128].to_vec();
    moved.push(newcomer.id);
    assert!(
        is_strictly_increasing(&positions(&order, &moved)),
        "moved batch started out of FIFO order"
    );
    assert!(
        is_strictly_increasing(&positions(&order, &ids[128..])),
        "LRQ remainder started out of FIFO order"
    );
}

#[test]
fn placement_overflow_never_loses_a_task_with_a_tiny_lrq() {
    let mut sim = SimRuntime::new(fixed(1, 1).sched(|s| s.lrq_capacity = 4));
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let mut tasks: Vec<SimTask> = (0..23)
        .map(|_| admitted(sim.submit_existing_to_proc(p0, d(2).finish())))
        .collect();
    let snap = sim.snapshot();
    assert!(snap.lrq(p0) <= 4, "the LRQ never exceeds its capacity");
    assert_eq!(
        snap.lrq(p0) + snap.grq_len(TaskKind::DENO),
        23,
        "every task is on the LRQ or the GRQ"
    );
    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
    assert_eq!(sim.scripts().started(), 23);
}

// ---------------------------------------------------------------------------------------------
// SCH-02: fairness against the GRQ
// ---------------------------------------------------------------------------------------------

/// SCH-02: P0's LRQ is refilled continuously and the GRQ holds 1 task; 61 picks later the GRQ
/// task has started (the every-61st-pick GRQ check).
#[test]
fn ac_sch_02_grq_task_starts_within_61_picks() {
    let mut sim = SimRuntime::new(fixed(1, 1).auto_ready(false));
    let p0 = sim.procs_of(TaskKind::DENO)[0];

    // Filler tasks need no build; the GRQ task G reaches the GRQ through the WaitingRuntime wake.
    sim.source().make_ready(key_of(&d(2).finish()));
    let g_spec = d(9).finish();
    let g_key = key_of(&g_spec);
    let mut g = admitted(sim.submit(g_spec, Script::noop()));
    sim.run_until_idle();
    assert_eq!(sim.snapshot().waiting_runtime, 1, "G waits for its artifact");
    sim.source().complete(&g_key);
    sim.wake_runtime_waiters(&g_key);
    assert_eq!(sim.snapshot().grq_len(TaskKind::DENO), 1, "G is the only GRQ task");

    let top_up = |sim: &mut SimRuntime| {
        while sim.snapshot().lrq(p0) < 50 {
            admitted(sim.submit_existing_to_proc(p0, d(2).finish()));
        }
    };
    top_up(&mut sim);

    let mut guard = 0;
    loop {
        if sim.scripts().start_order().contains(&g.id) {
            break;
        }
        let tick = sim.snapshot().p(p0).schedtick;
        assert!(
            tick < 61,
            "61 picks happened on P0 and the GRQ task never started (schedtick={tick})"
        );
        top_up(&mut sim);
        sim.step_proc(p0);
        guard += 1;
        assert!(guard < 10_000, "the simulation made no progress");
    }

    let picks = picks_of(&sim.trace(), g.id);
    assert_eq!(picks.len(), 1);
    assert_eq!(picks[0].0, p0);
    assert_eq!(
        picks[0].2,
        PickSource::GrqFairness,
        "the LRQ was never empty: only the fairness check reaches the GRQ"
    );

    sim.run_until_idle();
    assert_ok(sim.outcome(&mut g));
}

// ---------------------------------------------------------------------------------------------
// Power of two choices, GRQ fallback, wakep
// ---------------------------------------------------------------------------------------------

#[test]
fn placement_external_submit_prefers_the_shorter_of_two_lrqs() {
    let mut sim = sim(4, 1);
    let ps = sim.procs_of(TaskKind::DENO);
    assert_eq!(ps.len(), 4);
    for _ in 0..50 {
        admitted(sim.submit_existing_to_proc(ps[0], d(2).finish()));
    }
    for _ in 0..30 {
        admitted(sim.submit_existing(d(3).finish()));
    }
    let snap = sim.snapshot();
    assert_eq!(snap.grq_len(TaskKind::DENO), 0, "no LRQ was full");
    let total: u32 = ps.iter().map(|p| snap.lrq(*p)).sum();
    assert_eq!(total, 80, "every task is queued on some LRQ");
    // P0 already holds 50: two random choices almost never both land on it, so it gets (almost)
    // none of the 30 new tasks while the three short LRQs share them.
    assert!(
        snap.lrq(ps[0]) - 50 <= 6,
        "the long LRQ received {} of 30 new tasks",
        snap.lrq(ps[0]) - 50
    );
    for p in &ps[1..] {
        assert!(snap.lrq(*p) >= 5, "{p:?} received only {} tasks", snap.lrq(*p));
    }
}

#[test]
fn placement_both_chosen_lrqs_full_goes_to_the_grq() {
    let mut sim = SimRuntime::new(fixed(2, 1).sched(|s| s.lrq_capacity = 4));
    let ps = sim.procs_of(TaskKind::DENO);
    for p in &ps {
        for _ in 0..4 {
            admitted(sim.submit_existing_to_proc(*p, d(2).finish()));
        }
    }
    let snap = sim.snapshot();
    assert_eq!((snap.lrq(ps[0]), snap.lrq(ps[1])), (4, 4));
    assert_eq!(snap.grq_len(TaskKind::DENO), 0);

    let t = admitted(sim.submit_existing(d(3).finish()));
    let snap = sim.snapshot();
    assert_eq!(
        (snap.lrq(ps[0]), snap.lrq(ps[1])),
        (4, 4),
        "both candidate LRQs were full: untouched"
    );
    assert_eq!(snap.grq_len(TaskKind::DENO), 1, "the task went to GRQ[deno]");
    assert_eq!(snap.grq_len(TaskKind::WASM), 0);
    assert!(
        count_events(
            &sim.trace(),
            |e| matches!(e, TraceEvent::Place { task, grq: true, .. } if *task == t.id)
        ) == 1
    );
}

#[test]
fn placement_push_wakes_one_idle_p_unless_a_spinner_exists() {
    let mut sim = SimRuntime::new(fixed(3, 1).sched(|s| s.spare_machines = 0));
    let all: Vec<ProcId> = sim.snapshot().procs.iter().map(|p| p.id).collect();
    for p in all {
        sim.step_proc(p); // nothing to do: every Machine parks
    }
    assert!(
        sim.snapshot().machines.iter().all(|m| m.parked),
        "every Machine is parked"
    );

    let before = sim.trace().len();
    let unparks = |sim: &SimRuntime| count_events(&sim.trace()[before..], |e| matches!(e, TraceEvent::Unpark { .. }));

    admitted(sim.submit_existing(d(2).finish()));
    assert_eq!(unparks(&sim), 1, "wakep(kind) after the push wakes exactly one Machine");
    admitted(sim.submit_existing(d(2).finish()));
    assert_eq!(
        unparks(&sim),
        1,
        "a spinning M of the kind already exists: no second wakep"
    );
    let awake = sim.snapshot().machines.iter().filter(|m| !m.parked).count();
    assert_eq!(awake, 1, "only one Machine is awake (and it belongs to the deno kind)");
}

// ---------------------------------------------------------------------------------------------
// SCH-24: children go to runnext
// ---------------------------------------------------------------------------------------------

/// SCH-24: task A on P0 calls service B of the same kind -> the child is in P0.runnext and
/// starts before the other tasks in P0's LRQ.
#[test]
fn ac_sch_24_child_of_running_task_goes_to_runnext_and_starts_first() {
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let log = install_call_hook(&sim, TaskKind::DENO);

    let a = admitted(sim.submit_to_proc(p0, d(1).finish(), script![call("b"), io(5)]));
    let xs: Vec<SimTask> = (0..5)
        .map(|_| admitted(sim.submit_existing_to_proc(p0, d(3).finish())))
        .collect();

    sim.step_proc(p0); // A starts, spawns its child, waits
    let child = {
        let spawned = log.lock();
        assert_eq!(spawned.len(), 1, "A called one service");
        assert_eq!(
            spawned[0].snap.p(p0).runnext,
            Some(spawned[0].child),
            "child is in P0.runnext right after the spawn"
        );
        spawned[0].child
    };
    assert_eq!(
        count_events(&sim.trace(), |e| matches!(
            e,
            TraceEvent::Place { task, proc: Some(p), grq: false, runnext: true } if *task == child && *p == p0
        )),
        1
    );

    sim.run_until_idle();
    let order = sim.scripts().start_order();
    assert_eq!(order[0], a.id);
    assert_eq!(order[1], child, "the child starts before any other P0 LRQ task");
    for x in &xs {
        assert!(positions(&order, &[child])[0] < positions(&order, &[x.id])[0]);
    }
}

/// The displaced `runnext` moves to the LRQ tail (Placement 2).
#[test]
fn ac_sch_24_second_child_displaces_the_first_to_the_lrq_tail() {
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let log = install_call_hook(&sim, TaskKind::DENO);

    let a = admitted(sim.submit_to_proc(p0, d(1).finish(), script![call("b"), call("c"), io(5)]));
    let xs: Vec<SimTask> = (0..3)
        .map(|_| admitted(sim.submit_existing_to_proc(p0, d(3).finish())))
        .collect();

    sim.step_proc(p0);
    let (c1, c2) = {
        let spawned = log.lock();
        assert_eq!(spawned.len(), 2);
        assert_eq!(
            spawned[1].snap.p(p0).runnext,
            Some(spawned[1].child),
            "the newest child holds runnext"
        );
        assert_eq!(spawned[1].snap.lrq(p0), 4, "3 queued tasks + the displaced first child");
        (spawned[0].child, spawned[1].child)
    };

    sim.run_until_idle();
    let mut expected = vec![a.id, c2];
    expected.extend(xs.iter().map(|x| x.id));
    expected.push(c1);
    assert_eq!(
        sim.scripts().start_order(),
        expected,
        "A, newest child, the queued tasks, then the displaced child"
    );
}

/// Only a child of the *same kind* uses `runnext`; a WASM child of a Deno parent is placed like
/// any external task.
#[test]
fn ac_sch_24_child_of_another_kind_is_placed_normally() {
    let mut sim = sim(1, 2);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let log = install_call_hook(&sim, TaskKind::WASM);

    admitted(sim.submit_to_proc(p0, d(1).finish(), script![call("w"), io(5)]));
    sim.step_proc(p0);

    let spawned = log.lock();
    assert_eq!(spawned.len(), 1);
    let snap = &spawned[0].snap;
    assert!(
        snap.procs.iter().all(|p| p.runnext.is_none()),
        "no runnext slot is used across kinds"
    );
    let wasm_queued: u32 = snap
        .procs
        .iter()
        .filter(|p| p.kind == TaskKind::WASM)
        .map(|p| p.lrq_len)
        .sum();
    assert_eq!(wasm_queued, 1, "the child is on a WASM P's LRQ");
}

// ---------------------------------------------------------------------------------------------
// SCH-25: builds run on build processors only
// ---------------------------------------------------------------------------------------------

/// SCH-25: steady WASM load at 70 % utilization; a 5 s compile starts -> WASM p99 rises <= 10 %,
/// and the build never runs on Deno/WASM Ps.
#[test]
fn ac_sch_25_compile_does_not_disturb_wasm_latency() {
    // 2 WASM Ps; one 7 ms task every 5 ms = 1.4 busy Ps of 2 = 70 %.
    let mut sim = SimRuntime::new(fixed(1, 2).sched(|s| s.build_processors = 1));
    let load_spec = || w(2).finish();
    let window = |sim: &mut SimRuntime, tasks: &mut Vec<SimTask>| {
        for _ in 0..400 {
            tasks.push(admitted(sim.submit_existing(load_spec())));
            sim.run_for(ms(5));
        }
    };
    let latencies = |sim: &mut SimRuntime, tasks: &mut Vec<SimTask>| -> Vec<Duration> {
        tasks
            .iter_mut()
            .map(|t| {
                assert_ok(sim.outcome(t));
                t.timings().total()
            })
            .collect()
    };

    // Baseline window (2 s of virtual time).
    let mut base = Vec::new();
    admitted(sim.submit(load_spec(), script![cpu(7)])); // sets the service script; also load
    window(&mut sim, &mut base);
    sim.run_for(ms(100));
    let p99_base = p99(latencies(&mut sim, &mut base));

    // A 5 s compile starts; same load for another 2 s.
    let build_spec = b(1000)
        .trigger(Trigger::Build {
            key: key_of(&load_spec()),
        })
        .finish();
    let mut build = admitted(sim.submit(build_spec, script![cpu(5000)]));
    let mut during = Vec::new();
    window(&mut sim, &mut during);
    assert!(
        sim.outcome(&mut build).is_none(),
        "the 5 s compile is still running while we measure"
    );
    sim.run_for(ms(100));
    let p99_during = p99(latencies(&mut sim, &mut during));

    assert!(
        p99_during.as_secs_f64() <= p99_base.as_secs_f64() * 1.10,
        "WASM p99 rose from {p99_base:?} to {p99_during:?} (> 10 %) during the compile"
    );

    // The build only ever ran on a Build P; no guest task ever ran on a Build P.
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut build));
    let snap = sim.snapshot();
    let trace = sim.trace();
    for e in &trace {
        if let TraceEvent::Pick { task, proc, .. } = e {
            let on_build_p = snap.p(*proc).kind == TaskKind::Build;
            assert_eq!(
                on_build_p,
                *task == build.id,
                "task {task:?} picked on {proc:?} ({:?})",
                snap.p(*proc).kind
            );
        }
    }
    assert!(!picks_of(&trace, build.id).is_empty());
}

// ---------------------------------------------------------------------------------------------
// SCH-23: throughput (real threads)
// ---------------------------------------------------------------------------------------------

/// SCH-23: MockExecutor no-op tasks, 4 Ps, real threads -> >= 1,000,000 tasks/s scheduled and
/// completed.
#[test]
#[ignore = "perf"]
fn ac_sch_23_schedules_at_least_one_million_noop_tasks_per_second() {
    const TOTAL: usize = 2_000_000;
    const PRODUCERS: usize = 2;
    const BATCH: usize = 5_000;

    let _serial = serial();
    let cfg = SchedConfig {
        max_inflight: u32::MAX,
        ..real_cfg(4, 0)
    };
    let real = build_real(cfg);
    real.book.set_record_events(false);
    let mut knobs = ServiceKnobs {
        max_instances: u32::MAX,
        max_queued: u32::MAX,
        ..ServiceKnobs::default()
    };
    // a fixed limit: this measures the scheduler, not admission waits
    knobs.scaling.start = u32::MAX;
    knobs.scaling.min = u32::MAX;
    let spec = move || TaskSpec::new(TaskKind::DENO, service_ref(1, 2), Trigger::Http).with_knobs(knobs);

    let started = Instant::now();
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|_| {
            let sched = real.sched.clone();
            std::thread::spawn(move || {
                block_on(async move {
                    for _ in 0..(TOTAL / PRODUCERS / BATCH) {
                        let handles: Vec<_> = (0..BATCH)
                            .map(|_| sched.submit(spec()).expect("task admitted"))
                            .collect();
                        for out in futures_util::future::join_all(handles).await {
                            assert!(matches!(out, Outcome::Ok(_)), "no-op task failed: {out:?}");
                        }
                    }
                });
            })
        })
        .collect();
    for p in producers {
        p.join().expect("producer thread");
    }
    let rate = TOTAL as f64 / started.elapsed().as_secs_f64();
    real.shutdown();
    assert!(rate >= 1_000_000.0, "scheduled and completed only {rate:.0} tasks/s");
}
