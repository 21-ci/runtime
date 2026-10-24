//! Work stealing (6): same-kind unbound tasks only,
//! half of a victim's LRQ, never `runnext`, bounded spinning.
//!
//! ACs: SCH-03, SCH-04, SCH-08 (simulated and real threads).

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::*;
use proptest::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rt_sched::steal::{is_stealable, max_spinning, victim_order, STEAL_PASSES};
use rt_sched::trace::TraceEvent;
use rt_sched::StepResult;
use rt_testkit::{call, io, script, Script, SimConfig, SimRuntime, SimTask, Step};
use rt_types::{ProcStatus, TaskKind, TaskSpec};

fn spec_of(kind: TaskKind, service: u128) -> TaskSpec {
    match kind {
        TaskKind::Build => b(service).finish(),
        k if k == TaskKind::DENO => d(service).finish(),
        _ => w(service).finish(),
    }
}

// ---------------------------------------------------------------------------------------------
// Pure helpers of steal.rs
// ---------------------------------------------------------------------------------------------

#[test]
fn spinning_bound_is_max_of_one_and_half_the_busy_ps() {
    for (busy, bound) in [
        (0usize, 1usize),
        (1, 1),
        (2, 1),
        (3, 1),
        (4, 2),
        (5, 2),
        (6, 3),
        (8, 4),
        (100, 50),
    ] {
        assert_eq!(max_spinning(busy), bound, "busy = {busy}");
    }
}

#[test]
fn a_spinner_makes_four_passes() {
    assert_eq!(STEAL_PASSES, 4);
}

#[test]
fn only_same_kind_tasks_are_stealable() {
    for task in 0..3 {
        for thief in 0..3 {
            assert_eq!(is_stealable(task, thief), task == thief, "task {task} thief {thief}");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The victim order is a permutation of every other P (never the thief itself).
    #[test]
    fn victim_order_is_a_permutation_of_the_other_ps(n in 1usize..40, me_seed in any::<usize>(), seed in any::<u64>()) {
        let me = me_seed % n;
        let mut rng = StdRng::seed_from_u64(seed);
        let mut order = victim_order(n, me, &mut rng);
        prop_assert_eq!(order.len(), n - 1);
        order.sort_unstable();
        let expected: Vec<usize> = (0..n).filter(|i| *i != me).collect();
        prop_assert_eq!(order, expected);
    }

    /// Equal seed => equal order (simulation reproducibility, SCH-21).
    #[test]
    fn victim_order_is_deterministic_per_seed(n in 2usize..40, seed in any::<u64>()) {
        let a = victim_order(n, 0, &mut StdRng::seed_from_u64(seed));
        let b = victim_order(n, 0, &mut StdRng::seed_from_u64(seed));
        prop_assert_eq!(a, b);
    }
}

#[test]
fn victim_order_is_actually_random_across_seeds() {
    let mut firsts = std::collections::BTreeSet::new();
    for seed in 0..200u64 {
        let order = victim_order(4, 0, &mut StdRng::seed_from_u64(seed));
        firsts.insert(order[0]);
    }
    assert_eq!(firsts.len(), 3, "every other P is eventually tried first: {firsts:?}");
    assert!(
        victim_order(1, 0, &mut StdRng::seed_from_u64(1)).is_empty(),
        "a lone P has nobody to steal from"
    );
}

// ---------------------------------------------------------------------------------------------
// SCH-03: steal half
// ---------------------------------------------------------------------------------------------

fn steal_scenario(cfg: SimConfig, kind: TaskKind, queued: usize, expect_stolen: u32) {
    let mut sim = SimRuntime::new(cfg);
    let ps = sim.procs_of(kind);
    assert!(ps.len() >= 2, "need a victim and a thief of kind {kind}");
    let (victim, thief) = (ps[0], ps[1]);
    let mut tasks: Vec<SimTask> = (0..queued)
        .map(|_| admitted(sim.submit_existing_to_proc(victim, spec_of(kind, 2))))
        .collect();
    let snap = sim.snapshot();
    assert_eq!((snap.lrq(victim) as usize, snap.lrq(thief)), (queued, 0));

    sim.step_proc(thief); // the idle P searches for work

    let trace = sim.trace();
    let steals: Vec<_> = trace.iter().filter(|e| matches!(e, TraceEvent::Steal { .. })).collect();
    assert_eq!(steals.len(), 1, "one steal: {steals:?}");
    assert_eq!(
        steals[0],
        &TraceEvent::Steal {
            thief,
            victim,
            count: expect_stolen
        },
        "the thief takes half (rounded up) of the victim's LRQ"
    );
    let snap = sim.snapshot();
    assert_eq!(
        snap.lrq(victim),
        queued as u32 - expect_stolen,
        "the victim keeps the rest"
    );
    let run_by_thief = count_events(&trace, |e| matches!(e, TraceEvent::Pick { proc, .. } if *proc == thief));
    assert_eq!(
        snap.lrq(thief) as usize + run_by_thief,
        expect_stolen as usize,
        "stolen tasks are queued or running on the thief"
    );

    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
}

/// SCH-03: P0 has 100 unbound runnable tasks; P1 (same kind) is idle and searches -> P1 steals
/// 50, P0 keeps 50.
#[test]
fn ac_sch_03_idle_p_steals_half_of_100() {
    steal_scenario(fixed(2, 1), TaskKind::DENO, 100, 50);
}

#[test]
fn ac_sch_03_odd_queue_steals_rounded_up() {
    steal_scenario(fixed(2, 1), TaskKind::DENO, 101, 51);
}

#[test]
fn ac_sch_03_wasm_tasks_are_stealable() {
    steal_scenario(fixed(1, 2), TaskKind::WASM, 40, 20);
}

/// Build tasks are stealable among build Ps.
#[test]
fn ac_sch_03_build_tasks_are_stealable_among_build_ps() {
    steal_scenario(fixed(1, 1).sched(|s| s.build_processors = 2), TaskKind::Build, 10, 5);
}

// ---------------------------------------------------------------------------------------------
// SCH-04: kinds never steal across
// ---------------------------------------------------------------------------------------------

fn no_cross_kind_steal(victim_kind: TaskKind, thief_kind: TaskKind) {
    let cfg = fixed(1, 1).sched(|s| s.build_processors = 1);
    let mut sim = SimRuntime::new(cfg);
    let victim = sim.procs_of(victim_kind)[0];
    let thief = sim.procs_of(thief_kind)[0];
    for _ in 0..10 {
        admitted(sim.submit_existing_to_proc(victim, spec_of(victim_kind, 2)));
    }

    for _ in 0..5 {
        let r = sim.step_proc(thief);
        assert!(
            !matches!(r, StepResult::Progressed),
            "{thief_kind} P found work among {victim_kind} tasks"
        );
    }
    let trace = sim.trace();
    assert_eq!(
        count_events(&trace, |e| matches!(e, TraceEvent::Steal { .. })),
        0,
        "it steals nothing"
    );
    assert_eq!(
        count_events(&trace, |e| matches!(e, TraceEvent::Pick { proc, .. } if *proc == thief)),
        0
    );
    assert_eq!(sim.snapshot().lrq(victim), 10, "the backlog is untouched");
    assert!(sim.snapshot().machines.iter().any(|m| m.parked), "it parks");
}

/// SCH-04: WASM P idle, Deno P with backlog -> the WASM P steals nothing and parks.
#[test]
fn ac_sch_04_wasm_p_does_not_steal_from_a_deno_backlog() {
    no_cross_kind_steal(TaskKind::DENO, TaskKind::WASM);
}

#[test]
fn ac_sch_04_deno_p_does_not_steal_from_a_wasm_backlog() {
    no_cross_kind_steal(TaskKind::WASM, TaskKind::DENO);
}

#[test]
fn ac_sch_04_build_p_does_not_steal_guest_tasks_and_guest_ps_do_not_steal_builds() {
    no_cross_kind_steal(TaskKind::DENO, TaskKind::Build);
    no_cross_kind_steal(TaskKind::Build, TaskKind::DENO);
}

// ---------------------------------------------------------------------------------------------
// runnext is never stolen; bound cap keeps new tasks stealable
// ---------------------------------------------------------------------------------------------

#[test]
fn stealing_never_takes_runnext() {
    let mut sim = sim(2, 1);
    let ps = sim.procs_of(TaskKind::DENO);
    let (p0, p1) = (ps[0], ps[1]);
    let log = install_call_hook(&sim, TaskKind::DENO);

    admitted(sim.submit_to_proc(p0, d(1).finish(), script![call("b"), io(50)]));
    sim.step_proc(p0); // A spawns its child into P0.runnext, then waits
    let child = log.lock()[0].child;
    let snap = sim.snapshot();
    assert_eq!(snap.p(p0).runnext, Some(child));
    assert_eq!(snap.lrq(p0), 0);

    let r = sim.step_proc(p1);
    assert!(
        !matches!(r, StepResult::Progressed),
        "nothing stealable: runnext is off limits in v1"
    );
    assert_eq!(count_events(&sim.trace(), |e| matches!(e, TraceEvent::Steal { .. })), 0);
    assert!(
        !sim.scripts().start_order().contains(&child),
        "the child has not started elsewhere"
    );
    assert_eq!(sim.snapshot().p(p0).runnext, Some(child));
}

/// Machine: `bound_count <= deno.max_bound_per_m`; at the cap the M only resumes bound
/// tasks and new tasks stay in the LRQ where other Ps can steal them.
#[test]
fn bound_cap_leaves_new_tasks_stealable() {
    let mut sim = SimRuntime::new(fixed(2, 1).sched(|s| s.live.max_bound_per_m = 2));
    let ps = sim.procs_of(TaskKind::DENO);
    let (p0, p1) = (ps[0], ps[1]);
    let mut tasks: Vec<SimTask> = (0..5)
        .map(|_| admitted(sim.submit_to_proc(p0, d(7).finish(), script![io(100)])))
        .collect();
    let check_cap = |sim: &SimRuntime| {
        for m in sim.snapshot().machines {
            assert!(m.bound_count <= 2, "{:?} holds {} bound tasks", m.id, m.bound_count);
        }
    };

    for _ in 0..6 {
        sim.step_proc(p0);
        check_cap(&sim);
    }
    let trace = sim.trace();
    let starts: Vec<_> = trace.iter().filter(|e| matches!(e, TraceEvent::Start { .. })).collect();
    assert_eq!(starts.len(), 2, "M0 stops starting tasks at the cap: {starts:?}");
    assert_eq!(sim.snapshot().lrq(p0), 3, "the other three stay in the LRQ");
    let m0 = start_machine(&trace, tasks[0].id).expect("first task started");
    assert_eq!(sim.snapshot().m(m0).bound_count, 2);

    for _ in 0..10 {
        sim.step_proc(p1);
        check_cap(&sim);
    }
    let trace = sim.trace();
    let on_other = count_events(
        &trace,
        |e| matches!(e, TraceEvent::Start { machine, .. } if *machine != m0),
    );
    assert!(
        (1..=2).contains(&on_other),
        "P1 steals unbound tasks and starts them on its own M (started {on_other})"
    );
    assert_eq!(
        count_events(
            &trace,
            |e| matches!(e, TraceEvent::Start { machine, .. } if *machine == m0)
        ),
        2
    );

    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
}

// ---------------------------------------------------------------------------------------------
// SCH-08: bounded spinning
// ---------------------------------------------------------------------------------------------

fn spinning_ok(snap: &rt_types::SchedSnapshot, kind: TaskKind) -> bool {
    let busy = snap
        .procs
        .iter()
        .filter(|p| p.kind == kind && p.status != ProcStatus::Idle)
        .count();
    snap.spinning as usize <= max_spinning(busy)
}

/// SCH-08 (simulation, checked after every quantum): 4 Ps of one kind under mixed load ->
/// spinning Ms <= max(1, busy/2).
#[test]
fn ac_sch_08_spinning_ms_never_exceed_half_the_busy_ps_sim() {
    let mut sim = SimRuntime::new(fixed(4, 0));
    sim.scripts()
        .set_for_service(rt_testkit::ids::service(2), script![rt_testkit::cpu(1), io(2)]);
    sim.scripts().set_for_service(
        rt_testkit::ids::service(3),
        script![rt_testkit::yield_now(), rt_testkit::cpu(1)],
    );
    sim.scripts().set_for_service(
        rt_testkit::ids::service(4),
        script![io(1), rt_testkit::cpu(1), rt_testkit::yield_now()],
    );

    let mut tasks: Vec<SimTask> = Vec::new();
    let mut checks = 0u32;
    for wave in 0..30u128 {
        for i in 0..10u128 {
            tasks.push(admitted(sim.submit_existing(d(2 + (wave + i) % 3).finish())));
        }
        for _ in 0..8 {
            sim.step();
            let snap = sim.snapshot();
            assert!(
                spinning_ok(&snap, TaskKind::DENO),
                "spinning={} procs={:?}",
                snap.spinning,
                snap.procs
            );
            checks += 1;
        }
        sim.advance(us(200));
    }
    for _ in 0..1_000_000 {
        match sim.step() {
            StepResult::Progressed => {}
            StepResult::Idle { next_timer_in } => sim.advance(next_timer_in),
            StepResult::Quiescent => break,
        }
        let snap = sim.snapshot();
        assert!(
            spinning_ok(&snap, TaskKind::DENO),
            "spinning={} procs={:?}",
            snap.spinning,
            snap.procs
        );
        checks += 1;
    }
    assert!(checks > 300);
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
}

/// SCH-08 (real threads): sample the snapshot while 4 Ps run a mixed load. A snapshot is not an
/// atomic cut, so a sample only counts as a violation when two consecutive samples agree, and at
/// most 1 % of all samples may violate the bound.
#[test]
fn ac_sch_08_spinning_ms_bounded_real_threads() {
    let _serial = serial();
    let real = build_real(real_cfg(4, 0));
    real.book.set_default(Script::from(vec![
        Step::Cpu(us(50)),
        Step::Io(ms(1)),
        Step::Cpu(us(20)),
        Step::Yield,
    ]));

    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (sched, stop) = (real.sched.clone(), stop.clone());
        std::thread::spawn(move || {
            let (mut samples, mut violations, mut persistent, mut prev_bad) = (0u64, 0u64, 0u64, false);
            while !stop.load(Ordering::Relaxed) {
                let bad = !spinning_ok(&sched.snapshot(), TaskKind::DENO);
                violations += u64::from(bad);
                persistent += u64::from(bad && prev_bad);
                prev_bad = bad;
                samples += 1;
                std::thread::sleep(us(50));
            }
            (samples, violations, persistent)
        })
    };

    let sched = real.sched.clone();
    block_on(async move {
        for _ in 0..20 {
            let handles: Vec<_> = (0..1_000)
                .map(|_| sched.submit(d(2).finish()).expect("admitted"))
                .collect();
            for out in futures_util::future::join_all(handles).await {
                assert!(out.is_ok(), "{out:?}");
            }
        }
    });
    stop.store(true, Ordering::Relaxed);
    let (samples, violations, persistent) = sampler.join().expect("sampler");
    real.shutdown();

    assert!(samples > 100, "sampler ran only {samples} times");
    assert_eq!(
        persistent, 0,
        "spinning Ms exceeded max(1, busy/2) in consecutive samples"
    );
    assert!(
        violations * 100 <= samples,
        "{violations} of {samples} samples violated the bound"
    );
}
