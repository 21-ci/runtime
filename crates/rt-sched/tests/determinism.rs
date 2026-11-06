//! Determinism of the simulation: seeded RNG + virtual time => the same
//! workload yields the same event trace.
//!
//! ACs: SCH-21.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rt_sched::trace::{TraceEvent, TraceRecorder};
use rt_testkit::{call, cpu, io, panic, yield_now, Script, SimConfig, SimRuntime, SimTask};
use rt_types::SchedSnapshot;

struct Run {
    hash: u64,
    trace: Vec<TraceEvent>,
    timed: Vec<(Duration, TraceEvent)>,
    snapshot: SchedSnapshot,
    ok: usize,
}

fn script_for(kind: u32) -> Script {
    match kind {
        0 => Script::from(vec![cpu(1), io(2)]),
        1 => Script::from(vec![yield_now(), cpu(2)]),
        2 => Script::from(vec![io(5), cpu(1), io(1)]),
        3 => Script::from(vec![call("x"), cpu(1)]),
        4 => Script::from(vec![cpu(3), panic()]),
        _ => Script::noop(),
    }
}

/// A fixed workload (independent of the simulation seed): 4 + 3 + 1 processors, waves of mixed
/// Deno / WASM / Build tasks interleaved with virtual time.
fn run(sim_seed: u64, keep_trace: bool, waves: u32) -> Run {
    let mut cfg = SimConfig::default().seed(sim_seed).keep_trace(keep_trace);
    cfg.sched.processors = 6;
    cfg.sched.deno = rt_sched::KindRange { min: 1, max: 5 };
    cfg.sched.wasm = rt_sched::KindRange { min: 1, max: 5 };
    cfg.sched.build_processors = 2;
    let mut sim = SimRuntime::new(cfg);

    let mut workload = StdRng::seed_from_u64(42);
    let mut tasks: Vec<SimTask> = Vec::new();
    for wave in 0..waves {
        for i in 0..20u128 {
            let service = 1 + (u128::from(wave) * 20 + i) % 12;
            let script = script_for((service % 6) as u32);
            let spec = match workload.gen_range(0..10) {
                0 => b(100 + service).finish(),
                1..=5 => d(service).finish(),
                _ => w(50 + service).finish(),
            };
            tasks.push(admitted(sim.submit(spec, script)));
        }
        sim.run_for(ms(workload.gen_range(1..8)));
        sim.advance(us(workload.gen_range(0..500)));
    }
    sim.run_until_idle();

    let mut ok = 0;
    for t in &mut tasks {
        if sim.outcome(t).is_some_and(|o| o.is_ok()) {
            ok += 1;
        }
    }
    Run {
        hash: sim.trace_hash(),
        trace: if keep_trace { sim.trace() } else { Vec::new() },
        timed: if keep_trace { sim.trace_timed() } else { Vec::new() },
        snapshot: sim.snapshot(),
        ok,
    }
}

/// SCH-21: SimRuntime, fixed seed and workload, run twice -> identical event-trace hash.
#[test]
fn ac_sch_21_same_seed_and_workload_give_the_same_trace_hash() {
    let a = run(7, true, 10);
    let b = run(7, true, 10);
    assert_eq!(a.hash, b.hash, "trace hash differs between two runs of the same seed");
    assert_eq!(a.trace, b.trace, "event traces differ");
    assert_eq!(a.timed, b.timed, "event timestamps differ");
    assert_eq!(a.snapshot, b.snapshot, "final snapshots differ");
    assert!(
        a.trace.len() > 200,
        "the workload produced only {} events",
        a.trace.len()
    );
    assert!(a.ok > 0, "some tasks complete normally");
}

#[test]
fn ac_sch_21_every_seed_is_reproducible() {
    for seed in 0..40u64 {
        assert_eq!(run(seed, false, 3).hash, run(seed, false, 3).hash, "seed {seed}");
    }
}

#[test]
fn ac_sch_21_the_hash_does_not_depend_on_keeping_the_events() {
    assert_eq!(run(11, true, 6).hash, run(11, false, 6).hash);
}

#[test]
fn ac_sch_21_replaying_the_events_reproduces_the_hash() {
    let a = run(5, true, 6);
    let mut rec = TraceRecorder::new(false);
    for ev in &a.trace {
        rec.record(*ev);
    }
    assert_eq!(
        rec.hash(),
        a.hash,
        "the hash is exactly the FNV-1a over the recorded events"
    );
}

#[test]
fn ac_sch_21_different_seeds_explore_different_schedules() {
    let hashes: BTreeSet<u64> = (0..8u64).map(|seed| run(seed, false, 6).hash).collect();
    assert!(
        hashes.len() >= 2,
        "8 seeds produced a single schedule: placement and steal victims are not seeded"
    );
}

#[test]
fn ac_sch_21_the_trace_records_the_scheduling_decisions() {
    let a = run(3, true, 6);
    let has = |f: fn(&TraceEvent) -> bool| a.trace.iter().any(f);
    assert!(has(|e| matches!(e, TraceEvent::Submit { .. })));
    assert!(has(|e| matches!(e, TraceEvent::Place { .. })));
    assert!(has(|e| matches!(e, TraceEvent::Pick { .. })));
    assert!(has(|e| matches!(e, TraceEvent::Start { .. })));
    assert!(has(|e| matches!(e, TraceEvent::Wait { .. })), "I/O waits are traced");
    assert!(has(|e| matches!(e, TraceEvent::Wake { .. })), "wakes are traced");
    assert!(has(|e| matches!(e, TraceEvent::Finish { .. })));
    assert!(has(|e| matches!(e, TraceEvent::Park { .. })), "idle Machines park");
    // Time never goes backwards in the timed trace.
    assert!(a.timed.windows(2).all(|p| p[0].0 <= p[1].0));
}

/// Two tenants with mixed job sizes behind few node slots, so the fair order (deadline keys,
/// tenant virtual time, estimator samples) decides every grant.
fn fair_run(seed: u64) -> (u64, usize) {
    let mut cfg = SimConfig::default().seed(seed).keep_trace(false).fixed_split(2, 1);
    cfg.sched.max_inflight = 2;
    cfg.sched.max_queued = 1_000;
    let mut sim = SimRuntime::new(cfg);
    for (service, step) in [(1u128, io(2)), (2, io(40)), (3, cpu(3)), (4, io(300))] {
        sim.scripts()
            .set_for_service(rt_testkit::ids::service(service), Script::from(vec![step]));
    }
    let mut workload = StdRng::seed_from_u64(9);
    let mut tasks: Vec<SimTask> = Vec::new();
    for _ in 0..30 {
        for _ in 0..6 {
            let service = workload.gen_range(1..=4u128);
            let tenant = 1 + service % 2;
            tasks.push(admitted(
                sim.submit_existing(d(service).tenant(tenant).queue_timeout_ms(60_000).finish()),
            ));
        }
        sim.run_for(ms(workload.gen_range(1..20)));
    }
    sim.run_until_idle();
    let ok = tasks
        .iter_mut()
        .filter_map(|t| sim.outcome(t).filter(|o| o.is_ok()).map(|_| ()))
        .count();
    (sim.trace_hash(), ok)
}

/// SCH-21 with the fair order: mixed sizes, two tenants, the same trace hash twice.
#[test]
fn ac_sch_21_fair_order_is_deterministic() {
    let a = fair_run(13);
    assert_eq!(a, fair_run(13));
    assert_eq!(a.1, 180, "every task completes");
}
