//! Elastic re-kinding: the pure decision function and the
//! simulated end-to-end behavior driven by sysmon.
//!
//! ACs: SCH-19, SCH-20.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::*;
use proptest::prelude::*;
use rt_sched::rekind::{decide, DonorCandidate, KindLoad, Rekind};
use rt_sched::trace::TraceEvent;
use rt_sched::{KindRange, SchedConfig, StepResult};
use rt_testkit::{cpu, io, script, SimConfig, SimRuntime, SimTask};
use rt_types::{ProcId, TaskKind};

const THRESHOLD: f64 = 2.0;
const COOLDOWN: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------------------------
// decide(): examples
// ---------------------------------------------------------------------------------------------

fn cand(proc: u32, rekindable: bool, since_ms: u64) -> DonorCandidate {
    DonorCandidate {
        proc: ProcId(proc),
        rekindable,
        since_last_rekind: ms(since_ms),
    }
}

fn load(
    kind: TaskKind,
    procs: u32,
    min: u32,
    max: u32,
    work: (u32, u32, u32),
    candidates: Vec<DonorCandidate>,
) -> KindLoad {
    KindLoad {
        kind,
        procs,
        min,
        max,
        runnable: work.0,
        running: work.1,
        waiting_memory: work.2,
        p90_queue_wait: Duration::ZERO,
        candidates,
    }
}

/// Deno overloaded (pressure 5.0), WASM idle with two free, cooled-down Ps.
fn deno_hungry_wasm_idle() -> Vec<KindLoad> {
    vec![
        load(TaskKind::DENO, 2, 1, 3, (8, 2, 0), vec![]),
        load(
            TaskKind::WASM,
            2,
            1,
            3,
            (0, 0, 0),
            vec![cand(2, true, 5_000), cand(3, true, 5_000)],
        ),
    ]
}

#[test]
fn pressure_is_demand_over_ps() {
    let l = load(TaskKind::DENO, 4, 1, 3, (5, 2, 1), vec![]);
    assert!((l.pressure() - 2.0).abs() < 1e-9, "(5 + 2 + 1) / 4");
    let l = load(TaskKind::DENO, 2, 1, 3, (0, 0, 6), vec![]);
    assert!(
        (l.pressure() - 3.0).abs() < 1e-9,
        "tasks waiting for memory count as demand"
    );
    let l = load(TaskKind::DENO, 0, 0, 3, (3, 0, 0), vec![]);
    assert!(
        (l.pressure() - 3.0).abs() < 1e-9,
        "a kind without Ps is treated as having one"
    );
}

#[test]
fn a_hungry_kind_takes_one_idle_p_from_an_idle_kind() {
    let out = decide(&deno_hungry_wasm_idle(), THRESHOLD, COOLDOWN);
    assert_eq!(out.len(), 1, "at most one re-kind per hungry kind per round: {out:?}");
    assert_eq!((out[0].from, out[0].to), (TaskKind::WASM, TaskKind::DENO));
    assert!(
        [ProcId(2), ProcId(3)].contains(&out[0].proc),
        "only a donor candidate can move: {out:?}"
    );
}

#[test]
fn decide_is_deterministic_for_equal_inputs() {
    let loads = deno_hungry_wasm_idle();
    assert_eq!(decide(&loads, THRESHOLD, COOLDOWN), decide(&loads, THRESHOLD, COOLDOWN));
}

#[test]
fn pressure_must_exceed_the_threshold_strictly() {
    // (3 + 1) / 2 = 2.0: not greater than 2.0.
    let mut loads = deno_hungry_wasm_idle();
    loads[0] = load(TaskKind::DENO, 2, 1, 3, (3, 1, 0), vec![]);
    assert_eq!(decide(&loads, THRESHOLD, COOLDOWN), vec![]);
    // (4 + 1) / 2 = 2.5 > 2.0.
    loads[0] = load(TaskKind::DENO, 2, 1, 3, (4, 1, 0), vec![]);
    assert_eq!(decide(&loads, THRESHOLD, COOLDOWN).len(), 1);
}

#[test]
fn memory_waiters_make_a_kind_hungry() {
    let mut loads = deno_hungry_wasm_idle();
    loads[0] = load(TaskKind::DENO, 2, 1, 3, (0, 1, 5), vec![]);
    assert_eq!(decide(&loads, THRESHOLD, COOLDOWN).len(), 1);
}

#[test]
fn the_donor_never_goes_below_its_min() {
    let mut loads = deno_hungry_wasm_idle();
    loads[1] = load(TaskKind::WASM, 1, 1, 3, (0, 0, 0), vec![cand(3, true, 5_000)]);
    assert_eq!(decide(&loads, THRESHOLD, COOLDOWN), vec![], "WASM has exactly its min");
}

#[test]
fn the_hungry_kind_never_goes_above_its_max() {
    let mut loads = deno_hungry_wasm_idle();
    loads[0] = load(TaskKind::DENO, 3, 1, 3, (30, 3, 0), vec![]);
    assert_eq!(decide(&loads, THRESHOLD, COOLDOWN), vec![], "Deno is already at max");
}

#[test]
fn only_idle_ps_without_bound_tasks_can_be_donated() {
    let mut loads = deno_hungry_wasm_idle();
    loads[1] = load(
        TaskKind::WASM,
        2,
        1,
        3,
        (0, 0, 0),
        vec![cand(2, false, 5_000), cand(3, false, 5_000)],
    );
    assert_eq!(
        decide(&loads, THRESHOLD, COOLDOWN),
        vec![],
        "busy / bound Ps are never re-kinded"
    );
    // One becomes free: it is the one that moves.
    loads[1] = load(
        TaskKind::WASM,
        2,
        1,
        3,
        (0, 0, 0),
        vec![cand(2, false, 5_000), cand(3, true, 5_000)],
    );
    assert_eq!(
        decide(&loads, THRESHOLD, COOLDOWN),
        vec![Rekind {
            proc: ProcId(3),
            from: TaskKind::WASM,
            to: TaskKind::DENO
        }]
    );
}

#[test]
fn a_p_is_re_kinded_at_most_once_per_cooldown() {
    let mut loads = deno_hungry_wasm_idle();
    loads[1] = load(
        TaskKind::WASM,
        2,
        1,
        3,
        (0, 0, 0),
        vec![cand(2, true, 999), cand(3, true, 400)],
    );
    assert_eq!(
        decide(&loads, THRESHOLD, COOLDOWN),
        vec![],
        "both Ps changed kind less than 1 s ago"
    );
    loads[1] = load(
        TaskKind::WASM,
        2,
        1,
        3,
        (0, 0, 0),
        vec![cand(2, true, 999), cand(3, true, 1_000)],
    );
    assert_eq!(
        decide(&loads, THRESHOLD, COOLDOWN),
        vec![Rekind {
            proc: ProcId(3),
            from: TaskKind::WASM,
            to: TaskKind::DENO
        }],
        "exactly one cooldown after its last re-kind a P may move again"
    );
}

#[test]
fn build_processors_are_never_re_kinded() {
    let loads = vec![
        load(TaskKind::Build, 1, 1, 1, (50, 1, 0), vec![]),
        load(
            TaskKind::DENO,
            2,
            1,
            3,
            (0, 0, 0),
            vec![cand(0, true, 5_000), cand(1, true, 5_000)],
        ),
        load(
            TaskKind::WASM,
            2,
            1,
            3,
            (0, 0, 0),
            vec![cand(2, true, 5_000), cand(3, true, 5_000)],
        ),
    ];
    let out = decide(&loads, THRESHOLD, COOLDOWN);
    assert!(
        out.iter().all(|r| r.from != TaskKind::Build && r.to != TaskKind::Build),
        "{out:?}"
    );
}

fn arb_load(kind: TaskKind) -> impl Strategy<Value = KindLoad> {
    (
        1u32..6,
        0u32..4,
        0u32..6,
        0u32..40,
        0u32..8,
        0u32..8,
        prop::collection::vec((any::<bool>(), 0u64..3_000), 0..6),
    )
        .prop_map(move |(procs, min, extra, runnable, running, waiting_memory, cands)| {
            let min = min.min(procs);
            KindLoad {
                kind,
                procs,
                min,
                max: min + extra,
                runnable,
                running,
                waiting_memory,
                p90_queue_wait: Duration::ZERO,
                candidates: cands
                    .into_iter()
                    .enumerate()
                    .map(|(i, (rekindable, since))| DonorCandidate {
                        proc: ProcId(match kind {
                            TaskKind::Exec(rt_types::RuntimeKind::Deno) => i as u32,
                            _ => 100 + i as u32,
                        }),
                        rekindable,
                        since_last_rekind: ms(since),
                    })
                    .collect(),
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Whatever the loads, every decision respects the re-kinding rules: the hungry kind is above
    /// the threshold and below its max, the donor is above its min and gives a free, cooled-down
    /// P of its own, and there is at most one decision per receiving kind.
    #[test]
    fn decisions_always_respect_the_constraints(deno in arb_load(TaskKind::DENO), wasm in arb_load(TaskKind::WASM)) {
        let loads = vec![deno, wasm];
        let out = decide(&loads, THRESHOLD, COOLDOWN);
        prop_assert!(out.len() <= 2);
        let mut seen_procs = std::collections::HashSet::new();
        let mut receivers = std::collections::HashSet::new();
        for r in &out {
            prop_assert!(r.from != r.to);
            prop_assert!(seen_procs.insert(r.proc), "a P moves at most once per round: {out:?}");
            prop_assert!(receivers.insert(r.to), "one re-kind per hungry kind: {out:?}");
            let hungry = loads.iter().find(|l| l.kind == r.to).expect("receiver is a known kind");
            let donor = loads.iter().find(|l| l.kind == r.from).expect("donor is a known kind");
            prop_assert!(hungry.pressure() > THRESHOLD);
            prop_assert!(hungry.procs < hungry.max);
            prop_assert!(donor.procs > donor.min);
            let c = donor.candidates.iter().find(|c| c.proc == r.proc).expect("the moved P is a donor candidate");
            prop_assert!(c.rekindable);
            prop_assert!(c.since_last_rekind >= COOLDOWN);
        }
        prop_assert_eq!(decide(&loads, THRESHOLD, COOLDOWN), out, "deterministic");
    }
}

// ---------------------------------------------------------------------------------------------
// SchedConfig::kind_bounds
// ---------------------------------------------------------------------------------------------

#[test]
fn kind_bounds_expand_max_zero_to_what_the_other_kinds_min_leaves() {
    let cfg = SchedConfig::default(); // 4 Ps, deno {1, 0}, wasm {1, 0}
    assert_eq!(cfg.kind_bounds(TaskKind::DENO), (1, 3));
    assert_eq!(cfg.kind_bounds(TaskKind::WASM), (1, 3));
    assert_eq!(
        cfg.kind_bounds(TaskKind::Build),
        (1, 1),
        "build processors are dedicated"
    );

    let cfg = SchedConfig {
        wasm: KindRange { min: 2, max: 0 },
        ..SchedConfig::default()
    };
    assert_eq!(cfg.kind_bounds(TaskKind::DENO), (1, 2), "4 Ps - WASM's min of 2");
    assert_eq!(cfg.kind_bounds(TaskKind::WASM), (2, 3));
}

#[test]
fn kind_bounds_keep_explicit_max_and_never_return_max_below_min() {
    let cfg = SchedConfig {
        deno: KindRange { min: 1, max: 2 },
        ..SchedConfig::default()
    };
    assert_eq!(cfg.kind_bounds(TaskKind::DENO), (1, 2));
    let cfg = SchedConfig {
        deno: KindRange { min: 3, max: 1 },
        ..SchedConfig::default()
    };
    assert_eq!(cfg.kind_bounds(TaskKind::DENO), (3, 3));
}

// ---------------------------------------------------------------------------------------------
// SCH-19 / SCH-20 (simulation)
// ---------------------------------------------------------------------------------------------

/// 4 guest Ps, deno and wasm both min 1 / max 3.
fn elastic_cfg() -> SimConfig {
    let mut cfg = SimConfig::default().seed(SEED);
    cfg.sched.processors = 4;
    cfg.sched.deno = KindRange { min: 1, max: 3 };
    cfg.sched.wasm = KindRange { min: 1, max: 3 };
    cfg
}

fn split(sim: &SimRuntime) -> (usize, usize) {
    (sim.procs_of(TaskKind::DENO).len(), sim.procs_of(TaskKind::WASM).len())
}

/// Samples the Ps after every virtual millisecond.
#[derive(Default)]
struct Tracker {
    last_rekinds: HashMap<ProcId, u64>,
    rekind_times: HashMap<ProcId, Vec<Duration>>,
    reached: Option<Duration>,
}

impl Tracker {
    fn sample(&mut self, sim: &SimRuntime, target: Option<(usize, usize)>) {
        let now = sim.clock().elapsed();
        let snap = sim.snapshot();
        for p in &snap.procs {
            let before = self.last_rekinds.insert(p.id, p.rekinds).unwrap_or(0);
            assert!(
                p.rekinds - before <= 1,
                "{:?} was re-kinded twice within one millisecond",
                p.id
            );
            if p.rekinds > before {
                self.rekind_times.entry(p.id).or_default().push(now);
            }
        }
        let s = split(sim);
        assert_eq!(s.0 + s.1, 4, "the guest P count is constant");
        assert!(
            (1..=3).contains(&s.0) && (1..=3).contains(&s.1),
            "min/max violated: {s:?}"
        );
        if Some(s) == target && self.reached.is_none() {
            self.reached = Some(now);
        }
    }
}

/// 3 tasks of 1 ms CPU per virtual millisecond of `kind` for `load_for`; keeps sampling (without
/// new load) until the `target` split is reached, at most `until` after the start.
fn drive(
    sim: &mut SimRuntime,
    tracker: &mut Tracker,
    kind: TaskKind,
    load_for: Duration,
    until: Duration,
    target: (usize, usize),
    tasks: &mut Vec<SimTask>,
) {
    let start = sim.clock().elapsed();
    loop {
        let elapsed = sim.clock().elapsed() - start;
        if elapsed >= load_for && (tracker.reached.is_some() || elapsed >= until) {
            break;
        }
        if elapsed < load_for {
            for _ in 0..3 {
                let spec = if kind == TaskKind::DENO {
                    d(1).finish()
                } else {
                    w(2).finish()
                };
                tasks.push(admitted(sim.submit_existing(spec)));
            }
        }
        sim.run_for(ms(1));
        tracker.sample(sim, Some(target));
    }
}

/// SCH-19: 4 Ps, deno/wasm min 1 max 3, starting 2/2. 1 s of Deno-only load, then WASM-only
/// load -> converges to 3/1, then 1/3, each within 2 s; at most 1 re-kind per P per cooldown.
#[test]
fn ac_sch_19_rekinding_converges_with_the_load_and_respects_the_cooldown() {
    let mut sim = SimRuntime::new(elastic_cfg());
    sim.scripts()
        .set_for_service(rt_testkit::ids::service(1), script![cpu(1)]);
    sim.scripts()
        .set_for_service(rt_testkit::ids::service(2), script![cpu(1)]);
    assert_eq!(split(&sim), (2, 2), "starting 2/2");

    let mut tasks = Vec::new();
    let mut tracker = Tracker::default();

    // Phase 1: Deno-only load.
    let phase1 = sim.clock().elapsed();
    drive(
        &mut sim,
        &mut tracker,
        TaskKind::DENO,
        ms(1_000),
        ms(2_000),
        (3, 1),
        &mut tasks,
    );
    let reached = tracker.reached.expect("never converged to 3 deno / 1 wasm");
    assert!(reached - phase1 <= ms(2_000), "3/1 took {:?}", reached - phase1);
    sim.run_until_idle(); // drain the Deno backlog
    assert_eq!(split(&sim), (3, 1), "the split is stable while idle");

    // Phase 2: WASM-only load.
    tracker.reached = None;
    let phase2 = sim.clock().elapsed();
    drive(
        &mut sim,
        &mut tracker,
        TaskKind::WASM,
        ms(1_000),
        ms(2_000),
        (1, 3),
        &mut tasks,
    );
    let reached = tracker.reached.expect("never converged to 1 deno / 3 wasm");
    assert!(reached - phase2 <= ms(2_000), "1/3 took {:?}", reached - phase2);
    sim.run_until_idle();
    assert_eq!(split(&sim), (1, 3));

    // Hysteresis: one re-kind per P per cooldown.
    for (p, times) in &tracker.rekind_times {
        for pair in times.windows(2) {
            assert!(
                pair[1] - pair[0] >= COOLDOWN,
                "{p:?} re-kinded at {:?} and {:?}",
                pair[0],
                pair[1]
            );
        }
    }
    // Every re-kind is accounted for in the trace and in the per-P counters.
    let trace = sim.trace();
    let events = count_events(&trace, |e| matches!(e, TraceEvent::Rekind { .. }));
    let counted: u64 = sim.snapshot().procs.iter().map(|p| p.rekinds).sum();
    assert_eq!(events as u64, counted);
    assert_eq!(events, 3, "one P moved to Deno, then two Ps moved to WASM");

    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
}

/// SCH-20: a P whose M has bound Deno tasks is not re-kinded until `bound_count == 0`.
#[test]
fn ac_sch_20_p_with_bound_tasks_is_not_rekinded_until_they_finish() {
    let mut sim = SimRuntime::new(elastic_cfg());
    assert_eq!(split(&sim), (2, 2));
    let deno_ps = sim.procs_of(TaskKind::DENO);
    sim.scripts()
        .set_for_service(rt_testkit::ids::service(1), script![io(500)]);
    sim.scripts()
        .set_for_service(rt_testkit::ids::service(2), script![cpu(5)]);

    // One bound Deno task (waiting on I/O for 500 ms) on each Deno M: both Ps are idle but bound.
    let mut bound: Vec<SimTask> = deno_ps
        .iter()
        .map(|p| {
            let t = admitted(sim.submit_existing_to_proc(*p, d(1).finish()));
            sim.step_proc(*p);
            t
        })
        .collect();
    assert!(
        sim.snapshot().machines.iter().filter(|m| m.bound_count == 1).count() >= 2,
        "both Deno Ms hold a bound task"
    );

    // Heavy WASM-only load: 400 x 5 ms of CPU on 2 WASM Ps (pressure far above 2.0).
    let mut load: Vec<SimTask> = (0..400).map(|_| admitted(sim.submit_existing(w(2).finish()))).collect();

    for _ in 0..40 {
        sim.run_for(ms(10)); // t = 10 .. 400 ms
        assert_eq!(
            split(&sim),
            (2, 2),
            "no Deno P may be re-kinded while its M has bound tasks"
        );
        assert_eq!(
            count_events(&sim.trace(), |e| matches!(e, TraceEvent::Rekind { .. })),
            0
        );
    }

    sim.run_for(ms(600)); // the bound tasks finish at 500 ms; the re-kind may happen from then on
    assert_eq!(
        split(&sim),
        (1, 3),
        "after the bound tasks finished one Deno P moved to WASM (Deno min 1)"
    );
    let timed = sim.trace_timed();
    let at = time_of(&timed, |e| matches!(e, TraceEvent::Rekind { .. })).expect("a re-kind happened");
    assert!(at >= ms(500), "re-kinded at {at:?}, before the bound tasks finished");
    let moved = timed.iter().find_map(|(_, e)| match e {
        TraceEvent::Rekind { proc, from, to } => Some((*proc, *from, *to)),
        _ => None,
    });
    let (proc, from, to) = moved.expect("rekind event");
    assert!(deno_ps.contains(&proc));
    assert_eq!((from, to), (TaskKind::DENO, TaskKind::WASM));

    loop {
        match sim.step() {
            StepResult::Progressed => {}
            StepResult::Idle { next_timer_in } => sim.advance(next_timer_in),
            StepResult::Quiescent => break,
        }
    }
    for t in bound.iter_mut().chain(load.iter_mut()) {
        assert_ok(sim.outcome(t));
    }
}
