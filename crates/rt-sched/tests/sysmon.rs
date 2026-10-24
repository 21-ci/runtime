//! sysmon: the adaptive tick, hog detection / handoff, live reconfiguration.
//! CPU/wall limits and queue timeouts (also sysmon duties) are in `preemption_limits.rs` and
//! `admission.rs`; re-kinding is in `rekind.rs`.
//!
//! ACs: SCH-14.

mod common;

use std::time::Duration;

use common::*;
use proptest::collection::vec;
use proptest::prelude::*;
use rt_sched::sysmon::{TickState, BACKOFF_AFTER, MIN_TICK};
use rt_sched::trace::TraceEvent;
use rt_sched::LiveSchedConfig;
use rt_testkit::{cpu, script, SimRuntime, SimTask};
use rt_types::{ProcId, TaskKind};

// ---------------------------------------------------------------------------------------------
// Adaptive tick
// ---------------------------------------------------------------------------------------------

const MAX_TICK: Duration = Duration::from_millis(10);

#[test]
fn tick_starts_at_20_microseconds() {
    assert_eq!(MIN_TICK, us(20));
    assert_eq!(BACKOFF_AFTER, ms(1));
    let t = TickState::new();
    assert_eq!(t.tick, us(20));
    assert_eq!(t.idle_for, Duration::ZERO);
    assert_eq!(TickState::default(), t);
}

#[test]
fn tick_stays_at_20us_for_the_first_millisecond_then_doubles() {
    let mut t = TickState::new();
    for i in 1..=49u32 {
        t = t.next(false, MAX_TICK);
        assert_eq!(t.tick, us(20), "still within the first ms of idleness after {i} ticks");
        assert_eq!(t.idle_for, us(20) * i);
    }
    t = t.next(false, MAX_TICK); // 1 ms of no action reached
    assert!(t.idle_for >= BACKOFF_AFTER);
    assert_eq!(t.tick, us(40), "doubles once 1 ms passed without action");
}

#[test]
fn tick_keeps_doubling_while_idle_up_to_max_tick() {
    let mut t = TickState::new();
    while t.tick == MIN_TICK {
        t = t.next(false, MAX_TICK);
    }
    let mut prev = t.tick;
    let mut steps = 0;
    while t.tick < MAX_TICK {
        t = t.next(false, MAX_TICK);
        assert_eq!(
            t.tick,
            (prev * 2).min(MAX_TICK),
            "doubles each tick, capped at max_tick"
        );
        prev = t.tick;
        steps += 1;
        assert!(steps < 64, "never reaches max_tick");
    }
    assert_eq!(t.tick, MAX_TICK);
    for _ in 0..100 {
        t = t.next(false, MAX_TICK);
        assert_eq!(t.tick, MAX_TICK, "stays at max_tick");
    }
}

#[test]
fn any_action_resets_the_tick_to_20us() {
    let mut t = TickState::new();
    for _ in 0..500 {
        t = t.next(false, MAX_TICK);
    }
    assert_eq!(t.tick, MAX_TICK);
    assert_eq!(t.next(true, MAX_TICK), TickState::new());
}

#[test]
fn max_tick_below_the_minimum_never_shrinks_the_tick_below_20us() {
    let mut t = TickState::new();
    for _ in 0..200 {
        t = t.next(false, us(5));
        assert_eq!(t.tick, MIN_TICK);
    }
}

#[test]
fn live_max_tick_change_takes_effect_on_the_next_tick() {
    let mut t = TickState::new();
    for _ in 0..500 {
        t = t.next(false, ms(10));
    }
    assert_eq!(t.tick, ms(10));
    assert_eq!(
        t.next(false, ms(2)).tick,
        ms(2),
        "lowering sysmon.max_tick caps the tick immediately"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn tick_is_always_within_bounds_and_resets_on_action(
        steps in vec(prop::bool::weighted(0.1), 0..400),
        max_us in 1u64..50_000,
    ) {
        let max_tick = us(max_us);
        let cap = max_tick.max(MIN_TICK);
        let mut t = TickState::new();
        for acted in steps {
            let next = t.next(acted, max_tick);
            prop_assert!(next.tick >= MIN_TICK);
            prop_assert!(next.tick <= cap);
            if acted {
                prop_assert_eq!(next, TickState::new());
            } else {
                prop_assert!(next.tick >= t.tick, "the tick never shrinks while idle");
                prop_assert!(next.idle_for > t.idle_for);
            }
            t = next;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// SCH-14: hog handoff
// ---------------------------------------------------------------------------------------------

struct Hog {
    started: Duration,
    handoff: Duration,
    moved: u32,
}

/// P0 runs a 200 ms CPU task (a Deno hog: V8 cannot yield) with `queued` tasks in its LRQ while the
/// other Deno Ps stay idle. Advances virtual time (without running any Machine) until sysmon
/// hands the hog's queue off.
fn run_hog_until_handoff(sim: &mut SimRuntime, p0: ProcId, queued: usize) -> (SimTask, Vec<SimTask>, Hog) {
    let hog = admitted(sim.submit_to_proc(p0, d(1).finish(), script![cpu(200)]));
    let tasks: Vec<SimTask> = (0..queued)
        .map(|_| admitted(sim.submit_existing_to_proc(p0, d(2).finish())))
        .collect();
    sim.step_proc(p0); // the hog starts and burns CPU on M0

    let started = time_of(
        &sim.trace_timed(),
        |e| matches!(e, TraceEvent::Start { task, .. } if *task == hog.id),
    )
    .expect("the hog started");
    for _ in 0..200 {
        if count_events(&sim.trace(), |e| matches!(e, TraceEvent::HogHandoff { .. })) > 0 {
            break;
        }
        sim.advance(ms(1));
    }
    let timed = sim.trace_timed();
    let (handoff, moved) = timed
        .iter()
        .find_map(|(t, e)| match e {
            TraceEvent::HogHandoff { proc, moved } if *proc == p0 => Some((*t, *moved)),
            _ => None,
        })
        .expect("sysmon never detected the hog");
    (
        hog,
        tasks,
        Hog {
            started,
            handoff,
            moved,
        },
    )
}

/// SCH-14: a Deno P runs a 200 ms CPU task with 20 tasks in its LRQ, other Deno Ps idle -> after
/// the slice elapses, within 2 sysmon ticks the 20 tasks are moved to the GRQ and start on other
/// Ps.
#[test]
fn ac_sch_14_deno_hog_handoff_moves_the_lrq_to_the_grq_within_two_ticks() {
    let mut sim = sim(3, 1); // balanced: time_slice 10 ms, sysmon max_tick 10 ms
    let ps = sim.procs_of(TaskKind::DENO);
    let p0 = ps[0];
    let (mut hog, mut tasks, h) = run_hog_until_handoff(&mut sim, p0, 20);
    let (slice, tick) = (ms(10), ms(10));

    assert!(
        h.handoff >= h.started + slice,
        "handoff at {:?}, before the slice elapsed",
        h.handoff - h.started
    );
    assert!(
        h.handoff <= h.started + slice + 2 * tick,
        "handoff {:?} after the hog started: more than 2 sysmon ticks after the slice",
        h.handoff - h.started
    );
    assert_eq!(h.moved, 20, "the whole LRQ moved");
    let snap = sim.snapshot();
    assert_eq!(snap.grq_len(TaskKind::DENO), 20, "the 20 tasks are on GRQ[deno]");
    assert_eq!(snap.lrq(p0), 0);

    // The other Deno Ps pick them up.
    for _ in 0..30 {
        sim.step_proc(ps[1]);
        sim.step_proc(ps[2]);
    }
    assert!(sim.outcome(&mut hog).is_none(), "the hog is still running its 200 ms");
    let timed = sim.trace_timed();
    let trace = sim.trace();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
        let picks = picks_of(&trace, t.id);
        assert_eq!(picks.len(), 1);
        assert_ne!(picks[0].0, p0, "{:?} started on the hogged P", t.id);
        let at = time_of(&timed, |e| matches!(e, TraceEvent::Start { task, .. } if *task == t.id)).expect("started");
        assert!(
            at <= h.started + slice + 2 * tick + ms(1),
            "{:?} started {:?} after the hog",
            t.id,
            at - h.started
        );
    }

    sim.run_until_idle();
    assert_ok(sim.outcome(&mut hog));
}

/// `sched.handoff_on_hog`: sysmon detaches the hogged P from its M and gives it to a spare M, so
/// new work on that P runs while the hog keeps burning its core.
#[test]
fn ac_sch_14_handoff_on_hog_gives_the_p_to_a_spare_machine() {
    let mut cfg = fixed(1, 1).sched(|s| s.spare_machines = 1);
    cfg.sched.live.handoff_on_hog = true;
    let mut sim = SimRuntime::new(cfg);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let (mut hog, mut tasks, _h) = run_hog_until_handoff(&mut sim, p0, 0);
    let hog_machine = start_machine(&sim.trace(), hog.id).expect("hog started");

    let snap = sim.snapshot();
    assert_ne!(snap.p(p0).machine, Some(hog_machine), "the P left the hog's Machine");
    assert!(snap.p(p0).machine.is_some(), "the P now belongs to a spare Machine");

    tasks.push(admitted(sim.submit_existing_to_proc(p0, d(3).finish())));
    for _ in 0..4 {
        sim.step_proc(p0);
    }
    assert_ok(sim.outcome(&mut tasks[0]));
    assert!(
        sim.outcome(&mut hog).is_none(),
        "the hog is still running on its old Machine"
    );
    let (_, machine, _) = picks_of(&sim.trace(), tasks[0].id)[0];
    assert_ne!(machine, hog_machine, "new work on P0 ran on the spare Machine");

    sim.run_until_idle();
    assert_ok(sim.outcome(&mut hog));
}

#[test]
fn reconfigure_time_slice_moves_the_hog_detection_threshold() {
    let mut sim = sim(3, 1);
    let mut live: LiveSchedConfig = (*sim.scheduler().live_config()).clone();
    assert_eq!(live.time_slice, ms(10), "balanced default");
    live.time_slice = ms(50);
    sim.scheduler().reconfigure(live);
    assert_eq!(sim.scheduler().live_config().time_slice, ms(50));

    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let (_hog, _tasks, h) = run_hog_until_handoff(&mut sim, p0, 5);
    assert!(
        h.handoff >= h.started + ms(50),
        "a 50 ms slice: handoff at {:?}",
        h.handoff - h.started
    );
    assert!(
        h.handoff <= h.started + ms(50) + 2 * MAX_TICK,
        "handoff too late: {:?}",
        h.handoff - h.started
    );
}

#[test]
fn a_task_shorter_than_the_slice_is_never_a_hog() {
    let mut sim = sim(2, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let mut a = admitted(sim.submit_to_proc(p0, d(1).finish(), script![cpu(5)]));
    for _ in 0..5 {
        admitted(sim.submit_existing_to_proc(p0, d(2).finish()));
    }
    sim.step_proc(p0);
    sim.advance(ms(30));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut a));
    assert_eq!(
        count_events(&sim.trace(), |e| matches!(e, TraceEvent::HogHandoff { .. })),
        0
    );
}

#[test]
fn live_config_defaults_are_the_balanced_profile() {
    let live = LiveSchedConfig::default();
    assert_eq!(live.spin, us(10));
    assert_eq!(live.time_slice, ms(10));
    assert_eq!(live.sysmon_max_tick, ms(10));
    assert_eq!(live.rebalance_interval, ms(100));
    assert!(!live.handoff_on_hog);
    assert_eq!(live.max_bound_per_m, 64);
}
