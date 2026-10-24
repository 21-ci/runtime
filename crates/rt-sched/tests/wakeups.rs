//! Waiting and wakeups (task state rules): bound Deno tasks resume
//! on their Machine, WASM tasks migrate, no wake is ever lost, idle Machines park.
//!
//! ACs: SCH-05, SCH-06, SCH-07, SCH-09, SCH-10 (perf, ignored).

mod common;

use std::time::Duration;

use common::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rt_sched::trace::PickSource;
use rt_sched::{LiveSchedConfig, SchedConfig, StepResult, TaskState};
use rt_testkit::{cpu, io, script, yield_now, Script, SimConfig, SimRuntime, SimTask, Step};
use rt_types::TaskKind;

// ---------------------------------------------------------------------------------------------
// SCH-05: bound tasks resume only on their Machine
// ---------------------------------------------------------------------------------------------

/// SCH-05: a Deno task started on M0 then waits on I/O; P0 is busy, P1 idle; when the I/O
/// completes the task resumes only on M0.
#[test]
fn ac_sch_05_bound_deno_task_resumes_only_on_its_machine() {
    let mut sim = sim(2, 1);
    let ps = sim.procs_of(TaskKind::DENO);
    let (p0, p1) = (ps[0], ps[1]);

    let mut a = admitted(sim.submit_to_proc(p0, d(1).finish(), script![io(10), cpu(1)]));
    sim.step_proc(p0); // A starts on M0 and waits on I/O
    let m0 = start_machine(&sim.trace(), a.id).expect("A started");
    assert_eq!(
        sim.snapshot().m(m0).bound_count,
        1,
        "A is bound to M0 (its isolate lives there)"
    );
    assert!(sim.outcome(&mut a).is_none());

    // P0 stays busy with other work while P1 is idle.
    let mut others: Vec<SimTask> = (0..3)
        .map(|_| admitted(sim.submit_existing_to_proc(p0, d(2).finish())))
        .collect();

    sim.advance(ms(10)); // the I/O completes
    assert!(sim.outcome(&mut a).is_none(), "nobody has resumed A yet");

    for _ in 0..4 {
        sim.step_proc(p1); // the idle P searches: it may steal the others, never A
    }
    assert!(
        sim.outcome(&mut a).is_none(),
        "P1 must not resume A: its isolate is on M0"
    );
    for (_, machine, _) in picks_of(&sim.trace(), a.id) {
        assert_eq!(machine, m0, "A was only ever picked by M0");
    }

    for _ in 0..10 {
        if sim.outcome(&mut a).is_some() {
            break;
        }
        sim.step_proc(p0);
    }
    assert_ok(sim.outcome(&mut a));
    let picks = picks_of(&sim.trace(), a.id);
    assert_eq!(picks.len(), 2, "one pick to start, one to resume");
    let (_, machine, source) = picks[1];
    assert_eq!(machine, m0, "the task resumed on M0");
    assert_eq!(
        source,
        PickSource::BoundReady,
        "bound tasks resume through M0.bound_ready"
    );
    assert_eq!(sim.snapshot().m(m0).bound_count, 0, "the isolate is gone with the task");

    sim.run_until_idle();
    for t in &mut others {
        assert_ok(sim.outcome(t));
    }
}

/// Bound tasks are resumed before new work (they hold isolate memory): `bound_ready` is checked
/// before `runnext` and the LRQ.
#[test]
fn bound_ready_tasks_are_resumed_before_queued_work() {
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let mut a = admitted(sim.submit_to_proc(p0, d(1).finish(), script![io(5), cpu(1)]));
    sim.step_proc(p0);
    let x = admitted(sim.submit_existing_to_proc(p0, d(2).finish()));
    sim.advance(ms(5)); // A's I/O is done: it sits in bound_ready; X waits in the LRQ
    sim.step_proc(p0);
    assert_ok(sim.outcome(&mut a));
    assert!(
        !sim.scripts().start_order().contains(&x.id),
        "A resumed first; X has not started yet"
    );
}

// ---------------------------------------------------------------------------------------------
// SCH-06: WASM tasks migrate
// ---------------------------------------------------------------------------------------------

/// SCH-06: a WASM task waits; its `last_p` LRQ is full; the I/O completes -> the task goes to the
/// GRQ and may resume on any WASM P.
#[test]
fn ac_sch_06_wasm_wake_goes_to_grq_when_last_p_lrq_is_full() {
    let mut sim = SimRuntime::new(fixed(1, 2).sched(|s| s.lrq_capacity = 4));
    let ws = sim.procs_of(TaskKind::WASM);
    let (wa, wb) = (ws[0], ws[1]);

    let mut wt = admitted(sim.submit_to_proc(wa, w(1).finish(), script![cpu(1), io(10), cpu(1)]));
    sim.step_proc(wa); // runs on `wa` (its last_p), then waits
    assert!(sim.outcome(&mut wt).is_none());
    let mut fillers: Vec<SimTask> = (0..4)
        .map(|_| admitted(sim.submit_existing_to_proc(wa, w(2).finish())))
        .collect();
    assert_eq!(sim.snapshot().lrq(wa), 4, "`wa`'s LRQ is full");
    let grq_before = sim.snapshot().grq_len(TaskKind::WASM);

    sim.advance(ms(10)); // I/O completes
    let snap = sim.snapshot();
    assert_eq!(
        snap.grq_len(TaskKind::WASM),
        grq_before + 1,
        "the woken task went to GRQ[wasm]"
    );
    assert_eq!(snap.lrq(wa), 4, "the full LRQ is untouched");
    assert_eq!(snap.lrq(wb), 0);
    assert_eq!(wt.state(), TaskState::Runnable);

    // It may resume on any WASM P: the other one takes it from the GRQ.
    for _ in 0..20 {
        if sim.outcome(&mut wt).is_some() {
            break;
        }
        sim.step_proc(wb);
    }
    assert_ok(sim.outcome(&mut wt));
    let picks = picks_of(&sim.trace(), wt.id);
    assert_eq!(picks.first().map(|p| p.0), Some(wa));
    assert_eq!(picks.last().map(|p| p.0), Some(wb), "resumed on a different WASM P");

    sim.run_until_idle();
    for t in &mut fillers {
        assert_ok(sim.outcome(t));
    }
}

#[test]
fn ac_sch_06_wasm_wake_prefers_last_p_when_not_full() {
    let mut sim = SimRuntime::new(fixed(1, 2).sched(|s| s.lrq_capacity = 4));
    let ws = sim.procs_of(TaskKind::WASM);
    let (wa, wb) = (ws[0], ws[1]);
    let mut wt = admitted(sim.submit_to_proc(wa, w(1).finish(), script![cpu(1), io(10), cpu(1)]));
    sim.step_proc(wa);
    sim.advance(ms(10));
    let snap = sim.snapshot();
    assert_eq!(snap.lrq(wa), 1, "woken into last_p's LRQ");
    assert_eq!(snap.lrq(wb), 0);
    assert_eq!(snap.grq_len(TaskKind::WASM), 0);
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut wt));
}

// ---------------------------------------------------------------------------------------------
// SCH-07: no lost wakeups (10,000 seeds)
// ---------------------------------------------------------------------------------------------

fn random_script(rng: &mut StdRng) -> Script {
    let steps: Vec<Step> = (0..rng.gen_range(1..=5))
        .map(|_| match rng.gen_range(0..6) {
            0 => cpu(1),
            1 => Step::Io(Duration::ZERO), // completes before the poll returns: wake races the park
            2 => Step::Io(us(rng.gen_range(1..3_000))),
            3 | 4 => yield_now(), // self-wake while Running
            _ => Step::Cpu(us(rng.gen_range(1..800))),
        })
        .collect();
    Script::from(steps)
}

fn run_seed(seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut cfg = SimConfig::default().seed(seed).fixed_split(2, 2);
    cfg.keep_trace = false;
    let mut sim = SimRuntime::new(cfg);

    let n = rng.gen_range(8..=16u128);
    let mut tasks: Vec<SimTask> = Vec::new();
    for i in 0..n {
        let spec = if rng.gen_bool(0.5) { d(10 + i) } else { w(10 + i) }.finish();
        tasks.push(admitted(sim.submit(spec, random_script(&mut rng))));
    }

    let mut guard = 0u32;
    loop {
        match sim.step() {
            StepResult::Progressed => {
                if rng.gen_bool(0.3) {
                    sim.advance(us(rng.gen_range(0..400)));
                }
            }
            StepResult::Idle { next_timer_in } => sim.advance(next_timer_in.max(Duration::from_nanos(1))),
            StepResult::Quiescent => break,
        }
        guard += 1;
        assert!(
            guard < 200_000,
            "seed {seed}: the simulation does not quiesce (a task hangs)"
        );
    }

    for (i, t) in tasks.iter_mut().enumerate() {
        match sim.outcome(t) {
            Some(rt_types::Outcome::Ok(_)) => {}
            other => panic!("seed {seed}: task #{i} did not complete: {other:?}"),
        }
    }
    let snap = sim.snapshot();
    assert_eq!(snap.tasks_submitted, n as u64, "seed {seed}");
    assert_eq!(snap.tasks_completed, n as u64, "seed {seed}: a task was lost");
    assert_eq!(
        (snap.inflight, snap.waiting_runtime, snap.waiting_memory),
        (0, 0, 0),
        "seed {seed}"
    );
    assert_eq!((snap.grq.deno, snap.grq.wasm, snap.grq.build), (0, 0, 0), "seed {seed}");
}

/// SCH-07: SimRuntime, 10,000 seeds, wakes racing with `Running -> Waiting` -> every task
/// completes; none is lost, none hangs.
#[test]
fn ac_sch_07_no_lost_wakeups_over_10k_seeds() {
    // SCH-09 measures *process-wide* idle CPU (< 1 %); this CPU-heavy simulation must not run
    // inside that window.
    let _serial = serial();
    for seed in 0..10_000u64 {
        run_seed(seed);
    }
}

#[test]
fn ac_sch_07_self_wake_while_running_requeues_instead_of_parking() {
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let mut t = admitted(sim.submit_to_proc(
        p0,
        d(1).finish(),
        script![yield_now(), yield_now(), yield_now(), cpu(1)],
    ));
    for _ in 0..50 {
        if sim.outcome(&mut t).is_some() {
            break;
        }
        sim.step_proc(p0);
    }
    assert_ok(sim.outcome(&mut t));
    assert_eq!(sim.clock().pending_timers(), 0);
}

// ---------------------------------------------------------------------------------------------
// SCH-09: idle machines park (real threads)
// ---------------------------------------------------------------------------------------------

fn all_parked(real: &Real) -> bool {
    let snap = real.sched.snapshot();
    snap.spinning == 0 && snap.machines.iter().all(|m| m.parked)
}

/// SCH-09: no work for more than `spin_us`; over 10 s of idleness every Machine is parked and
/// the process burns < 1 % CPU.
#[test]
fn ac_sch_09_idle_node_parks_every_machine_and_burns_no_cpu() {
    let _serial = serial();
    let real = build_real(real_cfg(2, 2));

    // Wake every Machine with a burst of work first.
    let sched = real.sched.clone();
    block_on(async move {
        let handles: Vec<_> = (0..100)
            .flat_map(|i| {
                [
                    sched.submit(d(2 + i % 3).finish()),
                    sched.submit(w(10 + i % 3).finish()),
                ]
            })
            .map(|r| r.expect("admitted"))
            .collect();
        for out in futures_util::future::join_all(handles).await {
            assert!(out.is_ok(), "{out:?}");
        }
    });
    assert!(
        wait_until(ms(500), || all_parked(&real)),
        "Machines did not park after spin_us: {:?}",
        real.sched.snapshot()
    );

    let cpu0 = process_cpu_time();
    let t0 = std::time::Instant::now();
    std::thread::sleep(Duration::from_secs(10));
    let (cpu, wall) = (process_cpu_time() - cpu0, t0.elapsed());

    assert!(
        all_parked(&real),
        "a Machine woke up during the idle period: {:?}",
        real.sched.snapshot()
    );
    assert!(
        cpu.as_secs_f64() < wall.as_secs_f64() * 0.01,
        "idle process used {cpu:?} CPU in {wall:?} (>= 1 %)"
    );
    real.shutdown();
}

// ---------------------------------------------------------------------------------------------
// SCH-10: wake latency from all-parked (real threads, perf)
// ---------------------------------------------------------------------------------------------

fn wake_latencies(live: LiveSchedConfig, iterations: usize) -> Vec<Duration> {
    let _serial = serial();
    let real = build_real(SchedConfig { live, ..real_cfg(2, 2) });
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        assert!(wait_until(ms(500), || all_parked(&real)), "Machines never parked");
        let mut handle = admitted(real.sched.submit(d(2).finish()));
        let out = rt.block_on(&mut handle);
        assert!(out.is_ok(), "{out:?}");
        samples.push(handle.timings().queue); // submit -> start, includes the unpark
    }
    real.shutdown();
    samples
}

/// SCH-10 (balanced): all Ps parked, one task submitted -> it starts within p99 200 us.
#[test]
#[ignore = "perf"]
fn ac_sch_10_wake_latency_balanced_p99_200us() {
    let p = p99(wake_latencies(LiveSchedConfig::default(), 2_000));
    assert!(p <= us(200), "balanced wake latency p99 = {p:?}");
}

/// SCH-10 (min_latency): p99 50 us.
#[test]
#[ignore = "perf"]
fn ac_sch_10_wake_latency_min_latency_p99_50us() {
    let live = LiveSchedConfig {
        spin: us(50),
        time_slice: ms(5),
        sysmon_max_tick: ms(2),
        rebalance_interval: ms(50),
        handoff_on_hog: true,
        max_bound_per_m: 64,
    };
    let p = p99(wake_latencies(live, 2_000));
    assert!(p <= us(50), "min_latency wake latency p99 = {p:?}");
}
