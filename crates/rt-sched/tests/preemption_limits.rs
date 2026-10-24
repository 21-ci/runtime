//! Preemption and limits: time slice, CPU limit, wall limit,
//! abort. The Deno hog handoff (SCH-14) lives in `sysmon.rs`.
//!
//! ACs: SCH-11, SCH-12, SCH-13 (simulated and real threads), SCH-15.

mod common;

use std::time::Duration;

use common::*;
use rt_sched::SchedConfig;
use rt_testkit::{alloc, cpu, ids, io, loop_forever, script, yield_now, Script, SimRuntime};
use rt_types::{KillReason, TaskKind};

// ---------------------------------------------------------------------------------------------
// SCH-11: time slice
// ---------------------------------------------------------------------------------------------

/// SCH-11: a WASM infinite loop on P0 (slice 10 ms) and a second task queued on P0 -> the second
/// task starts within 20 ms.
#[test]
fn ac_sch_11_wasm_hog_does_not_starve_a_queued_task() {
    let mut sim = sim(1, 1); // balanced time slice: 10 ms
    let p0 = sim.procs_of(TaskKind::WASM)[0];
    let mut hog = admitted(sim.submit_to_proc(p0, w(1).finish(), script![loop_forever()]));
    let queued_at = sim.clock().elapsed();
    let mut second = admitted(sim.submit_existing_to_proc(p0, w(2).finish()));

    sim.run_for(ms(100));

    assert_ok(sim.outcome(&mut second));
    assert!(sim.outcome(&mut hog).is_none(), "the infinite loop is still running");
    let started = time_of(
        &sim.trace_timed(),
        |e| matches!(e, rt_sched::trace::TraceEvent::Start { task, .. } if *task == second.id),
    )
    .expect("the second task started");
    let waited = started.saturating_sub(queued_at);
    assert!(
        waited <= ms(20),
        "the queued task waited {waited:?} behind the hog (> 20 ms)"
    );
}

// ---------------------------------------------------------------------------------------------
// SCH-12 / SCH-13: CPU limit
// ---------------------------------------------------------------------------------------------

fn cpu_limit_kills_busy_loop(kind: TaskKind) {
    let mut sim = sim(1, 1);
    let spec = |service: u128, limit: Option<u64>| {
        let b = if kind == TaskKind::DENO { d(service) } else { w(service) };
        match limit {
            Some(ms) => b.cpu_limit(ms).finish(),
            None => b.finish(),
        }
    };

    let mut hog = admitted(sim.submit(spec(1, Some(50)), script![loop_forever()]));
    sim.run_until_idle();

    let out = sim.outcome(&mut hog).expect("the busy loop was killed");
    assert_killed(Some(out), KillReason::CpuLimit);
    assert_eq!(out.http_status(), 502);
    assert_eq!(out.reason(), Some("cpu_limit"));
    let cpu = hog.timings().cpu;
    assert!(cpu >= ms(50), "killed before the limit: {cpu:?}");
    assert!(cpu <= ms(60), "killed at {cpu:?} of CPU (> 60 ms)");

    // The Machine serves the next task.
    let mut next = admitted(sim.submit(spec(2, None), Script::noop()));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut next));
}

/// SCH-12: a WASM busy loop with `cpu_ms=50` is killed at <= 60 ms CPU with
/// `502 x-rt-reason: cpu_limit`, and its Machine serves the next task.
#[test]
fn ac_sch_12_wasm_busy_loop_is_killed_at_the_cpu_limit() {
    cpu_limit_kills_busy_loop(TaskKind::WASM);
}

/// SCH-13 (simulated): same contract for a Deno-kind busy loop.
#[test]
fn ac_sch_13_deno_busy_loop_is_killed_at_the_cpu_limit_sim() {
    cpu_limit_kills_busy_loop(TaskKind::DENO);
}

#[test]
fn ac_sch_12_a_task_below_its_cpu_limit_is_not_killed() {
    for kind in [TaskKind::WASM, TaskKind::DENO] {
        let mut sim = sim(1, 1);
        let b = if kind == TaskKind::DENO { d(1) } else { w(1) };
        let mut t = admitted(sim.submit(b.cpu_limit(50).finish(), script![cpu(30), yield_now(), cpu(15)]));
        sim.run_until_idle();
        assert_ok(sim.outcome(&mut t));
        assert!(t.timings().cpu >= ms(45));
    }
}

/// SCH-13 (real threads): Deno `while(true){}` with `cpu_ms=50` is terminated at <= 60 ms CPU
/// with `502 x-rt-reason: cpu_limit`, and the same M serves the next task.
#[test]
fn ac_sch_13_deno_busy_loop_is_terminated_and_the_machine_serves_the_next_task() {
    let _serial = serial();
    let real = build_real(SchedConfig {
        spare_machines: 0,
        ..real_cfg(1, 0)
    });
    real.book.set_for_service(ids::service(1), script![loop_forever()]);
    let machines_before = real.sched.snapshot().machines.len();

    let mut hog = admitted(real.sched.submit(d(1).cpu_limit(50).finish()));
    let out = block_on(async { tokio::time::timeout(Duration::from_secs(10), &mut hog).await })
        .expect("the busy loop must be terminated, not run forever");
    assert_killed(Some(&out), KillReason::CpuLimit);
    assert_eq!(out.http_status(), 502);
    assert_eq!(out.reason(), Some("cpu_limit"));
    let cpu = hog.timings().cpu;
    assert!(cpu >= ms(50), "terminated before the limit: {cpu:?}");
    assert!(cpu <= ms(60), "terminated after {cpu:?} of CPU (> 60 ms)");

    let mut next = admitted(real.sched.submit(d(2).finish()));
    let out = block_on(async { tokio::time::timeout(Duration::from_secs(5), &mut next).await })
        .expect("the same M serves the next task");
    assert_ok(Some(&out));
    assert_eq!(
        real.sched.snapshot().machines.len(),
        machines_before,
        "no Machine was lost or replaced"
    );
    real.shutdown();
}

// ---------------------------------------------------------------------------------------------
// SCH-15: wall limit
// ---------------------------------------------------------------------------------------------

fn wall_limit_kills_waiting_task(kind: TaskKind) {
    let mut sim = SimRuntime::new(fixed(1, 1).memory_mb(256));
    let baseline = sim.governor().committed();
    let b = if kind == TaskKind::DENO { d(1) } else { w(1) };
    let mut t = admitted(sim.submit(b.wall_limit(100).finish(), script![alloc(8), io(10_000)]));

    sim.run_for(ms(50));
    assert!(sim.outcome(&mut t).is_none(), "{kind}: still waiting on I/O");
    assert!(
        sim.governor().committed() >= baseline + 8 * MIB,
        "{kind}: the task holds its reservation while waiting"
    );

    sim.run_until_idle();
    assert_killed(sim.outcome(&mut t), KillReason::WallTimeout); // 504 x-rt-reason: timeout
    assert_eq!(sim.outcome(&mut t).and_then(|o| o.reason()), Some("timeout"));
    assert_eq!(sim.outcome(&mut t).map(|o| o.http_status()), Some(504));
    assert_eq!(
        sim.governor().committed(),
        baseline,
        "{kind}: all reservations released"
    );
    let elapsed = sim.clock().elapsed();
    assert!(elapsed >= ms(100), "{kind}: killed before wall_ms ({elapsed:?})");
    assert!(
        elapsed <= ms(150),
        "{kind}: the deadline was enforced late ({elapsed:?})"
    );
    assert_eq!(
        sim.clock().pending_timers(),
        0,
        "{kind}: the abandoned I/O timer is gone"
    );
}

/// SCH-15: a task waiting on I/O past `wall_ms` -> `504 x-rt-reason: timeout`, and the governor's
/// committed bytes return to the baseline.
#[test]
fn ac_sch_15_wall_deadline_kills_a_waiting_deno_task_and_releases_memory() {
    wall_limit_kills_waiting_task(TaskKind::DENO);
}

#[test]
fn ac_sch_15_wall_deadline_kills_a_waiting_wasm_task_and_releases_memory() {
    wall_limit_kills_waiting_task(TaskKind::WASM);
}

#[test]
fn ac_sch_15_the_wall_clock_starts_at_task_start_not_at_enqueue() {
    // 1 WASM P: B waits 150 ms in the queue behind A, then runs 60 ms with wall_ms = 100: it must
    // finish (queue wait is excluded from the wall limit).
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::WASM)[0];
    let mut a = admitted(sim.submit_to_proc(p0, w(1).finish(), script![io(150)]));
    let mut bt = admitted(sim.submit_to_proc(p0, w(2).wall_limit(100).finish(), script![io(60)]));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut a));
    assert_ok(sim.outcome(&mut bt));
}

// ---------------------------------------------------------------------------------------------
// TaskHandle::abort
// ---------------------------------------------------------------------------------------------

#[test]
fn abort_kills_a_running_task_with_the_requested_reason() {
    let mut sim = sim(1, 1);
    let mut t = admitted(sim.submit(w(1).finish(), script![loop_forever()]));
    sim.run_for(ms(5));
    assert!(sim.outcome(&mut t).is_none());
    t.abort(KillReason::Shutdown);
    sim.run_for(ms(20));
    assert_killed(sim.outcome(&mut t), KillReason::Shutdown); // 503 x-rt-reason: restarting
}

#[test]
fn abort_kills_a_waiting_task_promptly() {
    let mut sim = SimRuntime::new(fixed(1, 1).memory_mb(64));
    let mut t = admitted(sim.submit(d(1).finish(), script![alloc(4), io(10_000)]));
    sim.run_for(ms(5));
    t.abort(KillReason::Shutdown);
    sim.run_for(ms(30));
    assert_killed(sim.outcome(&mut t), KillReason::Shutdown);
    assert_eq!(sim.governor().committed(), 0, "the aborted task released its memory");
}
