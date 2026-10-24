//! The real-thread driver: one OS thread per Machine (current-thread tokio runtime + `LocalSet`)
//! and one sysmon thread.
//!
//! The scheduler decides *which Machine starts a task*; after `start` the executor's future is
//! spawned on that Machine's `LocalSet` and runs to completion there ([`crate::executor`]).
//! A Machine parks by awaiting its [`tokio::sync::Notify`] inside the runtime, so the same thread
//! keeps driving timers and I/O of its bound tasks while it has no P work.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{fence, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rt_types::{KillReason, Outcome, ProcId};

use crate::clock::ThreadCpuClock;
use crate::executor::{TaskFuture, BETWEEN_TASKS_BUDGET, BETWEEN_TASKS_EVERY, BETWEEN_TASKS_EVERY_BUSY};
use crate::inner::{LiveTask, MachineRt, SchedInner};
use crate::machine::Machine;
use crate::monitor::SysmonState;
use crate::pick::Begun;
use crate::scheduler::SchedError;
use crate::task::{Task, TaskState};
use crate::trace::PickSource;

/// Spawn one thread per Machine plus the sysmon thread.
pub(crate) fn spawn(core: &Arc<SchedInner>) -> Result<(), SchedError> {
    let mut handles = Vec::with_capacity(core.machines.len() + 1);
    let mut failure: Option<std::io::Error> = None;
    for idx in 0..core.machines.len() {
        let c = core.clone();
        match std::thread::Builder::new()
            .name(format!("rt-m{idx}"))
            .spawn(move || machine_main(c, idx))
        {
            Ok(h) => handles.push(h),
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }
    if failure.is_none() {
        let c = core.clone();
        match std::thread::Builder::new()
            .name("rt-sysmon".into())
            .spawn(move || sysmon_main(c))
        {
            Ok(h) => {
                *core.sysmon_thread.lock() = Some(h.thread().clone());
                handles.push(h);
            }
            Err(e) => failure = Some(e),
        }
    }
    if failure.is_none() {
        let c = core.clone();
        match std::thread::Builder::new()
            .name("rt-autoscale".into())
            .spawn(move || autoscale_main(c))
        {
            Ok(h) => {
                *core.autoscale_thread.lock() = Some(h.thread().clone());
                handles.push(h);
            }
            Err(e) => failure = Some(e),
        }
    }
    if let Some(e) = failure {
        core.stop.store(true, Ordering::Release);
        for m in &core.machines {
            m.notify.notify_one();
        }
        return Err(SchedError::Spawn(e));
    }
    *core.threads.lock() = handles;
    Ok(())
}

/// Linux only: `sched.pin_threads` pins guest Machines to cores, build Machines get `nice`.
#[cfg(target_os = "linux")]
fn tune_thread(core: &SchedInner, idx: usize) {
    if core.cfg.pin_threads {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from);
        // SAFETY: `set` is a plain bit set; `sched_setaffinity(0, ..)` only affects this thread.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(idx % cpus, &mut set);
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        }
    }
    let is_build = core.machines[idx]
        .proc()
        .is_some_and(|p| core.proc(p).kind() == rt_types::TaskKind::Build);
    if is_build && core.cfg.build_nice != 0 {
        // SAFETY: `setpriority(PRIO_PROCESS, 0, ..)` on Linux targets the calling thread only.
        unsafe {
            libc::setpriority(libc::PRIO_PROCESS, 0, core.cfg.build_nice);
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn tune_thread(_core: &SchedInner, _idx: usize) {}

fn machine_main(core: Arc<SchedInner>, idx: usize) {
    tune_thread(&core, idx);
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(machine = idx, error = %e, "machine could not build its runtime");
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    let m = core.machines[idx].clone();
    crate::warm::set_machine_rank(u32::try_from(idx).ok());
    // sysmon reads this thread's CPU clock for the poll in progress; only when it is the clock
    // the scheduler's `Clock` measures polls with (the OS thread clock of `SystemClock`).
    if let Some(c) = ThreadCpuClock::current() {
        let same = c
            .read()
            .is_some_and(|os| os.abs_diff(core.clock.thread_cpu_time()) < Duration::from_millis(20));
        if same {
            let _ = m.cpu_clock.set(c);
        }
    }
    local.block_on(&rt, machine_loop(core, m));
}

/// Start (or resume) the task `find_runnable` returned.
fn run_picked(core: &Arc<SchedInner>, m: &Arc<Machine>, mrt: &mut MachineRt, task: Arc<Task>, src: PickSource) {
    let pid = m.proc().unwrap_or(ProcId(u32::MAX));
    if let Begun::Started(fut, live) = core.begin(&task, m, pid, mrt, src) {
        tokio::task::spawn_local(run_started(core.clone(), m.clone(), live, fut));
    }
}

async fn machine_loop(core: Arc<SchedInner>, m: Arc<Machine>) {
    let mut mrt = MachineRt::new();
    // Picks since the executors last housekept (`Executor::between_tasks`).
    let mut streak = 0u32;
    loop {
        if core.stop.load(Ordering::Acquire) {
            break;
        }
        // 1. Work available right now?
        if let Some((task, src)) = core.find_runnable(&m, false) {
            run_picked(&core, &m, &mut mrt, task, src);
            // Let the new task (and tasks woken by I/O) run their first polls.
            tokio::task::yield_now().await;
            // A Machine that always finds work never parks: housekeep every few picks anyway,
            // rarely while tasks wait in its own queue (a unit can take milliseconds).
            streak += 1;
            let period = if core.local_work(&m) {
                BETWEEN_TASKS_EVERY_BUSY
            } else {
                BETWEEN_TASKS_EVERY
            };
            if streak >= period {
                streak = 0;
                mrt.between_tasks(BETWEEN_TASKS_BUDGET);
            }
            continue;
        }
        // 1b. Nothing to run right now: one bounded unit of deferred executor work (refill the
        // isolate pool, dispose a finished isolate) before spinning. Under load a Machine rarely
        // parks, so this, not `on_idle`, is what keeps the pool warm. It is not searching while
        // it does that: drop the spinner role so `wakep` can wake another Machine meanwhile.
        streak = 0;
        core.stop_spinning(&m);
        mrt.between_tasks(BETWEEN_TASKS_BUDGET);
        // 2. Spin for `sched.spin_us`, re-checking.
        let spin = core.live.load().spin;
        let began = std::time::Instant::now();
        let mut found = None;
        while began.elapsed() < spin && !core.stop.load(Ordering::Acquire) {
            std::hint::spin_loop();
            tokio::task::yield_now().await;
            found = core.find_runnable(&m, false);
            if found.is_some() {
                break;
            }
        }
        if let Some((task, src)) = found {
            run_picked(&core, &m, &mut mrt, task, src);
            streak += 1;
            tokio::task::yield_now().await;
            continue;
        }
        // 3. Park: announce, re-check everything once more (lost-wakeup guard), then sleep.
        core.mark_idle(&m);
        mrt.on_idle(Duration::from_micros(500));
        m.parked.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if let Some((task, src)) = core.find_runnable(&m, false) {
            m.parked.store(false, Ordering::SeqCst);
            run_picked(&core, &m, &mut mrt, task, src);
            streak += 1;
            tokio::task::yield_now().await;
            continue;
        }
        // The re-check ran `find_runnable`, whose steal step may have marked this M spinning again
        // (flag, per-kind count and P status). A parked spinner blocks every `wakep` and the
        // lost-wakeup guard, so its LRQ-queued work would never be picked up: undo it.
        core.mark_idle(&m);
        m.notify.notified().await;
        m.parked.store(false, Ordering::SeqCst);
    }
}

/// A started task's future with per-poll accounting (thread CPU time, `Running`/`Waiting`).
struct Tracked {
    core: Arc<SchedInner>,
    m: Arc<Machine>,
    live: Arc<LiveTask>,
    fut: TaskFuture,
}

impl Future for Tracked {
    type Output = Outcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome> {
        let this = &mut *self;
        let core = &this.core;
        let task = &this.live.task;
        task.shared.state.store(TaskState::Running);
        let cpu0 = core.clock.thread_cpu_time();
        this.m
            .begin_poll(this.live.id, this.live.kind, core.rel_ns(core.now()), cpu0);
        let result = catch_unwind(AssertUnwindSafe(|| this.fut.as_mut().poll(cx)));
        let burn = core.clock.thread_cpu_time().saturating_sub(cpu0);
        this.m.end_poll();

        this.live
            .cpu_ns
            .fetch_add(burn.as_nanos().min(u128::from(u64::MAX / 4)) as u64, Ordering::AcqRel);
        if let Some(p) = this.m.proc() {
            core.proc(p)
                .stats
                .busy_ns
                .fetch_add(burn.as_nanos() as u64, Ordering::Relaxed);
        }
        {
            let mut t = task.shared.timings.lock();
            t.polls += 1;
            t.cpu += burn;
        }
        match result {
            Ok(Poll::Ready(outcome)) => Poll::Ready(outcome),
            Ok(Poll::Pending) => {
                task.shared.state.store(TaskState::Waiting);
                task.shared.timings.lock().waits += 1;
                if this.live.cpu_used() >= this.live.cpu_limit {
                    let _ = core.kill_started(&this.live, KillReason::CpuLimit);
                }
                Poll::Pending
            }
            Err(_) => Poll::Ready(Outcome::GuestError),
        }
    }
}

async fn run_started(core: Arc<SchedInner>, m: Arc<Machine>, live: Arc<LiveTask>, fut: TaskFuture) {
    let outcome = {
        let mut tracked = Tracked {
            core: core.clone(),
            m,
            live: live.clone(),
            fut,
        };
        tokio::select! {
            biased;
            () = live.cancel.notified() => {
                let reason = (*live.cancel_reason.lock()).unwrap_or(KillReason::Shutdown);
                Outcome::Killed(reason)
            }
            o = &mut tracked => o,
        }
        // `tracked` (and with it the reservation) is dropped here, before the outcome is delivered.
    };
    core.finish(&live.task, outcome, Some(&live), false);
}

/// The autoscaler (adaptive limits, warm targets) every `rebalance.interval`, off the sysmon
/// thread: its tick is O(services).
fn autoscale_main(core: Arc<SchedInner>) {
    while !core.stop.load(Ordering::Acquire) {
        let live = core.live.load_full();
        std::thread::park_timeout(live.rebalance_interval.max(Duration::from_millis(1)));
        if core.stop.load(Ordering::Acquire) {
            break;
        }
        core.autoscale(core.now(), &live);
    }
}

fn sysmon_main(core: Arc<SchedInner>) {
    let mut st = SysmonState::new(&core);
    while !core.stop.load(Ordering::Acquire) {
        let now = core.now();
        let wake = st.next_wake();
        if wake > now {
            std::thread::park_timeout(wake - now);
        }
        if core.stop.load(Ordering::Acquire) {
            break;
        }
        core.sysmon_run(&mut st);
    }
}
