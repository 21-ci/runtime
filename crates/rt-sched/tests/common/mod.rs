//! Shared helpers for the rt-sched integration tests (`mod common;` in each test file).
//!
//! Conventions used by every suite:
//! - `fixed(d, w)` = `d` Deno + `w` WASM processors with `min == max` (no re-kinding), seeded;
//! - `d(n)` / `w(n)` / `b(n)` = spec builders for service `n` (Deno / WASM / Build) with
//!   service concurrency limits high enough not to interfere;
//! - scripts are per *service*: use different service numbers for tasks that behave differently.
#![allow(dead_code)]

use std::future::Future;
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use rt_sched::trace::{PickSource, TraceEvent};
use rt_sched::{KindRange, SchedConfig, Scheduler, SchedulerBuilder, SystemClock, UnitSource};
use rt_testkit::{
    MockBuildExecutor, MockDenoExecutor, MockPreparedSource, MockWasmExecutor, ScriptBook, SimConfig, SimRuntime,
    SpecBuilder, TestGovernor,
};
use rt_types::{
    KillReason, MachineId, MachineSnapshot, Outcome, PreparedKey, ProcId, ProcSnapshot, RejectReason, SchedSnapshot,
    TaskId, TaskKind, TaskSpec, Trigger,
};

pub const MIB: u64 = 1024 * 1024;
/// Service-level concurrency/queue limit that never triggers.
pub const BIG: u32 = 1_000_000;
pub const SEED: u64 = 0x5eed_0001;

pub fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

pub fn us(n: u64) -> Duration {
    Duration::from_micros(n)
}

// ---------------------------------------------------------------------------------------------
// Spec builders
// ---------------------------------------------------------------------------------------------

pub fn d(service: u128) -> SpecBuilder {
    SpecBuilder::deno().service(service).max_concurrency(BIG, BIG)
}

pub fn w(service: u128) -> SpecBuilder {
    SpecBuilder::wasm().service(service).max_concurrency(BIG, BIG)
}

pub fn b(service: u128) -> SpecBuilder {
    SpecBuilder::build().service(service).max_concurrency(BIG, BIG)
}

/// The prepared key a guest task needs (`engine_fp` is 0 in the testkit).
pub fn key_of(spec: &TaskSpec) -> PreparedKey {
    spec.prepared_key(0).expect("guest task has a prepared key")
}

// ---------------------------------------------------------------------------------------------
// Simulation setup
// ---------------------------------------------------------------------------------------------

pub fn fixed(deno: u32, wasm: u32) -> SimConfig {
    SimConfig::default().seed(SEED).fixed_split(deno, wasm)
}

pub fn sim(deno: u32, wasm: u32) -> SimRuntime {
    SimRuntime::new(fixed(deno, wasm))
}

// ---------------------------------------------------------------------------------------------
// Snapshot / trace helpers
// ---------------------------------------------------------------------------------------------

pub trait SnapExt {
    fn p(&self, p: ProcId) -> &ProcSnapshot;
    fn lrq(&self, p: ProcId) -> u32 {
        self.p(p).lrq_len
    }
    fn grq_len(&self, kind: TaskKind) -> u32;
    fn m(&self, m: MachineId) -> &MachineSnapshot;
}

impl SnapExt for SchedSnapshot {
    fn p(&self, p: ProcId) -> &ProcSnapshot {
        self.procs
            .iter()
            .find(|x| x.id == p)
            .unwrap_or_else(|| panic!("no {p:?} in snapshot"))
    }

    fn grq_len(&self, kind: TaskKind) -> u32 {
        self.grq.get(kind)
    }

    fn m(&self, m: MachineId) -> &MachineSnapshot {
        self.machines
            .iter()
            .find(|x| x.id == m)
            .unwrap_or_else(|| panic!("no {m:?} in snapshot"))
    }
}

/// All `Pick` events of `task`: `(proc, machine, source)`.
pub fn picks_of(trace: &[TraceEvent], task: TaskId) -> Vec<(ProcId, MachineId, PickSource)> {
    trace
        .iter()
        .filter_map(|e| match e {
            TraceEvent::Pick {
                task: t,
                proc,
                machine,
                from,
            } if *t == task => Some((*proc, *machine, *from)),
            _ => None,
        })
        .collect()
}

/// The Machine `task` was started on.
pub fn start_machine(trace: &[TraceEvent], task: TaskId) -> Option<MachineId> {
    trace.iter().find_map(|e| match e {
        TraceEvent::Start { task: t, machine } if *t == task => Some(*machine),
        _ => None,
    })
}

pub fn count_events(trace: &[TraceEvent], pred: impl Fn(&TraceEvent) -> bool) -> usize {
    trace.iter().filter(|e| pred(e)).count()
}

/// Virtual time of the first event matching `pred`.
pub fn time_of(timed: &[(Duration, TraceEvent)], pred: impl Fn(&TraceEvent) -> bool) -> Option<Duration> {
    timed.iter().find(|(_, e)| pred(e)).map(|(t, _)| *t)
}

/// Position of each id in `order` (panics if missing): used to compare start orders.
pub fn positions(order: &[TaskId], ids: &[TaskId]) -> Vec<usize> {
    ids.iter()
        .map(|id| {
            order
                .iter()
                .position(|x| x == id)
                .unwrap_or_else(|| panic!("{id:?} never started"))
        })
        .collect()
}

pub fn is_strictly_increasing(v: &[usize]) -> bool {
    v.windows(2).all(|p| p[0] < p[1])
}

// ---------------------------------------------------------------------------------------------
// Outcome assertions
// ---------------------------------------------------------------------------------------------

pub fn assert_ok(o: Option<&Outcome>) {
    match o {
        Some(Outcome::Ok(_)) => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

pub fn assert_killed(o: Option<&Outcome>, reason: KillReason) {
    match o {
        Some(out @ Outcome::Killed(r)) => {
            assert_eq!(*r, reason, "kill reason");
            assert_eq!(out.http_status(), reason.http_status(), "http status");
            assert_eq!(out.reason(), Some(reason.reason()), "x-rt-reason");
        }
        other => panic!("expected Killed({reason:?}), got {other:?}"),
    }
}

pub fn assert_rejected(o: Option<&Outcome>, reason: RejectReason) {
    match o {
        Some(out @ Outcome::Rejected(r)) => {
            assert_eq!(*r, reason, "reject reason");
            assert_eq!(out.http_status(), reason.http_status(), "http status");
            assert_eq!(out.reason(), Some(reason.reason()), "x-rt-reason");
        }
        other => panic!("expected Rejected({reason:?}), got {other:?}"),
    }
}

/// Unwrap the rejection of a `submit` whose handle type is not `Debug`.
pub fn rejection<T>(r: Result<T, RejectReason>) -> RejectReason {
    match r {
        Ok(_) => panic!("expected the submit to be rejected"),
        Err(e) => e,
    }
}

/// Unwrap an admitted submit with a readable message.
pub fn admitted<T>(r: Result<T, RejectReason>) -> T {
    match r {
        Ok(t) => t,
        Err(e) => panic!("expected the submit to be admitted, got rejection `{}`", e.reason()),
    }
}

/// 99th percentile (nearest rank) of `v`.
pub fn p99(mut v: Vec<Duration>) -> Duration {
    assert!(!v.is_empty(), "p99 of nothing");
    v.sort();
    let rank = ((v.len() as f64) * 0.99).ceil() as usize;
    v[rank.clamp(1, v.len()) - 1]
}

// ---------------------------------------------------------------------------------------------
// Real-thread scheduler (SCH-08/09/10/13/22/23)
// ---------------------------------------------------------------------------------------------

/// A real-thread scheduler over mock executors, `SystemClock` and a `TestGovernor`.
pub struct Real {
    pub sched: Scheduler,
    pub book: Arc<ScriptBook>,
    pub gov: Arc<TestGovernor>,
    pub source: Arc<MockPreparedSource>,
}

impl Real {
    /// Stop the scheduler (kills what is still running) and join its threads.
    pub fn shutdown(self) {
        block_on(self.sched.shutdown(ms(500)));
    }
}

/// `deno` + `wasm` processors with fixed kinds.
pub fn real_cfg(deno: u32, wasm: u32) -> SchedConfig {
    SchedConfig {
        processors: deno + wasm,
        deno: KindRange { min: deno, max: deno },
        wasm: KindRange { min: wasm, max: wasm },
        seed: SEED,
        ..SchedConfig::default()
    }
}

pub fn build_real(cfg: SchedConfig) -> Real {
    build_real_with(cfg, |b| b)
}

pub fn build_real_with(cfg: SchedConfig, f: impl FnOnce(SchedulerBuilder) -> SchedulerBuilder) -> Real {
    let clock = Arc::new(SystemClock::new());
    let gov = TestGovernor::with_mb(4096);
    let book = ScriptBook::new();
    let source = MockPreparedSource::new();
    source.set_auto_ready(true);
    let (b1, b2, b3) = (book.clone(), book.clone(), book.clone());
    let builder = Scheduler::builder(cfg, clock, gov.clone())
        .executor(move || MockDenoExecutor::new(b1.clone()), source.clone(), 0)
        .executor(move || MockWasmExecutor::new(b2.clone()), source.clone(), 0)
        .executor(move || MockBuildExecutor::new(b3.clone()), Arc::new(UnitSource), 0);
    let sched = f(builder).build().expect("real scheduler builds");
    Real {
        sched,
        book,
        gov,
        source,
    }
}

/// Run a future on a throwaway current-thread tokio runtime (timers enabled).
pub fn block_on<F: Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(f)
}

static REAL_LOCK: Mutex<()> = Mutex::new(());

/// Real-thread, timing-sensitive tests of one test binary must not overlap: hold this guard.
pub fn serial() -> MutexGuard<'static, ()> {
    REAL_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// User + system CPU time consumed by the whole process.
pub fn process_cpu_time() -> Duration {
    let mut ru = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fully initializes the `rusage` behind the valid out-pointer.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr()) };
    assert_eq!(rc, 0, "getrusage failed");
    // SAFETY: initialized by the successful call above.
    let ru = unsafe { ru.assume_init() };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, (t.tv_usec as u32) * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// Wait (real time) until `cond()` holds or `timeout` passes; returns whether it held.
pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_micros(200));
    }
    cond()
}

/// The documented shape of one schedtrace line (SCH-22): every field must be present, in order.
pub fn assert_schedtrace_fields(line: &str) {
    let mut at = 0usize;
    for field in [
        "SCHED ",
        "ms: procs=",
        " deno=",
        " wasm=",
        " idle=",
        " spinning=",
        " grq=[deno:",
        " wasm:",
        " build:",
        "] lrq=[",
        "] bound=[",
        "] waitrt=",
        " waitmem=",
    ] {
        match line[at..].find(field) {
            Some(i) => at += i + field.len(),
            None => panic!("schedtrace line misses `{field}` (after byte {at}): {line}"),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Child spawning from inside a running mock task (SCH-24 and friends)
// ---------------------------------------------------------------------------------------------

/// A child spawned by a `call(..)` step, with the snapshot taken right after its `submit`.
pub struct Spawned {
    pub child: TaskId,
    pub snap: SchedSnapshot,
}

/// Make every `call(..)` of a mock task submit a child of service 20 (`Trigger::Call`, depth 1,
/// runtime `child_kind`). Records the snapshot taken right after each spawn.
pub fn install_call_hook(sim: &SimRuntime, child_kind: TaskKind) -> Arc<parking_lot::Mutex<Vec<Spawned>>> {
    let sched = sim.scheduler().clone();
    let log: Arc<parking_lot::Mutex<Vec<Spawned>>> = Arc::default();
    let sink = log.clone();
    sim.scripts().set_call_hook(move |parent, _slug| {
        let builder = if child_kind == TaskKind::DENO { d(20) } else { w(20) };
        let spec = builder.trigger(Trigger::Call { parent, depth: 1 }).finish();
        let h = sched.submit(spec).expect("child admitted");
        sink.lock().push(Spawned {
            child: h.id,
            snap: sched.snapshot(),
        });
    });
    log
}
