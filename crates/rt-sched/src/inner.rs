//! Shared scheduler state ([`SchedInner`]) and the building blocks every driver uses.
//!
//! Two drivers sit on top of this state:
//! - the real-thread driver (`real.rs`): one OS thread per Machine plus the sysmon thread;
//! - the deterministic simulation driver (`sim.rs`): everything on one thread with virtual time.
//!
//! Placement, admission, finding work, stealing, waiting lists, kills and sysmon duties live in
//! sibling modules as `impl SchedInner` blocks so both drivers run *the same* decision code.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use rt_mem::{MemCategory, MemoryGovernor};
use rt_obs::{LogSink, Metrics, StatsStore};
use rt_types::{DegradationMode, GrqLens, MachineId, PreparedKey, ProcId, ProcStatus, SchedSnapshot, TaskId, TaskKind};

use crate::admission::{Admission, AdmissionLimits};
use crate::clock::Clock;
use crate::config::{LiveSchedConfig, SchedConfig};
use crate::executor::Interrupter;
use crate::machine::Machine;
use crate::processor::Processor;
use crate::queues::GlobalQueue;
use crate::sched_metrics::SchedMetrics;
use crate::scheduler::{ErasedExecutor, KindRegistration, SchedError};
use crate::task::Task;
use crate::trace::{TraceEvent, TraceRecorder};
use crate::warm::WarmTargets;

pub(crate) const MIB: u64 = 1024 * 1024;
/// Memory taken when a Deno task starts (`deno.isolate_base_mb`).
pub const DENO_ISOLATE_BASE_MB: u64 = 8;
/// Memory taken when a WASM task starts (declared initial memory).
pub const WASM_INITIAL_MB: u64 = 2;
/// Memory taken when a build task starts.
pub const BUILD_BASE_MB: u64 = 16;
/// `memory.check_interval`.
pub(crate) const MEMORY_CHECK_INTERVAL: Duration = Duration::from_millis(250);
/// Sysmon stats duty cadence.
pub(crate) const STATS_INTERVAL: Duration = Duration::from_secs(1);
/// Shards of the pending-task deadline index.
pub(crate) const DEADLINE_SHARDS: usize = 16;

/// `RT_SCHEDTRACE`: interval and sink of the schedtrace lines.
pub(crate) type SchedtraceSink = Option<(Duration, Arc<dyn Fn(&str) + Send + Sync>)>;
/// One shard of the queue-timeout index.
pub(crate) type DeadlineShard = Mutex<BTreeMap<(Instant, TaskId), Arc<Task>>>;

/// Bytes reserved for a task of `kind` whose limit is `limit_mb`.
pub(crate) fn reserve_bytes(kind: TaskKind, limit_mb: u32) -> u64 {
    let base = match kind {
        TaskKind::Exec(rt_types::RuntimeKind::Deno) => DENO_ISOLATE_BASE_MB,
        TaskKind::Exec(rt_types::RuntimeKind::Wasm) => WASM_INITIAL_MB,
        TaskKind::Build => BUILD_BASE_MB,
    };
    base.min(u64::from(limit_mb).max(1)) * MIB
}

pub(crate) fn mem_category(kind: TaskKind) -> MemCategory {
    match kind {
        TaskKind::Exec(rt_types::RuntimeKind::Deno) => MemCategory::DenoHeap,
        TaskKind::Exec(rt_types::RuntimeKind::Wasm) => MemCategory::WasmLinear,
        TaskKind::Build => MemCategory::Other,
    }
}

/// Bounds of the `Retry-After` hint.
pub(crate) const RETRY_AFTER_MIN: Duration = Duration::from_secs(1);
pub(crate) const RETRY_AFTER_MAX: Duration = Duration::from_secs(60);

/// Completion-rate sampler of the `Retry-After` hint.
#[derive(Debug, Default)]
pub(crate) struct DrainRate {
    pub at: Option<Instant>,
    pub completed: u64,
    /// Tasks per second (EWMA).
    pub rate: f64,
}

/// Log2 histogram of run-queue waits (bucket `b` holds waits below `2^b` µs), drained by the
/// re-kinding duty into the p90 EWMA that re-kinding reports and the autoscaler's CPU-saturation
/// signal uses.
pub(crate) struct WaitHist {
    buckets: [AtomicU32; 32],
}

impl WaitHist {
    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU32::new(0)),
        }
    }

    pub fn record(&self, d: Duration) {
        let us = d.as_micros().min(u128::from(u32::MAX)) as u32;
        let b = (32 - us.leading_zeros()).min(31) as usize;
        self.buckets[b].fetch_add(1, Ordering::Relaxed);
    }

    /// The p90 (upper bound of its bucket) of the waits recorded since the last call, which
    /// resets the histogram. `None` when nothing was recorded.
    pub fn take_p90(&self) -> Option<Duration> {
        let counts: Vec<u32> = self.buckets.iter().map(|b| b.swap(0, Ordering::Relaxed)).collect();
        let total: u64 = counts.iter().map(|c| u64::from(*c)).sum();
        if total == 0 {
            return None;
        }
        let want = total.saturating_mul(9).div_ceil(10);
        let mut seen = 0u64;
        for (b, c) in counts.iter().enumerate() {
            seen += u64::from(*c);
            if seen >= want {
                return Some(Duration::from_micros(1u64 << b));
            }
        }
        Some(Duration::from_micros(1u64 << 31))
    }
}

/// A started task (its future lives on a Machine). Shared with sysmon for limit enforcement.
pub(crate) struct LiveTask {
    pub task: Arc<Task>,
    pub id: TaskId,
    pub kind: TaskKind,
    /// Index of the Machine the task lives on.
    pub machine: usize,
    pub interrupter: Arc<dyn Interrupter>,
    /// Wall deadline (`start + limits.wall`).
    pub deadline: Option<Instant>,
    pub cpu_limit: Duration,
    /// Thread CPU consumed by finished polls.
    pub cpu_ns: AtomicU64,
    /// Counted in `Machine::bound_count`.
    pub bound: bool,
    /// An interrupt was already sent to the executor.
    pub interrupted: AtomicBool,
    /// The task was asked to be dropped (it was not running a poll).
    pub cancelled: AtomicBool,
    pub cancel_reason: Mutex<Option<rt_types::KillReason>>,
    /// Wakes the real-thread runner so it drops the future (`select!` on `notified`).
    pub cancel: tokio::sync::Notify,
}

impl LiveTask {
    pub fn cpu_used(&self) -> Duration {
        Duration::from_nanos(self.cpu_ns.load(Ordering::Acquire))
    }
}

/// Executors of one Machine, created lazily on the Machine's own thread.
pub(crate) struct MachineRt {
    execs: [Option<Box<dyn ErasedExecutor>>; 3],
}

impl MachineRt {
    pub fn new() -> Self {
        Self {
            execs: [None, None, None],
        }
    }

    pub fn executor(&mut self, core: &SchedInner, kind: TaskKind) -> &mut dyn ErasedExecutor {
        let k = kind.idx();
        let slot = &mut self.execs[k];
        if slot.is_none() {
            let reg = core.kind_slot[k]
                .map(|i| &core.kinds[i])
                .expect("a task of an unregistered kind was started");
            *slot = Some((reg.factory)());
        }
        slot.as_deref_mut().expect("executor created above")
    }

    /// Give every created executor a chance to housekeep before the Machine parks.
    pub fn on_idle(&mut self, budget: Duration) {
        for e in self.execs.iter_mut().flatten() {
            e.on_idle(budget);
        }
    }

    /// Bounded housekeeping between two tasks (`Executor::between_tasks`).
    pub fn between_tasks(&mut self, budget: Duration) {
        for e in self.execs.iter_mut().flatten() {
            e.between_tasks(budget);
        }
    }

    /// The earliest timed housekeeping of the created executors (`Executor::next_deadline`).
    pub fn next_deadline(&self) -> Option<std::time::Instant> {
        self.execs.iter().flatten().filter_map(|e| e.next_deadline()).min()
    }

    /// The executors' wake signals (`Executor::wake_signal`), at most one per kind.
    pub fn wake_signals(&self) -> Vec<Arc<tokio::sync::Notify>> {
        self.execs.iter().flatten().filter_map(|e| e.wake_signal()).collect()
    }
}

/// Recorded scheduling events.
pub(crate) struct TraceLog {
    pub rec: TraceRecorder,
    pub keep: bool,
    pub timed: Vec<(Duration, TraceEvent)>,
}

/// Everything needed to construct the shared state.
pub(crate) struct CoreOpts {
    pub cfg: SchedConfig,
    pub clock: Arc<dyn Clock>,
    pub mem: Arc<dyn MemoryGovernor>,
    pub kinds: Vec<KindRegistration>,
    pub stats: Option<Arc<dyn StatsStore>>,
    pub metrics: Option<Arc<Metrics>>,
    pub logs: Option<Arc<dyn LogSink>>,
    pub schedtrace: SchedtraceSink,
    pub keep_trace: bool,
    pub sim: bool,
    pub warm: Option<Arc<WarmTargets>>,
}

pub(crate) struct SchedInner {
    pub cfg: SchedConfig,
    pub clock: Arc<dyn Clock>,
    pub mem: Arc<dyn MemoryGovernor>,
    pub kinds: Vec<KindRegistration>,
    /// `TaskKind::idx()` → index into `kinds`.
    pub kind_slot: [Option<usize>; 3],
    pub mode: AtomicU8,
    pub live: ArcSwap<LiveSchedConfig>,
    pub procs: Vec<Arc<Processor>>,
    pub machines: Vec<Arc<Machine>>,
    pub grq: [GlobalQueue<Arc<Task>>; 3],
    pub admission: Admission<Arc<Task>>,
    /// Warm-capacity targets the autoscaler publishes (read by executors).
    pub warm: Arc<WarmTargets>,
    /// Shed mode was entered automatically at `Pressure::Critical` (left below `High`).
    pub auto_shed: AtomicBool,
    /// Run-queue waits per kind since the last re-kinding duty.
    pub qwait: [WaitHist; 3],
    /// EWMA of the p90 run-queue wait per kind, in ns.
    pub p90_wait_ns: [AtomicU64; 3],
    pub sched_metrics: Option<SchedMetrics>,
    /// Sysmon self-check: persistent spinner-count mismatches seen (invariant 1, `lib.rs`).
    pub spinner_mismatches: AtomicU64,
    /// When sysmon last asked the governor for an allocator purge.
    pub last_purge: Mutex<Option<Instant>>,
    /// Waiting tasks the autoscaler granted a run slot (the simulation steps right after them).
    pub sysmon_grants: AtomicU64,
    /// Completion rate (EWMA, tasks/s) and when it was last sampled, for the `Retry-After` hint.
    pub drain: Mutex<DrainRate>,
    /// `Retry-After` base in ms (before jitter), refreshed by the autoscaler.
    pub retry_after_ms: AtomicU64,
    pub rng: Mutex<StdRng>,
    pub next_id: AtomicU64,
    /// Clock reading at construction (virtual in simulation).
    pub start: Instant,
    pub submitted: AtomicU64,
    pub completed: AtomicU64,
    /// Machines currently marked spinning, per kind.
    pub spinning: [AtomicU32; 3],
    /// Tasks waiting for their artifact, per key, in the order they started waiting.
    pub wait_rt: Mutex<BTreeMap<PreparedKey, Vec<Arc<Task>>>>,
    pub wait_rt_count: AtomicU32,
    /// Node-level FIFO of tasks whose reservation was refused (ordered by task id).
    pub wait_mem: Mutex<BTreeMap<TaskId, Arc<Task>>>,
    pub wait_mem_count: AtomicU32,
    /// Not-yet-started tasks by queue deadline (sharded by task id).
    pub deadlines: Vec<DeadlineShard>,
    pub trace: Option<Mutex<TraceLog>>,
    /// Simulation only: futures sysmon wants dropped (`LiveTask`, reason).
    pub drop_queue: Mutex<Vec<(Arc<LiveTask>, rt_types::KillReason)>>,
    pub sim: bool,
    /// No new tasks are admitted.
    pub shutdown: AtomicBool,
    /// Machine and sysmon threads must exit.
    pub stop: AtomicBool,
    pub stats: Option<Arc<dyn StatsStore>>,
    pub metrics: Option<Arc<Metrics>>,
    pub logs: Option<Arc<dyn LogSink>>,
    pub schedtrace: SchedtraceSink,
    pub threads: Mutex<Vec<JoinHandle<()>>>,
    pub sysmon_thread: Mutex<Option<std::thread::Thread>>,
    /// The real-thread driver's autoscaler thread (unparked on shutdown).
    pub autoscale_thread: Mutex<Option<std::thread::Thread>>,
}

fn mode_bits(m: DegradationMode) -> u8 {
    match m {
        DegradationMode::Normal => 0,
        DegradationMode::Shed => 1,
        DegradationMode::Drain => 2,
    }
}

impl SchedInner {
    /// Validate the placement of Ps and create Processors and Machines (no threads yet).
    pub fn new(o: CoreOpts) -> Result<Arc<Self>, SchedError> {
        let cfg = o.cfg;
        if cfg.lrq_capacity < 2 {
            return Err(SchedError::InvalidConfig("lrq_capacity must be >= 2".into()));
        }
        let mut kind_slot = [None; 3];
        for (i, k) in o.kinds.iter().enumerate() {
            kind_slot[k.kind.idx()] = Some(i);
        }

        let (deno_n, wasm_n) = initial_split(&cfg, kind_slot[0].is_some(), kind_slot[1].is_some());
        let build_n = cfg.build_processors;
        let p_total = (deno_n + wasm_n + build_n) as usize;
        let m_total = p_total + cfg.spare_machines as usize;

        let mut procs = Vec::with_capacity(p_total);
        for i in 0..p_total {
            let i32_ = i as u32;
            let kind = if i32_ < deno_n {
                TaskKind::DENO
            } else if i32_ < deno_n + wasm_n {
                TaskKind::WASM
            } else {
                TaskKind::Build
            };
            procs.push(Arc::new(Processor::new(ProcId(i32_), kind, cfg.lrq_capacity)));
        }
        let mut machines = Vec::with_capacity(m_total);
        for i in 0..m_total {
            let m = Arc::new(Machine::new(MachineId(i as u32)));
            // Machines start parked: the first `wakep` / handoff wakes them.
            m.parked.store(true, Ordering::Release);
            if let Some(p) = procs.get(i) {
                m.set_proc(Some(ProcId(i as u32)));
                p.set_machine(Some(MachineId(i as u32)));
            }
            machines.push(m);
        }

        let start = o.clock.now();
        let trace = if o.sim || o.keep_trace {
            Some(Mutex::new(TraceLog {
                rec: TraceRecorder::new(o.keep_trace),
                keep: o.keep_trace,
                timed: Vec::new(),
            }))
        } else {
            None
        };
        let live = cfg.live.clone();
        let admission = Admission::new(AdmissionLimits::resolve(
            cfg.max_inflight,
            cfg.max_queued,
            cfg.max_queued_bytes,
            o.mem.budget_bytes(),
        ))
        .with_aging(cfg.aging_factor);
        let sched_metrics = o.metrics.as_deref().and_then(SchedMetrics::new);
        let seed = cfg.seed;
        let inner = Arc::new(Self {
            cfg,
            clock: o.clock,
            mem: o.mem,
            kinds: o.kinds,
            kind_slot,
            mode: AtomicU8::new(0),
            live: ArcSwap::from_pointee(live),
            procs,
            machines,
            grq: [GlobalQueue::new(), GlobalQueue::new(), GlobalQueue::new()],
            admission,
            warm: o.warm.unwrap_or_default(),
            auto_shed: AtomicBool::new(false),
            qwait: [WaitHist::new(), WaitHist::new(), WaitHist::new()],
            p90_wait_ns: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
            sched_metrics,
            spinner_mismatches: AtomicU64::new(0),
            last_purge: Mutex::new(None),
            sysmon_grants: AtomicU64::new(0),
            drain: Mutex::new(DrainRate::default()),
            retry_after_ms: AtomicU64::new(RETRY_AFTER_MIN.as_millis() as u64),
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            next_id: AtomicU64::new(0),
            start,
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            spinning: [AtomicU32::new(0), AtomicU32::new(0), AtomicU32::new(0)],
            wait_rt: Mutex::new(BTreeMap::new()),
            wait_rt_count: AtomicU32::new(0),
            wait_mem: Mutex::new(BTreeMap::new()),
            wait_mem_count: AtomicU32::new(0),
            deadlines: (0..DEADLINE_SHARDS).map(|_| Mutex::new(BTreeMap::new())).collect(),
            trace,
            drop_queue: Mutex::new(Vec::new()),
            sim: o.sim,
            shutdown: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            stats: o.stats,
            metrics: o.metrics,
            logs: o.logs,
            schedtrace: o.schedtrace,
            threads: Mutex::new(Vec::new()),
            sysmon_thread: Mutex::new(None),
            autoscale_thread: Mutex::new(None),
        });
        // Executors ask how much of a service waits (`min_run_time = 0` hand-off).
        let backlog: std::sync::Weak<dyn crate::warm::Backlog> = Arc::downgrade(&inner) as _;
        inner.warm.set_backlog(backlog);
        Ok(inner)
    }

    // -----------------------------------------------------------------------------------------
    // Small helpers
    // -----------------------------------------------------------------------------------------

    pub fn now(&self) -> Instant {
        self.clock.now()
    }

    /// Nanoseconds since scheduler start.
    pub fn rel_ns(&self, t: Instant) -> u64 {
        t.saturating_duration_since(self.start)
            .as_nanos()
            .min(u128::from(u64::MAX - 1)) as u64
    }

    pub fn trace(&self, ev: TraceEvent) {
        if let Some(t) = &self.trace {
            let at = self.clock.now().saturating_duration_since(self.start);
            let mut g = t.lock();
            g.rec.record(ev);
            if g.keep {
                g.timed.push((at, ev));
            }
        }
    }

    pub fn mode(&self) -> DegradationMode {
        match self.mode.load(Ordering::Acquire) {
            1 => DegradationMode::Shed,
            2 => DegradationMode::Drain,
            _ => DegradationMode::Normal,
        }
    }

    pub fn set_mode(&self, m: DegradationMode) {
        self.mode.store(mode_bits(m), Ordering::Release);
    }

    pub fn registered(&self, kind: TaskKind) -> bool {
        self.kind_slot[kind.idx()].is_some()
    }

    pub fn proc(&self, id: ProcId) -> &Arc<Processor> {
        &self.procs[id.0 as usize]
    }

    pub fn machine_of(&self, p: &Processor) -> Option<&Arc<Machine>> {
        p.machine().map(|m| &self.machines[m.0 as usize])
    }

    pub fn count_procs(&self, kind: TaskKind) -> usize {
        self.procs.iter().filter(|p| p.kind() == kind).count()
    }

    /// Run `f` with the scheduler's random source: the seeded generator in simulation (so runs
    /// are reproducible), the thread-local one on real threads (no shared lock on the hot path).
    pub fn with_rng<R>(&self, f: impl FnOnce(&mut dyn RngCore) -> R) -> R {
        if self.sim {
            f(&mut *self.rng.lock())
        } else {
            f(&mut rand::thread_rng())
        }
    }

    /// Tasks wait in `m`'s own queues: its P's LRQ or `runnext`, or its bound tasks ready to resume.
    pub fn local_work(&self, m: &Machine) -> bool {
        let lrq = m.proc().is_some_and(|p| {
            let p = self.proc(p);
            !p.lrq.is_empty() || p.runnext.lock().is_some()
        });
        lrq || !m.bound_ready.lock().is_empty()
    }

    /// Can the Machine start a task that is not yet bound anywhere? (`bound_count` cap)
    pub fn can_start_new(&self, m: &Machine, kind: TaskKind) -> bool {
        kind != TaskKind::DENO || m.bound_count.load(Ordering::Acquire) < self.live.load().max_bound_per_m
    }

    // -----------------------------------------------------------------------------------------
    // Snapshot
    // -----------------------------------------------------------------------------------------

    pub fn snapshot(&self) -> SchedSnapshot {
        let procs: Vec<_> = self.procs.iter().map(|p| p.snapshot()).collect();
        let machines: Vec<_> = self.machines.iter().map(|m| m.snapshot()).collect();
        let idle = procs.iter().filter(|p| p.status == ProcStatus::Idle).count() as u32;
        let spinning = machines.iter().filter(|m| m.spinning).count() as u32;
        SchedSnapshot {
            uptime_ms: self.clock.now().saturating_duration_since(self.start).as_millis() as u64,
            mode: self.mode(),
            procs,
            machines,
            grq: GrqLens {
                deno: self.grq[0].len() as u32,
                wasm: self.grq[1].len() as u32,
                build: self.grq[2].len() as u32,
            },
            idle,
            spinning,
            waiting_runtime: self.wait_rt_count.load(Ordering::Acquire),
            waiting_memory: self.wait_mem_count.load(Ordering::Acquire),
            inflight: self.admission.inflight(),
            tasks_submitted: self.submitted.load(Ordering::Acquire),
            tasks_completed: self.completed.load(Ordering::Acquire),
        }
    }
}

/// How many guest Ps each kind starts with: an even split (Deno takes the odd one), clamped to the
/// configured bounds. A kind without an executor gets none.
fn initial_split(cfg: &SchedConfig, has_deno: bool, has_wasm: bool) -> (u32, u32) {
    let n = cfg.processors;
    if has_deno && !has_wasm {
        return (n, 0);
    }
    if has_wasm && !has_deno {
        return (0, n);
    }
    if !has_deno && !has_wasm {
        return (n, 0);
    }
    let (dmin, dmax) = cfg.kind_bounds(TaskKind::DENO);
    let (wmin, wmax) = cfg.kind_bounds(TaskKind::WASM);
    let mut deno = n.div_ceil(2).clamp(dmin.min(n), dmax.max(dmin).min(n));
    let mut wasm = n - deno;
    if wasm < wmin {
        wasm = wmin.min(n);
        deno = n - wasm;
    }
    if wasm > wmax {
        wasm = wmax.min(n);
        deno = n - wasm;
    }
    (deno, wasm)
}

impl crate::warm::Backlog for SchedInner {
    fn waiting(&self, service: rt_types::ServiceId) -> u32 {
        self.admission.waiting(service)
    }
}
