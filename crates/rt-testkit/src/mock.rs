//! `MockExecutor`: a scripted `rt_sched::Executor` for scheduler tests.
//!
//! Scripts are built with the DSL (`cpu(ms)`, `io(ms)`, `yield_now()`, `alloc(mb)`,
//! `call(svc)`, `panic()`, `loop_forever()`):
//!
//! ```ignore
//! let script = Script::new().then(cpu(5)).then(io(10)).then(yield_now()).then(alloc(8));
//! books.set_for_service(service_id, script);
//! ```
//!
//! All waiting goes through `ExecCx::clock` (`sleep`, `burn_cpu`), so the same executor runs on
//! real threads (SCH-23 benchmark) and under `SimRuntime` with virtual time.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use parking_lot::Mutex;
use rt_sched::{ExecCx, Executor, Interrupt, Interrupter, Lookup, PreparedSource, TaskFuture};
use rt_types::{BuildFailure, KillReason, Outcome, PreparedKey, Reply, ServiceId, TaskId, TaskKind, TaskSpec};

// ---------------------------------------------------------------------------------------------
// Script DSL
// ---------------------------------------------------------------------------------------------

/// One step of a task script.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    /// Burn `d` of CPU on this thread (`Clock::burn_cpu`).
    Cpu(Duration),
    /// Wait `d` of clock time (an I/O wait: the task is `Waiting`).
    Io(Duration),
    /// Yield once (cooperative; like a WASM epoch yield).
    Yield,
    /// Grow the task's memory by `mb` (`Reservation::try_grow`); refusal ⇒ `Killed(Oom)`.
    Alloc(u64),
    /// Record a service call (`std:services`); takes no time in the mock.
    Call(String),
    /// The guest throws: `Outcome::GuestError`.
    Panic,
    /// The executor itself panics (a Rust `panic!` while polling the task, SCHED-002).
    Crash,
    /// Never finishes on its own: burns 1 ms CPU and yields forever (until killed).
    LoopForever,
}

pub fn cpu(ms: u64) -> Step {
    Step::Cpu(Duration::from_millis(ms))
}
pub fn io(ms: u64) -> Step {
    Step::Io(Duration::from_millis(ms))
}
pub fn yield_now() -> Step {
    Step::Yield
}
pub fn alloc(mb: u64) -> Step {
    Step::Alloc(mb)
}
pub fn call(service: &str) -> Step {
    Step::Call(service.to_string())
}
pub fn panic() -> Step {
    Step::Panic
}
/// The executor panics while running the task (not a guest error: a bug on the host side).
pub fn crash() -> Step {
    Step::Crash
}
pub fn loop_forever() -> Step {
    Step::LoopForever
}

/// An ordered list of steps. An empty script is a no-op task that returns `200`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Script {
    pub steps: Vec<Step>,
}

impl Script {
    pub fn new() -> Self {
        Self::default()
    }

    /// No-op task.
    pub fn noop() -> Self {
        Self::default()
    }

    pub fn then(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }

    pub fn cpu_ms(self, ms: u64) -> Self {
        self.then(cpu(ms))
    }
    pub fn io_ms(self, ms: u64) -> Self {
        self.then(io(ms))
    }
    pub fn yield_once(self) -> Self {
        self.then(yield_now())
    }
    pub fn alloc_mb(self, mb: u64) -> Self {
        self.then(alloc(mb))
    }
}

impl From<Vec<Step>> for Script {
    fn from(steps: Vec<Step>) -> Self {
        Self { steps }
    }
}

/// `script![cpu(5), io(10)]`
#[macro_export]
macro_rules! script {
    ($($step:expr),* $(,)?) => { $crate::mock::Script::from(vec![$($step),*]) };
}

// ---------------------------------------------------------------------------------------------
// Script book + events
// ---------------------------------------------------------------------------------------------

/// What happened to a mock task, in order.
#[derive(Clone, Debug, PartialEq)]
pub enum MockEvent {
    Started { task: TaskId, service: ServiceId },
    Called { task: TaskId, slug: String },
    Killed { task: TaskId, reason: KillReason },
    Finished { task: TaskId },
}

/// Callback fired inside the poll of a task that executes `call(slug)`: `(caller, slug)`.
/// A test uses it to submit the child task through the scheduler (SCH-24).
pub type CallHook = Arc<dyn Fn(TaskId, &str) + Send + Sync>;

/// Shared between the test and every per-Machine executor instance.
#[derive(Default)]
pub struct ScriptBook {
    by_service: Mutex<HashMap<ServiceId, Script>>,
    default: Mutex<Script>,
    events: Mutex<Vec<MockEvent>>,
    started: AtomicU64,
    call_hook: Mutex<Option<CallHook>>,
    quiet: AtomicBool,
}

impl ScriptBook {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Script for tasks of `service`.
    pub fn set_for_service(&self, service: ServiceId, script: Script) {
        self.by_service.lock().insert(service, script);
    }

    /// Script for services without their own.
    pub fn set_default(&self, script: Script) {
        *self.default.lock() = script;
    }

    fn script_for(&self, service: ServiceId) -> Script {
        self.by_service
            .lock()
            .get(&service)
            .cloned()
            .unwrap_or_else(|| self.default.lock().clone())
    }

    fn record(&self, ev: MockEvent) {
        if self.quiet.load(Ordering::Relaxed) {
            return;
        }
        self.events.lock().push(ev);
    }

    /// Run `hook` inside the poll of every task that executes `call(slug)` (after recording
    /// `MockEvent::Called`). The hook may call `Scheduler::submit` with a `Trigger::Call` child.
    pub fn set_call_hook(&self, hook: impl Fn(TaskId, &str) + Send + Sync + 'static) {
        *self.call_hook.lock() = Some(Arc::new(hook));
    }

    /// Stop recording `MockEvent`s (throughput benchmarks, SCH-23). `started()` keeps counting.
    pub fn set_record_events(&self, on: bool) {
        self.quiet.store(!on, Ordering::Relaxed);
    }

    fn fire_call_hook(&self, task: TaskId, slug: &str) {
        let hook = self.call_hook.lock().clone();
        if let Some(h) = hook {
            h(task, slug);
        }
    }

    /// All events so far, in order.
    pub fn events(&self) -> Vec<MockEvent> {
        self.events.lock().clone()
    }

    /// Tasks in start order.
    pub fn start_order(&self) -> Vec<TaskId> {
        self.events
            .lock()
            .iter()
            .filter_map(|e| {
                if let MockEvent::Started { task, .. } = e {
                    Some(*task)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Number of tasks started.
    pub fn started(&self) -> u64 {
        self.started.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------------------------

/// Marker choosing the `Executor::KIND` of a `MockExecutor`.
pub trait KindMarker: 'static {
    const KIND: TaskKind;
}
pub struct DenoKind;
pub struct WasmKind;
pub struct BuildKind;
impl KindMarker for DenoKind {
    const KIND: TaskKind = TaskKind::DENO;
}
impl KindMarker for WasmKind {
    const KIND: TaskKind = TaskKind::WASM;
}
impl KindMarker for BuildKind {
    const KIND: TaskKind = TaskKind::Build;
}

pub type MockDenoExecutor = MockExecutor<DenoKind>;
pub type MockWasmExecutor = MockExecutor<WasmKind>;
pub type MockBuildExecutor = MockExecutor<BuildKind>;

#[derive(Default)]
struct KillCells {
    cells: Mutex<HashMap<TaskId, Arc<Mutex<Option<KillReason>>>>>,
}

/// Interrupter shared by one executor instance's tasks.
#[derive(Default)]
pub struct MockInterrupter {
    kills: KillCells,
    yields: AtomicU64,
}

impl MockInterrupter {
    /// Number of `Interrupt::Yield` requests received (time-slice enforcement tests).
    pub fn yields(&self) -> u64 {
        self.yields.load(Ordering::Relaxed)
    }
}

impl Interrupter for MockInterrupter {
    fn interrupt(&self, task: TaskId, what: Interrupt) {
        match what {
            Interrupt::Yield => {
                self.yields.fetch_add(1, Ordering::Relaxed);
            }
            Interrupt::Kill(r) => {
                if let Some(c) = self.kills.cells.lock().get(&task) {
                    *c.lock() = Some(r);
                }
            }
        }
    }
}

/// Scripted executor. `Prepared = ()`: pair it with `UnitSource` or `MockPreparedSource`.
pub struct MockExecutor<K: KindMarker> {
    book: Arc<ScriptBook>,
    interrupter: Arc<MockInterrupter>,
    _k: PhantomData<K>,
}

impl<K: KindMarker> MockExecutor<K> {
    /// Create one per Machine inside the scheduler factory: `|| MockExecutor::new(book.clone())`.
    pub fn new(book: Arc<ScriptBook>) -> Self {
        Self {
            book,
            interrupter: Arc::new(MockInterrupter::default()),
            _k: PhantomData,
        }
    }
}

/// Completes after being polled once (a cooperative yield).
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl<K: KindMarker> Executor for MockExecutor<K> {
    const KIND: TaskKind = K::KIND;
    type Prepared = ();

    fn start(&mut self, spec: TaskSpec, _prepared: Arc<()>, cx: ExecCx) -> TaskFuture {
        let book = self.book.clone();
        let service = spec.service.service;
        let script = book.script_for(service);
        let kill: Arc<Mutex<Option<KillReason>>> = Arc::new(Mutex::new(None));
        self.interrupter.kills.cells.lock().insert(cx.task, kill.clone());
        let interrupter = self.interrupter.clone();
        book.started.fetch_add(1, Ordering::Relaxed);
        book.record(MockEvent::Started { task: cx.task, service });

        Box::pin(async move {
            let ExecCx {
                task,
                clock,
                mut reservation,
                ..
            } = cx;
            let killed = |kill: &Arc<Mutex<Option<KillReason>>>| *kill.lock();
            let mut outcome: Option<Outcome> = None;

            'run: for step in &script.steps {
                if let Some(r) = killed(&kill) {
                    outcome = Some(Outcome::Killed(r));
                    break 'run;
                }
                match step {
                    Step::Cpu(d) => clock.burn_cpu(*d),
                    Step::Io(d) => clock.sleep(*d).await,
                    Step::Yield => YieldOnce(false).await,
                    Step::Alloc(mb) => {
                        if !reservation.try_grow(mb * 1024 * 1024) {
                            outcome = Some(Outcome::Killed(KillReason::Oom));
                            break 'run;
                        }
                    }
                    Step::Call(slug) => {
                        book.record(MockEvent::Called {
                            task,
                            slug: slug.clone(),
                        });
                        book.fire_call_hook(task, slug);
                    }
                    Step::Panic => {
                        outcome = Some(Outcome::GuestError);
                        break 'run;
                    }
                    Step::Crash => panic!("mock executor crash (Step::Crash)"),
                    Step::LoopForever => loop {
                        if let Some(r) = killed(&kill) {
                            outcome = Some(Outcome::Killed(r));
                            break 'run;
                        }
                        clock.burn_cpu(Duration::from_millis(1));
                        YieldOnce(false).await;
                    },
                }
                if let Some(r) = killed(&kill) {
                    outcome = Some(Outcome::Killed(r));
                    break 'run;
                }
            }

            interrupter.kills.cells.lock().remove(&task);
            let outcome = outcome.unwrap_or_else(|| Outcome::Ok(Reply::ok()));
            match &outcome {
                Outcome::Killed(r) => book.record(MockEvent::Killed { task, reason: *r }),
                _ => book.record(MockEvent::Finished { task }),
            }
            outcome
        })
    }

    fn interrupter(&self) -> Arc<dyn Interrupter> {
        self.interrupter.clone()
    }

    fn on_idle(&mut self, _budget: Duration) {}
}

// ---------------------------------------------------------------------------------------------
// Prepared source
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Slot {
    Ready,
    Building,
    Failed(BuildFailure),
}

/// A `PreparedSource<()>` whose artifacts become ready on demand (single-flight build tests,
/// SCH-16): the first lookup of an unknown key starts a "build" and returns `Pending`.
#[derive(Default)]
pub struct MockPreparedSource {
    /// When `true`, unknown keys are immediately ready (no build). Default `false`.
    auto_ready: AtomicBool,
    slots: Mutex<HashMap<PreparedKey, Slot>>,
    builds_started: AtomicU64,
    waiters: Mutex<HashMap<PreparedKey, Vec<TaskId>>>,
}

impl MockPreparedSource {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Treat every unknown key as ready (tests that do not care about builds).
    pub fn set_auto_ready(&self, on: bool) {
        self.auto_ready.store(on, Ordering::Relaxed);
    }

    /// Mark `key` ready (a T1 hit from now on).
    pub fn make_ready(&self, key: PreparedKey) {
        self.slots.lock().insert(key, Slot::Ready);
    }

    /// Complete the in-flight build of `key`: returns the tasks that were waiting. The caller
    /// then calls `Scheduler::wake_runtime_waiters(&key)`.
    pub fn complete(&self, key: &PreparedKey) -> Vec<TaskId> {
        self.slots.lock().insert(*key, Slot::Ready);
        self.waiters.lock().remove(key).unwrap_or_default()
    }

    /// Fail the build of `key` (negative cache until `clear_failure`).
    pub fn fail(&self, key: &PreparedKey, failure: BuildFailure) -> Vec<TaskId> {
        self.slots.lock().insert(*key, Slot::Failed(failure));
        self.waiters.lock().remove(key).unwrap_or_default()
    }

    /// Number of distinct builds started (must be 1 for 100 concurrent requests).
    pub fn builds_started(&self) -> u64 {
        self.builds_started.load(Ordering::Relaxed)
    }
}

impl PreparedSource<()> for MockPreparedSource {
    fn lookup(&self, key: &PreparedKey, waiter: TaskId) -> Lookup<()> {
        let mut slots = self.slots.lock();
        match slots.get(key) {
            Some(Slot::Ready) => Lookup::Ready(Arc::new(())),
            Some(Slot::Failed(f)) => Lookup::Failed(f.clone()),
            Some(Slot::Building) => {
                self.waiters.lock().entry(*key).or_default().push(waiter);
                Lookup::Pending
            }
            None if self.auto_ready.load(Ordering::Relaxed) => Lookup::Ready(Arc::new(())),
            None => {
                slots.insert(*key, Slot::Building);
                self.builds_started.fetch_add(1, Ordering::Relaxed);
                self.waiters.lock().entry(*key).or_default().push(waiter);
                Lookup::Pending
            }
        }
    }

    fn is_ready(&self, key: &PreparedKey) -> bool {
        match self.slots.lock().get(key) {
            Some(Slot::Ready) => true,
            None => self.auto_ready.load(Ordering::Relaxed),
            _ => false,
        }
    }
}
