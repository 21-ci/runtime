//! CPU limit: the executor measures thread CPU time only while it polls the
//! task, and a shared watchdog thread interrupts V8 every few milliseconds so a synchronous
//! busy loop is terminated even though nothing can poll the future. The same interrupt samples
//! the task's memory (B5.1, `memory.rs`): wasm memory can grow inside a busy loop too.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use deno_core::v8;

use crate::memory::MemAccount;

/// CPU time of the calling thread in nanoseconds.
pub fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a valid out-pointer and CLOCK_THREAD_CPUTIME_ID is supported on Linux and macOS.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return 0; // LCOV_EXCL_LINE: the thread CPU clock does not fail on supported platforms
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// Per-task CPU accounting shared between the driver and the watchdog. One guard serves an
/// isolate for its whole life ([`CpuGuard::reset`] per task when the isolate is reused): an
/// interrupt the watchdog already requested carries a pointer to it, so it must outlive the
/// isolate, never just the task.
pub struct CpuGuard {
    budget_ns: AtomicU64,
    /// CPU consumed by completed polls.
    used_ns: AtomicU64,
    /// Thread CPU clock at the start of the poll in progress (0 = not polling).
    poll_start_ns: AtomicU64,
    active: AtomicBool,
    tripped: AtomicBool,
    handle: v8::IsolateHandle,
    /// Memory account sampled at each watchdog interrupt.
    memory: OnceLock<Arc<MemAccount>>,
}

impl CpuGuard {
    pub fn new(budget: Duration, handle: v8::IsolateHandle) -> Arc<Self> {
        Arc::new(Self {
            budget_ns: AtomicU64::new(u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX)),
            used_ns: AtomicU64::new(0),
            poll_start_ns: AtomicU64::new(0),
            active: AtomicBool::new(false),
            tripped: AtomicBool::new(false),
            handle,
            memory: OnceLock::new(),
        })
    }

    /// Also check `memory` at every watchdog interrupt; over the limit the isolate is terminated
    /// (the account is marked `oom`, the task ends as `Killed(Oom)`).
    pub fn watch_memory(&self, memory: Arc<MemAccount>) {
        let _ = self.memory.set(memory);
    }

    pub fn begin_poll(&self) {
        self.poll_start_ns.store(thread_cpu_ns().max(1), Ordering::Release);
    }

    pub fn end_poll(&self) {
        let start = self.poll_start_ns.swap(0, Ordering::AcqRel);
        if start != 0 {
            self.used_ns
                .fetch_add(thread_cpu_ns().saturating_sub(start), Ordering::AcqRel);
        }
    }

    pub fn used(&self) -> Duration {
        Duration::from_nanos(self.used_ns.load(Ordering::Acquire))
    }

    pub fn over_budget(&self, extra: Duration) -> bool {
        let extra = u64::try_from(extra.as_nanos()).unwrap_or(u64::MAX);
        self.used_ns.load(Ordering::Acquire).saturating_add(extra) > self.budget_ns.load(Ordering::Acquire)
    }

    /// Start a new task on a reused isolate (D-A4): a fresh budget, nothing used, not tripped.
    /// Only while inactive (between tasks).
    pub fn reset(&self, budget: Duration) {
        self.budget_ns
            .store(u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX), Ordering::Release);
        self.used_ns.store(0, Ordering::Release);
        self.poll_start_ns.store(0, Ordering::Release);
        self.tripped.store(false, Ordering::Release);
    }

    pub fn trip(&self) {
        self.tripped.store(true, Ordering::Release);
        self.handle.terminate_execution();
    }

    pub fn tripped(&self) -> bool {
        self.tripped.load(Ordering::Acquire)
    }

    /// Start watching (watchdog interrupts the isolate while active).
    pub fn activate(self: &Arc<Self>) {
        self.active.store(true, Ordering::Release);
        watchdog().register(self.clone());
    }

    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }
}

/// Runs on the isolate's thread at a V8 safe point.
unsafe extern "C" fn cpu_interrupt(isolate: v8::UnsafeRawIsolatePtr, data: *mut c_void) {
    // SAFETY: `data` is `Arc::as_ptr` of a `CpuGuard` that the isolate owner keeps alive until the
    // isolate is disposed (see `Isolate::cpu`), and the watchdog only passes pointers of guards it
    // still holds an `Arc` of.
    let guard = unsafe { &*(data as *const CpuGuard) };
    if !guard.active.load(Ordering::Acquire) {
        return; // LCOV_EXCL_LINE: the interrupt raced with the end of the task
    }
    let start = guard.poll_start_ns.load(Ordering::Acquire);
    if start == 0 {
        return; // LCOV_EXCL_LINE: the interrupt landed between two polls of the driver
    }
    // The driver is inside a poll, so guest code is running: sample its memory (wasm memory and
    // buffers made without the allocator can grow in a loop that never yields).
    let memory_ok = guard.memory.get().is_none_or(|memory| {
        let mut raw = isolate;
        // SAFETY: V8 runs interrupt callbacks on the isolate's own thread, at a stack check of
        // running script (a context is entered), where handles may be created and GC may run.
        let isolate = unsafe { v8::Isolate::ref_from_raw_isolate_ptr_mut(&mut raw) };
        v8::callback_scope!(unsafe let callback, isolate);
        v8::scope!(let scope, callback);
        memory.check_in_interrupt(scope)
    });
    if !memory_ok {
        guard.handle.terminate_execution();
        return;
    }
    let live = thread_cpu_ns().saturating_sub(start);
    if guard.used_ns.load(Ordering::Acquire).saturating_add(live) > guard.budget_ns.load(Ordering::Acquire) {
        guard.trip();
    }
}

struct Watchdog {
    guards: Mutex<Vec<Arc<CpuGuard>>>,
}

impl Watchdog {
    fn register(&self, g: Arc<CpuGuard>) {
        let mut guards = self.guards.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !guards.iter().any(|x| Arc::ptr_eq(x, &g)) {
            guards.push(g);
        }
    }
}

fn watchdog() -> &'static Watchdog {
    static W: OnceLock<&'static Watchdog> = OnceLock::new();
    W.get_or_init(|| {
        let w: &'static Watchdog = Box::leak(Box::new(Watchdog {
            guards: Mutex::new(Vec::new()),
        }));
        let spawned = std::thread::Builder::new()
            .name("rt-deno-watchdog".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_millis(4));
                let mut guards = w.guards.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                guards.retain(|g| g.active.load(Ordering::Acquire));
                for g in guards.iter() {
                    let _ = g.handle.request_interrupt(cpu_interrupt, Arc::as_ptr(g) as *mut c_void);
                }
            });
        // LCOV_EXCL_START: the OS refusing a thread cannot be provoked
        if let Err(e) = spawned {
            tracing::warn!("cannot start the CPU watchdog thread: {e}; CPU limits are checked at poll boundaries only");
        }
        // LCOV_EXCL_STOP
        w
    })
}
