//! Isolate creation: a `JsRuntime` from the base snapshot with the guest extensions, a memory
//! account (`memory.rs`: heap limit grown through the governor, a counting `ArrayBuffer`
//! allocator, wasm memories), a foreground-task queue (`platform.rs`), and per-task
//! re-attachment (loader, memory, clock).
//!
//! Every isolate starts from the base snapshot (bootstrap + web platform, no tenant data) that
//! `build.rs` embedded in the binary (`snapshot.rs`). There is no snapshot-less path: the web
//! platform JS is not available at runtime outside the snapshot.

use std::rc::Rc;
use std::sync::Arc;

use deno_core::v8;
use deno_core::{JsRuntime, RuntimeOptions};
use rt_mem::Reservation;

use crate::bundle_loader::{BundleLoader, SharedLoader};
use crate::clock::ClockState;
use crate::config::{DenoExecutorConfig, HEAP_GROW_STEP_MB};
use crate::cpu::CpuGuard;
use crate::exts::guest_extensions_with;
use crate::memory::{array_buffer_allocator, CountedBlobStore, MemAccount};
use crate::platform::{self, IsolateTasks};
use crate::prepared::init_v8_once;
use crate::snapshot::{base_runtime_options, seal};
use crate::warm::ReuseState;

pub const MIB: u64 = 1024 * 1024;

/// Smallest V8 heap limit an isolate is created with. V8 aborts the process when its heap limit
/// is hit before the near-heap-limit callback can raise it, so the limit starts here (and the
/// reservation is topped up to match when the task attaches), then grows in `HEAP_GROW_STEP_MB`
/// steps through the governor.
pub const V8_HEAP_FLOOR_MB: u64 = 16;

/// Registration of an isolate's foreground-task queue with the platform; dropping it unregisters.
pub struct PlatformTasks(pub Arc<IsolateTasks>);

impl Drop for PlatformTasks {
    fn drop(&mut self) {
        platform::unregister(&self.0);
    }
}

/// The isolate's [`MemAccount`], closed when the isolate is dropped: it is a field *after*
/// `runtime`, so by then V8 has freed the backing stores it owned, and the reservation is
/// released right away even if Rust still holds a backing store (which keeps the account alive).
pub struct IsolateMem(pub Arc<MemAccount>);

impl Drop for IsolateMem {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Guard that makes one isolate the thread's *current* isolate while it lives.
///
/// rusty_v8 enters every `OwnedIsolate` when it is created and expects strictly nested
/// enter/exit. A Machine thread hosts several isolates at once (one per in-flight task plus the
/// idle pool), so each isolate is exited right after creation and entered only for the duration of
/// a synchronous use (script execution, event-loop poll, disposal). A guard must never live across
/// an `.await`, and guards of different isolates must nest (the `!Send` raw pointer enforces the
/// thread; the lexical scope enforces the nesting).
pub struct Entered(v8::UnsafeRawIsolatePtr);

impl Entered {
    /// Enter `isolate`.
    ///
    /// # Safety
    /// `isolate` must be a live isolate that is currently *exited* on this thread (or entered
    /// by an enclosing guard), and the guard must be dropped before the isolate is.
    unsafe fn new(isolate: v8::UnsafeRawIsolatePtr) -> Self {
        // SAFETY: the caller guarantees the pointer is live; enter/exit pairs nest lexically.
        unsafe { v8::Isolate::ref_from_raw_isolate_ptr(&isolate).enter() };
        Self(isolate)
    }
}

impl Drop for Entered {
    fn drop(&mut self) {
        // SAFETY: balanced with `new`; the isolate is still current (guards nest).
        unsafe { v8::Isolate::ref_from_raw_isolate_ptr(&self.0).exit() };
    }
}

/// Enters the isolate when dropped and leaves it entered: it is the first field of [`Isolate`], so
/// it runs right before the `JsRuntime` drops, and `OwnedIsolate::drop` (which asserts that its
/// isolate is current and exits it) finds the entry count balanced.
struct EnterForDrop(v8::UnsafeRawIsolatePtr);

impl Drop for EnterForDrop {
    fn drop(&mut self) {
        // SAFETY: the runtime (and with it the isolate) is dropped after this field.
        unsafe { v8::Isolate::ref_from_raw_isolate_ptr(&self.0).enter() };
    }
}

/// One V8 isolate with everything the executor needs to run and later dispose it.
/// Field order is drop order: re-enter, then the runtime, then the memory reservation.
///
/// An `Isolate` is *exited* while it is idle: use [`Isolate::enter`] around every use of
/// `runtime`.
pub struct Isolate {
    _enter_for_drop: EnterForDrop,
    /// V8 foreground tasks of this isolate, run by the driver (`platform.rs`). Unregistered before
    /// the runtime (and with it the isolate) is dropped.
    pub tasks: PlatformTasks,
    pub runtime: JsRuntime,
    pub loader: Rc<BundleLoader>,
    /// Heap, `ArrayBuffer` and wasm memory of the task (B5.1, `memory.rs`).
    pub mem: IsolateMem,
    pub handle: v8::IsolateHandle,
    /// Kept alive until the isolate is gone (a V8 interrupt may still reference it); reset per
    /// task when the isolate is reused.
    pub cpu: Option<Arc<CpuGuard>>,
    /// Warm reuse bookkeeping (D-A4, `warm.rs`).
    pub reuse: ReuseState,
    raw: v8::UnsafeRawIsolatePtr,
}

/// Create an isolate holding `reservation` (pool: the idle isolate's own; task: attached later).
pub fn create_isolate(cfg: &DenoExecutorConfig, reservation: Option<Reservation>) -> Result<Isolate, String> {
    init_v8_once(cfg);
    let floor = u64::from(cfg.isolate_base_mb).max(V8_HEAP_FLOOR_MB) * MIB;
    let loader = BundleLoader::new();
    let mem = MemAccount::new(reservation, floor);
    // `Atomics.wait` (and wasm `memory.atomic.wait`) would park the Machine thread with no CPU
    // used, out of reach of the CPU watchdog: never allowed on a Machine thread.
    let params = v8::CreateParams::default()
        .heap_limits(0, floor as usize)
        .allow_atomics_wait(false)
        .array_buffer_allocator(array_buffer_allocator(&mem));
    let options = RuntimeOptions {
        module_loader: Some(Rc::new(SharedLoader(loader.clone()))),
        create_params: Some(params),
        extensions: guest_extensions_with(CountedBlobStore::shared(mem.clone())),
        ..base_runtime_options()?
    };
    let mut runtime = JsRuntime::try_new(options).map_err(|e| format!("cannot create isolate: {e}"))?;
    // SAFETY: only used as an opaque identity for enter/exit of the isolate `runtime` owns.
    let raw = unsafe { runtime.v8_isolate().as_raw_isolate_ptr() };
    let tasks = PlatformTasks(platform::register(raw));
    // The new isolate is entered (rusty_v8 does it on creation). From here on every error return
    // drops `runtime` while it is entered, which is what `OwnedIsolate::drop` needs; on success it
    // is exited again below so several isolates can live on this thread.
    seal(&mut runtime)?;
    let handle = runtime.v8_isolate().thread_safe_handle();
    {
        let state = runtime.op_state();
        let mut state = state.borrow_mut();
        state.put(ClockState::new(cfg.clock));
        state.put(mem.clone()); // `op_rt_wasm_track` registers wasm memories here
    }

    let cb_mem = mem.clone();
    let cb_handle = handle.clone();
    runtime.add_near_heap_limit_callback(move |current, _initial| {
        let step = u64::from(HEAP_GROW_STEP_MB) * MIB;
        if !cb_mem.grow_heap(current as u64, step) {
            // Refused (`oom` is set): stop the guest and give V8 just enough room to unwind.
            cb_handle.terminate_execution();
        }
        (current as u64 + step) as usize
    });
    // SAFETY: the isolate is entered exactly once (by its creation) and is current.
    unsafe { v8::Isolate::ref_from_raw_isolate_ptr(&raw).exit() };
    Ok(Isolate {
        _enter_for_drop: EnterForDrop(raw),
        tasks,
        runtime,
        loader,
        mem: IsolateMem(mem),
        handle,
        cpu: None,
        reuse: ReuseState::default(),
        raw,
    })
}

impl Isolate {
    /// Make this isolate current until the guard drops. Not tied to a borrow of `self`, so the
    /// runtime can be used mutably while the guard is held.
    pub fn enter(&self) -> Entered {
        // SAFETY: `self` owns a live isolate that is exited between uses.
        unsafe { Entered::new(self.raw) }
    }

    /// Bind a (possibly pooled) isolate to a task: its reservation becomes the isolate's (the
    /// pool's is released), topped up to the heap limit V8 already granted plus the buffers the
    /// bootstrap holds. `Err` = the governor refused (`oom`).
    #[allow(clippy::result_unit_err)] // the only failure is the governor's refusal (`oom` is set)
    pub fn attach(&mut self, reservation: Reservation, limit_mb: u32, clock: ClockState) -> Result<(), ()> {
        if !self.mem.0.attach(reservation, u64::from(limit_mb) * MIB) {
            return Err(());
        }
        *self.runtime.op_state().borrow_mut().borrow_mut::<ClockState>() = clock;
        Ok(())
    }

    pub fn oom(&self) -> bool {
        self.mem.0.oom()
    }
}
