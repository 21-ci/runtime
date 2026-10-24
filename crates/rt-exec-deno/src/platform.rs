//! The V8 platform: `deno.v8_platform_threads` worker threads (RP-01) plus per-isolate queues of
//! V8 *foreground* tasks, which the task driver runs on its isolate's thread at every poll.
//!
//! `deno_core` 0.412 no longer pumps V8's default message loop: it drains a task queue that only
//! its own (private) custom platform fills. With `v8::new_default_platform` the foreground tasks
//! V8 posts were queued inside V8 and never run, so anything finished by one never finished:
//! async `WebAssembly.compile` / `instantiate` (the usual way wasm-bindgen and npm packages load
//! wasm) busy-polled until the CPU limit killed the task, `FinalizationRegistry` callbacks never
//! ran, and V8's delayed GC tasks were dropped. This module is the equivalent of that platform for
//! the isolates this crate creates ([`register`] / [`IsolateTasks::run_pending`]).
//!
//! Tasks posted for an isolate that is not registered (the build's throwaway compile isolates) are
//! dropped without running, as before.

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::{Arc, OnceLock};
use std::task::Waker;
use std::time::Duration;

use deno_core::v8;
use parking_lot::Mutex;

/// Foreground tasks of one isolate.
pub struct IsolateTasks {
    key: usize,
    /// Distinguishes registrations of the same address: V8 reuses isolate addresses, and a
    /// delayed task must never land on a later isolate at the same address.
    generation: u64,
    tasks: Mutex<Vec<v8::Task>>,
    /// The driver's waker: a posted task wakes the driver so it runs the task.
    waker: Mutex<Option<Waker>>,
    /// Runtime of the isolate's Machine, for delayed tasks.
    handle: Option<tokio::runtime::Handle>,
}

impl IsolateTasks {
    /// Run the tasks posted so far. Must be called on the isolate's thread, with the isolate
    /// entered and no handle scope of a guest call active (the driver, between polls).
    pub fn run_pending(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for task in tasks {
            task.run();
        }
    }

    /// Tasks posted and not yet run.
    pub fn has_pending(&self) -> bool {
        !self.tasks.lock().is_empty()
    }

    /// Wake `waker` when a task is posted.
    pub fn set_waker(&self, waker: &Waker) {
        let mut w = self.waker.lock();
        if !w.as_ref().is_some_and(|w| w.will_wake(waker)) {
            *w = Some(waker.clone());
        }
    }

    fn push(&self, task: v8::Task) {
        self.tasks.lock().push(task);
        if let Some(w) = self.waker.lock().as_ref() {
            w.wake_by_ref();
        }
    }
}

/// Registered isolates by raw isolate pointer (what V8 passes to the platform callbacks).
fn registry() -> &'static Mutex<HashMap<usize, Arc<IsolateTasks>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<usize, Arc<IsolateTasks>>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

fn key_of(isolate: v8::UnsafeRawIsolatePtr) -> usize {
    // SAFETY: `UnsafeRawIsolatePtr` is `#[repr(transparent)]` over a raw pointer (the same
    // conversion `deno_core` uses for its own registry).
    unsafe { std::mem::transmute::<v8::UnsafeRawIsolatePtr, usize>(isolate) }
}

/// Start collecting foreground tasks for `isolate` (call on its Machine's thread, inside its
/// tokio runtime). [`unregister`] must run before the isolate is disposed: V8 reuses addresses.
pub fn register(isolate: v8::UnsafeRawIsolatePtr) -> Arc<IsolateTasks> {
    static GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let key = key_of(isolate);
    let entry = Arc::new(IsolateTasks {
        key,
        generation: GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        tasks: Mutex::default(),
        waker: Mutex::default(),
        handle: tokio::runtime::Handle::try_current().ok(),
    });
    registry().lock().insert(key, entry.clone());
    entry
}

/// Stop collecting; tasks still queued are dropped without running.
pub fn unregister(entry: &IsolateTasks) {
    let removed = registry().lock().remove(&entry.key);
    drop(removed);
    let pending = std::mem::take(&mut *entry.tasks.lock());
    drop(pending);
}

/// The registration of `key` if it is still generation `generation` (`None` = unregistered, or
/// the address now belongs to another isolate).
fn entry_of(key: usize, generation: Option<u64>) -> Option<Arc<IsolateTasks>> {
    let entry = registry().lock().get(&key).cloned()?;
    generation.is_none_or(|g| g == entry.generation).then_some(entry)
}

fn post(key: usize, generation: Option<u64>, task: v8::Task) {
    match entry_of(key, generation) {
        Some(e) => e.push(task),
        None => drop(task),
    }
}

fn post_delayed(key: usize, task: v8::Task, delay_in_seconds: f64) {
    let Some(entry) = entry_of(key, None) else { return };
    let Some(handle) = entry.handle.clone() else { return };
    let generation = entry.generation;
    let delay = Duration::try_from_secs_f64(delay_in_seconds).unwrap_or(Duration::ZERO);
    handle.spawn(async move {
        tokio::time::sleep(delay).await;
        post(key, Some(generation), task);
    });
}

struct RtPlatform;

impl v8::PlatformImpl for RtPlatform {
    fn post_task(&self, isolate: *mut c_void, task: v8::Task) {
        post(isolate as usize, None, task);
    }

    fn post_non_nestable_task(&self, isolate: *mut c_void, task: v8::Task) {
        post(isolate as usize, None, task);
    }

    fn post_delayed_task(&self, isolate: *mut c_void, task: v8::Task, delay_in_seconds: f64) {
        post_delayed(isolate as usize, task, delay_in_seconds);
    }

    fn post_non_nestable_delayed_task(&self, isolate: *mut c_void, task: v8::Task, delay_in_seconds: f64) {
        post_delayed(isolate as usize, task, delay_in_seconds);
    }

    // LCOV_EXCL_START: idle tasks are disabled (`idle_task_support = false`): V8 never posts one
    fn post_idle_task(&self, _isolate: *mut c_void, _task: v8::IdleTask) {}
    // LCOV_EXCL_STOP
}

/// The platform `init_v8_once` installs: `threads` worker threads.
pub fn new_platform(threads: u32) -> v8::SharedRef<v8::Platform> {
    v8::new_custom_platform(threads, false, false, RtPlatform).make_shared()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_isolate(addr: usize) -> v8::UnsafeRawIsolatePtr {
        // SAFETY: only used as a registry key, never dereferenced (see `key_of`).
        unsafe { std::mem::transmute::<usize, v8::UnsafeRawIsolatePtr>(addr) }
    }

    #[test]
    fn a_delayed_task_never_reaches_a_later_isolate_at_the_same_address() {
        let addr = 0x5eed_0000_usize;
        let first = register(fake_isolate(addr));
        let generation = first.generation;
        assert!(
            entry_of(addr, Some(generation)).is_some(),
            "delivered while the isolate lives"
        );
        unregister(&first);
        let second = register(fake_isolate(addr));
        assert!(
            entry_of(addr, Some(generation)).is_none(),
            "the old generation finds no target"
        );
        assert!(entry_of(addr, Some(second.generation)).is_some());
        assert!(
            entry_of(addr, None).is_some(),
            "immediate posts go to the current isolate"
        );
        unregister(&second);
    }
}
