//! Per-isolate memory account (B5.1, D-A2). A task may hold `limits.memory_mb` in total across:
//!
//! - the **V8 heap**: its limit grows through the near-heap-limit callback (`isolate.rs`);
//! - **`ArrayBuffer` backing stores**, which live outside the V8 heap limit: every isolate gets
//!   its own counting `ArrayBuffer::Allocator` ([`array_buffer_allocator`]) that refuses an
//!   allocation over the limit, so the guest sees a `RangeError` (V8 first collects garbage and
//!   retries) and can handle it;
//! - **backing stores made without the allocator**: buffers returned by ops (`fetch`/KV/blob
//!   bodies, `TextEncoder`, compression), and resizable `ArrayBuffer`s, which commit memory
//!   through their own reservation. V8 counts them in `HeapStatistics::external_memory` (with the
//!   allocator's): [`MemAccount::check`] samples it and charges the larger of the two. Growing a
//!   resizable buffer is admitted beforehand ([`MemAccount::admit_growth`], the bootstrap's
//!   `ArrayBuffer` wrappers): a single `fill` of a huge buffer cannot be interrupted;
//! - **`Blob` parts**, held by `deno_web` in Rust ([`CountedBlobStore`]);
//! - **`WebAssembly.Memory`**, which V8 counts nowhere: not through the allocator and not in
//!   `HeapStatistics::external_memory` (measured, see the B5.1 probe in `tests/limits_streaming.rs`).
//!   The bootstrap registers every memory the guest can reach (`op_rt_wasm_track`), held weakly
//!   (a collected memory no longer counts), and [`MemAccount::check`] sums their current
//!   `byteLength` (pages made accessible by `grow`, not the address space V8 reserves for guard
//!   regions).
//!
//! The samples are taken at every CPU-watchdog interrupt while guest code runs, after every
//! event-loop poll and before a response head leaves; over the limit the task is terminated as
//! `Killed(Oom)`, after one garbage collection when part of the usage may be garbage. The
//! interrupt never collects (a GC from an interrupt callback can deadlock in V8's `CppHeap`):
//! there, usage that may be garbage gets [`INTERRUPT_SLACK`] before the kill. A coarse process-wide backstop caps any single wasm memory at the largest
//! plan's `memory_mb` (`--wasm-max-mem-pages`, `config.rs`).
//!
//! Everything is charged to the task's governor reservation, which only grows (a high-water mark)
//! and is released with the isolate. Small buffers ride on the heap reservation
//! ([`BUFFER_ALLOWANCE`]).

use std::collections::HashMap;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use deno_core::url::Url;
use deno_core::v8;
use deno_web::{Blob, BlobPart, BlobStore, BlobStoreTrait};
use parking_lot::Mutex;
use rt_mem::Reservation;
use uuid::Uuid;

const MIB: u64 = 1024 * 1024;

/// `ArrayBuffer` bytes per isolate that are not charged on their own: bootstrap buffers and
/// ordinary request/response chunks fit in the slack of the V8 heap-floor reservation.
pub const BUFFER_ALLOWANCE: u64 = MIB;

/// How far past its limit a task that may hold garbage runs before the watchdog's interrupt
/// kills it ([`MemAccount::check_in_interrupt`]; at least a quarter of the limit).
pub const INTERRUPT_SLACK: u64 = 16 * MIB;

/// A `WebAssembly.Memory` the guest can reach, held weakly: once the guest drops it and V8
/// collects it, it stops counting.
struct WasmMem(v8::Weak<v8::WasmMemoryObject>);

// SAFETY: a `WasmMem` is created, read and dropped only on its isolate's thread (op, interrupt
// callback, driver poll, `MemAccount::close` when the isolate goes away); the `Mutex` around the
// list only exists because the account is shared with the allocator.
unsafe impl Send for WasmMem {}

/// Memory account of one isolate, shared by the near-heap-limit callback and the driver (isolate
/// thread) and by the `ArrayBuffer` allocator, which V8 may call from other threads (a backing
/// store can be freed on a GC helper thread, or after the isolate is gone if Rust still holds it).
pub struct MemAccount {
    state: Mutex<MemInner>,
    wasm: Mutex<Vec<WasmMem>>,
}

impl std::fmt::Debug for MemAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemAccount")
            .field("charged", &self.charged())
            .finish_non_exhaustive()
    }
}

struct MemInner {
    /// Pool isolate: its own (base) reservation; after `attach`: the task's.
    reservation: Option<Reservation>,
    /// A task is attached: the limit applies and usage is charged.
    attached: bool,
    /// `limits.memory_mb` in bytes (`u64::MAX` before attach).
    limit_bytes: u64,
    /// V8 heap limit granted so far.
    heap: u64,
    /// Live backing stores allocated through this isolate's allocator.
    buffers: u64,
    /// `HeapStatistics::external_memory` at the last sample: every backing store, also those
    /// made without the allocator (op results, resizable buffers).
    external: u64,
    /// Bytes of `Blob` parts held in Rust ([`CountedBlobStore`]).
    blobs: u64,
    /// `WebAssembly.Memory` bytes at the last [`MemAccount::check`].
    wasm: u64,
    /// Heap growth was refused or the total went over the limit: the task is being killed.
    oom: bool,
    /// An `ArrayBuffer` allocation (or growth) was refused (the guest got a `RangeError`).
    buffers_refused: bool,
}

impl MemInner {
    fn total_with(&self, heap: u64, buffers: u64) -> u64 {
        heap.saturating_add(buffers.max(self.external).saturating_sub(BUFFER_ALLOWANCE))
            .saturating_add(self.wasm)
            .saturating_add(self.blobs)
    }

    fn current(&self) -> u64 {
        self.total_with(self.heap, self.buffers)
    }

    /// Grow the reservation (never shrink) so it covers `total`.
    fn cover(&mut self, total: u64) -> bool {
        let Some(r) = self.reservation.as_mut() else {
            return true;
        };
        let have = r.bytes();
        total <= have || r.try_grow(total - have)
    }

    /// Admit `total` bytes of usage: within the limit and charged to the governor.
    fn admit(&mut self, total: u64) -> bool {
        !self.attached || (total <= self.limit_bytes && self.cover(total))
    }

    /// Something outside the V8 heap may be garbage a collection would free.
    fn collectable(&self) -> bool {
        self.buffers.max(self.external) > BUFFER_ALLOWANCE || self.wasm > 0
    }
}

impl MemAccount {
    /// A fresh isolate holding `reservation` (pool) or nothing (created for a task), with a V8
    /// heap limit of `heap` bytes.
    pub fn new(reservation: Option<Reservation>, heap: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(MemInner {
                reservation,
                attached: false,
                limit_bytes: u64::MAX,
                heap,
                buffers: 0,
                external: 0,
                blobs: 0,
                wasm: 0,
                oom: false,
                buffers_refused: false,
            }),
            wasm: Mutex::new(Vec::new()),
        })
    }

    /// Bind the isolate to a task: `reservation` replaces the pool's (dropping it releases the
    /// pool's bytes) and is topped up to the current usage. `false` = the governor refused (`oom`).
    pub fn attach(&self, reservation: Reservation, limit_bytes: u64) -> bool {
        let mut s = self.state.lock();
        s.reservation = Some(reservation);
        s.attached = true;
        s.limit_bytes = limit_bytes;
        s.buffers_refused = false;
        let total = s.current();
        s.oom = !s.cover(total);
        !s.oom
    }

    /// Near-heap-limit callback: may the heap limit grow from `current` by `step`? Before a task
    /// is attached only the pool reservation can pay for it.
    pub fn grow_heap(&self, current: u64, step: u64) -> bool {
        let mut s = self.state.lock();
        let want = current.saturating_add(step);
        let ok = if s.attached {
            let total = s.total_with(want, s.buffers);
            s.admit(total)
        } else {
            s.reservation.as_mut().is_some_and(|r| r.try_grow(step))
        };
        if ok {
            s.heap = want;
        } else {
            s.oom = true;
        }
        ok
    }

    /// The task is over its memory (heap growth refused, or [`MemAccount::check`] found it over).
    pub fn oom(&self) -> bool {
        self.state.lock().oom
    }

    /// An `ArrayBuffer` allocation or growth of this task was refused.
    pub fn buffers_refused(&self) -> bool {
        self.state.lock().buffers_refused
    }

    /// Bytes of live backing stores allocated through this isolate's allocator.
    pub fn buffer_bytes(&self) -> u64 {
        self.state.lock().buffers
    }

    /// Bytes of `Blob` parts held for this isolate.
    pub fn blob_bytes(&self) -> u64 {
        self.state.lock().blobs
    }

    /// What the isolate is charged at the last sample: V8 heap limit + `ArrayBuffer`s beyond the
    /// allowance + blob parts + wasm memories. A reused isolate (D-A4) carries this into its next
    /// task.
    pub fn charged(&self) -> u64 {
        self.state.lock().current()
    }

    fn charge_buffer(&self, len: u64) -> bool {
        let mut s = self.state.lock();
        let buffers = s.buffers.saturating_add(len);
        let total = s.total_with(s.heap, buffers);
        if !s.admit(total) {
            s.buffers_refused = true;
            return false;
        }
        s.buffers = buffers;
        true
    }

    fn release_buffer(&self, len: u64) {
        let mut s = self.state.lock();
        s.buffers = s.buffers.saturating_sub(len);
    }

    /// A `Blob` part was stored: it counts from now on (nothing can refuse it; the next
    /// [`MemAccount::check`] kills the task when it is over).
    fn charge_blob(&self, len: u64) {
        let mut s = self.state.lock();
        s.blobs = s.blobs.saturating_add(len);
    }

    fn release_blob(&self, len: u64) {
        let mut s = self.state.lock();
        s.blobs = s.blobs.saturating_sub(len);
    }

    /// Count `memory` against this task (called by `op_rt_wasm_track`; the bootstrap never
    /// registers one memory twice).
    pub fn track_wasm(&self, memory: v8::Weak<v8::WasmMemoryObject>) {
        self.wasm.lock().push(WasmMem(memory));
    }

    /// May the guest grow a backing store by `extra` bytes (a resizable `ArrayBuffer`, which
    /// commits memory without the allocator)? Samples first. A refusal is the guest's
    /// `RangeError` (`buffers_refused`), not a kill.
    pub fn admit_growth(&self, scope: &mut v8::PinScope<'_, '_, ()>, extra: u64) -> bool {
        self.sample(scope);
        let mut s = self.state.lock();
        let total = s.current().saturating_add(extra);
        if s.admit(total) {
            return true;
        }
        s.buffers_refused = true;
        false
    }

    /// Sample usage on the isolate's thread (interrupt callback, op, or between polls; `scope` must
    /// have a context entered: reading a wasm memory's buffer without one crashed V8): the
    /// `byteLength` of the live wasm memories and V8's external memory, then check the total
    /// against the limit and the governor. Over it, collect garbage once when something outside
    /// the heap may be garbage (unswept buffers, unreachable wasm memories still count) and check
    /// again: a GC that does not help ends in a kill, so this cannot repeat without progress.
    /// `false` = over the limit: `oom` is set and the caller terminates the task.
    pub fn check(&self, scope: &mut v8::PinScope<'_, '_, ()>) -> bool {
        if !self.sample(scope) {
            return true;
        }
        let gc = self.state.lock().collectable();
        // Outside the lock: the GC frees backing stores through the allocator.
        let _ = gc.then(|| scope.low_memory_notification());
        if gc && !self.sample(scope) {
            return true;
        }
        self.state.lock().oom = true;
        false
    }

    /// [`MemAccount::check`] for the CPU watchdog's interrupt callback, which must never collect
    /// garbage (a full GC from an interrupt can deadlock inside V8's `CppHeap`, measured). When
    /// part of the usage may be garbage, the kill waits until the usage passes the limit by
    /// [`INTERRUPT_SLACK`] (or a quarter of the limit): the next event-loop poll collects and
    /// applies the exact limit. A guest that never yields is bounded by the slack.
    pub fn check_in_interrupt(&self, scope: &mut v8::PinScope<'_, '_, ()>) -> bool {
        if !self.sample(scope) {
            return true;
        }
        let mut s = self.state.lock();
        if s.collectable() && s.current() <= s.limit_bytes.saturating_add(INTERRUPT_SLACK.max(s.limit_bytes / 4)) {
            return true;
        }
        s.oom = true;
        false
    }

    /// Refresh the samples (dropping wasm memories V8 collected); `true` = over the limit (or
    /// the governor refused the growth).
    fn sample(&self, scope: &mut v8::PinScope<'_, '_, ()>) -> bool {
        let wasm = {
            let mut list = self.wasm.lock();
            let mut sum = 0u64;
            list.retain(|m| match m.0.to_local(scope) {
                Some(memory) => {
                    sum = sum.saturating_add(memory.buffer().byte_length() as u64);
                    true
                }
                None => false,
            });
            sum
        };
        let external = scope.get_heap_statistics().external_memory() as u64;
        let mut s = self.state.lock();
        s.wasm = wasm;
        s.external = external;
        let total = s.current();
        !s.admit(total)
    }

    /// The isolate is gone: release the reservation now (backing stores Rust still holds are no
    /// longer the task's) and forget the wasm memories. Runs on the isolate's thread.
    pub fn close(&self) {
        let reservation = {
            let mut s = self.state.lock();
            s.attached = false;
            s.reservation.take()
        };
        drop(reservation);
        let tracked = std::mem::take(&mut *self.wasm.lock());
        drop(tracked);
    }
}

/// `deno_web`'s blob store with every part charged to the isolate's account (D-A2): `new Blob`
/// copies its data into Rust, where neither the allocator nor V8 sees it. A part is released
/// when `deno_web` removes it (the `Blob` was collected). Slices are charged too (their bytes are
/// shared with the sliced part, so this over-counts: safe, and slices are short-lived).
pub struct CountedBlobStore {
    inner: BlobStore,
    sizes: Mutex<HashMap<Uuid, u64>>,
    account: Arc<MemAccount>,
}

impl CountedBlobStore {
    pub fn shared(account: Arc<MemAccount>) -> Arc<dyn BlobStoreTrait> {
        Arc::new(Self {
            inner: BlobStore::default(),
            sizes: Mutex::default(),
            account,
        })
    }
}

impl std::fmt::Debug for CountedBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountedBlobStore")
            .field("parts", &self.sizes.lock().len())
            .finish_non_exhaustive()
    }
}

impl BlobStoreTrait for CountedBlobStore {
    fn insert_part(&self, part: Arc<dyn BlobPart + Send + Sync>) -> Uuid {
        let len = part.size() as u64;
        let id = self.inner.insert_part(part);
        self.sizes.lock().insert(id, len);
        self.account.charge_blob(len);
        id
    }

    fn get_part(&self, id: &Uuid) -> Option<Arc<dyn BlobPart + Send + Sync>> {
        self.inner.get_part(id)
    }

    fn remove_part(&self, id: &Uuid) -> Option<Arc<dyn BlobPart + Send + Sync>> {
        if let Some(len) = self.sizes.lock().remove(id) {
            self.account.release_blob(len);
        }
        self.inner.remove_part(id)
    }

    fn get_object_url(&self, url: Url) -> Option<Arc<Blob>> {
        self.inner.get_object_url(url)
    }

    fn insert_object_url(&self, blob: Blob, maybe_location: Option<Url>) -> Url {
        self.inner.insert_object_url(blob, maybe_location)
    }

    fn remove_object_url(&self, url: &Url) {
        self.inner.remove_object_url(url)
    }

    fn clear(&self) {
        let released: u64 = self.sizes.lock().drain().map(|(_, len)| len).sum();
        self.account.release_blob(released);
        self.inner.clear()
    }
}

/// A counting `ArrayBuffer::Allocator` charging `account` (B5.1). V8 owns it through a shared
/// pointer that every backing store it allocated also holds, so it (and the `Arc` it keeps)
/// outlives the isolate until the last such backing store is freed.
pub fn array_buffer_allocator(account: &Arc<MemAccount>) -> v8::UniqueRef<v8::Allocator> {
    static VTABLE: v8::RustAllocatorVtable<MemAccount> = v8::RustAllocatorVtable {
        allocate: ab_allocate,
        allocate_uninitialized: ab_allocate_uninitialized,
        free: ab_free,
        drop: ab_drop,
    };
    // SAFETY: the handle is a strong `Arc` reference handed to the allocator; V8 calls `drop`
    // exactly once, when it deletes the allocator, and `ab_drop` gives that reference back.
    unsafe { v8::new_rust_allocator(Arc::into_raw(account.clone()), &VTABLE) }
}

/// Charge `len`, then allocate with `alloc`. A refusal returns null: V8 collects garbage, retries,
/// and throws `RangeError: Array buffer allocation failed` when JS asked for the buffer.
fn allocate_with(account: &MemAccount, len: usize, alloc: impl FnOnce(usize) -> *mut c_void) -> *mut c_void {
    // Never unwind into V8.
    let charged = catch_unwind(AssertUnwindSafe(|| account.charge_buffer(len as u64))).unwrap_or(false);
    if !charged {
        return std::ptr::null_mut();
    }
    let p = alloc(len.max(1));
    if p.is_null() {
        account.release_buffer(len as u64);
    }
    p
}

unsafe extern "C" fn ab_allocate(account: &MemAccount, len: usize) -> *mut c_void {
    // SAFETY: plain C allocation, freed by `ab_free`.
    allocate_with(account, len, |n| unsafe { libc::calloc(n, 1) })
}

unsafe extern "C" fn ab_allocate_uninitialized(account: &MemAccount, len: usize) -> *mut c_void {
    // SAFETY: plain C allocation, freed by `ab_free`.
    allocate_with(account, len, |n| unsafe { libc::malloc(n) })
}

unsafe extern "C" fn ab_free(account: &MemAccount, data: *mut c_void, len: usize) {
    // SAFETY: `data` came from `ab_allocate*` (V8 passes back what this allocator returned).
    unsafe { libc::free(data) };
    account.release_buffer(len as u64);
}

unsafe extern "C" fn ab_drop(handle: *const MemAccount) {
    // SAFETY: `handle` is the `Arc::into_raw` of `array_buffer_allocator`, reclaimed once.
    drop(unsafe { Arc::from_raw(handle) });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attached(limit_mb: u64) -> Arc<MemAccount> {
        let a = MemAccount::new(None, 16 * MIB);
        assert!(a.attach(
            Reservation::detached(rt_mem::MemCategory::DenoHeap, 4 * MIB),
            limit_mb * MIB
        ));
        a
    }

    #[test]
    fn buffers_count_against_the_limit_after_the_allowance() {
        let a = attached(64);
        assert!(
            a.charge_buffer(BUFFER_ALLOWANCE + 48 * MIB),
            "16 heap + 48 charged = 64"
        );
        assert!(!a.charge_buffer(MIB), "one more MiB is over");
        assert!(a.buffers_refused());
        assert!(!a.oom(), "a refused buffer is the guest's RangeError, not a kill");
        a.release_buffer(8 * MIB);
        assert!(a.charge_buffer(MIB));
    }

    #[test]
    fn before_attach_nothing_is_refused() {
        // Bootstrap buffers (tenant-neutral, tiny) are never refused: an embedder-side refusal
        // (`v8::ArrayBuffer::New`) would abort the process.
        let a = MemAccount::new(None, 16 * MIB);
        assert!(a.charge_buffer(512 * MIB));
        assert_eq!(a.buffer_bytes(), 512 * MIB);
    }

    #[test]
    fn heap_growth_shares_the_limit_with_buffers() {
        let a = attached(32);
        assert!(a.charge_buffer(BUFFER_ALLOWANCE + 8 * MIB));
        assert!(a.grow_heap(16 * MIB, 8 * MIB), "24 heap + 8 buffers = 32");
        assert!(!a.grow_heap(24 * MIB, 8 * MIB));
        assert!(a.oom());
    }

    #[test]
    fn blob_parts_are_charged_on_insert_and_released_on_removal_or_clear() {
        let a = attached(64);
        let store = CountedBlobStore::shared(a.clone());
        let part =
            |n: usize| -> Arc<dyn BlobPart + Send + Sync> { Arc::new(deno_web::InMemoryBlobPart::from(vec![0u8; n])) };
        let first = store.insert_part(part(1000));
        let second = store.insert_part(part(500));
        assert_eq!(a.blob_bytes(), 1500);
        assert!(store.get_part(&first).is_some());
        assert!(store.remove_part(&first).is_some());
        assert_eq!(a.blob_bytes(), 500, "a removed part is released");
        assert!(store.remove_part(&first).is_none(), "and only once");
        assert_eq!(a.blob_bytes(), 500);
        assert!(format!("{store:?}").contains("parts: 1"), "{store:?}");
        store.clear();
        assert_eq!(a.blob_bytes(), 0, "clearing releases what is left");
        assert!(store.get_part(&second).is_none());
        assert!(format!("{a:?}").contains("MemAccount"));
    }

    #[test]
    fn object_urls_are_kept_by_the_inner_store_until_revoked() {
        let a = attached(64);
        let store = CountedBlobStore::shared(a);
        let blob = Blob {
            media_type: "text/plain".into(),
            parts: vec![Arc::new(deno_web::InMemoryBlobPart::from(b"hi".to_vec()))],
        };
        let url = store.insert_object_url(blob, None);
        assert_eq!(
            store
                .get_object_url(url.clone())
                .map(|b| b.media_type.clone())
                .as_deref(),
            Some("text/plain")
        );
        store.remove_object_url(&url);
        assert!(store.get_object_url(url).is_none());
    }

    #[test]
    fn a_failed_allocation_gives_back_what_it_charged() {
        let a = attached(64);
        let p = allocate_with(&a, 4096, |_| std::ptr::null_mut());
        assert!(p.is_null(), "the allocator reports the failure to V8");
        assert_eq!(a.buffer_bytes(), 0);
    }

    #[test]
    fn before_a_task_is_attached_the_heap_grows_on_the_pools_reservation() {
        let a = MemAccount::new(
            Some(Reservation::detached(rt_mem::MemCategory::DenoHeap, 4 * MIB)),
            4 * MIB,
        );
        assert!(a.grow_heap(4 * MIB, MIB));
        assert!(!a.oom());
    }
}
