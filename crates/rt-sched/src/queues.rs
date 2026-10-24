//! Run queues: per-P local run queue (LRQ, ring of 256) and per-kind global run queue (GRQ).
//!
//! Semantics follow Go's scheduler:
//! - the owner pushes to the tail and pops from the head;
//! - on overflow, **half of the LRQ plus the new item** move to the GRQ, FIFO preserved
//!   (`runqputslow`, SCH-01);
//! - thieves take **half** of a victim's LRQ (SCH-03).
//!
//! The LRQ is a bounded ring (Go's `runq`): `head` is advanced by consumers with a CAS (the owner's
//! `pop_front` and every thief), `tail` is written only by producers. Producers are serialized by
//! a tiny lock because external submits push into a P's LRQ from arbitrary threads (placement,
//! placement rule 1), so the ring is lock-free for consumers and wait-free for readers.
//!
//! Slots hold `AtomicPtr<T>` to boxed items: a consumer first reads the pointer(s) and only then
//! commits with a CAS on `head`; if the CAS fails the pointers are discarded unread, so a stale
//! read of a slot that the producer meanwhile reused is harmless (and not a data race).
//!
//! Under `cfg(loom)` the atomics and the producer lock come from `loom::sync` so that
//! `tests/loom.rs` explores their interleavings.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ptr;

use parking_lot::Mutex;

#[cfg(loom)]
use loom::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Go's `runqsize`.
pub const DEFAULT_LRQ_CAPACITY: usize = 256;

/// Result of [`LocalQueue::push_back`].
#[derive(Debug, PartialEq, Eq)]
pub enum Push<T> {
    /// Queued locally.
    Queued,
    /// The LRQ was full: these items (the oldest half of the LRQ in FIFO order, then the new
    /// item) must be placed on the GRQ.
    Overflow(Vec<T>),
}

/// Serializes producers of one LRQ.
struct ProducerLock {
    #[cfg(not(loom))]
    inner: Mutex<()>,
    #[cfg(loom)]
    inner: loom::sync::Mutex<()>,
}

impl ProducerLock {
    fn new() -> Self {
        #[cfg(not(loom))]
        {
            Self { inner: Mutex::new(()) }
        }
        #[cfg(loom)]
        {
            Self {
                inner: loom::sync::Mutex::new(()),
            }
        }
    }

    #[cfg(not(loom))]
    fn lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.inner.lock()
    }

    #[cfg(loom)]
    fn lock(&self) -> loom::sync::MutexGuard<'_, ()> {
        self.inner.lock().expect("loom producer lock is never poisoned")
    }
}

/// Local run queue of one P.
pub struct LocalQueue<T> {
    cap: usize,
    /// Index of the next item to consume. Monotonic (wrapping); advanced by CAS.
    head: AtomicUsize,
    /// Index one past the last item. Monotonic (wrapping); written only under `prod`.
    tail: AtomicUsize,
    slots: Box<[AtomicPtr<T>]>,
    prod: ProducerLock,
    /// The queue owns `T`s (Send/Sync follow `Mutex<T>`: `T: Send` suffices).
    _owns: PhantomData<Mutex<T>>,
}

impl<T> LocalQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity >= 2, "LRQ capacity must be at least 2");
        let slots: Vec<AtomicPtr<T>> = (0..capacity).map(|_| AtomicPtr::new(ptr::null_mut())).collect();
        Self {
            cap: capacity,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            slots: slots.into_boxed_slice(),
            prod: ProducerLock::new(),
            _owns: PhantomData,
        }
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn len(&self) -> usize {
        // `head` first: the tail is never behind a head value that was already observed.
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(head).min(self.cap)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn slot(&self, index: usize) -> &AtomicPtr<T> {
        &self.slots[index % self.cap]
    }

    /// Enqueue at the tail. When full, returns the overflow batch (`cap/2` oldest items
    /// followed by `item`) and leaves `cap - cap/2` items in the queue.
    pub fn push_back(&self, item: T) -> Push<T> {
        let new = Box::into_raw(Box::new(item));
        let _producer = self.prod.lock();
        // Only modified under the producer lock: exact.
        let tail = self.tail.load(Ordering::Relaxed);
        loop {
            let head = self.head.load(Ordering::Acquire);
            if tail.wrapping_sub(head) < self.cap {
                // The slot held item `tail - cap`, which has index < head: already consumed.
                self.slot(tail).store(new, Ordering::Relaxed);
                self.tail.store(tail.wrapping_add(1), Ordering::Release);
                return Push::Queued;
            }
            // Full: take the oldest half (Go `runqputslow`).
            let n = self.cap / 2;
            let grabbed: Vec<*mut T> = (0..n)
                .map(|i| self.slot(head.wrapping_add(i)).load(Ordering::Relaxed))
                .collect();
            if self
                .head
                .compare_exchange(head, head.wrapping_add(n), Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                // A consumer advanced `head` meanwhile (the queue may not be full any more).
                continue;
            }
            let mut batch: Vec<T> = Vec::with_capacity(n + 1);
            for p in grabbed {
                // SAFETY: the CAS above made us the exclusive owner of the items `[head, head + n)`;
                // every pointer was produced by `Box::into_raw` in `push_back`/`steal_half_into`.
                batch.push(*unsafe { Box::from_raw(p) });
            }
            // SAFETY: `new` came from `Box::into_raw` above and was not published.
            batch.push(*unsafe { Box::from_raw(new) });
            return Push::Overflow(batch);
        }
    }

    /// Dequeue from the head (the owner, or anyone draining the queue).
    pub fn pop_front(&self) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            if head == tail {
                return None;
            }
            let p = self.slot(head).load(Ordering::Relaxed);
            if self
                .head
                .compare_exchange(head, head.wrapping_add(1), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // SAFETY: the CAS made us the exclusive owner of item `head`; the pointer came
                // from `Box::into_raw` and is still the one published for that index.
                return Some(*unsafe { Box::from_raw(p) });
            }
        }
    }

    /// Thief: take half (rounded up) of this queue's items into `dst` (FIFO order preserved),
    /// returning how many moved. `dst` must have room; items that do not fit are left in `self`.
    /// `dst` must be a different queue.
    pub fn steal_half_into(&self, dst: &LocalQueue<T>) -> usize {
        let _producer = dst.prod.lock();
        let dtail = dst.tail.load(Ordering::Relaxed);
        let dhead = dst.head.load(Ordering::Acquire);
        let room = dst.cap.saturating_sub(dtail.wrapping_sub(dhead));
        if room == 0 {
            return 0;
        }
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            let n = tail.wrapping_sub(head);
            if n == 0 {
                return 0;
            }
            if n > self.cap {
                // `head` moved between the two loads: inconsistent snapshot, retry.
                continue;
            }
            let take = n.div_ceil(2).min(room);
            // Copy the pointers into slots of `dst` that nobody can see before its tail moves.
            for i in 0..take {
                let p = self.slot(head.wrapping_add(i)).load(Ordering::Relaxed);
                dst.slot(dtail.wrapping_add(i)).store(p, Ordering::Relaxed);
            }
            if self
                .head
                .compare_exchange(head, head.wrapping_add(take), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                dst.tail.store(dtail.wrapping_add(take), Ordering::Release);
                return take;
            }
        }
    }

    /// Remove everything (hog handoff: move the hogged P's LRQ to the GRQ), FIFO order.
    pub fn drain_all(&self) -> Vec<T> {
        std::iter::from_fn(|| self.pop_front()).collect()
    }

    /// Snapshot of the queue length for tracing.
    pub fn len_hint(&self) -> usize {
        self.len()
    }
}

impl<T> Drop for LocalQueue<T> {
    fn drop(&mut self) {
        while self.pop_front().is_some() {}
    }
}

/// Global run queue of one kind.
pub struct GlobalQueue<T> {
    inner: Mutex<VecDeque<T>>,
    /// Mirror of the length so emptiness checks on the pick path take no lock.
    len: std::sync::atomic::AtomicUsize,
}

impl<T> Default for GlobalQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GlobalQueue<T> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            len: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn sync_len(&self, q: &VecDeque<T>) {
        self.len.store(q.len(), std::sync::atomic::Ordering::Release);
    }

    pub fn len(&self) -> usize {
        self.len.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push_back(&self, item: T) {
        let mut q = self.inner.lock();
        q.push_back(item);
        self.sync_len(&q);
    }

    /// Append a batch preserving its order (overflow from an LRQ).
    pub fn push_batch(&self, items: impl IntoIterator<Item = T>) {
        let mut q = self.inner.lock();
        q.extend(items);
        self.sync_len(&q);
    }

    pub fn pop_front(&self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let mut q = self.inner.lock();
        let item = q.pop_front();
        self.sync_len(&q);
        item
    }

    /// Take up to `n` items from the head.
    pub fn take_batch(&self, n: usize) -> Vec<T> {
        let mut q = self.inner.lock();
        let n = n.min(q.len());
        let batch: Vec<T> = q.drain(..n).collect();
        self.sync_len(&q);
        batch
    }

    /// Batch size when a P refills from the GRQ: `min(len / ps_of_kind + 1, lrq_capacity / 2)`.
    pub fn refill_batch_size(len: usize, ps_of_kind: usize, lrq_capacity: usize) -> usize {
        (len / ps_of_kind.max(1) + 1).min(lrq_capacity / 2).max(1)
    }
}
