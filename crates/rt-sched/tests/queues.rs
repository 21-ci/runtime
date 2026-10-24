//! Run queues (LRQ / GRQ), the task state word and plain Processor / Machine state.
//!
//! Covers the queue-level halves of SCH-01 (overflow moves half + the new item to the GRQ) and
//! SCH-03 (a thief takes half), plus model-based property tests of every queue invariant:
//! FIFO order, capacity, conservation (nothing lost, nothing duplicated).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use proptest::collection::vec;
use proptest::prelude::*;
use rt_sched::machine::Machine;
use rt_sched::processor::Processor;
use rt_sched::queues::{GlobalQueue, LocalQueue, Push, DEFAULT_LRQ_CAPACITY};
use rt_sched::task::{Binding, StateWord, Task, TaskShared, TaskState};
use rt_types::{MachineId, ProcId, ProcStatus, TaskId, TaskKind};

// ---------------------------------------------------------------------------------------------
// LocalQueue: examples
// ---------------------------------------------------------------------------------------------

#[test]
fn lrq_default_capacity_is_256() {
    assert_eq!(DEFAULT_LRQ_CAPACITY, 256);
    assert_eq!(LocalQueue::<u32>::new(DEFAULT_LRQ_CAPACITY).capacity(), 256);
}

#[test]
#[should_panic(expected = "capacity")]
fn lrq_rejects_capacity_below_two() {
    let _ = LocalQueue::<u32>::new(1);
}

#[test]
fn lrq_is_fifo() {
    let q = LocalQueue::new(8);
    assert!(q.is_empty());
    for i in 0..5u32 {
        assert_eq!(q.push_back(i), Push::Queued);
    }
    assert_eq!(q.len(), 5);
    assert_eq!(q.len_hint(), 5);
    let got: Vec<u32> = std::iter::from_fn(|| q.pop_front()).collect();
    assert_eq!(got, vec![0, 1, 2, 3, 4]);
    assert!(q.is_empty());
    assert_eq!(q.pop_front(), None);
}

/// SCH-01 (queue level): P0's LRQ is full (256), another task is placed -> 128 LRQ tasks + the
/// new one move to the GRQ, FIFO order preserved inside the moved batch.
#[test]
fn ac_sch_01_lrq_overflow_moves_oldest_half_plus_new_item() {
    let q = LocalQueue::new(DEFAULT_LRQ_CAPACITY);
    for i in 0..256u32 {
        assert_eq!(q.push_back(i), Push::Queued, "item {i} fits");
    }
    assert_eq!(q.len(), 256);

    let overflow = q.push_back(256);
    let Push::Overflow(batch) = overflow else {
        panic!("a full LRQ must overflow, got {overflow:?}")
    };

    let mut expected: Vec<u32> = (0..128).collect();
    expected.push(256);
    assert_eq!(batch, expected, "the 128 oldest tasks, FIFO, then the new one");
    assert_eq!(batch.len(), 129);
    assert_eq!(q.len(), 128, "128 tasks stay in the LRQ");

    let rest: Vec<u32> = std::iter::from_fn(|| q.pop_front()).collect();
    assert_eq!(rest, (128..256).collect::<Vec<u32>>(), "the newer half stays, in order");
}

#[test]
fn lrq_overflow_with_small_and_odd_capacities() {
    // cap = 2: [0, 1] full; push 2 -> batch [0, 2], [1] stays.
    let q = LocalQueue::new(2);
    assert_eq!(q.push_back(0u32), Push::Queued);
    assert_eq!(q.push_back(1), Push::Queued);
    assert_eq!(q.push_back(2), Push::Overflow(vec![0, 2]));
    assert_eq!(q.len(), 1);
    assert_eq!(q.pop_front(), Some(1));

    // cap = 3: half = 1.
    let q = LocalQueue::new(3);
    for i in 0..3u32 {
        assert_eq!(q.push_back(i), Push::Queued);
    }
    assert_eq!(q.push_back(3), Push::Overflow(vec![0, 3]));
    assert_eq!(q.len(), 2);

    // cap = 5: half = 2.
    let q = LocalQueue::new(5);
    for i in 0..5u32 {
        assert_eq!(q.push_back(i), Push::Queued);
    }
    assert_eq!(q.push_back(5), Push::Overflow(vec![0, 1, 5]));
    assert_eq!(q.len(), 3);
}

#[test]
fn lrq_accepts_new_items_again_after_an_overflow() {
    let q = LocalQueue::new(4);
    for i in 0..4u32 {
        assert_eq!(q.push_back(i), Push::Queued);
    }
    assert_eq!(q.push_back(4), Push::Overflow(vec![0, 1, 4]));
    assert_eq!(q.len(), 2);
    assert_eq!(q.push_back(5), Push::Queued);
    assert_eq!(q.push_back(6), Push::Queued);
    assert_eq!(q.len(), 4);
    let got: Vec<u32> = std::iter::from_fn(|| q.pop_front()).collect();
    assert_eq!(got, vec![2, 3, 5, 6]);
}

/// SCH-03 (queue level): P0 has 100 runnable tasks, P1 is idle and searches -> P1 steals 50,
/// P0 keeps 50.
#[test]
fn ac_sch_03_steal_takes_half_of_100() {
    let victim = LocalQueue::new(DEFAULT_LRQ_CAPACITY);
    let thief = LocalQueue::new(DEFAULT_LRQ_CAPACITY);
    for i in 0..100u32 {
        assert_eq!(victim.push_back(i), Push::Queued);
    }

    assert_eq!(victim.steal_half_into(&thief), 50);
    assert_eq!(victim.len(), 50, "P0 keeps 50");
    assert_eq!(thief.len(), 50, "P1 holds the 50 it stole");

    let stolen: Vec<u32> = std::iter::from_fn(|| thief.pop_front()).collect();
    assert_eq!(stolen, (0..50).collect::<Vec<u32>>(), "the oldest half, FIFO");
    let kept: Vec<u32> = std::iter::from_fn(|| victim.pop_front()).collect();
    assert_eq!(kept, (50..100).collect::<Vec<u32>>());
}

#[test]
fn lrq_steal_rounds_half_up() {
    for (n, want) in [
        (0usize, 0usize),
        (1, 1),
        (2, 1),
        (3, 2),
        (4, 2),
        (101, 51),
        (255, 128),
        (256, 128),
    ] {
        let src = LocalQueue::new(DEFAULT_LRQ_CAPACITY);
        let dst = LocalQueue::new(DEFAULT_LRQ_CAPACITY);
        for i in 0..n {
            assert_eq!(src.push_back(i), Push::Queued);
        }
        assert_eq!(src.steal_half_into(&dst), want, "stealing from {n}");
        assert_eq!(src.len(), n - want);
        assert_eq!(dst.len(), want);
    }
}

#[test]
fn lrq_steal_is_limited_by_destination_room() {
    let src = LocalQueue::new(16);
    let dst = LocalQueue::new(4);
    for i in 0..10u32 {
        assert_eq!(src.push_back(i), Push::Queued);
    }
    assert_eq!(dst.push_back(100), Push::Queued);
    assert_eq!(dst.push_back(101), Push::Queued);

    // half of 10 is 5, but only 2 slots are free: 2 move, 8 stay.
    assert_eq!(src.steal_half_into(&dst), 2);
    assert_eq!(src.len(), 8);
    assert_eq!(dst.len(), 4);
    let got: Vec<u32> = std::iter::from_fn(|| dst.pop_front()).collect();
    assert_eq!(got, vec![100, 101, 0, 1]);
    assert_eq!(
        src.pop_front(),
        Some(2),
        "items that did not fit were not lost or reordered"
    );
}

#[test]
fn lrq_steal_into_a_full_destination_moves_nothing() {
    let src = LocalQueue::new(8);
    let dst = LocalQueue::new(2);
    for i in 0..4u32 {
        assert_eq!(src.push_back(i), Push::Queued);
    }
    assert_eq!(dst.push_back(7), Push::Queued);
    assert_eq!(dst.push_back(8), Push::Queued);
    assert_eq!(src.steal_half_into(&dst), 0);
    assert_eq!(src.len(), 4);
    assert_eq!(dst.len(), 2);
}

#[test]
fn lrq_drain_all_returns_fifo_and_empties() {
    let q = LocalQueue::new(8);
    for i in 0..6u32 {
        assert_eq!(q.push_back(i), Push::Queued);
    }
    assert_eq!(q.pop_front(), Some(0));
    assert_eq!(q.drain_all(), vec![1, 2, 3, 4, 5]);
    assert!(q.is_empty());
    assert_eq!(q.drain_all(), Vec::<u32>::new());
}

// ---------------------------------------------------------------------------------------------
// LocalQueue: model-based properties
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum LrqOp {
    Push,
    Pop,
    Steal,
    PopDst,
    Drain,
}

fn lrq_op() -> impl Strategy<Value = LrqOp> {
    prop_oneof![
        4 => Just(LrqOp::Push),
        2 => Just(LrqOp::Pop),
        2 => Just(LrqOp::Steal),
        1 => Just(LrqOp::PopDst),
        1 => Just(LrqOp::Drain),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The LRQ behaves exactly like a bounded FIFO whose overflow hands back the oldest
    /// `cap/2` items plus the new one, and whose steal moves `min(ceil(n/2), room)` oldest items.
    #[test]
    fn lrq_matches_the_bounded_fifo_model(cap in 2usize..24, ops in vec(lrq_op(), 0..300)) {
        let src = LocalQueue::new(cap);
        let dst = LocalQueue::new(cap);
        let mut msrc: VecDeque<u32> = VecDeque::new();
        let mut mdst: VecDeque<u32> = VecDeque::new();
        let mut next = 0u32;

        for op in ops {
            match op {
                LrqOp::Push => {
                    let item = next;
                    next += 1;
                    match src.push_back(item) {
                        Push::Queued => {
                            prop_assert!(msrc.len() < cap, "queued although full");
                            msrc.push_back(item);
                        }
                        Push::Overflow(batch) => {
                            prop_assert_eq!(msrc.len(), cap, "overflow although not full");
                            let mut expect: Vec<u32> = msrc.drain(..cap / 2).collect();
                            expect.push(item);
                            prop_assert_eq!(batch, expect);
                        }
                    }
                }
                LrqOp::Pop => prop_assert_eq!(src.pop_front(), msrc.pop_front()),
                LrqOp::PopDst => prop_assert_eq!(dst.pop_front(), mdst.pop_front()),
                LrqOp::Steal => {
                    let take = msrc.len().div_ceil(2).min(cap - mdst.len());
                    prop_assert_eq!(src.steal_half_into(&dst), take);
                    for _ in 0..take {
                        mdst.push_back(msrc.pop_front().expect("model has the items"));
                    }
                }
                LrqOp::Drain => {
                    let drained = src.drain_all();
                    prop_assert_eq!(drained, msrc.drain(..).collect::<Vec<u32>>());
                }
            }
            prop_assert_eq!(src.len(), msrc.len());
            prop_assert_eq!(dst.len(), mdst.len());
            prop_assert!(src.len() <= cap && dst.len() <= cap);
        }
    }

    /// Whatever the interleaving of pushes, overflows, steals and pops: every item is either
    /// still queued, was popped, or was handed to the GRQ - exactly once.
    #[test]
    fn lrq_never_loses_or_duplicates_items(cap in 2usize..16, ops in vec(lrq_op(), 0..300)) {
        let src = LocalQueue::new(cap);
        let dst = LocalQueue::new(cap);
        let mut seen: Vec<u32> = Vec::new(); // popped or overflowed
        let mut next = 0u32;

        for op in ops {
            match op {
                LrqOp::Push => {
                    if let Push::Overflow(batch) = src.push_back(next) {
                        seen.extend(batch);
                    }
                    next += 1;
                }
                LrqOp::Pop => seen.extend(src.pop_front()),
                LrqOp::PopDst => seen.extend(dst.pop_front()),
                LrqOp::Steal => {
                    src.steal_half_into(&dst);
                }
                LrqOp::Drain => seen.extend(src.drain_all()),
            }
        }
        seen.extend(src.drain_all());
        seen.extend(dst.drain_all());
        seen.sort_unstable();
        prop_assert_eq!(seen, (0..next).collect::<Vec<u32>>());
    }

    /// Stealing half from `n` items always leaves the victim the newer `floor(n/2)` items in order.
    #[test]
    fn lrq_steal_keeps_the_newer_half_in_order(n in 0usize..64) {
        let src = LocalQueue::new(64);
        let dst = LocalQueue::new(64);
        for i in 0..n {
            prop_assert_eq!(src.push_back(i), Push::Queued);
        }
        let moved = src.steal_half_into(&dst);
        prop_assert_eq!(moved, n.div_ceil(2));
        prop_assert_eq!(dst.drain_all(), (0..moved).collect::<Vec<usize>>());
        prop_assert_eq!(src.drain_all(), (moved..n).collect::<Vec<usize>>());
    }
}

// ---------------------------------------------------------------------------------------------
// LocalQueue: concurrency (real threads; loom variants live in tests/loom.rs)
// ---------------------------------------------------------------------------------------------

#[test]
fn lrq_concurrent_owner_and_thieves_conserve_items() {
    const N: u32 = 60_000;
    const THIEVES: usize = 3;

    let src = Arc::new(LocalQueue::<u32>::new(DEFAULT_LRQ_CAPACITY));
    let grq = Arc::new(GlobalQueue::<u32>::new());
    let done = Arc::new(AtomicBool::new(false));

    let thieves: Vec<_> = (0..THIEVES)
        .map(|_| {
            let (src, done) = (src.clone(), done.clone());
            std::thread::spawn(move || {
                let dst = LocalQueue::<u32>::new(DEFAULT_LRQ_CAPACITY);
                let mut got = Vec::new();
                loop {
                    let moved = src.steal_half_into(&dst);
                    got.extend(dst.drain_all());
                    if moved == 0 && done.load(Ordering::Acquire) && src.is_empty() {
                        break;
                    }
                    std::thread::yield_now();
                }
                got
            })
        })
        .collect();

    let mut mine = Vec::new();
    for i in 0..N {
        match src.push_back(i) {
            Push::Queued => {}
            Push::Overflow(batch) => grq.push_batch(batch),
        }
        if i % 7 == 0 {
            mine.extend(src.pop_front());
        }
    }
    done.store(true, Ordering::Release);
    while let Some(x) = src.pop_front() {
        mine.push(x);
    }

    let mut all = mine;
    for t in thieves {
        all.extend(t.join().expect("thief thread"));
    }
    while let Some(x) = grq.pop_front() {
        all.push(x);
    }
    assert_eq!(all.len(), N as usize, "no item lost or duplicated");
    all.sort_unstable();
    assert_eq!(all, (0..N).collect::<Vec<u32>>());
}

// ---------------------------------------------------------------------------------------------
// GlobalQueue
// ---------------------------------------------------------------------------------------------

#[test]
fn grq_is_fifo_and_batches_keep_order() {
    let g = GlobalQueue::new();
    assert!(g.is_empty());
    g.push_back(1u32);
    g.push_back(2);
    g.push_batch([3, 4, 5]);
    assert_eq!(g.len(), 5);
    assert_eq!(g.pop_front(), Some(1));
    assert_eq!(g.take_batch(2), vec![2, 3]);
    assert_eq!(g.take_batch(10), vec![4, 5], "takes at most what is there");
    assert!(g.is_empty());
    assert_eq!(g.pop_front(), None);
    assert_eq!(g.take_batch(3), Vec::<u32>::new());
}

#[test]
fn grq_refill_batch_size_is_len_over_ps_plus_one_capped_at_half_lrq() {
    // n = min(len / Ps_of_kind + 1, lrq_capacity / 2)
    assert_eq!(GlobalQueue::<u32>::refill_batch_size(100, 4, 256), 26);
    assert_eq!(GlobalQueue::<u32>::refill_batch_size(1000, 1, 256), 128);
    assert_eq!(GlobalQueue::<u32>::refill_batch_size(10_000, 2, 256), 128);
    assert_eq!(GlobalQueue::<u32>::refill_batch_size(0, 4, 256), 1);
    assert_eq!(GlobalQueue::<u32>::refill_batch_size(3, 4, 256), 1);
    assert_eq!(
        GlobalQueue::<u32>::refill_batch_size(5, 0, 256),
        6,
        "0 Ps is treated as 1"
    );
    assert_eq!(GlobalQueue::<u32>::refill_batch_size(50, 1, 2), 1);
}

#[derive(Clone, Debug)]
enum GrqOp {
    Push(u32),
    Batch(Vec<u32>),
    Pop,
    Take(usize),
}

fn grq_op() -> impl Strategy<Value = GrqOp> {
    prop_oneof![
        3 => any::<u32>().prop_map(GrqOp::Push),
        2 => vec(any::<u32>(), 0..8).prop_map(GrqOp::Batch),
        2 => Just(GrqOp::Pop),
        2 => (0usize..10).prop_map(GrqOp::Take),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn grq_matches_the_fifo_model(ops in vec(grq_op(), 0..200)) {
        let g = GlobalQueue::new();
        let mut m: VecDeque<u32> = VecDeque::new();
        for op in ops {
            match op {
                GrqOp::Push(x) => { g.push_back(x); m.push_back(x); }
                GrqOp::Batch(xs) => { g.push_batch(xs.clone()); m.extend(xs); }
                GrqOp::Pop => prop_assert_eq!(g.pop_front(), m.pop_front()),
                GrqOp::Take(n) => {
                    let want: Vec<u32> = m.drain(..n.min(m.len())).collect();
                    prop_assert_eq!(g.take_batch(n), want);
                }
            }
            prop_assert_eq!(g.len(), m.len());
            prop_assert_eq!(g.is_empty(), m.is_empty());
        }
    }

    #[test]
    fn grq_refill_batch_size_stays_in_bounds(len in 0usize..100_000, ps in 0usize..64, cap in 2usize..1024) {
        let n = GlobalQueue::<u32>::refill_batch_size(len, ps, cap);
        prop_assert!(n >= 1);
        prop_assert!(n <= (cap / 2).max(1));
        prop_assert!(n <= len / ps.max(1) + 1);
    }
}

// ---------------------------------------------------------------------------------------------
// StateWord: examples
// ---------------------------------------------------------------------------------------------

#[test]
fn state_word_starts_in_the_given_state_without_notification() {
    for st in [
        TaskState::New,
        TaskState::Runnable,
        TaskState::Running,
        TaskState::Waiting,
        TaskState::WaitingRuntime,
        TaskState::WaitingMemory,
        TaskState::Done,
    ] {
        let w = StateWord::new(st);
        assert_eq!(w.state(), st);
        assert!(!w.is_notified());
    }
}

#[test]
fn state_word_store_overwrites_state_and_clears_notified() {
    let w = StateWord::new(TaskState::Running);
    assert!(!w.wake(), "wake of a Running task only notifies");
    assert!(w.is_notified());
    w.store(TaskState::Runnable);
    assert_eq!(w.state(), TaskState::Runnable);
    assert!(!w.is_notified());
}

#[test]
fn state_word_transition_is_a_compare_and_swap() {
    let w = StateWord::new(TaskState::New);
    assert!(
        !w.transition(TaskState::Runnable, TaskState::Running),
        "wrong `from` fails"
    );
    assert_eq!(w.state(), TaskState::New);
    assert!(w.transition(TaskState::New, TaskState::Runnable));
    assert!(w.transition(TaskState::Runnable, TaskState::Running));
    assert_eq!(w.state(), TaskState::Running);
}

#[test]
fn wake_of_a_waiting_task_makes_it_runnable_and_asks_the_caller_to_enqueue() {
    let w = StateWord::new(TaskState::Waiting);
    assert!(w.wake(), "the waker must enqueue exactly once");
    assert_eq!(w.state(), TaskState::Runnable);
    assert!(!w.wake(), "second wake: already Runnable, nothing to enqueue");
    assert_eq!(w.state(), TaskState::Runnable);
}

#[test]
fn wake_of_a_running_task_sets_notified_instead_of_enqueueing() {
    let w = StateWord::new(TaskState::Running);
    assert!(!w.wake());
    assert_eq!(w.state(), TaskState::Running);
    assert!(w.is_notified());
}

#[test]
fn wake_is_a_noop_in_every_other_state() {
    for st in [
        TaskState::New,
        TaskState::Runnable,
        TaskState::WaitingRuntime,
        TaskState::WaitingMemory,
        TaskState::Done,
    ] {
        let w = StateWord::new(st);
        assert!(!w.wake(), "{st:?}");
        assert_eq!(w.state(), st, "{st:?}");
        assert!(!w.is_notified(), "{st:?}");
    }
}

#[test]
fn park_commits_running_to_waiting_when_nobody_woke_the_task() {
    let w = StateWord::new(TaskState::Running);
    assert!(w.park());
    assert_eq!(w.state(), TaskState::Waiting);
    assert!(!w.is_notified());
}

#[test]
fn park_after_a_wake_keeps_the_task_running_so_the_p_requeues_it() {
    let w = StateWord::new(TaskState::Running);
    assert!(!w.wake());
    assert!(
        !w.park(),
        "NOTIFIED was set: no lost wakeup, the task must be requeued, not parked"
    );
    assert_eq!(w.state(), TaskState::Running);
    assert!(!w.is_notified(), "the notification is consumed");
    assert!(w.park(), "a later park (no new wake) commits");
    assert_eq!(w.state(), TaskState::Waiting);
}

#[test]
fn several_wakes_before_a_park_coalesce_into_one_requeue() {
    let w = StateWord::new(TaskState::Running);
    for _ in 0..5 {
        assert!(!w.wake());
    }
    assert!(!w.park());
    assert!(w.park(), "only one notification was pending");
}

// ---------------------------------------------------------------------------------------------
// StateWord: model-based property
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum WordOp {
    Wake,
    Park,
    Resume,
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Single-threaded `wake` / `park` / "owner resumes a Runnable task" against a model of
    /// `(state, notified)`: a wake is never lost - after any interleaving, a task that is
    /// `Waiting` has no pending notification, and every wake either enqueued the task once or
    /// was remembered in `NOTIFIED`.
    #[test]
    fn state_word_never_loses_a_wake(ops in vec(prop_oneof![Just(WordOp::Wake), Just(WordOp::Park), Just(WordOp::Resume)], 0..200)) {
        let w = StateWord::new(TaskState::Running);
        let mut state = TaskState::Running;
        let mut notified = false;
        for op in ops {
            match op {
                WordOp::Wake => {
                    let enqueue = w.wake();
                    match state {
                        TaskState::Running => { notified = true; prop_assert!(!enqueue); }
                        TaskState::Waiting => { state = TaskState::Runnable; prop_assert!(enqueue); }
                        _ => prop_assert!(!enqueue),
                    }
                }
                WordOp::Park => {
                    if state != TaskState::Running { continue; }
                    let committed = w.park();
                    if notified { notified = false; prop_assert!(!committed); }
                    else { state = TaskState::Waiting; prop_assert!(committed); }
                }
                WordOp::Resume => {
                    if state == TaskState::Runnable {
                        prop_assert!(w.transition(TaskState::Runnable, TaskState::Running));
                        state = TaskState::Running;
                    }
                }
            }
            prop_assert_eq!(w.state(), state);
            prop_assert_eq!(w.is_notified(), notified);
            if state == TaskState::Waiting { prop_assert!(!notified, "a parked task must not hold a notification"); }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Processor / Machine plain state
// ---------------------------------------------------------------------------------------------

fn bare_task(id: u64, kind: TaskKind) -> Arc<Task> {
    Arc::new(Task {
        shared: TaskShared::new(TaskId(id), kind),
        spec: Mutex::new(None),
        enqueued_at: Instant::now(),
        binding: Mutex::new(Binding::Unbound),
        last_p: Mutex::new(None),
        done_tx: Mutex::new(None),
    })
}

#[test]
fn processor_starts_idle_with_empty_queues() {
    let p = Processor::new(ProcId(3), TaskKind::WASM, 256);
    assert_eq!(p.id, ProcId(3));
    assert_eq!(p.kind(), TaskKind::WASM);
    assert_eq!(p.status(), ProcStatus::Idle);
    assert_eq!(p.machine(), None);
    assert_eq!(p.lrq.capacity(), 256);
    assert!(p.lrq.is_empty());
    assert!(p.runnext.lock().is_none());
    assert_eq!(p.snapshot().schedtick, 0);
}

#[test]
fn processor_kind_status_and_machine_are_settable() {
    let p = Processor::new(ProcId(0), TaskKind::DENO, 8);
    p.set_kind(TaskKind::WASM);
    p.set_status(ProcStatus::Rekinding);
    p.set_machine(Some(MachineId(5)));
    assert_eq!(p.kind(), TaskKind::WASM);
    assert_eq!(p.status(), ProcStatus::Rekinding);
    assert_eq!(p.machine(), Some(MachineId(5)));
    p.set_machine(None);
    assert_eq!(p.machine(), None);
    for kind in TaskKind::ALL {
        p.set_kind(kind);
        assert_eq!(p.kind(), kind);
    }
}

#[test]
fn processor_snapshot_reports_queue_lengths_and_runnext() {
    let p = Processor::new(ProcId(1), TaskKind::DENO, 8);
    assert!(matches!(p.lrq.push_back(bare_task(10, TaskKind::DENO)), Push::Queued));
    assert!(matches!(p.lrq.push_back(bare_task(11, TaskKind::DENO)), Push::Queued));
    *p.runnext.lock() = Some(bare_task(12, TaskKind::DENO));
    let snap = p.snapshot();
    assert_eq!(snap.id, ProcId(1));
    assert_eq!(snap.kind, TaskKind::DENO);
    assert_eq!(snap.lrq_len, 2);
    assert_eq!(snap.runnext, Some(TaskId(12)));
}

#[test]
fn machine_starts_without_a_p_and_without_bound_tasks() {
    let m = Machine::new(MachineId(2));
    assert_eq!(m.id, MachineId(2));
    assert_eq!(m.proc(), None);
    assert_eq!(m.bound_count.load(Ordering::Relaxed), 0);
    assert!(m.bound_ready.lock().is_empty());
    m.set_proc(Some(ProcId(4)));
    assert_eq!(m.proc(), Some(ProcId(4)));
    let snap = m.snapshot();
    assert_eq!(snap.id, MachineId(2));
    assert_eq!(snap.proc, Some(ProcId(4)));
    assert_eq!(snap.bound_count, 0);
}
