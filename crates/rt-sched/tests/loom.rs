//! Exhaustive interleaving tests of the lock-free structures: `LocalQueue` (owner vs thieves)
//! and `StateWord` (wake racing `Running -> Waiting`, SCH-07).
//!
//! Run with: `RUSTFLAGS="--cfg loom" cargo test -p rt-sched --release --test loom`
//!
//! NOTE for the implementation (lane I-A): under `cfg(loom)` the atomics (and any lock) inside
//! `queues.rs` and `task.rs` must come from `loom::sync` (e.g. `#[cfg(loom)] use loom::sync::atomic`)
//! otherwise loom sees no scheduling points and explores a single interleaving. The invariants
//! asserted here must hold for every interleaving of the public API.
#![cfg(loom)]

use loom::sync::Arc;
use loom::thread;
use rt_sched::queues::{LocalQueue, Push};
use rt_sched::task::{StateWord, TaskState};

fn drain(q: &LocalQueue<usize>) -> Vec<usize> {
    std::iter::from_fn(|| q.pop_front()).collect()
}

/// The owner keeps popping while a thief steals half: every item ends up exactly once either
/// popped by the owner, in the thief's queue, or still in the victim.
#[test]
fn lrq_owner_pop_vs_thief_steal_conserves_items() {
    loom::model(|| {
        let victim = Arc::new(LocalQueue::new(4));
        let dst = Arc::new(LocalQueue::new(4));
        for i in 0..3usize {
            assert_eq!(victim.push_back(i), Push::Queued);
        }

        let thief = {
            let (victim, dst) = (victim.clone(), dst.clone());
            thread::spawn(move || victim.steal_half_into(&dst))
        };
        let mut seen = Vec::new();
        while let Some(x) = victim.pop_front() {
            seen.push(x);
        }
        let stolen = thief.join().expect("thief");

        let mut in_dst = drain(&dst);
        assert_eq!(in_dst.len(), stolen, "the reported count matches what moved");
        seen.append(&mut in_dst);
        seen.append(&mut drain(&victim));
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2]);
    });
}

/// Two thieves race for the same victim: no item is taken twice, none is lost.
#[test]
fn lrq_two_thieves_never_take_the_same_item() {
    loom::model(|| {
        let victim = Arc::new(LocalQueue::new(4));
        for i in 0..4usize {
            assert_eq!(victim.push_back(i), Push::Queued);
        }
        let (d1, d2) = (Arc::new(LocalQueue::new(4)), Arc::new(LocalQueue::new(4)));

        let t1 = {
            let (v, d) = (victim.clone(), d1.clone());
            thread::spawn(move || v.steal_half_into(&d))
        };
        let t2 = {
            let (v, d) = (victim.clone(), d2.clone());
            thread::spawn(move || v.steal_half_into(&d))
        };
        t1.join().expect("thief 1");
        t2.join().expect("thief 2");

        let mut all = drain(&d1);
        all.extend(drain(&d2));
        all.extend(drain(&victim));
        all.sort_unstable();
        assert_eq!(all, vec![0, 1, 2, 3]);
    });
}

/// SCH-01 under concurrency: the owner overflows the LRQ while a thief steals; the overflow batch,
/// the stolen items and what remains are exactly the pushed items.
#[test]
fn lrq_overflow_racing_a_steal_conserves_items() {
    loom::model(|| {
        let victim = Arc::new(LocalQueue::new(2));
        let dst = Arc::new(LocalQueue::new(2));
        assert_eq!(victim.push_back(0usize), Push::Queued);
        assert_eq!(victim.push_back(1), Push::Queued);

        let thief = {
            let (v, d) = (victim.clone(), dst.clone());
            thread::spawn(move || v.steal_half_into(&d))
        };
        let mut seen = match victim.push_back(2) {
            Push::Queued => Vec::new(),
            Push::Overflow(batch) => batch, // would go to the GRQ
        };
        thief.join().expect("thief");

        seen.extend(drain(&dst));
        seen.extend(drain(&victim));
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2]);
    });
}

/// SCH-07 core: a wake racing the `Running -> Waiting` commit is never lost. Exactly one of the
/// two happens: `park()` sees `NOTIFIED` and the task is requeued, or `wake()` sees `Waiting` and
/// asks its caller to enqueue the task.
#[test]
fn state_wake_racing_park_is_never_lost() {
    loom::model(|| {
        let w = Arc::new(StateWord::new(TaskState::Running));
        let waker = {
            let w = w.clone();
            thread::spawn(move || w.wake())
        };
        let parked = w.park();
        let enqueue_by_waker = waker.join().expect("waker");

        assert_eq!(
            parked, enqueue_by_waker,
            "lost wakeup: parked={parked}, waker enqueued={enqueue_by_waker}"
        );
        if parked {
            assert_eq!(
                w.state(),
                TaskState::Runnable,
                "the waker moved it to Runnable and owns the enqueue"
            );
        } else {
            assert_eq!(
                w.state(),
                TaskState::Running,
                "NOTIFIED consumed: the P requeues the task"
            );
            assert!(!w.is_notified());
        }
    });
}

/// Two wakers race on a `Waiting` task: exactly one enqueues it.
#[test]
fn state_two_wakers_enqueue_exactly_once() {
    loom::model(|| {
        let w = Arc::new(StateWord::new(TaskState::Waiting));
        let a = {
            let w = w.clone();
            thread::spawn(move || w.wake())
        };
        let b = {
            let w = w.clone();
            thread::spawn(move || w.wake())
        };
        let (ea, eb) = (a.join().expect("waker a"), b.join().expect("waker b"));
        assert_eq!(u8::from(ea) + u8::from(eb), 1, "the task must be enqueued exactly once");
        assert_eq!(w.state(), TaskState::Runnable);
    });
}

/// Two wakes racing one park: the extra wake is coalesced, and whichever way it interleaves the
/// task is neither lost nor enqueued twice.
#[test]
fn state_two_wakes_and_a_park_never_lose_or_duplicate_the_task() {
    loom::model(|| {
        let w = Arc::new(StateWord::new(TaskState::Running));
        let wakers: Vec<_> = (0..2)
            .map(|_| {
                let w = w.clone();
                thread::spawn(move || w.wake())
            })
            .collect();
        let parked = w.park();
        let enqueued: u8 = wakers.into_iter().map(|t| u8::from(t.join().expect("waker"))).sum();

        if parked {
            assert_eq!(enqueued, 1, "parked: exactly one waker owns the enqueue");
            assert_eq!(w.state(), TaskState::Runnable);
        } else {
            assert_eq!(enqueued, 0, "not parked: the P requeues, nobody else may");
            assert_eq!(w.state(), TaskState::Running);
        }
    });
}

/// A full cycle: park, wake, the P resumes the task (`Runnable -> Running`), park again while a
/// second wake races it.
#[test]
fn state_resume_cycle_with_a_racing_wake() {
    loom::model(|| {
        let w = Arc::new(StateWord::new(TaskState::Running));
        assert!(w.park(), "no waker yet: the first park commits");
        assert_eq!(w.state(), TaskState::Waiting);

        let waker = {
            let w = w.clone();
            thread::spawn(move || w.wake())
        };
        // The "P" side: once the wake enqueued the task, resume it and park again.
        let enqueued = waker.join().expect("waker");
        assert!(enqueued);
        assert!(w.transition(TaskState::Runnable, TaskState::Running));

        let again = {
            let w = w.clone();
            thread::spawn(move || w.wake())
        };
        let parked = w.park();
        let enqueue = again.join().expect("second waker");
        assert_eq!(parked, enqueue, "lost wakeup on the second cycle");
    });
}
