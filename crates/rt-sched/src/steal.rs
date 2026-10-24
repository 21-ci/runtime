//! Work stealing rules.
//!
//! Only **same-kind, unbound** tasks are stolen; thieves take half of a victim's LRQ;
//! `runnext` is never stolen in v1; a spinning M makes `STEAL_PASSES` passes over a random
//! permutation of the other Ps of its kind.

use rand::seq::SliceRandom;
use rand::Rng;

/// Passes over the victim permutation before giving up.
pub const STEAL_PASSES: usize = 4;

/// Upper bound of simultaneously spinning Ms of one kind: `max(1, busy / 2)` (SCH-08).
pub fn max_spinning(busy_of_kind: usize) -> usize {
    (busy_of_kind / 2).max(1)
}

/// A random permutation of the indices `0..n` excluding `me`, drawn from `rng`
/// (seeded in simulation so victim choice is reproducible, SCH-21).
pub fn victim_order<R: Rng + ?Sized>(n: usize, me: usize, rng: &mut R) -> Vec<usize> {
    let mut v: Vec<usize> = (0..n).filter(|&i| i != me).collect();
    v.shuffle(rng);
    v
}

/// May a thief take this task from an LRQ? Bound (started) tasks never live in LRQs, so the
/// only requirement is that the thief's P has the same kind.
pub fn is_stealable(task_kind_idx: usize, thief_kind_idx: usize) -> bool {
    task_kind_idx == thief_kind_idx
}
