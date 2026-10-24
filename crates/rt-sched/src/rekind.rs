//! Elastic re-kinding.
//!
//! A pure decision function so it can be unit-tested and replayed in simulation:
//!
//! - Constraints: `sched.kinds.{deno,wasm}.{min,max}`, Σ Ps = `sched.processors`; a kind with
//!   zero live services has an effective min of 0.
//! - `pressure(kind) = (runnable + running + waiting_memory) / Ps_of_kind`. The EWMA of the p90
//!   run-queue wait (slot granted → started) is sampled with it and reported in [`KindLoad`]; the
//!   rule below does not use it, the autoscaler's CPU-saturation signal does.
//! - If `pressure(A) > up_threshold` and kind B has an **idle** P with `Ps_B > min_B`,
//!   `Ps_A < max_A` and `bound_count == 0` → mark it `Rekinding`, switch its kind, `wakep(A)`.
//! - Hysteresis: one re-kind per P per `cooldown`.

use std::time::Duration;

use rt_types::{ProcId, TaskKind};

/// A processor that may be donated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DonorCandidate {
    pub proc: ProcId,
    /// Idle and its Machine has `bound_count == 0`.
    pub rekindable: bool,
    /// Time since this P last changed kind.
    pub since_last_rekind: Duration,
}

/// Load of one kind at sampling time.
#[derive(Clone, Debug, PartialEq)]
pub struct KindLoad {
    pub kind: TaskKind,
    pub procs: u32,
    pub min: u32,
    pub max: u32,
    pub runnable: u32,
    pub running: u32,
    pub waiting_memory: u32,
    /// EWMA of the p90 run-queue wait (informational here; see the module doc).
    pub p90_queue_wait: Duration,
    /// Candidates among this kind's Ps (only meaningful when donating).
    pub candidates: Vec<DonorCandidate>,
}

impl KindLoad {
    /// `(runnable + running + waiting_memory) / procs`.
    pub fn pressure(&self) -> f64 {
        let demand = f64::from(self.runnable + self.running + self.waiting_memory);
        demand / f64::from(self.procs.max(1))
    }
}

/// One re-kinding to perform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rekind {
    pub proc: ProcId,
    pub from: TaskKind,
    pub to: TaskKind,
}

/// Decide at most one re-kind per hungry kind this round.
///
/// A kind is *hungry* when its pressure is strictly above `up_threshold` and it is below its
/// `max`. Its donor is the non-hungry guest kind that still has more than its `min` Ps and owns a
/// free (idle, no bound tasks), cooled-down candidate P; the candidate with the lowest id moves.
/// Build processors never move. Hungry kinds are served in descending pressure (stable on the
/// input order), so the result is deterministic for equal inputs.
pub fn decide(loads: &[KindLoad], up_threshold: f64, cooldown: Duration) -> Vec<Rekind> {
    let guest = |k: TaskKind| k != TaskKind::Build;

    let mut hungry: Vec<usize> = (0..loads.len())
        .filter(|&i| guest(loads[i].kind) && loads[i].pressure() > up_threshold && loads[i].procs < loads[i].max)
        .collect();
    // `sort_by` is stable: equal pressures keep the input order.
    hungry.sort_by(|&a, &b| {
        loads[b]
            .pressure()
            .partial_cmp(&loads[a].pressure())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut procs: Vec<u32> = loads.iter().map(|l| l.procs).collect();
    let mut moved: Vec<ProcId> = Vec::new();
    let mut out = Vec::new();

    for a in hungry {
        let mut best: Option<(usize, ProcId, f64)> = None;
        for (b, donor) in loads.iter().enumerate() {
            if b == a || !guest(donor.kind) || donor.kind == loads[a].kind {
                continue;
            }
            if procs[b] <= donor.min || donor.pressure() > up_threshold {
                continue;
            }
            let candidate = donor
                .candidates
                .iter()
                .filter(|c| c.rekindable && c.since_last_rekind >= cooldown && !moved.contains(&c.proc))
                .map(|c| c.proc)
                .min();
            if let Some(proc) = candidate {
                let pressure = donor.pressure();
                if best.is_none_or(|(_, _, p)| pressure < p) {
                    best = Some((b, proc, pressure));
                }
            }
        }
        if let Some((b, proc, _)) = best {
            out.push(Rekind {
                proc,
                from: loads[b].kind,
                to: loads[a].kind,
            });
            procs[b] -= 1;
            procs[a] += 1;
            moved.push(proc);
        }
    }
    out
}
