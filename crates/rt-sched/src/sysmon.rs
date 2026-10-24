//! sysmon: a dedicated thread with no P.
//!
//! The tick starts at 20 µs and doubles after 1 ms of no action, up to `sysmon.max_tick`
//! (10 ms balanced); it resets to 20 µs after any action.
//!
//! | Duty | Cadence | Action |
//! |---|---|---|
//! | Queue timeouts | every tick | tasks not started within `queue_timeout` (waiting for a slot, a P, their artifact or memory) → `503 queue_timeout` |
//! | Limits | every tick | CPU/wall checks and aborts of started tasks; kill |
//! | Hog detection | every tick | Running > `time_slice` → WASM: `Interrupt::Yield`; Deno: hog handoff (both count a preemption) |
//! | Lost-wakeup guard | every tick | GRQ/LRQ non-empty + idle P + no spinning M → `wakep` |
//! | Re-kinding | `rebalance.interval` | p90 run-queue wait EWMA per kind; see [`crate::rekind`] |
//! | Autoscaler | `rebalance.interval` | adapt per-service concurrency limits, start waiting tasks that fit, publish warm targets (see [`crate::autoscale`]); on its own `rt-autoscale` thread with real threads, inside sysmon in simulation |
//! | Memory | `memory.check_interval` | wake `WaitingMemory` (below `Critical`); request an allocator purge at `High`/`Critical` (at most once per `memory.purge_min_interval`); enter `shed` at `Critical`, leave it below `High` |
//! | Stats | 1 s | scheduler gauges; spinner self-check (`rt_sched_spinner_mismatch_total`) |
//! | Trace | `RT_SCHEDTRACE` | print schedtrace line |

use std::time::Duration;

/// Initial / reset tick.
pub const MIN_TICK: Duration = Duration::from_micros(20);
/// Idle time after which the tick starts doubling.
pub const BACKOFF_AFTER: Duration = Duration::from_millis(1);

/// State of the adaptive tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickState {
    pub tick: Duration,
    /// Time since the last action.
    pub idle_for: Duration,
}

impl TickState {
    pub fn new() -> Self {
        Self {
            tick: MIN_TICK,
            idle_for: Duration::ZERO,
        }
    }

    /// Advance one tick: reset to `MIN_TICK` if `acted`; otherwise, after `BACKOFF_AFTER` of
    /// no action, double the tick up to `max_tick`.
    pub fn next(self, acted: bool, max_tick: Duration) -> TickState {
        if acted {
            return TickState::new();
        }
        let idle_for = self.idle_for + self.tick;
        let tick = if idle_for >= BACKOFF_AFTER {
            (self.tick * 2).min(max_tick.max(MIN_TICK))
        } else {
            self.tick
        };
        TickState { tick, idle_for }
    }
}

impl Default for TickState {
    fn default() -> Self {
        Self::new()
    }
}
