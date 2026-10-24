//! Guest clock (SECURITY.md). `security.clock = frozen` (default): `Date.now()` and
//! `performance.now()` advance only when the executor turns the event loop (timer and I/O
//! boundaries), never during synchronous execution. `coarse`: live time floored to 1 ms.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rt_config::ClockMode;

#[derive(Debug)]
pub struct ClockState {
    mode: ClockMode,
    start: Instant,
    frozen_epoch_ms: f64,
    frozen_perf_ms: f64,
}

fn epoch_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0)
}

impl ClockState {
    pub fn new(mode: ClockMode) -> Self {
        let now = epoch_ms();
        Self {
            mode,
            start: Instant::now(),
            frozen_epoch_ms: now,
            frozen_perf_ms: 0.0,
        }
    }

    /// Called by the executor right before the event loop is polled: this is the only place the
    /// frozen clock moves.
    pub fn advance(&mut self) {
        self.frozen_epoch_ms = epoch_ms();
        self.frozen_perf_ms = self.start.elapsed().as_secs_f64() * 1000.0;
    }

    /// `Date.now()` in whole milliseconds.
    pub fn now_ms(&self) -> f64 {
        match self.mode {
            ClockMode::Frozen => self.frozen_epoch_ms.floor(),
            ClockMode::Coarse => epoch_ms().floor(),
        }
    }

    /// `performance.now()`.
    pub fn perf_ms(&self) -> f64 {
        match self.mode {
            ClockMode::Frozen => self.frozen_perf_ms,
            ClockMode::Coarse => (self.start.elapsed().as_secs_f64() * 1000.0).floor(),
        }
    }
}
