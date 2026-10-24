//! `VirtualClock`: deterministic time for simulations and trigger tests.
//!
//! - `now()`/`unix_ms()` advance only through [`SimClock::advance`];
//! - `sleep(d)` completes when the clock has advanced by `d`;
//! - CPU time is per *virtual thread* (`set_thread`): `burn_cpu(d)` charges the current virtual
//!   thread without moving wall time; the simulation driver decides how CPU maps to wall time.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rt_sched::{BoxFuture, Clock, SimClock};

struct Inner {
    base: Instant,
    unix_base_ms: i64,
    elapsed: Duration,
    cpu: HashMap<u32, Duration>,
    current_thread: u32,
    next_seq: u64,
    /// `(deadline since base, seq)` → waker of the sleeping future.
    timers: BTreeMap<(Duration, u64), Option<Waker>>,
}

/// A clock moved by hand. Cheap to clone (shared state).
#[derive(Clone)]
pub struct VirtualClock {
    inner: Arc<Mutex<Inner>>,
}

impl VirtualClock {
    /// Starts at virtual time zero; `unix_ms()` starts at 1_700_000_000_000.
    pub fn new() -> Self {
        Self::with_unix_ms(1_700_000_000_000)
    }

    pub fn with_unix_ms(unix_ms: i64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                base: Instant::now(),
                unix_base_ms: unix_ms,
                elapsed: Duration::ZERO,
                cpu: HashMap::new(),
                current_thread: 0,
                next_seq: 0,
                timers: BTreeMap::new(),
            })),
        }
    }

    /// Select the virtual thread whose CPU counter `thread_cpu_time`/`burn_cpu` use.
    pub fn set_thread(&self, thread: u32) {
        self.inner.lock().current_thread = thread;
    }

    /// Virtual time elapsed since creation.
    pub fn elapsed(&self) -> Duration {
        self.inner.lock().elapsed
    }

    /// Number of pending sleep timers.
    pub fn pending_timers(&self) -> usize {
        self.inner.lock().timers.len()
    }

    /// CPU time charged to `thread` so far.
    pub fn cpu_of(&self, thread: u32) -> Duration {
        self.inner.lock().cpu.get(&thread).copied().unwrap_or_default()
    }

    /// Step the **wall** clock (`unix_ms`) to `unix_ms`, forwards or backwards, the way an NTP
    /// correction or an operator moves a host's time. The monotonic clock (`now()`), the CPU
    /// counters and pending sleeps are untouched: only code that reads wall time sees the jump.
    pub fn set_unix_ms(&self, unix_ms: i64) {
        let mut g = self.inner.lock();
        let elapsed_ms = i64::try_from(g.elapsed.as_millis()).unwrap_or(i64::MAX);
        g.unix_base_ms = unix_ms.saturating_sub(elapsed_ms);
    }

    /// Step the wall clock by `delta_ms` (negative = backwards); see [`Self::set_unix_ms`].
    pub fn step_unix_ms(&self, delta_ms: i64) {
        let mut g = self.inner.lock();
        g.unix_base_ms = g.unix_base_ms.saturating_add(delta_ms);
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> Instant {
        let g = self.inner.lock();
        g.base + g.elapsed
    }

    fn thread_cpu_time(&self) -> Duration {
        let g = self.inner.lock();
        g.cpu.get(&g.current_thread).copied().unwrap_or_default()
    }

    fn unix_ms(&self) -> i64 {
        let g = self.inner.lock();
        g.unix_base_ms + g.elapsed.as_millis() as i64
    }

    fn sleep(&self, d: Duration) -> BoxFuture<'static, ()> {
        let mut g = self.inner.lock();
        let deadline = g.elapsed + d;
        let seq = g.next_seq;
        g.next_seq += 1;
        g.timers.insert((deadline, seq), None);
        drop(g);
        Box::pin(Sleep {
            clock: self.inner.clone(),
            key: (deadline, seq),
        })
    }

    fn burn_cpu(&self, d: Duration) {
        let mut g = self.inner.lock();
        let t = g.current_thread;
        *g.cpu.entry(t).or_default() += d;
    }
}

impl SimClock for VirtualClock {
    fn advance(&self, d: Duration) {
        let mut wake = Vec::new();
        {
            let mut g = self.inner.lock();
            g.elapsed += d;
            let now = g.elapsed;
            let due: Vec<(Duration, u64)> = g.timers.range(..=(now, u64::MAX)).map(|(k, _)| *k).collect();
            for k in due {
                if let Some(Some(w)) = g.timers.get(&k) {
                    wake.push(w.clone());
                }
                // Keep the (now-expired) entry so the Sleep future can observe completion and
                // remove it itself; wake only once.
                if let Some(slot) = g.timers.get_mut(&k) {
                    *slot = None;
                }
            }
        }
        for w in wake {
            w.wake();
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        let g = self.inner.lock();
        // Only timers not yet due count.
        g.timers.keys().find(|(d, _)| *d > g.elapsed).map(|(d, _)| g.base + *d)
    }
}

struct Sleep {
    clock: Arc<Mutex<Inner>>,
    key: (Duration, u64),
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut g = self.clock.lock();
        if g.elapsed >= self.key.0 {
            g.timers.remove(&self.key);
            return Poll::Ready(());
        }
        if let Some(slot) = g.timers.get_mut(&self.key) {
            *slot = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.clock.lock().timers.remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TK-001: the wall clock can be stepped backwards and forwards (NTP corrections) while the
    /// monotonic clock and pending sleeps are unaffected.
    #[test]
    fn the_wall_clock_steps_both_ways_and_the_monotonic_clock_does_not_move() {
        let c = VirtualClock::with_unix_ms(10_000);
        let start = c.now();
        let sleeping = c.sleep(Duration::from_millis(500));
        c.advance(Duration::from_millis(100));
        assert_eq!(c.unix_ms(), 10_100);

        c.step_unix_ms(-3_600_000);
        assert_eq!(c.unix_ms(), 10_100 - 3_600_000, "an hour backwards");
        assert_eq!(
            c.now(),
            start + Duration::from_millis(100),
            "monotonic time is untouched"
        );
        assert_eq!(c.pending_timers(), 1, "the sleep is still pending");

        c.set_unix_ms(5_000);
        assert_eq!(c.unix_ms(), 5_000);
        c.advance(Duration::from_millis(400));
        assert_eq!(c.unix_ms(), 5_400, "wall time keeps advancing from the new value");
        assert_eq!(c.now(), start + Duration::from_millis(500));
        drop(sleeping);
        assert_eq!(c.pending_timers(), 0);
    }
}
