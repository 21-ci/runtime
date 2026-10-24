//! Frozen `wasi:clocks` (SECURITY.md, SEC-22, WSM-11).
//!
//! The guest's monotonic and wall clocks advance **only at I/O boundaries**: when a blocking
//! `wasi:io/poll` wait or a host call returns **after actually waiting** (a fetch response
//! arrived, a timer fired, a store call completed), never while synchronous guest code runs and
//! never for a pollable that was ready at once (`subscribe-duration(0)`). A busy loop therefore
//! measures zero elapsed time, which removes the guest's ability to build a timer out of its own
//! computation (Cloudflare model). Readings are whole milliseconds (the advertised resolution),
//! so even the time between I/O boundaries is only visible at 1 ms granularity.
//!
//! Across instance reuse (`wasm.reuse_instances`, warm instances) the two clocks differ:
//! - **monotonic** stays frozen over the gap: idle time between two tasks is not counted, so a
//!   guest cannot measure how long it sat idle (or how busy the node was);
//! - **wall** is re-anchored to real time when the next task starts ([`FrozenClock::resume`]),
//!   so `Date`-style timestamps stay correct no matter how long the instance was idle. Within
//!   one task the wall clock moves with the monotonic one (I/O boundaries only), and it never
//!   goes backwards on one instance.

use std::future::{poll_fn, Future};
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use wasmtime::component::{HasData, Resource, ResourceTable};
use wasmtime::Result;
use wasmtime_wasi::clocks::{HostMonotonicClock, HostWallClock};
use wasmtime_wasi::p2::bindings::clocks::monotonic_clock;
use wasmtime_wasi::p2::bindings::io::poll;
use wasmtime_wasi::p2::DynPollable;

/// Virtual time of one guest instance.
pub struct FrozenClock {
    /// Elapsed virtual nanoseconds since the clock was created.
    virt_ns: AtomicU64,
    /// Real instant virtual time was last synchronized to.
    synced_at: Mutex<Instant>,
    /// Unix time (ns) minus virtual time at the last anchor (creation or [`FrozenClock::resume`]):
    /// the wall clock is `wall_base_ns + virt_ns`.
    wall_base_ns: AtomicU64,
}

/// First value of the monotonic clock: non-zero so a guest never sees a "zero instant".
const MONOTONIC_BASE_NS: u64 = 1_000_000_000;
/// Clock resolution the guest sees (and is told): 1 ms.
pub const RESOLUTION_NS: u64 = 1_000_000;

fn quantize(ns: u64) -> u64 {
    ns - ns % RESOLUTION_NS
}

/// Run `fut`, reporting whether it had to wait (returned `Pending` at least once).
pub async fn waited<F: Future>(fut: F) -> (F::Output, bool) {
    let mut fut = pin!(fut);
    let mut waited = false;
    let out = poll_fn(|cx| match fut.as_mut().poll(cx) {
        Poll::Pending => {
            waited = true;
            Poll::Pending
        }
        Poll::Ready(v) => Poll::Ready(v),
    })
    .await;
    (out, waited)
}

fn unix_now_ns() -> u64 {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    ns.min(u128::from(u64::MAX)) as u64
}

impl FrozenClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            virt_ns: AtomicU64::new(0),
            synced_at: Mutex::new(Instant::now()),
            wall_base_ns: AtomicU64::new(unix_now_ns()),
        })
    }

    /// An I/O boundary was crossed: catch virtual time up with real time.
    pub fn advance(&self) {
        let mut last = self.synced_at.lock();
        let now = Instant::now();
        let step = now
            .saturating_duration_since(*last)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        *last = now;
        self.virt_ns.fetch_add(step, Ordering::Release);
    }

    /// Await an I/O operation of the guest (host call, poll): the clock catches up with real time
    /// only if it actually waited.
    pub async fn io<F: Future>(&self, fut: F) -> F::Output {
        let (out, waited) = waited(fut).await;
        if waited {
            self.advance();
        }
        out
    }

    /// A new task starts on this instance: idle time does not count on the monotonic clock, and
    /// the wall clock is re-anchored to real time (never moved backwards).
    pub fn resume(&self) {
        let mut last = self.synced_at.lock();
        *last = Instant::now();
        let base = unix_now_ns().saturating_sub(self.virt_ns.load(Ordering::Acquire));
        self.wall_base_ns.fetch_max(base, Ordering::AcqRel);
    }

    /// Virtual nanoseconds elapsed (what the guest can observe).
    pub fn elapsed_ns(&self) -> u64 {
        self.virt_ns.load(Ordering::Acquire)
    }

    /// The guest's wall clock, Unix nanoseconds.
    pub fn wall_ns(&self) -> u128 {
        u128::from(self.wall_base_ns.load(Ordering::Acquire)) + u128::from(self.elapsed_ns())
    }

    pub fn monotonic(self: &Arc<Self>) -> FrozenMonotonic {
        FrozenMonotonic(self.clone())
    }

    pub fn wall(self: &Arc<Self>) -> FrozenWall {
        FrozenWall(self.clone())
    }
}

pub struct FrozenMonotonic(Arc<FrozenClock>);

impl HostMonotonicClock for FrozenMonotonic {
    fn resolution(&self) -> u64 {
        // Coarse by design: 1 ms.
        RESOLUTION_NS
    }

    fn now(&self) -> u64 {
        MONOTONIC_BASE_NS + quantize(self.0.elapsed_ns())
    }
}

pub struct FrozenWall(Arc<FrozenClock>);

impl HostWallClock for FrozenWall {
    fn resolution(&self) -> Duration {
        Duration::from_nanos(RESOLUTION_NS)
    }

    fn now(&self) -> Duration {
        let ns = self.0.wall_ns();
        let ns = ns - ns % u128::from(RESOLUTION_NS);
        Duration::new((ns / 1_000_000_000) as u64, (ns % 1_000_000_000) as u32)
    }
}

// ---------------------------------------------------------------------------------------------
// `wasi:clocks/monotonic-clock` subscriptions that never yield when already due
// ---------------------------------------------------------------------------------------------

/// `wasi:clocks/monotonic-clock` over the frozen clock. Same as wasmtime-wasi's, except that a
/// deadline already due is ready at once: since wasmtime-wasi 48 it yields to tokio first, which
/// the I/O-boundary rule would count as a wait and so let a busy loop of ready pollables move the
/// clock (a timer for the guest).
pub struct FrozenMonotonicHost<'a> {
    pub table: &'a mut ResourceTable,
    pub clock: &'a Arc<FrozenClock>,
}

/// `HasData` for [`FrozenMonotonicHost`].
pub struct HasFrozenMonotonic;

impl HasData for HasFrozenMonotonic {
    type Data<'a> = FrozenMonotonicHost<'a>;
}

/// A monotonic-clock subscription: due now, at a real instant, or never.
enum Due {
    Now,
    At(tokio::time::Instant),
    Never,
}

#[async_trait::async_trait]
impl wasmtime_wasi::p2::Pollable for Due {
    async fn ready(&mut self) {
        match self {
            Due::Now => {}
            Due::At(at) => tokio::time::sleep_until(*at).await,
            Due::Never => std::future::pending().await,
        }
    }
}

impl FrozenMonotonicHost<'_> {
    fn subscribe_in(&mut self, duration: Duration) -> Result<Resource<DynPollable>> {
        let due = if duration.is_zero() {
            Due::Now
        } else {
            tokio::time::Instant::now()
                .checked_add(duration)
                .map_or(Due::Never, Due::At)
        };
        let r = self.table.push(due)?;
        wasmtime_wasi::p2::subscribe(self.table, r)
    }
}

impl monotonic_clock::Host for FrozenMonotonicHost<'_> {
    fn now(&mut self) -> Result<u64> {
        Ok(self.clock.monotonic().now())
    }

    fn resolution(&mut self) -> Result<u64> {
        Ok(RESOLUTION_NS)
    }

    fn subscribe_instant(&mut self, when: u64) -> Result<Resource<DynPollable>> {
        let now = self.clock.monotonic().now();
        self.subscribe_in(Duration::from_nanos(when.saturating_sub(now)))
    }

    fn subscribe_duration(&mut self, duration: u64) -> Result<Resource<DynPollable>> {
        self.subscribe_in(Duration::from_nanos(duration))
    }
}

// ---------------------------------------------------------------------------------------------
// `wasi:io/poll` with clock advancement
// ---------------------------------------------------------------------------------------------

/// The standard `wasi:io/poll` implementation (over the resource table), advancing the frozen
/// clock when a blocking wait returns after actually waiting.
pub struct FrozenPoll<'a> {
    pub table: &'a mut ResourceTable,
    pub clock: &'a FrozenClock,
}

/// `HasData` for [`FrozenPoll`], used with `wasi::io::poll::add_to_linker`.
pub struct HasFrozenPoll;

impl HasData for HasFrozenPoll {
    type Data<'a> = FrozenPoll<'a>;
}

impl poll::Host for FrozenPoll<'_> {
    async fn poll(&mut self, pollables: Vec<Resource<DynPollable>>) -> Result<Vec<u32>> {
        self.clock.io(poll::Host::poll(&mut *self.table, pollables)).await
    }
}

impl poll::HostPollable for FrozenPoll<'_> {
    async fn block(&mut self, pollable: Resource<DynPollable>) -> Result<()> {
        self.clock
            .io(poll::HostPollable::block(&mut *self.table, pollable))
            .await
    }

    async fn ready(&mut self, pollable: Resource<DynPollable>) -> Result<bool> {
        poll::HostPollable::ready(&mut *self.table, pollable).await
    }

    fn drop(&mut self, pollable: Resource<DynPollable>) -> Result<()> {
        poll::HostPollable::drop(&mut *self.table, pollable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_unix_ns() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    }

    #[test]
    fn resume_re_anchors_the_wall_clock_but_keeps_idle_time_off_the_monotonic_clock() {
        let clock = FrozenClock::new();
        std::thread::sleep(Duration::from_millis(20));
        clock.advance(); // an I/O boundary of the first task: both clocks move by ~20 ms
        let mono_before = clock.elapsed_ns();
        assert!(mono_before >= 20_000_000, "{mono_before}");

        // The instance sits idle between two tasks.
        std::thread::sleep(Duration::from_millis(120));
        clock.resume();

        assert_eq!(
            clock.elapsed_ns(),
            mono_before,
            "idle time is not on the monotonic clock"
        );
        let lag_ms = (real_unix_ns() as i128 - clock.wall_ns() as i128) / 1_000_000;
        assert!(
            (0..=10).contains(&lag_ms),
            "wall clock lags real time by {lag_ms} ms after resume"
        );
    }

    #[tokio::test]
    async fn io_that_did_not_wait_does_not_advance_the_clock() {
        let clock = FrozenClock::new();
        std::thread::sleep(Duration::from_millis(15)); // guest "compute"
        clock.io(async {}).await;
        assert_eq!(clock.elapsed_ns(), 0, "a ready operation is not an I/O boundary");
        clock.io(tokio::time::sleep(Duration::from_millis(5))).await;
        assert!(
            clock.elapsed_ns() >= 20_000_000,
            "a real wait catches up: {}",
            clock.elapsed_ns()
        );
    }

    #[test]
    fn readings_are_whole_milliseconds() {
        let clock = FrozenClock::new();
        std::thread::sleep(Duration::from_micros(2_500));
        clock.advance();
        let mono = clock.monotonic().now();
        assert_eq!(mono % RESOLUTION_NS, 0, "{mono}");
        assert!(mono >= MONOTONIC_BASE_NS + 2_000_000);
        let wall = clock.wall().now();
        assert_eq!(wall.subsec_nanos() % RESOLUTION_NS as u32, 0, "{wall:?}");
        assert_eq!(clock.monotonic().resolution(), RESOLUTION_NS);
    }

    #[test]
    fn the_wall_clock_never_moves_backwards_on_one_instance() {
        let clock = FrozenClock::new();
        let w0 = clock.wall_ns();
        clock.resume();
        let w1 = clock.wall_ns();
        clock.resume();
        assert!(clock.wall_ns() >= w1 && w1 >= w0);
    }

    #[test]
    fn the_wall_clock_advertises_the_same_one_millisecond_resolution() {
        let clock = FrozenClock::new();
        assert_eq!(clock.wall().resolution(), Duration::from_nanos(RESOLUTION_NS));
    }

    #[tokio::test]
    async fn a_subscription_already_due_is_ready_at_once_and_a_later_one_moves_the_clock_when_waited_for() {
        let clock = FrozenClock::new();
        let mut table = ResourceTable::new();
        let mut host = FrozenMonotonicHost {
            table: &mut table,
            clock: &clock,
        };
        assert_eq!(
            monotonic_clock::Host::resolution(&mut host).expect("resolution"),
            RESOLUTION_NS
        );
        let now = monotonic_clock::Host::now(&mut host).expect("now");
        let due = monotonic_clock::Host::subscribe_instant(&mut host, now).expect("an instant that is now");
        let past = monotonic_clock::Host::subscribe_instant(&mut host, 0).expect("an instant in the past");
        let later = monotonic_clock::Host::subscribe_duration(&mut host, 30_000_000).expect("30 ms from now");
        let borrow = |r: &Resource<DynPollable>| Resource::<DynPollable>::new_borrow(r.rep());
        let (due_b, past_b, later_b) = (borrow(&due), borrow(&past), borrow(&later));

        let mut poll = FrozenPoll {
            table: &mut table,
            clock: &clock,
        };
        assert!(poll::HostPollable::ready(&mut poll, borrow(&due_b))
            .await
            .expect("ready"));
        assert!(poll::HostPollable::ready(&mut poll, borrow(&past_b))
            .await
            .expect("ready"));
        assert!(!poll::HostPollable::ready(&mut poll, borrow(&later_b))
            .await
            .expect("ready"));
        let ready = poll::Host::poll(&mut poll, vec![borrow(&later_b), borrow(&due_b)])
            .await
            .expect("poll");
        assert_eq!(ready, vec![1], "only the pollable that is due is ready");
        assert_eq!(clock.elapsed_ns(), 0, "nothing waited, so the clock did not move");

        poll::HostPollable::block(&mut poll, borrow(&later_b))
            .await
            .expect("block");
        assert!(
            clock.elapsed_ns() >= 30_000_000,
            "blocking on a timer is an I/O boundary: {}",
            clock.elapsed_ns()
        );
        for r in [due, past, later] {
            poll::HostPollable::drop(&mut poll, r).expect("drop");
        }
    }
}
