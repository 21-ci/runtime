//! Time sources. The scheduler, triggers and executors only ever read time through [`Clock`],
//! so `rt-testkit`'s `VirtualClock` makes whole-node simulations deterministic.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// A boxed `Send` future (timers).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Monotonic time + per-thread CPU time + timers.
pub trait Clock: Send + Sync + 'static {
    /// Monotonic now (virtual in simulation).
    fn now(&self) -> Instant;

    /// CPU time consumed by the calling thread (`CLOCK_THREAD_CPUTIME_ID`; virtual in sim).
    fn thread_cpu_time(&self) -> Duration;

    /// Unix wall time in milliseconds (schedules, log timestamps).
    fn unix_ms(&self) -> i64;

    /// Sleep for `d` of *clock* time. Real clocks use the tokio timer; virtual clocks register
    /// a timer that fires when the simulation advances.
    fn sleep(&self, d: Duration) -> BoxFuture<'static, ()>;

    /// Consume `d` of CPU on this thread. Real clocks busy-spin until `thread_cpu_time`
    /// advanced by `d`; virtual clocks just advance their CPU counter. Used by `MockExecutor`.
    fn burn_cpu(&self, d: Duration);
}

/// A clock whose time is advanced by the simulation driver (`SimScheduler`).
pub trait SimClock: Clock {
    /// Move time forward by `d`, firing due timers (their wakers are woken).
    fn advance(&self, d: Duration);

    /// Earliest pending timer deadline, if any (so the driver can jump straight to it).
    fn next_deadline(&self) -> Option<Instant>;
}

/// Real wall/CPU time.
#[derive(Clone, Debug)]
pub struct SystemClock;

impl SystemClock {
    pub fn new() -> Self {
        SystemClock
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread CPU time via `clock_gettime(CLOCK_THREAD_CPUTIME_ID)`.
pub fn thread_cpu_time_now() -> Duration {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a valid out-pointer for clock_gettime.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return Duration::ZERO;
    }
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// One thread's CPU clock, readable from any thread. sysmon judges the CPU of a Machine's poll in
/// progress with it: never with the poll's wall time, which grows with contention (a thread
/// waiting for a core, or blocked) while the CPU used does not.
#[derive(Clone, Copy, Debug)]
pub struct ThreadCpuClock {
    #[cfg(target_os = "linux")]
    id: libc::clockid_t,
    #[cfg(target_os = "macos")]
    port: libc::mach_port_t,
}

impl ThreadCpuClock {
    /// The calling thread's clock (`None` where the platform has no per-thread CPU clock that
    /// other threads can read).
    pub fn current() -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            let mut id: libc::clockid_t = 0;
            // SAFETY: `pthread_self` is the calling thread; `id` is a valid out-pointer.
            let rc = unsafe { libc::pthread_getcpuclockid(libc::pthread_self(), &mut id) };
            (rc == 0).then_some(Self { id })
        }
        #[cfg(target_os = "macos")]
        {
            // SAFETY: the calling thread's own Mach port; no reference is taken, so none leaks.
            let port = unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) };
            (port != 0).then_some(Self { port })
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            None
        }
    }

    /// CPU time (user + system) the thread has used so far.
    pub fn read(&self) -> Option<Duration> {
        #[cfg(target_os = "linux")]
        {
            let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
            // SAFETY: `ts` is a valid out-pointer; the clock id belongs to a live Machine thread.
            let rc = unsafe { libc::clock_gettime(self.id, &mut ts) };
            (rc == 0).then(|| Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32))
        }
        #[cfg(target_os = "macos")]
        {
            // SAFETY: `thread_basic_info` is plain data; `info`/`count` are valid out-pointers of
            // the size `THREAD_BASIC_INFO_COUNT` describes.
            unsafe {
                let mut info: libc::thread_basic_info = std::mem::zeroed();
                let mut count = libc::THREAD_BASIC_INFO_COUNT;
                let kr = libc::thread_info(
                    self.port,
                    libc::THREAD_BASIC_INFO as libc::thread_flavor_t,
                    (&mut info as *mut libc::thread_basic_info).cast(),
                    &mut count,
                );
                if kr != libc::KERN_SUCCESS {
                    return None;
                }
                let t = |v: libc::time_value_t| {
                    Duration::from_secs(v.seconds.max(0) as u64) + Duration::from_micros(v.microseconds.max(0) as u64)
                };
                Some(t(info.user_time) + t(info.system_time))
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            None
        }
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn thread_cpu_time(&self) -> Duration {
        thread_cpu_time_now()
    }

    fn unix_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn sleep(&self, d: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(d))
    }

    fn burn_cpu(&self, d: Duration) {
        let start = thread_cpu_time_now();
        while thread_cpu_time_now().saturating_sub(start) < d {
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod cpu_clock_tests {
    use super::*;

    /// Read from another thread, a thread's CPU clock matches what the thread measures itself, and
    /// it stands still while the thread sleeps (what sysmon relies on for the poll in progress).
    #[test]
    fn another_threads_cpu_clock_counts_cpu_not_wall_time() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (go, wait) = std::sync::mpsc::channel::<()>();
        let h = std::thread::spawn(move || {
            let clock = ThreadCpuClock::current().expect("per-thread CPU clock");
            let start = thread_cpu_time_now();
            while thread_cpu_time_now().saturating_sub(start) < Duration::from_millis(40) {
                std::hint::spin_loop();
            }
            tx.send((clock, thread_cpu_time_now())).expect("send");
            let _ = wait.recv(); // blocked: no CPU
        });
        let (clock, own) = rx.recv().expect("clock");
        let seen = clock.read().expect("read");
        assert!(
            seen.abs_diff(own) < Duration::from_millis(5),
            "seen {seen:?}, thread says {own:?}"
        );
        assert!(seen >= Duration::from_millis(40));
        std::thread::sleep(Duration::from_millis(150));
        let later = clock.read().expect("read");
        assert!(
            later.saturating_sub(seen) < Duration::from_millis(10),
            "blocked yet charged {:?}",
            later.saturating_sub(seen)
        );
        go.send(()).expect("go");
        h.join().expect("thread");
    }
}
