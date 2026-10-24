//! Per-tenant fair blocking pool (SECURITY.md).
//!
//! Per-tenant FIFO queues served round-robin; under contention one tenant occupies at most
//! `plan.blocking_share` of the threads, so a tenant running SQL cross joins cannot starve another
//! tenant's KV reads (SEC-29). The pool is work-conserving: when every lane with queued jobs is at
//! its share and threads are idle, the lane with the fewest running jobs may borrow one, as long
//! as one thread stays free for a tenant that arrives meanwhile (a lone tenant uses all threads
//! but one, instead of its share). Job CPU time is charged to the calling task via
//! `rt_store::charge_cpu`.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use rt_store::{charge_cpu, BlockingRunner};
use rt_types::TenantId;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
pub struct FairPoolConfig {
    /// `blocking.threads`.
    pub threads: usize,
    /// Share used for tenants without an explicit setting.
    pub default_share: f64,
}

impl Default for FairPoolConfig {
    fn default() -> Self {
        Self {
            threads: 4,
            default_share: 0.25,
        }
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;
/// `None` is the node-internal lane (no per-tenant cap).
type Lane = Option<TenantId>;

#[derive(Default)]
struct State {
    queues: HashMap<Lane, VecDeque<Job>>,
    /// Lanes that currently have queued jobs, in round-robin order.
    rotation: VecDeque<Lane>,
    running: HashMap<Lane, usize>,
    shares: HashMap<TenantId, f64>,
    queued: usize,
    shutdown: bool,
}

struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    threads: usize,
    default_share: f64,
}

impl Shared {
    /// Threads a lane may occupy at once.
    fn cap(&self, st: &State, lane: Lane) -> usize {
        match lane {
            None => self.threads,
            Some(t) => {
                let share = st.shares.get(&t).copied().unwrap_or(self.default_share).clamp(0.0, 1.0);
                ((share * self.threads as f64).floor() as usize).clamp(1, self.threads)
            }
        }
    }

    /// Next job: round robin over lanes below their thread cap; else (work-conserving) the lane
    /// with the fewest running jobs borrows an idle thread, keeping one thread free.
    fn next_job(&self, st: &mut State) -> Option<(Lane, Job)> {
        for _ in 0..st.rotation.len() {
            let lane = st.rotation.pop_front()?;
            let running = st.running.get(&lane).copied().unwrap_or(0);
            if running >= self.cap(st, lane) {
                st.rotation.push_back(lane);
                continue;
            }
            if let Some(job) = Self::take(st, lane) {
                return Some((lane, job));
            }
        }
        let busy: usize = st.running.values().sum();
        if busy + 1 >= self.threads {
            return None;
        }
        let lane = st
            .rotation
            .iter()
            .copied()
            .min_by_key(|l| st.running.get(l).copied().unwrap_or(0))?;
        st.rotation.retain(|l| *l != lane);
        Self::take(st, lane).map(|job| (lane, job))
    }

    /// Pop `lane`'s next job (re-listing the lane at the back if more wait) and count it running.
    fn take(st: &mut State, lane: Lane) -> Option<Job> {
        let q = st.queues.get_mut(&lane)?;
        let job = q.pop_front();
        if q.is_empty() {
            st.queues.remove(&lane);
        } else {
            st.rotation.push_back(lane);
        }
        let job = job?;
        st.queued -= 1;
        *st.running.entry(lane).or_insert(0) += 1;
        Some(job)
    }

    fn worker(self: Arc<Self>) {
        let mut st = self.state.lock();
        loop {
            if let Some((lane, job)) = self.next_job(&mut st) {
                drop(st);
                let _ = catch_unwind(AssertUnwindSafe(job));
                st = self.state.lock();
                if let Some(r) = st.running.get_mut(&lane) {
                    *r = r.saturating_sub(1);
                    if *r == 0 {
                        st.running.remove(&lane);
                    }
                }
                // A freed slot may unblock a capped lane.
                self.wake.notify_all();
                continue;
            }
            if st.shutdown && st.queued == 0 {
                return;
            }
            self.wake.wait(&mut st);
        }
    }
}

pub struct FairBlockingPool {
    shared: Arc<Shared>,
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl FairBlockingPool {
    /// Spawn the worker threads.
    #[allow(
        clippy::expect_used,
        reason = "at node startup: a node that cannot spawn its pool cannot run"
    )]
    pub fn new(cfg: FairPoolConfig) -> Arc<Self> {
        let threads = cfg.threads.max(1);
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            threads,
            default_share: cfg.default_share,
        });
        let handles = (0..threads)
            .map(|i| {
                let s = shared.clone();
                std::thread::Builder::new()
                    .name(format!("rt-blocking-{i}"))
                    .spawn(move || s.worker())
                    .expect("spawn blocking pool worker")
            })
            .collect();
        Arc::new(Self {
            shared,
            handles: Mutex::new(handles),
        })
    }

    /// Update a tenant's `plan.blocking_share` (SIGHUP reload).
    pub fn set_tenant_share(&self, tenant: TenantId, share: f64) {
        self.shared.state.lock().shares.insert(tenant, share);
        self.shared.wake.notify_all();
    }

    /// Jobs queued (diagnostics).
    pub fn queued(&self) -> usize {
        self.shared.state.lock().queued
    }

    /// Stop the workers after draining queued jobs.
    pub fn shutdown(&self) {
        self.shared.state.lock().shutdown = true;
        self.shared.wake.notify_all();
        let me = std::thread::current().id();
        let handles: Vec<_> = self.handles.lock().drain(..).collect();
        for h in handles {
            if h.thread().id() != me {
                let _ = h.join();
            }
        }
    }

    fn enqueue(&self, lane: Lane, job: Job) -> bool {
        let mut st = self.shared.state.lock();
        if st.shutdown {
            return false;
        }
        let q = st.queues.entry(lane).or_default();
        let was_empty = q.is_empty();
        q.push_back(job);
        if was_empty {
            st.rotation.push_back(lane);
        }
        st.queued += 1;
        drop(st);
        self.shared.wake.notify_one();
        true
    }
}

impl Drop for FairBlockingPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn thread_cpu() -> Duration {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a valid out-pointer for the duration of the call.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } != 0 {
        return Duration::ZERO; // LCOV_EXCL_LINE: the thread CPU clock is always available on the supported platforms
    }
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

impl BlockingRunner for FairBlockingPool {
    fn run(
        &self,
        tenant: Option<TenantId>,
        job: Box<dyn FnOnce() + Send + 'static>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let (tx, rx) = oneshot::channel::<Duration>();
        let wrapped: Job = Box::new(move || {
            let start = thread_cpu();
            let outcome = catch_unwind(AssertUnwindSafe(job));
            let _ = tx.send(thread_cpu().saturating_sub(start));
            if let Err(p) = outcome {
                std::panic::resume_unwind(p);
            }
        });
        let accepted = self.enqueue(tenant, wrapped);
        Box::pin(async move {
            if !accepted {
                return;
            }
            // The job's CPU time lands on the calling task's meter once it is back on the task.
            if let Ok(cpu) = rx.await {
                charge_cpu(cpu);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use super::*;

    fn tenant(n: u128) -> Option<TenantId> {
        Some(TenantId::from_u128(n))
    }

    /// Review 12: a lone tenant is not held to its share while threads are idle: with 4 threads
    /// and a 0.25 share it runs 3 jobs at once (one thread stays free), not 1.
    #[test]
    fn a_lone_tenant_borrows_idle_threads_beyond_its_share() {
        let pool = FairBlockingPool::new(FairPoolConfig {
            threads: 4,
            default_share: 0.25,
        });
        let (now, peak, done) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        for _ in 0..12 {
            let (now, peak, done) = (now.clone(), peak.clone(), done.clone());
            assert!(pool.enqueue(
                tenant(1),
                Box::new(move || {
                    let n = now.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(n, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(30));
                    now.fetch_sub(1, Ordering::SeqCst);
                    done.fetch_add(1, Ordering::SeqCst);
                })
            ));
        }
        let until = Instant::now() + Duration::from_secs(5);
        while done.load(Ordering::SeqCst) < 12 && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(done.load(Ordering::SeqCst), 12);
        assert_eq!(peak.load(Ordering::SeqCst), 3, "all threads but one");
        pool.shutdown();
    }

    /// Under contention the free thread goes to the newcomer at once: a tenant flooding the pool
    /// with long jobs does not delay another tenant's short job (SEC-29).
    #[test]
    fn a_borrowing_tenant_leaves_a_thread_for_a_newcomer() {
        let pool = FairBlockingPool::new(FairPoolConfig {
            threads: 4,
            default_share: 0.25,
        });
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        for _ in 0..8 {
            let release = release.clone();
            assert!(pool.enqueue(
                tenant(1),
                Box::new(move || {
                    let (lock, cv) = &*release;
                    let mut open = lock.lock();
                    while !*open {
                        cv.wait(&mut open);
                    }
                })
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
        let started = Arc::new(Mutex::new(None::<Instant>));
        let s = started.clone();
        let asked = Instant::now();
        assert!(pool.enqueue(tenant(2), Box::new(move || *s.lock() = Some(Instant::now()))));
        let until = Instant::now() + Duration::from_secs(2);
        while started.lock().is_none() && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(1));
        }
        let waited = started.lock().map(|t| t.duration_since(asked));
        assert!(
            waited.is_some_and(|w| w < Duration::from_millis(500)),
            "the newcomer ran at once: {waited:?}"
        );
        let (lock, cv) = &*release;
        *lock.lock() = true;
        cv.notify_all();
        pool.shutdown();
    }

    #[test]
    fn the_default_pool_has_four_threads_and_a_quarter_share() {
        let d = FairPoolConfig::default();
        assert_eq!(d.threads, 4);
        assert_eq!(d.default_share, 0.25);
    }

    /// The node-internal lane is not capped by a tenant share: it may use every thread.
    #[test]
    fn the_node_lane_uses_every_thread() {
        let pool = FairBlockingPool::new(FairPoolConfig {
            threads: 4,
            default_share: 0.25,
        });
        let (now, peak, done) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        for _ in 0..12 {
            let (now, peak, done) = (now.clone(), peak.clone(), done.clone());
            assert!(pool.enqueue(
                None,
                Box::new(move || {
                    let n = now.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(n, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(30));
                    now.fetch_sub(1, Ordering::SeqCst);
                    done.fetch_add(1, Ordering::SeqCst);
                })
            ));
        }
        let until = Instant::now() + Duration::from_secs(5);
        while done.load(Ordering::SeqCst) < 12 && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(done.load(Ordering::SeqCst), 12);
        assert_eq!(peak.load(Ordering::SeqCst), 4, "the node lane is not held to a share");
        pool.shutdown();
    }

    /// `queued` counts the jobs that have not started: the one a thread runs is not queued.
    #[test]
    fn queued_counts_only_the_jobs_still_waiting_for_a_thread() {
        let pool = FairBlockingPool::new(FairPoolConfig {
            threads: 1,
            default_share: 1.0,
        });
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        assert!(pool.enqueue(
            tenant(1),
            Box::new(move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
            })
        ));
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the blocker is running");
        assert_eq!(pool.queued(), 0);
        for _ in 0..3 {
            assert!(pool.enqueue(tenant(1), Box::new(|| {})));
        }
        assert_eq!(pool.queued(), 3, "three jobs wait behind the blocker");
        release_tx.send(()).expect("release the blocker");
        pool.shutdown();
        assert_eq!(pool.queued(), 0, "shutdown drains the queue");
    }

    #[tokio::test]
    async fn a_job_submitted_after_shutdown_never_runs_and_its_caller_is_not_held() {
        let pool = FairBlockingPool::new(FairPoolConfig {
            threads: 2,
            default_share: 0.5,
        });
        pool.shutdown();
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        pool.run(
            tenant(1),
            Box::new(move || {
                r.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .await;
        assert_eq!(ran.load(Ordering::SeqCst), 0, "a stopped pool takes no work");
    }

    /// A job that panics is contained: its caller still returns, and the pool keeps running jobs.
    #[tokio::test]
    async fn a_panicking_job_returns_to_its_caller_and_the_pool_keeps_working() {
        let pool = FairBlockingPool::new(FairPoolConfig {
            threads: 2,
            default_share: 0.5,
        });
        pool.run(None, Box::new(|| panic!("job failed"))).await;
        let done = Arc::new(AtomicUsize::new(0));
        let d = done.clone();
        pool.run(
            None,
            Box::new(move || {
                d.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .await;
        assert_eq!(done.load(Ordering::SeqCst), 1, "a job after the panic still runs");
        pool.shutdown();
    }
}
