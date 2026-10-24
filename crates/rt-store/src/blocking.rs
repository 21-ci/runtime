//! Blocking-work seam and CPU charging (SECURITY.md).
//!
//! SQL and blob file work runs on blocking threads. The thread CPU time of every job is added to
//! the *calling task's* [`CpuMeter`] (installed with [`with_cpu_meter`]), so heavy SQL counts
//! against the guest's CPU limit (SEC-29). `rt-host` provides a per-tenant fair
//! implementation of [`BlockingRunner`]; the default here is tokio's blocking pool.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rt_types::TenantId;

/// Accumulates host-side CPU time on behalf of one task. Cheap to clone.
#[derive(Clone, Debug, Default)]
pub struct CpuMeter(Arc<AtomicU64>);

impl CpuMeter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self, d: Duration) {
        self.0.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Total charged so far.
    pub fn total(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::Relaxed))
    }
}

tokio::task_local! {
    static CURRENT_METER: CpuMeter;
}

/// Run `fut` with `meter` as the current task's CPU meter.
pub async fn with_cpu_meter<F: Future>(meter: CpuMeter, fut: F) -> F::Output {
    CURRENT_METER.scope(meter, fut).await
}

/// Charge `d` to the current task's meter (no-op outside [`with_cpu_meter`]).
pub fn charge_cpu(d: Duration) {
    let _ = CURRENT_METER.try_with(|m| m.add(d));
}

/// Runs blocking jobs. Implementations must call [`charge_cpu`] with the job's thread CPU time
/// after it completes.
pub trait BlockingRunner: Send + Sync + 'static {
    /// `tenant` selects the fair-queue lane (`None` for node-internal work).
    fn run(
        &self,
        tenant: Option<TenantId>,
        job: Box<dyn FnOnce() + Send + 'static>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
}

/// Default runner: `tokio::task::spawn_blocking`, measuring thread CPU time.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioBlocking;

fn thread_cpu() -> Duration {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a valid out-pointer.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } != 0 {
        return Duration::ZERO; // LCOV_EXCL_LINE: CLOCK_THREAD_CPUTIME_ID is always available on the supported platforms; no injection point
    }
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

impl BlockingRunner for TokioBlocking {
    fn run(
        &self,
        _tenant: Option<TenantId>,
        job: Box<dyn FnOnce() + Send + 'static>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(async move {
            let cpu = tokio::task::spawn_blocking(move || {
                let start = thread_cpu();
                job();
                thread_cpu().saturating_sub(start)
            })
            .await
            .unwrap_or_default();
            charge_cpu(cpu);
        })
    }
}
