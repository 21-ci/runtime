//! Async invocation engine: runs requests accepted with `X-Rt-Invocation-Type: Event` /
//! `Prefer: respond-async` (stored in `queue.db` by the invoke path) as HTTP tasks.
//!
//! - Lease: per service up to `max_concurrency` deliveries in flight, and at most
//!   `inflight_node` ([`ASYNC_INFLIGHT_NODE`]) across the node. Lease visibility comes from
//!   [`AsyncTaskFactory::visibility`] (queue timeout + build timeout + wall + 10 s).
//! - Outcome ([`classify`]): `status >= 500` or `429`, a guest error, a kill (cpu / oom / wall) or
//!   a build failure is a failed attempt, retried after [`async_backoff`] until the record's
//!   `max_attempts`; any other status is a success. An admission refusal or a shutdown kill is not
//!   an attempt: the lease is released with its attempt refunded after a throttle backoff.
//! - Not routable (`Ok(None)` from the factory) is a throttle too: the record waits until the
//!   service can run again or its `max_event_age` runs out (the sweep fails it).
//! - Quota ([`AsyncEngine::with_quota`]): every delivery, retries and redrives included, takes
//!   one tenant `invocations` token before it runs; over quota is a throttle (no attempt spent).
//! - A payload the factory cannot unseal fails without running the handler and without backoff.
//! - The response is dropped as soon as the outcome arrives (never buffered).
//! - Every settle is fenced by the leased attempt: a late settle of an expired lease is ignored.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use rt_sched::Clock;
use rt_store::queue::{InvocationState, InvocationStore, LeasedInvocation};
use rt_store::StoreError;
use rt_types::events::ASYNC_INFLIGHT_NODE;
use rt_types::{KillReason, Outcome, ServiceId, TaskSpec, TenantId};
use tokio::sync::Notify;

use crate::queue_engine::counts_as_attempt;
use crate::Submitter;

/// Error of a payload the factory could not unseal (fails at once, no retry).
pub const SEALED_UNREADABLE: &str = "sealed_unreadable";

/// Tenant quota check of one delivery (`QuotaOp::Invoke`): `false` = over quota (throttled).
pub type AsyncQuota = Arc<dyn Fn(TenantId) -> bool + Send + Sync>;

/// Builds the HTTP task of a leased invocation. Implemented by `rt-server`.
pub trait AsyncTaskFactory: Send + Sync {
    /// `Ok(None)`: the service cannot take it now (no live version, plan forbids `http`,
    /// `triggers.http` off): throttled, no attempt used. `Err(code)`: the stored request cannot
    /// be used (e.g. [`SEALED_UNREADABLE`]): the record fails without running.
    fn task_for(&self, inv: &LeasedInvocation) -> Result<Option<TaskSpec>, &'static str>;

    /// Lease visibility for `service`: queue timeout + build timeout + wall limit + 10 s.
    fn visibility(&self, service: ServiceId) -> Duration;

    /// Deliveries of `service` in flight at once (`queue.max_concurrency`); `None` = the engine
    /// default.
    fn max_concurrency(&self, service: ServiceId) -> Option<usize> {
        let _ = service;
        None
    }
}

#[derive(Clone, Debug)]
pub struct AsyncEngineConfig {
    /// Default per-service concurrency (`queue.max_concurrency`).
    pub max_concurrency: usize,
    /// Deliveries in flight across the node.
    pub inflight_node: usize,
    /// How often expired records are swept.
    pub sweep_every: Duration,
    /// How often the store is polled for due records.
    pub poll: Duration,
    /// Base / cap of the failed-attempt backoff.
    pub retry_base: Duration,
    pub retry_max: Duration,
    /// Base / cap of the throttle backoff.
    pub throttle_base: Duration,
    pub throttle_max: Duration,
}

impl Default for AsyncEngineConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            inflight_node: ASYNC_INFLIGHT_NODE,
            sweep_every: Duration::from_secs(30),
            poll: Duration::from_millis(250),
            retry_base: RETRY_BASE,
            retry_max: RETRY_MAX,
            throttle_base: crate::queue_engine::THROTTLE_BASE,
            throttle_max: crate::queue_engine::THROTTLE_MAX,
        }
    }
}

/// Base of the failed-attempt backoff.
pub const RETRY_BASE: Duration = Duration::from_secs(30);
/// Cap of the failed-attempt backoff.
pub const RETRY_MAX: Duration = Duration::from_secs(15 * 60);

fn exp_backoff(base: Duration, cap: Duration, n: u32, jitter: f64) -> Duration {
    let d = base.as_secs_f64() * f64::from(1u32 << n.min(20));
    let d = d.min(cap.as_secs_f64()) * (1.0 + jitter.clamp(-0.2, 0.2));
    Duration::from_secs_f64(d.max(0.0))
}

/// Retry delay after failed delivery `attempt` (1-based): `30 s × 2^(attempt-1)`, capped at
/// 15 min, scaled by `1 + jitter` (`jitter ∈ [-0.2, 0.2]`).
pub fn async_backoff(attempt: u16, jitter: f64) -> Duration {
    exp_backoff(RETRY_BASE, RETRY_MAX, u32::from(attempt.saturating_sub(1)), jitter)
}

/// How a finished delivery settles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Settle {
    /// The handler answered: `succeeded` with this status.
    Done(u16),
    /// A failed attempt (retried until `max_attempts`).
    Failed { error: &'static str, status: Option<u16> },
    /// Never ran to an answer: not an attempt.
    Throttled(&'static str),
}

/// Classify an outcome (the reply, if any, is only looked at, the caller drops it).
pub fn classify(o: &Outcome) -> Settle {
    if !counts_as_attempt(o) {
        return Settle::Throttled(match o {
            Outcome::Rejected(_) => "rejected",
            _ => "shutdown",
        });
    }
    match o {
        Outcome::Ok(r) => {
            let s = r.status.as_u16();
            if s == 429 {
                Settle::Failed {
                    error: "status_429",
                    status: Some(s),
                }
            } else if s >= 500 {
                Settle::Failed {
                    error: "status_5xx",
                    status: Some(s),
                }
            } else {
                Settle::Done(s)
            }
        }
        Outcome::GuestError => Settle::Failed {
            error: "guest_error",
            status: None,
        },
        Outcome::Killed(k) => Settle::Failed {
            error: match k {
                KillReason::CpuLimit => "cpu_limit",
                KillReason::Oom => "oom",
                _ => "timeout",
            },
            status: None,
        },
        _ => Settle::Failed {
            error: "build_failed",
            status: None,
        },
    }
}

/// Consecutive throttled settles per service.
type Throttles = Arc<Mutex<HashMap<ServiceId, u32>>>;
type Inflight = FuturesUnordered<Pin<Box<dyn Future<Output = ServiceId> + Send>>>;

#[derive(Clone)]
struct Shared {
    store: Arc<dyn InvocationStore>,
    metrics: Option<Arc<rt_obs::Metrics>>,
    throttles: Throttles,
    cfg: AsyncEngineConfig,
}

pub struct AsyncEngine {
    shared: Shared,
    tasks: Arc<dyn AsyncTaskFactory>,
    submit: Submitter,
    clock: Arc<dyn Clock>,
    quota: Option<AsyncQuota>,
    hints: Mutex<BTreeSet<ServiceId>>,
    wake: Notify,
    draining: std::sync::atomic::AtomicBool,
}

impl AsyncEngine {
    pub fn new(
        cfg: AsyncEngineConfig,
        store: Arc<dyn InvocationStore>,
        tasks: Arc<dyn AsyncTaskFactory>,
        submit: Submitter,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            shared: Shared {
                store,
                metrics: None,
                throttles: Arc::new(Mutex::new(HashMap::new())),
                cfg,
            },
            tasks,
            submit,
            clock,
            quota: None,
            hints: Mutex::new(BTreeSet::new()),
            wake: Notify::new(),
            draining: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Count `rt_async_throttled_total`, `rt_async_finished_total{condition}` and
    /// `rt_async_failed_evicted_total`.
    pub fn with_metrics(mut self, m: Arc<rt_obs::Metrics>) -> Self {
        self.shared.metrics = Some(m);
        self
    }

    /// Take a tenant quota token per delivery (each attempt, retries and redrives included).
    pub fn with_quota(mut self, q: AsyncQuota) -> Self {
        self.quota = Some(q);
        self
    }

    /// Wake the engine early (an invocation of `service` was just accepted).
    pub fn notify(&self, service: ServiceId) {
        self.hints.lock().insert(service);
        self.wake.notify_one();
    }

    /// Stop leasing (in-flight deliveries still settle).
    pub fn drain(&self) {
        self.draining.store(true, std::sync::atomic::Ordering::SeqCst);
        self.wake.notify_one();
    }

    fn draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Run until the future is dropped. Dropping forgets in-flight deliveries: their records come
    /// back after the lease visibility (at-least-once).
    pub async fn run(&self) {
        let mut inflight: Inflight = FuturesUnordered::new();
        let mut active: HashMap<ServiceId, usize> = HashMap::new();
        let mut last_sweep: Option<i64> = None;
        loop {
            let now = self.clock.unix_ms();
            if last_sweep.is_none_or(|t| now - t >= self.shared.cfg.sweep_every.as_millis() as i64) {
                last_sweep = Some(now);
                self.sweep(now).await;
            }
            if !self.draining() {
                let mut candidates: BTreeSet<ServiceId> = std::mem::take(&mut *self.hints.lock());
                match self.shared.store.services_with_invocations().await {
                    Ok(v) => candidates.extend(v),
                    Err(e) => tracing::warn!(error = %e, "async: listing due services failed"),
                }
                for service in candidates {
                    self.dispatch(service, &mut inflight, &mut active).await;
                }
            }
            let sleep = self.clock.sleep(self.shared.cfg.poll);
            tokio::select! {
                _ = sleep => {}
                _ = self.wake.notified() => {}
                Some(service) = inflight.next(), if !inflight.is_empty() => {
                    if let Some(n) = active.get_mut(&service) {
                        *n = n.saturating_sub(1);
                        if *n == 0 {
                            active.remove(&service);
                        }
                    }
                }
            }
        }
    }

    async fn sweep(&self, now: i64) {
        match self.shared.store.expire(now).await {
            Ok(stats) => {
                if let Some(m) = &self.shared.metrics {
                    m.async_finished_total
                        .with_label_values(&["event_age_exceeded"])
                        .inc_by(stats.aged_out);
                }
            }
            Err(e) => tracing::warn!(error = %e, "async: sweep failed"),
        }
        self.shared.count_evicted();
    }

    async fn dispatch(&self, service: ServiceId, inflight: &mut Inflight, active: &mut HashMap<ServiceId, usize>) {
        let cfg = &self.shared.cfg;
        let per_service = self
            .tasks
            .max_concurrency(service)
            .unwrap_or(cfg.max_concurrency)
            .max(1);
        let running = active.get(&service).copied().unwrap_or(0);
        let room = per_service
            .saturating_sub(running)
            .min(cfg.inflight_node.max(1).saturating_sub(inflight.len()));
        if room == 0 {
            return;
        }
        let visibility = self.tasks.visibility(service);
        let leased = match self.shared.store.lease_invocations(service, room, visibility).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(%service, error = %e, "async: lease failed");
                return;
            }
        };
        for inv in leased {
            let shared = self.shared.clone();
            let fut: Pin<Box<dyn Future<Output = ServiceId> + Send>> = match self.tasks.task_for(&inv) {
                Err(code) => Box::pin(async move {
                    shared.fail_unreadable(inv, code).await;
                    service
                }),
                Ok(None) => Box::pin(async move {
                    shared.settle(&inv, Settle::Throttled("not_routable")).await;
                    service
                }),
                Ok(Some(spec)) if self.quota.as_ref().is_some_and(|q| !q(spec.service.tenant)) => {
                    Box::pin(async move {
                        shared.settle(&inv, Settle::Throttled("quota")).await;
                        service
                    })
                }
                Ok(Some(spec)) => match (self.submit)(spec) {
                    Ok(handle) => Box::pin(async move {
                        let settle = {
                            let outcome = handle.await;
                            classify(&outcome)
                            // `outcome` (and any reply body) is dropped here.
                        };
                        shared.settle(&inv, settle).await;
                        service
                    }),
                    Err(_) => Box::pin(async move {
                        shared.settle(&inv, Settle::Throttled("rejected")).await;
                        service
                    }),
                },
            };
            inflight.push(fut);
            *active.entry(service).or_default() += 1;
        }
    }
}

impl Shared {
    fn count_evicted(&self) {
        let n = self.store.take_evicted();
        if let Some(m) = &self.metrics {
            m.async_failed_evicted_total.inc_by(n);
        }
    }

    fn finished(&self, condition: &str) {
        if let Some(m) = &self.metrics {
            m.async_finished_total.with_label_values(&[condition]).inc();
        }
    }

    async fn settle(&self, inv: &LeasedInvocation, settle: Settle) {
        let service = inv.service;
        match settle {
            Settle::Throttled(why) => {
                let delay = {
                    let mut t = self.throttles.lock();
                    let n = t.entry(service).or_insert(0);
                    let d = exp_backoff(self.cfg.throttle_base, self.cfg.throttle_max, *n, 0.0);
                    *n = n.saturating_add(1);
                    d
                };
                if let Some(m) = &self.metrics {
                    m.async_throttled_total.inc();
                }
                tracing::debug!(%service, reason = why, "async: delivery throttled");
                if let Err(e) = self.store.release(inv.id, inv.attempt, delay).await {
                    tracing::warn!(error = %e, "async: release failed (redelivered after visibility)");
                }
            }
            Settle::Done(status) => {
                self.throttles.lock().remove(&service);
                match self.store.complete(inv.id, inv.attempt, status).await {
                    Ok(true) => self.finished("success"),
                    Ok(false) => tracing::warn!(id = %inv.id, "async: late complete ignored"),
                    Err(e) => tracing::warn!(error = %e, "async: complete failed"),
                }
            }
            Settle::Failed { error, status } => {
                self.throttles.lock().remove(&service);
                let jitter = rand::thread_rng().gen_range(-0.2..=0.2);
                let delay = exp_backoff(
                    self.cfg.retry_base,
                    self.cfg.retry_max,
                    u32::from(inv.attempt.saturating_sub(1)),
                    jitter,
                );
                self.fail(inv, error, status, delay).await;
            }
        }
    }

    async fn fail(&self, inv: &LeasedInvocation, error: &str, status: Option<u16>, delay: Duration) -> bool {
        match self.store.fail_attempt(inv.id, inv.attempt, error, status, delay).await {
            Ok(InvocationState::Failed) => {
                self.finished("retries_exhausted");
                self.count_evicted();
                true
            }
            Ok(_) => false,
            Err(StoreError::NotFound) => {
                tracing::warn!(id = %inv.id, "async: late failure ignored");
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "async: fail failed");
                true
            }
        }
    }

    /// The stored request cannot be used: fail it at once, without running the handler and
    /// without spending the remaining attempts.
    async fn fail_unreadable(&self, inv: LeasedInvocation, code: &'static str) {
        match self.store.fail_now(inv.id, inv.attempt, code).await {
            Ok(()) => {
                self.finished("unreadable");
                self.count_evicted();
            }
            Err(StoreError::NotFound) => tracing::warn!(id = %inv.id, "async: late failure ignored"),
            Err(e) => tracing::warn!(error = %e, "async: fail failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_types::{RejectReason, Reply};

    fn status(s: u16) -> Outcome {
        Outcome::Ok(Reply::new(
            http::StatusCode::from_u16(s).expect("status"),
            rt_types::Body::empty(),
        ))
    }

    #[test]
    fn classify_2xx_3xx_4xx_are_done() {
        for s in [200, 204, 302, 400, 404, 499] {
            assert_eq!(classify(&status(s)), Settle::Done(s));
        }
    }

    #[test]
    fn classify_5xx_and_429_are_failed_attempts() {
        assert_eq!(
            classify(&status(503)),
            Settle::Failed {
                error: "status_5xx",
                status: Some(503)
            }
        );
        assert_eq!(
            classify(&status(429)),
            Settle::Failed {
                error: "status_429",
                status: Some(429)
            }
        );
    }

    #[test]
    fn classify_errors_and_kills() {
        let e = |o: Outcome| match classify(&o) {
            Settle::Failed { error, .. } => error,
            other => panic!("{other:?}"),
        };
        assert_eq!(e(Outcome::GuestError), "guest_error");
        assert_eq!(e(Outcome::Killed(KillReason::CpuLimit)), "cpu_limit");
        assert_eq!(e(Outcome::Killed(KillReason::Oom)), "oom");
        assert_eq!(e(Outcome::Killed(KillReason::WallTimeout)), "timeout");
    }

    #[test]
    fn classify_rejection_and_shutdown_are_throttles() {
        assert_eq!(
            classify(&Outcome::Rejected(RejectReason::Draining)),
            Settle::Throttled("rejected")
        );
        assert_eq!(
            classify(&Outcome::Killed(KillReason::Shutdown)),
            Settle::Throttled("shutdown")
        );
    }

    #[test]
    fn async_backoff_doubles_and_caps() {
        assert_eq!(async_backoff(1, 0.0), Duration::from_secs(30));
        assert_eq!(async_backoff(2, 0.0), Duration::from_secs(60));
        assert_eq!(async_backoff(3, 0.0), Duration::from_secs(120));
        assert_eq!(async_backoff(10, 0.0), RETRY_MAX);
        assert_eq!(async_backoff(1, 0.2), Duration::from_secs(36));
        assert_eq!(async_backoff(1, -5.0), Duration::from_secs(24), "jitter is clamped");
    }
}
