//! Queue engine: leases batches from `QueueStore` under per-service
//! concurrency, runs them as tasks, acks/retries from the `Outcome`.
//!
//! - Delivery: batches of up to `queue.batch_size` (10); visibility timeout = task wall limit + 10 s.
//! - Ack: Form B `msg.ack()/retry()`; unacked after success = acked. Form A / WASM: `2xx` acks all,
//!   anything else retries all.
//! - Retry: exponential backoff `2 s × 2^(attempt-1)`, ±20 % jitter, max `queue.max_attempts`,
//!   then DLQ.
//! - Concurrency: `queue.max_concurrency` per service within the service's `max_concurrency`.
//! - Throttle: an admission refusal or a shutdown kill is not a delivery attempt
//!   ([`counts_as_attempt`]): the batch is released with its attempt refunded, visible again after
//!   [`throttle_delay`] (`1 s × 2^n`, cap 60 s, n = consecutive throttled settles of the service).

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use rt_sched::{Clock, Scheduler};
use rt_store::{Leased, QueueStore, RetryResult};
use rt_types::{KillReason, MsgId, Outcome, ServiceId, Trigger};
use tokio::sync::Notify;

/// Base of the retry backoff.
pub const RETRY_BASE: Duration = Duration::from_secs(2);

/// Backoff after `failed_attempt` (1-based) failures: `2 s × 2^(failed_attempt-1)` scaled by
/// `1 + jitter` where `jitter ∈ [-0.2, 0.2]` (TRG-09: ≈2 s, ≈4 s, …).
pub fn backoff_delay(failed_attempt: u16, jitter: f64) -> Duration {
    let exp = u32::from(failed_attempt.saturating_sub(1)).min(20);
    let base = RETRY_BASE.as_secs_f64() * f64::from(1u32 << exp);
    Duration::from_secs_f64((base * (1.0 + jitter.clamp(-0.2, 0.2))).max(0.0))
}

/// Base of the throttle backoff (a refused or shut-down batch).
pub const THROTTLE_BASE: Duration = Duration::from_secs(1);
/// Cap of the throttle backoff.
pub const THROTTLE_MAX: Duration = Duration::from_secs(60);

/// Whether a finished delivery used up one of the message's attempts. An admission refusal
/// (`Rejected`) or a shutdown kill never ran the handler to an answer, so it must not push a
/// message toward the DLQ (Q§2.4).
pub fn counts_as_attempt(outcome: &Outcome) -> bool {
    !matches!(outcome, Outcome::Rejected(_) | Outcome::Killed(KillReason::Shutdown))
}

/// Delay before a throttled batch is visible again: `1 s × 2^n`, capped at 60 s, where `n` is the
/// number of throttled settles of the service in a row before this one.
pub fn throttle_delay(n: u32) -> Duration {
    THROTTLE_BASE.saturating_mul(1u32 << n.min(16)).min(THROTTLE_MAX)
}

/// Consecutive throttled settles per service (reset by a counted settle).
type Throttles = Arc<Mutex<HashMap<ServiceId, u32>>>;

#[derive(Clone, Debug)]
pub struct QueueEngineConfig {
    /// `queue.batch_size`.
    pub batch_size: usize,
    /// `queue.max_attempts`.
    pub max_attempts: u16,
    /// `queue.max_concurrency` (per service).
    pub max_concurrency: usize,
}

impl Default for QueueEngineConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            max_attempts: 5,
            max_concurrency: 4,
        }
    }
}

/// Headroom added to the task wall limit to get the lease visibility timeout.
pub const VISIBILITY_MARGIN: Duration = Duration::from_secs(10);

/// How often the engine looks for ready messages that nobody announced (delayed messages,
/// visibility timeouts, messages enqueued by another process).
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Response header a Form B handler may set on a 2xx reply to name the messages it `retry()`d
/// (comma-separated `msg_…` ids); every other message of the batch is acked.
pub const RETRY_HEADER: &str = "x-rt-queue-retry";

/// Builds the `TaskSpec` for a leased batch. Implemented by `rt-server`.
pub trait QueueTaskFactory: Send + Sync {
    fn task_for(&self, service: ServiceId, batch: &[Leased]) -> Option<rt_types::TaskSpec>;

    /// The wall limit tasks of `service` will run with, if known before anything is leased.
    /// The lease visibility timeout is `wall + 10 s`; without a hint the engine uses
    /// the last wall it saw for the service, or the queue-trigger default for the first lease.
    fn wall_hint(&self, service: ServiceId) -> Option<Duration> {
        let _ = service;
        None
    }

    /// Whether `service` can take queue tasks right now (it is routed and its `queue` trigger is
    /// on). The engine leases nothing for a service that cannot: leasing would only burn delivery
    /// attempts and dead-letter messages of, e.g., a disabled tenant or a service without a live
    /// version. Its messages stay queued until it can run them again.
    fn routable(&self, service: ServiceId) -> bool {
        let _ = service;
        true
    }
}

type Settled = (ServiceId, Option<Duration>);
/// A leased message and the delivery attempt it is on.
type Attempt = (MsgId, u16);
type Inflight = FuturesUnordered<Pin<Box<dyn Future<Output = Settled> + Send>>>;

pub struct QueueEngine {
    cfg: QueueEngineConfig,
    queue: Arc<dyn QueueStore>,
    scheduler: Scheduler,
    clock: Arc<dyn Clock>,
    tasks: Arc<dyn QueueTaskFactory>,
    /// Services that just got a message (see [`QueueEngine::notify`]).
    hints: Mutex<BTreeSet<ServiceId>>,
    wake: Notify,
    /// Last wall limit seen per service (visibility of the next lease).
    walls: Mutex<HashMap<ServiceId, Duration>>,
    metrics: Option<Arc<rt_obs::Metrics>>,
    throttles: Throttles,
}

impl QueueEngine {
    pub fn new(
        cfg: QueueEngineConfig,
        queue: Arc<dyn QueueStore>,
        scheduler: Scheduler,
        clock: Arc<dyn Clock>,
        tasks: Arc<dyn QueueTaskFactory>,
    ) -> Self {
        Self {
            cfg,
            queue,
            scheduler,
            clock,
            tasks,
            hints: Mutex::new(BTreeSet::new()),
            wake: Notify::new(),
            walls: Mutex::new(HashMap::new()),
            metrics: None,
            throttles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Count dead-lettered messages into `rt_queue_dlq_total` and throttled batches into
    /// `rt_queue_throttled_total`.
    pub fn with_metrics(mut self, metrics: Arc<rt_obs::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Wake the engine early (an enqueue just happened).
    pub fn notify(&self, service: ServiceId) {
        self.hints.lock().insert(service);
        self.wake.notify_one();
    }

    fn visibility_for(&self, service: ServiceId) -> Duration {
        let wall = self
            .tasks
            .wall_hint(service)
            .or_else(|| self.walls.lock().get(&service).copied())
            .unwrap_or_else(|| {
                rt_types::Limits::defaults_for(&Trigger::Queue {
                    batch: Vec::new(),
                    attempt: 1,
                })
                .wall
            });
        wall + VISIBILITY_MARGIN
    }

    /// Poll for ready messages and run them until the future is dropped (shutdown / abort).
    ///
    /// Dropping the future forgets every in-flight batch without acking or retrying it: the
    /// messages reappear after their visibility timeout (at-least-once, TRG-11).
    pub async fn run(&self) {
        let mut inflight: Inflight = FuturesUnordered::new();
        let mut active: HashMap<ServiceId, usize> = HashMap::new();
        let mut wakes: BinaryHeap<Reverse<i64>> = BinaryHeap::new();
        let max_conc = self.cfg.max_concurrency.max(1);
        loop {
            let mut candidates: BTreeSet<ServiceId> = std::mem::take(&mut *self.hints.lock());
            match self.queue.services_with_ready().await {
                Ok(v) => candidates.extend(v),
                Err(e) => tracing::warn!(error = %e, "queue: listing ready services failed"),
            }
            for service in candidates {
                while active.get(&service).copied().unwrap_or(0) < max_conc {
                    if !self.dispatch(service, &mut inflight).await {
                        break;
                    }
                    *active.entry(service).or_default() += 1;
                }
            }

            let now = self.clock.unix_ms();
            while wakes.peek().is_some_and(|Reverse(t)| *t <= now) {
                wakes.pop();
            }
            let wait = match wakes.peek() {
                Some(Reverse(t)) => POLL_INTERVAL.min(Duration::from_millis((*t - now).max(1) as u64)),
                None => POLL_INTERVAL,
            };
            let sleep = self.clock.sleep(wait);
            tokio::select! {
                _ = sleep => {}
                _ = self.wake.notified() => {}
                Some((service, retry_in)) = inflight.next(), if !inflight.is_empty() => {
                    if let Some(n) = active.get_mut(&service) {
                        *n = n.saturating_sub(1);
                    }
                    if let Some(d) = retry_in {
                        wakes.push(Reverse(self.clock.unix_ms() + d.as_millis() as i64));
                    }
                }
            }
        }
    }

    /// Lease one batch of `service` and submit it as a task. `false` = nothing to deliver.
    async fn dispatch(&self, service: ServiceId, inflight: &mut Inflight) -> bool {
        if !self.tasks.routable(service) {
            return false;
        }
        let visibility = self.visibility_for(service);
        let batch = match self.queue.lease(service, self.cfg.batch_size.max(1), visibility).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(%service, error = %e, "queue: lease failed");
                return false;
            }
        };
        if batch.is_empty() {
            return false;
        }
        let msgs: Vec<Attempt> = batch.iter().map(|m| (m.id, m.attempt)).collect();
        let ctx = SettleCtx {
            queue: self.queue.clone(),
            metrics: self.metrics.clone(),
            throttles: self.throttles.clone(),
            service,
        };

        let spec = self.tasks.task_for(service, &batch);
        let fut: Pin<Box<dyn Future<Output = Settled> + Send>> = match spec {
            None => {
                // The service is gone or not routable: back off; the DLQ catches it eventually.
                Box::pin(async move { (service, settle(ctx, msgs, Outcome::GuestError).await) })
            }
            Some(spec) => {
                self.walls.lock().insert(service, spec.limits.wall);
                match self.scheduler.submit(spec) {
                    Ok(handle) => Box::pin(async move {
                        let outcome = handle.await;
                        (service, settle(ctx, msgs, outcome).await)
                    }),
                    Err(reason) => {
                        Box::pin(async move { (service, settle(ctx, msgs, Outcome::Rejected(reason)).await) })
                    }
                }
            }
        };
        inflight.push(fut);
        true
    }
}

fn jitter_sample() -> f64 {
    rand::thread_rng().gen_range(-0.2..=0.2)
}

/// Messages named by the [`RETRY_HEADER`] of a reply, with the delay of an explicit
/// `msg.retry({ delaySeconds })`. The header is a comma-separated list of `msg_…` ids; the guest
/// bootstrap appends `;delay=<seconds>` to an id that asked for a delay.
fn retry_ids(headers: &http::HeaderMap) -> HashMap<MsgId, Option<Duration>> {
    headers
        .get_all(RETRY_HEADER)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|item| {
            let (id, params) = item.split_once(';').unwrap_or((item, ""));
            let id = id.trim().parse::<MsgId>().ok()?;
            let delay = params
                .split(';')
                .filter_map(|p| p.trim().strip_prefix("delay="))
                .find_map(|d| d.trim().parse::<u64>().ok())
                // The guest writes this header: an explicit delay is capped like an enqueue
                // delay (QENG-002).
                .map(|secs| Duration::from_secs(secs).min(rt_store::queue::MAX_DELAY));
            Some((id, delay))
        })
        .collect()
}

/// What a settling batch needs from its engine.
struct SettleCtx {
    queue: Arc<dyn QueueStore>,
    metrics: Option<Arc<rt_obs::Metrics>>,
    throttles: Throttles,
    service: ServiceId,
}

/// Ack / retry / release a finished batch from its `Outcome`. Returns the shortest retry delay
/// (so the engine can wake exactly when a message becomes visible again).
async fn settle(ctx: SettleCtx, msgs: Vec<Attempt>, outcome: Outcome) -> Option<Duration> {
    let SettleCtx {
        queue,
        metrics,
        throttles,
        service,
    } = ctx;
    if !counts_as_attempt(&outcome) {
        let delay = {
            let mut t = throttles.lock();
            let n = t.entry(service).or_insert(0);
            let d = throttle_delay(*n);
            *n = n.saturating_add(1);
            d
        };
        if let Some(m) = &metrics {
            m.queue_throttled_total.inc();
        }
        if let Err(e) = queue.release_leased(&msgs, delay).await {
            tracing::warn!(error = %e, "queue: release failed (messages will be redelivered)");
        }
        return Some(delay);
    }
    throttles.lock().remove(&service);
    // Explicit `retry({ delaySeconds })` delays; every other retry follows the backoff curve.
    let mut explicit: HashMap<MsgId, Duration> = HashMap::new();
    let (ack, retry): (Vec<Attempt>, Vec<Attempt>) = match &outcome {
        // 2xx: everything is acked, except what a Form B handler explicitly retried.
        Outcome::Ok(reply) if reply.status.is_success() => {
            let named = retry_ids(&reply.headers);
            explicit.extend(named.iter().filter_map(|(id, d)| d.map(|d| (*id, d))));
            msgs.into_iter().partition(|(id, _)| !named.contains_key(id))
        }
        // Anything else (non-2xx, guest error, kill, rejection, build failure): retry all.
        _ => (Vec::new(), msgs),
    };
    if !ack.is_empty() {
        // Fenced by the leased attempt: if this batch outlived its visibility timeout and was
        // re-delivered, the late ack must not delete the newer delivery.
        match queue.ack_leased(&ack).await {
            Ok(n) if (n as usize) < ack.len() => {
                tracing::warn!(
                    stale = ack.len() - n as usize,
                    "queue: ack after lease expiry ignored (message was re-delivered)"
                );
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "queue: ack failed (messages will be redelivered)"),
        }
    }
    let mut shortest: Option<Duration> = None;
    // One jitter sample per settled batch: its messages come back together (same attempt =
    // same delay), while different batches still spread out.
    let jitter = jitter_sample();
    for (id, attempt) in retry {
        let delay = explicit
            .get(&id)
            .copied()
            .unwrap_or_else(|| backoff_delay(attempt, jitter));
        match queue.retry_leased(id, attempt, delay).await {
            Ok(RetryResult::Requeued { .. }) => {
                shortest = Some(shortest.map_or(delay, |s| s.min(delay)));
            }
            Ok(RetryResult::DeadLettered) => {
                tracing::warn!(msg = %id, attempt, "queue: message dead-lettered");
                if let Some(m) = &metrics {
                    m.queue_dlq_total.inc();
                }
            }
            Err(e) => tracing::warn!(msg = %id, error = %e, "queue: retry failed"),
        }
    }
    shortest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(value: &str) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        h.insert(RETRY_HEADER, http::HeaderValue::from_str(value).expect("header value"));
        h
    }

    /// The guest bootstrap writes `msg_…` for `retry()` and `msg_…;delay=N` for
    /// `retry({ delaySeconds: N })`, comma-separated (js/bootstrap.js). A `;delay=` suffix must not
    /// make the id unparseable: the message would then be treated as acked and lost.
    #[test]
    fn retry_header_names_ids_with_an_optional_delay() {
        let (a, b, c) = (MsgId::from_u128(1), MsgId::from_u128(2), MsgId::from_u128(3));
        let h = headers(&format!("{a},{b};delay=7, {c};delay=oops"));
        let named = retry_ids(&h);
        assert_eq!(named.len(), 3, "{named:?}");
        assert_eq!(named[&a], None, "plain retry(): the backoff curve applies");
        assert_eq!(named[&b], Some(Duration::from_secs(7)), "retry({{ delaySeconds: 7 }})");
        assert_eq!(named[&c], None, "an unreadable delay falls back to the backoff curve");
    }

    /// QENG-002: the guest writes this header, so an explicit delay is capped at the queue's
    /// maximum delay (7 days) instead of overflowing the store's timestamps.
    #[test]
    fn qeng_002_an_explicit_retry_delay_is_capped_at_the_queues_maximum_delay() {
        let (a, b) = (MsgId::from_u128(1), MsgId::from_u128(2));
        let named = retry_ids(&headers(&format!("{a};delay=18446744073709551615,{b};delay=604800")));
        assert_eq!(named[&a], Some(rt_store::queue::MAX_DELAY));
        assert_eq!(named[&b], Some(Duration::from_secs(604_800)), "exactly 7 days is kept");
    }

    #[test]
    fn retry_header_ignores_garbage_ids() {
        let named = retry_ids(&headers("not-an-id;delay=1,"));
        assert!(named.is_empty(), "{named:?}");
    }
}
