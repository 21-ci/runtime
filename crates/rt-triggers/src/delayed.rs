//! The delayed engine of `std:schedule`: one-off schedules that guest code
//! created, delivered at their time as tasks of their service (`Trigger::Scheduled`, the guest's
//! `scheduled()` handler).
//!
//! The engine leases the due schedules from [`DelayedStore`] and submits one task per schedule. The
//! task's outcome acks the schedule, or retries it with the queue's backoff (dropped after
//! [`DELAYED_MAX_ATTEMPTS`]). The store keeps every pending schedule across a restart, and a lease
//! that runs out delivers the schedule again, so delivery is at-least-once: the handler gets
//! `event.id` to make its work idempotent.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use rt_sched::{Clock, Scheduler};
use rt_store::{DelayedItem, DelayedStore, RetryOutcome};
use rt_types::{Outcome, ServiceId, TaskSpec};

use crate::queue_engine::backoff_delay;

/// How long a delivery holds its schedule: longer than any task's wall limit. A delivery that
/// outlives it (or a node that stopped mid-delivery) is delivered again.
pub const DELAYED_LEASE: Duration = Duration::from_secs(600);

/// Engine tuning.
#[derive(Clone, Debug)]
pub struct DelayedEngineConfig {
    /// Deliveries in flight at once.
    pub max_concurrency: usize,
    /// How often the store is polled for due schedules (a new schedule waits at most this long).
    pub poll: Duration,
}

impl Default for DelayedEngineConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 16,
            poll: Duration::from_millis(250),
        }
    }
}

/// Builds the delivery task of a due schedule. Implemented by `rt-server`.
pub trait DelayedTaskFactory: Send + Sync {
    /// The task that delivers `item` to `service`, or `None` when the service cannot take it now
    /// (the schedule stays pending, without an attempt).
    fn task_for(&self, service: ServiceId, item: &DelayedItem) -> Option<TaskSpec>;
}

type Inflight = FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>>;

/// Delivers due one-off schedules from the store, as tasks.
pub struct DelayedEngine {
    cfg: DelayedEngineConfig,
    store: Arc<dyn DelayedStore>,
    scheduler: Scheduler,
    clock: Arc<dyn Clock>,
    tasks: Arc<dyn DelayedTaskFactory>,
}

impl DelayedEngine {
    pub fn new(
        cfg: DelayedEngineConfig,
        store: Arc<dyn DelayedStore>,
        scheduler: Scheduler,
        clock: Arc<dyn Clock>,
        tasks: Arc<dyn DelayedTaskFactory>,
    ) -> Self {
        Self {
            cfg,
            store,
            scheduler,
            clock,
            tasks,
        }
    }

    /// Deliver due schedules until the future is dropped (shutdown). Dropping it forgets the
    /// deliveries in flight without acking them: they are delivered again when their lease ends.
    pub async fn run(&self) {
        let mut inflight: Inflight = FuturesUnordered::new();
        let max = self.cfg.max_concurrency.max(1);
        loop {
            while inflight.len() < max && self.dispatch(&mut inflight).await {}
            tokio::select! {
                Some(()) = inflight.next(), if !inflight.is_empty() => {}
                _ = self.clock.sleep(self.cfg.poll) => {}
            }
        }
    }

    /// Lease one due schedule and start its delivery. `false` = nothing to deliver now.
    async fn dispatch(&self, inflight: &mut Inflight) -> bool {
        let now = self.clock.unix_ms();
        let leased = match self.store.lease(now, DELAYED_LEASE, 1).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "schedule: lease failed");
                return false;
            }
        };
        let Some((service, item)) = leased.into_iter().next() else {
            return false;
        };
        let store = self.store.clone();
        let clock = self.clock.clone();
        let fut: Pin<Box<dyn Future<Output = ()> + Send>> = match self.tasks.task_for(service, &item) {
            // The service cannot take it now: not an attempt, due again after the poll interval.
            None => {
                let run_at = now + self.cfg.poll.as_millis() as i64;
                Box::pin(async move {
                    if let Err(e) = store.release(service, &item.id, item.gen, run_at).await {
                        tracing::warn!(%service, error = %e, "schedule: release failed");
                    }
                })
            }
            Some(spec) => match self.scheduler.submit(spec) {
                Ok(handle) => Box::pin(async move {
                    let outcome = handle.await;
                    settle(store, clock, service, item, outcome).await;
                }),
                Err(reason) => Box::pin(async move {
                    settle(store, clock, service, item, Outcome::Rejected(reason)).await;
                }),
            },
        };
        inflight.push(fut);
        true
    }
}

/// Ack a delivery that replied 2xx; retry any other outcome with the queue's backoff.
async fn settle(
    store: Arc<dyn DelayedStore>,
    clock: Arc<dyn Clock>,
    service: ServiceId,
    item: DelayedItem,
    outcome: Outcome,
) {
    if matches!(&outcome, Outcome::Ok(reply) if reply.status.is_success()) {
        if let Err(e) = store.done(service, &item.id, item.gen).await {
            tracing::warn!(%service, schedule = %item.id, error = %e, "schedule: ack failed (delivered again later)");
        }
        return;
    }
    let jitter = rand::thread_rng().gen_range(-0.2..=0.2);
    let delay = backoff_delay(u16::try_from(item.attempt).unwrap_or(u16::MAX), jitter);
    let run_at = clock.unix_ms() + delay.as_millis() as i64;
    match store.retry(service, &item.id, item.gen, run_at).await {
        Ok(RetryOutcome::Dropped) => {
            tracing::warn!(%service, schedule = %item.id, attempts = item.attempt, "schedule: dropped after its last attempt");
        }
        Ok(RetryOutcome::Requeued | RetryOutcome::Gone) => {}
        Err(e) => tracing::warn!(%service, schedule = %item.id, error = %e, "schedule: retry failed"),
    }
}
