//! Shared helpers for the rt-triggers integration tests (wave 2, lane T-F).
//!
//! - `FakeQueueStore`: an in-memory `QueueStore` driven by the `VirtualClock` (visibility,
//!   delay, dedup, attempts, DLQ) so queue-engine tests run in virtual time.
//! - factories for the three `*TaskFactory` traits that record what the engines asked for and
//!   script the mock executor per delivery attempt.
//! - `drive`: advance the simulated scheduler + virtual clock and let spawned engine tasks run.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rt_sched::Clock;
use rt_sec::PlanSource;
use rt_store::{DlqEntry, Leased, NewMessage, QueueStats, QueueStore, RetryResult, StoreError};
use rt_testkit::{ids, Script, ScriptBook, SimConfig, SimRuntime, SpecBuilder, VirtualClock};
use rt_types::{
    Access, Digest, IngressRules, MsgId, Payload, Plan, QueueMessage, RouteEntry, RouteSnapshot, RuntimeKind, Schedule,
    ScheduleId, ScheduleKind, ScheduledEvent, ServiceId, TaskSpec, TenantId, Trigger, TriggerSwitches, UnixMs,
    VersionNo,
};

// ---------------------------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------------------------

/// A routable service entry (deno, public, version `version`).
pub fn entry(tenant: u128, service: u128, slug: &str, version: u32) -> RouteEntry {
    RouteEntry {
        tenant: ids::tenant(tenant),
        service: ids::service(service),
        slug: slug.to_string(),
        runtime: RuntimeKind::Deno,
        version: VersionNo(version),
        digest: Digest::of(format!("code-{service}-{version}").as_bytes()),
        access: Access::Public,
        triggers: TriggerSwitches::default(),
        ingress: IngressRules::default(),
        profile: None,
        hosts: Vec::new(),
        // One project per tenant: the services of a tenant share their environment labels.
        project: rt_types::ProjectId::from_u128(tenant),
        environment: rt_types::DEFAULT_ENVIRONMENT.to_string(),
        call: rt_types::CallAccess::default(),
    }
}

/// [`entry`] in environment `env` admitting `allowed_callers`.
pub fn entry_in(tenant: u128, service: u128, slug: &str, env: &str, allowed_callers: &[&str]) -> RouteEntry {
    let mut e = entry(tenant, service, slug, 1);
    e.environment = env.to_string();
    e.call.allowed_callers = allowed_callers.iter().map(|s| s.to_string()).collect();
    e
}

pub fn snapshot(generation: u64, entries: Vec<RouteEntry>) -> RouteSnapshot {
    RouteSnapshot {
        generation,
        entries,
        ..RouteSnapshot::default()
    }
}

// ---------------------------------------------------------------------------------------------
// Plans
// ---------------------------------------------------------------------------------------------

/// Every tenant gets `default`, unless overridden in `by_tenant`.
pub struct FixedPlans {
    pub default: Plan,
    pub by_tenant: HashMap<TenantId, Plan>,
}

impl FixedPlans {
    pub fn new(default: Plan) -> Arc<Self> {
        Arc::new(Self {
            default,
            by_tenant: HashMap::new(),
        })
    }

    pub fn with_tenant(default: Plan, tenant: TenantId, plan: Plan) -> Arc<Self> {
        let mut by_tenant = HashMap::new();
        by_tenant.insert(tenant, plan);
        Arc::new(Self { default, by_tenant })
    }
}

impl PlanSource for FixedPlans {
    fn plan_for(&self, tenant: TenantId) -> Plan {
        self.by_tenant
            .get(&tenant)
            .cloned()
            .unwrap_or_else(|| self.default.clone())
    }
}

// ---------------------------------------------------------------------------------------------
// FakeQueueStore
// ---------------------------------------------------------------------------------------------

/// One recorded `lease` call.
#[derive(Clone, Debug)]
pub struct LeaseRecord {
    pub service: ServiceId,
    pub at_ms: i64,
    pub visibility: Duration,
    pub ids: Vec<MsgId>,
    pub attempts: Vec<u16>,
}

/// One recorded `retry` call.
#[derive(Clone, Debug)]
pub struct RetryRecord {
    pub id: MsgId,
    pub at_ms: i64,
    pub delay: Duration,
    pub result: RetryResult,
}

struct Msg {
    id: MsgId,
    service: ServiceId,
    body: Bytes,
    /// Attempts delivered so far (0 until the first lease).
    attempt: u16,
    visible_at_ms: i64,
    in_flight_until_ms: i64,
    enqueued_at_ms: i64,
    dedup: Option<String>,
}

#[derive(Default)]
struct StoreInner {
    msgs: Vec<Msg>,
    dlq: Vec<(ServiceId, DlqEntry)>,
    leases: Vec<LeaseRecord>,
    retries: Vec<RetryRecord>,
    acked: Vec<MsgId>,
    enqueued: Vec<(ServiceId, NewMessage)>,
}

/// In-memory `QueueStore` on a virtual clock. Semantics follow `rt_store::queue`:
/// `attempt` counts deliveries (1 on the first), `retry` dead-letters once `attempt >=
/// max_attempts`, redrive re-queues with `attempt = 1` on the next delivery, dedup window 24 h.
pub struct FakeQueueStore {
    clock: Arc<VirtualClock>,
    max_attempts: u16,
    inner: Mutex<StoreInner>,
}

impl FakeQueueStore {
    pub fn new(clock: Arc<VirtualClock>, max_attempts: u16) -> Arc<Self> {
        Arc::new(Self {
            clock,
            max_attempts,
            inner: Mutex::new(StoreInner::default()),
        })
    }

    fn now(&self) -> i64 {
        self.clock.unix_ms()
    }

    pub fn lease_log(&self) -> Vec<LeaseRecord> {
        self.inner.lock().leases.clone()
    }

    pub fn retry_log(&self) -> Vec<RetryRecord> {
        self.inner.lock().retries.clone()
    }

    pub fn acked(&self) -> Vec<MsgId> {
        self.inner.lock().acked.clone()
    }

    pub fn enqueued(&self) -> Vec<(ServiceId, NewMessage)> {
        self.inner.lock().enqueued.clone()
    }

    /// Messages still stored (waiting, delayed or in flight), not counting the DLQ.
    pub fn stored(&self) -> usize {
        self.inner.lock().msgs.len()
    }

    pub fn dlq_len(&self, s: ServiceId) -> usize {
        self.inner.lock().dlq.iter().filter(|(svc, _)| *svc == s).count()
    }
}

#[async_trait]
impl QueueStore for FakeQueueStore {
    async fn enqueue(&self, s: ServiceId, m: NewMessage) -> Result<MsgId, StoreError> {
        let now = self.now();
        let mut g = self.inner.lock();
        g.enqueued.push((s, m.clone()));
        if let Some(d) = &m.dedup_id {
            let window_ms = rt_store::queue::DEDUP_WINDOW.as_millis() as i64;
            if let Some(existing) = g.msgs.iter().find(|x| {
                x.service == s && x.dedup.as_deref() == Some(d.as_str()) && now - x.enqueued_at_ms < window_ms
            }) {
                return Ok(existing.id);
            }
        }
        let id = MsgId::new();
        g.msgs.push(Msg {
            id,
            service: s,
            body: m.body,
            attempt: 0,
            visible_at_ms: now + m.delay.as_millis() as i64,
            in_flight_until_ms: 0,
            enqueued_at_ms: now,
            dedup: m.dedup_id,
        });
        Ok(id)
    }

    async fn lease(&self, s: ServiceId, max: usize, visibility: Duration) -> Result<Vec<Leased>, StoreError> {
        let now = self.now();
        let mut g = self.inner.lock();
        let mut out = Vec::new();
        for m in g.msgs.iter_mut() {
            if out.len() >= max {
                break;
            }
            if m.service == s && m.visible_at_ms <= now && m.in_flight_until_ms <= now {
                m.attempt += 1;
                m.in_flight_until_ms = now + visibility.as_millis() as i64;
                m.visible_at_ms = m.in_flight_until_ms;
                out.push(Leased {
                    id: m.id,
                    body: m.body.clone(),
                    attempt: m.attempt,
                    enqueued_at_ms: m.enqueued_at_ms,
                });
            }
        }
        if !out.is_empty() {
            let rec = LeaseRecord {
                service: s,
                at_ms: now,
                visibility,
                ids: out.iter().map(|l| l.id).collect(),
                attempts: out.iter().map(|l| l.attempt).collect(),
            };
            g.leases.push(rec);
        }
        Ok(out)
    }

    async fn ack(&self, ids: &[MsgId]) -> Result<(), StoreError> {
        let mut g = self.inner.lock();
        g.msgs.retain(|m| !ids.contains(&m.id));
        g.acked.extend_from_slice(ids);
        Ok(())
    }

    async fn retry(&self, id: MsgId, delay: Duration) -> Result<RetryResult, StoreError> {
        let now = self.now();
        let mut g = self.inner.lock();
        let Some(pos) = g.msgs.iter().position(|m| m.id == id) else {
            return Err(StoreError::NotFound);
        };
        let result = if g.msgs[pos].attempt >= self.max_attempts {
            let m = g.msgs.remove(pos);
            g.dlq.push((
                m.service,
                DlqEntry {
                    id: m.id,
                    body: m.body,
                    attempts: m.attempt,
                    failed_at_ms: now,
                },
            ));
            RetryResult::DeadLettered
        } else {
            let m = &mut g.msgs[pos];
            m.in_flight_until_ms = 0;
            m.visible_at_ms = now + delay.as_millis() as i64;
            RetryResult::Requeued { attempt: m.attempt }
        };
        g.retries.push(RetryRecord {
            id,
            at_ms: now,
            delay,
            result,
        });
        Ok(result)
    }

    async fn stats(&self, s: ServiceId) -> Result<QueueStats, StoreError> {
        let now = self.now();
        let g = self.inner.lock();
        let mine: Vec<&Msg> = g.msgs.iter().filter(|m| m.service == s).collect();
        let depth = mine.iter().filter(|m| m.in_flight_until_ms <= now).count() as u64;
        let in_flight = mine.iter().filter(|m| m.in_flight_until_ms > now).count() as u64;
        let oldest = mine
            .iter()
            .filter(|m| m.in_flight_until_ms <= now)
            .map(|m| now - m.enqueued_at_ms)
            .max()
            .unwrap_or(0);
        let dlq = g.dlq.iter().filter(|(svc, _)| *svc == s).count() as u64;
        Ok(QueueStats {
            depth,
            in_flight,
            oldest_age_ms: oldest.max(0) as u64,
            dlq,
        })
    }

    async fn dlq_list(&self, s: ServiceId, limit: usize) -> Result<Vec<DlqEntry>, StoreError> {
        let g = self.inner.lock();
        Ok(g.dlq
            .iter()
            .filter(|(svc, _)| *svc == s)
            .take(limit)
            .map(|(_, e)| e.clone())
            .collect())
    }

    async fn dlq_redrive(&self, s: ServiceId) -> Result<u64, StoreError> {
        let now = self.now();
        let mut g = self.inner.lock();
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut g.dlq).into_iter().partition(|(svc, _)| *svc == s);
        g.dlq = rest;
        let n = mine.len() as u64;
        for (svc, e) in mine {
            g.msgs.push(Msg {
                id: e.id,
                service: svc,
                body: e.body,
                attempt: 0,
                visible_at_ms: now,
                in_flight_until_ms: 0,
                enqueued_at_ms: now,
                dedup: None,
            });
        }
        Ok(n)
    }

    async fn dlq_purge(&self, s: ServiceId) -> Result<u64, StoreError> {
        let mut g = self.inner.lock();
        let before = g.dlq.len();
        g.dlq.retain(|(svc, _)| *svc != s);
        Ok((before - g.dlq.len()) as u64)
    }

    async fn services_with_ready(&self) -> Result<Vec<ServiceId>, StoreError> {
        let now = self.now();
        let g = self.inner.lock();
        let mut v: Vec<ServiceId> = Vec::new();
        for m in g
            .msgs
            .iter()
            .filter(|m| m.visible_at_ms <= now && m.in_flight_until_ms <= now)
        {
            if !v.contains(&m.service) {
                v.push(m.service);
            }
        }
        Ok(v)
    }
}

// ---------------------------------------------------------------------------------------------
// Task factories
// ---------------------------------------------------------------------------------------------

/// One schedule fire as seen by the factory.
#[derive(Clone, Debug)]
pub struct Fire {
    pub id: ScheduleId,
    pub scheduled_ms: UnixMs,
    /// Virtual unix ms when the engine asked for the task.
    pub at_ms: UnixMs,
}

/// `ScheduleTaskFactory` that records fires and builds a deno task for service `service_n`.
pub struct RecordingScheduleFactory {
    clock: Arc<VirtualClock>,
    service_n: u128,
    pub fires: Mutex<Vec<Fire>>,
}

impl RecordingScheduleFactory {
    pub fn new(clock: Arc<VirtualClock>, service_n: u128) -> Arc<Self> {
        Arc::new(Self {
            clock,
            service_n,
            fires: Mutex::new(Vec::new()),
        })
    }

    pub fn count(&self) -> usize {
        self.fires.lock().len()
    }

    pub fn scheduled_times(&self) -> Vec<UnixMs> {
        self.fires.lock().iter().map(|f| f.scheduled_ms).collect()
    }
}

impl rt_triggers::ScheduleTaskFactory for RecordingScheduleFactory {
    fn task_for(&self, schedule: &Schedule, scheduled_time_ms: UnixMs) -> Option<TaskSpec> {
        self.fires.lock().push(Fire {
            id: schedule.id,
            scheduled_ms: scheduled_time_ms,
            at_ms: self.clock.unix_ms(),
        });
        let trigger = match schedule.kind {
            ScheduleKind::Interval { .. } => Trigger::Interval { id: schedule.id },
            ScheduleKind::Cron { .. } => Trigger::Cron { id: schedule.id },
        };
        Some(
            SpecBuilder::deno()
                .service(self.service_n)
                .trigger(trigger)
                .wall_limit(60_000)
                .payload(Payload::Schedule(ScheduledEvent {
                    schedule_id: schedule.id,
                    scheduled_time_ms,
                    payload: schedule.payload.clone(),
                }))
                .finish(),
        )
    }
}

/// One queue delivery as seen by the factory.
#[derive(Clone, Debug)]
pub struct Delivery {
    pub ids: Vec<MsgId>,
    /// Attempt of the first message of the batch.
    pub attempt: u16,
    pub batch_len: usize,
    pub at_ms: UnixMs,
}

/// `QueueTaskFactory` that scripts the mock handler per delivery attempt:
/// the handler runs for `run_ms` of virtual time, then fails (`panic`) unless the attempt is
/// `>= succeed_on` (`None` = always fails).
pub struct AttemptFactory {
    clock: Arc<VirtualClock>,
    book: Arc<ScriptBook>,
    service_n: u128,
    pub succeed_on: Mutex<Option<u16>>,
    pub run_ms: Mutex<u64>,
    pub wall_ms: u64,
    pub deliveries: Mutex<Vec<Delivery>>,
}

impl AttemptFactory {
    pub fn new(sim: &SimRuntime, service_n: u128, succeed_on: Option<u16>, wall_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            clock: sim.clock().clone(),
            book: sim.scripts().clone(),
            service_n,
            succeed_on: Mutex::new(succeed_on),
            run_ms: Mutex::new(0),
            wall_ms,
            deliveries: Mutex::new(Vec::new()),
        })
    }

    pub fn attempts(&self) -> Vec<u16> {
        self.deliveries.lock().iter().map(|d| d.attempt).collect()
    }

    pub fn times_ms(&self) -> Vec<UnixMs> {
        self.deliveries.lock().iter().map(|d| d.at_ms).collect()
    }
}

impl rt_triggers::QueueTaskFactory for AttemptFactory {
    /// The lease visibility is `task wall + 10 s`, known before the first lease.
    fn wall_hint(&self, _service: ServiceId) -> Option<Duration> {
        Some(Duration::from_millis(self.wall_ms))
    }

    fn task_for(&self, service: ServiceId, batch: &[Leased]) -> Option<TaskSpec> {
        let attempt = batch.first().map(|m| m.attempt).unwrap_or(1);
        self.deliveries.lock().push(Delivery {
            ids: batch.iter().map(|m| m.id).collect(),
            attempt,
            batch_len: batch.len(),
            at_ms: self.clock.unix_ms(),
        });
        let mut script = Script::new().io_ms(*self.run_ms.lock());
        let ok = matches!(*self.succeed_on.lock(), Some(n) if attempt >= n);
        if !ok {
            script = script.then(rt_testkit::panic());
        }
        self.book.set_for_service(service, script);
        let ids_vec: Vec<MsgId> = batch.iter().map(|m| m.id).collect();
        let msgs: Vec<QueueMessage> = batch
            .iter()
            .map(|m| QueueMessage {
                id: m.id,
                body: m.body.clone(),
                attempt: m.attempt,
            })
            .collect();
        let service_n: u128 = service.ulid().into();
        Some(
            SpecBuilder::deno()
                .service(service_n)
                .trigger(Trigger::Queue {
                    batch: ids_vec,
                    attempt,
                })
                .wall_limit(self.wall_ms)
                .payload(Payload::Queue(msgs))
                .finish(),
        )
    }
}

// ---------------------------------------------------------------------------------------------
// Simulation driving
// ---------------------------------------------------------------------------------------------

pub fn new_sim() -> SimRuntime {
    SimRuntime::new(SimConfig::default().seed(7))
}

pub fn dyn_clock(sim: &SimRuntime) -> Arc<dyn Clock> {
    sim.clock().clone() as Arc<dyn Clock>
}

/// Let spawned (local) tasks run until they are all pending.
pub async fn settle() {
    for _ in 0..24 {
        tokio::task::yield_now().await;
    }
}

/// Advance `total` of virtual time in `step` quanta, running the simulated scheduler and then
/// letting the engine tasks (spawned on the current `LocalSet`) react.
pub async fn drive(sim: &mut SimRuntime, total: Duration, step: Duration) {
    let mut elapsed = Duration::ZERO;
    settle().await;
    while elapsed < total {
        sim.run_for(step);
        settle().await;
        elapsed += step;
    }
}

/// Maximum number of tasks of the mock executor running at the same time, from its event log.
pub fn max_parallel(book: &ScriptBook) -> usize {
    use rt_testkit::MockEvent;
    let mut cur: i64 = 0;
    let mut max: i64 = 0;
    for ev in book.events() {
        match ev {
            MockEvent::Started { .. } => {
                cur += 1;
                max = max.max(cur);
            }
            MockEvent::Finished { .. } | MockEvent::Killed { .. } => cur -= 1,
            MockEvent::Called { .. } => {}
        }
    }
    max as usize
}
