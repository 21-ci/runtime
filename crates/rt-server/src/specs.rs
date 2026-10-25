//! Config resolution for triggers: builds the `TaskSpec` of HTTP, schedule, queue and call tasks
//! from the route table, the service table and the effective config (profile + plan + service).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::{DashMap, DashSet};
use rt_config::Config;
use rt_host::{CallError, Request as CallRequest, TaskCtx};
use rt_sec::{KeyLookup, PlanSource, Sealed, SecretsBackend, WallClock};
use rt_store::queue::LeasedInvocation;
use rt_store::{DelayedItem, Leased};
use rt_triggers::async_engine::{AsyncTaskFactory, SEALED_UNREADABLE};
use rt_triggers::pubsub::{PubsubTaskFactory, SubRow};
use rt_triggers::stream_engine::{ConsumerDecl, StreamTaskFactory};
use rt_triggers::{
    CallTaskFactory, DelayedTaskFactory, QueueTaskFactory, RouteTable, ScheduleTaskFactory, SocketLimits,
    SocketTaskFactory,
};
use rt_types::events::invocation_aad;
use rt_types::{
    ApiKey, DelayedEvent, HttpRequest, KeyId, Namespace, ObjectRef, Payload, Plan, PubsubMessage, QueueMessage,
    RouteEntry, Schedule, ScheduleKind, ScheduledEvent, Service, ServiceId, ServiceRef, SocketEvent, SocketEventKind,
    StreamBatch, TaskKind, TaskSpec, Tenant, TenantId, TraceCtx, Trigger, TriggerKind, UnixMs,
};

/// Resolves a tenant's plan: config plan by name plus the tenant's overrides.
///
/// A request reads the plan several times (quota check, task spec, host policy), so resolved
/// plans are cached per tenant. An entry is used only while the config generation (`SIGHUP`
/// swaps the config `Arc`) and the tenant's plan name and overrides are still the ones it was
/// resolved from, so tenant create/patch/delete and plan reloads apply without an explicit
/// invalidation.
pub struct TenantPlans {
    pub config: Arc<ArcSwap<Config>>,
    pub tenants: Arc<DashMap<TenantId, Tenant>>,
    cache: DashMap<TenantId, CachedPlan>,
}

/// What a cached plan was resolved from, and the result.
struct CachedPlan {
    config: Arc<Config>,
    /// The tenant's plan name and overrides; `None` = the tenant was unknown (`free`).
    source: Option<(String, serde_json::Value)>,
    plan: Arc<Plan>,
}

impl CachedPlan {
    fn matches(&self, config: &Arc<Config>, tenant: Option<&Tenant>) -> bool {
        Arc::ptr_eq(&self.config, config)
            && match (&self.source, tenant) {
                (None, None) => true,
                (Some((plan, overrides)), Some(t)) => *plan == t.plan && *overrides == t.overrides,
                _ => false,
            }
    }
}

impl TenantPlans {
    pub fn new(config: Arc<ArcSwap<Config>>, tenants: Arc<DashMap<TenantId, Tenant>>) -> Self {
        Self {
            config,
            tenants,
            cache: DashMap::new(),
        }
    }

    fn resolve(cfg: &Config, tenant: Option<&Tenant>) -> Plan {
        let Some(t) = tenant else {
            return cfg.plan("free");
        };
        if rt_types::is_system_tenant(t.id) {
            return cfg
                .plans
                .get(rt_types::SYSTEM_PLAN)
                .cloned()
                .unwrap_or_else(Plan::system);
        }
        let base = cfg.plan(&t.plan);
        match &t.overrides {
            serde_json::Value::Object(o) if !o.is_empty() => {
                let Ok(serde_json::Value::Object(mut merged)) = serde_json::to_value(&base) else {
                    return base;
                };
                merged.extend(o.iter().map(|(k, v)| (k.clone(), v.clone())));
                serde_json::from_value(serde_json::Value::Object(merged)).unwrap_or(base)
            }
            _ => base,
        }
    }
}

impl PlanSource for TenantPlans {
    fn plan_for(&self, tenant: TenantId) -> Plan {
        (*self.plan_arc(tenant)).clone()
    }

    fn plan_arc(&self, tenant: TenantId) -> Arc<Plan> {
        let cfg = self.config.load();
        let t = self.tenants.get(&tenant);
        if let Some(hit) = self.cache.get(&tenant).filter(|c| c.matches(&cfg, t.as_deref())) {
            return hit.plan.clone();
        }
        let plan = Arc::new(Self::resolve(&cfg, t.as_deref()));
        let source = t.as_deref().map(|t| (t.plan.clone(), t.overrides.clone()));
        let config = arc_swap::Guard::into_inner(cfg);
        self.cache.insert(
            tenant,
            CachedPlan {
                config,
                source,
                plan: plan.clone(),
            },
        );
        plan
    }

    fn plan_name(&self, tenant: TenantId) -> String {
        match self.tenants.get(&tenant) {
            Some(t) => t.plan.clone(),
            None => "free".to_string(),
        }
    }
}

/// The operator SDKs a build can import, from the route table's SDK snapshot: every SDK that is
/// not removed and has a release, with the members of its `stable` (else newest) contract.
pub struct RouteSdkCatalog {
    pub routes: Arc<RouteTable>,
}

impl rt_build::SdkCatalog for RouteSdkCatalog {
    fn sdks(&self) -> std::collections::BTreeMap<String, Vec<String>> {
        let snap = self.routes.sdk();
        snap.sdks
            .iter()
            .filter(|s| s.state == rt_types::SdkState::Active)
            .filter_map(|s| {
                let release = snap
                    .channel_release(&s.name, rt_types::DEFAULT_CHANNEL)
                    .or_else(|| snap.releases.iter().rfind(|r| r.sdk == s.name))?;
                Some((s.name.clone(), release.members.clone()))
            })
            .collect()
    }
}

/// API keys held in memory: lookups on the auth path never touch `meta.db`; creation and
/// revocation update the map before the control call answers (SEC-06).
///
/// With a tenant table attached, keys of a disabled tenant are not found (they authenticate
/// again once the tenant is re-enabled).
///
/// Revocation is permanent, so revoked ids are remembered: an update that was read before the
/// revocation but delivered after it (a child minted while its parent's tree was being revoked,
/// KEY-035) cannot bring the key back.
#[derive(Default)]
pub struct KeyCache {
    map: DashMap<KeyId, ApiKey>,
    revoked: DashSet<KeyId>,
    tenants: Option<Arc<DashMap<TenantId, Tenant>>>,
}

impl KeyCache {
    /// A cache that rejects the keys of disabled tenants in `tenants`.
    pub fn with_tenants(tenants: Arc<DashMap<TenantId, Tenant>>) -> Self {
        Self {
            map: DashMap::new(),
            revoked: DashSet::new(),
            tenants: Some(tenants),
        }
    }

    fn tenant_disabled(&self, tenant: TenantId) -> bool {
        self.tenants
            .as_ref()
            .is_some_and(|t| t.get(&tenant).is_none_or(|t| t.disabled))
    }

    pub fn upsert(&self, key: ApiKey) {
        if key.revoked || self.revoked.contains(&key.id) {
            self.revoke(key.id);
            return;
        }
        self.map.insert(key.id, key);
    }

    pub fn revoke(&self, id: KeyId) {
        self.revoked.insert(id);
        self.map.remove(&id);
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl KeyLookup for KeyCache {
    fn find(&self, id: KeyId) -> Option<ApiKey> {
        self.map
            .get(&id)
            .map(|k| k.clone())
            .filter(|k| !k.revoked && !self.tenant_disabled(k.tenant_id))
    }
}

pub struct SystemWall;

impl WallClock for SystemWall {
    fn now_ms(&self) -> UnixMs {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// Builds task specs. Shared by ingress, the schedule engine, the queue engine and call dispatch.
pub struct Specs {
    pub config: Arc<ArcSwap<Config>>,
    pub plans: Arc<dyn PlanSource>,
    pub routes: Arc<RouteTable>,
    pub services: Arc<ArcSwap<HashMap<ServiceId, Arc<Service>>>>,
    /// Services whose `min_instances` floor is not kept: their tenant's floors are above the
    /// plan's `max_warm_floor_total` (a plan downgrade). Set by `NodeImpl::sync_floors`; their
    /// tasks carry no floor, so a request does not bring it back.
    pub unfloored: ArcSwap<HashSet<ServiceId>>,
}

impl Specs {
    /// The live route and the service record of `service`, if it is routable.
    pub fn lookup(&self, service: ServiceId) -> Option<(Arc<RouteEntry>, Arc<Service>)> {
        let svc = self.services.load().get(&service)?.clone();
        let entry = self.routes.by_slug(&svc.slug)?;
        (entry.service == service).then_some((entry, svc))
    }

    pub fn spec(
        &self,
        entry: &RouteEntry,
        svc: &Service,
        trigger: Trigger,
        payload: Payload,
        trace: Option<TraceCtx>,
    ) -> TaskSpec {
        let plan = self.plans.plan_arc(entry.tenant);
        let cfg = self.config.load();
        let eff = cfg.service_effective(svc.profile, &svc.config, &plan);
        let sref = ServiceRef {
            tenant: entry.tenant,
            service: entry.service,
            version: entry.version,
            digest: entry.digest,
        };
        let mut knobs = eff.knobs(&plan);
        if knobs.min_instances > 0 && self.unfloored.load().contains(&entry.service) {
            knobs.min_instances = 0;
        }
        let mut spec = TaskSpec::new(TaskKind::Exec(entry.runtime), sref, trigger.clone())
            .with_limits(eff.limits_for(&trigger))
            .with_knobs(knobs)
            .with_payload(payload)
            .with_slug(svc.slug.as_str());
        spec.trace = trace;
        spec
    }

    /// The `min_instances` floor the service's config and plan give it (before the tenant-wide
    /// `max_warm_floor_total`, see [`Specs::unfloored`]).
    pub fn floor_of(&self, entry: &RouteEntry, svc: &Service) -> u32 {
        let plan = self.plans.plan_arc(entry.tenant);
        self.config
            .load()
            .service_effective(svc.profile, &svc.config, &plan)
            .knobs(&plan)
            .min_instances
    }

    /// Effective wall limit of a service's queue tasks (queue visibility timeout).
    pub fn wall_of(&self, service: ServiceId) -> Option<Duration> {
        let (entry, svc) = self.lookup(service)?;
        let plan = self.plans.plan_arc(entry.tenant);
        let eff = self.config.load().service_effective(svc.profile, &svc.config, &plan);
        Some(
            eff.limits_for(&Trigger::Queue {
                batch: Vec::new(),
                attempt: 1,
            })
            .wall,
        )
    }

    pub fn http_task(&self, entry: &RouteEntry, req: HttpRequest, trace: Option<TraceCtx>) -> Option<TaskSpec> {
        let svc = self.services.load().get(&entry.service)?.clone();
        let mut spec = self.spec(entry, &svc, Trigger::Http, Payload::Http(req), trace);
        Self::durable_defaults(&svc, &mut spec);
        Some(spec)
    }

    /// A request that reached a durable service directly acts for its default object (ingress
    /// replaces the name with the object's turn); its isolate is kept like an event's.
    fn durable_defaults(svc: &Service, spec: &mut TaskSpec) {
        if svc.config.durable {
            spec.object = Some(ObjectRef::named(rt_types::DEFAULT_OBJECT));
            if svc.config.ws.warm() {
                spec.knobs.reuse_isolates = true;
                spec.knobs.reuse_instances = true;
            }
        }
    }
}

impl ScheduleTaskFactory for Specs {
    fn task_for(&self, schedule: &Schedule, scheduled_time_ms: UnixMs) -> Option<TaskSpec> {
        let (entry, svc) = self.lookup(schedule.service_id)?;
        let (trigger, enabled) = match schedule.kind {
            ScheduleKind::Interval { .. } => (Trigger::Interval { id: schedule.id }, entry.triggers.interval),
            ScheduleKind::Cron { .. } => (Trigger::Cron { id: schedule.id }, entry.triggers.cron),
        };
        if !enabled {
            return None;
        }
        let payload = Payload::Schedule(ScheduledEvent {
            schedule_id: schedule.id,
            scheduled_time_ms,
            payload: schedule.payload.clone(),
        });
        Some(self.spec(&entry, &svc, trigger, payload, None))
    }
}

impl DelayedTaskFactory for Specs {
    /// One-off schedules ride the service's interval switch (`triggers.interval`, timed triggers).
    fn task_for(&self, service: ServiceId, item: &DelayedItem) -> Option<TaskSpec> {
        let (entry, svc) = self.lookup(service)?;
        if !entry.triggers.interval {
            return None;
        }
        let payload = serde_json::from_slice(&item.payload).unwrap_or(serde_json::Value::Null);
        let event = DelayedEvent {
            id: item.id.clone(),
            scheduled_at_ms: item.run_at_ms,
            payload,
        };
        Some(self.spec(
            &entry,
            &svc,
            Trigger::Scheduled { id: item.id.clone() },
            Payload::Delayed(event),
            None,
        ))
    }
}

/// The socket hub's tasks: one task per socket event or alarm of an object, with the
/// service's HTTP limits, warm (a kept isolate or instance) unless `ws.warm = false`.
pub struct SocketTasks {
    pub specs: Arc<Specs>,
    pub logs: Arc<dyn rt_obs::LogSink>,
    /// Durable storage: a delivered alarm is removed from its object.
    pub data: Arc<dyn rt_store::ServiceDataStore>,
}

impl SocketTaskFactory for SocketTasks {
    fn event_task(&self, service: ServiceId, object: ObjectRef, event: SocketEvent) -> Option<TaskSpec> {
        let specs = &self.specs;
        let (entry, svc) = specs.lookup(service)?;
        let trigger = match event.kind {
            SocketEventKind::Alarm { .. } => Trigger::Alarm {
                object: object.name.clone(),
            },
            _ => {
                if !entry.triggers.websocket
                    || !specs
                        .plans
                        .plan_arc(entry.tenant)
                        .allows_trigger(TriggerKind::Websocket)
                {
                    return None;
                }
                Trigger::Socket {
                    object: object.name.clone(),
                }
            }
        };
        let mut spec = specs.spec(&entry, &svc, trigger, Payload::Socket(event), None);
        if svc.config.ws.warm() {
            spec.knobs.reuse_isolates = true;
            spec.knobs.reuse_instances = true;
        }
        spec.object = Some(object);
        Some(spec)
    }

    fn limits(&self, tenant: TenantId, service: ServiceId) -> SocketLimits {
        let plan = self.specs.plans.plan_arc(tenant);
        let max_per_service = self
            .specs
            .services
            .load()
            .get(&service)
            .and_then(|s| s.config.ws.max_sockets)
            .unwrap_or(0);
        SocketLimits {
            max_sockets: plan.max_sockets,
            max_per_object: plan.max_sockets_per_object,
            max_per_service,
            max_message_bytes: plan.ws_max_message_bytes.max(1),
        }
    }

    fn log(&self, tenant: TenantId, service: ServiceId, message: String) {
        let version = self
            .specs
            .lookup(service)
            .map_or(rt_types::VersionNo(0), |(e, _)| e.version);
        let mut rec = rt_obs::LogRecord::new(
            service,
            version,
            rt_obs::LogLevel::Warn,
            rt_obs::LogKind::System,
            message,
        );
        rec.ts_ms = chrono::Utc::now().timestamp_millis();
        rec.tenant_id = Some(tenant);
        rec.trigger = "websocket".to_string();
        self.logs.append(rec);
    }

    fn alarm_done(&self, service: ServiceId, object: &str, scheduled_ms: i64) {
        let (backend, object) = (self.data.durable(service), object.to_string());
        tokio::spawn(async move {
            if let Err(e) = backend.alarm_done(&object, scheduled_ms).await {
                tracing::warn!(%service, error = %e, "durable alarm not cleared (it fires again)");
            }
        });
    }
}

impl QueueTaskFactory for Specs {
    fn task_for(&self, service: ServiceId, batch: &[Leased]) -> Option<TaskSpec> {
        let (entry, svc) = self.lookup(service)?;
        if !self.consumes_queue(&entry) || batch.is_empty() {
            return None;
        }
        let trigger = Trigger::Queue {
            batch: batch.iter().map(|m| m.id).collect(),
            attempt: batch[0].attempt,
        };
        let msgs = batch
            .iter()
            .map(|m| QueueMessage {
                id: m.id,
                body: m.body.clone(),
                attempt: m.attempt,
            })
            .collect();
        Some(self.spec(&entry, &svc, trigger, Payload::Queue(msgs), None))
    }

    fn wall_hint(&self, service: ServiceId) -> Option<Duration> {
        self.wall_of(service)
    }

    fn routable(&self, service: ServiceId) -> bool {
        self.lookup(service)
            .is_some_and(|(entry, _)| self.consumes_queue(&entry))
    }
}

impl Specs {
    /// The service's `queue` trigger is on and its tenant's plan allows `queue` (B5.3, SEC-38):
    /// otherwise nothing is leased, so no delivery attempt is burned while it cannot run.
    fn consumes_queue(&self, entry: &RouteEntry) -> bool {
        entry.triggers.queue
            && self
                .plans
                .plan_arc(entry.tenant)
                .allows_trigger(rt_types::TriggerKind::Queue)
    }
}

impl CallTaskFactory for Specs {
    fn child_task(
        &self,
        parent: &TaskCtx,
        target: &RouteEntry,
        req: CallRequest,
        depth: u8,
    ) -> Result<TaskSpec, CallError> {
        let svc = self
            .services
            .load()
            .get(&target.service)
            .cloned()
            .ok_or(CallError::NotFound)?;
        let (parts, body) = req.into_parts();
        let mut headers = parts.headers;
        let names: Vec<_> = headers
            .keys()
            .filter(|k| k.as_str().starts_with("x-rt-"))
            .cloned()
            .collect();
        for n in names {
            headers.remove(n);
        }
        // Same request headers an HTTP task gets (docs/handlers.md): the callee sees its own
        // `/s/{slug}` prefix (a call carries only path + query) and that it was called, not routed.
        if let Ok(v) = http::HeaderValue::from_str(&format!("/s/{}", target.slug)) {
            headers.insert(http::HeaderName::from_static("x-rt-base-path"), v);
        }
        headers.insert(
            http::HeaderName::from_static("x-rt-trigger"),
            http::HeaderValue::from_static("call"),
        );
        let uri = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".to_string())
            .parse::<http::Uri>()
            .unwrap_or_else(|_| http::Uri::from_static("/"));
        let http_req = HttpRequest {
            method: parts.method,
            uri,
            headers,
            remote_addr: None,
            body,
        };
        let mut spec = self.spec(
            target,
            &svc,
            Trigger::Call {
                parent: parent.task,
                depth,
            },
            Payload::Http(http_req),
            parent.trace.clone(),
        );
        Self::durable_defaults(&svc, &mut spec);
        Ok(spec)
    }

    fn is_durable(&self, service: ServiceId) -> bool {
        self.services.load().get(&service).is_some_and(|s| s.config.durable)
    }
}

impl Specs {
    /// Changes with the route table and with every swap of the service table.
    fn events_generation(&self) -> u64 {
        self.routes.generation() ^ (Arc::as_ptr(&*self.services.load()) as usize as u64)
    }

    /// Routable services whose plan allows `kind`, with their route.
    fn event_services(&self, kind: TriggerKind) -> Vec<(Arc<RouteEntry>, Arc<Service>)> {
        let ids: Vec<ServiceId> = self.services.load().keys().copied().collect();
        ids.into_iter()
            .filter_map(|id| self.lookup(id))
            .filter(|(e, _)| self.plans.plan_arc(e.tenant).allows_trigger(kind))
            .collect()
    }

    fn namespace_of(entry: &RouteEntry) -> Namespace {
        Namespace {
            tenant: entry.tenant,
            project: entry.project,
            environment: entry.environment.clone(),
        }
    }
}

impl PubsubTaskFactory for Specs {
    fn generation(&self) -> u64 {
        self.events_generation()
    }

    fn subscriptions(&self) -> Vec<SubRow> {
        let mut out = Vec::new();
        for (entry, svc) in self.event_services(TriggerKind::Pubsub) {
            for s in &svc.config.events.subscribe {
                out.push(SubRow {
                    ns: Self::namespace_of(&entry),
                    service: entry.service,
                    pattern: s.subject.clone(),
                    group: s.group.clone(),
                });
            }
        }
        out
    }

    fn task_for(
        &self,
        service: ServiceId,
        msg: PubsubMessage,
        depth: u8,
        wall_cap: Option<Duration>,
    ) -> Option<TaskSpec> {
        let (entry, svc) = self.lookup(service)?;
        if !self.plans.plan_arc(entry.tenant).allows_trigger(TriggerKind::Pubsub) {
            return None;
        }
        let trigger = Trigger::Pubsub {
            subject: Arc::from(msg.subject.as_str()),
            depth,
            reply: msg.replyable,
        };
        let mut spec = self.spec(&entry, &svc, trigger, Payload::Pubsub(msg), None);
        if let Some(cap) = wall_cap {
            spec.limits.wall = spec.limits.wall.min(cap);
        }
        Some(spec)
    }
}

impl StreamTaskFactory for Specs {
    /// The same cap the event bus passes to append (`max_stream_mb`).
    fn stream_cap(&self, tenant: TenantId) -> u64 {
        self.plans.plan_arc(tenant).max_stream_mb.saturating_mul(1024 * 1024)
    }

    fn generation(&self) -> u64 {
        // A deleted topic sends no control event: a 30 s epoch makes the engine re-resolve its
        // topics (and drop the cursors of a missing one) at least that often.
        self.events_generation() ^ (SystemWall.now_ms() as u64 / 30_000).rotate_left(32)
    }

    fn consumers(&self) -> Vec<ConsumerDecl> {
        let mut out = Vec::new();
        for (entry, svc) in self.event_services(TriggerKind::Stream) {
            for c in &svc.config.events.consume {
                out.push(ConsumerDecl {
                    service: entry.service,
                    ns: Self::namespace_of(&entry),
                    topic: c.topic.clone(),
                    group: c.group.clone(),
                    start: c.start,
                    batch_size: c.batch_size,
                    max_attempts: c.max_attempts,
                    on_exhausted: c.on_exhausted,
                });
            }
        }
        out
    }

    fn task_for(&self, service: ServiceId, batch: StreamBatch) -> Option<TaskSpec> {
        let (entry, svc) = self.lookup(service)?;
        if !self.plans.plan_arc(entry.tenant).allows_trigger(TriggerKind::Stream) || batch.records.is_empty() {
            return None;
        }
        let trigger = Trigger::Stream {
            topic: Arc::from(batch.topic.as_str()),
            partition: batch.partition,
            attempt: batch.attempt,
        };
        Some(self.spec(&entry, &svc, trigger, Payload::Stream(batch), None))
    }
}

/// The async engine's tasks: the stored request (sealed in `queue.db`) opened with the node's
/// secrets backend and run as an HTTP task.
pub struct AsyncTasks {
    pub specs: Arc<Specs>,
    pub secrets: Arc<dyn SecretsBackend>,
}

impl AsyncTasks {
    fn open(&self, inv: &LeasedInvocation, part: u8, bytes: &[u8]) -> Option<Vec<u8>> {
        let sealed = Sealed::from_bytes(bytes).ok()?;
        let aad = invocation_aad(inv.service, inv.id, part);
        self.secrets.open(&aad, &sealed).ok().map(|z| z.to_vec())
    }
}

impl AsyncTaskFactory for AsyncTasks {
    fn task_for(&self, inv: &LeasedInvocation) -> Result<Option<TaskSpec>, &'static str> {
        let specs = &self.specs;
        let Some((entry, svc)) = specs.lookup(inv.service) else {
            return Ok(None);
        };
        if !entry.triggers.http || !specs.plans.plan_arc(entry.tenant).allows_trigger(TriggerKind::Http) {
            return Ok(None);
        }
        let headers = self.open(inv, b'h', &inv.sealed_headers).ok_or(SEALED_UNREADABLE)?;
        let body = self.open(inv, b'b', &inv.sealed_body).ok_or(SEALED_UNREADABLE)?;
        let pairs: Vec<(String, String)> = serde_json::from_slice(&headers).map_err(|_| SEALED_UNREADABLE)?;
        let method = http::Method::from_bytes(inv.method.as_bytes()).map_err(|_| SEALED_UNREADABLE)?;
        let uri = inv.uri.parse::<http::Uri>().map_err(|_| SEALED_UNREADABLE)?;
        let mut map = http::HeaderMap::new();
        for (k, v) in pairs {
            if k.starts_with("x-rt-") {
                continue;
            }
            if let (Ok(k), Ok(v)) = (
                http::HeaderName::from_bytes(k.as_bytes()),
                http::HeaderValue::from_str(&v),
            ) {
                map.append(k, v);
            }
        }
        let set = |map: &mut http::HeaderMap, k: &'static str, v: &str| {
            if let Ok(v) = http::HeaderValue::from_str(v) {
                map.insert(http::HeaderName::from_static(k), v);
            }
        };
        set(&mut map, "x-rt-base-path", &format!("/s/{}", entry.slug));
        set(&mut map, "x-rt-trigger", "http");
        set(&mut map, "x-rt-invocation-id", &inv.id.to_string());
        set(&mut map, "x-rt-invocation-attempt", &inv.attempt.to_string());
        let req = HttpRequest {
            method,
            uri,
            headers: map,
            remote_addr: None,
            body: rt_types::Body::from(bytes::Bytes::from(body)),
        };
        let mut spec = specs.spec(&entry, &svc, Trigger::Http, Payload::Http(req), None);
        Specs::durable_defaults(&svc, &mut spec);
        Ok(Some(spec))
    }

    fn visibility(&self, service: ServiceId) -> Duration {
        let specs = &self.specs;
        let Some((entry, svc)) = specs.lookup(service) else {
            return Duration::from_secs(60);
        };
        let plan = specs.plans.plan_arc(entry.tenant);
        let eff = specs.config.load().service_effective(svc.profile, &svc.config, &plan);
        let knobs = eff.knobs(&plan);
        // A first run may wait on a build (or a cold start) before the handler starts: the lease
        // must outlast it, or the record is leased again and runs twice.
        let config = specs.config.load();
        let build = match entry.runtime {
            rt_types::RuntimeKind::Wasm => config.build.wasm_timeout,
            _ => config.build.timeout,
        };
        knobs.queue_timeout + build + eff.limits_for(&Trigger::Http).wall + Duration::from_secs(10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(id: TenantId, plan: &str, overrides: serde_json::Value) -> Tenant {
        Tenant {
            id,
            name: "t".into(),
            plan: plan.into(),
            overrides,
            created_at_ms: 0,
            disabled: false,
        }
    }

    fn config_with_pro(storage_mb: u64) -> Config {
        let mut c = Config::default();
        c.plans.insert(
            "pro".into(),
            Plan {
                storage_mb,
                rps: 100,
                ..Plan::default()
            },
        );
        c
    }

    #[test]
    fn a_resolved_plan_is_shared_until_the_tenant_or_the_config_changes() {
        let config = Arc::new(ArcSwap::from_pointee(config_with_pro(500)));
        let tenants = Arc::new(DashMap::new());
        let id = TenantId::from_u128(1);
        tenants.insert(id, tenant(id, "pro", serde_json::json!({"rps": 7})));
        let plans = TenantPlans::new(config.clone(), tenants.clone());

        let first = plans.plan_arc(id);
        assert_eq!((first.rps, first.storage_mb), (7, 500), "plan + overrides");
        assert!(
            Arc::ptr_eq(&first, &plans.plan_arc(id)),
            "a repeated lookup reuses the resolution"
        );
        assert_eq!(plans.plan_for(id), *first);

        // Tenant PATCH (new overrides) applies on the next lookup.
        tenants.insert(id, tenant(id, "pro", serde_json::json!({"rps": 9})));
        let patched = plans.plan_arc(id);
        assert_eq!(patched.rps, 9);
        assert!(!Arc::ptr_eq(&first, &patched));

        // SIGHUP swaps the config: the plan body follows.
        config.store(Arc::new(config_with_pro(800)));
        assert_eq!(plans.plan_arc(id).storage_mb, 800);

        // A deleted tenant falls back to `free`.
        tenants.remove(&id);
        assert_eq!(*plans.plan_arc(id), config.load().plan("free"));
    }

    /// SEC-38: a plan without `queue` stops queue consumption at dispatch (nothing is leased).
    #[test]
    fn ac_sec_38_queue_consumption_needs_the_queue_trigger_in_the_plan() {
        use rt_types::{
            Access, Digest, IngressRules, RouteSnapshot, RuntimeKind, TriggerKind, TriggerSwitches, VersionNo,
        };
        let tid = TenantId::from_u128(1);
        let sid = ServiceId::from_u128(2);
        let mut cfg = Config::default();
        cfg.plans.insert(
            "noq".into(),
            Plan {
                allowed_triggers: vec![TriggerKind::Http, TriggerKind::Call],
                ..Plan::default()
            },
        );
        let config = Arc::new(ArcSwap::from_pointee(cfg));
        let tenants = Arc::new(DashMap::new());
        tenants.insert(tid, tenant(tid, "noq", serde_json::Value::Null));
        let routes = RouteTable::new(None);
        routes.swap(&RouteSnapshot {
            generation: 1,
            entries: vec![RouteEntry {
                tenant: tid,
                service: sid,
                slug: "q".into(),
                runtime: RuntimeKind::Wasm,
                version: VersionNo(1),
                digest: Digest::of(b"q"),
                access: Access::Public,
                triggers: TriggerSwitches::default(),
                ingress: IngressRules::default(),
                profile: None,
                hosts: Vec::new(),
                project: rt_types::ProjectId::from_u128(1),
                environment: rt_types::default_environment(),
                call: rt_types::CallAccess::default(),
            }],
            ..RouteSnapshot::default()
        });
        let svc = Service {
            id: sid,
            tenant_id: tid,
            slug: "q".into(),
            runtime: RuntimeKind::Wasm,
            name: None,
            profile: None,
            environment: rt_types::default_environment(),
            project_id: rt_types::ProjectId::from_u128(1),
            project: rt_types::default_project(),
            config: rt_types::ServiceConfig::default(),
            live_version: Some(VersionNo(1)),
            etag: rt_types::Etag(1),
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let specs = Specs {
            config: config.clone(),
            plans: Arc::new(TenantPlans::new(config.clone(), tenants.clone())),
            routes,
            services: Arc::new(ArcSwap::from_pointee(HashMap::from([(sid, Arc::new(svc))]))),
            unfloored: ArcSwap::default(),
        };
        let leased = [Leased {
            id: rt_types::MsgId::new(),
            body: bytes::Bytes::new(),
            attempt: 1,
            enqueued_at_ms: 0,
        }];
        assert!(!specs.routable(sid), "plan without `queue`: nothing is leased");
        assert!(QueueTaskFactory::task_for(&specs, sid, &leased).is_none());

        tenants.insert(tid, tenant(tid, "free", serde_json::Value::Null));
        assert!(specs.routable(sid), "the free plan allows `queue`");
        assert!(QueueTaskFactory::task_for(&specs, sid, &leased).is_some());
    }

    // ---- plans ---------------------------------------------------------------------------------

    #[test]
    fn an_unknown_tenant_is_free_and_stays_cached_until_it_becomes_known() {
        let config = Arc::new(ArcSwap::from_pointee(config_with_pro(500)));
        let tenants = Arc::new(DashMap::new());
        let plans = TenantPlans::new(config.clone(), tenants.clone());
        let id = TenantId::from_u128(3);
        let free = plans.plan_arc(id);
        assert!(Arc::ptr_eq(&free, &plans.plan_arc(id)), "the fallback is cached too");
        assert_eq!(plans.plan_name(id), "free");
        tenants.insert(id, tenant(id, "pro", serde_json::Value::Null));
        assert_eq!(
            plans.plan_arc(id).storage_mb,
            500,
            "once the tenant exists its own plan applies"
        );
        assert_eq!(plans.plan_name(id), "pro");
    }

    #[test]
    fn overrides_that_do_not_make_a_valid_plan_fall_back_to_the_plain_plan() {
        let config = Arc::new(ArcSwap::from_pointee(config_with_pro(500)));
        let tenants = Arc::new(DashMap::new());
        let id = TenantId::from_u128(4);
        tenants.insert(id, tenant(id, "pro", serde_json::json!({"rps": "fast"})));
        let plans = TenantPlans::new(config, tenants);
        assert_eq!(plans.plan_arc(id).rps, 100, "the pro plan without the bad override");
    }

    #[test]
    fn the_key_cache_counts_what_it_holds() {
        let cache = KeyCache::default();
        assert!(cache.is_empty());
        let key = ApiKey {
            id: KeyId::from_u128(1),
            tenant_id: TenantId::from_u128(1),
            name: None,
            scopes: rt_types::Scopes::default(),
            project_ids: None,
            environments: None,
            expires_at_ms: None,
            created_at_ms: 0,
            last_used_at_ms: None,
            revoked: false,
            parent_key_id: None,
            hmac: Vec::new(),
        };
        cache.upsert(key.clone());
        assert_eq!(cache.len(), 1);
        assert!(cache.find(key.id).is_some());
        cache.revoke(key.id);
        assert!(cache.is_empty() && cache.find(key.id).is_none());
    }

    /// KEY-035: the update of a key read before its revocation but delivered after it does not
    /// bring the key back; nor does a key that arrives already revoked.
    #[test]
    fn a_stale_update_after_a_revocation_does_not_bring_the_key_back() {
        let cache = KeyCache::default();
        let key = ApiKey {
            id: KeyId::from_u128(7),
            tenant_id: TenantId::from_u128(1),
            name: None,
            scopes: rt_types::Scopes::default(),
            project_ids: None,
            environments: None,
            expires_at_ms: None,
            created_at_ms: 0,
            last_used_at_ms: None,
            revoked: false,
            parent_key_id: Some(KeyId::from_u128(6)),
            hmac: Vec::new(),
        };
        cache.revoke(key.id);
        cache.upsert(key.clone());
        assert!(
            cache.find(key.id).is_none(),
            "revoked first, the late upsert is ignored"
        );
        assert!(cache.is_empty());
        let other = ApiKey {
            id: KeyId::from_u128(8),
            revoked: true,
            ..key
        };
        cache.upsert(other.clone());
        assert!(cache.find(other.id).is_none(), "a revoked key is never served");
    }

    // ---- task factories ------------------------------------------------------------------------

    fn specs_with(triggers: rt_types::TriggerSwitches) -> (Specs, ServiceId) {
        use rt_types::{Access, Digest, IngressRules, RouteSnapshot, RuntimeKind, VersionNo};
        let (tid, sid) = (TenantId::from_u128(1), ServiceId::from_u128(2));
        let config = Arc::new(ArcSwap::from_pointee(Config::default()));
        let tenants = Arc::new(DashMap::new());
        tenants.insert(tid, tenant(tid, "free", serde_json::Value::Null));
        let routes = RouteTable::new(None);
        routes.swap(&RouteSnapshot {
            generation: 1,
            entries: vec![RouteEntry {
                tenant: tid,
                service: sid,
                slug: "svc".into(),
                runtime: RuntimeKind::Wasm,
                version: VersionNo(1),
                digest: Digest::of(b"svc"),
                access: Access::Public,
                triggers,
                ingress: IngressRules::default(),
                profile: None,
                hosts: Vec::new(),
                project: rt_types::ProjectId::from_u128(1),
                environment: rt_types::default_environment(),
                call: rt_types::CallAccess::default(),
            }],
            ..RouteSnapshot::default()
        });
        let svc = Service {
            id: sid,
            tenant_id: tid,
            slug: "svc".into(),
            runtime: RuntimeKind::Wasm,
            name: None,
            profile: None,
            environment: rt_types::default_environment(),
            project_id: rt_types::ProjectId::from_u128(1),
            project: rt_types::default_project(),
            config: rt_types::ServiceConfig::default(),
            live_version: Some(VersionNo(1)),
            etag: rt_types::Etag(1),
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let specs = Specs {
            config: config.clone(),
            plans: Arc::new(TenantPlans::new(config, tenants)),
            routes,
            services: Arc::new(ArcSwap::from_pointee(HashMap::from([(sid, Arc::new(svc))]))),
            unfloored: ArcSwap::default(),
        };
        (specs, sid)
    }

    fn schedule(service: ServiceId, kind: ScheduleKind) -> Schedule {
        Schedule {
            id: rt_types::ScheduleId::from_u128(9),
            service_id: service,
            kind,
            payload: serde_json::json!({"k": 1}),
            enabled: true,
            next_due_ms: None,
        }
    }

    #[test]
    fn interval_and_cron_fires_make_tasks_only_while_their_trigger_is_on() {
        let on = rt_types::TriggerSwitches::default();
        let (specs, sid) = specs_with(on);
        let interval = schedule(sid, ScheduleKind::Interval { every_seconds: 60 });
        let cron = schedule(
            sid,
            ScheduleKind::Cron {
                expr: "* * * * *".into(),
                tz: "UTC".into(),
            },
        );
        let task = ScheduleTaskFactory::task_for(&specs, &cron, 5).expect("cron task");
        assert!(matches!(task.trigger, Trigger::Cron { .. }));
        assert!(matches!(
            ScheduleTaskFactory::task_for(&specs, &interval, 5)
                .expect("interval")
                .trigger,
            Trigger::Interval { .. }
        ));

        let (specs, sid) = specs_with(rt_types::TriggerSwitches {
            cron: false,
            interval: false,
            ..on
        });
        let cron = schedule(
            sid,
            ScheduleKind::Cron {
                expr: "* * * * *".into(),
                tz: "UTC".into(),
            },
        );
        assert!(
            ScheduleTaskFactory::task_for(&specs, &cron, 5).is_none(),
            "cron switched off"
        );
        assert!(
            ScheduleTaskFactory::task_for(&specs, &schedule(sid, ScheduleKind::Interval { every_seconds: 60 }), 5)
                .is_none()
        );
        let item = DelayedItem {
            id: "d".into(),
            gen: 1,
            run_at_ms: 1,
            attempt: 1,
            payload: bytes::Bytes::from_static(b"{}"),
        };
        assert!(
            DelayedTaskFactory::task_for(&specs, sid, &item).is_none(),
            "one-offs ride the interval switch"
        );
    }

    #[test]
    fn a_one_off_whose_payload_is_not_json_is_delivered_with_a_null_payload() {
        let (specs, sid) = specs_with(rt_types::TriggerSwitches::default());
        let item = DelayedItem {
            id: "d".into(),
            gen: 1,
            run_at_ms: 7,
            attempt: 1,
            payload: bytes::Bytes::from_static(b"{oops"),
        };
        let task = DelayedTaskFactory::task_for(&specs, sid, &item).expect("task");
        let Payload::Delayed(ev) = task.payload else {
            panic!("a delayed payload")
        };
        assert_eq!(
            (ev.id.as_str(), ev.scheduled_at_ms, ev.payload),
            ("d", 7, serde_json::Value::Null)
        );
    }

    #[test]
    fn socket_events_need_the_websocket_trigger_but_an_alarm_does_not() {
        let dir = tempfile::tempdir().expect("dir");
        let tasks = |triggers| {
            let (specs, sid) = specs_with(triggers);
            let data = rt_store::LocalServiceData::open(rt_store::DataConfig::new(dir.path())).expect("data");
            (
                SocketTasks {
                    specs: Arc::new(specs),
                    logs: rt_obs::MemoryLogSink::new(),
                    data,
                },
                sid,
            )
        };
        let message = SocketEvent {
            socket: None,
            kind: SocketEventKind::Message(rt_types::WsData::Text("x".into())),
            attempt: 1,
        };
        let alarm = SocketEvent {
            socket: None,
            kind: SocketEventKind::Alarm {
                scheduled_ms: 1,
                retry: 0,
            },
            attempt: 1,
        };
        let (on, sid) = tasks(rt_types::TriggerSwitches::default());
        assert!(on.event_task(sid, ObjectRef::named("room"), message.clone()).is_some());
        let (off, sid) = tasks(rt_types::TriggerSwitches {
            websocket: false,
            ..rt_types::TriggerSwitches::default()
        });
        assert!(
            off.event_task(sid, ObjectRef::named("room"), message).is_none(),
            "websocket switched off"
        );
        assert!(
            off.event_task(sid, ObjectRef::named("room"), alarm).is_some(),
            "alarms are the service's own"
        );
    }

    #[tokio::test]
    async fn an_alarm_the_store_cannot_clear_is_logged_so_the_operator_knows_it_fires_again() {
        let (_guard, logs) = rt_testkit::capture_tracing();
        let (specs, sid) = specs_with(rt_types::TriggerSwitches::default());
        let dir = tempfile::tempdir().expect("dir");
        let data = rt_store::LocalServiceData::open(rt_store::DataConfig::new(dir.path())).expect("data");
        let faults = rt_store::Faults::new();
        data.inject_faults(faults.clone());
        let tasks = SocketTasks {
            specs: Arc::new(specs),
            logs: rt_obs::MemoryLogSink::new(),
            data,
        };
        faults.fail_next(8, rt_store::Fault::Io);
        tasks.alarm_done(sid, "room", 5);
        for _ in 0..200 {
            if logs.contains("durable alarm not cleared") {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("no warning: {}", logs.text());
    }

    /// MINOR 6: a first run that waits on a build is not leased again before the build can end.
    #[test]
    fn async_visibility_covers_the_build_timeout_of_the_service_runtime() {
        let (specs, sid) = specs_with(rt_types::TriggerSwitches::default());
        let mut config = Config::default();
        config.build.wasm_timeout = Duration::from_secs(600);
        config.build.timeout = Duration::from_secs(5);
        specs.config.store(Arc::new(config));
        let tasks = AsyncTasks {
            specs: Arc::new(specs),
            secrets: Arc::new(rt_sec::LocalSecrets::new(&rt_sec::MasterKey::from_bytes([1; 32]))),
        };
        assert!(
            tasks.visibility(sid) > Duration::from_secs(600),
            "visibility {:?} must outlast the wasm build timeout",
            tasks.visibility(sid)
        );
        assert_eq!(tasks.visibility(ServiceId::from_u128(77)), Duration::from_secs(60));
    }
}
