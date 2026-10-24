//! [`HostHandle`]: the per-task capability bundle, and [`HostServices`], the node-wide factory.
//!
//! Executors never touch stores, sockets or the control plane directly. At task start they ask
//! `HostServices::handle_for(&spec)` for a handle scoped to *this* tenant/service/task: its KV/SQL/
//! blob facades point at this service's data only, its egress policy and budgets come from the
//! service's resolved config and limits, and its log lines are tagged with the task ID.

#![allow(clippy::derivable_impls)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::Mutex;
use rt_mem::MemoryGovernor;
use rt_obs::{LogKind, LogLevel, LogRecord, LogSink};
use rt_sec::EgressDenylist;
use rt_store::{
    CpuMeter, DelayedEntry, DelayedStore, DurableListOptions, DurableValue, DurableWrite, NewDelayed, QueueStore,
    ServiceDataStore,
};
use rt_types::{
    ControlPlane, EgressRules, EnvBundle, Limits, MsgId, ObjectRef, SecretsVersion, ServiceId, SocketInfo, TaskId,
    TaskSpec, TenantId, TraceCtx, Trigger, UsageBatch, UsageDelta, VersionNo, WsData, DEFAULT_OBJECT, MAX_CALL_DEPTH,
};
use tokio::sync::{OnceCell, Semaphore};
use zeroize::Zeroizing;

use crate::calls::{BindingError, CallError, Calls, EnqueueOpts, PlatformBindings, TaskCtx};
use crate::egress::{EgressBudget, EgressError, EgressGuard, EgressPolicy, EgressUsage, Request, Response};
use crate::facade::{BlobFacade, FacadeLimits, HostError, KvFacade, SqlFacade, StorageLeft};
use crate::lock::{LockHold, LockTable};
use crate::rpc::RpcError;
use crate::schedule::{check_delay, check_id, check_payload, new_id};
use crate::sockets::{AcceptOpts, Accepted, ObjectPage, SocketHost, SocketOp, SocketPage};

/// Everything the host layer needs, shared by the whole node.
#[derive(Clone)]
pub struct HostDeps {
    pub data: Arc<dyn ServiceDataStore>,
    pub queue: Arc<dyn QueueStore>,
    /// One-off schedules of `std:schedule` (the queue database's `delayed` table).
    pub delayed: Arc<dyn DelayedStore>,
    pub egress: Arc<dyn EgressGuard>,
    pub denylist: Arc<dyn EgressDenylist>,
    pub calls: Arc<dyn Calls>,
    pub platform: Arc<dyn PlatformBindings>,
    pub logs: Arc<dyn LogSink>,
    pub control: Arc<dyn ControlPlane>,
    pub mem: Arc<dyn MemoryGovernor>,
    /// The node's WebSockets (`std:sockets`); [`crate::sockets::NoSockets`] without a hub.
    pub sockets: Arc<dyn SocketHost>,
    /// The tenant's plan as the host must apply it (egress stance, remaining quotas). `None`
    /// applies no plan limits (tests, embedders without plans).
    pub policy: Option<Arc<dyn TenantPolicy>>,
}

/// Plan-derived rules the host layer applies per tenant (SECURITY.md). The node implements
/// it from the tenant's effective plan and its quota counters.
pub trait TenantPolicy: Send + Sync {
    /// `plan.egress_default = "deny"`: only hosts on a service's `egress.allowed_hosts` are
    /// reachable.
    fn egress_default_deny(&self, tenant: TenantId) -> bool;
    /// Bytes left in the tenant's daily egress quota (`None` = unlimited).
    fn egress_bytes_left(&self, tenant: TenantId) -> Option<u64>;
    /// Bytes left in the tenant's storage quota (KV + SQL + blobs; `None` = unlimited).
    fn storage_bytes_left(&self, tenant: TenantId) -> Option<u64>;
    /// Account the egress bytes a finished task received.
    fn record_egress(&self, tenant: TenantId, service: ServiceId, bytes: u64);
    /// Account guest CPU a task used after its outcome was delivered (the tail of a streamed
    /// response), which the scheduler's per-task accounting no longer sees.
    fn record_cpu(&self, tenant: TenantId, service: ServiceId, cpu: Duration) {
        let _ = (tenant, service, cpu);
    }
    /// The service's `egress` rules from the node's in-memory service table (an unknown service
    /// has none). `None` = this policy keeps no table: the host then asks the control plane, a
    /// `meta.db` round-trip on every task start.
    fn egress_rules(&self, service: ServiceId) -> Option<EgressRules> {
        let _ = service;
        None
    }
}

/// Per-service host configuration resolved at task start (from `rt-config` + plan + service).
#[derive(Clone, Debug)]
pub struct HostTaskConfig {
    pub egress_policy: EgressPolicy,
    pub facade_limits: FacadeLimits,
    /// Remaining daily egress quota for the tenant (`None` = unlimited).
    pub egress_quota_left: Option<u64>,
}

impl Default for HostTaskConfig {
    fn default() -> Self {
        Self {
            egress_policy: EgressPolicy::default(),
            facade_limits: FacadeLimits::default(),
            egress_quota_left: None,
        }
    }
}

/// Node-wide factory of [`HostHandle`]s. Cheap to clone.
#[derive(Clone)]
pub struct HostServices {
    deps: HostDeps,
    /// The node's `std:lock` table: every task of the node shares it.
    locks: Arc<LockTable>,
}

impl HostServices {
    pub fn new(deps: HostDeps) -> Self {
        Self {
            deps,
            locks: LockTable::new(),
        }
    }

    pub fn deps(&self) -> &HostDeps {
        &self.deps
    }

    /// Per-service host configuration for `spec`: the service's `egress` rules (SECURITY.md SEC-12)
    /// under the tenant plan's egress stance, and the tenant's remaining egress and storage
    /// quotas. With a policy that keeps the service table everything is read from memory;
    /// otherwise the rules come from the control plane.
    pub async fn config_for(&self, spec: &TaskSpec) -> HostTaskConfig {
        let mut cfg = HostTaskConfig::default();
        let tenant = spec.service.tenant;
        let policy = self.deps.policy.as_ref();
        let deny = policy.is_some_and(|p| p.egress_default_deny(tenant));
        let rules = match policy.and_then(|p| p.egress_rules(spec.service.service)) {
            Some(rules) => rules,
            None => match self.deps.control.service(spec.service.service).await {
                Ok(Some(svc)) => svc.config.egress,
                _ => Default::default(),
            },
        };
        cfg.egress_policy = EgressPolicy::from_rules(&rules, deny);
        if let Some(p) = policy {
            cfg.egress_quota_left = p.egress_bytes_left(tenant);
            cfg.facade_limits.storage_bytes_left = p.storage_bytes_left(tenant);
        }
        cfg
    }

    /// Scope a handle to `spec`'s tenant/service/task. `task` is the scheduler-assigned ID,
    /// `deadline` the task's wall deadline.
    pub fn handle_for(&self, spec: &TaskSpec, task: TaskId, deadline: Instant, cfg: HostTaskConfig) -> HostHandle {
        let ctx = TaskCtx {
            tenant: spec.service.tenant,
            service: spec.service.service,
            task,
            depth: spec.trigger.call_depth(),
            deadline,
            trace: spec.trace.clone(),
            on_behalf_of: spec
                .caller
                .as_ref()
                .filter(|c| c.sdk.is_some() && rt_types::is_system_tenant(spec.service.tenant))
                .map(|c| c.tenant),
            object: spec.object.as_ref().map(|o| o.name.clone()),
        };
        let mut h = HostHandle::build(
            self.clone(),
            ctx,
            spec.service.version,
            spec.limits,
            spec.trigger.clone(),
            cfg,
            false,
        );
        // The request offered a WebSocket upgrade (ingress kept `Upgrade: websocket` for it).
        let upgrade = matches!(&spec.payload, rt_types::Payload::Http(r)
            if r.headers.get(http::header::UPGRADE).and_then(|v| v.to_str().ok()).is_some_and(|v| v.trim().eq_ignore_ascii_case("websocket")));
        if let Some(inner) = Arc::get_mut(&mut h.inner) {
            inner.object = spec.object.clone();
            inner.upgrade_offered = upgrade;
        }
        h
    }

    /// A handle for `service` outside any task: an instance pre-instantiated off the request path
    /// or parked between tasks holds it. It refuses every capability (fetch, calls, enqueue,
    /// bindings, env and secrets, storage writes), drops log lines and charges nothing to the
    /// tenant.
    pub fn detached_handle(&self, service: rt_types::ServiceRef) -> HostHandle {
        let ctx = TaskCtx {
            tenant: service.tenant,
            service: service.service,
            task: TaskId(0),
            depth: 0,
            deadline: Instant::now(),
            trace: None,
            on_behalf_of: None,
            object: None,
        };
        let limits = Limits {
            max_fetches: 0,
            ..Limits::default()
        };
        let mut cfg = HostTaskConfig::default();
        cfg.facade_limits.storage_bytes_left = Some(0);
        HostHandle::build(self.clone(), ctx, service.version, limits, Trigger::Http, cfg, true)
    }
}

/// Service-to-service calls one task may have in flight at once; further `call()`s wait for a
/// permit within the task's deadline. Calls are admitted ahead of waiting requests (their parent
/// already holds a slot, admission), so an unbounded fan-out (`Promise.all` over 500
/// calls) could otherwise take every node slot.
pub const MAX_CONCURRENT_CALLS: usize = 16;

/// `std:lock` holds one task may have at once; more answer `lock_limit`.
pub const MAX_HELD_LOCKS: usize = 32;

/// Socket operations one task may buffer behind its output gate; more answer `limit_exceeded`.
pub const MAX_GATED_SOCKET_OPS: usize = 1_000;

/// Largest message a guest may send on a socket.
pub const MAX_SOCKET_SEND_BYTES: usize = 1024 * 1024;

/// Storage keys one task may write (put or delete) in its object; more answer `limit_exceeded`.
pub const MAX_STORAGE_WRITES: usize = 1_000;

/// Host-generated (not guest) log records per task, so a misbehaving guest cannot make the
/// runtime itself flood its log.
const MAX_SYSTEM_LOGS: u32 = 20;

/// Secret values at least this long are redacted from the task's log records; shorter ones would
/// match ordinary words.
pub const MIN_REDACTED_SECRET_LEN: usize = 8;

/// Replace every secret value (of [`MIN_REDACTED_SECRET_LEN`] or more) in a log message and in the
/// string values of its fields with `[secret]`. Guests can print what they read (WASM guests get
/// secrets as WASI env vars), and logs are kept and shown to more people than the secrets are.
fn redact_secrets(
    secrets: &[(String, String)],
    mut msg: String,
    mut fields: Option<serde_json::Map<String, serde_json::Value>>,
) -> (String, Option<serde_json::Map<String, serde_json::Value>>) {
    // Longest first: a secret that contains a shorter one is replaced whole, not left with the
    // shorter one's tail showing (OBS-003).
    let mut values: Vec<&str> = secrets
        .iter()
        .map(|(_, v)| v.as_str())
        .filter(|v| v.len() >= MIN_REDACTED_SECRET_LEN)
        .collect();
    if values.is_empty() {
        return (msg, fields);
    }
    values.sort_unstable_by_key(|v| std::cmp::Reverse(v.len()));
    let redact = |s: &mut String| {
        for v in &values {
            if s.contains(v) {
                *s = s.replace(v, "[secret]");
            }
        }
    };
    redact(&mut msg);
    if let Some(map) = fields.take() {
        let mut out = serde_json::Map::with_capacity(map.len());
        for (mut k, mut v) in map {
            redact(&mut k);
            redact_value(&mut v, &redact);
            out.insert(k, v);
        }
        fields = Some(out);
    }
    (msg, fields)
}

/// Redact every string inside `v`, object keys included, at any depth.
fn redact_value(v: &mut serde_json::Value, redact: &impl Fn(&mut String)) {
    match v {
        serde_json::Value::String(s) => redact(s),
        serde_json::Value::Array(items) => items.iter_mut().for_each(|i| redact_value(i, redact)),
        serde_json::Value::Object(map) => {
            let taken = std::mem::take(map);
            for (mut k, mut item) in taken {
                redact(&mut k);
                redact_value(&mut item, redact);
                map.insert(k, item);
            }
        }
        _ => {}
    }
}

/// Capability bundle of one running task.
#[derive(Clone)]
pub struct HostHandle {
    inner: Arc<Inner>,
}

struct Inner {
    services: HostServices,
    ctx: TaskCtx,
    version: VersionNo,
    limits: Limits,
    trigger: Trigger,
    cfg: HostTaskConfig,
    cpu: CpuMeter,
    fetches_left: AtomicU32,
    concurrency: Arc<Semaphore>,
    /// `call()`s in flight ([`MAX_CONCURRENT_CALLS`]).
    calls_in_flight: Semaphore,
    usage: Arc<EgressUsage>,
    log_lines: AtomicU32,
    log_dropped: AtomicU64,
    system_logs: AtomicU32,
    env: OnceCell<Arc<Zeroizing<EnvBundle>>>,
    /// The task's storage quota left, decremented by its writes.
    storage_left: Arc<StorageLeft>,
    /// Guest CPU after the outcome ([`HostHandle::charge_cpu`]), recorded on drop.
    tail_cpu_ns: AtomicU64,
    /// Outside any task ([`HostServices::detached_handle`]): every capability is refused.
    detached: bool,
    /// `std:lock` holds of this task by ID. Dropped with the handle: when the task ends (or is
    /// killed) every hold it still has is released.
    held_locks: Mutex<HashMap<u64, LockHold>>,
    /// The object this task acts for (`TaskSpec::object`): its socket operations wait behind the
    /// output gate until [`HostHandle::commit`].
    object: Option<ObjectRef>,
    /// Socket operations waiting for [`HostHandle::commit`]; dropped with the handle when the task
    /// fails (nothing is sent).
    gated: Mutex<Vec<SocketOp>>,
    /// The object's storage writes of this task, applied by [`HostHandle::commit`] (reads see them).
    storage: Mutex<DurableWrite>,
    /// The ticket of the socket this task accepted (a WASM guest answers 101 without it; the
    /// executor adds it).
    accepted: Mutex<Option<String>>,
    /// The task's request offered a WebSocket upgrade: only then may it `accept`.
    upgrade_offered: bool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.detached {
            return;
        }
        let egress_bytes = self.usage.received();
        let tail_cpu = Duration::from_nanos(self.tail_cpu_ns.load(Ordering::Acquire));
        if egress_bytes == 0 && tail_cpu.is_zero() {
            return;
        }
        match &self.services.deps.policy {
            // Counts against the daily quotas (and is persisted with the usage counters).
            Some(p) => {
                if egress_bytes > 0 {
                    p.record_egress(self.ctx.tenant, self.ctx.service, egress_bytes);
                }
                if !tail_cpu.is_zero() {
                    p.record_cpu(self.ctx.tenant, self.ctx.service, tail_cpu);
                }
            }
            None => {
                // Fire-and-forget; the control plane batches and flushes.
                let cpu_ms = tail_cpu.as_millis() as u64;
                let delta = UsageDelta {
                    egress_bytes,
                    cpu_ms,
                    ..UsageDelta::default()
                };
                self.services.deps.control.usage(UsageBatch {
                    items: vec![(self.ctx.tenant, self.ctx.service, delta)],
                });
            }
        }
    }
}

/// A secret name as `21ci secret set` takes it: `[A-Z_][A-Z0-9_]*`, at most 255 bytes, not `RT_*`.
pub fn valid_secret_name(n: &str) -> bool {
    let b = n.as_bytes();
    !b.is_empty()
        && b.len() <= 255
        && (b[0] == b'_' || b[0].is_ascii_uppercase())
        && b.iter()
            .all(|c| *c == b'_' || c.is_ascii_uppercase() || c.is_ascii_digit())
        && !n.starts_with("RT_")
}

/// Unix time now, in milliseconds.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl HostHandle {
    fn build(
        services: HostServices,
        ctx: TaskCtx,
        version: VersionNo,
        limits: Limits,
        trigger: Trigger,
        cfg: HostTaskConfig,
        detached: bool,
    ) -> Self {
        let usage = EgressUsage::new(cfg.egress_quota_left);
        let storage_left = StorageLeft::new(cfg.facade_limits.storage_bytes_left);
        Self {
            inner: Arc::new(Inner {
                services,
                ctx,
                version,
                fetches_left: AtomicU32::new(limits.max_fetches),
                concurrency: Arc::new(Semaphore::new(limits.max_concurrent_fetches as usize)),
                calls_in_flight: Semaphore::new(MAX_CONCURRENT_CALLS),
                limits,
                trigger,
                cfg,
                cpu: CpuMeter::new(),
                usage,
                log_lines: AtomicU32::new(0),
                log_dropped: AtomicU64::new(0),
                system_logs: AtomicU32::new(0),
                env: OnceCell::new(),
                storage_left,
                tail_cpu_ns: AtomicU64::new(0),
                detached,
                held_locks: Mutex::new(HashMap::new()),
                object: None,
                gated: Mutex::new(Vec::new()),
                storage: Mutex::new(DurableWrite::default()),
                accepted: Mutex::new(None),
                upgrade_offered: false,
            }),
        }
    }

    /// A handle outside any task ([`HostServices::detached_handle`]).
    pub fn is_detached(&self) -> bool {
        self.inner.detached
    }

    /// Charge guest CPU used after the task's outcome was delivered (a streamed response's tail)
    /// to the tenant: recorded through the plan policy when the last holder drops the handle.
    pub fn charge_cpu(&self, cpu: Duration) {
        let ns = cpu.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.inner.tail_cpu_ns.fetch_add(ns, Ordering::AcqRel);
    }

    /// The task's storage quota left (`None` = unlimited), after its own writes.
    pub fn storage_left(&self) -> Option<u64> {
        self.inner.storage_left.get()
    }

    pub fn ctx(&self) -> &TaskCtx {
        &self.inner.ctx
    }

    pub fn tenant(&self) -> TenantId {
        self.inner.ctx.tenant
    }

    pub fn service(&self) -> ServiceId {
        self.inner.ctx.service
    }

    pub fn limits(&self) -> &Limits {
        &self.inner.limits
    }

    pub fn trace(&self) -> Option<&TraceCtx> {
        self.inner.ctx.trace.as_ref()
    }

    /// The node-wide services this handle was scoped from (memory governor, stores, …).
    pub fn services(&self) -> &HostServices {
        &self.inner.services
    }

    /// Host-side CPU charged to this task so far (SQL, blob, … on blocking threads). The
    /// scheduler adds it to the task's CPU usage (`cpu_ms`).
    pub fn host_cpu(&self) -> Duration {
        self.inner.cpu.total()
    }

    /// The meter to install with `rt_store::with_cpu_meter` around host calls.
    pub fn cpu_meter(&self) -> CpuMeter {
        self.inner.cpu.clone()
    }

    pub fn kv(&self) -> KvFacade {
        let i = &self.inner;
        KvFacade::new(i.services.deps.data.kv(i.ctx.service), i.cfg.facade_limits)
            .with_cpu_meter(i.cpu.clone())
            .with_storage_left(i.storage_left.clone())
    }

    pub fn sql(&self) -> SqlFacade {
        let i = &self.inner;
        SqlFacade::new(i.services.deps.data.sql(i.ctx.service)).with_cpu_meter(i.cpu.clone())
    }

    pub fn blob(&self) -> BlobFacade {
        let i = &self.inner;
        BlobFacade::new(i.services.deps.data.blob(i.ctx.service), i.cfg.facade_limits)
            .with_cpu_meter(i.cpu.clone())
            .with_storage_left(i.storage_left.clone())
    }

    /// Guarded outbound HTTP (`fetch`, `wasi:http/outgoing-handler`).
    ///
    /// One fetch is claimed from the task's count up front (a redirect chain is one fetch); the
    /// concurrency semaphore and the byte/quota counters are shared by all of the task's fetches.
    pub async fn fetch(&self, req: Request) -> Result<Response, EgressError> {
        let i = &self.inner;
        if i.detached {
            return Err(EgressError::Denied);
        }
        let claimed = i
            .fetches_left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
        if claimed.is_err() {
            return Err(EgressError::Limit);
        }
        let host = req.uri().host().unwrap_or_default().to_string();
        // Pooled connections are scoped to this tenant (SEC-52).
        let mut budget =
            EgressBudget::shared(1, i.concurrency.clone(), i.usage.clone(), i.ctx.deadline).for_tenant(i.ctx.tenant);
        let result = i
            .services
            .deps
            .egress
            .send(req, &i.cfg.egress_policy, &mut budget)
            .await;
        if let Err(e) = &result {
            if matches!(e, EgressError::Denied) {
                // The egress event for the service's logs; names only what the guest asked for.
                self.log_system(LogLevel::Warn, format!("egress denied: {host}"));
            } else if matches!(e, EgressError::InsecureRedirect) {
                self.log_system(
                    LogLevel::Warn,
                    format!(
                        "egress denied: {host} redirected from https to http (egress.allow_https_downgrade is off)"
                    ),
                );
            }
            tracing::debug!(task = %i.ctx.task, code = e.code(), "egress request failed");
        }
        result
    }

    /// An outbound WebSocket (`new WebSocket(url, protocols)`, WIT `ws-client.connect`): a `ws:` or
    /// `wss:` URL through the egress guard. Counts as one of the task's fetches; received bytes
    /// count against the daily egress quota. Codes: the `egress_*` codes, `validation_failed`.
    pub async fn ws_connect(&self, url: &str, protocols: &[String]) -> Result<crate::ws_client::WsClient, HostError> {
        let i = &self.inner;
        if i.detached {
            return Err(HostError::new(
                "egress_denied",
                "no outbound connections outside a task",
            ));
        }
        let url = url::Url::parse(url).map_err(|_| HostError::new("validation_failed", "not a URL"))?;
        if !matches!(url.scheme(), "ws" | "wss") {
            return Err(HostError::new("validation_failed", "a WebSocket URL is ws: or wss:"));
        }
        if url.fragment().is_some() {
            return Err(HostError::new("validation_failed", "a WebSocket URL has no fragment"));
        }
        if protocols
            .iter()
            .any(|p| p.is_empty() || p.contains(|c: char| c.is_whitespace() || c.is_control() || c == ','))
        {
            return Err(HostError::new("validation_failed", "a subprotocol is one token"));
        }
        let claimed = i
            .fetches_left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
        if claimed.is_err() {
            return Err(HostError::new(
                EgressError::Limit.code(),
                "the task made as many outbound requests as it may",
            ));
        }
        let host = url.host_str().unwrap_or_default().to_string();
        let deadline = tokio::time::Instant::from_std(i.ctx.deadline);
        let open = async {
            // An open socket holds one of the task's `max_concurrent_fetches` slots, like a fetch in
            // flight; waiting for it counts against the deadline.
            let slot =
                i.concurrency.clone().acquire_owned().await.map_err(|_| {
                    HostError::new(EgressError::Limit.code(), "the task's outbound connections are closed")
                })?;
            let io = i
                .services
                .deps
                .egress
                .dial(&url, &i.cfg.egress_policy)
                .await
                .map_err(|e| {
                    if matches!(e, EgressError::Denied) {
                        self.log_system(LogLevel::Warn, format!("egress denied: {host}"));
                    }
                    HostError::new(e.code(), "the WebSocket connection was refused")
                })?;
            crate::ws_client::WsClient::handshake(io, &url, protocols, i.usage.clone(), Some(slot)).await
        };
        match tokio::time::timeout_at(deadline, open).await {
            Ok(r) => r,
            Err(_) => Err(HostError::new(
                EgressError::Timeout.code(),
                "the WebSocket did not connect within the task's time",
            )),
        }
    }

    /// `std:services` `call(slug, request)`.
    ///
    /// Depth ≤ 15 ([`MAX_CALL_DEPTH`]), same tenant only (`Calls` answers `cross_tenant_call`
    /// unless the caller's plan allows it, TRG-16), and the child inherits this task's deadline
    /// (the call is abandoned when the parent's wall time runs out).
    pub async fn call(&self, slug: &str, req: Request) -> Result<Response, CallError> {
        let i = &self.inner;
        if i.detached {
            return Err(CallError::Forbidden);
        }
        if i.ctx.depth >= MAX_CALL_DEPTH {
            return Err(CallError::Depth);
        }
        if Instant::now() >= i.ctx.deadline {
            return Err(CallError::Failed);
        }
        let deadline = tokio::time::Instant::from_std(i.ctx.deadline);
        let call = async {
            // Waiting for a permit counts against the deadline like the call itself.
            let _permit = i.calls_in_flight.acquire().await.map_err(|_| CallError::Failed)?;
            i.services.deps.calls.call(&i.ctx, slug, req).await
        };
        match tokio::time::timeout_at(deadline, call).await {
            Ok(r) => r,
            Err(_) => Err(CallError::Failed),
        }
    }

    /// `std:durable` `durable.get(name, { service }).fetch(req)`: a request to an object of a
    /// durable service (`slug` `None`: this service). Same depth, deadline and concurrency rules as
    /// [`Self::call`]; the request waits for the object's turn.
    pub async fn call_object(&self, slug: Option<&str>, object: &str, req: Request) -> Result<Response, CallError> {
        let i = &self.inner;
        if i.detached {
            return Err(CallError::Forbidden);
        }
        if !rt_types::valid_object_name(object) {
            return Err(CallError::NotFound);
        }
        if i.ctx.depth >= MAX_CALL_DEPTH {
            return Err(CallError::Depth);
        }
        if Instant::now() >= i.ctx.deadline {
            return Err(CallError::Failed);
        }
        let deadline = tokio::time::Instant::from_std(i.ctx.deadline);
        let call = async {
            let _permit = i.calls_in_flight.acquire().await.map_err(|_| CallError::Failed)?;
            i.services.deps.calls.call_object(&i.ctx, slug, object, req).await
        };
        match tokio::time::timeout_at(deadline, call).await {
            Ok(r) => r,
            Err(_) => Err(CallError::Failed),
        }
    }

    /// `std:services` RPC (`services.call`, WASM `call-rpc`): `method` of the service `slug` with
    /// the JSON argument `args_json` over [`HostHandle::call`]; the JSON result or the callee's
    /// (or the host's) error. The wire format is [`crate::rpc`].
    pub async fn rpc_call(&self, slug: &str, method: &str, args_json: &str) -> Result<String, RpcError> {
        let req = crate::rpc::rpc_request(method, args_json)?;
        match self.call(slug, req).await {
            Ok(resp) => crate::rpc::rpc_result(resp).await,
            Err(e) => Err(RpcError::from_call(&e)),
        }
    }

    /// `std:sdk` (WIT `sdk.invoke`): `method` of the operator SDK `name`, bound to the release this
    /// task's grants select. Same depth, deadline and concurrency rules as [`HostHandle::call`].
    pub async fn sdk_invoke(&self, name: &str, method: &str, args_json: &str) -> Result<String, RpcError> {
        let i = &self.inner;
        if i.detached {
            return Err(RpcError::from_call(&CallError::Forbidden));
        }
        if !crate::rpc::valid_method(method) {
            return Err(RpcError::new(
                "validation_failed",
                "the method is a dot-separated path of identifiers",
            ));
        }
        let args = crate::rpc::check_args(args_json)?;
        if i.ctx.depth >= MAX_CALL_DEPTH {
            return Err(RpcError::from_call(&CallError::Depth));
        }
        if Instant::now() >= i.ctx.deadline {
            return Err(RpcError::new("timeout", "the task is out of time"));
        }
        let deadline = tokio::time::Instant::from_std(i.ctx.deadline);
        let call = async {
            let _permit = i
                .calls_in_flight
                .acquire()
                .await
                .map_err(|_| RpcError::from_call(&CallError::Failed))?;
            i.services.deps.calls.sdk_invoke(&i.ctx, name, method, args).await
        };
        match tokio::time::timeout_at(deadline, call).await {
            Ok(r) => r,
            Err(_) => Err(RpcError::new(
                "timeout",
                "the SDK did not answer within the task's deadline",
            )),
        }
    }

    /// `std:queue` `enqueue(slug, body, opts)`.
    pub async fn enqueue(&self, slug: &str, body: Bytes, opts: EnqueueOpts) -> Result<MsgId, CallError> {
        let i = &self.inner;
        if i.detached {
            return Err(CallError::Forbidden);
        }
        i.services.deps.calls.enqueue(&i.ctx, slug, body, opts).await
    }

    /// `std:platform` `platform.fetch(binding, request)`.
    ///
    /// `binding_denied`, `binding_not_allowed` and `rate_limited` resolve to a normal response
    /// (`403`/`403`/`429` with `x-rt-reason`, contracts C-8); `platform.fetch` does not throw
    /// for them. Unknown bindings and upstream failures are errors.
    pub async fn platform_fetch(&self, binding: &str, req: Request) -> Result<Response, BindingError> {
        let i = &self.inner;
        if i.detached {
            return Ok(BindingError::Denied.to_response());
        }
        match i.services.deps.platform.fetch(&i.ctx, binding, req).await {
            Ok(resp) => Ok(resp),
            Err(e @ (BindingError::Denied | BindingError::NotAllowed | BindingError::RateLimited)) => {
                Ok(e.to_response())
            }
            Err(e) => Err(e),
        }
    }

    /// One secret of this service, `None` when it is not set. Only secret values are reachable (a
    /// plain env var is not one). `validation_failed` for a name that is not a secret name.
    pub async fn secret(&self, name: &str) -> Result<Option<String>, HostError> {
        if !valid_secret_name(name) {
            return Err(HostError::new("validation_failed", "not a secret name"));
        }
        let env = self.env().await?;
        Ok(env
            .secrets
            .iter()
            .rev()
            .find(|(k, _)| k.as_str() == name)
            .map(|(_, v)| v.clone()))
    }

    /// `std:lock` acquire of `key` in this service: waits at most `wait`, holds for at most `lease`
    /// (see [`crate::lock`]). The hold stays with the task under the returned ID until
    /// [`Self::lock_release`], or until the task ends.
    pub async fn lock_acquire(&self, key: &str, wait: Duration, lease: Duration) -> Result<u64, HostError> {
        let i = &self.inner;
        if i.detached {
            return Err(HostError::new(
                "unavailable",
                "std:lock is not available outside a task",
            ));
        }
        if i.held_locks.lock().len() >= MAX_HELD_LOCKS {
            return Err(HostError::new("lock_limit", "this task holds too many locks"));
        }
        let hold = i.services.locks.acquire(i.ctx.service, key, wait, lease).await?;
        let id = hold.id();
        i.held_locks.lock().insert(id, hold);
        Ok(id)
    }

    /// Release a hold of this task (a no-op for an unknown or already released ID).
    pub fn lock_release(&self, id: u64) {
        let hold = self.inner.held_locks.lock().remove(&id);
        drop(hold);
    }

    /// `std:schedule` `after(seconds, payload, { id })`: a schedule due in `delay` (at most 30 days).
    /// Returns its id (generated when `id` is `None`) and its due time.
    pub async fn schedule_after(
        &self,
        id: Option<String>,
        delay: Duration,
        payload: Bytes,
    ) -> Result<(String, i64), HostError> {
        check_delay(delay)?;
        self.schedule_put(id, now_ms().saturating_add(delay.as_millis() as i64), payload)
            .await
    }

    /// `std:schedule` `at(time, payload, { id })`: a schedule due at `run_at_ms` (a time in the past
    /// is due at once), at most 30 days from now.
    pub async fn schedule_at(
        &self,
        id: Option<String>,
        run_at_ms: i64,
        payload: Bytes,
    ) -> Result<(String, i64), HostError> {
        check_delay(Duration::from_millis(run_at_ms.saturating_sub(now_ms()).max(0) as u64))?;
        self.schedule_put(id, run_at_ms, payload).await
    }

    async fn schedule_put(
        &self,
        id: Option<String>,
        run_at_ms: i64,
        payload: Bytes,
    ) -> Result<(String, i64), HostError> {
        let i = &self.inner;
        if i.detached {
            return Err(HostError::new(
                "unavailable",
                "std:schedule is not available outside a task",
            ));
        }
        let id = match id {
            Some(id) => {
                check_id(&id)?;
                id
            }
            None => new_id(),
        };
        check_payload(&payload)?;
        let stored = i
            .services
            .deps
            .delayed
            .put(
                i.ctx.service,
                NewDelayed {
                    id: id.clone(),
                    run_at_ms,
                    payload,
                },
            )
            .await
            .map_err(|e| HostError::from_store(&e))?;
        match stored {
            Some(_) => Ok((id, run_at_ms)),
            None => Err(HostError::new(
                "schedule_limit",
                "too many pending schedules for this service",
            )),
        }
    }

    /// `std:schedule` `cancel(id)`: `false` when no such schedule is pending.
    pub async fn schedule_cancel(&self, id: &str) -> Result<bool, HostError> {
        let i = &self.inner;
        if i.detached {
            return Err(HostError::new(
                "unavailable",
                "std:schedule is not available outside a task",
            ));
        }
        check_id(id)?;
        i.services
            .deps
            .delayed
            .cancel(i.ctx.service, id)
            .await
            .map_err(|e| HostError::from_store(&e))
    }

    /// `std:schedule` `list({ after, limit })`: pending schedules in id order, after `after`.
    pub async fn schedule_list(&self, after: Option<&str>, limit: usize) -> Result<Vec<DelayedEntry>, HostError> {
        let i = &self.inner;
        if i.detached {
            return Err(HostError::new(
                "unavailable",
                "std:schedule is not available outside a task",
            ));
        }
        let limit = limit.clamp(1, 100);
        i.services
            .deps
            .delayed
            .list(i.ctx.service, after, limit)
            .await
            .map_err(|e| HostError::from_store(&e))
    }

    /// The object this task acts for, if any.
    pub fn object(&self) -> Option<&ObjectRef> {
        self.inner.object.as_ref()
    }

    /// The object a socket call without an explicit object addresses: the task's object, else
    /// [`DEFAULT_OBJECT`].
    pub fn object_name(&self) -> &str {
        self.inner.object.as_ref().map_or(DEFAULT_OBJECT, |o| &o.name)
    }

    fn socket_host(&self) -> Result<&Arc<dyn SocketHost>, HostError> {
        if self.inner.detached {
            return Err(HostError::new(
                "unavailable",
                "std:sockets is not available outside a task",
            ));
        }
        Ok(&self.inner.services.deps.sockets)
    }

    /// `sockets.accept(req, opts)`: reserve a socket of this task's object for the upgrade its
    /// request offered (the executor checked the request; an HTTP or call task only).
    pub fn ws_accept(&self, opts: AcceptOpts) -> Result<Accepted, HostError> {
        let hub = self.socket_host()?;
        if !self.inner.upgrade_offered || !matches!(self.inner.trigger, Trigger::Http | Trigger::Call { .. }) {
            return Err(HostError::new(
                "not_upgradable",
                "only a request that offered a WebSocket upgrade can be accepted",
            ));
        }
        let a = hub.accept(&self.inner.ctx, self.object_name(), opts)?;
        *self.inner.accepted.lock() = Some(a.ticket.clone());
        Ok(a)
    }

    /// The ticket of the socket this task accepted last.
    pub fn accepted_ticket(&self) -> Option<String> {
        self.inner.accepted.lock().clone()
    }

    /// A socket operation (`send`, `broadcast`, `close`, `closeAll`). Behind the output gate when
    /// this task acts for an object and `immediate` is off: it then runs at [`Self::commit`] and the
    /// count is the hub's count now. A message over [`MAX_SOCKET_SEND_BYTES`] is `too_large`.
    pub fn ws_apply(&self, op: SocketOp, immediate: bool) -> Result<u32, HostError> {
        let hub = self.socket_host()?;
        if let SocketOp::Send { data, .. } | SocketOp::Broadcast { data, .. } = &op {
            if data.len() > MAX_SOCKET_SEND_BYTES {
                return Err(HostError::new(
                    "too_large",
                    format!("a message is at most {MAX_SOCKET_SEND_BYTES} bytes"),
                ));
            }
        }
        if immediate || self.inner.object.is_none() {
            return hub.apply(&self.inner.ctx, op);
        }
        let ctx = &self.inner.ctx;
        let n = match &op {
            SocketOp::Send { socket, .. } | SocketOp::Close { socket, .. } => {
                if !hub.exists(ctx, socket) {
                    return Err(HostError::new("not_found", "no such socket"));
                }
                1
            }
            SocketOp::Broadcast {
                object, tag, except, ..
            } => {
                let all = hub.count(ctx, Some(object), tag.as_deref());
                all.saturating_sub(except.len() as u32)
            }
            SocketOp::CloseAll { object, tag, .. } => hub.count(ctx, Some(object), tag.as_deref()),
        };
        let mut gated = self.inner.gated.lock();
        if gated.len() >= MAX_GATED_SOCKET_OPS {
            return Err(HostError::new(
                "limit_exceeded",
                "too many socket operations in one task",
            ));
        }
        gated.push(op);
        Ok(n)
    }

    /// `ws.send(data)` of a socket (behind the output gate, see [`Self::ws_apply`]).
    pub fn ws_send(&self, socket: &str, data: WsData, immediate: bool) -> Result<(), HostError> {
        self.ws_apply(
            SocketOp::Send {
                socket: socket.to_string(),
                data,
            },
            immediate,
        )
        .map(|_| ())
    }

    pub fn ws_list(
        &self,
        object: Option<&str>,
        tag: Option<&str>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<SocketPage, HostError> {
        Ok(self
            .socket_host()?
            .list(&self.inner.ctx, object, tag, limit.clamp(1, 1000), cursor))
    }

    pub fn ws_get(&self, socket: &str) -> Result<Option<SocketInfo>, HostError> {
        Ok(self.socket_host()?.get(&self.inner.ctx, socket))
    }

    pub fn ws_count(&self, object: Option<&str>, tag: Option<&str>) -> Result<u32, HostError> {
        Ok(self.socket_host()?.count(&self.inner.ctx, object, tag))
    }

    pub fn ws_objects(&self, prefix: &str, limit: usize, cursor: Option<&str>) -> Result<ObjectPage, HostError> {
        Ok(self
            .socket_host()?
            .objects(&self.inner.ctx, prefix, limit.clamp(1, 1000), cursor))
    }

    pub fn ws_set_tags(&self, socket: &str, tags: Vec<String>) -> Result<(), HostError> {
        self.socket_host()?.set_tags(&self.inner.ctx, socket, tags)
    }

    pub fn ws_attachment(&self, socket: &str) -> Result<Option<Bytes>, HostError> {
        self.socket_host()?.attachment(&self.inner.ctx, socket)
    }

    pub fn ws_set_attachment(&self, socket: &str, value: Option<Bytes>) -> Result<(), HostError> {
        self.socket_host()?.set_attachment(&self.inner.ctx, socket, value)
    }

    pub fn ws_set_auto_response(&self, object: &str, pair: Option<(String, String)>) -> Result<(), HostError> {
        self.socket_host()?.set_auto_response(&self.inner.ctx, object, pair)
    }

    // ---- std:durable storage (the task's object; behind the output gate) ---------------------

    fn storage_object(&self) -> Result<&str, HostError> {
        if self.inner.detached {
            return Err(HostError::new(
                "unavailable",
                "std:durable is not available outside a task",
            ));
        }
        match &self.inner.object {
            Some(o) => Ok(&o.name),
            None => Err(HostError::new(
                "no_object",
                "storage belongs to an object: use it in a durable object's task",
            )),
        }
    }

    fn check_storage_key(key: &[u8]) -> Result<(), HostError> {
        if key.is_empty() {
            return Err(HostError::new("validation_failed", "key must not be empty"));
        }
        if key.len() > rt_store::data::MAX_KV_KEY_BYTES {
            return Err(HostError::new(
                "kv_key_too_large",
                format!("key exceeds {} bytes", rt_store::data::MAX_KV_KEY_BYTES),
            ));
        }
        Ok(())
    }

    async fn metered<F: std::future::Future>(&self, fut: F) -> F::Output {
        rt_store::with_cpu_meter(self.inner.cpu.clone(), fut).await
    }

    /// `storage.get(key | keys)`: this task's own writes first, then the object's stored values.
    ///
    /// At most [`rt_store::data::MAX_KV_LIST_LIMIT`] keys and [`rt_store::data::MAX_RESULT_BYTES`] of
    /// values (`validation_failed`); a key named twice is read once.
    pub async fn storage_get(&self, keys: &[Bytes]) -> Result<Vec<Option<DurableValue>>, HostError> {
        let object = self.storage_object()?.to_string();
        if keys.len() > rt_store::data::MAX_KV_LIST_LIMIT {
            return Err(HostError::new(
                "validation_failed",
                format!("storage.get takes at most {} keys", rt_store::data::MAX_KV_LIST_LIMIT),
            ));
        }
        for k in keys {
            Self::check_storage_key(k)?;
        }
        let mut out: Vec<Option<Option<DurableValue>>> = Vec::with_capacity(keys.len());
        let mut missing: Vec<Bytes> = Vec::new();
        {
            let w = self.inner.storage.lock();
            for k in keys {
                match w.writes.iter().rev().find(|(wk, _)| wk == k) {
                    Some((_, v)) => out.push(Some(v.clone())),
                    None if w.delete_all => out.push(Some(None)),
                    None => {
                        out.push(None);
                        if !missing.contains(k) {
                            missing.push(k.clone());
                        }
                    }
                }
            }
        }
        let stored: std::collections::HashMap<Bytes, Option<DurableValue>> = if missing.is_empty() {
            std::collections::HashMap::new()
        } else {
            let backend = self.inner.services.deps.data.durable(self.inner.ctx.service);
            let got = self
                .metered(backend.get(&object, &missing))
                .await
                .map_err(|e| HostError::from_store(&e))?;
            missing.into_iter().zip(got).collect()
        };
        let mut left = rt_store::data::MAX_RESULT_BYTES;
        let mut result = Vec::with_capacity(keys.len());
        for (k, o) in keys.iter().zip(out) {
            let v = o.unwrap_or_else(|| stored.get(k).cloned().flatten());
            left = left
                .checked_sub(v.as_ref().map_or(0, |v| v.data.len()))
                .ok_or_else(|| {
                    HostError::new(
                        "validation_failed",
                        format!("storage.get returns at most {} bytes", rt_store::data::MAX_RESULT_BYTES),
                    )
                })?;
            result.push(v);
        }
        Ok(result)
    }

    /// `storage.put(key, value)`: buffered until [`Self::commit`]; charged to the storage quota now.
    pub fn storage_put(&self, key: Bytes, value: DurableValue) -> Result<(), HostError> {
        self.storage_object()?;
        Self::check_storage_key(&key)?;
        if value.data.len() > rt_store::data::MAX_KV_VALUE_BYTES {
            return Err(HostError::new(
                "kv_value_too_large",
                format!("value exceeds {} bytes", rt_store::data::MAX_KV_VALUE_BYTES),
            ));
        }
        let mut w = self.inner.storage.lock();
        if w.writes.len() >= MAX_STORAGE_WRITES {
            return Err(HostError::new("limit_exceeded", "too many storage writes in one task"));
        }
        if !self
            .inner
            .storage_left
            .try_charge((key.len() + value.data.len()) as u64)
        {
            return Err(HostError::new("storage_quota", "storage quota exceeded"));
        }
        w.writes.push((key, Some(value)));
        Ok(())
    }

    /// `storage.delete(key | keys)`: how many of the keys existed.
    pub async fn storage_delete(&self, keys: &[Bytes]) -> Result<u32, HostError> {
        let existing = self.storage_get(keys).await?;
        let mut w = self.inner.storage.lock();
        if w.writes.len() + keys.len() > MAX_STORAGE_WRITES {
            return Err(HostError::new("limit_exceeded", "too many storage writes in one task"));
        }
        for k in keys {
            w.writes.push((k.clone(), None));
        }
        Ok(existing.iter().filter(|v| v.is_some()).count() as u32)
    }

    /// `storage.list(opts)`: the stored range with this task's own writes applied.
    pub async fn storage_list(&self, opts: DurableListOptions) -> Result<Vec<(Bytes, DurableValue)>, HostError> {
        let object = self.storage_object()?.to_string();
        let limit = opts.limit.clamp(1, rt_store::data::MAX_KV_LIST_LIMIT);
        let (delete_all, pending): (bool, std::collections::BTreeMap<Bytes, Option<DurableValue>>) = {
            let w = self.inner.storage.lock();
            (
                w.delete_all,
                w.writes.iter().filter(|(k, _)| opts.contains(k)).cloned().collect(),
            )
        };
        let mut merged: std::collections::BTreeMap<Bytes, DurableValue> = std::collections::BTreeMap::new();
        if !delete_all {
            let backend = self.inner.services.deps.data.durable(self.inner.ctx.service);
            let wide = DurableListOptions {
                limit: (limit + pending.len()).min(rt_store::data::MAX_KV_LIST_LIMIT * 2),
                ..opts.clone()
            };
            for (k, v) in self
                .metered(backend.list(&object, wide))
                .await
                .map_err(|e| HostError::from_store(&e))?
            {
                merged.insert(k, v);
            }
        }
        for (k, v) in pending {
            match v {
                Some(v) => {
                    merged.insert(k, v);
                }
                None => {
                    merged.remove(&k);
                }
            }
        }
        let mut out: Vec<(Bytes, DurableValue)> = merged.into_iter().collect();
        if opts.reverse {
            out.reverse();
        }
        out.truncate(limit);
        Ok(out)
    }

    /// `storage.deleteAll()`: every key and the alarm of the object, at commit.
    pub fn storage_delete_all(&self) -> Result<(), HostError> {
        self.storage_object()?;
        let mut w = self.inner.storage.lock();
        *w = DurableWrite {
            delete_all: true,
            writes: Vec::new(),
            alarm: Some(None),
        };
        Ok(())
    }

    /// `storage.getAlarm()`.
    pub async fn alarm_get(&self) -> Result<Option<i64>, HostError> {
        let object = self.storage_object()?.to_string();
        {
            let w = self.inner.storage.lock();
            if let Some(a) = w.alarm {
                return Ok(a);
            }
            if w.delete_all {
                return Ok(None);
            }
        }
        let backend = self.inner.services.deps.data.durable(self.inner.ctx.service);
        self.metered(backend.alarm(&object))
            .await
            .map_err(|e| HostError::from_store(&e))
    }

    /// `storage.setAlarm(time)` / `deleteAlarm()`: one alarm per object, at commit.
    pub fn alarm_set(&self, at_ms: Option<i64>) -> Result<(), HostError> {
        self.storage_object()?;
        self.inner.storage.lock().alarm = Some(at_ms);
        Ok(())
    }

    /// Storage writes waiting for [`Self::commit`] (tests, diagnostics).
    pub fn storage_pending(&self) -> usize {
        self.inner.storage.lock().writes.len()
    }

    /// The task's handler returned: commit its object's storage writes (one transaction), then open
    /// the output gate: its buffered socket operations go to the hub in order (a socket gone
    /// meanwhile is skipped). Executors call it once, after a handler
    /// that did not fail and before the task's outcome; a task that fails never calls it.
    pub async fn commit(&self) -> Result<(), HostError> {
        // 1. The object's storage writes, in one transaction.
        let write = std::mem::take(&mut *self.inner.storage.lock());
        if !write.is_empty() {
            let object = self.storage_object()?.to_string();
            let alarm = write.alarm;
            let backend = self.inner.services.deps.data.durable(self.inner.ctx.service);
            self.metered(backend.commit(&object, write))
                .await
                .map_err(|e| HostError::from_store(&e))?;
            if let Some(at) = alarm {
                self.inner.services.deps.sockets.set_alarm(&self.inner.ctx, &object, at);
            }
        }
        // 2. Then the sends: a client never sees a message whose writes were not committed.
        let ops: Vec<SocketOp> = std::mem::take(&mut *self.inner.gated.lock());
        if ops.is_empty() {
            return Ok(());
        }
        let hub = self.socket_host()?;
        for op in ops {
            if let Err(e) = hub.apply(&self.inner.ctx, op) {
                tracing::debug!(task = %self.inner.ctx.task, code = e.code, "gated socket operation skipped");
            }
        }
        Ok(())
    }

    /// Socket operations waiting behind the output gate (tests, diagnostics).
    pub fn gated_ops(&self) -> usize {
        self.inner.gated.lock().len()
    }

    /// Release every `std:lock` hold of this task. Executors call it when the task ends: a handle
    /// can outlive its task (an idle WASM instance keeps the one of its last task), and the holds
    /// must not.
    pub fn release_locks(&self) {
        let holds: Vec<LockHold> = self.inner.held_locks.lock().drain().map(|(_, hold)| hold).collect();
        drop(holds);
    }

    /// Env + decrypted secrets for this service, cached for the task. The bundle is the control
    /// plane's shared copy ([`ControlPlane::env_shared`]), so a plane with an env cache serves it
    /// without a round-trip; it is zeroized when its last holder (this task, other tasks, an idle
    /// WASM instance or the plane's cache) drops it, not when this handle does.
    pub async fn env(&self) -> Result<Arc<Zeroizing<EnvBundle>>, HostError> {
        let i = &self.inner;
        if i.detached {
            return Err(HostError::new("unavailable", "no environment outside a task"));
        }
        i.env
            .get_or_try_init(|| async {
                match i
                    .services
                    .deps
                    .control
                    .env_shared(i.ctx.service, SecretsVersion::default())
                    .await
                {
                    Ok(bundle) => Ok(bundle),
                    Err(rt_types::CpError::RateLimited) => {
                        Err(HostError::new("rate_limited", "env fetch rate limited"))
                    }
                    Err(_) => Err(HostError::new("internal", "env unavailable")),
                }
            })
            .await
            .cloned()
    }

    /// Append a log record tagged with this task. Enforces `limits.max_log_lines` (then drops and
    /// counts), truncates long messages, never blocks.
    pub fn log(&self, level: LogLevel, msg: String) {
        let i = &self.inner;
        if i.detached {
            return;
        }
        if i.log_lines.fetch_add(1, Ordering::Relaxed) >= i.limits.max_log_lines {
            i.log_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.append(level, LogKind::Runtime, msg, None);
    }

    /// `std:log` with structured fields: `fields_json` is a JSON object, checked by
    /// [`rt_obs::parse_log_fields`] (`validation_failed` and no record when refused). Counts
    /// against the task's `max_log_lines` like [`Self::log`].
    pub fn log_fields(&self, level: LogLevel, msg: String, fields_json: &str) -> Result<(), HostError> {
        let fields =
            rt_obs::parse_log_fields(fields_json).map_err(|e| HostError::new("validation_failed", e.to_string()))?;
        let i = &self.inner;
        if i.detached {
            return Ok(());
        }
        if i.log_lines.fetch_add(1, Ordering::Relaxed) >= i.limits.max_log_lines {
            i.log_dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.append(level, LogKind::Runtime, msg, Some(fields));
        Ok(())
    }

    /// Guest log lines dropped because the task hit `max_log_lines`.
    pub fn log_dropped(&self) -> u64 {
        self.inner.log_dropped.load(Ordering::Relaxed)
    }

    /// Runtime-originated record about this task (egress denials, …), capped per task.
    pub fn log_system(&self, level: LogLevel, msg: String) {
        if self.inner.detached {
            return;
        }
        if self.inner.system_logs.fetch_add(1, Ordering::Relaxed) < MAX_SYSTEM_LOGS {
            self.append(level, LogKind::System, msg, None);
        }
    }

    fn append(
        &self,
        level: LogLevel,
        kind: LogKind,
        msg: String,
        fields: Option<serde_json::Map<String, serde_json::Value>>,
    ) {
        let i = &self.inner;
        let (msg, fields) = match i.env.get() {
            Some(env) => redact_secrets(&env.secrets, msg, fields),
            None => (msg, fields),
        };
        let mut rec = LogRecord::new(i.ctx.service, i.version, level, kind, msg);
        rec.fields = fields;
        rec.ts_ms = now_ms();
        rec.tenant_id = Some(i.ctx.tenant);
        rec.task_id = Some(i.ctx.task);
        rec.trigger = i.trigger.label().to_string();
        i.services.deps.logs.append(rec);
    }

    /// Egress accounting for this task (for tests / the fetch implementation): what is left of the
    /// fetch count, the shared concurrency gate and the live byte counters.
    pub fn egress_budget(&self) -> EgressBudget {
        let i = &self.inner;
        EgressBudget::shared(
            i.fetches_left.load(Ordering::Acquire),
            i.concurrency.clone(),
            i.usage.clone(),
            i.ctx.deadline,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// OBS-003: every secret of 8+ characters is redacted from the message and from the fields at
    /// any depth, keys included, and a secret containing a shorter one is replaced whole.
    #[test]
    fn obs_003_secrets_are_redacted_everywhere_longest_first() {
        let secrets = vec![
            ("A".to_string(), "password1".to_string()),
            ("B".to_string(), "password1-extra-long".to_string()),
            ("C".to_string(), "short".to_string()),
        ];
        let fields = json!({
            "top": "password1-extra-long",
            "outer": {"token": "x password1-extra-long y", "list": ["password1", 3]},
            "password1-extra-long": 1,
            "plain": "short stays",
        });
        let (msg, f) = redact_secrets(
            &secrets,
            "a password1-extra-long b password1".into(),
            fields.as_object().cloned(),
        );
        assert_eq!(msg, "a [secret] b [secret]");
        let f = serde_json::Value::Object(f.expect("fields"));
        let text = f.to_string();
        assert!(!text.contains("password1"), "{text}");
        assert!(!text.contains("extra-long"), "{text}");
        assert_eq!(f["outer"]["token"], json!("x [secret] y"));
        assert_eq!(f["outer"]["list"][0], json!("[secret]"));
        assert_eq!(f["[secret]"], json!(1));
        assert_eq!(
            f["plain"],
            json!("short stays"),
            "values under 8 characters are not redacted"
        );
    }
}
