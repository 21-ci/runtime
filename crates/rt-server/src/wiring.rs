//! Node wiring: build every component from a [`Config`], start the listeners and background
//! engines, and hand back a [`RunningServer`].
//!
//! Startup order (SECURITY.md):
//! 1. load config + secrets; fail fast on unknown keys / missing master key;
//! 2. memory budget (cgroup v2) → `Governor`; metrics registry; log store;
//! 3. open `meta.db` / `queue.db` (migrations), artifact store, per-service data store;
//! 4. engines: wasmtime `Engine` + epoch ticker, V8 init, `PreparedCache`s, `Builder`;
//! 5. `Scheduler` with the Deno, WASM and Build executors; sysmon;
//! 6. triggers: `RouteTable`, `ScheduleEngine`, `QueueEngine`, `CallDispatcher`;
//! 7. API routers on the four listeners; `harden_process` (seccomp/Landlock, no-dump);
//! 8. report `ready` (`/readyz`).

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use rt_api::{
    control_router, health_router, invoke_router, ApiState, ControlEvent, IdemStore, IngressBuckets, IngressCache,
    ListenAddr, NodeConfigUpdate, NodeInfo, ServeLimits, SseLimiter,
};
use rt_build::{DefaultBuilder, DefaultResolver, FetcherConfig, GuardedFetcher, ResolverConfig, UploadLimits};
use rt_cache::{CacheSettings, PreparedCache, T2Store, TamperSink, WTinyLfu};
use rt_config::{Config, LoadOptions, NodeSecrets};
use rt_exec_deno::{DenoExecutor, DenoExecutorConfig, DenoLoader, V8CodeCacheProducer};
use rt_exec_wasm::{
    build_engine, publish_pool_slots, EpochTicker, IdleSlots, PoolSizing, WasmEngineConfig, WasmExecutor, WasmLoader,
    WasmPrecompiler, EPOCH_TICK,
};
use rt_host::{
    CallError, Calls, ConfigBindings, EgressGuard, EgressGuardConfig, EnqueueOpts, FairBlockingPool, FairPoolConfig,
    GuardedEgress, HostDeps, HostServices, Request, Response, TaskCtx,
};
use rt_mem::{Governor, GovernorConfig, MemoryGovernor};
use rt_obs::{FileLogStore, HdrStatsStore, LogStore, LogStoreConfig, Metrics, MetricsOptions};
use rt_sched::{AdmissionLimits, Clock, LiveSchedConfig, SchedConfig, Scheduler, SystemClock, UnitSource, WarmTargets};
use rt_sec::{
    ArtifactMac, AuditEvent, AuditSink, Authenticator, DefaultAuthenticator, HkdfArtifactMac, KeyHasher,
    LocalQuotaEnforcer, LocalSecrets, MasterKey, PlanSource, Plugins, QuotaEnforcer, SecretsBackend, SelfDenylist,
    TracingAuditSink,
};
use rt_store::{
    ArtifactStore, DataConfig, LocalArtifactStore, LocalServiceData, MetaStore, PageReq, QueueStore, ServiceDataStore,
    SqliteMetaStore, SqliteQueueStore, SqliteStreamStore, StreamStore,
};
use rt_triggers::{
    CallDispatcher, DelayedEngine, DelayedEngineConfig, HubConfig, QueueEngine, QueueEngineConfig, RouteTable,
    ScheduleEngine, SocketHub,
};
use rt_types::{
    BuildFailReason, BuildFailure, BuildReport, ControlPlane, Digest, HttpRequest, Listener, MsgId, NodeEvent, Outcome,
    PreparedKey, Profile, RouteEntry, RuntimeKind, ScheduleId, Service, ServiceId, ServiceRef, TaskSpec, Tenant,
    TenantId, TraceCtx, UsageDelta, VersionNo,
};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::build_exec::{BuildDeps, BuildExecutor, BuildSink};
use crate::control_plane::{InProcControlPlane, MeteredQuotas};
use crate::policy::PlanPolicy;
use crate::specs::{KeyCache, Specs, SystemWall, TenantPlans};

const MIB: u64 = 1024 * 1024;

/// What the control API does without `RT_ROOT_TOKEN`, as the startup warning words it.
#[cfg(debug_assertions)]
const NO_ROOT_TOKEN_CONTROL: &str = "OPEN (debug build)";
#[cfg(not(debug_assertions))]
const NO_ROOT_TOKEN_CONTROL: &str = "disabled (503 control_disabled)";
/// How long a graceful shutdown waits for in-flight tasks (`shutdown.grace`).
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
/// How often tenant storage is re-measured on disk for the storage quota.
const STORAGE_SCAN_INTERVAL: Duration = Duration::from_secs(30);
/// Cadence of the expired-KV sweep over open service databases.
const KV_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// How often warm floors are reconciled with the live services.
const FLOOR_RECONCILE_INTERVAL: Duration = Duration::from_secs(60);
/// Cadence of the orphaned-artifact sweep.
const ORPHAN_SWEEP_INTERVAL: Duration = Duration::from_secs(3600);
/// T2 disk garbage collection cadence, in 100 ms maintenance ticks (60 s).
const T2_GC_EVERY_TICKS: u64 = 600;
/// Cache frequency sketches are saved to `meta.db` `cache_freq` this often (5 min, CCH-11) and at
/// shutdown, in 100 ms maintenance ticks.
const CACHE_FREQ_SAVE_EVERY_TICKS: u64 = 3_000;
/// `rt_queue_depth` refresh cadence, in 100 ms maintenance ticks (30 s).
const QUEUE_DEPTH_EVERY_TICKS: u64 = 300;

/// Both caches' frequency sketches as one `cache_freq` blob (Deno first, then WASM).
async fn save_cache_freq(meta: &dyn MetaStore, deno: &PreparedCache<DenoLoader>, wasm: &PreparedCache<WasmLoader>) {
    store_cache_freq(
        meta,
        rt_cache::pack_snapshots(&[&deno.persist_frequencies(), &wasm.persist_frequencies()]),
    )
    .await;
}

/// Drop the queue rows of a deleted service.
async fn purge_queue_rows(queue: &SqliteQueueStore, svc: ServiceId) {
    if let Err(e) = queue.purge_service(svc).await {
        tracing::warn!(error = %e, service = %svc, "purging the queue rows of a deleted service failed");
    }
}

/// Drop the stream cursors and dead letters of a deleted service (topics belong to the namespace).
async fn purge_stream_rows(streams: &dyn StreamStore, svc: ServiceId) {
    if let Err(e) = streams.purge_service(svc).await {
        tracing::warn!(error = %e, service = %svc, "purging the stream rows of a deleted service failed");
    }
}

async fn store_cache_freq(meta: &dyn MetaStore, blob: Vec<u8>) {
    if let Err(e) = meta.save_cache_freq(blob).await {
        tracing::warn!(error = %e, "cache frequencies not saved");
    }
}

/// `rt_queue_depth`: messages waiting in the queues of the routable services that consume one.
async fn publish_queue_depth(queue: &dyn QueueStore, specs: &Specs, metrics: &Metrics) {
    let services: Vec<ServiceId> = specs.services.load().keys().copied().collect();
    let mut depth = 0u64;
    for s in services {
        if !specs.routes.by_service(s).is_some_and(|e| e.triggers.queue) {
            continue;
        }
        if let Ok(st) = queue.stats(s).await {
            depth += st.depth;
        }
    }
    metrics.queue_depth.set(i64::try_from(depth).unwrap_or(i64::MAX));
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("configuration error: {0}")]
    Config(#[from] rt_config::ConfigError),
    #[error("startup failed: {0}")]
    Startup(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// `security.require_v8_sandbox=true` on a build without the V8 sandbox (SEC-36).
    #[error("v8_sandbox_required: this build has no V8 sandbox; set security.require_v8_sandbox=false")]
    V8SandboxRequired,
    /// `SIGHUP`: the new configuration is in effect, but a later step (the route refresh) failed.
    #[error("the new configuration is in effect, but refreshing the routes failed: {0}")]
    ReloadPartial(String),
}

fn startup<E: std::fmt::Display>(what: &'static str) -> impl Fn(E) -> ServerError {
    move |e| ServerError::Startup(format!("{what}: {e}"))
}

/// Optional overrides for embedding / tests.
#[derive(Default)]
pub struct StartOptions {
    /// Replace the default extension-point implementations (paid builds).
    pub plugins: Option<Plugins>,
    /// An additional audit sink that receives every `AuditEvent` next to the configured one
    /// (SEC-20 tests record `emit` calls through it). Added by the W2 test lane.
    pub extra_audit: Option<Arc<dyn AuditSink>>,
    /// Wraps the opened `meta.db` store before anything uses it (tests count the calls the
    /// request path makes through it).
    pub meta_layer: Option<MetaLayer>,
    /// Failure plans attached to the opened stores (tests make store operations fail on demand).
    pub faults: StartFaults,
}

/// See [`StartOptions::faults`]: a plan per store; production never sets one.
#[derive(Default)]
pub struct StartFaults {
    pub meta: Option<Arc<rt_store::Faults>>,
    pub queue: Option<Arc<rt_store::Faults>>,
    pub data: Option<Arc<rt_store::Faults>>,
}

/// See [`StartOptions::meta_layer`].
pub type MetaLayer = Box<dyn FnOnce(Arc<dyn MetaStore>) -> Arc<dyn MetaStore> + Send>;

/// Where the node listens (actual addresses after binding `:0`).
#[derive(Clone, Debug)]
pub struct ServerAddrs {
    pub public: SocketAddr,
    pub control: SocketAddr,
    pub metrics: SocketAddr,
    /// Internal listener Unix socket path, if configured as `unix:`.
    pub internal: Option<PathBuf>,
}

// ---------------------------------------------------------------------------------------------
// Small adapters
// ---------------------------------------------------------------------------------------------

/// `Calls` that exists before the scheduler does (the executors need the host handle, the
/// dispatcher needs the scheduler).
struct LateCalls {
    inner: OnceLock<Arc<CallDispatcher>>,
}

#[async_trait]
impl Calls for LateCalls {
    async fn call(&self, from: &TaskCtx, slug: &str, req: Request) -> Result<Response, CallError> {
        match self.inner.get() {
            Some(d) => d.call(from, slug, req).await,
            None => Err(CallError::Failed),
        }
    }

    async fn call_object(
        &self,
        from: &TaskCtx,
        slug: Option<&str>,
        object: &str,
        req: Request,
    ) -> Result<Response, CallError> {
        match self.inner.get() {
            Some(d) => d.call_object(from, slug, object, req).await,
            None => Err(CallError::Failed),
        }
    }

    async fn enqueue(&self, from: &TaskCtx, slug: &str, body: Bytes, o: EnqueueOpts) -> Result<MsgId, CallError> {
        match self.inner.get() {
            Some(d) => d.enqueue(from, slug, body, o).await,
            None => Err(CallError::Failed),
        }
    }

    async fn sdk_invoke(
        &self,
        from: &TaskCtx,
        sdk: &str,
        method: &str,
        args_json: &str,
    ) -> Result<String, rt_host::RpcError> {
        match self.inner.get() {
            Some(d) => d.sdk_invoke(from, sdk, method, args_json).await,
            None => Err(rt_host::RpcError::from_call(&CallError::Failed)),
        }
    }
}

/// Audit sink: `events` table (through the control plane writer), a log line, and the optional
/// extra sink.
/// The socket hub, bound once the scheduler exists (the host services are built before it).
struct LateSockets {
    inner: OnceLock<Arc<SocketHub>>,
}

impl LateSockets {
    fn hub(&self) -> &dyn rt_host::SocketHost {
        match self.inner.get() {
            Some(h) => &**h,
            None => &rt_host::NoSockets,
        }
    }
}

impl rt_host::SocketHost for LateSockets {
    fn accept(
        &self,
        ctx: &TaskCtx,
        object: &str,
        opts: rt_host::AcceptOpts,
    ) -> Result<rt_host::Accepted, rt_host::HostError> {
        self.hub().accept(ctx, object, opts)
    }
    fn apply(&self, ctx: &TaskCtx, op: rt_host::SocketOp) -> Result<u32, rt_host::HostError> {
        self.hub().apply(ctx, op)
    }
    fn list(
        &self,
        ctx: &TaskCtx,
        object: Option<&str>,
        tag: Option<&str>,
        limit: usize,
        cursor: Option<&str>,
    ) -> rt_host::SocketPage {
        self.hub().list(ctx, object, tag, limit, cursor)
    }
    fn get(&self, ctx: &TaskCtx, socket: &str) -> Option<rt_types::SocketInfo> {
        self.hub().get(ctx, socket)
    }
    fn exists(&self, ctx: &TaskCtx, socket: &str) -> bool {
        self.hub().exists(ctx, socket)
    }
    fn count(&self, ctx: &TaskCtx, object: Option<&str>, tag: Option<&str>) -> u32 {
        self.hub().count(ctx, object, tag)
    }
    fn objects(&self, ctx: &TaskCtx, prefix: &str, limit: usize, cursor: Option<&str>) -> rt_host::ObjectPage {
        self.hub().objects(ctx, prefix, limit, cursor)
    }
    fn set_tags(&self, ctx: &TaskCtx, socket: &str, tags: Vec<String>) -> Result<(), rt_host::HostError> {
        self.hub().set_tags(ctx, socket, tags)
    }
    fn attachment(&self, ctx: &TaskCtx, socket: &str) -> Result<Option<Bytes>, rt_host::HostError> {
        self.hub().attachment(ctx, socket)
    }
    fn set_attachment(&self, ctx: &TaskCtx, socket: &str, value: Option<Bytes>) -> Result<(), rt_host::HostError> {
        self.hub().set_attachment(ctx, socket, value)
    }
    fn set_auto_response(
        &self,
        ctx: &TaskCtx,
        object: &str,
        pair: Option<(String, String)>,
    ) -> Result<(), rt_host::HostError> {
        self.hub().set_auto_response(ctx, object, pair)
    }
    fn set_alarm(&self, ctx: &TaskCtx, object: &str, at: Option<i64>) {
        self.hub().set_alarm(ctx, object, at)
    }
}

struct ServerAudit {
    events: mpsc::UnboundedSender<NodeEvent>,
    base: Arc<dyn AuditSink>,
    extra: Option<Arc<dyn AuditSink>>,
}

impl AuditSink for ServerAudit {
    fn emit(&self, ev: AuditEvent) {
        let mut row = NodeEvent::new(ev.action.clone(), ev.ts_ms);
        row.tenant = ev.tenant;
        row.service = ev.service;
        row.data = serde_json::json!({"actor": ev.actor, "data": ev.data});
        let _ = self.events.send(row);
        self.base.emit(ev.clone());
        if let Some(x) = &self.extra {
            x.emit(ev);
        }
    }
}

struct Tamper(Arc<dyn ControlPlane>);

impl TamperSink for Tamper {
    fn tampered(&self, key: &PreparedKey, file: &str) {
        let mut ev = NodeEvent::new("security.artifact_tamper", chrono::Utc::now().timestamp_millis());
        ev.data = serde_json::json!({"digest": key.digest.to_string(), "file": file});
        self.0.event(ev);
    }
}

struct Sinks {
    deno: Arc<PreparedCache<DenoLoader>>,
    wasm: Arc<PreparedCache<WasmLoader>>,
}

impl BuildSink for Sinks {
    fn promote_built(&self, key: &PreparedKey) -> Result<(), BuildFailure> {
        match key.kind {
            RuntimeKind::Deno => self.deno.promote_built(key),
            RuntimeKind::Wasm => self.wasm.promote_built(key),
        }
    }

    fn build_done(&self, key: &PreparedKey, res: Result<(), BuildFailure>) {
        match key.kind {
            RuntimeKind::Deno => self.deno.on_build_done(key, res),
            RuntimeKind::Wasm => self.wasm.on_build_done(key, res),
        }
    }
}

struct BuildJob {
    service: ServiceId,
    version: VersionNo,
    digest: Digest,
    runtime: RuntimeKind,
    rebuild: bool,
}

// ---------------------------------------------------------------------------------------------
// NodeInfo
// ---------------------------------------------------------------------------------------------

struct NodeImpl {
    config: Arc<ArcSwap<Config>>,
    draining: AtomicBool,
    stalled: AtomicBool,
    last_beat_ms: AtomicI64,
    scheduler: Scheduler,
    keys: Arc<KeyCache>,
    /// The default authenticator's own key cache (None with a plugin authenticator): revocation
    /// drops its entry at once instead of waiting for the TTL (SEC-06).
    auth_cache: Option<Arc<DefaultAuthenticator>>,
    specs: Arc<Specs>,
    schedules: Arc<ScheduleEngine>,
    queue_engine: Arc<QueueEngine>,
    /// queue.db: a deleted service's messages, dead letters and one-off schedules go with it.
    queue_db: Arc<SqliteQueueStore>,
    /// streams.db: a deleted service's cursors and dead letters go with it.
    streams: Arc<dyn StreamStore>,
    sched_services: Mutex<HashMap<ScheduleId, ServiceId>>,
    build_tx: mpsc::UnboundedSender<BuildJob>,
    deno_cache: Arc<PreparedCache<DenoLoader>>,
    wasm_cache: Arc<PreparedCache<WasmLoader>>,
    deno_fp: u64,
    wasm_fp: u64,
    /// Its env cache follows env/secrets changes and service deletion.
    control: Arc<InProcControlPlane>,
    /// Watermarks follow the live configuration ([`apply_live_knobs`]).
    governor: Arc<Governor>,
    /// Serializes the configuration writers (`PUT /v1/node/config`, `SIGHUP`): each one
    /// reads, changes and stores the whole config, so two at once would lose one's change.
    config_write: Mutex<()>,
    /// Services whose `min_instances` floor the scheduler holds ([`NodeImpl::sync_floors`]).
    floored: Mutex<std::collections::HashSet<ServiceId>>,
    /// The node's WebSockets: drained with the node, closed with a deleted service.
    sockets: Arc<SocketHub>,
}

/// Apply the live-updatable knobs of `cfg` (`Config::LIVE_KEYS`) to the running node: the
/// scheduler's live knobs and admission bounds, and the governor's watermarks.
/// `sched.queue_timeout` reaches tasks through the shared config (`Specs` resolves it per task).
fn apply_live_knobs(cfg: &Config, scheduler: &Scheduler, governor: &Governor) {
    scheduler.reconfigure(LiveSchedConfig::from_config(cfg));
    scheduler.set_admission_limits(AdmissionLimits::resolve(
        cfg.sched.max_inflight,
        cfg.sched.max_queued,
        u64::from(cfg.sched.max_queued_mb) * MIB,
        governor.budget_bytes(),
    ));
    let k = cfg.node_knobs();
    governor.set_watermarks(k.memory_high.value, k.memory_critical.value);
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl NodeImpl {
    fn heartbeat(&self) {
        if !self.stalled.load(Ordering::Relaxed) {
            self.last_beat_ms.store(now_ms(), Ordering::Relaxed);
        }
    }

    fn key_of(&self, e: &RouteEntry) -> PreparedKey {
        let engine_fp = match e.runtime {
            RuntimeKind::Deno => self.deno_fp,
            RuntimeKind::Wasm => self.wasm_fp,
        };
        PreparedKey {
            digest: e.digest,
            kind: e.runtime,
            engine_fp,
        }
    }

    /// Keep the `scaling.min_instances` floor of every routable service without waiting for its
    /// first request (D-B2). Runs whenever the route snapshot is re-read (node start, a version
    /// goes live, a service or its plan changes): a service with a floor is pinned in the
    /// scheduler with the spec it would run now (so its admission state and warm target exist and
    /// follow the live version) and its artifact is loaded, or built, into T1 off the request path
    /// so the executors can pre-warm instances. A service that lost its floor, was deleted or is
    /// no longer routable is released.
    fn sync_floors(&self) {
        // The services with a floor, oldest first: when a tenant's floors are above its plan's
        // `max_warm_floor_total` (the API refuses that, but a plan downgrade leaves it), the ones
        // that fit are kept and the rest go without (`Specs::unfloored`, so their requests do not
        // bring the floor back either).
        let mut floored: Vec<(ServiceId, Arc<RouteEntry>, Arc<Service>, u32)> = Vec::new();
        for (id, svc) in self.specs.services.load().iter() {
            // the floor only ever comes from the service's own config (the plan clamps it)
            if svc.config.scaling.min_instances.unwrap_or(0) == 0 {
                continue;
            }
            let Some((entry, svc)) = self.specs.lookup(*id) else {
                continue;
            };
            let floor = self.specs.floor_of(&entry, &svc);
            if floor > 0 {
                floored.push((*id, entry, svc, floor));
            }
        }
        floored.sort_by_key(|(id, ..)| *id);
        let mut totals: std::collections::HashMap<rt_types::TenantId, u64> = std::collections::HashMap::new();
        let mut unfloored: std::collections::HashSet<ServiceId> = std::collections::HashSet::new();
        floored.retain(|(id, entry, _, floor)| {
            let cap = u64::from(self.specs.plans.plan_arc(entry.tenant).max_warm_floor_total);
            let total = totals.entry(entry.tenant).or_default();
            if cap > 0 && *total + u64::from(*floor) > cap {
                unfloored.insert(*id);
                return false;
            }
            *total += u64::from(*floor);
            true
        });
        if !unfloored.is_empty() {
            tracing::warn!(
                services = unfloored.len(),
                "warm floors above the tenant's max_warm_floor_total are not kept"
            );
        }
        self.specs.unfloored.store(Arc::new(unfloored));
        let mut now: std::collections::HashSet<ServiceId> = std::collections::HashSet::new();
        for (id, entry, svc, _) in floored {
            let spec = self.specs.spec(
                &entry,
                &svc,
                rt_types::Trigger::Http,
                rt_types::Payload::Http(rt_types::HttpRequest::get("/")),
                None,
            );
            self.scheduler.pin_service(&spec);
            let key = self.key_of(&entry);
            match entry.runtime {
                RuntimeKind::Deno => self.deno_cache.preload(&[key]),
                RuntimeKind::Wasm => self.wasm_cache.preload(&[key]),
            }
            now.insert(id);
        }
        let gone: Vec<ServiceId> = {
            let mut held = self.floored.lock();
            let gone = held.difference(&now).copied().collect();
            *held = now;
            gone
        };
        for id in gone {
            self.scheduler.release_floor(id);
        }
        // floors the scheduler holds from requests (or a missed release) of services that should
        // not hold one
        let held = self.floored.lock().clone();
        let n = self.scheduler.reconcile_floors(&held);
        if n > 0 {
            tracing::info!(released = n, "stale warm floors released"); // LCOV_EXCL_LINE: a floor outside `held` is only re-seeded by a request racing a release; no test can order that race
        }
    }
}

impl NodeInfo for NodeImpl {
    fn cache(&self) -> serde_json::Value {
        let top = |keys: Vec<PreparedKey>| keys.iter().map(|k| k.digest.to_string()).collect::<Vec<_>>();
        serde_json::json!({
            "deno": serde_json::to_value(self.deno_cache.stats()).unwrap_or_default(),
            "wasm": serde_json::to_value(self.wasm_cache.stats()).unwrap_or_default(),
            "top": { "deno": top(self.deno_cache.top_keys(10)), "wasm": top(self.wasm_cache.top_keys(10)) },
        })
    }

    fn v8_sandbox(&self) -> bool {
        false
    }

    fn healthy(&self) -> bool {
        now_ms() - self.last_beat_ms.load(Ordering::Relaxed) < 1_000
    }

    fn ready(&self) -> bool {
        !self.draining.load(Ordering::Relaxed)
    }

    fn apply_live_config(&self, update: &NodeConfigUpdate) -> Result<Vec<String>, String> {
        if let Some(p) = &update.default_profile {
            let profile: Profile = p.parse()?;
            let _writer = self.config_write.lock();
            let mut c = (**self.config.load()).clone();
            c.node.default_profile = profile;
            // CFG-05: the profile's node knobs (spin, slice, sysmon tick, watermarks, ...) apply now.
            apply_live_knobs(&c, &self.scheduler, &self.governor);
            self.config.store(Arc::new(c));
        }
        let mut pending = Vec::new();
        if update.rate_limit.is_some() {
            pending.push("rate_limit".to_string());
        }
        Ok(pending)
    }

    fn start_drain(&self) {
        self.draining.store(true, Ordering::Relaxed);
        self.scheduler.set_mode(rt_types::DegradationMode::Drain);
        // Sockets are told the node goes away (1001); new upgrades are refused.
        self.sockets.drain();
    }

    fn notify(&self, ev: ControlEvent) {
        match ev {
            ControlEvent::ScheduleUpserted(s) => {
                self.sched_services.lock().insert(s.id, s.service_id);
                if let Err(e) = self.schedules.upsert(s) {
                    tracing::warn!(error = %e, "schedule rejected by the engine");
                    // LCOV_EXCL_LINE: rt-api runs rt_triggers::validate_kind (the engine's first check) before it stores a schedule
                }
            }
            ControlEvent::ScheduleRemoved(id) => {
                self.sched_services.lock().remove(&id);
                self.schedules.remove(id);
            }
            ControlEvent::ServiceDeleted(svc) => {
                self.control.invalidate_env(svc);
                // a deleted service keeps no warm floor (`scaling.min_instances`, D-B2)
                self.scheduler.release_floor(svc);
                self.sockets.close_service(svc, None, None, 1001, "service deleted");
                let ids: Vec<ScheduleId> = self
                    .sched_services
                    .lock()
                    .iter()
                    .filter(|(_, s)| **s == svc)
                    .map(|(id, _)| *id)
                    .collect();
                for id in ids {
                    self.sched_services.lock().remove(&id);
                    self.schedules.remove(id);
                }
                // queue.db is outside meta.db's cascade (META-010).
                let q = self.queue_db.clone();
                tokio::spawn(async move { purge_queue_rows(&q, svc).await });
                let st = self.streams.clone();
                tokio::spawn(async move { purge_stream_rows(&*st, svc).await });
            }
            ControlEvent::QueueEnqueued(svc) => self.queue_engine.notify(svc),
            ControlEvent::BuildRequested {
                service,
                version,
                digest,
                runtime,
                rebuild,
            } => {
                let _ = self.build_tx.send(BuildJob {
                    service,
                    version,
                    digest,
                    runtime,
                    rebuild,
                });
            }
            ControlEvent::KeyUpserted(k) => {
                let id = k.id;
                self.keys.upsert(k);
                // A changed key (or its tenant's state) must not be served from the 1 s cache.
                if let Some(a) = &self.auth_cache {
                    a.invalidate_key(id);
                }
            }
            ControlEvent::KeyRevoked(id) => {
                self.keys.revoke(id);
                if let Some(a) = &self.auth_cache {
                    a.invalidate_key(id);
                }
            }
            // The env cache drops the bundle; the next task start reloads it (rotation is
            // visible to the next request).
            ControlEvent::SecretsChanged(svc) => self.control.invalidate_env(svc),
            ControlEvent::RoutesRefreshed => {
                // A tenant may have been disabled (the tenant table is updated before the
                // refresh): drop cached keys the key table no longer serves, so they stop
                // authenticating now rather than after the 1 s cache TTL.
                if let Some(a) = &self.auth_cache {
                    a.invalidate_where(|k| rt_sec::KeyLookup::find(&*self.keys, k.id).is_none());
                }
                self.sync_floors();
            }
            // The event engines are wired later (stream engine, async engine): nothing to wake yet.
            ControlEvent::StreamAppended(_) | ControlEvent::AsyncAccepted(_) | ControlEvent::ConsumersReset(_) => {}
        }
    }

    fn http_task(&self, entry: &RouteEntry, req: HttpRequest, trace: Option<TraceCtx>) -> Option<TaskSpec> {
        self.specs.http_task(entry, req, trace)
    }

    fn tier(&self, entry: &RouteEntry) -> &'static str {
        let key = self.key_of(entry);
        let hit = match entry.runtime {
            RuntimeKind::Deno => self.deno_cache.contains(&key),
            RuntimeKind::Wasm => self.wasm_cache.contains(&key),
        };
        if hit {
            "T1"
        } else {
            "T3"
        }
    }

    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

// ---------------------------------------------------------------------------------------------
// Running server
// ---------------------------------------------------------------------------------------------

/// One pass of a periodic job; the argument is its 1-based tick count.
type PeriodicPass = Arc<dyn Fn(u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// Run `pass` every `every` (after one `every` when `skip_first`), counting ticks from 1. Returns the
/// task and the pass itself, which tests call directly instead of waiting for the interval.
fn spawn_periodic<F, Fut>(every: Duration, skip_first: bool, pass: F) -> (JoinHandle<()>, PeriodicPass)
where
    F: Fn(u64) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let pass: PeriodicPass = Arc::new(move |n| Box::pin(pass(n)));
    let looped = pass.clone();
    let task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        if skip_first {
            tick.tick().await;
        }
        let mut n: u64 = 0;
        loop {
            tick.tick().await;
            n += 1;
            looped(n).await;
        }
    });
    (task, pass)
}

struct Inner {
    node: Arc<NodeImpl>,
    scheduler: Scheduler,
    control: Arc<InProcControlPlane>,
    logs: Arc<FileLogStore>,
    shutdown_tx: watch::Sender<bool>,
    servers: Mutex<Vec<JoinHandle<std::io::Result<()>>>>,
    background: Mutex<Vec<JoinHandle<()>>>,
    /// The passes of the periodic jobs in `background`, for [`RunningServer::run_periodic_for_test`].
    periodic: Vec<PeriodicPass>,
    api: ApiState,
    _ticker: EpochTicker,
}

/// A running node.
pub struct RunningServer {
    addrs: ServerAddrs,
    inner: Arc<Inner>,
}

impl RunningServer {
    pub fn addrs(&self) -> ServerAddrs {
        self.addrs.clone()
    }

    /// Re-read the route and service tables from `meta.db`.
    pub async fn reload(&self) -> Result<(), ServerError> {
        self.refresh_tables().await.map_err(ServerError::Startup)
    }

    async fn refresh_tables(&self) -> Result<(), String> {
        rt_api::refresh_routes(&self.inner.api).await.map_err(|e| e.message)
    }

    /// `SIGHUP` (docs/config.md): re-read the configuration and apply what can change at runtime, the
    /// tenant plans (quotas, limits, egress stance, blocking shares: every check reads the shared
    /// config) and the `Config::LIVE_KEYS` (scheduler, admission bounds, memory watermarks), then
    /// the route and service tables. A file that fails to load or validate leaves
    /// the running configuration untouched. Other changed keys need a restart and are logged.
    ///
    /// Refused (nothing changes) when a plan that tenants still use is missing from the new file:
    /// those tenants would silently fall back to the built-in `free` plan. When the new
    /// configuration is in effect but the route refresh fails, the error says so
    /// ([`ServerError::ReloadPartial`]).
    pub async fn reload_config(&self, opts: &LoadOptions) -> Result<(), ServerError> {
        let loaded = Config::load(opts)?;
        loaded.config.validate()?;
        let mut missing: Vec<String> = self
            .inner
            .api
            .tenant_table
            .iter()
            // The system tenant's `sys` plan is built in (`Plan::system`) unless configured.
            .filter(|t| !rt_types::is_system_tenant(t.id))
            .map(|t| t.plan.clone())
            .filter(|p| p != "free" && !loaded.config.plans.contains_key(p))
            .collect();
        missing.sort();
        missing.dedup();
        if !missing.is_empty() {
            return Err(ServerError::Startup(format!(
                "plans still used by tenants are missing from the new configuration: {}",
                missing.join(", ")
            )));
        }
        let node = &self.inner.node;
        {
            let _writer = node.config_write.lock();
            let current = self.inner.api.config.load_full();
            let mut next = (*current).clone();
            next.plans = loaded.config.plans.clone();
            next.apply_live_keys(&loaded.config);
            let mut rest_new = loaded.config;
            rest_new.plans = current.plans.clone();
            rest_new.apply_live_keys(&current);
            if rest_new.print().ok() != current.print().ok() {
                tracing::warn!("SIGHUP: configuration keys other than [plans] and the live keys changed; they apply after a restart");
            }
            apply_live_knobs(&next, &node.scheduler, &node.governor);
            self.inner.control.set_plans(next.plans.clone());
            self.inner.api.config.store(Arc::new(next));
        }
        tracing::info!("SIGHUP: plans reloaded");
        self.refresh_tables().await.map_err(ServerError::ReloadPartial)
    }

    /// Switch to `drain` mode: finish in-flight tasks, refuse new ones.
    pub fn drain(&self) {
        self.inner.node.start_drain();
    }

    /// Test hook (OBS-10): `true` makes the sysmon heartbeat stale so `/healthz` answers `503`;
    /// `false` resumes it. Added by the W2 test lane.
    #[doc(hidden)]
    pub fn stall_sysmon_for_test(&self, stalled: bool) {
        self.inner.node.stalled.store(stalled, Ordering::Relaxed);
        if !stalled {
            self.inner.node.heartbeat();
        }
    }

    /// Test hook: run one pass of every periodic job (heartbeat and maintenance, storage scan, KV and
    /// orphan sweeps, floor reconciliation, self-denylist refresh) as its `tick`-th, now. Passing a
    /// tick count every interval divides makes every tick-gated step of the maintenance pass run.
    #[doc(hidden)]
    pub async fn run_periodic_for_test(&self, tick: u64) {
        for pass in &self.inner.periodic {
            pass(tick).await;
        }
    }

    /// Graceful shutdown: drain, wait for tasks up to a grace period, flush logs, close stores.
    pub async fn shutdown(self) {
        let inner = self.inner;
        inner.node.start_drain();
        // WebSockets were told the node goes away (1001): give their close handshakes and the
        // event tasks they started `ws.drain_grace`.
        let grace = Instant::now() + inner.api.config.load().ws.drain_grace;
        while inner.api.sockets.stats().sockets > 0 && Instant::now() < grace {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        // Let in-flight tasks finish (new ones are refused with `503 draining`).
        let deadline = Instant::now() + SHUTDOWN_GRACE;
        loop {
            if inner.scheduler.snapshot().inflight == 0 || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        inner.scheduler.shutdown(Duration::from_secs(1)).await;
        // Stop accepting, let open connections finish.
        let _ = inner.shutdown_tx.send(true);
        let servers: Vec<_> = std::mem::take(&mut *inner.servers.lock());
        for s in servers {
            let _ = tokio::time::timeout(Duration::from_secs(10), s).await;
        }
        for b in inner.background.lock().drain(..) {
            b.abort();
        }
        inner.control.flush_usage().await;
        save_cache_freq(&*inner.api.meta, &inner.node.deno_cache, &inner.node.wasm_cache).await;
        if let Err(e) = inner.logs.flush() {
            tracing::warn!(error = %e, "log flush failed"); // LCOV_EXCL_LINE: a log file that cannot be flushed at shutdown
        }
    }

    /// Resolves when the node should stop (SIGTERM/SIGINT). A `SIGHUP` in the meantime is
    /// consumed without stopping (use [`NodeSignals`] to act on it).
    pub async fn wait_for_signal() {
        match NodeSignals::new() {
            Some(mut s) => while s.next().await != NodeSignal::Stop {},
            None => {
                let _ = tokio::signal::ctrl_c().await; // LCOV_EXCL_LINE: unix signal handlers can always be installed
            }
        }
    }
}

/// A process signal the node acts on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeSignal {
    /// `SIGTERM` / `SIGINT`: drain and stop.
    Stop,
    /// `SIGHUP`: reload the configuration ([`RunningServer::reload_config`]).
    Reload,
}

/// The node's signal handlers. Registering `SIGHUP` also replaces its default action, which
/// would otherwise terminate the process.
pub struct NodeSignals {
    term: tokio::signal::unix::Signal,
    int: tokio::signal::unix::Signal,
    hup: tokio::signal::unix::Signal,
}

impl NodeSignals {
    /// `None` if the handlers cannot be installed.
    pub fn new() -> Option<Self> {
        use tokio::signal::unix::{signal, SignalKind};
        Some(Self {
            term: signal(SignalKind::terminate()).ok()?,
            int: signal(SignalKind::interrupt()).ok()?,
            hup: signal(SignalKind::hangup()).ok()?,
        })
    }

    pub async fn next(&mut self) -> NodeSignal {
        tokio::select! {
            _ = self.term.recv() => NodeSignal::Stop,
            _ = self.int.recv() => NodeSignal::Stop,
            _ = self.hup.recv() => NodeSignal::Reload,
        }
    }
}

/// Build and start a node (`--role=all`).
pub async fn start(config: Config, secrets: NodeSecrets) -> Result<RunningServer, ServerError> {
    start_with(config, secrets, StartOptions::default()).await
}

fn load_or_create_master_key(dir: &Path) -> Result<MasterKey, ServerError> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = dir.join("master.key");
    if let Ok(text) = std::fs::read_to_string(&path) {
        if let Some(problem) = rt_sec::key_file_mode_problem(&path) {
            tracing::warn!("{problem}");
        }
        return MasterKey::from_hex(text.trim()).map_err(startup("invalid master.key"));
    }
    let mut raw = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut raw);
    let hexed = hex::encode(raw);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    std::io::Write::write_all(&mut f, hexed.as_bytes())?;
    tracing::warn!("no master key configured: generated one in {}", path.display());
    MasterKey::from_hex(&hexed).map_err(startup("generated master key"))
}

async fn bind_tcp(
    addr: &str,
    router: axum::Router,
    limits: &ServeLimits,
    shutdown_rx: &watch::Receiver<bool>,
    servers: &mut Vec<JoinHandle<std::io::Result<()>>>,
) -> Result<SocketAddr, ServerError> {
    // `ingress.backlog` instead of mio's 128, so connection bursts are not reset.
    let listener = rt_api::bind_tcp_listener(addr, limits.backlog)
        .await
        .map_err(|e| ServerError::Startup(format!("bind {addr}: {e}")))?;
    let local = listener.local_addr()?;
    let mut rx = shutdown_rx.clone();
    let stop = async move {
        let _ = rx.wait_for(|v| *v).await;
    };
    servers.push(tokio::spawn(rt_api::serve_tcp_with(
        listener,
        router,
        limits.clone(),
        stop,
    )));
    Ok(local)
}

/// Bind the internal (trusted) Unix socket so that it is never reachable by other users, not
/// even briefly: bind under a temporary name, restrict it to the owner (0600), then rename it
/// over `path`. Any failure stops startup rather than leaving an open trusted socket.
fn bind_private_unix(path: &Path) -> Result<tokio::net::UnixListener, ServerError> {
    use std::os::unix::fs::PermissionsExt;
    let fail = |what: &str, e: std::io::Error| ServerError::Startup(format!("{what} {}: {e}", path.display()));
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".{}.tmp", std::process::id()));
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let listener = tokio::net::UnixListener::bind(&tmp).map_err(|e| fail("bind", e))?;
    let secured = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| fail("chmod", e))
        .and_then(|()| std::fs::rename(&tmp, path).map_err(|e| fail("rename", e)));
    if let Err(e) = secured {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(listener)
}

/// Free bytes available to this process on the filesystem holding `path` (`statvfs`).
fn free_disk_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statvfs` is a plain C struct of integers, for which all-zero bytes are a valid value.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path that outlives the call, and `st` is a valid,
    // exclusively borrowed out-pointer.
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return None;
    }
    #[allow(clippy::unnecessary_cast)] // the field widths differ between platforms
    Some((st.f_bavail as u64).saturating_mul(st.f_frsize as u64))
}

/// Hands one process RSS sample to the governor; a platform that reports none leaves it as it was.
fn observe_rss_sample(governor: &Governor, sample: Option<u64>) {
    if let Some(rss) = sample {
        governor.observe_rss(rss);
    }
}

/// Resident set size of this process (`VmRSS` from `/proc/self/status`; `None` elsewhere).
#[cfg(not(target_os = "linux"))]
fn process_rss_bytes() -> Option<u64> {
    None
}

/// Resident set size of this process (`VmRSS` from `/proc/self/status`; `None` elsewhere).
#[cfg(target_os = "linux")]
fn process_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))?
        .trim()
        .trim_end_matches("kB")
        .trim();
    kb.parse::<u64>().ok().map(|kb| kb * 1024)
}

/// `rt_mem_*` gauges from a governor snapshot.
fn publish_mem(metrics: &Metrics, s: &rt_mem::MemStats) {
    metrics.mem_budget_bytes.set(s.budget_bytes as i64);
    for (cat, bytes) in &s.by_category {
        metrics
            .mem_committed_bytes
            .with_label_values(&[cat.as_str()])
            .set(*bytes as i64);
    }
    metrics.mem_rss_bytes.set(s.rss_bytes as i64);
    metrics.mem_fragmentation_ratio.set(s.fragmentation_ratio);
    metrics.mem_pressure_level.set(i64::from(s.pressure.level()));
}

fn parse_nets(list: &[String]) -> Vec<ipnet::IpNet> {
    list.iter().filter_map(|c| c.parse().ok()).collect()
}

/// The local secrets backend: seals under `master`, also opens values of `previous` (SEC-53).
pub(crate) fn local_secrets(master: &MasterKey, previous: Option<&MasterKey>) -> LocalSecrets {
    match previous {
        Some(prev) => LocalSecrets::new(master).with_previous(prev),
        None => LocalSecrets::new(master),
    }
}

/// Daily quota counters survive a restart (SEC-54): today's persisted `usage` of every tenant
/// seeds the in-memory enforcer before the node serves.
async fn seed_daily_usage(meta: &Arc<dyn MetaStore>, tenants: &DashMap<TenantId, Tenant>, quotas: &LocalQuotaEnforcer) {
    let now = chrono::Utc::now();
    let day = now.format("%Y-%m-%d").to_string();
    let ids: Vec<TenantId> = tenants.iter().map(|t| t.id).collect();
    for tenant in ids {
        match meta.get_usage(tenant, &day, &day).await {
            Ok(rows) => {
                let invocations = rows.iter().fold(0u64, |a, r| a.saturating_add(r.invocations));
                let egress = rows.iter().fold(0u64, |a, r| a.saturating_add(r.egress_bytes));
                if invocations > 0 || egress > 0 {
                    quotas.seed_daily(tenant, now.timestamp_millis(), invocations, egress);
                }
            }
            Err(e) => {
                tracing::warn!(%tenant, error = %e, "usage seed failed: the tenant's daily counters start at zero")
            }
        }
    }
}

async fn load_tables(
    meta: &Arc<dyn MetaStore>,
    tenants: &DashMap<TenantId, Tenant>,
    keys: &KeyCache,
) -> Result<(), ServerError> {
    let mut cursor = None;
    loop {
        let page = meta
            .list_tenants(PageReq { limit: 200, cursor })
            .await
            .map_err(startup("tenants"))?;
        for t in page.items {
            for k in meta.list_keys(t.id).await.map_err(startup("keys"))? {
                keys.upsert(k);
            }
            tenants.insert(t.id, t);
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    Ok(())
}

pub async fn start_with(
    config: Config,
    secrets: NodeSecrets,
    opts: StartOptions,
) -> Result<RunningServer, ServerError> {
    config.validate()?;
    let config = Arc::new(ArcSwap::from_pointee(config));
    let cfg = config.load_full();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock::new());

    // 1. sandbox flags, secrets, data dir
    // The prebuilt rusty_v8 has no sandbox (`NodeInfo::v8_sandbox` says so too).
    if cfg.security.require_v8_sandbox {
        return Err(ServerError::V8SandboxRequired);
    }
    tracing::warn!("this build has no V8 sandbox (prebuilt rusty_v8): v8_sandbox=false");
    let data_dir = PathBuf::from(&cfg.node.data_dir);
    for d in ["artifacts", "cache", "services", "blobs", "logs", "deps"] {
        std::fs::create_dir_all(data_dir.join(d))?;
    }
    let master = match &secrets.master_key {
        Some(s) => MasterKey::from_hex(s.expose()).map_err(startup("invalid master key"))?,
        None => load_or_create_master_key(&data_dir)?,
    };
    // Key rotation (SEC-53): the previous master key only opens secrets and verifies API keys.
    let master_previous = match &secrets.master_key_previous {
        Some(s) => Some(MasterKey::from_hex(s.expose()).map_err(startup("invalid RT_MASTER_KEY_PREVIOUS"))?),
        None => None,
    };
    let root_token = secrets
        .root_token
        .as_ref()
        .map(|s| s.expose().to_string())
        .filter(|t| !t.is_empty());
    if let Some(t) = &root_token {
        rt_sec::check_root_token(t, &cfg.listeners.control).map_err(ServerError::Startup)?;
    }
    if root_token.is_none() {
        tracing::warn!(
            "no root token configured (RT_ROOT_TOKEN): the control API is {}",
            NO_ROOT_TOKEN_CONTROL
        );
        // A debug build without a token answers every control call as the operator: only ever on
        // a loopback address, and loudly (B5.2, SEC-42). Custom authenticators decide themselves.
        if rt_sec::control_open_without_token(false) && opts.plugins.is_none() {
            rt_sec::check_open_control_bind(&cfg.listeners.control).map_err(ServerError::Startup)?;
            eprintln!(
                "\n{bar}\n  CONTROL API OPEN: debug build started without RT_ROOT_TOKEN.\n  Every caller of {addr} acts as the operator (all tenants, all keys).\n  Set RT_ROOT_TOKEN before this node is reachable by anyone else.\n{bar}\n",
                bar = "=".repeat(78),
                addr = cfg.listeners.control
            );
        }
    }
    // A TCP internal listener cannot check who connects (no peer credentials): every local user
    // would get trusted invokes and `x-rt-invoker`. Explicit opt-in only (SEC-45).
    if let ListenAddr::Tcp(addr) = ListenAddr::parse(&cfg.listeners.internal) {
        if !cfg.listeners.allow_tcp_internal {
            return Err(ServerError::Startup(format!(
                "listeners.internal = {addr}: a TCP internal listener trusts every local user; use a unix: socket \
                 or set listeners.allow_tcp_internal = true"
            )));
        }
    }
    if cfg.security.seccomp {
        let hc = rt_sec::HardeningConfig {
            seccomp: false,
            landlock_rw: Vec::new(),
            landlock_ro: Vec::new(),
        };
        if let Err(e) = rt_sec::harden_process(&hc) {
            tracing::warn!(error = %e, "process hardening incomplete"); // LCOV_EXCL_LINE: prctl/setrlimit failing on a supported platform
        }
    }

    // 2. metrics, memory, logs, stats
    let metrics = Metrics::new(MetricsOptions {
        per_service: cfg.metrics.per_service,
        per_service_top_n: cfg.metrics.per_service_top_n as usize,
    })
    .map_err(startup("metrics"))?;
    let knobs = cfg.node_knobs();
    let budget = rt_mem::detect_budget(cfg.memory.budget_mb);
    budget.check().map_err(ServerError::Startup)?;
    let governor = Governor::new(GovernorConfig {
        budget_bytes: budget.bytes,
        high: knobs.memory_high.value,
        critical: knobs.memory_critical.value,
        critical_exit_hold: Duration::from_secs(5),
    });
    let mem: Arc<dyn MemoryGovernor> = governor.clone();
    let log_store = FileLogStore::open(LogStoreConfig {
        dir: data_dir.join("logs"),
        ring_lines: cfg.logs.ring_lines as usize,
        retention_days: cfg.logs.retention_days,
        max_bytes_per_service: cfg.logs.max_mb_per_service * MIB,
        rate_per_sec: 100,
        burst: 1000,
        stdout: cfg.logs.stdout,
        flush_interval: rt_obs::log::LOG_FLUSH_INTERVAL,
        fsync: false,
    })
    .map_err(startup("log store"))?;
    log_store.attach_metrics(metrics.clone());
    let stats = HdrStatsStore::new(Arc::new(now_ms));

    // 3. stores
    let meta_db = SqliteMetaStore::open(&data_dir.join("meta.db")).map_err(startup("meta.db"))?;
    if let Some(f) = &opts.faults.meta {
        meta_db.inject_faults(f.clone());
    }
    let meta: Arc<dyn MetaStore> = meta_db;
    // queue.db also holds the one-off schedules of std:schedule (its `delayed` table).
    let queue_db = SqliteQueueStore::open(
        &data_dir.join("queue.db"),
        cfg.queue.max_attempts.min(u32::from(u16::MAX)) as u16,
    )
    .map_err(startup("queue.db"))?;
    if let Some(f) = &opts.faults.queue {
        queue_db.inject_faults(f.clone());
    }
    let queue: Arc<dyn QueueStore> = queue_db.clone();
    let streams: Arc<dyn StreamStore> =
        SqliteStreamStore::open(&data_dir.join("streams.db")).map_err(startup("streams.db"))?;
    let artifacts: Arc<dyn ArtifactStore> =
        LocalArtifactStore::open(&data_dir.join("artifacts")).map_err(startup("artifact store"))?;
    let meta = match opts.meta_layer {
        Some(layer) => layer(meta),
        None => meta,
    };

    // 4. security services and tables. The system tenant owns the operator SDK services.
    meta.ensure_tenant(
        rt_types::system_tenant(),
        rt_types::SYSTEM_TENANT_NAME,
        rt_types::SYSTEM_PLAN,
    )
    .await
    .map_err(startup("system tenant"))?;
    let tenant_table: Arc<DashMap<TenantId, Tenant>> = Arc::new(DashMap::new());
    let service_table: Arc<ArcSwap<HashMap<ServiceId, Arc<Service>>>> = Arc::new(ArcSwap::from_pointee(HashMap::new()));
    let keys = Arc::new(KeyCache::with_tenants(tenant_table.clone()));
    load_tables(&meta, &tenant_table, &keys).await?;
    let plans = Arc::new(TenantPlans::new(config.clone(), tenant_table.clone()));
    let local_quotas = Arc::new(LocalQuotaEnforcer::new(plans.clone()));
    seed_daily_usage(&meta, &tenant_table, &local_quotas).await;
    let local_quotas: Arc<dyn QuotaEnforcer> = local_quotas;
    let hasher = match &master_previous {
        Some(prev) => KeyHasher::new(&master).with_previous(prev),
        None => KeyHasher::new(&master),
    };
    let mut auth_cache: Option<Arc<DefaultAuthenticator>> = None;
    let (secrets_backend, authenticator, audit_base): (
        Arc<dyn SecretsBackend>,
        Arc<dyn Authenticator>,
        Arc<dyn AuditSink>,
    ) = match opts.plugins {
        Some(p) => (p.secrets, p.authenticator, p.audit),
        None => {
            let default_auth = Arc::new(DefaultAuthenticator::new(
                root_token,
                hasher.clone(),
                keys.clone(),
                Arc::new(SystemWall),
            ));
            auth_cache = Some(default_auth.clone());
            (
                Arc::new(local_secrets(&master, master_previous.as_ref())),
                default_auth,
                Arc::new(TracingAuditSink),
            )
        }
    };

    // 5. control plane, host services
    let routes = RouteTable::new(Some(cfg.routing.base_domain.clone()).filter(|d| !d.is_empty()));
    let control = Arc::new(
        InProcControlPlane::new(meta.clone(), secrets_backend.clone(), routes.clone())
            .with_plans(cfg.plans.clone())
            .with_auto_rollback(cfg.deploy.auto_rollback),
    );
    let control_dyn: Arc<dyn ControlPlane> = control.clone();
    let quotas: Arc<dyn QuotaEnforcer> = Arc::new(MeteredQuotas::new(local_quotas, control.clone()));

    // Plan enforcement for the host and data layers (egress stance and quota, storage quota,
    // per-service database ceiling) and the per-tenant fair blocking pool for service data.
    let policy = Arc::new(PlanPolicy::new(
        plans.clone(),
        quotas.clone(),
        service_table.clone(),
        &data_dir,
    ));
    let blocking = FairBlockingPool::new(FairPoolConfig {
        threads: cfg.blocking.effective_threads(),
        default_share: rt_types::Plan::default().blocking_share,
    });
    for t in tenant_table.iter() {
        blocking.set_tenant_share(t.id, plans.plan_for(t.id).blocking_share);
    }
    let mut dc = DataConfig::new(&data_dir);
    dc.max_open_dbs = cfg.storage.max_open_dbs as usize;
    dc.readers_per_db = cfg.storage.readers_per_db as usize;
    dc.sql_cache_bytes = cfg.storage.sql_cache_mb.saturating_mul(MIB);
    dc.sql_budget = Duration::from_millis(cfg.storage.sql_budget_ms);
    dc.runner = blocking.clone();
    dc.directory = Some(policy.clone());
    // Concrete handle kept for the maintenance loop (expired-KV sweep).
    let local_data = LocalServiceData::open(dc).map_err(startup("service data"))?;
    if let Some(f) = &opts.faults.data {
        local_data.inject_faults(f.clone());
    }
    let data: Arc<dyn ServiceDataStore> = local_data.clone();

    let audit: Arc<dyn AuditSink> = Arc::new(ServerAudit {
        events: control.event_sender(),
        base: audit_base,
        extra: opts.extra_audit,
    });

    // The node's own endpoints (bindings, S3, OTLP) are resolved before anything is served and
    // every 5 min after (background tasks), on a blocking thread (SEC-51).
    let denylist = Arc::new(SelfDenylist::new(
        rt_host::self_endpoint_hosts(&cfg),
        parse_nets(&cfg.egress.denied_cidrs),
    ));
    {
        let d = denylist.clone();
        let _ = tokio::task::spawn_blocking(move || rt_sec::EgressDenylist::refresh(&*d)).await;
    }
    let guard_cfg = EgressGuardConfig {
        denied_ports: cfg.egress.denied_ports.clone(),
        denied_hosts: cfg.egress.denied_hosts.clone(),
        allow_https_downgrade: cfg.egress.allow_https_downgrade,
        ..EgressGuardConfig::default()
    };
    let egress: Arc<dyn EgressGuard> =
        Arc::new(GuardedEgress::new(guard_cfg.clone(), denylist.clone()).with_metrics(metrics.clone()));
    let trusted_egress: Arc<dyn EgressGuard> =
        Arc::new(GuardedEgress::for_trusted_endpoints(guard_cfg, denylist.clone()).with_metrics(metrics.clone()));
    let plan_of = {
        let tenants = tenant_table.clone();
        Arc::new(move |t: TenantId| {
            tenants
                .get(&t)
                .map(|t| t.plan.clone())
                .unwrap_or_else(|| "free".to_string())
        })
    };
    let platform = Arc::new(ConfigBindings::new(cfg.binding.clone(), plan_of, trusted_egress));
    let late_calls = Arc::new(LateCalls { inner: OnceLock::new() });
    let late_sockets = Arc::new(LateSockets { inner: OnceLock::new() });
    let host = HostServices::new(HostDeps {
        data: data.clone(),
        queue: queue.clone(),
        delayed: queue_db.clone(),
        egress: egress.clone(),
        denylist: denylist.clone(),
        calls: late_calls.clone(),
        platform,
        logs: log_store.clone(),
        control: control_dyn.clone(),
        mem: mem.clone(),
        sockets: late_sockets.clone(),
        policy: Some(policy.clone()),
    });

    // 6. engines and caches
    let dcfg = DenoExecutorConfig::from_config(&cfg);
    rt_exec_deno::init_v8_once(&dcfg);
    let deno_fp = rt_exec_deno::engine_fingerprint(&dcfg.v8_flags);
    let max_memory = u64::from(
        cfg.plans
            .values()
            .map(|p| p.limits_max.memory_mb)
            .max()
            .unwrap_or(128)
            .max(128),
    ) * MIB;
    let sizing = PoolSizing::from_budget(
        budget.bytes / 4,
        max_memory,
        knobs.wasm_linear_memory_keep_resident.value,
    )
    .with_warm_slots(knobs.wasm_max_unused_warm_slots.value);
    let engine = build_engine(&WasmEngineConfig {
        pool: sizing,
        fuel: cfg.wasm.fuel,
    })
    .map_err(startup("wasmtime engine"))?;
    // Idle (reusable / warm) WASM instances of all Machines hold at most half the pool's slots.
    let idle_slots = IdleSlots::for_pool(&sizing);
    let ticker = EpochTicker::spawn(engine.clone(), EPOCH_TICK);
    let wasm_fp = rt_exec_wasm::engine_fingerprint(&engine);
    publish_pool_slots(&metrics, &sizing);

    let t2 = Arc::new(T2Store::new(
        data_dir.join("cache"),
        Arc::new(HkdfArtifactMac::new(&master)) as Arc<dyn ArtifactMac>,
        Arc::new(Tamper(control_dyn.clone())),
    ));
    {
        // T2 layout v2 (SEC-55): drop the legacy layout and entries of another MAC key before the
        // first read, without tamper events; they are rebuilt on demand.
        let t2 = t2.clone();
        match tokio::task::spawn_blocking(move || t2.prepare()).await {
            Ok(Ok(r)) if r != rt_cache::T2Prepared::default() => tracing::info!(
                legacy_dirs = r.legacy_dirs_removed,
                stale_key_dirs = r.stale_key_dirs_removed,
                "T2 cache migrated to layout v2; entries are rebuilt on demand"
            ),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "T2 cache migration incomplete"), // LCOV_EXCL_LINE: the cache directory failing while the node starts
            Err(e) => tracing::warn!(error = %e, "T2 cache migration task failed"), // LCOV_EXCL_LINE: the blocking task panicking
        }
    }
    let build_spec: Arc<dyn Fn(&PreparedKey) -> TaskSpec + Send + Sync> = Arc::new(|key: &PreparedKey| {
        TaskSpec::build(
            ServiceRef {
                tenant: TenantId::from_u128(0),
                service: ServiceId::from_u128(0),
                version: VersionNo(0),
                digest: key.digest,
            },
            *key,
        )
    });
    let settings = CacheSettings {
        t1_bytes: (budget.bytes as f64 * knobs.cache_t1_share.value) as u64,
        idle_ttl: cfg
            .cache
            .idle_ttl
            .unwrap_or_else(|| rt_config::profile::service_knobs(cfg.node.default_profile).cache_idle_ttl),
        t2_max_bytes: cfg.cache.t2_max_mb * MIB,
        disk_low_free: cfg.disk.low_free_mb * MIB,
        disk_high_free: cfg.disk.high_free_mb * MIB,
    };
    let deno_cache = PreparedCache::new(
        Arc::new(DenoLoader::new(t2.clone(), build_spec.clone())),
        settings.clone(),
        Box::new(WTinyLfu::new(4096)),
        mem.clone(),
        Some(metrics.clone()),
    );
    deno_cache.attach_t2(t2.clone());
    let wasm_cache = PreparedCache::new(
        Arc::new(WasmLoader::new(engine.clone(), t2.clone(), build_spec.clone())),
        settings,
        Box::new(WTinyLfu::new(4096)),
        mem.clone(),
        Some(metrics.clone()),
    );
    wasm_cache.attach_t2(t2.clone());

    // 7. builder
    // Operator SDK imports build into shims of the published contracts (never SDK code).
    let sdk_catalog: Arc<dyn rt_build::SdkCatalog> = Arc::new(crate::specs::RouteSdkCatalog { routes: routes.clone() });
    let builder = Arc::new(DefaultBuilder::new(UploadLimits::default()).with_sdk_catalog(sdk_catalog.clone()));
    let fetcher = Arc::new(
        GuardedFetcher::new(
            egress.clone(),
            FetcherConfig {
                allowed_hosts: cfg.build.allowed_hosts.clone(),
                timeout: cfg.build.fetch_timeout,
                max_module_bytes: 10 * MIB,
                max_total_bytes: u64::from(cfg.build.max_deps_mb) * MIB,
            },
        )
        .with_vendor_store(artifacts.clone()),
    );
    let npm_cdn = url::Url::parse(&cfg.build.npm_cdn).map_err(startup("build.npm_cdn"))?;
    let resolver = Arc::new(DefaultResolver::new(ResolverConfig {
        npm_cdn,
        allowed_hosts: cfg.build.allowed_hosts.clone(),
        import_map: BTreeMap::new(),
    }));
    let build_deps = Arc::new(BuildDeps {
        builder: builder.clone(),
        artifacts: artifacts.clone(),
        fetcher,
        resolver,
        code_cache: Some(Arc::new(V8CodeCacheProducer::new(deno_fp))),
        precompiler: Some(Arc::new(WasmPrecompiler::new(engine.clone(), wasm_fp))),
        t2: t2.clone(),
        logs: log_store.clone(),
        control: control_dyn.clone(),
        sink: Arc::new(Sinks {
            deno: deno_cache.clone(),
            wasm: wasm_cache.clone(),
        }),
        metrics: metrics.clone(),
        timeout_deno: cfg.build.timeout,
        timeout_wasm: cfg.build.wasm_timeout,
        meta: Some(meta.clone()),
        sdks: Some(sdk_catalog.clone()),
    });

    // 8. scheduler
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(2);
    let sched_cfg = SchedConfig::from_config(&cfg, cpus);
    // Warm-capacity targets the autoscaler publishes (D-A3/D-A4); both executors read this same
    // `Arc` (`Scheduler::warm_targets` returns it too).
    let warm_targets = WarmTargets::new();
    let (host_d, metrics_d) = (host.clone(), metrics.clone());
    let dcfg_d = dcfg.clone().with_warm_targets(warm_targets.clone());
    let (host_w, engine_w, warm_w) = (host.clone(), engine.clone(), warm_targets.clone());
    let (slots_w, metrics_w) = (idle_slots.clone(), metrics.clone());
    let reuse_max_idle = cfg.wasm.reuse_max_idle as usize;
    let bd = build_deps.clone();
    let mut sb = Scheduler::builder(sched_cfg, clock.clone(), mem.clone())
        .warm_targets(warm_targets.clone())
        .executor(
            move || DenoExecutor::new(dcfg_d.clone(), host_d.clone()).with_metrics(metrics_d.clone()),
            deno_cache.clone(),
            deno_fp,
        )
        .executor(
            move || {
                WasmExecutor::new(engine_w.clone(), host_w.clone())
                    .with_reuse_max_idle(reuse_max_idle)
                    .with_warm_targets(warm_w.clone())
                    .with_idle_slots(slots_w.clone())
                    .with_metrics(metrics_w.clone())
            },
            wasm_cache.clone(),
            wasm_fp,
        )
        .executor(move || BuildExecutor::new(bd.clone()), Arc::new(UnitSource), 0)
        .stats(stats.clone())
        .metrics(metrics.clone())
        .log_sink(log_store.clone());
    if let Some(ms) = std::env::var("RT_SCHEDTRACE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    {
        sb = sb.schedtrace(Duration::from_millis(ms), Arc::new(|line: &str| eprintln!("{line}")));
    }
    let scheduler = sb.build().map_err(startup("scheduler"))?;
    deno_cache.bind_scheduler(scheduler.clone());
    wasm_cache.bind_scheduler(scheduler.clone());

    // 9. triggers
    let specs = Arc::new(Specs {
        config: config.clone(),
        plans: plans.clone(),
        routes: routes.clone(),
        services: service_table.clone(),
        unfloored: ArcSwap::default(),
    });
    let persist_next_due: rt_triggers::PersistNextDue = {
        let meta = meta.clone();
        Arc::new(move |id: ScheduleId, next_ms: i64| {
            let meta = meta.clone();
            tokio::spawn(async move {
                if let Err(e) = meta.set_schedule_next_due(id, Some(next_ms)).await {
                    tracing::debug!(schedule = %id, error = %e, "next due time not persisted");
                }
            });
        })
    };
    let schedule_engine = Arc::new(
        ScheduleEngine::new(clock.clone(), scheduler.clone(), specs.clone())
            .with_metrics(metrics.clone())
            .with_persist(persist_next_due)
            // `plan.allowed_triggers` at fire time too (in-memory plan cache, never meta.db).
            .with_plans(plans.clone()),
    );
    let queue_engine = Arc::new(
        QueueEngine::new(
            QueueEngineConfig {
                batch_size: cfg.queue.batch_size as usize,
                max_attempts: cfg.queue.max_attempts.min(u32::from(u16::MAX)) as u16,
                max_concurrency: cfg.queue.max_concurrency as usize,
            },
            queue.clone(),
            scheduler.clone(),
            clock.clone(),
            specs.clone(),
        )
        .with_metrics(metrics.clone()),
    );
    // One-off schedules of std:schedule: due ones are delivered by this engine (background below).
    let delayed_engine = Arc::new(DelayedEngine::new(
        DelayedEngineConfig::default(),
        queue_db.clone(),
        scheduler.clone(),
        clock.clone(),
        specs.clone(),
    ));
    let notifier = {
        let qe = queue_engine.clone();
        Arc::new(move |s: ServiceId| qe.notify(s))
    };
    // WebSockets: event tasks go through the scheduler; each counts as an invocation of
    // its service (1 event = 1 task).
    let submit: rt_triggers::Submitter = {
        let (scheduler, quotas) = (scheduler.clone(), quotas.clone());
        Arc::new(move |spec: TaskSpec| {
            let (tenant, service) = (spec.service.tenant, spec.service.service);
            let mut handle = scheduler.submit(spec)?;
            let quotas = quotas.clone();
            Ok(Box::pin(async move {
                let outcome = (&mut handle).await;
                if outcome.is_ok() || matches!(outcome, Outcome::GuestError | Outcome::Killed(_)) {
                    let cpu_ms = handle.timings().cpu.as_millis() as u64;
                    quotas.record(
                        tenant,
                        service,
                        UsageDelta {
                            invocations: 1,
                            cpu_ms,
                            ..UsageDelta::default()
                        },
                    );
                }
                outcome
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Outcome> + Send>>)
        })
    };
    let socket_tasks = Arc::new(crate::specs::SocketTasks {
        specs: specs.clone(),
        logs: log_store.clone(),
        data: data.clone(),
    });
    let ws = &cfg.ws;
    let hub_cfg = HubConfig {
        max_sockets: ws.max_sockets.max(1),
        ping_interval: ws.ping_interval,
        send_buffer_bytes: u64::from(ws.send_buffer_kb.max(1)) * 1024,
        inbox_max: ws.inbox_max.max(1) as usize,
        max_event_attempts: ws.max_event_attempts.max(1),
        max_alarm_attempts: ws.max_alarm_attempts.max(1),
        msgs_per_sec: ws.msgs_per_sec.max(1),
        msgs_burst: ws.msgs_burst.max(1),
        flood_close_after: ws.flood_close_after,
        ..HubConfig::default()
    };
    let sockets = SocketHub::new(hub_cfg, submit, socket_tasks, tokio::runtime::Handle::current());
    sockets.set_metrics(metrics.clone());
    let _ = late_sockets.inner.set(sockets.clone());
    let sdk_usage = quotas.clone();
    let dispatcher = CallDispatcher::new(
        routes.clone(),
        scheduler.clone(),
        queue.clone(),
        plans.clone(),
        specs.clone(),
    )
    .with_clock(clock.clone())
    .with_enqueue_notifier(notifier)
    .with_usage(Arc::new(move |tenant, service, delta| {
        sdk_usage.record(tenant, service, delta)
    }))
    .with_sockets(sockets.clone());
    let _ = late_calls.inner.set(Arc::new(dispatcher));

    let (build_tx, mut build_rx) = mpsc::unbounded_channel::<BuildJob>();
    let node = Arc::new(NodeImpl {
        config: config.clone(),
        draining: AtomicBool::new(false),
        stalled: AtomicBool::new(false),
        last_beat_ms: AtomicI64::new(now_ms()),
        scheduler: scheduler.clone(),
        keys: keys.clone(),
        auth_cache,
        specs: specs.clone(),
        schedules: schedule_engine.clone(),
        queue_engine: queue_engine.clone(),
        queue_db: queue_db.clone(),
        streams: streams.clone(),
        sched_services: Mutex::new(HashMap::new()),
        build_tx,
        deno_cache: deno_cache.clone(),
        wasm_cache: wasm_cache.clone(),
        deno_fp,
        wasm_fp,
        control: control.clone(),
        governor: governor.clone(),
        config_write: Mutex::new(()),
        floored: Mutex::new(std::collections::HashSet::new()),
        sockets: sockets.clone(),
    });

    // 10. API state and routers
    let state = ApiState {
        config: config.clone(),
        meta: meta.clone(),
        artifacts: artifacts.clone(),
        queue: queue.clone(),
        delayed: queue_db.clone(),
        data: data.clone(),
        streams: streams.clone(),
        invocations: queue_db.clone(),
        scheduler: scheduler.clone(),
        routes: routes.clone(),
        auth: authenticator,
        quotas,
        plans: plans.clone(),
        audit,
        secrets: secrets_backend,
        hasher,
        validator: builder.clone(),
        logs: log_store.clone() as Arc<dyn LogStore>,
        stats: stats.clone(),
        metrics: metrics.clone(),
        mem: mem.clone(),
        clock: clock.clone(),
        node: node.clone(),
        idem: Arc::new(IdemStore::default()),
        sse: Arc::new(SseLimiter::default()),
        buckets: Arc::new(IngressBuckets::default()),
        service_table: service_table.clone(),
        tenant_table: tenant_table.clone(),
        ingress: Arc::new(IngressCache::default()),
        sockets: sockets.clone(),
        uploads: Arc::new(tokio::sync::Semaphore::new(rt_api::MAX_CONCURRENT_UPLOADS)),
    };
    rt_api::refresh_routes(&state)
        .await
        .map_err(|e| ServerError::Startup(e.message))?;

    // Schedules persisted in meta.db come back (missed ticks are not backfilled, TRG-08).
    for sch in meta.all_schedules().await.map_err(startup("schedules"))? {
        node.sched_services.lock().insert(sch.id, sch.service_id);
        if let Err(e) = schedule_engine.upsert(sch) {
            tracing::warn!(error = %e, "persisted schedule rejected"); // LCOV_EXCL_LINE: rt-api ran the same validate_kind when the schedule was stored
        }
    }

    // Cache frequencies of the previous run (CCH-11), then the startup preload (CCH-10): per
    // runtime, the `cache.preload_top_n` most frequent live versions of the services whose
    // `cache.preload` is on (`min_latency`). Loads run on the blocking pool, off this path.
    match meta.load_cache_freq().await {
        Ok(Some(blob)) => {
            let parts = rt_cache::unpack_snapshots(&blob);
            if let Some(p) = parts.first() {
                deno_cache.restore_frequencies(p);
            }
            if let Some(p) = parts.get(1) {
                wasm_cache.restore_frequencies(p);
            }
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(error = %e, "cache frequencies not restored (cold start)"), // LCOV_EXCL_LINE: the store failing while the node starts
    }
    {
        let cfg_now = config.load();
        let (mut deno_keys, mut wasm_keys) = (Vec::new(), Vec::new());
        let ids: Vec<ServiceId> = service_table.load().keys().copied().collect();
        for id in ids {
            let Some((entry, svc)) = specs.lookup(id) else { continue };
            let plan = plans.plan_arc(entry.tenant);
            if !cfg_now
                .service_effective(svc.profile, &svc.config, &plan)
                .knobs(&plan)
                .preload
            {
                continue;
            }
            match entry.runtime {
                RuntimeKind::Deno => deno_keys.push(node.key_of(&entry)),
                RuntimeKind::Wasm => wasm_keys.push(node.key_of(&entry)),
            }
        }
        let n = cfg_now.cache.preload_top_n as usize;
        let preloaded = deno_cache.preload_top(&deno_keys, n).len() + wasm_cache.preload_top(&wasm_keys, n).len();
        if preloaded > 0 {
            tracing::info!(preloaded, "preloading hot services into T1");
        }
    }

    // 11. listeners
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let limits = ServeLimits {
        max_conns_per_ip: cfg.ingress.max_conns_per_ip as usize,
        // Every client behind a trusted proxy shares its address: the cap would throttle them all.
        uncapped: rt_sec::TrustedProxies::parse(&cfg.ingress.trusted_proxies).unwrap_or_default(),
        metrics: Some(metrics.clone()),
        backlog: cfg.ingress.backlog,
        header_timeout: cfg.ingress.header_timeout,
        idle_timeout: cfg.ingress.idle_timeout,
        h2_max_streams: cfg.ingress.h2_max_streams,
        grace: SHUTDOWN_GRACE,
        ..ServeLimits::default()
    };
    let stop = |rx: &watch::Receiver<bool>| {
        let mut rx = rx.clone();
        async move {
            let _ = rx.wait_for(|v| *v).await;
        }
    };
    let mut servers: Vec<JoinHandle<std::io::Result<()>>> = Vec::new();
    let public = bind_tcp(
        &cfg.listeners.public,
        invoke_router(state.clone(), Listener::Public),
        &limits,
        &shutdown_rx,
        &mut servers,
    )
    .await?;
    let control_addr = bind_tcp(
        &cfg.listeners.control,
        control_router(state.clone()),
        &limits,
        &shutdown_rx,
        &mut servers,
    )
    .await?;
    let metrics_addr = bind_tcp(
        &cfg.listeners.metrics,
        health_router(state.clone()),
        &limits,
        &shutdown_rx,
        &mut servers,
    )
    .await?;
    let internal_router = invoke_router(state.clone(), Listener::Internal);
    let mut internal_path = None;
    match ListenAddr::parse(&cfg.listeners.internal) {
        ListenAddr::Unix(path) => {
            let listener = bind_private_unix(&path)?;
            // Only the node's own UID and `listeners.internal_allowed_uids` may connect (SEC-41).
            let peers = rt_sec::UnixPeerPolicy::node_default(&cfg.listeners.internal_allowed_uids);
            servers.push(tokio::spawn(rt_api::serve_unix(
                listener,
                internal_router,
                peers,
                stop(&shutdown_rx),
            )));
            internal_path = Some(path);
        }
        ListenAddr::Tcp(addr) => {
            // The internal listener trusts every peer (no invoke access check, no ingress rules,
            // `x-rt-invoker` honored): it must not be reachable from the network.
            let loopback = addr
                .parse::<SocketAddr>()
                .map(|a| a.ip().is_loopback())
                .unwrap_or(false);
            if !loopback {
                return Err(ServerError::Startup(format!(
                    "listeners.internal = {addr}: a TCP internal listener must bind a loopback address (it trusts every caller)"
                )));
            }
            bind_tcp(&addr, internal_router, &limits, &shutdown_rx, &mut servers).await?;
        }
    }
    // The node's own addresses: what the listeners report plus every interface address (a
    // wildcard bind is reachable on all of them, including the host's public IP).
    denylist.add_addrs([public.ip(), control_addr.ip(), metrics_addr.ip()]);
    denylist.add_addrs(rt_sec::local_interface_addrs());

    // 12. background tasks
    let mut background: Vec<JoinHandle<()>> = Vec::new();
    let mut periodic: Vec<PeriodicPass> = Vec::new();
    {
        let e = schedule_engine.clone();
        background.push(tokio::spawn(async move { e.run().await }));
        let q = queue_engine.clone();
        background.push(tokio::spawn(async move { q.run().await }));
        let d = delayed_engine.clone();
        background.push(tokio::spawn(async move { d.run().await }));
        // Durable objects' alarms: the pending ones come back from each durable service's data.
        let hub = sockets.clone();
        background.push(tokio::spawn(async move { hub.run_alarms().await }));
        let (hub, data, table) = (sockets.clone(), data.clone(), service_table.clone());
        background.push(tokio::spawn(async move {
            let durable: Vec<(TenantId, ServiceId)> = table
                .load()
                .values()
                .filter(|s| s.config.durable)
                .map(|s| (s.tenant_id, s.id))
                .collect();
            for (tenant, service) in durable {
                match data.durable(service).alarms().await {
                    Ok(alarms) => {
                        for (object, at) in alarms {
                            hub.set_alarm_for(tenant, service, &object, Some(at));
                        }
                    }
                    Err(e) => tracing::warn!(%service, error = %e, "durable alarms not loaded"), // LCOV_EXCL_LINE: the data store failing while the node starts
                }
            }
        }));
    }
    background.push(control.spawn_usage_flusher());
    {
        // Self-denylist (SEC-51): re-resolve the node's own endpoints every 5 min on a blocking
        // thread, following config reloads; interface addresses may change too (DHCP).
        let (denylist, config) = (denylist.clone(), config.clone());
        let (task, pass) = spawn_periodic(rt_host::SELF_DENYLIST_REFRESH, true, move |_| {
            let (d, hosts) = (denylist.clone(), rt_host::self_endpoint_hosts(&config.load()));
            async move {
                let _ = tokio::task::spawn_blocking(move || {
                    d.set_hosts(hosts);
                    rt_sec::EgressDenylist::refresh(&*d);
                    d.add_addrs(rt_sec::local_interface_addrs());
                })
                .await;
            }
        });
        background.push(task);
        periodic.push(pass);
    }
    // Custom-host claims: expire stale pending claims, verify `_rt-verify` TXT records
    // (`[hosts]`; no DNS query when `hosts.verify_dns = false`).
    background.push(tokio::spawn(rt_api::run_host_claims(
        state.clone(),
        Arc::new(rt_api::HickoryTxtResolver::new()),
    )));
    {
        // Build worker: eager compiles and rebuilds.
        let (node, control, tables) = (node.clone(), control.clone(), service_table.clone());
        background.push(tokio::spawn(async move {
            while let Some(job) = build_rx.recv().await {
                let key = PreparedKey {
                    digest: job.digest,
                    kind: job.runtime,
                    engine_fp: match job.runtime {
                        RuntimeKind::Deno => deno_fp,
                        RuntimeKind::Wasm => wasm_fp,
                    },
                };
                control.register_build(job.digest, job.service, job.version);
                if job.rebuild {
                    match job.runtime {
                        RuntimeKind::Deno => {
                            node.deno_cache.clear_failure(&key);
                            node.deno_cache.invalidate(&key);
                        }
                        RuntimeKind::Wasm => {
                            node.wasm_cache.clear_failure(&key);
                            node.wasm_cache.invalidate(&key);
                        }
                    }
                }
                let tenant = tables
                    .load()
                    .get(&job.service)
                    .map_or(TenantId::from_u128(0), |s| s.tenant_id);
                let spec = TaskSpec::build(
                    ServiceRef {
                        tenant,
                        service: job.service,
                        version: job.version,
                        digest: job.digest,
                    },
                    key,
                );
                // Through the cache's single-flight: a lazy request for the same artifact parks
                // on this build, and an identical build already running is not repeated (its
                // report covers this version too, see `register_build`).
                let requested = match job.runtime {
                    RuntimeKind::Deno => node.deno_cache.request_build(&key, spec),
                    RuntimeKind::Wasm => node.wasm_cache.request_build(&key, spec),
                };
                let Some(submitted) = requested else { continue };
                match submitted {
                    Ok(handle) => {
                        tokio::spawn(async move {
                            let _ = handle.await;
                        });
                    }
                    // LCOV_EXCL_START: the build scheduler refusing a build (needs a refusal reason it never produces in the flows)
                    Err(reason) => {
                        let failure = BuildFailure::new(
                            BuildFailReason::Internal,
                            format!("build rejected: {}", reason.reason()),
                        );
                        let report = BuildReport {
                            key,
                            result: Err(failure),
                            duration_ms: 0,
                            lock_digest: None,
                            bundle_digest: None,
                        };
                        let _ = control.report_build(&key, report).await;
                    } // LCOV_EXCL_STOP
                }
            }
        })); // LCOV_EXCL_LINE: the build channel closes only when the node itself is dropped
    }
    {
        // Sysmon-style heartbeat and maintenance (stats merge, gauges, cache and log sweeps).
        let (node, sched, metrics, stats, logs) = (
            node.clone(),
            scheduler.clone(),
            metrics.clone(),
            stats.clone(),
            log_store.clone(),
        );
        let (dc, wc) = (deno_cache.clone(), wasm_cache.clone());
        let (meta_m, queue_m, specs_m) = (meta.clone(), queue.clone(), specs.clone());
        let governor = governor.clone();
        let cache_dir = data_dir.join("cache");
        let mem_check = cfg.memory.check_interval.max(Duration::from_millis(100));
        let mem_every = (mem_check.as_millis() / 100).max(1) as u64;
        node.heartbeat();
        let (task, pass) = spawn_periodic(Duration::from_millis(100), false, move |n| {
            let (node, sched, metrics, stats, logs) = (
                node.clone(),
                sched.clone(),
                metrics.clone(),
                stats.clone(),
                logs.clone(),
            );
            let (dc, wc, governor, cache_dir) = (dc.clone(), wc.clone(), governor.clone(), cache_dir.clone());
            let (meta_m, queue_m, specs_m) = (meta_m.clone(), queue_m.clone(), specs_m.clone());
            async move {
                node.heartbeat();
                if n.is_multiple_of(mem_every) {
                    // The pressure level otherwise only moves on large reservations, so a node at
                    // Critical would never notice usage falling and never leave it (`memory.check_interval`).
                    observe_rss_sample(&governor, process_rss_bytes());
                    governor.tick();
                    publish_mem(&metrics, &governor.stats());
                }
                if n.is_multiple_of(10) {
                    stats.merge_shards();
                    metrics.observe_sched(&sched.snapshot());
                }
                if n.is_multiple_of(100) {
                    dc.sweep_idle(Instant::now());
                    wc.sweep_idle(Instant::now());
                }
                if n.is_multiple_of(T2_GC_EVERY_TICKS) {
                    // T2 stays under `cache.t2_max_mb` and frees space when the disk runs low
                    // (`disk.low_free_mb` → `disk.high_free_mb`). Both caches share one T2 store:
                    // one pass sees the entries either of them uses.
                    let (dc, wc, dir) = (dc.clone(), wc.clone(), cache_dir.clone());
                    let _ = tokio::task::spawn_blocking(move || {
                        let free = free_disk_bytes(&dir).unwrap_or(u64::MAX);
                        let freed = dc.gc_disk_with(free, &[&*wc as &dyn rt_cache::T2Usage]).bytes_freed;
                        if freed > 0 {
                            tracing::info!(freed, "T2 cache garbage-collected");
                            // LCOV_EXCL_LINE: needs entries over the T2 budget that no cache uses
                        }
                    })
                    .await;
                }
                if n.is_multiple_of(36_000) {
                    logs.sweep_retention(now_ms());
                }
                if n.is_multiple_of(600) {
                    // Decrypted secrets of services nobody ran for 10 minutes leave memory.
                    node.control
                        .evict_idle_env(now_ms(), crate::control_plane::ENV_IDLE_EVICT);
                }
                if n.is_multiple_of(CACHE_FREQ_SAVE_EVERY_TICKS) {
                    save_cache_freq(&*meta_m, &dc, &wc).await;
                }
                if n.is_multiple_of(QUEUE_DEPTH_EVERY_TICKS) {
                    publish_queue_depth(&*queue_m, &specs_m, &metrics).await;
                }
            }
        });
        background.push(task);
        periodic.push(pass);
    }
    {
        // Tenant storage on disk (storage quota) and fair-pool shares from the current plans.
        let (policy, blocking, plans, tenants) =
            (policy.clone(), blocking.clone(), plans.clone(), tenant_table.clone());
        let local_data = local_data.clone();
        let (task, pass) = spawn_periodic(STORAGE_SCAN_INTERVAL, false, move |_| {
            let (policy, blocking, plans, tenants) = (policy.clone(), blocking.clone(), plans.clone(), tenants.clone());
            let local_data = local_data.clone();
            async move {
                let p = policy.clone();
                let _ = tokio::task::spawn_blocking(move || p.scan_storage()).await;
                // Open databases follow the fresh totals and any plan change: each may grow only
                // by what its tenant has left.
                local_data.refresh_limits().await;
                for t in tenants.iter() {
                    blocking.set_tenant_share(t.id, plans.plan_for(t.id).blocking_share);
                }
            }
        });
        background.push(task);
        periodic.push(pass);
    }
    {
        // Expired KV rows are hidden on read but only deleted here (B3.7). Each service's sweep
        // runs on the fair blocking pool in its tenant's lane; only open databases are swept.
        let local_data = local_data.clone();
        let (task, pass) = spawn_periodic(KV_SWEEP_INTERVAL, true, move |_| {
            let local_data = local_data.clone();
            async move {
                match local_data.sweep_open().await {
                    Ok(0) => {}
                    Ok(n) => tracing::debug!(deleted = n, "expired KV rows swept"),
                    Err(e) => tracing::warn!(error = %e, "expired-KV sweep failed"),
                }
            }
        });
        background.push(task);
        periodic.push(pass);
    }
    {
        // Warm floors (`scaling.min_instances`) of services that no longer hold one are released
        // even if the event that should have released them was missed.
        let node = node.clone();
        let (task, pass) = spawn_periodic(FLOOR_RECONCILE_INTERVAL, true, move |_| {
            let node = node.clone();
            async move { node.sync_floors() }
        });
        background.push(task);
        periodic.push(pass);
    }
    {
        // Artifacts nothing references and nothing wrote or touched for the grace period: modules
        // and locks of failed builds, uploads whose version was never added.
        let (meta, artifacts) = (meta.clone(), artifacts.clone());
        let (task, pass) = spawn_periodic(ORPHAN_SWEEP_INTERVAL, true, move |_| {
            let (meta, artifacts) = (meta.clone(), artifacts.clone());
            async move {
                match rt_store::artifact::sweep_orphans(&*meta, &*artifacts, rt_store::ARTIFACT_GRACE).await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(deleted = n, "orphaned artifacts swept"),
                    Err(e) => tracing::warn!(error = %e, "orphaned-artifact sweep failed"),
                }
            }
        });
        background.push(task);
        periodic.push(pass);
    }

    tracing::info!(public = %public, control = %control_addr, metrics = %metrics_addr, "node started");
    let inner = Arc::new(Inner {
        node,
        scheduler,
        control,
        logs: log_store,
        shutdown_tx,
        servers: Mutex::new(servers),
        background: Mutex::new(background),
        periodic,
        api: state,
        _ticker: ticker,
    });
    Ok(RunningServer {
        addrs: ServerAddrs {
            public,
            control: control_addr,
            metrics: metrics_addr,
            internal: internal_path,
        },
        inner,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_host::SocketHost as _;

    fn ctx() -> TaskCtx {
        TaskCtx {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(2),
            task: rt_types::TaskId(3),
            depth: 0,
            deadline: Instant::now() + Duration::from_secs(5),
            trace: None,
            on_behalf_of: None,
            object: None,
        }
    }

    #[tokio::test]
    async fn calls_made_before_the_dispatcher_exists_fail_cleanly() {
        let late = LateCalls { inner: OnceLock::new() };
        let req = || {
            http::Request::builder()
                .uri("/")
                .body(rt_types::Body::empty())
                .expect("request")
        };
        assert!(matches!(late.call(&ctx(), "svc", req()).await, Err(CallError::Failed)));
        assert!(matches!(
            late.call_object(&ctx(), None, "room", req()).await,
            Err(CallError::Failed)
        ));
        let enqueue = late.enqueue(&ctx(), "svc", Bytes::new(), EnqueueOpts::default()).await;
        assert!(matches!(enqueue, Err(CallError::Failed)));
        assert!(late.sdk_invoke(&ctx(), "acme", "m", "{}").await.is_err());
    }

    #[test]
    fn sockets_asked_before_the_hub_exists_are_simply_absent() {
        let late = LateSockets { inner: OnceLock::new() };
        let c = ctx();
        assert!(late.accept(&c, "room", rt_host::AcceptOpts::default()).is_err());
        let close = rt_host::SocketOp::Close {
            socket: "ws_1".into(),
            code: 1000,
            reason: String::new(),
        };
        assert!(late.apply(&c, close).is_err(), "no such socket");
        assert!(late.list(&c, None, None, 10, None).0.is_empty());
        assert!(late.get(&c, "ws_1").is_none() && !late.exists(&c, "ws_1"));
        assert_eq!(late.count(&c, None, None), 0);
        assert!(late.objects(&c, "", 10, None).0.is_empty());
        assert!(late.set_tags(&c, "ws_1", Vec::new()).is_err());
        assert!(late.attachment(&c, "ws_1").is_err());
        assert!(late.set_attachment(&c, "ws_1", None).is_err());
        assert!(late.set_auto_response(&c, "room", None).is_ok());
        late.set_alarm(&c, "room", Some(5)); // nothing to schedule it on
    }

    #[test]
    fn free_disk_is_read_for_a_real_path_and_unknown_for_a_missing_one() {
        let dir = tempfile::tempdir().expect("tmp");
        assert!(free_disk_bytes(dir.path()).is_some_and(|b| b > 0));
        assert_eq!(free_disk_bytes(&dir.path().join("gone")), None);
    }

    #[test]
    fn a_platform_without_an_rss_reading_leaves_the_governor_as_it_was() {
        let governor = Governor::new(rt_mem::GovernorConfig::balanced(1 << 30));
        observe_rss_sample(&governor, None);
        assert_eq!(governor.stats().rss_bytes, 0, "no sample, no change");
        observe_rss_sample(&governor, Some(12 << 20));
        assert_eq!(
            governor.stats().rss_bytes,
            12 << 20,
            "a sample is what the governor reports"
        );
    }

    /// A plan source for the quota enforcer: every tenant on the default plan.
    struct FreePlan;

    impl rt_sec::PlanSource for FreePlan {
        fn plan_for(&self, _tenant: TenantId) -> rt_types::Plan {
            rt_types::Plan::default()
        }
    }

    #[tokio::test]
    async fn a_store_that_refuses_the_usage_read_leaves_the_counters_at_zero_and_the_node_starts() {
        let (_guard, logs) = rt_testkit::capture_tracing();
        let dir = tempfile::tempdir().expect("tmp");
        let meta = rt_store::SqliteMetaStore::open(&dir.path().join("meta.db")).expect("meta.db");
        let faults = rt_store::Faults::new();
        meta.inject_faults(faults.clone());
        faults.fail_next(1000, rt_store::Fault::Io);
        let meta: Arc<dyn MetaStore> = meta;
        let tenant = Tenant {
            id: TenantId::from_u128(5),
            name: "acme".into(),
            plan: "free".into(),
            overrides: serde_json::json!({}),
            created_at_ms: 0,
            disabled: false,
        };
        let tenants = DashMap::new();
        tenants.insert(tenant.id, tenant);
        let quotas = LocalQuotaEnforcer::new(Arc::new(FreePlan));
        seed_daily_usage(&meta, &tenants, &quotas).await;
        assert!(
            logs.contains("the tenant's daily counters start at zero"),
            "{}",
            logs.text()
        );
    }

    /// A control plane that remembers its events.
    #[derive(Default)]
    struct Events(Mutex<Vec<NodeEvent>>);

    // LCOV_EXCL_START: a test double: only `event` is exercised, the rest completes the trait
    #[async_trait]
    impl ControlPlane for Events {
        fn routes(&self) -> Arc<rt_types::RouteSnapshot> {
            Arc::new(rt_types::RouteSnapshot::default())
        }
        async fn subscribe_config(&self) -> rt_types::ConfigStream {
            futures_util::StreamExt::boxed(futures_util::stream::empty())
        }
        async fn service(&self, _id: ServiceId) -> Result<Option<rt_types::Service>, rt_types::CpError> {
            Ok(None)
        }
        async fn env_for(
            &self,
            _service: ServiceId,
            _version: rt_types::SecretsVersion,
        ) -> Result<zeroize::Zeroizing<rt_types::EnvBundle>, rt_types::CpError> {
            Err(rt_types::CpError::NotFound)
        }
        async fn report_build(
            &self,
            _key: &PreparedKey,
            _report: rt_types::BuildReport,
        ) -> Result<(), rt_types::CpError> {
            Ok(())
        }
        fn usage(&self, _batch: rt_types::UsageBatch) {}
        // LCOV_EXCL_STOP
        fn event(&self, ev: NodeEvent) {
            self.0.lock().push(ev);
        }
    }

    #[test]
    fn a_tampered_cache_entry_raises_a_security_event_naming_the_digest_and_file() {
        let events = Arc::new(Events::default());
        let key = PreparedKey {
            digest: rt_types::Digest::of(b"x"),
            kind: RuntimeKind::Deno,
            engine_fp: 1,
        };
        Tamper(events.clone()).tampered(&key, "bundle");
        let seen = events.0.lock();
        assert_eq!(seen[0].kind, "security.artifact_tamper");
        assert_eq!(
            seen[0].data,
            serde_json::json!({"digest": key.digest.to_string(), "file": "bundle"})
        );
    }

    #[tokio::test]
    async fn a_store_that_refuses_the_cache_frequencies_or_the_queue_purge_is_logged() {
        let (_guard, logs) = rt_testkit::capture_tracing();
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = rt_store::SqliteMetaStore::open(&dir.path().join("meta.db")).expect("meta.db");
        let faults = rt_store::Faults::new();
        meta.inject_faults(faults.clone());
        faults.fail_next(1, rt_store::Fault::Io);
        store_cache_freq(&*meta, vec![1, 2, 3]).await;
        assert!(logs.contains("cache frequencies not saved"), "{}", logs.text());
        store_cache_freq(&*meta, vec![1, 2, 3]).await; // and it recovers

        let queue = SqliteQueueStore::open_in_memory(5).expect("queue");
        let qf = rt_store::Faults::new();
        queue.inject_faults(qf.clone());
        qf.fail_next(1, rt_store::Fault::Io);
        purge_queue_rows(&queue, ServiceId::from_u128(9)).await;
        assert!(
            logs.contains("purging the queue rows of a deleted service failed"),
            "{}",
            logs.text()
        );

        let streams = SqliteStreamStore::open_in_memory().expect("streams");
        let sf = rt_store::Faults::new();
        streams.inject_faults(sf.clone());
        sf.fail_next(1, rt_store::Fault::Io);
        purge_stream_rows(&*streams, ServiceId::from_u128(9)).await;
        assert!(
            logs.contains("purging the stream rows of a deleted service failed"),
            "{}",
            logs.text()
        );
    }
}
