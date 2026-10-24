//! In-process node for the handler tests: the real SQLite and file stores in a temp dir, the real
//! authenticator, quota enforcer, memory governor and scheduler, and small recording fakes for the
//! parts that belong to the server (plans, node facts, socket tasks, upload validation).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::Router;
use dashmap::DashMap;
use http_body_util::BodyExt;
use parking_lot::Mutex;
use rt_config::Config;
use rt_mem::{Governor, GovernorConfig, MemoryGovernor};
use rt_obs::{FileLogStore, HdrStatsStore, LogStore, LogStoreConfig, Metrics, MetricsOptions, StatsStore};
use rt_sched::{SchedConfig, Scheduler, SystemClock, UnitSource};
use rt_sec::{
    AuditEvent, AuditSink, DefaultAuthenticator, KeyHasher, KeyLookup, LocalQuotaEnforcer, LocalSecrets, MasterKey,
    PlanSource, QuotaEnforcer, SecretsBackend, SystemClock as WallSystemClock,
};
use rt_store::{DataConfig, LocalArtifactStore, LocalServiceData, MetaStore, SqliteMetaStore, SqliteQueueStore};
use rt_testkit::{MockBuildExecutor, MockDenoExecutor, MockPreparedSource, MockWasmExecutor, ScriptBook};
use rt_triggers::{HubConfig, RouteTable, SocketHub};
use rt_types::{
    ApiKey, KeyId, ObjectRef, Plan, RouteEntry, RuntimeKind, ServiceId, SocketEvent, TaskSpec, TenantId,
    UploadValidator, ValidatedUpload, ValidationError,
};
use tower::ServiceExt;

use crate::state::{
    ApiState, ControlEvent, IdemStore, IngressBuckets, IngressCache, NodeConfigUpdate, NodeInfo, SseLimiter,
};

/// The operator's root token of every test node.
pub const ROOT: &str = "test-root-token-0123456789abcdef";

/// Audit events in the order they were emitted.
#[derive(Default)]
pub struct RecordingAudit(pub Mutex<Vec<AuditEvent>>);

impl AuditSink for RecordingAudit {
    fn emit(&self, ev: AuditEvent) {
        self.0.lock().push(ev);
    }
}

/// Every tenant is on the default plan.
pub struct DefaultPlans;

impl PlanSource for DefaultPlans {
    fn plan_for(&self, _: TenantId) -> Plan {
        Plan::default()
    }
}

/// No `rtk_` key is stored: only the root token authenticates.
pub struct NoKeys;

impl KeyLookup for NoKeys {
    fn find(&self, _: KeyId) -> Option<ApiKey> {
        None
    }
}

/// Node facts; the test sets `ready` and `draining` and reads what the handlers notified.
pub struct FakeNode {
    pub ready: AtomicBool,
    pub draining: AtomicBool,
    pub notified: Mutex<Vec<ControlEvent>>,
}

impl NodeInfo for FakeNode {
    fn cache(&self) -> serde_json::Value {
        serde_json::json!({"deno": {"entries": 0}})
    }
    fn v8_sandbox(&self) -> bool {
        false
    }
    fn healthy(&self) -> bool {
        true
    }
    fn ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
    fn apply_live_config(&self, update: &NodeConfigUpdate) -> Result<Vec<String>, String> {
        let _ = update;
        Ok(Vec::new())
    }
    fn start_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }
    fn notify(&self, ev: ControlEvent) {
        self.notified.lock().push(ev);
    }
    fn http_task(&self, _: &RouteEntry, _: rt_types::HttpRequest, _: Option<rt_types::TraceCtx>) -> Option<TaskSpec> {
        None
    }
}

/// Uploads are not validated here: every upload is refused as an invalid artifact.
pub struct RefuseUploads;

impl UploadValidator for RefuseUploads {
    fn validate(&self, _: RuntimeKind, _: &str, _: &bytes::Bytes) -> Result<ValidatedUpload, ValidationError> {
        Err(ValidationError::InvalidArtifact("test node accepts no uploads".into()))
    }
}

/// The socket hub has no event tasks and no limits to apply in these tests.
pub struct NoSocketTasks;

impl rt_triggers::SocketTaskFactory for NoSocketTasks {
    fn event_task(&self, _: ServiceId, _: ObjectRef, _: SocketEvent) -> Option<TaskSpec> {
        None
    }
    fn limits(&self, _: TenantId, _: ServiceId) -> rt_triggers::SocketLimits {
        rt_triggers::SocketLimits::default()
    }
}

/// A node with every store on disk under `dir`, and the handles a test needs.
#[allow(dead_code)] // the fields are read by the tests that need them
pub struct TestNode {
    pub dir: tempfile::TempDir,
    pub state: ApiState,
    pub audit: Arc<RecordingAudit>,
    pub facts: Arc<FakeNode>,
}

impl TestNode {
    /// The control API router of this node.
    pub fn control(&self) -> Router {
        crate::control_router(self.state.clone())
    }

    /// Send one request to the control API as the operator of `tenant` (or the root without one).
    pub async fn call(
        &self,
        method: Method,
        uri: &str,
        tenant: Option<TenantId>,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        self.call_with(method, uri, tenant, &[], body).await
    }

    /// [`TestNode::call`] with extra request headers (`Idempotency-Key`, `If-Match`, ...).
    pub async fn call_with(
        &self,
        method: Method,
        uri: &str,
        tenant: Option<TenantId>,
        headers: &[(&str, &str)],
        body: Option<serde_json::Value>,
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {ROOT}"));
        if let Some(t) = tenant {
            req = req.header("x-rt-tenant", t.to_string());
        }
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let body = match body {
            Some(v) => {
                req = req.header("content-type", "application/json");
                Body::from(v.to_string())
            }
            None => Body::empty(),
        };
        let resp = self
            .control()
            .oneshot(req.body(body).expect("request"))
            .await
            .expect("router");
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.into_body().collect().await.expect("body").to_bytes();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, headers, json)
    }
}

/// Build a node in a fresh temp dir. Stores are real; the rest is the fakes above.
pub async fn node() -> TestNode {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    for d in ["artifacts", "logs", "services", "blobs"] {
        std::fs::create_dir_all(root.join(d)).expect("dir");
    }
    let master = MasterKey::generate();
    let hasher = KeyHasher::new(&master);
    let clock: Arc<dyn rt_sched::Clock> = Arc::new(SystemClock::new());
    let governor = Governor::new(GovernorConfig {
        budget_bytes: 1 << 30,
        high: 0.8,
        critical: 0.95,
        critical_exit_hold: Duration::from_secs(5),
    });
    let mem: Arc<dyn MemoryGovernor> = governor.clone();
    let metrics = Metrics::new(MetricsOptions {
        per_service: false,
        per_service_top_n: 5,
    })
    .expect("metrics");
    let logs: Arc<dyn LogStore> = FileLogStore::open(LogStoreConfig {
        dir: root.join("logs"),
        ring_lines: 1_000,
        retention_days: 1,
        max_bytes_per_service: 1 << 20,
        rate_per_sec: 1_000,
        burst: 10_000,
        stdout: false,
        flush_interval: rt_obs::log::LOG_FLUSH_INTERVAL,
        fsync: false,
    })
    .expect("log store");
    let stats: Arc<dyn StatsStore> = HdrStatsStore::new(Arc::new(|| 0));
    let meta = SqliteMetaStore::open(&root.join("meta.db")).expect("meta.db");
    let queue = SqliteQueueStore::open_in_memory(5).expect("queue");
    let data_cfg = DataConfig::new(root);
    let data = LocalServiceData::open(data_cfg).expect("service data");
    let artifacts = LocalArtifactStore::open(&root.join("artifacts")).expect("artifacts");
    meta.ensure_tenant(
        rt_types::system_tenant(),
        rt_types::SYSTEM_TENANT_NAME,
        rt_types::SYSTEM_PLAN,
    )
    .await
    .expect("system tenant");

    // The scheduler runs the testkit's scripted executors: nothing here runs guest code.
    let book = ScriptBook::new();
    let prepared = MockPreparedSource::new();
    prepared.set_auto_ready(true);
    let (b1, b2) = (book.clone(), book.clone());
    let scheduler: Scheduler = Scheduler::builder(SchedConfig::default(), clock.clone(), mem.clone())
        .executor(move || MockDenoExecutor::new(b1.clone()), prepared.clone(), 0)
        .executor(move || MockWasmExecutor::new(b2.clone()), prepared.clone(), 0)
        .executor(move || MockBuildExecutor::new(book.clone()), Arc::new(UnitSource), 0)
        .build()
        .expect("scheduler");
    let plans: Arc<dyn PlanSource> = Arc::new(DefaultPlans);
    let quotas: Arc<dyn QuotaEnforcer> = Arc::new(LocalQuotaEnforcer::new(plans.clone()));
    let secrets: Arc<dyn SecretsBackend> = Arc::new(LocalSecrets::new(&master));
    let auth = Arc::new(DefaultAuthenticator::new(
        Some(ROOT.to_string()),
        hasher.clone(),
        Arc::new(NoKeys),
        Arc::new(WallSystemClock),
    ));
    let audit = Arc::new(RecordingAudit::default());
    let facts = Arc::new(FakeNode {
        ready: AtomicBool::new(true),
        draining: AtomicBool::new(false),
        notified: Mutex::new(Vec::new()),
    });
    let sockets = SocketHub::new(
        HubConfig::default(),
        Arc::new(|_| Err(rt_types::RejectReason::Draining)),
        Arc::new(NoSocketTasks),
        tokio::runtime::Handle::current(),
    );
    let state = ApiState {
        config: Arc::new(ArcSwap::from_pointee(Config::default())),
        meta: meta.clone(),
        artifacts,
        queue: queue.clone(),
        delayed: queue.clone(),
        data,
        scheduler,
        routes: RouteTable::new(None),
        auth,
        quotas,
        plans,
        audit: audit.clone(),
        secrets,
        hasher,
        validator: Arc::new(RefuseUploads),
        logs,
        stats,
        metrics,
        mem,
        clock,
        node: facts.clone(),
        idem: Arc::new(IdemStore::default()),
        sse: Arc::new(SseLimiter::default()),
        buckets: Arc::new(IngressBuckets::default()),
        service_table: Arc::new(ArcSwap::from_pointee(Default::default())),
        tenant_table: Arc::new(DashMap::new()),
        ingress: Arc::new(IngressCache::default()),
        sockets,
        uploads: Arc::new(tokio::sync::Semaphore::new(crate::MAX_CONCURRENT_UPLOADS)),
    };
    TestNode {
        dir,
        state,
        audit,
        facts,
    }
}

/// Create a tenant through `POST /v1/tenants` and return its id.
pub async fn tenant(node: &TestNode, name: &str) -> TenantId {
    let (status, _, body) = node
        .call(
            Method::POST,
            "/v1/tenants",
            None,
            Some(serde_json::json!({"name": name, "plan": "free"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body["id"]
        .as_str()
        .expect("tenant id")
        .parse()
        .expect("tenant id parses")
}

/// Create a service in `tenant` through `POST /v1/services` (`extra` adds body fields such as
/// `durable`) and return its id.
pub async fn service(node: &TestNode, tenant: TenantId, slug: &str, extra: serde_json::Value) -> rt_types::ServiceId {
    let mut body = serde_json::json!({"slug": slug, "runtime": "deno"});
    if let (Some(b), Some(e)) = (body.as_object_mut(), extra.as_object()) {
        b.extend(e.clone());
    }
    let (status, _, out) = node.call(Method::POST, "/v1/services", Some(tenant), Some(body)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    out["id"]
        .as_str()
        .expect("service id")
        .parse()
        .expect("service id parses")
}
