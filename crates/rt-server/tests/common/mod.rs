//! Shared helpers for the rt-server integration tests (wave 2, lane T-F).
//!
//! - booting the real server in-process through `rt_testkit::NodeHarness` (temp data dir,
//!   random ports), a restartable variant for ARC-04 / E2E-08 and a subprocess variant for the
//!   tests that need a real process (SIGTERM, release-mode auth, log capture);
//! - small request helpers for the control API with a root token or a tenant key;
//! - guest sources (Deno TypeScript) used by the flows;
//! - raw HTTP over TCP / Unix sockets, a recording mock upstream, polling helpers.
//!
//! Fixtures from other lanes are referenced by path only: `fixtures/wasm/out/<name>.wasm`.

#![allow(dead_code)]

pub mod openapi;

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::header::HeaderMap;
use reqwest::{Method, RequestBuilder, StatusCode};
use rt_config::{Config, NodeSecrets, Secret};
use rt_server::{start_with, RunningServer, ServerAddrs, ServerError, StartOptions};
use rt_testkit::{Booted, NodeAddrs, NodeHarness, NodeSetup};
use rt_types::{EgressDefault, Plan, PlanLimitsMax, TriggerKind};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ---------------------------------------------------------------------------------------------
// Plans
// ---------------------------------------------------------------------------------------------

/// A generous plan for tests that are not about quotas (`pro`).
pub fn pro_plan() -> Plan {
    Plan {
        max_services: 1_000,
        max_concurrency: 2_000,
        max_instances: 0, // no per-service instance cap: the node default (profile ceiling)
        max_warm_floor_total: 0,
        rps: 1_000_000,
        invocations_per_day: 1_000_000_000,
        storage_mb: 1_024,
        egress_mb_per_day: 10_000,
        max_schedules: 20,
        min_schedule_seconds: 1,
        max_queue_depth: 10_000,
        max_pinned_mb: 64,
        allowed_triggers: TriggerKind::ALL.to_vec(),
        limits_max: PlanLimitsMax {
            wall_ms: 120_000,
            cpu_ms: 10_000,
            memory_mb: 512,
        },
        builds_per_hour: 100_000,
        custom_host_suffixes: vec![".bots.example.com".to_string()],
        blocking_share: 0.5,
        egress_default: EgressDefault::Allow,
        allow_cross_tenant_calls: false,
        max_sockets: 10_000,
        max_sockets_per_object: 10_000,
        ws_max_message_bytes: 1024 * 1024,
    }
}

pub const ALL_SCOPES: [&str; 10] = [
    "services:read",
    "services:write",
    "deploy",
    "invoke",
    "logs:read",
    "env:write",
    "secrets:write",
    "queue:write",
    "data:admin",
    "sockets:write",
];

// ---------------------------------------------------------------------------------------------
// Node access abstraction (NodeHarness, restartable node)
// ---------------------------------------------------------------------------------------------

pub trait Node {
    fn http(&self) -> &reqwest::Client;
    fn control_url(&self, path: &str) -> String;
    fn public_url(&self, path: &str) -> String;
    fn metrics_url(&self, path: &str) -> String;
    fn root_token(&self) -> &str;
}

impl Node for NodeHarness {
    fn http(&self) -> &reqwest::Client {
        NodeHarness::http(self)
    }
    fn control_url(&self, path: &str) -> String {
        NodeHarness::control_url(self, path)
    }
    fn public_url(&self, path: &str) -> String {
        NodeHarness::public_url(self, path)
    }
    fn metrics_url(&self, path: &str) -> String {
        NodeHarness::metrics_url(self, path)
    }
    fn root_token(&self) -> &str {
        &self.root_token
    }
}

// ---------------------------------------------------------------------------------------------
// Booting
// ---------------------------------------------------------------------------------------------

/// Keeps a handle to the `RunningServer` so a test can call hooks (`drain`, sysmon stall).
pub type Handle = Arc<Mutex<Option<Arc<RunningServer>>>>;

pub fn new_handle() -> Handle {
    Arc::new(Mutex::new(None))
}

/// The boot function handed to `NodeHarness::start`: the real server, in-process.
pub async fn boot(setup: NodeSetup, opts: StartOptions, handle: Option<Handle>) -> Result<Booted, ServerError> {
    let running = start_with(setup.config, setup.secrets, opts).await?;
    let a = running.addrs();
    let addrs = NodeAddrs {
        public: a.public,
        control: a.control,
        metrics: a.metrics,
        internal: a.internal,
    };
    let running = Arc::new(running);
    if let Some(h) = &handle {
        *h.lock().expect("handle lock") = Some(running.clone());
    }
    Ok(Booted {
        addrs,
        shutdown: Box::new(move || {
            Box::pin(async move {
                if let Some(h) = &handle {
                    h.lock().expect("handle lock").take();
                }
                if let Ok(r) = Arc::try_unwrap(running) {
                    r.shutdown().await;
                }
            })
        }),
    })
}

pub async fn start_node() -> NodeHarness {
    start_node_with(|_| {}).await
}

/// Start a node with the `pro` plan installed; `tweak` may change anything (including `pro`).
pub async fn start_node_with(tweak: impl FnOnce(&mut Config)) -> NodeHarness {
    start_node_opts(StartOptions::default(), None, tweak).await
}

pub async fn start_node_opts(
    opts: StartOptions,
    handle: Option<Handle>,
    tweak: impl FnOnce(&mut Config),
) -> NodeHarness {
    NodeHarness::start_with(
        move |setup| boot(setup, opts, handle),
        |c| {
            c.plans.insert("pro".to_string(), pro_plan());
            tweak(c);
        },
    )
    .await
    .expect("the node starts")
}

/// An in-process node whose data directory outlives a restart (ARC-04, E2E-08, TRG-08).
pub struct Restartable {
    pub dir: tempfile::TempDir,
    pub config: Config,
    pub secrets: NodeSecrets,
    pub root: String,
    http: reqwest::Client,
    running: Option<RunningServer>,
    addrs: Option<ServerAddrs>,
}

impl Restartable {
    pub async fn boot(tweak: impl FnOnce(&mut Config)) -> Self {
        let dir = tempfile::tempdir().expect("temp data dir");
        let mut setup = NodeSetup::new(dir.path());
        setup.config.plans.insert("pro".to_string(), pro_plan());
        tweak(&mut setup.config);
        let mut me = Restartable {
            dir,
            config: setup.config,
            secrets: setup.secrets,
            root: setup.root_token,
            http: reqwest::Client::builder().no_proxy().build().expect("http client"),
            running: None,
            addrs: None,
        };
        me.start().await;
        me
    }

    pub async fn start(&mut self) {
        assert!(self.running.is_none(), "already running");
        let running = rt_server::start(self.config.clone(), self.secrets.clone())
            .await
            .expect("the node starts");
        self.addrs = Some(running.addrs());
        self.running = Some(running);
    }

    /// Graceful stop (drain + flush); the data directory stays.
    pub async fn stop(&mut self) {
        if let Some(r) = self.running.take() {
            r.shutdown().await;
        }
        self.addrs = None;
    }

    pub async fn restart(&mut self) {
        self.stop().await;
        self.start().await;
    }

    pub fn addrs(&self) -> &ServerAddrs {
        self.addrs.as_ref().expect("node is running")
    }

    pub fn data_dir(&self) -> &Path {
        self.dir.path()
    }
}

impl Node for Restartable {
    fn http(&self) -> &reqwest::Client {
        &self.http
    }
    fn control_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addrs().control, path)
    }
    fn public_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addrs().public, path)
    }
    fn metrics_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addrs().metrics, path)
    }
    fn root_token(&self) -> &str {
        &self.root
    }
}

// ---------------------------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------------------------

pub struct Resp {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub text: String,
    pub json: Value,
    pub elapsed: Duration,
}

impl Resp {
    pub fn s(&self) -> u16 {
        self.status.as_u16()
    }

    /// `error.code` of a control error envelope (`""` when absent).
    pub fn code(&self) -> String {
        self.json["error"]["code"].as_str().unwrap_or("").to_string()
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
    }

    /// `items` of a page, or the array itself.
    pub fn items(&self) -> Vec<Value> {
        match &self.json {
            Value::Array(a) => a.clone(),
            v => v["items"].as_array().cloned().unwrap_or_default(),
        }
    }
}

pub async fn send(rb: RequestBuilder) -> Resp {
    let start = Instant::now();
    let r = rb.send().await.expect("request failed");
    let status = r.status();
    let headers = r.headers().clone();
    let text = r.text().await.unwrap_or_default();
    let json = serde_json::from_str(&text).unwrap_or(Value::Null);
    Resp {
        status,
        headers,
        text,
        json,
        elapsed: start.elapsed(),
    }
}

/// A control request authenticated with a tenant key (or any bearer token).
pub fn api(node: &impl Node, key: &str, m: Method, path: &str) -> RequestBuilder {
    node.http().request(m, node.control_url(path)).bearer_auth(key)
}

/// A control request authenticated with the operator token.
pub fn root(node: &impl Node, m: Method, path: &str) -> RequestBuilder {
    node.http()
        .request(m, node.control_url(path))
        .bearer_auth(node.root_token())
}

/// Operator token acting for `tenant_id` (`x-rt-tenant`).
pub fn root_for(node: &impl Node, tenant_id: &str, m: Method, path: &str) -> RequestBuilder {
    root(node, m, path).header("x-rt-tenant", tenant_id)
}

pub async fn call(rb: RequestBuilder, body: Option<Value>) -> Resp {
    send(match body {
        Some(b) => rb.json(&b),
        None => rb,
    })
    .await
}

pub async fn get(node: &impl Node, key: &str, path: &str) -> Resp {
    call(api(node, key, Method::GET, path), None).await
}

pub async fn post(node: &impl Node, key: &str, path: &str, body: Value) -> Resp {
    call(api(node, key, Method::POST, path), Some(body)).await
}

pub async fn put(node: &impl Node, key: &str, path: &str, body: Value) -> Resp {
    call(api(node, key, Method::PUT, path), Some(body)).await
}

pub async fn patch(node: &impl Node, key: &str, path: &str, body: Value) -> Resp {
    call(api(node, key, Method::PATCH, path), Some(body)).await
}

pub async fn delete(node: &impl Node, key: &str, path: &str) -> Resp {
    call(api(node, key, Method::DELETE, path), None).await
}

// ---------------------------------------------------------------------------------------------
// Tenants, keys, services, deploys
// ---------------------------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(1);

/// A unique, slug-safe name: `prefix-<n>` (prefix must start with a lowercase letter).
pub fn uniq(prefix: &str) -> String {
    format!("{prefix}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[derive(Clone, Debug)]
pub struct Tenant {
    pub id: String,
    /// `rtk_…` secret with the scopes it was created with.
    pub key: String,
    pub key_id: String,
}

pub async fn create_tenant(node: &impl Node, plan: &str) -> String {
    let r = call(
        root(node, Method::POST, "/v1/tenants"),
        Some(json!({"name": uniq("tenant"), "plan": plan})),
    )
    .await;
    assert_eq!(r.s(), 201, "create tenant: {}", r.text);
    r.json["id"].as_str().expect("tenant id").to_string()
}

/// `(key_id, secret)`: a key limited to `project_ids` (`prj_…`; `None` = all projects).
pub async fn make_key(
    node: &impl Node,
    tenant_id: &str,
    scopes: &[&str],
    project_ids: Option<Vec<String>>,
) -> (String, String) {
    make_key_in(node, tenant_id, scopes, project_ids, None).await
}

/// [`make_key`] with an environment limit too (`None` = all environments).
pub async fn make_key_in(
    node: &impl Node,
    tenant_id: &str,
    scopes: &[&str],
    project_ids: Option<Vec<String>>,
    environments: Option<Vec<String>>,
) -> (String, String) {
    let mut body = json!({"scopes": scopes});
    if let Some(p) = project_ids {
        body["project_ids"] = json!(p);
    }
    if let Some(e) = environments {
        body["environments"] = json!(e);
    }
    let r = call(
        root(node, Method::POST, &format!("/v1/tenants/{tenant_id}/keys")),
        Some(body),
    )
    .await;
    assert_eq!(r.s(), 201, "create key: {}", r.text);
    (
        r.json["id"].as_str().expect("key id").to_string(),
        r.json["secret"].as_str().expect("key secret").to_string(),
    )
}

pub async fn new_tenant(node: &impl Node, plan: &str) -> Tenant {
    new_tenant_scopes(node, plan, &ALL_SCOPES).await
}

pub async fn new_tenant_scopes(node: &impl Node, plan: &str, scopes: &[&str]) -> Tenant {
    let id = create_tenant(node, plan).await;
    let (key_id, key) = make_key(node, &id, scopes, None).await;
    Tenant { id, key, key_id }
}

/// A project of the caller's tenant, with `environments` (empty = the default list); its `prj_…` id.
pub async fn create_project(node: &impl Node, key: &str, slug: &str, environments: &[&str]) -> String {
    let mut body = json!({"slug": slug});
    if !environments.is_empty() {
        body["environments"] = json!(environments);
    }
    let r = post(node, key, "/v1/projects", body).await;
    assert_eq!(r.s(), 201, "create project `{slug}`: {}", r.text);
    r.json["id"].as_str().expect("project id").to_string()
}

/// A service created in `project` (a `prj_…` id or slug) and `environment`; its `svc_…` id.
pub async fn create_service_in(
    node: &impl Node,
    key: &str,
    project: &str,
    environment: &str,
    slug: &str,
    runtime: &str,
) -> String {
    let r = post(
        node,
        key,
        "/v1/services",
        json!({"slug": slug, "runtime": runtime, "project": project, "environment": environment}),
    )
    .await;
    assert_eq!(r.s(), 201, "create service `{slug}` in `{project}`: {}", r.text);
    r.json["id"].as_str().expect("service id").to_string()
}

pub async fn create_service(node: &impl Node, key: &str, slug: &str, runtime: &str) -> String {
    let r = post(node, key, "/v1/services", json!({"slug": slug, "runtime": runtime})).await;
    assert_eq!(r.s(), 201, "create service `{slug}`: {}", r.text);
    r.json["id"].as_str().expect("service id").to_string()
}

/// D-B3: a service's environment is one of its project's. Lists `envs` (plus `production`) as the
/// environments of the tenant's `default` project.
pub async fn allow_environments(node: &impl Node, key: &str, envs: &[&str]) {
    let mut all = vec!["production"];
    all.extend(envs.iter().filter(|e| **e != "production"));
    let r = patch(node, key, "/v1/projects/default", json!({"environments": all})).await;
    assert_eq!(r.s(), 200, "allow environments {all:?}: {}", r.text);
}

pub async fn upload(node: &impl Node, key: &str, svc: &str, content_type: &str, body: Vec<u8>, query: &str) -> Resp {
    send(
        api(node, key, Method::POST, &format!("/v1/services/{svc}/versions?{query}"))
            .header("content-type", content_type)
            .body(body),
    )
    .await
}

pub async fn upload_ts(node: &impl Node, key: &str, svc: &str, src: &str, query: &str) -> Resp {
    upload(node, key, svc, "application/typescript", src.as_bytes().to_vec(), query).await
}

/// Create a deno service and upload + activate `src`; returns the service id.
pub async fn ship(node: &impl Node, key: &str, slug: &str, src: &str) -> String {
    let id = create_service(node, key, slug, "deno").await;
    let r = upload_ts(node, key, &id, src, "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "upload to `{slug}`: {} {}", r.s(), r.text);
    id
}

/// Create a wasm service from `fixtures/wasm/out/<fixture>.wasm`, upload + activate.
pub async fn ship_wasm(node: &impl Node, key: &str, slug: &str, fixture: &str) -> String {
    let id = create_service(node, key, slug, "wasm").await;
    let r = upload(node, key, &id, "application/wasm", wasm_fixture(fixture), "activate=1").await;
    assert!(
        r.s() == 201 || r.s() == 202,
        "upload wasm to `{slug}`: {} {}",
        r.s(),
        r.text
    );
    id
}

pub fn repo_path(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").join(rel)
}

/// `fixtures/wasm/out/<name>.wasm` (built by another lane).
pub fn wasm_fixture(name: &str) -> Vec<u8> {
    let p = repo_path(&format!("fixtures/wasm/out/{name}.wasm"));
    std::fs::read(&p).unwrap_or_else(|e| panic!("missing WASM fixture {} ({e}); build the fixtures first", p.display()))
}

// ---------------------------------------------------------------------------------------------
// Invoking
// ---------------------------------------------------------------------------------------------

pub async fn invoke(node: &impl Node, slug: &str, path: &str) -> Resp {
    send(node.http().get(node.public_url(&format!("/s/{slug}{path}")))).await
}

pub async fn invoke_with(
    node: &impl Node,
    slug: &str,
    m: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Resp {
    let mut rb = node.http().request(m, node.public_url(&format!("/s/{slug}{path}")));
    for (k, v) in headers {
        rb = rb.header(*k, *v);
    }
    send(rb.body(body.to_string())).await
}

/// `x-rt-version` of a response as a number.
pub fn version_of(r: &Resp) -> Option<u32> {
    r.header("x-rt-version").and_then(|v| v.parse().ok())
}

/// An RFC 3339 UTC timestamp `secs_from_now` seconds away (key `expires_at`).
pub fn rfc3339_in(secs_from_now: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
        + secs_from_now;
    let (days, rem) = (now.div_euclid(86_400), now.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

// ---------------------------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------------------------

/// Poll `f` every 50 ms until it returns `true` or `timeout` passes (then panic with `what`).
pub async fn eventually<F, Fut>(timeout: Duration, what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if f().await {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timed out after {timeout:?} waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Parse `name{labels} value` samples of a Prometheus text exposition for metric `name`.
pub fn metric_values(text: &str, name: &str) -> Vec<(String, f64)> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| {
            let (series, value) = l.rsplit_once(' ')?;
            let base = series.split('{').next()?;
            if base == name {
                Some((series.to_string(), value.parse::<f64>().ok()?))
            } else {
                None
            }
        })
        .collect()
}

pub fn metric_sum(text: &str, name: &str) -> f64 {
    metric_values(text, name).iter().map(|(_, v)| v).sum()
}

// ---------------------------------------------------------------------------------------------
// Raw HTTP (TCP and Unix sockets)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RawResp {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RawResp {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
}

fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(pos) = b.windows(2).position(|w| w == b"\r\n") {
        let size = usize::from_str_radix(
            String::from_utf8_lossy(&b[..pos])
                .split(';')
                .next()
                .unwrap_or("0")
                .trim(),
            16,
        )
        .unwrap_or(0);
        b = &b[pos + 2..];
        if size == 0 || b.len() < size {
            if size > 0 {
                out.extend_from_slice(b);
            }
            break;
        }
        out.extend_from_slice(&b[..size]);
        b = &b[size..];
        if b.starts_with(b"\r\n") {
            b = &b[2..];
        }
    }
    out
}

pub fn parse_raw(buf: &[u8]) -> Option<RawResp> {
    let split = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let mut lines = head.lines();
    let status = lines.next()?.split_whitespace().nth(1)?.parse().ok()?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| {
            l.split_once(':')
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        })
        .collect();
    let raw_body = &buf[split + 4..];
    let chunked = headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v.contains("chunked"));
    let body = if chunked { dechunk(raw_body) } else { raw_body.to_vec() };
    Some(RawResp { status, headers, body })
}

async fn read_all<S: AsyncReadExt + Unpin>(s: &mut S, timeout: Duration) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(timeout, async {
        let mut chunk = [0u8; 8192];
        loop {
            match s.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
    })
    .await;
    buf
}

/// Send raw bytes to `addr` and read the response until the peer closes (use `Connection: close`).
/// `None` when the connection was refused or closed without a parsable response.
pub async fn raw_tcp(addr: SocketAddr, request: &[u8], timeout: Duration) -> Option<RawResp> {
    let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
    s.write_all(request).await.ok()?;
    let buf = read_all(&mut s, timeout).await;
    parse_raw(&buf)
}

pub async fn raw_unix(path: &Path, request: &[u8], timeout: Duration) -> Option<RawResp> {
    let mut s = tokio::net::UnixStream::connect(path).await.ok()?;
    s.write_all(request).await.ok()?;
    let buf = read_all(&mut s, timeout).await;
    parse_raw(&buf)
}

// ---------------------------------------------------------------------------------------------
// Mock upstream (operator platform API behind a binding)
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SeenReq {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
}

pub struct MockUpstream {
    pub addr: SocketAddr,
    pub seen: Arc<Mutex<Vec<SeenReq>>>,
    task: tokio::task::JoinHandle<()>,
}

impl MockUpstream {
    pub fn requests(&self) -> Vec<SeenReq> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl Drop for MockUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A tiny HTTP/1.1 server on loopback that records requests and answers `200 {"ok":true}`.
pub async fn start_upstream() -> MockUpstream {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("addr");
    let seen: Arc<Mutex<Vec<SeenReq>>> = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let seen = seen2.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match sock.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let head = String::from_utf8_lossy(&buf).into_owned();
                let mut lines = head.lines();
                let first = lines.next().unwrap_or("").to_string();
                let mut parts = first.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("").to_string();
                let headers: BTreeMap<String, String> = lines
                    .take_while(|l| !l.is_empty())
                    .filter_map(|l| {
                        l.split_once(':')
                            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                    })
                    .collect();
                seen.lock().expect("seen lock").push(SeenReq { method, path, headers });
                let body = r#"{"ok":true}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    MockUpstream { addr, seen, task }
}

// ---------------------------------------------------------------------------------------------
// Subprocess node (the real `rt` binary)
// ---------------------------------------------------------------------------------------------

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    l.local_addr().expect("addr").port()
}

pub struct ProcNode {
    pub child: Child,
    pub dir: tempfile::TempDir,
    pub public: SocketAddr,
    pub control: SocketAddr,
    pub metrics: SocketAddr,
    pub root: Option<String>,
    pub master_key_hex: String,
    http: reqwest::Client,
}

impl ProcNode {
    /// Start `rt serve` with a generated config. `root_token = false` omits `RT_ROOT_TOKEN`.
    pub fn spawn(root_token: bool, log: &str, tweak: impl FnOnce(&mut Config)) -> ProcNode {
        Self::spawn_with_env(root_token, log, tweak, &[])
    }

    /// [`ProcNode::spawn`] with more environment variables for the process.
    pub fn spawn_with_env(
        root_token: bool,
        log: &str,
        tweak: impl FnOnce(&mut Config),
        env: &[(&str, &str)],
    ) -> ProcNode {
        let dir = tempfile::tempdir().expect("temp data dir");
        let mut setup = NodeSetup::new(dir.path());
        setup.config.plans.insert("pro".to_string(), pro_plan());
        let (p, c, m) = (free_port(), free_port(), free_port());
        setup.config.listeners.public = format!("127.0.0.1:{p}");
        setup.config.listeners.control = format!("127.0.0.1:{c}");
        setup.config.listeners.metrics = format!("127.0.0.1:{m}");
        tweak(&mut setup.config);
        let cfg_path = dir.path().join("rt.toml");
        std::fs::write(&cfg_path, toml::to_string(&setup.config).expect("config serializes")).expect("write config");
        let out = std::fs::File::create(dir.path().join("stdout.log")).expect("stdout log");
        let err = std::fs::File::create(dir.path().join("stderr.log")).expect("stderr log");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rt"));
        cmd.arg("serve")
            .arg("--config")
            .arg(&cfg_path)
            .env_remove("RT_ROOT_TOKEN")
            .env_remove("RT_MASTER_KEY")
            .env("RT_MASTER_KEY", &setup.master_key_hex)
            .env("RT_LOG", log)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        if root_token {
            cmd.env("RT_ROOT_TOKEN", &setup.root_token);
        }
        cmd.envs(env.iter().copied());
        let child = cmd.spawn().expect("spawn rt");
        ProcNode {
            child,
            dir,
            public: format!("127.0.0.1:{p}").parse().expect("addr"),
            control: format!("127.0.0.1:{c}").parse().expect("addr"),
            metrics: format!("127.0.0.1:{m}").parse().expect("addr"),
            root: root_token.then(|| setup.root_token.clone()),
            master_key_hex: setup.master_key_hex.clone(),
            http: reqwest::Client::builder().no_proxy().build().expect("http client"),
        }
    }

    pub async fn wait_ready(&mut self, timeout: Duration) {
        let url = format!("http://{}/readyz", self.metrics);
        let start = Instant::now();
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!(
                    "rt exited early with {status}\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    self.stdout(),
                    self.stderr()
                );
            }
            if let Ok(r) = self.http.get(&url).send().await {
                if r.status().is_success() {
                    return;
                }
            }
            if start.elapsed() > timeout {
                panic!(
                    "rt did not become ready in {timeout:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    self.stdout(),
                    self.stderr()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn stdout(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("stdout.log")).unwrap_or_default()
    }

    pub fn stderr(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("stderr.log")).unwrap_or_default()
    }

    /// stdout + stderr (tracing writes JSON lines to stdout; panics and warnings may go to stderr).
    pub fn logs(&self) -> String {
        format!("{}\n{}", self.stdout(), self.stderr())
    }

    pub fn sigterm(&self) {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status();
    }

    pub async fn wait_exit(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let start = Instant::now();
        loop {
            if let Ok(Some(s)) = self.child.try_wait() {
                return Some(s);
            }
            if start.elapsed() > timeout {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Node for ProcNode {
    fn http(&self) -> &reqwest::Client {
        &self.http
    }
    fn control_url(&self, path: &str) -> String {
        format!("http://{}{}", self.control, path)
    }
    fn public_url(&self, path: &str) -> String {
        format!("http://{}{}", self.public, path)
    }
    fn metrics_url(&self, path: &str) -> String {
        format!("http://{}{}", self.metrics, path)
    }
    fn root_token(&self) -> &str {
        self.root.as_deref().unwrap_or("")
    }
}

impl Drop for ProcNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn secret(s: &str) -> Secret {
    Secret::new(s)
}

// ---------------------------------------------------------------------------------------------
// Guest sources (Deno, TypeScript)
// ---------------------------------------------------------------------------------------------

pub mod guests {
    /// E2E-01: `200 hi`.
    pub const HELLO: &str = r#"export default function (): Response { return new Response("hi"); }"#;

    /// Returns `v<n>` immediately.
    pub fn versioned(n: u32) -> String {
        format!(r#"export default function (): Response {{ return new Response("v{n}"); }}"#)
    }

    /// Returns `v<n>` after `ms` milliseconds (a timer, so it survives the frozen clock).
    pub fn slow(n: u32, ms: u32) -> String {
        format!(
            r#"export default async function (): Promise<Response> {{
  await new Promise((r) => setTimeout(r, {ms}));
  return new Response("v{n}");
}}"#
        )
    }

    /// Echoes the request headers and the handler context as JSON.
    pub const ECHO: &str = r#"
export default function (req: Request, ctx: any): Response {
  const headers: Record<string, string> = {};
  req.headers.forEach((v, k) => { headers[k] = v; });
  const url = new URL(req.url);
  return Response.json({
    headers,
    path: url.pathname,
    search: url.search,
    method: req.method,
    invoker: ctx.invoker ?? null,
    trigger: ctx.trigger,
    taskId: ctx.taskId,
    service: ctx.service,
  });
}"#;

    /// One `console.log("x")` per request (E2E-06, OBS-01/02).
    pub const LOGGER: &str = r#"
export default function (req: Request, ctx: any): Response {
  console.log("x");
  return new Response("ok");
}"#;

    /// `console.log` of the request's `?msg=` value, `console.error` for `?err=`.
    pub const LOG_PARAM: &str = r#"
export default function (req: Request): Response {
  const u = new URL(req.url);
  const m = u.searchParams.get("msg");
  if (m !== null) console.log(m);
  const e = u.searchParams.get("err");
  if (e !== null) console.error(e);
  return new Response("ok");
}"#;

    /// SQL notes: `POST` appends the body, `GET` lists (API-11, E2E-08).
    pub const NOTES: &str = r#"
import { sql } from "std:sql";
export default async function (req: Request): Promise<Response> {
  await sql.execute("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)");
  if (req.method === "POST") {
    const body = await req.text();
    await sql.execute("INSERT INTO notes (body) VALUES (?)", [body]);
    return new Response("ok", { status: 201 });
  }
  const r = await sql.execute("SELECT id, body FROM notes ORDER BY id");
  return Response.json({ notes: r.rows.map((row: any) => ({ id: row[0], body: row[1] })) });
}"#;

    /// KV: `PUT /k` stores the body, `GET /k` reads it back.
    pub const KV: &str = r#"
import { kv } from "std:kv";
export default async function (req: Request): Promise<Response> {
  const url = new URL(req.url);
  if (req.method === "PUT") { await kv.set(url.pathname, await req.text()); return new Response("ok"); }
  const v = await kv.get(url.pathname);
  return v === null || v === undefined ? new Response("missing", { status: 404 }) : new Response(String(v));
}"#;

    /// Imports a module that is not in the bundle: deploys, fails to build (E2E-05, API-10).
    pub const MISSING_IMPORT: &str = r#"
import { nothing } from "./missing.ts";
export default function (): Response { return new Response(String(nothing)); }"#;

    /// Form B recorder: `scheduled()` stores a row per run (start/finish), `fetch` lists them.
    pub const SCHEDULED_RECORDER: &str = r#"
import { sql } from "std:sql";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT, started INTEGER, finished INTEGER, payload TEXT)");
}
export default {
  async fetch(_req: Request): Promise<Response> {
    await ensure();
    const r = await sql.execute("SELECT id, started, finished, payload FROM runs ORDER BY id");
    return Response.json({ runs: r.rows.map((x: any) => ({ id: x[0], started: x[1], finished: x[2], payload: x[3] })) });
  },
  async scheduled(event: any, _ctx: any): Promise<void> {
    await ensure();
    const started = Date.now();
    await new Promise((r) => setTimeout(r, 500));
    await sql.execute("INSERT INTO runs (started, finished, payload) VALUES (?, ?, ?)", [started, Date.now(), JSON.stringify(event.payload)]);
  },
};"#;

    /// Form B queue consumer: stores `(body, attempt)` per delivery; `fetch` lists them.
    pub const QUEUE_RECORDER: &str = r#"
import { sql } from "std:sql";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS got (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT, attempt INTEGER)");
}
export default {
  async fetch(_req: Request): Promise<Response> {
    await ensure();
    const r = await sql.execute("SELECT body, attempt FROM got ORDER BY id");
    return Response.json({ got: r.rows.map((x: any) => ({ body: x[0], attempt: x[1] })) });
  },
  async queue(batch: any, _ctx: any): Promise<void> {
    await ensure();
    for (const m of batch.messages) {
      await sql.execute("INSERT INTO got (body, attempt) VALUES (?, ?)", [JSON.stringify(m.body), m.attempt]);
      m.ack();
    }
  },
};"#;

    /// A queue consumer that always fails (retry), for DLQ flows.
    pub const QUEUE_FAILING: &str = r#"
export default {
  async fetch(): Promise<Response> { return new Response("ok"); },
  async queue(batch: any): Promise<void> { for (const m of batch.messages) m.retry(); },
};"#;

    /// `std:services` caller: `/call?target=<slug>&path=/x`, `/delayed?wait=<ms>&target=<slug>`,
    /// `/recurse`. Failures of `call()` (response objects or exceptions) are reported as JSON.
    pub const CALLER: &str = r#"
import { call } from "std:services";
async function doCall(target: string, path: string): Promise<{ status: number; reason: string | null; body: string; thrown?: string }> {
  try {
    const res = await call(target, new Request("http://svc" + path));
    return { status: res.status, reason: res.headers.get("x-rt-reason"), body: await res.text() };
  } catch (e) {
    return { status: 0, reason: null, body: "", thrown: String((e as any)?.message ?? e) };
  }
}
export default async function (req: Request, ctx: any): Promise<Response> {
  const url = new URL(req.url);
  if (url.pathname === "/call") {
    return Response.json(await doCall(url.searchParams.get("target") ?? "hello", url.searchParams.get("path") ?? "/"));
  }
  if (url.pathname === "/delayed") {
    await new Promise((r) => setTimeout(r, Number(url.searchParams.get("wait") ?? 0)));
    return Response.json(await doCall(url.searchParams.get("target") ?? "hello", url.searchParams.get("path") ?? "/"));
  }
  if (url.pathname === "/recurse") {
    const depth = Number(url.searchParams.get("depth") ?? 0);
    const res = await doCall(ctx.service.slug, "/recurse?depth=" + (depth + 1));
    if (res.status !== 200) {
      return Response.json({ depth, deepest: depth, status: res.status, reason: res.reason, thrown: res.thrown ?? null });
    }
    const child = JSON.parse(res.body);
    return Response.json({ depth, deepest: child.deepest, status: child.status, reason: child.reason, thrown: child.thrown ?? null });
  }
  return new Response("not found", { status: 404 });
}"#;

    /// A queue consumer that records `(attempt, Date.now())` per delivery and retries (default
    /// backoff) until `attempt >= SUCCEED_ON` (an env var, `0` = never succeed).
    pub const QUEUE_ATTEMPTS: &str = r#"
import { sql } from "std:sql";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT, attempt INTEGER, at INTEGER)");
}
export default {
  async fetch(_req: Request): Promise<Response> {
    await ensure();
    const r = await sql.execute("SELECT msg, attempt, at FROM attempts ORDER BY id");
    return Response.json({ attempts: r.rows.map((x: any) => ({ msg: x[0], attempt: x[1], at: x[2] })) });
  },
  async queue(batch: any, _ctx: any): Promise<void> {
    await ensure();
    const succeedOn = Number(Deno.env.get("SUCCEED_ON") ?? "0");
    for (const m of batch.messages) {
      await sql.execute("INSERT INTO attempts (msg, attempt, at) VALUES (?, ?, ?)", [JSON.stringify(m.body), m.attempt, Date.now()]);
      if (succeedOn > 0 && m.attempt >= succeedOn) m.ack(); else m.retry();
    }
  },
};"#;

    /// Reads the secret `CANARY_SECRET` and reports only its length.
    pub const SECRET_READER: &str = r#"
export default function (): Response {
  const s = Deno.env.get("CANARY_SECRET");
  return new Response(s ? "len=" + s.length : "missing");
}"#;

    /// Triggers guest-visible errors from SQL, blob and egress; returns what the guest saw.
    pub const ERROR_PROBE: &str = r#"
import { sql } from "std:sql";
import { blob } from "std:blob";
async function attempt(f: () => Promise<unknown>): Promise<string> {
  try { await f(); return "no error"; }
  catch (e) { const a = e as any; return String(a?.code ?? "") + "|" + String(a?.message ?? e); }
}
export default async function (): Promise<Response> {
  return Response.json({
    sql: await attempt(() => sql.execute("SELECT * FROM table_that_does_not_exist")),
    attach: await attempt(() => sql.execute("ATTACH DATABASE '/etc/passwd' AS pw")),
    blob: await attempt(() => blob.get("../../../../etc/passwd")),
    egress: await attempt(() => fetch("http://127.0.0.1:1/")),
    egress_meta: await attempt(() => fetch("http://169.254.169.254/latest/meta-data/")),
  });
}"#;

    /// A SQL statement that runs far longer than `storage.sql_budget_ms` (SEC-29).
    pub const SQL_CROSS_JOIN: &str = r#"
import { sql } from "std:sql";
export default async function (): Promise<Response> {
  try {
    await sql.execute("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5000000) SELECT count(*) FROM c a, c b");
    return new Response("finished");
  } catch (e) {
    const a = e as any;
    return Response.json({ code: String(a?.code ?? ""), message: String(a?.message ?? e) }, { status: 500 });
  }
}"#;

    /// `platform.fetch("voicelab", …)`: `?path=` and `?method=`; sends its own `Authorization`.
    pub const PLATFORM_CALLER: &str = r#"
import { platform } from "std:platform";
export default async function (req: Request): Promise<Response> {
  const u = new URL(req.url);
  const path = u.searchParams.get("path") ?? "/v1/calls";
  const method = u.searchParams.get("method") ?? "POST";
  try {
    const res = await platform.fetch("voicelab", new Request("http://platform" + path, {
      method,
      headers: { authorization: "Bearer guest-supplied-token", "x-guest": "1" },
      body: method === "GET" ? undefined : "{}",
    }));
    return Response.json({ status: res.status, reason: res.headers.get("x-rt-reason") });
  } catch (e) {
    return Response.json({ thrown: String((e as Error)?.message ?? e) });
  }
}"#;

    /// The operator SDK `voicelab`: `call({ path, method })` is `platform.fetch("voicelab", …)` with its
    /// own `Authorization`; answers the binding's status and reason.
    pub const PLATFORM_SDK: &str = r#"
import { defineSdk } from "std:sdk/server";
import { platform } from "std:platform";
export default defineSdk({
  methods: {
    async call(args: { path: string; method: string }, ctx: any) {
      if (!ctx.caller) throw new Error("no caller");
      const res = await platform.fetch("voicelab", new Request("http://platform" + args.path, {
        method: args.method,
        headers: { authorization: "Bearer guest-supplied-token", "x-guest": "1" },
        body: args.method === "GET" ? undefined : "{}",
      }));
      return { status: res.status, reason: res.headers.get("x-rt-reason") };
    },
  },
});"#;

    /// A tenant service calling the `voicelab` SDK: `?path=` and `?method=`.
    pub const PLATFORM_SDK_CALLER: &str = r#"
import { voicelab } from "std:sdk";
export default async function (req: Request): Promise<Response> {
  const u = new URL(req.url);
  try {
    return Response.json(await voicelab.call({ path: u.searchParams.get("path") ?? "/v1/calls", method: u.searchParams.get("method") ?? "POST" }));
  } catch (e) {
    return Response.json({ thrown: String((e as any)?.code ?? e) });
  }
}"#;

    /// Writes a lot of log lines (OBS-03 style); `?n=` lines.
    pub const LOG_FLOOD: &str = r#"
export default function (req: Request): Response {
  const n = Number(new URL(req.url).searchParams.get("n") ?? 100);
  for (let i = 0; i < n; i++) console.log("line " + i);
  return new Response("done");
}"#;
}

// ---------------------------------------------------------------------------------------------
// Server-Sent Events
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct SseEvent {
    pub id: Option<String>,
    pub event: Option<String>,
    pub data: String,
}

impl SseEvent {
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.data).unwrap_or(Value::Null)
    }
}

/// A streaming SSE response read event by event.
pub struct Sse {
    resp: reqwest::Response,
    buf: String,
    pub status: u16,
    pub headers: HeaderMap,
}

impl Sse {
    pub async fn open(rb: RequestBuilder) -> Sse {
        let resp = rb
            .header("accept", "text/event-stream")
            .send()
            .await
            .expect("open SSE stream");
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        Sse {
            resp,
            buf: String::new(),
            status,
            headers,
        }
    }

    fn take_event(&mut self) -> Option<SseEvent> {
        loop {
            let end = self.buf.find("\n\n")?;
            let block: String = self.buf.drain(..end + 2).collect();
            let mut ev = SseEvent::default();
            let mut any = false;
            for line in block.lines() {
                if line.starts_with(':') || line.is_empty() {
                    continue;
                }
                let (k, v) = line.split_once(':').unwrap_or((line, ""));
                let v = v.strip_prefix(' ').unwrap_or(v);
                match k {
                    "id" => ev.id = Some(v.to_string()),
                    "event" => ev.event = Some(v.to_string()),
                    "data" => {
                        if any && !ev.data.is_empty() {
                            ev.data.push('\n');
                        }
                        ev.data.push_str(v);
                    }
                    _ => continue,
                }
                any = true;
            }
            if any {
                return Some(ev);
            }
        }
    }

    /// The next event, or `None` on timeout / end of stream.
    pub async fn next(&mut self, timeout: Duration) -> Option<SseEvent> {
        let deadline = Instant::now() + timeout;
        loop {
            self.buf = self.buf.replace("\r\n", "\n");
            if let Some(ev) = self.take_event() {
                return Some(ev);
            }
            let left = deadline.checked_duration_since(Instant::now())?;
            match tokio::time::timeout(left, self.resp.chunk()).await {
                Ok(Ok(Some(c))) => self.buf.push_str(&String::from_utf8_lossy(&c)),
                _ => return None,
            }
        }
    }

    /// Read events until `n` have arrived or `timeout` passes.
    pub async fn take(&mut self, n: usize, timeout: Duration) -> Vec<SseEvent> {
        let deadline = Instant::now() + timeout;
        let mut out = Vec::new();
        while out.len() < n {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match self.next(left).await {
                Some(e) => out.push(e),
                None => break,
            }
        }
        out
    }
}

/// `p`-th percentile (0..=100) of `samples` (nearest rank).
pub fn percentile(mut samples: Vec<f64>, p: f64) -> f64 {
    assert!(!samples.is_empty(), "no samples");
    samples.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    let rank = ((p / 100.0) * samples.len() as f64).ceil().max(1.0) as usize;
    samples[rank.min(samples.len()) - 1]
}

/// One request on a shared client: `(status, body, x-rt-version, elapsed_ms)`.
pub async fn fetch(client: reqwest::Client, url: String) -> (u16, String, Option<u32>, f64) {
    let start = Instant::now();
    let r = client.get(&url).send().await.expect("request");
    let status = r.status().as_u16();
    let version = r
        .headers()
        .get("x-rt-version")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let body = r.text().await.unwrap_or_default();
    (status, body, version, start.elapsed().as_secs_f64() * 1000.0)
}

// ---------------------------------------------------------------------------------------------
// Operator SDKs
// ---------------------------------------------------------------------------------------------

/// An operator SDK (TypeScript) answering `ping` with `{ pong, version, caller }`.
pub fn sdk_source(pong: &str) -> String {
    format!(
        r#"
import {{ defineSdk }} from "std:sdk/server";
export interface Pong {{ pong: string; version: string; caller: unknown }}
export default defineSdk({{
  methods: {{
    async ping(_args: void, ctx: any): Promise<Pong> {{
      return {{ pong: "{pong}", version: ctx.caller.sdk.version, caller: ctx.caller }};
    }},
    async echo(args: unknown, ctx: any): Promise<unknown> {{ return ctx.caller ? args : null; }},
    async fail(_a: void, ctx: any) {{ if (ctx.caller) {{ const e: any = new Error("no stock left"); e.code = "out_of_stock"; throw e; }} }},
    async slow(args: {{ ms: number }}, ctx: any) {{ if (ctx.caller) await new Promise((r) => setTimeout(r, args.ms)); return "late"; }},
    async burn(args: {{ n: number }}, ctx: any) {{ let x = 0; const n = ctx.caller ? args.n : 0; for (let i = 0; i < n; i++) {{ x = (x * 31 + i) | 0; }} return x !== 1; }},
  }},
}});
"#
    )
}

/// `POST /v1/sdks` (operator): publish `version` of the SDK `name` from TypeScript source.
pub async fn publish_sdk(node: &impl Node, name: &str, version: &str, src: &str, channel: Option<&str>) -> Resp {
    use base64::Engine as _;
    let mut body = json!({
        "name": name,
        "version": version,
        "content_type": "application/typescript",
        "source": base64::engine::general_purpose::STANDARD.encode(src.as_bytes()),
    });
    if let Some(c) = channel {
        body["channel"] = json!(c);
    }
    call(root(node, Method::POST, "/v1/sdks"), Some(body)).await
}

/// `PUT /v1/sdk-grants` (operator).
pub async fn put_grant(node: &impl Node, grant: Value) -> Resp {
    call(root(node, Method::PUT, "/v1/sdk-grants"), Some(grant)).await
}

// ---------------------------------------------------------------------------------------------
// WebSockets
// ---------------------------------------------------------------------------------------------

pub type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
pub use tokio_tungstenite::tungstenite::Message as WsMsg;

/// Open a WebSocket to `/s/{slug}{path}` on the public listener with extra request headers.
/// `Err(status)` when the server answered the upgrade with a plain HTTP response.
pub async fn ws_connect_with(node: &impl Node, slug: &str, path: &str, headers: &[(&str, &str)]) -> Result<Ws, u16> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let url = node
        .public_url(&format!("/s/{slug}{path}"))
        .replacen("http://", "ws://", 1);
    let mut req = url.into_client_request().expect("ws request");
    for (k, v) in headers {
        req.headers_mut().insert(
            reqwest::header::HeaderName::from_bytes(k.as_bytes()).expect("header name"),
            reqwest::header::HeaderValue::from_str(v).expect("header value"),
        );
    }
    match tokio::time::timeout(Duration::from_secs(20), tokio_tungstenite::connect_async(req)).await {
        Ok(Ok((ws, _))) => Ok(ws),
        Ok(Err(tokio_tungstenite::tungstenite::Error::Http(resp))) => Err(resp.status().as_u16()),
        Ok(Err(e)) => panic!("websocket connect to {slug}{path}: {e}"),
        Err(_) => panic!("websocket connect to {slug}{path} timed out"),
    }
}

pub async fn ws_connect(node: &impl Node, slug: &str, path: &str) -> Ws {
    ws_connect_with(node, slug, path, &[])
        .await
        .unwrap_or_else(|s| panic!("upgrade of {slug}{path} answered {s}"))
}

/// The next data or close frame (pings and pongs are skipped); `None` when the stream ended or
/// nothing came within `wait`.
pub async fn ws_next(ws: &mut Ws, wait: Duration) -> Option<WsMsg> {
    use futures_util::StreamExt;
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(WsMsg::Ping(_) | WsMsg::Pong(_)))) => continue,
            Ok(Some(Ok(m))) => return Some(m),
            _ => return None,
        }
    }
}

/// The next text frame, panicking on anything else.
pub async fn ws_text(ws: &mut Ws) -> String {
    match ws_next(ws, Duration::from_secs(20)).await {
        Some(WsMsg::Text(t)) => t.to_string(),
        other => panic!("expected a text frame, got {other:?}"),
    }
}

pub async fn ws_send(ws: &mut Ws, text: &str) {
    use futures_util::SinkExt;
    ws.send(WsMsg::text(text)).await.expect("websocket send");
}

/// The close code the server sent (`None`: the stream ended without a close frame).
pub async fn ws_close_code(ws: &mut Ws, wait: Duration) -> Option<u16> {
    loop {
        match ws_next(ws, wait).await {
            Some(WsMsg::Close(Some(f))) => return Some(u16::from(f.code)),
            Some(WsMsg::Close(None)) | None => return None,
            Some(_) => continue,
        }
    }
}
