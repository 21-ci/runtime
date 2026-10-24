//! `NodeHarness`: start a whole node in-process on random ports with a temp data dir and talk
//! to it over HTTP.
//!
//! `rt-testkit` deliberately does **not** depend on `rt-server` (that would link V8 and wasmtime
//! into every crate's test binaries). Instead the booting is injected:
//!
//! ```ignore
//! // in crates/rt-server/tests/*.rs
//! let node = NodeHarness::start(|setup| async move {
//!     let running = rt_server::start(setup.config, setup.secrets).await?;
//!     Ok::<_, anyhow::Error>(Booted {
//!         addrs: running.addrs().into(),
//!         shutdown: Box::new(move || Box::pin(async move { running.shutdown().await })),
//!     })
//! }).await?;
//! let r = node.invoke("hello", Method::GET, "/", "").await;
//! ```

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use bytes::Bytes;
use reqwest::{Method, StatusCode};
use rt_config::{Config, NodeSecrets, Secret};
use serde_json::{json, Value};
use tempfile::TempDir;

/// Everything a boot function needs: a ready-to-use config (temp data dir, `127.0.0.1:0`
/// listeners, MVP security defaults) and the node secrets.
pub struct NodeSetup {
    pub config: Config,
    pub secrets: NodeSecrets,
    pub root_token: String,
    pub master_key_hex: String,
    pub data_dir: PathBuf,
}

impl NodeSetup {
    /// Test configuration rooted at `data_dir`: listeners on `127.0.0.1:0` and a Unix socket for
    /// the internal listener, `security.seccomp=false`, `security.require_v8_sandbox=false`
    /// (prebuilt V8), small thread counts.
    pub fn new(data_dir: &Path) -> Self {
        let mut config = Config::default();
        config.node.data_dir = data_dir.display().to_string();
        config.listeners.public = "127.0.0.1:0".into();
        config.listeners.control = "127.0.0.1:0".into();
        config.listeners.metrics = "127.0.0.1:0".into();
        config.listeners.internal = format!("unix:{}", data_dir.join("rt.sock").display());
        config.security.seccomp = false;
        config.security.require_v8_sandbox = false;
        config.sched.processors = 4;
        config.io.threads = 1;
        config.blocking.threads = 2;
        let root_token = format!("test-root-{}", rt_types::TenantId::new().bare());
        let master_key_hex = {
            use rand::RngCore;
            let mut b = [0u8; 32];
            rand::rngs::OsRng.fill_bytes(&mut b);
            hex::encode(b)
        };
        let secrets = NodeSecrets {
            root_token: Some(Secret::new(root_token.clone())),
            master_key: Some(Secret::new(master_key_hex.clone())),
            master_key_previous: None,
        };
        Self {
            config,
            secrets,
            root_token,
            master_key_hex,
            data_dir: data_dir.to_path_buf(),
        }
    }
}

/// Addresses the booted node listens on.
#[derive(Clone, Debug)]
pub struct NodeAddrs {
    pub public: SocketAddr,
    pub control: SocketAddr,
    pub metrics: SocketAddr,
    /// Unix socket of the internal listener.
    pub internal: Option<PathBuf>,
}

pub type ShutdownFn = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// What a boot function returns.
pub struct Booted {
    pub addrs: NodeAddrs,
    pub shutdown: ShutdownFn,
}

pub struct NodeHarness {
    _tmp: TempDir,
    pub addrs: NodeAddrs,
    pub root_token: String,
    pub master_key_hex: String,
    http: reqwest::Client,
    shutdown: Option<ShutdownFn>,
}

impl NodeHarness {
    /// Create the temp dir and setup, call `boot`, and connect.
    pub async fn start<F, Fut, E>(boot: F) -> Result<Self, E>
    where
        F: FnOnce(NodeSetup) -> Fut,
        Fut: Future<Output = Result<Booted, E>>,
    {
        Self::start_with(boot, |_| {}).await
    }

    /// Like [`NodeHarness::start`] with a hook to tweak the config (profile, plans, limits).
    pub async fn start_with<F, Fut, E>(boot: F, tweak: impl FnOnce(&mut Config)) -> Result<Self, E>
    where
        F: FnOnce(NodeSetup) -> Fut,
        Fut: Future<Output = Result<Booted, E>>,
    {
        let tmp = tempfile::tempdir().expect("create temp data dir");
        let mut setup = NodeSetup::new(tmp.path());
        tweak(&mut setup.config);
        let root_token = setup.root_token.clone();
        let master_key_hex = setup.master_key_hex.clone();
        let booted = boot(setup).await?;
        let http = reqwest::Client::builder().no_proxy().build().expect("http client");
        Ok(Self {
            _tmp: tmp,
            addrs: booted.addrs,
            root_token,
            master_key_hex,
            http,
            shutdown: Some(booted.shutdown),
        })
    }

    pub fn data_dir(&self) -> &Path {
        self._tmp.path()
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    pub fn control_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addrs.control, path)
    }

    pub fn public_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addrs.public, path)
    }

    pub fn metrics_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addrs.metrics, path)
    }

    // ---- control API helpers ---------------------------------------------------------------

    /// A control request authenticated with the operator token.
    pub fn control(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, self.control_url(path))
            .bearer_auth(&self.root_token)
    }

    /// Send a control request with an optional JSON body; returns `(status, json body)`.
    pub async fn control_json(&self, method: Method, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        let mut rb = self.control(method, path);
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        let resp = rb.send().await.expect("control request");
        let status = resp.status();
        let json = resp.json::<Value>().await.unwrap_or(Value::Null);
        (status, json)
    }

    /// `POST /v1/tenants`; returns the tenant JSON (panics on non-2xx).
    pub async fn create_tenant(&self, name: &str, plan: &str) -> Value {
        let (st, v) = self
            .control_json(Method::POST, "/v1/tenants", Some(json!({"name": name, "plan": plan})))
            .await;
        assert!(st.is_success(), "create tenant failed: {st} {v}");
        v
    }

    /// `POST /v1/tenants/{t}/keys`; returns the full `rtk_…` secret.
    pub async fn create_key(&self, tenant_id: &str, scopes: &[&str]) -> String {
        let (st, v) = self
            .control_json(
                Method::POST,
                &format!("/v1/tenants/{tenant_id}/keys"),
                Some(json!({"scopes": scopes})),
            )
            .await;
        assert!(st.is_success(), "create key failed: {st} {v}");
        v["secret"].as_str().expect("secret in response").to_string()
    }

    /// Create a service as the operator on behalf of `tenant_id` (`x-rt-tenant`); returns its JSON.
    pub async fn create_service(&self, tenant_id: &str, slug: &str, runtime: &str) -> Value {
        let resp = self
            .control(Method::POST, "/v1/services")
            .header("x-rt-tenant", tenant_id)
            .json(&json!({"slug": slug, "runtime": runtime}))
            .send()
            .await
            .expect("create service request");
        let st = resp.status();
        let v = resp.json::<Value>().await.unwrap_or(Value::Null);
        assert!(st.is_success(), "create service failed: {st} {v}");
        v
    }

    /// Upload a version (`POST /v1/services/{s}/versions`); returns `(status, json)`.
    pub async fn deploy(
        &self,
        tenant_id: &str,
        service_id: &str,
        content_type: &str,
        body: impl Into<Bytes>,
        activate: bool,
    ) -> (StatusCode, Value) {
        let path = format!(
            "/v1/services/{service_id}/versions?activate={}",
            if activate { 1 } else { 0 }
        );
        let resp = self
            .control(Method::POST, &path)
            .header("x-rt-tenant", tenant_id)
            .header("content-type", content_type)
            .body(body.into())
            .send()
            .await
            .expect("deploy request");
        let st = resp.status();
        (st, resp.json::<Value>().await.unwrap_or(Value::Null))
    }

    // ---- invoke / observability helpers ------------------------------------------------------

    /// `ANY /s/{slug}{path}` on the public listener.
    pub async fn invoke(&self, slug: &str, method: Method, path: &str, body: impl Into<Bytes>) -> reqwest::Response {
        let url = self.public_url(&format!("/s/{slug}{path}"));
        self.http
            .request(method, url)
            .body(body.into())
            .send()
            .await
            .expect("invoke request")
    }

    /// Scrape `/metrics`.
    pub async fn scrape_metrics(&self) -> String {
        self.http
            .get(self.metrics_url("/metrics"))
            .send()
            .await
            .expect("scrape")
            .text()
            .await
            .expect("metrics body")
    }

    /// Graceful shutdown.
    pub async fn shutdown(mut self) {
        if let Some(f) = self.shutdown.take() {
            f().await;
        }
    }
}
