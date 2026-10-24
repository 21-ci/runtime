//! Guarded outbound HTTP (SECURITY-CRITICAL; ported from the worker's `egress_guard.rs`,
//! see SECURITY.md "Egress / SSRF guard").
//!
//! Every outbound request from guest code — `fetch` in Deno, `wasi:http/outgoing-handler` in
//! WASM — goes through an [`EgressGuard`]. Guarantees:
//!
//! 1. **Schemes** `http`/`https` only, re-checked per redirect hop.
//! 2. **Ports** in `egress.denied_ports` (25, 465, 587) denied.
//! 3. **Host rules**: per-service `allowed_hosts` (non-empty ⇒ allowlist), `denied_hosts`
//!    (service + node), exact or dot-suffix match.
//! 4. **DNS**: resolve once, validate *every* address with `rt_sec::is_blocked_ip` and the
//!    self-denylist, connect to the validated IP (no rebinding).
//! 5. **Redirects** followed by the guard (max 5), re-validated per hop; 303 → GET; an
//!    `https` → `http` downgrade is refused unless `egress.allow_https_downgrade` (SEC-50).
//! 6. **Caps**: per-task fetch count and concurrency (`EgressBudget`), connect 10 s, total = task
//!    wall, **decompressed** response bytes ≤ cap (inflate ratio > 100 aborts, SEC-30), daily
//!    egress quota (`egress_quota`).
//! 7. **Connection reuse** (SEC-52): one pinned client per `(tenant, scheme, host, port, pinned
//!    IP)`, kept in a bounded LRU with an idle timeout. A connection therefore only ever carries
//!    requests of one tenant, and only to the address that hop validated.
//!
//! The builder's module fetches use the same guard with `build.allowed_hosts`. They run for no
//! tenant (`tenant = None` in the pool key): build fetches share connections with each other
//! (anonymous `GET`s of public modules, no credentials), never with a tenant's guest fetches.

use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use http::{header, HeaderMap, HeaderName, HeaderValue};
use parking_lot::Mutex;
use rt_sec::{host_allowed, host_denied, is_blocked_ip, EgressDenylist};
use rt_types::{Body, BodyError, EgressRules, Limits, TenantId};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::{Host, Url};

pub type Request = http::Request<Body>;
pub type Response = http::Response<Body>;

/// A decompressed response may be at most this many times larger than its compressed form
/// (SECURITY.md).
const MAX_INFLATE_RATIO: u64 = 100;
/// Bytes of free expansion before the ratio rule applies (tiny bodies compress extremely well).
const INFLATE_SLACK: u64 = 64 * 1024;
/// Decoded output is handed to the guest in frames of at most this size.
const FRAME: usize = 64 * 1024;

/// Stable, sanitized failure codes (guests see only these; SEC-26).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EgressError {
    /// Blocked by scheme/port/host/IP rules. Also logged as an egress event.
    #[error("egress_denied")]
    Denied,
    /// Per-task fetch count exhausted.
    #[error("egress_limit")]
    Limit,
    /// Response (decompressed) over the cap, or inflate ratio > 100.
    #[error("egress_too_large")]
    TooLarge,
    /// Daily `egress_mb_per_day` exhausted.
    #[error("egress_quota")]
    Quota,
    /// Connect timeout or the task's wall deadline.
    #[error("egress_timeout")]
    Timeout,
    /// Name did not resolve.
    #[error("egress_dns")]
    Dns,
    /// Connection/TLS/protocol failure (sanitized message).
    #[error("egress_upstream")]
    Upstream,
    /// An `https` URL redirected to plain `http` and `egress.allow_https_downgrade` is off
    /// (SEC-50). Also logged as an egress event.
    #[error("egress_insecure_redirect")]
    InsecureRedirect,
}

impl EgressError {
    /// Stable code for guest-visible errors and `rt_egress_requests_total{result}`.
    pub fn code(&self) -> &'static str {
        match self {
            EgressError::Denied => "egress_denied",
            EgressError::Limit => "egress_limit",
            EgressError::TooLarge => "egress_too_large",
            EgressError::Quota => "egress_quota",
            EgressError::Timeout => "egress_timeout",
            EgressError::Dns => "egress_dns",
            EgressError::Upstream => "egress_upstream",
            EgressError::InsecureRedirect => "egress_insecure_redirect",
        }
    }
}

/// Process-wide guard settings.
#[derive(Clone, Debug)]
pub struct EgressGuardConfig {
    pub max_redirects: usize,
    /// Cap on decompressed response bytes (≤ 100 MB).
    pub max_response_bytes: u64,
    /// Cap on request body bytes.
    pub max_request_bytes: u64,
    pub connect_timeout: Duration,
    /// `egress.denied_ports`.
    pub denied_ports: Vec<u16>,
    /// `egress.denied_hosts` (node-wide).
    pub denied_hosts: Vec<String>,
    /// `egress.allow_https_downgrade`: follow an `https` → `http` redirect. Off by default: the
    /// fetch fails with [`EgressError::InsecureRedirect`].
    pub allow_https_downgrade: bool,
    /// Pooled clients (and their idle connections) unused for this long are dropped.
    pub pool_idle_timeout: Duration,
    /// Pinned clients kept for connection reuse (least recently used evicted first). `0` = no
    /// reuse: a fresh client per hop.
    pub max_pooled_clients: usize,
}

impl Default for EgressGuardConfig {
    fn default() -> Self {
        Self {
            max_redirects: 5,
            max_response_bytes: 100 * 1024 * 1024,
            max_request_bytes: 10 * 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            denied_ports: vec![25, 465, 587],
            denied_hosts: Vec::new(),
            allow_https_downgrade: false,
            pool_idle_timeout: Duration::from_secs(30),
            max_pooled_clients: 256,
        }
    }
}

/// Per-service policy.
#[derive(Clone, Debug, Default)]
pub struct EgressPolicy {
    /// Non-empty ⇒ only these hosts (exact or dot-suffix).
    pub allowed_hosts: Vec<String>,
    pub denied_hosts: Vec<String>,
    /// Plan `egress_default = "deny"`: with an empty `allowed_hosts`, everything is denied.
    pub default_deny: bool,
}

impl EgressPolicy {
    pub fn from_rules(rules: &EgressRules, default_deny: bool) -> Self {
        Self {
            allowed_hosts: rules.allowed_hosts.clone(),
            denied_hosts: rules.denied_hosts.clone(),
            default_deny,
        }
    }

    /// Policy used by the builder's module fetcher (`build.allowed_hosts`).
    pub fn for_build(allowed_hosts: Vec<String>) -> Self {
        Self {
            allowed_hosts,
            denied_hosts: Vec::new(),
            default_deny: false,
        }
    }
}

/// Live byte accounting of one task, shared by all of its fetches and by the response bodies that
/// are still streaming after `send` returned.
#[derive(Debug)]
pub struct EgressUsage {
    received: AtomicU64,
    /// Remaining daily quota; `u64::MAX` = unlimited.
    quota_left: AtomicU64,
}

impl EgressUsage {
    pub fn new(quota_bytes_left: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            received: AtomicU64::new(0),
            quota_left: AtomicU64::new(quota_bytes_left.unwrap_or(u64::MAX)),
        })
    }

    /// Wire bytes received so far.
    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }

    /// Remaining quota (`None` = unlimited).
    pub fn quota_left(&self) -> Option<u64> {
        match self.quota_left.load(Ordering::Relaxed) {
            u64::MAX => None,
            n => Some(n),
        }
    }

    /// Charge `n` received bytes; `false` once the quota is exhausted (the bytes still count as
    /// received).
    pub fn consume(&self, n: u64) -> bool {
        self.received.fetch_add(n, Ordering::Relaxed);
        let mut cur = self.quota_left.load(Ordering::Relaxed);
        loop {
            if cur == u64::MAX {
                return true;
            }
            if cur < n {
                self.quota_left.store(0, Ordering::Relaxed);
                return false;
            }
            match self
                .quota_left
                .compare_exchange_weak(cur, cur - n, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
}

/// Per-task egress accounting (one per `HostHandle`).
#[derive(Debug)]
pub struct EgressBudget {
    /// Fetches still allowed (`limits.max_fetches`). Decremented once per `send` (a redirect
    /// chain is one fetch).
    pub fetches_left: u32,
    /// Concurrent outbound requests (`limits.max_concurrent_fetches`).
    pub concurrency: Arc<Semaphore>,
    /// Bytes received so far (counted toward `egress_mb_per_day` by the caller). A snapshot taken
    /// when `send` returns; bodies still streaming keep counting in [`EgressBudget::usage`].
    pub bytes_received: u64,
    /// Remaining daily quota in bytes, if the plan has one (snapshot, see above).
    pub quota_bytes_left: Option<u64>,
    /// Task wall deadline: total time for the call incl. redirects.
    pub deadline: Instant,
    /// Live counters (shared with streaming response bodies).
    pub usage: Arc<EgressUsage>,
    /// Connection-reuse scope: pooled connections are only shared between fetches of the same
    /// tenant (`None` = node-owned fetches: builder, platform bindings).
    pub tenant: Option<TenantId>,
}

impl EgressBudget {
    pub fn new(limits: &Limits, deadline: Instant, quota_bytes_left: Option<u64>) -> Self {
        Self {
            fetches_left: limits.max_fetches,
            concurrency: Arc::new(Semaphore::new(limits.max_concurrent_fetches as usize)),
            bytes_received: 0,
            quota_bytes_left,
            deadline,
            usage: EgressUsage::new(quota_bytes_left),
            tenant: None,
        }
    }

    /// Budget view over state shared between the concurrent fetches of one task.
    pub fn shared(fetches_left: u32, concurrency: Arc<Semaphore>, usage: Arc<EgressUsage>, deadline: Instant) -> Self {
        Self {
            fetches_left,
            concurrency,
            bytes_received: usage.received(),
            quota_bytes_left: usage.quota_left(),
            deadline,
            usage,
            tenant: None,
        }
    }

    /// Scope connection reuse to `tenant` (every guest fetch sets it).
    pub fn for_tenant(mut self, tenant: TenantId) -> Self {
        self.tenant = Some(tenant);
        self
    }
}

/// A byte stream the egress guard opened for a guest (a WebSocket's TCP or TLS connection).
pub trait EgressIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> EgressIo for T {}

#[async_trait]
pub trait EgressGuard: Send + Sync {
    /// Validate and send `req`. Redirects are followed internally.
    async fn send(
        &self,
        req: Request,
        policy: &EgressPolicy,
        budget: &mut EgressBudget,
    ) -> Result<Response, EgressError>;

    /// Open the connection of an outbound WebSocket (`ws:` / `wss:` URL) after the same checks as
    /// [`Self::send`] (scheme, port, host lists, one DNS lookup, every address public): TCP to
    /// the validated address, TLS for `wss:`. No redirects.
    async fn dial(&self, url: &Url, policy: &EgressPolicy) -> Result<Box<dyn EgressIo>, EgressError> {
        let _ = (url, policy);
        Err(EgressError::Denied)
    }
}

/// The TLS client of outbound WebSockets: webpki roots, the `ring` provider (named explicitly:
/// the process links more than one, docs/plan/deps.md).
fn ws_tls() -> Option<Arc<tokio_rustls::rustls::ClientConfig>> {
    static CFG: std::sync::OnceLock<Option<Arc<tokio_rustls::rustls::ClientConfig>>> = std::sync::OnceLock::new();
    CFG.get_or_init(|| {
        let roots = tokio_rustls::rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        tokio_rustls::rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .ok()
            .map(|b| Arc::new(b.with_root_certificates(roots).with_no_client_auth()))
    })
    .clone()
}

/// Name resolution used by [`GuardedEgress`]: **one** lookup per hop, every returned address is
/// validated, and the connection goes to a validated address (no rebinding window).
/// Injectable so tests can simulate DNS rebinding and mixed public/private answers (SEC-10).
#[async_trait]
pub trait Resolver: Send + Sync {
    /// All addresses (IPv4 and IPv6) of `host`; `Err` maps to [`EgressError::Dns`].
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<std::net::IpAddr>>;
}

/// Default [`Resolver`]: the system resolver (`tokio::net::lookup_host`).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemResolver;

#[async_trait]
impl Resolver for SystemResolver {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<std::net::IpAddr>> {
        let mut out: Vec<IpAddr> = Vec::new();
        for addr in tokio::net::lookup_host((host, 0)).await? {
            if !out.contains(&addr.ip()) {
                out.push(addr.ip());
            }
        }
        Ok(out)
    }
}

/// **Test seam**, never set in production: maps a *validated* address to the address that is
/// actually dialed (right before connecting). Tests classify documentation-range "public" IPs
/// and dial a loopback mock server instead; the SSRF decision is still made on the original IP.
pub type ConnectRewrite = Arc<dyn Fn(std::net::IpAddr) -> std::net::IpAddr + Send + Sync>;

/// Idle connections kept per pooled client (one client serves one origin at one address).
const POOL_IDLE_PER_CLIENT: usize = 8;

/// What a pooled client is bound to. Everything that decides *where* and *for whom* a connection
/// goes is part of the key, so a cached connection is never reused for another tenant or for an
/// address that was not validated for this hop.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClientKey {
    tenant: Option<TenantId>,
    https: bool,
    /// Lower-cased domain, or the literal address as written in the URL.
    host: String,
    port: u16,
    pinned: IpAddr,
    /// Equals `pinned` outside tests ([`ConnectRewrite`]).
    dial: IpAddr,
}

struct PooledClient {
    client: reqwest::Client,
    last_used: Instant,
}

/// Bounded LRU of pinned clients with an idle timeout.
struct ClientPool {
    max: usize,
    idle: Duration,
    clients: Mutex<HashMap<ClientKey, PooledClient>>,
}

impl ClientPool {
    fn new(max: usize, idle: Duration) -> Self {
        Self {
            max,
            idle,
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn len(&self) -> usize {
        self.clients.lock().len()
    }

    /// The cached client for `key`, or a new one from `build` (built outside the lock: a TLS
    /// client takes milliseconds to set up).
    fn get_or_build(
        &self,
        key: ClientKey,
        build: impl FnOnce() -> Result<reqwest::Client, EgressError>,
    ) -> Result<reqwest::Client, EgressError> {
        {
            let mut clients = self.clients.lock();
            let now = Instant::now();
            if let Some(c) = clients.get_mut(&key) {
                if now.saturating_duration_since(c.last_used) < self.idle {
                    c.last_used = now;
                    return Ok(c.client.clone());
                }
                clients.remove(&key);
            }
        }
        let client = build()?;
        let mut clients = self.clients.lock();
        let now = Instant::now();
        clients.retain(|_, c| now.saturating_duration_since(c.last_used) < self.idle);
        if !clients.contains_key(&key) {
            while clients.len() >= self.max {
                let Some(oldest) = clients.iter().min_by_key(|(_, c)| c.last_used).map(|(k, _)| k.clone()) else {
                    break;
                };
                clients.remove(&oldest);
            }
        }
        // A concurrent miss may have inserted the same key meanwhile: keep one client.
        let entry = clients.entry(key).or_insert(PooledClient { client, last_used: now });
        entry.last_used = now;
        Ok(entry.client.clone())
    }
}

/// `reqwest`-based guard: pinned-IP clients (pooled per tenant and validated address), no
/// proxies, no automatic redirects.
pub struct GuardedEgress {
    cfg: EgressGuardConfig,
    denylist: Arc<dyn EgressDenylist>,
    resolver: Arc<dyn Resolver>,
    rewrite: Option<ConnectRewrite>,
    /// Operator-trusted upstreams (platform bindings): skip the address classification. Never set
    /// on the guard guests use.
    trusted: bool,
    metrics: Option<Arc<rt_obs::Metrics>>,
    pool: ClientPool,
}

impl GuardedEgress {
    pub fn new(cfg: EgressGuardConfig, denylist: Arc<dyn EgressDenylist>) -> Self {
        let pool = ClientPool::new(cfg.max_pooled_clients, cfg.pool_idle_timeout);
        Self {
            cfg,
            denylist,
            resolver: Arc::new(SystemResolver),
            rewrite: None,
            trusted: false,
            metrics: None,
            pool,
        }
    }

    /// Pinned clients currently cached for connection reuse.
    pub fn pooled_clients(&self) -> usize {
        self.pool.len()
    }

    /// Use `resolver` instead of [`SystemResolver`].
    pub fn with_resolver(mut self, resolver: Arc<dyn Resolver>) -> Self {
        self.resolver = resolver;
        self
    }

    /// Install the [`ConnectRewrite`] test seam.
    pub fn with_connect_rewrite(mut self, rewrite: ConnectRewrite) -> Self {
        self.rewrite = Some(rewrite);
        self
    }

    /// Count requests (`rt_egress_requests_total{result}`) and bytes (`rt_egress_bytes_total`).
    pub fn with_metrics(mut self, metrics: Arc<rt_obs::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// A guard for **operator-configured upstreams** (platform bindings): scheme, port,
    /// host-list, redirect and size rules still apply, but private/internal addresses are
    /// reachable because the operator named the endpoint. Must never be handed to guest code.
    pub fn for_trusted_endpoints(cfg: EgressGuardConfig, denylist: Arc<dyn EgressDenylist>) -> Self {
        let mut g = Self::new(cfg, denylist);
        g.cfg.max_redirects = 0;
        g.trusted = true;
        g
    }

    /// Validate a binding/connector endpoint (`host:port`) the same way a guest request would be:
    /// resolve once and reject if any address is blocked (SEC-24 for operator-configured URLs).
    pub async fn validate_endpoint(&self, host: &str, port: u16) -> Result<(), EgressError> {
        // `egress.denied_ports` binds operator endpoints too (EGR-017).
        if self.cfg.denied_ports.contains(&port) {
            return Err(EgressError::Denied);
        }
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        let ips: Vec<IpAddr> = match bare.parse::<IpAddr>() {
            Ok(ip) => vec![ip],
            Err(_) => self.lookup(bare).await?,
        };
        self.check_addrs(bare, &ips, false)?;
        Ok(())
    }

    async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, EgressError> {
        let resolved = tokio::time::timeout(self.cfg.connect_timeout, self.resolver.resolve(host))
            .await
            .map_err(|_| EgressError::Timeout)?;
        match resolved {
            Ok(ips) if !ips.is_empty() => Ok(ips),
            _ => Err(EgressError::Dns),
        }
    }

    /// Every address must be public and not one of the node's own: a mixed answer is refused as a
    /// whole (never "pick the first safe one").
    fn check_addrs(&self, host: &str, ips: &[IpAddr], trusted: bool) -> Result<(), EgressError> {
        if trusted {
            return Ok(());
        }
        for ip in ips {
            if address_blocked(*ip) || self.denylist.contains(*ip) {
                tracing::warn!(host, "egress guard: refused non-public or self-network address");
                return Err(EgressError::Denied);
            }
        }
        Ok(())
    }

    /// Scheme/port/host rules, DNS resolution (once) and address validation for one hop.
    async fn validate_hop(&self, url: &Url, policy: &EgressPolicy) -> Result<HopTarget, EgressError> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(EgressError::Denied);
        }
        let host = url.host().ok_or(EgressError::Denied)?;
        let port = url.port_or_known_default().ok_or(EgressError::Denied)?;
        if self.cfg.denied_ports.contains(&port) {
            return Err(EgressError::Denied);
        }
        let host_name = match &host {
            Host::Domain(d) => (*d).to_string(),
            Host::Ipv4(a) => a.to_string(),
            Host::Ipv6(a) => a.to_string(),
        };
        if host_denied(&host_name, &policy.denied_hosts) || host_denied(&host_name, &self.cfg.denied_hosts) {
            return Err(EgressError::Denied);
        }
        if !policy.allowed_hosts.is_empty() {
            if !host_allowed(&host_name, &policy.allowed_hosts) {
                return Err(EgressError::Denied);
            }
        } else if policy.default_deny {
            return Err(EgressError::Denied);
        }
        let ips: Vec<IpAddr> = match host {
            Host::Ipv4(a) => vec![IpAddr::V4(a)],
            Host::Ipv6(a) => vec![IpAddr::V6(a)],
            Host::Domain(d) => {
                if is_localhost_name(d) && !self.trusted {
                    return Err(EgressError::Denied);
                }
                self.lookup(d).await?
            }
        };
        self.check_addrs(&host_name, &ips, self.trusted)?;
        let pinned = *ips.first().ok_or(EgressError::Dns)?;
        let rewrite = |ip: IpAddr| self.rewrite.as_ref().map_or(ip, |f| f(ip));
        let dial = rewrite(pinned);
        let fallback = ips[1..].iter().map(|ip| rewrite(*ip)).collect();
        Ok(HopTarget {
            port,
            pinned,
            dial,
            fallback,
            is_literal: !matches!(url.host(), Some(Host::Domain(_))),
        })
    }

    /// The pinned-IP client of one hop: reqwest never resolves the name itself (the domain maps
    /// to the validated address), redirects are off, proxies are off. Clients are pooled per
    /// [`ClientKey`], so a kept-alive connection is only reused by the same tenant, to the same
    /// origin, at the address this hop validated. Timeouts are set per request.
    fn client_for_hop(
        &self,
        url: &mut Url,
        target: &HopTarget,
        tenant: Option<TenantId>,
    ) -> Result<(reqwest::Client, Option<HeaderValue>), EgressError> {
        let mut host_override = None;
        let domain: Option<String> = match url.host() {
            Some(Host::Domain(d)) => Some(d.to_string()),
            _ => None,
        };
        let key = ClientKey {
            tenant,
            https: url.scheme() == "https",
            host: domain
                .clone()
                .unwrap_or_else(|| url.host_str().unwrap_or_default().to_string()),
            port: target.port,
            pinned: target.pinned,
            dial: target.dial,
        };
        if domain.is_none() && target.dial != target.pinned {
            // Test seam only: dial the rewritten literal but keep the original authority.
            let authority = authority_of(url);
            url.set_ip_host(target.dial).map_err(|()| EgressError::Denied)?;
            host_override = HeaderValue::from_str(&authority).ok();
        }
        let pooled = self.cfg.max_pooled_clients > 0;
        let build = || {
            let mut builder = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .connect_timeout(self.cfg.connect_timeout);
            builder = if pooled {
                builder
                    .pool_max_idle_per_host(POOL_IDLE_PER_CLIENT)
                    .pool_idle_timeout(self.cfg.pool_idle_timeout)
            } else {
                builder.pool_max_idle_per_host(0)
            };
            if let Some(d) = &domain {
                let addrs: Vec<SocketAddr> = std::iter::once(target.dial)
                    .chain(target.fallback.iter().copied())
                    .map(|ip| SocketAddr::new(ip, target.port))
                    .collect();
                builder = builder.resolve_to_addrs(d, &addrs);
            }
            builder.build().map_err(|_| EgressError::Upstream)
        };
        let client = if pooled {
            self.pool.get_or_build(key, build)?
        } else {
            build()?
        };
        Ok((client, host_override))
    }

    fn record(&self, result: &str) {
        if let Some(m) = &self.metrics {
            m.egress_requests_total.with_label_values(&[result]).inc();
        }
    }

    async fn send_inner(
        &self,
        req: Request,
        policy: &EgressPolicy,
        budget: &mut EgressBudget,
        permit: OwnedSemaphorePermit,
    ) -> Result<Response, EgressError> {
        let (parts, body) = req.into_parts();
        let mut method = parts.method;
        let mut url = Url::parse(&parts.uri.to_string()).map_err(|_| EgressError::Denied)?;
        let mut headers = sanitize_request_headers(parts.headers);
        let mut body_bytes = collect_request_body(body, self.cfg.max_request_bytes).await?;

        for hop in 0..=self.cfg.max_redirects {
            let remaining = budget.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(EgressError::Timeout);
            }
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_fragment(None);
            let target = self.validate_hop(&url, policy).await?;
            let mut hop_url = url.clone();
            let (client, host_override) = self.client_for_hop(&mut hop_url, &target, budget.tenant)?;

            let mut rb = client
                .request(method.clone(), hop_url)
                .headers(headers.clone())
                .timeout(remaining);
            if let Some(h) = host_override {
                rb = rb.header(header::HOST, h);
            }
            if !body_bytes.is_empty() {
                rb = rb.body(body_bytes.clone());
            }
            let resp = rb.send().await.map_err(map_send_error)?;
            let status = resp.status();

            if is_redirect(status) {
                let location = resp
                    .headers()
                    .get(header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                if let Some(location) = location {
                    if hop == self.cfg.max_redirects {
                        // Too many redirects: refuse without following the last one.
                        return Err(EgressError::Upstream);
                    }
                    let next = redirect_target(&url, &location, self.cfg.allow_https_downgrade)?;
                    let (next_method, keep_body) = redirect_method(status, &method);
                    if !same_origin(&url, &next) {
                        headers.remove(header::AUTHORIZATION);
                        headers.remove(header::COOKIE);
                        headers.remove(header::PROXY_AUTHORIZATION);
                    }
                    if !keep_body {
                        body_bytes = Bytes::new();
                        headers.remove(header::CONTENT_TYPE);
                        headers.remove(header::CONTENT_LENGTH);
                    }
                    method = next_method;
                    url = next;
                    continue;
                }
                // A 3xx without `Location` is an ordinary response.
            }
            return self.finish_response(resp, &method, budget, permit);
        }
        Err(EgressError::Upstream)
    }

    fn finish_response(
        &self,
        resp: reqwest::Response,
        method: &http::Method,
        budget: &mut EgressBudget,
        permit: OwnedSemaphorePermit,
    ) -> Result<Response, EgressError> {
        let status = resp.status();
        let mut headers = resp.headers().clone();
        strip_hop_by_hop(&mut headers);
        let no_body = *method == http::Method::HEAD
            || status == http::StatusCode::NO_CONTENT
            || status == http::StatusCode::NOT_MODIFIED
            || status.is_informational();
        let decoder = if no_body {
            Decoder::Identity
        } else {
            Decoder::for_encoding(headers.get(header::CONTENT_ENCODING))?
        };
        let encoded = !matches!(decoder, Decoder::Identity);
        let cap = self.cfg.max_response_bytes;
        let content_length = if encoded || no_body {
            None
        } else {
            resp.content_length()
        };
        if let Some(len) = content_length {
            if len > cap {
                return Err(EgressError::TooLarge);
            }
        }
        if encoded {
            headers.remove(header::CONTENT_ENCODING);
            headers.remove(header::CONTENT_LENGTH);
        }
        let stream = GuardedBody {
            inner: resp.bytes_stream().boxed(),
            decoder,
            cap,
            usage: budget.usage.clone(),
            bytes_metric: self.metrics.as_ref().map(|m| m.egress_bytes_total.clone()),
            total_in: 0,
            total_out: 0,
            out: VecDeque::new(),
            done: false,
            _permit: Some(permit),
        };
        let body = match content_length {
            Some(len) => Body::from_stream_sized(stream, len),
            None => Body::from_stream(stream),
        };
        let mut out = http::Response::new(body);
        *out.status_mut() = status;
        *out.headers_mut() = headers;
        budget.bytes_received = budget.usage.received();
        budget.quota_bytes_left = budget.usage.quota_left();
        Ok(out)
    }
}

#[async_trait]
impl EgressGuard for GuardedEgress {
    async fn dial(&self, url: &Url, policy: &EgressPolicy) -> Result<Box<dyn EgressIo>, EgressError> {
        let tls = match url.scheme() {
            "ws" => false,
            "wss" => true,
            _ => return Err(EgressError::Denied),
        };
        // The same rules as an HTTP hop of the matching scheme.
        let mut http_url = url.clone();
        http_url
            .set_scheme(if tls { "https" } else { "http" })
            .map_err(|()| EgressError::Denied)?;
        let target = match self.validate_hop(&http_url, policy).await {
            Ok(t) => t,
            Err(e) => {
                self.record(e.code());
                return Err(e);
            }
        };
        // The validated addresses in order: the first one that answers (EGR-006).
        let mut tcp = Err(EgressError::Upstream);
        for ip in std::iter::once(target.dial).chain(target.fallback.iter().copied()) {
            tcp = match tokio::time::timeout(
                self.cfg.connect_timeout,
                tokio::net::TcpStream::connect(SocketAddr::new(ip, target.port)),
            )
            .await
            {
                Ok(Ok(s)) => Ok(s),
                Ok(Err(_)) => Err(EgressError::Upstream),
                Err(_) => Err(EgressError::Timeout),
            };
            if tcp.is_ok() {
                break;
            }
        }
        let tcp = tcp?;
        let _ = tcp.set_nodelay(true);
        self.record("ok");
        if !tls {
            return Ok(Box::new(tcp));
        }
        let host = url
            .host_str()
            .unwrap_or_default()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let name = tokio_rustls::rustls::pki_types::ServerName::try_from(host).map_err(|_| EgressError::Denied)?;
        let cfg = ws_tls().ok_or(EgressError::Upstream)?;
        let stream = tokio::time::timeout(
            self.cfg.connect_timeout,
            tokio_rustls::TlsConnector::from(cfg).connect(name, tcp),
        )
        .await
        .map_err(|_| EgressError::Timeout)?
        .map_err(|_| EgressError::Upstream)?;
        Ok(Box::new(stream))
    }

    async fn send(
        &self,
        req: Request,
        policy: &EgressPolicy,
        budget: &mut EgressBudget,
    ) -> Result<Response, EgressError> {
        let result = self.send_checked(req, policy, budget).await;
        self.record(match &result {
            Ok(_) => "ok",
            Err(e) => e.code(),
        });
        result
    }
}

impl GuardedEgress {
    async fn send_checked(
        &self,
        req: Request,
        policy: &EgressPolicy,
        budget: &mut EgressBudget,
    ) -> Result<Response, EgressError> {
        if budget.fetches_left == 0 {
            return Err(EgressError::Limit);
        }
        budget.fetches_left -= 1;
        if budget.usage.quota_left() == Some(0) {
            return Err(EgressError::Quota);
        }
        let deadline = tokio::time::Instant::from_std(budget.deadline);
        let permit = tokio::time::timeout_at(deadline, budget.concurrency.clone().acquire_owned())
            .await
            .map_err(|_| EgressError::Timeout)?
            .map_err(|_| EgressError::Limit)?;
        match tokio::time::timeout_at(deadline, self.send_inner(req, policy, budget, permit)).await {
            Ok(r) => r,
            Err(_) => Err(EgressError::Timeout),
        }
    }
}

/// What one hop resolved to.
struct HopTarget {
    port: u16,
    /// The validated address.
    pinned: IpAddr,
    /// The address actually dialed (differs from `pinned` only through [`ConnectRewrite`]).
    dial: IpAddr,
    /// The name's other validated addresses (rewritten like `dial`), tried in order when `dial`
    /// cannot be reached: a dual-stack host whose IPv6 route is down is reached over IPv4
    /// (EGR-006).
    fallback: Vec<IpAddr>,
    #[allow(dead_code, reason = "kept for diagnostics of the pinned hop")]
    is_literal: bool,
}

fn is_localhost_name(host: &str) -> bool {
    let h = host.trim_end_matches('.').to_ascii_lowercase();
    h == "localhost" || h.ends_with(".localhost")
}

/// `is_blocked_ip` plus IPv6 forms that embed an IPv4 address (`::a.b.c.d`, NAT64 `64:ff9b::/96`),
/// judged by the embedded address.
fn address_blocked(ip: IpAddr) -> bool {
    if is_blocked_ip(ip) {
        return true;
    }
    if let IpAddr::V6(v6) = ip {
        let seg = v6.segments();
        // NAT64 local-use prefix (RFC 8215): the embedded IPv4 address sits wherever the
        // operator's prefix length puts it, so the whole /48 is refused.
        if seg[..3] == [0x64, 0xff9b, 1] {
            return true;
        }
        if seg[..6] == [0, 0, 0, 0, 0, 0] || seg[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
            let v4 = Ipv4Addr::new((seg[6] >> 8) as u8, seg[6] as u8, (seg[7] >> 8) as u8, seg[7] as u8);
            return is_blocked_ip(IpAddr::V4(v4));
        }
    }
    false
}

fn authority_of(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    }
}

fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme() && a.host_str() == b.host_str() && a.port_or_known_default() == b.port_or_known_default()
}

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Remove hop-by-hop headers (RFC 9110 §7.6.1), including those named by `Connection`.
fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .filter_map(|n| HeaderName::from_bytes(n.trim().as_bytes()).ok())
        .collect();
    for n in named {
        headers.remove(n);
    }
    for n in HOP_BY_HOP {
        headers.remove(*n);
    }
}

/// Guest request headers as sent upstream: hop-by-hop, `Host` (the guard sets it from the URL),
/// `Content-Length` (recomputed) removed; `Accept-Encoding` pinned to what the guard can decode
/// and cap.
fn sanitize_request_headers(mut headers: HeaderMap) -> HeaderMap {
    strip_hop_by_hop(&mut headers);
    headers.remove(header::HOST);
    headers.remove(header::CONTENT_LENGTH);
    headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
    headers
}

async fn collect_request_body(body: Body, cap: u64) -> Result<Bytes, EgressError> {
    body.collect(cap).await.map_err(|e| match e {
        BodyError::TooLarge { .. } => EgressError::TooLarge,
        _ => EgressError::Upstream,
    })
}

fn map_send_error(e: reqwest::Error) -> EgressError {
    if e.is_timeout() {
        EgressError::Timeout
    } else {
        EgressError::Upstream
    }
}

// ---------------------------------------------------------------------------------------------
// Response body: decoding + caps
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
struct LimitHit;

impl std::fmt::Display for LimitHit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("decoded size limit reached")
    }
}

impl std::error::Error for LimitHit {}

/// Output buffer of a decoder that refuses to grow past `limit`: the decoder can never expand a
/// single input chunk beyond the remaining allowance.
struct CappedSink {
    buf: Vec<u8>,
    limit: usize,
}

impl std::io::Write for CappedSink {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.buf.len().saturating_add(data.len()) > self.limit {
            return Err(std::io::Error::other(LimitHit));
        }
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

enum Decoder {
    Identity,
    Gzip(flate2::write::GzDecoder<CappedSink>),
    Zlib(flate2::write::ZlibDecoder<CappedSink>),
}

impl Decoder {
    fn for_encoding(value: Option<&HeaderValue>) -> Result<Self, EgressError> {
        let Some(value) = value else {
            return Ok(Decoder::Identity);
        };
        let v = value
            .to_str()
            .map_err(|_| EgressError::Upstream)?
            .trim()
            .to_ascii_lowercase();
        let sink = || CappedSink {
            buf: Vec::new(),
            limit: 0,
        };
        match v.as_str() {
            "" | "identity" => Ok(Decoder::Identity),
            "gzip" | "x-gzip" => Ok(Decoder::Gzip(flate2::write::GzDecoder::new(sink()))),
            "deflate" => Ok(Decoder::Zlib(flate2::write::ZlibDecoder::new(sink()))),
            // br / zstd / stacked encodings: not requested, cannot be capped here.
            _ => Err(EgressError::Upstream),
        }
    }
}

/// The body handed to the guest: counts wire bytes against the daily quota, decodes, and enforces
/// the response cap and the inflate ratio on the **decompressed** bytes.
struct GuardedBody {
    inner: BoxStream<'static, Result<Bytes, reqwest::Error>>,
    decoder: Decoder,
    cap: u64,
    usage: Arc<EgressUsage>,
    bytes_metric: Option<prometheus::IntCounter>,
    total_in: u64,
    total_out: u64,
    out: VecDeque<Bytes>,
    done: bool,
    /// Held until the body is dropped or finished: concurrent-fetch accounting covers streaming.
    _permit: Option<OwnedSemaphorePermit>,
}

impl GuardedBody {
    fn fail(&mut self, e: BodyError) -> Poll<Option<Result<Bytes, BodyError>>> {
        self.done = true;
        self.out.clear();
        self._permit = None;
        Poll::Ready(Some(Err(e)))
    }

    fn push_frames(&mut self, mut data: BytesMut) {
        while !data.is_empty() {
            let n = data.len().min(FRAME);
            self.out.push_back(data.split_to(n).freeze());
        }
    }

    /// Feed one wire chunk through the decoder. `Err` carries the body error to surface.
    fn accept(&mut self, chunk: Bytes) -> Result<(), BodyError> {
        let wire = chunk.len() as u64;
        self.total_in += wire;
        if let Some(m) = &self.bytes_metric {
            m.inc_by(wire);
        }
        if !self.usage.consume(wire) {
            return Err(BodyError::Aborted(EgressError::Quota.code().to_string()));
        }
        let cap = self.cap;
        let too_large = move || BodyError::TooLarge { limit: cap };
        // Allowance for decoded bytes: the response cap and the inflate ratio.
        let allowed_total = self
            .total_in
            .saturating_mul(MAX_INFLATE_RATIO)
            .saturating_add(INFLATE_SLACK)
            .min(self.cap);
        let allowance = allowed_total.saturating_sub(self.total_out);
        match &mut self.decoder {
            Decoder::Identity => {
                self.total_out += wire;
                if self.total_out > self.cap {
                    return Err(too_large());
                }
                if !chunk.is_empty() {
                    self.out.push_back(chunk);
                }
                Ok(())
            }
            Decoder::Gzip(d) => {
                d.get_mut().limit = allowance as usize;
                let r = d.write_all(&chunk);
                let produced = std::mem::take(&mut d.get_mut().buf);
                Self::after_decode(self, r, produced, too_large)
            }
            Decoder::Zlib(d) => {
                d.get_mut().limit = allowance as usize;
                let r = d.write_all(&chunk);
                let produced = std::mem::take(&mut d.get_mut().buf);
                Self::after_decode(self, r, produced, too_large)
            }
        }
    }

    fn after_decode(
        &mut self,
        r: std::io::Result<()>,
        produced: Vec<u8>,
        too_large: impl Fn() -> BodyError,
    ) -> Result<(), BodyError> {
        if let Err(e) = r {
            let limit_hit = e.get_ref().is_some_and(|inner| inner.is::<LimitHit>());
            return Err(if limit_hit {
                too_large()
            } else {
                BodyError::Io(EgressError::Upstream.code().to_string())
            });
        }
        self.total_out += produced.len() as u64;
        self.push_frames(BytesMut::from(produced.as_slice()));
        Ok(())
    }

    /// End of the wire stream: flush the decoder and check it saw a complete stream.
    fn finish(&mut self) -> Result<(), BodyError> {
        if self.total_in == 0 {
            return Ok(());
        }
        let allowed_total = self
            .total_in
            .saturating_mul(MAX_INFLATE_RATIO)
            .saturating_add(INFLATE_SLACK)
            .min(self.cap);
        let allowance = allowed_total.saturating_sub(self.total_out) as usize;
        let too_large = BodyError::TooLarge { limit: self.cap };
        let (r, produced) = match &mut self.decoder {
            Decoder::Identity => return Ok(()),
            Decoder::Gzip(d) => {
                d.get_mut().limit = allowance;
                (d.try_finish(), std::mem::take(&mut d.get_mut().buf))
            }
            Decoder::Zlib(d) => {
                d.get_mut().limit = allowance;
                (d.try_finish(), std::mem::take(&mut d.get_mut().buf))
            }
        };
        if let Err(e) = r {
            let limit_hit = e.get_ref().is_some_and(|inner| inner.is::<LimitHit>());
            return Err(if limit_hit {
                too_large
            } else {
                BodyError::Io(EgressError::Upstream.code().to_string())
            });
        }
        self.total_out += produced.len() as u64;
        self.push_frames(BytesMut::from(produced.as_slice()));
        Ok(())
    }
}

impl Stream for GuardedBody {
    type Item = Result<Bytes, BodyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(b) = this.out.pop_front() {
                return Poll::Ready(Some(Ok(b)));
            }
            if this.done {
                return Poll::Ready(None);
            }
            match this.inner.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.done = true;
                    this._permit = None;
                    if let Err(e) = this.finish() {
                        return this.fail(e);
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    let code = if e.is_timeout() {
                        EgressError::Timeout
                    } else {
                        EgressError::Upstream
                    };
                    return this.fail(BodyError::Io(code.code().to_string()));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    if let Err(e) = this.accept(chunk) {
                        return this.fail(e);
                    }
                }
            }
        }
    }
}

/// Method/body rewriting on redirect (RFC 9110 §15.4): 303 → bodiless GET; 307/308 preserve;
/// 301/302 turn POST into bodiless GET.
pub fn redirect_method(status: http::StatusCode, method: &http::Method) -> (http::Method, bool /* keep_body */) {
    use http::StatusCode;
    match status {
        StatusCode::SEE_OTHER => (http::Method::GET, false),
        StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT => (method.clone(), true),
        _ if *method == http::Method::POST => (http::Method::GET, false),
        _ => (method.clone(), true),
    }
}

pub fn is_redirect(status: http::StatusCode) -> bool {
    use http::StatusCode;
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

/// A redirect from `https` to plain `http` (SEC-50): the rest of the chain would travel, and could
/// be altered, in clear text.
pub fn is_https_downgrade(from: &Url, to: &Url) -> bool {
    from.scheme() == "https" && to.scheme() == "http"
}

/// The next hop of a redirect: `location` resolved against `current`, refused when it downgrades
/// `https` to `http` and the operator did not allow that.
fn redirect_target(current: &Url, location: &str, allow_https_downgrade: bool) -> Result<Url, EgressError> {
    let next = current.join(location).map_err(|_| EgressError::Upstream)?;
    if is_https_downgrade(current, &next) && !allow_https_downgrade {
        tracing::warn!(
            host = current.host_str().unwrap_or_default(),
            "egress guard: refused an https -> http redirect"
        );
        return Err(EgressError::InsecureRedirect);
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nat64_local_use_and_ipv4_embedding_forms_of_blocked_addresses_are_blocked() {
        let ip = |s: &str| s.parse::<IpAddr>().expect("test address");
        for blocked in [
            "64:ff9b:1::8.8.8.8",
            "64:ff9b:1:ffff::1",
            "64:ff9b:1:0:8:808:800::",
            "64:ff9b::127.0.0.1",
            "64:ff9b::10.0.0.1",
            "::169.254.169.254",
            "2002:7f00:1::1",
        ] {
            assert!(address_blocked(ip(blocked)), "{blocked}");
        }
        for public in ["64:ff9b::8.8.8.8", "64:ff9b:2::1", "2606:4700::1111", "8.8.8.8"] {
            assert!(!address_blocked(ip(public)), "{public}");
        }
    }

    fn url(s: &str) -> Url {
        Url::parse(s).expect("test url")
    }

    #[test]
    fn ac_sec_50_an_https_to_http_redirect_is_refused_unless_allowed() {
        let from = url("https://api.example.com/start");
        for location in [
            "http://api.example.com/next",
            "http://other.example.com/",
            "HTTP://api.example.com:443/x",
        ] {
            assert_eq!(
                redirect_target(&from, location, false),
                Err(EgressError::InsecureRedirect),
                "{location}"
            );
            assert_eq!(
                redirect_target(&from, location, true).map(|u| u.scheme().to_string()),
                Ok("http".into())
            );
        }
        assert_eq!(EgressError::InsecureRedirect.code(), "egress_insecure_redirect");
        assert!(
            !EgressGuardConfig::default().allow_https_downgrade,
            "refused by default"
        );
    }

    #[test]
    fn ac_sec_50_upgrades_and_same_scheme_redirects_are_followed() {
        let plain = url("http://api.example.com/a");
        let tls = url("https://api.example.com/a");
        assert_eq!(
            redirect_target(&plain, "https://api.example.com/b", false),
            Ok(url("https://api.example.com/b"))
        );
        assert_eq!(
            redirect_target(&plain, "/b", false),
            Ok(url("http://api.example.com/b"))
        );
        assert_eq!(
            redirect_target(&tls, "/b", false),
            Ok(url("https://api.example.com/b")),
            "relative stays https"
        );
        assert_eq!(
            redirect_target(&tls, "//cdn.example.com/b", false),
            Ok(url("https://cdn.example.com/b"))
        );
        // Other schemes are not a downgrade here: the next hop's scheme check denies them.
        assert!(redirect_target(&tls, "ftp://files.example.com/", false).is_ok());
    }

    #[test]
    fn pooled_clients_are_evicted_least_recently_used_first() {
        let pool = ClientPool::new(2, Duration::from_secs(60));
        let key = |tenant: u128, host: &str| ClientKey {
            tenant: Some(TenantId::from_u128(tenant)),
            https: true,
            host: host.into(),
            port: 443,
            pinned: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
            dial: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
        };
        let build = || reqwest::Client::builder().build().map_err(|_| EgressError::Upstream);
        let built = std::cell::Cell::new(0);
        let counting = || {
            built.set(built.get() + 1);
            build()
        };
        let tick = || std::thread::sleep(Duration::from_millis(2));
        pool.get_or_build(key(1, "a"), counting).expect("a");
        tick();
        pool.get_or_build(key(1, "b"), build).expect("b");
        tick();
        pool.get_or_build(key(1, "a"), || panic!("a is cached"))
            .expect("a again");
        tick();
        pool.get_or_build(key(1, "c"), build).expect("c");
        assert_eq!(pool.len(), 2, "bounded");
        assert!(pool.clients.lock().contains_key(&key(1, "a")), "recently used survives");
        assert!(
            !pool.clients.lock().contains_key(&key(1, "b")),
            "least recently used is evicted"
        );
        assert_eq!(built.get(), 1);
    }

    #[test]
    fn idle_pooled_clients_expire() {
        let pool = ClientPool::new(8, Duration::from_millis(1));
        let k = ClientKey {
            tenant: None,
            https: false,
            host: "a".into(),
            port: 80,
            pinned: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
            dial: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
        };
        let build = || reqwest::Client::builder().build().map_err(|_| EgressError::Upstream);
        pool.get_or_build(k.clone(), build).expect("first");
        std::thread::sleep(Duration::from_millis(5));
        let rebuilt = std::cell::Cell::new(false);
        pool.get_or_build(k, || {
            rebuilt.set(true);
            build()
        })
        .expect("second");
        assert!(rebuilt.get(), "an idle client is not reused after the idle timeout");
    }
}
