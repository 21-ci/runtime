//! Authentication (SECURITY.md).
//!
//! | Principal | Credential |
//! |---|---|
//! | Operator | `RT_ROOT_TOKEN` (constant-time compare) |
//! | Tenant key | `rtk_<key_id>_<secret>` (HMAC-hashed at rest; cached up to 1 s; revocation and tenant disable drop cached copies before the control call answers, otherwise a change is seen within 1 s) |
//! | Internal | peer on the internal Unix socket |

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use rt_types::{scope_admits, ApiKey, KeyId, Listener, ProjectId, Scope, Scopes, TenantId, UnixMs};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::keys::{parse_key, KeyHasher, KEY_PREFIX};

/// Who is calling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    /// Root token holder: all control routes, any tenant (`x-rt-tenant` selects one).
    Operator,
    Tenant {
        tenant: TenantId,
        key: KeyId,
        scopes: Scopes,
        /// `None` = every project of the tenant.
        project_ids: Option<Vec<ProjectId>>,
        /// `None` = every environment.
        environments: Option<Vec<String>>,
    },
    /// Trusted peer on the internal listener; `x-rt-invoker` honored.
    Internal {
        invoker: Option<String>,
    },
    Anonymous,
}

impl Principal {
    /// Operator, or a tenant key holding `scope`.
    pub fn has_scope(&self, scope: Scope) -> bool {
        match self {
            Principal::Operator => true,
            Principal::Tenant { scopes, .. } => scopes.contains(scope),
            Principal::Internal { .. } | Principal::Anonymous => false,
        }
    }

    /// Operator, or a tenant key allowed to touch a service of `project` in `environment` (no
    /// restriction list = all; an explicit empty list allows nothing).
    pub fn can_touch(&self, project: ProjectId, environment: &str) -> bool {
        match self {
            Principal::Operator => true,
            Principal::Tenant {
                project_ids,
                environments,
                ..
            } => scope_admits(project_ids.as_deref(), environments.as_deref(), project, environment),
            Principal::Internal { .. } | Principal::Anonymous => false,
        }
    }

    /// The tenant this principal is bound to, if any.
    pub fn tenant(&self) -> Option<TenantId> {
        match self {
            Principal::Tenant { tenant, .. } => Some(*tenant),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum AuthError {
    /// No credential presented where one is required (`401 unauthorized`).
    #[error("missing credentials")]
    Missing,
    /// Wrong token/secret, unknown key, revoked, expired (`401`; one error so existence is not revealed).
    #[error("invalid credentials")]
    Invalid,
    /// Too many failures from one IP (`429 rate_limited`).
    #[error("too many authentication failures")]
    RateLimited { retry_after: Duration },
    /// Release build without `RT_ROOT_TOKEN` (`503 control_disabled`).
    #[error("control api disabled: no root token configured")]
    ControlDisabled,
}

impl AuthError {
    pub fn http_status(&self) -> u16 {
        match self {
            AuthError::Missing | AuthError::Invalid => 401,
            AuthError::RateLimited { .. } => 429,
            AuthError::ControlDisabled => 503,
        }
    }

    /// Control API error `code`.
    pub fn code(&self) -> &'static str {
        match self {
            AuthError::Missing | AuthError::Invalid => "unauthorized",
            AuthError::RateLimited { .. } => "rate_limited",
            AuthError::ControlDisabled => "control_disabled",
        }
    }
}

/// Extension point (paid: OIDC / SSO / RBAC).
pub trait Authenticator: Send + Sync {
    fn authenticate(&self, parts: &http::request::Parts, listener: Listener) -> Result<Principal, AuthError>;

    /// Whether `token` is the operator's root token (compared in constant time). The invoke path
    /// uses it to strip the root token from a request before any guest sees it (INV-009).
    fn is_root_token(&self, token: &str) -> bool {
        let _ = token;
        false
    }
}

/// Where the default authenticator finds stored keys (implemented in `rt-server` over `MetaStore`
/// with a cache that is invalidated on revoke).
pub trait KeyLookup: Send + Sync {
    fn find(&self, id: KeyId) -> Option<ApiKey>;
}

/// Wall clock source (virtual in tests).
pub trait WallClock: Send + Sync {
    fn now_ms(&self) -> UnixMs;
}

/// Real wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl WallClock for SystemClock {
    fn now_ms(&self) -> UnixMs {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

const FAILURE_WINDOW_MS: i64 = 60_000;
/// Addresses (IPv4) or IPv6 /64 networks a limiter tracks at once (hard cap, see
/// [`AuthFailureLimiter`]).
pub const LIMITER_CAPACITY: usize = 65_536;
const LIMITER_SHARDS: usize = 16;
/// Expired windows are swept one shard at a time, at most once per this interval.
const SWEEP_EVERY_MS: i64 = 1_000;
/// Entries looked at to pick a victim when a full shard must admit a new address.
const EVICT_SAMPLE: usize = 16;

#[derive(Clone, Copy, Debug)]
struct Window {
    start_ms: UnixMs,
    failures: u32,
}

/// What a limiter counts failures against: the IPv4 address, or the IPv6 /64 (one host usually
/// holds a whole /64, so per-/128 counting would let it rotate addresses freely). IPv4-mapped
/// IPv6 addresses count as their IPv4 address.
pub fn limiter_key(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V4(v4) => IpAddr::V4(v4),
        IpAddr::V6(v6) => IpAddr::V6(std::net::Ipv6Addr::from(u128::from(v6) & !((1u128 << 64) - 1))),
    }
}

/// Failed-auth limiter: 10 failures per minute per client, then `429` (SEC-02, SEC-39).
///
/// Fixed one-minute window per client ([`limiter_key`]: IPv4 address or IPv6 /64), opened by the
/// first failure. Once the window holds more than `max_per_min` failures the client is blocked
/// until the window ends.
///
/// The table is reachable from the public listener, so it is bounded and cheap to maintain:
/// at most `capacity` clients in `LIMITER_SHARDS` independently locked shards. A full shard
/// admits a new client by evicting one sampled entry (expired first, then the oldest window
/// that is not blocking anyone, then the oldest). Expired windows are swept one shard at a time,
/// at most once a second, piggybacked on failures; no failure ever scans the whole table.
pub struct AuthFailureLimiter {
    max_per_min: u32,
    shard_cap: usize,
    shards: Box<[parking_lot::Mutex<std::collections::HashMap<IpAddr, Window>>]>,
    next_sweep_ms: AtomicI64,
    sweep_cursor: AtomicUsize,
}

impl AuthFailureLimiter {
    pub fn new(max_per_min: u32) -> Self {
        Self::with_capacity(max_per_min, LIMITER_CAPACITY)
    }

    /// A limiter tracking at most `capacity` clients (rounded up to whole shards).
    pub fn with_capacity(max_per_min: u32, capacity: usize) -> Self {
        let shard_cap = capacity.div_ceil(LIMITER_SHARDS).max(1);
        Self {
            max_per_min,
            shard_cap,
            shards: (0..LIMITER_SHARDS)
                .map(|_| parking_lot::Mutex::new(std::collections::HashMap::new()))
                .collect(),
            next_sweep_ms: AtomicI64::new(i64::MIN),
            sweep_cursor: AtomicUsize::new(0),
        }
    }

    fn shard(&self, key: &IpAddr) -> &parking_lot::Mutex<std::collections::HashMap<IpAddr, Window>> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        &self.shards[(h.finish() as usize) % LIMITER_SHARDS]
    }

    fn retry_after(w: &Window, now_ms: UnixMs) -> Duration {
        let left = w
            .start_ms
            .saturating_add(FAILURE_WINDOW_MS)
            .saturating_sub(now_ms)
            .clamp(1, FAILURE_WINDOW_MS);
        Duration::from_millis(left as u64)
    }

    fn expired(w: &Window, now_ms: UnixMs) -> bool {
        now_ms.saturating_sub(w.start_ms) >= FAILURE_WINDOW_MS
    }

    /// Make room in a full shard: drop one entry out of a small sample.
    fn evict_one(&self, map: &mut std::collections::HashMap<IpAddr, Window>, now_ms: UnixMs) {
        let victim = map
            .iter()
            .take(EVICT_SAMPLE)
            .min_by_key(|(_, w)| (!Self::expired(w, now_ms), w.failures > self.max_per_min, w.start_ms))
            .map(|(k, _)| *k);
        if let Some(k) = victim {
            map.remove(&k);
        }
    }

    /// Sweep the next shard's expired windows, at most once per `SWEEP_EVERY_MS`.
    fn maybe_sweep(&self, now_ms: UnixMs) {
        let due = self.next_sweep_ms.load(Ordering::Relaxed);
        if now_ms < due
            || self
                .next_sweep_ms
                .compare_exchange(
                    due,
                    now_ms.saturating_add(SWEEP_EVERY_MS),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return;
        }
        let i = self.sweep_cursor.fetch_add(1, Ordering::Relaxed) % LIMITER_SHARDS;
        self.shards[i].lock().retain(|_, w| !Self::expired(w, now_ms));
    }

    /// Drop every expired window now (for a maintenance timer; failures already sweep
    /// incrementally).
    pub fn sweep(&self, now_ms: UnixMs) {
        for shard in self.shards.iter() {
            shard.lock().retain(|_, w| !Self::expired(w, now_ms));
        }
    }

    /// Clients currently tracked.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Record a failure; `Err(retry_after)` once the limit is exceeded.
    pub fn record_failure(&self, ip: IpAddr, now_ms: UnixMs) -> Result<(), Duration> {
        self.maybe_sweep(now_ms);
        let key = limiter_key(ip);
        let mut map = self.shard(&key).lock();
        if !map.contains_key(&key) && map.len() >= self.shard_cap {
            self.evict_one(&mut map, now_ms);
        }
        let w = map.entry(key).or_insert(Window {
            start_ms: now_ms,
            failures: 0,
        });
        if Self::expired(w, now_ms) {
            *w = Window {
                start_ms: now_ms,
                failures: 0,
            };
        }
        w.failures = w.failures.saturating_add(1);
        if w.failures > self.max_per_min {
            Err(Self::retry_after(w, now_ms))
        } else {
            Ok(())
        }
    }

    /// Is `ip` currently blocked?
    pub fn check(&self, ip: IpAddr, now_ms: UnixMs) -> Result<(), Duration> {
        let key = limiter_key(ip);
        match self.shard(&key).lock().get(&key) {
            Some(w) if !Self::expired(w, now_ms) && w.failures > self.max_per_min => Err(Self::retry_after(w, now_ms)),
            _ => Ok(()),
        }
    }
}

/// How long a key stays cached; bounds revocation latency when `invalidate_key` is not called
/// (SEC-06: effective within 1 s).
const KEY_CACHE_TTL: Duration = Duration::from_secs(1);
const KEY_CACHE_CAPACITY: usize = 4096;

struct CachedKey {
    key: Arc<ApiKey>,
    loaded: Instant,
}

/// One presented credential.
#[derive(Clone, Copy)]
enum Cred<'a> {
    /// `Authorization: Bearer <token>`: the root token or an `rtk_` key.
    Bearer(&'a str),
    /// `x-rt-key: <rtk_ key>`.
    Key(&'a str),
}

/// The real client address of a request, after walking `X-Forwarded-For` through
/// `ingress.trusted_proxies` (`TrustedProxies::client_ip`). The server puts it into the request
/// extensions; the failed-auth limiter keys on it, so clients behind one reverse proxy do not
/// share a lockout (SEC-40).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

/// Root token + tenant keys.
///
/// The failed-auth limiter keys on the client address, which the server must put into the
/// request extensions before authenticating: a [`ClientIp`] (preferred), else the peer as a
/// `std::net::SocketAddr` (or `std::net::IpAddr`); without one, failures are not rate-limited.
/// The control listener and `access=key` invokes on the public listener have separate budgets
/// (SEC-02, SEC-39): wrong invoke keys from a shared address (CGNAT, an office) never lock the
/// operator out of the control API.
pub struct DefaultAuthenticator {
    /// SHA-256 of the root token: comparing fixed-size digests hides the token's length.
    root_digest: Option<[u8; 32]>,
    hasher: KeyHasher,
    keys: Arc<dyn KeyLookup>,
    clock: Arc<dyn WallClock>,
    /// Control-listener failures.
    limiter: AuthFailureLimiter,
    /// Failed `rtk_` keys on the public listener.
    public_limiter: AuthFailureLimiter,
    cache: DashMap<KeyId, CachedKey>,
    /// Bumped by every invalidation: a cache fill that raced one drops what it inserted, so a
    /// revoked key is never re-cached from a lookup that started before the revocation.
    epoch: AtomicU64,
}

impl DefaultAuthenticator {
    /// `root_token = None` ⇒ control routes answer `ControlDisabled` in release builds; debug
    /// builds stay open (SECURITY.md).
    pub fn new(
        root_token: Option<String>,
        hasher: KeyHasher,
        keys: Arc<dyn KeyLookup>,
        clock: Arc<dyn WallClock>,
    ) -> Self {
        let root = root_token.map(zeroize::Zeroizing::new).filter(|t| !t.is_empty());
        Self {
            root_digest: root.as_ref().map(|t| Sha256::digest(t.as_bytes()).into()),
            hasher,
            keys,
            clock,
            limiter: AuthFailureLimiter::new(10),
            public_limiter: AuthFailureLimiter::new(10),
            cache: DashMap::new(),
            epoch: AtomicU64::new(0),
        }
    }

    /// Drop a cached key immediately (SEC-06). Also defeats an in-flight cache fill for it.
    pub fn invalidate_key(&self, id: KeyId) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.cache.remove(&id);
    }

    /// Drop every cached key of `tenant` (tenant disabled or changed).
    pub fn invalidate_tenant(&self, tenant: TenantId) {
        self.invalidate_where(|k| k.tenant_id == tenant);
    }

    /// Drop every cached key matching `pred` (e.g. keys the lookup no longer serves).
    pub fn invalidate_where(&self, pred: impl Fn(&ApiKey) -> bool) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.cache.retain(|_, c| !pred(&c.key));
    }

    #[doc(hidden)]
    pub fn cached_len(&self) -> usize {
        self.cache.len()
    }

    fn is_root(&self, token: &str) -> bool {
        match &self.root_digest {
            Some(root) => {
                let presented: [u8; 32] = Sha256::digest(token.as_bytes()).into();
                bool::from(presented.ct_eq(root))
            }
            None => false,
        }
    }

    fn load_key(&self, id: KeyId) -> Option<Arc<ApiKey>> {
        let cached = self
            .cache
            .get(&id)
            .and_then(|c| (c.loaded.elapsed() < KEY_CACHE_TTL).then(|| c.key.clone()));
        if cached.is_some() {
            return cached;
        }
        self.cache.remove(&id);
        let epoch = self.epoch.load(Ordering::SeqCst);
        let key = Arc::new(self.keys.find(id)?);
        if !key.revoked {
            if self.cache.len() >= KEY_CACHE_CAPACITY {
                // O(1) amortized: evict an arbitrary entry (first non-empty shard), not the oldest.
                let victim = self.cache.iter().next().map(|e| *e.key());
                if let Some(victim) = victim {
                    self.cache.remove(&victim);
                }
            }
            self.cache.insert(
                id,
                CachedKey {
                    key: key.clone(),
                    loaded: Instant::now(),
                },
            );
            // Insert-then-check: an invalidation bumps the epoch before removing, so either it
            // removes our entry or we see the bump and remove it ourselves.
            if self.epoch.load(Ordering::SeqCst) != epoch {
                self.cache.remove_if(&id, |_, c| Arc::ptr_eq(&c.key, &key));
            }
        }
        Some(key)
    }

    /// Validate an `rtk_` key. Every failure is the same `Invalid`; unknown ids still pay for one
    /// HMAC so timing does not reveal which ids exist.
    fn resolve_key(&self, token: &str) -> Result<Principal, AuthError> {
        const NO_KEY: [u8; 32] = [0; 32];
        let parsed = parse_key(token).map_err(|_| AuthError::Invalid)?;
        let key = self.load_key(parsed.id);
        let stored = key.as_ref().map_or(&NO_KEY[..], |k| k.hmac.as_slice());
        let secret_ok = self.hasher.verify(parsed.secret, stored);
        let Some(key) = key else { return Err(AuthError::Invalid) };
        let now = self.clock.now_ms();
        let expired = key.expires_at_ms.is_some_and(|at| at <= now);
        if !secret_ok || key.revoked || expired {
            return Err(AuthError::Invalid);
        }
        Ok(Principal::Tenant {
            tenant: key.tenant_id,
            key: key.id,
            scopes: key.scopes.clone(),
            project_ids: key.project_ids.clone(),
            environments: key.environments.clone(),
        })
    }

    fn resolve(&self, cred: Cred<'_>) -> Result<Principal, AuthError> {
        let (token, allow_root) = match cred {
            Cred::Bearer(t) => (t, true),
            Cred::Key(t) => (t, false),
        };
        if allow_root && self.is_root(token) {
            return Ok(Principal::Operator);
        }
        if is_key_shaped(token) {
            return self.resolve_key(token);
        }
        Err(AuthError::Invalid)
    }

    fn control(&self, parts: &http::request::Parts) -> Result<Principal, AuthError> {
        let creds = credentials(parts);
        if self.root_digest.is_none() {
            if !cfg!(debug_assertions) {
                return Err(AuthError::ControlDisabled);
            }
            // debug builds stay open: no credentials at all ⇒ operator, but only for a local tool,
            // never for a browser page of another site or a DNS-rebound host name (AUTH-013).
            if creds.is_empty() {
                return if local_request(parts) {
                    Ok(Principal::Operator)
                } else {
                    Err(AuthError::Missing)
                };
            }
        }
        let ip = client_ip(parts);
        let now = self.clock.now_ms();
        if let Some(ip) = ip {
            self.limiter
                .check(ip, now)
                .map_err(|retry_after| AuthError::RateLimited { retry_after })?;
        }
        if creds.is_empty() {
            return Err(AuthError::Missing);
        }
        for cred in &creds {
            if let Ok(p) = self.resolve(*cred) {
                return Ok(p);
            }
        }
        if let Some(ip) = ip {
            self.limiter
                .record_failure(ip, now)
                .map_err(|retry_after| AuthError::RateLimited { retry_after })?;
        }
        Err(AuthError::Invalid)
    }

    /// Public listener (`access=key` invokes): only `rtk_` credentials are ours (the guest's own
    /// `Authorization` is none of our business). A failed key counts against the client's public
    /// budget (separate from the control budget); a blocked client gets `RateLimited` (SEC-39).
    /// Anything else is anonymous.
    fn public(&self, parts: &http::request::Parts) -> Result<Principal, AuthError> {
        let keys: Vec<&str> = credentials(parts)
            .into_iter()
            .filter_map(|c| match c {
                Cred::Bearer(t) | Cred::Key(t) if is_key_shaped(t) => Some(t),
                _ => None,
            })
            .collect();
        if keys.is_empty() {
            return Ok(Principal::Anonymous);
        }
        let ip = client_ip(parts);
        let now = self.clock.now_ms();
        if let Some(ip) = ip {
            self.public_limiter
                .check(ip, now)
                .map_err(|retry_after| AuthError::RateLimited { retry_after })?;
        }
        if let Some(p) = keys.iter().find_map(|t| self.resolve_key(t).ok()) {
            return Ok(p);
        }
        if let Some(ip) = ip {
            self.public_limiter
                .record_failure(ip, now)
                .map_err(|retry_after| AuthError::RateLimited { retry_after })?;
        }
        Ok(Principal::Anonymous)
    }
}

/// A loopback host name or address, with or without a port (`localhost`, `127.0.0.0/8`, `[::1]`).
fn is_loopback_host(authority: &str) -> bool {
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => authority.rsplit_once(':').map_or(authority, |(h, port)| {
            if port.bytes().all(|b| b.is_ascii_digit()) {
                h
            } else {
                authority
            }
        }),
    };
    host.eq_ignore_ascii_case("localhost") || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// A request from a local tool: no `Origin`, or a loopback `http(s)` origin, and a loopback `Host`
/// (or none). A browser on another site always sends its own `Origin`; a DNS-rebinding page sends
/// its own host name as `Host`.
fn local_request(parts: &http::request::Parts) -> bool {
    let header = |n: http::header::HeaderName| {
        parts
            .headers
            .get(n)
            .map(|v| v.to_str().unwrap_or("\0").trim().to_string())
    };
    let origin_ok = header(http::header::ORIGIN).is_none_or(|o| {
        o.split_once("://").is_some_and(|(scheme, rest)| {
            (scheme == "http" || scheme == "https")
                && is_loopback_host(rest.split(['/', '?', '#']).next().unwrap_or(""))
        })
    });
    let host_ok = header(http::header::HOST).is_none_or(|h| is_loopback_host(&h));
    origin_ok && host_ok
}

/// `rtk_…`: shaped like a tenant key.
fn is_key_shaped(t: &str) -> bool {
    t.starts_with(KEY_PREFIX) && t[KEY_PREFIX.len()..].starts_with('_')
}

/// Debug builds without a root token leave the control API open (every caller is the operator).
/// `has_root_token = false` in a debug build ⇒ `true`; release builds disable the control API
/// instead (`503 control_disabled`).
pub fn control_open_without_token(has_root_token: bool) -> bool {
    !has_root_token && cfg!(debug_assertions)
}

/// While the control API is open ([`control_open_without_token`]) it may only listen on a
/// loopback address (`127.0.0.0/8`, `::1`, `localhost`). `Err` explains how to fix the config
/// (SEC-42).
pub fn check_open_control_bind(addr: &str) -> Result<(), String> {
    let addr = addr.trim();
    if is_loopback_bind(addr) {
        Ok(())
    } else {
        Err(format!(
            "listeners.control = \"{addr}\" is not a loopback address, and without RT_ROOT_TOKEN this debug build \
             leaves the control API open to every caller: set RT_ROOT_TOKEN or bind the control listener to 127.0.0.1"
        ))
    }
}

/// `127.0.0.0/8`, `::1` or `localhost:<port>`.
fn is_loopback_bind(addr: &str) -> bool {
    match addr.parse::<SocketAddr>() {
        Ok(a) => a.ip().is_loopback(),
        Err(_) => addr.rsplit_once(':').is_some_and(|(host, port)| {
            host.eq_ignore_ascii_case("localhost") && !port.is_empty() && port.bytes().all(|c| c.is_ascii_digit())
        }),
    }
}

/// Shortest root token accepted on a control listener that is not loopback-only.
pub const MIN_ROOT_TOKEN_LEN: usize = 32;

/// A root token must not be the published `.env.example` placeholder (`change-me…`), and when the
/// control API listens beyond loopback it must be at least [`MIN_ROOT_TOKEN_LEN`] characters. A
/// token with leading or trailing whitespace is refused: a presented token is trimmed, so it could
/// never match (AUTH-007). `Err` explains how to fix it.
pub fn check_root_token(token: &str, control_addr: &str) -> Result<(), String> {
    const FIX: &str = "generate one with `openssl rand -hex 32`";
    if token.trim() != token {
        return Err("RT_ROOT_TOKEN starts or ends with whitespace, so no request could present it: remove it".into());
    }
    if token.trim().to_ascii_lowercase().starts_with("change-me") {
        return Err(format!(
            "RT_ROOT_TOKEN is the `change-me` placeholder from .env.example: {FIX}"
        ));
    }
    if token.chars().count() < MIN_ROOT_TOKEN_LEN && !is_loopback_bind(control_addr.trim()) {
        return Err(format!(
            "RT_ROOT_TOKEN is shorter than {MIN_ROOT_TOKEN_LEN} characters and listeners.control = \"{}\" is not a \
             loopback address: {FIX}",
            control_addr.trim()
        ));
    }
    Ok(())
}

/// Credentials in precedence order: bearer first, then `x-rt-key`.
fn credentials(parts: &http::request::Parts) -> Vec<Cred<'_>> {
    let mut out = Vec::with_capacity(2);
    if let Some(v) = parts
        .headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some((scheme, rest)) = v.trim().split_once(char::is_whitespace) {
            let token = rest.trim();
            if scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() {
                out.push(Cred::Bearer(token));
            }
        }
    }
    if let Some(v) = parts.headers.get("x-rt-key").and_then(|v| v.to_str().ok()) {
        let v = v.trim();
        if !v.is_empty() {
            out.push(Cred::Key(v));
        }
    }
    out
}

/// Client address placed in the request extensions by the server: the [`ClientIp`] resolved
/// behind trusted proxies, else the socket peer.
fn client_ip(parts: &http::request::Parts) -> Option<IpAddr> {
    parts
        .extensions
        .get::<ClientIp>()
        .map(|c| c.0)
        .or_else(|| parts.extensions.get::<SocketAddr>().map(SocketAddr::ip))
        .or_else(|| parts.extensions.get::<IpAddr>().copied())
}

impl Authenticator for DefaultAuthenticator {
    fn is_root_token(&self, token: &str) -> bool {
        self.is_root(token)
    }

    /// - `Control`: root token or `rtk_` key (bearer / `x-rt-key`); failures are limited per IP.
    /// - `Public`: a valid `rtk_` key gives a tenant principal, anything else is `Anonymous`
    ///   (the guest's own `Authorization` header is none of our business). Failed `rtk_` keys
    ///   are limited per client like control failures (`RateLimited` once blocked).
    /// - `Internal`: trusted peer; `x-rt-invoker` is honored here and nowhere else.
    /// - `Metrics`: anonymous.
    fn authenticate(&self, parts: &http::request::Parts, listener: Listener) -> Result<Principal, AuthError> {
        match listener {
            Listener::Metrics => Ok(Principal::Anonymous),
            Listener::Internal => {
                let invoker = parts
                    .headers
                    .get("x-rt-invoker")
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string);
                Ok(Principal::Internal { invoker })
            }
            Listener::Public => self.public(parts),
            Listener::Control => self.control(parts),
        }
    }
}
