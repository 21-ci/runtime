//! Shared state of the HTTP surfaces. `rt-api` must not depend on executors, `rt-build` parsing
//! or `rt-host` (layer rules): everything it needs from them arrives through the traits
//! below or through `rt-types` seams (`UploadValidator`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::collections::HashMap;

use arc_swap::{ArcSwap, ArcSwapOption};
use dashmap::DashMap;
use rt_config::Config;
use rt_mem::MemoryGovernor;
use rt_obs::{LogStore, Metrics, StatsStore};
use rt_sched::{Clock, Scheduler};
use rt_sec::{
    AuditSink, Authenticator, IpAllowList, KeyHasher, PlanSource, QuotaEnforcer, SecretsBackend, TrustedProxies,
};
use rt_store::{ArtifactStore, DelayedStore, MetaStore, QueueStore, ServiceDataStore};
use rt_triggers::{RouteTable, ShardedBucket, SocketHub};
use rt_types::{
    ApiKey, Digest, HttpRequest, KeyId, RouteEntry, RuntimeKind, Schedule, ScheduleId, ServiceId, TaskSpec, TraceCtx,
    UploadValidator, VersionNo,
};
use serde::Serialize;
use utoipa::ToSchema;

/// Something the control API changed that the rest of the node must react to.
#[derive(Clone, Debug)]
pub enum ControlEvent {
    /// A schedule was created or changed.
    ScheduleUpserted(Schedule),
    ScheduleRemoved(ScheduleId),
    /// A service was deleted: drop its schedules, caches, data handles.
    ServiceDeleted(ServiceId),
    /// A message was enqueued: wake the queue engine.
    QueueEnqueued(ServiceId),
    /// A build was requested (eager compile or rebuild).
    BuildRequested {
        service: ServiceId,
        version: VersionNo,
        digest: Digest,
        runtime: RuntimeKind,
        rebuild: bool,
    },
    /// An API key was created (carries the stored record, including its keyed hash).
    KeyUpserted(ApiKey),
    /// An API key was revoked: forget it immediately (SEC-06).
    KeyRevoked(KeyId),
    /// Env or secrets of a service changed.
    SecretsChanged(ServiceId),
    /// The route snapshot and the service table were re-read (a service was created, updated or
    /// deleted, a version went live, a tenant changed): keep what depends on them in step, such as
    /// the `scaling.min_instances` floors (D-B2).
    RoutesRefreshed,
}

/// Facts and actions only the server knows (cache, V8 build, readiness, live config apply).
/// Implemented by `rt-server`.
pub trait NodeInfo: Send + Sync {
    /// `GET /v1/node/cache`: tier sizes, hit rates, top entries.
    fn cache(&self) -> serde_json::Value;

    /// `status.v8_sandbox` (prebuilt `rusty_v8` ⇒ `false`, SEC-36).
    fn v8_sandbox(&self) -> bool;

    /// `/healthz`: sysmon heartbeat within the last second (OBS-10).
    fn healthy(&self) -> bool;

    /// `/readyz`: migrations applied, base snapshot loaded, not draining.
    fn ready(&self) -> bool;

    /// Apply `PUT /v1/node/config` live-updatable keys; returns the restart-only keys the caller
    /// asked to change (`pending_restart`, CFG-05).
    fn apply_live_config(&self, update: &NodeConfigUpdate) -> Result<Vec<String>, String>;

    /// Begin draining (`POST /v1/node/drain`).
    fn start_drain(&self);

    /// Reaction to a control change (schedules, queue, builds, key cache).
    fn notify(&self, ev: ControlEvent);

    /// Build the `TaskSpec` of an HTTP invocation (config resolution: limits, knobs, plan).
    /// `None` when the service cannot be run (e.g. unknown plan).
    fn http_task(&self, entry: &RouteEntry, req: HttpRequest, trace: Option<TraceCtx>) -> Option<TaskSpec>;

    /// Cache tier (`T1`/`T2`/`T3`) of the live artifact of `entry`.
    fn tier(&self, entry: &RouteEntry) -> &'static str {
        let _ = entry;
        "T3"
    }

    /// Crate version reported by `GET /v1/node/status`.
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

/// Body of `PUT /v1/node/config`.
#[derive(Debug, Clone, Default, serde::Deserialize, Serialize, ToSchema)]
pub struct NodeConfigUpdate {
    /// `normal` | `shed` | `drain`.
    #[serde(default)]
    pub degradation_mode: Option<String>,
    /// Profile name: `max_density` | `balanced` | `min_latency`.
    #[serde(default)]
    pub default_profile: Option<String>,
    /// Rate-limit tweaks (`rps`, `burst`).
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub rate_limit: Option<serde_json::Value>,
}

/// Cached idempotent response (docs/api.md: 24 h, per principal).
#[derive(Clone)]
pub struct IdemEntry {
    pub fingerprint: [u8; 32],
    pub status: u16,
    pub body: serde_json::Value,
    pub expires: Instant,
}

/// 24 h in-memory idempotency store, at most [`IDEMPOTENCY_MAX_ENTRIES`] entries: past it the
/// expired entries go, then the oldest eighth (CTRL-009).
pub struct IdemStore {
    map: DashMap<String, IdemEntry>,
    cap: usize,
    /// Requests with the same key take turns (striped by key hash), so a second request sent while
    /// the first still runs waits and replays its answer instead of running again (CTRL-008).
    turns: Box<[Arc<tokio::sync::Mutex<()>>]>,
}

pub const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 3600);
/// Idempotency entries kept per node.
pub const IDEMPOTENCY_MAX_ENTRIES: usize = 100_000;
/// Lock stripes for requests that carry an Idempotency-Key.
const IDEMPOTENCY_TURNS: usize = 256;
/// Longest `Idempotency-Key` accepted (`422` past it).
pub const IDEMPOTENCY_MAX_KEY_BYTES: usize = 255;

impl Default for IdemStore {
    fn default() -> Self {
        Self::with_capacity(IDEMPOTENCY_MAX_ENTRIES)
    }
}

impl IdemStore {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: DashMap::new(),
            cap: cap.max(8),
            turns: (0..IDEMPOTENCY_TURNS)
                .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                .collect(),
        }
    }

    /// Wait for `key`'s turn; the guard is held until the request has stored its answer.
    pub async fn turn(&self, key: &str) -> tokio::sync::OwnedMutexGuard<()> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::hash::DefaultHasher::new();
        key.hash(&mut hasher);
        let i = (hasher.finish() as usize) % self.turns.len();
        self.turns[i].clone().lock_owned().await
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<IdemEntry> {
        let e = self.map.get(key)?.clone();
        if e.expires <= Instant::now() {
            self.map.remove(key);
            return None;
        }
        Some(e)
    }

    pub fn put(&self, key: String, e: IdemEntry) {
        if self.map.len() >= self.cap {
            let now = Instant::now();
            self.map.retain(|_, v| v.expires > now);
            if self.map.len() >= self.cap {
                // Every entry is fresh: drop the oldest eighth at once, so the next eighth of
                // inserts does not scan again.
                let mut by_age: Vec<(Instant, String)> =
                    self.map.iter().map(|e| (e.value().expires, e.key().clone())).collect();
                by_age.sort_unstable();
                for (_, k) in by_age.into_iter().take((self.cap / 8).max(1)) {
                    self.map.remove(&k);
                }
            }
        }
        self.map.insert(key, e);
    }
}

/// At most `max` concurrent SSE streams per credential (SEC-28).
#[derive(Default)]
pub struct SseLimiter {
    counts: DashMap<String, Arc<AtomicUsize>>,
}

/// Releases the slot on drop.
pub struct SseSlot {
    counter: Arc<AtomicUsize>,
}

impl Drop for SseSlot {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Credentials tracked by [`SseLimiter`] before the ones with no open stream are dropped.
pub const SSE_TRACKED_MAX: usize = 1024;

impl SseLimiter {
    /// Credentials with a counter (tests of the pruning).
    pub fn tracked(&self) -> usize {
        self.counts.len()
    }

    pub fn acquire(&self, who: &str, max: usize) -> Option<SseSlot> {
        if self.counts.len() >= SSE_TRACKED_MAX && !self.counts.contains_key(who) {
            self.counts.retain(|_, c| c.load(Ordering::Acquire) > 0);
        }
        let counter = self.counts.entry(who.to_string()).or_default().clone();
        let prev = counter.fetch_add(1, Ordering::AcqRel);
        if prev >= max {
            counter.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(SseSlot { counter })
    }
}

/// Per-service ingress rate limiters (`ingress.rate_limit {rps, burst}`).
#[derive(Default)]
pub struct IngressBuckets {
    map: DashMap<ServiceId, (u32, u32, Arc<ShardedBucket>)>,
}

impl IngressBuckets {
    /// Drop the buckets of services `keep` rejects (deleted ones, on every route refresh).
    pub fn retain_services(&self, keep: impl Fn(&ServiceId) -> bool) {
        self.map.retain(|s, _| keep(s));
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn bucket(&self, clock: &Arc<dyn Clock>, svc: ServiceId, rps: u32, burst: u32) -> Arc<ShardedBucket> {
        if let Some(e) = self.map.get(&svc) {
            if e.0 == rps && e.1 == burst {
                return e.2.clone();
            }
            e.2.set_limit(u64::from(rps), u64::from(burst.max(1)));
            drop(e);
            if let Some(mut m) = self.map.get_mut(&svc) {
                m.0 = rps;
                m.1 = burst;
                return m.2.clone();
            }
        }
        // One shard: requests are spread over server threads, not Ms, and nothing rebalances these
        // buckets, so with several shards a single busy client would only ever see `burst / shards`.
        let b = Arc::new(ShardedBucket::new(
            clock.clone(),
            u64::from(rps),
            u64::from(burst.max(1)),
            1,
        ));
        self.map.insert(svc, (rps, burst, b.clone()));
        b
    }
}

/// Ingress rules parsed once and reused by every request: the trusted proxies of the current
/// config generation and each service's `ip_allow` CIDRs (instead of re-parsing them per request).
#[derive(Default)]
pub struct IngressCache {
    proxies: ArcSwapOption<ProxiesOf>,
    allow: DashMap<ServiceId, Arc<AllowOf>>,
}

/// `ingress.trusted_proxies` of one config generation.
struct ProxiesOf {
    config: Arc<Config>,
    proxies: Arc<TrustedProxies>,
}

/// One service's `ip_allow` strings and what they parse to.
struct AllowOf {
    rules: Vec<String>,
    list: Arc<IpAllowList>,
}

impl IngressCache {
    /// Drop the `ip_allow` lists of services `keep` rejects (deleted ones, on every route refresh).
    pub fn retain_services(&self, keep: impl Fn(&ServiceId) -> bool) {
        self.allow.retain(|s, _| keep(s));
    }

    pub fn allow_len(&self) -> usize {
        self.allow.len()
    }

    /// The trusted proxies of `config` (malformed entries skipped), parsed once per generation.
    pub fn trusted_proxies(&self, config: &Arc<Config>) -> Arc<TrustedProxies> {
        if let Some(p) = &*self.proxies.load() {
            if Arc::ptr_eq(&p.config, config) {
                return p.proxies.clone();
            }
        }
        let valid: Vec<&String> = config
            .ingress
            .trusted_proxies
            .iter()
            .filter(|t| rt_sec::parse_cidr(t).is_ok())
            .collect();
        let proxies = Arc::new(TrustedProxies::parse(&valid).unwrap_or_default());
        self.proxies.store(Some(Arc::new(ProxiesOf {
            config: config.clone(),
            proxies: proxies.clone(),
        })));
        proxies
    }

    /// `service`'s `ip_allow` list, parsed once per rule set. Malformed entries never match, so a
    /// list of only malformed entries admits nobody (as `rt_sec::ip_allowed`).
    pub fn ip_allow(&self, service: ServiceId, rules: &[String]) -> Arc<IpAllowList> {
        if let Some(a) = self.allow.get(&service) {
            if a.rules == rules {
                return a.list.clone();
            }
        }
        let list = Arc::new(IpAllowList::parse_lossy(rules));
        self.allow.insert(
            service,
            Arc::new(AllowOf {
                rules: rules.to_vec(),
                list: list.clone(),
            }),
        );
        list
    }
}

/// Everything the handlers need. Cheap to clone.
#[derive(Clone)]
pub struct ApiState {
    pub config: Arc<ArcSwap<Config>>,
    pub meta: Arc<dyn MetaStore>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub queue: Arc<dyn QueueStore>,
    /// One-off schedules of `std:schedule` (listed by `GET /v1/services/{s}/schedules/once`).
    pub delayed: Arc<dyn DelayedStore>,
    pub data: Arc<dyn ServiceDataStore>,
    pub scheduler: Scheduler,
    pub routes: Arc<RouteTable>,
    pub auth: Arc<dyn Authenticator>,
    pub quotas: Arc<dyn QuotaEnforcer>,
    pub plans: Arc<dyn PlanSource>,
    pub audit: Arc<dyn AuditSink>,
    pub secrets: Arc<dyn SecretsBackend>,
    pub hasher: KeyHasher,
    pub validator: Arc<dyn UploadValidator>,
    pub logs: Arc<dyn LogStore>,
    pub stats: Arc<dyn StatsStore>,
    pub metrics: Arc<Metrics>,
    pub mem: Arc<dyn MemoryGovernor>,
    pub clock: Arc<dyn Clock>,
    pub node: Arc<dyn NodeInfo>,
    pub idem: Arc<IdemStore>,
    pub sse: Arc<SseLimiter>,
    pub buckets: Arc<IngressBuckets>,
    /// Every service (refreshed by `refresh_routes` on each control change): config lookups on
    /// the invoke path without touching `meta.db`.
    pub service_table: Arc<ArcSwap<HashMap<ServiceId, Arc<rt_types::Service>>>>,
    /// Every tenant (kept in sync by the tenant routes): plan lookups.
    pub tenant_table: Arc<DashMap<rt_types::TenantId, rt_types::Tenant>>,
    /// Parsed trusted proxies and `ip_allow` lists for the invoke path.
    pub ingress: Arc<IngressCache>,
    /// The node's WebSockets (upgrades bind here; the control API lists and closes them).
    pub sockets: Arc<SocketHub>,
    /// Artifact uploads buffered at once ([`crate::control::MAX_CONCURRENT_UPLOADS`] permits).
    pub uploads: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("ip")
    }

    fn idem(expires_in: Duration) -> IdemEntry {
        IdemEntry {
            fingerprint: [0; 32],
            status: 201,
            body: serde_json::Value::Null,
            expires: Instant::now() + expires_in,
        }
    }

    /// CTRL-009: the idempotency store stays bounded even when every entry is fresh (within its
    /// 24 h TTL), and an insert past the bound does not scan the whole map each time.
    #[test]
    fn ctrl_009_the_idempotency_store_is_bounded_with_fresh_entries() {
        let s = IdemStore::with_capacity(1_000);
        let start = Instant::now();
        for i in 0..50_000 {
            s.put(format!("op|k{i}"), idem(IDEMPOTENCY_TTL));
        }
        assert!(s.len() <= 1_000, "{} entries", s.len());
        assert!(s.get("op|k49999").is_some(), "the newest entry is kept");
        assert!(s.get("op|k0").is_none(), "the oldest went first");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "amortized, not a full scan per insert: {:?}",
            start.elapsed()
        );
        // Expired entries go before any fresh one is evicted.
        let s = IdemStore::with_capacity(10);
        for i in 0..5 {
            s.put(format!("old{i}"), idem(Duration::ZERO));
        }
        for i in 0..10 {
            s.put(format!("new{i}"), idem(IDEMPOTENCY_TTL));
        }
        assert!((0..10).all(|i| s.get(&format!("new{i}")).is_some()));
    }

    /// Step 5c (unbounded maps): per-credential SSE counters and per-service ingress state do not
    /// outlive their credential or service forever.
    #[test]
    fn per_key_and_per_service_maps_are_pruned() {
        let sse = SseLimiter::default();
        for i in 0..5_000 {
            drop(sse.acquire(&format!("key-{i}"), 1).expect("slot"));
        }
        let held = sse.acquire("busy", 1).expect("slot");
        assert!(sse.tracked() <= SSE_TRACKED_MAX + 1, "{}", sse.tracked());
        assert!(sse.acquire("busy", 1).is_none(), "a held slot is never pruned");
        drop(held);

        let clock: Arc<dyn Clock> = Arc::new(rt_sched::SystemClock);
        let (a, b) = (ServiceId::from_u128(1), ServiceId::from_u128(2));
        let buckets = IngressBuckets::default();
        buckets.bucket(&clock, a, 10, 10);
        buckets.bucket(&clock, b, 10, 10);
        let cache = IngressCache::default();
        cache.ip_allow(a, &["10.0.0.0/8".to_string()]);
        cache.ip_allow(b, &["10.0.0.0/8".to_string()]);
        buckets.retain_services(|s| *s == a);
        cache.retain_services(|s| *s == a);
        assert_eq!((buckets.len(), cache.allow_len()), (1, 1));
    }

    #[test]
    fn trusted_proxies_are_parsed_once_per_config_generation() {
        let cache = IngressCache::default();
        let mut c = Config::default();
        c.ingress.trusted_proxies = vec!["10.0.0.0/8".into(), "not-a-cidr".into()];
        let first = Arc::new(c.clone());
        let a = cache.trusted_proxies(&first);
        assert!(
            Arc::ptr_eq(&a, &cache.trusted_proxies(&first)),
            "same generation: reused"
        );
        assert!(
            a.is_trusted(ip("10.1.2.3")),
            "valid entries apply, malformed ones are skipped"
        );

        c.ingress.trusted_proxies = vec!["192.168.0.0/16".into()];
        let reloaded = cache.trusted_proxies(&Arc::new(c));
        assert!(!Arc::ptr_eq(&a, &reloaded), "a new generation is parsed again");
        assert!(reloaded.is_trusted(ip("192.168.1.1")) && !reloaded.is_trusted(ip("10.1.2.3")));
    }

    #[test]
    fn ip_allow_lists_are_parsed_once_per_rule_set_and_keep_their_semantics() {
        let cache = IngressCache::default();
        let s = ServiceId::from_u128(1);
        let rules = vec!["10.0.0.0/8".to_string(), "2001:db8::/32".to_string()];
        let a = cache.ip_allow(s, &rules);
        assert!(Arc::ptr_eq(&a, &cache.ip_allow(s, &rules)), "same rules: reused");
        assert!(a.allows(ip("10.9.9.9")) && a.allows(ip("::ffff:10.0.0.1")) && a.allows(ip("2001:db8::1")));
        assert!(!a.allows(ip("192.168.0.1")));

        let changed = cache.ip_allow(s, &["192.168.0.0/16".to_string()]);
        assert!(
            changed.allows(ip("192.168.0.1")) && !changed.allows(ip("10.9.9.9")),
            "a rule change applies"
        );

        let broken = cache.ip_allow(ServiceId::from_u128(2), &["nonsense".to_string()]);
        assert!(
            !broken.allows(ip("10.9.9.9")),
            "a list with only malformed entries fails closed"
        );
    }
}
