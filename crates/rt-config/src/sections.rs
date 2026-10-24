//! One struct per `rt.toml` section (docs/config.md + review additions).
//!
//! Every section is `deny_unknown_fields` and `default`, so an empty file yields the built-in
//! (`balanced`) configuration. Knobs that profiles control are `Option<T>`: `None` means "left
//! unset, let the profile fill it" (see [`crate::profile`] and [`crate::resolve`]).

#![allow(clippy::derivable_impls)] // explicit defaults document the built-in `balanced` values

use std::collections::BTreeMap;
use std::time::Duration;

use rt_types::{CompileMode, Plan, Profile};
use serde::{Deserialize, Serialize};

use crate::serde_duration;

macro_rules! section {
    ($(#[$m:meta])* pub struct $name:ident { $($body:tt)* }) => {
        $(#[$m])*
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields, default)]
        pub struct $name { $($body)* }
    };
}

section! {
    /// `[node]`
    pub struct NodeConfig {
        pub data_dir: String,
        pub default_profile: Profile,
    }
}
impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            data_dir: "/data".into(),
            default_profile: Profile::Balanced,
        }
    }
}

section! {
    /// `[listeners]` — `host:port` or `unix:/path`.
    pub struct ListenersConfig {
        pub public: String,
        pub internal: String,
        pub control: String,
        pub metrics: String,
        /// UIDs besides the node's own that may connect to the internal Unix socket (checked with
        /// `SO_PEERCRED` / `getpeereid` before serving). Root (`0`) only when listed.
        pub internal_allowed_uids: Vec<u32>,
        /// Allow `internal` to be a (loopback) TCP address. A TCP listener cannot check who
        /// connects, so every local user gets trusted invokes and `x-rt-invoker`; off by default.
        pub allow_tcp_internal: bool,
    }
}
impl Default for ListenersConfig {
    fn default() -> Self {
        Self {
            public: "0.0.0.0:8080".into(),
            internal: "unix:/data/rt.sock".into(),
            control: "127.0.0.1:8081".into(),
            metrics: "127.0.0.1:9090".into(),
            internal_allowed_uids: Vec::new(),
            allow_tcp_internal: false,
        }
    }
}

section! {
    /// `[hosts]` — tenant custom-host claims (SECURITY.md).
    pub struct HostsConfig {
        /// A claim still `pending_verification` after this long expires: it is deleted and stops
        /// blocking other services.
        #[serde(with = "serde_duration")]
        pub pending_ttl: Duration,
        /// Look up `_rt-verify.<host>` TXT records of pending claims; a record equal to the
        /// claim's `verify_token` activates the host. `false` = operator approval only.
        pub verify_dns: bool,
        /// How often pending claims are checked (DNS and expiry).
        #[serde(with = "serde_duration")]
        pub verify_interval: Duration,
        /// Pending claims one tenant may hold at once (`403 quota_exceeded` beyond).
        pub max_pending_per_tenant: u32,
    }
}
impl Default for HostsConfig {
    fn default() -> Self {
        Self {
            pending_ttl: Duration::from_secs(7 * 24 * 3600),
            verify_dns: true,
            verify_interval: Duration::from_secs(60),
            max_pending_per_tenant: 10,
        }
    }
}

section! {
    /// `[routing]`
    pub struct RoutingConfig {
        /// e.g. `fn.example.com` → `{slug}.fn.example.com`. Empty = disabled.
        pub base_domain: String,
    }
}
impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            base_domain: String::new(),
        }
    }
}

section! {
    /// `[sched.kinds.<deno|wasm>]`
    pub struct KindRangeConfig {
        pub min: u32,
        /// 0 = `processors - other kinds' mins`.
        pub max: u32,
    }
}
impl Default for KindRangeConfig {
    fn default() -> Self {
        Self { min: 1, max: 0 }
    }
}

section! {
    /// `[sched.kinds]`
    pub struct SchedKindsConfig {
        pub deno: KindRangeConfig,
        pub wasm: KindRangeConfig,
    }
}
impl Default for SchedKindsConfig {
    fn default() -> Self {
        Self {
            deno: KindRangeConfig::default(),
            wasm: KindRangeConfig::default(),
        }
    }
}

section! {
    /// `[sched]`. Profile-controlled knobs are `Option`.
    pub struct SchedConfig {
        /// 0 = cgroup cpu quota / cores.
        pub processors: u32,
        pub spare_machines: u32,
        pub lrq_capacity: u32,
        /// Run slots node-wide; tasks beyond it wait in their service's queue (D-A3).
        pub max_inflight: u32,
        /// Node bound on queued tasks; over it `503 overloaded`. 0 = auto (4 x `max_inflight`).
        pub max_queued: u32,
        /// Node bound on the bytes held by queued tasks, in MB. 0 = auto (1/16 of the memory budget).
        pub max_queued_mb: u32,
        /// Largest `scaling.min_run_time_ms` any service may set on this node (the plan may lower it).
        pub min_run_time_max_ms: u64,
        /// `k` in the admission deadline key `since + min(k * pred, queue_timeout / 2)`;
        /// 0 = arrival order within a tenant.
        pub aging_factor: u32,
        pub kinds: SchedKindsConfig,
        /// Profile knob (node): spin duration in microseconds before parking.
        pub spin_us: Option<u64>,
        /// Profile knob (node): preemption time slice.
        #[serde(with = "serde_duration::opt")]
        pub time_slice: Option<Duration>,
        /// Profile knob (node).
        pub pin_threads: Option<bool>,
        /// Profile knob (node).
        pub handoff_on_hog: Option<bool>,
        /// Profile knob (service): shed tasks not started within this time.
        #[serde(with = "serde_duration::opt")]
        pub queue_timeout: Option<Duration>,
        /// Profile knob (node): share of the memory budget warm instances may use (D-A3).
        pub warm_share: Option<f64>,
    }
}
impl Default for SchedConfig {
    fn default() -> Self {
        Self {
            processors: 0,
            spare_machines: 2,
            lrq_capacity: 256,
            max_inflight: 10_000,
            max_queued: 0,
            max_queued_mb: 0,
            min_run_time_max_ms: 600_000,
            aging_factor: 4,
            kinds: SchedKindsConfig::default(),
            spin_us: None,
            time_slice: None,
            pin_threads: None,
            handoff_on_hog: None,
            queue_timeout: None,
            warm_share: None,
        }
    }
}

section! {
    /// `[rebalance]`
    pub struct RebalanceConfig {
        pub up_threshold: f64,
        #[serde(with = "serde_duration")]
        pub cooldown: Duration,
        /// Profile knob (node).
        #[serde(with = "serde_duration::opt")]
        pub interval: Option<Duration>,
    }
}
impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            up_threshold: 2.0,
            cooldown: Duration::from_secs(1),
            interval: None,
        }
    }
}

section! {
    /// `[sysmon]`
    pub struct SysmonConfig {
        /// Profile knob (node): upper bound of the adaptive tick.
        #[serde(with = "serde_duration::opt")]
        pub max_tick: Option<Duration>,
    }
}
impl Default for SysmonConfig {
    fn default() -> Self {
        Self { max_tick: None }
    }
}

section! {
    /// `[io]` — dedicated I/O runtime (S-07 variant A).
    pub struct IoConfig {
        pub threads: u32,
    }
}
impl Default for IoConfig {
    fn default() -> Self {
        Self { threads: 2 }
    }
}

section! {
    /// `[blocking]` — the per-tenant fair pool that runs service-data (SQLite, blob) work.
    pub struct BlockingConfig {
        /// Worker threads; 0 = auto, `max(4, available cores)` ([`BlockingConfig::effective_threads`]).
        /// One tenant may occupy `floor(plan.blocking_share × threads)` of them, at least 1.
        pub threads: u32,
    }
}
impl Default for BlockingConfig {
    fn default() -> Self {
        Self { threads: 0 }
    }
}
impl BlockingConfig {
    /// The thread count to start: `threads`, or `max(4, available cores)` when it is 0.
    pub fn effective_threads(&self) -> usize {
        match self.threads {
            0 => std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .max(4),
            n => n as usize,
        }
    }
}

section! {
    /// `[memory]`
    pub struct MemoryConfig {
        /// 0 = 85 % of cgroup `memory.max` (else physical RAM).
        pub budget_mb: u64,
        #[serde(with = "serde_duration")]
        pub check_interval: Duration,
        /// Profile knob (node): high watermark as a fraction of the budget.
        pub high: Option<f64>,
        /// Profile knob (node): critical watermark.
        pub critical: Option<f64>,
        /// Profile knob (node).
        #[serde(with = "serde_duration::opt")]
        pub purge_min_interval: Option<Duration>,
    }
}
impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            budget_mb: 0,
            check_interval: Duration::from_millis(250),
            high: None,
            critical: None,
            purge_min_interval: None,
        }
    }
}

section! {
    /// `[cache]`
    pub struct CacheConfig {
        pub t2_max_mb: u64,
        /// Profile knob (node): fraction of the memory budget for T1.
        pub t1_share: Option<f64>,
        /// Profile knob (service): T1 idle TTL.
        #[serde(with = "serde_duration::opt")]
        pub idle_ttl: Option<Duration>,
        /// Profile knob (service): preload this service's top-N membership at startup.
        pub preload: Option<bool>,
        /// Top-N services by persisted frequency to preload when `preload` applies.
        pub preload_top_n: u32,
    }
}
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            t2_max_mb: 4096,
            t1_share: None,
            idle_ttl: None,
            preload: None,
            preload_top_n: 100,
        }
    }
}

section! {
    /// `[disk]`
    pub struct DiskConfig {
        pub low_free_mb: u64,
        pub high_free_mb: u64,
    }
}
impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            low_free_mb: 2048,
            high_free_mb: 4096,
        }
    }
}

section! {
    /// `[deno]`
    pub struct DenoConfig {
        pub isolate_base_mb: u32,
        pub v8_platform_threads: u32,
        pub dispose_backlog: u32,
        pub v8_flags: Vec<String>,
        /// Profile knob (node).
        pub max_bound_per_m: Option<u32>,
        /// Profile knob (node).
        pub isolate_pool_per_p: Option<u32>,
        /// Profile knob (service): keep evaluated isolates per service version (D-A4).
        pub reuse_isolates: Option<bool>,
        /// Kept isolates per service version on one Machine when the scheduler publishes no
        /// warm target for it.
        pub reuse_max_idle: u32,
        /// Kept isolates per Machine across services (the oldest go first).
        pub reuse_max_idle_total: u32,
        /// A kept isolate unused this long is disposed.
        #[serde(with = "serde_duration")]
        pub reuse_idle_ttl: Duration,
        /// Tasks one kept isolate serves before it is replaced.
        pub reuse_max_uses: u32,
    }
}
impl Default for DenoConfig {
    fn default() -> Self {
        Self {
            isolate_base_mb: 4,
            v8_platform_threads: 1,
            dispose_backlog: 8,
            v8_flags: vec!["--single-threaded-gc".into()],
            max_bound_per_m: None,
            isolate_pool_per_p: None,
            reuse_isolates: None,
            reuse_max_idle: 4,
            reuse_max_idle_total: 16,
            reuse_idle_ttl: Duration::from_secs(60),
            reuse_max_uses: 1000,
        }
    }
}

section! {
    /// `[wasm]`
    pub struct WasmConfig {
        pub fuel: bool,
        pub reuse_max_idle: u32,
        /// Profile knob (node): linear memory kept resident on slot release, **in MB**
        /// (0 / 1 / 8 per profile; the resolved knob is in bytes).
        pub linear_memory_keep_resident: Option<u64>,
        /// Profile knob (service).
        pub reuse_instances: Option<bool>,
        /// Profile knob (node): pooling-allocator slots kept warm (affine to their module) after
        /// release (0 / 100 / 1000 per profile; capped at the pool size).
        pub max_unused_warm_slots: Option<u32>,
    }
}
impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            fuel: false,
            reuse_max_idle: 8,
            linear_memory_keep_resident: None,
            reuse_instances: None,
            max_unused_warm_slots: None,
        }
    }
}

section! {
    /// `[build]`
    pub struct BuildConfig {
        pub threads: u32,
        pub memory_mb: u32,
        /// Deno build timeout.
        #[serde(with = "serde_duration")]
        pub timeout: Duration,
        /// WASM build timeout.
        #[serde(with = "serde_duration")]
        pub wasm_timeout: Duration,
        /// Per-module fetch timeout.
        #[serde(with = "serde_duration")]
        pub fetch_timeout: Duration,
        pub npm_cdn: String,
        pub allowed_hosts: Vec<String>,
        pub max_deps_mb: u32,
        /// Profile knob (service).
        pub compile: Option<CompileMode>,
    }
}
impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            memory_mb: 256,
            timeout: Duration::from_secs(60),
            wasm_timeout: Duration::from_secs(120),
            fetch_timeout: Duration::from_secs(30),
            npm_cdn: "https://esm.sh".into(),
            allowed_hosts: ["esm.sh", "jsr.io", "deno.land", "cdn.jsdelivr.net", "unpkg.com"]
                .into_iter()
                .map(String::from)
                .collect(),
            max_deps_mb: 50,
            compile: None,
        }
    }
}

section! {
    /// `[deploy]`
    pub struct DeployConfig {
        pub retain_versions: u32,
        pub auto_rollback: bool,
    }
}
impl Default for DeployConfig {
    fn default() -> Self {
        Self {
            retain_versions: 10,
            auto_rollback: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlobBackendKind {
    #[default]
    Local,
    S3,
}

section! {
    /// `[storage]`
    pub struct StorageConfig {
        pub blob_backend: BlobBackendKind,
        /// Cap on open SQLite connections of service databases, writers and readers together;
        /// idle services are closed whole, least recently used first.
        pub max_open_dbs: u32,
        /// Read-only connections per service database next to its one writer (0 = reads use
        /// the writer). Opened on demand under concurrent reads.
        pub readers_per_db: u32,
        /// SQLite page-cache budget of all service-database connections together, in MB; each
        /// connection gets `sql_cache_mb / max_open_dbs` (`PRAGMA cache_size`). 0 = SQLite's default.
        pub sql_cache_mb: u64,
        pub blob_cache_mb: u64,
        /// SQLite progress-handler budget per statement (`sql_timeout`), in ms.
        pub sql_budget_ms: u64,
    }
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            blob_backend: BlobBackendKind::Local,
            max_open_dbs: 256,
            readers_per_db: 4,
            sql_cache_mb: 512,
            blob_cache_mb: 512,
            sql_budget_ms: 1000,
        }
    }
}

section! {
    /// `[s3]` (stretch; trait + local backend in the MVP)
    pub struct S3Config {
        pub endpoint: String,
        pub bucket: String,
        pub prefix: String,
        pub region: String,
    }
}
impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bucket: String::new(),
            prefix: "rt".into(),
            region: "us-east-1".into(),
        }
    }
}

/// `security.clock`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClockMode {
    /// `Date.now()`/`performance.now()`/WASI clocks advance only at I/O and timer boundaries.
    #[default]
    Frozen,
    /// Coarsened to 1 ms steps.
    Coarse,
}

section! {
    /// `[security]`
    pub struct SecurityConfig {
        /// Linux best-effort (stretch).
        pub seccomp: bool,
        pub clock: ClockMode,
        /// Official images: `true` (refuses to start without a V8 sandbox). The MVP uses prebuilt
        /// `rusty_v8` (no sandbox), so the shipped defconfigs set it `false`.
        pub require_v8_sandbox: bool,
        pub reserved_hosts: Vec<String>,
        pub master_key_file: String,
    }
}
impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            seccomp: true,
            clock: ClockMode::Frozen,
            require_v8_sandbox: true,
            reserved_hosts: Vec::new(),
            master_key_file: String::new(),
        }
    }
}

section! {
    /// `[supervisor]`
    pub struct SupervisorConfig {
        pub reserve_mb: u32,
        #[serde(with = "serde_duration::vec")]
        pub executor_restart_backoff: Vec<Duration>,
    }
}
impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            reserve_mb: 30,
            executor_restart_backoff: vec![
                Duration::from_millis(100),
                Duration::from_secs(1),
                Duration::from_secs(5),
            ],
        }
    }
}

section! {
    /// `[secrets]`
    pub struct SecretsConfig {
        /// 0 = fetch per task.
        #[serde(with = "serde_duration")]
        pub cache_ttl: Duration,
        /// Anomaly threshold for executor secret requests.
        pub max_services_per_min: u32,
    }
}
impl Default for SecretsConfig {
    fn default() -> Self {
        Self {
            cache_ttl: Duration::from_secs(60),
            max_services_per_min: 200,
        }
    }
}

section! {
    /// `[ingress]`
    pub struct IngressConfig {
        /// CIDRs whose `X-Forwarded-For` is trusted. A trusted proxy is also exempt from
        /// `max_conns_per_ip` (all of its clients share its address).
        pub trusted_proxies: Vec<String>,
        #[serde(with = "serde_duration")]
        pub header_timeout: Duration,
        #[serde(with = "serde_duration")]
        pub idle_timeout: Duration,
        /// Open connections per peer IP; extra ones are closed at accept
        /// (`rt_ingress_conn_rejected_total`).
        pub max_conns_per_ip: u32,
        pub h2_max_streams: u32,
        pub sse_streams_per_key: u32,
        /// `listen(2)` backlog of the TCP listeners (the kernel caps it at `kern.ipc.somaxconn`
        /// / `net.core.somaxconn`).
        pub backlog: u32,
    }
}
impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            trusted_proxies: Vec::new(),
            header_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            max_conns_per_ip: 256,
            h2_max_streams: 100,
            sse_streams_per_key: 10,
            backlog: 4096,
        }
    }
}

section! {
    /// `[egress]`
    pub struct EgressConfig {
        pub denied_ports: Vec<u16>,
        pub denied_hosts: Vec<String>,
        /// Added to the automatic self-denylist.
        pub denied_cidrs: Vec<String>,
        /// Follow `https` → `http` redirects. Off: such a fetch fails with
        /// `egress_insecure_redirect` (SECURITY.md SEC-50).
        pub allow_https_downgrade: bool,
    }
}
impl Default for EgressConfig {
    fn default() -> Self {
        Self {
            denied_ports: vec![25, 465, 587],
            denied_hosts: Vec::new(),
            denied_cidrs: Vec::new(),
            allow_https_downgrade: false,
        }
    }
}

section! {
    /// `[queue]`
    pub struct QueueConfig {
        pub batch_size: u32,
        pub max_attempts: u32,
        /// Per-service concurrent queue tasks (within the service's `max_concurrency`).
        pub max_concurrency: u32,
    }
}
impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            max_attempts: 5,
            max_concurrency: 4,
        }
    }
}

section! {
    /// `[ws]`: the node's WebSockets (docs/handlers.md "WebSockets").
    pub struct WsConfig {
        /// Open sockets on the node (every tenant together); an upgrade over it is refused.
        pub max_sockets: u32,
        /// Server pings every interval; a socket silent for two intervals is dropped.
        #[serde(with = "serde_duration")]
        pub ping_interval: Duration,
        /// KiB queued for one client before the socket is closed with 1013 (slow consumer).
        pub send_buffer_kb: u32,
        /// Events queued per object; a full inbox stops reading that object's sockets.
        pub inbox_max: u32,
        /// Deliveries of a socket event before it is given up (a message's socket then closes 1011).
        pub max_event_attempts: u32,
        /// Deliveries of a durable object's alarm before it is dropped.
        pub max_alarm_attempts: u32,
        /// Messages one client may send per second, and the burst above it.
        pub msgs_per_sec: u32,
        pub msgs_burst: u32,
        /// A client throttled without a break this long is closed with 1008.
        #[serde(with = "serde_duration")]
        pub flood_close_after: Duration,
        /// How long a drain waits for event tasks after closing the sockets (1001).
        #[serde(with = "serde_duration")]
        pub drain_grace: Duration,
    }
}
impl Default for WsConfig {
    fn default() -> Self {
        Self {
            max_sockets: 20_000,
            ping_interval: Duration::from_secs(30),
            send_buffer_kb: 1024,
            inbox_max: 1024,
            max_event_attempts: 3,
            max_alarm_attempts: 6,
            msgs_per_sec: 50,
            msgs_burst: 200,
            flood_close_after: Duration::from_secs(10),
            drain_grace: Duration::from_secs(5),
        }
    }
}

section! {
    /// `[logs]`
    pub struct LogsConfig {
        pub ring_lines: u32,
        pub retention_days: u32,
        pub max_mb_per_service: u64,
        pub stdout: bool,
    }
}
impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            ring_lines: 1000,
            retention_days: 7,
            max_mb_per_service: 50,
            stdout: false,
        }
    }
}

section! {
    /// `[metrics]`
    pub struct MetricsConfig {
        pub per_service: bool,
        pub per_service_top_n: u32,
    }
}
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            per_service: false,
            per_service_top_n: 100,
        }
    }
}

section! {
    /// `[otel]` (stretch)
    pub struct OtelConfig {
        pub endpoint: String,
        pub sample_ratio: f64,
    }
}
impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            sample_ratio: 0.01,
        }
    }
}

section! {
    /// `[[binding]]` operator platform binding (`std:platform`).
    pub struct BindingConfig {
        pub name: String,
        pub base_url: String,
        /// Header name → value, where a value of the form `env:NAME` is read from the node env.
        pub inject_headers: BTreeMap<String, String>,
        pub tenant_header: String,
        pub allowed_plans: Vec<String>,
        pub allowed_methods: Vec<String>,
        pub allowed_paths: Vec<String>,
        pub rate_limit: BindingRateLimit,
    }
}
impl Default for BindingConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            base_url: String::new(),
            inject_headers: BTreeMap::new(),
            tenant_header: String::new(),
            allowed_plans: Vec::new(),
            allowed_methods: Vec::new(),
            allowed_paths: Vec::new(),
            rate_limit: BindingRateLimit::default(),
        }
    }
}

section! {
    /// Per-tenant rate limit of a binding.
    pub struct BindingRateLimit {
        pub rps: u32,
        pub burst: u32,
    }
}
impl Default for BindingRateLimit {
    fn default() -> Self {
        Self { rps: 0, burst: 0 }
    }
}

/// Plans by name (`[plans.<name>]`). A `free` plan always exists.
pub type Plans = BTreeMap<String, Plan>;
