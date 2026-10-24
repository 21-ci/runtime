//! rt-config (L1): config structs for every key in docs/config.md, profiles, loading
//! (`rt.toml` + `RT__SECTION__KEY` env overlay), strict unknown-key checking, effective-value
//! resolution with provenance, and `--print-config`.
//!
//! Precedence (docs/config.md): built-in defaults (= `balanced`) < `rt.toml` < `RT__…` env.
//! Secrets (`RT_ROOT_TOKEN`, `RT_MASTER_KEY`, and their `*_FILE` variants) never live in the file.

mod loader;
pub mod profile;
pub mod resolve;
mod secrets;
pub mod sections;
pub mod serde_duration;
mod validate;

use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

pub use resolve::{resolve_node, resolve_service, EffectiveService, NodeKnobs, PlanConflict, Resolved, Source};
pub use rt_types::{CompileMode, Plan, Profile};
pub use sections::*;

/// Environment variable prefix for the overlay: `RT__SCHED__PROCESSORS=8`.
pub const ENV_PREFIX: &str = "RT__";

/// The whole configuration (`rt.toml`). `Default` is the built-in `balanced` configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub node: NodeConfig,
    pub listeners: ListenersConfig,
    pub routing: RoutingConfig,
    pub hosts: HostsConfig,
    pub sched: SchedConfig,
    pub rebalance: RebalanceConfig,
    pub sysmon: SysmonConfig,
    pub io: IoConfig,
    pub blocking: BlockingConfig,
    pub memory: MemoryConfig,
    pub cache: CacheConfig,
    pub disk: DiskConfig,
    pub deno: DenoConfig,
    pub wasm: WasmConfig,
    pub build: BuildConfig,
    pub deploy: DeployConfig,
    pub storage: StorageConfig,
    pub s3: S3Config,
    pub security: SecurityConfig,
    pub supervisor: SupervisorConfig,
    pub secrets: SecretsConfig,
    pub ingress: IngressConfig,
    pub egress: EgressConfig,
    pub queue: QueueConfig,
    pub ws: WsConfig,
    pub logs: LogsConfig,
    pub metrics: MetricsConfig,
    pub otel: OtelConfig,
    pub plans: Plans,
    pub binding: Vec<BindingConfig>,
}

impl Default for Config {
    fn default() -> Self {
        let mut plans = Plans::new();
        plans.insert("free".to_string(), Plan::default());
        Self {
            node: NodeConfig::default(),
            listeners: ListenersConfig::default(),
            routing: RoutingConfig::default(),
            hosts: HostsConfig::default(),
            sched: SchedConfig::default(),
            rebalance: RebalanceConfig::default(),
            sysmon: SysmonConfig::default(),
            io: IoConfig::default(),
            blocking: BlockingConfig::default(),
            memory: MemoryConfig::default(),
            cache: CacheConfig::default(),
            disk: DiskConfig::default(),
            deno: DenoConfig::default(),
            wasm: WasmConfig::default(),
            build: BuildConfig::default(),
            deploy: DeployConfig::default(),
            storage: StorageConfig::default(),
            s3: S3Config::default(),
            security: SecurityConfig::default(),
            supervisor: SupervisorConfig::default(),
            secrets: SecretsConfig::default(),
            ingress: IngressConfig::default(),
            egress: EgressConfig::default(),
            queue: QueueConfig::default(),
            ws: WsConfig::default(),
            logs: LogsConfig::default(),
            metrics: MetricsConfig::default(),
            otel: OtelConfig::default(),
            plans,
            binding: Vec::new(),
        }
    }
}

/// Errors from loading or validating a configuration. Messages carry the key path.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
    /// TOML syntax error (with line/column in the message).
    #[error("invalid toml: {0}")]
    Parse(String),
    /// `unknown key sched.procesors`.
    #[error("unknown key {0}")]
    UnknownKey(String),
    /// A section or key that was removed, with what replaces it.
    #[error("`{key}` was removed: {hint}")]
    Removed { key: String, hint: &'static str },
    /// `invalid value for sched.processors: expected integer`.
    #[error("invalid value for {key}: {reason}")]
    InvalidValue { key: String, reason: String },
    /// Bad `RT__…` variable.
    #[error("invalid environment override {var}: {reason}")]
    Env { var: String, reason: String },
    /// Cross-field validation failures (all of them).
    #[error("invalid configuration: {}", .0.join("; "))]
    Validation(Vec<String>),
    #[error("secret `{name}`: {reason}")]
    Secret { name: &'static str, reason: String },
}

/// Inputs to [`Config::load`].
#[derive(Clone, Default)]
pub struct LoadOptions {
    /// `--config <path>`; when `None`, `$RT_DATA/rt.toml` is used if it exists.
    pub file: Option<PathBuf>,
    /// Environment as `(name, value)` pairs. Inject for tests; use
    /// [`LoadOptions::with_process_env`] in the binary.
    pub env: Vec<(String, String)>,
}

/// Names only: the env holds `RT_ROOT_TOKEN` and `RT_MASTER_KEY`.
impl std::fmt::Debug for LoadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.env.iter().map(|(k, _)| k.as_str()).collect();
        f.debug_struct("LoadOptions")
            .field("file", &self.file)
            .field("env", &names)
            .finish()
    }
}

impl LoadOptions {
    /// Same options but with the current process environment.
    pub fn with_process_env(mut self) -> Self {
        self.env = std::env::vars().collect();
        self
    }
}

/// A secret value from env/file. Redacted in `Debug`, zeroized on drop.
#[derive(Clone)]
pub struct Secret(pub Zeroizing<String>);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Secret(Zeroizing::new(value.into()))
    }

    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// Secrets only ever sourced from env/files (docs/config.md source 4).
#[derive(Clone, Debug, Default)]
pub struct NodeSecrets {
    /// `RT_ROOT_TOKEN` / `RT_ROOT_TOKEN_FILE`. `None` ⇒ control routes answer `503 control_disabled`
    /// in release builds.
    pub root_token: Option<Secret>,
    /// `RT_MASTER_KEY` (hex, 32 bytes) / `RT_MASTER_KEY_FILE` / `security.master_key_file`.
    pub master_key: Option<Secret>,
    /// `RT_MASTER_KEY_PREVIOUS` / `RT_MASTER_KEY_PREVIOUS_FILE` (hex, 32 bytes): during a key
    /// rotation, still opens secrets and verifies API keys made under the old key; never used to
    /// seal (docs/deploy.md "Rotating the master key").
    pub master_key_previous: Option<Secret>,
}

/// A loaded configuration plus where it came from.
#[derive(Clone, Debug)]
pub struct LoadedConfig {
    pub config: Config,
    pub secrets: NodeSecrets,
    /// The file that was read, if any.
    pub file: Option<PathBuf>,
    /// Keys set through the env overlay (`sched.processors`, …), for `print`/diagnostics.
    pub env_overrides: Vec<String>,
}

impl Config {
    /// Load `rt.toml` (if any), overlay `RT__A__B` env vars, check unknown keys and values,
    /// and read secrets.
    ///
    /// Errors name the offending key path (`unknown key sched.procesors`, CFG-02).
    pub fn load(opts: &LoadOptions) -> Result<LoadedConfig, ConfigError> {
        let file = match &opts.file {
            Some(p) => Some(p.clone()),
            None => {
                let data = opts
                    .env
                    .iter()
                    .rev()
                    .find(|(k, _)| k == "RT_DATA")
                    .map(|(_, v)| v.as_str());
                let candidate = data
                    .filter(|d| !d.is_empty())
                    .map(|d| default_config_path(Path::new(d)));
                candidate.filter(|p| p.is_file())
            }
        };
        let text = match &file {
            Some(path) => match std::fs::read_to_string(path) {
                Ok(t) => t,
                Err(source) => {
                    return Err(ConfigError::Io {
                        path: path.clone(),
                        source,
                    })
                }
            },
            None => String::new(),
        };
        let (config, env_overrides) = loader::build(&text, &opts.env)?;
        let secrets = NodeSecrets::from_env(&opts.env, &config.security)?;
        Ok(LoadedConfig {
            config,
            secrets,
            file,
            env_overrides,
        })
    }

    /// Pure variant of [`Config::load`] for tests: `toml_text` + env pairs, no secrets, no I/O.
    pub fn from_toml(toml_text: &str, env: &[(String, String)]) -> Result<Config, ConfigError> {
        loader::build(toml_text, env).map(|(cfg, _)| cfg)
    }

    /// Cross-field validation (e.g. `kinds.*.min` sums, `memory.high < memory.critical`,
    /// listener syntax, plan references). Returns every problem found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let problems = validate::problems(self);
        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Validation(problems))
        }
    }

    /// Resolved node-scope knobs (explicit config > node default profile).
    pub fn node_knobs(&self) -> NodeKnobs {
        resolve_node(self)
    }

    /// Effective values of one service (CFG-04, CFG-06).
    pub fn service_effective(
        &self,
        service_profile: Option<Profile>,
        service: &rt_types::ServiceConfig,
        plan: &Plan,
    ) -> EffectiveService {
        resolve_service(self, service_profile, service, plan)
    }

    /// Number of processors: `sched.processors`, or `available` (cgroup quota / cores) when 0
    /// (CFG-07).
    pub fn resolve_processors(&self, available: u32) -> u32 {
        if self.sched.processors > 0 {
            self.sched.processors
        } else {
            available.max(1)
        }
    }

    /// Look up a plan by name (falling back to the built-in `free` plan values).
    pub fn plan(&self, name: &str) -> Plan {
        self.plans.get(name).cloned().unwrap_or_default()
    }

    /// `--print-config`: the fully resolved configuration as TOML (profile-controlled knobs
    /// filled in from the node default profile).
    pub fn print(&self) -> Result<String, ConfigError> {
        let k = resolve_node(self);
        let sk = profile::service_knobs(self.node.default_profile);
        let mut c = self.clone();
        c.sched.spin_us.get_or_insert(k.spin_us.value);
        c.sched.time_slice.get_or_insert(k.time_slice.value);
        c.sched.pin_threads.get_or_insert(k.pin_threads.value);
        c.sched.handoff_on_hog.get_or_insert(k.handoff_on_hog.value);
        c.sched.queue_timeout.get_or_insert(sk.queue_timeout);
        c.sched.warm_share.get_or_insert(k.warm_share.value);
        c.sysmon.max_tick.get_or_insert(k.sysmon_max_tick.value);
        c.rebalance.interval.get_or_insert(k.rebalance_interval.value);
        c.deno.max_bound_per_m.get_or_insert(k.deno_max_bound_per_m.value);
        c.deno.isolate_pool_per_p.get_or_insert(k.deno_isolate_pool_per_p.value);
        c.deno.reuse_isolates.get_or_insert(sk.deno_reuse_isolates);
        c.memory.high.get_or_insert(k.memory_high.value);
        c.memory.critical.get_or_insert(k.memory_critical.value);
        c.memory
            .purge_min_interval
            .get_or_insert(k.memory_purge_min_interval.value);
        c.cache.t1_share.get_or_insert(k.cache_t1_share.value);
        c.cache.idle_ttl.get_or_insert(sk.cache_idle_ttl);
        c.cache.preload.get_or_insert(sk.cache_preload);
        // the file/`Config` unit is MB; the resolved knob is bytes
        c.wasm
            .linear_memory_keep_resident
            .get_or_insert(k.wasm_linear_memory_keep_resident.value / resolve::MIB);
        c.wasm.reuse_instances.get_or_insert(sk.wasm_reuse_instances);
        c.wasm
            .max_unused_warm_slots
            .get_or_insert(k.wasm_max_unused_warm_slots.value);
        c.build.compile.get_or_insert(sk.build_compile);
        toml::to_string_pretty(&c).map_err(|e| ConfigError::Parse(e.to_string()))
    }

    /// Keys (dotted paths) applied to a running node (docs/config.md): re-read from the file on `SIGHUP`
    /// (`rt-server` `RunningServer::reload_config`, via [`Config::apply_live_keys`]), and, for the
    /// profile-controlled ones, re-derived when `PUT /v1/node/config` changes `default_profile`.
    /// They reach the scheduler (`Scheduler::reconfigure`, admission bounds), the memory governor
    /// (`Governor::set_watermarks`) and, for `sched.queue_timeout`, every task resolved after.
    pub const LIVE_KEYS: &'static [&'static str] = &[
        "sched.spin_us",
        "sched.time_slice",
        "sched.handoff_on_hog",
        "sched.max_inflight",
        "sched.max_queued",
        "sched.max_queued_mb",
        "sched.queue_timeout",
        "sysmon.max_tick",
        "rebalance.interval",
        "deno.max_bound_per_m",
        "memory.high",
        "memory.critical",
    ];

    /// Copy the [`Config::LIVE_KEYS`] values of `from` into `self` (everything else unchanged).
    pub fn apply_live_keys(&mut self, from: &Config) {
        self.sched.spin_us = from.sched.spin_us;
        self.sched.time_slice = from.sched.time_slice;
        self.sched.handoff_on_hog = from.sched.handoff_on_hog;
        self.sched.max_inflight = from.sched.max_inflight;
        self.sched.max_queued = from.sched.max_queued;
        self.sched.max_queued_mb = from.sched.max_queued_mb;
        self.sched.queue_timeout = from.sched.queue_timeout;
        self.sysmon.max_tick = from.sysmon.max_tick;
        self.rebalance.interval = from.rebalance.interval;
        self.deno.max_bound_per_m = from.deno.max_bound_per_m;
        self.memory.high = from.memory.high;
        self.memory.critical = from.memory.critical;
    }

    /// Config sections applied on `SIGHUP` (`rt-server` `RunningServer::reload_config`): the tenant
    /// plans (quotas, limits, egress stance, blocking shares). The same signal also re-reads the
    /// route and service tables from `meta.db`. Every other changed key is logged and applies
    /// after a restart; `[binding]`, `[egress]`, `[logs]` and `[metrics]` are not reloaded.
    pub const RELOAD_KEYS: &'static [&'static str] = &["plans"];
}

/// `$RT_DATA/rt.toml` if `data_dir` has one.
pub fn default_config_path(data_dir: &Path) -> PathBuf {
    data_dir.join("rt.toml")
}
