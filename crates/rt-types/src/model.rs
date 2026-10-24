//! Domain model shared by `rt-store`, `rt-api`, `rt-triggers`, `rt-sec` and the executors:
//! tenants, keys, scopes, services, versions, schedules, hosts, usage, events.
//!
//! These are plain serde data types; persistence lives in `rt-store`, HTTP shapes in `rt-api`.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

use crate::digest::Digest;
use crate::ids::{EventId, KeyId, ProjectId, ScheduleId, ServiceId, TenantId, VersionNo};
use crate::kinds::{Profile, RuntimeKind};
use crate::limits::CompileMode;
use crate::outcome::BuildFailure;
use crate::plan::TriggerKind;

/// Unix time in milliseconds.
pub type UnixMs = i64;

/// Entity tag of a service (`"<revision>"`), bumped on every write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Etag(pub u64);

impl Etag {
    /// Header form, e.g. `"7"` (with quotes).
    pub fn to_header(&self) -> String {
        format!("\"{}\"", self.0)
    }

    /// Parse an `ETag` / `If-Match` header value (`"7"`, `W/"7"` or bare `7`).
    pub fn parse_header(v: &str) -> Option<Etag> {
        let v = v.trim();
        let v = v.strip_prefix("W/").unwrap_or(v);
        v.trim_matches('"').parse().ok().map(Etag)
    }
}

// ---------------------------------------------------------------------------------------------
// Scopes
// ---------------------------------------------------------------------------------------------

/// API key scope (SECURITY.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Scope {
    #[serde(rename = "services:read")]
    ServicesRead,
    #[serde(rename = "services:write")]
    ServicesWrite,
    #[serde(rename = "deploy")]
    Deploy,
    #[serde(rename = "invoke")]
    Invoke,
    #[serde(rename = "logs:read")]
    LogsRead,
    #[serde(rename = "env:write")]
    EnvWrite,
    #[serde(rename = "secrets:write")]
    SecretsWrite,
    #[serde(rename = "queue:write")]
    QueueWrite,
    #[serde(rename = "data:admin")]
    DataAdmin,
    /// Create, list and revoke keys of the caller's own tenant (decision D-B4).
    #[serde(rename = "keys:write")]
    KeysWrite,
    /// Close a service's WebSockets through the control API.
    #[serde(rename = "sockets:write")]
    SocketsWrite,
}

impl Scope {
    pub const ALL: [Scope; 11] = [
        Scope::ServicesRead,
        Scope::ServicesWrite,
        Scope::Deploy,
        Scope::Invoke,
        Scope::LogsRead,
        Scope::EnvWrite,
        Scope::SecretsWrite,
        Scope::QueueWrite,
        Scope::DataAdmin,
        Scope::KeysWrite,
        Scope::SocketsWrite,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Scope::ServicesRead => "services:read",
            Scope::ServicesWrite => "services:write",
            Scope::Deploy => "deploy",
            Scope::Invoke => "invoke",
            Scope::LogsRead => "logs:read",
            Scope::EnvWrite => "env:write",
            Scope::SecretsWrite => "secrets:write",
            Scope::QueueWrite => "queue:write",
            Scope::DataAdmin => "data:admin",
            Scope::KeysWrite => "keys:write",
            Scope::SocketsWrite => "sockets:write",
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Scope {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Scope::ALL
            .iter()
            .copied()
            .find(|x| x.as_str() == s)
            .ok_or_else(|| format!("unknown scope `{s}`"))
    }
}

/// A set of scopes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Scopes(pub BTreeSet<Scope>);

impl Scopes {
    pub fn contains(&self, s: Scope) -> bool {
        self.0.contains(&s)
    }

    pub fn all() -> Self {
        Self(Scope::ALL.into_iter().collect())
    }

    pub fn iter(&self) -> impl Iterator<Item = Scope> + '_ {
        self.0.iter().copied()
    }
}

impl FromIterator<Scope> for Scopes {
    fn from_iter<I: IntoIterator<Item = Scope>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

// ---------------------------------------------------------------------------------------------
// Tenants and keys
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Tenant {
    pub id: TenantId,
    pub name: String,
    /// Plan name from config (`[plans.<name>]`).
    pub plan: String,
    /// Per-tenant overrides of plan fields (partial `Plan` JSON object).
    pub overrides: serde_json::Value,
    pub created_at_ms: UnixMs,
    pub disabled: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewTenant {
    pub name: String,
    pub plan: String,
    #[serde(default)]
    pub overrides: Option<serde_json::Value>,
}

/// The plaintext of a freshly issued API key (`rtk_<key_id>_<secret>`). Shown once.
#[derive(Clone)]
pub struct KeySecret(pub Zeroizing<String>);

impl KeySecret {
    pub fn new(s: String) -> Self {
        Self(Zeroizing::new(s))
    }
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for KeySecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KeySecret(<redacted>)")
    }
}

/// Stored API key (never contains the secret, only its keyed hash).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: KeyId,
    pub tenant_id: TenantId,
    pub name: Option<String>,
    pub scopes: Scopes,
    /// The projects the key may touch; `None` = every project of the tenant (then also projects
    /// created later). An explicit empty list admits nothing.
    pub project_ids: Option<Vec<ProjectId>>,
    /// The environments the key may touch, in each of its projects; `None` = every environment.
    /// An explicit empty list admits nothing.
    pub environments: Option<Vec<String>>,
    pub expires_at_ms: Option<UnixMs>,
    pub created_at_ms: UnixMs,
    pub last_used_at_ms: Option<UnixMs>,
    pub revoked: bool,
    /// The key that minted this one through `POST /v1/keys`; revoking it revokes this key too.
    /// `None` for keys the operator created.
    #[serde(default)]
    pub parent_key_id: Option<KeyId>,
    /// HMAC-SHA256(pepper, secret). Never serialized to API clients.
    #[serde(skip)]
    pub hmac: Vec<u8>,
}

/// Input to `MetaStore::create_key`: `rt-sec` generates the ID, secret and HMAC; the store persists.
#[derive(Clone, Debug, PartialEq)]
pub struct NewKey {
    pub id: KeyId,
    pub name: Option<String>,
    pub scopes: Scopes,
    /// See [`ApiKey::project_ids`].
    pub project_ids: Option<Vec<ProjectId>>,
    /// See [`ApiKey::environments`].
    pub environments: Option<Vec<String>>,
    pub expires_at_ms: Option<UnixMs>,
    pub hmac: Vec<u8>,
}

/// The rule behind every key scope: `project_ids` and `environments` (`None` = all) admit a
/// service of `project` in `environment`.
pub fn scope_admits(
    project_ids: Option<&[ProjectId]>,
    environments: Option<&[String]>,
    project: ProjectId,
    environment: &str,
) -> bool {
    project_ids.is_none_or(|l| l.contains(&project)) && environments.is_none_or(|l| l.iter().any(|e| e == environment))
}

// ---------------------------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------------------------

/// Who may invoke a service over HTTP (SECURITY.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    #[default]
    Public,
    Key,
    Internal,
}

/// Explicit per-service limits (unset = profile/plan/default).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LimitsConfig {
    pub wall_ms: Option<u64>,
    pub cpu_ms: Option<u64>,
    pub memory_mb: Option<u32>,
    pub max_body_bytes: Option<u64>,
}

/// Per-service scaling block (decision D-B2). An *instance* is one concurrently running copy of
/// the service on the node (a Deno isolate or a WASM instance).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ScalingConfig {
    /// Instances kept warm: never scaled to zero, and the autoscaler's limit never falls below it.
    /// Unset or 0 = scale to zero. Above the plan's `max_instances`: clamped (`plan_conflict`).
    pub min_instances: Option<u32>,
    /// Hard cap on concurrently running instances; excess requests queue (D-A3). Unset or 0 = the
    /// plan's limit (else the node default). Above the plan's `max_instances`: clamped. The name
    /// `max_concurrency` of earlier versions is accepted as an alias on input.
    #[serde(alias = "max_concurrency")]
    pub max_instances: Option<u32>,
    pub max_queued: Option<u32>,
    pub keep_resident: Option<bool>,
}

impl ScalingConfig {
    /// A bound that contradicts another: `min_instances` above `max_instances`. Returned text is
    /// the validation message (`422 invalid_request` in the API).
    pub fn validate(&self) -> Result<(), String> {
        match (self.min_instances, self.max_instances) {
            (Some(min), Some(max)) if max > 0 && min > max => Err(format!(
                "`scaling.min_instances` ({min}) is greater than `scaling.max_instances` ({max})"
            )),
            _ => Ok(()),
        }
    }
}

/// Per-service trigger switches (all on by default).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TriggerSwitches {
    pub http: bool,
    pub interval: bool,
    pub cron: bool,
    pub queue: bool,
    pub call: bool,
    /// WebSocket upgrades (`std:sockets.accept`) and their events.
    pub websocket: bool,
}

impl Default for TriggerSwitches {
    fn default() -> Self {
        Self {
            http: true,
            interval: true,
            cron: true,
            queue: true,
            call: true,
            websocket: true,
        }
    }
}

impl TriggerSwitches {
    pub fn enabled(&self, t: TriggerKind) -> bool {
        match t {
            TriggerKind::Http => self.http,
            TriggerKind::Interval => self.interval,
            TriggerKind::Cron => self.cron,
            TriggerKind::Queue => self.queue,
            TriggerKind::Call => self.call,
            TriggerKind::Websocket => self.websocket,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RateLimit {
    pub rps: u32,
    pub burst: u32,
}

/// Ingress rules evaluated before admission (SECURITY.md).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngressRules {
    pub allowed_methods: Vec<String>,
    pub allow_paths: Vec<String>,
    /// IPv4 and IPv6 CIDRs.
    pub ip_allow: Vec<String>,
    pub rate_limit: RateLimit,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EgressRules {
    pub allowed_hosts: Vec<String>,
    pub denied_hosts: Vec<String>,
}

/// The environment label a service gets when none is given (D-A1).
pub const DEFAULT_ENVIRONMENT: &str = "production";

/// Longest environment label.
pub const MAX_ENVIRONMENT_LEN: usize = 32;

/// Most entries `call.allowed_callers` may hold.
pub const MAX_ALLOWED_CALLERS: usize = 64;

/// `production`: the environment of services created without one (and of every service that
/// existed before environments, so their call behavior does not change).
pub fn default_environment() -> String {
    DEFAULT_ENVIRONMENT.to_string()
}

/// An environment label is a short slug: `^[a-z][a-z0-9-]{0,31}$` (`production`, `staging`,
/// `pr-42`).
pub fn valid_environment(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= MAX_ENVIRONMENT_LEN
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// A service slug: `^[a-z][a-z0-9-]{1,62}$` (API-06).
pub fn valid_service_slug(s: &str) -> bool {
    let b = s.as_bytes();
    (2..=63).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// Who may call (and enqueue to) a service from another service (D-A1).
///
/// Each `allowed_callers` entry is a caller service slug, a service id (`svc_…`, stored in its
/// canonical form) or `env:<name>`. For callers of the **same tenant**, an empty list admits
/// callers in the same environment as the target; a non-empty list admits only the callers it
/// names. A caller of **another tenant** (only possible when its plan allows cross-tenant calls)
/// must be listed by service id: slug and `env:` entries, and the empty list, never admit other
/// tenants. A service may always call itself.
///
/// A slug entry admits whichever service holds that slug when the call is made (a deleted
/// service's slug can be taken by another service of the tenant); prefer ids where that matters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CallAccess {
    pub allowed_callers: Vec<String>,
}

/// The calling service, as the call dispatcher resolves it (route table + task identity).
/// `slug`, `project` and `environment` are `None` when the caller is not routable (then only id
/// entries can match).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Caller<'a> {
    pub tenant: TenantId,
    pub service: ServiceId,
    pub slug: Option<&'a str>,
    pub project: Option<ProjectId>,
    pub environment: Option<&'a str>,
}

/// The service being called.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallTarget<'a> {
    pub tenant: TenantId,
    pub service: ServiceId,
    pub project: ProjectId,
    pub environment: &'a str,
}

impl CallAccess {
    /// Check every entry (`env:<valid environment>`, a `svc_…` id, or a service slug) without
    /// changing it. See [`CallAccess::canonicalize`] for what is stored.
    pub fn validate(&self) -> Result<(), String> {
        self.clone().canonicalize()
    }

    /// Check every entry and rewrite `svc_…` ids to their canonical form (a lowercase ULID would
    /// otherwise validate but never match).
    pub fn canonicalize(&mut self) -> Result<(), String> {
        if self.allowed_callers.len() > MAX_ALLOWED_CALLERS {
            return Err(format!(
                "`call.allowed_callers` holds at most {MAX_ALLOWED_CALLERS} entries"
            ));
        }
        for e in &mut self.allowed_callers {
            let canonical = match e.strip_prefix("env:") {
                Some(env) => valid_environment(env).then(|| e.clone()),
                None if e.starts_with("svc_") || e.starts_with("SVC_") => format!("svc_{}", &e[4..])
                    .parse::<ServiceId>()
                    .ok()
                    .map(|id| id.to_string()),
                None => valid_service_slug(e).then(|| e.clone()),
            };
            match canonical {
                Some(c) => *e = c,
                None => {
                    return Err(format!(
                        "`call.allowed_callers` entry `{e}` is not a service slug, a svc_… id or env:<environment>"
                    ))
                }
            }
        }
        Ok(())
    }

    /// The D-A1 rule. `target` admits `caller` when it is the target itself, or:
    /// - same tenant: the caller is listed by slug or id, or `env:<name>` is listed and the caller
    ///   runs in that environment, or the list is empty and both run in the same environment;
    /// - another tenant: the caller is listed by service id (nothing else admits it).
    ///
    /// An environment is a label of one project: both sides must be in the same project for an
    /// environment to match (`env:production` never reaches the production of another project).
    pub fn admits(&self, target: &CallTarget<'_>, caller: &Caller<'_>) -> bool {
        if caller.service == target.service {
            return true;
        }
        let listed_by_id = |id: &str| self.allowed_callers.iter().any(|e| e.eq_ignore_ascii_case(id));
        if caller.tenant != target.tenant {
            return listed_by_id(&caller.service.to_string());
        }
        let same_environment = caller.project == Some(target.project) && caller.environment == Some(target.environment);
        if self.allowed_callers.is_empty() {
            return same_environment;
        }
        let id = caller.service.to_string();
        self.allowed_callers.iter().any(|e| match e.strip_prefix("env:") {
            Some(env) => caller.project == Some(target.project) && caller.environment == Some(env),
            None => e.eq_ignore_ascii_case(&id) || caller.slug.is_some_and(|s| s.eq_ignore_ascii_case(e)),
        })
    }
}

/// All service config blocks (`PATCH` replaces one top-level block at a time).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServiceConfig {
    pub limits: LimitsConfig,
    pub scaling: ScalingConfig,
    pub access: Access,
    pub triggers: TriggerSwitches,
    pub ingress: IngressRules,
    pub egress: EgressRules,
    /// Service-to-service caller rules (D-A1).
    pub call: CallAccess,
    /// WASM knobs (`runtime: wasm` only; ignored for Deno). Omitted from JSON while unset.
    #[serde(skip_serializing_if = "WasmServiceConfig::is_unset")]
    pub wasm: WasmServiceConfig,
    /// Deno knobs (`runtime: deno` only; ignored for WASM). Omitted from JSON while unset.
    #[serde(skip_serializing_if = "DenoServiceConfig::is_unset")]
    pub deno: DenoServiceConfig,
    /// A durable service: its default export is a class whose objects are chosen in code
    /// (`durable.get(name)`), each with its own storage and alarm, its events and requests run one
    /// at a time per object. Omitted from JSON while false.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub durable: bool,
    /// WebSocket knobs. Omitted from JSON while unset.
    #[serde(skip_serializing_if = "WsServiceConfig::is_unset")]
    pub ws: WsServiceConfig,
}

/// Per-service WebSocket knobs (`config.ws`, docs/handlers.md "WebSockets").
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WsServiceConfig {
    /// `Origin` values an upgrade may come with (exact, e.g. `https://app.example`); others get
    /// `403` before any task runs. Empty = any origin (the guest still sees `Origin`). Set it for
    /// cookie-authenticated apps (cross-site WebSocket hijacking).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allowed_origins: Vec<String>,
    /// Warm objects (decision 9, default on): the isolate that ran an event is kept for the next
    /// event of the service, so an object's instance (constructor state) survives between events
    /// while it stays warm. `false` = a fresh isolate per event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm: Option<bool>,
    /// The service's own cap on open sockets (within the plan's `max_sockets`); unset = none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_sockets: Option<u32>,
}

impl WsServiceConfig {
    /// No knob set (the block is left out of the service's JSON).
    pub fn is_unset(&self) -> bool {
        *self == Self::default()
    }

    /// Warm objects are on unless switched off.
    pub fn warm(&self) -> bool {
        self.warm.unwrap_or(true)
    }

    /// `true` when an upgrade with this `Origin` header (or none) may proceed.
    pub fn origin_allowed(&self, origin: Option<&str>) -> bool {
        self.allowed_origins.is_empty()
            || origin.is_some_and(|o| {
                self.allowed_origins
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(o.trim_end_matches('/')))
            })
    }
}

/// Per-service Deno knobs (`config.deno`). An unset value comes from the node config, then the
/// service's profile (docs/config.md).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DenoServiceConfig {
    /// Reuse an isolate with its evaluated module for the next task of the same service and
    /// version (`deno.reuse_isolates`, decision D-A4; profile default: on only in `min_latency`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reuse_isolates: Option<bool>,
}

impl DenoServiceConfig {
    /// No knob set (the block is left out of the service's JSON).
    pub fn is_unset(&self) -> bool {
        *self == Self::default()
    }
}

/// Per-service WASM knobs (`config.wasm`). An unset value comes from the service's profile
/// (docs/config.md).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WasmServiceConfig {
    /// Keep a cleanly finished instance for the next task of this service on the same P
    /// (`wasm.reuse_instances`; profile default: on only in `min_latency`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reuse_instances: Option<bool>,
}

impl WasmServiceConfig {
    /// No knob set (the block is left out of the service's JSON).
    pub fn is_unset(&self) -> bool {
        *self == Self::default()
    }
}

/// A service (the `GET /v1/services/{s}` resource minus `status`/`effective`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Service {
    pub id: ServiceId,
    pub tenant_id: TenantId,
    pub slug: String,
    pub runtime: RuntimeKind,
    pub name: Option<String>,
    /// `None` = node default profile.
    pub profile: Option<Profile>,
    /// Environment label (D-A1, default `production`): same-environment services may call each
    /// other unless `config.call.allowed_callers` says otherwise.
    #[serde(default = "default_environment")]
    pub environment: String,
    /// The project the service belongs to (D-B3); `environment` is one of its environments.
    pub project_id: ProjectId,
    /// Slug of [`Service::project_id`], for display.
    #[serde(default = "crate::project::default_project")]
    pub project: String,
    pub config: ServiceConfig,
    /// Version the service serves (derived from `versions.is_live`).
    pub live_version: Option<VersionNo>,
    pub etag: Etag,
    pub created_at_ms: UnixMs,
    pub updated_at_ms: UnixMs,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewService {
    pub slug: String,
    pub runtime: RuntimeKind,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub profile: Option<Profile>,
    /// Environment label ([`DEFAULT_ENVIRONMENT`] when not given).
    #[serde(default = "default_environment")]
    pub environment: String,
    /// Slug of the project to create the service in ([`crate::DEFAULT_PROJECT`] when not given).
    #[serde(default = "crate::project::default_project")]
    pub project: String,
    #[serde(default)]
    pub config: ServiceConfig,
}

/// Partial update: each present block replaces that block.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServicePatch {
    pub name: Option<String>,
    pub profile: Option<Profile>,
    pub environment: Option<String>,
    pub limits: Option<LimitsConfig>,
    pub scaling: Option<ScalingConfig>,
    pub access: Option<Access>,
    pub triggers: Option<TriggerSwitches>,
    pub ingress: Option<IngressRules>,
    pub egress: Option<EgressRules>,
    pub call: Option<CallAccess>,
    pub wasm: Option<WasmServiceConfig>,
    pub deno: Option<DenoServiceConfig>,
    pub durable: Option<bool>,
    pub ws: Option<WsServiceConfig>,
}

/// Build state of a version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BuildState {
    Pending,
    Building,
    Ready,
    Failed,
}

/// Immutable uploaded version of a service.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Version {
    pub service_id: ServiceId,
    pub number: VersionNo,
    pub source_digest: Digest,
    pub lock_digest: Option<Digest>,
    pub build_state: BuildState,
    /// e.g. `engine_changed` while `Pending`.
    pub pending_reason: Option<String>,
    pub failure: Option<BuildFailure>,
    pub created_at_ms: UnixMs,
    /// Digest of the bundle the last successful build produced (Deno; audit). `None` before a
    /// build reports it and for WASM.
    #[serde(default)]
    pub bundle_digest: Option<Digest>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewVersion {
    pub source_digest: Digest,
    pub compile: CompileMode,
}

/// Schedule definition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ScheduleKind {
    Interval {
        every_seconds: u64,
    },
    /// 5-field cron (optional leading seconds field) + IANA timezone (default `UTC`).
    /// Wire form (contracts C-1): `{"kind":"cron","cron":"*/5 * * * *","timezone":"UTC"}`.
    Cron {
        #[serde(rename = "cron")]
        expr: String,
        #[serde(rename = "timezone", default = "default_tz")]
        tz: String,
    },
}

fn default_tz() -> String {
    "UTC".to_string()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Schedule {
    pub id: ScheduleId,
    pub service_id: ServiceId,
    #[serde(flatten)]
    pub kind: ScheduleKind,
    /// JSON payload ≤ 64 KB delivered as `event.payload`.
    pub payload: serde_json::Value,
    pub enabled: bool,
    pub next_due_ms: Option<UnixMs>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewSchedule {
    #[serde(flatten)]
    pub kind: ScheduleKind,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default = "yes")]
    pub enabled: bool,
}

fn yes() -> bool {
    true
}

/// Custom host mapped to a service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostMapping {
    pub host: String,
    pub service_id: ServiceId,
    pub status: HostStatus,
    /// Value the tenant must publish at `_rt-verify.<host>` (TXT) when verification is on.
    pub verify_token: Option<String>,
    /// When the claim was made (`0` on input = now). A pending claim older than
    /// `hosts.pending_ttl` expires: it is deleted and no longer blocks other services.
    #[serde(default)]
    pub created_at_ms: UnixMs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostStatus {
    PendingVerification,
    Active,
}

// ---------------------------------------------------------------------------------------------
// Routes (immutable snapshot pushed to triggers/ingress)
// ---------------------------------------------------------------------------------------------

/// One routable service (only services with a live version appear).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RouteEntry {
    pub tenant: TenantId,
    pub service: ServiceId,
    pub slug: String,
    pub runtime: RuntimeKind,
    pub version: VersionNo,
    pub digest: Digest,
    pub access: Access,
    pub triggers: TriggerSwitches,
    pub ingress: IngressRules,
    pub profile: Option<Profile>,
    /// Verified custom hosts mapping to this service.
    pub hosts: Vec<String>,
    /// Project of the service (its environment labels are project-local).
    pub project: ProjectId,
    /// Environment label (D-A1).
    #[serde(default = "default_environment")]
    pub environment: String,
    /// Who may call this service (D-A1).
    #[serde(default)]
    pub call: CallAccess,
}

/// Complete set of routes at one generation. Swapped atomically.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RouteSnapshot {
    pub generation: u64,
    pub entries: Vec<RouteEntry>,
    /// Operator SDKs, their releases, channels and grants.
    #[serde(default)]
    pub sdk: crate::sdk::SdkSnapshot,
}

// ---------------------------------------------------------------------------------------------
// Usage and events
// ---------------------------------------------------------------------------------------------

/// Counters added to `usage` rows (batched, fire-and-forget).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageDelta {
    pub invocations: u64,
    pub cpu_ms: u64,
    pub egress_bytes: u64,
    pub storage_bytes: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageBatch {
    pub items: Vec<(TenantId, ServiceId, UsageDelta)>,
}

/// A node event (`events` table, audit, `GET …/events`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeEvent {
    pub id: EventId,
    pub ts_ms: UnixMs,
    pub tenant: Option<TenantId>,
    pub service: Option<ServiceId>,
    /// `deploy`, `rollback.auto`, `key.created`, `plan_conflict`, `security.artifact_tamper`, …
    pub kind: String,
    pub data: serde_json::Value,
}

impl NodeEvent {
    pub fn new(kind: impl Into<String>, ts_ms: UnixMs) -> Self {
        Self {
            id: EventId::new(),
            ts_ms,
            tenant: None,
            service: None,
            kind: kind.into(),
            data: serde_json::Value::Null,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Env bundle (decrypted env + secrets for one task)
// ---------------------------------------------------------------------------------------------

/// Plain env and decrypted secrets of a service. Wrap in `Zeroizing` when held.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct EnvBundle {
    pub env: Vec<(String, String)>,
    pub secrets: Vec<(String, String)>,
}

impl Zeroize for EnvBundle {
    fn zeroize(&mut self) {
        for (k, v) in self.env.iter_mut().chain(self.secrets.iter_mut()) {
            k.zeroize();
            v.zeroize();
        }
        self.env.clear();
        self.secrets.clear();
    }
}

impl fmt::Debug for EnvBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print values.
        f.debug_struct("EnvBundle")
            .field(
                "env_names",
                &self.env.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            )
            .field(
                "secret_names",
                &self.secrets.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            )
            .finish()
    }
}
