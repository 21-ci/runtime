//! Request/response bodies of the control API (docs/api.md). They carry `ToSchema` so the OpenAPI
//! document is generated from the same types the handlers use. IDs are strings on the wire
//! (`svc_…`, `ten_…`, …); nested config blocks are JSON objects.

use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

// ---- pagination ------------------------------------------------------------------------------

/// `?limit=50&cursor=…`
#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PageQuery {
    /// Page size (default 50, max 200).
    pub limit: Option<u32>,
    /// Opaque cursor from a previous `next_cursor`.
    pub cursor: Option<String>,
}

/// `GET /v1/services?limit&cursor&slug&project&environment` — `slug` filters to 0/1 items
/// (contracts C-9); `project` and `environment` narrow the list (D-B3).
#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ServicesQuery {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    /// Exact slug match.
    pub slug: Option<String>,
    /// Only services of this project (slug or `prj_…` id).
    pub project: Option<String>,
    /// Only services in this environment.
    pub environment: Option<String>,
}

// ---- node ------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct NodeStatus {
    pub version: String,
    /// `normal` | `shed` | `drain`.
    pub mode: String,
    pub default_profile: String,
    /// `false` with the prebuilt-V8 MVP builds (SEC-36).
    pub v8_sandbox: bool,
    pub processors: u32,
    /// Processors per kind: `{"deno": 3, "wasm": 1, "build": 1}`.
    #[schema(value_type = Object)]
    pub kinds: serde_json::Value,
    #[schema(value_type = Object)]
    pub memory: serde_json::Value,
    pub inflight: u32,
    /// WebSockets the node holds.
    pub sockets: NodeSockets,
}

/// `status.sockets`: open sockets, objects holding them, accepted sockets not connected yet, and
/// the node's cap (`ws.max_sockets`).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct NodeSockets {
    pub open: u32,
    pub objects: u32,
    pub pending: u32,
    pub max: u32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct NodeConfigApplied {
    /// Keys applied live.
    pub applied: Vec<String>,
    /// Keys that need a restart (CFG-05).
    pub pending_restart: Vec<String>,
}

/// `GET /v1/version`: what any authenticated caller may know about the node.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct VersionInfo {
    /// Node (crate) version.
    pub version: String,
    /// Version of the control API document (`info.version` of `/v1/openapi.json`).
    pub api: String,
}

/// One plan from the node config (`GET /v1/plans`), or a tenant's effective plan (`GET /v1/me`).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PlanView {
    pub name: String,
    /// The plan's settings (`max_services`, `rps`, `limits_max`, `custom_host_suffixes`, …); in
    /// `/v1/me` with the tenant's overrides applied.
    #[schema(value_type = Object)]
    pub settings: serde_json::Value,
}

/// `GET /v1/me`: who the credentials are and what they may do.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeView {
    /// `operator` | `tenant`.
    pub principal: String,
    /// The tenant acted on: the key's tenant, or the operator's `x-rt-tenant`.
    pub tenant_id: Option<String>,
    /// The key (`key_…`) of a tenant principal.
    pub key_id: Option<String>,
    /// Scopes held (the operator holds all).
    pub scopes: Vec<String>,
    /// Projects the key is limited to (`prj_…` ids; `null` = every project of the tenant).
    pub project_ids: Option<Vec<String>>,
    /// Environments the key is limited to (`null` = every environment).
    pub environments: Option<Vec<String>>,
    /// The tenant's effective plan; `null` for the operator without `x-rt-tenant`.
    pub plan: Option<PlanView>,
    /// Node version.
    pub version: String,
}

// ---- tenants and keys ------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateTenantBody {
    pub name: String,
    /// Plan name from config.
    pub plan: String,
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub overrides: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PatchTenantBody {
    pub name: Option<String>,
    pub plan: Option<String>,
    /// Replaces the tenant's plan overrides.
    #[schema(value_type = Option<Object>)]
    pub overrides: Option<serde_json::Value>,
    pub disabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TenantView {
    pub id: String,
    pub name: String,
    pub plan: String,
    #[schema(value_type = Object)]
    pub overrides: serde_json::Value,
    pub created_at: i64,
    pub disabled: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TenantPage {
    pub items: Vec<TenantView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PurgeQuery {
    /// `1|true|yes|on` (case-insensitive) deletes the tenant's data too.
    pub purge: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UsageQuery {
    /// `YYYY-MM-DD`
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UsageView {
    pub service_id: String,
    pub day: String,
    pub invocations: u64,
    pub cpu_ms: u64,
    pub egress_bytes: u64,
    pub storage_bytes: i64,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateKeyBody {
    /// Scopes such as `services:read`, `deploy`, `invoke`.
    pub scopes: Vec<String>,
    /// Projects the key may touch (`prj_…` ids of the tenant; `null` = all projects).
    pub project_ids: Option<Vec<String>>,
    /// Environments the key may touch (`null` = all environments).
    pub environments: Option<Vec<String>>,
    /// RFC 3339 timestamp.
    pub expires_at: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct KeyView {
    pub id: String,
    pub name: Option<String>,
    pub scopes: Vec<String>,
    /// `null` = every project of the tenant.
    pub project_ids: Option<Vec<String>>,
    /// `null` = every environment.
    pub environments: Option<Vec<String>>,
    pub expires_at: Option<i64>,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

/// Returned once at creation; `GET …/keys` never shows the secret again (API-13).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct KeyCreated {
    #[serde(flatten)]
    pub key: KeyView,
    /// The full `rtk_<id>_<secret>` string.
    pub secret: String,
}

// ---- services --------------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateServiceBody {
    /// `^[a-z][a-z0-9-]{1,62}$`
    pub slug: String,
    /// `deno` | `wasm`.
    pub runtime: String,
    pub name: Option<String>,
    pub profile: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub limits: Option<serde_json::Value>,
    #[schema(value_type = Option<Object>)]
    pub scaling: Option<serde_json::Value>,
    /// `public` | `key` | `internal`.
    pub access: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub triggers: Option<serde_json::Value>,
    #[schema(value_type = Option<Object>)]
    pub ingress: Option<serde_json::Value>,
    #[schema(value_type = Option<Object>)]
    pub egress: Option<serde_json::Value>,
    /// Project to create the service in: its slug or `prj_…` id (default `default`).
    pub project: Option<String>,
    /// Environment label, `^[a-z][a-z0-9-]{0,31}$` (default `production`); one of the project's
    /// environments.
    pub environment: Option<String>,
    /// `{"allowed_callers": ["<slug>" | "svc_…" | "env:<name>", …]}`: who may call this service
    /// (empty = same-environment services of the tenant).
    #[schema(value_type = Option<Object>)]
    pub call: Option<serde_json::Value>,
    /// WASM knobs: `{"reuse_instances": bool}` (unset = the profile's value).
    #[schema(value_type = Option<Object>)]
    pub wasm: Option<serde_json::Value>,
    /// Deno knobs: `{"reuse_isolates": bool}` (unset = node config, then the profile's value).
    #[schema(value_type = Option<Object>)]
    pub deno: Option<serde_json::Value>,
    /// A durable service: its objects are chosen in code (`durable.get(name)`), each with its own
    /// storage and alarm, events and requests one at a time per object (default false).
    pub durable: Option<bool>,
    /// WebSocket knobs: `{"allowed_origins": [..], "warm": bool}`.
    #[schema(value_type = Option<Object>)]
    pub ws: Option<serde_json::Value>,
    /// Pub/sub subscriptions and stream consumers: `{"subscribe": [{"subject", "group"?}],
    /// "consume": [{"topic", "group", "start", "batch_size", "max_attempts", "on_exhausted"}]}`.
    /// Needs the plan to allow the `pubsub` / `stream` triggers; refused for durable services.
    #[schema(value_type = Option<Object>)]
    pub events: Option<serde_json::Value>,
    /// Async invocation: `{"enabled": bool, "max_attempts": 1..=10, "max_event_age_s": 60..=86400}`.
    #[serde(rename = "async")]
    #[schema(value_type = Option<Object>)]
    pub async_invoke: Option<serde_json::Value>,
}

/// Top-level block merge: each provided block replaces that block.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PatchServiceBody {
    pub name: Option<String>,
    pub profile: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub limits: Option<serde_json::Value>,
    #[schema(value_type = Option<Object>)]
    pub scaling: Option<serde_json::Value>,
    pub access: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub triggers: Option<serde_json::Value>,
    #[schema(value_type = Option<Object>)]
    pub ingress: Option<serde_json::Value>,
    #[schema(value_type = Option<Object>)]
    pub egress: Option<serde_json::Value>,
    /// Environment label, `^[a-z][a-z0-9-]{0,31}$`; one of the service's project's environments.
    pub environment: Option<String>,
    /// The service's current project (slug or `prj_…` id). A service cannot move between
    /// projects: any other project is `422`.
    pub project: Option<String>,
    /// Replaces the `call` block (`allowed_callers`).
    #[schema(value_type = Option<Object>)]
    pub call: Option<serde_json::Value>,
    /// Replaces the `wasm` block (`reuse_instances`).
    #[schema(value_type = Option<Object>)]
    pub wasm: Option<serde_json::Value>,
    /// Replaces the `deno` block (`reuse_isolates`).
    #[schema(value_type = Option<Object>)]
    pub deno: Option<serde_json::Value>,
    /// Makes the service durable (or not).
    pub durable: Option<bool>,
    /// Replaces the `ws` block (`allowed_origins`, `warm`).
    #[schema(value_type = Option<Object>)]
    pub ws: Option<serde_json::Value>,
    /// Pub/sub subscriptions and stream consumers: `{"subscribe": [{"subject", "group"?}],
    /// "consume": [{"topic", "group", "start", "batch_size", "max_attempts", "on_exhausted"}]}`.
    /// Needs the plan to allow the `pubsub` / `stream` triggers; refused for durable services.
    #[schema(value_type = Option<Object>)]
    pub events: Option<serde_json::Value>,
    /// Async invocation: `{"enabled": bool, "max_attempts": 1..=10, "max_event_age_s": 60..=86400}`.
    #[serde(rename = "async")]
    #[schema(value_type = Option<Object>)]
    pub async_invoke: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ServiceStatus {
    /// `pending` | `building` | `ready` | `failed`.
    pub build_state: String,
    /// `T1` | `T2` | `T3`.
    pub tier: String,
    /// Tasks of the service holding a run slot now.
    pub inflight: u32,
    pub last_error: Option<String>,
    /// Queueing and autoscaler state; `null` while the service has none (never run, or idle
    /// long enough to be forgotten).
    pub admission: Option<AdmissionView>,
}

/// A service's admission and autoscaler state (decision D-A3): the adaptive concurrency limit,
/// what runs and waits, and what the autoscaler has learned.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdmissionView {
    /// Kind of the service's latest task: `deno` | `wasm` | `build`.
    pub kind: String,
    /// Current adaptive concurrency limit.
    pub limit: u32,
    /// Bounds of the limit (`scaling.min` .. the ceiling and the plan's tenant cap).
    pub min: u32,
    pub max: u32,
    /// Tasks holding a run slot.
    pub running: u32,
    /// Tasks waiting for a run slot, and the bytes charged to them.
    pub queued: u32,
    pub queued_bytes: u64,
    /// Bound of the service's wait queue (over it `503 overloaded`).
    pub max_queued: u32,
    /// Smoothed wait (ms) of the requests that had to queue; fades to 0 while requests start
    /// without waiting.
    pub queue_wait_ms: f64,
    /// `503 queue_timeout` answers since the state was created.
    pub queue_timeouts: u64,
    /// Decaying peak of running tasks (warm capacity follows it).
    pub demand: f64,
    /// Latency baseline (ms) the autoscaler holds the limit against; `null` before samples.
    pub latency_baseline_ms: Option<f64>,
    /// Warm instances the autoscaler asks each processor of the kind to keep (0 = scale to zero).
    pub warm_per_proc: u32,
    /// Instances running now: concurrently running copies of the service (the same as `running`).
    pub instances: u32,
    /// Instances kept warm and floor of the limit (`scaling.min_instances`, within the plan's
    /// `max_instances`; 0 = scales to zero when idle).
    pub min_instances: u32,
    /// Most instances at once (`scaling.max_instances`, the plan's `max_instances`, the profile
    /// ceiling and the tenant cap): the same as `max`; excess requests queue.
    pub max_instances: u32,
    /// Warm instances the autoscaler wants kept ready in total (`warm_per_proc` times the
    /// processors of the kind; at least `min_instances` once the service has run).
    pub target_instances: u32,
    /// Smoothed arrival rate, requests per second.
    pub rps: f64,
    /// The latest move of the limit and why; `null` if it never moved.
    pub last_change: Option<LimitChangeView>,
    /// Milliseconds since the service last started or finished a task.
    pub idle_ms: u64,
    /// Predicted slot time (ms) of an interactive task (HTTP, calls, pub/sub); `null` before samples.
    pub pred_ms: Option<f64>,
    /// Predicted slot time (ms) of a background task (queue, stream, schedules); `null` before samples.
    pub pred_background_ms: Option<f64>,
}

/// One move of a service's concurrency limit (`admission.last_change`).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LimitChangeView {
    /// What moved it: `queue` (requests waited), `trend` (arrival rate rose fast), `latency`,
    /// `memory`, `cpu`, `idle` (unused limit decays), `bounds` (the service or its plan changed).
    pub reason: String,
    pub from: u32,
    pub to: u32,
    /// Milliseconds since the move.
    pub ago_ms: u64,
}

/// `GET /v1/services/{s}`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ServiceView {
    pub id: String,
    pub tenant_id: String,
    pub slug: String,
    pub runtime: String,
    pub name: Option<String>,
    pub profile: Option<String>,
    pub live_version: Option<u32>,
    /// Also sent as the `ETag` header.
    pub etag: String,
    #[schema(value_type = Object)]
    pub limits: serde_json::Value,
    #[schema(value_type = Object)]
    pub scaling: serde_json::Value,
    pub access: String,
    #[schema(value_type = Object)]
    pub triggers: serde_json::Value,
    #[schema(value_type = Object)]
    pub ingress: serde_json::Value,
    #[schema(value_type = Object)]
    pub egress: serde_json::Value,
    /// Id of the project the service belongs to (`prj_…`).
    pub project_id: String,
    /// Slug of that project (default `default`).
    pub project: String,
    /// Environment label (default `production`), one of the project's environments.
    pub environment: String,
    /// `{"allowed_callers": […]}`.
    #[schema(value_type = Object)]
    pub call: serde_json::Value,
    /// `{"reuse_instances": bool}`; omitted while no WASM knob is set.
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    #[schema(value_type = Object)]
    pub wasm: serde_json::Value,
    /// `{"reuse_isolates": bool}`; omitted while no Deno knob is set.
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    #[schema(value_type = Object)]
    pub deno: serde_json::Value,
    /// A durable service (its objects are chosen in code).
    pub durable: bool,
    /// `{"allowed_origins": […], "warm": bool, "max_sockets": n}`; omitted while no WebSocket knob is set.
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    #[schema(value_type = Object)]
    pub ws: serde_json::Value,
    /// The `events` block; omitted while it has no subscription or consumer.
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    #[schema(value_type = Object)]
    pub events: serde_json::Value,
    /// The `async` block; omitted while no knob is set.
    #[serde(rename = "async", skip_serializing_if = "serde_json::Value::is_null")]
    #[schema(value_type = Object)]
    pub async_invoke: serde_json::Value,
    pub status: ServiceStatus,
    /// Each value with its source: `{"cpu_ms": {"value": 1000, "source": "profile:balanced"}}`.
    #[schema(value_type = Object)]
    pub effective: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ServicePage {
    pub items: Vec<ServiceView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct DeleteServiceQuery {
    /// `1|true|yes|on` keeps the service's data.
    pub keep_data: Option<String>,
}

// ---- versions --------------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UploadQuery {
    /// Activate (flip `live`) once accepted (`1|true|yes|on`).
    pub activate: Option<String>,
    /// With `activate`: close the service's WebSockets with 1012 once the version is live
    /// (`1|true|yes|on`; default: sockets stay open and the next event runs the new version).
    pub close_sockets: Option<String>,
    /// `lazy` | `eager`.
    pub compile: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TypesQuery {
    /// A version number (default: the live version).
    pub version: Option<u32>,
}

/// Upload response (contracts C-10): `201 {"version":N,"source_digest":"sha256:…","build_state":"pending"}`;
/// eager compile → `202` with the same shape and `build_state:"building"`. Also the shape of
/// `GET …/versions/{v}` (plus `lock_digest`, `failure`, `created_at`).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct VersionView {
    pub version: u32,
    pub source_digest: String,
    pub lock_digest: Option<String>,
    /// `pending` | `building` | `ready` | `failed`.
    pub build_state: String,
    #[schema(value_type = Option<Object>)]
    pub failure: Option<serde_json::Value>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct SetLiveBody {
    pub version: u32,
    /// Close the service's WebSockets with 1012 once the version is live (default false: they
    /// stay open and the next event runs this version).
    #[serde(default)]
    pub close_sockets: bool,
}

// ---- env, secrets, hosts ---------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct EnvMap(pub std::collections::BTreeMap<String, String>);

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PutSecretBody {
    pub value: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SecretNameView {
    pub name: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HostView {
    pub host: String,
    /// `pending_verification` | `active`.
    pub status: String,
    /// TXT value for `_rt-verify.<host>` while pending.
    pub verify_token: Option<String>,
    /// Unix ms when the claim was made.
    pub created_at: i64,
    /// Unix ms when a pending claim expires (`hosts.pending_ttl`); `null` once active.
    pub expires_at: Option<i64>,
}

// ---- schedules, queue ------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct ScheduleBody {
    /// `interval` | `cron`.
    pub kind: String,
    pub every_seconds: Option<u64>,
    /// Cron expression (contracts C-1).
    pub cron: Option<String>,
    /// IANA timezone, default `UTC`.
    pub timezone: Option<String>,
    /// JSON handed to the handler; on `PATCH` an absent payload keeps the current one.
    #[schema(value_type = Option<Object>)]
    pub payload: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ScheduleView {
    pub id: String,
    pub kind: String,
    pub every_seconds: Option<u64>,
    pub cron: Option<String>,
    pub timezone: Option<String>,
    #[schema(value_type = Object)]
    pub payload: serde_json::Value,
    pub enabled: bool,
    pub next_due: Option<i64>,
}

/// A pending one-off schedule (`std:schedule`). Its payload and enabled flag are not listed.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct OnceScheduleView {
    pub id: String,
    /// Always `once`.
    pub kind: String,
    /// Unix ms when the schedule is due.
    pub run_at: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct OncePage {
    pub items: Vec<OnceScheduleView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct EnqueueBody {
    #[schema(value_type = Object)]
    pub body: serde_json::Value,
    pub delay_seconds: Option<u64>,
    pub dedup_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Enqueued {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueueStatsView {
    pub depth: u64,
    pub in_flight: u64,
    pub oldest_age_ms: u64,
    pub dlq: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Redriven {
    pub redriven: u64,
}

// ---- logs, stats, events ---------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct LogsQuery {
    pub since: Option<String>,
    pub until: Option<String>,
    pub level: Option<String>,
    pub task_id: Option<String>,
    pub kind: Option<String>,
    /// `1|true|yes|on` switches to Server-Sent Events.
    pub follow: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LogView {
    pub seq: u64,
    pub ts: i64,
    pub level: String,
    pub service_id: String,
    pub version: u32,
    pub task_id: Option<String>,
    pub trigger: String,
    pub kind: String,
    pub msg: String,
    pub truncated: bool,
    /// Structured fields of a `std:log` record; absent on plain records.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub fields: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LogsPage {
    pub items: Vec<LogView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct StatsQuery {
    /// `5m` | `1h` | `24h`.
    pub window: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EventView {
    pub id: String,
    pub ts: i64,
    pub kind: String,
    #[schema(value_type = Object)]
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EventsPage {
    pub items: Vec<EventView>,
    pub next_cursor: Option<String>,
}

// ---- projects (D-B3) -------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateProjectBody {
    /// `^[a-z][a-z0-9-]{1,62}$`; unique per tenant, cannot be changed.
    pub slug: String,
    /// Display name (default: the slug).
    pub name: Option<String>,
    /// Environments services of the project may be in (default `["production"]`): 1 to 20 labels
    /// matching `^[a-z][a-z0-9-]{0,31}$`.
    pub environments: Option<Vec<String>>,
}

/// Each provided field replaces that field.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PatchProjectBody {
    pub name: Option<String>,
    /// The complete new list. Dropping an environment a service is in is `409 environment_in_use`.
    pub environments: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProjectView {
    pub id: String,
    pub tenant_id: String,
    pub slug: String,
    pub name: String,
    pub environments: Vec<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProjectPage {
    pub items: Vec<ProjectView>,
    pub next_cursor: Option<String>,
}

/// `GET /v1/projects/{p}/environments`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EnvironmentsView {
    /// Slug of the project.
    pub project: String,
    pub environments: Vec<String>,
}

// ---- self-service tokens (D-B4) --------------------------------------------------------------

/// `POST /v1/keys`: the scopes and project and environment restrictions must be within the
/// creating key's own; an omitted `project_ids` / `environments` / `expires_at` is the creating
/// key's own restriction / expiry.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateTokenBody {
    /// Scopes such as `services:read`, `deploy`; each one the creating key holds.
    pub scopes: Vec<String>,
    /// Limit the key to these projects (`prj_…` ids; a subset of the creating key's restriction).
    pub project_ids: Option<Vec<String>>,
    /// Limit the key to these environments (a subset of the creating key's restriction).
    pub environments: Option<Vec<String>>,
    /// RFC 3339 timestamp in the future, not later than the creating key's own expiry.
    pub expires_at: Option<String>,
    pub name: Option<String>,
}

// ---- limits ----------------------------------------------------------------------------------

/// One quota: the plan's limit and what is used.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QuotaView {
    /// The plan's limit; `null` = unlimited.
    pub limit: Option<u64>,
    pub used: u64,
    /// `limit - used` (0 when over); `null` = unlimited.
    pub remaining: Option<u64>,
    /// `day` (counted since UTC midnight), `total` (current holdings), `per_service`.
    pub period: String,
}

/// `GET /v1/limits`: the tenant's plan limits and what is left of them today.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LimitsView {
    /// Plan name.
    pub plan: String,
    /// UTC day the daily counters belong to (`YYYY-MM-DD`).
    pub day: String,
    /// Unix ms when the daily counters reset.
    pub resets_at: i64,
    pub invocations: QuotaView,
    pub egress_bytes: QuotaView,
    pub storage_bytes: QuotaView,
    pub services: QuotaView,
    /// `plan.max_schedules` applies per service: `used` is the most schedules any one service has.
    pub schedules: QuotaView,
}
