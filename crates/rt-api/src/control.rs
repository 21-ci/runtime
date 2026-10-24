//! The `/v1` control API (docs/api.md): route map, handlers, and the OpenAPI document generated
//! from the `#[utoipa::path]` annotations.
//!
//! Conventions (docs/api.md): ULIDs with prefixes; JSON bodies except artifact uploads,
//! source download and SSE; control errors use the `{error:{code,message,details}}` envelope;
//! `?limit&cursor` pagination; `Idempotency-Key` on POST (24 h, per principal); `ETag` on services
//! and `If-Match` on `PATCH`/`PUT live` (`412`); operator may act for a tenant with `x-rt-tenant`;
//! values above plan maxima → `422 limit_exceeds_plan`.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::Infallible;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use futures_util::stream::{self, Stream, StreamExt};
use rt_obs::{LogCursor, LogKind, LogLevel, LogQuery, LogRecord, Window};
use rt_sec::{secret_aad, AuditEvent, Principal, QuotaOp};
use rt_store::{PageReq, ServiceFilter, StoreError, TenantPatch, TenantScope, WarmFloorCap};
use rt_types::{
    Access, BuildState, CallAccess, CompileMode, DegradationMode, DenoServiceConfig, Digest, EgressRules, Etag,
    HostMapping, HostStatus, IngressRules, LimitsConfig, NewSchedule, NewService, NewTenant, NewVersion, Plan, Profile,
    ProjectId, RuntimeKind, ScalingConfig, Schedule, ScheduleId, ScheduleKind, Scope, Scopes, Service, ServiceConfig,
    ServiceId, ServicePatch, TaskId, TaskKind, TenantId, TriggerKind, TriggerSwitches, ValidationError, Version,
    VersionNo, WasmServiceConfig, WsServiceConfig,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

use crate::auth::Auth;
use crate::dto::*;
use crate::error::{ApiError, ErrorBody, ErrorEnvelope};
use crate::extract::{ApiJson, ApiQuery};
use crate::limits::limits_get;
use crate::projects::{
    check_environment_in_project, environments_get, find_project, project_slug_of, projects_create, projects_delete,
    projects_get, projects_list, projects_patch,
};
use crate::state::{ApiState, ControlEvent, IdemEntry, NodeConfigUpdate, SseSlot, IDEMPOTENCY_TTL};
use crate::tokens::{issue_key, revoke_key, tokens_create, tokens_delete, tokens_list};

type R<T> = Result<Json<T>, ApiError>;
type Created<T> = Result<(StatusCode, Json<T>), ApiError>;

// =============================================================================================
// helpers
// =============================================================================================

fn sid(s: &str) -> Result<ServiceId, ApiError> {
    s.parse::<ServiceId>().map_err(|_| ApiError::not_found())
}

fn tid(s: &str) -> Result<TenantId, ApiError> {
    s.parse::<TenantId>().map_err(|_| ApiError::not_found())
}

/// The system tenant (operator SDK services) is managed through `/v1/sdks`, never as a tenant.
fn not_reserved(t: TenantId) -> Result<TenantId, ApiError> {
    if rt_types::is_system_tenant(t) {
        Err(ApiError::new(
            "reserved",
            "the system tenant is managed through /v1/sdks",
        ))
    } else {
        Ok(t)
    }
}

/// A version number in a path: canonical decimal only (no sign, no leading zero), so one version
/// has one URL (CTRL-014).
fn vno(s: &str) -> Result<VersionNo, ApiError> {
    let canonical = !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && !(s.len() > 1 && s.starts_with('0'));
    if !canonical {
        return Err(ApiError::not_found());
    }
    s.parse::<u32>().map(VersionNo).map_err(|_| ApiError::not_found())
}

/// `1|true|yes|on` (case-insensitive) are true (worker convention).
fn truthy(v: Option<&str>) -> bool {
    v.is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

pub(crate) fn page_req(limit: Option<u32>, cursor: Option<String>) -> PageReq {
    PageReq {
        limit: limit.unwrap_or(50).clamp(1, 200),
        cursor,
    }
}

/// A display name (tenant, key, project): 1 to [`rt_types::MAX_PROJECT_NAME_LEN`] characters once
/// trimmed, no control characters. Names are echoed in lists, audit events and logs (KEY-037,
/// TEN-013).
pub(crate) fn check_display_name(field: &str, name: &str) -> Result<(), ApiError> {
    let n = name.trim().chars().count();
    if !(1..=rt_types::MAX_PROJECT_NAME_LEN).contains(&n) {
        return Err(ApiError::invalid(format!(
            "`{field}` must be 1 to {} characters",
            rt_types::MAX_PROJECT_NAME_LEN
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(ApiError::invalid(format!(
            "`{field}` must not contain control characters"
        )));
    }
    Ok(())
}

/// `ingress.ip_allow` must parse as CIDRs or addresses (INGRESS-006), and egress host entries must
/// be ASCII (the punycode form a URL host takes, EGR-007): otherwise an entry is stored and
/// silently matches nothing.
fn check_ingress_egress(ingress: Option<&IngressRules>, egress: Option<&EgressRules>) -> Result<(), ApiError> {
    if let Some(i) = ingress {
        rt_sec::IpAllowList::parse(&i.ip_allow).map_err(|e| ApiError::invalid(format!("ingress.ip_allow: {e}")))?;
    }
    if let Some(e) = egress {
        for (field, list) in [("allowed_hosts", &e.allowed_hosts), ("denied_hosts", &e.denied_hosts)] {
            if let Some(bad) = list.iter().find(|h| {
                h.is_empty() || !h.is_ascii() || h.chars().any(|c| c.is_ascii_whitespace() || c.is_ascii_control())
            }) {
                return Err(ApiError::invalid(format!(
                    "egress.{field}: `{bad}` must be an ASCII host name (write internationalized names in punycode, `xn--…`)"
                )));
            }
        }
    }
    Ok(())
}

/// A DNS host name a custom host may be (CHOST-003): at most 253 characters, at least two labels,
/// each 1 to 63 of `a-z0-9-` not starting or ending with `-`, and a top-level label that is not
/// all digits (no IP literals). Internationalized names come in punycode (`xn--`).
fn dns_host_name(host: &str) -> bool {
    let labels: Vec<&str> = host.split('.').collect();
    host.len() <= 253
        && labels.len() >= 2
        && labels.iter().all(|l| {
            (1..=63).contains(&l.len())
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        })
        && labels
            .last()
            .is_some_and(|tld| !tld.bytes().all(|c| c.is_ascii_digit()))
}

/// Tenant `overrides`: a JSON object, or `null` (TEN-011).
fn check_overrides(o: Option<&Value>) -> Result<(), ApiError> {
    match o {
        Some(o) if !o.is_object() && !o.is_null() => Err(ApiError::invalid("`overrides` must be an object")),
        _ => Ok(()),
    }
}

pub(crate) fn rfc3339_ms(s: &str) -> Result<i64, ApiError> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|d| d.timestamp_millis())
        .map_err(|_| ApiError::invalid(format!("`{s}` is not an RFC 3339 timestamp")))
}

fn block<T: DeserializeOwned>(name: &str, v: Option<Value>) -> Result<Option<T>, ApiError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => serde_json::from_value(v)
            .map(Some)
            .map_err(|e| ApiError::invalid(format!("invalid `{name}`: {e}"))),
    }
}

/// The `scaling` block of a PATCH: parsed and checked (`min_instances` above `max_instances` is
/// refused).
fn scaling_block(v: Option<Value>) -> Result<Option<ScalingConfig>, ApiError> {
    let scaling: Option<ScalingConfig> = block("scaling", v)?;
    if let Some(sc) = &scaling {
        sc.validate().map_err(ApiError::invalid)?;
    }
    Ok(scaling)
}

/// `scaling.min_instances` summed over the tenant's services must stay within the plan's
/// `max_warm_floor_total` (0 = no cap): warm floors are kept without traffic, so one tenant must
/// not reserve the node's warm memory. Each floor counts as the scheduler keeps it, clamped to the
/// plan's per-service `max_instances`. The meta store checks it in the transaction that writes the
/// service (two requests at once cannot both fit); refused with 422 `limit_exceeds_plan`. A plan
/// downgrade can leave a tenant above it: the node then keeps only the floors that fit (D-B2).
fn warm_floor_cap(plan: &rt_types::Plan) -> WarmFloorCap {
    WarmFloorCap {
        total: plan.max_warm_floor_total,
        per_service: plan.max_instances,
    }
}

fn parse_access(s: &str) -> Result<Access, ApiError> {
    match s {
        "public" => Ok(Access::Public),
        "key" => Ok(Access::Key),
        "internal" => Ok(Access::Internal),
        other => Err(ApiError::invalid(format!(
            "unknown access `{other}` (public|key|internal)"
        ))),
    }
}

fn parse_profile(s: &str) -> Result<Profile, ApiError> {
    Profile::from_str(s).map_err(ApiError::invalid)
}

/// An environment label (D-A1): a short slug.
fn parse_environment(s: &str) -> Result<String, ApiError> {
    if rt_types::valid_environment(s) {
        Ok(s.to_string())
    } else {
        Err(ApiError::invalid("`environment` must match ^[a-z][a-z0-9-]{0,31}$"))
    }
}

/// The `call` block: every `allowed_callers` entry is a slug, a `svc_…` id or `env:<name>`.
fn parse_call(v: Option<Value>) -> Result<Option<CallAccess>, ApiError> {
    let mut call: Option<CallAccess> = block("call", v)?;
    if let Some(c) = &mut call {
        // Ids are stored canonically: a lowercase ULID would validate but never match.
        c.canonicalize().map_err(ApiError::invalid)?;
    }
    Ok(call)
}

fn valid_slug(s: &str) -> bool {
    let b = s.as_bytes();
    (2..=63).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn valid_secret_name(n: &str) -> bool {
    let b = n.as_bytes();
    !b.is_empty()
        && b.len() <= 255
        && (b[0] == b'_' || b[0].is_ascii_uppercase())
        && b.iter()
            .all(|c| *c == b'_' || c.is_ascii_uppercase() || c.is_ascii_digit())
        && !n.starts_with("RT_")
}

fn check_limits(plan: &Plan, l: &LimitsConfig) -> Result<(), ApiError> {
    let over = |field: &str, requested: u64, max: u64| {
        ApiError::new(
            "limit_exceeds_plan",
            format!("`limits.{field}` {requested} exceeds the plan maximum {max}"),
        )
        .with_details(json!({"field": field, "requested": requested, "max": max}))
    };
    if let Some(v) = l.wall_ms {
        if v > plan.limits_max.wall_ms {
            return Err(over("wall_ms", v, plan.limits_max.wall_ms));
        }
    }
    if let Some(v) = l.cpu_ms {
        if v > plan.limits_max.cpu_ms {
            return Err(over("cpu_ms", v, plan.limits_max.cpu_ms));
        }
    }
    if let Some(v) = l.memory_mb {
        if v > plan.limits_max.memory_mb {
            return Err(over("memory_mb", u64::from(v), u64::from(plan.limits_max.memory_mb)));
        }
    }
    Ok(())
}

/// Re-read the route snapshot and the service table after a control change. Called by every
/// mutating handler before it answers, so a request right after the response sees the change.
pub async fn refresh_routes(s: &ApiState) -> Result<(), ApiError> {
    let snap = s.meta.route_snapshot().await?;
    s.routes.swap(&snap);
    let mut table = std::collections::HashMap::new();
    let mut cursor = None;
    loop {
        let page = s
            .meta
            .list_services(
                TenantScope::Operator,
                ServiceFilter::default(),
                PageReq { limit: 200, cursor },
            )
            .await?;
        for svc in page.items {
            table.insert(svc.id, Arc::new(svc));
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    // Per-service ingress state of deleted services goes with them.
    s.buckets.retain_services(|id| table.contains_key(id));
    s.ingress.retain_services(|id| table.contains_key(id));
    s.service_table.store(Arc::new(table));
    s.node.notify(ControlEvent::RoutesRefreshed);
    Ok(())
}

pub(crate) fn audit(
    s: &ApiState,
    a: &Auth,
    action: &str,
    tenant: Option<TenantId>,
    service: Option<ServiceId>,
    data: Value,
) {
    s.audit.emit(AuditEvent {
        ts_ms: s.clock.unix_ms(),
        actor: a.actor(),
        tenant,
        service,
        action: action.to_string(),
        data,
    });
}

/// Load a service the caller may see; anything else is `404` (existence is never revealed). A
/// key's scope is the service's project and environment, so the row is read first.
pub(crate) async fn load_service(s: &ApiState, a: &Auth, id: &str) -> Result<Service, ApiError> {
    let id = sid(id)?;
    let svc = s.meta.get_service(a.scope, id).await?.ok_or_else(ApiError::not_found)?;
    if !a.principal.can_touch(svc.project_id, &svc.environment) {
        return Err(ApiError::not_found());
    }
    Ok(svc)
}

/// Every service of `tenant` inside a key's project and environment scope (`None` = no scope).
async fn scoped_service_ids(
    s: &ApiState,
    tenant: TenantId,
    project_ids: Option<Vec<ProjectId>>,
    environments: Option<Vec<String>>,
) -> Result<Vec<ServiceId>, ApiError> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let filter = ServiceFilter {
            project_ids: project_ids.clone(),
            environments: environments.clone(),
            ..ServiceFilter::default()
        };
        let page = s
            .meta
            .list_services(TenantScope::Tenant(tenant), filter, page_req(Some(200), cursor.take()))
            .await?;
        out.extend(page.items.iter().map(|x| x.id));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(out),
        }
    }
}

fn json_resp<T: serde::Serialize>(status: StatusCode, v: &T) -> Response {
    let value = serde_json::to_value(v).unwrap_or(Value::Null);
    value_resp(status, &value)
}

pub(crate) fn value_resp(status: StatusCode, value: &Value) -> Response {
    let mut resp = (status, Json(value.clone())).into_response();
    if let Some(e) = value.get("etag").and_then(Value::as_str) {
        if let Ok(h) = HeaderValue::from_str(e) {
            resp.headers_mut().insert(header::ETAG, h);
        }
    }
    resp
}

fn if_match(h: &HeaderMap) -> Result<Option<Etag>, ApiError> {
    match h.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) {
        None => Ok(None),
        Some(v) => Etag::parse_header(v)
            .map(Some)
            .ok_or_else(|| ApiError::invalid("malformed If-Match header")),
    }
}

pub(crate) fn fingerprint(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_le_bytes());
        h.update(p);
    }
    h.finalize().into()
}

/// `Idempotency-Key` handling: `Ok(Some(resp))` replays a stored response, `Err` is a mismatch.
/// The stored answer for this request's `Idempotency-Key`, if any, and the key's turn: the guard
/// must live until the handler has stored its answer, so a second request with the same key
/// waits for the first and replays it (CTRL-008).
pub(crate) type IdemTurn = Option<tokio::sync::OwnedMutexGuard<()>>;

pub(crate) async fn idem_replay(
    s: &ApiState,
    a: &Auth,
    h: &HeaderMap,
    fp: [u8; 32],
) -> Result<(Option<Response>, IdemTurn), ApiError> {
    let Some(k) = h
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|k| !k.is_empty())
    else {
        return Ok((None, None));
    };
    if k.len() > crate::state::IDEMPOTENCY_MAX_KEY_BYTES {
        return Err(ApiError::invalid(format!(
            "an Idempotency-Key is at most {} bytes",
            crate::state::IDEMPOTENCY_MAX_KEY_BYTES
        )));
    }
    let key = format!("{}|{}", a.identity(), k);
    let turn = s.idem.turn(&key).await;
    match s.idem.get(&key) {
        Some(e) if e.fingerprint == fp => {
            let status = StatusCode::from_u16(e.status).unwrap_or(StatusCode::OK);
            Ok((Some(value_resp(status, &e.body)), None))
        }
        Some(_) => Err(ApiError::new(
            "idempotency_mismatch",
            "the Idempotency-Key was used with a different request",
        )),
        None => Ok((None, Some(turn))),
    }
}

pub(crate) fn idem_store(s: &ApiState, a: &Auth, h: &HeaderMap, fp: [u8; 32], status: StatusCode, body: &Value) {
    if let Some(k) = h
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|k| !k.is_empty() && k.len() <= crate::state::IDEMPOTENCY_MAX_KEY_BYTES)
    {
        s.idem.put(
            format!("{}|{}", a.identity(), k),
            IdemEntry {
                fingerprint: fp,
                status: status.as_u16(),
                body: body.clone(),
                expires: std::time::Instant::now() + IDEMPOTENCY_TTL,
            },
        );
    }
}

// ---- views -------------------------------------------------------------------------------------

fn tenant_view(t: &rt_types::Tenant) -> TenantView {
    TenantView {
        id: t.id.to_string(),
        name: t.name.clone(),
        plan: t.plan.clone(),
        overrides: if t.overrides.is_object() {
            t.overrides.clone()
        } else {
            json!({})
        },
        created_at: t.created_at_ms,
        disabled: t.disabled,
    }
}

/// A tenant key limited to some projects or environments (not the operator, not an unlimited key).
pub(crate) fn restricted(p: &Principal) -> bool {
    matches!(
        p,
        Principal::Tenant {
            project_ids: Some(_),
            ..
        } | Principal::Tenant {
            environments: Some(_),
            ..
        }
    )
}

pub(crate) fn key_view(k: &rt_types::ApiKey) -> KeyView {
    KeyView {
        id: k.id.to_string(),
        name: k.name.clone(),
        scopes: k.scopes.iter().map(|s| s.as_str().to_string()).collect(),
        project_ids: k
            .project_ids
            .as_ref()
            .map(|v| v.iter().map(ToString::to_string).collect()),
        environments: k.environments.clone(),
        expires_at: k.expires_at_ms,
        created_at: k.created_at_ms,
        last_used_at: k.last_used_at_ms,
    }
}

fn build_state_str(b: BuildState) -> String {
    match b {
        BuildState::Pending => "pending",
        BuildState::Building => "building",
        BuildState::Ready => "ready",
        BuildState::Failed => "failed",
    }
    .to_string()
}

fn version_view(v: &Version) -> VersionView {
    VersionView {
        version: v.number.0,
        source_digest: v.source_digest.to_string(),
        lock_digest: v.lock_digest.map(|d| d.to_string()),
        build_state: build_state_str(v.build_state),
        failure: v.failure.as_ref().and_then(|f| serde_json::to_value(f).ok()),
        created_at: v.created_at_ms,
    }
}

fn schedule_view(sch: &Schedule, now_ms: i64) -> ScheduleView {
    let (kind, every, cron, tz) = match &sch.kind {
        ScheduleKind::Interval { every_seconds } => ("interval", Some(*every_seconds), None, None),
        ScheduleKind::Cron { expr, tz } => ("cron", None, Some(expr.clone()), Some(tz.clone())),
    };
    let next_due = if sch.enabled {
        sch.next_due_ms
            .filter(|d| *d > now_ms)
            .or_else(|| rt_triggers::next_fire(&sch.kind, now_ms).ok().flatten())
    } else {
        None
    };
    ScheduleView {
        id: sch.id.to_string(),
        kind: kind.to_string(),
        every_seconds: every,
        cron,
        timezone: tz,
        payload: if sch.payload.is_null() {
            json!({})
        } else {
            sch.payload.clone()
        },
        enabled: sch.enabled,
        next_due,
    }
}

async fn service_view(s: &ApiState, svc: &Service) -> Result<ServiceView, ApiError> {
    let plan = s.plans.plan_for(svc.tenant_id);
    let cfg = s.config.load();
    let eff = cfg.service_effective(svc.profile, &svc.config, &plan);
    let versions = s.meta.list_versions(svc.id).await?;
    let live = svc.live_version.and_then(|n| versions.iter().find(|v| v.number == n));
    let latest = versions.iter().max_by_key(|v| v.number);
    let shown = live.or(latest);
    let build_state = shown.map_or("pending".to_string(), |v| build_state_str(v.build_state));
    let last_error = shown.and_then(|v| v.failure.as_ref()).map(|f| f.message.clone());
    let tier = s
        .routes
        .by_slug(&svc.slug)
        .map_or("T3", |e| if e.service == svc.id { s.node.tier(&e) } else { "T3" });
    let blk = |v: &dyn erased::Ser| v.to_value();
    let admission = admission_view(s, svc.id);
    let inflight = admission.as_ref().map_or(0, |a| a.running);
    Ok(ServiceView {
        id: svc.id.to_string(),
        tenant_id: svc.tenant_id.to_string(),
        slug: svc.slug.clone(),
        runtime: svc.runtime.as_str().to_string(),
        name: svc.name.clone(),
        profile: svc.profile.map(|p| p.as_str().to_string()),
        live_version: svc.live_version.map(|v| v.0),
        etag: svc.etag.to_header(),
        limits: blk(&svc.config.limits),
        scaling: blk(&svc.config.scaling),
        access: serde_json::to_value(svc.config.access)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default(),
        triggers: blk(&svc.config.triggers),
        ingress: blk(&svc.config.ingress),
        egress: blk(&svc.config.egress),
        project_id: svc.project_id.to_string(),
        project: svc.project.clone(),
        environment: svc.environment.clone(),
        call: blk(&svc.config.call),
        wasm: if svc.config.wasm.is_unset() {
            Value::Null
        } else {
            blk(&svc.config.wasm)
        },
        deno: if svc.config.deno.is_unset() {
            Value::Null
        } else {
            blk(&svc.config.deno)
        },
        durable: svc.config.durable,
        ws: if svc.config.ws.is_unset() {
            Value::Null
        } else {
            blk(&svc.config.ws)
        },
        status: ServiceStatus {
            build_state,
            tier: tier.to_string(),
            inflight,
            last_error,
            admission,
        },
        effective: serde_json::to_value(&eff).unwrap_or_else(|_| json!({})),
    })
}

/// Milliseconds with microsecond precision.
fn ms_f64(d: Duration) -> f64 {
    (d.as_secs_f64() * 1_000_000.0).round() / 1_000.0
}

/// The queueing / autoscaler view of one service (D-A3) from the scheduler's read-only accessor
/// and the published warm targets.
fn admission_view_of(
    sc: &rt_sched::ServiceScaling,
    warm: &HashMap<rt_sched::WarmKey, rt_sched::WarmTarget>,
    targets: &HashMap<ServiceId, u32>,
) -> AdmissionView {
    let a = &sc.admission;
    AdmissionView {
        kind: sc.kind.as_str().to_string(),
        limit: a.limit,
        min: a.min,
        max: a.max,
        running: a.running,
        queued: a.queued,
        queued_bytes: a.queued_bytes,
        max_queued: sc.max_queued,
        queue_wait_ms: ms_f64(sc.queue_wait),
        queue_timeouts: sc.queue_timeouts,
        demand: if sc.demand.is_finite() {
            (sc.demand * 1_000.0).round() / 1_000.0
        } else {
            0.0
        },
        latency_baseline_ms: sc.latency_baseline.map(ms_f64),
        warm_per_proc: warm
            .iter()
            .filter(|(k, _)| k.service == a.service)
            .map(|(_, t)| t.per_proc)
            .sum(),
        instances: a.running,
        min_instances: sc.floor,
        max_instances: a.max,
        target_instances: targets.get(&a.service).copied().unwrap_or(0),
        rps: if sc.rps.is_finite() {
            (sc.rps * 100.0).round() / 100.0
        } else {
            0.0
        },
        last_change: sc.last_change.map(|c| LimitChangeView {
            reason: c.cause.as_str().to_string(),
            from: c.from,
            to: c.to,
            ago_ms: u64::try_from(c.ago.as_millis()).unwrap_or(u64::MAX),
        }),
        idle_ms: u64::try_from(sc.idle_for.as_millis()).unwrap_or(u64::MAX),
    }
}

/// `None` while the service has no admission state (never run, or idle past its TTL).
fn admission_view(s: &ApiState, service: ServiceId) -> Option<AdmissionView> {
    let sc = s.scheduler.service_scaling(service)?;
    Some(admission_view_of(
        &sc,
        &s.scheduler.warm_targets().snapshot(),
        &s.scheduler.warm_instances(),
    ))
}

mod erased {
    pub trait Ser {
        fn to_value(&self) -> serde_json::Value;
    }
    impl<T: serde::Serialize> Ser for T {
        fn to_value(&self) -> serde_json::Value {
            serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
        }
    }
}

// =============================================================================================
// NODE (operator)
// =============================================================================================

/// Capacity, memory, profile, mode, P kinds.
#[utoipa::path(get, path = "/v1/node/status", tag = "node",
    responses((status = 200, body = NodeStatus), (status = 401, body = ErrorEnvelope)))]
async fn node_status(State(s): State<ApiState>, a: Auth) -> R<NodeStatus> {
    a.require_operator()?;
    let snap = s.scheduler.snapshot();
    let mut kinds = serde_json::Map::new();
    for k in TaskKind::ALL {
        kinds.insert(k.as_str().to_string(), json!(snap.procs_of(k)));
    }
    let processors = (snap.procs_of(TaskKind::DENO) + snap.procs_of(TaskKind::WASM)) as u32;
    let mode = match s.scheduler.mode() {
        DegradationMode::Normal => "normal",
        DegradationMode::Shed => "shed",
        DegradationMode::Drain => "drain",
    };
    Ok(Json(NodeStatus {
        version: s.node.version(),
        mode: mode.to_string(),
        default_profile: s.config.load().node.default_profile.as_str().to_string(),
        v8_sandbox: s.node.v8_sandbox(),
        processors,
        kinds: Value::Object(kinds),
        memory: json!({"budget_bytes": s.mem.budget_bytes(), "committed_bytes": s.mem.stats().committed_bytes, "pressure": format!("{:?}", s.mem.pressure())}),
        inflight: snap.inflight,
        sockets: {
            let st = s.sockets.stats();
            NodeSockets {
                open: st.sockets,
                objects: st.objects,
                pending: st.pending,
                max: s.sockets.config().max_sockets,
            }
        },
    }))
}

/// Live-updatable knobs; restart-only keys come back as `pending_restart` (CFG-05).
#[utoipa::path(put, path = "/v1/node/config", tag = "node", request_body = NodeConfigUpdate,
    responses((status = 200, body = NodeConfigApplied), (status = 422, body = ErrorEnvelope)))]
async fn node_config_put(
    State(s): State<ApiState>,
    a: Auth,
    ApiJson(b): ApiJson<NodeConfigUpdate>,
) -> R<NodeConfigApplied> {
    a.require_operator()?;
    let mode = match b.degradation_mode.as_deref() {
        None => None,
        Some("normal") => Some(DegradationMode::Normal),
        Some("shed") => Some(DegradationMode::Shed),
        Some("drain") => Some(DegradationMode::Drain),
        Some(other) => {
            return Err(ApiError::invalid(format!(
                "unknown degradation_mode `{other}` (normal|shed|drain)"
            )))
        }
    };
    if let Some(p) = &b.default_profile {
        parse_profile(p)?;
    }
    let mut asked = Vec::new();
    if b.degradation_mode.is_some() {
        asked.push("degradation_mode".to_string());
    }
    if b.default_profile.is_some() {
        asked.push("default_profile".to_string());
    }
    if b.rate_limit.is_some() {
        asked.push("rate_limit".to_string());
    }
    let pending = s.node.apply_live_config(&b).map_err(ApiError::invalid)?;
    if let Some(m) = mode {
        s.scheduler.set_mode(m);
        if m == DegradationMode::Drain {
            s.node.start_drain();
        }
    }
    let applied = asked.into_iter().filter(|k| !pending.contains(k)).collect();
    Ok(Json(NodeConfigApplied {
        applied,
        pending_restart: pending,
    }))
}

#[utoipa::path(post, path = "/v1/node/drain", tag = "node", responses((status = 202, description = "draining")))]
async fn node_drain(State(s): State<ApiState>, a: Auth) -> Result<StatusCode, ApiError> {
    a.require_operator()?;
    s.scheduler.set_mode(DegradationMode::Drain);
    s.node.start_drain();
    Ok(StatusCode::ACCEPTED)
}

/// P/M/queue snapshot (schedtrace JSON) plus `admission` (node-wide waiting tasks and bounds) and
/// `services` (per service: `service_id`, `tenant_id`, `slug` and the `AdmissionView` fields:
/// adaptive limit and its instance bounds, running, queued, smoothed queue wait, timeouts, demand,
/// warm target, arrival rate, the reason of the limit's last move); operator only (API-14).
#[utoipa::path(get, path = "/v1/node/scheduler", tag = "node",
    responses((status = 200, description = "SchedSnapshot + admission + services", body = Object), (status = 403, body = ErrorEnvelope)))]
async fn node_scheduler(State(s): State<ApiState>, a: Auth) -> R<Value> {
    a.require_operator()?;
    let mut v = serde_json::to_value(s.scheduler.snapshot()).unwrap_or(Value::Null);
    let warm = s.scheduler.warm_targets().snapshot();
    let targets = s.scheduler.warm_instances();
    let table = s.service_table.load();
    let services: Vec<Value> = s
        .scheduler
        .services_scaling()
        .iter()
        .map(|sc| {
            let mut e = serde_json::Map::new();
            e.insert("service_id".into(), json!(sc.admission.service.to_string()));
            e.insert("tenant_id".into(), json!(sc.admission.tenant.to_string()));
            e.insert(
                "slug".into(),
                table.get(&sc.admission.service).map_or(Value::Null, |x| json!(x.slug)),
            );
            if let Value::Object(fields) =
                serde_json::to_value(admission_view_of(sc, &warm, &targets)).unwrap_or(Value::Null)
            {
                e.extend(fields);
            }
            Value::Object(e)
        })
        .collect();
    let (queued, queued_bytes) = s.scheduler.queued();
    let lim = s.scheduler.admission_limits();
    if let Some(o) = v.as_object_mut() {
        o.insert(
            "admission".into(),
            json!({
                "queued": queued, "queued_bytes": queued_bytes, "max_inflight": lim.max_inflight,
                "max_queued": lim.max_queued, "max_queued_bytes": lim.max_queued_bytes,
            }),
        );
        o.insert("services".into(), Value::Array(services));
    }
    Ok(Json(v))
}

/// Secrets never live in `rt.toml` (root token, master keys come from env or files), but two kinds
/// of values can still carry credentials: `[[binding]].inject_headers` values that are not
/// `env:NAME` references, and URLs with user info. `GET /v1/node/config` masks both.
const REDACTED: &str = "<redacted>";

fn redact_config(v: &mut Value) {
    if let Some(bindings) = v.get_mut("binding").and_then(Value::as_array_mut) {
        for b in bindings {
            if let Some(h) = b.get_mut("inject_headers").and_then(Value::as_object_mut) {
                for val in h.values_mut() {
                    if !val.as_str().is_some_and(|s| s.starts_with("env:")) {
                        *val = json!(REDACTED);
                    }
                }
            }
        }
    }
    redact_userinfo(v);
}

fn redact_userinfo(v: &mut Value) {
    match v {
        Value::String(s) => {
            if let Some(r) = strip_userinfo(s) {
                *s = r;
            }
        }
        Value::Array(a) => a.iter_mut().for_each(redact_userinfo),
        Value::Object(o) => o.values_mut().for_each(redact_userinfo),
        _ => {}
    }
}

/// `scheme://user:pass@host/…` -> `scheme://<redacted>@host/…`.
fn strip_userinfo(s: &str) -> Option<String> {
    let (scheme, rest) = s.split_once("://")?;
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let at = rest[..end].rfind('@')?;
    Some(format!("{scheme}://{REDACTED}@{}", &rest[at + 1..]))
}

/// The configuration the node runs with (`rt.toml` plus reloads), credentials masked; operator
/// only. Node secrets (root token, master keys) are never part of it.
#[utoipa::path(get, path = "/v1/node/config", tag = "node",
    responses((status = 200, description = "The node configuration (rt.toml sections), credentials masked", body = Object),
        (status = 403, body = ErrorEnvelope)))]
async fn node_config_get(State(s): State<ApiState>, a: Auth) -> R<Value> {
    a.require_operator()?;
    let mut v = serde_json::to_value(&**s.config.load()).unwrap_or_else(|_| json!({}));
    redact_config(&mut v);
    Ok(Json(v))
}

/// The configured plans (`[plans.*]`); operator only. A tenant reads its own plan from `/v1/me`.
#[utoipa::path(get, path = "/v1/plans", tag = "tenants",
    responses((status = 200, body = Vec<PlanView>), (status = 403, body = ErrorEnvelope)))]
async fn plans_list(State(s): State<ApiState>, a: Auth) -> R<Vec<PlanView>> {
    a.require_operator()?;
    let cfg = s.config.load();
    Ok(Json(
        cfg.plans
            .iter()
            .map(|(name, p)| PlanView {
                name: name.clone(),
                settings: serde_json::to_value(p).unwrap_or_else(|_| json!({})),
            })
            .collect(),
    ))
}

/// `info.version` of the OpenAPI document, computed once.
fn api_version() -> &'static str {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    V.get_or_init(|| api_doc().info.version)
}

/// The node and API versions; any authenticated caller (no node internals).
#[utoipa::path(get, path = "/v1/version", tag = "meta",
    responses((status = 200, body = VersionInfo), (status = 401, body = ErrorEnvelope)))]
async fn version_get(State(s): State<ApiState>, _a: Auth) -> R<VersionInfo> {
    Ok(Json(VersionInfo {
        version: s.node.version(),
        api: api_version().to_string(),
    }))
}

/// Who the credentials are: principal, tenant, key, scopes, project and environment restriction, and the
/// tenant's effective plan (with its overrides). Any authenticated caller.
#[utoipa::path(get, path = "/v1/me", tag = "meta",
    responses((status = 200, body = MeView), (status = 401, body = ErrorEnvelope)))]
async fn me_get(State(s): State<ApiState>, a: Auth) -> R<MeView> {
    let names = |sc: &Scopes| sc.iter().map(|x| x.as_str().to_string()).collect::<Vec<_>>();
    let (principal, key_id, scopes, project_ids, environments) = match &a.principal {
        Principal::Operator => ("operator", None, names(&Scopes::all()), None, None),
        Principal::Tenant {
            key,
            scopes,
            project_ids,
            environments,
            ..
        } => (
            "tenant",
            Some(key.to_string()),
            names(scopes),
            project_ids
                .as_ref()
                .map(|v| v.iter().map(ToString::to_string).collect()),
            environments.clone(),
        ),
        Principal::Internal { .. } | Principal::Anonymous => return Err(ApiError::new("unauthorized", "unauthorized")),
    };
    let tenant = match a.scope {
        TenantScope::Tenant(t) => Some(t),
        TenantScope::Operator => None,
    };
    let plan = match tenant {
        Some(t) => {
            let name = match s.tenant_table.get(&t).map(|x| x.plan.clone()) {
                Some(n) => n,
                None => s.meta.get_tenant(t).await?.map(|x| x.plan).unwrap_or_default(),
            };
            Some(PlanView {
                name,
                settings: serde_json::to_value(s.plans.plan_for(t)).unwrap_or_else(|_| json!({})),
            })
        }
        None => None,
    };
    Ok(Json(MeView {
        principal: principal.to_string(),
        tenant_id: tenant.map(|t| t.to_string()),
        key_id,
        scopes,
        project_ids,
        environments,
        plan,
        version: s.node.version(),
    }))
}

/// Tier sizes, hit rates, top entries.
#[utoipa::path(get, path = "/v1/node/cache", tag = "node", responses((status = 200, body = Object)))]
async fn node_cache(State(s): State<ApiState>, a: Auth) -> R<Value> {
    a.require_operator()?;
    let v = s.node.cache();
    Ok(Json(if v.is_object() { v } else { json!({}) }))
}

// =============================================================================================
// TENANTS + KEYS (operator)
// =============================================================================================

#[utoipa::path(post, path = "/v1/tenants", tag = "tenants", request_body = CreateTenantBody,
    responses((status = 201, body = TenantView), (status = 422, body = ErrorEnvelope)))]
async fn tenants_create(
    State(s): State<ApiState>,
    a: Auth,
    h: HeaderMap,
    ApiJson(b): ApiJson<CreateTenantBody>,
) -> Result<Response, ApiError> {
    a.require_operator()?;
    let fp = fingerprint(&[b"tenants", format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    check_display_name("name", &b.name)?;
    if !s.config.load().plans.contains_key(&b.plan) {
        return Err(ApiError::invalid(format!("unknown plan `{}`", b.plan)));
    }
    check_overrides(b.overrides.as_ref())?;
    let t = s
        .meta
        .create_tenant(NewTenant {
            name: b.name,
            plan: b.plan,
            overrides: b.overrides,
        })
        .await?;
    s.tenant_table.insert(t.id, t.clone());
    audit(&s, &a, "tenant.created", Some(t.id), None, json!({"plan": t.plan}));
    let view = serde_json::to_value(tenant_view(&t)).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::CREATED, &view);
    Ok(value_resp(StatusCode::CREATED, &view))
}

#[utoipa::path(get, path = "/v1/tenants", tag = "tenants", params(PageQuery), responses((status = 200, body = TenantPage)))]
async fn tenants_list(State(s): State<ApiState>, a: Auth, ApiQuery(q): ApiQuery<PageQuery>) -> R<TenantPage> {
    a.require_operator()?;
    let page = s.meta.list_tenants(page_req(q.limit, q.cursor)).await?;
    let items = page
        .items
        .iter()
        .filter(|t| !rt_types::is_system_tenant(t.id))
        .map(tenant_view)
        .collect();
    Ok(Json(TenantPage {
        items,
        next_cursor: page.next_cursor,
    }))
}

#[utoipa::path(get, path = "/v1/tenants/{t}", tag = "tenants", params(("t" = String, Path, description = "tenant id")),
    responses((status = 200, body = TenantView), (status = 404, body = ErrorEnvelope)))]
async fn tenants_get(State(s): State<ApiState>, a: Auth, Path(t): Path<String>) -> R<TenantView> {
    a.require_operator()?;
    let t = s.meta.get_tenant(tid(&t)?).await?.ok_or_else(ApiError::not_found)?;
    Ok(Json(tenant_view(&t)))
}

#[utoipa::path(patch, path = "/v1/tenants/{t}", tag = "tenants", request_body = PatchTenantBody,
    params(("t" = String, Path, description = "tenant id")), responses((status = 200, body = TenantView)))]
async fn tenants_patch(
    State(s): State<ApiState>,
    a: Auth,
    Path(t): Path<String>,
    ApiJson(b): ApiJson<PatchTenantBody>,
) -> R<TenantView> {
    a.require_operator()?;
    let id = not_reserved(tid(&t)?)?;
    if let Some(n) = &b.name {
        check_display_name("name", n)?;
    }
    check_overrides(b.overrides.as_ref())?;
    if let Some(p) = &b.plan {
        if !s.config.load().plans.contains_key(p) {
            return Err(ApiError::invalid(format!("unknown plan `{p}`")));
        }
    }
    let before = s
        .tenant_table
        .get(&id)
        .map(|t| (t.disabled, t.plan.clone(), t.overrides.clone()));
    let was_disabled = before.as_ref().map(|b| b.0);
    let t = s
        .meta
        .update_tenant(
            id,
            TenantPatch {
                name: b.name,
                plan: b.plan,
                overrides: b.overrides,
                disabled: b.disabled,
            },
        )
        .await?;
    s.tenant_table.insert(t.id, t.clone());
    if was_disabled != Some(t.disabled) {
        // Disabling (or re-enabling) takes effect at once: the route snapshot drops the tenant's
        // services and cached copies of its keys are re-checked against the tenant table.
        if t.disabled {
            // Their decrypted env/secrets leave the node's memory too.
            let svcs: Vec<ServiceId> = s
                .service_table
                .load()
                .values()
                .filter(|x| x.tenant_id == t.id)
                .map(|x| x.id)
                .collect();
            for svc in svcs {
                s.node.notify(ControlEvent::SecretsChanged(svc));
            }
        }
        refresh_routes(&s).await?;
        for k in s.meta.list_keys(t.id).await? {
            s.node.notify(ControlEvent::KeyUpserted(k));
        }
    } else if before.as_ref().map(|b| (&b.1, &b.2)) != Some((&t.plan, &t.overrides)) {
        // A plan (or override) change re-resolves what the plan bounds now, before the answer:
        // warm floors above the new `max_warm_floor_total` are released (`sync_floors`).
        s.node.notify(ControlEvent::RoutesRefreshed);
    }
    audit(
        &s,
        &a,
        "tenant.updated",
        Some(t.id),
        None,
        json!({"plan": t.plan, "disabled": t.disabled}),
    );
    Ok(Json(tenant_view(&t)))
}

#[utoipa::path(delete, path = "/v1/tenants/{t}", tag = "tenants",
    params(("t" = String, Path, description = "tenant id"), PurgeQuery),
    responses((status = 204), (status = 404, body = ErrorEnvelope), (status = 409, body = ErrorEnvelope)))]
async fn tenants_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path(t): Path<String>,
    ApiQuery(q): ApiQuery<PurgeQuery>,
) -> Result<StatusCode, ApiError> {
    a.require_operator()?;
    let id = not_reserved(tid(&t)?)?;
    s.meta.get_tenant(id).await?.ok_or_else(ApiError::not_found)?;
    let key_ids: Vec<rt_types::KeyId> = s.meta.list_keys(id).await?.iter().map(|k| k.id).collect();
    let purge = truthy(q.purge.as_deref());
    let mut services = Vec::new();
    let mut cursor = None;
    loop {
        let page = s
            .meta
            .list_services(
                TenantScope::Tenant(id),
                ServiceFilter::default(),
                PageReq { limit: 200, cursor },
            )
            .await?;
        services.extend(page.items.iter().map(|x| x.id));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    s.meta.delete_tenant(id, purge).await?;
    for svc in services {
        if purge {
            let _ = s.data.retire(svc);
        }
        // Schedules, caches and the decrypted env/secrets of every service go.
        s.node.notify(ControlEvent::ServiceDeleted(svc));
    }
    for k in key_ids {
        s.node.notify(ControlEvent::KeyRevoked(k));
    }
    s.tenant_table.remove(&id);
    refresh_routes(&s).await?;
    audit(&s, &a, "tenant.deleted", Some(id), None, json!({"purge": purge}));
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path = "/v1/tenants/{t}/usage", tag = "tenants",
    params(("t" = String, Path, description = "tenant id"), UsageQuery), responses((status = 200, body = Vec<UsageView>)))]
async fn tenants_usage(
    State(s): State<ApiState>,
    a: Auth,
    Path(t): Path<String>,
    ApiQuery(q): ApiQuery<UsageQuery>,
) -> R<Vec<UsageView>> {
    let id = tid(&t)?;
    // The operator reads any tenant's usage; a tenant key its own (scope `logs:read`), limited to
    // the services the key may touch. Another tenant's usage does not exist for it (404).
    let only: Option<Vec<ServiceId>> = match &a.principal {
        Principal::Operator => None,
        Principal::Tenant {
            tenant,
            project_ids,
            environments,
            ..
        } if *tenant == id => {
            a.require_scope(Scope::LogsRead)?;
            match (project_ids, environments) {
                (None, None) => None,
                (p, e) => Some(scoped_service_ids(&s, id, p.clone(), e.clone()).await?),
            }
        }
        _ => return Err(ApiError::not_found()),
    };
    s.meta.get_tenant(id).await?.ok_or_else(ApiError::not_found)?;
    let day = |v: &Option<String>, default: &str| -> Result<String, ApiError> {
        match v {
            None => Ok(default.to_string()),
            // A real calendar day in the `YYYY-MM-DD` shape (SCOPE-012).
            Some(d)
                if d.len() == 10
                    && d.bytes().enumerate().all(|(i, c)| {
                        if i == 4 || i == 7 {
                            c == b'-'
                        } else {
                            c.is_ascii_digit()
                        }
                    })
                    && chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").is_ok() =>
            {
                Ok(d.clone())
            }
            Some(d) => Err(ApiError::invalid(format!("`{d}` is not a YYYY-MM-DD date"))),
        }
    };
    let rows = s
        .meta
        .get_usage(id, &day(&q.from, "0000-01-01")?, &day(&q.to, "9999-12-31")?)
        .await?;
    Ok(Json(
        rows.into_iter()
            .filter(|r| only.as_ref().is_none_or(|o| o.contains(&r.service)))
            .map(|r| UsageView {
                service_id: r.service.to_string(),
                day: r.day,
                invocations: r.invocations,
                cpu_ms: r.cpu_ms,
                egress_bytes: r.egress_bytes,
                storage_bytes: r.storage_bytes,
            })
            .collect(),
    ))
}

/// The secret is shown once (API-13).
#[utoipa::path(post, path = "/v1/tenants/{t}/keys", tag = "keys", request_body = CreateKeyBody,
    params(("t" = String, Path, description = "tenant id")), responses((status = 201, body = KeyCreated)))]
async fn keys_create(
    State(s): State<ApiState>,
    a: Auth,
    Path(t): Path<String>,
    ApiJson(b): ApiJson<CreateKeyBody>,
) -> Created<KeyCreated> {
    a.require_operator()?;
    let tenant = not_reserved(tid(&t)?)?;
    Ok((StatusCode::CREATED, Json(issue_key(&s, &a, tenant, b, false).await?)))
}

#[utoipa::path(get, path = "/v1/tenants/{t}/keys", tag = "keys",
    params(("t" = String, Path, description = "tenant id")), responses((status = 200, body = Vec<KeyView>)))]
async fn keys_list(State(s): State<ApiState>, a: Auth, Path(t): Path<String>) -> R<Vec<KeyView>> {
    a.require_operator()?;
    let tenant = tid(&t)?;
    s.meta.get_tenant(tenant).await?.ok_or_else(ApiError::not_found)?;
    let keys = s.meta.list_keys(tenant).await?;
    Ok(Json(keys.iter().filter(|k| !k.revoked).map(key_view).collect()))
}

#[utoipa::path(delete, path = "/v1/tenants/{t}/keys/{key}", tag = "keys",
    params(("t" = String, Path, description = "tenant id"), ("key" = String, Path, description = "key id")),
    responses((status = 204)))]
async fn keys_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path((t, k)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    a.require_operator()?;
    let tenant = tid(&t)?;
    let key: rt_types::KeyId = k.parse().map_err(|_| ApiError::not_found())?;
    revoke_key(&s, &a, tenant, key, false).await?;
    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================================
// SERVICES (tenant key with scopes, or operator)
// =============================================================================================

#[utoipa::path(post, path = "/v1/services", tag = "services", request_body = CreateServiceBody,
    responses((status = 201, body = ServiceView), (status = 409, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
async fn services_create(
    State(s): State<ApiState>,
    a: Auth,
    h: HeaderMap,
    ApiJson(b): ApiJson<CreateServiceBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let fp = fingerprint(&[b"services", format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    let tenant = a.tenant()?;
    if !valid_slug(&b.slug) {
        return Err(ApiError::invalid("`slug` must match ^[a-z][a-z0-9-]{1,62}$"));
    }
    if b.slug.starts_with(rt_types::SYSTEM_SLUG_PREFIX) && !rt_types::is_system_tenant(tenant) {
        return Err(ApiError::invalid(format!(
            "slugs starting with `{}` are reserved",
            rt_types::SYSTEM_SLUG_PREFIX
        )));
    }
    let runtime = RuntimeKind::from_str(&b.runtime).map_err(ApiError::invalid)?;
    let profile = b.profile.as_deref().map(parse_profile).transpose()?;
    let access = b.access.as_deref().map(parse_access).transpose()?.unwrap_or_default();
    let limits: LimitsConfig = block("limits", b.limits)?.unwrap_or_default();
    let scaling: ScalingConfig = block("scaling", b.scaling)?.unwrap_or_default();
    scaling.validate().map_err(ApiError::invalid)?;
    let triggers: TriggerSwitches = block("triggers", b.triggers)?.unwrap_or_default();
    let ingress: IngressRules = block("ingress", b.ingress)?.unwrap_or_default();
    let egress: EgressRules = block("egress", b.egress)?.unwrap_or_default();
    check_ingress_egress(Some(&ingress), Some(&egress))?;
    let call = parse_call(b.call)?.unwrap_or_default();
    let wasm: WasmServiceConfig = block("wasm", b.wasm)?.unwrap_or_default();
    let deno: DenoServiceConfig = block("deno", b.deno)?.unwrap_or_default();
    let ws: WsServiceConfig = block("ws", b.ws)?.unwrap_or_default();
    let durable = b.durable.unwrap_or(false);
    let environment = b
        .environment
        .as_deref()
        .map(parse_environment)
        .transpose()?
        .unwrap_or_else(rt_types::default_environment);
    let project = project_slug_of(&s, tenant, b.project.as_deref()).await?;
    check_environment_in_project(&s, tenant, &project, &environment).await?;
    // A key limited to some projects or environments creates services only inside them, the same
    // rule `load_service` applies to every later read and change.
    let project_id = match find_project(&s, tenant, &project).await? {
        Some(p) => p.id,
        None => rt_types::default_project_id(tenant),
    };
    if !a.principal.can_touch(project_id, &environment) {
        return Err(ApiError::new(
            "insufficient_scope",
            format!("this key may not create services in project `{project}`, environment `{environment}`"),
        ));
    }
    let plan = s.plans.plan_for(tenant);
    check_limits(&plan, &limits)?;
    let current = s.meta.count_services(tenant).await?;
    s.quotas.check(tenant, QuotaOp::CreateService { current })?;
    let svc = s
        .meta
        .create_service_limited(
            a.scope,
            tenant,
            NewService {
                slug: b.slug,
                runtime,
                name: b.name,
                profile,
                environment,
                project,
                config: ServiceConfig {
                    limits,
                    scaling,
                    access,
                    triggers,
                    ingress,
                    egress,
                    call,
                    wasm,
                    deno,
                    durable,
                    ws,
                    events: Default::default(),
                    async_invoke: Default::default(),
                },
            },
            warm_floor_cap(&plan),
            plan.max_services,
        )
        .await?;
    audit(
        &s,
        &a,
        "service.created",
        Some(tenant),
        Some(svc.id),
        json!({"slug": svc.slug}),
    );
    let view = serde_json::to_value(service_view(&s, &svc).await?).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::CREATED, &view);
    Ok(value_resp(StatusCode::CREATED, &view))
}

/// `?slug=<slug>` returns a list with 0 or 1 item (contracts C-9).
#[utoipa::path(get, path = "/v1/services", tag = "services", params(ServicesQuery), responses((status = 200, body = ServicePage)))]
async fn services_list(State(s): State<ApiState>, a: Auth, ApiQuery(q): ApiQuery<ServicesQuery>) -> R<ServicePage> {
    a.require_scope(Scope::ServicesRead)?;
    // The store resolves the project by slug or `prj_` id, and applies the key's scope in SQL, so
    // pages are full and the total is exact.
    let (project_ids, environments) = match &a.principal {
        Principal::Tenant {
            project_ids,
            environments,
            ..
        } => (project_ids.clone(), environments.clone()),
        _ => (None, None),
    };
    let filter = ServiceFilter {
        slug: q.slug,
        project: q.project,
        environment: q.environment,
        project_ids,
        environments,
    };
    let page = s
        .meta
        .list_services(a.scope, filter, page_req(q.limit, q.cursor))
        .await?;
    let mut items = Vec::new();
    for svc in &page.items {
        items.push(service_view(&s, svc).await?);
    }
    Ok(Json(ServicePage {
        items,
        next_cursor: page.next_cursor,
    }))
}

#[utoipa::path(get, path = "/v1/services/{s}", tag = "services", params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = ServiceView), (status = 404, body = ErrorEnvelope)))]
async fn services_get(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    Ok(json_resp(StatusCode::OK, &service_view(&s, &svc).await?))
}

#[utoipa::path(patch, path = "/v1/services/{s}", tag = "services", request_body = PatchServiceBody,
    params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = ServiceView), (status = 412, body = ErrorEnvelope)))]
async fn services_patch(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
    ApiJson(b): ApiJson<PatchServiceBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    if let Some(named) = b.project.as_deref() {
        let same = find_project(&s, svc.tenant_id, named)
            .await?
            .is_some_and(|p| p.slug == svc.project);
        if !same {
            return Err(ApiError::invalid(format!(
                "a service cannot move between projects: it stays in `{}`; create it in the other project instead",
                svc.project
            )));
        }
    }
    let limits: Option<LimitsConfig> = block("limits", b.limits)?;
    if let Some(l) = &limits {
        check_limits(&s.plans.plan_for(svc.tenant_id), l)?;
    }
    let environment = b.environment.as_deref().map(parse_environment).transpose()?;
    let env_change = environment
        .as_deref()
        .filter(|e| *e != svc.environment)
        .map(str::to_string);
    if env_change.is_some() && restricted(&a.principal) {
        // The environment decides which services may call this one and which ones it may call
        // (D-A1): a key limited to some projects or environments must not move one elsewhere.
        return Err(ApiError::new(
            "insufficient_scope",
            "changing `environment` needs the operator or a tenant key that is not limited to some projects or environments",
        ));
    }
    if let Some(env) = &env_change {
        // D-B3: the new environment must be one of the service's project.
        check_environment_in_project(&s, svc.tenant_id, &svc.project, env).await?;
    }
    // Turning a WASM service durable: its live version must take socket events (as an upload must).
    if b.durable == Some(true) && !svc.config.durable && svc.runtime == RuntimeKind::Wasm {
        if let Some(live) = svc.live_version {
            check_version_socket_handler(&s, &svc, live).await?;
        }
    }
    let patch = ServicePatch {
        name: b.name,
        profile: b.profile.as_deref().map(parse_profile).transpose()?,
        environment,
        call: parse_call(b.call)?,
        wasm: block("wasm", b.wasm)?,
        deno: block("deno", b.deno)?,
        durable: b.durable,
        ws: block("ws", b.ws)?,
        limits,
        scaling: scaling_block(b.scaling)?,
        access: b.access.as_deref().map(parse_access).transpose()?,
        triggers: block("triggers", b.triggers)?,
        ingress: block("ingress", b.ingress)?,
        egress: block("egress", b.egress)?,
        events: Default::default(),
        async_invoke: Default::default(),
    };
    check_ingress_egress(patch.ingress.as_ref(), patch.egress.as_ref())?;
    // `min_instances` / `max_instances` come from the scaling block (and the profile's ceiling):
    // the scheduler learns the new bounds with the service's next request, but a floor it holds for
    // an idle service must not outlive an update that may have removed it (D-B2)
    let bounds_may_change = patch.scaling.is_some() || patch.profile.is_some();
    let cap = warm_floor_cap(&s.plans.plan_for(svc.tenant_id));
    let updated = s
        .meta
        .patch_service_capped(a.scope, svc.id, patch, if_match(&h)?, cap)
        .await?;
    if bounds_may_change {
        s.scheduler.release_floor(updated.id);
    }
    refresh_routes(&s).await?;
    audit(
        &s,
        &a,
        "service.updated",
        Some(updated.tenant_id),
        Some(updated.id),
        json!({}),
    );
    if let Some(to) = env_change {
        let data = json!({"from": svc.environment, "to": to});
        audit(
            &s,
            &a,
            "service.environment_changed",
            Some(updated.tenant_id),
            Some(updated.id),
            data,
        );
    }
    Ok(json_resp(StatusCode::OK, &service_view(&s, &updated).await?))
}

#[utoipa::path(delete, path = "/v1/services/{s}", tag = "services",
    params(("s" = String, Path, description = "service id"), DeleteServiceQuery), responses((status = 204)))]
async fn services_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<DeleteServiceQuery>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let keep = truthy(q.keep_data.as_deref());
    s.meta.delete_service(a.scope, svc.id, keep).await?;
    if !keep {
        // Retired, not only purged: a task of the deleted service that is still running must not
        // re-create its files (KV-024).
        let _ = s.data.retire(svc.id);
    }
    refresh_routes(&s).await?;
    s.node.notify(ControlEvent::ServiceDeleted(svc.id));
    audit(
        &s,
        &a,
        "service.deleted",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"keep_data": keep}),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// A durable WASM service takes its events and alarms through `std:host/socket-handler`:
/// `422 missing_export` without it.
fn check_socket_handler(exports: &[String]) -> Result<(), ApiError> {
    if exports.iter().any(|e| e.starts_with("std:host/socket-handler")) {
        return Ok(());
    }
    Err(ApiError::new(
        "missing_export",
        "a durable WASM service must export `std:host/socket-handler` (world `socket-service`)",
    ))
}

/// [`check_socket_handler`] on the stored artifact of version `number` (a version that no longer
/// validates fails the same way).
async fn check_version_socket_handler(s: &ApiState, svc: &Service, number: VersionNo) -> Result<(), ApiError> {
    let Some(v) = s.meta.get_version(svc.id, number).await? else {
        return Ok(()); // the store answers `not_found` for the version itself
    };
    let body = s.artifacts.get(&v.source_digest).await?;
    let content_type = s
        .artifacts
        .content_type(&v.source_digest)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "application/wasm".to_string());
    let exports = s
        .validator
        .validate(svc.runtime, &content_type, &body)
        .map(|v| v.exports)
        .unwrap_or_default();
    check_socket_handler(&exports)
}

/// Largest artifact upload (`POST /v1/services/{s}/versions`); the validator has smaller per-format caps.
pub const MAX_UPLOAD_BODY: usize = 64 * 1024 * 1024;
/// Uploads buffered at once on one node; one more answers `429 rate_limited`.
pub const MAX_CONCURRENT_UPLOADS: usize = 4;

/// What an in-flight upload holds until its handler returns: a slot of [`ApiState::uploads`] and the
/// memory it was charged.
pub(crate) struct UploadSlot {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _mem: rt_mem::Reservation,
}

/// Buffer an upload body. It takes one of [`MAX_CONCURRENT_UPLOADS`] slots and charges the governor
/// for its `Content-Length` (or [`MAX_UPLOAD_BODY`] when unknown) before the first byte is read, so
/// many large uploads at once can neither exhaust node memory nor go unaccounted (security review
/// finding 4).
pub(crate) async fn read_upload(
    s: &ApiState,
    tenant: TenantId,
    h: &HeaderMap,
    body: Body,
) -> Result<(Bytes, UploadSlot), ApiError> {
    let too_large = || ApiError::new("too_large", format!("uploads are limited to {MAX_UPLOAD_BODY} bytes"));
    let declared = h
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|n| n > MAX_UPLOAD_BODY as u64) {
        return Err(too_large());
    }
    let busy = || {
        ApiError::new(
            "rate_limited",
            "too many uploads in progress on this node; retry shortly",
        )
    };
    let permit = s.uploads.clone().try_acquire_owned().map_err(|_| busy())?;
    let charge = declared.unwrap_or(MAX_UPLOAD_BODY as u64);
    let owner = rt_mem::Owner {
        tenant: Some(tenant),
        ..rt_mem::Owner::NONE
    };
    let mem = s
        .mem
        .reserve(rt_mem::MemCategory::IoBuffers, charge, owner)
        .map_err(|_| busy())?;
    let bytes = axum::body::to_bytes(body, MAX_UPLOAD_BODY)
        .await
        .map_err(|_| too_large())?;
    Ok((
        bytes,
        UploadSlot {
            _permit: permit,
            _mem: mem,
        },
    ))
}

/// Raw body upload; `Content-Type` selects the format.
#[utoipa::path(post, path = "/v1/services/{s}/versions", tag = "versions",
    params(("s" = String, Path, description = "service id"), UploadQuery),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses((status = 201, body = VersionView), (status = 202, body = VersionView),
        (status = 413, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope), (status = 429, body = ErrorEnvelope)))]
async fn versions_upload(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<UploadQuery>,
    h: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::Deploy)?;
    let svc = load_service(&s, &a, &id).await?;
    let (body, _slot) = read_upload(&s, svc.tenant_id, &h, body).await?;
    let content_type = h
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let fp = fingerprint(&[
        b"versions",
        svc.id.to_string().as_bytes(),
        content_type.as_bytes(),
        q.activate.as_deref().unwrap_or("").as_bytes(),
        q.compile.as_deref().unwrap_or("").as_bytes(),
        q.close_sockets.as_deref().unwrap_or("").as_bytes(),
        &body,
    ]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    let compile = match q.compile.as_deref() {
        None => {
            let plan = s.plans.plan_for(svc.tenant_id);
            s.config
                .load()
                .service_effective(svc.profile, &svc.config, &plan)
                .compile
                .value
        }
        Some("lazy") => CompileMode::Lazy,
        Some("eager") => CompileMode::Eager,
        Some(other) => {
            return Err(ApiError::invalid(format!(
                "unknown compile mode `{other}` (lazy|eager)"
            )))
        }
    };
    let validated = s.validator.validate(svc.runtime, &content_type, &body);
    if let Ok(v) = &validated {
        if svc.runtime == RuntimeKind::Wasm && svc.config.durable {
            check_socket_handler(&v.exports)?;
        }
    }
    if let Err(e) = validated {
        return Err(match e {
            ValidationError::Invalid(d) => ApiError::new("validation_failed", "the upload failed validation")
                .with_details(json!({"diagnostics": d})),
            ValidationError::InvalidArtifact(m) => ApiError::new("invalid_artifact", m),
            ValidationError::TooLarge(m) => ApiError::new("too_large", m),
        });
    }
    s.quotas.check(svc.tenant_id, QuotaOp::Build)?;
    let digest = s.artifacts.put_typed(body, &content_type).await?;
    let mut version = s
        .meta
        .add_version(
            svc.id,
            NewVersion {
                source_digest: digest,
                compile,
            },
        )
        .await?;
    // Keep `deploy.retain_versions` versions (plus the live one) and delete the uploads no
    // version references any more, so `artifacts/` does not grow with every deploy.
    let keep = s.config.load().deploy.retain_versions.max(1);
    match s.meta.gc_versions(svc.id, keep).await {
        // Re-checked right before each delete, and anything written or touched within the grace
        // period stays (a concurrent upload of the same bytes, a build in flight); the node's
        // orphan sweep takes what is left later.
        Ok(freed) => {
            rt_store::artifact::delete_unreferenced(&*s.meta, &*s.artifacts, freed, rt_store::ARTIFACT_GRACE).await;
        }
        Err(e) => tracing::warn!(service = %svc.id, error = %e, "version GC failed"),
    }
    let eager = compile == CompileMode::Eager;
    if eager {
        s.meta
            .set_build_state(svc.id, version.number, BuildState::Building, None, None)
            .await?;
        version.build_state = BuildState::Building;
        s.node.notify(ControlEvent::BuildRequested {
            service: svc.id,
            version: version.number,
            digest,
            runtime: svc.runtime,
            rebuild: false,
        });
    }
    let activate = truthy(q.activate.as_deref());
    let close_sockets = activate && truthy(q.close_sockets.as_deref());
    if activate {
        s.meta.set_live(svc.id, version.number, None).await?;
        refresh_routes(&s).await?;
    }
    // Sockets survive a deploy (the next event runs the new version) unless asked otherwise.
    let closed = if close_sockets {
        s.sockets.close_service(svc.id, None, None, 1012, "service restart")
    } else {
        0
    };
    audit(
        &s,
        &a,
        "deploy",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"version": version.number.0, "digest": digest.to_string(), "activated": activate, "sockets_closed": closed}),
    );
    let status = if eager {
        StatusCode::ACCEPTED
    } else {
        StatusCode::CREATED
    };
    let view = serde_json::to_value(version_view(&version)).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, status, &view);
    Ok(value_resp(status, &view))
}

#[utoipa::path(get, path = "/v1/services/{s}/versions", tag = "versions",
    params(("s" = String, Path, description = "service id")), responses((status = 200, body = Vec<VersionView>)))]
async fn versions_list(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<Vec<VersionView>> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    Ok(Json(
        s.meta.list_versions(svc.id).await?.iter().map(version_view).collect(),
    ))
}

async fn find_version(s: &ApiState, a: &Auth, id: &str, v: &str) -> Result<(Service, Version), ApiError> {
    let svc = load_service(s, a, id).await?;
    let v = s
        .meta
        .get_version(svc.id, vno(v)?)
        .await?
        .ok_or_else(ApiError::not_found)?;
    Ok((svc, v))
}

#[utoipa::path(get, path = "/v1/services/{s}/versions/{v}", tag = "versions",
    params(("s" = String, Path, description = "service id"), ("v" = u32, Path, description = "version number")),
    responses((status = 200, body = VersionView)))]
async fn versions_get(State(s): State<ApiState>, a: Auth, Path((id, v)): Path<(String, String)>) -> R<VersionView> {
    a.require_scope(Scope::ServicesRead)?;
    let (_, v) = find_version(&s, &a, &id, &v).await?;
    Ok(Json(version_view(&v)))
}

#[utoipa::path(get, path = "/v1/services/{s}/versions/{v}/source", tag = "versions",
    params(("s" = String, Path, description = "service id"), ("v" = u32, Path, description = "version number")),
    responses((status = 200, description = "source artifact bytes", content_type = "application/octet-stream")))]
async fn versions_source(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, v)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesRead)?;
    let (_, v) = find_version(&s, &a, &id, &v).await?;
    let bytes = s.artifacts.get(&v.source_digest).await?;
    let mut resp = bytes.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(resp)
}

/// The RPC surface of a Deno service as TypeScript declarations (`21ci types`): read from the
/// source of the live version (or `?version=`), never run.
#[utoipa::path(get, path = "/v1/services/{s}/types", tag = "versions",
    params(("s" = String, Path, description = "service id"), TypesQuery),
    responses((status = 200, description = "the `.d.ts` of the service's `rpc` export", content_type = "text/plain"),
        (status = 422, body = ErrorEnvelope)))]
async fn services_types(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<TypesQuery>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    if svc.runtime != RuntimeKind::Deno {
        return Err(ApiError::new(
            "no_rpc_surface",
            "types are generated from the source of a Deno service; a WASM component declares its RPC in WIT",
        ));
    }
    let number = match q.version {
        Some(v) => VersionNo(v),
        None => svc
            .live_version
            .ok_or_else(|| ApiError::new("no_rpc_surface", "the service has no live version"))?,
    };
    let v = s
        .meta
        .get_version(svc.id, number)
        .await?
        .ok_or_else(ApiError::not_found)?;
    let body = s.artifacts.get(&v.source_digest).await?;
    let content_type = s
        .artifacts
        .content_type(&v.source_digest)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let target = rt_types::ContractTarget::Service { slug: svc.slug.clone() };
    let contract = s
        .validator
        .rpc_contract(&content_type, &body, &target)
        .map_err(|m| ApiError::new("no_rpc_surface", m))?;
    Ok(typescript_resp(contract.dts))
}

/// A `.d.ts` answer.
pub(crate) fn typescript_resp(text: String) -> Response {
    let mut resp = text.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp
}

/// Clears a failed build (negative cache) and rebuilds.
#[utoipa::path(post, path = "/v1/services/{s}/versions/{v}/rebuild", tag = "versions",
    params(("s" = String, Path, description = "service id"), ("v" = u32, Path, description = "version number")),
    responses((status = 202, body = VersionView)))]
async fn versions_rebuild(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, v)): Path<(String, String)>,
) -> Created<VersionView> {
    a.require_scope(Scope::Deploy)?;
    let (svc, mut v) = find_version(&s, &a, &id, &v).await?;
    // A rebuild is a build: it counts against the plan's `builds_per_hour` (CTRL-018).
    s.quotas.check(svc.tenant_id, QuotaOp::Build)?;
    s.meta
        .set_build_state(svc.id, v.number, BuildState::Building, None, None)
        .await?;
    v.build_state = BuildState::Building;
    v.failure = None;
    s.node.notify(ControlEvent::BuildRequested {
        service: svc.id,
        version: v.number,
        digest: v.source_digest,
        runtime: svc.runtime,
        rebuild: true,
    });
    audit(
        &s,
        &a,
        "rebuild",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"version": v.number.0}),
    );
    Ok((StatusCode::ACCEPTED, Json(version_view(&v))))
}

/// Stream wrapper that keeps an SSE slot until the client disconnects.
struct Guarded<S> {
    inner: S,
    _slot: SseSlot,
}

impl<S: Stream + Unpin> Stream for Guarded<S> {
    type Item = S::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

fn log_view(r: &LogRecord) -> LogView {
    LogView {
        seq: r.seq,
        ts: r.ts_ms,
        level: match r.level {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
        .to_string(),
        service_id: r.service_id.to_string(),
        version: r.version.0,
        task_id: r.task_id.map(|t| t.to_string()),
        trigger: r.trigger.clone(),
        kind: match r.kind {
            LogKind::Runtime => "runtime",
            LogKind::Build => "build",
            LogKind::System => "system",
        }
        .to_string(),
        msg: r.msg.clone(),
        truncated: r.truncated,
        fields: r.fields.clone(),
    }
}

fn log_event(r: &LogRecord) -> Event {
    Event::default()
        .id(r.seq.to_string())
        .event("log")
        .data(serde_json::to_string(&log_view(r)).unwrap_or_default())
}

fn sse_slot(s: &ApiState, a: &Auth) -> Result<SseSlot, ApiError> {
    let max = s.config.load().ingress.sse_streams_per_key.max(1) as usize;
    s.sse.acquire(&a.identity(), max).ok_or_else(|| {
        let mut e = ApiError::new("rate_limited", "too many concurrent streams for this key");
        e.retry_after = Some(1);
        e
    })
}

struct BuildTail {
    s: ApiState,
    service: ServiceId,
    version: VersionNo,
    cursor: Option<LogCursor>,
    buf: VecDeque<LogRecord>,
    done: bool,
}

/// Build log as Server-Sent Events.
#[utoipa::path(get, path = "/v1/services/{s}/versions/{v}/build/logs", tag = "versions",
    params(("s" = String, Path, description = "service id"), ("v" = u32, Path, description = "version number")),
    responses((status = 200, description = "SSE stream of build log records", content_type = "text/event-stream")))]
async fn versions_build_logs(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, v)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::LogsRead)?;
    let (svc, ver) = find_version(&s, &a, &id, &v).await?;
    let slot = sse_slot(&s, &a)?;
    let tail = BuildTail {
        s: s.clone(),
        service: svc.id,
        version: ver.number,
        cursor: None,
        buf: VecDeque::new(),
        done: false,
    };
    let stream = stream::unfold(tail, |mut t| async move {
        loop {
            if let Some(rec) = t.buf.pop_front() {
                return Some((Ok::<_, Infallible>(log_event(&rec)), t));
            }
            if t.done {
                return None;
            }
            let mut q = LogQuery::for_service(t.service);
            q.kind = Some(LogKind::Build);
            q.limit = 200;
            q.cursor = t.cursor;
            let page = t.s.logs.query(q);
            let before = t.buf.len();
            for r in page.records {
                t.cursor = Some(LogCursor(r.seq));
                if r.version == t.version {
                    t.buf.push_back(r);
                }
            }
            if t.buf.len() > before {
                continue;
            }
            let finished = matches!(
                t.s.meta.get_version(t.service, t.version).await,
                Ok(Some(v)) if matches!(v.build_state, BuildState::Ready | BuildState::Failed)
            ) || matches!(t.s.meta.get_version(t.service, t.version).await, Ok(None) | Err(_));
            if finished {
                t.done = true;
                continue;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    Ok(Sse::new(Guarded {
        inner: stream.boxed(),
        _slot: slot,
    })
    .keep_alive(KeepAlive::default())
    .into_response())
}

/// `409 version_not_ready` for a failed version; `pending` is accepted (API-10).
#[utoipa::path(put, path = "/v1/services/{s}/live", tag = "services", request_body = SetLiveBody,
    params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = ServiceView), (status = 409, body = ErrorEnvelope), (status = 412, body = ErrorEnvelope)))]
async fn services_set_live(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
    ApiJson(b): ApiJson<SetLiveBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::Deploy)?;
    let svc = load_service(&s, &a, &id).await?;
    let target = VersionNo(b.version);
    if svc.config.durable && svc.runtime == RuntimeKind::Wasm {
        check_version_socket_handler(&s, &svc, target).await?;
    }
    let updated = s.meta.set_live(svc.id, target, if_match(&h)?).await?;
    refresh_routes(&s).await?;
    let closed = if b.close_sockets {
        s.sockets.close_service(svc.id, None, None, 1012, "service restart")
    } else {
        0
    };
    let rollback = svc.live_version.is_some_and(|prev| target < prev);
    audit(
        &s,
        &a,
        if rollback { "rollback" } else { "live.changed" },
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"from": svc.live_version.map(|v| v.0), "to": target.0, "sockets_closed": closed}),
    );
    Ok(json_resp(StatusCode::OK, &service_view(&s, &updated).await?))
}

// ---- env, secrets, hosts ---------------------------------------------------------------------

#[utoipa::path(get, path = "/v1/services/{s}/env", tag = "env", params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = EnvMap)))]
async fn env_get(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<EnvMap> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    Ok(Json(EnvMap(
        s.meta.get_env(svc.id).await?.into_iter().collect::<BTreeMap<_, _>>(),
    )))
}

/// Replaces the whole map.
#[utoipa::path(put, path = "/v1/services/{s}/env", tag = "env", request_body = EnvMap,
    params(("s" = String, Path, description = "service id")), responses((status = 200, body = EnvMap)))]
async fn env_put(State(s): State<ApiState>, a: Auth, Path(id): Path<String>, ApiJson(b): ApiJson<EnvMap>) -> R<EnvMap> {
    a.require_scope(Scope::EnvWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    s.meta.put_env(svc.id, b.0.clone().into_iter().collect()).await?;
    s.node.notify(ControlEvent::SecretsChanged(svc.id));
    audit(
        &s,
        &a,
        "env.written",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"names": b.0.keys().collect::<Vec<_>>()}),
    );
    Ok(Json(b))
}

/// Names and update times only; values are write-only.
#[utoipa::path(get, path = "/v1/services/{s}/secrets", tag = "secrets", params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = Vec<SecretNameView>)))]
async fn secrets_list(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<Vec<SecretNameView>> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    Ok(Json(
        s.meta
            .list_secrets(svc.id)
            .await?
            .into_iter()
            .map(|m| SecretNameView {
                name: m.name,
                updated_at: m.updated_at_ms,
            })
            .collect(),
    ))
}

#[utoipa::path(put, path = "/v1/services/{s}/secrets/{name}", tag = "secrets", request_body = PutSecretBody,
    params(("s" = String, Path, description = "service id"), ("name" = String, Path, description = "secret name")),
    responses((status = 204)))]
async fn secrets_put(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name)): Path<(String, String)>,
    ApiJson(b): ApiJson<PutSecretBody>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::SecretsWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    if !valid_secret_name(&name) {
        return Err(ApiError::invalid(
            "secret names match [A-Z_][A-Z0-9_]* and must not start with RT_",
        ));
    }
    if b.value.len() > 32 * 1024 {
        return Err(ApiError::invalid("secret values are limited to 32 KB"));
    }
    let sealed = s
        .secrets
        .seal(&secret_aad(svc.tenant_id, svc.id, &name), b.value.as_bytes());
    s.meta.put_secret(svc.id, &name, sealed.to_bytes()).await?;
    s.node.notify(ControlEvent::SecretsChanged(svc.id));
    audit(
        &s,
        &a,
        "secret.written",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"name": name}),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(delete, path = "/v1/services/{s}/secrets/{name}", tag = "secrets",
    params(("s" = String, Path, description = "service id"), ("name" = String, Path, description = "secret name")),
    responses((status = 204)))]
async fn secrets_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::SecretsWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    s.meta.delete_secret(svc.id, &name).await?;
    s.node.notify(ControlEvent::SecretsChanged(svc.id));
    audit(
        &s,
        &a,
        "secret.deleted",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"name": name}),
    );
    Ok(StatusCode::NO_CONTENT)
}

fn host_view(h: &HostMapping, pending_ttl: Duration) -> HostView {
    let pending = h.status == HostStatus::PendingVerification;
    HostView {
        host: h.host.clone(),
        status: if pending { "pending_verification" } else { "active" }.to_string(),
        verify_token: h.verify_token.clone(),
        created_at: h.created_at_ms,
        expires_at: pending.then(|| crate::hosts::expires_at_ms(h, pending_ttl)),
    }
}

#[utoipa::path(get, path = "/v1/services/{s}/hosts", tag = "hosts", params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = Vec<HostView>)))]
async fn hosts_list(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<Vec<HostView>> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let ttl = s.config.load().hosts.pending_ttl;
    Ok(Json(
        s.meta
            .list_hosts(svc.id)
            .await?
            .iter()
            .map(|h| host_view(h, ttl))
            .collect(),
    ))
}

/// One custom host of the service (status, verify token, expiry); `404` when the host is not
/// mapped to this service.
#[utoipa::path(get, path = "/v1/services/{s}/hosts/{host}", tag = "hosts",
    params(("s" = String, Path, description = "service id"), ("host" = String, Path, description = "custom host")),
    responses((status = 200, body = HostView), (status = 404, body = ErrorEnvelope)))]
async fn hosts_get(State(s): State<ApiState>, a: Auth, Path((id, host)): Path<(String, String)>) -> R<HostView> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    let ttl = s.config.load().hosts.pending_ttl;
    let h = s
        .meta
        .find_host(&host)
        .await?
        .filter(|h| h.service_id == svc.id)
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(host_view(&h, ttl)))
}

#[utoipa::path(put, path = "/v1/services/{s}/hosts/{host}", tag = "hosts",
    params(("s" = String, Path, description = "service id"), ("host" = String, Path, description = "custom host")),
    responses((status = 200, body = HostView), (status = 403, body = ErrorEnvelope), (status = 409, body = ErrorEnvelope)))]
async fn hosts_put(State(s): State<ApiState>, a: Auth, Path((id, host)): Path<(String, String)>) -> R<HostView> {
    a.require_scope(Scope::ServicesWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if !dns_host_name(&host) {
        return Err(ApiError::invalid("`host` is not a valid host name"));
    }
    let cfg = s.config.load();
    let base = cfg.routing.base_domain.trim_matches('.').to_ascii_lowercase();
    let reserved = cfg
        .security
        .reserved_hosts
        .iter()
        .any(|r| r.eq_ignore_ascii_case(&host));
    let under_base = !base.is_empty() && (host == base || host.ends_with(&format!(".{base}")));
    if reserved || under_base {
        return Err(ApiError::new("host_not_allowed", "this host can never be claimed"));
    }
    let operator = matches!(a.principal, Principal::Operator);
    if !operator {
        let plan = s.plans.plan_for(svc.tenant_id);
        let allowed = plan
            .custom_host_suffixes
            .iter()
            .any(|suf| host_under_suffix(&host, suf));
        if !allowed {
            return Err(ApiError::new("host_not_allowed", "the plan does not allow this host"));
        }
    }
    // An expired pending claim (`hosts.pending_ttl`) no longer blocks anyone. The maintenance job
    // deletes expired claims too; this covers the gap. Only `pending_verification` rows older than
    // the TTL are deleted (an approval that lands meanwhile is kept).
    let ttl = cfg.hosts.pending_ttl;
    let now = s.clock.unix_ms();
    let mut current = s.meta.find_host(&host).await?;
    if current.as_ref().is_some_and(|c| crate::hosts::expired(c, ttl, now)) {
        crate::hosts::expire_claims(s.meta.as_ref(), ttl, now).await?;
        current = s.meta.find_host(&host).await?;
    }
    // Operator approval: the operator's mapping is active immediately. A tenant's claim waits for
    // DNS TXT verification (`_rt-verify.<host>` = token) or operator approval. Re-claiming one's
    // own pending host keeps its token and its TTL, so a published TXT record stays valid; so does
    // claiming it again after a DELETE or an expiry (`host_releases`), which therefore never
    // restarts the TTL: once the claim's TTL is over, the tenant waits one more TTL.
    let own_pending = current
        .as_ref()
        .filter(|c| c.service_id == svc.id && c.status == HostStatus::PendingVerification)
        .cloned();
    let (status, verify_token, created_at_ms) = if operator {
        (HostStatus::Active, None, 0)
    } else if let Some(c) = own_pending {
        (
            HostStatus::PendingVerification,
            c.verify_token.or_else(|| Some(rt_sec::generate_secret())),
            c.created_at_ms,
        )
    } else {
        let max = u64::from(cfg.hosts.max_pending_per_tenant);
        if current.is_none() && s.meta.count_pending_hosts(svc.tenant_id).await? >= max {
            return Err(ApiError::new(
                "quota_exceeded",
                format!("at most {max} pending host claims per tenant"),
            )
            .with_details(json!({"max_pending_per_tenant": max})));
        }
        match s.meta.host_release(&host, svc.tenant_id).await? {
            Some(r) if crate::hosts::still_cooling(&r, ttl, now) => {
                if crate::hosts::expires_at(r.created_at_ms, ttl) > now {
                    let token = r.verify_token.or_else(|| Some(rt_sec::generate_secret()));
                    (HostStatus::PendingVerification, token, r.created_at_ms)
                } else {
                    let retry_at = crate::hosts::expires_at(r.released_at_ms, ttl);
                    return Err(ApiError::new(
                        "host_cooldown",
                        "this tenant's claim of the host expired unverified; it can be claimed again later",
                    )
                    .with_details(json!({"retry_at": retry_at})));
                }
            }
            _ => (HostStatus::PendingVerification, Some(rt_sec::generate_secret()), 0),
        }
    };
    let mapping = HostMapping {
        host,
        service_id: svc.id,
        status,
        verify_token,
        created_at_ms,
    };
    s.meta.put_host(mapping.clone()).await?;
    // What is stored now (a re-claimed active mapping stays active and keeps no token).
    let mapping = s.meta.find_host(&mapping.host).await?.unwrap_or(mapping);
    refresh_routes(&s).await?;
    audit(
        &s,
        &a,
        "host.attached",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"host": mapping.host}),
    );
    Ok(Json(host_view(&mapping, ttl)))
}

/// `host` is a proper subdomain of the plan suffix `suf`, on a label boundary: `example.com`
/// (or `.example.com`) admits `api.example.com` but neither `example.com` nor `evilexample.com`.
fn host_under_suffix(host: &str, suf: &str) -> bool {
    let suf = suf.trim().trim_matches('.').to_ascii_lowercase();
    !suf.is_empty() && host.len() > suf.len() + 1 && host.ends_with(&format!(".{suf}"))
}

#[utoipa::path(delete, path = "/v1/services/{s}/hosts/{host}", tag = "hosts",
    params(("s" = String, Path, description = "service id"), ("host" = String, Path, description = "custom host")),
    responses((status = 204)))]
async fn hosts_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, host)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    let before = s.meta.find_host(&host).await?.filter(|h| h.service_id == svc.id);
    if !s.meta.delete_host(svc.id, &host).await? {
        return Err(ApiError::not_found());
    }
    // A deleted pending claim is remembered: claiming the host again continues it (same token,
    // same expiry) instead of restarting the verification TTL.
    if let Some(h) = before.filter(|h| h.status == HostStatus::PendingVerification) {
        crate::hosts::remember_release(s.meta.as_ref(), &h, svc.tenant_id, s.clock.unix_ms()).await?;
    }
    refresh_routes(&s).await?;
    audit(
        &s,
        &a,
        "host.detached",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"host": host}),
    );
    Ok(StatusCode::NO_CONTENT)
}

// ---- schedules, queue ------------------------------------------------------------------------

#[utoipa::path(get, path = "/v1/services/{s}/schedules", tag = "schedules", params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = Vec<ScheduleView>)))]
async fn schedules_list(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<Vec<ScheduleView>> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let now = s.clock.unix_ms();
    Ok(Json(
        s.meta
            .list_schedules(svc.id)
            .await?
            .iter()
            .map(|x| schedule_view(x, now))
            .collect(),
    ))
}

#[utoipa::path(get, path = "/v1/services/{s}/schedules/once", tag = "schedules",
    params(("s" = String, Path, description = "service id"), PageQuery),
    responses((status = 200, body = OncePage), (status = 404, body = ErrorEnvelope)))]
async fn schedules_once_list(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<PageQuery>,
) -> R<OncePage> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let req = page_req(q.limit, q.cursor);
    let limit = req.limit as usize;
    let rows = s.delayed.list(svc.id, req.cursor.as_deref(), limit).await?;
    // a full page may be followed by more rows: its last id is the cursor of the next one
    let next_cursor = if rows.len() == limit {
        rows.last().map(|e| e.id.clone())
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(|e| OnceScheduleView {
            id: e.id,
            kind: "once".into(),
            run_at: e.run_at_ms,
        })
        .collect();
    Ok(Json(OncePage { items, next_cursor }))
}

fn schedule_kind(b: &ScheduleBody, existing: Option<&ScheduleKind>) -> Result<ScheduleKind, ApiError> {
    match b.kind.as_str() {
        "interval" => {
            let every = b.every_seconds.or(match existing {
                Some(ScheduleKind::Interval { every_seconds }) => Some(*every_seconds),
                _ => None,
            });
            Ok(ScheduleKind::Interval {
                every_seconds: every
                    .ok_or_else(|| ApiError::invalid("`every_seconds` is required for interval schedules"))?,
            })
        }
        "cron" => {
            let (old_expr, old_tz) = match existing {
                Some(ScheduleKind::Cron { expr, tz }) => (Some(expr.clone()), Some(tz.clone())),
                _ => (None, None),
            };
            let expr = b
                .cron
                .clone()
                .or(old_expr)
                .ok_or_else(|| ApiError::invalid("`cron` is required for cron schedules"))?;
            let tz = b.timezone.clone().or(old_tz).unwrap_or_else(|| "UTC".to_string());
            Ok(ScheduleKind::Cron { expr, tz })
        }
        other => Err(ApiError::invalid(format!(
            "unknown schedule kind `{other}` (interval|cron)"
        ))),
    }
}

/// A schedule of `kind` needs its trigger switched on for the service and allowed by the plan.
fn check_schedule_trigger(s: &ApiState, svc: &Service, kind: &ScheduleKind) -> Result<(), ApiError> {
    let trigger = match kind {
        ScheduleKind::Interval { .. } => TriggerKind::Interval,
        ScheduleKind::Cron { .. } => TriggerKind::Cron,
    };
    if !svc.config.triggers.enabled(trigger) {
        return Err(ApiError::new(
            "trigger_not_allowed",
            format!("the `{}` trigger is disabled on this service", trigger.as_str()),
        ));
    }
    s.quotas.check(svc.tenant_id, QuotaOp::UseTrigger(trigger))?;
    Ok(())
}

/// Estimated seconds between two fires (for the `min_schedule_seconds` check).
fn period_seconds(kind: &ScheduleKind, now_ms: i64) -> u64 {
    match kind {
        ScheduleKind::Interval { every_seconds } => *every_seconds,
        // The shortest gap between consecutive fires over the coming year (at most 2000 fires):
        // the first gap alone depends on the time of day (QUOTA-005).
        ScheduleKind::Cron { .. } => {
            const HORIZON_MS: i64 = 400 * 86_400_000;
            let Ok(Some(mut prev)) = rt_triggers::next_fire(kind, now_ms) else {
                return u64::MAX;
            };
            let mut shortest = u64::MAX;
            for _ in 0..2000 {
                let Ok(Some(next)) = rt_triggers::next_fire(kind, prev) else {
                    break;
                };
                shortest = shortest.min(u64::try_from((next - prev) / 1000).unwrap_or(u64::MAX));
                if shortest <= 1 || next - now_ms > HORIZON_MS {
                    break;
                }
                prev = next;
            }
            shortest
        }
    }
}

#[utoipa::path(post, path = "/v1/services/{s}/schedules", tag = "schedules", request_body = ScheduleBody,
    params(("s" = String, Path, description = "service id")),
    responses((status = 201, body = ScheduleView), (status = 403, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
async fn schedules_create(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
    ApiJson(b): ApiJson<ScheduleBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let fp = fingerprint(&[b"schedules", svc.id.to_string().as_bytes(), format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    let kind = schedule_kind(&b, None)?;
    rt_triggers::validate_kind(&kind).map_err(|e| ApiError::invalid(e.to_string()))?;
    check_schedule_trigger(&s, &svc, &kind)?;
    let now = s.clock.unix_ms();
    let current = s.meta.list_schedules(svc.id).await?.len() as u64;
    s.quotas.check(
        svc.tenant_id,
        QuotaOp::CreateSchedule {
            current,
            every_seconds: period_seconds(&kind, now),
        },
    )?;
    let payload = b.payload.filter(|p| !p.is_null()).unwrap_or_else(|| json!({}));
    let sch = s
        .meta
        .create_schedule(
            svc.id,
            NewSchedule {
                kind,
                payload,
                enabled: b.enabled.unwrap_or(true),
            },
        )
        .await?;
    s.node.notify(ControlEvent::ScheduleUpserted(sch.clone()));
    audit(
        &s,
        &a,
        "schedule.created",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"schedule": sch.id.to_string()}),
    );
    let view = serde_json::to_value(schedule_view(&sch, now)).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::CREATED, &view);
    Ok(value_resp(StatusCode::CREATED, &view))
}

#[utoipa::path(patch, path = "/v1/services/{s}/schedules/{id}", tag = "schedules", request_body = ScheduleBody,
    params(("s" = String, Path, description = "service id"), ("id" = String, Path, description = "schedule id")),
    responses((status = 200, body = ScheduleView)))]
async fn schedules_patch(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, sch)): Path<(String, String)>,
    ApiJson(b): ApiJson<ScheduleBody>,
) -> R<ScheduleView> {
    a.require_scope(Scope::ServicesWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let sch_id: ScheduleId = sch.parse().map_err(|_| ApiError::not_found())?;
    let existing = s
        .meta
        .list_schedules(svc.id)
        .await?
        .into_iter()
        .find(|x| x.id == sch_id)
        .ok_or_else(ApiError::not_found)?;
    let kind = schedule_kind(&b, Some(&existing.kind))?;
    rt_triggers::validate_kind(&kind).map_err(|e| ApiError::invalid(e.to_string()))?;
    // The same trigger rules as a create: the service's switch and the plan's allowed_triggers
    // (a free plan has no `cron`, so an interval schedule cannot be patched into one).
    check_schedule_trigger(&s, &svc, &kind)?;
    let now = s.clock.unix_ms();
    let others = s.meta.list_schedules(svc.id).await?.len().saturating_sub(1) as u64;
    s.quotas.check(
        svc.tenant_id,
        QuotaOp::CreateSchedule {
            current: others,
            every_seconds: period_seconds(&kind, now),
        },
    )?;
    let new = NewSchedule {
        kind,
        payload: b.payload.filter(|p| !p.is_null()).unwrap_or(existing.payload.clone()),
        enabled: b.enabled.unwrap_or(existing.enabled),
    };
    let updated = s.meta.update_schedule(svc.id, sch_id, new).await?;
    s.node.notify(ControlEvent::ScheduleUpserted(updated.clone()));
    audit(
        &s,
        &a,
        "schedule.updated",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"schedule": sch_id.to_string()}),
    );
    Ok(Json(schedule_view(&updated, now)))
}

#[utoipa::path(delete, path = "/v1/services/{s}/schedules/{id}", tag = "schedules",
    params(("s" = String, Path, description = "service id"), ("id" = String, Path, description = "schedule id")),
    responses((status = 204)))]
async fn schedules_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, sch)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let sch_id: ScheduleId = sch.parse().map_err(|_| ApiError::not_found())?;
    if !s.meta.delete_schedule(svc.id, sch_id).await? {
        return Err(ApiError::not_found());
    }
    s.node.notify(ControlEvent::ScheduleRemoved(sch_id));
    audit(
        &s,
        &a,
        "schedule.deleted",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"schedule": sch_id.to_string()}),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/v1/services/{s}/queue", tag = "queue", request_body = EnqueueBody,
    params(("s" = String, Path, description = "service id")),
    responses((status = 202, body = Enqueued), (status = 429, body = ErrorEnvelope)))]
async fn queue_enqueue(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
    ApiJson(b): ApiJson<EnqueueBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::QueueWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let fp = fingerprint(&[b"queue", svc.id.to_string().as_bytes(), format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    if !svc.config.triggers.queue {
        return Err(ApiError::new(
            "trigger_not_allowed",
            "the `queue` trigger is disabled on this service",
        ));
    }
    if !s.plans.plan_arc(svc.tenant_id).allows_trigger(TriggerKind::Queue) {
        return Err(ApiError::new(
            "trigger_not_allowed",
            "the tenant's plan does not allow the `queue` trigger",
        ));
    }
    let delay = Duration::from_secs(b.delay_seconds.unwrap_or(0));
    if delay > rt_store::queue::MAX_DELAY {
        return Err(ApiError::invalid("`delay_seconds` is limited to 7 days"));
    }
    let bytes = serde_json::to_vec(&b.body).map_err(|e| ApiError::invalid(e.to_string()))?;
    if bytes.len() > rt_store::queue::MAX_MESSAGE_BYTES {
        return Err(ApiError::new("too_large", "queue messages are limited to 1 MB"));
    }
    let msg = rt_store::NewMessage {
        body: Bytes::from(bytes),
        delay,
        dedup_id: b.dedup_id.filter(|d| !d.is_empty()),
    };
    // Counted in the inserting transaction (CALL-011): `429 queue_full` at `max_queue_depth`.
    let max_depth = s.plans.plan_arc(svc.tenant_id).max_queue_depth;
    let id = s
        .queue
        .enqueue_capped(svc.id, msg, max_depth)
        .await
        .map_err(|e| match e {
            rt_store::StoreError::Conflict("queue_full") => ApiError::from(rt_sec::QuotaError::QueueFull),
            e => e.into(),
        })?;
    s.node.notify(ControlEvent::QueueEnqueued(svc.id));
    let view = json!({"id": id.to_string()});
    idem_store(&s, &a, &h, fp, StatusCode::ACCEPTED, &view);
    Ok(value_resp(StatusCode::ACCEPTED, &view))
}

#[utoipa::path(get, path = "/v1/services/{s}/queue", tag = "queue", params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = QueueStatsView)))]
async fn queue_stats(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<QueueStatsView> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let st = s.queue.stats(svc.id).await?;
    Ok(Json(QueueStatsView {
        depth: st.depth,
        in_flight: st.in_flight,
        oldest_age_ms: st.oldest_age_ms,
        dlq: st.dlq,
    }))
}

#[utoipa::path(get, path = "/v1/services/{s}/dlq", tag = "queue", params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = Object)))]
async fn dlq_list(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<Value> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let items: Vec<Value> = s
        .queue
        .dlq_list(svc.id, 100)
        .await?
        .into_iter()
        .map(|e| {
            let body = serde_json::from_slice::<Value>(&e.body)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&e.body).into_owned()));
            json!({"id": e.id.to_string(), "body": body, "attempts": e.attempts, "failed_at": e.failed_at_ms})
        })
        .collect();
    Ok(Json(json!({"items": items})))
}

#[utoipa::path(post, path = "/v1/services/{s}/dlq/redrive", tag = "queue", params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = Redriven)))]
async fn dlq_redrive(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<Redriven> {
    a.require_scope(Scope::QueueWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let n = s.queue.dlq_redrive(svc.id).await?;
    if n > 0 {
        s.node.notify(ControlEvent::QueueEnqueued(svc.id));
    }
    audit(
        &s,
        &a,
        "dlq.redrive",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"redriven": n}),
    );
    Ok(Json(Redriven { redriven: n }))
}

#[utoipa::path(delete, path = "/v1/services/{s}/dlq", tag = "queue", params(("s" = String, Path, description = "service id")),
    responses((status = 204)))]
async fn dlq_purge(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::QueueWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    s.queue.dlq_purge(svc.id).await?;
    audit(&s, &a, "dlq.purge", Some(svc.tenant_id), Some(svc.id), json!({}));
    Ok(StatusCode::NO_CONTENT)
}

// ---- logs, stats, events, data ---------------------------------------------------------------

/// JSON page, or Server-Sent Events when `follow` is truthy (`Last-Event-ID` resumes, OBS-02).
#[utoipa::path(get, path = "/v1/services/{s}/logs", tag = "observability",
    params(("s" = String, Path, description = "service id"), LogsQuery),
    responses((status = 200, body = LogsPage), (status = 200, description = "SSE when follow=1", content_type = "text/event-stream")))]
async fn logs_get(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<LogsQuery>,
    h: HeaderMap,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::LogsRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let level = match q.level.as_deref() {
        None => None,
        Some("debug") => Some(LogLevel::Debug),
        Some("info") => Some(LogLevel::Info),
        Some("warn") => Some(LogLevel::Warn),
        Some("error") => Some(LogLevel::Error),
        Some(o) => return Err(ApiError::invalid(format!("unknown level `{o}`"))),
    };
    let kind = match q.kind.as_deref() {
        None => None,
        Some("runtime") => Some(LogKind::Runtime),
        Some("build") => Some(LogKind::Build),
        Some("system") => Some(LogKind::System),
        Some(o) => return Err(ApiError::invalid(format!("unknown kind `{o}`"))),
    };
    let task_id = q
        .task_id
        .as_deref()
        .map(|t| TaskId::from_str(t).map_err(|_| ApiError::invalid("malformed task_id")))
        .transpose()?;
    let cursor = match &q.cursor {
        Some(c) => Some(LogCursor(
            c.parse::<u64>().map_err(|_| ApiError::invalid("malformed cursor"))?,
        )),
        None => None,
    };
    if truthy(q.follow.as_deref()) {
        let slot = sse_slot(&s, &a)?;
        let from = h
            .get("last-event-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(LogCursor)
            .or(cursor);
        let stream = s
            .logs
            .tail(svc.id, from)
            .filter(move |r| {
                let ok = level.is_none_or(|l| r.level >= l)
                    && kind.is_none_or(|k| r.kind == k)
                    && task_id.is_none_or(|t| r.task_id == Some(t));
                futures_util::future::ready(ok)
            })
            .map(|r| Ok::<_, Infallible>(log_event(&r)))
            .boxed();
        return Ok(Sse::new(Guarded {
            inner: stream,
            _slot: slot,
        })
        .keep_alive(KeepAlive::default())
        .into_response());
    }
    let mut query = LogQuery::for_service(svc.id);
    query.since_ms = q.since.as_deref().map(rfc3339_ms).transpose()?;
    query.until_ms = q.until.as_deref().map(rfc3339_ms).transpose()?;
    query.level = level;
    query.task_id = task_id;
    query.kind = kind;
    query.limit = q.limit.unwrap_or(100).clamp(1, 1000) as usize;
    query.cursor = cursor;
    let page = s.logs.query(query);
    let body = LogsPage {
        items: page.records.iter().map(log_view).collect(),
        next_cursor: page.next_cursor.map(|c| c.0.to_string()),
    };
    Ok(Json(body).into_response())
}

/// Request counts, error rate and phase percentiles of a window, plus `admission`: the service's
/// queueing and autoscaler state (`AdmissionView`, `null` while it has none).
#[utoipa::path(get, path = "/v1/services/{s}/stats", tag = "observability",
    params(("s" = String, Path, description = "service id"), StatsQuery), responses((status = 200, body = Object)))]
async fn stats_get(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<StatsQuery>,
) -> R<Value> {
    a.require_scope(Scope::LogsRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let window = match q.window.as_deref() {
        None => Window::M5,
        Some(w) => Window::parse(w).ok_or_else(|| ApiError::invalid("`window` must be 5m, 1h or 24h"))?,
    };
    let mut body = serde_json::to_value(s.stats.window(svc.id, window)).unwrap_or(Value::Null);
    if let Some(o) = body.as_object_mut() {
        o.insert(
            "admission".into(),
            serde_json::to_value(admission_view(&s, svc.id)).unwrap_or(Value::Null),
        );
        // Open WebSockets of the service and the objects holding them.
        let ws = s.sockets.service_stats(svc.id);
        o.insert("sockets".into(), json!({"open": ws.sockets, "objects": ws.objects}));
    }
    Ok(Json(body))
}

#[utoipa::path(get, path = "/v1/services/{s}/events", tag = "observability",
    params(("s" = String, Path, description = "service id"), PageQuery), responses((status = 200, body = EventsPage)))]
async fn events_get(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<PageQuery>,
) -> R<EventsPage> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let page = s.meta.list_events(svc.id, page_req(q.limit, q.cursor)).await?;
    Ok(Json(EventsPage {
        items: page
            .items
            .into_iter()
            .map(|e| EventView {
                id: e.id.to_string(),
                ts: e.ts_ms,
                kind: e.kind,
                data: if e.data.is_null() { json!({}) } else { e.data },
            })
            .collect(),
        next_cursor: page.next_cursor,
    }))
}

/// Consistent SQLite snapshot of the service's `data.db` (scope `data:admin`, API-11).
#[utoipa::path(get, path = "/v1/services/{s}/data/export", tag = "services", params(("s" = String, Path, description = "service id")),
    responses((status = 200, description = "SQLite database file", content_type = "application/vnd.sqlite3")))]
async fn data_export(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> Result<Response, ApiError> {
    a.require_scope(Scope::DataAdmin)?;
    let svc = load_service(&s, &a, &id).await?;
    let bytes = s
        .data
        .sql(svc.id)
        .export()
        .await
        .map_err(|e: StoreError| ApiError::from(e))?;
    audit(
        &s,
        &a,
        "data.exported",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"bytes": bytes.len()}),
    );
    let mut resp = bytes.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.sqlite3"),
    );
    Ok(resp)
}

// =============================================================================================
// OpenAPI + router
// =============================================================================================

/// The OpenAPI 3.1 document served at `GET /v1/openapi.json` and rendered by Scalar at `/docs`.
/// CI fails when `openapi/openapi.json` drifts from this (API-01).
#[derive(OpenApi)]
#[openapi(
    modifiers(&CommonErrors),
    info(title = "21 Cloud Functions Runtime — control API", version = "1.0.0",
        description = "Self-hosted functions runtime: management API."),
    paths(
        node_status, node_config_get, node_config_put, node_drain, node_scheduler, node_cache,
        version_get, me_get, plans_list, crate::limits::limits_get,
        tenants_create, tenants_list, tenants_get, tenants_patch, tenants_delete, tenants_usage,
        keys_create, keys_list, keys_delete,
        crate::tokens::tokens_create, crate::tokens::tokens_list, crate::tokens::tokens_delete,
        crate::projects::projects_create, crate::projects::projects_list, crate::projects::projects_get,
        crate::projects::projects_patch, crate::projects::projects_delete, crate::projects::environments_get,
        services_create, services_list, services_get, services_patch, services_delete,
        versions_upload, versions_list, versions_get, versions_source, versions_rebuild, versions_build_logs, services_types,
        services_set_live,
        env_get, env_put, secrets_list, secrets_put, secrets_delete,
        hosts_list, hosts_get, hosts_put, hosts_delete,
        schedules_list, schedules_once_list, schedules_create, schedules_patch, schedules_delete,
        queue_enqueue, queue_stats, dlq_list, dlq_redrive, dlq_purge,
        logs_get, stats_get, events_get, data_export,
        crate::sdks::sdks_publish, crate::sdks::sdks_list, crate::sdks::sdks_releases, crate::sdks::sdks_set_channel,
        crate::sdks::sdks_remove, crate::sdks::grants_list, crate::sdks::grants_put, crate::sdks::sdks_effective,
        crate::sdks::sdks_contract,
        crate::sockets::objects_list, crate::sockets::sockets_list, crate::sockets::socket_close, crate::sockets::sockets_close,
        crate::sockets::object_delete,
    ),
    components(schemas(
        crate::sockets::ObjectView, crate::sockets::ObjectPage, crate::sockets::SocketView, crate::sockets::SocketPage,
        crate::sockets::ClosedView, NodeSockets,
        crate::sdks::PublishSdkBody, crate::sdks::SdkReleaseView, crate::sdks::SdkChannelView, crate::sdks::SdkView,
        crate::sdks::SetChannelBody, crate::sdks::SdkGrantBody, crate::sdks::SdkGrantView, crate::sdks::EffectiveSdkView,
        ErrorEnvelope, ErrorBody, NodeConfigUpdate, NodeStatus, NodeConfigApplied, VersionInfo, PlanView, MeView,
        CreateTenantBody, PatchTenantBody, TenantView, TenantPage, UsageView,
        CreateKeyBody, CreateTokenBody, KeyView, KeyCreated,
        CreateProjectBody, PatchProjectBody, ProjectView, ProjectPage, EnvironmentsView, LimitsView, QuotaView,
        CreateServiceBody, PatchServiceBody, ServiceStatus, AdmissionView, ServiceView, ServicePage,
        VersionView, SetLiveBody, EnvMap, PutSecretBody, SecretNameView, HostView,
        ScheduleBody, ScheduleView, OnceScheduleView, OncePage, EnqueueBody, Enqueued, QueueStatsView, Redriven,
        LogView, LogsPage, EventView, EventsPage,
    )),
    tags(
        (name = "meta", description = "Who am I, versions"),
        (name = "node", description = "Node status, config, drain, scheduler, cache (operator)"),
        (name = "tenants", description = "Tenants (operator)"),
        (name = "keys", description = "API keys; self-service tokens of a tenant (scope keys:write)"),
        (name = "projects", description = "Projects and their environments"),
        (name = "services", description = "Services"),
        (name = "versions", description = "Immutable versions and builds"),
        (name = "env", description = "Environment variables"),
        (name = "secrets", description = "Write-only secrets"),
        (name = "hosts", description = "Custom hosts"),
        (name = "schedules", description = "Interval and cron schedules; the pending one-off schedules of std:schedule"),
        (name = "queue", description = "Queue and dead-letter queue"),
        (name = "sdks", description = "Operator SDKs: releases, channels and grants (operator); the effective SDKs and contracts of a tenant"),
        (name = "observability", description = "Logs, stats, events"),
    )
)]
pub struct ApiDoc;

/// The complete OpenAPI document: [`ApiDoc`] merged with the event modules' own documents
/// (`topics`, `consumers`, `invocations`). Served, rendered and checked against `openapi/openapi.json`.
pub fn api_doc() -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    doc.merge(crate::topics::Doc::openapi());
    doc.merge(crate::consumers::Doc::openapi());
    doc.merge(crate::invocations::Doc::openapi());
    doc
}

/// Errors any authenticated route can answer, documented on every operation (OAPI-003): a missing
/// or bad credential (401), a missing scope or plan limit (403), the failed-auth or rate limiter
/// (429), and an unknown or foreign id (404) on paths that name one.
struct CommonErrors;

impl utoipa::Modify for CommonErrors {
    fn modify(&self, api: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::{ContentBuilder, Ref, ResponseBuilder};
        let error = |what: &str| {
            ResponseBuilder::new()
                .description(what)
                .content(
                    "application/json",
                    ContentBuilder::new()
                        .schema(Some(Ref::from_schema_name("ErrorEnvelope")))
                        .build(),
                )
                .build()
        };
        for (path, item) in api.paths.paths.iter_mut() {
            let named = path.contains('{');
            for op in [
                &mut item.get,
                &mut item.put,
                &mut item.post,
                &mut item.delete,
                &mut item.patch,
            ]
            .into_iter()
            .flatten()
            {
                let r = &mut op.responses.responses;
                r.entry("401".into())
                    .or_insert_with(|| error("missing, invalid, revoked or expired credential").into());
                r.entry("403".into())
                    .or_insert_with(|| error("insufficient_scope, quota_exceeded or another refusal").into());
                r.entry("429".into())
                    .or_insert_with(|| error("rate_limited (with Retry-After)").into());
                if named {
                    r.entry("404".into())
                        .or_insert_with(|| error("not_found: unknown, or not visible to this key").into());
                }
            }
        }
    }
}

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(api_doc())
}

/// The OpenAPI document as pretty JSON (used by `rt openapi` / `make openapi`).
pub fn openapi_json_string() -> String {
    api_doc()
        .to_pretty_json()
        .unwrap_or_else(|e| format!("{{\"error\":\"openapi serialization failed: {e}\"}}"))
}

/// Unknown routes answer with the error envelope, not a bare 404.
async fn not_found_fallback() -> ApiError {
    ApiError::not_found()
}

/// `/v1/*` + `/v1/openapi.json` + the Scalar page at `/docs`.
pub fn control_router(state: ApiState) -> Router {
    let metrics = state.metrics.clone();
    let api: Router<ApiState> = Router::new()
        // node
        .route("/v1/node/status", get(node_status))
        .route("/v1/node/config", get(node_config_get).put(node_config_put))
        .route("/v1/node/drain", post(node_drain))
        .route("/v1/node/scheduler", get(node_scheduler))
        .route("/v1/node/cache", get(node_cache))
        // who am I, versions, plans
        .route("/v1/version", get(version_get))
        .route("/v1/me", get(me_get))
        .route("/v1/limits", get(limits_get))
        .route("/v1/plans", get(plans_list))
        // tenants + keys
        .route("/v1/tenants", post(tenants_create).get(tenants_list))
        .route(
            "/v1/tenants/{t}",
            get(tenants_get).patch(tenants_patch).delete(tenants_delete),
        )
        .route("/v1/tenants/{t}/usage", get(tenants_usage))
        .route("/v1/tenants/{t}/keys", post(keys_create).get(keys_list))
        .route("/v1/tenants/{t}/keys/{key}", delete(keys_delete))
        // self-service tokens (D-B4)
        .route("/v1/keys", post(tokens_create).get(tokens_list))
        .route("/v1/keys/{key}", delete(tokens_delete))
        // projects (D-B3)
        .route("/v1/projects", post(projects_create).get(projects_list))
        .route(
            "/v1/projects/{p}",
            get(projects_get).patch(projects_patch).delete(projects_delete),
        )
        .route("/v1/projects/{p}/environments", get(environments_get))
        // services
        .route("/v1/services", post(services_create).get(services_list))
        .route(
            "/v1/services/{s}",
            get(services_get).patch(services_patch).delete(services_delete),
        )
        .route("/v1/services/{s}/versions", post(versions_upload).get(versions_list))
        .route("/v1/services/{s}/versions/{v}", get(versions_get))
        .route("/v1/services/{s}/versions/{v}/source", get(versions_source))
        .route("/v1/services/{s}/versions/{v}/rebuild", post(versions_rebuild))
        .route("/v1/services/{s}/types", get(services_types))
        .route("/v1/services/{s}/versions/{v}/build/logs", get(versions_build_logs))
        .route("/v1/services/{s}/live", put(services_set_live))
        .route("/v1/services/{s}/env", get(env_get).put(env_put))
        .route("/v1/services/{s}/secrets", get(secrets_list))
        .route(
            "/v1/services/{s}/secrets/{name}",
            put(secrets_put).delete(secrets_delete),
        )
        .route("/v1/services/{s}/hosts", get(hosts_list))
        .route(
            "/v1/services/{s}/hosts/{host}",
            get(hosts_get).put(hosts_put).delete(hosts_delete),
        )
        .route("/v1/services/{s}/schedules", get(schedules_list).post(schedules_create))
        .route("/v1/services/{s}/schedules/once", get(schedules_once_list))
        .route(
            "/v1/services/{s}/schedules/{id}",
            axum::routing::patch(schedules_patch).delete(schedules_delete),
        )
        .route("/v1/services/{s}/queue", post(queue_enqueue).get(queue_stats))
        .route("/v1/services/{s}/dlq", get(dlq_list).delete(dlq_purge))
        .route("/v1/services/{s}/dlq/redrive", post(dlq_redrive))
        .route("/v1/services/{s}/logs", get(logs_get))
        .route("/v1/services/{s}/stats", get(stats_get))
        .route("/v1/services/{s}/events", get(events_get))
        .route("/v1/services/{s}/data/export", get(data_export))
        // WebSockets and durable objects
        .route("/v1/services/{s}/objects", get(crate::sockets::objects_list))
        .route("/v1/services/{s}/objects/{name}", delete(crate::sockets::object_delete))
        .route(
            "/v1/services/{s}/objects/{name}/sockets",
            get(crate::sockets::sockets_list).delete(crate::sockets::sockets_close),
        )
        .route(
            "/v1/services/{s}/objects/{name}/sockets/{ws}",
            delete(crate::sockets::socket_close),
        )
        // operator SDKs
        .route("/v1/sdks", post(crate::sdks::sdks_publish).get(crate::sdks::sdks_list))
        .route("/v1/sdks/effective", get(crate::sdks::sdks_effective))
        .route("/v1/sdks/{name}", delete(crate::sdks::sdks_remove))
        .route("/v1/sdks/{name}/releases", get(crate::sdks::sdks_releases))
        .route("/v1/sdks/{name}/contract", get(crate::sdks::sdks_contract))
        .route("/v1/sdks/{name}/channels/{channel}", put(crate::sdks::sdks_set_channel))
        .route(
            "/v1/sdk-grants",
            get(crate::sdks::grants_list).put(crate::sdks::grants_put),
        )
        // event modules (feature stubs until their owners fill them in)
        .merge(crate::topics::routes())
        .merge(crate::consumers::routes())
        .merge(crate::invocations::routes())
        .route("/v1/openapi.json", get(openapi_json))
        .fallback(not_found_fallback)
        // JSON bodies are capped by `ApiJson`, uploads by `read_upload`; anything else stays small.
        .layer(DefaultBodyLimit::max(crate::extract::MAX_JSON_BODY));

    // `/mcp` calls the `/v1` routes in-process, so it gets its own copy of that router.
    let mcp = crate::mcp::mcp_router(state.clone(), api.clone().with_state(state.clone()));
    let router = Router::new()
        .merge(api.with_state(state))
        .merge(mcp)
        .merge(Scalar::with_url("/docs", api_doc()));
    crate::invoke::with_http_metrics(router, metrics, "control")
}

// Unused-import guard for items only referenced from generated OpenAPI code.
#[allow(dead_code)]
fn _types(_: Digest, _: ServiceConfig) {}

#[cfg(test)]
mod tests {
    use super::{api_doc, host_under_suffix, redact_config, strip_userinfo};
    use serde_json::{json, Value};

    fn doc() -> Value {
        serde_json::to_value(api_doc()).expect("document")
    }

    /// Every parameter that is not part of the path is a query parameter, and an `Option` one is
    /// optional (API-01; the document used to declare them `in: path, required: true`).
    #[test]
    fn query_parameters_are_declared_in_query_and_optional() {
        let d = doc();
        let mut seen = 0;
        for (path, item) in d["paths"].as_object().expect("paths") {
            for (method, op) in item.as_object().expect("item") {
                for p in op["parameters"].as_array().into_iter().flatten() {
                    let name = p["name"].as_str().expect("name");
                    let in_path = path.contains(&format!("{{{name}}}"));
                    let want = if in_path { "path" } else { "query" };
                    assert_eq!(p["in"], want, "{method} {path} `{name}`");
                    if !in_path {
                        seen += 1;
                        assert_ne!(p["required"], json!(true), "{method} {path} `{name}` is optional");
                    }
                }
            }
        }
        assert!(seen >= 20, "query parameters seen: {seen}");
        let services = &d["paths"]["/v1/services"]["get"]["parameters"];
        let names: Vec<&str> = services
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p["name"].as_str())
            .collect();
        assert_eq!(names, ["limit", "cursor", "slug", "project", "environment"]);
    }

    #[test]
    fn optional_body_fields_are_not_required() {
        let d = doc();
        for (schema, field) in [
            ("PatchTenantBody", "overrides"),
            ("ScheduleBody", "payload"),
            ("CreateTenantBody", "overrides"),
        ] {
            let req = d["components"]["schemas"][schema]["required"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            assert!(
                !req.contains(&json!(field)),
                "{schema}.{field} must be optional: {req:?}"
            );
        }
        let req = &d["components"]["schemas"]["ScheduleBody"]["required"];
        assert_eq!(req, &json!(["kind"]));
    }

    #[test]
    fn the_new_operations_are_documented() {
        let d = doc();
        for (m, p) in [
            ("get", "/v1/node/config"),
            ("get", "/v1/plans"),
            ("get", "/v1/me"),
            ("get", "/v1/version"),
            ("get", "/v1/services/{s}/hosts/{host}"),
        ] {
            assert!(d["paths"][p][m].is_object(), "{m} {p}");
        }
        let status = &d["components"]["schemas"]["ServiceStatus"]["properties"];
        assert!(status["admission"].is_object(), "ServiceStatus.admission");
        assert!(d["components"]["schemas"]["AdmissionView"]["properties"]["queue_wait_ms"].is_object());
    }

    #[test]
    fn node_config_credentials_are_masked() {
        let mut v = json!({
            "otel": {"endpoint": "https://user:pa55@otel.example.com:4318/v1"},
            "s3": {"endpoint": "http://minio:9000"},
            "binding": [{"name": "billing", "base_url": "https://u@billing.internal/api",
                         "inject_headers": {"authorization": "Bearer s3cr3t", "x-key": "env:BILLING_KEY"}}],
            "plans": {"free": {"rps": 50}}
        });
        redact_config(&mut v);
        assert_eq!(v["otel"]["endpoint"], "https://<redacted>@otel.example.com:4318/v1");
        assert_eq!(v["s3"]["endpoint"], "http://minio:9000", "no user info: unchanged");
        assert_eq!(v["binding"][0]["base_url"], "https://<redacted>@billing.internal/api");
        assert_eq!(v["binding"][0]["inject_headers"]["authorization"], "<redacted>");
        assert_eq!(
            v["binding"][0]["inject_headers"]["x-key"], "env:BILLING_KEY",
            "a reference names no secret"
        );
        assert_eq!(v["plans"]["free"]["rps"], 50);
        assert_eq!(strip_userinfo("mailto:a@b"), None);
        assert_eq!(
            strip_userinfo("https://host/path@x"),
            None,
            "an @ after the authority is not user info"
        );
    }

    /// API-01 drift check in `cargo test`: `openapi/openapi.json` is exactly what `rt openapi`
    /// prints (`make openapi`). `RT_UPDATE_OPENAPI=1` rewrites the file instead of failing.
    #[test]
    fn the_committed_openapi_document_matches_the_code() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../openapi/openapi.json");
        let generated = format!("{}\n", super::openapi_json_string());
        if std::env::var_os("RT_UPDATE_OPENAPI").is_some() {
            std::fs::write(&path, &generated).expect("write openapi/openapi.json");
            return;
        }
        let committed = std::fs::read_to_string(&path).expect("openapi/openapi.json");
        assert!(
            committed == generated,
            "openapi/openapi.json is out of date: run `make openapi` (or RT_UPDATE_OPENAPI=1 cargo test -p rt-api)"
        );
    }

    #[test]
    fn plan_host_suffixes_match_on_a_label_boundary_only() {
        for suf in ["example.com", ".example.com", "Example.COM."] {
            assert!(host_under_suffix("api.example.com", suf), "{suf}");
            assert!(host_under_suffix("a.b.example.com", suf), "{suf}");
            assert!(!host_under_suffix("evilexample.com", suf), "{suf}: no label boundary");
            assert!(
                !host_under_suffix("example.com", suf),
                "{suf}: the suffix itself is not a subdomain"
            );
            assert!(!host_under_suffix("example.com.evil.net", suf), "{suf}");
        }
        assert!(
            !host_under_suffix("api.example.com", ""),
            "an empty suffix allows nothing"
        );
        assert!(!host_under_suffix("api.example.com", "."), "neither does a lone dot");
    }

    /// QUOTA-004 / QUOTA-005: a cron schedule is judged by the shortest gap between its fires, not
    /// by the first gap after "now" (which made `0 0,1 * * *` pass or fail by the hour of day).
    #[test]
    fn quota_005_cron_period_is_the_shortest_gap() {
        use rt_types::ScheduleKind;
        let cron = |e: &str| ScheduleKind::Cron {
            expr: e.to_string(),
            tz: "UTC".to_string(),
        };
        let at = |h: i64| 1_800_000_000_000 - 1_800_000_000_000 % 86_400_000 + h * 3_600_000 + 1_000;
        for h in [0, 1, 2, 12, 23] {
            assert_eq!(
                super::period_seconds(&cron("0 0,1 * * *"), at(h)),
                3_600,
                "now = {h}:00"
            );
        }
        assert_eq!(super::period_seconds(&cron("0,1 * * * *"), at(5)), 60);
        assert!(
            super::period_seconds(&cron("0 0 1 1 *"), at(3)) >= 365 * 86_400,
            "yearly"
        );
        assert_eq!(super::period_seconds(&cron("*/5 * * * *"), at(7)), 300);
        assert_eq!(
            super::period_seconds(&ScheduleKind::Interval { every_seconds: 90 }, 0),
            90
        );
    }

    /// OAPI-003: every operation documents the errors any authenticated route can answer (401,
    /// 403, 429), and every operation on a path with an id documents 404.
    #[test]
    fn oapi_003_every_operation_documents_the_common_errors() {
        let d = doc();
        let mut missing = Vec::new();
        for (path, item) in d["paths"].as_object().expect("paths") {
            for (method, op) in item.as_object().expect("item") {
                let responses = op["responses"].as_object().expect("responses");
                let mut want = vec!["401", "403", "429"];
                if path.contains('{') {
                    want.push("404");
                }
                for code in want {
                    if !responses.contains_key(code) {
                        missing.push(format!("{method} {path}: {code}"));
                    }
                }
            }
        }
        assert!(
            missing.is_empty(),
            "{} gaps, e.g. {:?}",
            missing.len(),
            &missing[..missing.len().min(8)]
        );
    }
    #[tokio::test]
    async fn a_tenant_created_through_the_api_reads_back_on_the_same_node() {
        let node = crate::test_support::node().await;
        let t = crate::test_support::tenant(&node, "acme").await;
        let (status, _, body) = node
            .call(axum::http::Method::GET, &format!("/v1/tenants/{t}"), None, None)
            .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        assert_eq!(body["name"], "acme");
        assert_eq!(
            node.audit
                .0
                .lock()
                .iter()
                .filter(|e| e.action == "tenant.created")
                .count(),
            1
        );
    }
}
