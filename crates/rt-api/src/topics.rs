//! Topic CRUD and topic messages on `/v1/topics` (`std:stream` logs, plan-final T22).
//!
//! `{t}` is a topic id (`tpc_…`) or a name. A name resolves in the namespace given by the query
//! (GET/PATCH/DELETE) or body (POST) fields `project` (slug or `prj_…`, default `default`) and
//! `environment` (default `production`). Topics are only ever loaded through [`load_topic`]: a
//! topic of another tenant, or outside a restricted key's projects/environments, is `404`.

use std::collections::{BTreeMap, HashMap};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use rt_sec::QuotaOp;
use rt_store::stream::{NewRecord, ReadFrom, Topic, TopicSpec};
use rt_store::{StoreError, TenantScope};
use rt_types::events::{
    body_json, valid_topic_name, validate_headers, Namespace, APPEND_BATCH_MAX, DEFAULT_RETENTION_HOURS,
    PARTITIONS_HARD_MAX, READ_LIMIT_MAX, READ_MAX_BYTES, TOPIC_ID_PREFIX,
};
use rt_types::{ProjectId, Scope, TenantId, TopicId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::auth::Auth;
use crate::control::{audit, fingerprint, idem_replay, idem_store, rfc3339_ms, value_resp};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::{ApiJson, ApiQuery};
use crate::projects::{check_environment_in_project, find_project, project_slug_of};
use crate::state::{ApiState, ControlEvent};

const MIB: u64 = 1024 * 1024;
const HOUR_MS: u64 = 3_600_000;

// ---- DTOs --------------------------------------------------------------------------------------

/// `POST /v1/topics`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicBody {
    /// `[a-z0-9][a-z0-9._-]{0,62}`, not starting with `tpc_`.
    pub name: String,
    /// Project slug or `prj_…` id (default `default`).
    pub project: Option<String>,
    /// Default `production`.
    pub environment: Option<String>,
    /// Default 1, at most the plan's `max_topic_partitions`.
    pub partitions: Option<u32>,
    /// Default 24, at most the plan's `max_stream_retention_hours`.
    pub retention_hours: Option<u32>,
    /// Size cap of this topic in MiB; `0` (default) = only the tenant cap.
    pub max_mb: Option<u64>,
}

/// `PATCH /v1/topics/{t}`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchTopicBody {
    pub retention_hours: Option<u32>,
    pub max_mb: Option<u64>,
}

/// Namespace of a topic name (ignored for `tpc_…` ids).
#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TopicNsQuery {
    pub project: Option<String>,
    pub environment: Option<String>,
}

/// `GET /v1/topics/{t}/messages`.
#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ReadQuery {
    pub project: Option<String>,
    pub environment: Option<String>,
    /// Required when the topic has more than one partition.
    pub partition: Option<u32>,
    /// `earliest` (default), `latest` or an offset.
    pub from: Option<String>,
    /// RFC 3339: the first record at or after this time (instead of `from`).
    pub from_time: Option<String>,
    /// 1..=100 (default 100).
    pub limit: Option<u32>,
}

/// One record to append.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AppendRecord {
    /// A JSON string is stored as its text, any other JSON value as its serialization.
    #[schema(value_type = Object)]
    pub body: Value,
    /// Records with the same key go to the same partition.
    pub key: Option<String>,
    pub headers: Option<BTreeMap<String, String>>,
    /// The same id on the topic within 2 minutes returns the original offset.
    pub dedup_id: Option<String>,
}

/// `POST /v1/topics/{t}/messages`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AppendBody {
    pub project: Option<String>,
    pub environment: Option<String>,
    /// 1..=100 records, all or nothing.
    pub records: Vec<AppendRecord>,
}

/// Where one appended record landed.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppendedView {
    pub partition: u32,
    pub offset: u64,
    /// `true` when `dedup_id` matched an earlier record (nothing was appended).
    pub duplicate: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppendedPage {
    pub items: Vec<AppendedView>,
}

/// `first` = first retained offset, `next` = offset of the next append.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TopicHead {
    pub partition: u32,
    pub first: u64,
    pub next: u64,
}

/// A consumer group of a service on the topic.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TopicConsumer {
    pub service: String,
    pub slug: String,
    pub group: String,
    /// Records not yet committed, summed over partitions.
    pub lag: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TopicView {
    pub id: String,
    pub name: String,
    pub project: String,
    pub project_id: String,
    pub environment: String,
    pub partitions: u32,
    pub retention_hours: u64,
    pub max_mb: u64,
    pub bytes: u64,
    pub created_at: i64,
    pub heads: Vec<TopicHead>,
    /// Only on `GET /v1/topics/{t}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumers: Option<Vec<TopicConsumer>>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TopicPage {
    pub items: Vec<TopicView>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RecordView {
    pub partition: u32,
    pub offset: u64,
    pub key: Option<String>,
    #[schema(value_type = Object)]
    pub headers: Value,
    /// JSON when the body parses as JSON, else a string.
    #[schema(value_type = Object)]
    pub body: Value,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RecordPage {
    pub items: Vec<RecordView>,
    /// Offset to pass as `from` for the next page.
    pub next: u64,
    /// `true` when `next` is the partition's head.
    pub end: bool,
}

// ---- helpers -----------------------------------------------------------------------------------

/// Store errors of this module: our own validation text is kept, the rest is sanitized.
fn store_err(e: StoreError) -> ApiError {
    match e {
        StoreError::Invalid(m) => ApiError::invalid(m),
        StoreError::Conflict("topic_limit") => ApiError::new("quota_exceeded", "the plan's topic limit is reached"),
        StoreError::Conflict("topic_exists") => ApiError::new("topic_exists", "a topic with this name exists"),
        StoreError::Conflict("stream_full") => ApiError::new(
            "stream_full",
            "the tenant's stream storage (records + dead letters) is full",
        ),
        e => e.into(),
    }
}

fn limit_err(field: &str, max: u64) -> ApiError {
    ApiError::new(
        "limit_exceeds_plan",
        format!("`{field}` is over the plan maximum of {max}"),
    )
    .with_details(json!({"field": field, "plan_max": max}))
}

fn environment_of(e: Option<&str>) -> Result<String, ApiError> {
    match e {
        None => Ok(rt_types::default_environment()),
        Some(e) if rt_types::valid_environment(e) => Ok(e.to_string()),
        Some(_) => Err(ApiError::invalid("`environment` is not a valid environment name")),
    }
}

/// The namespace a request names (project must exist, environment must be one of its).
async fn resolve_ns(
    s: &ApiState,
    tenant: TenantId,
    project: Option<&str>,
    environment: Option<&str>,
) -> Result<Namespace, ApiError> {
    let environment = environment_of(environment)?;
    let slug = project_slug_of(s, tenant, project).await?;
    check_environment_in_project(s, tenant, &slug, &environment).await?;
    let project = match find_project(s, tenant, &slug).await? {
        Some(p) => p.id,
        None => rt_types::default_project_id(tenant),
    };
    Ok(Namespace {
        tenant,
        project,
        environment,
    })
}

/// Load a topic the caller may see; anything else is `404` (existence is never revealed).
pub(crate) async fn load_topic(
    s: &ApiState,
    a: &Auth,
    t: &str,
    project: Option<&str>,
    environment: Option<&str>,
) -> Result<Topic, ApiError> {
    let tenant = a.tenant()?;
    let topic = if t.starts_with(TOPIC_ID_PREFIX) {
        let id: TopicId = t.parse().map_err(|_| ApiError::not_found())?;
        s.streams.topic_by_id(id).await.map_err(store_err)?
    } else {
        let ns = match resolve_ns(s, tenant, project, environment).await {
            Ok(ns) => ns,
            Err(e) if e.code == "validation_failed" => return Err(ApiError::not_found()),
            Err(e) => return Err(e),
        };
        s.streams.get_topic(&ns, t).await.map_err(store_err)?
    };
    let topic = topic.ok_or_else(ApiError::not_found)?;
    if topic.ns.tenant != tenant || !a.principal.can_touch(topic.ns.project, &topic.ns.environment) {
        return Err(ApiError::not_found());
    }
    Ok(topic)
}

async fn project_slug(
    s: &ApiState,
    tenant: TenantId,
    id: ProjectId,
    cache: &mut HashMap<ProjectId, String>,
) -> Result<String, ApiError> {
    if let Some(v) = cache.get(&id) {
        return Ok(v.clone());
    }
    let slug = match s.meta.get_project_by_id(tenant, id).await? {
        Some(p) => p.slug,
        None => rt_types::default_project(),
    };
    cache.insert(id, slug.clone());
    Ok(slug)
}

async fn topic_view(s: &ApiState, t: &Topic, cache: &mut HashMap<ProjectId, String>) -> Result<TopicView, ApiError> {
    let heads = s.streams.heads(t.id).await.map_err(store_err)?;
    Ok(TopicView {
        id: t.id.to_string(),
        name: t.name.clone(),
        project: project_slug(s, t.ns.tenant, t.ns.project, cache).await?,
        project_id: t.ns.project.to_string(),
        environment: t.ns.environment.clone(),
        partitions: t.partitions,
        retention_hours: t.retention_ms / HOUR_MS,
        max_mb: t.max_bytes / MIB,
        bytes: t.bytes,
        created_at: t.created_at_ms,
        heads: heads
            .into_iter()
            .map(|h| TopicHead {
                partition: h.partition,
                first: h.first,
                next: h.next,
            })
            .collect(),
        consumers: None,
    })
}

async fn consumers_of(s: &ApiState, t: &Topic, view: &TopicView) -> Result<Vec<TopicConsumer>, ApiError> {
    let next: HashMap<u32, u64> = view.heads.iter().map(|h| (h.partition, h.next)).collect();
    let mut groups: BTreeMap<(String, String), (rt_types::ServiceId, u64)> = BTreeMap::new();
    for c in s.streams.cursors_on(t.id).await.map_err(store_err)? {
        let lag = next
            .get(&c.key.partition)
            .copied()
            .unwrap_or(0)
            .saturating_sub(c.committed);
        let e = groups
            .entry((c.key.service.to_string(), c.key.group.clone()))
            .or_insert((c.key.service, 0));
        e.1 += lag;
    }
    let mut out = Vec::new();
    for ((service, group), (id, lag)) in groups {
        let Some(svc) = s.meta.get_service(TenantScope::Tenant(t.ns.tenant), id).await? else {
            continue;
        };
        out.push(TopicConsumer {
            service,
            slug: svc.slug,
            group,
            lag,
        });
    }
    Ok(out)
}

fn check_partitions(n: u32, plan_max: u32) -> Result<(), ApiError> {
    if n == 0 || n > PARTITIONS_HARD_MAX {
        return Err(ApiError::invalid(format!(
            "`partitions` must be 1..={PARTITIONS_HARD_MAX}"
        )));
    }
    if n > plan_max {
        return Err(limit_err("partitions", plan_max.into()));
    }
    Ok(())
}

fn check_retention(hours: u32, plan_max: u32) -> Result<(), ApiError> {
    if hours == 0 {
        return Err(ApiError::invalid("`retention_hours` must be at least 1"));
    }
    if hours > plan_max {
        return Err(limit_err("retention_hours", plan_max.into()));
    }
    Ok(())
}

fn check_max_mb(mb: u64, plan_max: u64) -> Result<(), ApiError> {
    if mb > plan_max {
        return Err(limit_err("max_mb", plan_max));
    }
    Ok(())
}

/// `from` / `from_time` of a read (`earliest` when neither is given).
fn parse_from(from: Option<&str>, from_time: Option<&str>) -> Result<ReadFrom, ApiError> {
    match (from, from_time) {
        (Some(_), Some(_)) => Err(ApiError::invalid("pass either `from` or `from_time`, not both")),
        (None, Some(t)) => Ok(ReadFrom::Time(rfc3339_ms(t)?)),
        (None, None) | (Some("earliest"), None) => Ok(ReadFrom::Earliest),
        (Some("latest"), None) => Ok(ReadFrom::Latest),
        (Some(o), None) => o
            .parse::<u64>()
            .map(ReadFrom::Offset)
            .map_err(|_| ApiError::invalid("`from` must be `earliest`, `latest` or an offset")),
    }
}

/// A body as stored: a JSON string is its text, anything else its JSON serialization.
fn body_bytes(v: &Value) -> Bytes {
    match v {
        Value::String(s) => Bytes::from(s.clone().into_bytes()),
        v => Bytes::from(serde_json::to_vec(v).unwrap_or_default()),
    }
}

fn to_records(records: Vec<AppendRecord>) -> Result<Vec<NewRecord>, ApiError> {
    if records.is_empty() || records.len() > APPEND_BATCH_MAX {
        return Err(ApiError::invalid(format!(
            "`records` must hold 1..={APPEND_BATCH_MAX} records"
        )));
    }
    records
        .into_iter()
        .map(|r| {
            let headers: Vec<(String, String)> = r.headers.unwrap_or_default().into_iter().collect();
            validate_headers(&headers).map_err(ApiError::invalid)?;
            Ok(NewRecord {
                key: r.key,
                headers,
                body: body_bytes(&r.body),
                dedup_id: r.dedup_id,
            })
        })
        .collect()
}

// ---- handlers ----------------------------------------------------------------------------------

#[utoipa::path(post, path = "/v1/topics", tag = "topics", request_body = CreateTopicBody,
    responses((status = 201, body = TopicView), (status = 403, body = ErrorEnvelope),
        (status = 409, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
async fn topics_create(
    State(s): State<ApiState>,
    a: Auth,
    h: HeaderMap,
    ApiJson(b): ApiJson<CreateTopicBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let fp = fingerprint(&[b"topics", format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    let tenant = a.tenant()?;
    if !valid_topic_name(&b.name) {
        return Err(ApiError::invalid(
            "`name` must match ^[a-z0-9][a-z0-9._-]{0,62}$ and not start with `tpc_`",
        ));
    }
    let partitions = b.partitions.unwrap_or(1);
    let retention = b.retention_hours.unwrap_or(DEFAULT_RETENTION_HOURS);
    let max_mb = b.max_mb.unwrap_or(0);
    let ns = resolve_ns(&s, tenant, b.project.as_deref(), b.environment.as_deref()).await?;
    if !a.principal.can_touch(ns.project, &ns.environment) {
        return Err(ApiError::new(
            "insufficient_scope",
            format!(
                "this key may not create topics in environment `{}` of this project",
                ns.environment
            ),
        ));
    }
    let plan = s.plans.plan_for(tenant);
    check_partitions(partitions, plan.max_topic_partitions)?;
    check_retention(retention, plan.max_stream_retention_hours)?;
    check_max_mb(max_mb, plan.max_stream_mb)?;
    let topic = s
        .streams
        .create_topic(
            &ns,
            TopicSpec {
                name: b.name,
                partitions,
                retention_ms: u64::from(retention) * HOUR_MS,
                max_bytes: max_mb.saturating_mul(MIB),
            },
            plan.max_topics,
        )
        .await
        .map_err(store_err)?;
    audit(
        &s,
        &a,
        "topic.created",
        Some(tenant),
        None,
        json!({"topic": topic.id.to_string(), "name": topic.name, "partitions": topic.partitions}),
    );
    let view = topic_view(&s, &topic, &mut HashMap::new()).await?;
    let view = serde_json::to_value(view).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::CREATED, &view);
    Ok(value_resp(StatusCode::CREATED, &view))
}

/// Without `project` / `environment`: every topic the caller may see.
#[utoipa::path(get, path = "/v1/topics", tag = "topics", params(TopicNsQuery),
    responses((status = 200, body = TopicPage)))]
async fn topics_list(
    State(s): State<ApiState>,
    a: Auth,
    ApiQuery(q): ApiQuery<TopicNsQuery>,
) -> Result<axum::Json<TopicPage>, ApiError> {
    a.require_scope(Scope::ServicesRead)?;
    let tenant = a.tenant()?;
    let project = match q.project.as_deref() {
        None => None,
        Some(p) => match find_project(&s, tenant, p).await? {
            Some(p) => Some(p.id),
            None if p == rt_types::DEFAULT_PROJECT => Some(rt_types::default_project_id(tenant)),
            None => return Ok(axum::Json(TopicPage { items: Vec::new() })),
        },
    };
    let env = q.environment.as_deref().map(|e| environment_of(Some(e))).transpose()?;
    let topics = s
        .streams
        .list_topics(tenant, project, env.as_deref())
        .await
        .map_err(store_err)?;
    let mut cache = HashMap::new();
    let mut items = Vec::new();
    for t in topics
        .iter()
        .filter(|t| a.principal.can_touch(t.ns.project, &t.ns.environment))
    {
        items.push(topic_view(&s, t, &mut cache).await?);
    }
    Ok(axum::Json(TopicPage { items }))
}

#[utoipa::path(get, path = "/v1/topics/{t}", tag = "topics",
    params(("t" = String, Path, description = "topic id (`tpc_…`) or name"), TopicNsQuery),
    responses((status = 200, body = TopicView), (status = 404, body = ErrorEnvelope)))]
async fn topics_get(
    State(s): State<ApiState>,
    a: Auth,
    Path(t): Path<String>,
    ApiQuery(q): ApiQuery<TopicNsQuery>,
) -> Result<axum::Json<TopicView>, ApiError> {
    a.require_scope(Scope::ServicesRead)?;
    let topic = load_topic(&s, &a, &t, q.project.as_deref(), q.environment.as_deref()).await?;
    let mut view = topic_view(&s, &topic, &mut HashMap::new()).await?;
    view.consumers = Some(consumers_of(&s, &topic, &view).await?);
    Ok(axum::Json(view))
}

#[utoipa::path(patch, path = "/v1/topics/{t}", tag = "topics", request_body = PatchTopicBody,
    params(("t" = String, Path, description = "topic id (`tpc_…`) or name"), TopicNsQuery),
    responses((status = 200, body = TopicView), (status = 404, body = ErrorEnvelope),
        (status = 422, body = ErrorEnvelope)))]
async fn topics_patch(
    State(s): State<ApiState>,
    a: Auth,
    Path(t): Path<String>,
    ApiQuery(q): ApiQuery<TopicNsQuery>,
    ApiJson(b): ApiJson<PatchTopicBody>,
) -> Result<axum::Json<TopicView>, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let topic = load_topic(&s, &a, &t, q.project.as_deref(), q.environment.as_deref()).await?;
    let plan = s.plans.plan_for(topic.ns.tenant);
    if let Some(h) = b.retention_hours {
        check_retention(h, plan.max_stream_retention_hours)?;
    }
    if let Some(mb) = b.max_mb {
        check_max_mb(mb, plan.max_stream_mb)?;
    }
    let updated = s
        .streams
        .update_topic(
            topic.id,
            b.retention_hours.map(|h| u64::from(h) * HOUR_MS),
            b.max_mb.map(|mb| mb.saturating_mul(MIB)),
        )
        .await
        .map_err(store_err)?;
    audit(
        &s,
        &a,
        "topic.updated",
        Some(topic.ns.tenant),
        None,
        json!({"topic": topic.id.to_string(), "retention_hours": b.retention_hours, "max_mb": b.max_mb}),
    );
    Ok(axum::Json(topic_view(&s, &updated, &mut HashMap::new()).await?))
}

/// Deletes the topic with its records, consumer cursors and dead letters.
#[utoipa::path(delete, path = "/v1/topics/{t}", tag = "topics",
    params(("t" = String, Path, description = "topic id (`tpc_…`) or name"), TopicNsQuery),
    responses((status = 204), (status = 404, body = ErrorEnvelope)))]
async fn topics_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path(t): Path<String>,
    ApiQuery(q): ApiQuery<TopicNsQuery>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let topic = load_topic(&s, &a, &t, q.project.as_deref(), q.environment.as_deref()).await?;
    s.streams.delete_topic(topic.id).await.map_err(store_err)?;
    audit(
        &s,
        &a,
        "topic.deleted",
        Some(topic.ns.tenant),
        None,
        json!({"topic": topic.id.to_string(), "name": topic.name}),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/v1/topics/{t}/messages", tag = "topics", request_body = AppendBody,
    params(("t" = String, Path, description = "topic id (`tpc_…`) or name")),
    responses((status = 202, body = AppendedPage), (status = 404, body = ErrorEnvelope),
        (status = 422, body = ErrorEnvelope), (status = 429, body = ErrorEnvelope)))]
async fn messages_append(
    State(s): State<ApiState>,
    a: Auth,
    h: HeaderMap,
    Path(t): Path<String>,
    ApiJson(b): ApiJson<AppendBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::QueueWrite)?;
    let fp = fingerprint(&[b"topic-append", t.as_bytes(), format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    let topic = load_topic(&s, &a, &t, b.project.as_deref(), b.environment.as_deref()).await?;
    let records = to_records(b.records)?;
    let tenant = topic.ns.tenant;
    s.quotas.check(tenant, QuotaOp::Invoke)?;
    let cap = s.plans.plan_for(tenant).max_stream_mb.saturating_mul(MIB);
    let count = records.len();
    let out = s.streams.append(topic.id, records, cap).await.map_err(store_err)?;
    s.node.notify(ControlEvent::StreamAppended(topic.id));
    audit(
        &s,
        &a,
        "topic.appended",
        Some(tenant),
        None,
        json!({"topic": topic.id.to_string(), "count": count}),
    );
    let page = AppendedPage {
        items: out
            .into_iter()
            .map(|x| AppendedView {
                partition: x.partition,
                offset: x.offset,
                duplicate: x.duplicate,
            })
            .collect(),
    };
    let view = serde_json::to_value(page).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::ACCEPTED, &view);
    Ok(value_resp(StatusCode::ACCEPTED, &view))
}

#[utoipa::path(get, path = "/v1/topics/{t}/messages", tag = "topics",
    params(("t" = String, Path, description = "topic id (`tpc_…`) or name"), ReadQuery),
    responses((status = 200, body = RecordPage), (status = 404, body = ErrorEnvelope),
        (status = 422, body = ErrorEnvelope)))]
async fn messages_read(
    State(s): State<ApiState>,
    a: Auth,
    Path(t): Path<String>,
    ApiQuery(q): ApiQuery<ReadQuery>,
) -> Result<axum::Json<RecordPage>, ApiError> {
    a.require_scope(Scope::ServicesRead)?;
    let topic = load_topic(&s, &a, &t, q.project.as_deref(), q.environment.as_deref()).await?;
    let partition = match q.partition {
        Some(p) if p >= topic.partitions => {
            return Err(ApiError::invalid(format!(
                "partition is out of range (topic `{}` has {} partitions)",
                topic.name, topic.partitions
            )))
        }
        Some(p) => p,
        None if topic.partitions > 1 => {
            return Err(ApiError::invalid(format!(
                "topic `{}` has {} partitions: pass `partition`",
                topic.name, topic.partitions
            )))
        }
        None => 0,
    };
    let from = parse_from(q.from.as_deref(), q.from_time.as_deref())?;
    let limit = q.limit.unwrap_or(READ_LIMIT_MAX as u32).clamp(1, READ_LIMIT_MAX as u32) as usize;
    let records = s
        .streams
        .read(topic.id, partition, from, limit, READ_MAX_BYTES)
        .await
        .map_err(store_err)?;
    let head = s
        .streams
        .heads(topic.id)
        .await
        .map_err(store_err)?
        .into_iter()
        .find(|h| h.partition == partition);
    let (first, head_next) = head.map(|h| (h.first, h.next)).unwrap_or((0, 0));
    let next = match (records.last(), from) {
        (Some(r), _) => r.offset + 1,
        (None, ReadFrom::Offset(o)) => o.max(first).min(head_next),
        (None, ReadFrom::Earliest) => first,
        (None, ReadFrom::Latest | ReadFrom::Time(_)) => head_next,
    };
    let items = records
        .into_iter()
        .map(|r| {
            let g = r.guest_json();
            RecordView {
                partition: r.partition,
                offset: r.offset,
                key: r.key,
                headers: g.get("headers").cloned().unwrap_or_else(|| json!({})),
                body: body_json(&r.body),
                timestamp: r.ts_ms,
            }
        })
        .collect();
    Ok(axum::Json(RecordPage {
        items,
        next,
        end: next >= head_next,
    }))
}

/// Topic routes.
pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/topics", get(topics_list).post(topics_create))
        .route(
            "/v1/topics/{t}",
            get(topics_get).patch(topics_patch).delete(topics_delete),
        )
        .route("/v1/topics/{t}/messages", get(messages_read).post(messages_append))
}

/// OpenAPI paths and schemas of this module; merged into `control::api_doc()`.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(topics_create, topics_list, topics_get, topics_patch, topics_delete, messages_append, messages_read),
    components(schemas(
        CreateTopicBody,
        PatchTopicBody,
        AppendBody,
        AppendRecord,
        AppendedView,
        AppendedPage,
        TopicHead,
        TopicConsumer,
        TopicView,
        TopicPage,
        RecordView,
        RecordPage
    )),
    tags((name = "topics", description = "Persistent, replayable topic logs (`std:stream`)"))
)]
pub struct Doc;

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};
    use serde_json::json;

    use super::*;
    use crate::test_support;

    /// `from` accepts `earliest` (default), `latest` and an offset; `from_time` is RFC 3339; not both.
    #[test]
    fn read_positions_parse() {
        assert_eq!(parse_from(None, None).unwrap(), ReadFrom::Earliest);
        assert_eq!(parse_from(Some("earliest"), None).unwrap(), ReadFrom::Earliest);
        assert_eq!(parse_from(Some("latest"), None).unwrap(), ReadFrom::Latest);
        assert_eq!(parse_from(Some("42"), None).unwrap(), ReadFrom::Offset(42));
        assert_eq!(
            parse_from(None, Some("1970-01-01T00:00:01Z")).unwrap(),
            ReadFrom::Time(1000)
        );
        assert_eq!(parse_from(Some("-1"), None).unwrap_err().code, "validation_failed");
        assert_eq!(
            parse_from(None, Some("yesterday")).unwrap_err().code,
            "validation_failed"
        );
        assert_eq!(
            parse_from(Some("1"), Some("1970-01-01T00:00:01Z")).unwrap_err().code,
            "validation_failed"
        );
    }

    /// A JSON string body is stored as its text, other JSON as its serialization.
    #[test]
    fn bodies_are_stored_as_text_or_json() {
        assert_eq!(body_bytes(&json!("hi")), Bytes::from_static(b"hi"));
        assert_eq!(body_bytes(&json!({"a": 1})), Bytes::from_static(br#"{"a":1}"#));
    }

    /// Bodies reject unknown fields; batches hold 1..=100 records with valid headers.
    #[test]
    fn dto_parsing_and_batch_rules() {
        assert!(serde_json::from_value::<CreateTopicBody>(json!({"name": "a", "bogus": 1})).is_err());
        let b: CreateTopicBody = serde_json::from_value(json!({"name": "orders"})).unwrap();
        assert!(b.project.is_none() && b.environment.is_none() && b.partitions.is_none());
        assert!(serde_json::from_value::<PatchTopicBody>(json!({"partitions": 2})).is_err());
        assert_eq!(to_records(vec![]).unwrap_err().code, "validation_failed");
        let many = (0..=APPEND_BATCH_MAX)
            .map(|_| AppendRecord {
                body: json!(1),
                key: None,
                headers: None,
                dedup_id: None,
            })
            .collect();
        assert_eq!(to_records(many).unwrap_err().code, "validation_failed");
        let bad = AppendRecord {
            body: json!(1),
            key: None,
            headers: Some(BTreeMap::from([("x-rt-tenant".to_string(), "v".to_string())])),
            dedup_id: None,
        };
        assert_eq!(to_records(vec![bad]).unwrap_err().code, "validation_failed");
    }

    /// Plan limits are `limit_exceeds_plan`; out-of-range values are `validation_failed`.
    #[test]
    fn plan_limits_are_checked() {
        assert_eq!(check_partitions(0, 4).unwrap_err().code, "validation_failed");
        assert_eq!(check_partitions(17, 32).unwrap_err().code, "validation_failed");
        assert_eq!(check_partitions(5, 4).unwrap_err().code, "limit_exceeds_plan");
        assert!(check_partitions(4, 4).is_ok());
        assert_eq!(check_retention(0, 72).unwrap_err().code, "validation_failed");
        assert_eq!(check_retention(73, 72).unwrap_err().code, "limit_exceeds_plan");
        assert_eq!(check_max_mb(101, 100).unwrap_err().code, "limit_exceeds_plan");
        assert!(check_max_mb(0, 100).is_ok());
    }

    /// Store conflicts map to their stable codes.
    #[test]
    fn store_errors_map_to_codes() {
        assert_eq!(store_err(StoreError::Conflict("topic_limit")).code, "quota_exceeded");
        assert_eq!(store_err(StoreError::Conflict("topic_exists")).code, "topic_exists");
        assert_eq!(store_err(StoreError::Conflict("stream_full")).code, "stream_full");
        assert_eq!(store_err(StoreError::Invalid("x".into())).message, "x");
        assert_eq!(store_err(StoreError::NotFound).code, "not_found");
    }

    /// Create defaults to `default` / `production`, 1 partition, 24 h; get by name or id; list; patch; delete.
    #[tokio::test]
    async fn topic_crud_with_namespace_defaults() {
        let node = test_support::node().await;
        let t = test_support::tenant(&node, "acme").await;
        let (st, _, v) = node
            .call(Method::POST, "/v1/topics", Some(t), Some(json!({"name": "orders"})))
            .await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
        assert_eq!(v["project"], "default");
        assert_eq!(v["environment"], "production");
        assert_eq!(v["partitions"], 1);
        assert_eq!(v["retention_hours"], 24);
        assert_eq!(v["heads"][0]["next"], 0);
        assert!(v.get("consumers").is_none());
        let id = v["id"].as_str().unwrap().to_string();
        assert!(id.starts_with("tpc_"));
        assert_eq!(node.audit.0.lock().last().unwrap().action, "topic.created");

        let (st, _, v) = node
            .call(Method::POST, "/v1/topics", Some(t), Some(json!({"name": "orders"})))
            .await;
        assert_eq!(
            (st, v["error"]["code"].as_str()),
            (StatusCode::CONFLICT, Some("topic_exists"))
        );
        let (st, _, _) = node
            .call(Method::POST, "/v1/topics", Some(t), Some(json!({"name": "tpc_x"})))
            .await;
        assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY);
        let (_, _, v) = node
            .call(
                Method::POST,
                "/v1/topics",
                Some(t),
                Some(json!({"name": "b", "partitions": 5})),
            )
            .await;
        assert_eq!(v["error"]["code"], "limit_exceeds_plan");

        let (st, _, v) = node.call(Method::GET, "/v1/topics/orders", Some(t), None).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["consumers"], json!([]));
        let (st, _, _) = node.call(Method::GET, &format!("/v1/topics/{id}"), Some(t), None).await;
        assert_eq!(st, StatusCode::OK);
        let (st, _, _) = node
            .call(Method::GET, "/v1/topics/orders?environment=staging", Some(t), None)
            .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        let (_, _, v) = node.call(Method::GET, "/v1/topics", Some(t), None).await;
        assert_eq!(v["items"].as_array().unwrap().len(), 1);
        let (_, _, v) = node.call(Method::GET, "/v1/topics?project=nope", Some(t), None).await;
        assert_eq!(v["items"], json!([]));

        let (st, _, v) = node
            .call(
                Method::PATCH,
                "/v1/topics/orders",
                Some(t),
                Some(json!({"retention_hours": 48, "max_mb": 10})),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(
            (v["retention_hours"].as_u64(), v["max_mb"].as_u64()),
            (Some(48), Some(10))
        );
        let (_, _, v) = node
            .call(
                Method::PATCH,
                "/v1/topics/orders",
                Some(t),
                Some(json!({"retention_hours": 1000})),
            )
            .await;
        assert_eq!(v["error"]["code"], "limit_exceeds_plan");

        let (st, _, _) = node.call(Method::DELETE, "/v1/topics/orders", Some(t), None).await;
        assert_eq!(st, StatusCode::NO_CONTENT);
        let (st, _, _) = node.call(Method::GET, &format!("/v1/topics/{id}"), Some(t), None).await;
        assert_eq!(st, StatusCode::NOT_FOUND);
    }

    /// Another tenant's topic is `404`, by name and by id.
    #[tokio::test]
    async fn topics_of_other_tenants_are_not_found() {
        let node = test_support::node().await;
        let a = test_support::tenant(&node, "a").await;
        let b = test_support::tenant(&node, "b").await;
        let (_, _, v) = node
            .call(Method::POST, "/v1/topics", Some(a), Some(json!({"name": "orders"})))
            .await;
        let id = v["id"].as_str().unwrap().to_string();
        for uri in [
            format!("/v1/topics/{id}"),
            "/v1/topics/orders".into(),
            "/v1/topics/tpc_bad".into(),
        ] {
            let (st, _, _) = node.call(Method::GET, &uri, Some(b), None).await;
            assert_eq!(st, StatusCode::NOT_FOUND, "{uri}");
        }
        let (st, _, _) = node
            .call(
                Method::POST,
                &format!("/v1/topics/{id}/messages"),
                Some(b),
                Some(json!({"records": [{"body": 1}]})),
            )
            .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        let (st, _, _) = node
            .call(Method::DELETE, &format!("/v1/topics/{id}"), Some(b), None)
            .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
    }

    /// Append returns offsets (`202`), replays an `Idempotency-Key`, notifies the engine; read pages and
    /// requires `partition` on multi-partition topics.
    #[tokio::test]
    async fn append_and_read_messages() {
        let node = test_support::node().await;
        let t = test_support::tenant(&node, "acme").await;
        node.call(Method::POST, "/v1/topics", Some(t), Some(json!({"name": "one"})))
            .await;
        let body = json!({"records": [{"body": {"n": 1}, "headers": {"h": "v"}}, {"body": "two", "key": "k"}]});
        let (st, _, v) = node
            .call_with(
                Method::POST,
                "/v1/topics/one/messages",
                Some(t),
                &[("idempotency-key", "k1")],
                Some(body.clone()),
            )
            .await;
        assert_eq!(st, StatusCode::ACCEPTED, "{v}");
        assert_eq!(v["items"][1]["offset"], 1);
        let (st, _, v2) = node
            .call_with(
                Method::POST,
                "/v1/topics/one/messages",
                Some(t),
                &[("idempotency-key", "k1")],
                Some(body),
            )
            .await;
        assert_eq!((st, &v2), (StatusCode::ACCEPTED, &v));
        assert_eq!(
            node.audit
                .0
                .lock()
                .iter()
                .filter(|e| e.action == "topic.appended")
                .count(),
            1
        );
        assert!(node
            .facts
            .notified
            .lock()
            .iter()
            .any(|e| matches!(e, ControlEvent::StreamAppended(_))));

        let (st, _, v) = node
            .call(Method::GET, "/v1/topics/one/messages?limit=1", Some(t), None)
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["items"][0]["body"], json!({"n": 1}));
        assert_eq!(v["items"][0]["headers"], json!({"h": "v"}));
        assert_eq!((v["next"].as_u64(), v["end"].as_bool()), (Some(1), Some(false)));
        let (_, _, v) = node
            .call(Method::GET, "/v1/topics/one/messages?from=1", Some(t), None)
            .await;
        assert_eq!(v["items"][0]["body"], "two");
        assert_eq!(v["items"][0]["key"], "k");
        assert_eq!((v["next"].as_u64(), v["end"].as_bool()), (Some(2), Some(true)));
        let (_, _, v) = node
            .call(Method::GET, "/v1/topics/one/messages?from=latest", Some(t), None)
            .await;
        assert_eq!((v["items"].as_array().unwrap().len(), v["next"].as_u64()), (0, Some(2)));
        let (_, _, v) = node
            .call(
                Method::GET,
                "/v1/topics/one/messages?from=0&from_time=1970-01-01T00:00:00Z",
                Some(t),
                None,
            )
            .await;
        assert_eq!(v["error"]["code"], "validation_failed");
        let (_, _, v) = node
            .call(Method::GET, "/v1/topics/one/messages?partition=1", Some(t), None)
            .await;
        assert_eq!(v["error"]["code"], "validation_failed");

        node.call(
            Method::POST,
            "/v1/topics",
            Some(t),
            Some(json!({"name": "multi", "partitions": 2})),
        )
        .await;
        let (st, _, v) = node.call(Method::GET, "/v1/topics/multi/messages", Some(t), None).await;
        assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            v["error"]["message"].as_str().unwrap().contains("pass `partition`"),
            "{v}"
        );
        let (st, _, _) = node
            .call(Method::GET, "/v1/topics/multi/messages?partition=1", Some(t), None)
            .await;
        assert_eq!(st, StatusCode::OK);
        let (_, _, v) = node
            .call(
                Method::POST,
                "/v1/topics/multi/messages",
                Some(t),
                Some(json!({"records": []})),
            )
            .await;
        assert_eq!(v["error"]["code"], "validation_failed");
    }
}
