//! Consumer routes (`/v1/services/{s}/consumers…`: list, reset, dead letters) and
//! `POST /v1/pubsub/publish`.
//!
//! A consumer is a `(topic, group)` of a service: declared in its `events.consume` block, or left
//! over as cursors after the block changed. Topics resolve in the service's own namespace, so a
//! caller who may see the service may see its consumers; anything else is `404`.

use std::collections::{BTreeMap, BTreeSet};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use rt_store::{Cursor, ReadFrom, Topic};
use rt_types::events::{
    body_json, valid_group_name, validate_headers, validate_subject, Namespace, DEAD_MAX_PER_CONSUMER,
    EVENT_BODY_MAX_BYTES, TOPIC_ID_PREFIX,
};
use rt_types::{Scope, Service, TopicId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::auth::Auth;
use crate::control::{audit, load_service, rfc3339_ms};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::{ApiJson, ApiQuery};
use crate::projects::{check_environment_in_project, find_project, project_slug_of};
use crate::state::{ApiState, ControlEvent};

type R<T> = Result<Json<T>, ApiError>;

/// Default and largest page of `GET …/dlq`.
const DLQ_PAGE_DEFAULT: usize = 100;

// ---- DTOs ------------------------------------------------------------------------------------

/// One partition of a consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct ConsumerPartition {
    pub partition: u32,
    /// Next offset to deliver.
    pub committed: u64,
    /// Offset of the next append to the partition.
    pub next: u64,
    pub lag: u64,
    /// Failed deliveries of the record at `committed`.
    pub attempt: u16,
    /// `active` | `blocked`.
    pub state: String,
    pub last_error: Option<String>,
    /// Not before (unix ms); `0` = now.
    pub next_at: i64,
}

/// A `(topic, group)` consumer of a service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct ConsumerView {
    pub topic: String,
    pub group: String,
    /// In the service's `events.consume` block.
    pub declared: bool,
    pub topic_exists: bool,
    /// Dead letters of the consumer.
    pub dead: u64,
    pub partitions: Vec<ConsumerPartition>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ConsumerList {
    pub items: Vec<ConsumerView>,
}

/// A record that failed `max_attempts` deliveries.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DeadView {
    pub partition: u32,
    pub offset: u64,
    pub key: Option<String>,
    #[schema(value_type = Object)]
    pub headers: Value,
    /// JSON when the body parses as JSON, else a string.
    #[schema(value_type = Object)]
    pub body: Value,
    pub attempts: u16,
    pub error: String,
    pub failed_at: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DeadList {
    pub items: Vec<DeadView>,
}

/// Body of `POST /v1/services/{s}/consumers/{topic}/{group}/reset`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ResetBody {
    /// `"earliest"`, `"latest"`, an offset (integer) or an RFC 3339 timestamp.
    #[schema(value_type = Object)]
    pub to: Value,
    /// One partition; all when absent.
    pub partition: Option<u32>,
}

/// Body of `POST /v1/pubsub/publish`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PublishBody {
    pub subject: String,
    #[schema(value_type = Object)]
    pub body: Value,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Project slug or id (default `default`).
    pub project: Option<String>,
    /// Default `production`.
    pub environment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct Published {
    /// Subscribers the message was handed to.
    pub receivers: u32,
    /// Subscribers it was not delivered to (fan-out cap, quota, backlog, refused).
    pub dropped: u32,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct DlqQuery {
    /// At most this many (default 100, max 1000).
    pub limit: Option<u32>,
}

// ---- helpers ---------------------------------------------------------------------------------

/// The `to` of a reset: a keyword, an offset or an RFC 3339 time.
fn parse_to(v: &Value) -> Result<ReadFrom, ApiError> {
    match v {
        Value::String(s) if s == "earliest" => Ok(ReadFrom::Earliest),
        Value::String(s) if s == "latest" => Ok(ReadFrom::Latest),
        Value::String(s) => match s.parse::<u64>() {
            Ok(n) => Ok(ReadFrom::Offset(n)),
            Err(_) => rfc3339_ms(s).map(ReadFrom::Time).map_err(|_| bad_to()),
        },
        Value::Number(n) => n.as_u64().map(ReadFrom::Offset).ok_or_else(bad_to),
        _ => Err(bad_to()),
    }
}

fn bad_to() -> ApiError {
    ApiError::invalid("`to` must be \"earliest\", \"latest\", an offset or an RFC 3339 timestamp")
}

fn to_label(to: ReadFrom) -> Value {
    match to {
        ReadFrom::Earliest => json!("earliest"),
        ReadFrom::Latest => json!("latest"),
        ReadFrom::Offset(n) => json!(n),
        ReadFrom::Time(ms) => json!({ "time": ms }),
    }
}

fn ns_of(svc: &Service) -> Namespace {
    Namespace {
        tenant: svc.tenant_id,
        project: svc.project_id,
        environment: svc.environment.clone(),
    }
}

fn check_group(group: &str) -> Result<(), ApiError> {
    if valid_group_name(group) {
        Ok(())
    } else {
        Err(ApiError::invalid(format!(
            "`{group}` is not a valid consumer group name"
        )))
    }
}

/// The topic `t` (name or `tpc_` id) in the service's namespace, if it exists.
async fn topic_of(s: &ApiState, svc: &Service, t: &str) -> Result<Option<Topic>, ApiError> {
    let ns = ns_of(svc);
    if t.starts_with(TOPIC_ID_PREFIX) {
        let Ok(id) = t.parse::<TopicId>() else {
            return Ok(None);
        };
        return Ok(s.streams.topic_by_id(id).await?.filter(|x| x.ns == ns));
    }
    Ok(s.streams.get_topic(&ns, t).await?)
}

async fn require_topic(s: &ApiState, svc: &Service, t: &str) -> Result<Topic, ApiError> {
    topic_of(s, svc, t).await?.ok_or_else(ApiError::not_found)
}

/// The view of one consumer from its cursors (any order) and the topic, when it exists.
async fn consumer_view(
    s: &ApiState,
    svc: &Service,
    name: &str,
    group: &str,
    topic: Option<&Topic>,
    cursors: &[Cursor],
) -> Result<ConsumerView, ApiError> {
    let mut partitions = Vec::new();
    let mut dead = 0;
    if let Some(topic) = topic {
        let heads = s.streams.heads(topic.id).await?;
        let mut mine: Vec<&Cursor> = cursors
            .iter()
            .filter(|c| c.key.topic == topic.id && c.key.service == svc.id && c.key.group == group)
            .collect();
        mine.sort_by_key(|c| c.key.partition);
        for c in mine {
            let next = heads
                .iter()
                .find(|h| h.partition == c.key.partition)
                .map_or(c.committed, |h| h.next);
            partitions.push(ConsumerPartition {
                partition: c.key.partition,
                committed: c.committed,
                next,
                lag: next.saturating_sub(c.committed),
                attempt: c.attempt,
                state: if c.blocked { "blocked" } else { "active" }.to_string(),
                last_error: c.last_error.clone(),
                next_at: c.next_at_ms,
            });
        }
        dead = s.streams.dead_count(topic.id, svc.id, group).await?;
    }
    let declared = svc
        .config
        .events
        .consume
        .iter()
        .any(|c| c.topic == name && c.group == group);
    Ok(ConsumerView {
        topic: name.to_string(),
        group: group.to_string(),
        declared,
        topic_exists: topic.is_some(),
        dead,
        partitions,
    })
}

// ---- handlers --------------------------------------------------------------------------------

#[utoipa::path(get, path = "/v1/services/{s}/consumers", tag = "topics",
    params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = ConsumerList), (status = 404, body = ErrorEnvelope)))]
async fn consumers_list(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<ConsumerList> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let cursors = s.streams.cursors_of(svc.id).await?;
    // Declared consumers first (block order), then groups left over in cursors.
    let mut pairs: Vec<(String, String)> = svc
        .config
        .events
        .consume
        .iter()
        .map(|c| (c.topic.clone(), c.group.clone()))
        .collect();
    let mut topics: BTreeMap<TopicId, Option<Topic>> = BTreeMap::new();
    let mut extra = BTreeSet::new();
    for c in &cursors {
        if let std::collections::btree_map::Entry::Vacant(e) = topics.entry(c.key.topic) {
            e.insert(s.streams.topic_by_id(c.key.topic).await?);
        }
        if let Some(Some(t)) = topics.get(&c.key.topic) {
            let pair = (t.name.clone(), c.key.group.clone());
            if !pairs.contains(&pair) {
                extra.insert(pair);
            }
        }
    }
    pairs.extend(extra);
    let mut items = Vec::with_capacity(pairs.len());
    for (name, group) in pairs {
        let topic = topic_of(&s, &svc, &name).await?;
        items.push(consumer_view(&s, &svc, &name, &group, topic.as_ref(), &cursors).await?);
    }
    Ok(Json(ConsumerList { items }))
}

#[utoipa::path(post, path = "/v1/services/{s}/consumers/{topic}/{group}/reset", tag = "topics",
    params(("s" = String, Path, description = "service id"), ("topic" = String, Path, description = "topic name or id"),
        ("group" = String, Path, description = "consumer group")),
    request_body = ResetBody,
    responses((status = 200, body = ConsumerView), (status = 404, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
async fn consumers_reset(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, t, group)): Path<(String, String, String)>,
    ApiJson(b): ApiJson<ResetBody>,
) -> R<ConsumerView> {
    a.require_scope(Scope::QueueWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    check_group(&group)?;
    let to = parse_to(&b.to)?;
    let topic = require_topic(&s, &svc, &t).await?;
    if let Some(p) = b.partition {
        if p >= topic.partitions {
            return Err(ApiError::invalid(format!(
                "topic `{}` has {} partitions: `partition` {p} is out of range",
                topic.name, topic.partitions
            )));
        }
    }
    s.streams.reset(topic.id, svc.id, &group, b.partition, to).await?;
    s.node.notify(ControlEvent::ConsumersReset(svc.id));
    audit(
        &s,
        &a,
        "consumer.reset",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"topic": topic.name, "group": group, "partition": b.partition, "to": to_label(to)}),
    );
    let cursors = s.streams.cursors_on(topic.id).await?;
    Ok(Json(
        consumer_view(&s, &svc, &topic.name, &group, Some(&topic), &cursors).await?,
    ))
}

#[utoipa::path(get, path = "/v1/services/{s}/consumers/{topic}/{group}/dlq", tag = "topics",
    params(("s" = String, Path, description = "service id"), ("topic" = String, Path, description = "topic name or id"),
        ("group" = String, Path, description = "consumer group"), DlqQuery),
    responses((status = 200, body = DeadList), (status = 404, body = ErrorEnvelope)))]
async fn dlq_list(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, t, group)): Path<(String, String, String)>,
    ApiQuery(q): ApiQuery<DlqQuery>,
) -> R<DeadList> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    check_group(&group)?;
    let topic = require_topic(&s, &svc, &t).await?;
    let limit = q
        .limit
        .map_or(DLQ_PAGE_DEFAULT, |n| n as usize)
        .clamp(1, DEAD_MAX_PER_CONSUMER as usize);
    let items = s
        .streams
        .dead_list(topic.id, svc.id, &group, limit)
        .await?
        .into_iter()
        .map(|d| {
            let r = d.record.guest_json();
            DeadView {
                partition: d.record.partition,
                offset: d.record.offset,
                key: d.record.key.clone(),
                headers: r.get("headers").cloned().unwrap_or(Value::Null),
                body: body_json(&d.record.body),
                attempts: d.attempts,
                error: d.error,
                failed_at: d.failed_at_ms,
            }
        })
        .collect();
    Ok(Json(DeadList { items }))
}

#[utoipa::path(delete, path = "/v1/services/{s}/consumers/{topic}/{group}/dlq", tag = "topics",
    params(("s" = String, Path, description = "service id"), ("topic" = String, Path, description = "topic name or id"),
        ("group" = String, Path, description = "consumer group")),
    responses((status = 204), (status = 404, body = ErrorEnvelope)))]
async fn dlq_purge(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, t, group)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::QueueWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    check_group(&group)?;
    let topic = require_topic(&s, &svc, &t).await?;
    let n = s.streams.dead_purge(topic.id, svc.id, &group).await?;
    audit(
        &s,
        &a,
        "consumer.dlq_purged",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"topic": topic.name, "group": group, "purged": n}),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/v1/pubsub/publish", tag = "pubsub",
    request_body = PublishBody,
    responses((status = 200, body = Published), (status = 403, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope),
        (status = 429, body = ErrorEnvelope)))]
async fn pubsub_publish(State(s): State<ApiState>, a: Auth, ApiJson(b): ApiJson<PublishBody>) -> R<Published> {
    a.require_scope(Scope::QueueWrite)?;
    let tenant = a.tenant()?;
    validate_subject(&b.subject).map_err(ApiError::invalid)?;
    let headers: Vec<(String, String)> = b.headers.into_iter().collect();
    validate_headers(&headers).map_err(ApiError::invalid)?;
    let body = serde_json::to_vec(&b.body).map_err(|e| ApiError::invalid(e.to_string()))?;
    if body.len() > EVENT_BODY_MAX_BYTES {
        return Err(ApiError::new("too_large", "pub/sub messages are limited to 1 MiB"));
    }
    let environment = match b.environment.as_deref() {
        None => rt_types::default_environment(),
        Some(e) if rt_types::valid_environment(e) => e.to_string(),
        Some(e) => return Err(ApiError::invalid(format!("`{e}` is not a valid environment name"))),
    };
    let project = project_slug_of(&s, tenant, b.project.as_deref()).await?;
    check_environment_in_project(&s, tenant, &project, &environment).await?;
    let project_id = match find_project(&s, tenant, &project).await? {
        Some(p) => p.id,
        None => rt_types::default_project_id(tenant),
    };
    // A restricted key publishes only inside its projects and environments.
    if !a.principal.can_touch(project_id, &environment) {
        return Err(ApiError::new(
            "insufficient_scope",
            format!("this key may not publish in project `{project}`, environment `{environment}`"),
        ));
    }
    let ns = Namespace {
        tenant,
        project: project_id,
        environment,
    };
    let (receivers, dropped) = s
        .node
        .publish(&ns, &b.subject, Bytes::from(body), headers)
        .map_err(|(code, msg)| ApiError::new(code, msg))?;
    audit(
        &s,
        &a,
        "pubsub.published",
        Some(tenant),
        None,
        json!({"subject": b.subject, "receivers": receivers}),
    );
    Ok(Json(Published { receivers, dropped }))
}

/// Consumer routes and `POST /v1/pubsub/publish`.
pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/pubsub/publish", post(pubsub_publish))
        .route("/v1/services/{s}/consumers", get(consumers_list))
        .route(
            "/v1/services/{s}/consumers/{topic}/{group}/reset",
            post(consumers_reset),
        )
        .route(
            "/v1/services/{s}/consumers/{topic}/{group}/dlq",
            get(dlq_list).delete(dlq_purge),
        )
}

/// OpenAPI paths and schemas of this module; merged into `control::api_doc()`.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(consumers_list, consumers_reset, dlq_list, dlq_purge, pubsub_publish),
    components(schemas(
        ConsumerView,
        ConsumerPartition,
        ConsumerList,
        DeadView,
        DeadList,
        ResetBody,
        PublishBody,
        Published
    ))
)]
pub struct Doc;

#[cfg(test)]
mod tests {
    use super::*;

    /// `to` accepts the two keywords, an offset (number or numeric string) and an RFC 3339 time.
    #[test]
    fn reset_to_parses_keywords_offsets_and_times() {
        assert_eq!(parse_to(&json!("earliest")).unwrap(), ReadFrom::Earliest);
        assert_eq!(parse_to(&json!("latest")).unwrap(), ReadFrom::Latest);
        assert_eq!(parse_to(&json!(42)).unwrap(), ReadFrom::Offset(42));
        assert_eq!(parse_to(&json!("17")).unwrap(), ReadFrom::Offset(17));
        assert_eq!(
            parse_to(&json!("2023-11-14T22:13:20Z")).unwrap(),
            ReadFrom::Time(1_700_000_000_000)
        );
    }

    /// Anything else is `validation_failed`, never a server error.
    #[test]
    fn reset_to_refuses_other_values() {
        for v in [json!(-1), json!(1.5), json!("soon"), json!(null), json!([1]), json!({})] {
            assert_eq!(parse_to(&v).unwrap_err().code, "validation_failed", "{v}");
        }
    }

    /// The audit label keeps the shape of the request.
    #[test]
    fn reset_to_label() {
        assert_eq!(to_label(ReadFrom::Earliest), json!("earliest"));
        assert_eq!(to_label(ReadFrom::Latest), json!("latest"));
        assert_eq!(to_label(ReadFrom::Offset(3)), json!(3));
        assert_eq!(to_label(ReadFrom::Time(5)), json!({"time": 5}));
    }

    /// Bodies refuse unknown fields; publish headers default to none.
    #[test]
    fn bodies_parse() {
        let b: PublishBody = serde_json::from_value(json!({"subject": "a.b", "body": {"x": 1}})).unwrap();
        assert!(b.headers.is_empty() && b.project.is_none());
        assert!(serde_json::from_value::<PublishBody>(json!({"subject": "a", "body": 1, "x": 1})).is_err());
        let r: ResetBody = serde_json::from_value(json!({"to": "latest", "partition": 2})).unwrap();
        assert_eq!(r.partition, Some(2));
        assert!(serde_json::from_value::<ResetBody>(json!({"partition": 2})).is_err());
    }

    /// Group names follow the shared grammar.
    #[test]
    fn group_names_are_checked() {
        assert!(check_group("default").is_ok());
        assert_eq!(check_group("bad group!").unwrap_err().code, "validation_failed");
    }
}
