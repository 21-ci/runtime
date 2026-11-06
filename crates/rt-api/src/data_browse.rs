//! The data browser (R9): read a service's SQL rows, KV and blob listing under scope `data:read`.
//! No write path. The sandbox itself is `rt_store::browse`; here live the limits, the rendering and
//! the audit: every read leaves an event, and a read whose event cannot be written is refused, since
//! direct keys, `21ci` and MCP reach this route without passing through any console.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use parking_lot::Mutex;
use rt_store::{BrowseCell, BrowseKvEntry, BrowseLimits, BrowseResult, SqlStatement, SqlValue, StoreError};
use rt_types::{NodeEvent, Scope, Service, ServiceId, TenantId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use utoipa::{IntoParams, OpenApi, ToSchema};

use crate::auth::Auth;
use crate::control::load_service;
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::{ApiJson, ApiQuery};
use crate::state::ApiState;

/// Browse requests running at once, per tenant.
pub const TENANT_CONCURRENCY: usize = 2;
/// `POST …/data/query` per service per minute. Per service, not per key: a parent key can make up
/// to 200 child keys.
pub const QUERIES_PER_MINUTE: u32 = 10;
const RATE_WINDOW_MS: i64 = 60_000;
/// Over this many entries, the gate's maps drop what is idle. Every entry is a tenant or a service
/// that exists and was browsed, so this only bounds the scan.
const GATE_PRUNE_AT: usize = 4096;

const MAX_LIMIT: usize = 500;
const DEFAULT_LIMIT: usize = 100;
const MAX_BYTES: usize = 128 * 1024;
const MAX_CELL: usize = 64 * 1024;
const DEFAULT_CELL: usize = 4096;
const MAX_ARGS: usize = 100;
const PAGE_DEFAULT: usize = 50;
const PAGE_MAX: usize = 200;
/// Bytes of a value in a KV listing.
const KV_PREVIEW: usize = 128;
/// Bytes of a value from `GET …/data/kv/{key}`.
const KV_VALUE_MAX: usize = 128 * 1024;
/// Largest integer a JSON reader keeps exact.
const JS_SAFE: i64 = (1 << 53) - 1;

/// The concurrency and rate limits of the data browser, apart from invocation quotas.
#[derive(Default)]
pub struct DataBrowseGate {
    tenants: Mutex<HashMap<TenantId, Arc<Semaphore>>>,
    rates: Mutex<HashMap<ServiceId, (i64, u32)>>,
}

impl DataBrowseGate {
    fn enter(&self, tenant: TenantId) -> Result<OwnedSemaphorePermit, ApiError> {
        let sem = {
            let mut map = self.tenants.lock();
            if map.len() >= GATE_PRUNE_AT {
                map.retain(|_, s| Arc::strong_count(s) > 1);
            }
            map.entry(tenant)
                .or_insert_with(|| Arc::new(Semaphore::new(TENANT_CONCURRENCY)))
                .clone()
        };
        sem.try_acquire_owned().map_err(|_| busy(1))
    }

    fn count_query(&self, service: ServiceId, now_ms: i64) -> Result<(), ApiError> {
        let mut map = self.rates.lock();
        if map.len() >= GATE_PRUNE_AT {
            map.retain(|_, (start, _)| now_ms.saturating_sub(*start) < RATE_WINDOW_MS);
        }
        let (start, used) = map.entry(service).or_insert((now_ms, 0));
        if now_ms.saturating_sub(*start) >= RATE_WINDOW_MS {
            *start = now_ms;
            *used = 0;
        }
        if *used >= QUERIES_PER_MINUTE {
            let left = RATE_WINDOW_MS - now_ms.saturating_sub(*start);
            return Err(busy(u64::try_from(left / 1000).unwrap_or(1).max(1)));
        }
        *used += 1;
        Ok(())
    }
}

fn busy(retry_after: u64) -> ApiError {
    let mut e = ApiError::new("rate_limited", "too many data browser requests; try again shortly");
    e.retry_after = Some(retry_after);
    e
}

fn store_err(e: StoreError) -> ApiError {
    match e {
        StoreError::SqlResultTooLarge => ApiError::new("result_too_large", "the first row alone is over `max_bytes`"),
        other => other.into(),
    }
}

/// What this route never opens: the system tenant (operator SDKs) and the console's own service,
/// whose database holds sessions and sealed values.
fn refuse_reserved(s: &ApiState, svc: &Service) -> Result<(), ApiError> {
    let console = s.tenant_table.get(&svc.tenant_id).is_some_and(|t| t.plan == "rtweb");
    if rt_types::is_system_tenant(svc.tenant_id) || svc.slug.starts_with("sys-") || console {
        return Err(ApiError::new("reserved", "this service's data cannot be browsed"));
    }
    Ok(())
}

async fn open(s: &ApiState, a: &Auth, id: &str) -> Result<(Service, OwnedSemaphorePermit), ApiError> {
    a.require_scope(Scope::DataRead)?;
    let svc = load_service(s, a, id).await?;
    refuse_reserved(s, &svc)?;
    let permit = s.data_browse.enter(svc.tenant_id)?;
    Ok((svc, permit))
}

/// Write the read's audit event and wait for it: failing to is failing the read. Never values, args
/// or keys in clear: a hash, counts and the outcome.
async fn audit_read(s: &ApiState, a: &Auth, kind: &str, svc: &Service, data: Value) -> Result<(), ApiError> {
    let mut row = NodeEvent::new(kind, s.clock.unix_ms());
    row.tenant = Some(svc.tenant_id);
    row.service = Some(svc.id);
    row.data = json!({"actor": a.actor(), "data": data});
    s.meta.append_event(row).await.map_err(|e| {
        tracing::error!(error = %e, kind, "could not write a data browser audit event");
        ApiError::new("unavailable", "the read could not be audited, so it was refused")
    })
}

fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(b.len() * 2);
    for byte in b {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// One cell as JSON. The result is data: strings are never interpreted.
fn cell_json(c: BrowseCell) -> Value {
    match c {
        BrowseCell::Null => Value::Null,
        BrowseCell::Integer(i) if (-JS_SAFE..=JS_SAFE).contains(&i) => json!(i),
        BrowseCell::Integer(i) => Value::String(i.to_string()),
        BrowseCell::Real(r) if r.is_finite() => json!(r),
        BrowseCell::Real(r) if r.is_nan() => Value::String("NaN".into()),
        BrowseCell::Real(r) => Value::String(if r > 0.0 { "Infinity" } else { "-Infinity" }.into()),
        BrowseCell::Text { text, cut: false, .. } => Value::String(text),
        BrowseCell::Text { text, size, cut: true } => json!({"$text": text, "size": size, "truncated": true}),
        BrowseCell::Blob { head, size } => json!({"$blob": size, "hex": hex(&head)}),
    }
}

fn arg_of(v: &Value) -> Result<SqlValue, ApiError> {
    Ok(match v {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => SqlValue::Integer(i64::from(*b)),
        Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => SqlValue::Integer(i),
            (None, Some(f)) => SqlValue::Real(f),
            _ => return Err(ApiError::invalid("a number in `args` is out of range")),
        },
        Value::String(t) => SqlValue::Text(t.clone()),
        _ => return Err(ApiError::invalid("`args` holds null, booleans, numbers and strings")),
    })
}

fn bounded(name: &str, v: Option<u64>, default: usize, max: usize) -> Result<usize, ApiError> {
    match v {
        None => Ok(default),
        Some(n) if n >= 1 && n <= max as u64 => Ok(n as usize),
        Some(_) => Err(ApiError::invalid(format!("`{name}` is between 1 and {max}"))),
    }
}

// ---- DTOs -----------------------------------------------------------------------------------------

#[derive(Debug, Serialize, ToSchema)]
pub struct DataColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub decl_type: String,
    pub pk: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DataTable {
    pub name: String,
    /// `table` or `view`.
    #[serde(rename = "type")]
    pub kind: String,
    pub columns: Vec<DataColumn>,
    /// Taken without a scan: `sqlite_stat1`, else `MAX(rowid)`, else null.
    pub rows_estimate: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DataTables {
    pub items: Vec<DataTable>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DataQueryBody {
    /// One read-only statement that returns rows.
    pub sql: String,
    /// Bound parameters: null, booleans, numbers, strings.
    #[serde(default)]
    pub args: Vec<Value>,
    /// Rows, 1..500 (default 100).
    #[serde(default)]
    pub limit: Option<u64>,
    /// Result bytes, up to 131072 (the default).
    #[serde(default)]
    pub max_bytes: Option<u64>,
    /// Bytes kept of a text cell, up to 65536 (default 4096).
    #[serde(default)]
    pub cell_max: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DataQueryResult {
    pub columns: Vec<String>,
    /// Cells: JSON values; integers beyond 2^53 and non-finite reals as strings; `{"$blob": size,
    /// "hex": first 64 bytes}`; text over `cell_max` as `{"$text", "size", "truncated": true}`.
    pub rows: Vec<Vec<Value>>,
    /// More rows exist than fit in `limit` or `max_bytes`.
    pub truncated: bool,
    pub elapsed_ms: u64,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct DataPageQuery {
    /// Keys starting with this text.
    pub prefix: Option<String>,
    /// `next_cursor` of the previous page.
    pub cursor: Option<String>,
    /// 1..200 (default 50).
    pub limit: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DataKvItem {
    /// The key as text, when it is UTF-8; else null.
    pub key: Option<String>,
    /// The key, base64url: the path segment of `GET …/data/kv/{key}`.
    pub key_b64: String,
    /// `json` or `bytes`.
    #[serde(rename = "type")]
    pub kind: String,
    pub size: u64,
    pub expires_at: Option<i64>,
    /// The value's first 128 bytes: text for `json`, hex for `bytes`.
    pub preview: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DataKvPage {
    pub items: Vec<DataKvItem>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DataKvValue {
    pub key: Option<String>,
    pub key_b64: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub size: u64,
    pub expires_at: Option<i64>,
    /// At most 128 KiB: text for `json`, hex for `bytes`.
    pub value: String,
    pub truncated: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DataBlobItem {
    pub key: String,
    pub size: u64,
    pub content_type: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DataBlobPage {
    pub items: Vec<DataBlobItem>,
    pub next_cursor: Option<String>,
}

fn kv_kind(tag: i64) -> &'static str {
    if tag == 0 {
        "json"
    } else {
        "bytes"
    }
}

fn kv_text(e: &BrowseKvEntry) -> String {
    if e.tag == 0 {
        String::from_utf8_lossy(&e.head).into_owned()
    } else {
        hex(&e.head)
    }
}

fn kv_key_text(k: &[u8]) -> Option<String> {
    std::str::from_utf8(k).ok().map(str::to_string)
}

// ---- Handlers -------------------------------------------------------------------------------------

/// The service's tables and views.
#[utoipa::path(get, path = "/v1/services/{s}/data/tables", tag = "data",
    params(("s" = String, Path, description = "Service id")),
    responses((status = 200, body = DataTables), (status = 403, body = ErrorEnvelope),
        (status = 404, body = ErrorEnvelope), (status = 429, body = ErrorEnvelope)))]
pub async fn data_tables(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
) -> Result<Json<DataTables>, ApiError> {
    let (svc, _permit) = open(&s, &a, &id).await?;
    let tables = s.data.sql(svc.id).browse_tables().await.map_err(store_err)?;
    audit_read(
        &s,
        &a,
        "data.queried",
        &svc,
        json!({"what": "tables", "rows": tables.len()}),
    )
    .await?;
    Ok(Json(DataTables {
        items: tables
            .into_iter()
            .map(|t| DataTable {
                name: t.name,
                kind: t.kind,
                columns: t
                    .columns
                    .into_iter()
                    .map(|c| DataColumn {
                        name: c.name,
                        decl_type: c.decl_type,
                        pk: c.pk,
                    })
                    .collect(),
                rows_estimate: t.rows_estimate,
            })
            .collect(),
    }))
}

/// Run one read-only statement under the sandbox.
#[utoipa::path(post, path = "/v1/services/{s}/data/query", tag = "data", request_body = DataQueryBody,
    params(("s" = String, Path, description = "Service id")),
    responses((status = 200, body = DataQueryResult), (status = 403, body = ErrorEnvelope),
        (status = 404, body = ErrorEnvelope), (status = 413, body = ErrorEnvelope),
        (status = 422, body = ErrorEnvelope), (status = 429, body = ErrorEnvelope),
        (status = 503, body = ErrorEnvelope)))]
pub async fn data_query(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiJson(b): ApiJson<DataQueryBody>,
) -> Result<Json<DataQueryResult>, ApiError> {
    let (svc, _permit) = open(&s, &a, &id).await?;
    let limits = BrowseLimits {
        rows: bounded("limit", b.limit, DEFAULT_LIMIT, MAX_LIMIT)?,
        bytes: bounded("max_bytes", b.max_bytes, MAX_BYTES, MAX_BYTES)?,
        cell: bounded("cell_max", b.cell_max, DEFAULT_CELL, MAX_CELL)?,
        ..BrowseLimits::default()
    };
    if b.args.len() > MAX_ARGS {
        return Err(ApiError::invalid(format!("at most {MAX_ARGS} `args`")));
    }
    let args = b.args.iter().map(arg_of).collect::<Result<Vec<_>, _>>()?;
    s.data_browse.count_query(svc.id, s.clock.unix_ms())?;
    let hash = sha256_hex(b.sql.as_bytes());
    let run = s
        .data
        .sql(svc.id)
        .query_readonly(SqlStatement { sql: b.sql, args }, limits)
        .await;
    // A refused or failed attempt is on the record too.
    let (rows, outcome) = match &run {
        Ok(r) => (r.rows.len(), "ok"),
        Err(e) => (0, e.code()),
    };
    audit_read(
        &s,
        &a,
        "data.queried",
        &svc,
        json!({"what": "query", "sql_sha256": hash, "rows": rows, "outcome": outcome}),
    )
    .await?;
    let BrowseResult {
        columns,
        rows,
        truncated,
        elapsed_ms,
    } = run.map_err(store_err)?;
    Ok(Json(DataQueryResult {
        columns,
        rows: rows
            .into_iter()
            .map(|row| row.into_iter().map(cell_json).collect())
            .collect(),
        truncated,
        elapsed_ms,
    }))
}

fn cursor_bytes(c: Option<&str>) -> Result<Option<Vec<u8>>, ApiError> {
    c.map(|c| URL_SAFE_NO_PAD.decode(c).map_err(|_| ApiError::invalid("bad `cursor`")))
        .transpose()
}

/// A page of KV entries: sizes and previews, not whole values.
#[utoipa::path(get, path = "/v1/services/{s}/data/kv", tag = "data",
    params(("s" = String, Path, description = "Service id"), DataPageQuery),
    responses((status = 200, body = DataKvPage), (status = 403, body = ErrorEnvelope),
        (status = 404, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope),
        (status = 429, body = ErrorEnvelope)))]
pub async fn data_kv_list(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<DataPageQuery>,
) -> Result<Json<DataKvPage>, ApiError> {
    let (svc, _permit) = open(&s, &a, &id).await?;
    let limit = bounded("limit", q.limit, PAGE_DEFAULT, PAGE_MAX)?;
    let cursor = cursor_bytes(q.cursor.as_deref())?;
    let prefix = q.prefix.unwrap_or_default().into_bytes();
    let page = s
        .data
        .sql(svc.id)
        .browse_kv_list(prefix, cursor, limit, KV_PREVIEW)
        .await
        .map_err(store_err)?;
    audit_read(
        &s,
        &a,
        "data.kv_read",
        &svc,
        json!({"what": "list", "rows": page.items.len()}),
    )
    .await?;
    Ok(Json(DataKvPage {
        next_cursor: page.next_cursor.as_deref().map(|c| URL_SAFE_NO_PAD.encode(c)),
        items: page
            .items
            .iter()
            .map(|e| DataKvItem {
                key: kv_key_text(&e.key),
                key_b64: URL_SAFE_NO_PAD.encode(&e.key),
                kind: kv_kind(e.tag).into(),
                size: e.size,
                expires_at: e.expires_at,
                preview: kv_text(e),
            })
            .collect(),
    }))
}

/// One KV value, up to 128 KiB. The audit event has the key's hash, not the key.
#[utoipa::path(get, path = "/v1/services/{s}/data/kv/{key}", tag = "data",
    params(("s" = String, Path, description = "Service id"), ("key" = String, Path, description = "The key, base64url")),
    responses((status = 200, body = DataKvValue), (status = 403, body = ErrorEnvelope),
        (status = 404, body = ErrorEnvelope), (status = 429, body = ErrorEnvelope)))]
pub async fn data_kv_get(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, key)): Path<(String, String)>,
) -> Result<Json<DataKvValue>, ApiError> {
    let (svc, _permit) = open(&s, &a, &id).await?;
    let key = URL_SAFE_NO_PAD.decode(&key).map_err(|_| ApiError::not_found())?;
    let hash = sha256_hex(&key);
    let found = s
        .data
        .sql(svc.id)
        .browse_kv_get(key, KV_VALUE_MAX)
        .await
        .map_err(store_err)?;
    audit_read(
        &s,
        &a,
        "data.kv_read",
        &svc,
        json!({"what": "value", "key_sha256": hash, "rows": usize::from(found.is_some())}),
    )
    .await?;
    let e = found.ok_or_else(ApiError::not_found)?;
    Ok(Json(DataKvValue {
        key: kv_key_text(&e.key),
        key_b64: URL_SAFE_NO_PAD.encode(&e.key),
        kind: kv_kind(e.tag).into(),
        size: e.size,
        expires_at: e.expires_at,
        value: kv_text(&e),
        truncated: e.size > e.head.len() as u64,
    }))
}

/// A page of blob metadata; never the bytes.
#[utoipa::path(get, path = "/v1/services/{s}/data/blobs", tag = "data",
    params(("s" = String, Path, description = "Service id"), DataPageQuery),
    responses((status = 200, body = DataBlobPage), (status = 403, body = ErrorEnvelope),
        (status = 404, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope),
        (status = 429, body = ErrorEnvelope)))]
pub async fn data_blobs_list(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<DataPageQuery>,
) -> Result<Json<DataBlobPage>, ApiError> {
    let (svc, _permit) = open(&s, &a, &id).await?;
    let limit = bounded("limit", q.limit, PAGE_DEFAULT, PAGE_MAX)?;
    let page = s
        .data
        .blob(svc.id)
        .list(rt_store::BlobListOptions {
            prefix: q.prefix.unwrap_or_default(),
            cursor: q.cursor,
            limit,
        })
        .await
        .map_err(store_err)?;
    audit_read(
        &s,
        &a,
        "data.blobs_read",
        &svc,
        json!({"what": "list", "rows": page.items.len()}),
    )
    .await?;
    Ok(Json(DataBlobPage {
        next_cursor: page.next_cursor,
        items: page
            .items
            .into_iter()
            .map(|m| DataBlobItem {
                key: m.key,
                size: m.size,
                content_type: m.content_type,
            })
            .collect(),
    }))
}

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/services/{s}/data/tables", get(data_tables))
        .route("/v1/services/{s}/data/query", post(data_query))
        .route("/v1/services/{s}/data/kv", get(data_kv_list))
        .route("/v1/services/{s}/data/kv/{key}", get(data_kv_get))
        .route("/v1/services/{s}/data/blobs", get(data_blobs_list))
}

#[derive(OpenApi)]
#[openapi(
    paths(data_tables, data_query, data_kv_list, data_kv_get, data_blobs_list),
    components(schemas(
        DataColumn,
        DataTable,
        DataTables,
        DataQueryBody,
        DataQueryResult,
        DataKvItem,
        DataKvPage,
        DataKvValue,
        DataBlobItem,
        DataBlobPage
    ))
)]
pub struct Doc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cells_render_safely_for_json_readers() {
        assert_eq!(cell_json(BrowseCell::Integer(JS_SAFE)), json!(JS_SAFE));
        assert_eq!(cell_json(BrowseCell::Integer(JS_SAFE + 1)), json!("9007199254740992"));
        assert_eq!(cell_json(BrowseCell::Integer(-JS_SAFE - 1)), json!("-9007199254740992"));
        assert_eq!(cell_json(BrowseCell::Real(1.5)), json!(1.5));
        assert_eq!(cell_json(BrowseCell::Real(f64::INFINITY)), json!("Infinity"));
        assert_eq!(cell_json(BrowseCell::Real(f64::NEG_INFINITY)), json!("-Infinity"));
        assert_eq!(cell_json(BrowseCell::Real(f64::NAN)), json!("NaN"));
        assert_eq!(cell_json(BrowseCell::Null), Value::Null);
        assert_eq!(
            cell_json(BrowseCell::Text {
                text: "<b>".into(),
                size: 3,
                cut: false
            }),
            json!("<b>")
        );
        assert_eq!(
            cell_json(BrowseCell::Text {
                text: "ab".into(),
                size: 9,
                cut: true
            }),
            json!({"$text": "ab", "size": 9, "truncated": true})
        );
        assert_eq!(
            cell_json(BrowseCell::Blob {
                head: vec![0xab, 1],
                size: 70
            }),
            json!({"$blob": 70, "hex": "ab01"})
        );
    }

    #[test]
    fn the_rate_is_per_service_per_minute_and_recovers() {
        let g = DataBrowseGate::default();
        let (a, b) = (ServiceId::new(), ServiceId::new());
        for _ in 0..QUERIES_PER_MINUTE {
            g.count_query(a, 1000).unwrap();
        }
        let e = g.count_query(a, 1001).unwrap_err();
        assert_eq!(e.code, "rate_limited");
        assert_eq!(e.retry_after, Some(59));
        g.count_query(b, 1001).unwrap();
        g.count_query(a, 1000 + RATE_WINDOW_MS).unwrap();
    }

    #[test]
    fn a_tenant_has_two_slots_at_a_time() {
        let g = DataBrowseGate::default();
        let t = TenantId::new();
        let (one, _two) = (g.enter(t).unwrap(), g.enter(t).unwrap());
        assert_eq!(g.enter(t).unwrap_err().code, "rate_limited");
        let _other = g.enter(TenantId::new()).unwrap();
        drop(one);
        let _again = g.enter(t).unwrap();
    }

    #[test]
    fn args_take_scalars_only_and_limits_are_bounded() {
        assert_eq!(arg_of(&json!(true)).unwrap(), SqlValue::Integer(1));
        assert_eq!(arg_of(&json!(1.5)).unwrap(), SqlValue::Real(1.5));
        assert!(arg_of(&json!({"a": 1})).is_err());
        assert_eq!(bounded("limit", None, 100, 500).unwrap(), 100);
        assert!(bounded("limit", Some(0), 100, 500).is_err());
        assert!(bounded("limit", Some(501), 100, 500).is_err());
    }
}
