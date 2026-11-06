//! `POST /v1/services/{s}/versions/from-url`: rt fetches a deploy archive (R4).
//!
//! The URL is tenant-chosen and the response comes back as readable version source, so the fetch
//! obeys everything a guest's fetch obeys: the guard, the service's egress rules, the plan's stance
//! and the tenant's quota. The URL's query and the header values are credentials: they reach the
//! fetcher and nothing else, not the store, logs, events, errors or the idempotency fingerprint.
//! Userinfo in the URL is refused (`422`): the fetcher would not send it, so use
//! `headers.authorization`.
//!
//! A fetch is the time of a remote origin, which the tenant may control: it holds a [`FetchSlot`],
//! not one of the node's upload permits. Those are taken only once the bytes are here.
//!
//! The download may take minutes, so the answer does not wait for it: the version is created
//! `fetching` and `202` goes back before the first byte is asked for. The work runs on its own task,
//! which outlives the client; how it ended is on the version (`build_state`, `failure.code`). A
//! restart during a fetch turns the version `failed` at startup
//! (`MetaStore::fail_interrupted_fetches`).

use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use base64::Engine;
use bytes::Bytes;
use rt_sec::QuotaOp;
use rt_types::{
    ArchiveShape, BuildFailReason, BuildFailure, BuildState, RuntimeKind, Scope, Service, UsageDelta, ValidationError,
    VersionNo,
};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt;

use crate::auth::Auth;
use crate::control::{
    audit, fingerprint, finish_upload, idem_replay, idem_store, load_service, switch_actor, upload_compile, value_resp,
    version_view, UploadActivate, UploadOpts, MAX_UPLOAD_BODY,
};
use crate::dto::{FromUrlBody, VersionView};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiJson;
use crate::state::{ApiState, ArchiveFetchError, ArchiveFetchFailure, ArchiveRequest};

/// Base64 JSON of a version's `meta` on `POST …/versions`.
pub(crate) const VERSION_META_HEADER: &str = "x-rt-version-meta";

const MAX_META_BYTES: usize = 4096;
const MAX_URL_BYTES: usize = 8192;
const MAX_HEADERS: usize = 16;
const MAX_HEADER_VALUE_BYTES: usize = 8192;
const MAX_STRIP_COMPONENTS: u32 = 16;
const MAX_PATH_ARG_BYTES: usize = 1024;
const MAX_FETCHES_PER_TENANT: usize = 2;
const MAX_FETCHES_ON_NODE: usize = 32;

/// Hop-by-hop and routing headers a caller may not set on the fetch.
const REFUSED_HEADERS: &[&str] = &[
    "host",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
];

/// A string field that is short, one line and free of control characters.
fn check_text(field: &str, v: &Value, max: usize) -> Result<(), ApiError> {
    let text = v
        .as_str()
        .ok_or_else(|| ApiError::invalid(format!("meta: `{field}` must be a string")))?;
    if text.len() > max || (field != "git.message" && text.chars().any(char::is_control)) {
        return Err(ApiError::invalid(format!(
            "meta: `{field}` is too long or has control characters"
        )));
    }
    Ok(())
}

/// The shape of a version's `meta`. It is display data, but it must stay small and must not become
/// a place for a credential: a `repo` URL with userinfo is refused.
pub(crate) fn check_version_meta(meta: &Value) -> Result<(), ApiError> {
    let top = meta
        .as_object()
        .ok_or_else(|| ApiError::invalid("meta must be an object"))?;
    if serde_json::to_vec(meta).map_or(usize::MAX, |b| b.len()) > MAX_META_BYTES {
        return Err(ApiError::invalid(format!("meta is limited to {MAX_META_BYTES} bytes")));
    }
    for (key, value) in top {
        match key.as_str() {
            "label" => check_text("label", value, 256)?,
            "git" => {
                let git = value
                    .as_object()
                    .ok_or_else(|| ApiError::invalid("meta: `git` must be an object"))?;
                for (k, v) in git {
                    match k.as_str() {
                        "sha" => {
                            let sha = v.as_str().unwrap_or_default();
                            let hex = sha.bytes().all(|b| b.is_ascii_hexdigit());
                            if !hex || !(7..=64).contains(&sha.len()) {
                                return Err(ApiError::invalid("meta: `git.sha` must be 7 to 64 hex digits"));
                            }
                        }
                        "ref" => check_text("git.ref", v, 256)?,
                        "repo" => {
                            check_text("git.repo", v, 512)?;
                            if v.as_str().is_some_and(|r| r.contains("://") && r.contains('@')) {
                                return Err(ApiError::invalid("meta: `git.repo` must not carry credentials"));
                            }
                        }
                        "author" => check_text("git.author", v, 256)?,
                        "message" => check_text("git.message", v, 1024)?,
                        other => return Err(ApiError::invalid(format!("meta: unknown key `git.{other}`"))),
                    }
                }
            }
            other => return Err(ApiError::invalid(format!("meta: unknown key `{other}`"))),
        }
    }
    Ok(())
}

/// The `x-rt-version-meta` header of an upload: base64 JSON.
pub(crate) fn version_meta_header(h: &HeaderMap) -> Result<Option<Value>, ApiError> {
    let Some(raw) = h.get(VERSION_META_HEADER) else {
        return Ok(None);
    };
    let meta = base64::engine::general_purpose::STANDARD
        .decode(raw.as_bytes())
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .ok_or_else(|| ApiError::invalid("x-rt-version-meta must be base64 of a JSON object"))?;
    check_version_meta(&meta)?;
    Ok(Some(meta))
}

/// `scheme://host[:port]/path` of a parsed URL: what may be fingerprinted and audited.
fn public_part(url: &url::Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(p) => format!("{}://{host}:{p}{}", url.scheme(), url.path()),
        None => format!("{}://{host}{}", url.scheme(), url.path()),
    }
}

/// Header names and values; refusals name the header, never its value.
fn checked_headers(raw: &std::collections::BTreeMap<String, String>) -> Result<Vec<(String, String)>, ApiError> {
    if raw.len() > MAX_HEADERS {
        return Err(ApiError::invalid(format!("at most {MAX_HEADERS} headers")));
    }
    let mut out = Vec::new();
    for (name, value) in raw {
        let lower = name.to_ascii_lowercase();
        if HeaderName::from_bytes(lower.as_bytes()).is_err() {
            return Err(ApiError::invalid("a header name is not valid"));
        }
        if REFUSED_HEADERS.contains(&lower.as_str()) || lower.starts_with("x-rt-") {
            return Err(ApiError::invalid(format!("header `{lower}` cannot be set")));
        }
        if value.len() > MAX_HEADER_VALUE_BYTES || HeaderValue::from_str(value).is_err() {
            return Err(ApiError::invalid(format!("the value of header `{lower}` is not valid")));
        }
        out.push((lower, value.clone()));
    }
    Ok(out)
}

/// 64 lowercase hex digits from `sha256:<hex>` or `<hex>`.
fn checked_sha256(raw: &str) -> Result<String, ApiError> {
    let hex = raw.strip_prefix("sha256:").unwrap_or(raw).to_ascii_lowercase();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ApiError::invalid("`sha256` must be 64 hex digits"));
    }
    Ok(hex)
}

fn fetch_error(f: ArchiveFetchFailure) -> ApiError {
    match f.error {
        ArchiveFetchError::Denied => ApiError::new("egress_denied", "the egress rules refuse this URL"),
        ArchiveFetchError::TooLarge => {
            ApiError::new("too_large", format!("archives are limited to {MAX_UPLOAD_BODY} bytes"))
        }
        ArchiveFetchError::Quota => ApiError::new("egress_quota", "the daily egress quota is used up"),
        ArchiveFetchError::Failed { status } => {
            let message = match status {
                Some(code) => format!("the archive could not be fetched: the origin answered {code}"),
                None => "the archive could not be fetched".to_string(),
            };
            ApiError::new("fetch_failed", message)
        }
    }
}

fn validation_error(e: ValidationError) -> ApiError {
    match e {
        ValidationError::Invalid(d) => {
            ApiError::new("validation_failed", "the archive failed validation").with_details(json!({"diagnostics": d}))
        }
        ValidationError::InvalidArtifact(m) => ApiError::new("invalid_artifact", m),
        ValidationError::TooLarge(m) => ApiError::new("too_large", m),
    }
}

/// What a fetch that went wrong leaves on its version. `message` is ours: never the URL, a header or
/// driver text.
fn failure_of(e: &ApiError) -> BuildFailure {
    let reason = match e.code {
        "fetch_failed" => BuildFailReason::FetchFailed,
        "egress_denied" => BuildFailReason::EgressDenied,
        "egress_quota" => BuildFailReason::EgressQuota,
        "too_large" => BuildFailReason::TooLarge,
        "validation_failed" => BuildFailReason::ValidationFailed,
        "invalid_artifact" => BuildFailReason::InvalidArtifact,
        _ => BuildFailReason::Internal,
    };
    let mut failure = BuildFailure::new(reason, e.message.clone());
    if let Some(d) = e.details.get("diagnostics") {
        failure.diagnostics = serde_json::from_value(d.clone()).unwrap_or_default();
    }
    failure
}

/// Fetches in flight, per tenant and on the node. An entry goes with its last slot.
#[derive(Default)]
pub struct FetchGate {
    tenants: parking_lot::Mutex<std::collections::HashMap<rt_types::TenantId, usize>>,
}

/// A place in the [`FetchGate`], given back on drop.
struct FetchSlot(std::sync::Arc<FetchGate>, rt_types::TenantId);

impl FetchGate {
    fn enter(self: &std::sync::Arc<Self>, tenant: rt_types::TenantId) -> Result<FetchSlot, ApiError> {
        let mut tenants = self.tenants.lock();
        let total: usize = tenants.values().sum();
        let mine = tenants.get(&tenant).copied().unwrap_or(0);
        if mine >= MAX_FETCHES_PER_TENANT || total >= MAX_FETCHES_ON_NODE {
            return Err(ApiError::new(
                "rate_limited",
                "too many deploys from a URL in progress; retry shortly",
            ));
        }
        tenants.insert(tenant, mine + 1);
        Ok(FetchSlot(self.clone(), tenant))
    }
}

impl Drop for FetchSlot {
    fn drop(&mut self) {
        let mut tenants = self.0.tenants.lock();
        if let Some(n) = tenants.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                tenants.remove(&self.1);
            }
        }
    }
}

/// One deploy from a URL, after the answer went out. No `Debug`: the request holds credentials.
struct FetchJob {
    s: ApiState,
    a: Auth,
    svc: Service,
    request: ArchiveRequest,
    want_sha: Option<String>,
    shape: ArchiveShape,
    opts: UploadOpts,
    _slot: FetchSlot,
}

/// Fetch, reshape and hand over to the path of an upload.
async fn fetch_and_deploy(job: FetchJob) -> Result<(), ApiError> {
    let FetchJob {
        s,
        a,
        svc,
        request,
        want_sha,
        shape,
        opts,
        _slot,
    } = job;
    let fetched = s.archives.fetch(request).await;
    let charge = |received: u64| {
        if received > 0 {
            let delta = UsageDelta {
                egress_bytes: received,
                ..UsageDelta::default()
            };
            s.quotas.record(svc.tenant_id, svc.id, delta);
        }
    };
    let mut fetched = match fetched {
        Ok(f) => {
            charge(f.received);
            f
        }
        Err(e) => {
            charge(e.received);
            return Err(fetch_error(e));
        }
    };

    // From here on it is an upload like any other. The job is detached, so it can wait its turn.
    let _permit = s
        .uploads
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::new("draining", "the node is shutting down"))?;
    // Into memory only under a reservation for its real size, as uploads are.
    let owner = rt_mem::Owner {
        tenant: Some(svc.tenant_id),
        ..rt_mem::Owner::NONE
    };
    let _mem = s
        .mem
        .reserve(rt_mem::MemCategory::IoBuffers, fetched.len, owner)
        .map_err(|_| {
            ApiError::new(
                "rate_limited",
                "the node is short of memory for this archive; deploy again",
            )
        })?;
    let mut raw = Vec::with_capacity(fetched.len as usize);
    fetched
        .file
        .read_to_end(&mut raw)
        .await
        .map_err(|_| ApiError::new("fetch_failed", "the archive could not be fetched"))?;
    drop(fetched);
    let raw = Bytes::from(raw);
    if let Some(want) = &want_sha {
        let got = hex_of(&Sha256::digest(&raw));
        if &got != want {
            return Err(ApiError::invalid("the archive does not match `sha256`"));
        }
    }
    let (content_type, source) = if svc.runtime == RuntimeKind::Wasm {
        ("application/wasm", raw)
    } else {
        // Inflating is CPU work bounded by the upload limits; keep it off the async workers.
        let validator = s.validator.clone();
        let reshaped = tokio::task::spawn_blocking(move || validator.reshape_archive(&raw, &shape))
            .await
            .map_err(|_| ApiError::new("validation_failed", "the archive failed validation"))?;
        ("application/x-tar", reshaped.map_err(validation_error)?)
    };
    // No idempotency here: the `202` was stored when it was sent.
    finish_upload(&s, &a, &HeaderMap::new(), &svc, [0; 32], content_type, source, opts)
        .await
        .map(drop)
}

/// The version says how it went; an `after_build` activation that can no longer happen says so too.
async fn fetch_failed(s: &ApiState, a: &Auth, svc: &Service, number: VersionNo, host: Option<String>, e: &ApiError) {
    let failure = failure_of(e);
    let code = failure.reason.as_str();
    // Not found: the service was deleted during the fetch, and there is nothing to report on.
    if s.meta
        .set_build_state(svc.id, number, BuildState::Failed, Some(failure.clone()), None)
        .await
        .is_err()
    {
        return;
    }
    if matches!(s.meta.take_version_activation(svc.id, number).await, Ok(Some(_))) {
        s.promoter.emit_migration(
            "service.promote_failed",
            svc,
            json!({"version": number.0, "code": code}),
        );
    }
    audit(
        s,
        a,
        "deploy.failed",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"version": number.0, "code": code, "source_host": host}),
    );
}

/// Start deploying an archive as a new version and answer `202` with it, `fetching`. No build steps
/// run: rt bundles what the archive contains.
#[utoipa::path(post, path = "/v1/services/{s}/versions/from-url", tag = "versions",
    params(("s" = String, Path, description = "service id")),
    request_body = FromUrlBody,
    responses((status = 202, description = "the version, `fetching` (a replay: as it was when first answered). Poll `GET /v1/services/{s}/versions/{v}`: `build_state` and `failure.code`", body = VersionView),
        (status = 403, body = ErrorEnvelope), (status = 404, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope),
        (status = 429, body = ErrorEnvelope)))]
async fn versions_from_url(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
    ApiJson(b): ApiJson<FromUrlBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::Deploy)?;
    let svc = load_service(&s, &a, &id).await?;

    if b.url.len() > MAX_URL_BYTES {
        return Err(ApiError::invalid("`url` is too long"));
    }
    let url = url::Url::parse(&b.url).map_err(|_| ApiError::invalid("`url` is not a valid URL"))?;
    if url.host_str().is_none() {
        return Err(ApiError::invalid("`url` has no host"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ApiError::invalid(
            "`url` holds a username or password, which is not sent: use `headers.authorization`",
        ));
    }
    let headers = checked_headers(&b.headers)?;
    let want_sha = b.sha256.as_deref().map(checked_sha256).transpose()?;
    let strip_components = b.strip_components.unwrap_or(0);
    if strip_components > MAX_STRIP_COMPONENTS {
        return Err(ApiError::invalid(format!(
            "`strip_components` is at most {MAX_STRIP_COMPONENTS}"
        )));
    }
    for (name, v) in [("subdir", &b.subdir), ("entry", &b.entry)] {
        if v.as_ref().is_some_and(|v| v.len() > MAX_PATH_ARG_BYTES) {
            return Err(ApiError::invalid(format!("`{name}` is too long")));
        }
    }
    let activate = match b.activate.as_deref() {
        None => UploadActivate::No,
        Some("after_build") => UploadActivate::AfterBuild,
        Some(_) => return Err(ApiError::invalid("`activate` is `after_build` or absent")),
    };
    if let Some(meta) = &b.meta {
        check_version_meta(meta)?;
    }
    if svc.runtime == RuntimeKind::Wasm && (strip_components != 0 || b.subdir.is_some() || b.entry.is_some()) {
        return Err(ApiError::invalid(
            "`strip_components`, `subdir` and `entry` apply to archives; a WASM service takes one component",
        ));
    }

    // Header values and the URL's query and userinfo stay out: they are credentials, and a signed
    // URL that was re-signed is the same deploy.
    let public = public_part(&url);
    let names: Vec<&str> = headers.iter().map(|(n, _)| n.as_str()).collect();
    let meta_text = b.meta.as_ref().map(Value::to_string).unwrap_or_default();
    let strip_text = strip_components.to_string();
    let fp = fingerprint(&[
        b"versions-from-url",
        svc.id.to_string().as_bytes(),
        public.as_bytes(),
        names.join(",").as_bytes(),
        want_sha.as_deref().unwrap_or("").as_bytes(),
        strip_text.as_bytes(),
        b.subdir.as_deref().unwrap_or("").as_bytes(),
        b.entry.as_deref().unwrap_or("").as_bytes(),
        meta_text.as_bytes(),
        b.compile.as_deref().unwrap_or("").as_bytes(),
        b.activate.as_deref().unwrap_or("").as_bytes(),
    ]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }

    // Refuse what would be refused anyway before there is a version, and before a build is charged.
    let slot = s.fetches.enter(svc.tenant_id)?;
    s.quotas.check(svc.tenant_id, QuotaOp::Build)?;
    upload_compile(&s, &svc, activate, false, b.compile.as_deref())?;
    let mut version = s.meta.reserve_fetching_version(svc.id).await?;
    let after_build = activate == UploadActivate::AfterBuild;
    if b.meta.is_some() || after_build {
        s.meta
            .set_version_deploy(
                svc.id,
                version.number,
                b.meta.clone(),
                after_build.then(|| switch_actor(&a)),
            )
            .await?;
        version.meta.clone_from(&b.meta);
    }
    let view = serde_json::to_value(version_view(&version)).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::ACCEPTED, &view);

    let number = version.number;
    let source_host = url.host_str().map(str::to_string);
    let job = FetchJob {
        s: s.clone(),
        a: a.clone(),
        svc: svc.clone(),
        request: ArchiveRequest {
            url: b.url.clone(),
            headers,
            tenant: svc.tenant_id,
            rules: svc.config.egress.clone(),
            default_deny: s.plans.plan_arc(svc.tenant_id).egress_default == rt_types::EgressDefault::Deny,
            quota_left: s.quotas.egress_left(svc.tenant_id),
        },
        want_sha,
        shape: ArchiveShape {
            strip_components,
            subdir: b.subdir.clone(),
            entry: b.entry.clone(),
        },
        opts: UploadOpts {
            activate,
            close_sockets: false,
            compile: b.compile.clone(),
            meta: b.meta.clone(),
            source_host: source_host.clone(),
            reserved: Some(number),
            // Charged above, before the version existed: a deploy is one build token.
            build_charged: true,
        },
        _slot: slot,
    };
    // Detached, and watched: even a panic must not leave the version `fetching`.
    let (st, au) = (s.clone(), a.clone());
    tokio::spawn(async move {
        let outcome = tokio::spawn(fetch_and_deploy(job))
            .await
            .unwrap_or_else(|_| Err(ApiError::new("internal", "the deploy stopped unexpectedly")));
        if let Err(e) = outcome {
            fetch_failed(&st, &au, &svc, number, source_host, &e).await;
        }
    });
    Ok(value_resp(StatusCode::ACCEPTED, &view))
}

fn hex_of(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

#[derive(utoipa::OpenApi)]
#[openapi(paths(versions_from_url), components(schemas(FromUrlBody)))]
pub(crate) struct Doc;

pub(crate) fn routes() -> Router<ApiState> {
    Router::new().route("/v1/services/{s}/versions/from-url", post(versions_from_url))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_public_part_of_a_url_has_no_query_and_no_userinfo() {
        let url = url::Url::parse("https://me:hunter2@git.example:8443/a/b.tar.gz?token=s3cret#x").unwrap();
        assert_eq!(public_part(&url), "https://git.example:8443/a/b.tar.gz");
    }

    #[test]
    fn meta_keeps_its_shape_and_refuses_credentials() {
        assert!(
            check_version_meta(&json!({"git": {"sha": "abc1234", "ref": "refs/heads/main"}, "label": "x"})).is_ok()
        );
        for bad in [
            json!("x"),
            json!({"other": 1}),
            json!({"git": {"sha": "not hex!"}}),
            json!({"git": {"token": "x"}}),
            json!({"git": {"repo": "https://me:tok@github.com/a/b"}}),
            json!({"label": "x".repeat(MAX_META_BYTES)}),
        ] {
            assert!(check_version_meta(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_refused_header_is_named_without_its_value() {
        for name in [
            "Host",
            "x-rt-tenant",
            "Connection",
            "transfer-encoding",
            "Proxy-Authorization",
        ] {
            let raw = std::collections::BTreeMap::from([(name.to_string(), "s3cret".to_string())]);
            let e = checked_headers(&raw).expect_err(name);
            assert!(!e.message.contains("s3cret"), "{}", e.message);
        }
        let ok = std::collections::BTreeMap::from([("PRIVATE-TOKEN".to_string(), "t".to_string())]);
        assert_eq!(
            checked_headers(&ok).unwrap(),
            vec![("private-token".to_string(), "t".to_string())]
        );
    }
}
