//! App export, import and fork (R12). `/v1/services/{s}/bundle*` and `/v1/services/import`.
//!
//! A bundle is hostile input whoever made it, including one this node just made for a fork: every
//! import goes through `rt_backup::app::read_bundle`, and then through the same handlers a client
//! would call. `tenant`, `project` and `environment` never come from a manifest. Versions are built
//! again from source; no compiled artifact is ever imported.

use std::collections::{HashMap, HashSet};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use parking_lot::Mutex;
use rt_backup::app::{self, AppManifest, AppService, AppVersion, ReadLimits, StagedBundle};
use rt_backup::BackupError;
use rt_sec::QuotaOp;
use rt_store::TenantScope;
use rt_types::{Access, Scope, Service, TenantId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use utoipa::{IntoParams, OpenApi, ToSchema};

use crate::auth::Auth;
use crate::control::{
    audit, fingerprint, finish_upload, idem_replay, idem_store, is_system_service, load_service, refresh_routes,
    remove_service, schedules_create, services_create, sid, truthy, value_resp, UploadActivate, UploadOpts,
    MAX_UPLOAD_BODY,
};
use crate::dto::{CreateServiceBody, ScheduleBody};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::{ApiJson, ApiQuery};
use crate::state::{ApiState, ControlEvent};

/// How long a finished bundle stays.
const BUNDLE_TTL_MS: i64 = 60 * 60 * 1000;
/// Bundle records kept (running or waiting to expire). Over it: `429 heavy_job_busy`.
const MAX_JOBS: usize = 256;
/// Running or finished bundles one tenant holds on disk. Over it: `429 heavy_job_busy`.
const MAX_TENANT_BUNDLES: usize = 2;
/// Free disk an import's spool leaves.
const SPOOL_DISK_FLOOR: u64 = 64 << 20;
const BUNDLE_TYPE: &str = "application/zstd";

#[derive(Clone)]
struct Job {
    tenant: TenantId,
    service: rt_types::ServiceId,
    state: &'static str,
    file: Option<String>,
    bytes: Option<u64>,
    sha256: Option<String>,
    expires_at: i64,
    failure: Option<Value>,
}

/// Bundles of this node: in memory and under `dir`. A restart forgets them and removes the files.
pub struct AppBundles {
    dir: PathBuf,
    jobs: Mutex<HashMap<String, Job>>,
    /// One export or import per tenant, as the heavy-job rule says.
    busy: Mutex<HashSet<TenantId>>,
}

impl AppBundles {
    pub fn new(dir: PathBuf) -> Arc<Self> {
        // What a previous run left: nothing refers to it any more.
        let _ = std::fs::remove_dir_all(&dir);
        Arc::new(Self {
            dir,
            jobs: Mutex::new(HashMap::new()),
            busy: Mutex::new(HashSet::new()),
        })
    }

    fn turn(self: &Arc<Self>, tenant: TenantId) -> Result<Turn, ApiError> {
        if !self.busy.lock().insert(tenant) {
            return Err(ApiError::new(
                "heavy_job_busy",
                "this tenant already has an export or import running",
            ));
        }
        Ok(Turn(self.clone(), tenant))
    }

    /// Drop the expired, their files with them.
    fn sweep(&self, now: i64) {
        let expired: Vec<Job> = {
            let mut jobs = self.jobs.lock();
            let ids: Vec<String> = jobs
                .iter()
                .filter(|(_, j)| j.state != "running" && j.expires_at <= now)
                .map(|(id, _)| id.clone())
                .collect();
            ids.iter().filter_map(|id| jobs.remove(id)).collect()
        };
        for j in expired {
            if let Some(f) = j.file {
                let _ = std::fs::remove_file(self.dir.join(f));
            }
        }
    }
}

struct Turn(Arc<AppBundles>, TenantId);

impl Drop for Turn {
    fn drop(&mut self) {
        self.0.busy.lock().remove(&self.1);
    }
}

/// A private directory that goes away with its value.
struct Scratch(PathBuf);

impl Scratch {
    async fn new(root: &FsPath) -> Result<Self, ApiError> {
        let p = root.join(format!("work-{}", ulid::Ulid::new()));
        tokio::fs::create_dir_all(&p).await.map_err(internal)?;
        tokio::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(internal)?;
        Ok(Self(p))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn internal(_: impl std::fmt::Display) -> ApiError {
    ApiError::new("internal", "the bundle could not be processed")
}

fn bundle_error(e: BackupError) -> ApiError {
    match e.code {
        "too_large" => ApiError::new("too_large", e.message),
        "insufficient_storage" => ApiError::new("insufficient_storage", e.message),
        // Messages of these codes are this crate's own and name no host path; an I/O or driver
        // text stays out of the answer.
        "unsafe_entry" | "bad_manifest" | "corrupt" | "schema_too_new" => {
            ApiError::new("bundle_invalid", e.message).with_details(json!({"reason": e.code}))
        }
        _ => {
            ApiError::new("bundle_invalid", "this bundle cannot be read").with_details(json!({"reason": "unreadable"}))
        }
    }
}

/// What a client sees of an export while it runs and when it failed.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BundleView {
    pub id: String,
    /// `running`, `ready` or `failed`.
    pub state: String,
    pub bytes: Option<u64>,
    pub sha256: Option<String>,
    pub expires_at: Option<i64>,
    #[schema(value_type = Option<Object>)]
    pub failure: Option<Value>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ImportQuery {
    /// The new service's slug. Leaving it out is a `validation_failed`, like any other bad slug.
    #[serde(default)]
    #[param(required = false)]
    pub slug: String,
    /// Project slug or id (default `default`). Never taken from the bundle.
    pub project: Option<String>,
    /// Environment (default `production`). Never taken from the bundle.
    pub environment: Option<String>,
    /// Make the bundle's live version live when its build is ready (default false).
    pub activate: Option<String>,
    /// Fork: a service id on this node, instead of a request body.
    pub from: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ImportStarted {
    pub id: String,
    pub service_id: String,
    /// `missing_secrets` when the bundle's secret names have no values here yet.
    pub warnings: Vec<String>,
    /// To enter again with `PUT …/secrets/{name}`.
    pub missing_secrets: Vec<String>,
    /// Builds run in the background: follow `GET …/versions`.
    pub versions: usize,
}

fn app_service(svc: &Service) -> Result<AppService, ApiError> {
    let Value::Object(mut c) = serde_json::to_value(&svc.config).map_err(internal)? else {
        return Err(internal("config"));
    };
    Ok(AppService {
        runtime: svc.runtime.to_string(),
        name: svc.name.clone(),
        profile: svc
            .profile
            .and_then(|p| serde_json::to_value(p).ok())
            .and_then(|v| v.as_str().map(str::to_string)),
        access: match svc.config.access {
            Access::Public => Some("public".to_string()),
            Access::Key => Some("key".to_string()),
            Access::Internal => Some("internal".to_string()),
        },
        durable: svc.config.durable.then_some(true),
        limits: c.remove("limits"),
        scaling: c.remove("scaling"),
        triggers: c.remove("triggers"),
        ingress: c.remove("ingress"),
        egress: c.remove("egress"),
        call: c.remove("call"),
        wasm: c.remove("wasm"),
        deno: c.remove("deno"),
        ws: c.remove("ws"),
        events: c.remove("events"),
        async_invoke: c.remove("async"),
    })
}

/// Write the bundle of `svc` to `out`, through a work directory under `root`.
async fn build_bundle(s: &ApiState, svc: &Service, root: &FsPath, out: PathBuf) -> Result<app::AppFile, ApiError> {
    let work = Scratch::new(root).await?;
    let w = work.0.clone();
    let sources = w.join("sources");
    tokio::fs::create_dir_all(&sources).await.map_err(internal)?;
    let mut all = s.meta.list_versions(svc.id).await?;
    // A version whose fetch is running or failed has no source to carry.
    all.retain(rt_types::Version::has_source);
    all.sort_by_key(|v| v.number.0);
    // `deploy.retain_versions` may be set above the format's cap: the newest go, and the live one.
    let keep_from = all.len().saturating_sub(app::MAX_VERSIONS - 1);
    let mut versions = Vec::new();
    for (i, v) in all.iter().enumerate() {
        let live = svc.live_version == Some(v.number);
        if i < keep_from && !live {
            continue;
        }
        let bytes = s.artifacts.get(&v.source_digest).await?;
        tokio::fs::write(sources.join(v.number.0.to_string()), &bytes)
            .await
            .map_err(internal)?;
        let content_type = s
            .artifacts
            .content_type(&v.source_digest)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| "application/octet-stream".to_string());
        versions.push(AppVersion {
            number: v.number.0,
            size: 0,
            sha256: String::new(),
            content_type,
            live,
            meta: v.meta.clone(),
        });
    }
    let schedules = s
        .meta
        .list_schedules(svc.id)
        .await?
        .into_iter()
        .map(|sch| {
            let mut o = serde_json::to_value(&sch.kind).unwrap_or(Value::Null);
            o["payload"] = sch.payload;
            o["enabled"] = json!(sch.enabled);
            o
        })
        .collect();
    let manifest = AppManifest {
        format: app::FORMAT,
        rt_version: env!("CARGO_PKG_VERSION").to_string(),
        exported_at: s.clock.unix_ms(),
        slug: svc.slug.clone(),
        service: app_service(svc)?,
        env: s.meta.get_env(svc.id).await?.into_iter().collect(),
        secret_names: s.meta.list_secrets(svc.id).await?.into_iter().map(|m| m.name).collect(),
        schedules,
        versions,
        data: app::AppFile::default(),
        blobs: Vec::new(),
    };
    s.data
        .sql(svc.id)
        .snapshot_into(w.join("data.db"), w.join("objects"))
        .await?;
    tokio::task::spawn_blocking(move || {
        let _work = work;
        app::write_bundle(&w, manifest, &out).map_err(|_| internal("write"))
    })
    .await
    .map_err(internal)?
}

fn refuse_system(svc: &Service) -> Result<(), ApiError> {
    if is_system_service(svc) {
        return Err(ApiError::new("reserved", "a system service does not move"));
    }
    Ok(())
}

/// Start building a bundle: it holds the data, the env values and the sources. Poll
/// `GET …/bundle/{id}`.
#[utoipa::path(post, path = "/v1/services/{s}/bundle", tag = "services",
    params(("s" = String, Path, description = "service id")),
    responses((status = 202, body = BundleView), (status = 403, body = ErrorEnvelope), (status = 404, body = ErrorEnvelope),
        (status = 429, description = "`heavy_job_busy`", body = ErrorEnvelope)))]
async fn bundle_create(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> Result<Response, ApiError> {
    a.require_scope(Scope::DataAdmin)?;
    let svc = load_service(&s, &a, &id).await?;
    refuse_system(&svc)?;
    let now = s.clock.unix_ms();
    s.app_bundles.sweep(now);
    let turn = s.app_bundles.turn(svc.tenant_id)?;
    let bundle_id = format!("bdl_{}", ulid::Ulid::new());
    {
        let mut jobs = s.app_bundles.jobs.lock();
        if jobs.len() >= MAX_JOBS {
            return Err(ApiError::new(
                "heavy_job_busy",
                "too many bundles on this node; retry later",
            ));
        }
        let held = jobs
            .values()
            .filter(|j| j.tenant == svc.tenant_id && j.state != "failed")
            .count();
        if held >= MAX_TENANT_BUNDLES {
            return Err(ApiError::new(
                "heavy_job_busy",
                "this tenant holds too many bundles; they expire an hour after they finish",
            ));
        }
        jobs.insert(
            bundle_id.clone(),
            Job {
                tenant: svc.tenant_id,
                service: svc.id,
                state: "running",
                file: None,
                bytes: None,
                sha256: None,
                expires_at: now + BUNDLE_TTL_MS,
                failure: None,
            },
        );
    }
    audit(
        &s,
        &a,
        "app.exported",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"bundle": bundle_id}),
    );
    let (st, bid) = (s.clone(), bundle_id.clone());
    tokio::spawn(async move {
        let _turn = turn;
        let root = st.app_bundles.dir.clone();
        let file = format!("{bid}.tar.zst");
        let out = root.join(&file);
        let res = match tokio::fs::create_dir_all(&root).await {
            Ok(()) => build_bundle(&st, &svc, &root, out).await,
            Err(e) => Err(internal(e)),
        };
        let mut jobs = st.app_bundles.jobs.lock();
        if let Some(j) = jobs.get_mut(&bid) {
            j.expires_at = st.clock.unix_ms() + BUNDLE_TTL_MS;
            match res {
                Ok(f) => {
                    j.state = "ready";
                    j.file = Some(file);
                    j.bytes = Some(f.size);
                    j.sha256 = Some(f.sha256);
                }
                Err(e) => {
                    j.state = "failed";
                    j.failure = Some(json!({"code": e.code, "message": e.message}));
                }
            }
        }
    });
    let view = BundleView {
        id: bundle_id,
        state: "running".into(),
        bytes: None,
        sha256: None,
        expires_at: Some(now + BUNDLE_TTL_MS),
        failure: None,
    };
    Ok((StatusCode::ACCEPTED, Json(view)).into_response())
}

/// The state as JSON until the bundle is ready; then the bundle, with `Range`.
#[utoipa::path(get, path = "/v1/services/{s}/bundle/{id}", tag = "services",
    params(("s" = String, Path, description = "service id"), ("id" = String, Path, description = "bundle id")),
    responses((status = 200, description = "the state (JSON) or the bundle", body = BundleView),
        (status = 206, description = "a range of the bundle", content_type = "application/zstd"),
        (status = 404, body = ErrorEnvelope)))]
async fn bundle_get(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, bundle)): Path<(String, String)>,
    h: HeaderMap,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::DataAdmin)?;
    let svc = load_service(&s, &a, &id).await?;
    let range = match crate::file_response::parse_range(&h) {
        Ok(r) => r,
        Err(refused) => return Ok(refused),
    };
    s.app_bundles.sweep(s.clock.unix_ms());
    let job = s
        .app_bundles
        .jobs
        .lock()
        .get(&bundle)
        .filter(|j| j.service == svc.id && j.tenant == svc.tenant_id)
        .cloned()
        .ok_or_else(ApiError::not_found)?;
    let (Some(file), "ready") = (job.file.clone(), job.state) else {
        let view = BundleView {
            id: bundle,
            state: job.state.to_string(),
            bytes: job.bytes,
            sha256: job.sha256,
            expires_at: Some(job.expires_at),
            failure: job.failure,
        };
        return Ok(Json(view).into_response());
    };
    let path = s.app_bundles.dir.join(file);
    let size = tokio::fs::metadata(&path)
        .await
        .map_err(|_| ApiError::not_found())?
        .len();
    if range.is_none_or(|r| r.starts_at_zero()) {
        audit(
            &s,
            &a,
            "app.downloaded",
            Some(svc.tenant_id),
            Some(svc.id),
            json!({"bundle": bundle, "bytes": size}),
        );
    }
    let mut resp =
        crate::file_response::file_response(&path, size, BUNDLE_TYPE, job.sha256.as_deref(), range, ()).await?;
    let name = format!(
        "attachment; filename=\"app-{}-{}.tar.zst\"",
        svc.slug,
        s.clock.unix_ms()
    );
    if let Ok(v) = HeaderValue::from_str(&name) {
        resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
    }
    Ok(resp)
}

/// Before a byte is written: the body as declared, or the cap when it declares nothing, must leave
/// [`SPOOL_DISK_FLOOR`] free, so that an import cannot starve `meta.db`'s WAL. Unknown free space
/// passes, as the unpack check does.
fn check_spool_room(free: Option<u64>, declared: Option<u64>, cap: u64) -> Result<(), ApiError> {
    let need = declared.unwrap_or(cap).saturating_add(SPOOL_DISK_FLOOR);
    if free.is_some_and(|f| need > f) {
        return Err(ApiError::new(
            "insufficient_storage",
            "not enough free disk to take this bundle",
        ));
    }
    Ok(())
}

/// Counted as it arrives: `Content-Length` is only a claim.
async fn spool(body: Body, path: &FsPath, cap: u64) -> Result<(), ApiError> {
    let mut f = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .await
        .map_err(internal)?;
    let mut stream = body.into_data_stream();
    let mut read = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ApiError::invalid("the upload was interrupted"))?;
        read = read.saturating_add(chunk.len() as u64);
        if read > cap {
            return Err(ApiError::new(
                "too_large",
                format!("bundles are limited to {cap} bytes"),
            ));
        }
        f.write_all(&chunk).await.map_err(internal)?;
    }
    f.flush().await.map_err(internal)?;
    Ok(())
}

fn hash_file(path: &FsPath) -> std::io::Result<Vec<u8>> {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    std::io::copy(&mut std::fs::File::open(path)?, &mut h)?;
    Ok(h.finalize().to_vec())
}

fn create_body(q: &ImportQuery, m: &AppManifest) -> CreateServiceBody {
    let v = m.service.clone();
    CreateServiceBody {
        slug: q.slug.clone(),
        runtime: v.runtime,
        name: v.name,
        profile: v.profile,
        limits: v.limits,
        scaling: v.scaling,
        access: v.access,
        triggers: v.triggers,
        ingress: v.ingress,
        egress: v.egress,
        project: q.project.clone(),
        environment: q.environment.clone(),
        call: v.call,
        wasm: v.wasm,
        deno: v.deno,
        durable: v.durable,
        ws: v.ws,
        // A bundle's manifest cannot turn on publishing: that takes `data:read` and a `PUT`.
        events: v.events.map(|mut e| {
            if let Some(o) = e.as_object_mut() {
                o.remove("data_events");
            }
            e
        }),
        async_invoke: v.async_invoke,
    }
}

/// Env, data, schedules, versions: each as a client would, so plan, trigger and Wasm policy checks
/// all apply. A failure takes the service back out in the caller.
async fn fill_service(
    s: &ApiState,
    a: &Auth,
    svc: &Service,
    staged: &StagedBundle,
    activate: bool,
) -> Result<(), ApiError> {
    let m = &staged.manifest;
    s.meta
        .put_env(svc.id, m.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .await?;
    s.node.notify(ControlEvent::SecretsChanged(svc.id));
    s.data
        .sql(svc.id)
        .install_staged(staged.db.clone(), staged.objects.clone())
        .await?;
    for sch in &m.schedules {
        let body: ScheduleBody = serde_json::from_value(sch.clone())
            .map_err(|_| ApiError::new("bundle_invalid", "a schedule in the bundle is not valid"))?;
        schedules_create(
            State(s.clone()),
            a.clone(),
            Path(svc.id.to_string()),
            HeaderMap::new(),
            ApiJson(body),
        )
        .await?;
    }
    for v in &m.versions {
        if v.size > MAX_UPLOAD_BODY as u64 {
            return Err(ApiError::new(
                "too_large",
                "a version's source is over the upload limit",
            ));
        }
        if let Some(meta) = &v.meta {
            crate::from_url::check_version_meta(meta)?;
        }
        let bytes = tokio::fs::read(staged.sources.join(v.number.to_string()))
            .await
            .map_err(internal)?;
        let after_build = activate && v.live;
        let opts = UploadOpts {
            activate: if after_build {
                UploadActivate::AfterBuild
            } else {
                UploadActivate::No
            },
            close_sockets: false,
            compile: Some(if after_build { "eager" } else { "lazy" }.to_string()),
            meta: v.meta.clone(),
            source_host: None,
            reserved: None,
            build_charged: true,
        };
        // Validates the source, runs the Wasm policy and recomputes its digest.
        finish_upload(
            s,
            a,
            &HeaderMap::new(),
            svc,
            [0; 32],
            &v.content_type,
            bytes.into(),
            opts,
        )
        .await?;
    }
    Ok(())
}

/// What a crash left of imports: their services go, data with them. At startup, before the listeners.
pub async fn sweep_interrupted_imports(s: &ApiState) -> Result<u64, ApiError> {
    let mut swept = 0;
    for (import, service) in s.meta.open_imports().await? {
        if let Some(id) = service {
            if let Some(svc) = s.meta.get_service(TenantScope::Operator, id).await? {
                remove_service(s, TenantScope::Operator, &svc, false).await?;
                swept += 1;
            }
        }
        s.meta.import_end(import).await?;
    }
    Ok(swept)
}

/// The note first: from its commit on, a crash takes the service back out at the next start.
async fn import_staged(s: ApiState, a: Auth, q: ImportQuery, staged: StagedBundle) -> Result<ImportStarted, ApiError> {
    let tenant = a.tenant()?;
    let note = s.meta.import_begin(tenant, q.slug.clone()).await?;
    let done = import_noted(s.clone(), a, q, staged, &note).await;
    if let Err(e) = s.meta.import_end(note).await {
        tracing::warn!(error = %e, "import note not cleared");
    }
    done
}

async fn import_noted(
    s: ApiState,
    a: Auth,
    q: ImportQuery,
    staged: StagedBundle,
    note: &str,
) -> Result<ImportStarted, ApiError> {
    let tenant = a.tenant()?;
    s.quotas.check(
        tenant,
        QuotaOp::Storage {
            delta_bytes: staged.unpacked_data_bytes(),
        },
    )?;
    // Every version is a build: taken here, before a service exists or data is unpacked into one.
    for _ in &staged.manifest.versions {
        s.quotas.check(tenant, QuotaOp::Build)?;
    }
    let resp = services_create(
        State(s.clone()),
        a.clone(),
        HeaderMap::new(),
        ApiJson(create_body(&q, &staged.manifest)),
    )
    .await?;
    let created = axum::body::to_bytes(resp.into_body(), crate::extract::MAX_JSON_BODY)
        .await
        .map_err(internal)?;
    let created: Value = serde_json::from_slice(&created).map_err(internal)?;
    let id = sid(created["id"].as_str().unwrap_or_default())?;
    s.meta.import_bind(note.to_string(), id).await?;
    let svc = s.meta.get_service(a.scope, id).await?.ok_or_else(ApiError::not_found)?;
    let activate = truthy(q.activate.as_deref());
    if let Err(e) = fill_service(&s, &a, &svc, &staged, activate).await {
        let _ = remove_service(&s, a.scope, &svc, false).await;
        return Err(e);
    }
    refresh_routes(&s).await?;
    let mut missing = staged.manifest.secret_names.clone();
    missing.sort();
    let warnings = if missing.is_empty() {
        Vec::new()
    } else {
        vec!["missing_secrets".to_string()]
    };
    // Names and counts: env values and data stay out of the audit trail.
    audit(
        &s,
        &a,
        "app.imported",
        Some(tenant),
        Some(svc.id),
        json!({
            "from": q.from,
            "versions": staged.manifest.versions.len(),
            "env_names": staged.manifest.env.keys().collect::<Vec<_>>(),
            "missing_secrets": missing,
        }),
    );
    if !staged.manifest.env.is_empty() {
        // The env trail has no gap for a write that came in a bundle.
        audit(
            &s,
            &a,
            "env.written",
            Some(tenant),
            Some(svc.id),
            json!({"names": staged.manifest.env.keys().collect::<Vec<_>>()}),
        );
    }
    Ok(ImportStarted {
        id: format!("imp_{}", ulid::Ulid::new()),
        service_id: svc.id.to_string(),
        warnings,
        missing_secrets: missing,
        versions: staged.manifest.versions.len(),
    })
}

/// A fork's source. A key sees its own tenant's services, `404` otherwise; only the operator's
/// token reaches another tenant's.
async fn fork_source(s: &ApiState, a: &Auth, from: &str) -> Result<Service, ApiError> {
    if a.require_operator().is_ok() {
        return s
            .meta
            .get_service(TenantScope::Operator, sid(from)?)
            .await?
            .ok_or_else(ApiError::not_found);
    }
    load_service(s, a, from).await
}

/// Create a service from a bundle (the body) or from a service on this node (`?from=`). Everything
/// about it is checked before the service exists; an import that fails leaves none.
#[utoipa::path(post, path = "/v1/services/import", tag = "services", params(ImportQuery),
    request_body(content = Vec<u8>, content_type = "application/zstd"),
    responses((status = 202, body = ImportStarted),
        (status = 403, body = ErrorEnvelope), (status = 404, description = "`?from=` names no service you may see", body = ErrorEnvelope),
        (status = 409, description = "`slug_taken`", body = ErrorEnvelope),
        (status = 413, description = "`too_large`", body = ErrorEnvelope),
        (status = 422, description = "`bundle_invalid`", body = ErrorEnvelope),
        (status = 429, description = "`heavy_job_busy`", body = ErrorEnvelope)))]
async fn services_import(
    State(s): State<ApiState>,
    a: Auth,
    ApiQuery(q): ApiQuery<ImportQuery>,
    h: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    a.require_scope(Scope::Deploy)?;
    a.require_scope(Scope::DataAdmin)?;
    let tenant = a.tenant()?;
    if !crate::control::valid_slug(&q.slug) {
        return Err(ApiError::invalid("`slug` must match ^[a-z][a-z0-9-]{1,62}$"));
    }
    let cap = u64::from(s.config.load().import.max_bundle_mb).saturating_mul(1 << 20);
    let declared = h
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    // A bundle that cannot fit the plan's storage is not worth spooling.
    let plan_bytes = s.plans.plan_for(tenant).storage_mb.saturating_mul(1 << 20);
    let spool_cap = if plan_bytes > 0 { cap.min(plan_bytes) } else { cap };
    if q.from.is_none() && declared.is_some_and(|n| n > spool_cap) {
        return Err(ApiError::new(
            "too_large",
            format!("bundles are limited to {spool_cap} bytes"),
        ));
    }
    let source = match q.from.as_deref() {
        Some(from) => Some(fork_source(&s, &a, from).await?),
        None => None,
    };
    if let Some(src) = &source {
        refuse_system(src)?;
    }
    let turn = s.app_bundles.turn(tenant)?;
    let root = s.app_bundles.dir.clone();
    tokio::fs::create_dir_all(&root).await.map_err(internal)?;
    let work = Scratch::new(&root).await?;
    let bundle = work.0.join("in.tar.zst");
    match &source {
        Some(src) => {
            build_bundle(&s, src, &work.0, bundle.clone()).await?;
        }
        None => {
            check_spool_room(rt_store::free_bytes(&root), declared, spool_cap)?;
            spool(body, &bundle, spool_cap).await?;
        }
    }
    let digest = {
        let p = bundle.clone();
        tokio::task::spawn_blocking(move || hash_file(&p))
            .await
            .map_err(internal)?
            .map_err(internal)?
    };
    let fp = fingerprint(&[
        b"import",
        q.slug.as_bytes(),
        q.project.as_deref().unwrap_or("").as_bytes(),
        q.environment.as_deref().unwrap_or("").as_bytes(),
        q.activate.as_deref().unwrap_or("").as_bytes(),
        q.from.as_deref().unwrap_or("").as_bytes(),
        // A fork's bundle differs every time; the request is what repeats.
        if source.is_some() { b"" } else { &digest[..] },
    ]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }

    let staging = work.0.join("staged");
    let unpack = tokio::task::spawn_blocking(move || {
        let limits = ReadLimits {
            max_unpacked: cap.saturating_mul(8),
        };
        app::read_bundle(&bundle, &staging, limits)
    })
    .await
    .map_err(internal)?;
    let staged = match unpack {
        Ok(st) => st,
        Err(e) => {
            audit(
                &s,
                &a,
                "app.import_refused",
                Some(tenant),
                None,
                json!({"slug": q.slug, "reason": e.code}),
            );
            return Err(bundle_error(e));
        }
    };
    // What the bundle writes takes the scope of the route that writes it. Schedules take
    // `services:write`, asked for above.
    if !staged.manifest.env.is_empty() {
        a.require_scope(Scope::EnvWrite)?;
    }
    // On its own task, so that a client that goes away cannot leave half a service behind.
    let (st, au) = (s.clone(), a.clone());
    let done = tokio::spawn(async move {
        let _keep = (work, turn);
        import_staged(st, au, q, staged).await
    })
    .await
    .map_err(internal)??;
    let view = serde_json::to_value(&done).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::ACCEPTED, &view);
    Ok(value_resp(StatusCode::ACCEPTED, &view))
}

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/services/import", post(services_import))
        .route("/v1/services/{s}/bundle", post(bundle_create))
        .route("/v1/services/{s}/bundle/{id}", get(bundle_get))
}

#[derive(OpenApi)]
#[openapi(
    paths(bundle_create, bundle_get, services_import),
    components(schemas(BundleView, ImportStarted))
)]
pub struct Doc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spool_leaves_the_floor_free() {
        let (floor, cap) = (SPOOL_DISK_FLOOR, 4 << 30);
        assert!(check_spool_room(Some(floor + 10), Some(10), cap).is_ok());
        let e = check_spool_room(Some(floor + 9), Some(10), cap).unwrap_err();
        assert_eq!(e.code, "insufficient_storage");
        // Nothing declared: the cap is what may come.
        assert!(check_spool_room(Some(floor + 10), None, cap).is_err());
        assert!(check_spool_room(None, None, cap).is_ok());
    }
}
