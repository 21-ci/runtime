//! `meta.db`: tenants, keys, services, versions, env, sealed secrets, hosts, schedules, usage,
//! events, cache frequencies.
//!
//! Other crates use [`MetaStore`], never SQL. Migrations are forward-only, embedded in the
//! binary and checked at startup ([`MIGRATIONS`]). SQLite settings: WAL, `synchronous=NORMAL`,
//! prepared-statement cache (RP-07). Cross-tenant lookups through a tenant-scoped
//! [`TenantScope`] behave as `NotFound`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use rt_types::{
    ApiKey, BuildFailure, BuildState, CompileMode, Digest, Etag, EventId, HostMapping, HostStatus, KeyId, NewKey,
    NewProject, NewSchedule, NewService, NewTenant, NewVersion, NodeEvent, Profile, Project, ProjectId, ProjectPatch,
    RouteEntry, RouteSnapshot, Schedule, ScheduleId, ScheduleKind, SecretsVersion, Service, ServiceConfig, ServiceId,
    ServicePatch, Tenant, TenantId, UnixMs, UsageBatch, Version, VersionNo,
};
use rt_types::{
    GrantEffect, GrantScope, NewSdkRelease, Sdk, SdkChannel, SdkGrant, SdkRelease, SdkSnapshot, SdkState, SdkTarget,
};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};

use crate::error::{Result, StoreError};
use crate::sqlutil::{
    bad_col, is_unique_violation, json_col, migrate, now_ms, open_reader, page_limit, paginate, parse_col, to_json, Db,
};

/// Forward-only migrations: `(version, sql)`. The newest version is the schema version.
pub const MIGRATIONS: &[(u32, &str)] = &[(1, include_str!("../migrations/meta_0001_init.sql"))];

/// Whose view a call has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantScope {
    /// Operator (root token): any tenant. `x-rt-tenant` narrows to `Tenant`.
    Operator,
    Tenant(TenantId),
}

/// Cursor pagination (`?limit=50&cursor=…`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageReq {
    pub limit: u32,
    /// Opaque; from a previous `next_cursor`.
    pub cursor: Option<String>,
}

impl Default for PageReq {
    fn default() -> Self {
        Self {
            limit: 50,
            cursor: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TenantPatch {
    pub name: Option<String>,
    pub plan: Option<String>,
    pub overrides: Option<serde_json::Value>,
    pub disabled: Option<bool>,
}

/// Filters for `list_services`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServiceFilter {
    /// Exact slug match (0 or 1 result).
    pub slug: Option<String>,
    /// Services of this project, by slug or `prj_` id.
    pub project: Option<String>,
    /// Services in this environment.
    pub environment: Option<String>,
    /// A key's scope: only services of these projects (`None` = any project).
    pub project_ids: Option<Vec<ProjectId>>,
    /// A key's scope: only services in these environments (`None` = any environment).
    pub environments: Option<Vec<String>>,
}

/// A tenant-wide bound on warm floors: `scaling.min_instances` summed over the tenant's services,
/// each counted at most `per_service` (what the scheduler keeps under the plan's `max_instances`),
/// stays within `total`. Checked in the transaction that writes the service
/// ([`MetaStore::create_service_capped`], [`MetaStore::patch_service_capped`]), so two requests
/// at once cannot both fit. A write that sets no floor (0) always fits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WarmFloorCap {
    /// 0 = no cap.
    pub total: u32,
    /// 0 = no per-service bound.
    pub per_service: u32,
}

impl WarmFloorCap {
    pub const NONE: WarmFloorCap = WarmFloorCap {
        total: 0,
        per_service: 0,
    };

    /// What a service with floor `min` counts.
    pub fn clamp(&self, min: u32) -> u32 {
        if self.per_service == 0 {
            min
        } else {
            min.min(self.per_service)
        }
    }
}

/// `StoreError::WarmFloorTotal` unless `tenant`'s floors (with `svc`'s replaced by `min`) fit `cap`.
fn check_warm_floor_total(
    c: &Connection,
    tenant: TenantId,
    svc: Option<ServiceId>,
    min: u32,
    cap: WarmFloorCap,
) -> Result<()> {
    if cap.total == 0 || min == 0 {
        return Ok(());
    }
    let mut total = u64::from(cap.clamp(min));
    let mut stmt = c.prepare_cached(
        "SELECT id, COALESCE(json_extract(config, '$.scaling.min_instances'), 0) FROM services WHERE tenant_id = ?1",
    )?;
    let mut rows = stmt.query([tenant.to_string()])?;
    let except = svc.map(|s| s.to_string());
    while let Some(r) = rows.next()? {
        let id: String = r.get(0)?;
        if except.as_deref() == Some(id.as_str()) {
            continue;
        }
        let m: i64 = r.get(1)?;
        total += u64::from(cap.clamp(u32::try_from(m.max(0)).unwrap_or(u32::MAX)));
    }
    if total > u64::from(cap.total) {
        return Err(StoreError::WarmFloorTotal { total, cap: cap.total });
    }
    Ok(())
}

/// A tenant's pending custom-host claim that ended (deleted or expired), remembered so that a new
/// claim of the same host by the same tenant cannot restart the verification TTL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostRelease {
    pub host: String,
    pub tenant: TenantId,
    /// When the released claim was first made.
    pub created_at_ms: UnixMs,
    pub verify_token: Option<String>,
    pub released_at_ms: UnixMs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretMeta {
    pub name: String,
    pub updated_at_ms: UnixMs,
}

/// One `usage` row (per tenant, service, UTC day).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRow {
    pub tenant: TenantId,
    pub service: ServiceId,
    /// `YYYY-MM-DD` (UTC).
    pub day: String,
    pub invocations: u64,
    pub cpu_ms: u64,
    pub egress_bytes: u64,
    pub storage_bytes: i64,
}

#[async_trait]
pub trait MetaStore: Send + Sync {
    /// Applied schema version.
    async fn schema_version(&self) -> Result<u32>;

    // ---- tenants & keys ----------------------------------------------------------------------
    async fn create_tenant(&self, t: NewTenant) -> Result<Tenant>;
    async fn get_tenant(&self, id: TenantId) -> Result<Option<Tenant>>;
    async fn list_tenants(&self, page: PageReq) -> Result<Page<Tenant>>;
    async fn update_tenant(&self, id: TenantId, patch: TenantPatch) -> Result<Tenant>;
    /// `purge` also deletes the tenant's services, data and keys (`DELETE …?purge=1`).
    async fn delete_tenant(&self, id: TenantId, purge: bool) -> Result<()>;

    /// Persist a key issued by `rt-sec` (`KeyHasher::issue`).
    async fn create_key(&self, tenant: TenantId, k: NewKey) -> Result<ApiKey>;
    async fn find_key(&self, id: KeyId) -> Result<Option<ApiKey>>;
    async fn list_keys(&self, tenant: TenantId) -> Result<Vec<ApiKey>>;
    /// Mark revoked; `false` if it did not exist for this tenant.
    async fn revoke_key(&self, tenant: TenantId, id: KeyId) -> Result<bool>;
    async fn touch_key(&self, id: KeyId, at_ms: UnixMs) -> Result<()>;
    /// Persist a key minted by the key `parent` (`POST /v1/keys`), so revoking `parent` revokes it.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn create_child_key(&self, tenant: TenantId, k: NewKey, parent: Option<KeyId>) -> Result<ApiKey> {
        let _ = parent;
        self.create_key(tenant, k).await
    }
    // LCOV_EXCL_STOP
    /// [`MetaStore::create_child_key`] unless `tenant` already holds `max_live` keys that are
    /// neither revoked nor expired at `now_ms` (`Conflict("quota_exceeded")`): counted in the
    /// transaction that inserts, so requests at once cannot overshoot the cap.
    async fn create_child_key_capped(
        &self,
        tenant: TenantId,
        k: NewKey,
        parent: Option<KeyId>,
        max_live: u64,
        now_ms: UnixMs,
    ) -> Result<ApiKey>;
    /// Keys of `tenant` that are neither revoked nor expired at `now_ms`.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn count_live_keys(&self, tenant: TenantId, now_ms: UnixMs) -> Result<u64> {
        let keys = self.list_keys(tenant).await?;
        Ok(keys
            .iter()
            .filter(|k| !k.revoked && k.expires_at_ms.is_none_or(|at| at > now_ms))
            .count() as u64)
    }
    // LCOV_EXCL_STOP
    /// Revoke `id` and every not-yet-revoked key it minted, transitively, in one transaction.
    /// Returns the keys revoked (`id` first); empty when `id` is not a live key of `tenant`.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn revoke_key_tree(&self, tenant: TenantId, id: KeyId) -> Result<Vec<KeyId>> {
        Ok(if self.revoke_key(tenant, id).await? {
            vec![id]
        } else {
            Vec::new()
        })
    }
    // LCOV_EXCL_STOP
    /// Which of `ids` are projects of `tenant` (a key's project list is checked against this).
    async fn tenant_projects_among(&self, tenant: TenantId, ids: &[ProjectId]) -> Result<Vec<ProjectId>>;

    // ---- projects (D-B3) ---------------------------------------------------------------------
    /// Every tenant has a `default` project (created with the tenant, and again on demand when it
    /// was deleted while empty). `Conflict("project_slug_taken")` for a slug in use by the tenant,
    /// `Conflict("quota_exceeded")` at [`rt_types::MAX_PROJECTS_PER_TENANT`].
    async fn create_project(&self, tenant: TenantId, p: NewProject) -> Result<Project>;
    /// The tenant's project with this slug; another tenant's behaves as missing.
    async fn get_project(&self, tenant: TenantId, slug: &str) -> Result<Option<Project>>;
    async fn get_project_by_id(&self, tenant: TenantId, id: ProjectId) -> Result<Option<Project>>;
    /// Ordered by slug; the cursor is the last slug returned.
    async fn list_projects(&self, tenant: TenantId, page: PageReq) -> Result<Page<Project>>;
    /// `Conflict("environment_in_use")` when the new list drops an environment a service is in.
    async fn update_project(&self, tenant: TenantId, slug: &str, patch: ProjectPatch) -> Result<Project>;
    /// `Conflict("project_not_empty")` while any service is in the project.
    async fn delete_project(&self, tenant: TenantId, slug: &str) -> Result<()>;

    // ---- services & versions -----------------------------------------------------------------
    async fn create_service(&self, scope: TenantScope, tenant: TenantId, s: NewService) -> Result<Service>;
    /// [`MetaStore::create_service`] that keeps the tenant's warm floors within `cap`, checked in
    /// the same transaction (`StoreError::WarmFloorTotal`).
    async fn create_service_capped(
        &self,
        scope: TenantScope,
        tenant: TenantId,
        s: NewService,
        cap: WarmFloorCap,
    ) -> Result<Service>;
    /// [`MetaStore::create_service_capped`] that also refuses the tenant's service past
    /// `max_services` (`Conflict("quota_exceeded")`; 0 = no limit). The count and the insert are
    /// one transaction, so concurrent creates cannot overshoot the plan (QUOTA-001).
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn create_service_limited(
        &self,
        scope: TenantScope,
        tenant: TenantId,
        s: NewService,
        cap: WarmFloorCap,
        max_services: u32,
    ) -> Result<Service> {
        if max_services > 0 && self.count_services(tenant).await? >= u64::from(max_services) {
            return Err(StoreError::Conflict("quota_exceeded"));
        }
        self.create_service_capped(scope, tenant, s, cap).await
    }
    // LCOV_EXCL_STOP
    async fn get_service(&self, scope: TenantScope, id: ServiceId) -> Result<Option<Service>>;
    /// `filter.slug` supports `GET /v1/services?slug=<slug>` (contracts C-9).
    async fn list_services(&self, scope: TenantScope, filter: ServiceFilter, page: PageReq) -> Result<Page<Service>>;
    async fn count_services(&self, tenant: TenantId) -> Result<u64>;
    /// The most schedules any one service of `tenant` has (`plan.max_schedules` is per service).
    async fn busiest_service_schedules(&self, tenant: TenantId) -> Result<u64>;
    /// `if_match`: the ETag the caller read (`412` on mismatch). Bumps the ETag.
    async fn patch_service(
        &self,
        scope: TenantScope,
        id: ServiceId,
        patch: ServicePatch,
        if_match: Option<Etag>,
    ) -> Result<Service>;
    /// [`MetaStore::patch_service`] that keeps the tenant's warm floors within `cap` when the
    /// patch sets `scaling`, checked in the same transaction (`StoreError::WarmFloorTotal`).
    async fn patch_service_capped(
        &self,
        scope: TenantScope,
        id: ServiceId,
        patch: ServicePatch,
        if_match: Option<Etag>,
        cap: WarmFloorCap,
    ) -> Result<Service>;
    async fn delete_service(&self, scope: TenantScope, id: ServiceId, keep_data: bool) -> Result<()>;

    async fn add_version(&self, id: ServiceId, v: NewVersion) -> Result<Version>;
    async fn get_version(&self, id: ServiceId, v: VersionNo) -> Result<Option<Version>>;
    async fn list_versions(&self, id: ServiceId) -> Result<Vec<Version>>;
    async fn set_build_state(
        &self,
        id: ServiceId,
        v: VersionNo,
        state: BuildState,
        failure: Option<BuildFailure>,
        lock_digest: Option<Digest>,
    ) -> Result<()>;
    /// `409 version_not_ready` for `Failed`; `Pending` is accepted (lazy compile).
    async fn set_live(&self, id: ServiceId, v: VersionNo, if_match: Option<Etag>) -> Result<Service>;
    /// Builds started by `tenant` since `since_ms` (`plan.builds_per_hour`).
    async fn count_builds_since(&self, tenant: TenantId, since_ms: UnixMs) -> Result<u64>;
    /// Delete old versions beyond `keep` (never `live`); returns the artifact digests nothing
    /// references any more: sources, their `rt.lock`s, and the vendored modules of those locks
    /// that no remaining lock lists ([`MetaStore::record_lock_modules`]).
    async fn gc_versions(&self, id: ServiceId, keep: u32) -> Result<Vec<Digest>>;
    /// True while any version (source or lock) or any recorded lock (module) still references
    /// `digest` (artifact refcount is computed live).
    async fn digest_referenced(&self, digest: &Digest) -> Result<bool>;
    /// Record the vendored module artifacts `lock` lists, after a successful build stored them
    /// (idempotent). Until then GC never frees a module. The default records nothing, so modules
    /// of such a store are never freed.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn record_lock_modules(&self, lock: &Digest, modules: &[Digest]) -> Result<()> {
        let _ = (lock, modules);
        Ok(())
    }
    // LCOV_EXCL_STOP
    /// Record the digest of the bundle a successful build of version `v` produced (audit,
    /// `Version::bundle_digest`). `NotFound` for an unknown version. The default records nothing.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn set_bundle_digest(&self, id: ServiceId, v: VersionNo, bundle: Digest) -> Result<()> {
        let _ = (id, v, bundle);
        Ok(())
    }
    // LCOV_EXCL_STOP
    /// The `rt.lock` digest of a successful build of `source` (any service), if one exists: a
    /// rebuild of the same source must reproduce it (BLD-06).
    async fn lock_for_source(&self, source: &Digest) -> Result<Option<Digest>>;

    /// All routable services (live version set) + verified hosts, as one immutable snapshot.
    async fn route_snapshot(&self) -> Result<RouteSnapshot>;

    // ---- env, sealed secrets -----------------------------------------------------------------
    async fn get_env(&self, id: ServiceId) -> Result<Vec<(String, String)>>;
    /// Replace the whole map (`PUT …/env`).
    async fn put_env(&self, id: ServiceId, env: Vec<(String, String)>) -> Result<SecretsVersion>;
    async fn list_secrets(&self, id: ServiceId) -> Result<Vec<SecretMeta>>;
    /// `sealed` is `rt_sec::Sealed::to_bytes()`.
    async fn put_secret(&self, id: ServiceId, name: &str, sealed: Vec<u8>) -> Result<SecretsVersion>;
    /// Compare-and-swap: replace secret `name` with `sealed` only while its stored bytes still
    /// equal `expected` (`rt secrets reseal` must not undo a write or delete that happened after
    /// it read the value). `false` when the secret changed or is gone.
    async fn replace_secret_if(&self, id: ServiceId, name: &str, expected: Vec<u8>, sealed: Vec<u8>) -> Result<bool>;
    async fn delete_secret(&self, id: ServiceId, name: &str) -> Result<SecretsVersion>;
    async fn get_sealed_secrets(&self, id: ServiceId) -> Result<Vec<(String, Vec<u8>)>>;
    async fn secrets_version(&self, id: ServiceId) -> Result<SecretsVersion>;

    // ---- hosts -------------------------------------------------------------------------------
    async fn put_host(&self, h: HostMapping) -> Result<()>;
    async fn delete_host(&self, id: ServiceId, host: &str) -> Result<bool>;
    async fn list_hosts(&self, id: ServiceId) -> Result<Vec<HostMapping>>;
    async fn find_host(&self, host: &str) -> Result<Option<HostMapping>>;
    /// Every `pending_verification` claim (DNS TXT verification job, claim expiry).
    async fn pending_hosts(&self) -> Result<Vec<HostMapping>>;
    /// Flip `host` to `active` if it is still `service`'s pending claim with `token` (the DNS TXT
    /// record matched). `false` when the claim changed or is gone meanwhile.
    async fn activate_host(&self, host: &str, service: ServiceId, token: &str) -> Result<bool>;
    /// Delete the pending claims made before `created_before_ms` (`hosts.pending_ttl`); returns
    /// what was deleted. Active mappings never expire.
    async fn expire_pending_hosts(&self, created_before_ms: UnixMs) -> Result<Vec<HostMapping>>;
    /// Remember that a tenant's pending claim ended (replaces an earlier record for the pair).
    async fn record_host_release(&self, r: HostRelease) -> Result<()>;
    /// The remembered end of `tenant`'s last pending claim of `host`, if any.
    async fn host_release(&self, host: &str, tenant: TenantId) -> Result<Option<HostRelease>>;
    /// Forget releases recorded before `released_before_ms`; returns how many.
    async fn prune_host_releases(&self, released_before_ms: UnixMs) -> Result<u64>;
    /// Pending claims held by `tenant`'s services (`hosts.max_pending_per_tenant`).
    async fn count_pending_hosts(&self, tenant: TenantId) -> Result<u64>;

    // ---- schedules ---------------------------------------------------------------------------
    async fn create_schedule(&self, id: ServiceId, s: NewSchedule) -> Result<Schedule>;
    async fn list_schedules(&self, id: ServiceId) -> Result<Vec<Schedule>>;
    async fn all_schedules(&self) -> Result<Vec<Schedule>>;
    async fn update_schedule(&self, id: ServiceId, sch: ScheduleId, s: NewSchedule) -> Result<Schedule>;
    async fn delete_schedule(&self, id: ServiceId, sch: ScheduleId) -> Result<bool>;
    async fn set_schedule_next_due(&self, sch: ScheduleId, next_due_ms: Option<UnixMs>) -> Result<()>;

    // ---- operator SDKs ----------------------------------------------
    /// Create the tenant `id` (the system tenant) with its `default` project when it does not
    /// exist; nothing changes when it does.
    async fn ensure_tenant(&self, id: TenantId, name: &str, plan: &str) -> Result<()>;
    async fn get_sdk(&self, name: &str) -> Result<Option<Sdk>>;
    async fn list_sdks(&self) -> Result<Vec<Sdk>>;
    /// Register the SDK `name` on its system service, or make a removed one active again.
    async fn upsert_sdk(&self, name: &str, service: ServiceId, description: Option<String>) -> Result<Sdk>;
    /// `Conflict("sdk_release_exists")` when the version was published before.
    async fn add_sdk_release(&self, r: NewSdkRelease) -> Result<SdkRelease>;
    /// Oldest first.
    async fn list_sdk_releases(&self, sdk: &str) -> Result<Vec<SdkRelease>>;
    async fn get_sdk_release(&self, sdk: &str, version: &str) -> Result<Option<SdkRelease>>;
    async fn list_sdk_channels(&self, sdk: &str) -> Result<Vec<SdkChannel>>;
    /// Point `channel` at the release `version` (`NotFound` when there is none); the version it
    /// pointed to becomes `previous`.
    async fn set_sdk_channel(&self, sdk: &str, channel: &str, version: &str) -> Result<SdkChannel>;
    /// Mark the SDK removed; `false` when there is none.
    async fn remove_sdk(&self, name: &str) -> Result<bool>;
    async fn list_sdk_grants(&self) -> Result<Vec<SdkGrant>>;
    /// Replace the grant of `(scope, sdk)`; one with no effect, requirement or channel is deleted
    /// (`None`).
    async fn put_sdk_grant(&self, g: SdkGrant) -> Result<Option<SdkGrant>>;

    // ---- usage, events, cache frequencies ----------------------------------------------------
    async fn add_usage(&self, day: &str, batch: UsageBatch) -> Result<()>;
    async fn get_usage(&self, tenant: TenantId, from_day: &str, to_day: &str) -> Result<Vec<UsageRow>>;
    async fn append_event(&self, ev: NodeEvent) -> Result<()>;
    async fn list_events(&self, service: ServiceId, page: PageReq) -> Result<Page<NodeEvent>>;
    async fn event_by_id(&self, id: EventId) -> Result<Option<NodeEvent>>;
    async fn save_cache_freq(&self, snapshot: Vec<u8>) -> Result<()>;
    async fn load_cache_freq(&self) -> Result<Option<Vec<u8>>>;
}

/// Read-only connections of a file-backed `meta.db` ([`SqliteMetaStore::open`]).
pub const META_READERS: usize = 4;

/// `rusqlite`-backed [`MetaStore`]. Writes run on one writer thread (the only read-write
/// connection); reads run on [`META_READERS`] read-only connections, each on its own thread, so
/// the control API, route snapshots and usage flushes do not serialize behind each other (WAL).
pub struct SqliteMetaStore {
    db: Arc<Db>,
    /// Bumped by every write; reported as `RouteSnapshot::generation`.
    generation: Arc<AtomicU64>,
}

impl SqliteMetaStore {
    /// Make the next operations of this store fail as `faults` plans (tests, TK-002).
    pub fn inject_faults(&self, faults: Arc<crate::faults::Faults>) {
        self.db.inject_faults(faults);
    }

    /// Open or create `meta.db`, apply [`MIGRATIONS`], set WAL/pragmas. Refuses a database whose
    /// schema is newer than this binary.
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        Self::open_with_readers(path, META_READERS)
    }

    /// [`SqliteMetaStore::open`] with `readers` read-only connections (0: reads use the writer).
    pub fn open_with_readers(path: &Path, readers: usize) -> Result<Arc<Self>> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let conn = Connection::open(path)?;
        conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get::<_, String>(0))?;
        Self::init(conn, Some((path, readers)))
    }

    /// In-memory database (tests). Reads and writes share the one connection.
    pub fn open_in_memory() -> Result<Arc<Self>> {
        Self::init(Connection::open_in_memory()?, None)
    }

    fn init(mut conn: Connection, file: Option<(&Path, usize)>) -> Result<Arc<Self>> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", true)?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        conn.set_prepared_statement_cache_capacity(128);
        migrate(&mut conn, MIGRATIONS)?;
        // Readers open after the migration, so they never see an older schema.
        let readers = match file {
            Some((path, n)) => (0..n).map(|_| open_reader(path, 128)).collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };
        Ok(Arc::new(Self {
            db: Db::new("rt-meta", conn, readers)?,
            generation: Arc::new(AtomicU64::new(1)),
        }))
    }

    /// Transactional write; bumps the route generation on success.
    async fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let out = self.db.write(f).await?;
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(out)
    }
}

// ---- constants & validation ------------------------------------------------------------------

const MAX_ENV_ENTRIES: usize = 100;
const MAX_ENV_VALUE_BYTES: usize = 32 * 1024;
/// Sealed value = 12-byte nonce + ciphertext + 16-byte tag around a plaintext of at most 32 KB.
const MAX_SEALED_BYTES: usize = MAX_ENV_VALUE_BYTES + 64;
const MAX_SCHEDULE_PAYLOAD_BYTES: usize = 64 * 1024;

fn valid_env_name(n: &str) -> bool {
    let b = n.as_bytes();
    !b.is_empty()
        && b.len() <= 255
        && (b[0] == b'_' || b[0].is_ascii_uppercase())
        && b.iter()
            .all(|c| *c == b'_' || c.is_ascii_uppercase() || c.is_ascii_digit())
        && !n.starts_with("RT_")
}

fn check_env_name(n: &str) -> Result<()> {
    if valid_env_name(n) {
        Ok(())
    } else {
        Err(StoreError::Invalid(format!(
            "invalid name `{n}`: expected [A-Z_][A-Z0-9_]* without the RT_ prefix"
        )))
    }
}

fn normalize_host(h: &str) -> String {
    h.trim().trim_end_matches('.').to_ascii_lowercase()
}

// ---- row mappers -----------------------------------------------------------------------------

const TENANT_COLS: &str = "id, name, plan, overrides, created_at, disabled";

fn tenant_row(r: &Row<'_>) -> rusqlite::Result<Tenant> {
    Ok(Tenant {
        id: parse_col(&r.get::<_, String>(0)?, 0)?,
        name: r.get(1)?,
        plan: r.get(2)?,
        overrides: json_col(&r.get::<_, String>(3)?, 3)?,
        created_at_ms: r.get(4)?,
        disabled: r.get::<_, i64>(5)? != 0,
    })
}

/// A key's scope comes from its rows: `all_*` = `None`, otherwise the project and environment
/// rows (an empty list when there are none).
const KEY_COLS: &str =
    "id, tenant_id, name, secret_hmac, scopes, all_projects, all_environments, expires_at, created_at, \
     last_used_at, revoked_at, parent_key_id, \
     (SELECT json_group_array(project_id) FROM api_key_projects WHERE key_id = api_keys.id), \
     (SELECT json_group_array(environment) FROM api_key_environments WHERE key_id = api_keys.id)";

fn key_row(r: &Row<'_>) -> rusqlite::Result<ApiKey> {
    let all_projects = r.get::<_, i64>(5)? != 0;
    let all_environments = r.get::<_, i64>(6)? != 0;
    Ok(ApiKey {
        id: parse_col(&r.get::<_, String>(0)?, 0)?,
        tenant_id: parse_col(&r.get::<_, String>(1)?, 1)?,
        name: r.get(2)?,
        hmac: r.get(3)?,
        scopes: json_col(&r.get::<_, String>(4)?, 4)?,
        project_ids: match all_projects {
            true => None,
            false => Some(json_col::<Vec<ProjectId>>(&r.get::<_, String>(12)?, 12)?),
        },
        environments: match all_environments {
            true => None,
            false => Some(json_col::<Vec<String>>(&r.get::<_, String>(13)?, 13)?),
        },
        expires_at_ms: r.get(7)?,
        created_at_ms: r.get(8)?,
        last_used_at_ms: r.get(9)?,
        revoked: r.get::<_, Option<i64>>(10)?.is_some(),
        parent_key_id: r.get::<_, Option<String>>(11)?.map(|p| parse_col(&p, 11)).transpose()?,
    })
}

/// `live_version` is the number of the version flagged live (`versions.is_live`); the project's
/// slug comes from its row. Both are correlated subqueries, so the query needs no join.
const SERVICE_COLS: &str = "id, tenant_id, slug, runtime, name, profile, config, \
     (SELECT number FROM versions WHERE versions.service_id = services.id AND versions.is_live = 1), \
     etag, created_at, updated_at, environment, project_id, (SELECT slug FROM projects WHERE projects.id = services.project_id)";

fn service_row(r: &Row<'_>) -> rusqlite::Result<Service> {
    Ok(Service {
        id: parse_col(&r.get::<_, String>(0)?, 0)?,
        tenant_id: parse_col(&r.get::<_, String>(1)?, 1)?,
        slug: r.get(2)?,
        runtime: parse_col(&r.get::<_, String>(3)?, 3)?,
        name: r.get(4)?,
        profile: r
            .get::<_, Option<String>>(5)?
            .map(|p| parse_col::<Profile>(&p, 5))
            .transpose()?,
        config: json_col(&r.get::<_, String>(6)?, 6)?,
        live_version: r.get::<_, Option<i64>>(7)?.map(|n| VersionNo(n as u32)),
        etag: Etag(r.get::<_, i64>(8)? as u64),
        created_at_ms: r.get(9)?,
        updated_at_ms: r.get(10)?,
        environment: r.get(11)?,
        project_id: parse_col(&r.get::<_, String>(12)?, 12)?,
        project: r.get(13)?,
    })
}

/// `environments` is the project's list in position order, as a JSON array.
const PROJECT_COLS: &str = "id, tenant_id, slug, name, \
     (SELECT json_group_array(name) FROM (SELECT name FROM project_environments WHERE project_id = projects.id ORDER BY position)), \
     created_at, updated_at";

fn project_row(r: &Row<'_>) -> rusqlite::Result<Project> {
    Ok(Project {
        id: parse_col(&r.get::<_, String>(0)?, 0)?,
        tenant_id: parse_col(&r.get::<_, String>(1)?, 1)?,
        slug: r.get(2)?,
        name: r.get(3)?,
        environments: json_col(&r.get::<_, String>(4)?, 4)?,
        created_at_ms: r.get(5)?,
        updated_at_ms: r.get(6)?,
    })
}

const VERSION_COLS: &str =
    "service_id, number, source_digest, lock_digest, build_state, pending_reason, failure, created_at, bundle_digest";

fn state_str(s: BuildState) -> &'static str {
    match s {
        BuildState::Pending => "pending",
        BuildState::Building => "building",
        BuildState::Ready => "ready",
        BuildState::Failed => "failed",
    }
}

fn state_from(s: &str, col: usize) -> rusqlite::Result<BuildState> {
    match s {
        "pending" => Ok(BuildState::Pending),
        "building" => Ok(BuildState::Building),
        "ready" => Ok(BuildState::Ready),
        "failed" => Ok(BuildState::Failed),
        other => Err(bad_col(col, format!("unknown build state `{other}`"))),
    }
}

fn version_row(r: &Row<'_>) -> rusqlite::Result<Version> {
    Ok(Version {
        service_id: parse_col(&r.get::<_, String>(0)?, 0)?,
        number: VersionNo(r.get::<_, i64>(1)? as u32),
        source_digest: parse_col(&r.get::<_, String>(2)?, 2)?,
        lock_digest: r
            .get::<_, Option<String>>(3)?
            .map(|d| parse_col::<Digest>(&d, 3))
            .transpose()?,
        build_state: state_from(&r.get::<_, String>(4)?, 4)?,
        pending_reason: r.get(5)?,
        failure: r
            .get::<_, Option<String>>(6)?
            .map(|f| json_col::<BuildFailure>(&f, 6))
            .transpose()?,
        created_at_ms: r.get(7)?,
        bundle_digest: r
            .get::<_, Option<String>>(8)?
            .map(|d| parse_col::<Digest>(&d, 8))
            .transpose()?,
    })
}

fn host_status_str(s: HostStatus) -> &'static str {
    match s {
        HostStatus::PendingVerification => "pending_verification",
        HostStatus::Active => "active",
    }
}

fn host_row(r: &Row<'_>) -> rusqlite::Result<HostMapping> {
    let status = match r.get::<_, String>(2)?.as_str() {
        "active" => HostStatus::Active,
        "pending_verification" => HostStatus::PendingVerification,
        other => return Err(bad_col(2, format!("unknown host status `{other}`"))),
    };
    Ok(HostMapping {
        host: r.get(0)?,
        service_id: parse_col(&r.get::<_, String>(1)?, 1)?,
        status,
        verify_token: r.get(3)?,
        created_at_ms: r.get(4)?,
    })
}

const HOST_COLS: &str = "host, service_id, status, verify_token, created_at";

/// The schedule's kind and the columns that hold it: `(kind, every_seconds, cron, timezone)`.
fn schedule_columns(kind: &ScheduleKind) -> (&'static str, Option<i64>, Option<String>, Option<String>) {
    match kind {
        ScheduleKind::Interval { every_seconds } => (
            "interval",
            Some(i64::try_from(*every_seconds).unwrap_or(i64::MAX)),
            None,
            None,
        ),
        ScheduleKind::Cron { expr, tz } => ("cron", None, Some(expr.clone()), Some(tz.clone())),
    }
}

const SCHEDULE_COLS: &str = "id, service_id, kind, every_seconds, cron, timezone, payload, enabled, next_due_at";

fn schedule_row(r: &Row<'_>) -> rusqlite::Result<Schedule> {
    let kind = match r.get::<_, String>(2)?.as_str() {
        "interval" => ScheduleKind::Interval {
            every_seconds: r
                .get::<_, Option<i64>>(3)?
                .ok_or_else(|| bad_col(3, "missing every_seconds"))? as u64,
        },
        "cron" => ScheduleKind::Cron {
            expr: r
                .get::<_, Option<String>>(4)?
                .ok_or_else(|| bad_col(4, "missing cron"))?,
            tz: r
                .get::<_, Option<String>>(5)?
                .ok_or_else(|| bad_col(5, "missing timezone"))?,
        },
        other => return Err(bad_col(2, format!("unknown schedule kind `{other}`"))),
    };
    Ok(Schedule {
        id: parse_col(&r.get::<_, String>(0)?, 0)?,
        service_id: parse_col(&r.get::<_, String>(1)?, 1)?,
        kind,
        payload: json_col(&r.get::<_, String>(6)?, 6)?,
        enabled: r.get::<_, i64>(7)? != 0,
        next_due_ms: r.get(8)?,
    })
}

const EVENT_COLS: &str = "id, ts, tenant_id, service_id, kind, data";

fn event_row(r: &Row<'_>) -> rusqlite::Result<NodeEvent> {
    Ok(NodeEvent {
        id: parse_col(&r.get::<_, String>(0)?, 0)?,
        ts_ms: r.get(1)?,
        tenant: r
            .get::<_, Option<String>>(2)?
            .map(|t| parse_col::<TenantId>(&t, 2))
            .transpose()?,
        service: r
            .get::<_, Option<String>>(3)?
            .map(|s| parse_col::<ServiceId>(&s, 3))
            .transpose()?,
        kind: r.get(4)?,
        data: json_col(&r.get::<_, String>(5)?, 5)?,
    })
}

// ---- query helpers ---------------------------------------------------------------------------

const SDK_COLS: &str = "name, service_id, description, state, created_at, removed_at";

fn sdk_row(r: &Row<'_>) -> rusqlite::Result<Sdk> {
    Ok(Sdk {
        name: r.get(0)?,
        service_id: parse_col(&r.get::<_, String>(1)?, 1)?,
        description: r.get(2)?,
        state: if r.get::<_, String>(3)? == "removed" {
            SdkState::Removed
        } else {
            SdkState::Active
        },
        created_at_ms: r.get(4)?,
        removed_at_ms: r.get(5)?,
    })
}

const SDK_RELEASE_COLS: &str = "sdk, version, version_no, members, contract, created_at";

fn sdk_release_row(r: &Row<'_>) -> rusqlite::Result<SdkRelease> {
    Ok(SdkRelease {
        sdk: r.get(0)?,
        version: r.get(1)?,
        version_no: VersionNo(r.get::<_, i64>(2)? as u32),
        members: json_col(&r.get::<_, String>(3)?, 3)?,
        contract: r.get(4)?,
        created_at_ms: r.get(5)?,
    })
}

const SDK_CHANNEL_COLS: &str = "sdk, channel, version, previous, updated_at";

fn sdk_channel_row(r: &Row<'_>) -> rusqlite::Result<SdkChannel> {
    Ok(SdkChannel {
        sdk: r.get(0)?,
        channel: r.get(1)?,
        version: r.get(2)?,
        previous: r.get(3)?,
        updated_at_ms: r.get(4)?,
    })
}

const SDK_GRANT_COLS: &str = "scope_kind, scope_id, sdk, effect, version_req, channel, created_by, updated_at";

fn sdk_grant_row(r: &Row<'_>) -> rusqlite::Result<SdkGrant> {
    let kind: String = r.get(0)?;
    let id: String = r.get(1)?;
    let scope = GrantScope::from_parts(&kind, &id).map_err(|e| bad_col(1, e))?;
    let effect = match r.get::<_, Option<String>>(3)?.as_deref() {
        Some("allow") => Some(GrantEffect::Allow),
        Some("deny") => Some(GrantEffect::Deny),
        _ => None,
    };
    Ok(SdkGrant {
        scope,
        sdk: r.get(2)?,
        effect,
        version_req: r.get(4)?,
        channel: r.get(5)?,
        created_by: r.get(6)?,
        updated_at_ms: r.get(7)?,
    })
}

/// The SDK part of the route snapshot: what the data plane binds calls with.
fn sdk_snapshot(c: &Connection) -> Result<SdkSnapshot> {
    let sdks = c
        .prepare_cached(&format!("SELECT {SDK_COLS} FROM sdks ORDER BY name"))?
        .query_map([], sdk_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let releases = c
        .prepare_cached(
            "SELECT r.sdk, r.version, k.service_id, r.version_no, v.source_digest, s.runtime, r.members \
             FROM sdk_releases r JOIN sdks k ON k.name = r.sdk \
             JOIN versions v ON v.service_id = k.service_id AND v.number = r.version_no \
             JOIN services s ON s.id = k.service_id ORDER BY r.sdk, r.created_at",
        )?
        .query_map([], |r| {
            Ok(SdkTarget {
                sdk: r.get(0)?,
                version: r.get(1)?,
                service: parse_col(&r.get::<_, String>(2)?, 2)?,
                version_no: VersionNo(r.get::<_, i64>(3)? as u32),
                digest: parse_col(&r.get::<_, String>(4)?, 4)?,
                runtime: parse_col(&r.get::<_, String>(5)?, 5)?,
                members: json_col(&r.get::<_, String>(6)?, 6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let channels = c
        .prepare_cached(&format!(
            "SELECT {SDK_CHANNEL_COLS} FROM sdk_channels ORDER BY sdk, channel"
        ))?
        .query_map([], sdk_channel_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let grants = sdk_snapshot_grants(c)?;
    Ok(SdkSnapshot {
        sdks,
        releases,
        channels,
        grants,
    })
}

fn sdk_snapshot_grants(c: &Connection) -> Result<Vec<SdkGrant>> {
    Ok(c.prepare_cached(&format!(
        "SELECT {SDK_GRANT_COLS} FROM sdk_grants ORDER BY scope_kind, scope_id, sdk"
    ))?
    .query_map([], sdk_grant_row)?
    .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn sdk_by_name(c: &Connection, name: &str) -> Result<Option<Sdk>> {
    Ok(
        c.prepare_cached(&format!("SELECT {SDK_COLS} FROM sdks WHERE name = ?1"))?
            .query_row([name], sdk_row)
            .optional()?,
    )
}

fn tenant_by_id(c: &Connection, id: TenantId) -> Result<Option<Tenant>> {
    Ok(
        c.prepare_cached(&format!("SELECT {TENANT_COLS} FROM tenants WHERE id = ?1"))?
            .query_row([id.to_string()], tenant_row)
            .optional()?,
    )
}

/// Insert a key of `tenant` (minted by `parent`).
fn insert_key(c: &Connection, tenant: TenantId, k: NewKey, parent: Option<KeyId>) -> Result<ApiKey> {
    require_tenant(c, tenant)?;
    // The parent must still be live in this transaction: a revocation that committed after the
    // parent authenticated would otherwise miss the new child (KEY-035).
    if let Some(p) = parent {
        let live: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM api_keys WHERE id = ?1 AND tenant_id = ?2 AND revoked_at IS NULL)",
            params![p.to_string(), tenant.to_string()],
            |r| r.get(0),
        )?;
        if !live {
            return Err(StoreError::NotFound);
        }
    }
    let scopes = to_json(&k.scopes)?;
    let created = now_ms();
    let key = k.id.to_string();
    c.execute(
        "INSERT INTO api_keys (id, tenant_id, name, secret_hmac, scopes, all_projects, all_environments, expires_at, \
           created_at, revoked_at, parent_key_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10)",
        params![
            key,
            tenant.to_string(),
            k.name,
            k.hmac,
            scopes,
            k.project_ids.is_none(),
            k.environments.is_none(),
            k.expires_at_ms,
            created,
            parent.map(|p| p.to_string())
        ],
    )
    .map_err(|e| {
        if is_unique_violation(&e) {
            StoreError::Conflict("key_exists")
        } else {
            e.into()
        }
    })?;
    for p in k.project_ids.iter().flatten() {
        c.execute(
            "INSERT OR IGNORE INTO api_key_projects (key_id, tenant_id, project_id) VALUES (?1, ?2, ?3)",
            params![key, tenant.to_string(), p.to_string()],
        )?;
    }
    for e in k.environments.iter().flatten() {
        c.execute(
            "INSERT OR IGNORE INTO api_key_environments (key_id, environment) VALUES (?1, ?2)",
            params![key, e],
        )?;
    }
    Ok(ApiKey {
        id: k.id,
        tenant_id: tenant,
        name: k.name,
        scopes: k.scopes,
        project_ids: k.project_ids,
        environments: k.environments,
        expires_at_ms: k.expires_at_ms,
        created_at_ms: created,
        last_used_at_ms: None,
        revoked: false,
        parent_key_id: parent,
        hmac: k.hmac,
    })
}

/// Keys of `tenant` neither revoked nor expired at `now_ms`.
fn live_keys(c: &Connection, tenant: TenantId, now_ms: UnixMs) -> Result<u64> {
    let n: i64 = c
        .prepare_cached(
            "SELECT count(*) FROM api_keys WHERE tenant_id = ?1 AND revoked_at IS NULL \
             AND (expires_at IS NULL OR expires_at > ?2)",
        )?
        .query_row(params![tenant.to_string(), now_ms], |r| r.get(0))?;
    Ok(u64::try_from(n).unwrap_or(0))
}

fn require_tenant(c: &Connection, id: TenantId) -> Result<Tenant> {
    tenant_by_id(c, id)?.ok_or(StoreError::NotFound)
}

/// Tenant-scoped lookup: a service of another tenant behaves as missing.
fn service_by_id(c: &Connection, scope: TenantScope, id: ServiceId) -> Result<Option<Service>> {
    let svc: Option<Service> = c
        .prepare_cached(&format!("SELECT {SERVICE_COLS} FROM services WHERE id = ?1"))?
        .query_row([id.to_string()], service_row)
        .optional()?;
    Ok(svc.filter(|s| match scope {
        TenantScope::Operator => true,
        TenantScope::Tenant(t) => s.tenant_id == t,
    }))
}

fn require_service(c: &Connection, id: ServiceId) -> Result<Service> {
    service_by_id(c, TenantScope::Operator, id)?.ok_or(StoreError::NotFound)
}

fn project_by_slug(c: &Connection, tenant: TenantId, slug: &str) -> Result<Option<Project>> {
    Ok(c.prepare_cached(&format!(
        "SELECT {PROJECT_COLS} FROM projects WHERE tenant_id = ?1 AND slug = ?2"
    ))?
    .query_row(params![tenant.to_string(), slug], project_row)
    .optional()?)
}

fn insert_project(c: &Connection, p: &Project) -> Result<()> {
    c.execute(
        "INSERT INTO projects (id, tenant_id, slug, name, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            p.id.to_string(),
            p.tenant_id.to_string(),
            p.slug,
            p.name,
            p.created_at_ms,
            p.updated_at_ms
        ],
    )
    .map_err(|e| {
        if is_unique_violation(&e) {
            StoreError::Conflict("project_slug_taken")
        } else {
            e.into()
        }
    })?;
    set_project_environments(c, p.id, &p.environments)
}

/// Makes `names` the project's environments, in order. Environments not listed are deleted, so the
/// caller checks first that no service is in one (the foreign key would refuse it).
fn set_project_environments(c: &Connection, project: ProjectId, names: &[String]) -> Result<()> {
    let id = project.to_string();
    for (position, name) in names.iter().enumerate() {
        c.execute(
            "INSERT INTO project_environments (project_id, name, position) VALUES (?1, ?2, ?3) \
             ON CONFLICT (project_id, name) DO UPDATE SET position = excluded.position",
            params![id, name, position as i64],
        )?;
    }
    let keep = to_json(&names)?;
    c.execute(
        "DELETE FROM project_environments WHERE project_id = ?1 AND name NOT IN (SELECT value FROM json_each(?2))",
        params![id, keep],
    )?;
    Ok(())
}

/// The `default` project every tenant has: its id derives from the tenant's (the schema
/// migration derives the same ids in SQL).
fn default_project_row(tenant: TenantId, now: UnixMs) -> Project {
    Project {
        id: rt_types::default_project_id(tenant),
        tenant_id: tenant,
        slug: rt_types::DEFAULT_PROJECT.to_string(),
        name: "Default".to_string(),
        environments: rt_types::default_project_environments(),
        created_at_ms: now,
        updated_at_ms: now,
    }
}

/// A display name: not blank, at most [`rt_types::MAX_PROJECT_NAME_LEN`] characters, trimmed.
fn check_project_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > rt_types::MAX_PROJECT_NAME_LEN {
        return Err(StoreError::Invalid(format!(
            "`name` must be 1 to {} characters",
            rt_types::MAX_PROJECT_NAME_LEN
        )));
    }
    Ok(name.to_string())
}

/// The project a service of `tenant` is created in: it must exist (the `default` one is created
/// on demand) and list `environment`.
fn project_for_service(c: &Connection, tenant: TenantId, project: &str, environment: &str) -> Result<Project> {
    let found = match project_by_ref(c, tenant, project)? {
        Some(p) => p,
        None if project == rt_types::DEFAULT_PROJECT => {
            let p = default_project_row(tenant, now_ms());
            insert_project(c, &p)?;
            p
        }
        None => return Err(StoreError::Invalid(format!("project `{project}` does not exist"))),
    };
    if !found.environments.iter().any(|e| e == environment) {
        return Err(StoreError::Invalid(format!(
            "`{environment}` is not an environment of project `{}`",
            found.slug
        )));
    }
    Ok(found)
}

/// A tenant's project named by its id (`prj_…`) or its slug.
fn project_by_ref(c: &Connection, tenant: TenantId, project: &str) -> Result<Option<Project>> {
    match project.parse::<ProjectId>() {
        Ok(id) if project.starts_with("prj_") => Ok(c
            .prepare_cached(&format!(
                "SELECT {PROJECT_COLS} FROM projects WHERE id = ?1 AND tenant_id = ?2"
            ))?
            .query_row(params![id.to_string(), tenant.to_string()], project_row)
            .optional()?),
        _ => project_by_slug(c, tenant, project),
    }
}

/// Adds one to the generation of the service's env and secrets.
fn bump_secrets_version(c: &Connection, id: ServiceId) -> Result<SecretsVersion> {
    c.execute(
        "UPDATE services SET secrets_version = secrets_version + 1 WHERE id = ?1",
        [id.to_string()],
    )?;
    let v: i64 = c.query_row(
        "SELECT secrets_version FROM services WHERE id = ?1",
        [id.to_string()],
        |r| r.get(0),
    )?;
    Ok(SecretsVersion(v as u64))
}

fn load_schedule(c: &Connection, id: ScheduleId) -> Result<Option<Schedule>> {
    Ok(
        c.prepare_cached(&format!("SELECT {SCHEDULE_COLS} FROM schedules WHERE id = ?1"))?
            .query_row([id.to_string()], schedule_row)
            .optional()?,
    )
}

fn check_schedule(s: &NewSchedule) -> Result<()> {
    if let ScheduleKind::Interval { every_seconds: 0 } = s.kind {
        return Err(StoreError::Invalid("interval must be at least 1 second".into()));
    }
    if let ScheduleKind::Cron { expr, .. } = &s.kind {
        if expr.trim().is_empty() {
            return Err(StoreError::Invalid("cron expression is empty".into()));
        }
    }
    if to_json(&s.payload)?.len() > MAX_SCHEDULE_PAYLOAD_BYTES {
        return Err(StoreError::Invalid("schedule payload exceeds 64 KB".into()));
    }
    Ok(())
}

/// An artifact digest is in use while a version names it (source or lock) or a recorded lock
/// lists it (vendored module).
const DIGEST_REFERENCED_SQL: &str =
    "SELECT EXISTS(SELECT 1 FROM versions WHERE source_digest = ?1 OR lock_digest = ?1) \
     OR EXISTS(SELECT 1 FROM lock_modules WHERE module_digest = ?1)";

fn clamp_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[async_trait]
impl MetaStore for SqliteMetaStore {
    async fn schema_version(&self) -> Result<u32> {
        self.db
            .read(|c| Ok(c.query_row("PRAGMA user_version", [], |r| r.get::<_, u32>(0))?))
            .await
    }

    // ---- tenants & keys ----------------------------------------------------------------------

    async fn create_tenant(&self, t: NewTenant) -> Result<Tenant> {
        self.write(move |c| {
            let tenant = Tenant {
                id: TenantId::new(),
                name: t.name,
                plan: t.plan,
                overrides: t.overrides.unwrap_or(serde_json::Value::Null),
                created_at_ms: now_ms(),
                disabled: false,
            };
            c.execute(
                "INSERT INTO tenants (id, name, plan, overrides, created_at, disabled) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                params![
                    tenant.id.to_string(),
                    tenant.name,
                    tenant.plan,
                    to_json(&tenant.overrides)?,
                    tenant.created_at_ms
                ],
            )?;
            insert_project(c, &default_project_row(tenant.id, tenant.created_at_ms))?;
            Ok(tenant)
        })
        .await
    }

    async fn get_tenant(&self, id: TenantId) -> Result<Option<Tenant>> {
        self.db.read(move |c| tenant_by_id(c, id)).await
    }

    async fn list_tenants(&self, page: PageReq) -> Result<Page<Tenant>> {
        self.db
            .read(move |c| {
                let limit = page_limit(page.limit);
                let rows = c
                    .prepare_cached(&format!(
                        "SELECT {TENANT_COLS} FROM tenants WHERE id > ?1 ORDER BY id LIMIT ?2"
                    ))?
                    .query_map(params![page.cursor.unwrap_or_default(), limit as i64 + 1], tenant_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(paginate(rows, limit, |t| t.id.to_string()))
            })
            .await
    }

    async fn update_tenant(&self, id: TenantId, patch: TenantPatch) -> Result<Tenant> {
        self.write(move |c| {
            let cur = require_tenant(c, id)?;
            let t = Tenant {
                name: patch.name.unwrap_or(cur.name),
                plan: patch.plan.unwrap_or(cur.plan),
                overrides: patch.overrides.unwrap_or(cur.overrides),
                disabled: patch.disabled.unwrap_or(cur.disabled),
                ..cur
            };
            c.execute(
                "UPDATE tenants SET name = ?2, plan = ?3, overrides = ?4, disabled = ?5 WHERE id = ?1",
                params![
                    id.to_string(),
                    t.name,
                    t.plan,
                    to_json(&t.overrides)?,
                    i64::from(t.disabled)
                ],
            )?;
            Ok(t)
        })
        .await
    }

    async fn delete_tenant(&self, id: TenantId, purge: bool) -> Result<()> {
        self.write(move |c| {
            require_tenant(c, id)?;
            let key = id.to_string();
            if !purge {
                let n: i64 = c.query_row("SELECT count(*) FROM services WHERE tenant_id = ?1", [&key], |r| {
                    r.get(0)
                })?;
                if n > 0 {
                    return Err(StoreError::Conflict("tenant_not_empty"));
                }
            } else {
                c.execute("DELETE FROM usage WHERE tenant_id = ?1", [&key])?;
                c.execute("DELETE FROM events WHERE tenant_id = ?1", [&key])?;
            }
            // Services, versions, env, secrets, hosts, schedules and keys cascade.
            c.execute("DELETE FROM tenants WHERE id = ?1", [&key])?;
            Ok(())
        })
        .await
    }

    async fn create_key(&self, tenant: TenantId, k: NewKey) -> Result<ApiKey> {
        self.create_child_key(tenant, k, None).await
    }

    async fn create_child_key(&self, tenant: TenantId, k: NewKey, parent: Option<KeyId>) -> Result<ApiKey> {
        self.write(move |c| insert_key(c, tenant, k, parent)).await
    }

    async fn create_child_key_capped(
        &self,
        tenant: TenantId,
        k: NewKey,
        parent: Option<KeyId>,
        max_live: u64,
        now_ms: UnixMs,
    ) -> Result<ApiKey> {
        self.write(move |c| {
            if live_keys(c, tenant, now_ms)? >= max_live {
                return Err(StoreError::Conflict("quota_exceeded"));
            }
            insert_key(c, tenant, k, parent)
        })
        .await
    }

    async fn find_key(&self, id: KeyId) -> Result<Option<ApiKey>> {
        self.db
            .read(move |c| {
                Ok(
                    c.prepare_cached(&format!("SELECT {KEY_COLS} FROM api_keys WHERE id = ?1"))?
                        .query_row([id.to_string()], key_row)
                        .optional()?,
                )
            })
            .await
    }

    async fn list_keys(&self, tenant: TenantId) -> Result<Vec<ApiKey>> {
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {KEY_COLS} FROM api_keys WHERE tenant_id = ?1 ORDER BY id"
                ))?
                .query_map([tenant.to_string()], key_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn revoke_key(&self, tenant: TenantId, id: KeyId) -> Result<bool> {
        self.write(move |c| {
            let n = c.execute(
                "UPDATE api_keys SET revoked_at = COALESCE(revoked_at, ?3) WHERE id = ?1 AND tenant_id = ?2",
                params![id.to_string(), tenant.to_string(), now_ms()],
            )?;
            Ok(n > 0)
        })
        .await
    }

    async fn count_live_keys(&self, tenant: TenantId, now_ms: UnixMs) -> Result<u64> {
        self.db.read(move |c| live_keys(c, tenant, now_ms)).await
    }

    async fn revoke_key_tree(&self, tenant: TenantId, id: KeyId) -> Result<Vec<KeyId>> {
        self.write(move |c| {
            // The root must be live; its descendants are collected whatever their state (a revoked
            // middle key does not shield the keys below it) and the live ones revoked.
            let ids: Vec<String> = c
                .prepare_cached(
                    "WITH RECURSIVE tree(id) AS ( \
                       SELECT id FROM api_keys WHERE id = ?1 AND tenant_id = ?2 AND revoked_at IS NULL \
                       UNION SELECT k.id FROM api_keys k JOIN tree t ON k.parent_key_id = t.id \
                         WHERE k.tenant_id = ?2) \
                     UPDATE api_keys SET revoked_at = ?3 WHERE revoked_at IS NULL AND id IN (SELECT id FROM tree) \
                     RETURNING id",
                )?
                .query_map(params![id.to_string(), tenant.to_string(), now_ms()], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            let mut out = Vec::with_capacity(ids.len());
            out.push(id);
            for raw in &ids {
                let k: KeyId = parse_col(raw, 0)?;
                if k != id {
                    out.push(k);
                }
            }
            Ok(if ids.is_empty() { Vec::new() } else { out })
        })
        .await
    }

    async fn tenant_projects_among(&self, tenant: TenantId, ids: &[ProjectId]) -> Result<Vec<ProjectId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let raw = to_json(&ids.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        self.db
            .read(move |c| {
                let found: Vec<String> = c
                    .prepare_cached(
                        "SELECT id FROM projects WHERE tenant_id = ?1 AND id IN (SELECT value FROM json_each(?2))",
                    )?
                    .query_map(params![tenant.to_string(), raw], |r| r.get(0))?
                    .collect::<rusqlite::Result<_>>()?;
                found.iter().map(|s| Ok(parse_col::<ProjectId>(s, 0)?)).collect()
            })
            .await
    }

    async fn touch_key(&self, id: KeyId, at_ms: UnixMs) -> Result<()> {
        self.db
            .call(move |c| {
                c.prepare_cached("UPDATE api_keys SET last_used_at = ?2 WHERE id = ?1")?
                    .execute(params![id.to_string(), at_ms])?;
                Ok(())
            })
            .await
    }

    // ---- projects ----------------------------------------------------------------------------

    async fn create_project(&self, tenant: TenantId, p: NewProject) -> Result<Project> {
        if !rt_types::valid_project_slug(&p.slug) {
            return Err(StoreError::Invalid("slug must match ^[a-z][a-z0-9-]{1,62}$".into()));
        }
        let name = match &p.name {
            Some(n) => check_project_name(n)?,
            None => p.slug.clone(),
        };
        rt_types::check_project_environments(&p.environments).map_err(StoreError::Invalid)?;
        self.write(move |c| {
            require_tenant(c, tenant)?;
            let n: i64 = c.query_row(
                "SELECT count(*) FROM projects WHERE tenant_id = ?1",
                [tenant.to_string()],
                |r| r.get(0),
            )?;
            if usize::try_from(n).unwrap_or(usize::MAX) >= rt_types::MAX_PROJECTS_PER_TENANT {
                return Err(StoreError::Conflict("quota_exceeded"));
            }
            let now = now_ms();
            let project = Project {
                id: ProjectId::new(),
                tenant_id: tenant,
                slug: p.slug,
                name,
                environments: p.environments,
                created_at_ms: now,
                updated_at_ms: now,
            };
            insert_project(c, &project)?;
            Ok(project)
        })
        .await
    }

    async fn get_project(&self, tenant: TenantId, slug: &str) -> Result<Option<Project>> {
        let slug = slug.to_string();
        self.db.read(move |c| project_by_slug(c, tenant, &slug)).await
    }

    async fn get_project_by_id(&self, tenant: TenantId, id: ProjectId) -> Result<Option<Project>> {
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {PROJECT_COLS} FROM projects WHERE id = ?1 AND tenant_id = ?2"
                ))?
                .query_row(params![id.to_string(), tenant.to_string()], project_row)
                .optional()?)
            })
            .await
    }

    async fn list_projects(&self, tenant: TenantId, page: PageReq) -> Result<Page<Project>> {
        self.db
            .read(move |c| {
                let limit = page_limit(page.limit);
                let rows = c
                    .prepare_cached(&format!(
                        "SELECT {PROJECT_COLS} FROM projects WHERE tenant_id = ?1 AND slug > ?2 ORDER BY slug LIMIT ?3"
                    ))?
                    .query_map(
                        params![tenant.to_string(), page.cursor.unwrap_or_default(), limit as i64 + 1],
                        project_row,
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(paginate(rows, limit, |p| p.slug.clone()))
            })
            .await
    }

    async fn update_project(&self, tenant: TenantId, slug: &str, patch: ProjectPatch) -> Result<Project> {
        let name = patch.name.as_deref().map(check_project_name).transpose()?;
        if let Some(envs) = &patch.environments {
            rt_types::check_project_environments(envs).map_err(StoreError::Invalid)?;
        }
        let slug = slug.to_string();
        self.write(move |c| {
            let cur = project_by_slug(c, tenant, &slug)?.ok_or(StoreError::NotFound)?;
            if let Some(envs) = &patch.environments {
                let used = c
                    .prepare_cached("SELECT DISTINCT environment FROM services WHERE project_id = ?1")?
                    .query_map([cur.id.to_string()], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                if used.iter().any(|e| !envs.contains(e)) {
                    return Err(StoreError::Conflict("environment_in_use"));
                }
                set_project_environments(c, cur.id, envs)?;
            }
            let updated = Project {
                name: name.unwrap_or(cur.name),
                environments: patch.environments.unwrap_or(cur.environments),
                updated_at_ms: now_ms(),
                ..cur
            };
            c.execute(
                "UPDATE projects SET name = ?2, updated_at = ?3 WHERE id = ?1",
                params![updated.id.to_string(), updated.name, updated.updated_at_ms],
            )?;
            Ok(updated)
        })
        .await
    }

    async fn delete_project(&self, tenant: TenantId, slug: &str) -> Result<()> {
        let slug = slug.to_string();
        self.write(move |c| {
            let cur = project_by_slug(c, tenant, &slug)?.ok_or(StoreError::NotFound)?;
            let n: i64 = c.query_row(
                "SELECT count(*) FROM services WHERE project_id = ?1",
                [cur.id.to_string()],
                |r| r.get(0),
            )?;
            if n > 0 {
                return Err(StoreError::Conflict("project_not_empty"));
            }
            c.execute("DELETE FROM projects WHERE id = ?1", [cur.id.to_string()])?;
            Ok(())
        })
        .await
    }

    // ---- services & versions -----------------------------------------------------------------

    async fn create_service(&self, scope: TenantScope, tenant: TenantId, s: NewService) -> Result<Service> {
        self.create_service_capped(scope, tenant, s, WarmFloorCap::NONE).await
    }

    async fn create_service_capped(
        &self,
        scope: TenantScope,
        tenant: TenantId,
        s: NewService,
        cap: WarmFloorCap,
    ) -> Result<Service> {
        self.create_service_limited(scope, tenant, s, cap, 0).await
    }

    async fn create_service_limited(
        &self,
        scope: TenantScope,
        tenant: TenantId,
        s: NewService,
        cap: WarmFloorCap,
        max_services: u32,
    ) -> Result<Service> {
        if let TenantScope::Tenant(t) = scope {
            if t != tenant {
                return Err(StoreError::NotFound);
            }
        }
        if s.slug.is_empty() {
            return Err(StoreError::Invalid("slug is empty".into()));
        }
        if !rt_types::valid_environment(&s.environment) {
            return Err(StoreError::Invalid(
                "environment must match ^[a-z][a-z0-9-]{0,31}$".into(),
            ));
        }
        self.write(move |c| {
            require_tenant(c, tenant)?;
            if max_services > 0 {
                let n: i64 = c.query_row("SELECT count(*) FROM services WHERE tenant_id = ?1", [tenant.to_string()], |r| r.get(0))?;
                if u64::try_from(n).unwrap_or(u64::MAX) >= u64::from(max_services) {
                    return Err(StoreError::Conflict("quota_exceeded"));
                }
            }
            check_warm_floor_total(c, tenant, None, s.config.scaling.min_instances.unwrap_or(0), cap)?;
            let now = now_ms();
            let project = project_for_service(c, tenant, &s.project, &s.environment)?;
            let svc = Service {
                id: ServiceId::new(),
                tenant_id: tenant,
                slug: s.slug,
                runtime: s.runtime,
                name: s.name,
                profile: s.profile,
                environment: s.environment,
                project_id: project.id,
                project: project.slug.clone(),
                config: s.config,
                live_version: None,
                etag: Etag(1),
                created_at_ms: now,
                updated_at_ms: now,
            };
            c.execute(
                "INSERT INTO services (id, tenant_id, slug, runtime, name, profile, config, etag, created_at, updated_at, environment, project_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?8, ?9, ?10)",
                params![
                    svc.id.to_string(),
                    tenant.to_string(),
                    svc.slug,
                    svc.runtime.as_str(),
                    svc.name,
                    svc.profile.map(Profile::as_str),
                    to_json(&svc.config)?,
                    now,
                    svc.environment,
                    svc.project_id.to_string()
                ],
            )
            .map_err(|e| if is_unique_violation(&e) { StoreError::Conflict("slug_taken") } else { e.into() })?;
            Ok(svc)
        })
        .await
    }

    async fn get_service(&self, scope: TenantScope, id: ServiceId) -> Result<Option<Service>> {
        self.db.read(move |c| service_by_id(c, scope, id)).await
    }

    async fn list_services(&self, scope: TenantScope, filter: ServiceFilter, page: PageReq) -> Result<Page<Service>> {
        self.db
            .read(move |c| {
                let limit = page_limit(page.limit);
                let mut sql = format!("SELECT {SERVICE_COLS} FROM services WHERE id > ?");
                let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(page.cursor.unwrap_or_default())];
                if let TenantScope::Tenant(t) = scope {
                    sql.push_str(" AND tenant_id = ?");
                    args.push(Box::new(t.to_string()));
                }
                if let Some(slug) = filter.slug {
                    sql.push_str(" AND slug = ?");
                    args.push(Box::new(slug));
                }
                if let Some(project) = filter.project {
                    sql.push_str(" AND project_id = (SELECT id FROM projects WHERE tenant_id = services.tenant_id AND (id = ? OR slug = ?))");
                    args.push(Box::new(project.clone()));
                    args.push(Box::new(project));
                }
                if let Some(environment) = filter.environment {
                    sql.push_str(" AND environment = ?");
                    args.push(Box::new(environment));
                }
                // A key's scope: only services of its projects and environments.
                if let Some(ids) = &filter.project_ids {
                    let list = to_json(&ids.iter().map(ToString::to_string).collect::<Vec<_>>())?;
                    sql.push_str(" AND project_id IN (SELECT value FROM json_each(?))");
                    args.push(Box::new(list));
                }
                if let Some(envs) = &filter.environments {
                    sql.push_str(" AND environment IN (SELECT value FROM json_each(?))");
                    args.push(Box::new(to_json(envs)?));
                }
                sql.push_str(" ORDER BY id LIMIT ?");
                args.push(Box::new(limit as i64 + 1));
                let rows = c
                    .prepare_cached(&sql)?
                    .query_map(rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())), service_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(paginate(rows, limit, |s| s.id.to_string()))
            })
            .await
    }

    async fn count_services(&self, tenant: TenantId) -> Result<u64> {
        self.db
            .read(move |c| {
                let n: i64 = c
                    .prepare_cached("SELECT count(*) FROM services WHERE tenant_id = ?1")?
                    .query_row([tenant.to_string()], |r| r.get(0))?;
                Ok(n.max(0) as u64)
            })
            .await
    }

    async fn busiest_service_schedules(&self, tenant: TenantId) -> Result<u64> {
        self.db
            .read(move |c| {
                let n: i64 = c
                    .prepare_cached(
                        "SELECT COALESCE(MAX(n), 0) FROM (SELECT count(*) AS n FROM schedules sc \
                         JOIN services s ON s.id = sc.service_id WHERE s.tenant_id = ?1 GROUP BY sc.service_id)",
                    )?
                    .query_row([tenant.to_string()], |r| r.get(0))?;
                Ok(n.max(0) as u64)
            })
            .await
    }

    async fn patch_service(
        &self,
        scope: TenantScope,
        id: ServiceId,
        patch: ServicePatch,
        if_match: Option<Etag>,
    ) -> Result<Service> {
        self.patch_service_capped(scope, id, patch, if_match, WarmFloorCap::NONE)
            .await
    }

    async fn patch_service_capped(
        &self,
        scope: TenantScope,
        id: ServiceId,
        patch: ServicePatch,
        if_match: Option<Etag>,
        cap: WarmFloorCap,
    ) -> Result<Service> {
        self.write(move |c| {
            let cur = service_by_id(c, scope, id)?.ok_or(StoreError::NotFound)?;
            if if_match.is_some_and(|e| e != cur.etag) {
                return Err(StoreError::PreconditionFailed);
            }
            let mut config = cur.config.clone();
            if let Some(v) = patch.limits {
                config.limits = v;
            }
            if let Some(v) = patch.scaling {
                check_warm_floor_total(c, cur.tenant_id, Some(id), v.min_instances.unwrap_or(0), cap)?;
                config.scaling = v;
            }
            if let Some(v) = patch.access {
                config.access = v;
            }
            if let Some(v) = patch.triggers {
                config.triggers = v;
            }
            if let Some(v) = patch.ingress {
                config.ingress = v;
            }
            if let Some(v) = patch.egress {
                config.egress = v;
            }
            if let Some(v) = patch.call {
                config.call = v;
            }
            if let Some(v) = patch.wasm {
                config.wasm = v;
            }
            if let Some(v) = patch.deno {
                config.deno = v;
            }
            if let Some(v) = patch.durable {
                config.durable = v;
            }
            if let Some(v) = patch.ws {
                config.ws = v;
            }
            if patch.environment.as_deref().is_some_and(|e| !rt_types::valid_environment(e)) {
                return Err(StoreError::Invalid("environment must match ^[a-z][a-z0-9-]{0,31}$".into()));
            }
            // D-B3: a new environment must be one of the service's project.
            if let Some(env) = patch.environment.as_deref().filter(|e| *e != cur.environment) {
                project_for_service(c, cur.tenant_id, &cur.project_id.to_string(), env)?;
            }
            let svc = Service {
                name: patch.name.or(cur.name),
                profile: patch.profile.or(cur.profile),
                environment: patch.environment.unwrap_or(cur.environment),
                config,
                etag: Etag(cur.etag.0 + 1),
                updated_at_ms: now_ms(),
                ..cur
            };
            c.execute(
                "UPDATE services SET name = ?2, profile = ?3, config = ?4, etag = ?5, updated_at = ?6, environment = ?7 WHERE id = ?1",
                params![
                    id.to_string(),
                    svc.name,
                    svc.profile.map(Profile::as_str),
                    to_json(&svc.config)?,
                    clamp_i64(svc.etag.0),
                    svc.updated_at_ms,
                    svc.environment
                ],
            )?;
            Ok(svc)
        })
        .await
    }

    async fn delete_service(&self, scope: TenantScope, id: ServiceId, _keep_data: bool) -> Result<()> {
        // `keep_data` concerns files owned by the data store / queue; metadata always goes.
        self.write(move |c| {
            service_by_id(c, scope, id)?.ok_or(StoreError::NotFound)?;
            // Versions, env, secrets, hosts and schedules cascade (usage and events are history).
            c.execute("DELETE FROM services WHERE id = ?1", [id.to_string()])?;
            Ok(())
        })
        .await
    }

    async fn add_version(&self, id: ServiceId, v: NewVersion) -> Result<Version> {
        self.write(move |c| {
            require_service(c, id)?;
            let key = id.to_string();
            let number: i64 = c.query_row(
                "SELECT COALESCE(MAX(number), 0) + 1 FROM versions WHERE service_id = ?1",
                [&key],
                |r| r.get(0),
            )?; // LCOV_EXCL_LINE: require_service reads the live version of the same table, so a damaged versions table fails there first
            let now = now_ms();
            let (state, started) = match v.compile {
                CompileMode::Eager => (BuildState::Building, Some(now)),
                CompileMode::Lazy => (BuildState::Pending, None),
            };
            c.execute(
                "INSERT INTO versions (service_id, number, source_digest, lock_digest, build_state, pending_reason, failure, created_at, build_started_at) \
                 VALUES (?1, ?2, ?3, NULL, ?4, NULL, NULL, ?5, ?6)",
                params![key, number, v.source_digest.to_string(), state_str(state), now, started],
            )?;
            Ok(Version {
                service_id: id,
                number: VersionNo(number as u32),
                source_digest: v.source_digest,
                lock_digest: None,
                build_state: state,
                pending_reason: None,
                failure: None,
                created_at_ms: now,
                bundle_digest: None,
            })
        })
        .await
    }

    async fn get_version(&self, id: ServiceId, v: VersionNo) -> Result<Option<Version>> {
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {VERSION_COLS} FROM versions WHERE service_id = ?1 AND number = ?2"
                ))?
                .query_row(params![id.to_string(), v.0], version_row)
                .optional()?)
            })
            .await
    }

    async fn list_versions(&self, id: ServiceId) -> Result<Vec<Version>> {
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {VERSION_COLS} FROM versions WHERE service_id = ?1 ORDER BY number"
                ))?
                .query_map([id.to_string()], version_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn set_build_state(
        &self,
        id: ServiceId,
        v: VersionNo,
        state: BuildState,
        failure: Option<BuildFailure>,
        lock_digest: Option<Digest>,
    ) -> Result<()> {
        self.write(move |c| {
            let failure = failure.as_ref().map(to_json).transpose()?;
            let n = c.execute(
                "UPDATE versions SET build_state = ?3, failure = ?4, lock_digest = COALESCE(?5, lock_digest), \
                 pending_reason = CASE WHEN ?3 = 'pending' THEN pending_reason ELSE NULL END, \
                 build_started_at = CASE WHEN ?3 = 'building' THEN ?6 ELSE build_started_at END \
                 WHERE service_id = ?1 AND number = ?2",
                params![
                    id.to_string(),
                    v.0,
                    state_str(state),
                    failure,
                    lock_digest.map(|d| d.to_string()),
                    now_ms()
                ],
            )?;
            if n == 0 {
                return Err(StoreError::NotFound);
            }
            Ok(())
        })
        .await
    }

    async fn set_live(&self, id: ServiceId, v: VersionNo, if_match: Option<Etag>) -> Result<Service> {
        self.write(move |c| {
            let cur = require_service(c, id)?;
            if if_match.is_some_and(|e| e != cur.etag) {
                return Err(StoreError::PreconditionFailed);
            }
            let state: Option<String> = c
                .query_row(
                    "SELECT build_state FROM versions WHERE service_id = ?1 AND number = ?2",
                    params![id.to_string(), v.0],
                    |r| r.get(0),
                )
                .optional()?;
            match state.as_deref() {
                None => return Err(StoreError::NotFound),
                Some("failed") => return Err(StoreError::Conflict("version_not_ready")),
                Some(_) => {}
            }
            let now = now_ms();
            // One live version per service (`versions_one_live`): clear the old flag first.
            c.execute(
                "UPDATE versions SET is_live = 0 WHERE service_id = ?1 AND is_live = 1",
                [id.to_string()],
            )?;
            c.execute(
                "UPDATE versions SET is_live = 1 WHERE service_id = ?1 AND number = ?2",
                params![id.to_string(), v.0],
            )?;
            c.execute(
                "UPDATE services SET etag = etag + 1, updated_at = ?2 WHERE id = ?1",
                params![id.to_string(), now],
            )?;
            Ok(Service {
                live_version: Some(v),
                etag: Etag(cur.etag.0 + 1),
                updated_at_ms: now,
                ..cur
            })
        })
        .await
    }

    async fn count_builds_since(&self, tenant: TenantId, since_ms: UnixMs) -> Result<u64> {
        self.db
            .read(move |c| {
                let n: i64 = c.query_row(
                    "SELECT count(*) FROM versions v JOIN services s ON s.id = v.service_id \
                     WHERE s.tenant_id = ?1 AND v.build_started_at IS NOT NULL AND v.build_started_at >= ?2",
                    params![tenant.to_string(), since_ms],
                    |r| r.get(0),
                )?;
                Ok(n.max(0) as u64)
            })
            .await
    }

    async fn gc_versions(&self, id: ServiceId, keep: u32) -> Result<Vec<Digest>> {
        self.write(move |c| {
            let key = id.to_string();
            let live: Option<i64> = c
                .query_row("SELECT number FROM versions WHERE service_id = ?1 AND is_live = 1", [&key], |r| r.get(0))
                .optional()?;
            // Everything except the live version and the newest `keep` versions goes.
            let doomed: Vec<(i64, String, Option<String>)> = c
                .prepare(
                    "SELECT number, source_digest, lock_digest FROM versions WHERE service_id = ?1 \
                     AND number NOT IN (SELECT number FROM versions WHERE service_id = ?1 ORDER BY number DESC LIMIT ?2) \
                     AND (?3 IS NULL OR number != ?3)",
                )?
                .query_map(params![key, i64::from(keep), live], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<rusqlite::Result<_>>()?;
            for (number, _, _) in &doomed {
                c.execute("DELETE FROM versions WHERE service_id = ?1 AND number = ?2", params![key, number])?;
            }
            // Candidates: the doomed sources, their locks that no version uses any more, and the
            // modules those locks listed (their module lists go with them).
            let mut candidates: Vec<String> = Vec::new();
            for (_, src, lock) in &doomed {
                candidates.push(src.clone());
                let Some(lock) = lock else { continue };
                let in_use: bool = c
                    .prepare_cached("SELECT EXISTS(SELECT 1 FROM versions WHERE lock_digest = ?1)")?
                    .query_row([lock], |r| r.get(0))?;
                if !in_use {
                    let modules: Vec<String> = c
                        .prepare_cached("SELECT module_digest FROM lock_modules WHERE lock_digest = ?1")?
                        .query_map([lock], |r| r.get(0))?
                        .collect::<rusqlite::Result<_>>()?;
                    c.prepare_cached("DELETE FROM lock_modules WHERE lock_digest = ?1")?.execute([lock])?;
                    candidates.push(lock.clone());
                    candidates.extend(modules);
                }
            }
            // Freed: candidates nothing references any more (content addressing lets one digest
            // be a source, a lock and a module at once).
            let mut freed: Vec<Digest> = Vec::new();
            for digest in &candidates {
                let referenced: bool = c.prepare_cached(DIGEST_REFERENCED_SQL)?.query_row([digest], |r| r.get(0))?;
                if !referenced {
                    let d: Digest = parse_col(digest, 0)?;
                    if !freed.contains(&d) {
                        freed.push(d);
                    }
                }
            }
            Ok(freed)
        })
        .await
    }

    async fn digest_referenced(&self, digest: &Digest) -> Result<bool> {
        let d = digest.to_string();
        self.db
            .read(move |c| Ok(c.prepare_cached(DIGEST_REFERENCED_SQL)?.query_row([d], |r| r.get(0))?))
            .await
    }

    async fn record_lock_modules(&self, lock: &Digest, modules: &[Digest]) -> Result<()> {
        let lock = lock.to_string();
        let modules: Vec<String> = modules.iter().map(ToString::to_string).collect();
        // Not a route change: no generation bump.
        self.db
            .write(move |c| {
                let mut stmt = c.prepare_cached(
                    "INSERT OR IGNORE INTO lock_modules (lock_digest, module_digest) VALUES (?1, ?2)",
                )?;
                for m in &modules {
                    stmt.execute(params![lock, m])?;
                }
                Ok(())
            })
            .await
    }

    async fn set_bundle_digest(&self, id: ServiceId, v: VersionNo, bundle: Digest) -> Result<()> {
        // Audit only: not a route change, no generation bump.
        self.db
            .write(move |c| {
                let n = c.execute(
                    "UPDATE versions SET bundle_digest = ?3 WHERE service_id = ?1 AND number = ?2",
                    params![id.to_string(), v.0, bundle.to_string()],
                )?;
                if n == 0 {
                    return Err(StoreError::NotFound);
                }
                Ok(())
            })
            .await
    }

    async fn lock_for_source(&self, source: &Digest) -> Result<Option<Digest>> {
        let d = source.to_string();
        self.db
            .read(move |c| {
                let lock: Option<String> = c
                    .prepare_cached(
                        "SELECT lock_digest FROM versions WHERE source_digest = ?1 AND lock_digest IS NOT NULL \
                         AND build_state = 'ready' ORDER BY created_at LIMIT 1",
                    )?
                    .query_row([d], |r| r.get(0))
                    .optional()?;
                Ok(lock.map(|l| parse_col(&l, 0)).transpose()?)
            })
            .await
    }

    async fn route_snapshot(&self) -> Result<RouteSnapshot> {
        let generation = self.generation.clone();
        self.db
            .read(move |c| {
                let gen = generation.load(Ordering::Relaxed);
                let mut hosts: BTreeMap<String, Vec<String>> = BTreeMap::new();
                {
                    let mut stmt = c.prepare_cached("SELECT service_id, host FROM hosts WHERE status = 'active' ORDER BY host")?;
                    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
                    for row in rows {
                        let (svc, host) = row?;
                        hosts.entry(svc).or_default().push(host);
                    }
                }
                // Services of a disabled tenant are not routable: no HTTP, schedule, queue or call
                // task can resolve them until the tenant is enabled again.
                let mut stmt = c.prepare_cached(
                    "SELECT s.id, s.tenant_id, s.slug, s.runtime, s.config, s.profile, v.number, v.source_digest, s.environment, s.project_id \
                     FROM services s JOIN versions v ON v.service_id = s.id AND v.is_live = 1 \
                     JOIN tenants t ON t.id = s.tenant_id AND t.disabled = 0 \
                     ORDER BY s.id",
                )?;
                let rows = stmt.query_map([], |r| {
                    let id: String = r.get(0)?;
                    let config: ServiceConfig = json_col(&r.get::<_, String>(4)?, 4)?;
                    Ok(RouteEntry {
                        tenant: parse_col(&r.get::<_, String>(1)?, 1)?,
                        service: parse_col(&id, 0)?,
                        slug: r.get(2)?,
                        runtime: parse_col(&r.get::<_, String>(3)?, 3)?,
                        version: VersionNo(r.get::<_, i64>(6)? as u32),
                        digest: parse_col(&r.get::<_, String>(7)?, 7)?,
                        access: config.access,
                        triggers: config.triggers,
                        ingress: config.ingress,
                        profile: r.get::<_, Option<String>>(5)?.map(|p| parse_col::<Profile>(&p, 5)).transpose()?,
                        hosts: hosts.get(&id).cloned().unwrap_or_default(),
                        project: parse_col(&r.get::<_, String>(9)?, 9)?,
                        environment: r.get(8)?,
                        call: config.call,
                    })
                })?;
                let entries = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                let sdk = sdk_snapshot(c)?;
                Ok(RouteSnapshot { generation: gen, entries, sdk })
            })
            .await
    }

    // ---- operator SDKs -----------------------------------------------------------------------

    async fn ensure_tenant(&self, id: TenantId, name: &str, plan: &str) -> Result<()> {
        let (name, plan) = (name.to_string(), plan.to_string());
        self.write(move |c| {
            if tenant_by_id(c, id)?.is_some() {
                return Ok(());
            }
            let now = now_ms();
            c.execute(
                "INSERT INTO tenants (id, name, plan, overrides, created_at, disabled) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                params![id.to_string(), name, plan, to_json(&serde_json::Value::Null)?, now],
            )?;
            insert_project(c, &default_project_row(id, now))
        })
        .await
    }

    async fn get_sdk(&self, name: &str) -> Result<Option<Sdk>> {
        let name = name.to_string();
        self.db.read(move |c| sdk_by_name(c, &name)).await
    }

    async fn list_sdks(&self) -> Result<Vec<Sdk>> {
        self.db
            .read(|c| {
                Ok(c.prepare_cached(&format!("SELECT {SDK_COLS} FROM sdks ORDER BY name"))?
                    .query_map([], sdk_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn upsert_sdk(&self, name: &str, service: ServiceId, description: Option<String>) -> Result<Sdk> {
        let name = name.to_string();
        self.write(move |c| {
            c.execute(
                "INSERT INTO sdks (name, service_id, description, state, created_at) VALUES (?1, ?2, ?3, 'active', ?4) \
                 ON CONFLICT (name) DO UPDATE SET state = 'active', removed_at = NULL, \
                   description = COALESCE(excluded.description, sdks.description)",
                params![name, service.to_string(), description, now_ms()],
            )?;
            sdk_by_name(c, &name)?.ok_or(StoreError::NotFound)
        })
        .await
    }

    async fn add_sdk_release(&self, r: NewSdkRelease) -> Result<SdkRelease> {
        self.write(move |c| {
            if sdk_by_name(c, &r.sdk)?.is_none() {
                return Err(StoreError::NotFound);
            }
            let members = to_json(&r.members)?;
            c.execute(
                "INSERT INTO sdk_releases (sdk, version, version_no, members, contract, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![r.sdk, r.version, i64::from(r.version_no.0), members, r.contract, now_ms()],
            )
            .map_err(|e| if is_unique_violation(&e) { StoreError::Conflict("sdk_release_exists") } else { e.into() })?;
            Ok(c.prepare_cached(&format!("SELECT {SDK_RELEASE_COLS} FROM sdk_releases WHERE sdk = ?1 AND version = ?2"))?
                .query_row(params![r.sdk, r.version], sdk_release_row)?)
        })
        .await
    }

    async fn list_sdk_releases(&self, sdk: &str) -> Result<Vec<SdkRelease>> {
        let sdk = sdk.to_string();
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {SDK_RELEASE_COLS} FROM sdk_releases WHERE sdk = ?1 ORDER BY created_at, version"
                ))?
                .query_map([sdk], sdk_release_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn get_sdk_release(&self, sdk: &str, version: &str) -> Result<Option<SdkRelease>> {
        let (sdk, version) = (sdk.to_string(), version.to_string());
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {SDK_RELEASE_COLS} FROM sdk_releases WHERE sdk = ?1 AND version = ?2"
                ))?
                .query_row(params![sdk, version], sdk_release_row)
                .optional()?)
            })
            .await
    }

    async fn list_sdk_channels(&self, sdk: &str) -> Result<Vec<SdkChannel>> {
        let sdk = sdk.to_string();
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {SDK_CHANNEL_COLS} FROM sdk_channels WHERE sdk = ?1 ORDER BY channel"
                ))?
                .query_map([sdk], sdk_channel_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn set_sdk_channel(&self, sdk: &str, channel: &str, version: &str) -> Result<SdkChannel> {
        let (sdk, channel, version) = (sdk.to_string(), channel.to_string(), version.to_string());
        self.write(move |c| {
            let exists: Option<i64> = c
                .query_row("SELECT 1 FROM sdk_releases WHERE sdk = ?1 AND version = ?2", params![sdk, version], |r| r.get(0))
                .optional()?;
            if exists.is_none() {
                return Err(StoreError::NotFound);
            }
            c.execute(
                "INSERT INTO sdk_channels (sdk, channel, version, previous, updated_at) VALUES (?1, ?2, ?3, NULL, ?4) \
                 ON CONFLICT (sdk, channel) DO UPDATE SET \
                   previous = CASE WHEN sdk_channels.version = excluded.version THEN sdk_channels.previous ELSE sdk_channels.version END, \
                   version = excluded.version, updated_at = excluded.updated_at",
                params![sdk, channel, version, now_ms()],
            )?;
            Ok(c.prepare_cached(&format!("SELECT {SDK_CHANNEL_COLS} FROM sdk_channels WHERE sdk = ?1 AND channel = ?2"))?
                .query_row(params![sdk, channel], sdk_channel_row)?)
        })
        .await
    }

    async fn remove_sdk(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.write(move |c| {
            let n = c.execute(
                "UPDATE sdks SET state = 'removed', removed_at = ?2 WHERE name = ?1 AND state = 'active'",
                params![name, now_ms()],
            )?;
            Ok(n > 0 || sdk_by_name(c, &name)?.is_some())
        })
        .await
    }

    async fn list_sdk_grants(&self) -> Result<Vec<SdkGrant>> {
        self.db.read(sdk_snapshot_grants).await
    }

    async fn put_sdk_grant(&self, g: SdkGrant) -> Result<Option<SdkGrant>> {
        self.write(move |c| {
            let (kind, id) = g.scope.parts();
            if g.effect.is_none() && g.version_req.is_none() && g.channel.is_none() {
                c.execute("DELETE FROM sdk_grants WHERE scope_kind = ?1 AND scope_id = ?2 AND sdk = ?3", params![kind, id, g.sdk])?;
                return Ok(None);
            }
            let now = now_ms();
            c.execute(
                "INSERT INTO sdk_grants (scope_kind, scope_id, sdk, effect, version_req, channel, created_by, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT (scope_kind, scope_id, sdk) DO UPDATE SET \
                   effect = excluded.effect, version_req = excluded.version_req, channel = excluded.channel, \
                   created_by = excluded.created_by, updated_at = excluded.updated_at",
                params![kind, id, g.sdk, g.effect.map(GrantEffect::as_str), g.version_req, g.channel, g.created_by, now],
            )?;
            Ok(Some(SdkGrant { updated_at_ms: now, ..g }))
        })
        .await
    }

    // ---- env, sealed secrets -----------------------------------------------------------------

    async fn get_env(&self, id: ServiceId) -> Result<Vec<(String, String)>> {
        self.db
            .read(move |c| {
                Ok(
                    c.prepare_cached("SELECT name, value FROM env WHERE service_id = ?1 ORDER BY name")?
                        .query_map([id.to_string()], |r| Ok((r.get(0)?, r.get(1)?)))?
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                )
            })
            .await
    }

    async fn put_env(&self, id: ServiceId, env: Vec<(String, String)>) -> Result<SecretsVersion> {
        if env.len() > MAX_ENV_ENTRIES {
            return Err(StoreError::Invalid(format!(
                "at most {MAX_ENV_ENTRIES} env entries are allowed"
            )));
        }
        for (k, v) in &env {
            check_env_name(k)?;
            if v.len() > MAX_ENV_VALUE_BYTES {
                return Err(StoreError::Invalid(format!("value of `{k}` exceeds 32 KB")));
            }
        }
        let mut names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err(StoreError::Invalid("duplicate env name".into()));
        }
        self.write(move |c| {
            require_service(c, id)?;
            let key = id.to_string();
            let now = now_ms();
            c.execute("DELETE FROM env WHERE service_id = ?1", [&key])?;
            let mut stmt =
                c.prepare_cached("INSERT INTO env (service_id, name, value, updated_at) VALUES (?1, ?2, ?3, ?4)")?;
            for (k, v) in &env {
                stmt.execute(params![key, k, v, now])?;
            }
            drop(stmt);
            bump_secrets_version(c, id)
        })
        .await
    }

    async fn list_secrets(&self, id: ServiceId) -> Result<Vec<SecretMeta>> {
        self.db
            .read(move |c| {
                Ok(
                    c.prepare_cached("SELECT name, updated_at FROM secrets WHERE service_id = ?1 ORDER BY name")?
                        .query_map([id.to_string()], |r| {
                            Ok(SecretMeta {
                                name: r.get(0)?,
                                updated_at_ms: r.get(1)?,
                            })
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                )
            })
            .await
    }

    async fn put_secret(&self, id: ServiceId, name: &str, sealed: Vec<u8>) -> Result<SecretsVersion> {
        check_env_name(name)?;
        if sealed.len() > MAX_SEALED_BYTES {
            return Err(StoreError::Invalid("secret value exceeds 32 KB".into()));
        }
        let name = name.to_string();
        self.write(move |c| {
            require_service(c, id)?;
            let key = id.to_string();
            let exists: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM secrets WHERE service_id = ?1 AND name = ?2)",
                params![key, name],
                |r| r.get(0),
            )?;
            if !exists {
                let n: i64 = c.query_row("SELECT count(*) FROM secrets WHERE service_id = ?1", [&key], |r| r.get(0))?;
                if n as usize >= MAX_ENV_ENTRIES {
                    return Err(StoreError::Invalid(format!("at most {MAX_ENV_ENTRIES} secrets are allowed")));
                }
            }
            c.execute(
                "INSERT INTO secrets (service_id, name, sealed, updated_at) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(service_id, name) DO UPDATE SET sealed = excluded.sealed, updated_at = excluded.updated_at",
                params![key, name, sealed, now_ms()],
            )?;
            bump_secrets_version(c, id)
        })
        .await
    }

    async fn replace_secret_if(&self, id: ServiceId, name: &str, expected: Vec<u8>, sealed: Vec<u8>) -> Result<bool> {
        if sealed.len() > MAX_SEALED_BYTES {
            return Err(StoreError::Invalid("secret value exceeds 32 KB".into()));
        }
        let name = name.to_string();
        self.write(move |c| {
            let n = c.execute(
                "UPDATE secrets SET sealed = ?4, updated_at = ?5 WHERE service_id = ?1 AND name = ?2 AND sealed = ?3",
                params![id.to_string(), name, expected, sealed, now_ms()],
            )?;
            if n > 0 {
                bump_secrets_version(c, id)?;
            }
            Ok(n > 0)
        })
        .await
    }

    async fn delete_secret(&self, id: ServiceId, name: &str) -> Result<SecretsVersion> {
        let name = name.to_string();
        self.write(move |c| {
            let n = c.execute(
                "DELETE FROM secrets WHERE service_id = ?1 AND name = ?2",
                params![id.to_string(), name],
            )?;
            if n == 0 {
                return Err(StoreError::NotFound);
            }
            bump_secrets_version(c, id)
        })
        .await
    }

    async fn get_sealed_secrets(&self, id: ServiceId) -> Result<Vec<(String, Vec<u8>)>> {
        self.db
            .read(move |c| {
                Ok(
                    c.prepare_cached("SELECT name, sealed FROM secrets WHERE service_id = ?1 ORDER BY name")?
                        .query_map([id.to_string()], |r| Ok((r.get(0)?, r.get(1)?)))?
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                )
            })
            .await
    }

    async fn secrets_version(&self, id: ServiceId) -> Result<SecretsVersion> {
        self.db
            .read(move |c| {
                let v: Option<i64> = c
                    .query_row(
                        "SELECT secrets_version FROM services WHERE id = ?1",
                        [id.to_string()],
                        |r| r.get(0),
                    )
                    .optional()?;
                Ok(SecretsVersion(v.unwrap_or(0) as u64))
            })
            .await
    }

    // ---- hosts -------------------------------------------------------------------------------

    async fn put_host(&self, h: HostMapping) -> Result<()> {
        self.write(move |c| {
            require_service(c, h.service_id)?;
            let host = normalize_host(&h.host);
            if host.is_empty() {
                return Err(StoreError::Invalid("host is empty".into()));
            }
            let current: Option<(String, String)> = c
                .query_row("SELECT service_id, status FROM hosts WHERE host = ?1", [&host], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()?;
            let me = h.service_id.to_string();
            let active = host_status_str(HostStatus::Active);
            if let Some((owner, status)) = &current {
                // Another service's claim blocks this one, unless the claim is still unverified
                // and this mapping is an approved (active) one: the operator's approval decides.
                if *owner != me && (status == active || h.status != HostStatus::Active) {
                    return Err(StoreError::Conflict("host_taken"));
                }
                // Re-claiming an already active mapping of the same service keeps it active (a
                // tenant's repeated PUT must not take its own host out of routing).
                if *owner == me && status == active {
                    return Ok(());
                }
            }
            let created_at = if h.created_at_ms > 0 { h.created_at_ms } else { now_ms() };
            c.execute(
                "INSERT INTO hosts (host, service_id, status, verify_token, created_at) VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(host) DO UPDATE SET service_id = excluded.service_id, status = excluded.status, \
                 verify_token = excluded.verify_token, created_at = excluded.created_at",
                params![host, me, host_status_str(h.status), h.verify_token, created_at],
            )?;
            Ok(())
        })
        .await
    }

    async fn delete_host(&self, id: ServiceId, host: &str) -> Result<bool> {
        let host = normalize_host(host);
        self.write(move |c| {
            let n = c.execute(
                "DELETE FROM hosts WHERE host = ?1 AND service_id = ?2",
                params![host, id.to_string()],
            )?;
            Ok(n > 0)
        })
        .await
    }

    async fn list_hosts(&self, id: ServiceId) -> Result<Vec<HostMapping>> {
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {HOST_COLS} FROM hosts WHERE service_id = ?1 ORDER BY host"
                ))?
                .query_map([id.to_string()], host_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn find_host(&self, host: &str) -> Result<Option<HostMapping>> {
        let host = normalize_host(host);
        self.db
            .read(move |c| {
                Ok(
                    c.prepare_cached(&format!("SELECT {HOST_COLS} FROM hosts WHERE host = ?1"))?
                        .query_row([host], host_row)
                        .optional()?,
                )
            })
            .await
    }

    async fn pending_hosts(&self) -> Result<Vec<HostMapping>> {
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {HOST_COLS} FROM hosts WHERE status = 'pending_verification' ORDER BY created_at, host"
                ))?
                .query_map([], host_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn activate_host(&self, host: &str, service: ServiceId, token: &str) -> Result<bool> {
        let (host, token) = (normalize_host(host), token.to_string());
        self.write(move |c| {
            let n = c.execute(
                "UPDATE hosts SET status = 'active', verify_token = NULL \
                 WHERE host = ?1 AND service_id = ?2 AND status = 'pending_verification' AND verify_token = ?3",
                params![host, service.to_string(), token],
            )?;
            Ok(n > 0)
        })
        .await
    }

    async fn expire_pending_hosts(&self, created_before_ms: UnixMs) -> Result<Vec<HostMapping>> {
        self.write(move |c| {
            let expired = c
                .prepare_cached(&format!(
                    "SELECT {HOST_COLS} FROM hosts WHERE status = 'pending_verification' AND created_at < ?1"
                ))?
                .query_map([created_before_ms], host_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            c.execute(
                "DELETE FROM hosts WHERE status = 'pending_verification' AND created_at < ?1",
                [created_before_ms],
            )?;
            Ok(expired)
        })
        .await
    }

    async fn record_host_release(&self, r: HostRelease) -> Result<()> {
        let host = normalize_host(&r.host);
        self.write(move |c| {
            c.execute(
                "INSERT INTO host_releases (host, tenant_id, created_at, verify_token, released_at) VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(host, tenant_id) DO UPDATE SET created_at = excluded.created_at, \
                 verify_token = excluded.verify_token, released_at = excluded.released_at",
                params![host, r.tenant.to_string(), r.created_at_ms, r.verify_token, r.released_at_ms],
            )?;
            Ok(())
        })
        .await
    }

    async fn host_release(&self, host: &str, tenant: TenantId) -> Result<Option<HostRelease>> {
        let host = normalize_host(host);
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(
                    "SELECT host, tenant_id, created_at, verify_token, released_at FROM host_releases \
                     WHERE host = ?1 AND tenant_id = ?2",
                )?
                .query_row(params![host, tenant.to_string()], |r| {
                    Ok(HostRelease {
                        host: r.get(0)?,
                        tenant: parse_col(&r.get::<_, String>(1)?, 1)?,
                        created_at_ms: r.get(2)?,
                        verify_token: r.get(3)?,
                        released_at_ms: r.get(4)?,
                    })
                })
                .optional()?)
            })
            .await
    }

    async fn prune_host_releases(&self, released_before_ms: UnixMs) -> Result<u64> {
        self.write(move |c| {
            let n = c.execute("DELETE FROM host_releases WHERE released_at < ?1", [released_before_ms])?;
            Ok(n as u64)
        })
        .await
    }

    async fn count_pending_hosts(&self, tenant: TenantId) -> Result<u64> {
        self.db
            .read(move |c| {
                let n: i64 = c
                    .prepare_cached(
                        "SELECT count(*) FROM hosts h JOIN services s ON s.id = h.service_id \
                         WHERE s.tenant_id = ?1 AND h.status = 'pending_verification'",
                    )?
                    .query_row([tenant.to_string()], |r| r.get(0))?;
                Ok(n.max(0) as u64)
            })
            .await
    }

    // ---- schedules ---------------------------------------------------------------------------

    async fn create_schedule(&self, id: ServiceId, s: NewSchedule) -> Result<Schedule> {
        check_schedule(&s)?;
        self.write(move |c| {
            require_service(c, id)?;
            let sch = Schedule {
                id: ScheduleId::new(),
                service_id: id,
                kind: s.kind,
                payload: s.payload,
                enabled: s.enabled,
                next_due_ms: None,
            };
            let (kind, every, cron, tz) = schedule_columns(&sch.kind);
            c.execute(
                "INSERT INTO schedules (id, service_id, kind, every_seconds, cron, timezone, payload, enabled, next_due_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
                params![sch.id.to_string(), id.to_string(), kind, every, cron, tz, to_json(&sch.payload)?, i64::from(sch.enabled)],
            )?;
            Ok(sch)
        })
        .await
    }

    async fn list_schedules(&self, id: ServiceId) -> Result<Vec<Schedule>> {
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {SCHEDULE_COLS} FROM schedules WHERE service_id = ?1 ORDER BY id"
                ))?
                .query_map([id.to_string()], schedule_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn all_schedules(&self) -> Result<Vec<Schedule>> {
        self.db
            .read(move |c| {
                Ok(
                    c.prepare_cached(&format!("SELECT {SCHEDULE_COLS} FROM schedules ORDER BY id"))?
                        .query_map([], schedule_row)?
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                )
            })
            .await
    }

    async fn update_schedule(&self, id: ServiceId, sch: ScheduleId, s: NewSchedule) -> Result<Schedule> {
        check_schedule(&s)?;
        self.write(move |c| {
            let (kind, every, cron, tz) = schedule_columns(&s.kind);
            // The trigger engine recomputes `next_due_at` for the changed definition.
            let n = c.execute(
                "UPDATE schedules SET kind = ?3, every_seconds = ?4, cron = ?5, timezone = ?6, payload = ?7, enabled = ?8, \
                   next_due_at = NULL WHERE id = ?1 AND service_id = ?2",
                params![sch.to_string(), id.to_string(), kind, every, cron, tz, to_json(&s.payload)?, i64::from(s.enabled)],
            )?;
            if n == 0 {
                return Err(StoreError::NotFound);
            }
            load_schedule(c, sch)?.ok_or(StoreError::NotFound)
        })
        .await
    }

    async fn delete_schedule(&self, id: ServiceId, sch: ScheduleId) -> Result<bool> {
        self.write(move |c| {
            let n = c.execute(
                "DELETE FROM schedules WHERE id = ?1 AND service_id = ?2",
                params![sch.to_string(), id.to_string()],
            )?;
            Ok(n > 0)
        })
        .await
    }

    async fn set_schedule_next_due(&self, sch: ScheduleId, next_due_ms: Option<UnixMs>) -> Result<()> {
        self.db
            .call(move |c| {
                c.prepare_cached("UPDATE schedules SET next_due_at = ?2 WHERE id = ?1")?
                    .execute(params![sch.to_string(), next_due_ms])?;
                Ok(())
            })
            .await
    }

    // ---- usage, events, cache frequencies ----------------------------------------------------

    async fn add_usage(&self, day: &str, batch: UsageBatch) -> Result<()> {
        let day = day.to_string();
        self.db
            .write(move |c| {
                let mut stmt = c.prepare_cached(
                    "INSERT INTO usage (tenant_id, service_id, day, invocations, cpu_ms, egress_bytes, storage_bytes) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                     ON CONFLICT(tenant_id, service_id, day) DO UPDATE SET \
                       invocations = invocations + excluded.invocations, \
                       cpu_ms = cpu_ms + excluded.cpu_ms, \
                       egress_bytes = egress_bytes + excluded.egress_bytes, \
                       storage_bytes = storage_bytes + excluded.storage_bytes",
                )?; // LCOV_EXCL_LINE: llvm-cov does not count this prepare's error arm; the damaged-usage test takes it
                for (tenant, service, d) in &batch.items {
                    stmt.execute(params![
                        tenant.to_string(),
                        service.to_string(),
                        day,
                        clamp_i64(d.invocations),
                        clamp_i64(d.cpu_ms),
                        clamp_i64(d.egress_bytes),
                        d.storage_bytes
                    ])?;
                }
                Ok(())
            })
            .await
    }

    async fn get_usage(&self, tenant: TenantId, from_day: &str, to_day: &str) -> Result<Vec<UsageRow>> {
        let (from, to) = (from_day.to_string(), to_day.to_string());
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(
                    "SELECT service_id, day, invocations, cpu_ms, egress_bytes, storage_bytes FROM usage \
                     WHERE tenant_id = ?1 AND day >= ?2 AND day <= ?3 ORDER BY day, service_id",
                )?
                .query_map(params![tenant.to_string(), from, to], |r| {
                    Ok(UsageRow {
                        tenant,
                        service: parse_col(&r.get::<_, String>(0)?, 0)?,
                        day: r.get(1)?,
                        invocations: r.get::<_, i64>(2)?.max(0) as u64,
                        cpu_ms: r.get::<_, i64>(3)?.max(0) as u64,
                        egress_bytes: r.get::<_, i64>(4)?.max(0) as u64,
                        storage_bytes: r.get(5)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn append_event(&self, ev: NodeEvent) -> Result<()> {
        self.db
            .call(move |c| {
                c.prepare_cached(
                    "INSERT OR REPLACE INTO events (id, ts, tenant_id, service_id, kind, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )? // LCOV_EXCL_LINE: llvm-cov does not count this prepare's error arm; the damaged-events test takes it
                .execute(params![
                    ev.id.to_string(),
                    ev.ts_ms,
                    ev.tenant.map(|t| t.to_string()),
                    ev.service.map(|s| s.to_string()),
                    ev.kind,
                    to_json(&ev.data)?
                ])?;
                Ok(())
            })
            .await
    }

    /// Newest first (event ids are time-ordered); the cursor is the last id returned.
    async fn list_events(&self, service: ServiceId, page: PageReq) -> Result<Page<NodeEvent>> {
        self.db
            .read(move |c| {
                let limit = page_limit(page.limit);
                let rows = match page.cursor {
                    Some(cur) => c
                        .prepare_cached(&format!(
                            "SELECT {EVENT_COLS} FROM events WHERE service_id = ?1 AND id < ?2 ORDER BY id DESC LIMIT ?3"
                        ))?
                        .query_map(params![service.to_string(), cur, limit as i64 + 1], event_row)?
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                    None => c
                        .prepare_cached(&format!(
                            "SELECT {EVENT_COLS} FROM events WHERE service_id = ?1 ORDER BY id DESC LIMIT ?2"
                        ))?
                        .query_map(params![service.to_string(), limit as i64 + 1], event_row)?
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                };
                Ok(paginate(rows, limit, |e| e.id.to_string()))
            })
            .await
    }

    async fn event_by_id(&self, id: EventId) -> Result<Option<NodeEvent>> {
        self.db
            .read(move |c| {
                Ok(
                    c.prepare_cached(&format!("SELECT {EVENT_COLS} FROM events WHERE id = ?1"))?
                        .query_row([id.to_string()], event_row)
                        .optional()?,
                )
            })
            .await
    }

    async fn save_cache_freq(&self, snapshot: Vec<u8>) -> Result<()> {
        self.db
            .call(move |c| {
                c.execute(
                    "INSERT OR REPLACE INTO cache_freq (id, snapshot) VALUES (1, ?1)",
                    [snapshot],
                )?;
                Ok(())
            })
            .await
    }

    async fn load_cache_freq(&self) -> Result<Option<Vec<u8>>> {
        self.db
            .read(move |c| {
                Ok(
                    c.query_row("SELECT snapshot FROM cache_freq WHERE id = 1", [], |r| r.get(0))
                        .optional()?,
                )
            })
            .await
    }
}
