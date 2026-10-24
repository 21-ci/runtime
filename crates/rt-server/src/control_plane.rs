//! In-process `ControlPlane` (MVP: `--role=all`, the supervisor/executor split is a follow-up).
//!
//! The same trait later gets an IPC client in the executor and a server in the supervisor; here
//! it is a thin adapter over `MetaStore`, `SecretsBackend` and the route table.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use futures_util::stream::{self, StreamExt};
use parking_lot::Mutex;
use rt_sec::{secret_aad, QuotaEnforcer, QuotaError, QuotaOp, Sealed, SecretsBackend};
use rt_store::{MetaStore, PageReq, ServiceFilter, TenantScope};
use rt_triggers::RouteTable;
use rt_types::{
    BuildReport, BuildState, ConfigEvent, ConfigSnapshot, ConfigStream, ControlPlane, CpError, Digest, EnvBundle,
    NodeEvent, PreparedKey, RouteSnapshot, SecretsVersion, Service, ServiceId, TenantId, UsageBatch, UsageDelta,
    VersionNo,
};
use tokio::sync::{mpsc, OnceCell};
use zeroize::Zeroizing;

/// A service's env bundle as loaded once: the secrets generation read before the load, and the
/// decrypted bundle every task of the service shares.
type EnvEntry = (SecretsVersion, Arc<Zeroizing<EnvBundle>>);

/// One cache slot. The first task start loads it (single flight); an env or secrets change
/// replaces the whole slot, so a load that raced with the change fills a detached slot that is
/// never served again.
#[derive(Default)]
struct EnvSlot {
    cell: OnceCell<EnvEntry>,
    /// Wall-clock ms of the last task start served from this slot (coarse: updated at most once
    /// a second), for [`InProcControlPlane::evict_idle_env`].
    last_used_ms: AtomicI64,
}

impl EnvSlot {
    fn touch(&self) {
        let now = chrono::Utc::now().timestamp_millis();
        if now.saturating_sub(self.last_used_ms.load(Ordering::Relaxed)) >= 1_000 {
            self.last_used_ms.store(now, Ordering::Relaxed);
        }
    }
}

/// Decrypted secrets of a service nobody ran for this long are dropped from memory.
pub const ENV_IDLE_EVICT: std::time::Duration = std::time::Duration::from_secs(600);

/// Usage of one (tenant, service) since the last flush. Adders hold the map's shard read lock,
/// so concurrent requests of a hot service do not serialize on one lock.
#[derive(Default)]
struct UsageCounters {
    invocations: AtomicU64,
    cpu_ms: AtomicU64,
    egress_bytes: AtomicU64,
    storage_bytes: AtomicI64,
}

impl UsageCounters {
    fn add(&self, d: &UsageDelta) {
        self.invocations.fetch_add(d.invocations, Ordering::Relaxed);
        self.cpu_ms.fetch_add(d.cpu_ms, Ordering::Relaxed);
        self.egress_bytes.fetch_add(d.egress_bytes, Ordering::Relaxed);
        self.storage_bytes.fetch_add(d.storage_bytes, Ordering::Relaxed);
    }

    /// Take what accumulated (counters restart at zero).
    fn take(&self) -> UsageDelta {
        UsageDelta {
            invocations: self.invocations.swap(0, Ordering::Relaxed),
            cpu_ms: self.cpu_ms.swap(0, Ordering::Relaxed),
            egress_bytes: self.egress_bytes.swap(0, Ordering::Relaxed),
            storage_bytes: self.storage_bytes.swap(0, Ordering::Relaxed),
        }
    }

    fn is_zero(&self) -> bool {
        self.invocations.load(Ordering::Relaxed) == 0
            && self.cpu_ms.load(Ordering::Relaxed) == 0
            && self.egress_bytes.load(Ordering::Relaxed) == 0
            && self.storage_bytes.load(Ordering::Relaxed) == 0
    }
}

pub struct InProcControlPlane {
    meta: Arc<dyn MetaStore>,
    secrets: Arc<dyn SecretsBackend>,
    routes: Arc<RouteTable>,
    plans: Mutex<BTreeMap<String, rt_types::Plan>>,
    /// Versions waiting for a build, by source digest (eager uploads and rebuilds).
    pending_builds: DashMap<Digest, Vec<(ServiceId, VersionNo)>>,
    usage: DashMap<(TenantId, ServiceId), UsageCounters>,
    /// Env + decrypted secrets per service: task starts never reach `meta.db` (B2.1).
    /// Invalidated by [`InProcControlPlane::invalidate_env`] on env/secrets changes and service
    /// deletion.
    env: DashMap<ServiceId, Arc<EnvSlot>>,
    events: mpsc::UnboundedSender<NodeEvent>,
    auto_rollback: bool,
}

impl InProcControlPlane {
    pub fn new(meta: Arc<dyn MetaStore>, secrets: Arc<dyn SecretsBackend>, routes: Arc<RouteTable>) -> Self {
        let (events, rx) = mpsc::unbounded_channel();
        let me = Self {
            meta,
            secrets,
            routes,
            plans: Mutex::new(BTreeMap::new()),
            pending_builds: DashMap::new(),
            usage: DashMap::new(),
            env: DashMap::new(),
            events,
            auto_rollback: true,
        };
        me.spawn_event_writer(rx);
        me
    }

    pub fn with_plans(self, plans: BTreeMap<String, rt_types::Plan>) -> Self {
        *self.plans.lock() = plans;
        self
    }

    /// Replace the plans served in config snapshots (`SIGHUP` reload).
    pub fn set_plans(&self, plans: BTreeMap<String, rt_types::Plan>) {
        *self.plans.lock() = plans;
    }

    pub fn with_auto_rollback(mut self, on: bool) -> Self {
        self.auto_rollback = on;
        self
    }

    /// Where `NodeEvent`s go: the writer task appends them to the `events` table.
    pub fn event_sender(&self) -> mpsc::UnboundedSender<NodeEvent> {
        self.events.clone()
    }

    fn spawn_event_writer(&self, mut rx: mpsc::UnboundedReceiver<NodeEvent>) {
        let meta = self.meta.clone();
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                if let Err(e) = meta.append_event(ev).await {
                    tracing::warn!(error = %e, "could not append event");
                }
            }
        });
    }

    /// Forget `service`'s cached env bundle: its env or secrets changed, or it was deleted. The
    /// next task start reloads it. A load still running is not stored (its slot is gone).
    pub fn invalidate_env(&self, service: ServiceId) {
        self.env.remove(&service);
    }

    /// Drop the cached bundles (decrypted secrets) of services without a task start for
    /// `max_idle`; the next task start reloads. Returns how many were dropped.
    pub fn evict_idle_env(&self, now_ms: i64, max_idle: std::time::Duration) -> usize {
        let idle_ms = i64::try_from(max_idle.as_millis()).unwrap_or(i64::MAX);
        let before = self.env.len();
        self.env
            .retain(|_, slot| now_ms.saturating_sub(slot.last_used_ms.load(Ordering::Relaxed)) < idle_ms);
        before.saturating_sub(self.env.len())
    }

    /// The live cache slot of `service`; a fresh one when the cached bundle is older than
    /// `version`.
    fn env_slot(&self, service: ServiceId, version: SecretsVersion) -> Arc<EnvSlot> {
        let mut slot = self.env.entry(service).or_default();
        if slot.cell.get().is_some_and(|(v, _)| *v < version) {
            *slot = Arc::new(EnvSlot::default());
        }
        slot.value().clone()
    }

    /// One `meta.db` load of `service`'s env bundle. The generation is read first, so a change
    /// committed during the load leaves the entry looking older, never newer, than its content.
    async fn load_env(&self, service: ServiceId) -> Result<EnvEntry, CpError> {
        let version = self
            .meta
            .secrets_version(service)
            .await
            .map_err(|e| CpError::Internal(e.code().to_string()))?;
        let bundle = self.env_for(service, version).await?;
        Ok((version, Arc::new(bundle)))
    }

    /// Remember that `service`'s `version` waits for the build of `digest`.
    pub fn register_build(&self, digest: Digest, service: ServiceId, version: VersionNo) {
        let mut e = self.pending_builds.entry(digest).or_default();
        if !e.contains(&(service, version)) {
            e.push((service, version));
        }
    }

    /// Add one usage delta to the counters [`InProcControlPlane::flush_usage`] persists.
    pub fn add_usage(&self, tenant: TenantId, service: ServiceId, delta: &UsageDelta) {
        // Add under the shard read lock: `flush_usage` drops idle entries under the write lock,
        // so a delta never lands in an entry that was already removed.
        if let Some(c) = self.usage.get(&(tenant, service)) {
            c.add(delta);
            return;
        }
        self.usage.entry((tenant, service)).or_default().add(delta);
    }

    /// Persist the accumulated usage deltas (called every 5 s and at shutdown).
    pub async fn flush_usage(&self) {
        let keys: Vec<(TenantId, ServiceId)> = self.usage.iter().map(|e| *e.key()).collect();
        let mut drained = Vec::new();
        for key in keys {
            let Some(delta) = self.usage.get(&key).map(|c| c.take()) else {
                continue;
            };
            if delta == UsageDelta::default() {
                // Idle for a whole flush period: drop the entry (deleted services do not pile up).
                self.usage.remove_if(&key, |_, c| c.is_zero());
            } else {
                drained.push((key.0, key.1, delta));
            }
        }
        if drained.is_empty() {
            return;
        }
        let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let batch = UsageBatch { items: drained };
        if let Err(e) = self.meta.add_usage(&day, batch).await {
            tracing::warn!(error = %e, "usage flush failed");
        }
    }

    /// Run the periodic usage flush until aborted.
    pub fn spawn_usage_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                me.flush_usage().await;
            }
        })
    }
}

/// The node's quota enforcer plus the persistence of what it records: every accounted invocation
/// also lands in the usage counters that [`InProcControlPlane::flush_usage`] writes to `meta.db`
/// every 5 s (persistence table: usage counters survive a restart).
pub struct MeteredQuotas {
    inner: Arc<dyn QuotaEnforcer>,
    control: Arc<InProcControlPlane>,
}

impl MeteredQuotas {
    pub fn new(inner: Arc<dyn QuotaEnforcer>, control: Arc<InProcControlPlane>) -> Self {
        Self { inner, control }
    }
}

impl QuotaEnforcer for MeteredQuotas {
    fn check(&self, tenant: rt_types::TenantId, op: QuotaOp) -> Result<(), QuotaError> {
        self.inner.check(tenant, op)
    }

    fn record(&self, tenant: rt_types::TenantId, service: ServiceId, delta: UsageDelta) {
        self.control.add_usage(tenant, service, &delta);
        self.inner.record(tenant, service, delta);
    }

    fn egress_left(&self, tenant: rt_types::TenantId) -> Option<u64> {
        self.inner.egress_left(tenant)
    }

    fn usage_today(&self, tenant: rt_types::TenantId) -> Option<rt_sec::QuotaUsage> {
        self.inner.usage_today(tenant)
    }
}

#[async_trait]
impl ControlPlane for InProcControlPlane {
    fn routes(&self) -> Arc<RouteSnapshot> {
        Arc::new(RouteSnapshot {
            generation: self.routes.generation(),
            ..RouteSnapshot::default()
        })
    }

    async fn subscribe_config(&self) -> ConfigStream {
        let routes = self.meta.route_snapshot().await.unwrap_or_default();
        let mut services: Vec<Service> = Vec::new();
        let mut cursor = None;
        while let Ok(page) = self
            .meta
            .list_services(
                TenantScope::Operator,
                ServiceFilter::default(),
                PageReq { limit: 200, cursor },
            )
            .await
        {
            services.extend(page.items);
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        let schedules = self.meta.all_schedules().await.unwrap_or_default();
        let snap = ConfigSnapshot {
            routes: Arc::new(routes),
            services,
            schedules,
            plans: self.plans.lock().clone(),
        };
        // Later deltas are not pushed in the single-process MVP (the node applies changes directly).
        stream::once(async move { ConfigEvent::Snapshot(Arc::new(snap)) })
            .chain(stream::pending())
            .boxed()
    }

    async fn service(&self, id: ServiceId) -> Result<Option<Service>, CpError> {
        self.meta
            .get_service(TenantScope::Operator, id)
            .await
            .map_err(|e| CpError::Internal(e.code().to_string()))
    }

    async fn env_for(&self, service: ServiceId, _version: SecretsVersion) -> Result<Zeroizing<EnvBundle>, CpError> {
        let svc = self
            .meta
            .get_service(TenantScope::Operator, service)
            .await
            .map_err(|e| CpError::Internal(e.code().to_string()))?
            .ok_or(CpError::NotFound)?;
        let env = self
            .meta
            .get_env(service)
            .await
            .map_err(|e| CpError::Internal(e.code().to_string()))?;
        let sealed = self
            .meta
            .get_sealed_secrets(service)
            .await
            .map_err(|e| CpError::Internal(e.code().to_string()))?;
        let mut secrets = Vec::with_capacity(sealed.len());
        for (name, bytes) in sealed {
            let opened = Sealed::from_bytes(&bytes)
                .ok()
                .and_then(|s| self.secrets.open(&secret_aad(svc.tenant_id, service, &name), &s).ok());
            match opened {
                Some(v) => secrets.push((name, String::from_utf8_lossy(&v).into_owned())),
                None => tracing::warn!(service = %service, secret = %name, "a sealed secret could not be opened"),
            }
        }
        Ok(Zeroizing::new(EnvBundle { env, secrets }))
    }

    /// From the env cache: `meta.db` is read (and the secrets opened) once per service until its
    /// env or secrets change. A cached bundle older than `version` is reloaded.
    async fn env_shared(
        &self,
        service: ServiceId,
        version: SecretsVersion,
    ) -> Result<Arc<Zeroizing<EnvBundle>>, CpError> {
        if let Some(slot) = self.env.get(&service) {
            if let Some((v, bundle)) = slot.cell.get() {
                if *v >= version {
                    slot.touch();
                    return Ok(bundle.clone());
                }
            }
        }
        let slot = self.env_slot(service, version);
        slot.touch();
        let (_, bundle) = slot.cell.get_or_try_init(|| self.load_env(service)).await?;
        Ok(bundle.clone())
    }

    async fn report_build(&self, key: &PreparedKey, report: BuildReport) -> Result<(), CpError> {
        let mut targets: Vec<(ServiceId, VersionNo)> = self
            .pending_builds
            .remove(&key.digest)
            .map(|(_, v)| v)
            .unwrap_or_default();
        if let Ok(snap) = self.meta.route_snapshot().await {
            for e in snap
                .entries
                .iter()
                .filter(|e| e.digest == key.digest && e.runtime == key.kind)
            {
                if !targets.contains(&(e.service, e.version)) {
                    targets.push((e.service, e.version));
                }
            }
        }
        let (state, failure) = match &report.result {
            Ok(()) => (BuildState::Ready, None),
            Err(f) => (BuildState::Failed, Some(f.clone())),
        };
        for (service, version) in targets {
            if let Err(e) = self
                .meta
                .set_build_state(service, version, state, failure.clone(), report.lock_digest)
                .await
            {
                tracing::debug!(%service, version = version.0, error = %e, "build state not recorded");
            }
            if let (BuildState::Ready, Some(bundle)) = (state, report.bundle_digest) {
                if let Err(e) = self.meta.set_bundle_digest(service, version, bundle).await {
                    tracing::debug!(%service, version = version.0, error = %e, "bundle digest not recorded");
                }
            }
            if state == BuildState::Failed {
                let mut ev = NodeEvent::new("build.failed", chrono::Utc::now().timestamp_millis());
                ev.service = Some(service);
                ev.data =
                    serde_json::json!({"version": version.0, "reason": failure.as_ref().map(|f| f.reason.as_str())});
                let _ = self.events.send(ev);
                if self.auto_rollback {
                    self.auto_rollback(service, version).await;
                }
            }
        }
        Ok(())
    }

    fn usage(&self, batch: UsageBatch) {
        for (t, s, d) in batch.items {
            self.add_usage(t, s, &d);
        }
    }

    fn event(&self, ev: NodeEvent) {
        let _ = self.events.send(ev);
    }
}

impl InProcControlPlane {
    /// A failed build of the live version flips `live` back to the newest ready version
    ///.
    async fn auto_rollback(&self, service: ServiceId, failed: VersionNo) {
        let Ok(Some(svc)) = self.meta.get_service(TenantScope::Operator, service).await else {
            return;
        };
        if svc.live_version != Some(failed) {
            return;
        }
        let Ok(versions) = self.meta.list_versions(service).await else {
            return;
        };
        let Some(prev) = versions
            .iter()
            .filter(|v| v.number < failed && v.build_state == BuildState::Ready)
            .max_by_key(|v| v.number)
        else {
            return;
        };
        if self.meta.set_live(service, prev.number, None).await.is_ok() {
            if let Ok(snap) = self.meta.route_snapshot().await {
                self.routes.swap(&snap);
            }
            let mut ev = NodeEvent::new("rollback.auto", chrono::Utc::now().timestamp_millis());
            ev.service = Some(service);
            ev.data = serde_json::json!({"from": failed.0, "to": prev.number.0});
            let _ = self.events.send(ev);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_sec::{LocalSecrets, MasterKey};
    use rt_store::SqliteMetaStore;
    use rt_types::{NewService, NewTenant, RuntimeKind};

    struct Plane {
        _dir: tempfile::TempDir,
        meta: Arc<dyn MetaStore>,
        cp: InProcControlPlane,
        tenant: TenantId,
        service: ServiceId,
    }

    async fn plane() -> Plane {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta: Arc<dyn MetaStore> = SqliteMetaStore::open(&dir.path().join("meta.db")).expect("meta.db");
        let key = MasterKey::from_hex(&"11".repeat(32)).expect("master key");
        let secrets: Arc<dyn SecretsBackend> = Arc::new(LocalSecrets::new(&key));
        let t = meta
            .create_tenant(NewTenant {
                name: "t".into(),
                plan: "free".into(),
                overrides: None,
            })
            .await
            .expect("tenant");
        let new = NewService {
            slug: "envs".into(),
            runtime: RuntimeKind::Wasm,
            name: None,
            profile: None,
            environment: rt_types::default_environment(),
            project: rt_types::default_project(),
            config: Default::default(),
        };
        let s = meta
            .create_service(TenantScope::Operator, t.id, new)
            .await
            .expect("service");
        let cp = InProcControlPlane::new(meta.clone(), secrets, RouteTable::new(None));
        Plane {
            _dir: dir,
            meta,
            cp,
            tenant: t.id,
            service: s.id,
        }
    }

    impl Plane {
        async fn put_secret(&self, name: &str, value: &str) -> SecretsVersion {
            let sealed = self
                .cp
                .secrets
                .seal(&secret_aad(self.tenant, self.service, name), value.as_bytes());
            self.meta
                .put_secret(self.service, name, sealed.to_bytes())
                .await
                .expect("put secret")
        }

        async fn env(&self, version: SecretsVersion) -> Arc<Zeroizing<EnvBundle>> {
            self.cp.env_shared(self.service, version).await.expect("env bundle")
        }
    }

    #[tokio::test]
    async fn the_env_bundle_is_loaded_once_and_reloaded_after_a_change() {
        let p = plane().await;
        p.meta
            .put_env(p.service, vec![("MODE".into(), "a".into())])
            .await
            .expect("env");
        p.put_secret("TOKEN", "one").await;

        let first = p.env(SecretsVersion::default()).await;
        assert_eq!(first.env, vec![("MODE".to_string(), "a".to_string())]);
        assert_eq!(first.secrets, vec![("TOKEN".to_string(), "one".to_string())]);
        assert!(
            Arc::ptr_eq(&first, &p.env(SecretsVersion::default()).await),
            "the second task start is served from the cache"
        );

        // Written behind the cache's back: not seen until announced...
        let v = p
            .meta
            .put_env(p.service, vec![("MODE".into(), "b".into())])
            .await
            .expect("env");
        assert_eq!(p.env(SecretsVersion::default()).await.env[0].1, "a");
        // ...unless the caller knows the newer generation.
        assert_eq!(p.env(v).await.env[0].1, "b");

        // A rotated secret is served once the change is announced (`SecretsChanged`).
        p.put_secret("TOKEN", "two").await;
        p.cp.invalidate_env(p.service);
        assert_eq!(p.env(SecretsVersion::default()).await.secrets[0].1, "two");
    }

    /// Review finding 12: decrypted secrets do not stay in memory for services nobody runs.
    #[tokio::test]
    async fn idle_env_bundles_are_evicted_and_reloaded_on_the_next_start() {
        let p = plane().await;
        p.put_secret("TOKEN", "one").await;
        let first = p.env(SecretsVersion::default()).await;
        let now = chrono::Utc::now().timestamp_millis();
        assert_eq!(p.cp.evict_idle_env(now, ENV_IDLE_EVICT), 0, "just used: kept");
        assert_eq!(
            p.cp.evict_idle_env(now + 11 * 60_000, ENV_IDLE_EVICT),
            1,
            "idle for 11 min: dropped"
        );
        assert!(p.cp.env.is_empty());
        let again = p.env(SecretsVersion::default()).await;
        assert!(!Arc::ptr_eq(&first, &again), "reloaded from meta.db");
        assert_eq!(again.secrets[0].1, "one");
    }

    #[tokio::test]
    async fn usage_is_summed_per_service_and_flushed_once() {
        let p = plane().await;
        let d = UsageDelta {
            invocations: 1,
            cpu_ms: 2,
            egress_bytes: 3,
            storage_bytes: 4,
        };
        p.cp.add_usage(p.tenant, p.service, &d);
        p.cp.usage(UsageBatch {
            items: vec![(p.tenant, p.service, d), (p.tenant, p.service, d)],
        });
        p.cp.flush_usage().await;
        p.cp.flush_usage().await;
        let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let rows = p.meta.get_usage(p.tenant, &day, &day).await.expect("usage rows");
        let sums: Vec<_> = rows
            .iter()
            .map(|r| (r.invocations, r.cpu_ms, r.egress_bytes, r.storage_bytes))
            .collect();
        assert_eq!(sums, vec![(3, 6, 9, 12)], "three deltas, persisted once");
        assert!(
            p.cp.usage.is_empty(),
            "an entry idle for a whole flush period is dropped"
        );
    }

    // ---- the rest of the ControlPlane contract -------------------------------------------------

    /// A control plane over a meta store that can be made to fail.
    struct Faulty {
        _dir: tempfile::TempDir,
        meta: Arc<SqliteMetaStore>,
        faults: Arc<rt_store::Faults>,
        cp: InProcControlPlane,
        tenant: TenantId,
        service: ServiceId,
    }

    async fn faulty() -> Faulty {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = SqliteMetaStore::open(&dir.path().join("meta.db")).expect("meta.db");
        let faults = rt_store::Faults::new();
        meta.inject_faults(faults.clone());
        let secrets: Arc<dyn SecretsBackend> =
            Arc::new(LocalSecrets::new(&MasterKey::from_hex(&"11".repeat(32)).expect("key")));
        let t = meta
            .create_tenant(NewTenant {
                name: "t".into(),
                plan: "free".into(),
                overrides: None,
            })
            .await
            .expect("tenant");
        let new = NewService {
            slug: "svc".into(),
            runtime: RuntimeKind::Wasm,
            name: None,
            profile: None,
            environment: rt_types::default_environment(),
            project: rt_types::default_project(),
            config: Default::default(),
        };
        let s = meta
            .create_service(TenantScope::Operator, t.id, new)
            .await
            .expect("service");
        let cp = InProcControlPlane::new(meta.clone(), secrets, RouteTable::new(None));
        Faulty {
            _dir: dir,
            meta,
            faults,
            cp,
            tenant: t.id,
            service: s.id,
        }
    }

    #[tokio::test]
    async fn an_event_the_store_cannot_append_is_logged_and_the_next_one_still_lands() {
        let (_guard, logs) = rt_testkit::capture_tracing();
        let f = faulty().await;
        let mut lost = NodeEvent::new("first.lost", 1);
        lost.service = Some(f.service);
        let mut kept = NodeEvent::new("second.kept", 2);
        kept.service = Some(f.service);
        f.faults.fail_next(1, rt_store::Fault::Io);
        f.cp.event(lost);
        f.cp.event(kept);
        for _ in 0..200 {
            if f.faults.pending() == 0 && logs.contains("could not append event") {
                let page = f
                    .meta
                    .list_events(
                        f.service,
                        PageReq {
                            limit: 10,
                            cursor: None,
                        },
                    )
                    .await
                    .expect("events");
                if page.items.iter().any(|e| e.kind == "second.kept") {
                    assert!(page.items.iter().all(|e| e.kind != "first.lost"));
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the writer did not recover: {}", logs.text());
    }

    #[tokio::test]
    async fn a_usage_flush_the_store_refuses_is_logged_and_the_deltas_are_not_kept() {
        let (_guard, logs) = rt_testkit::capture_tracing();
        let f = faulty().await;
        f.cp.add_usage(
            f.tenant,
            f.service,
            &UsageDelta {
                invocations: 3,
                ..UsageDelta::default()
            },
        );
        f.faults.fail_next(1, rt_store::Fault::Io);
        f.cp.flush_usage().await;
        assert!(logs.contains("usage flush failed"), "{}", logs.text());
    }

    #[tokio::test]
    async fn the_config_snapshot_lists_services_schedules_and_plans_and_then_stays_quiet() {
        let f = faulty().await;
        f.cp.set_plans(BTreeMap::from([("free".to_string(), rt_types::Plan::default())]));
        let mut stream = f.cp.subscribe_config().await;
        let Some(ConfigEvent::Snapshot(snap)) = stream.next().await else {
            panic!("a snapshot first")
        };
        assert_eq!(snap.services.iter().map(|s| s.id).collect::<Vec<_>>(), vec![f.service]);
        assert!(snap.plans.contains_key("free"));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), stream.next())
                .await
                .is_err(),
            "no deltas are pushed"
        );
        assert_eq!(f.cp.routes().generation, f.cp.routes.generation());
    }

    #[tokio::test]
    async fn service_and_env_lookups_say_not_found_or_pass_the_stores_error_on() {
        let f = faulty().await;
        assert_eq!(
            f.cp.service(f.service).await.expect("lookup").map(|s| s.id),
            Some(f.service)
        );
        let missing = ServiceId::from_u128(999);
        assert!(f.cp.service(missing).await.expect("lookup").is_none());
        assert!(matches!(
            f.cp.env_for(missing, SecretsVersion::default()).await,
            Err(CpError::NotFound)
        ));

        f.faults.fail_next(1, rt_store::Fault::Io);
        assert!(matches!(f.cp.service(f.service).await, Err(CpError::Internal(_))));
        f.faults.fail_next(1, rt_store::Fault::Io);
        assert!(matches!(
            f.cp.env_for(f.service, SecretsVersion::default()).await,
            Err(CpError::Internal(_))
        ));
    }

    #[tokio::test]
    async fn a_failed_build_of_the_live_version_goes_back_to_the_newest_ready_one_or_stays_when_there_is_none() {
        let f = faulty().await;
        let version = |n: u8| rt_types::NewVersion {
            source_digest: rt_types::Digest::of(&[n]),
            compile: rt_types::CompileMode::default(),
        };
        let v1 = f.meta.add_version(f.service, version(1)).await.expect("v1").number;
        let v2 = f.meta.add_version(f.service, version(2)).await.expect("v2").number;
        let live = |f: &Faulty| {
            let meta = f.meta.clone();
            let service = f.service;
            async move {
                meta.get_service(TenantScope::Operator, service)
                    .await
                    .expect("get")
                    .expect("service")
                    .live_version
            }
        };
        f.meta.set_live(f.service, v2, None).await.expect("live");

        f.cp.auto_rollback(f.service, VersionNo(99)).await; // not the live version: nothing to do
        assert_eq!(live(&f).await, Some(v2));
        f.cp.auto_rollback(f.service, v2).await; // nothing is ready yet
        assert_eq!(live(&f).await, Some(v2), "no earlier ready version to go back to");

        f.meta
            .set_build_state(f.service, v1, BuildState::Ready, None, None)
            .await
            .expect("ready");
        f.cp.auto_rollback(f.service, v2).await;
        assert_eq!(live(&f).await, Some(v1));
    }

    #[tokio::test]
    async fn env_and_secret_reads_that_fail_after_the_service_was_found_are_internal_errors() {
        let f = faulty().await;
        f.faults.pass_next(1);
        f.faults.fail_next(1, rt_store::Fault::Io); // get_service passes, get_env fails
        assert!(matches!(
            f.cp.env_for(f.service, SecretsVersion::default()).await,
            Err(CpError::Internal(_))
        ));
        f.faults.pass_next(2);
        f.faults.fail_next(1, rt_store::Fault::Io); // get_env passes too, get_sealed_secrets fails
        assert!(matches!(
            f.cp.env_for(f.service, SecretsVersion::default()).await,
            Err(CpError::Internal(_))
        ));
    }

    #[tokio::test]
    async fn a_rollback_the_store_refuses_leaves_the_live_version_and_raises_no_event() {
        let f = faulty().await;
        let version = |n: u8| rt_types::NewVersion {
            source_digest: rt_types::Digest::of(&[n]),
            compile: Default::default(),
        };
        let v1 = f.meta.add_version(f.service, version(1)).await.expect("v1").number;
        let v2 = f.meta.add_version(f.service, version(2)).await.expect("v2").number;
        f.meta
            .set_build_state(f.service, v1, BuildState::Ready, None, None)
            .await
            .expect("ready");
        f.meta.set_live(f.service, v2, None).await.expect("live");
        f.faults.pass_next(2); // get_service, list_versions
        f.faults.fail_next(1, rt_store::Fault::Io); // set_live
        f.cp.auto_rollback(f.service, v2).await;
        let live = f
            .meta
            .get_service(TenantScope::Operator, f.service)
            .await
            .expect("get")
            .expect("service")
            .live_version;
        assert_eq!(live, Some(v2));
    }

    #[tokio::test]
    async fn the_event_writer_ends_and_lets_go_of_the_store_when_the_control_plane_is_dropped() {
        let f = faulty().await;
        let held_by_plane_and_writer = Arc::strong_count(&f.meta);
        drop(f.cp);
        for _ in 0..200 {
            if Arc::strong_count(&f.meta) < held_by_plane_and_writer - 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the writer kept the store: {}", Arc::strong_count(&f.meta));
    }
}
