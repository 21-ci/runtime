//! Plan enforcement the host and data layers ask the node for (SECURITY.md): a plan's egress
//! stance, the tenant's remaining egress and storage quota, and the owner and size ceiling of each
//! service's database.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use rt_host::TenantPolicy;
use rt_sec::{PlanSource, QuotaEnforcer};
use rt_store::ServiceDirectory;
use rt_types::{EgressDefault, EgressRules, Service, ServiceId, TenantId, UsageDelta};

const MIB: u64 = 1024 * 1024;

/// [`TenantPolicy`] + [`ServiceDirectory`] from the tenant plans, the quota counters and the
/// node's service table.
pub struct PlanPolicy {
    plans: Arc<dyn PlanSource>,
    quotas: Arc<dyn QuotaEnforcer>,
    services: Arc<ArcSwap<HashMap<ServiceId, Arc<Service>>>>,
    data_dir: PathBuf,
    /// Bytes on disk per tenant (databases + WAL + blob objects), from the last
    /// [`PlanPolicy::scan_storage`].
    storage: DashMap<TenantId, u64>,
}

impl PlanPolicy {
    pub fn new(
        plans: Arc<dyn PlanSource>,
        quotas: Arc<dyn QuotaEnforcer>,
        services: Arc<ArcSwap<HashMap<ServiceId, Arc<Service>>>>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            plans,
            quotas,
            services,
            data_dir: data_dir.into(),
            storage: DashMap::new(),
        }
    }

    /// Re-measure every service's storage on disk and total it per tenant. Blocking file
    /// metadata calls: run it on the blocking pool.
    pub fn scan_storage(&self) {
        let services = self.services.load();
        let mut per_tenant: HashMap<TenantId, u64> = HashMap::new();
        for svc in services.values() {
            *per_tenant.entry(svc.tenant_id).or_default() += service_bytes(&self.data_dir, svc.id);
        }
        self.storage.retain(|t, _| per_tenant.contains_key(t));
        for (t, bytes) in per_tenant {
            self.storage.insert(t, bytes);
        }
    }

    /// Storage the tenant used at the last scan.
    pub fn storage_used(&self, tenant: TenantId) -> u64 {
        self.storage.get(&tenant).map_or(0, |v| *v)
    }
}

fn file_len(p: &Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn dir_bytes(p: &Path, depth: u8) -> u64 {
    let Ok(entries) = std::fs::read_dir(p) else { return 0 };
    entries
        .flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_file() => e.metadata().map(|m| m.len()).unwrap_or(0),
            Ok(t) if t.is_dir() && depth > 0 => dir_bytes(&e.path(), depth - 1),
            _ => 0,
        })
        .sum()
}

/// `services/<id>/data.db` (+ WAL) and `blobs/<id>/…`: KV, guest SQL and blob objects.
fn service_bytes(root: &Path, s: ServiceId) -> u64 {
    let db = root.join("services").join(s.to_string());
    file_len(&db.join("data.db"))
        + file_len(&db.join("data.db-wal"))
        + dir_bytes(&root.join("blobs").join(s.to_string()), 2)
}

impl TenantPolicy for PlanPolicy {
    fn egress_default_deny(&self, tenant: TenantId) -> bool {
        self.plans.plan_arc(tenant).egress_default == EgressDefault::Deny
    }

    fn egress_bytes_left(&self, tenant: TenantId) -> Option<u64> {
        self.quotas.egress_left(tenant)
    }

    fn storage_bytes_left(&self, tenant: TenantId) -> Option<u64> {
        let limit = self.plans.plan_arc(tenant).storage_mb.saturating_mul(MIB);
        Some(limit.saturating_sub(self.storage_used(tenant)))
    }

    fn record_egress(&self, tenant: TenantId, service: ServiceId, bytes: u64) {
        self.quotas.record(
            tenant,
            service,
            UsageDelta {
                egress_bytes: bytes,
                ..UsageDelta::default()
            },
        );
    }

    /// CPU a streamed response's tail used after the invocation was billed (rt-api `invoke`).
    fn record_cpu(&self, tenant: TenantId, service: ServiceId, cpu: std::time::Duration) {
        let cpu_ms = cpu.as_millis() as u64;
        if cpu_ms > 0 {
            self.quotas.record(
                tenant,
                service,
                UsageDelta {
                    cpu_ms,
                    ..UsageDelta::default()
                },
            );
        }
    }

    /// From the service table, which every control change refreshes (no `meta.db` read per task).
    fn egress_rules(&self, service: ServiceId) -> Option<EgressRules> {
        Some(
            self.services
                .load()
                .get(&service)
                .map(|svc| svc.config.egress.clone())
                .unwrap_or_default(),
        )
    }
}

impl ServiceDirectory for PlanPolicy {
    fn tenant_of(&self, s: ServiceId) -> Option<TenantId> {
        self.services.load().get(&s).map(|svc| svc.tenant_id)
    }

    /// A database may grow by what is left of its tenant's quota across all its services (plan
    /// `storage_mb` minus everything the tenant stored at the last scan), so N services cannot
    /// each fill the whole quota; the data store re-applies it to open databases after each scan.
    /// `storage_mb = 0` allows nothing; a service with no known owner may not grow at all.
    fn db_growth_bytes(&self, s: ServiceId) -> Option<u64> {
        match self.tenant_of(s) {
            Some(t) => Some(
                self.plans
                    .plan_arc(t)
                    .storage_mb
                    .saturating_mul(MIB)
                    .saturating_sub(self.storage_used(t)),
            ),
            None => Some(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FreePlans;

    impl PlanSource for FreePlans {
        fn plan_for(&self, _tenant: TenantId) -> rt_types::Plan {
            rt_types::Plan::default()
        }
    }

    #[test]
    fn egress_rules_come_from_the_service_table() {
        let s = ServiceId::from_u128(7);
        let mut svc = Service {
            id: s,
            tenant_id: TenantId::from_u128(1),
            slug: "a".into(),
            runtime: rt_types::RuntimeKind::Wasm,
            name: None,
            profile: None,
            environment: rt_types::default_environment(),
            project_id: rt_types::ProjectId::from_u128(1),
            project: rt_types::default_project(),
            config: rt_types::ServiceConfig::default(),
            live_version: None,
            etag: rt_types::Etag(1),
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        svc.config.egress.allowed_hosts = vec!["api.example.com".into()];
        let table = Arc::new(ArcSwap::from_pointee(HashMap::from([(s, Arc::new(svc))])));
        let plans: Arc<dyn PlanSource> = Arc::new(FreePlans);
        let quotas: Arc<dyn QuotaEnforcer> = Arc::new(rt_sec::LocalQuotaEnforcer::new(plans.clone()));
        let policy = PlanPolicy::new(plans, quotas, table, "/nonexistent");
        let rules = policy.egress_rules(s).expect("the policy keeps a service table");
        assert_eq!(rules.allowed_hosts, vec!["api.example.com".to_string()]);
        assert_eq!(
            policy.egress_rules(ServiceId::from_u128(8)),
            Some(EgressRules::default()),
            "unknown: no rules"
        );
    }

    #[test]
    fn service_bytes_counts_database_wal_and_blob_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ServiceId::from_u128(7);
        let db = dir.path().join("services").join(s.to_string());
        let objects = dir.path().join("blobs").join(s.to_string()).join("objects");
        std::fs::create_dir_all(&db).expect("db dir");
        std::fs::create_dir_all(&objects).expect("blob dir");
        std::fs::write(db.join("data.db"), vec![0u8; 1000]).expect("db");
        std::fs::write(db.join("data.db-wal"), vec![0u8; 200]).expect("wal");
        std::fs::write(db.join("data.db-shm"), vec![0u8; 32_768]).expect("shm (not data)");
        std::fs::write(objects.join("a"), vec![0u8; 30]).expect("blob");
        assert_eq!(service_bytes(dir.path(), s), 1230);
        assert_eq!(
            service_bytes(dir.path(), ServiceId::from_u128(8)),
            0,
            "a service with no files uses nothing"
        );
    }

    #[test]
    fn every_database_of_a_tenant_grows_only_by_the_tenants_remaining_quota() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tenant = TenantId::from_u128(1);
        let svc = |n: u128| Service {
            id: ServiceId::from_u128(n),
            tenant_id: tenant,
            slug: format!("s{n}"),
            runtime: rt_types::RuntimeKind::Wasm,
            name: None,
            profile: None,
            environment: rt_types::default_environment(),
            project_id: rt_types::ProjectId::from_u128(1),
            project: rt_types::default_project(),
            config: rt_types::ServiceConfig::default(),
            live_version: None,
            etag: rt_types::Etag(1),
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let table = Arc::new(ArcSwap::from_pointee(HashMap::from([
            (ServiceId::from_u128(1), Arc::new(svc(1))),
            (ServiceId::from_u128(2), Arc::new(svc(2))),
        ])));
        let plans: Arc<dyn PlanSource> = Arc::new(FreePlans);
        let quotas: Arc<dyn QuotaEnforcer> = Arc::new(rt_sec::LocalQuotaEnforcer::new(plans.clone()));
        let policy = PlanPolicy::new(plans, quotas, table, dir.path());
        let quota = rt_types::Plan::default().storage_mb * MIB;
        for n in [1u128, 2] {
            let db = dir.path().join("services").join(ServiceId::from_u128(n).to_string());
            std::fs::create_dir_all(&db).expect("db dir");
            std::fs::write(db.join("data.db"), vec![0u8; (10 * MIB) as usize]).expect("db");
        }
        policy.scan_storage();
        for n in [1u128, 2] {
            assert_eq!(
                policy.db_growth_bytes(ServiceId::from_u128(n)),
                Some(quota - 20 * MIB),
                "the rest of the tenant's quota, shared"
            );
        }
        assert_eq!(
            policy.db_growth_bytes(ServiceId::from_u128(99)),
            Some(0),
            "no owner: no growth"
        );
    }

    /// A quota enforcer that remembers what it was told to record.
    #[derive(Default)]
    struct Recorder(parking_lot::Mutex<Vec<(TenantId, ServiceId, rt_types::UsageDelta)>>);

    impl QuotaEnforcer for Recorder {
        // LCOV_EXCL_START: a test double: `PlanPolicy` only records, it never asks to check
        fn check(&self, _tenant: TenantId, _op: rt_sec::QuotaOp) -> Result<(), rt_sec::QuotaError> {
            Ok(())
        }
        // LCOV_EXCL_STOP

        fn record(&self, tenant: TenantId, service: ServiceId, delta: rt_types::UsageDelta) {
            self.0.lock().push((tenant, service, delta));
        }
    }

    #[test]
    fn egress_and_streamed_cpu_are_recorded_against_the_tenants_usage() {
        let recorder = Arc::new(Recorder::default());
        let plans: Arc<dyn PlanSource> = Arc::new(FreePlans);
        let table = Arc::new(ArcSwap::from_pointee(HashMap::new()));
        let policy = PlanPolicy::new(plans, recorder.clone(), table, "/nonexistent");
        let (tenant, service) = (TenantId::from_u128(1), ServiceId::from_u128(2));
        policy.record_egress(tenant, service, 4096);
        policy.record_cpu(tenant, service, std::time::Duration::from_millis(250));
        policy.record_cpu(tenant, service, std::time::Duration::from_micros(400)); // under a millisecond: nothing to bill
        let seen: Vec<_> = recorder
            .0
            .lock()
            .iter()
            .map(|(t, s, d)| (*t, *s, d.egress_bytes, d.cpu_ms))
            .collect();
        assert_eq!(seen, [(tenant, service, 4096, 0), (tenant, service, 0, 250)]);
    }

    #[test]
    fn storage_is_measured_through_nested_blob_directories_only_down_to_two_levels() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ServiceId::from_u128(7);
        let blobs = dir.path().join("blobs").join(s.to_string());
        std::fs::create_dir_all(blobs.join("a").join("b").join("c")).expect("dirs");
        std::fs::write(blobs.join("top"), vec![0u8; 1]).expect("top");
        std::fs::write(blobs.join("a").join("mid"), vec![0u8; 10]).expect("mid");
        std::fs::write(blobs.join("a").join("b").join("low"), vec![0u8; 100]).expect("low");
        std::fs::write(blobs.join("a").join("b").join("c").join("too-deep"), vec![0u8; 1000]).expect("deep");
        std::os::unix::fs::symlink("/nonexistent", blobs.join("link")).expect("symlink (neither file nor directory)");
        assert_eq!(service_bytes(dir.path(), s), 111);
    }
}
