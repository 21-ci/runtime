//! Per-task host configuration without control-plane round-trips (B2.1): the egress rules come
//! from the node's in-memory service table through [`TenantPolicy::egress_rules`], and the env
//! bundle from [`ControlPlane::env_shared`] (the plane's cache), so a task start does not reach
//! `meta.db`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use rt_host::{
    BindingError, CallError, Calls, EgressGuardConfig, EnqueueOpts, GuardedEgress, HostDeps, HostServices,
    PlatformBindings, Request, Response, TaskCtx, TenantPolicy,
};
use rt_obs::NullLogSink;
use rt_sec::SelfDenylist;
use rt_store::{DataConfig, LocalServiceData, SqliteQueueStore};
use rt_testkit::{SpecBuilder, TestGovernor};
use rt_types::{
    BuildReport, ConfigStream, ControlPlane, CpError, EgressRules, EnvBundle, MsgId, NodeEvent, PreparedKey,
    RouteSnapshot, SecretsVersion, Service, ServiceId, TaskId, TenantId, UsageBatch,
};
use zeroize::Zeroizing;

struct NoCalls;

#[async_trait]
impl Calls for NoCalls {
    async fn call(&self, _from: &TaskCtx, _slug: &str, _req: Request) -> Result<Response, CallError> {
        Err(CallError::NotFound)
    }

    async fn enqueue(&self, _from: &TaskCtx, _slug: &str, _body: Bytes, _o: EnqueueOpts) -> Result<MsgId, CallError> {
        Err(CallError::NotFound)
    }
}

struct NoBindings;

#[async_trait]
impl PlatformBindings for NoBindings {
    async fn fetch(&self, _from: &TaskCtx, _binding: &str, _req: Request) -> Result<Response, BindingError> {
        Err(BindingError::NotFound)
    }
}

/// Counts the calls that would be `meta.db` round-trips in the node. `shared` is what its
/// cache answers `env_shared` with (`None` = the trait default, i.e. `env_for`).
#[derive(Default)]
struct CountingControl {
    service_calls: AtomicU64,
    env_calls: AtomicU64,
    shared: Option<Arc<Zeroizing<EnvBundle>>>,
}

#[async_trait]
impl ControlPlane for CountingControl {
    fn routes(&self) -> Arc<RouteSnapshot> {
        Arc::new(RouteSnapshot::default())
    }

    async fn subscribe_config(&self) -> ConfigStream {
        stream::empty().boxed()
    }

    async fn service(&self, _id: ServiceId) -> Result<Option<Service>, CpError> {
        self.service_calls.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }

    async fn env_for(&self, _service: ServiceId, _version: SecretsVersion) -> Result<Zeroizing<EnvBundle>, CpError> {
        self.env_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Zeroizing::new(EnvBundle {
            env: vec![("FROM".into(), "env_for".into())],
            secrets: Vec::new(),
        }))
    }

    async fn env_shared(
        &self,
        service: ServiceId,
        version: SecretsVersion,
    ) -> Result<Arc<Zeroizing<EnvBundle>>, CpError> {
        match &self.shared {
            Some(b) => Ok(b.clone()),
            None => self.env_for(service, version).await.map(Arc::new),
        }
    }

    async fn report_build(&self, _key: &PreparedKey, _report: BuildReport) -> Result<(), CpError> {
        Ok(())
    }

    fn usage(&self, _batch: UsageBatch) {}

    fn event(&self, _ev: NodeEvent) {}
}

/// A policy with the node's service table: deny stance and the service's rules in memory.
struct TablePolicy {
    rules: EgressRules,
}

impl TenantPolicy for TablePolicy {
    fn egress_default_deny(&self, _tenant: TenantId) -> bool {
        true
    }

    fn egress_bytes_left(&self, _tenant: TenantId) -> Option<u64> {
        Some(1234)
    }

    fn storage_bytes_left(&self, _tenant: TenantId) -> Option<u64> {
        Some(99)
    }

    fn record_egress(&self, _tenant: TenantId, _service: ServiceId, _bytes: u64) {}

    fn egress_rules(&self, _service: ServiceId) -> Option<EgressRules> {
        Some(self.rules.clone())
    }
}

fn host(control: Arc<CountingControl>, policy: Option<Arc<dyn TenantPolicy>>, dir: &tempfile::TempDir) -> HostServices {
    let denylist = Arc::new(SelfDenylist::new(Vec::new(), Vec::new()));
    HostServices::new(HostDeps {
        data: LocalServiceData::open(DataConfig::new(dir.path())).expect("data store"),
        queue: SqliteQueueStore::open_in_memory(5).expect("queue store"),
        delayed: SqliteQueueStore::open_in_memory(5).expect("queue store"),
        egress: Arc::new(GuardedEgress::new(EgressGuardConfig::default(), denylist.clone())),
        denylist,
        calls: Arc::new(NoCalls),
        platform: Arc::new(NoBindings),
        logs: Arc::new(NullLogSink),
        control,
        mem: TestGovernor::with_mb(64),
        sockets: Arc::new(rt_host::NoSockets),
        policy,
    })
}

#[tokio::test]
async fn the_policy_table_answers_the_egress_rules_without_asking_the_control_plane() {
    let dir = tempfile::tempdir().expect("tempdir");
    let control = Arc::new(CountingControl::default());
    let rules = EgressRules {
        allowed_hosts: vec!["api.example.com".into()],
        denied_hosts: vec!["bad.example.com".into()],
    };
    let host = host(control.clone(), Some(Arc::new(TablePolicy { rules })), &dir);
    let spec = SpecBuilder::wasm().finish();

    let cfg = host.config_for(&spec).await;
    assert_eq!(cfg.egress_policy.allowed_hosts, vec!["api.example.com".to_string()]);
    assert_eq!(cfg.egress_policy.denied_hosts, vec!["bad.example.com".to_string()]);
    assert!(cfg.egress_policy.default_deny, "the plan's deny stance");
    assert_eq!(cfg.egress_quota_left, Some(1234));
    assert_eq!(cfg.facade_limits.storage_bytes_left, Some(99));
    assert_eq!(
        control.service_calls.load(Ordering::SeqCst),
        0,
        "no control-plane (meta.db) lookup per task"
    );
}

#[tokio::test]
async fn without_a_service_table_the_control_plane_is_asked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let control = Arc::new(CountingControl::default());
    let host = host(control.clone(), None, &dir);
    let cfg = host.config_for(&SpecBuilder::wasm().finish()).await;
    assert!(cfg.egress_policy.allowed_hosts.is_empty());
    assert_eq!(control.service_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_handle_takes_the_env_bundle_the_control_plane_shares() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bundle = Arc::new(Zeroizing::new(EnvBundle {
        env: vec![("FROM".into(), "cache".into())],
        secrets: Vec::new(),
    }));
    let control = Arc::new(CountingControl {
        shared: Some(bundle.clone()),
        ..CountingControl::default()
    });
    let host = host(control.clone(), None, &dir);
    let spec = SpecBuilder::wasm().finish();
    let deadline = Instant::now() + Duration::from_secs(5);

    for task in 1..=3 {
        let cfg = host.config_for(&spec).await;
        let handle = host.handle_for(&spec, TaskId(task), deadline, cfg);
        let env = handle.env().await.expect("env");
        assert!(Arc::ptr_eq(&env, &bundle), "the shared bundle itself, not a fresh copy");
    }
    assert_eq!(control.env_calls.load(Ordering::SeqCst), 0, "no per-task env fetch");
}

// ---------------------------------------------------------------------------------------------
// std:durable storage behind the output gate
// ---------------------------------------------------------------------------------------------

fn val(s: &str) -> rt_store::DurableValue {
    rt_store::DurableValue {
        format: rt_store::DURABLE_FORMAT_V8,
        data: Bytes::copy_from_slice(s.as_bytes()),
    }
}

fn keys(page: &[(Bytes, rt_store::DurableValue)]) -> Vec<String> {
    page.iter()
        .map(|(k, _)| String::from_utf8(k.to_vec()).expect("utf8"))
        .collect()
}

#[tokio::test]
async fn object_storage_writes_wait_for_commit_and_reads_see_them_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host(Arc::new(CountingControl::default()), None, &dir);
    let deadline = Instant::now() + Duration::from_secs(30);
    let spec = SpecBuilder::wasm()
        .finish()
        .with_object(rt_types::ObjectRef::named("room"));
    let h = host.handle_for(&spec, TaskId(1), deadline, Default::default());
    h.storage_put(Bytes::from_static(b"a"), val("1")).expect("put");
    h.storage_put(Bytes::from_static(b"b"), val("2")).expect("put");
    assert_eq!(
        h.storage_get(&[Bytes::from_static(b"a")]).await.expect("get"),
        vec![Some(val("1"))],
        "own write"
    );
    // Nothing is stored before the commit: another task of the object sees nothing.
    let other = host.handle_for(&spec, TaskId(2), deadline, Default::default());
    assert_eq!(
        other.storage_get(&[Bytes::from_static(b"a")]).await.expect("get"),
        vec![None]
    );
    h.commit().await.expect("commit");
    assert_eq!(
        other.storage_get(&[Bytes::from_static(b"a")]).await.expect("get"),
        vec![Some(val("1"))]
    );
    // A pending delete and put are merged into a list of the stored range.
    assert_eq!(
        other.storage_delete(&[Bytes::from_static(b"a")]).await.expect("delete"),
        1
    );
    other.storage_put(Bytes::from_static(b"c"), val("3")).expect("put");
    let page = other
        .storage_list(rt_store::DurableListOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .expect("list");
    assert_eq!(keys(&page), ["b", "c"]);
    drop(other); // a task that fails never commits: its writes are gone
    let third = host.handle_for(&spec, TaskId(3), deadline, Default::default());
    let page = third
        .storage_list(rt_store::DurableListOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .expect("list");
    assert_eq!(keys(&page), ["a", "b"]);
    // Alarms ride the same commit; deleteAll clears keys and alarm.
    third.alarm_set(Some(42)).expect("alarm");
    assert_eq!(third.alarm_get().await.expect("alarm"), Some(42));
    third.commit().await.expect("commit");
    let fourth = host.handle_for(&spec, TaskId(4), deadline, Default::default());
    assert_eq!(fourth.alarm_get().await.expect("alarm"), Some(42));
    fourth.storage_delete_all().expect("delete all");
    fourth.commit().await.expect("commit");
    let fifth = host.handle_for(&spec, TaskId(5), deadline, Default::default());
    assert!(fifth
        .storage_list(rt_store::DurableListOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .expect("list")
        .is_empty());
    assert_eq!(fifth.alarm_get().await.expect("alarm"), None);
}

/// Security review pass 2: a multi-key read names at most 1000 keys and returns at most 10 MiB,
/// so a few hundred KB of repeated keys cannot make the host copy gigabytes of one big value.
#[tokio::test]
async fn multi_key_reads_are_capped_in_keys_and_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host(Arc::new(CountingControl::default()), None, &dir);
    let deadline = Instant::now() + Duration::from_secs(30);
    let big = Bytes::from(vec![7u8; 1 << 20]);
    let key = Bytes::from_static(b"k");

    let h = host.handle_for(&SpecBuilder::wasm().finish(), TaskId(1), deadline, Default::default());
    let kv = h.kv();
    kv.set(
        &key,
        rt_store::KvValue {
            tag: rt_store::ValueTag::Bytes,
            data: big.clone(),
        },
        None,
    )
    .await
    .expect("set");
    assert_eq!(kv.get_many(&vec![key.clone(); 10]).await.expect("10 MiB").len(), 10);
    assert_eq!(
        kv.get_many(&vec![key.clone(); 11]).await.map_err(|e| e.code).err(),
        Some("validation_failed"),
        "over 10 MiB"
    );
    let small: Vec<Bytes> = (0..1001).map(|i| Bytes::from(format!("s{i}"))).collect();
    assert_eq!(
        kv.get_many(&small).await.map_err(|e| e.code).err(),
        Some("validation_failed"),
        "over 1000 keys"
    );

    let spec = SpecBuilder::wasm()
        .finish()
        .with_object(rt_types::ObjectRef::named("o"));
    let o = host.handle_for(&spec, TaskId(2), deadline, Default::default());
    o.storage_put(
        key.clone(),
        rt_store::DurableValue {
            format: rt_store::DURABLE_FORMAT_V8,
            data: big,
        },
    )
    .expect("put");
    assert_eq!(o.storage_get(&vec![key.clone(); 10]).await.expect("10 MiB").len(), 10);
    assert_eq!(
        o.storage_get(&vec![key.clone(); 11]).await.map_err(|e| e.code).err(),
        Some("validation_failed")
    );
    assert_eq!(
        o.storage_get(&small).await.map_err(|e| e.code).err(),
        Some("validation_failed")
    );
}

#[tokio::test]
async fn object_storage_needs_an_object_and_counts_against_the_storage_quota() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rules = EgressRules::default();
    let host = host(
        Arc::new(CountingControl::default()),
        Some(Arc::new(TablePolicy { rules })),
        &dir,
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let plain = host.handle_for(&SpecBuilder::wasm().finish(), TaskId(1), deadline, Default::default());
    assert_eq!(
        plain
            .storage_put(Bytes::from_static(b"k"), val("v"))
            .map_err(|e| e.code),
        Err("no_object")
    );
    let spec = SpecBuilder::wasm()
        .finish()
        .with_object(rt_types::ObjectRef::named("o"));
    let cfg = host.config_for(&spec).await; // the policy leaves 99 bytes of storage
    let h = host.handle_for(&spec, TaskId(2), deadline, cfg);
    h.storage_put(Bytes::from_static(b"k"), val(&"x".repeat(90)))
        .expect("fits");
    assert_eq!(
        h.storage_put(Bytes::from_static(b"l"), val(&"x".repeat(20)))
            .map_err(|e| e.code),
        Err("storage_quota")
    );
}
