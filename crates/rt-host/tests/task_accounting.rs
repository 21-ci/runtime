//! Per-task accounting of the host handle:
//! - the storage quota left at task start is decremented by every KV / blob write of the task, so
//!   many writes cannot each pass the start-of-task check (each just under the quota);
//! - CPU charged after the outcome (a streamed response's tail) reaches the tenant's usage when
//!   the handle is dropped;
//! - a detached handle (pre-instantiated WASM instances, idle instances) refuses every capability.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use parking_lot::Mutex;
use rt_host::{
    BindingError, CallError, Calls, EgressError, EgressGuardConfig, EnqueueOpts, GuardedEgress, HostDeps, HostServices,
    PlatformBindings, Request, Response, TaskCtx, TenantPolicy,
};
use rt_obs::NullLogSink;
use rt_sec::SelfDenylist;
use rt_store::{DataConfig, KvValue, LocalServiceData, SqliteQueueStore, ValueTag};
use rt_testkit::{SpecBuilder, TestGovernor};
use rt_types::{
    Body, BuildReport, ConfigStream, ControlPlane, CpError, EnvBundle, MsgId, NodeEvent, PreparedKey, RouteSnapshot,
    SecretsVersion, Service, ServiceId, TaskId, TenantId, Trigger, UsageBatch, MAX_CALL_DEPTH,
};
use zeroize::Zeroizing;

struct NoCalls;

#[async_trait]
impl Calls for NoCalls {
    async fn call(&self, _from: &TaskCtx, _slug: &str, _req: Request) -> Result<Response, CallError> {
        Ok(http::Response::new(Body::empty()))
    }

    async fn enqueue(&self, _from: &TaskCtx, _slug: &str, _body: Bytes, _o: EnqueueOpts) -> Result<MsgId, CallError> {
        Ok(MsgId::from_u128(1))
    }
}

struct NoBindings;

#[async_trait]
impl PlatformBindings for NoBindings {
    async fn fetch(&self, _from: &TaskCtx, _binding: &str, _req: Request) -> Result<Response, BindingError> {
        Ok(http::Response::new(Body::empty()))
    }
}

struct Control;

#[async_trait]
impl ControlPlane for Control {
    fn routes(&self) -> Arc<RouteSnapshot> {
        Arc::new(RouteSnapshot::default())
    }
    async fn subscribe_config(&self) -> ConfigStream {
        stream::empty().boxed()
    }
    async fn service(&self, _id: ServiceId) -> Result<Option<Service>, CpError> {
        Ok(None)
    }
    async fn env_for(&self, _service: ServiceId, _version: SecretsVersion) -> Result<Zeroizing<EnvBundle>, CpError> {
        Ok(Zeroizing::new(EnvBundle::default()))
    }
    async fn report_build(&self, _key: &PreparedKey, _report: BuildReport) -> Result<(), CpError> {
        Ok(())
    }
    fn usage(&self, _batch: UsageBatch) {}
    fn event(&self, _ev: NodeEvent) {}
}

#[derive(Default)]
struct Policy {
    cpu: Mutex<Vec<(ServiceId, Duration)>>,
}

impl TenantPolicy for Policy {
    fn egress_default_deny(&self, _tenant: TenantId) -> bool {
        false
    }
    fn egress_bytes_left(&self, _tenant: TenantId) -> Option<u64> {
        None
    }
    fn storage_bytes_left(&self, _tenant: TenantId) -> Option<u64> {
        Some(100)
    }
    fn record_egress(&self, _tenant: TenantId, _service: ServiceId, _bytes: u64) {}
    fn record_cpu(&self, _tenant: TenantId, service: ServiceId, cpu: Duration) {
        self.cpu.lock().push((service, cpu));
    }
}

fn host(policy: Arc<Policy>, dir: &tempfile::TempDir) -> HostServices {
    host_with_calls(policy, dir, Arc::new(NoCalls))
}

fn host_with_calls(policy: Arc<Policy>, dir: &tempfile::TempDir, calls: Arc<dyn Calls>) -> HostServices {
    let denylist = Arc::new(SelfDenylist::new(Vec::new(), Vec::new()));
    HostServices::new(HostDeps {
        data: LocalServiceData::open(DataConfig::new(dir.path())).expect("data store"),
        queue: SqliteQueueStore::open_in_memory(5).expect("queue store"),
        delayed: SqliteQueueStore::open_in_memory(5).expect("queue store"),
        egress: Arc::new(GuardedEgress::new(EgressGuardConfig::default(), denylist.clone())),
        denylist,
        calls,
        platform: Arc::new(NoBindings),
        logs: Arc::new(NullLogSink),
        control: Arc::new(Control),
        mem: TestGovernor::with_mb(64),
        sockets: Arc::new(rt_host::NoSockets),
        policy: Some(policy),
    })
}

fn bytes(n: usize) -> KvValue {
    KvValue {
        tag: ValueTag::Bytes,
        data: Bytes::from(vec![b'x'; n]),
    }
}

#[tokio::test]
async fn the_storage_quota_left_shrinks_with_every_write_of_the_task() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host(Arc::new(Policy::default()), &dir);
    let spec = SpecBuilder::wasm().finish();
    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, TaskId(1), Instant::now() + Duration::from_secs(5), cfg);

    // 100 bytes left: two writes of 40 (+ 1-byte keys) pass, the third does not
    handle.kv().set(b"a", bytes(40), None).await.expect("first write");
    handle.kv().set(b"b", bytes(40), None).await.expect("second write");
    let err = handle
        .kv()
        .set(b"c", bytes(40), None)
        .await
        .expect_err("over the quota left");
    assert_eq!(err.code, "storage_quota");
    // a blob counts against the same remainder (18 bytes are left)
    let err = handle
        .blob()
        .put("k", Body::from(vec![0u8; 19]), None)
        .await
        .expect_err("blob over the quota left");
    assert_eq!(err.code, "storage_quota");
    handle
        .blob()
        .put("k", Body::from(vec![0u8; 10]), None)
        .await
        .expect("blob within");
    let err = handle.kv().set(b"d", bytes(8), None).await.expect_err("8 left");
    assert_eq!(err.code, "storage_quota");
}

/// HOST-003: a task's storage quota is charged for growth, not per write: overwriting one key
/// again and again (a counter, a session) is charged once for its largest value, so it never runs
/// into a false `storage_quota` while the stored bytes stay the same.
#[tokio::test]
async fn host_003_overwrites_of_one_key_are_not_charged_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host(Arc::new(Policy::default()), &dir);
    let spec = SpecBuilder::wasm().finish();
    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, TaskId(1), Instant::now() + Duration::from_secs(5), cfg);
    let kv = handle.kv();
    // 100 bytes left: 50 overwrites of 40 bytes under one key all fit.
    for i in 0..50 {
        kv.set(b"a", bytes(40), None)
            .await
            .unwrap_or_else(|e| panic!("overwrite {i}: {e:?}"));
    }
    assert_eq!(handle.storage_left(), Some(100 - 41), "charged once");
    kv.set(b"a", bytes(50), None)
        .await
        .expect("a larger value is charged its growth only");
    assert_eq!(handle.storage_left(), Some(100 - 51));
    for i in 0..200 {
        kv.incr(b"n", 1, None)
            .await
            .unwrap_or_else(|e| panic!("increment {i}: {e:?}"));
    }
    assert_eq!(
        handle.storage_left(),
        Some(100 - 51 - (1 + rt_host::facade::MAX_COUNTER_BYTES as u64))
    );
    // A new key is still charged in full, and the quota still holds.
    let err = kv.set(b"b", bytes(40), None).await.expect_err("over the quota left");
    assert_eq!(err.code, "storage_quota");
}

/// STO-17 (host side): `kv.incr` passes the same key limits and storage quota as `set`, charged
/// for the largest counter text (`MAX_COUNTER_BYTES`) and refunded when the store refuses, and the
/// store's refusals reach the guest as stable codes.
#[tokio::test]
async fn kv_incr_is_charged_like_a_set_and_its_refusals_keep_stable_codes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host(Arc::new(Policy::default()), &dir);
    let spec = SpecBuilder::wasm().finish();
    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, TaskId(1), Instant::now() + Duration::from_secs(5), cfg);
    let kv = handle.kv();
    let per_incr = 1 + rt_host::facade::MAX_COUNTER_BYTES as u64;
    let json = |s: &str| KvValue {
        tag: ValueTag::Json,
        data: Bytes::from(s.to_string()),
    };

    assert_eq!(kv.incr(b"n", 1, None).await.expect("first"), 1);
    assert_eq!(
        kv.incr(b"n", 41, Some(Duration::from_secs(60))).await.expect("second"),
        42
    );
    assert_eq!(
        handle.storage_left(),
        Some(100 - per_incr),
        "one counter is charged once (HOST-003)"
    );
    assert_eq!(
        kv.incr(b"", 1, None).await.expect_err("empty key").code,
        "validation_failed"
    );
    assert_eq!(
        kv.incr(&[b'k'; 2049], 1, None).await.expect_err("long key").code,
        "kv_key_too_large"
    );

    kv.set(b"s", json("\"x\""), None).await.expect("string");
    kv.set(b"m", json(&i64::MAX.to_string()), None).await.expect("max");
    let left = handle.storage_left();
    assert_eq!(
        kv.incr(b"s", 1, None).await.expect_err("a string").code,
        "kv_not_integer"
    );
    assert_eq!(kv.incr(b"m", 1, None).await.expect_err("past i64").code, "kv_overflow");
    assert_eq!(handle.storage_left(), left, "a refused increment is refunded");

    // 55 bytes left: the existing counter keeps counting, two new counters fit, a third does not.
    assert_eq!(handle.storage_left(), Some(100 - per_incr - 4 - 20));
    assert_eq!(kv.incr(b"n", 1, None).await.expect("third"), 43);
    assert_eq!(kv.incr(b"o", 1, None).await.expect("a new counter"), 1);
    assert_eq!(kv.incr(b"p", 1, None).await.expect("another"), 1);
    assert_eq!(
        kv.incr(b"q", 1, None).await.expect_err("over the quota left").code,
        "storage_quota"
    );
    assert_eq!(kv.get(b"n").await.expect("get"), Some(json("43")));
}

#[tokio::test]
async fn cpu_charged_after_the_outcome_reaches_the_policy_when_the_handle_drops() {
    let dir = tempfile::tempdir().expect("tempdir");
    let policy = Arc::new(Policy::default());
    let host = host(policy.clone(), &dir);
    let spec = SpecBuilder::wasm().service(7).finish();
    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, TaskId(1), Instant::now() + Duration::from_secs(5), cfg);
    handle.charge_cpu(Duration::from_millis(30));
    handle.charge_cpu(Duration::from_millis(12));
    let clone = handle.clone();
    drop(handle);
    assert!(
        policy.cpu.lock().is_empty(),
        "recorded once, when the last holder is gone"
    );
    drop(clone);
    assert_eq!(
        *policy.cpu.lock(),
        vec![(spec.service.service, Duration::from_millis(42))]
    );
}

#[tokio::test]
async fn a_detached_handle_refuses_every_capability() {
    let dir = tempfile::tempdir().expect("tempdir");
    let policy = Arc::new(Policy::default());
    let host = host(policy.clone(), &dir);
    let spec = SpecBuilder::wasm().finish();
    let h = host.detached_handle(spec.service);
    assert!(h.is_detached());
    let req = || {
        http::Request::builder()
            .uri("https://example.com/")
            .body(Body::empty())
            .expect("request")
    };
    assert_eq!(h.fetch(req()).await.expect_err("no fetch"), EgressError::Denied);
    assert_eq!(h.call("other", req()).await.expect_err("no call"), CallError::Forbidden);
    assert_eq!(
        h.enqueue("other", Bytes::new(), EnqueueOpts::default())
            .await
            .expect_err("no enqueue"),
        CallError::Forbidden
    );
    assert_eq!(
        h.platform_fetch("b", req()).await.map(|r| r.status().as_u16()).ok(),
        Some(403)
    );
    assert!(h.env().await.is_err(), "no env or secrets");
    assert_eq!(
        h.kv().set(b"k", bytes(1), None).await.expect_err("no storage").code,
        "storage_quota"
    );
    h.charge_cpu(Duration::from_millis(5));
    drop(h);
    assert!(
        policy.cpu.lock().is_empty(),
        "nothing is charged to a tenant for a detached handle"
    );
}

/// Counts calls in flight; each call waits until the gate opens.
struct GatedCalls {
    now: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    gate: tokio::sync::Notify,
    open: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl Calls for GatedCalls {
    async fn call(&self, _from: &TaskCtx, _slug: &str, _req: Request) -> Result<Response, CallError> {
        use std::sync::atomic::Ordering::SeqCst;
        let n = self.now.fetch_add(1, SeqCst) + 1;
        self.peak.fetch_max(n, SeqCst);
        while !self.open.load(SeqCst) {
            let notified = self.gate.notified();
            if self.open.load(SeqCst) {
                break;
            }
            notified.await;
        }
        self.now.fetch_sub(1, SeqCst);
        Ok(http::Response::new(Body::empty()))
    }

    async fn enqueue(&self, _from: &TaskCtx, _slug: &str, _body: Bytes, _o: EnqueueOpts) -> Result<MsgId, CallError> {
        Ok(MsgId::from_u128(1))
    }
}

/// Review 4: one task's `Promise.all(500 calls)` keeps at most `MAX_CONCURRENT_CALLS` child
/// tasks in flight; the rest wait for a permit (within the task's deadline) and all complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_keeps_at_most_max_concurrent_calls_in_flight() {
    use std::sync::atomic::Ordering::SeqCst;
    let dir = tempfile::tempdir().expect("tempdir");
    let calls = Arc::new(GatedCalls {
        now: Default::default(),
        peak: Default::default(),
        gate: tokio::sync::Notify::new(),
        open: Default::default(),
    });
    let host = host_with_calls(Arc::new(Policy::default()), &dir, calls.clone());
    let spec = SpecBuilder::wasm().finish();
    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, TaskId(1), Instant::now() + Duration::from_secs(10), cfg);
    let req = || {
        http::Request::builder()
            .uri("http://svc/")
            .body(Body::empty())
            .expect("request")
    };
    let all: Vec<_> = (0..100)
        .map(|_| {
            let (h, r) = (handle.clone(), req());
            tokio::spawn(async move { h.call("other", r).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        calls.now.load(SeqCst),
        rt_host::MAX_CONCURRENT_CALLS,
        "the rest wait for a permit"
    );
    calls.open.store(true, SeqCst);
    calls.gate.notify_waiters();
    for c in all {
        c.await.expect("join").expect("call");
    }
    assert_eq!(calls.peak.load(SeqCst), rt_host::MAX_CONCURRENT_CALLS);
}

/// HOST-004: a blob of exactly the storage left fits; one byte more is `storage_quota`.
#[tokio::test]
async fn host_004_a_blob_of_exactly_the_quota_left_fits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host(Arc::new(Policy::default()), &dir);
    let spec = SpecBuilder::wasm().finish();
    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, TaskId(1), Instant::now() + Duration::from_secs(5), cfg);
    let err = handle
        .blob()
        .put("big", Body::from(vec![0u8; 101]), None)
        .await
        .expect_err("one over");
    assert_eq!(err.code, "storage_quota");
    handle
        .blob()
        .put("big", Body::from(vec![0u8; 100]), None)
        .await
        .expect("exactly the 100 bytes left");
    let err = handle
        .blob()
        .put("more", Body::from(vec![0u8; 1]), None)
        .await
        .expect_err("nothing left");
    assert_eq!(err.code, "storage_quota");
}

/// A call past the depth limit, or past the task's deadline, is refused before the calls layer.
#[tokio::test]
async fn calls_are_refused_past_the_depth_limit_and_after_the_deadline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host(Arc::new(Policy::default()), &dir);
    let req = || {
        http::Request::builder()
            .uri("http://svc/")
            .body(Body::empty())
            .expect("request")
    };

    let deep = SpecBuilder::wasm()
        .trigger(Trigger::Call {
            parent: TaskId(9),
            depth: MAX_CALL_DEPTH,
        })
        .finish();
    let cfg = host.config_for(&deep).await;
    let at_limit = host.handle_for(&deep, TaskId(2), Instant::now() + Duration::from_secs(5), cfg);
    assert_eq!(
        at_limit.call("other", req()).await.expect_err("too deep").code(),
        "call_depth"
    );
    assert_eq!(
        at_limit
            .call_object(None, "obj", req())
            .await
            .expect_err("too deep")
            .code(),
        "call_depth"
    );

    let spec = SpecBuilder::wasm().finish();
    let cfg = host.config_for(&spec).await;
    let expired = host.handle_for(&spec, TaskId(3), Instant::now(), cfg.clone());
    assert_eq!(
        expired.call("other", req()).await.expect_err("out of time").code(),
        "call_failed"
    );
    assert_eq!(
        expired
            .call_object(None, "obj", req())
            .await
            .expect_err("out of time")
            .code(),
        "call_failed"
    );

    let live = host.handle_for(&spec, TaskId(4), Instant::now() + Duration::from_secs(5), cfg);
    let err = live
        .call_object(None, "bad\u{1}name", req())
        .await
        .expect_err("a control character is no object name");
    assert_eq!(err.code(), "not_found");
}

/// A task has 50 outbound fetches. Each one counts, a denied one too, and the 51st is refused by
/// the limit (`egress_limit`) before anything is sent.
#[tokio::test]
async fn a_task_has_fifty_fetches_and_the_fifty_first_is_refused_by_the_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host(Arc::new(Policy::default()), &dir);
    let spec = SpecBuilder::wasm().finish();
    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, TaskId(1), Instant::now() + Duration::from_secs(5), cfg);
    let req = || {
        http::Request::builder()
            .uri("http://127.0.0.1/")
            .body(Body::empty())
            .expect("request")
    };
    for i in 0..50 {
        assert_eq!(
            handle.fetch(req()).await.expect_err("loopback is denied"),
            EgressError::Denied,
            "fetch {i}"
        );
    }
    assert_eq!(
        handle.fetch(req()).await.expect_err("the fifty-first"),
        EgressError::Limit
    );
}

/// The calls layer refuses a call with its own error; over RPC that error reaches the guest with
/// the same stable code.
struct Refusing;

#[async_trait]
impl Calls for Refusing {
    async fn call(&self, _from: &TaskCtx, _slug: &str, _req: Request) -> Result<Response, CallError> {
        Err(CallError::NotFound)
    }

    async fn enqueue(&self, _from: &TaskCtx, _slug: &str, _body: Bytes, _o: EnqueueOpts) -> Result<MsgId, CallError> {
        Err(CallError::NotFound)
    }
}

#[tokio::test]
async fn an_rpc_to_a_missing_service_reports_the_call_error_code() {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = host_with_calls(Arc::new(Policy::default()), &dir, Arc::new(Refusing));
    let spec = SpecBuilder::wasm().finish();
    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, TaskId(1), Instant::now() + Duration::from_secs(5), cfg);
    let err = handle
        .rpc_call("missing", "ping", "[]")
        .await
        .expect_err("no such service");
    assert_eq!(err.code, "not_found");
}
