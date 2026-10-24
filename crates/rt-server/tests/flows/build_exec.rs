//! The Build executor on its own: what it stores, what it reports and how it fails when the pieces
//! around it do (artifact store, T2, meta store, the cache that promotes, the control plane).

#[path = "../../../rt-build/tests/common/support.rs"]
mod support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use rt_cache::{NoTamperSink, T2Store};
use rt_mem::{MemCategory, Reservation};
use rt_obs::{MemoryLogSink, Metrics, MetricsOptions};
use rt_sched::{ExecCx, Executor, SystemClock};
use rt_sec::{HkdfArtifactMac, MasterKey};
use rt_server::build_exec::{BuildDeps, BuildExecutor, BuildSink};
use rt_store::{ArtifactStore, Fault, Faults, MetaStore, SqliteMetaStore, TenantScope};
use rt_testkit::SpecBuilder;
use rt_types::{
    BuildFailReason, BuildFailure, BuildReport, BuildState, ConfigStream, ControlPlane, CpError, Digest, EnvBundle,
    MachineId, NewService, NewTenant, NewVersion, NodeEvent, Outcome, PreparedKey, ProcId, RouteSnapshot, RuntimeKind,
    SecretsVersion, Service, ServiceId, TaskId, TaskSpec, Trigger, UsageBatch,
};
use support::*;
use zeroize::Zeroizing;

// ---- fakes -------------------------------------------------------------------------------------

/// The in-memory artifact store; `put` can be made to fail (the lock is stored with `put`).
struct Artifacts {
    inner: Arc<MemArtifacts>,
    fail_put: AtomicBool,
}

#[async_trait]
impl ArtifactStore for Artifacts {
    async fn put(&self, bytes: Bytes) -> rt_store::Result<Digest> {
        if self.fail_put.load(Ordering::SeqCst) {
            return Err(rt_store::StoreError::Io(std::io::Error::other("disk full")));
        }
        self.inner.put(bytes).await
    }
    async fn put_vendored(&self, bytes: Bytes, url: &str) -> rt_store::Result<Digest> {
        self.inner.put_vendored(bytes, url).await
    }
    async fn vendored_from(&self, d: &Digest, url: &str) -> rt_store::Result<bool> {
        self.inner.vendored_from(d, url).await
    }
    async fn get(&self, d: &Digest) -> rt_store::Result<Bytes> {
        self.inner.get(d).await
    }
    async fn exists(&self, d: &Digest) -> rt_store::Result<bool> {
        self.inner.exists(d).await
    }
    async fn delete(&self, d: &Digest) -> rt_store::Result<()> {
        self.inner.delete(d).await
    }
    async fn touch(&self, d: &Digest) -> rt_store::Result<()> {
        self.inner.touch(d).await
    }
    async fn size(&self, d: &Digest) -> rt_store::Result<Option<u64>> {
        self.inner.size(d).await
    }
}

/// Remembers what the cache that asked for the build was told.
#[derive(Default)]
struct Sink {
    promoted: Mutex<Vec<PreparedKey>>,
    done: Mutex<Vec<Result<(), BuildFailure>>>,
    /// What `promote_built` answers: `None` ok, `Some(None)` a failure, `Some(Some(_))` never used.
    promote: Mutex<Option<BuildFailure>>,
    panic_on_promote: AtomicBool,
}

impl BuildSink for Sink {
    fn promote_built(&self, key: &PreparedKey) -> Result<(), BuildFailure> {
        assert!(!self.panic_on_promote.load(Ordering::SeqCst), "the load went wrong");
        self.promoted.lock().expect("lock").push(*key);
        match self.promote.lock().expect("lock").clone() {
            Some(f) => Err(f),
            None => Ok(()),
        }
    }

    fn build_done(&self, _key: &PreparedKey, res: Result<(), BuildFailure>) {
        self.done.lock().expect("lock").push(res);
    }
}

/// A sink that only reports (it keeps the default `promote_built`).
struct ReportingOnly;

impl BuildSink for ReportingOnly {
    fn build_done(&self, _key: &PreparedKey, _res: Result<(), BuildFailure>) {}
}

#[derive(Default)]
struct Control {
    reports: Mutex<Vec<BuildReport>>,
    fail_report: AtomicBool,
}

#[async_trait]
impl ControlPlane for Control {
    fn routes(&self) -> Arc<RouteSnapshot> {
        Arc::new(RouteSnapshot::default())
    }
    async fn subscribe_config(&self) -> ConfigStream {
        futures_util::stream::empty().boxed()
    }
    async fn service(&self, _id: ServiceId) -> Result<Option<Service>, CpError> {
        Ok(None)
    }
    async fn env_for(&self, _service: ServiceId, _version: SecretsVersion) -> Result<Zeroizing<EnvBundle>, CpError> {
        Ok(Zeroizing::new(EnvBundle::default()))
    }
    async fn report_build(&self, _key: &PreparedKey, report: BuildReport) -> Result<(), CpError> {
        if self.fail_report.load(Ordering::SeqCst) {
            return Err(CpError::Unavailable);
        }
        self.reports.lock().expect("lock").push(report);
        Ok(())
    }
    fn usage(&self, _batch: UsageBatch) {}
    fn event(&self, _ev: NodeEvent) {}
}

struct Rig {
    exec: BuildExecutor,
    artifacts: Arc<Artifacts>,
    sink: Arc<Sink>,
    control: Arc<Control>,
    logs: Arc<MemoryLogSink>,
    metrics: Arc<Metrics>,
    meta: Arc<SqliteMetaStore>,
    faults: Arc<Faults>,
    cdn: rt_testkit::MockCdn,
    t2_root: std::path::PathBuf,
    _dir: tempfile::TempDir,
    next: u64,
}

impl Rig {
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let cdn = cdn_with_zod().await;
        let artifacts = Arc::new(Artifacts {
            inner: MemArtifacts::new(),
            fail_put: AtomicBool::new(false),
        });
        let (sink, control, logs) = (
            Arc::new(Sink::default()),
            Arc::new(Control::default()),
            MemoryLogSink::new(),
        );
        let metrics = Metrics::new(MetricsOptions::default()).expect("metrics");
        let meta = SqliteMetaStore::open(&dir.path().join("meta.db")).expect("meta.db");
        let faults = Faults::new();
        meta.inject_faults(faults.clone());
        let t2_root = dir.path().join("cache");
        let mac = Arc::new(HkdfArtifactMac::new(&MasterKey::from_bytes([3; 32])));
        let t2 = Arc::new(T2Store::new(&t2_root, mac, Arc::new(NoTamperSink)));
        let mut resolver = mirror_resolver_cfg(&cdn);
        resolver.import_map = Default::default();
        let deps = BuildDeps {
            builder: Arc::new(default_builder()),
            artifacts: artifacts.clone(),
            fetcher: guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]),
            resolver: Arc::new(rt_build::DefaultResolver::new(resolver)),
            code_cache: None,
            precompiler: Some(FakePrecompiler::new(TEST_ENGINE_FP)),
            t2,
            logs: logs.clone(),
            control: control.clone(),
            sink: sink.clone(),
            metrics: metrics.clone(),
            timeout_deno: Duration::from_secs(30),
            timeout_wasm: Duration::from_secs(30),
            meta: Some(meta.clone()),
            sdks: None,
        };
        Self {
            exec: BuildExecutor::new(Arc::new(deps)),
            artifacts,
            sink,
            control,
            logs,
            metrics,
            meta,
            faults,
            cdn,
            t2_root,
            _dir: dir,
            next: 0,
        }
    }

    /// Store `upload` as a source and return the key of its Deno build.
    async fn source(&self, upload: &rt_build::Upload, kind: RuntimeKind) -> PreparedKey {
        let digest = self
            .artifacts
            .inner
            .put(upload.body.clone())
            .await
            .expect("store the source");
        PreparedKey {
            digest,
            kind,
            engine_fp: TEST_ENGINE_FP,
        }
    }

    fn spec(&self, key: PreparedKey, service: ServiceId) -> TaskSpec {
        let mut spec = SpecBuilder::build().trigger(Trigger::Build { key }).finish();
        spec.service.service = service;
        spec
    }

    async fn run(&mut self, spec: TaskSpec) -> Outcome {
        self.next += 1;
        let cx = ExecCx {
            task: TaskId(self.next),
            machine: MachineId(0),
            proc: ProcId(0),
            clock: Arc::new(SystemClock::new()),
            reservation: Reservation::detached(MemCategory::Other, 64 * 1024 * 1024),
            deadline: Instant::now() + Duration::from_secs(60),
            time_slice: Duration::from_millis(10),
            cpu_budget: Duration::from_secs(60),
        };
        self.exec.start(spec, Arc::new(()), cx).await
    }

    fn logged(&self, needle: &str) -> bool {
        self.logs.records().iter().any(|r| r.msg.contains(needle))
    }

    fn built(&self, kind: &str, result: &str) -> u64 {
        self.metrics.build_total.with_label_values(&[kind, result]).get()
    }
}

fn service() -> ServiceId {
    ServiceId::from_u128(77)
}

/// A Deno source that imports `zod` through the mirror, so its build produces a lock.
fn zod_upload() -> rt_build::Upload {
    fixture_upload("npm_zod")
}

fn hello_upload() -> rt_build::Upload {
    ts_upload("export default () => new Response('hi');")
}

// ---- tests -------------------------------------------------------------------------------------

#[tokio::test]
async fn a_good_build_is_stored_promoted_reported_and_logged() {
    let mut rig = Rig::new().await;
    let key = rig.source(&hello_upload(), RuntimeKind::Deno).await;
    let spec = rig.spec(key, service());
    let out = rig.run(spec).await;
    assert!(matches!(out, Outcome::Ok(_)), "{out:?}");

    assert_eq!(
        rig.sink.promoted.lock().expect("lock").as_slice(),
        [key],
        "T2 -> T1 before the report"
    );
    assert_eq!(rig.sink.done.lock().expect("lock").len(), 1);
    assert!(rig.sink.done.lock().expect("lock")[0].is_ok());
    let reports = rig.control.reports.lock().expect("lock");
    assert!(reports.len() == 1 && reports[0].result.is_ok());
    assert!(rig.t2_root.join("v2").is_dir(), "the artifact was written to T2");
    assert_eq!(rig.built("deno", "ok"), 1);
    assert!(rig.logged("build started (deno)") && rig.logged("build finished"));
}

#[tokio::test]
async fn a_build_that_fails_is_reported_with_its_reason_and_leaves_nothing_promoted() {
    let mut rig = Rig::new().await;
    let key = rig.source(&ts_upload("export default ( {"), RuntimeKind::Deno).await;
    let spec = rig.spec(key, service());
    let out = rig.run(spec).await;
    let Outcome::BuildFailed(failure) = out else {
        panic!("{out:?}")
    };
    assert!(rig.sink.promoted.lock().expect("lock").is_empty());
    assert_eq!(rig.sink.done.lock().expect("lock").clone(), vec![Err(failure.clone())]);
    assert_eq!(rig.control.reports.lock().expect("lock")[0].result, Err(failure));
    assert_eq!(rig.built("deno", "failed"), 1);
    assert!(rig.logged("build failed"));
}

#[tokio::test]
async fn a_task_that_is_not_a_build_is_a_guest_error() {
    let mut rig = Rig::new().await;
    let out = rig.run(SpecBuilder::deno().http_get("/").finish()).await;
    assert!(matches!(out, Outcome::GuestError), "{out:?}");
}

#[tokio::test]
async fn a_build_without_a_service_writes_no_service_log() {
    let mut rig = Rig::new().await;
    let key = rig.source(&hello_upload(), RuntimeKind::Deno).await;
    let spec = rig.spec(key, ServiceId::from_u128(0));
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
    assert!(rig.logs.records().is_empty(), "{:?}", rig.logs.records());
}

#[tokio::test]
async fn the_default_promotion_of_a_sink_accepts_the_artifact() {
    let key = PreparedKey {
        digest: Digest::of(b"x"),
        kind: RuntimeKind::Deno,
        engine_fp: 1,
    };
    assert!(ReportingOnly.promote_built(&key).is_ok());
}

#[tokio::test]
async fn an_artifact_that_does_not_load_fails_the_build_instead_of_marking_it_ready() {
    let mut rig = Rig::new().await;
    let key = rig.source(&hello_upload(), RuntimeKind::Deno).await;
    *rig.sink.promote.lock().expect("lock") = Some(BuildFailure::new(BuildFailReason::Internal, "does not load"));
    let spec = rig.spec(key, service());
    let out = rig.run(spec).await;
    assert!(
        matches!(&out, Outcome::BuildFailed(f) if f.message.contains("does not load")),
        "{out:?}"
    );
    assert!(rig.control.reports.lock().expect("lock")[0].result.is_err());

    rig.sink.panic_on_promote.store(true, Ordering::SeqCst);
    let spec = rig.spec(key, service());
    let out = rig.run(spec).await;
    assert!(
        matches!(&out, Outcome::BuildFailed(f) if f.message.contains("panicked")),
        "{out:?}"
    );
}

#[tokio::test]
async fn a_t2_that_cannot_be_written_fails_the_build() {
    let mut rig = Rig::new().await;
    std::fs::write(&rig.t2_root, b"a file where the cache directory should be").expect("block T2");
    let key = rig.source(&hello_upload(), RuntimeKind::Deno).await;
    let spec = rig.spec(key, service());
    let out = rig.run(spec).await;
    assert!(
        matches!(&out, Outcome::BuildFailed(f) if f.message.contains("could not store the artifact")),
        "{out:?}"
    );
    assert!(rig.sink.promoted.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn a_report_the_control_plane_refuses_is_logged_and_the_cache_is_still_told() {
    let (_guard, logs) = rt_testkit::capture_tracing();
    let mut rig = Rig::new().await;
    rig.control.fail_report.store(true, Ordering::SeqCst);
    let ok = rig.source(&hello_upload(), RuntimeKind::Deno).await;
    let spec = rig.spec(ok, service());
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
    let bad = rig.source(&ts_upload("export default ( {"), RuntimeKind::Deno).await;
    let spec = rig.spec(bad, service());
    assert!(matches!(rig.run(spec).await, Outcome::BuildFailed(_)));
    assert_eq!(
        rig.sink.done.lock().expect("lock").len(),
        2,
        "both builds reached the cache"
    );
    assert!(
        logs.text().matches("report_build failed").count() >= 2,
        "{}",
        logs.text()
    );
}

// ---- builds with a lock ------------------------------------------------------------------------

/// Record `lock` as the lock of a version built from `source`, as an earlier build would have.
async fn remember_lock(rig: &Rig, source: Digest, lock: &rt_build::RtLock) {
    let lock_digest = rig
        .artifacts
        .inner
        .put(Bytes::from(lock.to_bytes()))
        .await
        .expect("store the lock");
    let tenant = rig
        .meta
        .create_tenant(NewTenant {
            name: "t".into(),
            plan: "free".into(),
            overrides: None,
        })
        .await
        .expect("tenant");
    let new = NewService {
        slug: "svc".into(),
        runtime: RuntimeKind::Deno,
        name: None,
        profile: None,
        environment: rt_types::default_environment(),
        project: rt_types::default_project(),
        config: Default::default(),
    };
    let svc = rig
        .meta
        .create_service(TenantScope::Operator, tenant.id, new)
        .await
        .expect("service")
        .id;
    let v = rig
        .meta
        .add_version(
            svc,
            NewVersion {
                source_digest: source,
                compile: Default::default(),
            },
        )
        .await
        .expect("version")
        .number;
    rig.meta
        .set_build_state(svc, v, BuildState::Ready, None, Some(lock_digest))
        .await
        .expect("lock recorded");
}

#[tokio::test]
async fn a_rebuild_reads_the_dependencies_the_first_build_locked_and_a_first_build_stores_its_lock() {
    let mut rig = Rig::new().await;
    let upload = zod_upload();
    let key = rig.source(&upload, RuntimeKind::Deno).await;

    // The first build resolves afresh and stores the lock it made.
    let spec = rig.spec(key, service());
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
    let lock_digest = rig.control.reports.lock().expect("lock")[0]
        .lock_digest
        .expect("the build made a lock");
    let lock_bytes = rig.artifacts.inner.get(&lock_digest).await.expect("the lock is stored");
    let lock = rt_build::RtLock::from_bytes(&lock_bytes).expect("a lock");
    assert!(!rig.logged("pinned by rt.lock"));

    // A version that names the lock makes the next build of the source reproduce it.
    remember_lock(&rig, key.digest, &lock).await;
    rig.cdn.reset_hits();
    let spec = rig.spec(key, service());
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
    assert!(rig.logged("dependencies pinned by rt.lock"), "{:?}", rig.logs.records());
}

#[tokio::test]
async fn a_lock_the_store_cannot_keep_or_the_meta_store_cannot_index_does_not_fail_the_build() {
    let (_guard, logs) = rt_testkit::capture_tracing();
    let mut rig = Rig::new().await;
    let key = rig.source(&zod_upload(), RuntimeKind::Deno).await;

    rig.artifacts.fail_put.store(true, Ordering::SeqCst);
    let spec = rig.spec(key, service());
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
    assert!(logs.contains("could not store rt.lock"), "{}", logs.text());
    rig.artifacts.fail_put.store(false, Ordering::SeqCst);

    rig.faults.fail_next(2, Fault::Io); // the lookup of an earlier lock (so: none), then the index of the new one
    let spec = rig.spec(key, service());
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
    assert!(logs.contains("could not record the lock's modules"), "{}", logs.text());
}

#[tokio::test]
async fn a_lock_that_is_gone_means_the_build_resolves_afresh() {
    let mut rig = Rig::new().await;
    let upload = zod_upload();
    let key = rig.source(&upload, RuntimeKind::Deno).await;
    let spec = rig.spec(key, service());
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
    let lock_digest = rig.control.reports.lock().expect("lock")[0]
        .lock_digest
        .expect("lock digest");
    let lock =
        rt_build::RtLock::from_bytes(&rig.artifacts.inner.get(&lock_digest).await.expect("lock")).expect("a lock");
    remember_lock(&rig, key.digest, &lock).await;

    // The lock artifact was garbage-collected.
    rig.artifacts.inner.delete(&lock_digest).await.expect("delete");
    let spec = rig.spec(key, service());
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
    assert!(
        !rig.logged("pinned by rt.lock"),
        "an unreadable lock is not used: {:?}",
        rig.logs.records()
    );
}

#[tokio::test]
async fn interrupting_a_build_does_nothing_and_the_build_still_finishes() {
    let mut rig = Rig::new().await;
    let key = rig.source(&hello_upload(), RuntimeKind::Deno).await;
    let spec = rig.spec(key, service());
    rig.next += 1;
    let task = TaskId(rig.next);
    let interrupter = rig.exec.interrupter();
    interrupter.interrupt(task, rt_sched::Interrupt::Kill(rt_types::KillReason::Shutdown));
    interrupter.interrupt(task, rt_sched::Interrupt::Yield);
    assert!(matches!(rig.run(spec).await, Outcome::Ok(_)));
}
