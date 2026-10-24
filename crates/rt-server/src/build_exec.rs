//! The Build executor: runs `rt-build` on a build `Processor`, writes the T2 artifact,
//! promotes it in the right cache and reports the result to the control plane.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rt_build::RtLock;
use rt_build::{BuildCx, BuildRequest, Builder, DefaultBuilder, ModuleFetcher, SpecifierResolver};
use rt_build::{CodeCacheProducer, ComponentPrecompiler};
use rt_cache::T2Store;
use rt_obs::{LogKind, LogLevel, LogRecord, LogSink, Metrics};
use rt_sched::{ExecCx, Executor, Interrupt, Interrupter, TaskFuture};
use rt_store::{ArtifactStore, MetaStore};
use rt_types::{
    BuildFailReason, BuildFailure, BuildReport, ControlPlane, Outcome, PreparedKey, Reply, RuntimeKind, ServiceId,
    TaskId, TaskKind, TaskSpec, Trigger,
};

/// Receives a finished build so the cache that asked for it can promote (or fail) the entry.
pub trait BuildSink: Send + Sync {
    /// Load the artifact just written to T2 into T1 (blocking; called on the blocking pool)
    /// **before** the build is reported, so a version is marked `ready` only once its artifact
    /// loads. The error is reported as the build's failure instead. The default does nothing.
    fn promote_built(&self, key: &PreparedKey) -> Result<(), BuildFailure> {
        let _ = key;
        Ok(())
    }

    fn build_done(&self, key: &PreparedKey, res: Result<(), BuildFailure>);
}

/// Everything a build needs. Shared by every build Machine.
pub struct BuildDeps {
    pub builder: Arc<DefaultBuilder>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub fetcher: Arc<dyn ModuleFetcher>,
    pub resolver: Arc<dyn SpecifierResolver>,
    pub code_cache: Option<Arc<dyn CodeCacheProducer>>,
    pub precompiler: Option<Arc<dyn ComponentPrecompiler>>,
    pub t2: Arc<T2Store>,
    pub logs: Arc<dyn LogSink>,
    pub control: Arc<dyn ControlPlane>,
    pub sink: Arc<dyn BuildSink>,
    pub metrics: Arc<Metrics>,
    pub timeout_deno: Duration,
    pub timeout_wasm: Duration,
    /// Where a source's `rt.lock` digest is found, so rebuilds reproduce the first build's
    /// dependencies (BLD-06). `None`: every build resolves dependencies afresh.
    pub meta: Option<Arc<dyn MetaStore>>,
    /// The published operator SDKs (`std:sdk` / `sdk:<name>` shims).
    pub sdks: Option<Arc<dyn rt_build::SdkCatalog>>,
}

/// The `rt.lock` of an earlier successful build of `source`, from the artifact store. A missing
/// or unreadable lock means "resolve afresh" (the build then records a new one).
async fn prior_lock(deps: &BuildDeps, source: &rt_types::Digest) -> Option<RtLock> {
    let meta = deps.meta.as_ref()?;
    let digest = meta.lock_for_source(source).await.ok()??;
    let bytes = deps.artifacts.get(&digest).await.ok()?;
    RtLock::from_bytes(&bytes).ok().filter(|l| l.digest() == digest)
}

pub struct BuildExecutor {
    deps: Arc<BuildDeps>,
}

impl BuildExecutor {
    pub fn new(deps: Arc<BuildDeps>) -> Self {
        Self { deps }
    }
}

struct NoopInterrupter;

impl Interrupter for NoopInterrupter {
    fn interrupt(&self, _task: TaskId, _what: Interrupt) {}
}

fn note(deps: &BuildDeps, svc: ServiceId, version: rt_types::VersionNo, level: LogLevel, msg: String) {
    if svc != ServiceId::from_u128(0) {
        deps.logs
            .append(LogRecord::new(svc, version, level, LogKind::Build, msg));
    }
}

async fn run_build(deps: Arc<BuildDeps>, spec: TaskSpec, cx: ExecCx) -> Outcome {
    let Trigger::Build { key } = spec.trigger.clone() else {
        return Outcome::GuestError;
    };
    let started = Instant::now();
    let kind = match key.kind {
        RuntimeKind::Deno => "deno",
        RuntimeKind::Wasm => "wasm",
    };
    let (svc, version) = (spec.service.service, spec.service.version);
    note(&deps, svc, version, LogLevel::Info, format!("build started ({kind})"));
    let timeout = match key.kind {
        RuntimeKind::Deno => deps.timeout_deno,
        RuntimeKind::Wasm => deps.timeout_wasm,
    };
    let lock = match key.kind {
        RuntimeKind::Deno => prior_lock(&deps, &key.digest).await,
        RuntimeKind::Wasm => None,
    };
    if lock.is_some() {
        note(
            &deps,
            svc,
            version,
            LogLevel::Info,
            "dependencies pinned by rt.lock".to_string(),
        );
    }
    // Pin what this build reads: a touch restarts an artifact's GC grace period, so neither version
    // GC nor the orphan sweep deletes the source, the lock or its modules while the build runs.
    let _ = deps.artifacts.touch(&key.digest).await;
    if let Some(l) = &lock {
        let _ = deps.artifacts.touch(&l.digest()).await;
        for e in l.entries.values() {
            let _ = deps.artifacts.touch(&e.digest).await;
        }
    }
    let req = BuildRequest {
        key,
        kind: key.kind,
        source_digest: key.digest,
        lock,
        timeout,
    };
    let mut bcx = BuildCx {
        artifacts: deps.artifacts.clone(),
        fetcher: deps.fetcher.clone(),
        resolver: deps.resolver.clone(),
        code_cache: deps.code_cache.clone(),
        precompiler: deps.precompiler.clone(),
        log: deps.logs.clone(),
        sdks: deps.sdks.clone(),
        reservation: cx.reservation,
        deadline: cx.deadline,
    };
    let built = deps.builder.build(&req, &mut bcx).await;
    // (lock digest, bundle digest) of a build whose artifact is stored and loads.
    type Digests = (Option<rt_types::Digest>, Option<rt_types::Digest>);
    let result: Result<Digests, BuildFailure> = match built {
        Ok(art) => {
            // Keep the lock (content-addressed: its artifact digest is the lock digest) so a
            // later rebuild of this source fetches exactly these modules.
            if let Some(lock) = &art.lock {
                if let Err(e) = deps.artifacts.put(bytes::Bytes::from(lock.to_bytes())).await {
                    tracing::warn!(error = %e, "could not store rt.lock (rebuilds resolve afresh)");
                }
            }
            let mut res = Ok((art.lock.as_ref().map(|l| l.digest()), art.bundle_digest));
            for f in &art.files {
                if let Err(e) = deps.t2.write(&key, &f.name, &f.bytes) {
                    res = Err(BuildFailure::new(
                        BuildFailReason::Internal,
                        format!("could not store the artifact: {e}"),
                    ));
                    break;
                }
            }
            // Record which vendored modules the lock lists before the version references the lock,
            // so version GC can free them once no lock needs them (R1).
            if let (Ok((Some(lock_digest), _)), Some(lock), Some(meta)) = (&res, &art.lock, &deps.meta) {
                let modules: Vec<rt_types::Digest> = lock.entries.values().map(|e| e.digest).collect();
                if let Err(e) = meta.record_lock_modules(lock_digest, &modules).await {
                    tracing::warn!(error = %e, "could not record the lock's modules (they will not be garbage-collected)");
                }
            }
            if res.is_ok() {
                // T2 -> T1 before the version is reported `ready`: an artifact that does not load
                // is a failed build, never a ready version whose cache entry holds a failure.
                let sink = deps.sink.clone();
                let promoted = tokio::task::spawn_blocking(move || sink.promote_built(&key))
                    .await
                    .unwrap_or_else(|e| {
                        Err(BuildFailure::new(
                            BuildFailReason::Internal,
                            format!("prepared artifact load panicked: {e}"),
                        ))
                    });
                if let Err(f) = promoted {
                    res = Err(f);
                }
            }
            res
        }
        Err(f) => Err(f),
    };
    let elapsed = started.elapsed();
    deps.metrics
        .build_seconds
        .with_label_values(&[kind])
        .observe(elapsed.as_secs_f64());
    match result {
        Ok((lock_digest, bundle_digest)) => {
            deps.metrics.build_total.with_label_values(&[kind, "ok"]).inc();
            note(
                &deps,
                svc,
                version,
                LogLevel::Info,
                format!("build finished in {} ms", elapsed.as_millis()),
            );
            // Record the state first, then release the requests parked on this build: a client that
            // got its response must find `build_state: ready` (or `failed`) on the version.
            let report = BuildReport {
                key,
                result: Ok(()),
                duration_ms: elapsed.as_millis() as u64,
                lock_digest,
                bundle_digest,
            };
            if let Err(e) = deps.control.report_build(&key, report).await {
                tracing::warn!(error = %e, "report_build failed");
            }
            deps.sink.build_done(&key, Ok(()));
            Outcome::Ok(Reply::ok())
        }
        Err(failure) => {
            deps.metrics.build_total.with_label_values(&[kind, "failed"]).inc();
            note(&deps, svc, version, LogLevel::Error, format!("build failed: {failure}"));
            let report = BuildReport {
                key,
                result: Err(failure.clone()),
                duration_ms: elapsed.as_millis() as u64,
                lock_digest: None,
                bundle_digest: None,
            };
            if let Err(e) = deps.control.report_build(&key, report).await {
                tracing::warn!(error = %e, "report_build failed");
            }
            deps.sink.build_done(&key, Err(failure.clone()));
            Outcome::BuildFailed(failure)
        }
    }
}

impl Executor for BuildExecutor {
    const KIND: TaskKind = TaskKind::Build;
    type Prepared = ();

    fn start(&mut self, spec: TaskSpec, _prepared: Arc<()>, cx: ExecCx) -> TaskFuture {
        Box::pin(run_build(self.deps.clone(), spec, cx))
    }

    fn interrupter(&self) -> Arc<dyn Interrupter> {
        Arc::new(NoopInterrupter)
    }

    fn on_idle(&mut self, _budget: Duration) {}
}
