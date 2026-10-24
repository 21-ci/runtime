//! Async invocation engine against an in-memory `SqliteQueueStore` (real time, short intervals:
//! the store stamps records with the system clock).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rt_sched::{Clock, SystemClock};
use rt_store::queue::{Accepted, InvocationRecord, InvocationState, InvocationStore, LeasedInvocation, NewInvocation};
use rt_store::SqliteQueueStore;
use rt_triggers::async_engine::{AsyncEngine, AsyncEngineConfig, AsyncTaskFactory, SEALED_UNREADABLE};
use rt_triggers::Submitter;
use rt_types::{
    Body, Digest, InvocationId, KillReason, Outcome, RejectReason, Reply, ServiceId, ServiceRef, TaskKind, TaskSpec,
    TenantId, Trigger, VersionNo,
};

fn svc(n: u128) -> ServiceId {
    ServiceId::from_u128(n)
}

fn cfg() -> AsyncEngineConfig {
    AsyncEngineConfig {
        max_concurrency: 4,
        inflight_node: 256,
        sweep_every: Duration::from_millis(40),
        poll: Duration::from_millis(10),
        retry_base: Duration::from_millis(20),
        retry_max: Duration::from_millis(100),
        throttle_base: Duration::from_millis(10),
        throttle_max: Duration::from_millis(40),
    }
}

/// Records what it was asked for; `routable` / `unreadable` switch its answer.
struct Factory {
    seen: Mutex<Vec<(InvocationId, u16)>>,
    routable: AtomicBool,
    unreadable: AtomicBool,
    conc: Option<usize>,
}

impl Factory {
    fn new() -> Arc<Self> {
        Self::with_conc(None)
    }
    fn with_conc(conc: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            routable: AtomicBool::new(true),
            unreadable: AtomicBool::new(false),
            conc,
        })
    }
}

impl AsyncTaskFactory for Factory {
    fn task_for(&self, inv: &LeasedInvocation) -> Result<Option<TaskSpec>, &'static str> {
        self.seen.lock().push((inv.id, inv.attempt));
        if self.unreadable.load(Ordering::SeqCst) {
            return Err(SEALED_UNREADABLE);
        }
        if !self.routable.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let sref = ServiceRef {
            tenant: TenantId::from_u128(1),
            service: inv.service,
            version: VersionNo(1),
            digest: Digest::of(b"code"),
        };
        Ok(Some(TaskSpec::new(TaskKind::DENO, sref, Trigger::Http)))
    }
    fn visibility(&self, _service: ServiceId) -> Duration {
        Duration::from_secs(30)
    }
    fn max_concurrency(&self, _service: ServiceId) -> Option<usize> {
        self.conc
    }
}

/// `answer(n)` gives the outcome of the n-th submitted task (0-based); the first `refuse`
/// submits are refused. Each task holds `hold` before answering.
struct Sched {
    submitted: AtomicUsize,
    refuse: AtomicUsize,
    running: AtomicUsize,
    peak: AtomicUsize,
}

fn submitter(hold: Duration, answer: impl Fn(usize) -> Outcome + Send + Sync + 'static) -> (Arc<Sched>, Submitter) {
    let s = Arc::new(Sched {
        submitted: AtomicUsize::new(0),
        refuse: AtomicUsize::new(0),
        running: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let s2 = s.clone();
    let sub: Submitter = Arc::new(move |_spec: TaskSpec| {
        if s2
            .refuse
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(RejectReason::Overloaded);
        }
        let n = s2.submitted.fetch_add(1, Ordering::SeqCst);
        let out = answer(n);
        let now = s2.running.fetch_add(1, Ordering::SeqCst) + 1;
        s2.peak.fetch_max(now, Ordering::SeqCst);
        let s3 = s2.clone();
        Ok(Box::pin(async move {
            tokio::time::sleep(hold).await;
            s3.running.fetch_sub(1, Ordering::SeqCst);
            out
        }))
    });
    (s, sub)
}

fn status(s: u16) -> Outcome {
    Outcome::Ok(Reply::new(http::StatusCode::from_u16(s).unwrap(), Body::empty()))
}

async fn accept(store: &SqliteQueueStore, service: ServiceId, max_attempts: u16, max_age: Duration) -> InvocationId {
    let id = InvocationId::new();
    let got = store
        .accept(
            NewInvocation {
                id,
                service,
                method: "POST".into(),
                uri: "/hook".into(),
                sealed_headers: vec![1],
                sealed_body: vec![2],
                body_bytes: 1,
                max_age,
                max_attempts,
                idempotency_key: None,
                request_hash: "h".into(),
            },
            1000,
        )
        .await
        .unwrap();
    assert_eq!(got, Accepted::New(id));
    id
}

struct Run {
    store: Arc<SqliteQueueStore>,
    engine: Arc<AsyncEngine>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Run {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn start(
    store: Arc<SqliteQueueStore>,
    cfg: AsyncEngineConfig,
    f: Arc<Factory>,
    sub: Submitter,
    metrics: Option<Arc<rt_obs::Metrics>>,
) -> Run {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mut e = AsyncEngine::new(cfg, store.clone(), f, sub, clock);
    if let Some(m) = metrics {
        e = e.with_metrics(m);
    }
    let engine = Arc::new(e);
    let e2 = engine.clone();
    let task = tokio::spawn(async move { e2.run().await });
    Run { store, engine, task }
}

async fn record(store: &SqliteQueueStore, service: ServiceId, id: InvocationId) -> InvocationRecord {
    store.invocation(service, id).await.unwrap().expect("record")
}

/// Wait (up to 5 s) until the record leaves `pending`.
async fn finished(store: &SqliteQueueStore, service: ServiceId, id: InvocationId) -> InvocationRecord {
    for _ in 0..500 {
        let r = record(store, service, id).await;
        if r.state != InvocationState::Pending {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("invocation {id} still pending");
}

fn hour() -> Duration {
    Duration::from_secs(3600)
}

/// A 2xx and a 4xx answer both succeed after one run (a 4xx is the handler's answer).
#[tokio::test]
async fn ac_async_eng_01_runs_once_and_completes_on_2xx_and_4xx() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let a = accept(&store, svc(1), 3, hour()).await;
    let b = accept(&store, svc(1), 3, hour()).await;
    let (s, sub) = submitter(Duration::ZERO, |n| status(if n == 0 { 200 } else { 404 }));
    let m = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let run = start(store, cfg(), Factory::new(), sub, Some(m.clone()));
    let (ra, rb) = (
        finished(&run.store, svc(1), a).await,
        finished(&run.store, svc(1), b).await,
    );
    assert_eq!(ra.state, InvocationState::Succeeded);
    assert_eq!(rb.state, InvocationState::Succeeded);
    let mut st = [ra.status, rb.status];
    st.sort();
    assert_eq!(st, [Some(200), Some(404)]);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(s.submitted.load(Ordering::SeqCst), 2, "each ran exactly once");
    assert_eq!(m.async_finished_total.with_label_values(&["success"]).get(), 2);
}

/// 5xx and guest errors are failed attempts: retried with backoff, then `retries_exhausted`.
#[tokio::test]
async fn ac_async_eng_02_5xx_and_guest_error_retry_with_backoff_then_fail() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let a = accept(&store, svc(1), 3, hour()).await;
    let b = accept(&store, svc(2), 2, hour()).await;
    let f = Factory::new();
    let fc = f.clone();
    let (s, sub) = submitter(Duration::ZERO, move |n| {
        // The n-th submit belongs to the invocation the factory saw n-th.
        let id = fc.seen.lock()[n].0;
        if id == a {
            status(503)
        } else {
            Outcome::GuestError
        }
    });
    let m = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let run = start(store, cfg(), f.clone(), sub, Some(m.clone()));
    let ra = finished(&run.store, svc(1), a).await;
    let rb = finished(&run.store, svc(2), b).await;
    assert_eq!(ra.state, InvocationState::Failed);
    assert_eq!(ra.condition.as_deref(), Some("retries_exhausted"));
    assert_eq!(ra.error.as_deref(), Some("status_5xx"));
    assert_eq!(ra.status, Some(503));
    assert_eq!(ra.attempts, 3);
    assert_eq!(rb.error.as_deref(), Some("guest_error"));
    assert_eq!(rb.attempts, 2);
    assert_eq!(s.submitted.load(Ordering::SeqCst), 5);
    let attempts: Vec<u16> = f.seen.lock().iter().filter(|(id, _)| *id == a).map(|x| x.1).collect();
    assert_eq!(attempts, vec![1, 2, 3]);
    assert_eq!(
        m.async_finished_total.with_label_values(&["retries_exhausted"]).get(),
        2
    );
}

/// Admission refusals and shutdown kills are released with the attempt refunded.
#[tokio::test]
async fn ac_async_eng_03_overload_rejection_is_not_an_attempt() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let a = accept(&store, svc(1), 1, hour()).await;
    let (s, sub) = submitter(Duration::ZERO, |n| {
        if n == 0 {
            Outcome::Killed(KillReason::Shutdown)
        } else if n == 1 {
            Outcome::Rejected(RejectReason::Overloaded)
        } else {
            status(200)
        }
    });
    s.refuse.store(3, Ordering::SeqCst);
    let m = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let run = start(store, cfg(), Factory::new(), sub, Some(m.clone()));
    let r = finished(&run.store, svc(1), a).await;
    assert_eq!(
        r.state,
        InvocationState::Succeeded,
        "max_attempts 1 is not used up by throttles"
    );
    assert_eq!(r.attempts, 1);
    assert_eq!(m.async_throttled_total.get(), 5);
}

/// A service that cannot run keeps its record pending (no attempt) until `max_event_age`.
#[tokio::test]
async fn ac_async_eng_04_not_routable_is_throttled_until_max_event_age() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let a = accept(&store, svc(1), 1, Duration::from_millis(300)).await;
    let f = Factory::new();
    f.routable.store(false, Ordering::SeqCst);
    let (s, sub) = submitter(Duration::ZERO, |_| status(200));
    let m = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let run = start(store, cfg(), f.clone(), sub, Some(m.clone()));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mid = record(&run.store, svc(1), a).await;
    assert_eq!(mid.state, InvocationState::Pending);
    assert!(f.seen.lock().len() >= 2, "retried while throttled");
    let r = finished(&run.store, svc(1), a).await;
    assert_eq!(r.state, InvocationState::Failed);
    assert_eq!(r.condition.as_deref(), Some("event_age_exceeded"));
    assert_eq!(s.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(
        m.async_finished_total.with_label_values(&["event_age_exceeded"]).get(),
        1
    );
}

/// At most `max_concurrency` deliveries of one service run at once.
#[tokio::test]
async fn ac_async_eng_05_per_service_concurrency_cap() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let mut ids = Vec::new();
    for _ in 0..6 {
        ids.push(accept(&store, svc(1), 3, hour()).await);
    }
    let (s, sub) = submitter(Duration::from_millis(60), |_| status(200));
    let run = start(store, cfg(), Factory::with_conc(Some(2)), sub, None);
    for id in ids {
        assert_eq!(finished(&run.store, svc(1), id).await.state, InvocationState::Succeeded);
    }
    assert_eq!(s.peak.load(Ordering::SeqCst), 2);
}

/// The sweep fails records past their age even while the engine is drained (nothing leased).
#[tokio::test]
async fn ac_async_eng_06_sweep_expires_old_pending() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let a = accept(&store, svc(1), 3, Duration::from_millis(100)).await;
    let (s, sub) = submitter(Duration::ZERO, |_| status(200));
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let engine = Arc::new(AsyncEngine::new(cfg(), store.clone(), Factory::new(), sub, clock));
    engine.drain();
    let e2 = engine.clone();
    let task = tokio::spawn(async move { e2.run().await });
    let r = finished(&store, svc(1), a).await;
    task.abort();
    assert_eq!(r.condition.as_deref(), Some("event_age_exceeded"));
    assert_eq!(s.submitted.load(Ordering::SeqCst), 0, "a drained engine leases nothing");
}

/// The factory gets the same id with the 1-based attempt of each delivery.
#[tokio::test]
async fn ac_async_eng_07_task_carries_invocation_headers_and_attempt() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let a = accept(&store, svc(1), 3, hour()).await;
    let f = Factory::new();
    let (_s, sub) = submitter(Duration::ZERO, |n| status(if n == 0 { 500 } else { 200 }));
    let run = start(store, cfg(), f.clone(), sub, None);
    run.engine.notify(svc(1));
    let r = finished(&run.store, svc(1), a).await;
    assert_eq!(r.state, InvocationState::Succeeded);
    assert_eq!(*f.seen.lock(), vec![(a, 1), (a, 2)]);
}

/// A 429 answer is a failed attempt and is retried.
#[tokio::test]
async fn ac_async_eng_08_status_429_is_retried() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let a = accept(&store, svc(1), 3, hour()).await;
    let (s, sub) = submitter(Duration::ZERO, |n| status(if n == 0 { 429 } else { 201 }));
    let run = start(store, cfg(), Factory::new(), sub, None);
    let r = finished(&run.store, svc(1), a).await;
    assert_eq!(r.state, InvocationState::Succeeded);
    assert_eq!(r.status, Some(201));
    assert_eq!(r.attempts, 2);
    assert_eq!(s.submitted.load(Ordering::SeqCst), 2);
}

/// An unreadable sealed payload fails at once: no task, no backoff wait.
#[tokio::test]
async fn ac_async_eng_09_unreadable_payload_fails_without_retry() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let a = accept(&store, svc(1), 5, hour()).await;
    let f = Factory::new();
    f.unreadable.store(true, Ordering::SeqCst);
    let mut c = cfg();
    c.retry_base = Duration::from_secs(600);
    c.retry_max = Duration::from_secs(600);
    let (s, sub) = submitter(Duration::ZERO, |_| status(200));
    let run = start(store, c, f, sub, None);
    let r = finished(&run.store, svc(1), a).await;
    assert_eq!(r.state, InvocationState::Failed);
    assert_eq!(r.error.as_deref(), Some(SEALED_UNREADABLE));
    assert_eq!(s.submitted.load(Ordering::SeqCst), 0);
}

/// At most `inflight_node` deliveries run across all services.
#[tokio::test]
async fn ac_async_eng_10_node_inflight_cap() {
    let store = SqliteQueueStore::open_in_memory(5).unwrap();
    let mut ids = Vec::new();
    for s in 1..=3 {
        for _ in 0..3 {
            ids.push((svc(s), accept(&store, svc(s), 3, hour()).await));
        }
    }
    let mut c = cfg();
    c.inflight_node = 3;
    let (s, sub) = submitter(Duration::from_millis(60), |_| status(200));
    let run = start(store, c, Factory::new(), sub, None);
    for (svc, id) in ids {
        assert_eq!(finished(&run.store, svc, id).await.state, InvocationState::Succeeded);
    }
    assert_eq!(s.peak.load(Ordering::SeqCst), 3);
}
