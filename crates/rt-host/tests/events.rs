//! `std:pubsub` / `std:stream` on the host handle: a detached handle or a node without the bus
//! answers `unavailable`, the bounds are checked before the bus sees a call, the request timeout
//! is defaulted and clamped, and bus errors reach the guest with their code.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use rt_host::{
    AppendEntry, AppendedOut, BindingError, CallError, Calls, EgressGuardConfig, EnqueueOpts, GuardedEgress, HostDeps,
    HostError, HostHandle, HostServices, HostTaskConfig, PlatformBindings, PublishOpts, PublishOut, PubsubBus,
    ReadOpts, ReadPage, ReadStart, Reply, Request, RequestOpts, Response, StreamBus, TaskCtx, TopicInfo,
};
use rt_obs::NullLogSink;
use rt_sec::SelfDenylist;
use rt_store::{DataConfig, LocalServiceData, SqliteQueueStore};
use rt_testkit::{SpecBuilder, TestGovernor};
use rt_types::events::{EVENT_BODY_MAX_BYTES, EVENT_HEADERS_MAX};
use rt_types::{
    BuildReport, ConfigStream, ControlPlane, CpError, EnvBundle, MsgId, NodeEvent, PreparedKey, RouteSnapshot,
    SecretsVersion, Service, ServiceId, TaskId, Trigger, UsageBatch,
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

struct NoControl;

#[async_trait]
impl ControlPlane for NoControl {
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

/// Records what reached it; answers `fail` when set.
#[derive(Default)]
struct FakeBus {
    calls: AtomicU32,
    timeouts: Mutex<Vec<Option<Duration>>>,
    read_opts: Mutex<Vec<ReadOpts>>,
    fail: Option<&'static str>,
    hang: bool,
}

impl FakeBus {
    fn seen(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn enter(&self) -> Result<(), HostError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.fail {
            Some(code) => Err(HostError::new(code, "bus says no")),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl PubsubBus for FakeBus {
    async fn publish(&self, _f: &TaskCtx, _s: &str, _b: Bytes, _o: PublishOpts) -> Result<PublishOut, HostError> {
        self.enter()?;
        Ok(PublishOut {
            receivers: 2,
            dropped: 1,
        })
    }
    async fn request(&self, _f: &TaskCtx, _s: &str, body: Bytes, o: RequestOpts) -> Result<Reply, HostError> {
        self.timeouts.lock().unwrap().push(o.timeout);
        self.enter()?;
        if self.hang {
            std::future::pending::<()>().await;
        }
        Ok(Reply {
            content_type: Some("text/plain".into()),
            body,
        })
    }
}

#[async_trait]
impl StreamBus for FakeBus {
    async fn append(&self, _f: &TaskCtx, _t: &str, e: Vec<AppendEntry>) -> Result<Vec<AppendedOut>, HostError> {
        self.enter()?;
        Ok(e.iter()
            .enumerate()
            .map(|(i, _)| AppendedOut {
                partition: 0,
                offset: i as u64,
                duplicate: false,
            })
            .collect())
    }
    async fn read(&self, _f: &TaskCtx, _t: &str, o: ReadOpts) -> Result<ReadPage, HostError> {
        self.read_opts.lock().unwrap().push(o);
        self.enter()?;
        Ok(ReadPage::default())
    }
    async fn info(&self, _f: &TaskCtx, t: &str) -> Result<TopicInfo, HostError> {
        self.enter()?;
        Ok(TopicInfo {
            name: t.into(),
            ..Default::default()
        })
    }
}

fn services(dir: &tempfile::TempDir) -> HostServices {
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
        control: Arc::new(NoControl),
        mem: TestGovernor::with_mb(64),
        sockets: Arc::new(rt_host::NoSockets),
        policy: None,
    })
}

fn with_bus(dir: &tempfile::TempDir, bus: Arc<FakeBus>) -> HostServices {
    services(dir).with_pubsub(bus.clone()).with_streams(bus)
}

fn handle(s: &HostServices, deadline: Duration, trigger: Trigger) -> HostHandle {
    let spec = SpecBuilder::deno().trigger(trigger).finish();
    s.handle_for(&spec, TaskId(1), Instant::now() + deadline, HostTaskConfig::default())
}

fn task(s: &HostServices) -> HostHandle {
    handle(s, Duration::from_secs(60), Trigger::Http)
}

fn code<T>(r: Result<T, HostError>) -> &'static str {
    match r {
        Ok(_) => panic!("expected an error"),
        Err(e) => e.code,
    }
}

fn entry(body: &[u8]) -> AppendEntry {
    AppendEntry {
        body: Bytes::copy_from_slice(body),
        ..Default::default()
    }
}

fn read(limit: u32, partition: Option<u32>) -> ReadOpts {
    ReadOpts {
        partition,
        start: ReadStart::Earliest,
        limit,
    }
}

/// A handle outside any task refuses every bus call with `unavailable`, and the bus is not asked.
#[tokio::test]
async fn ac_evt_host_01_detached_handle_refuses_with_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(FakeBus::default());
    let s = with_bus(&dir, bus.clone());
    let h = s.detached_handle(SpecBuilder::deno().finish().service);
    assert_eq!(
        code(h.pubsub_publish("a.b", Bytes::new(), PublishOpts::default()).await),
        "unavailable"
    );
    assert_eq!(
        code(h.pubsub_request("a.b", Bytes::new(), RequestOpts::default()).await),
        "unavailable"
    );
    assert_eq!(code(h.stream_append("t", vec![entry(b"x")]).await), "unavailable");
    assert_eq!(code(h.stream_read("t", ReadOpts::default()).await), "unavailable");
    assert_eq!(code(h.stream_info("t").await), "unavailable");
    assert_eq!(bus.seen(), 0);
}

/// A node without the event bus answers `unavailable` for every call.
#[tokio::test]
async fn ac_evt_host_02_without_a_bus_every_call_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let h = task(&services(&dir));
    assert_eq!(
        code(h.pubsub_publish("a.b", Bytes::new(), PublishOpts::default()).await),
        "unavailable"
    );
    assert_eq!(
        code(h.pubsub_request("a.b", Bytes::new(), RequestOpts::default()).await),
        "unavailable"
    );
    assert_eq!(code(h.stream_append("t", vec![entry(b"x")]).await), "unavailable");
    assert_eq!(code(h.stream_read("t", ReadOpts::default()).await), "unavailable");
    assert_eq!(code(h.stream_info("t").await), "unavailable");
}

/// Subject, body size, headers and the call depth are checked before the bus sees a publish;
/// a valid publish returns the bus's `{receivers, dropped}`.
#[tokio::test]
async fn ac_evt_host_03_publish_validates_subject_body_and_headers_before_the_bus() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(FakeBus::default());
    let s = with_bus(&dir, bus.clone());
    let h = task(&s);
    let p = PublishOpts::default();
    for bad in ["", "a.*", "a.>", "_rt.x", "a..b", "a b"] {
        assert_eq!(
            code(h.pubsub_publish(bad, Bytes::new(), p.clone()).await),
            "validation_failed",
            "{bad:?}"
        );
    }
    let big = Bytes::from(vec![0u8; EVENT_BODY_MAX_BYTES + 1]);
    assert_eq!(code(h.pubsub_publish("a.b", big.clone(), p.clone()).await), "too_large");
    assert_eq!(
        code(h.pubsub_request("a.b", big, RequestOpts::default()).await),
        "too_large"
    );
    let many = PublishOpts {
        headers: (0..=EVENT_HEADERS_MAX).map(|i| (format!("h{i}"), "v".into())).collect(),
    };
    assert_eq!(code(h.pubsub_publish("a.b", Bytes::new(), many).await), "too_large");
    let crlf = PublishOpts {
        headers: vec![("h".into(), "a\r\nb".into())],
    };
    assert_eq!(
        code(h.pubsub_publish("a.b", Bytes::new(), crlf).await),
        "validation_failed"
    );
    let reserved = PublishOpts {
        headers: vec![("x-rt-trigger".into(), "x".into())],
    };
    assert_eq!(
        code(h.pubsub_publish("a.b", Bytes::new(), reserved).await),
        "validation_failed"
    );
    assert_eq!(bus.seen(), 0, "no invalid publish reaches the bus");

    // A delivery already 15 hops deep cannot publish (loop guard).
    let deep = handle(
        &s,
        Duration::from_secs(60),
        Trigger::Pubsub {
            subject: "a.b".into(),
            depth: rt_types::MAX_CALL_DEPTH,
            reply: false,
        },
    );
    assert_eq!(
        code(deep.pubsub_publish("a.b", Bytes::new(), p.clone()).await),
        "call_depth"
    );
    assert_eq!(
        code(deep.pubsub_request("a.b", Bytes::new(), RequestOpts::default()).await),
        "call_depth"
    );
    assert_eq!(bus.seen(), 0);

    let out = h
        .pubsub_publish("a.b", Bytes::from_static(b"{}"), p)
        .await
        .expect("publish");
    assert_eq!(
        out,
        PublishOut {
            receivers: 2,
            dropped: 1
        }
    );
}

/// The request timeout defaults to 5 s, is capped at 30 s and at the task's remaining time,
/// a task without time left gets `timeout`, and a responder that never answers ends in `timeout`.
#[tokio::test(start_paused = true)]
async fn ac_evt_host_04_request_timeout_defaults_to_5s_and_is_clamped_to_the_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(FakeBus::default());
    let s = with_bus(&dir, bus.clone());
    let h = task(&s);
    let reply = h
        .pubsub_request("a.b", Bytes::from_static(b"hi"), RequestOpts::default())
        .await
        .expect("reply");
    assert_eq!(reply.body, Bytes::from_static(b"hi"));
    let long = RequestOpts {
        timeout: Some(Duration::from_secs(120)),
        ..Default::default()
    };
    h.pubsub_request("a.b", Bytes::new(), long.clone())
        .await
        .expect("reply");
    let short = handle(&s, Duration::from_secs(2), Trigger::Http);
    short.pubsub_request("a.b", Bytes::new(), long).await.expect("reply");
    let seen = bus.timeouts.lock().unwrap().clone();
    assert_eq!(seen[0], Some(Duration::from_secs(5)));
    assert_eq!(seen[1], Some(Duration::from_secs(30)));
    let clamped = seen[2].expect("resolved");
    assert!(
        clamped <= Duration::from_secs(2) && clamped > Duration::from_secs(1),
        "{clamped:?}"
    );

    let zero = RequestOpts {
        timeout: Some(Duration::ZERO),
        ..Default::default()
    };
    assert_eq!(code(h.pubsub_request("a.b", Bytes::new(), zero).await), "timeout");
    let over = handle(&s, Duration::ZERO, Trigger::Http);
    assert_eq!(
        code(over.pubsub_request("a.b", Bytes::new(), RequestOpts::default()).await),
        "timeout"
    );

    let hanging = Arc::new(FakeBus {
        hang: true,
        ..Default::default()
    });
    let s2 = with_bus(&dir, hanging);
    let h2 = task(&s2);
    let started = tokio::time::Instant::now();
    assert_eq!(
        code(h2.pubsub_request("a.b", Bytes::new(), RequestOpts::default()).await),
        "timeout"
    );
    assert_eq!(started.elapsed(), Duration::from_secs(5));
}

/// An append holds 1..=100 records and at most 4 MiB of bodies; record limits apply per entry.
#[tokio::test]
async fn ac_evt_host_05_append_checks_batch_count_and_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(FakeBus::default());
    let h = task(&with_bus(&dir, bus.clone()));
    assert_eq!(code(h.stream_append("t", Vec::new()).await), "validation_failed");
    assert_eq!(code(h.stream_append("t", vec![entry(b"x"); 101]).await), "too_large");
    let mib = vec![0u8; 1024 * 1024];
    assert_eq!(code(h.stream_append("t", vec![entry(&mib); 5]).await), "too_large");
    assert_eq!(
        code(
            h.stream_append("t", vec![entry(&vec![0u8; EVENT_BODY_MAX_BYTES + 1])])
                .await
        ),
        "too_large"
    );
    assert_eq!(
        code(h.stream_append("Bad", vec![entry(b"x")]).await),
        "validation_failed"
    );
    assert_eq!(
        code(h.stream_append("tpc_x", vec![entry(b"x")]).await),
        "validation_failed"
    );
    let long_key = AppendEntry {
        key: Some("k".repeat(257)),
        ..entry(b"x")
    };
    assert_eq!(code(h.stream_append("t", vec![long_key]).await), "validation_failed");
    let long_dedup = AppendEntry {
        dedup_id: Some("d".repeat(129)),
        ..entry(b"x")
    };
    assert_eq!(code(h.stream_append("t", vec![long_dedup]).await), "validation_failed");
    let bad_header = AppendEntry {
        headers: vec![("x-rt-a".into(), "v".into())],
        ..entry(b"x")
    };
    assert_eq!(code(h.stream_append("t", vec![bad_header]).await), "validation_failed");
    assert_eq!(bus.seen(), 0);

    let out = h.stream_append("t", vec![entry(&mib); 4]).await.expect("append");
    assert_eq!(out.len(), 4);
    assert_eq!(
        h.stream_append("t", vec![entry(b"x"); 100])
            .await
            .expect("append")
            .len(),
        100
    );
}

/// A read takes `limit` 1..=100 and a partition below the hard maximum; whether the partition
/// exists or is required is left to the bus, which gets the options unchanged.
#[tokio::test]
async fn ac_evt_host_06_read_checks_limit_and_partition() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(FakeBus::default());
    let h = task(&with_bus(&dir, bus.clone()));
    assert_eq!(code(h.stream_read("t", read(0, None)).await), "validation_failed");
    assert_eq!(code(h.stream_read("t", read(101, None)).await), "validation_failed");
    assert_eq!(code(h.stream_read("t", read(10, Some(16))).await), "validation_failed");
    assert_eq!(code(h.stream_read("Bad", read(10, None)).await), "validation_failed");
    assert_eq!(code(h.stream_info("Bad").await), "validation_failed");
    assert_eq!(bus.seen(), 0);

    h.stream_read("t", read(100, Some(15))).await.expect("read");
    h.stream_read("t", read(1, None)).await.expect("read");
    let seen = bus.read_opts.lock().unwrap().clone();
    assert_eq!(seen, vec![read(100, Some(15)), read(1, None)]);
    assert_eq!(h.stream_info("t").await.expect("info").name, "t");
}

/// An error from the bus reaches the guest with its code and message unchanged.
#[tokio::test]
async fn ac_evt_host_07_bus_errors_keep_their_code() {
    let dir = tempfile::tempdir().unwrap();
    for code_in in ["no_responders", "stream_full", "not_found", "rate_limited"] {
        let bus = Arc::new(FakeBus {
            fail: Some(code_in),
            ..Default::default()
        });
        let h = task(&with_bus(&dir, bus));
        let e = h
            .pubsub_publish("a.b", Bytes::new(), PublishOpts::default())
            .await
            .unwrap_err();
        assert_eq!((e.code, e.message.as_str()), (code_in, "bus says no"));
        assert_eq!(
            code(h.pubsub_request("a.b", Bytes::new(), RequestOpts::default()).await),
            code_in
        );
        assert_eq!(code(h.stream_append("t", vec![entry(b"x")]).await), code_in);
        assert_eq!(code(h.stream_read("t", ReadOpts::default()).await), code_in);
        assert_eq!(code(h.stream_info("t").await), code_in);
    }
}
