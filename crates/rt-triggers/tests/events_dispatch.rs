//! Event dispatcher: host pub/sub and stream buses (plan-queues 4.6, plan-final 2.4/2.5).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use rt_host::events::{AppendEntry, PublishOpts, PubsubBus, ReadOpts, ReadStart, RequestOpts, StreamBus};
use rt_host::TaskCtx;
use rt_sec::PlanSource;
use rt_store::stream::{SqliteStreamStore, StreamStore, TopicSpec};
use rt_triggers::events::EventDispatcher;
use rt_triggers::pubsub::{PubsubError, PubsubRouter, PubsubTaskFactory, SubRow};
use rt_triggers::{RouteTable, Submitter};
use rt_types::events::{Namespace, PubsubMessage};
use rt_types::{
    Access, CallAccess, Digest, IngressRules, Outcome, Plan, ProjectId, Reply, RouteEntry, RouteSnapshot, RuntimeKind,
    ServiceId, ServiceRef, TaskId, TaskKind, TaskSpec, TenantId, TopicId, Trigger, TriggerSwitches, VersionNo,
    MAX_CALL_DEPTH,
};

fn entry(tenant: u128, service: u128, project: u128, env: &str) -> RouteEntry {
    RouteEntry {
        tenant: TenantId::from_u128(tenant),
        service: ServiceId::from_u128(service),
        slug: format!("svc-{service}"),
        runtime: RuntimeKind::Deno,
        version: VersionNo(1),
        digest: Digest::of(b"code"),
        access: Access::Public,
        triggers: TriggerSwitches::default(),
        ingress: IngressRules::default(),
        profile: None,
        hosts: Vec::new(),
        project: ProjectId::from_u128(project),
        environment: env.to_string(),
        call: CallAccess::default(),
    }
}

fn ns_of(e: &RouteEntry) -> Namespace {
    Namespace {
        tenant: e.tenant,
        project: e.project,
        environment: e.environment.clone(),
    }
}

fn ctx(tenant: u128, service: u128, depth: u8) -> TaskCtx {
    TaskCtx {
        tenant: TenantId::from_u128(tenant),
        service: ServiceId::from_u128(service),
        task: TaskId(1),
        depth,
        deadline: Instant::now() + Duration::from_secs(30),
        trace: None,
        on_behalf_of: None,
        object: None,
    }
}

struct Plans(Plan);

impl PlanSource for Plans {
    fn plan_for(&self, _tenant: TenantId) -> Plan {
        self.0.clone()
    }
}

struct Factory(Vec<SubRow>);

impl PubsubTaskFactory for Factory {
    fn generation(&self) -> u64 {
        1
    }
    fn subscriptions(&self) -> Vec<SubRow> {
        self.0.clone()
    }
    fn task_for(&self, service: ServiceId, msg: PubsubMessage, depth: u8, _cap: Option<Duration>) -> Option<TaskSpec> {
        let sref = ServiceRef {
            tenant: TenantId::from_u128(1),
            service,
            version: VersionNo(1),
            digest: Digest::of(b"code"),
        };
        let trig = Trigger::Pubsub {
            subject: Arc::from(msg.subject.as_str()),
            depth,
            reply: false,
        };
        Some(TaskSpec::new(TaskKind::DENO, sref, trig).with_payload(rt_types::Payload::Pubsub(msg)))
    }
}

struct Harness {
    d: EventDispatcher,
    store: Arc<SqliteStreamStore>,
    specs: Arc<Mutex<Vec<TaskSpec>>>,
    notified: Arc<Mutex<Vec<TopicId>>>,
    entries: Vec<RouteEntry>,
}

/// Routes: service 1 (tenant 1, project 1, prod), service 2 (tenant 1, project 1, staging),
/// service 3 (tenant 2, project 2, prod). Subscriptions given per test.
fn harness(subs: impl Fn(&[RouteEntry]) -> Vec<SubRow>, plan: Plan) -> Harness {
    let entries = vec![
        entry(1, 1, 1, "prod"),
        entry(1, 2, 1, "staging"),
        entry(2, 3, 2, "prod"),
    ];
    let routes = RouteTable::new(None);
    routes.swap(&RouteSnapshot {
        generation: 1,
        entries: entries.clone(),
        ..RouteSnapshot::default()
    });
    let specs = Arc::new(Mutex::new(Vec::new()));
    let s2 = specs.clone();
    let submit: Submitter = Arc::new(move |spec: TaskSpec| {
        s2.lock().push(spec);
        Ok(Box::pin(async { Outcome::Ok(Reply::ok()) }))
    });
    let router = Arc::new(PubsubRouter::new(Arc::new(Factory(subs(&entries))), submit));
    let store = SqliteStreamStore::open_in_memory().unwrap();
    let notified = Arc::new(Mutex::new(Vec::new()));
    let n2 = notified.clone();
    let d = EventDispatcher::new(
        routes,
        router,
        store.clone(),
        Arc::new(Plans(plan)),
        Arc::new(move |t| n2.lock().push(t)),
    );
    Harness {
        d,
        store,
        specs,
        notified,
        entries,
    }
}

async fn topic(h: &Harness, e: usize, name: &str, partitions: u32) -> TopicId {
    h.store
        .create_topic(
            &ns_of(&h.entries[e]),
            TopicSpec {
                name: name.to_string(),
                partitions,
                retention_ms: 3_600_000,
                max_bytes: 0,
            },
            100,
        )
        .await
        .unwrap()
        .id
}

fn rec(body: &'static [u8]) -> AppendEntry {
    AppendEntry {
        body: Bytes::from_static(body),
        ..Default::default()
    }
}

/// The namespace is the route table entry's, never the guest's: a task claiming another tenant is
/// `unavailable`, an unknown service is `unavailable`, and topics/subscribers of other
/// environments or tenants are invisible.
#[tokio::test]
async fn ac_evt_disp_01_namespace_comes_from_the_route_table_not_the_guest() {
    let h = harness(
        |e| {
            vec![
                SubRow {
                    ns: ns_of(&e[0]),
                    service: e[0].service,
                    pattern: "a.*".into(),
                    group: None,
                },
                SubRow {
                    ns: ns_of(&e[1]),
                    service: e[1].service,
                    pattern: "a.*".into(),
                    group: None,
                },
            ]
        },
        Plan::default(),
    );
    let ns = h.d.namespace(&ctx(1, 1, 0)).unwrap();
    assert_eq!(ns, ns_of(&h.entries[0]));
    // Service 1 belongs to tenant 1: a task claiming tenant 2 for it is refused.
    let e = h.d.namespace(&ctx(2, 1, 0)).unwrap_err();
    assert_eq!(e.code, "unavailable");
    let e =
        h.d.publish(&ctx(1, 99, 0), "a.b", Bytes::new(), PublishOpts::default())
            .await
            .unwrap_err();
    assert_eq!(e.code, "unavailable");

    // Publish from prod reaches only the prod subscriber.
    let out =
        h.d.publish(&ctx(1, 1, 0), "a.b", Bytes::new(), PublishOpts::default())
            .await
            .unwrap();
    assert_eq!((out.receivers, out.dropped), (1, 0));
    assert_eq!(h.specs.lock()[0].service.service, ServiceId::from_u128(1));

    // A topic of tenant 1 / prod is not found from staging or from tenant 2.
    topic(&h, 0, "orders", 1).await;
    assert!(h.d.info(&ctx(1, 1, 0), "orders").await.is_ok());
    assert_eq!(h.d.info(&ctx(1, 2, 0), "orders").await.unwrap_err().code, "not_found");
    assert_eq!(h.d.info(&ctx(2, 3, 0), "orders").await.unwrap_err().code, "not_found");
}

/// An unknown topic is `not_found` for append/read/info; a successful append notifies the engine
/// with the topic id, and the records can be read back.
#[tokio::test]
async fn ac_evt_disp_02_unknown_topic_is_not_found_and_append_notifies_the_engine() {
    let h = harness(|_| vec![], Plan::default());
    let c = ctx(1, 1, 0);
    assert_eq!(
        h.d.append(&c, "nope", vec![rec(b"x")]).await.unwrap_err().code,
        "not_found"
    );
    assert_eq!(
        h.d.read(&c, "nope", ReadOpts::default()).await.unwrap_err().code,
        "not_found"
    );
    assert!(h.notified.lock().is_empty());

    let id = topic(&h, 0, "orders", 1).await;
    let out = h.d.append(&c, "orders", vec![rec(b"a"), rec(b"b")]).await.unwrap();
    assert_eq!(out.iter().map(|a| a.offset).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(*h.notified.lock(), vec![id]);

    let page = h.d.read(&c, "orders", ReadOpts::default()).await.unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!((page.next, page.end), (2, 2));
    let page =
        h.d.read(
            &c,
            "orders",
            ReadOpts {
                start: ReadStart::Latest,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(page.records.is_empty());
    assert_eq!((page.next, page.end), (2, 2));

    let info = h.d.info(&c, "orders").await.unwrap();
    assert_eq!(info.partitions, vec![(0, 0, 2)]);
    assert_eq!(info.retention_hours, 1);
    assert_eq!(info.bytes, 2);
}

/// The store's `stream_full` conflict (tenant cap from `plan.max_stream_mb`) maps to the guest
/// code `stream_full`, and nothing is notified.
#[tokio::test]
async fn ac_evt_disp_03_stream_full_maps_to_stream_full() {
    let plan = Plan {
        max_stream_mb: 0,
        ..Plan::default()
    };
    let h = harness(|_| vec![], plan);
    topic(&h, 0, "orders", 1).await;
    let e = h.d.append(&ctx(1, 1, 0), "orders", vec![rec(b"x")]).await.unwrap_err();
    assert_eq!(e.code, "stream_full");
    assert!(!e.message.contains("conflict"));
    assert!(h.notified.lock().is_empty());
}

/// A publish runs its deliveries at `child_depth(from.depth)`; past the maximum it is
/// `call_depth` (publish and request), with nothing delivered.
#[tokio::test]
async fn ac_evt_disp_04_publish_depth_is_child_depth() {
    let h = harness(
        |e| {
            vec![SubRow {
                ns: ns_of(&e[0]),
                service: e[0].service,
                pattern: "a.b".into(),
                group: None,
            }]
        },
        Plan::default(),
    );
    h.d.publish(&ctx(1, 1, 3), "a.b", Bytes::new(), PublishOpts::default())
        .await
        .unwrap();
    match &h.specs.lock()[0].trigger {
        Trigger::Pubsub { depth, .. } => assert_eq!(*depth, 4),
        t => panic!("unexpected trigger {t:?}"),
    }
    let c = ctx(1, 1, MAX_CALL_DEPTH);
    let e =
        h.d.publish(&c, "a.b", Bytes::new(), PublishOpts::default())
            .await
            .unwrap_err();
    assert_eq!(e.code, "call_depth");
    let e =
        h.d.request(&c, "a.b", Bytes::new(), RequestOpts::default())
            .await
            .unwrap_err();
    assert_eq!(e.code, "call_depth");
    assert_eq!(h.specs.lock().len(), 1);

    // A request below the cap gets the responder's reply.
    let r =
        h.d.request(&ctx(1, 1, 0), "a.b", Bytes::new(), RequestOpts::default())
            .await
            .unwrap();
    assert!(r.body.is_empty());
    // No responder.
    let e =
        h.d.request(&ctx(1, 1, 0), "z.z", Bytes::new(), RequestOpts::default())
            .await
            .unwrap_err();
    assert_eq!(e.code, "no_responders");
}

/// Each append call takes exactly one tenant quota token (however many records); a refusal is
/// returned to the guest (`rate_limited` / `quota_exceeded`) and nothing is appended.
#[tokio::test]
async fn ac_evt_disp_05_append_takes_a_quota_token() {
    let calls = Arc::new(AtomicUsize::new(0));
    let refuse = Arc::new(Mutex::new(None::<PubsubError>));
    let (c2, r2) = (calls.clone(), refuse.clone());
    let mut h = harness(|_| vec![], Plan::default());
    h.d = h.d.with_quota(Arc::new(move |t| {
        assert_eq!(t, TenantId::from_u128(1));
        c2.fetch_add(1, Ordering::SeqCst);
        match r2.lock().clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }));
    let id = topic(&h, 0, "orders", 1).await;
    let c = ctx(1, 1, 0);
    h.d.append(&c, "orders", vec![rec(b"a"), rec(b"b"), rec(b"c")])
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    *refuse.lock() = Some(PubsubError::RateLimited);
    assert_eq!(
        h.d.append(&c, "orders", vec![rec(b"d")]).await.unwrap_err().code,
        "rate_limited"
    );
    *refuse.lock() = Some(PubsubError::QuotaExceeded);
    assert_eq!(
        h.d.append(&c, "orders", vec![rec(b"d")]).await.unwrap_err().code,
        "quota_exceeded"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(h.store.heads(id).await.unwrap()[0].next, 3);
}

/// On a topic with more than one partition `read` without `partition` is `validation_failed`
/// (the message names the count); an out-of-range partition is too; a one-partition topic reads
/// partition 0 by default.
#[tokio::test]
async fn ac_evt_disp_06_read_without_partition_on_a_multi_partition_topic_is_validation_failed() {
    let h = harness(|_| vec![], Plan::default());
    topic(&h, 0, "multi", 3).await;
    topic(&h, 0, "single", 1).await;
    let c = ctx(1, 1, 0);
    let e = h.d.read(&c, "multi", ReadOpts::default()).await.unwrap_err();
    assert_eq!(e.code, "validation_failed");
    assert!(
        e.message.contains("has 3 partitions: pass `partition`"),
        "{}",
        e.message
    );
    let o = ReadOpts {
        partition: Some(3),
        ..Default::default()
    };
    assert_eq!(h.d.read(&c, "multi", o).await.unwrap_err().code, "validation_failed");
    let o = ReadOpts {
        partition: Some(2),
        start: ReadStart::Offset(5),
        ..Default::default()
    };
    let page = h.d.read(&c, "multi", o).await.unwrap();
    assert_eq!((page.next, page.end), (0, 0));
    assert!(h.d.read(&c, "single", ReadOpts::default()).await.is_ok());
    let o = ReadOpts {
        start: ReadStart::TimeMs(0),
        ..Default::default()
    };
    assert!(h.d.read(&c, "single", o).await.unwrap().records.is_empty());
}
