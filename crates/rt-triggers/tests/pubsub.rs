//! Pub/sub router (plan-queues 2.1, plan-final 2.4).

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use rt_triggers::pubsub::{PubsubError, PubsubQuota, PubsubRouter, PubsubTaskFactory, SubRow};
use rt_triggers::Submitter;
use rt_types::events::{Namespace, PubsubMessage};
use rt_types::{
    Body, Digest, Outcome, ProjectId, RejectReason, Reply, ServiceId, ServiceRef, TaskKind, TaskSpec, TenantId,
    Trigger, VersionNo,
};

fn ns(t: u128, p: u128, env: &str) -> Namespace {
    Namespace {
        tenant: TenantId::from_u128(t),
        project: ProjectId::from_u128(p),
        environment: env.to_string(),
    }
}

fn svc(n: u128) -> ServiceId {
    ServiceId::from_u128(n)
}

fn row(ns: &Namespace, s: u128, pattern: &str, group: Option<&str>) -> SubRow {
    SubRow {
        ns: ns.clone(),
        service: svc(s),
        pattern: pattern.to_string(),
        group: group.map(str::to_string),
    }
}

struct Factory {
    gen: AtomicU64,
    rows: Mutex<Vec<SubRow>>,
    reads: AtomicUsize,
}

impl Factory {
    fn new(rows: Vec<SubRow>) -> Arc<Self> {
        Arc::new(Self {
            gen: AtomicU64::new(1),
            rows: Mutex::new(rows),
            reads: AtomicUsize::new(0),
        })
    }
}

impl PubsubTaskFactory for Factory {
    fn generation(&self) -> u64 {
        self.gen.load(Ordering::SeqCst)
    }
    fn subscriptions(&self) -> Vec<SubRow> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.rows.lock().clone()
    }
    fn task_for(&self, service: ServiceId, msg: PubsubMessage, depth: u8, _cap: Option<Duration>) -> Option<TaskSpec> {
        let sref = ServiceRef {
            tenant: TenantId::from_u128(1),
            service,
            version: VersionNo(1),
            digest: Digest::of(b"code"),
        };
        // Deliberately `reply: false`: the router sets the trigger itself.
        let trig = Trigger::Pubsub {
            subject: Arc::from(msg.subject.as_str()),
            depth,
            reply: false,
        };
        Some(TaskSpec::new(TaskKind::DENO, sref, trig).with_payload(rt_types::Payload::Pubsub(msg)))
    }
}

type Answer = Arc<dyn Fn(&TaskSpec) -> Option<Outcome> + Send + Sync>;

/// Records every submitted spec; `answer` gives the outcome (`None` = never completes) or the
/// submitter refuses with `refuse`.
struct Sched {
    specs: Mutex<Vec<TaskSpec>>,
    refuse: Mutex<Option<RejectReason>>,
}

fn submitter(answer: Answer) -> (Arc<Sched>, Submitter) {
    let s = Arc::new(Sched {
        specs: Mutex::new(Vec::new()),
        refuse: Mutex::new(None),
    });
    let s2 = s.clone();
    let sub: Submitter = Arc::new(move |spec: TaskSpec| {
        if let Some(r) = *s2.refuse.lock() {
            return Err(r);
        }
        let out = answer(&spec);
        s2.specs.lock().push(spec);
        Ok(Box::pin(async move {
            match out {
                Some(o) => o,
                None => std::future::pending().await,
            }
        }))
    });
    (s, sub)
}

fn ok_answer() -> Answer {
    Arc::new(|_| Some(Outcome::Ok(Reply::ok())))
}

fn delivered(s: &Sched) -> Vec<ServiceId> {
    s.specs.lock().iter().map(|t| t.service.service).collect()
}

fn publish(r: &PubsubRouter, n: &Namespace, subject: &str) -> Result<(u32, u32), PubsubError> {
    r.publish(n, 1, subject, Bytes::from_static(b"{}"), vec![])
}

/// Every matching ungrouped service gets exactly one copy, even with several matching patterns.
#[tokio::test]
async fn ac_ps_01_publish_delivers_one_copy_per_matching_ungrouped_service() {
    let n = ns(1, 1, "prod");
    let f = Factory::new(vec![
        row(&n, 1, "orders.*", None),
        row(&n, 1, "orders.>", None),
        row(&n, 2, "orders.created", None),
        row(&n, 3, "users.*", None),
    ]);
    let (s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(f, sub);
    assert_eq!(publish(&r, &n, "orders.created").unwrap(), (2, 0));
    let mut got = delivered(&s);
    got.sort();
    assert_eq!(got, vec![svc(1), svc(2)]);
}

/// A group delivers each message to one member, round robin.
#[tokio::test]
async fn ac_ps_02_a_group_delivers_to_one_member_round_robin() {
    let n = ns(1, 1, "prod");
    let f = Factory::new(vec![
        row(&n, 1, "jobs.*", Some("billing")),
        row(&n, 2, "jobs.*", Some("billing")),
    ]);
    let (s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(f, sub);
    for _ in 0..4 {
        assert_eq!(publish(&r, &n, "jobs.run").unwrap(), (1, 0));
    }
    assert_eq!(delivered(&s), vec![svc(1), svc(2), svc(1), svc(2)]);
}

/// Subscriptions of another environment, project or tenant never receive.
#[tokio::test]
async fn ac_ps_03_namespaces_never_cross() {
    let n = ns(1, 1, "prod");
    let f = Factory::new(vec![
        row(&ns(1, 1, "staging"), 2, "a.b", None),
        row(&ns(1, 2, "prod"), 3, "a.b", None),
        row(&ns(2, 1, "prod"), 4, "a.b", None),
        row(&n, 5, "a.b", None),
    ]);
    let (s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(f, sub);
    assert_eq!(publish(&r, &n, "a.b").unwrap(), (1, 0));
    assert_eq!(delivered(&s), vec![svc(5)]);
}

/// No subscriber: zero receivers, no error.
#[tokio::test]
async fn ac_ps_04_no_subscriber_means_zero_receivers() {
    let n = ns(1, 1, "prod");
    let (s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(Factory::new(vec![]), sub);
    assert_eq!(publish(&r, &n, "x").unwrap(), (0, 0));
    assert!(delivered(&s).is_empty());
}

/// More than 100 receivers: 100 delivered, the rest dropped and counted as `fanout`.
#[tokio::test]
async fn ac_ps_05_fanout_is_capped_at_100_and_drops_are_counted() {
    let n = ns(1, 1, "prod");
    let rows = (1..=130).map(|i| row(&n, i, "e", None)).collect();
    let (s, sub) = submitter(ok_answer());
    let m = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let r = PubsubRouter::new(Factory::new(rows), sub).with_metrics(m.clone());
    assert_eq!(publish(&r, &n, "e").unwrap(), (100, 30));
    assert_eq!(delivered(&s).len(), 100);
    assert_eq!(m.pubsub_dropped_total.with_label_values(&["fanout"]).get(), 30);
}

/// A refused delivery is dropped (`rejected`), never retried.
#[tokio::test]
async fn ac_ps_06_refused_deliveries_are_dropped_not_retried() {
    let n = ns(1, 1, "prod");
    let (s, sub) = submitter(ok_answer());
    *s.refuse.lock() = Some(RejectReason::Overloaded);
    let m = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let r = PubsubRouter::new(Factory::new(vec![row(&n, 1, "a", None), row(&n, 2, "a", None)]), sub)
        .with_metrics(m.clone());
    assert_eq!(publish(&r, &n, "a").unwrap(), (0, 2));
    *s.refuse.lock() = None;
    tokio::task::yield_now().await;
    assert!(delivered(&s).is_empty(), "no retry");
    assert_eq!(m.pubsub_dropped_total.with_label_values(&["rejected"]).get(), 2);
}

/// A request goes to exactly one responder and resolves to its reply body and content type.
#[tokio::test]
async fn ac_ps_07_request_picks_one_responder_and_returns_its_body() {
    let n = ns(1, 1, "prod");
    let answer: Answer = Arc::new(|spec| {
        let mut reply = Reply::new(http::StatusCode::OK, Body::from_bytes(spec.service.service.to_string()));
        reply
            .headers
            .insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("text/plain"));
        Some(Outcome::Ok(reply))
    });
    let (s, sub) = submitter(answer);
    let r = PubsubRouter::new(Factory::new(vec![row(&n, 1, "q", None), row(&n, 2, "q", None)]), sub);
    let (ct, body) = r
        .request(&n, 1, "q", Bytes::new(), vec![], Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(ct.as_deref(), Some("text/plain"));
    assert_eq!(delivered(&s).len(), 1);
    assert_eq!(body, Bytes::from(delivered(&s)[0].to_string()));
    r.request(&n, 1, "q", Bytes::new(), vec![], Duration::from_secs(1))
        .await
        .unwrap();
    let got = delivered(&s);
    assert_ne!(got[0], got[1], "round robin over responders");
}

/// A request without a matching subscription fails `no_responders` at once.
#[tokio::test]
async fn ac_ps_08_request_without_subscribers_is_no_responders() {
    let n = ns(1, 1, "prod");
    let (_s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(Factory::new(vec![row(&n, 1, "other", None)]), sub);
    let e = r
        .request(&n, 1, "q", Bytes::new(), vec![], Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "no_responders");
}

/// No reply within the timeout: `timeout`; the responder's wall is capped by it.
#[tokio::test(start_paused = true)]
async fn ac_ps_09_request_times_out() {
    let n = ns(1, 1, "prod");
    let (s, sub) = submitter(Arc::new(|_| None));
    let r = PubsubRouter::new(Factory::new(vec![row(&n, 1, "q", None)]), sub);
    let e = r
        .request(&n, 1, "q", Bytes::new(), vec![], Duration::from_millis(200))
        .await
        .unwrap_err();
    assert_eq!(e, PubsubError::Timeout);
    assert!(s.specs.lock()[0].limits.wall <= Duration::from_millis(200));
}

/// A non-2xx reply or a guest error is `responder_failed`; a refusal keeps its reason.
#[tokio::test]
async fn ac_ps_10_request_non_2xx_or_guest_error_is_responder_failed() {
    let n = ns(1, 1, "prod");
    let rows = vec![row(&n, 1, "q", None)];
    for (out, code) in [
        (
            Arc::new(|_: &TaskSpec| Some(Outcome::Ok(Reply::new(http::StatusCode::BAD_GATEWAY, Body::empty()))))
                as Answer,
            "responder_failed",
        ),
        (
            Arc::new(|_: &TaskSpec| Some(Outcome::GuestError)) as Answer,
            "responder_failed",
        ),
        (
            Arc::new(|_: &TaskSpec| Some(Outcome::Rejected(RejectReason::QueueTimeout))) as Answer,
            "queue_timeout",
        ),
    ] {
        let (_s, sub) = submitter(out);
        let r = PubsubRouter::new(Factory::new(rows.clone()), sub);
        let e = r
            .request(&n, 1, "q", Bytes::new(), vec![], Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(e.code(), code);
    }
    // A reply over 1 MiB is too_large.
    let big: Answer = Arc::new(|_| {
        Some(Outcome::Ok(Reply::new(
            http::StatusCode::OK,
            Body::from(vec![0u8; 1024 * 1024 + 1]),
        )))
    });
    let (_s, sub) = submitter(big);
    let r = PubsubRouter::new(Factory::new(rows.clone()), sub);
    let e = r
        .request(&n, 1, "q", Bytes::new(), vec![], Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(e, PubsubError::TooLarge);
    // A refused submit maps to its reason.
    let (s, sub) = submitter(ok_answer());
    *s.refuse.lock() = Some(RejectReason::Draining);
    let r = PubsubRouter::new(Factory::new(rows), sub);
    let e = r
        .request(&n, 1, "q", Bytes::new(), vec![], Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "draining");
}

/// A delivery deeper than MAX_CALL_DEPTH fails `call_depth`; bad input is rejected first.
#[tokio::test]
async fn ac_ps_11_depth_past_max_is_call_depth() {
    let n = ns(1, 1, "prod");
    let (_s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(Factory::new(vec![row(&n, 1, "a", None)]), sub);
    let e = r
        .publish(&n, rt_types::MAX_CALL_DEPTH + 1, "a", Bytes::new(), vec![])
        .unwrap_err();
    assert_eq!(e.code(), "call_depth");
    let e = r
        .request(
            &n,
            rt_types::MAX_CALL_DEPTH + 1,
            "a",
            Bytes::new(),
            vec![],
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(e.unwrap_err().code(), "call_depth");
    assert_eq!(publish(&r, &n, "a.*").unwrap_err().code(), "validation_failed");
    let big = Bytes::from(vec![0u8; 1024 * 1024 + 1]);
    assert_eq!(r.publish(&n, 1, "a", big, vec![]).unwrap_err().code(), "too_large");
    let bad = vec![("x".to_string(), "a\nb".to_string())];
    assert_eq!(
        r.publish(&n, 1, "a", Bytes::new(), bad).unwrap_err().code(),
        "validation_failed"
    );
    assert_eq!(PubsubError::Unavailable.code(), "unavailable");
    assert_eq!(PubsubError::Validation("m".into()).to_string(), "m");
    assert_eq!(PubsubError::Timeout.to_string(), "timeout");
}

/// The index is cached per generation and rebuilt when it changes.
#[tokio::test]
async fn ac_ps_12_index_rebuilds_when_the_generation_changes() {
    let n = ns(1, 1, "prod");
    let f = Factory::new(vec![row(&n, 1, "a", None)]);
    let (_s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(f.clone(), sub);
    assert_eq!(publish(&r, &n, "a").unwrap(), (1, 0));
    f.rows.lock().push(row(&n, 2, "a", None));
    assert_eq!(publish(&r, &n, "a").unwrap(), (1, 0), "cached");
    assert_eq!(f.reads.load(Ordering::SeqCst), 1);
    f.gen.fetch_add(1, Ordering::SeqCst);
    assert_eq!(publish(&r, &n, "a").unwrap(), (2, 0));
    assert_eq!(f.reads.load(Ordering::SeqCst), 2);
}

/// The publish's own quota refusal goes back to the publisher, nothing is delivered.
#[tokio::test]
async fn ac_ps_13_quota_refusal_is_returned_to_the_publisher() {
    let n = ns(1, 1, "prod");
    let (s, sub) = submitter(ok_answer());
    let q: PubsubQuota = Arc::new(|_| Err(PubsubError::RateLimited));
    let r = PubsubRouter::new(Factory::new(vec![row(&n, 1, "a", None)]), sub).with_quota(q);
    assert_eq!(publish(&r, &n, "a").unwrap_err().code(), "rate_limited");
    let e = r
        .request(&n, 1, "a", Bytes::new(), vec![], Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "rate_limited");
    assert!(delivered(&s).is_empty());
}

/// Every delivery takes its own quota check; a refused one is dropped as `quota`.
#[tokio::test]
async fn ac_ps_14_each_delivery_is_quota_checked() {
    let n = ns(1, 1, "prod");
    let (s, sub) = submitter(ok_answer());
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    // The publish check and the first delivery pass; the rest are refused.
    let q: PubsubQuota = Arc::new(move |_| {
        if c.fetch_add(1, Ordering::SeqCst) < 2 {
            Ok(())
        } else {
            Err(PubsubError::QuotaExceeded)
        }
    });
    let m = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let rows = (1..=3).map(|i| row(&n, i, "a", None)).collect();
    let r = PubsubRouter::new(Factory::new(rows), sub)
        .with_quota(q)
        .with_metrics(m.clone());
    assert_eq!(publish(&r, &n, "a").unwrap(), (1, 2));
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(delivered(&s).len(), 1);
    assert_eq!(m.pubsub_dropped_total.with_label_values(&["quota"]).get(), 2);
}

/// A subscriber with PUBSUB_INFLIGHT_PER_SERVICE deliveries in flight drops more as `backlog`;
/// the counter falls again when deliveries finish.
#[tokio::test]
async fn ac_ps_15_inflight_cap_drops_with_backlog() {
    let n = ns(1, 1, "prod");
    let gate = Arc::new(tokio::sync::Notify::new());
    let (s, sub0) = submitter(ok_answer());
    let g = gate.clone();
    let sub: Submitter = Arc::new(move |spec| {
        let fut = sub0(spec)?;
        let g = g.clone();
        Ok(Box::pin(async move {
            g.notified().await;
            fut.await
        }))
    });
    let m = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).unwrap();
    let r = PubsubRouter::new(Factory::new(vec![row(&n, 1, "a", None)]), sub).with_metrics(m.clone());
    let cap = rt_types::events::PUBSUB_INFLIGHT_PER_SERVICE;
    for _ in 0..cap {
        assert_eq!(publish(&r, &n, "a").unwrap(), (1, 0));
    }
    assert_eq!(r.inflight(svc(1)), cap);
    assert_eq!(publish(&r, &n, "a").unwrap(), (0, 1));
    assert_eq!(m.pubsub_dropped_total.with_label_values(&["backlog"]).get(), 1);
    assert_eq!(delivered(&s).len(), cap as usize);
    for _ in 0..200 {
        gate.notify_waiters();
        tokio::task::yield_now().await;
        if r.inflight(svc(1)) == 0 {
            break;
        }
    }
    assert_eq!(r.inflight(svc(1)), 0);
    assert_eq!(publish(&r, &n, "a").unwrap(), (1, 0));
}

/// Fan-out truncation starts at a rotating offset, so all receivers get turns.
#[tokio::test]
async fn ac_ps_16_fanout_truncation_rotates() {
    let n = ns(1, 1, "prod");
    let rows = (1..=150).map(|i| row(&n, i, "e", None)).collect();
    let (s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(Factory::new(rows), sub);
    publish(&r, &n, "e").unwrap();
    let first: Vec<_> = delivered(&s);
    s.specs.lock().clear();
    publish(&r, &n, "e").unwrap();
    let second = delivered(&s);
    assert_ne!(first[0], second[0]);
    let mut all: Vec<_> = first.into_iter().chain(second).collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 150, "two publishes cover every receiver");
}

/// A request's delivery task is `Trigger::Pubsub { reply: true }`; a publish's is `reply: false`.
#[tokio::test]
async fn ac_ps_17_request_task_has_reply_true() {
    let n = ns(1, 1, "prod");
    let (s, sub) = submitter(ok_answer());
    let r = PubsubRouter::new(Factory::new(vec![row(&n, 1, "a", None)]), sub);
    r.request(&n, 3, "a", Bytes::new(), vec![], Duration::from_secs(1))
        .await
        .unwrap();
    publish(&r, &n, "a").unwrap();
    let specs = s.specs.lock();
    assert!(matches!(&specs[0].trigger, Trigger::Pubsub { reply: true, depth: 3, subject } if &**subject == "a"));
    assert!(matches!(
        &specs[1].trigger,
        Trigger::Pubsub {
            reply: false,
            depth: 1,
            ..
        }
    ));
}
