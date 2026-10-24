//! Pub/sub router: subject fan-out, groups, and request/reply responders.
//!
//! Delivery is at-most-once and in memory: every delivery is a `Trigger::Pubsub` task submitted
//! through the scheduler's normal admission; a refused delivery is dropped and counted, never
//! retried. Routing stays inside the publisher's namespace (tenant/project/environment).
//!
//! Per publish (plan-final 2.4): one quota check for the publish itself (refusal goes back to the
//! publisher), then the receiver list (one copy per matching ungrouped service, one member per
//! group, round robin per (namespace, group)); past [`PUBSUB_FANOUT_MAX`] receivers a rotating
//! window per namespace is taken and the rest dropped (`fanout`). Each delivery is quota checked
//! (`quota`), capped at [`PUBSUB_INFLIGHT_PER_SERVICE`] in flight per subscriber (`backlog`), and
//! dropped when the factory has no task (`no_task`) or admission refuses it (`rejected`).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures_util::StreamExt;
use parking_lot::Mutex;
use rt_types::events::{
    headers_over_limits, subject_matches, validate_headers, validate_subject, Namespace, PubsubMessage,
    EVENT_BODY_MAX_BYTES, PUBSUB_FANOUT_MAX, PUBSUB_INFLIGHT_PER_SERVICE,
};
use rt_types::{MsgId, Outcome, RejectReason, ServiceId, TaskSpec, TenantId, Trigger, MAX_CALL_DEPTH};

use crate::sockets::Submitter;

/// One subscription of a routable service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubRow {
    pub ns: Namespace,
    pub service: ServiceId,
    pub pattern: String,
    pub group: Option<String>,
}

/// Supplies subscriptions and builds delivery tasks. Implemented by `rt-server`.
pub trait PubsubTaskFactory: Send + Sync {
    /// Changes whenever the service table / routes change (the index is rebuilt then).
    fn generation(&self) -> u64;
    /// Subscriptions of routable services whose plan allows `pubsub`.
    fn subscriptions(&self) -> Vec<SubRow>;
    /// The delivery task for `service`; `None` = no task (not routable any more).
    fn task_for(
        &self,
        service: ServiceId,
        msg: PubsubMessage,
        depth: u8,
        wall_cap: Option<Duration>,
    ) -> Option<TaskSpec>;
}

/// Why a publish or request failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PubsubError {
    Validation(String),
    TooLarge,
    Depth,
    NoResponders,
    Timeout,
    ResponderFailed,
    Rejected(RejectReason),
    RateLimited,
    QuotaExceeded,
    Unavailable,
}

impl PubsubError {
    /// The stable guest-facing code.
    pub fn code(&self) -> &'static str {
        match self {
            PubsubError::Validation(_) => "validation_failed",
            PubsubError::TooLarge => "too_large",
            PubsubError::Depth => "call_depth",
            PubsubError::NoResponders => "no_responders",
            PubsubError::Timeout => "timeout",
            PubsubError::ResponderFailed => "responder_failed",
            PubsubError::Rejected(r) => r.reason(),
            PubsubError::RateLimited => "rate_limited",
            PubsubError::QuotaExceeded => "quota_exceeded",
            PubsubError::Unavailable => "unavailable",
        }
    }
}

impl std::fmt::Display for PubsubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubsubError::Validation(m) => write!(f, "{m}"),
            other => f.write_str(other.code()),
        }
    }
}

impl std::error::Error for PubsubError {}

/// Tenant quota check (rps + daily), taken per publish/request and per delivery.
pub type PubsubQuota = Arc<dyn Fn(TenantId) -> Result<(), PubsubError> + Send + Sync>;

struct Entry {
    pattern: String,
    group: Option<String>,
    service: ServiceId,
}

struct Index {
    generation: u64,
    by_ns: HashMap<Namespace, Vec<Entry>>,
}

/// Matching subscriptions of one subject: ungrouped services (deduplicated, first-seen order) and
/// the members of each group (by group name).
struct Matches {
    ungrouped: Vec<ServiceId>,
    groups: BTreeMap<String, Vec<ServiceId>>,
}

fn push_unique(v: &mut Vec<ServiceId>, s: ServiceId) {
    if !v.contains(&s) {
        v.push(s);
    }
}

/// The in-memory pub/sub router.
pub struct PubsubRouter {
    tasks: Arc<dyn PubsubTaskFactory>,
    submit: Submitter,
    metrics: Option<Arc<rt_obs::Metrics>>,
    quota: Option<PubsubQuota>,
    index: Mutex<Option<Arc<Index>>>,
    group_rr: DashMap<(Namespace, String), Arc<AtomicUsize>>,
    subject_rr: DashMap<(Namespace, String), Arc<AtomicUsize>>,
    fanout_rr: DashMap<Namespace, Arc<AtomicUsize>>,
    inflight: Arc<DashMap<ServiceId, Arc<AtomicU32>>>,
}

impl PubsubRouter {
    pub fn new(tasks: Arc<dyn PubsubTaskFactory>, submit: Submitter) -> Self {
        Self {
            tasks,
            submit,
            metrics: None,
            quota: None,
            index: Mutex::new(None),
            group_rr: DashMap::new(),
            subject_rr: DashMap::new(),
            fanout_rr: DashMap::new(),
            inflight: Arc::new(DashMap::new()),
        }
    }

    pub fn with_metrics(mut self, m: Arc<rt_obs::Metrics>) -> Self {
        self.metrics = Some(m);
        self
    }

    pub fn with_quota(mut self, q: PubsubQuota) -> Self {
        self.quota = Some(q);
        self
    }

    /// Deliveries of `service` currently in flight.
    pub fn inflight(&self, service: ServiceId) -> u32 {
        self.inflight
            .get(&service)
            .map(|c| c.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    fn index(&self) -> Arc<Index> {
        let generation = self.tasks.generation();
        let mut slot = self.index.lock();
        if let Some(idx) = slot.as_ref() {
            if idx.generation == generation {
                return idx.clone();
            }
        }
        let mut by_ns: HashMap<Namespace, Vec<Entry>> = HashMap::new();
        for row in self.tasks.subscriptions() {
            by_ns.entry(row.ns).or_default().push(Entry {
                pattern: row.pattern,
                group: row.group,
                service: row.service,
            });
        }
        let idx = Arc::new(Index { generation, by_ns });
        *slot = Some(idx.clone());
        idx
    }

    fn matches(&self, ns: &Namespace, subject: &str) -> Matches {
        let idx = self.index();
        let mut m = Matches {
            ungrouped: Vec::new(),
            groups: BTreeMap::new(),
        };
        for e in idx.by_ns.get(ns).map(Vec::as_slice).unwrap_or(&[]) {
            if !subject_matches(&e.pattern, subject) {
                continue;
            }
            match &e.group {
                None => push_unique(&mut m.ungrouped, e.service),
                Some(g) => push_unique(m.groups.entry(g.clone()).or_default(), e.service),
            }
        }
        m
    }

    fn rr(map: &DashMap<(Namespace, String), Arc<AtomicUsize>>, ns: &Namespace, key: &str) -> usize {
        let c = map
            .entry((ns.clone(), key.to_string()))
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        c.fetch_add(1, Ordering::Relaxed)
    }

    fn drop_count(&self, reason: &str, n: u64) {
        if n > 0 {
            if let Some(m) = &self.metrics {
                m.pubsub_dropped_total.with_label_values(&[reason]).inc_by(n);
            }
        }
    }

    fn check_quota(&self, tenant: TenantId) -> Result<(), PubsubError> {
        match &self.quota {
            Some(q) => q(tenant),
            None => Ok(()),
        }
    }

    fn validate(depth: u8, subject: &str, body: &Bytes, headers: &[(String, String)]) -> Result<(), PubsubError> {
        if depth > MAX_CALL_DEPTH {
            return Err(PubsubError::Depth);
        }
        validate_subject(subject).map_err(PubsubError::Validation)?;
        if body.len() > EVENT_BODY_MAX_BYTES || headers_over_limits(headers) {
            return Err(PubsubError::TooLarge);
        }
        validate_headers(headers).map_err(PubsubError::Validation)
    }

    fn message(subject: &str, body: Bytes, headers: Vec<(String, String)>, replyable: bool) -> PubsubMessage {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        PubsubMessage {
            id: MsgId::new(),
            subject: subject.to_string(),
            body,
            headers,
            replyable,
            published_at_ms: now,
        }
    }

    /// The factory's task with the trigger the router guarantees.
    fn task(&self, service: ServiceId, msg: PubsubMessage, depth: u8, wall_cap: Option<Duration>) -> Option<TaskSpec> {
        let subject: Arc<str> = Arc::from(msg.subject.as_str());
        let reply = msg.replyable;
        let mut spec = self.tasks.task_for(service, msg, depth, wall_cap)?;
        spec.trigger = Trigger::Pubsub { subject, depth, reply };
        if let Some(cap) = wall_cap {
            spec.limits.wall = spec.limits.wall.min(cap);
        }
        Some(spec)
    }

    /// Publish to every matching subscription of `ns`. `depth` is the delivery depth (publisher
    /// depth + 1). Returns `(receivers, dropped)`.
    pub fn publish(
        &self,
        ns: &Namespace,
        depth: u8,
        subject: &str,
        body: Bytes,
        headers: Vec<(String, String)>,
    ) -> Result<(u32, u32), PubsubError> {
        Self::validate(depth, subject, &body, &headers)?;
        self.check_quota(ns.tenant)?;
        if let Some(m) = &self.metrics {
            m.pubsub_published_total.inc();
        }

        let m = self.matches(ns, subject);
        let mut list = m.ungrouped;
        for (g, members) in &m.groups {
            let i = Self::rr(&self.group_rr, ns, g) % members.len();
            list.push(members[i]);
        }

        let mut dropped: u32 = 0;
        if list.len() > PUBSUB_FANOUT_MAX {
            let c = self
                .fanout_rr
                .entry(ns.clone())
                .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
                .clone();
            let start = c.fetch_add(PUBSUB_FANOUT_MAX, Ordering::Relaxed) % list.len();
            let over = list.len() - PUBSUB_FANOUT_MAX;
            list = (0..PUBSUB_FANOUT_MAX).map(|k| list[(start + k) % list.len()]).collect();
            dropped += over as u32;
            self.drop_count("fanout", over as u64);
        }

        let msg = Self::message(subject, body, headers, false);
        let mut receivers: u32 = 0;
        for service in list {
            if self.check_quota(ns.tenant).is_err() {
                dropped += 1;
                self.drop_count("quota", 1);
                continue;
            }
            let counter = self
                .inflight
                .entry(service)
                .or_insert_with(|| Arc::new(AtomicU32::new(0)))
                .clone();
            if counter.load(Ordering::Acquire) >= PUBSUB_INFLIGHT_PER_SERVICE {
                dropped += 1;
                self.drop_count("backlog", 1);
                continue;
            }
            let mut copy = msg.clone();
            copy.id = MsgId::new();
            let Some(spec) = self.task(service, copy, depth, None) else {
                dropped += 1;
                self.drop_count("no_task", 1);
                continue;
            };
            match (self.submit)(spec) {
                Ok(fut) => {
                    receivers += 1;
                    if let Some(m) = &self.metrics {
                        m.pubsub_delivered_total.inc();
                    }
                    counter.fetch_add(1, Ordering::AcqRel);
                    let inflight = self.inflight.clone();
                    tokio::spawn(async move {
                        let _ = fut.await;
                        if counter.fetch_sub(1, Ordering::AcqRel) == 1 {
                            inflight.remove_if(&service, |_, c| c.load(Ordering::Acquire) == 0);
                        }
                    });
                }
                Err(_) => {
                    dropped += 1;
                    self.drop_count("rejected", 1);
                }
            }
        }
        Ok((receivers, dropped))
    }

    /// Send to one responder and wait for its reply: `(content-type, body)`.
    pub async fn request(
        &self,
        ns: &Namespace,
        depth: u8,
        subject: &str,
        body: Bytes,
        headers: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<(Option<String>, Bytes), PubsubError> {
        Self::validate(depth, subject, &body, &headers)?;
        self.check_quota(ns.tenant)?;
        if let Some(m) = &self.metrics {
            m.pubsub_published_total.inc();
        }
        let m = self.matches(ns, subject);
        let mut list = m.ungrouped;
        for members in m.groups.values() {
            for s in members {
                push_unique(&mut list, *s);
            }
        }
        if list.is_empty() {
            return Err(PubsubError::NoResponders);
        }
        let service = list[Self::rr(&self.subject_rr, ns, subject) % list.len()];
        self.check_quota(ns.tenant)?;
        let msg = Self::message(subject, body, headers, true);
        let spec = self
            .task(service, msg, depth, Some(timeout))
            .ok_or(PubsubError::NoResponders)?;
        let fut = (self.submit)(spec).map_err(PubsubError::Rejected)?;
        if let Some(m) = &self.metrics {
            m.pubsub_delivered_total.inc();
        }
        let outcome = tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| PubsubError::Timeout)?;
        match outcome {
            Outcome::Ok(reply) if reply.status.is_success() => {
                let ct = reply
                    .headers
                    .get(http::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let mut buf = BytesMut::new();
                let mut stream = reply.body.into_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|_| PubsubError::ResponderFailed)?;
                    if buf.len() + chunk.len() > EVENT_BODY_MAX_BYTES {
                        return Err(PubsubError::TooLarge);
                    }
                    buf.extend_from_slice(&chunk);
                }
                Ok((ct, buf.freeze()))
            }
            Outcome::Rejected(r) => Err(PubsubError::Rejected(r)),
            _ => Err(PubsubError::ResponderFailed),
        }
    }
}
