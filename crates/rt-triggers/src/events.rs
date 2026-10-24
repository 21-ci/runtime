//! Event dispatcher: the host's `std:pubsub` and `std:stream` buses.
//!
//! [`EventDispatcher`] implements [`rt_host::PubsubBus`] and [`rt_host::StreamBus`]. The caller's
//! namespace (tenant/project/environment) always comes from the route table entry of the calling
//! service, never from the guest; a service missing from the table, or whose entry belongs to
//! another tenant, gets `unavailable`. Topics are looked up by name in that namespace
//! (`not_found` otherwise). Publishes run at `child_depth(from.depth)`; each append takes one
//! tenant quota check and passes the tenant's `max_stream_mb` cap to the store. Errors carry
//! stable codes only (store errors are sanitized by [`HostError::from_store`]).

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use rt_host::events::{
    AppendEntry, AppendedOut, PublishOpts, PublishOut, PubsubBus, ReadOpts, ReadPage, ReadStart, Reply, RequestOpts,
    StreamBus, TopicInfo,
};
use rt_host::{HostError, TaskCtx};
use rt_sec::PlanSource;
use rt_store::stream::{NewRecord, ReadFrom, StreamStore, Topic};
use rt_store::StoreError;
use rt_types::events::{Namespace, READ_MAX_BYTES, REQUEST_TIMEOUT_DEFAULT_MS, REQUEST_TIMEOUT_MAX_MS};
use rt_types::{TenantId, TopicId};

use crate::calls::child_depth;
use crate::pubsub::{PubsubError, PubsubQuota, PubsubRouter};
use crate::routes::RouteTable;

const MIB: u64 = 1024 * 1024;
const HOUR_MS: u64 = 3_600_000;

/// Called after a successful append (wakes the stream engine for that topic).
pub type StreamNotify = Arc<dyn Fn(TopicId) + Send + Sync>;

/// Implements the host pub/sub and stream buses on top of the router and the stream store.
pub struct EventDispatcher {
    routes: Arc<RouteTable>,
    pubsub: Arc<PubsubRouter>,
    streams: Arc<dyn StreamStore>,
    plans: Arc<dyn PlanSource>,
    notify_stream: StreamNotify,
    quota: Option<PubsubQuota>,
}

fn pubsub_err(e: PubsubError) -> HostError {
    match e {
        PubsubError::Validation(m) => HostError::new("validation_failed", m),
        other => HostError::new(other.code(), other.code().replace('_', " ")),
    }
}

fn store_err(e: StoreError) -> HostError {
    match e {
        StoreError::Conflict("stream_full") => HostError::new("stream_full", "the tenant's streams are full"),
        other => HostError::from_store(&other),
    }
}

impl EventDispatcher {
    pub fn new(
        routes: Arc<RouteTable>,
        pubsub: Arc<PubsubRouter>,
        streams: Arc<dyn StreamStore>,
        plans: Arc<dyn PlanSource>,
        notify_stream: StreamNotify,
    ) -> Self {
        Self {
            routes,
            pubsub,
            streams,
            plans,
            notify_stream,
            quota: None,
        }
    }

    /// Tenant quota check taken once per append (`rate_limited` / `quota_exceeded`).
    pub fn with_quota(mut self, q: PubsubQuota) -> Self {
        self.quota = Some(q);
        self
    }

    /// The caller's namespace from the route table (tenant must match the task's tenant).
    pub fn namespace(&self, from: &TaskCtx) -> Result<Namespace, HostError> {
        match self.routes.by_service(from.service) {
            Some(e) if e.tenant == from.tenant => Ok(Namespace {
                tenant: e.tenant,
                project: e.project,
                environment: e.environment.clone(),
            }),
            _ => Err(HostError::new("unavailable", "the service is not routable")),
        }
    }

    fn depth(from: &TaskCtx) -> Result<u8, HostError> {
        child_depth(from.depth).map_err(|_| HostError::new("call_depth", "call depth exceeded"))
    }

    async fn topic(&self, from: &TaskCtx, name: &str) -> Result<Topic, HostError> {
        let ns = self.namespace(from)?;
        self.streams
            .get_topic(&ns, name)
            .await
            .map_err(store_err)?
            .ok_or_else(|| HostError::new("not_found", format!("topic `{name}` not found")))
    }

    fn check_quota(&self, tenant: TenantId) -> Result<(), HostError> {
        match &self.quota {
            Some(q) => q(tenant).map_err(pubsub_err),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl PubsubBus for EventDispatcher {
    async fn publish(
        &self,
        from: &TaskCtx,
        subject: &str,
        body: Bytes,
        o: PublishOpts,
    ) -> Result<PublishOut, HostError> {
        let ns = self.namespace(from)?;
        let depth = Self::depth(from)?;
        let (receivers, dropped) = self
            .pubsub
            .publish(&ns, depth, subject, body, o.headers)
            .map_err(pubsub_err)?;
        Ok(PublishOut { receivers, dropped })
    }

    async fn request(&self, from: &TaskCtx, subject: &str, body: Bytes, o: RequestOpts) -> Result<Reply, HostError> {
        let ns = self.namespace(from)?;
        let depth = Self::depth(from)?;
        let max = Duration::from_millis(REQUEST_TIMEOUT_MAX_MS);
        let mut timeout = o
            .timeout
            .unwrap_or(Duration::from_millis(REQUEST_TIMEOUT_DEFAULT_MS))
            .min(max);
        timeout = timeout.min(from.deadline.saturating_duration_since(Instant::now()));
        if timeout.is_zero() {
            return Err(HostError::new("timeout", "timeout"));
        }
        let (content_type, body) = self
            .pubsub
            .request(&ns, depth, subject, body, o.headers, timeout)
            .await
            .map_err(pubsub_err)?;
        Ok(Reply { content_type, body })
    }
}

#[async_trait]
impl StreamBus for EventDispatcher {
    async fn append(
        &self,
        from: &TaskCtx,
        topic: &str,
        entries: Vec<AppendEntry>,
    ) -> Result<Vec<AppendedOut>, HostError> {
        let t = self.topic(from, topic).await?;
        self.check_quota(t.ns.tenant)?;
        let cap = self.plans.plan_arc(t.ns.tenant).max_stream_mb.saturating_mul(MIB);
        let records = entries
            .into_iter()
            .map(|e| NewRecord {
                key: e.key,
                headers: e.headers,
                body: e.body,
                dedup_id: e.dedup_id,
            })
            .collect();
        let out = self.streams.append(t.id, records, cap).await.map_err(store_err)?;
        (self.notify_stream)(t.id);
        Ok(out
            .into_iter()
            .map(|a| AppendedOut {
                partition: a.partition,
                offset: a.offset,
                duplicate: a.duplicate,
            })
            .collect())
    }

    async fn read(&self, from: &TaskCtx, topic: &str, o: ReadOpts) -> Result<ReadPage, HostError> {
        let t = self.topic(from, topic).await?;
        let partition = match o.partition {
            Some(p) if p >= t.partitions => {
                return Err(HostError::new(
                    "validation_failed",
                    format!(
                        "partition is out of range (topic `{}` has {} partitions)",
                        t.name, t.partitions
                    ),
                ))
            }
            Some(p) => p,
            None if t.partitions > 1 => {
                return Err(HostError::new(
                    "validation_failed",
                    format!("topic `{}` has {} partitions: pass `partition`", t.name, t.partitions),
                ))
            }
            None => 0,
        };
        let from_pos = match o.start {
            ReadStart::Earliest => ReadFrom::Earliest,
            ReadStart::Latest => ReadFrom::Latest,
            ReadStart::Offset(n) => ReadFrom::Offset(n),
            ReadStart::TimeMs(ms) => ReadFrom::Time(ms),
        };
        let records = self
            .streams
            .read(t.id, partition, from_pos, o.limit as usize, READ_MAX_BYTES)
            .await
            .map_err(store_err)?;
        let heads = self.streams.heads(t.id).await.map_err(store_err)?;
        let end = heads.iter().find(|h| h.partition == partition).map_or(0, |h| h.next);
        let next = match records.last() {
            Some(r) => r.offset + 1,
            None => match o.start {
                ReadStart::Offset(n) => n.min(end),
                _ => end,
            },
        };
        Ok(ReadPage { records, next, end })
    }

    async fn info(&self, from: &TaskCtx, topic: &str) -> Result<TopicInfo, HostError> {
        let t = self.topic(from, topic).await?;
        let heads = self.streams.heads(t.id).await.map_err(store_err)?;
        Ok(TopicInfo {
            name: t.name,
            partitions: heads.into_iter().map(|h| (h.partition, h.first, h.next)).collect(),
            retention_hours: (t.retention_ms / HOUR_MS) as u32,
            max_mb: t.max_bytes / MIB,
            bytes: t.bytes,
        })
    }
}
