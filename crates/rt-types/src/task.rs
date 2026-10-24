//! `TaskSpec` and its parts (rt-types).

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::body::Body;
use crate::digest::Digest;
use crate::ids::{MsgId, ProjectId, ScheduleId, ServiceId, TaskId, TenantId, VersionNo};
use crate::kinds::{RuntimeKind, TaskKind};
use crate::limits::{Limits, ServiceKnobs};
use crate::sockets::{ObjectRef, SocketEvent};

/// Immutable reference to one deployed version of a service.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceRef {
    pub tenant: TenantId,
    pub service: ServiceId,
    pub version: VersionNo,
    /// Digest of the source artifact (T3).
    pub digest: Digest,
}

/// Key of a prepared (compiled) artifact in the tiered cache:
/// `(artifact_digest, kind, engine_fp)`. Identical code shares entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PreparedKey {
    pub digest: Digest,
    pub kind: RuntimeKind,
    /// Fingerprint of the engine configuration (V8 version+flags / wasmtime version+config).
    pub engine_fp: u64,
}

impl PreparedKey {
    /// 64-bit hash of the key, used by the frequency sketch and sharding.
    pub fn hash64(&self) -> u64 {
        // FNV-1a over digest bytes + kind + engine_fp: stable across runs (persisted sketches).
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut feed = |b: u8| {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        };
        self.digest.0.iter().for_each(|b| feed(*b));
        feed(self.kind as u8);
        self.engine_fp.to_le_bytes().iter().for_each(|b| feed(*b));
        h
    }
}

/// What caused a task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Trigger {
    Http,
    Interval {
        id: ScheduleId,
    },
    Cron {
        id: ScheduleId,
    },
    /// A one-off schedule created by guest code (`std:schedule`); `id` is the schedule's id.
    Scheduled {
        id: String,
    },
    Queue {
        batch: Vec<MsgId>,
        attempt: u16,
    },
    Call {
        parent: TaskId,
        depth: u8,
    },
    Build {
        key: PreparedKey,
    },
    /// A WebSocket event (message, close, error) of a socket of the object `object`
    /// (`webSocketMessage` / `webSocketClose` / `webSocketError`).
    Socket {
        object: Arc<str>,
    },
    /// A durable object's alarm (`alarm()`).
    Alarm {
        object: Arc<str>,
    },
}

impl Trigger {
    /// Label used in metrics, logs and `ctx.trigger`.
    pub const fn label(&self) -> &'static str {
        match self {
            Trigger::Http => "http",
            Trigger::Interval { .. } => "interval",
            Trigger::Cron { .. } => "cron",
            Trigger::Scheduled { .. } => "schedule",
            Trigger::Queue { .. } => "queue",
            Trigger::Call { .. } => "call",
            Trigger::Build { .. } => "build",
            Trigger::Socket { .. } => "websocket",
            Trigger::Alarm { .. } => "alarm",
        }
    }

    /// Call depth: 0 unless this is a service-to-service call.
    pub const fn call_depth(&self) -> u8 {
        match self {
            Trigger::Call { depth, .. } => *depth,
            _ => 0,
        }
    }
}

/// W3C trace context carried with a task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceCtx {
    /// `traceparent` header value.
    pub traceparent: String,
    pub sampled: bool,
}

/// An HTTP request delivered to a guest (ingress, internal listener, or service call).
#[derive(Debug)]
pub struct HttpRequest {
    pub method: http::Method,
    /// Path + query as the *handler* sees it (after `/s/{slug}` stripping).
    pub uri: http::Uri,
    /// Request headers after hygiene (no `x-rt-*` from the public listener, no invoke credential).
    pub headers: http::HeaderMap,
    pub remote_addr: Option<SocketAddr>,
    pub body: Body,
}

impl HttpRequest {
    /// `GET <uri>` with no headers and an empty body.
    pub fn get(uri: &str) -> Self {
        Self {
            method: http::Method::GET,
            uri: uri.parse().unwrap_or_else(|_| http::Uri::from_static("/")),
            headers: http::HeaderMap::new(),
            remote_addr: None,
            body: Body::empty(),
        }
    }
}

/// Payload of an interval/cron fire (`event.payload`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScheduledEvent {
    pub schedule_id: ScheduleId,
    /// Scheduled (not actual) time, Unix ms.
    pub scheduled_time_ms: i64,
    pub payload: serde_json::Value,
}

/// Payload of a one-off schedule fire (`event.id`, `event.scheduledAt`, `event.payload`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DelayedEvent {
    pub id: String,
    /// The time the schedule was due (its `runAt`), Unix ms.
    pub scheduled_at_ms: i64,
    pub payload: serde_json::Value,
}

/// One queue message as delivered to a handler.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMessage {
    pub id: MsgId,
    pub body: Bytes,
    pub attempt: u16,
}

/// Task input.
#[derive(Debug, Default)]
pub enum Payload {
    /// Builds, and triggers without data.
    #[default]
    None,
    Http(HttpRequest),
    Schedule(ScheduledEvent),
    Delayed(DelayedEvent),
    Queue(Vec<QueueMessage>),
    /// A WebSocket event or a durable object's alarm (`Trigger::Socket` / `Trigger::Alarm`).
    Socket(SocketEvent),
}

/// Who made an in-process call, as the runtime stamped it: the callee's `ctx.caller`. Set by the
/// call dispatcher from its own routes, never from the request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallerStamp {
    pub service: ServiceId,
    /// The caller's tenant.
    pub tenant: TenantId,
    /// The caller's slug; `None` when the caller is not visible from the callee's tenant.
    pub slug: Option<String>,
    pub project: Option<ProjectId>,
    pub environment: Option<String>,
    /// An operator SDK call: the caller tenant's plan.
    pub plan: Option<String>,
    /// An operator SDK call: the SDK and the release that runs it.
    pub sdk: Option<SdkStamp>,
}

/// The SDK release an SDK call is bound to (`ctx.caller.sdk`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SdkStamp {
    pub name: String,
    pub version: String,
}

/// Everything the scheduler and executors need to run one task.
#[derive(Debug)]
pub struct TaskSpec {
    pub kind: TaskKind,
    pub service: ServiceRef,
    pub trigger: Trigger,
    pub limits: Limits,
    pub knobs: ServiceKnobs,
    pub payload: Payload,
    pub trace: Option<TraceCtx>,
    /// The service's slug (`ctx.service.slug`); empty when the caller does not know it.
    pub slug: Arc<str>,
    /// For a service-to-service call: who called (`ctx.caller`).
    pub caller: Option<CallerStamp>,
    /// The object this task acts for: a socket event or alarm, a request to a durable object, or
    /// the default object of a durable service. `None` for every other task.
    pub object: Option<ObjectRef>,
}

impl TaskSpec {
    /// A spec with default limits/knobs and no payload.
    pub fn new(kind: TaskKind, service: ServiceRef, trigger: Trigger) -> Self {
        let limits = Limits::defaults_for(&trigger);
        Self {
            kind,
            service,
            trigger,
            limits,
            knobs: ServiceKnobs::default(),
            payload: Payload::None,
            trace: None,
            slug: Arc::from(""),
            caller: None,
            object: None,
        }
    }

    /// HTTP task for a guest runtime.
    pub fn http(runtime: RuntimeKind, service: ServiceRef, req: HttpRequest) -> Self {
        let mut s = Self::new(TaskKind::Exec(runtime), service, Trigger::Http);
        s.payload = Payload::Http(req);
        s
    }

    /// Build task producing the prepared artifact `key` for `service`.
    pub fn build(service: ServiceRef, key: PreparedKey) -> Self {
        Self::new(TaskKind::Build, service, Trigger::Build { key })
    }

    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_knobs(mut self, knobs: ServiceKnobs) -> Self {
        self.knobs = knobs;
        self
    }

    pub fn with_payload(mut self, payload: Payload) -> Self {
        self.payload = payload;
        self
    }

    pub fn with_trace(mut self, trace: TraceCtx) -> Self {
        self.trace = Some(trace);
        self
    }

    pub fn with_object(mut self, object: ObjectRef) -> Self {
        self.object = Some(object);
        self
    }

    pub fn with_slug(mut self, slug: impl Into<Arc<str>>) -> Self {
        self.slug = slug.into();
        self
    }

    /// The prepared-artifact key this task needs in T1 (`None` for builds).
    pub fn prepared_key(&self, engine_fp: u64) -> Option<PreparedKey> {
        match self.kind {
            TaskKind::Exec(kind) => Some(PreparedKey {
                digest: self.service.digest,
                kind,
                engine_fp,
            }),
            TaskKind::Build => None,
        }
    }
}
