//! Builders for deterministic IDs and `TaskSpec`s in tests.

use std::time::Duration;

use rt_types::{
    Digest, HttpRequest, Limits, Payload, PreparedKey, RuntimeKind, ServiceId, ServiceKnobs, ServiceRef, TaskKind,
    TaskSpec, TenantId, Trigger, VersionNo,
};

/// Deterministic IDs: `tenant(1)` is always the same ID (simulation reproducibility).
pub mod ids {
    use super::*;

    pub fn tenant(n: u128) -> TenantId {
        TenantId::from_u128(n)
    }

    pub fn service(n: u128) -> ServiceId {
        ServiceId::from_u128(n)
    }

    /// Digest of the bytes `"code-<n>"` (distinct per `n`).
    pub fn digest(n: u64) -> Digest {
        Digest::of(format!("code-{n}").as_bytes())
    }
}

/// `ServiceRef` for tenant `t`, service `s`, version 1, code `s`.
pub fn service_ref(t: u128, s: u128) -> ServiceRef {
    ServiceRef {
        tenant: ids::tenant(t),
        service: ids::service(s),
        version: VersionNo(1),
        digest: ids::digest(s as u64),
    }
}

/// Fluent `TaskSpec` builder.
#[derive(Debug)]
pub struct SpecBuilder {
    kind: TaskKind,
    service: ServiceRef,
    trigger: Trigger,
    limits: Limits,
    knobs: ServiceKnobs,
    payload: Payload,
}

impl SpecBuilder {
    pub fn new(kind: TaskKind) -> Self {
        let trigger = if kind == TaskKind::Build {
            Trigger::Build {
                key: PreparedKey {
                    digest: ids::digest(0),
                    kind: RuntimeKind::Deno,
                    engine_fp: 0,
                },
            }
        } else {
            Trigger::Http
        };
        Self {
            kind,
            service: service_ref(1, 1),
            limits: Limits::defaults_for(&trigger),
            trigger,
            knobs: ServiceKnobs::default(),
            payload: Payload::None,
        }
    }

    pub fn deno() -> Self {
        Self::new(TaskKind::DENO)
    }

    pub fn wasm() -> Self {
        Self::new(TaskKind::WASM)
    }

    pub fn build() -> Self {
        Self::new(TaskKind::Build)
    }

    pub fn tenant(mut self, n: u128) -> Self {
        self.service.tenant = ids::tenant(n);
        self
    }

    /// Sets the service ID and (so different services get different code) its digest.
    pub fn service(mut self, n: u128) -> Self {
        self.service.service = ids::service(n);
        self.service.digest = ids::digest(n as u64);
        self
    }

    pub fn service_ref(mut self, r: ServiceRef) -> Self {
        self.service = r;
        self
    }

    pub fn trigger(mut self, t: Trigger) -> Self {
        self.trigger = t;
        self
    }

    pub fn limits(mut self, l: Limits) -> Self {
        self.limits = l;
        self
    }

    pub fn cpu_limit(mut self, ms: u64) -> Self {
        self.limits.cpu = Duration::from_millis(ms);
        self
    }

    pub fn wall_limit(mut self, ms: u64) -> Self {
        self.limits.wall = Duration::from_millis(ms);
        self
    }

    pub fn memory_mb(mut self, mb: u32) -> Self {
        self.limits.memory_mb = mb;
        self
    }

    pub fn knobs(mut self, k: ServiceKnobs) -> Self {
        self.knobs = k;
        self
    }

    /// A fixed service concurrency limit: the ceiling (`scaling.max_instances`) with the
    /// adaptive limit pinned to it (`start = min = max_instances`, D-A3), so scheduler tests
    /// are not steered by the autoscaler. `max_queued` is the explicit queue bound (0 = node share).
    pub fn max_concurrency(mut self, max_instances: u32, max_queued: u32) -> Self {
        self.knobs.max_instances = max_instances;
        self.knobs.max_queued = max_queued;
        self.knobs.scaling.start = max_instances;
        self.knobs.scaling.min = max_instances;
        self
    }

    /// The service's instance bounds (D-B2): `min_instances` stay warm and floor the limit,
    /// `max_instances` caps it. The adaptive limit's start and floor are left alone.
    pub fn instances(mut self, min_instances: u32, max_instances: u32) -> Self {
        self.knobs.min_instances = min_instances;
        self.knobs.max_instances = max_instances;
        self
    }

    /// Autoscaling bounds (`max_instances` stays the ceiling).
    pub fn scaling(mut self, scaling: rt_types::ScalingKnobs) -> Self {
        self.knobs.scaling = scaling;
        self
    }

    pub fn queue_timeout_ms(mut self, ms: u64) -> Self {
        self.knobs.queue_timeout = Duration::from_millis(ms);
        self
    }

    pub fn http_get(mut self, path: &str) -> Self {
        self.payload = Payload::Http(HttpRequest::get(path));
        self
    }

    pub fn payload(mut self, p: Payload) -> Self {
        self.payload = p;
        self
    }

    pub fn finish(self) -> TaskSpec {
        TaskSpec {
            kind: self.kind,
            service: self.service,
            trigger: self.trigger,
            limits: self.limits,
            knobs: self.knobs,
            payload: self.payload,
            trace: None,
            slug: "".into(),
            caller: None,
            object: None,
            reentrant: false,
        }
    }
}
