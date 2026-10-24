//! Effective-value resolution with provenance (docs/config.md):
//!
//! ```text
//! plan maximum  ≥  service explicit  >  service profile  >  node default profile  >  built-in
//! ```

use std::time::Duration;

use rt_types::{CompileMode, Limits, Plan, Profile, ServiceConfig, ServiceKnobs, Trigger};
use serde::{Serialize, Serializer};

use crate::profile;
use crate::Config;

/// Where an effective value came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// Explicit service config.
    Service,
    /// Explicit node config file / env (`rt.toml`, `RT__…`).
    Config,
    /// A profile's table value.
    Profile(Profile),
    /// The plan's maximum clamped the value.
    Plan,
    /// Built-in default.
    Default,
}

impl Source {
    /// `service`, `config`, `profile:<name>`, `plan`, `default`.
    pub fn label(&self) -> String {
        match self {
            Source::Service => "service".into(),
            Source::Config => "config".into(),
            Source::Profile(p) => format!("profile:{p}"),
            Source::Plan => "plan".into(),
            Source::Default => "default".into(),
        }
    }
}

impl Serialize for Source {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.label())
    }
}

/// A value with its provenance: `{"value": 1000, "source": "profile:balanced"}`.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct Resolved<T> {
    pub value: T,
    pub source: Source,
}

impl<T> Resolved<T> {
    pub fn new(value: T, source: Source) -> Self {
        Self { value, source }
    }
}

/// Resolved node-scope knobs (explicit config > node default profile).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NodeKnobs {
    pub spin_us: Resolved<u64>,
    pub time_slice: Resolved<Duration>,
    pub pin_threads: Resolved<bool>,
    pub handoff_on_hog: Resolved<bool>,
    pub sysmon_max_tick: Resolved<Duration>,
    pub rebalance_interval: Resolved<Duration>,
    pub deno_max_bound_per_m: Resolved<u32>,
    pub deno_isolate_pool_per_p: Resolved<u32>,
    pub memory_high: Resolved<f64>,
    pub memory_critical: Resolved<f64>,
    pub memory_purge_min_interval: Resolved<Duration>,
    pub cache_t1_share: Resolved<f64>,
    /// Bytes.
    pub wasm_linear_memory_keep_resident: Resolved<u64>,
    pub wasm_max_unused_warm_slots: Resolved<u32>,
    /// Share of the memory budget warm instances may use (`sched.warm_share`, D-A3).
    pub warm_share: Resolved<f64>,
}

/// Bytes per MB as used by the `wasm.linear_memory_keep_resident` file unit.
pub(crate) const MIB: u64 = 1024 * 1024;

fn pick_knob_p<T>(p: Profile, explicit: Option<T>, from_profile: T) -> Resolved<T> {
    match explicit {
        Some(v) => Resolved::new(v, Source::Config),
        None => Resolved::new(from_profile, Source::Profile(p)),
    }
}

/// Resolve node-scope knobs from `cfg` (explicit values win; the rest come from
/// `cfg.node.default_profile`).
pub fn resolve_node(cfg: &Config) -> NodeKnobs {
    let p = cfg.node.default_profile;
    let k = profile::node_knobs(p);
    NodeKnobs {
        spin_us: pick_knob_p(p, cfg.sched.spin_us, k.spin_us),
        time_slice: pick_knob_p(p, cfg.sched.time_slice, k.time_slice),
        pin_threads: pick_knob_p(p, cfg.sched.pin_threads, k.pin_threads),
        handoff_on_hog: pick_knob_p(p, cfg.sched.handoff_on_hog, k.handoff_on_hog),
        sysmon_max_tick: pick_knob_p(p, cfg.sysmon.max_tick, k.sysmon_max_tick),
        rebalance_interval: pick_knob_p(p, cfg.rebalance.interval, k.rebalance_interval),
        deno_max_bound_per_m: pick_knob_p(p, cfg.deno.max_bound_per_m, k.deno_max_bound_per_m),
        deno_isolate_pool_per_p: pick_knob_p(p, cfg.deno.isolate_pool_per_p, k.deno_isolate_pool_per_p),
        memory_high: pick_knob_p(p, cfg.memory.high, k.memory_high),
        memory_critical: pick_knob_p(p, cfg.memory.critical, k.memory_critical),
        memory_purge_min_interval: pick_knob_p(p, cfg.memory.purge_min_interval, k.memory_purge_min_interval),
        cache_t1_share: pick_knob_p(p, cfg.cache.t1_share, k.cache_t1_share),
        // the config file states this knob in MB (config/schema.json); the resolved value is bytes
        wasm_linear_memory_keep_resident: pick_knob_p(
            p,
            cfg.wasm.linear_memory_keep_resident.map(|mb| mb.saturating_mul(MIB)),
            k.wasm_linear_memory_keep_resident,
        ),
        wasm_max_unused_warm_slots: pick_knob_p(p, cfg.wasm.max_unused_warm_slots, k.wasm_max_unused_warm_slots),
        warm_share: pick_knob_p(p, cfg.sched.warm_share, profile::warm_share(p)),
    }
}

/// Recorded when a plan was lowered below a service's explicit value (CFG-06).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PlanConflict {
    pub field: &'static str,
    pub explicit: u64,
    pub plan_max: u64,
}

/// `effective` block of `GET /v1/services/{s}`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EffectiveService {
    pub wall_ms: Resolved<u64>,
    pub cpu_ms: Resolved<u64>,
    pub memory_mb: Resolved<u32>,
    pub max_body_bytes: Resolved<u64>,
    /// Instances kept warm (D-B2): explicit `scaling.min_instances`, at most the plan's
    /// `max_instances` and the effective `max_instances`; default 0 (scale to zero).
    pub min_instances: Resolved<u32>,
    /// Ceiling of the adaptive concurrency limit (D-A3, D-B2): explicit `scaling.max_instances`
    /// (hard cap, clamped to the plan's `max_instances`) > the plan's `max_instances` and the
    /// service's profile ceiling, whichever is lower.
    pub max_instances: Resolved<u32>,
    pub max_queued: Resolved<u32>,
    /// Initial adaptive concurrency limit (profile, within `[min_instances, max_instances]`).
    pub concurrency_start: Resolved<u32>,
    /// Floor of the adaptive concurrency limit (profile, within `[min_instances, max_instances]`).
    pub concurrency_min: Resolved<u32>,
    pub keep_resident: Resolved<bool>,
    pub queue_timeout: Resolved<Duration>,
    pub idle_ttl: Resolved<Duration>,
    pub preload: Resolved<bool>,
    pub compile: Resolved<CompileMode>,
    pub reuse_instances: Resolved<bool>,
    /// `deno.reuse_isolates` (D-A4): the service's `config.deno` > node config > the service's
    /// profile.
    pub reuse_isolates: Resolved<bool>,
    /// Explicit values that exceeded the plan at runtime (plan wins; event `plan_conflict`).
    #[serde(skip)]
    pub plan_conflicts: Vec<PlanConflict>,
    /// What [`EffectiveService::limits_for`] needs to keep the per-trigger defaults.
    #[serde(skip)]
    trigger_defaults: TriggerDefaults,
    /// Autoscaling knobs of the service's profile, `start` / `min` already capped.
    #[serde(skip)]
    scaling: rt_types::ScalingKnobs,
}

/// Which limits the service set explicitly, and the plan ceilings that cap the per-trigger
/// defaults (interval/cron/queue tasks default to a longer wall and CPU than HTTP).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct TriggerDefaults {
    explicit_wall: bool,
    explicit_cpu: bool,
    explicit_body: bool,
    plan_wall_ms: u64,
    plan_cpu_ms: u64,
}

/// Resolve the effective values of one service.
///
/// `service_profile` is the service's own profile (`None` = node default). Explicit service
/// values are clamped to the plan maximum (source becomes `plan`, conflict recorded).
pub fn resolve_service(
    cfg: &Config,
    service_profile: Option<Profile>,
    service: &ServiceConfig,
    plan: &Plan,
) -> EffectiveService {
    let p = service_profile.unwrap_or(cfg.node.default_profile);
    let sk = profile::service_knobs(p);
    let defaults = Limits::default();
    let mut conflicts = Vec::new();
    // D-A3, D-B2: an explicit `scaling.max_instances` is the owner's hard cap, at most the plan's
    // `max_instances` (0 = no plan cap: the node default). Without it the plan's limit and the
    // profile's ceiling bound the adaptive limit, whichever is lower. `min_instances` (default 0)
    // is kept warm; it is at most the plan's cap and the effective max. Both bound the limit.
    let scale = profile::scaling_knobs(p);
    let plan_cap = plan.max_instances;
    let max_instances = match service.scaling.max_instances {
        // 0 = not set (as the plan's 0 = no cap), never a limit of nothing
        Some(v) if v > 0 => {
            if plan_cap > 0 && v > plan_cap {
                conflicts.push(PlanConflict {
                    field: "max_instances",
                    explicit: u64::from(v),
                    plan_max: u64::from(plan_cap),
                });
                Resolved::new(plan_cap, Source::Plan)
            } else {
                Resolved::new(v, Source::Service)
            }
        }
        _ if plan_cap > 0 && plan_cap < scale.max_instances => Resolved::new(plan_cap, Source::Plan),
        _ => Resolved::new(scale.max_instances, Source::Profile(p)),
    };
    let min_instances = match service.scaling.min_instances {
        Some(v) if v > 0 => {
            let (v, source) = if plan_cap > 0 && v > plan_cap {
                conflicts.push(PlanConflict {
                    field: "min_instances",
                    explicit: u64::from(v),
                    plan_max: u64::from(plan_cap),
                });
                (plan_cap, Source::Plan)
            } else {
                (v, Source::Service)
            };
            // a contradicting pair is refused by the API; here the floor never exceeds the cap
            Resolved::new(v.min(max_instances.value), source)
        }
        _ => Resolved::new(0, Source::Default),
    };
    let mut scaling = scale.scaling;
    scaling.start = scaling.start.min(max_instances.value).max(min_instances.value);
    scaling.min = scaling.min.min(scaling.start).max(min_instances.value);

    let mut clamp = |field: &'static str, explicit: Option<u64>, plan_max: u64, default: u64| -> Resolved<u64> {
        match explicit {
            Some(v) if v > plan_max => {
                conflicts.push(PlanConflict {
                    field,
                    explicit: v,
                    plan_max,
                });
                Resolved::new(plan_max, Source::Plan)
            }
            Some(v) => Resolved::new(v, Source::Service),
            None => {
                let d = default.min(plan_max);
                Resolved::new(d, if d < default { Source::Plan } else { Source::Default })
            }
        }
    };

    let wall_ms = clamp(
        "wall_ms",
        service.limits.wall_ms,
        plan.limits_max.wall_ms,
        defaults.wall.as_millis() as u64,
    );
    let cpu_ms = clamp(
        "cpu_ms",
        service.limits.cpu_ms,
        plan.limits_max.cpu_ms,
        defaults.cpu.as_millis() as u64,
    );
    let memory = clamp(
        "memory_mb",
        service.limits.memory_mb.map(u64::from),
        u64::from(plan.limits_max.memory_mb),
        u64::from(defaults.memory_mb),
    );

    EffectiveService {
        wall_ms,
        cpu_ms,
        memory_mb: Resolved::new(memory.value as u32, memory.source),
        max_body_bytes: match service.limits.max_body_bytes {
            Some(v) => Resolved::new(v, Source::Service),
            None => Resolved::new(defaults.max_request_body_bytes, Source::Default),
        },
        min_instances,
        max_instances,
        max_queued: match service.scaling.max_queued {
            Some(v) => Resolved::new(v, Source::Service),
            None => Resolved::new(ServiceKnobs::default().max_queued, Source::Default),
        },
        concurrency_start: Resolved::new(scaling.start, Source::Profile(p)),
        concurrency_min: Resolved::new(scaling.min, Source::Profile(p)),
        keep_resident: match service.scaling.keep_resident {
            Some(v) => Resolved::new(v, Source::Service),
            None => Resolved::new(sk.scaling_keep_resident, Source::Profile(p)),
        },
        queue_timeout: pick_knob_p(p, cfg.sched.queue_timeout, sk.queue_timeout),
        idle_ttl: pick_knob_p(p, cfg.cache.idle_ttl, sk.cache_idle_ttl),
        preload: pick_knob_p(p, cfg.cache.preload, sk.cache_preload),
        compile: pick_knob_p(p, cfg.build.compile, sk.build_compile),
        // `config.wasm.reuse_instances` (service) > node config > the service's profile
        reuse_instances: match service.wasm.reuse_instances {
            Some(v) => Resolved::new(v, Source::Service),
            None => pick_knob_p(p, cfg.wasm.reuse_instances, sk.wasm_reuse_instances),
        },
        // `config.deno.reuse_isolates` (service) > node config > the service's profile (D-A4)
        reuse_isolates: match service.deno.reuse_isolates {
            Some(v) => Resolved::new(v, Source::Service),
            None => pick_knob_p(p, cfg.deno.reuse_isolates, sk.deno_reuse_isolates),
        },
        plan_conflicts: conflicts,
        trigger_defaults: TriggerDefaults {
            explicit_wall: service.limits.wall_ms.is_some(),
            explicit_cpu: service.limits.cpu_ms.is_some(),
            explicit_body: service.limits.max_body_bytes.is_some(),
            plan_wall_ms: plan.limits_max.wall_ms,
            plan_cpu_ms: plan.limits_max.cpu_ms,
        },
        scaling,
    }
}

impl EffectiveService {
    /// Task limits for `trigger`: the HTTP/interval/queue defaults, with this service's
    /// resolved memory and its explicit wall/cpu/body values applied.
    ///
    /// A wall/cpu the service did not set keeps the trigger's own default (interval, cron and
    /// queue tasks get 300 s / 10 s, HTTP and calls 30 s / 1 s), capped by the plan maximum; an
    /// unset body limit keeps the trigger's default too (1 MiB per queue batch).
    pub fn limits_for(&self, trigger: &Trigger) -> Limits {
        let mut l = Limits::defaults_for(trigger);
        let td = &self.trigger_defaults;
        l.wall = if td.explicit_wall {
            Duration::from_millis(self.wall_ms.value)
        } else {
            l.wall.min(Duration::from_millis(td.plan_wall_ms))
        };
        l.cpu = if td.explicit_cpu {
            Duration::from_millis(self.cpu_ms.value)
        } else {
            l.cpu.min(Duration::from_millis(td.plan_cpu_ms))
        };
        l.memory_mb = self.memory_mb.value;
        if td.explicit_body {
            l.max_request_body_bytes = self.max_body_bytes.value;
        }
        l
    }

    /// Scheduler knobs for this service; `plan` supplies the tenant concurrency cap.
    pub fn knobs(&self, plan: &Plan) -> ServiceKnobs {
        ServiceKnobs {
            max_instances: self.max_instances.value,
            min_instances: self.min_instances.value,
            max_queued: self.max_queued.value,
            queue_timeout: self.queue_timeout.value,
            tenant_max_concurrency: plan.max_concurrency,
            keep_resident: self.keep_resident.value,
            reuse_instances: self.reuse_instances.value,
            reuse_isolates: self.reuse_isolates.value,
            idle_ttl: self.idle_ttl.value,
            preload: self.preload.value,
            compile: self.compile.value,
            min_run_time: None,
            scaling: self.scaling,
        }
    }
}
