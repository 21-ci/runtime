//! Profile knob table (docs/config.md). One value per knob per profile.

use std::time::Duration;

use rt_types::{CompileMode, Profile};

/// Node-scope knobs a profile provides.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NodeProfileKnobs {
    pub spin_us: u64,
    pub time_slice: Duration,
    pub pin_threads: bool,
    pub handoff_on_hog: bool,
    pub sysmon_max_tick: Duration,
    pub rebalance_interval: Duration,
    pub deno_max_bound_per_m: u32,
    pub deno_isolate_pool_per_p: u32,
    pub memory_high: f64,
    pub memory_critical: f64,
    pub memory_purge_min_interval: Duration,
    pub cache_t1_share: f64,
    /// Bytes.
    pub wasm_linear_memory_keep_resident: u64,
    /// Pooling allocator: released slots kept affine (warm) to their module.
    pub wasm_max_unused_warm_slots: u32,
}

/// Service-scope knobs a profile provides.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ServiceProfileKnobs {
    pub queue_timeout: Duration,
    pub cache_idle_ttl: Duration,
    pub cache_preload: bool,
    pub build_compile: CompileMode,
    pub wasm_reuse_instances: bool,
    pub scaling_keep_resident: bool,
    /// `deno.reuse_isolates` (decision D-A4).
    pub deno_reuse_isolates: bool,
}

const MIB: u64 = 1024 * 1024;

/// Node-scope knob values of `p` (table in docs/config.md).
pub fn node_knobs(p: Profile) -> NodeProfileKnobs {
    match p {
        Profile::MaxDensity => NodeProfileKnobs {
            spin_us: 0,
            time_slice: Duration::from_millis(20),
            pin_threads: false,
            handoff_on_hog: false,
            sysmon_max_tick: Duration::from_millis(20),
            rebalance_interval: Duration::from_millis(250),
            deno_max_bound_per_m: 128,
            deno_isolate_pool_per_p: 0,
            memory_high: 0.90,
            memory_critical: 0.97,
            memory_purge_min_interval: Duration::from_secs(5),
            cache_t1_share: 0.10,
            wasm_linear_memory_keep_resident: 0,
            wasm_max_unused_warm_slots: 0,
        },
        Profile::Balanced => NodeProfileKnobs {
            spin_us: 10,
            time_slice: Duration::from_millis(10),
            pin_threads: false,
            handoff_on_hog: false,
            sysmon_max_tick: Duration::from_millis(10),
            rebalance_interval: Duration::from_millis(100),
            deno_max_bound_per_m: 64,
            deno_isolate_pool_per_p: 1,
            memory_high: 0.80,
            memory_critical: 0.92,
            memory_purge_min_interval: Duration::from_secs(30),
            cache_t1_share: 0.20,
            wasm_linear_memory_keep_resident: MIB,
            wasm_max_unused_warm_slots: 100,
        },
        Profile::MinLatency => NodeProfileKnobs {
            spin_us: 50,
            time_slice: Duration::from_millis(5),
            pin_threads: true,
            handoff_on_hog: true,
            sysmon_max_tick: Duration::from_millis(2),
            rebalance_interval: Duration::from_millis(50),
            deno_max_bound_per_m: 32,
            deno_isolate_pool_per_p: 4,
            memory_high: 0.70,
            memory_critical: 0.88,
            memory_purge_min_interval: Duration::from_secs(120),
            cache_t1_share: 0.35,
            wasm_linear_memory_keep_resident: 8 * MIB,
            wasm_max_unused_warm_slots: 1000,
        },
    }
}

/// Service-scope knob values of `p` (table in docs/config.md).
pub fn service_knobs(p: Profile) -> ServiceProfileKnobs {
    match p {
        Profile::MaxDensity => ServiceProfileKnobs {
            queue_timeout: Duration::from_secs(10),
            cache_idle_ttl: Duration::from_secs(60),
            cache_preload: false,
            build_compile: CompileMode::Lazy,
            wasm_reuse_instances: false,
            scaling_keep_resident: false,
            deno_reuse_isolates: false,
        },
        Profile::Balanced => ServiceProfileKnobs {
            queue_timeout: Duration::from_secs(5),
            cache_idle_ttl: Duration::from_secs(600),
            cache_preload: false,
            build_compile: CompileMode::Lazy,
            wasm_reuse_instances: false,
            scaling_keep_resident: false,
            deno_reuse_isolates: false,
        },
        Profile::MinLatency => ServiceProfileKnobs {
            queue_timeout: Duration::from_secs(1),
            cache_idle_ttl: Duration::from_secs(3600),
            cache_preload: true,
            build_compile: CompileMode::Eager,
            wasm_reuse_instances: true,
            scaling_keep_resident: false,
            deno_reuse_isolates: true,
        },
    }
}

/// Autoscaling knobs of a profile (decisions D-A3, D-B2): the node-default ceiling of a service's instances (its adaptive concurrency
/// limit) when neither the service's `scaling.max_instances` nor the plan's `max_instances` sets
/// one, and the limit's start / floor / latency tolerance and warm bounds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScalingProfileKnobs {
    pub max_instances: u32,
    pub scaling: rt_types::ScalingKnobs,
}

/// Autoscaling knob values of `p` (table in docs/config.md).
pub fn scaling_knobs(p: Profile) -> ScalingProfileKnobs {
    use rt_types::ScalingKnobs;
    match p {
        Profile::MaxDensity => ScalingProfileKnobs {
            max_instances: 256,
            scaling: ScalingKnobs {
                start: 4,
                min: 1,
                latency_tolerance_pct: 300,
                warm_headroom_pct: 100,
                warm_max_per_proc: 1,
            },
        },
        Profile::Balanced => ScalingProfileKnobs {
            max_instances: 1024,
            scaling: ScalingKnobs::default(),
        },
        Profile::MinLatency => ScalingProfileKnobs {
            max_instances: 1024,
            scaling: ScalingKnobs {
                start: 32,
                min: 4,
                latency_tolerance_pct: 150,
                warm_headroom_pct: 150,
                warm_max_per_proc: 16,
            },
        },
    }
}

/// Node knob of a profile: the share of the memory budget warm instances may use
/// (`sched.warm_share`, decision D-A3).
pub fn warm_share(p: Profile) -> f64 {
    match p {
        Profile::MaxDensity => 0.05,
        Profile::Balanced => 0.15,
        Profile::MinLatency => 0.30,
    }
}
