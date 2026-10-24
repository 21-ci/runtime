//! Scheduler configuration (docs/config.md). `SchedConfig` is fixed at start; the live-updatable
//! subset is [`LiveSchedConfig`] (`PUT /v1/node/config`).

use std::time::Duration;

use rt_types::TaskKind;

/// `sched.kinds.<kind>`: bounds on how many Ps a kind may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KindRange {
    pub min: u32,
    /// 0 = `processors - sum(other kinds' mins)`.
    pub max: u32,
}

/// Knobs that may change while running.
#[derive(Clone, Debug, PartialEq)]
pub struct LiveSchedConfig {
    /// Spin before parking (`sched.spin_us`).
    pub spin: Duration,
    /// Preemption slice (`sched.time_slice`).
    pub time_slice: Duration,
    /// Upper bound of the adaptive sysmon tick (`sysmon.max_tick`).
    pub sysmon_max_tick: Duration,
    /// Re-kinding cadence (`rebalance.interval`).
    pub rebalance_interval: Duration,
    /// Move a hogged P's queue to the GRQ and give the P to a spare M (`sched.handoff_on_hog`).
    pub handoff_on_hog: bool,
    /// Cap on bound tasks per Machine (`deno.max_bound_per_m`).
    pub max_bound_per_m: u32,
}

impl LiveSchedConfig {
    /// The live knobs of a resolved node configuration (explicit keys, else the node default
    /// profile). `rt-server` re-applies them on `SIGHUP` and when `PUT /v1/node/config` changes the
    /// default profile ([`crate::Scheduler::reconfigure`]).
    pub fn from_config(cfg: &rt_config::Config) -> Self {
        let k = cfg.node_knobs();
        Self {
            spin: Duration::from_micros(k.spin_us.value),
            time_slice: k.time_slice.value,
            sysmon_max_tick: k.sysmon_max_tick.value,
            rebalance_interval: k.rebalance_interval.value,
            handoff_on_hog: k.handoff_on_hog.value,
            max_bound_per_m: k.deno_max_bound_per_m.value,
        }
    }
}

impl Default for LiveSchedConfig {
    /// `balanced` profile.
    fn default() -> Self {
        Self {
            spin: Duration::from_micros(10),
            time_slice: Duration::from_millis(10),
            sysmon_max_tick: Duration::from_millis(10),
            rebalance_interval: Duration::from_millis(100),
            handoff_on_hog: false,
            max_bound_per_m: 64,
        }
    }
}

/// Fixed scheduler configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct SchedConfig {
    /// Total processors (Σ over guest kinds), already resolved from `sched.processors` / cgroup.
    pub processors: u32,
    pub deno: KindRange,
    pub wasm: KindRange,
    /// Dedicated low-priority build processors (`build.threads`, max 2). Not part of `processors`.
    pub build_processors: u32,
    /// Extra Machines available for P hand-off.
    pub spare_machines: u32,
    /// Local run queue capacity (256).
    pub lrq_capacity: usize,
    /// Run slots node-wide (`sched.max_inflight`); tasks beyond it wait (D-A3).
    pub max_inflight: u32,
    /// Node bound on waiting tasks (`sched.max_queued`; 0 = auto, 4 x `max_inflight`).
    pub max_queued: u32,
    /// Node bound on bytes held by waiting tasks (`sched.max_queued_mb`; 0 = auto, 1/16 of the
    /// memory budget).
    pub max_queued_bytes: u64,
    /// Share of the memory budget warm instances may use (`sched.warm_share`).
    pub warm_share: f64,
    /// Least time between two allocator purge requests under memory pressure
    /// (`memory.purge_min_interval`).
    pub purge_min_interval: Duration,
    /// Fairness: every N-th pick takes from the GRQ first (Go uses 61).
    pub grq_fairness_tick: u64,
    /// `up_threshold` of elastic re-kinding.
    pub rebalance_up_threshold: f64,
    /// One re-kind per P per cooldown.
    pub rebalance_cooldown: Duration,
    /// Pin Machines to cores (`sched.pin_threads`).
    pub pin_threads: bool,
    /// Nice value for build Machines.
    pub build_nice: i32,
    pub live: LiveSchedConfig,
    /// Seed for placement / victim selection (always set so runs are reproducible when asked).
    pub seed: u64,
    /// `k` of the admission deadline key (`sched.aging_factor`; 0 = arrival order in a tenant).
    pub aging_factor: u32,
}

impl Default for SchedConfig {
    /// `balanced`, 4 processors split 2 deno + 2 wasm at start, 1 build processor.
    fn default() -> Self {
        Self {
            processors: 4,
            deno: KindRange { min: 1, max: 0 },
            wasm: KindRange { min: 1, max: 0 },
            build_processors: 1,
            spare_machines: 2,
            lrq_capacity: 256,
            max_inflight: 10_000,
            max_queued: 0,
            max_queued_bytes: 0,
            warm_share: 0.15,
            purge_min_interval: Duration::from_secs(30),
            grq_fairness_tick: 61,
            rebalance_up_threshold: 2.0,
            rebalance_cooldown: Duration::from_secs(1),
            pin_threads: false,
            build_nice: 10,
            live: LiveSchedConfig::default(),
            seed: 0x5eed,
            aging_factor: crate::admission::DEFAULT_AGING,
        }
    }
}

impl SchedConfig {
    /// Build from the resolved node configuration. `available_cpus` is the cgroup-aware CPU
    /// count used when `sched.processors = 0` (CFG-07).
    pub fn from_config(cfg: &rt_config::Config, available_cpus: u32) -> Self {
        let k = cfg.node_knobs();
        Self {
            processors: cfg.resolve_processors(available_cpus),
            deno: KindRange {
                min: cfg.sched.kinds.deno.min,
                max: cfg.sched.kinds.deno.max,
            },
            wasm: KindRange {
                min: cfg.sched.kinds.wasm.min,
                max: cfg.sched.kinds.wasm.max,
            },
            build_processors: cfg.build.threads.clamp(1, 2),
            spare_machines: cfg.sched.spare_machines,
            lrq_capacity: cfg.sched.lrq_capacity as usize,
            max_inflight: cfg.sched.max_inflight,
            max_queued: cfg.sched.max_queued,
            max_queued_bytes: u64::from(cfg.sched.max_queued_mb) * 1024 * 1024,
            warm_share: k.warm_share.value,
            purge_min_interval: k.memory_purge_min_interval.value,
            grq_fairness_tick: 61,
            rebalance_up_threshold: cfg.rebalance.up_threshold,
            rebalance_cooldown: cfg.rebalance.cooldown,
            pin_threads: k.pin_threads.value,
            build_nice: 10,
            live: LiveSchedConfig::from_config(cfg),
            seed: 0x5eed,
            aging_factor: cfg.sched.aging_factor,
        }
    }

    /// Effective `(min, max)` for a guest kind with `max = 0` expanded
    /// (`processors - other kind's min`).
    pub fn kind_bounds(&self, kind: TaskKind) -> (u32, u32) {
        let (me, other) = match kind {
            TaskKind::Exec(rt_types::RuntimeKind::Deno) => (self.deno, self.wasm),
            TaskKind::Exec(rt_types::RuntimeKind::Wasm) => (self.wasm, self.deno),
            TaskKind::Build => return (self.build_processors, self.build_processors),
        };
        let max = if me.max == 0 {
            self.processors.saturating_sub(other.min)
        } else {
            me.max
        };
        (me.min, max.max(me.min))
    }
}
