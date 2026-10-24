//! Profiles and effective values (profile table of docs/config.md, CFG-04, CFG-06, CFG-08). The expectation tables below are typed in from it and
//! are intentionally independent of `rt_config::profile` so a wrong table value fails.
#![allow(clippy::bool_assert_comparison, clippy::field_reassign_with_default)] // lint-only allowance: test style, not behavior

use std::time::Duration;

use rt_config::profile::{node_knobs, service_knobs};
use rt_config::{Config, Resolved, Source};
use rt_types::{CompileMode, LimitsConfig, Plan, PlanLimitsMax, Profile, ScalingConfig, ServiceConfig, Trigger};

const MIB: u64 = 1024 * 1024;

/// Profile table, node-scope rows.
struct NodeRow {
    spin_us: u64,
    time_slice: Duration,
    pin_threads: bool,
    handoff_on_hog: bool,
    sysmon_max_tick: Duration,
    rebalance_interval: Duration,
    deno_max_bound_per_m: u32,
    deno_isolate_pool_per_p: u32,
    memory_high: f64,
    memory_critical: f64,
    memory_purge_min_interval: Duration,
    cache_t1_share: f64,
    wasm_keep_resident_bytes: u64,
}

/// Profile table, service-scope rows.
struct ServiceRow {
    queue_timeout: Duration,
    cache_idle_ttl: Duration,
    cache_preload: bool,
    build_compile: CompileMode,
    wasm_reuse_instances: bool,
    scaling_keep_resident: bool,
    deno_reuse_isolates: bool,
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn s(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn doc_node(p: Profile) -> NodeRow {
    match p {
        Profile::MaxDensity => NodeRow {
            spin_us: 0,
            time_slice: ms(20),
            pin_threads: false,
            handoff_on_hog: false,
            sysmon_max_tick: ms(20),
            rebalance_interval: ms(250),
            deno_max_bound_per_m: 128,
            deno_isolate_pool_per_p: 0,
            memory_high: 0.90,
            memory_critical: 0.97,
            memory_purge_min_interval: s(5),
            cache_t1_share: 0.10,
            wasm_keep_resident_bytes: 0,
        },
        Profile::Balanced => NodeRow {
            spin_us: 10,
            time_slice: ms(10),
            pin_threads: false,
            handoff_on_hog: false,
            sysmon_max_tick: ms(10),
            rebalance_interval: ms(100),
            deno_max_bound_per_m: 64,
            deno_isolate_pool_per_p: 1,
            memory_high: 0.80,
            memory_critical: 0.92,
            memory_purge_min_interval: s(30),
            cache_t1_share: 0.20,
            wasm_keep_resident_bytes: MIB,
        },
        Profile::MinLatency => NodeRow {
            spin_us: 50,
            time_slice: ms(5),
            pin_threads: true,
            handoff_on_hog: true,
            sysmon_max_tick: ms(2),
            rebalance_interval: ms(50),
            deno_max_bound_per_m: 32,
            deno_isolate_pool_per_p: 4,
            memory_high: 0.70,
            memory_critical: 0.88,
            memory_purge_min_interval: s(120),
            cache_t1_share: 0.35,
            wasm_keep_resident_bytes: 8 * MIB,
        },
    }
}

fn doc_service(p: Profile) -> ServiceRow {
    match p {
        Profile::MaxDensity => ServiceRow {
            queue_timeout: s(10),
            cache_idle_ttl: s(60),
            cache_preload: false,
            build_compile: CompileMode::Lazy,
            wasm_reuse_instances: false,
            scaling_keep_resident: false,
            deno_reuse_isolates: false,
        },
        Profile::Balanced => ServiceRow {
            queue_timeout: s(5),
            cache_idle_ttl: s(600),
            cache_preload: false,
            build_compile: CompileMode::Lazy,
            wasm_reuse_instances: false,
            scaling_keep_resident: false,
            deno_reuse_isolates: false,
        },
        Profile::MinLatency => ServiceRow {
            queue_timeout: s(1),
            cache_idle_ttl: s(3600),
            cache_preload: true,
            build_compile: CompileMode::Eager,
            wasm_reuse_instances: true,
            scaling_keep_resident: false, // opt-in only
            deno_reuse_isolates: true,
        },
    }
}

fn cfg_with_profile(p: Profile) -> Config {
    let mut cfg = Config::default();
    cfg.node.default_profile = p;
    cfg
}

fn assert_resolved<T: PartialEq + std::fmt::Debug>(name: &str, got: Resolved<T>, want: T, source: Source) {
    assert_eq!(got.value, want, "{name}: value");
    assert_eq!(got.source, source, "{name}: source");
}

// ---------------------------------------------------------------------------------------------
// CFG-08: every knob of every profile resolves to the documented value
// ---------------------------------------------------------------------------------------------

#[test]
fn cfg_08_every_node_knob_of_every_profile_resolves_to_the_documented_value() {
    for p in Profile::ALL {
        let want = doc_node(p);
        let src = Source::Profile(p);
        let k = cfg_with_profile(p).node_knobs();
        assert_resolved(&format!("{p} sched.spin_us"), k.spin_us, want.spin_us, src);
        assert_resolved(&format!("{p} sched.time_slice"), k.time_slice, want.time_slice, src);
        assert_resolved(&format!("{p} sched.pin_threads"), k.pin_threads, want.pin_threads, src);
        assert_resolved(
            &format!("{p} sched.handoff_on_hog"),
            k.handoff_on_hog,
            want.handoff_on_hog,
            src,
        );
        assert_resolved(
            &format!("{p} sysmon.max_tick"),
            k.sysmon_max_tick,
            want.sysmon_max_tick,
            src,
        );
        assert_resolved(
            &format!("{p} rebalance.interval"),
            k.rebalance_interval,
            want.rebalance_interval,
            src,
        );
        assert_resolved(
            &format!("{p} deno.max_bound_per_m"),
            k.deno_max_bound_per_m,
            want.deno_max_bound_per_m,
            src,
        );
        assert_resolved(
            &format!("{p} deno.isolate_pool_per_p"),
            k.deno_isolate_pool_per_p,
            want.deno_isolate_pool_per_p,
            src,
        );
        assert_resolved(&format!("{p} memory.high"), k.memory_high, want.memory_high, src);
        assert_resolved(
            &format!("{p} memory.critical"),
            k.memory_critical,
            want.memory_critical,
            src,
        );
        assert_resolved(
            &format!("{p} memory.purge_min_interval"),
            k.memory_purge_min_interval,
            want.memory_purge_min_interval,
            src,
        );
        assert_resolved(
            &format!("{p} cache.t1_share"),
            k.cache_t1_share,
            want.cache_t1_share,
            src,
        );
        assert_resolved(
            &format!("{p} wasm.linear_memory_keep_resident"),
            k.wasm_linear_memory_keep_resident,
            want.wasm_keep_resident_bytes,
            src,
        );
    }
}

#[test]
fn cfg_08_every_service_knob_of_every_profile_resolves_to_the_documented_value() {
    for p in Profile::ALL {
        let want = doc_service(p);
        let src = Source::Profile(p);
        let e = cfg_with_profile(p).service_effective(None, &ServiceConfig::default(), &Plan::default());
        assert_resolved(
            &format!("{p} sched.queue_timeout"),
            e.queue_timeout,
            want.queue_timeout,
            src,
        );
        assert_resolved(&format!("{p} cache.idle_ttl"), e.idle_ttl, want.cache_idle_ttl, src);
        assert_resolved(&format!("{p} cache.preload"), e.preload, want.cache_preload, src);
        assert_resolved(&format!("{p} build.compile"), e.compile, want.build_compile, src);
        assert_resolved(
            &format!("{p} wasm.reuse_instances"),
            e.reuse_instances,
            want.wasm_reuse_instances,
            src,
        );
        assert_resolved(
            &format!("{p} deno.reuse_isolates"),
            e.reuse_isolates,
            want.deno_reuse_isolates,
            src,
        );
        assert_resolved(
            &format!("{p} scaling.keep_resident"),
            e.keep_resident,
            want.scaling_keep_resident,
            src,
        );
    }
}

#[test]
fn profile_tables_match_the_docs_table_row_by_row() {
    for p in Profile::ALL {
        let (got, want) = (node_knobs(p), doc_node(p));
        assert_eq!(got.spin_us, want.spin_us, "{p} spin_us");
        assert_eq!(got.time_slice, want.time_slice, "{p} time_slice");
        assert_eq!(got.pin_threads, want.pin_threads, "{p} pin_threads");
        assert_eq!(got.handoff_on_hog, want.handoff_on_hog, "{p} handoff_on_hog");
        assert_eq!(got.sysmon_max_tick, want.sysmon_max_tick, "{p} sysmon_max_tick");
        assert_eq!(
            got.rebalance_interval, want.rebalance_interval,
            "{p} rebalance_interval"
        );
        assert_eq!(
            got.deno_max_bound_per_m, want.deno_max_bound_per_m,
            "{p} max_bound_per_m"
        );
        assert_eq!(
            got.deno_isolate_pool_per_p, want.deno_isolate_pool_per_p,
            "{p} isolate_pool_per_p"
        );
        assert_eq!(got.memory_high, want.memory_high, "{p} memory_high");
        assert_eq!(got.memory_critical, want.memory_critical, "{p} memory_critical");
        assert_eq!(
            got.memory_purge_min_interval, want.memory_purge_min_interval,
            "{p} purge_min_interval"
        );
        assert_eq!(got.cache_t1_share, want.cache_t1_share, "{p} t1_share");
        assert_eq!(
            got.wasm_linear_memory_keep_resident, want.wasm_keep_resident_bytes,
            "{p} keep_resident"
        );

        let (got, want) = (service_knobs(p), doc_service(p));
        assert_eq!(got.queue_timeout, want.queue_timeout, "{p} queue_timeout");
        assert_eq!(got.cache_idle_ttl, want.cache_idle_ttl, "{p} idle_ttl");
        assert_eq!(got.cache_preload, want.cache_preload, "{p} preload");
        assert_eq!(got.build_compile, want.build_compile, "{p} compile");
        assert_eq!(
            got.wasm_reuse_instances, want.wasm_reuse_instances,
            "{p} reuse_instances"
        );
        assert_eq!(got.deno_reuse_isolates, want.deno_reuse_isolates, "{p} reuse_isolates");
        assert_eq!(
            got.scaling_keep_resident, want.scaling_keep_resident,
            "{p} keep_resident"
        );
    }
}

#[test]
fn profiles_are_monotone_in_the_directions_the_docs_describe() {
    // max_density trades latency for density, min_latency the reverse.
    let (d, b, l) = (
        doc_node(Profile::MaxDensity),
        doc_node(Profile::Balanced),
        doc_node(Profile::MinLatency),
    );
    assert!(d.memory_high > b.memory_high && b.memory_high > l.memory_high);
    assert!(d.memory_critical > b.memory_critical && b.memory_critical > l.memory_critical);
    assert!(d.cache_t1_share < b.cache_t1_share && b.cache_t1_share < l.cache_t1_share);
    assert!(d.time_slice > b.time_slice && b.time_slice > l.time_slice);
    for p in Profile::ALL {
        let k = node_knobs(p);
        assert!(
            k.memory_high < k.memory_critical,
            "{p}: high watermark must be below critical"
        );
    }
}

#[test]
fn the_loaded_default_profile_selects_the_node_knobs() {
    for p in Profile::ALL {
        let toml = format!("[node]\ndefault_profile = \"{p}\"");
        let cfg = Config::from_toml(&toml, &[]).expect("load");
        let k = cfg.node_knobs();
        assert_eq!(k.spin_us.value, doc_node(p).spin_us, "{p}");
        assert_eq!(k.spin_us.source, Source::Profile(p), "{p}");
    }
    // via env as well
    let cfg = Config::from_toml("", &[("RT__NODE__DEFAULT_PROFILE".into(), "min_latency".into())]).expect("load");
    assert_eq!(cfg.node_knobs().time_slice.value, ms(5));
}

// ---------------------------------------------------------------------------------------------
// Explicit values beat profiles; sources
// ---------------------------------------------------------------------------------------------

#[test]
fn explicit_node_values_win_over_the_profile_and_are_marked_config() {
    let mut cfg = cfg_with_profile(Profile::MaxDensity);
    cfg.sched.spin_us = Some(77);
    cfg.memory.high = Some(0.5);
    cfg.sched.pin_threads = Some(true);
    cfg.sysmon.max_tick = Some(ms(3));
    let k = cfg.node_knobs();
    assert_resolved("spin_us", k.spin_us, 77, Source::Config);
    assert_resolved("memory.high", k.memory_high, 0.5, Source::Config);
    assert_resolved("pin_threads", k.pin_threads, true, Source::Config);
    assert_resolved("sysmon.max_tick", k.sysmon_max_tick, ms(3), Source::Config);
    // everything else still comes from the profile
    assert_resolved("time_slice", k.time_slice, ms(20), Source::Profile(Profile::MaxDensity));
    assert_resolved(
        "memory.critical",
        k.memory_critical,
        0.97,
        Source::Profile(Profile::MaxDensity),
    );
}

#[test]
fn explicit_zero_is_an_explicit_value_not_unset() {
    let mut cfg = cfg_with_profile(Profile::MinLatency);
    cfg.sched.spin_us = Some(0);
    cfg.wasm.linear_memory_keep_resident = Some(0);
    let k = cfg.node_knobs();
    assert_resolved("spin_us", k.spin_us, 0, Source::Config);
    assert_resolved("keep_resident", k.wasm_linear_memory_keep_resident, 0, Source::Config);
}

#[test]
fn cfg_04_service_node_and_profile_sources_are_reported_per_value() {
    // CFG-04: service with explicit cpu_ms, profile min_latency, node max_density
    let cfg = cfg_with_profile(Profile::MaxDensity);
    let service = ServiceConfig {
        limits: LimitsConfig {
            cpu_ms: Some(500),
            ..Default::default()
        },
        ..Default::default()
    };
    let eff = cfg.service_effective(Some(Profile::MinLatency), &service, &Plan::default());
    assert_resolved("cpu_ms", eff.cpu_ms, 500, Source::Service);
    assert_resolved(
        "queue_timeout",
        eff.queue_timeout,
        s(1),
        Source::Profile(Profile::MinLatency),
    );
    assert_eq!(eff.queue_timeout.source.label(), "profile:min_latency");
    let knobs = cfg.node_knobs();
    assert_resolved("spin_us", knobs.spin_us, 0, Source::Profile(Profile::MaxDensity));
    assert_eq!(knobs.spin_us.source.label(), "profile:max_density");
    // the service profile does not leak into node-scope knobs
    assert_ne!(knobs.time_slice.value, ms(5));
}

#[test]
fn service_without_a_profile_uses_the_node_default_profile() {
    let cfg = cfg_with_profile(Profile::MaxDensity);
    let eff = cfg.service_effective(None, &ServiceConfig::default(), &Plan::default());
    assert_resolved(
        "queue_timeout",
        eff.queue_timeout,
        s(10),
        Source::Profile(Profile::MaxDensity),
    );
    assert_resolved("idle_ttl", eff.idle_ttl, s(60), Source::Profile(Profile::MaxDensity));
}

#[test]
fn explicit_service_scaling_values_beat_the_service_profile() {
    let cfg = cfg_with_profile(Profile::Balanced);
    let service = ServiceConfig {
        scaling: ScalingConfig {
            keep_resident: Some(true),
            max_instances: Some(7),
            max_queued: Some(9),
            ..Default::default()
        },
        ..Default::default()
    };
    let plan = node_default_instances();
    let eff = cfg.service_effective(Some(Profile::MinLatency), &service, &plan);
    assert_resolved("keep_resident", eff.keep_resident, true, Source::Service);
    assert_resolved("max_instances", eff.max_instances, 7, Source::Service);
    assert_resolved("max_queued", eff.max_queued, 9, Source::Service);

    let unset = cfg.service_effective(Some(Profile::MinLatency), &ServiceConfig::default(), &plan);
    assert_eq!(unset.keep_resident.value, false, "min_latency keep_resident is opt-in");
    assert_eq!(unset.keep_resident.source, Source::Profile(Profile::MinLatency));
    // D-A3: without an explicit hard cap the profile's ceiling bounds the adaptive limit
    assert_resolved(
        "max_instances",
        unset.max_instances,
        1024,
        Source::Profile(Profile::MinLatency),
    );
    assert_eq!(unset.max_queued.source, Source::Default);
    assert_eq!(unset.max_queued.value, 0, "0 = the node queue share");
}

/// A plan without a per-service instance cap (`max_instances = 0`: the node default applies).
fn node_default_instances() -> Plan {
    Plan {
        max_instances: 0,
        ..Plan::default()
    }
}

/// D-A3: the adaptive concurrency limit starts and floors per profile; an explicit
/// `scaling.max_instances` caps both.
#[test]
fn autoscaling_bounds_come_from_the_profile_and_an_explicit_cap_clamps_them() {
    let cfg = cfg_with_profile(Profile::Balanced);
    let plan = node_default_instances();
    let start_min_max = |p: Profile, max: Option<u32>| {
        let service = ServiceConfig {
            scaling: ScalingConfig {
                max_instances: max,
                ..Default::default()
            },
            ..Default::default()
        };
        let eff = cfg.service_effective(Some(p), &service, &plan);
        let k = eff.knobs(&plan);
        assert_eq!(
            (eff.concurrency_start.value, eff.concurrency_min.value),
            (k.scaling.start, k.scaling.min)
        );
        (k.scaling.start, k.scaling.min, k.max_instances)
    };
    assert_eq!(start_min_max(Profile::MaxDensity, None), (4, 1, 256));
    assert_eq!(start_min_max(Profile::Balanced, None), (16, 2, 1024));
    assert_eq!(start_min_max(Profile::MinLatency, None), (32, 4, 1024));
    assert_eq!(
        start_min_max(Profile::MinLatency, Some(3)),
        (3, 3, 3),
        "the hard cap clamps start and min"
    );
    // review 11: 0 is "no explicit cap" (like the plan's 0 = unlimited), never "run nothing"
    assert_eq!(start_min_max(Profile::Balanced, Some(0)), (16, 2, 1024));
    let eff = cfg.service_effective(Some(Profile::MinLatency), &ServiceConfig::default(), &plan);
    assert_eq!(eff.concurrency_start.source, Source::Profile(Profile::MinLatency));
    assert_eq!(cfg.node_knobs().warm_share.value, 0.15);
    assert_eq!(
        cfg_with_profile(Profile::MinLatency).node_knobs().warm_share.value,
        0.30
    );
}

#[test]
fn explicit_node_config_for_a_service_scope_knob_applies_to_services_and_is_marked_config() {
    let mut cfg = cfg_with_profile(Profile::Balanced);
    cfg.sched.queue_timeout = Some(s(2));
    cfg.cache.idle_ttl = Some(s(30));
    cfg.cache.preload = Some(true);
    cfg.build.compile = Some(CompileMode::Eager);
    cfg.wasm.reuse_instances = Some(true);
    cfg.deno.reuse_isolates = Some(true);
    let eff = cfg.service_effective(None, &ServiceConfig::default(), &Plan::default());
    assert_resolved("queue_timeout", eff.queue_timeout, s(2), Source::Config);
    assert_resolved("idle_ttl", eff.idle_ttl, s(30), Source::Config);
    assert_resolved("preload", eff.preload, true, Source::Config);
    assert_resolved("compile", eff.compile, CompileMode::Eager, Source::Config);
    assert_resolved("reuse_instances", eff.reuse_instances, true, Source::Config);
    assert_resolved("reuse_isolates", eff.reuse_isolates, true, Source::Config);
    assert!(
        eff.knobs(&Plan::default()).reuse_isolates,
        "the resolved knob reaches the task spec"
    );
}

#[test]
fn default_limits_have_the_default_source_and_the_documented_values() {
    let eff = Config::default().service_effective(None, &ServiceConfig::default(), &Plan::default());
    assert_resolved("wall_ms", eff.wall_ms, 30_000, Source::Default);
    assert_resolved("cpu_ms", eff.cpu_ms, 1_000, Source::Default);
    assert_resolved("memory_mb", eff.memory_mb, 128, Source::Default);
    assert_resolved("max_body_bytes", eff.max_body_bytes, 10 * MIB, Source::Default);
    assert!(eff.plan_conflicts.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Instances (D-B2, CFG-09..CFG-12)
// ---------------------------------------------------------------------------------------------

fn scaling(min_instances: Option<u32>, max_instances: Option<u32>) -> ServiceConfig {
    ServiceConfig {
        scaling: ScalingConfig {
            min_instances,
            max_instances,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn plan_instances(max_instances: u32) -> Plan {
    Plan {
        max_instances,
        ..Plan::default()
    }
}

/// CFG-09: defaults. `min_instances` is 0 (scale to zero); `max_instances` is the plan's limit,
/// and the node default (the profile ceiling) while the plan sets none (0).
#[test]
fn cfg_09_instance_bounds_default_to_zero_min_and_the_plan_limit_as_max() {
    let cfg = cfg_with_profile(Profile::Balanced);
    let eff = cfg.service_effective(None, &ServiceConfig::default(), &plan_instances(32));
    assert_resolved("min_instances", eff.min_instances, 0, Source::Default);
    assert_resolved("max_instances", eff.max_instances, 32, Source::Plan);
    assert!(eff.plan_conflicts.is_empty(), "a default is not a conflict");
    let k = eff.knobs(&plan_instances(32));
    assert_eq!((k.min_instances, k.max_instances), (0, 32));

    let eff = cfg.service_effective(None, &ServiceConfig::default(), &node_default_instances());
    assert_resolved(
        "max_instances",
        eff.max_instances,
        1024,
        Source::Profile(Profile::Balanced),
    );
    let eff =
        cfg_with_profile(Profile::MaxDensity).service_effective(None, &ServiceConfig::default(), &plan_instances(256));
    assert_resolved(
        "max_instances",
        eff.max_instances,
        256,
        Source::Profile(Profile::MaxDensity),
    );
    let eff =
        cfg_with_profile(Profile::MaxDensity).service_effective(None, &ServiceConfig::default(), &plan_instances(4096));
    assert_resolved(
        "max_instances",
        eff.max_instances,
        256,
        Source::Profile(Profile::MaxDensity),
    );
}

/// CFG-10: explicit bounds win within the plan; above the plan they are clamped to it and reported
/// as `plan_conflict`s, like the other limits (CFG-06).
#[test]
fn cfg_10_instance_bounds_above_the_plan_are_clamped_and_reported() {
    let cfg = cfg_with_profile(Profile::Balanced);
    let plan = plan_instances(8);
    let eff = cfg.service_effective(None, &scaling(Some(2), Some(6)), &plan);
    assert_resolved("min_instances", eff.min_instances, 2, Source::Service);
    assert_resolved("max_instances", eff.max_instances, 6, Source::Service);
    assert!(eff.plan_conflicts.is_empty());

    let eff = cfg.service_effective(None, &scaling(Some(20), Some(100)), &plan);
    assert_resolved("max_instances", eff.max_instances, 8, Source::Plan);
    assert_resolved("min_instances", eff.min_instances, 8, Source::Plan);
    let mut found: Vec<_> = eff
        .plan_conflicts
        .iter()
        .map(|c| (c.field, c.explicit, c.plan_max))
        .collect();
    found.sort_unstable();
    assert_eq!(found, vec![("max_instances", 100, 8), ("min_instances", 20, 8)]);
    let k = eff.knobs(&plan);
    assert_eq!((k.min_instances, k.max_instances), (8, 8));

    // a plan without a cap (0) clamps nothing
    let eff = cfg.service_effective(None, &scaling(Some(50), Some(5000)), &node_default_instances());
    assert_resolved("max_instances", eff.max_instances, 5000, Source::Service);
    assert_resolved("min_instances", eff.min_instances, 50, Source::Service);
    assert!(eff.plan_conflicts.is_empty());
}

/// CFG-11: `0` means "not set" for both bounds (the old `max_concurrency = 0` read the same), and
/// a `min_instances` above the effective max is cut to it (the API refuses that pair outright).
#[test]
fn cfg_11_zero_means_unset_and_min_never_exceeds_max() {
    let cfg = cfg_with_profile(Profile::Balanced);
    let plan = plan_instances(32);
    let eff = cfg.service_effective(None, &scaling(Some(0), Some(0)), &plan);
    assert_resolved("min_instances", eff.min_instances, 0, Source::Default);
    assert_resolved("max_instances", eff.max_instances, 32, Source::Plan);
    let eff = cfg.service_effective(None, &scaling(Some(10), Some(4)), &plan);
    assert_eq!((eff.min_instances.value, eff.max_instances.value), (4, 4));
    // min alone above the default max (the plan's 32)
    let eff = cfg.service_effective(None, &scaling(Some(40), None), &plan);
    assert_eq!((eff.min_instances.value, eff.max_instances.value), (32, 32));
    assert_eq!(eff.plan_conflicts.len(), 1);
}

/// CFG-12: the instance bounds shape the adaptive limit: `max_instances` caps start and floor,
/// `min_instances` raises them, and the knobs the scheduler gets carry both.
#[test]
fn cfg_12_instance_bounds_clamp_and_raise_the_adaptive_limit_bounds() {
    let cfg = cfg_with_profile(Profile::Balanced); // start 16, floor 2
    let plan = node_default_instances();
    let knobs = |min, max| {
        let eff = cfg.service_effective(None, &scaling(min, max), &plan);
        let k = eff.knobs(&plan);
        assert_eq!(
            (eff.concurrency_start.value, eff.concurrency_min.value),
            (k.scaling.start, k.scaling.min)
        );
        (k.scaling.start, k.scaling.min, k.min_instances, k.max_instances)
    };
    assert_eq!(knobs(None, None), (16, 2, 0, 1024));
    assert_eq!(
        knobs(Some(8), None),
        (16, 8, 8, 1024),
        "the floor rises to min_instances"
    );
    assert_eq!(knobs(Some(40), None), (40, 40, 40, 1024), "so does the start");
    assert_eq!(knobs(None, Some(3)), (3, 2, 0, 3), "max_instances caps the start");
    assert_eq!(knobs(Some(2), Some(2)), (2, 2, 2, 2), "min = max pins the limit");
    assert_eq!(knobs(None, Some(1)), (1, 1, 0, 1));
}

// ---------------------------------------------------------------------------------------------
// Plan maximum (CFG-06)
// ---------------------------------------------------------------------------------------------

fn plan_with(wall_ms: u64, cpu_ms: u64, memory_mb: u32) -> Plan {
    Plan {
        limits_max: PlanLimitsMax {
            wall_ms,
            cpu_ms,
            memory_mb,
        },
        ..Plan::default()
    }
}

#[test]
fn cfg_06_plan_lowered_below_an_explicit_value_wins_and_records_a_conflict() {
    let service = ServiceConfig {
        limits: LimitsConfig {
            memory_mb: Some(512),
            wall_ms: Some(20_000),
            cpu_ms: Some(900),
            ..Default::default()
        },
        ..Default::default()
    };
    // explicit 512 MB was valid for an older plan; the plan now allows only 128 MB
    let eff = Config::default().service_effective(None, &service, &plan_with(30_000, 1_000, 128));
    assert_resolved("memory_mb", eff.memory_mb, 128, Source::Plan);
    assert_eq!(eff.plan_conflicts.len(), 1, "{:?}", eff.plan_conflicts);
    let c = &eff.plan_conflicts[0];
    assert_eq!((c.field, c.explicit, c.plan_max), ("memory_mb", 512, 128));
    // values within the plan keep their explicit source
    assert_resolved("wall_ms", eff.wall_ms, 20_000, Source::Service);
    assert_resolved("cpu_ms", eff.cpu_ms, 900, Source::Service);
}

#[test]
fn explicit_value_equal_to_the_plan_maximum_is_not_a_conflict() {
    let service = ServiceConfig {
        limits: LimitsConfig {
            memory_mb: Some(128),
            ..Default::default()
        },
        ..Default::default()
    };
    let eff = Config::default().service_effective(None, &service, &plan_with(30_000, 1_000, 128));
    assert_resolved("memory_mb", eff.memory_mb, 128, Source::Service);
    assert!(eff.plan_conflicts.is_empty());
}

#[test]
fn every_limit_over_the_plan_is_clamped_and_reported() {
    let service = ServiceConfig {
        limits: LimitsConfig {
            wall_ms: Some(60_000),
            cpu_ms: Some(5_000),
            memory_mb: Some(1024),
            ..Default::default()
        },
        ..Default::default()
    };
    let eff = Config::default().service_effective(None, &service, &plan_with(10_000, 200, 64));
    assert_resolved("wall_ms", eff.wall_ms, 10_000, Source::Plan);
    assert_resolved("cpu_ms", eff.cpu_ms, 200, Source::Plan);
    assert_resolved("memory_mb", eff.memory_mb, 64, Source::Plan);
    let mut fields: Vec<_> = eff.plan_conflicts.iter().map(|c| c.field).collect();
    fields.sort_unstable();
    assert_eq!(fields, vec!["cpu_ms", "memory_mb", "wall_ms"]);
}

#[test]
fn a_plan_below_the_builtin_default_clamps_unset_values_without_a_conflict() {
    let eff = Config::default().service_effective(None, &ServiceConfig::default(), &plan_with(30_000, 500, 64));
    assert_resolved("cpu_ms", eff.cpu_ms, 500, Source::Plan);
    assert_resolved("memory_mb", eff.memory_mb, 64, Source::Plan);
    assert_resolved("wall_ms", eff.wall_ms, 30_000, Source::Default);
    assert!(eff.plan_conflicts.is_empty(), "nothing explicit was overridden");
}

// ---------------------------------------------------------------------------------------------
// Effective values feed the scheduler
// ---------------------------------------------------------------------------------------------

#[test]
fn effective_values_become_task_limits_and_service_knobs() {
    let mut plan = Plan::default();
    plan.max_concurrency = 33;
    let service = ServiceConfig {
        limits: LimitsConfig {
            wall_ms: Some(5_000),
            cpu_ms: Some(250),
            memory_mb: Some(64),
            max_body_bytes: Some(1234),
        },
        scaling: ScalingConfig {
            max_instances: Some(4),
            min_instances: Some(2),
            max_queued: Some(16),
            keep_resident: Some(true),
        },
        ..Default::default()
    };
    let eff = cfg_with_profile(Profile::MinLatency).service_effective(None, &service, &plan);

    let limits = eff.limits_for(&Trigger::Http);
    assert_eq!(limits.wall, ms(5_000));
    assert_eq!(limits.cpu, ms(250));
    assert_eq!(limits.memory_mb, 64);
    assert_eq!(limits.max_request_body_bytes, 1234);

    let knobs = eff.knobs(&plan);
    assert_eq!((knobs.min_instances, knobs.max_instances, knobs.max_queued), (2, 4, 16));
    assert_eq!(knobs.tenant_max_concurrency, 33, "tenant cap comes from the plan");
    assert!(knobs.keep_resident);
    assert_eq!(knobs.queue_timeout, s(1));
    assert_eq!(knobs.idle_ttl, s(3600));
    assert!(knobs.preload && knobs.reuse_instances && knobs.reuse_isolates);
    assert_eq!(knobs.compile, CompileMode::Eager);
}

#[test]
fn unset_limits_keep_each_triggers_default_capped_by_the_plan() {
    let queue = Trigger::Queue {
        batch: Vec::new(),
        attempt: 1,
    };
    let interval = Trigger::Interval {
        id: rt_types::ScheduleId::from_u128(1),
    };

    // A plan that allows long tasks: schedules and queue batches get their own 300 s / 10 s.
    let roomy = plan_with(600_000, 60_000, 128);
    let eff = Config::default().service_effective(None, &ServiceConfig::default(), &roomy);
    for t in [&queue, &interval] {
        let l = eff.limits_for(t);
        assert_eq!((l.wall, l.cpu), (s(300), s(10)), "{t:?} keeps its trigger default");
    }
    assert_eq!(
        eff.limits_for(&queue).max_request_body_bytes,
        MIB,
        "a queue batch body defaults to 1 MiB"
    );
    let http = eff.limits_for(&Trigger::Http);
    assert_eq!(
        (http.wall, http.cpu, http.max_request_body_bytes),
        (s(30), ms(1_000), 10 * MIB)
    );

    // The default (free) plan caps every trigger at 30 s / 1 s.
    let eff = Config::default().service_effective(None, &ServiceConfig::default(), &Plan::default());
    let l = eff.limits_for(&queue);
    assert_eq!((l.wall, l.cpu), (s(30), ms(1_000)), "the plan maximum still wins");

    // Explicit service values apply to every trigger.
    let service = ServiceConfig {
        limits: LimitsConfig {
            wall_ms: Some(5_000),
            cpu_ms: Some(250),
            memory_mb: None,
            max_body_bytes: Some(4096),
        },
        ..Default::default()
    };
    let eff = Config::default().service_effective(None, &service, &roomy);
    let l = eff.limits_for(&queue);
    assert_eq!((l.wall, l.cpu, l.max_request_body_bytes), (ms(5_000), ms(250), 4096));
}

// ---------------------------------------------------------------------------------------------
// Source labels / JSON shape (GET /v1/services/{s} `effective`)
// ---------------------------------------------------------------------------------------------

#[test]
fn source_labels_match_the_documented_vocabulary() {
    assert_eq!(Source::Service.label(), "service");
    assert_eq!(Source::Config.label(), "config");
    assert_eq!(Source::Plan.label(), "plan");
    assert_eq!(Source::Default.label(), "default");
    assert_eq!(Source::Profile(Profile::MaxDensity).label(), "profile:max_density");
    assert_eq!(Source::Profile(Profile::Balanced).label(), "profile:balanced");
    assert_eq!(Source::Profile(Profile::MinLatency).label(), "profile:min_latency");
}

#[test]
fn resolved_values_serialize_as_value_and_source() {
    let v = serde_json::to_value(Resolved::new(1000u64, Source::Profile(Profile::Balanced))).unwrap();
    assert_eq!(v, serde_json::json!({"value": 1000, "source": "profile:balanced"}));
    let v = serde_json::to_value(Resolved::new(true, Source::Service)).unwrap();
    assert_eq!(v, serde_json::json!({"value": true, "source": "service"}));
}

#[test]
fn effective_block_serializes_every_value_with_its_source_and_hides_conflicts() {
    let eff = Config::default().service_effective(None, &ServiceConfig::default(), &Plan::default());
    let v = serde_json::to_value(&eff).unwrap();
    for key in [
        "wall_ms",
        "cpu_ms",
        "memory_mb",
        "max_body_bytes",
        "min_instances",
        "max_instances",
        "max_queued",
        "keep_resident",
        "queue_timeout",
        "idle_ttl",
        "preload",
        "compile",
        "reuse_instances",
        "reuse_isolates",
    ] {
        assert!(v[key]["source"].is_string(), "effective.{key}.source missing in {v}");
        assert!(v[key].get("value").is_some(), "effective.{key}.value missing in {v}");
    }
    assert!(
        v.get("plan_conflicts").is_none(),
        "conflicts are an event, not part of the effective block"
    );
    assert_eq!(v["queue_timeout"]["source"], "profile:balanced");
}

// ---------------------------------------------------------------------------------------------
// Units: MB in the schema/defconfigs vs bytes in the Rust field
// ---------------------------------------------------------------------------------------------

#[test]
fn wasm_keep_resident_in_the_file_is_megabytes_like_the_schema_and_defconfigs() {
    // config/schema.json: "Keep resident linear memory in MB", profile defaults 0 / 1 / 8; the
    // shipped defconfigs write `linear_memory_keep_resident = 1`. The resolved knob is bytes.
    let cfg = Config::from_toml("[wasm]\nlinear_memory_keep_resident = 8", &[]).expect("load");
    assert_eq!(
        cfg.node_knobs().wasm_linear_memory_keep_resident.value,
        8 * MIB,
        "a file value of 8 means 8 MB, i.e. 8 MiB in bytes"
    );
}
