//! WASM knobs (docs/config.md):
//! - WSM-12: `wasm.reuse_instances` is a per-service knob (`config.wasm.reuse_instances`) that
//!   beats the service's profile and the node config, with its source reported;
//! - WSM-17: the pooling allocator's warm-slot and resident-memory knobs come from the profile
//!   (`min_latency` keeps the most warm/resident, `max_density` the least) unless set explicitly.

use rt_config::profile::node_knobs;
use rt_config::{resolve_node, Config, Source};
use rt_types::{Plan, Profile, ServiceConfig, ServicePatch, WasmServiceConfig};

fn cfg_with_profile(p: Profile) -> Config {
    let mut c = Config::default();
    c.node.default_profile = p;
    c
}

fn wasm_reuse(v: bool) -> ServiceConfig {
    ServiceConfig {
        wasm: WasmServiceConfig {
            reuse_instances: Some(v),
        },
        ..Default::default()
    }
}

#[test]
fn ac_wsm_12_service_reuse_instances_beats_the_profile_and_is_marked_service() {
    // balanced: off by profile, on by the service
    let cfg = cfg_with_profile(Profile::Balanced);
    let eff = cfg.service_effective(None, &wasm_reuse(true), &Plan::default());
    assert_eq!(
        (eff.reuse_instances.value, eff.reuse_instances.source),
        (true, Source::Service)
    );
    assert!(
        eff.knobs(&Plan::default()).reuse_instances,
        "the knob must reach ServiceKnobs"
    );

    // min_latency service profile: on by profile, off by the service
    let eff = cfg.service_effective(Some(Profile::MinLatency), &wasm_reuse(false), &Plan::default());
    assert_eq!(
        (eff.reuse_instances.value, eff.reuse_instances.source),
        (false, Source::Service)
    );
    assert!(!eff.knobs(&Plan::default()).reuse_instances);

    // unset: the profile decides, as before
    let eff = cfg.service_effective(Some(Profile::MinLatency), &ServiceConfig::default(), &Plan::default());
    assert_eq!(
        (eff.reuse_instances.value, eff.reuse_instances.source),
        (true, Source::Profile(Profile::MinLatency))
    );
}

#[test]
fn ac_wsm_12_service_reuse_instances_beats_an_explicit_node_value() {
    let mut cfg = cfg_with_profile(Profile::Balanced);
    cfg.wasm.reuse_instances = Some(true);
    let node = cfg.service_effective(None, &ServiceConfig::default(), &Plan::default());
    assert_eq!(
        (node.reuse_instances.value, node.reuse_instances.source),
        (true, Source::Config)
    );
    let svc = cfg.service_effective(None, &wasm_reuse(false), &Plan::default());
    assert_eq!(
        (svc.reuse_instances.value, svc.reuse_instances.source),
        (false, Source::Service)
    );
}

#[test]
fn ac_wsm_12_wasm_block_is_part_of_the_service_config_and_patch() {
    let c: ServiceConfig = serde_json::from_str(r#"{"wasm":{"reuse_instances":true}}"#).expect("wasm block");
    assert_eq!(c.wasm.reuse_instances, Some(true));
    assert!(
        serde_json::from_str::<ServiceConfig>(r#"{"wasm":{"reuse":true}}"#).is_err(),
        "unknown wasm keys are rejected"
    );
    // the block is optional and stays out of the JSON of services that never set it
    let none: ServiceConfig = serde_json::from_str("{}").expect("empty config");
    assert_eq!(none.wasm, WasmServiceConfig::default());
    let json = serde_json::to_value(&none).expect("serialize");
    assert!(json.get("wasm").is_none(), "{json}");
    let p: ServicePatch = serde_json::from_str(r#"{"wasm":{"reuse_instances":false}}"#).expect("patch");
    assert_eq!(
        p.wasm,
        Some(WasmServiceConfig {
            reuse_instances: Some(false)
        })
    );
}

#[test]
fn ac_wsm_17_pool_knobs_come_from_the_profile() {
    let d = node_knobs(Profile::MaxDensity);
    let b = node_knobs(Profile::Balanced);
    let l = node_knobs(Profile::MinLatency);
    // 0 / 100 / 1000 unused warm slots; 0 / 1 MB / 8 MB kept resident
    assert_eq!(
        (
            d.wasm_max_unused_warm_slots,
            b.wasm_max_unused_warm_slots,
            l.wasm_max_unused_warm_slots
        ),
        (0, 100, 1000)
    );
    assert!(d.wasm_linear_memory_keep_resident < b.wasm_linear_memory_keep_resident);
    assert!(b.wasm_linear_memory_keep_resident < l.wasm_linear_memory_keep_resident);

    let k = resolve_node(&cfg_with_profile(Profile::MinLatency));
    assert_eq!(
        (k.wasm_max_unused_warm_slots.value, k.wasm_max_unused_warm_slots.source),
        (1000, Source::Profile(Profile::MinLatency))
    );
    let mut cfg = cfg_with_profile(Profile::MinLatency);
    cfg.wasm.max_unused_warm_slots = Some(7);
    let k = resolve_node(&cfg);
    assert_eq!(
        (k.wasm_max_unused_warm_slots.value, k.wasm_max_unused_warm_slots.source),
        (7, Source::Config)
    );
}

#[test]
fn ac_wsm_17_print_config_fills_in_the_warm_slot_knob() {
    let out = cfg_with_profile(Profile::MaxDensity).print().expect("print config");
    assert!(out.contains("max_unused_warm_slots = 0"), "{out}");
}
