//! Decision D-A4: `deno.reuse_isolates` is also a per-service knob (`config.deno.reuse_isolates`),
//! resolved service > node config > the service's profile, with its source reported.

use rt_config::{Config, Source};
use rt_types::{DenoServiceConfig, Plan, Profile, ServiceConfig};

fn deno_reuse(v: bool) -> ServiceConfig {
    ServiceConfig {
        deno: DenoServiceConfig {
            reuse_isolates: Some(v),
        },
        ..Default::default()
    }
}

#[test]
fn service_reuse_isolates_beats_the_node_and_the_profile() {
    let cfg = Config::default(); // balanced: off by profile
    let eff = cfg.service_effective(None, &deno_reuse(true), &Plan::default());
    assert_eq!(
        (eff.reuse_isolates.value, eff.reuse_isolates.source),
        (true, Source::Service)
    );
    assert!(
        eff.knobs(&Plan::default()).reuse_isolates,
        "the knob must reach ServiceKnobs"
    );

    let eff = cfg.service_effective(Some(Profile::MinLatency), &deno_reuse(false), &Plan::default());
    assert_eq!(
        (eff.reuse_isolates.value, eff.reuse_isolates.source),
        (false, Source::Service)
    );

    let eff = cfg.service_effective(Some(Profile::MinLatency), &ServiceConfig::default(), &Plan::default());
    assert_eq!(
        (eff.reuse_isolates.value, eff.reuse_isolates.source),
        (true, Source::Profile(Profile::MinLatency))
    );

    let mut node = Config::default();
    node.deno.reuse_isolates = Some(true);
    let eff = node.service_effective(None, &ServiceConfig::default(), &Plan::default());
    assert_eq!(
        (eff.reuse_isolates.value, eff.reuse_isolates.source),
        (true, Source::Config)
    );
    let eff = node.service_effective(None, &deno_reuse(false), &Plan::default());
    assert_eq!(
        (eff.reuse_isolates.value, eff.reuse_isolates.source),
        (false, Source::Service)
    );
}
