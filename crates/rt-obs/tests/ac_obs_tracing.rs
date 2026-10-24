//! System logs: `tracing` JSON to stdout filtered by `RT_LOG`.
//!
//! The global subscriber can be installed once per process, so this file holds exactly one test
//! (a separate test binary).

use rt_obs::{init_tracing, ObsError, TracingOptions};

#[test]
fn the_global_subscriber_installs_once_and_a_second_install_is_an_error() {
    // `RT_LOG` must not be set by the harness for the fallback filter to apply.
    std::env::remove_var("RT_LOG");
    let opts = TracingOptions::default();
    assert_eq!(opts.default_filter, "info");
    assert!(opts.json, "system logs are JSON lines by default");

    init_tracing(&opts).expect("first installation succeeds");
    tracing::info!(target: "rt::test", "system log line");

    match init_tracing(&opts) {
        Err(ObsError::Tracing(msg)) => assert!(!msg.is_empty()),
        other => panic!("second installation must fail with ObsError::Tracing, got {other:?}"),
    }
}
