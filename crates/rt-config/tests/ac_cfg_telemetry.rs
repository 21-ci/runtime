//! `[telemetry]`: which URL wins, and what `telemetry.url` accepts.

use rt_config::{telemetry_url_with, valid_telemetry_url, TelemetryConfig};

const BUILT: Option<&str> = Some("https://built.example/t");

fn resolve(enabled: bool, url: &str, env: &[(&str, &str)], built: Option<&str>) -> Option<String> {
    let cfg = TelemetryConfig {
        enabled,
        url: url.to_string(),
    };
    let lookup = |k: &str| env.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).to_string());
    telemetry_url_with(&cfg, &lookup, built)
}

#[test]
fn no_url_anywhere_sends_nothing() {
    assert_eq!(resolve(true, "auto", &[], None), None);
    assert_eq!(resolve(true, "auto", &[], Some("")), None);
}

#[test]
fn auto_is_the_build_time_url_and_empty_turns_it_off() {
    assert_eq!(resolve(true, "auto", &[], BUILT).as_deref(), BUILT);
    assert_eq!(resolve(true, "", &[], BUILT), None);
    assert_eq!(
        resolve(true, "https://own.example", &[], BUILT).as_deref(),
        Some("https://own.example")
    );
}

#[test]
fn the_run_time_override_beats_the_config_and_can_turn_it_off() {
    let env = [("RT_TELEMETRY_URL", "https://env.example")];
    assert_eq!(
        resolve(true, "https://own.example", &env, BUILT).as_deref(),
        Some("https://env.example")
    );
    assert_eq!(resolve(true, "auto", &[("RT_TELEMETRY_URL", "")], BUILT), None);
    assert_eq!(
        resolve(true, "auto", &[("RT_TELEMETRY_URL", "http://env.example")], BUILT),
        None
    );
}

#[test]
fn disabled_and_do_not_track_beat_every_url() {
    let env = [("RT_TELEMETRY_URL", "https://env.example")];
    assert_eq!(resolve(false, "https://own.example", &env, BUILT), None);
    let dnt = [("RT_TELEMETRY_URL", "https://env.example"), ("DO_NOT_TRACK", "1")];
    assert_eq!(resolve(true, "https://own.example", &dnt, BUILT), None);
    let not_one = [("DO_NOT_TRACK", "0")];
    assert_eq!(resolve(true, "auto", &not_one, BUILT).as_deref(), BUILT);
}

#[test]
fn only_plain_https_urls_are_valid() {
    assert!(valid_telemetry_url("https://t.example/s/telemetry"));
    assert!(valid_telemetry_url("https://t.example:8443"));
    for bad in [
        "",
        "auto",
        "http://t.example",
        "wss://t.example",
        "https://",
        "https://:443",
        "https://u:p@t.example",
        "https://t.example?a=1",
        "https://t.example#f",
        "https://t.example/a b",
    ] {
        assert!(!valid_telemetry_url(bad), "{bad}");
    }
    assert!(!valid_telemetry_url(&format!("https://t.example/{}", "a".repeat(2048))));
}

#[test]
fn validation_names_the_key() {
    let mut cfg = rt_config::Config::default();
    cfg.telemetry.url = "http://t.example".into();
    let err = cfg.validate().expect_err("http is refused").to_string();
    assert!(err.contains("telemetry.url"), "{err}");
    for ok in ["", "auto", "https://t.example"] {
        cfg.telemetry.url = ok.into();
        cfg.validate().expect("accepted");
    }
}
