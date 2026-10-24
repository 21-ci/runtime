//! `rt_config::serde_duration`: the human duration syntax of `rt.toml` ("250ms", "10s", "1h";
//! a bare integer means seconds). Pure serde behavior, independent of the loader.

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Plain {
    #[serde(with = "rt_config::serde_duration")]
    d: Duration,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Opt {
    #[serde(default, with = "rt_config::serde_duration::opt")]
    d: Option<Duration>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Many {
    #[serde(with = "rt_config::serde_duration::vec")]
    d: Vec<Duration>,
}

fn plain(toml_value: &str) -> Result<Duration, toml::de::Error> {
    toml::from_str::<Plain>(&format!("d = {toml_value}")).map(|p| p.d)
}

#[test]
fn human_strings_parse_to_exact_durations() {
    let table = [
        ("\"250ms\"", Duration::from_millis(250)),
        ("\"10s\"", Duration::from_secs(10)),
        ("\"1m\"", Duration::from_secs(60)),
        ("\"1h\"", Duration::from_secs(3600)),
        ("\"600s\"", Duration::from_secs(600)),
        ("\"1m 30s\"", Duration::from_secs(90)),
        ("\"100us\"", Duration::from_micros(100)),
        ("\" 5s \"", Duration::from_secs(5)),
        ("\"2ms\"", Duration::from_millis(2)),
    ];
    for (input, want) in table {
        assert_eq!(plain(input).unwrap_or_else(|e| panic!("{input}: {e}")), want, "{input}");
    }
}

#[test]
fn a_bare_integer_means_seconds() {
    assert_eq!(plain("0").unwrap(), Duration::ZERO);
    assert_eq!(plain("60").unwrap(), Duration::from_secs(60));
    assert_eq!(plain("3600").unwrap(), Duration::from_secs(3600));
}

#[test]
fn malformed_durations_are_errors_that_quote_the_input() {
    for bad in [
        "\"ten seconds\"",
        "\"10 parsecs\"",
        "\"\"",
        "\"ms\"",
        "-5",
        "1.5",
        "true",
    ] {
        assert!(plain(bad).is_err(), "{bad} must be rejected");
    }
    let msg = plain("\"soon\"").unwrap_err().to_string();
    assert!(msg.contains("invalid duration") && msg.contains("soon"), "{msg}");
}

#[test]
fn serialization_round_trips_through_the_human_form() {
    for d in [
        Duration::ZERO,
        Duration::from_millis(1),
        Duration::from_millis(250),
        Duration::from_secs(1),
        Duration::from_secs(60),
        Duration::from_secs(600),
        Duration::from_secs(3600),
        Duration::from_millis(1500),
        Duration::from_micros(2_500),
    ] {
        let text = toml::to_string(&Plain { d }).unwrap();
        assert!(text.contains('"'), "durations are written as strings, got `{text}`");
        let back: Plain = toml::from_str(&text).unwrap_or_else(|e| panic!("{text}: {e}"));
        assert_eq!(back.d, d, "{text}");
    }
    assert_eq!(
        toml::to_string(&Plain {
            d: Duration::from_millis(250)
        })
        .unwrap()
        .trim(),
        "d = \"250ms\""
    );
    assert_eq!(
        toml::to_string(&Plain {
            d: Duration::from_secs(1)
        })
        .unwrap()
        .trim(),
        "d = \"1s\""
    );
}

#[test]
fn optional_durations_distinguish_unset_from_zero() {
    assert_eq!(toml::from_str::<Opt>("").unwrap().d, None);
    assert_eq!(
        toml::from_str::<Opt>("d = \"5s\"").unwrap().d,
        Some(Duration::from_secs(5))
    );
    assert_eq!(toml::from_str::<Opt>("d = 0").unwrap().d, Some(Duration::ZERO));
    assert!(toml::from_str::<Opt>("d = \"nope\"").is_err());
    // `None` is skipped by TOML (no null); JSON keeps it as null
    assert_eq!(
        serde_json::to_value(Opt { d: None }).unwrap(),
        serde_json::json!({"d": null})
    );
    assert_eq!(
        serde_json::to_value(Opt {
            d: Some(Duration::from_secs(2))
        })
        .unwrap(),
        serde_json::json!({"d": "2s"})
    );
}

#[test]
fn duration_lists_parse_in_order_and_fail_on_any_bad_element() {
    let m: Many = toml::from_str(r#"d = ["100ms", "1s", 5]"#).unwrap();
    assert_eq!(
        m.d,
        vec![
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(5)
        ]
    );
    assert!(toml::from_str::<Many>(r#"d = ["100ms", "later"]"#).is_err());
    assert_eq!(toml::from_str::<Many>("d = []").unwrap().d, Vec::<Duration>::new());
    let json = serde_json::to_value(Many {
        d: vec![Duration::from_millis(100), Duration::from_secs(5)],
    })
    .unwrap();
    assert_eq!(json, serde_json::json!({"d": ["100ms", "5s"]}));
}
