//! The shared path pattern syntax and the canonical form rate rules match on.

use rt_types::path_pattern::{canonical_path, MalformedPath, PathPattern};
use rt_types::{validate_rate_rules, RateBy, RateRule, MAX_RATE_RULES};

fn matches(pattern: &str, path: &str) -> bool {
    PathPattern::parse(pattern).expect("pattern").matches(path.as_bytes())
}

#[test]
fn the_pattern_table() {
    for (pattern, path, want) in [
        ("/", "/", true),
        ("/", "/a", false),
        ("/api/login", "/api/login", true),
        ("/api/login", "/api/login/x", false),
        ("/api/login", "/api", false),
        ("/api/login", "/API/login", false),
        ("/api/*", "/api/x", true),
        ("/api/*", "/api", false),
        ("/api/*", "/api/x/y", false),
        ("/a*z/b", "/abcz/b", true),
        ("/a*z/b", "/az/b", true),
        ("/a*z/b", "/a/b", false),
        ("/*.map", "/x.map", true),
        ("/users/:id", "/users/7", true),
        ("/users/:id", "/users/7/x", false),
        ("/users/:id/edit", "/users/7/edit", true),
        ("/assets/**", "/assets", true),
        ("/assets/**", "/assets/a/b", true),
        ("/assets/**", "/assetsx", false),
        ("/**/*.map", "/x.map", true),
        ("/**/*.map", "/a/b/x.map", true),
        ("/**/*.map", "/a/b/x.js", false),
        ("/a/**/z", "/a/z", true),
        ("/a/**/z", "/a/b/c/z", true),
        ("/a/**/z", "/a/b/c", false),
        ("/**/a/**/b", "/x/a/y/b", true),
        ("/**", "/", true),
        ("/**", "/anything/at/all", true),
    ] {
        assert_eq!(matches(pattern, path), want, "{pattern} on {path}");
    }
}

#[test]
fn folded_matching_ignores_ascii_case_only() {
    let p = PathPattern::parse("/Api/Login").expect("pattern");
    assert!(p.matches_folded(b"/api/LOGIN"));
    assert!(!p.matches_folded(b"/api/logins"));
}

#[test]
fn bad_patterns_are_refused() {
    for bad in [
        "", "api", "/a//b", "/a/", "/a/../b", "/./b", "/a\\b", "/%61", "/a***", "/**x", "/a*b*c", "/:", "/:1a",
        "/a\0b", "/a b",
    ] {
        assert!(PathPattern::parse(bad).is_err(), "{bad:?} should be refused");
    }
    assert!(PathPattern::parse(&format!("/{}", "a".repeat(512))).is_err());
    assert!(PathPattern::parse(&"/a".repeat(64)).is_err());
    assert!(PathPattern::parse("/**/a/**/b/**/c/**").is_err(), "too many `**`");
}

#[test]
fn hostile_backtracking_stays_bounded() {
    let p = PathPattern::parse("/**/a/**/a/**/b").unwrap();
    let long = "/a".repeat(8000);
    let t = std::time::Instant::now();
    assert!(!p.matches(long.as_bytes()));
    assert!(t.elapsed() < std::time::Duration::from_secs(1));
}

#[test]
fn canonical_forms() {
    for (raw, want) in [
        ("/", "/"),
        ("/api/login", "/api/login"),
        ("/api/login/", "/api/login"),
        ("//api//login", "/api/login"),
        ("/x/../api/login", "/api/login"),
        ("/./api/./login", "/api/login"),
        ("/../../api/login", "/api/login"),
        ("/api/%6cogin", "/api/login"),
        ("/%2e%2E/api/login", "/api/login"),
        ("/x/%2e%2e/api/login", "/api/login"),
        ("/x\\..\\api\\login", "/api/login"),
        ("/api%2flogin", "/api/login"),
        ("/api%5clogin", "/api/login"),
        ("/api/%256cogin", "/api/login"),
        ("/100%25", "/100%"),
    ] {
        let got = canonical_path(raw).expect(raw);
        assert_eq!(String::from_utf8_lossy(&got), want, "{raw}");
    }
}

#[test]
fn an_already_canonical_path_is_not_copied() {
    assert!(matches!(
        canonical_path("/api/login").unwrap(),
        std::borrow::Cow::Borrowed(_)
    ));
}

#[test]
fn malformed_paths_are_errors() {
    for bad in ["", "*", "api", "/a%", "/a%z1", "/a%00b", "/a%0ab", "/%25252525252e"] {
        assert_eq!(canonical_path(bad), Err(MalformedPath), "{bad:?}");
    }
}

fn rule(path: &str) -> RateRule {
    RateRule {
        path: path.into(),
        methods: vec!["POST".into()],
        rps: 5,
        burst: 10,
        by: RateBy::Ip,
    }
}

#[test]
fn rate_rule_validation() {
    assert!(validate_rate_rules(&[rule("/api/login")]).is_ok());
    assert!(validate_rate_rules(&vec![rule("/a"); MAX_RATE_RULES]).is_ok());
    let too_many = validate_rate_rules(&vec![rule("/a"); MAX_RATE_RULES + 1]).unwrap_err();
    assert!(too_many.contains("rate_rules"), "{too_many}");
    assert!(validate_rate_rules(&[rule("nope")]).is_err());
    let mut r = rule("/a");
    r.rps = 0;
    assert!(validate_rate_rules(&[r]).is_err());
    let mut r = rule("/a");
    r.rps = 1_000_001;
    assert!(validate_rate_rules(&[r]).is_err());
    let mut r = rule("/a");
    r.burst = 1_000_001;
    assert!(validate_rate_rules(&[r]).is_err());
    let mut r = rule("/a");
    r.methods = vec!["PO ST".into()];
    assert!(validate_rate_rules(&[r]).is_err());
}

#[test]
fn rate_rules_parse_additively() {
    let i: rt_types::IngressRules = serde_json::from_value(serde_json::json!({
        "rate_rules": [{"path": "/api/login", "methods": ["POST"], "rps": 5, "burst": 10, "by": "ip"}]
    }))
    .unwrap();
    assert_eq!(i.rate_rules[0].by, RateBy::Ip);
    // Absent `by` is the service; no rules are not written out.
    let i: rt_types::IngressRules =
        serde_json::from_value(serde_json::json!({"rate_rules": [{"path": "/a", "rps": 1}]})).unwrap();
    assert_eq!(i.rate_rules[0].by, RateBy::Service);
    let empty = serde_json::to_value(rt_types::IngressRules::default()).unwrap();
    assert!(empty.get("rate_rules").is_none());
    assert!(serde_json::from_value::<RateRule>(serde_json::json!({"path": "/a", "nope": 1})).is_err());
}
