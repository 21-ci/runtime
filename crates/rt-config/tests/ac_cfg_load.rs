//! rt-config loading (docs/config.md, CFG-01..03, CFG-07): `rt.toml` + `RT__A__B`
//! environment overlay, strict unknown-key checking with key paths, invalid values, durations,
//! cross-field validation, secrets, `--print-config`.
//!
//! `Config::from_toml`, `Config::load`, `Config::validate` and `NodeSecrets::from_env` are
//! `todo!()` in the skeleton: tests that use them are expected to fail until implemented.

use std::time::Duration;

use proptest::prelude::*;
use rt_config::{default_config_path, Config, ConfigError, LoadOptions, NodeSecrets, Profile, Secret, SecurityConfig};

fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn load(toml: &str, pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
    Config::from_toml(toml, &env(pairs))
}

fn ok(toml: &str, pairs: &[(&str, &str)]) -> Config {
    load(toml, pairs).unwrap_or_else(|e| panic!("config should load: {e}\n--- toml ---\n{toml}"))
}

fn err_text(toml: &str, pairs: &[(&str, &str)]) -> String {
    match load(toml, pairs) {
        Ok(_) => panic!("config should be rejected:\n{toml}\nenv: {pairs:?}"),
        Err(e) => e.to_string(),
    }
}

// ---------------------------------------------------------------------------------------------
// Defaults and the documented reference file
// ---------------------------------------------------------------------------------------------

#[test]
fn empty_file_and_no_env_yield_the_builtin_balanced_configuration() {
    let cfg = ok("", &[]);
    assert_eq!(cfg, Config::default());
    assert_eq!(cfg.node.default_profile, Profile::Balanced);
    // profile-controlled knobs stay unset so the profile can fill them
    assert!(cfg.sched.spin_us.is_none() && cfg.memory.high.is_none() && cfg.cache.t1_share.is_none());
}

#[test]
fn the_sample_blocks_of_the_config_guide_load() {
    const DOC: &str = include_str!("../../../docs/config.md");
    let sections = DOC
        .split("## Configuration Sections")
        .nth(1)
        .expect("sections")
        .split("## Common Configuration Tasks")
        .next()
        .unwrap();
    // every ```toml fence of the section reference is one table or several; together they form one file
    let block: String = sections
        .split("```toml")
        .skip(1)
        .filter_map(|b| b.split("```").next())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        block.contains("[sched]") && block.contains("[memory]"),
        "toml fences not found in docs/config.md"
    );
    let cfg = ok(&block, &[]);
    assert_eq!(cfg.sched.processors, 0);
    assert_eq!(cfg.sched.lrq_capacity, Config::default().sched.lrq_capacity);
    assert_eq!(cfg.node.default_profile, Profile::Balanced);
}

#[test]
fn file_values_land_in_the_right_sections() {
    let cfg = ok(
        r#"
        [node]
        data_dir = "/var/rt"
        default_profile = "min_latency"
        [listeners]
        public = "127.0.0.1:9000"
        control = "127.0.0.1:9001"
        [routing]
        base_domain = "fn.example.com"
        [sched]
        processors = 6
        spare_machines = 3
        spin_us = 25
        time_slice = "7ms"
        pin_threads = true
        [sched.kinds.deno]
        min = 2
        max = 4
        [memory]
        budget_mb = 2048
        check_interval = "500ms"
        high = 0.75
        critical = 0.9
        [build]
        timeout = "90s"
        allowed_hosts = ["esm.sh"]
        [egress]
        denied_ports = [25, 2525]
        denied_cidrs = ["203.0.113.0/24", "2001:db8::/32"]
        [storage]
        blob_backend = "s3"
        [security]
        clock = "coarse"
        [supervisor]
        executor_restart_backoff = ["50ms", "2s"]
        [plans.pro]
        max_services = 100
        "#,
        &[],
    );
    assert_eq!(cfg.node.data_dir, "/var/rt");
    assert_eq!(cfg.node.default_profile, Profile::MinLatency);
    assert_eq!(cfg.listeners.public, "127.0.0.1:9000");
    assert_eq!(cfg.listeners.control, "127.0.0.1:9001");
    assert_eq!(
        cfg.listeners.metrics, "127.0.0.1:9090",
        "unset keys keep their defaults"
    );
    assert_eq!(cfg.routing.base_domain, "fn.example.com");
    assert_eq!(cfg.sched.processors, 6);
    assert_eq!(cfg.sched.spare_machines, 3);
    assert_eq!(cfg.sched.lrq_capacity, 256);
    assert_eq!(cfg.sched.spin_us, Some(25));
    assert_eq!(cfg.sched.time_slice, Some(Duration::from_millis(7)));
    assert_eq!(cfg.sched.pin_threads, Some(true));
    assert_eq!(cfg.sched.handoff_on_hog, None);
    assert_eq!((cfg.sched.kinds.deno.min, cfg.sched.kinds.deno.max), (2, 4));
    assert_eq!((cfg.sched.kinds.wasm.min, cfg.sched.kinds.wasm.max), (1, 0));
    assert_eq!(cfg.memory.budget_mb, 2048);
    assert_eq!(cfg.memory.check_interval, Duration::from_millis(500));
    assert_eq!((cfg.memory.high, cfg.memory.critical), (Some(0.75), Some(0.9)));
    assert_eq!(cfg.build.timeout, Duration::from_secs(90));
    assert_eq!(cfg.build.allowed_hosts, vec!["esm.sh".to_string()]);
    assert_eq!(cfg.egress.denied_ports, vec![25, 2525]);
    assert_eq!(
        cfg.egress.denied_cidrs,
        vec!["203.0.113.0/24".to_string(), "2001:db8::/32".to_string()]
    );
    assert_eq!(cfg.storage.blob_backend, rt_config::BlobBackendKind::S3);
    assert_eq!(cfg.security.clock, rt_config::ClockMode::Coarse);
    assert_eq!(
        cfg.supervisor.executor_restart_backoff,
        vec![Duration::from_millis(50), Duration::from_secs(2)]
    );
    assert_eq!(cfg.plan("pro").max_services, 100);
    assert_eq!(
        cfg.plan("pro").max_concurrency,
        rt_types::Plan::default().max_concurrency,
        "unset plan fields default"
    );
}

/// The `[[sdk]]` section (bundled and preloaded operator SDKs) was removed: refused with a
/// pointer to its replacement, never ignored.
#[test]
fn the_removed_sdk_section_is_a_clear_error() {
    let err = load(
        "[[sdk]]\nspecifier = \"sdk:acme\"\nsource = \"/opt/acme/mod.ts\"\npreload = true\n",
        &[],
    )
    .expect_err("[[sdk]] is refused");
    let text = err.to_string();
    assert!(matches!(err, ConfigError::Removed { .. }), "{text}");
    assert!(
        text.contains("[[sdk]]") && text.contains("21ci sdk publish") && text.contains("preload"),
        "{text}"
    );
}

#[test]
fn array_tables_binding_are_loaded() {
    let cfg = ok(
        r#"
        [[binding]]
        name = "voicelab"
        base_url = "https://api.voicelab.example"
        allowed_methods = ["POST"]
        allowed_paths = ["/v1/calls*"]
        [binding.inject_headers]
        authorization = "env:VOICELAB_TOKEN"
        [binding.rate_limit]
        rps = 5
        burst = 10
        "#,
        &[],
    );
    assert_eq!(cfg.binding.len(), 1);
    let b = &cfg.binding[0];
    assert_eq!(b.name, "voicelab");
    assert_eq!(b.allowed_methods, vec!["POST".to_string()]);
    assert_eq!(
        b.inject_headers.get("authorization").map(String::as_str),
        Some("env:VOICELAB_TOKEN")
    );
    assert_eq!((b.rate_limit.rps, b.rate_limit.burst), (5, 10));
}

// ---------------------------------------------------------------------------------------------
// Env overlay and precedence (CFG-03)
// ---------------------------------------------------------------------------------------------

#[test]
fn env_beats_file_beats_default_cfg_03() {
    // CFG-03: file processors=2, env RT__SCHED__PROCESSORS=3 -> 3
    assert_eq!(
        ok("[sched]\nprocessors = 2", &[("RT__SCHED__PROCESSORS", "3")])
            .sched
            .processors,
        3
    );
    assert_eq!(ok("[sched]\nprocessors = 2", &[]).sched.processors, 2);
    assert_eq!(ok("", &[("RT__SCHED__PROCESSORS", "3")]).sched.processors, 3);
    assert_eq!(ok("", &[]).sched.processors, 0);
    // the winning value is what the scheduler uses
    let cfg = ok("[sched]\nprocessors = 2", &[("RT__SCHED__PROCESSORS", "3")]);
    assert_eq!(cfg.resolve_processors(16), 3);
}

#[test]
fn env_overrides_one_key_without_touching_the_rest_of_the_file() {
    let cfg = ok(
        "[sched]\nprocessors = 2\nspare_machines = 5\n[listeners]\npublic = \"0.0.0.0:1\"",
        &[("RT__SCHED__PROCESSORS", "3"), ("RT__LISTENERS__PUBLIC", "0.0.0.0:2")],
    );
    assert_eq!(cfg.sched.processors, 3);
    assert_eq!(
        cfg.sched.spare_machines, 5,
        "file value not overridden by env must persist"
    );
    assert_eq!(cfg.listeners.public, "0.0.0.0:2");
    assert_eq!(cfg.listeners.control, "127.0.0.1:8081");
}

#[test]
fn env_names_map_to_dotted_keys_with_single_underscores_kept() {
    let cfg = ok(
        "",
        &[
            ("RT__SCHED__SPIN_US", "7"),
            ("RT__SCHED__TIME_SLICE", "15ms"),
            ("RT__SCHED__PIN_THREADS", "true"),
            ("RT__SCHED__KINDS__DENO__MIN", "2"),
            ("RT__MEMORY__HIGH", "0.75"),
            ("RT__MEMORY__CRITICAL", "0.9"),
            ("RT__NODE__DEFAULT_PROFILE", "min_latency"),
            ("RT__BUILD__COMPILE", "eager"),
            ("RT__STORAGE__BLOB_BACKEND", "s3"),
            ("RT__SECURITY__CLOCK", "coarse"),
            ("RT__LOGS__STDOUT", "true"),
            ("RT__LOGS__RING_LINES", "42"),
            ("RT__LISTENERS__PUBLIC", "127.0.0.1:7777"),
            ("RT__CACHE__IDLE_TTL", "90s"),
        ],
    );
    assert_eq!(cfg.sched.spin_us, Some(7));
    assert_eq!(cfg.sched.time_slice, Some(Duration::from_millis(15)));
    assert_eq!(cfg.sched.pin_threads, Some(true));
    assert_eq!(cfg.sched.kinds.deno.min, 2);
    assert_eq!((cfg.memory.high, cfg.memory.critical), (Some(0.75), Some(0.9)));
    assert_eq!(cfg.node.default_profile, Profile::MinLatency);
    assert_eq!(cfg.build.compile, Some(rt_types::CompileMode::Eager));
    assert_eq!(cfg.storage.blob_backend, rt_config::BlobBackendKind::S3);
    assert_eq!(cfg.security.clock, rt_config::ClockMode::Coarse);
    assert!(cfg.logs.stdout);
    assert_eq!(cfg.logs.ring_lines, 42);
    assert_eq!(cfg.listeners.public, "127.0.0.1:7777");
    assert_eq!(cfg.cache.idle_ttl, Some(Duration::from_secs(90)));
}

#[test]
fn environment_variables_outside_the_rt_double_underscore_namespace_are_ignored() {
    let cfg = ok(
        "",
        &[
            ("RT_ROOT_TOKEN", "secret-token"),
            ("RT_MASTER_KEY", "00"),
            ("RT_LOG", "debug"),
            ("RT_SCHEDTRACE", "1000"),
            ("RTX__SCHED__PROCESSORS", "9"),
            ("HOME", "/home/x"),
            ("PATH", "/usr/bin"),
            ("MY_RT__SCHED__PROCESSORS", "9"),
        ],
    );
    assert_eq!(cfg, Config::default());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn prop_env_always_wins_over_file(file_n in 0u32..10_000, env_n in 0u32..10_000) {
        let toml = format!("[sched]\nprocessors = {file_n}");
        let e = env_n.to_string();
        let cfg = Config::from_toml(&toml, &[("RT__SCHED__PROCESSORS".to_string(), e)]).expect("valid config");
        prop_assert_eq!(cfg.sched.processors, env_n);
    }

    #[test]
    fn prop_millisecond_durations_parse_exactly(ms in 1u64..1_000_000) {
        let toml = format!("[sysmon]\nmax_tick = \"{ms}ms\"");
        let cfg = Config::from_toml(&toml, &[]).expect("valid duration");
        prop_assert_eq!(cfg.sysmon.max_tick, Some(Duration::from_millis(ms)));
    }
}

// ---------------------------------------------------------------------------------------------
// Unknown keys (CFG-02) and invalid values: errors carry the key path
// ---------------------------------------------------------------------------------------------

#[test]
fn unknown_key_fails_with_its_full_path_cfg_02() {
    let e = load("[sched]\nprocesors = 2", &[]).unwrap_err();
    assert!(
        matches!(&e, ConfigError::UnknownKey(p) if p == "sched.procesors"),
        "got {e:?}"
    );
    assert_eq!(e.to_string(), "unknown key sched.procesors");
}

#[test]
fn unknown_keys_are_reported_with_their_path_at_every_depth() {
    let cases = [
        ("[sched.kinds.deno]\nmx = 3", "sched.kinds.deno.mx"),
        ("[plans.free]\nmax_servicez = 3", "plans.free.max_servicez"),
        ("[memory]\nbudget = 1", "memory.budget"),
        ("[listeners]\npublik = \"x\"", "listeners.publik"),
        ("totally_unknown = 1", "totally_unknown"),
    ];
    for (toml, path) in cases {
        match load(toml, &[]) {
            Err(ConfigError::UnknownKey(p)) => assert_eq!(p, path, "for\n{toml}"),
            other => panic!("expected UnknownKey({path}) for\n{toml}\ngot {other:?}"),
        }
    }
    // unknown top-level section: the path names at least the section
    let msg = err_text("[nope]\nx = 1", &[]);
    assert!(msg.contains("nope"), "{msg}");
    // unknown key inside an array table
    let msg = err_text("[[binding]]\nname = \"x\"\nbogus = 1", &[]);
    assert!(msg.contains("bogus"), "{msg}");
}

#[test]
fn unknown_key_in_env_overlay_is_an_error_naming_the_key() {
    for var in ["RT__SCHED__PROCESORS", "RT__NOPE__X", "RT__SCHED__KINDS__DENO__MX"] {
        let msg = err_text("", &[(var, "1")]);
        let path = var.trim_start_matches("RT__").to_ascii_lowercase().replace("__", ".");
        assert!(
            msg.contains(&path) || msg.contains(var),
            "error for {var} must name the key (`{path}`) or the variable, got: {msg}"
        );
    }
}

#[test]
fn secrets_never_live_in_the_file() {
    for toml in [
        "[secrets]\nroot_token = \"abc\"",
        "[node]\nroot_token = \"abc\"",
        "RT_ROOT_TOKEN = \"abc\"",
        "[security]\nmaster_key = \"00\"",
    ] {
        assert!(
            matches!(load(toml, &[]), Err(ConfigError::UnknownKey(_))),
            "file must not accept secrets:\n{toml}"
        );
    }
}

#[test]
fn invalid_value_names_the_key_and_the_expected_type() {
    let e = load("[sched]\nprocessors = \"many\"", &[]).unwrap_err();
    match &e {
        ConfigError::InvalidValue { key, reason } => {
            assert_eq!(key, "sched.processors");
            assert!(!reason.is_empty());
        }
        other => panic!("expected InvalidValue, got {other:?}"),
    }
    assert!(e.to_string().starts_with("invalid value for sched.processors"), "{e}");
}

#[test]
fn invalid_values_are_rejected_with_the_key_path() {
    let cases = [
        ("[sched]\nprocessors = -1", "sched.processors"),
        ("[sched]\ntime_slice = \"ten ms\"", "sched.time_slice"),
        ("[sched]\npin_threads = \"yes\"", "sched.pin_threads"),
        ("[node]\ndefault_profile = \"turbo\"", "node.default_profile"),
        ("[storage]\nblob_backend = \"gcs\"", "storage.blob_backend"),
        ("[security]\nclock = \"fast\"", "security.clock"),
        ("[memory]\nhigh = \"high\"", "memory.high"),
        ("[build]\ncompile = \"sometimes\"", "build.compile"),
        ("[egress]\ndenied_ports = [70000]", "egress.denied_ports"),
        ("[egress]\ndenied_ports = \"25\"", "egress.denied_ports"),
        (
            "[supervisor]\nexecutor_restart_backoff = [\"soon\"]",
            "supervisor.executor_restart_backoff",
        ),
        ("[cache]\nidle_ttl = true", "cache.idle_ttl"),
    ];
    for (toml, path) in cases {
        let msg = err_text(toml, &[]);
        assert!(msg.contains(path), "error for\n{toml}\nmust name `{path}`, got: {msg}");
    }
}

#[test]
fn invalid_env_values_are_rejected_naming_the_key_or_variable() {
    let cases = [
        ("RT__SCHED__PROCESSORS", "abc", "sched.processors"),
        ("RT__SCHED__PIN_THREADS", "maybe", "sched.pin_threads"),
        ("RT__SCHED__TIME_SLICE", "ten ms", "sched.time_slice"),
        ("RT__MEMORY__HIGH", "lots", "memory.high"),
        ("RT__NODE__DEFAULT_PROFILE", "turbo", "node.default_profile"),
    ];
    for (var, value, path) in cases {
        let msg = err_text("", &[(var, value)]);
        assert!(
            msg.contains(path) || msg.contains(var),
            "{var}={value}: error must name the key, got: {msg}"
        );
    }
}

#[test]
fn toml_syntax_errors_are_parse_errors() {
    for bad in [
        "[sched\nprocessors = 1",
        "processors = = 1",
        "[sched]\nprocessors = 1\nprocessors = 2",
    ] {
        assert!(
            matches!(load(bad, &[]), Err(ConfigError::Parse(m)) if !m.is_empty()),
            "for\n{bad}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Durations
// ---------------------------------------------------------------------------------------------

#[test]
fn durations_accept_human_strings_and_bare_seconds() {
    let cfg = ok(
        r#"
        [sysmon]
        max_tick = "250ms"
        [secrets]
        cache_ttl = 0
        [ingress]
        header_timeout = "10s"
        idle_timeout = "1m"
        [cache]
        idle_ttl = 600
        [rebalance]
        cooldown = "1500ms"
        interval = "2s"
        "#,
        &[],
    );
    assert_eq!(cfg.sysmon.max_tick, Some(Duration::from_millis(250)));
    assert_eq!(cfg.secrets.cache_ttl, Duration::ZERO, "0 = fetch per task over IPC");
    assert_eq!(cfg.ingress.header_timeout, Duration::from_secs(10));
    assert_eq!(cfg.ingress.idle_timeout, Duration::from_secs(60));
    assert_eq!(cfg.cache.idle_ttl, Some(Duration::from_secs(600)));
    assert_eq!(cfg.rebalance.cooldown, Duration::from_millis(1500));
    assert_eq!(cfg.rebalance.interval, Some(Duration::from_secs(2)));
}

// ---------------------------------------------------------------------------------------------
// Cross-field validation
// ---------------------------------------------------------------------------------------------

fn validation_messages(cfg: &Config) -> Vec<String> {
    match cfg.validate() {
        Err(ConfigError::Validation(v)) => v,
        other => panic!("expected ConfigError::Validation, got {other:?}"),
    }
}

/// Security review pass 2: a `budget_mb` whose bytes overflow u64 used to wrap to a tiny budget.
#[test]
fn memory_budget_mb_is_bounded() {
    let mut cfg = Config::default();
    cfg.memory.budget_mb = 17_592_186_044_416;
    let v = validation_messages(&cfg);
    assert!(v.iter().any(|m| m.starts_with("memory.budget_mb")), "{v:?}");
    cfg.memory.budget_mb = 64 * 1024;
    cfg.validate().expect("64 GiB is fine");
}

/// `sched.aging_factor` is at most 64 and `sched.min_run_time_max_ms` at most 24 h; one past
/// either edge is refused with the key named.
#[test]
fn aging_factor_and_min_run_time_max_bounds_name_the_key() {
    let mut cfg = Config::default();
    cfg.sched.aging_factor = 64;
    cfg.sched.min_run_time_max_ms = 86_400_000;
    cfg.validate().expect("the edges are accepted");
    cfg.sched.aging_factor = 65;
    cfg.sched.min_run_time_max_ms = 86_400_001;
    let v = validation_messages(&cfg);
    assert!(
        v.iter().any(|m| m.starts_with("sched.aging_factor must be <= 64")),
        "{v:?}"
    );
    assert!(
        v.iter()
            .any(|m| m.starts_with("sched.min_run_time_max_ms must be <= 86400000")),
        "{v:?}"
    );
}

/// `plans.<name>.max_topic_partitions` is within `1..=PARTITIONS_HARD_MAX` (16).
#[test]
fn plan_topic_partitions_are_bounded_by_the_hard_max() {
    let doc = |n: u32| format!("[plans.pro]\nmax_topic_partitions = {n}");
    Config::from_toml(&doc(16), &[])
        .expect("16 is the hard max")
        .validate()
        .expect("valid");
    Config::from_toml(&doc(1), &[]).expect("1").validate().expect("valid");
    for n in [0, 17] {
        let e = err_text(&doc(n), &[]);
        assert!(e.contains("plans.pro.max_topic_partitions"), "{n}: {e}");
    }
}

/// The event plan keys and the new trigger kinds parse from `[plans.<name>]`.
#[test]
fn event_plan_keys_and_trigger_kinds_parse() {
    let cfg = Config::from_toml(
        "[plans.pro]\nmax_async_pending = 5\nmax_topics = 6\nmax_topic_partitions = 7\n\
         max_stream_mb = 8\nmax_stream_retention_hours = 9\nallowed_triggers = [\"pubsub\", \"stream\"]\n\
         [plans.pro.limits_max]\nmin_run_time_ms = 1234",
        &[],
    )
    .expect("parses");
    cfg.validate().expect("valid");
    let p = &cfg.plans["pro"];
    assert_eq!(
        (
            p.max_async_pending,
            p.max_topics,
            p.max_topic_partitions,
            p.max_stream_mb,
            p.max_stream_retention_hours
        ),
        (5, 6, 7, 8, 9)
    );
    assert_eq!(
        p.allowed_triggers,
        vec![rt_types::TriggerKind::Pubsub, rt_types::TriggerKind::Stream]
    );
    assert_eq!(p.limits_max.min_run_time_ms, 1234);
}

#[test]
fn default_and_profile_configs_validate() {
    Config::default().validate().expect("built-in config is valid");
    for p in Profile::ALL {
        let mut cfg = Config::default();
        cfg.node.default_profile = p;
        cfg.validate().unwrap_or_else(|e| panic!("{p}: {e}"));
    }
}

#[test]
fn validate_rejects_inverted_or_out_of_range_watermarks() {
    let mut cfg = Config::default();
    cfg.memory.high = Some(0.95);
    cfg.memory.critical = Some(0.90);
    let msgs = validation_messages(&cfg);
    assert!(
        msgs.iter()
            .any(|m| m.contains("memory.high") && m.contains("memory.critical")),
        "{msgs:?}"
    );

    let mut cfg = Config::default();
    cfg.memory.critical = Some(1.5);
    assert!(validation_messages(&cfg).iter().any(|m| m.contains("memory.critical")));

    // a lone explicit value is judged against the profile value of the other
    let mut cfg = Config::default();
    cfg.memory.high = Some(0.95); // balanced critical is 0.92
    assert!(validation_messages(&cfg).iter().any(|m| m.contains("memory.high")));
}

#[test]
fn validate_checks_listener_syntax() {
    for good in [
        "0.0.0.0:8080",
        "127.0.0.1:1",
        "[::1]:8081",
        "unix:/data/rt.sock",
        "localhost:9090",
    ] {
        let mut cfg = Config::default();
        cfg.listeners.public = good.to_string();
        cfg.validate()
            .unwrap_or_else(|e| panic!("`{good}` should be valid: {e}"));
    }
    for bad in ["", "8080", "0.0.0.0", "host:port", "0.0.0.0:99999", "unix:"] {
        let mut cfg = Config::default();
        cfg.listeners.control = bad.to_string();
        let msgs = validation_messages(&cfg);
        assert!(
            msgs.iter().any(|m| m.contains("listeners.control")),
            "`{bad}` must be rejected naming listeners.control: {msgs:?}"
        );
    }
}

#[test]
fn ingress_backlog_defaults_to_4096_loads_from_file_and_env_and_must_be_positive() {
    assert_eq!(Config::default().ingress.backlog, 4096);
    assert_eq!(ok("[ingress]\nbacklog = 1024\n", &[]).ingress.backlog, 1024);
    assert_eq!(ok("", &[("RT__INGRESS__BACKLOG", "512")]).ingress.backlog, 512);
    let mut cfg = Config::default();
    cfg.ingress.backlog = 0;
    assert!(validation_messages(&cfg).iter().any(|m| m.contains("ingress.backlog")));
}

#[test]
fn validate_rejects_kind_minimums_exceeding_the_processors() {
    let mut cfg = Config::default();
    cfg.sched.processors = 2;
    cfg.sched.kinds.deno.min = 2;
    cfg.sched.kinds.wasm.min = 1;
    assert!(validation_messages(&cfg).iter().any(|m| m.contains("sched.kinds")));
    cfg.sched.kinds.deno.min = 1;
    cfg.validate().expect("1 + 1 <= 2 processors");
}

#[test]
fn validate_reports_every_problem_not_just_the_first() {
    let mut cfg = Config::default();
    cfg.memory.high = Some(0.99);
    cfg.memory.critical = Some(0.50);
    cfg.listeners.public = "nonsense".to_string();
    cfg.listeners.metrics = "also nonsense".to_string();
    let msgs = validation_messages(&cfg);
    assert!(msgs.len() >= 3, "expected >= 3 problems, got {msgs:?}");
    let joined = ConfigError::Validation(msgs).to_string();
    assert!(
        joined.contains("listeners.public") && joined.contains("listeners.metrics") && joined.contains("memory."),
        "{joined}"
    );
}

#[test]
fn validate_rejects_plan_references_that_do_not_exist() {
    let mut cfg = Config::default();
    cfg.binding.push(rt_config::BindingConfig {
        name: "voicelab".into(),
        allowed_plans: vec!["no-such-plan".into()],
        ..Default::default()
    });
    assert!(validation_messages(&cfg).iter().any(|m| m.contains("no-such-plan")));
    cfg.binding[0].allowed_plans = vec!["free".into()];
    cfg.validate().expect("the free plan always exists");
}

// ---------------------------------------------------------------------------------------------
// resolve_processors (CFG-07) and plan lookup
// ---------------------------------------------------------------------------------------------

#[test]
fn processors_explicit_wins_zero_means_available_never_below_one() {
    let table = [
        // (configured, available, expected)
        (4u32, 16u32, 4u32),
        (4, 2, 4),
        (0, 4, 4), // CFG-07: container --cpus=4, processors=0
        (0, 8, 8),
        (0, 1, 1),
        (0, 0, 1),
    ];
    for (configured, available, expected) in table {
        let mut cfg = Config::default();
        cfg.sched.processors = configured;
        assert_eq!(
            cfg.resolve_processors(available),
            expected,
            "processors={configured} available={available}"
        );
    }
}

#[test]
fn plan_lookup_uses_configured_plans_and_falls_back_to_the_free_defaults() {
    let cfg = ok(
        "[plans.pro]\nmax_services = 100\nrps = 500\n[plans.free]\nmax_services = 3",
        &[],
    );
    assert_eq!((cfg.plan("pro").max_services, cfg.plan("pro").rps), (100, 500));
    assert_eq!(
        cfg.plan("free").max_services,
        3,
        "an explicit [plans.free] overrides the built-in"
    );
    assert_eq!(cfg.plan("nonexistent"), rt_types::Plan::default());
    assert_eq!(Config::default().plan("free"), rt_types::Plan::default());
    assert!(
        Config::default().plans.contains_key("free"),
        "a `free` plan always exists"
    );
}

// ---------------------------------------------------------------------------------------------
// --print-config (CFG-01)
// ---------------------------------------------------------------------------------------------

#[test]
fn print_config_fills_every_profile_knob_and_round_trips() {
    // (profile, spin_us, time_slice ms, high, critical, t1_share)
    let expect = [
        (Profile::MaxDensity, 0u64, 20u64, 0.90f64, 0.97f64, 0.10f64),
        (Profile::Balanced, 10, 10, 0.80, 0.92, 0.20),
        (Profile::MinLatency, 50, 5, 0.70, 0.88, 0.35),
    ];
    for (p, spin, slice_ms, high, critical, t1) in expect {
        let mut cfg = Config::default();
        cfg.node.default_profile = p;
        let printed = cfg.print().expect("print");
        let back =
            Config::from_toml(&printed, &[]).unwrap_or_else(|e| panic!("printed config must reload: {e}\n{printed}"));
        assert_eq!(back.node.default_profile, p);
        assert_eq!(back.sched.spin_us, Some(spin), "{p}");
        assert_eq!(back.sched.time_slice, Some(Duration::from_millis(slice_ms)), "{p}");
        assert_eq!(
            (back.memory.high, back.memory.critical),
            (Some(high), Some(critical)),
            "{p}"
        );
        assert_eq!(back.cache.t1_share, Some(t1), "{p}");
        // idempotent: printing the reloaded config changes nothing
        assert_eq!(back.print().unwrap(), printed, "{p}");
    }
}

#[test]
fn print_config_keeps_explicit_values_over_profile_values() {
    let mut cfg = Config::default();
    cfg.sched.spin_us = Some(99);
    cfg.memory.high = Some(0.5);
    cfg.memory.critical = Some(0.6);
    let printed = cfg.print().unwrap();
    let back = Config::from_toml(&printed, &[]).unwrap();
    assert_eq!(back.sched.spin_us, Some(99));
    assert_eq!((back.memory.high, back.memory.critical), (Some(0.5), Some(0.6)));
}

// ---------------------------------------------------------------------------------------------
// Config::load: files, env, secrets
// ---------------------------------------------------------------------------------------------

fn write_file(dir: &tempfile::TempDir, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, content).unwrap();
    path
}

#[test]
fn load_reads_the_file_and_reports_where_it_came_from() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "rt.toml", "[sched]\nprocessors = 2\n[logs]\nretention_days = 3\n");
    let loaded = Config::load(&LoadOptions {
        file: Some(path.clone()),
        env: vec![],
    })
    .expect("load");
    assert_eq!(loaded.config.sched.processors, 2);
    assert_eq!(loaded.config.logs.retention_days, 3);
    assert_eq!(loaded.file.as_deref(), Some(path.as_path()));
    assert!(loaded.env_overrides.is_empty());
}

#[test]
fn load_applies_env_over_the_file_and_lists_the_overridden_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "rt.toml", "[sched]\nprocessors = 2\n");
    let loaded = Config::load(&LoadOptions {
        file: Some(path),
        env: env(&[
            ("RT__SCHED__PROCESSORS", "3"),
            ("RT__LOGS__STDOUT", "true"),
            ("UNRELATED", "x"),
        ]),
    })
    .expect("load");
    assert_eq!(loaded.config.sched.processors, 3);
    assert!(loaded.config.logs.stdout);
    let mut keys = loaded.env_overrides.clone();
    keys.sort();
    assert_eq!(keys, vec!["logs.stdout".to_string(), "sched.processors".to_string()]);
}

#[test]
fn load_without_a_file_uses_the_defaults() {
    let loaded = Config::load(&LoadOptions::default()).expect("load without file");
    assert_eq!(loaded.config, Config::default());
    assert!(loaded.file.is_none());
}

#[test]
fn load_fails_on_an_explicit_file_that_does_not_exist() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.toml");
    match Config::load(&LoadOptions {
        file: Some(missing.clone()),
        env: vec![],
    }) {
        Err(ConfigError::Io { path, .. }) => assert_eq!(path, missing),
        other => panic!("expected Io error, got {other:?}"),
    }
}

#[test]
fn load_reports_unknown_keys_in_the_file_with_their_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "rt.toml", "[sched]\nprocesors = 2\n");
    let e = Config::load(&LoadOptions {
        file: Some(path),
        env: vec![],
    })
    .unwrap_err();
    assert_eq!(e.to_string(), "unknown key sched.procesors");
}

#[test]
fn load_validates_the_merged_result() {
    // each source is fine alone; the combination (file high, env critical) is inverted
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "rt.toml", "[memory]\nhigh = 0.9\ncritical = 0.95\n");
    let e = Config::load(&LoadOptions {
        file: Some(path),
        env: env(&[("RT__MEMORY__CRITICAL", "0.5")]),
    })
    .unwrap_err();
    assert!(matches!(e, ConfigError::Validation(_)), "got {e:?}");
}

const MASTER_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

#[test]
fn secrets_come_from_env_and_are_redacted_in_debug() {
    let loaded = Config::load(&LoadOptions {
        file: None,
        env: env(&[("RT_ROOT_TOKEN", "root-token-value"), ("RT_MASTER_KEY", MASTER_HEX)]),
    })
    .unwrap();
    assert_eq!(loaded.secrets.root_token.as_ref().unwrap().expose(), "root-token-value");
    assert_eq!(loaded.secrets.master_key.as_ref().unwrap().expose(), MASTER_HEX);
    let dbg = format!("{loaded:?}");
    assert!(
        !dbg.contains("root-token-value") && !dbg.contains(MASTER_HEX),
        "Debug leaked a secret: {dbg}"
    );
    assert!(dbg.contains("redacted"));
    // no secrets configured: both None (control routes then answer 503 control_disabled in release)
    let none = Config::load(&LoadOptions::default()).unwrap();
    assert!(none.secrets.root_token.is_none() && none.secrets.master_key.is_none());
}

#[test]
fn secret_files_are_read_and_trailing_newlines_trimmed() {
    let dir = tempfile::tempdir().unwrap();
    let tok = write_file(&dir, "root.token", "file-token\n");
    let key = write_file(&dir, "master.key", &format!("{MASTER_HEX}\n"));
    let s = NodeSecrets::from_env(
        &env(&[
            ("RT_ROOT_TOKEN_FILE", tok.to_str().unwrap()),
            ("RT_MASTER_KEY_FILE", key.to_str().unwrap()),
        ]),
        &SecurityConfig::default(),
    )
    .unwrap();
    assert_eq!(s.root_token.unwrap().expose(), "file-token");
    assert_eq!(s.master_key.unwrap().expose(), MASTER_HEX);

    let missing = dir.path().join("absent");
    let e = NodeSecrets::from_env(
        &env(&[("RT_ROOT_TOKEN_FILE", missing.to_str().unwrap())]),
        &SecurityConfig::default(),
    );
    assert!(
        matches!(e, Err(ConfigError::Secret { .. })),
        "unreadable *_FILE must be an error, got {e:?}"
    );
}

#[test]
fn master_key_may_come_from_security_master_key_file() {
    let dir = tempfile::tempdir().unwrap();
    let key = write_file(&dir, "master.key", MASTER_HEX);
    let sec = SecurityConfig {
        master_key_file: key.to_str().unwrap().to_string(),
        ..SecurityConfig::default()
    };
    let s = NodeSecrets::from_env(&[], &sec).unwrap();
    assert_eq!(s.master_key.unwrap().expose(), MASTER_HEX);
}

#[test]
fn malformed_master_keys_are_rejected() {
    let bad_keys: Vec<String> = vec![
        "abc".into(),
        MASTER_HEX[..62].into(),
        format!("{MASTER_HEX}00"),
        "zz".repeat(32),
    ];
    for bad in &bad_keys {
        let e = NodeSecrets::from_env(&env(&[("RT_MASTER_KEY", bad.as_str())]), &SecurityConfig::default());
        match e {
            Err(ConfigError::Secret { name, .. }) => assert!(name.contains("MASTER"), "{name}"),
            other => panic!("master key `{bad}` must be rejected, got {other:?}"),
        }
    }
}

/// Security review pass 2: `LoadOptions` holds the process env (root token, master key); its
/// `Debug` shows names only, so a stray `?opts` in a log line leaks nothing.
#[test]
fn load_options_debug_shows_no_env_values() {
    let opts = LoadOptions {
        file: None,
        env: env(&[("RT_ROOT_TOKEN", "tok-value-123"), ("RT_MASTER_KEY", MASTER_HEX)]),
    };
    let shown = format!("{opts:?}");
    assert!(
        shown.contains("RT_ROOT_TOKEN") && !shown.contains("tok-value-123") && !shown.contains(MASTER_HEX),
        "{shown}"
    );
}

#[test]
fn previous_master_key_is_optional_and_validated_like_the_master_key() {
    // Key rotation (SECURITY.md SEC-53): RT_MASTER_KEY_PREVIOUS[_FILE] only decrypts.
    let none = NodeSecrets::from_env(&env(&[("RT_MASTER_KEY", MASTER_HEX)]), &SecurityConfig::default()).expect("ok");
    assert!(none.master_key_previous.is_none());
    let prev = "ab".repeat(32);
    let s = NodeSecrets::from_env(
        &env(&[("RT_MASTER_KEY", MASTER_HEX), ("RT_MASTER_KEY_PREVIOUS", prev.as_str())]),
        &SecurityConfig::default(),
    )
    .expect("ok");
    assert_eq!(s.master_key_previous.expect("previous").expose(), prev);
    match NodeSecrets::from_env(&env(&[("RT_MASTER_KEY_PREVIOUS", "abc")]), &SecurityConfig::default()) {
        Err(ConfigError::Secret { name, .. }) => assert_eq!(name, "RT_MASTER_KEY_PREVIOUS"),
        other => panic!("a malformed previous key must be rejected, got {other:?}"),
    }
}

#[test]
fn secret_value_is_redacted_but_exposable() {
    let s = Secret::new("sensitive-value");
    assert_eq!(s.expose(), "sensitive-value");
    let dbg = format!("{s:?}");
    assert!(dbg.contains("redacted") && !dbg.contains("sensitive"), "{dbg}");
    let cloned = s.clone();
    assert_eq!(cloned.expose(), "sensitive-value");
}

// ---------------------------------------------------------------------------------------------
// Runtime-reloadable keys point at real config keys
// ---------------------------------------------------------------------------------------------

#[test]
fn live_and_reload_keys_are_real_config_paths() {
    let tree = serde_json::to_value(Config::default()).unwrap();
    let lookup = |path: &str| path.split('.').try_fold(&tree, |v, seg| v.get(seg)).is_some();
    for key in Config::LIVE_KEYS {
        assert!(lookup(key), "LIVE_KEYS entry `{key}` is not a key of Config");
    }
    for key in Config::RELOAD_KEYS {
        assert!(lookup(key), "RELOAD_KEYS entry `{key}` is not a section of Config");
    }
    // live keys are exactly the hot-swappable knobs (the cache settings are fixed at
    // start, so `cache.t1_share` / `cache.idle_ttl` are not live)
    let docs_live = [
        "sched.spin_us",
        "sched.time_slice",
        "sched.handoff_on_hog",
        "sched.max_inflight",
        "sched.max_queued",
        "sched.max_queued_mb",
        "sched.queue_timeout",
        "sysmon.max_tick",
        "rebalance.interval",
        "deno.max_bound_per_m",
        "memory.high",
        "memory.critical",
    ];
    let mut live: Vec<&str> = Config::LIVE_KEYS.to_vec();
    live.sort_unstable();
    let mut want = docs_live.to_vec();
    want.sort_unstable();
    assert_eq!(live, want);
}

/// `apply_live_keys` copies exactly the `LIVE_KEYS` (what `SIGHUP` applies to a running node).
#[test]
fn apply_live_keys_copies_every_live_key_and_nothing_else() {
    let file =
        "[sched]\nspin_us = 7\ntime_slice = \"3ms\"\nhandoff_on_hog = true\nmax_inflight = 77\nmax_queued = 99\n\
                max_queued_mb = 12\nqueue_timeout = \"9s\"\nprocessors = 3\n[sysmon]\nmax_tick = \"4ms\"\n[rebalance]\n\
                interval = \"70ms\"\n[deno]\nmax_bound_per_m = 9\n[memory]\nhigh = 0.5\ncritical = 0.6\n";
    let from = Config::from_toml(file, &[]).expect("load");
    let mut cur = Config::default();
    cur.apply_live_keys(&from);
    let (a, b) = (
        serde_json::to_value(&cur).unwrap(),
        serde_json::to_value(&from).unwrap(),
    );
    let get = |v: &serde_json::Value, path: &str| {
        path.split('.')
            .try_fold(v.clone(), |v, seg| v.get(seg).cloned())
            .unwrap()
    };
    for key in Config::LIVE_KEYS {
        assert_eq!(get(&a, key), get(&b, key), "{key} was not applied");
        assert_ne!(
            get(&a, key),
            get(&serde_json::to_value(Config::default()).unwrap(), key),
            "{key}: test value equals the default"
        );
    }
    assert_eq!(
        cur.sched.processors,
        Config::default().sched.processors,
        "a restart-only key is untouched"
    );
}

// ---------------------------------------------------------------------------------------------
// Blocking pool and service-database knobs (B3.1–B3.3)
// ---------------------------------------------------------------------------------------------

#[test]
fn blocking_threads_default_to_auto_which_is_at_least_four_and_one_per_core() {
    let cfg = ok("", &[]);
    assert_eq!(cfg.blocking.threads, 0, "0 = auto");
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    assert_eq!(cfg.blocking.effective_threads(), cores.max(4));
    assert_eq!(
        ok("[blocking]\nthreads = 2\n", &[]).blocking.effective_threads(),
        2,
        "an explicit value wins"
    );
}

#[test]
fn storage_reader_pool_and_page_cache_budget_have_defaults_and_load() {
    let cfg = ok("", &[]);
    assert_eq!((cfg.storage.readers_per_db, cfg.storage.sql_cache_mb), (4, 512));
    let cfg = ok("[storage]\nreaders_per_db = 0\nsql_cache_mb = 64\n", &[]);
    assert_eq!((cfg.storage.readers_per_db, cfg.storage.sql_cache_mb), (0, 64));
    assert!(err_text("[storage]\nreaders_per_db = 65\n", &[]).contains("storage.readers_per_db"));
}

/// CFG-004: a plan whose per-service maxima are zero would fail every task: refused at load.
#[test]
fn cfg_004_zero_plan_maxima_are_refused() {
    for (field, set) in [
        (
            "limits_max.wall_ms",
            (|p: &mut rt_types::Plan| p.limits_max.wall_ms = 0) as fn(&mut rt_types::Plan),
        ),
        ("limits_max.cpu_ms", |p| p.limits_max.cpu_ms = 0),
        ("limits_max.memory_mb", |p| p.limits_max.memory_mb = 0),
    ] {
        let mut cfg = Config::default();
        set(cfg.plans.get_mut("free").expect("free plan"));
        let v = validation_messages(&cfg);
        assert!(
            v.iter().any(|m| m.starts_with(&format!("plans.free.{field}"))),
            "{field}: {v:?}"
        );
    }
}

/// GOV-005: memory watermarks are fractions with `0 < high < critical <= 1`: NaN, 1.5, a high
/// above critical and a zero high (every tick would be High) are refused.
#[test]
fn gov_005_memory_watermarks_are_validated() {
    for (high, critical, key) in [
        (Some(f64::NAN), None, "memory.high"),
        (None, Some(1.5), "memory.critical"),
        (Some(0.97), Some(0.9), "memory.high"),
        (Some(0.0), Some(0.9), "memory.high"),
    ] {
        let mut cfg = Config::default();
        cfg.memory.high = high;
        cfg.memory.critical = critical;
        let v = validation_messages(&cfg);
        assert!(v.iter().any(|m| m.starts_with(key)), "{high:?}/{critical:?}: {v:?}");
    }
    let mut cfg = Config::default();
    cfg.memory.high = Some(0.8);
    cfg.memory.critical = Some(0.95);
    cfg.validate().expect("sane watermarks");
}

/// CFG-005: `sched.max_inflight` is at least 2, so one run slot can stay reserved for nested
/// service calls (ADM-003); 1 is refused with the key named.
#[test]
fn cfg_005_max_inflight_leaves_room_for_the_call_reserve() {
    let mut cfg = Config::default();
    cfg.sched.max_inflight = 1;
    let v = validation_messages(&cfg);
    assert!(v.iter().any(|m| m.starts_with("sched.max_inflight")), "{v:?}");
    cfg.sched.max_inflight = 2;
    cfg.validate().expect("2 is the minimum");
}

// ---------------------------------------------------------------------------------------------
// Env overlay: paths, values and refusals
// ---------------------------------------------------------------------------------------------

#[test]
fn env_overrides_reach_configured_plans_and_refuse_array_tables() {
    assert_eq!(
        ok("", &[("RT__PLANS__PRO__MAX_SERVICES", "5")])
            .plan("pro")
            .max_services,
        5
    );
    for var in ["RT__BINDING__NAME", "RT__BINDING"] {
        let msg = err_text("", &[(var, "x")]);
        assert!(
            msg.contains("array tables ([[binding]]) cannot be set from the environment"),
            "{var}: {msg}"
        );
    }
}

#[test]
fn env_paths_past_a_scalar_name_the_full_key() {
    let msg = err_text("", &[("RT__NODE__DEFAULT_PROFILE__X", "1")]);
    assert!(msg.contains("node.default_profile.x"), "{msg}");
}

#[test]
fn env_names_with_an_empty_segment_are_refused() {
    let msg = err_text("", &[("RT__SCHED__", "1")]);
    assert!(msg.contains("empty key segment"), "{msg}");
}

#[test]
fn env_cannot_write_through_a_scalar_in_the_file() {
    let msg = err_text("[plans]\npro = 1", &[("RT__PLANS__PRO__MAX_SERVICES", "5")]);
    assert!(msg.contains("`pro` is not a table in the config file"), "{msg}");
}

#[test]
fn env_bracketed_and_comma_lists_fill_list_keys() {
    let bracketed = ok("", &[("RT__INGRESS__TRUSTED_PROXIES", "[\"10.0.0.0/8\"]")]);
    assert_eq!(bracketed.ingress.trusted_proxies, vec!["10.0.0.0/8".to_string()]);
    let comma = ok("", &[("RT__INGRESS__TRUSTED_PROXIES", "10.0.0.0/8, 192.168.0.0/16")]);
    assert_eq!(
        comma.ingress.trusted_proxies,
        vec!["10.0.0.0/8".to_string(), "192.168.0.0/16".to_string()]
    );
    // a bracket that is not TOML, and an empty value, are both kept as text and refused for a list
    for raw in ["[not toml", ""] {
        let msg = err_text("", &[("RT__INGRESS__TRUSTED_PROXIES", raw)]);
        assert!(msg.contains("ingress.trusted_proxies"), "{raw:?}: {msg}");
    }
}

#[test]
fn an_invalid_array_table_row_is_named_by_index_and_key() {
    let msg = err_text("[[binding]]\nname = \"a\"\nrate_limit = 5", &[]);
    assert!(msg.contains("binding[0].rate_limit"), "{msg}");
}

#[test]
fn a_table_where_a_scalar_belongs_is_a_parse_error() {
    assert!(matches!(
        load("[node]\ndefault_profile = {}", &[]),
        Err(ConfigError::Parse(_))
    ));
}

#[test]
fn validation_names_bad_cidrs_and_inverted_kind_bounds() {
    for (toml, want) in [
        (
            "[ingress]\ntrusted_proxies = [\"nonsense\"]",
            "ingress.trusted_proxies[0]: `nonsense` is not an IP address or CIDR",
        ),
        (
            "[egress]\ndenied_cidrs = [\"10.0.0.0/33\"]",
            "egress.denied_cidrs[0]: `10.0.0.0/33`: prefix length must be 0..=32",
        ),
        (
            "[sched.kinds.wasm]\nmin = 2\nmax = 1",
            "sched.kinds.wasm: max (1) is below min (2)",
        ),
    ] {
        let msg = err_text(toml, &[]);
        assert!(msg.contains(want), "{toml}: {msg}");
    }
}

#[test]
fn default_config_path_lives_in_the_data_dir_and_process_env_is_captured() {
    assert_eq!(
        default_config_path(std::path::Path::new("/var/rt")),
        std::path::PathBuf::from("/var/rt/rt.toml")
    );
    let opts = LoadOptions::default().with_process_env();
    assert!(opts.env.iter().any(|(k, _)| k == "PATH"));
}

// ---------------------------------------------------------------------------------------------
// Node secrets from files
// ---------------------------------------------------------------------------------------------

fn secrets_err(pairs: &[(&str, &str)], security: &SecurityConfig) -> String {
    match NodeSecrets::from_env(&env(pairs), security) {
        Ok(_) => panic!("secrets should be refused: {pairs:?}"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn secret_files_are_read_and_refused_when_empty_doubled_or_unreadable() {
    let dir = tempfile::TempDir::new().unwrap();
    let token = write_file(&dir, "token", "tok-123\n");
    let empty = write_file(&dir, "empty", "\n");
    let token_path = token.display().to_string();
    let empty_path = empty.display().to_string();
    let missing = dir.path().join("missing").display().to_string();
    assert!(NodeSecrets::from_env(&env(&[("RT_ROOT_TOKEN_FILE", &token_path)]), &SecurityConfig::default()).is_ok());
    assert!(secrets_err(&[("RT_ROOT_TOKEN_FILE", &empty_path)], &SecurityConfig::default()).contains("is empty"));
    let doubled = secrets_err(
        &[("RT_ROOT_TOKEN", "a"), ("RT_ROOT_TOKEN_FILE", &token_path)],
        &SecurityConfig::default(),
    );
    assert!(
        doubled.contains("both RT_ROOT_TOKEN and RT_ROOT_TOKEN_FILE are set"),
        "{doubled}"
    );
    let unreadable = SecurityConfig {
        master_key_file: missing,
        ..Default::default()
    };
    let msg = secrets_err(&[], &unreadable);
    assert!(msg.contains("security.master_key_file: cannot read"), "{msg}");
    let empty_key = SecurityConfig {
        master_key_file: empty_path,
        ..Default::default()
    };
    assert!(
        secrets_err(&[], &empty_key).contains("security.master_key_file: `"),
        "empty master key file is named"
    );
}
