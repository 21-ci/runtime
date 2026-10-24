//! `config/schema.json` is the machine-readable description of every operator-facing key
//! (used by `make menuconfig` / `validate-config`). These tests parse it at test time and hold the
//! Rust loader to it: every key accepted (file and env), bounds and enums enforced, defaults and
//! per-profile defaults equal to `rt_config`'s tables, typos rejected with their path, and every
//! shipped defconfig loadable.
//!
//! The loader entry points are `todo!()` in the skeleton: loader-backed tests fail until implemented.

use std::time::Duration;

use rt_config::profile::{node_knobs, service_knobs};
use rt_config::{Config, ConfigError};
use rt_types::{Profile, ScalingConfig};
use serde_json::Value;

const SCHEMA: &str = include_str!("../../../config/schema.json");
const MIB: u64 = 1024 * 1024;

#[derive(Clone, Debug)]
struct Key {
    section: String,
    name: String,
    ty: String,
    default: Value,
    spec: Value,
}

impl Key {
    fn path(&self) -> String {
        format!("{}.{}", self.section, self.name)
    }
    /// `Some("node" | "service")` for profile-controlled knobs.
    fn profile_scope(&self) -> Option<&str> {
        self.spec.get("profile_scoped").and_then(Value::as_str)
    }
}

fn schema() -> Value {
    serde_json::from_str(SCHEMA).expect("config/schema.json is valid JSON")
}

/// Every key of every section, in file order.
fn all_keys() -> Vec<Key> {
    let schema = schema();
    let mut out = Vec::new();
    for (section, body) in schema.as_object().expect("schema is an object") {
        let keys = body
            .get("keys")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("section {section} has no `keys`"));
        for (name, spec) in keys {
            out.push(Key {
                section: section.clone(),
                name: name.clone(),
                ty: spec["type"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{section}.{name} has no type"))
                    .to_string(),
                default: spec
                    .get("default")
                    .cloned()
                    .unwrap_or_else(|| panic!("{section}.{name} has no default")),
                spec: spec.clone(),
            });
        }
    }
    assert!(out.len() > 60, "schema looks truncated ({} keys)", out.len());
    out
}

/// Keys that live in `rt.toml`. `scaling` is the per-service block (`ServiceConfig.scaling`), not a
/// node config section; `plans` has no fixed keys.
fn rt_toml_keys() -> Vec<Key> {
    all_keys().into_iter().filter(|k| k.section != "scaling").collect()
}

fn toml_value(v: &Value, ty: &str) -> toml::Value {
    match ty {
        "int" => toml::Value::Integer(v.as_i64().expect("int")),
        "float" => toml::Value::Float(v.as_f64().expect("float")),
        "bool" => toml::Value::Boolean(v.as_bool().expect("bool")),
        "string" | "enum" | "duration" => toml::Value::String(v.as_str().expect("string").to_string()),
        "list" => toml::Value::Array(
            v.as_array()
                .expect("list")
                .iter()
                .map(|e| match e {
                    Value::String(s) => toml::Value::String(s.clone()),
                    Value::Number(n) => toml::Value::Integer(n.as_i64().expect("integer list element")),
                    other => panic!("unsupported list element {other}"),
                })
                .collect(),
        ),
        other => panic!("unknown schema type `{other}`"),
    }
}

fn one_key_doc(section: &str, name: &str, value: &toml::Value) -> String {
    format!("[{section}]\n{name} = {value}\n")
}

fn load(toml: &str) -> Result<Config, ConfigError> {
    Config::from_toml(toml, &[])
}

// ---------------------------------------------------------------------------------------------
// Acceptance
// ---------------------------------------------------------------------------------------------

#[test]
fn the_schema_covers_every_documented_section() {
    let schema = schema();
    for section in [
        "node",
        "listeners",
        "routing",
        "sched",
        "sysmon",
        "rebalance",
        "io",
        "blocking",
        "memory",
        "cache",
        "disk",
        "deno",
        "wasm",
        "build",
        "deploy",
        "storage",
        "s3",
        "security",
        "supervisor",
        "secrets",
        "ingress",
        "egress",
        "queue",
        "logs",
        "metrics",
        "otel",
        "plans",
        "scaling",
    ] {
        assert!(schema.get(section).is_some(), "schema lacks section `{section}`");
    }
}

#[test]
fn every_schema_key_is_accepted_by_the_loader_with_its_default_value() {
    let mut failures = Vec::new();
    for k in rt_toml_keys() {
        let doc = one_key_doc(&k.section, &k.name, &toml_value(&k.default, &k.ty));
        if let Err(e) = load(&doc) {
            failures.push(format!("{}: {e}", k.path()));
        }
    }
    assert!(
        failures.is_empty(),
        "keys rejected by the loader:\n{}",
        failures.join("\n")
    );
}

#[test]
fn a_file_containing_every_schema_default_at_once_loads() {
    let mut doc = String::new();
    let mut current = String::new();
    for k in rt_toml_keys() {
        if k.section != current {
            doc.push_str(&format!("\n[{}]\n", k.section));
            current = k.section.clone();
        }
        doc.push_str(&format!("{} = {}\n", k.name, toml_value(&k.default, &k.ty)));
    }
    load(&doc).unwrap_or_else(|e| panic!("{e}\n{doc}"));
}

#[test]
fn schema_defaults_of_plain_keys_equal_the_builtin_defaults() {
    // Only keys that are not profile-controlled: those are `Option` in Config and covered below.
    let mut doc = String::new();
    let mut current = String::new();
    for k in rt_toml_keys()
        .into_iter()
        .filter(|k| k.profile_scope().is_none() && k.section != "plans")
    {
        if k.section != current {
            doc.push_str(&format!("\n[{}]\n", k.section));
            current = k.section.clone();
        }
        doc.push_str(&format!("{} = {}\n", k.name, toml_value(&k.default, &k.ty)));
    }
    let cfg = load(&doc).unwrap_or_else(|e| panic!("{e}\n{doc}"));
    assert_eq!(
        cfg,
        Config::default(),
        "schema defaults must describe Config::default()"
    );
}

#[test]
fn every_scalar_schema_key_is_accepted_through_the_env_overlay_and_equals_the_file_form() {
    for k in rt_toml_keys() {
        let text = match (&k.ty[..], &k.default) {
            ("int", v) => v.as_i64().unwrap().to_string(),
            ("float", v) => format!("{}", v.as_f64().unwrap()),
            ("bool", v) => v.as_bool().unwrap().to_string(),
            ("string" | "enum" | "duration", Value::String(s)) if !s.is_empty() => s.clone(),
            _ => continue, // lists and empty strings have no portable env form
        };
        let var = format!("RT__{}__{}", k.section.to_uppercase(), k.name.to_uppercase());
        let from_env =
            Config::from_toml("", &[(var.clone(), text.clone())]).unwrap_or_else(|e| panic!("{var}={text}: {e}"));
        let from_file = load(&one_key_doc(&k.section, &k.name, &toml_value(&k.default, &k.ty))).unwrap();
        assert_eq!(
            from_env,
            from_file,
            "{var}={text} must mean the same as the file key {}",
            k.path()
        );
    }
}

#[test]
fn every_schema_enum_value_is_accepted_and_anything_else_is_rejected_with_the_key_path() {
    for k in rt_toml_keys().into_iter().filter(|k| k.ty == "enum") {
        let values: Vec<String> = k.spec["enum"]
            .as_array()
            .expect("enum list")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(!values.is_empty());
        for v in &values {
            let doc = one_key_doc(&k.section, &k.name, &toml::Value::String(v.clone()));
            load(&doc).unwrap_or_else(|e| panic!("{}={v}: {e}", k.path()));
        }
        let bad = one_key_doc(
            &k.section,
            &k.name,
            &toml::Value::String("definitely-not-a-valid-value".into()),
        );
        let msg = load(&bad).expect_err("unknown enum value must be rejected").to_string();
        assert!(
            msg.contains(&k.path()),
            "error for {} must name the key: {msg}",
            k.path()
        );
    }
}

#[test]
fn scaling_keys_are_the_per_service_block_not_node_config() {
    for k in all_keys().into_iter().filter(|k| k.section == "scaling") {
        let obj = serde_json::json!({ k.name.clone(): k.default.clone() });
        serde_json::from_value::<ScalingConfig>(obj).unwrap_or_else(|e| panic!("scaling.{}: {e}", k.name));
        // and `rt.toml` has no [scaling] section
        let doc = one_key_doc("scaling", &k.name, &toml_value(&k.default, &k.ty));
        assert!(
            matches!(load(&doc), Err(ConfigError::UnknownKey(_))),
            "[scaling] must not be an rt.toml section"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Strictness
// ---------------------------------------------------------------------------------------------

#[test]
fn a_typo_of_any_schema_key_is_rejected_as_an_unknown_key_with_its_path() {
    for k in rt_toml_keys() {
        let typo = format!("{}_typo", k.name);
        let doc = format!("[{}]\n{typo} = 1\n", k.section);
        match load(&doc) {
            Err(ConfigError::UnknownKey(p)) => assert_eq!(p, format!("{}.{typo}", k.section)),
            other => panic!("typo `{}.{typo}` must be UnknownKey, got {other:?}", k.section),
        }
    }
}

#[test]
fn every_config_section_is_in_the_schema_and_vice_versa() {
    let schema = schema();
    let cfg = serde_json::to_value(Config::default()).unwrap();
    for section in cfg.as_object().unwrap().keys() {
        if section == "binding" {
            continue; // `[[binding]]` is an array table, documented in docs/config.md
        }
        assert!(
            schema.get(section).is_some(),
            "Config section `{section}` is missing from schema.json"
        );
    }
    for section in schema.as_object().unwrap().keys() {
        if section == "scaling" {
            continue;
        }
        assert!(
            cfg.get(section).is_some(),
            "schema section `{section}` is not a Config section"
        );
    }
}

#[test]
fn every_schema_key_exists_in_the_config_struct_and_option_means_profile_controlled() {
    let cfg = serde_json::to_value(Config::default()).unwrap();
    for k in rt_toml_keys().into_iter().filter(|k| k.section != "plans") {
        let v = cfg
            .get(&k.section)
            .and_then(|s| s.get(&k.name))
            .unwrap_or_else(|| panic!("Config has no field {}", k.path()));
        assert_eq!(
            v.is_null(),
            k.profile_scope().is_some(),
            "{}: profile-controlled knobs must be Option (unset) in Config::default(), plain keys must not",
            k.path()
        );
    }
}

#[test]
fn schema_bounds_are_enforced_naming_the_key() {
    let mut checked = 0;
    for k in rt_toml_keys() {
        let (min, max) = (
            k.spec.get("min").and_then(Value::as_f64),
            k.spec.get("max").and_then(Value::as_f64),
        );
        let below = match (k.ty.as_str(), min) {
            ("int", Some(m)) if m >= 1.0 => Some(toml::Value::Integer(m as i64 - 1)),
            ("float", Some(m)) => Some(toml::Value::Float(m - 0.5)),
            _ => None,
        };
        let above = match (k.ty.as_str(), max) {
            ("int", Some(m)) => Some(toml::Value::Integer(m as i64 + 1)),
            ("float", Some(m)) => Some(toml::Value::Float(m + 0.5)),
            _ => None,
        };
        for (what, v) in [("below min", below), ("above max", above)] {
            let Some(v) = v else { continue };
            let doc = one_key_doc(&k.section, &k.name, &v);
            let msg = match load(&doc) {
                Ok(_) => panic!("{} = {v} ({what}) must be rejected", k.path()),
                Err(e) => e.to_string(),
            };
            assert!(
                msg.contains(&k.path()),
                "error for {} = {v} must name the key: {msg}",
                k.path()
            );
            checked += 1;
        }
    }
    assert!(
        checked >= 30,
        "expected the schema to carry many bounds, only {checked} checked"
    );
}

#[test]
fn schema_bounds_are_inclusive() {
    for k in rt_toml_keys() {
        // memory watermarks are cross-checked against each other; covered in ac_cfg_load.rs
        if k.section == "memory" && (k.name == "high" || k.name == "critical") {
            continue;
        }
        let at_min = match (k.ty.as_str(), k.spec.get("min").and_then(Value::as_f64)) {
            ("int", Some(m)) => Some(toml::Value::Integer(m as i64)),
            ("float", Some(m)) => Some(toml::Value::Float(m)),
            _ => None,
        };
        let at_max = match (k.ty.as_str(), k.spec.get("max").and_then(Value::as_f64)) {
            ("float", Some(m)) => Some(toml::Value::Float(m)),
            ("int", Some(m)) => Some(toml::Value::Integer(m as i64)),
            _ => None,
        };
        for v in [at_min, at_max].into_iter().flatten() {
            load(&one_key_doc(&k.section, &k.name, &v))
                .unwrap_or_else(|e| panic!("{} = {v} is within bounds: {e}", k.path()));
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Profile defaults in the schema agree with the Rust tables (and so with docs/config.md)
// ---------------------------------------------------------------------------------------------

enum Exp {
    Int(u64),
    Float(f64),
    Bool(bool),
    Dur(Duration),
    Str(&'static str),
}

/// Expected schema value of a profile-controlled key, from the Rust profile tables.
fn expected(section: &str, name: &str, p: Profile) -> Exp {
    let n = node_knobs(p);
    let s = service_knobs(p);
    match (section, name) {
        ("sched", "spin_us") => Exp::Int(n.spin_us),
        ("sched", "time_slice") => Exp::Dur(n.time_slice),
        ("sched", "pin_threads") => Exp::Bool(n.pin_threads),
        ("sched", "handoff_on_hog") => Exp::Bool(n.handoff_on_hog),
        ("sysmon", "max_tick") => Exp::Dur(n.sysmon_max_tick),
        ("rebalance", "interval") => Exp::Dur(n.rebalance_interval),
        ("deno", "max_bound_per_m") => Exp::Int(u64::from(n.deno_max_bound_per_m)),
        ("deno", "isolate_pool_per_p") => Exp::Int(u64::from(n.deno_isolate_pool_per_p)),
        ("memory", "high") => Exp::Float(n.memory_high),
        ("memory", "critical") => Exp::Float(n.memory_critical),
        ("memory", "purge_min_interval") => Exp::Dur(n.memory_purge_min_interval),
        ("cache", "t1_share") => Exp::Float(n.cache_t1_share),
        // the schema states this knob in MB, the Rust table in bytes
        ("wasm", "linear_memory_keep_resident") => Exp::Int(n.wasm_linear_memory_keep_resident / MIB),
        ("wasm", "max_unused_warm_slots") => Exp::Int(u64::from(n.wasm_max_unused_warm_slots)),
        ("sched", "warm_share") => Exp::Float(rt_config::profile::warm_share(p)),
        ("sched", "queue_timeout") => Exp::Dur(s.queue_timeout),
        ("cache", "idle_ttl") => Exp::Dur(s.cache_idle_ttl),
        ("cache", "preload") => Exp::Bool(s.cache_preload),
        ("build", "compile") => Exp::Str(match s.build_compile {
            rt_types::CompileMode::Lazy => "lazy",
            rt_types::CompileMode::Eager => "eager",
        }),
        ("wasm", "reuse_instances") => Exp::Bool(s.wasm_reuse_instances),
        ("deno", "reuse_isolates") => Exp::Bool(s.deno_reuse_isolates),
        ("scaling", "keep_resident") => Exp::Bool(s.scaling_keep_resident),
        other => panic!("profile-controlled schema key {other:?} has no entry in the Rust profile tables"),
    }
}

fn matches(exp: &Exp, v: &Value) -> bool {
    match exp {
        Exp::Int(n) => v.as_u64() == Some(*n),
        Exp::Float(f) => v.as_f64().is_some_and(|x| (x - f).abs() < 1e-12),
        Exp::Bool(b) => v.as_bool() == Some(*b),
        Exp::Dur(d) => v.as_str().and_then(|s| humantime::parse_duration(s).ok()) == Some(*d),
        Exp::Str(s) => v.as_str() == Some(*s),
    }
}

#[test]
fn schema_profile_defaults_equal_the_rust_profile_tables_for_every_profile() {
    let mut checked = 0;
    for k in all_keys().into_iter().filter(|k| k.profile_scope().is_some()) {
        let table = k
            .spec
            .get("profile_defaults")
            .unwrap_or_else(|| panic!("{} is profile-scoped but has no profile_defaults", k.path()));
        for p in Profile::ALL {
            let got = table
                .get(p.as_str())
                .unwrap_or_else(|| panic!("{}: no `{}` profile default", k.path(), p));
            assert!(
                matches(&expected(&k.section, &k.name, p), got),
                "{} / {p}: schema says {got}, profile table differs",
                k.path()
            );
            checked += 1;
        }
        // the schema's plain `default` is the balanced value
        assert!(
            matches(&expected(&k.section, &k.name, Profile::Balanced), &k.default),
            "{}: `default` must be the balanced value",
            k.path()
        );
    }
    assert_eq!(
        checked,
        22 * 3,
        "22 profile-controlled knobs x 3 profiles (docs/config.md table)"
    );
}

#[test]
fn profile_scope_in_the_schema_matches_the_docs_node_vs_service_split() {
    let node: Vec<String> = all_keys()
        .iter()
        .filter(|k| k.profile_scope() == Some("node"))
        .map(Key::path)
        .collect();
    let service: Vec<String> = all_keys()
        .iter()
        .filter(|k| k.profile_scope() == Some("service"))
        .map(Key::path)
        .collect();
    for want in [
        "sched.spin_us",
        "sched.time_slice",
        "sched.pin_threads",
        "sched.handoff_on_hog",
        "sysmon.max_tick",
        "rebalance.interval",
        "deno.max_bound_per_m",
        "deno.isolate_pool_per_p",
        "memory.high",
        "memory.critical",
        "memory.purge_min_interval",
        "cache.t1_share",
        "wasm.linear_memory_keep_resident",
        "wasm.max_unused_warm_slots",
        "sched.warm_share",
    ] {
        assert!(
            node.iter().any(|p| p == want),
            "{want} must be node scope; node keys: {node:?}"
        );
    }
    for want in [
        "sched.queue_timeout",
        "cache.idle_ttl",
        "cache.preload",
        "build.compile",
        "wasm.reuse_instances",
        "scaling.keep_resident",
        "deno.reuse_isolates",
    ] {
        assert!(
            service.iter().any(|p| p == want),
            "{want} must be service scope; service keys: {service:?}"
        );
    }
    assert_eq!(node.len() + service.len(), 22);
}

// ---------------------------------------------------------------------------------------------
// Shipped defconfigs
// ---------------------------------------------------------------------------------------------

fn defconfigs() -> Vec<(String, String)> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/defconfigs");
    let mut out: Vec<(String, String)> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{dir}: {e}"))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "toml"))
        .map(|p| {
            (
                p.file_stem().unwrap().to_string_lossy().into_owned(),
                std::fs::read_to_string(&p).unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

#[test]
fn every_shipped_defconfig_loads_and_validates() {
    let all = defconfigs();
    let names: Vec<&str> = all.iter().map(|(n, _)| n.as_str()).collect();
    for want in ["balanced", "dev", "edge-1gb", "max_density", "min_latency"] {
        assert!(names.contains(&want), "missing defconfig {want}: {names:?}");
    }
    for (name, text) in &all {
        let cfg = load(text).unwrap_or_else(|e| panic!("defconfig {name}: {e}"));
        cfg.validate()
            .unwrap_or_else(|e| panic!("defconfig {name} fails validation: {e}"));
    }
}

#[test]
fn profile_defconfigs_select_their_profile_and_state_the_profile_table_values() {
    for p in Profile::ALL {
        let (_, text) = defconfigs()
            .into_iter()
            .find(|(n, _)| n == p.as_str())
            .unwrap_or_else(|| panic!("no defconfig named {p}"));
        let cfg = load(&text).unwrap_or_else(|e| panic!("{p}: {e}"));
        assert_eq!(cfg.node.default_profile, p, "{p}.toml must select its own profile");
        let n = node_knobs(p);
        let k = cfg.node_knobs();
        assert_eq!(k.spin_us.value, n.spin_us, "{p} spin_us");
        assert_eq!(k.time_slice.value, n.time_slice, "{p} time_slice");
        assert_eq!(k.pin_threads.value, n.pin_threads, "{p} pin_threads");
        assert_eq!(k.handoff_on_hog.value, n.handoff_on_hog, "{p} handoff_on_hog");
        assert_eq!(k.sysmon_max_tick.value, n.sysmon_max_tick, "{p} sysmon.max_tick");
        assert_eq!(
            k.rebalance_interval.value, n.rebalance_interval,
            "{p} rebalance.interval"
        );
        assert_eq!(
            k.deno_max_bound_per_m.value, n.deno_max_bound_per_m,
            "{p} max_bound_per_m"
        );
        assert_eq!(
            k.deno_isolate_pool_per_p.value, n.deno_isolate_pool_per_p,
            "{p} isolate_pool_per_p"
        );
        assert_eq!(k.memory_high.value, n.memory_high, "{p} memory.high");
        assert_eq!(k.memory_critical.value, n.memory_critical, "{p} memory.critical");
        assert_eq!(
            k.memory_purge_min_interval.value, n.memory_purge_min_interval,
            "{p} purge_min_interval"
        );
        assert_eq!(k.cache_t1_share.value, n.cache_t1_share, "{p} t1_share");
        assert_eq!(
            k.wasm_linear_memory_keep_resident.value, n.wasm_linear_memory_keep_resident,
            "{p} keep_resident (file is MB)"
        );

        let s = service_knobs(p);
        let e = cfg.service_effective(None, &Default::default(), &Default::default());
        assert_eq!(e.queue_timeout.value, s.queue_timeout, "{p} queue_timeout");
        assert_eq!(e.idle_ttl.value, s.cache_idle_ttl, "{p} idle_ttl");
        assert_eq!(e.preload.value, s.cache_preload, "{p} preload");
        assert_eq!(e.compile.value, s.build_compile, "{p} compile");
        assert_eq!(e.reuse_instances.value, s.wasm_reuse_instances, "{p} reuse_instances");
        assert_eq!(e.reuse_isolates.value, s.deno_reuse_isolates, "{p} reuse_isolates");
    }
}
