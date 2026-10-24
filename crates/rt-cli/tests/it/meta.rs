//! `21ci whoami`, `plan list | get`, `node config get`, the admission view in `service stats` /
//! `stats` / `service get`, and `tenant usage` without an id.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn me_tenant() -> Value {
    json!({
        "principal": "tenant", "tenant_id": "ten_1", "key_id": "key_1",
        "scopes": ["services:read", "logs:read"], "project_ids": null, "environments": null,
        "plan": {"name": "pro", "settings": {"rps": 100}}, "version": "0.1.0"
    })
}

// ---- whoami ----------------------------------------------------------------------------------

#[test]
fn whoami_shows_the_principal_and_its_plan() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me").header("authorization", auth());
        then.status(200).json_body(me_tenant());
    });
    let out = ctx.run(&["whoami"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "Principal: tenant",
            "Tenant: ten_1",
            "Key: key_1",
            "Scopes: services:read,logs:read",
            "Projects: all",
            "Environments: all",
            "Plan: pro",
            "Node version: 0.1.0",
        ]
    );
    assert_eq!(ctx.run(&["--json", "whoami"]).ok().json(), me_tenant());
}

#[test]
fn whoami_as_the_operator_has_no_plan_line() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200).json_body(json!({
            "principal": "operator", "tenant_id": null, "key_id": null, "scopes": ["deploy"],
            "project_ids": null, "environments": null, "plan": null, "version": "0.1.0"
        }));
    });
    let lines = ctx.run(&["whoami"]).ok().lines();
    assert_eq!(lines[0], "Principal: operator");
    assert!(!lines.iter().any(|l| l.starts_with("Plan:")), "{lines:?}");
}

// ---- plans -----------------------------------------------------------------------------------

fn plans() -> Value {
    json!([
        {"name": "free", "settings": {"max_services": 10, "max_concurrency": 10, "rps": 50,
                                      "invocations_per_day": 100000, "storage_mb": 100, "custom_host_suffixes": [],
                                      "limits_max": {"cpu_ms": 1000}}},
        {"name": "pro", "settings": {"max_services": 1000, "max_concurrency": 0, "rps": 0,
                                     "invocations_per_day": 0, "storage_mb": 1024,
                                     "custom_host_suffixes": [".bots.example.com"], "limits_max": {"cpu_ms": 10000}}}
    ])
}

#[test]
fn plan_list_renders_the_main_quotas() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/plans").header("authorization", auth());
        then.status(200).json_body(plans());
    });
    let out = ctx.run(&["plan", "list"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "NAME SERVICES CONCURRENCY RPS INVOCATIONS/DAY STORAGE_MB HOST_SUFFIXES",
            "free 10 10 50 100000 100 -",
            "pro 1000 unlimited unlimited unlimited 1024 .bots.example.com",
        ]
    );
    assert_eq!(ctx.run(&["--json", "plan", "list"]).ok().json(), plans());
}

#[test]
fn plan_get_flattens_the_settings_or_is_not_found() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/plans");
        then.status(200).json_body(plans());
    });
    let lines = ctx.run(&["plan", "get", "pro"]).ok().lines();
    assert_eq!(lines[0], "Plan: pro");
    assert_eq!(lines[1], "KEY VALUE");
    assert!(lines.contains(&"limits_max.cpu_ms 10000".to_string()), "{lines:?}");
    assert!(
        lines.contains(&r#"custom_host_suffixes [".bots.example.com"]"#.to_string()),
        "{lines:?}"
    );
    assert_eq!(ctx.run(&["--json", "plan", "get", "pro"]).ok().json(), plans()[1]);
    let out = ctx.run(&["plan", "get", "gold"]).fails_with(1);
    assert!(out.stderr.contains("plan \"gold\" is not configured"), "{}", out.stderr);
}

#[test]
fn plans_need_the_operator_token() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/plans");
        then.status(403)
            .json_body(err_json("insufficient_scope", "operator token required"));
    });
    let out = ctx.run(&["plan", "list"]).fails_with(1);
    assert_eq!(
        out.stderr,
        "error: insufficient_scope: operator token required (HTTP 403)\n"
    );
}

// ---- node config get -------------------------------------------------------------------------

fn node_config() -> Value {
    json!({
        "node": {"default_profile": "balanced"},
        "sched": {"max_inflight": 0, "max_queued": 4096},
        "binding": [{"name": "billing", "inject_headers": {"authorization": "<redacted>"}}]
    })
}

#[test]
fn node_config_get_flattens_the_whole_config_a_section_or_a_key() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/config").header("authorization", auth());
        then.status(200).json_body(node_config());
    });
    let out = ctx.run(&["node", "config", "get"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "KEY VALUE",
            "node.default_profile balanced",
            "sched.max_inflight 0",
            "sched.max_queued 4096",
            r#"binding [{"name":"billing","inject_headers":{"authorization":"<redacted>"}}]"#,
        ]
    );
    assert_eq!(
        ctx.run(&["node", "config", "get", "sched"]).ok().lines(),
        vec!["KEY VALUE", "max_inflight 0", "max_queued 4096"]
    );
    assert_eq!(
        ctx.run(&["node", "config", "get", "sched.max_queued"]).ok().stdout,
        "4096\n"
    );
    assert_eq!(
        ctx.run(&["--json", "node", "config", "get", "sched"]).ok().json(),
        json!({"max_inflight": 0, "max_queued": 4096})
    );
    let out = ctx.run(&["node", "config", "get", "sched.nope"]).fails_with(1);
    assert!(out.stderr.contains("no config key `sched.nope`"), "{}", out.stderr);
    m.assert_calls(5);
}

// ---- admission view --------------------------------------------------------------------------

fn admission() -> Value {
    json!({
        "kind": "deno", "limit": 8, "min": 1, "max": 64, "running": 3, "queued": 2, "queued_bytes": 8192,
        "max_queued": 256, "queue_wait_ms": 12.5, "queue_timeouts": 1, "demand": 4.25,
        "latency_baseline_ms": 7.0, "warm_per_proc": 2, "idle_ms": 0
    })
}

fn stats_with(adm: Value) -> Value {
    json!({"count": 10, "errors": 0, "error_rate": 0.0, "outcomes": {"ok": 10}, "admission": adm})
}

fn instances_admission() -> Value {
    let mut a = admission();
    a["instances"] = json!(3);
    a["min_instances"] = json!(2);
    a["max_instances"] = json!(64);
    a["target_instances"] = json!(4);
    a["rps"] = json!(12.5);
    a["last_change"] = json!({"reason": "trend", "from": 4, "to": 8, "ago_ms": 3200});
    a
}

#[test]
fn service_stats_shows_the_instances_the_rate_and_the_last_change() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/stats");
        then.status(200).json_body(stats_with(instances_admission()));
    });
    let lines = ctx.run(&["service", "stats", "hello"]).ok().lines();
    for want in [
        "Concurrency: limit 8 (1..64), running 3, queued 2 (max 256)",
        "Instances: 3 running, min 2, max 64, warm target 4",
        "Autoscaler: demand 4.2, latency baseline 7.0 ms, warm 2/proc, 12.5 req/s, idle 0s",
        "Last change: trend, limit 4 -> 8, 3s ago",
    ] {
        assert!(lines.contains(&want.to_string()), "missing {want:?} in {lines:?}");
    }
    // a limit that never moved
    let mut never = instances_admission();
    never["last_change"] = Value::Null;
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/stats");
        then.status(200).json_body(stats_with(never));
    });
    let lines = ctx.run(&["service", "stats", "hello"]).ok().lines();
    assert!(lines.contains(&"Last change: none".to_string()), "{lines:?}");
}

#[test]
fn service_stats_shows_queueing_and_the_autoscaler() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/stats")
            .header("authorization", auth());
        then.status(200).json_body(stats_with(admission()));
    });
    let out = ctx.run(&["service", "stats", "hello"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "Window: 5m",
            "Requests: 10",
            "Errors: 0 (0.00%)",
            "Outcomes: ok=10",
            "Concurrency: limit 8 (1..64), running 3, queued 2 (max 256)",
            "Queue wait: 12.5 ms (timeouts 1)",
            "Autoscaler: demand 4.2, latency baseline 7.0 ms, warm 2/proc, idle 0s",
        ]
    );
    // `21ci stats` is the same command
    assert_eq!(ctx.run(&["stats", "hello"]).ok().lines(), out.lines());
}

/// The "Queue wait" row adds the predicted run time of the service version (`pred_ms`) and, when
/// the server has one, the background-lane estimate (`pred_background_ms`); `null` adds nothing.
#[test]
fn service_stats_shows_the_predicted_run_time() {
    for (pred, bg, want) in [
        (
            json!(4.3),
            json!(2100.0),
            "Queue wait: 12.5 ms (timeouts 1), predicted run 4.3 ms (background 2.1 s)",
        ),
        (
            json!(1500.0),
            Value::Null,
            "Queue wait: 12.5 ms (timeouts 1), predicted run 1.5 s",
        ),
        (Value::Null, json!(9.0), "Queue wait: 12.5 ms (timeouts 1)"),
    ] {
        let mut adm = admission();
        adm["pred_ms"] = pred;
        adm["pred_background_ms"] = bg;
        let ctx = Ctx::new();
        ctx.lookup("hello", "svc_1");
        ctx.server.mock(|when, then| {
            when.method(GET).path("/v1/services/svc_1/stats");
            then.status(200).json_body(stats_with(adm));
        });
        let lines = ctx.run(&["service", "stats", "hello"]).ok().lines();
        assert!(lines.contains(&want.to_string()), "missing {want:?} in {lines:?}");
    }
}

#[test]
fn stats_of_a_service_without_admission_state() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/stats");
        then.status(200).json_body(stats_with(Value::Null));
    });
    let lines = ctx.run(&["service", "stats", "svc_1"]).ok().lines();
    assert_eq!(
        lines.last().unwrap(),
        "Concurrency: - (no admission state: the service has not run recently)"
    );
}

#[test]
fn service_get_shows_the_concurrency_line() {
    let ctx = Ctx::new();
    let mut svc = service_json("svc_1", "hello");
    svc["status"]["admission"] = admission();
    svc["status"]["inflight"] = json!(3);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200).json_body(svc);
    });
    let lines = ctx.run(&["service", "get", "svc_1"]).ok().lines();
    assert!(
        lines.contains(&"Concurrency: limit 8 (1..64), running 3, queued 2 (max 256)".to_string()),
        "{lines:?}"
    );
    assert!(lines.contains(&"In flight: 3".to_string()), "{lines:?}");
}

// ---- tenant usage without an id --------------------------------------------------------------

#[test]
fn tenant_usage_defaults_to_the_tenant_of_the_credentials() {
    let ctx = Ctx::new();
    let me = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200).json_body(me_tenant());
    });
    let usage = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants/ten_1/usage")
            .header("authorization", auth());
        then.status(200).json_body(json!([]));
    });
    ctx.run(&["tenant", "usage"]).ok();
    me.assert();
    usage.assert();
}

#[test]
fn tenant_usage_without_an_id_needs_a_tenant() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200)
            .json_body(json!({"principal": "operator", "tenant_id": null, "scopes": [], "version": "0.1.0"}));
    });
    let out = ctx.run(&["tenant", "usage"]).fails_with(2);
    assert!(out.stderr.contains("pass the tenant id"), "{}", out.stderr);
}

/// CLINET-002: a redirect is never followed, so the credential is never re-sent elsewhere.
#[test]
fn clinet_002_redirects_are_not_followed() {
    let ctx = Ctx::new();
    let elsewhere = ctx.server.mock(|when, then| {
        when.path("/elsewhere");
        then.status(200).json_body(me_tenant());
    });
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(302).header("location", format!("{}/elsewhere", ctx.base()));
    });
    let out = ctx.run(&["whoami"]);
    m.assert();
    assert_ne!(out.code, 0, "a redirect is an error: {}", out.stdout);
    assert_eq!(elsewhere.calls(), 0, "the redirect target was never contacted");
}

/// CLINET-001: a credential sent over plain `http://` to a host that is not loopback is warned
/// about on stderr; a loopback `http://` (the default `127.0.0.1:8081`) is not.
#[test]
fn clinet_001_plain_http_beyond_loopback_is_warned_about() {
    let ctx = Ctx::new();
    let out = ctx
        .cmd()
        .env("RT_API", "http://control.invalid:8081")
        .args(["whoami"])
        .write_stdin("")
        .output()
        .expect("spawn");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("plain http"), "{stderr}");
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200).json_body(me_tenant());
    });
    let ok = ctx.run(&["whoami"]).ok();
    m.assert();
    assert!(!ok.stderr.contains("plain http"), "{}", ok.stderr);
}
