//! `Retry-After` handling (exit 4, `--retry`, `x-rt-reason`), `--json`, and `21ci doctor`.
use crate::common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::*;
use httpmock::prelude::*;
use httpmock::HttpMockResponse;
use serde_json::json;

const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

fn reply(status: u16, retry_after: Option<&str>, body: &str) -> HttpMockResponse {
    let mut b = HttpMockResponse::builder()
        .status(status)
        .header("content-type", "application/json");
    if let Some(r) = retry_after {
        b = b.header("retry-after", r);
    }
    b.body(body.to_string()).build()
}

/// `GET /v1/tenants/ten_1` answering `busy` (503 + `Retry-After: 1`) `n` times, then the tenant.
fn busy_then_ok(ctx: &Ctx, n: usize) -> httpmock::Mock<'_> {
    let calls = Arc::new(AtomicUsize::new(0));
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants/ten_1");
        then.respond_with(move |_| {
            if calls.fetch_add(1, Ordering::SeqCst) < n {
                reply(503, Some("1"), &err_json("overloaded", "the node is busy").to_string())
            } else {
                reply(
                    200,
                    None,
                    &json!({"id": "ten_1", "name": "acme", "plan": "free", "overrides": {}, "created_at": T0_MS, "disabled": false})
                        .to_string(),
                )
            }
        });
    })
}

// ---- Retry-After -----------------------------------------------------------------------------

#[test]
fn a_busy_answer_shows_retry_after_and_exits_4() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants/ten_1");
        then.status(429)
            .header("retry-after", "3")
            .json_body(err_json("rate_limited", "too many requests"));
    });
    let out = ctx.run(&["tenant", "get", "ten_1"]).fails_with(4);
    assert_eq!(
        out.stderr,
        "error: rate_limited: too many requests (HTTP 429, retry after 3s)\n"
    );
    let out = ctx.run(&["--json", "tenant", "get", "ten_1"]).fails_with(4);
    let e: serde_json::Value = serde_json::from_str(out.stderr.trim()).unwrap();
    assert_eq!(e["error"]["retry_after"], 3);
    assert_eq!(e["error"]["status"], 429);
}

#[test]
fn a_503_without_retry_after_is_a_plain_failure() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants/ten_1");
        then.status(503)
            .json_body(err_json("control_disabled", "set RT_ROOT_TOKEN"));
    });
    let out = ctx.run(&["--retry", "3", "tenant", "get", "ten_1"]).fails_with(1);
    m.assert_calls(1); // nothing says when to come back: no retry
    assert_eq!(out.stderr, "error: control_disabled: set RT_ROOT_TOKEN (HTTP 503)\n");
}

#[test]
fn retry_waits_for_retry_after_and_then_succeeds() {
    let ctx = Ctx::new();
    let m = busy_then_ok(&ctx, 1);
    let out = ctx.run(&["--retry", "2", "tenant", "get", "ten_1"]).ok();
    m.assert_calls(2);
    assert!(
        out.stderr
            .contains("HTTP 503 overloaded: retrying in 1s (attempt 1 of 2)"),
        "{}",
        out.stderr
    );
    assert!(out.lines().contains(&"Name: acme".to_string()), "{:?}", out.lines());
}

#[test]
fn retry_gives_up_after_n_attempts_and_rt_retry_sets_it() {
    let ctx = Ctx::new();
    let m = busy_then_ok(&ctx, usize::MAX);
    let out = ctx
        .cmd()
        .env("RT_RETRY", "1")
        .args(["tenant", "get", "ten_1"])
        .output()
        .unwrap();
    m.assert_calls(2);
    assert_eq!(out.status.code(), Some(4));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.ends_with("error: overloaded: the node is busy (HTTP 503, retry after 1s)\n"),
        "{stderr}"
    );
}

#[test]
fn a_long_retry_after_is_reported_at_once() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/queue");
        then.status(429)
            .header("retry-after", "3600")
            .json_body(err_json("quota_exceeded", "daily invocations used up"));
    });
    let out = ctx
        .run(&["--retry", "5", "queue", "send", "svc_1", "--body", "{}"])
        .fails_with(4);
    m.assert_calls(1);
    assert!(out.stderr.contains("(HTTP 429, retry after 1h)"), "{}", out.stderr);
}

#[test]
fn a_non_json_error_takes_its_code_from_x_rt_reason() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants/ten_1");
        then.status(503)
            .header("content-type", "text/plain")
            .header("x-rt-reason", "route_stale")
            .header("retry-after", "1")
            .body("route_stale");
    });
    let out = ctx.run(&["tenant", "get", "ten_1"]).fails_with(4);
    assert_eq!(
        out.stderr,
        "error: route_stale: route_stale (HTTP 503, retry after 1s)\n"
    );
}

// ---- invoke ----------------------------------------------------------------------------------

fn ctx_with_invoke() -> Ctx {
    let mut ctx = Ctx::new();
    ctx.invoke_base = ctx.base();
    ctx
}

#[test]
fn invoke_fail_names_the_reason_and_a_busy_answer_exits_4() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/hello/");
        then.status(503)
            .header("content-type", "text/plain")
            .header("x-rt-reason", "queue_timeout")
            .header("retry-after", "2")
            .body("queue_timeout");
    });
    let out = ctx.run(&["invoke", "hello", "--fail"]).fails_with(4);
    assert!(out.stdout.contains("<- HTTP 503"), "{}", out.stdout);
    assert!(out.stdout.contains("retry-after: 2"), "{}", out.stdout);
    assert_eq!(out.stderr, "error: HTTP 503 queue_timeout (retry after 2s)\n");
    // without --fail a 503 is still a completed request
    ctx.run(&["invoke", "hello"]).ok();
}

#[test]
fn invoke_retry_repeats_a_busy_answer() {
    let ctx = ctx_with_invoke();
    let calls = Arc::new(AtomicUsize::new(0));
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/s/hello/").body("hi");
        then.respond_with(move |_| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                HttpMockResponse::builder()
                    .status(503)
                    .header("x-rt-reason", "queue_timeout")
                    .header("retry-after", "1")
                    .body("queue_timeout")
                    .build()
            } else {
                HttpMockResponse::builder().status(200).body("ok").build()
            }
        });
    });
    let out = ctx
        .run(&["--retry", "1", "invoke", "hello", "-d", "hi", "--fail", "--body-only"])
        .ok();
    m.assert_calls(2);
    assert_eq!(out.stdout, "ok");
    assert!(
        out.stderr.contains("HTTP 503 queue_timeout: retrying in 1s"),
        "{}",
        out.stderr
    );
}

// ---- --json ----------------------------------------------------------------------------------

#[test]
fn json_flag_equals_output_json() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    let a = ctx.run(&["--json", "service", "list"]).ok().json();
    let b = ctx.run(&["service", "list", "-o", "json"]).ok().json();
    assert_eq!(a, b);
    assert_eq!(a, json!({"items": [], "next_cursor": null}));
}

// ---- doctor ----------------------------------------------------------------------------------

fn openapi(ctx: &Ctx) {
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/openapi.json");
        then.status(200)
            .json_body(json!({"openapi": "3.1.0", "info": {"version": "1.0.0"}}));
    });
}

fn node_status(ctx: &Ctx, version: &str, mode: &str) {
    let body = json!({"version": version, "mode": mode, "default_profile": "balanced", "v8_sandbox": false,
                      "processors": 1, "kinds": {}, "memory": {}, "inflight": 0});
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(200).json_body(body);
    });
}

/// `CHECK STATUS …` rows of the doctor table, keyed by check.
fn row<'a>(lines: &'a [String], check: &str) -> &'a str {
    lines
        .iter()
        .find(|l| l.starts_with(&format!("{check} ")))
        .map(String::as_str)
        .unwrap_or_else(|| panic!("no {check} row in {lines:?}"))
}

#[test]
fn doctor_with_an_operator_token_checks_everything() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    node_status(&ctx, CLIENT_VERSION, "normal");
    let out = ctx.run(&["doctor"]).ok();
    let lines = out.lines();
    assert_eq!(lines[0], "CHECK STATUS DETAIL");
    assert!(row(&lines, "config").starts_with("config ok no context"), "{lines:?}");
    assert_eq!(row(&lines, "credentials"), "credentials ok tenant key (rtk_…)");
    assert!(row(&lines, "control api").contains("ok") && row(&lines, "control api").contains("(API 1.0.0)"));
    assert_eq!(row(&lines, "auth"), "auth ok operator: node routes allowed");
    assert_eq!(
        row(&lines, "version"),
        format!("version ok client {CLIENT_VERSION}, node {CLIENT_VERSION}")
    );
    assert_eq!(row(&lines, "node mode"), "node mode ok normal");
    // any HTTP answer proves the invoke listener is there (here: the mock's 404)
    assert!(row(&lines, "invoke listener").contains("(HTTP 404)"), "{lines:?}");
    assert!(!out.stdout.contains(TOKEN), "the token is never printed");
}

fn tenant_key_doctor(ctx: &Ctx) {
    openapi(ctx);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(403)
            .json_body(err_json("insufficient_scope", "operator only"));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("limit", "1");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
}

#[test]
fn doctor_with_a_tenant_key_checks_the_version_through_v1_version() {
    let ctx = ctx_with_invoke();
    tenant_key_doctor(&ctx);
    let version = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/version")
            .header("authorization", format!("Bearer {TOKEN}"));
        then.status(200)
            .json_body(json!({"version": CLIENT_VERSION, "api": "1.0.0"}));
    });
    let out = ctx.run(&["doctor"]).ok();
    version.assert();
    let lines = out.lines();
    assert_eq!(row(&lines, "auth"), "auth ok tenant key: services:read allowed");
    assert_eq!(
        row(&lines, "version"),
        format!("version ok client {CLIENT_VERSION}, node {CLIENT_VERSION}")
    );
    assert!(
        !lines.iter().any(|l| l.starts_with("node mode ")),
        "the mode needs the operator: {lines:?}"
    );
}

#[test]
fn doctor_against_a_node_without_v1_version_skips_the_check() {
    let ctx = ctx_with_invoke();
    tenant_key_doctor(&ctx);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/version");
        then.status(404).json_body(err_json("not_found", "not found"));
    });
    let out = ctx.run(&["doctor"]).ok();
    assert_eq!(
        row(&out.lines(), "version"),
        "version skip the node predates GET /v1/version"
    );
}

#[test]
fn doctor_warns_about_a_version_mismatch_and_a_draining_node() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    node_status(&ctx, "99.0.0", "drain");
    let out = ctx.run(&["--json", "doctor"]).ok();
    let v = out.json();
    assert_eq!(v["ok"], true);
    let status = |name: &str| {
        v["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["check"] == name)
            .unwrap()["status"]
            .clone()
    };
    assert_eq!(status("version"), "warn");
    assert_eq!(status("node mode"), "warn");
}

#[test]
fn doctor_fails_on_a_rejected_token_or_an_unreachable_api() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(401).json_body(err_json("unauthorized", "bad token"));
    });
    let out = ctx.run(&["doctor"]).fails_with(1);
    assert!(
        row(&out.lines(), "auth").starts_with("auth fail the token was rejected"),
        "{}",
        out.stdout
    );

    let out = ctx
        .cmd()
        .env("RT_API", "http://127.0.0.1:1")
        .env("RT_INVOKE", "http://127.0.0.1:1")
        .args(["--json", "doctor"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], false);
    let checks = v["checks"].as_array().unwrap();
    let get = |name: &str| checks.iter().find(|c| c["check"] == name).unwrap().clone();
    assert_eq!(get("control api")["status"], "fail");
    assert!(get("control api")["detail"].as_str().unwrap().contains("cannot reach"));
    assert_eq!(get("auth")["status"], "skip");
    assert_eq!(get("invoke listener")["status"], "warn");
}

/// The config file is named when it exists, and `--as-tenant` shows whose data the check runs for.
#[test]
fn doctor_names_the_config_file_and_the_tenant_it_acts_for() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    node_status(&ctx, CLIENT_VERSION, "normal");
    std::fs::create_dir_all(ctx.config_path().parent().unwrap()).unwrap();
    std::fs::write(ctx.config_path(), "").unwrap();
    let out = ctx.run(&["--as-tenant", "ten_9", "doctor"]).ok();
    let lines = out.lines();
    assert!(!row(&lines, "config").contains("absent"), "{lines:?}");
    assert!(row(&lines, "credentials").ends_with("acting for ten_9"), "{lines:?}");
}

#[test]
fn doctor_warns_when_no_token_is_configured() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    node_status(&ctx, CLIENT_VERSION, "normal");
    let out = ctx.cmd().env_remove("RT_KEY").arg("doctor").output().unwrap();
    let lines = norm(&String::from_utf8_lossy(&out.stdout));
    assert!(
        row(&lines, "credentials").starts_with("credentials warn no token"),
        "{lines:?}"
    );
}

/// A failed status probe that is not a transport error still fails the control API check.
#[test]
fn doctor_fails_the_control_api_check_on_an_api_error_answer() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/openapi.json");
        then.status(500).json_body(err_json("internal", "boom"));
    });
    let out = ctx.run(&["doctor"]).fails_with(1);
    let lines = out.lines();
    assert!(row(&lines, "control api").starts_with("control api fail"), "{lines:?}");
    assert_eq!(row(&lines, "auth"), "auth skip the control API is not reachable");
}

#[test]
fn doctor_shows_an_unknown_node_mode_verbatim_as_a_warning() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    node_status(&ctx, CLIENT_VERSION, "quarantine");
    let out = ctx.run(&["doctor"]).ok();
    assert_eq!(row(&out.lines(), "node mode"), "node mode warn quarantine");
}

/// A tenant key without `services:read` is a warning; any other failure of that probe is a failure.
#[test]
fn doctor_grades_a_tenant_key_whose_services_probe_is_refused_or_broken() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(403)
            .json_body(err_json("insufficient_scope", "operator only"));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("limit", "1");
        then.status(403)
            .json_body(err_json("insufficient_scope", "no services:read"));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/version");
        then.status(500).json_body(err_json("internal", "boom"));
    });
    let out = ctx.run(&["doctor"]).ok();
    let lines = out.lines();
    assert_eq!(
        row(&lines, "auth"),
        "auth warn tenant key without services:read (insufficient_scope)"
    );
    assert!(row(&lines, "version").starts_with("version warn"), "{lines:?}");
}

#[test]
fn doctor_fails_the_auth_check_when_the_services_probe_breaks() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(403)
            .json_body(err_json("insufficient_scope", "operator only"));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("limit", "1");
        then.status(500).json_body(err_json("internal", "boom"));
    });
    let out = ctx.run(&["doctor"]).fails_with(1);
    assert!(row(&out.lines(), "auth").starts_with("auth fail"), "{:?}", out.lines());
}

/// Any other status failure of the node probe fails the auth check and skips the version check.
#[test]
fn doctor_fails_auth_on_a_server_error_from_the_node_probe() {
    let ctx = ctx_with_invoke();
    openapi(&ctx);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(500).json_body(err_json("internal", "boom"));
    });
    let out = ctx.run(&["doctor"]).fails_with(1);
    let lines = out.lines();
    assert!(row(&lines, "auth").starts_with("auth fail"), "{lines:?}");
    assert_eq!(row(&lines, "version"), "version skip needs a working control API");
}
