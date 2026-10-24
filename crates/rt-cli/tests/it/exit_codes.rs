//! Error rendering and exit codes: 0 ok, 1 API error, 2 usage, 3 network.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

#[test]
fn api_error_shows_code_message_and_status_and_exits_1() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants/ten_x");
        then.status(404).json_body(err_json("not_found", "not found"));
    });
    let out = ctx.run(&["tenant", "get", "ten_x"]).fails_with(1);
    assert_eq!(out.stdout, "");
    assert_eq!(out.stderr, "error: not_found: not found (HTTP 404)\n");
}

#[test]
fn api_error_details_are_printed_when_present() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/tenants");
        then.status(422).json_body(json!({"error": {
            "code": "validation_failed", "message": "the upload failed validation",
            "details": {"diagnostics": [{"file": "main.ts", "line": 3, "col": 5, "message": "unexpected token"}]}
        }}));
    });
    let out = ctx.run(&["tenant", "create", "acme"]).fails_with(1);
    assert!(
        out.stderr
            .starts_with("error: validation_failed: the upload failed validation (HTTP 422)\n"),
        "{}",
        out.stderr
    );
    assert!(out.stderr.contains("main.ts"), "{}", out.stderr);
    assert!(out.stderr.contains("unexpected token"), "{}", out.stderr);
}

#[test]
fn api_error_in_json_mode_is_the_envelope_plus_status_on_stderr() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants/ten_x");
        then.status(404).json_body(err_json("not_found", "not found"));
    });
    let out = ctx.run(&["-o", "json", "tenant", "get", "ten_x"]).fails_with(1);
    assert_eq!(out.stdout, "");
    let v: serde_json::Value = serde_json::from_str(out.stderr.trim()).expect("stderr is JSON");
    assert_eq!(v["error"]["code"], "not_found");
    assert_eq!(v["error"]["message"], "not found");
    assert_eq!(v["error"]["status"], 404);
}

#[test]
fn non_envelope_error_body_is_reported_with_the_http_status() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants/ten_x");
        then.status(502).body("<html>bad gateway</html>");
    });
    let out = ctx.run(&["tenant", "get", "ten_x"]).fails_with(1);
    assert!(out.stderr.contains("HTTP 502"), "{}", out.stderr);
    assert!(out.stderr.contains("bad gateway"), "{}", out.stderr);
}

#[test]
fn unreachable_server_exits_3() {
    let ctx = Ctx::new();
    let out = ctx
        .cmd()
        .env("RT_API", "http://127.0.0.1:1")
        .args(["tenant", "list"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(3));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.starts_with("error: cannot reach http://127.0.0.1:1"), "{stderr}");
}

#[test]
fn usage_errors_exit_2() {
    let ctx = Ctx::new();
    ctx.run(&["frobnicate"]).fails_with(2);
    ctx.run(&["tenant"]).fails_with(2);
    ctx.run(&["tenant", "get"]).fails_with(2); // missing <id>
    ctx.run(&["key", "list"]).fails_with(2); // missing --tenant
    ctx.run(&["--output", "yaml", "tenant", "list"]).fails_with(2);
}

#[test]
fn usage_errors_never_touch_the_network() {
    let ctx = Ctx::new();
    let any = ctx.server.mock(|when, then| {
        when.any_request();
        then.status(200).json_body(json!({}));
    });
    ctx.run(&["service", "create", "Bad_Slug"]).fails_with(2);
    ctx.run(&["service", "create", "a"]).fails_with(2); // too short
    ctx.run(&["env", "set", "hello", "NOEQUALS"]).fails_with(2);
    any.assert_calls(0);
}

#[test]
fn help_and_version_flags_exit_0() {
    let ctx = Ctx::new();
    let out = ctx.run(&["--help"]).ok();
    assert!(out.stdout.contains("21ci"), "{}", out.stdout);
    let out = ctx.run(&["--version"]).ok();
    assert!(out.stdout.starts_with("21ci "), "{}", out.stdout);
}
