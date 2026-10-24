//! `21ci config …`: contexts, config file, env/flag precedence.
use crate::common;

use common::*;
use httpmock::prelude::*;
use predicates::prelude::*;
use serde_json::json;

fn read_cfg(ctx: &Ctx) -> toml::Value {
    let text = std::fs::read_to_string(ctx.config_path()).expect("config file written");
    toml::from_str(&text).expect("config file is valid TOML")
}

#[test]
fn set_context_writes_config_and_first_context_becomes_current() {
    let ctx = Ctx::bare();
    let out = ctx
        .run(&[
            "config",
            "set-context",
            "local",
            "--api",
            "http://127.0.0.1:8081/",
            "--invoke",
            "http://127.0.0.1:8080",
            "--token",
            "rtk_abc",
        ])
        .ok();
    assert_eq!(out.lines(), vec![r#"Context "local" set."#]);

    let cfg = read_cfg(&ctx);
    assert_eq!(cfg["current_context"].as_str(), Some("local"));
    let c = &cfg["contexts"]["local"];
    assert_eq!(
        c["api"].as_str(),
        Some("http://127.0.0.1:8081"),
        "trailing slash is trimmed"
    );
    assert_eq!(c["invoke"].as_str(), Some("http://127.0.0.1:8080"));
    assert_eq!(c["token"].as_str(), Some("rtk_abc"));
}

#[cfg(unix)]
#[test]
fn config_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let ctx = Ctx::bare();
    ctx.run(&["config", "set-context", "local", "--api", "http://x", "--token", "t"])
        .ok();
    let mode = std::fs::metadata(ctx.config_path()).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "the file holds tokens");
}

#[test]
fn set_context_updates_only_given_fields() {
    let ctx = Ctx::bare();
    ctx.run(&[
        "config",
        "set-context",
        "local",
        "--api",
        "http://a",
        "--invoke",
        "http://i",
        "--token",
        "tok",
    ])
    .ok();
    ctx.run(&["config", "set-context", "local", "--api", "http://b"]).ok();
    let cfg = read_cfg(&ctx);
    let c = &cfg["contexts"]["local"];
    assert_eq!(c["api"].as_str(), Some("http://b"));
    assert_eq!(c["invoke"].as_str(), Some("http://i"));
    assert_eq!(c["token"].as_str(), Some("tok"));
}

#[test]
fn set_context_without_api_for_a_new_context_is_a_usage_error() {
    let ctx = Ctx::bare();
    let out = ctx
        .run(&["config", "set-context", "local", "--token", "x"])
        .fails_with(2);
    assert!(out.stderr.contains("--api"), "{}", out.stderr);
    assert!(!ctx.config_path().exists());
}

#[test]
fn get_contexts_table_marks_current_and_hides_tokens() {
    let ctx = Ctx::bare();
    ctx.run(&[
        "config",
        "set-context",
        "local",
        "--api",
        "http://127.0.0.1:8081",
        "--invoke",
        "http://127.0.0.1:8080",
        "--token",
        "SECRET1",
    ])
    .ok();
    ctx.run(&[
        "config",
        "set-context",
        "prod",
        "--api",
        "https://rt.example.com",
        "--invoke",
        "https://fn.example.com",
        "--tenant",
        "ten_1",
    ])
    .ok();
    let out = ctx.run(&["config", "get-contexts"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "CURRENT NAME API INVOKE TENANT TOKEN",
            "* local http://127.0.0.1:8081 http://127.0.0.1:8080 - yes",
            "prod https://rt.example.com https://fn.example.com ten_1 no",
        ]
    );
    assert!(!out.stdout.contains("SECRET1"));
    // header + 2 rows, no trailing whitespace on any line
    assert!(out.stdout.lines().all(|l| l == l.trim_end()));
}

#[test]
fn get_contexts_json() {
    let ctx = Ctx::bare();
    ctx.run(&[
        "config",
        "set-context",
        "local",
        "--api",
        "http://a",
        "--token",
        "SECRET1",
    ])
    .ok();
    let out = ctx.run(&["--output", "json", "config", "get-contexts"]).ok();
    assert_eq!(
        out.json(),
        json!({"current": "local", "contexts": [
            {"name": "local", "api": "http://a", "invoke": null, "tenant": null, "token": true}
        ]})
    );
}

#[test]
fn use_context_switches_and_current_prints_the_name() {
    let ctx = Ctx::bare();
    ctx.run(&["config", "set-context", "a", "--api", "http://a"]).ok();
    ctx.run(&["config", "set-context", "b", "--api", "http://b"]).ok();
    assert_eq!(ctx.run(&["config", "current"]).ok().stdout, "a\n");
    let out = ctx.run(&["config", "use-context", "b"]).ok();
    assert_eq!(out.lines(), vec![r#"Switched to context "b"."#]);
    assert_eq!(ctx.run(&["config", "current"]).ok().stdout, "b\n");
    assert_eq!(read_cfg(&ctx)["current_context"].as_str(), Some("b"));
}

#[test]
fn use_unknown_context_is_a_usage_error() {
    let ctx = Ctx::bare();
    ctx.run(&["config", "set-context", "a", "--api", "http://a"]).ok();
    let out = ctx.run(&["config", "use-context", "nope"]).fails_with(2);
    assert!(out.stderr.contains(r#"context "nope" not found"#), "{}", out.stderr);
}

#[test]
fn current_without_any_context_is_an_error() {
    let ctx = Ctx::bare();
    ctx.run(&["config", "current"]).fails_with(2);
}

#[test]
fn current_json_shows_the_resolved_values_with_env_applied() {
    let ctx = Ctx::bare();
    ctx.run(&[
        "config",
        "set-context",
        "a",
        "--api",
        "http://a",
        "--invoke",
        "http://ia",
        "--token",
        "tok",
    ])
    .ok();
    let mut cmd = ctx.cmd();
    let out = cmd
        .env("RT_API", "http://from-env")
        .args(["-o", "json", "config", "current"])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        v,
        json!({"context": "a", "api": "http://from-env", "invoke": "http://ia", "tenant": null, "token_set": true})
    );
}

#[test]
fn context_flag_selects_that_context_and_sends_its_token() {
    let ctx = Ctx::bare();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/node/status")
            .header("authorization", "Bearer ctx-b-token");
        then.status(200).json_body(json!({
            "version": "9.9.9", "mode": "normal", "default_profile": "balanced", "v8_sandbox": false,
            "processors": 1, "kinds": {}, "memory": {}, "inflight": 0
        }));
    });
    ctx.run(&[
        "config",
        "set-context",
        "a",
        "--api",
        "http://127.0.0.1:1",
        "--token",
        "ctx-a-token",
    ])
    .ok();
    ctx.run(&[
        "config",
        "set-context",
        "b",
        "--api",
        &ctx.base(),
        "--token",
        "ctx-b-token",
    ])
    .ok();
    // current is "a" (dead port); --context b must win
    ctx.run(&["--context", "b", "node", "status"]).ok();
    m.assert();
}

#[test]
fn context_flag_with_unknown_name_is_a_usage_error() {
    let ctx = Ctx::bare();
    ctx.run(&["config", "set-context", "a", "--api", "http://a"]).ok();
    let out = ctx.run(&["--context", "zzz", "node", "status"]).fails_with(2);
    assert!(out.stderr.contains(r#"context "zzz" not found"#), "{}", out.stderr);
}

#[test]
fn env_overrides_context_values() {
    let ctx = Ctx::bare();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/node/status")
            .header("authorization", "Bearer env-key");
        then.status(200).json_body(json!({
            "version": "1", "mode": "normal", "default_profile": "balanced", "v8_sandbox": false,
            "processors": 1, "kinds": {}, "memory": {}, "inflight": 0
        }));
    });
    ctx.run(&[
        "config",
        "set-context",
        "a",
        "--api",
        "http://127.0.0.1:1",
        "--token",
        "ctx-token",
    ])
    .ok();
    ctx.cmd()
        .env("RT_API", ctx.base())
        .env("RT_KEY", "env-key")
        .args(["node", "status"])
        .assert()
        .success();
    m.assert();
}

#[test]
fn rt_root_token_is_used_when_rt_key_is_unset_and_beats_the_context_token() {
    let ctx = Ctx::bare();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/node/status")
            .header("authorization", "Bearer root-tok");
        then.status(200).json_body(json!({
            "version": "1", "mode": "normal", "default_profile": "balanced", "v8_sandbox": false,
            "processors": 1, "kinds": {}, "memory": {}, "inflight": 0
        }));
    });
    ctx.run(&[
        "config",
        "set-context",
        "a",
        "--api",
        &ctx.base(),
        "--token",
        "ctx-token",
    ])
    .ok();
    ctx.cmd()
        .env("RT_ROOT_TOKEN", "root-tok")
        .args(["node", "status"])
        .assert()
        .success();
    m.assert();
}

#[test]
fn no_token_means_no_authorization_header() {
    let ctx = Ctx::bare();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status").header_missing("authorization");
        then.status(200).json_body(json!({
            "version": "1", "mode": "normal", "default_profile": "balanced", "v8_sandbox": false,
            "processors": 1, "kinds": {}, "memory": {}, "inflight": 0
        }));
    });
    ctx.cmd()
        .env("RT_API", ctx.base())
        .args(["node", "status"])
        .assert()
        .success();
    m.assert();
}

#[test]
fn defaults_point_at_the_local_dev_node() {
    // nothing configured: the default control API is http://127.0.0.1:8081
    let ctx = Ctx::bare();
    ctx.cmd()
        .args(["docs"])
        .assert()
        .success()
        .stdout(predicate::eq("http://127.0.0.1:8081/docs\n"));
}

#[test]
fn explicit_config_file_flag() {
    let ctx = Ctx::bare();
    let file = ctx.cfg.path().join("other.toml");
    ctx.run(&[
        "--config",
        file.to_str().unwrap(),
        "config",
        "set-context",
        "x",
        "--api",
        "http://x",
    ])
    .ok();
    assert!(file.exists());
    assert!(!ctx.config_path().exists());
    let out = ctx.run(&["--config", file.to_str().unwrap(), "config", "current"]).ok();
    assert_eq!(out.stdout, "x\n");
}

#[test]
fn x_rt_tenant_header_from_flag_env_and_context() {
    let ctx = Ctx::bare();
    let status = json!({
        "version": "1", "mode": "normal", "default_profile": "balanced", "v8_sandbox": false,
        "processors": 1, "kinds": {}, "memory": {}, "inflight": 0
    });
    let flag = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/node/status")
            .header("x-rt-tenant", "ten_flag");
        then.status(200).json_body(status.clone());
    });
    ctx.cmd()
        .env("RT_API", ctx.base())
        .args(["--as-tenant", "ten_flag", "node", "status"])
        .assert()
        .success();
    flag.assert();

    ctx.run(&[
        "config",
        "set-context",
        "a",
        "--api",
        &ctx.base(),
        "--tenant",
        "ten_ctx",
    ])
    .ok();
    let from_ctx = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/node/status")
            .header("x-rt-tenant", "ten_ctx");
        then.status(200).json_body(status.clone());
    });
    ctx.run(&["node", "status"]).ok();
    from_ctx.assert();
}

#[test]
fn a_blank_context_name_is_a_usage_error_and_json_describes_the_context() {
    let ctx = Ctx::bare();
    let out = ctx
        .run(&["config", "set-context", "   ", "--api", "http://x"])
        .fails_with(2);
    assert!(out.stderr.contains("must not be empty"), "{}", out.stderr);

    let out = ctx
        .run(&[
            "--json",
            "config",
            "set-context",
            "dev",
            "--api",
            "https://rt.example.com/",
            "--token",
            "rtk_x",
        ])
        .ok();
    assert_eq!(
        out.json(),
        json!({"name": "dev", "api": "https://rt.example.com", "invoke": null, "tenant": null, "token": true})
    );
    let out = ctx.run(&["--json", "config", "use-context", "dev"]).ok();
    assert_eq!(out.json(), json!({"current": "dev"}));
}

#[test]
fn get_contexts_without_any_context_says_how_to_make_one() {
    let ctx = Ctx::bare();
    let out = ctx.run(&["config", "get-contexts"]).ok();
    assert_eq!(out.stdout, "");
    assert!(out.stderr.contains("No contexts. Create one with"), "{}", out.stderr);
}

#[test]
fn an_https_api_with_a_key_is_not_warned_about() {
    let ctx = Ctx::new();
    let out = ctx
        .cmd()
        .env("RT_API", "https://127.0.0.1:1")
        .args(["host", "list", "svc_1"])
        .output()
        .expect("spawn");
    // the connection is refused: a network error, and no plain-http warning
    assert_eq!(out.status.code(), Some(3));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!err.contains("plain http"), "{err}");
}

#[test]
fn a_malformed_api_url_is_a_usage_error_before_any_request() {
    let ctx = Ctx::new();
    let out = ctx
        .cmd()
        .env("RT_API", "not a url")
        .args(["env", "get", "svc_1"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("invalid URL"), "{err}");
}
