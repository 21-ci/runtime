//! `21ci env …` and `21ci secret …`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn env_get(ctx: &Ctx, current: serde_json::Value) -> httpmock::Mock<'_> {
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/env")
            .header("authorization", auth());
        then.status(200).json_body(current);
    })
}

#[test]
fn env_get_prints_a_sorted_table() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    env_get(&ctx, json!({"B": "two words", "A": "1"}));
    let out = ctx.run(&["env", "get", "hello"]).ok();
    assert_eq!(out.lines(), vec!["KEY VALUE", "A 1", "B two words"]);
}

#[test]
fn env_get_json_and_empty() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    env_get(&ctx, json!({}));
    let out = ctx.run(&["env", "get", "hello"]).ok();
    assert_eq!(out.stdout, "");
    assert!(out.stderr.contains("No variables set"), "{}", out.stderr);
    let out = ctx.run(&["-o", "json", "env", "get", "hello"]).ok();
    assert_eq!(out.json(), json!({}));
}

#[test]
fn env_set_merges_into_the_current_map_and_replaces_the_whole_map() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    env_get(&ctx, json!({"X": "9", "A": "old"}));
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/env")
            .header("authorization", auth())
            .header("content-type", "application/json")
            .json_body(json!({"A": "1", "B": "two=2", "X": "9"}));
        then.status(200).json_body(json!({"A": "1", "B": "two=2", "X": "9"}));
    });
    let out = ctx.run(&["env", "set", "hello", "A=1", "B=two=2"]).ok();
    put.assert();
    assert_eq!(out.lines(), vec!["Set 2 variables on hello (svc_1)."]);
}

#[test]
fn env_set_single_variable_and_json_output() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    env_get(&ctx, json!({}));
    ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/env")
            .json_body(json!({"A": ""}));
        then.status(200).json_body(json!({"A": ""}));
    });
    let out = ctx.run(&["env", "set", "hello", "A="]).ok();
    assert_eq!(out.lines(), vec!["Set 1 variable on hello (svc_1)."]);
    let out = ctx.run(&["-o", "json", "env", "set", "hello", "A="]).ok();
    assert_eq!(out.json(), json!({"A": ""}));
}

#[test]
fn env_set_requires_key_value_pairs() {
    let ctx = Ctx::new();
    ctx.run(&["env", "set", "hello", "NOEQUALS"]).fails_with(2);
    ctx.run(&["env", "set", "hello", "=value"]).fails_with(2);
    ctx.run(&["env", "set", "hello"]).fails_with(2);
}

#[test]
fn env_unset_removes_keys() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    env_get(&ctx, json!({"X": "9", "Y": "1", "Z": "2"}));
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/env")
            .json_body(json!({"Y": "1"}));
        then.status(200).json_body(json!({"Y": "1"}));
    });
    let out = ctx.run(&["env", "unset", "hello", "X", "Z"]).ok();
    put.assert();
    assert_eq!(out.lines(), vec!["Unset 2 variables on hello (svc_1)."]);
}

#[test]
fn env_unset_json_prints_the_map_the_server_saved() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    env_get(&ctx, json!({"X": "9", "Y": "1"}));
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/env")
            .json_body(json!({"Y": "1"}));
        then.status(200).json_body(json!({"Y": "1"}));
    });
    let out = ctx.run(&["--json", "env", "unset", "hello", "X"]).ok();
    put.assert();
    assert_eq!(out.json(), json!({"Y": "1"}));
}

#[test]
fn an_empty_reply_is_an_empty_map_and_a_long_text_error_is_cut_at_300_characters() {
    // an empty body (no JSON at all) reads as an empty map
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/env");
        then.status(200);
    });
    let out = ctx.run(&["env", "get", "hello"]).ok();
    assert!(out.stderr.contains("No variables set."), "{}", out.stderr);

    // a plain-text error longer than 300 characters is shown cut, with an ellipsis
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/env");
        then.status(502).body("x".repeat(400));
    });
    let out = ctx.run(&["env", "get", "hello"]).fails_with(1);
    assert!(out.stderr.contains(&format!("{}…", "x".repeat(300))), "{}", out.stderr);
    assert!(!out.stderr.contains(&"x".repeat(301)), "{}", out.stderr);
}

// ---- secrets ---------------------------------------------------------------------------------

#[test]
fn secret_set_from_file_is_verbatim() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("key.pem");
    std::fs::write(&f, "line1\nline2\n").unwrap();
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/secrets/API_KEY")
            .header("authorization", auth())
            .header("content-type", "application/json")
            .json_body(json!({"value": "line1\nline2\n"}));
        then.status(204);
    });
    let out = ctx
        .run(&["secret", "set", "hello", "API_KEY", "--from-file", f.to_str().unwrap()])
        .ok();
    put.assert();
    assert_eq!(out.lines(), vec!["Secret API_KEY set on hello (svc_1)."]);
}

#[test]
fn secret_set_from_stdin_strips_one_trailing_newline_and_never_echoes_the_value() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/secrets/TOKEN")
            .json_body(json!({"value": "s3cret"}));
        then.status(204);
    });
    let out = ctx.run_stdin(&["secret", "set", "hello", "TOKEN"], "s3cret\n");
    let out = out.ok();
    put.assert();
    assert!(!out.stdout.contains("s3cret") && !out.stderr.contains("s3cret"));
    let out = ctx
        .run_stdin(&["-o", "json", "secret", "set", "hello", "TOKEN"], "s3cret\n")
        .ok();
    assert_eq!(out.json(), json!({"service": "svc_1", "secret": "TOKEN", "set": true}));
}

#[test]
fn secret_set_without_a_value_source_is_a_usage_error() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    // empty stdin (EOF immediately) and no --from-file
    let out = ctx.run_stdin(&["secret", "set", "hello", "TOKEN"], "").fails_with(2);
    assert!(out.stderr.contains("empty"), "{}", out.stderr);
}

#[test]
fn secret_set_missing_file_is_a_usage_error() {
    let ctx = Ctx::new();
    ctx.run(&["secret", "set", "hello", "TOKEN", "--from-file", "/definitely/not/here"])
        .fails_with(2);
}

#[test]
fn secret_list_shows_names_and_update_times_only() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/secrets")
            .header("authorization", auth());
        then.status(200)
            .json_body(json!([{"name": "API_KEY", "updated_at": T0_MS}, {"name": "TOKEN", "updated_at": T0_MS}]));
    });
    let out = ctx.run(&["secret", "list", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec!["NAME UPDATED", &format!("API_KEY {T0}"), &format!("TOKEN {T0}")]
    );
}

#[test]
fn secret_delete() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/secrets/TOKEN")
            .header("authorization", auth());
        then.status(204);
    });
    let out = ctx.run(&["secret", "delete", "hello", "TOKEN"]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Secret TOKEN deleted from hello (svc_1)."]);
}
