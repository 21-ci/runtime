//! `21ci switch`, `switch status`, `rollback --switch`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

const SW: &str = "sw_01M3XM1PCHQ8DECFTC5TY0CXSX";

fn view(state: &str) -> Value {
    json!({
        "id": SW, "service": "svc_1", "kind": "switch", "state": state,
        "from_version": 3, "to_version": 4,
        "source": {"service": "svc_2", "version": 2, "digest": "sha256:abc"},
        "inflight_old": 0, "interrupted": false, "warnings": [],
        "created_at": T0_MS, "updated_at": T0_MS
    })
}

fn get_switch(ctx: &Ctx, body: Value) {
    ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/services/svc_1/switches/{SW}"));
        then.status(200).json_body(body);
    });
}

#[test]
fn switch_posts_the_branch_with_if_match() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let post = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/switch")
            .header("if-match", "\"7\"")
            .json_body(json!({"from": {"branch": "one"}}));
        then.status(202).json_body(view("draining"));
    });
    let out = ctx.run(&["switch", "hello", "--from", "one"]).ok();
    post.assert();
    assert!(out.stdout.contains(SW), "{}", out.stdout);
    assert!(out.stdout.contains("draining"), "{}", out.stdout);
}

#[test]
fn switch_sends_the_options() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let post = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/switch").json_body(json!({
            "from": {"branch": "one"}, "close_sockets": true, "drain_timeout": "10m"
        }));
        then.status(202).json_body(view("draining"));
    });
    ctx.run(&[
        "switch",
        "hello",
        "--from",
        "one",
        "--close-sockets",
        "--drain-timeout",
        "10m",
    ])
    .ok();
    post.assert();
}

#[test]
fn dry_run_says_nothing_was_written_and_lists_warnings() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let mut v = view("preparing");
    v["id"] = Value::Null;
    v["dry_run"] = json!(true);
    v["warnings"] = json!([{"code": "env_missing_on_parent", "names": ["API_KEY"]}]);
    ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/switch")
            .json_body(json!({"from": {"branch": "one"}, "dry_run": true}));
        then.status(200).json_body(v);
    });
    let out = ctx.run(&["switch", "hello", "--from", "one", "--dry-run"]).ok();
    assert!(out.stdout.contains("env_missing_on_parent: API_KEY"), "{}", out.stdout);
    assert!(out.stderr.contains("nothing was written"), "{}", out.stderr);
}

#[test]
fn wait_follows_the_switch_to_completed_and_keeps_the_warnings() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let mut started = view("draining");
    started["warnings"] = json!([{"code": "routes_refresh_delayed", "names": []}]);
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/switch");
        then.status(202).json_body(started);
    });
    get_switch(&ctx, view("completed"));
    let out = ctx
        .run(&["-o", "json", "switch", "hello", "--from", "one", "--wait"])
        .ok();
    let v = out.json();
    assert_eq!(v["state"], json!("completed"));
    assert_eq!(v["warnings"][0]["code"], json!("routes_refresh_delayed"));
}

#[test]
fn wait_fails_on_a_failed_switch() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/switch");
        then.status(202).json_body(view("preparing"));
    });
    let mut failed = view("failed");
    failed["error"] = json!("interrupted");
    get_switch(&ctx, failed);
    let out = ctx.run(&["switch", "hello", "--from", "one", "--wait"]).fails_with(1);
    assert!(out.stderr.contains("interrupted"), "{}", out.stderr);
}

#[test]
fn wait_succeeds_on_a_drain_timeout_with_a_note() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/switch");
        then.status(202).json_body(view("draining"));
    });
    let mut timed_out = view("drain_timed_out");
    timed_out["error"] = json!("drain_unknown");
    get_switch(&ctx, timed_out);
    let out = ctx.run(&["switch", "hello", "--from", "one", "--wait"]).ok();
    assert!(out.stderr.contains("new version is live"), "{}", out.stderr);
    assert!(out.stderr.contains("drain_unknown"), "{}", out.stderr);
}

#[test]
fn wait_gives_up_after_the_timeout_and_says_the_flip_stands() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/switch");
        then.status(202).json_body(view("draining"));
    });
    get_switch(&ctx, view("draining"));
    let out = ctx
        .run(&["switch", "hello", "--from", "one", "--wait", "--wait-timeout", "0s"])
        .fails_with(1);
    assert!(out.stderr.contains("21ci switch status"), "{}", out.stderr);
}

#[test]
fn usage_errors() {
    let ctx = Ctx::new();
    ctx.run(&["switch", "hello"]).fails_with(2);
    ctx.run(&["switch", "--from", "one"]).fails_with(2);
    ctx.run(&["switch", "hello", "--from", "a/b"]).fails_with(2);
    ctx.run(&["switch", "hello", "--from", "one", "--wait", "--dry-run"])
        .fails_with(2);
    ctx.run(&["switch", "hello", "--from", "one", "--drain-timeout", "soon"])
        .fails_with(2);
    ctx.run(&["switch", "status"]).fails_with(2);
    ctx.run(&["switch", "status", "hello", "../x"]).fails_with(2);
}

#[test]
fn refusals_keep_their_code() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/switch");
        then.status(409).json_body(err_json("switch_in_progress", "busy"));
    });
    let out = ctx.run(&["switch", "hello", "--from", "one"]).fails_with(1);
    assert!(out.stderr.contains("switch_in_progress"), "{}", out.stderr);
}

#[test]
fn status_lists_newest_first_as_the_server_answers() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/switches");
        then.status(200)
            .json_body(json!({"items": [view("completed")], "next_cursor": null}));
    });
    let out = ctx.run(&["switch", "status", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID KIND STATE FROM TO CREATED",
            &format!("{SW} switch completed 3 4 {T0}"),
        ]
    );
}

#[test]
fn status_of_one_switch() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    get_switch(&ctx, view("draining"));
    let out = ctx.run(&["-o", "json", "switch", "status", "hello", SW]).ok();
    assert_eq!(out.json()["state"], json!("draining"));
}

#[test]
fn rollback_switch_posts_to_the_switch_route() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let mut back = view("draining");
    back["kind"] = json!("rollback");
    back["from_version"] = json!(4);
    back["to_version"] = json!(3);
    let post = ctx.server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/services/svc_1/switches/{SW}/rollback"))
            .header("if-match", "\"7\"");
        then.status(202).json_body(back);
    });
    let out = ctx.run(&["rollback", "hello", "--switch", SW]).ok();
    post.assert();
    assert!(out.stdout.contains("rollback"), "{}", out.stdout);
}

#[test]
fn rollback_switch_target_gone_keeps_its_code() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/services/svc_1/switches/{SW}/rollback"));
        then.status(409).json_body(err_json("rollback_target_gone", "gone"));
    });
    let out = ctx.run(&["rollback", "hello", "--switch", SW]).fails_with(1);
    assert!(out.stderr.contains("rollback_target_gone"), "{}", out.stderr);
}

#[test]
fn rollback_switch_conflicts_with_to_and_close_sockets() {
    let ctx = Ctx::new();
    ctx.run(&["rollback", "hello", "--switch", SW, "--to", "2"])
        .fails_with(2);
    ctx.run(&["rollback", "hello", "--switch", SW, "--close-sockets"])
        .fails_with(2);
    ctx.run(&["rollback", "hello", "--switch", "../x"]).fails_with(2);
}
