//! `21ci branch` and `push --branch`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

fn branch_json(name: &str) -> Value {
    let mut v = service_json_with("svc_2", &format!("hello-br-{name}"), Some(1), "\"3\"");
    v["environment"] = json!(format!("br-{name}"));
    v["scratch"] = json!({"of": "svc_1", "kind": "branch", "name": name, "expires_at": null, "copied_data": false});
    v
}

fn lookup_body(ctx: &Ctx, slug: &str, body: Value) {
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("slug", slug);
        then.status(200)
            .json_body(json!({"items": [body], "next_cursor": null}));
    });
}

#[test]
fn create_posts_every_flag() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let post = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/branches").json_body(json!({
            "name": "one",
            "environment": "staging",
            "copy_data": true,
            "schedules": true,
            "events_of_parent": true,
            "env_names_from_parent": true,
            "egress_of_parent": false,
        }));
        then.status(201).json_body(branch_json("one"));
    });
    let out = ctx
        .run(&[
            "branch",
            "create",
            "hello",
            "--name",
            "one",
            "--copy-data",
            "--schedules",
            "--events-of-parent",
            "--environment",
            "staging",
            "--env-names",
            "--no-egress",
        ])
        .ok();
    post.assert();
    assert!(out.stdout.contains("hello-br-one"), "{}", out.stdout);
    assert!(
        out.stdout.contains("21ci push --service hello --branch one"),
        "{}",
        out.stdout
    );
}

#[test]
fn create_sends_only_the_name_by_default_and_prints_the_view_as_json() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/branches")
            .json_body(json!({"name": "one"}));
        then.status(201).json_body(branch_json("one"));
    });
    let out = ctx
        .run(&["-o", "json", "branch", "create", "hello", "--name", "one"])
        .ok();
    assert_eq!(out.json(), branch_json("one"));
}

#[test]
fn ttl_becomes_expires_at_in_the_future() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let future = T0_MS + 1;
    let post = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/branches")
            .is_true(move |req| {
                let v: Value = serde_json::from_slice(req.body_ref()).unwrap_or(Value::Null);
                v["expires_at"].as_i64().is_some_and(|at| at > future)
            });
        then.status(201).json_body(branch_json("one"));
    });
    ctx.run(&["branch", "create", "hello", "--name", "one", "--ttl", "7d"])
        .ok();
    post.assert();
}

#[test]
fn a_bad_name_is_a_usage_error_before_any_request() {
    let ctx = Ctx::new();
    for name in ["One", "1a", "a--b", "../x", "a/b"] {
        ctx.run(&["branch", "create", "hello", "--name", name]).fails_with(2);
        ctx.run(&["branch", "delete", "hello", name]).fails_with(2);
    }
}

#[test]
fn list_shows_a_row_per_branch() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/branches");
        then.status(200)
            .json_body(json!({"items": [branch_json("one")], "next_cursor": null}));
    });
    let out = ctx.run(&["branch", "list", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "NAME SLUG ID LIVE ENV EXPIRES DATA",
            "one hello-br-one svc_2 1 br-one - empty",
        ]
    );
}

#[test]
fn delete_calls_the_branch_route() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let del = ctx.server.mock(|when, then| {
        when.method(DELETE).path("/v1/services/svc_1/branches/one");
        then.status(204);
    });
    let out = ctx.run(&["branch", "delete", "hello", "one"]).ok();
    del.assert();
    assert!(out.stdout.contains("deleted"), "{}", out.stdout);
}

#[test]
fn delete_reports_not_a_branch() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(DELETE).path("/v1/services/svc_1/branches/one");
        then.status(409).json_body(err_json("not_a_branch", "not a branch"));
    });
    let out = ctx.run(&["branch", "delete", "hello", "one"]).fails_with(1);
    assert!(out.stderr.contains("not_a_branch"), "{}", out.stderr);
}

const TS: &str = "export default () => new Response(\"hi\");\n";

#[test]
fn push_branch_uploads_to_the_branch_service() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    ctx.lookup("hello", "svc_1");
    lookup_body(&ctx, "hello-br-one", branch_json("one"));
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_2/versions")
            .query_param("activate", "1");
        then.status(201).json_body(
            json!({"version": 2, "source_digest": "sha256:abc", "build_state": "pending", "created_at": T0_MS}),
        );
    });
    let file = dir.path().join("hello.ts");
    let out = ctx.run(&["push", file.to_str().unwrap(), "--branch", "one"]).ok();
    upload.assert();
    assert!(out.stdout.contains("/s/hello-br-one/"), "{}", out.stdout);
    assert!(out.stdout.contains("21ci switch hello --from one"), "{}", out.stdout);
}

#[test]
fn push_branch_refuses_a_service_only_named_like_a_branch() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    ctx.lookup("hello", "svc_1");
    ctx.lookup("hello-br-one", "svc_9"); // no `scratch`
    let upload = ctx.server.mock(|when, then| {
        when.method(POST).path_includes("/versions");
        then.status(201);
    });
    let file = dir.path().join("hello.ts");
    let out = ctx
        .run(&["push", file.to_str().unwrap(), "--branch", "one"])
        .fails_with(1);
    assert!(out.stderr.contains("not a branch"), "{}", out.stderr);
    upload.assert_calls(0);
}

#[test]
fn push_branch_refuses_a_branch_of_another_parent() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    ctx.lookup("hello", "svc_other");
    lookup_body(&ctx, "hello-br-one", branch_json("one")); // of svc_1
    let file = dir.path().join("hello.ts");
    let out = ctx
        .run(&["push", file.to_str().unwrap(), "--branch", "one"])
        .fails_with(1);
    assert!(out.stderr.contains("not a branch"), "{}", out.stderr);
}

#[test]
fn push_branch_that_does_not_exist_says_how_to_create_it() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    ctx.lookup("hello", "svc_1");
    ctx.lookup_empty("hello-br-one");
    let file = dir.path().join("hello.ts");
    let out = ctx
        .run(&["push", file.to_str().unwrap(), "--branch", "one"])
        .fails_with(1);
    assert!(
        out.stderr.contains("21ci branch create hello --name one"),
        "{}",
        out.stderr
    );
}

#[test]
fn push_branch_does_not_combine_with_create() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    let file = dir.path().join("hello.ts");
    ctx.run(&["push", file.to_str().unwrap(), "--branch", "one", "--create"])
        .fails_with(2);
}

#[test]
fn service_scratch_posts_every_flag_with_the_parents_id() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let post = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/scratch").json_body(json!({
            "of": "svc_1",
            "name": "pr-1",
            "ttl_seconds": 7200,
            "copy_data": true,
            "allowed_hosts": ["api.example.com"],
        }));
        then.status(201)
            .json_body(json!({"id": "svc_9", "slug": "hello-pr-1-0123abcd",
            "scratch": {"of": "svc_1", "kind": "preview", "name": "pr-1", "expires_at": 1, "copied_data": true}}));
    });
    let out = ctx
        .run(&[
            "service",
            "scratch",
            "--of",
            "hello",
            "--name",
            "pr-1",
            "--ttl",
            "2h",
            "--copy-data",
            "--allow-host",
            "api.example.com",
        ])
        .ok();
    post.assert();
    assert!(out.stdout.contains("hello-pr-1-0123abcd"), "{}", out.stdout);
}

#[test]
fn service_scratch_stands_alone_with_a_runtime_and_needs_one_or_the_other() {
    let ctx = Ctx::new();
    let post = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/scratch")
            .json_body(json!({"runtime": "deno"}));
        then.status(201)
            .json_body(json!({"id": "svc_9", "slug": "scratch-preview-0123abcd"}));
    });
    ctx.run(&["service", "scratch", "--runtime", "deno"]).ok();
    post.assert();
    ctx.run(&["service", "scratch"]).fails_with(2);
}
