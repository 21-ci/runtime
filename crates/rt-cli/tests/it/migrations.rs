//! `21ci migrations status`, `promote`, `push --migrations`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

#[test]
fn status_lists_applied_and_pending() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/migrations");
        then.status(200).json_body(json!({
            "applied": [{"name": "0001_a", "sha256": "sha256:aa", "applied_at": T0_MS, "version": 2}],
            "pending_for_latest": [{"name": "0002_b", "sha256": "sha256:bb"}],
        }));
    });
    let out = ctx.run(&["migrations", "status", "hello"]);
    assert!(
        out.stdout.contains("0001_a") && out.stdout.contains("0002_b"),
        "{}",
        out.stdout
    );
}

#[test]
fn promote_dry_run_asks_for_a_dry_run() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/live")
            .json_body(json!({"version": 3, "close_sockets": false, "dry_run": true}));
        then.status(200)
            .json_body(json!({"dry_run": true, "pending_migrations": ["0002_b"]}));
    });
    let out = ctx.run(&["promote", "hello", "3", "--dry-run"]);
    m.assert();
    assert!(out.stdout.contains("0002_b"), "{}", out.stdout);
}

#[test]
fn push_with_migrations_uploads_then_posts_them_then_flips() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().expect("dir");
    std::fs::write(
        dir.path().join("app.wasm"),
        b"\0asm\x0d\0\x01\0 component bytes \xff\xfe" as &[u8],
    )
    .expect("write");
    let sql = dir.path().join("sql");
    std::fs::create_dir(&sql).expect("mkdir");
    std::fs::write(sql.join("0001_a.sql"), "CREATE TABLE a (x);").expect("write");
    ctx.lookup("hello", "svc_1");
    let upload = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(201)
            .json_body(json!({"version": 4, "source_digest": "sha256:ab", "build_state": "pending"}));
    });
    let post = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions/4/migrations")
            .json_body(json!({"files": [{"name": "0001_a.sql", "sql": "CREATE TABLE a (x);"}]}));
        then.status(201).json_body(json!({"version": 4}));
    });
    let flip = ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/live");
        then.status(200).json_body(json!({"live_version": 4}));
    });
    let app = dir.path().join("app.wasm");
    ctx.run(&[
        "push",
        app.to_str().expect("p"),
        "--service",
        "hello",
        "--migrations",
        sql.to_str().expect("p"),
    ]);
    assert_eq!((upload.calls(), post.calls(), flip.calls()), (1, 1, 1));
}
