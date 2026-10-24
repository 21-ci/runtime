//! `21ci versions`, `rollback`, `rebuild`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn versions_page(ctx: &Ctx, states: &[(u32, &str)]) {
    let items: Vec<_> = states.iter().map(|(n, s)| version_json(*n, s)).collect();
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/versions")
            .header("authorization", auth());
        then.status(200).json_body(items);
    });
}

#[test]
fn versions_table_marks_the_live_one() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1"); // live_version = 3
    versions_page(&ctx, &[(3, "ready"), (2, "failed"), (1, "ready")]);
    let out = ctx.run(&["versions", "hello"]).ok();
    let digest = format!("sha256:{}", "ab".repeat(6)); // shortened to 12 hex chars
    assert_eq!(
        out.lines(),
        vec![
            "VERSION BUILD LIVE DIGEST CREATED",
            &format!("3 ready * {digest} {T0}"),
            &format!("2 failed - {digest} {T0}"),
            &format!("1 ready - {digest} {T0}"),
        ]
    );
}

#[test]
fn versions_json_is_the_raw_list() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    versions_page(&ctx, &[(1, "ready")]);
    let out = ctx.run(&["-o", "json", "versions", "hello"]).ok();
    assert_eq!(out.json(), json!([version_json(1, "ready")]));
}

#[test]
fn versions_by_id_fetches_the_service_for_the_live_marker() {
    let ctx = Ctx::new();
    let get = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200)
            .json_body(service_json_with("svc_1", "hello", Some(1), "\"7\""));
    });
    versions_page(&ctx, &[(1, "ready")]);
    let out = ctx.run(&["versions", "svc_1"]).ok();
    get.assert();
    assert!(out.lines()[1].starts_with("1 ready *"), "{:?}", out.lines());
}

#[test]
fn rollback_goes_to_the_previous_usable_version_with_if_match() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1"); // live 3, etag "7"
    versions_page(&ctx, &[(3, "ready"), (2, "failed"), (1, "ready")]);
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/live")
            .header("authorization", auth())
            .header("if-match", "\"7\"")
            .header("content-type", "application/json")
            .json_body(json!({"version": 1}));
        then.status(200)
            .json_body(service_json_with("svc_1", "hello", Some(1), "\"8\""));
    });
    let out = ctx.run(&["rollback", "hello"]).ok();
    put.assert();
    assert_eq!(
        out.lines(),
        vec!["Service hello (svc_1) is now live at version 1 (was 3)."]
    );
}

#[test]
fn rollback_to_an_explicit_version_does_not_list_versions() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let list = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/versions");
        then.status(200).json_body(json!([]));
    });
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/live")
            .header("if-match", "\"7\"")
            .json_body(json!({"version": 2}));
        then.status(200)
            .json_body(service_json_with("svc_1", "hello", Some(2), "\"8\""));
    });
    ctx.run(&["rollback", "hello", "--to", "2"]).ok();
    put.assert();
    list.assert_calls(0);
}

#[test]
fn rollback_json_prints_the_updated_service() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let updated = service_json_with("svc_1", "hello", Some(2), "\"8\"");
    ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/live");
        then.status(200).json_body(updated.clone());
    });
    let out = ctx.run(&["-o", "json", "rollback", "hello", "--to", "2"]).ok();
    assert_eq!(out.json(), updated);
}

#[test]
fn rollback_stale_etag_is_a_precondition_failure() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/live");
        then.status(412)
            .json_body(err_json("precondition_failed", "etag mismatch"));
    });
    let out = ctx.run(&["rollback", "hello", "--to", "1"]).fails_with(1);
    assert_eq!(out.stderr, "error: precondition_failed: etag mismatch (HTTP 412)\n");
}

#[test]
fn rollback_failed_version_is_version_not_ready() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/live");
        then.status(409)
            .json_body(err_json("version_not_ready", "version 2 failed to build"));
    });
    let out = ctx.run(&["rollback", "hello", "--to", "2"]).fails_with(1);
    assert!(out.stderr.contains("version_not_ready"), "{}", out.stderr);
}

#[test]
fn rollback_with_nothing_older_is_an_error() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("slug", "hello");
        then.status(200)
            .json_body(json!({"items": [service_json_with("svc_1", "hello", Some(1), "\"1\"")], "next_cursor": null}));
    });
    versions_page(&ctx, &[(1, "ready")]);
    let put = ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/live");
        then.status(200).json_body(json!({}));
    });
    let out = ctx.run(&["rollback", "hello"]).fails_with(1);
    put.assert_calls(0);
    assert!(out.stderr.contains("nothing to roll back to"), "{}", out.stderr);
}

#[test]
fn rollback_of_a_service_without_a_live_version_needs_to() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("slug", "hello");
        then.status(200)
            .json_body(json!({"items": [service_json_with("svc_1", "hello", None, "\"1\"")], "next_cursor": null}));
    });
    let out = ctx.run(&["rollback", "hello"]).fails_with(1);
    assert!(out.stderr.contains("--to"), "{}", out.stderr);
}

#[test]
fn rollback_uses_the_etag_header_when_fetching_by_id() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200)
            .header("etag", "\"42\"")
            .json_body(service_json_with("svc_1", "hello", Some(3), "\"7\""));
    });
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/live")
            .header("if-match", "\"42\"");
        then.status(200)
            .json_body(service_json_with("svc_1", "hello", Some(2), "\"43\""));
    });
    ctx.run(&["rollback", "svc_1", "--to", "2"]).ok();
    put.assert();
}

#[test]
fn rebuild_posts_to_the_version() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions/2/rebuild")
            .header("authorization", auth());
        then.status(202).json_body(version_json(2, "building"));
    });
    let out = ctx.run(&["rebuild", "hello", "2"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec!["Rebuild of hello (svc_1) version 2 started (build_state: building)."]
    );
}

#[test]
fn rebuild_needs_a_numeric_version() {
    Ctx::new().run(&["rebuild", "hello", "two"]).fails_with(2);
}

// ---- one version, build logs, source ---------------------------------------------------------

#[test]
fn versions_with_a_number_shows_that_version() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1"); // live_version = 3
    let mut failed = version_json(2, "failed");
    failed["failure"] = json!({"reason": "build_failed", "message": "cannot resolve npm:nope"});
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/versions/2")
            .header("authorization", auth());
        then.status(200).json_body(failed.clone());
    });
    let out = ctx.run(&["versions", "hello", "2"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "Version: 2".to_string(),
            "Build: failed".to_string(),
            "Live: no".to_string(),
            format!("Source digest: sha256:{}", "ab".repeat(32)),
            "Lock digest: -".to_string(),
            "Created: 2026-01-01T00:00:00Z".to_string(),
            r#"Failure: {"reason":"build_failed","message":"cannot resolve npm:nope"}"#.to_string(),
        ]
    );
    assert_eq!(ctx.run(&["--json", "versions", "hello", "2"]).ok().json(), failed);
}

fn build_log(seq: u64, msg: &str) -> serde_json::Value {
    json!({"seq": seq, "ts": T0_MS, "level": "info", "service_id": "svc_1", "version": 4, "task_id": null,
           "trigger": null, "kind": "build", "msg": msg, "truncated": false})
}

#[test]
fn build_logs_streams_the_build_log() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let mut body = String::from(": keep-alive\n\n");
    for r in [
        build_log(1, "build started (deno)"),
        build_log(2, "build finished in 25 ms"),
    ] {
        body.push_str(&format!("id: {}\nevent: log\ndata: {}\n\n", r["seq"], r));
    }
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/versions/4/build/logs")
            .header("authorization", auth())
            .header("accept", "text/event-stream");
        then.status(200).header("content-type", "text/event-stream").body(body);
    });
    let out = ctx.run(&["build-logs", "hello", "4"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "2026-01-01T00:00:00.000Z info v4 - build started (deno)",
            "2026-01-01T00:00:00.000Z info v4 - build finished in 25 ms",
        ]
    );
    let out = ctx.run(&["-o", "json", "build-logs", "hello", "4"]).ok();
    let first: serde_json::Value = serde_json::from_str(out.stdout.lines().next().unwrap()).unwrap();
    assert_eq!(first, build_log(1, "build started (deno)"));
}

#[test]
fn source_prints_text_or_writes_a_file() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/versions/3/source")
            .header("authorization", auth());
        then.status(200)
            .header("content-type", "application/octet-stream")
            .body("export default { fetch() { return new Response('hi') } }\n");
    });
    let out = ctx.run(&["source", "hello", "3"]).ok();
    assert_eq!(out.stdout, "export default { fetch() { return new Response('hi') } }\n");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.ts");
    let out = ctx
        .run(&["-o", "json", "source", "hello", "3", "--out", path.to_str().unwrap()])
        .ok();
    m.assert_calls(2);
    assert_eq!(out.json(), json!({"path": path.display().to_string(), "bytes": 57}));
    assert!(std::fs::read_to_string(&path).unwrap().starts_with("export default"));
}

#[test]
fn source_of_a_missing_version_is_not_found() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/versions/9/source");
        then.status(404).json_body(err_json("not_found", "no such version"));
    });
    let out = ctx.run(&["source", "svc_1", "9"]).fails_with(1);
    assert_eq!(out.stderr, "error: not_found: no such version (HTTP 404)\n");
}

/// `rollback --close-sockets` closes the service's WebSockets with 1012.
#[test]
fn rollback_close_sockets_is_in_the_body() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let put = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/live")
            .json_body(json!({"version": 2, "close_sockets": true}));
        then.status(200)
            .json_body(service_json_with("svc_1", "hello", Some(2), "\"8\""));
    });
    ctx.run(&["rollback", "hello", "--to", "2", "--close-sockets"]).ok();
    put.assert();
}
