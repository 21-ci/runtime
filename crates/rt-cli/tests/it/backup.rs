//! `21ci backup …` against a mock control API.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

const ID: &str = "bkp_01ARZ3NDEKTSV4RRFFQ69G5FAV";

#[test]
fn create_posts_and_wait_follows_the_run_to_its_end() {
    let ctx = Ctx::new();
    let post = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/node/backups");
        then.status(202).json_body(json!({"id": ID, "state": "running"}));
    });
    let get = ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/node/backups/{ID}"));
        then.status(200)
            .json_body(json!({"id": ID, "state": "ok", "trigger": "api", "bytes": 10, "sha256": "ab"}));
    });
    let out = ctx.run(&["backup", "create"]).ok();
    assert!(out.stdout.contains(ID));
    post.assert();
    let out = ctx.run(&["backup", "create", "--wait"]).ok();
    assert!(out.stdout.contains("State:    ok"), "{}", out.stdout);
    get.assert();
}

#[test]
fn a_failed_wait_exits_non_zero_and_shows_why() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/node/backups");
        then.status(202).json_body(json!({"id": ID, "state": "running"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/node/backups/{ID}"));
        then.status(200)
            .json_body(json!({"id": ID, "state": "failed", "failure": {"code": "corrupt"}}));
    });
    let out = ctx.run(&["backup", "create", "--wait"]);
    assert_ne!(out.code, 0);
    assert!(out.stdout.contains("corrupt"), "{}", out.stdout);
}

#[test]
fn create_reports_a_refusal() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/node/backups");
        then.status(503)
            .json_body(json!({"error": {"code": "backup_disabled", "message": "backups are off"}}));
    });
    let out = ctx.run(&["backup", "create"]);
    assert_ne!(out.code, 0);
    assert!(out.stderr.contains("backups are off"), "{}", out.stderr);
}

#[test]
fn list_status_and_get_read_their_routes() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/backups");
        then.status(200).json_body(
            json!({"items": [{"id": ID, "state": "ok", "trigger": "schedule", "bytes": 5, "started_at": T0_MS}], "next_cursor": null}),
        );
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/backups/status");
        then.status(200).json_body(json!({
            "enabled": true, "schedule": "0 3 * * *", "last_ok_id": ID, "last_failure": {"code": "x"}
        }));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/node/backups/{ID}"));
        then.status(200).json_body(json!({"id": ID, "state": "ok"}));
    });
    assert!(ctx.run(&["backup", "list"]).ok().stdout.contains(ID));
    let s = ctx.run(&["backup", "status"]).ok().stdout;
    assert!(s.contains("0 3 * * *") && s.contains(ID) && s.contains("\"x\""), "{s}");
    assert!(ctx.run(&["backup", "get", ID]).ok().stdout.contains(ID));
    assert_eq!(
        ctx.run(&["-o", "json", "backup", "status"]).ok().json()["enabled"],
        json!(true)
    );
}

#[test]
fn download_streams_to_the_file_and_delete_deletes() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/node/backups/{ID}/archive"));
        then.status(200).body("archive bytes");
    });
    let del = ctx.server.mock(|when, then| {
        when.method(DELETE).path(format!("/v1/node/backups/{ID}"));
        then.status(204);
    });
    let dir = tempfile::tempdir().expect("dir");
    let out = dir.path().join("a.tar");
    ctx.run(&["backup", "download", ID, "--out", out.to_str().expect("utf8")])
        .ok();
    assert_eq!(std::fs::read(&out).expect("read"), b"archive bytes");
    assert!(!dir.path().join("a.tar.part").exists());
    ctx.run(&["backup", "delete", ID]).ok();
    del.assert();
}

/// A failed download leaves no file behind that looks like an archive.
#[test]
fn a_gone_archive_writes_nothing_and_ids_are_not_paths() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/node/backups/{ID}/archive"));
        then.status(410)
            .json_body(json!({"error": {"code": "archive_gone", "message": "no archive"}}));
    });
    let dir = tempfile::tempdir().expect("dir");
    let out = dir.path().join("a.tar");
    let r = ctx.run(&["backup", "download", ID, "--out", out.to_str().expect("utf8")]);
    assert_ne!(r.code, 0);
    assert!(!out.exists());
    assert_ne!(ctx.run(&["backup", "get", "../../v1/tenants"]).code, 0);
}
