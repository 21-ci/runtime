//! `21ci app …` against a mock control API.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

const SVC: &str = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const BDL: &str = "bdl_01ARZ3NDEKTSV4RRFFQ69G5FAV";

#[test]
fn export_follows_the_job_and_writes_the_file() {
    let ctx = Ctx::new();
    let d = tempfile::tempdir().expect("dir");
    let out = d.path().join("a.tar.zst");
    let post = ctx.server.mock(|when, then| {
        when.method(POST).path(format!("/v1/services/{SVC}/bundle"));
        then.status(202).json_body(json!({"id": BDL, "state": "running"}));
    });
    let get = ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/services/{SVC}/bundle/{BDL}"));
        then.status(200)
            .header("content-type", "application/zstd")
            .body("BUNDLE");
    });
    let o = ctx
        .run(&["app", "export", SVC, "--out", out.to_str().expect("path")])
        .ok();
    post.assert();
    get.assert();
    assert!(o.stdout.contains("6 bytes"), "{}", o.stdout);
    assert_eq!(std::fs::read(&out).expect("read"), b"BUNDLE");
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&out).expect("meta").permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "it holds env values");
}

#[test]
fn a_failed_export_exits_non_zero_and_writes_nothing() {
    let ctx = Ctx::new();
    let d = tempfile::tempdir().expect("dir");
    let out = d.path().join("a.tar.zst");
    ctx.server.mock(|when, then| {
        when.method(POST).path(format!("/v1/services/{SVC}/bundle"));
        then.status(202).json_body(json!({"id": BDL, "state": "running"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/services/{SVC}/bundle/{BDL}"));
        then.status(200)
            .json_body(json!({"id": BDL, "state": "failed", "failure": {"code": "internal"}}));
    });
    let o = ctx.run(&["app", "export", SVC, "--out", out.to_str().expect("path")]);
    assert_ne!(o.code, 0);
    assert!(!out.exists());
}

#[test]
fn import_streams_the_file_and_names_missing_secrets() {
    let ctx = Ctx::new();
    let d = tempfile::tempdir().expect("dir");
    let file = d.path().join("a.tar.zst");
    std::fs::write(&file, "BUNDLE").expect("write");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/import")
            .query_param("slug", "copy")
            .query_param("project", "web")
            .query_param("environment", "staging")
            .query_param("activate", "1")
            .body("BUNDLE");
        then.status(202).json_body(json!({
            "id": "imp_1", "service_id": SVC, "warnings": ["missing_secrets"],
            "missing_secrets": ["API_KEY"], "versions": 1
        }));
    });
    let o = ctx
        .run(&[
            "app",
            "import",
            file.to_str().expect("path"),
            "--slug",
            "copy",
            "--project",
            "web",
            "--env",
            "staging",
            "--activate",
        ])
        .ok();
    m.assert();
    assert!(o.stdout.contains("API_KEY"), "{}", o.stdout);
}

#[test]
fn an_invalid_bundle_is_a_failure_with_its_reason() {
    let ctx = Ctx::new();
    let d = tempfile::tempdir().expect("dir");
    let file = d.path().join("a.tar.zst");
    std::fs::write(&file, "BUNDLE").expect("write");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/import");
        then.status(422).json_body(json!({"error": {"code": "bundle_invalid", "message": "a blob row names a file the store would not have made"}}));
    });
    let o = ctx.run(&["app", "import", file.to_str().expect("path"), "--slug", "copy"]);
    assert_ne!(o.code, 0);
    assert!(
        o.stderr.contains("bundle_invalid") || o.stderr.contains("blob row"),
        "{}",
        o.stderr
    );
}

#[test]
fn fork_posts_from_with_the_service_id() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/import")
            .query_param("slug", "copy")
            .query_param("from", SVC);
        then.status(202).json_body(
            json!({"id": "imp_1", "service_id": "svc_2", "warnings": [], "missing_secrets": [], "versions": 1}),
        );
    });
    let o = ctx.run(&["app", "fork", SVC, "--slug", "copy"]).ok();
    m.assert();
    assert!(o.stdout.contains("svc_2"), "{}", o.stdout);
}

/// Exported from the current node, imported on the named context's node with that context's token;
/// no bundle stays behind.
#[test]
fn move_exports_here_and_imports_on_the_named_context() {
    let ctx = Ctx::new();
    let other = MockServer::start();
    let cfg = ctx.config_path();
    std::fs::create_dir_all(cfg.parent().expect("dir")).expect("mkdir");
    std::fs::write(
        &cfg,
        format!(
            "[contexts.prod]\napi = \"{}\"\ntoken = \"prod-token\"\n",
            other.base_url()
        ),
    )
    .expect("cfg");
    ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/services/{SVC}"));
        then.status(200).json_body(json!({"id": SVC, "slug": "hello"}));
    });
    ctx.server.mock(|when, then| {
        when.method(POST).path(format!("/v1/services/{SVC}/bundle"));
        then.status(202).json_body(json!({"id": BDL, "state": "running"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path(format!("/v1/services/{SVC}/bundle/{BDL}"));
        then.status(200)
            .header("content-type", "application/zstd")
            .body("BUNDLE");
    });
    let import = other.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/import")
            .query_param("slug", "hello")
            .header("authorization", "Bearer prod-token")
            .body("BUNDLE");
        then.status(202)
            .json_body(json!({"id": "imp_1", "service_id": SVC, "versions": 1}));
    });
    let o = ctx.run(&["app", "move", SVC, "--to", "prod"]).ok();
    import.assert();
    assert!(o.stdout.contains("Imported hello"), "{}", o.stdout);
}

#[test]
fn move_to_an_unknown_context_is_refused() {
    let ctx = Ctx::new();
    let cfg = ctx.config_path();
    std::fs::create_dir_all(cfg.parent().expect("dir")).expect("mkdir");
    std::fs::write(&cfg, "").expect("cfg");
    let o = ctx.run(&["app", "move", SVC, "--to", "nope"]);
    assert!(o.stderr.contains("not found"), "{}", o.stderr);
}
