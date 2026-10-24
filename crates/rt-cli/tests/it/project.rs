//! `21ci project …` and the `--project` flags of `21ci service …` (decision D-B3).
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn project(slug: &str, envs: &[&str]) -> Value {
    json!({
        "id": "prj_01", "tenant_id": "ten_1", "slug": slug, "name": slug, "environments": envs,
        "created_at": T0_MS, "updated_at": T0_MS
    })
}

#[test]
fn create_posts_the_slug_name_and_environments() {
    let ctx = Ctx::new();
    let mut body = project("web", &["production", "staging"]);
    body["name"] = json!("Web shop");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/projects")
            .header("authorization", auth())
            .json_body(json!({"slug": "web", "name": "Web shop", "environments": ["production", "staging"]}));
        then.status(201).json_body(body);
    });
    let out = ctx
        .run(&[
            "project",
            "create",
            "web",
            "--name",
            "Web shop",
            "--env",
            "production,staging",
        ])
        .ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "ID: prj_01",
            "Slug: web",
            "Name: Web shop",
            "Environments: production, staging",
            &format!("Created: {T0}"),
        ]
    );
}

#[test]
fn create_with_only_a_slug_sends_only_the_slug_and_repeated_env_flags_add_up() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/projects")
            .json_body(json!({"slug": "jobs"}));
        then.status(201).json_body(project("jobs", &["production"]));
    });
    ctx.run(&["project", "create", "jobs"]).ok();
    m.assert();

    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/projects")
            .json_body(json!({"slug": "api", "environments": ["production", "dev", "pr-1"]}));
        then.status(201)
            .json_body(project("api", &["production", "dev", "pr-1"]));
    });
    ctx.run(&["project", "create", "api", "--env", "production", "--env", "dev,pr-1"])
        .ok();
    m.assert();
}

#[test]
fn create_checks_the_slug_and_environments_before_sending() {
    let ctx = Ctx::new();
    ctx.run(&["project", "create", "Web"]).fails_with(2);
    ctx.run(&["project", "create", "x"]).fails_with(2);
    ctx.run(&["project", "create", "web", "--env", "Prod"]).fails_with(2);
    let out = ctx.run(&["project", "create", "web", "--env", "a,a"]).fails_with(2);
    assert!(out.stderr.contains("twice"), "{}", out.stderr);
    ctx.run(&["project", "create", "web", "--name", "  "]).fails_with(2);
}

#[test]
fn create_json_prints_the_api_body() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/projects");
        then.status(201).json_body(project("web", &["production"]));
    });
    let out = ctx.run(&["--json", "project", "create", "web"]).ok();
    assert_eq!(out.json()["slug"], json!("web"));
}

#[test]
fn a_taken_slug_is_an_api_error() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/projects");
        then.status(409)
            .json_body(err_json("project_slug_taken", "project slug taken"));
    });
    let out = ctx.run(&["project", "create", "web"]).fails_with(1);
    assert_eq!(out.stderr, "error: project_slug_taken: project slug taken (HTTP 409)\n");
}

#[test]
fn list_renders_a_table() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/projects").header("authorization", auth());
        then.status(200).json_body(json!({
            "items": [project("default", &["production"]), project("web", &["production", "staging"])],
            "next_cursor": null
        }));
    });
    let out = ctx.run(&["project", "list"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID SLUG NAME ENVIRONMENTS",
            "prj_01 default default production",
            "prj_01 web web production,staging",
        ]
    );
}

#[test]
fn list_follows_pages_with_all() {
    let ctx = Ctx::new();
    let first = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/projects").query_param_missing("cursor");
        then.status(200)
            .json_body(json!({"items": [project("a1", &["production"])], "next_cursor": "a1"}));
    });
    let second = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/projects").query_param("cursor", "a1");
        then.status(200)
            .json_body(json!({"items": [project("b2", &["production"])], "next_cursor": null}));
    });
    let out = ctx.run(&["--json", "project", "list", "--all"]).ok();
    first.assert();
    second.assert();
    let slugs: Vec<Value> = out.json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["slug"].clone())
        .collect();
    assert_eq!(slugs, vec![json!("a1"), json!("b2")]);
}

#[test]
fn get_shows_one_project_by_slug() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/projects/web");
        then.status(200).json_body(project("web", &["production", "staging"]));
    });
    let out = ctx.run(&["project", "get", "web"]).ok();
    m.assert();
    assert_eq!(out.lines()[1], "Slug: web");
    assert!(
        out.lines().contains(&"Environments: production, staging".to_string()),
        "{:?}",
        out.lines()
    );
}

#[test]
fn get_accepts_an_id_and_reports_a_missing_project() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/projects/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV");
        then.status(404).json_body(err_json("not_found", "not found"));
    });
    let out = ctx
        .run(&["project", "get", "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"])
        .fails_with(1);
    assert_eq!(out.stderr, "error: not_found: not found (HTTP 404)\n");
}

#[test]
fn update_patches_the_name_and_replaces_the_environment_list() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/projects/web")
            .header("authorization", auth())
            .json_body(json!({"name": "Storefront", "environments": ["production", "dev"]}));
        then.status(200).json_body(project("web", &["production", "dev"]));
    });
    ctx.run(&[
        "project",
        "update",
        "web",
        "--name",
        "Storefront",
        "--env",
        "production,dev",
    ])
    .ok();
    m.assert();
}

#[test]
fn update_can_add_and_remove_single_environments() {
    let ctx = Ctx::new();
    let current = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/projects/web/environments");
        then.status(200)
            .json_body(json!({"project": "web", "environments": ["production", "staging"]}));
    });
    let patched = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/projects/web")
            .json_body(json!({"environments": ["production", "dev"]}));
        then.status(200).json_body(project("web", &["production", "dev"]));
    });
    ctx.run(&[
        "project",
        "update",
        "web",
        "--add-env",
        "dev",
        "--remove-env",
        "staging",
    ])
    .ok();
    current.assert();
    patched.assert();
}

#[test]
fn update_with_nothing_to_change_or_conflicting_flags_is_a_usage_error() {
    let ctx = Ctx::new();
    ctx.run(&["project", "update", "web"]).fails_with(2);
    ctx.run(&["project", "update", "web", "--env", "production", "--add-env", "dev"])
        .fails_with(2);
    ctx.run(&["project", "update", "web", "--add-env", "Bad"]).fails_with(2);
    // removing the last environment would leave the project with none
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/projects/web/environments");
        then.status(200)
            .json_body(json!({"project": "web", "environments": ["production"]}));
    });
    let out = ctx
        .run(&["project", "update", "web", "--remove-env", "production"])
        .fails_with(2);
    assert!(out.stderr.contains("at least one"), "{}", out.stderr);
}

#[test]
fn update_reports_an_environment_still_in_use() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(PATCH).path("/v1/projects/web");
        then.status(409)
            .json_body(err_json("environment_in_use", "environment in use"));
    });
    let out = ctx
        .run(&["project", "update", "web", "--env", "production"])
        .fails_with(1);
    assert_eq!(out.stderr, "error: environment_in_use: environment in use (HTTP 409)\n");
}

#[test]
fn delete_removes_a_project() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/projects/web")
            .header("authorization", auth());
        then.status(204);
    });
    let out = ctx.run(&["project", "delete", "web"]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Project web deleted."]);
    let out = ctx.run(&["--json", "project", "delete", "web"]).ok();
    assert_eq!(out.json(), json!({"deleted": "web"}));
}

#[test]
fn delete_of_a_project_with_services_is_refused_by_the_api() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(DELETE).path("/v1/projects/web");
        then.status(409)
            .json_body(err_json("project_not_empty", "project not empty"));
    });
    let out = ctx.run(&["project", "delete", "web"]).fails_with(1);
    assert_eq!(out.stderr, "error: project_not_empty: project not empty (HTTP 409)\n");
}

// ---- service flags -------------------------------------------------------------------------------

#[test]
fn service_create_sends_the_project() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services")
            .json_body(json!({"slug": "front", "runtime": "deno", "project": "web", "environment": "staging"}));
        then.status(201).json_body(service_json("svc_1", "front"));
    });
    ctx.run(&["service", "create", "front", "--project", "web", "--env", "staging"])
        .ok();
    m.assert();
}

#[test]
fn service_list_filters_by_project_and_environment() {
    let ctx = Ctx::new();
    let mut s = service_json("svc_1", "front");
    s["project"] = json!("web");
    s["environment"] = json!("staging");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services")
            .query_param("project", "web")
            .query_param("environment", "staging");
        then.status(200).json_body(json!({"items": [s], "next_cursor": null}));
    });
    let out = ctx
        .run(&["service", "list", "--project", "web", "--env", "staging"])
        .ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "ID SLUG RUNTIME PROJECT ENV LIVE BUILD ACCESS",
            "svc_1 front deno web staging 3 ready public"
        ]
    );
}

#[test]
fn service_get_shows_the_project() {
    let ctx = Ctx::new();
    let mut s = service_json("svc_1", "front");
    s["project"] = json!("web");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200).json_body(s);
    });
    let lines = ctx.run(&["service", "get", "svc_1"]).ok().lines();
    assert!(lines.contains(&"Project: web".to_string()), "{lines:?}");
}

#[test]
fn push_create_puts_the_new_service_in_the_given_project_and_environment() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(
        dir.path(),
        &[("hello.ts", "export default () => new Response(\"hi\");\n")],
    );
    ctx.lookup_empty("hello");
    let create = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services").json_body(json!({
            "slug": "hello", "runtime": "deno", "name": "hello", "project": "web", "environment": "staging"
        }));
        then.status(201).json_body(service_json("svc_1", "hello"));
    });
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(201).json_body(json!({
            "version": 1, "source_digest": "sha256:abc", "build_state": "pending", "created_at": T0_MS
        }));
    });
    ctx.run(&[
        "push",
        dir.path().join("hello.ts").to_str().unwrap(),
        "--create",
        "--project",
        "web",
        "--env",
        "staging",
    ])
    .ok();
    create.assert();
}

#[test]
fn push_project_and_env_need_create_and_are_checked_locally() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(
        dir.path(),
        &[("hello.ts", "export default () => new Response(\"hi\");\n")],
    );
    let file = dir.path().join("hello.ts");
    let file = file.to_str().unwrap();
    ctx.run(&["push", file, "--project", "web"]).fails_with(2);
    ctx.run(&["push", file, "--create", "--env", "Bad Env"]).fails_with(2);
}
