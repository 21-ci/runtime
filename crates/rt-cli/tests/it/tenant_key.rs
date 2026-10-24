//! `21ci tenant …` and `21ci key …`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use predicates::prelude::*;
use serde_json::{json, Value};

fn tenant(id: &str, name: &str) -> Value {
    json!({"id": id, "name": name, "plan": "free", "overrides": {}, "created_at": T0_MS, "disabled": false})
}

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

#[test]
fn tenant_create_posts_name_and_plan() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/tenants")
            .header("authorization", auth())
            .header("content-type", "application/json")
            .json_body(json!({"name": "acme", "plan": "pro"}));
        then.status(201).json_body(tenant("ten_1", "acme"));
    });
    let out = ctx.run(&["tenant", "create", "acme", "--plan", "pro"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "ID: ten_1",
            "Name: acme",
            "Plan: free",
            "Disabled: false",
            &format!("Created: {T0}")
        ]
    );
}

#[test]
fn tenant_create_defaults_to_the_free_plan_and_accepts_overrides() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/tenants")
            .json_body(json!({"name": "acme", "plan": "free", "overrides": {"limits": {"memory_mb": 64}}}));
        then.status(201).json_body(tenant("ten_1", "acme"));
    });
    ctx.run(&[
        "tenant",
        "create",
        "acme",
        "--overrides",
        r#"{"limits":{"memory_mb":64}}"#,
    ])
    .ok();
    m.assert();
}

#[test]
fn tenant_create_rejects_invalid_overrides_json() {
    let ctx = Ctx::new();
    ctx.run(&["tenant", "create", "acme", "--overrides", "{nope"])
        .fails_with(2);
}

#[test]
fn tenant_list_renders_a_table() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants")
            .header("authorization", auth())
            .query_param_missing("cursor");
        then.status(200)
            .json_body(json!({"items": [tenant("ten_1", "acme"), tenant("ten_2", "globex")], "next_cursor": null}));
    });
    let out = ctx.run(&["tenant", "list"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID NAME PLAN DISABLED CREATED",
            &format!("ten_1 acme free false {T0}"),
            &format!("ten_2 globex free false {T0}"),
        ]
    );
    // exact column layout: two spaces between columns, no trailing blanks
    assert_eq!(
        out.stdout.lines().next().unwrap(),
        "ID     NAME    PLAN  DISABLED  CREATED"
    );
}

#[test]
fn tenant_list_passes_limit_and_cursor_and_hints_at_the_next_page() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants")
            .query_param("limit", "1")
            .query_param("cursor", "c0");
        then.status(200)
            .json_body(json!({"items": [tenant("ten_1", "acme")], "next_cursor": "c1"}));
    });
    let out = ctx.run(&["tenant", "list", "--limit", "1", "--cursor", "c0"]).ok();
    m.assert();
    assert!(out.stderr.contains("--cursor c1"), "{}", out.stderr);
}

#[test]
fn tenant_list_all_follows_the_cursor() {
    let ctx = Ctx::new();
    let p1 = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants").query_param_missing("cursor");
        then.status(200)
            .json_body(json!({"items": [tenant("ten_1", "acme")], "next_cursor": "c1"}));
    });
    let p2 = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants").query_param("cursor", "c1");
        then.status(200)
            .json_body(json!({"items": [tenant("ten_2", "globex")], "next_cursor": null}));
    });
    let out = ctx.run(&["tenant", "list", "--all"]).ok();
    p1.assert();
    p2.assert();
    assert_eq!(out.lines().len(), 3);
    assert!(out.stderr.is_empty(), "{}", out.stderr);
}

#[test]
fn tenant_list_json_prints_the_page_as_returned() {
    let ctx = Ctx::new();
    let page = json!({"items": [tenant("ten_1", "acme")], "next_cursor": null});
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/tenants");
        then.status(200).json_body(page.clone());
    });
    let out = ctx.run(&["--output", "json", "tenant", "list"]).ok();
    assert_eq!(out.json(), page);
}

#[test]
fn tenant_get_and_json() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants/ten_1")
            .header("authorization", auth());
        then.status(200).json_body(tenant("ten_1", "acme"));
    });
    let out = ctx.run(&["tenant", "get", "ten_1"]).ok();
    assert_eq!(out.lines()[0], "ID: ten_1");
    let out = ctx.run(&["-o", "json", "tenant", "get", "ten_1"]).ok();
    assert_eq!(out.json(), tenant("ten_1", "acme"));
}

#[test]
fn tenant_delete_with_and_without_purge() {
    let ctx = Ctx::new();
    let plain = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/tenants/ten_1")
            .query_param_missing("purge");
        then.status(204);
    });
    let out = ctx.run(&["tenant", "delete", "ten_1"]).ok();
    plain.assert();
    assert_eq!(out.lines(), vec!["Tenant ten_1 deleted."]);

    let purge = ctx.server.mock(|when, then| {
        when.method(DELETE).path("/v1/tenants/ten_2").query_param("purge", "1");
        then.status(204);
    });
    ctx.run(&["tenant", "delete", "ten_2", "--purge"]).ok();
    purge.assert();

    let out = ctx.run(&["-o", "json", "tenant", "delete", "ten_1"]).ok();
    assert_eq!(out.json(), json!({"deleted": "ten_1"}));
}

#[test]
fn tenant_delete_conflict_is_an_api_error() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(DELETE).path("/v1/tenants/ten_1");
        then.status(409)
            .json_body(err_json("tenant_not_empty", "tenant still has services"));
    });
    let out = ctx.run(&["tenant", "delete", "ten_1"]).fails_with(1);
    assert!(
        out.stderr
            .contains("tenant_not_empty: tenant still has services (HTTP 409)"),
        "{}",
        out.stderr
    );
}

// ---- keys ------------------------------------------------------------------------------------

fn key_json(id: &str, name: Option<&str>) -> Value {
    json!({"id": id, "name": name, "scopes": ["services:read", "deploy"], "project_ids": null, "environments": null,
           "expires_at": null, "created_at": T0_MS, "last_used_at": null})
}

#[test]
fn key_create_sends_scopes_and_shows_the_secret_once() {
    let ctx = Ctx::new();
    let mut created = key_json("key_1", Some("ci"));
    created["secret"] = json!("rtk_key_1_s3cr3t");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/tenants/ten_1/keys")
            .header("authorization", auth())
            .json_body(json!({
                "scopes": ["services:read", "deploy"], "name": "ci",
                "project_ids": ["prj_1", "prj_2"], "environments": ["production"], "expires_at": "2027-01-01T00:00:00Z"
            }));
        then.status(201).json_body(created);
    });
    let out = ctx
        .run(&[
            "key",
            "create",
            "--tenant",
            "ten_1",
            "--scopes",
            "services:read,deploy",
            "--name",
            "ci",
            "--project",
            "prj_1,prj_2",
            "--environment",
            "production",
            "--expires-at",
            "2027-01-01T00:00:00Z",
        ])
        .ok();
    m.assert();
    let lines = out.lines();
    assert!(lines.contains(&"ID: key_1".to_string()), "{lines:?}");
    assert!(lines.contains(&"Secret: rtk_key_1_s3cr3t".to_string()), "{lines:?}");
    assert!(lines.contains(&"Scopes: services:read,deploy".to_string()), "{lines:?}");
    assert!(out.stderr.contains("shown only once"), "{}", out.stderr);
}

#[test]
fn key_create_defaults_to_the_working_set_of_scopes() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/tenants/ten_1/keys").json_body(json!({"scopes": [
            "services:read", "services:write", "deploy", "invoke", "logs:read", "env:write", "secrets:write", "queue:write"
        ]}));
        let mut k = key_json("key_1", None);
        k["secret"] = json!("rtk_key_1_x");
        then.status(201).json_body(k);
    });
    ctx.run(&["key", "create", "--tenant", "ten_1"]).ok();
    m.assert();
}

#[test]
fn key_create_json_contains_the_secret() {
    let ctx = Ctx::new();
    let mut created = key_json("key_1", None);
    created["secret"] = json!("rtk_key_1_s3cr3t");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/tenants/ten_1/keys");
        then.status(201).json_body(created.clone());
    });
    let out = ctx.run(&["-o", "json", "key", "create", "--tenant", "ten_1"]).ok();
    assert_eq!(out.json(), created);
}

#[test]
fn key_list_renders_a_table_without_secrets() {
    let ctx = Ctx::new();
    let mut scoped = key_json("key_2", None);
    scoped["project_ids"] = json!(["prj_1"]);
    scoped["environments"] = json!(["production"]);
    scoped["expires_at"] = json!(T0_MS);
    scoped["last_used_at"] = json!(T0_MS);
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants/ten_1/keys")
            .header("authorization", auth());
        then.status(200)
            .json_body(json!([key_json("key_1", Some("ci")), scoped]));
    });
    let out = ctx.run(&["key", "list", "--tenant", "ten_1"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID NAME SCOPES PROJECTS ENVIRONMENTS EXPIRES LAST_USED",
            "key_1 ci services:read,deploy all all never -",
            &format!("key_2 - services:read,deploy prj_1 production {T0} {T0}"),
        ]
    );
}

#[test]
fn key_list_shows_an_empty_restriction_as_none_and_json_is_the_array() {
    let ctx = Ctx::new();
    let mut none = key_json("key_3", None);
    none["project_ids"] = json!([]);
    let keys = json!([none]);
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants/ten_1/keys")
            .header("authorization", auth());
        then.status(200).json_body(keys.clone());
    });
    let out = ctx.run(&["key", "list", "--tenant", "ten_1"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID NAME SCOPES PROJECTS ENVIRONMENTS EXPIRES LAST_USED",
            "key_3 - services:read,deploy none all never -",
        ]
    );
    let out = ctx.run(&["--json", "key", "list", "--tenant", "ten_1"]).ok();
    assert_eq!(out.json(), keys);
}

#[test]
fn key_revoke_deletes_the_key() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/tenants/ten_1/keys/key_1")
            .header("authorization", auth());
        then.status(204);
    });
    let out = ctx.run(&["key", "revoke", "key_1", "--tenant", "ten_1"]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Key key_1 revoked."]);
    let out = ctx
        .run(&["-o", "json", "key", "revoke", "key_1", "--tenant", "ten_1"])
        .ok();
    assert_eq!(out.json(), json!({"revoked": "key_1"}));
}

#[test]
fn requests_carry_the_cli_user_agent() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants")
            .header_prefix("user-agent", "21ci/");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    ctx.cmd()
        .args(["tenant", "list"])
        .assert()
        .success()
        .stderr(predicate::str::contains("No tenants"));
    m.assert();
}

// ---- tenant update, usage --------------------------------------------------------------------

#[test]
fn tenant_update_patches_only_what_is_given() {
    let ctx = Ctx::new();
    let mut t = tenant("ten_1", "acme");
    t["disabled"] = json!(true);
    t["plan"] = json!("pro");
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/tenants/ten_1")
            .header("authorization", auth())
            .json_body(json!({"plan": "pro", "overrides": {"limits": {"cpu_ms": 100}}, "disabled": true}));
        then.status(200).json_body(t.clone());
    });
    let out = ctx
        .run(&[
            "tenant",
            "update",
            "ten_1",
            "--plan",
            "pro",
            "--overrides",
            r#"{"limits":{"cpu_ms":100}}"#,
            "--disable",
        ])
        .ok();
    m.assert();
    assert!(out.lines().contains(&"Disabled: true".to_string()), "{:?}", out.lines());

    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/tenants/ten_1")
            .json_body(json!({"name": "Acme Inc", "disabled": false}));
        then.status(200).json_body(tenant("ten_1", "Acme Inc"));
    });
    ctx.run(&["tenant", "update", "ten_1", "--name", "Acme Inc", "--enable"])
        .ok();
    m.assert();
}

#[test]
fn tenant_update_validates_its_flags() {
    let ctx = Ctx::new();
    ctx.run(&["tenant", "update", "ten_1"]).fails_with(2);
    ctx.run(&["tenant", "update", "ten_1", "--disable", "--enable"])
        .fails_with(2);
    ctx.run(&["tenant", "update", "ten_1", "--overrides", "[1]"])
        .fails_with(2);
    ctx.run(&["tenant", "update", "ten_1", "--overrides", "{nope"])
        .fails_with(2);
}

fn usage_row(svc: &str, day: &str, inv: u64) -> Value {
    json!({"service_id": svc, "day": day, "invocations": inv, "cpu_ms": inv * 10,
           "egress_bytes": 2048, "storage_bytes": 1_048_576})
}

#[test]
fn tenant_usage_renders_rows_and_a_total() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants/ten_1/usage")
            .header("authorization", auth())
            .query_param("from", "2026-10-01")
            .query_param("to", "2026-10-02");
        then.status(200).json_body(json!([
            usage_row("svc_1", "2026-10-01", 100),
            usage_row("svc_1", "2026-10-02", 50),
        ]));
    });
    let out = ctx
        .run(&["tenant", "usage", "ten_1", "--from", "2026-10-01", "--to", "2026-10-02"])
        .ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "DAY SERVICE INVOCATIONS CPU_MS EGRESS STORAGE",
            "2026-10-01 svc_1 100 1000 2.0 KiB 1.0 MiB",
            "2026-10-02 svc_1 50 500 2.0 KiB 1.0 MiB",
            "TOTAL - 150 1500 4.0 KiB -",
        ]
    );
    let out = ctx
        .run(&[
            "-o",
            "json",
            "tenant",
            "usage",
            "ten_1",
            "--from",
            "2026-10-01",
            "--to",
            "2026-10-02",
        ])
        .ok();
    assert_eq!(out.json()[1]["invocations"], 50);
}

#[test]
fn tenant_usage_takes_relative_days_and_reports_nothing() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/tenants/ten_1/usage")
            .query_param_exists("from")
            .query_param_missing("to");
        then.status(200).json_body(json!([]));
    });
    let out = ctx.run(&["tenant", "usage", "ten_1", "--from", "7d"]).ok();
    m.assert();
    assert!(out.stderr.contains("No usage recorded"), "{}", out.stderr);
    ctx.run(&["tenant", "usage", "ten_1", "--from", "last week"])
        .fails_with(2);
}
