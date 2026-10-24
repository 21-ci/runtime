//! `21ci token create | list | revoke`: self-service API keys of the caller's own tenant
//! (decision D-B4; `21ci key …` is the operator's).
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn key(id: &str, scopes: &[&str]) -> Value {
    json!({
        "id": id, "name": null, "scopes": scopes, "project_ids": null, "environments": null, "expires_at": null,
        "created_at": T0_MS, "last_used_at": null
    })
}

#[test]
fn create_posts_the_scopes_to_the_tenants_own_key_route_and_shows_the_secret_once() {
    let ctx = Ctx::new();
    let mut created = key("key_1", &["services:read", "deploy"]);
    created["name"] = json!("ci");
    created["secret"] = json!("rtk_key1_s3cr3t");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/keys")
            .header("authorization", auth())
            .json_body(json!({"scopes": ["services:read", "deploy"], "name": "ci"}));
        then.status(201).json_body(created);
    });
    let out = ctx
        .run(&["token", "create", "--scopes", "services:read,deploy", "--name", "ci"])
        .ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "ID: key_1",
            "Name: ci",
            "Scopes: services:read,deploy",
            "Projects: all",
            "Environments: all",
            "Expires: never",
            &format!("Created: {T0}"),
            "Secret: rtk_key1_s3cr3t",
        ]
    );
    assert!(out.stderr.contains("shown only once"), "{}", out.stderr);
}

#[test]
fn create_sends_projects_environments_and_an_expiry() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/keys").json_body(json!({
            "scopes": ["logs:read"],
            "project_ids": ["prj_1", "prj_2"],
            "environments": ["production", "staging"],
            "expires_at": "2027-01-01T00:00:00Z"
        }));
        then.status(201).json_body(json!({
            "id": "key_2", "name": null, "scopes": ["logs:read"], "project_ids": ["prj_1", "prj_2"], "environments": ["production", "staging"],
            "expires_at": 1_798_761_600_000i64, "created_at": T0_MS, "last_used_at": null, "secret": "rtk_x"
        }));
    });
    let out = ctx
        .run(&[
            "token",
            "create",
            "--scopes",
            "logs:read",
            "--project",
            "prj_1,prj_2",
            "--environment",
            "production,staging",
            "--expires-at",
            "2027-01-01T00:00:00Z",
        ])
        .ok();
    m.assert();
    assert!(
        out.lines().contains(&"Projects: prj_1,prj_2".to_string())
            && out.lines().contains(&"Environments: production,staging".to_string()),
        "{:?}",
        out.lines()
    );
    assert!(
        out.lines().contains(&"Expires: 2027-01-01T00:00:00Z".to_string()),
        "{:?}",
        out.lines()
    );
}

#[test]
fn create_expires_in_sends_an_rfc3339_time_in_the_future() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/keys").is_true(|req| {
            let body: Value = serde_json::from_slice(req.body_ref()).unwrap_or_default();
            let at = body["expires_at"].as_str().unwrap_or_default();
            let secs_ahead = humantime::parse_rfc3339(at)
                .ok()
                .and_then(|t| t.duration_since(std::time::SystemTime::now()).ok())
                .map(|d| d.as_secs());
            secs_ahead.is_some_and(|s| (3_500..=3_700).contains(&s))
        });
        then.status(201).json_body(json!({
            "id": "key_3", "name": null, "scopes": ["invoke"], "project_ids": null, "environments": null, "expires_at": null,
            "created_at": T0_MS, "last_used_at": null, "secret": "rtk_y"
        }));
    });
    ctx.run(&["token", "create", "--scopes", "invoke", "--expires-in", "1h"])
        .ok();
    m.assert();
}

#[test]
fn create_needs_scopes_and_one_way_to_say_when_it_expires() {
    let ctx = Ctx::new();
    ctx.run(&["token", "create"]).fails_with(2);
    ctx.run(&[
        "token",
        "create",
        "--scopes",
        "invoke",
        "--expires-in",
        "1h",
        "--expires-at",
        "2027-01-01T00:00:00Z",
    ])
    .fails_with(2);
    ctx.run(&["token", "create", "--scopes", "invoke", "--expires-in", "soon"])
        .fails_with(2);
    ctx.run(&["token", "create", "--scopes", "invoke", "--expires-at", "tomorrow"])
        .fails_with(2);
}

#[test]
fn a_refused_escalation_is_an_api_error() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/keys");
        then.status(403).json_body(err_json(
            "insufficient_scope",
            "scope `deploy` is not held by the key that creates the new key",
        ));
    });
    let out = ctx.run(&["token", "create", "--scopes", "deploy"]).fails_with(1);
    assert!(
        out.stderr
            .starts_with("error: insufficient_scope: scope `deploy` is not held"),
        "{}",
        out.stderr
    );
}

#[test]
fn create_json_prints_the_body_with_the_secret() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/keys");
        then.status(201).json_body(json!({
            "id": "key_9", "name": null, "scopes": ["invoke"], "project_ids": null, "environments": null, "expires_at": null,
            "created_at": T0_MS, "last_used_at": null, "secret": "rtk_z"
        }));
    });
    let out = ctx.run(&["--json", "token", "create", "--scopes", "invoke"]).ok();
    assert_eq!(out.json()["secret"], json!("rtk_z"));
    assert_eq!(out.stderr, "", "the --json output has no notes around it");
}

#[test]
fn list_renders_the_tenants_keys() {
    let ctx = Ctx::new();
    let mut a = key("key_1", &["services:read", "keys:write"]);
    a["name"] = json!("admin");
    a["last_used_at"] = json!(T0_MS);
    let mut b = key("key_2", &["invoke"]);
    b["project_ids"] = json!(["prj_1"]);
    b["expires_at"] = json!(1_798_761_600_000i64);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/keys").header("authorization", auth());
        then.status(200).json_body(json!([a, b]));
    });
    let out = ctx.run(&["token", "list"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID NAME SCOPES PROJECTS ENVIRONMENTS EXPIRES LAST_USED",
            &format!("key_1 admin services:read,keys:write all all never {T0}"),
            "key_2 - invoke prj_1 all 2027-01-01T00:00:00Z -",
        ]
    );
}

#[test]
fn list_of_nothing_says_so() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/keys");
        then.status(200).json_body(json!([]));
    });
    let out = ctx.run(&["token", "list"]).ok();
    assert_eq!(out.stdout, "");
    assert!(out.stderr.contains("No keys found"), "{}", out.stderr);
}

#[test]
fn a_caller_without_keys_write_gets_a_scope_error() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/keys");
        then.status(403)
            .json_body(err_json("insufficient_scope", "missing scope `keys:write`"));
    });
    let out = ctx.run(&["token", "list"]).fails_with(1);
    assert_eq!(
        out.stderr,
        "error: insufficient_scope: missing scope `keys:write` (HTTP 403)\n"
    );
}

#[test]
fn revoke_deletes_the_key() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/keys/key_1")
            .header("authorization", auth());
        then.status(204);
    });
    let out = ctx.run(&["token", "revoke", "key_1"]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Key key_1 revoked."]);
    let out = ctx.run(&["--json", "token", "revoke", "key_1"]).ok();
    assert_eq!(out.json(), json!({"revoked": "key_1"}));
}

#[test]
fn revoking_an_unknown_key_is_not_found() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(DELETE).path("/v1/keys/key_x");
        then.status(404).json_body(err_json("not_found", "not found"));
    });
    ctx.run(&["token", "revoke", "key_x"]).fails_with(1);
}

// ---- limits ------------------------------------------------------------------------------------

#[test]
fn limits_shows_what_is_left_of_the_plan() {
    let ctx = Ctx::new();
    let quota = |limit: Value, used: u64, period: &str| {
        let remaining = limit.as_u64().map(|l| l.saturating_sub(used));
        json!({"limit": limit, "used": used, "remaining": remaining, "period": period})
    };
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/limits").header("authorization", auth());
        then.status(200).json_body(json!({
            "plan": "pro", "day": "2026-10-12", "resets_at": 1_791_849_600_000i64,
            "invocations": quota(Value::Null, 12, "day"),
            "egress_bytes": quota(json!(10_485_760_000u64), 3_145_728, "day"),
            "storage_bytes": quota(json!(1_073_741_824u64), 0, "total"),
            "services": quota(json!(10), 3, "total"),
            "schedules": quota(json!(3), 2, "per_service"),
        }));
    });
    let out = ctx.run(&["limits"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "Plan: pro",
            "Day: 2026-10-12 (daily counters reset 2026-10-13T00:00:00Z)",
            "QUOTA LIMIT USED LEFT PERIOD",
            "invocations unlimited 12 unlimited day",
            "egress 9.8 GiB 3.0 MiB 9.8 GiB day",
            "storage 1.0 GiB 0 B 1.0 GiB total",
            "services 10 3 7 total",
            "schedules 3 2 1 per_service",
        ]
    );
    let json = ctx.run(&["--json", "limits"]).ok().json();
    assert_eq!(json["plan"], json!("pro"));
}
