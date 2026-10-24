//! `21ci types <service>` (RPC contract of a service) and `21ci sdk …` (operator SDKs).
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

const DTS: &str = "export interface NotifyRpc {\n  ping(): Promise<string>;\n}\n";

#[test]
fn types_writes_the_declarations_of_a_service_into_the_out_dir() {
    let ctx = Ctx::new();
    ctx.lookup("notify", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/types")
            .header("authorization", format!("Bearer {TOKEN}"));
        then.status(200)
            .header("content-type", "text/plain; charset=utf-8")
            .body(DTS);
    });
    let dir = ctx.cfg.path().join("types");
    let out = ctx.run(&["types", "notify", "-d", dir.to_str().unwrap()]).ok();
    m.assert();
    assert_eq!(std::fs::read_to_string(dir.join("notify.d.ts")).unwrap(), DTS);
    assert!(out.stdout.contains("notify.d.ts"), "{}", out.stdout);
}

#[test]
fn types_prints_the_declarations_without_an_out_dir() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/types")
            .query_param("version", "4");
        then.status(200).body(DTS);
    });
    let out = ctx.run(&["types", "svc_1", "4"]).ok();
    assert_eq!(out.stdout, DTS);
}

#[test]
fn types_of_a_service_without_an_rpc_surface_fails_with_the_api_error() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/types");
        then.status(422)
            .json_body(json!({"error": {"code": "no_rpc_surface", "message": "no rpc export", "details": {}}}));
    });
    let out = ctx.run(&["types", "svc_1"]);
    assert_ne!(out.code, 0);
    assert!(out.stderr.contains("no_rpc_surface"), "{}", out.stderr);
}

// ---- 21ci sdk (operator) -------------------------------------------------------------------------

#[test]
fn sdk_publish_sends_the_source_and_the_release() {
    let ctx = Ctx::new();
    let dir = ctx.cfg.path().join("acme-sdk");
    write_tree(&dir, &[("mod.ts", "export default 1;\n")]);
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/sdks")
            .body_includes("\"name\":\"acme\"")
            .body_includes("\"version\":\"1.1.0\"")
            .body_includes("\"channel\":\"stable\"")
            .body_includes("\"description\":\"Acme API\"")
            .body_includes("\"source\":\"")
            .body_includes("\"runtime\":\"deno\"");
        then.status(201).json_body(json!({"sdk": "acme", "version": "1.1.0", "version_no": 2, "members": ["ping"], "channels": ["stable"], "created_at": 0}));
    });
    let out = ctx
        .run(&[
            "sdk",
            "publish",
            "acme",
            "1.1.0",
            dir.to_str().unwrap(),
            "--channel",
            "stable",
            "--description",
            "Acme API",
        ])
        .ok();
    m.assert();
    assert!(
        out.stdout.contains("acme 1.1.0") && out.stdout.contains("stable"),
        "{}",
        out.stdout
    );
}

#[test]
fn sdk_promote_and_rollback_move_a_channel() {
    let ctx = Ctx::new();
    let promote = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/sdks/acme/channels/canary")
            .json_body(json!({"version": "1.2.0"}));
        then.status(200)
            .json_body(json!({"channel": "canary", "version": "1.2.0", "previous": "1.1.0", "updated_at": 0}));
    });
    let out = ctx
        .run(&["sdk", "promote", "acme", "1.2.0", "--channel", "canary"])
        .ok();
    promote.assert();
    assert!(
        out.stdout.contains("canary") && out.stdout.contains("1.2.0"),
        "{}",
        out.stdout
    );

    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks");
        then.status(200).json_body(
            json!([{"name": "acme", "state": "active", "service_id": "svc_1", "releases": 2, "latest": "1.1.0",
            "created_at": 0, "description": null,
            "channels": {"stable": {"channel": "stable", "version": "1.1.0", "previous": "1.0.0", "updated_at": 0}}}]),
        );
    });
    let back = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/sdks/acme/channels/stable")
            .json_body(json!({"version": "1.0.0"}));
        then.status(200)
            .json_body(json!({"channel": "stable", "version": "1.0.0", "previous": "1.1.0", "updated_at": 0}));
    });
    ctx.run(&["sdk", "rollback", "acme"]).ok();
    back.assert();
}

#[test]
fn sdk_grants_set_writes_one_grant_and_get_lists_them() {
    let ctx = Ctx::new();
    let set = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/sdk-grants")
            .json_body(json!({"scope": "plan:pro", "sdk": "acme", "effect": "allow", "version_req": "^1", "channel": null}));
        then.status(200).json_body(json!({"scope": "plan:pro", "sdk": "acme", "effect": "allow", "version_req": "^1", "channel": null, "created_by": "root", "updated_at": 0}));
    });
    ctx.run(&[
        "sdk",
        "grants",
        "set",
        "plan:pro",
        "acme",
        "--allow",
        "--version-req",
        "^1",
    ])
    .ok();
    set.assert();

    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdk-grants");
        then.status(200).json_body(json!([{"scope": "node", "sdk": "*", "effect": "allow", "version_req": null, "channel": null, "created_by": "root", "updated_at": 0}]));
    });
    let out = ctx.run(&["sdk", "grants", "get"]).ok();
    assert!(
        out.stdout.contains("node") && out.stdout.contains("allow"),
        "{}",
        out.stdout
    );
}

#[test]
fn sdk_rm_and_releases() {
    let ctx = Ctx::new();
    let rm = ctx.server.mock(|when, then| {
        when.method(DELETE).path("/v1/sdks/acme");
        then.status(204);
    });
    ctx.run(&["sdk", "rm", "acme"]).ok();
    rm.assert();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks/acme/releases");
        then.status(200).json_body(json!([{"sdk": "acme", "version": "1.0.0", "version_no": 1, "members": ["ping"], "channels": ["stable"], "created_at": 0}]));
    });
    let out = ctx.run(&["sdk", "releases", "acme"]).ok();
    assert!(
        out.stdout.contains("1.0.0") && out.stdout.contains("stable"),
        "{}",
        out.stdout
    );
}

// ---- 21ci sdk (tenant) ---------------------------------------------------------------------------

#[test]
fn sdk_ls_of_a_tenant_key_lists_the_effective_sdks() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200)
            .json_body(json!({"principal": "tenant", "tenant_id": "ten_1"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/sdks/effective")
            .query_param("environment", "staging");
        then.status(200).json_body(
            json!([{"name": "acme", "description": null, "version": "1.1.0", "channel": "canary",
            "version_reqs": ["^1"], "members": ["ping"], "reason": null}]),
        );
    });
    let out = ctx.run(&["sdk", "ls", "--environment", "staging"]).ok();
    assert!(
        out.stdout.contains("acme") && out.stdout.contains("1.1.0") && out.stdout.contains("canary"),
        "{}",
        out.stdout
    );
}

#[test]
fn sdk_types_downloads_the_contract() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks/acme/contract");
        then.status(200).body("declare module \"sdk:acme\" {}\n");
    });
    let dir = ctx.cfg.path().join("types");
    ctx.run(&["sdk", "types", "acme", "-d", dir.to_str().unwrap()]).ok();
    m.assert();
    assert_eq!(
        std::fs::read_to_string(dir.join("acme.d.ts")).unwrap(),
        "declare module \"sdk:acme\" {}\n"
    );
}

// ---- edge paths: JSON output, optional inputs, operator and tenant listings ------------------------

#[test]
fn types_by_id_with_an_out_dir_names_the_file_after_the_service_slug() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200).json_body(service_json("svc_1", "notify"));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/types");
        then.status(200).body(DTS);
    });
    let dir = ctx.cfg.path().join("types");
    ctx.run(&["types", "svc_1", "-d", dir.to_str().unwrap()]).ok();
    assert_eq!(std::fs::read_to_string(dir.join("notify.d.ts")).unwrap(), DTS);
}

#[test]
fn types_with_json_reports_the_written_file() {
    let ctx = Ctx::new();
    ctx.lookup("notify", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/types");
        then.status(200).body(DTS);
    });
    let dir = ctx.cfg.path().join("types");
    let out = ctx
        .run(&["--json", "types", "notify", "-d", dir.to_str().unwrap()])
        .ok();
    assert_eq!(out.json()["bytes"], DTS.len());
}

#[test]
fn sdk_publish_with_a_contract_members_and_json_output() {
    let ctx = Ctx::new();
    let dir = ctx.cfg.path().join("acme-sdk");
    write_tree(
        &dir,
        &[
            ("mod.ts", "export default 1;\n"),
            ("contract.d.ts", "declare const x: 1;\n"),
        ],
    );
    let contract = dir.join("contract.d.ts");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/sdks")
            .body_includes("\"contract\":\"declare const x: 1;")
            .body_includes("\"members\":[\"ping\",\"pong\"]");
        then.status(201).json_body(json!({"sdk": "acme", "version": "1.2.0", "version_no": 3, "members": ["ping", "pong"], "channels": [], "created_at": 0}));
    });
    let out = ctx
        .run(&[
            "--json",
            "sdk",
            "publish",
            "acme",
            "1.2.0",
            dir.to_str().unwrap(),
            "--contract",
            contract.to_str().unwrap(),
            "--member",
            "ping",
            "--member",
            "pong",
        ])
        .ok();
    m.assert();
    assert_eq!(out.json()["version_no"], 3);
}

#[test]
fn sdk_publish_without_members_prints_a_dash_for_them() {
    let ctx = Ctx::new();
    let dir = ctx.cfg.path().join("acme-sdk");
    write_tree(&dir, &[("mod.ts", "export default 1;\n")]);
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/sdks");
        then.status(201)
            .json_body(json!({"sdk": "acme", "version": "1.2.0", "version_no": 3}));
    });
    let out = ctx
        .run(&["sdk", "publish", "acme", "1.2.0", dir.to_str().unwrap()])
        .ok();
    let members = out
        .stdout
        .lines()
        .find(|l| l.starts_with("Members:"))
        .unwrap_or_default();
    assert_eq!(members.trim_start_matches("Members:").trim(), "-", "{}", out.stdout);
}

#[test]
fn sdk_ls_of_an_operator_lists_every_sdk_with_its_channels() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200).json_body(json!({"principal": "operator"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks");
        then.status(200).json_body(
            json!([{"name": "acme", "state": "active", "service_id": "svc_1", "releases": 2,
            "latest": "1.1.0", "channels": {"stable": {"version": "1.1.0"}}}]),
        );
    });
    let out = ctx.run(&["sdk", "ls"]).ok();
    assert!(
        out.stdout.contains("acme") && out.stdout.contains("stable=1.1.0"),
        "{}",
        out.stdout
    );
}

#[test]
fn sdk_ls_of_an_operator_with_no_sdks_says_how_to_publish_one() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200).json_body(json!({"principal": "operator"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks");
        then.status(200).json_body(json!([]));
    });
    let out = ctx.run(&["sdk", "ls"]).ok();
    assert!(out.stderr.contains("No SDKs published"), "{}", out.stderr);
}

#[test]
fn sdk_ls_of_a_tenant_key_takes_the_project_and_can_print_json() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200)
            .json_body(json!({"principal": "tenant", "tenant_id": "ten_1"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks/effective").query_param("project", "prj_1");
        then.status(200).json_body(json!([{"name": "acme", "version": "1.1.0", "channel": "stable", "version_reqs": [], "members": [], "reason": null}]));
    });
    let out = ctx.run(&["--json", "sdk", "ls", "--project", "prj_1"]).ok();
    assert_eq!(out.json()[0]["name"], "acme");
}

#[test]
fn sdk_ls_of_a_tenant_key_with_no_grants_says_so() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/me");
        then.status(200)
            .json_body(json!({"principal": "tenant", "tenant_id": "ten_1"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks/effective");
        then.status(200).json_body(json!([]));
    });
    let out = ctx.run(&["sdk", "ls"]).ok();
    assert!(out.stderr.contains("No SDK is granted"), "{}", out.stderr);
}

#[test]
fn sdk_releases_and_promote_print_json_when_asked() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks/acme/releases");
        then.status(200)
            .json_body(json!([{"version": "1.0.0", "version_no": 1, "channels": [], "members": [], "created_at": 0}]));
    });
    let out = ctx.run(&["--json", "sdk", "releases", "acme"]).ok();
    assert_eq!(out.json()[0]["version"], "1.0.0");

    ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/sdks/acme/channels/stable");
        then.status(200)
            .json_body(json!({"channel": "stable", "version": "1.0.0", "previous": null}));
    });
    let out = ctx
        .run(&["--json", "sdk", "promote", "acme", "1.0.0", "--channel", "stable"])
        .ok();
    assert_eq!(out.json()["version"], "1.0.0");
}

#[test]
fn sdk_grants_get_filters_by_sdk_and_handles_empty_and_json() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdk-grants");
        then.status(200).json_body(json!([
            {"scope": "plan:pro", "sdk": "acme", "effect": "allow", "version_req": null, "channel": null, "created_by": "root"},
            {"scope": "plan:free", "sdk": "other", "effect": "deny", "version_req": null, "channel": null, "created_by": "root"}
        ]));
    });
    let out = ctx.run(&["--json", "sdk", "grants", "get", "--sdk", "acme"]).ok();
    assert_eq!(out.json().as_array().unwrap().len(), 1, "only acme's grants");

    let out = ctx.run(&["sdk", "grants", "get", "--sdk", "missing"]).ok();
    assert!(out.stderr.contains("No grants"), "{}", out.stderr);
}

#[test]
fn sdk_grants_set_deny_and_a_bare_set_that_deletes_the_grant() {
    let ctx = Ctx::new();
    let deny = ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/sdk-grants").json_body(json!({"scope": "tenant:ten_1", "sdk": "acme", "effect": "deny", "version_req": null, "channel": null}));
        then.status(200).json_body(json!({"scope": "tenant:ten_1", "sdk": "acme", "effect": "deny", "version_req": null, "channel": null, "created_by": "root", "updated_at": 0}));
    });
    ctx.run(&["sdk", "grants", "set", "tenant:ten_1", "acme", "--deny"])
        .ok();
    deny.assert();

    let del = ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/sdk-grants").json_body(
            json!({"scope": "tenant:ten_1", "sdk": "acme", "effect": null, "version_req": null, "channel": null}),
        );
        then.status(200).json_body(json!(null));
    });
    let out = ctx.run(&["sdk", "grants", "set", "tenant:ten_1", "acme"]).ok();
    del.assert();
    assert!(
        out.stdout.contains("Deleted the grant of acme at tenant:ten_1"),
        "{}",
        out.stdout
    );
}

#[test]
fn sdk_types_passes_project_environment_and_release_and_can_print_json() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/sdks/acme/contract")
            .query_param("project", "prj_1")
            .query_param("environment", "staging")
            .query_param("version", "1.0.0");
        then.status(200).body(DTS);
    });
    let out = ctx
        .run(&[
            "sdk",
            "types",
            "acme",
            "--project",
            "prj_1",
            "--environment",
            "staging",
            "--release",
            "1.0.0",
        ])
        .ok();
    m.assert();
    assert_eq!(out.stdout, DTS);
}

#[test]
fn sdk_types_with_json_and_an_out_dir_reports_the_file() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/sdks/acme/contract");
        then.status(200).body(DTS);
    });
    let dir = ctx.cfg.path().join("types");
    let out = ctx
        .run(&["--json", "sdk", "types", "acme", "-d", dir.to_str().unwrap()])
        .ok();
    assert_eq!(out.json()["bytes"], DTS.len());
}
