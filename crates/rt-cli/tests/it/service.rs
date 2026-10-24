//! `21ci service …` and the slug/id resolution every service command shares.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

#[test]
fn create_posts_slug_and_runtime() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services")
            .header("authorization", auth())
            .json_body(json!({"slug": "hello", "runtime": "deno"}));
        then.status(201).json_body(service_json("svc_1", "hello"));
    });
    let out = ctx.run(&["service", "create", "hello"]).ok();
    m.assert();
    let lines = out.lines();
    assert_eq!(lines[0], "ID: svc_1");
    assert!(lines.contains(&"Slug: hello".to_string()), "{lines:?}");
    assert!(lines.contains(&"Runtime: deno".to_string()), "{lines:?}");
    assert!(lines.contains(&"Live: 3".to_string()), "{lines:?}");
    assert!(lines.contains(&"Build: ready".to_string()), "{lines:?}");
    assert!(lines.contains(&r#"ETag: "7""#.to_string()), "{lines:?}");
}

#[test]
fn create_with_every_option() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services").json_body(json!({
            "slug": "hello", "runtime": "wasm", "name": "Hello", "profile": "balanced", "access": "key"
        }));
        then.status(201).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&[
        "service",
        "create",
        "hello",
        "--runtime",
        "wasm",
        "--name",
        "Hello",
        "--profile",
        "balanced",
        "--access",
        "key",
    ])
    .ok();
    m.assert();
}

#[test]
fn create_validates_the_slug_locally() {
    let ctx = Ctx::new();
    let out = ctx.run(&["service", "create", "Bad_Slug"]).fails_with(2);
    assert!(out.stderr.contains("^[a-z][a-z0-9-]{1,62}$"), "{}", out.stderr);
}

#[test]
fn create_rejects_unknown_runtime() {
    Ctx::new()
        .run(&["service", "create", "hello", "--runtime", "python"])
        .fails_with(2);
}

#[test]
fn create_slug_taken_is_an_api_error() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services");
        then.status(409)
            .json_body(err_json("slug_taken", "slug already in use"));
    });
    let out = ctx.run(&["service", "create", "hello"]).fails_with(1);
    assert_eq!(out.stderr, "error: slug_taken: slug already in use (HTTP 409)\n");
}

#[test]
fn list_renders_a_table() {
    let ctx = Ctx::new();
    let mut idle = service_json_with("svc_2", "idle", None, "\"1\"");
    idle["runtime"] = json!("wasm");
    idle["access"] = json!("key");
    idle["status"]["build_state"] = json!("pending");
    idle["environment"] = json!("staging");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services")
            .header("authorization", auth())
            .query_param_missing("slug");
        then.status(200)
            .json_body(json!({"items": [service_json("svc_1", "hello"), idle], "next_cursor": null}));
    });
    let out = ctx.run(&["service", "list"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID SLUG RUNTIME PROJECT ENV LIVE BUILD ACCESS",
            "svc_1 hello deno default production 3 ready public",
            "svc_2 idle wasm default staging - pending key",
        ]
    );
}

#[test]
fn list_all_and_empty() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    let out = ctx.run(&["service", "list", "--all"]).ok();
    assert_eq!(out.stdout, "");
    assert!(out.stderr.contains("No services found"), "{}", out.stderr);
}

#[test]
fn get_by_slug_resolves_through_the_slug_filter() {
    let ctx = Ctx::new();
    let lookup = ctx.lookup("hello", "svc_1");
    let out = ctx.run(&["service", "get", "hello"]).ok();
    lookup.assert();
    assert_eq!(out.lines()[0], "ID: svc_1");
}

#[test]
fn get_by_id_skips_the_lookup() {
    let ctx = Ctx::new();
    let any_lookup = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param_exists("slug");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    let get = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1")
            .header("authorization", auth());
        then.status(200).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&["service", "get", "svc_1"]).ok();
    get.assert();
    any_lookup.assert_calls(0);
}

#[test]
fn get_json_is_the_service_view() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let out = ctx.run(&["-o", "json", "service", "get", "hello"]).ok();
    assert_eq!(out.json(), service_json("svc_1", "hello"));
}

#[test]
fn unknown_slug_is_not_found() {
    let ctx = Ctx::new();
    ctx.lookup_empty("nope");
    let out = ctx.run(&["service", "get", "nope"]).fails_with(1);
    assert_eq!(out.stderr, "error: not_found: service \"nope\" not found\n");
}

#[test]
fn delete_with_keep_data() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1")
            .query_param("keep_data", "1");
        then.status(204);
    });
    let out = ctx.run(&["service", "delete", "hello", "--keep-data"]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Service hello (svc_1) deleted."]);
}

#[test]
fn delete_by_id_without_keep_data() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1")
            .query_param_missing("keep_data");
        then.status(204);
    });
    let out = ctx.run(&["service", "delete", "svc_1"]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Service svc_1 deleted."]);
    let out = ctx.run(&["-o", "json", "service", "delete", "svc_1"]).ok();
    assert_eq!(out.json(), json!({"deleted": "svc_1"}));
}

#[test]
fn as_tenant_flag_adds_the_operator_tenant_header() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services").header("x-rt-tenant", "ten_9");
        then.status(201).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&["--as-tenant", "ten_9", "service", "create", "hello"]).ok();
    m.assert();
    // and through the environment
    let m2 = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services").header("x-rt-tenant", "ten_env");
        then.status(201).json_body(service_json("svc_1", "other"));
    });
    ctx.cmd()
        .env("RT_TENANT", "ten_env")
        .args(["service", "create", "other"])
        .assert()
        .success();
    m2.assert();
}

// ---- environments and call access (decision D-A1) --------------------------------------------

#[test]
fn create_with_an_environment_and_allowed_callers() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services").json_body(json!({
            "slug": "hello", "runtime": "deno", "environment": "staging",
            "call": {"allowed_callers": ["web", "env:dev"]}
        }));
        let mut svc = service_json("svc_1", "hello");
        svc["environment"] = json!("staging");
        then.status(201).json_body(svc);
    });
    let out = ctx
        .run(&[
            "service",
            "create",
            "hello",
            "--env",
            "staging",
            "--allowed-caller",
            "web",
            "--allowed-caller",
            "env:dev",
        ])
        .ok();
    m.assert();
    assert!(
        out.lines().contains(&"Environment: staging".to_string()),
        "{:?}",
        out.lines()
    );
}

#[test]
fn an_invalid_environment_is_rejected_locally() {
    let out = Ctx::new()
        .run(&["service", "create", "hello", "--env", "Prod Env"])
        .fails_with(2);
    assert!(out.stderr.contains("^[a-z][a-z0-9-]{0,31}$"), "{}", out.stderr);
}

#[test]
fn update_patches_the_environment_and_the_caller_list() {
    let ctx = Ctx::new();
    let lookup = ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/services/svc_1")
            .header("authorization", auth())
            .json_body(json!({"environment": "dev", "call": {"allowed_callers": ["env:staging", "svc_01J00000000000000000000000"]}}));
        let mut svc = service_json("svc_1", "hello");
        svc["environment"] = json!("dev");
        then.status(200).json_body(svc);
    });
    let out = ctx
        .run(&[
            "service",
            "update",
            "hello",
            "--env",
            "dev",
            "--allowed-caller",
            "env:staging",
            "--allowed-caller",
            "svc_01J00000000000000000000000",
        ])
        .ok();
    lookup.assert();
    m.assert();
    assert!(
        out.lines().contains(&"Environment: dev".to_string()),
        "{:?}",
        out.lines()
    );
}

#[test]
fn update_can_clear_the_caller_list() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/services/svc_1")
            .json_body(json!({"call": {"allowed_callers": []}}));
        then.status(200).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&["service", "update", "svc_1", "--no-allowed-callers"]).ok();
    m.assert();
}

#[test]
fn update_needs_a_change_and_valid_entries() {
    Ctx::new().run(&["service", "update", "svc_1"]).fails_with(2);
    let out = Ctx::new()
        .run(&["service", "update", "svc_1", "--allowed-caller", "env:"])
        .fails_with(2);
    assert!(out.stderr.contains("allowed_callers"), "{}", out.stderr);
    Ctx::new()
        .run(&[
            "service",
            "update",
            "svc_1",
            "--allowed-caller",
            "web",
            "--no-allowed-callers",
        ])
        .fails_with(2);
}

#[test]
fn caller_ids_are_sent_canonical_and_the_deno_reuse_knob_is_a_flag() {
    let ctx = Ctx::new();
    // a block edit reads the current block first (PATCH replaces whole blocks)
    let get = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200)
            .header("etag", "\"7\"")
            .json_body(service_json("svc_1", "hello"));
    });
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH).path("/v1/services/svc_1").json_body(json!({
            "call": {"allowed_callers": ["svc_01J00000000000000000000ABC"]},
            "deno": {"reuse_isolates": true}
        }));
        then.status(200).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&[
        "service",
        "update",
        "svc_1",
        "--allowed-caller",
        "svc_01j00000000000000000000abc",
        "--reuse-isolates",
        "true",
    ])
    .ok();
    get.assert();
    m.assert();

    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services").json_body(json!({
            "slug": "worker", "runtime": "deno", "deno": {"reuse_isolates": false}
        }));
        then.status(201).json_body(service_json("svc_2", "worker"));
    });
    ctx.run(&["service", "create", "worker", "--reuse-isolates", "false"])
        .ok();
    m.assert();
}

// ---- config blocks, reuse knobs, the detail view ---------------------------------------------

#[test]
fn get_shows_callers_reuse_knobs_and_config_blocks() {
    let ctx = Ctx::new();
    let mut svc = service_json("svc_1", "hello");
    svc["call"] = json!({"allowed_callers": ["env:staging", "web"]});
    svc["deno"] = json!({"reuse_isolates": true});
    svc["limits"] = json!({"wall_ms": null, "cpu_ms": 500, "memory_mb": null, "max_body_bytes": null});
    svc["scaling"] = json!({"min_instances": null, "max_instances": 8, "max_queued": null, "keep_resident": null});
    svc["triggers"] = json!({"http": true, "interval": true, "cron": false, "queue": true, "call": true});
    svc["egress"] = json!({"allowed_hosts": ["api.example.com"], "denied_hosts": []});
    svc["status"]["last_error"] = json!("boom");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200).json_body(svc);
    });
    let lines = ctx.run(&["service", "get", "svc_1"]).ok().lines();
    for want in [
        "Environment: production",
        "Allowed callers: env:staging, web",
        "Reuse isolates: true",
        "Limits: cpu_ms=500",
        "Scaling: max_instances=8",
        "Triggers: off: cron",
        r#"Egress: allowed_hosts=["api.example.com"]"#,
        "Last error: boom",
    ] {
        assert!(lines.contains(&want.to_string()), "missing {want:?} in {lines:?}");
    }
    assert!(!lines.iter().any(|l| l.starts_with("Ingress:")), "{lines:?}");
}

#[test]
fn get_explains_the_defaults() {
    let ctx = Ctx::new();
    let mut wasm = service_json("svc_2", "edge");
    wasm["runtime"] = json!("wasm");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_2");
        then.status(200).json_body(wasm);
    });
    let lines = ctx.run(&["service", "get", "svc_2"]).ok().lines();
    for want in [
        "Allowed callers: (services in the same environment)",
        "Reuse instances: default (profile)",
        "Limits: -",
        "Scaling: -",
        "Triggers: all on",
    ] {
        assert!(lines.contains(&want.to_string()), "missing {want:?} in {lines:?}");
    }
}

#[test]
fn create_sets_config_blocks_and_the_wasm_reuse_knob() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services").json_body(json!({
            "slug": "edge", "runtime": "wasm",
            "limits": {"cpu_ms": 50},
            "scaling": {"max_instances": 64, "max_queued": 1000},
            "triggers": {"cron": false},
            "egress": {"allowed_hosts": ["api.example.com"]},
            "wasm": {"reuse_instances": true}
        }));
        then.status(201).json_body(service_json("svc_2", "edge"));
    });
    ctx.run(&[
        "service",
        "create",
        "edge",
        "--runtime",
        "wasm",
        "--reuse-instances",
        "true",
        "--set",
        "limits.cpu_ms=50",
        "--set",
        "scaling.max_instances=64",
        "--set",
        "scaling.max_queued=1000",
        "--set",
        "triggers.cron=false",
        "--set",
        r#"egress.allowed_hosts=["api.example.com"]"#,
    ])
    .ok();
    m.assert();
}

#[test]
fn update_merges_into_the_current_blocks_and_sends_if_match() {
    let ctx = Ctx::new();
    let mut svc = service_json("svc_1", "hello");
    svc["scaling"] = json!({"max_instances": 4, "max_queued": 10, "keep_resident": null});
    svc["ingress"] = json!({"allowed_methods": ["GET"], "rate_limit": {"rps": 1, "burst": 2}});
    let lookup = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("slug", "hello");
        then.status(200).json_body(json!({"items": [svc], "next_cursor": null}));
    });
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/services/svc_1")
            .header("if-match", "\"7\"")
            .json_body(json!({
                "name": "Hello", "profile": "min_latency", "access": "key",
                "scaling": {"max_queued": 50, "keep_resident": null},
                "ingress": {"allowed_methods": ["GET"], "rate_limit": {"rps": 9, "burst": 2}}
            }));
        then.status(200).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&[
        "service",
        "update",
        "hello",
        "--name",
        "Hello",
        "--profile",
        "min_latency",
        "--access",
        "key",
        "--set",
        "scaling.max_queued=50",
        "--unset",
        "scaling.max_instances",
        "--set",
        "ingress.rate_limit.rps=9",
    ])
    .ok();
    lookup.assert();
    m.assert();
}

// ---- instances (D-B2) ------------------------------------------------------------------------

#[test]
fn create_sets_the_instance_bounds_with_flags() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services").json_body(json!({
            "slug": "worker", "runtime": "deno",
            "scaling": {"min_instances": 2, "max_instances": 8, "max_queued": 100}
        }));
        then.status(201).json_body(service_json("svc_2", "worker"));
    });
    ctx.run(&[
        "service",
        "create",
        "worker",
        "--min-instances",
        "2",
        "--max-instances",
        "8",
        "--set",
        "scaling.max_queued=100",
    ])
    .ok();
    m.assert();
}

#[test]
fn update_merges_the_instance_flags_into_the_current_scaling_block() {
    let ctx = Ctx::new();
    let mut svc = service_json("svc_1", "hello");
    svc["scaling"] = json!({"min_instances": null, "max_instances": 4, "max_queued": 10, "keep_resident": null});
    let lookup = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("slug", "hello");
        then.status(200).json_body(json!({"items": [svc], "next_cursor": null}));
    });
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH).path("/v1/services/svc_1").json_body(json!({
            "scaling": {"min_instances": 1, "max_instances": 4, "max_queued": 10, "keep_resident": null}
        }));
        then.status(200).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&["service", "update", "hello", "--min-instances", "1"]).ok();
    lookup.assert();
    m.assert();
}

/// `scaling.max_concurrency` is the old name of `scaling.max_instances`: `--set` and `--unset`
/// still take it and send the new one (the server refuses a block that holds both).
#[test]
fn the_legacy_max_concurrency_key_maps_to_max_instances() {
    let ctx = Ctx::new();
    let mut svc = service_json("svc_1", "hello");
    svc["scaling"] = json!({"min_instances": null, "max_instances": 4, "max_queued": null, "keep_resident": null});
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("slug", "hello");
        then.status(200).json_body(json!({"items": [svc], "next_cursor": null}));
    });
    let set = ctx.server.mock(|when, then| {
        when.method(PATCH).path("/v1/services/svc_1").json_body(json!({
            "scaling": {"min_instances": null, "max_instances": 8, "max_queued": null, "keep_resident": null}
        }));
        then.status(200).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&["service", "update", "hello", "--set", "scaling.max_concurrency=8"])
        .ok();
    set.assert();
    let unset = ctx.server.mock(|when, then| {
        when.method(PATCH).path("/v1/services/svc_1").json_body(json!({
            "scaling": {"min_instances": null, "max_queued": null, "keep_resident": null}
        }));
        then.status(200).json_body(service_json("svc_1", "hello"));
    });
    ctx.run(&["service", "update", "hello", "--unset", "scaling.max_concurrency"])
        .ok();
    unset.assert();
}

#[test]
fn contradicting_instance_bounds_are_a_usage_error_before_any_request() {
    let ctx = Ctx::new();
    let out = ctx
        .run(&[
            "service",
            "create",
            "worker",
            "--min-instances",
            "5",
            "--max-instances",
            "2",
        ])
        .fails_with(2);
    assert!(
        out.stderr.contains("min-instances") && out.stderr.contains("max-instances"),
        "{}",
        out.stderr
    );
    let out = ctx
        .run(&["service", "create", "worker", "--max-instances", "0"])
        .fails_with(2);
    assert!(out.stderr.contains("max-instances"), "{}", out.stderr);
}

#[test]
fn get_shows_the_effective_instance_bounds_and_where_they_come_from() {
    let ctx = Ctx::new();
    let mut svc = service_json("svc_1", "hello");
    svc["scaling"] = json!({"min_instances": 9, "max_instances": 50, "max_queued": null, "keep_resident": null});
    svc["effective"] = json!({
        "min_instances": {"value": 4, "source": "plan"},
        "max_instances": {"value": 4, "source": "plan"},
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200).json_body(svc);
    });
    let lines = ctx.run(&["service", "get", "svc_1"]).ok().lines();
    assert!(
        lines.contains(&"Instances: min 4 (plan), max 4 (plan)".to_string()),
        "{lines:?}"
    );
}

#[test]
fn update_of_a_service_changed_meanwhile_is_a_precondition_failure() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(PATCH).path("/v1/services/svc_1");
        then.status(412)
            .json_body(err_json("precondition_failed", "the service changed; read it again"));
    });
    let out = ctx
        .run(&["service", "update", "hello", "--set", "limits.cpu_ms=100"])
        .fails_with(1);
    assert!(out.stderr.contains("precondition_failed"), "{}", out.stderr);
}

#[test]
fn bad_block_edits_are_usage_errors_before_any_request() {
    let ctx = Ctx::new();
    let any = ctx.server.mock(|when, then| {
        when.any_request();
        then.status(200).json_body(json!({}));
    });
    for args in [
        vec!["service", "update", "svc_1", "--set", "nope.x=1"],
        vec!["service", "update", "svc_1", "--set", "limits=1"],
        vec!["service", "update", "svc_1", "--set", "limits.cpu_ms"],
        vec!["service", "update", "svc_1", "--unset", "limits"],
        vec!["service", "update", "svc_1", "--set", "call.allowed_callers=[]"],
        vec!["service", "create", "edge", "--set", "scaling..x=1"],
    ] {
        let out = ctx.run(&args).fails_with(2);
        assert!(out.stderr.starts_with("error: "), "{args:?}: {}", out.stderr);
    }
    any.assert_calls(0);
}

/// `--durable` makes a durable service; `ws.*` fields are set like other blocks.
#[test]
fn create_and_update_a_durable_service_with_ws_knobs() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services")
            .json_body(json!({"slug": "rooms", "runtime": "deno", "durable": true, "ws": {"warm": false}}));
        then.status(201).json_body(service_json("svc_1", "rooms"));
    });
    ctx.run(&["service", "create", "rooms", "--durable", "--set", "ws.warm=false"])
        .ok();
    m.assert();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1");
        then.status(200).json_body(service_json("svc_1", "rooms"));
    });
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/services/svc_1")
            .json_body(json!({"durable": false, "ws": {"allowed_origins": ["https://a.example"]}}));
        then.status(200).json_body(service_json("svc_1", "rooms"));
    });
    ctx.run(&[
        "service",
        "update",
        "svc_1",
        "--durable",
        "false",
        "--set",
        r#"ws.allowed_origins=["https://a.example"]"#,
    ])
    .ok();
    m.assert();
}
