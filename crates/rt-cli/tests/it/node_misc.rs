//! `21ci node …`, `openapi`, `docs`, `version`, `completion`, `init`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use predicates::prelude::*;
use serde_json::json;

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn status_body() -> serde_json::Value {
    json!({
        "version": "0.1.0", "mode": "normal", "default_profile": "balanced", "v8_sandbox": false,
        "processors": 4, "kinds": {"deno": 3, "wasm": 1, "build": 1},
        "memory": {"budget_bytes": 1073741824u64, "pressure": "Normal"}, "inflight": 2
    })
}

// ---- node ------------------------------------------------------------------------------------

#[test]
fn node_status_table_and_json() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status").header("authorization", auth());
        then.status(200).json_body(status_body());
    });
    let out = ctx.run(&["node", "status"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "Version: 0.1.0",
            "Mode: normal",
            "Default profile: balanced",
            "V8 sandbox: false",
            "Processors: 4",
            "Kinds: deno=3 wasm=1 build=1",
            "In flight: 2",
            r#"Memory: {"budget_bytes":1073741824,"pressure":"Normal"}"#,
        ]
    );
    let out = ctx.run(&["-o", "json", "node", "status"]).ok();
    assert_eq!(out.json(), status_body());
}

fn sched_body() -> serde_json::Value {
    json!({
        "uptime_ms": 3_723_000, "mode": "normal",
        "procs": [
            {"id": 0, "kind": "deno", "status": "running", "lrq_len": 2, "runnext": null, "schedtick": 9,
             "machine": 3, "busy_ns": 1_500_000_000u64, "steals_in": 1, "steals_out": 0, "preemptions": 4, "rekinds": 0},
            {"id": 1, "kind": "wasm", "status": "idle", "lrq_len": 0, "runnext": null, "schedtick": 1,
             "machine": null, "busy_ns": 0, "steals_in": 0, "steals_out": 1, "preemptions": 0, "rekinds": 0}
        ],
        "machines": [
            {"id": 3, "proc": 0, "bound_count": 2, "spinning": false, "parked": false},
            {"id": 4, "proc": null, "bound_count": 0, "spinning": false, "parked": true}
        ],
        "grq": {"deno": 5, "wasm": 0, "build": 1},
        "idle": 1, "spinning": 0, "waiting_runtime": 1, "waiting_memory": 0, "inflight": 7,
        "tasks_submitted": 120, "tasks_completed": 113
    })
}

#[test]
fn node_scheduler_summarises_the_snapshot_and_json_is_raw() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/node/scheduler")
            .header("authorization", auth());
        then.status(200).json_body(sched_body());
    });
    let out = ctx.run(&["node", "scheduler"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "Uptime: 1h 2m",
            "Mode: normal",
            "In flight: 7",
            "Tasks: 120 submitted, 113 completed",
            "Global queues: deno=5 wasm=0 build=1",
            "Local queues: 2",
            "Waiting: runtime 1, memory 0",
            "Processors: 2 (idle 1, spinning 0)",
            "Machines: 2 (parked 1, spinning 0, bound tasks 2)",
            "P KIND STATUS LRQ M BUSY_MS STEALS_IN STEALS_OUT PREEMPTIONS",
            "0 deno running 2 3 1500 1 0 4",
            "1 wasm idle 0 - 0 0 1 0",
        ]
    );
    assert_eq!(ctx.run(&["--json", "node", "scheduler"]).ok().json(), sched_body());
}

#[test]
fn node_cache_prints_the_snapshot_json() {
    let ctx = Ctx::new();
    let cache = json!({"tiers": {"t1": {"bytes": 1}}, "hit_rate": 0.5});
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/cache").header("authorization", auth());
        then.status(200).json_body(cache.clone());
    });
    assert_eq!(ctx.run(&["node", "cache"]).ok().json(), cache);
    assert_eq!(ctx.run(&["-o", "json", "node", "cache"]).ok().json(), cache);
}

#[test]
fn node_scheduler_for_a_tenant_key_is_forbidden() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/scheduler");
        then.status(403)
            .json_body(err_json("insufficient_scope", "operator token required"));
    });
    let out = ctx.run(&["node", "scheduler"]).fails_with(1);
    assert_eq!(
        out.stderr,
        "error: insufficient_scope: operator token required (HTTP 403)\n"
    );
}

#[test]
fn node_drain() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/node/drain").header("authorization", auth());
        then.status(202);
    });
    let out = ctx.run(&["node", "drain"]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Node is draining."]);
    assert_eq!(
        ctx.run(&["-o", "json", "node", "drain"]).ok().json(),
        json!({"draining": true})
    );
}

#[test]
fn node_config_set_builds_the_update_body() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/node/config").header("authorization", auth()).header("content-type", "application/json")
            .json_body(json!({"degradation_mode": "shed", "default_profile": "balanced", "rate_limit": {"rps": 100, "burst": 200}}));
        then.status(200).json_body(json!({"applied": ["degradation_mode", "default_profile"], "pending_restart": ["rate_limit"]}));
    });
    let out = ctx
        .run(&[
            "node",
            "config",
            "set",
            "degradation.mode=shed",
            "default_profile=balanced",
            "rate_limit.rps=100",
            "rate_limit.burst=200",
        ])
        .ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "Applied: degradation_mode, default_profile",
            "Pending restart: rate_limit"
        ]
    );
}

#[test]
fn node_config_set_accepts_a_json_object_value() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/node/config")
            .json_body(json!({"rate_limit": {"rps": 5}}));
        then.status(200)
            .json_body(json!({"applied": ["rate_limit"], "pending_restart": []}));
    });
    let out = ctx.run(&["node", "config", "set", r#"rate_limit={"rps":5}"#]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Applied: rate_limit"]);
}

#[test]
fn node_config_set_validates_arguments() {
    let ctx = Ctx::new();
    ctx.run(&["node", "config", "set"]).fails_with(2);
    ctx.run(&["node", "config", "set", "novalue"]).fails_with(2);
    ctx.run(&["node", "config", "set", "mystery=1"]).fails_with(2);
}

// ---- openapi / docs / version ----------------------------------------------------------------

#[test]
fn openapi_prints_the_document() {
    let ctx = Ctx::new();
    let doc = json!({"openapi": "3.1.0", "info": {"title": "t", "version": "1"}, "paths": {}});
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/openapi.json");
        then.status(200).json_body(doc.clone());
    });
    let out = ctx.run(&["openapi"]).ok();
    assert_eq!(out.json(), doc);
    assert!(out.stdout.contains("\n  \"openapi\""), "pretty printed with two spaces");
}

#[test]
fn openapi_out_writes_a_file() {
    let ctx = Ctx::new();
    let doc = json!({"openapi": "3.1.0", "paths": {"/v1/x": {}}});
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/openapi.json");
        then.status(200).json_body(doc.clone());
    });
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("openapi.json");
    let out = ctx.run(&["openapi", "--out", file.to_str().unwrap()]).ok();
    let written = std::fs::read_to_string(&file).unwrap();
    assert!(written.ends_with('\n'));
    assert_eq!(serde_json::from_str::<serde_json::Value>(&written).unwrap(), doc);
    assert_eq!(
        out.lines(),
        vec![format!("Wrote {} bytes to {}", written.len(), file.display())]
    );
}

#[test]
fn docs_prints_the_scalar_url() {
    let ctx = Ctx::new();
    ctx.cmd()
        .arg("docs")
        .assert()
        .success()
        .stdout(predicate::eq(format!("{}/docs\n", ctx.base())));
    let out = ctx.run(&["-o", "json", "docs"]).ok();
    assert_eq!(out.json(), json!({"url": format!("{}/docs", ctx.base())}));
}

#[test]
fn version_prints_the_client_version() {
    let ctx = Ctx::new();
    let v = env!("CARGO_PKG_VERSION");
    let out = ctx.run(&["version"]).ok();
    assert_eq!(out.stdout, format!("21ci {v}\n"));
    assert_eq!(ctx.run(&["-o", "json", "version"]).ok().json(), json!({"client": v}));
}

#[test]
fn version_server_asks_the_node() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(200).json_body(status_body());
    });
    let v = env!("CARGO_PKG_VERSION");
    let out = ctx.run(&["version", "--server"]).ok();
    assert_eq!(out.lines(), vec![format!("21ci {v}"), "server 0.1.0".to_string()]);
    assert_eq!(
        ctx.run(&["-o", "json", "version", "--server"]).ok().json(),
        json!({"client": v, "server": "0.1.0"})
    );
}

#[test]
fn completion_scripts() {
    let ctx = Ctx::new();
    for shell in ["bash", "zsh", "fish"] {
        let out = ctx.run(&["completion", shell]).ok();
        assert!(out.stdout.len() > 200, "{shell}: {}", out.stdout);
        assert!(out.stdout.contains("21ci"), "{shell}");
        assert!(out.stdout.contains("rollback"), "{shell} knows the subcommands");
    }
    ctx.run(&["completion", "tcsh"]).fails_with(2);
}

// ---- init ------------------------------------------------------------------------------------

fn sample(rel: &str) -> String {
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../sample")
        .join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

#[test]
fn init_deno_scaffolds_the_hello_sample() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("my-app");
    let out = ctx.run(&["init", target.to_str().unwrap(), "--template", "deno"]).ok();
    assert_eq!(
        std::fs::read_to_string(target.join("main.ts")).unwrap(),
        sample("deno/hello/main.ts")
    );
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert!(readme.contains("21ci push . --service my-app --create"), "{readme}");
    assert!(!readme.contains("deploy.sh"));
    let lines = out.lines();
    assert_eq!(lines[0], format!("Created {} (template deno)", target.display()));
    assert!(
        lines.contains(&"main.ts".to_string()) && lines.contains(&"README.md".to_string()),
        "{lines:?}"
    );
    assert!(lines.last().unwrap().starts_with("Next: cd "), "{lines:?}");
}

#[test]
fn init_defaults_to_the_deno_template() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("x1");
    ctx.run(&["init", target.to_str().unwrap()]).ok();
    assert!(target.join("main.ts").exists());
}

#[test]
fn init_deno_kv() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("counter");
    ctx.run(&["init", target.to_str().unwrap(), "--template", "deno-kv"])
        .ok();
    assert_eq!(
        std::fs::read_to_string(target.join("main.ts")).unwrap(),
        sample("deno/kv-counter/main.ts")
    );
    assert!(target.join("README.md").exists());
}

#[test]
fn init_wasm_rust() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("proj-x");
    ctx.run(&["init", target.to_str().unwrap(), "--template", "wasm-rust"])
        .ok();
    assert_eq!(
        std::fs::read_to_string(target.join("src/lib.rs")).unwrap(),
        sample("wasm/rust-hello/src/lib.rs")
    );
    let cargo = std::fs::read_to_string(target.join("Cargo.toml")).unwrap();
    assert!(cargo.contains(r#"name = "proj-x-wasm""#), "{cargo}");
    assert!(cargo.contains("\n[workspace]"), "standalone crate: {cargo}");
    let build = std::fs::read_to_string(target.join("build.sh")).unwrap();
    assert!(
        build.contains("target/wasm32-wasip2/release/proj_x_wasm.wasm"),
        "{build}"
    );
    assert!(!build.contains("deploy.sh"), "{build}");
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert!(
        readme.contains("21ci push target/wasm32-wasip2/release/proj_x_wasm.wasm --service proj-x --create"),
        "{readme}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            std::fs::metadata(target.join("build.sh")).unwrap().permissions().mode() & 0o111,
            0,
            "build.sh is executable"
        );
    }
}

#[test]
fn init_wasm_js() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("js-fn");
    ctx.run(&["init", target.to_str().unwrap(), "--template", "wasm-js"])
        .ok();
    assert_eq!(
        std::fs::read_to_string(target.join("src/app.js")).unwrap(),
        sample("wasm/js-componentize/src/app.js")
    );
    assert_eq!(
        std::fs::read_to_string(target.join("wit/world.wit")).unwrap(),
        sample("wasm/js-componentize/wit/world.wit")
    );
    for dep in ["wasi-clocks", "wasi-http", "wasi-io"] {
        assert_eq!(
            std::fs::read_to_string(target.join(format!("wit/deps/{dep}/package.wit"))).unwrap(),
            sample(&format!("wasm/js-componentize/wit/deps/{dep}/package.wit")),
        );
    }
    assert!(target.join("package.json").exists());
    let build = std::fs::read_to_string(target.join("build.sh")).unwrap();
    assert!(!build.contains("deploy.sh"), "{build}");
    assert!(!build.contains("npm ci"), "no lock file is shipped: {build}");
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert!(
        readme.contains("21ci push dist/app.wasm --service js-fn --create"),
        "{readme}"
    );
}

#[test]
fn init_refuses_a_path_that_is_a_file() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("taken");
    std::fs::write(&file, "mine").unwrap();
    let out = ctx.run(&["init", file.to_str().unwrap()]).fails_with(2);
    assert!(out.stderr.contains("exists and is not a directory"), "{}", out.stderr);
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "mine");
}

#[test]
fn init_refuses_a_non_empty_directory_unless_forced() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("keep.txt", "mine")]);
    let out = ctx.run(&["init", dir.path().to_str().unwrap()]).fails_with(2);
    assert!(out.stderr.contains("not empty"), "{}", out.stderr);
    assert!(!dir.path().join("main.ts").exists());
    ctx.run(&["init", dir.path().to_str().unwrap(), "--force"]).ok();
    assert!(dir.path().join("main.ts").exists());
    assert_eq!(std::fs::read_to_string(dir.path().join("keep.txt")).unwrap(), "mine");
}

#[test]
fn init_rejects_unknown_templates_and_names() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    ctx.run(&["init", dir.path().join("a").to_str().unwrap(), "--template", "cobol"])
        .fails_with(2);
    // a directory name that does not make a valid slug needs --name
    let out = ctx
        .run(&["init", dir.path().join("1st-app").to_str().unwrap()])
        .fails_with(2);
    assert!(out.stderr.contains("--name"), "{}", out.stderr);
    ctx.run(&[
        "init",
        dir.path().join("1st-app").to_str().unwrap(),
        "--name",
        "fine-name",
    ])
    .ok();
    // a messy but sluggable directory name is cleaned up
    let out = ctx
        .run(&["-o", "json", "init", dir.path().join("My Cool_App").to_str().unwrap()])
        .ok()
        .json();
    assert_eq!(out["name"], "my-cool-app");
}

#[test]
fn init_json() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("j1");
    let v = ctx
        .run(&["-o", "json", "init", target.to_str().unwrap(), "--template", "deno-kv"])
        .ok()
        .json();
    assert_eq!(v["template"], "deno-kv");
    assert_eq!(v["name"], "j1");
    assert_eq!(v["files"], json!(["README.md", "main.ts"]));
}

#[test]
fn version_server_failure_still_prints_the_client_version_and_fails() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(403)
            .json_body(err_json("insufficient_scope", "operator token required"));
    });
    let out = ctx.run(&["version", "--server"]).fails_with(1);
    assert_eq!(out.stdout, format!("21ci {}\n", env!("CARGO_PKG_VERSION")));
    assert!(out.stderr.contains("insufficient_scope"), "{}", out.stderr);
}

#[test]
fn node_scheduler_lists_the_admission_state_per_service() {
    let ctx = Ctx::new();
    let mut body = sched_body();
    body["procs"] = json!([]);
    body["admission"] = json!({"queued": 2, "queued_bytes": 8192, "max_inflight": 256, "max_queued": 1024, "max_queued_bytes": 67108864});
    body["services"] = json!([
        {"service_id": "svc_1", "tenant_id": "ten_1", "slug": "hello", "kind": "deno", "limit": 8, "min": 1, "max": 64,
         "running": 3, "queued": 2, "queued_bytes": 8192, "max_queued": 256, "queue_wait_ms": 12.5, "queue_timeouts": 1,
         "demand": 4.0, "latency_baseline_ms": 7.0, "warm_per_proc": 2, "idle_ms": 0},
        {"service_id": "svc_2", "tenant_id": "ten_1", "slug": null, "kind": "wasm", "limit": 4, "min": 1, "max": 16,
         "running": 0, "queued": 0, "queued_bytes": 0, "max_queued": 64, "queue_wait_ms": 0.0, "queue_timeouts": 0,
         "demand": 0.0, "latency_baseline_ms": null, "warm_per_proc": 0, "idle_ms": 5000}
    ]);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/scheduler");
        then.status(200).json_body(body);
    });
    let lines = ctx.run(&["node", "scheduler"]).ok().lines();
    assert!(
        lines.contains(&"Admission: queued 2 (max 1024), max in flight 256".to_string()),
        "{lines:?}"
    );
    let at = lines
        .iter()
        .position(|l| l.starts_with("SERVICE "))
        .expect("services table");
    assert_eq!(
        lines[at..].to_vec(),
        vec![
            "SERVICE KIND LIMIT RUNNING QUEUED WAIT_MS TIMEOUTS WARM/P",
            "hello deno 8 (1..64) 3 2 12.5 1 2",
            "svc_2 wasm 4 (1..16) 0 0 0.0 0 0",
        ]
    );
    assert!(
        !lines.iter().any(|l| l.starts_with("P KIND")),
        "no processors, no processor table"
    );
}

#[test]
fn version_server_uses_v1_version_when_the_node_has_it() {
    let ctx = Ctx::new();
    let v = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/version").header("authorization", auth());
        then.status(200).json_body(json!({"version": "0.2.0", "api": "1.0.0"}));
    });
    let status = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/node/status");
        then.status(403)
            .json_body(err_json("insufficient_scope", "operator token required"));
    });
    let out = ctx.run(&["version", "--server"]).ok();
    v.assert();
    status.assert_calls(0);
    assert_eq!(out.lines()[1], "server 0.2.0");
}
