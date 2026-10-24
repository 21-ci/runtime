//! `21ci push`: artifact formats (same as sample/scripts/deploy.sh), service creation, activation,
//! compile modes and `--wait`.
use crate::common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::*;
use httpmock::prelude::*;
use httpmock::HttpMockResponse;
use serde_json::{json, Value};

fn upload_ok(version: u32) -> Value {
    json!({"version": version, "source_digest": "sha256:abc", "build_state": "pending", "created_at": T0_MS})
}

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

/// An existing service `hello`/`svc_1`.
fn existing(ctx: &Ctx) {
    ctx.lookup("hello", "svc_1");
}

fn body_is(expected: &'static [u8]) -> impl Fn(&httpmock::HttpMockRequest) -> bool + Send + Sync + 'static {
    move |req| req.body_ref() == expected
}

fn body_json_is(expected: Value) -> impl Fn(&httpmock::HttpMockRequest) -> bool + Send + Sync + 'static {
    move |req| serde_json::from_slice::<Value>(req.body_ref()).is_ok_and(|v| v == expected)
}

const TS: &str = "export default () => new Response(\"hi\");\n";

#[test]
fn single_ts_file_creates_the_service_and_uploads_it() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    let lookup = ctx.lookup_empty("hello");
    let create = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services")
            .header("authorization", auth())
            .json_body(json!({"slug": "hello", "runtime": "deno", "name": "hello"}));
        then.status(201).json_body(service_json("svc_1", "hello"));
    });
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .header("authorization", auth())
            .header("content-type", "application/typescript")
            .query_param("activate", "1")
            .query_param_missing("compile")
            .is_true(body_is(TS.as_bytes()));
        then.status(201).json_body(upload_ok(1));
    });
    let out = ctx
        .run(&["push", dir.path().join("hello.ts").to_str().unwrap(), "--create"])
        .ok();
    lookup.assert();
    create.assert();
    upload.assert();
    assert_eq!(
        out.lines(),
        vec![
            "Service: svc_1 (slug: hello, runtime: deno, created)",
            &format!("Upload: application/typescript, {} bytes", TS.len()),
            "Version: 1 (build_state: pending, sha256:abc)",
            "Live: yes",
            "Invoke: http://invoke.example/s/hello/",
        ]
    );
}

#[test]
fn js_and_mjs_files_are_sent_as_javascript() {
    for name in ["app.js", "app.mjs"] {
        let ctx = Ctx::new();
        let dir = tempfile::tempdir().unwrap();
        write_tree(dir.path(), &[(name, "export default () => new Response('x');\n")]);
        ctx.lookup("app", "svc_7");
        let upload = ctx.server.mock(|when, then| {
            when.method(POST)
                .path("/v1/services/svc_7/versions")
                .header("content-type", "text/javascript");
            then.status(201).json_body(upload_ok(1));
        });
        ctx.run(&["push", dir.path().join(name).to_str().unwrap()]).ok();
        upload.assert();
    }
}

#[test]
fn wasm_file_is_created_as_a_wasm_service_and_sent_raw() {
    const WASM: &[u8] = b"\0asm\x0d\0\x01\0 component bytes \xff\xfe";
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("rust_hello.wasm"), WASM).unwrap();
    ctx.lookup_empty("rust-hello");
    let create = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services")
            .json_body(json!({"slug": "rust-hello", "runtime": "wasm", "name": "rust-hello"}));
        let mut v = service_json("svc_2", "rust-hello");
        v["runtime"] = json!("wasm");
        then.status(201).json_body(v);
    });
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_2/versions")
            .header("content-type", "application/wasm")
            .is_true(body_is(WASM));
        then.status(201).json_body(upload_ok(1));
    });
    let out = ctx
        .run(&[
            "push",
            dir.path().join("rust_hello.wasm").to_str().unwrap(),
            "--service",
            "rust-hello",
            "--create",
        ])
        .ok();
    create.assert();
    upload.assert();
    assert!(
        out.lines()
            .contains(&"Service: svc_2 (slug: rust-hello, runtime: wasm, created)".to_string()),
        "{:?}",
        out.lines()
    );
}

#[test]
fn archives_are_passed_through_with_their_content_type() {
    for (name, ct) in [("bundle.zip", "application/zip"), ("bundle.tar", "application/x-tar")] {
        let ctx = Ctx::new();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(name), b"PK\x03\x04binary").unwrap();
        ctx.lookup("bundle", "svc_3");
        let upload = ctx.server.mock(|when, then| {
            when.method(POST)
                .path("/v1/services/svc_3/versions")
                .header("content-type", ct)
                .is_true(body_is(b"PK\x03\x04binary"));
            then.status(201).json_body(upload_ok(2));
        });
        ctx.run(&["push", dir.path().join(name).to_str().unwrap()]).ok();
        upload.assert();
    }
}

#[test]
fn existing_service_is_reused_without_create() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    let create = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services");
        then.status(201).json_body(service_json("svc_x", "hello"));
    });
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(201).json_body(upload_ok(4));
    });
    let out = ctx.run(&["push", dir.path().join("hello.ts").to_str().unwrap()]).ok();
    create.assert_calls(0);
    assert_eq!(out.lines()[0], "Service: svc_1 (slug: hello, existing)");
    assert!(out
        .lines()
        .contains(&"Version: 4 (build_state: pending, sha256:abc)".to_string()));
}

#[test]
fn missing_service_without_create_is_an_error_that_mentions_create() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    ctx.lookup_empty("hello");
    let out = ctx
        .run(&["push", dir.path().join("hello.ts").to_str().unwrap()])
        .fails_with(1);
    assert!(out.stderr.contains("not_found"), "{}", out.stderr);
    assert!(out.stderr.contains("--create"), "{}", out.stderr);
}

#[test]
fn create_race_slug_taken_falls_back_to_the_existing_service() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    // first lookup: nothing; second lookup (after 409): the service somebody else created
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let svc = service_json("svc_1", "hello");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services").query_param("slug", "hello");
        then.respond_with(move |_| {
            let n = c.fetch_add(1, Ordering::SeqCst);
            let items = if n == 0 { json!([]) } else { json!([svc.clone()]) };
            HttpMockResponse::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(json!({"items": items, "next_cursor": null}).to_string())
                .build()
        });
    });
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services");
        then.status(409)
            .json_body(err_json("slug_taken", "slug already in use"));
    });
    let upload = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(201).json_body(upload_ok(1));
    });
    let out = ctx
        .run(&["push", dir.path().join("hello.ts").to_str().unwrap(), "--create"])
        .ok();
    upload.assert();
    assert_eq!(out.lines()[0], "Service: svc_1 (slug: hello, existing)");
}

#[test]
fn dir_with_rt_json_becomes_a_json_bundle() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("app");
    write_tree(
        &app,
        &[
            ("rt.json", r#"{"entry":"main.ts","imports":{"zod":"npm:zod@3"}}"#),
            (
                "main.ts",
                "import { z } from \"zod\";\nexport default () => new Response(\"ok\");\n",
            ),
            ("lib/util.ts", "export const x = 1;\n"),
            ("data.json", "{\"a\":1}\n"),
            ("notes.md", "not a source file"),
            (".hidden/secret.ts", "export {};"),
            (".env", "A=1"),
            ("node_modules/pkg/index.js", "module.exports = 1;"),
        ],
    );
    ctx.lookup("app", "svc_1");
    let expected = json!({
        "entry": "main.ts",
        "files": {
            "data.json": "{\"a\":1}\n",
            "lib/util.ts": "export const x = 1;\n",
            "main.ts": "import { z } from \"zod\";\nexport default () => new Response(\"ok\");\n",
        },
        "imports": {"zod": "npm:zod@3"}
    });
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .header("content-type", "application/json")
            .is_true(body_json_is(expected));
        then.status(201).json_body(upload_ok(1));
    });
    let out = ctx.run(&["push", app.to_str().unwrap()]).ok();
    upload.assert();
    assert!(
        out.lines()[1].starts_with("Upload: application/json,"),
        "{:?}",
        out.lines()
    );
}

#[test]
fn bundle_without_imports_omits_the_key_and_orders_entry_files_imports() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("app");
    write_tree(
        &app,
        &[("rt.json", r#"{"entry":"main.ts"}"#), ("main.ts", "export {};\n")],
    );
    ctx.lookup("app", "svc_1");
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .header("content-type", "application/json")
            .is_true(|req| req.body_string() == r#"{"entry":"main.ts","files":{"main.ts":"export {};\n"}}"#);
        then.status(201).json_body(upload_ok(1));
    });
    ctx.run(&["push", app.to_str().unwrap()]).ok();
    upload.assert();
}

#[test]
fn dir_without_rt_json_and_one_source_is_a_plain_single_file_upload() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("hello");
    write_tree(&app, &[("main.ts", TS), ("README.md", "# readme")]);
    ctx.lookup("hello", "svc_1");
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .header("content-type", "application/typescript")
            .is_true(body_is(TS.as_bytes()));
        then.status(201).json_body(upload_ok(1));
    });
    ctx.run(&["push", app.to_str().unwrap()]).ok();
    upload.assert();
}

#[test]
fn dir_without_rt_json_and_several_sources_is_a_bundle_with_a_guessed_entry() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("app");
    write_tree(&app, &[("index.ts", "import './b.ts';\n"), ("b.ts", "export {};\n")]);
    ctx.lookup("app", "svc_1");
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .header("content-type", "application/json")
            .is_true(body_json_is(
                json!({"entry": "index.ts", "files": {"b.ts": "export {};\n", "index.ts": "import './b.ts';\n"}}),
            ));
        then.status(201).json_body(upload_ok(1));
    });
    ctx.run(&["push", app.to_str().unwrap()]).ok();
    upload.assert();
}

#[test]
fn dir_with_several_sources_and_no_entry_needs_rt_json() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("app");
    write_tree(&app, &[("a.ts", "export {};\n"), ("b.ts", "export {};\n")]);
    let out = ctx
        .run(&["push", app.to_str().unwrap(), "--service", "app", "--create"])
        .fails_with(2);
    assert!(out.stderr.contains("rt.json"), "{}", out.stderr);
}

#[test]
fn dir_without_sources_is_a_usage_error() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("README.md", "x")]);
    let out = ctx
        .run(&["push", dir.path().to_str().unwrap(), "--service", "app"])
        .fails_with(2);
    assert!(out.stderr.contains("no source files"), "{}", out.stderr);
}

#[test]
fn rt_json_entry_must_exist() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(
        dir.path(),
        &[("rt.json", r#"{"entry":"nope.ts"}"#), ("main.ts", "export {};")],
    );
    let out = ctx
        .run(&["push", dir.path().to_str().unwrap(), "--service", "app"])
        .fails_with(2);
    assert!(out.stderr.contains("nope.ts"), "{}", out.stderr);
}

#[test]
fn slug_defaults_to_the_slugified_file_stem_and_can_be_overridden() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("My_Cool App.ts", TS)]);
    let lookup = ctx.lookup("my-cool-app", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(201).json_body(upload_ok(1));
    });
    ctx.run(&["push", dir.path().join("My_Cool App.ts").to_str().unwrap()])
        .ok();
    lookup.assert();

    let other = ctx.lookup("custom", "svc_9");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_9/versions");
        then.status(201).json_body(upload_ok(1));
    });
    let out = ctx
        .run(&[
            "push",
            dir.path().join("My_Cool App.ts").to_str().unwrap(),
            "--service",
            "custom",
        ])
        .ok();
    other.assert();
    assert!(out.lines().last().unwrap().ends_with("/s/custom/"));
}

#[test]
fn invalid_derived_slug_asks_for_service() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("1abc.ts", TS)]);
    let out = ctx
        .run(&["push", dir.path().join("1abc.ts").to_str().unwrap()])
        .fails_with(2);
    assert!(out.stderr.contains("--service"), "{}", out.stderr);
}

#[test]
fn bad_inputs_are_usage_errors() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("notes.txt", "x"), ("a.ts", TS), ("b.wasm", "\0asm")]);
    ctx.run(&[
        "push",
        dir.path().join("missing.ts").to_str().unwrap(),
        "--service",
        "x",
    ])
    .fails_with(2);
    let out = ctx
        .run(&[
            "push",
            dir.path().join("notes.txt").to_str().unwrap(),
            "--service",
            "xx",
        ])
        .fails_with(2);
    assert!(out.stderr.contains("unsupported file type"), "{}", out.stderr);
    // runtime flag contradicting the artifact
    ctx.run(&[
        "push",
        dir.path().join("a.ts").to_str().unwrap(),
        "--service",
        "xx",
        "--runtime",
        "wasm",
    ])
    .fails_with(2);
    ctx.run(&[
        "push",
        dir.path().join("b.wasm").to_str().unwrap(),
        "--service",
        "xx",
        "--runtime",
        "deno",
    ])
    .fails_with(2);
    ctx.run(&[
        "push",
        dir.path().join("a.ts").to_str().unwrap(),
        "--service",
        "xx",
        "--compile",
        "sometimes",
    ])
    .fails_with(2);
}

#[test]
fn no_activate_leaves_the_live_version_alone() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .query_param_missing("activate");
        then.status(201).json_body(upload_ok(5));
    });
    let out = ctx
        .run(&["push", dir.path().join("hello.ts").to_str().unwrap(), "--no-activate"])
        .ok();
    upload.assert();
    let lines = out.lines();
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("Live: no") && l.contains("not activated")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("21ci rollback hello --to 5")),
        "{lines:?}"
    );
    assert!(!lines.iter().any(|l| l.starts_with("Invoke:")), "{lines:?}");
}

#[test]
fn a_symlink_in_the_source_dir_is_not_part_of_the_upload() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("hello");
    write_tree(&app, &[("app.ts", TS)]);
    // an alias of the one source file: were it collected, the upload would be a bundle
    std::os::unix::fs::symlink(app.join("app.ts"), app.join("alias.ts")).unwrap();
    existing(&ctx);
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .header("content-type", "application/typescript")
            .is_true(body_is(TS.as_bytes()));
        then.status(201).json_body(upload_ok(1));
    });
    ctx.run(&["push", app.to_str().unwrap()]).ok();
    upload.assert();
}

#[test]
fn a_lone_source_file_of_no_known_kind_needs_an_entry_point() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    let app = dir.path().join("hello");
    write_tree(&app, &[("app.tsx", TS)]);
    let out = ctx.run(&["push", app.to_str().unwrap()]).fails_with(2);
    assert!(out.stderr.contains("cannot guess the entry point"), "{}", out.stderr);
}

#[test]
fn compile_lazy_is_forwarded_as_the_query_value() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .query_param("compile", "lazy");
        then.status(201).json_body(upload_ok(1));
    });
    ctx.run(&[
        "push",
        dir.path().join("hello.ts").to_str().unwrap(),
        "--compile",
        "lazy",
    ])
    .ok();
    upload.assert();
}

#[test]
fn compile_mode_is_forwarded() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .query_param("activate", "1")
            .query_param("compile", "eager");
        then.status(202).json_body(
            json!({"version": 2, "source_digest": "sha256:abc", "build_state": "building", "created_at": T0_MS}),
        );
    });
    let out = ctx
        .run(&[
            "push",
            dir.path().join("hello.ts").to_str().unwrap(),
            "--compile",
            "eager",
        ])
        .ok();
    upload.assert();
    assert!(
        out.lines()
            .contains(&"Version: 2 (build_state: building, sha256:abc)".to_string()),
        "{:?}",
        out.lines()
    );
}

/// `GET …/versions/N` answering `states` in order (the last one repeats).
fn version_states<'a>(ctx: &'a Ctx, v: u32, states: Vec<&'static str>, failure: Option<Value>) -> httpmock::Mock<'a> {
    let n = Arc::new(AtomicUsize::new(0));
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/services/svc_1/versions/{v}"))
            .header("authorization", auth());
        then.respond_with(move |_| {
            let i = n.fetch_add(1, Ordering::SeqCst).min(states.len() - 1);
            let mut body = version_json(v, states[i]);
            if states[i] == "failed" {
                body["failure"] = failure.clone().unwrap_or(json!({"reason": "build_failed"}));
            }
            HttpMockResponse::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(body.to_string())
                .build()
        });
    })
}

#[test]
fn wait_implies_eager_and_polls_until_ready() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .query_param("activate", "1")
            .query_param("compile", "eager");
        then.status(202).json_body(
            json!({"version": 1, "source_digest": "sha256:abc", "build_state": "building", "created_at": T0_MS}),
        );
    });
    let poll = version_states(&ctx, 1, vec!["building", "building", "ready"], None);
    let out = ctx
        .run(&[
            "push",
            dir.path().join("hello.ts").to_str().unwrap(),
            "--wait",
            "--wait-interval",
            "20ms",
        ])
        .ok();
    upload.assert();
    poll.assert_calls(3);
    let lines = out.lines();
    assert!(lines.contains(&"Building: ready".to_string()), "{lines:?}");
    assert_eq!(lines.last().unwrap(), "Invoke: http://invoke.example/s/hello/");
}

#[test]
fn wait_with_an_explicit_lazy_compile_is_a_usage_error() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    ctx.run(&[
        "push",
        dir.path().join("hello.ts").to_str().unwrap(),
        "--service",
        "xx",
        "--wait",
        "--compile",
        "lazy",
    ])
    .fails_with(2);
}

#[test]
fn wait_reports_a_failed_build_and_exits_1() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(202).json_body(
            json!({"version": 1, "source_digest": "sha256:abc", "build_state": "building", "created_at": T0_MS}),
        );
    });
    version_states(
        &ctx,
        1,
        vec!["building", "failed"],
        Some(json!({"reason": "type error in main.ts"})),
    );
    let out = ctx
        .run(&[
            "push",
            dir.path().join("hello.ts").to_str().unwrap(),
            "--wait",
            "--wait-interval",
            "20ms",
        ])
        .fails_with(1);
    assert!(
        out.lines().contains(&"Building: failed".to_string()),
        "{:?}",
        out.lines()
    );
    assert!(out.stderr.contains("type error in main.ts"), "{}", out.stderr);
    assert!(!out.lines().iter().any(|l| l.starts_with("Invoke:")));
}

#[test]
fn wait_times_out() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(202).json_body(
            json!({"version": 1, "source_digest": "sha256:abc", "build_state": "building", "created_at": T0_MS}),
        );
    });
    version_states(&ctx, 1, vec!["building"], None);
    let out = ctx
        .run(&[
            "push",
            dir.path().join("hello.ts").to_str().unwrap(),
            "--wait",
            "--wait-interval",
            "20ms",
            "--wait-timeout",
            "200ms",
        ])
        .fails_with(1);
    assert!(out.stderr.contains("still building after"), "{}", out.stderr);
}

#[test]
fn json_output_describes_the_whole_push() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    ctx.lookup_empty("hello");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services");
        then.status(201).json_body(service_json("svc_1", "hello"));
    });
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(201).json_body(upload_ok(1));
    });
    let out = ctx
        .run(&[
            "-o",
            "json",
            "push",
            dir.path().join("hello.ts").to_str().unwrap(),
            "--create",
        ])
        .ok();
    assert_eq!(
        out.json(),
        json!({
            "service": {"id": "svc_1", "slug": "hello", "runtime": "deno", "created": true},
            "version": upload_ok(1),
            "activated": true,
            "invoke_url": "http://invoke.example/s/hello/"
        })
    );
}

#[test]
fn upload_validation_error_shows_the_diagnostics() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", "export default (")]);
    existing(&ctx);
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(422).json_body(json!({"error": {
            "code": "validation_failed", "message": "the upload failed validation",
            "details": {"diagnostics": [{"file": "hello.ts", "line": 1, "col": 17, "message": "Expression expected"}]}
        }}));
    });
    let out = ctx
        .run(&["push", dir.path().join("hello.ts").to_str().unwrap()])
        .fails_with(1);
    assert!(out.stderr.contains("validation_failed"), "{}", out.stderr);
    assert!(out.stderr.contains("Expression expected"), "{}", out.stderr);
}

#[test]
fn pushing_twice_uploads_twice() {
    // every push creates a new version: nothing is cached between runs
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    let upload = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/versions");
        then.status(201).json_body(upload_ok(8));
    });
    ctx.run(&["push", dir.path().join("hello.ts").to_str().unwrap()]).ok();
    ctx.run(&["push", dir.path().join("hello.ts").to_str().unwrap()]).ok();
    upload.assert_calls(2);
}

/// `--close-sockets` asks the deploy to close the service's WebSockets with 1012.
#[test]
fn close_sockets_is_forwarded_with_activate() {
    let ctx = Ctx::new();
    let dir = tempfile::tempdir().unwrap();
    write_tree(dir.path(), &[("hello.ts", TS)]);
    existing(&ctx);
    let upload = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/versions")
            .query_param("activate", "1")
            .query_param("close_sockets", "1");
        then.status(201).json_body(upload_ok(5));
    });
    ctx.run(&["push", dir.path().join("hello.ts").to_str().unwrap(), "--close-sockets"])
        .ok();
    upload.assert();
    ctx.run(&[
        "push",
        dir.path().join("hello.ts").to_str().unwrap(),
        "--close-sockets",
        "--no-activate",
    ])
    .fails_with(2);
}
