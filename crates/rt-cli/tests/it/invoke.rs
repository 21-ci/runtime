//! `21ci invoke`: the public listener, status + x-rt-* headers, body handling.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

fn ctx_with_invoke() -> Ctx {
    let mut ctx = Ctx::new();
    // the mock server plays both listeners; paths never overlap (/v1 vs /s)
    ctx.invoke_base = ctx.base();
    ctx
}

#[test]
fn get_root_shows_status_headers_and_pretty_body() {
    let ctx = ctx_with_invoke();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/s/hello/").header_missing("authorization");
        then.status(200)
            .header("content-type", "application/json")
            .header("x-rt-task-id", "task_1")
            .header("x-rt-version", "3")
            .header("server-timing", "total;dur=1.5")
            .header("x-unrelated", "nope")
            .body(r#"{"ok":true,"n":[1,2]}"#);
    });
    let out = ctx.run(&["invoke", "hello"]).ok();
    m.assert(); // the control token is never sent to the invoke listener
    let lines = out.stdout.lines().map(str::to_string).collect::<Vec<_>>();
    assert_eq!(lines[0], format!("-> GET {}/s/hello/", ctx.base()));
    assert_eq!(lines[1], "<- HTTP 200");
    let headers: Vec<_> = lines[2..]
        .iter()
        .take_while(|l| l.starts_with("   "))
        .map(|l| l.trim().to_string())
        .collect();
    assert_eq!(
        headers,
        vec![
            "content-type: application/json",
            "server-timing: total;dur=1.5",
            "x-rt-task-id: task_1",
            "x-rt-version: 3"
        ]
    );
    assert!(!out.stdout.contains("x-unrelated"));
    let body_start = lines.iter().position(|l| l == "{").expect("pretty JSON body");
    let body = lines[body_start..].join("\n");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        json!({"ok": true, "n": [1, 2]})
    );
    assert!(lines[body_start - 1].is_empty(), "blank line between headers and body");
}

#[test]
fn path_method_data_and_headers() {
    let ctx = ctx_with_invoke();
    let m = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/s/hello/x/y")
            .query_param("name", "Ada")
            .header("content-type", "application/json")
            .header("x-custom", "1")
            .body(r#"{"a":1}"#);
        then.status(201).header("content-type", "text/plain").body("created");
    });
    let out = ctx
        .run(&[
            "invoke",
            "hello",
            "/x/y?name=Ada",
            "-X",
            "put",
            "-d",
            r#"{"a":1}"#,
            "-H",
            "x-custom: 1",
        ])
        .ok();
    m.assert();
    assert!(out.stdout.contains("<- HTTP 201"), "{}", out.stdout);
    assert!(out.stdout.trim_end().ends_with("created"), "{}", out.stdout);
}

#[test]
fn data_implies_post_and_path_gets_a_leading_slash() {
    let ctx = ctx_with_invoke();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/s/hello/echo")
            .header("content-type", "text/plain")
            .body("hi there");
        then.status(200).body("ok");
    });
    ctx.run(&["invoke", "hello", "echo", "-d", "hi there"]).ok();
    m.assert();
}

#[test]
fn query_only_path() {
    let ctx = ctx_with_invoke();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/s/hello/").query_param("name", "Ada");
        then.status(200).body("ok");
    });
    ctx.run(&["invoke", "hello", "?name=Ada"]).ok();
    m.assert();
}

#[test]
fn data_from_file_is_sent_as_is_and_content_type_can_be_overridden() {
    let ctx = ctx_with_invoke();
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("payload.bin");
    std::fs::write(&f, b"\x00\x01binary\xff").unwrap();
    let default_ct = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/s/hello/up")
            .header("content-type", "application/octet-stream")
            .is_true(|r| r.body_ref() == b"\x00\x01binary\xff");
        then.status(200).body("ok");
    });
    ctx.run(&["invoke", "hello", "/up", "-d", &format!("@{}", f.display())])
        .ok();
    default_ct.assert();

    let csv = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/s/hello/csv")
            .header("content-type", "text/csv");
        then.status(200).body("ok");
    });
    ctx.run(&["invoke", "hello", "/csv", "-d", "a,b", "-H", "Content-Type: text/csv"])
        .ok();
    csv.assert();
}

#[test]
fn invoke_key_flag_and_env_send_a_bearer_token() {
    let ctx = ctx_with_invoke();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/s/private/")
            .header("authorization", "Bearer rtk_invoke_key");
        then.status(200).body("ok");
    });
    ctx.run(&["invoke", "private", "--key", "rtk_invoke_key"]).ok();
    ctx.cmd()
        .env("RT_INVOKE_KEY", "rtk_invoke_key")
        .args(["invoke", "private"])
        .assert()
        .success();
    m.assert_calls(2);
}

#[test]
fn guest_error_is_shown_with_its_reason_and_exit_0_unless_fail() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/broken/");
        then.status(500)
            .header("content-type", "text/plain")
            .header("x-rt-reason", "guest_error")
            .body("TypeError: x is not a function");
    });
    let out = ctx.run(&["invoke", "broken"]).ok();
    assert!(out.stdout.contains("<- HTTP 500"), "{}", out.stdout);
    assert!(out.stdout.contains("x-rt-reason: guest_error"), "{}", out.stdout);
    assert!(out.stdout.contains("TypeError: x is not a function"), "{}", out.stdout);
    ctx.run(&["invoke", "broken", "--fail"]).fails_with(1);
}

#[test]
fn body_only_prints_just_the_body() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/hello/");
        then.status(200)
            .header("content-type", "text/plain")
            .header("x-rt-task-id", "task_1")
            .body("plain body");
    });
    let out = ctx.run(&["invoke", "hello", "--body-only"]).ok();
    assert_eq!(out.stdout, "plain body");
}

#[test]
fn json_mode_is_one_document_with_status_headers_and_body() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/hello/");
        then.status(200)
            .header("content-type", "application/json")
            .header("x-rt-task-id", "task_1")
            .body(r#"{"ok":true}"#);
    });
    let v = ctx.run(&["-o", "json", "invoke", "hello"]).ok().json();
    assert_eq!(v["status"], 200);
    assert_eq!(v["headers"]["x-rt-task-id"], "task_1");
    assert_eq!(v["body"], json!({"ok": true}));

    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/text/");
        then.status(200).header("content-type", "text/plain").body("hello");
    });
    let v = ctx.run(&["-o", "json", "invoke", "text"]).ok().json();
    assert_eq!(v["body"], "hello");
}

#[test]
fn binary_bodies_are_summarised() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/img/");
        then.status(200)
            .header("content-type", "image/png")
            .body(b"\x89PNG\r\n\x1a\n");
    });
    let out = ctx.run(&["invoke", "img"]).ok();
    assert!(out.stdout.contains("(binary body, 8 bytes"), "{}", out.stdout);
}

#[test]
fn service_id_is_resolved_to_its_slug_first() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1")
            .header("authorization", format!("Bearer {TOKEN}"));
        then.status(200).json_body(service_json("svc_1", "hello"));
    });
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/s/hello/");
        then.status(200).body("ok");
    });
    ctx.run(&["invoke", "svc_1"]).ok();
    m.assert();
}

#[test]
fn unreachable_invoke_listener_exits_3() {
    let ctx = Ctx::new();
    let out = ctx
        .cmd()
        .env("RT_INVOKE", "http://127.0.0.1:1")
        .args(["invoke", "hello"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(3));
}

#[test]
fn a_full_url_is_the_base_and_the_path_is_appended() {
    let ctx = ctx_with_invoke();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/s/direct/");
        then.status(200).body("direct");
    });
    let out = ctx
        .run(&["invoke", &format!("{}/s/direct/", ctx.base()), "--body-only"])
        .ok();
    m.assert();
    assert_eq!(out.stdout, "direct");
}

#[test]
fn an_empty_body_prints_only_the_status_line_and_headers() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/empty/");
        then.status(204);
    });
    let out = ctx.run(&["invoke", "empty"]).ok();
    assert_eq!(out.lines().len(), 2, "{}", out.stdout);
    assert_eq!(out.lines()[1], "<- HTTP 204");
}

#[test]
fn json_mode_summarises_a_body_that_is_not_utf8() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/bin/");
        then.status(200)
            .header("content-type", "application/octet-stream")
            .body(b"\xff\xfe\x00");
    });
    let v = ctx.run(&["-o", "json", "invoke", "bin"]).ok().json();
    assert_eq!(v["body"], json!(null));
    assert_eq!(v["binary"], json!(true));
    assert_eq!(v["size"], json!(3));
}

#[test]
fn body_only_ends_quietly_when_the_reader_closes_stdout() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/big/");
        then.status(200)
            .header("content-type", "text/plain")
            .body("x".repeat(1 << 20));
    });
    let mut child = ctx
        .raw_cmd()
        .args(["invoke", "big", "--body-only"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn 21ci");
    // read one byte and close the pipe: the rest of the 1 MiB body meets a broken pipe
    let mut stdout = child.stdout.take().unwrap();
    let mut first = [0u8; 1];
    std::io::Read::read_exact(&mut stdout, &mut first).unwrap();
    drop(stdout);
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(0));
}

// ---- --async -----------------------------------------------------------------------------------

/// `--async` sends `Prefer: respond-async` (and the idempotency key) and prints the id of the
/// accepted invocation, exit 0.
#[test]
fn invoke_async_sends_prefer_and_prints_the_id() {
    let ctx = ctx_with_invoke();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/s/hook/run")
            .header("prefer", "respond-async")
            .header("idempotency-key", "k1")
            .body("hi");
        then.status(202)
            .header("content-type", "application/json")
            .header("x-rt-invocation-id", "inv_1")
            .header("preference-applied", "respond-async")
            .body(r#"{"id":"inv_1","state":"pending"}"#);
    });
    let out = ctx
        .run(&[
            "invoke",
            "hook",
            "/run",
            "-d",
            "hi",
            "--async",
            "--idempotency-key",
            "k1",
        ])
        .ok();
    m.assert();
    assert_eq!(out.stdout, "inv_1\n");
    assert!(!out.stderr.contains("replay"), "{}", out.stderr);

    let out = ctx
        .run(&[
            "invoke",
            "hook",
            "/run",
            "-d",
            "hi",
            "--async",
            "--idempotency-key",
            "k1",
            "--json",
        ])
        .ok();
    assert_eq!(out.json(), json!({"id": "inv_1", "state": "pending", "replay": false}));
}

/// A replay (same key, same request) prints the original id and says so; the id may come from
/// the header alone.
#[test]
fn invoke_async_replay_and_the_id_header() {
    let ctx = ctx_with_invoke();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/s/hook/")
            .header("prefer", "respond-async")
            .header_missing("idempotency-key");
        then.status(202)
            .header("x-rt-invocation-id", "inv_7")
            .header("x-rt-idempotent-replay", "true")
            .body("");
    });
    let out = ctx.run(&["invoke", "hook", "--async"]).ok();
    m.assert();
    assert_eq!(out.stdout, "inv_7\n");
    assert!(out.stderr.contains("idempotent replay"), "{}", out.stderr);
}

/// A key mismatch is `409 idempotency_mismatch`: the code is printed, exit 1.
#[test]
fn invoke_async_409_prints_idempotency_mismatch() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/s/hook/");
        then.status(409)
            .header("content-type", "application/json")
            .body(r#"{"error":{"code":"idempotency_mismatch","message":"the key was used for another request"}}"#);
    });
    let out = ctx
        .run(&["invoke", "hook", "-d", "x", "--async", "--idempotency-key", "k1"])
        .fails_with(1);
    assert!(out.stderr.contains("idempotency_mismatch"), "{}", out.stderr);
    assert!(out.stdout.is_empty(), "{}", out.stdout);
}

/// The other refusals print their code: 403 / 413 exit 1, 429 with Retry-After exits 4 (busy);
/// a body without the envelope falls back to `x-rt-reason`.
#[test]
fn invoke_async_refusals_print_the_code() {
    for (status, code, retry, exit) in [
        (403, "async_not_allowed", false, 1),
        (413, "too_large", false, 1),
        (429, "async_backlog_full", true, 4),
    ] {
        let ctx = ctx_with_invoke();
        ctx.server.mock(|when, then| {
            when.method(GET).path("/s/hook/");
            let t = then.status(status).header("x-rt-reason", code).body("refused");
            if retry {
                t.header("retry-after", "1");
            }
        });
        let out = ctx.run(&["invoke", "hook", "--async"]).fails_with(exit);
        assert!(out.stderr.contains(code), "{status}: {}", out.stderr);
    }
}

/// A preference the service ignored (async disabled) runs synchronously: printed as usual.
#[test]
fn invoke_async_ignored_prints_the_sync_answer() {
    let ctx = ctx_with_invoke();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/s/hook/");
        then.status(200).header("content-type", "text/plain").body("done");
    });
    let out = ctx.run(&["invoke", "hook", "--async"]).ok();
    assert!(out.stdout.contains("<- HTTP 200"), "{}", out.stdout);
    assert!(out.stdout.contains("done"), "{}", out.stdout);
    // --idempotency-key needs --async
    ctx.run(&["invoke", "hook", "--idempotency-key", "k"]).fails_with(2);
}
