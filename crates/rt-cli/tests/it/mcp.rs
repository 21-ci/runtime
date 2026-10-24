//! `21ci mcp`: the stdio bridge to the MCP endpoint of the control API (decision D-B5). One
//! JSON-RPC message per line in, one per line out; stdout carries nothing else.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn lines(out: &Out) -> Vec<Value> {
    out.stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("stdout line is not JSON ({e}): {l}")))
        .collect()
}

fn run(ctx: &Ctx, input: &[Value]) -> Out {
    let text: String = input.iter().map(|v| format!("{v}\n")).collect();
    ctx.run_stdin(&["mcp"], &text)
}

#[test]
fn a_request_is_posted_with_the_key_and_the_answer_is_printed_as_one_line() {
    let ctx = Ctx::new();
    let init = json!({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "c", "version": "1"}}});
    let answer = json!({"jsonrpc": "2.0", "id": 0, "result": {"protocolVersion": "2025-11-25", "capabilities": {"tools": {}}, "serverInfo": {"name": "21cloud-runtime", "version": "1"}}});
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/mcp")
            .header("authorization", auth())
            .header("content-type", "application/json")
            .header_exists("accept")
            .json_body(init.clone());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(answer.clone());
    });
    let out = run(&ctx, &[init]).ok();
    m.assert();
    assert_eq!(out.stdout.lines().count(), 1, "{}", out.stdout);
    assert_eq!(lines(&out), vec![answer]);
    assert!(!out.stdout.contains("\n  "), "compact, never pretty: {}", out.stdout);
}

#[test]
fn the_accept_header_names_both_json_and_event_streams() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp").is_true(|req| {
            req.headers_vec().iter().any(|(k, v)| {
                k.eq_ignore_ascii_case("accept") && v.contains("application/json") && v.contains("text/event-stream")
            })
        });
        then.status(200)
            .json_body(json!({"jsonrpc": "2.0", "id": 1, "result": {}}));
    });
    run(&ctx, &[json!({"jsonrpc": "2.0", "id": 1, "method": "ping"})]).ok();
    m.assert();
}

#[test]
fn a_notification_gets_202_and_prints_nothing() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/mcp")
            .json_body(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        then.status(202);
    });
    let out = run(
        &ctx,
        &[json!({"jsonrpc": "2.0", "method": "notifications/initialized"})],
    )
    .ok();
    m.assert();
    assert_eq!(out.stdout, "");
}

#[test]
fn the_negotiated_version_is_sent_on_every_later_request() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp").body_includes("\"initialize\"");
        then.status(200).json_body(
            json!({"jsonrpc": "2.0", "id": 0, "result": {"protocolVersion": "2025-06-18", "capabilities": {}}}),
        );
    });
    let listed = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/mcp")
            .header("mcp-protocol-version", "2025-06-18")
            .body_includes("tools/list");
        then.status(200)
            .json_body(json!({"jsonrpc": "2.0", "id": 1, "result": {"tools": []}}));
    });
    let out = run(
        &ctx,
        &[
            json!({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {"protocolVersion": "2025-11-25"}}),
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
        ],
    )
    .ok();
    listed.assert();
    assert_eq!(lines(&out).len(), 2);
}

#[test]
fn a_modern_request_gets_its_headers_mirrored_from_the_body() {
    let ctx = Ctx::new();
    let meta = json!({"io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {}});
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/mcp")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "tools/call")
            .header("mcp-name", "whoami");
        then.status(200)
            .json_body(json!({"jsonrpc": "2.0", "id": "a", "result": {"resultType": "complete", "content": []}}));
    });
    let out = run(&ctx, &[json!({"jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": {"name": "whoami", "arguments": {}, "_meta": meta}})]).ok();
    m.assert();
    assert_eq!(lines(&out)[0]["id"], json!("a"));
}

#[test]
fn an_event_stream_answer_is_printed_one_message_per_line_in_order() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(200).header("content-type", "text/event-stream").body(
            "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":1}}\n\n\
             event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n\n",
        );
    });
    let out = run(
        &ctx,
        &[json!({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": "x"}})],
    )
    .ok();
    let got = lines(&out);
    assert_eq!(got.len(), 2, "{}", out.stdout);
    assert_eq!(got[0]["method"], json!("notifications/progress"));
    assert_eq!(got[1]["id"], json!(7));
}

#[test]
fn a_rejected_request_becomes_a_json_rpc_error_with_the_same_id() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(401)
            .header("www-authenticate", "Bearer error=\"invalid_token\"")
            .json_body(err_json("unauthorized", "invalid credentials"));
    });
    let out = run(&ctx, &[json!({"jsonrpc": "2.0", "id": 5, "method": "tools/list"})]).ok();
    let got = lines(&out);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["id"], json!(5));
    assert_eq!(got[0]["error"]["code"], json!(-32000));
    let msg = got[0]["error"]["message"].as_str().unwrap_or_default();
    assert!(msg.contains("HTTP 401") && msg.contains("unauthorized"), "{msg}");
}

#[test]
fn a_json_rpc_error_body_of_the_server_is_passed_through() {
    let ctx = Ctx::new();
    let body = json!({"jsonrpc": "2.0", "id": 3, "error": {"code": -32022, "message": "Unsupported protocol version", "data": {"supported": ["2026-07-28"]}}});
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(400).json_body(body.clone());
    });
    let out = run(&ctx, &[json!({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})]).ok();
    assert_eq!(lines(&out), vec![body], "the server's own JSON-RPC error is the answer");
}

#[test]
fn a_failed_notification_is_reported_on_stderr_only() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(403).json_body(err_json("insufficient_scope", "nope"));
    });
    let out = run(
        &ctx,
        &[json!({"jsonrpc": "2.0", "method": "notifications/initialized"})],
    )
    .ok();
    assert_eq!(out.stdout, "");
    assert!(out.stderr.contains("HTTP 403"), "{}", out.stderr);
}

#[test]
fn an_unreachable_server_answers_every_request_with_an_error_and_the_bridge_keeps_reading() {
    let ctx = Ctx::new();
    let mut c = ctx.cmd();
    c.env("RT_API", "http://127.0.0.1:1");
    c.arg("mcp").write_stdin(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n",
    );
    let o = c.output().expect("spawn");
    assert_eq!(o.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&o.stdout).into_owned();
    let got: Vec<Value> = stdout.lines().map(|l| serde_json::from_str(l).expect("json")).collect();
    assert_eq!(
        got.iter().map(|g| g["id"].clone()).collect::<Vec<_>>(),
        vec![json!(1), json!(2)]
    );
    assert!(got.iter().all(|g| g["error"]["code"] == json!(-32000)));
    assert!(
        got[0]["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("cannot reach"),
        "{}",
        got[0]
    );
}

#[test]
fn garbage_lines_are_answered_with_a_parse_error_and_blank_lines_are_ignored() {
    let ctx = Ctx::new();
    let out = ctx
        .run_stdin(&["mcp"], "\n   \n{nope\n[1,2]\n{\"jsonrpc\":\"2.0\"}\n")
        .ok();
    let got = lines(&out);
    assert_eq!(got.len(), 3, "{}", out.stdout);
    assert_eq!(got[0]["error"]["code"], json!(-32700));
    assert!(got[0]["id"].is_null());
    assert_eq!(got[1]["error"]["code"], json!(-32600), "a batch: {}", got[1]);
    assert_eq!(got[2]["error"]["code"], json!(-32600), "no method: {}", got[2]);
}

#[test]
fn it_exits_cleanly_when_stdin_ends_and_keeps_stdout_for_messages_only() {
    let ctx = Ctx::new();
    let out = ctx.run_stdin(&["mcp"], "").ok();
    assert_eq!((out.stdout.as_str(), out.stderr.as_str()), ("", ""));
}

#[test]
fn without_a_key_it_refuses_to_start() {
    let ctx = Ctx::new();
    let mut c = ctx.cmd();
    c.env_remove("RT_KEY");
    let o = c.arg("mcp").write_stdin("").output().expect("spawn");
    assert_eq!(o.status.code(), Some(2));
    let err = String::from_utf8_lossy(&o.stderr).into_owned();
    assert!(err.contains("tenant key") && err.contains("RT_KEY"), "{err}");
    assert_eq!(String::from_utf8_lossy(&o.stdout), "", "nothing on stdout");
}

#[test]
fn the_url_is_the_contexts_api_with_mcp_appended() {
    let ctx = Ctx::new();
    let mut c = ctx.cmd();
    c.env("RT_API", format!("{}/", ctx.base()));
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(200)
            .json_body(json!({"jsonrpc": "2.0", "id": 1, "result": {}}));
    });
    let o = c
        .arg("mcp")
        .write_stdin("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n")
        .output()
        .expect("spawn");
    assert_eq!(o.status.code(), Some(0));
    m.assert();
}

#[test]
fn a_modern_request_that_is_not_a_tool_call_gets_no_tool_name_header() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/mcp")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "tools/list")
            .header_missing("mcp-name");
        then.status(200)
            .json_body(json!({"jsonrpc": "2.0", "id": 9, "result": {"tools": []}}));
    });
    let meta = json!({"io.modelcontextprotocol/protocolVersion": "2026-07-28"});
    let out = run(
        &ctx,
        &[json!({"jsonrpc": "2.0", "id": 9, "method": "tools/list", "params": {"_meta": meta}})],
    )
    .ok();
    m.assert();
    assert_eq!(lines(&out).len(), 1);
}

#[test]
fn a_non_json_event_in_an_answer_stream_is_skipped_with_a_note() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(200)
            .header("content-type", "text/event-stream")
            .body("data: not json\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\n");
    });
    let out = run(&ctx, &[json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})]).ok();
    assert_eq!(lines(&out), vec![json!({"jsonrpc": "2.0", "id": 1, "result": {}})]);
    assert!(out.stderr.contains("ignored a non-JSON event"), "{}", out.stderr);
}

#[test]
fn an_answer_stream_that_breaks_is_reported_to_its_request() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        // not UTF-8: the stream cannot be read line by line
        then.status(200)
            .header("content-type", "text/event-stream")
            .body(b"data: \xff\n\n");
    });
    let out = run(&ctx, &[json!({"jsonrpc": "2.0", "id": 4, "method": "tools/list"})]).ok();
    let got = lines(&out);
    assert_eq!(got.len(), 1, "{}", out.stdout);
    assert_eq!(got[0]["id"], json!(4));
    assert_eq!(got[0]["error"]["code"], json!(-32000));
    let msg = got[0]["error"]["message"].as_str().unwrap_or_default();
    assert!(msg.contains("event stream broke"), "{msg}");
}

#[test]
fn an_answer_that_cannot_be_read_is_an_error_for_its_request() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(200)
            .header("content-type", "application/json")
            .body(b"\xff\xfe");
    });
    let out = run(&ctx, &[json!({"jsonrpc": "2.0", "id": 6, "method": "tools/list"})]).ok();
    let got = lines(&out);
    assert_eq!(got.len(), 1, "{}", out.stdout);
    assert_eq!(got[0]["id"], json!(6));
    let msg = got[0]["error"]["message"].as_str().unwrap_or_default();
    assert!(msg.contains("cannot read the answer"), "{msg}");
}

#[test]
fn a_success_with_a_body_that_is_not_json_is_an_error_for_a_request_and_silent_for_a_notification() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(200).header("content-type", "text/plain").body("hello");
    });
    let out = run(&ctx, &[json!({"jsonrpc": "2.0", "id": 7, "method": "tools/list"})]).ok();
    let got = lines(&out);
    assert_eq!(got.len(), 1, "{}", out.stdout);
    assert_eq!(got[0]["id"], json!(7));
    let msg = got[0]["error"]["message"].as_str().unwrap_or_default();
    assert!(msg.contains("not JSON"), "{msg}");

    let out = run(
        &ctx,
        &[json!({"jsonrpc": "2.0", "method": "notifications/initialized"})],
    )
    .ok();
    assert_eq!(out.stdout, "", "a notification has no id to answer");
}

#[test]
fn a_failed_notification_with_a_json_rpc_error_body_is_reported_on_stderr() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/mcp");
        then.status(500).json_body(json!({
            "jsonrpc": "2.0",
            "error": {"code": -32603, "message": "the tool host is down"}
        }));
    });
    let out = run(
        &ctx,
        &[json!({"jsonrpc": "2.0", "method": "notifications/initialized"})],
    )
    .ok();
    assert_eq!(out.stdout, "");
    assert!(out.stderr.contains("HTTP 500: the tool host is down"), "{}", out.stderr);
}

#[test]
fn an_unreadable_stdin_stops_the_bridge_with_a_failure() {
    let ctx = Ctx::new();
    // a directory opens, but reading it fails: the bridge cannot take its input
    let out = ctx
        .raw_cmd()
        .arg("mcp")
        .stdin(std::fs::File::open(ctx.cfg.path()).unwrap())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("cannot read stdin"), "{err}");
}

#[test]
fn the_key_is_sent_as_a_header_never_on_the_command_line() {
    let ctx = Ctx::new();
    let out = ctx.run_stdin(&["mcp", "--help"], "").ok();
    assert!(
        !out.stdout.contains("--token") && !out.stdout.contains("--key"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains("RT_KEY"),
        "the help names the environment variable: {}",
        out.stdout
    );
}
