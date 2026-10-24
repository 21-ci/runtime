//! WSM-RPC: typed RPC from and to WASM components (`/wit/rt-host.wit`).
//!
//! - Caller: `std:host/call` `call-rpc(slug, method, args-json)` is `POST /__rt/rpc/<method>` on the
//!   call path (`rt_host::Calls`, mocked here) with the JSON argument; the callee's envelope is
//!   unwrapped into the result or its `{code, message}` error, a refused call into the host's code.
//! - Callee: a component exporting `std:host/rpc` (world `rpc-service`) gets a call to
//!   `/__rt/rpc/<method>` through `handle(method, args-json, caller)` instead of its HTTP handler,
//!   with the caller the runtime stamped; the host answers the envelope. A component without the
//!   export answers `method_not_found` without running guest code.

use crate::common;

use common::{prepared, Req, Rig};
use rt_types::ServiceId;
use serde_json::{json, Value};

fn envelope(reply: &common::Reply) -> Value {
    assert_eq!(
        reply.status,
        200,
        "an RPC answers 200 with an envelope: {}",
        reply.text()
    );
    serde_json::from_slice(&reply.body).expect("the RPC envelope is JSON")
}

#[tokio::test]
async fn ac_wsm_rpc_01_call_rpc_posts_the_method_and_unwraps_the_result() {
    let mut rig = Rig::new();
    let p = prepared("rpc");
    let reply = rig
        .run(&p, Req::put("/rpc?slug=callee&method=echo", br#"{"a":1}"#).service(7))
        .await
        .reply();
    assert_eq!(reply.kv("ok"), r#"{"a":1}"#);

    let seen = rig.calls.calls.lock().clone();
    assert_eq!(seen.len(), 1);
    let c = &seen[0];
    assert_eq!(
        c.from,
        ServiceId::from_u128(7),
        "the call carries the caller's identity"
    );
    assert_eq!(
        (c.slug.as_str(), c.method.as_str(), c.path.as_str()),
        ("callee", "POST", "/__rt/rpc/echo")
    );
    assert_eq!(c.body, br#"{"a":1}"#.to_vec());

    let nested = rig
        .run(&p, Req::put("/rpc?slug=callee&method=invoices.create", b"null"))
        .await
        .reply();
    assert_eq!(nested.kv("ok"), "null");
    assert_eq!(
        rig.calls.calls.lock()[1].path,
        "/__rt/rpc/invoices.create",
        "dotted names pass as they are"
    );
}

#[tokio::test]
async fn ac_wsm_rpc_02_errors_are_the_callees_code_or_the_hosts() {
    let mut rig = Rig::new();
    let p = prepared("rpc");
    let run = |slug: &str, method: &str, args: &[u8]| Req::put(&format!("/rpc?slug={slug}&method={method}"), args);

    let failed = rig.run(&p, run("callee", "fail", b"null")).await.reply();
    assert_eq!(
        (failed.kv("error").as_str(), failed.kv("message").as_str()),
        ("out_of_stock", "no stock left")
    );
    // A refusal of the call path keeps its code; a 5xx answer is its `x-rt-reason`.
    assert_eq!(
        rig.run(&p, run("private", "echo", b"1")).await.reply().kv("error"),
        "call_not_allowed"
    );
    assert_eq!(
        rig.run(&p, run("down", "echo", b"1")).await.reply().kv("error"),
        "guest_error"
    );
    assert_eq!(
        rig.run(&p, run("deep", "echo", b"1")).await.reply().kv("error"),
        "call_depth"
    );
    assert_eq!(
        rig.run(&p, run("garbage", "echo", b"1")).await.reply().kv("error"),
        "invalid_response"
    );

    let calls_before = rig.calls.calls.lock().len();
    // Checked before anything is sent.
    assert_eq!(
        rig.run(&p, run("callee", "not-a-name", b"1")).await.reply().kv("error"),
        "validation_failed"
    );
    assert_eq!(
        rig.run(&p, run("callee", "echo", b"{not json"))
            .await
            .reply()
            .kv("error"),
        "validation_failed"
    );
    let big = format!("\"{}\"", "x".repeat(300_000));
    assert_eq!(
        rig.run(&p, run("callee", "echo", big.as_bytes()))
            .await
            .reply()
            .kv("error"),
        "validation_failed"
    );
    assert_eq!(
        rig.calls.calls.lock().len(),
        calls_before,
        "refused arguments never reach the call path"
    );
}

#[tokio::test]
async fn ac_wsm_rpc_03_the_rpc_export_serves_a_call() {
    let mut rig = Rig::new();
    let p = prepared("rpc");
    let echo = rig.run(&p, Req::rpc("echo", br#"{"x":[1,2]}"#)).await.reply();
    assert_eq!(envelope(&echo), json!({"ok": true, "value": {"x": [1, 2]}}));
    let add = rig.run(&p, Req::rpc("nested.add", br#"{"a":2,"b":40}"#)).await.reply();
    assert_eq!(envelope(&add), json!({"ok": true, "value": 42}));
    let failed = rig.run(&p, Req::rpc("fail", b"null")).await.reply();
    assert_eq!(
        envelope(&failed),
        json!({"ok": false, "error": {"code": "out_of_stock", "message": "no stock left"}})
    );
    let unknown = rig.run(&p, Req::rpc("nope", b"null")).await.reply();
    assert_eq!(envelope(&unknown)["error"]["code"], "method_not_found");
    let big = rig.run(&p, Req::rpc("big", b"null")).await.reply();
    assert_eq!(envelope(&big)["error"]["code"], "response_too_large");
    // An empty body is `null`.
    let empty = rig.run(&p, Req::rpc("echo", b"")).await.reply();
    assert_eq!(envelope(&empty), json!({"ok": true, "value": null}));
}

#[tokio::test]
async fn ac_wsm_rpc_04_the_callee_reads_the_caller_the_runtime_stamped() {
    let mut rig = Rig::new();
    let p = prepared("rpc");
    let who = rig.run(&p, Req::rpc("whoami", b"null")).await.reply();
    let caller = &envelope(&who)["value"];
    let stamp = common::caller_stamp();
    assert_eq!(caller["service"], stamp.service.to_string());
    assert_eq!(caller["tenant"], stamp.tenant.to_string());
    assert_eq!(caller["slug"], "the-caller");
    assert_eq!(caller["environment"], "production");
    assert_eq!(
        caller["project"],
        stamp.project.map(|p| p.to_string()).unwrap_or_default()
    );
}

#[tokio::test]
async fn ac_wsm_rpc_05_a_component_without_the_export_answers_method_not_found() {
    let mut rig = Rig::new();
    let p = prepared("hello");
    let reply = rig.run(&p, Req::rpc("echo", b"1")).await.reply();
    assert_eq!(envelope(&reply)["error"]["code"], "method_not_found");
    // Its HTTP handler still serves plain calls and requests.
    assert_eq!(rig.run(&p, Req::get("/")).await.reply().status, 200);
}

#[tokio::test]
async fn ac_wsm_rpc_06_the_host_refuses_a_bad_method_or_argument_before_the_guest_runs() {
    let mut rig = Rig::new();
    let p = prepared("rpc");
    let code = |reply: &common::Reply| {
        envelope(reply)["error"]["code"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    };

    let bad_method = rig.run(&p, Req::rpc("not..a.path", b"null")).await.reply();
    assert_eq!(
        code(&bad_method),
        "method_not_found",
        "a method is a dot-separated path of identifiers"
    );

    let not_json = rig.run(&p, Req::rpc("echo", b"{oops")).await.reply();
    assert_eq!(code(&not_json), "validation_failed");
    assert_eq!(envelope(&not_json)["error"]["message"], "the argument is not JSON");

    let not_utf8 = rig.run(&p, Req::rpc("echo", &[0xff, 0xfe])).await.reply();
    assert_eq!(code(&not_utf8), "validation_failed");

    let huge = vec![b' '; 256 * 1024 + 1];
    let too_big = rig.run(&p, Req::rpc("echo", &huge)).await.reply();
    assert_eq!(code(&too_big), "validation_failed");
    assert_eq!(
        envelope(&too_big)["error"]["message"],
        "the argument is larger than 256 KiB"
    );
}
