//! Typed service-to-service RPC over the in-process call path: `services.call`
//! resolves a method of the callee's `export const rpc` table, `services.bind` proxies it, the
//! callee's `ctx.caller` is stamped by the runtime, its errors arrive as a typed `ServiceError`
//! without a stack, and the call rules (access, depth, JSON cap) are the ones `services.fetch` has.
//! `services.fetch` and the legacy `call(slug, request)` keep working.

use serde_json::{json, Value};

use crate::common::*;

/// The callee: an `rpc` table with plain, failing, nested and recursive methods.
const RPC_CALLEE: &str = r#"
import { ServiceError, services } from "std:services";
export const rpc = {
  async add(args: any) { return { sum: args.a + args.b }; },
  async echo(args: any) { return args; },
  async whoCalled(_args: any, ctx: any) { return ctx.caller ?? null; },
  async fail() { throw new ServiceError("out_of_stock", "no stock left"); },
  async boom() { throw new Error("boom"); },
  async big() { return "x".repeat(300000); },
  async recurse(args: any, ctx: any) { return await services.call(ctx.service.slug, "recurse", { n: args.n + 1 }); },
  nested: { async ping() { return "pong"; } },
};
export default {
  async fetch(): Promise<Response> { return new Response("plain"); },
};"#;

/// The caller: every route answers `{ ok, value }` or the error's `name`, `code`, `message` and
/// whether it is a `ServiceError`.
const RPC_CALLER: &str = r#"
import { call, ServiceError, services } from "std:services";
import { kv } from "std:kv";
async function attempt(f: () => Promise<unknown>): Promise<Record<string, unknown>> {
  try {
    return { ok: true, value: await f() };
  } catch (e) {
    const a = e as any;
    return { ok: false, name: a?.name ?? null, code: a?.code ?? null, message: String(a?.message ?? a), serviceError: a instanceof ServiceError };
  }
}
export default {
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    const target = u.searchParams.get("target") ?? "";
    let out: Record<string, unknown>;
    switch (u.pathname) {
      case "/add": out = await attempt(() => services.call(target, "add", { a: 1, b: 2 })); break;
      case "/bind": out = await attempt(() => (services.bind(target) as any).add({ a: 2, b: 3 })); break;
      case "/nested": out = await attempt(() => services.call(target, "nested.ping")); break;
      case "/who": out = await attempt(() => services.call(target, "whoCalled")); break;
      case "/fail": out = await attempt(() => services.call(target, "fail")); break;
      case "/boom": out = await attempt(() => services.call(target, "boom")); break;
      case "/unknown": out = await attempt(() => services.call(target, "nope")); break;
      case "/inherited": out = await attempt(() => services.call(target, "constructor")); break;
      case "/proto": out = await attempt(() => services.call(target, "__proto__")); break;
      case "/big-args": out = await attempt(() => services.call(target, "echo", "x".repeat(300000))); break;
      case "/big-result": out = await attempt(() => services.call(target, "big")); break;
      case "/recurse": out = await attempt(() => services.call(target, "recurse", { n: 0 })); break;
      case "/fetch": out = await attempt(async () => {
        const r = await services.fetch(target, new Request("http://svc/"));
        return { status: r.status, body: await r.text() };
      }); break;
      case "/legacy": out = await attempt(async () => {
        const r = await call(target, new Request("http://svc/"));
        return { status: r.status, body: await r.text() };
      }); break;
      case "/reserved": out = await attempt(() => services.fetch(target, new Request("http://svc/__rt/rpc/add"))); break;
      case "/after-await": out = await attempt(async () => {
        // The caller yields (a timer, then a store read) before it calls: the callee must still run.
        await new Promise((r) => setTimeout(r, 5));
        await kv.get("nothing");
        return services.call(target, "add", { a: 1, b: 2 });
      }); break;
      default: return new Response("not found", { status: 404 });
    }
    return Response.json(out);
  },
};"#;

/// The caller's answer to one route, as `{ ok, value }` or the error fields.
async fn rpc_from(node: &impl Node, caller: &str, callee: &str, path: &str) -> Value {
    let r = invoke(node, caller, &format!("{path}?target={callee}")).await;
    assert_eq!(r.s(), 200, "{path}: {}", r.text);
    r.json
}

/// The callee and the caller, both in `production`, built and ready.
async fn two_services(node: &impl Node, key: &str) -> (String, String) {
    let callee = uniq("callee");
    let caller = uniq("caller");
    ship(node, key, &callee, RPC_CALLEE).await;
    ship(node, key, &caller, RPC_CALLER).await;
    assert_eq!(invoke(node, &callee, "/").await.s(), 200); // build
    assert_eq!(invoke(node, &caller, "/nothing").await.s(), 404); // build
    (callee, caller)
}

/// RPC-01: `services.call` resolves the callee's method (and a dotted nested one) and returns its
/// value; `services.bind` proxies the same methods as `await proxy.method(args)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_01_call_and_bind_return_the_callees_value() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;

    assert_eq!(
        rpc_from(&node, &caller, &callee, "/add").await,
        json!({"ok": true, "value": {"sum": 3}})
    );
    assert_eq!(
        rpc_from(&node, &caller, &callee, "/bind").await,
        json!({"ok": true, "value": {"sum": 5}})
    );
    assert_eq!(
        rpc_from(&node, &caller, &callee, "/nested").await,
        json!({"ok": true, "value": "pong"})
    );
    node.shutdown().await;
}

/// RPC-02: the callee's `ctx.caller` is what the runtime stamped: the calling service's id and
/// slug, its project (`prj_…`) and environment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_02_the_callee_sees_the_caller_in_ctx_caller() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;
    let caller_id = get(&node, &t.key, "/v1/services")
        .await
        .items()
        .iter()
        .find(|s| s["slug"] == json!(caller))
        .expect("caller listed")["id"]
        .clone();

    let r = rpc_from(&node, &caller, &callee, "/who").await;
    assert_eq!(r["ok"], json!(true), "{r}");
    let c = &r["value"];
    assert_eq!(c["service"]["id"], caller_id, "{c}");
    assert_eq!(c["service"]["slug"], json!(caller), "{c}");
    assert_eq!(c["environment"], json!("production"), "{c}");
    assert!(c["project"].as_str().is_some_and(|p| p.starts_with("prj_")), "{c}");
    node.shutdown().await;
}

/// RPC-03: a callee's `ServiceError` arrives as a `ServiceError` with its code and message; any
/// other error arrives as `internal_error` with only its message (no stack, no frames).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_03_callee_errors_arrive_typed_and_without_a_stack() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;

    let typed = rpc_from(&node, &caller, &callee, "/fail").await;
    assert_eq!(
        typed,
        json!({"ok": false, "name": "ServiceError", "code": "out_of_stock", "message": "no stock left", "serviceError": true}),
        "{typed}"
    );

    let plain = rpc_from(&node, &caller, &callee, "/boom").await;
    assert_eq!(plain["ok"], json!(false), "{plain}");
    assert_eq!(plain["code"], json!("internal_error"), "{plain}");
    assert_eq!(plain["message"], json!("boom"), "the message only: {plain}");
    assert!(
        !plain["message"].as_str().unwrap_or("").contains("\n"),
        "no stack frames: {plain}"
    );
    node.shutdown().await;
}

/// RPC-04: a method the callee does not export (or an inherited / reserved name) is
/// `method_not_found`; a method name that is not an identifier path is `validation_failed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_04_unknown_and_inherited_methods_are_method_not_found() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;

    for path in ["/unknown", "/inherited", "/proto"] {
        let r = rpc_from(&node, &caller, &callee, path).await;
        assert_eq!(r["code"], json!("method_not_found"), "{path}: {r}");
    }
    node.shutdown().await;
}

/// RPC-05: the access rules of `services.fetch` apply to RPC: a callee that lists other callers
/// refuses this one with `call_not_allowed`, and the callee's own fetch handler is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_05_the_callees_call_rules_apply_to_rpc() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;
    let callee_id = get(&node, &t.key, "/v1/services")
        .await
        .items()
        .iter()
        .find(|s| s["slug"] == json!(callee))
        .expect("callee listed")["id"]
        .as_str()
        .expect("id")
        .to_string();
    let r = patch(
        &node,
        &t.key,
        &format!("/v1/services/{callee_id}"),
        json!({"call": {"allowed_callers": [uniq("someone-else")]}}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);

    let refused = rpc_from(&node, &caller, &callee, "/add").await;
    assert_eq!(refused["code"], json!("call_not_allowed"), "{refused}");
    assert_eq!(refused["serviceError"], json!(true), "{refused}");
    node.shutdown().await;
}

/// RPC-06: the JSON cap applies both ways: arguments over 256 KiB are `validation_failed` before
/// any call, and a result over 256 KiB is `response_too_large`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_06_arguments_and_results_are_capped() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;

    let big_args = rpc_from(&node, &caller, &callee, "/big-args").await;
    assert_eq!(big_args["code"], json!("validation_failed"), "{big_args}");
    let big_result = rpc_from(&node, &caller, &callee, "/big-result").await;
    assert_eq!(big_result["code"], json!("response_too_large"), "{big_result}");
    node.shutdown().await;
}

/// RPC-07: a recursive RPC is cut at call depth 15 like any call: the typed error is `call_depth`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_07_the_call_depth_limit_applies_to_rpc() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;

    let r = rpc_from(&node, &caller, &callee, "/recurse").await;
    assert_eq!(r["code"], json!("call_depth"), "{r}");
    node.shutdown().await;
}

/// RPC-09: a caller that yields before it calls (a timer, a store read) reaches the callee promptly.
/// The callee's child task is placed on the caller's processor; a parked machine must still run it
/// (before the fix it waited for the 5 s queue timeout, every time).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_09_a_call_after_the_caller_yields_is_admitted_promptly() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;

    for round in 0..5 {
        let started = std::time::Instant::now();
        let r = rpc_from(&node, &caller, &callee, "/after-await").await;
        let took = started.elapsed();
        assert_eq!(r, json!({"ok": true, "value": {"sum": 3}}), "round {round}");
        assert!(
            took < std::time::Duration::from_secs(2),
            "round {round}: the callee waited {took:?} (queue timeout is 5 s)"
        );
    }
    node.shutdown().await;
}

/// RPC-08: `services.fetch` keeps its old meaning (and the legacy `call(slug, request)` import
/// still works), and the reserved `/__rt/` paths are not reachable through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_08_fetch_and_the_legacy_call_still_work_and_rt_paths_are_reserved() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (callee, caller) = two_services(&node, &t.key).await;

    assert_eq!(
        rpc_from(&node, &caller, &callee, "/fetch").await,
        json!({"ok": true, "value": {"status": 200, "body": "plain"}})
    );
    assert_eq!(
        rpc_from(&node, &caller, &callee, "/legacy").await,
        json!({"ok": true, "value": {"status": 200, "body": "plain"}})
    );
    let reserved = rpc_from(&node, &caller, &callee, "/reserved").await;
    assert_eq!(reserved["code"], json!("validation_failed"), "{reserved}");
    node.shutdown().await;
}

/// RPC-10: RPC crosses runtimes. A Deno caller's `services.call` reaches a WASM component's
/// `std:host/rpc` export (with the stamped caller), and a WASM caller's `call-rpc` reaches a Deno
/// callee's `rpc` table, typed errors included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_10_rpc_between_deno_and_wasm() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (deno_callee, deno_caller) = two_services(&node, &t.key).await;
    let wasm = uniq("wasm-rpc");
    ship_wasm(&node, &t.key, &wasm, "rpc").await;

    // Deno -> WASM: the export answers, its errors are typed, the caller is stamped.
    assert_eq!(
        rpc_from(&node, &deno_caller, &wasm, "/fail").await["code"],
        json!("out_of_stock")
    );
    let unknown = rpc_from(&node, &deno_caller, &wasm, "/add").await;
    assert_eq!(unknown["code"], json!("method_not_found"), "{unknown}");

    // WASM -> Deno: `call-rpc` returns the callee's JSON value or its error.
    let r = node_put(
        &node,
        &wasm,
        &format!("/rpc?slug={deno_callee}&method=add"),
        r#"{"a":20,"b":22}"#,
    )
    .await;
    assert!(r.contains("ok={\"sum\":42}"), "{r}");
    let r = node_put(&node, &wasm, &format!("/rpc?slug={deno_callee}&method=fail"), "null").await;
    assert!(
        r.contains("error=out_of_stock") && r.contains("message=no stock left"),
        "{r}"
    );
    let r = node_put(
        &node,
        &wasm,
        &format!("/rpc?slug={deno_callee}&method=whoCalled"),
        "null",
    )
    .await;
    assert!(
        r.contains(&format!("\"slug\":\"{wasm}\"")),
        "the Deno callee sees the WASM caller: {r}"
    );
    // WASM -> WASM, through the export.
    let r = node_put(
        &node,
        &wasm,
        &format!("/rpc?slug={wasm}&method=nested.add"),
        r#"{"a":1,"b":2}"#,
    )
    .await;
    assert!(r.contains("ok=3"), "{r}");
    let r = node_put(&node, &wasm, &format!("/rpc?slug={wasm}&method=whoami"), "null").await;
    assert!(
        r.contains(&format!("\"slug\":\"{wasm}\"")) && r.contains("\"tenant\":\"ten_"),
        "the export gets the stamped caller: {r}"
    );
    node.shutdown().await;
}

/// `PUT` with a body on the public listener; the response text.
async fn node_put(node: &impl Node, slug: &str, path: &str, body: &str) -> String {
    let r = invoke_with(node, slug, reqwest::Method::PUT, path, &[], body).await;
    assert_eq!(r.s(), 200, "{path}: {}", r.text);
    r.text
}

/// A service that calls its own `rpc` table from `fetch`.
const SELF_CALLER: &str = r#"
import { services } from "std:services";
export const rpc = { async add(args: any) { return { sum: args.a + args.b }; } };
export default {
  async fetch(_req: Request, ctx: any): Promise<Response> {
    return Response.json(await services.call(ctx.service.slug, "add", { a: 1, b: 2 }));
  },
};"#;

/// RPC-11: a service at `max_instances: 1` that calls itself does not deadlock: the child call is
/// re-entrant (its parent holds the only instance and waits for it), so it runs past the ceiling
/// instead of waiting to `queue_timeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_rpc_11_a_self_call_with_max_instances_1_does_not_deadlock() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("selfcall");
    let r = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": slug, "runtime": "deno", "scaling": {"max_instances": 1}}),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let svc = r.json["id"].as_str().expect("id").to_string();
    let u = upload_ts(&node, &t.key, &svc, SELF_CALLER, "activate=1").await;
    assert!(u.s() == 201 || u.s() == 202, "{}", u.text);

    let started = std::time::Instant::now();
    let r = tokio::time::timeout(std::time::Duration::from_secs(15), invoke(&node, &slug, "/"))
        .await
        .expect("the self-call answers instead of hanging");
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json, json!({"sum": 3}), "{}", r.text);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(7),
        "{:?}",
        started.elapsed()
    );
    node.shutdown().await;
}
