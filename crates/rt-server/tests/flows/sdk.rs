//! The operator SDK as a runtime service (plan 2026-10-22 SDK-01..21 within
//! the owner's scope): a tenant service imports the SDK's contract (`std:sdk`, `sdk:<name>`), each
//! call is bound at run time to the release its grants select, and an SDK update, rollback or
//! removal reaches it without a rebuild or a redeploy.

use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{json, Value};

use crate::common::*;

/// A tenant service using the SDK `acme` through all three import forms; every route answers
/// `{ ok, value }` or the error's `name`, `code`, `message` and whether it is an `SdkError`.
const CONSUMER: &str = r#"
import { acme, sdk, SdkError } from "std:sdk";
import acmeDefault, { ping } from "sdk:acme";
import { services } from "std:services";
async function attempt(f: () => Promise<unknown>): Promise<Record<string, unknown>> {
  try {
    return { ok: true, value: await f() };
  } catch (e) {
    const a = e as any;
    return { ok: false, name: a?.name ?? null, code: a?.code ?? null, message: String(a?.message ?? a), sdkError: a instanceof SdkError };
  }
}
export const rpc = {
  async recurse(args: any, ctx: any) {
    if (args.n >= 15) return await acme.ping();
    return await services.call(ctx.service.slug, "recurse", { n: args.n + 1 });
  },
};
export default {
  async fetch(req: Request, ctx: any): Promise<Response> {
    const u = new URL(req.url);
    let out: Record<string, unknown>;
    switch (u.pathname) {
      case "/ping": out = await attempt(() => acme.ping()); break;
      case "/default": out = await attempt(() => acmeDefault.ping()); break;
      case "/named": out = await attempt(() => ping()); break;
      case "/echo": out = await attempt(() => acme.echo({ n: 1, s: "x" })); break;
      case "/fail": out = await attempt(() => acme.fail()); break;
      case "/slow": out = await attempt(() => acme.slow({ ms: Number(u.searchParams.get("ms") ?? "100") })); break;
      case "/burn": out = await attempt(() => acme.burn({ n: 20000000 })); break;
      case "/unknown": out = await attempt(() => (acme as any).nope()); break;
      case "/other": out = await attempt(() => (sdk("other") as any).ping()); break;
      case "/recurse": out = await attempt(() => services.call(ctx.service.slug, "recurse", { n: 1 })); break;
      case "/burst": {
        const n = Number(u.searchParams.get("n") ?? "1");
        const ms = Number(u.searchParams.get("ms") ?? "0");
        const all = await Promise.all(Array.from({ length: n }, () => attempt(() => ms > 0 ? acme.slow({ ms }) : acme.ping())));
        const codes: Record<string, number> = {};
        for (const r of all) { const k = r.ok ? "ok" : String(r.code); codes[k] = (codes[k] ?? 0) + 1; }
        out = { ok: true, value: codes };
        break;
      }
      default: return new Response("not found", { status: 404 });
    }
    return Response.json(out);
  },
};"#;

/// The consumer's answer to `path`.
async fn from(node: &impl Node, slug: &str, path: &str) -> Value {
    let r = invoke(node, slug, path).await;
    assert_eq!(r.s(), 200, "{path}: {}", r.text);
    r.json
}

/// The SDK `acme` 1.0.0 (stable, answers `v1`), allowed for everyone, and a consumer service.
async fn setup(node: &impl Node) -> (Tenant, String, String) {
    assert_eq!(
        publish_sdk(node, "acme", "1.0.0", &sdk_source("v1"), None).await.s(),
        201
    );
    assert_eq!(
        put_grant(node, json!({"scope": "node", "sdk": "acme", "effect": "allow"}))
            .await
            .s(),
        200
    );
    let t = new_tenant(node, "pro").await;
    let slug = uniq("consumer");
    let id = ship(node, &t.key, &slug, CONSUMER).await;
    (t, slug, id)
}

async fn set_channel(node: &impl Node, channel: &str, version: &str) {
    let r = call(
        root(node, Method::PUT, &format!("/v1/sdks/acme/channels/{channel}")),
        Some(json!({"version": version})),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
}

/// The consumer's versions and live digest: what a redeploy would change.
async fn deployed(node: &impl Node, key: &str, id: &str) -> Value {
    let v = get(node, key, &format!("/v1/services/{id}/versions")).await;
    assert_eq!(v.s(), 200, "{}", v.text);
    json!(v
        .json
        .as_array()
        .expect("versions")
        .iter()
        .map(|x| json!([x["version"], x["source_digest"]]))
        .collect::<Vec<_>>())
}

/// SDK-01, SDK-02, SDK-03, SDK-18: the consumer calls the stable release through `std:sdk` and
/// `sdk:acme`; a new release on `stable` answers the next call and a rollback the one after, while
/// the consumer keeps its single version (no rebuild, no redeploy).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_01_02_03_update_and_rollback_without_redeploy() {
    let node = start_node().await;
    let (t, slug, id) = setup(&node).await;

    for path in ["/ping", "/default", "/named"] {
        let r = from(&node, &slug, path).await;
        assert_eq!(
            (r["value"]["pong"].clone(), r["value"]["version"].clone()),
            (json!("v1"), json!("1.0.0")),
            "{path}: {r}"
        );
    }
    let before = deployed(&node, &t.key, &id).await;

    assert_eq!(
        publish_sdk(&node, "acme", "1.1.0", &sdk_source("v2"), Some("stable"))
            .await
            .s(),
        201
    );
    let r = from(&node, &slug, "/ping").await;
    assert_eq!(
        (r["value"]["pong"].clone(), r["value"]["version"].clone()),
        (json!("v2"), json!("1.1.0")),
        "{r}"
    );
    assert_eq!(
        deployed(&node, &t.key, &id).await,
        before,
        "the consumer was not rebuilt or redeployed"
    );

    set_channel(&node, "stable", "1.0.0").await;
    assert_eq!(
        from(&node, &slug, "/ping").await["value"]["pong"],
        json!("v1"),
        "rolled back"
    );
    assert_eq!(deployed(&node, &t.key, &id).await, before);
    node.shutdown().await;
}

/// SDK-04: an environment grant with `channel = canary` binds that environment's services to the
/// canary release; production stays on stable. A major release on stable is not served to grants
/// pinned to the old major (the grant written for 1.x is `^1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_04_channels_per_environment_and_major_versions() {
    let node = start_node().await;
    let (t, prod, _) = setup(&node).await;
    assert_eq!(
        publish_sdk(&node, "acme", "1.1.0", &sdk_source("canary"), Some("canary"))
            .await
            .s(),
        201
    );
    let project = get(&node, &t.key, "/v1/projects/default").await.json["id"]
        .as_str()
        .expect("project id")
        .to_string();
    let r = patch(
        &node,
        &t.key,
        "/v1/projects/default",
        json!({"environments": ["production", "staging"]}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let staging = uniq("consumer-staging");
    let id = ship(&node, &t.key, &staging, CONSUMER).await;
    assert_eq!(
        patch(
            &node,
            &t.key,
            &format!("/v1/services/{id}"),
            json!({"environment": "staging"})
        )
        .await
        .s(),
        200
    );
    let g = put_grant(
        &node,
        json!({"scope": format!("environment:{project}/staging"), "sdk": "acme", "channel": "canary"}),
    )
    .await;
    assert_eq!(g.s(), 200, "{}", g.text);

    assert_eq!(from(&node, &staging, "/ping").await["value"]["version"], json!("1.1.0"));
    assert_eq!(from(&node, &prod, "/ping").await["value"]["version"], json!("1.0.0"));

    // 2.0.0 on stable: the node grant is pinned to ^1, so production keeps the newest 1.x.
    assert_eq!(
        publish_sdk(&node, "acme", "2.0.0", &sdk_source("v2-major"), Some("stable"))
            .await
            .s(),
        201
    );
    assert_eq!(from(&node, &prod, "/ping").await["value"]["version"], json!("1.1.0"));
    // Moving tenants to the new major is a grant change.
    assert_eq!(
        put_grant(
            &node,
            json!({"scope": "node", "sdk": "acme", "effect": "allow", "version_req": "^2"})
        )
        .await
        .s(),
        200
    );
    assert_eq!(from(&node, &prod, "/ping").await["value"]["version"], json!("2.0.0"));
    node.shutdown().await;
}

/// SDK-05, SDK-08: a deny, a removed SDK and an unknown one are typed `SdkError`s the consumer can
/// handle (`sdk_not_granted`, `sdk_unavailable`, `sdk_not_found`); the consumer keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_05_08_refusals_are_typed_errors() {
    let node = start_node().await;
    let (t, slug, _) = setup(&node).await;
    let other = from(&node, &slug, "/other").await;
    assert_eq!(
        (other["code"].clone(), other["sdkError"].clone()),
        (json!("sdk_not_found"), json!(true)),
        "{other}"
    );

    put_grant(
        &node,
        json!({"scope": format!("tenant:{}", t.id), "sdk": "acme", "effect": "deny"}),
    )
    .await;
    let denied = from(&node, &slug, "/ping").await;
    assert_eq!(
        (denied["code"].clone(), denied["name"].clone()),
        (json!("sdk_not_granted"), json!("SdkError")),
        "{denied}"
    );
    put_grant(&node, json!({"scope": format!("tenant:{}", t.id), "sdk": "acme"})).await;
    assert_eq!(from(&node, &slug, "/ping").await["ok"], json!(true));

    assert_eq!(call(root(&node, Method::DELETE, "/v1/sdks/acme"), None).await.s(), 204);
    let removed = from(&node, &slug, "/ping").await;
    assert_eq!(
        (removed["code"].clone(), removed["sdkError"].clone()),
        (json!("sdk_unavailable"), json!(true)),
        "{removed}"
    );
    node.shutdown().await;
}

/// SDK-09: the SDK reads the caller the runtime stamped (`ctx.caller`): tenant, plan, project,
/// environment and service, plus the release; nothing in the request can change it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_09_the_sdk_sees_the_stamped_caller() {
    let node = start_node().await;
    let (t, slug, id) = setup(&node).await;
    let r = invoke_with(
        &node,
        &slug,
        Method::GET,
        "/ping",
        &[("x-rt-caller-tenant", "ten_forged"), ("x-rt-tenant", "ten_forged")],
        "",
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let c = &r.json["value"]["caller"];
    assert_eq!(c["tenant"], json!(t.id), "{c}");
    assert_eq!(c["plan"], json!("pro"), "{c}");
    assert_eq!(
        (c["service"]["id"].clone(), c["service"]["slug"].clone()),
        (json!(id), json!(slug)),
        "{c}"
    );
    assert_eq!(c["environment"], json!("production"), "{c}");
    assert!(c["project"].as_str().is_some_and(|p| p.starts_with("prj_")), "{c}");
    assert_eq!(c["sdk"], json!({"name": "acme", "version": "1.0.0"}), "{c}");
    assert!(!r.text.contains("ten_forged"), "{}", r.text);
    node.shutdown().await;
}

/// SDK-11: an SDK error reaches the caller as its code and message (no stack); an unknown method is
/// `method_not_found`; arguments and results are JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_11_sdk_errors_reach_the_caller_typed() {
    let node = start_node().await;
    let (_, slug, _) = setup(&node).await;
    let failed = from(&node, &slug, "/fail").await;
    assert_eq!(
        (failed["code"].clone(), failed["message"].clone()),
        (json!("out_of_stock"), json!("no stock left")),
        "{failed}"
    );
    assert_eq!(from(&node, &slug, "/unknown").await["code"], json!("method_not_found"));
    assert_eq!(from(&node, &slug, "/echo").await["value"], json!({"n": 1, "s": "x"}));
    node.shutdown().await;
}

/// SDK-13: the SDK runs within the caller's remaining time: a caller with a 1 s wall limit is
/// answered at its deadline, not when the SDK would finish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_13_the_sdk_runs_within_the_callers_deadline() {
    let node = start_node().await;
    let (t, slug, id) = setup(&node).await;
    assert_eq!(
        patch(
            &node,
            &t.key,
            &format!("/v1/services/{id}"),
            json!({"limits": {"wall_ms": 1000}})
        )
        .await
        .s(),
        200
    );
    assert_eq!(from(&node, &slug, "/slow?ms=10").await["value"], json!("late"));
    let started = Instant::now();
    let r = invoke(&node, &slug, "/slow?ms=8000").await;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(4), "answered after {took:?}");
    assert!(
        r.s() == 504 || r.json["code"] == json!("timeout"),
        "{} {}",
        r.s(),
        r.text
    );
    node.shutdown().await;
}

/// SDK-15: the SDK's CPU is recorded in the caller tenant's usage, against the SDK's service.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_15_sdk_cpu_is_the_callers_usage() {
    let node = start_node().await;
    let (t, slug, _) = setup(&node).await;
    let sdk_service = call(root(&node, Method::GET, "/v1/sdks"), None).await.json[0]["service_id"].clone();
    assert_eq!(from(&node, &slug, "/burn").await["value"], json!(true));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let u = call(root(&node, Method::GET, &format!("/v1/tenants/{}/usage", t.id)), None).await;
        let row = u
            .json
            .as_array()
            .and_then(|rows| rows.iter().find(|r| r["service_id"] == sdk_service).cloned());
        if let Some(row) = row {
            assert!(row["cpu_ms"].as_u64().is_some_and(|c| c > 0), "{row}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no usage row for the SDK service: {}",
            u.text
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    node.shutdown().await;
}

/// SDK-17: a WASM component calls the SDK through `std:host/sdk` `invoke` with the same results and
/// refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_17_wasm_guests_call_sdks() {
    let node = start_node().await;
    let (t, _, _) = setup(&node).await;
    let wasm = uniq("wasm-sdk");
    ship_wasm(&node, &t.key, &wasm, "rpc").await;
    let r = invoke_with(&node, &wasm, Method::PUT, "/sdk?name=acme&method=ping", &[], "null").await;
    assert!(
        r.text.contains("ok={") && r.text.contains("\"pong\":\"v1\""),
        "{}",
        r.text
    );
    let r = invoke_with(&node, &wasm, Method::PUT, "/sdk?name=acme&method=fail", &[], "null").await;
    assert!(r.text.contains("error=out_of_stock"), "{}", r.text);
    let r = invoke_with(&node, &wasm, Method::PUT, "/sdk?name=nope&method=ping", &[], "null").await;
    assert!(r.text.contains("error=sdk_not_found"), "{}", r.text);
    put_grant(
        &node,
        json!({"scope": format!("tenant:{}", t.id), "sdk": "acme", "effect": "deny"}),
    )
    .await;
    let r = invoke_with(&node, &wasm, Method::PUT, "/sdk?name=acme&method=ping", &[], "null").await;
    assert!(r.text.contains("error=sdk_not_granted"), "{}", r.text);
    node.shutdown().await;
}

/// SDK-21: an SDK call is a call hop: past depth 15 it is `call_depth`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_21_an_sdk_call_counts_as_a_call_hop() {
    let node = start_node().await;
    let (_, slug, _) = setup(&node).await;
    let r = from(&node, &slug, "/recurse").await;
    assert_eq!(r["code"], json!("call_depth"), "{r}");
    node.shutdown().await;
}

/// SDK-14 (known gap of the SDK step): SDK tasks run in the `sys` tenant, so one tenant's calls must
/// not use up the SDK for everyone. A caller tenant has at most its plan's `max_concurrency` SDK
/// calls in flight and `rps` SDK calls per second; beyond that a call is `sdk_rate_limited`, and
/// another tenant is unaffected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_14_sdk_calls_are_capped_per_caller_tenant() {
    let node = start_node_with(|c| {
        c.plans.insert(
            "narrow".into(),
            rt_types::Plan {
                max_concurrency: 4,
                ..pro_plan()
            },
        );
        c.plans
            .insert("slow-rate".into(), rt_types::Plan { rps: 5, ..pro_plan() });
    })
    .await;
    let (_, pro_slug, _) = setup(&node).await;
    let narrow = new_tenant(&node, "narrow").await;
    let narrow_slug = uniq("narrow");
    ship(&node, &narrow.key, &narrow_slug, CONSUMER).await;
    let rate = new_tenant(&node, "slow-rate").await;
    let rate_slug = uniq("rate");
    ship(&node, &rate.key, &rate_slug, CONSUMER).await;

    let codes = from(&node, &narrow_slug, "/burst?n=20&ms=400").await["value"].clone();
    let ok = codes["ok"].as_u64().unwrap_or(0);
    assert!((1..=4).contains(&ok), "at most max_concurrency in flight: {codes}");
    assert_eq!(codes["sdk_rate_limited"].as_u64().unwrap_or(0) + ok, 20, "{codes}");

    let codes = from(&node, &rate_slug, "/burst?n=30").await["value"].clone();
    let ok = codes["ok"].as_u64().unwrap_or(0);
    assert!((1..=10).contains(&ok), "about rps calls per second: {codes}");
    assert_eq!(codes["sdk_rate_limited"].as_u64().unwrap_or(0) + ok, 30, "{codes}");

    let codes = from(&node, &pro_slug, "/burst?n=20").await["value"].clone();
    assert_eq!(codes, json!({"ok": 20}), "another tenant is unaffected");
    node.shutdown().await;
}
