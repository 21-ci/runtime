//! RPC contracts and the operator SDK control API (docs/api.md).
//!
//! - `GET /v1/services/{s}/types`: the `.d.ts` of a Deno service's RPC surface (`21ci types`).

use reqwest::Method;
use serde_json::json;

use crate::common::*;

const RPC_SERVICE: &str = r#"
export interface Registration { email: string; event: string }
export const rpc = {
  async confirmRegistration(r: Registration): Promise<{ receipt: string }> { return { receipt: r.email }; },
  admin: { async purge(): Promise<number> { return 0; } },
};
export default { fetch() { return new Response("ok"); } };
"#;

/// TYPES-01: the live version's RPC surface as TypeScript; a service without one, or a WASM
/// service, is `422 no_rpc_surface`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_types_01_a_service_rpc_surface_as_typescript() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("notify");
    let id = ship(&node, &t.key, &slug, RPC_SERVICE).await;

    let r = get(&node, &t.key, &format!("/v1/services/{id}/types")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(r
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|c| c.starts_with("text/plain")));
    assert!(
        r.text
            .contains("export interface Registration { email: string; event: string }"),
        "{}",
        r.text
    );
    assert!(
        r.text
            .contains("  confirmRegistration(arg: Registration): Promise<{ receipt: string }>;"),
        "{}",
        r.text
    );
    assert!(
        r.text.contains("  admin: {\n    purge(): Promise<number>;\n  };"),
        "{}",
        r.text
    );

    let plain = ship(
        &node,
        &t.key,
        &uniq("plain"),
        "export default { fetch() { return new Response('x'); } };",
    )
    .await;
    let r = get(&node, &t.key, &format!("/v1/services/{plain}/types")).await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].clone()),
        (422, json!("no_rpc_surface")),
        "{}",
        r.text
    );

    let wasm = ship_wasm(&node, &t.key, &uniq("wasm"), "rpc").await;
    let r = get(&node, &t.key, &format!("/v1/services/{wasm}/types")).await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].clone()),
        (422, json!("no_rpc_surface")),
        "{}",
        r.text
    );

    // Another tenant's service does not exist for this key.
    let other = new_tenant(&node, "pro").await;
    assert_eq!(
        get(&node, &other.key, &format!("/v1/services/{id}/types")).await.s(),
        404
    );
    node.shutdown().await;
}

// ---------------------------------------------------------------------------------------------
// The operator SDK control API (operator: root token; tenant side: effective SDKs, contracts)
// ---------------------------------------------------------------------------------------------

/// SDK-API-01: a published release is listed with its contract members and becomes `stable`
/// (the first one); the same version again is `409 sdk_release_exists`; names and versions are
/// checked; tenants cannot publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_api_01_publish_list_and_releases() {
    let node = start_node().await;
    let r = publish_sdk(&node, "acme", "1.0.0", &sdk_source("v1"), None).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    assert_eq!(
        (r.json["sdk"].clone(), r.json["version"].clone()),
        (json!("acme"), json!("1.0.0"))
    );
    assert_eq!(r.json["members"], json!(["ping", "echo", "fail", "slow", "burn"]));
    assert_eq!(r.json["channels"], json!(["stable"]), "the first release is stable");

    let again = publish_sdk(&node, "acme", "1.0.0", &sdk_source("v1"), None).await;
    assert_eq!(
        (again.s(), again.json["error"]["code"].clone()),
        (409, json!("sdk_release_exists")),
        "{}",
        again.text
    );
    let r = publish_sdk(&node, "acme", "1.1.0", &sdk_source("v2"), None).await;
    assert_eq!(
        (r.s(), r.json["channels"].clone()),
        (201, json!([])),
        "a later release is not promoted: {}",
        r.text
    );

    let list = call(root(&node, Method::GET, "/v1/sdks"), None).await;
    assert_eq!(list.s(), 200, "{}", list.text);
    let acme = &list.json.as_array().expect("array")[0];
    assert_eq!(
        (acme["name"].clone(), acme["state"].clone()),
        (json!("acme"), json!("active"))
    );
    assert_eq!(acme["channels"]["stable"]["version"], json!("1.0.0"));
    assert_eq!(acme["releases"], json!(2));
    let rels = call(root(&node, Method::GET, "/v1/sdks/acme/releases"), None).await;
    let versions: Vec<_> = rels
        .json
        .as_array()
        .expect("array")
        .iter()
        .map(|r| r["version"].clone())
        .collect();
    assert_eq!(versions, [json!("1.0.0"), json!("1.1.0")]);

    for (name, version) in [("Acme", "1.0.0"), ("a", "1.0.0"), ("acme", "1.0"), ("acme", "x")] {
        let r = publish_sdk(&node, name, version, &sdk_source("v1"), None).await;
        assert_eq!(r.s(), 422, "{name} {version}: {}", r.text);
    }
    let no_surface = publish_sdk(
        &node,
        "plain",
        "1.0.0",
        "export default { fetch() { return new Response('x'); } };",
        None,
    )
    .await;
    assert_eq!(
        (no_surface.s(), no_surface.json["error"]["code"].clone()),
        (422, json!("sdk_contract_missing"))
    );
    // An SDK method that never reads ctx.caller is refused when the SDK is published.
    let unchecked = "import { defineSdk } from \"std:sdk/server\";\nexport default defineSdk({ methods: { async ping() { return 1; } } });";
    let r = publish_sdk(&node, "lax", "1.0.0", unchecked, None).await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].clone()),
        (422, json!("sdk_unchecked_caller")),
        "{}",
        r.text
    );

    let t = new_tenant(&node, "pro").await;
    let r = post(
        &node,
        &t.key,
        "/v1/sdks",
        json!({"name": "mine", "version": "1.0.0", "source": ""}),
    )
    .await;
    assert_eq!(r.s(), 403, "{}", r.text);
    assert_eq!(
        get(&node, &t.key, "/v1/sdks").await.s(),
        403,
        "tenants list their effective SDKs instead"
    );
    node.shutdown().await;
}

/// SDK-API-02 (SDK-05, SDK-06 within this scope): grants are the operator's; an allow without a
/// requirement is pinned to the stable major; the tenant sees what its grants resolve to and
/// downloads that contract; an SDK it is not granted does not exist for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_api_02_grants_effective_sdks_and_contracts() {
    let node = start_node().await;
    assert_eq!(
        publish_sdk(&node, "acme", "1.0.0", &sdk_source("v1"), None).await.s(),
        201
    );
    let t = new_tenant(&node, "pro").await;

    // Nothing granted yet.
    let eff = get(&node, &t.key, "/v1/sdks/effective").await;
    assert_eq!((eff.s(), eff.json.clone()), (200, json!([])), "{}", eff.text);
    assert_eq!(get(&node, &t.key, "/v1/sdks/acme/contract").await.s(), 404);

    let g = put_grant(&node, json!({"scope": "node", "sdk": "acme", "effect": "allow"})).await;
    assert_eq!(g.s(), 200, "{}", g.text);
    assert_eq!(
        g.json["version_req"],
        json!("^1"),
        "pinned to the stable major: {}",
        g.text
    );
    let eff = get(&node, &t.key, "/v1/sdks/effective").await;
    let acme = &eff.json[0];
    assert_eq!(
        (acme["name"].clone(), acme["version"].clone(), acme["channel"].clone()),
        (json!("acme"), json!("1.0.0"), json!("stable")),
        "{}",
        eff.text
    );
    let c = get(&node, &t.key, "/v1/sdks/acme/contract").await;
    assert_eq!(c.s(), 200, "{}", c.text);
    assert!(
        c.text.contains("declare module \"sdk:acme\"") && c.text.contains("ping(arg: void): Promise<Pong>;"),
        "{}",
        c.text
    );
    assert!(
        c.text.contains("export const acme: import(\"sdk:acme\").Acme;"),
        "{}",
        c.text
    );

    // A deny at the tenant's scope removes it (a lower level only narrows).
    let deny = put_grant(
        &node,
        json!({"scope": format!("tenant:{}", t.id), "sdk": "acme", "effect": "deny"}),
    )
    .await;
    assert_eq!(deny.s(), 200, "{}", deny.text);
    assert_eq!(get(&node, &t.key, "/v1/sdks/effective").await.json, json!([]));
    assert_eq!(get(&node, &t.key, "/v1/sdks/acme/contract").await.s(), 404);
    let all = call(root(&node, Method::GET, "/v1/sdk-grants"), None).await;
    assert_eq!(all.json.as_array().map(Vec::len), Some(2), "{}", all.text);
    // Clearing every field deletes the grant.
    let clear = put_grant(&node, json!({"scope": format!("tenant:{}", t.id), "sdk": "acme"})).await;
    assert_eq!(clear.s(), 204, "{}", clear.text);
    assert_eq!(
        get(&node, &t.key, "/v1/sdks/effective").await.json[0]["name"],
        json!("acme")
    );

    // Grants are written by the operator only; bad scopes and requirements are refused.
    let r = put(
        &node,
        &t.key,
        "/v1/sdk-grants",
        json!({"scope": format!("tenant:{}", t.id), "sdk": "acme", "effect": "allow"}),
    )
    .await;
    assert_eq!(r.s(), 403, "{}", r.text);
    for bad in [
        json!({"scope": "galaxy", "sdk": "acme", "effect": "allow"}),
        json!({"scope": "node", "sdk": "Acme!", "effect": "allow"}),
        json!({"scope": "node", "sdk": "acme", "version_req": "not a req"}),
        json!({"scope": "node", "sdk": "acme", "channel": "Bad Channel"}),
        json!({"scope": "node", "sdk": "acme", "effect": "maybe"}),
    ] {
        assert_eq!(put_grant(&node, bad.clone()).await.s(), 422, "{bad}");
    }
    node.shutdown().await;
}

/// SDK-API-03: promote and roll back a channel (the version it pointed to is kept as `previous`);
/// an unknown version is 404; removal marks the SDK removed and takes it out of the effective list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_api_03_channels_and_removal() {
    let node = start_node().await;
    assert_eq!(
        publish_sdk(&node, "acme", "1.0.0", &sdk_source("v1"), None).await.s(),
        201
    );
    assert_eq!(
        publish_sdk(&node, "acme", "1.1.0", &sdk_source("v2"), Some("canary"))
            .await
            .json["channels"],
        json!(["canary"])
    );
    let r = call(
        root(&node, Method::PUT, "/v1/sdks/acme/channels/stable"),
        Some(json!({"version": "1.1.0"})),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(
        (r.json["version"].clone(), r.json["previous"].clone()),
        (json!("1.1.0"), json!("1.0.0"))
    );
    let r = call(
        root(&node, Method::PUT, "/v1/sdks/acme/channels/stable"),
        Some(json!({"version": "9.0.0"})),
    )
    .await;
    assert_eq!(r.s(), 404, "{}", r.text);
    let r = call(
        root(&node, Method::PUT, "/v1/sdks/acme/channels/Bad"),
        Some(json!({"version": "1.0.0"})),
    )
    .await;
    assert_eq!(r.s(), 422, "{}", r.text);

    put_grant(&node, json!({"scope": "node", "sdk": "*", "effect": "allow"})).await;
    let t = new_tenant(&node, "pro").await;
    assert_eq!(
        get(&node, &t.key, "/v1/sdks/effective").await.json[0]["version"],
        json!("1.1.0")
    );

    assert_eq!(call(root(&node, Method::DELETE, "/v1/sdks/acme"), None).await.s(), 204);
    assert_eq!(call(root(&node, Method::DELETE, "/v1/sdks/nope"), None).await.s(), 404);
    let list = call(root(&node, Method::GET, "/v1/sdks"), None).await;
    assert_eq!(list.json[0]["state"], json!("removed"));
    assert_eq!(get(&node, &t.key, "/v1/sdks/effective").await.json, json!([]));
    // Publishing again restores it.
    assert_eq!(
        publish_sdk(&node, "acme", "1.2.0", &sdk_source("v3"), Some("stable"))
            .await
            .s(),
        201
    );
    assert_eq!(
        get(&node, &t.key, "/v1/sdks/effective").await.json[0]["version"],
        json!("1.2.0")
    );
    node.shutdown().await;
}

/// SDK-API-04: the system tenant that owns the SDK services is not a tenant like the others: it is
/// not listed, cannot be changed or deleted, and its slug prefix is reserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sdk_api_04_the_system_tenant_is_reserved() {
    let node = start_node().await;
    let sys = "ten_00000000000000000000000SYS";
    let list = call(root(&node, Method::GET, "/v1/tenants"), None).await;
    assert!(!list.text.contains(sys), "{}", list.text);
    let r = call(
        root(&node, Method::PATCH, &format!("/v1/tenants/{sys}")),
        Some(json!({"plan": "pro"})),
    )
    .await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].clone()),
        (403, json!("reserved")),
        "{}",
        r.text
    );
    assert_eq!(
        call(root(&node, Method::DELETE, &format!("/v1/tenants/{sys}")), None)
            .await
            .s(),
        403
    );
    assert_eq!(
        call(
            root(&node, Method::POST, &format!("/v1/tenants/{sys}/keys")),
            Some(json!({"scopes": ["deploy"]}))
        )
        .await
        .s(),
        403
    );

    let t = new_tenant(&node, "pro").await;
    let r = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": "sys-sdk-acme", "runtime": "deno"}),
    )
    .await;
    assert_eq!(r.s(), 422, "{}", r.text);
    node.shutdown().await;
}
