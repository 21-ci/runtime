//! Edge cases of keys, scopes and tenants on the control API (scratchpad edge-cases.md §1.2-1.6):
//! request validation, the self-service key cap, delegation and revocation, restriction lists,
//! cross-tenant ids and tenant deletion.

use reqwest::Method;
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

async fn self_key(node: &impl Node, key: &str, body: Value) -> Resp {
    post(node, key, "/v1/keys", body).await
}

node_test! {
/// KEY-008: deleting a tenant revokes its keys at once: the next request with one is `401`.
async fn key_008_a_deleted_tenants_key_stops_at_once() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    assert_eq!(get(&node, &t.key, "/v1/services").await.s(), 200, "warms the key cache");
    let d = call(root(&node, Method::DELETE, &format!("/v1/tenants/{}?purge=1", t.id)), None).await;
    assert_eq!(d.s(), 204, "{}", d.text);
    let r = get(&node, &t.key, "/v1/services").await;
    assert_eq!(r.s(), 401, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// KEY-023 / KEY-024 / KEY-026: `expires_at` must be RFC 3339 with an offset; `scopes` must be a
/// non-empty list of known scopes (duplicates collapse); environment labels follow
/// `^[a-z][a-z0-9-]{0,31}$`. Each refusal is `422 validation_failed`.
async fn key_023_024_026_key_requests_are_validated() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let path = format!("/v1/tenants/{}/keys", t.id);
    let bad = [
        json!({"scopes": ["deploy"], "expires_at": "2099-10-23"}),
        json!({"scopes": ["deploy"], "expires_at": "4102444800000"}),
        json!({"scopes": ["deploy"], "expires_at": "2099-10-23T00:00:00"}),
        json!({"scopes": []}),
        json!({"scopes": ["admin"]}),
        json!({"scopes": ["deploy"], "environments": ["Prod"]}),
        json!({"scopes": ["deploy"], "environments": ["-x"]}),
        json!({"scopes": ["deploy"], "environments": ["a".repeat(33)]}),
        json!({"scopes": ["deploy"], "environments": [""]}),
    ];
    for body in bad {
        let r = call(root(&node, Method::POST, &path), Some(body.clone())).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{body}: {}", r.text);
    }
    let ok = call(root(&node, Method::POST, &path), Some(json!({"scopes": ["deploy", "deploy", "services:read"], "expires_at": "2099-10-23T00:00:00Z"}))).await;
    assert_eq!(ok.s(), 201, "{}", ok.text);
    let mut scopes: Vec<String> = ok.json["scopes"].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_string()).collect();
    scopes.sort();
    assert_eq!(scopes, ["deploy", "services:read"], "duplicates collapse: {}", ok.text);
    node.shutdown().await;
}
}

node_test! {
/// KEY-025 / SCOPE-007: a key's `project_ids` must be `prj_` ids of the tenant's own projects, and
/// a service cannot be created in another tenant's project (named as "does not exist": no leak).
async fn key_025_scope_007_projects_of_another_tenant_are_refused() {
    let node = start_node().await;
    let a = new_tenant(&node, "pro").await;
    let b = new_tenant(&node, "pro").await;
    let foreign = create_project(&node, &b.key, &uniq("p"), &[]).await;
    let path = format!("/v1/tenants/{}/keys", a.id);
    for ids in [json!([foreign.clone()]), json!(["prj_not-an-id"]), json!(["default"])] {
        let r = call(root(&node, Method::POST, &path), Some(json!({"scopes": ["deploy"], "project_ids": ids}))).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{ids}: {}", r.text);
    }
    let own = create_project(&node, &a.key, &uniq("own"), &[]).await;
    let (_, restricted) = make_key(&node, &a.id, &["services:read", "services:write"], Some(vec![own])).await;
    let r = post(&node, &restricted, "/v1/services", json!({"slug": uniq("x"), "runtime": "deno", "project": foreign})).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    assert!(r.text.contains("does not exist"), "{}", r.text);
    let listed = call(root_for(&node, &b.id, Method::GET, "/v1/services"), None).await;
    assert!(listed.items().is_empty(), "nothing was created in tenant B: {}", listed.text);
    node.shutdown().await;
}
}

node_test! {
/// KEY-028 / KEY-029: the self-service cap counts live keys only (revoked and expired ones free a
/// place); keys the operator creates are not capped, but they count, so `/v1/keys` then refuses.
async fn key_028_029_the_self_service_cap_counts_live_keys() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["services:read", "keys:write"]).await;
    // `t.key` is key 1. Fill to 200 with keys that expire in 2 s, and one long-lived key.
    let mut ids = Vec::new();
    for _ in 0..198 {
        let r = self_key(&node, &t.key, json!({"scopes": ["services:read"], "expires_at": rfc3339_in(2)})).await;
        assert_eq!(r.s(), 201, "{}", r.text);
    }
    let r = self_key(&node, &t.key, json!({"scopes": ["services:read"]})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    ids.push(r.json["id"].as_str().unwrap().to_string());
    let full = self_key(&node, &t.key, json!({"scopes": ["services:read"]})).await;
    assert_eq!((full.s(), full.code()), (403, "quota_exceeded".to_string()), "201st: {}", full.text);
    // A revoked key frees a place.
    assert_eq!(delete(&node, &t.key, &format!("/v1/keys/{}", ids[0])).await.s(), 204);
    assert_eq!(self_key(&node, &t.key, json!({"scopes": ["services:read"]})).await.s(), 201, "after a revoke");
    assert_eq!(self_key(&node, &t.key, json!({"scopes": ["services:read"]})).await.s(), 403, "full again");
    // Expired keys free their places.
    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;
    let after = self_key(&node, &t.key, json!({"scopes": ["services:read"]})).await;
    assert_eq!(after.s(), 201, "expired keys do not count: {}", after.text);

    // KEY-029: the operator is not capped…
    let u = new_tenant_scopes(&node, "pro", &["services:read", "keys:write"]).await;
    for _ in 0..200 {
        make_key(&node, &u.id, &["services:read"], None).await;
    }
    // …and those keys count against the tenant's own minting.
    let r = self_key(&node, &u.key, json!({"scopes": ["services:read"]})).await;
    assert_eq!((r.s(), r.code()), (403, "quota_exceeded".to_string()), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// KEY-032 (pinned, SECURITY.md "Tenant tokens"): a child holding the same scopes as its parent
/// may revoke the parent, and with it the whole tree, itself included.
async fn key_032_a_child_with_equal_scopes_can_revoke_its_parent_tree() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["services:read", "keys:write"]).await;
    let child = self_key(&node, &t.key, json!({"scopes": ["services:read", "keys:write"]})).await;
    assert_eq!(child.s(), 201, "{}", child.text);
    let child_secret = child.json["secret"].as_str().unwrap().to_string();
    let sibling = self_key(&node, &t.key, json!({"scopes": ["services:read"]})).await;
    let sibling_secret = sibling.json["secret"].as_str().unwrap().to_string();
    let r = delete(&node, &child_secret, &format!("/v1/keys/{}", t.key_id)).await;
    assert_eq!(r.s(), 204, "{}", r.text);
    for (who, k) in [("parent", &t.key), ("child", &child_secret), ("sibling", &sibling_secret)] {
        assert_eq!(get(&node, k, "/v1/services").await.s(), 401, "{who} is revoked");
    }
    node.shutdown().await;
}
}

node_test! {
/// KEY-035: a child minted while its parent is being revoked never outlives the parent: after both
/// requests, the child (if one was created) does not authenticate.
async fn key_035_a_child_minted_during_the_parents_revocation_dies_with_it() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    for round in 0..20 {
        let (pid, parent) = make_key(&node, &t.id, &["services:read", "keys:write"], None).await;
        let revoke = call(root(&node, Method::DELETE, &format!("/v1/tenants/{}/keys/{pid}", t.id)), None);
        let mint = self_key(&node, &parent, json!({"scopes": ["services:read"]}));
        let (rv, mk) = tokio::join!(revoke, mint);
        assert_eq!(rv.s(), 204, "round {round}: {}", rv.text);
        // Refused either as unauthenticated (401) or, once the rounds' failed attempts trip the failed-auth
        // limiter, as rate limited (429 `rate_limited`): both mean the key did not authenticate.
        let refused = |s: u16, code: String| s == 401 || (s == 429 && code == "rate_limited");
        if mk.s() == 201 {
            let secret = mk.json["secret"].as_str().unwrap();
            let r = get(&node, secret, "/v1/services").await;
            assert!(refused(r.s(), r.code()), "round {round}: a child of a revoked parent authenticated: {}", r.text);
        } else {
            assert!(refused(mk.s(), mk.code()), "round {round}: {}", mk.text);
        }
    }
    node.shutdown().await;
}
}

node_test! {
/// KEY-037 / TEN-013: display names are bounded (1 to 100 characters, no control characters):
/// they are echoed in lists, audit events and logs.
async fn key_037_ten_013_names_are_bounded_and_printable() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["services:read", "keys:write"]).await;
    let long = "n".repeat(101);
    for name in [long.as_str(), "line\nbreak", "bell\u{7}"] {
        let r = self_key(&node, &t.key, json!({"scopes": ["services:read"], "name": name})).await;
        assert_eq!(r.s(), 422, "key name {:?}: {}", &name[..name.len().min(20)], r.text);
        let r = call(root(&node, Method::POST, "/v1/tenants"), Some(json!({"name": name, "plan": "pro"}))).await;
        assert_eq!(r.s(), 422, "tenant name {:?}: {}", &name[..name.len().min(20)], r.text);
        let r = call(root(&node, Method::PATCH, &format!("/v1/tenants/{}", t.id)), Some(json!({"name": name}))).await;
        assert_eq!(r.s(), 422, "tenant rename {:?}: {}", &name[..name.len().min(20)], r.text);
    }
    let r = call(root(&node, Method::POST, "/v1/tenants"), Some(json!({"name": "   ", "plan": "pro"}))).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    let ok = self_key(&node, &t.key, json!({"scopes": ["services:read"], "name": "n".repeat(100)})).await;
    assert_eq!(ok.s(), 201, "{}", ok.text);
    let ok = self_key(&node, &t.key, json!({"scopes": ["services:read"], "name": "ci – deploy key ✓"})).await;
    assert_eq!(ok.s(), 201, "{}", ok.text);
    node.shutdown().await;
}
}

node_test! {
/// SCOPE-001: write routes beyond the ones `scopes_gate_every_write_surface` lists refuse a
/// `services:read`-only key with `403 insufficient_scope`.
async fn scope_001_more_write_routes_refuse_a_read_only_key() {
    let node = start_node().await;
    let full = new_tenant(&node, "pro").await;
    let svc = ship(&node, &full.key, &uniq("s001"), guests::HELLO).await;
    let sch = post(&node, &full.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "interval", "every_seconds": 60})).await;
    let sch = sch.json["id"].as_str().expect("schedule").to_string();
    let (_, ro) = make_key(&node, &full.id, &["services:read"], None).await;
    let routes: Vec<(Method, String, Option<Value>)> = vec![
        (Method::POST, format!("/v1/services/{svc}/versions/1/rebuild"), None),
        (Method::PUT, format!("/v1/services/{svc}/hosts/app.example.com"), None),
        (Method::DELETE, format!("/v1/services/{svc}/hosts/app.example.com"), None),
        (Method::PATCH, format!("/v1/services/{svc}/schedules/{sch}"), Some(json!({"kind": "interval", "every_seconds": 120}))),
        (Method::DELETE, format!("/v1/services/{svc}/schedules/{sch}"), None),
        (Method::POST, format!("/v1/services/{svc}/dlq/redrive"), Some(json!({}))),
        (Method::DELETE, format!("/v1/services/{svc}/dlq"), None),
        (Method::POST, "/v1/projects".into(), Some(json!({"slug": uniq("p")}))),
        (Method::PATCH, "/v1/projects/default".into(), Some(json!({"name": "x"}))),
        (Method::DELETE, "/v1/projects/default".into(), None),
        (Method::POST, "/v1/keys".into(), Some(json!({"scopes": ["services:read"]}))),
        (Method::GET, format!("/v1/services/{svc}/versions/1/build/logs"), None),
        (Method::GET, format!("/v1/services/{svc}/stats"), None),
        (Method::DELETE, format!("/v1/services/{svc}/objects/o1"), None),
    ];
    for (m, path, body) in routes {
        let r = call(api(&node, &ro, m.clone(), &path), body).await;
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{m} {path}: {}", r.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// SCOPE-004: explicit empty restriction lists allow nothing: no services listed, none
/// readable, none creatable.
async fn scope_004_empty_restriction_lists_allow_nothing() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("s004"), "deno").await;
    let empty_projects = make_key(&node, &t.id, &["services:read", "services:write"], Some(vec![])).await.1;
    let empty_envs = make_key_in(&node, &t.id, &["services:read", "services:write"], None, Some(vec![])).await.1;
    for k in [&empty_projects, &empty_envs] {
        let l = get(&node, k, "/v1/services").await;
        assert_eq!(l.s(), 200, "{}", l.text);
        assert!(l.items().is_empty(), "{}", l.text);
        assert_eq!(get(&node, k, &format!("/v1/services/{svc}")).await.s(), 404);
        let c = post(&node, k, "/v1/services", json!({"slug": uniq("no"), "runtime": "deno"})).await;
        assert!(matches!(c.s(), 403 | 422), "create with an empty restriction: {}", c.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// SCOPE-009: a service never moves between projects, not even by the operator (`422`).
async fn scope_009_no_service_moves_between_projects() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let other = create_project(&node, &t.key, &uniq("other"), &[]).await;
    let svc = create_service(&node, &t.key, &uniq("s009"), "deno").await;
    let r = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"project": other})).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    let r = call(root_for(&node, &t.id, Method::PATCH, &format!("/v1/services/{svc}")), Some(json!({"project": other}))).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    let same = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"project": "default"})).await;
    assert_eq!(same.s(), 200, "naming its own project is fine: {}", same.text);
    node.shutdown().await;
}
}

node_test! {
/// SCOPE-012: usage `from`/`to` must be real calendar dates `YYYY-MM-DD`; `from > to` is an empty
/// answer, not an error.
async fn scope_012_usage_dates_are_calendar_dates() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let base = format!("/v1/tenants/{}/usage", t.id);
    for q in ["from=2026-13-45", "from=2026-1-01", "to=20260101", "from=0000-00-00", "to=2026-02-30"] {
        let r = get(&node, &t.key, &format!("{base}?{q}")).await;
        assert_eq!(r.s(), 422, "{q}: {}", r.text);
    }
    let r = get(&node, &t.key, &format!("{base}?from=2026-12-31&to=2026-01-01")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json, json!([]));
    node.shutdown().await;
}
}

node_test! {
/// TEN-002: a schedule id of another tenant's service is `404` on one's own service, and stays.
async fn ten_002_a_foreign_schedule_id_is_not_found() {
    let node = start_node().await;
    let a = new_tenant(&node, "pro").await;
    let b = new_tenant(&node, "pro").await;
    let sa = create_service(&node, &a.key, &uniq("sa"), "deno").await;
    let sb = create_service(&node, &b.key, &uniq("sb"), "deno").await;
    let x = post(&node, &b.key, &format!("/v1/services/{sb}/schedules"), json!({"kind": "interval", "every_seconds": 60})).await;
    let x = x.json["id"].as_str().expect("schedule").to_string();
    let p = patch(&node, &a.key, &format!("/v1/services/{sa}/schedules/{x}"), json!({"kind": "interval", "every_seconds": 5})).await;
    assert_eq!(p.s(), 404, "{}", p.text);
    assert_eq!(delete(&node, &a.key, &format!("/v1/services/{sa}/schedules/{x}")).await.s(), 404);
    let l = get(&node, &b.key, &format!("/v1/services/{sb}/schedules")).await;
    assert_eq!(l.json[0]["every_seconds"], json!(60), "untouched: {}", l.text);
    node.shutdown().await;
}
}

node_test! {
/// TEN-004: the operator naming a malformed or unknown tenant in `x-rt-tenant` gets `404`, never
/// a global listing.
async fn ten_004_an_unknown_x_rt_tenant_is_not_found() {
    let node = start_node().await;
    new_tenant(&node, "pro").await;
    for h in ["ten_nope", "ten_01ARZ3NDEKTSV4RRFFQ69G5FAV", "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"] {
        let r = call(root_for(&node, h, Method::GET, "/v1/services"), None).await;
        assert_eq!(r.s(), 404, "{h}: {}", r.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// TEN-011: tenant `overrides` must be an object (or `null`) on create and on PATCH.
async fn ten_011_overrides_must_be_an_object() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    for o in [json!([1]), json!("x"), json!(3)] {
        let c = call(root(&node, Method::POST, "/v1/tenants"), Some(json!({"name": "n", "plan": "pro", "overrides": o}))).await;
        assert_eq!(c.s(), 422, "create {o}: {}", c.text);
        let p = call(root(&node, Method::PATCH, &format!("/v1/tenants/{}", t.id)), Some(json!({"overrides": o}))).await;
        assert_eq!(p.s(), 422, "patch {o}: {}", p.text);
    }
    let c = call(root(&node, Method::POST, "/v1/tenants"), Some(json!({"name": "n", "plan": "pro", "overrides": null}))).await;
    assert_eq!(c.s(), 201, "{}", c.text);
    node.shutdown().await;
}
}

node_test! {
/// TEN-009: a tenant delete racing a service create leaves no service behind: a create that
/// answered `201` is gone with the tenant.
async fn ten_009_a_tenant_delete_racing_a_service_create_leaves_nothing() {
    let node = start_node().await;
    for round in 0..20 {
        let t = create_tenant(&node, "pro").await;
        let del = call(root(&node, Method::DELETE, &format!("/v1/tenants/{t}?purge=1")), None);
        let mk = call(root_for(&node, &t, Method::POST, "/v1/services"), Some(json!({"slug": uniq("race"), "runtime": "deno"})));
        let (d, c) = tokio::join!(del, mk);
        assert_eq!(d.s(), 204, "round {round}: {}", d.text);
        if c.s() == 201 {
            let id = c.json["id"].as_str().unwrap();
            let g = call(root(&node, Method::GET, &format!("/v1/services/{id}")), None).await;
            assert_eq!(g.s(), 404, "round {round}: the service outlived its tenant: {}", g.text);
        }
    }
    node.shutdown().await;
}
}

node_test! {
/// SDKAPI-003 / SDKAPI-004: a tenant reads effective SDKs only for its own projects inside its key
/// restriction, and cannot pick a release with `?version=` (only the operator can).
async fn sdkapi_003_004_tenant_sdk_reads_stay_in_scope() {
    let node = start_node().await;
    assert_eq!(publish_sdk(&node, "acme", "1.0.0", &sdk_source("v1"), None).await.s(), 201);
    let v2 = sdk_source("v2").replace("async echo(", "async extra(_a: void, ctx: any): Promise<number> { return ctx.caller ? 1 : 0; },\n    async echo(");
    let r = publish_sdk(&node, "acme", "2.0.0", &v2, None).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    assert_eq!(put_grant(&node, json!({"scope": "node", "sdk": "acme", "effect": "allow"})).await.s(), 200);
    let t = new_tenant(&node, "pro").await;
    let other = new_tenant(&node, "pro").await;
    let foreign = create_project(&node, &other.key, &uniq("fp"), &[]).await;
    let r = get(&node, &t.key, &format!("/v1/sdks/effective?project={foreign}")).await;
    assert_eq!(r.s(), 404, "another tenant's project: {}", r.text);
    let own = create_project(&node, &t.key, &uniq("own"), &[]).await;
    let (_, restricted) = make_key(&node, &t.id, &["services:read"], Some(vec![own])).await;
    let r = get(&node, &restricted, "/v1/sdks/effective?project=default").await;
    assert_eq!(r.s(), 404, "a project outside the key's restriction: {}", r.text);

    let c = get(&node, &t.key, "/v1/sdks/acme/contract?version=2.0.0").await;
    assert_eq!(c.s(), 200, "{}", c.text);
    let selected = get(&node, &t.key, "/v1/sdks/acme/contract").await;
    assert_eq!(c.text, selected.text, "`?version=` is ignored for a tenant: the granted (stable 1.x) release");
    assert!(!c.text.contains("extra("), "the tenant got 1.0.0: {}", c.text);
    let op = call(root(&node, Method::GET, "/v1/sdks/acme/contract?version=2.0.0"), None).await;
    assert_eq!(op.s(), 200);
    assert!(op.text.contains("extra("), "the operator names 2.0.0: {}", op.text);
    node.shutdown().await;
}
}
