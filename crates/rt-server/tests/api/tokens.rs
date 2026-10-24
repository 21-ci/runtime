//! Decision D-B4 end to end: a tenant key with `keys:write` creates, lists and revokes keys of its
//! own tenant (SEC-57..60). A new key never holds more than its creator: scopes, project and
//! environment restriction and lifetime are all capped; an expired key stops authenticating; every change is
//! audited and the secret is shown once.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::Method;
use rt_sec::{Actor, AuditEvent, AuditSink};
use rt_server::StartOptions;
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<AuditEvent>>,
}

impl AuditSink for Recorder {
    fn emit(&self, ev: AuditEvent) {
        self.events.lock().expect("recorder lock").push(ev);
    }
}

impl Recorder {
    fn all(&self, action: &str) -> Vec<AuditEvent> {
        self.events
            .lock()
            .expect("lock")
            .iter()
            .filter(|e| e.action == action)
            .cloned()
            .collect()
    }
}

/// Operator-made key with `keys:write` next to `scopes`: `(key_id, secret)`.
async fn manager(node: &impl Node, tenant: &str, scopes: &[&str], projects: Option<Vec<String>>) -> (String, String) {
    let mut all = scopes.to_vec();
    all.push("keys:write");
    make_key(node, tenant, &all, projects).await
}

fn scopes_of(v: &Value) -> Vec<String> {
    v["scopes"]
        .as_array()
        .expect("scopes")
        .iter()
        .map(|s| s.as_str().expect("scope").to_string())
        .collect()
}

node_test! {
async fn ac_sec_57_a_key_with_keys_write_creates_keys_within_its_own_scopes() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, k1) = manager(&node, &t.id, &["services:read", "services:write", "deploy"], None).await;

    // A subset of its scopes: created, the secret is returned, and the new key works.
    let r = post(&node, &k1, "/v1/keys", json!({"scopes": ["services:read", "deploy"], "name": "ci"})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    assert_eq!(scopes_of(&r.json), ["services:read", "deploy"]);
    assert_eq!(r.json["name"], json!("ci"));
    assert!(r.json["project_ids"].is_null() && r.json["environments"].is_null() && r.json["expires_at"].is_null());
    let secret = r.json["secret"].as_str().expect("the secret is returned once").to_string();
    assert!(secret.starts_with("rtk_"), "{secret}");
    let child_id = r.json["id"].as_str().expect("id").to_string();
    assert_eq!(get(&node, &secret, "/v1/services").await.s(), 200, "the new key authenticates");
    let me = get(&node, &secret, "/v1/me").await;
    assert_eq!((me.json["tenant_id"].clone(), me.json["key_id"].clone()), (json!(t.id), json!(child_id)), "in the creator's tenant");

    // Escalation is refused, whatever it asks for beyond the creator.
    for scopes in [json!(["secrets:write"]), json!(["services:read", "data:admin"]), json!(["env:write", "deploy"])] {
        let r = post(&node, &k1, "/v1/keys", json!({"scopes": scopes})).await;
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{scopes}: {}", r.text);
        assert_eq!(r.json["error"]["details"]["reason"], json!("escalation"));
    }
    // `keys:write` itself may be passed on (it is the creator's own scope).
    let r = post(&node, &k1, "/v1/keys", json!({"scopes": ["keys:write"]})).await;
    assert_eq!(r.s(), 201, "{}", r.text);

    // A key without keys:write cannot create, list or revoke keys.
    for r in [
        post(&node, &secret, "/v1/keys", json!({"scopes": ["services:read"]})).await,
        get(&node, &secret, "/v1/keys").await,
        delete(&node, &secret, &format!("/v1/keys/{child_id}")).await,
    ] {
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);
    }

    // Bad requests.
    for bad in [
        json!({"scopes": []}),
        json!({"scopes": ["root"]}),
        json!({}),
        json!({"scopes": ["services:read"], "expires_at": "tomorrow"}),
        json!({"scopes": ["services:read"], "expires_at": rfc3339_in(-60)}),
        json!({"scopes": ["services:read"], "project_ids": ["prj_nope"]}),
        json!({"scopes": ["services:read"], "project_ids": ["prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"]}),
        json!({"scopes": ["services:read"], "environments": ["Production!"]}),
        json!({"scopes": ["services:read"], "project_ids": ["svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"]}),
    ] {
        let r = post(&node, &k1, "/v1/keys", bad.clone()).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{bad}: {}", r.text);
    }
    assert_eq!(send(node.http().post(node.control_url("/v1/keys")).json(&json!({"scopes": ["services:read"]}))).await.s(), 401);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_57_a_new_key_stays_within_the_creators_project_and_environment_restriction() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let pa = create_project(&node, &t.key, &uniq("pa"), &["production", "staging"]).await;
    let pb = create_project(&node, &t.key, &uniq("pb"), &[]).await;
    let a = create_service_in(&node, &t.key, &pa, "production", &uniq("a"), "wasm").await;
    let b = create_service_in(&node, &t.key, &pb, "production", &uniq("b"), "wasm").await;
    let (_, limited) = make_key_in(&node, &t.id, &["services:read", "keys:write"], Some(vec![pa.clone()]), None).await;
    let (_, free) = make_key(&node, &t.id, &["services:read", "keys:write"], None).await;

    // Inside the restriction.
    let r = post(&node, &limited, "/v1/keys", json!({"scopes": ["services:read"], "project_ids": [pa]})).await;
    assert_eq!((r.s(), r.json["project_ids"].clone()), (201, json!([pa])), "{}", r.text);
    // Outside it, or partly outside.
    for ids in [json!([pb]), json!([pa, pb])] {
        let r = post(&node, &limited, "/v1/keys", json!({"scopes": ["services:read"], "project_ids": ids})).await;
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{ids}: {}", r.text);
    }
    // Asking for no restriction gives the creator's own, never every project.
    let r = post(&node, &limited, "/v1/keys", json!({"scopes": ["services:read"]})).await;
    assert_eq!((r.s(), r.json["project_ids"].clone()), (201, json!([pa])), "{}", r.text);
    let child = r.json["secret"].as_str().expect("secret").to_string();
    assert_eq!(get(&node, &child, &format!("/v1/services/{a}")).await.s(), 200);
    assert_eq!(get(&node, &child, &format!("/v1/services/{b}")).await.s(), 404, "the other project stays hidden");

    // An environment restriction only narrows: a staging-only key cannot mint a production key.
    let (_, staging) = make_key_in(&node, &t.id, &["services:read", "keys:write"], Some(vec![pa.clone()]), Some(vec!["staging".into()])).await;
    let r = post(&node, &staging, "/v1/keys", json!({"scopes": ["services:read"], "environments": ["production"]})).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);
    let r = post(&node, &staging, "/v1/keys", json!({"scopes": ["services:read"]})).await;
    assert_eq!((r.s(), r.json["environments"].clone()), (201, json!(["staging"])), "{}", r.text);

    // An unrestricted key may restrict a new key to any project of its tenant.
    let r = post(&node, &free, "/v1/keys", json!({"scopes": ["services:read"], "project_ids": [pb]})).await;
    assert_eq!((r.s(), r.json["project_ids"].clone()), (201, json!([pb])), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_57_a_new_key_cannot_outlive_its_creator_and_expired_keys_stop_authenticating() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;

    // An operator-made key that expires in an hour, with keys:write.
    let r = call(
        root(&node, Method::POST, &format!("/v1/tenants/{}/keys", t.id)),
        Some(json!({"scopes": ["services:read", "keys:write"], "expires_at": rfc3339_in(3_600)})),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let (expiring, expiring_expiry) = (r.json["secret"].as_str().expect("secret").to_string(), r.json["expires_at"].as_i64().expect("expiry"));

    let r = post(&node, &expiring, "/v1/keys", json!({"scopes": ["services:read"], "expires_at": rfc3339_in(7_200)})).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "later than its creator: {}", r.text);
    let r = post(&node, &expiring, "/v1/keys", json!({"scopes": ["services:read"]})).await;
    assert_eq!((r.s(), r.json["expires_at"].as_i64()), (201, Some(expiring_expiry)), "no expiry asked: the creator's own, {}", r.text);
    let r = post(&node, &expiring, "/v1/keys", json!({"scopes": ["services:read"], "expires_at": rfc3339_in(1_800)})).await;
    assert_eq!(r.s(), 201, "sooner is fine: {}", r.text);
    assert!(r.json["expires_at"].as_i64().expect("expiry") < expiring_expiry);

    // A key that expires in two seconds works now and not after.
    let (_, long) = manager(&node, &t.id, &["services:read"], None).await;
    let r = post(&node, &long, "/v1/keys", json!({"scopes": ["services:read"], "expires_at": rfc3339_in(2)})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let (short_id, short) = (r.json["id"].as_str().expect("id").to_string(), r.json["secret"].as_str().expect("secret").to_string());
    assert_eq!(get(&node, &short, "/v1/services").await.s(), 200);
    tokio::time::sleep(Duration::from_millis(3_200)).await;
    let r = get(&node, &short, "/v1/services").await;
    assert_eq!((r.s(), r.code()), (401, "unauthorized".to_string()), "expired keys stop authenticating: {}", r.text);
    // It is still listed (with its expiry) until it is revoked.
    let listed = get(&node, &long, "/v1/keys").await;
    assert!(listed.items().iter().any(|k| k["id"] == json!(short_id) && k["expires_at"].is_i64()), "{}", listed.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_58_keys_are_listed_and_revoked_within_the_callers_own_tenant_only() {
    let node = start_node().await;
    let (a, b) = (new_tenant(&node, "pro").await, new_tenant(&node, "pro").await);
    let (admin_id, admin) = manager(&node, &a.id, &["services:read", "deploy"], None).await;
    let (b_key_id, b_secret) = manager(&node, &b.id, &["services:read"], None).await;

    let c1 = post(&node, &admin, "/v1/keys", json!({"scopes": ["services:read"], "name": "one"})).await;
    let c2 = post(&node, &admin, "/v1/keys", json!({"scopes": ["deploy"], "name": "two"})).await;
    let (c1_id, c1_secret) = (c1.json["id"].as_str().expect("id").to_string(), c1.json["secret"].as_str().expect("secret").to_string());
    let c2_id = c2.json["id"].as_str().expect("id").to_string();

    // The list: this tenant's live keys (operator-made ones too), never a secret, never another tenant's.
    let list = get(&node, &admin, "/v1/keys").await;
    assert_eq!(list.s(), 200, "{}", list.text);
    let ids: Vec<String> = list.items().iter().map(|k| k["id"].as_str().unwrap().to_string()).collect();
    for id in [&admin_id, &c1_id, &c2_id, &a.key_id] {
        assert!(ids.contains(id), "{id} listed in {ids:?}");
    }
    assert!(!ids.contains(&b_key_id), "another tenant's key is not listed");
    assert!(
        list.items().iter().all(|k| k.get("secret").is_none()) && !list.text.contains(&c1_secret),
        "secrets are never listed: {}",
        list.text
    );

    // Another tenant's key id is not found, and still works afterwards.
    let r = delete(&node, &admin, &format!("/v1/keys/{b_key_id}")).await;
    assert_eq!(r.s(), 404, "{}", r.text);
    assert_eq!(get(&node, &b_secret, "/v1/services").await.s(), 200);
    assert_eq!(delete(&node, &admin, "/v1/keys/not-an-id").await.s(), 404);

    // Revoking stops the key at once; a second revoke is a 404; the list drops it.
    assert_eq!(get(&node, &c1_secret, "/v1/services").await.s(), 200);
    assert_eq!(delete(&node, &admin, &format!("/v1/keys/{c1_id}")).await.s(), 204);
    let r = get(&node, &c1_secret, "/v1/services").await;
    assert_eq!((r.s(), r.code()), (401, "unauthorized".to_string()), "{}", r.text);
    assert_eq!(delete(&node, &admin, &format!("/v1/keys/{c1_id}")).await.s(), 404);
    let ids: Vec<String> = get(&node, &admin, "/v1/keys").await.items().iter().map(|k| k["id"].as_str().unwrap().to_string()).collect();
    assert!(!ids.contains(&c1_id) && ids.contains(&c2_id));

    // A key may revoke itself.
    assert_eq!(delete(&node, &admin, &format!("/v1/keys/{admin_id}")).await.s(), 204);
    assert_eq!(get(&node, &admin, "/v1/keys").await.s(), 401);

    // The operator manages keys of a tenant through x-rt-tenant, with every scope.
    let r = call(root_for(&node, &a.id, Method::POST, "/v1/keys"), Some(json!({"scopes": ["data:admin"]}))).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let r = call(root(&node, Method::POST, "/v1/keys"), Some(json!({"scopes": ["data:admin"]}))).await;
    assert_eq!(r.s(), 422, "no tenant to act for: {}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_58_a_key_limited_to_some_projects_manages_only_keys_limited_to_those() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (pa, pb) = (create_project(&node, &t.key, &uniq("pa"), &[]).await, create_project(&node, &t.key, &uniq("pb"), &[]).await);
    let (_, limited) = manager(&node, &t.id, &["services:read"], Some(vec![pa.clone()])).await;
    let broad_key_id = make_key(&node, &t.id, &["services:read"], None).await.0;
    let (b_key_id, _) = make_key(&node, &t.id, &["services:read"], Some(vec![pb.clone()])).await;
    let (a_key_id, _) = make_key(&node, &t.id, &["services:read"], Some(vec![pa.clone()])).await;

    let ids: Vec<String> = get(&node, &limited, "/v1/keys").await.items().iter().map(|k| k["id"].as_str().unwrap().to_string()).collect();
    assert!(ids.contains(&a_key_id), "a key limited to its own project is listed: {ids:?}");
    for hidden in [&broad_key_id, &b_key_id, &t.key_id] {
        assert!(!ids.contains(hidden), "{hidden} is outside what the key manages: {ids:?}");
        assert_eq!(delete(&node, &limited, &format!("/v1/keys/{hidden}")).await.s(), 404);
    }
    assert_eq!(delete(&node, &limited, &format!("/v1/keys/{a_key_id}")).await.s(), 204);
    // The hidden keys are untouched.
    assert_eq!(get(&node, &t.key, "/v1/services").await.s(), 200);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_59_creating_and_revoking_keys_is_audited_and_names_the_key_that_did_it() {
    let rec = Arc::new(Recorder::default());
    let sink: Arc<dyn AuditSink> = rec.clone();
    let node = start_node_opts(StartOptions { extra_audit: Some(sink), ..Default::default() }, None, |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let (admin_id, admin) = manager(&node, &t.id, &["services:read", "deploy"], None).await;

    let r = post(&node, &admin, "/v1/keys", json!({"scopes": ["deploy"], "name": "ci"})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let (child_id, secret) = (r.json["id"].as_str().expect("id").to_string(), r.json["secret"].as_str().expect("secret").to_string());
    let created: Vec<AuditEvent> = rec.all("key.created").into_iter().filter(|e| matches!(&e.actor, Actor::Key { .. })).collect();
    assert_eq!(created.len(), 1, "one event for the self-service creation");
    match &created[0].actor {
        Actor::Key { key_id, tenant } => {
            assert!(key_id.contains(admin_id.trim_start_matches("key_")), "the actor is the creating key: {key_id} vs {admin_id}");
            assert_eq!(tenant.to_string(), t.id);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(created[0].tenant.map(|x| x.to_string()), Some(t.id.clone()));
    assert_eq!(created[0].data["key_id"], json!(child_id));
    assert_eq!(created[0].data["scopes"], json!(["deploy"]));
    assert_eq!(created[0].data["self_service"], json!(true));
    assert!(!serde_json::to_string(&created[0]).expect("json").contains(&secret), "the secret never reaches the audit log");

    assert_eq!(delete(&node, &admin, &format!("/v1/keys/{child_id}")).await.s(), 204);
    let deleted = rec.all("key.deleted");
    assert_eq!(deleted.len(), 1);
    assert_eq!((deleted[0].data["key_id"].clone(), deleted[0].data["self_service"].clone()), (json!(child_id), json!(true)));
    assert!(matches!(&deleted[0].actor, Actor::Key { .. }));

    // A refused request is not an event.
    let before = rec.all("key.created").len();
    assert_eq!(post(&node, &admin, "/v1/keys", json!({"scopes": ["secrets:write"]})).await.s(), 403);
    assert_eq!(rec.all("key.created").len(), before);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_60_a_tenant_may_hold_a_bounded_number_of_active_keys_through_the_token_api() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, admin) = manager(&node, &t.id, &["services:read"], None).await;
    // The tenant already holds two keys (`new_tenant` and the manager).
    let mut made = 0;
    let first_refusal = loop {
        let r = post(&node, &admin, "/v1/keys", json!({"scopes": ["services:read"]})).await;
        if r.s() != 201 {
            break r;
        }
        made += 1;
        assert!(made < 500, "the cap never applied");
    };
    assert_eq!((first_refusal.s(), first_refusal.code()), (403, "quota_exceeded".to_string()), "{}", first_refusal.text);
    assert_eq!(made, 198, "200 active keys in all");
    // Revoking one makes room.
    let list = get(&node, &admin, "/v1/keys").await;
    let victim = list.items().iter().find(|k| k["name"].is_null() && k["scopes"] == json!(["services:read"])).expect("a key").clone();
    assert_eq!(delete(&node, &admin, &format!("/v1/keys/{}", victim["id"].as_str().unwrap())).await.s(), 204);
    assert_eq!(post(&node, &admin, "/v1/keys", json!({"scopes": ["services:read"]})).await.s(), 201);
    node.shutdown().await;
}
}

node_test! {
async fn revoking_a_key_revokes_every_key_it_minted() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (root_id, root_secret) = manager(&node, &t.id, &["services:read"], None).await;
    let mk = |secret: String| {
        let node = &node;
        async move {
            let r = post(node, &secret, "/v1/keys", json!({"scopes": ["services:read", "keys:write"]})).await;
            assert_eq!(r.s(), 201, "{}", r.text);
            (r.json["id"].as_str().expect("id").to_string(), r.json["secret"].as_str().expect("secret").to_string())
        }
    };
    let (child_id, child) = mk(root_secret.clone()).await;
    let (_, grandchild) = mk(child.clone()).await;
    let (_, sibling) = mk(root_secret.clone()).await;
    let (_, unrelated) = manager(&node, &t.id, &["services:read"], None).await;

    // The listing names each key's parent.
    let list = get(&node, &root_secret, "/v1/keys").await;
    let child_row = list.items().into_iter().find(|k| k["id"] == json!(child_id)).expect("child listed");
    assert_eq!(child_row["parent_key_id"], json!(root_id), "{}", list.text);

    let (_, full) = manager(&node, &t.id, &ALL_SCOPES, None).await;
    assert_eq!(delete(&node, &full, &format!("/v1/keys/{root_id}")).await.s(), 204);
    for (who, secret) in [("root", &root_secret), ("child", &child), ("grandchild", &grandchild), ("sibling", &sibling)] {
        let r = get(&node, secret, "/v1/services").await;
        assert_eq!((r.s(), r.code()), (401, "unauthorized".to_string()), "{who}: {}", r.text);
    }
    assert_eq!(get(&node, &unrelated, "/v1/services").await.s(), 200, "keys outside the chain keep working");
    node.shutdown().await;
}
}

node_test! {
async fn a_weaker_key_cannot_revoke_a_stronger_one() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, weak) = manager(&node, &t.id, &["services:read"], None).await;
    let (strong_id, strong) = manager(&node, &t.id, &["services:read", "deploy", "secrets:write"], None).await;

    let r = delete(&node, &weak, &format!("/v1/keys/{strong_id}")).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);
    assert_eq!(get(&node, &strong, "/v1/services").await.s(), 200, "the stronger key still works");
    // A key holding every scope (and the operator) still can.
    let (_, full) = manager(&node, &t.id, &ALL_SCOPES, None).await;
    assert_eq!(delete(&node, &full, &format!("/v1/keys/{strong_id}")).await.s(), 204);
    node.shutdown().await;
}
}
