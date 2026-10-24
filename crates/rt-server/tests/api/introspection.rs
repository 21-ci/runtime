//! Introspection routes: `GET /v1/version`, `/v1/me`, `/v1/plans`, `/v1/node/config`, one host
//! (`GET …/hosts/{host}`), a tenant's own usage, and the queueing / autoscaler view (service
//! `status.admission`, `stats.admission`, `node/scheduler` `services`).

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

node_test! {
    async fn version_and_me_describe_the_caller() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;

        let v = get(&node, &t.key, "/v1/version").await;
        assert_eq!(v.s(), 200, "{}", v.text);
        assert!(v.json["version"].as_str().is_some_and(|s| !s.is_empty()), "{}", v.text);
        assert_eq!(v.json["api"], "1.0.0");
        let anon = send(node.http().get(node.control_url("/v1/version"))).await;
        assert_eq!(anon.s(), 401, "the version needs credentials: {}", anon.text);

        let me = get(&node, &t.key, "/v1/me").await;
        assert_eq!(me.s(), 200, "{}", me.text);
        assert_eq!(me.json["principal"], "tenant");
        assert_eq!(me.json["tenant_id"], json!(t.id));
        assert_eq!(me.json["key_id"], json!(t.key_id));
        assert!(me.json["scopes"].as_array().unwrap().contains(&json!("logs:read")), "{}", me.text);
        assert_eq!((me.json["project_ids"].clone(), me.json["environments"].clone()), (Value::Null, Value::Null));
        assert_eq!(me.json["plan"]["name"], "pro");
        assert_eq!(me.json["plan"]["settings"]["max_services"], 1_000, "the effective plan: {}", me.text);
        assert_eq!(me.json["version"], v.json["version"]);

        let op = call(root(&node, Method::GET, "/v1/me"), None).await;
        assert_eq!(op.json["principal"], "operator");
        assert_eq!(op.json["plan"], Value::Null);
        assert!(op.json["scopes"].as_array().unwrap().contains(&json!("data:admin")));
        let acting = call(root_for(&node, &t.id, Method::GET, "/v1/me"), None).await;
        assert_eq!(acting.json["tenant_id"], json!(t.id));
        assert_eq!(acting.json["plan"]["name"], "pro");
        node.shutdown().await;
    }
}

node_test! {
    async fn plans_and_node_config_are_operator_only() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;

        let plans = call(root(&node, Method::GET, "/v1/plans"), None).await;
        assert_eq!(plans.s(), 200, "{}", plans.text);
        let names: Vec<&str> = plans.json.as_array().unwrap().iter().filter_map(|p| p["name"].as_str()).collect();
        assert!(names.contains(&"free") && names.contains(&"pro"), "{names:?}");
        let pro = plans.json.as_array().unwrap().iter().find(|p| p["name"] == "pro").unwrap();
        assert_eq!(pro["settings"]["custom_host_suffixes"], json!([".bots.example.com"]));
        assert_eq!(get(&node, &t.key, "/v1/plans").await.code(), "insufficient_scope");

        let cfg = call(root(&node, Method::GET, "/v1/node/config"), None).await;
        assert_eq!(cfg.s(), 200, "{}", cfg.text);
        for section in ["node", "sched", "hosts", "plans", "ingress"] {
            assert!(cfg.json[section].is_object(), "section `{section}` in {}", cfg.text);
        }
        assert!(!cfg.text.contains(node.root_token()), "node secrets are never part of the config");
        assert_eq!(get(&node, &t.key, "/v1/node/config").await.code(), "insufficient_scope");
        node.shutdown().await;
    }
}

node_test! {
    async fn one_host_can_be_read_by_its_service() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let a = create_service(&node, &t.key, &uniq("hosta"), "deno").await;
        let b = create_service(&node, &t.key, &uniq("hostb"), "deno").await;

        // operator mapping: active at once; tenant claim: pending with a token and an expiry
        let host_a = format!("{}.bots.example.com", uniq("a"));
        let r = call(root_for(&node, &t.id, Method::PUT, &format!("/v1/services/{a}/hosts/{host_a}")), None).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        let host_p = format!("{}.bots.example.com", uniq("p"));
        let r = put(&node, &t.key, &format!("/v1/services/{a}/hosts/{host_p}"), json!({})).await;
        assert_eq!(r.s(), 200, "{}", r.text);

        let one = get(&node, &t.key, &format!("/v1/services/{a}/hosts/{host_a}")).await;
        assert_eq!(one.s(), 200, "{}", one.text);
        assert_eq!((one.json["host"].as_str(), one.json["status"].as_str()), (Some(host_a.as_str()), Some("active")));
        assert_eq!(one.json["expires_at"], Value::Null);

        let pending = get(&node, &t.key, &format!("/v1/services/{a}/hosts/{}.", host_p.to_uppercase())).await;
        assert_eq!(pending.s(), 200, "the host is normalised like on PUT: {}", pending.text);
        assert_eq!(pending.json["status"], "pending_verification");
        assert_eq!(pending.json["verify_token"], r.json["verify_token"]);
        assert!(pending.json["expires_at"].as_i64().is_some(), "{}", pending.text);

        assert_eq!(get(&node, &t.key, &format!("/v1/services/{b}/hosts/{host_a}")).await.s(), 404, "another service's host");
        assert_eq!(get(&node, &t.key, &format!("/v1/services/{a}/hosts/nope.bots.example.com")).await.s(), 404);
        node.shutdown().await;
    }
}

node_test! {
    async fn a_tenant_reads_its_own_usage_with_logs_read() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let other = new_tenant(&node, "pro").await;

        let own = get(&node, &t.key, &format!("/v1/tenants/{}/usage", t.id)).await;
        assert_eq!(own.s(), 200, "{}", own.text);
        assert!(own.json.is_array(), "{}", own.text);
        assert_eq!(get(&node, &t.key, &format!("/v1/tenants/{}/usage", other.id)).await.s(), 404, "another tenant's usage");

        let (_, no_logs) = make_key(&node, &t.id, &["services:read"], None).await;
        let r = get(&node, &no_logs, &format!("/v1/tenants/{}/usage", t.id)).await;
        assert_eq!((r.s(), r.code().as_str()), (403, "insufficient_scope"), "{}", r.text);
        // the rest of the tenant routes stay operator-only
        assert_eq!(get(&node, &t.key, &format!("/v1/tenants/{}", t.id)).await.code(), "insufficient_scope");
        node.shutdown().await;
    }
}

node_test! {
    async fn the_admission_view_shows_up_once_a_service_has_run() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let slug = uniq("adm");
        let svc = ship(&node, &t.key, &slug, guests::HELLO).await;

        let before = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
        assert_eq!(before.json["status"]["admission"], Value::Null, "never run: {}", before.text);

        for _ in 0..3 {
            let r = invoke(&node, &slug, "/").await;
            assert_eq!(r.s(), 200, "{}", r.text);
        }
        let view = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
        let adm = &view.json["status"]["admission"];
        assert!(adm.is_object(), "{}", view.text);
        assert!(adm["limit"].as_u64().is_some_and(|l| l >= 1), "{adm}");
        assert!(adm["max"].as_u64() >= adm["limit"].as_u64(), "{adm}");
        assert_eq!((adm["running"].as_u64(), adm["queued"].as_u64()), (Some(0), Some(0)), "{adm}");
        assert!(adm["queue_wait_ms"].as_f64().is_some() && adm["warm_per_proc"].as_u64().is_some(), "{adm}");
        // D-B2: instances, their bounds, the warm target, the arrival rate and the last move of the limit
        assert_eq!((adm["min_instances"].as_u64(), adm["instances"].as_u64()), (Some(0), Some(0)), "{adm}");
        assert_eq!(adm["max_instances"], adm["max"], "{adm}");
        assert!(adm["target_instances"].as_u64().is_some(), "{adm}");
        assert!(adm["rps"].as_f64().is_some_and(|r| r >= 0.0), "{adm}");
        assert!(adm["last_change"].is_null() || adm["last_change"]["reason"].is_string(), "{adm}");
        assert_eq!(view.json["status"]["inflight"], 0);

        let stats = get(&node, &t.key, &format!("/v1/services/{svc}/stats")).await;
        assert_eq!(stats.s(), 200, "{}", stats.text);
        assert_eq!(stats.json["admission"]["limit"], adm["limit"], "{}", stats.text);

        let sched = call(root(&node, Method::GET, "/v1/node/scheduler"), None).await;
        assert_eq!(sched.s(), 200, "{}", sched.text);
        assert!(sched.json["procs"].is_array(), "the snapshot is still there: {}", sched.text);
        assert!(sched.json["admission"]["max_inflight"].as_u64().is_some_and(|m| m > 0), "{}", sched.text);
        let mine = sched.json["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["service_id"] == json!(svc))
            .unwrap_or_else(|| panic!("{svc} in {}", sched.text));
        assert_eq!(mine["slug"], json!(slug));
        assert_eq!(mine["tenant_id"], json!(t.id));
        assert_eq!(mine["limit"], adm["limit"]);
        for field in ["min_instances", "max_instances", "instances", "target_instances", "rps"] {
            assert!(!mine[field].is_null(), "{field} in the node view: {mine}");
        }
        node.shutdown().await;
    }
}
