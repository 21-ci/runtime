//! Operator monitoring (`docs/design/platform-plan.md` R2): host status, node series, bulk stats,
//! service series, node events. Host and bulk data never reach a tenant key.

use std::time::Duration;

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

async fn op(node: &impl Node, path: &str) -> Resp {
    call(root(node, Method::GET, path), None).await
}

const HELLO: &str = r#"export default { fetch() { return new Response("hi"); } };"#;

/// Waits until the sysmon merge shows `service` in the bulk counts.
async fn bulk_row(node: &impl Node, path: &str, svc: &str) -> Value {
    for _ in 0..50 {
        let r = op(node, path).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        if let Some(row) = r.items().into_iter().find(|i| i["service"] == svc) {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{svc} never showed up in {path}");
}

node_test! {
    async fn status_reports_host_facts_without_a_host_path() {
        let node = start_node().await;
        let r = op(&node, "/v1/node/status").await;
        assert_eq!(r.s(), 200, "{}", r.text);
        let h = &r.json["host"];
        assert!(h["uptime_s"].is_u64(), "{}", r.text);
        assert!(["cgroup2", "cgroup1", "none"].contains(&h["container"].as_str().unwrap()));
        assert!(h["cores"].as_u64().unwrap() >= 1);
        assert_eq!(h["disk"]["path_label"], "data");
        assert!(h["disk"]["total_bytes"].as_u64().unwrap() > 0);
        assert!(h["services"].is_u64() && h["storage_bytes"].is_u64());
        for key in ["load", "cpu_pct", "net"] {
            assert!(h.get(key).is_some(), "`{key}` is present, null when unknown");
        }
        let dir = node.data_dir().to_str().unwrap().to_string();
        assert!(!r.text.contains(&dir), "no host path: {}", r.text);
    }
}

node_test! {
    async fn node_routes_refuse_tenant_keys_even_with_every_scope() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        for path in [
            "/v1/node/status",
            "/v1/node/series?range=1h",
            "/v1/node/stats",
            "/v1/node/stats?fields=counts",
            "/v1/node/events",
        ] {
            let r = get(&node, &t.key, path).await;
            assert_eq!((r.s(), r.code().as_str()), (403, "insufficient_scope"), "{path}: {}", r.text);
            assert!(!r.text.contains("cores"), "{path}: {}", r.text);
        }
    }
}

node_test! {
    async fn node_series_has_points_and_validates_the_range() {
        let node = start_node().await;
        let r = op(&node, "/v1/node/series?range=1h").await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert_eq!(r.json["range"], "1h");
        assert_eq!(r.json["step_s"], 60);
        let pts = r.json["points"].as_array().expect("points");
        assert!(!pts.is_empty(), "the first sample is taken at startup: {}", r.text);
        for key in ["ts", "cpu_pct", "load1", "mem_committed", "disk_free", "rx", "tx"] {
            assert!(pts[0].get(key).is_some(), "point lacks {key}: {}", r.text);
        }
        assert_eq!(op(&node, "/v1/node/series").await.json["range"], "1h", "1h is the default");
        for range in ["24h", "6h"] {
            assert_eq!(op(&node, &format!("/v1/node/series?range={range}")).await.s(), 200);
        }
        let bad = op(&node, "/v1/node/series?range=7d").await;
        assert_eq!((bad.s(), bad.code().as_str()), (422, "validation_failed"), "{}", bad.text);
    }
}

node_test! {
    async fn service_stats_add_a_series_only_when_a_step_is_asked_for() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let slug = uniq("series");
        let svc = ship(&node, &t.key, &slug, HELLO).await;
        assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

        let plain = get(&node, &t.key, &format!("/v1/services/{svc}/stats?window=1h")).await;
        assert!(plain.json.get("series").is_none(), "{}", plain.text);

        let mut point = Value::Null;
        for _ in 0..50 {
            let r = get(&node, &t.key, &format!("/v1/services/{svc}/stats?window=1h&step=1m")).await;
            assert_eq!(r.s(), 200, "{}", r.text);
            if let Some(p) = r.json["series"].as_array().and_then(|a| a.first()) {
                point = p.clone();
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(point["count"], 1, "{point}");
        for key in ["ts", "errors", "queue_p95", "init_p95", "exec_p95", "total_p95"] {
            assert!(point.get(key).is_some(), "{key}: {point}");
        }

        let base = format!("/v1/services/{svc}/stats");
        assert_eq!(get(&node, &t.key, &format!("{base}?window=24h&step=5m")).await.s(), 200);
        assert_eq!(get(&node, &t.key, &format!("{base}?window=24h&step=1h")).await.s(), 200);
        for bad in ["window=24h&step=1m", "window=1h&step=2m"] {
            let r = get(&node, &t.key, &format!("{base}?{bad}")).await;
            assert_eq!((r.s(), r.code().as_str()), (422, "validation_failed"), "{bad}: {}", r.text);
        }
        // another tenant sees no series of this service
        let other = new_tenant(&node, "pro").await;
        assert_eq!(get(&node, &other.key, &format!("{base}?window=1h&step=1m")).await.s(), 404);
    }
}

node_test! {
    async fn bulk_stats_list_active_services_with_owner_and_paginate() {
        let node = start_node().await;
        let (a, b) = (new_tenant(&node, "pro").await, new_tenant(&node, "pro").await);
        let (sa, sb) = (uniq("bulk-a"), uniq("bulk-b"));
        let ia = ship(&node, &a.key, &sa, HELLO).await;
        let ib = ship(&node, &b.key, &sb, HELLO).await;
        let quiet = ship(&node, &b.key, &uniq("bulk-quiet"), HELLO).await;
        assert_eq!(invoke(&node, &sa, "/").await.s(), 200);
        assert_eq!(invoke(&node, &sb, "/").await.s(), 200);

        let row = bulk_row(&node, "/v1/node/stats?fields=counts", &ia).await;
        assert_eq!(row["slug"], sa.as_str());
        assert_eq!(row["tenant"], a.id.as_str());
        assert_eq!((row["count"].as_u64(), row["errors"].as_u64()), (Some(1), Some(0)));
        assert!(row["p95"].is_null(), "counts merge no histogram: {row}");
        assert_eq!((row["dlq"].as_u64(), row["depth"].as_u64()), (Some(0), Some(0)));

        let full = bulk_row(&node, "/v1/node/stats?fields=all", &ib).await;
        assert!(full["p50"].as_f64().unwrap() > 0.0 && full["p99"].as_f64().unwrap() > 0.0, "{full}");
        let default = bulk_row(&node, "/v1/node/stats", &ib).await;
        assert!(default["p95"].is_f64(), "all is the default: {default}");

        let listed = op(&node, "/v1/node/stats?fields=counts&limit=200").await;
        assert!(listed.items().iter().all(|i| i["service"] != quiet.as_str()), "{}", listed.text);

        // x-rt-tenant narrows the operator's view to that tenant
        let scoped = call(root_for(&node, &a.id, Method::GET, "/v1/node/stats?fields=counts"), None).await;
        assert_eq!(scoped.s(), 200, "{}", scoped.text);
        assert!(scoped.items().iter().any(|i| i["service"] == ia.as_str()), "{}", scoped.text);
        assert!(scoped.items().iter().all(|i| i["tenant"] == a.id.as_str()), "{}", scoped.text);

        // pages walk every row exactly once
        let first = op(&node, "/v1/node/stats?fields=counts&limit=1").await;
        assert_eq!(first.items().len(), 1, "{}", first.text);
        let cursor = first.json["next_cursor"].as_str().expect("more rows").to_string();
        let second = op(&node, &format!("/v1/node/stats?fields=counts&limit=1&cursor={cursor}")).await;
        assert_ne!(first.items()[0]["service"], second.items()[0]["service"]);

        // a range outside the traffic
        let old = op(&node, "/v1/node/stats?fields=counts&from=1000&to=2000").await;
        assert!(old.items().is_empty(), "{}", old.text);
        for bad in ["from=5&to=5", "fields=some", "cursor=nope"] {
            let r = op(&node, &format!("/v1/node/stats?{bad}")).await;
            assert_eq!((r.s(), r.code().as_str()), (422, "validation_failed"), "{bad}: {}", r.text);
        }
    }
}

node_test! {
    async fn bulk_stats_show_a_service_with_dead_letters_and_no_traffic() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let svc = ship(&node, &t.key, &uniq("bulk-dlq"), HELLO).await;
        let sid = svc.parse().unwrap();
        let q = rt_store::SqliteQueueStore::open(&node.data_dir().join("queue.db"), 1).expect("queue.db");
        use rt_store::QueueStore;
        q.enqueue(sid, rt_store::NewMessage::new(bytes::Bytes::from_static(b"x"))).await.unwrap();
        let leased = q.lease(sid, 1, Duration::from_secs(30)).await.unwrap();
        q.retry(leased[0].id, Duration::ZERO).await.unwrap();
        let row = bulk_row(&node, "/v1/node/stats?fields=counts", &svc).await;
        assert_eq!(row["count"], 0);
        assert_eq!(row["dlq"], 1, "{row}");
    }
}

node_test! {
    async fn node_events_read_forward_across_services_and_filter_by_kind() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let svc = ship(&node, &t.key, &uniq("evt"), HELLO).await;
        // audit events are written in the background
        let mut all = op(&node, "/v1/node/events?limit=1000").await;
        for _ in 0..50 {
            assert_eq!(all.s(), 200, "{}", all.text);
            if all.items().len() >= 2 && all.items().iter().any(|e| e["service"] == svc.as_str()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            all = op(&node, "/v1/node/events?limit=1000").await;
        }
        let items = all.items();
        assert!(items.len() >= 2, "{}", all.text);
        let ids: Vec<&str> = items.iter().map(|e| e["id"].as_str().unwrap()).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "oldest first: {ids:?}");
        let mine = items.iter().find(|e| e["service"] == svc.as_str()).expect("event of the service");
        assert_eq!(mine["tenant"], t.id.as_str(), "{mine}");
        assert!(mine["ts"].is_i64() && mine["kind"].is_string() && mine["data"].is_object());

        // `after` resumes past an id
        let after = op(&node, &format!("/v1/node/events?after={}&limit=1000", ids[0])).await;
        assert_eq!(after.items().len(), items.len() - 1);
        // paging
        let page = op(&node, "/v1/node/events?limit=1").await;
        assert_eq!(page.items().len(), 1);
        assert_eq!(page.json["next_cursor"], json!(ids[0]));
        // kinds
        let kind = mine["kind"].as_str().unwrap();
        let only = op(&node, &format!("/v1/node/events?kinds={kind}&limit=1000")).await;
        assert!(!only.items().is_empty());
        assert!(only.items().iter().all(|e| e["kind"] == kind), "{}", only.text);
        let none = op(&node, "/v1/node/events?kinds=no.such.kind").await;
        assert!(none.items().is_empty());
        let bad = op(&node, "/v1/node/events?after=nope").await;
        assert_eq!((bad.s(), bad.code().as_str()), (422, "validation_failed"), "{}", bad.text);
        let many = vec!["k"; 33].join(",");
        let bad = op(&node, &format!("/v1/node/events?kinds={many}")).await;
        assert_eq!(bad.s(), 422, "{}", bad.text);
    }
}
