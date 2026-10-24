//! Control API tests for topics (`/v1/topics…`), consumers (`/v1/services/{s}/consumers…`) and
//! `POST /v1/pubsub/publish` (plan-final T32, Q§6.6 with the renamed routes): CRUD, plan limits,
//! scopes, tenancy, idempotency, audit, messages, consumer views and the `events` / `async`
//! service blocks.

use std::sync::{Arc, Mutex};

use reqwest::Method;
use rt_server::StartOptions;
use rt_types::TriggerKind;
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
    events: Mutex<Vec<rt_sec::AuditEvent>>,
}

impl rt_sec::AuditSink for Recorder {
    fn emit(&self, ev: rt_sec::AuditEvent) {
        self.events.lock().expect("recorder lock").push(ev);
    }
}

impl Recorder {
    fn actions(&self) -> Vec<String> {
        self.events
            .lock()
            .expect("lock")
            .iter()
            .map(|e| e.action.clone())
            .collect()
    }
}

/// A plan with small stream limits (`tiny`) next to `pro`.
async fn node_with_tiny(audit: Option<Arc<Recorder>>) -> rt_testkit::NodeHarness {
    let extra_audit = audit.map(|r| r as Arc<dyn rt_sec::AuditSink>);
    start_node_opts(
        StartOptions {
            extra_audit,
            ..Default::default()
        },
        None,
        |c| {
            let mut tiny = pro_plan();
            tiny.max_topics = 2;
            tiny.max_topic_partitions = 4;
            tiny.max_stream_retention_hours = 48;
            tiny.max_stream_mb = 10;
            c.plans.insert("tiny".into(), tiny);
            let mut nostream = pro_plan();
            nostream
                .allowed_triggers
                .retain(|k| !matches!(k, TriggerKind::Stream | TriggerKind::Pubsub));
            c.plans.insert("nostream".into(), nostream);
        },
    )
    .await
}

fn names(r: &Resp) -> Vec<String> {
    r.items()
        .iter()
        .map(|t| t["name"].as_str().unwrap_or("").to_string())
        .collect()
}

node_test! {
/// Topics are created, listed, read by name or id, patched and deleted; every write is audited
/// as `topic.*`; a second topic with the same name in the namespace is `409 topic_exists`.
async fn ac_topics_api_01_crud_happy_path_with_audit() {
    let rec = Arc::new(Recorder::default());
    let node = node_with_tiny(Some(rec.clone())).await;
    let t = new_tenant(&node, "pro").await;

    let r = post(&node, &t.key, "/v1/topics", json!({"name": "orders", "partitions": 2, "retention_hours": 12, "max_mb": 5})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    assert!(id.starts_with("tpc_"), "{id}");
    assert_eq!(
        (r.json["name"].clone(), r.json["project"].clone(), r.json["environment"].clone()),
        (json!("orders"), json!("default"), json!("production"))
    );
    assert_eq!((r.json["partitions"].clone(), r.json["retention_hours"].clone(), r.json["max_mb"].clone()), (json!(2), json!(12), json!(5)));
    assert_eq!(r.json["heads"].as_array().map(Vec::len), Some(2));
    assert!(r.json.get("consumers").is_none(), "consumers only on GET one");

    // Defaults.
    let d = post(&node, &t.key, "/v1/topics", json!({"name": "audit.log"})).await;
    assert_eq!(d.s(), 201, "{}", d.text);
    assert_eq!((d.json["partitions"].clone(), d.json["retention_hours"].clone(), d.json["max_mb"].clone()), (json!(1), json!(24), json!(0)));

    let dup = post(&node, &t.key, "/v1/topics", json!({"name": "orders"})).await;
    assert_eq!((dup.s(), dup.code()), (409, "topic_exists".to_string()), "{}", dup.text);

    let by_name = get(&node, &t.key, "/v1/topics/orders").await;
    let by_id = get(&node, &t.key, &format!("/v1/topics/{id}")).await;
    assert_eq!((by_name.s(), by_id.s()), (200, 200), "{} / {}", by_name.text, by_id.text);
    assert_eq!(by_name.json, by_id.json);
    assert_eq!(by_name.json["consumers"], json!([]));

    let list = get(&node, &t.key, "/v1/topics").await;
    assert_eq!(list.s(), 200, "{}", list.text);
    let mut got = names(&list);
    got.sort();
    assert_eq!(got, ["audit.log", "orders"]);
    let none = get(&node, &t.key, "/v1/topics?project=nope").await;
    assert_eq!((none.s(), names(&none).len()), (200, 0), "{}", none.text);

    let p = patch(&node, &t.key, "/v1/topics/orders", json!({"retention_hours": 6, "max_mb": 1})).await;
    assert_eq!((p.s(), p.json["retention_hours"].clone(), p.json["max_mb"].clone()), (200, json!(6), json!(1)), "{}", p.text);
    assert_eq!(p.json["partitions"], json!(2), "partitions do not change");
    let bad = patch(&node, &t.key, "/v1/topics/orders", json!({"partitions": 3})).await;
    assert_eq!(bad.s(), 422, "unknown patch field: {}", bad.text);

    assert_eq!(delete(&node, &t.key, &format!("/v1/topics/{id}")).await.s(), 204);
    assert_eq!(get(&node, &t.key, "/v1/topics/orders").await.s(), 404);
    assert_eq!(delete(&node, &t.key, "/v1/topics/orders").await.s(), 404);

    let acts = rec.actions();
    for a in ["topic.created", "topic.updated", "topic.deleted"] {
        assert!(acts.iter().any(|x| x == a), "{a} audited: {acts:?}");
    }
    node.shutdown().await;
}
}

node_test! {
/// A topic name that starts with the id prefix `tpc_` is `422 validation_failed` (so `{t}` is
/// never ambiguous), as is any name outside the grammar.
async fn topic_name_with_the_id_prefix_is_validation_failed() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    for name in ["tpc_orders", "tpc_", "Orders", "", "-x", &"a".repeat(64), "a b"] {
        let r = post(&node, &t.key, "/v1/topics", json!({"name": name})).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{name}: {}", r.text);
    }
    assert_eq!(names(&get(&node, &t.key, "/v1/topics").await).len(), 0);
    // A malformed `tpc_` id is a plain 404.
    assert_eq!(get(&node, &t.key, "/v1/topics/tpc_nope").await.s(), 404);
    node.shutdown().await;
}
}

node_test! {
/// Partitions, retention and `max_mb` over the plan are `422 limit_exceeds_plan` (on create and
/// patch); the plan's `max_topics` is `403 quota_exceeded`; out-of-range values are
/// `validation_failed`.
async fn ac_topics_api_02_plan_limits_and_topic_quota() {
    let node = node_with_tiny(None).await;
    let t = new_tenant(&node, "tiny").await;
    for (body, field) in [
        (json!({"name": "a", "partitions": 5}), "partitions"),
        (json!({"name": "a", "retention_hours": 49}), "retention_hours"),
        (json!({"name": "a", "max_mb": 11}), "max_mb"),
    ] {
        let r = post(&node, &t.key, "/v1/topics", body.clone()).await;
        assert_eq!((r.s(), r.code()), (422, "limit_exceeds_plan".to_string()), "{body}: {}", r.text);
        assert_eq!(r.json["error"]["details"]["field"], json!(field), "{}", r.text);
    }
    for body in [json!({"name": "a", "partitions": 0}), json!({"name": "a", "retention_hours": 0})] {
        let r = post(&node, &t.key, "/v1/topics", body.clone()).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{body}: {}", r.text);
    }
    assert_eq!(post(&node, &t.key, "/v1/topics", json!({"name": "a", "partitions": 4, "retention_hours": 48, "max_mb": 10})).await.s(), 201);
    assert_eq!(post(&node, &t.key, "/v1/topics", json!({"name": "b"})).await.s(), 201);
    let r = post(&node, &t.key, "/v1/topics", json!({"name": "c"})).await;
    assert_eq!((r.s(), r.code()), (403, "quota_exceeded".to_string()), "{}", r.text);

    let r = patch(&node, &t.key, "/v1/topics/a", json!({"retention_hours": 100})).await;
    assert_eq!((r.s(), r.code()), (422, "limit_exceeds_plan".to_string()), "{}", r.text);
    let r = patch(&node, &t.key, "/v1/topics/a", json!({"max_mb": 11})).await;
    assert_eq!((r.s(), r.code()), (422, "limit_exceeds_plan".to_string()), "{}", r.text);

    // Deleting one frees a slot.
    assert_eq!(delete(&node, &t.key, "/v1/topics/b").await.s(), 204);
    assert_eq!(post(&node, &t.key, "/v1/topics", json!({"name": "c"})).await.s(), 201);
    node.shutdown().await;
}
}

node_test! {
/// Each route checks its scope: reads need `services:read`, topic writes `services:write`,
/// appends, resets, dead-letter purges and publishes `queue:write`.
async fn ac_topics_api_03_scope_denials_for_each_route() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    assert_eq!(post(&node, &t.key, "/v1/topics", json!({"name": "s"})).await.s(), 201);
    let svc = create_service(&node, &t.key, &uniq("cons"), "deno").await;
    let (_, invoke_only) = make_key(&node, &t.id, &["invoke"], None).await;
    let (_, read_only) = make_key(&node, &t.id, &["services:read"], None).await;

    let reads: Vec<(Method, String, Option<Value>)> = vec![
        (Method::GET, "/v1/topics".into(), None),
        (Method::GET, "/v1/topics/s".into(), None),
        (Method::GET, "/v1/topics/s/messages".into(), None),
        (Method::GET, format!("/v1/services/{svc}/consumers"), None),
        (Method::GET, format!("/v1/services/{svc}/consumers/s/default/dlq"), None),
    ];
    let writes: Vec<(Method, String, Option<Value>)> = vec![
        (Method::POST, "/v1/topics".into(), Some(json!({"name": "x"}))),
        (Method::PATCH, "/v1/topics/s".into(), Some(json!({"retention_hours": 2}))),
        (Method::DELETE, "/v1/topics/s".into(), None),
        (Method::POST, "/v1/topics/s/messages".into(), Some(json!({"records": [{"body": "x"}]}))),
        (Method::POST, format!("/v1/services/{svc}/consumers/s/default/reset"), Some(json!({"to": "earliest"}))),
        (Method::DELETE, format!("/v1/services/{svc}/consumers/s/default/dlq"), None),
        (Method::POST, "/v1/pubsub/publish".into(), Some(json!({"subject": "a.b", "body": {}}))),
    ];
    for (m, path, body) in reads.iter().chain(writes.iter()) {
        let r = call(api(&node, &invoke_only, m.clone(), path), body.clone()).await;
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{m} {path}: {}", r.text);
    }
    for (m, path, body) in &reads {
        let r = call(api(&node, &read_only, m.clone(), path), body.clone()).await;
        assert_eq!(r.s(), 200, "{m} {path} with services:read: {}", r.text);
    }
    for (m, path, body) in &writes {
        let r = call(api(&node, &read_only, m.clone(), path), body.clone()).await;
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{m} {path}: {}", r.text);
    }
    // `services:write` alone does not append; `queue:write` alone does not create.
    let (_, sw) = make_key(&node, &t.id, &["services:write"], None).await;
    let (_, qw) = make_key(&node, &t.id, &["queue:write"], None).await;
    assert_eq!(post(&node, &sw, "/v1/topics/s/messages", json!({"records": [{"body": "x"}]})).await.code(), "insufficient_scope");
    assert_eq!(post(&node, &qw, "/v1/topics", json!({"name": "y"})).await.code(), "insufficient_scope");
    assert_eq!(post(&node, &qw, "/v1/topics/s/messages", json!({"records": [{"body": "x"}]})).await.s(), 202);
    node.shutdown().await;
}
}

node_test! {
/// Another tenant's topics and consumers are `404` by name and by id; a key restricted to other
/// projects or environments sees `404` too (and cannot create there); the operator acting as the
/// system tenant may not write (`403 reserved`).
async fn ac_topics_api_04_tenancy_restricted_keys_and_the_system_tenant() {
    let node = start_node().await;
    let a = new_tenant(&node, "pro").await;
    let b = new_tenant(&node, "pro").await;
    let r = post(&node, &a.key, "/v1/topics", json!({"name": "secret"})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    let svc = create_service(&node, &a.key, &uniq("own"), "deno").await;

    for path in ["/v1/topics/secret".to_string(), format!("/v1/topics/{id}"), format!("/v1/topics/{id}/messages")] {
        assert_eq!(get(&node, &b.key, &path).await.s(), 404, "{path}");
    }
    assert_eq!(patch(&node, &b.key, &format!("/v1/topics/{id}"), json!({"max_mb": 1})).await.s(), 404);
    assert_eq!(delete(&node, &b.key, &format!("/v1/topics/{id}")).await.s(), 404);
    assert_eq!(post(&node, &b.key, &format!("/v1/topics/{id}/messages"), json!({"records": [{"body": "x"}]})).await.s(), 404);
    assert_eq!(get(&node, &b.key, &format!("/v1/services/{svc}/consumers")).await.s(), 404);
    assert!(names(&get(&node, &b.key, "/v1/topics").await).is_empty());
    // The name is free in the other tenant.
    assert_eq!(post(&node, &b.key, "/v1/topics", json!({"name": "secret"})).await.s(), 201);

    // A key restricted to another project, and one restricted to another environment.
    let other = create_project(&node, &a.key, "other", &["production", "staging"]).await;
    let (_, in_other) = make_key(&node, &a.id, &ALL_SCOPES, Some(vec![other.clone()])).await;
    assert_eq!(get(&node, &in_other, "/v1/topics/secret").await.s(), 404);
    assert_eq!(get(&node, &in_other, &format!("/v1/topics/{id}")).await.s(), 404);
    assert!(names(&get(&node, &in_other, "/v1/topics").await).is_empty());
    let r = post(&node, &in_other, "/v1/topics", json!({"name": "mine"})).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "default project: {}", r.text);
    let r = post(&node, &in_other, "/v1/topics", json!({"name": "mine", "project": "other"})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let (_, staging) = make_key_in(&node, &a.id, &ALL_SCOPES, None, Some(vec!["staging".into()])).await;
    assert_eq!(get(&node, &staging, "/v1/topics/mine?project=other").await.s(), 404);
    let r = post(&node, &staging, "/v1/pubsub/publish", json!({"subject": "a", "body": 1, "project": "other"})).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);
    assert_eq!(get(&node, &a.key, "/v1/topics/mine?project=other").await.s(), 200);

    // The operator: for a tenant it may act; as the system tenant it may only read.
    let r = call(root_for(&node, &a.id, Method::GET, "/v1/topics/secret"), None).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let sys = rt_types::system_tenant().to_string();
    let r = call(root_for(&node, &sys, Method::POST, "/v1/topics"), Some(json!({"name": "x"}))).await;
    assert_eq!((r.s(), r.code()), (403, "reserved".to_string()), "{}", r.text);
    let r = call(root_for(&node, &sys, Method::POST, "/v1/pubsub/publish"), Some(json!({"subject": "a", "body": 1}))).await;
    assert_eq!((r.s(), r.code()), (403, "reserved".to_string()), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// The same `Idempotency-Key` with the same create replays the first answer (one topic); with a
/// different body it is `422 idempotency_mismatch` (control API). A project with topics cannot be deleted.
async fn ac_topics_api_05_idempotent_create_and_project_delete_refused() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let create = |name: &'static str| {
        call(
            api(&node, &t.key, Method::POST, "/v1/topics").header("idempotency-key", "k-1"),
            Some(json!({"name": name})),
        )
    };
    let first = create("once").await;
    let again = create("once").await;
    assert_eq!((first.s(), again.s()), (201, 201), "{} / {}", first.text, again.text);
    assert_eq!(first.json["id"], again.json["id"]);
    assert_eq!(names(&get(&node, &t.key, "/v1/topics").await), ["once"]);
    let other = create("twice").await;
    assert_eq!((other.s(), other.code()), (422, "idempotency_mismatch".to_string()), "{}", other.text);

    create_project(&node, &t.key, "jobs", &[]).await;
    assert_eq!(post(&node, &t.key, "/v1/topics", json!({"name": "q", "project": "jobs"})).await.s(), 201);
    let r = delete(&node, &t.key, "/v1/projects/jobs").await;
    assert_eq!((r.s(), r.code()), (409, "project_not_empty".to_string()), "{}", r.text);
    assert_eq!(delete(&node, &t.key, "/v1/topics/q?project=jobs").await.s(), 204);
    assert_eq!(delete(&node, &t.key, "/v1/projects/jobs").await.s(), 204);
    node.shutdown().await;
}
}

node_test! {
/// Appends land per key in one partition and read back in order; `partition` is required on a
/// multi-partition topic; `dedup_id` returns the original offset; bad batches are refused.
async fn ac_topics_api_06_append_and_read() {
    let rec = Arc::new(Recorder::default());
    let node = node_with_tiny(Some(rec.clone())).await;
    let t = new_tenant(&node, "pro").await;
    assert_eq!(post(&node, &t.key, "/v1/topics", json!({"name": "one"})).await.s(), 201);
    assert_eq!(post(&node, &t.key, "/v1/topics", json!({"name": "two", "partitions": 2})).await.s(), 201);

    let r = post(&node, &t.key, "/v1/topics/one/messages", json!({"records": [
        {"body": "plain"}, {"body": {"n": 1}, "key": "k", "headers": {"h": "v"}}, {"body": "d", "dedup_id": "d1"}
    ]})).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let offs: Vec<u64> = r.items().iter().map(|x| x["offset"].as_u64().unwrap()).collect();
    assert_eq!(offs, [0, 1, 2]);
    let dup = post(&node, &t.key, "/v1/topics/one/messages", json!({"records": [{"body": "d", "dedup_id": "d1"}]})).await;
    assert_eq!((dup.items()[0]["offset"].clone(), dup.items()[0]["duplicate"].clone()), (json!(2), json!(true)), "{}", dup.text);

    let read = get(&node, &t.key, "/v1/topics/one/messages").await;
    assert_eq!(read.s(), 200, "{}", read.text);
    let items = read.items();
    assert_eq!(items.len(), 3);
    assert_eq!((items[0]["body"].clone(), items[1]["body"].clone()), (json!("plain"), json!({"n": 1})));
    assert_eq!((items[1]["key"].clone(), items[1]["headers"]["h"].clone()), (json!("k"), json!("v")));
    assert_eq!((read.json["next"].clone(), read.json["end"].clone()), (json!(3), json!(true)));
    let page = get(&node, &t.key, "/v1/topics/one/messages?from=1&limit=1").await;
    assert_eq!((page.items().len(), page.json["next"].clone(), page.json["end"].clone()), (1, json!(2), json!(false)));
    let latest = get(&node, &t.key, "/v1/topics/one/messages?from=latest").await;
    assert_eq!((latest.items().len(), latest.json["next"].clone()), (0, json!(3)));
    let bad = get(&node, &t.key, "/v1/topics/one/messages?from=1&from_time=2026-01-01T00:00:00Z").await;
    assert_eq!(bad.s(), 422, "{}", bad.text);

    // Multi-partition: `partition` is required and range-checked.
    let r = get(&node, &t.key, "/v1/topics/two/messages").await;
    assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{}", r.text);
    assert!(r.text.contains("pass `partition`"), "{}", r.text);
    assert_eq!(get(&node, &t.key, "/v1/topics/two/messages?partition=2").await.s(), 422);
    let r = post(&node, &t.key, "/v1/topics/two/messages", json!({"records": [{"body": "a", "key": "same"}, {"body": "b", "key": "same"}]})).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let p = r.items()[0]["partition"].as_u64().unwrap();
    assert_eq!(r.items()[1]["partition"].as_u64(), Some(p), "same key, same partition");
    let r = get(&node, &t.key, &format!("/v1/topics/two/messages?partition={p}")).await;
    assert_eq!(r.items().iter().map(|x| x["body"].clone()).collect::<Vec<_>>(), [json!("a"), json!("b")]);

    // Batch rules: empty, over 100, reserved header.
    for body in [
        json!({"records": []}),
        json!({"records": (0..101).map(|_| json!({"body": "x"})).collect::<Vec<_>>()}),
        json!({"records": [{"body": "x", "headers": {"x-rt-evil": "1"}}]}),
    ] {
        let r = post(&node, &t.key, "/v1/topics/one/messages", body).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{}", r.text);
    }
    assert!(rec.actions().iter().any(|a| a == "topic.appended"));
    node.shutdown().await;
}
}

node_test! {
/// The consumers of a service: declared ones show up (no cursors before a reset); a reset
/// creates cursors at the chosen position; dead letters list empty and purge; bad `to`, group or
/// partition is `422`; an unknown topic is `404`.
async fn ac_topics_api_07_consumers_view_reset_and_dlq() {
    let rec = Arc::new(Recorder::default());
    let node = node_with_tiny(Some(rec.clone())).await;
    let t = new_tenant(&node, "pro").await;
    assert_eq!(post(&node, &t.key, "/v1/topics", json!({"name": "ev", "partitions": 2})).await.s(), 201);
    let r = post(&node, &t.key, "/v1/topics/ev/messages", json!({"records": [{"body": "a", "key": "x"}, {"body": "b", "key": "x"}]})).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let p = r.items()[0]["partition"].as_u64().unwrap();
    let r = post(&node, &t.key, "/v1/services", json!({
        "slug": uniq("cons"), "runtime": "deno",
        "events": {"consume": [{"topic": "ev", "group": "g1", "start": "latest"}, {"topic": "missing"}]}
    })).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let svc = r.json["id"].as_str().unwrap().to_string();

    let list = get(&node, &t.key, &format!("/v1/services/{svc}/consumers")).await;
    assert_eq!(list.s(), 200, "{}", list.text);
    let items = list.items();
    assert_eq!(items.len(), 2, "{}", list.text);
    assert_eq!((items[0]["topic"].clone(), items[0]["group"].clone(), items[0]["declared"].clone(), items[0]["topic_exists"].clone()), (json!("ev"), json!("g1"), json!(true), json!(true)));
    assert_eq!((items[1]["topic"].clone(), items[1]["group"].clone(), items[1]["topic_exists"].clone()), (json!("missing"), json!("default"), json!(false)));

    let r = post(&node, &t.key, &format!("/v1/services/{svc}/consumers/ev/g1/reset"), json!({"to": "earliest"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let parts = r.json["partitions"].as_array().cloned().unwrap_or_default();
    assert_eq!(parts.len(), 2, "{}", r.text);
    let mine = parts.iter().find(|x| x["partition"].as_u64() == Some(p)).expect("partition");
    assert_eq!((mine["committed"].clone(), mine["next"].clone(), mine["lag"].clone(), mine["state"].clone()), (json!(0), json!(2), json!(2), json!("active")), "{}", r.text);
    let r = post(&node, &t.key, &format!("/v1/services/{svc}/consumers/ev/g1/reset"), json!({"to": 1, "partition": p})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let mine = r.json["partitions"].as_array().unwrap().iter().find(|x| x["partition"].as_u64() == Some(p)).cloned().unwrap();
    assert_eq!(mine["committed"], json!(1), "{}", r.text);
    // The topic view reports the group's lag.
    let topic = get(&node, &t.key, "/v1/topics/ev").await;
    assert_eq!(topic.json["consumers"].as_array().map(Vec::len), Some(1), "{}", topic.text);
    assert_eq!(topic.json["consumers"][0]["group"], json!("g1"));

    for (body, what) in [
        (json!({"to": "soon"}), "bad to"),
        (json!({"to": true}), "bad to type"),
        (json!({"to": "earliest", "partition": 2}), "partition out of range"),
    ] {
        let r = post(&node, &t.key, &format!("/v1/services/{svc}/consumers/ev/g1/reset"), body).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{what}: {}", r.text);
    }
    let r = post(&node, &t.key, &format!("/v1/services/{svc}/consumers/ev/BAD%20G/reset"), json!({"to": "earliest"})).await;
    assert_eq!(r.s(), 422, "bad group: {}", r.text);
    assert_eq!(post(&node, &t.key, &format!("/v1/services/{svc}/consumers/missing/default/reset"), json!({"to": "earliest"})).await.s(), 404);

    let dlq = get(&node, &t.key, &format!("/v1/services/{svc}/consumers/ev/g1/dlq?limit=5")).await;
    assert_eq!((dlq.s(), dlq.items().len()), (200, 0), "{}", dlq.text);
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{svc}/consumers/ev/g1/dlq")).await.s(), 204);
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}/consumers/missing/default/dlq")).await.s(), 404);
    let acts = rec.actions();
    assert!(acts.iter().any(|a| a == "consumer.reset") && acts.iter().any(|a| a == "consumer.dlq_purged"), "{acts:?}");
    node.shutdown().await;
}
}

node_test! {
/// `POST /v1/pubsub/publish` with no subscriber answers `{receivers: 0, dropped: 0}`; bad
/// subjects, reserved headers and an unknown project are refused.
async fn ac_topics_api_08_publish_validation() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let r = post(&node, &t.key, "/v1/pubsub/publish", json!({"subject": "orders.created", "body": {"id": 1}})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json, json!({"receivers": 0, "dropped": 0}));
    for body in [
        json!({"subject": "", "body": 1}),
        json!({"subject": "a.*", "body": 1}),
        json!({"subject": "a", "body": 1, "headers": {"x-rt-trace": "1"}}),
        json!({"subject": "a", "body": 1, "environment": "Bad Env"}),
        json!({"subject": "a", "body": 1, "extra": true}),
    ] {
        let r = post(&node, &t.key, "/v1/pubsub/publish", body.clone()).await;
        assert_eq!(r.s(), 422, "{body}: {}", r.text);
    }
    let r = post(&node, &t.key, "/v1/pubsub/publish", json!({"subject": "a", "body": 1, "project": "nope"})).await;
    assert!(r.s() == 404 || r.s() == 422, "{}", r.text);
    let r = post(&node, &t.key, "/v1/pubsub/publish", json!({"subject": "a", "body": "x".repeat(1024 * 1024 + 1)})).await;
    assert_eq!((r.s(), r.code()), (413, "too_large".to_string()), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// The `events` and `async` service blocks are validated on create and patch: each bound,
/// durable services may not subscribe or consume, and a plan without the `pubsub` / `stream`
/// trigger refuses the block (`403 trigger_not_allowed`).
async fn ac_topics_api_09_events_and_async_blocks_are_validated() {
    let node = node_with_tiny(None).await;
    let t = new_tenant(&node, "pro").await;
    let ok = post(&node, &t.key, "/v1/services", json!({
        "slug": uniq("ev"), "runtime": "deno",
        "events": {"subscribe": [{"subject": "orders.*"}], "consume": [{"topic": "t1", "group": "g", "batch_size": 10, "max_attempts": 3}]},
        "async": {"enabled": true, "max_attempts": 2, "max_event_age_s": 120}
    })).await;
    assert_eq!(ok.s(), 201, "{}", ok.text);
    let id = ok.json["id"].as_str().unwrap().to_string();
    let got = get(&node, &t.key, &format!("/v1/services/{id}")).await;
    let cfg = if got.json["config"].is_object() { got.json["config"].clone() } else { got.json.clone() };
    assert_eq!(cfg["async"]["max_attempts"], json!(2), "{}", got.text);
    assert_eq!(cfg["events"]["subscribe"][0]["subject"], json!("orders.*"), "{}", got.text);

    let bad_blocks = [
        json!({"async": {"max_attempts": 0}}),
        json!({"async": {"max_attempts": 11}}),
        json!({"async": {"max_event_age_s": 59}}),
        json!({"async": {"max_event_age_s": 86_401}}),
        json!({"async": {"nope": 1}}),
        json!({"events": {"subscribe": [{"subject": ""}]}}),
        json!({"events": {"consume": [{"topic": "Bad Topic"}]}}),
        json!({"events": {"consume": [{"topic": "t", "batch_size": 0}]}}),
        json!({"events": {"consume": [{"topic": "t", "max_attempts": 0}]}}),
        json!({"durable": true, "events": {"subscribe": [{"subject": "a"}]}}),
    ];
    for block in &bad_blocks {
        let mut body = json!({"slug": uniq("bad"), "runtime": "deno"});
        for (k, v) in block.as_object().unwrap() {
            body[k] = v.clone();
        }
        let r = post(&node, &t.key, "/v1/services", body).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "create {block}: {}", r.text);
        let r = patch(&node, &t.key, &format!("/v1/services/{id}"), block.clone()).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "patch {block}: {}", r.text);
    }

    // Durable: the existing block refuses turning the service durable.
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"durable": true})).await;
    assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{}", r.text);

    // A plan without the triggers.
    let n = new_tenant(&node, "nostream").await;
    for block in [json!({"subscribe": [{"subject": "a"}]}), json!({"consume": [{"topic": "t"}]})] {
        let r = post(&node, &n.key, "/v1/services", json!({"slug": uniq("ns"), "runtime": "deno", "events": block.clone()})).await;
        assert_eq!((r.s(), r.code()), (403, "trigger_not_allowed".to_string()), "{block}: {}", r.text);
    }
    let plain = create_service(&node, &n.key, &uniq("plain"), "deno").await;
    let r = patch(&node, &n.key, &format!("/v1/services/{plain}"), json!({"events": {"consume": [{"topic": "t"}]}})).await;
    assert_eq!((r.s(), r.code()), (403, "trigger_not_allowed".to_string()), "{}", r.text);
    node.shutdown().await;
}
}
