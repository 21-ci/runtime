//! `std:stream` topics through the real node (Q§6.6, plan-final §2.5): topics created with
//! `/v1/topics`, records appended by the API or a guest, consumers declared in `events.consume`
//! and fed by the stream engine (order per partition, `failAt`, retries, DLQ / block, reset),
//! persistence across restarts and the plan's `stream` trigger switch.

use std::time::Duration;

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

/// Consumer: `stream(batch)` records `(partition, offset, key, body, attempt)` per message, and
/// calls `failAt` on a message whose body has `poison: true` (recorded too). `GET /got` lists
/// them; `GET /append?topic=&key=&b=` appends `{tag: b}` with `std:stream`.
const CONSUMER: &str = r#"
import { sql } from "std:sql";
import { append } from "std:stream";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS got (id INTEGER PRIMARY KEY AUTOINCREMENT, partition INTEGER, off INTEGER, k TEXT, body TEXT, attempt INTEGER)");
}
export default {
  async fetch(req: Request): Promise<Response> {
    await ensure();
    const u = new URL(req.url);
    if (u.pathname === "/append") {
      try {
        return Response.json(await append(u.searchParams.get("topic") ?? "", { tag: u.searchParams.get("b") ?? "" }, { key: u.searchParams.get("key") ?? undefined }));
      } catch (e) { return Response.json({ error: (e as any)?.code ?? String(e) }); }
    }
    const r = await sql.execute("SELECT partition, off, k, body, attempt FROM got ORDER BY id");
    return Response.json({ got: r.rows.map((x: any) => ({ partition: x[0], offset: x[1], key: x[2], body: JSON.parse(x[3]), attempt: x[4] })) });
  },
  async stream(batch: any, _ctx: any): Promise<void> {
    await ensure();
    for (const m of batch.messages) {
      await sql.execute("INSERT INTO got (partition, off, k, body, attempt) VALUES (?, ?, ?, ?, ?)", [m.partition, m.offset, m.key, JSON.stringify(m.body), batch.attempt]);
      if (m.body && m.body.poison) { batch.failAt(m); return; }
    }
  },
};"#;

async fn create_topic(node: &impl Node, key: &str, name: &str, partitions: u32) -> String {
    let r = post(node, key, "/v1/topics", json!({"name": name, "partitions": partitions})).await;
    assert_eq!(r.s(), 201, "create topic `{name}`: {}", r.text);
    r.json["id"].as_str().expect("topic id").to_string()
}

async fn append(node: &impl Node, key: &str, topic: &str, records: Value) -> Vec<Value> {
    let r = post(
        node,
        key,
        &format!("/v1/topics/{topic}/messages"),
        json!({"records": records}),
    )
    .await;
    assert_eq!(r.s(), 202, "append: {}", r.text);
    r.items()
}

async fn consume(node: &impl Node, key: &str, svc: &str, consumers: Value) -> Resp {
    patch(
        node,
        key,
        &format!("/v1/services/{svc}"),
        json!({"events": {"consume": consumers}}),
    )
    .await
}

/// A CONSUMER service on `topic` (group `default`, from `earliest`, `extra` fields), built.
async fn consumer(node: &impl Node, key: &str, slug: &str, topic: &str, extra: Value) -> String {
    let svc = ship(node, key, slug, CONSUMER).await;
    let mut c = json!({"topic": topic, "group": "default", "start": "earliest"});
    if let (Some(b), Some(e)) = (c.as_object_mut(), extra.as_object()) {
        b.extend(e.clone());
    }
    let r = consume(node, key, &svc, json!([c])).await;
    assert_eq!(r.s(), 200, "consume: {}", r.text);
    assert_eq!(invoke(node, slug, "/got").await.s(), 200, "build `{slug}`");
    svc
}

async fn got(node: &impl Node, slug: &str) -> Vec<Value> {
    invoke(node, slug, "/got").await.json["got"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn wait_got(node: &impl Node, slug: &str, want: usize, timeout: Duration) {
    let what = format!("{want} deliveries to `{slug}`");
    eventually(timeout, &what, || async move { got(node, slug).await.len() >= want }).await;
}

async fn consumer_view(node: &impl Node, key: &str, svc: &str) -> Value {
    let r = get(node, key, &format!("/v1/services/{svc}/consumers")).await;
    assert_eq!(r.s(), 200, "consumers: {}", r.text);
    r.items().into_iter().next().unwrap_or(Value::Null)
}

fn tags(g: &[Value]) -> Vec<String> {
    g.iter()
        .map(|x| x["body"]["tag"].as_str().unwrap_or("").to_string())
        .collect()
}

async fn reset(node: &impl Node, key: &str, svc: &str, topic: &str, to: Value) -> Resp {
    post(
        node,
        key,
        &format!("/v1/services/{svc}/consumers/{topic}/default/reset"),
        json!({"to": to}),
    )
    .await
}

node_test! {
/// AC-STM-E2E-01: records with the same key land in the same partition and are consumed in
/// append order; every record is delivered once and the consumer ends with lag 0.
async fn ac_stm_e2e_01_append_consume_in_order_per_key() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    create_topic(&node, &t.key, "orders", 2).await;
    let slug = uniq("stmorder");
    let svc = consumer(&node, &t.key, &slug, "orders", json!({"batch_size": 3})).await;

    let mut records = Vec::new();
    for i in 0..10 {
        for k in ["alice", "bob"] {
            records.push(json!({"key": k, "body": {"tag": format!("{k}-{i}")}}));
        }
    }
    let placed = append(&node, &t.key, "orders", json!(records)).await;
    assert_eq!(placed.len(), 20);
    let part = |k: usize| placed[k]["partition"].clone();
    assert!((0..10).all(|i| part(2 * i) == part(0) && part(2 * i + 1) == part(1)), "a key keeps its partition: {placed:?}");

    wait_got(&node, &slug, 20, Duration::from_secs(20)).await;
    let g = got(&node, &slug).await;
    assert_eq!(g.len(), 20, "each record once: {g:?}");
    for k in ["alice", "bob"] {
        let mine: Vec<_> = g.iter().filter(|x| x["key"] == json!(k)).cloned().collect();
        let want: Vec<String> = (0..10).map(|i| format!("{k}-{i}")).collect();
        assert_eq!(tags(&mine), want, "per-key order for `{k}`");
    }
    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(10), "lag 0", || async move {
        let v = consumer_view(n, k, s).await;
        v["partitions"].as_array().is_some_and(|ps| ps.iter().all(|p| p["lag"] == json!(0)))
    })
    .await;
    node.shutdown().await;
}
}

node_test! {
/// AC-STM-E2E-02: `failAt` commits the records before the poison one and retries it alone; after
/// `max_attempts` deliveries it is dead-lettered (visible in `GET …/dlq` and the consumer view)
/// and the records after it are consumed; the DLQ can be purged.
async fn ac_stm_e2e_02_fail_at_then_dlq_after_max_attempts_visible_in_api() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    create_topic(&node, &t.key, "jobs", 1).await;
    let slug = uniq("stmdlq");
    let svc = consumer(&node, &t.key, &slug, "jobs", json!({"max_attempts": 2, "on_exhausted": "dlq"})).await;
    append(
        &node,
        &t.key,
        "jobs",
        json!([
            {"body": {"tag": "a"}}, {"body": {"tag": "b"}}, {"body": {"tag": "bad", "poison": true}},
            {"body": {"tag": "c"}}, {"body": {"tag": "d"}}
        ]),
    )
    .await;

    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(30), "c and d after the dead letter", || {
        let sl = sl.clone();
        async move { tags(&got(n, &sl).await).contains(&"d".to_string()) }
    })
    .await;
    let g = got(&node, &slug).await;
    let bad: Vec<_> = g.iter().filter(|x| x["body"]["tag"] == json!("bad")).map(|x| x["attempt"].clone()).collect();
    assert_eq!(bad, vec![json!(1), json!(2)], "the poison record gets exactly max_attempts deliveries: {g:?}");
    for tag in ["a", "b", "c", "d"] {
        assert_eq!(tags(&g).iter().filter(|x| *x == tag).count(), 1, "`{tag}` delivered once: {g:?}");
    }

    let dlq = get(&node, &t.key, &format!("/v1/services/{svc}/consumers/jobs/default/dlq")).await;
    assert_eq!(dlq.s(), 200, "{}", dlq.text);
    let items = dlq.items();
    assert_eq!(items.len(), 1, "{}", dlq.text);
    assert_eq!((items[0]["offset"].clone(), items[0]["attempts"].clone()), (json!(2), json!(2)), "{}", dlq.text);
    assert_eq!(items[0]["body"]["tag"], json!("bad"));
    assert_eq!(consumer_view(&node, &t.key, &svc).await["dead"], json!(1));

    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{svc}/consumers/jobs/default/dlq")).await.s(), 204);
    assert!(get(&node, &t.key, &format!("/v1/services/{svc}/consumers/jobs/default/dlq")).await.items().is_empty());
    node.shutdown().await;
}
}

node_test! {
/// AC-STM-E2E-03: records and the committed cursor survive a restart: after it, only new records
/// are delivered (nothing replayed) and the old ones are still readable.
async fn ac_stm_e2e_03_cursor_and_records_survive_restart() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    create_topic(&node, &t.key, "log", 1).await;
    let slug = uniq("stmrst");
    let svc = consumer(&node, &t.key, &slug, "log", json!({})).await;
    append(&node, &t.key, "log", json!([{"body": {"tag": "1"}}, {"body": {"tag": "2"}}])).await;
    wait_got(&node, &slug, 2, Duration::from_secs(20)).await;
    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(10), "committed 2", || async move {
        consumer_view(n, k, s).await["partitions"][0]["committed"] == json!(2)
    })
    .await;

    node.restart().await;
    append(&node, &t.key, "log", json!([{"body": {"tag": "3"}}])).await;
    wait_got(&node, &slug, 3, Duration::from_secs(20)).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(tags(&got(&node, &slug).await), vec!["1", "2", "3"], "nothing replayed after the restart");
    let r = get(&node, &t.key, "/v1/topics/log/messages?from=earliest").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.items().len(), 3, "{}", r.text);
    assert_eq!((r.json["next"].clone(), r.json["end"].clone()), (json!(3), json!(true)), "{}", r.text);
    node.stop().await;
}
}

node_test! {
/// AC-STM-E2E-04: resetting a consumer to `earliest` replays every record.
async fn ac_stm_e2e_04_reset_to_earliest_replays() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    create_topic(&node, &t.key, "replay", 1).await;
    let slug = uniq("stmreplay");
    let svc = consumer(&node, &t.key, &slug, "replay", json!({})).await;
    append(&node, &t.key, "replay", json!([{"body": {"tag": "x"}}, {"body": {"tag": "y"}}, {"body": {"tag": "z"}}])).await;
    wait_got(&node, &slug, 3, Duration::from_secs(20)).await;

    let r = reset(&node, &t.key, &svc, "replay", json!("earliest")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["partitions"][0]["committed"], json!(0), "{}", r.text);
    wait_got(&node, &slug, 6, Duration::from_secs(20)).await;
    assert_eq!(tags(&got(&node, &slug).await), vec!["x", "y", "z", "x", "y", "z"]);
    node.shutdown().await;
}
}

node_test! {
/// AC-STM-E2E-05: with `on_exhausted: block` an exhausted record blocks the cursor (state
/// `blocked` in the view, later records wait); a reset past it unblocks and delivery resumes.
async fn ac_stm_e2e_05_block_stops_and_reset_resumes() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    create_topic(&node, &t.key, "strict", 1).await;
    let slug = uniq("stmblock");
    let svc = consumer(&node, &t.key, &slug, "strict", json!({"max_attempts": 1, "on_exhausted": "block"})).await;
    append(&node, &t.key, "strict", json!([{"body": {"tag": "ok"}}, {"body": {"tag": "bad", "poison": true}}, {"body": {"tag": "after"}}])).await;

    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(20), "the cursor to block", || async move {
        consumer_view(n, k, s).await["partitions"][0]["state"] == json!("blocked")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(tags(&got(&node, &slug).await), vec!["ok", "bad"], "nothing after the blocked record");
    let v = consumer_view(&node, &t.key, &svc).await;
    assert_eq!(v["partitions"][0]["committed"], json!(1), "{v}");

    let r = reset(&node, &t.key, &svc, "strict", json!(2)).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["partitions"][0]["state"], json!("active"), "{}", r.text);
    wait_got(&node, &slug, 3, Duration::from_secs(20)).await;
    assert_eq!(tags(&got(&node, &slug).await), vec!["ok", "bad", "after"]);
    node.shutdown().await;
}
}

node_test! {
/// AC-STM-E2E-06: a WASM component appends with `std:host/streams` and consumes through its
/// `POST /__rt/stream` handler (a 2xx commits the batch); a Deno guest appends to the same topic.
async fn ac_stm_e2e_06_wasm_component_appends_and_consumes() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    create_topic(&node, &t.key, "mixed", 1).await;
    let wasm = uniq("stmwasm");
    let wsvc = ship_wasm(&node, &t.key, &wasm, "events").await;
    let r = consume(&node, &t.key, &wsvc, json!([{"topic": "mixed", "start": "earliest"}])).await;
    assert_eq!(r.s(), 200, "{}", r.text);

    let r = invoke(&node, &wasm, "/append?topic=mixed&key=k&body=%7B%22tag%22%3A%22w%22%7D&n=2").await;
    let ok = r.json["ok"].as_array().cloned().unwrap_or_default();
    assert_eq!(ok.len(), 2, "{}", r.text);
    assert_eq!((ok[0]["offset"].clone(), ok[1]["offset"].clone()), (json!(0), json!(1)), "{}", r.text);
    let deno = uniq("stmdenoapp");
    ship(&node, &t.key, &deno, CONSUMER).await;
    let r = invoke(&node, &deno, "/append?topic=mixed&b=d").await;
    assert_eq!((r.json["offset"].clone(), r.json["duplicate"].clone()), (json!(2), json!(false)), "{}", r.text);

    let (n, k, s) = (&node, &t.key, &wsvc);
    eventually(Duration::from_secs(20), "the component to commit all three", || async move {
        consumer_view(n, k, s).await["partitions"][0]["committed"] == json!(3)
    })
    .await;
    let r = invoke(&node, &wasm, "/read?topic=mixed&from=earliest").await;
    assert_eq!(r.json["ok"]["entries"].as_array().map(Vec::len), Some(3), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// AC-STM-E2E-07: a plan without the `stream` trigger refuses an `events.consume` block with
/// `403 trigger_not_allowed` (on patch and on create); an empty block still passes.
async fn ac_stm_e2e_07_plan_without_stream_trigger_refuses_the_consume_block() {
    let node = start_node_with(|c| {
        let mut p = pro_plan();
        p.allowed_triggers.retain(|k| *k != TriggerKind::Stream);
        c.plans.insert("nostream".to_string(), p);
    })
    .await;
    let t = new_tenant(&node, "nostream").await;
    let slug = uniq("stmnoplan");
    let svc = ship(&node, &t.key, &slug, CONSUMER).await;
    let r = consume(&node, &t.key, &svc, json!([{"topic": "orders"}])).await;
    assert_eq!((r.s(), r.code()), (403, "trigger_not_allowed".to_string()), "{}", r.text);
    let r = consume(&node, &t.key, &svc, json!([])).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let r = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": uniq("stmnoplan2"), "runtime": "deno", "events": {"consume": [{"topic": "orders"}]}}),
    )
    .await;
    assert_eq!((r.s(), r.code()), (403, "trigger_not_allowed".to_string()), "{}", r.text);
    // Subscribing (pubsub) is still allowed on this plan.
    let r = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"events": {"subscribe": [{"subject": "a.b"}]}})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    node.shutdown().await;
}
}
