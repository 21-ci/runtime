//! What a flip does to work of the service (`docs/design/branching-services-design.md` §1, §5):
//! durable turns stay one at a time across it, and a stream consumer goes on from its cursor.

use std::time::Duration;

use serde_json::{json, Value};

use crate::common::*;

/// One durable object logging its turns: `slow` takes a while, each entry names the version.
fn room(tag: &str) -> String {
    format!(
        r#"import {{ sockets }} from "std:sockets";
export default class Room {{
  state: any;
  constructor(state: any, _env: any) {{ this.state = state; }}
  async log(entry: string) {{
    const s = this.state.storage;
    const l = (await s.get("log")) ?? [];
    l.push(entry);
    await s.put("log", l);
  }}
  async fetch(req: Request): Promise<Response> {{
    if (req.headers.get("upgrade")) return sockets.accept(req, {{ tags: ["u"] }});
    return Response.json((await this.state.storage.get("log")) ?? []);
  }}
  async webSocketMessage(ws: any, msg: string): Promise<void> {{
    await this.log("{tag}:" + msg + ":start");
    if (msg === "slow") await new Promise((r) => setTimeout(r, 1500));
    await this.log("{tag}:" + msg + ":end");
    ws.send(msg);
  }}
}}"#
    )
}

/// A turn running on v1 at the flip finishes; the next event runs on v2, after it, never beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_turns_stay_ordered_across_a_flip() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("prmd");
    let r = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": slug, "runtime": "deno", "durable": true}),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    let r = upload_ts(&node, &t.key, &id, &room("v1"), "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    let r = upload_ts(&node, &t.key, &id, &room("v2"), "").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);

    let mut ws = ws_connect(&node, &slug, "/").await;
    ws_send(&mut ws, "slow").await;
    let (n, s) = (&node, &slug);
    eventually(Duration::from_secs(20), "the slow turn to start", || async move {
        invoke(n, s, "/log").await.text.contains("v1:slow:start")
    })
    .await;

    let flip = put(&node, &t.key, &format!("/v1/services/{id}/live"), json!({"version": 2})).await;
    assert_eq!(flip.s(), 200, "{}", flip.text);
    ws_send(&mut ws, "next").await;

    assert_eq!(ws_text(&mut ws).await, "slow");
    assert_eq!(ws_text(&mut ws).await, "next");
    let log = invoke(&node, &slug, "/log").await.json;
    assert_eq!(
        log,
        json!(["v1:slow:start", "v1:slow:end", "v2:next:start", "v2:next:end"]),
        "one turn at a time, the old one first, the next on the new version"
    );
    node.shutdown().await;
}

/// Records each delivered offset with the version that consumed it.
fn consumer(tag: &str) -> String {
    format!(
        r#"import {{ sql }} from "std:sql";
async function ensure() {{
  await sql.execute("CREATE TABLE IF NOT EXISTS got (off INTEGER, ver TEXT)");
}}
export default {{
  async fetch(_req: Request): Promise<Response> {{
    await ensure();
    const r = await sql.execute("SELECT off, ver FROM got ORDER BY rowid");
    return Response.json(r.rows);
  }},
  async stream(batch: any, _ctx: any): Promise<void> {{
    await ensure();
    for (const m of batch.messages) await sql.execute("INSERT INTO got VALUES (?, ?)", [m.offset, "{tag}"]);
  }},
}};"#
    )
}

/// The cursor belongs to the service, so v2 goes on after what v1 committed: nothing replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_batch_continues_from_the_cursor_across_a_flip() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let r = post(&node, &t.key, "/v1/topics", json!({"name": "log", "partitions": 1})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let slug = uniq("prms");
    let id = ship(&node, &t.key, &slug, &consumer("v1")).await;
    let r = patch(
        &node,
        &t.key,
        &format!("/v1/services/{id}"),
        json!({"events": {"consume": [{"topic": "log", "group": "default", "start": "earliest"}]}}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

    let append = |tag: &'static str| {
        let (n, k) = (&node, &t.key);
        async move {
            let r = post(
                n,
                k,
                "/v1/topics/log/messages",
                json!({"records": [{"body": {"tag": tag}}]}),
            )
            .await;
            assert_eq!(r.s(), 202, "{}", r.text);
        }
    };
    append("a").await;
    append("b").await;
    let (n, k, s) = (&node, &t.key, &id);
    eventually(Duration::from_secs(20), "v1 to commit two records", || async move {
        let r = get(n, k, &format!("/v1/services/{s}/consumers")).await;
        r.items().first().map(|c| c["partitions"][0]["committed"].clone()) == Some(json!(2))
    })
    .await;

    let r = upload_ts(&node, &t.key, &id, &consumer("v2"), "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    append("c").await;
    let sl = &slug;
    eventually(Duration::from_secs(20), "the third delivery", || async move {
        invoke(n, sl, "/").await.json.as_array().is_some_and(|a| a.len() >= 3)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let got: Value = invoke(&node, &slug, "/").await.json;
    assert_eq!(
        got,
        json!([[0, "v1"], [1, "v1"], [2, "v2"]]),
        "nothing replayed or skipped"
    );
    node.shutdown().await;
}
