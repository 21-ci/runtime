//! `std:stream` from a Deno guest: `append` / `appendBatch` / `read` / `info` through the host
//! handle with JS-side bounds, the partition rule of `read`, and a `stream(batch, ctx)` consumer
//! (Form B) whose `failAt` becomes the `x-rt-stream-fail-at` header, or a plain `fetch` (Form A,
//! a synthetic `POST /__rt/stream`). Also `ctx.invocation` of an async HTTP invocation.

use std::sync::Arc;

use crate::common;

use bytes::Bytes;
use common::*;
use rt_types::events::{StreamBatch, StreamRecord};
use rt_types::{Outcome, Payload, Trigger};
use serde_json::{json, Value};

const PRODUCER: &str = "import { append, appendBatch, read, info } from 'std:stream';\n\
const fail = (e) => ({ error: (e && e.code) || String(e) });\n\
export default {\n\
  async fetch(req) {\n\
    const u = new URL(req.url);\n\
    const q = (k) => u.searchParams.get(k);\n\
    const mode = q('mode');\n\
    const t = q('t') ?? 'events';\n\
    try {\n\
      if (mode === 'append') return Response.json(await append(t, { n: Number(q('n') ?? 0) }, { key: q('key') ?? undefined, dedupId: q('dedup') ?? undefined, headers: { h: 'v' } }));\n\
      if (mode === 'batch') return Response.json(await appendBatch(t, [{ body: 'a' }, { body: new Uint8Array([1]), key: 'kk' }, { body: { c: 3 }, headers: { x: 'y' } }]));\n\
      if (mode === 'read') return Response.json(await read(t, JSON.parse(q('o') ?? '{}')));\n\
      if (mode === 'info') return Response.json(await info(t));\n\
      if (mode === 'bad') {\n\
        const big = 'x'.repeat(1024 * 1024 + 1);\n\
        const c = q('case');\n\
        const cases = {\n\
          topic: () => append('Bad', 1),\n\
          topicid: () => append('tpc_x', 1),\n\
          topiclong: () => append('a'.repeat(64), 1),\n\
          key: () => append('events', 1, { key: 'k'.repeat(257) }),\n\
          dedup: () => append('events', 1, { dedupId: 'd'.repeat(129) }),\n\
          dedupempty: () => append('events', 1, { dedupId: '' }),\n\
          body: () => append('events', big),\n\
          header: () => append('events', 1, { headers: { 'x-rt-a': 'b' } }),\n\
          batchempty: () => appendBatch('events', []),\n\
          batchnotarray: () => appendBatch('events', 'x'),\n\
          batchmany: () => appendBatch('events', Array(101).fill({ body: 1 })),\n\
          batchbytes: () => appendBatch('events', Array(5).fill({ body: new Uint8Array(1024 * 1024) })),\n\
          batchitem: () => appendBatch('events', [null]),\n\
          limit0: () => read('events', { limit: 0 }),\n\
          limit101: () => read('events', { limit: 101 }),\n\
          partition: () => read('events', { partition: 16 }),\n\
          partitionneg: () => read('events', { partition: -1 }),\n\
          both: () => read('events', { from: 0, fromTime: 1 }),\n\
          from: () => read('events', { from: 'middle' }),\n\
          fromtime: () => read('events', { fromTime: -5 }),\n\
          opts: () => read('events', 7),\n\
          infotopic: () => info('A'),\n\
          multi: () => read('multi', {}),\n\
          multirange: () => read('multi', { partition: 3 }),\n\
          missing: () => append('nope', 1),\n\
        };\n\
        await cases[c]();\n\
        return Response.json({ error: null });\n\
      }\n\
    } catch (e) { return Response.json(fail(e)); }\n\
    return new Response('unknown mode', { status: 400 });\n\
  },\n\
};\n";

async fn json_of(h: &mut Harness, p: &Arc<rt_exec_deno::DenoPrepared>, path: &str) -> Value {
    h.get(path, p).await.0.json()
}

/// `append` and `appendBatch` land in the bus with their bodies, keys, headers and dedup ids, and
/// answer `{partition, offset, duplicate}` per record.
#[test]
fn ac_dno_stm_01_append_and_append_batch() {
    run_local(async {
        let (mut h, _, st) = event_harness();
        let p = h.prepare(&ts_upload(PRODUCER)).await;
        assert_eq!(
            json_of(&mut h, &p, "/?mode=append&n=1&dedup=d1").await,
            json!({ "partition": 0, "offset": 0, "duplicate": false })
        );
        assert_eq!(
            json_of(&mut h, &p, "/?mode=append&n=2&dedup=d1").await,
            json!({ "partition": 0, "offset": 0, "duplicate": true })
        );
        assert_eq!(
            json_of(&mut h, &p, "/?mode=batch").await,
            json!([
                { "partition": 0, "offset": 1, "duplicate": false },
                { "partition": 0, "offset": 2, "duplicate": false },
                { "partition": 0, "offset": 3, "duplicate": false },
            ])
        );
        assert_eq!(
            json_of(&mut h, &p, "/?mode=append&t=multi&key=k").await,
            json!({ "partition": 1, "offset": 0, "duplicate": false })
        );
        let all = st.records.lock().unwrap();
        let ev = &all["events"];
        assert_eq!(ev[0].body, Bytes::from_static(br#"{"n":1}"#));
        assert!(ev[0].headers.contains(&("h".to_string(), "v".to_string())));
        assert_eq!(ev[1].body, Bytes::from_static(br#""a""#));
        assert_eq!(
            (ev[2].body.clone(), ev[2].key.clone()),
            (Bytes::from_static(&[1]), Some("kk".into()))
        );
        assert_eq!(ev[3].headers, vec![("x".to_string(), "y".to_string())]);
    });
}

/// `read` pages records (decoded bodies, header objects) from earliest / latest / an offset / a
/// time; `info` lists the partitions.
#[test]
fn ac_dno_stm_02_read_and_info() {
    run_local(async {
        let (mut h, _, _) = event_harness();
        let p = h.prepare(&ts_upload(PRODUCER)).await;
        for n in 0..3 {
            json_of(&mut h, &p, &format!("/?mode=append&n={n}")).await;
        }
        let page = json_of(&mut h, &p, "/?mode=read").await;
        assert_eq!((page["next"].clone(), page["end"].clone()), (json!(3), json!(3)));
        assert_eq!(
            page["records"][0],
            json!({ "partition": 0, "offset": 0, "key": null, "body": { "n": 0 }, "headers": { "h": "v" }, "timestamp": 1000 })
        );
        let page = json_of(&mut h, &p, "/?mode=read&o=%7B%22from%22%3A1%2C%22limit%22%3A1%7D").await;
        assert_eq!(page["records"].as_array().unwrap().len(), 1);
        assert_eq!(
            (page["records"][0]["offset"].clone(), page["next"].clone()),
            (json!(1), json!(2))
        );
        let page = json_of(&mut h, &p, "/?mode=read&o=%7B%22from%22%3A%22latest%22%7D").await;
        assert_eq!(page, json!({ "records": [], "next": 3, "end": 3 }));
        let page = json_of(&mut h, &p, "/?mode=read&o=%7B%22fromTime%22%3A1002%7D").await;
        assert_eq!(page["records"][0]["offset"], 2);
        let page = json_of(&mut h, &p, "/?mode=read&t=multi&o=%7B%22partition%22%3A2%7D").await;
        assert_eq!(page, json!({ "records": [], "next": 0, "end": 0 }));
        assert_eq!(
            json_of(&mut h, &p, "/?mode=info").await,
            json!({ "name": "events", "partitions": [{ "partition": 0, "first": 0, "next": 3 }], "retentionHours": 24, "maxMb": 0, "bytes": 21 })
        );
    });
}

/// Every bound is refused with the host's code; the partition rule and `not_found` come from the
/// bus.
#[test]
fn ac_dno_stm_03_validation_codes() {
    run_local(async {
        let (mut h, _, st) = event_harness();
        let p = h.prepare(&ts_upload(PRODUCER)).await;
        let cases = [
            ("topic", "validation_failed"),
            ("topicid", "validation_failed"),
            ("topiclong", "validation_failed"),
            ("key", "validation_failed"),
            ("dedup", "validation_failed"),
            ("dedupempty", "validation_failed"),
            ("body", "too_large"),
            ("header", "validation_failed"),
            ("batchempty", "validation_failed"),
            ("batchnotarray", "validation_failed"),
            ("batchmany", "too_large"),
            ("batchbytes", "too_large"),
            ("batchitem", "validation_failed"),
            ("limit0", "validation_failed"),
            ("limit101", "validation_failed"),
            ("partition", "validation_failed"),
            ("partitionneg", "validation_failed"),
            ("both", "validation_failed"),
            ("from", "validation_failed"),
            ("fromtime", "validation_failed"),
            ("opts", "validation_failed"),
            ("infotopic", "validation_failed"),
            ("multi", "validation_failed"),
            ("multirange", "validation_failed"),
            ("missing", "not_found"),
        ];
        for (c, code) in cases {
            assert_eq!(
                json_of(&mut h, &p, &format!("/?mode=bad&case={c}")).await,
                json!({ "error": code }),
                "{c}"
            );
        }
        assert!(st.records.lock().unwrap().is_empty());

        // Without a bus every call is `unavailable`.
        let mut bare = Harness::new(cfg0());
        let p = bare.prepare(&ts_upload(PRODUCER)).await;
        for m in ["append", "read", "info"] {
            assert_eq!(
                json_of(&mut bare, &p, &format!("/?mode={m}")).await,
                json!({ "error": "unavailable" }),
                "{m}"
            );
        }
    });
}

fn record(offset: u64) -> StreamRecord {
    StreamRecord {
        partition: 2,
        offset,
        key: Some("k".into()),
        headers: vec![("a".into(), "b".into())],
        body: Bytes::from(format!("{{\"n\":{offset}}}")),
        ts_ms: 500 + offset as i64,
    }
}

fn batch(offsets: std::ops::Range<u64>) -> rt_types::TaskSpec {
    let b = StreamBatch {
        topic: "orders".into(),
        group: "billing".into(),
        partition: 2,
        attempt: 3,
        records: offsets.map(record).collect(),
    };
    spec()
        .trigger(Trigger::Stream {
            topic: Arc::from("orders"),
            partition: 2,
            attempt: 3,
        })
        .payload(Payload::Stream(b))
        .finish()
}

const CONSUMER: &str = "import { kv } from 'std:kv';\n\
export default {\n\
  async stream(batch, ctx) {\n\
    await kv.set('seen', { trigger: ctx.trigger, topic: batch.topic, group: batch.group, partition: batch.partition, attempt: batch.attempt, frozen: Object.isFrozen(batch.messages), messages: batch.messages });\n\
    const mode = batch.messages[0].body.n === 10 ? 'fail' : batch.messages[0].body.n === 20 ? 'outside' : batch.messages[0].body.n === 30 ? 'throw' : 'ok';\n\
    if (mode === 'fail') { batch.failAt(batch.messages[2]); batch.failAt(12); batch.failAt(batch.messages[1]); batch.failAt(13); }\n\
    if (mode === 'outside') {\n\
      const errs = [];\n\
      for (const x of [99, { offset: 5 }, null, 'x']) { try { batch.failAt(x); errs.push(null); } catch (e) { errs.push(e.code); } }\n\
      await kv.set('errs', errs);\n\
    }\n\
    if (mode === 'throw') throw new Error('poison');\n\
  },\n\
  async fetch(req) {\n\
    return Response.json({ seen: await kv.get('seen'), errs: (await kv.get('errs')) ?? null });\n\
  },\n\
};\n";

/// A normal return commits the whole batch (200 without the fail-at header); the handler sees the
/// batch fields and decoded records.
#[test]
fn ac_dno_stm_04_stream_handler_commit_all() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(CONSUMER)).await;
        let got = read(h.run(batch(0..2), &p).await.outcome).await;
        assert_eq!(got.status, 200);
        assert!(got.headers.get("x-rt-stream-fail-at").is_none());
        let seen = json_of(&mut h, &p, "/").await["seen"].clone();
        assert_eq!(
            seen,
            json!({
                "trigger": "stream", "topic": "orders", "group": "billing", "partition": 2, "attempt": 3, "frozen": true,
                "messages": [
                    { "partition": 2, "offset": 0, "key": "k", "body": { "n": 0 }, "headers": { "a": "b" }, "timestamp": 500 },
                    { "partition": 2, "offset": 1, "key": "k", "body": { "n": 1 }, "headers": { "a": "b" }, "timestamp": 501 },
                ],
            })
        );
    });
}

/// `failAt` (a record or its offset) sets `x-rt-stream-fail-at`; the lowest call wins.
#[test]
fn ac_dno_stm_05_fail_at_sets_the_header_lowest_wins() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(CONSUMER)).await;
        let got = read(h.run(batch(10..14), &p).await.outcome).await;
        assert_eq!(got.status, 200);
        assert_eq!(got.headers["x-rt-stream-fail-at"], "11");
    });
}

/// `failAt` with an offset or a record outside the batch throws `validation_failed`; a thrown
/// handler fails the whole batch (guest error).
#[test]
fn ac_dno_stm_06_fail_at_outside_batch_throws_validation_failed() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(CONSUMER)).await;
        let got = read(h.run(batch(20..22), &p).await.outcome).await;
        assert!(got.headers.get("x-rt-stream-fail-at").is_none());
        assert_eq!(
            json_of(&mut h, &p, "/").await["errs"],
            json!([
                "validation_failed",
                "validation_failed",
                "validation_failed",
                "validation_failed"
            ])
        );
        assert!(matches!(h.run(batch(30..31), &p).await.outcome, Outcome::GuestError));
    });
}

const FORM_A: &str = "export default async function (req, ctx) {\n\
  const body = await req.json();\n\
  return new Response(null, { status: 200, headers: { 'x-rt-stream-fail-at': String(body.messages[1].offset), 'x-path': new URL(req.url).pathname, 'x-trigger': req.headers.get('x-rt-trigger') + '/' + ctx.trigger, 'x-group': body.group } });\n\
}\n";

/// A plain `fetch` gets the batch as `POST /__rt/stream`; its response headers go back as is.
#[test]
fn ac_dno_stm_07_form_a_gets_synthetic_post() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(FORM_A)).await;
        let got = read(h.run(batch(4..7), &p).await.outcome).await;
        assert_eq!(got.headers["x-path"], "/__rt/stream");
        assert_eq!(got.headers["x-trigger"], "stream/stream");
        assert_eq!(got.headers["x-group"], "billing");
        assert_eq!(got.headers["x-rt-stream-fail-at"], "5");

        let none = h.prepare(&ts_upload("export default { async pubsub() {} };\n")).await;
        assert!(matches!(h.run(batch(0..1), &none).await.outcome, Outcome::GuestError));
    });
}

/// `ctx.invocation` is `{id, attempt}` (frozen) for an async HTTP invocation and `undefined` for a
/// plain request.
#[test]
fn ac_dno_stm_08_ctx_invocation_for_async_http() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = "export default { fetch(req, ctx) { return Response.json({ inv: ctx.invocation ?? null, frozen: ctx.invocation ? Object.isFrozen(ctx.invocation) : null }); } };\n";
        let p = h.prepare(&ts_upload(src)).await;
        let mut req = rt_types::HttpRequest::get("/");
        req.headers
            .insert("x-rt-invocation-id", http::HeaderValue::from_static("inv_01"));
        req.headers
            .insert("x-rt-invocation-attempt", http::HeaderValue::from_static("2"));
        let s = spec().trigger(Trigger::Http).payload(Payload::Http(req)).finish();
        let got = read(h.run(s, &p).await.outcome).await;
        assert_eq!(
            got.json(),
            json!({ "inv": { "id": "inv_01", "attempt": 2 }, "frozen": true })
        );
        assert_eq!(json_of(&mut h, &p, "/").await, json!({ "inv": null, "frozen": null }));
    });
}

/// Both modules resolve as built-ins and export their API.
#[test]
fn ac_dno_stm_09_import_of_std_stream_resolves() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = "import * as s from 'std:stream';\nimport * as ps from 'std:pubsub';\n\
export default { fetch() { return Response.json({ s: Object.keys(s).sort(), ps: Object.keys(ps).sort() }); } };\n";
        let p = h.prepare(&ts_upload(src)).await;
        assert_eq!(
            json_of(&mut h, &p, "/").await,
            json!({ "s": ["append", "appendBatch", "info", "read"], "ps": ["publish", "request"] })
        );
    });
}
