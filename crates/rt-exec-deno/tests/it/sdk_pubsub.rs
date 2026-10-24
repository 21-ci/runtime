//! `std:pubsub` from a Deno guest: `publish` resolves to `{receivers, dropped}`, `request`
//! decodes the reply by its content type, the JS side refuses what the host would refuse with
//! the same codes, and a `pubsub(msg, ctx)` handler (Form B) or a plain `fetch` (Form A, a
//! synthetic `POST /__rt/pubsub`) receives deliveries.

use std::sync::Arc;

use crate::common;

use bytes::Bytes;
use common::*;
use rt_types::events::PubsubMessage;
use rt_types::{MsgId, Outcome, Payload, Trigger};
use serde_json::{json, Value};

const PUB: &str = "import { publish, request } from 'std:pubsub';\n\
const fail = (e) => ({ error: (e && e.code) || String(e) });\n\
export default {\n\
  async fetch(req) {\n\
    const u = new URL(req.url);\n\
    const mode = u.searchParams.get('mode');\n\
    const s = u.searchParams.get('s') ?? 'a.b';\n\
    try {\n\
      if (mode === 'publish') return Response.json(await publish(s, { n: 1 }, { headers: { h: 'v' } }));\n\
      if (mode === 'publish-bytes') return Response.json(await publish(s, new Uint8Array([1, 2, 3])));\n\
      if (mode === 'request') {\n\
        const r = await request(s, { q: 1 }, { timeoutMs: Number(u.searchParams.get('t') ?? 5000) });\n\
        const kind = r === undefined ? 'undefined' : r instanceof Uint8Array ? 'bytes' : typeof r;\n\
        return Response.json({ kind, value: r instanceof Uint8Array ? Array.from(r) : r ?? null });\n\
      }\n\
      if (mode === 'bad') {\n\
        const c = u.searchParams.get('case');\n\
        const big = 'x'.repeat(1024 * 1024 + 1);\n\
        const many = {}; for (let i = 0; i < 33; i++) many['h' + i] = 'v';\n\
        const cases = {\n\
          wildcard: () => publish('a.*', 1),\n\
          gt: () => publish('a.>', 1),\n\
          empty: () => publish('', 1),\n\
          emptytok: () => publish('a..b', 1),\n\
          reserved: () => publish('_rt.x', 1),\n\
          longsubj: () => publish('a'.repeat(257), 1),\n\
          tokens: () => publish(Array(17).fill('a').join('.'), 1),\n\
          nonstring: () => publish(5, 1),\n\
          body: () => publish('a', big),\n\
          hcount: () => publish('a', 1, { headers: many }),\n\
          hbytes: () => publish('a', 1, { headers: { a: 'x'.repeat(16 * 1024) } }),\n\
          hname: () => publish('a', 1, { headers: { 'bad name': 'v' } }),\n\
          hreserved: () => publish('a', 1, { headers: { 'X-Rt-Trace': 'v' } }),\n\
          hvalue: () => publish('a', 1, { headers: { a: 'x\\ny' } }),\n\
          hlong: () => publish('a', 1, { headers: { a: 'x'.repeat(4097) } }),\n\
          harray: () => publish('a', 1, { headers: [['a', 'b']] }),\n\
          opts: () => publish('a', 1, 'nope'),\n\
          timeout: () => request('a', 1, { timeoutMs: 'soon' }),\n\
          zero: () => request('a', 1, { timeoutMs: 0 }),\n\
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

/// `publish` hands body, headers and the subject to the bus and resolves to its counts.
#[test]
fn ac_dno_ps_01_publish_returns_receivers() {
    run_local(async {
        let (mut h, ps, _) = event_harness();
        let p = h.prepare(&ts_upload(PUB)).await;
        assert_eq!(
            json_of(&mut h, &p, "/?mode=publish&s=orders.eu.created").await,
            json!({ "receivers": 2, "dropped": 1 })
        );
        assert_eq!(
            json_of(&mut h, &p, "/?mode=publish-bytes").await,
            json!({ "receivers": 2, "dropped": 1 })
        );
        let calls = ps.calls.lock().unwrap().clone();
        assert_eq!(calls[0].0, "orders.eu.created");
        assert_eq!(calls[0].1, Bytes::from_static(br#"{"n":1}"#));
        assert_eq!(calls[0].2, vec![("h".to_string(), "v".to_string())]);
        assert_eq!(calls[1].1, Bytes::from_static(&[1, 2, 3]));
    });
}

/// The reply of a request is parsed JSON, text, bytes or `undefined`; bus errors keep their code;
/// the timeout reaches the bus clamped by the handle.
#[test]
fn ac_dno_ps_02_request_returns_parsed_json_text_bytes_or_undefined() {
    run_local(async {
        let (mut h, ps, _) = event_harness();
        let p = h.prepare(&ts_upload(PUB)).await;
        let cases = [
            ("r.json", json!({ "kind": "object", "value": { "ok": true } })),
            ("r.text", json!({ "kind": "string", "value": "hi" })),
            ("r.bytes", json!({ "kind": "bytes", "value": [1, 2] })),
            ("r.empty", json!({ "kind": "undefined", "value": null })),
            ("echo", json!({ "kind": "object", "value": { "q": 1 } })),
            ("r.none", json!({ "error": "no_responders" })),
        ];
        for (s, want) in cases {
            assert_eq!(json_of(&mut h, &p, &format!("/?mode=request&s={s}")).await, want, "{s}");
        }
        assert_eq!(
            json_of(&mut h, &p, "/?mode=request&s=r.slow&t=50").await,
            json!({ "error": "timeout" })
        );
        let t = ps.timeouts.lock().unwrap().clone();
        assert_eq!(t[0], Some(std::time::Duration::from_millis(5000)));
        assert_eq!(*t.last().unwrap(), Some(std::time::Duration::from_millis(50)));
    });
}

/// Every bound the host checks is refused in JS first with the same code, and nothing reaches
/// the bus.
#[test]
fn ac_dno_ps_03_js_validation_mirrors_rust() {
    run_local(async {
        let (mut h, ps, _) = event_harness();
        let p = h.prepare(&ts_upload(PUB)).await;
        let cases = [
            ("wildcard", "validation_failed"),
            ("gt", "validation_failed"),
            ("empty", "validation_failed"),
            ("emptytok", "validation_failed"),
            ("reserved", "validation_failed"),
            ("longsubj", "validation_failed"),
            ("tokens", "validation_failed"),
            ("nonstring", "validation_failed"),
            ("body", "too_large"),
            ("hcount", "too_large"),
            ("hbytes", "too_large"),
            ("hname", "validation_failed"),
            ("hreserved", "validation_failed"),
            ("hvalue", "validation_failed"),
            ("hlong", "validation_failed"),
            ("harray", "validation_failed"),
            ("opts", "validation_failed"),
            ("timeout", "validation_failed"),
            ("zero", "timeout"),
        ];
        for (c, code) in cases {
            assert_eq!(
                json_of(&mut h, &p, &format!("/?mode=bad&case={c}")).await,
                json!({ "error": code }),
                "{c}"
            );
        }
        assert!(ps.calls.lock().unwrap().is_empty());
    });
}

/// Without a bus (or outside a task) the calls are `unavailable`.
#[test]
fn ac_dno_ps_03b_no_bus_is_unavailable() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(PUB)).await;
        assert_eq!(
            json_of(&mut h, &p, "/?mode=publish").await,
            json!({ "error": "unavailable" })
        );
        assert_eq!(
            json_of(&mut h, &p, "/?mode=request").await,
            json!({ "error": "unavailable" })
        );
    });
}

fn delivery(replyable: bool, body: &'static [u8]) -> rt_types::TaskSpec {
    let msg = PubsubMessage {
        id: MsgId::from_u128(7),
        subject: "orders.created".into(),
        body: Bytes::from_static(body),
        headers: vec![("k".into(), "v".into())],
        replyable,
        published_at_ms: 42,
    };
    spec()
        .trigger(Trigger::Pubsub {
            subject: Arc::from("orders.created"),
            depth: 1,
            reply: replyable,
        })
        .payload(Payload::Pubsub(msg))
        .finish()
}

const SUB: &str = "export default {\n\
  async pubsub(msg, ctx) {\n\
    if (ctx.trigger !== 'pubsub' || !Object.isFrozen(msg)) throw new Error('bad ctx');\n\
    const k = msg.body.kind;\n\
    if (k === 'undef') return undefined;\n\
    if (k === 'resp') return new Response('made', { status: 201, headers: { 'x-a': 'b' } });\n\
    if (k === 'bytes') return new Uint8Array([9, 8]);\n\
    if (k === 'text') return 'plain';\n\
    if (k === 'throw') throw new Error('boom');\n\
    return { got: msg };\n\
  },\n\
};\n";

/// The return value of `pubsub()` is the reply of a request: undefined -> 204, a Response as is,
/// bytes, text or JSON; for a plain publish it is dropped (204).
#[test]
fn ac_dno_ps_04_message_handler_reply_encodings() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SUB)).await;

        let got = read(h.run(delivery(true, br#"{"kind":"undef"}"#), &p).await.outcome).await;
        assert_eq!((got.status, got.body.len()), (204, 0));

        let got = read(h.run(delivery(true, br#"{"kind":"resp"}"#), &p).await.outcome).await;
        assert_eq!((got.status, got.text().as_str()), (201, "made"));
        assert_eq!(got.headers["x-a"], "b");

        let got = read(h.run(delivery(true, br#"{"kind":"bytes"}"#), &p).await.outcome).await;
        assert_eq!(got.headers["content-type"], "application/octet-stream");
        assert_eq!(&got.body[..], &[9, 8]);

        let got = read(h.run(delivery(true, br#"{"kind":"text"}"#), &p).await.outcome).await;
        assert_eq!(got.headers["content-type"], "text/plain; charset=utf-8");
        assert_eq!(got.text(), "plain");

        let got = read(h.run(delivery(true, br#"{"kind":"json"}"#), &p).await.outcome).await;
        assert_eq!(got.headers["content-type"], "application/json");
        assert_eq!(
            got.json(),
            json!({ "got": {
                "id": MsgId::from_u128(7).to_string(),
                "subject": "orders.created",
                "body": { "kind": "json" },
                "headers": { "k": "v" },
                "replyable": true,
                "publishedAt": 42,
            } })
        );

        let got = read(h.run(delivery(false, br#"{"kind":"json"}"#), &p).await.outcome).await;
        assert_eq!((got.status, got.body.len()), (204, 0));
    });
}

const FORM_A: &str = "export default async function (req, ctx) {\n\
  const body = await req.json();\n\
  return Response.json({ method: req.method, path: new URL(req.url).pathname, trigger: req.headers.get('x-rt-trigger'), ctx: ctx.trigger, body });\n\
}\n";

/// A plain `fetch` handler gets the message as `POST /__rt/pubsub` with `x-rt-trigger: pubsub`;
/// a module with neither handler is a guest error.
#[test]
fn ac_dno_ps_05_form_a_gets_synthetic_post() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(FORM_A)).await;
        let got = read(h.run(delivery(true, b"plain"), &p).await.outcome).await;
        let v = got.json();
        assert_eq!(
            (&v["method"], &v["path"], &v["trigger"], &v["ctx"]),
            (
                &json!("POST"),
                &json!("/__rt/pubsub"),
                &json!("pubsub"),
                &json!("pubsub")
            )
        );
        assert_eq!(v["body"]["body"], "plain");
        assert_eq!(v["body"]["subject"], "orders.created");

        let none = h.prepare(&ts_upload("export default { async stream() {} };\n")).await;
        assert!(matches!(
            h.run(delivery(false, b"1"), &none).await.outcome,
            Outcome::GuestError
        ));
    });
}

/// A `pubsub()` handler that throws fails its task as a guest error (the requester gets
/// `responder_failed`).
#[test]
fn ac_dno_ps_06_thrown_message_handler_is_a_guest_error() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SUB)).await;
        let ran = h.run(delivery(true, br#"{"kind":"throw"}"#), &p).await;
        assert!(matches!(ran.outcome, Outcome::GuestError), "got {:?}", ran.outcome);
    });
}
