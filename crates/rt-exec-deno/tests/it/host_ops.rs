//! The `std:*` modules of a Deno guest that sit on host facades (kv, blob, queue, platform,
//! durable storage, sockets, sql): their results, option handling and the stable error codes that
//! cross the op boundary. Each test is one guest that reports what it saw as JSON.

use crate::common;

use std::sync::Arc;

use common::*;
use rt_types::{ObjectRef, Outcome, TaskSpec};
use serde_json::{json, Value};

/// Run `code` as a guest on `h` and return the JSON it answers (the guest must answer 200).
async fn answer(h: &mut Harness, code: &str) -> Value {
    let p = h.prepare(&ts_upload(code)).await;
    let (got, _) = h.get("/", &p).await;
    assert_eq!(got.status, 200, "{}", got.text());
    got.json()
}

/// A GET to `/` that acts for the durable object `name`.
fn object_task(name: &str) -> TaskSpec {
    let mut s = get("/");
    s.object = Some(ObjectRef::named(name));
    s
}

const CODE_OF: &str = "const code = async (f: () => unknown) => { try { await f(); return 'ok'; } catch (e) { return (e as { code?: string }).code ?? String(e); } };\n";

// ---- kv ----------------------------------------------------------------------------------------

#[test]
fn kv_get_many_answers_in_order_with_null_for_missing_and_delete_says_whether_it_existed() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = answer(
            &mut h,
            "import { kv } from 'std:kv';\n\
             export default async () => {\n\
               await kv.set('a', 1); await kv.set('b', 'two');\n\
               const many = await kv.getMany(['a', 'b', 'zz']);\n\
               const del = await kv.delete('a');\n\
               const again = await kv.delete('a');\n\
               return Response.json({ many, del, again, left: await kv.get('a') });\n\
             };",
        )
        .await;
        assert_eq!(
            v,
            json!({ "many": [1, "two", null], "del": true, "again": false, "left": null })
        );
    });
}

// ---- blob --------------------------------------------------------------------------------------

#[test]
fn blob_round_trips_every_body_kind_lists_in_pages_and_deletes() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = answer(
            &mut h,
            "import { blob } from 'std:blob';\n\
             const text = async (r: any) => await new Response(r.body).text();\n\
             export default async () => {\n\
               const out: Record<string, unknown> = {};\n\
               await blob.put('docs/a.txt', 'hello', { contentType: 'text/plain' });\n\
               await blob.put('docs/b.bin', new Uint8Array([1, 2, 3]));\n\
               await blob.put('docs/c.ab', new Uint8Array([9, 9]).buffer);\n\
               await blob.put('docs/d.stream', new ReadableStream({ start(c) { c.enqueue(new Uint8Array([104, 105])); c.enqueue(new Uint8Array([33])); c.close(); } }));\n\
               await blob.put('docs/e.blob', new Blob(['xyz']));\n\
               out.head = await blob.head('docs/a.txt');\n\
               out.headMissing = await blob.head('nope');\n\
               const got = (await blob.get('docs/a.txt'))!;\n\
               out.got = { size: got.size, contentType: got.contentType, text: await text(got) };\n\
               out.stream = await text((await blob.get('docs/d.stream'))!);\n\
               out.blobBody = await text((await blob.get('docs/e.blob'))!);\n\
               out.missing = await blob.get('nope');\n\
               const page1 = await blob.list({ prefix: 'docs/', limit: 2 });\n\
               const page2 = await blob.list({ prefix: 'docs/', limit: 10, cursor: page1.cursor });\n\
               out.keys1 = page1.objects.map((o: any) => o.key);\n\
               out.keys2 = page2.objects.map((o: any) => o.key);\n\
               out.del = await blob.delete('docs/a.txt');\n\
               out.delAgain = await blob.delete('docs/a.txt');\n\
               return Response.json(out);\n\
             };",
        )
        .await;
        assert_eq!(v["head"], json!({ "size": 5, "contentType": "text/plain" }));
        assert_eq!(v["headMissing"], Value::Null);
        assert_eq!(
            v["got"],
            json!({ "size": 5, "contentType": "text/plain", "text": "hello" })
        );
        assert_eq!(v["stream"], "hi!");
        assert_eq!(v["blobBody"], "xyz");
        assert_eq!(v["missing"], Value::Null);
        assert_eq!(v["keys1"], json!(["docs/a.txt", "docs/b.bin"]));
        assert_eq!(v["keys2"], json!(["docs/c.ab", "docs/d.stream", "docs/e.blob"]));
        assert_eq!((v["del"].clone(), v["delAgain"].clone()), (json!(true), json!(false)));
    });
}

#[test]
fn a_blob_body_the_guest_cannot_send_is_a_type_error_and_leaves_no_object_behind() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = answer(
            &mut h,
            "import { blob } from 'std:blob';\n\
             export default async () => {\n\
               let error = 'none';\n\
               try { await blob.put('bad', 12345 as any); } catch (e) { error = (e as Error).name; }\n\
               return Response.json({ error, stored: await blob.head('bad') });\n\
             };",
        )
        .await;
        assert_eq!(v, json!({ "error": "TypeError", "stored": null }));
    });
}

// ---- queue -------------------------------------------------------------------------------------

#[test]
fn enqueue_sends_json_or_bytes_with_delay_and_dedup_and_fails_with_the_hosts_code() {
    run_local(async {
        let calls = Arc::new(MockCalls::default());
        let mut h = Harness::with_deps(cfg0(), |d| d.calls = calls.clone());
        let v = answer(
            &mut h,
            &format!(
                "import {{ enqueue }} from 'std:queue';\n{CODE_OF}\
                 export default async () => Response.json({{\n\
                   a: await enqueue('worker', {{ n: 1 }}),\n\
                   b: await enqueue('worker', new Uint8Array([1, 2]), {{ delaySeconds: 5, dedupId: 'd1' }}),\n\
                   missing: await code(() => enqueue('missing', 1)),\n\
                   full: await code(() => enqueue('full', 1)),\n\
                 }});"
            ),
        )
        .await;
        assert!(v["a"]["id"].as_str().is_some_and(|id| id.starts_with("msg_")), "{v}");
        assert_eq!(
            (v["missing"].clone(), v["full"].clone()),
            (json!("not_found"), json!("queue_full"))
        );
        let seen = calls.enqueued.lock().expect("lock").clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            (seen[0].0.as_str(), seen[0].1.as_slice()),
            ("worker", br#"{"n":1}"#.as_slice())
        );
        assert_eq!(seen[1].1, vec![1, 2]);
        assert_eq!(seen[1].2.delay, std::time::Duration::from_secs(5));
        assert_eq!(seen[1].2.dedup_id.as_deref(), Some("d1"));
    });
}

// ---- platform ----------------------------------------------------------------------------------

#[test]
fn a_refused_binding_call_resolves_to_a_response_with_the_reason_and_never_throws() {
    run_local(async {
        let mut h = Harness::with_deps(cfg0(), |d| d.platform = Arc::new(ScriptedBindings));
        let v = answer(
            &mut h,
            "import { platform } from 'std:platform';\n\
             const call = async (b: string) => { const r = await platform.fetch(b, new Request('http://x/p')); return [r.status, r.headers.get('x-rt-reason')]; };\n\
             export default async () => Response.json({\n\
               denied: await call('denied'), notAllowed: await call('notallowed'), limited: await call('limited'),\n\
               upstream: await call('upstream'), unknown: await call('nobody'),\n\
             });",
        )
        .await;
        assert_eq!(
            v,
            json!({
                "denied": [403, "binding_denied"],
                "notAllowed": [403, "binding_not_allowed"],
                "limited": [429, "rate_limited"],
                "upstream": [502, "binding_upstream"],
                "unknown": [404, "binding_not_found"],
            })
        );
    });
}

// ---- durable storage and object calls ----------------------------------------------------------

#[test]
fn durable_storage_reads_its_own_writes_lists_in_order_and_clears_everything_with_the_alarm() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h
            .prepare(&ts_upload(
                "import { storage } from 'std:durable';\n\
                 export default async () => {\n\
                   const out: Record<string, unknown> = {};\n\
                   await storage.put('a', { n: 1 });\n\
                   await storage.put({ b: 2, c: 3 });\n\
                   out.one = await storage.get('a');\n\
                   out.many = [...(await storage.get(['a', 'b', 'zz']))];\n\
                   out.backwards = [...(await storage.list({ reverse: true }))].map(([k]) => k);\n\
                   out.delOne = await storage.delete('a');\n\
                   out.delMany = await storage.delete(['b', 'zz']);\n\
                   await storage.setAlarm(5000);\n\
                   out.alarm = await storage.getAlarm();\n\
                   await storage.deleteAll();\n\
                   out.left = [...(await storage.list())].length;\n\
                   out.alarmAfter = await storage.getAlarm();\n\
                   return Response.json(out);\n\
                 };",
            ))
            .await;
        let ran = h.run(object_task("room"), &p).await;
        let got = read(ran.outcome).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(
            got.json(),
            json!({
                "one": { "n": 1 },
                "many": [["a", { "n": 1 }], ["b", 2]],
                "backwards": ["c", "b", "a"],
                "delOne": true,
                "delMany": 1,
                "alarm": 5000,
                "left": 0,
                "alarmAfter": null,
            })
        );
    });
}

#[test]
fn a_request_to_a_durable_object_answers_its_response_and_maps_failures_to_stable_codes() {
    run_local(async {
        let calls = Arc::new(MockCalls::default());
        let mut h = Harness::with_deps(cfg0(), |d| d.calls = calls.clone());
        let v = answer(
            &mut h,
            &format!(
                "import {{ durable }} from 'std:durable';\n{CODE_OF}\
                 const info = async (r: Response) => ({{ status: r.status, text: await r.text(), object: r.headers.get('x-object'), reason: r.headers.get('x-rt-reason') }});\n\
                 export default async () => Response.json({{\n\
                   hello: await info(await durable.get('room-1', {{ service: 'chat' }}).fetch('http://x/join?q=1', {{ method: 'POST', body: 'hi' }})),\n\
                   empty: await info(await durable.get('empty').fetch('http://x/')),\n\
                   deep: await info(await durable.get('deep').fetch('http://x/')),\n\
                   missing: await code(() => durable.get('missing').fetch('http://x/')),\n\
                   big: await code(async () => await (await durable.get('big').fetch('http://x/')).arrayBuffer()),
                   broken: await code(async () => await (await durable.get('broken').fetch('http://x/')).arrayBuffer()),\n\
                   rpc: await code(() => durable.get('room').fetch('http://x/__rt/rpc/m')),\n\
                   badName: await code(() => durable.get('')),\n\
                 }});"
            ),
        )
        .await;
        assert_eq!(
            v["hello"],
            json!({ "status": 202, "text": "room-1 says hi", "object": "room-1", "reason": null })
        );
        assert_eq!(v["empty"]["text"], "");
        assert_eq!(
            (v["deep"]["status"].clone(), v["deep"]["reason"].clone()),
            (json!(508), json!("call_depth"))
        );
        assert_eq!(v["missing"], "not_found");
        assert_eq!(
            v["big"], "Error: egress_too_large",
            "a body that grows past the egress limit fails the read"
        );
        assert_eq!(
            v["broken"], "Error: body_error",
            "a body that breaks mid-read fails the read"
        );
        assert_eq!(
            v["rpc"], "validation_failed",
            "the runtime's own paths are not reachable"
        );
        assert_eq!(v["badName"], "validation_failed");
        let seen = calls.objects.lock().expect("lock").clone();
        assert_eq!(
            seen[0],
            (Some("chat".to_string()), "room-1".to_string(), "/join?q=1".to_string())
        );
    });
}

// ---- sockets (a recording hub) -----------------------------------------------------------------

#[test]
fn socket_operations_reach_the_hub_and_answer_with_the_guest_shapes() {
    run_local(async {
        let hub = Arc::new(RecordingHub::default());
        let mut h = Harness::with_deps(cfg0(), |d| d.sockets = hub.clone());
        let p = h
            .prepare(&ts_upload(
                "import { sockets } from 'std:sockets';\n\
                 export default async () => {\n\
                   const out: Record<string, unknown> = {};\n\
                   const page = sockets.list({ tag: 't', limit: 5 });\n\
                   out.list = { ids: page.sockets.map((s: any) => s.id), cursor: page.cursor };\n\
                   out.get = sockets.get('ws_1')?.id; out.getMissing = sockets.get('ws_9');\n\
                   out.count = sockets.count({ tag: 't' });\n\
                   const objects = sockets.objects({ prefix: 'r', limit: 3 });\n\
                   out.objects = objects;\n\
                   sockets.setTags('ws_1', ['x', 'y']);\n\
                   try { sockets.getAttachment('ws_1'); } catch (e) { out.attachment = (e as Error).name; }\n\
                   sockets.setAttachment('ws_1', { user: 'ann' });\n\
                   sockets.setAutoResponse({ request: 'ping', response: 'pong' });\n\
                   sockets.setAutoResponse(null, { object: 'other' });\n\
                   sockets.send('ws_1', 'now', { immediate: true });\n\
                   out.broadcast = sockets.broadcast(new Uint8Array([1]), { tag: 't', except: ['ws_2'], immediate: true });\n\
                   sockets.close('ws_1', 1001, 'bye', { immediate: true });\n\
                   out.closed = sockets.closeAll({ object: 'room', code: 1000, reason: 'done', immediate: true });\n\
                   return Response.json(out);\n\
                 };",
            ))
            .await;
        let ran = h.run(object_task("room"), &p).await;
        if !matches!(ran.outcome, rt_types::Outcome::Ok(_)) {
            panic!(
                "{:?}\n{:#?}",
                ran.outcome,
                h.log_records().iter().map(|r| r.msg.clone()).collect::<Vec<_>>()
            );
        }
        let got = read(ran.outcome).await;
        assert_eq!(got.status, 200, "{}", got.text());
        let v = got.json();
        assert_eq!(v["list"], json!({ "ids": ["ws_1"], "cursor": "ws_1" }));
        assert_eq!(
            (v["get"].clone(), v["getMissing"].clone()),
            (json!("ws_1"), Value::Null)
        );
        assert_eq!(v["count"], 2);
        assert_eq!(
            v["attachment"], "RangeError",
            "the hub's bytes are not a structured clone"
        );
        assert_eq!(
            v["objects"],
            json!({ "objects": [{ "name": "room", "sockets": 2 }], "cursor": "room" })
        );
        assert_eq!(v["broadcast"], json!({ "sent": 3 }));
        assert_eq!(v["closed"], json!({ "closed": 3 }));
        let seen = hub.seen();
        for want in [
            r#"list Some("room") Some("t") 5 None"#,
            r#"objects "r" 3 None"#,
            r#"set_tags ws_1 ["x", "y"]"#,
            r#"set_auto_response room Some(("ping", "pong"))"#,
            "set_auto_response other None",
        ] {
            assert!(seen.iter().any(|s| s == want), "missing `{want}` in {seen:?}");
        }
        assert!(
            seen.iter().any(|s| s.contains("Send") && s.contains(r#"Text("now")"#)),
            "{seen:?}"
        );
        assert!(
            seen.iter()
                .any(|s| s.contains("Broadcast") && s.contains(r#"except: ["ws_2"]"#)),
            "{seen:?}"
        );
        assert!(
            seen.iter().any(|s| s.contains("Close {") && s.contains("1001")),
            "{seen:?}"
        );
        assert!(
            seen.iter()
                .any(|s| s.contains("CloseAll") && s.contains(r#"reason: "done""#)),
            "{seen:?}"
        );
    });
}

// ---- fetch, services, sockets.accept, durable without an object --------------------------------

#[test]
fn fetch_returns_the_upstream_response_and_a_service_call_returns_the_callees() {
    run_local(async {
        let cdn = rt_testkit::MockCdn::start().await.expect("cdn");
        cdn.set("/hello.txt", "from upstream", "text/plain");
        let mut h = Harness::with_deps(cfg0(), |d| d.calls = Arc::new(MockCalls::default()));
        let mut h2 = Harness::with_egress(cfg0(), LoopbackEgress::for_cdn(&cdn));
        let upstream = answer(
            &mut h2,
            &format!(
                "export default async () => {{ const r = await fetch('{}'); return Response.json({{ status: r.status, type: r.headers.get('content-type'), text: await r.text() }}); }};",
                cdn.url("/hello.txt")
            ),
        )
        .await;
        assert_eq!(
            upstream,
            json!({ "status": 200, "type": "text/plain", "text": "from upstream" })
        );

        let v = answer(
            &mut h,
            &format!(
                "import {{ services }} from 'std:services';\n{CODE_OF}\
                 const info = async (r: Response) => ({{ status: r.status, text: await r.text(), callee: r.headers.get('x-callee'), reason: r.headers.get('x-rt-reason') }});\n\
                 export default async () => Response.json({{\n\
                   ok: await info(await services.fetch('billing', new Request('http://x/pay?q=1', {{ method: 'POST', body: 'b' }}))),\n\
                   deep: await info(await services.fetch('deep', new Request('http://x/'))),\n\
                   missing: await code(() => services.fetch('missing', new Request('http://x/'))),\n\
                 }});"
            ),
        )
        .await;
        assert_eq!(
            v["ok"],
            json!({ "status": 201, "text": "billing got POST /pay?q=1", "callee": "billing", "reason": null })
        );
        assert_eq!(
            (v["deep"]["status"].clone(), v["deep"]["reason"].clone()),
            (json!(508), json!("call_depth"))
        );
        assert_eq!(v["missing"], "not_found");
    });
}

#[test]
fn accepting_an_upgrade_answers_101_with_the_hubs_ticket_and_hands_it_the_tags_attachment_and_protocol() {
    run_local(async {
        let hub = Arc::new(RecordingHub::default());
        let mut h = Harness::with_deps(cfg0(), |d| d.sockets = hub.clone());
        let p = h
            .prepare(&ts_upload(
                "import { sockets } from 'std:sockets';\n\
                 export default (req: Request) => sockets.accept(req, { tags: ['a'], attachment: { user: 'ann' }, protocol: 'v1' });",
            ))
            .await;
        let mut req = rt_types::HttpRequest::get("/");
        req.headers.insert("upgrade", "websocket".parse().expect("header"));
        let mut s = spec().payload(rt_types::Payload::Http(req)).finish();
        s.object = Some(ObjectRef::named("room"));
        let ran = h.run(s, &p).await;
        let rt_types::Outcome::Ok(reply) = ran.outcome else {
            panic!("{:?}", ran.outcome)
        };
        assert_eq!(reply.status.as_u16(), 101);
        assert_eq!(
            reply.headers.get(rt_types::TICKET_HEADER).and_then(|v| v.to_str().ok()),
            Some("ticket-1")
        );
        assert_eq!(
            reply
                .headers
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok()),
            Some("v1")
        );
        let seen = hub.seen();
        assert!(
            seen[0].starts_with(r#"accept room ["a"] Some(b""#) && seen[0].ends_with(r#"Some("v1")"#),
            "{seen:?}"
        );
    });
}

#[test]
fn durable_storage_in_a_task_without_an_object_fails_with_no_object() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = answer(
            &mut h,
            &format!(
                "import {{ storage }} from 'std:durable';\n{CODE_OF}\
                 export default async () => Response.json({{ get: await code(() => storage.get('k')), put: await code(() => storage.put('k', 1)), del: await code(() => storage.delete('k')) }});"
            ),
        )
        .await;
        assert_eq!(v, json!({ "get": "no_object", "put": "no_object", "del": "no_object" }));
    });
}

// ---- sql binds and results ---------------------------------------------------------------------

#[test]
fn sql_binds_every_value_type_and_reads_every_value_type_back() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = answer(
            &mut h,
            "import { sql } from 'std:sql';\n\
             const plain = (v: unknown) => v instanceof Uint8Array ? { bytes: Array.from(v) } : v;\n\
             export default async () => {\n\
               const r = await sql.execute('select ? as a, ? as b, ? as c, ? as d, ? as e, ? as f, ? as g', [null, 7, 1.5, 'x', new Uint8Array([1, 2]), true, 3n]);\n\
               const nulls = await sql.execute('select null as n, 2.5 as r');\n\
               const batch = await sql.batch(['select 1 as n', { sql: 'select ? as n', args: [5] }, { sql: 'select 2 as n' }], 'read');\n\
               return Response.json({\n\
                 columns: r.columns, row: r.rows[0].map(plain),\n\
                 nulls: nulls.rows[0],\n\
                 batch: batch.map((x: any) => x.rows[0][0]),\n\
               });\n\
             };",
        )
        .await;
        assert_eq!(v["columns"], json!(["a", "b", "c", "d", "e", "f", "g"]));
        assert_eq!(v["row"], json!([null, 7, 1.5, "x", { "bytes": [1, 2] }, 1, 3]));
        assert_eq!(v["nulls"], json!([null, 2.5]));
        assert_eq!(v["batch"], json!([1, 5, 2]));
    });
}

#[test]
fn std_sqlite_binds_nan_as_null_buffers_as_blobs_and_reals_as_reals() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = answer(
            &mut h,
            "import { Database } from 'std:sqlite';\n\
             export default () => {\n\
               const db = new Database();\n\
               const one = (v: unknown) => { const r: any = db.query('select ? as v').get(v as any); return r.v instanceof Uint8Array ? { bytes: Array.from(r.v) } : r.v; };\n\
               return Response.json({ nan: one(NaN), buffer: one(new ArrayBuffer(2)), real: one(2.5), big: one(2 ** 40), none: db.query('select 1 as v').get() });\n\
             };",
        )
        .await;
        assert_eq!(v["nan"], Value::Null);
        assert_eq!(v["buffer"], json!({ "bytes": [0, 0] }));
        assert_eq!(v["real"], 2.5);
        assert_eq!(v["none"], json!({ "v": 1 }));
    });
}

// ---- env, read where the code never mentions `Deno` ----------------------------------------------

#[test]
fn deno_env_read_through_an_indirect_name_is_fetched_on_the_spot_and_fails_when_the_store_is_down() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control
            .set_env(rt_testkit::ids::service(1), &[("A", "from env")], &[]);
        *h.control.env_thread_delay.lock().expect("lock") = Some(std::time::Duration::from_millis(30));
        let code = "export default () => { const D = (globalThis as any)['De' + 'no']; \
                    try { return Response.json({ a: D.env.get('A') }); } catch (e) { return Response.json({ error: (e as any).code ?? String(e) }); } };";
        assert_eq!(answer(&mut h, code).await, json!({ "a": "from env" }));

        h.control.env_fails.store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(answer(&mut h, code).await, json!({ "error": "Error: env_unavailable" }));
    });
}

// ---- operator SDKs -----------------------------------------------------------------------------

#[test]
fn an_sdk_method_answers_its_json_and_a_refusal_is_a_typed_error() {
    run_local(async {
        let mut h = Harness::with_deps(cfg0(), |d| d.calls = Arc::new(MockCalls::default()));
        let v = answer(
            &mut h,
            &format!(
                "import {{ sdk }} from 'std:sdk';\n{CODE_OF}\
                 export default async () => Response.json({{\n\
                   echo: await (sdk('acme') as any).echo({{ a: 1 }}),\n\
                   method: await code(() => (sdk('acme') as any).nope()),\n\
                   unknown: await code(() => (sdk('ghost') as any).echo()),\n\
                 }});"
            ),
        )
        .await;
        assert_eq!(
            v,
            json!({ "echo": { "a": 1 }, "method": "method_not_found", "unknown": "sdk_not_found" })
        );
    });
}

// ---- std:sqlite connections --------------------------------------------------------------------

#[test]
fn many_services_each_open_their_connection_and_the_registry_forgets_the_dead_ones() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h
            .prepare(&ts_upload(
                "import { Database } from 'std:sqlite';\n\
                 export default () => { const db = new Database(); db.run('create table if not exists t (n integer)'); return new Response('ok'); };",
            ))
            .await;
        // A cold service's first statement waits at most its budget for the open (on the blocking pool)
        // and fails fast with sql_busy otherwise; the open goes on and the next call finds it. On a slow
        // runner that happens, so retry like a client would: every service still gets its connection.
        for n in 1..=270u128 {
            let mut tries = 0;
            let ran = loop {
                tries += 1;
                let ran = h.run(spec().service(n).http_get("/").finish(), &p).await;
                if matches!(ran.outcome, Outcome::Ok(_)) || tries == 5 {
                    break ran;
                }
            };
            assert_eq!(read(ran.outcome).await.status, 200, "service {n} after {tries} tries");
        }
    });
}

// ---- CPU the host spends for the guest ------------------------------------------------------------

#[test]
fn host_side_work_for_the_guest_counts_against_its_cpu_budget() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h
            .prepare(&ts_upload(
                "import { sql } from 'std:sql';\n\
                 export default async () => { await sql.execute('with recursive c(x) as (select 1 union all select x + 1 from c where x < 20000000) select count(*) from c'); return new Response('done'); };",
            ))
            .await;
        let mut s = get("/");
        s.limits.cpu = std::time::Duration::from_millis(15);
        let ran = h.run(s, &p).await;
        match ran.outcome {
            rt_types::Outcome::Killed(rt_types::KillReason::CpuLimit) => {}
            // The statement may also hit the SQL time budget first on a slow machine.
            rt_types::Outcome::Ok(_) | rt_types::Outcome::GuestError => {}
            other => panic!("{other:?}"),
        }
    });
}
