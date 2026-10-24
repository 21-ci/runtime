//! Streaming, `waitUntil`, memory limits and the dispose queue: DNO-06, DNO-07, DNO-08, DNO-16.
//!
//! Timing assertions use the real clock (`SystemClock`); each test warms V8 first so cold-start
//! cost does not count against the spec's budgets.

use crate::common;

use std::time::{Duration, Instant};

use common::*;
use futures_util::StreamExt;
use rt_types::{KillReason, Outcome};

// ---------------------------------------------------------------------------------------------
// DNO-06 streaming
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_06_first_chunk_arrives_before_the_handler_finishes() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.warm_up().await;
        let p = h.prepare_fixture("stream_chunks").await;

        let t0 = Instant::now();
        let ran = h.run(get("/"), &p).await;
        let Outcome::Ok(reply) = ran.outcome else {
            panic!("expected a streamed reply")
        };
        assert_eq!(reply.status.as_u16(), 200);

        let mut body = reply.body;
        let mut arrivals: Vec<(Duration, String)> = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.expect("chunk");
            arrivals.push((t0.elapsed(), String::from_utf8_lossy(&chunk).to_string()));
        }
        let total = t0.elapsed();

        assert!(
            arrivals.len() >= 2,
            "the response must be streamed, not buffered: {arrivals:?}"
        );
        let (first_at, first) = &arrivals[0];
        assert!(
            *first_at < Duration::from_millis(150),
            "first chunk after {first_at:?}, budget 150 ms"
        );
        assert!(first.starts_with("chunk0"), "got {first:?}");
        let joined: String = arrivals.iter().map(|(_, c)| c.as_str()).collect();
        assert_eq!(joined, "chunk0\nchunk1\nchunk2\n");
        assert!(
            total >= Duration::from_millis(250),
            "three 100 ms pauses take ~300 ms, took {total:?}"
        );
        assert!(total < Duration::from_millis(900), "took {total:?}");
        // Spacing: the last chunk is well after the first.
        let last_at = arrivals.last().expect("chunks").0;
        assert!(
            last_at - *first_at >= Duration::from_millis(150),
            "chunks arrived together: {arrivals:?}"
        );
    });
}

#[test]
fn dno_06_a_slow_reader_applies_backpressure_instead_of_buffering() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let upload = ts_upload(
            "export default () => { let i = 0; const enc = new TextEncoder(); \
             return new Response(new ReadableStream({ pull(c) { if (i++ < 200) c.enqueue(enc.encode('x'.repeat(65536))); else c.close(); } })); };\n",
        );
        let p = h.prepare(&upload).await;
        let Outcome::Ok(reply) = h.run(get("/"), &p).await.outcome else {
            panic!("ok")
        };
        // Read slowly; the bounded channel + `pull` semantics mean the guest is paused, not the
        // whole 12.8 MB produced up front. We can only assert it completes correctly here.
        let mut body = reply.body;
        let mut n = 0usize;
        while let Some(chunk) = body.next().await {
            n += chunk.expect("chunk").len();
            if n < 4 * 65536 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        assert_eq!(n, 200 * 65536);
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-22 request body streaming (B4.4)
// ---------------------------------------------------------------------------------------------

/// A POST whose body arrives through a channel the test feeds.
fn streamed_post(path: &str) -> (rt_types::BodySender, rt_types::Payload) {
    let (tx, body) = rt_types::body_channel(4);
    let mut req = rt_types::HttpRequest::get(path);
    req.method = http::Method::POST;
    req.body = body;
    (tx, rt_types::Payload::Http(req))
}

#[test]
fn ac_dno_22_the_handler_reads_the_first_request_chunk_before_the_client_finishes() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h
            .prepare(&ts_upload(
                "export default async (req: Request) => {
  const first = await req.body!.getReader().read();
  return new Response('got ' + new TextDecoder().decode(first.value));
};
",
            ))
            .await;
        let (tx, payload) = streamed_post("/upload");
        tx.send(bytes::Bytes::from_static(b"chunk 1"))
            .await
            .expect("send the first chunk");
        // The client has not finished (the sender is open): a buffering executor never answers.
        let ran = tokio::time::timeout(Duration::from_secs(5), h.run(spec().payload(payload).finish(), &p))
            .await
            .expect("the handler answered while the request body was still open");
        assert_eq!(read(ran.outcome).await.text(), "got chunk 1");
        drop(tx);
    });
}

#[test]
fn dno_22_a_streamed_request_body_is_read_whole_and_bounded_by_max_body_bytes() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h
            .prepare(&ts_upload(
                "export default async (req: Request) => {
  try { return new Response('read ' + (await req.text())); }
  catch (e) { return new Response((e as Error).message, { status: 413 }); }
};
",
            ))
            .await;
        // chunks arrive over time and are all read
        let (tx, payload) = streamed_post("/");
        tokio::task::spawn_local(async move {
            for c in ["a", "bb", "ccc"] {
                tokio::time::sleep(Duration::from_millis(20)).await;
                tx.send(bytes::Bytes::from(c)).await.expect("send");
            }
        });
        let got = read(h.run(spec().payload(payload).finish(), &p).await.outcome).await;
        assert_eq!(got.text(), "read abbccc");

        // past limits.max_body_bytes the read fails with a stable code (no buffering first)
        let mut over = spec()
            .payload(post_payload("/", "text/plain", &"x".repeat(64)))
            .finish();
        over.limits.max_request_body_bytes = 16;
        let got = read(h.run(over, &p).await.outcome).await;
        assert_eq!((got.status, got.text()), (413, "request_too_large".to_string()));
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-07 waitUntil
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_07_wait_until_runs_after_the_response_and_the_isolate_is_disposed_afterwards() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(1));
        h.warm_up().await;
        let p = h.prepare_fixture("waituntil_kv").await;
        let kv = rt_store::ServiceDataStore::kv(&*h.data, svc(1));

        let t0 = Instant::now();
        let ran = h.run(get("/"), &p).await;
        let got = read(ran.outcome).await;
        let responded = t0.elapsed();
        assert_eq!(got.status, 202);
        assert_eq!(got.text(), "accepted");
        assert!(
            responded < Duration::from_millis(50),
            "response took {responded:?}, budget 50 ms (not 500 ms)"
        );

        // The write has not happened yet...
        assert!(
            kv.get(b"waituntil").await.expect("kv get").is_none(),
            "KV written before the 500 ms timer"
        );

        // ...but it does, because the isolate is kept alive for the promise.
        h.settle(Duration::from_millis(800)).await;
        let v = kv
            .get(b"waituntil")
            .await
            .expect("kv get")
            .expect("waitUntil promise wrote KV");
        assert!(String::from_utf8_lossy(&v.data).contains("done"));

        // After the promise settled the isolate is queued for disposal and goes away on idle.
        assert!(
            h.exec.dispose_backlog_len() >= 1,
            "settled isolate waits in the dispose queue"
        );
        h.exec.on_idle(Duration::from_millis(200));
        assert_eq!(h.exec.dispose_backlog_len(), 0);
    });
}

#[test]
fn dno_07_the_isolate_is_not_disposed_while_wait_until_is_pending() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.warm_up().await;
        let p = h.prepare_fixture("waituntil_kv").await;
        let ran = h.run(get("/"), &p).await;
        let _ = read(ran.outcome).await;

        // 100 ms in, the 500 ms promise is still pending: nothing may be disposed yet.
        h.settle(Duration::from_millis(100)).await;
        h.exec.on_idle(Duration::from_millis(100));
        let kv = rt_store::ServiceDataStore::kv(&*h.data, svc(1));
        h.settle(Duration::from_millis(700)).await;
        assert!(
            kv.get(b"waituntil").await.expect("kv get").is_some(),
            "an idle pass must not kill an isolate that still has waitUntil work"
        );
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-08 memory limit
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_08_allocating_past_memory_limit_is_502_oom_and_the_next_request_is_served() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let oom = h.prepare_fixture("oom").await;
        let hello = h.prepare_fixture("hello").await;

        let ran = h.run(spec().memory_mb(24).http_get("/").finish(), &oom).await;
        assert!(
            matches!(ran.outcome, Outcome::Killed(KillReason::Oom)),
            "got {:?}",
            ran.outcome
        );
        assert_eq!(ran.outcome.http_status(), 502);
        assert_eq!(ran.outcome.reason(), Some("oom"));

        // The node continues.
        let (got, _) = h.get("/", &hello).await;
        assert_eq!(got.text(), "hello");

        // Committed memory returns to its baseline once the dead isolates are disposed.
        h.settle(Duration::from_millis(30)).await;
        h.exec.on_idle(Duration::from_millis(500));
        assert_eq!(h.exec.dispose_backlog_len(), 0);
        assert_eq!(
            h.gov.committed(),
            0,
            "governor must be back to baseline after oom + next request"
        );
    });
}

#[test]
fn dno_08_heap_growth_beyond_the_governor_budget_is_also_oom() {
    run_local(async {
        // A governor with room for the base heap but not for 8 MB growth steps.
        let mut h = Harness::new(cfg0());
        let oom = h.prepare_fixture("oom").await;
        let hog = h
            .gov
            .reserve(rt_mem::MemCategory::Other, 506 * MIB, rt_mem::Owner::NONE)
            .expect("fill the budget");
        let ran = h.run(spec().memory_mb(128).http_get("/").finish(), &oom).await;
        assert!(
            matches!(ran.outcome, Outcome::Killed(KillReason::Oom)),
            "refused try_grow => oom, got {:?}",
            ran.outcome
        );
        drop(hog);
    });
}

// ---------------------------------------------------------------------------------------------
// B5.1: ArrayBuffer backing stores live outside the V8 heap limit; they count against
// `limits.memory_mb` and the governor through the isolate's ArrayBuffer allocator
// ---------------------------------------------------------------------------------------------

/// Tries to keep 1 GiB of `Uint8Array`s alive in 64 MiB chunks; reports how far it got.
const AB_1GIB_CAUGHT: &str = "export default () => {
  const keep: Uint8Array[] = [];
  let err = 'none';
  try {
    for (let i = 0; i < 16; i++) {
      const a = new Uint8Array(64 * 1024 * 1024);
      a[0] = 1; a[a.length - 1] = 1;
      keep.push(a);
    }
  } catch (e) {
    err = String((e as Error).name) + ': ' + String((e as Error).message);
  }
  return Response.json({ mb: keep.length * 64, err });
};
";

/// The same without a `try`: the refusal escapes the handler.
const AB_1GIB_UNCAUGHT: &str = "export default () => {
  const keep: Uint8Array[] = [];
  for (let i = 0; i < 16; i++) keep.push(new Uint8Array(64 * 1024 * 1024));
  return new Response(String(keep.length));
};
";

#[test]
fn b5_1_array_buffers_count_against_the_task_memory_limit() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(AB_1GIB_CAUGHT)).await;
        let ran = h.run(spec().memory_mb(128).http_get("/").finish(), &p).await;
        let v = read(ran.outcome).await.json();
        let mb = v["mb"].as_u64().expect("mb");
        assert!(mb <= 128, "kept {mb} MiB of ArrayBuffers with memory_mb = 128: {v}");
        assert!(mb >= 64, "one 64 MiB chunk fits next to the heap: {v}");
        assert!(
            v["err"].as_str().unwrap_or("").starts_with("RangeError"),
            "the guest sees a RangeError: {v}"
        );

        // Everything is released with the isolate.
        h.settle(Duration::from_millis(30)).await;
        h.exec.on_idle(Duration::from_millis(500));
        assert_eq!(h.gov.committed(), 0, "buffers are released with the isolate");
    });
}

#[test]
fn b5_1_array_buffers_are_charged_to_the_governor() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(AB_1GIB_CAUGHT)).await;
        // The task may use 256 MiB, but the node has only ~40 MiB left.
        let hog = h
            .gov
            .reserve(rt_mem::MemCategory::Other, 472 * MIB, rt_mem::Owner::NONE)
            .expect("fill the budget");
        let ran = h.run(spec().memory_mb(256).http_get("/").finish(), &p).await;
        let v = read(ran.outcome).await.json();
        assert_eq!(v["mb"], 0, "the governor refused the first 64 MiB chunk: {v}");
        assert!(v["err"].as_str().unwrap_or("").starts_with("RangeError"), "{v}");
        drop(hog);
    });
}

#[test]
fn b5_1_an_uncaught_array_buffer_refusal_is_oom_and_the_node_continues() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(AB_1GIB_UNCAUGHT)).await;
        let hello = h.prepare_fixture("hello").await;
        let ran = h.run(spec().memory_mb(128).http_get("/").finish(), &p).await;
        assert!(
            matches!(ran.outcome, Outcome::Killed(KillReason::Oom)),
            "got {:?}",
            ran.outcome
        );
        assert_eq!(ran.outcome.http_status(), 502);

        let (got, _) = h.get("/", &hello).await;
        assert_eq!(got.text(), "hello");
        h.settle(Duration::from_millis(30)).await;
        h.exec.on_idle(Duration::from_millis(500));
        assert_eq!(h.gov.committed(), 0);
    });
}

/// `(module (memory (export "mem") 1) (func (export "add") (param i32 i32) (result i32) ...))`
const WASM_ADD: &str = "[0,97,115,109,1,0,0,0, 1,7,1,96,2,127,127,1,127, 3,2,1,0, 5,3,1,0,1, \
    7,13,2,3,97,100,100,0,0,3,109,101,109,2,0, 10,9,1,7,0,32,0,32,1,106,11]";
/// `(module (memory (export "mem") 1) (func (export "grow") (param i32) (result i32) memory.grow))`
const WASM_GROW: &str = "[0,97,115,109,1,0,0,0, 1,6,1,96,1,127,1,127, 3,2,1,0, 5,3,1,0,1, \
    7,14,2,4,103,114,111,119,0,0,3,109,101,109,2,0, 10,8,1,6,0,32,0,64,0,11]";
/// The same module with its memory not exported.
const WASM_HIDDEN: &str = "[0,97,115,109,1,0,0,0, 1,6,1,96,1,127,1,127, 3,2,1,0, 5,3,1,0,1, \
    7,8,1,4,103,114,111,119,0,0, 10,8,1,6,0,32,0,64,0,11]";
/// `(module (memory (export "mem") 1 65536))`: declares a 4 GiB maximum.
const WASM_BIG_MAX: &str = "[0,97,115,109,1,0,0,0, 5,6,1,1,1,128,128,4, 7,7,1,3,109,101,109,2,0]";

async fn run_ts(h: &mut Harness, memory_mb: u32, src: &str) -> Outcome {
    let p = h.prepare(&ts_upload(src)).await;
    let ran = h.run(spec().memory_mb(memory_mb).http_get("/").finish(), &p).await;
    if !matches!(ran.outcome, Outcome::Ok(_)) {
        for r in h.log_records().iter().filter(|r| r.task_id == Some(ran.task)) {
            eprintln!("task log: {}", r.msg);
        }
    }
    ran.outcome
}

async fn assert_node_recovers(h: &mut Harness) {
    let hello = h.prepare_fixture("hello").await;
    let (got, _) = h.get("/", &hello).await;
    assert_eq!(got.text(), "hello");
    h.settle(Duration::from_millis(30)).await;
    h.exec.on_idle(Duration::from_millis(500));
    assert_eq!(h.exec.dispose_backlog_len(), 0);
    assert_eq!(h.gov.committed(), 0, "everything is released with the isolates");
}

#[test]
fn b5_1_a_wasm_memory_grown_from_js_past_the_limit_is_oom() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = "export default () => {
  const m = new WebAssembly.Memory({ initial: 1 });
  for (let i = 0; i < 3; i++) m.grow(512); // +96 MiB
  return new Response(String(m.buffer.byteLength >> 20));
};
";
        let outcome = run_ts(&mut h, 64, src).await;
        assert!(matches!(outcome, Outcome::Killed(KillReason::Oom)), "got {outcome:?}");
        assert_node_recovers(&mut h).await;
    });
}

#[test]
fn b5_1_memory_grow_inside_a_module_past_the_limit_is_oom_even_in_a_busy_loop() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        // `memory.grow` runs inside wasm (no JS API call), then the guest never yields: only the
        // watchdog's interrupt can see the memory.
        let src = format!(
            "export default () => {{
  const inst = new WebAssembly.Instance(new WebAssembly.Module(new Uint8Array({WASM_GROW})));
  for (let i = 0; i < 3; i++) inst.exports.grow(512);
  let x = 0;
  for (;;) x++;
}};
"
        );
        let started = Instant::now();
        let outcome = run_ts(&mut h, 64, &src).await;
        assert!(matches!(outcome, Outcome::Killed(KillReason::Oom)), "got {outcome:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "killed by the memory check, not a later limit"
        );
        assert_node_recovers(&mut h).await;
    });
}

#[test]
fn b5_1_small_wasm_from_js_still_works() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = format!(
            "export default async () => {{
  const bytes = new Uint8Array({WASM_ADD});
  const sync = new WebAssembly.Instance(new WebAssembly.Module(bytes));
  const {{ module, instance }} = await WebAssembly.instantiate(bytes);
  const again = await WebAssembly.instantiate(module, {{}});
  const compiled = await WebAssembly.compile(bytes.buffer);
  const m = new WebAssembly.Memory({{ initial: 1, maximum: 4 }});
  m.grow(1);
  return Response.json({{
    sync: sync.exports.add(2, 3),
    async: instance.exports.add(20, 22),
    again: again.exports.add(1, 1),
    compiled: compiled instanceof WebAssembly.Module,
    valid: WebAssembly.validate(bytes),
    memPages: sync.exports.mem.buffer.byteLength / 65536,
    isMemory: sync.exports.mem instanceof WebAssembly.Memory && m instanceof WebAssembly.Memory,
    ctor: Object.getPrototypeOf(m).constructor === WebAssembly.Memory &&
      Object.getPrototypeOf(sync).constructor === WebAssembly.Instance &&
      Object.getPrototypeOf(module).constructor === WebAssembly.Module,
    grown: m.buffer.byteLength / 65536,
  }});
}};
"
        );
        let outcome = run_ts(&mut h, 128, &src).await;
        let v = read(outcome).await.json();
        assert_eq!(
            v,
            serde_json::json!({"sync": 5, "async": 42, "again": 2, "compiled": true, "valid": true, "memPages": 1,
                "isMemory": true, "ctor": true, "grown": 2})
        );
    });
}

/// `(module (memory 1) (func $s (memory.grow (i32.const 512)) drop ×3) (start $s))`: no export
/// section at all, and the start function grows the hidden memory by 96 MiB at instantiation.
const WASM_HIDDEN_START_GROW: &str = "[0,97,115,109,1,0,0,0, 1,4,1,96,0,0, 3,2,1,0, 5,3,1,0,1, 8,1,0, \
    10,22,1,20,0, 65,128,4,64,0,26, 65,128,4,64,0,26, 65,128,4,64,0,26, 11]";

/// D-A2 follow-up: a module that defines a memory without exporting it compiles and runs through
/// every API; the memory is exported under a reserved name for the accounting only, and that
/// export is invisible to the guest.
#[test]
fn b5_1_a_module_hiding_its_memory_runs_and_the_added_export_is_invisible() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = format!(
            "export default async () => {{
  const bytes = new Uint8Array({WASM_HIDDEN});
  const sync = new WebAssembly.Instance(new WebAssembly.Module(bytes));
  const compiled = await WebAssembly.compile(bytes);
  const viaModule = await WebAssembly.instantiate(compiled);
  const {{ module, instance }} = await WebAssembly.instantiate(bytes);
  return Response.json({{
    grown: [sync.exports.grow(1), viaModule.exports.grow(2), instance.exports.grow(1), sync.exports.grow(0)],
    exports: [Object.keys(sync.exports), Object.keys(viaModule.exports), Object.keys(instance.exports)],
    moduleExports: WebAssembly.Module.exports(module).map((e) => e.name + ':' + e.kind),
    ownKeys: Object.keys(sync),
    frozen: Object.isFrozen(sync.exports),
    isInstance: sync instanceof WebAssembly.Instance && module instanceof WebAssembly.Module,
  }});
}};
"
        );
        let v = read(run_ts(&mut h, 128, &src).await).await.json();
        assert_eq!(
            v,
            serde_json::json!({
                "grown": [1, 1, 1, 2],
                "exports": [["grow"], ["grow"], ["grow"]],
                "moduleExports": ["grow:function"],
                "ownKeys": [],
                "frozen": true,
                "isInstance": true,
            })
        );
    });
}

#[test]
fn b5_1_a_hidden_memory_grown_inside_the_module_past_the_limit_is_oom() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = format!(
            "export default () => {{
  const inst = new WebAssembly.Instance(new WebAssembly.Module(new Uint8Array({WASM_HIDDEN})));
  for (let i = 0; i < 3; i++) inst.exports.grow(512);
  let x = 0;
  for (;;) x++;
}};
"
        );
        let started = Instant::now();
        let outcome = run_ts(&mut h, 64, &src).await;
        assert!(matches!(outcome, Outcome::Killed(KillReason::Oom)), "got {outcome:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "killed by the memory check, not a later limit"
        );
        assert_node_recovers(&mut h).await;
    });
}

#[test]
fn b5_1_a_hidden_memory_of_a_module_without_exports_is_counted() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        // Under the limit: the module instantiates and the task answers.
        let small = "export default async () => {
  const m = await WebAssembly.compile(new Uint8Array([0,97,115,109,1,0,0,0, 5,3,1,0,1]));
  const i = new WebAssembly.Instance(m);
  return Response.json({ keys: Object.keys(i.exports), exports: WebAssembly.Module.exports(m).length });
};
";
        let v = read(run_ts(&mut h, 128, small).await).await.json();
        assert_eq!(v, serde_json::json!({"keys": [], "exports": 0}));
        // The start function grows the hidden memory to 96 MiB: over a 64 MiB task.
        let src = format!(
            "export default async () => {{
  await WebAssembly.instantiate(new Uint8Array({WASM_HIDDEN_START_GROW}));
  return new Response('fits');
}};
"
        );
        let outcome = run_ts(&mut h, 64, &src).await;
        assert!(matches!(outcome, Outcome::Killed(KillReason::Oom)), "got {outcome:?}");
        assert_node_recovers(&mut h).await;
    });
}

/// Probe (B5.1): the `--wasm-max-mem-pages` backstop (128 MiB with the test config) against
/// declared maximums and growth. Run with `--ignored --nocapture` and read the printed result.
#[test]
#[ignore]
fn b5_1_probe_wasm_backstop() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = format!(
            "export default async () => {{
  const out: Record<string, unknown> = {{}};
  const tryIt = (k: string, f: () => unknown) => {{ try {{ out[k] = f(); }} catch (e) {{ out[k] = String(e); }} }};
  tryIt('jsMax4GiB', () => new WebAssembly.Memory({{ initial: 1, maximum: 65536 }}).buffer.byteLength);
  tryIt('moduleMax4GiB', () => new WebAssembly.Instance(new WebAssembly.Module(new Uint8Array({WASM_BIG_MAX}))).exports.mem.buffer.byteLength);
  tryIt('grow200MiB', () => {{ const m = new WebAssembly.Memory({{ initial: 1 }}); m.grow(3200); return m.buffer.byteLength; }});
  tryIt('initial200MiB', () => new WebAssembly.Memory({{ initial: 3200 }}).buffer.byteLength);
  return Response.json(out);
}};
"
        );
        match run_ts(&mut h, 512, &src).await {
            Outcome::Ok(r) => println!("wasm backstop probe: {}", read(Outcome::Ok(r)).await.text()),
            other => println!("wasm backstop probe: outcome {other:?}"),
        }
    });
}

/// Probe (B5.1 finding): which V8 heap statistics move when JS grows a `WebAssembly.Memory`, touches
/// it, or allocates an `ArrayBuffer`. Plain `deno_core` runtime, no executor (the process-wide
/// `--wasm-max-mem-pages` backstop applies: 128 MiB per memory). Run with `--ignored --nocapture`.
#[test]
#[ignore]
fn b5_1_probe_v8_heap_statistics_for_wasm_memory() {
    rt_exec_deno::init_v8_once(&cfg0());
    run_local(async {
        let mut js = deno_core::JsRuntime::new(Default::default());
        let show = |js: &mut deno_core::JsRuntime, label: &str, code: &str| {
            js.execute_script("probe", code.to_string()).expect("script");
            let s = js.v8_isolate().get_heap_statistics();
            println!(
                "{label:<40} used_heap={:>6}K total_heap={:>6}K external={:>8}K malloced={:>6}K physical={:>6}K",
                s.used_heap_size() / 1024,
                s.total_heap_size() / 1024,
                s.external_memory() / 1024,
                s.malloced_memory() / 1024,
                s.total_physical_size() / 1024
            );
        };
        show(&mut js, "start", "0");
        show(
            &mut js,
            "Memory initial 1 page (max 1 GiB)",
            "globalThis.m = new WebAssembly.Memory({ initial: 1, maximum: 16385 })",
        );
        show(&mut js, "grow +64 MiB (untouched)", "m.grow(1024)");
        show(&mut js, "touch all 64 MiB", "new Uint8Array(m.buffer).fill(1)");
        show(&mut js, "grow +32 MiB more (untouched)", "m.grow(512)");
        show(
            &mut js,
            "ArrayBuffer 64 MiB",
            "globalThis.ab = new ArrayBuffer(64 << 20)",
        );
        show(&mut js, "drop both + gc", "m = null; ab = null; 0");
        js.v8_isolate().low_memory_notification();
        show(&mut js, "after low_memory_notification", "0");
        show(
            &mut js,
            "module memory: memory.grow 64 MiB in wasm",
            &format!("globalThis.inst = new WebAssembly.Instance(new WebAssembly.Module(new Uint8Array({WASM_GROW}))); inst.exports.grow(1024)"),
        );
        show(&mut js, "drop inst + gc", "inst = null; 0");
        js.v8_isolate().low_memory_notification();
        show(&mut js, "after gc", "0");
        show(
            &mut js,
            "core.encode 64 MiB (op-made buffer)",
            "globalThis.e = Deno.core.encode('x'.repeat(64 << 20)); 0",
        );
        show(&mut js, "drop encoded + gc", "e = null; 0");
        js.v8_isolate().low_memory_notification();
        show(&mut js, "after gc", "0");
        show(
            &mut js,
            "resizable AB max 1 GiB, resize+fill 64 MiB",
            "globalThis.r = new ArrayBuffer(0, { maxByteLength: 1 << 30 }); r.resize(64 << 20); new Uint8Array(r).fill(1); 0",
        );
    });
}

// ---------------------------------------------------------------------------------------------
// D-A2 review: memory the counting allocator never sees
// ---------------------------------------------------------------------------------------------

#[test]
fn d_a2_a_resizable_buffer_cannot_grow_past_the_task_limit() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        // 16 GiB of reservation is only address space; growing it is admitted against 64 MiB.
        let src = "export default () => {
  const b = new ArrayBuffer(0, { maxByteLength: 16 * 2 ** 30 });
  const out: Record<string, unknown> = {};
  try { b.resize(16 * 2 ** 30); new Uint8Array(b).fill(1); out.resize = 'grew'; } catch (e) { out.resize = (e as Error).name; }
  try { new ArrayBuffer(2 ** 30, { maxByteLength: 2 ** 30 }); out.ctor = 'made'; } catch (e) { out.ctor = (e as Error).name; }
  try { b.transfer(2 ** 30); out.transfer = 'grew'; } catch (e) { out.transfer = (e as Error).name; }
  const tricky = { n: 0, valueOf() { return this.n++ === 0 ? 1 : 2 ** 34; } };
  try { b.resize(tricky as unknown as number); out.valueOf = b.byteLength; } catch (e) { out.valueOf = (e as Error).name; }
  b.resize(8 << 20);
  new Uint8Array(b).fill(1);
  out.len = b.byteLength;
  out.ctorWrapped = Object.getPrototypeOf(b).constructor === ArrayBuffer && b instanceof ArrayBuffer;
  return Response.json(out);
};
";
        let v = read(run_ts(&mut h, 64, src).await).await.json();
        assert_eq!(
            v,
            serde_json::json!({"resize": "RangeError", "ctor": "RangeError", "transfer": "RangeError",
                "valueOf": 1, "len": 8 << 20, "ctorWrapped": true})
        );
        assert_node_recovers(&mut h).await;
    });
}

#[test]
fn d_a2_resizable_buffers_count_together_and_an_uncaught_refusal_is_oom() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        // Each 16 MiB growth fits on its own; together they pass 64 MiB.
        let src = "export default () => {
  const keep = [];
  for (let i = 0; i < 16; i++) {
    const b = new ArrayBuffer(0, { maxByteLength: 64 << 20 });
    b.resize(16 << 20);
    new Uint8Array(b).fill(1);
    keep.push(b);
  }
  return new Response(String(keep.length));
};
";
        let outcome = run_ts(&mut h, 64, src).await;
        assert!(matches!(outcome, Outcome::Killed(KillReason::Oom)), "got {outcome:?}");
        assert_node_recovers(&mut h).await;
    });
}

#[test]
fn d_a2_op_made_buffers_count_text_encoder_and_kv_values() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        // `TextEncoder.encode` returns a buffer made by an op, outside the allocator.
        let encode = "export default () => {
  const s = 'x'.repeat(8 << 20);
  const keep = [];
  for (let i = 0; i < 64; i++) keep.push(new TextEncoder().encode(s));
  return new Response(String(keep.length));
};
";
        let outcome = run_ts(&mut h, 64, encode).await;
        assert!(
            matches!(outcome, Outcome::Killed(KillReason::Oom)),
            "encode: got {outcome:?}"
        );
        assert_node_recovers(&mut h).await;
        // A KV value read back is an op result too.
        let kv = "import { kv } from 'std:kv';
export default async () => {
  await kv.set('big', new Uint8Array(1_000_000));
  const keep = [];
  for (let i = 0; i < 200; i++) keep.push(await kv.get('big'));
  return new Response(String(keep.length));
};
";
        let outcome = run_ts(&mut h, 64, kv).await;
        assert!(
            matches!(outcome, Outcome::Killed(KillReason::Oom)),
            "kv: got {outcome:?}"
        );
        assert_node_recovers(&mut h).await;
    });
}

#[test]
fn d_a2_blob_parts_count_against_the_task() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = "export default () => {
  const data = new Uint8Array(8 << 20);
  const keep = [];
  for (let i = 0; i < 64; i++) keep.push(new Blob([data]));
  return new Response(String(keep.length));
};
";
        let outcome = run_ts(&mut h, 64, src).await;
        assert!(matches!(outcome, Outcome::Killed(KillReason::Oom)), "got {outcome:?}");
        assert_node_recovers(&mut h).await;
        // A few small blobs are fine.
        let small = "export default async () => new Response(await new Blob(['a', 'b']).text());\n";
        assert_eq!(read(run_ts(&mut h, 64, small).await).await.text(), "ab");
    });
}

#[test]
fn d_a2_a_tampered_length_getter_cannot_hide_a_wasm_memory() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let src = format!(
            "export default () => {{
  Object.defineProperty(Object.getPrototypeOf(Uint8Array.prototype), 'length', {{ get() {{ return 8; }} }});
  const inst = new WebAssembly.Instance(new WebAssembly.Module(new Uint8Array({WASM_HIDDEN})));
  for (let i = 0; i < 3; i++) inst.exports.grow(512);
  return new Response('grew unseen');
}};
"
        );
        let outcome = run_ts(&mut h, 64, &src).await;
        assert!(matches!(outcome, Outcome::Killed(KillReason::Oom)), "got {outcome:?}");
        assert_node_recovers(&mut h).await;
    });
}

#[test]
fn d_a2_collected_wasm_memories_stop_counting() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        // 400 x 1 MiB, never more than one alive: only the live ones count.
        let src = "export default () => {
  let n = 0;
  for (let i = 0; i < 400; i++) n += new WebAssembly.Memory({ initial: 16 }).buffer.byteLength >> 20;
  return new Response(String(n));
};
";
        assert_eq!(read(run_ts(&mut h, 64, src).await).await.text(), "400");
        assert_node_recovers(&mut h).await;
    });
}

#[test]
fn a_killed_stream_reaches_the_client_as_an_error_not_a_short_body() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h
            .prepare(&ts_upload(
                "export default () => {
  const enc = new TextEncoder();
  let sent = false;
  return new Response(new ReadableStream({ async pull(c) {
    if (!sent) { sent = true; c.enqueue(enc.encode('first')); return; }
    await new Promise((r) => setTimeout(r, 10));
    for (;;) {}
  } }));
};
",
            ))
            .await;
        let ran = h.run(spec().cpu_limit(100).http_get("/").finish(), &p).await;
        let Outcome::Ok(reply) = ran.outcome else {
            panic!("the head was sent: {:?}", ran.outcome)
        };
        let body = reply.body.collect(MIB).await;
        assert!(
            body.is_err(),
            "a CPU kill after the head must abort the body, got {body:?}"
        );
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-16 disposal off the response path
// ---------------------------------------------------------------------------------------------

fn exec_ms(server_timing: &str) -> f64 {
    server_timing
        .split(',')
        .find_map(|p| p.trim().strip_prefix("exec;dur="))
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("no exec;dur in `{server_timing}`"))
}

#[test]
fn ac_dno_16_disposal_happens_on_idle_not_in_the_response() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.warm_up().await;
        assert_eq!(h.exec.dispose_backlog_len(), 0);
        assert_eq!(h.gov.committed(), 0);
        let p = h.prepare_fixture("hello").await;

        let ran = h.run(get("/"), &p).await;
        let at_response = ran.took;
        let got = read(ran.outcome).await;
        h.settle(Duration::from_millis(30)).await;

        // `server-timing` covers queue + wait + init + exec, never the disposal: it cannot exceed
        // the wall time it took to produce the response.
        let st = got.timings.server_timing();
        assert!(
            got.timings.total() <= at_response + Duration::from_millis(1),
            "{st} vs {at_response:?}"
        );
        assert!(exec_ms(&st) >= 0.0);

        // The finished isolate is parked in the dispose queue with its memory still reserved...
        assert_eq!(h.exec.dispose_backlog_len(), 1);
        assert!(h.gov.committed() > 0, "reservation is released only after disposal");

        // ...and goes away when the Machine is idle.
        h.exec.on_idle(Duration::from_millis(200));
        assert_eq!(h.exec.dispose_backlog_len(), 0);
        assert_eq!(
            h.gov.committed(),
            0,
            "committed memory returns to baseline after disposal"
        );
    });
}

#[test]
fn ac_dno_16_backlog_beyond_the_limit_is_disposed_inline() {
    run_local(async {
        let cfg = rt_exec_deno::DenoExecutorConfig {
            dispose_backlog: 2,
            ..cfg0()
        };
        let mut h = Harness::new(cfg);
        h.warm_up().await;
        let p = h.prepare_fixture("hello").await;

        for i in 0..6 {
            let (got, _) = h.get("/", &p).await;
            assert_eq!(got.text(), "hello");
            h.settle(Duration::from_millis(20)).await;
            assert!(
                h.exec.dispose_backlog_len() <= 2,
                "after task {i} the backlog is {} (> deno.dispose_backlog = 2): disposal must run inline",
                h.exec.dispose_backlog_len()
            );
        }
        assert_eq!(
            h.exec.dispose_backlog_len(),
            2,
            "idle disposal has not run, the queue sits at its cap"
        );
        // Inline disposal released the rest: only the queued isolates still hold memory.
        assert!(
            h.gov.committed() <= 2 * u64::from(h.cfg.isolate_base_mb) * MIB * 4,
            "committed {}",
            h.gov.committed()
        );

        h.exec.on_idle(Duration::from_millis(200));
        assert_eq!(h.exec.dispose_backlog_len(), 0);
        assert_eq!(h.gov.committed(), 0);
    });
}

#[test]
fn dno_16_on_idle_respects_its_time_budget() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.warm_up().await;
        let p = h.prepare_fixture("hello").await;
        for _ in 0..4 {
            let _ = h.get("/", &p).await;
            h.settle(Duration::from_millis(10)).await;
        }
        let t0 = Instant::now();
        h.exec.on_idle(Duration::from_millis(50));
        assert!(
            t0.elapsed() < Duration::from_millis(250),
            "on_idle must return within its budget, took {:?}",
            t0.elapsed()
        );
    });
}

/// Security review pass 2: a response with more distinct header names than `http::HeaderMap`
/// holds used to panic inside the op (aborting the node). Past the cap it is a guest error.
const MANY_HEADERS: &str = "export default (req) => {\n\
  const n = Number(new URL(req.url).searchParams.get('n'));\n\
  const h = new Headers();\n\
  for (let i = 0; i < n; i++) h.append('h' + i, '1');\n\
  return new Response('x', { headers: h });\n\
};\n";

#[test]
fn a_response_with_too_many_headers_is_a_guest_error_not_a_crash() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(MANY_HEADERS)).await;
        // (the runtime adds `content-type` to a string body)
        let n = rt_exec_deno::MAX_RESPONSE_HEADERS - 1;
        let (ok, _) = h.get(&format!("/?n={n}"), &p).await;
        assert_eq!(ok.status, 200);
        assert_eq!(ok.headers.len(), rt_exec_deno::MAX_RESPONSE_HEADERS);
        let ran = h
            .run(get(&format!("/?n={}", rt_exec_deno::MAX_RESPONSE_HEADERS + 1)), &p)
            .await;
        assert!(matches!(ran.outcome, Outcome::GuestError), "over the cap");
        let ran = h.run(get("/?n=30000"), &p).await;
        // (a guest error, or killed on its CPU limit while building the headers; never a panic)
        assert!(
            !matches!(ran.outcome, Outcome::Ok(_)),
            "far over the cap, without a panic"
        );
    });
}
