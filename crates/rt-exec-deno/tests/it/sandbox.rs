//! What a tenant can and cannot reach: DNO-03 (forbidden APIs), DNO-04 (env + secrets),
//! DNO-05 (SSRF through `fetch`), SEC-23 (banned globals and runtime imports, Deno side).
//!
//! `fixtures/deno/forbidden_apis` reports every probe as `undefined`, `threw:<msg>` or
//! `allowed:<type>`; only the first two are acceptable.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use rt_host::{EgressGuardConfig, GuardedEgress};
use serde_json::Value;

async fn run_probes(h: &mut Harness) -> Value {
    let p = h.prepare_fixture("forbidden_apis").await;
    let (got, _) = h.get("/", &p).await;
    assert_eq!(got.status, 200, "the probing handler must itself run: {}", got.text());
    got.json()
}

fn probe(v: &Value, name: &str) -> String {
    v["probes"][name]
        .as_str()
        .unwrap_or_else(|| panic!("probe `{name}` missing from {}", v["probes"]))
        .to_string()
}

fn denied(result: &str) -> bool {
    result == "undefined" || result.starts_with("threw:")
}

// ---------------------------------------------------------------------------------------------
// DNO-03
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_03_forbidden_apis_are_unavailable_or_throw() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = run_probes(&mut h).await;

        for name in ["Deno.readFile", "Deno.connect", "Deno.Command", "Worker"] {
            let r = probe(&v, name);
            assert!(denied(&r), "`{name}` must be undefined or throw, got `{r}`");
        }
        // Runtime imports of anything outside the bundle fail with `module_not_in_bundle`.
        for name in ["import(node:fs)", "import(https)", "import(file)"] {
            let r = probe(&v, name);
            assert!(r.starts_with("threw:"), "`{name}` must throw, got `{r}`");
            assert!(
                r.contains("module_not_in_bundle"),
                "`{name}` should fail with module_not_in_bundle, got `{r}`"
            );
        }
    });
}

#[test]
fn ac_dno_03_nothing_touches_host_fs_or_network() {
    run_local(async {
        // A listener the guest's `Deno.connect` / `WebSocket` would hit if they worked.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let mut h = Harness::new(cfg0());
        let v = run_probes(&mut h).await;
        for (name, r) in v["probes"].as_object().expect("probes") {
            assert!(
                !r.as_str().unwrap_or("").starts_with("allowed:"),
                "`{name}` escaped the sandbox: {r}"
            );
        }
        let accepted = tokio::time::timeout(Duration::from_millis(100), listener.accept()).await;
        assert!(accepted.is_err(), "no guest connection may ever reach the host network");
    });
}

// ---------------------------------------------------------------------------------------------
// SEC-23 (Deno half; the WASM threads-proposal rejection is in rt-build's validation tests)
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_sec_23_banned_globals_and_apis_are_undefined_or_throwing() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = run_probes(&mut h).await;

        for name in [
            "Worker",
            "SharedArrayBuffer",
            "Atomics.wait",
            "ShadowRealm",
            "Deno.run",
            "Deno.openKv",
            "Deno.readFile",
            "Deno.connect",
            "Deno.Command",
            "WebSocket(loopback)",
            "caches",
        ] {
            let r = probe(&v, name);
            assert!(denied(&r), "SEC-23: `{name}` must be undefined or throw, got `{r}`");
        }
    });
}

#[test]
fn ac_sec_23_import_meta_gives_no_way_out_of_the_bundle() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = run_probes(&mut h).await;

        let r = probe(&v, "import.meta.resolve+import");
        assert!(
            r.starts_with("threw:") && r.contains("module_not_in_bundle"),
            "got `{r}`"
        );

        // `import.meta.url` is a virtual bundle URL: it must not leak host paths.
        let url = v["metaUrl"].as_str().expect("metaUrl");
        assert!(url.starts_with("file:///"), "got {url}");
        let host_dir = common::fixtures_root();
        assert!(
            !url.contains(host_dir.to_str().expect("utf-8")),
            "host path leaked: {url}"
        );
        for needle in ["/Users/", "/home/", "/private/", "target/"] {
            assert!(!url.contains(needle), "host path leaked in import.meta.url: {url}");
        }
    });
}

#[test]
fn ac_sec_23_only_the_env_shim_survives_on_deno() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = run_probes(&mut h).await;
        let keys: Vec<&str> = v["denoKeys"]
            .as_array()
            .expect("denoKeys")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert!(
            keys.iter().all(|k| *k == "env"),
            "`Deno.*` except the read-only env shim must be gone, got {keys:?}"
        );
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-04
// ---------------------------------------------------------------------------------------------

const CANARY: &str = "canary-secret-7f3a91";

#[test]
fn ac_dno_04_env_and_secrets_are_readable_and_read_only() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[("A", "1")], &[("S", CANARY)]);
        h.control.set_env(svc(2), &[("A", "2")], &[]);
        let p = h.prepare_fixture("env_secrets").await;

        let ran = h.run(spec().service(1).http_get("/").finish(), &p).await;
        let v = read(ran.outcome).await.json();
        assert_eq!(v["a"], "1");
        assert_eq!(v["s"], CANARY, "secrets are exposed through the same env shim");
        assert!(
            v["setResult"].as_str().unwrap_or("").starts_with("threw:"),
            "Deno.env.set must throw: {v}"
        );
        assert!(
            v["deleteResult"].as_str().unwrap_or("").starts_with("threw:"),
            "Deno.env.delete must throw: {v}"
        );
        assert_eq!(v["x"], Value::Null, "the failed set left no trace");
    });
}

#[test]
fn dno_04_env_is_fetched_only_when_the_guest_reads_it() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[("A", "1")], &[("S", CANARY)]);
        let hello = h.prepare_fixture("hello").await;
        let env = h.prepare_fixture("env_secrets").await;

        let (got, _) = h.get("/", &hello).await;
        assert_eq!(got.text(), "hello");
        assert_eq!(
            h.control.env_calls(),
            0,
            "a handler that never reads Deno.env costs no env fetch"
        );

        // The fixture calls get() three times plus set()/delete(): one fetch for the task.
        let v = read(h.run(spec().service(1).http_get("/").finish(), &env).await.outcome)
            .await
            .json();
        assert_eq!((v["a"].as_str(), v["s"].as_str()), (Some("1"), Some(CANARY)));
        assert_eq!(
            h.control.env_calls(),
            1,
            "fetched on first use, then reused for the rest of the task"
        );

        // `toObject()` goes through the same lazy path.
        let all = ts_upload("export default () => Response.json((globalThis as any).Deno.env.toObject());\n");
        let all = h.prepare(&all).await;
        let v = read(h.run(spec().service(1).http_get("/").finish(), &all).await.outcome)
            .await
            .json();
        assert_eq!(v["A"], "1");
        assert_eq!(h.control.env_calls(), 2);
    });
}

#[test]
fn ac_dno_04_other_services_cannot_read_the_secret() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[("A", "1")], &[("S", CANARY)]);
        h.control.set_env(svc(2), &[("A", "2")], &[]);
        let p = h.prepare_fixture("env_secrets").await;

        let first = read(h.run(spec().service(1).http_get("/").finish(), &p).await.outcome)
            .await
            .json();
        assert_eq!(first["s"], CANARY);
        let second = read(h.run(spec().service(2).http_get("/").finish(), &p).await.outcome)
            .await
            .json();
        assert_eq!(second["a"], "2");
        assert_eq!(
            second["s"],
            Value::Null,
            "service 2 has no secret S and must not see service 1's"
        );
        // And a task of service 1 afterwards still sees its own.
        let third = read(h.run(spec().service(1).http_get("/").finish(), &p).await.outcome)
            .await
            .json();
        assert_eq!(third["s"], CANARY);
    });
}

#[test]
fn dno_04_secrets_never_appear_in_logs() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[("A", "1")], &[("S", CANARY)]);
        let p = h.prepare_fixture("env_secrets").await;
        let _ = h.run(spec().service(1).http_get("/").finish(), &p).await;
        h.settle(Duration::from_millis(30)).await;
        for r in h.log_records() {
            assert!(!r.msg.contains(CANARY), "SEC-17: secret value in a log record: {r:?}");
        }
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-05
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_05_fetches_to_metadata_and_loopback_reject_with_egress_denied() {
    run_local(async {
        // Stand-in for the node's control port: any connection to it would be an SSRF success.
        let control = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind control stand-in");
        let port = control.local_addr().expect("addr").port();

        let guard = GuardedEgress::new(
            EgressGuardConfig::default(),
            Arc::new(rt_sec::SelfDenylist::new(vec![], vec![])),
        );
        let mut h = Harness::with_egress(cfg0(), Arc::new(guard));
        let p = h.prepare_fixture("egress_metadata").await;
        let (got, _) = h.get(&format!("/?port={port}"), &p).await;
        assert_eq!(got.status, 200);
        let v = got.json();

        for which in ["metadata", "control"] {
            let r = v[which].as_str().unwrap_or_default();
            assert!(r.starts_with("rejected:"), "{which}: fetch must reject, got `{r}`");
            assert!(
                r.contains("egress_denied"),
                "{which}: stable error code expected, got `{r}`"
            );
        }

        let accepted = tokio::time::timeout(Duration::from_millis(150), control.accept()).await;
        assert!(accepted.is_err(), "the guard must refuse before connecting");

        h.settle(Duration::from_millis(30)).await;
        assert!(
            h.log_records().iter().any(|r| r.msg.to_lowercase().contains("egress")),
            "an egress event is recorded in the service log: {:?}",
            h.log_records()
        );
    });
}

#[test]
fn dno_05_guest_visible_errors_do_not_leak_addresses_of_the_node() {
    run_local(async {
        let control = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = control.local_addr().expect("addr").port();
        let guard = GuardedEgress::new(
            EgressGuardConfig::default(),
            Arc::new(rt_sec::SelfDenylist::new(vec![], vec![])),
        );
        let mut h = Harness::with_egress(cfg0(), Arc::new(guard));
        let p = h.prepare_fixture("egress_metadata").await;
        let (got, _) = h.get(&format!("/?port={port}"), &p).await;
        let body = got.text();
        // The guest passed these URLs itself; what it must not see is anything about resolution.
        for needle in ["blocked", "private", "loopback", "denylist", "SSRF"] {
            assert!(
                !body.to_lowercase().contains(&needle.to_lowercase()),
                "error text leaks policy detail `{needle}`: {body}"
            );
        }
    });
}

// ---------------------------------------------------------------------------------------------
// Shared memory and atomics wait (review): nothing may park the Machine thread
// ---------------------------------------------------------------------------------------------

#[test]
fn shared_wasm_memory_is_refused_so_no_shared_array_buffer_or_atomic_wait_is_reachable() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        // `(module (memory 1 1 shared))`: limits flags 0x03.
        let src = "export default async () => {
  const out: Record<string, unknown> = {};
  try { new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }); out.ctor = 'made'; } catch (e) { out.ctor = (e as Error).name; }
  let reads = 0;
  const flip = { initial: 1, maximum: 1, get shared() { return reads++ > 0; } };
  const m = new WebAssembly.Memory(flip);
  out.flip = Object.prototype.toString.call(m.buffer);
  const bytes = new Uint8Array([0,97,115,109,1,0,0,0, 5,4,1,3,1,1]);
  try { new WebAssembly.Module(bytes); out.module = 'compiled'; } catch (e) { out.module = (e as Error).name; }
  try { await WebAssembly.compile(bytes); out.compile = 'compiled'; } catch (e) { out.compile = (e as Error).name; }
  out.sab = typeof SharedArrayBuffer;
  out.wait = typeof Atomics.wait;
  return Response.json(out);
};
";
        let p = h.prepare(&ts_upload(src)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(
            got.json(),
            serde_json::json!({"ctor": "TypeError", "flip": "[object ArrayBuffer]", "module": "CompileError",
                "compile": "CompileError", "sab": "undefined", "wait": "undefined"})
        );
    });
}

// ---------------------------------------------------------------------------------------------
// Env prefetch (review): a `Deno.env` read never parks the Machine thread, and a failed fetch
// throws instead of reading as an empty env
// ---------------------------------------------------------------------------------------------

#[test]
fn a_slow_env_fetch_is_awaited_before_dispatch_not_on_the_synchronous_read() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[("A", "1")], &[]);
        // The stub's delay is a timer of this very thread's runtime: parking the thread in the
        // synchronous read would never let it fire (the old 2 s timeout, then an empty env).
        *h.control.env_delay.lock().expect("lock") = Some(Duration::from_millis(50));
        let p = h
            .prepare(&ts_upload(
                "export default () => new Response(String(Deno.env.get('A')));\n",
            ))
            .await;
        let t0 = std::time::Instant::now();
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.text(), "1");
        assert!(t0.elapsed() < Duration::from_secs(1), "took {:?}", t0.elapsed());
    });
}

#[test]
fn a_failed_env_fetch_throws_env_unavailable() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[("A", "1")], &[]);
        h.control.env_fails.store(true, std::sync::atomic::Ordering::SeqCst);
        let p = h
            .prepare(&ts_upload(
                "export default () => {
  try { return new Response('read ' + Deno.env.get('A')); }
  catch (e) { return new Response((e as Error).message, { status: 503 }); }
};
",
            ))
            .await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!((got.status, got.text()), (503, "env_unavailable".to_string()));
    });
}

/// Security review pass 2: the reserved `/__rt/` paths and the RPC size cap hold even when the
/// guest replaced the built-ins the JS side checks them with: the host checks them too.
const RESERVED_BYPASS: &str = "import { services } from 'std:services';\n\
export default async () => {\n\
  String.prototype.startsWith = () => false;\n\
  TextEncoder.prototype.encode = function () { return new Uint8Array(0); };\n\
  const code = async (f) => { try { await f(); return 'ok'; } catch (e) { return e.code ?? String(e); } };\n\
  return Response.json({\n\
    rpcPath: await code(() => services.fetch('other', new Request('http://x/__rt/rpc/m', { method: 'POST', body: '1' }))),\n\
    plain: await code(() => services.fetch('other', 'http://x/ok')),\n\
  });\n\
};\n";

#[test]
fn reserved_call_paths_hold_without_the_guests_built_ins() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(RESERVED_BYPASS)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(
            got.json(),
            serde_json::json!({"rpcPath": "validation_failed", "plain": "not_found"}),
            "{}",
            got.text()
        );
    });
}
