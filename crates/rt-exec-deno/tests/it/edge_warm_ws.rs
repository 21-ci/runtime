//! Warm-reuse and outbound-WebSocket edge cases of the Deno executor (scratchpad edge-cases.md):
//! prototype pollution across reused tasks, host error stacks, timers, memory limits, sockets
//! left open in a kept isolate, oversized frames and the egress quota.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::common;

use async_trait::async_trait;
use common::*;
use futures_util::{SinkExt, StreamExt};
use rt_host::{EgressGuardConfig, GuardedEgress, Resolver, TenantPolicy};
use rt_types::{KillReason, Outcome, ServiceId, ServiceKnobs, TenantId};
use serde_json::json;

fn reuse() -> rt_testkit::SpecBuilder {
    spec().knobs(ServiceKnobs {
        reuse_isolates: true,
        ..ServiceKnobs::default()
    })
}

// ---------------------------------------------------------------------------------------------
// Warm reuse
// ---------------------------------------------------------------------------------------------

const POLLUTER: &str = "import { kv } from 'std:kv';\n\
export default async (req: Request, ctx: any) => {\n\
  const u = new URL(req.url);\n\
  if (u.pathname === '/pollute') {\n\
    Array.prototype.push = function () { throw new Error('polluted push'); };\n\
    JSON.parse = () => ({ polluted: true });\n\
    JSON.stringify = () => '\"polluted\"';\n\
    Object.prototype.polluted = true;\n\
    (Response.prototype as any).json = () => { throw new Error('polluted json'); };\n\
    Promise.prototype.then = function () { throw new Error('polluted then'); };\n\
    return new Response('polluted');\n\
  }\n\
  await kv.set('k', { a: 1 });\n\
  const v = await kv.get('k');\n\
  const body = await req.text();\n\
  return new Response(`trigger=${ctx.trigger} v=${typeof v === 'object' ? (v as any).a : v} body=${body}`);\n\
};\n";

/// SBX-004: a task that pollutes built-in prototypes in a reused isolate cannot break the
/// runtime for the next task: its request, `ctx` and std:* calls still work (the runtime keeps
/// its own copies of the built-ins). What the guest itself sees stays polluted: its own state.
#[test]
fn sbx_004_prototype_pollution_does_not_break_the_runtime_in_the_next_task() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(POLLUTER)).await;
        let ran = h.run(reuse().http_get("/pollute").finish(), &p).await;
        assert_eq!(read(ran.outcome).await.text(), "polluted");
        h.settle(Duration::from_millis(20)).await;
        let payload = post_payload("/", "text/plain", "hello");
        let ran = h.run(reuse().payload(payload).finish(), &p).await;
        match ran.outcome {
            Outcome::Ok(_) => {
                let got = read(ran.outcome).await;
                assert_eq!(got.text(), "trigger=http v=1 body=hello");
            }
            // The polluted isolate may also be discarded instead (a failed or unclean task).
            other => panic!(
                "the next task failed in a polluted isolate: {other:?} {:?}",
                h.log_records().iter().map(|r| r.msg.clone()).collect::<Vec<_>>()
            ),
        }
    });
}

const STACKS: &str = "import { kv } from 'std:kv';\n\
export default async () => {\n\
  (Error as any).prepareStackTrace = (_e: any, frames: any[]) => frames.map((f) => `${f.getFileName()}:${f.getFunctionName()}`).join('|');\n\
  let stack = '';\n\
  try { await kv.get(''); } catch (e) { (Error as any).captureStackTrace(e); stack = String((e as any).stack); }\n\
  return new Response(stack);\n\
};\n";

/// SBX-006: rewriting stack traces of a host error shows only guest and runtime-module frames:
/// no host file paths, Rust frames or other services' modules.
#[test]
fn sbx_006_host_error_stacks_reveal_no_host_paths() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(STACKS)).await;
        let (got, _) = h.get("/", &p).await;
        let stack = got.text();
        for leak in ["/Users/", "/home/", "/root/", ".rs", "crates/", "target/", "rt-exec"] {
            assert!(!stack.contains(leak), "`{leak}` in {stack}");
        }
    });
}

const CLOCK: &str = "export default async () => {\n\
  const sab = typeof SharedArrayBuffer;\n\
  let wait = 'none';\n\
  try { (Atomics as any).wait(new Int32Array(new ArrayBuffer(4)), 0, 0, 1); wait = 'ran'; } catch (e) { wait = (e as Error).name; }\n\
  const a = performance.now();\n\
  let spins = 0; while (performance.now() === a && spins < 1e6) spins++;\n\
  return Response.json({ sab, wait, frozenInSyncCode: performance.now() === a });\n\
};\n";

/// DCLK-002: no high-resolution timer can be built: `SharedArrayBuffer` is gone, `Atomics.wait`
/// does not run, and the clock does not move within synchronous code (a spin loop cannot count).
#[test]
fn dclk_002_no_high_resolution_timer_from_shared_memory_or_spinning() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(CLOCK)).await;
        let (got, _) = h.get("/", &p).await;
        let v = got.json();
        assert_eq!(v["sab"], json!("undefined"));
        assert_ne!(v["wait"], json!("ran"), "{v}");
        assert_eq!(v["frozenInSyncCode"], json!(true), "{v}");
    });
}

const MEMORY: &str = "export default (req: Request) => {\n\
  const mb = Number(new URL(req.url).searchParams.get('mb'));\n\
  try { const b = new Uint8Array(mb * 1024 * 1024); b[b.length - 1] = 1; return Response.json({ ok: b.length }); }\n\
  catch (e) { return Response.json({ error: (e as Error).name }); }\n\
};\n";

/// DMEM-003: a buffer well under `memory_mb` is allocated; one past the limit is refused (a
/// `RangeError` the guest can catch, or the task is killed `oom`), never more memory than allowed.
#[test]
fn dmem_003_allocations_past_the_memory_limit_are_refused() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(MEMORY)).await;
        let mut s = get("/?mb=16");
        s.limits.memory_mb = 64;
        let ran = h.run(s, &p).await;
        assert_eq!(read(ran.outcome).await.json(), json!({ "ok": 16 * 1024 * 1024 }));
        let mut s = get("/?mb=65");
        s.limits.memory_mb = 64;
        let ran = h.run(s, &p).await;
        match ran.outcome {
            Outcome::Killed(KillReason::Oom) => {}
            Outcome::Ok(_) => assert_eq!(read(ran.outcome).await.json(), json!({ "error": "RangeError" })),
            other => panic!("{other:?}"),
        }
    });
}

const SECRET_GLOBAL: &str = "import { secrets } from 'std:secrets';\n\
export default (req: Request) => {\n\
  const g = globalThis as any;\n\
  if (new URL(req.url).pathname === '/load') { g.kept = secrets.get('TOKEN'); return new Response('loaded'); }\n\
  console.log('kept=' + g.kept);\n\
  return new Response('logged');\n\
};\n";

/// OBS-004 (pinned, SECURITY.md "Secrets"): redaction applies to tasks that loaded the env; a
/// later task of a reused isolate that never reads env but logs a secret kept in a module global
/// is redacted too, because the reused isolate's env was loaded (same secrets version).
#[test]
fn obs_004_a_secret_kept_in_a_warm_global_is_still_redacted_in_logs() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[], &[("TOKEN", "tok-canary-1234567")]);
        let p = h.prepare(&ts_upload(SECRET_GLOBAL)).await;
        let ran = h.run(reuse().http_get("/load").finish(), &p).await;
        assert_eq!(read(ran.outcome).await.text(), "loaded");
        h.settle(Duration::from_millis(20)).await;
        let ran = h.run(reuse().http_get("/log").finish(), &p).await;
        assert_eq!(read(ran.outcome).await.text(), "logged");
        let logs: Vec<String> = h.log_records().into_iter().map(|r| r.msg).collect();
        let line = logs.iter().find(|m| m.starts_with("kept=")).expect("the log line");
        assert!(!line.contains("tok-canary-1234567"), "{line}");
    });
}

// ---------------------------------------------------------------------------------------------
// Outbound WebSockets
// ---------------------------------------------------------------------------------------------

struct EchoDns;

#[async_trait]
impl Resolver for EchoDns {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<IpAddr>> {
        match host {
            "echo.test" => Ok(vec![IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))]),
            // A dual-stack host whose IPv6 address is unreachable (EGR-006).
            "dual.test" => Ok(vec![
                "2001:db8::1".parse().expect("v6"),
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 8)),
            ]),
            _ => Err(std::io::Error::other("no such host")),
        }
    }
}

fn egress(cfg: EgressGuardConfig) -> Arc<GuardedEgress> {
    let denylist = Arc::new(rt_sec::SelfDenylist::new(vec![], vec![]));
    Arc::new(
        GuardedEgress::new(cfg, denylist)
            .with_resolver(Arc::new(EchoDns))
            .with_connect_rewrite(Arc::new(|ip: IpAddr| match ip {
                IpAddr::V4(a) if a.octets()[..3] == [203, 0, 113] => IpAddr::V4(Ipv4Addr::LOCALHOST),
                other => other,
            })),
    )
}

/// A WebSocket server on loopback: `/echo` echoes, `/big` sends one message of `big` bytes,
/// `/silent` never closes. `open` counts connections still open.
async fn ws_server(big: usize, open: Arc<AtomicUsize>) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            let open = open.clone();
            tokio::spawn(async move {
                let path = Arc::new(std::sync::Mutex::new(String::new()));
                let seen = path.clone();
                #[allow(clippy::result_large_err, reason = "the tungstenite callback signature")]
                let cb = move |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp| {
                    *seen.lock().unwrap() = req.uri().path().to_string();
                    Ok(resp)
                };
                let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(s, cb).await else {
                    return;
                };
                open.fetch_add(1, Ordering::SeqCst);
                let p = path.lock().unwrap().clone();
                if p == "/big" {
                    let _ = ws
                        .send(tokio_tungstenite::tungstenite::Message::binary(vec![7u8; big]))
                        .await;
                }
                while let Some(Ok(m)) = ws.next().await {
                    if p == "/echo" && (m.is_text() || m.is_binary()) {
                        let _ = ws.send(m).await;
                    }
                }
                open.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    port
}

const WS_GUEST: &str = "export default async (req: Request) => {\n\
  const u = new URL(req.url);\n\
  const port = u.searchParams.get('port');\n\
  const ws = new WebSocket(`ws://${u.searchParams.get('host') ?? 'echo.test'}:${port}${u.pathname}`);\n\
  ws.binaryType = 'arraybuffer';\n\
  const events: string[] = [];\n\
  const done = await new Promise((resolve) => {\n\
    ws.onopen = () => { events.push('open'); if (u.pathname === '/echo') ws.send('x'.repeat(Number(u.searchParams.get('n') ?? 1))); if (u.pathname === '/silent') resolve('left-open'); };\n\
    ws.onmessage = (e) => { events.push('message:' + (typeof e.data === 'string' ? e.data.length : (e.data as ArrayBuffer).byteLength)); if (u.searchParams.has('loop')) ws.send('again'); else if (u.pathname === '/echo') ws.close(1000); };\n\
    ws.onerror = (e: any) => { events.push('error:' + (e.code ?? '')); };\n\
    ws.onclose = (e) => { events.push('close:' + e.code); resolve('closed'); };\n\
    setTimeout(() => resolve('timeout'), 5000);\n\
  });\n\
  return Response.json({ done, events });\n\
};\n";

/// DWS-002: a message over the client's 16 MiB limit is not delivered: the socket closes with an
/// error and the task finishes (bounded memory, no hang).
#[test]
fn dws_002_an_oversized_inbound_message_closes_the_socket() {
    run_local(async {
        let port = ws_server(16 * 1024 * 1024 + 1, Arc::default()).await;
        let mut h = Harness::with_egress(cfg0(), egress(EgressGuardConfig::default()));
        let p = h.prepare(&ts_upload(WS_GUEST)).await;
        let (got, _) = h.get(&format!("/big?port={port}"), &p).await;
        let v = got.json();
        assert_eq!(v["done"], json!("closed"), "{v}");
        let events = v["events"].as_array().expect("events");
        assert!(
            !events
                .iter()
                .any(|e| e.as_str().is_some_and(|s| s.starts_with("message:"))),
            "{v}"
        );
        assert!(
            events
                .iter()
                .any(|e| e.as_str().is_some_and(|s| s.starts_with("close:"))),
            "{v}"
        );
    });
}

/// DWARM-003 (SECURITY.md: "it closes when the task ends, also in a warm isolate that is kept"):
/// an outbound socket a task leaves open is closed when the task ends, reuse on or off.
#[test]
fn dwarm_003_a_socket_left_open_is_closed_at_task_end_in_a_kept_isolate() {
    run_local(async {
        let open = Arc::new(AtomicUsize::new(0));
        let port = ws_server(0, open.clone()).await;
        let mut h = Harness::with_egress(cfg0(), egress(EgressGuardConfig::default()));
        let p = h.prepare(&ts_upload(WS_GUEST)).await;
        let ran = h
            .run(reuse().http_get(&format!("/silent?port={port}")).finish(), &p)
            .await;
        assert_eq!(read(ran.outcome).await.json()["done"], json!("left-open"));
        for _ in 0..100 {
            if open.load(Ordering::SeqCst) == 0 {
                break;
            }
            h.settle(Duration::from_millis(20)).await;
        }
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "the server saw the socket close after the task (kept isolates: {})",
            h.exec.warm_len()
        );
    });
}

struct EgressLeft(u64);

impl TenantPolicy for EgressLeft {
    fn egress_default_deny(&self, _: TenantId) -> bool {
        false
    }
    fn egress_bytes_left(&self, _: TenantId) -> Option<u64> {
        Some(self.0)
    }
    fn storage_bytes_left(&self, _: TenantId) -> Option<u64> {
        None
    }
    fn record_egress(&self, _: TenantId, _: ServiceId, _: u64) {}
}

/// WSC-002: bytes received on an outbound WebSocket count against the daily egress quota; once it
/// is spent the socket closes with `egress_quota` instead of delivering more.
#[test]
fn wsc_002_outbound_websocket_bytes_count_against_the_egress_quota() {
    run_local(async {
        let port = ws_server(0, Arc::default()).await;
        let mut h = Harness::with_deps(cfg0(), |d| {
            d.egress = egress(EgressGuardConfig::default());
            d.policy = Some(Arc::new(EgressLeft(100)));
        });
        let p = h.prepare(&ts_upload(WS_GUEST)).await;
        let (got, _) = h.get(&format!("/echo?port={port}&n=60&loop=1"), &p).await;
        let v = got.json();
        let events: Vec<String> = v["events"]
            .as_array()
            .expect("events")
            .iter()
            .map(|e| e.as_str().unwrap_or("").to_string())
            .collect();
        let received: u64 = events
            .iter()
            .filter_map(|e| e.strip_prefix("message:"))
            .map(|n| n.parse::<u64>().expect("size"))
            .sum();
        assert!(received <= 100, "never more than the 100 bytes left: {received} {v}");
        assert!(
            events.iter().any(|e| e.contains("egress_quota")),
            "the quota stops the socket: {v}"
        );
    });
}

/// EGR-006: a dual-stack host whose first address (IPv6) is unreachable is still reached over
/// IPv4 within the connect timeout.
#[test]
fn egr_006_an_unreachable_ipv6_falls_back_to_ipv4() {
    run_local(async {
        let port = ws_server(0, Arc::default()).await;
        let cfg = EgressGuardConfig {
            connect_timeout: Duration::from_secs(3),
            ..EgressGuardConfig::default()
        };
        let mut h = Harness::with_egress(cfg0(), egress(cfg));
        let p = h.prepare(&ts_upload(WS_GUEST)).await;
        let (got, _) = h.get(&format!("/echo?port={port}&host=dual.test&n=3"), &p).await;
        let v = got.json();
        assert!(
            v["events"].as_array().expect("events").contains(&json!("message:3")),
            "{v}"
        );
    });
}
