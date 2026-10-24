//! Outbound WebSockets from a Deno guest (owner decision 8): `new WebSocket(url,
//! protocols)` connects through the egress guard (a public host here reaches a loopback echo
//! server through the guard's connect seam), sends text and bytes, gets `open` / `message` /
//! `close` events, and counts as one of the task's fetches.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use crate::common;

use async_trait::async_trait;
use common::*;
use futures_util::{SinkExt, StreamExt};
use rt_host::{EgressGuardConfig, GuardedEgress, Resolver};
use serde_json::json;

struct EchoDns;

#[async_trait]
impl Resolver for EchoDns {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<IpAddr>> {
        match host {
            "echo.test" => Ok(vec![IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))]),
            _ => Err(std::io::Error::other("no such host")),
        }
    }
}

type Handshake = tokio_tungstenite::tungstenite::handshake::server::Response;
type Refusal = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse;

/// Answer with the subprotocol `chat` when the client offers it.
#[allow(clippy::result_large_err, reason = "the tungstenite callback signature")]
fn choose_chat(
    req: &tokio_tungstenite::tungstenite::handshake::server::Request,
    mut resp: Handshake,
) -> Result<Handshake, Refusal> {
    if req
        .headers()
        .get("sec-websocket-protocol")
        .is_some_and(|v| v.to_str().unwrap_or("").contains("chat"))
    {
        resp.headers_mut()
            .insert("sec-websocket-protocol", "chat".parse().expect("header"));
    }
    Ok(resp)
}

/// A WebSocket echo server on loopback that answers with the subprotocol `chat` when offered.
async fn echo_server() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(s, choose_chat).await else {
                    return;
                };
                while let Some(Ok(m)) = ws.next().await {
                    if m.is_text() || m.is_binary() {
                        let _ = ws.send(m).await;
                    }
                }
            });
        }
    });
    port
}

const CLIENT: &str = "export default async function (req) {\n\
  const port = new URL(req.url).searchParams.get('port');\n\
  const ws = new WebSocket(`ws://echo.test:${port}/`, ['chat']);\n\
  ws.binaryType = 'arraybuffer';\n\
  const got = [];\n\
  const events = [];\n\
  const code = await new Promise((resolve, reject) => {\n\
    ws.addEventListener('open', () => { events.push('open'); ws.send('hello'); ws.send(new Uint8Array([1, 2, 3])); });\n\
    ws.onmessage = (e) => {\n\
      got.push(typeof e.data === 'string' ? e.data : 'bytes:' + new Uint8Array(e.data).join(','));\n\
      if (got.length === 2) ws.close(1000, 'done');\n\
    };\n\
    ws.onclose = (e) => { events.push('close'); resolve(e.code); };\n\
    ws.onerror = (e) => reject(new Error(e.code ?? 'error'));\n\
  });\n\
  return Response.json({ got, events, code, protocol: ws.protocol, readyState: ws.readyState, url: ws.url });\n\
}\n";

const REFUSED: &str = "export default async function () {\n\
  const ws = new WebSocket('ws://127.0.0.1:9/');\n\
  const r = await new Promise((resolve) => { ws.onerror = (e) => resolve('error:' + e.code); ws.onopen = () => resolve('open'); });\n\
  let bad = 'none';\n\
  try { new WebSocket('ftp://echo.test/'); } catch (e) { bad = e.name; }\n\
  return Response.json({ r, readyState: ws.readyState, bad });\n\
}\n";

fn egress() -> Arc<GuardedEgress> {
    let denylist = Arc::new(rt_sec::SelfDenylist::new(vec![], vec![]));
    Arc::new(
        GuardedEgress::new(EgressGuardConfig::default(), denylist)
            .with_resolver(Arc::new(EchoDns))
            .with_connect_rewrite(Arc::new(|ip: IpAddr| match ip {
                IpAddr::V4(a) if a.octets()[..3] == [203, 0, 113] => IpAddr::V4(Ipv4Addr::LOCALHOST),
                other => other,
            })),
    )
}

#[test]
fn ac_dno_ws_client_connects_sends_and_receives_through_the_egress_guard() {
    run_local(async {
        let port = echo_server().await;
        let mut h = Harness::with_egress(cfg0(), egress());
        let p = h.prepare(&ts_upload(CLIENT)).await;
        let (got, _) = h.get(&format!("/?port={port}"), &p).await;
        let v = got.json();
        assert_eq!(v["got"], json!(["hello", "bytes:1,2,3"]), "{v}");
        assert_eq!(v["events"], json!(["open", "close"]));
        assert_eq!(
            (v["code"].clone(), v["protocol"].clone(), v["readyState"].clone()),
            (json!(1000), json!("chat"), json!(3))
        );
        assert_eq!(v["url"], json!(format!("ws://echo.test:{port}/")));
    });
}

#[test]
fn ac_dno_ws_client_to_a_loopback_address_is_an_error_event() {
    run_local(async {
        let mut h = Harness::with_egress(cfg0(), egress());
        let p = h.prepare(&ts_upload(REFUSED)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(
            got.json(),
            json!({"r": "error:egress_denied", "readyState": 3, "bad": "SyntaxError"})
        );
    });
}

/// With `max_concurrent_fetches = 1` an open outbound socket holds the task's only slot: a second
/// socket waits until the first one closes (security review finding 5).
const TWO_SOCKETS: &str = "export default async function (req) {\n\
  const port = new URL(req.url).searchParams.get('port');\n\
  const opened = (ws) => new Promise((resolve) => { ws.onopen = () => resolve('open'); ws.onerror = (e) => resolve('error:' + e.code); });\n\
  const a = new WebSocket(`ws://echo.test:${port}/`);\n\
  const first = await opened(a);\n\
  const b = new WebSocket(`ws://echo.test:${port}/`);\n\
  const bOpened = opened(b);\n\
  const early = await Promise.race([bOpened, new Promise((r) => setTimeout(() => r('waiting'), 300))]);\n\
  a.close(1000);\n\
  const late = await bOpened;\n\
  b.close(1000);\n\
  return Response.json({ first, early, late });\n\
}\n";

#[test]
fn ac_dno_ws_client_counts_against_max_concurrent_fetches() {
    run_local(async {
        let port = echo_server().await;
        let mut h = Harness::with_egress(cfg0(), egress());
        let p = h.prepare(&ts_upload(TWO_SOCKETS)).await;
        let limits = rt_types::Limits {
            max_concurrent_fetches: 1,
            ..Default::default()
        };
        let ran = h
            .run(spec().limits(limits).http_get(&format!("/?port={port}")).finish(), &p)
            .await;
        let v = read(ran.outcome).await.json();
        assert_eq!(v, json!({"first": "open", "early": "waiting", "late": "open"}));
    });
}
