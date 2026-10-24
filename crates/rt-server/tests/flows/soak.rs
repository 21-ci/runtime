//! Soak (security review step 5c, memory leaks): one in-process node takes ~200k mixed requests
//! (Deno and WASM HTTP; kv, sql, lock, schedule; service RPC; operator SDK calls; WebSocket
//! connect / message / disconnect) while the process RSS is sampled. RSS must plateau: less than
//! 5 % growth over the second half of the run.
//!
//! Ignored by default (minutes in a debug build):
//! `cargo test -p rt-server --test flows -- --ignored soak --nocapture`
//! `RT_SOAK_REQUESTS` overrides the request count.

use std::time::Instant;

use futures_util::stream::{self, StreamExt};
use serde_json::json;

use crate::common::*;

const MIX: &str = r#"
import { kv } from "std:kv";
import { sql } from "std:sql";
import { lock } from "std:lock";
import { schedule } from "std:schedule";
import { services } from "std:services";
import { acme } from "std:sdk";
export const rpc = { async add(a: any) { return a.x + 1; } };
export default {
  async fetch(req: Request, ctx: any): Promise<Response> {
    const u = new URL(req.url);
    const n = Number(u.searchParams.get("n") ?? "0");
    switch (u.pathname) {
      case "/kv": { await kv.set("k" + (n % 500), { n, pad: "x".repeat(200) }); return Response.json(await kv.get("k" + (n % 500))); }
      case "/sql": {
        await sql.execute("CREATE TABLE IF NOT EXISTS t (k INTEGER PRIMARY KEY, v TEXT)");
        await sql.execute("INSERT OR REPLACE INTO t (k, v) VALUES (?, ?)", [n % 500, "v" + n]);
        return Response.json((await sql.execute("SELECT v FROM t WHERE k = ?", [n % 500])).rows);
      }
      case "/lock": return Response.json(await lock("l" + (n % 50), async () => n, { waitMs: 2000, leaseMs: 5000 }));
      case "/schedule": {
        const s = await schedule.after(3600, { n }, { id: "s" + (n % 100) });
        return Response.json(await schedule.cancel(s.id));
      }
      case "/call": return Response.json(await services.call(ctx.service.slug, "add", { x: n }));
      case "/sdk": return Response.json(await acme.ping());
      default: return new Response("ok " + n);
    }
  },
  async scheduled(): Promise<void> {},
};"#;

const ECHO: &str = r#"
import { sockets } from "std:sockets";
export default {
  async fetch(req: Request): Promise<Response> {
    if (req.headers.get("upgrade")) return sockets.accept(req);
    return new Response("plain");
  },
  async webSocketMessage(ws: any, message: string): Promise<void> { ws.send(message); },
};"#;

/// Resident set size of this process in bytes (`ps`, so it works on macOS and Linux).
fn rss_bytes() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();
    out.ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
        * 1024
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "soak: minutes long; run on demand"]
async fn soak_rss_plateaus_under_mixed_load() {
    let total: usize = std::env::var("RT_SOAK_REQUESTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let node = start_node().await;
    assert_eq!(
        publish_sdk(&node, "acme", "1.0.0", &sdk_source("v1"), None).await.s(),
        201
    );
    assert_eq!(
        put_grant(&node, json!({"scope": "node", "sdk": "acme", "effect": "allow"}))
            .await
            .s(),
        200
    );
    let t = new_tenant(&node, "pro").await;
    let mix = uniq("mix");
    ship(&node, &t.key, &mix, MIX).await;
    let wasm = uniq("wasm");
    ship_wasm(&node, &t.key, &wasm, "hello").await;
    let echo = uniq("echo");
    ship(&node, &t.key, &echo, ECHO).await;

    let paths = ["/", "/kv", "/sql", "/lock", "/schedule", "/call", "/sdk"];
    let batch = 10_000usize;
    let mut samples: Vec<(usize, u64)> = Vec::new();
    let started = Instant::now();
    let mut failures = 0usize;
    for b in 0..total.div_ceil(batch) {
        let from = b * batch;
        let to = (from + batch).min(total);
        let results: Vec<bool> = stream::iter(from..to)
            .map(|i| {
                let (node, mix, wasm, echo) = (&node, &mix, &wasm, &echo);
                async move {
                    match i % 10 {
                        // A WebSocket connect, one echoed message, a close (1 in 50 requests).
                        9 if i % 50 == 9 => match ws_connect_with(node, echo, "/", &[]).await {
                            Ok(mut ws) => {
                                ws_send(&mut ws, "hi").await;
                                ws_text(&mut ws).await == "hi"
                            }
                            Err(_) => false,
                        },
                        8 | 9 => invoke(node, wasm, "/").await.s() == 200,
                        k => {
                            invoke(node, mix, &format!("{}?n={i}", paths[k % paths.len()]))
                                .await
                                .s()
                                == 200
                        }
                    }
                }
            })
            .buffer_unordered(64)
            .collect()
            .await;
        failures += results.iter().filter(|ok| !**ok).count();
        let rss = rss_bytes();
        samples.push((to, rss));
        eprintln!(
            "soak: {to:>7} requests, rss {:>6} MiB, {:?}",
            rss >> 20,
            started.elapsed()
        );
    }
    let mid = samples[samples.len() / 2].1;
    let end = samples.last().map(|s| s.1).unwrap_or(0);
    let peak_second_half = samples[samples.len() / 2..].iter().map(|s| s.1).max().unwrap_or(0);
    let growth = (end as f64 - mid as f64) / mid as f64 * 100.0;
    eprintln!(
        "soak: {total} requests in {:?}, {failures} failed; rss mid {} MiB, end {} MiB, peak (2nd half) {} MiB, growth {growth:.1} %",
        started.elapsed(),
        mid >> 20,
        end >> 20,
        peak_second_half >> 20
    );
    assert!(
        failures * 1000 < total,
        "more than 0.1 % of the requests failed: {failures}"
    );
    assert!(growth < 5.0, "RSS grew {growth:.1} % over the second half: {samples:?}");
    node.shutdown().await;
}
