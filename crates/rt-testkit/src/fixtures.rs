//! Fixture builders: tiny guest sources and deploy payloads (`fixtures/deno`).
//!
//! Compiled WASM fixtures (`hello`, `counter`, `kv`, `loop`, `grow`, `fs-attempt`) are built in
//! CI from Rust guest crates under `fixtures/wasm/` (target `wasm32-wasip2`).

use serde_json::json;

/// `application/typescript` upload: returns "hello".
pub const DENO_HELLO: &str = r#"export default function (_req: Request): Response { return new Response("hello"); }"#;

/// Echoes the request body.
pub const DENO_ECHO: &str =
    r#"export default async function (req: Request): Promise<Response> { return new Response(await req.text()); }"#;

/// Throws (`500 guest_error`, body must not contain "boom").
pub const DENO_THROW: &str = r#"export default function (): Response { throw new Error("boom"); }"#;

/// `while (true) {}` (CPU limit kill, SCH-13).
pub const DENO_LOOP: &str = r#"export default function (): Response { while (true) {} }"#;

/// Allocates until the heap limit is hit (`502 oom`, DNO-08).
pub const DENO_OOM: &str = r#"export default function (): Response { const a: number[][] = []; while (true) a.push(new Array(1_000_000).fill(1)); }"#;

/// Counts through a global (`1` every time: top-level state is per task, DNO-01).
pub const DENO_GLOBAL_COUNTER: &str = r#"export default function (): Response { (globalThis as any).x = ((globalThis as any).x ?? 0) + 1; return new Response(String((globalThis as any).x)); }"#;

/// Streams 3 chunks 100 ms apart (DNO-06).
pub const DENO_STREAM: &str = r#"
export default function (): Response {
  const enc = new TextEncoder();
  const body = new ReadableStream({
    async start(c) { for (let i = 0; i < 3; i++) { c.enqueue(enc.encode(`chunk${i}\n`)); await new Promise((r) => setTimeout(r, 100)); } c.close(); },
  });
  return new Response(body);
}"#;

/// KV round trip through `std:kv`.
pub const DENO_KV: &str = r#"
import { kv } from "std:kv";
export default async function (req: Request): Promise<Response> {
  const url = new URL(req.url);
  if (req.method === "PUT") { await kv.set(url.pathname, await req.text()); return new Response("ok"); }
  const v = await kv.get(url.pathname);
  return v === null ? new Response("missing", { status: 404 }) : new Response(String(v));
}"#;

/// `application/json` deploy payload: `{entry, files: {path: content}}`.
pub fn json_deploy(entry: &str, files: &[(&str, &str)]) -> String {
    let map: serde_json::Map<String, serde_json::Value> =
        files.iter().map(|(p, c)| (p.to_string(), json!(c))).collect();
    json!({"entry": entry, "files": map}).to_string()
}

/// A single-file deploy as `(content_type, body)`.
pub fn single_file(source: &str) -> (&'static str, String) {
    ("application/typescript", source.to_string())
}
