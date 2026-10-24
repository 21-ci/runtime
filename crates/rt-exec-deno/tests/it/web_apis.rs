//! Web APIs every real service uses keep working inside the sandbox (regression for the sample
//! apps: `crypto.getRandomValues`, `crypto.subtle.digest`, `TextEncoder` after the D-A2 memory
//! wrappers). Each check runs in a fresh task and reports which call failed.

use crate::common;

use bytes::Bytes;
use common::*;
use rt_build::Upload;

const PROBE: &str = r#"
const checks = {
  getRandomValues: () => crypto.getRandomValues(new Uint8Array(16)).length === 16,
  randomUUID: () => crypto.randomUUID().length === 36,
  encode: () => new TextEncoder().encode("abc").length === 3,
  encodeInto: () => new TextEncoder().encodeInto("abc", new Uint8Array(8)).written === 3,
  decode: () => new TextDecoder().decode(new Uint8Array([104, 105])) === "hi",
  digest: async () => (await crypto.subtle.digest("SHA-256", new TextEncoder().encode("x"))).byteLength === 32,
  digestBuffer: async () => (await crypto.subtle.digest("SHA-256", new ArrayBuffer(4))).byteLength === 32,
  hmac: async () => {
    const key = await crypto.subtle.importKey("raw", new TextEncoder().encode("k"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    return (await crypto.subtle.sign("HMAC", key, new TextEncoder().encode("m"))).byteLength === 32;
  },
  sliceBuffer: () => new ArrayBuffer(8).slice(2).byteLength === 6,
  dataView: () => new DataView(new ArrayBuffer(4)).getUint32(0) === 0,
  btoa: () => btoa("hi") === "aGk=",
  structuredClone: () => structuredClone(new Uint8Array([1, 2])).length === 2,
  blobText: async () => (await new Blob(["ab"]).text()) === "ab",
  responseBytes: async () => (await new Response(new Uint8Array([1, 2, 3])).arrayBuffer()).byteLength === 3,
  requestJson: async () => (await new Request("http://x/", { method: "POST", body: '{"a":1}' }).json()).a === 1,
};

export default async function () {
  const failed = [];
  for (const [name, f] of Object.entries(checks)) {
    try {
      if (!(await f())) failed.push(`${name}: false`);
    } catch (e) {
      failed.push(`${name}: ${e && e.name}: ${e && e.message}`);
    }
  }
  return new Response(failed.length ? failed.join("\n") : "ok", { status: failed.length ? 500 : 200 });
}
"#;

#[test]
fn web_crypto_encoding_and_buffers_work_in_a_task() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let upload = Upload {
            content_type: "application/typescript".into(),
            body: Bytes::from_static(PROBE.as_bytes()),
        };
        let p = h.prepare(&upload).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!((got.status, got.text()), (200, "ok".to_string()));
    });
}

const PLATFORM_PROBE: &str = r#"
import { sql } from "std:sql";
import { kv } from "std:kv";
const checks = {
  incr: async () => (await kv.incr("n", 2)) === 2,
  incrWrapped: async () => { const f = (k, d, o) => kv.incr(k, d, o); return (await f("m", 1, { ttlSeconds: 60 })) === 1; },
  execute: async () => { await sql.execute("CREATE TABLE IF NOT EXISTS t (v INTEGER)"); return true; },
  batch: async () => { await sql.batch([{ sql: "INSERT INTO t VALUES (?)", args: [1] }]); return true; },
  transaction: async () => sql.transaction(async (tx) => {
    await tx.execute("INSERT INTO t VALUES (?)", [2]);
    const r = await tx.execute("SELECT count(*) AS n FROM t");
    return r.rows.length === 1;
  }),
};
export default async function () {
  const failed = [];
  for (const [name, f] of Object.entries(checks)) {
    try { if (!(await f())) failed.push(`${name}: false`); }
    catch (e) { failed.push(`${name}: ${e && e.name}: ${e && e.message}\n${e && e.stack}`); }
  }
  return new Response(failed.length ? failed.join("\n") : "ok", { status: failed.length ? 500 : 200 });
}
"#;

#[test]
fn platform_kv_and_sql_calls_work_in_a_task() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let upload = Upload {
            content_type: "application/typescript".into(),
            body: Bytes::from_static(PLATFORM_PROBE.as_bytes()),
        };
        let p = h.prepare(&upload).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!((got.status, got.text()), (200, "ok".to_string()));
    });
}
