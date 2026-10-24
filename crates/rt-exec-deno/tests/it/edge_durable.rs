//! Edge cases of `std:durable` storage in Deno guests (scratchpad edge-cases.md: DDUR-002, DUR-006):
//! values that cannot be structured-cloned, values too large for a put, and stored bytes written by
//! another runtime (format 0) or a malformed structured clone (format 1).

use crate::common;

use bytes::Bytes;
use common::*;
use rt_store::data::{
    DurableValue, DurableWrite, KvSetOptions, KvValue, ServiceDataStore, ValueTag, DURABLE_FORMAT_BYTES,
    DURABLE_FORMAT_V8,
};
use serde_json::{json, Value};

const CODE_OF: &str = "const code = async (f: () => unknown) => { try { await f(); return 'ok'; } catch (e) { return (e as { code?: string }).code ?? String(e); } };\n";

/// Commit `rows` (key, format, bytes) to object `object` of service 1 as if a task had written them.
async fn seed(h: &Harness, object: &str, rows: Vec<(&str, u8, Vec<u8>)>) {
    let writes = rows
        .into_iter()
        .map(|(k, format, data)| {
            (
                Bytes::from(k.to_string()),
                Some(DurableValue {
                    format,
                    data: Bytes::from(data),
                }),
            )
        })
        .collect();
    let w = DurableWrite {
        writes,
        ..Default::default()
    };
    h.data
        .durable(svc(1))
        .commit(object, w)
        .await
        .expect("seed the durable object");
}

/// Run `code` as the durable object `room` and return the JSON it answers (200 required).
async fn answer_in_object(h: &mut Harness, code: &str) -> Value {
    let p = h.prepare(&ts_upload(code)).await;
    let ran = h.run(object_task("room"), &p).await;
    let got = read(ran.outcome).await;
    assert_eq!(got.status, 200, "{}", got.text());
    got.json()
}

fn object_task(name: &str) -> rt_types::TaskSpec {
    let mut s = get("/");
    s.object = Some(rt_types::ObjectRef::named(name));
    s
}

/// DDUR-002: values that do not structured-clone (a function, a symbol, a WeakMap) fail with a stable
/// code and nothing is committed; a 2 MiB value is refused by size. A failed put inside one
/// `put({...})` leaves none of the object's writes behind.
#[test]
fn ddur_002_values_that_do_not_clone_or_are_too_large_commit_nothing() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = answer_in_object(
            &mut h,
            &format!(
                "import {{ storage }} from 'std:durable';\n{CODE_OF}\
                 export default async () => Response.json({{\n\
                   fn: await code(() => storage.put('fn', () => 1)),\n\
                   sym: await code(() => storage.put('sym', Symbol('x'))),\n\
                   weak: await code(() => storage.put('weak', new WeakMap())),\n\
                   big: await code(() => storage.put('big', 'x'.repeat(2 * 1024 * 1024))),\n\
                   partial: await code(() => storage.put({{ a: 1, b: () => 1 }})),\n\
                   keys: [...(await storage.list())].map(([k]) => k),\n\
                 }});"
            ),
        )
        .await;
        assert_eq!(v["fn"], json!("validation_failed"), "{v}");
        assert_eq!(v["sym"], json!("validation_failed"), "{v}");
        assert_eq!(v["weak"], json!("validation_failed"), "{v}");
        assert_eq!(v["big"], json!("kv_value_too_large"), "{v}");
        assert_eq!(v["partial"], json!("validation_failed"), "{v}");
        assert_eq!(v["keys"], json!([]), "a refused put commits nothing: {v}");
    });
}

/// DUR-006 (format 0): bytes written by a WASM component come back to a Deno guest as a `Uint8Array`.
/// A malformed structured clone (format 1) is a catchable error for that key, and the other keys
/// of the object still read.
#[test]
fn dur_006_wasm_bytes_read_as_bytes_and_a_malformed_clone_is_a_clean_error() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        seed(
            &h,
            "room",
            vec![
                ("raw", DURABLE_FORMAT_BYTES, vec![1, 2, 3]),
                ("bad", DURABLE_FORMAT_V8, vec![0xff, 0x00, 0xde, 0xad]),
                ("good", DURABLE_FORMAT_V8, vec![0xff, 0x0f, 0x22, 0x01, 0x6b]),
            ],
        )
        .await;
        let v = answer_in_object(
            &mut h,
            "import { storage } from 'std:durable';\n\
             export default async () => {\n\
               const raw = await storage.get('raw');\n\
               let bad: string;\n\
               try { await storage.get('bad'); bad = 'returned'; } catch (e) { bad = 'threw'; }\n\
               const good = await storage.get('good');\n\
               return Response.json({ rawIsBytes: raw instanceof Uint8Array, raw: raw ? Array.from(raw as Uint8Array) : null, bad, good: good ?? null });\n\
             };",
        )
        .await;
        assert_eq!(v["rawIsBytes"], json!(true), "{v}");
        assert_eq!(v["raw"], json!([1, 2, 3]), "{v}");
        assert_eq!(
            v["bad"],
            json!("threw"),
            "a malformed clone must throw, not return garbage: {v}"
        );
        assert_eq!(
            v["good"],
            json!("k"),
            "a well-formed clone next to a bad one still reads: {v}"
        );
    });
}

/// WPAR-001: a KV key is bytes in WASM (`list<u8>`) but a JS string in Deno. A key that is not UTF-8
/// reaches a Deno guest as a lossy string, so the guest cannot address it. Paging must still see
/// every key once: a page that ends on the non-UTF-8 key hands out a cursor, and a lossy cursor would
/// restart the listing from an earlier byte position and repeat that key.
#[test]
fn wpar_001_a_non_utf8_kv_key_is_listed_once_and_paging_does_not_repeat_it() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let kv = h.data.kv(svc(1));
        for key in [&b"a"[..], &[0xff, 0xfe, b'a'][..], &[0xff, 0xff][..]] {
            let value = KvValue {
                tag: ValueTag::Json,
                data: Bytes::from_static(b"1"),
            };
            kv.set(key, value, KvSetOptions::default())
                .await
                .expect("seed a kv key");
        }
        let v = answer_in_object(
            &mut h,
            "import { kv } from 'std:kv';\n\
             export default async () => {\n\
               const seen: string[] = [];\n\
               let cursor: string | null | undefined = undefined;\n\
               for (let i = 0; i < 10; i++) {\n\
                 const page = await kv.list({ limit: 1, cursor });\n\
                 for (const e of page.entries) seen.push(e.key);\n\
                 cursor = page.cursor;\n\
                 if (!cursor) break;\n\
               }\n\
               return Response.json({ seen });\n\
             };",
        )
        .await;
        let seen = v["seen"].as_array().expect("a list of keys").clone();
        assert_eq!(
            seen.len(),
            3,
            "each key is listed once, the non-UTF-8 one included: {v}"
        );
        assert_eq!(seen[0], json!("a"), "{v}");
        assert_eq!(
            seen[1],
            json!("\u{fffd}\u{fffd}a"),
            "the non-UTF-8 key reaches JS as a lossy string: {v}"
        );
        assert_eq!(
            seen[2],
            json!("\u{fffd}\u{fffd}"),
            "the key after it is not repeated or skipped: {v}"
        );
    });
}

/// A KV list cursor the runtime did not issue (odd length, not hex) is `validation_failed`, not a page
/// from some other position.
#[test]
fn kv_list_refuses_a_cursor_it_did_not_issue() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let v = answer_in_object(
            &mut h,
            "import { kv } from 'std:kv';\n\
             const code = async (f: () => unknown) => { try { await f(); return 'ok'; } catch (e) { return (e as { code?: string }).code ?? String(e); } };\n\
             export default async () => Response.json({\n\
               odd: await code(() => kv.list({ cursor: 'abc' })),\n\
               notHex: await code(() => kv.list({ cursor: 'zz' })),\n\
             });",
        )
        .await;
        assert_eq!(v, json!({ "odd": "validation_failed", "notHex": "validation_failed" }));
    });
}
