//! `std:kv` `cas`, `expire`, `ttl` and `list({reverse})` from a Deno guest: a swap
//! from many concurrent tasks lets exactly one win, `null` means absent / clear, the JS argument
//! checks reuse the stable `.code`s (`validation_failed`).

use crate::common;

use common::*;
use futures_util::future::join_all;
use serde_json::json;

/// `?who=<owner>`: tries to take the lock; returns whether this task won.
const CAS_LOCK: &str = "import { kv } from 'std:kv';\n\
export default async (req: Request) => {\n\
  const who = new URL(req.url).searchParams.get('who') ?? 'x';\n\
  const won = await kv.cas('lock', null, who);\n\
  return Response.json({ won, holder: await kv.get('lock') });\n\
};\n";

#[test]
fn ac_dno_30_concurrent_cas_from_many_tasks_exactly_one_wins() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(CAS_LOCK)).await;
        let started: Vec<_> = (0..16)
            .map(|i| h.start(spec().http_get(&format!("/?who=t{i}")).finish(), &p).1)
            .collect();
        let mut winners = 0;
        for outcome in join_all(started).await {
            let got = read(outcome).await;
            assert_eq!(got.status, 200, "{}", got.text());
            if got.json()["won"] == json!(true) {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "exactly one task wins the swap");
    });
}

const CAS_API: &str = "import { kv } from 'std:kv';\n\
const code = async (f: () => Promise<unknown>) => { try { await f(); return 'ok'; } catch (e) { return (e as { code?: string }).code ?? String(e); } };\n\
export default async () => {\n\
  const out: Record<string, unknown> = {};\n\
  out.casAbsent = await kv.cas('k', null, 1);\n\
  out.casPresent = await kv.cas('k', null, 2);\n\
  out.casMatch = await kv.cas('k', 1, 3);\n\
  out.casOtherType = await kv.cas('k', '3', 4);\n\
  out.afterCas = await kv.get('k');\n\
  out.ttlNone = await kv.ttl('k');\n\
  out.expire = await kv.expire('k', 60);\n\
  const left = await kv.ttl('k');\n\
  out.ttlLeft = typeof left === 'number' && left > 55 && left <= 60;\n\
  out.expireMissing = await kv.expire('nope', 60);\n\
  out.ttlMissing = await kv.ttl('nope');\n\
  out.clear = await kv.expire('k', null);\n\
  out.ttlCleared = await kv.ttl('k');\n\
  out.bytesAbsent = await kv.cas('b', null, new Uint8Array([1, 2]));\n\
  out.bytesGet = Array.from((await kv.get('b')) as Uint8Array);\n\
  out.bytesDelete = await kv.cas('b', new Uint8Array([1, 2]), null);\n\
  out.bytesGone = await kv.get('b');\n\
  out.withTtl = await kv.cas('t', null, 'v', { ttlSeconds: 60 });\n\
  const tl = await kv.ttl('t');\n\
  out.withTtlLeft = typeof tl === 'number' && tl > 55 && tl <= 60;\n\
  out.undefinedNextDeletes = await kv.cas('t', 'v', undefined);\n\
  out.badExpire = await code(() => kv.expire('k', -1));\n\
  out.badKey = await code(() => kv.cas('', null, 1));\n\
  await kv.set('p1', 'a'); await kv.set('p3', 'c'); await kv.set('p2', 'b'); await kv.set('q0', 'z');\n\
  const page = await kv.list({ prefix: 'p', reverse: true, limit: 2 });\n\
  out.reverseKeys = page.entries.map((e: { key: string }) => e.key);\n\
  const rest = await kv.list({ prefix: 'p', reverse: true, limit: 2, cursor: page.cursor });\n\
  out.reverseRest = rest.entries.map((e: { key: string }) => e.key);\n\
  return Response.json(out);\n\
};\n";

#[test]
fn ac_dno_31_cas_expire_ttl_and_reverse_list_semantics() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(CAS_API)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(
            got.json(),
            json!({
                "casAbsent": true, "casPresent": false, "casMatch": true, "casOtherType": false,
                "afterCas": 3,
                "ttlNone": null, "expire": true, "ttlLeft": true,
                "expireMissing": false, "ttlMissing": null,
                "clear": true, "ttlCleared": null,
                "bytesAbsent": true, "bytesGet": [1, 2], "bytesDelete": true, "bytesGone": null,
                "withTtl": true, "withTtlLeft": true, "undefinedNextDeletes": true,
                "badExpire": "validation_failed", "badKey": "validation_failed",
                "reverseKeys": ["p3", "p2"], "reverseRest": ["p1"],
            })
        );
    });
}

/// Each call's error `.code` (or `ok`) for `ttlSeconds` values a computation can produce.
const TTL_ARGS: &str = "import { kv } from 'std:kv';\n\
const code = async (f: () => Promise<unknown>) => { try { await f(); return 'ok'; } catch (e) { return (e as any).code ?? String(e); } };\n\
export default async () => Response.json({\n\
  nan: await code(() => kv.set('a', 1, { ttlSeconds: NaN })),\n\
  neg: await code(() => kv.set('a', 1, { ttlSeconds: -1 })),\n\
  str: await code(() => kv.set('a', 1, { ttlSeconds: '60' as any })),\n\
  inf: await code(() => kv.incr('n', 1, { ttlSeconds: Infinity })),\n\
  casNan: await code(() => kv.cas('c', null, 1, { ttlSeconds: NaN })),\n\
  ok: await code(() => kv.set('a', 1, { ttlSeconds: 60 })),\n\
  none: await code(() => kv.set('a', 1, {})),\n\
  undef: await code(() => kv.set('a', 1, { ttlSeconds: undefined })),\n\
  nul: await code(() => kv.set('a', 1, { ttlSeconds: null as any })),\n\
  persisted: await kv.ttl('a'),\n\
});\n";

/// DOPS-001: `set`, `incr` and `cas` validate `ttlSeconds` like `expire`: a NaN, negative,
/// infinite or non-number TTL is `validation_failed`, never silently "no expiry".
#[test]
fn dops_001_set_incr_and_cas_refuse_a_bad_ttl() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(TTL_ARGS)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(
            got.json(),
            json!({
                "nan": "validation_failed", "neg": "validation_failed", "str": "validation_failed",
                "inf": "validation_failed", "casNan": "validation_failed",
                "ok": "ok", "none": "ok", "undef": "ok", "nul": "ok", "persisted": null,
            })
        );
    });
}
