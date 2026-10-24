//! Guest-facing edge cases of the Deno executor (scratchpad edge-cases.md §Deno): reachability of
//! runtime internals, secret/env precedence, std:schedule argument checks, sync SQL caps and odd
//! request header bytes.

use crate::common;

use common::*;
use serde_json::json;

const INTERNALS: &str = "export default async () => {\n\
  const out: Record<string, unknown> = {};\n\
  const d: any = (globalThis as any).Deno;\n\
  out.denoCore = typeof d?.core;\n\
  out.denoKeys = d ? Object.keys(d).sort() : null;\n\
  const rt: any = (globalThis as any).__rt;\n\
  out.rtType = typeof rt;\n\
  try { (globalThis as any).__rt = 1; out.replaced = (globalThis as any).__rt === 1; } catch { out.replaced = false; }\n\
  try { out.deleted = delete (globalThis as any).__rt && typeof (globalThis as any).__rt === 'undefined'; } catch { out.deleted = false; }\n\
  let second = 'none';\n\
  if (rt && typeof rt.start === 'function') {\n\
    try { await rt.start({ trigger: 'queue', caller: { tenant: 'forged' } }); second = 'returned'; } catch (e) { second = 'threw'; }\n\
  }\n\
  out.secondStart = second;\n\
  return Response.json(out);\n\
};\n";

/// SBX-003: guests cannot reach the op layer (`Deno.core`) and cannot replace or delete the
/// runtime's entry point; calling it again never dispatches a second task with forged context.
#[test]
fn sbx_003_runtime_internals_are_out_of_reach() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(INTERNALS)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        let v = got.json();
        assert_eq!(v["denoCore"], json!("undefined"), "{v}");
        assert_eq!(v["denoKeys"], json!(["env"]), "{v}");
        assert_eq!(v["replaced"], json!(false), "{v}");
        assert_eq!(v["deleted"], json!(false), "{v}");
        assert_ne!(
            v["secondStart"],
            json!("returned"),
            "a second start must not run a task: {v}"
        );
    });
}

const PRECEDENCE: &str = "import { secrets } from 'std:secrets';\n\
export default () => Response.json({ secret: secrets.get('FOO'), env: (globalThis as any).Deno.env.get('FOO') ?? null });\n";

/// HOST-008: a secret and an env var with the same name stay apart: `secrets.get` returns the
/// secret, `Deno.env.get` the env var.
#[test]
fn host_008_secrets_and_env_with_one_name_stay_apart() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control
            .set_env(svc(1), &[("FOO", "from-env")], &[("FOO", "from-secret")]);
        let p = h.prepare(&ts_upload(PRECEDENCE)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(got.json(), json!({ "secret": "from-secret", "env": "from-env" }));
    });
}

const SCHEDULE_ARGS: &str = "import { schedule } from 'std:schedule';\n\
const code = async (f: () => Promise<unknown>) => { try { await f(); return 'ok'; } catch (e) { return (e as any).code ?? String(e); } };\n\
export default async () => Response.json({\n\
  max: await code(() => schedule.at(Number.MAX_SAFE_INTEGER, {})),\n\
  negative: await code(() => schedule.at(-1, {})),\n\
  nan: await code(() => schedule.at(NaN, {})),\n\
  invalidDate: await code(() => schedule.at(new Date('invalid') as any, {})),\n\
  afterNaN: await code(() => schedule.after(NaN, {})),\n\
  afterNegative: await code(() => schedule.after(-5, {})),\n\
  ok: await code(() => schedule.after(60, {})),\n\
});\n";

/// HSCH-004: absurd `schedule.at` / `after` times are `validation_failed`, never an overflow; a
/// past `at` (also a negative Unix ms) is due at once, as documented.
#[test]
fn hsch_004_bad_schedule_times_are_validation_failed() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SCHEDULE_ARGS)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(
            got.json(),
            json!({
                "max": "validation_failed", "negative": "ok", "nan": "validation_failed",
                "invalidDate": "validation_failed", "afterNaN": "validation_failed", "afterNegative": "validation_failed",
                "ok": "ok",
            })
        );
    });
}

const ITERATE: &str = "import { Database } from 'bun:sqlite';\n\
export default () => {\n\
  const db = new Database();\n\
  db.exec('CREATE TABLE IF NOT EXISTS n (i INTEGER)');\n\
  db.exec('DELETE FROM n');\n\
  db.exec('WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 10001) INSERT INTO n SELECT i FROM c');\n\
  let seen = 0; let code = 'ok';\n\
  try { for (const _ of db.query('SELECT i FROM n').iterate()) seen++; } catch (e) { code = (e as any).rtCode ?? String(e); }\n\
  return Response.json({ seen, code });\n\
};\n";

/// DSQL-003: iterating more than 10,000 rows ends in `sql_result_too_large` (`rtCode`; the bun
/// `code` is `SQLITE_TOOBIG`), never an unbounded stream.
#[test]
fn dsql_003_iterate_is_capped() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(ITERATE)).await;
        let got = read(h.run_past_cold_open(|| get("/"), &p).await.outcome).await;
        assert_eq!(got.status, 200, "{}", got.text());
        let v = got.json();
        assert_eq!(v["code"], json!("sql_result_too_large"), "{v}");
        assert!(v["seen"].as_u64().is_some_and(|n| n <= 10_000), "{v}");
    });
}

const HEADER: &str = "export default (req: Request) => {\n\
  const v = req.headers.get('x-raw') ?? '';\n\
  return Response.json({ codes: Array.from(v).map((c) => c.charCodeAt(0)) });\n\
};\n";

/// DREQ-006: header bytes 0x80-0xFF from a client reach the guest as a ByteString (one char per
/// byte), never a panic.
#[test]
fn dreq_006_non_utf8_header_bytes_reach_the_guest_as_a_byte_string() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(HEADER)).await;
        let mut req = rt_types::HttpRequest::get("/");
        req.headers.insert(
            "x-raw",
            http::HeaderValue::from_bytes(&[b'a', 0x80, 0xff, b'z']).expect("obs-text"),
        );
        let ran = h.run(spec().payload(rt_types::Payload::Http(req)).finish(), &p).await;
        let got = read(ran.outcome).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(got.json(), json!({ "codes": [97, 128, 255, 122] }));
    });
}

const SQL_BURN: &str = "import { Database } from 'bun:sqlite';\n\
export default () => {\n\
  const db = new Database();\n\
  for (let i = 0; i < 20000; i++) db.query('WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5000) SELECT count(*) FROM c').get();\n\
  return new Response('done');\n\
};\n";

/// PREEMPT-003: CPU the host spends running the guest's SQL counts toward the task's CPU limit:
/// many statements, each well under the per-statement SQL budget, add up to `cpu_limit`.
#[test]
fn preempt_003_host_sql_cpu_counts_toward_the_cpu_limit() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SQL_BURN)).await;
        let spec = || {
            let mut s = get("/");
            s.limits.cpu = std::time::Duration::from_millis(150);
            s.limits.wall = std::time::Duration::from_secs(30);
            s
        };
        let ran = h.run_past_cold_open(spec, &p).await;
        let logs: Vec<String> = h.log_records().into_iter().map(|r| r.msg).collect();
        assert!(
            matches!(ran.outcome, rt_types::Outcome::Killed(rt_types::KillReason::CpuLimit)),
            "{:?} {logs:?}",
            ran.outcome
        );
    });
}
