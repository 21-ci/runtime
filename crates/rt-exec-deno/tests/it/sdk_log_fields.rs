//! `std:log` from a Deno guest: `log.info|warn|error|debug(message, fields)` writes a
//! record with the fields, `log.with(fields)` binds fields to a child logger (call fields win),
//! and a field object over the caps or not an object is refused with `validation_failed`
//! without a record. `console.*` keeps writing plain records without fields.

use crate::common;

use common::*;
use serde_json::json;

const LOGGING: &str = "import { log } from 'std:log';\n\
const code = async (f: () => unknown) => { try { await f(); return 'ok'; } catch (e) { return (e as { code?: string }).code ?? String(e); } };\n\
export default async () => {\n\
  const out: Record<string, unknown> = {};\n\
  log.info('plain', { a: 1 });\n\
  log.info('bare');\n\
  const child = log.with({ req: 'r1' });\n\
  child.warn('bound', { b: 2 });\n\
  child.error('override', { req: 'r2' });\n\
  console.log('console line');\n\
  out.tooMany = await code(() => log.info('refused', Object.fromEntries(Array.from({ length: 33 }, (_, i) => ['k' + i, i]))));\n\
  out.notObject = await code(() => log.info('refused', [1, 2]));\n\
  out.big = await code(() => log.info('refused', { v: 'x'.repeat(9000) }));\n\
  out.bigint = await code(() => log.info('refused', { v: 1n }));\n\
  out.childFrozen = Object.isFrozen(child);\n\
  return Response.json(out);\n\
};\n";

#[test]
fn ac_dno_32_std_log_writes_structured_records_and_refuses_bad_fields() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(LOGGING)).await;
        let (got, task) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(
            got.json(),
            json!({
                "tooMany": "validation_failed", "notObject": "validation_failed",
                "big": "validation_failed", "bigint": "validation_failed",
                "childFrozen": true,
            })
        );

        let records: Vec<_> = h
            .log_records()
            .into_iter()
            .filter(|r| r.task_id == Some(task))
            .collect();
        let shape: Vec<(String, Option<serde_json::Value>)> = records
            .iter()
            .map(|r| (r.msg.clone(), r.fields.clone().map(serde_json::Value::Object)))
            .collect();
        assert_eq!(
            shape,
            vec![
                ("plain".to_string(), Some(json!({ "a": 1 }))),
                ("bare".to_string(), None),
                ("bound".to_string(), Some(json!({ "req": "r1", "b": 2 }))),
                ("override".to_string(), Some(json!({ "req": "r2" }))),
                ("console line".to_string(), None),
            ],
            "refused calls write nothing; console keeps plain records"
        );
    });
}
