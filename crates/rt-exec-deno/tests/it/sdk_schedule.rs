//! `std:schedule` from a Deno guest: `after` and `at` store a one-off schedule of
//! the service and answer its id and due time, `cancel` and `list` act on the pending ones, the
//! arguments are checked (`validation_failed`), and a due schedule reaches the handler's
//! `scheduled(event, ctx)` with `event.id`, `event.scheduledAt` and `event.payload`.

use crate::common;

use common::*;
use rt_types::{DelayedEvent, Payload, Trigger};
use serde_json::json;

const SCHED: &str = "import { schedule } from 'std:schedule';\n\
import { kv } from 'std:kv';\n\
const fail = (e) => ({ error: (e && e.code) || String(e) });\n\
export default {\n\
  async fetch(req) {\n\
    const u = new URL(req.url);\n\
    const q = (k) => u.searchParams.get(k);\n\
    try {\n\
      switch (q('op')) {\n\
        case 'after': return Response.json(await schedule.after(Number(q('seconds')), { n: 1 }, q('id') ? { id: q('id') } : undefined));\n\
        case 'at': return Response.json(await schedule.at(Number(q('at')), { n: 2 }, { id: 'at-1' }));\n\
        case 'cancel': return Response.json({ cancelled: await schedule.cancel(q('id')) });\n\
        case 'list': return Response.json(await schedule.list({ limit: 10 }));\n\
        case 'big': return Response.json(await schedule.after(1, 'x'.repeat(20000)).then(() => 'ok', fail));\n\
        case 'last': return Response.json((await kv.get('last')) ?? null);\n\
        default: return Response.json({ error: 'unknown op' }, { status: 400 });\n\
      }\n\
    } catch (e) { return Response.json(fail(e)); }\n\
  },\n\
  async scheduled(event, ctx) {\n\
    await kv.set('last', { id: event.id, scheduledAt: event.scheduledAt, payload: event.payload, trigger: ctx.trigger });\n\
  },\n\
};\n";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

#[test]
fn ac_dno_35_after_at_list_and_cancel_manage_the_pending_schedules() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SCHED)).await;
        let (after, _) = h.get("/?op=after&seconds=60&id=digest", &p).await;
        let a = after.json();
        assert_eq!(a["id"], json!("digest"));
        let due = a["runAt"].as_i64().expect("runAt");
        assert!(
            (now_ms() + 59_000..=now_ms() + 61_000).contains(&due),
            "due in about 60 s: {due}"
        );

        let (generated, _) = h.get("/?op=after&seconds=1", &p).await;
        let id = generated.json()["id"].as_str().expect("generated id").to_string();
        assert!(id.starts_with("sch_"), "{id}");

        let (at, _) = h.get(&format!("/?op=at&at={}", now_ms() + 120_000), &p).await;
        assert_eq!(at.json()["id"], json!("at-1"));

        let (list, _) = h.get("/?op=list", &p).await;
        let ids: Vec<String> = list.json()["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .map(|e| e["id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            ids,
            vec!["at-1", "digest", id.as_str()],
            "pending schedules in id order"
        );

        let (c1, _) = h.get("/?op=cancel&id=digest", &p).await;
        assert_eq!(c1.json(), json!({ "cancelled": true }));
        let (c2, _) = h.get("/?op=cancel&id=digest", &p).await;
        assert_eq!(c2.json(), json!({ "cancelled": false }), "cancel once");
    });
}

#[test]
fn ac_dno_35_invalid_arguments_are_validation_failed() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SCHED)).await;
        let (bad_id, _) = h.get("/?op=after&seconds=1&id=bad%20id", &p).await;
        assert_eq!(bad_id.json(), json!({ "error": "validation_failed" }), "the id charset");
        let (far, _) = h.get("/?op=after&seconds=2678401", &p).await;
        assert_eq!(
            far.json(),
            json!({ "error": "validation_failed" }),
            "more than 30 days ahead"
        );
        let (big, _) = h.get("/?op=big", &p).await;
        assert_eq!(
            big.json(),
            json!({ "error": "validation_failed" }),
            "a payload over 16 KiB"
        );
    });
}

#[test]
fn ac_dno_35_a_due_schedule_reaches_scheduled_with_its_event() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SCHED)).await;
        let event = DelayedEvent {
            id: "digest".into(),
            scheduled_at_ms: 1_700_000_000_000,
            payload: json!({ "n": 1 }),
        };
        let task = spec()
            .trigger(Trigger::Scheduled { id: "digest".into() })
            .payload(Payload::Delayed(event))
            .finish();
        let ran = h.run(task, &p).await;
        let got = read(ran.outcome).await;
        assert_eq!(got.status, 200, "{}", got.text());
        let (last, _) = h.get("/?op=last", &p).await;
        assert_eq!(
            last.json(),
            json!({ "id": "digest", "scheduledAt": 1_700_000_000_000i64, "payload": { "n": 1 }, "trigger": "schedule" })
        );
    });
}
