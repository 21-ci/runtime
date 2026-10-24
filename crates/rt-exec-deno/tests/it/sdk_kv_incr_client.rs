//! `std:kv` `kv.incr` and `ctx.client` from a Deno guest (docs/handlers.md, DNO-23,
//! DNO-24): increments from concurrent tasks lose nothing, the delta is validated in the SDK, store
//! refusals arrive as stable `.code`s; `ctx.client.ip` comes from the runtime's `x-rt-client-ip`
//! request header for the `http` trigger only.

use crate::common;

use common::*;
use futures_util::future::join_all;
use rt_types::{HttpRequest, Payload, ScheduleId, ScheduledEvent, TaskId, Trigger};
use serde_json::json;

/// `?n=<increments>&d=<delta>`: increments `hits` n times (awaiting each), returns the last value.
const INCR_LOOP: &str = "import { kv } from 'std:kv';\n\
export default async (req: Request) => {\n\
  const q = new URL(req.url).searchParams;\n\
  let last = 0;\n\
  for (let i = 0; i < Number(q.get('n') ?? '1'); i++) last = await kv.incr('hits', Number(q.get('d') ?? '1'));\n\
  return Response.json({ last, read: await kv.get('hits') });\n\
};\n";

#[test]
fn ac_dno_23_increments_from_concurrent_tasks_lose_nothing() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(INCR_LOOP)).await;
        let started: Vec<_> = (0..8)
            .map(|_| h.start(spec().http_get("/?n=25").finish(), &p).1)
            .collect();
        let mut lasts = Vec::new();
        for outcome in join_all(started).await {
            let got = read(outcome).await;
            assert_eq!(got.status, 200, "{}", got.text());
            lasts.push(got.json()["last"].as_i64().expect("last"));
        }
        lasts.sort_unstable();
        lasts.dedup();
        assert_eq!(lasts.len(), 8, "every task ended on a different value: {lasts:?}");
        assert_eq!(lasts.last(), Some(&200), "8 tasks x 25 increments");
        let (got, _) = h.get("/?n=1&d=-200", &p).await;
        assert_eq!(
            got.json(),
            json!({ "last": 0, "read": 0 }),
            "a counter reads back as a number through kv.get"
        );
    });
}

const INCR_CASES: &str = "import { kv } from 'std:kv';\n\
const code = async (f: () => Promise<unknown>) => { try { await f(); return 'ok'; } catch (e) { return (e as { code?: string }).code ?? String(e); } };\n\
export default async () => {\n\
  const out: Record<string, unknown> = {};\n\
  out.first = await kv.incr('c');\n\
  out.second = await kv.incr('c', 4);\n\
  out.down = await kv.incr('c', -10);\n\
  const big = await kv.incr('big', 9007199254740993n);\n\
  out.bigType = typeof big;\n\
  out.big = String(big);\n\
  out.bigPlus = String(await kv.incr('big', 1n));\n\
  out.ttlFirst = await kv.incr('win', 1, { ttlSeconds: 60 });\n\
  out.ttlSecond = await kv.incr('win', 1, { ttlSeconds: 60 });\n\
  await kv.set('text', 'seven');\n\
  out.notInteger = await code(() => kv.incr('text'));\n\
  out.textKept = await kv.get('text');\n\
  await kv.set('max', 9223372036854775807n.toString());\n\
  out.stringNumber = await code(() => kv.incr('max'));\n\
  await kv.incr('top', 9223372036854775807n);\n\
  out.overflow = await code(() => kv.incr('top', 1));\n\
  out.fraction = await code(() => kv.incr('c', 1.5));\n\
  out.nan = await code(() => kv.incr('c', NaN));\n\
  out.unsafe = await code(() => kv.incr('c', 2 ** 53));\n\
  out.stringDelta = await code(() => kv.incr('c', '1' as unknown as number));\n\
  out.bigRange = await code(() => kv.incr('c', 2n ** 63n));\n\
  out.emptyKey = await code(() => kv.incr(''));\n\
  out.after = await kv.get('c');\n\
  return Response.json(out);\n\
};\n";

#[test]
fn ac_dno_23_incr_semantics_validation_and_error_codes() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(INCR_CASES)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(
            got.json(),
            json!({
                "first": 1, "second": 5, "down": -5,
                "bigType": "bigint", "big": "9007199254740993", "bigPlus": "9007199254740994",
                "ttlFirst": 1, "ttlSecond": 2,
                "notInteger": "kv_not_integer", "textKept": "seven",
                "stringNumber": "kv_not_integer",
                "overflow": "kv_overflow",
                "fraction": "validation_failed", "nan": "validation_failed", "unsafe": "validation_failed",
                "stringDelta": "validation_failed", "bigRange": "validation_failed",
                "emptyKey": "validation_failed",
                "after": -5,
            })
        );
    });
}

/// Echoes what the guest sees of its client.
const CLIENT_ECHO: &str = "export default {\n\
  fetch(req: Request, ctx: { client?: { ip: string }; trigger: string }) {\n\
    return Response.json({ trigger: ctx.trigger, client: ctx.client ?? null, header: req.headers.get('x-rt-client-ip'),\n\
      frozen: ctx.client ? Object.isFrozen(ctx.client) : null });\n\
  },\n\
};\n";

fn with_client_header(trigger: Trigger, value: &str) -> rt_types::TaskSpec {
    let mut req = HttpRequest::get("/");
    req.headers
        .insert("x-rt-client-ip", value.parse().expect("header value"));
    spec().trigger(trigger).payload(Payload::Http(req)).finish()
}

#[test]
fn ac_dno_24_ctx_client_ip_is_set_for_the_http_trigger_only() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(CLIENT_ECHO)).await;

        let v = read(
            h.run(with_client_header(Trigger::Http, "2001:db8::7"), &p)
                .await
                .outcome,
        )
        .await
        .json();
        assert_eq!(v["client"], json!({ "ip": "2001:db8::7" }));
        assert_eq!(v["header"], "2001:db8::7", "WASM-style header access works too");
        assert_eq!(v["frozen"], true);

        let (got, _) = h.get("/", &p).await;
        assert_eq!(
            got.json()["client"],
            json!(null),
            "no header (e.g. the internal Unix socket): no ctx.client"
        );

        let v = read(h.run(with_client_header(Trigger::Http, "not-an-ip"), &p).await.outcome)
            .await
            .json();
        assert_eq!(v["client"], json!(null), "only an address becomes ctx.client");

        // A call's headers are the calling guest's: never a client address.
        let v = read(
            h.run(
                with_client_header(
                    Trigger::Call {
                        parent: TaskId(9),
                        depth: 1,
                    },
                    "198.51.100.1",
                ),
                &p,
            )
            .await
            .outcome,
        )
        .await
        .json();
        assert_eq!((v["trigger"].as_str(), &v["client"]), (Some("call"), &json!(null)));

        let id = ScheduleId::from_u128(5);
        let sched = spec()
            .trigger(Trigger::Interval { id })
            .payload(Payload::Schedule(ScheduledEvent {
                schedule_id: id,
                scheduled_time_ms: 1,
                payload: json!(null),
            }))
            .finish();
        let v = read(h.run(sched, &p).await.outcome).await.json();
        assert_eq!((v["trigger"].as_str(), &v["client"]), (Some("interval"), &json!(null)));
    });
}
