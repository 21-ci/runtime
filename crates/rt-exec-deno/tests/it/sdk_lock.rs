//! `std:lock` from a Deno guest: callers of one key run one at a time, a wait past
//! `waitMs` throws `lock_timeout` while the other tasks keep running, `lock.try` never waits, the
//! lock is released when the callback throws, when its task ends while still holding it, and a
//! lease hands a key over from a holder that outlives it.

use crate::common;

use common::*;
use futures_util::future::join_all;
use serde_json::json;

const LOCK: &str = "import { lock } from 'std:lock';\n\
import { kv } from 'std:kv';\n\
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));\n\
const fail = (e) => ({ error: (e && e.code) || String(e) });\n\
export default {\n\
  async fetch(req, ctx) {\n\
    const u = new URL(req.url);\n\
    const q = (k) => u.searchParams.get(k);\n\
    const key = q('key') ?? 'k';\n\
    const opts = { waitMs: Number(q('wait') ?? 10000), leaseMs: Number(q('lease') ?? 30000) };\n\
    const mode = q('mode');\n\
    if (mode === 'read') return Response.json({ c: (await kv.get('c')) ?? 0 });\n\
    if (mode === 'bump') {\n\
      try {\n\
        const value = await lock(key, async () => {\n\
          const v = (await kv.get('c')) ?? 0;\n\
          await sleep(3);\n\
          await kv.set('c', v + 1);\n\
          return 'bumped';\n\
        }, opts);\n\
        return Response.json({ value });\n\
      } catch (e) { return Response.json(fail(e)); }\n\
    }\n\
    if (mode === 'hold') {\n\
      try {\n\
        const value = await lock(key, async () => {\n\
          await sleep(Number(q('hold') ?? 0));\n\
          if (q('boom')) throw new Error('boom');\n\
          return 'held';\n\
        }, opts);\n\
        return Response.json({ value });\n\
      } catch (e) { return Response.json(fail(e)); }\n\
    }\n\
    if (mode === 'try') {\n\
      try {\n\
        return Response.json(await lock.try(key, async () => 'tried', opts));\n\
      } catch (e) { return Response.json(fail(e)); }\n\
    }\n\
    if (mode === 'leak') {\n\
      // Not awaited: the task ends while the callback never settles, still holding the key.\n\
      lock(key, () => new Promise(() => {}), opts);\n\
      return Response.json({ started: true });\n\
    }\n\
    if (mode === 'lease') {\n\
      // A holds with a 50 ms lease for 300 ms; B comes at 100 ms and must not wait for A's end.\n\
      const a = lock(key, async () => { await sleep(300); await kv.set('a-done', 1); return 'a'; }, { waitMs: 0, leaseMs: 50 });\n\
      await sleep(100);\n\
      const b = await lock(key, async () => ({ aDone: (await kv.get('a-done')) ?? null }), { waitMs: 2000, leaseMs: 1000 });\n\
      await a;\n\
      return Response.json({ b });\n\
    }\n\
    return Response.json({ error: 'unknown mode' }, { status: 400 });\n\
  },\n\
};\n";

#[test]
fn ac_dno_34_lock_serializes_the_callers_of_one_key() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(LOCK)).await;
        // Each caller reads, waits, then writes back the count: without the lock the updates race.
        let runs: Vec<_> = (0..8)
            .map(|_| h.start(spec().http_get("/?mode=bump&key=k").finish(), &p).1)
            .collect();
        for outcome in join_all(runs).await {
            assert_eq!(read(outcome).await.json(), json!({ "value": "bumped" }));
        }
        let (got, _) = h.get("/?mode=read", &p).await;
        assert_eq!(
            got.json(),
            json!({ "c": 8 }),
            "every caller's read-modify-write ran alone"
        );
    });
}

#[test]
fn ac_dno_34_a_wait_past_waitms_is_lock_timeout_and_other_tasks_run_on() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(LOCK)).await;
        let holder = h.start(spec().http_get("/?mode=hold&key=k&hold=400").finish(), &p).1;
        let waiter = h
            .start(spec().http_get("/?mode=hold&key=k&hold=0&wait=50").finish(), &p)
            .1;
        let other = h.start(spec().http_get("/?mode=try&key=other").finish(), &p).1;
        let (held, timed_out, free) = tokio::join!(
            async { read(holder.await).await },
            async { read(waiter.await).await },
            async { read(other.await).await }
        );
        assert_eq!(held.json(), json!({ "value": "held" }));
        assert_eq!(timed_out.json(), json!({ "error": "lock_timeout" }));
        assert_eq!(
            free.json(),
            json!({ "acquired": true, "value": "tried" }),
            "a free key is not blocked"
        );
    });
}

#[test]
fn ac_dno_34_try_does_not_wait_and_reports_a_held_key() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(LOCK)).await;
        let holder = h.start(spec().http_get("/?mode=hold&key=k&hold=300").finish(), &p).1;
        let attempt = async {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            read(h.start(spec().http_get("/?mode=try&key=k").finish(), &p).1.await).await
        };
        let (held, tried) = tokio::join!(async { read(holder.await).await }, attempt);
        assert_eq!(held.json(), json!({ "value": "held" }));
        assert_eq!(tried.json(), json!({ "acquired": false }));
        let (after, _) = h.get("/?mode=try&key=k", &p).await;
        assert_eq!(after.json(), json!({ "acquired": true, "value": "tried" }));
    });
}

#[test]
fn ac_dno_34_a_throwing_callback_releases_the_key() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(LOCK)).await;
        let (failed, _) = h.get("/?mode=hold&key=k&hold=0&boom=1", &p).await;
        let msg = failed.json()["error"].as_str().expect("error text").to_string();
        assert!(msg.contains("boom"), "the callback's error reaches the caller: {msg}");
        let (after, _) = h.get("/?mode=try&key=k", &p).await;
        assert_eq!(
            after.json(),
            json!({ "acquired": true, "value": "tried" }),
            "released on throw"
        );
    });
}

#[test]
fn ac_dno_34_a_task_that_ends_while_holding_releases_its_locks() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(LOCK)).await;
        let (leaked, _) = h.get("/?mode=leak&key=k", &p).await;
        assert_eq!(leaked.json(), json!({ "started": true }));
        let (after, _) = h.get("/?mode=try&key=k", &p).await;
        assert_eq!(
            after.json(),
            json!({ "acquired": true, "value": "tried" }),
            "the ended task's hold is gone"
        );
    });
}

#[test]
fn ac_dno_34_a_lease_hands_the_key_over_from_a_holder_that_outlives_it() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(LOCK)).await;
        let (got, _) = h.get("/?mode=lease&key=k", &p).await;
        assert_eq!(
            got.json(),
            json!({ "b": { "aDone": null } }),
            "B entered before A's callback finished"
        );
    });
}

#[test]
fn ac_dno_34_an_invalid_key_is_validation_failed() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(LOCK)).await;
        let (got, _) = h.get("/?mode=try&key=", &p).await;
        assert_eq!(got.json(), json!({ "error": "validation_failed" }));
    });
}
