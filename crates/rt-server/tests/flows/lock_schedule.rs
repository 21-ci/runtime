//! `std:schedule` and `std:lock` from Deno guests on a real node (and):
//! a schedule created from code reaches the service's `scheduled()` with its id and payload, the
//! same id rescheduled replaces the pending one, a pending schedule survives a restart and is
//! delivered at least once, a task killed while holding a lock frees the key without waiting for
//! the lease, and a guest pages `schedule.list` past one page.

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::common::*;

/// Schedules from code (`/after`, `/cancel`, `/list`, `/walk`) and records each `scheduled()`
/// delivery as a row (`fetch` on any other path lists the rows).
const SCHED_GUEST: &str = r#"
import { schedule } from "std:schedule";
import { sql } from "std:sql";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS got (id INTEGER PRIMARY KEY AUTOINCREMENT, sid TEXT, payload TEXT, scheduled_at INTEGER)");
}
export default {
  async fetch(req: Request): Promise<Response> {
    await ensure();
    const u = new URL(req.url);
    const q = (k: string) => u.searchParams.get(k);
    if (u.pathname === "/after") {
      const id = q("id");
      const page = await schedule.after(Number(q("seconds") ?? 1), { n: Number(q("n") ?? 0) }, id === null ? undefined : { id });
      return Response.json(page);
    }
    if (u.pathname === "/list") {
      return Response.json(await schedule.list({ limit: Number(q("limit") ?? 100) }));
    }
    if (u.pathname === "/walk") {
      // Pages through every pending schedule, `limit` at a time, following the cursor.
      const limit = Number(q("limit") ?? 2);
      const ids: string[] = [];
      let cursor: string | null = null;
      let pages = 0;
      for (;;) {
        const page = await schedule.list({ cursor: cursor ?? undefined, limit });
        pages++;
        for (const e of page.entries) ids.push(e.id);
        if (page.cursor === null || page.cursor === undefined || page.entries.length === 0 || pages > 50) break;
        cursor = page.cursor;
      }
      return Response.json({ ids, pages });
    }
    const r = await sql.execute("SELECT sid, payload, scheduled_at FROM got ORDER BY id");
    return Response.json({ got: r.rows.map((x: any) => ({ sid: x[0], payload: JSON.parse(x[1]), scheduledAt: x[2] })) });
  },
  async scheduled(event: any, _ctx: any): Promise<void> {
    await ensure();
    await sql.execute("INSERT INTO got (sid, payload, scheduled_at) VALUES (?, ?, ?)", [event.id, JSON.stringify(event.payload), event.scheduledAt]);
  },
};"#;

/// Holds `k` for good (`/hold`: only the wall limit ends the task) and takes it with a wait
/// (`/take`), answering the value it got.
const LOCK_GUEST: &str = r#"
import { lock } from "std:lock";
export default {
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    if (u.pathname === "/hold") {
      return Response.json({ value: await lock("k", () => new Promise(() => {}), { leaseMs: 60000 }) });
    }
    const value = await lock("k", async () => "took", { waitMs: 5000, leaseMs: 60000 });
    return Response.json({ value });
  },
};"#;

/// The rows `SCHED_GUEST` recorded for its scheduled deliveries.
async fn rows_of(node: &impl Node, slug: &str) -> Vec<Value> {
    invoke(node, slug, "/").await.json["got"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Ship `src` with a wall-clock limit of `wall_ms` (the plan maximum is far above it).
async fn ship_with_wall(node: &impl Node, key: &str, slug: &str, src: &str, wall_ms: u64) -> String {
    let r = post(
        node,
        key,
        "/v1/services",
        json!({"slug": slug, "runtime": "deno", "limits": {"wall_ms": wall_ms}}),
    )
    .await;
    assert_eq!(r.s(), 201, "create service `{slug}`: {}", r.text);
    let id = r.json["id"].as_str().expect("service id").to_string();
    let r = upload_ts(node, key, &id, src, "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "upload to `{slug}`: {} {}", r.s(), r.text);
    id
}

/// SCH-E2E-1: a schedule created from code is delivered to `scheduled(event)` once, with the id it
/// was given, the payload it was created with, and the due time as `scheduledAt`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_e2e_09_a_schedule_from_code_reaches_scheduled_with_its_id_and_payload() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("sched");
    ship(&node, &t.key, &slug, SCHED_GUEST).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200); // build

    let made = invoke(&node, &slug, "/after?seconds=1&id=digest-1&n=7").await;
    assert_eq!(made.s(), 200, "{}", made.text);
    assert_eq!(made.json["id"], json!("digest-1"));
    let run_at = made.json["runAt"].clone();

    let (n, s) = (&node, slug.clone());
    eventually(Duration::from_secs(15), "the schedule to run", || {
        let s = s.clone();
        async move { !rows_of(n, &s).await.is_empty() }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let got = rows_of(&node, &slug).await;
    assert_eq!(got.len(), 1, "one delivery for one schedule: {got:?}");
    assert_eq!(got[0]["sid"], json!("digest-1"), "event.id");
    assert_eq!(got[0]["payload"], json!({"n": 7}), "event.payload");
    assert_eq!(got[0]["scheduledAt"], run_at, "event.scheduledAt is the due time");
    node.shutdown().await;
}

/// SCH-E2E-2: a schedule with an id that is still pending is replaced by the next one with the same
/// id: one entry in the list, one delivery, carrying the replacing payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_e2e_10_rescheduling_an_id_replaces_the_pending_schedule() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("resched");
    ship(&node, &t.key, &slug, SCHED_GUEST).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200); // build

    assert_eq!(invoke(&node, &slug, "/after?seconds=2&id=dup&n=1").await.s(), 200);
    assert_eq!(invoke(&node, &slug, "/after?seconds=2&id=dup&n=2").await.s(), 200);
    let pending = invoke(&node, &slug, "/list").await.json["entries"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        pending.iter().map(|e| e["id"].clone()).collect::<Vec<_>>(),
        vec![json!("dup")],
        "one pending entry"
    );

    let (n, s) = (&node, slug.clone());
    eventually(Duration::from_secs(15), "the schedule to run", || {
        let s = s.clone();
        async move { !rows_of(n, &s).await.is_empty() }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let got = rows_of(&node, &slug).await;
    assert_eq!(got.len(), 1, "the replaced schedule never runs: {got:?}");
    assert_eq!(got[0]["payload"], json!({"n": 2}), "the replacing payload runs");
    node.shutdown().await;
}

/// SCH-E2E-3: schedules are persisted. A schedule still pending across a graceful restart stays
/// listed; one that came due while the node was down is delivered after the restart (at-least-once:
/// at least one delivery with its id and payload).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_e2e_11_a_pending_schedule_survives_a_restart_and_is_delivered_at_least_once() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("sched-restart");
    ship(&node, &t.key, &slug, SCHED_GUEST).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200); // build

    assert_eq!(
        invoke(&node, &slug, "/after?seconds=2&id=due-while-down&n=1").await.s(),
        200
    );
    assert_eq!(
        invoke(&node, &slug, "/after?seconds=3600&id=still-pending&n=2")
            .await
            .s(),
        200
    );
    node.stop().await;
    tokio::time::sleep(Duration::from_millis(3_000)).await; // the first one comes due while down
    node.start().await;

    let listed = invoke(&node, &slug, "/list").await.json["entries"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let ids: Vec<Value> = listed.iter().map(|e| e["id"].clone()).collect();
    assert!(
        ids.contains(&json!("still-pending")),
        "the pending schedule survives the restart: {ids:?}"
    );

    let (n, s) = (&node, slug.clone());
    eventually(
        Duration::from_secs(20),
        "the overdue schedule to run after the restart",
        || {
            let s = s.clone();
            async move { rows_of(n, &s).await.iter().any(|r| r["sid"] == json!("due-while-down")) }
        },
    )
    .await;
    let got = rows_of(&node, &slug).await;
    let mine: Vec<&Value> = got.iter().filter(|r| r["sid"] == json!("due-while-down")).collect();
    assert!(!mine.is_empty(), "at least one delivery: {got:?}");
    assert!(
        mine.iter().all(|r| r["payload"] == json!({"n": 1})),
        "every delivery carries the payload: {mine:?}"
    );
    assert!(
        got.iter().all(|r| r["sid"] != json!("still-pending")),
        "a schedule not yet due does not run"
    );
    node.stop().await;
}

/// SCH-E2E-4: `schedule.list({ cursor, limit })` from a guest pages through more than one page and
/// returns every pending schedule exactly once, in id order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_e2e_12_a_guest_pages_schedule_list_past_one_page() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("sched-page");
    ship(&node, &t.key, &slug, SCHED_GUEST).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200); // build

    let mut want = Vec::new();
    for i in 1..=5 {
        let id = format!("p-{i}");
        assert_eq!(
            invoke(&node, &slug, &format!("/after?seconds=3600&id={id}&n={i}"))
                .await
                .s(),
            200
        );
        want.push(id);
    }
    let walked = invoke(&node, &slug, "/walk?limit=2").await;
    assert_eq!(walked.s(), 200, "{}", walked.text);
    assert_eq!(
        walked.json["ids"],
        json!(want),
        "every pending schedule once, in id order"
    );
    assert!(
        walked.json["pages"].as_u64().unwrap_or(0) > 1,
        "more than one page: {}",
        walked.text
    );
    node.shutdown().await;
}

/// SCH-E2E-5: a task killed by its wall limit while holding `std:lock` frees the key at once: the
/// next caller takes it well before the 60 s lease of the killed hold would run out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_e2e_13_a_killed_task_releases_its_lock_before_the_lease_runs_out() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("lock-kill");
    ship_with_wall(&node, &t.key, &slug, LOCK_GUEST, 2_000).await;
    assert_eq!(invoke(&node, &slug, "/take").await.s(), 200); // build; takes and releases `k`

    let holder = invoke(&node, &slug, "/hold").await;
    assert_ne!(
        holder.s(),
        200,
        "the holder is killed by its wall limit: {}",
        holder.text
    );

    let started = Instant::now();
    let taken = invoke(&node, &slug, "/take").await;
    let waited = started.elapsed();
    assert_eq!(taken.s(), 200, "{}", taken.text);
    assert_eq!(
        taken.json,
        json!({ "value": "took" }),
        "the key was released by the kill: {}",
        taken.text
    );
    assert!(
        waited < Duration::from_secs(5),
        "the key was free, not granted by lease expiry: waited {waited:?}"
    );
    node.shutdown().await;
}
