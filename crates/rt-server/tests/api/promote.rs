//! `promote()` (`docs/design/branching-services-design.md` §5): every flip is tracked by a
//! `version_switches` row, the flip is one transaction, a route refresh that keeps failing still
//! answers, and a drain ends only in a state that is true.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use rt_api::promote::RouteRefresh;
use rt_api::ApiError;
use rt_sched::Clock;
use rt_server::StartOptions;
use rt_store::{MetaStore, PageReq, SqliteMetaStore, TenantScope};
use rt_testkit::{NodeHarness, VirtualClock};
use rt_types::{ServiceId, Switch, SwitchKind, SwitchState, VersionNo};
use serde_json::json;

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

/// Fails the next `failures` route refreshes of `promote()`, then passes them on.
struct FlakyRefresh {
    inner: Arc<dyn RouteRefresh>,
    failures: Arc<AtomicU32>,
}

#[async_trait]
impl RouteRefresh for FlakyRefresh {
    async fn refresh(&self) -> Result<(), ApiError> {
        let left = self.failures.load(Ordering::SeqCst);
        if left > 0 {
            self.failures.store(left - 1, Ordering::SeqCst);
            return Err(ApiError::new("internal", "injected"));
        }
        self.inner.refresh().await
    }
}

struct Rig {
    node: NodeHarness,
    t: Tenant,
    slug: String,
    id: String,
    svc: ServiceId,
    meta: Arc<dyn MetaStore>,
    clock: Arc<VirtualClock>,
    failures: Arc<AtomicU32>,
}

/// `/`: the version's tag. `/hold?ms=`: records that it started, then waits (an old task).
/// `/started`: how many holds started.
fn guest(tag: &str) -> String {
    format!(
        r#"import {{ sql }} from "std:sql";
export default {{
  async fetch(req: Request): Promise<Response> {{
    const u = new URL(req.url);
    await sql.execute("CREATE TABLE IF NOT EXISTS h (id INTEGER PRIMARY KEY)");
    if (u.pathname === "/hold") {{
      await sql.execute("INSERT INTO h DEFAULT VALUES");
      await new Promise((r) => setTimeout(r, Number(u.searchParams.get("ms"))));
      return new Response("{tag}");
    }}
    if (u.pathname === "/started") {{
      const r = await sql.execute("SELECT count(*) FROM h");
      return new Response(String(r.rows[0][0]));
    }}
    return new Response("{tag}");
  }},
}};"#
    )
}

async fn rig() -> Rig {
    let clock = Arc::new(VirtualClock::new());
    let failures = Arc::new(AtomicU32::new(0));
    let captured: Arc<OnceLock<Arc<dyn MetaStore>>> = Arc::new(OnceLock::new());
    let (slot, fails) = (captured.clone(), failures.clone());
    let opts = StartOptions {
        meta_layer: Some(Box::new(move |m| {
            let _ = slot.set(m.clone());
            m
        })),
        promote_clock: Some(clock.clone() as Arc<dyn Clock>),
        route_refresh_layer: Some(Box::new(move |inner| {
            Arc::new(FlakyRefresh { inner, failures: fails }) as Arc<dyn RouteRefresh>
        })),
        ..StartOptions::default()
    };
    let node = start_node_opts(opts, None, |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("prm");
    let id = ship(&node, &t.key, &slug, &guest("v1")).await;
    let meta = captured.get().expect("the meta layer ran").clone();
    let svc: ServiceId = id.parse().expect("service id");
    Rig {
        node,
        t,
        slug,
        id,
        svc,
        meta,
        clock,
        failures,
    }
}

impl Rig {
    /// Upload version `n` without activating it, and build it.
    async fn add(&self, n: u32) {
        let r = upload_ts(&self.node, &self.t.key, &self.id, &guest(&format!("v{n}")), "").await;
        assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    }

    async fn live(&self, n: u32) -> Resp {
        put(
            &self.node,
            &self.t.key,
            &format!("/v1/services/{}/live", self.id),
            json!({"version": n}),
        )
        .await
    }

    async fn serves(&self) -> String {
        invoke(&self.node, &self.slug, "/").await.text
    }

    /// Newest first.
    async fn rows(&self) -> Vec<Switch> {
        self.meta
            .list_switches(
                self.svc,
                PageReq {
                    limit: 100,
                    cursor: None,
                },
            )
            .await
            .expect("switches")
            .items
    }

    async fn row_to(&self, to: u32) -> Switch {
        self.rows()
            .await
            .into_iter()
            .find(|r| r.to_version == VersionNo(to))
            .expect("the switch row")
    }

    /// Start a hold on the live version and return when it runs; polled next to the body.
    async fn wait_started(&self, n: u64) {
        eventually(Duration::from_secs(20), "the hold to start", || async move {
            invoke(&self.node, &self.slug, "/started").await.text == n.to_string()
        })
        .await;
    }

    async fn wait_state(&self, to: u32, state: SwitchState) -> Switch {
        let what = format!("the switch to v{to} to reach {state:?}");
        eventually(Duration::from_secs(20), &what, || async move {
            self.row_to(to).await.state == state
        })
        .await;
        self.row_to(to).await
    }

    async fn events(&self) -> Vec<String> {
        let r = get(&self.node, &self.t.key, &format!("/v1/services/{}/events", self.id)).await;
        r.items()
            .iter()
            .filter_map(|e| e["kind"].as_str().map(str::to_string))
            .collect()
    }

    async fn wait_event(&self, kind: &str) {
        let what = format!("event {kind}");
        eventually(Duration::from_secs(10), &what, || async move {
            self.events().await.iter().any(|k| k == kind)
        })
        .await;
    }
}

node_test! {
/// Activating an upload, `PUT /live`: each flip has a row, nothing to drain completes at once.
async fn every_flip_writes_a_tracked_row_and_answers_as_before() {
    let r = rig().await;
    let first = r.row_to(1).await;
    assert_eq!((first.kind, first.state, first.from_version), (SwitchKind::Promote, SwitchState::Completed, None));
    assert!(first.created_by.is_some(), "the actor is recorded");

    r.add(2).await;
    let resp = r.live(2).await;
    assert_eq!(resp.s(), 200, "{}", resp.text);
    assert_eq!(resp.json["live_version"], json!(2));
    let row = r.row_to(2).await;
    assert_eq!(resp.json["switch_id"], json!(row.id.to_string()));
    assert!(resp.json.get("warnings").is_none(), "{}", resp.text);
    assert_eq!(row.from_version, Some(VersionNo(1)));
    assert_eq!(r.serves().await, "v2");
    r.wait_state(2, SwitchState::Completed).await;
    r.wait_event("service.switch.started").await;
    r.wait_event("service.switch.flipped").await;
    r.wait_event("service.switch.drained").await;
    r.node.shutdown().await;
}
}

node_test! {
/// A refused flip leaves the live version and closes its row with a stable code.
async fn a_refused_flip_is_atomic_and_its_row_records_a_stable_code() {
    let r = rig().await;
    r.add(2).await;
    let stale = send(
        api(&r.node, &r.t.key, reqwest::Method::PUT, &format!("/v1/services/{}/live", r.id))
            .header("if-match", "\"999999\"")
            .json(&json!({"version": 2})),
    )
    .await;
    assert_eq!(stale.s(), 412, "{}", stale.text);
    assert_eq!(stale.code(), "precondition_failed");
    assert_eq!(r.serves().await, "v1");
    let row = r.row_to(2).await;
    assert_eq!((row.state, row.error.as_deref()), (SwitchState::Failed, Some("precondition_failed")));
    r.wait_event("service.switch.failed").await;

    let missing = r.live(99).await;
    assert_eq!(missing.s(), 404, "{}", missing.text);
    // The failed row did not block the next flip.
    assert_eq!(r.live(2).await.s(), 200);
    assert_eq!(r.serves().await, "v2");
    r.node.shutdown().await;
}
}

node_test! {
/// A store failure at each step in turn: an error means the old version is live and was not
/// flipped; a success means the new one. The flip never half-happens, and a row whose closing
/// failed too does not block the flip after it.
async fn the_flip_commits_or_nothing_changed() {
    let faults = rt_store::Faults::new();
    let mut opts = StartOptions::default();
    opts.faults.meta = Some(faults.clone());
    let node = start_node_opts(opts, None, |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("prmf");
    let id = ship(&node, &t.key, &slug, &guest("v1")).await;
    let up = upload_ts(&node, &t.key, &id, &guest("v2"), "").await;
    assert!(up.s() == 201 || up.s() == 202);
    assert_eq!(invoke(&node, &slug, "/").await.text, "v1");
    let path = format!("/v1/services/{id}/live");
    for failing in [1, 2] {
        for pass in 0..8 {
            faults.clear();
            faults.pass_next(pass);
            faults.fail_next(failing, rt_store::Fault::Io);
            let resp = put(&node, &t.key, &path, json!({"version": 2})).await;
            faults.clear();
            if resp.s() < 300 {
                // Past the commit point; go back for the next round.
                let back = put(&node, &t.key, &path, json!({"version": 1})).await;
                assert_eq!(back.s(), 200, "{}", back.text);
                break;
            }
            assert!(!resp.text.contains("injected"), "{}", resp.text);
            assert_eq!(invoke(&node, &slug, "/").await.text, "v1", "{}", resp.text);
        }
    }
    let resp = put(&node, &t.key, &path, json!({"version": 2})).await;
    assert_eq!(resp.s(), 200, "{}", resp.text);
    assert_eq!(invoke(&node, &slug, "/").await.text, "v2");
    node.shutdown().await;
}
}

node_test! {
/// One failed refresh is retried and nobody hears about it.
async fn a_refresh_that_recovers_within_the_retries_answers_200() {
    let r = rig().await;
    r.add(2).await;
    r.failures.store(2, Ordering::SeqCst);
    let resp = r.live(2).await;
    assert_eq!(resp.s(), 200, "{}", resp.text);
    assert!(resp.json.get("warnings").is_none(), "{}", resp.text);
    assert_eq!(r.failures.load(Ordering::SeqCst), 0);
    assert_eq!(r.serves().await, "v2");
    r.node.shutdown().await;
}
}

node_test! {
/// The flip committed but the routes could not be re-read: `202` with `routes_refresh_delayed`,
/// not an error that invites a retry, and the table is repaired in the background.
async fn a_refresh_that_keeps_failing_answers_202_with_a_warning_and_is_repaired() {
    let r = rig().await;
    r.add(2).await;
    r.failures.store(4, Ordering::SeqCst); // the attempt and its three retries
    let resp = r.live(2).await;
    assert_eq!(resp.s(), 202, "{}", resp.text);
    assert_eq!(resp.json["live_version"], json!(2), "{}", resp.text);
    assert_eq!(resp.json["warnings"], json!([{"code": "routes_refresh_delayed"}]), "{}", resp.text);
    let row = r.row_to(2).await;
    assert_ne!(row.state, SwitchState::Failed, "the flip happened");
    assert_eq!(row.from_version, Some(VersionNo(1)));
    let got = r.meta.get_service(TenantScope::Operator, r.svc).await.expect("get").expect("svc");
    assert_eq!(got.live_version, Some(VersionNo(2)));
    let (n, s) = (&r.node, &r.slug);
    eventually(Duration::from_secs(10), "the repair to serve v2", || async move {
        invoke(n, s, "/").await.text == "v2"
    })
    .await;
    r.node.shutdown().await;
}
}

node_test! {
/// The same on an upload with `activate`: `202` and the warning, version created.
async fn an_upload_activation_reports_the_delayed_refresh_too() {
    let r = rig().await;
    r.failures.store(4, Ordering::SeqCst);
    let up = upload_ts(&r.node, &r.t.key, &r.id, &guest("v2"), "activate=1").await;
    assert_eq!(up.s(), 202, "{}", up.text);
    assert_eq!(up.json["version"], json!(2));
    assert_eq!(up.json["warnings"], json!([{"code": "routes_refresh_delayed"}]), "{}", up.text);
    r.node.shutdown().await;
}
}

node_test! {
/// The drain waits for the old task and only then says `completed`; virtual time has not moved.
async fn a_drain_completes_when_the_old_task_ends() {
    let r = rig().await;
    r.add(2).await;
    let hold = invoke(&r.node, &r.slug, "/hold?ms=2500");
    let body = async {
        r.wait_started(1).await;
        assert_eq!(r.live(2).await.s(), 200);
        assert_eq!(r.serves().await, "v2", "new traffic runs the new version");
        let row = r.row_to(2).await;
        assert_eq!(row.state, SwitchState::Draining, "the old task still runs");
        assert!(row.drain_deadline_ms.expect("deadline") > r.clock.unix_ms());
        r.wait_state(2, SwitchState::Completed).await;
    };
    let (held, ()) = tokio::join!(hold, body);
    assert_eq!(held.text, "v1", "the old task finished on the old version");
    r.wait_event("service.switch.drained").await;
    assert!(!r.row_to(2).await.interrupted);
    r.node.shutdown().await;
}
}

node_test! {
/// Past the deadline with the old task running the row says `drain_timed_out`, never `completed`.
async fn a_drain_times_out_at_its_deadline() {
    let r = rig().await;
    r.add(2).await;
    let hold = invoke(&r.node, &r.slug, "/hold?ms=3000");
    let body = async {
        r.wait_started(1).await;
        assert_eq!(r.live(2).await.s(), 200);
        let row = r.row_to(2).await;
        assert_eq!(row.state, SwitchState::Draining);
        let left = row.drain_deadline_ms.expect("deadline") - r.clock.unix_ms();
        r.clock.step_unix_ms(left - 1);
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(r.row_to(2).await.state, SwitchState::Draining, "one ms before the deadline");
        r.clock.step_unix_ms(1);
        let row = r.wait_state(2, SwitchState::DrainTimedOut).await;
        assert!(row.error.is_none(), "it simply ran out of time");
    };
    let (held, ()) = tokio::join!(hold, body);
    assert_eq!(held.text, "v1", "no force kill in v1");
    r.wait_event("service.switch.drain_timed_out").await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(r.row_to(2).await.state, SwitchState::DrainTimedOut, "a closed row stays closed");
    r.node.shutdown().await;
}
}

node_test! {
/// A third version with only idle versions around: v2 has no task, so v1 stays counted and its
/// drain ends truthfully as `completed`.
async fn a_third_version_keeps_a_drain_known_while_the_second_is_idle() {
    let r = rig().await;
    r.add(2).await;
    r.add(3).await;
    let hold = invoke(&r.node, &r.slug, "/hold?ms=2500");
    let body = async {
        r.wait_started(1).await;
        assert_eq!(r.live(2).await.s(), 200);
        assert_eq!(r.serves().await, "v2");
        assert_eq!(r.live(3).await.s(), 200, "a new flip is not refused during a drain");
        assert_eq!(r.serves().await, "v3");
        assert_eq!(r.row_to(2).await.state, SwitchState::Draining);
        r.wait_state(2, SwitchState::Completed).await;
    };
    let (held, ()) = tokio::join!(hold, body);
    assert_eq!(held.text, "v1");
    r.node.shutdown().await;
}
}

node_test! {
/// v1 and v2 both run tasks when v3 arrives: the counter pushes v1 out, so its drain closes as
/// `drain_timed_out` + `drain_unknown` and never says `completed`; v2 stays tracked. Back on v1
/// and on to v4, the drain of v1 is unknown at once: its count misses the old task.
async fn a_version_pushed_out_with_tasks_is_never_reported_drained() {
    let r = rig().await;
    r.add(2).await;
    r.add(3).await;
    r.add(4).await;
    let hold_v1 = invoke(&r.node, &r.slug, "/hold?ms=6000");
    let body = async {
        r.wait_started(1).await;
        assert_eq!(r.live(2).await.s(), 200);
        let hold_v2 = invoke(&r.node, &r.slug, "/hold?ms=1500");
        let inner = async {
            r.wait_started(2).await;
            assert_eq!(r.live(3).await.s(), 200);
            assert_eq!(r.serves().await, "v3");
            let first = r.wait_state(2, SwitchState::DrainTimedOut).await;
            assert_eq!(first.error.as_deref(), Some("drain_unknown"));
            r.wait_event("service.switch.drain_timed_out").await;
        };
        let (held, ()) = tokio::join!(hold_v2, inner);
        assert_eq!(held.text, "v2");
        r.wait_state(3, SwitchState::Completed).await; // v2's own drain was tracked

        assert_eq!(r.live(1).await.s(), 200);
        assert_eq!(r.serves().await, "v1");
        assert_eq!(r.live(4).await.s(), 200);
        let row = r.row_to(4).await;
        assert_eq!(row.from_version, Some(VersionNo(1)));
        assert_eq!(
            (row.state, row.error.as_deref()),
            (SwitchState::DrainTimedOut, Some("drain_unknown")),
            "the old v1 task is still running and the counter cannot see it"
        );
    };
    let (held, ()) = tokio::join!(hold_v1, body);
    assert_eq!(held.text, "v1");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(r.row_to(2).await.state, SwitchState::DrainTimedOut, "a closed row stays closed");
    r.node.shutdown().await;
}
}

node_test! {
/// v1 → v2 → v1 keeps both counted: the first drain is moot, and v1 stays trustworthy.
async fn going_back_during_a_drain_keeps_both_versions_counted() {
    let r = rig().await;
    r.add(2).await;
    r.add(3).await;
    let hold = invoke(&r.node, &r.slug, "/hold?ms=2500");
    let body = async {
        r.wait_started(1).await;
        assert_eq!(r.live(2).await.s(), 200);
        assert_eq!(r.serves().await, "v2");
        assert_eq!(r.live(1).await.s(), 200);
        let first = r.row_to(2).await;
        assert_eq!((first.state, first.error.as_deref()), (SwitchState::DrainTimedOut, Some("drain_unknown")));
        r.wait_state(1, SwitchState::Completed).await; // the drain of v2, which has no task
        assert_eq!(r.live(3).await.s(), 200);
        assert_eq!(r.serves().await, "v3");
        assert_eq!(r.row_to(3).await.state, SwitchState::Draining, "the v1 count is still known");
        r.wait_state(3, SwitchState::Completed).await;
    };
    let (held, ()) = tokio::join!(hold, body);
    assert_eq!(held.text, "v1");
    r.node.shutdown().await;
}
}

node_test! {
/// A flip to the version already live is tracked and leaves a running drain alone.
async fn a_flip_to_the_live_version_does_not_touch_the_running_drain() {
    let r = rig().await;
    r.add(2).await;
    let hold = invoke(&r.node, &r.slug, "/hold?ms=2500");
    let body = async {
        r.wait_started(1).await;
        assert_eq!(r.live(2).await.s(), 200);
        let draining = r.row_to(2).await.id;
        assert_eq!(r.live(2).await.s(), 200);
        let row = r.meta.get_switch(r.svc, draining).await.expect("get").expect("row");
        assert_eq!(row.state, SwitchState::Draining);
        let what = "the drain to finish";
        eventually(Duration::from_secs(20), what, || async {
            r.meta.get_switch(r.svc, draining).await.expect("get").expect("row").state == SwitchState::Completed
        })
        .await;
    };
    let (held, ()) = tokio::join!(hold, body);
    assert_eq!(held.text, "v1");
    r.node.shutdown().await;
}
}

node_test! {
/// A service deleted during a drain ends the drain without an error or a stuck task.
async fn deleting_the_service_during_a_drain_ends_it() {
    let r = rig().await;
    r.add(2).await;
    let hold = invoke(&r.node, &r.slug, "/hold?ms=1000");
    let body = async {
        r.wait_started(1).await;
        assert_eq!(r.live(2).await.s(), 200);
        let del = delete(&r.node, &r.t.key, &format!("/v1/services/{}", r.id)).await;
        assert!(del.s() < 300, "{}", del.text);
    };
    let _ = tokio::join!(hold, body);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(r.rows().await.is_empty());
    r.node.shutdown().await;
}
}

/// §5 crash recovery: a restart closes what was open. Routes come from `meta.db`, so what the row
/// flipped is what runs; `draining` closes as `completed` flagged `interrupted`, `preparing` fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_closes_the_open_switches_and_keeps_the_flip() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("prmr");
    let id = ship(&node, &t.key, &slug, &guest("v1")).await;
    for n in [2, 3] {
        let r = upload_ts(&node, &t.key, &id, &guest(&format!("v{n}")), "").await;
        assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    }
    assert_eq!(invoke(&node, &slug, "/").await.text, "v1");
    node.stop().await;

    let svc: ServiceId = id.parse().expect("id");
    let path = node.dir.path().join("meta.db");
    let (draining, preparing) = {
        let m = SqliteMetaStore::open(&path).expect("meta.db");
        let new = |to: u32| rt_types::NewSwitch {
            kind: SwitchKind::Promote,
            source: None,
            from_version: None,
            to_version: VersionNo(to),
            created_by: None,
        };
        let a = m.create_switch(svc, new(2)).await.expect("create");
        m.flip_switch(svc, a.id, None, i64::MAX).await.expect("flip");
        let b = m.create_switch(svc, new(3)).await.expect("create");
        (a.id, b.id)
    };

    node.start().await;
    assert_eq!(invoke(&node, &slug, "/").await.text, "v2", "the flip survived");
    let m = SqliteMetaStore::open(&path).expect("meta.db");
    let row = m.get_switch(svc, draining).await.expect("get").expect("row");
    assert_eq!((row.state, row.interrupted), (SwitchState::Completed, true));
    let row = m.get_switch(svc, preparing).await.expect("get").expect("row");
    assert_eq!(
        (row.state, row.interrupted, row.error.as_deref()),
        (SwitchState::Failed, true, Some("interrupted"))
    );
    // The interrupted row no longer blocks the next flip.
    let r = put(&node, &t.key, &format!("/v1/services/{id}/live"), json!({"version": 3})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(invoke(&node, &slug, "/").await.text, "v3");
    node.stop().await;
}
