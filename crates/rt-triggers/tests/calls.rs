//! Service-to-service calls: TRG-14..17 at the
//! `CallDispatcher` / `child_depth` level. The in-process "no TCP hop" and the guest-visible
//! behavior (508 body, parent sees a 504 response object) are covered end to end in rt-server.
//!
//! The mock executor cannot make real nested calls, so recursion is driven from the test: one
//! `dispatcher.call()` per level with a `TaskCtx` of the right depth, the way the host layer
//! builds it for a running task.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{drive, entry, new_sim, snapshot, FakeQueueStore, FixedPlans};
use parking_lot::Mutex;
use proptest::prelude::*;
use rt_host::{CallError, Calls, EnqueueOpts, Request, Response, TaskCtx};
use rt_sched::Clock;
use rt_store::{NewMessage, QueueStore};
use rt_testkit::{ids, MockEvent, Script, SimRuntime, SpecBuilder};
use rt_triggers::{child_depth, CallDispatcher, CallTaskFactory, RouteTable};
use rt_types::{Access, Plan, RouteEntry, ServiceRef, TaskId, TaskSpec, Trigger, MAX_CALL_DEPTH};

// ---------------------------------------------------------------------------------------------
// child_depth (pure)
// ---------------------------------------------------------------------------------------------

#[test]
fn max_call_depth_is_fifteen() {
    assert_eq!(MAX_CALL_DEPTH, 15);
}

#[test]
fn ac_trg_15_child_depth_allows_up_to_fifteen_and_rejects_beyond() {
    assert_eq!(child_depth(0), Ok(1));
    assert_eq!(child_depth(1), Ok(2));
    assert_eq!(child_depth(14), Ok(15));
    assert_eq!(child_depth(15), Err(CallError::Depth));
    assert_eq!(child_depth(16), Err(CallError::Depth));
    assert_eq!(child_depth(u8::MAX), Err(CallError::Depth), "no overflow wrap-around");
    assert_eq!(CallError::Depth.code(), "call_depth");
}

proptest! {
    #[test]
    fn child_depth_is_parent_plus_one_up_to_the_cap(parent in 0u8..=255) {
        match child_depth(parent) {
            Ok(d) => { prop_assert_eq!(d, parent + 1); prop_assert!(d <= MAX_CALL_DEPTH); }
            Err(e) => { prop_assert_eq!(e, CallError::Depth); prop_assert!(parent >= MAX_CALL_DEPTH); }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------------------------

const T1: u128 = 1;
const T2: u128 = 2;
const A: u128 = 1; // tenant 1, slug "a"
const B: u128 = 2; // tenant 1, slug "b"
const X: u128 = 3; // tenant 2, slug "x"

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenCall {
    parent_task: TaskId,
    parent_depth: u8,
    target: String,
    depth: u8,
    path: String,
}

/// Builds the child `TaskSpec` the way `rt-server` does: `Trigger::Call{parent, depth}`, default
/// call limits (wall 30 s). The dispatcher is responsible for clamping the wall to the parent's
/// remaining time.
struct ChildFactory {
    seen: Mutex<Vec<SeenCall>>,
    child_wall_ms: u64,
}

impl CallTaskFactory for ChildFactory {
    fn child_task(
        &self,
        parent: &TaskCtx,
        target: &RouteEntry,
        req: Request,
        depth: u8,
    ) -> Result<TaskSpec, CallError> {
        self.seen.lock().push(SeenCall {
            parent_task: parent.task,
            parent_depth: parent.depth,
            target: target.slug.clone(),
            depth,
            path: req.uri().path().to_string(),
        });
        Ok(SpecBuilder::deno()
            .service_ref(ServiceRef {
                tenant: target.tenant,
                service: target.service,
                version: target.version,
                digest: target.digest,
            })
            .trigger(Trigger::Call {
                parent: parent.task,
                depth,
            })
            .wall_limit(self.child_wall_ms)
            .finish())
    }
}

struct Rig {
    sim: SimRuntime,
    routes: Arc<RouteTable>,
    store: Arc<FakeQueueStore>,
    factory: Arc<ChildFactory>,
    dispatcher: Arc<CallDispatcher>,
}

fn rig_with(entries: Vec<RouteEntry>, plans: Arc<FixedPlans>) -> Rig {
    let sim = new_sim();
    let routes = RouteTable::new(None);
    routes.swap(&snapshot(1, entries));
    let store = FakeQueueStore::new(sim.clock().clone(), 5);
    let factory = Arc::new(ChildFactory {
        seen: Mutex::new(Vec::new()),
        child_wall_ms: 30_000,
    });
    let dispatcher = Arc::new(CallDispatcher::new(
        routes.clone(),
        sim.scheduler().clone(),
        store.clone(),
        plans,
        factory.clone(),
    ));
    Rig {
        sim,
        routes,
        store,
        factory,
        dispatcher,
    }
}

fn default_entries() -> Vec<RouteEntry> {
    vec![entry(T1, A, "a", 1), entry(T1, B, "b", 1), entry(T2, X, "x", 1)]
}

fn rig() -> Rig {
    rig_with(default_entries(), FixedPlans::new(Plan::default()))
}

fn ctx_for(tenant_n: u128, service_n: u128, task: u64, depth: u8, deadline: Instant) -> TaskCtx {
    TaskCtx {
        tenant: ids::tenant(tenant_n),
        service: ids::service(service_n),
        task: TaskId(task),
        depth,
        deadline,
        trace: None,
        on_behalf_of: None,
        object: None,
    }
}

fn get(path: &str) -> Request {
    http::Request::builder()
        .method("GET")
        .uri(format!("http://svc{path}"))
        .body(rt_types::Body::empty())
        .expect("request")
}

/// Run `dispatcher.call` as a local task and step virtual time until it resolves (max `limit`).
async fn run_call(
    r: &mut Rig,
    ctx: TaskCtx,
    slug: &'static str,
    path: &'static str,
    limit: Duration,
) -> (Result<Response, CallError>, Duration) {
    let d = r.dispatcher.clone();
    let h = tokio::task::spawn_local(async move { d.call(&ctx, slug, get(path)).await });
    let start = r.sim.clock().elapsed();
    let step = Duration::from_millis(20);
    let mut waited = Duration::ZERO;
    while !h.is_finished() && waited < limit {
        drive(&mut r.sim, step, step).await;
        waited += step;
    }
    let elapsed = r.sim.clock().elapsed() - start;
    (h.await.expect("call task"), elapsed)
}

fn started_services(r: &Rig) -> Vec<rt_types::ServiceId> {
    r.sim
        .scripts()
        .events()
        .into_iter()
        .filter_map(|e| {
            if let MockEvent::Started { service, .. } = e {
                Some(service)
            } else {
                None
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// TRG-14
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_14_call_runs_a_child_task_of_the_target_in_process() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            let ctx = ctx_for(T1, A, 7, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx, "b", "/charge", Duration::from_secs(2)).await;
            let resp = res.expect("call succeeds");
            assert_eq!(resp.status(), 200, "the caller gets the target's response");

            let seen = r.factory.seen.lock().clone();
            assert_eq!(
                seen,
                vec![SeenCall {
                    parent_task: TaskId(7),
                    parent_depth: 0,
                    target: "b".into(),
                    depth: 1,
                    path: "/charge".into()
                }]
            );
            assert_eq!(
                started_services(&r),
                vec![ids::service(B)],
                "exactly one child task, of service b"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn call_to_an_unknown_slug_is_not_found_and_starts_nothing() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx, "nope", "/", Duration::from_secs(1)).await;
            assert_eq!(res.expect_err("unknown slug"), CallError::NotFound);
            assert_eq!(r.sim.scripts().started(), 0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn call_respects_the_targets_trigger_switch_and_treats_internal_access_as_callable() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut no_call = entry(T1, B, "b", 1);
            no_call.triggers.call = false;
            let mut internal = entry(T1, 9, "hidden", 1);
            internal.access = Access::Internal;
            let mut r = rig_with(
                vec![entry(T1, A, "a", 1), no_call, internal],
                FixedPlans::new(Plan::default()),
            );

            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx.clone(), "b", "/", Duration::from_secs(1)).await;
            assert_eq!(
                res.expect_err("call trigger disabled on the target"),
                CallError::Forbidden
            );

            // `access=internal` services are reachable from same-tenant callers (internal-tenant principal).
            let (res, _) = run_call(&mut r, ctx, "hidden", "/", Duration::from_secs(2)).await;
            assert_eq!(res.expect("internal service is callable").status(), 200);
        })
        .await;
}

// ---------------------------------------------------------------------------------------------
// TRG-15
// ---------------------------------------------------------------------------------------------

/// A recursive self-call chain: depth 15 is the last allowed; the 16th level gets `call_depth`,
/// every started task finishes and all memory reservations are released.
#[tokio::test(flavor = "current_thread")]
async fn ac_trg_15_recursive_calls_stop_at_depth_15_and_release_everything() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            r.sim
                .scripts()
                .set_for_service(ids::service(A), Script::new().cpu_ms(1).io_ms(5));
            let deadline = r.sim.clock().now() + Duration::from_secs(600);

            // The task at depth d (0 = the HTTP request) calls "a" again.
            let mut ok = 0u32;
            let mut refused_at = None;
            for depth in 0u8..=16 {
                let ctx = ctx_for(T1, A, 100 + u64::from(depth), depth, deadline);
                let (res, _) = run_call(&mut r, ctx, "a", "/recurse", Duration::from_secs(2)).await;
                match res {
                    Ok(resp) => {
                        assert_eq!(resp.status(), 200);
                        ok += 1;
                    }
                    Err(CallError::Depth) => {
                        refused_at = Some(depth);
                        break;
                    }
                    Err(e) => panic!("unexpected error at depth {depth}: {e:?}"),
                }
            }
            assert_eq!(refused_at, Some(15), "a task at depth 15 may not call: 508 call_depth");
            assert_eq!(ok, 15, "callers at depth 0..=14 each started one child");

            let seen = r.factory.seen.lock().clone();
            assert_eq!(seen.len(), 15);
            assert_eq!(seen.last().expect("last").depth, 15, "deepest child runs at depth 15");
            assert_eq!(r.sim.scripts().started(), 15, "the refused call started nothing");

            // All tasks finished; reservations released.
            let snap = r.sim.snapshot();
            assert_eq!(snap.inflight, 0, "no task left in flight");
            assert_eq!(r.sim.governor().committed(), 0, "reservations released");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn call_beyond_the_depth_limit_never_reaches_the_task_factory() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            let ctx = ctx_for(T1, A, 1, 15, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx, "b", "/", Duration::from_secs(1)).await;
            assert_eq!(res.expect_err("depth"), CallError::Depth);
            assert!(r.factory.seen.lock().is_empty(), "refused before building a child spec");
            assert_eq!(r.sim.scripts().started(), 0);
        })
        .await;
}

// ---------------------------------------------------------------------------------------------
// TRG-16
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_16_cross_tenant_call_needs_the_callers_plan_and_the_targets_id_listing() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Default plans: cross-tenant calls off.
            let mut r = rig();
            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx.clone(), "x", "/", Duration::from_secs(1)).await;
            let err = res.expect_err("t1 calling t2's service");
            assert_eq!(err, CallError::CrossTenant, "403 cross_tenant_call");
            assert_eq!(err.code(), "cross_tenant_call");
            assert_eq!(r.sim.scripts().started(), 0);
            assert!(r.factory.seen.lock().is_empty());

            // Operator-enabled `plan.allow_cross_tenant_calls` for t1 is not enough: the target
            // must list the caller by service id (an empty list, `env:` or slug entries never
            // admit another tenant).
            let allow = Plan {
                allow_cross_tenant_calls: true,
                ..Plan::default()
            };
            let plans = FixedPlans::with_tenant(Plan::default(), ids::tenant(T1), allow.clone());
            let mut r = rig_with(default_entries(), plans);
            let (res, _) = run_call(&mut r, ctx.clone(), "x", "/", Duration::from_secs(1)).await;
            assert_eq!(res.expect_err("not listed by the target"), CallError::NotAllowed);
            let mut x_by_slug = entry(T2, X, "x", 1);
            x_by_slug.call.allowed_callers = vec!["a".into(), "env:production".into()];
            let plans = FixedPlans::with_tenant(Plan::default(), ids::tenant(T1), allow.clone());
            let mut r = rig_with(vec![entry(T1, A, "a", 1), x_by_slug], plans);
            let (res, _) = run_call(&mut r, ctx.clone(), "x", "/", Duration::from_secs(1)).await;
            assert_eq!(
                res.expect_err("slug / env entries do not admit other tenants"),
                CallError::NotAllowed
            );

            let mut x_by_id = entry(T2, X, "x", 1);
            x_by_id.call.allowed_callers = vec![ids::service(A).to_string()];
            let plans = FixedPlans::with_tenant(Plan::default(), ids::tenant(T1), allow);
            let mut r = rig_with(vec![entry(T1, A, "a", 1), x_by_id], plans);
            let (res, _) = run_call(&mut r, ctx, "x", "/", Duration::from_secs(2)).await;
            assert_eq!(
                res.expect("plan allows and the target lists the caller's id").status(),
                200
            );
            assert_eq!(started_services(&r), vec![ids::service(X)]);
        })
        .await;
}

// ---------------------------------------------------------------------------------------------
// TRG-17
// ---------------------------------------------------------------------------------------------

/// Parent has 1 s of wall left, the child's own limit is 30 s and it needs 2 s: the child is
/// killed at ≈ 1 s with `timeout`; the parent sees a 504 response object (not an error).
#[tokio::test(flavor = "current_thread")]
async fn ac_trg_17_child_wall_is_clamped_to_the_parents_remaining_time() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            r.sim
                .scripts()
                .set_for_service(ids::service(B), Script::new().io_ms(2_000));
            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(1));
            let (res, elapsed) = run_call(&mut r, ctx, "b", "/slow", Duration::from_secs(5)).await;
            let resp = res.expect("a killed child is a response object, not a CallError");
            assert_eq!(resp.status(), 504);
            assert_eq!(
                resp.headers().get("x-rt-reason").map(|v| v.to_str().unwrap_or("")),
                Some("timeout")
            );
            assert!(
                elapsed >= Duration::from_millis(950) && elapsed <= Duration::from_millis(1_300),
                "child must die at ≈ the parent's deadline (1 s), took {elapsed:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn child_with_enough_parent_time_runs_to_completion() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            r.sim
                .scripts()
                .set_for_service(ids::service(B), Script::new().io_ms(2_000));
            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(10));
            let (res, elapsed) = run_call(&mut r, ctx, "b", "/slow", Duration::from_secs(5)).await;
            assert_eq!(res.expect("ok").status(), 200);
            assert!(
                elapsed >= Duration::from_millis(1_950) && elapsed <= Duration::from_millis(2_400),
                "took {elapsed:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn child_own_limit_wins_when_it_is_shorter_than_the_parents_remaining_time() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig();
            // Child wall 500 ms, parent has 30 s, child needs 2 s: killed at its own limit.
            let factory = Arc::new(ChildFactory {
                seen: Mutex::new(Vec::new()),
                child_wall_ms: 500,
            });
            r.dispatcher = Arc::new(CallDispatcher::new(
                r.routes.clone(),
                r.sim.scheduler().clone(),
                r.store.clone(),
                FixedPlans::new(Plan::default()),
                factory,
            ));
            r.sim
                .scripts()
                .set_for_service(ids::service(B), Script::new().io_ms(2_000));
            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, elapsed) = run_call(&mut r, ctx, "b", "/slow", Duration::from_secs(5)).await;
            assert_eq!(res.expect("response").status(), 504);
            assert!(
                elapsed <= Duration::from_millis(800),
                "own wall limit of 500 ms applies, took {elapsed:?}"
            );
        })
        .await;
}

// ---------------------------------------------------------------------------------------------
// std:queue enqueue through the dispatcher
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn enqueue_to_a_service_of_the_same_tenant_stores_body_delay_and_dedup() {
    let r = rig();
    let ctx = ctx_for(T1, A, 1, 0, Instant::now() + Duration::from_secs(30));
    let opts = EnqueueOpts {
        delay: Duration::from_secs(5),
        dedup_id: Some("job-1".to_string()),
    };
    let id = r
        .dispatcher
        .enqueue(&ctx, "b", Bytes::from_static(b"hello"), opts)
        .await
        .expect("enqueue");

    let stored = r.store.enqueued();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].0, ids::service(B));
    assert_eq!(stored[0].1.body, Bytes::from_static(b"hello"));
    assert_eq!(stored[0].1.delay, Duration::from_secs(5));
    assert_eq!(stored[0].1.dedup_id.as_deref(), Some("job-1"));
    assert_eq!(r.store.stats(ids::service(B)).await.expect("stats").depth, 1);
    let _ = id;
}

#[tokio::test]
async fn enqueue_errors_unknown_cross_tenant_disabled_and_full() {
    let ctx = ctx_for(T1, A, 1, 0, Instant::now() + Duration::from_secs(30));

    let r = rig();
    assert_eq!(
        r.dispatcher
            .enqueue(&ctx, "nope", Bytes::new(), EnqueueOpts::default())
            .await
            .expect_err("unknown"),
        CallError::NotFound
    );
    assert_eq!(
        r.dispatcher
            .enqueue(&ctx, "x", Bytes::new(), EnqueueOpts::default())
            .await
            .expect_err("cross tenant"),
        CallError::CrossTenant
    );
    assert!(r.store.enqueued().is_empty());

    // Queue trigger disabled on the target.
    let mut no_queue = entry(T1, B, "b", 1);
    no_queue.triggers.queue = false;
    let r = rig_with(vec![entry(T1, A, "a", 1), no_queue], FixedPlans::new(Plan::default()));
    assert_eq!(
        r.dispatcher
            .enqueue(&ctx, "b", Bytes::new(), EnqueueOpts::default())
            .await
            .expect_err("queue disabled"),
        CallError::Forbidden
    );

    // Depth at the plan maximum: `429 queue_full` (TRG-13 through std:queue).
    let plan = Plan {
        max_queue_depth: 2,
        ..Plan::default()
    };
    let r = rig_with(default_entries(), FixedPlans::new(plan));
    for i in 0..2 {
        r.store
            .enqueue(ids::service(B), NewMessage::new(format!("m{i}")))
            .await
            .expect("seed");
    }
    assert_eq!(
        r.dispatcher
            .enqueue(&ctx, "b", Bytes::from_static(b"one too many"), EnqueueOpts::default())
            .await
            .expect_err("full"),
        CallError::QueueFull
    );
    assert_eq!(CallError::QueueFull.code(), "queue_full");
}

// ---------------------------------------------------------------------------------------------
// TRG-19..21: environments and `call.allowed_callers` (decision D-A1)
// ---------------------------------------------------------------------------------------------

const STAGING_WEB: u128 = 20; // tenant 1, slug "web", environment staging
const PROD_API: u128 = 21; // tenant 1, slug "api", environment production
const LOCKED: u128 = 22; // tenant 1, slug "locked", production, allowed_callers = ["web"]

fn env_entries(api_callers: &[&str]) -> Vec<RouteEntry> {
    vec![
        common::entry_in(T1, STAGING_WEB, "web", "staging", &[]),
        common::entry_in(T1, PROD_API, "api", "production", api_callers),
        common::entry_in(T1, LOCKED, "locked", "production", &["web"]),
        entry(T1, A, "a", 1), // production, empty list
        entry(T2, X, "x", 1),
    ]
}

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_19_same_environment_callers_are_admitted_by_an_empty_list() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig_with(env_entries(&[]), FixedPlans::new(Plan::default()));
            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx, "api", "/", Duration::from_secs(2)).await;
            assert_eq!(res.expect("production → production").status(), 200);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_20_cross_environment_calls_need_an_env_entry() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig_with(env_entries(&[]), FixedPlans::new(Plan::default()));
            let ctx = ctx_for(T1, STAGING_WEB, 1, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx.clone(), "api", "/", Duration::from_secs(1)).await;
            let err = res.expect_err("staging → production with an empty list");
            assert_eq!(err, CallError::NotAllowed);
            assert_eq!(err.code(), "call_not_allowed");
            assert_eq!(r.sim.scripts().started(), 0, "nothing runs");
            assert!(r.factory.seen.lock().is_empty());

            let mut r = rig_with(env_entries(&["env:staging"]), FixedPlans::new(Plan::default()));
            let (res, _) = run_call(&mut r, ctx, "api", "/", Duration::from_secs(2)).await;
            assert_eq!(res.expect("env:staging listed").status(), 200);

            // The env entry replaces the implicit same-environment rule.
            let ctx = ctx_for(T1, A, 2, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx, "api", "/", Duration::from_secs(1)).await;
            assert_eq!(res.expect_err("production caller, not listed"), CallError::NotAllowed);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn ac_trg_21_listed_callers_by_slug_or_id_and_nobody_else() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let a_id = ids::service(A).to_string();
            let mut r = rig_with(env_entries(&[a_id.as_str()]), FixedPlans::new(Plan::default()));
            let deadline = r.sim.clock().now() + Duration::from_secs(30);

            let (res, _) = run_call(
                &mut r,
                ctx_for(T1, STAGING_WEB, 1, 0, deadline),
                "locked",
                "/",
                Duration::from_secs(2),
            )
            .await;
            assert_eq!(res.expect("`web` is listed by slug (any environment)").status(), 200);
            let (res, _) = run_call(
                &mut r,
                ctx_for(T1, A, 2, 0, deadline),
                "locked",
                "/",
                Duration::from_secs(1),
            )
            .await;
            assert_eq!(res.expect_err("same environment but not listed"), CallError::NotAllowed);
            let (res, _) = run_call(
                &mut r,
                ctx_for(T1, A, 3, 0, deadline),
                "api",
                "/",
                Duration::from_secs(2),
            )
            .await;
            assert_eq!(res.expect("listed by service id").status(), 200);

            // `std:queue` enqueues follow the same caller rule.
            let opts = EnqueueOpts::default;
            let err = r
                .dispatcher
                .enqueue(&ctx_for(T1, A, 4, 0, deadline), "locked", Bytes::new(), opts())
                .await;
            assert_eq!(err.expect_err("enqueue not allowed"), CallError::NotAllowed);
            r.dispatcher
                .enqueue(
                    &ctx_for(T1, STAGING_WEB, 5, 0, deadline),
                    "locked",
                    Bytes::new(),
                    opts(),
                )
                .await
                .expect("listed");
            assert_eq!(r.store.enqueued().len(), 1);

            // Cross-tenant stays `cross_tenant_call` (TRG-16), whatever the lists say.
            let (res, _) = run_call(
                &mut r,
                ctx_for(T2, X, 6, 0, deadline),
                "api",
                "/",
                Duration::from_secs(1),
            )
            .await;
            assert_eq!(res.expect_err("t2 → t1"), CallError::CrossTenant);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn ac_sec_38_a_plan_without_call_refuses_calls_to_its_services() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let no_call = Plan {
                allowed_triggers: vec![rt_types::TriggerKind::Http, rt_types::TriggerKind::Queue],
                ..Plan::default()
            };
            let mut r = rig_with(default_entries(), FixedPlans::new(no_call));
            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(30));
            let (res, _) = run_call(&mut r, ctx.clone(), "b", "/", Duration::from_secs(1)).await;
            let err = res.expect_err("plan forbids call");
            assert_eq!(
                (err.clone(), err.code()),
                (CallError::TriggerNotAllowed, "trigger_not_allowed")
            );
            assert_eq!(r.sim.scripts().started(), 0);
            // Enqueueing is not a call: the queue trigger is allowed.
            r.dispatcher
                .enqueue(&ctx, "b", Bytes::new(), EnqueueOpts::default())
                .await
                .expect("enqueue still works");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn ac_sec_38_a_plan_without_queue_refuses_enqueues_to_its_services() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let no_queue = Plan {
                allowed_triggers: vec![rt_types::TriggerKind::Http, rt_types::TriggerKind::Call],
                ..Plan::default()
            };
            let mut r = rig_with(default_entries(), FixedPlans::new(no_queue));
            let ctx = ctx_for(T1, A, 1, 0, r.sim.clock().now() + Duration::from_secs(30));
            let err = r
                .dispatcher
                .enqueue(&ctx, "b", Bytes::new(), EnqueueOpts::default())
                .await
                .expect_err("plan forbids queue");
            assert_eq!(
                (err.clone(), err.code()),
                (CallError::TriggerNotAllowed, "trigger_not_allowed")
            );
            assert!(r.store.enqueued().is_empty(), "nothing was stored");
            // Calls are not enqueues: the call trigger is allowed.
            let (res, _) = run_call(&mut r, ctx, "b", "/", Duration::from_secs(1)).await;
            assert!(res.is_ok(), "{res:?}");
        })
        .await;
}
