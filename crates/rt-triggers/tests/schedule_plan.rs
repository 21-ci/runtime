//! A plan that stops allowing `interval` / `cron` stops the tenant's existing schedules at fire
//! time (SECURITY.md trigger table): the fire is skipped, counted in `rt_schedule_denied_total`, and
//! resumes when the plan allows the trigger again. The plan comes from the in-memory
//! `PlanSource`, never from `meta.db`.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{drive, dyn_clock, new_sim, RecordingScheduleFactory};
use parking_lot::Mutex;
use rt_obs::{Metrics, MetricsOptions};
use rt_sec::PlanSource;
use rt_testkit::{ids, Script};
use rt_triggers::ScheduleEngine;
use rt_types::{Plan, Schedule, ScheduleId, ScheduleKind, TenantId, TriggerKind};
use serde_json::json;

/// A plan source whose plan can change at run time (a `SIGHUP` plan reload), counting lookups.
struct SwitchPlans {
    plan: Mutex<Arc<Plan>>,
    lookups: AtomicUsize,
}

impl SwitchPlans {
    fn new(plan: Plan) -> Arc<Self> {
        Arc::new(Self {
            plan: Mutex::new(Arc::new(plan)),
            lookups: AtomicUsize::new(0),
        })
    }

    fn set(&self, plan: Plan) {
        *self.plan.lock() = Arc::new(plan);
    }
}

impl PlanSource for SwitchPlans {
    fn plan_for(&self, tenant: TenantId) -> Plan {
        (*self.plan_arc(tenant)).clone()
    }

    fn plan_arc(&self, _tenant: TenantId) -> Arc<Plan> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        self.plan.lock().clone()
    }
}

fn plan_with(triggers: &[TriggerKind]) -> Plan {
    Plan {
        allowed_triggers: triggers.to_vec(),
        ..Plan::default()
    }
}

fn interval_schedule(service_n: u128, every_seconds: u64) -> Schedule {
    Schedule {
        id: ScheduleId::new(),
        service_id: ids::service(service_n),
        kind: ScheduleKind::Interval { every_seconds },
        payload: json!({}),
        enabled: true,
        next_due_ms: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_schedule_stops_firing_while_the_plan_no_longer_allows_its_trigger() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut sim = new_sim();
            let factory = RecordingScheduleFactory::new(sim.clock().clone(), 1);
            let plans = SwitchPlans::new(plan_with(&[TriggerKind::Http, TriggerKind::Interval]));
            let metrics = Metrics::new(MetricsOptions::default()).unwrap();
            let engine = Arc::new(
                ScheduleEngine::new(dyn_clock(&sim), sim.scheduler().clone(), factory.clone())
                    .with_metrics(metrics.clone())
                    .with_plans(plans.clone()),
            );
            sim.scripts().set_for_service(ids::service(1), Script::new().io_ms(10));
            engine.upsert(interval_schedule(1, 1)).expect("upsert");
            let e = engine.clone();
            let run = tokio::task::spawn_local(async move { e.run().await });

            drive(&mut sim, Duration::from_millis(3_500), Duration::from_millis(50)).await;
            let allowed = engine.stats();
            assert!(allowed.fires >= 2, "fires while allowed: {allowed:?}");
            assert_eq!(allowed.denied, 0);
            assert!(
                plans.lookups.load(Ordering::Relaxed) >= 2,
                "the plan is checked on every fire"
            );

            // the operator removes `interval` from the plan (SIGHUP): no new fire, each one counted
            plans.set(plan_with(&[TriggerKind::Http]));
            drive(&mut sim, Duration::from_millis(3_000), Duration::from_millis(50)).await;
            let denied = engine.stats();
            assert_eq!(
                denied.fires, allowed.fires,
                "no task is submitted while the plan denies `interval`"
            );
            assert!(denied.denied >= 2, "{denied:?}");
            assert_eq!(metrics.schedule_denied_total.get(), denied.denied);
            assert_eq!(metrics.schedule_fires_total.get(), denied.fires);

            // allowed again: the schedule resumes on its grid
            plans.set(plan_with(&[TriggerKind::Http, TriggerKind::Interval]));
            drive(&mut sim, Duration::from_millis(3_000), Duration::from_millis(50)).await;
            let resumed = engine.stats();
            assert!(resumed.fires >= denied.fires + 2, "{resumed:?}");
            assert_eq!(resumed.denied, denied.denied);
            run.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn without_a_plan_source_schedules_fire_as_before() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut sim = new_sim();
            let factory = RecordingScheduleFactory::new(sim.clock().clone(), 1);
            let engine = Arc::new(ScheduleEngine::new(
                dyn_clock(&sim),
                sim.scheduler().clone(),
                factory.clone(),
            ));
            sim.scripts().set_for_service(ids::service(1), Script::new().io_ms(10));
            engine.upsert(interval_schedule(1, 1)).expect("upsert");
            let e = engine.clone();
            let run = tokio::task::spawn_local(async move { e.run().await });
            drive(&mut sim, Duration::from_millis(2_500), Duration::from_millis(50)).await;
            assert!(engine.stats().fires >= 2);
            assert_eq!(engine.stats().denied, 0);
            run.abort();
        })
        .await;
}
