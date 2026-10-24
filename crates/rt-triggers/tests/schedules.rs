//! Schedule engine: TRG-05..08.
//!
//! Pure functions (`next_fire`, `normalize_cron`, `validate_kind`, `jitter`) are tested directly;
//! the engine runs on the testkit's `VirtualClock` + `SimRuntime` (one thread, virtual time):
//! the engine's `run()` loop is spawned on a `LocalSet` and `common::drive` alternates between
//! advancing virtual time (which also runs the simulated scheduler) and letting the loop react.

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use common::{drive, dyn_clock, max_parallel, new_sim, FixedPlans, RecordingScheduleFactory};
use proptest::prelude::*;
use rt_sched::Clock;
use rt_sec::{LocalQuotaEnforcer, QuotaEnforcer, QuotaError, QuotaOp};
use rt_testkit::{ids, Script, SimRuntime};
use rt_triggers::{
    jitter, next_fire, normalize_cron, validate_kind, ScheduleEngine, ScheduleError, MAX_INTERVAL_SECONDS,
};
use rt_types::{Plan, Schedule, ScheduleId, ScheduleKind, TriggerKind};
use serde_json::json;

fn utc_ms(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
        .single()
        .expect("valid time")
        .timestamp_millis()
}

fn interval_schedule(service_n: u128, every_seconds: u64) -> Schedule {
    Schedule {
        id: ScheduleId::new(),
        service_id: ids::service(service_n),
        kind: ScheduleKind::Interval { every_seconds },
        payload: json!({"tick": true}),
        enabled: true,
        next_due_ms: None,
    }
}

fn cron_schedule(service_n: u128, expr: &str, tz: &str) -> Schedule {
    Schedule {
        id: ScheduleId::new(),
        service_id: ids::service(service_n),
        kind: ScheduleKind::Cron {
            expr: expr.to_string(),
            tz: tz.to_string(),
        },
        payload: json!({}),
        enabled: true,
        next_due_ms: None,
    }
}

struct Rig {
    sim: SimRuntime,
    factory: Arc<RecordingScheduleFactory>,
    engine: Arc<ScheduleEngine>,
}

fn rig(service_n: u128) -> Rig {
    let sim = new_sim();
    let factory = RecordingScheduleFactory::new(sim.clock().clone(), service_n);
    let engine = Arc::new(ScheduleEngine::new(
        dyn_clock(&sim),
        sim.scheduler().clone(),
        factory.clone(),
    ));
    Rig { sim, factory, engine }
}

fn spawn_engine(engine: &Arc<ScheduleEngine>) -> tokio::task::JoinHandle<()> {
    let e = engine.clone();
    tokio::task::spawn_local(async move { e.run().await })
}

// ---------------------------------------------------------------------------------------------
// Pure functions
// ---------------------------------------------------------------------------------------------

#[test]
fn normalize_cron_prefixes_a_seconds_field_to_five_field_expressions() {
    assert_eq!(normalize_cron("*/5 * * * *"), "0 */5 * * * *");
    assert_eq!(normalize_cron("  0 12 * * *  "), "0 0 12 * * *");
    // 6-field (seconds) expressions are left alone.
    assert_eq!(normalize_cron("*/15 * * * * *"), "*/15 * * * * *");
}

#[test]
fn validate_kind_accepts_good_and_names_the_problem_of_bad_schedules() {
    assert_eq!(validate_kind(&ScheduleKind::Interval { every_seconds: 1 }), Ok(()));
    assert_eq!(
        validate_kind(&ScheduleKind::Interval { every_seconds: 0 }),
        Err(ScheduleError::BadInterval)
    );

    let cron = |expr: &str, tz: &str| ScheduleKind::Cron {
        expr: expr.into(),
        tz: tz.into(),
    };
    assert_eq!(validate_kind(&cron("*/5 * * * *", "UTC")), Ok(()));
    assert_eq!(validate_kind(&cron("0 9 * * *", "Asia/Tashkent")), Ok(()));
    assert_eq!(
        validate_kind(&cron("*/15 * * * * *", "UTC")),
        Ok(()),
        "optional seconds field"
    );
    assert_eq!(
        validate_kind(&cron("*/5 * * * *", "Mars/Olympus")),
        Err(ScheduleError::BadTimezone("Mars/Olympus".into()))
    );
    assert!(matches!(
        validate_kind(&cron("not a cron", "UTC")),
        Err(ScheduleError::BadCron(_))
    ));
    assert!(matches!(
        validate_kind(&cron("99 * * * *", "UTC")),
        Err(ScheduleError::BadCron(_))
    ));
}

/// CRON-002: an interval past [`MAX_INTERVAL_SECONDS`] is refused up front, so `every_seconds *
/// 1000` can never overflow into a negative period (that fired about every second on any plan,
/// or panicked the shared schedule engine in debug builds).
#[test]
fn cron_002_huge_intervals_are_refused_and_never_overflow() {
    for every_seconds in [
        MAX_INTERVAL_SECONDS + 1,
        9_300_000_000_000_000,
        i64::MAX as u64,
        u64::MAX,
    ] {
        let k = ScheduleKind::Interval { every_seconds };
        assert_eq!(validate_kind(&k), Err(ScheduleError::BadInterval), "{every_seconds}");
        assert_eq!(next_fire(&k, 1_000), Err(ScheduleError::BadInterval), "{every_seconds}");
    }
    let max = ScheduleKind::Interval {
        every_seconds: MAX_INTERVAL_SECONDS,
    };
    assert_eq!(validate_kind(&max), Ok(()));
    let at = 1_700_000_000_000;
    assert_eq!(next_fire(&max, at), Ok(Some(at + MAX_INTERVAL_SECONDS as i64 * 1000)));
    assert_eq!(
        next_fire(&max, i64::MAX - 5),
        Ok(Some(i64::MAX)),
        "saturates at the end of time"
    );
}

#[test]
fn next_fire_interval_adds_the_period() {
    let k = ScheduleKind::Interval { every_seconds: 2 };
    assert_eq!(next_fire(&k, 1_000), Ok(Some(3_000)));
    assert_eq!(
        next_fire(&ScheduleKind::Interval { every_seconds: 0 }, 0),
        Err(ScheduleError::BadInterval)
    );
}

#[test]
fn next_fire_cron_every_minute_in_tashkent_hits_minute_boundaries() {
    let k = ScheduleKind::Cron {
        expr: "*/1 * * * *".into(),
        tz: "Asia/Tashkent".into(),
    };
    let start = utc_ms(2023, 11, 14, 22, 13, 20);
    let f1 = next_fire(&k, start).expect("ok").expect("fire");
    let f2 = next_fire(&k, f1).expect("ok").expect("fire");
    let f3 = next_fire(&k, f2).expect("ok").expect("fire");
    assert_eq!(f1, utc_ms(2023, 11, 14, 22, 14, 0));
    assert_eq!(f2, utc_ms(2023, 11, 14, 22, 15, 0));
    assert_eq!(f3, utc_ms(2023, 11, 14, 22, 16, 0));
    // "strictly after": asking at a boundary yields the next one.
    assert_eq!(next_fire(&k, f1).expect("ok"), Some(f2));
}

#[test]
fn next_fire_cron_uses_the_schedules_timezone() {
    let daily9 = |tz: &str| ScheduleKind::Cron {
        expr: "0 9 * * *".into(),
        tz: tz.into(),
    };
    let after = utc_ms(2024, 3, 10, 0, 0, 0);

    // Asia/Tashkent is UTC+5 all year: 09:00 local = 04:00Z.
    assert_eq!(
        next_fire(&daily9("Asia/Tashkent"), after),
        Ok(Some(utc_ms(2024, 3, 10, 4, 0, 0)))
    );
    // UTC is the default zone.
    assert_eq!(next_fire(&daily9("UTC"), after), Ok(Some(utc_ms(2024, 3, 10, 9, 0, 0))));
    // DST: New York switches to EDT (UTC-4) on 2024-03-10 at 02:00 local, so 09:00 local is 13:00Z
    // that day, while the day before it was 14:00Z (EST, UTC-5).
    assert_eq!(
        next_fire(&daily9("America/New_York"), utc_ms(2024, 3, 9, 15, 0, 0)),
        Ok(Some(utc_ms(2024, 3, 10, 13, 0, 0)))
    );
    assert_eq!(
        next_fire(&daily9("America/New_York"), utc_ms(2024, 3, 9, 0, 0, 0)),
        Ok(Some(utc_ms(2024, 3, 9, 14, 0, 0)))
    );
}

#[test]
fn next_fire_cron_with_seconds_field() {
    let k = ScheduleKind::Cron {
        expr: "*/15 * * * * *".into(),
        tz: "UTC".into(),
    };
    let after = utc_ms(2024, 1, 1, 12, 0, 20);
    assert_eq!(next_fire(&k, after), Ok(Some(utc_ms(2024, 1, 1, 12, 0, 30))));
}

#[test]
fn next_fire_rejects_bad_timezone_and_cron() {
    let bad_tz = ScheduleKind::Cron {
        expr: "* * * * *".into(),
        tz: "Nowhere/City".into(),
    };
    assert_eq!(
        next_fire(&bad_tz, 0),
        Err(ScheduleError::BadTimezone("Nowhere/City".into()))
    );
    let bad_cron = ScheduleKind::Cron {
        expr: "banana".into(),
        tz: "UTC".into(),
    };
    assert!(matches!(next_fire(&bad_cron, 0), Err(ScheduleError::BadCron(_))));
}

#[test]
fn jitter_is_bounded_by_min_of_one_second_and_a_tenth_of_the_interval() {
    let id = ScheduleId::from_u128(42);
    assert!(jitter(id, Duration::from_secs(2)) < Duration::from_millis(200));
    assert!(jitter(id, Duration::from_secs(5)) < Duration::from_millis(500));
    assert!(jitter(id, Duration::from_secs(100)) < Duration::from_secs(1));
    assert!(jitter(id, Duration::from_secs(86_400)) < Duration::from_secs(1));
    assert_eq!(jitter(id, Duration::ZERO), Duration::ZERO);
    // Deterministic per schedule id (seeded by it).
    assert_eq!(jitter(id, Duration::from_secs(60)), jitter(id, Duration::from_secs(60)));
}

#[test]
fn jitter_differs_between_schedules_to_avoid_thundering_herds() {
    let distinct: std::collections::BTreeSet<u128> = (1..=50u128)
        .map(|n| jitter(ScheduleId::from_u128(n), Duration::from_secs(100)).as_nanos())
        .collect();
    assert!(
        distinct.len() > 10,
        "jitter should spread schedules out, got {} distinct values",
        distinct.len()
    );
}

proptest! {
    #[test]
    fn jitter_stays_in_range(n in any::<u128>(), secs in 1u64..=1_000_000) {
        let interval = Duration::from_secs(secs);
        let max = Duration::from_secs(1).min(interval / 10);
        let j = jitter(ScheduleId::from_u128(n), interval);
        if max.is_zero() {
            prop_assert_eq!(j, Duration::ZERO);
        } else {
            prop_assert!(j < max, "jitter {:?} must be < {:?}", j, max);
        }
    }

    #[test]
    fn next_fire_interval_is_strictly_after_and_exact(after in 0i64..4_000_000_000_000, secs in 1u64..100_000) {
        let k = ScheduleKind::Interval { every_seconds: secs };
        let n = next_fire(&k, after).unwrap().unwrap();
        prop_assert_eq!(n, after + (secs as i64) * 1000);
    }

    #[test]
    fn next_fire_cron_every_n_minutes_is_aligned_and_in_the_future(after in 1_600_000_000_000i64..2_000_000_000_000, n in 1u32..30) {
        let k = ScheduleKind::Cron { expr: format!("*/{n} * * * *"), tz: "Asia/Tashkent".into() };
        let f = next_fire(&k, after).unwrap().unwrap();
        prop_assert!(f > after);
        prop_assert!(f - after <= (n as i64) * 60_000);
        prop_assert_eq!(f % 60_000, 0, "fires on a whole minute");
    }
}

// ---------------------------------------------------------------------------------------------
// TRG-07: minimum interval (plan setting) — unit level; the HTTP 422 is in rt-server tests.
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_trg_07_interval_below_plan_minimum_is_schedule_too_frequent() {
    let plan = Plan {
        min_schedule_seconds: 10,
        max_schedules: 3,
        ..Plan::default()
    };
    let q = LocalQuotaEnforcer::new(FixedPlans::new(plan));
    let tenant = ids::tenant(1);

    let too_fast = q.check(
        tenant,
        QuotaOp::CreateSchedule {
            current: 0,
            every_seconds: 5,
        },
    );
    assert_eq!(too_fast, Err(QuotaError::ScheduleTooFrequent { min_seconds: 10 }));
    let e = too_fast.unwrap_err();
    assert_eq!(e.http_status(), 422);
    assert_eq!(e.code(), "schedule_too_frequent");

    assert!(q
        .check(
            tenant,
            QuotaOp::CreateSchedule {
                current: 0,
                every_seconds: 10
            }
        )
        .is_ok());
    assert!(q
        .check(
            tenant,
            QuotaOp::CreateSchedule {
                current: 2,
                every_seconds: 3600
            }
        )
        .is_ok());

    // Per-service schedule cap: `403 quota_exceeded`.
    assert!(matches!(
        q.check(
            tenant,
            QuotaOp::CreateSchedule {
                current: 3,
                every_seconds: 60
            }
        ),
        Err(QuotaError::Exceeded { .. })
    ));
}

#[test]
fn trigger_switches_of_the_plan_gate_cron() {
    // The default (free) plan has no `cron` in `allowed_triggers`.
    let q = LocalQuotaEnforcer::new(FixedPlans::new(Plan::default()));
    let tenant = ids::tenant(1);
    assert_eq!(
        q.check(tenant, QuotaOp::UseTrigger(TriggerKind::Cron)),
        Err(QuotaError::TriggerNotAllowed)
    );
    assert!(q.check(tenant, QuotaOp::UseTrigger(TriggerKind::Interval)).is_ok());
}

// ---------------------------------------------------------------------------------------------
// Engine on virtual time
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn upsert_rejects_invalid_schedules() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let r = rig(1);
            assert_eq!(
                r.engine.upsert(interval_schedule(1, 0)),
                Err(ScheduleError::BadInterval)
            );
            assert!(matches!(
                r.engine.upsert(cron_schedule(1, "nope", "UTC")),
                Err(ScheduleError::BadCron(_))
            ));
            assert!(matches!(
                r.engine.upsert(cron_schedule(1, "* * * * *", "Nowhere/Land")),
                Err(ScheduleError::BadTimezone(_))
            ));
        })
        .await;
}

/// TRG-05: interval 2 s, handler takes 3 s, 10 s pass → ≤ 4 fires, never overlapping, skipped
/// ticks counted in `rt_schedule_skipped_total`.
#[tokio::test(flavor = "current_thread")]
async fn ac_trg_05_slow_handler_never_overlaps_and_skipped_ticks_are_counted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(1);
            r.sim
                .scripts()
                .set_for_service(ids::service(1), Script::new().io_ms(3_000));
            let metrics = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).expect("metrics");
            let engine = Arc::new(
                ScheduleEngine::new(dyn_clock(&r.sim), r.sim.scheduler().clone(), r.factory.clone())
                    .with_metrics(metrics.clone()),
            );
            engine.upsert(interval_schedule(1, 2)).expect("upsert");
            let run = spawn_engine(&engine);

            drive(&mut r.sim, Duration::from_secs(10), Duration::from_millis(50)).await;
            run.abort();

            let fires = r.factory.fires.lock().clone();
            assert!(
                (2..=4).contains(&fires.len()),
                "expected 2..=4 fires in 10 s, got {}",
                fires.len()
            );
            assert_eq!(
                max_parallel(r.sim.scripts()),
                1,
                "runs of one schedule must never overlap"
            );
            for w in fires.windows(2) {
                assert!(
                    w[1].at_ms - w[0].at_ms >= 3_000,
                    "next fire ({}) must come after the previous run finished (started {} + 3 s)",
                    w[1].at_ms,
                    w[0].at_ms
                );
            }

            let stats = engine.stats();
            assert_eq!(stats.fires as usize, fires.len());
            assert!(
                stats.skipped >= 1,
                "ticks that fell into a running task are skipped, got {}",
                stats.skipped
            );
            assert_eq!(metrics.schedule_fires_total.get(), stats.fires);
            assert_eq!(metrics.schedule_skipped_total.get(), stats.skipped);
        })
        .await;
}

/// A handler faster than the interval fires on every tick and skips nothing; fires are not
/// earlier than their scheduled time and are jittered by at most `min(1 s, interval/10)`.
#[tokio::test(flavor = "current_thread")]
async fn ac_trg_05_fast_handler_fires_every_tick() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(1);
            r.sim
                .scripts()
                .set_for_service(ids::service(1), Script::new().io_ms(100));
            r.engine.upsert(interval_schedule(1, 2)).expect("upsert");
            let run = spawn_engine(&r.engine);

            drive(&mut r.sim, Duration::from_secs(10), Duration::from_millis(50)).await;
            run.abort();

            let fires = r.factory.fires.lock().clone();
            assert!(
                (4..=6).contains(&fires.len()),
                "10 s / 2 s ≈ 5 fires, got {}",
                fires.len()
            );
            assert_eq!(r.engine.stats().skipped, 0);
            for f in &fires {
                let lag = f.at_ms - f.scheduled_ms;
                assert!(
                    (0..=300).contains(&lag),
                    "fire at {} for scheduled {} (jitter < 200 ms)",
                    f.at_ms,
                    f.scheduled_ms
                );
            }
            for w in fires.windows(2) {
                let gap = w[1].scheduled_ms - w[0].scheduled_ms;
                assert_eq!(gap, 2_000, "scheduled times stay on the 2 s grid");
            }
        })
        .await;
}

/// TRG-06: cron `*/1 * * * *` in Asia/Tashkent, virtual clock passes 3 minute boundaries.
#[tokio::test(flavor = "current_thread")]
async fn ac_trg_06_cron_every_minute_in_tashkent_fires_on_minute_boundaries() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(1);
            let start_ms = r.sim.clock().unix_ms();
            r.engine
                .upsert(cron_schedule(1, "*/1 * * * *", "Asia/Tashkent"))
                .expect("upsert");
            let run = spawn_engine(&r.engine);

            // The clock starts at 22:13:20Z: boundaries at +40 s, +100 s, +160 s; the 4th is at +220 s.
            drive(&mut r.sim, Duration::from_secs(170), Duration::from_millis(500)).await;
            run.abort();

            let times = r.factory.scheduled_times();
            assert_eq!(times.len(), 3, "3 boundaries passed: {times:?}");
            for t in &times {
                assert_eq!(t % 60_000, 0, "scheduled time {t} is a minute boundary");
                let local = chrono::TimeZone::timestamp_millis_opt(&chrono_tz::Asia::Tashkent, *t)
                    .single()
                    .expect("time");
                assert_eq!(chrono::Timelike::second(&local), 0);
            }
            assert_eq!(times[0], start_ms + 40_000);
            assert_eq!(times[1] - times[0], 60_000);
            assert_eq!(times[2] - times[1], 60_000);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn removed_and_disabled_schedules_do_not_fire() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(1);
            let live = interval_schedule(1, 2);
            let mut disabled = interval_schedule(1, 2);
            disabled.enabled = false;
            r.engine.upsert(live.clone()).expect("upsert");
            r.engine.upsert(disabled.clone()).expect("upsert");
            let run = spawn_engine(&r.engine);

            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            let before = r.factory.count();
            assert!(before >= 1, "the enabled schedule fires");
            assert!(
                r.factory.fires.lock().iter().all(|f| f.id == live.id),
                "the disabled schedule never fires"
            );

            r.engine.remove(live.id);
            drive(&mut r.sim, Duration::from_secs(10), Duration::from_millis(50)).await;
            run.abort();
            assert!(
                r.factory.count() <= before + 1,
                "no further fires after removal (one may already be due)"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn upsert_replaces_an_existing_schedule_and_recomputes_the_next_fire() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let r = rig(1);
            let mut s = interval_schedule(1, 100);
            r.engine.upsert(s.clone()).expect("upsert");
            let now = r.sim.clock().unix_ms();
            let due1 = r.engine.next_due(s.id).expect("next due");
            assert!(
                due1 >= now + 100_000 && due1 <= now + 100_000 + 1_000 + 100,
                "100 s interval (+ jitter < 1 s): {due1} vs {now}"
            );

            s.kind = ScheduleKind::Interval { every_seconds: 20 };
            r.engine.upsert(s.clone()).expect("replace");
            let due2 = r.engine.next_due(s.id).expect("next due");
            assert!(
                due2 >= now + 20_000 && due2 <= now + 20_000 + 2_000 + 100,
                "replaced by a 20 s interval: {due2}"
            );

            r.engine.remove(s.id);
            assert_eq!(r.engine.next_due(s.id), None);
        })
        .await;
}

/// TRG-08 (engine part): schedules persisted in `meta.db` come back with a stale `next_due_ms`
/// after a restart. They resume on their grid; missed ticks are **not** backfilled.
#[tokio::test(flavor = "current_thread")]
async fn ac_trg_08_restart_resumes_schedules_without_backfilling_missed_ticks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(1);
            let sch = interval_schedule(1, 2);
            r.engine.upsert(sch.clone()).expect("upsert");
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(5), Duration::from_millis(50)).await;
            let last_due = r.engine.next_due(sch.id).expect("next due before stop");
            let fired_before = r.factory.count();
            assert!(fired_before >= 1);

            // Node stopped for one minute.
            run.abort();
            drop(r.engine);
            r.sim.advance(Duration::from_secs(60));
            let now = r.sim.clock().unix_ms();
            assert!(
                last_due < now - 50_000,
                "the persisted next_due is a minute in the past"
            );

            // Restart: a new engine rehydrates the persisted schedule.
            let factory2 = RecordingScheduleFactory::new(r.sim.clock().clone(), 1);
            let engine2 = Arc::new(ScheduleEngine::new(
                dyn_clock(&r.sim),
                r.sim.scheduler().clone(),
                factory2.clone(),
            ));
            let mut persisted = sch.clone();
            persisted.next_due_ms = Some(last_due);
            engine2.upsert(persisted).expect("rehydrate");

            let due = engine2.next_due(sch.id).expect("next due after restart");
            assert!(
                due > now,
                "next due {due} must be in the future (now {now}): ticks are not backfilled"
            );
            assert!(
                due - now <= 2_000 + 200,
                "resumes on the next tick, got +{} ms",
                due - now
            );

            let run2 = spawn_engine(&engine2);
            drive(&mut r.sim, Duration::from_secs(1), Duration::from_millis(50)).await;
            assert!(
                factory2.count() <= 1,
                "no burst of catch-up fires, got {}",
                factory2.count()
            );
            drive(&mut r.sim, Duration::from_secs(10), Duration::from_millis(50)).await;
            run2.abort();
            assert!(
                factory2.count() <= 7,
                "11 s after the restart is ~5 fires, not the 30 missed ones: {}",
                factory2.count()
            );
            assert!(factory2.count() >= 4, "schedules resume: {}", factory2.count());
        })
        .await;
}

fn fires(expr: &str, tz: &str, from: i64, n: usize) -> Vec<i64> {
    let kind = ScheduleKind::Cron {
        expr: expr.to_string(),
        tz: tz.to_string(),
    };
    let mut out = Vec::new();
    let mut at = from;
    for _ in 0..n {
        match next_fire(&kind, at).expect("valid") {
            Some(t) => {
                out.push(t);
                at = t;
            }
            None => break,
        }
    }
    out
}

/// CRON-003: a daily 02:30 in Europe/Berlin fires exactly once on each DST transition day: on
/// the spring-forward day (02:30 does not exist) it is not skipped, on the fall-back day (02:30
/// happens twice) it does not fire twice.
#[test]
fn cron_003_dst_days_fire_once() {
    let day = |ms: i64| {
        Utc.timestamp_millis_opt(ms)
            .single()
            .expect("ms")
            .with_timezone(&chrono_tz::Europe::Berlin)
            .date_naive()
    };
    // 2027-03-28: clocks go 02:00 -> 03:00. 2027-10-31: 03:00 -> 02:00.
    for (from, label) in [
        (utc_ms(2027, 3, 26, 12, 0, 0), "spring"),
        (utc_ms(2027, 10, 29, 12, 0, 0), "fall"),
    ] {
        let f = fires("30 2 * * *", "Europe/Berlin", from, 4);
        let days: Vec<_> = f.iter().map(|ms| day(*ms)).collect();
        for w in days.windows(2) {
            assert_eq!(
                (w[1] - w[0]).num_days(),
                1,
                "{label}: one fire per day, none skipped: {days:?}"
            );
        }
    }
}

/// CRON-004: Feb 29 fires only in leap years; day 31 skips shorter months; Feb 30 never fires.
#[test]
fn cron_004_calendar_edges() {
    let f = fires("0 0 29 2 *", "UTC", utc_ms(2026, 1, 1, 0, 0, 0), 2);
    assert_eq!(f, vec![utc_ms(2028, 2, 29, 0, 0, 0), utc_ms(2032, 2, 29, 0, 0, 0)]);
    let f = fires("0 0 31 * *", "UTC", utc_ms(2026, 3, 1, 0, 0, 0), 3);
    assert_eq!(
        f,
        vec![
            utc_ms(2026, 3, 31, 0, 0, 0),
            utc_ms(2026, 5, 31, 0, 0, 0),
            utc_ms(2026, 7, 31, 0, 0, 0)
        ]
    );
    assert!(
        fires("0 0 30 2 *", "UTC", utc_ms(2026, 1, 1, 0, 0, 0), 1).is_empty(),
        "never"
    );
}

/// CRON-007: after a day-long stall, an every-second cron neither replays the missed ticks nor
/// stalls: the next due is just ahead of now and only a handful fire right after.
#[tokio::test(flavor = "current_thread")]
async fn cron_007_a_long_stall_does_not_replay_missed_ticks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(1);
            let sch = cron_schedule(1, "* * * * * *", "UTC");
            r.engine.upsert(sch.clone()).expect("upsert");
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(3), Duration::from_millis(100)).await;
            run.abort();
            let last_due = r.engine.next_due(sch.id).expect("due");
            drop(r.engine);
            r.sim.advance(Duration::from_secs(86_400));
            let now = r.sim.clock().unix_ms();

            let factory2 = RecordingScheduleFactory::new(r.sim.clock().clone(), 1);
            let engine2 = Arc::new(ScheduleEngine::new(
                dyn_clock(&r.sim),
                r.sim.scheduler().clone(),
                factory2.clone(),
            ));
            let mut persisted = sch.clone();
            persisted.next_due_ms = Some(last_due);
            engine2.upsert(persisted).expect("rehydrate");
            let due = engine2.next_due(sch.id).expect("due");
            assert!(due > now && due - now <= 1_100, "next due {} ms ahead", due - now);
            let run2 = spawn_engine(&engine2);
            drive(&mut r.sim, Duration::from_secs(3), Duration::from_millis(100)).await;
            run2.abort();
            assert!(factory2.count() <= 4, "no catch-up burst: {}", factory2.count());
        })
        .await;
}

/// CRON-009: the wall clock stepping back by an hour neither stalls a minute cron for that hour
/// nor fires it twice per minute.
#[tokio::test(flavor = "current_thread")]
async fn cron_009_a_wall_clock_step_back_does_not_stall_or_double_fire() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut r = rig(1);
            r.engine.upsert(cron_schedule(1, "* * * * *", "UTC")).expect("upsert");
            let run = spawn_engine(&r.engine);
            drive(&mut r.sim, Duration::from_secs(150), Duration::from_millis(500)).await;
            let before = r.factory.count();
            assert!(before >= 2, "fires before the step: {before}");
            r.sim.clock().step_unix_ms(-3_600_000);
            drive(&mut r.sim, Duration::from_secs(300), Duration::from_millis(500)).await;
            run.abort();
            let after = r.factory.count() - before;
            assert!(
                (4..=6).contains(&after),
                "about one fire per real minute after the step, got {after}"
            );
        })
        .await;
}
