//! Schedules: `interval` (`every_seconds`) and `cron` (5-field, optional seconds field, IANA
//! timezone, default UTC).
//!
//! One timer wheel fires `Scheduler::submit` with `Trigger::Interval{id}` / `Cron{id}`.
//! Jitter `0..min(1 s, interval/10)` per schedule, seeded by the schedule ID. A tick is skipped
//! (and counted in `rt_schedule_skipped_total`) while the previous run of the same schedule is
//! still running — runs never overlap (TRG-05). Missed ticks are **not** backfilled after a
//! restart (TRG-08).
//!
//! The tenant's `plan.allowed_triggers` is checked when a schedule is created (control API) and
//! again on every fire ([`ScheduleEngine::with_plans`], in-memory plan cache): a fire the plan no
//! longer allows is dropped, counted in `rt_schedule_denied_total` and logged once per change.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rt_sched::{BoxFuture, Clock, Scheduler};
use rt_sec::PlanSource;
use rt_types::{Schedule, ScheduleId, ScheduleKind, TenantId, TriggerKind, UnixMs};
use thiserror::Error;
use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScheduleError {
    #[error("invalid cron expression: {0}")]
    BadCron(String),
    #[error("unknown timezone `{0}`")]
    BadTimezone(String),
    #[error("interval must be 1 second to 10 years")]
    BadInterval,
}

/// Longest interval schedule (10 years). Anything longer is refused at validation, so
/// `every_seconds * 1000` always fits an `i64` (CRON-002).
pub const MAX_INTERVAL_SECONDS: u64 = 10 * 366 * 24 * 3600;

/// `every_seconds` of a valid interval in milliseconds, else `BadInterval`.
fn interval_ms(every_seconds: u64) -> Result<i64, ScheduleError> {
    if every_seconds == 0 || every_seconds > MAX_INTERVAL_SECONDS {
        return Err(ScheduleError::BadInterval);
    }
    i64::try_from(every_seconds)
        .map(|s| s.saturating_mul(1000))
        .map_err(|_| ScheduleError::BadInterval)
}

/// Normalize a cron expression to the 6/7-field form the `cron` crate parses: a 5-field
/// expression gets a leading `0` seconds field.
pub fn normalize_cron(expr: &str) -> String {
    let fields = expr.split_whitespace().count();
    if fields == 5 {
        format!("0 {}", expr.trim())
    } else {
        expr.trim().to_string()
    }
}

/// Validate a cron expression and timezone without computing fires.
pub fn validate_kind(kind: &ScheduleKind) -> Result<(), ScheduleError> {
    match kind {
        ScheduleKind::Interval { every_seconds } => interval_ms(*every_seconds).map(|_| ()),
        ScheduleKind::Cron { expr, tz } => {
            Tz::from_str(tz).map_err(|_| ScheduleError::BadTimezone(tz.clone()))?;
            cron::Schedule::from_str(&normalize_cron(expr)).map_err(|e| ScheduleError::BadCron(e.to_string()))?;
            Ok(())
        }
    }
}

/// The next fire time strictly after `after_ms` (Unix ms), in the schedule's timezone for cron.
pub fn next_fire(kind: &ScheduleKind, after_ms: UnixMs) -> Result<Option<UnixMs>, ScheduleError> {
    match kind {
        ScheduleKind::Interval { every_seconds } => Ok(Some(after_ms.saturating_add(interval_ms(*every_seconds)?))),
        ScheduleKind::Cron { expr, tz } => {
            let tz = Tz::from_str(tz).map_err(|_| ScheduleError::BadTimezone(tz.clone()))?;
            let sched =
                cron::Schedule::from_str(&normalize_cron(expr)).map_err(|e| ScheduleError::BadCron(e.to_string()))?;
            let after: DateTime<Utc> = Utc
                .timestamp_millis_opt(after_ms)
                .single()
                .ok_or(ScheduleError::BadInterval)?;
            Ok(next_local_fire(&sched, tz, after))
        }
    }
}

/// The next fire after `after` of a cron expression read in local time of `tz` (CRON-003). The
/// expression walks the local wall clock: a local time that a DST change skips fires at the same
/// distance past the gap (02:30 on a 02:00 -> 03:00 day fires at 03:30), and a local time that
/// happens twice fires once, at its first occurrence.
fn next_local_fire(sched: &cron::Schedule, tz: Tz, after: DateTime<Utc>) -> Option<UnixMs> {
    use chrono::{LocalResult, Offset};
    let local_after = after.with_timezone(&tz).naive_local();
    // Candidates on the naive local clock (evaluated as if it were UTC).
    for naive in sched
        .after(&Utc.from_utc_datetime(&local_after))
        .map(|d| d.naive_utc())
        .take(4)
    {
        let at = match tz.from_local_datetime(&naive) {
            LocalResult::Single(t) => t.with_timezone(&Utc),
            LocalResult::Ambiguous(first, second) => {
                if first.with_timezone(&Utc) > after {
                    first.with_timezone(&Utc)
                } else {
                    second.with_timezone(&Utc)
                }
            }
            LocalResult::None => {
                // In a gap: use the offset in force just before it.
                let before = tz
                    .from_local_datetime(&(naive - chrono::Duration::hours(3)))
                    .earliest()?;
                let offset = before.offset().fix();
                Utc.from_utc_datetime(&(naive - chrono::Duration::seconds(i64::from(offset.local_minus_utc()))))
            }
        };
        if at > after {
            return Some(at.timestamp_millis());
        }
    }
    None
}

/// Per-schedule jitter: deterministic in the schedule ID, `0..min(1 s, interval/10)`.
pub fn jitter(id: ScheduleId, interval: Duration) -> Duration {
    let max = Duration::from_secs(1).min(interval / 10);
    if max.is_zero() {
        return Duration::ZERO;
    }
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in id.bare().bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    Duration::from_nanos(h % max.as_nanos() as u64)
}

/// Safety bound on the number of cron ticks walked while catching up after a long stall.
const MAX_CATCHUP_TICKS: u64 = 1_000;

/// One live schedule.
struct Entry {
    schedule: Schedule,
    /// Scheduled (grid) time of the next fire, Unix ms. This is what the task sees.
    sched_ms: UnixMs,
    /// `sched_ms` plus the per-schedule jitter: when the fire actually happens.
    due_ms: UnixMs,
}

#[derive(Default)]
struct State {
    entries: HashMap<ScheduleId, Entry>,
    /// Schedules whose previous task has not finished yet (no overlap).
    running: HashSet<ScheduleId>,
    /// Schedules whose last fire the tenant's plan refused (logged once until allowed again).
    denied: HashSet<ScheduleId>,
}

type Inflight = FuturesUnordered<Pin<Box<dyn Future<Output = (ScheduleId, &'static str)> + Send>>>;

/// Fires schedules through the scheduler using the injected clock (virtual in simulation).
///
/// One loop (`run`) owns the whole timer wheel: it sleeps until the earliest due schedule,
/// submits the task, and tracks the in-flight handle so a schedule never overlaps itself.
pub struct ScheduleEngine {
    clock: Arc<dyn Clock>,
    scheduler: Scheduler,
    tasks: Arc<dyn ScheduleTaskFactory>,
    state: Mutex<State>,
    wake: Notify,
    fires: AtomicU64,
    skipped: AtomicU64,
    denied: AtomicU64,
    metrics: Option<Arc<rt_obs::Metrics>>,
    persist: Option<PersistNextDue>,
    plans: Option<Arc<dyn PlanSource>>,
}

fn trigger_of(kind: &ScheduleKind) -> TriggerKind {
    match kind {
        ScheduleKind::Interval { .. } => TriggerKind::Interval,
        ScheduleKind::Cron { .. } => TriggerKind::Cron,
    }
}

/// Stores a schedule's next scheduled time (unjittered), so a restart keeps the grid instead of
/// re-anchoring the schedule at the restart time.
pub type PersistNextDue = Arc<dyn Fn(ScheduleId, UnixMs) + Send + Sync>;

fn jitter_ms(id: ScheduleId, interval: Duration) -> i64 {
    i64::try_from(jitter(id, interval).as_millis()).unwrap_or(0)
}

/// Typical gap between fires of `kind` around `at_ms` (jitter is derived from it).
fn period_of(kind: &ScheduleKind, at_ms: UnixMs) -> Duration {
    match kind {
        ScheduleKind::Interval { every_seconds } => Duration::from_secs(*every_seconds),
        ScheduleKind::Cron { .. } => match next_fire(kind, at_ms) {
            Ok(Some(n)) => Duration::from_millis(u64::try_from(n - at_ms).unwrap_or(0)),
            _ => Duration::ZERO,
        },
    }
}

impl Entry {
    /// Set the first scheduled time (strictly after `after_ms`).
    fn first(schedule: Schedule, after_ms: UnixMs) -> Result<Entry, ScheduleError> {
        // A cron expression with no future fire: park it far away.
        let sched = next_fire(&schedule.kind, after_ms)?.unwrap_or(i64::MAX / 2);
        let j = jitter_ms(schedule.id, period_of(&schedule.kind, sched));
        Ok(Entry {
            sched_ms: sched,
            due_ms: sched.saturating_add(j),
            schedule,
        })
    }

    /// Advance past `prev_sched` to the next tick whose due time is after `now`. Returns how
    /// many whole ticks were passed over (not fired).
    fn advance(&mut self, prev_sched: UnixMs, now: UnixMs) -> u64 {
        let mut skipped = 0u64;
        let mut s = prev_sched;
        loop {
            let next = match next_fire(&self.schedule.kind, s) {
                Ok(Some(n)) => n,
                _ => {
                    self.sched_ms = i64::MAX / 2;
                    self.due_ms = i64::MAX / 2;
                    return skipped;
                }
            };
            let j = jitter_ms(self.schedule.id, period_of(&self.schedule.kind, next));
            if next.saturating_add(j) > now {
                self.sched_ms = next;
                self.due_ms = next.saturating_add(j);
                return skipped;
            }
            skipped += 1;
            match &self.schedule.kind {
                ScheduleKind::Interval { every_seconds } => {
                    // Jump over the whole stalled stretch at once.
                    let step = interval_ms(*every_seconds).unwrap_or(1000).max(1000);
                    let behind = now.saturating_sub(j).saturating_sub(next);
                    let extra = behind.div_euclid(step).saturating_add(1);
                    skipped += (extra - 1).max(0) as u64;
                    let n = next.saturating_add(extra.saturating_mul(step));
                    self.sched_ms = n;
                    self.due_ms = n.saturating_add(j);
                    return skipped;
                }
                ScheduleKind::Cron { .. } => {
                    if skipped >= MAX_CATCHUP_TICKS {
                        s = now;
                    } else {
                        s = next;
                    }
                }
            }
        }
    }
}

impl ScheduleEngine {
    pub fn new(clock: Arc<dyn Clock>, scheduler: Scheduler, tasks: Arc<dyn ScheduleTaskFactory>) -> Self {
        Self {
            clock,
            scheduler,
            tasks,
            state: Mutex::new(State::default()),
            wake: Notify::new(),
            fires: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            denied: AtomicU64::new(0),
            metrics: None,
            persist: None,
            plans: None,
        }
    }

    /// Check the tenant's `plan.allowed_triggers` on every fire, not only when the schedule is
    /// created: a plan changed since (e.g. reloaded on `SIGHUP`) stops the fires. `plans` must be
    /// the in-memory plan cache (`plan_arc`); it is called once per fire.
    pub fn with_plans(mut self, plans: Arc<dyn PlanSource>) -> Self {
        self.plans = Some(plans);
        self
    }

    /// Whether `tenant`'s plan allows `schedule`'s trigger now; logs the transitions.
    fn plan_allows(&self, tenant: TenantId, schedule: &Schedule) -> bool {
        let Some(plans) = &self.plans else { return true };
        let trigger = trigger_of(&schedule.kind);
        let allowed = plans.plan_arc(tenant).allows_trigger(trigger);
        let mut st = self.state.lock();
        if allowed {
            if st.denied.remove(&schedule.id) {
                tracing::info!(schedule = %schedule.id, service = %schedule.service_id, trigger = trigger.as_str(), "schedule allowed by the plan again");
            }
        } else if st.denied.insert(schedule.id) {
            tracing::warn!(
                schedule = %schedule.id,
                service = %schedule.service_id,
                %tenant,
                trigger = trigger.as_str(),
                "schedule fire dropped: the tenant's plan no longer allows this trigger"
            );
        }
        allowed
    }

    /// Persist each schedule's next scheduled time after it moves (`schedules.next_due_ms`);
    /// [`ScheduleEngine::upsert`] honors it after a restart while it is still on the grid.
    pub fn with_persist(mut self, persist: PersistNextDue) -> Self {
        self.persist = Some(persist);
        self
    }

    /// Add or replace a schedule (computes its next fire). A disabled schedule is removed from
    /// the wheel. The first fire is on the schedule's grid strictly after *now*: stale
    /// `next_due_ms` values from before a restart are never backfilled (TRG-08).
    pub fn upsert(&self, schedule: Schedule) -> Result<(), ScheduleError> {
        validate_kind(&schedule.kind)?;
        let now = self.clock.unix_ms();
        if !schedule.enabled {
            self.state.lock().entries.remove(&schedule.id);
            self.wake.notify_one();
            return Ok(());
        }
        // A persisted future due time is honored only while it is still on the grid.
        let anchor = match (&schedule.kind, schedule.next_due_ms) {
            (ScheduleKind::Interval { every_seconds }, Some(t))
                if t > now && t <= now.saturating_add(interval_ms(*every_seconds).unwrap_or(0)) =>
            {
                Some(t)
            }
            (ScheduleKind::Cron { .. }, Some(t)) if t > now && next_fire(&schedule.kind, t - 1) == Ok(Some(t)) => {
                Some(t)
            }
            _ => None,
        };
        let entry = match anchor {
            Some(t) => {
                let j = jitter_ms(schedule.id, period_of(&schedule.kind, t));
                Entry {
                    sched_ms: t,
                    due_ms: t + j,
                    schedule,
                }
            }
            None => Entry::first(schedule, now)?,
        };
        self.state.lock().entries.insert(entry.schedule.id, entry);
        self.wake.notify_one();
        Ok(())
    }

    pub fn remove(&self, id: ScheduleId) {
        let mut st = self.state.lock();
        st.entries.remove(&id);
        st.denied.remove(&id);
        drop(st);
        self.wake.notify_one();
    }

    /// Run the timer loop until the future is dropped / aborted.
    pub async fn run(&self) {
        let mut inflight: Inflight = FuturesUnordered::new();
        let mut last_now = self.clock.unix_ms();
        loop {
            let now = self.clock.unix_ms();
            if now < last_now - 1_000 {
                self.reanchor(now);
            }
            last_now = now;
            self.fire_due(now, &mut inflight);

            let earliest = self.state.lock().entries.values().map(|e| e.due_ms).min();
            let sleep: BoxFuture<'static, ()> = match earliest {
                Some(due) => {
                    let ms = (due.saturating_sub(now)).clamp(1, i64::MAX / 4);
                    self.clock.sleep(Duration::from_millis(ms as u64))
                }
                None => Box::pin(std::future::pending()),
            };
            tokio::select! {
                _ = sleep => {}
                _ = self.wake.notified() => {}
                Some((id, label)) = inflight.next(), if !inflight.is_empty() => {
                    self.state.lock().running.remove(&id);
                    tracing::debug!(schedule = %id, outcome = label, "schedule run finished");
                }
            }
        }
    }

    /// The wall clock stepped back (NTP, an operator): a next fire computed on the old clock may
    /// now be far ahead. Move every schedule whose fresh next fire is earlier to it, so nothing
    /// stalls for the size of the step (CRON-009); fires still follow real time, never doubled.
    fn reanchor(&self, now: UnixMs) {
        let mut st = self.state.lock();
        for e in st.entries.values_mut() {
            if let Ok(fresh) = Entry::first(e.schedule.clone(), now) {
                if fresh.sched_ms < e.sched_ms {
                    *e = fresh;
                }
            }
        }
    }

    fn count_fire(&self) {
        self.fires.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.schedule_fires_total.inc();
        }
    }

    fn count_denied(&self) {
        self.denied.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.schedule_denied_total.inc();
        }
    }

    fn count_skipped(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.skipped.fetch_add(n, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.schedule_skipped_total.inc_by(n);
        }
    }

    /// Fire (or skip) every schedule that is due at `now`, then move it to its next tick.
    fn fire_due(&self, now: UnixMs, inflight: &mut Inflight) {
        let due: Vec<(Schedule, UnixMs, bool)> = {
            let st = self.state.lock();
            st.entries
                .values()
                .filter(|e| e.due_ms <= now)
                .map(|e| (e.schedule.clone(), e.sched_ms, st.running.contains(&e.schedule.id)))
                .collect()
        };
        for (schedule, sched_ms, running) in due {
            let id = schedule.id;
            if running {
                // The previous run is still going: this tick is skipped (no overlap).
                self.count_skipped(1);
            } else {
                match self.tasks.task_for(&schedule, sched_ms) {
                    Some(spec) if !self.plan_allows(spec.service.tenant, &schedule) => self.count_denied(),
                    Some(spec) => match self.scheduler.submit(spec) {
                        Ok(handle) => {
                            self.state.lock().running.insert(id);
                            self.count_fire();
                            inflight.push(Box::pin(async move {
                                let outcome = handle.await;
                                (id, outcome.label())
                            }));
                        }
                        Err(reason) => {
                            tracing::debug!(schedule = %id, reason = reason.reason(), "schedule fire rejected");
                            self.count_skipped(1);
                        }
                    },
                    None => self.count_skipped(1),
                }
            }
            let mut st = self.state.lock();
            if let Some(e) = st.entries.get_mut(&id) {
                if e.sched_ms == sched_ms {
                    let extra = e.advance(sched_ms, now);
                    let next = e.sched_ms;
                    drop(st);
                    self.count_skipped(extra);
                    if let Some(p) = &self.persist {
                        p(id, next);
                    }
                }
            }
        }
    }

    /// Next due time (scheduled time plus jitter) for introspection and tests.
    pub fn next_due(&self, id: ScheduleId) -> Option<UnixMs> {
        self.state.lock().entries.get(&id).map(|e| e.due_ms)
    }

    /// Count fires / skipped ticks into `rt_schedule_fires_total` / `rt_schedule_skipped_total`
    /// (added by the W2 test lane so TRG-05 can be asserted through the registry).
    pub fn with_metrics(mut self, metrics: Arc<rt_obs::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// In-engine counters (same numbers as the metrics; usable without a registry).
    pub fn stats(&self) -> ScheduleStats {
        ScheduleStats {
            fires: self.fires.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            denied: self.denied.load(Ordering::Relaxed),
        }
    }
}

/// Counters of one [`ScheduleEngine`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScheduleStats {
    /// Tasks submitted for schedule fires.
    pub fires: u64,
    /// Ticks skipped because the previous run of the same schedule was still running.
    pub skipped: u64,
    /// Fires dropped because the tenant's plan no longer allows the trigger
    /// (`rt_schedule_denied_total`, [`ScheduleEngine::with_plans`]).
    pub denied: u64,
}

/// Builds the `TaskSpec` of a fire (resolves service config, limits, payload). Implemented by
/// `rt-server` (it owns config + route resolution).
pub trait ScheduleTaskFactory: Send + Sync {
    /// `None` when the service is gone or not routable (the fire is skipped).
    fn task_for(&self, schedule: &Schedule, scheduled_time_ms: UnixMs) -> Option<rt_types::TaskSpec>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// The typical gap between fires: an interval's period, a cron's gap to its next fire, and
    /// zero for a cron with no next fire (it never fires, or its zone is unknown).
    #[test]
    fn the_period_is_the_gap_between_fires_and_zero_without_a_next_fire() {
        let every = ScheduleKind::Interval { every_seconds: 90 };
        assert_eq!(period_of(&every, 0), Duration::from_secs(90));
        let at = chrono::Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("date")
            .timestamp_millis();
        let daily = ScheduleKind::Cron {
            expr: "30 2 * * *".into(),
            tz: "UTC".into(),
        };
        assert_eq!(period_of(&daily, at), Duration::from_secs(2 * 3600 + 30 * 60));
        let never = ScheduleKind::Cron {
            expr: "0 0 30 2 *".into(),
            tz: "UTC".into(),
        };
        assert_eq!(
            period_of(&never, at),
            Duration::ZERO,
            "a cron that never fires has no period"
        );
        let bad_zone = ScheduleKind::Cron {
            expr: "0 0 * * *".into(),
            tz: "Nowhere/Land".into(),
        };
        assert_eq!(
            period_of(&bad_zone, at),
            Duration::ZERO,
            "an unknown zone has no period"
        );
    }
}
