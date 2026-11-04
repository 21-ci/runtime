//! `promote()`: the one way a live version changes (`docs/design/branching-services-design.md` §5).
//!
//! Every flip writes a `version_switches` row and commits with it in one transaction. Then the
//! routes are re-read, and a task per switch watches the admission counter of the previous version
//! until it reaches zero or the deadline passes. All of this is control path: the invoke path reads
//! neither this module's state nor `meta.db`.
//!
//! **What a drain can know.** The scheduler counts two versions per service. If a third pushes out
//! a version with tasks running, [`Scheduler::inflight_known`] says `None` for that version from
//! then on, and its drain closes as `drain_timed_out` with `error = drain_unknown`, never as
//! `completed`. Flips are not refused during a drain: a rollback must not wait for old tasks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use dashmap::DashMap;
use rt_config::Config;
use rt_sched::{Clock, Scheduler};
use rt_sec::PlanSource;
use rt_store::{MetaStore, PageReq, StoreError};
use rt_types::{
    Etag, NewSwitch, NodeEvent, Service, ServiceId, Switch, SwitchId, SwitchKind, SwitchPatch, SwitchState, TenantId,
    UnixMs, VersionNo, VersionOrigin,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::AbortHandle;

use crate::dto::SwitchWarning;
use crate::error::ApiError;

/// `error` of a switch whose drain cannot be known (see the module docs).
pub const DRAIN_UNKNOWN: &str = "drain_unknown";
/// The warning of a flip whose route refresh keeps failing.
pub const ROUTES_REFRESH_DELAYED: &str = "routes_refresh_delayed";

/// Waits between route refresh attempts after a flip.
const REFRESH_BACKOFF: [Duration; 3] = [
    Duration::from_millis(50),
    Duration::from_millis(200),
    Duration::from_millis(800),
];
const REPAIR_EVERY: Duration = Duration::from_secs(1);
const DRAIN_POLL: Duration = Duration::from_millis(250);
/// Added to the longest a task can take.
const DRAIN_MARGIN_MS: u64 = 30_000;
/// The cap of a drain, until `deploy.drain_max` exists (§9 T6).
const DRAIN_MAX_MS: u64 = 15 * 60 * 1000;

/// Re-read the routes and the service table.
#[async_trait]
pub trait RouteRefresh: Send + Sync {
    async fn refresh(&self) -> Result<(), ApiError>;
}

/// One flip to ask for.
#[derive(Clone, Debug)]
pub struct PromoteReq {
    pub target: PromoteTarget,
    pub kind: SwitchKind,
    pub if_match: Option<Etag>,
    pub created_by: Option<String>,
    /// Answer `switch_in_progress` instead of waiting for a flip of this service that is running.
    pub no_wait: bool,
    /// Wait this long for the previous version instead of the longest a task can take.
    pub drain_timeout: Option<Duration>,
}

/// The version to make live.
#[derive(Clone, Copy, Debug)]
pub enum PromoteTarget {
    /// One of the service's own.
    Version(VersionNo),
    /// A version of another service, adopted by digest first, inside the lock.
    Adopt(VersionOrigin),
}

impl PromoteReq {
    pub fn new(to: VersionNo) -> Self {
        Self {
            target: PromoteTarget::Version(to),
            kind: SwitchKind::Promote,
            if_match: None,
            created_by: None,
            no_wait: false,
            drain_timeout: None,
        }
    }

    /// Adopt `origin` and flip to it, tracked as a switch.
    pub fn adopt(origin: VersionOrigin) -> Self {
        Self {
            target: PromoteTarget::Adopt(origin),
            kind: SwitchKind::Switch,
            ..Self::new(VersionNo(0))
        }
    }

    pub fn kind(mut self, kind: SwitchKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn no_wait(mut self) -> Self {
        self.no_wait = true;
        self
    }

    pub fn drain_timeout(mut self, wait: Option<Duration>) -> Self {
        self.drain_timeout = wait;
        self
    }

    pub fn if_match(mut self, etag: Option<Etag>) -> Self {
        self.if_match = etag;
        self
    }

    pub fn by(mut self, actor: impl Into<String>) -> Self {
        self.created_by = Some(actor.into());
        self
    }
}

/// A committed flip.
#[derive(Clone, Debug)]
pub struct Promoted {
    pub service: Service,
    pub switch: Switch,
    pub warnings: Vec<SwitchWarning>,
}

/// What [`Promoter::new`] needs.
pub struct PromoterDeps {
    pub meta: Arc<dyn MetaStore>,
    pub scheduler: Scheduler,
    pub config: Arc<ArcSwap<Config>>,
    pub plans: Arc<dyn PlanSource>,
    pub clock: Arc<dyn Clock>,
    pub events: UnboundedSender<NodeEvent>,
    pub refresher: Arc<dyn RouteRefresh>,
}

struct ActiveDrain {
    switch: SwitchId,
    from: VersionNo,
    task: AbortHandle,
}

/// The locks of one service.
///
/// `flip` is held by a flip from before adoption until it returns. `drains` is at most a few
/// entries, one per flip while old tasks run; the drain tasks take only this one, so a drain ending
/// never looks like a flip in progress.
#[derive(Default)]
struct ServiceLocks {
    flip: tokio::sync::Mutex<()>,
    drains: tokio::sync::Mutex<Vec<ActiveDrain>>,
}

type Slot = Arc<ServiceLocks>;

/// Flips live versions; one per node.
pub struct Promoter {
    deps: PromoterDeps,
    slots: DashMap<ServiceId, Slot>,
    repairing: AtomicBool,
    me: Weak<Promoter>,
}

impl Promoter {
    pub fn new(deps: PromoterDeps) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            deps,
            slots: DashMap::new(),
            repairing: AtomicBool::new(false),
            me: me.clone(),
        })
    }

    fn slot(&self, id: ServiceId) -> Slot {
        self.slots.entry(id).or_default().clone()
    }

    /// One route refresh, no retries.
    pub async fn refresh(&self) -> Result<(), ApiError> {
        self.deps.refresher.refresh().await
    }

    /// Drop the state of services `keep` rejects (deleted ones, on every route refresh).
    pub fn retain_services(&self, keep: impl Fn(&ServiceId) -> bool) {
        self.slots.retain(|id, _| keep(id));
    }

    fn emit(&self, kind: &str, tenant: TenantId, service: ServiceId, sw: &Switch) {
        let mut ev = NodeEvent::new(kind, self.deps.clock.unix_ms());
        ev.tenant = Some(tenant);
        ev.service = Some(service);
        ev.data = serde_json::json!({
            "switch": sw.id.to_string(),
            "kind": sw.kind,
            "from": sw.from_version.map(|v| v.0),
            "to": sw.to_version.0,
            "state": sw.state,
            "error": sw.error,
        });
        let _ = self.deps.events.send(ev);
    }

    /// The longest the previous version's tasks can take, from now.
    fn drain_deadline(&self, svc: &Service, asked: Option<Duration>) -> UnixMs {
        let plan = self.deps.plans.plan_for(svc.tenant_id);
        let eff = self
            .deps
            .config
            .load()
            .service_effective(svc.profile, &svc.config, &plan);
        let longest = eff.wall_ms.value.max(eff.queue_timeout.value.as_millis() as u64);
        let wait = asked
            .map_or(longest.saturating_add(DRAIN_MARGIN_MS), |d| d.as_millis() as u64)
            .min(DRAIN_MAX_MS);
        self.deps.clock.unix_ms().saturating_add(wait as i64)
    }

    /// Flip `svc` to `req.to`. After the commit nothing fails the call: a refresh that keeps
    /// failing comes back as a warning, because the flip already happened.
    pub async fn promote(&self, svc: &Service, req: PromoteReq) -> Result<Promoted, ApiError> {
        let slot = self.slot(svc.id);
        let _flip = if req.no_wait {
            slot.flip
                .try_lock()
                .map_err(|_| ApiError::new("switch_in_progress", "another switch of this service is running"))?
        } else {
            slot.flip.lock().await
        };
        // Adoption first: the row names the adopted number. Reused per origin, so a flip refused
        // below leaves a version the next attempt takes.
        let (to, source) = match req.target {
            PromoteTarget::Version(v) => (v, None),
            PromoteTarget::Adopt(origin) => {
                let adopted = self
                    .deps
                    .meta
                    .adopt_version(svc.id, origin.service, origin.version)
                    .await?;
                (adopted.number, Some(origin))
            }
        };
        let mut drains = slot.drains.lock().await;
        let new = NewSwitch {
            kind: req.kind,
            source,
            from_version: None, // written by the flip, with what was really live
            to_version: to,
            created_by: req.created_by,
        };
        let row = match self.deps.meta.create_switch(svc.id, new.clone()).await {
            Err(StoreError::Conflict("switch_in_progress")) => {
                self.close_stale(svc.id).await?;
                self.deps.meta.create_switch(svc.id, new).await?
            }
            other => other?,
        };
        self.emit("service.switch.started", svc.tenant_id, svc.id, &row);

        let deadline = self.drain_deadline(svc, req.drain_timeout);
        let (service, mut switch) = match self.deps.meta.flip_switch(svc.id, row.id, req.if_match, deadline).await {
            Ok(done) => done,
            Err(e) => return Err(self.flip_failed(svc, row, e).await),
        };
        self.emit("service.switch.flipped", svc.tenant_id, svc.id, &switch);

        // The target is live again: a drain waiting for it can never end, and is moot.
        for d in drains.iter().filter(|d| d.from == to) {
            d.task.abort();
            if let Some(closed) = self
                .close(svc.id, d.switch, SwitchState::DrainTimedOut, Some(DRAIN_UNKNOWN))
                .await
            {
                self.emit("service.switch.drain_timed_out", svc.tenant_id, svc.id, &closed);
            }
        }
        drains.retain(|d| d.from != to);

        if switch.state == SwitchState::Draining {
            if let Some(from) = switch.from_version {
                let known = self.deps.scheduler.inflight_known(svc.id, from.0).is_some();
                if known {
                    let task = tokio::spawn(watch_drain(
                        self.me.clone(),
                        svc.tenant_id,
                        svc.id,
                        switch.id,
                        from,
                        deadline,
                    ));
                    drains.push(ActiveDrain {
                        switch: switch.id,
                        from,
                        task: task.abort_handle(),
                    });
                } else if let Some(closed) = self
                    .close(svc.id, switch.id, SwitchState::DrainTimedOut, Some(DRAIN_UNKNOWN))
                    .await
                {
                    self.emit("service.switch.drain_timed_out", svc.tenant_id, svc.id, &closed);
                    switch = closed;
                }
            }
        }

        let mut warnings = Vec::new();
        if !self.refresh_with_retries().await {
            warnings.push(SwitchWarning {
                code: ROUTES_REFRESH_DELAYED.to_string(),
                names: Vec::new(),
            });
            self.spawn_repair();
        }
        Ok(Promoted {
            service,
            switch,
            warnings,
        })
    }

    /// This node's flips run under the flip lock we hold, so an open row is one whose closing failed
    /// after a store error. Left alone it would block every flip until the next start.
    async fn close_stale(&self, service: ServiceId) -> Result<(), ApiError> {
        let page = PageReq { limit: 1, cursor: None };
        for row in self.deps.meta.list_switches(service, page).await?.items {
            if matches!(row.state, SwitchState::Preparing | SwitchState::Migrating) {
                let patch = SwitchPatch {
                    state: Some(SwitchState::Failed),
                    error: Some("interrupted"),
                    ..SwitchPatch::default()
                };
                self.deps.meta.update_switch(service, row.id, patch).await?;
            }
        }
        Ok(())
    }

    /// The refused flip changed nothing; its row must not stay open, it would block the next one.
    async fn flip_failed(&self, svc: &Service, row: Switch, e: StoreError) -> ApiError {
        let code: &'static str = match &e {
            StoreError::Conflict(c) => c,
            StoreError::PreconditionFailed => "precondition_failed",
            StoreError::NotFound => "not_found",
            _ => "internal",
        };
        if let Some(closed) = self.close(svc.id, row.id, SwitchState::Failed, Some(code)).await {
            self.emit("service.switch.failed", svc.tenant_id, svc.id, &closed);
        }
        e.into()
    }

    /// Write a final state; a store failure is logged, and the row is closed at the next start.
    async fn close(
        &self,
        service: ServiceId,
        switch: SwitchId,
        state: SwitchState,
        error: Option<&'static str>,
    ) -> Option<Switch> {
        let patch = SwitchPatch {
            state: Some(state),
            error,
            ..SwitchPatch::default()
        };
        match self.deps.meta.update_switch(service, switch, patch).await {
            Ok(row) => Some(row),
            Err(e) => {
                tracing::warn!(%service, %switch, code = e.code(), "could not close a switch row");
                None
            }
        }
    }

    async fn refresh_with_retries(&self) -> bool {
        if self.refresh().await.is_ok() {
            return true;
        }
        for wait in REFRESH_BACKOFF {
            tokio::time::sleep(wait).await;
            if self.refresh().await.is_ok() {
                return true;
            }
        }
        false
    }

    /// Keep trying until the table is right. One task per node, whatever failed.
    fn spawn_repair(&self) {
        if self.repairing.swap(true, Ordering::SeqCst) {
            return;
        }
        let me = self.me.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(REPAIR_EVERY).await;
                let Some(p) = me.upgrade() else { return };
                if p.refresh().await.is_ok() {
                    p.repairing.store(false, Ordering::SeqCst);
                    return;
                }
            }
        });
    }
}

/// Holds no strong reference between polls, so a stopped node lets go of its stores; a row left
/// open by that is closed at the next start. The only store I/O is the final update.
async fn watch_drain(
    me: Weak<Promoter>,
    tenant: TenantId,
    service: ServiceId,
    switch: SwitchId,
    from: VersionNo,
    deadline: UnixMs,
) {
    loop {
        tokio::time::sleep(DRAIN_POLL).await;
        let Some(p) = me.upgrade() else { return };
        let ended = |p: &Promoter| {
            let known = p.deps.scheduler.inflight_known(service, from.0);
            match known {
                Some(0) => Some((SwitchState::Completed, None, "service.switch.drained")),
                None => Some((
                    SwitchState::DrainTimedOut,
                    Some(DRAIN_UNKNOWN),
                    "service.switch.drain_timed_out",
                )),
                Some(_) if p.deps.clock.unix_ms() >= deadline => {
                    Some((SwitchState::DrainTimedOut, None, "service.switch.drain_timed_out"))
                }
                Some(_) => None,
            }
        };
        if ended(&p).is_none() {
            continue;
        }
        // `promote` aborts this task under the same lock, so a closed row is not written again.
        let slot = p.slot(service);
        let mut drains = slot.drains.lock().await;
        let Some((state, error, event)) = ended(&p) else {
            continue;
        };
        let patch = SwitchPatch {
            state: Some(state),
            error,
            ..SwitchPatch::default()
        };
        match p.deps.meta.update_switch(service, switch, patch).await {
            Ok(row) => p.emit(event, tenant, service, &row),
            Err(StoreError::NotFound) => {} // the service was deleted
            Err(e) => {
                tracing::warn!(%service, %switch, code = e.code(), "drain update failed, trying again");
                continue;
            }
        }
        drains.retain(|d| d.switch != switch);
        return;
    }
}
