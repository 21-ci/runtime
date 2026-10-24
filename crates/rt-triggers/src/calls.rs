//! Service-to-service calls: in-process child tasks, no HTTP hop.
//!
//! The child gets `Trigger::Call{parent, depth+1}`, is placed in the caller P's `runnext` when
//! kinds match, runs under `wall = min(own, parent remaining)`, and counts toward the child
//! service's CPU/memory and the tenant's concurrency. Same tenant only unless
//! `plan.allow_cross_tenant_calls`.
//!
//! Same-tenant callers must also be admitted by the target (decision D-A1): listed in its
//! `call.allowed_callers` (slug, id or `env:<name>`), or, with an empty list, in the same
//! environment. Calls additionally need `call` in the target tenant's `plan.allowed_triggers`.
//! Everything is resolved from the in-memory route table and plan cache (no `meta.db`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use rt_host::rpc::{check_args, rpc_request, rpc_result, valid_method};
use rt_host::{CallError, Calls, EnqueueOpts, Request, Response, RpcError, TaskCtx};
use rt_sched::{Clock, Scheduler};
use rt_sec::PlanSource;
use rt_store::{NewMessage, QueueStore};
use rt_types::{
    Access, Body, CallAccess, CallTarget, Caller, CallerStamp, IngressRules, MsgId, Outcome, RejectReason, RouteEntry,
    SdkCaller, SdkStamp, SdkTarget, ServiceId, TenantId, TriggerKind, TriggerSwitches, UsageDelta, MAX_CALL_DEPTH,
};

use crate::routes::RouteTable;

/// Depth of a call made by a task at `parent_depth`; `Err(Depth)` past [`MAX_CALL_DEPTH`]
/// (`508 call_depth`, TRG-15).
pub fn child_depth(parent_depth: u8) -> Result<u8, CallError> {
    let d = parent_depth.saturating_add(1);
    if d > MAX_CALL_DEPTH {
        Err(CallError::Depth)
    } else {
        Ok(d)
    }
}

/// Builds the child `TaskSpec` from a resolved route + request. Implemented by `rt-server`
/// (it owns config resolution and the engine fingerprints).
pub trait CallTaskFactory: Send + Sync {
    fn child_task(
        &self,
        parent: &TaskCtx,
        target: &rt_types::RouteEntry,
        req: Request,
        depth: u8,
    ) -> Result<rt_types::TaskSpec, CallError>;

    /// The service is durable: its requests run one at a time per object.
    fn is_durable(&self, service: ServiceId) -> bool {
        let _ = service;
        false
    }
}

/// Called after a successful `enqueue` so the queue engine can deliver without waiting for its
/// next poll.
pub type EnqueueNotifier = Arc<dyn Fn(ServiceId) + Send + Sync>;

/// Records usage on behalf of a tenant: an operator SDK call's CPU goes to the caller's tenant
/// (with the SDK's service), not to the system tenant.
pub type UsageRecorder = Arc<dyn Fn(TenantId, ServiceId, UsageDelta) + Send + Sync>;

/// `rt_host::Calls` over the route table and scheduler.
pub struct CallDispatcher {
    routes: Arc<RouteTable>,
    scheduler: Scheduler,
    queue: Arc<dyn QueueStore>,
    plans: Arc<dyn PlanSource>,
    tasks: Arc<dyn CallTaskFactory>,
    clock: Option<Arc<dyn Clock>>,
    notifier: Option<EnqueueNotifier>,
    usage: Option<UsageRecorder>,
    /// Turns on durable objects (requests to one object run one at a time).
    sockets: Option<Arc<crate::sockets::SocketHub>>,
    /// Per-caller-tenant caps on SDK calls (they run in `sys`, SDK-14); built on first use so
    /// [`CallDispatcher::with_clock`] reaches it.
    sdk_limits: std::sync::OnceLock<crate::sdk_limits::SdkCallerLimits>,
}

impl CallDispatcher {
    pub fn new(
        routes: Arc<RouteTable>,
        scheduler: Scheduler,
        queue: Arc<dyn QueueStore>,
        plans: Arc<dyn PlanSource>,
        tasks: Arc<dyn CallTaskFactory>,
    ) -> Self {
        Self {
            routes,
            scheduler,
            queue,
            plans,
            tasks,
            clock: None,
            notifier: None,
            usage: None,
            sockets: None,
            sdk_limits: std::sync::OnceLock::new(),
        }
    }

    fn sdk_limits(&self) -> &crate::sdk_limits::SdkCallerLimits {
        self.sdk_limits.get_or_init(|| {
            let clock = self.clock.clone().unwrap_or_else(|| Arc::new(rt_sched::SystemClock));
            crate::sdk_limits::SdkCallerLimits::new(clock)
        })
    }

    /// Serialize requests to durable objects through the socket hub's object turns.
    pub fn with_sockets(mut self, hub: Arc<crate::sockets::SocketHub>) -> Self {
        self.sockets = Some(hub);
        self
    }

    /// Run a child task of a durable target in its object's turn (unless the caller already holds
    /// it: a durable object calling itself), else as is. Resolves to the child's response.
    async fn run_child(
        &self,
        from: &TaskCtx,
        target: &RouteEntry,
        mut spec: rt_types::TaskSpec,
        object: Option<&str>,
    ) -> Result<Response, CallError> {
        let mut turn = None;
        if let Some(object) = object {
            let reentrant = from.service == target.service && from.object.as_deref() == Some(object);
            if let (false, Some(hub)) = (reentrant, &self.sockets) {
                let t = hub
                    .turn(target.tenant, target.service, object)
                    .await
                    .map_err(|e| match e {
                        crate::sockets::TurnError::Draining => CallError::Rejected(RejectReason::Draining),
                        crate::sockets::TurnError::Busy => CallError::Rejected(RejectReason::Overloaded),
                    })?;
                spec.object = Some(t.object.clone());
                turn = Some(t);
            } else {
                spec.object = Some(rt_types::ObjectRef::named(object));
            }
        }
        let handle = self.scheduler.submit(spec).map_err(|r| match r {
            RejectReason::CallDepth => CallError::Depth,
            other => CallError::Rejected(other),
        })?;
        let resp = outcome_response(handle.await);
        drop(turn);
        Ok(resp)
    }

    /// Record the CPU of operator SDK calls for the calling tenant.
    pub fn with_usage(mut self, f: UsageRecorder) -> Self {
        self.usage = Some(f);
        self
    }

    /// The clock the callers' `TaskCtx::deadline` instants come from (the scheduler's clock).
    /// Without it deadlines are compared against `Instant::now()`.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Tell the queue engine about every successful enqueue.
    pub fn with_enqueue_notifier(mut self, f: EnqueueNotifier) -> Self {
        self.notifier = Some(f);
        self
    }

    fn now(&self) -> Instant {
        self.clock.as_ref().map_or_else(Instant::now, |c| c.now())
    }

    /// Resolve `slug` and apply the checks every service-to-service operation shares:
    /// same tenant (unless the caller's plan allows cross-tenant calls, TRG-16).
    fn target(&self, from: &TaskCtx, slug: &str) -> Result<Arc<RouteEntry>, CallError> {
        let target = self.routes.by_slug(slug).ok_or(CallError::NotFound)?;
        if target.tenant != from.tenant && !self.plans.plan_arc(from.tenant).allow_cross_tenant_calls {
            return Err(CallError::CrossTenant);
        }
        Ok(target)
    }

    /// `ctx.caller` of the callee: the caller's service, slug, project and environment as this
    /// dispatcher's routes know them (the slug, project and environment only within its tenant).
    fn caller_stamp(&self, from: &TaskCtx) -> CallerStamp {
        let entry = self.routes.by_service(from.service).filter(|e| e.tenant == from.tenant);
        CallerStamp {
            service: from.service,
            tenant: from.tenant,
            slug: entry.as_ref().map(|e| e.slug.clone()),
            project: entry.as_ref().map(|e| e.project),
            environment: entry.as_ref().map(|e| e.environment.clone()),
            plan: None,
            sdk: None,
        }
    }

    /// D-A1: the caller must be admitted by the target's `call.allowed_callers`. Same tenant: a
    /// listed slug/id/`env:`, or (empty list) the same environment. Another tenant (its plan
    /// allows cross-tenant calls, see [`Self::target`]): listed by service id, nothing else.
    fn admit_caller(&self, from: &TaskCtx, target: &RouteEntry) -> Result<(), CallError> {
        let caller_entry = self.routes.by_service(from.service).filter(|e| e.tenant == from.tenant);
        let caller = Caller {
            tenant: from.tenant,
            service: from.service,
            slug: caller_entry.as_ref().map(|e| e.slug.as_str()),
            project: caller_entry.as_ref().map(|e| e.project),
            environment: caller_entry.as_ref().map(|e| e.environment.as_str()),
        };
        let callee = CallTarget {
            tenant: target.tenant,
            service: target.service,
            project: target.project,
            environment: &target.environment,
        };
        if target.call.admits(&callee, &caller) {
            Ok(())
        } else {
            Err(CallError::NotAllowed)
        }
    }
}

/// The target of an SDK call: the SDK's system service at the release's version (never its live
/// version, so channels and pins bind different versions of one service).
fn sdk_route(t: &SdkTarget) -> RouteEntry {
    RouteEntry {
        tenant: rt_types::system_tenant(),
        service: t.service,
        slug: rt_types::sdk_service_slug(&t.sdk),
        runtime: t.runtime,
        version: t.version_no,
        digest: t.digest,
        access: Access::Internal,
        triggers: TriggerSwitches::default(),
        ingress: IngressRules::default(),
        profile: None,
        hosts: Vec::new(),
        project: rt_types::default_project_id(rt_types::system_tenant()),
        environment: rt_types::default_environment(),
        call: CallAccess::default(),
    }
}

/// An error-shaped response object (`x-rt-reason`), what a parent sees for a child that was
/// killed or refused (TRG-17: the parent sees a 504 response object).
fn reason_response(status: u16, reason: &str) -> Response {
    let mut resp = http::Response::new(Body::empty());
    *resp.status_mut() = http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    if let Ok(v) = http::HeaderValue::from_str(reason) {
        resp.headers_mut().insert("x-rt-reason", v);
    }
    resp
}

fn outcome_response(outcome: Outcome) -> Response {
    match outcome {
        Outcome::Ok(reply) => {
            let mut resp = http::Response::new(reply.body);
            *resp.status_mut() = reply.status;
            *resp.headers_mut() = reply.headers;
            resp
        }
        other => reason_response(other.http_status(), other.reason().unwrap_or("guest_error")),
    }
}

#[async_trait]
impl Calls for CallDispatcher {
    async fn call(&self, from: &TaskCtx, slug: &str, req: Request) -> Result<Response, CallError> {
        let depth = child_depth(from.depth)?;
        let target = self.target(from, slug)?;
        if !target.triggers.call {
            return Err(CallError::Forbidden);
        }
        if !self.plans.plan_arc(target.tenant).allows_trigger(TriggerKind::Call) {
            return Err(CallError::TriggerNotAllowed);
        }
        self.admit_caller(from, &target)?;
        let remaining = from.deadline.saturating_duration_since(self.now());
        if remaining.is_zero() {
            // The parent is already out of time: the child cannot run at all.
            return Ok(reason_response(504, "timeout"));
        }

        let mut spec = self.tasks.child_task(from, &target, req, depth)?;
        spec.caller = Some(self.caller_stamp(from));
        // Child wall = min(own limit, parent remaining) (TRG-17).
        spec.limits.wall = spec.limits.wall.min(remaining);
        if spec.limits.wall < Duration::from_millis(1) {
            return Ok(reason_response(504, "timeout"));
        }
        // A plain request to a durable service is a request to its default object.
        let object = self
            .tasks
            .is_durable(target.service)
            .then_some(rt_types::DEFAULT_OBJECT);
        self.run_child(from, &target, spec, object).await
    }

    async fn call_object(
        &self,
        from: &TaskCtx,
        slug: Option<&str>,
        object: &str,
        req: Request,
    ) -> Result<Response, CallError> {
        let depth = child_depth(from.depth)?;
        let target = match slug {
            Some(slug) => self.target(from, slug)?,
            None => self
                .routes
                .by_service(from.service)
                .filter(|e| e.tenant == from.tenant)
                .ok_or(CallError::NotFound)?,
        };
        if !self.tasks.is_durable(target.service) {
            return Err(CallError::NotDurable);
        }
        if target.service != from.service {
            if !target.triggers.call {
                return Err(CallError::Forbidden);
            }
            if !self.plans.plan_arc(target.tenant).allows_trigger(TriggerKind::Call) {
                return Err(CallError::TriggerNotAllowed);
            }
            self.admit_caller(from, &target)?;
        }
        let remaining = from.deadline.saturating_duration_since(self.now());
        if remaining < Duration::from_millis(1) {
            return Ok(reason_response(504, "timeout"));
        }
        let mut spec = self.tasks.child_task(from, &target, req, depth)?;
        spec.caller = Some(self.caller_stamp(from));
        spec.limits.wall = spec.limits.wall.min(remaining);
        self.run_child(from, &target, spec, Some(object)).await
    }

    async fn sdk_invoke(&self, from: &TaskCtx, sdk: &str, method: &str, args_json: &str) -> Result<String, RpcError> {
        let depth = child_depth(from.depth).map_err(|e| RpcError::from_call(&e))?;
        if !valid_method(method) {
            return Err(RpcError::new(
                "validation_failed",
                "the method is a dot-separated path of identifiers",
            ));
        }
        let args = check_args(args_json)?;
        // Grants attach to the caller's plan, tenant, project and environment: a caller the
        // routes do not know (another SDK, a parked task) is granted nothing.
        let entry = self
            .routes
            .by_service(from.service)
            .filter(|e| e.tenant == from.tenant)
            .ok_or_else(|| {
                RpcError::new(
                    "sdk_not_granted",
                    format!("the SDK `{sdk}` is not granted to this service"),
                )
            })?;
        let plan = self.plans.plan_name(from.tenant);
        let caller = SdkCaller {
            tenant: from.tenant,
            plan: &plan,
            project: entry.project,
            environment: &entry.environment,
        };
        let target = self
            .routes
            .sdk_resolve(&caller, sdk)
            .map_err(|d| RpcError::new(d.code(), d.message(sdk)))?;
        let _permit = self
            .sdk_limits()
            .admit(from.tenant, &self.plans.plan_arc(from.tenant))
            .map_err(|r| {
                RpcError::new(
                    "sdk_rate_limited",
                    match r {
                        crate::sdk_limits::SdkRefusal::Concurrency => {
                            "too many SDK calls of this tenant in flight (plan max_concurrency)"
                        }
                        crate::sdk_limits::SdkRefusal::Rate => {
                            "too many SDK calls of this tenant per second (plan rps)"
                        }
                    },
                )
            })?;
        let remaining = from.deadline.saturating_duration_since(self.now());
        if remaining < Duration::from_millis(1) {
            return Err(RpcError::new("timeout", "the caller is out of time"));
        }

        let req = rpc_request(method, args)?;
        let route = sdk_route(&target);
        let mut spec = self
            .tasks
            .child_task(from, &route, req, depth)
            .map_err(|e| RpcError::from_call(&e))?;
        spec.caller = Some(CallerStamp {
            service: from.service,
            tenant: from.tenant,
            slug: Some(entry.slug.clone()),
            project: Some(entry.project),
            environment: Some(entry.environment.clone()),
            plan: Some(plan.clone()),
            sdk: Some(SdkStamp {
                name: sdk.to_string(),
                version: target.version.clone(),
            }),
        });
        spec.limits.wall = spec.limits.wall.min(remaining);
        let mut handle = self.scheduler.submit(spec).map_err(|r| match r {
            RejectReason::CallDepth => RpcError::from_call(&CallError::Depth),
            other => RpcError::from_call(&CallError::Rejected(other)),
        })?;
        let outcome = (&mut handle).await;
        // The SDK's CPU is the caller tenant's usage (attribution only, not billed separately).
        if let Some(record) = &self.usage {
            let cpu_ms = handle.timings().cpu.as_millis() as u64;
            if cpu_ms > 0 {
                record(
                    from.tenant,
                    target.service,
                    UsageDelta {
                        cpu_ms,
                        ..UsageDelta::default()
                    },
                );
            }
        }
        rpc_result(outcome_response(outcome)).await
    }

    async fn enqueue(&self, from: &TaskCtx, slug: &str, body: Bytes, o: EnqueueOpts) -> Result<MsgId, CallError> {
        let target = self.target(from, slug)?;
        if !target.triggers.queue {
            return Err(CallError::Forbidden);
        }
        let plan = self.plans.plan_arc(target.tenant);
        if !plan.allows_trigger(TriggerKind::Queue) {
            return Err(CallError::TriggerNotAllowed);
        }
        self.admit_caller(from, &target)?;
        let msg = NewMessage {
            body,
            delay: o.delay,
            dedup_id: o.dedup_id,
        };
        // The depth is counted where the message is inserted: concurrent enqueues cannot
        // overshoot `max_queue_depth` (CALL-011).
        let id = self
            .queue
            .enqueue_capped(target.service, msg, plan.max_queue_depth)
            .await
            .map_err(|e| match e {
                rt_store::StoreError::Conflict("queue_full") => CallError::QueueFull,
                e => {
                    tracing::warn!(error = %e, "queue enqueue failed");
                    CallError::Failed
                }
            })?;
        if let Some(n) = &self.notifier {
            n(target.service);
        }
        Ok(id)
    }
}
