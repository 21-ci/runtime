//! rt-triggers (L5): the route table, schedule engine (interval + cron), queue engine and
//! service-to-service call dispatch. Every trigger produces a `TaskSpec` and calls
//! `Scheduler::submit`; triggers never execute code themselves.

pub mod calls;
pub mod delayed;
pub mod queue_engine;
pub mod ratelimit;
pub mod routes;
pub mod schedules;
pub mod sdk_limits;
pub mod sockets;

pub use calls::{child_depth, CallDispatcher, CallTaskFactory, EnqueueNotifier};
pub use delayed::{DelayedEngine, DelayedEngineConfig, DelayedTaskFactory};
pub use queue_engine::{
    backoff_delay, QueueEngine, QueueEngineConfig, QueueTaskFactory, RETRY_BASE, RETRY_HEADER, VISIBILITY_MARGIN,
};
pub use ratelimit::ShardedBucket;
pub use routes::{normalize_host, Route, RouteMatch, RouteResolver, RouteTable};
pub use schedules::{
    jitter, next_fire, normalize_cron, validate_kind, PersistNextDue, ScheduleEngine, ScheduleError, ScheduleStats,
    ScheduleTaskFactory, MAX_INTERVAL_SECONDS,
};
pub use sockets::{
    accept_key, is_upgrade_request, Claim, HubConfig, HubStats, ObjectTurn, SocketHub, SocketLimits, SocketTaskFactory,
    Submitter, TurnError,
};
