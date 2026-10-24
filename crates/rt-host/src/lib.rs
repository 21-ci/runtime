//! rt-host (L3): host capabilities exposed to guests (SECURITY.md).
//!
//! - [`EgressGuard`] / [`GuardedEgress`] — SSRF-safe outbound HTTP (ported from the worker);
//! - [`KvFacade`] / [`SqlFacade`] / [`BlobFacade`] — guest-facing storage with stable errors;
//! - [`Calls`] / [`PlatformBindings`] — service-to-service calls and operator bindings;
//! - [`FairBlockingPool`] — per-tenant fair blocking work with CPU charging;
//! - [`HostServices`] / [`HostHandle`] — per-task capability bundle used by both executors.

// Request-path hygiene (security review step 5c): no panicking unwraps outside tests.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]
#![warn(clippy::await_holding_lock, clippy::mem_forget, clippy::undocumented_unsafe_blocks)]

pub mod blocking;
pub mod calls;
pub mod egress;
pub mod facade;
pub mod handle;
pub mod lock;
pub mod rpc;
pub mod schedule;
pub mod self_endpoints;
pub mod sockets;
pub mod ws_client;

pub use blocking::{FairBlockingPool, FairPoolConfig};
pub use calls::{BindingError, CallError, Calls, ConfigBindings, EnqueueOpts, PlatformBindings, TaskCtx};
pub use egress::{
    is_https_downgrade, is_redirect, redirect_method, ConnectRewrite, EgressBudget, EgressError, EgressGuard,
    EgressGuardConfig, EgressIo, EgressPolicy, EgressUsage, GuardedEgress, Request, Resolver, Response, SystemResolver,
};
pub use facade::{BlobFacade, FacadeLimits, HostError, KvFacade, SqlFacade, StorageLeft};
pub use handle::{
    HostDeps, HostHandle, HostServices, HostTaskConfig, TenantPolicy, MAX_CONCURRENT_CALLS, MAX_GATED_SOCKET_OPS,
    MAX_SOCKET_SEND_BYTES, MAX_STORAGE_WRITES,
};
pub use lock::{LockHold, LockTable, LOCK_KEYS_PER_SERVICE, LOCK_WAITERS_PER_KEY};
pub use rpc::RpcError;
pub use rt_obs::{LogRecord, LogSink};
pub use self_endpoints::{self_endpoint_hosts, SELF_DENYLIST_REFRESH};
pub use sockets::{AcceptOpts, Accepted, NoSockets, ObjectPage, SocketHost, SocketOp, SocketPage};
pub use ws_client::{ClientEvent, WsClient};
