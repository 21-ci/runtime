//! rt-exec-wasm (L4): the WASM executor.
//!
//! `wasmtime` 44 (same as the old worker): one shared `Engine` (component model, epoch
//! interruption, optional fuel, pooling allocator sized from the memory budget), a fresh `Store`
//! per task (or a warm / reused instance of the same service), WASI P2 with **no** filesystem or
//! sockets, `wasi:http` through the egress guard (responses streamed), and the
//! `std:host/{kv,sql,blob,log,queue,call}` imports described by `/wit/rt-host.wit`.
//!
//! - [`WasmExecutor`] implements `rt_sched::Executor` (one per Machine), including the warm pool
//!   that follows `rt_sched::WarmTargets`;
//! - [`WasmPrepared`] is the T1 value; [`WasmLoader`] feeds `rt-cache`;
//! - [`WasmPrecompiler`] implements `rt_build::ComponentPrecompiler`.

pub mod clock;
pub mod engine;
pub mod executor;
pub mod host_wit;
pub mod host_ws;
pub mod interrupt;
pub mod loader;
pub mod prepared;
pub mod state;
pub mod stdio;
pub(crate) mod warm;

pub use clock::FrozenClock;
pub use engine::{
    build_engine, engine_fingerprint, publish_pool_slots, EpochTicker, PoolSizing, WasmEngineConfig, EPOCH_TICK,
    MAX_POOL_SLOTS,
};
pub use executor::WasmExecutor;
pub use interrupt::WasmInterrupter;
pub use loader::{WasmLoader, WasmPrecompiler, T2_CWASM};
pub use prepared::{build_proxy_pre, WasmPrepared};
pub use state::{GovernorLimiter, GuardedHttpHooks, WasmState};
pub use stdio::GuestLogs;
pub use warm::IdleSlots;

/// The `std:host` WIT package (kv, sql, blob, log, queue, call) as shipped in `/wit/rt-host.wit`.
pub const RT_HOST_WIT: &str = include_str!("../../../wit/rt-host.wit");
