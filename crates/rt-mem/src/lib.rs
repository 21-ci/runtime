//! rt-mem (L1): the unified memory governor.
//!
//! All memory that scales with load is reserved through **one governor**, whichever runtime asks:
//! V8 near-heap-limit callback, wasmtime `ResourceLimiter`, cache inserts (and IO buffer pools,
//! which no I/O path uses yet, see [`pools`]). Reservations are RAII: dropping one (including
//! during panic unwind) releases its bytes.
//!
//! Pressure ladder: the scheduler's sysmon requests purges ([`MemoryGovernor::request_purge`],
//! `Light` at `High`, `Full` at `Critical`, at most once per `memory.purge_min_interval`), puts the
//! node in `shed` mode at `Critical` and leaves it below `High`, shrinks per-service concurrency
//! limits and warm targets (rt-sched `autoscale`). Serving a purge is up to whoever owns a
//! thread-local heap or pool: they compare [`MemoryGovernor::purge_request`]'s epoch with the last
//! one they served (no executor serves purges yet).

pub mod budget;
pub mod governor;
pub mod pools;
pub mod reservation;
pub mod types;

pub use budget::{
    budget_from_limit, detect_budget, detect_budget_in, parse_cgroup_limit, Budget, BudgetSource,
    BUDGET_FRACTION_PERCENT,
};
pub use governor::{Governor, GovernorConfig, MemoryGovernor, PressureWatch};
pub use pools::{IoBufferPools, PooledBuf, SizeClass};
pub use reservation::{Reservation, ReservationBackend};
pub use types::{MemCategory, MemError, MemStats, Owner, Pressure, PurgeLevel, PurgeRequest};
