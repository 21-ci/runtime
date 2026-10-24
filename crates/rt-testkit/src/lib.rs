//! rt-testkit: test support for every crate (never a dependency of production code).
//!
//! - [`VirtualClock`] — deterministic time (`Clock` + `SimClock`);
//! - [`mock`] — `MockExecutor` + script DSL (`cpu`, `io`, `yield_now`, `alloc`, `call`, `panic`,
//!   `loop_forever`), `MockPreparedSource`;
//! - [`SimRuntime`] — deterministic single-thread scheduler simulation, seeded;
//! - [`TestGovernor`] — a working budgeted `MemoryGovernor`;
//! - [`NodeHarness`] — start a node in-process on random ports with a temp data dir (boot is
//!   injected so this crate does not link V8/wasmtime);
//! - [`MockCdn`] — local HTTP server serving fixture modules;
//! - [`capture_tracing`] — the `tracing` events of the current thread, as text, to assert log lines;
//! - [`fixtures`] / [`spec`] — guest sources, deploy payloads, deterministic IDs and `TaskSpec`
//!   builders.

pub mod cdn;
pub mod clock;
pub mod fixtures;
pub mod governor;
pub mod mock;
pub mod node;
pub mod sim;
pub mod spec;
pub mod tracing_capture;

pub use cdn::MockCdn;
pub use clock::VirtualClock;
pub use governor::TestGovernor;
pub use mock::{
    alloc, call, cpu, crash, io, loop_forever, panic, yield_now, CallHook, MockBuildExecutor, MockDenoExecutor,
    MockEvent, MockExecutor, MockPreparedSource, MockWasmExecutor, Script, ScriptBook, Step,
};
pub use node::{Booted, NodeAddrs, NodeHarness, NodeSetup};
pub use sim::{SimConfig, SimRuntime, SimTask};
pub use spec::{ids, service_ref, SpecBuilder};
pub use tracing_capture::{capture_tracing, capture_tracing_globally, CapturedLogs};
