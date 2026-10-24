//! rt-exec-deno (L4): the Deno executor.
//!
//! `deno_core` + `deno_webidl`/`deno_web`/`deno_fetch`/`deno_crypto` from Deno v2.9.7, a fresh
//! V8 isolate per task (or, with `deno.reuse_isolates`, an evaluated isolate kept per service
//! version, `warm.rs`), a bundle-only module loader, and `std:*` modules backed by `rt-host`.
//!
//! - [`DenoExecutor`] implements `rt_sched::Executor` (one per Machine, `!Send`);
//! - [`DenoPrepared`] is the T1 value; [`DenoLoader`] feeds `rt-cache`;
//! - [`V8CodeCacheProducer`] implements `rt_build::CodeCacheProducer`;
//! - `/js` (repo root) holds the bootstrap and the `std:*` module sources, embedded via `include_str!`.

pub mod bundle_loader;
pub mod clock;
pub mod config;
pub mod cpu;
pub mod executor;
pub mod exts;
pub mod interrupt;
pub mod isolate;
pub mod js;
pub mod loader;
pub mod memory;
pub mod ops;
pub mod ops_durable;
pub mod ops_events;
pub mod ops_sqlite;
pub mod ops_ws;
pub mod platform;
pub mod prepared;
mod snapshot;
mod v8sql;
pub mod warm;

pub use config::{DenoExecutorConfig, BASE_V8_FLAGS, HEAP_GROW_STEP_MB};
pub use executor::DenoExecutor;
pub use exts::guest_extensions;
pub use interrupt::DenoInterrupter;
pub use loader::{DenoLoader, V8CodeCacheProducer, T2_BUNDLE, T2_CODE_CACHE};
pub use ops::{MAX_RESPONSE_HEADERS, MAX_RESPONSE_HEADER_BYTES};
pub use prepared::{engine_fingerprint, init_v8_once, DenoPrepared};
