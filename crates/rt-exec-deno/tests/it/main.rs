//! The integration tests of `rt-exec-deno`, one test binary (`cargo test -p rt-exec-deno --test it`).
//! Every V8/wasmtime/SQLite-linked test executable costs tens of MB, so the files are modules
//! of this one target instead of one target each. Select a file with a name filter:
//! `cargo test -p rt-exec-deno --test it <module>::`.
//!
//! `tests/v8_threads.rs` stays its own target (= own process): V8's platform is initialized once per
//! process and that test asserts the exact V8 thread count of its process.

#[path = "../common/mod.rs"]
mod common;

mod clock;
mod code_cache_pool;
mod concurrent_isolates;
mod edge_durable;
mod edge_guest;
mod edge_warm_ws;
mod guest_basics;
mod host_ops;
mod idle_hooks;
mod keep_alive;
mod limits_streaming;
mod loader_glue;
mod perf;
mod sandbox;
mod sdk_kv_cas_expire;
mod sdk_kv_incr_client;
mod sdk_lock;
mod sdk_log_fields;
mod sdk_pubsub;
mod sdk_schedule;
mod sdk_secrets;
mod sdk_sqlite;
mod sdk_stream;
mod task_inputs;
mod warm_floor;
mod warm_reuse;
mod web_apis;
mod ws_client;
