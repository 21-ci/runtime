//! The integration tests of `rt-exec-wasm`, one test binary (`cargo test -p rt-exec-wasm --test it`).
//! Every V8/wasmtime/SQLite-linked test executable costs tens of MB, so the files are modules
//! of this one target instead of one target each. Select a file with a name filter:
//! `cargo test -p rt-exec-wasm --test it <module>::`.

#[path = "../common/mod.rs"]
mod common;

mod wsm_calls;
mod wsm_deploy;
mod wsm_detached;
mod wsm_edge;
mod wsm_exec;
mod wsm_host_ops;
mod wsm_kv_cas;
mod wsm_kv_incr_client;
mod wsm_loader;
mod wsm_lock;
mod wsm_log_fields;
mod wsm_perf;
mod wsm_platform;
mod wsm_pool;
mod wsm_reuse;
mod wsm_rpc;
mod wsm_schedule;
mod wsm_secrets;
mod wsm_slots;
mod wsm_sockets;
mod wsm_std;
mod wsm_stream;
mod wsm_warm;
