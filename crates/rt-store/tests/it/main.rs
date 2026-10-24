//! The integration tests of `rt-store`, one test binary (`cargo test -p rt-store --test it`).
//! Every V8/wasmtime/SQLite-linked test executable costs tens of MB, so the files are modules
//! of this one target instead of one target each. Select a file with a name filter:
//! `cargo test -p rt-store --test it <module>::`.

#[path = "../common/mod.rs"]
mod common;

mod artifact_edges;
mod artifact_gc;
mod artifact_store;
mod blob_store;
mod bundle_digest;
mod data_edges;
mod data_store;
mod delayed_store;
mod durable_edges;
mod faults;
mod fs_full;
mod group_commit;
mod hardening;
mod kv_cas;
mod kv_edge;
mod kv_incr;
mod kv_store;
mod meta_edges;
mod meta_env_hosts;
mod meta_key_scope;
mod meta_projects;
mod meta_sdk;
mod meta_service_wasm;
mod meta_store;
mod queue_edges;
mod queue_store;
mod reader_pool;
mod sql_stmt_cache;
mod sql_store;
mod stream_store;
mod sync_sql;
mod sync_sql_edges;
mod version_gc;
