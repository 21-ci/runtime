//! The integration tests of `rt-build`, one test binary (`cargo test -p rt-build --test it`).
//! Every V8/wasmtime/SQLite-linked test executable costs tens of MB, so the files are modules
//! of this one target instead of one target each. Select a file with a name filter:
//! `cargo test -p rt-build --test it <module>::`.

#[path = "../common/mod.rs"]
mod common;

mod archive_entries;
mod build_stages;
mod bundle_build;
mod fetch_budget;
mod fetch_lock;
mod limits_failures;
mod locked_media_type;
mod resolution;
mod validation;
mod wasm_build;
