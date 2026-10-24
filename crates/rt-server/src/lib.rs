//! rt-server (L6): the `rt` binary's library: CLI, node wiring, the in-process control plane.
//!
//! The binary (`src/main.rs`) is a thin shell over this library so integration tests can start a
//! node in-process (`rt_server::start`). One process in the MVP (`--role=all`); the executors
//! (`rt-exec-deno`, `rt-exec-wasm`) are linked here and nowhere else except their own crates.

// Request-path hygiene (security review step 5c): no panicking unwraps outside tests.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]
#![warn(clippy::await_holding_lock, clippy::mem_forget, clippy::undocumented_unsafe_blocks)]

pub mod build_exec;
pub mod cli;
pub mod control_plane;
pub mod policy;
pub mod reseal;
pub mod specs;
pub mod wiring;

pub use cli::{Cli, Command, Role, SecretsCommand};
pub use control_plane::InProcControlPlane;
pub use wiring::{
    start, start_with, MetaLayer, NodeSignal, NodeSignals, RunningServer, ServerAddrs, ServerError, StartFaults,
    StartOptions,
};

/// Engine versions this binary was built with (reported by `GET /v1/node/status`).
pub fn build_info() -> serde_json::Value {
    serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "deno_core": "0.412.0",
        "deno_release": "2.9.7",
        "wasmtime": "44",
        "v8_sandbox": false,
    })
}

/// Fingerprints and sizes of the linked engines. Referencing both executors here keeps V8 and
/// wasmtime linked into the binary (and into this crate's tests), so link size and time regress
/// visibly rather than at the first real use.
pub fn engine_info() -> serde_json::Value {
    let flags: Vec<String> = rt_exec_deno::BASE_V8_FLAGS.iter().map(|s| s.to_string()).collect();
    serde_json::json!({
        "v8_engine_fp": rt_exec_deno::engine_fingerprint(&flags),
        "guest_extensions": rt_exec_deno::guest_extensions().len(),
        "wasm_wit_bytes": rt_exec_wasm::RT_HOST_WIT.len(),
        "wasm_max_pool_slots": rt_exec_wasm::MAX_POOL_SLOTS,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_build_info_names_the_crate_version_and_the_linked_engines() {
        let info = build_info();
        assert_eq!(info["version"], env!("CARGO_PKG_VERSION"));
        assert!(info["deno_core"].is_string() && info["wasmtime"].is_string());
        assert_eq!(info["v8_sandbox"], false);
    }

    #[test]
    fn the_engine_info_reports_both_executors() {
        let info = engine_info();
        assert!(info["v8_engine_fp"].is_u64());
        assert!(info["guest_extensions"].as_u64().is_some_and(|n| n > 0));
        assert!(info["wasm_wit_bytes"].as_u64().is_some_and(|n| n > 0));
        assert!(info["wasm_max_pool_slots"].as_u64().is_some_and(|n| n > 0));
    }
}
