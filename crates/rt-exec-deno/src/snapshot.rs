//! The base V8 startup snapshot, built by `build.rs` and embedded in the binary.
//!
//! It holds the web platform (`deno_webidl`/`deno_web`/`deno_fetch`/`deno_crypto`) plus
//! `js/bootstrap.js`, no tenant data. Their JS sources live in the cargo registry, so nothing but
//! this blob (and the residual lazy tables below) may be used at runtime: the Docker image only
//! ships the `rt` binary.

use std::sync::OnceLock;

use deno_core::{JsRuntime, RuntimeOptions};

use crate::exts::guest_extensions;

// `EXTENSIONS`, `OPS`, `RESIDUAL_LAZY_JS`, `RESIDUAL_LAZY_ESM` (see build.rs).
include!(concat!(env!("OUT_DIR"), "/rt_snapshot_tables.rs"));

static SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/RT_SNAPSHOT.bin"));

/// The extension set recorded in the snapshot must be the one an isolate is created with: same
/// extensions, same op names, order, sync/async kind and argument count (the JS wrapper of an
/// async op is generated from it). `build.rs` keeps stub ops for `rt_ext`,
/// so a drift (an op added to `exts.rs` but not to `build.rs`) would otherwise surface as a
/// mis-bound op at some later call.
fn verify() -> Result<(), String> {
    static RESULT: OnceLock<Result<(), String>> = OnceLock::new();
    RESULT
        .get_or_init(|| {
            let exts = guest_extensions();
            let names: Vec<&str> = exts.iter().map(|e| e.name).collect();
            check_extensions(&names, EXTENSIONS)?;
            let live: Vec<(&str, bool, u8)> = exts
                .iter()
                .flat_map(|e| e.ops.iter().map(|d| (d.name, d.is_async, d.arg_count)))
                .collect();
            check_ops(&live, OPS)?;
            Ok(())
        })
        .clone()
}

fn check_extensions(runtime: &[&str], snapshot: &[&str]) -> Result<(), String> {
    if runtime != snapshot {
        return Err(format!(
            "snapshot extensions {snapshot:?} differ from the runtime's {runtime:?}"
        ));
    }
    Ok(())
}

/// The ops of the runtime against the stubs the snapshot was made with, first difference named.
fn check_ops(live: &[(&str, bool, u8)], recorded: &[(&str, bool, u8)]) -> Result<(), String> {
    if let Some(i) = (0..live.len().max(recorded.len())).find(|&i| live.get(i) != recorded.get(i)) {
        return Err(format!(
            "base snapshot is out of sync with the ops: op #{i} is {:?} at runtime but {:?} in the snapshot; \
             update the stub ops in crates/rt-exec-deno/build.rs",
            live.get(i),
            recorded.get(i)
        ));
    }
    Ok(())
}

/// `RuntimeOptions` of every guest isolate: the guest extensions on top of the embedded snapshot.
/// Callers add their module loader and V8 create params, create the runtime and [`seal`] it.
pub(crate) fn base_runtime_options() -> Result<RuntimeOptions, String> {
    verify()?;
    Ok(RuntimeOptions {
        extensions: guest_extensions(),
        startup_snapshot: Some(SNAPSHOT),
        // The snapshot already holds the op bindings (their native side is resolved through the
        // external reference table by position, checked in `verify`); binding them again on every
        // isolate would cost ~1k V8 functions each time.
        skip_op_registration: true,
        residual_lazy_js_sources: RESIDUAL_LAZY_JS,
        residual_lazy_esm_sources: RESIDUAL_LAZY_ESM,
        ..Default::default()
    })
}

/// Runs `__rt_seal()` (see `js/bootstrap.js`): `Deno` must still hold `core` while deno_core
/// restores the snapshot, so it is reduced to the env shim right after, before any guest code.
pub(crate) fn seal(runtime: &mut JsRuntime) -> Result<(), String> {
    runtime
        .execute_script("[rt:seal]", "globalThis.__rt_seal()")
        .map(drop)
        .map_err(|e| format!("cannot seal the isolate: {e}"))
}

// Per-service snapshots (B4.3 / D-A5) are not implemented. A probe layered a second snapshot on the
// base one (deno_core 0.412 / v8 150.4, 2026-10-06): creating the layer works, but restoring it
// crashes the process with SIGSEGV inside `JsRuntime::try_new` even when the layer adds nothing,
// while the same restore path with the base snapshot works.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_extension_list_that_differs_from_the_snapshots_is_refused_with_both_lists() {
        assert!(check_extensions(&["a", "b"], &["a", "b"]).is_ok());
        let err = check_extensions(&["a"], &["a", "b"]).expect_err("the runtime lacks `b`");
        assert!(err.contains(r#"["a", "b"]"#) && err.contains(r#"["a"]"#), "{err}");
    }

    #[test]
    fn the_first_op_that_differs_from_the_snapshots_stubs_is_named() {
        let a = [("op_a", false, 1), ("op_b", true, 2)];
        assert!(check_ops(&a, &a).is_ok());
        let renamed = [("op_a", false, 1), ("op_c", true, 2)];
        let err = check_ops(&renamed, &a).expect_err("op #1 differs");
        assert!(
            err.contains("op #1") && err.contains("op_c") && err.contains("op_b") && err.contains("build.rs"),
            "{err}"
        );
        let err = check_ops(&a[..1], &a).expect_err("the runtime has one op fewer");
        assert!(err.contains("op #1") && err.contains("None"), "{err}");
    }
}
