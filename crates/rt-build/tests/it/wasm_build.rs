//! WASM builds through the `ComponentPrecompiler` trait (BLD-14), the shared WASM import policy
//! (contracts C-7) and the content sniffing helpers.
//!
//! `rt-build` never links wasmtime: the precompiler is injected (`FakePrecompiler` here; the real
//! `rt-exec-deno`-independent one lives in `rt-exec-wasm`). The component under test is either a
//! compiled fixture from `fixtures/wasm/out/` (built in CI) or the embedded tiny component.

use crate::common;

use std::time::Duration;

use common::wasm_fixtures as wasm;
use common::*;
use rt_build::wasm_policy::{
    is_import_allowed, is_incoming_handler, is_wasi_02, renamed_import, split_interface, REQUIRED_EXPORT,
    STD_HOST_PACKAGE,
};
use rt_build::{is_wasm_component, is_wasm_core_module, media_type_of, Upload};
use rt_types::{BuildFailReason, BuildFailure, RuntimeKind};

fn wasm_upload(bytes: &[u8]) -> Upload {
    upload("application/wasm", bytes.to_vec())
}

async fn build_wasm(
    bytes: &[u8],
    precompiler: Option<std::sync::Arc<dyn rt_build::ComponentPrecompiler>>,
) -> Result<rt_build::BuiltArtifact, BuildFailure> {
    let builder = default_builder();
    let mut cx = offline_cx(MemArtifacts::new());
    cx.precompiler = precompiler;
    validate_and_build(
        &builder,
        &mut cx,
        &wasm_upload(bytes),
        RuntimeKind::Wasm,
        None,
        Duration::from_secs(120),
    )
    .await
}

/// A compiled hello component from `fixtures/wasm/out/` when CI built it.
fn fixture_component() -> Option<Vec<u8>> {
    let dir = fixtures_root().join("wasm/out");
    for name in ["hello.wasm", "hello.component.wasm"] {
        if let Ok(b) = std::fs::read(dir.join(name)) {
            return Some(b);
        }
    }
    None
}

// ---------------------------------------------------------------------------------------------
// BLD-14
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_bld_14_wasm_build_precompiles_through_the_injected_trait() {
    let fake = FakePrecompiler::new(0xFA57);
    let art = build_wasm(wasm::OK_COMPONENT, Some(fake.clone()))
        .await
        .expect("wasm build");

    assert_eq!(fake.calls(), 1, "one precompile per build");
    assert_eq!(
        fake.inputs.lock().expect("lock")[0],
        wasm::OK_COMPONENT,
        "the uploaded component is what gets compiled"
    );

    let cwasm = built_file(&art, "component.cwasm").expect("component.cwasm is the T2 file");
    assert_eq!(
        cwasm.bytes.as_ref(),
        FakePrecompiler::output_for(wasm::OK_COMPONENT).as_slice()
    );

    // WASM entries have no bundle, no lock and no code cache.
    assert!(built_file(&art, "bundle").is_none());
    assert!(built_file(&art, "code_cache.bin").is_none());
    assert!(art.lock.is_none());
    assert!(art.bundle_digest.is_none());
}

#[tokio::test]
async fn ac_bld_14_build_uses_the_requested_key_and_the_trait_output_per_engine() {
    // A different engine fingerprint compiles to a different artifact: the key carries the fp and
    // the bytes come from whichever precompiler the node injected.
    let a = FakePrecompiler::new(1);
    let b = FakePrecompiler::new(2);
    let builder = default_builder();
    let up = wasm_upload(wasm::OK_COMPONENT);

    for (fp, p) in [(1u64, a.clone()), (2u64, b.clone())] {
        let mut cx = offline_cx(MemArtifacts::new());
        cx.precompiler = Some(p);
        let digest = cx.artifacts.put(up.body.clone()).await.expect("put");
        let key = rt_types::PreparedKey {
            digest,
            kind: RuntimeKind::Wasm,
            engine_fp: fp,
        };
        let req = rt_build::BuildRequest {
            key,
            kind: RuntimeKind::Wasm,
            source_digest: digest,
            lock: None,
            timeout: Duration::from_secs(120),
        };
        let art = rt_build::Builder::build(&builder, &req, &mut cx).await.expect("build");
        assert_eq!(art.key, key);
        assert_eq!(art.key.engine_fp, fp);
    }
    assert_eq!(a.calls(), 1);
    assert_eq!(b.calls(), 1);
    assert_eq!(rt_build::ComponentPrecompiler::engine_fp(a.as_ref()), 1);
}

#[tokio::test]
async fn bld_14_component_with_linkable_imports_builds() {
    let fake = FakePrecompiler::new(1);
    build_wasm(wasm::OK_COMPONENT_WITH_IMPORTS, Some(fake.clone()))
        .await
        .expect("build");
    assert_eq!(fake.calls(), 1);
}

#[tokio::test]
async fn bld_14_precompiler_failure_is_the_build_failure() {
    let fake = FakePrecompiler::failing(1, BuildFailure::new(BuildFailReason::Internal, "cranelift said no"));
    let failure = build_wasm(wasm::OK_COMPONENT, Some(fake))
        .await
        .expect_err("precompile fails");
    assert_eq!(failure.reason, BuildFailReason::Internal);
    assert!(failure.message.contains("cranelift"), "{}", failure.message);
}

#[tokio::test]
async fn bld_14_wasm_build_without_a_precompiler_is_an_internal_failure() {
    let failure = build_wasm(wasm::OK_COMPONENT, None)
        .await
        .expect_err("no ComponentPrecompiler injected");
    assert_eq!(failure.reason, BuildFailReason::Internal);
}

#[tokio::test]
async fn bld_14_builds_are_deterministic_for_a_deterministic_precompiler() {
    let a = build_wasm(wasm::OK_COMPONENT, Some(FakePrecompiler::new(1)))
        .await
        .expect("a");
    let b = build_wasm(wasm::OK_COMPONENT, Some(FakePrecompiler::new(1)))
        .await
        .expect("b");
    assert_eq!(built_file(&a, "component.cwasm"), built_file(&b, "component.cwasm"));
}

#[tokio::test]
async fn bld_14_fixture_component_from_ci_precompiles_when_available() {
    let Some(bytes) = fixture_component() else {
        eprintln!("fixtures/wasm/out/hello.wasm not built (CI builds the Rust guests); skipping");
        return;
    };
    let fake = FakePrecompiler::new(1);
    let art = build_wasm(&bytes, Some(fake.clone()))
        .await
        .expect("fixture component builds");
    assert_eq!(fake.calls(), 1);
    assert_eq!(
        built_file(&art, "component.cwasm").expect("cwasm").bytes.as_ref(),
        FakePrecompiler::output_for(&bytes).as_slice()
    );
}

// ---------------------------------------------------------------------------------------------
// wasm_policy (contracts C-7)
// ---------------------------------------------------------------------------------------------

#[test]
fn policy_splits_interface_names() {
    assert_eq!(
        split_interface("wasi:http/types@0.2.6"),
        Some(("wasi:http", "types", Some("0.2.6")))
    );
    assert_eq!(split_interface("std:host/kv"), Some(("std:host", "kv", None)));
    assert_eq!(split_interface("nonsense"), None);
}

#[test]
fn policy_accepts_any_wasi_0_2_minor_only() {
    assert!(is_wasi_02(Some("0.2")));
    assert!(is_wasi_02(Some("0.2.0")));
    assert!(is_wasi_02(Some("0.2.9")));
    assert!(!is_wasi_02(Some("0.1.0")));
    assert!(!is_wasi_02(Some("0.3.0")));
    assert!(!is_wasi_02(Some("1.0.0")));
    assert!(!is_wasi_02(None));
}

#[test]
fn policy_import_allowlist() {
    for ok in [
        "wasi:io/streams@0.2.6",
        "wasi:clocks/monotonic-clock@0.2.0",
        "wasi:clocks/wall-clock@0.2.3",
        "wasi:random/random@0.2.1",
        "wasi:cli/environment@0.2.3",
        "wasi:cli/stdout@0.2.3",
        "wasi:http/outgoing-handler@0.2.6",
        "wasi:http/types@0.2.0",
        "wasi:filesystem/types@0.2.3", // linked with no preopens
        "wasi:sockets/tcp@0.2.3",      // linked with every address denied
        "std:host/kv@0.1.0",
        "std:host/sql@0.1.0",
        "std:host/blob@0.1.0",
        "std:host/queue@0.1.0",
        "std:host/call@0.1.0",
        "std:host/log@0.1.0",
    ] {
        assert!(is_import_allowed(ok), "{ok} must be linkable");
    }
    for bad in [
        "wasi:clocks/monotonic-clock@0.1.0",
        "wasi:http/types@0.3.0",
        "wasi:http/types",
        "wasi:nn/graph@0.2.0",
        "wasi:threads/thread-spawn@0.1.0",
        "evil:pkg/thing@0.1.0",
        "env",
        "",
    ] {
        assert!(!is_import_allowed(bad), "{bad} must be rejected");
    }
    assert_eq!(STD_HOST_PACKAGE, "std:host");
    assert_eq!(
        renamed_import("rt:host/kv@0.1.0").as_deref(),
        Some("`rt:host/kv@0.1.0` was renamed to `std:host/kv@0.1.0`")
    );
    assert_eq!(renamed_import("std:host/kv@0.1.0"), None);
}

#[test]
fn policy_required_export() {
    assert_eq!(REQUIRED_EXPORT, "wasi:http/incoming-handler");
    assert!(is_incoming_handler("wasi:http/incoming-handler@0.2.0"));
    assert!(is_incoming_handler("wasi:http/incoming-handler@0.2.6"));
    assert!(!is_incoming_handler("wasi:http/incoming-handler@0.3.0"));
    assert!(!is_incoming_handler("wasi:http/outgoing-handler@0.2.0"));
    assert!(!is_incoming_handler("wasi:cli/run@0.2.0"));
    assert!(!is_incoming_handler("incoming-handler"));
}

// ---------------------------------------------------------------------------------------------
// sniffing
// ---------------------------------------------------------------------------------------------

#[test]
fn sniff_distinguishes_components_core_modules_and_text() {
    assert!(is_wasm_component(wasm::OK_COMPONENT));
    assert!(!is_wasm_core_module(wasm::OK_COMPONENT));
    assert!(is_wasm_core_module(wasm::CORE_MODULE));
    assert!(!is_wasm_component(wasm::CORE_MODULE));
    assert!(!is_wasm_component(b"export default 1;"));
    assert!(!is_wasm_core_module(b"export default 1;"));
    assert!(!is_wasm_component(&[]));
}

#[test]
fn sniff_media_types_follow_deno_ast() {
    use deno_ast::MediaType;
    assert_eq!(media_type_of("main.ts"), MediaType::TypeScript);
    assert_eq!(media_type_of("lib/a.tsx"), MediaType::Tsx);
    assert_eq!(media_type_of("a.js"), MediaType::JavaScript);
    assert_eq!(media_type_of("a.mjs"), MediaType::Mjs);
    assert_eq!(media_type_of("a.jsx"), MediaType::Jsx);
    assert_eq!(media_type_of("data.json"), MediaType::Json);
}
