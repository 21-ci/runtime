//! WSM-08 (deploy-time rejection of non-components / components without `incoming-handler`) and
//! conformance of every built fixture with the import policy (contracts C-7).
//!
//! Validation lives in `rt-build` (`DefaultBuilder`, called by `rt-api` through
//! `UploadValidator`); the executor crate's tests own this AC because the fixtures are its guests.

use crate::common;

use bytes::Bytes;
use common::fixture_bytes;
use rt_build::{wasm_policy, Builder, DefaultBuilder, Upload, UploadLimits};
use rt_types::{RuntimeKind, UploadValidator, ValidatedUpload, ValidationError};

/// Fixtures that must deploy.
const VALID: &[&str] = &[
    "hello",
    "counter",
    "spin",
    "grow",
    "fs_socket",
    "stdout_log",
    "clock",
    "kv",
    "calls",
    "stream",
    "platform",
    "std_cli",
];

fn builder() -> DefaultBuilder {
    DefaultBuilder::new(UploadLimits::default())
}

fn validate(bytes: Vec<u8>) -> Result<ValidatedUpload, ValidationError> {
    UploadValidator::validate(&builder(), RuntimeKind::Wasm, "application/wasm", &Bytes::from(bytes))
}

fn invalid_artifact_reason(name: &str, bytes: Vec<u8>) -> String {
    match validate(bytes) {
        Err(ValidationError::InvalidArtifact(reason)) => reason,
        other => panic!("{name}: expected 422 invalid_artifact, got {other:?}"),
    }
}

#[test]
fn ac_wsm_08_core_module_is_rejected_with_a_reason() {
    let reason = invalid_artifact_reason("invalid-core", fixture_bytes("invalid-core"));
    assert!(!reason.trim().is_empty(), "the 422 must say why");
    assert!(
        reason.to_lowercase().contains("component"),
        "reason should say a component is required: {reason}"
    );
}

#[test]
fn ac_wsm_08_component_without_incoming_handler_is_rejected_with_a_reason() {
    let reason = invalid_artifact_reason("no-handler", fixture_bytes("no-handler"));
    assert!(
        reason.contains("incoming-handler"),
        "reason should name the missing wasi:http/incoming-handler export: {reason}"
    );
}

#[test]
fn ac_wsm_08_garbage_and_empty_uploads_are_rejected() {
    for (name, bytes) in [
        ("empty", Vec::new()),
        ("text", b"not a wasm file".to_vec()),
        ("truncated header", b"\0asm".to_vec()),
    ] {
        let reason = invalid_artifact_reason(name, bytes);
        assert!(!reason.trim().is_empty(), "{name}: the 422 must say why");
    }
}

#[test]
fn ac_wsm_08_valid_components_are_accepted() {
    for name in VALID {
        let bytes = fixture_bytes(name);
        let size = bytes.len() as u64;
        let ok = validate(bytes).unwrap_or_else(|e| panic!("fixture `{name}` must deploy: {e:?}"));
        assert_eq!(ok.size, size, "{name}");
    }
}

#[test]
fn ac_wsm_08_builder_reports_the_static_facts_of_an_upload() {
    let b = builder();
    let upload = |name: &str| Upload {
        content_type: "application/wasm".into(),
        body: Bytes::from(fixture_bytes(name)),
    };

    let core = Builder::validate(&b, RuntimeKind::Wasm, &upload("invalid-core"));
    assert!(core.is_err(), "a core module must not validate: {core:?}");
    let empty = Builder::validate(&b, RuntimeKind::Wasm, &upload("no-handler"));
    assert!(
        empty.is_err(),
        "a component without incoming-handler must not validate: {empty:?}"
    );

    let hello = Builder::validate(&b, RuntimeKind::Wasm, &upload("hello")).expect("hello validates");
    let info = hello.wasm.expect("wasm info for a wasm upload");
    assert!(info.is_component);
    assert!(info.exports_incoming_handler);
}

/// Contracts C-7: guests may import `wasi:io|clocks|random|cli|http|filesystem|sockets` (any
/// `0.2.x`) and `std:host/*`; they must export `wasi:http/incoming-handler`.
#[test]
fn wsm_every_fixture_stays_inside_the_c7_import_policy() {
    let b = builder();
    for name in VALID {
        let upload = Upload {
            content_type: "application/wasm".into(),
            body: Bytes::from(fixture_bytes(name)),
        };
        let v = Builder::validate(&b, RuntimeKind::Wasm, &upload).unwrap_or_else(|d| panic!("{name}: {d:?}"));
        let info = v.wasm.unwrap_or_else(|| panic!("{name}: no wasm info"));
        assert!(info.is_component && info.exports_incoming_handler, "{name}: {info:?}");
        assert!(!info.imports.is_empty(), "{name}: a std guest imports wasi:io at least");
        for import in &info.imports {
            assert!(
                wasm_policy::is_import_allowed(import),
                "{name}: import `{import}` is outside the C-7 policy"
            );
        }
    }
}

#[test]
fn wsm_kv_fixture_imports_rt_host_kv_and_nothing_else_from_rt_host() {
    let upload = Upload {
        content_type: "application/wasm".into(),
        body: Bytes::from(fixture_bytes("kv")),
    };
    let v = Builder::validate(&builder(), RuntimeKind::Wasm, &upload).expect("kv validates");
    let rt_host: Vec<String> = v
        .wasm
        .expect("wasm info")
        .imports
        .into_iter()
        .filter(|i| i.starts_with("std:host/"))
        .collect();
    assert_eq!(rt_host.len(), 1, "{rt_host:?}");
    assert!(rt_host[0].starts_with("std:host/kv@"), "{rt_host:?}");
}

#[test]
fn wsm_import_policy_accepts_any_wasi_02_minor_and_rejects_the_rest() {
    // the fixtures are built against wasi 0.2.12 while the host links an older minor: any 0.2.x is fine
    for ok in [
        "wasi:http/types@0.2.0",
        "wasi:http/types@0.2.12",
        "wasi:cli/stdout@0.2.6",
        "wasi:io/streams@0.2.3",
        "std:host/kv@0.1.0",
    ] {
        assert!(wasm_policy::is_import_allowed(ok), "{ok}");
    }
    for bad in [
        "wasi:http/types@0.3.0",
        "wasi:keyvalue/store@0.2.0",
        "wasi:nn/graph@0.2.0",
        "env:thing/x@0.2.0",
        "wasi:http/types",
    ] {
        assert!(!wasm_policy::is_import_allowed(bad), "{bad}");
    }
    assert!(wasm_policy::is_incoming_handler("wasi:http/incoming-handler@0.2.12"));
    assert!(wasm_policy::is_incoming_handler("wasi:http/incoming-handler@0.2.0"));
    assert!(!wasm_policy::is_incoming_handler("wasi:http/outgoing-handler@0.2.0"));
}
