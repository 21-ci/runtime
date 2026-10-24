//! The build stages' failure and remote-module paths: every fetch error maps to its failure reason,
//! remote JSON and unsupported module types, missing and non-UTF-8 modules, import maps into
//! operator SDKs, the rt.lock check, and the size and syntax guards that validation normally
//! stops first (their inputs are stored directly, the way a build reads them from T3).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rt_build::{
    BuildCx, BuildRequest, Builder, BuiltArtifact, DefaultBuilder, FetchError, Fetched, ModuleFetcher, ModuleKind,
    UploadLimits,
};
use rt_types::{BuildFailReason, BuildFailure, Digest, PreparedKey, RuntimeKind};
use url::Url;

use crate::common::wasm_fixtures::OK_COMPONENT;
use crate::common::{
    build_cx, build_deno, bundle_of, default_builder, json_upload, offline_cx, resolver_cfg, sdk_catalog, ts_upload,
    validate_and_build, zip_bytes, MemArtifacts, TEST_ENGINE_FP,
};

type Reply = Box<dyn Fn(&Url) -> Result<Fetched, FetchError> + Send + Sync>;

/// Serves each URL from `reply`.
struct Scripted {
    reply: Reply,
}

#[async_trait]
impl ModuleFetcher for Scripted {
    async fn fetch(&self, url: &Url, _expected: Option<Digest>) -> Result<Fetched, FetchError> {
        (self.reply)(url)
    }
}

fn scripted(reply: impl Fn(&Url) -> Result<Fetched, FetchError> + Send + Sync + 'static) -> Arc<dyn ModuleFetcher> {
    Arc::new(Scripted { reply: Box::new(reply) })
}

fn served(url: &Url, body: &[u8], content_type: Option<&str>) -> Fetched {
    Fetched {
        url: url.clone(),
        bytes: Bytes::copy_from_slice(body),
        digest: Digest::of(body),
        content_type: content_type.map(str::to_string),
        from_cache: false,
    }
}

const ESM_LIMITS: &[&str] = &["esm.sh"];

fn remote_cx(reply: impl Fn(&Url) -> Result<Fetched, FetchError> + Send + Sync + 'static) -> BuildCx {
    build_cx(
        MemArtifacts::new(),
        scripted(reply),
        resolver_cfg("https://esm.sh", ESM_LIMITS),
    )
}

const REMOTE_IMPORT: &str =
    "import { x } from 'https://esm.sh/dep@1.js'; export default () => new Response(String(x));";

/// Build `stored` as the source of a build of `kind` with `builder`, skipping validation: the
/// build reads whatever is in T3.
async fn build_stored(
    builder: &DefaultBuilder,
    cx: &mut BuildCx,
    stored: &[u8],
    kind: RuntimeKind,
) -> Result<BuiltArtifact, BuildFailure> {
    let digest = cx
        .artifacts
        .put(Bytes::copy_from_slice(stored))
        .await
        .expect("store source in T3");
    let req = BuildRequest {
        key: PreparedKey {
            digest,
            kind,
            engine_fp: TEST_ENGINE_FP,
        },
        kind,
        source_digest: digest,
        lock: None,
        timeout: Duration::from_secs(60),
    };
    builder.build(&req, cx).await
}

// ---------------------------------------------------------------------------------------------
// Fetch errors
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn every_fetch_error_maps_to_its_build_failure() {
    let dep = Url::parse("https://esm.sh/dep@1.js").unwrap();
    let cases: Vec<(FetchError, BuildFailReason)> = vec![
        (FetchError::Denied(dep.to_string()), BuildFailReason::Fetch),
        (FetchError::NotFound(dep.to_string()), BuildFailReason::Fetch),
        (FetchError::Timeout(dep.to_string()), BuildFailReason::Fetch),
        (FetchError::TooLarge, BuildFailReason::TooLarge),
        (
            FetchError::DigestMismatch {
                url: dep.to_string(),
                expected: Digest::of(b"a"),
                actual: Digest::of(b"b"),
            },
            BuildFailReason::LockMismatch,
        ),
        (FetchError::Upstream(dep.to_string()), BuildFailReason::Fetch),
    ];
    for (err, reason) in cases {
        let mut cx = remote_cx(move |_| Err(err.clone()));
        let failure = build_deno(&default_builder(), &mut cx, &ts_upload(REMOTE_IMPORT))
            .await
            .expect_err("a failed fetch fails the build");
        assert_eq!(failure.reason, reason, "{failure:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// Remote modules by media type
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_remote_json_module_is_bundled_as_json() {
    let upload = ts_upload(
        "import data from 'https://esm.sh/data.json'; export default () => new Response(JSON.stringify(data));",
    );
    let mut cx = remote_cx(|u| Ok(served(u, br#"{"a": 1}"#, Some("application/json"))));
    let built = build_deno(&default_builder(), &mut cx, &upload).await.expect("builds");
    let bundle = bundle_of(&built);
    assert!(
        bundle.modules.iter().any(|m| matches!(m.kind, ModuleKind::Json)),
        "a Json module in the bundle"
    );
}

#[tokio::test]
async fn an_invalid_remote_json_module_fails_with_a_syntax_error() {
    let upload = ts_upload(
        "import data from 'https://esm.sh/data.json'; export default () => new Response(JSON.stringify(data));",
    );
    let mut cx = remote_cx(|u| Ok(served(u, b"{", Some("application/json"))));
    let failure = build_deno(&default_builder(), &mut cx, &upload)
        .await
        .expect_err("bad JSON");
    assert_eq!(failure.reason, BuildFailReason::Syntax, "{failure:?}");
    assert!(failure.message.contains("invalid JSON"), "{}", failure.message);
}

#[tokio::test]
async fn a_remote_module_of_an_unsupported_type_is_refused() {
    let upload = ts_upload("import s from 'https://esm.sh/style.css'; export default () => new Response(String(s));");
    let mut cx = remote_cx(|u| Ok(served(u, b"a { color: red }", Some("text/css"))));
    let failure = build_deno(&default_builder(), &mut cx, &upload)
        .await
        .expect_err("css is not a module");
    assert_eq!(failure.reason, BuildFailReason::Resolve, "{failure:?}");
    assert!(
        failure.message.contains("unsupported module type"),
        "{}",
        failure.message
    );
}

#[tokio::test]
async fn non_utf8_remote_bytes_fail_with_a_syntax_error() {
    let mut cx = remote_cx(|u| Ok(served(u, &[0xff, 0xfe], Some("application/javascript"))));
    let failure = build_deno(&default_builder(), &mut cx, &ts_upload(REMOTE_IMPORT))
        .await
        .expect_err("not UTF-8");
    assert_eq!(failure.reason, BuildFailReason::Syntax, "{failure:?}");
    assert!(failure.message.contains("not valid UTF-8"), "{}", failure.message);
}

// ---------------------------------------------------------------------------------------------
// rt.lock
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_rebuild_whose_remote_bytes_changed_fails_the_lock_check() {
    let mut first_cx = remote_cx(|u| Ok(served(u, b"export const x = 1;", Some("application/javascript"))));
    let first = build_deno(&default_builder(), &mut first_cx, &ts_upload(REMOTE_IMPORT))
        .await
        .expect("first build");
    let lock = first.lock.clone();
    assert!(lock.is_some(), "the first build records rt.lock");

    // The fetcher ignores the expected digest, so the bytes it returns reach the lock check.
    let mut again = remote_cx(|u| Ok(served(u, b"export const x = 2;", Some("application/javascript"))));
    let failure = validate_and_build(
        &default_builder(),
        &mut again,
        &ts_upload(REMOTE_IMPORT),
        RuntimeKind::Deno,
        lock,
        Duration::from_secs(60),
    )
    .await
    .expect_err("changed bytes");
    assert_eq!(failure.reason, BuildFailReason::LockMismatch, "{failure:?}");
}

// ---------------------------------------------------------------------------------------------
// Local modules
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_relative_import_of_a_missing_file_fails_at_build_time() {
    // Deploy accepts the import (E2E-05: the file may be added later); the build cannot.
    let upload = ts_upload("import { x } from './missing.ts'; export default () => new Response(String(x));");
    let failure = build_deno(&default_builder(), &mut offline_cx(MemArtifacts::new()), &upload)
        .await
        .expect_err("missing module");
    assert_eq!(failure.reason, BuildFailReason::Resolve, "{failure:?}");
    assert!(
        failure.message.contains("no such file in the upload"),
        "{}",
        failure.message
    );
}

#[tokio::test]
async fn a_non_utf8_archive_module_fails_with_a_syntax_error() {
    // Validation refuses a non-UTF-8 module, so store the archive with it as the source.
    let stored = zip_bytes(&[
        (
            "main.ts",
            b"import { v } from './lib.ts'; export default () => new Response(v);",
        ),
        ("lib.ts", &[0xff, 0xfe]),
    ]);
    let mut cx = offline_cx(MemArtifacts::new());
    let failure = build_stored(&default_builder(), &mut cx, &stored, RuntimeKind::Deno)
        .await
        .expect_err("not UTF-8");
    assert_eq!(failure.reason, BuildFailReason::Syntax, "{failure:?}");
}

// ---------------------------------------------------------------------------------------------
// Import maps into operator SDKs
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn an_import_map_entry_naming_an_sdk_resolves_to_its_shim() {
    let upload = json_upload(
        "main.ts",
        &[(
            "main.ts",
            "import { Bot } from 'telegram'; export default () => new Response(String(Bot));",
        )],
        Some(&[("telegram", "sdk:telegram")]),
    );
    let builder = DefaultBuilder::new(UploadLimits::default()).with_sdk_catalog(sdk_catalog(&[("telegram", &["Bot"])]));
    let mut cx = offline_cx(MemArtifacts::new());
    cx.sdks = Some(sdk_catalog(&[("telegram", &["Bot"])]));
    let built = validate_and_build(
        &builder,
        &mut cx,
        &upload,
        RuntimeKind::Deno,
        None,
        Duration::from_secs(60),
    )
    .await
    .expect("the mapped SDK builds");
    let bundle = bundle_of(&built);
    assert!(
        bundle.modules.iter().any(|m| m.specifier.contains("telegram")),
        "the SDK shim is bundled"
    );
}

// ---------------------------------------------------------------------------------------------
// Guards that validation normally stops first
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn an_entry_missing_from_stored_json_is_an_invalid_source() {
    // The deploy payload validated; the stored bytes name an entry that is not in it.
    let stored = br#"{"entry": "gone.ts", "files": {"main.ts": "export default () => 1;"}}"#;
    let failure = build_stored(
        &default_builder(),
        &mut offline_cx(MemArtifacts::new()),
        stored,
        RuntimeKind::Deno,
    )
    .await
    .expect_err("entry missing");
    assert_eq!(failure.reason, BuildFailReason::Syntax, "{failure:?}");
    assert!(failure.message.contains("does not exist"), "{}", failure.message);
}

#[tokio::test]
async fn a_single_file_over_the_build_limit_is_too_large() {
    let small = DefaultBuilder::new(UploadLimits {
        max_single_file: 4,
        ..UploadLimits::default()
    });
    let failure = build_stored(
        &small,
        &mut offline_cx(MemArtifacts::new()),
        b"export default () => new Response('x');",
        RuntimeKind::Deno,
    )
    .await
    .expect_err("over the limit");
    assert_eq!(failure.reason, BuildFailReason::TooLarge, "{failure:?}");
}

#[tokio::test]
async fn a_component_over_the_build_limit_is_too_large() {
    let small = DefaultBuilder::new(UploadLimits {
        max_wasm: 8,
        ..UploadLimits::default()
    });
    let failure = build_stored(
        &small,
        &mut offline_cx(MemArtifacts::new()),
        OK_COMPONENT,
        RuntimeKind::Wasm,
    )
    .await
    .expect_err("over the limit");
    assert_eq!(failure.reason, BuildFailReason::TooLarge, "{failure:?}");
}

#[tokio::test]
async fn stored_bytes_that_are_not_a_component_are_an_internal_failure() {
    let failure = build_stored(
        &default_builder(),
        &mut offline_cx(MemArtifacts::new()),
        b"not wasm",
        RuntimeKind::Wasm,
    )
    .await
    .expect_err("not a component");
    assert_eq!(failure.reason, BuildFailReason::Internal, "{failure:?}");
}

#[tokio::test]
async fn the_builder_reports_its_upload_limits() {
    let limits = UploadLimits {
        max_files: 7,
        ..UploadLimits::default()
    };
    assert_eq!(DefaultBuilder::new(limits).limits().max_files, 7);
}

#[tokio::test]
async fn an_offline_build_of_a_plain_module_needs_no_network() {
    let built = build_deno(
        &default_builder(),
        &mut offline_cx(MemArtifacts::new()),
        &ts_upload("export default () => new Response('x');"),
    )
    .await
    .expect("no imports, no fetch");
    assert!(built.bundle_digest.is_some());
}
