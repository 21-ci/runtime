//! Build execution limits and failure reasons: BLD-10 (timeout), BLD-11 (oom), plus
//! `syntax` / `resolve` outcomes of `Builder::build`.

use crate::common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use rt_mem::{MemCategory, MemoryGovernor, Owner};
use rt_testkit::{MockCdn, TestGovernor};
use rt_types::{BuildFailReason, RuntimeKind};

const MIB: u64 = 1024 * 1024;

#[tokio::test]
async fn ac_bld_10_build_exceeding_its_timeout_fails_with_timeout_and_frees_the_slot() {
    let builder = default_builder();
    let upload = ts_upload("import \"npm:slow@1\";\nexport default () => new Response('x');\n");
    let mut cx = build_cx(
        MemArtifacts::new(),
        Arc::new(HangingFetcher),
        resolver_cfg("https://esm.sh", &["esm.sh"]),
    );
    let timeout = Duration::from_millis(300);
    cx.deadline = Instant::now() + timeout;

    let t0 = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        validate_and_build(&builder, &mut cx, &upload, RuntimeKind::Deno, None, timeout),
    )
    .await
    .expect("the builder must enforce build.timeout itself; the fetch never answers");
    let failure = outcome.expect_err("build over its timeout");

    assert_eq!(failure.reason, BuildFailReason::Timeout);
    assert!(
        t0.elapsed() < Duration::from_secs(10),
        "gave up after {:?}",
        t0.elapsed()
    );

    // The build processor is free for the next build (same builder, new context).
    let ok_upload = fixture_upload("hello");
    let mut cx2 = offline_cx(MemArtifacts::new());
    build_deno(&builder, &mut cx2, &ok_upload)
        .await
        .expect("next build runs normally");
}

#[tokio::test]
async fn ac_bld_11_build_exceeding_its_memory_reservation_fails_with_oom() {
    // The governor only has 2 MiB; the build holds 1 MiB and its dependency is 4 MiB.
    let gov = TestGovernor::new(2 * MIB);
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js(
        "/big@1",
        &format!("export const s = \"{}\";\n", "x".repeat((4 * MIB) as usize)),
    );
    let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = ts_upload("import \"npm:big@1\";\nexport default () => new Response('x');\n");

    {
        let mut cx = build_cx(MemArtifacts::new(), fetcher, mirror_resolver_cfg(&cdn));
        cx.reservation = gov
            .reserve(MemCategory::Other, MIB, Owner::NONE)
            .expect("reserve build memory");
        assert_eq!(gov.committed(), MIB);

        let failure = build_deno(&builder, &mut cx, &upload)
            .await
            .expect_err("working set above build.memory_mb");
        assert_eq!(failure.reason, BuildFailReason::Oom);
    }
    // The node is unaffected: everything the build held is returned.
    assert_eq!(gov.committed(), 0, "build memory must be released after a failed build");
}

#[tokio::test]
async fn bld_11_a_build_within_its_reservation_succeeds_and_releases_memory() {
    let gov = TestGovernor::new(64 * MIB);
    let builder = default_builder();
    let upload = fixture_upload("multi_file");
    {
        let mut cx = offline_cx(MemArtifacts::new());
        cx.reservation = gov.reserve(MemCategory::Other, 16 * MIB, Owner::NONE).expect("reserve");
        // multi_file needs the import map; validate to feed it to the resolver.
        let v = rt_build::Builder::validate(&builder, RuntimeKind::Deno, &upload).expect("valid");
        let mut cfg = resolver_cfg("https://esm.sh", &["esm.sh"]);
        cfg.import_map = v.import_map;
        cx.resolver = Arc::new(rt_build::DefaultResolver::new(cfg));
        build_deno(&builder, &mut cx, &upload)
            .await
            .expect("small build fits in 16 MiB");
    }
    assert_eq!(gov.committed(), 0);
}

#[tokio::test]
async fn build_unresolvable_import_fails_with_resolve() {
    let builder = default_builder();
    let upload = ts_upload("import \"https://evil.example/x.js\";\nexport default () => new Response('x');\n");
    let mut cx = offline_cx(MemArtifacts::new());
    let failure = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("host not allowed");
    assert_eq!(failure.reason, BuildFailReason::Resolve);
}

#[tokio::test]
async fn build_syntax_error_in_a_dependency_fails_with_syntax() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/broken@1", "export const x = {;\n");
    let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = ts_upload("import \"npm:broken@1\";\nexport default () => new Response('x');\n");
    let mut cx = build_cx(MemArtifacts::new(), fetcher, mirror_resolver_cfg(&cdn));

    let failure = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("dependency does not parse");
    assert_eq!(failure.reason, BuildFailReason::Syntax);
    assert!(!failure.diagnostics.is_empty() || !failure.message.is_empty());
}

#[tokio::test]
async fn build_of_a_missing_source_artifact_fails_with_internal() {
    let builder = default_builder();
    let mut cx = offline_cx(MemArtifacts::new());
    let digest = rt_types::Digest::of(b"never stored");
    let req = rt_build::BuildRequest {
        key: rt_types::PreparedKey {
            digest,
            kind: RuntimeKind::Deno,
            engine_fp: TEST_ENGINE_FP,
        },
        kind: RuntimeKind::Deno,
        source_digest: digest,
        lock: None,
        timeout: Duration::from_secs(5),
    };
    let failure = rt_build::Builder::build(&builder, &req, &mut cx)
        .await
        .expect_err("T3 miss");
    assert_eq!(failure.reason, BuildFailReason::Internal);
}
