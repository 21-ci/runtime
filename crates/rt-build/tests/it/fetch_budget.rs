//! B7: `build.max_deps_mb` is a total per build (not a per-fetch cap), and the in-memory deps
//! cache of `GuardedFetcher` is bounded by bytes (LRU).

use crate::common;

use std::sync::Arc;

use common::*;
use rt_build::{BuildCx, Builder, GuardedFetcher, ModuleFetcher, Upload};
use rt_testkit::MockCdn;
use rt_types::{BuildFailReason, RuntimeKind};

fn module_of(bytes: usize) -> String {
    format!("export const s = \"{}\";\n", "x".repeat(bytes))
}

fn cx_for(
    builder: &rt_build::DefaultBuilder,
    upload: &Upload,
    cdn: &MockCdn,
    fetcher: Arc<dyn ModuleFetcher>,
) -> BuildCx {
    let v = builder
        .validate(RuntimeKind::Deno, upload)
        .unwrap_or_else(|d| panic!("validation failed: {d:?}"));
    let mut cfg = mirror_resolver_cfg(cdn);
    cfg.import_map = v.import_map.clone();
    build_cx(MemArtifacts::new(), fetcher, cfg)
}

fn two_deps_upload() -> Upload {
    ts_upload("import \"npm:a@1\";\nimport \"npm:b@1\";\nexport default () => new Response('x');\n")
}

#[tokio::test]
async fn max_deps_is_a_total_per_build_not_a_per_fetch_cap() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/a@1", &module_of(10 * 1024));
    cdn.set_js("/b@1", &module_of(10 * 1024));
    let egress = LoopbackEgress::for_cdn(&cdn);
    let mut fcfg = fetcher_cfg(&["127.0.0.1"]);
    fcfg.max_module_bytes = 16 * 1024;
    fcfg.max_total_bytes = 15 * 1024; // each module fits, both together do not
    let fetcher = Arc::new(GuardedFetcher::new(egress, fcfg));
    let builder = default_builder();
    let upload = two_deps_upload();
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    let failure = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("20 KiB of deps over a 15 KiB total");
    assert_eq!(failure.reason, BuildFailReason::TooLarge, "{failure}");
}

#[tokio::test]
async fn deps_served_from_the_cache_count_toward_the_total() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/a@1", &module_of(10 * 1024));
    cdn.set_js("/b@1", &module_of(10 * 1024));
    let egress = LoopbackEgress::for_cdn(&cdn);
    let mut fcfg = fetcher_cfg(&["127.0.0.1"]);
    fcfg.max_total_bytes = 15 * 1024;
    let fetcher: Arc<dyn ModuleFetcher> = Arc::new(GuardedFetcher::new(egress, fcfg));
    let builder = default_builder();

    // warm the deps cache with each module in its own (small enough) build
    for spec in ["npm:a@1", "npm:b@1"] {
        let upload = ts_upload(&format!(
            "import \"{spec}\";\nexport default () => new Response('x');\n"
        ));
        let mut cx = cx_for(&builder, &upload, &cdn, fetcher.clone());
        build_deno(&builder, &mut cx, &upload)
            .await
            .expect("one 10 KiB dependency fits");
    }
    let upload = two_deps_upload();
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher.clone());
    let failure = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("cached deps still count");
    assert_eq!(failure.reason, BuildFailReason::TooLarge);
}

#[tokio::test]
async fn a_build_within_the_total_succeeds() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/a@1", &module_of(4 * 1024));
    cdn.set_js("/b@1", &module_of(4 * 1024));
    let egress = LoopbackEgress::for_cdn(&cdn);
    let mut fcfg = fetcher_cfg(&["127.0.0.1"]);
    fcfg.max_total_bytes = 15 * 1024;
    let fetcher = Arc::new(GuardedFetcher::new(egress, fcfg));
    let builder = default_builder();
    let upload = two_deps_upload();
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);
    build_deno(&builder, &mut cx, &upload)
        .await
        .expect("8 KiB of deps under a 15 KiB total");
}

#[tokio::test]
async fn the_deps_cache_is_bounded_by_bytes_and_evicts_the_least_recently_used() {
    let cdn = MockCdn::start().await.expect("cdn");
    for p in ["/m1.js", "/m2.js", "/m3.js"] {
        cdn.set_js(p, &module_of(10 * 1024));
    }
    let egress = LoopbackEgress::for_cdn(&cdn);
    let f = GuardedFetcher::new(egress, fetcher_cfg(&["127.0.0.1"])).with_cache_capacity(25 * 1024);
    let get = |p: &str| url(&cdn.url(p));

    f.fetch(&get("/m1.js"), None).await.expect("m1");
    f.fetch(&get("/m2.js"), None).await.expect("m2");
    // touch m1: m2 becomes the least recently used
    assert!(f.fetch(&get("/m1.js"), None).await.expect("m1 again").from_cache);
    f.fetch(&get("/m3.js"), None).await.expect("m3");
    assert!(f.cache_bytes() <= 25 * 1024, "cache holds {} bytes", f.cache_bytes());

    assert!(
        f.fetch(&get("/m1.js"), None).await.expect("m1").from_cache,
        "recently used m1 stays"
    );
    assert!(
        f.fetch(&get("/m3.js"), None).await.expect("m3").from_cache,
        "newest m3 stays"
    );
    let m2 = f.fetch(&get("/m2.js"), None).await.expect("m2");
    assert!(!m2.from_cache, "m2 was evicted and is fetched again");
    assert_eq!(cdn.hits("/m2.js"), 2);
}

#[tokio::test]
async fn a_module_larger_than_the_cache_is_not_cached() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/big.js", &module_of(8 * 1024));
    let egress = LoopbackEgress::for_cdn(&cdn);
    let f = GuardedFetcher::new(egress, fetcher_cfg(&["127.0.0.1"])).with_cache_capacity(4 * 1024);
    let u = url(&cdn.url("/big.js"));
    f.fetch(&u, None).await.expect("big");
    assert_eq!(f.cache_bytes(), 0);
    assert!(!f.fetch(&u, None).await.expect("big again").from_cache);
    assert_eq!(cdn.hits("/big.js"), 2);
}

#[tokio::test]
async fn a_refetch_for_a_different_locked_digest_replaces_the_cached_copy() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/mod.js", "export const v = 1;");
    let egress = LoopbackEgress::for_cdn(&cdn);
    let f = GuardedFetcher::new(egress, fetcher_cfg(&["127.0.0.1"])).with_vendor_store(MemArtifacts::new());
    let target = url(&cdn.url("/mod.js"));

    let first = f.fetch(&target, None).await.expect("first fetch");
    assert_eq!(first.bytes.as_ref(), b"export const v = 1;");

    // The CDN now serves other bytes; the lock asks for them, so the cached copy cannot answer.
    cdn.set_js("/mod.js", "export const v = 2;");
    let want = rt_types::Digest::of(b"export const v = 2;");
    let second = f
        .fetch(&target, Some(want))
        .await
        .expect("refetch under the new digest");
    assert_eq!(second.digest, want);
    assert!(!second.from_cache, "the new bytes came from the network");
}

#[tokio::test]
async fn a_url_with_a_query_string_is_not_shared_through_the_deps_cache() {
    // FETCH-004: `?token=…` may be a credential; another tenant's build must not be served it.
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/mod.js", "export const v = 1;");
    let f = GuardedFetcher::new(LoopbackEgress::for_cdn(&cdn), fetcher_cfg(&["127.0.0.1"]));
    let with_token = url(&cdn.url("/mod.js?token=tenant-a-secret"));

    let first_build = f.fetch(&with_token, None).await.expect("first build");
    assert!(!first_build.from_cache);
    let second_build = f.fetch(&with_token, None).await.expect("second build");
    assert!(
        !second_build.from_cache,
        "a token-bearing URL is fetched again, never served from the cache"
    );
    assert_eq!(cdn.hits("/mod.js"), 2, "both builds reached the network");

    // The rule is about query strings: a plain URL is still cached.
    let plain = url(&cdn.url("/mod.js"));
    f.fetch(&plain, None).await.expect("plain URL");
    assert!(f.fetch(&plain, None).await.expect("plain URL again").from_cache);
}
