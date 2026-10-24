//! The `rt-bundle v1` format, the Deno build stages (transpile -> resolve -> graph -> bundle ->
//! code cache) and the reproducible digest: BLD-12, BLD-13, BLD-15.
//!
//! Format tests exercise `Bundle`/`CodeCache` directly; stage tests run `DefaultBuilder::build`
//! with in-memory collaborators (`MemArtifacts`, fake `CodeCacheProducer`).

use crate::common;

use std::sync::Arc;

use common::*;
use rt_build::{BuildCx, Builder, Bundle, BundleError, BundleModule, CodeCache, ModuleKind, Upload, BUNDLE_FORMAT};

use rt_types::{Digest, HandlerForm, RuntimeKind};

fn module(spec: &str, src: &str) -> BundleModule {
    BundleModule {
        specifier: spec.to_string(),
        kind: ModuleKind::Js,
        source: src.to_string(),
        source_map: None,
    }
}

fn offline(upload: &Upload) -> (rt_build::DefaultBuilder, BuildCx) {
    let builder = default_builder();
    let v = Builder::validate(&builder, RuntimeKind::Deno, upload).unwrap_or_else(|d| panic!("validation: {d:?}"));
    let mut cfg = resolver_cfg("https://esm.sh", &["esm.sh"]);
    cfg.import_map = v.import_map;
    let cx = build_cx(MemArtifacts::new(), Arc::new(NoNetworkFetcher), cfg);
    (builder, cx)
}

async fn build_offline(upload: &Upload) -> rt_build::BuiltArtifact {
    let (builder, mut cx) = offline(upload);
    build_deno(&builder, &mut cx, upload)
        .await
        .unwrap_or_else(|f| panic!("build failed: {f}"))
}

// ---------------------------------------------------------------------------------------------
// bundle format
// ---------------------------------------------------------------------------------------------

#[test]
fn bundle_format_header_and_roundtrip() {
    assert_eq!(BUNDLE_FORMAT, "rt-bundle/1");
    let lock_digest = Digest::of(b"lock");
    let mut b = Bundle::new("file:///main.js", HandlerForm::B, Some(lock_digest));
    b.modules.push(module("file:///main.js", "export default {}"));
    b.modules.push(BundleModule {
        specifier: "file:///data.json".into(),
        kind: ModuleKind::Json,
        source: "{\"a\":1}".into(),
        source_map: None,
    });

    let parsed = Bundle::from_bytes(&b.to_bytes()).expect("roundtrip");
    assert_eq!(parsed.format, BUNDLE_FORMAT);
    assert_eq!(parsed.entry, "file:///main.js");
    assert_eq!(parsed.handler_form, HandlerForm::B);
    assert_eq!(parsed.lock_digest, Some(lock_digest));
    assert_eq!(parsed.modules.len(), 2);
    assert_eq!(
        parsed.module("file:///data.json").map(|m| m.kind),
        Some(ModuleKind::Json)
    );
    assert!(parsed.module("file:///missing.js").is_none());

    // Wire shape: header fields + module table with a lowercase kind.
    let v: serde_json::Value = serde_json::from_slice(&b.to_bytes()).expect("json");
    for key in ["format", "entry", "handler_form", "lock_digest", "modules"] {
        assert!(v.get(key).is_some(), "bundle header misses `{key}`");
    }
    assert!(v["modules"]
        .as_array()
        .expect("modules")
        .iter()
        .any(|m| m["kind"] == "json"));
}

#[test]
fn bundle_bytes_are_canonical_whatever_the_module_order() {
    let mk = |order: &[&str]| {
        let mut b = Bundle::new("file:///main.js", HandlerForm::A, None);
        for s in order {
            b.modules.push(module(s, &format!("// {s}")));
        }
        b
    };
    let a = mk(&["file:///main.js", "https://esm.sh/zod@3", "file:///util.js"]);
    let b = mk(&["file:///util.js", "file:///main.js", "https://esm.sh/zod@3"]);
    assert_eq!(a.to_bytes(), b.to_bytes());
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.digest(), Digest::of(&a.to_bytes()));

    let parsed = Bundle::from_bytes(&a.to_bytes()).expect("parse");
    let specs: Vec<&str> = parsed.modules.iter().map(|m| m.specifier.as_str()).collect();
    let mut sorted = specs.clone();
    sorted.sort();
    assert_eq!(specs, sorted, "module table is sorted by specifier");
}

#[test]
fn bundle_digest_covers_the_lock_and_the_sources() {
    let mut a = Bundle::new("file:///main.js", HandlerForm::A, None);
    a.modules.push(module("file:///main.js", "1"));
    let mut b = a.clone();
    b.modules[0].source = "2".into();
    let mut c = a.clone();
    c.lock_digest = Some(Digest::of(b"x"));
    assert_ne!(a.digest(), b.digest());
    assert_ne!(a.digest(), c.digest());
}

#[test]
fn bundle_parser_rejects_foreign_formats_and_missing_entries() {
    let mut b = Bundle::new("file:///main.js", HandlerForm::A, None);
    b.modules.push(module("file:///main.js", "x"));

    let mut foreign = b.clone();
    foreign.format = "rt-bundle/99".into();
    assert!(matches!(Bundle::from_bytes(&foreign.to_bytes()), Err(BundleError::Format(f)) if f == "rt-bundle/99"));

    let mut headless = b.clone();
    headless.entry = "file:///other.js".into();
    assert!(matches!(
        Bundle::from_bytes(&headless.to_bytes()),
        Err(BundleError::MissingEntry(_))
    ));

    assert!(matches!(Bundle::from_bytes(b"not json"), Err(BundleError::Json(_))));
}

#[test]
fn code_cache_roundtrips() {
    let mut cc = CodeCache {
        engine_fp: 7,
        per_module: Default::default(),
    };
    cc.per_module.insert("file:///main.js".into(), vec![1, 2, 3]);
    cc.per_module.insert("https://esm.sh/zod@3".into(), vec![]);
    assert_eq!(CodeCache::from_bytes(&cc.to_bytes()).expect("parse"), cc);
    assert!(CodeCache::from_bytes(b"garbage").is_err());
}

// ---------------------------------------------------------------------------------------------
// build stages
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn build_transpiles_typescript_and_keeps_source_maps() {
    let art = build_offline(&fixture_upload("hello")).await;
    let bundle = bundle_of(&art);

    let entry = bundle.module(&bundle.entry).expect("entry module");
    assert_eq!(entry.kind, ModuleKind::Js);
    assert!(
        !entry.source.contains(": Response"),
        "type annotations are stripped: {}",
        entry.source
    );
    assert!(entry.source.contains("hello"));
    assert!(entry.source_map.is_some(), "source maps are kept for stack traces");
    assert_eq!(bundle.handler_form, HandlerForm::A);
    assert_eq!(bundle.format, BUNDLE_FORMAT);
    assert_eq!(art.bundle_digest, Some(bundle.digest()));
    assert_eq!(
        art.bundle_digest,
        Some(Digest::of(&built_file(&art, "bundle").expect("bundle file").bytes)),
        "the digest is of the stored bundle bytes"
    );
}

#[tokio::test]
async fn build_without_remote_dependencies_has_an_empty_lock() {
    let art = build_offline(&fixture_upload("hello")).await;
    assert!(
        art.lock.as_ref().is_none_or(|l| l.entries.is_empty()),
        "got {:?}",
        art.lock
    );
}

#[tokio::test]
async fn build_records_the_handler_form_in_the_header() {
    let b = bundle_of(&build_offline(&fixture_upload("form_b")).await);
    assert_eq!(b.handler_form, HandlerForm::B);
    let b = bundle_of(&build_offline(&fixture_upload("form_a_scheduled")).await);
    assert_eq!(b.handler_form, HandlerForm::A);
    let ambiguous = ts_upload("function make() { return () => new Response('x'); }\nexport default make();\n");
    let b = bundle_of(&build_offline(&ambiguous).await);
    assert_eq!(b.handler_form, HandlerForm::Unknown);
}

#[tokio::test]
async fn build_bundles_the_module_graph_and_nothing_else() {
    let upload = fixture_upload("multi_file");
    let bundle = bundle_of(&build_offline(&upload).await);

    let all_source: String = bundle
        .modules
        .iter()
        .map(|m| m.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        all_source.contains("hello, "),
        "greet.ts (import-map alias `greet`) is bundled"
    );
    assert!(
        all_source.contains("toUpperCase"),
        "util.ts (relative import) is bundled"
    );
    assert!(
        all_source.contains("lazy-loaded"),
        "a literal dynamic import is part of the graph"
    );
    assert!(
        !all_source.contains("never-bundled"),
        "files nothing imports are not bundled"
    );

    let json = bundle
        .modules
        .iter()
        .find(|m| m.kind == ModuleKind::Json)
        .expect("data.json as a json module");
    assert!(json.source.contains("defaultName"));
    assert!(json.source_map.is_none());
}

#[tokio::test]
async fn build_does_not_bundle_runtime_builtins() {
    let upload = ts_upload(
        "import { kv } from \"std:kv\";\nexport default async () => new Response(String(await kv.get('a')));\n",
    );
    let bundle = bundle_of(&build_offline(&upload).await);
    assert!(
        !bundle.modules.iter().any(|m| m.specifier.starts_with("std:")),
        "std:* modules come from the base snapshot: {:?}",
        bundle.modules.iter().map(|m| &m.specifier).collect::<Vec<_>>()
    );
    // ...but the import itself is kept so the loader can serve it.
    let entry = bundle.module(&bundle.entry).expect("entry");
    assert!(entry.source.contains("std:kv"));
}

#[tokio::test]
async fn build_non_literal_dynamic_imports_do_not_pull_in_anything() {
    let upload = ts_upload(
        "export default async (req: Request) => {\n  const m = await import(new URL(req.url).pathname);\n  return new Response(String(m));\n};\n",
    );
    let bundle = bundle_of(&build_offline(&upload).await);
    assert_eq!(bundle.modules.len(), 1, "only the entry module");
}

#[tokio::test]
async fn build_artifact_key_is_the_requested_key() {
    let upload = ts_upload(&fixture_source("hello"));
    let (builder, mut cx) = offline(&upload);
    let digest = cx.artifacts.put(upload.body.clone()).await.expect("put");
    let key = rt_types::PreparedKey {
        digest,
        kind: RuntimeKind::Deno,
        engine_fp: 0xABCD,
    };
    let req = rt_build::BuildRequest {
        key,
        kind: RuntimeKind::Deno,
        source_digest: digest,
        lock: None,
        timeout: std::time::Duration::from_secs(60),
    };
    let art = builder.build(&req, &mut cx).await.expect("build");
    assert_eq!(art.key, key);
    assert!(built_file(&art, "bundle").is_some());
}

// ---------------------------------------------------------------------------------------------
// BLD-12 reproducible digest
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_bld_12_same_source_built_twice_has_the_same_bundle_digest() {
    for name in ["hello", "multi_file", "form_b"] {
        let upload = fixture_upload(name);
        let first = build_offline(&upload).await;
        let second = build_offline(&upload).await; // fresh store, fresh context
        assert_eq!(first.bundle_digest, second.bundle_digest, "{name}");
        assert_eq!(
            built_file(&first, "bundle").map(|f| f.bytes.clone()),
            built_file(&second, "bundle").map(|f| f.bytes.clone()),
            "{name}: byte-identical bundle"
        );
    }
}

#[tokio::test]
async fn ac_bld_12_same_source_and_same_lock_with_cleared_caches_is_identical() {
    let cdn = cdn_with_zod().await;
    let builder = default_builder();
    let upload = fixture_upload("npm_zod");

    let run = |lock: Option<rt_build::RtLock>| {
        let cdn = &cdn;
        let builder = &builder;
        let upload = &upload;
        async move {
            // Cleared caches: fresh fetcher (deps cache), fresh T3 store.
            let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(cdn), &["127.0.0.1"]);
            let mut cx = build_cx(MemArtifacts::new(), fetcher, mirror_resolver_cfg(cdn));
            validate_and_build(
                builder,
                &mut cx,
                upload,
                RuntimeKind::Deno,
                lock,
                std::time::Duration::from_secs(60),
            )
            .await
            .expect("build")
        }
    };

    let first = run(None).await;
    let second = run(first.lock.clone()).await;
    let third = run(first.lock.clone()).await;

    assert_eq!(first.bundle_digest, second.bundle_digest);
    assert_eq!(second.bundle_digest, third.bundle_digest);
    assert_eq!(first.lock, second.lock);
    assert_eq!(
        built_file(&first, "bundle").map(|f| f.bytes.clone()),
        built_file(&third, "bundle").map(|f| f.bytes.clone())
    );
    assert_eq!(bundle_of(&first).lock_digest, Some(first.lock.expect("lock").digest()));
}

#[tokio::test]
async fn bld_12_digest_does_not_depend_on_the_container_format_or_file_order() {
    let files = fixture_files("multi_file");
    let in_order: Vec<(&str, &[u8])> = files.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect();
    let mut reversed = in_order.clone();
    reversed.reverse();

    let a = build_offline(&zip_upload(&in_order)).await;
    let b = build_offline(&zip_upload(&reversed)).await;
    let c = build_offline(&fixture_tar_upload("multi_file")).await;
    assert_eq!(a.bundle_digest, b.bundle_digest, "zip entry order must not matter");
    assert_eq!(
        a.bundle_digest, c.bundle_digest,
        "zip and tar of the same files give the same bundle"
    );
}

#[tokio::test]
async fn bld_12_different_sources_have_different_digests() {
    let a = build_offline(&fixture_upload("hello")).await;
    let b = build_offline(&fixture_upload("global_counter")).await;
    assert_ne!(a.bundle_digest, b.bundle_digest);
}

// ---------------------------------------------------------------------------------------------
// BLD-13 code cache production
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_bld_13_builder_asks_the_producer_for_a_code_cache_of_the_canonical_bundle() {
    let upload = fixture_upload("multi_file");
    let (builder, mut cx) = offline(&upload);
    let producer = FakeCodeCache::new(0xC0DE);
    cx.code_cache = Some(producer.clone());

    let art = build_deno(&builder, &mut cx, &upload).await.expect("build");
    let bundle = bundle_of(&art);

    assert_eq!(producer.calls(), 1, "one code-cache pass per build");
    let seen = producer
        .last_bundle
        .lock()
        .expect("lock")
        .clone()
        .expect("producer saw a bundle");
    assert_eq!(
        seen.digest(),
        bundle.digest(),
        "the cache is produced for the exact bundle that is stored"
    );

    let file = built_file(&art, "code_cache.bin").expect("code_cache.bin is part of the T2 entry");
    let cc = CodeCache::from_bytes(&file.bytes).expect("code cache parses");
    assert_eq!(
        cc.engine_fp, 0xC0DE,
        "the cache carries the engine fingerprint it was produced under"
    );
    let mut want: Vec<&str> = bundle.modules.iter().map(|m| m.specifier.as_str()).collect();
    let mut got: Vec<&str> = cc.per_module.keys().map(String::as_str).collect();
    want.sort();
    got.sort();
    assert_eq!(got, want, "one blob per module specifier");
    for m in &bundle.modules {
        assert_eq!(cc.per_module[&m.specifier], FakeCodeCache::blob_for(&m.specifier));
    }
    // The bundle file itself is unaffected by the cache.
    assert_eq!(art.bundle_digest, Some(bundle.digest()));
}

#[tokio::test]
async fn bld_13_without_a_producer_only_the_bundle_is_written() {
    let upload = fixture_upload("hello");
    let (builder, mut cx) = offline(&upload);
    cx.code_cache = None;
    let art = build_deno(&builder, &mut cx, &upload).await.expect("build");
    assert!(built_file(&art, "bundle").is_some());
    assert!(built_file(&art, "code_cache.bin").is_none());
    let names: Vec<&str> = art.files.iter().map(|f| f.name.as_str()).collect();
    assert!(!names.contains(&"component.cwasm"), "{names:?}");
}

#[tokio::test]
async fn bld_13_code_cache_covers_remote_modules_too() {
    let cdn = cdn_with_zod().await;
    let upload = fixture_upload("npm_zod");
    let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(&cdn), &["127.0.0.1"]);
    let mut cx = build_cx(MemArtifacts::new(), fetcher, mirror_resolver_cfg(&cdn));
    let producer = FakeCodeCache::new(1);
    cx.code_cache = Some(producer.clone());

    let art = validate_and_build(
        &default_builder(),
        &mut cx,
        &upload,
        RuntimeKind::Deno,
        None,
        std::time::Duration::from_secs(60),
    )
    .await
    .expect("build");
    let cc = CodeCache::from_bytes(&built_file(&art, "code_cache.bin").expect("cache").bytes).expect("parse");
    assert!(
        cc.per_module.keys().any(|k| k.contains(ZOD_IMPL_PATH)),
        "{:?}",
        cc.per_module.keys()
    );
}

// ---------------------------------------------------------------------------------------------
// BLD-15 operator SDK
// ---------------------------------------------------------------------------------------------

const SDK_CONSUMER: &str = "import { acme } from \"std:sdk\";\nimport pay, { charge } from \"sdk:pay\";\n\
export default async () => new Response(String(await acme.ping()) + String(await charge(1)) + String(pay));\n";

/// BLD-15 / SDK-18: an operator SDK import never bundles SDK code. `std:sdk` and `sdk:<name>` become
/// shims generated from the published contracts over the runtime's `sdk(name)` proxy.
#[tokio::test]
async fn ac_bld_15_sdk_imports_build_into_contract_shims_without_sdk_code() {
    let builder =
        default_builder().with_sdk_catalog(sdk_catalog(&[("acme", &["ping"]), ("pay", &["charge", "refunds"])]));
    let upload = ts_upload(SDK_CONSUMER);
    let mut cx = offline_cx(MemArtifacts::new());
    cx.sdks = Some(sdk_catalog(&[("acme", &["ping"]), ("pay", &["charge", "refunds"])]));

    let art = build_deno(&builder, &mut cx, &upload)
        .await
        .expect("service importing SDKs builds");
    let bundle = bundle_of(&art);
    let std = bundle.module("file:///__sdk__/std.js").expect("the std:sdk shim");
    assert!(std.source.contains("from \"std:sdk\""), "{}", std.source);
    assert!(
        std.source.contains("export const acme = sdk(\"acme\");"),
        "{}",
        std.source
    );
    let pay = bundle.module("file:///__sdk__/pay.js").expect("the sdk:pay shim");
    assert!(
        pay.source.contains("export default pay;") && pay.source.contains("export const charge = pay.charge;"),
        "{}",
        pay.source
    );
    let entry = bundle.module(&bundle.entry).expect("entry");
    assert!(
        entry.source.contains("file:///__sdk__/std.js") && entry.source.contains("file:///__sdk__/pay.js"),
        "{}",
        entry.source
    );
    // Only the runtime module stays an import: no SDK code, nothing fetched or locked.
    assert_eq!(
        bundle.modules.len(),
        3,
        "{:?}",
        bundle.modules.iter().map(|m| &m.specifier).collect::<Vec<_>>()
    );
    assert!(art.lock.as_ref().is_none_or(|l| l.entries.is_empty()));
}

#[tokio::test]
async fn ac_bld_15_an_sdk_that_is_not_published_fails_the_build() {
    let builder = default_builder().with_sdk_catalog(sdk_catalog(&[("pay", &["charge"])]));
    let upload = ts_upload("import x from \"sdk:pay\";\nexport default () => new Response(String(x));\n");
    let mut cx = offline_cx(MemArtifacts::new());
    cx.sdks = Some(sdk_catalog(&[("acme", &["ping"])]));
    let err = build_deno(&builder, &mut cx, &upload)
        .await
        .expect_err("sdk:pay is not published on this node");
    assert_eq!(err.reason, rt_types::BuildFailReason::Resolve);
    assert!(err.message.contains("unknown sdk `sdk:pay`"), "{}", err.message);
}
