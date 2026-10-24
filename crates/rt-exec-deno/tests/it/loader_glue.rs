//! The glue around the executor: engine fingerprint, config mapping, embedded `std:*` JS, the T2 ->
//! T1 `DenoLoader` (MAC verified before parsing), the V8 `CodeCacheProducer`, and the
//! `DenoInterrupter`.

use crate::common;

use std::sync::Arc;

use common::*;
use rt_build::{Builder, Bundle, CodeCache, CodeCacheProducer, ModuleKind, BUILTIN_MODULES};
use rt_cache::{LoadError, Loader, NoTamperSink, T2Store};
use rt_config::{ClockMode, Config};
use rt_exec_deno::{
    engine_fingerprint, DenoExecutorConfig, DenoInterrupter, DenoLoader, V8CodeCacheProducer, BASE_V8_FLAGS,
    HEAP_GROW_STEP_MB, T2_BUNDLE, T2_CODE_CACHE,
};
use rt_sched::{Interrupt, Interrupter};
use rt_sec::{HkdfArtifactMac, MasterKey};
use rt_types::{BuildFailReason, HandlerForm, KillReason, PreparedKey, RuntimeKind, TaskId, Trigger};

// ---------------------------------------------------------------------------------------------
// config and fingerprint
// ---------------------------------------------------------------------------------------------

#[test]
fn executor_config_defaults_match_the_spec() {
    let c = DenoExecutorConfig::default();
    assert_eq!(c.isolate_base_mb, 4);
    assert_eq!(c.v8_platform_threads, 1);
    assert_eq!(c.dispose_backlog, 8);
    assert_eq!(c.clock, ClockMode::Frozen);
    assert!(c.v8_flags.iter().any(|f| f == "--single-threaded-gc"));
    assert_eq!(BASE_V8_FLAGS, &["--single-threaded-gc"]);
    assert_eq!(HEAP_GROW_STEP_MB, 8);
}

#[test]
fn executor_config_follows_the_profile_pool_size() {
    for (profile, pool) in [
        (rt_types::Profile::MaxDensity, 0),
        (rt_types::Profile::Balanced, 1),
        (rt_types::Profile::MinLatency, 4),
    ] {
        let mut cfg = Config::default();
        cfg.node.default_profile = profile;
        let c = DenoExecutorConfig::from_config(&cfg);
        assert_eq!(c.isolate_pool_per_p, pool, "{profile:?}");
        assert_eq!(c.isolate_base_mb, cfg.deno.isolate_base_mb);
        assert_eq!(c.clock, cfg.security.clock);
    }
}

#[test]
fn engine_fingerprint_is_stable_order_independent_and_flag_sensitive() {
    let a = vec!["--single-threaded-gc".to_string(), "--max-lazy".to_string()];
    let b = vec!["--max-lazy".to_string(), "--single-threaded-gc".to_string()];
    let c = vec!["--single-threaded-gc".to_string()];
    assert_eq!(engine_fingerprint(&a), engine_fingerprint(&a));
    assert_eq!(
        engine_fingerprint(&a),
        engine_fingerprint(&b),
        "flag order must not matter"
    );
    assert_ne!(
        engine_fingerprint(&a),
        engine_fingerprint(&c),
        "different flags = different engine"
    );
    assert_ne!(engine_fingerprint(&c), engine_fingerprint(&[]));
}

// ---------------------------------------------------------------------------------------------
// embedded std:* modules
// ---------------------------------------------------------------------------------------------

#[test]
fn every_builtin_the_builder_accepts_has_an_embedded_module() {
    for m in BUILTIN_MODULES {
        let src = rt_exec_deno::js::rt_module_source(m).unwrap_or_else(|| panic!("no embedded JS for {m}"));
        assert!(src.contains("export"), "{m} must export its API");
    }
    assert!(rt_exec_deno::js::rt_module_source("std:nope").is_none());
    for (spec, _) in rt_exec_deno::js::RT_MODULES {
        assert!(
            BUILTIN_MODULES.contains(spec),
            "{spec} is embedded but the builder would reject it"
        );
    }
}

#[test]
fn renamed_rt_modules_say_where_they_moved() {
    assert_eq!(
        rt_exec_deno::js::renamed_module_message("rt:kv").as_deref(),
        Some("`rt:kv` was renamed to `std:kv`")
    );
    assert_eq!(rt_exec_deno::js::renamed_module_message("std:kv"), None);
    assert_eq!(rt_exec_deno::js::renamed_module_message("rt:nope"), None);
}

// ---------------------------------------------------------------------------------------------
// DenoInterrupter
// ---------------------------------------------------------------------------------------------

#[test]
fn interrupter_ignores_unknown_tasks() {
    let i = DenoInterrupter::new();
    assert_eq!(i.running(), 0);
    i.interrupt(TaskId(99), Interrupt::Kill(KillReason::CpuLimit));
    i.interrupt(TaskId(99), Interrupt::Yield);
    i.unregister(TaskId(99));
    assert_eq!(i.running(), 0);
}

// ---------------------------------------------------------------------------------------------
// V8CodeCacheProducer
// ---------------------------------------------------------------------------------------------

#[test]
fn v8_code_cache_producer_caches_every_js_module_without_running_it() {
    run_local(async {
        let h = Harness::new(cfg0()); // initializes V8
        let upload = fixture_upload("multi_file");
        let p = h
            .prepare_with(
                &upload,
                Arc::new(NoNetworkFetcher),
                mirror_free_resolver(&upload),
                false,
            )
            .await;
        assert!(p.code_cache.is_none());

        let producer = V8CodeCacheProducer::new(h.engine_fp);
        assert_eq!(producer.engine_fp(), h.engine_fp);
        let cc = producer.produce(&p.bundle).expect("produce");
        assert_eq!(cc.engine_fp, h.engine_fp);
        let js: Vec<&str> = p
            .bundle
            .modules
            .iter()
            .filter(|m| m.kind == ModuleKind::Js)
            .map(|m| m.specifier.as_str())
            .collect();
        assert_eq!(cc.per_module.len(), js.len());
        for s in js {
            assert!(cc.per_module.get(s).is_some_and(|b| !b.is_empty()), "no cache for {s}");
        }
    });
}

#[test]
fn v8_code_cache_producer_does_not_evaluate_module_code() {
    run_local(async {
        let h = Harness::new(cfg0());
        // Evaluating this module would throw at top level; compiling it must not.
        let upload =
            ts_upload("throw new Error('evaluated at build time');\nexport default () => new Response('x');\n");
        let p = h
            .prepare_with(
                &upload,
                Arc::new(NoNetworkFetcher),
                mirror_free_resolver(&upload),
                false,
            )
            .await;
        V8CodeCacheProducer::new(h.engine_fp)
            .produce(&p.bundle)
            .expect("compile only, no evaluation");
    });
}

#[test]
fn v8_code_cache_producer_reports_a_module_that_does_not_compile() {
    run_local(async {
        let h = Harness::new(cfg0());
        let mut bundle = Bundle::new("file:///main.js", HandlerForm::A, None);
        bundle.modules.push(rt_build::BundleModule {
            specifier: "file:///main.js".into(),
            kind: ModuleKind::Js,
            source: "export default function ( {".into(),
            source_map: None,
        });
        let err = V8CodeCacheProducer::new(h.engine_fp)
            .produce(&bundle)
            .expect_err("syntax error in a bundle module");
        assert!(
            matches!(err.reason, BuildFailReason::Syntax | BuildFailReason::Internal),
            "{err}"
        );
    });
}

// ---------------------------------------------------------------------------------------------
// DenoLoader: T2 -> T1
// ---------------------------------------------------------------------------------------------

struct T2Fixture {
    _dir: tempfile::TempDir,
    t2: Arc<T2Store>,
    key: PreparedKey,
    bundle: Bundle,
    cache: CodeCache,
}

fn t2_with_entry() -> T2Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let mac = Arc::new(HkdfArtifactMac::new(&MasterKey::from_bytes([7u8; 32])));
    let t2 = Arc::new(T2Store::new(dir.path(), mac, Arc::new(NoTamperSink)));

    let upload = fixture_upload("hello");
    let b = default_builder();
    let v = Builder::validate(&b, RuntimeKind::Deno, &upload).expect("valid");
    let key = PreparedKey {
        digest: v.upload.source_digest,
        kind: RuntimeKind::Deno,
        engine_fp: 0xE5,
    };

    let mut bundle = Bundle::new("file:///main.js", HandlerForm::A, None);
    bundle.modules.push(rt_build::BundleModule {
        specifier: "file:///main.js".into(),
        kind: ModuleKind::Js,
        source: "export default () => new Response('hello');".into(),
        source_map: None,
    });
    let mut cache = CodeCache {
        engine_fp: 0xE5,
        per_module: Default::default(),
    };
    cache.per_module.insert("file:///main.js".into(), vec![1, 2, 3, 4]);

    t2.write(&key, T2_BUNDLE, &bundle.to_bytes()).expect("write bundle");
    t2.write(&key, T2_CODE_CACHE, &cache.to_bytes()).expect("write cache");
    T2Fixture {
        _dir: dir,
        t2,
        key,
        bundle,
        cache,
    }
}

fn loader(t2: &Arc<T2Store>) -> DenoLoader {
    DenoLoader::new(
        t2.clone(),
        Arc::new(|_k: &PreparedKey| rt_testkit::SpecBuilder::build().finish()),
    )
}

#[test]
fn t2_file_names_are_the_documented_ones() {
    assert_eq!(T2_BUNDLE, "bundle");
    assert_eq!(T2_CODE_CACHE, "code_cache.bin");
}

#[test]
fn loader_reads_bundle_and_code_cache_from_t2() {
    let f = t2_with_entry();
    let (prepared, bytes) = loader(&f.t2).load_t2(&f.key).expect("load").expect("entry present");
    assert_eq!(prepared.bundle, f.bundle);
    assert_eq!(prepared.code_cache, Some(f.cache.clone()));
    assert_eq!(prepared.handler_form, HandlerForm::A);
    assert_eq!(prepared.key, f.key);
    assert!(
        bytes >= f.bundle.to_bytes().len() as u64,
        "accounted bytes cover at least the bundle"
    );
}

#[test]
fn loader_reports_absent_entries_as_a_miss() {
    let f = t2_with_entry();
    let other = PreparedKey {
        digest: rt_types::Digest::of(b"nothing"),
        ..f.key
    };
    assert!(
        loader(&f.t2).load_t2(&other).expect("load").is_none(),
        "Ok(None) = needs a build"
    );
}

#[test]
fn loader_without_a_code_cache_file_still_loads() {
    let f = t2_with_entry();
    std::fs::remove_file(f.t2.path_for(&f.key, T2_CODE_CACHE)).expect("remove cache");
    let _ = std::fs::remove_file(f.t2.path_for(&f.key, &format!("{T2_CODE_CACHE}.mac")));
    let (prepared, _) = loader(&f.t2)
        .load_t2(&f.key)
        .expect("load")
        .expect("bundle alone is enough");
    assert!(prepared.code_cache.is_none(), "falls back to compile");
}

#[test]
fn ac_dno_10_loader_unparsable_code_cache_falls_back_to_compile() {
    let f = t2_with_entry();
    f.t2.write(&f.key, T2_CODE_CACHE, b"not a code cache")
        .expect("overwrite with valid MAC but garbage");
    let (prepared, _) = loader(&f.t2)
        .load_t2(&f.key)
        .expect("load must not fail")
        .expect("entry");
    assert!(prepared.code_cache.is_none());
}

#[test]
fn dno_10_a_code_cache_in_the_old_json_layout_is_a_miss_not_an_error() {
    let f = t2_with_entry();
    // T2 entries written before the binary layout: valid MAC, JSON body.
    let json = serde_json::to_vec(&f.cache).expect("json");
    f.t2.write(&f.key, T2_CODE_CACHE, &json)
        .expect("overwrite with the old layout");
    let (prepared, _) = loader(&f.t2)
        .load_t2(&f.key)
        .expect("not tampered, not corrupt")
        .expect("entry");
    assert!(prepared.code_cache.is_none(), "compiles from source instead");
    assert_eq!(prepared.bundle, f.bundle);
    // The binary layout written today is what the loader reads back.
    assert!(f.cache.to_bytes().starts_with(b"RTCC"));
}

#[test]
fn sec_21_loader_never_parses_a_tampered_bundle() {
    let f = t2_with_entry();
    let path = f.t2.path_for(&f.key, T2_BUNDLE);
    let mut bytes = std::fs::read(&path).expect("read bundle");
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x01; // one flipped byte; still valid-looking JSON in most positions
    std::fs::write(&path, bytes).expect("tamper");

    match loader(&f.t2).load_t2(&f.key) {
        Err(LoadError::Tampered) => {}
        other => panic!("expected Tampered, got {other:?}"),
    }
    assert!(!path.exists(), "the tampered entry is deleted so it is rebuilt from T3");
}

#[test]
fn loader_builds_the_task_spec_through_its_callback() {
    let f = t2_with_entry();
    let spec = loader(&f.t2).build_spec(&f.key);
    assert!(matches!(spec.trigger, Trigger::Build { .. }));
}

#[test]
fn loader_reports_an_unreadable_entry_as_an_io_error_and_has_no_reload_cost_estimate() {
    let f = t2_with_entry();
    let path = f.t2.path_for(&f.key, T2_BUNDLE);
    std::fs::remove_file(&path).expect("remove");
    std::fs::create_dir(&path).expect("a directory where the bundle should be");
    assert!(matches!(loader(&f.t2).load_t2(&f.key), Err(LoadError::Io(_))));
    assert_eq!(loader(&f.t2).reload_cost_hint(&f.key), None);
}

#[test]
fn loader_reports_a_signed_bundle_that_does_not_parse_as_corrupt() {
    let f = t2_with_entry();
    f.t2.write(&f.key, T2_BUNDLE, b"signed, but not a bundle")
        .expect("overwrite with a valid MAC");
    assert!(matches!(loader(&f.t2).load_t2(&f.key), Err(LoadError::Corrupt(_))));
}
