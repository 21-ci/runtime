//! The T2 -> T1 path of a WASM artifact: `WasmPrecompiler` makes the `.cwasm` rt-build stores,
//! `WasmLoader` maps it back only after its MAC checked out, and says why when it cannot.

use crate::common;

use std::sync::{Arc, Mutex};

use common::{fixture_bytes, shared};
use rt_build::ComponentPrecompiler;
use rt_cache::{LoadError, Loader, NoTamperSink, T2Store};
use rt_exec_wasm::{engine_fingerprint, WasmLoader, WasmPrecompiler, T2_CWASM};
use rt_sec::{HkdfArtifactMac, MasterKey};
use rt_testkit::SpecBuilder;
use rt_types::{BuildFailReason, Digest, PreparedKey, RuntimeKind};

struct Rigging {
    _dir: tempfile::TempDir,
    t2: Arc<T2Store>,
    loader: WasmLoader,
    key: PreparedKey,
    built_for: Arc<Mutex<Vec<PreparedKey>>>,
}

fn rigging() -> Rigging {
    let engine = shared().engine.clone();
    let dir = tempfile::tempdir().expect("temp dir");
    let mac = Arc::new(HkdfArtifactMac::new(&MasterKey::from_bytes([7; 32])));
    let t2 = Arc::new(T2Store::new(dir.path(), mac, Arc::new(NoTamperSink)));
    let built_for = Arc::new(Mutex::new(Vec::new()));
    let seen = built_for.clone();
    let spec = Arc::new(move |k: &PreparedKey| {
        seen.lock().expect("lock").push(*k);
        SpecBuilder::build().finish()
    });
    let bytes = fixture_bytes("hello");
    let key = PreparedKey {
        digest: Digest::of(&bytes),
        kind: RuntimeKind::Wasm,
        engine_fp: engine_fingerprint(&engine),
    };
    Rigging {
        loader: WasmLoader::new(engine, t2.clone(), spec),
        t2,
        _dir: dir,
        key,
        built_for,
    }
}

fn precompiled() -> Vec<u8> {
    let p = WasmPrecompiler::new(shared().engine.clone(), 0);
    p.precompile(&fixture_bytes("hello")).expect("precompile")
}

#[tokio::test]
async fn a_precompiled_artifact_in_t2_loads_into_a_prepared_value_that_serves_requests() {
    let r = rigging();
    let cwasm = precompiled();
    r.t2.write(&r.key, T2_CWASM, &cwasm).expect("write");
    let (prepared, bytes) = r.loader.load_t2(&r.key).expect("load").expect("present");
    assert_eq!(bytes, cwasm.len() as u64);
    assert_eq!(prepared.key, r.key);
    let reply = common::Rig::new()
        .run(&Arc::new(prepared), common::Req::get("/"))
        .await
        .reply();
    assert_eq!(reply.text(), "hello wasm");
}

#[test]
fn an_artifact_that_is_not_in_t2_is_none_so_the_cache_builds_it() {
    let r = rigging();
    assert!(r.loader.load_t2(&r.key).expect("load").is_none());
}

#[test]
fn a_modified_artifact_is_refused_before_it_is_mapped() {
    let r = rigging();
    r.t2.write(&r.key, T2_CWASM, &precompiled()).expect("write");
    let mut file = std::fs::read(r.t2.path_for(&r.key, T2_CWASM)).expect("read");
    file[100] ^= 0xff;
    std::fs::write(r.t2.path_for(&r.key, T2_CWASM), file).expect("tamper");
    assert!(matches!(r.loader.load_t2(&r.key), Err(LoadError::Tampered)));
}

#[test]
fn a_signed_file_that_is_not_a_compiled_component_is_corrupt() {
    let r = rigging();
    r.t2.write(&r.key, T2_CWASM, b"signed, but not a cwasm").expect("write");
    match r.loader.load_t2(&r.key) {
        Err(LoadError::Corrupt(msg)) => assert!(msg.contains("deserialize"), "{msg}"),
        other => panic!("expected Corrupt, got {:?}", other.map(|_| ())),
    }
}

#[test]
fn an_unreadable_artifact_is_an_io_error() {
    let r = rigging();
    r.t2.write(&r.key, T2_CWASM, &precompiled()).expect("write");
    let path = r.t2.path_for(&r.key, T2_CWASM);
    std::fs::remove_file(&path).expect("remove");
    std::fs::create_dir(&path).expect("a directory where the file should be");
    assert!(matches!(r.loader.load_t2(&r.key), Err(LoadError::Io(_))));
}

#[test]
fn the_loader_asks_the_server_for_the_build_task_and_has_no_reload_cost_estimate() {
    let r = rigging();
    let _spec = r.loader.build_spec(&r.key);
    assert_eq!(r.built_for.lock().expect("lock").as_slice(), [r.key]);
    assert_eq!(r.loader.reload_cost_hint(&r.key), None);
}

#[test]
fn the_precompiler_reports_its_engine_fingerprint_and_refuses_what_is_not_a_component() {
    let p = WasmPrecompiler::new(shared().engine.clone(), 0xfeed);
    assert_eq!(p.engine_fp(), 0xfeed);
    let err = p.precompile(b"not wasm at all").expect_err("garbage");
    assert_eq!(err.reason, BuildFailReason::Internal);
    assert!(err.message.contains("precompile"), "{}", err.message);
}
