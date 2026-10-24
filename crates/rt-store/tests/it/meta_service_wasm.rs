//! WSM-12: the per-service `wasm` block (`config.wasm.reuse_instances`) is stored with the
//! service and replaced as one block by `PATCH`.

use crate::common;

use common::*;
use rt_store::{MetaStore, TenantScope};
use rt_types::{ServicePatch, WasmServiceConfig};

#[tokio::test]
async fn ac_wsm_12_the_wasm_block_is_stored_and_patched() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;

    let mut ns = new_service("w");
    ns.config.wasm = WasmServiceConfig {
        reuse_instances: Some(true),
    };
    let created = m
        .create_service(TenantScope::Tenant(t.id), t.id, ns)
        .await
        .expect("create");
    let got = m
        .get_service(TenantScope::Operator, created.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(got.config.wasm.reuse_instances, Some(true));

    // another block's patch keeps the wasm block
    let p = ServicePatch {
        name: Some("renamed".into()),
        ..Default::default()
    };
    let kept = m
        .patch_service(TenantScope::Tenant(t.id), created.id, p, None)
        .await
        .expect("patch name");
    assert_eq!(kept.config.wasm.reuse_instances, Some(true));

    // the wasm block is replaced as a whole
    let p = ServicePatch {
        wasm: Some(WasmServiceConfig::default()),
        ..Default::default()
    };
    let cleared = m
        .patch_service(TenantScope::Tenant(t.id), created.id, p, None)
        .await
        .expect("patch wasm");
    assert_eq!(
        cleared.config.wasm,
        WasmServiceConfig::default(),
        "unset again: the profile decides"
    );
}
