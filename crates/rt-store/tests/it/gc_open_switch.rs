//! Version GC never takes what a flip in progress is about to make live: migrations may already
//! have run for it, and a target gone at the flip would leave old code on a new schema.

use crate::common;

use common::*;
use rt_store::MetaStore;
use rt_types::{NewSwitch, SwitchKind, SwitchPatch, SwitchState, VersionNo};

fn numbers(list: &[rt_types::Version]) -> Vec<u32> {
    list.iter().map(|v| v.number.0).collect()
}

#[tokio::test]
async fn the_target_of_an_open_switch_survives_gc_until_it_closes() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "gc").await;
    mk_versions(&m, s.id, 1, 6).await;
    m.set_live(s.id, VersionNo(6), None).await.expect("live");
    let sw = m
        .create_switch(
            s.id,
            NewSwitch {
                kind: SwitchKind::Promote,
                source: None,
                from_version: None,
                to_version: VersionNo(1),
                created_by: None,
            },
        )
        .await
        .expect("switch");

    m.gc_versions(s.id, 1).await.expect("gc");
    assert_eq!(numbers(&m.list_versions(s.id).await.expect("list")), vec![1, 6]);

    let closed = SwitchPatch {
        state: Some(SwitchState::Failed),
        ..SwitchPatch::default()
    };
    m.update_switch(s.id, sw.id, closed).await.expect("close");
    m.gc_versions(s.id, 1).await.expect("gc");
    assert_eq!(numbers(&m.list_versions(s.id).await.expect("list")), vec![6]);
}

#[tokio::test]
async fn a_version_waiting_to_go_live_after_its_build_survives_gc() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "gc").await;
    mk_versions(&m, s.id, 1, 4).await;
    m.set_version_deploy(s.id, VersionNo(2), None, Some("op".into()))
        .await
        .expect("deploy");

    m.gc_versions(s.id, 1).await.expect("gc");
    assert_eq!(numbers(&m.list_versions(s.id).await.expect("list")), vec![2, 4]);
}
