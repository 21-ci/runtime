//! `std:host/kv.cas`, `kv.expire` and `kv.ttl` from a component, the `kv` fixture's `/cas`,
//! `/expire` and `/ttl` routes: a swap from concurrent tasks lets exactly one win, and the
//! expiry calls read and clear the TTL like the Deno SDK does.

use crate::common;

use common::*;

#[tokio::test]
async fn ac_wsm_30_concurrent_cas_from_many_tasks_exactly_one_wins() {
    let mut rig = Rig::new();
    let kv = prepared("kv");
    let tasks = (0..6)
        .map(|i| (kv.clone(), Req::get(&format!("/cas?key=lock&next=owner-{i}"))))
        .collect();
    let wins = rig
        .run_all(tasks)
        .await
        .into_iter()
        .map(|(done, _)| done.text())
        .filter(|t| t == "true")
        .count();
    assert_eq!(wins, 1, "exactly one swap succeeds");
}

#[tokio::test]
async fn ac_wsm_30_cas_compares_then_swaps_and_absent_next_deletes() {
    let mut rig = Rig::new();
    let kv = prepared("kv");
    macro_rules! go {
        ($path:expr) => {
            rig.run(&kv, Req::get($path)).await.text()
        };
    }

    assert_eq!(go!("/cas?key=c&expected=nope"), "false", "expected present, key absent");
    assert_eq!(go!("/cas?key=c&next=v1"), "true");
    assert_eq!(go!("/cas?key=c&expected=v1&next=v2"), "true");
    assert_eq!(go!("/cas?key=c&expected=v1&next=v3"), "false");
    assert_eq!(go!("/get?key=c"), "v2", "a refused swap writes nothing");
    assert_eq!(go!("/cas?key=c&expected=v2"), "true", "absent next deletes");
    assert_eq!(go!("/cas?key=c&expected=v2"), "false");
}

#[tokio::test]
async fn ac_wsm_30_expire_and_ttl_read_and_clear_the_expiry() {
    let mut rig = Rig::new();
    let kv = prepared("kv");
    macro_rules! go {
        ($path:expr) => {
            rig.run(&kv, Req::get($path)).await.text()
        };
    }

    assert_eq!(go!("/ttl?key=e"), "none", "missing key");
    assert_eq!(go!("/expire?key=e&ttl=60"), "false", "missing key");
    assert_eq!(go!("/cas?key=e&next=v"), "true");
    assert_eq!(go!("/ttl?key=e"), "none", "no expiry yet");
    assert_eq!(go!("/expire?key=e&ttl=60"), "true");
    let left: u64 = go!("/ttl?key=e").parse().expect("seconds left");
    assert!((55..=60).contains(&left), "{left}");
    assert_eq!(go!("/expire?key=e"), "true", "no ttl clears the expiry");
    assert_eq!(go!("/ttl?key=e"), "none");
}
