//! WSM-22: `std:host/kv.incr` from a component, the `kv` fixture's `/incr` route.
//! Increments of concurrent tasks lose nothing; TTL, refusals and their codes are the Deno SDK's.
//! WSM-23: the `x-rt-client-ip` request header ingress sets reaches the component unchanged (the
//! `hello` fixture's `/header/{name}` route); ingress itself is covered by rt-api and rt-server.

use crate::common;

use std::time::Duration;

use common::*;

#[tokio::test]
async fn ac_wsm_22_increments_from_concurrent_tasks_lose_nothing() {
    let mut rig = Rig::new();
    let kv = prepared("kv");
    let tasks = (0..6).map(|_| (kv.clone(), Req::get("/incr?key=hits&n=20"))).collect();
    let mut lasts: Vec<i64> = rig
        .run_all(tasks)
        .await
        .into_iter()
        .map(|(done, _)| done.text().parse().expect("a counter value"))
        .collect();
    lasts.sort_unstable();
    lasts.dedup();
    assert_eq!(lasts.len(), 6, "every task ended on a different value: {lasts:?}");
    assert_eq!(lasts.last(), Some(&120), "6 tasks x 20 increments");

    // The counter is JSON text for `get`, and negative deltas work.
    assert_eq!(rig.run(&kv, Req::get("/get?key=hits")).await.text(), "120");
    assert_eq!(rig.run(&kv, Req::get("/incr?key=hits&delta=-121")).await.text(), "-1");
}

#[tokio::test]
async fn ac_wsm_22_ttl_on_creation_and_stable_error_codes() {
    let mut rig = Rig::new();
    let kv = prepared("kv");
    let text = |done: Done| {
        let r = done.reply();
        (r.status, r.text())
    };

    assert_eq!(
        text(rig.run(&kv, Req::get("/incr?key=win&ttl=1")).await),
        (200, "1".into())
    );
    assert_eq!(
        text(rig.run(&kv, Req::get("/incr?key=win&ttl=3600")).await),
        (200, "2".into()),
        "the expiry is kept"
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        rig.run(&kv, Req::get("/get?key=win")).await.reply().status,
        404,
        "the creating call's TTL holds"
    );
    assert_eq!(
        text(rig.run(&kv, Req::get("/incr?key=win")).await),
        (200, "1".into()),
        "an expired counter starts over"
    );

    rig.run(&kv, Req::get("/set?key=raw&value=7")).await.reply();
    assert_eq!(
        text(rig.run(&kv, Req::get("/incr?key=raw")).await),
        (500, "kv_error:kv_not_integer".into())
    );
    assert_eq!(
        rig.run(&kv, Req::get("/get?key=raw")).await.text(),
        "7",
        "a refused value is unchanged"
    );

    let max = i64::MAX.to_string();
    assert_eq!(
        text(rig.run(&kv, Req::get(&format!("/incr?key=top&delta={max}"))).await),
        (200, max.clone())
    );
    assert_eq!(
        text(rig.run(&kv, Req::get("/incr?key=top")).await),
        (500, "kv_error:kv_overflow".into())
    );
    assert_eq!(
        text(rig.run(&kv, Req::get("/incr?key=")).await),
        (500, "kv_error:validation_failed".into())
    );
}

#[tokio::test]
async fn ac_wsm_23_the_client_ip_header_reaches_the_component() {
    let mut rig = Rig::new();
    let hello = prepared("hello");
    let got = rig
        .run(
            &hello,
            Req::get("/header/x-rt-client-ip").header("x-rt-client-ip", "2001:db8::9"),
        )
        .await
        .reply();
    assert_eq!((got.status, got.text().as_str()), (200, "2001:db8::9"));
    let got = rig.run(&hello, Req::get("/header/x-rt-client-ip")).await.reply();
    assert_eq!(
        (got.status, got.text().as_str()),
        (404, "absent"),
        "no address known: no header"
    );
}
