//! WSM-15: a WASM guest calls another service and enqueues through `std:host/call` and
//! `std:host/queue` (`/wit/rt-host.wit`). The requests reach `rt_host::Calls` (the dispatcher that
//! owns the access rules: `call.allowed_callers`, environments, plan triggers, cross-tenant and
//! depth) with the caller's identity; its errors reach the guest as stable codes.
//!
//! `common::MockCalls` stands in for the dispatcher and scripts answers by slug.

use crate::common;

use std::time::Duration;

use common::{prepared, Req, Rig};
use rt_types::ServiceId;

#[tokio::test]
async fn ac_wsm_15_guest_calls_a_service_of_its_tenant() {
    let mut rig = Rig::new();
    let p = prepared("calls");
    let reply = rig
        .run(&p, Req::get("/call?slug=billing&path=/charge?sku=a").service(3))
        .await
        .reply();
    assert_eq!(reply.kv("status"), "201");
    assert_eq!(reply.kv("reason"), "-");
    assert_eq!(reply.kv("body"), "billing got POST /charge?sku=a ping");

    let seen = rig.calls.calls.lock().clone();
    assert_eq!(seen.len(), 1);
    let c = &seen[0];
    assert_eq!(
        c.from,
        ServiceId::from_u128(3),
        "the call carries the caller's identity"
    );
    assert_eq!(
        (c.slug.as_str(), c.method.as_str(), c.path.as_str()),
        ("billing", "POST", "/charge?sku=a")
    );
    assert_eq!((c.x_from.as_deref(), c.body.as_slice()), (Some("calls"), &b"ping"[..]));
}

#[tokio::test]
async fn ac_wsm_15_call_failures_are_stable_codes() {
    let mut rig = Rig::new();
    let p = prepared("calls");
    let code = |r: common::Reply| r.kv("error");
    assert_eq!(
        code(rig.run(&p, Req::get("/call?slug=missing&path=/")).await.reply()),
        "not_found"
    );
    // the dispatcher's access decision (`call.allowed_callers` / environment) reaches the guest
    assert_eq!(
        code(rig.run(&p, Req::get("/call?slug=private&path=/")).await.reply()),
        "call_not_allowed"
    );
    assert_eq!(
        code(rig.run(&p, Req::get("/call?slug=big&path=/")).await.reply()),
        "response_too_large"
    );
    assert_eq!(
        code(rig.run(&p, Req::get("/call?slug=any&path=nope")).await.reply()),
        "invalid_request"
    );
    // past depth 15 the call is a 508 response, like the Deno SDK (TRG-15)
    let deep = rig.run(&p, Req::get("/call?slug=deep&path=/")).await.reply();
    assert_eq!(
        (deep.kv("status").as_str(), deep.kv("reason").as_str()),
        ("508", "call_depth")
    );
}

#[tokio::test]
async fn ac_wsm_15_a_call_past_the_callers_deadline_fails_without_reaching_the_dispatcher() {
    let mut rig = Rig::new();
    let p = prepared("calls");
    // a deadline that has already passed when the guest calls
    let r = rig.run(&p, Req::get("/call?slug=billing&path=/").wall_ms(0)).await;
    match r.res {
        common::Res::Ok(reply) => assert_eq!(reply.kv("error"), "call_failed"),
        common::Res::Killed(_) => {} // the wall deadline may hit the guest first
        other => panic!("{other:?}"),
    }
    assert!(rig.calls.calls.lock().is_empty());
}

#[tokio::test]
async fn ac_wsm_15_guest_enqueues_to_a_service_of_its_tenant() {
    let mut rig = Rig::new();
    let p = prepared("calls");
    let reply = rig
        .run(
            &p,
            Req::get("/enqueue?slug=worker&body=hello&delay=30&dedup=d1").service(4),
        )
        .await
        .reply();
    assert!(reply.kv("id").starts_with("msg_"), "{}", reply.text());

    let seen = rig.calls.enqueued.lock().clone();
    assert_eq!(seen.len(), 1);
    let e = &seen[0];
    assert_eq!(
        (e.from, e.slug.as_str(), e.body.as_slice()),
        (ServiceId::from_u128(4), "worker", &b"hello"[..])
    );
    assert_eq!(
        (e.opts.delay, e.opts.dedup_id.as_deref()),
        (Duration::from_secs(30), Some("d1"))
    );

    // defaults: no delay, no dedup id
    rig.run(&p, Req::get("/enqueue?slug=worker&body=x")).await.reply();
    let last = rig.calls.enqueued.lock().last().cloned().expect("second enqueue");
    assert_eq!((last.opts.delay, last.opts.dedup_id), (Duration::ZERO, None));
}

#[tokio::test]
async fn ac_wsm_15_enqueue_failures_are_stable_codes() {
    let mut rig = Rig::new();
    let p = prepared("calls");
    assert_eq!(
        rig.run(&p, Req::get("/enqueue?slug=missing&body=x"))
            .await
            .reply()
            .kv("error"),
        "not_found"
    );
    assert_eq!(
        rig.run(&p, Req::get("/enqueue?slug=full&body=x"))
            .await
            .reply()
            .kv("error"),
        "queue_full"
    );
}
