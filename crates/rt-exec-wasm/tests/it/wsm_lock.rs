//! WSM-34: `std:host/lock.acquire` from a component (the `hello` fixture's
//! `/lock` route). Callers of one key wait their turn, a wait past `wait-ms` is `lock_timeout`
//! while other keys stay free, and a lock the task ends holding is released when the task ends.

use crate::common;

use common::*;

#[tokio::test]
async fn ac_wsm_34_callers_wait_their_turn_and_a_late_one_times_out() {
    let mut rig = Rig::new();
    let p = prepared("hello");
    let runs = rig
        .run_all(vec![
            (p.clone(), Req::get("/lock?key=k&op=hold&hold=300")),
            (p.clone(), Req::get("/lock?key=k&op=hold&hold=0&wait=50")),
            (p.clone(), Req::get("/lock?key=other&op=hold&hold=0")),
        ])
        .await;
    let texts: Vec<String> = runs.into_iter().map(|(done, _)| done.text()).collect();
    assert_eq!(texts, vec!["held", "error:lock_timeout", "held"]);
}

#[tokio::test]
async fn ac_wsm_34_a_task_that_ends_holding_a_lock_releases_it() {
    let mut rig = Rig::new();
    let p = prepared("hello");
    assert_eq!(rig.run(&p, Req::get("/lock?key=k&op=leak")).await.text(), "leaked");
    assert_eq!(
        rig.run(&p, Req::get("/lock?key=k&wait=0")).await.text(),
        "held",
        "the ended task's lock is free"
    );
}

#[tokio::test]
async fn ac_wsm_34_invalid_key_is_validation_failed() {
    let mut rig = Rig::new();
    let p = prepared("hello");
    assert_eq!(
        rig.run(&p, Req::get("/lock?key=")).await.text(),
        "error:validation_failed"
    );
}
