//! WSM-35: `std:host/schedule` from a component (the `hello` fixture's `/schedule`
//! route). `after` answers the id and due time, `list` and `cancel` act on the pending schedules, and an
//! id that is not a schedule id is refused with `validation_failed`.

use crate::common;

use common::*;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

#[tokio::test]
async fn ac_wsm_35_after_list_and_cancel_manage_the_pending_schedules() {
    let mut rig = Rig::new();
    let p = prepared("hello");
    let made = rig
        .run(&p, Req::get("/schedule?op=after&seconds=60&id=digest"))
        .await
        .text();
    let (id, run_at) = made.split_once('|').expect("id|runAt");
    assert_eq!(id, "digest");
    let run_at: i64 = run_at.parse().expect("runAt");
    assert!(
        (now_ms() + 59_000..=now_ms() + 61_000).contains(&run_at),
        "due in about 60 s: {run_at}"
    );

    let generated = rig.run(&p, Req::get("/schedule?op=after&seconds=1")).await.text();
    assert!(generated.starts_with("sch_"), "{generated}");

    let listed = rig.run(&p, Req::get("/schedule?op=list")).await.text();
    let ids: Vec<&str> = listed.split(',').collect();
    assert_eq!(ids.len(), 2, "both pending: {listed}");
    assert_eq!(ids[0], "digest", "pending in id order");
    assert!(ids[1].starts_with("sch_"), "{listed}");

    assert_eq!(
        rig.run(&p, Req::get("/schedule?op=cancel&id=digest")).await.text(),
        "true"
    );
    assert_eq!(
        rig.run(&p, Req::get("/schedule?op=cancel&id=digest")).await.text(),
        "false"
    );
}

#[tokio::test]
async fn ac_wsm_35_an_id_that_is_not_a_schedule_id_is_validation_failed() {
    let mut rig = Rig::new();
    let p = prepared("hello");
    assert_eq!(
        rig.run(&p, Req::get("/schedule?op=after&seconds=1&id=bad%20id"))
            .await
            .text(),
        "error:validation_failed"
    );
}
