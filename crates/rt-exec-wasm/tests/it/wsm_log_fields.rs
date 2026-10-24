//! WSM-31: `std:host/log.log-fields` from a component (the `stdout_log` fixture's
//! `/fields` route): the record carries the fields, the task id and the level; a field object over
//! the 32-key cap is refused with `validation_failed` and writes no record.

use crate::common;

use common::*;
use rt_obs::LogLevel;

#[tokio::test]
async fn ac_wsm_31_log_fields_write_a_record_with_fields_and_refuse_too_many() {
    let mut rig = Rig::new();
    let p = prepared("stdout_log");
    let done = rig.run(&p, Req::get("/fields?level=warn&msg=structured&n=3")).await;
    let task = done.task;
    assert_eq!(done.text(), "logged");

    let records = rig.log_records();
    let rec = records
        .iter()
        .find(|r| r.msg == "structured")
        .expect("structured record");
    assert_eq!(rec.level, LogLevel::Warn);
    assert_eq!(rec.task_id, Some(task));
    assert_eq!(rec.fields.as_ref().map(|f| f.len()), Some(3));
    assert_eq!(
        rec.fields.as_ref().and_then(|f| f.get("k2")).and_then(|v| v.as_u64()),
        Some(2)
    );

    let refused = rig.run(&p, Req::get("/fields?msg=refused&n=33")).await.reply();
    assert_eq!(refused.status, 500);
    assert_eq!(refused.text(), "log_error:validation_failed");
    assert!(
        rig.log_records().iter().all(|r| r.msg != "refused"),
        "a refused call writes nothing"
    );
}
