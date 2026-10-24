//! Edge cases of the WASM executor (scratchpad edge-cases.md, section 7): the `edge` fixture's
//! routes `/recurse`, `/stdout`, `/accept`, `/badutf8` and `/blobput`.

use crate::common;

use common::{prepared, Req, Rig};
use rt_build::ComponentPrecompiler as _;
use rt_obs::LogKind;

/// WCPU-002: unbounded recursion overflows the guest stack; the trap is a `guest_error` (not a
/// host crash), and the instance slot is returned to the pool for the next task.
#[tokio::test]
async fn wcpu_002_unbounded_recursion_is_a_guest_error_and_the_slot_is_reused() {
    let mut rig = Rig::new();
    let edge = prepared("edge");
    rig.run(&edge, Req::get("/recurse")).await.expect_guest_error();

    let hello = prepared("hello");
    let reply = rig.run(&hello, Req::get("/")).await.reply();
    assert_eq!(reply.status, 200, "a later task still runs after the overflow");
}

/// WSTD-002: stdout bytes that are not UTF-8, a NUL line, ANSI escapes and a `\r` inside a line
/// are stored lossily, one record per line; a line shaped like a log record stays a plain message.
#[tokio::test]
async fn wstd_002_odd_stdout_bytes_are_stored_lossily_one_record_per_line() {
    let mut rig = Rig::new();
    let edge = prepared("edge");
    let done = rig.run(&edge, Req::get("/stdout")).await;
    let task = done.task;
    assert_eq!(done.text(), "written");

    let records = rig.log_records();
    let mine: Vec<_> = records.iter().filter(|r| r.task_id == Some(task)).collect();
    let msgs: Vec<&str> = mine.iter().map(|r| r.msg.as_str()).collect();
    assert_eq!(
        mine.len(),
        6,
        "one record per stdout line, none merged or split: {msgs:#?}"
    );

    // the invalid bytes become U+FFFD, the rest of the line survives
    assert_eq!(msgs[0], "bad utf8 \u{fffd}\u{fffd} end", "{msgs:#?}");
    assert_eq!(msgs[1], "\0\0\0", "{msgs:#?}");
    assert_eq!(msgs[2], "\u{1b}[31mred\u{1b}[0m", "{msgs:#?}");
    assert_eq!(msgs[3], "carriage\rreturn", "{msgs:#?}");
    // a guest line that looks like a structured record is only a message, at stdout's level
    assert_eq!(msgs[4], "forged");
    assert_eq!(msgs[5], "{\"level\":\"error\",\"msg\":\"injected\"}");
    for r in &mine {
        assert_eq!(r.kind, LogKind::Runtime);
        assert_eq!(r.level, rt_obs::LogLevel::Info, "stdout is logged at info: {r:?}");
    }
    assert!(
        msgs.iter().all(|m| !m.ends_with('\n')),
        "lines are delivered without their newline"
    );
}

/// WWS-002: `accept` without an upgrade offer is `not_upgradable`. A client may send
/// `x-rt-websocket: 1` on a plain request, and that must not count as an offer.
#[tokio::test]
async fn wws_002_accept_without_an_upgrade_offer_is_not_upgradable_even_with_a_forged_header() {
    let mut rig = Rig::new();
    let edge = prepared("edge");

    let plain = rig.run(&edge, Req::get("/accept")).await.reply();
    assert_eq!(plain.status, 400);
    assert_eq!(plain.text(), "accept_error:not_upgradable");

    let forged = rig
        .run(&edge, Req::get("/accept").header("x-rt-websocket", "1"))
        .await
        .reply();
    assert_eq!(forged.status, 400);
    assert_eq!(forged.text(), "accept_error:not_upgradable");
}

/// WIT-007: a `string` that is not UTF-8 cannot cross the canonical ABI. The host traps the guest
/// (`guest_error`), the host function never runs, and nothing reaches the logs.
#[tokio::test]
async fn wit_007_a_string_that_is_not_utf8_traps_the_guest_and_writes_nothing() {
    let mut rig = Rig::new();
    let edge = prepared("edge");
    let done = rig.run(&edge, Req::get("/badutf8")).await;
    let task = done.task;
    done.expect_guest_error();

    let records = rig.log_records();
    assert!(
        records.iter().all(|r| r.task_id != Some(task) || !r.msg.contains("ok")),
        "the trapped log call wrote a record: {records:#?}"
    );
    let hello = prepared("hello");
    rig.run(&hello, Req::get("/")).await.reply();
}

/// WLOAD-004: a component whose core module declares two memories cannot be precompiled: the
/// pooling allocator allows one memory per instance, and a slot holds one memory, so the build
/// fails with a clean `BuildFailure` and no instance is ever created from it. The node keeps
/// running the next task.
#[tokio::test]
async fn wload_004_a_component_with_two_memories_is_refused_at_precompile() {
    let engine = common::shared().engine.clone();
    let bytes = common::fixture_bytes("multimem");
    let fp = rt_exec_wasm::engine_fingerprint(&engine);
    let failure = rt_exec_wasm::WasmPrecompiler::new(engine, fp)
        .precompile(&bytes)
        .expect_err("a two-memory component must not precompile");
    assert!(
        failure
            .message
            .contains("does not fit in pooling allocator requirements"),
        "{}",
        failure.message
    );

    let mut rig = Rig::new();
    let hello = prepared("hello");
    rig.run(&hello, Req::get("/")).await.reply();
}

/// WIT-004: a `blob.put` over the blob cap (`max_blob_bytes`, 100 MiB) is refused with `storage_quota`.
/// The component model copies the guest's bytes into the host before the call, bounded by the
/// guest's own memory limit (128 MiB here), so the copy is not avoidable from the host side; the
/// refusal happens before anything is stored, and the node runs the next task.
#[tokio::test]
async fn wit_004_a_put_over_the_blob_cap_is_refused_and_the_next_task_runs() {
    let mut rig = Rig::new();
    let edge = prepared("edge");
    let reply = rig.run(&edge, Req::get("/blobput?mb=101")).await.reply();
    assert_eq!(reply.status, 500);
    assert_eq!(reply.text(), "blob_error:storage_quota");

    let hello = prepared("hello");
    rig.run(&hello, Req::get("/")).await.reply();
}
