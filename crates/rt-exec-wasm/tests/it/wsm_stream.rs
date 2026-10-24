//! WSM-14: the response is streamed: the outcome is
//! decided by the head, body chunks reach the client while the guest still runs, the
//! `max_response_stream_bytes` cap and failures after the head abort the body, and a cleanly
//! streamed instance is reusable.

use crate::common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{prepared, Req, Rig};
use futures_util::StreamExt;
use rt_types::{Body, BodyError, KillReason, Outcome};
use tokio::task::LocalSet;

/// Each chunk with the time it arrived (since `t0`), then the terminal error if any.
async fn read_chunks(mut body: Body, t0: Instant) -> (Vec<(Bytes, Duration)>, Option<BodyError>) {
    let mut chunks = Vec::new();
    while let Some(item) = body.next().await {
        match item {
            Ok(c) => chunks.push((c, t0.elapsed())),
            Err(e) => return (chunks, Some(e)),
        }
    }
    (chunks, None)
}

fn text(chunks: &[(Bytes, Duration)]) -> String {
    chunks
        .iter()
        .map(|(c, _)| String::from_utf8_lossy(c).into_owned())
        .collect()
}

#[tokio::test]
async fn ac_wsm_14_first_chunk_arrives_before_the_guest_finishes() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let (_task, fut) = rig.start(&p, &Req::get("/?chunks=3&delay_ms=400"));
    LocalSet::new()
        .run_until(async move {
            let t0 = Instant::now();
            let Outcome::Ok(reply) = fut.await else {
                panic!("expected a response")
            };
            let head_at = t0.elapsed();
            assert_eq!(reply.status.as_u16(), 200);
            assert_eq!(
                reply.headers.get("x-count").map(|v| v.to_str().unwrap_or_default()),
                Some("1")
            );
            let (chunks, err) = read_chunks(reply.body, t0).await;
            assert!(err.is_none(), "{err:?}");
            assert_eq!(text(&chunks), "chunk-0\nchunk-1\nchunk-2\n");
            let first_at = chunks[0].1;
            let done_at = chunks.last().map(|c| c.1).unwrap_or_default();
            assert!(
                head_at < Duration::from_millis(350),
                "the head waited for the guest: {head_at:?}"
            );
            assert!(
                first_at < Duration::from_millis(350),
                "the first chunk waited for the guest: {first_at:?}"
            );
            assert!(
                done_at >= Duration::from_millis(800),
                "the guest pauses 2 x 400 ms: {done_at:?}"
            );
        })
        .await;
}

#[tokio::test]
async fn ac_wsm_14_stream_past_the_size_limit_aborts_the_body() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let req = Req::get("/?chunks=5&delay_ms=30&size=1000").max_stream_bytes(2500);
    let (_task, fut) = rig.start(&p, &req);
    LocalSet::new()
        .run_until(async move {
            let t0 = Instant::now();
            let Outcome::Ok(reply) = fut.await else {
                panic!("the head is sent before the limit is hit")
            };
            let (chunks, err) = read_chunks(reply.body, t0).await;
            let sent: usize = chunks.iter().map(|(c, _)| c.len()).sum();
            assert!(sent <= 2500, "{sent} bytes passed a 2500-byte limit");
            assert!(matches!(err, Some(BodyError::TooLarge { limit: 2500 })), "{err:?}");
        })
        .await;
    assert!(
        rig.log_records()
            .iter()
            .any(|r| r.msg.contains("response exceeds the size limit")),
        "{:?}",
        rig.log_records()
    );
}

#[tokio::test]
async fn ac_wsm_14_buffered_or_streamed_a_body_never_passes_the_limit() {
    // `hello /echo` may finish within one run (buffered path) or stream: either way no more than
    // the limit reaches the client and the response does not complete as a success.
    let mut rig = Rig::new();
    let p = prepared("hello");
    let (_task, fut) = rig.start(&p, &Req::put("/echo", &[b'a'; 3000]).max_stream_bytes(1000));
    LocalSet::new()
        .run_until(async move {
            match fut.await {
                Outcome::GuestError => {}
                Outcome::Ok(reply) => {
                    let (chunks, err) = read_chunks(reply.body, Instant::now()).await;
                    assert!(chunks.iter().map(|(c, _)| c.len()).sum::<usize>() <= 1000);
                    assert!(matches!(err, Some(BodyError::TooLarge { .. })), "{err:?}");
                }
                other => panic!("{other:?}"),
            }
        })
        .await;
}

#[tokio::test]
async fn ac_wsm_14_trap_after_the_head_aborts_the_body_and_drops_the_instance() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let (_task, fut) = rig.start(&p, &Req::get("/trap?delay_ms=50").reuse());
    LocalSet::new()
        .run_until(async move {
            let Outcome::Ok(reply) = fut.await else {
                panic!("the head is sent before the trap")
            };
            let (chunks, err) = read_chunks(reply.body, Instant::now()).await;
            assert_eq!(text(&chunks), "chunk-0\n");
            assert!(matches!(err, Some(BodyError::Aborted(_))), "{err:?}");
        })
        .await;
    // the trapped instance is not reused
    let next = rig.run(&p, Req::get("/?chunks=1").reuse()).await.reply();
    assert_eq!(
        next.headers.get("x-count").map(|v| v.to_str().unwrap_or_default()),
        Some("1")
    );
}

#[tokio::test]
async fn ac_wsm_14_kill_mid_stream_aborts_the_body() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let (task, fut) = rig.start(&p, &Req::get("/?chunks=20&delay_ms=100"));
    let killer = rig.kill_later(task, Duration::from_millis(250), KillReason::CpuLimit);
    LocalSet::new()
        .run_until(async move {
            let Outcome::Ok(reply) = fut.await else {
                panic!("the head is sent before the kill")
            };
            let (chunks, err) = read_chunks(reply.body, Instant::now()).await;
            assert!(!chunks.is_empty() && chunks.len() < 20, "{} chunks", chunks.len());
            assert!(matches!(err, Some(BodyError::Aborted(_))), "{err:?}");
        })
        .await;
    killer.join().expect("killer thread");
    assert!(
        rig.log_records().iter().any(|r| r.msg.contains("task killed")),
        "{:?}",
        rig.log_records()
    );
}

#[tokio::test]
async fn ac_wsm_14_a_cleanly_streamed_instance_is_reused() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let baseline = rig.governor.committed();
    let count = |r: &common::Reply| {
        r.headers
            .get("x-count")
            .map(|v| v.to_str().unwrap_or_default().to_string())
    };
    let a = rig.run(&p, Req::get("/?chunks=2&delay_ms=20").reuse()).await.reply();
    assert_eq!(
        (count(&a).as_deref(), a.text().as_str()),
        (Some("1"), "chunk-0\nchunk-1\n")
    );
    let b = rig.run(&p, Req::get("/?chunks=2&delay_ms=20").reuse()).await.reply();
    assert_eq!(
        count(&b).as_deref(),
        Some("2"),
        "the instance went back to the idle list before the body ended"
    );
    // without reuse nothing stays behind once the body ended
    let mut rig2 = Rig::new();
    let base2 = rig2.governor.committed();
    rig2.run(&p, Req::get("/?chunks=2&delay_ms=20")).await.reply();
    assert_eq!(
        rig2.governor.committed(),
        base2,
        "a streamed task must release its memory"
    );
    let _ = baseline;
}

// ---------------------------------------------------------------------------------------------
// WSM-18: CPU accounting of streamed responses
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_18_guest_cpu_before_the_head_runs_inside_the_task_future() {
    // The scheduler measures (and bills, and limits) the thread CPU of the task future's polls:
    // the guest must run there, not in a task the executor spawned.
    let mut rig = Rig::new();
    let p = prepared("clock");
    let (done, cpu) = rig.run_measured(&p, Req::get("/?iters=60000000")).await;
    done.reply();
    assert!(
        cpu >= Duration::from_millis(20),
        "only {cpu:?} of guest CPU was measured in the task future"
    );
}

#[tokio::test]
async fn ac_wsm_18_cpu_limit_applies_to_the_streamed_tail() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let (_task, fut) = rig.start(&p, &Req::get("/burn?iters=20000000000").cpu_ms(150));
    LocalSet::new()
        .run_until(async move {
            let t0 = Instant::now();
            let Outcome::Ok(reply) = fut.await else {
                panic!("the head is sent before the burn")
            };
            let (chunks, err) = read_chunks(reply.body, t0).await;
            assert_eq!(text(&chunks), "start\n");
            assert!(
                matches!(err, Some(BodyError::Aborted(ref r)) if r == "cpu_limit"),
                "{err:?}"
            );
            assert!(
                t0.elapsed() < Duration::from_secs(5),
                "the tail ran {:?} past a 150 ms CPU limit",
                t0.elapsed()
            );
        })
        .await;
}

#[tokio::test]
async fn ac_wsm_18_tail_cpu_is_charged_to_the_tenant_when_the_stream_ends() {
    // With reuse on the instance goes back to the idle list: the task's host handle must not stay
    // with it, or the usage would only be recorded when the instance is reused or dropped.
    let mut rig = Rig::new();
    let p = prepared("stream");
    let svc = rt_types::ServiceId::from_u128(41);
    let reply = rig
        .run(&p, Req::get("/burn?iters=150000000").service(41).reuse())
        .await
        .reply();
    assert_eq!(reply.text(), "start\nend\n");
    assert_eq!(rig.exec.idle_instances(), 1, "the instance was kept");
    let charged = rig.control.cpu_ms(svc);
    assert!(charged >= 20, "the tail's CPU was not charged: {charged} ms");
}

#[tokio::test]
async fn ac_wsm_14_a_guest_returning_with_an_unfinished_body_fails_and_is_not_reused() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let r = rig.run(&p, Req::get("/unfinished").reuse()).await;
    match r.res {
        common::Res::GuestError => {}
        common::Res::Ok(reply) => panic!("a truncated response looked successful: {:?}", reply.text()),
        other => panic!("{other:?}"),
    }
    assert!(
        rig.log_records().iter().any(|l| l.msg.contains("response body failed")),
        "{:?}",
        rig.log_records()
    );
    let next = rig.run(&p, Req::get("/?chunks=1").reuse()).await.reply();
    assert_eq!(
        next.headers.get("x-count").map(|v| v.to_str().unwrap_or_default()),
        Some("1"),
        "reused after a failure"
    );
}

// ---------------------------------------------------------------------------------------------
// Responses that go wrong, and the output gate of a request to a durable object
// ---------------------------------------------------------------------------------------------

fn logged(rig: &Rig, needle: &str) -> bool {
    rig.log_records().iter().any(|l| l.msg.contains(needle))
}

#[tokio::test]
async fn a_guest_that_drops_its_body_unfinished_fails_the_task() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let r = rig.run(&p, Req::get("/abort").reuse()).await;
    r.expect_guest_error();
    assert!(logged(&rig, "response body failed"), "{:?}", rig.log_records());
    let next = rig.run(&p, Req::get("/?chunks=1").reuse()).await.reply();
    assert_eq!(
        next.headers.get("x-count").map(|v| v.to_str().unwrap_or_default()),
        Some("1"),
        "not reused"
    );
}

#[tokio::test]
async fn a_streamed_body_the_guest_drops_unfinished_ends_in_a_body_error_after_the_head() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let (_task, fut) = rig.start(&p, &Req::get("/abort?delay_ms=50"));
    LocalSet::new()
        .run_until(async move {
            let Outcome::Ok(reply) = fut.await else {
                panic!("the head is sent before the body fails")
            };
            let (chunks, err) = read_chunks(reply.body, Instant::now()).await;
            assert_eq!(text(&chunks), "chunk-0\n");
            assert!(matches!(err, Some(BodyError::Aborted(_))), "{err:?}");
        })
        .await;
    assert!(logged(&rig, "response body failed"));
}

#[tokio::test]
async fn a_guest_that_answers_with_an_http_error_or_not_at_all_fails_the_task() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    rig.run(&p, Req::get("/error")).await.expect_guest_error();
    assert!(logged(&rig, "guest returned an http error"), "{:?}", rig.log_records());
    rig.run(&p, Req::get("/noresponse")).await.expect_guest_error();
    assert!(
        logged(&rig, "guest returned without sending a response"),
        "{:?}",
        rig.log_records()
    );
}

#[tokio::test]
async fn a_client_that_hangs_up_does_not_stop_the_guest_and_its_later_trap_still_fails_the_task() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let (_task, fut) = rig.start(&p, &Req::get("/trap?before=3&delay_ms=60"));
    LocalSet::new()
        .run_until(async move {
            let Outcome::Ok(mut reply) = fut.await else {
                panic!("the head is sent before the trap")
            };
            let first = reply.body.next().await.expect("a chunk").expect("not an error");
            assert_eq!(&first[..], b"chunk-0\n");
            drop(reply); // the client is gone; the guest has two more chunks and a trap to go
            tokio::time::sleep(Duration::from_millis(400)).await;
        })
        .await;
    assert!(logged(&rig, "guest trapped"), "{:?}", rig.log_records());
}

#[tokio::test]
async fn an_object_task_holds_its_response_until_the_run_ended_and_then_answers_it_whole() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let r = rig
        .run(&p, Req::get("/?chunks=3&delay_ms=30").object("o"))
        .await
        .reply();
    assert_eq!(r.text(), "chunk-0\nchunk-1\nchunk-2\n");
    assert_eq!(
        r.headers.get("x-count").map(|v| v.to_str().unwrap_or_default()),
        Some("1")
    );
}

#[tokio::test]
async fn an_object_task_whose_response_passes_the_size_limit_fails_whether_it_streamed_slowly_or_fast() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    for delay in [30, 0] {
        let req = Req::get(&format!("/?chunks=5&delay_ms={delay}&size=1000"))
            .object("o")
            .max_stream_bytes(2500);
        rig.run(&p, req).await.expect_guest_error();
        assert!(
            logged(&rig, "response exceeds the size limit"),
            "delay {delay}: {:?}",
            rig.log_records()
        );
    }
}

#[tokio::test]
async fn an_object_task_whose_body_fails_or_never_ends_fails_instead_of_answering() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    for path in ["/abort", "/abort?delay_ms=50", "/unfinished"] {
        rig.run(&p, Req::get(path).object("o")).await.expect_guest_error();
    }
    assert!(logged(&rig, "response body failed"), "{:?}", rig.log_records());
}

#[tokio::test]
async fn a_guest_that_keeps_running_after_dropping_or_failing_its_outparam_still_fails_the_task() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    rig.run(&p, Req::get("/error?delay_ms=50")).await.expect_guest_error();
    assert!(logged(&rig, "guest returned an http error"), "{:?}", rig.log_records());
    rig.run(&p, Req::get("/noresponse?delay_ms=50"))
        .await
        .expect_guest_error();
    assert!(
        logged(&rig, "guest returned without sending a response"),
        "{:?}",
        rig.log_records()
    );
}

#[tokio::test]
async fn a_streamed_guest_that_returns_holding_its_body_fails_the_task() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    let (_task, fut) = rig.start(&p, &Req::get("/unfinished?delay_ms=50"));
    LocalSet::new()
        .run_until(async move {
            let Outcome::Ok(reply) = fut.await else {
                panic!("the head is sent while the guest pauses")
            };
            let (chunks, err) = read_chunks(reply.body, Instant::now()).await;
            assert_eq!(text(&chunks), "chunk-0\n");
            assert!(matches!(err, Some(BodyError::Aborted(_))), "{err:?}");
        })
        .await;
    assert!(logged(&rig, "without finishing it"), "{:?}", rig.log_records());
}

#[tokio::test]
async fn a_chunk_written_in_the_guests_last_run_still_counts_against_the_size_limit() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    // chunk-0 streams during the pause; chunk-1 is written just before the guest returns.
    let req = Req::get("/?chunks=2&delay_ms=50&size=1000").max_stream_bytes(1500);
    let (_task, fut) = rig.start(&p, &req);
    LocalSet::new()
        .run_until(async move {
            let Outcome::Ok(reply) = fut.await else {
                panic!("the head is sent first")
            };
            let (_, err) = read_chunks(reply.body, Instant::now()).await;
            assert!(matches!(err, Some(BodyError::TooLarge { limit: 1500 })), "{err:?}");
        })
        .await;
    // The same for a request to an object, which holds the response until the run ended.
    rig.run(&p, req_gated()).await.expect_guest_error();
}

fn req_gated() -> Req {
    Req::get("/?chunks=2&delay_ms=50&size=1000")
        .object("o")
        .max_stream_bytes(1500)
}

#[tokio::test]
async fn an_object_task_that_returns_holding_its_body_after_the_head_went_out_fails() {
    let mut rig = Rig::new();
    let p = prepared("stream");
    rig.run(&p, Req::get("/unfinished?delay_ms=50").object("o"))
        .await
        .expect_guest_error();
    assert!(logged(&rig, "without finishing it"), "{:?}", rig.log_records());
}
