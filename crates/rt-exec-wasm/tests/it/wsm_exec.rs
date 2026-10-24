//! WSM-01 .. WSM-04, WSM-06, WSM-07, WSM-11 (acceptance criteria) plus the executor
//! contract around limits: outcome mapping, memory limit, preemption, kill.
//!
//! The guests are the fixtures built by `scripts/build-fixtures.sh` into `fixtures/wasm/out`.
//! Tasks are driven through `rt_sched::Executor::start` on a `LocalSet` (see `common`).

use crate::common;

use std::time::{Duration, Instant};

use common::{prepared, run_once, Req, Res, Rig};
use rt_obs::LogKind;
use rt_types::KillReason;

// ---------------------------------------------------------------------------------------------
// WSM-01 hello + outcome mapping
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_01_hello_component_answers_200_with_expected_body() {
    let reply = run_once("hello", Req::get("/")).await.reply();
    assert_eq!(reply.status, 200);
    assert_eq!(reply.text(), "hello wasm");
    assert!(reply.headers.get("x-rt-reason").is_none(), "{:?}", reply.headers);
    assert_eq!(
        reply
            .headers
            .get("content-type")
            .map(|v| v.to_str().unwrap_or_default()),
        Some("text/plain")
    );
}

#[tokio::test]
async fn ac_wsm_01_request_body_and_guest_status_reach_the_host() {
    let mut rig = Rig::new();
    let hello = prepared("hello");
    // the request body is delivered to the guest
    let echoed = rig.run(&hello, Req::put("/echo", b"abc")).await.reply();
    assert_eq!(echoed.status, 200);
    assert_eq!(echoed.body.as_ref(), b"abc");
    // any status the guest picks is passed through, it is not a runtime outcome
    let teapot = rig.run(&hello, Req::get("/status/418")).await.reply();
    assert_eq!(teapot.status, 418);
}

#[tokio::test]
async fn wsm_guest_trap_is_500_guest_error_and_the_node_continues() {
    let mut rig = Rig::new();
    let hello = prepared("hello");
    let baseline = rig.governor.committed();

    let done = rig.run(&hello, Req::get("/trap")).await;
    match &done.res {
        Res::GuestError => {}
        other => panic!("expected GuestError, got {other:?}"),
    }
    // outcome mapping: 500 guest_error
    let outcome = rt_types::Outcome::GuestError;
    assert_eq!((outcome.http_status(), outcome.reason()), (500, Some("guest_error")));
    assert_eq!(
        rig.governor.committed(),
        baseline,
        "a trapped task must release its memory"
    );

    // the executor is still healthy
    assert_eq!(rig.run(&hello, Req::get("/")).await.text(), "hello wasm");
}

#[tokio::test]
async fn wsm_two_services_running_interleaved_do_not_share_state() {
    // Same component, two services, run concurrently on one Machine: each gets its own instance.
    let mut rig = Rig::new();
    let counter = prepared("counter");
    let results = rig
        .run_all(vec![
            (counter.clone(), Req::get("/").service(1)),
            (counter.clone(), Req::get("/").service(2)),
            (counter.clone(), Req::get("/").service(1)),
        ])
        .await;
    let texts: Vec<String> = results.into_iter().map(|(d, _)| d.text()).collect();
    assert_eq!(texts, vec!["1", "1", "1"]);
}

// ---------------------------------------------------------------------------------------------
// WSM-02 sandbox: no preopens, sockets denied
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_02_filesystem_has_no_preopens_and_socket_connect_is_denied() {
    let mut rig = Rig::new();
    let p = prepared("fs_socket");
    for path in ["/", "/?port=443", "/?port=22"] {
        let reply = rig.run(&p, Req::get(path)).await.reply();
        // the guest handled every failure itself: the request completes normally
        assert_eq!(reply.status, 200, "{path}: {}", reply.text());
        assert_eq!(reply.kv("preopens"), "0", "{path}");
        assert_eq!(reply.kv("fs_open"), "no-preopens", "{path}");
        assert!(reply.kv("std_fs").starts_with("denied:"), "{path}: {}", reply.text());
        let connect = reply.kv("connect");
        assert!(
            connect.starts_with("denied:"),
            "{path}: socket connect must be denied, got `{connect}`"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// WSM-03 memory limit
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_03_growing_past_memory_mb_is_502_oom_and_the_node_continues() {
    let mut rig = Rig::new();
    let grow = prepared("grow");
    let hello = prepared("hello");
    let baseline = rig.governor.committed();

    // 16 MB limit, the guest asks for 1 GiB
    let done = rig.run(&grow, Req::get("/").memory_mb(16)).await;
    let reason = done.killed();
    assert_eq!(reason, KillReason::Oom);
    assert_eq!((reason.http_status(), reason.reason()), (502, "oom"));
    assert_eq!(
        rig.governor.committed(),
        baseline,
        "committed memory returns to baseline after the kill"
    );

    // the node continues: the next request on the same executor is served
    assert_eq!(rig.run(&hello, Req::get("/")).await.text(), "hello wasm");

    // growth below the limit succeeds and is fully released afterwards
    let under = rig.run(&grow, Req::get("/?mb=8").memory_mb(32)).await;
    assert_eq!(under.text(), "grown 8");
    assert_eq!(rig.governor.committed(), baseline);
}

#[tokio::test]
async fn ac_wsm_03_governor_refusal_is_oom_too() {
    // Plenty of `memory_mb`, but the governor budget (32 MiB) cannot cover the growth.
    let mut rig = Rig::with(32, Duration::from_millis(1));
    let grow = prepared("grow");
    let baseline = rig.governor.committed();
    let reason = rig.run(&grow, Req::get("/?mb=64").memory_mb(128)).await.killed();
    assert_eq!(reason, KillReason::Oom);
    assert_eq!(rig.governor.committed(), baseline);
}

// ---------------------------------------------------------------------------------------------
// WSM-04 fresh instance per task
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_04_global_counter_is_one_on_every_request_in_balanced() {
    let mut rig = Rig::new();
    let counter = prepared("counter");
    for i in 0..3 {
        let text = rig.run(&counter, Req::get("/")).await.text();
        assert_eq!(text, "1", "request {i}: linear memory must not survive between tasks");
    }
}

// ---------------------------------------------------------------------------------------------
// WSM-06 std:host/kv
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_06_kv_value_round_trips_through_rt_host() {
    let mut rig = Rig::new();
    let kv = prepared("kv");

    // set and get in one request
    let rt = rig.run(&kv, Req::get("/roundtrip?key=k1&value=v1")).await.reply();
    assert_eq!((rt.status, rt.text().as_str()), (200, "v1"));

    // set in one task, get in another: the value is persisted in the service's data store
    let set = rig.run(&kv, Req::put("/set?key=k2", b"hello-kv")).await.reply();
    assert_eq!((set.status, set.text().as_str()), (200, "ok"));
    let got = rig.run(&kv, Req::get("/get?key=k2")).await.reply();
    assert_eq!((got.status, got.text().as_str()), (200, "hello-kv"));

    // missing key and delete
    assert_eq!(rig.run(&kv, Req::get("/get?key=nope")).await.reply().status, 404);
    assert_eq!(rig.run(&kv, Req::get("/delete?key=k2")).await.text(), "deleted");
    assert_eq!(rig.run(&kv, Req::get("/get?key=k2")).await.reply().status, 404);
}

#[tokio::test]
async fn ac_wsm_06_kv_is_isolated_per_service() {
    let mut rig = Rig::new();
    let kv = prepared("kv");
    rig.run(&kv, Req::put("/set?key=shared-name", b"secret-of-service-1").service(1))
        .await
        .reply();
    // service 2 (same component, same tenant) sees nothing under that key
    let other = rig.run(&kv, Req::get("/get?key=shared-name").service(2)).await.reply();
    assert_eq!(other.status, 404, "{}", other.text());
    let own = rig.run(&kv, Req::get("/get?key=shared-name").service(1)).await.reply();
    assert_eq!(own.text(), "secret-of-service-1");
}

// ---------------------------------------------------------------------------------------------
// WSM-07 stdout -> service logs
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_07_stdout_lines_appear_in_service_logs_with_task_id() {
    let mut rig = Rig::new();
    let p = prepared("stdout_log");
    let done = rig.run(&p, Req::get("/?lines=2")).await;
    let task = done.task;
    assert_eq!(done.text(), "logged");

    let records = rig.log_records();
    let stdout_lines: Vec<_> = records
        .iter()
        .filter(|r| r.msg.contains("fixture stdout line"))
        .collect();
    assert_eq!(stdout_lines.len(), 2, "records: {records:#?}");
    assert!(
        stdout_lines[0].msg.contains("fixture stdout line 1"),
        "{:?}",
        stdout_lines[0].msg
    );
    assert!(
        stdout_lines[1].msg.contains("fixture stdout line 2"),
        "{:?}",
        stdout_lines[1].msg
    );
    for r in &stdout_lines {
        assert_eq!(r.task_id, Some(task), "log line must be tagged with the task id");
        assert_eq!(r.kind, LogKind::Runtime);
        assert_eq!(r.trigger, "http");
    }
    // stderr is logged too (same tagging)
    let stderr_line = records
        .iter()
        .find(|r| r.msg.contains("fixture stderr line"))
        .expect("stderr line logged");
    assert_eq!(stderr_line.task_id, Some(task));
    // lines are delivered without the trailing newline
    assert!(!stdout_lines[0].msg.ends_with('\n'));
}

#[tokio::test]
async fn ac_wsm_07_log_lines_per_task_are_capped() {
    // limits.max_log_lines = 1000: the rest is dropped (and counted), the task still completes
    let mut rig = Rig::new();
    let p = prepared("stdout_log");
    let done = rig.run(&p, Req::get("/?lines=1500")).await;
    let task = done.task;
    assert_eq!(done.text(), "logged");
    let n = rig
        .log_records()
        .iter()
        .filter(|r| r.task_id == Some(task) && r.msg.contains("fixture stdout line"))
        .count();
    assert!(n <= 1000, "{n} guest log lines recorded for one task");
    assert!(n >= 900, "only {n} guest log lines recorded");
}

// ---------------------------------------------------------------------------------------------
// WSM-11 / SEC-22 clocks
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_wsm_11_wasi_clocks_do_not_advance_during_a_busy_loop() {
    let mut rig = Rig::with(512, Duration::from_millis(1));
    let p = prepared("clock");
    let started = Instant::now();
    let reply = rig.run(&p, Req::get("/?iters=150000000")).await.reply();
    let real = started.elapsed();

    // the busy loop really took time ...
    assert!(
        real >= Duration::from_millis(50),
        "the busy loop finished in {real:?}: fixture or machine too fast"
    );
    // ... but the guest saw a frozen clock (SECURITY.md: advances only at I/O / timer boundaries)
    assert_eq!(reply.kv("spin_mono_delta_ns"), "0");
    assert_eq!(reply.kv("spin_wall_delta_ns"), "0");
    assert_eq!(reply.kv("spin_std_instant_delta_ns"), "0");
}

#[tokio::test]
async fn ac_wsm_11_clocks_advance_after_an_outbound_call() {
    // The mock upstream takes 25 ms of real time.
    let mut rig = Rig::with(512, Duration::from_millis(25));
    let p = prepared("clock");
    let reply = rig
        .run(&p, Req::get("/?iters=1000000&fetch=example.test&path=/x"))
        .await
        .reply();

    assert_eq!(reply.kv("spin_mono_delta_ns"), "0");
    assert_eq!(reply.kv("fetch_status"), "200", "{}", reply.text());
    let mono: u128 = reply.kv("after_fetch_mono_delta_ns").parse().expect("mono delta");
    let wall: u128 = reply.kv("after_fetch_wall_delta_ns").parse().expect("wall delta");
    let ten_ms = 10_000_000u128;
    assert!(
        mono >= ten_ms,
        "monotonic clock advanced only {mono} ns across a 25 ms fetch"
    );
    assert!(
        wall >= ten_ms,
        "wall clock advanced only {wall} ns across a 25 ms fetch"
    );

    // the call went through the host's egress guard, to the URL the guest built
    let seen = rig.egress.requests.lock().clone();
    assert_eq!(seen, vec!["http://example.test/x".to_string()]);
}

// ---------------------------------------------------------------------------------------------
// Limits: preemption and kill (Interrupter contract)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn wsm_cpu_hog_is_preempted_so_other_tasks_make_progress() {
    // A finite hog (~0.5 s of pure computation) and a hello started right after it on the same
    // Machine. Epoch yields at every time slice must let hello finish long before the hog.
    // The hog gets a generous CPU budget: this test is about preemption, not
    // the CPU limit, and the hog's real cost depends on the machine (>1 s on a loaded one).
    let mut rig = Rig::new();
    let spin = prepared("spin");
    let hello = prepared("hello");
    let results = rig
        .run_all(vec![
            (spin, Req::get("/?iters=500000000").service(1).cpu_ms(20_000)),
            (hello, Req::get("/").service(2)),
        ])
        .await;
    let mut it = results.into_iter();
    let (hog, hog_done_at) = it.next().expect("hog");
    let (small, small_done_at) = it.next().expect("hello");
    assert_eq!(small.text(), "hello wasm");
    assert!(hog.text().starts_with("spun "));
    assert!(
        small_done_at < hog_done_at,
        "hello finished {:?} after the hog: the hog was not preempted at its time slice",
        small_done_at.saturating_duration_since(hog_done_at)
    );
}

#[tokio::test]
async fn wsm_interrupter_kill_ends_an_endless_loop_with_the_given_reason() {
    for (reason, status, header) in [
        (KillReason::CpuLimit, 502, "cpu_limit"),
        (KillReason::WallTimeout, 504, "timeout"),
        (KillReason::Shutdown, 503, "restarting"),
    ] {
        let mut rig = Rig::new();
        let spin = prepared("spin");
        let baseline = rig.governor.committed();
        let (task, fut) = rig.start(&spin, &Req::get("/"));
        let killer = rig.kill_later(task, Duration::from_millis(100), reason);

        let local = tokio::task::LocalSet::new();
        let started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(20), local.run_until(fut))
            .await
            .expect("a killed task must resolve promptly");
        killer.join().expect("killer thread");

        match outcome {
            rt_types::Outcome::Killed(k) => {
                assert_eq!(k, reason);
                assert_eq!((k.http_status(), k.reason()), (status, header));
            }
            other => panic!("expected Killed({reason:?}), got {other:?}"),
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "kill took {:?}",
            started.elapsed()
        );
        assert_eq!(
            rig.governor.committed(),
            baseline,
            "kill ({reason:?}) must release the task's memory"
        );
    }
}

#[tokio::test]
async fn wsm_kill_is_per_task_only() {
    // Killing the spinner must not disturb a task running next to it.
    let mut rig = Rig::new();
    let spin = prepared("spin");
    let hello = prepared("hello");
    let (spin_task, spin_fut) = rig.start(&spin, &Req::get("/").service(1));
    let (_hello_task, hello_fut) = rig.start(&hello, &Req::get("/").service(2));
    let killer = rig.kill_later(spin_task, Duration::from_millis(150), KillReason::CpuLimit);

    let local = tokio::task::LocalSet::new();
    let a = local.spawn_local(spin_fut);
    let b = local.spawn_local(hello_fut);
    let (spin_out, hello_out) = tokio::time::timeout(
        Duration::from_secs(20),
        local.run_until(async move { (a.await.expect("join spin"), b.await.expect("join hello")) }),
    )
    .await
    .expect("both tasks resolve");
    killer.join().expect("killer thread");

    assert!(
        matches!(spin_out, rt_types::Outcome::Killed(KillReason::CpuLimit)),
        "{spin_out:?}"
    );
    match hello_out {
        rt_types::Outcome::Ok(r) => assert_eq!(r.status.as_u16(), 200),
        other => panic!("hello was disturbed by the kill of another task: {other:?}"),
    }
}

#[tokio::test]
async fn ac_wsm_11_a_pollable_that_is_ready_at_once_does_not_advance_the_clock() {
    // `subscribe-duration(0)` + `block` between pieces of a busy loop never waits: if each such
    // "I/O boundary" caught the clock up with real time, the guest could time its own code.
    let mut rig = Rig::with(512, Duration::from_millis(1));
    let p = prepared("clock");
    let started = Instant::now();
    let reply = rig.run(&p, Req::get("/?iters=100000000&ready_polls=50")).await.reply();
    assert!(
        started.elapsed() >= Duration::from_millis(30),
        "the busy loop finished too fast to tell"
    );
    assert_eq!(
        reply.kv("spin_mono_delta_ns"),
        "0",
        "ready pollables advanced the monotonic clock"
    );
    assert_eq!(
        reply.kv("spin_wall_delta_ns"),
        "0",
        "ready pollables advanced the wall clock"
    );
}

#[tokio::test]
async fn ac_wsm_11_clock_readings_are_whole_milliseconds() {
    // SEC-22: 1 ms resolution is advertised, and that is all a guest can read.
    let mut rig = Rig::with(512, Duration::from_millis(3));
    let p = prepared("clock");
    let reply = rig
        .run(&p, Req::get("/?iters=1000&fetch=example.test&path=/x"))
        .await
        .reply();
    for key in [
        "start_mono_ns",
        "start_wall_ns",
        "after_fetch_mono_delta_ns",
        "after_fetch_wall_delta_ns",
    ] {
        let v: i128 = reply.kv(key).parse().unwrap_or_else(|e| panic!("{key}: {e}"));
        assert_eq!(v % 1_000_000, 0, "{key} = {v} is finer than 1 ms");
    }
}

// ---------------------------------------------------------------------------------------------
// Limits that fire inside the guest's slice, environment failures, metrics
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_busy_guest_is_killed_at_the_wall_deadline_by_its_own_epoch_check() {
    let mut rig = Rig::new();
    let spin = prepared("spin");
    let started = Instant::now();
    let done = rig.run(&spin, Req::get("/").wall_ms(3)).await;
    assert_eq!(done.killed(), KillReason::WallTimeout);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the loop outlived its deadline: {:?}",
        started.elapsed()
    );
    assert!(
        rig.log_records().iter().any(|r| r.msg.contains("task killed")),
        "{:?}",
        rig.log_records()
    );
}

#[tokio::test]
async fn with_fuel_metering_a_guest_that_burns_its_fuel_inside_one_slice_is_killed_as_cpu_limit() {
    let (engine, _ticker) = common::fuel_engine();
    let mut rig = Rig::on_engine(engine.clone(), 512, Duration::from_millis(1));
    let spin = common::prepared_on(&engine, "spin");
    // 1 ms of CPU buys ~10 M fuel; the 10 ms slice is far from over when it is gone.
    let done = rig.run(&spin, Req::get("/").cpu_ms(1).wall_ms(30_000)).await;
    assert_eq!(done.killed(), KillReason::CpuLimit);
}

#[tokio::test]
async fn a_task_whose_environment_cannot_be_read_fails_and_says_why() {
    let mut rig = Rig::new();
    rig.control
        .env_unavailable
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let p = prepared("hello");
    rig.run(&p, Req::get("/")).await.expect_guest_error();
    let logs = rig.log_records();
    assert!(
        logs.iter().any(|l| l.msg.contains("environment unavailable")),
        "{logs:?}"
    );
}

#[tokio::test]
async fn instantiation_time_is_recorded_per_task_when_the_executor_has_metrics() {
    let metrics = rt_obs::Metrics::new(rt_obs::MetricsOptions::default()).expect("metrics");
    let mut rig = Rig::new().metrics(metrics.clone());
    let p = prepared("hello");
    rig.run(&p, Req::get("/")).await.reply();
    rig.run(&p, Req::get("/")).await.reply();
    assert_eq!(metrics.wasm_instantiate_seconds.get_sample_count(), 2);
}
