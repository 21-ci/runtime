//! Outcome -> HTTP status / `x-rt-reason` mapping, checked against the `x-rt-reason` table of
//! docs/api.md ("Invoke API").

use std::collections::BTreeMap;
use std::time::Duration;

use rt_types::{
    Body, BuildFailReason, BuildFailure, Diagnostic, KillReason, Outcome, RejectReason, Reply, TaskTimings,
    MAX_CALL_DEPTH,
};

const API_DOC: &str = include_str!("../../../docs/api.md");

/// The backticked reasons of one reason cell, e.g. "`a`, `b` (note with `code`)" -> ["a", "b"].
/// Text inside a parenthesized note (including backticked words there) is not a reason.
fn reasons_in_cell(cell: &str) -> Vec<String> {
    let (mut depth, mut current, mut reasons) = (0u32, None::<String>, Vec::new());
    for c in cell.chars() {
        match (c, current.as_mut()) {
            ('`', Some(_)) => reasons.extend(current.take().filter(|_| depth == 0)),
            ('`', None) => current = Some(String::new()),
            (c, Some(word)) => word.push(c),
            ('(', None) => depth += 1,
            (')', None) => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    assert!(
        current.is_none() && depth == 0,
        "unbalanced backticks or parentheses in `{cell}`"
    );
    reasons
}

/// Parses the `x-rt-reason | HTTP` table of docs/api.md into `reason -> status`. The status cell may
/// carry a note after the number ("429 (+ `Retry-After`)").
fn documented_invoke_table() -> BTreeMap<String, u16> {
    let mut lines = API_DOC.lines().skip_while(|l| !l.starts_with("| `x-rt-reason`"));
    assert!(
        lines.next().is_some(),
        "invoke reason table header not found in docs/api.md"
    );
    let mut table = BTreeMap::new();
    for line in lines.skip(1).take_while(|l| l.starts_with('|')) {
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        assert_eq!(cells.len(), 2, "unexpected row `{line}`");
        let digits: String = cells[1].chars().take_while(char::is_ascii_digit).collect();
        let status: u16 = digits.parse().unwrap_or_else(|_| panic!("bad status in `{line}`"));
        let reasons = reasons_in_cell(cells[0]);
        assert!(!reasons.is_empty(), "no reason in `{line}`");
        for reason in reasons {
            assert!(
                !reason.is_empty() && reason.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
                "bad reason `{reason}` in `{line}`"
            );
            assert!(
                table.insert(reason.clone(), status).is_none(),
                "duplicate reason {reason}"
            );
        }
    }
    table
}

#[test]
fn reason_cells_ignore_parenthesized_notes() {
    assert_eq!(reasons_in_cell("`forbidden` (ingress path / IP rule)"), ["forbidden"]);
    assert_eq!(
        reasons_in_cell("`trigger_not_allowed` (the plan does not allow `http`)"),
        ["trigger_not_allowed"]
    );
    assert_eq!(reasons_in_cell("`a`, `b` (x, `y`), `c`"), ["a", "b", "c"]);
}

fn reply(status: u16) -> Reply {
    Reply::new(http::StatusCode::from_u16(status).unwrap(), Body::empty())
}

/// Every non-`Ok` outcome the runtime can produce. The two exhaustive matches below fail to
/// compile when a variant is added without updating the expectations.
fn all_non_ok_outcomes() -> Vec<Outcome> {
    let mut v = vec![
        Outcome::GuestError,
        Outcome::BuildFailed(BuildFailure::new(BuildFailReason::Syntax, "boom")),
    ];
    for k in [
        KillReason::CpuLimit,
        KillReason::Oom,
        KillReason::WallTimeout,
        KillReason::Shutdown,
    ] {
        v.push(Outcome::Killed(k));
    }
    for r in RejectReason::ALL {
        v.push(Outcome::Rejected(r));
    }
    v
}

fn expected_kill(k: KillReason) -> (u16, &'static str) {
    match k {
        KillReason::CpuLimit => (502, "cpu_limit"),
        KillReason::Oom => (502, "oom"),
        KillReason::WallTimeout => (504, "timeout"),
        KillReason::Shutdown => (503, "restarting"),
    }
}

fn expected_reject(r: RejectReason) -> (u16, &'static str) {
    match r {
        RejectReason::Draining => (503, "draining"),
        RejectReason::Shed => (503, "shed"),
        RejectReason::Overloaded => (503, "overloaded"),
        RejectReason::QueueTimeout => (503, "queue_timeout"),
        RejectReason::Memory => (503, "memory"),
        RejectReason::TenantConcurrency => (429, "tenant_concurrency"),
        RejectReason::ServiceConcurrency => (429, "service_concurrency"),
        RejectReason::RateLimited => (429, "rate_limited"),
        RejectReason::QuotaExceeded => (429, "quota_exceeded"),
        RejectReason::CallDepth => (508, "call_depth"),
    }
}

#[test]
fn every_outcome_matches_the_status_and_reason_in_the_api_doc_table() {
    let doc = documented_invoke_table();
    for o in all_non_ok_outcomes() {
        let reason = o.reason().unwrap_or_else(|| panic!("{o:?} must carry an x-rt-reason"));
        let documented = doc
            .get(reason)
            .unwrap_or_else(|| panic!("reason `{reason}` ({o:?}) missing from docs/api.md"));
        assert_eq!(
            o.http_status(),
            *documented,
            "status of {o:?} (`{reason}`) differs from docs/api.md"
        );
    }
}

#[test]
fn every_documented_reason_except_ingress_only_ones_is_produced_by_some_outcome() {
    let doc = documented_invoke_table();
    // Produced by the ingress path (rt-api `invoke.rs`: routing, the plan's `http` trigger, invoke
    // access, path / IP rules, method and body checks) before any task exists, not by task outcomes.
    let ingress_only = [
        ("not_found", 404),
        ("unauthorized", 401),
        ("forbidden", 403),
        ("trigger_not_allowed", 403),
        ("method_not_allowed", 405),
        ("too_large", 413),
        ("route_stale", 503),
        // async invocation: refused at accept, before any task exists
        ("async_not_allowed", 403),
        ("async_backlog_full", 429),
        ("idempotency_mismatch", 409),
        ("bad_request", 400),
    ];
    let produced: Vec<&str> = all_non_ok_outcomes().iter().filter_map(Outcome::reason).collect();
    for reason in doc.keys() {
        if ingress_only.iter().any(|(r, _)| r == reason) {
            assert!(
                !produced.contains(&reason.as_str()),
                "`{reason}` is both ingress-only and an Outcome reason"
            );
            continue;
        }
        assert!(
            produced.contains(&reason.as_str()),
            "documented reason `{reason}` is never produced by an Outcome"
        );
    }
    for (reason, status) in ingress_only {
        assert_eq!(doc.get(reason), Some(&status), "ingress reason `{reason}`");
    }
}

#[test]
fn guest_visible_error_codes_are_not_invoke_reasons() {
    // Thrown inside the guest (`call()`, `fetch()`, `sql`); uncaught they surface as `guest_error`.
    let doc = documented_invoke_table();
    for code in ["call_not_allowed", "egress_insecure_redirect", "sql_result_too_large"] {
        assert!(
            !doc.contains_key(code),
            "`{code}` is a guest error code, not an invoke x-rt-reason"
        );
    }
}

#[test]
fn killed_outcomes_map_exactly() {
    for k in [
        KillReason::CpuLimit,
        KillReason::Oom,
        KillReason::WallTimeout,
        KillReason::Shutdown,
    ] {
        let (status, reason) = expected_kill(k);
        let o = Outcome::Killed(k);
        assert_eq!((o.http_status(), o.reason()), (status, Some(reason)), "{k:?}");
        assert_eq!((k.http_status(), k.reason()), (status, reason), "{k:?}");
    }
    // CPU limit 502, memory limit 502, wall timeout 504.
    assert_eq!(Outcome::Killed(KillReason::CpuLimit).http_status(), 502);
    assert_eq!(Outcome::Killed(KillReason::Oom).http_status(), 502);
    assert_eq!(Outcome::Killed(KillReason::WallTimeout).http_status(), 504);
}

#[test]
fn rejected_outcomes_map_exactly_and_all_reasons_are_covered() {
    assert_eq!(RejectReason::ALL.len(), 10);
    for r in RejectReason::ALL {
        let (status, reason) = expected_reject(r);
        let o = Outcome::Rejected(r);
        assert_eq!((o.http_status(), o.reason()), (status, Some(reason)), "{r:?}");
        assert_eq!((r.http_status(), r.reason()), (status, reason), "{r:?}");
    }
    let mut reasons: Vec<&str> = RejectReason::ALL.iter().map(|r| r.reason()).collect();
    reasons.sort_unstable();
    reasons.dedup();
    assert_eq!(
        reasons.len(),
        RejectReason::ALL.len(),
        "RejectReason::ALL must list each variant once"
    );
}

#[test]
fn admission_rejects_are_429_for_limits_and_503_for_node_state() {
    // Admission: drain/shed/overloaded/queue_timeout -> 503; tenant/service concurrency -> 429.
    for (r, status) in [
        (RejectReason::Draining, 503),
        (RejectReason::Shed, 503),
        (RejectReason::Overloaded, 503),
        (RejectReason::QueueTimeout, 503),
        (RejectReason::TenantConcurrency, 429),
        (RejectReason::ServiceConcurrency, 429),
    ] {
        assert_eq!(Outcome::Rejected(r).http_status(), status, "{r:?}");
    }
    // Call depth > MAX_CALL_DEPTH (15) -> 508 call_depth
    assert_eq!(MAX_CALL_DEPTH, 15);
    assert_eq!(
        (
            Outcome::Rejected(RejectReason::CallDepth).http_status(),
            Outcome::Rejected(RejectReason::CallDepth).reason()
        ),
        (508, Some("call_depth"))
    );
}

#[test]
fn guest_error_and_build_failed_map_exactly() {
    assert_eq!(Outcome::GuestError.http_status(), 500);
    assert_eq!(Outcome::GuestError.reason(), Some("guest_error"));
    let bf = Outcome::BuildFailed(BuildFailure::new(BuildFailReason::Resolve, "cannot resolve ./x.ts"));
    assert_eq!(bf.http_status(), 502);
    assert_eq!(bf.reason(), Some("build_failed"));
}

#[test]
fn ok_outcome_passes_the_guest_status_through_and_has_no_reason() {
    for status in [200u16, 201, 204, 301, 400, 404, 418, 500, 503] {
        let o = Outcome::Ok(reply(status));
        assert_eq!(o.http_status(), status);
        assert_eq!(o.reason(), None, "guest-chosen {status} must not get an x-rt-reason");
        assert!(o.is_ok());
    }
}

#[test]
fn only_ok_is_ok() {
    for o in all_non_ok_outcomes() {
        assert!(!o.is_ok(), "{o:?}");
    }
}

#[test]
fn metric_label_is_ok_or_the_reason() {
    assert_eq!(Outcome::Ok(Reply::ok()).label(), "ok");
    for o in all_non_ok_outcomes() {
        assert_eq!(Some(o.label()), o.reason(), "{o:?}");
    }
    // `ok` must never collide with a failure reason
    assert!(all_non_ok_outcomes().iter().all(|o| o.label() != "ok"));
}

#[test]
fn all_reasons_are_distinct_snake_case_tokens() {
    let mut seen = std::collections::BTreeSet::new();
    for o in all_non_ok_outcomes() {
        let r = o.reason().unwrap();
        assert!(
            r.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "`{r}` is not snake_case"
        );
        // `Outcome::label` shares the namespace: Killed(Oom)="oom" and Rejected(Memory)="memory" are different.
        assert!(seen.insert(r), "reason `{r}` is produced by two different outcomes");
    }
}

#[test]
fn reject_reason_serde_name_equals_x_rt_reason() {
    for r in RejectReason::ALL {
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, format!("\"{}\"", r.reason()));
        assert_eq!(serde_json::from_str::<RejectReason>(&json).unwrap(), r);
    }
}

#[test]
fn kill_reason_serde_names_differ_from_x_rt_reason_for_timeout_and_shutdown() {
    // The wire reason is client facing (`timeout`, `restarting`); the serde name is the internal enum name.
    assert_eq!(
        serde_json::to_string(&KillReason::WallTimeout).unwrap(),
        "\"wall_timeout\""
    );
    assert_eq!(serde_json::to_string(&KillReason::Shutdown).unwrap(), "\"shutdown\"");
    assert_eq!(serde_json::to_string(&KillReason::CpuLimit).unwrap(), "\"cpu_limit\"");
    assert_eq!(serde_json::to_string(&KillReason::Oom).unwrap(), "\"oom\"");
    assert_eq!(KillReason::WallTimeout.reason(), "timeout");
    assert_eq!(KillReason::Shutdown.reason(), "restarting");
}

#[test]
fn build_fail_reason_string_equals_serde_name_for_every_variant() {
    let all = [
        (BuildFailReason::Syntax, "syntax"),
        (BuildFailReason::Resolve, "resolve"),
        (BuildFailReason::Fetch, "fetch"),
        (BuildFailReason::LockMismatch, "lock_mismatch"),
        (BuildFailReason::Timeout, "timeout"),
        (BuildFailReason::Oom, "oom"),
        (BuildFailReason::TooLarge, "too_large"),
        (BuildFailReason::Internal, "internal"),
    ];
    for (r, s) in all {
        assert_eq!(r.as_str(), s);
        assert_eq!(serde_json::to_string(&r).unwrap(), format!("\"{s}\""));
    }
}

#[test]
fn build_failure_display_and_diagnostics_round_trip() {
    let mut f = BuildFailure::new(BuildFailReason::LockMismatch, "rt.lock does not match");
    assert_eq!(f.to_string(), "build failed (lock_mismatch): rt.lock does not match");
    assert!(f.diagnostics.is_empty());
    f.diagnostics.push(Diagnostic {
        file: "a.ts".into(),
        line: 3,
        col: 7,
        message: "unexpected token".into(),
    });
    let json = serde_json::to_value(&f).unwrap();
    assert_eq!(json["reason"], "lock_mismatch");
    assert_eq!(json["diagnostics"][0]["file"], "a.ts");
    assert_eq!(json["diagnostics"][0]["line"], 3);
    assert_eq!(json["diagnostics"][0]["col"], 7);
    let back: BuildFailure = serde_json::from_value(json).unwrap();
    assert_eq!(back, f);
}

#[test]
fn reply_ok_is_200_with_empty_body_and_zero_timings() {
    let r = Reply::ok();
    assert_eq!(r.status, http::StatusCode::OK);
    assert!(r.headers.is_empty());
    assert_eq!(r.body.exact_len(), Some(0));
    assert_eq!(r.timings, TaskTimings::default());
}

// ---------------------------------------------------------------------------------------------
// server-timing (OBS-11)
// ---------------------------------------------------------------------------------------------

fn timings(queue_us: u64, wait_us: u64, init_us: u64, exec_us: u64) -> TaskTimings {
    TaskTimings {
        queue: Duration::from_micros(queue_us),
        wait_runtime: Duration::from_micros(wait_us),
        init: Duration::from_micros(init_us),
        exec: Duration::from_micros(exec_us),
        ..TaskTimings::default()
    }
}

#[test]
fn server_timing_header_exact_format() {
    let t = timings(1_500, 0, 2_000, 10_250);
    assert_eq!(
        t.server_timing(),
        "queue;dur=1.500, wait_runtime;dur=0.000, init;dur=2.000, exec;dur=10.250"
    );
    assert_eq!(
        TaskTimings::default().server_timing(),
        "queue;dur=0.000, wait_runtime;dur=0.000, init;dur=0.000, exec;dur=0.000"
    );
}

#[test]
fn server_timing_phases_sum_to_total_within_one_ms() {
    let cases = [
        timings(0, 0, 0, 0),
        timings(1_500, 250, 2_000, 10_250),
        timings(999, 999, 999, 999),
        timings(30_000, 5, 77, 123_456),
    ];
    for t in cases {
        let header = t.server_timing();
        let sum_ms: f64 = header
            .split(", ")
            .map(|part| {
                part.split_once(";dur=")
                    .expect("name;dur=value")
                    .1
                    .parse::<f64>()
                    .unwrap()
            })
            .sum();
        let total_ms = t.total().as_secs_f64() * 1000.0;
        assert!(
            (sum_ms - total_ms).abs() <= 1.0,
            "header sum {sum_ms} vs total {total_ms} for `{header}`"
        );
        assert_eq!(t.total(), t.queue + t.wait_runtime + t.init + t.exec);
    }
}

#[test]
fn server_timing_lists_phases_in_documented_order_and_excludes_cpu() {
    let mut t = timings(1, 2, 3, 4);
    t.cpu = Duration::from_millis(999);
    let h = t.server_timing();
    let names: Vec<&str> = h.split(", ").map(|p| p.split(';').next().unwrap()).collect();
    assert_eq!(names, ["queue", "wait_runtime", "init", "exec"]);
    assert_eq!(
        t.total(),
        Duration::from_micros(10),
        "cpu time is not part of the wall phases"
    );
}

#[test]
fn only_capacity_refusals_carry_a_retry_hint() {
    for r in [
        RejectReason::Draining,
        RejectReason::RateLimited,
        RejectReason::QuotaExceeded,
        RejectReason::CallDepth,
    ] {
        assert_eq!(
            r.retry_after(),
            None,
            "{r:?} is retried elsewhere or by its own enforcer"
        );
    }
    assert_eq!(RejectReason::Overloaded.retry_after(), Some(Duration::from_secs(1)));
    assert_eq!(RejectReason::Memory.retry_after(), Some(Duration::from_secs(1)));
}
