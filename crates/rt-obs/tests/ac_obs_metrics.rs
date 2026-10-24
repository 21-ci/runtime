//! Prometheus registry (docs/deploy.md "Health and metrics", OBS-06, OBS-07): every documented metric present in the
//! first scrape, task/admission/scheduler series fed from outcomes and snapshots, per-service
//! series opt-in and capped at the top-N services.
//!
//! `Metrics::{new, render, task_finished, observe_sched, service_label}` are `todo!()` in the
//! skeleton: tests that use them are expected to fail until implemented. The metric-name table is
//! real and its test already passes.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use rt_obs::{metric_names, Metrics, MetricsOptions, NODE_METRIC_NAMES};
use rt_types::{
    GrqLens, KillReason, MachineId, MachineSnapshot, Outcome, ProcId, ProcSnapshot, ProcStatus, RejectReason, Reply,
    SchedSnapshot, ServiceId, TaskKind, TaskTimings,
};

const OBS_DOC: &str = include_str!("../../../docs/deploy.md");

// ---------------------------------------------------------------------------------------------
// Prometheus text parsing
// ---------------------------------------------------------------------------------------------

type Labels = BTreeMap<String, String>;

#[derive(Debug, Clone)]
struct Sample {
    name: String,
    labels: Labels,
    value: f64,
}

fn parse_labels(inner: &str) -> Labels {
    let mut out = Labels::new();
    let mut chars = inner.chars().peekable();
    loop {
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' {
                break;
            }
            key.push(c);
            chars.next();
        }
        if chars.next().is_none() {
            break; // no more `key=`
        }
        assert_eq!(chars.next(), Some('"'), "label value must be quoted in `{inner}`");
        let mut val = String::new();
        loop {
            match chars.next() {
                Some('\\') => val.push(chars.next().expect("escape")),
                Some('"') => break,
                Some(c) => val.push(c),
                None => panic!("unterminated label value in `{inner}`"),
            }
        }
        out.insert(key.trim_start_matches(',').to_string(), val);
        if chars.peek() == Some(&',') {
            chars.next();
        }
    }
    out
}

fn parse(text: &str) -> Vec<Sample> {
    text.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .map(|line| {
            let (head, value) = line
                .rsplit_once(' ')
                .unwrap_or_else(|| panic!("not a sample line: `{line}`"));
            let (name, labels) = match head.find('{') {
                Some(i) => (&head[..i], parse_labels(head[i + 1..].trim_end_matches('}'))),
                None => (head, Labels::new()),
            };
            let value = match value {
                "+Inf" => f64::INFINITY,
                "-Inf" => f64::NEG_INFINITY,
                v => v.parse().unwrap_or_else(|_| panic!("bad sample value in `{line}`")),
            };
            Sample {
                name: name.to_string(),
                labels,
                value,
            }
        })
        .collect()
}

fn find(samples: &[Sample], name: &str, want: &[(&str, &str)]) -> Vec<f64> {
    samples
        .iter()
        .filter(|s| {
            s.name == name
                && want
                    .iter()
                    .all(|(k, v)| s.labels.get(*k).map(String::as_str) == Some(*v))
        })
        .map(|s| s.value)
        .collect()
}

/// The single value of `name{want}`; panics when absent or ambiguous.
fn value(m: &Metrics, name: &str, want: &[(&str, &str)]) -> f64 {
    let samples = parse(&m.render());
    let v = find(&samples, name, want);
    assert_eq!(
        v.len(),
        1,
        "expected exactly one `{name}{want:?}` sample, found {}: {:#?}",
        v.len(),
        samples.iter().filter(|s| s.name == name).collect::<Vec<_>>()
    );
    v[0]
}

/// Sum over all series matching `want` (0 when none exists).
fn total(m: &Metrics, name: &str, want: &[(&str, &str)]) -> f64 {
    find(&parse(&m.render()), name, want).iter().sum()
}

fn metrics(per_service: bool, top_n: usize) -> std::sync::Arc<Metrics> {
    Metrics::new(MetricsOptions {
        per_service,
        per_service_top_n: top_n,
    })
    .expect("register metrics")
}

fn ok() -> Outcome {
    Outcome::Ok(Reply::ok())
}

fn timings(queue_ms: u64, init_ms: u64, exec_ms: u64) -> TaskTimings {
    TaskTimings {
        queue: Duration::from_millis(queue_ms),
        init: Duration::from_millis(init_ms),
        exec: Duration::from_millis(exec_ms),
        ..TaskTimings::default()
    }
}

// ---------------------------------------------------------------------------------------------
// OBS-06: every metric of docs/deploy.md is exposed
// ---------------------------------------------------------------------------------------------

/// `rt_*` identifiers of the always-on metrics table in docs/deploy.md (between "Node-level metrics
/// (always on" and the per-service paragraph).
fn documented_node_metrics() -> BTreeSet<String> {
    let section = OBS_DOC
        .split("Node-level metrics (always on")
        .nth(1)
        .expect("node-level metrics table");
    let table = section.split("Per-service metrics").next().unwrap();
    let mut names = BTreeSet::new();
    for token in table.split('`') {
        // backticked items look like `rt_tasks_total{kind,trigger,outcome}` or `rt_mem_budget_bytes`
        let name = token.split('{').next().unwrap();
        if name.starts_with("rt_") && name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
            names.insert(name.to_string());
        }
    }
    names
}

#[test]
fn the_node_metric_list_equals_the_metrics_documented_in_the_deploy_guide() {
    let documented = documented_node_metrics();
    let listed: BTreeSet<String> = NODE_METRIC_NAMES.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        listed.len(),
        NODE_METRIC_NAMES.len(),
        "NODE_METRIC_NAMES has duplicates"
    );
    assert_eq!(
        documented.len(),
        60,
        "docs/deploy.md lists 60 always-on metrics (parser sanity)"
    );
    let missing_in_code: Vec<_> = documented.difference(&listed).collect();
    let undocumented: Vec<_> = listed.difference(&documented).collect();
    assert!(
        missing_in_code.is_empty(),
        "documented but not in NODE_METRIC_NAMES: {missing_in_code:?}"
    );
    assert!(
        undocumented.is_empty(),
        "in NODE_METRIC_NAMES but not documented: {undocumented:?}"
    );
    assert!(NODE_METRIC_NAMES.iter().all(|n| n.starts_with("rt_")));
}

#[test]
fn per_service_series_are_opt_in_and_not_part_of_the_node_list() {
    assert!(!NODE_METRIC_NAMES.contains(&metric_names::SERVICE_REQUESTS_TOTAL));
    assert!(!NODE_METRIC_NAMES.contains(&metric_names::SERVICE_DURATION_SECONDS));
    assert_eq!(metric_names::SERVICE_REQUESTS_TOTAL, "rt_service_requests_total");
    assert_eq!(metric_names::SERVICE_DURATION_SECONDS, "rt_service_duration_seconds");
}

#[test]
fn obs_06_a_fresh_registry_exposes_every_node_metric_before_any_traffic() {
    let m = metrics(false, 100);
    let text = m.render();
    let mut missing = Vec::new();
    for name in NODE_METRIC_NAMES.iter() {
        let typed = text.contains(&format!("# TYPE {name} "));
        let sampled = text
            .lines()
            .any(|l| l.starts_with(name) && matches!(l.as_bytes().get(name.len()), Some(b' ' | b'{' | b'_')));
        if !(typed && sampled) {
            missing.push(*name);
        }
    }
    assert!(
        missing.is_empty(),
        "metrics missing from the first scrape (need HELP/TYPE and at least one sample): {missing:?}"
    );
}

#[test]
fn every_rendered_line_is_valid_prometheus_text() {
    let m = metrics(false, 100);
    m.task_finished(TaskKind::DENO, "http", ServiceId::new(), &ok(), &timings(1, 2, 3));
    let text = m.render();
    assert!(text.ends_with('\n'));
    for line in text.lines().filter(|l| !l.is_empty()) {
        assert!(
            line.starts_with("# HELP ") || line.starts_with("# TYPE ") || line.starts_with("rt_"),
            "unexpected line in the exposition: `{line}`"
        );
    }
    let samples = parse(&text);
    assert!(
        samples.iter().all(|s| s.name.starts_with("rt_")),
        "everything is namespaced `rt_`"
    );
    assert!(samples.iter().all(|s| !s.value.is_nan()));
}

#[test]
fn per_service_series_do_not_exist_unless_enabled() {
    let off = metrics(false, 100);
    assert!(off.service_requests_total.is_none() && off.service_duration_seconds.is_none());
    off.task_finished(TaskKind::DENO, "http", ServiceId::new(), &ok(), &timings(1, 1, 1));
    assert!(
        !off.render().contains("rt_service_requests_total"),
        "per_service=false: no per-service cardinality"
    );

    let on = metrics(true, 100);
    assert!(on.service_requests_total.is_some() && on.service_duration_seconds.is_some());
}

#[test]
fn two_registries_are_independent() {
    let a = metrics(false, 100);
    let b = metrics(false, 100);
    a.task_finished(TaskKind::DENO, "http", ServiceId::new(), &ok(), &timings(0, 0, 1));
    assert_eq!(total(&a, "rt_tasks_total", &[("outcome", "ok")]), 1.0);
    assert_eq!(
        total(&b, "rt_tasks_total", &[("outcome", "ok")]),
        0.0,
        "no process-global registry"
    );
}

// ---------------------------------------------------------------------------------------------
// Direct instruments are wired to the rendered registry
// ---------------------------------------------------------------------------------------------

#[test]
fn simple_instruments_show_up_in_the_scrape() {
    let m = metrics(false, 100);
    m.logs_dropped_total.inc_by(3);
    m.mem_budget_bytes.set(123_456);
    m.queue_depth.set(7);
    m.sched_hog_handoffs_total.inc();
    m.mem_pressure_level.set(2);
    m.mem_fragmentation_ratio.set(1.25);
    m.egress_bytes_total.inc_by(2048);
    m.mem_committed_bytes.with_label_values(&["deno_heap"]).set(99);
    assert_eq!(value(&m, "rt_logs_dropped_total", &[]), 3.0);
    assert_eq!(value(&m, "rt_mem_budget_bytes", &[]), 123_456.0);
    assert_eq!(value(&m, "rt_queue_depth", &[]), 7.0);
    assert_eq!(value(&m, "rt_sched_hog_handoffs_total", &[]), 1.0);
    assert_eq!(value(&m, "rt_mem_pressure_level", &[]), 2.0);
    assert_eq!(value(&m, "rt_mem_fragmentation_ratio", &[]), 1.25);
    assert_eq!(value(&m, "rt_egress_bytes_total", &[]), 2048.0);
    assert_eq!(value(&m, "rt_mem_committed_bytes", &[("category", "deno_heap")]), 99.0);
}

// ---------------------------------------------------------------------------------------------
// Event queues, pub/sub, streams, async invocation, keep-alive
// ---------------------------------------------------------------------------------------------

/// Every event metric is pre-seeded: a fresh registry lists each reason/condition/outcome series
/// at zero, so dashboards see the full label set before any traffic.
#[test]
fn event_metrics_preseed_every_bounded_label_value_at_zero() {
    let m = metrics(false, 100);
    for reason in ["fanout", "rejected", "no_task", "quota", "backlog"] {
        assert_eq!(value(&m, "rt_pubsub_dropped_total", &[("reason", reason)]), 0.0);
    }
    for reason in ["exhausted", "out_of_range"] {
        assert_eq!(value(&m, "rt_stream_skipped_total", &[("reason", reason)]), 0.0);
    }
    for condition in ["success", "retries_exhausted", "event_age_exceeded"] {
        assert_eq!(value(&m, "rt_async_finished_total", &[("condition", condition)]), 0.0);
    }
    for kind in ["deno", "wasm"] {
        for outcome in ["kept", "handoff", "disposed", "reused", "expired", "shed"] {
            assert_eq!(
                value(
                    &m,
                    "rt_instance_keepalive_total",
                    &[("kind", kind), ("outcome", outcome)]
                ),
                0.0
            );
        }
    }
}

/// Event counters reach the scrape, and the dropped/skipped/finished/keep-alive series carry only
/// their bounded labels (no tenant, service, topic or subject label).
#[test]
fn event_counters_are_scraped_with_bounded_labels_only() {
    let m = metrics(false, 100);
    m.queue_throttled_total.inc();
    m.pubsub_published_total.inc_by(2);
    m.pubsub_delivered_total.inc_by(5);
    m.pubsub_dropped_total.with_label_values(&["backlog"]).inc_by(4);
    m.stream_appended_total.inc_by(10);
    m.stream_delivered_total.inc_by(9);
    m.stream_dlq_total.inc();
    m.stream_trimmed_total.inc_by(3);
    m.stream_skipped_total.with_label_values(&["out_of_range"]).inc();
    m.async_accepted_total.inc_by(7);
    m.async_throttled_total.inc_by(2);
    m.async_finished_total
        .with_label_values(&["retries_exhausted"])
        .inc_by(6);
    m.async_failed_evicted_total.inc();
    m.instance_keepalive_total
        .with_label_values(&["wasm", "handoff"])
        .inc_by(8);

    assert_eq!(value(&m, "rt_queue_throttled_total", &[]), 1.0);
    assert_eq!(value(&m, "rt_pubsub_published_total", &[]), 2.0);
    assert_eq!(value(&m, "rt_pubsub_delivered_total", &[]), 5.0);
    assert_eq!(value(&m, "rt_pubsub_dropped_total", &[("reason", "backlog")]), 4.0);
    assert_eq!(value(&m, "rt_stream_appended_total", &[]), 10.0);
    assert_eq!(value(&m, "rt_stream_delivered_total", &[]), 9.0);
    assert_eq!(value(&m, "rt_stream_dlq_total", &[]), 1.0);
    assert_eq!(value(&m, "rt_stream_trimmed_total", &[]), 3.0);
    assert_eq!(value(&m, "rt_stream_skipped_total", &[("reason", "out_of_range")]), 1.0);
    assert_eq!(value(&m, "rt_async_accepted_total", &[]), 7.0);
    assert_eq!(value(&m, "rt_async_throttled_total", &[]), 2.0);
    assert_eq!(
        value(&m, "rt_async_finished_total", &[("condition", "retries_exhausted")]),
        6.0
    );
    assert_eq!(value(&m, "rt_async_failed_evicted_total", &[]), 1.0);
    assert_eq!(
        value(
            &m,
            "rt_instance_keepalive_total",
            &[("kind", "wasm"), ("outcome", "handoff")]
        ),
        8.0
    );

    for line in m.render().lines().filter(|l| {
        l.starts_with("rt_pubsub_")
            || l.starts_with("rt_stream_")
            || l.starts_with("rt_async_")
            || l.starts_with("rt_instance_keepalive_")
    }) {
        for banned in ["tenant", "service", "topic", "subject"] {
            assert!(!line.contains(&format!("{banned}=")), "label `{banned}` in {line}");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// task_finished
// ---------------------------------------------------------------------------------------------

#[test]
fn finished_tasks_are_counted_by_kind_trigger_and_outcome() {
    let m = metrics(false, 100);
    let svc = ServiceId::new();
    for _ in 0..3 {
        m.task_finished(TaskKind::DENO, "http", svc, &ok(), &timings(1, 1, 5));
    }
    m.task_finished(TaskKind::WASM, "interval", svc, &Outcome::GuestError, &timings(0, 0, 1));
    m.task_finished(
        TaskKind::DENO,
        "http",
        svc,
        &Outcome::Killed(KillReason::CpuLimit),
        &timings(0, 0, 60),
    );
    m.task_finished(
        TaskKind::DENO,
        "http",
        svc,
        &Outcome::Killed(KillReason::WallTimeout),
        &timings(0, 0, 60),
    );

    assert_eq!(
        value(
            &m,
            "rt_tasks_total",
            &[("kind", "deno"), ("trigger", "http"), ("outcome", "ok")]
        ),
        3.0
    );
    assert_eq!(
        value(
            &m,
            "rt_tasks_total",
            &[("kind", "wasm"), ("trigger", "interval"), ("outcome", "guest_error")]
        ),
        1.0
    );
    assert_eq!(
        value(
            &m,
            "rt_tasks_total",
            &[("kind", "deno"), ("trigger", "http"), ("outcome", "cpu_limit")]
        ),
        1.0
    );
    assert_eq!(
        value(
            &m,
            "rt_tasks_total",
            &[("kind", "deno"), ("trigger", "http"), ("outcome", "timeout")]
        ),
        1.0,
        "WallTimeout is labelled `timeout`"
    );
    assert_eq!(total(&m, "rt_tasks_total", &[]), 6.0, "one increment per finished task");
}

#[test]
fn admission_rejects_are_counted_by_reason_and_kills_are_not_rejects() {
    let m = metrics(false, 100);
    let svc = ServiceId::new();
    for r in [
        RejectReason::Shed,
        RejectReason::Shed,
        RejectReason::RateLimited,
        RejectReason::Draining,
    ] {
        m.task_finished(
            TaskKind::DENO,
            "http",
            svc,
            &Outcome::Rejected(r),
            &TaskTimings::default(),
        );
    }
    m.task_finished(
        TaskKind::DENO,
        "http",
        svc,
        &Outcome::Killed(KillReason::Oom),
        &timings(0, 0, 9),
    );
    assert_eq!(value(&m, "rt_admission_rejects_total", &[("reason", "shed")]), 2.0);
    assert_eq!(
        value(&m, "rt_admission_rejects_total", &[("reason", "rate_limited")]),
        1.0
    );
    assert_eq!(value(&m, "rt_admission_rejects_total", &[("reason", "draining")]), 1.0);
    assert_eq!(
        total(&m, "rt_admission_rejects_total", &[]),
        4.0,
        "an oom kill is not an admission reject"
    );
    // rejected tasks still count as finished tasks with the reason as outcome
    assert_eq!(total(&m, "rt_tasks_total", &[("outcome", "shed")]), 2.0);
}

#[test]
fn phase_histograms_record_each_phase_of_a_finished_task() {
    let m = metrics(false, 100);
    let t = TaskTimings {
        queue: Duration::from_millis(5),
        wait_runtime: Duration::from_millis(7),
        init: Duration::from_millis(11),
        exec: Duration::from_millis(20),
        ..TaskTimings::default()
    };
    m.task_finished(TaskKind::DENO, "http", ServiceId::new(), &ok(), &t);
    m.task_finished(TaskKind::DENO, "http", ServiceId::new(), &ok(), &t);
    for (phase, secs) in [
        ("queue", 0.005),
        ("wait_runtime", 0.007),
        ("init", 0.011),
        ("exec", 0.020),
    ] {
        let want = [("kind", "deno"), ("phase", phase)];
        assert_eq!(value(&m, "rt_task_phase_seconds_count", &want), 2.0, "{phase} count");
        let sum = value(&m, "rt_task_phase_seconds_sum", &want);
        assert!(
            (sum - 2.0 * secs).abs() < 1e-9,
            "{phase}: sum {sum}, expected {}",
            2.0 * secs
        );
    }
    // buckets are cumulative and end in +Inf == count
    let samples = parse(&m.render());
    let buckets: Vec<(f64, f64)> = samples
        .iter()
        .filter(|s| {
            s.name == "rt_task_phase_seconds_bucket" && s.labels.get("phase").map(String::as_str) == Some("exec")
        })
        .map(|s| (s.labels["le"].replace("+Inf", "inf").parse::<f64>().unwrap(), s.value))
        .collect();
    assert!(buckets.len() >= 5, "a useful histogram has several buckets");
    let mut sorted = buckets.clone();
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    assert!(
        sorted.windows(2).all(|w| w[0].1 <= w[1].1),
        "bucket counts must be cumulative: {sorted:?}"
    );
    assert_eq!(sorted.last().unwrap().1, 2.0);
    // 20 ms falls in a bucket whose upper bound is >= 20 ms and below the +Inf bucket's
    let first_with_both = sorted.iter().find(|(_, c)| *c == 2.0).unwrap();
    assert!(first_with_both.0 >= 0.020);
}

// ---------------------------------------------------------------------------------------------
// observe_sched
// ---------------------------------------------------------------------------------------------

fn proc_snapshot(id: u32, kind: TaskKind, lrq: u32) -> ProcSnapshot {
    ProcSnapshot {
        id: ProcId(id),
        kind,
        status: ProcStatus::Running,
        lrq_len: lrq,
        runnext: None,
        schedtick: 0,
        machine: Some(MachineId(id)),
        busy_ns: 0,
        steals_in: 0,
        steals_out: 0,
        preemptions: 0,
        rekinds: 0,
    }
}

fn snapshot(procs: Vec<ProcSnapshot>, grq: GrqLens) -> SchedSnapshot {
    SchedSnapshot {
        machines: procs
            .iter()
            .map(|p| MachineSnapshot {
                id: MachineId(p.id.0),
                proc: Some(p.id),
                bound_count: 0,
                spinning: false,
                parked: false,
            })
            .collect(),
        procs,
        grq,
        ..SchedSnapshot::default()
    }
}

#[test]
fn scheduler_snapshots_are_copied_into_the_gauges() {
    let m = metrics(false, 100);
    let snap = snapshot(
        vec![
            proc_snapshot(0, TaskKind::DENO, 3),
            proc_snapshot(1, TaskKind::DENO, 5),
            proc_snapshot(2, TaskKind::WASM, 0),
        ],
        GrqLens {
            deno: 12,
            wasm: 0,
            build: 1,
        },
    );
    m.observe_sched(&snap);
    assert_eq!(value(&m, "rt_sched_processors", &[("kind", "deno")]), 2.0);
    assert_eq!(value(&m, "rt_sched_processors", &[("kind", "wasm")]), 1.0);
    assert_eq!(value(&m, "rt_sched_grq_len", &[("kind", "deno")]), 12.0);
    assert_eq!(value(&m, "rt_sched_grq_len", &[("kind", "wasm")]), 0.0);
    assert_eq!(value(&m, "rt_sched_grq_len", &[("kind", "build")]), 1.0);
    assert_eq!(value(&m, "rt_sched_lrq_len", &[("p", "0")]), 3.0);
    assert_eq!(value(&m, "rt_sched_lrq_len", &[("p", "1")]), 5.0);
    assert_eq!(value(&m, "rt_sched_lrq_len", &[("p", "2")]), 0.0);
}

#[test]
fn observing_again_overwrites_gauges_instead_of_accumulating() {
    let m = metrics(false, 100);
    m.observe_sched(&snapshot(
        vec![proc_snapshot(0, TaskKind::DENO, 9)],
        GrqLens {
            deno: 40,
            wasm: 0,
            build: 0,
        },
    ));
    m.observe_sched(&snapshot(
        vec![proc_snapshot(0, TaskKind::DENO, 2)],
        GrqLens {
            deno: 1,
            wasm: 0,
            build: 0,
        },
    ));
    assert_eq!(value(&m, "rt_sched_lrq_len", &[("p", "0")]), 2.0);
    assert_eq!(value(&m, "rt_sched_grq_len", &[("kind", "deno")]), 1.0);
    assert_eq!(value(&m, "rt_sched_processors", &[("kind", "deno")]), 1.0);
}

// ---------------------------------------------------------------------------------------------
// OBS-07: per-service cardinality
// ---------------------------------------------------------------------------------------------

fn per_service_labels(m: &Metrics) -> BTreeSet<String> {
    parse(&m.render())
        .into_iter()
        .filter(|s| s.name == "rt_service_requests_total")
        .map(|s| s.labels.get("service").cloned().expect("service label"))
        .collect()
}

#[test]
fn obs_07_a_thousand_services_produce_at_most_top_n_plus_one_series() {
    let m = metrics(true, 100);
    for _ in 0..1_000 {
        m.task_finished(TaskKind::DENO, "http", ServiceId::new(), &ok(), &timings(0, 0, 1));
    }
    let labels = per_service_labels(&m);
    assert!(
        labels.len() <= 101,
        "100 services + `_other`, got {} distinct service labels",
        labels.len()
    );
    assert!(
        labels.contains("_other"),
        "the long tail aggregates into service=\"_other\""
    );

    let samples = parse(&m.render());
    let series = samples.iter().filter(|s| s.name == "rt_service_requests_total").count();
    let status_classes: BTreeSet<_> = samples
        .iter()
        .filter(|s| s.name == "rt_service_requests_total")
        .map(|s| s.labels.get("status_class").cloned().unwrap())
        .collect();
    assert!(
        series <= 101 * status_classes.len().max(1),
        "series {series} must stay <= 101 x status classes"
    );
    let total: f64 = samples
        .iter()
        .filter(|s| s.name == "rt_service_requests_total")
        .map(|s| s.value)
        .sum();
    assert_eq!(total, 1_000.0, "aggregation into _other must not lose requests");
}

#[test]
fn the_busiest_services_keep_their_own_label_and_the_rest_share_other() {
    let m = metrics(true, 3);
    let svcs: Vec<ServiceId> = (0..5).map(|_| ServiceId::new()).collect();
    // A=100, B=80, C=60, D=1, E=1 requests, interleaved round-robin
    let plan = [100usize, 80, 60, 1, 1];
    for round in 0..100 {
        for (i, svc) in svcs.iter().enumerate() {
            if round < plan[i] {
                m.task_finished(TaskKind::DENO, "http", *svc, &ok(), &timings(0, 0, 1));
            }
        }
    }
    for hot in &svcs[..3] {
        assert_ne!(m.service_label(*hot), "_other");
        assert!(
            m.service_label(*hot).contains(&hot.bare()),
            "label identifies the service: {}",
            m.service_label(*hot)
        );
    }
    for cold in &svcs[3..] {
        assert_eq!(m.service_label(*cold), "_other", "services outside the top 3 aggregate");
    }
    let labels = per_service_labels(&m);
    assert_eq!(labels.len(), 4, "3 top services + _other: {labels:?}");
    let samples = parse(&m.render());
    let other: f64 = find(&samples, "rt_service_requests_total", &[("service", "_other")])
        .iter()
        .sum();
    assert_eq!(other, 2.0);
    let all: f64 = find(&samples, "rt_service_requests_total", &[]).iter().sum();
    assert_eq!(all, 242.0);
}

#[test]
fn per_service_status_classes_follow_the_outcome_status() {
    let m = metrics(true, 10);
    let svc = ServiceId::new();
    m.task_finished(TaskKind::DENO, "http", svc, &ok(), &timings(0, 0, 1));
    m.task_finished(TaskKind::DENO, "http", svc, &Outcome::GuestError, &timings(0, 0, 1));
    m.task_finished(
        TaskKind::DENO,
        "http",
        svc,
        &Outcome::Rejected(RejectReason::RateLimited),
        &timings(0, 0, 0),
    );
    let label = m.service_label(svc);
    let samples = parse(&m.render());
    let class_count = |prefix: char| -> f64 {
        samples
            .iter()
            .filter(|s| {
                s.name == "rt_service_requests_total"
                    && s.labels.get("service") == Some(&label)
                    && s.labels["status_class"].starts_with(prefix)
            })
            .map(|s| s.value)
            .sum()
    };
    assert_eq!((class_count('2'), class_count('4'), class_count('5')), (1.0, 1.0, 1.0));
}
