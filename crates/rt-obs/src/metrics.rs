//! Prometheus metrics registry.
//!
//! One [`Metrics`] per node. Every metric below exists from startup (zero-valued) so a scrape
//! always lists all of them (OBS-06). Per-service metrics are opt-in and capped at the top-N
//! services (OBS-07); the rest aggregate into `service="_other"`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use rt_types::{Outcome, RejectReason, SchedSnapshot, ServiceId, TaskKind, TaskTimings};

use crate::ObsError;

/// Metric names. Tests enumerate [`NODE_METRIC_NAMES`].
pub mod names {
    pub const TASKS_TOTAL: &str = "rt_tasks_total";
    pub const TASK_PHASE_SECONDS: &str = "rt_task_phase_seconds";
    pub const INFLIGHT_TASKS: &str = "rt_inflight_tasks";
    pub const ADMISSION_REJECTS_TOTAL: &str = "rt_admission_rejects_total";

    pub const SCHED_PROCESSORS: &str = "rt_sched_processors";
    pub const SCHED_GRQ_LEN: &str = "rt_sched_grq_len";
    pub const SCHED_LRQ_LEN: &str = "rt_sched_lrq_len";
    pub const SCHED_STEALS_TOTAL: &str = "rt_sched_steals_total";
    pub const SCHED_REKINDS_TOTAL: &str = "rt_sched_rekinds_total";
    pub const SCHED_PREEMPTIONS_TOTAL: &str = "rt_sched_preemptions_total";
    pub const SCHED_HOG_HANDOFFS_TOTAL: &str = "rt_sched_hog_handoffs_total";
    pub const SCHED_SPINNING_SECONDS_TOTAL: &str = "rt_sched_spinning_seconds_total";
    pub const SYSMON_TICK_SECONDS: &str = "rt_sysmon_tick_seconds";

    pub const MEM_BUDGET_BYTES: &str = "rt_mem_budget_bytes";
    pub const MEM_COMMITTED_BYTES: &str = "rt_mem_committed_bytes";
    pub const MEM_RSS_BYTES: &str = "rt_mem_rss_bytes";
    pub const MEM_FRAGMENTATION_RATIO: &str = "rt_mem_fragmentation_ratio";
    pub const MEM_PRESSURE_LEVEL: &str = "rt_mem_pressure_level";

    pub const CACHE_ENTRIES: &str = "rt_cache_entries";
    pub const CACHE_BYTES: &str = "rt_cache_bytes";
    pub const CACHE_HITS_TOTAL: &str = "rt_cache_hits_total";
    pub const CACHE_MISSES_TOTAL: &str = "rt_cache_misses_total";
    pub const CACHE_EVICTIONS_TOTAL: &str = "rt_cache_evictions_total";
    pub const CACHE_UNRESERVED_INSERTS_TOTAL: &str = "rt_cache_unreserved_inserts_total";

    pub const DENO_ISOLATE_CREATE_SECONDS: &str = "rt_deno_isolate_create_seconds";
    pub const DENO_CODE_CACHE_TOTAL: &str = "rt_deno_code_cache_total";
    pub const WASM_INSTANTIATE_SECONDS: &str = "rt_wasm_instantiate_seconds";
    pub const WASM_POOL_SLOTS: &str = "rt_wasm_pool_slots";

    pub const BUILD_TOTAL: &str = "rt_build_total";
    pub const BUILD_SECONDS: &str = "rt_build_seconds";

    pub const HTTP_REQUESTS_TOTAL: &str = "rt_http_requests_total";
    pub const INGRESS_CONN_REJECTED_TOTAL: &str = "rt_ingress_conn_rejected_total";
    pub const EGRESS_REQUESTS_TOTAL: &str = "rt_egress_requests_total";
    pub const EGRESS_BYTES_TOTAL: &str = "rt_egress_bytes_total";

    pub const SCHEDULE_FIRES_TOTAL: &str = "rt_schedule_fires_total";
    pub const SCHEDULE_SKIPPED_TOTAL: &str = "rt_schedule_skipped_total";
    pub const SCHEDULE_DENIED_TOTAL: &str = "rt_schedule_denied_total";
    pub const QUEUE_DEPTH: &str = "rt_queue_depth";
    pub const QUEUE_DLQ_TOTAL: &str = "rt_queue_dlq_total";

    pub const LOGS_RECORDS_TOTAL: &str = "rt_logs_records_total";
    pub const LOGS_DROPPED_TOTAL: &str = "rt_logs_dropped_total";

    pub const WS_SOCKETS: &str = "rt_ws_sockets";
    pub const WS_UPGRADES_TOTAL: &str = "rt_ws_upgrades_total";
    pub const WS_MESSAGES_TOTAL: &str = "rt_ws_messages_total";
    pub const WS_EVENT_TASKS_TOTAL: &str = "rt_ws_event_tasks_total";
    pub const WS_CLOSED_TOTAL: &str = "rt_ws_closed_total";

    // Opt-in (`metrics.per_service`).
    pub const SERVICE_REQUESTS_TOTAL: &str = "rt_service_requests_total";
    pub const SERVICE_DURATION_SECONDS: &str = "rt_service_duration_seconds";
}

/// Every always-on node metric.
pub const NODE_METRIC_NAMES: &[&str] = &[
    names::TASKS_TOTAL,
    names::TASK_PHASE_SECONDS,
    names::INFLIGHT_TASKS,
    names::ADMISSION_REJECTS_TOTAL,
    names::SCHED_PROCESSORS,
    names::SCHED_GRQ_LEN,
    names::SCHED_LRQ_LEN,
    names::SCHED_STEALS_TOTAL,
    names::SCHED_REKINDS_TOTAL,
    names::SCHED_PREEMPTIONS_TOTAL,
    names::SCHED_HOG_HANDOFFS_TOTAL,
    names::SCHED_SPINNING_SECONDS_TOTAL,
    names::SYSMON_TICK_SECONDS,
    names::MEM_BUDGET_BYTES,
    names::MEM_COMMITTED_BYTES,
    names::MEM_RSS_BYTES,
    names::MEM_FRAGMENTATION_RATIO,
    names::MEM_PRESSURE_LEVEL,
    names::CACHE_ENTRIES,
    names::CACHE_BYTES,
    names::CACHE_HITS_TOTAL,
    names::CACHE_MISSES_TOTAL,
    names::CACHE_EVICTIONS_TOTAL,
    names::CACHE_UNRESERVED_INSERTS_TOTAL,
    names::DENO_ISOLATE_CREATE_SECONDS,
    names::DENO_CODE_CACHE_TOTAL,
    names::WASM_INSTANTIATE_SECONDS,
    names::WASM_POOL_SLOTS,
    names::BUILD_TOTAL,
    names::BUILD_SECONDS,
    names::HTTP_REQUESTS_TOTAL,
    names::INGRESS_CONN_REJECTED_TOTAL,
    names::EGRESS_REQUESTS_TOTAL,
    names::EGRESS_BYTES_TOTAL,
    names::SCHEDULE_FIRES_TOTAL,
    names::SCHEDULE_SKIPPED_TOTAL,
    names::SCHEDULE_DENIED_TOTAL,
    names::QUEUE_DEPTH,
    names::QUEUE_DLQ_TOTAL,
    names::LOGS_RECORDS_TOTAL,
    names::LOGS_DROPPED_TOTAL,
    names::WS_SOCKETS,
    names::WS_UPGRADES_TOTAL,
    names::WS_MESSAGES_TOTAL,
    names::WS_EVENT_TASKS_TOTAL,
    names::WS_CLOSED_TOTAL,
];

/// Registration options.
#[derive(Clone, Debug)]
pub struct MetricsOptions {
    /// `metrics.per_service`.
    pub per_service: bool,
    /// `metrics.per_service_top_n`.
    pub per_service_top_n: usize,
}

impl Default for MetricsOptions {
    fn default() -> Self {
        Self {
            per_service: false,
            per_service_top_n: 100,
        }
    }
}

/// All node metrics. Cheap to clone via `Arc`.
pub struct Metrics {
    pub registry: Registry,
    // Tasks
    pub tasks_total: IntCounterVec,             // kind, trigger, outcome
    pub task_phase_seconds: HistogramVec,       // kind, phase=queue|wait_runtime|init|exec
    pub inflight_tasks: IntGaugeVec,            // kind
    pub admission_rejects_total: IntCounterVec, // reason
    // Scheduler
    pub sched_processors: IntGaugeVec,          // kind
    pub sched_grq_len: IntGaugeVec,             // kind
    pub sched_lrq_len: IntGaugeVec,             // p
    pub sched_steals_total: IntCounterVec,      // kind
    pub sched_rekinds_total: IntCounterVec,     // from, to
    pub sched_preemptions_total: IntCounterVec, // kind
    pub sched_hog_handoffs_total: IntCounter,
    pub sched_spinning_seconds_total: Gauge,
    pub sysmon_tick_seconds: Histogram,
    // Memory
    pub mem_budget_bytes: IntGauge,
    pub mem_committed_bytes: IntGaugeVec, // category
    pub mem_rss_bytes: IntGauge,
    pub mem_fragmentation_ratio: Gauge,
    pub mem_pressure_level: IntGauge,
    // Cache
    pub cache_entries: IntGaugeVec,           // tier
    pub cache_bytes: IntGaugeVec,             // tier
    pub cache_hits_total: IntCounterVec,      // tier
    pub cache_misses_total: IntCounterVec,    // tier
    pub cache_evictions_total: IntCounterVec, // tier, reason
    /// T1 inserts the memory governor refused while nothing was evictable (held unreserved).
    pub cache_unreserved_inserts_total: IntCounter,
    // Executors
    pub deno_isolate_create_seconds: Histogram,
    pub deno_code_cache_total: IntCounterVec, // result=hit|miss|rejected
    pub wasm_instantiate_seconds: Histogram,
    pub wasm_pool_slots: IntGauge,
    // Build
    pub build_total: IntCounterVec,  // kind, result
    pub build_seconds: HistogramVec, // kind
    // I/O
    pub http_requests_total: IntCounterVec, // listener, status_class
    /// Connections closed at accept by `ingress.max_conns_per_ip`.
    pub ingress_conn_rejected_total: IntCounter,
    pub egress_requests_total: IntCounterVec, // result
    pub egress_bytes_total: IntCounter,
    // Triggers
    pub schedule_fires_total: IntCounter,
    pub schedule_skipped_total: IntCounter,
    /// Fires dropped because the tenant's plan no longer allows the schedule's trigger.
    pub schedule_denied_total: IntCounter,
    pub queue_depth: IntGauge,
    pub queue_dlq_total: IntCounter,
    // Logs
    pub logs_records_total: IntCounterVec, // kind
    pub logs_dropped_total: IntCounter,
    // WebSockets
    pub ws_sockets: IntGauge,
    pub ws_upgrades_total: IntCounterVec,    // result = ok | limited
    pub ws_messages_total: IntCounterVec,    // dir = in | out
    pub ws_event_tasks_total: IntCounterVec, // outcome = ok | failed | given_up
    pub ws_closed_total: IntCounterVec,      // code
    // Opt-in per-service
    pub service_requests_total: Option<IntCounterVec>, // service, status_class
    pub service_duration_seconds: Option<HistogramVec>, // service
    _gauge_vec: Option<GaugeVec>,
    opts: MetricsOptions,
    /// Per-service request counts and the services that own a label (first come, up to top-N).
    service_traffic: Mutex<ServiceTraffic>,
    /// Scheduler counters seen at the previous `observe_sched` (per kind: steals, preemptions).
    sched_seen: Mutex<[(u64, u64); 3]>,
}

#[derive(Default)]
struct ServiceTraffic {
    counts: HashMap<ServiceId, u64>,
    labelled: HashSet<ServiceId>,
}

const SERVICE_OTHER: &str = "_other";
const PHASES: [&str; 4] = ["queue", "wait_runtime", "init", "exec"];
const TIERS: [&str; 2] = ["t1", "t2"];
/// Latency buckets from 0.5 ms to 2 minutes (a build can run that long).
const LATENCY_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];

fn counter_vec(r: &Registry, name: &str, help: &str, labels: &[&str]) -> Result<IntCounterVec, ObsError> {
    let v = IntCounterVec::new(Opts::new(name, help), labels)?;
    r.register(Box::new(v.clone()))?;
    Ok(v)
}

fn gauge_vec_int(r: &Registry, name: &str, help: &str, labels: &[&str]) -> Result<IntGaugeVec, ObsError> {
    let v = IntGaugeVec::new(Opts::new(name, help), labels)?;
    r.register(Box::new(v.clone()))?;
    Ok(v)
}

fn hist_vec(r: &Registry, name: &str, help: &str, labels: &[&str]) -> Result<HistogramVec, ObsError> {
    let v = HistogramVec::new(HistogramOpts::new(name, help).buckets(LATENCY_BUCKETS.to_vec()), labels)?;
    r.register(Box::new(v.clone()))?;
    Ok(v)
}

fn hist(r: &Registry, name: &str, help: &str) -> Result<Histogram, ObsError> {
    let h = Histogram::with_opts(HistogramOpts::new(name, help).buckets(LATENCY_BUCKETS.to_vec()))?;
    r.register(Box::new(h.clone()))?;
    Ok(h)
}

fn int_counter(r: &Registry, name: &str, help: &str) -> Result<IntCounter, ObsError> {
    let c = IntCounter::new(name, help)?;
    r.register(Box::new(c.clone()))?;
    Ok(c)
}

fn int_gauge(r: &Registry, name: &str, help: &str) -> Result<IntGauge, ObsError> {
    let g = IntGauge::new(name, help)?;
    r.register(Box::new(g.clone()))?;
    Ok(g)
}

fn kind_idx(kind: TaskKind) -> usize {
    kind.idx()
}

fn status_class(status: u16) -> &'static str {
    match status / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        _ => "5xx",
    }
}

impl Metrics {
    /// Register every metric in a fresh registry, pre-seeding label values so all names are
    /// visible in the first scrape.
    pub fn new(opts: MetricsOptions) -> Result<Arc<Metrics>, ObsError> {
        use names::*;
        let r = Registry::new();
        let kinds = [
            TaskKind::DENO.as_str(),
            TaskKind::WASM.as_str(),
            TaskKind::Build.as_str(),
        ];

        let tasks_total = counter_vec(
            &r,
            TASKS_TOTAL,
            "Finished tasks by kind, trigger and outcome",
            &["kind", "trigger", "outcome"],
        )?;
        let task_phase_seconds = hist_vec(&r, TASK_PHASE_SECONDS, "Task phase durations", &["kind", "phase"])?;
        let inflight_tasks = gauge_vec_int(&r, INFLIGHT_TASKS, "Admitted, unfinished tasks by kind", &["kind"])?;
        let admission_rejects_total =
            counter_vec(&r, ADMISSION_REJECTS_TOTAL, "Tasks refused before running", &["reason"])?;

        let sched_processors = gauge_vec_int(&r, SCHED_PROCESSORS, "Processors (Ps) by kind", &["kind"])?;
        let sched_grq_len = gauge_vec_int(&r, SCHED_GRQ_LEN, "Global run queue length by kind", &["kind"])?;
        let sched_lrq_len = gauge_vec_int(&r, SCHED_LRQ_LEN, "Local run queue length by P", &["p"])?;
        let sched_steals_total = counter_vec(&r, SCHED_STEALS_TOTAL, "Work steals by kind", &["kind"])?;
        let sched_rekinds_total = counter_vec(&r, SCHED_REKINDS_TOTAL, "Processor re-kindings", &["from", "to"])?;
        let sched_preemptions_total = counter_vec(&r, SCHED_PREEMPTIONS_TOTAL, "Preemptions by kind", &["kind"])?;
        let sched_hog_handoffs_total = int_counter(&r, SCHED_HOG_HANDOFFS_TOTAL, "Hog handoffs")?;
        let sched_spinning_seconds_total = Gauge::new(SCHED_SPINNING_SECONDS_TOTAL, "Cumulative spinning time")?;
        r.register(Box::new(sched_spinning_seconds_total.clone()))?;
        let sysmon_tick_seconds = hist(&r, SYSMON_TICK_SECONDS, "Sysmon tick duration")?;

        let mem_budget_bytes = int_gauge(&r, MEM_BUDGET_BYTES, "Memory budget")?;
        let mem_committed_bytes = gauge_vec_int(&r, MEM_COMMITTED_BYTES, "Committed bytes by category", &["category"])?;
        let mem_rss_bytes = int_gauge(&r, MEM_RSS_BYTES, "Process RSS")?;
        let mem_fragmentation_ratio = Gauge::new(MEM_FRAGMENTATION_RATIO, "rss / (committed + baseline rss)")?;
        r.register(Box::new(mem_fragmentation_ratio.clone()))?;
        let mem_pressure_level = int_gauge(&r, MEM_PRESSURE_LEVEL, "0 normal, 1 high, 2 critical")?;

        let cache_entries = gauge_vec_int(&r, CACHE_ENTRIES, "Cache entries by tier", &["tier"])?;
        let cache_bytes = gauge_vec_int(&r, CACHE_BYTES, "Cache bytes by tier", &["tier"])?;
        let cache_hits_total = counter_vec(&r, CACHE_HITS_TOTAL, "Cache hits by tier", &["tier"])?;
        let cache_misses_total = counter_vec(&r, CACHE_MISSES_TOTAL, "Cache misses by tier", &["tier"])?;
        let cache_evictions_total = counter_vec(&r, CACHE_EVICTIONS_TOTAL, "Cache evictions", &["tier", "reason"])?;
        let cache_unreserved_inserts_total = int_counter(
            &r,
            CACHE_UNRESERVED_INSERTS_TOTAL,
            "T1 inserts made without a memory reservation",
        )?;

        let deno_isolate_create_seconds = hist(&r, DENO_ISOLATE_CREATE_SECONDS, "Deno isolate creation time")?;
        let deno_code_cache_total = counter_vec(&r, DENO_CODE_CACHE_TOTAL, "V8 code cache results", &["result"])?;
        let wasm_instantiate_seconds = hist(&r, WASM_INSTANTIATE_SECONDS, "WASM instantiation time")?;
        let wasm_pool_slots = int_gauge(&r, WASM_POOL_SLOTS, "WASM pooling allocator slots in use")?;

        let build_total = counter_vec(&r, BUILD_TOTAL, "Builds by kind and result", &["kind", "result"])?;
        let build_seconds = hist_vec(&r, BUILD_SECONDS, "Build durations", &["kind"])?;

        let http_requests_total = counter_vec(&r, HTTP_REQUESTS_TOTAL, "HTTP requests", &["listener", "status_class"])?;
        let ingress_conn_rejected_total = int_counter(
            &r,
            INGRESS_CONN_REJECTED_TOTAL,
            "Connections closed at accept by the per-IP connection cap",
        )?;
        let egress_requests_total = counter_vec(&r, EGRESS_REQUESTS_TOTAL, "Guest egress requests", &["result"])?;
        let egress_bytes_total = int_counter(&r, EGRESS_BYTES_TOTAL, "Guest egress bytes")?;

        let schedule_fires_total = int_counter(&r, SCHEDULE_FIRES_TOTAL, "Schedule fires")?;
        let schedule_skipped_total = int_counter(&r, SCHEDULE_SKIPPED_TOTAL, "Skipped schedule fires")?;
        let schedule_denied_total = int_counter(
            &r,
            SCHEDULE_DENIED_TOTAL,
            "Schedule fires dropped: trigger not allowed by the plan",
        )?;
        let queue_depth = int_gauge(&r, QUEUE_DEPTH, "Queue depth")?;
        let queue_dlq_total = int_counter(&r, QUEUE_DLQ_TOTAL, "Messages moved to the dead-letter queue")?;

        let logs_records_total = counter_vec(&r, LOGS_RECORDS_TOTAL, "Log records by kind", &["kind"])?;
        let logs_dropped_total = int_counter(&r, LOGS_DROPPED_TOTAL, "Log lines dropped by the rate limit")?;

        let ws_sockets = int_gauge(&r, WS_SOCKETS, "Open WebSockets the node holds")?;
        let ws_upgrades_total = counter_vec(&r, WS_UPGRADES_TOTAL, "WebSocket accepts by result", &["result"])?;
        let ws_messages_total = counter_vec(&r, WS_MESSAGES_TOTAL, "WebSocket messages by direction", &["dir"])?;
        let ws_event_tasks_total = counter_vec(
            &r,
            WS_EVENT_TASKS_TOTAL,
            "Socket event and alarm tasks by outcome",
            &["outcome"],
        )?;
        let ws_closed_total = counter_vec(&r, WS_CLOSED_TOTAL, "Closed WebSockets by close code", &["code"])?;

        let (service_requests_total, service_duration_seconds) = if opts.per_service {
            (
                Some(counter_vec(
                    &r,
                    SERVICE_REQUESTS_TOTAL,
                    "Per-service requests",
                    &["service", "status_class"],
                )?),
                Some(hist_vec(
                    &r,
                    SERVICE_DURATION_SECONDS,
                    "Per-service duration",
                    &["service"],
                )?),
            )
        } else {
            (None, None)
        };

        // Pre-seed one series per labelled metric so the first scrape lists every name. Histogram
        // vecs get only the `deno` series (the others appear on first use).
        for k in kinds {
            inflight_tasks.with_label_values(&[k]).set(0);
            sched_processors.with_label_values(&[k]).set(0);
            sched_grq_len.with_label_values(&[k]).set(0);
            sched_steals_total.with_label_values(&[k]).inc_by(0);
            sched_preemptions_total.with_label_values(&[k]).inc_by(0);
        }
        for phase in PHASES {
            task_phase_seconds.with_label_values(&[TaskKind::DENO.as_str(), phase]);
        }
        tasks_total
            .with_label_values(&[TaskKind::DENO.as_str(), "http", "ok"])
            .inc_by(0);
        for reason in RejectReason::ALL {
            admission_rejects_total.with_label_values(&[reason.reason()]).inc_by(0);
        }
        sched_lrq_len.with_label_values(&["0"]).set(0);
        sched_rekinds_total.with_label_values(&["deno", "wasm"]).inc_by(0);
        sched_rekinds_total.with_label_values(&["wasm", "deno"]).inc_by(0);
        for cat in rt_mem_categories() {
            mem_committed_bytes.with_label_values(&[cat]).set(0);
        }
        for tier in TIERS {
            cache_entries.with_label_values(&[tier]).set(0);
            cache_bytes.with_label_values(&[tier]).set(0);
            cache_hits_total.with_label_values(&[tier]).inc_by(0);
            cache_misses_total.with_label_values(&[tier]).inc_by(0);
        }
        for (tier, reason) in [("t1", "pressure"), ("t1", "ttl"), ("t1", "capacity"), ("t2", "gc")] {
            cache_evictions_total.with_label_values(&[tier, reason]).inc_by(0);
        }
        for result in ["hit", "miss", "rejected"] {
            deno_code_cache_total.with_label_values(&[result]).inc_by(0);
        }
        for kind in ["deno", "wasm"] {
            build_total.with_label_values(&[kind, "ok"]).inc_by(0);
            build_seconds.with_label_values(&[kind]);
        }
        http_requests_total.with_label_values(&["public", "2xx"]).inc_by(0);
        for result in ["ok", "denied"] {
            egress_requests_total.with_label_values(&[result]).inc_by(0);
        }
        for kind in ["runtime", "build", "system"] {
            logs_records_total.with_label_values(&[kind]).inc_by(0);
        }
        for result in ["ok", "limited"] {
            ws_upgrades_total.with_label_values(&[result]).inc_by(0);
        }
        for dir in ["in", "out"] {
            ws_messages_total.with_label_values(&[dir]).inc_by(0);
        }
        for outcome in ["ok", "failed", "given_up"] {
            ws_event_tasks_total.with_label_values(&[outcome]).inc_by(0);
        }
        ws_closed_total.with_label_values(&["1000"]).inc_by(0);

        Ok(Arc::new(Metrics {
            registry: r,
            tasks_total,
            task_phase_seconds,
            inflight_tasks,
            admission_rejects_total,
            sched_processors,
            sched_grq_len,
            sched_lrq_len,
            sched_steals_total,
            sched_rekinds_total,
            sched_preemptions_total,
            sched_hog_handoffs_total,
            sched_spinning_seconds_total,
            sysmon_tick_seconds,
            mem_budget_bytes,
            mem_committed_bytes,
            mem_rss_bytes,
            mem_fragmentation_ratio,
            mem_pressure_level,
            cache_entries,
            cache_bytes,
            cache_hits_total,
            cache_misses_total,
            cache_evictions_total,
            cache_unreserved_inserts_total,
            deno_isolate_create_seconds,
            deno_code_cache_total,
            wasm_instantiate_seconds,
            wasm_pool_slots,
            build_total,
            build_seconds,
            http_requests_total,
            ingress_conn_rejected_total,
            egress_requests_total,
            egress_bytes_total,
            schedule_fires_total,
            schedule_skipped_total,
            schedule_denied_total,
            queue_depth,
            queue_dlq_total,
            logs_records_total,
            logs_dropped_total,
            ws_sockets,
            ws_upgrades_total,
            ws_messages_total,
            ws_event_tasks_total,
            ws_closed_total,
            service_requests_total,
            service_duration_seconds,
            _gauge_vec: None,
            opts,
            service_traffic: Mutex::new(ServiceTraffic::default()),
            sched_seen: Mutex::new([(0, 0); 3]),
        }))
    }

    /// Prometheus text exposition of the whole registry.
    pub fn render(&self) -> String {
        let mut out = String::new();
        if TextEncoder::new()
            .encode_utf8(&self.registry.gather(), &mut out)
            .is_err()
        {
            tracing::warn!("failed to encode metrics");
        }
        out
    }

    /// Record a finished task: counters, phase histograms and (opt-in) per-service series.
    pub fn task_finished(
        &self,
        kind: TaskKind,
        trigger: &str,
        service: ServiceId,
        outcome: &Outcome,
        timings: &TaskTimings,
    ) {
        let k = kind.as_str();
        self.tasks_total.with_label_values(&[k, trigger, outcome.label()]).inc();
        if let Outcome::Rejected(reason) = outcome {
            self.admission_rejects_total.with_label_values(&[reason.reason()]).inc();
        }
        for (phase, d) in [
            ("queue", timings.queue),
            ("wait_runtime", timings.wait_runtime),
            ("init", timings.init),
            ("exec", timings.exec),
        ] {
            self.task_phase_seconds
                .with_label_values(&[k, phase])
                .observe(d.as_secs_f64());
        }
        if let (Some(requests), Some(durations)) = (&self.service_requests_total, &self.service_duration_seconds) {
            let label = self.admit_service(service);
            requests
                .with_label_values(&[label.as_str(), status_class(outcome.http_status())])
                .inc();
            let total = timings.queue + timings.wait_runtime + timings.init + timings.exec;
            durations
                .with_label_values(&[label.as_str()])
                .observe(total.as_secs_f64());
        }
    }

    /// Copy a scheduler snapshot into gauges (`rt_sched_*`, `rt_inflight_tasks`).
    pub fn observe_sched(&self, snap: &SchedSnapshot) {
        let mut procs = [0i64; 3];
        let mut busy = [0i64; 3];
        let mut lrq = [0i64; 3];
        let mut totals = [(0u64, 0u64); 3];
        self.sched_lrq_len.reset();
        for p in &snap.procs {
            let i = kind_idx(p.kind);
            procs[i] += 1;
            lrq[i] += i64::from(p.lrq_len);
            busy[i] += i64::from(p.status == rt_types::ProcStatus::Running);
            totals[i].0 += p.steals_in;
            totals[i].1 += p.preemptions;
            self.sched_lrq_len
                .with_label_values(&[&p.id.0.to_string()])
                .set(i64::from(p.lrq_len));
        }
        let mut seen = self.sched_seen.lock();
        for kind in TaskKind::ALL {
            let i = kind_idx(kind);
            let k = kind.as_str();
            self.sched_processors.with_label_values(&[k]).set(procs[i]);
            self.sched_grq_len
                .with_label_values(&[k])
                .set(i64::from(snap.grq.get(kind)));
            self.inflight_tasks
                .with_label_values(&[k])
                .set(busy[i] + lrq[i] + i64::from(snap.grq.get(kind)));
            self.sched_steals_total
                .with_label_values(&[k])
                .inc_by(totals[i].0.saturating_sub(seen[i].0));
            self.sched_preemptions_total
                .with_label_values(&[k])
                .inc_by(totals[i].1.saturating_sub(seen[i].1));
            seen[i] = totals[i];
        }
    }

    /// The `service` label for per-service series: the ID for the top-N services by traffic,
    /// `_other` for the rest.
    pub fn service_label(&self, service: ServiceId) -> String {
        if self.service_traffic.lock().labelled.contains(&service) {
            service.to_string()
        } else {
            SERVICE_OTHER.to_string()
        }
    }

    /// Count one request for `service` and return its label. Services are admitted to the labelled
    /// set first-come up to `per_service_top_n`, which keeps the series count bounded for good.
    fn admit_service(&self, service: ServiceId) -> String {
        let mut t = self.service_traffic.lock();
        *t.counts.entry(service).or_insert(0) += 1;
        if t.labelled.contains(&service)
            || (t.labelled.len() < self.opts.per_service_top_n && t.labelled.insert(service))
        {
            return service.to_string();
        }
        // bound the bookkeeping for the long tail
        if t.counts.len() > self.opts.per_service_top_n.saturating_mul(20).max(1000) {
            let keep: HashSet<ServiceId> = t.labelled.clone();
            t.counts.retain(|s, _| keep.contains(s));
        }
        SERVICE_OTHER.to_string()
    }
}

fn rt_mem_categories() -> [&'static str; 8] {
    [
        "deno_heap",
        "wasm_linear",
        "wasm_reuse",
        "t1_cache",
        "io_buffers",
        "sql_cache",
        "logs",
        "other",
    ]
}
