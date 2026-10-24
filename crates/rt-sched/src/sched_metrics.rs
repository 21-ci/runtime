//! Collectors rt-sched registers on the node registry (`rt_obs::Metrics::registry`), listed in
//! Admission and autoscaling (docs/deploy.md). Per-service series exist only for services that own a
//! per-service label (`metrics.per_service`, top-N by traffic), so their cardinality is bounded by
//! `metrics.per_service_top_n`; everything else is node-level with fixed labels.

use prometheus::{
    GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;
use rt_obs::Metrics;
use rt_types::{ServiceId, TaskKind};

use crate::admission::ServiceReport;

/// Admission waits from 1 ms to 30 s.
const WAIT_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Ticks between two label lookups of a service.
pub(crate) const LABEL_REFRESH: u64 = 10;

#[derive(Default)]
struct LabelCache {
    tick: u64,
    /// `None`: the service has no label of its own (`_other`).
    labels: HashMap<ServiceId, Option<String>>,
    /// Labels with series right now.
    exported: HashSet<String>,
}

pub(crate) struct SchedMetrics {
    admission_running: IntGauge,
    admission_queued: IntGauge,
    admission_queued_bytes: IntGauge,
    pub admission_queue_wait_seconds: Histogram,
    autoscale_adjustments_total: IntCounterVec, // direction, cause
    warm_target_instances: IntGaugeVec,         // kind
    pub spinner_mismatch_total: IntCounter,
    service_concurrency_limit: IntGaugeVec, // service
    service_running: IntGaugeVec,           // service
    service_queued: IntGaugeVec,            // service
    service_queue_wait_seconds: GaugeVec,   // service
    service_min_instances: IntGaugeVec,     // service
    service_max_instances: IntGaugeVec,     // service
    service_target_instances: IntGaugeVec,  // service
    service_arrival_rate: GaugeVec,         // service
    labels: Mutex<LabelCache>,
}

fn register<C: prometheus::core::Collector + Clone + 'static>(r: &Registry, c: C) -> C {
    // A second scheduler on the same registry (tests) keeps its own, unexported copy.
    if let Err(e) = r.register(Box::new(c.clone())) {
        tracing::debug!(error = %e, "scheduler metric not registered");
    }
    c
}

impl SchedMetrics {
    pub fn new(m: &Metrics) -> Option<Self> {
        let r = &m.registry;
        let g = |name: &str, help: &str| IntGauge::new(name, help).ok().map(|c| register(r, c));
        let gv = |name: &str, help: &str, labels: &[&str]| {
            IntGaugeVec::new(Opts::new(name, help), labels)
                .ok()
                .map(|c| register(r, c))
        };
        Some(Self {
            admission_running: g("rt_admission_running", "Tasks holding a run slot")?,
            admission_queued: g("rt_admission_queued", "Tasks waiting in service queues for a run slot")?,
            admission_queued_bytes: g("rt_admission_queued_bytes", "Bytes charged to waiting tasks")?,
            admission_queue_wait_seconds: Histogram::with_opts(
                HistogramOpts::new(
                    "rt_admission_queue_wait_seconds",
                    "Admission wait of tasks that queued, until granted",
                )
                .buckets(WAIT_BUCKETS.to_vec()),
            )
            .ok()
            .map(|c| register(r, c))?,
            autoscale_adjustments_total: IntCounterVec::new(
                Opts::new("rt_autoscale_adjustments_total", "Adaptive concurrency limit changes"),
                &["direction", "cause"],
            )
            .ok()
            .map(|c| register(r, c))?,
            warm_target_instances: gv(
                "rt_warm_target_instances",
                "Warm instances the autoscaler asks executors to keep",
                &["kind"],
            )?,
            spinner_mismatch_total: IntCounter::new(
                "rt_sched_spinner_mismatch_total",
                "Sysmon checks where a kind's spinner count disagreed with the spinning Machines twice in a row",
            )
            .ok()
            .map(|c| register(r, c))?,
            service_concurrency_limit: gv(
                "rt_service_concurrency_limit",
                "Adaptive concurrency limit",
                &["service"],
            )?,
            service_running: gv(
                "rt_service_running",
                "Tasks of the service holding a run slot",
                &["service"],
            )?,
            service_queued: gv(
                "rt_service_queued",
                "Tasks of the service waiting for a run slot",
                &["service"],
            )?,
            service_queue_wait_seconds: GaugeVec::new(
                Opts::new(
                    "rt_service_queue_wait_seconds",
                    "Mean admission wait over the last autoscaler tick",
                ),
                &["service"],
            )
            .ok()
            .map(|c| register(r, c))?,
            service_min_instances: gv(
                "rt_service_min_instances",
                "Instances the service keeps warm (scaling.min_instances, floor of the limit)",
                &["service"],
            )?,
            service_max_instances: gv(
                "rt_service_max_instances",
                "Most instances the service may run at once (ceiling of the limit)",
                &["service"],
            )?,
            service_target_instances: gv(
                "rt_service_target_instances",
                "Warm instances the autoscaler asks executors to keep for the service",
                &["service"],
            )?,
            service_arrival_rate: GaugeVec::new(
                Opts::new(
                    "rt_service_arrival_rate",
                    "Smoothed arrival rate of the service, requests per second",
                ),
                &["service"],
            )
            .ok()
            .map(|c| register(r, c))?,
            labels: Mutex::new(LabelCache::default()),
        })
    }

    /// Publish one autoscaler tick. Per-service series exist only with `metrics.per_service`
    /// and only for services that own a label (resolved at most once per [`LABEL_REFRESH`]
    /// ticks per service, so the traffic table's lock is not taken per service per tick) and
    /// had activity this tick; series of services that went quiet are removed (no reset, so a
    /// scrape never sees the active ones missing).
    pub fn observe_tick(
        &self,
        m: &Metrics,
        running: u32,
        queued: (u32, u64),
        reports: &[ServiceReport],
        warm: [u64; 3],
        targets: &HashMap<ServiceId, u32>,
    ) {
        self.admission_running.set(i64::from(running));
        self.admission_queued.set(i64::from(queued.0));
        self.admission_queued_bytes
            .set(i64::try_from(queued.1).unwrap_or(i64::MAX));
        for kind in [TaskKind::DENO, TaskKind::WASM] {
            self.warm_target_instances
                .with_label_values(&[kind.as_str()])
                .set(i64::try_from(warm[kind.idx()]).unwrap_or(i64::MAX));
        }
        for r in reports {
            if let Some((old, cause)) = r.change {
                let dir = if r.admission.limit > old { "up" } else { "down" };
                self.autoscale_adjustments_total
                    .with_label_values(&[dir, cause.as_str()])
                    .inc();
            }
        }
        let mut cache = self.labels.lock();
        let per_service = m.service_requests_total.is_some();
        cache.tick += 1;
        if cache.tick.is_multiple_of(LABEL_REFRESH) || !per_service {
            cache.labels.clear();
        }
        let mut exported = HashSet::new();
        if per_service {
            for r in reports {
                let a = &r.admission;
                if a.running == 0 && a.queued == 0 && r.change.is_none() && r.mean_wait.is_none() {
                    continue;
                }
                let label = cache
                    .labels
                    .entry(a.service)
                    .or_insert_with(|| Some(m.service_label(a.service)).filter(|l| l != "_other"))
                    .clone();
                let Some(label) = label else { continue };
                self.service_concurrency_limit
                    .with_label_values(&[&label])
                    .set(i64::from(a.limit));
                self.service_running
                    .with_label_values(&[&label])
                    .set(i64::from(a.running));
                self.service_queued
                    .with_label_values(&[&label])
                    .set(i64::from(a.queued));
                self.service_queue_wait_seconds
                    .with_label_values(&[&label])
                    .set(r.mean_wait.map_or(0.0, |w| w.as_secs_f64()));
                self.service_min_instances
                    .with_label_values(&[&label])
                    .set(i64::from(r.meta.floor_instances()));
                self.service_max_instances
                    .with_label_values(&[&label])
                    .set(i64::from(a.max));
                self.service_target_instances
                    .with_label_values(&[&label])
                    .set(i64::from(targets.get(&a.service).copied().unwrap_or(0)));
                self.service_arrival_rate
                    .with_label_values(&[&label])
                    .set(if r.rps.is_finite() { r.rps } else { 0.0 });
                exported.insert(label);
            }
        }
        for gone in cache.exported.difference(&exported) {
            let _ = self.service_concurrency_limit.remove_label_values(&[gone]);
            let _ = self.service_running.remove_label_values(&[gone]);
            let _ = self.service_queued.remove_label_values(&[gone]);
            let _ = self.service_queue_wait_seconds.remove_label_values(&[gone]);
            let _ = self.service_min_instances.remove_label_values(&[gone]);
            let _ = self.service_max_instances.remove_label_values(&[gone]);
            let _ = self.service_target_instances.remove_label_values(&[gone]);
            let _ = self.service_arrival_rate.remove_label_values(&[gone]);
        }
        cache.exported = exported;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::{ServiceAdmission, SvcMeta};
    use rt_obs::MetricsOptions;
    use rt_types::{Digest, Outcome, ScalingKnobs, ServiceRef, TaskTimings, TenantId, VersionNo};
    use std::time::Duration;

    fn report(service: u128, running: u32) -> ServiceReport {
        let sref = ServiceRef {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(service),
            version: VersionNo(1),
            digest: Digest::of(b"x"),
        };
        ServiceReport {
            admission: ServiceAdmission {
                service: sref.service,
                tenant: sref.tenant,
                limit: 8,
                min: 1,
                max: 64,
                running,
                queued: 0,
                queued_bytes: 0,
            },
            meta: SvcMeta {
                service: sref,
                kind: TaskKind::DENO,
                key: None,
                instance_bytes: 0,
                ceiling: 64,
                floor: 0,
                max_queued: 0,
                scaling: ScalingKnobs::default(),
                tenant_cap: 0,
                idle_ttl: Duration::from_secs(60),
                queue_timeout: Duration::from_secs(5),
            },
            demand: 0.0,
            rps: 0.0,
            mean_wait: None,
            idle_for: Duration::ZERO,
            change: None,
            timeouts: 0,
        }
    }

    fn series(m: &Metrics, name: &str) -> usize {
        m.render()
            .lines()
            .filter(|l| l.starts_with(name) && l.contains("service="))
            .count()
    }

    /// Review 8: per-service series only for labelled, active services; a quiet service's
    /// series are removed without resetting the others.
    #[test]
    fn per_service_series_follow_labelled_active_services() {
        let m = Metrics::new(MetricsOptions {
            per_service: true,
            per_service_top_n: 1,
        })
        .expect("metrics");
        let sm = SchedMetrics::new(&m).expect("collectors");
        // service 1 owns the only label (first with traffic); service 2 is `_other`
        for s in [1, 2] {
            m.task_finished(
                TaskKind::DENO,
                "http",
                ServiceId::from_u128(s),
                &Outcome::GuestError,
                &TaskTimings::default(),
            );
        }
        sm.observe_tick(&m, 2, (0, 0), &[report(1, 1), report(2, 1)], [0; 3], &HashMap::new());
        assert_eq!(
            series(&m, "rt_service_running"),
            1,
            "only the labelled service:\n{}",
            m.render()
        );
        sm.observe_tick(&m, 0, (0, 0), &[report(1, 0), report(2, 0)], [0; 3], &HashMap::new());
        assert_eq!(
            series(&m, "rt_service_running"),
            0,
            "a quiet service's series are removed"
        );
    }
}
