//! Buffered segment appends (B7): records reach the segment files within the flush interval
//! without an explicit flush, reads always see buffered records, and the record / drop counters
//! are published to the `rt_logs_*` metrics in batches.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rt_obs::{
    FileLogStore, LogKind, LogLevel, LogQuery, LogRecord, LogSink, LogStore, LogStoreConfig, Metrics, MetricsOptions,
};
use rt_types::{ServiceId, VersionNo};

const NOW: i64 = 1_760_000_000_000;

fn unlimited(dir: &Path) -> LogStoreConfig {
    let mut c = LogStoreConfig::defaults(dir);
    c.rate_per_sec = 1_000_000;
    c.burst = 1_000_000;
    c
}

fn rec(svc: ServiceId, ts_ms: i64, kind: LogKind, msg: impl Into<String>) -> LogRecord {
    let mut r = LogRecord::new(svc, VersionNo(1), LogLevel::Info, kind, msg);
    r.ts_ms = ts_ms;
    r
}

fn seg_text(dir: &Path, svc: ServiceId) -> String {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join(svc.to_string()))
        .map(|rd| rd.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    files.sort();
    files
        .iter()
        .map(|f| std::fs::read_to_string(f).unwrap_or_default())
        .collect()
}

/// Polls `cond` until it holds or `within` passes.
fn eventually(within: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    cond()
}

#[test]
fn appends_are_buffered_and_reach_the_segment_within_the_flush_interval() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.flush_interval = Duration::from_millis(50);
    let store = FileLogStore::open(cfg).unwrap();
    let svc = ServiceId::new();
    store.append(rec(svc, NOW, LogKind::Runtime, "first-line"));
    assert!(
        eventually(Duration::from_secs(3), || seg_text(dir.path(), svc)
            .contains("first-line")),
        "the background flush must write the record without an explicit flush()"
    );
}

#[test]
fn a_long_flush_interval_keeps_records_in_memory_until_flush() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.flush_interval = Duration::from_secs(3_600);
    let store = FileLogStore::open(cfg).unwrap();
    let svc = ServiceId::new();
    for i in 0..10 {
        store.append(rec(svc, NOW + i, LogKind::Runtime, format!("buffered-{i}")));
    }
    assert!(
        !seg_text(dir.path(), svc).contains("buffered-9"),
        "appends are buffered, not one write per record"
    );
    store.flush().unwrap();
    assert!(
        seg_text(dir.path(), svc).contains("buffered-9"),
        "flush() writes the buffer through"
    );
}

#[test]
fn queries_and_tails_see_buffered_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.flush_interval = Duration::from_secs(3_600);
    cfg.ring_lines = 2;
    let store = FileLogStore::open(cfg).unwrap();
    let svc = ServiceId::new();
    for i in 0..6 {
        store.append(rec(svc, NOW + i, LogKind::Runtime, format!("m{i}")));
    }
    let mut q = LogQuery::for_service(svc);
    q.limit = 100;
    let got: Vec<String> = store.query(q).records.into_iter().map(|r| r.msg).collect();
    assert_eq!(
        got,
        ["m0", "m1", "m2", "m3", "m4", "m5"],
        "a query reads the segments after flushing the buffer"
    );

    // attaching a tail flushes too, so an older cursor (served from disk, not the 2-line ring) is complete
    store.append(rec(svc, NOW + 10, LogKind::Runtime, "m6"));
    let _tail = store.tail(svc, Some(rt_obs::LogCursor(0)));
    assert!(
        seg_text(dir.path(), svc).contains("\"m6\""),
        "attaching a tail flushes the service's buffer"
    );
}

#[test]
fn dropping_the_store_flushes_the_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let svc = ServiceId::new();
    {
        let mut cfg = unlimited(dir.path());
        cfg.flush_interval = Duration::from_secs(3_600);
        let store = FileLogStore::open(cfg).unwrap();
        store.append(rec(svc, NOW, LogKind::Runtime, "last-words"));
    }
    assert!(seg_text(dir.path(), svc).contains("last-words"));
}

fn metrics() -> Arc<Metrics> {
    Metrics::new(MetricsOptions::default()).unwrap()
}

#[test]
fn record_and_drop_counts_are_published_to_the_log_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = LogStoreConfig::defaults(dir.path());
    cfg.rate_per_sec = 1;
    cfg.burst = 5;
    cfg.flush_interval = Duration::from_secs(3_600);
    let store = FileLogStore::open(cfg).unwrap();
    let m = metrics();
    store.attach_metrics(m.clone());
    let (a, b) = (ServiceId::new(), ServiceId::new());
    for i in 0..3 {
        store.append(rec(a, NOW + i, LogKind::Runtime, format!("r{i}")));
    }
    store.append(rec(a, NOW + 3, LogKind::Build, "b0"));
    for i in 0..20 {
        store.append(rec(b, NOW + i, LogKind::Runtime, format!("x{i}")));
    }
    store.flush().unwrap();

    let dropped = store.dropped_total();
    assert!(dropped >= 14, "service b keeps about its burst of 5: dropped {dropped}");
    assert_eq!(
        m.logs_dropped_total.get(),
        dropped,
        "rt_logs_dropped_total equals the store's count"
    );
    assert_eq!(m.logs_records_total.with_label_values(&["build"]).get(), 1);
    // flush() emits the `"N lines dropped"` summary of service b, a `system` record
    assert_eq!(m.logs_records_total.with_label_values(&["system"]).get(), 1);
    assert_eq!(
        m.logs_records_total.with_label_values(&["runtime"]).get(),
        3 + (20 - dropped)
    );

    // publishing is incremental: a second flush adds nothing, new records add exactly themselves
    store.flush().unwrap();
    assert_eq!(
        m.logs_records_total.with_label_values(&["runtime"]).get(),
        3 + (20 - dropped)
    );
    store.append(rec(a, NOW + 50, LogKind::Runtime, "r-late"));
    store.flush().unwrap();
    assert_eq!(
        m.logs_records_total.with_label_values(&["runtime"]).get(),
        4 + (20 - dropped)
    );
}

#[test]
fn the_background_flush_publishes_metrics_and_summarizes_drops_without_further_appends() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = LogStoreConfig::defaults(dir.path());
    cfg.rate_per_sec = 1;
    cfg.burst = 2;
    cfg.flush_interval = Duration::from_millis(50);
    let store = FileLogStore::open(cfg).unwrap();
    let m = metrics();
    store.attach_metrics(m.clone());
    let svc = ServiceId::new();
    for i in 0..10 {
        store.append(rec(svc, NOW + i, LogKind::Runtime, format!("l{i}")));
    }
    let dropped = store.dropped_total();
    assert!(dropped >= 7, "dropped {dropped}");
    // the summary waits for a quiet second, then the flusher writes it: no later append needed
    assert!(
        eventually(Duration::from_secs(5), || seg_text(dir.path(), svc)
            .contains(&format!("{dropped} lines dropped"))),
        "the drop summary is written by the background flush"
    );
    assert!(eventually(Duration::from_secs(2), || m.logs_dropped_total.get() == dropped));
}
