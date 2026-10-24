//! rt-obs guest logs (OBS-01..05): record shape and truncation, the in-memory
//! sink, and the `FileLogStore` pipeline: per-service ring with eviction order, rate limit with
//! dropped counter and synthetic "N lines dropped" record, JSONL segments round trip, queries,
//! cursors, restart, retention.
//!
//! `FileLogStore` is `todo!()` in the skeleton: tests that open it are expected to fail until it is
//! implemented. `LogRecord` and `MemoryLogSink` are real and their tests already pass.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use rt_obs::{
    FileLogStore, LogCursor, LogKind, LogLevel, LogQuery, LogRecord, LogSink, LogStore, LogStoreConfig, LogStream,
    MemoryLogSink, MAX_MSG_CHARS,
};
use rt_types::{ServiceId, TaskId, TenantId, VersionNo};

const DAY_MS: i64 = 86_400_000;
const NOW: i64 = 1_760_000_000_000;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/// Effectively no rate limit, so tests of the ring / segments are not affected by it.
fn unlimited(dir: &Path) -> LogStoreConfig {
    let mut c = LogStoreConfig::defaults(dir);
    c.rate_per_sec = 1_000_000;
    c.burst = 1_000_000;
    c
}

fn open(cfg: LogStoreConfig) -> Arc<FileLogStore> {
    FileLogStore::open(cfg).expect("open log store")
}

fn rec(svc: ServiceId, ts_ms: i64, level: LogLevel, msg: impl Into<String>) -> LogRecord {
    let mut r = LogRecord::new(svc, VersionNo(1), level, LogKind::Runtime, msg);
    r.ts_ms = ts_ms;
    r.trigger = "http".into();
    r
}

fn msgs(records: &[LogRecord]) -> Vec<String> {
    records.iter().map(|r| r.msg.clone()).collect()
}

fn seqs(records: &[LogRecord]) -> Vec<u64> {
    records.iter().map(|r| r.seq).collect()
}

/// Everything the store returns for `svc`, following cursors.
fn query_all(store: &FileLogStore, svc: ServiceId) -> Vec<LogRecord> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let mut q = LogQuery::for_service(svc);
        q.limit = 1_000;
        q.cursor = cursor;
        let page = store.query(q);
        out.extend(page.records);
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => return out,
        }
    }
}

async fn take(stream: LogStream, n: usize) -> Vec<LogRecord> {
    tokio::time::timeout(Duration::from_secs(3), stream.take(n).collect::<Vec<_>>())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {n} records"))
}

fn seg_files(dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(rd) = std::fs::read_dir(dir) else { return };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "seg") {
                out.push(p);
            }
        }
    }
    let mut v = Vec::new();
    walk(dir, &mut v);
    v.sort();
    v
}

// ---------------------------------------------------------------------------------------------
// LogRecord (real code)
// ---------------------------------------------------------------------------------------------

#[test]
fn message_truncation_is_exact_at_the_documented_limit() {
    assert_eq!(MAX_MSG_CHARS, 10_000);
    let svc = ServiceId::new();
    let exact = LogRecord::new(
        svc,
        VersionNo(1),
        LogLevel::Info,
        LogKind::Runtime,
        "x".repeat(MAX_MSG_CHARS),
    );
    assert!(!exact.truncated);
    assert_eq!(exact.msg.chars().count(), MAX_MSG_CHARS);

    let over = LogRecord::new(
        svc,
        VersionNo(1),
        LogLevel::Info,
        LogKind::Runtime,
        "x".repeat(MAX_MSG_CHARS + 1),
    );
    assert!(over.truncated, "truncated: true when cut");
    assert_eq!(over.msg.chars().count(), MAX_MSG_CHARS);

    let huge = LogRecord::new(
        svc,
        VersionNo(1),
        LogLevel::Info,
        LogKind::Runtime,
        "y".repeat(5_000_000),
    );
    assert!(huge.truncated);
    assert_eq!(huge.msg.len(), MAX_MSG_CHARS);
}

#[test]
fn truncation_counts_characters_and_never_splits_one() {
    let svc = ServiceId::new();
    for ch in ["é", "日", "🦀"] {
        let r = LogRecord::new(
            svc,
            VersionNo(1),
            LogLevel::Info,
            LogKind::Runtime,
            ch.repeat(MAX_MSG_CHARS + 7),
        );
        assert!(r.truncated, "{ch}");
        assert_eq!(r.msg.chars().count(), MAX_MSG_CHARS, "{ch}");
        assert!(
            r.msg.chars().all(|c| c.to_string() == ch),
            "{ch}: no corrupted characters"
        );
    }
    // multi-byte text exactly at the limit is not truncated even though it is longer in bytes
    let r = LogRecord::new(
        svc,
        VersionNo(1),
        LogLevel::Info,
        LogKind::Runtime,
        "日".repeat(MAX_MSG_CHARS),
    );
    assert!(!r.truncated);
    assert!(r.msg.len() > MAX_MSG_CHARS);
}

#[test]
fn truncate_msg_is_idempotent_and_keeps_the_flag() {
    let mut r = LogRecord::new(
        ServiceId::new(),
        VersionNo(1),
        LogLevel::Info,
        LogKind::Runtime,
        "z".repeat(MAX_MSG_CHARS + 5),
    );
    let once = r.msg.clone();
    r.truncate_msg();
    assert_eq!(r.msg, once);
    assert!(r.truncated);
    let mut short = LogRecord::new(
        ServiceId::new(),
        VersionNo(1),
        LogLevel::Info,
        LogKind::Runtime,
        "short",
    );
    short.truncate_msg();
    assert!(!short.truncated);
}

#[test]
fn a_new_record_has_no_sequence_task_or_tenant_yet() {
    let svc = ServiceId::new();
    let r = LogRecord::new(svc, VersionNo(7), LogLevel::Warn, LogKind::System, "cpu_limit");
    assert_eq!((r.seq, r.ts_ms), (0, 0), "seq is assigned by the store");
    assert_eq!(
        (r.service_id, r.version, r.level, r.kind),
        (svc, VersionNo(7), LogLevel::Warn, LogKind::System)
    );
    assert!(r.task_id.is_none() && r.tenant_id.is_none());
    assert_eq!(r.msg, "cpu_limit");
}

#[test]
fn log_levels_are_ordered_so_a_minimum_level_filter_works() {
    assert!(LogLevel::Debug < LogLevel::Info && LogLevel::Info < LogLevel::Warn && LogLevel::Warn < LogLevel::Error);
}

#[test]
fn records_serialize_with_lowercase_enums_and_the_task_id_header_form() {
    let mut r = rec(ServiceId::from_u128(2), 1234, LogLevel::Warn, "hello");
    r.kind = LogKind::Build;
    r.task_id = Some(TaskId(0x2a));
    r.tenant_id = Some(TenantId::from_u128(1));
    r.seq = 9;
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["level"], "warn");
    assert_eq!(v["kind"], "build");
    assert_eq!(
        v["task_id"], "task_000000000000002a",
        "task_id equals the x-rt-task-id header (OBS-01)"
    );
    assert_eq!(v["service_id"], "svc_00000000000000000000000002");
    assert_eq!(v["tenant_id"], "ten_00000000000000000000000001");
    assert_eq!(v["seq"], 9);
    assert_eq!(v["msg"], "hello");
    assert_eq!(v["trigger"], "http");
    assert_eq!(v["truncated"], false);
    assert_eq!(serde_json::from_value::<LogRecord>(v).unwrap(), r);
}

#[test]
fn records_without_seq_or_truncated_still_deserialize() {
    let json = serde_json::json!({
        "ts_ms": 5, "level": "info", "tenant_id": null, "service_id": "svc_00000000000000000000000002",
        "version": 3, "task_id": null, "trigger": "cron", "kind": "runtime", "msg": "m"
    });
    let r: LogRecord = serde_json::from_value(json).unwrap();
    assert_eq!((r.seq, r.truncated, r.version), (0, false, VersionNo(3)));
}

// ---------------------------------------------------------------------------------------------
// MemoryLogSink (real code)
// ---------------------------------------------------------------------------------------------

#[test]
fn the_memory_sink_keeps_everything_in_order_with_increasing_sequences() {
    let sink = MemoryLogSink::new();
    let svc = ServiceId::new();
    for i in 0..50 {
        sink.append(rec(svc, i, LogLevel::Info, format!("m{i}")));
    }
    let got = sink.records();
    assert_eq!(got.len(), 50);
    assert_eq!(msgs(&got), (0..50).map(|i| format!("m{i}")).collect::<Vec<_>>());
    assert_eq!(seqs(&got), (1..=50).collect::<Vec<u64>>());
    sink.clear();
    assert!(sink.records().is_empty());
}

#[test]
fn the_memory_sink_is_safe_to_share_between_threads() {
    let sink = MemoryLogSink::new();
    let svc = ServiceId::new();
    std::thread::scope(|s| {
        for t in 0..8 {
            let sink = sink.clone();
            s.spawn(move || {
                for i in 0..250 {
                    sink.append(rec(svc, 0, LogLevel::Info, format!("t{t}-{i}")));
                }
            });
        }
    });
    let got = sink.records();
    assert_eq!(got.len(), 2_000);
    let mut s = seqs(&got);
    s.sort_unstable();
    s.dedup();
    assert_eq!(s.len(), 2_000, "sequences must be unique");
    // within one thread the order is preserved
    for t in 0..8 {
        let mine: Vec<usize> = got
            .iter()
            .filter(|r| r.msg.starts_with(&format!("t{t}-")))
            .map(|r| r.msg.rsplit('-').next().unwrap().parse().unwrap())
            .collect();
        assert!(mine.windows(2).all(|w| w[0] < w[1]), "thread {t} records out of order");
    }
}

// ---------------------------------------------------------------------------------------------
// Ring (OBS-02 backlog) and sequences
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn the_ring_evicts_the_oldest_lines_first_while_segments_keep_everything() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.ring_lines = 5;
    let store = open(cfg);
    let svc = ServiceId::new();
    for i in 1..=8 {
        store.append(rec(svc, NOW + i, LogLevel::Info, format!("m{i}")));
    }

    let backlog = take(store.tail(svc, None), 5).await;
    assert_eq!(
        msgs(&backlog),
        ["m4", "m5", "m6", "m7", "m8"],
        "ring keeps the newest 5, in order"
    );
    assert_eq!(seqs(&backlog), [4, 5, 6, 7, 8]);

    let mut s = store.tail(svc, None);
    for _ in 0..5 {
        s.next().await.expect("backlog record");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), s.next())
            .await
            .is_err(),
        "m1..m3 were evicted from the ring"
    );

    store.flush().unwrap();
    let all = query_all(&store, svc);
    assert_eq!(
        msgs(&all),
        (1..=8).map(|i| format!("m{i}")).collect::<Vec<_>>(),
        "segments are not limited by the ring"
    );
}

#[tokio::test]
async fn eviction_order_holds_for_a_long_run_through_a_small_ring() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.ring_lines = 10;
    let store = open(cfg);
    let svc = ServiceId::new();
    for i in 1..=1_000 {
        store.append(rec(svc, NOW + i, LogLevel::Info, format!("m{i}")));
    }
    let backlog = take(store.tail(svc, None), 10).await;
    assert_eq!(seqs(&backlog), (991..=1_000).collect::<Vec<u64>>());
}

#[tokio::test]
async fn sequences_and_rings_are_per_service() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.ring_lines = 3;
    let store = open(cfg);
    let (a, b) = (ServiceId::new(), ServiceId::new());
    for i in 1..=5 {
        store.append(rec(a, NOW + i, LogLevel::Info, format!("a{i}")));
        store.append(rec(b, NOW + i, LogLevel::Info, format!("b{i}")));
    }
    let ta = take(store.tail(a, None), 3).await;
    let tb = take(store.tail(b, None), 3).await;
    assert_eq!(msgs(&ta), ["a3", "a4", "a5"]);
    assert_eq!(msgs(&tb), ["b3", "b4", "b5"]);
    assert_eq!(seqs(&ta), [3, 4, 5], "each service numbers its own records from 1");
    assert_eq!(seqs(&tb), [3, 4, 5]);
    assert!(ta.iter().all(|r| r.service_id == a) && tb.iter().all(|r| r.service_id == b));
}

#[tokio::test]
async fn obs_02_reconnecting_with_the_last_seen_cursor_delivers_every_record_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    for i in 1..=3 {
        store.append(rec(svc, NOW + i, LogLevel::Info, format!("req{i}")));
    }
    let first = take(store.tail(svc, None), 3).await;
    assert_eq!(msgs(&first), ["req1", "req2", "req3"]);
    let cursor = LogCursor(first.last().unwrap().seq);

    // client disconnects; two more requests are logged meanwhile
    store.append(rec(svc, NOW + 4, LogLevel::Info, "req4"));
    store.append(rec(svc, NOW + 5, LogLevel::Info, "req5"));

    let mut resumed = store.tail(svc, Some(cursor));
    let got = vec![resumed.next().await.unwrap(), resumed.next().await.unwrap()];
    assert_eq!(msgs(&got), ["req4", "req5"], "no duplicates, no gaps");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), resumed.next())
            .await
            .is_err(),
        "nothing else is pending"
    );
}

#[tokio::test]
async fn a_tail_continues_with_live_records_after_the_backlog() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    store.append(rec(svc, NOW, LogLevel::Info, "old"));
    let mut s = store.tail(svc, None);
    assert_eq!(s.next().await.unwrap().msg, "old");
    let writer = {
        let store = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            store.append(rec(svc, NOW + 1, LogLevel::Info, "live-1"));
            store.append(rec(svc, NOW + 2, LogLevel::Info, "live-2"));
        })
    };
    let live = take(s, 2).await;
    assert_eq!(
        msgs(&live),
        ["live-1", "live-2"],
        "delivered within the 1 s SLO of OBS-02"
    );
    writer.await.unwrap();
}

#[tokio::test]
async fn a_cursor_older_than_the_ring_is_served_from_the_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.ring_lines = 3;
    let store = open(cfg);
    let svc = ServiceId::new();
    for i in 1..=10 {
        store.append(rec(svc, NOW + i, LogLevel::Info, format!("m{i}")));
    }
    store.flush().unwrap();
    let got = take(store.tail(svc, Some(LogCursor(2))), 8).await;
    assert_eq!(
        seqs(&got),
        (3..=10).collect::<Vec<u64>>(),
        "records 3..=10 exactly once, in order"
    );
}

#[test]
fn concurrent_writers_get_unique_gap_free_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    std::thread::scope(|s| {
        for t in 0..8 {
            let store = store.clone();
            s.spawn(move || {
                for i in 0..500 {
                    store.append(rec(svc, NOW, LogLevel::Info, format!("t{t}-{i}")));
                }
            });
        }
    });
    store.flush().unwrap();
    let all = query_all(&store, svc);
    assert_eq!(all.len(), 4_000);
    assert_eq!(
        seqs(&all),
        (1..=4_000).collect::<Vec<u64>>(),
        "query returns records in sequence order without gaps"
    );
}

// ---------------------------------------------------------------------------------------------
// Rate limit (OBS-03)
// ---------------------------------------------------------------------------------------------

fn synthetic_drop_records(all: &[LogRecord]) -> Vec<&LogRecord> {
    all.iter().filter(|r| r.msg.ends_with(" lines dropped")).collect()
}

#[test]
fn obs_03_five_thousand_lines_in_a_burst_keep_about_a_thousand_and_summarize_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(LogStoreConfig::defaults(dir.path())); // 100 lines/s, burst 1,000
    let svc = ServiceId::new();
    assert_eq!(store.dropped_total(), 0);

    let start = Instant::now();
    for i in 0..5_000 {
        store.append(rec(svc, NOW + i, LogLevel::Info, format!("line-{i}")));
    }
    let elapsed = start.elapsed();
    let dropped = store.dropped_total();
    let kept = 5_000 - dropped;
    let max_kept = 1_000 + (elapsed.as_secs_f64() * 100.0).ceil() as u64 + 2;
    assert!(
        (1_000..=max_kept).contains(&kept),
        "kept {kept}, expected between the 1,000 burst and {max_kept} (burst + 100/s over {elapsed:?})"
    );

    // after the bucket refills, the next line is accepted and the drop summary is emitted
    std::thread::sleep(Duration::from_millis(1_300));
    store.append(rec(svc, NOW + 10_000, LogLevel::Info, "marker"));
    store.flush().unwrap();

    let all = query_all(&store, svc);
    let summaries = synthetic_drop_records(&all);
    assert_eq!(
        summaries.len(),
        1,
        "exactly one synthetic record: {:?}",
        summaries.iter().map(|r| &r.msg).collect::<Vec<_>>()
    );
    let n: u64 = summaries[0]
        .msg
        .trim_end_matches(" lines dropped")
        .parse()
        .expect("message has the form `N lines dropped`");
    assert_eq!(
        n,
        store.dropped_total(),
        "the record reports the same N as rt_logs_dropped_total"
    );
    assert_eq!(n, dropped, "the marker line was kept, so nothing else was dropped");
    assert_eq!(summaries[0].service_id, svc);

    let kept_lines = all.iter().filter(|r| r.msg.starts_with("line-")).count() as u64;
    assert_eq!(kept_lines, kept);
    assert_eq!(kept_lines + n, 5_000, "every line is either kept or counted as dropped");
    assert!(all.iter().any(|r| r.msg == "marker"));
    // the kept lines are the first ones: the limiter drops the overflow, not the beginning
    assert!(all.iter().any(|r| r.msg == "line-0") && all.iter().any(|r| r.msg == "line-999"));
    assert!(!all.iter().any(|r| r.msg == "line-4999"));
}

#[test]
fn the_rate_limit_is_per_service_and_configurable() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = LogStoreConfig::defaults(dir.path());
    cfg.rate_per_sec = 10;
    cfg.burst = 20;
    let store = open(cfg);
    let (noisy, quiet) = (ServiceId::new(), ServiceId::new());

    let start = Instant::now();
    for i in 0..200 {
        store.append(rec(noisy, NOW + i, LogLevel::Info, format!("n{i}")));
    }
    for i in 0..15 {
        store.append(rec(quiet, NOW + i, LogLevel::Info, format!("q{i}")));
    }
    let elapsed = start.elapsed();
    store.flush().unwrap();

    let quiet_kept = query_all(&store, quiet)
        .into_iter()
        .filter(|r| r.msg.starts_with('q'))
        .count();
    assert_eq!(quiet_kept, 15, "a quiet service is not affected by its noisy neighbour");

    let noisy_kept = query_all(&store, noisy)
        .into_iter()
        .filter(|r| r.msg.starts_with('n'))
        .count() as u64;
    let max_kept = 20 + (elapsed.as_secs_f64() * 10.0).ceil() as u64 + 1;
    assert!(
        (20..=max_kept).contains(&noisy_kept),
        "noisy kept {noisy_kept}, expected 20..={max_kept}"
    );
    assert_eq!(
        store.dropped_total(),
        200 - noisy_kept,
        "only the noisy service dropped lines"
    );
}

#[test]
fn staying_within_the_rate_limit_drops_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(LogStoreConfig::defaults(dir.path()));
    let svc = ServiceId::new();
    for i in 0..900 {
        store.append(rec(svc, NOW + i, LogLevel::Info, format!("l{i}")));
    }
    store.flush().unwrap();
    assert_eq!(store.dropped_total(), 0);
    let all = query_all(&store, svc);
    assert_eq!(all.len(), 900, "within the burst allowance every line is kept");
    assert!(synthetic_drop_records(&all).is_empty());
}

// ---------------------------------------------------------------------------------------------
// JSONL segments
// ---------------------------------------------------------------------------------------------

fn varied_records(svc: ServiceId) -> Vec<LogRecord> {
    let mut v = Vec::new();
    for (i, (level, kind, trigger)) in [
        (LogLevel::Debug, LogKind::Runtime, "http"),
        (LogLevel::Info, LogKind::Runtime, "interval"),
        (LogLevel::Warn, LogKind::System, "queue"),
        (LogLevel::Error, LogKind::Build, "build"),
        (LogLevel::Info, LogKind::Runtime, "call"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut r = rec(
            svc,
            NOW + (i as i64) * 1_000,
            level,
            format!("message {i} with \"quotes\", newline\\n, unicode é日🦀 and {{braces}}"),
        );
        r.kind = kind;
        r.trigger = trigger.into();
        r.tenant_id = Some(TenantId::from_u128(7));
        r.task_id = Some(TaskId(0x1000 + i as u64));
        r.version = VersionNo(3 + i as u32);
        v.push(r);
    }
    v
}

fn same_ignoring_seq(a: &LogRecord, b: &LogRecord) -> bool {
    let mut a = a.clone();
    a.seq = b.seq;
    &a == b
}

#[test]
fn segments_are_jsonl_files_per_service_and_round_trip_every_field() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    let expected = varied_records(svc);
    for r in &expected {
        store.append(r.clone());
    }
    store.flush().unwrap();

    // on disk: `<dir>/<svc>/<day>-<n>.seg`, one JSON object per line
    let files = seg_files(dir.path());
    assert!(
        !files.is_empty(),
        "flush must write at least one .seg file under {:?}",
        dir.path()
    );
    assert!(
        files.iter().all(|f| f.to_string_lossy().contains(&svc.bare())),
        "segments live in a per-service directory: {files:?}"
    );
    let mut from_disk = Vec::new();
    for f in &files {
        let text = std::fs::read_to_string(f).unwrap();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let v: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("{f:?}: line is not JSON ({e}): {line}"));
            assert!(v.is_object());
            if let Ok(r) = serde_json::from_value::<LogRecord>(v) {
                from_disk.push(r);
            }
        }
    }
    assert_eq!(from_disk.len(), expected.len(), "every record is on disk exactly once");

    // through the query API
    let queried = query_all(&store, svc);
    assert_eq!(queried.len(), expected.len());
    for ((got, want), disk) in queried.iter().zip(&expected).zip(&from_disk) {
        assert!(
            same_ignoring_seq(want, got),
            "query result differs:\n  want {want:?}\n  got  {got:?}"
        );
        assert_eq!(got, disk, "disk and query agree");
    }
    assert_eq!(seqs(&queried), (1..=5).collect::<Vec<u64>>());
}

#[test]
fn one_service_never_writes_into_anothers_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let (a, b) = (ServiceId::new(), ServiceId::new());
    store.append(rec(a, NOW, LogLevel::Info, "from a"));
    store.append(rec(b, NOW, LogLevel::Info, "from b"));
    store.flush().unwrap();
    assert_eq!(msgs(&query_all(&store, a)), ["from a"]);
    assert_eq!(msgs(&query_all(&store, b)), ["from b"]);
    for f in seg_files(dir.path()) {
        let text = std::fs::read_to_string(&f).unwrap();
        let is_a = f.to_string_lossy().contains(&a.bare());
        assert_eq!(text.contains("from a"), is_a, "{f:?}");
        assert_eq!(text.contains("from b"), !is_a, "{f:?}");
    }
}

#[test]
fn oversized_messages_are_truncated_by_the_store_and_flagged() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    let mut r = rec(svc, NOW, LogLevel::Info, "short");
    r.msg = "q".repeat(MAX_MSG_CHARS * 2); // bypasses LogRecord::new
    store.append(r);
    store.flush().unwrap();
    let got = query_all(&store, svc);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].msg.chars().count(), MAX_MSG_CHARS);
    assert!(got[0].truncated);
}

// ---------------------------------------------------------------------------------------------
// OBS-05: restart
// ---------------------------------------------------------------------------------------------

#[test]
fn obs_05_logs_survive_a_restart_and_sequences_keep_increasing() {
    let dir = tempfile::tempdir().unwrap();
    let svc = ServiceId::new();
    {
        let store = open(unlimited(dir.path()));
        for i in 0..5 {
            store.append(rec(svc, NOW + i * 1_000, LogLevel::Info, format!("before-{i}")));
        }
        store.flush().unwrap();
    } // store dropped: process "restarts"

    let store = open(unlimited(dir.path()));
    let mut q = LogQuery::for_service(svc);
    q.since_ms = Some(NOW + 500);
    q.until_ms = Some(NOW + 3_500);
    let ranged = store.query(q).records;
    assert_eq!(
        msgs(&ranged),
        ["before-1", "before-2", "before-3"],
        "queryable by time range after restart"
    );

    let before = query_all(&store, svc);
    assert_eq!(msgs(&before), (0..5).map(|i| format!("before-{i}")).collect::<Vec<_>>());
    let last_seq = before.last().unwrap().seq;

    store.append(rec(svc, NOW + 10_000, LogLevel::Info, "after"));
    store.flush().unwrap();
    let all = query_all(&store, svc);
    assert_eq!(all.len(), 6);
    assert!(
        all.last().unwrap().seq > last_seq,
        "sequence numbers must not restart (SSE Last-Event-ID stays valid)"
    );
    assert_eq!(all.last().unwrap().msg, "after");
}

// ---------------------------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------------------------

fn query_fixture() -> (tempfile::TempDir, Arc<FileLogStore>, ServiceId, TaskId, TaskId) {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    let (t1, t2) = (TaskId(1), TaskId(2));
    let rows: [(i64, LogLevel, LogKind, Option<TaskId>, &str); 5] = [
        (1_000, LogLevel::Debug, LogKind::Runtime, None, "r1"),
        (2_000, LogLevel::Info, LogKind::Runtime, Some(t1), "r2"),
        (3_000, LogLevel::Warn, LogKind::Build, Some(t1), "r3"),
        (4_000, LogLevel::Error, LogKind::System, Some(t2), "r4"),
        (5_000, LogLevel::Info, LogKind::Runtime, Some(t2), "r5"),
    ];
    for (ts, level, kind, task, msg) in rows {
        let mut r = rec(svc, NOW + ts, level, msg);
        r.kind = kind;
        r.task_id = task;
        store.append(r);
    }
    // noise from another service must never leak into results
    store.append(rec(ServiceId::new(), NOW + 2_500, LogLevel::Error, "other-service"));
    store.flush().unwrap();
    (dir, store, svc, t1, t2)
}

fn run(store: &FileLogStore, svc: ServiceId, f: impl FnOnce(&mut LogQuery)) -> Vec<String> {
    let mut q = LogQuery::for_service(svc);
    f(&mut q);
    msgs(&store.query(q).records)
}

#[test]
fn queries_filter_by_time_range_level_task_and_kind() {
    let (_dir, store, svc, t1, t2) = query_fixture();
    assert_eq!(
        run(&store, svc, |_| {}),
        ["r1", "r2", "r3", "r4", "r5"],
        "no filter: everything of this service, oldest first"
    );
    assert_eq!(
        run(&store, svc, |q| q.since_ms = Some(NOW + 1_500)),
        ["r2", "r3", "r4", "r5"]
    );
    assert_eq!(
        run(&store, svc, |q| q.until_ms = Some(NOW + 4_500)),
        ["r1", "r2", "r3", "r4"]
    );
    assert_eq!(
        run(&store, svc, |q| {
            q.since_ms = Some(NOW + 1_500);
            q.until_ms = Some(NOW + 4_500);
        }),
        ["r2", "r3", "r4"]
    );
    assert_eq!(
        run(&store, svc, |q| q.level = Some(LogLevel::Warn)),
        ["r3", "r4"],
        "level is a minimum"
    );
    assert_eq!(run(&store, svc, |q| q.level = Some(LogLevel::Error)), ["r4"]);
    assert_eq!(
        run(&store, svc, |q| q.level = Some(LogLevel::Debug)),
        ["r1", "r2", "r3", "r4", "r5"]
    );
    assert_eq!(run(&store, svc, |q| q.task_id = Some(t1)), ["r2", "r3"]);
    assert_eq!(run(&store, svc, |q| q.task_id = Some(t2)), ["r4", "r5"]);
    assert_eq!(run(&store, svc, |q| q.task_id = Some(TaskId(99))), Vec::<String>::new());
    assert_eq!(run(&store, svc, |q| q.kind = Some(LogKind::Build)), ["r3"]);
    assert_eq!(run(&store, svc, |q| q.kind = Some(LogKind::System)), ["r4"]);
    assert_eq!(
        run(&store, svc, |q| {
            q.kind = Some(LogKind::Runtime);
            q.level = Some(LogLevel::Info);
        }),
        ["r2", "r5"],
        "filters combine with AND"
    );
    assert_eq!(
        run(&store, ServiceId::new(), |_| {}),
        Vec::<String>::new(),
        "unknown service"
    );
}

#[test]
fn pagination_walks_every_record_once_with_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    for i in 0..10 {
        store.append(rec(svc, NOW + i, LogLevel::Info, format!("p{i}")));
    }
    store.flush().unwrap();

    let mut q = LogQuery::for_service(svc);
    q.limit = 3;
    let p1 = store.query(q.clone());
    assert_eq!(msgs(&p1.records), ["p0", "p1", "p2"]);
    let c1 = p1.next_cursor.expect("more records match");

    q.cursor = Some(c1);
    let p2 = store.query(q.clone());
    assert_eq!(
        msgs(&p2.records),
        ["p3", "p4", "p5"],
        "the cursor continues after the last record of the previous page"
    );

    q.cursor = Some(p2.next_cursor.unwrap());
    let p3 = store.query(q.clone());
    assert_eq!(msgs(&p3.records), ["p6", "p7", "p8"]);

    q.cursor = Some(p3.next_cursor.unwrap());
    let p4 = store.query(q.clone());
    assert_eq!(msgs(&p4.records), ["p9"]);
    assert!(p4.next_cursor.is_none(), "last page has no cursor");

    let mut big = LogQuery::for_service(svc);
    big.limit = 100;
    let all = store.query(big);
    assert_eq!(all.records.len(), 10);
    assert!(all.next_cursor.is_none());
}

#[test]
fn a_page_of_exactly_the_remaining_records_has_no_next_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    for i in 0..6 {
        store.append(rec(svc, NOW + i, LogLevel::Info, format!("e{i}")));
    }
    store.flush().unwrap();
    let mut q = LogQuery::for_service(svc);
    q.limit = 6;
    let page = store.query(q);
    assert_eq!(page.records.len(), 6);
    assert!(page.next_cursor.is_none(), "nothing matches beyond the last record");
}

// ---------------------------------------------------------------------------------------------
// Retention (OBS-04)
// ---------------------------------------------------------------------------------------------

#[test]
fn obs_04_segments_older_than_the_retention_are_deleted_oldest_first_newest_kept() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.retention_days = 7;
    let store = open(cfg);
    let svc = ServiceId::new();
    let days_ago = [10, 8, 1, 0];
    for d in days_ago {
        for i in 0..3 {
            store.append(rec(
                svc,
                NOW - d * DAY_MS + i * 1_000,
                LogLevel::Info,
                format!("d{d}-{i}"),
            ));
        }
    }
    store.flush().unwrap();
    let before = seg_files(dir.path()).len();
    assert!(
        before >= 4,
        "records of four different days must land in at least four segments, got {before}"
    );

    let report = store.sweep_retention(NOW);
    assert!(
        report.segments_deleted >= 2,
        "the 10- and 8-day-old segments must go: {report:?}"
    );
    assert!(report.bytes_freed > 0);
    assert_eq!(
        seg_files(dir.path()).len() as u32,
        before as u32 - report.segments_deleted
    );

    let left = msgs(&query_all(&store, svc));
    assert_eq!(
        left,
        ["d1-0", "d1-1", "d1-2", "d0-0", "d0-1", "d0-2"],
        "only the recent days remain, in order"
    );

    let again = store.sweep_retention(NOW);
    assert_eq!(
        (again.segments_deleted, again.bytes_freed),
        (0, 0),
        "a second sweep finds nothing to do"
    );
}

#[test]
fn obs_04_the_size_cap_deletes_the_oldest_segments_first() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.retention_days = 365;
    cfg.max_bytes_per_service = 4_000;
    let store = open(cfg);
    let svc = ServiceId::new();
    let mut all_msgs = Vec::new();
    for day in (0..5).rev() {
        for i in 0..10 {
            let m = format!("day{day}-line{i}-{}", "x".repeat(60));
            all_msgs.push(m.clone());
            store.append(rec(svc, NOW - day * DAY_MS + i * 1_000, LogLevel::Info, m));
        }
    }
    store.flush().unwrap();
    let total_before: u64 = seg_files(dir.path())
        .iter()
        .map(|f| std::fs::metadata(f).unwrap().len())
        .sum();
    assert!(
        total_before > 4_000,
        "fixture must exceed the cap ({total_before} bytes)"
    );

    let report = store.sweep_retention(NOW);
    assert!(report.segments_deleted >= 1 && report.bytes_freed > 0, "{report:?}");

    let remaining = msgs(&query_all(&store, svc));
    assert!(!remaining.is_empty(), "the newest segment is always kept");
    assert_eq!(remaining.last(), all_msgs.last(), "newest record survives");
    assert_eq!(
        remaining,
        all_msgs[all_msgs.len() - remaining.len()..],
        "what remains is a contiguous newest suffix: oldest segments went first"
    );
    let total_after: u64 = seg_files(dir.path())
        .iter()
        .map(|f| std::fs::metadata(f).unwrap().len())
        .sum();
    assert!(
        total_after <= 4_000 || seg_files(dir.path()).len() == 1,
        "{total_after} bytes remain over the cap"
    );
}

#[test]
fn the_sweep_only_touches_the_service_over_its_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = unlimited(dir.path());
    cfg.retention_days = 7;
    let store = open(cfg);
    let (old_svc, fresh_svc) = (ServiceId::new(), ServiceId::new());
    store.append(rec(old_svc, NOW - 30 * DAY_MS, LogLevel::Info, "ancient"));
    store.append(rec(fresh_svc, NOW, LogLevel::Info, "fresh"));
    store.flush().unwrap();
    store.sweep_retention(NOW);
    assert!(query_all(&store, old_svc).is_empty());
    assert_eq!(msgs(&query_all(&store, fresh_svc)), ["fresh"]);
}

/// OBS-005: guest messages carrying line breaks, ANSI escapes, NUL and replacement characters are
/// each one JSON object on one line; none of them splits, merges or corrupts a segment line.
#[test]
fn injected_control_characters_stay_inside_one_jsonl_record_each() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(unlimited(dir.path()));
    let svc = ServiceId::new();
    let hostile = [
        "line one\nline two",
        "carriage\rreturn",
        "\u{1b}[31mred\u{1b}[0m",
        "nul\u{0}byte",
        "data: fake event\n\ndata: {\"x\":1}",
        "replacement \u{FFFD} char",
    ];
    for (i, m) in hostile.iter().enumerate() {
        store.append(rec(svc, NOW + i as i64, LogLevel::Info, *m));
    }
    store.flush().unwrap();

    let mut on_disk = 0;
    for f in seg_files(dir.path()) {
        let text = std::fs::read_to_string(&f).unwrap();
        for line in text.split('\n').filter(|l| !l.is_empty()) {
            let v: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| panic!("{f:?}: {e}: {line:?}"));
            assert!(v.is_object());
            on_disk += 1;
        }
    }
    assert_eq!(on_disk, hostile.len(), "one record per line, none split or merged");
    let queried: Vec<String> = msgs(&query_all(&store, svc));
    assert_eq!(queried, hostile.iter().map(|m| m.to_string()).collect::<Vec<_>>());
}
