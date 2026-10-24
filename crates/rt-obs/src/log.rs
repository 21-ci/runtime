//! Guest and system logs.
//!
//! Pipeline: the executor calls [`LogSink::append`], which takes the service's mutex (one per
//! service, so services never contend with each other), applies the per-service rate limit,
//! assigns the sequence number and then (1) pushes to the per-service in-memory ring that serves
//! tail/SSE, (2) appends to a per-service buffered writer for the JSONL segments
//! `logs/<svc>/<day>-<n>.seg` (per-segment time index in memory), (3) optionally echoes to stdout.
//!
//! Segment writes are buffered: a background thread flushes every service's buffer every
//! [`LogStoreConfig::flush_interval`] (100 ms), and a buffer is also flushed before any read from
//! the segments (queries, a tail catching up from disk), when a tail attaches, on
//! [`FileLogStore::flush`] (graceful shutdown) and when the store is dropped. A crash loses at most
//! one flush interval of records; `fsync` after each flush is optional ([`LogStoreConfig::fsync`]).
//! Record and drop counts are kept per service under the same mutex and published to
//! `rt_logs_records_total{kind}` / `rt_logs_dropped_total` in batches by the same flush.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures_util::stream::{self, BoxStream, StreamExt};
use parking_lot::Mutex;
use rt_types::{ServiceId, TaskId, TenantId, UnixMs, VersionNo};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::watch;

use crate::{Metrics, ObsError};

/// Messages longer than this are truncated with `truncated: true`.
pub const MAX_MSG_CHARS: usize = 10_000;

/// Most top-level keys of a record's structured fields (`std:log`).
pub const MAX_LOG_FIELDS: usize = 32;

/// Most bytes of JSON text of a record's structured fields (`std:log`).
pub const MAX_LOG_FIELDS_BYTES: usize = 8 * 1024;

/// Why a structured field object was refused by [`parse_log_fields`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LogFieldsError {
    #[error("log fields must be a JSON object")]
    NotObject,
    #[error("log fields have {n} keys, at most {MAX_LOG_FIELDS}")]
    TooManyKeys { n: usize },
    #[error("log fields are {size} bytes, at most {MAX_LOG_FIELDS_BYTES}")]
    TooLarge { size: usize },
}

/// Parse and check the structured fields of one record: a JSON object of at most
/// [`MAX_LOG_FIELDS`] top-level keys and [`MAX_LOG_FIELDS_BYTES`] bytes of JSON text.
pub fn parse_log_fields(json: &str) -> Result<Map<String, Value>, LogFieldsError> {
    if json.len() > MAX_LOG_FIELDS_BYTES {
        return Err(LogFieldsError::TooLarge { size: json.len() });
    }
    let map = match serde_json::from_str::<Value>(json) {
        Ok(Value::Object(map)) => map,
        _ => return Err(LogFieldsError::NotObject),
    };
    if map.len() > MAX_LOG_FIELDS {
        return Err(LogFieldsError::TooManyKeys { n: map.len() });
    }
    Ok(map)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// Origin of a record: guest code, a build, or the runtime talking about this service.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogKind {
    Runtime,
    Build,
    System,
}

impl LogKind {
    const ALL: [LogKind; 3] = [LogKind::Runtime, LogKind::Build, LogKind::System];

    /// The `kind` label of `rt_logs_records_total` (equals the serde name).
    pub fn as_str(self) -> &'static str {
        match self {
            LogKind::Runtime => "runtime",
            LogKind::Build => "build",
            LogKind::System => "system",
        }
    }

    fn index(self) -> usize {
        match self {
            LogKind::Runtime => 0,
            LogKind::Build => 1,
            LogKind::System => 2,
        }
    }
}

/// One log record (also the SSE `data:` payload).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    /// Per-service monotonically increasing sequence, assigned by the store (0 before append).
    /// Used as SSE `id:` / `Last-Event-ID`.
    #[serde(default)]
    pub seq: u64,
    pub ts_ms: UnixMs,
    pub level: LogLevel,
    pub tenant_id: Option<TenantId>,
    pub service_id: ServiceId,
    pub version: VersionNo,
    /// Equals the `x-rt-task-id` response header.
    pub task_id: Option<TaskId>,
    /// `http|interval|cron|queue|call|build`.
    pub trigger: String,
    pub kind: LogKind,
    pub msg: String,
    #[serde(default)]
    pub truncated: bool,
    /// Structured fields of a `std:log` record (checked by [`parse_log_fields`]); absent on plain
    /// records, which are stored and served exactly as before fields existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Map<String, Value>>,
}

impl LogRecord {
    pub fn new(
        service_id: ServiceId,
        version: VersionNo,
        level: LogLevel,
        kind: LogKind,
        msg: impl Into<String>,
    ) -> Self {
        let mut r = Self {
            seq: 0,
            ts_ms: 0,
            level,
            tenant_id: None,
            service_id,
            version,
            task_id: None,
            trigger: String::new(),
            kind,
            msg: msg.into(),
            truncated: false,
            fields: None,
        };
        r.truncate_msg();
        r
    }

    /// Enforce [`MAX_MSG_CHARS`] (on a char boundary).
    pub fn truncate_msg(&mut self) {
        if self.msg.chars().count() > MAX_MSG_CHARS {
            self.msg = self.msg.chars().take(MAX_MSG_CHARS).collect();
            self.truncated = true;
        }
    }
}

/// Non-blocking record sink. Implementations may drop under pressure (counted).
pub trait LogSink: Send + Sync {
    fn append(&self, rec: LogRecord);
}

/// Position in a service's log (the `seq` of the last record seen).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogCursor(pub u64);

/// Time-range query over persisted logs. No full-text search in core.
#[derive(Clone, Debug)]
pub struct LogQuery {
    pub service: ServiceId,
    pub since_ms: Option<UnixMs>,
    pub until_ms: Option<UnixMs>,
    /// Minimum level.
    pub level: Option<LogLevel>,
    pub task_id: Option<TaskId>,
    pub kind: Option<LogKind>,
    pub limit: usize,
    /// Continue after this record.
    pub cursor: Option<LogCursor>,
}

impl LogQuery {
    pub fn for_service(service: ServiceId) -> Self {
        Self {
            service,
            since_ms: None,
            until_ms: None,
            level: None,
            task_id: None,
            kind: None,
            limit: 100,
            cursor: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogPage {
    pub records: Vec<LogRecord>,
    /// `Some` when more records match.
    pub next_cursor: Option<LogCursor>,
}

/// Backlog (from the in-memory ring or segments) followed by live records.
pub type LogStream = BoxStream<'static, LogRecord>;

/// Result of a retention sweep.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub segments_deleted: u32,
    pub bytes_freed: u64,
}

pub trait LogStore: LogSink {
    /// Backlog after `from` (or the ring tail when `None`), then live records. Reconnecting with
    /// the last seen cursor delivers every record exactly once (OBS-02).
    fn tail(&self, service: ServiceId, from: Option<LogCursor>) -> LogStream;

    fn query(&self, q: LogQuery) -> LogPage;

    /// Delete segments older than `retention_days` or beyond `max_mb_per_service`, oldest first
    /// (OBS-04). Called periodically by sysmon / an I/O timer.
    fn sweep_retention(&self, now_ms: UnixMs) -> SweepReport;

    /// Records dropped by the per-service rate limit (`rt_logs_dropped_total`).
    fn dropped_total(&self) -> u64;
}

/// Configuration of [`FileLogStore`] (`[logs]` + rate limit).
#[derive(Clone, Debug)]
pub struct LogStoreConfig {
    /// `$RT_DATA/logs`.
    pub dir: PathBuf,
    pub ring_lines: usize,
    pub retention_days: u32,
    pub max_bytes_per_service: u64,
    /// Sustained lines per second per service (default 100).
    pub rate_per_sec: u32,
    /// Burst size (default 1,000).
    pub burst: u32,
    /// Echo records to stdout (`logs.stdout`).
    pub stdout: bool,
    /// How often buffered segment appends are written out (default [`LOG_FLUSH_INTERVAL`]).
    pub flush_interval: Duration,
    /// `fsync` each segment after a flush (`logs.fsync`, default off): survives a power loss,
    /// not only a process crash, at the cost of one `fsync` per written service per flush.
    pub fsync: bool,
}

/// Default [`LogStoreConfig::flush_interval`].
pub const LOG_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

impl LogStoreConfig {
    /// Defaults (docs/handlers.md, logs) with `dir` as the log root.
    pub fn defaults(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ring_lines: 1000,
            retention_days: 7,
            max_bytes_per_service: 50 * 1024 * 1024,
            rate_per_sec: 100,
            burst: 1000,
            stdout: false,
            flush_interval: LOG_FLUSH_INTERVAL,
            fsync: false,
        }
    }
}

const DAY_MS: i64 = 86_400_000;
/// Records fetched per step by a tail stream catching up from the segments.
const TAIL_BATCH: usize = 256;
/// A drop summary is held back until the limiter has been quiet this long, so one burst yields one
/// `"N lines dropped"` record (OBS-03) instead of one per refilled token.
const DROP_QUIET: Duration = Duration::from_secs(1);
/// Under sustained overload the quiet period never comes; summarize at least this often.
const DROP_MAX_HOLD: Duration = Duration::from_secs(10);
/// Write buffer of one service's active segment.
const WRITE_BUFFER: usize = 8 * 1024;
/// A service's segment file is closed (buffer and descriptor released) after this long without
/// appends; the next append reopens it.
const WRITER_IDLE_CLOSE: Duration = Duration::from_secs(60);

fn now_ms() -> UnixMs {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// In-memory index entry of one segment file `<day>-<n>.seg`.
#[derive(Clone, Debug)]
struct SegMeta {
    path: PathBuf,
    day: i64,
    n: u32,
    min_ts: i64,
    max_ts: i64,
    first_seq: u64,
    last_seq: u64,
    bytes: u64,
    count: u64,
}

impl SegMeta {
    fn empty(path: PathBuf, day: i64, n: u32) -> Self {
        Self {
            path,
            day,
            n,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            first_seq: 0,
            last_seq: 0,
            bytes: 0,
            count: 0,
        }
    }

    fn note(&mut self, rec: &LogRecord, line_bytes: u64) {
        self.min_ts = self.min_ts.min(rec.ts_ms);
        self.max_ts = self.max_ts.max(rec.ts_ms);
        if self.count == 0 {
            self.first_seq = rec.seq;
        }
        self.last_seq = rec.seq;
        self.count += 1;
        self.bytes += line_bytes;
    }
}

fn parse_seg_name(path: &Path) -> Option<(i64, u32)> {
    let stem = path.file_name()?.to_str()?.strip_suffix(".seg")?;
    let (day, n) = stem.rsplit_once('-')?;
    // the day may be negative for pre-1970 timestamps: `-5-0` splits at the last dash
    Some((day.parse().ok()?, n.parse().ok()?))
}

/// Read every parsable record of a segment (a torn final line is ignored).
fn read_segment(path: &Path) -> Vec<LogRecord> {
    let Ok(f) = File::open(path) else { return Vec::new() };
    BufReader::new(f)
        .lines()
        .map_while(Result::ok)
        .filter_map(|l| serde_json::from_str::<LogRecord>(&l).ok())
        .collect()
}

/// Records written per [`LogKind`] and lines dropped by the rate limit, for one service.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts {
    records: [u64; 3],
    dropped: u64,
}

/// All state of one service's log.
struct Svc {
    dir: PathBuf,
    segs: Vec<SegMeta>,
    next_seq: u64,
    ring: VecDeque<LogRecord>,
    ring_loaded: bool,
    /// Buffered appends to the newest segment.
    writer: Option<BufWriter<File>>,
    /// The writer holds bytes not yet handed to the OS.
    dirty: bool,
    /// Last append written to `writer`.
    last_write: Instant,
    last_flush: Instant,
    tokens: f64,
    refilled: Instant,
    dropped_pending: u64,
    /// When the oldest pending drop happened / the newest one did.
    first_drop: Instant,
    last_drop: Instant,
    /// Lifetime counts, and the part of them already added to the metrics.
    counts: Counts,
    published: Counts,
    live: watch::Sender<u64>,
}

impl Svc {
    /// Scan the service directory (segments only; the ring is hydrated on demand).
    fn load(cfg: &LogStoreConfig, id: ServiceId) -> Svc {
        let dir = cfg.dir.join(id.to_string());
        let mut segs = Vec::new();
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for e in rd.flatten() {
                let path = e.path();
                let Some((day, n)) = parse_seg_name(&path) else {
                    continue;
                };
                let mut meta = SegMeta::empty(path.clone(), day, n);
                for rec in read_segment(&path) {
                    meta.note(&rec, 0);
                }
                meta.bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                segs.push(meta);
            }
        }
        segs.sort_by_key(|s| (s.day, s.n));
        let next_seq = segs.iter().map(|s| s.last_seq).max().unwrap_or(0) + 1;
        let now = Instant::now();
        Svc {
            dir,
            segs,
            next_seq,
            ring: VecDeque::new(),
            ring_loaded: false,
            writer: None,
            dirty: false,
            last_write: now,
            last_flush: now,
            tokens: f64::from(cfg.burst),
            refilled: now,
            dropped_pending: 0,
            first_drop: now,
            last_drop: now,
            counts: Counts::default(),
            published: Counts::default(),
            live: watch::channel(next_seq - 1).0,
        }
    }

    /// Hand buffered appends to the OS (and `fsync` them when `sync`). On a write error the
    /// writer is dropped; the next append reopens the segment on a fresh line.
    fn flush_writer(&mut self, sync: bool) {
        self.last_flush = Instant::now();
        if !self.dirty {
            return;
        }
        self.dirty = false;
        let Some(w) = self.writer.as_mut() else { return };
        let res = w
            .flush()
            .and_then(|()| if sync { w.get_ref().sync_data() } else { Ok(()) });
        if let Err(e) = res {
            tracing::warn!(target: "rt::logs", error = %e, "log segment flush failed");
            self.writer = None;
        }
    }

    /// Close the segment file of a service that has been quiet for [`WRITER_IDLE_CLOSE`].
    fn close_if_idle(&mut self, now: Instant) {
        if self.writer.is_some() && !self.dirty && now.saturating_duration_since(self.last_write) >= WRITER_IDLE_CLOSE {
            self.writer = None;
        }
    }

    /// Add the counts not yet published to the metrics.
    fn publish(&mut self, metrics: &Metrics) {
        if self.counts == self.published {
            return;
        }
        for kind in LogKind::ALL {
            let i = kind.index();
            let delta = self.counts.records[i] - self.published.records[i];
            if delta > 0 {
                metrics
                    .logs_records_total
                    .with_label_values(&[kind.as_str()])
                    .inc_by(delta);
            }
        }
        let dropped = self.counts.dropped - self.published.dropped;
        if dropped > 0 {
            metrics.logs_dropped_total.inc_by(dropped);
        }
        self.published = self.counts;
    }

    /// Fill the ring with the newest persisted records (after a restart).
    fn ensure_ring(&mut self, ring_lines: usize) {
        if self.ring_loaded {
            return;
        }
        self.ring_loaded = true;
        let mut recent: VecDeque<LogRecord> = VecDeque::new();
        for seg in self.segs.iter().rev() {
            let mut recs = read_segment(&seg.path);
            recs.extend(recent.drain(..));
            recent = recs.into();
            if recent.len() >= ring_lines {
                break;
            }
        }
        while recent.len() > ring_lines {
            recent.pop_front();
        }
        self.ring = recent;
    }

    fn take_token(&mut self, cfg: &LogStoreConfig, now: Instant) -> bool {
        let rate = f64::from(cfg.rate_per_sec);
        let burst = f64::from(cfg.burst);
        self.tokens = (self.tokens + now.saturating_duration_since(self.refilled).as_secs_f64() * rate).min(burst);
        self.refilled = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn note_drop(&mut self, now: Instant) {
        if self.dropped_pending == 0 {
            self.first_drop = now;
        }
        self.last_drop = now;
        self.dropped_pending += 1;
        self.counts.dropped += 1;
    }

    /// Whether the pending drops should be summarized now: the burst is over (quiet for
    /// [`DROP_QUIET`]) or has been going on for [`DROP_MAX_HOLD`].
    fn summary_due(&self) -> bool {
        self.dropped_pending > 0 && {
            let now = Instant::now();
            now.saturating_duration_since(self.last_drop) >= DROP_QUIET
                || now.saturating_duration_since(self.first_drop) >= DROP_MAX_HOLD
        }
    }

    /// Segment to append to: the newest one, or a fresh one when the day advanced or it is full.
    fn active_segment(&mut self, cfg: &LogStoreConfig, ts_ms: i64, line_len: u64) -> std::io::Result<usize> {
        let day = ts_ms.div_euclid(DAY_MS);
        let max_seg = (cfg.max_bytes_per_service / 8).clamp(64 * 1024, 4 * 1024 * 1024);
        let need_new = match self.segs.last() {
            None => Some((day, 0)),
            Some(last) if day > last.day => Some((day, 0)),
            Some(last) if last.bytes > 0 && last.bytes + line_len > max_seg => Some((last.day, last.n + 1)),
            Some(_) => None,
        };
        if let Some((day, n)) = need_new {
            std::fs::create_dir_all(&self.dir)?;
            let path = self.dir.join(format!("{day}-{n}.seg"));
            self.segs.push(SegMeta::empty(path, day, n));
            // the previous segment is complete: write its buffer out before switching
            self.flush_writer(false);
            self.writer = None;
        }
        if self.writer.is_none() {
            let path = &self.segs.last().expect("a segment exists").path;
            std::fs::create_dir_all(&self.dir)?;
            let mut f = OpenOptions::new().create(true).append(true).read(true).open(path)?;
            // a crash can leave a torn last line: start on a fresh line
            let len = f.metadata()?.len();
            if len > 0 {
                let mut last = [0u8; 1];
                f.seek(SeekFrom::Start(len - 1))?;
                f.read_exact(&mut last)?;
                if last[0] != b'\n' {
                    f.write_all(b"\n")?;
                }
            }
            self.writer = Some(BufWriter::with_capacity(WRITE_BUFFER, f));
        }
        Ok(self.segs.len() - 1)
    }

    /// Assign a sequence number, persist (buffered), push to the ring and wake tails.
    fn write(&mut self, cfg: &LogStoreConfig, mut rec: LogRecord) {
        rec.seq = self.next_seq;
        self.next_seq += 1;
        self.counts.records[rec.kind.index()] += 1;
        match serde_json::to_string(&rec) {
            Ok(mut line) => {
                line.push('\n');
                let len = line.len() as u64;
                match self.active_segment(cfg, rec.ts_ms, len) {
                    Ok(idx) => {
                        let res = self.writer.as_mut().map(|w| w.write_all(line.as_bytes()));
                        if let Some(Err(e)) = res {
                            tracing::warn!(target: "rt::logs", error = %e, "log segment write failed");
                            self.writer = None;
                        } else {
                            self.segs[idx].note(&rec, len);
                            self.dirty = true;
                            self.last_write = Instant::now();
                        }
                    }
                    Err(e) => tracing::warn!(target: "rt::logs", error = %e, "cannot open log segment"),
                }
                if cfg.stdout {
                    let _ = std::io::stdout().lock().write_all(line.as_bytes());
                }
            }
            Err(e) => tracing::warn!(target: "rt::logs", error = %e, "cannot serialize log record"),
        }
        self.ring.push_back(rec);
        while self.ring.len() > cfg.ring_lines.max(1) {
            self.ring.pop_front();
        }
        self.live.send_replace(self.next_seq - 1);
    }

    /// Emit the synthetic `"N lines dropped"` record if lines were dropped.
    fn flush_dropped(&mut self, cfg: &LogStoreConfig, like: &LogRecord) {
        if self.dropped_pending == 0 {
            return;
        }
        let n = std::mem::take(&mut self.dropped_pending);
        let mut synth = LogRecord::new(
            like.service_id,
            like.version,
            LogLevel::Warn,
            LogKind::System,
            format!("{n} lines dropped"),
        );
        synth.ts_ms = like.ts_ms;
        synth.tenant_id = like.tenant_id;
        synth.trigger = like.trigger.clone();
        self.write(cfg, synth);
    }

    /// Records matching `q` after `q.cursor`, oldest first, at most `limit + 1` of them.
    fn scan(&self, q: &LogQuery, limit: usize) -> Vec<LogRecord> {
        let after = q.cursor.map_or(0, |c| c.0);
        let mut out = Vec::new();
        for seg in &self.segs {
            if seg.count == 0 || seg.last_seq <= after {
                continue;
            }
            if q.since_ms.is_some_and(|s| seg.max_ts < s) || q.until_ms.is_some_and(|u| seg.min_ts > u) {
                continue;
            }
            for rec in read_segment(&seg.path) {
                if rec.seq <= after
                    || q.since_ms.is_some_and(|s| rec.ts_ms < s)
                    || q.until_ms.is_some_and(|u| rec.ts_ms > u)
                    || q.level.is_some_and(|l| rec.level < l)
                    || q.task_id.is_some_and(|t| rec.task_id != Some(t))
                    || q.kind.is_some_and(|k| rec.kind != k)
                {
                    continue;
                }
                out.push(rec);
                if out.len() > limit {
                    return out;
                }
            }
        }
        out
    }

    /// Up to `max` records with `seq > last`, from the ring when it covers them, else from disk.
    fn read_after(&mut self, service: ServiceId, last: u64, max: usize) -> Vec<LogRecord> {
        let covered = match self.ring.front() {
            Some(front) => front.seq <= last + 1,
            None => self.next_seq - 1 <= last,
        };
        if covered {
            return self.ring.iter().filter(|r| r.seq > last).take(max).cloned().collect();
        }
        self.flush_writer(false);
        let mut q = LogQuery::for_service(service);
        q.cursor = Some(LogCursor(last));
        let mut recs = self.scan(&q, max);
        recs.truncate(max);
        recs
    }
}

struct Inner {
    cfg: LogStoreConfig,
    services: DashMap<ServiceId, Arc<Mutex<Svc>>>,
    metrics: OnceLock<Arc<Metrics>>,
}

impl Inner {
    /// One flush pass over every service: emit due drop summaries (all pending ones when
    /// `all_summaries`), write buffers out, publish counts, close idle files.
    fn flush_all(&self, all_summaries: bool) {
        let cfg = &self.cfg;
        let entries: Vec<_> = self.services.iter().map(|e| e.value().clone()).collect();
        let metrics = self.metrics.get();
        let now = Instant::now();
        for svc in entries {
            let mut s = svc.lock();
            if s.dropped_pending > 0 && (all_summaries || s.summary_due()) {
                s.ensure_ring(cfg.ring_lines);
                if let Some(like) = s.ring.back().cloned() {
                    s.flush_dropped(cfg, &like);
                }
            }
            s.flush_writer(cfg.fsync);
            s.close_if_idle(now);
            if let Some(m) = metrics {
                s.publish(m);
            }
        }
    }

    /// Background flusher: runs until the store is dropped.
    fn spawn_flusher(inner: &Arc<Inner>) {
        let weak: Weak<Inner> = Arc::downgrade(inner);
        let every = inner.cfg.flush_interval.max(Duration::from_millis(1));
        let spawned = std::thread::Builder::new()
            .name("rt-logs-flush".into())
            .spawn(move || loop {
                std::thread::sleep(every);
                let Some(inner) = weak.upgrade() else { return };
                inner.flush_all(false);
            });
        if let Err(e) = spawned {
            // appends still flush themselves once the interval has passed
            tracing::warn!(target: "rt::logs", error = %e, "cannot start the log flusher thread");
        }
    }

    fn svc(&self, id: ServiceId) -> Arc<Mutex<Svc>> {
        if let Some(s) = self.services.get(&id) {
            return s.clone();
        }
        let loaded = Arc::new(Mutex::new(Svc::load(&self.cfg, id)));
        self.services.entry(id).or_insert(loaded).clone()
    }

    /// Services with a directory on disk plus every one seen in this process.
    fn all_services(&self) -> Vec<ServiceId> {
        let mut ids: Vec<ServiceId> = self.services.iter().map(|e| *e.key()).collect();
        if let Ok(rd) = std::fs::read_dir(&self.cfg.dir) {
            for e in rd.flatten() {
                if let Some(id) = e.file_name().to_str().and_then(|n| n.parse::<ServiceId>().ok()) {
                    if !ids.contains(&id) {
                        ids.push(id);
                    }
                }
            }
        }
        ids
    }
}

/// Ring + JSONL segment store with per-service rate limiting.
///
/// Layout: `<dir>/<svc_id>/<day>-<n>.seg`, one JSON record per line. Each segment has an
/// in-memory time/sequence index (rebuilt from the files when the service is first touched after a
/// restart), so queries skip segments that cannot match.
pub struct FileLogStore {
    inner: Arc<Inner>,
}

impl FileLogStore {
    /// Open (creating directories and scanning existing segments so logs survive restart, OBS-05).
    pub fn open(cfg: LogStoreConfig) -> Result<Arc<Self>, ObsError> {
        std::fs::create_dir_all(&cfg.dir)?;
        let inner = Arc::new(Inner {
            cfg,
            services: DashMap::new(),
            metrics: OnceLock::new(),
        });
        Inner::spawn_flusher(&inner);
        Ok(Arc::new(Self { inner }))
    }

    /// Publish record and drop counts to `rt_logs_records_total{kind}` / `rt_logs_dropped_total`
    /// on every flush. Only the first call takes effect.
    pub fn attach_metrics(&self, metrics: Arc<Metrics>) {
        let _ = self.inner.metrics.set(metrics);
    }

    /// Write every buffered record to its segment, emit pending `"N lines dropped"` summaries and
    /// publish the counts (graceful shutdown, tests). `fsync`s when [`LogStoreConfig::fsync`].
    pub fn flush(&self) -> Result<(), ObsError> {
        self.inner.flush_all(true);
        Ok(())
    }
}

impl Drop for FileLogStore {
    fn drop(&mut self) {
        // the buffers would be written by `BufWriter`'s own drop too; this also logs failures
        // and publishes the last counts
        self.inner.flush_all(false);
    }
}

impl LogSink for FileLogStore {
    fn append(&self, mut rec: LogRecord) {
        let cfg = &self.inner.cfg;
        rec.truncate_msg();
        if rec.ts_ms == 0 {
            rec.ts_ms = now_ms();
        }
        let svc = self.inner.svc(rec.service_id);
        let mut s = svc.lock();
        s.ensure_ring(cfg.ring_lines);
        let now = Instant::now();
        if !s.take_token(cfg, now) {
            s.note_drop(now);
            return;
        }
        if s.summary_due() {
            s.flush_dropped(cfg, &rec);
        }
        s.write(cfg, rec);
        // normally the flusher thread does this; bounds the delay if it is behind or missing
        if now.saturating_duration_since(s.last_flush) >= cfg.flush_interval.saturating_mul(2) {
            s.flush_writer(cfg.fsync);
        }
    }
}

struct TailState {
    svc: Arc<Mutex<Svc>>,
    service: ServiceId,
    last: u64,
    backlog: VecDeque<LogRecord>,
    rx: watch::Receiver<u64>,
}

impl LogStore for FileLogStore {
    fn tail(&self, service: ServiceId, from: Option<LogCursor>) -> LogStream {
        let svc = self.inner.svc(service);
        let (last, rx) = {
            let mut s = svc.lock();
            s.ensure_ring(self.inner.cfg.ring_lines);
            let last = match from {
                Some(c) => c.0,
                None => s.ring.front().map_or(s.next_seq - 1, |r| r.seq - 1),
            };
            // a tail attaching makes the on-disk view of this service current
            s.flush_writer(false);
            (last, s.live.subscribe())
        };
        let state = TailState {
            svc,
            service,
            last,
            backlog: VecDeque::new(),
            rx,
        };
        stream::unfold(state, |mut st| async move {
            loop {
                if let Some(rec) = st.backlog.pop_front() {
                    st.last = rec.seq;
                    return Some((rec, st));
                }
                // mark the current version seen *before* reading, so no append can be missed
                st.rx.borrow_and_update();
                let batch = st.svc.lock().read_after(st.service, st.last, TAIL_BATCH);
                if batch.is_empty() {
                    if st.rx.changed().await.is_err() {
                        return None;
                    }
                } else {
                    st.backlog = batch.into();
                }
            }
        })
        .boxed()
    }

    fn query(&self, q: LogQuery) -> LogPage {
        let known =
            self.inner.services.contains_key(&q.service) || self.inner.cfg.dir.join(q.service.to_string()).is_dir();
        if !known {
            return LogPage {
                records: Vec::new(),
                next_cursor: None,
            };
        }
        let limit = q.limit.max(1);
        let svc = self.inner.svc(q.service);
        let mut records = {
            let mut s = svc.lock();
            s.flush_writer(false);
            s.scan(&q, limit)
        };
        let next_cursor = if records.len() > limit {
            records.truncate(limit);
            records.last().map(|r| LogCursor(r.seq))
        } else {
            None
        };
        LogPage { records, next_cursor }
    }

    fn sweep_retention(&self, now_ms: UnixMs) -> SweepReport {
        let cfg = &self.inner.cfg;
        let cutoff = now_ms.saturating_sub(i64::from(cfg.retention_days) * DAY_MS);
        let mut report = SweepReport::default();
        for id in self.inner.all_services() {
            let svc = self.inner.svc(id);
            let mut s = svc.lock();
            let mut doomed: Vec<usize> = s
                .segs
                .iter()
                .enumerate()
                .filter(|(_, g)| g.count > 0 && g.max_ts < cutoff)
                .map(|(i, _)| i)
                .collect();
            // size cap: oldest first, the newest segment always stays
            let mut total: u64 = s
                .segs
                .iter()
                .enumerate()
                .filter(|(i, _)| !doomed.contains(i))
                .map(|(_, g)| g.bytes)
                .sum();
            let mut kept = s.segs.len() - doomed.len();
            for i in 0..s.segs.len() {
                if total <= cfg.max_bytes_per_service || kept <= 1 {
                    break;
                }
                if !doomed.contains(&i) {
                    doomed.push(i);
                    total -= s.segs[i].bytes;
                    kept -= 1;
                }
            }
            if doomed.is_empty() {
                continue;
            }
            let newest = s.segs.len() - 1;
            for &i in &doomed {
                let seg = &s.segs[i];
                if std::fs::remove_file(&seg.path).is_ok() {
                    report.segments_deleted += 1;
                    report.bytes_freed += seg.bytes;
                }
            }
            if doomed.contains(&newest) {
                s.writer = None;
                s.dirty = false;
            }
            let mut idx = 0;
            s.segs.retain(|_| {
                let keep = !doomed.contains(&idx);
                idx += 1;
                keep
            });
            if s.segs.is_empty() {
                let _ = std::fs::remove_dir(&s.dir); // only succeeds when empty
            }
        }
        report
    }

    fn dropped_total(&self) -> u64 {
        let entries: Vec<_> = self.inner.services.iter().map(|e| e.value().clone()).collect();
        entries.iter().map(|s| s.lock().counts.dropped).sum()
    }
}

/// Sink that discards everything.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullLogSink;

impl LogSink for NullLogSink {
    fn append(&self, _rec: LogRecord) {}
}

/// In-memory sink for tests: keeps every record in order.
#[derive(Debug, Default)]
pub struct MemoryLogSink {
    records: Mutex<Vec<LogRecord>>,
}

impl MemoryLogSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Everything appended so far.
    pub fn records(&self) -> Vec<LogRecord> {
        self.records.lock().clone()
    }

    pub fn clear(&self) {
        self.records.lock().clear();
    }
}

impl LogSink for MemoryLogSink {
    fn append(&self, mut rec: LogRecord) {
        let mut g = self.records.lock();
        rec.seq = g.len() as u64 + 1;
        g.push(rec);
    }
}
