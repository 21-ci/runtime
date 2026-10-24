//! Stream consumer engine (`events.consume[]`): one in-flight batch per cursor, commits from the
//! outcome, `x-rt-stream-fail-at`, bisect-to-one retries with backoff, and `dlq` / `skip` /
//! `block` once a record used up its attempts (plan-queues 2.2, plan-final 2.5).
//!
//! - Delivery: up to `batch_size` contiguous records (<= 1 MiB of bodies, always >= 1) from the
//!   committed offset; after a failed attempt the next delivery holds only the first uncommitted
//!   record, after [`stream_backoff`].
//! - Settle: 2xx commits through the last record (or up to a valid fail-at offset, counting one
//!   failed attempt for that record); anything else is one failed attempt for the first record.
//!   An admission refusal, a shutdown kill or a service that cannot take the task is not an
//!   attempt ([`counts_as_attempt`]): the cursor is deferred by [`throttle_delay`].
//! - Every store update is fenced by the cursor's `gen`, so a settle that outlived a `reset` is
//!   ignored.
//! - Retention runs every `trim_every`; cursors the trim moved up are reloaded and counted as
//!   `rt_stream_skipped_total{reason="out_of_range"}`.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use rt_sched::Clock;
use rt_store::stream::{Cursor, CursorKey, PartitionHead, ReadFrom, StreamStore, Topic};
use rt_types::events::{
    Namespace, OnExhausted, StartPosition, StreamBatch, StreamRecord, DELIVERY_BATCH_MAX_BYTES, READ_LIMIT_MAX,
    STREAM_FAIL_AT_HEADER, STREAM_INFLIGHT_PER_SERVICE,
};
use rt_types::{KillReason, Outcome, ServiceId, TaskSpec, TopicId};
use tokio::sync::Notify;

use crate::queue_engine::{counts_as_attempt, throttle_delay};
use crate::Submitter;

/// Base of the retry backoff after a failed attempt.
pub const STREAM_RETRY_BASE: Duration = Duration::from_secs(1);
/// Cap of the retry backoff.
pub const STREAM_RETRY_MAX: Duration = Duration::from_secs(300);

/// Backoff after `attempt` (1-based) failed deliveries of a record: `1 s × 2^(attempt-1)` scaled
/// by `1 + jitter` (`jitter` clamped to ±20 %), capped at 5 min.
pub fn stream_backoff(attempt: u16, jitter: f64) -> Duration {
    let exp = u32::from(attempt.saturating_sub(1)).min(20);
    let base = STREAM_RETRY_BASE.as_secs_f64() * f64::from(1u32 << exp);
    Duration::from_secs_f64((base * (1.0 + jitter.clamp(-0.2, 0.2))).max(0.0)).min(STREAM_RETRY_MAX)
}

/// The `x-rt-stream-fail-at` offset of a 2xx reply to a batch of offsets `first..=last`.
/// `Ok(None)` = no header (commit everything); `Err(())` = malformed or not one of the delivered
/// offsets (the whole batch failed).
#[allow(clippy::result_unit_err)]
pub fn parse_fail_at(headers: &http::HeaderMap, first: u64, last: u64) -> Result<Option<u64>, ()> {
    let Some(v) = headers.get(STREAM_FAIL_AT_HEADER) else {
        return Ok(None);
    };
    let off = v.to_str().map_err(|_| ())?.trim().parse::<u64>().map_err(|_| ())?;
    if (first..=last).contains(&off) {
        Ok(Some(off))
    } else {
        Err(())
    }
}

/// One `events.consume[]` entry of a routable service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerDecl {
    pub service: ServiceId,
    pub ns: Namespace,
    pub topic: String,
    pub group: String,
    pub start: StartPosition,
    pub batch_size: u32,
    pub max_attempts: u16,
    pub on_exhausted: OnExhausted,
}

/// The engine's view of services. Implemented by `rt-server`.
pub trait StreamTaskFactory: Send + Sync {
    /// Changes whenever the service table / configs change.
    fn generation(&self) -> u64;
    /// Consumers of routable services whose plan allows `TriggerKind::Stream`.
    fn consumers(&self) -> Vec<ConsumerDecl>;
    /// `None` = the service cannot take the batch now (throttled, no attempt).
    fn task_for(&self, service: ServiceId, batch: StreamBatch) -> Option<TaskSpec>;
}

#[derive(Clone, Debug)]
pub struct StreamEngineConfig {
    /// Longest sleep between passes when nothing woke the engine.
    pub poll: Duration,
    /// Partition heads and unresolved topics are re-read this often.
    pub head_refresh: Duration,
    /// Retention pass interval.
    pub trim_every: Duration,
    pub inflight_per_service: usize,
    pub inflight_node: usize,
}

impl Default for StreamEngineConfig {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(250),
            head_refresh: Duration::from_secs(5),
            trim_every: Duration::from_secs(60),
            inflight_per_service: STREAM_INFLIGHT_PER_SERVICE as usize,
            inflight_node: 256,
        }
    }
}

/// A cached cursor with the engine's own schedule (clock time).
struct Slot {
    cursor: Cursor,
    decl: Arc<ConsumerDecl>,
    topic_name: String,
    /// Not before (clock unix ms).
    next_at: i64,
    /// Consecutive throttled settles.
    throttles: u32,
}

struct Settled {
    key: CursorKey,
    service: ServiceId,
    /// Reloaded cursor after the settle (`None` = store error).
    cursor: Option<Cursor>,
    delay: Duration,
    throttled: bool,
}

type Inflight = FuturesUnordered<Pin<Box<dyn Future<Output = Settled> + Send>>>;

#[derive(Default)]
struct State {
    generation: Option<u64>,
    /// Declarations of the current generation with their topic once resolved.
    decls: Vec<(Arc<ConsumerDecl>, Option<Topic>)>,
    slots: HashMap<CursorKey, Slot>,
    heads: HashMap<TopicId, HashMap<u32, PartitionHead>>,
    inflight: HashSet<CursorKey>,
    per_service: HashMap<ServiceId, usize>,
    last_refresh: Option<i64>,
    last_trim: Option<i64>,
}

pub struct StreamEngine {
    cfg: StreamEngineConfig,
    store: Arc<dyn StreamStore>,
    tasks: Arc<dyn StreamTaskFactory>,
    submit: Submitter,
    clock: Arc<dyn Clock>,
    metrics: Option<Arc<rt_obs::Metrics>>,
    dirty_topics: Mutex<HashSet<TopicId>>,
    invalid: Mutex<HashSet<ServiceId>>,
    draining: AtomicBool,
    wake: Notify,
}

fn system_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn jitter_sample() -> f64 {
    rand::thread_rng().gen_range(-0.2..=0.2)
}

/// Stable error value of a failed delivery (stored as the cursor's `last_error`).
fn failure_error(o: &Outcome) -> String {
    match o {
        Outcome::Ok(r) => format!("status_{}", r.status.as_u16()),
        Outcome::GuestError => "guest_error".into(),
        Outcome::Killed(KillReason::CpuLimit) => "cpu_limit".into(),
        Outcome::Killed(KillReason::Oom) => "oom".into(),
        Outcome::Killed(KillReason::WallTimeout) => "wall_timeout".into(),
        Outcome::Killed(KillReason::Shutdown) => "shutdown".into(),
        Outcome::Rejected(_) => "rejected".into(),
        Outcome::BuildFailed(_) => "build_failed".into(),
    }
}

impl StreamEngine {
    pub fn new(
        cfg: StreamEngineConfig,
        store: Arc<dyn StreamStore>,
        tasks: Arc<dyn StreamTaskFactory>,
        submit: Submitter,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            cfg,
            store,
            tasks,
            submit,
            clock,
            metrics: None,
            dirty_topics: Mutex::new(HashSet::new()),
            invalid: Mutex::new(HashSet::new()),
            draining: AtomicBool::new(false),
            wake: Notify::new(),
        }
    }

    /// Count deliveries, dead letters, trims and skips into the `rt_stream_*` metrics.
    pub fn with_metrics(mut self, m: Arc<rt_obs::Metrics>) -> Self {
        self.metrics = Some(m);
        self
    }

    /// Records were appended to `topic`: re-read its heads now.
    pub fn notify(&self, topic: TopicId) {
        self.dirty_topics.lock().insert(topic);
        self.wake.notify_one();
    }

    /// The service's cursors changed outside the engine (an API reset): reload them.
    pub fn invalidate(&self, service: ServiceId) {
        self.invalid.lock().insert(service);
        self.wake.notify_one();
    }

    /// Stop dispatching new batches (in-flight ones still settle).
    pub fn drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
        self.wake.notify_one();
    }

    /// Dispatch and settle batches until the future is dropped. Dropping it forgets in-flight
    /// batches without settling them: they are redelivered (at-least-once).
    pub async fn run(&self) {
        let mut st = State::default();
        let mut inflight: Inflight = FuturesUnordered::new();
        loop {
            self.refresh(&mut st).await;
            if !self.draining.load(Ordering::SeqCst) {
                self.dispatch_ready(&mut st, &mut inflight).await;
            }
            self.maybe_trim(&mut st).await;

            let now = self.clock.unix_ms();
            let mut wait = self.cfg.poll;
            for (k, s) in &st.slots {
                if s.next_at > now && !st.inflight.contains(k) && !s.cursor.blocked {
                    wait = wait.min(Duration::from_millis((s.next_at - now) as u64));
                }
            }
            let sleep = self.clock.sleep(wait.max(Duration::from_millis(1)));
            tokio::select! {
                _ = sleep => {}
                _ = self.wake.notified() => {}
                Some(done) = inflight.next(), if !inflight.is_empty() => self.apply(&mut st, done),
            }
            // Apply everything else that already finished.
            while let Some(Some(done)) = futures_util::FutureExt::now_or_never(inflight.next()) {
                self.apply(&mut st, done);
            }
        }
    }

    fn apply(&self, st: &mut State, done: Settled) {
        st.inflight.remove(&done.key);
        if let Some(n) = st.per_service.get_mut(&done.service) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                st.per_service.remove(&done.service);
            }
        }
        let now = self.clock.unix_ms();
        if let Some(slot) = st.slots.get_mut(&done.key) {
            if let Some(c) = done.cursor {
                // A reload that predates a reset must not overwrite the reset cursor.
                if c.gen < slot.cursor.gen {
                    return;
                }
                let reset = c.gen > slot.cursor.gen;
                slot.cursor = c;
                if reset {
                    slot.next_at = now;
                    slot.throttles = 0;
                    return;
                }
            }
            slot.next_at = now.saturating_add(ms(done.delay));
            slot.throttles = if done.throttled {
                slot.throttles.saturating_add(1)
            } else {
                0
            };
        }
    }

    /// Generation changes, topic resolution, reset invalidations and partition heads.
    async fn refresh(&self, st: &mut State) {
        let now = self.clock.unix_ms();
        let periodic = st
            .last_refresh
            .is_none_or(|t| now.saturating_sub(t) >= ms(self.cfg.head_refresh));
        let gen = self.tasks.generation();
        let mut load_all = false;
        if st.generation != Some(gen) {
            st.generation = Some(gen);
            let old: HashMap<(ServiceId, String, String), Option<Topic>> = st
                .decls
                .drain(..)
                .map(|(d, t)| ((d.service, d.topic.clone(), d.group.clone()), t))
                .collect();
            st.decls = self
                .tasks
                .consumers()
                .into_iter()
                .map(|d| {
                    let t = old
                        .get(&(d.service, d.topic.clone(), d.group.clone()))
                        .cloned()
                        .flatten();
                    (Arc::new(d), t)
                })
                .collect();
            load_all = true;
        }
        if periodic {
            st.last_refresh = Some(now);
        }
        // Resolve topics (new generation, or periodically while unresolved).
        let mut topics_changed = false;
        for (d, t) in st.decls.iter_mut() {
            if t.is_some() && !load_all {
                continue;
            }
            if t.is_none() && !(load_all || periodic) {
                continue;
            }
            match self.store.get_topic(&d.ns, &d.topic).await {
                Ok(found) => {
                    if found.as_ref().map(|x| x.id) != t.as_ref().map(|x| x.id) {
                        topics_changed = true;
                    }
                    *t = found;
                }
                Err(e) => tracing::warn!(error = %e, "stream: topic lookup failed"),
            }
        }
        if load_all || topics_changed {
            self.rebuild_slots(st).await;
        }

        let invalid: Vec<ServiceId> = self.invalid.lock().drain().collect();
        for service in invalid {
            self.reload_service(st, service).await;
        }

        let mut dirty: HashSet<TopicId> = std::mem::take(&mut *self.dirty_topics.lock());
        if periodic || load_all || topics_changed {
            dirty.extend(st.decls.iter().filter_map(|(_, t)| t.as_ref().map(|t| t.id)));
        }
        for topic in dirty {
            if !st.decls.iter().any(|(_, t)| t.as_ref().is_some_and(|t| t.id == topic)) {
                continue;
            }
            self.load_heads(st, topic).await;
        }
    }

    async fn load_heads(&self, st: &mut State, topic: TopicId) {
        match self.store.heads(topic).await {
            Ok(h) => {
                st.heads
                    .insert(topic, h.into_iter().map(|h| (h.partition, h)).collect());
            }
            Err(e) => tracing::warn!(error = %e, "stream: reading partition heads failed"),
        }
    }

    /// Slots for every (resolved consumer, partition); keeps existing ones.
    async fn rebuild_slots(&self, st: &mut State) {
        let mut keep: HashSet<CursorKey> = HashSet::new();
        let decls = st.decls.clone();
        for (d, t) in decls {
            let Some(t) = t else { continue };
            for partition in 0..t.partitions {
                let key = CursorKey {
                    topic: t.id,
                    service: d.service,
                    group: d.group.clone(),
                    partition,
                };
                keep.insert(key.clone());
                if let Some(slot) = st.slots.get_mut(&key) {
                    slot.decl = d.clone();
                    continue;
                }
                match self.store.cursor(&key, d.start).await {
                    Ok(c) => {
                        let slot = self.slot_from(c, d.clone(), t.name.clone());
                        st.slots.insert(key, slot);
                    }
                    Err(e) => tracing::warn!(error = %e, "stream: loading a cursor failed"),
                }
            }
        }
        st.slots.retain(|k, _| keep.contains(k));
    }

    fn slot_from(&self, cursor: Cursor, decl: Arc<ConsumerDecl>, topic_name: String) -> Slot {
        // The store keeps its backoff in system time; the engine schedules on its own clock.
        let remaining = if cursor.next_at_ms > 0 {
            (cursor.next_at_ms - system_ms()).max(0)
        } else {
            0
        };
        Slot {
            cursor,
            decl,
            topic_name,
            next_at: self.clock.unix_ms().saturating_add(remaining),
            throttles: 0,
        }
    }

    async fn reload_service(&self, st: &mut State, service: ServiceId) {
        let cursors = match self.store.cursors_of(service).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "stream: reloading cursors failed");
                return;
            }
        };
        for c in cursors {
            if let Some(slot) = st.slots.get(&c.key) {
                let (decl, name) = (slot.decl.clone(), slot.topic_name.clone());
                let key = c.key.clone();
                let fresh = self.slot_from(c, decl, name);
                st.slots.insert(key, fresh);
            }
        }
        // Reset may have created cursors for new partitions of a known consumer.
        self.rebuild_slots(st).await;
    }

    async fn maybe_trim(&self, st: &mut State) {
        let now = self.clock.unix_ms();
        match st.last_trim {
            None => {
                st.last_trim = Some(now);
                return;
            }
            Some(t) if now.saturating_sub(t) < ms(self.cfg.trim_every) => return,
            Some(_) => {}
        }
        st.last_trim = Some(now);
        let stats = match self.store.trim(now).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "stream: retention pass failed");
                return;
            }
        };
        if let Some(m) = &self.metrics {
            m.stream_trimmed_total.inc_by(stats.records);
            m.stream_skipped_total
                .with_label_values(&["out_of_range"])
                .inc_by(stats.cursors_moved);
        }
        if stats.cursors_moved > 0 {
            tracing::warn!(
                cursors = stats.cursors_moved,
                "stream: consumers fell behind retention and skipped ahead"
            );
            let services: HashSet<ServiceId> = st.slots.keys().map(|k| k.service).collect();
            for s in services {
                self.reload_service(st, s).await;
            }
        }
        let topics: Vec<TopicId> = st.heads.keys().copied().collect();
        for t in topics {
            self.load_heads(st, t).await;
        }
    }

    async fn dispatch_ready(&self, st: &mut State, inflight: &mut Inflight) {
        let now = self.clock.unix_ms();
        let mut ready: Vec<CursorKey> = st
            .slots
            .iter()
            .filter(|(k, s)| {
                !s.cursor.blocked
                    && s.next_at <= now
                    && !st.inflight.contains(*k)
                    && st
                        .heads
                        .get(&k.topic)
                        .and_then(|h| h.get(&k.partition))
                        .is_some_and(|h| s.cursor.committed < h.next)
            })
            .map(|(k, _)| k.clone())
            .collect();
        // Stable order: oldest schedule first, then by key.
        ready.sort_by_key(|k| (st.slots[k].next_at, k.service, k.topic, k.group.clone(), k.partition));
        for key in ready {
            if st.inflight.len() >= self.cfg.inflight_node.max(1) {
                break;
            }
            if st.per_service.get(&key.service).copied().unwrap_or(0) >= self.cfg.inflight_per_service.max(1) {
                continue;
            }
            if self.dispatch(st, &key, inflight).await {
                st.inflight.insert(key.clone());
                *st.per_service.entry(key.service).or_default() += 1;
            }
        }
    }

    /// Read one batch for `key` and submit it. `false` = nothing was started.
    async fn dispatch(&self, st: &mut State, key: &CursorKey, inflight: &mut Inflight) -> bool {
        let Some(head) = st.heads.get(&key.topic).and_then(|h| h.get(&key.partition)).copied() else {
            return false;
        };
        let Some(slot) = st.slots.get_mut(key) else {
            return false;
        };
        if slot.cursor.committed < head.first {
            // Behind retention (records trimmed since the cursor was read): jump to the first one.
            match self.store.commit(key, slot.cursor.gen, head.first).await {
                Ok(true) => {
                    tracing::warn!(
                        partition = key.partition,
                        "stream: consumer fell behind retention and skipped ahead"
                    );
                    if let Some(m) = &self.metrics {
                        m.stream_skipped_total.with_label_values(&["out_of_range"]).inc();
                    }
                    slot.cursor.committed = head.first;
                    slot.cursor.attempt = 0;
                }
                Ok(false) => {
                    self.invalid.lock().insert(key.service);
                    return false;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "stream: commit failed");
                    return false;
                }
            }
            if slot.cursor.committed >= head.next {
                return false;
            }
        }
        let cursor = slot.cursor.clone();
        let decl = slot.decl.clone();
        let size = if cursor.attempt > 0 {
            1
        } else {
            decl.batch_size.max(1) as usize
        };
        let records = match self.read_batch(key, cursor.committed, size).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "stream: reading a batch failed");
                return false;
            }
        };
        if records.is_empty() {
            // The head was stale; wait for the next refresh.
            if let Some(h) = st.heads.get_mut(&key.topic).and_then(|h| h.get_mut(&key.partition)) {
                h.next = cursor.committed;
            }
            return false;
        }
        let batch = StreamBatch {
            topic: slot.topic_name.clone(),
            group: key.group.clone(),
            partition: key.partition,
            attempt: cursor.attempt.saturating_add(1),
            records,
        };
        let throttles = slot.throttles;
        let first = batch.records[0].offset;
        let last = batch.records[batch.records.len() - 1].offset;
        let job = Job {
            store: self.store.clone(),
            metrics: self.metrics.clone(),
            key: key.clone(),
            gen: cursor.gen,
            attempt_before: if cursor.committed == first { cursor.attempt } else { 0 },
            decl,
            first,
            last,
            throttles,
            records: batch.records.clone(),
        };
        let outcome: Pin<Box<dyn Future<Output = Outcome> + Send>> = match self.tasks.task_for(key.service, batch) {
            None => Box::pin(async { Outcome::Killed(KillReason::Shutdown) }),
            Some(spec) => match (self.submit)(spec) {
                Ok(fut) => fut,
                Err(r) => Box::pin(async move { Outcome::Rejected(r) }),
            },
        };
        inflight.push(Box::pin(async move {
            let o = outcome.await;
            job.settle(o).await
        }));
        true
    }

    /// Up to `size` contiguous records from `from`, at most [`DELIVERY_BATCH_MAX_BYTES`] of
    /// bodies (always at least one record when there is one).
    async fn read_batch(&self, key: &CursorKey, from: u64, size: usize) -> rt_store::Result<Vec<StreamRecord>> {
        let mut out: Vec<StreamRecord> = Vec::new();
        let mut bytes = 0usize;
        let mut next = from;
        while out.len() < size {
            let budget = DELIVERY_BATCH_MAX_BYTES.saturating_sub(bytes);
            if budget == 0 {
                break;
            }
            let limit = (size - out.len()).min(READ_LIMIT_MAX);
            let page = self
                .store
                .read(key.topic, key.partition, ReadFrom::Offset(next), limit, budget)
                .await?;
            if page.is_empty() {
                break;
            }
            let mut stop = page.len() < limit;
            for r in page {
                if !out.is_empty() && bytes + r.body.len() > DELIVERY_BATCH_MAX_BYTES {
                    stop = true;
                    break;
                }
                bytes += r.body.len();
                next = r.offset + 1;
                out.push(r);
            }
            if stop {
                break;
            }
        }
        Ok(out)
    }
}

/// How a finished delivery settles.
enum Verdict {
    /// Not an attempt: defer the cursor.
    Throttle,
    /// Commit through the last record.
    Commit,
    /// One failed attempt of the record at this offset.
    Fail(u64, String),
}

fn classify(o: &Outcome, first: u64, last: u64) -> Verdict {
    if !counts_as_attempt(o) {
        return Verdict::Throttle;
    }
    match o {
        Outcome::Ok(r) if r.status.is_success() => match parse_fail_at(&r.headers, first, last) {
            Ok(None) => Verdict::Commit,
            Ok(Some(f)) => Verdict::Fail(f, "fail_at".into()),
            Err(()) => Verdict::Fail(first, "bad_fail_at".into()),
        },
        other => Verdict::Fail(first, failure_error(other)),
    }
}

/// What a batch needs to settle once its outcome arrives.
struct Job {
    store: Arc<dyn StreamStore>,
    metrics: Option<Arc<rt_obs::Metrics>>,
    key: CursorKey,
    gen: u64,
    /// Failed attempts of the first record before this delivery.
    attempt_before: u16,
    decl: Arc<ConsumerDecl>,
    first: u64,
    last: u64,
    throttles: u32,
    records: Vec<StreamRecord>,
}

impl Job {
    async fn settle(self, outcome: Outcome) -> Settled {
        let (delay, throttled) = self.apply(outcome).await;
        let cursor = match self.store.cursor(&self.key, self.decl.start).await {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(error = %e, "stream: reloading a cursor failed");
                None
            }
        };
        Settled {
            key: self.key,
            service: self.decl.service,
            cursor,
            delay,
            throttled,
        }
    }

    fn delivered(&self, n: u64) {
        if let Some(m) = &self.metrics {
            m.stream_delivered_total.inc_by(n);
        }
    }

    async fn apply(&self, outcome: Outcome) -> (Duration, bool) {
        let key = &self.key;
        // Decide synchronously and drop the outcome (and its reply body) before any await.
        let verdict = classify(&outcome, self.first, self.last);
        drop(outcome);
        let (at, error) = match verdict {
            Verdict::Throttle => {
                let d = throttle_delay(self.throttles);
                if let Err(e) = self.store.defer(key, self.gen, d).await {
                    tracing::warn!(error = %e, "stream: defer failed");
                }
                return (d, true);
            }
            Verdict::Commit => {
                match self.store.commit(key, self.gen, self.last + 1).await {
                    Ok(true) => self.delivered(self.records.len() as u64),
                    Ok(false) => tracing::debug!("stream: settle after a reset ignored"),
                    Err(e) => tracing::warn!(error = %e, "stream: commit failed (batch will be redelivered)"),
                }
                return (Duration::ZERO, false);
            }
            Verdict::Fail(at, error) => (at, error),
        };
        let expected = if at == self.first {
            self.attempt_before.saturating_add(1)
        } else {
            1
        };
        let delay = stream_backoff(expected, jitter_sample());
        let attempt = match self.store.fail(key, self.gen, at, delay, &error).await {
            Ok(Some(a)) => a,
            Ok(None) => return (Duration::ZERO, false),
            Err(e) => {
                tracing::warn!(error = %e, "stream: recording a failed attempt failed");
                return (delay, false);
            }
        };
        self.delivered(at - self.first);
        if attempt < self.decl.max_attempts {
            return (delay, false);
        }
        let Some(record) = self.records.iter().find(|r| r.offset == at) else {
            return (delay, false); // LCOV_EXCL_LINE: `at` is always one of the delivered offsets
        };
        let res = match self.decl.on_exhausted {
            OnExhausted::Dlq => {
                let r = self.store.dead_letter(key, self.gen, record, attempt, &error).await;
                if matches!(r, Ok(true)) {
                    tracing::warn!(
                        partition = key.partition,
                        offset = at,
                        attempt,
                        "stream: record dead-lettered"
                    );
                    if let Some(m) = &self.metrics {
                        m.stream_dlq_total.inc();
                    }
                }
                r
            }
            OnExhausted::Skip => {
                let r = self.store.commit(key, self.gen, at + 1).await;
                if matches!(r, Ok(true)) {
                    tracing::warn!(
                        partition = key.partition,
                        offset = at,
                        attempt,
                        "stream: record skipped"
                    );
                    if let Some(m) = &self.metrics {
                        m.stream_skipped_total.with_label_values(&["exhausted"]).inc();
                    }
                }
                r
            }
            OnExhausted::Block => {
                let r = self.store.block(key, self.gen, &error).await;
                if matches!(r, Ok(true)) {
                    tracing::warn!(
                        partition = key.partition,
                        offset = at,
                        attempt,
                        "stream: consumer blocked"
                    );
                }
                r
            }
        };
        if let Err(e) = res {
            tracing::warn!(error = %e, "stream: exhausting a record failed");
            return (delay, false);
        }
        (Duration::ZERO, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_errors_are_stable_codes() {
        assert_eq!(failure_error(&Outcome::GuestError), "guest_error");
        assert_eq!(failure_error(&Outcome::Killed(KillReason::CpuLimit)), "cpu_limit");
        assert_eq!(failure_error(&Outcome::Killed(KillReason::Oom)), "oom");
        assert_eq!(failure_error(&Outcome::Killed(KillReason::WallTimeout)), "wall_timeout");
        assert_eq!(failure_error(&Outcome::Killed(KillReason::Shutdown)), "shutdown");
        assert_eq!(
            failure_error(&Outcome::Rejected(rt_types::RejectReason::Draining)),
            "rejected"
        );
    }
}
