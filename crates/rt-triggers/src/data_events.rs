//! The pump of data-change events (R14): `_rt_changes` into the service's topic, at least once and
//! in order per service. Idle services cost one atomic load per pass.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use rt_store::data_events::{ChangeRow, DataEventsStore};
use rt_store::{NewRecord, StoreError, StreamStore};
use rt_types::events::{DataEventsConfig, Namespace, APPEND_BATCH_MAX, APPEND_BATCH_MAX_BYTES};
use rt_types::{ServiceId, TenantId};

/// Dirty services handled per pass.
pub const MAX_DIRTY_PER_PASS: usize = 500;
/// Passes a service stays looked at after a commit: a commit hook runs just before the commit is
/// visible, so the first read may miss it.
const LINGER_PASSES: u8 = 5;
/// Every this many passes each subscribed service is looked at regardless (schema drift, a slow
/// commit that outlived the linger).
const SWEEP_EVERY: u64 = 60;
/// Appends per service per pass, so one busy service does not hold the others.
const MAX_BATCHES_PER_PASS: usize = 10;

/// One service with `events.data_events`.
#[derive(Clone, Debug)]
pub struct DataEventsSub {
    pub service: ServiceId,
    pub tenant: TenantId,
    pub ns: Namespace,
    pub cfg: DataEventsConfig,
    /// The plan's stream cap for the tenant, in bytes.
    pub max_stream_bytes: u64,
}

/// What the node supplies.
pub trait DataEventsHost: Send + Sync {
    fn subscriptions(&self) -> Vec<DataEventsSub>;
    /// A service event (`data_events.paused`).
    fn service_event(&self, tenant: TenantId, service: ServiceId, action: &str, message: &str);
    /// Records were appended to a topic: wake its consumers.
    fn appended(&self, topic: rt_types::TopicId);
}

/// `state` of the status.
pub const STATE_ACTIVE: &str = "active";
pub const STATE_PAUSED: &str = "paused_overflow";
pub const STATE_ERROR: &str = "error";

/// What `GET /v1/services/{s}/data-events` reports of the pump.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PumpStatus {
    pub state: String,
    pub backlog: u64,
    pub lag_ms: u64,
    pub last_error: Option<String>,
    pub warnings: Vec<String>,
}

#[derive(Default)]
struct Entry {
    linger: u8,
    rearm: bool,
    status: PumpStatus,
}

pub struct DataEventsPump {
    store: Arc<dyn DataEventsStore>,
    streams: Arc<dyn StreamStore>,
    host: Arc<dyn DataEventsHost>,
    entries: Mutex<HashMap<ServiceId, Entry>>,
    now_ms: fn() -> i64,
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// The record of one change. Bodies are built from JSON the triggers made.
fn record(service: ServiceId, c: &ChangeRow) -> NewRecord {
    let parse = |s: &str| serde_json::from_str::<serde_json::Value>(s).unwrap_or(serde_json::Value::Null);
    let event_id = format!("{service}:{}", c.seq);
    let mut body = serde_json::json!({
        "event_id": event_id,
        "service": service.to_string(),
        "seq": c.seq,
        "ts": c.ts,
        "source": c.src,
        "op": c.op,
        "key": parse(&c.key),
    });
    if let Some(r) = &c.row {
        body["row"] = parse(r);
    }
    NewRecord {
        // One partition per service: that is what keeps its events in order.
        key: Some(service.to_string()),
        headers: Vec::new(),
        body: Bytes::from(body.to_string()),
        dedup_id: Some(event_id),
    }
}

impl DataEventsPump {
    pub fn new(store: Arc<dyn DataEventsStore>, streams: Arc<dyn StreamStore>, host: Arc<dyn DataEventsHost>) -> Self {
        Self {
            store,
            streams,
            host,
            entries: Mutex::new(HashMap::new()),
            now_ms: wall_ms,
        }
    }

    /// The config was saved: make triggers on the next pass, and clear an overflow pause.
    pub fn rearm(&self, service: ServiceId) {
        let mut entries = self.entries.lock();
        let e = entries.entry(service).or_default();
        e.rearm = true;
        e.linger = LINGER_PASSES;
    }

    pub fn status(&self, service: ServiceId) -> Option<PumpStatus> {
        self.entries.lock().get(&service).map(|e| e.status.clone())
    }

    /// One pass; `tick` counts from 1.
    pub async fn pass(&self, tick: u64) {
        let subs = self.host.subscriptions();
        self.retire_gone(&subs).await;
        let sweep = tick % SWEEP_EVERY == 1;
        let mut handled = 0;
        for sub in subs {
            let dirty = self.store.take_dirty(sub.service);
            let wanted = {
                let mut entries = self.entries.lock();
                let fresh = !entries.contains_key(&sub.service);
                let e = entries.entry(sub.service).or_default();
                if dirty {
                    e.linger = LINGER_PASSES;
                }
                let lingering = e.linger > 0;
                e.linger = e.linger.saturating_sub(1);
                dirty || lingering || fresh || e.rearm || sweep
            };
            if !wanted {
                continue;
            }
            if handled == MAX_DIRTY_PER_PASS {
                // Its turn comes next pass.
                if let Some(e) = self.entries.lock().get_mut(&sub.service) {
                    e.linger = e.linger.max(1);
                }
                continue;
            }
            handled += 1;
            let outcome = self.pump_one(&sub).await;
            let mut entries = self.entries.lock();
            let e = entries.entry(sub.service).or_default();
            match outcome {
                Ok(()) => e.status.last_error = None,
                Err(code) => {
                    e.status.state = STATE_ERROR.into();
                    e.status.last_error = Some(code);
                    // Keep trying: the topic may come back, or have room.
                    e.linger = e.linger.max(1);
                }
            }
        }
    }

    /// Unsubscribed services lose their triggers. A deleted one has nothing left to lose.
    async fn retire_gone(&self, subs: &[DataEventsSub]) {
        let gone: Vec<ServiceId> = {
            let mut entries = self.entries.lock();
            if entries.is_empty() {
                return;
            }
            let live: HashSet<ServiceId> = subs.iter().map(|s| s.service).collect();
            let gone: Vec<ServiceId> = entries.keys().filter(|id| !live.contains(*id)).copied().collect();
            for g in &gone {
                entries.remove(g);
            }
            gone
        };
        for g in gone {
            let _ = self.store.disable(g).await;
        }
    }

    async fn pump_one(&self, sub: &DataEventsSub) -> Result<(), String> {
        let s = sub.service;
        let code = |e: StoreError| match e {
            StoreError::Conflict("stream_full") => "stream_full".to_string(),
            _ => "storage_error".to_string(),
        };
        let rearm = self
            .entries
            .lock()
            .get_mut(&s)
            .is_some_and(|e| std::mem::take(&mut e.rearm));
        if rearm {
            self.store.resume(s).await.map_err(code)?;
        }
        let start = self
            .store
            .open_pass(s, &sub.cfg, APPEND_BATCH_MAX, APPEND_BATCH_MAX_BYTES / 2)
            .await
            .map_err(code)?;
        let (report, mut backlog, rows) = (start.report, start.backlog, start.rows);
        let mut paused = report.paused;
        if !paused && backlog > sub.cfg.max_backlog {
            // Give up events rather than break the service's writes.
            self.store.pause(s).await.map_err(code)?;
            paused = true;
            self.host.service_event(
                sub.tenant,
                s,
                "data_events.paused",
                "data-change backlog over max_backlog: events paused, PUT data-events to rearm",
            );
            tracing::warn!(service = %s, backlog, "data-change events paused on overflow");
        }
        let publish = self.publish(sub, rows, &mut backlog).await;
        let lag_ms = if backlog == 0 {
            0
        } else {
            match self.store.read_changes(s, 1, 1).await {
                Ok(rows) => rows.first().map_or(0, |c| ((self.now_ms)() - c.ts).max(0) as u64),
                Err(_) => 0, // LCOV_EXCL_LINE: a read right after the writes above; only for the status
            }
        };
        let mut entries = self.entries.lock();
        let e = entries.entry(s).or_default();
        e.status.state = if paused { STATE_PAUSED } else { STATE_ACTIVE }.into();
        e.status.backlog = backlog;
        e.status.lag_ms = lag_ms;
        e.status.warnings = report.warnings;
        if backlog > 0 {
            e.linger = e.linger.max(1);
        }
        publish
    }

    /// A change the stream refuses by its nature would refuse forever, and everything behind it
    /// would wait: one at a time, so that only that change is given up, said out loud.
    async fn append_singly(
        &self,
        sub: &DataEventsSub,
        topic: rt_types::TopicId,
        rows: &[ChangeRow],
    ) -> Result<bool, String> {
        let mut dropped = false;
        for c in rows {
            match self
                .streams
                .append(topic, vec![record(sub.service, c)], sub.max_stream_bytes)
                .await
            {
                Ok(_) => {}
                Err(StoreError::Invalid(_)) => {
                    dropped = true;
                    tracing::warn!(service = %sub.service, seq = c.seq, "data-change event dropped: not publishable");
                    self.host.service_event(
                        sub.tenant,
                        sub.service,
                        "data_events.dropped",
                        &format!("change {} cannot be published and was dropped", c.seq),
                    );
                }
                Err(StoreError::Conflict("stream_full")) => return Err("stream_full".into()),
                Err(_) => return Err("append_failed".into()),
            }
        }
        Ok(dropped)
    }

    async fn publish(&self, sub: &DataEventsSub, first: Vec<ChangeRow>, backlog: &mut u64) -> Result<(), String> {
        let s = sub.service;
        if *backlog == 0 {
            return Ok(());
        }
        // The service's own namespace is the only one looked in: a staging or preview service can
        // never reach a production topic of the same name.
        let topic = match self.streams.get_topic(&sub.ns, &sub.cfg.topic).await {
            Ok(Some(t)) => t,
            Ok(None) => return Err("topic_not_found".into()),
            Err(_) => return Err("storage_error".into()), // LCOV_EXCL_LINE: streams.db read failure
        };
        let mut rows = first;
        let mut dropped = false;
        for _ in 0..MAX_BATCHES_PER_PASS {
            let Some(last) = rows.last().map(|c| c.seq) else {
                break;
            };
            let records: Vec<NewRecord> = rows.iter().map(|c| record(s, c)).collect();
            match self.streams.append(topic.id, records, sub.max_stream_bytes).await {
                Ok(_) => {}
                Err(StoreError::Invalid(_)) => {
                    dropped |= self.append_singly(sub, topic.id, &rows).await?;
                }
                Err(StoreError::Conflict("stream_full")) => return Err("stream_full".into()),
                Err(_) => return Err("append_failed".into()),
            }
            self.host.appended(topic.id);
            // A crash here repeats the batch with the same dedup ids.
            self.store.ack(s, last).await.map_err(|_| "storage_error".to_string())?;
            *backlog = backlog.saturating_sub(rows.len() as u64);
            if *backlog == 0 {
                // What came since has set the dirty flag: the next pass.
                break;
            }
            rows = self
                .store
                .read_changes(s, APPEND_BATCH_MAX, APPEND_BATCH_MAX_BYTES / 2)
                .await
                .map_err(|_| "storage_error".to_string())?;
        }
        if dropped {
            return Err("event_dropped".into());
        }
        Ok(())
    }
}
