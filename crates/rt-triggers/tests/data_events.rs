//! The data-change pump (R14) on a real data store and a real streams store.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rt_store::data_events::{ChangeRow, DataEventsStore, PassStart, SyncReport};
use rt_store::{
    DataConfig, LocalServiceData, ReadFrom, ServiceDataStore, SqlBackend, SqlStatement, SqliteStreamStore, StreamStore,
    TopicSpec,
};
use rt_triggers::data_events::{DataEventsHost, DataEventsPump, DataEventsSub, STATE_ERROR, STATE_PAUSED};
use rt_types::events::{DataEventsConfig, Namespace};
use rt_types::{ProjectId, ServiceId, TenantId, TopicId};

struct Host {
    subs: Mutex<Vec<DataEventsSub>>,
    events: Mutex<Vec<String>>,
}

impl DataEventsHost for Host {
    fn subscriptions(&self) -> Vec<DataEventsSub> {
        self.subs.lock().clone()
    }
    fn service_event(&self, _: TenantId, _: ServiceId, action: &str, _: &str) {
        self.events.lock().push(action.into());
    }
    fn appended(&self, _: TopicId) {}
}

struct Rig {
    store: Arc<LocalServiceData>,
    sql: Arc<dyn SqlBackend>,
    streams: Arc<dyn StreamStore>,
    host: Arc<Host>,
    pump: DataEventsPump,
    ns: Namespace,
    service: ServiceId,
    _dir: tempfile::TempDir,
}

fn stmt(sql: &str) -> SqlStatement {
    SqlStatement {
        sql: sql.into(),
        args: Vec::new(),
    }
}

async fn rig(max_backlog: u64, topic: &str) -> Rig {
    let dir = tempfile::tempdir().expect("dir");
    let store = LocalServiceData::open(DataConfig::new(dir.path())).expect("store");
    let service = ServiceId::new();
    let sql = store.sql(service);
    sql.execute(stmt("CREATE TABLE orders (id INTEGER PRIMARY KEY)"))
        .await
        .expect("t");
    let streams: Arc<dyn StreamStore> = SqliteStreamStore::open_in_memory().expect("streams");
    let ns = Namespace {
        tenant: TenantId::new(),
        project: ProjectId::new(),
        environment: "prod".into(),
    };
    streams
        .create_topic(
            &ns,
            TopicSpec {
                name: "orders-changes".into(),
                partitions: 2,
                retention_ms: 86_400_000,
                max_bytes: 0,
            },
            10,
        )
        .await
        .expect("topic");
    let cfg: DataEventsConfig = serde_json::from_value(serde_json::json!({
        "topic": topic, "tables": [{"name": "orders"}], "max_backlog": max_backlog
    }))
    .expect("cfg");
    let host = Arc::new(Host {
        subs: Mutex::new(vec![DataEventsSub {
            service,
            tenant: ns.tenant,
            ns: ns.clone(),
            cfg,
            max_stream_bytes: 1 << 30,
        }]),
        events: Mutex::new(Vec::new()),
    });
    let pump = DataEventsPump::new(store.clone(), streams.clone(), host.clone());
    pump.pass(1).await; // makes the triggers
    Rig {
        store,
        sql,
        streams,
        host,
        pump,
        ns,
        service,
        _dir: dir,
    }
}

async fn bodies(r: &Rig) -> Vec<serde_json::Value> {
    let t = r
        .streams
        .get_topic(&r.ns, "orders-changes")
        .await
        .expect("get")
        .expect("topic");
    let mut out = Vec::new();
    for p in 0..t.partitions {
        let page = r
            .streams
            .read(t.id, p, ReadFrom::Earliest, 100, 1 << 20)
            .await
            .expect("read");
        out.extend(page.into_iter().map(|m| serde_json::from_slice(&m.body).expect("json")));
    }
    out
}

#[tokio::test]
async fn committed_changes_reach_the_topic_with_stable_ids_and_are_removed() {
    let r = rig(100, "orders-changes").await;
    r.sql.execute(stmt("INSERT INTO orders VALUES (1)")).await.expect("i");
    r.sql.execute(stmt("DELETE FROM orders WHERE id = 1")).await.expect("d");
    r.pump.pass(2).await;
    let got = bodies(&r).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0]["op"], "insert");
    assert_eq!(got[0]["key"], serde_json::json!([1]));
    assert_eq!(got[0]["source"], "sql:orders");
    assert_eq!(got[1]["op"], "delete");
    assert_eq!(got[0]["event_id"], format!("{}:{}", r.service, got[0]["seq"]));
    assert_eq!(r.store.backlog(r.service).await.expect("b"), 0);
    // Nothing happened: nothing more.
    r.pump.pass(3).await;
    assert_eq!(bodies(&r).await.len(), 2);
}

#[tokio::test]
async fn a_topic_in_another_namespace_is_never_reached() {
    let r = rig(100, "elsewhere").await;
    // The same name exists in production; the service's namespace is staging.
    let staging = Namespace {
        environment: "staging".into(),
        ..r.ns.clone()
    };
    r.host.subs.lock()[0].ns = staging;
    r.host.subs.lock()[0].cfg.topic = "orders-changes".into();
    r.sql.execute(stmt("INSERT INTO orders VALUES (1)")).await.expect("i");
    r.pump.pass(2).await;
    assert_eq!(bodies(&r).await.len(), 0);
    let st = r.pump.status(r.service).expect("status");
    assert_eq!(st.state, STATE_ERROR);
    assert_eq!(st.last_error.as_deref(), Some("topic_not_found"));
    assert_eq!(r.store.backlog(r.service).await.expect("b"), 1, "kept for later");
}

#[tokio::test]
async fn overflow_pauses_and_a_save_rearms() {
    let r = rig(2, "nope").await;
    for i in 0..3 {
        r.sql
            .execute(stmt(&format!("INSERT INTO orders VALUES ({i})")))
            .await
            .expect("i");
    }
    r.pump.pass(2).await;
    assert_eq!(r.host.events.lock().as_slice(), ["data_events.paused"]);
    assert_eq!(r.pump.status(r.service).expect("status").state, STATE_ERROR);
    r.host.subs.lock()[0].cfg.topic = "orders-changes".into();
    r.pump.pass(3).await;
    assert_eq!(r.pump.status(r.service).expect("status").state, STATE_PAUSED);
    assert_eq!(bodies(&r).await.len(), 3, "what was recorded still drains");
    r.sql
        .execute(stmt("INSERT INTO orders VALUES (10)"))
        .await
        .expect("guest writes go on");
    assert_eq!(r.store.backlog(r.service).await.expect("b"), 0);
    r.pump.rearm(r.service);
    r.pump.pass(4).await;
    r.sql.execute(stmt("INSERT INTO orders VALUES (11)")).await.expect("i");
    r.pump.pass(5).await;
    assert_eq!(bodies(&r).await.len(), 4);
}

#[tokio::test]
async fn a_crash_between_append_and_ack_duplicates_nothing_new() {
    let r = rig(100, "orders-changes").await;
    r.sql.execute(stmt("INSERT INTO orders VALUES (1)")).await.expect("i");
    let rows = r.store.read_changes(r.service, 10, 1 << 20).await.expect("rows");
    r.pump.pass(2).await;
    assert_eq!(bodies(&r).await.len(), 1);
    // The same row again, as after a crash before the delete: dedup by id on the stream.
    let t = r
        .streams
        .get_topic(&r.ns, "orders-changes")
        .await
        .expect("g")
        .expect("t");
    let id = format!("{}:{}", r.service, rows[0].seq);
    let mut rec = rt_store::NewRecord::new(&b"{}"[..]);
    rec.dedup_id = Some(id);
    let out = r.streams.append(t.id, vec![rec], 1 << 30).await.expect("append");
    assert!(out[0].duplicate);
}

#[tokio::test]
async fn unsubscribing_drops_the_triggers() {
    let r = rig(100, "orders-changes").await;
    r.host.subs.lock().clear();
    r.pump.pass(2).await;
    r.sql.execute(stmt("INSERT INTO orders VALUES (1)")).await.expect("i");
    assert_eq!(r.store.backlog(r.service).await.expect("b"), 0);
    assert!(r.pump.status(r.service).is_none());
}

/// Counts the store's jobs on the writer, and can hold one row too large to publish.
struct Spy {
    inner: Arc<LocalServiceData>,
    jobs: AtomicUsize,
    poison: Mutex<Option<ChangeRow>>,
}

impl Spy {
    fn with_poison(&self, mut rows: Vec<ChangeRow>) -> Vec<ChangeRow> {
        if let Some(p) = self.poison.lock().clone() {
            rows.insert(0, p);
        }
        rows
    }
}

#[async_trait::async_trait]
impl DataEventsStore for Spy {
    async fn sync_triggers(&self, s: ServiceId, c: &DataEventsConfig) -> rt_store::Result<SyncReport> {
        self.jobs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync_triggers(s, c).await
    }
    async fn open_pass(
        &self,
        s: ServiceId,
        c: &DataEventsConfig,
        limit: usize,
        max_bytes: usize,
    ) -> rt_store::Result<PassStart> {
        self.jobs.fetch_add(1, Ordering::SeqCst);
        let mut p = self.inner.open_pass(s, c, limit, max_bytes).await?;
        if self.poison.lock().is_some() {
            p.backlog += 1;
        }
        p.rows = self.with_poison(p.rows);
        Ok(p)
    }
    async fn disable(&self, s: ServiceId) -> rt_store::Result<()> {
        self.jobs.fetch_add(1, Ordering::SeqCst);
        self.inner.disable(s).await
    }
    fn take_dirty(&self, s: ServiceId) -> bool {
        self.inner.take_dirty(s)
    }
    async fn read_changes(&self, s: ServiceId, limit: usize, max: usize) -> rt_store::Result<Vec<ChangeRow>> {
        self.jobs.fetch_add(1, Ordering::SeqCst);
        Ok(self.with_poison(self.inner.read_changes(s, limit, max).await?))
    }
    async fn ack(&self, s: ServiceId, seq: i64) -> rt_store::Result<()> {
        self.jobs.fetch_add(1, Ordering::SeqCst);
        *self.poison.lock() = None;
        self.inner.ack(s, seq).await
    }
    async fn backlog(&self, s: ServiceId) -> rt_store::Result<u64> {
        self.jobs.fetch_add(1, Ordering::SeqCst);
        self.inner.backlog(s).await
    }
    async fn pause(&self, s: ServiceId) -> rt_store::Result<()> {
        self.inner.pause(s).await
    }
    async fn resume(&self, s: ServiceId) -> rt_store::Result<()> {
        self.inner.resume(s).await
    }
}

fn spied(r: &Rig, poison: Option<ChangeRow>) -> (Arc<Spy>, DataEventsPump) {
    let spy = Arc::new(Spy {
        inner: r.store.clone(),
        jobs: AtomicUsize::new(0),
        poison: Mutex::new(poison),
    });
    let pump = DataEventsPump::new(spy.clone(), r.streams.clone(), r.host.clone());
    (spy, pump)
}

/// M11: one committed change costs two turns on the guest's writer: open the pass, ack.
#[tokio::test]
async fn one_change_takes_two_jobs_on_the_writer() {
    let r = rig(100, "orders-changes").await;
    let (spy, pump) = spied(&r, None);
    r.sql.execute(stmt("INSERT INTO orders VALUES (1)")).await.expect("i");
    pump.pass(2).await;
    assert_eq!(bodies(&r).await.len(), 1);
    assert_eq!(spy.jobs.load(Ordering::SeqCst), 2);
}

/// M4: a row the stream refuses (say, left by an older node) is given up, and said so; the events
/// behind it flow.
#[tokio::test]
async fn an_unpublishable_row_is_dropped_and_the_rest_flows() {
    let r = rig(100, "orders-changes").await;
    let poison = ChangeRow {
        seq: 0,
        ts: 0,
        src: "sql:orders".into(),
        op: "insert".into(),
        key: format!("[\"{}\"]", "x".repeat(2 << 20)),
        row: None,
    };
    let (_, pump) = spied(&r, Some(poison));
    r.sql.execute(stmt("INSERT INTO orders VALUES (1)")).await.expect("i");
    pump.pass(2).await;

    let got = bodies(&r).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["key"], serde_json::json!([1]));
    assert_eq!(r.store.backlog(r.service).await.expect("b"), 0);
    assert_eq!(r.host.events.lock().as_slice(), ["data_events.dropped"]);
    let st = pump.status(r.service).expect("status");
    assert_eq!(st.last_error.as_deref(), Some("event_dropped"));
}

/// M4: a guest's primary key cannot make a change too large to publish; later events keep flowing.
#[tokio::test]
async fn a_huge_primary_key_does_not_wedge_the_events_behind_it() {
    let r = rig(100, "orders-changes").await;
    r.sql
        .execute(stmt("CREATE TABLE big (k TEXT PRIMARY KEY)"))
        .await
        .expect("t");
    r.host.subs.lock()[0]
        .cfg
        .tables
        .push(serde_json::from_value(serde_json::json!({"name": "big"})).expect("table"));
    r.pump.pass(2).await;
    r.sql
        .execute(stmt("INSERT INTO big VALUES (hex(zeroblob(1100000)))"))
        .await
        .expect("2 MiB key");
    r.sql.execute(stmt("INSERT INTO orders VALUES (1)")).await.expect("i");
    r.pump.pass(3).await;

    let got = bodies(&r).await;
    assert_eq!(got.len(), 2, "{:?}", r.pump.status(r.service));
    assert_eq!(got[0]["key"][0]["truncated"], true);
    assert!(got[0].to_string().len() < 128 * 1024);
    assert_eq!(got[1]["source"], "sql:orders");
    assert_eq!(r.store.backlog(r.service).await.expect("b"), 0);
}
