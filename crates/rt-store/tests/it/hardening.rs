//! Review fixes for the per-service data store: temp objects denied to guests, cancellation never
//! leaks connection slots or leaves an ownerless transaction on the writer, result accounting at
//! real memory cost, tenant-wide database ceilings, chunked TTL sweeps and per-connection SQL
//! functions answered by the writer.

use crate::common;

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use parking_lot::Mutex;
use rt_store::{
    BlockingRunner, DataConfig, KvSetOptions, KvValue, LocalServiceData, ServiceDataStore, ServiceDirectory, SqlValue,
    ValueTag,
};
use rt_types::{ServiceId, TenantId};

fn int(r: &rt_store::SqlResult) -> i64 {
    match r.rows.first().and_then(|row| row.first()) {
        Some(SqlValue::Integer(n)) => *n,
        other => panic!("expected one integer, got {other:?}"),
    }
}

/// Poll `f` `n` times (yielding in between) and drop it unless it finished: a caller that gives up.
async fn abandon_after<F: Future>(f: F, n: usize) -> bool {
    let mut f = Box::pin(f);
    for _ in 0..n {
        if std::future::poll_fn(|cx| Poll::Ready(f.as_mut().poll(cx).is_ready())).await {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

// ---- finding 1: no temp objects for guests ---------------------------------------------------------

#[tokio::test]
async fn guests_cannot_create_temp_objects_which_would_live_outside_every_quota() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (v INTEGER)")).await.expect("create");
    for ddl in [
        "CREATE TEMP TABLE big AS SELECT 1 AS v",
        "CREATE TEMPORARY TABLE x (v)",
        "CREATE TABLE temp.x (v)",
        "CREATE TEMP VIEW v AS SELECT 1",
        "CREATE VIEW temp.v2 AS SELECT 1",
        "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
        "CREATE VIRTUAL TABLE temp.f USING fts5(body)",
    ] {
        let err = sql.execute(st(ddl)).await.expect_err(ddl);
        assert_eq!(err.code(), "sql_not_authorized", "{ddl}: {err:?}");
        let err = sql
            .batch(vec![st(ddl)], rt_store::BatchMode::Write)
            .await
            .expect_err(ddl);
        assert_eq!(err.code(), "sql_not_authorized", "{ddl} in a batch: {err:?}");
    }
    let mut tx = sql.begin().await.expect("begin");
    assert_eq!(
        tx.execute(st("CREATE TEMP TABLE y (v)")).await.expect_err("tx").code(),
        "sql_not_authorized"
    );
    tx.rollback().await.expect("rollback");
    // Transient sort/materialization space spills to files under the page-cache bound.
    assert_eq!(
        store.pragma(svc(1), "temp_store").await.expect("temp_store"),
        SqlValue::Integer(1),
        "temp_store=FILE"
    );
    // Ordinary sorting and CTE materialization still work.
    let r = sql
        .execute(st("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 500) SELECT x FROM c ORDER BY x DESC LIMIT 1"))
        .await
        .expect("sort");
    assert_eq!(int(&r), 500);
}

// ---- finding 2: an evicting caller that gives up leaks no slot --------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callers_cancelled_during_eviction_do_not_leak_connection_slots() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cap = 2;
    let store = LocalServiceData::open(DataConfig {
        max_open_dbs: cap,
        readers_per_db: 0,
        ..data_cfg(dir.path())
    })
    .expect("open");
    for n in 1..=20u128 {
        store.sql(svc(n)).execute(st("SELECT 1")).await.expect("warm");
    }
    for round in 0..300usize {
        let sql = store.sql(svc(1 + (round % 20) as u128));
        abandon_after(sql.execute(st("SELECT 1")), 1 + round % 5).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    for n in 1..=20u128 {
        tokio::time::timeout(Duration::from_secs(5), store.sql(svc(n)).execute(st("SELECT 1")))
            .await
            .unwrap_or_else(|_| panic!("service {n} waits forever for a connection slot"))
            .expect("select");
        assert!(
            store.open_connections() <= cap,
            "cap exceeded: {}",
            store.open_connections()
        );
    }
}

// ---- finding 3: abandoned writer jobs keep the gate and never leave a transaction behind -------------

/// A runner whose jobs wait in a queue until the test runs them (or runs them at once in `auto`).
type QueuedJob = (Box<dyn FnOnce() + Send>, tokio::sync::oneshot::Sender<()>);

#[derive(Default)]
struct ManualRunner {
    manual: AtomicBool,
    jobs: Mutex<VecDeque<QueuedJob>>,
}

impl ManualRunner {
    fn queued(&self) -> usize {
        self.jobs.lock().len()
    }

    /// Run the oldest (`newest = false`) or newest queued job on this thread.
    fn step(&self, newest: bool) -> bool {
        let job = if newest {
            self.jobs.lock().pop_back()
        } else {
            self.jobs.lock().pop_front()
        };
        match job {
            Some((job, done)) => {
                job();
                let _ = done.send(());
                true
            }
            None => false,
        }
    }
}

impl BlockingRunner for ManualRunner {
    fn run(
        &self,
        _tenant: Option<TenantId>,
        job: Box<dyn FnOnce() + Send + 'static>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        if !self.manual.load(Ordering::SeqCst) {
            return Box::pin(async move {
                let _ = tokio::task::spawn_blocking(job).await;
            });
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.jobs.lock().push_back((job, tx));
        Box::pin(async move {
            let _ = rx.await;
        })
    }
}

fn manual_store(root: &std::path::Path) -> (Arc<LocalServiceData>, Arc<ManualRunner>) {
    let runner = Arc::new(ManualRunner::default());
    let store = LocalServiceData::open(DataConfig {
        runner: runner.clone(),
        ..data_cfg(root)
    })
    .expect("open");
    (store, runner)
}

/// Rows of `t` as a separate connection sees them: committed data only.
fn committed_rows(root: &std::path::Path) -> i64 {
    let db = find_files(root, "data.db").into_iter().next().expect("data.db");
    let c = rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).expect("open");
    c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).expect("count")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_begin_abandoned_while_queued_leaves_no_transaction_that_swallows_later_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, runner) = manual_store(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (v INTEGER)")).await.expect("create");

    runner.manual.store(true, Ordering::SeqCst);
    assert!(!abandon_after(sql.begin(), 3).await, "BEGIN waits in the runner queue");
    assert_eq!(runner.queued(), 1);
    assert!(runner.step(false), "the abandoned BEGIN still runs");
    runner.manual.store(false, Ordering::SeqCst);

    let r = sql
        .execute(st("INSERT INTO t VALUES (1)"))
        .await
        .expect("acknowledged insert");
    assert_eq!(r.rows_affected, 1);
    assert_eq!(
        committed_rows(dir.path()),
        1,
        "the acknowledged insert must be committed, not inside an orphaned BEGIN"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_abandoned_group_leader_keeps_the_writer_until_its_writes_ran() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, runner) = manual_store(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (v INTEGER)")).await.expect("create");
    let kv = store.kv(svc(1));
    kv.set(
        b"warm",
        KvValue {
            tag: ValueTag::Json,
            data: Bytes::from_static(b"1"),
        },
        KvSetOptions::default(),
    )
    .await
    .expect("warm");

    runner.manual.store(true, Ordering::SeqCst);
    let val = KvValue {
        tag: ValueTag::Json,
        data: Bytes::from_static(b"2"),
    };
    assert!(
        !abandon_after(kv.set(b"k", val, KvSetOptions::default()), 3).await,
        "the write waits in the runner queue"
    );
    assert_eq!(runner.queued(), 1);

    // A guest transaction starts meanwhile: it must not get the writer before the queued write ran.
    let begin = tokio::spawn({
        let sql = sql.clone();
        async move { sql.begin().await }
    });
    for _ in 0..20 {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // Run the newest job first: with the bug that is the transaction's BEGIN, ahead of the write.
    while runner.step(true) {}
    runner.manual.store(false, Ordering::SeqCst);
    let mut tx = begin.await.expect("join").expect("begin");
    tx.execute(st("INSERT INTO t VALUES (1)")).await.expect("insert in tx");
    tx.rollback().await.expect("rollback");

    assert_eq!(
        kv.get(b"k").await.expect("get"),
        Some(KvValue {
            tag: ValueTag::Json,
            data: Bytes::from_static(b"2")
        }),
        "the queued write committed on its own"
    );
    assert_eq!(committed_rows(dir.path()), 0);
}

// ---- finding 4: result caps count real memory ------------------------------------------------------

#[tokio::test]
async fn result_caps_charge_the_memory_each_value_takes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let nulls = vec!["NULL"; 1000].join(", ");
    // 400 rows x 1,000 NULLs: tiny payload, but ~13 MB of materialized values.
    let q =
        format!("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 400) SELECT {nulls} FROM c");
    let err = store.sql(svc(1)).execute(st(&q)).await.expect_err("over the byte cap");
    assert_eq!(err.code(), "sql_result_too_large");
    let small =
        format!("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100) SELECT {nulls} FROM c");
    assert_eq!(
        store.sql(svc(1)).execute(st(&small)).await.expect("~3 MB").rows.len(),
        100
    );
}

// ---- finding 5: one tenant-wide storage ceiling, applied to open databases -----------------------------

#[derive(Default)]
struct Growth(Mutex<HashMap<ServiceId, Option<u64>>>);

impl ServiceDirectory for Growth {
    fn tenant_of(&self, s: ServiceId) -> Option<TenantId> {
        self.0.lock().contains_key(&s).then(|| TenantId::from_u128(1))
    }
    fn db_growth_bytes(&self, s: ServiceId) -> Option<u64> {
        self.0.lock().get(&s).copied().unwrap_or(Some(0))
    }
}

fn blob_row(kib: usize) -> rt_store::SqlStatement {
    st_args(
        "INSERT INTO t VALUES (?)",
        vec![SqlValue::Blob(noise(kib * 1024, kib as u64))],
    )
}

#[tokio::test]
async fn databases_grow_only_by_the_tenants_remaining_quota_and_follow_its_changes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let growth = Arc::new(Growth::default());
    growth.0.lock().insert(svc(1), Some(512 * 1024));
    let store = LocalServiceData::open(DataConfig {
        directory: Some(growth.clone()),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (b BLOB)")).await.expect("create");
    sql.execute(blob_row(200)).await.expect("within the remaining quota");
    assert_eq!(
        sql.execute(blob_row(400)).await.expect_err("beyond it").code(),
        "storage_quota"
    );

    // The plan grows (or other services freed space): the open connection follows at the next refresh.
    growth.0.lock().insert(svc(1), Some(4 * 1024 * 1024));
    store.refresh_limits().await;
    sql.execute(blob_row(400))
        .await
        .expect("the larger ceiling applies to the open connection");

    // The tenant is out of quota: nothing may grow any more, reads and deletes still work.
    growth.0.lock().insert(svc(1), Some(0));
    store.refresh_limits().await;
    assert_eq!(
        sql.execute(blob_row(200)).await.expect_err("frozen").code(),
        "storage_quota"
    );
    assert_eq!(int(&sql.execute(st("SELECT count(*) FROM t")).await.expect("read")), 2);
    sql.execute(st("DELETE FROM t")).await.expect("delete");
}

#[tokio::test]
async fn a_database_of_an_unknown_service_cannot_grow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        directory: Some(Arc::new(Growth::default())),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let err = store
        .sql(svc(9))
        .execute(st("CREATE TABLE t (b BLOB)"))
        .await
        .expect_err("no owner, no growth");
    assert_eq!(err.code(), "storage_quota");
}

// ---- finding 8: the TTL sweep works through any backlog -----------------------------------------------

#[tokio::test]
async fn the_ttl_sweep_deletes_a_large_backlog_in_chunks() {
    let dir = tempfile::tempdir().expect("tempdir");
    // A short statement budget: one DELETE of the whole backlog would be interrupted every time.
    let store = LocalServiceData::open(DataConfig {
        sql_budget: Duration::from_millis(150),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let sql = store.sql(svc(1));
    store
        .kv(svc(1))
        .set(
            b"keep",
            KvValue {
                tag: ValueTag::Json,
                data: Bytes::from_static(b"1"),
            },
            KvSetOptions::default(),
        )
        .await
        .expect("seed");
    // 200,000 rows that expired long ago, written straight into the KV table.
    let db = find_files(dir.path(), "data.db").into_iter().next().expect("data.db");
    {
        let c = rusqlite::Connection::open(&db).expect("open");
        let tx = c.unchecked_transaction().expect("tx");
        for i in 0..200_000 {
            tx.execute(
                "INSERT INTO _rt_kv (key, value, tag, expires_at) VALUES (?1, x'00', 0, 1)",
                [format!("old{i}").as_bytes()],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    let _ = sql;
    assert_eq!(store.kv(svc(1)).sweep_expired().await.expect("sweep"), 200_000);
    assert_eq!(store.kv(svc(1)).sweep_expired().await.expect("again"), 0);
    assert!(store.kv(svc(1)).get(b"keep").await.expect("get").is_some());
}

// ---- finding 9: per-connection functions are answered by the writer -----------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_insert_rowid_and_changes_report_the_writers_last_statement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"))
        .await
        .expect("create");
    // Warm readers, so a query would normally go to one.
    let mut warm = Vec::new();
    for _ in 0..8 {
        let sql = sql.clone();
        warm.push(tokio::spawn(
            async move { sql.execute(st("SELECT count(*) FROM t")).await },
        ));
    }
    for w in warm {
        w.await.expect("join").expect("warm");
    }
    sql.execute(st("INSERT INTO t (v) VALUES (1), (2), (3)"))
        .await
        .expect("insert");
    assert_eq!(
        int(&sql.execute(st("SELECT last_insert_rowid()")).await.expect("rowid")),
        3
    );
    assert_eq!(int(&sql.execute(st("select changes()")).await.expect("changes")), 3);
    assert!(int(&sql.execute(st("SELECT total_changes()")).await.expect("total")) >= 3);
}
