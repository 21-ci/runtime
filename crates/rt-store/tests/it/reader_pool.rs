//! Reader pool per service database (B3.1): reads run on read-only connections next to the one
//! writer, so they proceed while the writer is busy (WAL snapshot), `SQLITE_BUSY` never reaches
//! callers under mixed load, and `storage.max_open_dbs` counts every connection.

use crate::common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{
    BatchMode, BlobListOptions, DataConfig, KvListOptions, KvSetOptions, KvValue, LocalServiceData, ServiceDataStore,
    SqlValue, ValueTag,
};
use rt_types::Body;

fn kv_val(s: &str) -> KvValue {
    KvValue {
        tag: ValueTag::Json,
        data: Bytes::from(s.to_string()),
    }
}

fn int(r: &rt_store::SqlResult) -> i64 {
    match r.rows.first().and_then(|row| row.first()) {
        Some(SqlValue::Integer(n)) => *n,
        other => panic!("expected one integer, got {other:?}"),
    }
}

async fn seeded(cfg: DataConfig) -> Arc<LocalServiceData> {
    let store = LocalServiceData::open(cfg).expect("open");
    let sql = store.sql(svc(1));
    sql.batch(
        vec![st("CREATE TABLE t (v INTEGER)"), st("INSERT INTO t VALUES (1)")],
        BatchMode::Write,
    )
    .await
    .expect("seed sql");
    store
        .kv(svc(1))
        .set(b"k", kv_val("1"), KvSetOptions::default())
        .await
        .expect("seed kv");
    store
        .blob(svc(1))
        .put("b", Body::from_bytes(Bytes::from_static(b"blob")), None, 1 << 20)
        .await
        .expect("seed blob");
    store
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_b31_reads_proceed_while_an_interactive_transaction_holds_the_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = seeded(data_cfg(dir.path())).await;
    let sql = store.sql(svc(1));

    // The transaction holds the writer for its whole life (up to the 5 s cap).
    let mut tx = sql.begin().await.expect("begin");
    tx.execute(st("INSERT INTO t VALUES (2)")).await.expect("insert in tx");

    let reads = async {
        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let sql = store.sql(svc(1));
                assert_eq!(
                    int(&sql.execute(st("SELECT count(*) FROM t")).await.expect("execute")),
                    1,
                    "the snapshot excludes uncommitted rows"
                );
                let batch = sql
                    .batch(vec![st("SELECT count(*) FROM t"), st("SELECT 2")], BatchMode::Read)
                    .await
                    .expect("read batch");
                assert_eq!(int(&batch[0]), 1);
                let kv = store.kv(svc(1));
                assert_eq!(kv.get(b"k").await.expect("kv get"), Some(kv_val("1")));
                assert_eq!(
                    kv.get_many(&[Bytes::from_static(b"k")]).await.expect("get_many").len(),
                    1
                );
                assert_eq!(
                    kv.list(KvListOptions {
                        prefix: Bytes::new(),
                        cursor: None,
                        limit: 10,
                        reverse: false
                    })
                    .await
                    .expect("kv list")
                    .entries
                    .len(),
                    1
                );
                let blob = store.blob(svc(1));
                assert_eq!(blob.head("b").await.expect("head").expect("exists").size, 4);
                assert!(blob.get("b").await.expect("get").is_some());
                assert_eq!(
                    blob.list(BlobListOptions {
                        prefix: String::new(),
                        cursor: None,
                        limit: 10
                    })
                    .await
                    .expect("list")
                    .items
                    .len(),
                    1
                );
            }));
        }
        for h in handles {
            h.await.expect("join");
        }
    };
    tokio::time::timeout(Duration::from_secs(3), reads)
        .await
        .expect("reads must not wait for the open transaction");

    tx.commit().await.expect("commit");
    assert_eq!(
        int(&sql.execute(st("SELECT count(*) FROM t")).await.expect("after commit")),
        2,
        "a read after the commit sees it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_b31_mixed_reads_and_writes_never_surface_busy_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = seeded(data_cfg(dir.path())).await;
    let mut handles = Vec::new();
    for w in 0..6 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let (sql, kv) = (store.sql(svc(1)), store.kv(svc(1)));
            for n in 0..100 {
                sql.execute(st_args(
                    "INSERT INTO t VALUES (?)",
                    vec![SqlValue::Integer(w * 1000 + n)],
                ))
                .await
                .expect("insert");
                kv.set(
                    format!("w{w}").as_bytes(),
                    kv_val(&n.to_string()),
                    KvSetOptions::default(),
                )
                .await
                .expect("kv set");
            }
        }));
    }
    for _ in 0..12 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let (sql, kv) = (store.sql(svc(1)), store.kv(svc(1)));
            for _ in 0..100 {
                assert!(int(&sql.execute(st("SELECT count(*) FROM t")).await.expect("select")) >= 1);
                sql.batch(vec![st("SELECT max(v) FROM t")], BatchMode::Read)
                    .await
                    .expect("read batch");
                kv.get(b"k").await.expect("kv get");
            }
        }));
    }
    for h in handles {
        h.await
            .expect("no error, in particular no busy error, may reach a caller");
    }
    assert_eq!(
        int(&store
            .sql(svc(1))
            .execute(st("SELECT count(*) FROM t"))
            .await
            .expect("count")),
        601
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_b31_readers_per_db_bounds_the_connections_of_one_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = seeded(DataConfig {
        readers_per_db: 2,
        ..data_cfg(dir.path())
    })
    .await;
    let done = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicUsize::new(0));
    let sampler = {
        let (store, done, peak) = (store.clone(), done.clone(), peak.clone());
        tokio::spawn(async move {
            while !done.load(Ordering::Relaxed) {
                peak.fetch_max(store.open_connections(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };
    let mut handles = Vec::new();
    for _ in 0..32 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..20 {
                store
                    .sql(svc(1))
                    .execute(st("SELECT count(*) FROM t"))
                    .await
                    .expect("select");
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    done.store(true, Ordering::Relaxed);
    sampler.await.expect("sampler");
    let peak = peak.load(Ordering::Relaxed).max(store.open_connections());
    assert!(peak <= 3, "1 writer + 2 readers at most, saw {peak}");
    assert!(
        peak >= 2,
        "concurrent reads open readers next to the writer, saw {peak}"
    );
}

#[tokio::test]
async fn ac_b31_without_readers_everything_uses_the_one_connection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = seeded(DataConfig {
        readers_per_db: 0,
        ..data_cfg(dir.path())
    })
    .await;
    for _ in 0..5 {
        store
            .sql(svc(1))
            .execute(st("SELECT count(*) FROM t"))
            .await
            .expect("select");
        store.kv(svc(1)).get(b"k").await.expect("get");
    }
    assert_eq!(store.open_connections(), 1);
}

#[tokio::test]
async fn ac_b31_execute_routes_writes_and_connection_state_to_the_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = seeded(data_cfg(dir.path())).await;
    let sql = store.sql(svc(1));
    // Warm a reader so the routing below really has one to pick.
    sql.execute(st("SELECT 1")).await.expect("warm");
    sql.execute(st("SELECT 1")).await.expect("warm");

    // Starts like a read, writes: it must run (once) on the writer.
    let r = sql
        .execute(st("WITH x(v) AS (SELECT 5) INSERT INTO t SELECT v FROM x"))
        .await
        .expect("CTE insert");
    assert_eq!(r.rows_affected, 1);
    assert_eq!(
        int(&sql
            .execute(st("SELECT count(*) FROM t WHERE v = 5"))
            .await
            .expect("select")),
        1
    );

    // Per-connection state (the last insert) is the writer's: such queries are answered there.
    sql.execute(st("INSERT INTO t VALUES (7)")).await.expect("insert");
    assert_eq!(int(&sql.execute(st("SELECT changes()")).await.expect("changes")), 1);
    // Temp objects are not available to guests at all (they would escape every quota).
    assert_eq!(
        sql.execute(st("CREATE TEMP TABLE scratch (v INTEGER)"))
            .await
            .expect_err("temp")
            .code(),
        "sql_not_authorized"
    );

    // Read batches stay read-only.
    let err = sql
        .batch(vec![st("INSERT INTO t VALUES (9)")], BatchMode::Read)
        .await
        .expect_err("write in a read batch");
    assert_eq!(err.code(), "sql_error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_b31_max_open_dbs_counts_connections_and_evicts_whole_services() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cap = 4;
    let store = LocalServiceData::open(DataConfig {
        max_open_dbs: cap,
        readers_per_db: 2,
        ..data_cfg(dir.path())
    })
    .expect("open");
    let mut handles = Vec::new();
    for n in 1..=12u128 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let sql = store.sql(svc(n));
            sql.execute(st("CREATE TABLE IF NOT EXISTS one (v INTEGER)"))
                .await
                .expect("create");
            sql.execute(st_args("INSERT INTO one VALUES (?)", vec![SqlValue::Integer(n as i64)]))
                .await
                .expect("insert");
            for _ in 0..10 {
                let r = sql.execute(st("SELECT v FROM one")).await.expect("select");
                assert_eq!(r.rows, vec![vec![SqlValue::Integer(n as i64)]]);
                assert!(
                    store.open_connections() <= cap,
                    "cap exceeded: {}",
                    store.open_connections()
                );
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    assert!(store.open_connections() <= cap);
    // Every service still reads its own data after the churn.
    for n in 1..=12u128 {
        let r = store
            .sql(svc(n))
            .execute(st("SELECT v FROM one"))
            .await
            .expect("reopen");
        assert_eq!(r.rows, vec![vec![SqlValue::Integer(n as i64)]]);
    }
}
