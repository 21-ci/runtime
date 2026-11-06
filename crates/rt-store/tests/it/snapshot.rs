//! F1/F2: `snapshot_db`, the dedicated heavy-job thread, the bounded export cache, the heavy-job
//! rule, `claim_snapshot_slot` and `BackupHolds`.

use crate::common;

use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::*;
use rt_store::{
    snapshot_db, BlockingRunner, DataConfig, KvSetOptions, KvValue, LocalServiceData, ServiceDataStore,
    ServiceDirectory, SqlValue, StoreError, ValueTag,
};
use rt_types::{Body, ServiceId, TenantId};

const BLOB_MAX: u64 = 1 << 30;

fn busy(e: StoreError) -> bool {
    e.code() == "heavy_job_busy"
}

/// `[blocking] threads = 1` of `edge-1gb`: one thread runs every store job, in order.
struct OneThread(mpsc::Sender<Box<dyn FnOnce() + Send>>);

impl OneThread {
    fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<Box<dyn FnOnce() + Send>>();
        std::thread::spawn(move || {
            while let Ok(job) = rx.recv() {
                job();
            }
        });
        Arc::new(Self(tx))
    }
}

impl BlockingRunner for OneThread {
    fn run(
        &self,
        _tenant: Option<TenantId>,
        job: Box<dyn FnOnce() + Send + 'static>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let (done, wait) = tokio::sync::oneshot::channel();
        self.0
            .send(Box::new(move || {
                job();
                let _ = done.send(());
            }))
            .expect("runner alive");
        Box::pin(async move {
            let _ = wait.await;
        })
    }
}

/// Everything belongs to the tenant `of` says.
struct Owners(Box<dyn Fn(ServiceId) -> TenantId + Send + Sync>);

impl ServiceDirectory for Owners {
    fn tenant_of(&self, s: ServiceId) -> Option<TenantId> {
        Some((self.0)(s))
    }
    fn db_growth_bytes(&self, _s: ServiceId) -> Option<u64> {
        None
    }
}

/// Parks the heavy thread until the returned sender is dropped; returns once it is parked.
fn park_heavy_thread(store: &LocalServiceData) -> mpsc::Sender<()> {
    let (release, parked) = mpsc::channel::<()>();
    let (entered_tx, entered) = mpsc::channel::<()>();
    store.run_on_heavy_thread(Box::new(move || {
        let _ = entered_tx.send(());
        let _ = parked.recv();
    }));
    entered.recv().expect("heavy thread runs the job");
    release
}

async fn seed(store: &LocalServiceData, s: ServiceId) {
    let sql = store.sql(s);
    sql.execute(st("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .await
        .expect("create");
    sql.execute(st("INSERT INTO t (v) VALUES ('one'), ('two')"))
        .await
        .expect("insert");
}

fn src_path(root: &std::path::Path, s: ServiceId) -> std::path::PathBuf {
    root.join("services").join(s.to_string()).join("data.db")
}

// ---- snapshot_db -------------------------------------------------------------------------------

/// The copy holds every committed row, stands alone (no WAL mark, no side files) and is private.
#[tokio::test]
async fn snapshot_db_copies_committed_rows_into_a_private_standalone_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    let dst = dir.path().join("copy.db");

    let size = snapshot_db(&src_path(dir.path(), svc(1)), &dst).expect("snapshot");

    assert_eq!(size, std::fs::metadata(&dst).expect("stat").len());
    assert_eq!(
        std::fs::metadata(&dst).expect("stat").permissions().mode() & 0o777,
        0o600
    );
    let head = std::fs::read(&dst).expect("read");
    assert!(head.starts_with(b"SQLite format 3\0"));
    assert_eq!((head[18], head[19]), (1, 1), "the copy must not announce WAL");
    assert!(!dir.path().join("copy.db-wal").exists());
    assert!(!dir.path().join("copy.db-journal").exists());
    let c = rusqlite::Connection::open(&dst).expect("open");
    let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).expect("count");
    assert_eq!(n, 2);
}

/// An existing destination is never overwritten, and a failed snapshot leaves nothing behind.
#[tokio::test]
async fn snapshot_db_refuses_an_existing_destination_and_cleans_up_on_failure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    let src = src_path(dir.path(), svc(1));

    let taken = dir.path().join("taken.db");
    std::fs::write(&taken, b"keep me").expect("write");
    assert!(snapshot_db(&src, &taken).is_err());
    assert_eq!(std::fs::read(&taken).expect("read"), b"keep me");

    let dst = dir.path().join("nope.db");
    assert!(snapshot_db(&dir.path().join("missing.db"), &dst).is_err());
    assert!(!dst.exists());
}

/// One step holds one read snapshot: writers keep going, and the copy is one consistent commit
/// (the two tables always move together). Prints the WAL size for the spike the plan asks for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_under_writes_is_consistent_and_does_not_stop_writers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE a (id INTEGER PRIMARY KEY, v BLOB)"))
        .await
        .expect("a");
    sql.execute(st("CREATE TABLE b (id INTEGER PRIMARY KEY, v BLOB)"))
        .await
        .expect("b");
    for _ in 0..200 {
        sql.batch(
            vec![
                st_args("INSERT INTO a (v) VALUES (?)", vec![SqlValue::Blob(vec![7; 4096])]),
                st_args("INSERT INTO b (v) VALUES (?)", vec![SqlValue::Blob(vec![7; 4096])]),
            ],
            rt_store::BatchMode::Write,
        )
        .await
        .expect("seed");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writes = {
        let (sql, stop) = (sql.clone(), stop.clone());
        tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                sql.batch(
                    vec![
                        st_args("INSERT INTO a (v) VALUES (?)", vec![SqlValue::Blob(vec![1; 256])]),
                        st_args("INSERT INTO b (v) VALUES (?)", vec![SqlValue::Blob(vec![1; 256])]),
                    ],
                    rt_store::BatchMode::Write,
                )
                .await
                .expect("write during snapshot");
                n += 1;
            }
            n
        })
    };

    let wal = src_path(dir.path(), svc(1)).with_extension("db-wal");
    let mut wal_max = 0;
    let mut last = None;
    for _ in 0..10 {
        let file = sql.export().await.expect("export");
        wal_max = wal_max.max(std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0));
        let c = rusqlite::Connection::open(file.path()).expect("open");
        let (a, b): (i64, i64) = c
            .query_row("SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b)", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .expect("counts");
        assert_eq!(a, b, "a snapshot saw half of a transaction");
        last = Some(a);
    }
    stop.store(true, Ordering::Relaxed);
    let n = writes.await.expect("writer");
    eprintln!("spike: {n} write txs during 10 snapshots; last snapshot {last:?} rows; WAL peak {wal_max} bytes");
    assert!(n > 0);
}

// ---- thread and stalls -------------------------------------------------------------------------

/// `edge-1gb`: with one blocking thread, KV and SQL on the snapshotted service and on others keep
/// answering while a snapshot sits on the heavy thread, and no job of it holds the blocking thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_does_not_stall_during_a_snapshot_on_one_blocking_thread() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        runner: OneThread::new(),
        ..data_cfg(dir.path())
    })
    .expect("open");
    seed(&store, svc(1)).await;
    seed(&store, svc(2)).await;

    let parked = park_heavy_thread(&store);
    let sql = store.sql(svc(1));
    let export = tokio::spawn(async move { sql.export().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!export.is_finished(), "the export must wait for the heavy thread");

    let quick = Duration::from_secs(5);
    tokio::time::timeout(quick, async {
        for s in [svc(1), svc(2)] {
            store
                .kv(s)
                .set(
                    b"k",
                    KvValue {
                        tag: ValueTag::Bytes,
                        data: Bytes::from_static(b"v"),
                    },
                    KvSetOptions::default(),
                )
                .await
                .expect("kv set");
            store
                .sql(s)
                .execute(st("INSERT INTO t (v) VALUES ('during')"))
                .await
                .expect("sql write");
            store
                .sql(s)
                .execute(st("SELECT count(*) FROM t"))
                .await
                .expect("sql read");
        }
    })
    .await
    .expect("storage stalled behind a snapshot");

    drop(parked);
    let file = export.await.expect("join").expect("export");
    let c = rusqlite::Connection::open(file.path()).expect("open");
    let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).expect("count");
    assert_eq!(n, 3, "the snapshot starts when the thread reaches it");
}

/// Heavy jobs run on one thread of their own, not on the blocking pool.
#[tokio::test]
async fn heavy_jobs_run_on_one_named_thread() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let names = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..3 {
        let (names, (tx, rx)) = (names.clone(), mpsc::channel());
        store.run_on_heavy_thread(Box::new(move || {
            names
                .lock()
                .expect("names")
                .push(std::thread::current().name().map(str::to_string));
            let _ = tx.send(());
        }));
        rx.recv().expect("ran");
    }
    let names = names.lock().expect("names");
    assert!(names.iter().all(|n| n.as_deref() == Some("rt-snapshot")), "{names:?}");
}

/// A panicking job does not take the thread with it.
#[tokio::test]
async fn a_panicking_heavy_job_leaves_the_thread_running() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    store.run_on_heavy_thread(Box::new(|| panic!("job failed on purpose")));
    store.sql(svc(1)).export().await.expect("export after a panic");
}

// ---- heavy-job rule ----------------------------------------------------------------------------

/// One heavy job per tenant, two queued on the node; the refusal is a stable code and frees up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heavy_jobs_are_one_per_tenant_and_two_on_the_node() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        directory: Some(Arc::new(Owners(Box::new(|s| {
            // Services 1 and 2 share a tenant; 3 and 4 have their own.
            let n = if s == svc(1) || s == svc(2) {
                1
            } else if s == svc(3) {
                3
            } else {
                4
            };
            TenantId::from_u128(n)
        })))),
        ..data_cfg(dir.path())
    })
    .expect("open");
    for n in 1..=4 {
        seed(&store, svc(n)).await;
    }
    let parked = park_heavy_thread(&store);

    let sql1 = store.sql(svc(1));
    let first = tokio::spawn(async move { sql1.export().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let same_tenant = store.sql(svc(2)).export().await.expect_err("same tenant refused");
    assert!(busy(same_tenant));

    let sql3 = store.sql(svc(3));
    let other = tokio::spawn(async move { sql3.export().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let node_full = store.sql(svc(4)).export().await.expect_err("node full");
    assert!(busy(node_full));

    drop(parked);
    first.await.expect("join").expect("first");
    other.await.expect("join").expect("other");
    store.sql(svc(2)).export().await.expect("the slot came back");
    store.sql(svc(4)).export().await.expect("the node slot came back");
}

// ---- export cache ------------------------------------------------------------------------------

/// A snapshot is found again by its own service and ETag only; a tampered tag never reaches a path.
#[tokio::test]
async fn export_cache_is_keyed_by_service_and_etag() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    seed(&store, svc(2)).await;

    let file = store.sql(svc(1)).export().await.expect("export");
    assert!(file.path().starts_with(dir.path().join("exports")));
    assert!(std::fs::read(file.path())
        .expect("read")
        .starts_with(b"SQLite format 3\0"));
    assert_eq!(file.size(), std::fs::metadata(file.path()).expect("stat").len());

    let again = store.sql(svc(1)).export_cached(file.etag()).await.expect("found");
    assert_eq!(again.path(), file.path());

    assert!(
        store.sql(svc(2)).export_cached(file.etag()).await.is_none(),
        "another service"
    );
    for tag in ["", "../../services/x/data.db", "..", "/etc/passwd", "nope"] {
        assert!(store.sql(svc(1)).export_cached(tag).await.is_none(), "{tag}");
    }
}

/// The TTL ends a snapshot, and its file goes when the last reader lets go of it.
#[tokio::test]
async fn export_ttl_unlinks_the_file_after_the_last_reader() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        export_ttl: Duration::from_millis(1),
        ..data_cfg(dir.path())
    })
    .expect("open");
    seed(&store, svc(1)).await;
    let file = store.sql(svc(1)).export().await.expect("export");
    let path = file.path().to_path_buf();
    tokio::time::sleep(Duration::from_millis(5)).await;

    store.sweep_open().await.expect("sweep");
    assert!(path.exists(), "a reader still streams it");
    assert!(store.sql(svc(1)).export_cached(file.etag()).await.is_none());
    drop(file);
    assert!(!path.exists());
}

/// Past the byte cap, idle snapshots go first; only then is a new one refused, with nothing written.
#[tokio::test]
async fn export_cache_cap_evicts_idle_snapshots_then_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    let size = store.sql(svc(1)).export().await.expect("probe").size();
    drop(store);

    let store = LocalServiceData::open(DataConfig {
        export_cache_max_bytes: size + size / 2,
        ..data_cfg(dir.path())
    })
    .expect("reopen");

    // Idle: the older one makes room.
    let first = store.sql(svc(1)).export().await.expect("first");
    let (first_tag, first_path) = (first.etag().to_string(), first.path().to_path_buf());
    drop(first);
    let second = store.sql(svc(1)).export().await.expect("second evicts the idle first");
    assert!(!first_path.exists());
    assert!(store.sql(svc(1)).export_cached(&first_tag).await.is_none());

    // In use: nothing can go.
    let err = store.sql(svc(1)).export().await.expect_err("over the cap");
    assert_eq!(err.code(), "insufficient_storage");
    drop(second);
    store.sql(svc(1)).export().await.expect("room again");
}

/// Under the global disk floor nothing is even started.
#[tokio::test]
async fn scratch_is_refused_under_the_disk_floor() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        scratch_floor_bytes: u64::MAX / 2,
        ..data_cfg(dir.path())
    })
    .expect("open");
    seed(&store, svc(1)).await;
    let err = store.sql(svc(1)).export().await.expect_err("refused");
    assert_eq!(err.code(), "insufficient_storage");
    let err = store.sql(svc(1)).copy_into(svc(2)).await.expect_err("refused");
    assert_eq!(err.code(), "insufficient_storage");
    assert!(!src_path(dir.path(), svc(2)).exists());
}

/// Files left by a crash go when the store opens; purging a service forgets its snapshots.
#[tokio::test]
async fn exports_do_not_outlive_a_restart_or_a_purge() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    let file = store.sql(svc(1)).export().await.expect("export");
    let tag = file.etag().to_string();
    let path = file.path().to_path_buf();
    drop(file);

    store.purge(svc(1)).expect("purge");
    assert!(store.sql(svc(1)).export_cached(&tag).await.is_none());
    assert!(!path.exists());
    drop(store);

    let leftovers = dir.path().join("exports");
    std::fs::create_dir_all(&leftovers).expect("mkdir");
    std::fs::write(leftovers.join("crashed"), b"x").expect("write");
    let _store = data(dir.path());
    assert!(!leftovers.join("crashed").exists());
}

/// An export of a retired service is gone; a purged one exports empty and revives, as before.
#[tokio::test]
async fn export_of_purged_and_retired_services() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    store.purge(svc(1)).expect("purge");
    let file = store.sql(svc(1)).export().await.expect("purged exports empty");
    let c = rusqlite::Connection::open(file.path()).expect("open");
    let n: i64 = c
        .query_row("SELECT count(*) FROM sqlite_master WHERE name = 't'", [], |r| r.get(0))
        .expect("count");
    assert_eq!(n, 0);

    seed(&store, svc(2)).await;
    store.retire(svc(2)).expect("retire");
    let err = store.sql(svc(2)).export().await.expect_err("retired");
    assert_eq!(err.code(), "not_found");
}

/// A caller that goes away while queued costs no snapshot and leaves no file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_export_leaves_nothing_behind() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    let parked = park_heavy_thread(&store);
    let sql = store.sql(svc(1));
    let job = tokio::spawn(async move { sql.export().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    job.abort();
    let _ = job.await;
    drop(parked);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // The slot comes back once the thread dropped the job.
        match store.sql(svc(1)).export().await {
            Ok(file) => {
                let left: Vec<_> = std::fs::read_dir(dir.path().join("exports"))
                    .expect("dir")
                    .map(|e| e.expect("entry").path())
                    .collect();
                assert_eq!(left, vec![file.path().to_path_buf()]);
                break;
            }
            Err(e) if e.code() == "heavy_job_busy" && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => panic!("{e}"),
        }
    }
}

// ---- snapshot slot -----------------------------------------------------------------------------

/// The snapshot connection counts against `max_open_dbs` and evicts nobody: a full pool keeps its
/// writers, the count never passes the cap (STO-14).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_slot_is_charged_and_never_evicts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        max_open_dbs: 4,
        readers_per_db: 0,
        ..data_cfg(dir.path())
    })
    .expect("open");
    for n in 1..=3 {
        seed(&store, svc(n)).await;
    }
    assert_eq!(store.open_connections(), 3);

    // A fourth service evicts within the pool's share: the snapshot's slot is not for writers.
    seed(&store, svc(4)).await;
    assert_eq!(store.open_connections(), 3, "one slot stays reserved for a snapshot");
    let open_before = store.open_connections();

    let (seen_tx, seen) = mpsc::channel();
    let probe = store.clone();
    store.sql(svc(1)).export().await.expect("export");
    store.run_on_heavy_thread(Box::new(move || {
        let _ = seen_tx.send(probe.open_connections());
    }));
    assert!(seen.recv().expect("probe") <= 4);
    assert_eq!(
        store.open_connections(),
        open_before,
        "nobody was evicted, and the slot came back"
    );
}

/// A very small pool reserves nothing: a held writer makes the snapshot wait, then refuse.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_slot_waits_and_gives_up_instead_of_evicting() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        max_open_dbs: 1,
        readers_per_db: 0,
        snapshot_slot_wait: Duration::from_millis(100),
        ..data_cfg(dir.path())
    })
    .expect("open");
    seed(&store, svc(1)).await;
    let err = store.sql(svc(1)).export().await.expect_err("no slot");
    assert!(busy(err));
    assert_eq!(store.open_connections(), 1);
    store
        .sql(svc(1))
        .execute(st("SELECT 1"))
        .await
        .expect("the writer was left alone");
}

// ---- holds -------------------------------------------------------------------------------------

async fn put(store: &LocalServiceData, s: ServiceId, key: &str, body: &'static [u8]) {
    store
        .blob(s)
        .put(key, Body::from_bytes(Bytes::from_static(body)), None, BLOB_MAX)
        .await
        .expect("put");
}

fn objects(root: &std::path::Path, s: ServiceId) -> usize {
    std::fs::read_dir(root.join("blobs").join(s.to_string()).join("objects"))
        .map(|d| d.count())
        .unwrap_or(0)
}

/// While held, overwritten and deleted blobs keep their files; release unlinks them.
#[tokio::test]
async fn a_hold_defers_blob_unlinks_until_the_last_release() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    put(&store, svc(1), "over", b"old").await;
    put(&store, svc(1), "gone", b"bye").await;
    put(&store, svc(2), "free", b"old").await;

    let holds = store.holds();
    let (one, two) = (holds.hold(svc(1)), holds.hold(svc(1)));
    assert!(holds.is_held(svc(1)));

    put(&store, svc(1), "over", b"new").await;
    assert!(store.blob(svc(1)).delete("gone").await.expect("delete"));
    assert_eq!(objects(dir.path(), svc(1)), 3, "both old files stay while held");

    // Another service is not held.
    put(&store, svc(2), "free", b"new").await;
    assert_eq!(objects(dir.path(), svc(2)), 1);

    drop(one);
    assert_eq!(objects(dir.path(), svc(1)), 3, "another hold remains");
    drop(two);
    assert!(!holds.is_held(svc(1)));
    assert_eq!(objects(dir.path(), svc(1)), 1, "deferred deletes drained");

    // And it is back to immediate.
    assert!(store.blob(svc(1)).delete("over").await.expect("delete"));
    assert_eq!(objects(dir.path(), svc(1)), 0);
}

/// The GC pause is a counter: paused until the last one lets go.
#[tokio::test]
async fn gc_pause_nests() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let holds = store.holds();
    assert!(!holds.gc_paused());
    let (a, b) = (holds.pause_gc(), holds.pause_gc());
    assert!(holds.gc_paused());
    drop(a);
    assert!(holds.gc_paused());
    drop(b);
    assert!(!holds.gc_paused());
}
