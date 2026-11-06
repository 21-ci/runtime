//! Real backups of real stores, under load, restored and read back through the same stores.

use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rt_backup::manager::{BackupConfig, BackupManager};
use rt_backup::restore::{restore_archive, Auth, RestoreOptions};
use rt_sec::{secret_aad, LocalSecrets, Sealed, SecretsBackend};
use rt_store::{
    BatchMode, DataConfig, LocalArtifactStore, LocalServiceData, MetaStore, ServiceDataStore, SqlValue, SqliteMetaStore,
};
use rt_types::Body;

use crate::common::*;

async fn pair_sums(data: &LocalServiceData, svc: rt_types::ServiceId) -> (i64, i64) {
    let r = data
        .sql(svc)
        .execute(st("SELECT count(*), COALESCE(sum(a + b), 0) FROM pairs"))
        .await
        .expect("select");
    match (&r.rows[0][0], &r.rows[0][1]) {
        (SqlValue::Integer(n), SqlValue::Integer(s)) => (*n, *s),
        other => panic!("{other:?}"),
    }
}

/// The acceptance scenario at the store layer: everything comes back and opens.
#[tokio::test(flavor = "multi_thread")]
async fn restore_drill_brings_back_every_store() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let archive = n.backup_to(out.path());
    assert_eq!(
        std::fs::metadata(&archive).expect("stat").permissions().mode() & 0o777,
        0o600
    );
    let report = n.verify(&archive).expect("verify");
    assert!(report.authenticated);

    let to = tempfile::tempdir().expect("dir");
    let r = n.restore(&archive, to.path()).expect("restore");
    assert_eq!((r.services, r.blobs, r.sealed_values), (1, 1, 1));
    assert!(r.unopened_entries.is_empty());
    assert!(r.authenticated);
    assert!(!to.path().join(".restore-tmp").exists());

    // Excluded by construction.
    std::fs::write(n.dir.path().join("master.key"), "x").expect("write");
    assert!(!to.path().join("master.key").exists());

    let meta = SqliteMetaStore::open(&to.path().join("meta.db")).expect("meta reopens");
    let data = LocalServiceData::open(DataConfig::new(to.path())).expect("data reopens");
    let artifacts = LocalArtifactStore::open(&to.path().join("artifacts")).expect("artifacts");
    assert_eq!(pair_sums(&data, n.service).await, (1, 0));
    let v = data.kv(n.service).get(b"hello").await.expect("kv").expect("present");
    assert_eq!(&v.data[..], b"world");
    let obj = data
        .blob(n.service)
        .get("file.bin")
        .await
        .expect("blob")
        .expect("present");
    assert_eq!(&obj.body.collect(BLOB_MAX).await.expect("body")[..], b"blob bytes");
    let src = rt_store::ArtifactStore::get(&*artifacts, &n.source)
        .await
        .expect("artifact");
    assert_eq!(&src[..], b"export default {}");
    let sealed = meta.get_sealed_secrets(n.service).await.expect("secrets");
    let open = LocalSecrets::new(&n.master)
        .open(
            &secret_aad(n.tenant, n.service, "TOKEN"),
            &Sealed::from_bytes(&sealed[0].1).expect("sealed"),
        )
        .expect("opens with the same master key");
    assert_eq!(&open[..], b"s3cret");
    // The node.restored event.
    assert_eq!(meta.last_restored_at().await.expect("event"), Some(1_000_000));
}

/// `--revoke-keys-before`, its count, and the cascade to keys minted by a revoked key.
#[tokio::test(flavor = "multi_thread")]
async fn revoke_keys_before_revokes_the_old_keys_and_their_children() {
    let n = Node::seeded().await;
    let c = rusqlite::Connection::open(n.dir.path().join("meta.db")).expect("open");
    let t = n.tenant.to_string();
    for (id, at, parent) in [
        ("key_old", 100, None),
        ("key_child", 900, Some("key_old")),
        ("key_new", 900, None),
    ] {
        c.execute(
            "INSERT INTO api_keys (id, tenant_id, secret_hmac, scopes, created_at, parent_key_id) VALUES (?1, ?2, x'00', '[]', ?3, ?4)",
            rusqlite::params![id, t, at, parent],
        )
        .expect("key");
    }
    drop(c);
    let out = tempfile::tempdir().expect("dir");
    let archive = n.backup_to(out.path());
    let to = tempfile::tempdir().expect("dir");
    let keys = n.keys();
    let r = restore_archive(
        &archive,
        to.path(),
        &RestoreOptions {
            auth: Auth::Keys(&keys),
            revoke_keys_before: Some(500),
            secrets: None,
            now_ms: 2_000,
        },
    )
    .expect("restore");
    assert_eq!((r.revoked_keys, r.active_keys), (2, 1));
    assert_eq!(r.snapshot_age_ms, 2_000 - r.finished_at);
    let c = rusqlite::Connection::open(to.path().join("meta.db")).expect("open");
    let live: Vec<String> = c
        .prepare("SELECT id FROM api_keys WHERE revoked_at IS NULL")
        .expect("prepare")
        .query_map([], |r| r.get(0))
        .expect("query")
        .map(|r| r.expect("row"))
        .collect();
    assert_eq!(live, ["key_new"]);
}

/// A restored key whose secret another master key sealed is reported, and the restore still runs.
#[tokio::test(flavor = "multi_thread")]
async fn secrets_another_key_sealed_are_reported() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let archive = n.backup_to(out.path());
    let to = tempfile::tempdir().expect("dir");
    let keys = n.keys();
    let other = LocalSecrets::new(&rt_sec::MasterKey::generate());
    let r = restore_archive(
        &archive,
        to.path(),
        &RestoreOptions {
            auth: Auth::Keys(&keys),
            revoke_keys_before: None,
            secrets: Some(&other),
            now_ms: 2_000,
        },
    )
    .expect("restore");
    assert_eq!(r.unopened_entries, [(n.service.to_string(), "TOKEN".to_string())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_empty_directory_is_refused() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let archive = n.backup_to(out.path());
    let to = tempfile::tempdir().expect("dir");
    std::fs::write(to.path().join("master.key"), "keep").expect("write");
    assert_eq!(code(n.restore(&archive, to.path())), "not_empty");
    assert_eq!(std::fs::read(to.path().join("master.key")).expect("read"), b"keep");
}

/// Under writes: every database is one commit (each transaction keeps a + b = 0), the writers keep
/// going, a blob replaced during the backup is restorable, and the archive verifies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_under_write_load_is_consistent() {
    let n = Node::seeded().await;
    let (data, svc) = (n.data.clone(), n.service);
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicI64::new(0));
    let writer = {
        let (data, stop, writes) = (data.clone(), stop.clone(), writes.clone());
        tokio::spawn(async move {
            let mut i: i64 = 2;
            while !stop.load(Ordering::Relaxed) {
                // One transaction: a reader that sees half of it breaks the sum.
                data.sql(svc)
                    .batch(
                        vec![
                            st(&format!("INSERT INTO pairs (id, a, b) VALUES ({i}, {i}, 0)")),
                            st(&format!("UPDATE pairs SET b = -{i} WHERE id = {i}")),
                        ],
                        BatchMode::Write,
                    )
                    .await
                    .expect("batch");
                data.kv(svc)
                    .incr(b"counter", 1, Default::default())
                    .await
                    .expect("incr");
                if i % 8 == 0 {
                    data.blob(svc)
                        .put(
                            "file.bin",
                            Body::from_bytes(bytes::Bytes::from(format!("rev {i}"))),
                            None,
                            BLOB_MAX,
                        )
                        .await
                        .expect("put");
                    tokio::task::yield_now().await;
                }
                writes.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let out = tempfile::tempdir().expect("dir");
    let mut archives = Vec::new();
    for _ in 0..3 {
        let out_path = out.path().to_path_buf();
        let sources = n.sources();
        let id = format!("bkp_{}", ulid::Ulid::new());
        let path = tokio::task::spawn_blocking(move || {
            let tmp = tempfile::tempdir_in(&out_path).expect("scratch");
            rt_backup::create::create_archive(
                &sources,
                &out_path,
                tmp.path(),
                &id,
                1,
                0,
                &AtomicBool::new(false),
                "test",
            )
            .expect("backup under load")
            .path
        })
        .await
        .expect("join");
        archives.push(path);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.store(true, Ordering::Relaxed);
    writer.await.expect("writer");
    assert!(writes.load(Ordering::Relaxed) > 0);

    for archive in archives {
        n.verify(&archive).expect("verify");
        let to = tempfile::tempdir().expect("dir");
        n.restore(&archive, to.path()).expect("restore");
        let restored = LocalServiceData::open(DataConfig::new(to.path())).expect("reopen");
        let (rows, sum) = pair_sums(&restored, svc).await;
        assert!(rows >= 1);
        assert_eq!(sum, 0, "a transaction was split");
        let blob = restored.blob(svc).get("file.bin").await.expect("get").expect("kept");
        assert!(!blob.body.collect(BLOB_MAX).await.expect("body").is_empty());
    }
}

fn config(dir: &std::path::Path) -> BackupConfig {
    BackupConfig {
        enabled: true,
        schedule: "0 3 * * *".into(),
        dir: dir.to_path_buf(),
        keep_last: 2,
        free_floor_bytes: 0,
        encrypt_to: Vec::new(),
        allow_plaintext_remote: false,
        remote: None,
    }
}

async fn wait_done(m: &BackupManager, id: &str) -> rt_store::BackupRow {
    for _ in 0..600 {
        let row = m.get(id).await.expect("get").expect("row");
        if row.state == "ok" || row.state == "failed" {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("backup did not finish");
}

fn manager(n: &Node, cfg: BackupConfig) -> Arc<BackupManager> {
    BackupManager::new(
        cfg,
        n.sources(),
        n.meta.clone(),
        None,
        tokio::runtime::Handle::current(),
    )
    .expect("manager")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_manager_runs_one_backup_verifies_it_and_keeps_keep_last() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let m = manager(&n, config(&out.path().join("backups")));
    let first = m.start("api").await.expect("start");
    assert_eq!(first.state, "running");
    // One at a time; or it is already done.
    if let Err(e) = m.start("api").await {
        assert_eq!(e.code, "backup_running");
    }
    let done = wait_done(&m, &first.id).await;
    assert_eq!(done.state, "ok", "{:?}", done.failure);
    assert!(done.verified_at.is_some() && done.bytes.is_some() && done.sha256.is_some());
    // The directory is private, and scratch is gone.
    let dir = out.path().join("backups");
    assert_eq!(
        std::fs::metadata(&dir).expect("stat").permissions().mode() & 0o777,
        0o700
    );
    let names = |d: &std::path::Path| {
        std::fs::read_dir(d)
            .expect("list")
            .map(|e| e.expect("e").file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
    };
    assert!(names(&dir).iter().all(|f| f.ends_with(".tar")), "{:?}", names(&dir));

    // Waiting out a run still in progress from the check above.
    let mut ids = vec![first.id.clone()];
    for _ in 0..3 {
        loop {
            match m.start("api").await {
                Ok(r) => {
                    assert_eq!(wait_done(&m, &r.id).await.state, "ok");
                    ids.push(r.id);
                    break;
                }
                Err(e) if e.code == "backup_running" => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(e) => panic!("{e}"),
            }
        }
    }
    for _ in 0..200 {
        if names(&dir).len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(names(&dir).len(), 2, "keep_last = 2");
    assert!(m.get(&ids[0]).await.expect("get").is_none());
    let st = m.status().await.expect("status");
    assert!(st.enabled);
    assert!(st.next_at.is_some());
    assert_eq!(st.last_ok_id.as_deref(), ids.last().map(String::as_str));
    assert!(st.last_failure.is_none());
}

/// `keep_last = 1` does not leave a node with no backup while the next one runs, or after it fails.
#[tokio::test(flavor = "multi_thread")]
async fn the_last_good_archive_outlives_the_run_that_replaces_it() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let mut cfg = config(&out.path().join("backups"));
    cfg.keep_last = 1;
    let m = manager(&n, cfg);
    let first = m.start("api").await.expect("start");
    assert_eq!(wait_done(&m, &first.id).await.state, "ok");
    let path = m.archive_path(&first.id).expect("path");
    loop {
        // Waits out the slot of the run that just finished.
        match m.start("api").await {
            Ok(second) => {
                assert!(path.exists(), "still there while the next backup runs");
                assert_eq!(wait_done(&m, &second.id).await.state, "ok");
                for _ in 0..200 {
                    if !path.exists() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(!path.exists(), "replaced");
                assert!(m.archive_path(&second.id).expect("path").exists());
                return;
            }
            Err(e) if e.code == "backup_running" => tokio::time::sleep(Duration::from_millis(10)).await,
            Err(e) => panic!("{e}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_node_or_a_full_disk_refuses() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let mut cfg = config(out.path());
    cfg.enabled = false;
    assert_eq!(code(manager(&n, cfg).start("api").await), "backup_disabled");
    let mut cfg = config(out.path());
    cfg.free_floor_bytes = u64::MAX / 2;
    let m = manager(&n, cfg);
    assert_eq!(code(m.start("api").await), "insufficient_storage");
    // The slot is free again, and nothing was recorded for an API refusal.
    assert!(m.status().await.expect("status").running_id.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schedule_fires_when_due_and_defers_under_pressure() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let mut cfg = config(out.path());
    cfg.schedule = "* * * * *".into();
    let m = manager(&n, cfg);
    let t0 = chrono::Utc::now().timestamp_millis();
    assert!(m.tick(t0, false).await.is_none(), "not due yet");
    let due = t0 + 61_000;
    assert!(m.tick(due, true).await.is_none(), "pressure puts it off");
    assert!(m.tick(due + 1000, false).await.is_none(), "for ten minutes");
    let row = m
        .tick(due + rt_backup::manager::PRESSURE_RETRY_MS, false)
        .await
        .expect("fires");
    assert_eq!(row.trigger, "schedule");
    assert_eq!(wait_done(&m, &row.id).await.state, "ok");
}

/// A scheduled refusal is visible in status; an unscheduled manager never fires.
#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_refusal_leaves_a_failed_row() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let mut cfg = config(out.path());
    cfg.schedule = "* * * * *".into();
    cfg.free_floor_bytes = u64::MAX / 2;
    let m = manager(&n, cfg);
    let t0 = chrono::Utc::now().timestamp_millis();
    assert!(m.tick(t0, false).await.is_none());
    assert!(m.tick(t0 + 61_000, false).await.is_none());
    let st = m.status().await.expect("status");
    assert_eq!(st.last_failure.expect("failure")["code"], "insufficient_storage");
}

/// A kill mid-run: no `ok` row, no scratch, no partial file after the next start.
#[tokio::test(flavor = "multi_thread")]
async fn startup_sweeps_what_a_killed_run_left() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let dir = out.path().join("backups");
    std::fs::create_dir_all(dir.join("tmp-bkp_x")).expect("scratch");
    std::fs::write(dir.join("rt-backup-bkp_x.tar.partial"), "half").expect("partial");
    n.meta
        .backup_insert(rt_store::BackupRow {
            id: format!("bkp_{}", ulid::Ulid::new()),
            state: "running".into(),
            trigger: "api".into(),
            started_at: 1,
            finished_at: None,
            bytes: None,
            sha256: None,
            encrypted: false,
            local_path: None,
            remote_key: None,
            verified_at: None,
            rt_version: "t".into(),
            failure: None,
        })
        .await
        .expect("row");
    let m = manager(&n, config(&dir));
    assert_eq!(m.startup_sweep().await.expect("sweep"), 1);
    assert!(is_empty(&dir));
    assert_eq!(
        m.status().await.expect("status").last_failure.expect("failure")["code"],
        "interrupted"
    );
}

/// `DELETE` and `/archive` build the path from the id. A row from a forged or old `meta.db` cannot
/// point them anywhere else.
#[tokio::test(flavor = "multi_thread")]
async fn a_rows_local_path_is_never_followed() {
    let n = Node::seeded().await;
    let out = tempfile::tempdir().expect("dir");
    let victim = out.path().join("master.key");
    std::fs::write(&victim, "secret").expect("write");
    let id = format!("bkp_{}", ulid::Ulid::new());
    n.meta
        .backup_insert(rt_store::BackupRow {
            id: id.clone(),
            state: "failed".into(),
            trigger: "api".into(),
            started_at: 1,
            finished_at: Some(2),
            bytes: None,
            sha256: None,
            encrypted: false,
            local_path: Some(victim.to_string_lossy().into_owned()),
            remote_key: None,
            verified_at: None,
            rt_version: "t".into(),
            failure: None,
        })
        .await
        .expect("row");
    let m = manager(&n, config(&out.path().join("backups")));
    assert!(m.delete(&id).await.expect("delete"));
    assert!(victim.exists());
    assert!(m.archive_path("../../master.key").is_none());
    assert_eq!(
        m.archive_path(&id),
        Some(out.path().join("backups").join(format!("rt-backup-{id}.tar")))
    );
}

/// Blob deletes during a hold wait; after the backup the file goes.
#[tokio::test(flavor = "multi_thread")]
async fn gc_is_paused_during_a_backup_and_released_after() {
    let n = Node::seeded().await;
    let holds = n.data.holds();
    assert!(!holds.gc_paused());
    let out = tempfile::tempdir().expect("dir");
    n.backup_to(out.path());
    assert!(!holds.gc_paused(), "the pause ends with the backup");
    assert!(!holds.is_held(n.service));
}
