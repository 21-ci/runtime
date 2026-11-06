//! S3 destination and age encryption: against a mock on loopback, never an external host.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use age::secrecy::ExposeSecret;
use rt_backup::archive::{sign_manifest, MANIFEST};
use rt_backup::encrypt::AgeIdentity;
use rt_backup::manager::{BackupConfig, BackupManager};
use rt_backup::remote::{RemoteConfig, RetentionPolicy};
use rt_backup::restore::{restore_archive_age, verify_archive_age, Auth, RestoreOptions};
use rt_backup::s3::{S3Config, S3Credentials};
use rt_sec::MasterKey;

use crate::common::{code, is_empty, read_entries, write_entries, Node};
use crate::mock_s3::{MockS3, BUCKET};

const SECRET_KEY: &str = "very-secret-key-do-not-log";
const BLOB_MARKER: &[u8] = b"blob bytes"; // stored raw in a plaintext archive

struct Keys {
    identity: age::x25519::Identity,
    recipient: String,
    file: tempfile::NamedTempFile,
}

fn keys() -> Keys {
    let identity = age::x25519::Identity::generate();
    let recipient = identity.to_public().to_string();
    let file = tempfile::NamedTempFile::new().expect("file");
    std::fs::write(file.path(), format!("{}\n", identity.to_string().expose_secret())).expect("write");
    Keys {
        identity,
        recipient,
        file,
    }
}

fn s3_config(url: &str) -> S3Config {
    let mut c = S3Config::new(
        url,
        BUCKET,
        "node1",
        "auto",
        S3Credentials::new("access-id", SECRET_KEY),
    );
    c.part_bytes = 1024;
    c
}

fn remote(url: &str) -> RemoteConfig {
    RemoteConfig {
        s3: s3_config(url),
        keep_local: 1,
        retention: RetentionPolicy {
            keep_last: 7,
            keep_daily: 0,
            keep_weekly: 0,
        },
    }
}

fn config(dir: &Path, recipients: Vec<String>, remote: Option<RemoteConfig>) -> BackupConfig {
    BackupConfig {
        enabled: true,
        schedule: "0 3 * * *".into(),
        dir: dir.to_path_buf(),
        keep_last: 2,
        free_floor_bytes: 0,
        encrypt_to: recipients,
        allow_plaintext_remote: false,
        remote,
    }
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

async fn run(m: &Arc<BackupManager>) -> rt_store::BackupRow {
    let row = m.start("api").await.expect("start");
    for _ in 0..1500 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let r = m.get(&row.id).await.expect("get").expect("row");
        if matches!(r.state.as_str(), "ok" | "failed") {
            return r;
        }
    }
    panic!("the backup did not finish");
}

fn opts<'a>(keys: &'a [MasterKey]) -> RestoreOptions<'a> {
    RestoreOptions {
        auth: Auth::Keys(keys),
        revoke_keys_before: None,
        secrets: None,
        now_ms: 1_000_000,
    }
}

fn encrypt_to(recipient: &str, plain: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let r: age::x25519::Recipient = recipient.parse().expect("recipient");
    let enc = age::Encryptor::with_recipients(std::iter::once(&r as &dyn age::Recipient)).expect("encryptor");
    let mut out = Vec::new();
    let mut w = enc.wrap_output(&mut out).expect("wrap");
    w.write_all(plain).expect("write");
    w.finish().expect("finish");
    out
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(d) else { continue };
        for e in rd.flatten() {
            if e.path().is_dir() {
                stack.push(e.path());
            } else {
                out.push(e.path());
            }
        }
    }
    out
}

/// A good encrypted backup, on `out`.
fn encrypted_backup(n: &Node, out: &Path, recipient: &str) -> PathBuf {
    let tmp = out.join("scratch");
    std::fs::create_dir_all(&tmp).expect("scratch");
    let r: age::x25519::Recipient = recipient.parse().expect("recipient");
    let id = format!("bkp_{}", ulid::Ulid::new());
    rt_backup::create::create_archive_to(
        &n.sources(),
        out,
        &tmp,
        &id,
        1,
        0,
        &AtomicBool::new(false),
        "test",
        &[r],
    )
    .expect("archive")
    .path
}

#[tokio::test(flavor = "multi_thread")]
async fn an_encrypted_backup_goes_to_s3_and_restores_with_the_identity() {
    let n = Node::seeded().await;
    let k = keys();
    let s3 = MockS3::start().await;
    let out = tempfile::tempdir().expect("out");
    let m = manager(&n, config(out.path(), vec![k.recipient.clone()], Some(remote(&s3.url))));
    let row = run(&m).await;
    assert_eq!(row.state, "ok", "{:?}", row.failure);
    assert!(row.encrypted);

    let base = format!("node1/rt-backup-{}.tar.age", row.id);
    assert_eq!(row.remote_key.as_deref(), Some(base.as_str()));
    assert_eq!(
        s3.keys(),
        vec![base.clone(), format!("{base}.complete"), format!("{base}.sha256")]
    );
    let sidecar = String::from_utf8(s3.object(&format!("{base}.sha256")).expect("sidecar")).expect("utf8");
    assert!(sidecar.starts_with(row.sha256.as_deref().expect("sha")), "{sidecar}");

    // 8 MB parts in production, 1 KiB here; never more than two at once, and every request signed.
    {
        let i = s3.state.inner.lock();
        assert!(i.part_sizes.iter().all(|s| *s <= 1024));
        assert!(i.part_sizes.len() > 1, "{:?}", i.part_sizes);
        assert!(i
            .auth_headers
            .iter()
            .all(|a| a.starts_with("AWS4-HMAC-SHA256 Credential=access-id/")));
        assert!(!i.auth_headers.iter().any(|a| a.contains(SECRET_KEY)));
    }
    assert!(s3.state.max_in_flight.load(Ordering::SeqCst) <= 2);
    assert!(s3.state.max_in_flight.load(Ordering::SeqCst) >= 1);

    // What the bucket holds restores, with the identity only.
    let got = tempfile::NamedTempFile::new().expect("file");
    std::fs::write(got.path(), s3.object(&base).expect("object")).expect("write");
    let identity = AgeIdentity::from_file(k.file.path()).expect("identity");
    let keys_ = n.keys();
    let report = verify_archive_age(got.path(), &Auth::Keys(&keys_), Some(&identity)).expect("verify");
    assert_eq!(report.id, row.id);
    let to = tempfile::tempdir().expect("to");
    let r = restore_archive_age(got.path(), to.path(), &opts(&keys_), Some(&identity)).expect("restore");
    assert_eq!((r.services, r.blobs, r.secrets), (1, 1, 1));
    assert!(to.path().join("meta.db").exists());

    let status = m.status().await.expect("status");
    assert_eq!(status.remote_bucket.as_deref(), Some(BUCKET));
    assert!(status.remote_host.is_some());
}

/// `age` recipients are public, so a party who can write to the bucket can encrypt anything to
/// them, with valid sidecars. Only `manifest.mac` tells.
#[tokio::test(flavor = "multi_thread")]
async fn bucket_write_access_cannot_forge_an_archive() {
    let n = Node::seeded().await;
    let k = keys();
    let out = tempfile::tempdir().expect("out");
    let plain = n.backup_to(out.path());

    // Same content, changed manifest, and a MAC under a key the attacker does have: their own.
    let mut entries = read_entries(&plain);
    let i = entries.iter().position(|e| e.path == MANIFEST).expect("manifest");
    entries[i].body = String::from_utf8(entries[i].body.clone())
        .expect("utf8")
        .replace("per_database", "per_database ")
        .into_bytes();
    let attacker = MasterKey::generate();
    entries[i + 1].body = sign_manifest(&attacker, &entries[i].body).into_bytes();
    let forged_tar = out.path().join("forged.tar");
    write_entries(&forged_tar, &entries);
    let forged_age = out.path().join("forged.tar.age");
    std::fs::write(
        &forged_age,
        encrypt_to(&k.recipient, &std::fs::read(&forged_tar).expect("read")),
    )
    .expect("write");

    let identity = AgeIdentity::from_file(k.file.path()).expect("identity");
    let keys_ = n.keys();
    assert_eq!(
        code(verify_archive_age(&forged_age, &Auth::Keys(&keys_), Some(&identity))),
        "bad_mac"
    );
    let to = tempfile::tempdir().expect("to");
    assert_eq!(
        code(restore_archive_age(
            &forged_age,
            to.path(),
            &opts(&keys_),
            Some(&identity)
        )),
        "bad_mac"
    );
    assert!(is_empty(to.path()), "nothing of a forged archive stays");

    // A manifest with no MAC at all does not pass either.
    entries.truncate(i + 1);
    let unsigned = out.path().join("unsigned.tar");
    write_entries(&unsigned, &entries);
    let unsigned_age = out.path().join("unsigned.tar.age");
    std::fs::write(
        &unsigned_age,
        encrypt_to(&k.recipient, &std::fs::read(&unsigned).expect("read")),
    )
    .expect("write");
    assert_eq!(
        code(verify_archive_age(&unsigned_age, &Auth::Keys(&keys_), Some(&identity))),
        "truncated"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_right_identity_opens_an_archive() {
    let n = Node::seeded().await;
    let (right, wrong) = (keys(), keys());
    let out = tempfile::tempdir().expect("out");
    let archive = encrypted_backup(&n, out.path(), &right.recipient);
    assert!(archive.to_string_lossy().ends_with(".tar.age"));
    let keys_ = n.keys();

    let bad = AgeIdentity::from_file(wrong.file.path()).expect("identity");
    let to = tempfile::tempdir().expect("to");
    assert_eq!(
        code(restore_archive_age(&archive, to.path(), &opts(&keys_), Some(&bad))),
        "wrong_identity"
    );
    assert!(is_empty(to.path()));
    assert_eq!(
        code(restore_archive_age(&archive, to.path(), &opts(&keys_), None)),
        "age_identity_required"
    );
    assert_eq!(code(n.verify(&archive)), "age_identity_required");

    let good = AgeIdentity::from_file(right.file.path()).expect("identity");
    // A plain archive where an encrypted one was expected is a substitution, or a mistake.
    let plain = n.backup_to(out.path());
    assert_eq!(
        code(verify_archive_age(&plain, &Auth::Keys(&keys_), Some(&good))),
        "not_encrypted"
    );

    // A changed or cut ciphertext is refused, and leaves nothing.
    let bytes = std::fs::read(&archive).expect("read");
    let mut flipped = bytes.clone();
    let at = flipped.len() / 2;
    flipped[at] ^= 0x40;
    let f = out.path().join("flipped.tar.age");
    std::fs::write(&f, &flipped).expect("write");
    assert_eq!(
        code(restore_archive_age(&f, to.path(), &opts(&keys_), Some(&good))),
        "corrupt"
    );
    assert!(is_empty(to.path()));
    let c = out.path().join("cut.tar.age");
    std::fs::write(&c, &bytes[..bytes.len() - 40]).expect("write");
    // Inside a chunk a cut and a change look alike to AEAD.
    let cut = code(restore_archive_age(&c, to.path(), &opts(&keys_), Some(&good)));
    assert!(matches!(cut, "truncated" | "corrupt"), "{cut}");
    assert!(is_empty(to.path()));

    let junk = tempfile::NamedTempFile::new().expect("file");
    std::fs::write(junk.path(), "not a key").expect("write");
    assert_eq!(code(AgeIdentity::from_file(junk.path()).map(|_| ())), "bad_identity");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_plaintext_archive_is_ever_on_disk_with_recipients() {
    let n = Node::seeded().await;
    let k = keys();
    let out = tempfile::tempdir().expect("out");
    let dir = out.path().to_path_buf();

    // Control: the marker is what a plaintext archive would show.
    let control = tempfile::tempdir().expect("control");
    let plain = n.backup_to(control.path());
    assert!(contains(&std::fs::read(&plain).expect("read"), BLOB_MARKER));
    assert!(contains(&std::fs::read(&plain).expect("read"), b"ustar"));

    // Watch the directory throughout the run, including `.partial`.
    let stop = Arc::new(AtomicBool::new(false));
    let watcher = {
        let (stop, dir) = (stop.clone(), dir.clone());
        std::thread::spawn(move || {
            let mut partials = 0u32;
            while !stop.load(Ordering::SeqCst) {
                for f in files_under(&dir) {
                    let Ok(b) = std::fs::read(&f) else { continue };
                    let name = f.to_string_lossy().into_owned();
                    assert!(!contains(&b, BLOB_MARKER), "plaintext blob in {name}");
                    if name.ends_with(".partial") {
                        partials += 1;
                        assert!(!contains(&b, b"ustar"), "a tar header in {name}");
                    }
                }
            }
            partials
        })
    };
    for _ in 0..3 {
        encrypted_backup(&n, &dir, &k.recipient);
    }
    stop.store(true, Ordering::SeqCst);
    watcher.join().expect("no plaintext seen");

    let finals: Vec<_> = files_under(&dir)
        .into_iter()
        .filter(|f| !f.to_string_lossy().contains("scratch"))
        .collect();
    assert_eq!(finals.len(), 3);
    for f in finals {
        let b = std::fs::read(&f).expect("read");
        assert!(f.to_string_lossy().ends_with(".tar.age"));
        assert!(b.starts_with(b"age-encryption.org/v1"));
        assert!(!contains(&b, BLOB_MARKER));
        assert!(!contains(&b, b"ustar"));
    }
    assert!(is_empty(&dir.join("scratch")), "scratch is emptied as it goes");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partial_upload_is_never_marked_complete() {
    let n = Node::seeded().await;
    let k = keys();
    let s3 = MockS3::start().await;
    let out = tempfile::tempdir().expect("out");
    let m = manager(&n, config(out.path(), vec![k.recipient.clone()], Some(remote(&s3.url))));

    s3.state.inner.lock().fail_every_part = true;
    let row = run(&m).await;
    assert_eq!(row.state, "failed");
    assert_eq!(row.failure.as_ref().expect("failure")["code"], "upload_failed");
    assert!(s3.keys().is_empty(), "{:?}", s3.keys());
    assert_eq!(s3.state.inner.lock().aborted, 1, "the multipart upload is aborted");
    let local = m.archive_path(&row.id).expect("path");
    assert!(local.exists(), "a failed upload leaves the local copy");
    assert!(m.status().await.expect("status").last_ok_id.is_none());

    // The object arrives and the marker does not: still not complete, and no `ok`.
    {
        let mut i = s3.state.inner.lock();
        i.fail_every_part = false;
        i.refuse_suffix = Some(".complete".into());
        i.error_body = format!("<Error>{SECRET_KEY}</Error>");
    }
    let row2 = run(&m).await;
    assert_eq!(row2.state, "failed");
    assert!(!s3.keys().iter().any(|k| k.ends_with(".complete")), "{:?}", s3.keys());
    let shown = row2.failure.as_ref().expect("failure").to_string();
    assert!(!shown.contains(SECRET_KEY), "{shown}");
    assert!(!shown.contains(&s3.url), "{shown}");

    // One failed part is retried, and the run succeeds; the leftovers then go.
    {
        let mut i = s3.state.inner.lock();
        i.refuse_suffix = None;
        i.fail_part_once = Some(1);
    }
    let attempts_before = s3.state.inner.lock().part_attempts;
    let row3 = run(&m).await;
    assert_eq!(row3.state, "ok", "{:?}", row3.failure);
    assert!(s3.state.inner.lock().part_attempts > attempts_before + 1);
    assert!(!local.exists(), "a good run removes the local copies of failed ones");
    assert!(m.archive_path(&row3.id).expect("path").exists(), "keep_local = 1");
}

fn seed_remote(s3: &MockS3, age_ms: u64, complete: bool) -> String {
    let id = format!("bkp_{}", ulid::Ulid::from_parts(now() - age_ms, 7));
    let base = format!("node1/rt-backup-{id}.tar.age");
    s3.put(&base, b"x");
    s3.put(&format!("{base}.sha256"), b"x");
    if complete {
        s3.put(&format!("{base}.complete"), b"x");
    }
    id
}

fn now() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}

/// This node's record of a backup in the bucket.
async fn own_row(n: &Node, id: &str) {
    n.meta
        .backup_insert(rt_store::BackupRow {
            id: id.into(),
            state: "ok".into(),
            trigger: "api".into(),
            started_at: 0,
            finished_at: Some(0),
            bytes: None,
            sha256: None,
            encrypted: true,
            local_path: None,
            remote_key: Some(format!("node1/rt-backup-{id}.tar.age")),
            verified_at: None,
            rt_version: "test".into(),
            failure: None,
        })
        .await
        .expect("row");
}

fn one_a_day() -> RetentionPolicy {
    RetentionPolicy {
        keep_last: 1,
        keep_daily: 0,
        keep_weekly: 0,
    }
}

/// M7: two nodes on one bucket and prefix: an object this node has no row for is another's.
#[tokio::test(flavor = "multi_thread")]
async fn retention_leaves_the_backups_of_another_node_alone() {
    let n = Node::seeded().await;
    let k = keys();
    let s3 = MockS3::start().await;
    let out = tempfile::tempdir().expect("out");
    let foreign_old = seed_remote(&s3, 30 * DAY, true);
    let foreign_stale = seed_remote(&s3, 20 * DAY, false);
    let mine_old = seed_remote(&s3, 9 * DAY, true);
    let mine_new = seed_remote(&s3, DAY, true);
    own_row(&n, &mine_old).await;
    own_row(&n, &mine_new).await;

    let mut r = remote(&s3.url);
    r.retention = one_a_day();
    let m = manager(&n, config(out.path(), vec![k.recipient.clone()], Some(r)));
    m.apply_remote_retention().await.expect("retention");

    let keys_now = s3.keys();
    let has = |id: &str| keys_now.iter().any(|k| k.contains(id));
    assert!(has(&foreign_old) && has(&foreign_stale), "{keys_now:?}");
    assert!(has(&mine_new), "{keys_now:?}");
    assert!(!has(&mine_old), "{keys_now:?}");
}

/// L7: `.tar` and `.tar.age` of one id are both the backup; neither variant escapes the prune.
#[tokio::test(flavor = "multi_thread")]
async fn retention_prunes_both_variants_of_an_id() {
    let n = Node::seeded().await;
    let k = keys();
    let s3 = MockS3::start().await;
    let out = tempfile::tempdir().expect("out");
    let old = seed_remote(&s3, 9 * DAY, true);
    let plain = format!("node1/rt-backup-{old}.tar");
    s3.put(&plain, b"x");
    s3.put(&format!("{plain}.complete"), b"x");
    let new = seed_remote(&s3, DAY, true);
    own_row(&n, &old).await;
    own_row(&n, &new).await;

    let mut r = remote(&s3.url);
    r.retention = one_a_day();
    let m = manager(&n, config(out.path(), vec![k.recipient.clone()], Some(r)));
    m.apply_remote_retention().await.expect("retention");
    let keys_now = s3.keys();
    assert!(!keys_now.iter().any(|k| k.contains(&old)), "{keys_now:?}");
}

/// L6: a row's `encrypted` flag does not decide which object a delete removes.
#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_backup_removes_its_objects_whatever_the_row_says() {
    let n = Node::seeded().await;
    let k = keys();
    let s3 = MockS3::start().await;
    let out = tempfile::tempdir().expect("out");
    let id = seed_remote(&s3, DAY, true);
    n.meta
        .backup_insert(rt_store::BackupRow {
            id: id.clone(),
            state: "ok".into(),
            trigger: "api".into(),
            started_at: 0,
            finished_at: Some(0),
            bytes: None,
            sha256: None,
            encrypted: false, // flipped: the object is `.tar.age`
            local_path: None,
            remote_key: Some("x".into()),
            verified_at: None,
            rt_version: "test".into(),
            failure: None,
        })
        .await
        .expect("row");
    n.meta.backup_set_remote(&id, "x".into()).await.expect("remote");
    let m = manager(&n, config(out.path(), vec![k.recipient.clone()], Some(remote(&s3.url))));
    assert!(m.delete(&id).await.expect("delete"));
    let keys_now = s3.keys();
    assert!(!keys_now.iter().any(|k| k.contains(&id)), "{keys_now:?}");
}

const DAY: u64 = 86_400_000;

#[tokio::test(flavor = "multi_thread")]
async fn retention_never_deletes_the_newest_good_and_counts_only_complete_objects() {
    let n = Node::seeded().await;
    let k = keys();
    let s3 = MockS3::start().await;
    let out = tempfile::tempdir().expect("out");

    let oldest = seed_remote(&s3, 30 * DAY, true);
    let newest_good = seed_remote(&s3, 3 * DAY, true);
    // Newer than the newest good, with no marker: neither counted nor, while fresh, deleted.
    let unfinished = seed_remote(&s3, 60_000, false);
    let stale = seed_remote(&s3, 9 * DAY, false);
    for id in [&oldest, &newest_good, &unfinished, &stale] {
        own_row(&n, id).await;
    }
    s3.put("node1/other-file.tar", b"not ours");
    s3.put("elsewhere/rt-backup-x", b"not ours");

    let mut r = remote(&s3.url);
    r.retention = RetentionPolicy {
        keep_last: 1,
        keep_daily: 0,
        keep_weekly: 0,
    };
    let m = manager(&n, config(out.path(), vec![k.recipient.clone()], Some(r)));
    m.apply_remote_retention().await.expect("retention");

    let keys_now = s3.keys();
    let has = |id: &str| keys_now.iter().any(|k| k.contains(id));
    assert!(has(&newest_good), "{keys_now:?}");
    assert!(keys_now
        .iter()
        .any(|k| k.contains(&newest_good) && k.ends_with(".complete")));
    assert!(!has(&oldest), "{keys_now:?}");
    assert!(has(&unfinished), "an upload may be running: {keys_now:?}");
    assert!(!has(&stale), "{keys_now:?}");
    assert!(keys_now.contains(&"node1/other-file.tar".to_string()));
    assert!(keys_now.contains(&"elsewhere/rt-backup-x".to_string()));
}

#[test]
fn the_policy_keeps_the_last_daily_and_weekly_and_always_the_newest() {
    use rt_backup::remote::keep_set;
    let t = 1_700_000_000_000i64;
    let d = DAY as i64;
    let items: Vec<(String, i64)> = (0..40).map(|i| (format!("b{i}"), t - i * d)).collect();
    let k = keep_set(
        &items,
        &RetentionPolicy {
            keep_last: 2,
            keep_daily: 3,
            keep_weekly: 3,
        },
    );
    // t is a Tuesday: b0 and b1 are this ISO week, b2 opens the last one, b9 the one before.
    let mut got: Vec<&str> = k.iter().map(String::as_str).collect();
    got.sort_unstable();
    assert_eq!(got, ["b0", "b1", "b2", "b9"]);

    let none = RetentionPolicy {
        keep_last: 0,
        keep_daily: 0,
        keep_weekly: 0,
    };
    let k = keep_set(&items, &none);
    assert_eq!(k.len(), 1, "the newest survives any policy");
    assert!(k.contains("b0"));
    assert!(keep_set(&[], &none).is_empty());
}

/// Decision D2: S3 needs recipients, unless the operator says otherwise out loud.
#[tokio::test(flavor = "multi_thread")]
async fn s3_without_recipients_is_refused_unless_explicitly_allowed() {
    let n = Node::seeded().await;
    let s3 = MockS3::start().await;
    let out = tempfile::tempdir().expect("out");
    let refused = BackupManager::new(
        config(out.path(), Vec::new(), Some(remote(&s3.url))),
        n.sources(),
        n.meta.clone(),
        None,
        tokio::runtime::Handle::current(),
    );
    assert_eq!(code(refused.map(|_| ())), "plaintext_remote");

    let mut allowed = config(out.path(), Vec::new(), Some(remote(&s3.url)));
    allowed.allow_plaintext_remote = true;
    let m = manager(&n, allowed);
    let row = run(&m).await;
    assert_eq!(row.state, "ok", "{:?}", row.failure);
    assert!(!row.encrypted);
    assert!(s3.keys().iter().any(|k| k.ends_with(".tar.complete")));

    let bad = config(out.path(), vec!["age1notakey".into()], None);
    let refused = BackupManager::new(
        bad,
        n.sources(),
        n.meta.clone(),
        None,
        tokio::runtime::Handle::current(),
    );
    assert_eq!(code(refused.map(|_| ())), "bad_recipient");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_endpoint_passes_the_egress_rules_before_any_connection() {
    use rt_backup::s3::S3Client;
    for (url, why) in [
        ("http://10.0.0.1:9000", "private"),
        ("https://10.0.0.1", "private over TLS too"),
        ("https://169.254.169.254", "metadata service"),
        ("http://203.0.113.9", "plain http to a non-loopback address"),
        ("https://user:pw@203.0.113.9", "credentials in the URL"),
        ("ftp://203.0.113.9", "scheme"),
        ("https://203.0.113.9/path", "a path"),
    ] {
        let c = s3_config(url);
        let client = S3Client::new(c);
        let refused = match client {
            Err(e) => e.code,
            Ok(c) => c.session().await.err().map_or("none", |e| e.code),
        };
        assert_eq!(refused, "endpoint_refused", "{url}: {why}");
    }
    // Denied ports and hosts bind it as they bind guests.
    let mut c = s3_config("https://203.0.113.9:465");
    c.denied_ports = vec![465];
    let e = S3Client::new(c)
        .expect("client")
        .session()
        .await
        .err()
        .expect("refused");
    assert_eq!(e.code, "endpoint_refused");
    let mut c = s3_config("https://203.0.113.9");
    c.denied_hosts = vec!["203.0.113.9".into()];
    let e = S3Client::new(c)
        .expect("client")
        .session()
        .await
        .err()
        .expect("refused");
    assert_eq!(e.code, "endpoint_refused");
}

#[test]
fn credentials_do_not_print() {
    let c = S3Credentials::new("access-id", SECRET_KEY);
    let shown = format!("{c:?}");
    assert!(!shown.contains(SECRET_KEY) && !shown.contains("access-id"), "{shown}");
    let shown = format!("{:?}", s3_config("http://127.0.0.1:1"));
    assert!(!shown.contains(SECRET_KEY), "{shown}");
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_backup_removes_its_objects() {
    let n = Node::seeded().await;
    let k = keys();
    let s3 = MockS3::start().await;
    let out = tempfile::tempdir().expect("out");
    let m = manager(&n, config(out.path(), vec![k.recipient.clone()], Some(remote(&s3.url))));
    let row = run(&m).await;
    assert_eq!(row.state, "ok", "{:?}", row.failure);
    assert_eq!(s3.keys().len(), 3);
    assert!(m.delete(&row.id).await.expect("delete"));
    assert!(s3.keys().is_empty(), "{:?}", s3.keys());
    assert!(!m.archive_path(&row.id).expect("path").exists());
    let _ = &k.identity;
}
