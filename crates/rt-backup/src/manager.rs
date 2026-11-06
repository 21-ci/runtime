//! One backup at a time for the node: its row, its own thread, retention, schedule and status.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use age::x25519::Recipient;
use parking_lot::Mutex;
use rt_obs::Metrics;
use rt_store::{BackupRow, Page, PageReq, SqliteMetaStore};

use crate::archive::{archive_name, encrypted_archive_name, HashReader};
use crate::create::{create_archive_to, private_dir, Sources, VERIFY_SAMPLE};
use crate::encrypt::parse_recipients;
use crate::remote::{apply_retention, delete_objects, upload_archive, RemoteConfig, RetentionPolicy};
use crate::restore::{verify_archive, Auth};
use crate::s3::S3Client;
use crate::{BackupError, Result};

/// While the memory governor reports pressure, a due scheduled backup waits this long.
pub const PRESSURE_RETRY_MS: i64 = 10 * 60 * 1000;
const WHOLE_NODE_MARGIN_PERCENT: u64 = 120;

/// `[backup]`, resolved.
#[derive(Clone, Debug)]
pub struct BackupConfig {
    pub enabled: bool,
    pub schedule: String,
    pub dir: PathBuf,
    pub keep_last: u32,
    /// Free disk a backup leaves, beyond what it is estimated to need.
    pub free_floor_bytes: u64,
    /// `age` public keys. With any, no plaintext archive is ever written, and the node cannot read
    /// its own archives.
    pub encrypt_to: Vec<String>,
    /// Decision D2: S3 needs recipients unless this says otherwise.
    pub allow_plaintext_remote: bool,
    pub remote: Option<RemoteConfig>,
}

struct Remote {
    client: S3Client,
    keep_local: u32,
    retention: RetentionPolicy,
}

/// A cron expression of 5 fields (or the crate's own 6 or 7).
pub fn parse_schedule(expr: &str) -> Result<cron::Schedule> {
    let normalized = if expr.split_whitespace().count() == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    };
    cron::Schedule::from_str(&normalized)
        .map_err(|_| BackupError::new("bad_schedule", "backup.schedule is not a cron expression"))
}

/// `bkp_<ulid>` only. The archive's path is built from the id, never read from a row: a restored
/// `meta.db` describes other paths, and a forged one could name any file.
pub fn id_ok(id: &str) -> bool {
    id.strip_prefix("bkp_")
        .is_some_and(|u| u.len() == 26 && u.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()))
}

/// `GET /v1/node/backups/status`, before the API's shape.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BackupStatus {
    pub enabled: bool,
    pub schedule: String,
    pub last_ok_at: Option<i64>,
    pub last_ok_id: Option<String>,
    pub last_failure: Option<serde_json::Value>,
    pub next_at: Option<i64>,
    pub running_id: Option<String>,
    pub last_restored_at: Option<i64>,
    pub remote_host: Option<String>,
    pub remote_bucket: Option<String>,
}

pub struct BackupManager {
    cfg: BackupConfig,
    recipients: Vec<Recipient>,
    remote: Option<Remote>,
    schedule: Option<cron::Schedule>,
    sources: Sources,
    meta: Arc<SqliteMetaStore>,
    metrics: Option<Arc<Metrics>>,
    runtime: tokio::runtime::Handle,
    running: Arc<Mutex<Option<String>>>,
    cancel: Arc<AtomicBool>,
    next_at: Mutex<Option<i64>>,
    runs: AtomicUsize,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Clears the running slot however the thread ends.
struct RunGuard(Arc<Mutex<Option<String>>>);

impl Drop for RunGuard {
    fn drop(&mut self) {
        *self.0.lock() = None;
    }
}

fn file_len(p: &Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

/// `(largest database, all databases, all blobs and artifacts)`: stat calls only.
fn node_sizes(data_dir: &Path) -> (u64, u64, u64) {
    let (mut largest, mut dbs, mut files) = (0u64, 0u64, 0u64);
    let mut add_db = |p: PathBuf| {
        let mut wal = p.as_os_str().to_os_string();
        wal.push("-wal");
        let n = file_len(&p).saturating_add(file_len(Path::new(&wal)));
        largest = largest.max(n);
        dbs = dbs.saturating_add(n);
    };
    for db in ["meta.db", "queue.db", "streams.db"] {
        add_db(data_dir.join(db));
    }
    if let Ok(rd) = std::fs::read_dir(data_dir.join("services")) {
        for e in rd.flatten() {
            add_db(e.path().join("data.db"));
        }
    }
    for root in ["blobs", "artifacts"] {
        let mut stack = vec![data_dir.join(root)];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(dir) else { continue };
            for e in rd.flatten() {
                match e.file_type() {
                    Ok(t) if t.is_dir() => stack.push(e.path()),
                    Ok(t) if t.is_file() => {
                        files = files.saturating_add(e.metadata().map(|m| m.len()).unwrap_or(0));
                    }
                    _ => {}
                }
            }
        }
    }
    (largest, dbs, files)
}

impl BackupManager {
    /// `schedule` must parse when the schedule is on: bad configuration stops startup.
    pub fn new(
        cfg: BackupConfig,
        sources: Sources,
        meta: Arc<SqliteMetaStore>,
        metrics: Option<Arc<Metrics>>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Arc<Self>> {
        let schedule = if cfg.enabled {
            Some(parse_schedule(&cfg.schedule)?)
        } else {
            None
        };
        let recipients = parse_recipients(&cfg.encrypt_to)?;
        let remote = match &cfg.remote {
            None => None,
            Some(r) => {
                if recipients.is_empty() && !cfg.allow_plaintext_remote {
                    return Err(BackupError::new(
                        "plaintext_remote",
                        "backup.s3 needs backup.encrypt_to, or backup.allow_plaintext_remote = true",
                    ));
                }
                Some(Remote {
                    client: S3Client::new(r.s3.clone())?,
                    keep_local: r.keep_local,
                    retention: r.retention,
                })
            }
        };
        Ok(Arc::new(Self {
            cfg,
            recipients,
            remote,
            schedule,
            sources,
            meta,
            metrics,
            runtime,
            running: Arc::new(Mutex::new(None)),
            cancel: Arc::new(AtomicBool::new(false)),
            next_at: Mutex::new(None),
            runs: AtomicUsize::new(0),
        }))
    }

    pub fn dir(&self) -> &Path {
        &self.cfg.dir
    }

    /// The archive of `id`: built from the id, inside the backup directory. With recipients set
    /// only the encrypted name is ever answered: `/archive` serves ciphertext or nothing.
    pub fn archive_path(&self, id: &str) -> Option<PathBuf> {
        if !id_ok(id) {
            return None;
        }
        let sealed = self.cfg.dir.join(encrypted_archive_name(id));
        if !self.recipients.is_empty() || sealed.exists() {
            return Some(sealed);
        }
        Some(self.cfg.dir.join(archive_name(id)))
    }

    /// Every local file of `id`, whichever way it was written.
    fn remove_local(&self, id: &str) {
        if !id_ok(id) {
            return;
        }
        for name in [archive_name(id), encrypted_archive_name(id)] {
            let path = self.cfg.dir.join(&name);
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(self.cfg.dir.join(format!("{name}.partial")));
        }
    }

    /// What a killed process left: its scratch, partial archives and `running` rows.
    pub async fn startup_sweep(&self) -> Result<u64> {
        if let Ok(rd) = std::fs::read_dir(&self.cfg.dir) {
            for e in rd.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("tmp-") {
                    let _ = std::fs::remove_dir_all(e.path());
                } else if name.ends_with(".partial") {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
        Ok(self.meta.backup_fail_unfinished(now_ms()).await?)
    }

    /// Stop the running backup at its next file, for shutdown.
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }

    /// Free disk >= largest database + sampled databases + 1.2 x the last archive (on a first run
    /// the sum of databases and files stands in for it).
    fn preflight(&self, last_bytes: Option<u64>) -> Result<()> {
        private_dir(&self.cfg.dir)?;
        let (largest, dbs, files) = node_sizes(&self.sources.data_dir);
        let archive = last_bytes.unwrap_or_else(|| dbs.saturating_add(files));
        let needed = largest
            .saturating_mul(2)
            .saturating_add(archive.saturating_mul(WHOLE_NODE_MARGIN_PERCENT) / 100)
            .saturating_add(self.cfg.free_floor_bytes);
        match rt_store::free_bytes(&self.cfg.dir) {
            Some(free) if needed > free => Err(BackupError::new(
                "insufficient_storage",
                "not enough free disk for a backup",
            )),
            _ => Ok(()),
        }
    }

    /// Start a backup and return its row while it runs on its own thread. `backup_disabled`,
    /// `backup_running`, `insufficient_storage`.
    pub async fn start(self: &Arc<Self>, trigger: &'static str) -> Result<BackupRow> {
        if !self.cfg.enabled {
            return Err(BackupError::new(
                "backup_disabled",
                "backups are off: set backup.enabled",
            ));
        }
        let id = format!("bkp_{}", ulid::Ulid::new());
        {
            let mut running = self.running.lock();
            if running.is_some() {
                return Err(BackupError::new("backup_running", "a backup is already running"));
            }
            *running = Some(id.clone());
        }
        let guard = RunGuard(self.running.clone());
        // Retention runs before the new backup, so that its disk is there to be used. The newest
        // good archive stays until this run has replaced it, whatever `keep_last` says.
        let keep = match &self.remote {
            // The newest good archive is safe in the bucket, whatever happens to the disk.
            Some(r) => r.keep_local.saturating_sub(1),
            None => self.cfg.keep_last.saturating_sub(1).max(1),
        };
        self.apply_retention(keep).await?;
        self.sweep_failed_uploads(true).await?;
        if let Err(e) = self.apply_remote_retention().await {
            tracing::warn!(code = e.code, "remote backup retention failed: {}", e.message);
        }
        let last = self.meta.backup_last_ok().await?.and_then(|b| b.bytes);
        let me = self.clone();
        tokio::task::spawn_blocking(move || me.preflight(last))
            .await
            .map_err(|_| BackupError::new("internal", "preflight did not finish"))??;
        let started_at = now_ms();
        let row = BackupRow {
            id: id.clone(),
            state: "running".into(),
            trigger: trigger.into(),
            started_at,
            finished_at: None,
            bytes: None,
            sha256: None,
            encrypted: !self.recipients.is_empty(),
            local_path: None,
            remote_key: None,
            verified_at: None,
            rt_version: env!("CARGO_PKG_VERSION").into(),
            failure: None,
        };
        self.meta.backup_insert(row.clone()).await?;
        let me = self.clone();
        std::thread::Builder::new()
            .name("rt-backup".into())
            .spawn(move || {
                let _guard = guard;
                rt_store::lower_this_thread();
                me.run(&id, started_at);
            })
            .map_err(BackupError::from)?;
        Ok(row)
    }

    fn run(&self, id: &str, started_at: i64) {
        let t0 = Instant::now();
        let scratch = self.cfg.dir.join(format!("tmp-{id}"));
        let mut keep_local = false;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.work(id, started_at, &scratch, &mut keep_local)
        }))
        .unwrap_or_else(|_| Err(BackupError::new("internal", "the backup thread panicked")));
        let _ = std::fs::remove_dir_all(&scratch);
        let end = now_ms();
        match result {
            Ok((path, bytes, sha)) => {
                let path = path.to_string_lossy().into_owned();
                if let Err(e) = self
                    .runtime
                    .block_on(self.meta.backup_mark_ok(id, end, bytes, sha, path))
                {
                    tracing::error!(error = %e, "could not record a finished backup");
                }
                if let Some(m) = &self.metrics {
                    m.backup_last_success_timestamp.set(end / 1000);
                    m.backup_duration_seconds.set(t0.elapsed().as_secs() as i64);
                }
                let keep = self.remote.as_ref().map_or(self.cfg.keep_last, |r| r.keep_local);
                let after = self.runtime.block_on(async {
                    self.sweep_failed_uploads(false).await?;
                    self.apply_retention(keep).await?;
                    self.apply_remote_retention().await
                });
                if let Err(e) = after {
                    tracing::warn!(code = e.code, "backup retention failed: {}", e.message);
                }
                tracing::info!(backup = id, bytes, "backup finished");
            }
            Err(e) => {
                if !keep_local {
                    self.remove_local(id);
                }
                tracing::warn!(backup = id, code = e.code, "backup failed: {}", e.message);
                if let Err(db) = self
                    .runtime
                    .block_on(self.meta.backup_mark_failed(id, end, e.to_json()))
                {
                    tracing::error!(error = %db, "could not record a failed backup");
                }
            }
        }
    }

    fn work(&self, id: &str, started_at: i64, scratch: &Path, keep_local: &mut bool) -> Result<(PathBuf, u64, String)> {
        private_dir(scratch)?;
        let offset = self.runs.fetch_add(1, Ordering::Relaxed).wrapping_mul(VERIFY_SAMPLE);
        let created = create_archive_to(
            &self.sources,
            &self.cfg.dir,
            scratch,
            id,
            started_at,
            offset,
            &self.cancel,
            env!("CARGO_PKG_VERSION"),
            &self.recipients,
        )?;
        if let Some(m) = &self.metrics {
            m.backup_peak_wal_bytes
                .set(created.peak_wal_bytes.min(i64::MAX as u64) as i64);
        }
        self.runtime.block_on(self.meta.backup_set_state(id, "verifying"))?;
        if self.recipients.is_empty() {
            verify_archive(&created.path, &Auth::Keys(std::slice::from_ref(&self.sources.master)))?;
        } else {
            // The node cannot read what it wrote: the snapshots were checked before packaging, and
            // the file must still be the bytes that were written.
            let mut hashed = HashReader::new(std::fs::File::open(&created.path)?);
            std::io::copy(&mut hashed, &mut std::io::sink())?;
            if hashed.finish().1 != created.sha256 {
                return Err(BackupError::new(
                    "digest_mismatch",
                    "the archive on disk is not what was written",
                ));
            }
        }
        if let Some(remote) = &self.remote {
            self.runtime.block_on(self.meta.backup_set_state(id, "uploading"))?;
            let name = created
                .path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let key = remote.client.key(&name);
            let uploaded = self.runtime.block_on(async {
                let upload = async {
                    let session = remote.client.session().await?;
                    upload_archive(&session, &key, &created.path, &created.sha256).await
                };
                tokio::select! {
                    done = upload => done,
                    () = self.cancelled() => Err(BackupError::new("cancelled", "the node is shutting down")),
                }
            });
            if let Err(e) = uploaded {
                *keep_local = true;
                return Err(BackupError::new(
                    "upload_failed",
                    format!("{} ({}); the archive stays on the local disk", e.message, e.code),
                ));
            }
            self.runtime.block_on(self.meta.backup_set_remote(id, key))?;
        }
        Ok((created.path, created.bytes, created.sha256))
    }

    async fn cancelled(&self) {
        while !self.cancel.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    /// Past the newest `keep`, delete local archives. A backup that is also in the bucket keeps its
    /// row: the remote retention decides that one.
    async fn apply_retention(&self, keep: u32) -> Result<()> {
        for old in self.meta.backup_old_ok(keep).await? {
            self.remove_local(&old.id);
            if old.remote_key.is_some() && self.remote.is_some() {
                self.meta.backup_clear_local(&old.id).await?;
            } else {
                self.meta.backup_delete(&old.id).await?;
            }
        }
        Ok(())
    }

    /// Local copies a failed upload left. They bridge an outage, so one stays until a run succeeds;
    /// more would fill the disk during a long one.
    async fn sweep_failed_uploads(&self, keep_newest: bool) -> Result<()> {
        let mut failed = Vec::new();
        if let Ok(rd) = std::fs::read_dir(&self.cfg.dir) {
            for e in rd.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                let id = name
                    .strip_prefix("rt-backup-")
                    .and_then(|n| n.strip_suffix(".tar.age").or_else(|| n.strip_suffix(".tar")));
                let Some(id) = id else { continue };
                if self.meta.backup_get(id).await?.is_some_and(|r| r.state == "failed") {
                    failed.push(id.to_string());
                }
            }
        }
        failed.sort_unstable(); // ulids: oldest first
        if keep_newest {
            failed.pop();
        }
        for id in failed {
            self.remove_local(&id);
        }
        Ok(())
    }

    /// Apply `[backup.s3]` retention to the bucket: rows of what it removed go too.
    pub async fn apply_remote_retention(&self) -> Result<()> {
        let Some(remote) = &self.remote else {
            return Ok(());
        };
        // Only what this node made: another one may share the prefix.
        let mut owned = std::collections::HashSet::new();
        let mut cursor = None;
        loop {
            let page = self.meta.backup_list(PageReq { limit: 200, cursor }).await?;
            owned.extend(page.items.into_iter().map(|r| r.id));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let session = remote.client.session().await?;
        let pruned = apply_retention(&session, remote.client.prefix(), &remote.retention, now_ms(), &owned).await?;
        for id in pruned.ids {
            self.remove_local(&id);
            self.meta.backup_delete(&id).await?;
        }
        Ok(())
    }

    /// One scheduler pass: start a due backup. A refusal the operator would want to know about
    /// (no disk) leaves a failed row; a pressured node retries in [`PRESSURE_RETRY_MS`].
    pub async fn tick(self: &Arc<Self>, now: i64, pressure: bool) -> Option<BackupRow> {
        let schedule = self.schedule.as_ref()?;
        let due = {
            let mut next = self.next_at.lock();
            let at = *next.get_or_insert_with(|| next_after(schedule, now));
            if at > now {
                return None;
            }
            if pressure {
                *next = Some(now + PRESSURE_RETRY_MS);
                false
            } else {
                *next = Some(next_after(schedule, now));
                true
            }
        };
        if !due {
            tracing::info!("scheduled backup put off: memory pressure");
            return None;
        }
        match self.start("schedule").await {
            Ok(row) => Some(row),
            Err(e) if e.code == "backup_running" => None,
            Err(e) => {
                tracing::warn!(code = e.code, "scheduled backup not started: {}", e.message);
                let id = format!("bkp_{}", ulid::Ulid::new());
                let row = BackupRow {
                    id: id.clone(),
                    state: "running".into(),
                    trigger: "schedule".into(),
                    started_at: now,
                    finished_at: None,
                    bytes: None,
                    sha256: None,
                    encrypted: false,
                    local_path: None,
                    remote_key: None,
                    verified_at: None,
                    rt_version: env!("CARGO_PKG_VERSION").into(),
                    failure: None,
                };
                if self.meta.backup_insert(row).await.is_ok() {
                    let _ = self.meta.backup_mark_failed(&id, now, e.to_json()).await;
                }
                None
            }
        }
    }

    pub async fn list(&self, page: PageReq) -> Result<Page<BackupRow>> {
        Ok(self.meta.backup_list(page).await?)
    }

    pub async fn get(&self, id: &str) -> Result<Option<BackupRow>> {
        Ok(self.meta.backup_get(id).await?)
    }

    /// Delete a finished backup: its row and its file, built from its id.
    pub async fn delete(&self, id: &str) -> Result<bool> {
        let Some(row) = self.meta.backup_get(id).await? else {
            return Ok(false);
        };
        if matches!(row.state.as_str(), "running" | "uploading" | "verifying") {
            return Err(BackupError::new("backup_running", "this backup is still running"));
        }
        if let (Some(remote), true) = (&self.remote, row.remote_key.is_some() && id_ok(id)) {
            // Built from the id, both forms: a restored row can say anything.
            let session = remote.client.session().await?;
            for name in [encrypted_archive_name(id), archive_name(id)] {
                delete_objects(&session, &remote.client.key(&name)).await?;
            }
        }
        self.remove_local(id);
        Ok(self.meta.backup_delete(id).await?)
    }

    pub async fn status(&self) -> Result<BackupStatus> {
        let last_ok = self.meta.backup_last_ok().await?;
        let last_failure = self.meta.backup_last_failure().await?.and_then(|b| b.failure);
        let next_at = self
            .schedule
            .as_ref()
            .map(|s| *self.next_at.lock().get_or_insert_with(|| next_after(s, now_ms())));
        Ok(BackupStatus {
            enabled: self.cfg.enabled,
            schedule: self.cfg.schedule.clone(),
            last_ok_at: last_ok.as_ref().and_then(|b| b.finished_at),
            last_ok_id: last_ok.map(|b| b.id),
            last_failure,
            next_at,
            last_restored_at: self.meta.last_restored_at().await?,
            running_id: self.running.lock().clone(),
            remote_host: self.remote.as_ref().map(|r| r.client.endpoint_host().to_string()),
            remote_bucket: self.remote.as_ref().map(|r| r.client.bucket().to_string()),
        })
    }
}

fn next_after(s: &cron::Schedule, now: i64) -> i64 {
    chrono::DateTime::from_timestamp_millis(now)
        .and_then(|t| s.after(&t).next())
        .map_or(i64::MAX, |t| t.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_field_cron_is_accepted_and_junk_is_not() {
        assert!(parse_schedule("0 3 * * *").is_ok());
        assert!(parse_schedule("0 0 3 * * *").is_ok());
        assert_eq!(parse_schedule("nightly").unwrap_err().code, "bad_schedule");
    }

    /// The archive path is built from the id, so an id cannot be a path.
    #[test]
    fn only_a_backup_id_makes_a_path() {
        assert!(id_ok(&format!("bkp_{}", ulid::Ulid::new())));
        for bad in [
            "bkp_",
            "../../master.key",
            "bkp_../../../../etc/passwd/xxxxxxx",
            "bkp_01hzzzzzzzzzzzzzzzzzzzzzzz",
            "svc_01HZZZZZZZZZZZZZZZZZZZZZZZ",
        ] {
            assert!(!id_ok(bad), "{bad}");
        }
    }
}
