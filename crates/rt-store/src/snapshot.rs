//! Whole-database snapshots and what keeps them from hurting the node.
//!
//! - [`snapshot_db`]: one-step SQLite Backup from a dedicated read-only connection. One step holds
//!   one WAL read snapshot, so writers keep going and only checkpointing waits; a stepped backup
//!   restarts when another connection writes between steps. `VACUUM INTO` is not used: it runs on
//!   the writer connection, blocking the service's writes, and rewrites every page.
//! - [`HeavyThread`]: heavy jobs run in order on one low-priority `std::thread`, never through
//!   `BlockingRunner`: `edge-1gb` has one blocking thread shared with the store, and jobs there are
//!   not preemptive, so one long step would stall all `std:sql` and `std:kv` on the node.
//! - [`HeavyJobs`]: at most one job per tenant and [`MAX_NODE_HEAVY_JOBS`] on the node.
//! - [`ExportCache`]: snapshots kept for a TTL so Range hops see one file, bounded in bytes and by
//!   a free-disk floor. Keys are in memory: nothing a client sends becomes a path.
//! - [`BackupHolds`]: deferred blob unlinks, and the pause for artifact GC.

use std::collections::{HashMap, HashSet};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rt_types::{ServiceId, TenantId};
use rusqlite::backup::{Backup, StepResult};
use rusqlite::{Connection, OpenFlags};

use crate::error::{Result, StoreError};
use crate::sqlutil::unique_name;

/// A heavy job is refused (`429`) rather than queued past this many.
pub const MAX_NODE_HEAVY_JOBS: usize = 2;
/// How long one step waits out a locked source.
const SNAPSHOT_BUSY_WAIT: Duration = Duration::from_secs(5);

pub(crate) const HEAVY_JOB_BUSY: StoreError = StoreError::Conflict("heavy_job_busy");
pub(crate) const INSUFFICIENT_STORAGE: StoreError = StoreError::Conflict("insufficient_storage");

fn map_full(e: rusqlite::Error) -> StoreError {
    if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
        return INSUFFICIENT_STORAGE;
    }
    e.into()
}

/// Copy the database at `src` to the new file `dst`, private to the node, as one consistent commit
/// in a standalone (non-WAL) file. Returns its size. `dst` is disposable until the caller syncs it:
/// no flushes happen here. On failure `dst` is gone, unless it already existed: that is refused.
pub fn snapshot_db(src: &Path, dst: &Path) -> Result<u64> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dst)?;
    let res = copy_pages(src, dst);
    if res.is_err() {
        let _ = std::fs::remove_file(dst);
    }
    res
}

fn copy_pages(src: &Path, dst: &Path) -> Result<u64> {
    let source = Connection::open_with_flags(src, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
    source.busy_timeout(SNAPSHOT_BUSY_WAIT)?;
    source.execute_batch("PRAGMA cache_size = -1024; PRAGMA mmap_size = 0")?;
    let mut out =
        Connection::open_with_flags(dst, OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
    out.execute_batch("PRAGMA synchronous = OFF; PRAGMA journal_mode = OFF; PRAGMA cache_size = -2048")?;
    {
        let backup = Backup::new(&source, &mut out).map_err(map_full)?;
        let deadline = Instant::now() + SNAPSHOT_BUSY_WAIT;
        loop {
            match backup.step(-1).map_err(map_full)? {
                StepResult::Done => break,
                // LCOV_EXCL_START: only a writer holding an exclusive lock on the source; WAL readers do not meet it
                _ if Instant::now() >= deadline => return Err(StoreError::SqlBusy),
                _ => std::thread::sleep(Duration::from_millis(50)),
                // LCOV_EXCL_STOP
            }
        }
    }
    drop(out);
    drop(source);
    // The copied header announces WAL; the copy is a standalone file.
    let copy = Connection::open(dst)?;
    copy.execute_batch("PRAGMA synchronous = OFF")?;
    copy.query_row("PRAGMA journal_mode = DELETE", [], |r| r.get::<_, String>(0))
        .map_err(map_full)?;
    drop(copy);
    Ok(std::fs::metadata(dst)?.len())
}

/// Bytes a snapshot of `src` can take: the file and its WAL.
pub(crate) fn snapshot_estimate(src: &Path) -> u64 {
    let len = |p: &Path| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
    let mut wal = src.as_os_str().to_os_string();
    wal.push("-wal");
    len(src).saturating_add(len(Path::new(&wal)))
}

/// `507 insufficient_storage` when writing `needed` bytes under `dir` would leave less than `floor`.
pub(crate) fn check_disk_floor(dir: &Path, needed: u64, floor: u64) -> Result<()> {
    let Some(free) = free_bytes(dir) else {
        return Ok(()); // LCOV_EXCL_LINE: statvfs on a directory the store just created does not fail
    };
    if needed.saturating_add(floor) > free {
        return Err(INSUFFICIENT_STORAGE);
    }
    Ok(())
}

/// Free bytes on the file system of `dir`, for an unprivileged writer.
#[allow(clippy::unnecessary_cast)] // the statvfs field types differ between platforms
pub fn free_bytes(dir: &Path) -> Option<u64> {
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `path` is NUL-terminated and `st` is a valid out-pointer for the call.
    if unsafe { libc::statvfs(path.as_ptr(), &mut st) } != 0 {
        return None; // LCOV_EXCL_LINE: see check_disk_floor
    }
    Some((st.f_bavail as u64).saturating_mul(st.f_frsize as u64))
}

// ---- the thread --------------------------------------------------------------------------------

type HeavyFn = Box<dyn FnOnce() + Send + 'static>;

/// One low-priority thread running heavy jobs in order, started on first use. It ends when the
/// store goes away.
#[derive(Default)]
pub(crate) struct HeavyThread {
    tx: Mutex<Option<mpsc::Sender<HeavyFn>>>,
}

impl HeavyThread {
    pub(crate) fn submit(&self, job: HeavyFn) -> Result<()> {
        let mut tx = self.tx.lock();
        if tx.is_none() {
            let (sender, rx) = mpsc::channel::<HeavyFn>();
            std::thread::Builder::new().name("rt-snapshot".into()).spawn(move || {
                lower_this_thread();
                while let Ok(job) = rx.recv() {
                    // A panic ends the job, whose result slot drops; the thread stays for the next.
                    let _ = std::panic::catch_unwind(AssertUnwindSafe(job));
                }
            })?;
            *tx = Some(sender);
        }
        tx.as_ref()
            .and_then(|t| t.send(job).ok())
            .ok_or_else(|| StoreError::Internal("heavy-job thread is gone".into()))
        // LCOV_EXCL_LINE: the thread outlives the sender and catches panics
    }

    /// Run `f` there and wait for it. A caller that went away before its turn costs no work.
    pub(crate) async fn run<T: Send + 'static>(&self, f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.submit(Box::new(move || {
            if !tx.is_closed() {
                let _ = tx.send(f());
            }
        }))?;
        rx.await
            .unwrap_or_else(|_| Err(StoreError::Internal("heavy job did not complete".into())))
    }
}

/// `nice` and `ioprio` for this thread where a thread can have its own (Linux); elsewhere
/// `setpriority` would reach the whole process, so it stays as is.
pub fn lower_this_thread() {
    #[cfg(target_os = "linux")]
    // SAFETY: both calls only take integers, and `0` means the calling thread on Linux.
    unsafe {
        const IOPRIO_WHO_PROCESS: libc::c_long = 1;
        const IOPRIO_BEST_EFFORT_LOWEST: libc::c_long = (2 << 13) | 7;
        let _ = libc::setpriority(libc::PRIO_PROCESS, 0, 10);
        let _ = libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, IOPRIO_BEST_EFFORT_LOWEST);
    }
}

// ---- the heavy-job rule ------------------------------------------------------------------------

#[derive(Default)]
struct HeavyState {
    running: usize,
    tenants: HashSet<TenantId>,
}

/// One heavy job per tenant, [`MAX_NODE_HEAVY_JOBS`] on the node.
#[derive(Clone, Default)]
pub(crate) struct HeavyJobs(Arc<Mutex<HeavyState>>);

/// A claim on the rule; dropping it gives the slot back.
pub(crate) struct HeavySlot {
    jobs: Arc<Mutex<HeavyState>>,
    tenant: Option<TenantId>,
}

impl HeavyJobs {
    pub(crate) fn claim(&self, tenant: Option<TenantId>) -> Result<HeavySlot> {
        let mut st = self.0.lock();
        if st.running >= MAX_NODE_HEAVY_JOBS || tenant.is_some_and(|t| st.tenants.contains(&t)) {
            return Err(HEAVY_JOB_BUSY);
        }
        st.running += 1;
        if let Some(t) = tenant {
            st.tenants.insert(t);
        }
        Ok(HeavySlot {
            jobs: self.0.clone(),
            tenant,
        })
    }
}

impl Drop for HeavySlot {
    fn drop(&mut self) {
        let mut st = self.jobs.lock();
        st.running -= 1;
        if let Some(t) = self.tenant {
            st.tenants.remove(&t);
        }
    }
}

// ---- the export cache --------------------------------------------------------------------------

/// Bytes counted against the cache, given back on drop.
struct Reservation {
    used: Arc<AtomicU64>,
    bytes: u64,
}

impl Reservation {
    fn settle(&mut self, actual: u64) {
        self.used.fetch_add(actual, Ordering::AcqRel);
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
        self.bytes = actual;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct ExportEntry {
    path: PathBuf,
    size: u64,
    etag: String,
    created: Instant,
    _bytes: Reservation,
}

impl Drop for ExportEntry {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A finished snapshot on disk. The file stays while any clone lives, whatever the cache does.
#[derive(Clone)]
pub struct ExportFile(Arc<ExportEntry>);

impl ExportFile {
    pub fn path(&self) -> &Path {
        &self.0.path
    }
    pub fn size(&self) -> u64 {
        self.0.size
    }
    /// Opaque and fresh per snapshot: two snapshots of unchanged data still differ.
    pub fn etag(&self) -> &str {
        &self.0.etag
    }
}

impl std::fmt::Debug for ExportFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportFile")
            .field("size", &self.0.size)
            .finish_non_exhaustive()
    }
}

/// A snapshot being taken: where it goes, and the bytes reserved for it.
pub(crate) struct PendingExport {
    pub(crate) path: PathBuf,
    bytes: Reservation,
}

pub(crate) struct ExportCache {
    dir: PathBuf,
    ttl: Duration,
    max_bytes: u64,
    floor: u64,
    used: Arc<AtomicU64>,
    entries: Mutex<HashMap<(ServiceId, String), Arc<ExportEntry>>>,
}

impl ExportCache {
    /// Clears what a previous process left in `dir`.
    pub(crate) fn new(dir: PathBuf, ttl: Duration, max_bytes: u64, floor: u64) -> Self {
        let _ = std::fs::remove_dir_all(&dir);
        Self {
            dir,
            ttl,
            max_bytes,
            floor,
            used: Arc::new(AtomicU64::new(0)),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Room for `estimate` bytes: idle snapshots go first, oldest first; else `507`.
    pub(crate) fn reserve(&self, estimate: u64) -> Result<PendingExport> {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&self.dir)?;
        check_disk_floor(&self.dir, estimate, self.floor)?;
        self.sweep();
        let mut victims = Vec::new();
        let reserved = {
            let mut entries = self.entries.lock();
            loop {
                let used = self.used.load(Ordering::Acquire);
                let freeing: u64 = victims.iter().map(|v: &Arc<ExportEntry>| v.size).sum();
                if used.saturating_sub(freeing).saturating_add(estimate) <= self.max_bytes {
                    break true;
                }
                let idle = entries
                    .iter()
                    .filter(|(_, e)| Arc::strong_count(e) == 1)
                    .min_by_key(|(_, e)| e.created)
                    .map(|(k, _)| k.clone());
                match idle.and_then(|k| entries.remove(&k)) {
                    Some(v) => victims.push(v),
                    None => break false,
                }
            }
        };
        // Unlink outside the lock; only then are their bytes back.
        drop(victims);
        if !reserved {
            return Err(INSUFFICIENT_STORAGE);
        }
        self.used.fetch_add(estimate, Ordering::AcqRel);
        Ok(PendingExport {
            path: self.dir.join(format!("{}.db", unique_name())),
            bytes: Reservation {
                used: self.used.clone(),
                bytes: estimate,
            },
        })
    }

    pub(crate) fn finish(pending: PendingExport, size: u64) -> ExportFile {
        let PendingExport { path, mut bytes } = pending;
        bytes.settle(size);
        ExportFile(Arc::new(ExportEntry {
            path,
            size,
            etag: unique_name(),
            created: Instant::now(),
            _bytes: bytes,
        }))
    }

    pub(crate) fn insert(&self, s: ServiceId, file: &ExportFile) {
        self.entries.lock().insert((s, file.0.etag.clone()), file.0.clone());
    }

    pub(crate) fn get(&self, s: ServiceId, etag: &str) -> Option<ExportFile> {
        self.sweep();
        self.entries
            .lock()
            .get(&(s, etag.to_string()))
            .map(|e| ExportFile(e.clone()))
    }

    /// Forget snapshots past the TTL (a file still being streamed goes when its reader ends).
    pub(crate) fn sweep(&self) {
        let ttl = self.ttl;
        let mut gone = Vec::new();
        self.entries.lock().retain(|_, e| {
            let keep = e.created.elapsed() < ttl;
            if !keep {
                gone.push(e.clone());
            }
            keep
        });
    }

    pub(crate) fn forget_service(&self, s: ServiceId) {
        let mut gone = Vec::new();
        self.entries.lock().retain(|(svc, _), e| {
            let keep = *svc != s;
            if !keep {
                gone.push(e.clone());
            }
            keep
        });
    }
}

// ---- holds -------------------------------------------------------------------------------------

#[derive(Default)]
struct HoldState {
    held: HashMap<ServiceId, usize>,
    deferred: HashMap<ServiceId, Vec<PathBuf>>,
}

/// Keeps files a running snapshot may still read. While a service is held, its blob unlinks wait on
/// a list drained at the last release. The global pause stops artifact GC and the orphan sweep.
/// Cost when idle: one atomic load on the blob write path, none on KV or SQL.
#[derive(Default)]
pub struct BackupHolds {
    /// Services held. A deleter commits its row change and then reads this; a holder raises it
    /// before its snapshot starts. So a deleter that reads 0 changed a row the snapshot will not see.
    active: AtomicUsize,
    gc: AtomicUsize,
    state: Mutex<HoldState>,
}

impl BackupHolds {
    /// Hold `s` until the returned guard drops. Take it before the snapshot.
    pub fn hold(self: &Arc<Self>, s: ServiceId) -> ServiceHold {
        let mut st = self.state.lock();
        *st.held.entry(s).or_insert(0) += 1;
        self.active.fetch_add(1, Ordering::SeqCst);
        ServiceHold {
            holds: self.clone(),
            svc: s,
        }
    }

    pub fn is_held(&self, s: ServiceId) -> bool {
        self.active.load(Ordering::SeqCst) > 0 && self.state.lock().held.contains_key(&s)
    }

    /// Pause artifact GC until the returned guard drops.
    pub fn pause_gc(self: &Arc<Self>) -> GcPause {
        self.gc.fetch_add(1, Ordering::SeqCst);
        GcPause { holds: self.clone() }
    }

    pub fn gc_paused(&self) -> bool {
        self.gc.load(Ordering::SeqCst) > 0
    }

    /// `true` when the unlink of `path` was deferred; the caller deletes it otherwise.
    pub(crate) fn defer_unlink(&self, s: ServiceId, path: &Path) -> bool {
        if self.active.load(Ordering::SeqCst) == 0 {
            return false;
        }
        let mut st = self.state.lock();
        if !st.held.contains_key(&s) {
            return false;
        }
        st.deferred.entry(s).or_default().push(path.to_path_buf());
        true
    }
}

/// See [`BackupHolds::hold`]. The last one released deletes what was deferred, on this thread.
pub struct ServiceHold {
    holds: Arc<BackupHolds>,
    svc: ServiceId,
}

impl Drop for ServiceHold {
    fn drop(&mut self) {
        let files = {
            let mut st = self.holds.state.lock();
            self.holds.active.fetch_sub(1, Ordering::SeqCst);
            match st.held.get_mut(&self.svc) {
                Some(n) if *n > 1 => {
                    *n -= 1;
                    None
                }
                _ => {
                    st.held.remove(&self.svc);
                    st.deferred.remove(&self.svc)
                }
            }
        };
        for f in files.into_iter().flatten() {
            let _ = std::fs::remove_file(f);
        }
    }
}

/// See [`BackupHolds::pause_gc`].
pub struct GcPause {
    holds: Arc<BackupHolds>,
}

impl Drop for GcPause {
    fn drop(&mut self) {
        self.holds.gc.fetch_sub(1, Ordering::SeqCst);
    }
}
