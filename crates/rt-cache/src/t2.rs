//! T2: precompiled artifacts on local disk with a MAC (SEC-21, SEC-55).
//!
//! Layout v2: `cache/v2/<engine-fp>/<digest>/<file>` plus `<file>.mac` (e.g. `component.cwasm`,
//! `bundle`, `code_cache.bin`). Files are mode 0600 and written atomically (tmp + rename).
//! Every read verifies the MAC — bound to the entry's key **and the file name** — **before** the
//! bytes are handed to a deserializer; a mismatch deletes the entry, emits
//! `security.artifact_tamper` through the [`TamperSink`], and the entry is rebuilt from T3.
//! Compiled artifacts are never accepted from uploads or S3.
//!
//! [`T2Store::prepare`] runs once at startup: it deletes the legacy layout (`cache/<engine-fp>/`,
//! whose tags do not bind the file name) and v2 entries written under another MAC key (master-key
//! rotation), both **without** tamper events: they are stale, not attacked, and are rebuilt
//! lazily. Legacy files are never verified under the v2 MAC.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use rt_sec::{ArtifactMac, Mac};
use rt_types::{Digest, PreparedKey};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum T2Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// MAC mismatch or missing MAC file. The entry has already been deleted.
    #[error("artifact failed MAC verification")]
    Tampered,
}

/// Receives tamper notifications (the server turns them into `security.artifact_tamper` events).
pub trait TamperSink: Send + Sync {
    fn tampered(&self, key: &PreparedKey, file: &str);
}

/// Sink that ignores everything.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoTamperSink;

impl TamperSink for NoTamperSink {
    fn tampered(&self, _key: &PreparedKey, _file: &str) {}
}

/// One entry found on disk. The runtime kind is not part of the path (the engine fingerprint
/// already encodes it), so a `PreparedKey` cannot be rebuilt from the directory alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct T2Entry {
    pub engine_fp: u64,
    pub digest: rt_types::Digest,
    pub bytes: u64,
}

pub struct T2Store {
    root: PathBuf,
    /// `root/v2`: every entry of the current layout lives here.
    layout: PathBuf,
    mac: Arc<dyn ArtifactMac>,
    tamper: Arc<dyn TamperSink>,
    /// Readers share it; writers, removals and tamper cleanup take it exclusively, so a reader
    /// never sees half of an (artifact, MAC) pair.
    lock: RwLock<()>,
}

/// What [`T2Store::prepare`] removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct T2Prepared {
    /// Legacy-layout engine-fingerprint directories (`cache/<engine-fp>/`).
    pub legacy_dirs_removed: usize,
    /// v2 engine-fingerprint directories written under another MAC key.
    pub stale_key_dirs_removed: usize,
}

/// Directory of the current T2 layout under the cache root.
pub const T2_LAYOUT: &str = "v2";
/// File under `cache/v2/` naming the MAC key the entries were written with.
const KEY_MARKER: &str = ".mac-key";
const MAC_SUFFIX: &str = ".mac";
const MAC_LEN: usize = 32;

/// An engine-fingerprint directory name (16 hex digits).
fn is_fp_dir_name(name: &str) -> bool {
    name.len() == 16 && name.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Remove every engine-fingerprint directory directly under `dir`; returns how many.
fn remove_fp_dirs(dir: &Path) -> std::io::Result<usize> {
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut n = 0;
    for entry in rd.flatten() {
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        if is_dir && entry.file_name().to_str().is_some_and(is_fp_dir_name) {
            fs::remove_dir_all(entry.path())?;
            n += 1;
        }
    }
    Ok(n)
}

fn tmp_name() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(".tmp-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed))
}

fn private_dir(path: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new().recursive(true).mode(0o700).create(path)
}

/// tmp + rename, mode 0600.
fn write_atomic(dest: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = dest.with_file_name(tmp_name());
    let res = (|| {
        let mut f = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_data()?;
        drop(f);
        fs::rename(&tmp, dest)
    })();
    if res.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    res
}

fn read_opt(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Outcome of checking one file against its MAC sidecar.
enum Check {
    Absent,
    Good(Vec<u8>),
    Bad,
}

impl T2Store {
    /// `root` = `$RT_DATA/cache`; entries live under `root/v2`.
    pub fn new(root: impl Into<PathBuf>, mac: Arc<dyn ArtifactMac>, tamper: Arc<dyn TamperSink>) -> Self {
        let root = root.into();
        let layout = root.join(T2_LAYOUT);
        Self {
            root,
            layout,
            mac,
            tamper,
            lock: RwLock::new(()),
        }
    }

    /// Startup migration (SEC-55), before the first read: delete the legacy layout and the v2
    /// entries of another MAC key, then record the current key. Nothing removed here raises a
    /// tamper event; the entries are rebuilt from T3 on demand.
    pub fn prepare(&self) -> Result<T2Prepared, T2Error> {
        let _excl = self.lock.write();
        let mut out = T2Prepared {
            legacy_dirs_removed: remove_fp_dirs(&self.root)?,
            ..T2Prepared::default()
        };
        let key_id = self.mac.key_id();
        let marker = self.layout.join(KEY_MARKER);
        let recorded = read_opt(&marker)?.map(|b| String::from_utf8_lossy(&b).trim().to_string());
        if recorded.as_deref() != Some(key_id.as_str()) {
            // Another key, or no record at all: nothing under v2 can be trusted to verify.
            out.stale_key_dirs_removed = remove_fp_dirs(&self.layout)?;
            private_dir(&self.layout)?;
            write_atomic(&marker, key_id.as_bytes())?;
        }
        Ok(out)
    }

    /// `cache/v2/<engine-fp hex>/<digest hex>/<file>`.
    pub fn path_for(&self, key: &PreparedKey, file: &str) -> PathBuf {
        self.entry_dir(key).join(file)
    }

    /// Directory of one entry.
    pub fn entry_dir(&self, key: &PreparedKey) -> PathBuf {
        self.layout
            .join(format!("{:016x}", key.engine_fp))
            .join(key.digest.hex())
    }

    fn mac_path(&self, key: &PreparedKey, file: &str) -> PathBuf {
        self.path_for(key, &format!("{file}{MAC_SUFFIX}"))
    }

    fn check(&self, key: &PreparedKey, file: &str) -> Result<Check, T2Error> {
        let Some(bytes) = read_opt(&self.path_for(key, file))? else {
            return Ok(Check::Absent);
        };
        let Some(tag) = read_opt(&self.mac_path(key, file))? else {
            return Ok(Check::Bad);
        };
        let Ok(tag) = <[u8; MAC_LEN]>::try_from(tag.as_slice()) else {
            return Ok(Check::Bad);
        };
        Ok(if self.mac.verify(key, file, &bytes, &Mac(tag)) {
            Check::Good(bytes)
        } else {
            Check::Bad
        })
    }

    /// Verify `file` (MAC first, nothing is returned for a bad entry). A failure deletes the
    /// whole entry and reports it through the [`TamperSink`].
    fn verified(&self, key: &PreparedKey, file: &str) -> Result<Option<Vec<u8>>, T2Error> {
        {
            let _shared = self.lock.read();
            match self.check(key, file)? {
                Check::Absent => return Ok(None),
                Check::Good(b) => return Ok(Some(b)),
                Check::Bad => {}
            }
        }
        #[cfg(test)]
        failpoint::between_locks();
        // Re-check under the exclusive lock: a concurrent rewrite may have fixed the entry.
        let _excl = self.lock.write();
        match self.check(key, file)? {
            Check::Absent => Ok(None),
            Check::Good(b) => Ok(Some(b)),
            Check::Bad => {
                // Report before deleting: observers that see the entry vanish must already see the event.
                tracing::warn!(digest = %key.digest, file, "T2 artifact failed MAC verification; deleting entry");
                self.tamper.tampered(key, file);
                self.remove_locked(key)?;
                Err(T2Error::Tampered)
            }
        }
    }

    /// Write `bytes` and its MAC atomically (tmp + rename, mode 0600).
    ///
    /// Crash-consistent: whatever step a crash interrupts, a reader finds the entry absent (and
    /// it is rebuilt), never a MAC that does not match its artifact (a false `artifact_tamper`).
    /// The old artifact goes first, so a rewrite cannot leave the new MAC next to the old bytes;
    /// then the MAC, then the artifact, each by atomic rename.
    pub fn write(&self, key: &PreparedKey, file: &str, bytes: &[u8]) -> Result<(), T2Error> {
        let tag = self.mac.sign(key, file, bytes);
        let _excl = self.lock.write();
        private_dir(&self.entry_dir(key))?;
        let artifact = self.path_for(key, file);
        match fs::remove_file(&artifact) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        write_atomic(&self.mac_path(key, file), &tag.0)?;
        #[cfg(test)]
        if failpoint::CRASH_AFTER_MAC.with(|f| f.get()) {
            return Err(T2Error::Io(std::io::Error::other(
                "simulated crash after the MAC was written",
            )));
        }
        write_atomic(&artifact, bytes)?;
        Ok(())
    }

    /// Read and verify. `Ok(None)` when absent; `Err(Tampered)` after deleting a bad entry.
    pub fn read_verified(&self, key: &PreparedKey, file: &str) -> Result<Option<Bytes>, T2Error> {
        Ok(self.verified(key, file)?.map(Bytes::from))
    }

    /// Verify a file in place and return its path, for mmap-based loads (`.cwasm`): the caller
    /// maps the file only after this returns `Ok(Some(_))`.
    pub fn verify_path(&self, key: &PreparedKey, file: &str) -> Result<Option<PathBuf>, T2Error> {
        Ok(self.verified(key, file)?.map(|_| self.path_for(key, file)))
    }

    /// Total bytes under the cache root (disk GC accounting).
    pub fn total_bytes(&self) -> Result<u64, T2Error> {
        fn walk(dir: &Path) -> std::io::Result<u64> {
            let mut total = 0;
            let rd = match fs::read_dir(dir) {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
                Err(e) => return Err(e),
            };
            for entry in rd.flatten() {
                let meta = entry.metadata()?;
                total += if meta.is_dir() {
                    walk(&entry.path())?
                } else {
                    meta.len()
                };
            }
            Ok(total)
        }
        let _shared = self.lock.read();
        Ok(walk(&self.root)?)
    }

    fn remove_locked(&self, key: &PreparedKey) -> Result<(), T2Error> {
        // Artifacts before their MACs: a crash midway leaves MACs without artifacts (absent,
        // rebuilt), never an artifact without its MAC (a false tamper report).
        if let Ok(files) = fs::read_dir(self.entry_dir(key)) {
            for f in files.flatten() {
                if !f.file_name().to_string_lossy().ends_with(MAC_SUFFIX) {
                    let _ = fs::remove_file(f.path());
                }
            }
        }
        match fs::remove_dir_all(self.entry_dir(key)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        // Drop the engine-fingerprint directory once it is empty (fails harmlessly otherwise).
        let _ = fs::remove_dir(self.layout.join(format!("{:016x}", key.engine_fp)));
        Ok(())
    }

    /// Delete one entry (all files).
    pub fn remove(&self, key: &PreparedKey) -> Result<(), T2Error> {
        let _excl = self.lock.write();
        self.remove_locked(key)
    }

    /// Entries on disk (engine-fp and digest dirs), for startup scan and GC.
    pub fn list(&self) -> Result<Vec<T2Entry>, T2Error> {
        let _shared = self.lock.read();
        let mut out = Vec::new();
        let fps = match fs::read_dir(&self.layout) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        for fp_dir in fps.flatten() {
            let Some(engine_fp) = fp_dir
                .file_name()
                .to_str()
                .filter(|n| n.len() == 16)
                .and_then(|n| u64::from_str_radix(n, 16).ok())
            else {
                continue;
            };
            let Ok(digests) = fs::read_dir(fp_dir.path()) else {
                continue;
            };
            for d in digests.flatten() {
                let Some(digest) = d.file_name().to_str().and_then(|n| Digest::from_hex(n).ok()) else {
                    continue;
                };
                let bytes = fs::read_dir(d.path())
                    .map(|rd| {
                        rd.flatten()
                            .filter_map(|f| f.metadata().ok())
                            .filter(|m| m.is_file())
                            .map(|m| m.len())
                            .sum()
                    })
                    .unwrap_or(0);
                out.push(T2Entry {
                    engine_fp,
                    digest,
                    bytes,
                });
            }
        }
        Ok(out)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod failpoint {
    use std::cell::{Cell, RefCell};

    thread_local! {
        /// `write` stops right after the MAC, as if the node crashed there.
        pub static CRASH_AFTER_MAC: Cell<bool> = const { Cell::new(false) };
        /// Runs once, in `verified`, in the gap between the shared check and the exclusive re-check.
        pub static BETWEEN_LOCKS: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    pub fn between_locks() {
        if let Some(hook) = BETWEEN_LOCKS.with(|h| h.borrow_mut().take()) {
            hook();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    struct CountingSink(AtomicUsize);

    impl TamperSink for CountingSink {
        fn tampered(&self, _key: &PreparedKey, _file: &str) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_crash_while_rewriting_an_entry_reads_as_absent_not_as_tampered() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = Arc::new(CountingSink(AtomicUsize::new(0)));
        let t2 = T2Store::new(
            dir.path(),
            Arc::new(rt_sec::HkdfArtifactMac::new(&rt_sec::MasterKey::from_bytes([9; 32]))),
            sink.clone(),
        );
        let key = PreparedKey {
            digest: Digest::of(b"src"),
            kind: rt_types::RuntimeKind::Deno,
            engine_fp: 7,
        };
        t2.write(&key, "bundle", b"build one").expect("first write");
        assert_eq!(
            t2.read_verified(&key, "bundle").expect("read").as_deref(),
            Some(&b"build one"[..])
        );

        failpoint::CRASH_AFTER_MAC.with(|f| f.set(true));
        assert!(t2.write(&key, "bundle", b"build two, different bytes").is_err());
        failpoint::CRASH_AFTER_MAC.with(|f| f.set(false));

        assert!(
            t2.read_verified(&key, "bundle").expect("not a tamper error").is_none(),
            "absent: rebuilt on demand"
        );
        assert_eq!(sink.0.load(Ordering::SeqCst), 0, "no artifact_tamper event for a crash");
        t2.write(&key, "bundle", b"build two, different bytes")
            .expect("rewrite");
        assert!(t2.read_verified(&key, "bundle").expect("read").is_some());
    }

    fn sample_key() -> PreparedKey {
        PreparedKey {
            digest: Digest::of(b"src"),
            kind: rt_types::RuntimeKind::Deno,
            engine_fp: 7,
        }
    }

    fn store_at(root: &Path, sink: Arc<dyn TamperSink>) -> Arc<T2Store> {
        Arc::new(T2Store::new(
            root,
            Arc::new(rt_sec::HkdfArtifactMac::new(&rt_sec::MasterKey::from_bytes([9; 32]))),
            sink,
        ))
    }

    #[test]
    fn the_no_op_sink_accepts_tamper_reports_silently() {
        NoTamperSink.tampered(&sample_key(), "bundle");
    }

    #[test]
    fn private_file_helpers_report_io_errors_and_leave_no_temp_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A regular file is not a directory: listing its children is an error, not an empty set.
        let file = dir.path().join("file");
        std::fs::write(&file, b"x").expect("write");
        assert!(remove_fp_dirs(&file).is_err());
        // A directory where the atomic write lands: the rename fails and the temp file is removed.
        let target = dir.path().join("target");
        std::fs::create_dir(&target).expect("mkdir");
        assert!(write_atomic(&target, b"bytes").is_err());
        assert_eq!(
            std::fs::read_dir(dir.path()).expect("list").count(),
            2,
            "no temp file left behind"
        );
        // Reading a directory is an I/O error; a missing file is absence.
        assert!(read_opt(&target).is_err());
        assert!(read_opt(&dir.path().join("missing"))
            .expect("absent is not an error")
            .is_none());
    }

    #[test]
    fn a_truncated_mac_sidecar_is_a_tamper_and_the_entry_is_deleted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = Arc::new(CountingSink(AtomicUsize::new(0)));
        let t2 = store_at(dir.path(), sink.clone());
        let key = sample_key();
        t2.write(&key, "bundle", b"payload").expect("write");
        std::fs::write(t2.path_for(&key, "bundle.mac"), [0u8; 5]).expect("truncate the MAC");

        assert!(matches!(t2.read_verified(&key, "bundle"), Err(T2Error::Tampered)));
        assert_eq!(sink.0.load(Ordering::SeqCst), 1, "one tamper event");
        assert!(
            t2.read_verified(&key, "bundle").expect("read").is_none(),
            "gone after the delete"
        );
    }

    #[test]
    fn a_repair_or_removal_in_the_gap_between_the_two_checks_is_honoured() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = Arc::new(CountingSink(AtomicUsize::new(0)));
        let t2 = store_at(dir.path(), sink.clone());
        let key = sample_key();
        t2.write(&key, "bundle", b"good").expect("write");
        std::fs::write(t2.path_for(&key, "bundle"), b"evil").expect("corrupt the artifact");

        // Another writer repairs the entry before the exclusive re-check: the repaired bytes are returned.
        let repair = t2.clone();
        failpoint::BETWEEN_LOCKS.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || repair.write(&key, "bundle", b"good").expect("repair")));
        });
        assert_eq!(
            t2.read_verified(&key, "bundle").expect("read").as_deref(),
            Some(&b"good"[..])
        );

        // Another writer removes the entry instead: the re-check finds it absent.
        std::fs::write(t2.path_for(&key, "bundle"), b"evil").expect("corrupt again");
        let remove = t2.clone();
        failpoint::BETWEEN_LOCKS.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || remove.remove(&key).expect("remove")));
        });
        assert!(t2.read_verified(&key, "bundle").expect("read").is_none());
        assert_eq!(sink.0.load(Ordering::SeqCst), 0, "neither outcome is a tamper");
    }

    #[test]
    fn an_artifact_path_that_cannot_be_replaced_fails_the_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let t2 = store_at(dir.path(), Arc::new(NoTamperSink));
        let key = sample_key();
        std::fs::create_dir_all(t2.path_for(&key, "bundle")).expect("a directory where the artifact goes");
        assert!(t2.write(&key, "bundle", b"x").is_err());
    }

    #[test]
    fn removing_an_entry_whose_directory_is_a_file_reports_the_io_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let t2 = store_at(dir.path(), Arc::new(NoTamperSink));
        let key = sample_key();
        let entry = t2.entry_dir(&key);
        std::fs::create_dir_all(entry.parent().expect("parent")).expect("layout");
        std::fs::write(&entry, b"not a directory").expect("file in the entry's place");
        assert!(t2.remove(&key).is_err());
    }

    #[test]
    fn a_missing_cache_root_is_an_empty_cache() {
        let dir = tempfile::tempdir().expect("tempdir");
        let t2 = store_at(&dir.path().join("never-created"), Arc::new(NoTamperSink));
        assert_eq!(t2.total_bytes().expect("total"), 0);
        assert!(t2.list().expect("list").is_empty());
    }
}
