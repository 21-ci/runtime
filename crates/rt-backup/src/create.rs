//! Taking a backup: everything here runs on the backup's own thread, one file after another.
//!
//! Order (docs/deploy.md): the caller's GC pause, then `meta.db`, which fixes the set of services
//! and versions; `queue.db`, `streams.db`; each service in that meta snapshot under its hold;
//! artifacts the meta copy references. Scratch is one database at a time: each is copied, checked,
//! compressed into the archive and deleted.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rt_sec::{secret_aad, MasterKey, Sealed, SecretsBackend};
use rt_store::{snapshot_db, LocalServiceData, ServiceDataStore};
use rt_types::{Digest, ServiceId, TenantId};
use rusqlite::{Connection, OpenFlags};

use crate::archive::{
    archive_name, encrypted_archive_name, sign_manifest, FileEntry, HashReader, Manifest, SchemaVersions, FORMAT, MAC,
    MANIFEST,
};
use crate::encrypt::Sink;
use crate::untrusted::{quick_check, EntryKind, NodeDb};
use crate::{BackupError, Result};

/// Data databases `quick_check`ed per run; the sample rotates, so all get covered.
pub const VERIFY_SAMPLE: usize = 20;

/// What a backup reads. Nothing here comes from a request.
pub struct Sources {
    pub data_dir: PathBuf,
    pub data: Arc<LocalServiceData>,
    pub secrets: Arc<dyn SecretsBackend>,
    pub master: MasterKey,
}

/// A finished archive.
#[derive(Clone, Debug)]
pub struct Created {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
    /// Largest `-wal` seen right after a snapshot, when checkpoints were still held back.
    pub peak_wal_bytes: u64,
    pub manifest: Manifest,
}

struct Writer {
    tar: tar::Builder<Sink>,
    files: Vec<FileEntry>,
}

impl Writer {
    fn append(&mut self, path: &str, size: u64, reader: &mut impl Read) -> Result<(u64, String)> {
        let mut header = tar::Header::new_gnu();
        header.set_size(size);
        header.set_mode(0o600);
        header.set_mtime(0);
        header.set_entry_type(tar::EntryType::Regular);
        let mut hashing = HashReader::new(reader.take(size));
        self.tar.append_data(&mut header, path, &mut hashing)?;
        let (n, sha) = hashing.finish();
        if n != size {
            // The tar is already short of this entry's bytes: this archive cannot be finished.
            return Err(BackupError::new(
                "io_error",
                "a file changed size while it was archived",
            ));
        }
        Ok((n, sha))
    }

    fn add_file(&mut self, kind: &EntryKind, path: &Path) -> Result<bool> {
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let size = f.metadata()?.len();
        let name = kind.archive_path();
        let (size, sha256) = self.append(&name, size, &mut f)?;
        self.files.push(FileEntry {
            path: name,
            size,
            sha256,
            raw_size: None,
        });
        Ok(true)
    }

    /// Compress the snapshot `db` next to itself, append it, delete both.
    fn add_db(&mut self, kind: &EntryKind, db: &Path) -> Result<()> {
        let raw_size = std::fs::metadata(db)?.len();
        let packed = db.with_extension("zst");
        {
            let mut out = private_file(&packed)?;
            let mut enc = zstd::stream::write::Encoder::new(BufWriter::new(&mut out), 3)?;
            enc.include_checksum(true)?;
            io::copy(&mut File::open(db)?, &mut enc)?;
            enc.finish()?.flush()?;
        }
        std::fs::remove_file(db)?;
        let mut f = File::open(&packed)?;
        let size = f.metadata()?.len();
        let name = kind.archive_path();
        let (size, sha256) = self.append(&name, size, &mut f)?;
        std::fs::remove_file(&packed)?;
        self.files.push(FileEntry {
            path: name,
            size,
            sha256,
            raw_size: Some(raw_size),
        });
        Ok(())
    }
}

fn private_file(path: &Path) -> Result<File> {
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}

fn open_copy(path: &Path, flags: OpenFlags) -> Result<Connection> {
    let c = Connection::open_with_flags(path, flags | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
    c.execute_batch("PRAGMA mmap_size = 0; PRAGMA synchronous = OFF")?;
    Ok(c)
}

fn wal_len(db: &Path) -> u64 {
    let mut wal = db.as_os_str().to_os_string();
    wal.push("-wal");
    std::fs::metadata(wal).map(|m| m.len()).unwrap_or(0)
}

fn schema_version(db: &Path) -> Result<u32> {
    let c = open_copy(db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    Ok(c.query_row("PRAGMA user_version", [], |r| r.get(0))?)
}

/// What the meta copy says must be in the archive.
struct MetaFacts {
    services: Vec<ServiceId>,
    artifacts: BTreeSet<Digest>,
}

fn read_meta_facts(meta_copy: &Path) -> Result<MetaFacts> {
    let c = open_copy(meta_copy, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let services = c
        .prepare("SELECT id FROM services ORDER BY id")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    let artifacts = c
        .prepare(
            "SELECT source_digest FROM versions \
             UNION SELECT lock_digest FROM versions WHERE lock_digest IS NOT NULL \
             UNION SELECT bundle_digest FROM versions WHERE bundle_digest IS NOT NULL \
             UNION SELECT module_digest FROM lock_modules",
        )?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .filter_map(|d| d.strip_prefix(Digest::PREFIX).and_then(|h| Digest::from_hex(h).ok()))
        .collect();
    Ok(MetaFacts { services, artifacts })
}

/// Open one sealed secret of the meta copy under the current keys: a backup whose secrets nothing
/// can open is worth knowing about now, not at the restore.
fn check_one_secret(meta_copy: &Path, backend: &dyn SecretsBackend) -> Result<()> {
    let c = open_copy(meta_copy, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let row = c.query_row(
        "SELECT s.tenant_id, x.service_id, x.name, x.sealed FROM secrets x JOIN services s ON s.id = x.service_id LIMIT 1",
        [],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Vec<u8>>(3)?,
            ))
        },
    );
    let (tenant, service, name, bytes) = match row {
        Ok(r) => r,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let bad = || {
        BackupError::new(
            "seal_check_failed",
            "a stored secret does not open under the master key",
        )
    };
    let tenant: TenantId = tenant.parse().map_err(|_| bad())?;
    let service: ServiceId = service.parse().map_err(|_| bad())?;
    let sealed = Sealed::from_bytes(&bytes).map_err(|_| bad())?;
    backend
        .open(&secret_aad(tenant, service, &name), &sealed)
        .map_err(|_| bad())?;
    Ok(())
}

/// Blob rows of the service copy: `(key, file)`.
fn blob_rows(copy: &Path) -> Result<Vec<(Vec<u8>, String)>> {
    let c = open_copy(copy, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let exists: i64 = c.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_rt_blobs'",
        [],
        |r| r.get(0),
    )?;
    if exists == 0 {
        return Ok(Vec::new());
    }
    let rows = c
        .prepare("SELECT key, file FROM _rt_blobs")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Does service number `i` of `n` fall in this run's rotating sample?
fn in_sample(i: usize, n: usize, offset: usize) -> bool {
    n <= VERIFY_SAMPLE || (i + n - offset % n) % n < VERIFY_SAMPLE
}

fn check_cancel(cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::Relaxed) {
        return Err(BackupError::new("cancelled", "the node is shutting down"));
    }
    Ok(())
}

/// Write `<dir>/rt-backup-<id>.tar`, in plaintext.
#[allow(clippy::too_many_arguments)]
pub fn create_archive(
    src: &Sources,
    dir: &Path,
    tmp: &Path,
    id: &str,
    started_at: i64,
    sample_offset: usize,
    cancel: &AtomicBool,
    rt_version: &str,
) -> Result<Created> {
    create_archive_to(src, dir, tmp, id, started_at, sample_offset, cancel, rt_version, &[])
}

/// Write `<dir>/rt-backup-<id>.tar`, or `.tar.age` encrypted to `recipients`: the tar then goes
/// through `age` straight to the file. `tmp` is an empty private directory for scratch, holding one
/// database snapshot at a time (the caller removes it); `.partial` is renamed into place only when
/// the archive is complete and on disk.
#[allow(clippy::too_many_arguments)]
pub fn create_archive_to(
    src: &Sources,
    dir: &Path,
    tmp: &Path,
    id: &str,
    started_at: i64,
    sample_offset: usize,
    cancel: &AtomicBool,
    rt_version: &str,
    recipients: &[age::x25519::Recipient],
) -> Result<Created> {
    let name = if recipients.is_empty() {
        archive_name(id)
    } else {
        encrypted_archive_name(id)
    };
    let final_path = dir.join(&name);
    let partial = dir.join(format!("{name}.partial"));
    let out = private_file(&partial)?;
    let mut w = Writer {
        tar: tar::Builder::new(Sink::new(out, recipients)?),
        files: Vec::new(),
    };
    let mut peak_wal = 0u64;
    let mut schema = SchemaVersions::default();
    let mut dropped = 0u64;

    let holds = src.data.holds();
    let _gc = holds.pause_gc();

    // 1. meta.db: fixes the services and versions; checked before anything depends on it.
    let meta_copy = tmp.join("meta.db");
    let meta_src = src.data_dir.join(NodeDb::Meta.file_name());
    snapshot_db(&meta_src, &meta_copy)?;
    peak_wal = peak_wal.max(wal_len(&meta_src));
    {
        let c = open_copy(&meta_copy, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        quick_check(&c)?;
    }
    schema.meta = schema_version(&meta_copy)?;
    check_one_secret(&meta_copy, src.secrets.as_ref())?;
    let facts = read_meta_facts(&meta_copy)?;
    w.add_db(&EntryKind::NodeDb(NodeDb::Meta), &meta_copy)?;

    // 2. queue.db, streams.db
    for db in [NodeDb::Queue, NodeDb::Streams] {
        check_cancel(cancel)?;
        let source = src.data_dir.join(db.file_name());
        if !source.exists() {
            continue;
        }
        let copy = tmp.join(db.file_name());
        snapshot_db(&source, &copy)?;
        peak_wal = peak_wal.max(wal_len(&source));
        let version = schema_version(&copy)?;
        match db {
            NodeDb::Queue => schema.queue = version,
            _ => schema.streams = version,
        }
        w.add_db(&EntryKind::NodeDb(db), &copy)?;
    }

    // 3. services
    let total = facts.services.len();
    for (i, &svc) in facts.services.iter().enumerate() {
        check_cancel(cancel)?;
        // Before the snapshot: a blob deleted after it still has its file for the walk below.
        let hold = holds.hold(svc);
        let copy = tmp.join("data.db");
        match src.data.snapshot_for_backup(svc, &copy) {
            Ok(_) => {}
            Err(rt_store::StoreError::NotFound) => continue, // no data, or deleted mid-run
            Err(e) => return Err(e.into()),
        }
        peak_wal = peak_wal.max(wal_len(
            &src.data_dir.join("services").join(svc.to_string()).join("data.db"),
        ));
        if in_sample(i, total, sample_offset) {
            let c = open_copy(&copy, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            quick_check(&c)?;
        }
        let objects = src.data.blob_objects_dir(svc);
        let mut missing: Vec<Vec<u8>> = Vec::new();
        for (key, file) in blob_rows(&copy)? {
            check_cancel(cancel)?;
            if !rt_store::blob_file_name_ok(&file) {
                return Err(BackupError::new("corrupt", "a blob row has a bad file name"));
            }
            let kind = EntryKind::Blob(svc, file.clone());
            if !w.add_file(&kind, &objects.join(&file))? {
                missing.push(key); // the service was purged during the backup
            }
        }
        if !missing.is_empty() {
            dropped += missing.len() as u64;
            let c = open_copy(&copy, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
            for key in missing {
                c.execute("DELETE FROM _rt_blobs WHERE key = ?1", [key])?;
            }
        }
        w.add_db(&EntryKind::ServiceDb(svc), &copy)?;
        drop(hold); // deletes what was deferred, on this thread
    }

    // 4. artifacts the meta copy references
    let artifacts = src.data_dir.join("artifacts");
    for d in &facts.artifacts {
        check_cancel(cancel)?;
        let entry = artifacts.join(d.hex());
        if w.add_file(&EntryKind::ArtifactBlob(*d), &entry.join("blob"))? {
            w.add_file(&EntryKind::ArtifactMeta(*d), &entry.join("meta.json"))?;
        }
    }

    // 5. manifest and MAC, last
    let Writer { mut tar, files } = w;
    let manifest = Manifest {
        format: FORMAT,
        id: id.to_string(),
        rt_version: rt_version.to_string(),
        schema,
        started_at,
        finished_at: chrono::Utc::now().timestamp_millis(),
        consistency: "per_database".to_string(),
        dropped_blob_rows: dropped,
        files,
    };
    let bytes = serde_json::to_vec(&manifest).map_err(|_| BackupError::new("io_error", "manifest encoding"))?;
    let tag = sign_manifest(&src.master, &bytes);
    for (name, body) in [(MANIFEST, bytes.as_slice()), (MAC, tag.as_bytes())] {
        let mut header = tar::Header::new_gnu();
        header.set_size(body.len() as u64);
        header.set_mode(0o600);
        header.set_mtime(0);
        tar.append_data(&mut header, name, body)?;
    }
    let hashed = tar.into_inner()?.finish()?;
    let (buffered, total_bytes, sha256) = hashed.finish();
    let file = buffered.into_inner().map_err(|e| BackupError::from(e.into_error()))?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&partial, &final_path)?;
    File::open(dir)?.sync_all()?;
    Ok(Created {
        path: final_path,
        bytes: total_bytes,
        sha256,
        peak_wal_bytes: peak_wal,
        manifest,
    })
}

/// The backup directory, private to the node.
pub fn private_dir(dir: &Path) -> Result<()> {
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
    // `recursive` leaves one that already exists as it was.
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::in_sample;

    #[test]
    fn a_private_dir_tightens_an_existing_one() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().expect("dir");
        let p = d.path().join("open");
        std::fs::create_dir(&p).expect("mkdir");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        super::private_dir(&p).expect("private");
        let mode = std::fs::metadata(&p).expect("meta").permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn the_sample_rotates_over_every_service() {
        let n = 50;
        let mut covered = vec![false; n];
        for run in 0..3 {
            for (i, c) in covered.iter_mut().enumerate() {
                if in_sample(i, n, run * super::VERIFY_SAMPLE) {
                    *c = true;
                }
            }
        }
        assert!(covered.iter().all(|c| *c));
        assert_eq!((0..n).filter(|i| in_sample(*i, n, 0)).count(), 20);
        assert_eq!((0..5).filter(|i| in_sample(*i, 5, 3)).count(), 5);
    }
}
