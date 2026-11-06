//! Offline `rt backup verify` and `rt backup restore`. Input is hostile until proven otherwise;
//! see [`crate::untrusted`]. The node is not running: nothing here touches a live data directory.
//!
//! The MAC is checked before the manifest is parsed, and the manifest before any data is written.
//! A restore builds everything under `<data-dir>/.restore-tmp` (same file system, also when the
//! directory is a volume mount) and moves it into place only when all of it has passed.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use rt_sec::{secret_aad, MasterKey, Sealed, SecretsBackend};
use rt_store::{META_MIGRATIONS, QUEUE_MIGRATIONS, STREAM_MIGRATIONS};
use rt_types::{ServiceId, TenantId};
use rusqlite::params;

use crate::archive::{check_mac, FileEntry, HashReader, Manifest, FORMAT, MAC, MANIFEST};
use crate::encrypt::{AgeIdentity, Input};
use crate::untrusted::{
    check_entry_type, check_node_db, check_service_db, copy_capped, create_private, open_untrusted, zstd_reader,
    BlobFiles, EntryKind, NodeDb, MAX_ARTIFACT_META_BYTES, MAX_MANIFEST_BYTES,
};
use crate::{BackupError, Result};

const TMP: &str = ".restore-tmp";
/// Free space a restore leaves on the disk.
const RESTORE_FLOOR: u64 = 64 << 20;

/// How an archive is authenticated.
pub enum Auth<'a> {
    /// `manifest.mac` must match under one of these keys.
    Keys(&'a [MasterKey]),
    /// `--no-auth`: the operator vouches for the file. The structural checks all still run.
    None,
}

fn truncated() -> BackupError {
    BackupError::new("truncated", "the archive ends before its manifest and MAC")
}

fn newest(m: &[(u32, &str)]) -> u32 {
    m.iter().map(|(v, _)| *v).max().unwrap_or(0)
}

/// First pass: every header is checked, data is skipped, and the two trailer files are read.
fn read_trailer<R: Read + Seek>(file: &mut R) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
    file.seek(SeekFrom::Start(0))?;
    let mut archive = tar::Archive::new(&mut *file);
    let mut manifest: Option<Vec<u8>> = None;
    let mut mac: Option<Vec<u8>> = None;
    let entries = archive.entries_with_seek().map_err(|_| truncated())?;
    for e in entries {
        let mut e = e.map_err(|_| truncated())?;
        check_entry_type(e.header().entry_type())?;
        let path = entry_path_ref(&e)?;
        if mac.is_some() {
            return Err(BackupError::new("unsafe_entry", "an entry follows the MAC"));
        }
        match path.as_str() {
            MANIFEST => {
                if manifest.is_some() {
                    return Err(BackupError::new("unsafe_entry", "the manifest appears twice"));
                }
                if e.size() > MAX_MANIFEST_BYTES {
                    return Err(BackupError::new("too_large", "the manifest is too large"));
                }
                let mut b = Vec::with_capacity(e.size() as usize);
                e.read_to_end(&mut b).map_err(|_| truncated())?;
                if b.len() as u64 != e.size() {
                    return Err(truncated());
                }
                manifest = Some(b);
            }
            MAC => {
                if manifest.is_none() {
                    return Err(BackupError::new("unsafe_entry", "the MAC precedes the manifest"));
                }
                if e.size() > 1024 {
                    return Err(BackupError::new("bad_mac", "manifest.mac is malformed"));
                }
                let mut b = Vec::new();
                e.read_to_end(&mut b).map_err(|_| truncated())?;
                mac = Some(b);
            }
            other => {
                if manifest.is_some() {
                    return Err(BackupError::new("unsafe_entry", "a data entry follows the manifest"));
                }
                EntryKind::parse(other)?;
            }
        }
    }
    Ok((manifest.ok_or_else(truncated)?, mac))
}

fn entry_path_ref<R: Read>(e: &tar::Entry<'_, R>) -> Result<String> {
    std::str::from_utf8(&e.path_bytes())
        .map(str::to_string)
        .map_err(|_| BackupError::new("unsafe_entry", "an entry path is not UTF-8"))
}

/// The manifest, authenticated and structurally checked; no data was looked at yet.
fn load_manifest<R: Read + Seek>(file: &mut R, auth: &Auth<'_>) -> Result<Manifest> {
    let (bytes, mac) = read_trailer(file)?;
    match (auth, &mac) {
        (Auth::Keys(_), None) => return Err(truncated()),
        (Auth::Keys(keys), Some(tag)) => check_mac(keys, &bytes, tag)?,
        (Auth::None, _) => {}
    }
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|_| BackupError::new("corrupt", "manifest.json is malformed"))?;
    if manifest.format != FORMAT {
        return Err(BackupError::new(
            "unsupported_format",
            format!("archive format {} is not known to this rt", manifest.format),
        ));
    }
    let mut seen = HashSet::with_capacity(manifest.files.len());
    for f in &manifest.files {
        let kind = EntryKind::parse(&f.path)?;
        if !seen.insert(f.path.as_str()) {
            return Err(BackupError::new("unsafe_entry", "the manifest lists a path twice"));
        }
        if kind.compressed() != f.raw_size.is_some() {
            return Err(BackupError::new("corrupt", "the manifest misdescribes an entry"));
        }
    }
    if !seen.contains("meta.db.zst") {
        return Err(BackupError::new("corrupt", "the archive has no meta.db"));
    }
    Ok(manifest)
}

/// Second pass: each data entry is listed in the manifest, once, and its bytes match. `open_out`
/// gives where the decoded bytes go. Decoding happens while hashing, on a stream, so nothing is
/// held in memory or written elsewhere; an output past the declared size stops the walk.
fn walk<R: Read + Seek, W: Write>(
    file: &mut R,
    manifest: &Manifest,
    mut open_out: impl FnMut(&EntryKind) -> Result<W>,
    mut done: impl FnMut(W, &FileEntry) -> Result<()>,
) -> Result<()> {
    let listed: HashMap<&str, &FileEntry> = manifest.files.iter().map(|f| (f.path.as_str(), f)).collect();
    let mut left: HashSet<&str> = listed.keys().copied().collect();
    file.seek(SeekFrom::Start(0))?;
    let mut archive = tar::Archive::new(&mut *file);
    for e in archive.entries().map_err(|_| truncated())? {
        let e = e.map_err(|_| truncated())?;
        check_entry_type(e.header().entry_type())?;
        let path = entry_path_ref(&e)?;
        if path == MANIFEST || path == MAC {
            continue;
        }
        let kind = EntryKind::parse(&path)?;
        let Some(&meta) = listed.get(path.as_str()) else {
            return Err(BackupError::new(
                "unsafe_entry",
                "the archive holds an entry its manifest does not list",
            ));
        };
        if !left.remove(path.as_str()) {
            return Err(BackupError::new("unsafe_entry", "an entry appears twice"));
        }
        if e.size() != meta.size {
            return Err(digest_mismatch());
        }
        let mut out = open_out(&kind)?;
        let mut hashed = HashReader::new(e);
        match meta.raw_size {
            Some(declared) => {
                let raw = decode_entry(&mut hashed, &mut out, declared)?;
                if raw != declared {
                    return Err(digest_mismatch());
                }
            }
            None => {
                let cap = match kind {
                    EntryKind::ArtifactMeta(_) => MAX_ARTIFACT_META_BYTES,
                    _ => meta.size,
                };
                copy_capped(&mut hashed, &mut out, cap)?;
            }
        }
        io::copy(&mut hashed, &mut io::sink())?; // what a decoder left unread still counts
        let (n, sha) = hashed.finish();
        if n != meta.size {
            return Err(truncated());
        }
        if !sha.eq_ignore_ascii_case(&meta.sha256) {
            return Err(digest_mismatch());
        }
        done(out, meta)?;
    }
    if !left.is_empty() {
        return Err(truncated());
    }
    Ok(())
}

/// A reader remembering that a read failed: `io::copy` does not say which side of it did.
struct ReadWatch<R> {
    inner: R,
    failed: bool,
}

impl<R> ReadWatch<R> {
    fn new(inner: R) -> Self {
        Self { inner, failed: false }
    }
}

impl<R: Read> Read for ReadWatch<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let r = self.inner.read(buf);
        // `io::copy` retries `Interrupted` itself.
        if r.as_ref().is_err_and(|e| e.kind() != io::ErrorKind::Interrupted) {
            self.failed = true;
        }
        r
    }
}

/// Decompress one entry into `out`. Only a failure of the decoder itself says the frame is bad; a
/// failure reading the archive or writing `out` stays the I/O error it is.
fn decode_entry(src: &mut impl Read, out: &mut impl Write, declared: u64) -> Result<u64> {
    let mut source = ReadWatch::new(src);
    let copied = {
        let mut dec = ReadWatch::new(zstd_reader(&mut source).map_err(|_| digest_mismatch())?);
        copy_capped(&mut dec, out, declared).map_err(|e| (e, dec.failed))
    };
    match copied {
        Ok(n) => Ok(n),
        Err((e, decoder_failed)) if decoder_failed && !source.failed => {
            debug_assert_eq!(e.code, "io_error");
            Err(digest_mismatch())
        }
        Err((e, _)) => Err(e),
    }
}

fn digest_mismatch() -> BackupError {
    BackupError::new("digest_mismatch", "an entry does not match its manifest digest")
}

/// What `rt backup verify` found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyReport {
    pub id: String,
    pub rt_version: String,
    pub finished_at: i64,
    pub files: u64,
    pub bytes: u64,
    pub authenticated: bool,
}

/// Check a whole archive into a hash sink: nothing is written anywhere.
pub fn verify_archive(path: &Path, auth: &Auth<'_>) -> Result<VerifyReport> {
    verify_archive_age(path, auth, None)
}

/// [`verify_archive`], for an archive that may be encrypted: it is decrypted on demand.
pub fn verify_archive_age(path: &Path, auth: &Auth<'_>, identity: Option<&AgeIdentity>) -> Result<VerifyReport> {
    let mut input = Input::open(path, identity)?;
    let checked = verify_input(&mut input, auth);
    checked.map_err(|e| input.name_error(e))
}

fn verify_input(input: &mut Input, auth: &Auth<'_>) -> Result<VerifyReport> {
    let manifest = load_manifest(input, auth)?;
    let mut bytes = 0u64;
    walk(
        input,
        &manifest,
        |_| Ok(io::sink()),
        |_, m| {
            bytes += m.size;
            Ok(())
        },
    )?;
    Ok(VerifyReport {
        id: manifest.id.clone(),
        rt_version: manifest.rt_version.clone(),
        finished_at: manifest.finished_at,
        files: manifest.files.len() as u64,
        bytes,
        authenticated: matches!(auth, Auth::Keys(_)),
    })
}

/// What a restore does besides copying files.
pub struct RestoreOptions<'a> {
    pub auth: Auth<'a>,
    /// Revoke keys created before this time (ms), for the credentials revoked after the snapshot.
    pub revoke_keys_before: Option<i64>,
    /// Reports which sealed secrets these keys cannot open.
    pub secrets: Option<&'a dyn SecretsBackend>,
    pub now_ms: i64,
}

/// What `rt backup restore` tells the operator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreReport {
    pub id: String,
    pub finished_at: i64,
    pub snapshot_age_ms: i64,
    pub active_keys: u64,
    pub revoked_keys: u64,
    pub services: u64,
    pub blobs: u64,
    pub secrets: u64,
    /// `(service, name)` of secrets the supplied master key does not open.
    pub unopened_secrets: Vec<(String, String)>,
    pub authenticated: bool,
}

fn ensure_empty(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        if name != TMP {
            return Err(BackupError::new(
                "not_empty",
                "the data directory is not empty; restore only into an empty one",
            ));
        }
    }
    Ok(())
}

/// Restore `archive` into the empty directory `data_dir`. On any failure the directory is left
/// empty, as it was found.
pub fn restore_archive(archive: &Path, data_dir: &Path, opts: &RestoreOptions<'_>) -> Result<RestoreReport> {
    restore_archive_age(archive, data_dir, opts, None)
}

/// [`restore_archive`], for an archive that may be encrypted. The MAC is still what authenticates
/// it: recipients are public, so anyone can encrypt to them.
pub fn restore_archive_age(
    archive: &Path,
    data_dir: &Path,
    opts: &RestoreOptions<'_>,
    identity: Option<&AgeIdentity>,
) -> Result<RestoreReport> {
    ensure_empty(data_dir)?;
    let mut input = Input::open(archive, identity)?;
    let tmp = data_dir.join(TMP);
    let _ = std::fs::remove_dir_all(&tmp);
    crate::create::private_dir(&tmp)?;
    let result = restore_into(&mut input, data_dir, &tmp, opts).map_err(|e| input.name_error(e));
    let _ = std::fs::remove_dir_all(&tmp);
    result
}

fn restore_into(file: &mut Input, data_dir: &Path, tmp: &Path, opts: &RestoreOptions<'_>) -> Result<RestoreReport> {
    let manifest = load_manifest(file, &opts.auth)?;
    // Sizes come from the manifest; a forged one is caught below, with at most this much written.
    let needed = manifest
        .files
        .iter()
        .fold(0u64, |a, f| a.saturating_add(f.raw_size.unwrap_or(f.size)));
    if rt_store::free_bytes(data_dir).is_some_and(|free| needed.saturating_add(RESTORE_FLOOR) > free) {
        return Err(BackupError::new(
            "insufficient_storage",
            "not enough free disk for the restored data",
        ));
    }

    // Blob files of each service, from the manifest; checked against the rows after extraction.
    let mut blobs: HashMap<ServiceId, BlobFiles> = HashMap::new();
    let mut service_dbs = Vec::new();
    let mut digests = Vec::new();
    for f in &manifest.files {
        match EntryKind::parse(&f.path)? {
            EntryKind::Blob(svc, name) => {
                blobs.entry(svc).or_default().insert(name, f.size);
            }
            EntryKind::ServiceDb(svc) => service_dbs.push(svc),
            EntryKind::ArtifactBlob(d) => digests.push(d),
            _ => {}
        }
    }

    let tmp_owned = tmp.to_path_buf();
    walk(
        file,
        &manifest,
        |kind| {
            let f = create_private(&tmp_owned, &kind.target())?;
            Ok(io::BufWriter::with_capacity(1 << 20, f))
        },
        |out, _| {
            let f = out.into_inner().map_err(|e| BackupError::from(e.into_error()))?;
            f.sync_all()?;
            Ok(())
        },
    )?;

    // Artifacts are named by their content.
    for d in &digests {
        let path = tmp.join(EntryKind::ArtifactBlob(*d).target());
        let mut hashed = HashReader::new(File::open(path)?);
        io::copy(&mut hashed, &mut io::sink())?;
        if hashed.finish().1 != d.hex() {
            return Err(BackupError::new(
                "digest_mismatch",
                "an artifact does not match its name",
            ));
        }
    }

    // Every database: integrity, version, and for services the blob rows.
    let newest_of = |db: NodeDb| match db {
        NodeDb::Meta => newest(META_MIGRATIONS),
        NodeDb::Queue => newest(QUEUE_MIGRATIONS),
        NodeDb::Streams => newest(STREAM_MIGRATIONS),
    };
    for db in NodeDb::ALL {
        let path = tmp.join(db.file_name());
        if path.exists() {
            check_node_db(&path, newest_of(db))?;
        }
    }
    let empty = BlobFiles::new();
    let mut blob_count = 0u64;
    for svc in &service_dbs {
        let path = tmp.join(EntryKind::ServiceDb(*svc).target());
        blob_count += check_service_db(&path, blobs.get(svc).unwrap_or(&empty))?;
        blobs.remove(svc);
    }
    if !blobs.is_empty() {
        return Err(BackupError::new(
            "unsafe_entry",
            "the archive holds blobs of a service with no database",
        ));
    }

    let mut report = finish_meta(&tmp.join(NodeDb::Meta.file_name()), &manifest, &service_dbs, opts)?;
    report.blobs = blob_count;

    // All good: move into place. A crash here leaves a partial directory that later restores refuse.
    for entry in std::fs::read_dir(tmp)? {
        let entry = entry?;
        std::fs::rename(entry.path(), data_dir.join(entry.file_name()))?;
    }
    File::open(data_dir)?.sync_all()?;
    Ok(report)
}

/// Bring the restored `meta.db` to the present: rows of runs that were in flight, the credentials
/// the operator revokes, and the `node.restored` event. Reports what the operator must know.
fn finish_meta(
    path: &Path,
    manifest: &Manifest,
    service_dbs: &[ServiceId],
    opts: &RestoreOptions<'_>,
) -> Result<RestoreReport> {
    let c = open_untrusted(path, true)?;
    // A service database of a service the snapshot does not know has no owner.
    for svc in service_dbs {
        let known: i64 = c.query_row("SELECT count(*) FROM services WHERE id = ?1", [svc.to_string()], |r| {
            r.get(0)
        })?;
        if known == 0 {
            return Err(BackupError::new(
                "unsafe_entry",
                "the archive holds a database of an unknown service",
            ));
        }
    }
    let services: i64 = c.query_row("SELECT count(*) FROM services", [], |r| r.get(0))?;
    let now = opts.now_ms;
    c.execute(
        "UPDATE backups SET state = 'failed', finished_at = ?1, local_path = NULL, \
         failure = '{\"code\":\"interrupted\",\"message\":\"the snapshot was taken during this backup\"}' \
         WHERE state IN ('running', 'uploading', 'verifying')",
        [now],
    )?;
    let revoked = match opts.revoke_keys_before {
        // A key minted by a revoked key dies with it, whatever its own age.
        Some(ts) => c.execute(
            "WITH RECURSIVE doomed(id) AS ( \
               SELECT id FROM api_keys WHERE created_at < ?2 \
               UNION SELECT k.id FROM api_keys k JOIN doomed d ON k.parent_key_id = d.id) \
             UPDATE api_keys SET revoked_at = ?1 WHERE revoked_at IS NULL AND id IN (SELECT id FROM doomed)",
            params![now, ts],
        )? as u64,
        None => 0,
    };
    let active: i64 = c.query_row(
        "SELECT count(*) FROM api_keys WHERE revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ?1)",
        [now],
        |r| r.get(0),
    )?;
    let (secrets, unopened) = check_secrets(&c, opts.secrets)?;
    let authenticated = matches!(opts.auth, Auth::Keys(_));
    let data = serde_json::json!({
        "backup_id": manifest.id,
        "snapshot_finished_at": manifest.finished_at,
        "rt_version": manifest.rt_version,
        "revoked_keys": revoked,
        "authenticated": authenticated,
    });
    c.execute(
        "INSERT INTO events (id, ts, tenant_id, service_id, kind, data) VALUES (?1, ?2, NULL, NULL, 'node.restored', ?3)",
        params![rt_types::EventId::new().to_string(), now, data.to_string()],
    )?;
    Ok(RestoreReport {
        id: manifest.id.clone(),
        finished_at: manifest.finished_at,
        snapshot_age_ms: now.saturating_sub(manifest.finished_at),
        active_keys: active.max(0) as u64,
        revoked_keys: revoked,
        services: services.max(0) as u64,
        blobs: 0,
        secrets,
        unopened_secrets: unopened,
        authenticated,
    })
}

fn check_secrets(
    c: &rusqlite::Connection,
    backend: Option<&dyn SecretsBackend>,
) -> Result<(u64, Vec<(String, String)>)> {
    let mut st = c.prepare(
        "SELECT s.tenant_id, x.service_id, x.name, x.sealed FROM secrets x JOIN services s ON s.id = x.service_id",
    )?;
    let mut rows = st.query([])?;
    let (mut total, mut failed) = (0u64, Vec::new());
    while let Some(r) = rows.next()? {
        total += 1;
        let (tenant, service, name, bytes): (String, String, String, Vec<u8>) =
            (r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?);
        let Some(backend) = backend else { continue };
        let opens = (|| {
            let t: TenantId = tenant.parse().ok()?;
            let s: ServiceId = service.parse().ok()?;
            let sealed = Sealed::from_bytes(&bytes).ok()?;
            backend.open(&secret_aad(t, s, &name), &sealed).ok()
        })();
        if opens.is_none() {
            failed.push((service, name));
        }
    }
    Ok((total, failed))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Failing(io::ErrorKind);

    impl Read for Failing {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(self.0.into())
        }
    }

    impl Write for Failing {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(self.0.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn frame(raw: &[u8]) -> Vec<u8> {
        zstd::encode_all(raw, 1).expect("encode")
    }

    fn code(r: Result<u64>) -> &'static str {
        r.expect_err("refused").code
    }

    #[test]
    fn a_good_frame_decodes() {
        let mut out = Vec::new();
        let n = decode_entry(&mut frame(b"hello").as_slice(), &mut out, 5).expect("decode");
        assert_eq!((n, out.as_slice()), (5, b"hello".as_slice()));
    }

    #[test]
    fn a_bad_or_cut_frame_is_a_digest_mismatch() {
        let bad = decode_entry(&mut b"not a zstd frame".as_slice(), &mut Vec::new(), 5);
        assert_eq!(code(bad), "digest_mismatch");
        let whole = frame(&[7u8; 4096]);
        let cut = decode_entry(&mut &whole[..whole.len() / 2], &mut Vec::new(), 4096);
        assert_eq!(code(cut), "digest_mismatch");
    }

    #[test]
    fn a_frame_past_its_declared_size_is_too_large() {
        assert_eq!(
            code(decode_entry(&mut frame(b"hello").as_slice(), &mut Vec::new(), 2)),
            "too_large"
        );
    }

    #[test]
    fn a_failing_disk_is_an_io_error_whatever_its_kind() {
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::InvalidData,
            io::ErrorKind::UnexpectedEof,
        ] {
            let read = decode_entry(&mut Failing(kind), &mut Vec::new(), 5);
            assert_eq!(code(read), "io_error", "read {kind:?}");
            let write = decode_entry(&mut frame(b"hello").as_slice(), &mut Failing(kind), 5);
            assert_eq!(code(write), "io_error", "write {kind:?}");
        }
    }
}
