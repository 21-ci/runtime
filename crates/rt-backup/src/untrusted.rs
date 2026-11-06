//! What an archive may contain. Every archive is hostile input, signed or not: it may come from a
//! bucket someone else can write. App import (R12, [`crate::app`]) uses this module too.
//!
//! - Tar entries: regular files only, with a path of an exact known shape. No link, no device, no
//!   `..`, no absolute path, no other name. A path is parsed into [`EntryKind`] and the target is
//!   rebuilt from the parsed parts, so nothing from the archive becomes a path segment unchecked.
//! - Files: created with `O_EXCL | O_NOFOLLOW`, mode 0600, under a directory the restore made.
//! - Decompression: capped by the size declared in the manifest, whose sum is checked against free
//!   disk first, and by a bounded zstd window.
//! - Databases: opened defensively (no triggers, no views, no trusted schema), `quick_check`ed,
//!   schema version at most this binary's. `_rt_blobs.file` rows must name a generated file name
//!   that exists in the archive at the recorded size, each exactly once, since a crafted row could
//!   otherwise read or delete a host file such as `master.key` through `Path::join`.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use rt_store::blob_file_name_ok;
use rt_types::{Digest, ServiceId};
use rusqlite::config::DbConfig;
use rusqlite::{Connection, OpenFlags};

use crate::{BackupError, Result};

/// `manifest.json` larger than this is refused.
pub const MAX_MANIFEST_BYTES: u64 = 256 << 20;
/// Largest `artifacts/<hex>/meta.json`.
pub const MAX_ARTIFACT_META_BYTES: u64 = 64 << 10;
/// Largest zstd window a decoder allocates (128 MiB; the encoder's level never goes above it).
pub const ZSTD_WINDOW_LOG_MAX: u32 = 27;

fn refuse(msg: impl Into<String>) -> BackupError {
    BackupError::new("unsafe_entry", msg)
}

/// The node database an entry holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeDb {
    Meta,
    Queue,
    Streams,
}

impl NodeDb {
    pub const ALL: [NodeDb; 3] = [NodeDb::Meta, NodeDb::Queue, NodeDb::Streams];

    pub fn file_name(self) -> &'static str {
        match self {
            NodeDb::Meta => "meta.db",
            NodeDb::Queue => "queue.db",
            NodeDb::Streams => "streams.db",
        }
    }
}

/// A tar path of a known shape.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EntryKind {
    NodeDb(NodeDb),
    ServiceDb(ServiceId),
    Blob(ServiceId, String),
    ArtifactBlob(Digest),
    ArtifactMeta(Digest),
}

fn service_id(s: &str) -> Result<ServiceId> {
    let id = ServiceId::from_str(s).map_err(|_| refuse("not a service id"))?;
    // The canonical spelling only: the name becomes a directory.
    if id.to_string() != s {
        return Err(refuse("not a canonical service id"));
    }
    Ok(id)
}

fn digest(s: &str) -> Result<Digest> {
    if s.len() != 64 || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(refuse("not an artifact digest"));
    }
    Digest::from_hex(s).map_err(|_| refuse("not an artifact digest"))
}

impl EntryKind {
    /// Parse a tar path. Anything but the exact shapes of the format is refused, which covers
    /// `..`, `.`, absolute paths, empty segments, backslashes and NUL.
    pub fn parse(path: &str) -> Result<Self> {
        let parts: Vec<&str> = path.split('/').collect();
        match parts.as_slice() {
            ["meta.db.zst"] => Ok(Self::NodeDb(NodeDb::Meta)),
            ["queue.db.zst"] => Ok(Self::NodeDb(NodeDb::Queue)),
            ["streams.db.zst"] => Ok(Self::NodeDb(NodeDb::Streams)),
            ["services", id, "data.db.zst"] => Ok(Self::ServiceDb(service_id(id)?)),
            ["blobs", id, "objects", file] if blob_file_name_ok(file) => {
                Ok(Self::Blob(service_id(id)?, (*file).to_string()))
            }
            ["artifacts", hex, "blob"] => Ok(Self::ArtifactBlob(digest(hex)?)),
            ["artifacts", hex, "meta.json"] => Ok(Self::ArtifactMeta(digest(hex)?)),
            _ => Err(refuse("an entry has a path outside the format")),
        }
    }

    /// The path in the archive.
    pub fn archive_path(&self) -> String {
        match self {
            Self::NodeDb(db) => format!("{}.zst", db.file_name()),
            Self::ServiceDb(id) => format!("services/{id}/data.db.zst"),
            Self::Blob(id, file) => format!("blobs/{id}/objects/{file}"),
            Self::ArtifactBlob(d) => format!("artifacts/{}/blob", d.hex()),
            Self::ArtifactMeta(d) => format!("artifacts/{}/meta.json", d.hex()),
        }
    }

    /// The path under a data directory, rebuilt from the parsed parts.
    pub fn target(&self) -> PathBuf {
        match self {
            Self::NodeDb(db) => PathBuf::from(db.file_name()),
            Self::ServiceDb(id) => Path::new("services").join(id.to_string()).join("data.db"),
            Self::Blob(id, file) => Path::new("blobs").join(id.to_string()).join("objects").join(file),
            Self::ArtifactBlob(d) => Path::new("artifacts").join(d.hex()).join("blob"),
            Self::ArtifactMeta(d) => Path::new("artifacts").join(d.hex()).join("meta.json"),
        }
    }

    pub fn compressed(&self) -> bool {
        matches!(self, Self::NodeDb(_) | Self::ServiceDb(_))
    }
}

/// Refuse every tar entry type but a regular file. Links, devices, FIFOs, sparse files and the like
/// have no place in the format; the tar reader resolves long names and pax paths before this.
pub fn check_entry_type(t: tar::EntryType) -> Result<()> {
    if t == tar::EntryType::Regular {
        Ok(())
    } else {
        Err(refuse("an entry is not a regular file"))
    }
}

/// Copy at most `cap` bytes; more is `too_large`. Returns the bytes copied.
pub fn copy_capped(r: &mut impl Read, w: &mut impl Write, cap: u64) -> Result<u64> {
    let mut limited = r.take(cap.saturating_add(1));
    let n = io::copy(&mut limited, w)?;
    if n > cap {
        return Err(BackupError::new(
            "too_large",
            "an entry decompresses past its declared size",
        ));
    }
    Ok(n)
}

/// A zstd decoder of an untrusted frame with a bounded window.
pub fn zstd_reader<'a, R: Read + 'a>(r: R) -> Result<zstd::stream::read::Decoder<'a, io::BufReader<R>>> {
    let mut d = zstd::stream::read::Decoder::new(r)?;
    d.window_log_max(ZSTD_WINDOW_LOG_MAX)?;
    Ok(d)
}

/// Create `rel` under `root` as a new private file, refusing to follow or reuse anything there.
pub fn create_private(root: &Path, rel: &Path) -> Result<File> {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)?;
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| match e.kind() {
            io::ErrorKind::AlreadyExists => refuse("an entry appears twice"),
            _ => e.into(),
        })
}

/// A connection to a database that came out of an archive: nothing in its schema runs.
pub fn open_untrusted(path: &Path, writable: bool) -> Result<Connection> {
    let flags = if writable {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    } else {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    } | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let c = Connection::open_with_flags(path, flags)?;
    c.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    c.set_db_config(DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false)?;
    c.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false)?;
    c.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_VIEW, false)?;
    c.execute_batch("PRAGMA cell_size_check = ON; PRAGMA mmap_size = 0")?;
    Ok(c)
}

/// `PRAGMA quick_check` on a live connection.
pub fn quick_check(c: &Connection) -> Result<()> {
    let verdict: String = c.query_row("PRAGMA quick_check(1)", [], |r| r.get(0))?;
    if verdict != "ok" {
        return Err(BackupError::new("corrupt", "a database failed its integrity check"));
    }
    Ok(())
}

/// Open and check a node database: integrity, schema version at most `newest`, no trigger or view
/// (no migration makes one, and none may wait for the store to fire it).
pub fn check_node_db(path: &Path, newest: u32) -> Result<u32> {
    let c = open_untrusted(path, false)?;
    quick_check(&c)?;
    let version: u32 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version > newest {
        return Err(BackupError::new(
            "schema_too_new",
            "the archive comes from a newer version of rt; install that version first",
        ));
    }
    let active: i64 = c.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type IN ('trigger', 'view')",
        [],
        |r| r.get(0),
    )?;
    if active > 0 {
        return Err(refuse("a node database holds a trigger or view"));
    }
    Ok(version)
}

/// Blob files an archive holds for one service: name to size.
pub type BlobFiles = HashMap<String, u64>;

/// Check a service database against the blob files of its archive and hand back how many rows it
/// has. Guest schema objects may stay (guests can make them), but none may touch `_rt_*`: the host
/// writes those tables. Every `_rt_blobs.file` is a generated name, once, of a file in `files` at
/// the recorded size, and every file in `files` is referenced.
pub fn check_service_db(path: &Path, files: &BlobFiles) -> Result<u64> {
    let c = open_untrusted(path, false)?;
    quick_check(&c)?;
    let hostile: i64 = c.query_row(
        "SELECT count(*) FROM sqlite_master WHERE \
         (type = 'view' AND name LIKE '\\_rt\\_%' ESCAPE '\\') OR \
         (type = 'trigger' AND tbl_name LIKE '\\_rt\\_%' ESCAPE '\\') OR \
         (name = '_rt_blobs' AND type <> 'table')",
        [],
        |r| r.get(0),
    )?;
    if hostile > 0 {
        return Err(refuse("a service database holds a view or trigger on a runtime table"));
    }
    let has_table: i64 = c.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_rt_blobs'",
        [],
        |r| r.get(0),
    )?;
    let mut seen: HashSet<String> = HashSet::with_capacity(files.len());
    if has_table > 0 {
        let mut st = c.prepare("SELECT file, size FROM _rt_blobs")?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let file = row
                .get::<_, String>(0)
                .map_err(|_| refuse("a blob row has no file name"))?;
            let size = row.get::<_, i64>(1).map_err(|_| refuse("a blob row has no size"))?;
            if !blob_file_name_ok(&file) {
                return Err(refuse("a blob row names a file the store would not have made"));
            }
            match files.get(&file) {
                Some(&actual) if u64::try_from(size).ok() == Some(actual) => {}
                Some(_) => return Err(refuse("a blob row records the wrong size")),
                None => return Err(refuse("a blob row names a file the archive does not hold")),
            }
            if !seen.insert(file) {
                return Err(refuse("two blob rows share one file"));
            }
        }
    }
    if seen.len() != files.len() {
        return Err(refuse("the archive holds a blob file no row references"));
    }
    Ok(seen.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SVC: &str = "svc_01HZZZZZZZZZZZZZZZZZZZZZZZ";

    #[test]
    fn the_known_shapes_round_trip_to_their_target() {
        let hex = "ab".repeat(32);
        for (path, target) in [
            ("meta.db.zst".to_string(), "meta.db".to_string()),
            ("streams.db.zst".to_string(), "streams.db".to_string()),
            (format!("services/{SVC}/data.db.zst"), format!("services/{SVC}/data.db")),
            (
                format!("blobs/{SVC}/objects/1a-2b-3c"),
                format!("blobs/{SVC}/objects/1a-2b-3c"),
            ),
            (format!("artifacts/{hex}/blob"), format!("artifacts/{hex}/blob")),
            (
                format!("artifacts/{hex}/meta.json"),
                format!("artifacts/{hex}/meta.json"),
            ),
        ] {
            let k = EntryKind::parse(&path).expect(&path);
            assert_eq!(k.archive_path(), path);
            assert_eq!(k.target(), PathBuf::from(target));
        }
    }

    /// No path outside the format gets through, whatever it is dressed as.
    #[test]
    fn hostile_paths_are_refused() {
        let hex = "ab".repeat(32);
        for path in [
            "",
            "/etc/passwd",
            "../master.key",
            "./meta.db.zst",
            "meta.db",
            "master.key",
            "manifest.json/../meta.db.zst",
            "services//data.db.zst",
            &format!("services/../{SVC}/data.db.zst"),
            &format!("services/{SVC}/../../master.key"),
            &format!("services/{SVC}/data.db"),
            "services/svc_NOTAULID/data.db.zst",
            &format!("blobs/{SVC}/objects/../../../master.key"),
            &format!("blobs/{SVC}/objects//etc/passwd"),
            &format!("blobs/{SVC}/objects/1A-2B-3C"),
            &format!("blobs/{SVC}/objects/.."),
            &format!("blobs/{SVC}/objects/a\\b-1-2"),
            &format!("blobs/{SVC}/tmp/1a-2b-3c"),
            &format!("artifacts/{}/blob", "AB".repeat(32)),
            &format!("artifacts/{hex}/../../master.key"),
            &format!("artifacts/{hex}/other"),
            "artifacts/ab/blob",
            "cache/x",
            "meta.db.zst\0",
        ] {
            assert_eq!(EntryKind::parse(path).unwrap_err().code, "unsafe_entry", "{path:?}");
        }
    }

    #[test]
    fn only_regular_entries_are_allowed() {
        assert!(check_entry_type(tar::EntryType::Regular).is_ok());
        for t in [
            tar::EntryType::Symlink,
            tar::EntryType::Link,
            tar::EntryType::Char,
            tar::EntryType::Block,
            tar::EntryType::Fifo,
            tar::EntryType::Directory,
            tar::EntryType::GNUSparse,
        ] {
            assert_eq!(check_entry_type(t).unwrap_err().code, "unsafe_entry", "{t:?}");
        }
    }

    #[test]
    fn a_copy_stops_at_its_cap() {
        let mut out = Vec::new();
        assert_eq!(copy_capped(&mut &b"abc"[..], &mut out, 3), Ok(3));
        let e = copy_capped(&mut &b"abcd"[..], &mut Vec::new(), 3).unwrap_err();
        assert_eq!(e.code, "too_large");
    }

    #[test]
    fn create_private_does_not_follow_or_reuse() {
        let d = tempfile::tempdir().expect("dir");
        let outside = d.path().join("outside");
        std::fs::write(&outside, "keep").expect("write");
        std::os::unix::fs::symlink(&outside, d.path().join("link")).expect("symlink");
        assert!(create_private(d.path(), Path::new("link")).is_err());
        assert_eq!(std::fs::read(&outside).expect("read"), b"keep");
        create_private(d.path(), Path::new("a/b")).expect("first");
        assert_eq!(
            create_private(d.path(), Path::new("a/b")).unwrap_err().code,
            "unsafe_entry"
        );
    }

    fn service_db(dir: &Path, sql: &str) -> PathBuf {
        let p = dir.join("data.db");
        let c = Connection::open(&p).expect("open");
        c.execute_batch(
            "CREATE TABLE _rt_blobs (key BLOB PRIMARY KEY, size INTEGER NOT NULL, content_type TEXT, file TEXT NOT NULL);",
        )
        .expect("schema");
        c.execute_batch(sql).expect("sql");
        p
    }

    fn files(items: &[(&str, u64)]) -> BlobFiles {
        items.iter().map(|(n, s)| ((*n).to_string(), *s)).collect()
    }

    /// `_rt_blobs.file` is the way a crafted archive would reach `master.key`.
    #[test]
    fn crafted_blob_rows_are_refused() {
        for (row, have) in [
            ("'/data/master.key'", vec![]),
            ("'../../../master.key'", vec![]),
            ("'a/b'", vec![]),
            ("''", vec![]),
            ("'1a-2b-3c'", vec![]),                // not in the archive
            ("X'2f6574632f706173737764'", vec![]), // a BLOB, not a name
        ] {
            let d = tempfile::tempdir().expect("dir");
            let p = service_db(
                d.path(),
                &format!("INSERT INTO _rt_blobs VALUES (X'01', 1, NULL, {row});"),
            );
            let e = check_service_db(&p, &files(&have)).unwrap_err();
            assert_eq!(e.code, "unsafe_entry", "{row}");
        }
    }

    #[test]
    fn blob_rows_must_match_the_files_exactly() {
        let d = tempfile::tempdir().expect("dir");
        let p = service_db(d.path(), "INSERT INTO _rt_blobs VALUES (X'01', 3, NULL, '1a-2b-3c');");
        assert_eq!(check_service_db(&p, &files(&[("1a-2b-3c", 3)])), Ok(1));
        // wrong size, unreferenced file
        assert!(check_service_db(&p, &files(&[("1a-2b-3c", 4)])).is_err());
        assert!(check_service_db(&p, &files(&[("1a-2b-3c", 3), ("4d-5e-6f", 1)])).is_err());
        // the same file twice: deleting one blob would unlink the other's bytes
        let d = tempfile::tempdir().expect("dir");
        let p = service_db(
            d.path(),
            "INSERT INTO _rt_blobs VALUES (X'01', 3, NULL, '1a-2b-3c'), (X'02', 3, NULL, '1a-2b-3c');",
        );
        assert!(check_service_db(&p, &files(&[("1a-2b-3c", 3)])).is_err());
    }

    #[test]
    fn views_and_triggers_on_runtime_tables_are_refused() {
        for sql in [
            "CREATE TRIGGER x AFTER DELETE ON _rt_blobs BEGIN SELECT 1; END;",
            "CREATE VIEW _rt_other AS SELECT 1;",
        ] {
            let d = tempfile::tempdir().expect("dir");
            let p = service_db(d.path(), sql);
            assert_eq!(check_service_db(&p, &files(&[])).unwrap_err().code, "unsafe_entry");
        }
        // a guest's own objects are fine
        let d = tempfile::tempdir().expect("dir");
        let p = service_db(
            d.path(),
            "CREATE TABLE t (a); CREATE VIEW v AS SELECT a FROM t; CREATE TRIGGER g AFTER INSERT ON t BEGIN SELECT 1; END;",
        );
        assert_eq!(check_service_db(&p, &files(&[])), Ok(0));
    }

    #[test]
    fn a_node_db_is_checked_for_version_and_active_schema() {
        let d = tempfile::tempdir().expect("dir");
        let p = d.path().join("n.db");
        let c = Connection::open(&p).expect("open");
        c.execute_batch("PRAGMA user_version = 3; CREATE TABLE t (a);")
            .expect("sql");
        drop(c);
        assert_eq!(check_node_db(&p, 3), Ok(3));
        assert_eq!(check_node_db(&p, 2).unwrap_err().code, "schema_too_new");
        let c = Connection::open(&p).expect("open");
        c.execute_batch("CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT 1; END;")
            .expect("sql");
        drop(c);
        assert_eq!(check_node_db(&p, 3).unwrap_err().code, "unsafe_entry");
    }

    #[test]
    fn a_damaged_database_does_not_pass() {
        let d = tempfile::tempdir().expect("dir");
        let p = d.path().join("n.db");
        std::fs::write(&p, b"this is not a database at all, in any page size").expect("write");
        assert_eq!(check_node_db(&p, 3).unwrap_err().code, "corrupt");
    }
}
