//! App bundles (R12): `app-<slug>-<ts>.tar.zst`, one service moved between tenants or nodes.
//!
//! A bundle holds `manifest.json` (first), `data.db`, `blobs/<generated name>` and
//! `sources/<version>`. Never hosts, key material, logs, queue or stream contents, sealed secrets
//! or compiled artifacts: secrets travel by name and are entered again, and the importer rebuilds
//! every version from its source.
//!
//! [`read_bundle`] treats every byte as hostile. It leaves a staging directory the store may
//! install, or refuses with a stable code; it runs nothing from the bundle.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::untrusted::{self, check_entry_type, copy_capped, create_private, BlobFiles};
use crate::{BackupError, Result};

pub const FORMAT: u32 = 1;
/// `manifest.json` larger than this is refused.
pub const MAX_APP_MANIFEST_BYTES: u64 = 16 << 20;
/// Declared bytes per compressed byte, above [`RATIO_FREE_BYTES`]. A real SQLite file and its blobs
/// stay far below it; zstd itself reaches tens of thousands on zeros.
pub const MAX_RATIO: u64 = 200;
/// Small bundles compress however they like.
pub const RATIO_FREE_BYTES: u64 = 64 << 20;
pub const MAX_VERSIONS: usize = 64;
pub const MAX_SCHEDULES: usize = 256;
pub const MAX_ENV: usize = 4096;
pub const MAX_SECRET_NAMES: usize = 4096;
pub const MAX_BLOBS: usize = 100_000;
/// What one entry weighs in the ratio check, whatever its size: it is a file to create and a row.
const ENTRY_WEIGHT: u64 = 512;
const DISK_FLOOR: u64 = 64 << 20;
/// Our own encoder's level: its window stays within what [`untrusted::zstd_reader`] accepts.
const ZSTD_LEVEL: i32 = 3;

fn refuse(msg: impl Into<String>) -> BackupError {
    BackupError::new("unsafe_entry", msg)
}

fn bad_manifest(msg: impl Into<String>) -> BackupError {
    BackupError::new("bad_manifest", msg)
}

/// The service's settings. An allowlist: `tenant`, `project`, `environment`, `scratch`, hosts or
/// anything else in a manifest is refused, and each block goes through normal create validation.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppService {
    pub runtime: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wasm: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deno: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<Value>,
    #[serde(rename = "async", default, skip_serializing_if = "Option::is_none")]
    pub async_invoke: Option<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppFile {
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppBlob {
    pub file: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppVersion {
    pub number: u32,
    pub size: u64,
    pub sha256: String,
    pub content_type: String,
    #[serde(default)]
    pub live: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

/// `manifest.json`. Unknown fields are refused at every level.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppManifest {
    pub format: u32,
    pub rt_version: String,
    pub exported_at: i64,
    /// The exporter's slug; the importer names its own.
    pub slug: String,
    pub service: AppService,
    /// May hold secrets someone put in env: export is `data:admin` and audited.
    pub env: BTreeMap<String, String>,
    /// Names only. They have to be entered again.
    pub secret_names: Vec<String>,
    pub schedules: Vec<Value>,
    pub versions: Vec<AppVersion>,
    pub data: AppFile,
    pub blobs: Vec<AppBlob>,
}

/// A tar path of a known shape.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum AppEntry {
    Data,
    Blob(String),
    Source(u32),
}

impl AppEntry {
    /// Anything but the exact shapes is refused, which covers `..`, absolute paths, empty segments.
    fn parse(path: &str) -> Result<Self> {
        let parts: Vec<&str> = path.split('/').collect();
        match parts.as_slice() {
            ["data.db"] => Ok(Self::Data),
            ["blobs", file] if rt_store::blob_file_name_ok(file) => Ok(Self::Blob((*file).to_string())),
            ["sources", n] => {
                let number = n
                    .parse::<u32>()
                    .map_err(|_| refuse("a source entry has a bad version"))?;
                if number.to_string() != *n {
                    return Err(refuse("a source entry has a bad version"));
                }
                Ok(Self::Source(number))
            }
            _ => Err(refuse("an entry has a path outside the format")),
        }
    }

    fn archive_path(&self) -> String {
        match self {
            Self::Data => "data.db".into(),
            Self::Blob(f) => format!("blobs/{f}"),
            Self::Source(n) => format!("sources/{n}"),
        }
    }
}

fn hex_of(d: impl AsRef<[u8]>) -> String {
    hex::encode(d)
}

fn file_digest(path: &Path) -> Result<AppFile> {
    let mut h = HashWriter::default();
    let size = io::copy(&mut File::open(path)?, &mut h)?;
    Ok(AppFile {
        size,
        sha256: hex_of(h.0.finalize()),
    })
}

#[derive(Default)]
struct HashWriter(Sha256);

impl Write for HashWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.update(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct TeeWriter<'a>(&'a mut Sha256, &'a mut File);

impl Write for TeeWriter<'_> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let n = self.1.write(b)?;
        self.0.update(&b[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.1.flush()
    }
}

fn append(b: &mut tar::Builder<impl Write>, path: &str, size: u64, r: impl Read) -> Result<()> {
    let mut h = tar::Header::new_gnu();
    h.set_size(size);
    h.set_mode(0o600);
    h.set_entry_type(tar::EntryType::Regular);
    b.append_data(&mut h, path, r)?;
    Ok(())
}

/// Write the bundle at `out`, which appears only once whole, from a work directory holding
/// `data.db`, `objects/<generated name>` and `sources/<version>`. Sizes and digests of `manifest`
/// are filled in from the files. Returns the bundle's size and SHA-256.
pub fn write_bundle(work: &Path, mut manifest: AppManifest, out: &Path) -> Result<AppFile> {
    manifest.data = file_digest(&work.join("data.db"))?;
    manifest.blobs.clear();
    match std::fs::read_dir(work.join("objects")) {
        Ok(dir) => {
            for e in dir {
                let e = e?;
                let file = e
                    .file_name()
                    .into_string()
                    .map_err(|_| refuse("a blob file has a bad name"))?;
                let d = file_digest(&e.path())?;
                manifest.blobs.push(AppBlob {
                    file,
                    size: d.size,
                    sha256: d.sha256,
                });
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    manifest.blobs.sort_by(|a, b| a.file.cmp(&b.file));
    for v in &mut manifest.versions {
        let d = file_digest(&work.join("sources").join(v.number.to_string()))?;
        v.size = d.size;
        v.sha256 = d.sha256;
    }

    let part = out.with_extension("part");
    let file = create_private(Path::new(""), &part)?;
    let mut tar = tar::Builder::new(zstd::stream::write::Encoder::new(file, ZSTD_LEVEL)?);
    let json = serde_json::to_vec(&manifest).map_err(|_| bad_manifest("the manifest does not serialize"))?;
    append(&mut tar, "manifest.json", json.len() as u64, &json[..])?;
    append(
        &mut tar,
        "data.db",
        manifest.data.size,
        File::open(work.join("data.db"))?,
    )?;
    for b in &manifest.blobs {
        append(
            &mut tar,
            &format!("blobs/{}", b.file),
            b.size,
            File::open(work.join("objects").join(&b.file))?,
        )?;
    }
    for v in &manifest.versions {
        append(
            &mut tar,
            &format!("sources/{}", v.number),
            v.size,
            File::open(work.join("sources").join(v.number.to_string()))?,
        )?;
    }
    let file = tar.into_inner()?.finish()?;
    file.sync_all()?;
    std::fs::rename(&part, out)?;
    file_digest(out)
}

/// What a reader will unpack.
#[derive(Clone, Copy, Debug)]
pub struct ReadLimits {
    pub max_unpacked: u64,
}

/// A bundle that has passed every check, unpacked under the staging directory.
#[derive(Debug)]
pub struct StagedBundle {
    pub manifest: AppManifest,
    pub db: PathBuf,
    pub objects: PathBuf,
    pub sources: PathBuf,
}

impl StagedBundle {
    /// Recomputed from the files: the bytes that count against the tenant's storage.
    pub fn unpacked_data_bytes(&self) -> u64 {
        self.manifest
            .blobs
            .iter()
            .fold(self.manifest.data.size, |sum, b| sum.saturating_add(b.size))
    }
}

fn hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn check_manifest(m: &AppManifest) -> Result<()> {
    if m.format != FORMAT {
        return Err(bad_manifest("this bundle has an unknown format"));
    }
    if m.versions.len() > MAX_VERSIONS
        || m.schedules.len() > MAX_SCHEDULES
        || m.env.len() > MAX_ENV
        || m.secret_names.len() > MAX_SECRET_NAMES
        || m.blobs.len() > MAX_BLOBS
    {
        return Err(bad_manifest("the manifest lists too much"));
    }
    let name_ok = |s: &str| {
        !s.is_empty()
            && s.len() <= 256
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    };
    if !m.secret_names.iter().all(|n| name_ok(n)) {
        return Err(bad_manifest("a secret name is not one"));
    }
    if m.versions.iter().filter(|v| v.live).count() > 1 {
        return Err(bad_manifest("two versions are live"));
    }
    if !m
        .versions
        .iter()
        .all(|v| hex64(&v.sha256) && v.content_type.len() <= 256)
        || !m.blobs.iter().all(|b| hex64(&b.sha256))
        || !hex64(&m.data.sha256)
    {
        return Err(bad_manifest("a digest is malformed"));
    }
    Ok(())
}

fn expected(m: &AppManifest) -> Result<HashMap<AppEntry, AppFile>> {
    let mut want = HashMap::with_capacity(1 + m.blobs.len() + m.versions.len());
    want.insert(AppEntry::Data, m.data.clone());
    for b in &m.blobs {
        if !rt_store::blob_file_name_ok(&b.file) {
            return Err(refuse("the manifest names a blob file the store would not have made"));
        }
        let f = AppFile {
            size: b.size,
            sha256: b.sha256.clone(),
        };
        if want.insert(AppEntry::Blob(b.file.clone()), f).is_some() {
            return Err(refuse("the manifest lists a blob twice"));
        }
    }
    for v in &m.versions {
        let f = AppFile {
            size: v.size,
            sha256: v.sha256.clone(),
        };
        if want.insert(AppEntry::Source(v.number), f).is_some() {
            return Err(refuse("the manifest lists a version twice"));
        }
    }
    Ok(want)
}

/// Declared sizes against the caps, the compressed size and the free disk, before anything is read.
fn check_sizes(want: &HashMap<AppEntry, AppFile>, compressed: u64, limits: ReadLimits, staging: &Path) -> Result<()> {
    let too_large = |m: &str| BackupError::new("too_large", m);
    let total = want.values().fold(0u64, |sum, f| sum.saturating_add(f.size));
    if total > limits.max_unpacked {
        return Err(too_large("this bundle unpacks past the import limit"));
    }
    let weighed = total.saturating_add((want.len() as u64).saturating_mul(ENTRY_WEIGHT));
    if weighed > RATIO_FREE_BYTES && weighed / MAX_RATIO > compressed {
        return Err(too_large("this bundle decompresses too far for its size"));
    }
    if rt_store::free_bytes(staging).is_some_and(|free| total.saturating_add(DISK_FLOOR) > free) {
        return Err(BackupError::new(
            "insufficient_storage",
            "not enough free disk to unpack this bundle",
        ));
    }
    Ok(())
}

/// The guest schema and the runtime tables of an unpacked `data.db`, on top of
/// [`untrusted::check_service_db`].
fn check_data_db(db: &Path, m: &AppManifest) -> Result<()> {
    let files: BlobFiles = m.blobs.iter().map(|b| (b.file.clone(), b.size)).collect();
    untrusted::check_service_db(db, &files)?;
    let c: Connection = untrusted::open_untrusted(db, false)?;
    let problem = rt_store::import_check::schema_problem(&c)
        .map_err(|_| BackupError::new("corrupt", "the database's schema cannot be read"))?;
    if let Some(p) = problem {
        return Err(refuse(p));
    }
    Ok(())
}

/// What may not arrive with the data: a timer would wake guest code the importer has not looked at.
/// Triggers are off on this connection, so these writes fire nothing. `_rt_migrations` stays.
fn scrub(db: &Path) -> Result<()> {
    let c = untrusted::open_untrusted(db, true)?;
    c.execute_batch("DELETE FROM _rt_durable_alarm").or_else(|e| match &e {
        // An old file without the table: it is created at first open.
        rusqlite::Error::SqliteFailure(_, Some(m)) if m.contains("no such table") => Ok(()),
        _ => Err(e),
    })?;
    // Outbox rows would publish into the importer's topic; the importer enables events itself.
    rt_store::data_events::strip(&c).map_err(|_| BackupError::new("corrupt", "the database cannot be cleaned"))?;
    Ok(())
}

/// Unpack and check a bundle into the new, private directory `staging`. Nothing from the bundle
/// runs: no trigger, no view, no code; a source is only a file here.
pub fn read_bundle(bundle: &Path, staging: &Path, limits: ReadLimits) -> Result<StagedBundle> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(staging)?;
    let compressed = std::fs::metadata(bundle)?.len();
    let reader = untrusted::zstd_reader(File::open(bundle)?)?;
    let mut archive = tar::Archive::new(reader);
    // Raw: a long-name or pax entry is one more entry to refuse, not a header read into memory.
    let mut entries = archive.entries()?.raw(true);

    let mut first = entries.next().ok_or_else(|| refuse("the bundle is empty"))??;
    check_entry_type(first.header().entry_type())?;
    if &*first.path_bytes() != b"manifest.json" {
        return Err(refuse("the manifest must come first"));
    }
    let mut json = Vec::new();
    copy_capped(&mut first, &mut json, MAX_APP_MANIFEST_BYTES)?;
    drop(first);
    let manifest: AppManifest =
        serde_json::from_slice(&json).map_err(|_| bad_manifest("this manifest is not valid"))?;
    check_manifest(&manifest)?;
    let mut want = expected(&manifest)?;
    check_sizes(&want, compressed, limits, staging)?;

    let mut seen = HashSet::with_capacity(want.len());
    for entry in entries {
        let mut entry = entry?;
        check_entry_type(entry.header().entry_type())?;
        let path = std::str::from_utf8(&entry.path_bytes())
            .map_err(|_| refuse("an entry's path is not text"))?
            .to_string();
        let kind = AppEntry::parse(&path)?;
        if seen.contains(&kind) {
            return Err(refuse("an entry appears twice"));
        }
        let Some(file) = want.remove(&kind) else {
            return Err(refuse("an entry is not in the manifest"));
        };
        seen.insert(kind.clone());
        let rel = match &kind {
            AppEntry::Data => PathBuf::from("data.db"),
            AppEntry::Blob(f) => Path::new("objects").join(f),
            AppEntry::Source(n) => Path::new("sources").join(n.to_string()),
        };
        debug_assert_eq!(kind.archive_path(), path);
        let mut out = create_private(staging, &rel)?;
        let mut hash = Sha256::new();
        let n = copy_capped(&mut entry, &mut TeeWriter(&mut hash, &mut out), file.size)?;
        if n != file.size || hex_of(hash.finalize()) != file.sha256 {
            return Err(refuse("an entry does not match the manifest"));
        }
    }
    if !want.is_empty() {
        return Err(refuse("the bundle is missing an entry its manifest lists"));
    }
    let db = staging.join("data.db");
    check_data_db(&db, &manifest)?;
    scrub(&db)?;
    Ok(StagedBundle {
        manifest,
        db,
        objects: staging.join("objects"),
        sources: staging.join("sources"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(n: usize) -> HashMap<AppEntry, AppFile> {
        (0..n)
            .map(|i| {
                let f = AppFile {
                    size: 0,
                    sha256: String::new(),
                };
                (AppEntry::Blob(i.to_string()), f)
            })
            .collect()
    }

    /// M10: zero-byte entries cost inodes and rows; they count even with no declared bytes.
    #[test]
    fn an_inode_bomb_is_stopped_by_the_ratio() {
        let dir = tempfile::tempdir().expect("dir");
        let limits = ReadLimits { max_unpacked: 1 << 40 };
        let e = check_sizes(&entries(200_000), 1 << 10, limits, dir.path()).unwrap_err();
        assert_eq!(e.code, "too_large");
        assert!(check_sizes(&entries(1000), 1 << 10, limits, dir.path()).is_ok());
    }
}
