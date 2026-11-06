//! The archive format, version 1.
//!
//! A tar of regular files, `manifest.json` next to last and `manifest.mac` last:
//!
//! ```text
//! meta.db.zst  queue.db.zst  streams.db.zst      node databases, one zstd frame each
//! services/<id>/data.db.zst                      per-service databases
//! blobs/<id>/objects/<file>                      referenced blob files, as stored
//! artifacts/<sha256 hex>/{blob,meta.json}        referenced artifacts, as stored
//! manifest.json                                  format, versions, per-file size and sha256
//! manifest.mac                                   hex HMAC-SHA256(manifest.json), HKDF `rt/backup-mac/v1`
//! ```
//!
//! Each database is one consistent commit. There is no point in time across databases: see
//! [`Manifest::consistency`]. A later format can add entries (`static/`, `objects/<sha256>`); a
//! reader refuses a format it does not know.

use std::io::{self, Read, Write};

use hmac::{Hmac, Mac};
use rt_sec::{purpose, MasterKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BackupError, Result};

pub const FORMAT: u32 = 1;
pub const MANIFEST: &str = "manifest.json";
pub const MAC: &str = "manifest.mac";
/// The file name of a backup's archive.
pub fn archive_name(id: &str) -> String {
    format!("rt-backup-{id}.tar")
}

/// The file name of an archive encrypted with `age`.
pub fn encrypted_archive_name(id: &str) -> String {
    format!("rt-backup-{id}.tar.age")
}

/// Schema versions of the node databases in the archive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVersions {
    pub meta: u32,
    pub queue: u32,
    pub streams: u32,
}

/// One data entry of the archive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    /// Bytes of the entry in the tar.
    pub size: u64,
    /// sha256 of those bytes (hex).
    pub sha256: String,
    /// Bytes after decompression, for a `.zst` entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub id: String,
    pub rt_version: String,
    pub schema: SchemaVersions,
    /// The snapshot window: each database was copied somewhere inside it.
    pub started_at: i64,
    pub finished_at: i64,
    /// Always `per_database`: queue, stream and data state can differ by the length of the window.
    pub consistency: String,
    /// Blob rows dropped because the service was deleted during the backup.
    #[serde(default)]
    pub dropped_blob_rows: u64,
    pub files: Vec<FileEntry>,
}

type HmacSha256 = Hmac<Sha256>;

fn mac_key(master: &MasterKey) -> HmacSha256 {
    let key = master.derive(purpose::BACKUP_MAC);
    #[allow(clippy::expect_used, reason = "HMAC takes a key of any length")]
    HmacSha256::new_from_slice(&key[..]).expect("hmac key")
}

/// `manifest.mac` of `manifest`: lowercase hex.
pub fn sign_manifest(master: &MasterKey, manifest: &[u8]) -> String {
    let mut mac = mac_key(master);
    mac.update(manifest);
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time check of `manifest.mac` against any of `keys`.
pub fn check_mac(keys: &[MasterKey], manifest: &[u8], tag_hex: &[u8]) -> Result<()> {
    let tag = std::str::from_utf8(tag_hex)
        .ok()
        .and_then(|t| hex::decode(t.trim()).ok())
        .ok_or_else(|| BackupError::new("bad_mac", "manifest.mac is malformed"))?;
    for key in keys {
        let mut mac = mac_key(key);
        mac.update(manifest);
        if mac.verify_slice(&tag).is_ok() {
            return Ok(());
        }
    }
    Err(BackupError::new(
        "bad_mac",
        "manifest.mac does not match: the archive was changed, forged, or made under another master key",
    ))
}

/// A reader counting and hashing what passes through it.
pub struct HashReader<R> {
    inner: R,
    hash: Sha256,
    count: u64,
}

impl<R> HashReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hash: Sha256::new(),
            count: 0,
        }
    }
    pub fn finish(self) -> (u64, String) {
        (self.count, hex::encode(self.hash.finalize()))
    }
    pub fn count(&self) -> u64 {
        self.count
    }
}

impl<R: Read> Read for HashReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hash.update(&buf[..n]);
        self.count += n as u64;
        Ok(n)
    }
}

/// A writer counting and hashing what passes through it (the archive's own digest).
pub struct HashWriter<W> {
    pub(crate) inner: W,
    hash: Sha256,
    count: u64,
}

impl<W> HashWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hash: Sha256::new(),
            count: 0,
        }
    }
    pub fn finish(self) -> (W, u64, String) {
        (self.inner, self.count, hex::encode(self.hash.finalize()))
    }
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hash.update(&buf[..n]);
        self.count += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
