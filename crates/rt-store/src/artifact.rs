//! Content-addressed artifact store (T3).
//!
//! Local layout: `$RT_DATA/artifacts/<sha256>/{blob, meta.json}`. Content-addressed and
//! immutable; the refcount is computed live from `meta.db` (`MetaStore::digest_referenced`:
//! versions' sources and locks, and the modules recorded locks list).
//! S3 (`rusty-s3`, URL signing only) is a stretch backend behind the same trait.
//!
//! Deletion is race-safe ([`delete_unreferenced`], [`sweep_orphans`]): an artifact is deleted only
//! when nothing references it right before the delete **and** it was not written or touched within
//! a grace period ([`ARTIFACT_GRACE`]). Every `put` of existing bytes touches them, so a concurrent
//! re-upload (put, then add the version) or a build in flight (it touches its source and the
//! modules it uses) keeps its artifacts; puts and deletes are serialized per store.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rt_types::Digest;

use crate::error::{Result, StoreError};
use crate::meta::MetaStore;
use crate::sqlutil::{now_ms, unique_name};

/// How long an artifact stays after its last write or touch even when nothing references it: a
/// generous bound on an upload's put→add-version window and on a build (its timeout plus queueing).
pub const ARTIFACT_GRACE: Duration = Duration::from_secs(30 * 60);

/// URLs one vendored artifact remembers (`meta.json` `vendored_from`), newest kept.
const MAX_VENDORED_URLS: usize = 64;

#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Store `bytes`, returning their digest. Idempotent (same bytes ⇒ same digest, one copy).
    async fn put(&self, bytes: Bytes) -> Result<Digest>;
    /// [`ArtifactStore::put`] an upload and remember the `Content-Type` it came with, so a build
    /// can extract it as it was validated. The first upload of these bytes decides, except that a
    /// TypeScript upload replaces a JavaScript type: bytes validated as TypeScript build as
    /// TypeScript, and TypeScript accepts what JavaScript does (builds are keyed by digest, so the
    /// same bytes have one build).
    async fn put_typed(&self, bytes: Bytes, content_type: &str) -> Result<Digest> {
        let _ = content_type;
        self.put(bytes).await
    }
    /// [`ArtifactStore::put`] a remote module a build fetched from `url`, remembering that URL:
    /// a later build whose lock names `url` with this digest may read it back without the network.
    async fn put_vendored(&self, bytes: Bytes, url: &str) -> Result<Digest> {
        let _ = url;
        self.put(bytes).await
    }
    /// Whether `d` was vendored from `url` ([`ArtifactStore::put_vendored`]). Only then is it served
    /// for a lock entry of `url`: the store also holds every tenant's uploads, and a lock must not
    /// read an unrelated artifact by its digest (FETCH-003). A store that cannot tell says no.
    async fn vendored_from(&self, d: &Digest, url: &str) -> Result<bool> {
        let _ = (d, url);
        Ok(false)
    }
    /// The `Content-Type` recorded by [`ArtifactStore::put_typed`], if any.
    async fn content_type(&self, d: &Digest) -> Result<Option<String>> {
        let _ = d;
        Ok(None)
    }
    /// Fetch and verify. `NotFound` if absent; `Corrupt` if the content no longer matches.
    async fn get(&self, d: &Digest) -> Result<Bytes>;
    async fn exists(&self, d: &Digest) -> Result<bool>;
    async fn delete(&self, d: &Digest) -> Result<()>;
    /// Size in bytes if present.
    async fn size(&self, d: &Digest) -> Result<Option<u64>>;
    /// Mark `d` as just used (a build reading it): its deletion waits for the grace period again.
    async fn touch(&self, d: &Digest) -> Result<()> {
        let _ = d;
        Ok(())
    }
    /// Delete `d` only if it was not written or touched within `idle_for`, atomically with
    /// respect to puts; whether it was deleted. A store that cannot tell keeps everything.
    async fn delete_if_idle(&self, d: &Digest, idle_for: Duration) -> Result<bool> {
        let _ = (d, idle_for);
        Ok(false)
    }
    /// Every artifact not written or touched within `idle_for` (orphan sweeps).
    async fn idle(&self, idle_for: Duration) -> Result<Vec<Digest>> {
        let _ = idle_for;
        Ok(Vec::new())
    }
}

/// Delete those of `digests` (typically what version GC freed) that nothing references right now
/// and that were not written or touched within `grace`. Returns how many were deleted; failures are
/// logged and leave the artifact for the next sweep.
pub async fn delete_unreferenced(
    meta: &dyn MetaStore,
    artifacts: &dyn ArtifactStore,
    digests: impl IntoIterator<Item = Digest>,
    grace: Duration,
) -> u64 {
    let mut deleted = 0;
    for d in digests {
        match meta.digest_referenced(&d).await {
            Ok(false) => {}
            Ok(true) => continue,
            Err(e) => {
                tracing::warn!(digest = %d, error = %e, "could not check an artifact's references; kept");
                continue;
            }
        }
        match artifacts.delete_if_idle(&d, grace).await {
            Ok(true) => deleted += 1,
            Ok(false) => {}
            Err(e) => tracing::warn!(digest = %d, error = %e, "could not delete an unreferenced artifact"),
        }
    }
    deleted
}

/// Delete every artifact nothing references that was not written or touched within `grace`: what
/// failed builds vendored (modules, locks) and uploads whose version was never added.
pub async fn sweep_orphans(meta: &dyn MetaStore, artifacts: &dyn ArtifactStore, grace: Duration) -> Result<u64> {
    let idle = artifacts.idle(grace).await?;
    Ok(delete_unreferenced(meta, artifacts, idle, grace).await)
}

fn is_typescript(ct: &str) -> bool {
    ct.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
        .ends_with("typescript")
}

fn is_javascript(ct: &str) -> bool {
    ct.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
        .ends_with("javascript")
}

/// Seconds since `path` was last written or touched (0 if its time is in the future).
fn idle_for(path: &Path) -> std::io::Result<Duration> {
    let modified = fs::metadata(path)?.modified()?;
    Ok(SystemTime::now().duration_since(modified).unwrap_or_default())
}

/// Local-disk implementation.
pub struct LocalArtifactStore {
    root: PathBuf,
    /// Serializes writes/touches against idle-checked deletes, so a put never loses its bytes to a
    /// delete that judged them idle a moment before.
    ops: Arc<Mutex<()>>,
}

fn private_dir(path: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new().recursive(true).mode(0o700).create(path)
}

/// Write `bytes` to `dest` atomically (tmp + rename), mode 0600.
fn write_atomic(dest: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = dest.parent().unwrap_or_else(|| Path::new("."));
    let tmp = dir.join(format!(".tmp-{}", unique_name()));
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

impl LocalArtifactStore {
    /// Create `root` if needed. Writes are atomic (tmp + rename); files mode 0600.
    pub fn open(root: &Path) -> Result<Arc<Self>> {
        private_dir(root)?;
        Ok(Arc::new(Self {
            root: root.to_path_buf(),
            ops: Arc::new(Mutex::new(())),
        }))
    }

    /// Path of the blob for `d` (diagnostics, tests asserting "one stored artifact", API-09).
    pub fn blob_path(&self, d: &Digest) -> PathBuf {
        self.root.join(d.hex()).join("blob")
    }

    fn entry_dir(&self, d: &Digest) -> PathBuf {
        self.root.join(d.hex())
    }

    /// `put` / `put_typed`: the first write of these bytes also writes `meta.json` (with the
    /// content type when one is given); a later put touches them (see [`ARTIFACT_GRACE`]).
    async fn put_inner(&self, bytes: Bytes, content_type: Option<String>) -> Result<Digest> {
        let (dir, ops) = (self.root.clone(), self.ops.clone());
        tokio::task::spawn_blocking(move || -> Result<Digest> {
            let _ops = ops.lock();
            let digest = Digest::of(&bytes);
            let entry = dir.join(digest.hex());
            let blob = entry.join("blob");
            let meta = entry.join("meta.json");
            // Present only if the file still holds these exact bytes: a copy damaged in place (same
            // length) is rewritten by the upload of the right bytes (ART-004).
            let present = fs::metadata(&blob)
                .map(|m| m.is_file() && m.len() == bytes.len() as u64)
                .unwrap_or(false)
                && fs::read(&blob).is_ok_and(|on_disk| Digest::of(&on_disk) == digest);
            if present {
                touch_file(&blob)?;
            } else {
                private_dir(&entry)?;
                write_atomic(&blob, &bytes)?;
            }
            if meta.is_file() {
                if let Some(ct) = content_type.as_deref().filter(|ct| is_typescript(ct)) {
                    upgrade_to_typescript(&meta, ct)?;
                }
            } else {
                let mut doc = serde_json::json!({
                    "sha256": digest.hex(),
                    "size": bytes.len(),
                    "created_at_ms": now_ms(),
                });
                if let Some(ct) = content_type {
                    doc["content_type"] = serde_json::Value::String(ct);
                }
                write_atomic(&meta, doc.to_string().as_bytes())?;
            }
            Ok(digest)
        })
        .await
        .map_err(join_err)?
    }
}

/// Set `path`'s modification time to now (its last use).
fn touch_file(path: &Path) -> std::io::Result<()> {
    OpenOptions::new()
        .write(true)
        .open(path)?
        .set_modified(SystemTime::now())
}

/// A TypeScript upload of bytes first uploaded as JavaScript records TypeScript (see `put_typed`).
fn upgrade_to_typescript(meta: &Path, ct: &str) -> Result<()> {
    let mut doc: serde_json::Value = serde_json::from_str(&fs::read_to_string(meta)?).unwrap_or_default();
    let recorded = doc.get("content_type").and_then(|v| v.as_str()).unwrap_or("");
    if is_javascript(recorded) {
        doc["content_type"] = serde_json::Value::String(ct.to_string());
        write_atomic(meta, doc.to_string().as_bytes())?;
    }
    Ok(())
}

// LCOV_EXCL_START: a blocking task fails only by panicking or when its runtime shuts down; the closures here have no injection point
fn join_err(e: tokio::task::JoinError) -> StoreError {
    StoreError::Internal(format!("blocking task failed: {e}"))
}
// LCOV_EXCL_STOP

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn put(&self, bytes: Bytes) -> Result<Digest> {
        self.put_inner(bytes, None).await
    }

    async fn put_typed(&self, bytes: Bytes, content_type: &str) -> Result<Digest> {
        self.put_inner(bytes, Some(content_type.to_string())).await
    }

    async fn put_vendored(&self, bytes: Bytes, url: &str) -> Result<Digest> {
        let digest = self.put_inner(bytes, None).await?;
        let (meta, ops, url) = (
            self.entry_dir(&digest).join("meta.json"),
            self.ops.clone(),
            url.to_string(),
        );
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _ops = ops.lock();
            let mut doc: serde_json::Value = fs::read_to_string(&meta)
                .ok()
                .and_then(|t| serde_json::from_str(&t).ok())
                .unwrap_or_default();
            let mut urls: Vec<String> = doc
                .get("vendored_from")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            if !urls.contains(&url) {
                urls.push(url);
                // Bounded: the same bytes served under many URLs keep the newest ones.
                let excess = urls.len().saturating_sub(MAX_VENDORED_URLS);
                urls.drain(..excess);
                doc["vendored_from"] = serde_json::json!(urls);
                write_atomic(&meta, doc.to_string().as_bytes())?;
            }
            Ok(())
        })
        .await
        .map_err(join_err)??;
        Ok(digest)
    }

    async fn vendored_from(&self, d: &Digest, url: &str) -> Result<bool> {
        let (meta, url) = (self.entry_dir(d).join("meta.json"), url.to_string());
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let doc: serde_json::Value = match fs::read_to_string(&meta) {
                Ok(t) => serde_json::from_str(&t).unwrap_or_default(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(e) => return Err(e.into()),
            };
            Ok(doc
                .get("vendored_from")
                .and_then(|v| v.as_array())
                .is_some_and(|a| a.iter().any(|u| u.as_str() == Some(url.as_str()))))
        })
        .await
        .map_err(join_err)?
    }

    async fn content_type(&self, d: &Digest) -> Result<Option<String>> {
        let meta = self.entry_dir(d).join("meta.json");
        tokio::task::spawn_blocking(move || -> Result<Option<String>> {
            let text = match fs::read_to_string(&meta) {
                Ok(t) => t,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let doc: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            Ok(doc.get("content_type").and_then(|v| v.as_str()).map(str::to_string))
        })
        .await
        .map_err(join_err)?
    }

    async fn get(&self, d: &Digest) -> Result<Bytes> {
        let (path, want) = (self.blob_path(d), *d);
        tokio::task::spawn_blocking(move || -> Result<Bytes> {
            let data = match fs::read(&path) {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(StoreError::NotFound),
                Err(e) => return Err(e.into()),
            };
            if Digest::of(&data) != want {
                return Err(StoreError::Corrupt(format!("artifact {want} failed its digest check")));
            }
            Ok(Bytes::from(data))
        })
        .await
        .map_err(join_err)?
    }

    async fn exists(&self, d: &Digest) -> Result<bool> {
        let path = self.blob_path(d);
        tokio::task::spawn_blocking(move || Ok(path.is_file()))
            .await
            .map_err(join_err)?
    }

    async fn delete(&self, d: &Digest) -> Result<()> {
        let dir = self.entry_dir(d);
        tokio::task::spawn_blocking(move || -> Result<()> {
            match fs::remove_dir_all(&dir) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        })
        .await
        .map_err(join_err)?
    }

    async fn size(&self, d: &Digest) -> Result<Option<u64>> {
        let path = self.blob_path(d);
        tokio::task::spawn_blocking(move || -> Result<Option<u64>> {
            match fs::metadata(&path) {
                Ok(m) if m.is_file() => Ok(Some(m.len())),
                Ok(_) => Ok(None),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await
        .map_err(join_err)?
    }

    async fn touch(&self, d: &Digest) -> Result<()> {
        let (path, ops) = (self.blob_path(d), self.ops.clone());
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _ops = ops.lock();
            match touch_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        })
        .await
        .map_err(join_err)?
    }

    async fn delete_if_idle(&self, d: &Digest, idle: Duration) -> Result<bool> {
        let (dir, blob, ops) = (self.entry_dir(d), self.blob_path(d), self.ops.clone());
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let _ops = ops.lock();
            match idle_for(&blob) {
                Ok(age) if age < idle => return Ok(false),
                Ok(_) => {}
                // No blob: a leftover directory (or nothing) goes.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            match fs::remove_dir_all(&dir) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(e.into()),
            }
        })
        .await
        .map_err(join_err)?
    }

    async fn idle(&self, idle: Duration) -> Result<Vec<Digest>> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<Digest>> {
            let mut out = Vec::new();
            for entry in fs::read_dir(&root)?.flatten() {
                let Some(d) = entry.file_name().to_str().and_then(|n| Digest::from_hex(n).ok()) else {
                    continue;
                };
                // A blob that vanished meanwhile, or an entry without one, is idle too.
                if idle_for(&entry.path().join("blob")).map_or(true, |age| age >= idle) {
                    out.push(d);
                }
            }
            Ok(out)
        })
        .await
        .map_err(join_err)?
    }
}
