//! `rt.lock`: the hash of every remote module a version's build fetched (step 3).
//!
//! A rebuild from the same source + lock must fetch identical bytes; a changed byte at a locked
//! URL fails the build with `lock_mismatch` (BLD-06).

use std::collections::BTreeMap;

use rt_types::Digest;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const LOCK_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockEntry {
    pub digest: Digest,
    pub size: u64,
    /// The `Content-Type` the module was served with (decides JS vs JSON when a rebuild takes
    /// the locked bytes from the local artifact store instead of the network).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtLock {
    pub version: u32,
    /// URL → entry, sorted (BTreeMap) for a deterministic lock digest.
    pub entries: BTreeMap<String, LockEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("locked module {url} changed: expected {expected}, got {actual}")]
pub struct LockMismatch {
    pub url: String,
    pub expected: Digest,
    pub actual: Digest,
}

impl Default for RtLock {
    fn default() -> Self {
        Self {
            version: LOCK_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

impl RtLock {
    pub fn record(&mut self, url: &str, digest: Digest, size: u64) {
        self.record_typed(url, digest, size, None);
    }

    /// [`RtLock::record`] with the module's content type.
    pub fn record_typed(&mut self, url: &str, digest: Digest, size: u64, content_type: Option<String>) {
        self.entries.insert(
            url.to_string(),
            LockEntry {
                digest,
                size,
                content_type,
            },
        );
    }

    /// Check `digest` against the locked value for `url`; unknown URLs pass (they get recorded).
    pub fn verify(&self, url: &str, digest: &Digest) -> Result<(), LockMismatch> {
        match self.entries.get(url) {
            Some(e) if e.digest != *digest => Err(LockMismatch {
                url: url.to_string(),
                expected: e.digest,
                actual: *digest,
            }),
            _ => Ok(()),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("lock serialization cannot fail")
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(b)
    }

    /// Digest of the canonical bytes (`versions.lock_digest`).
    pub fn digest(&self) -> Digest {
        Digest::of(&self.to_bytes())
    }
}
