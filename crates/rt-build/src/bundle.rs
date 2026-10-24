//! `rt-bundle v1`: the prepared form of a Deno service (step 5).
//!
//! A serde-JSON module map: header `{format, entry, handler_form, lock_digest}` plus modules
//! `{specifier, kind, source, source_map}` in **deterministic order** (sorted by specifier) so the
//! same source + same lock always yields the same bundle digest (BLD-12).

use rt_types::{Digest, HandlerForm};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const BUNDLE_FORMAT: &str = "rt-bundle/1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModuleKind {
    Js,
    Json,
    Wasm,
}

/// One module of the bundle. `source` is UTF-8 text for `js`/`json`, base64 for `wasm`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleModule {
    /// Canonical specifier the bundle loader serves (`file:///main.js`, `https://esm.sh/zod@3`).
    pub specifier: String,
    pub kind: ModuleKind,
    pub source: String,
    pub source_map: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bundle {
    pub format: String,
    /// Entry module specifier (a key of `modules`).
    pub entry: String,
    pub handler_form: HandlerForm,
    pub lock_digest: Option<Digest>,
    pub modules: Vec<BundleModule>,
}

#[derive(Debug, Error)]
pub enum BundleError {
    #[error("bundle is not valid json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported bundle format `{0}`")]
    Format(String),
    #[error("entry module `{0}` is not in the bundle")]
    MissingEntry(String),
}

impl Bundle {
    pub fn new(entry: impl Into<String>, handler_form: HandlerForm, lock_digest: Option<Digest>) -> Self {
        Self {
            format: BUNDLE_FORMAT.to_string(),
            entry: entry.into(),
            handler_form,
            lock_digest,
            modules: Vec::new(),
        }
    }

    /// Sort modules by specifier (the canonical order).
    pub fn canonicalize(&mut self) {
        self.modules.sort_by(|a, b| a.specifier.cmp(&b.specifier));
    }

    /// Canonical serialization (modules sorted). Equal inputs ⇒ equal bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut c = self.clone();
        c.canonicalize();
        serde_json::to_vec(&c).expect("bundle serialization cannot fail")
    }

    /// Parse and validate the header and entry.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BundleError> {
        let b: Bundle = serde_json::from_slice(bytes)?;
        if b.format != BUNDLE_FORMAT {
            return Err(BundleError::Format(b.format));
        }
        if !b.modules.iter().any(|m| m.specifier == b.entry) {
            return Err(BundleError::MissingEntry(b.entry));
        }
        Ok(b)
    }

    /// Digest of the canonical bytes.
    pub fn digest(&self) -> Digest {
        Digest::of(&self.to_bytes())
    }

    pub fn module(&self, specifier: &str) -> Option<&BundleModule> {
        self.modules.iter().find(|m| m.specifier == specifier)
    }
}
