//! Synchronous deploy-time validation seam.
//!
//! `rt-api` must not depend on `rt-build` (layer rules), so the API calls validation
//! through this trait; `rt-server` injects the `rt-build` implementation.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::digest::Digest;
use crate::kinds::RuntimeKind;
use crate::outcome::Diagnostic;

/// How a Deno entry module exports its handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HandlerForm {
    /// `export default function (req, ctx)`.
    #[serde(rename = "a")]
    A,
    /// `export default { fetch, scheduled, queue }`.
    #[serde(rename = "b")]
    B,
    /// Could not be decided statically; detected at runtime.
    Unknown,
}

/// A deploy upload that passed synchronous validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedUpload {
    /// Digest of the stored source artifact (the raw body or normalized archive).
    pub source_digest: Digest,
    /// Entry module (deno) if known.
    pub entry: Option<String>,
    pub handler_form: HandlerForm,
    /// Size of the upload in bytes.
    pub size: u64,
    /// WASM: the component's top-level exports (`wasi:http/incoming-handler@0.2.x`,
    /// `std:host/socket-handler@0.1.0`, …); empty for Deno.
    pub exports: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    /// `422 validation_failed` with `[{file,line,col,message}]`.
    #[error("validation failed ({} diagnostics)", .0.len())]
    Invalid(Vec<Diagnostic>),
    /// `422 invalid_artifact` (wasm: not a component / no incoming-handler / threads proposal).
    #[error("invalid artifact: {0}")]
    InvalidArtifact(String),
    /// `413 too_large`.
    #[error("upload too large: {0}")]
    TooLarge(String),
}

/// What an RPC contract is rendered for (`rt_build::contract`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContractTarget {
    /// `21ci types <service>`: an interface to cast `services.bind(slug)` to.
    Service { slug: String },
    /// `21ci sdk types <name>`: ambient `sdk:<name>` / `std:sdk` modules.
    Sdk { name: String, version: String },
}

/// The RPC surface of a Deno upload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcContractText {
    /// Top-level names (methods and namespaces).
    pub members: Vec<String>,
    /// The `.d.ts` text.
    pub dts: String,
    /// `defineSdk` methods that never read the caller (no `authorize` hook).
    pub unchecked: Vec<String>,
}

/// Validates an upload without network or disk access.
pub trait UploadValidator: Send + Sync {
    fn validate(&self, kind: RuntimeKind, content_type: &str, body: &Bytes)
        -> Result<ValidatedUpload, ValidationError>;

    /// The RPC surface of a Deno upload (its `rpc` export or `defineSdk` methods), read from the
    /// source without running it. `Err` with a message when it has none.
    fn rpc_contract(
        &self,
        content_type: &str,
        body: &Bytes,
        target: &ContractTarget,
    ) -> Result<RpcContractText, String> {
        let _ = (content_type, body, target);
        Err("RPC contracts are not available on this node".to_string())
    }
}
