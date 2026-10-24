//! The builder: synchronous validation at deploy, asynchronous build as a `Build` task
//!.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use rt_mem::Reservation;
use rt_obs::LogSink;
use rt_store::ArtifactStore;
use rt_types::{
    BuildFailReason, BuildFailure, ContractTarget, Diagnostic, Digest, PreparedKey, RpcContractText, RuntimeKind,
    UploadValidator, ValidatedUpload, ValidationError,
};

use crate::extract::VErr;
use crate::fetch::ModuleFetcher;
use crate::graph;
use crate::lock::RtLock;
use crate::precompile::{CodeCacheProducer, ComponentPrecompiler};
use crate::resolve::{SpecifierKind, SpecifierResolver};
use crate::sdk_shim::SdkCatalog;
use crate::validate;

/// Raw deploy upload.
#[derive(Clone, Debug)]
pub struct Upload {
    /// `application/typescript`, `text/javascript`, `application/x-tar`, `application/zip`,
    /// `application/json`, `application/wasm`.
    pub content_type: String,
    pub body: Bytes,
}

/// One file of an extracted Deno upload (path normalized, no `..`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFile {
    pub path: String,
    pub content: Bytes,
}

/// Static facts about a WASM upload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WasmInfo {
    /// Import policy: see `wasm_policy` (contracts C-7).
    pub is_component: bool,
    pub exports_incoming_handler: bool,
    pub imports: Vec<String>,
    /// The component's top-level exports.
    pub exports: Vec<String>,
}

/// Result of synchronous validation (no network, no disk).
#[derive(Clone, Debug)]
pub struct Validated {
    pub upload: ValidatedUpload,
    /// Deno: extracted source files.
    pub files: Vec<SourceFile>,
    /// Deno: `(file, specifier, class)` for every static import.
    pub imports: Vec<(String, String, SpecifierKind)>,
    /// `rt.json` import map.
    pub import_map: BTreeMap<String, String>,
    pub wasm: Option<WasmInfo>,
}

/// Limits for uploads (plan defaults).
#[derive(Clone, Debug)]
pub struct UploadLimits {
    pub max_single_file: u64,
    pub max_archive_total: u64,
    pub max_files: usize,
    pub max_file: u64,
    pub max_wasm: u64,
    /// Decompressed/compressed ratio above which an archive is rejected (zip bomb).
    pub max_inflate_ratio: u64,
}

impl Default for UploadLimits {
    fn default() -> Self {
        Self {
            max_single_file: 1024 * 1024,
            max_archive_total: 10 * 1024 * 1024,
            max_files: 1000,
            max_file: 1024 * 1024,
            max_wasm: 50 * 1024 * 1024,
            max_inflate_ratio: 100,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BuildRequest {
    pub key: PreparedKey,
    pub kind: RuntimeKind,
    /// Source artifact (T3) to build from.
    pub source_digest: Digest,
    /// Existing `rt.lock` (rebuilds must reproduce it; `None` on the first build).
    pub lock: Option<RtLock>,
    /// `build.timeout` (60 s deno / 120 s wasm).
    pub timeout: Duration,
}

/// Dependencies of one build run.
pub struct BuildCx {
    pub artifacts: Arc<dyn ArtifactStore>,
    pub fetcher: Arc<dyn ModuleFetcher>,
    pub resolver: Arc<dyn SpecifierResolver>,
    /// Injected by `rt-server` from `rt-exec-deno`.
    pub code_cache: Option<Arc<dyn CodeCacheProducer>>,
    /// Injected by `rt-server` from `rt-exec-wasm`.
    pub precompiler: Option<Arc<dyn ComponentPrecompiler>>,
    /// Build log (`kind=build` records, SSE `…/build/logs`).
    pub log: Arc<dyn LogSink>,
    /// The published operator SDKs (`std:sdk` / `sdk:<name>` shims). `None`: no SDK imports.
    pub sdks: Option<Arc<dyn SdkCatalog>>,
    /// `build.memory_mb` reserved from the governor for the duration of the build.
    pub reservation: Reservation,
    pub deadline: Instant,
}

/// One file destined for T2 (`bundle`, `code_cache.bin`, `component.cwasm`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltFile {
    pub name: String,
    pub bytes: Bytes,
}

#[derive(Clone, Debug)]
pub struct BuiltArtifact {
    pub key: PreparedKey,
    /// Written to T2 (with MACs) by the build task glue.
    pub files: Vec<BuiltFile>,
    /// Fetched remote modules, hashed (Deno).
    pub lock: Option<RtLock>,
    /// Digest of the canonical bundle (Deno).
    pub bundle_digest: Option<Digest>,
}

#[async_trait]
pub trait Builder: Send + Sync {
    /// Synchronous, no I/O: archive safety, parse, entry exists, classify specifiers; or for
    /// WASM component validation.
    fn validate(&self, kind: RuntimeKind, upload: &Upload) -> Result<Validated, Vec<Diagnostic>>;

    /// Run on a build processor: transpile → resolve → fetch → graph → bundle → code cache
    /// (Deno), or pre-initialize → precompile (WASM).
    async fn build(&self, req: &BuildRequest, cx: &mut BuildCx) -> Result<BuiltArtifact, BuildFailure>;
}

/// The default builder (`deno_ast` for TS, `wasmparser` for components).
pub struct DefaultBuilder {
    limits: UploadLimits,
    sdks: Option<Arc<dyn SdkCatalog>>,
}

impl DefaultBuilder {
    pub fn new(limits: UploadLimits) -> Self {
        Self { limits, sdks: None }
    }

    /// The published operator SDKs: deploy-time validation accepts `sdk:<name>` only for one of
    /// them (`422` otherwise, BLD-02, BLD-15). Without a catalog every `sdk:` import is refused.
    pub fn with_sdk_catalog(mut self, catalog: Arc<dyn SdkCatalog>) -> Self {
        self.sdks = Some(catalog);
        self
    }

    pub fn limits(&self) -> &UploadLimits {
        &self.limits
    }

    fn validate_inner(&self, kind: RuntimeKind, upload: &Upload) -> Result<Validated, VErr> {
        match kind {
            RuntimeKind::Deno => {
                let sdks: BTreeSet<String> = self
                    .sdks
                    .as_ref()
                    .map(|c| c.sdks().into_keys().map(|n| format!("sdk:{n}")).collect())
                    .unwrap_or_default();
                validate::deno(&self.limits, &sdks, upload)
            }
            RuntimeKind::Wasm => validate::wasm(&self.limits, upload),
        }
    }
}

#[async_trait]
impl Builder for DefaultBuilder {
    fn validate(&self, kind: RuntimeKind, upload: &Upload) -> Result<Validated, Vec<Diagnostic>> {
        self.validate_inner(kind, upload).map_err(VErr::into_diagnostics)
    }

    async fn build(&self, req: &BuildRequest, cx: &mut BuildCx) -> Result<BuiltArtifact, BuildFailure> {
        let budget = req.timeout.min(cx.deadline.saturating_duration_since(Instant::now()));
        let run = async {
            match req.kind {
                RuntimeKind::Deno => graph::build_deno(req, cx, &self.limits).await,
                RuntimeKind::Wasm => graph::build_wasm(req, cx, &self.limits).await,
            }
        };
        match tokio::time::timeout(budget, run).await {
            Ok(r) => r,
            Err(_) => Err(BuildFailure::new(
                BuildFailReason::Timeout,
                format!("build exceeded {} ms", budget.as_millis()),
            )),
        }
    }
}

/// `rt-api` validates uploads through this seam (it must not depend on `rt-build`).
impl UploadValidator for DefaultBuilder {
    fn validate(
        &self,
        kind: RuntimeKind,
        content_type: &str,
        body: &Bytes,
    ) -> Result<ValidatedUpload, ValidationError> {
        let upload = Upload {
            content_type: content_type.to_string(),
            body: body.clone(),
        };
        match self.validate_inner(kind, &upload) {
            Ok(v) => Ok(v.upload),
            Err(VErr::Invalid(d)) => Err(ValidationError::Invalid(d)),
            Err(VErr::InvalidArtifact(r)) => Err(ValidationError::InvalidArtifact(r)),
            Err(VErr::TooLarge(r)) => Err(ValidationError::TooLarge(r)),
        }
    }

    fn rpc_contract(
        &self,
        content_type: &str,
        body: &Bytes,
        target: &ContractTarget,
    ) -> Result<RpcContractText, String> {
        let contract =
            crate::contract::extract_upload(Some(content_type), body, &self.limits).map_err(|e| e.to_string())?;
        let dts = match target {
            ContractTarget::Service { slug } => contract.render_service(slug),
            ContractTarget::Sdk { name, version } => contract.render_sdk(name, version),
        };
        Ok(RpcContractText {
            members: contract.members(),
            dts,
            unchecked: contract.unchecked,
        })
    }
}
