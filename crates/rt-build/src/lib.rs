//! rt-build (L4): validation, TypeScript transpile (`deno_ast`), specifier resolution, module
//! fetching, bundling (`rt-bundle v1`), `rt.lock`, code-cache and `.cwasm` precompile traits
//!.
//!
//! Layer rule: this crate never depends on an executor; `rt-exec-deno` / `rt-exec-wasm`
//! implement [`CodeCacheProducer`] / [`ComponentPrecompiler`] and `rt-server` injects them.

mod analyze;
pub mod builder;
pub mod bundle;
pub mod contract;
mod extract;
pub mod fetch;
mod graph;
pub mod lock;
pub mod precompile;
pub mod resolve;
pub mod sdk_shim;
pub mod sniff;
mod validate;
pub mod wasm_policy;

pub use builder::{
    BuildCx, BuildRequest, Builder, BuiltArtifact, BuiltFile, DefaultBuilder, SourceFile, Upload, UploadLimits,
    Validated, WasmInfo,
};
pub use bundle::{Bundle, BundleError, BundleModule, ModuleKind, BUNDLE_FORMAT};
pub use contract::{ContractError, RpcContract, RpcMethod};
pub use fetch::{FetchError, Fetched, FetcherConfig, GuardedFetcher, ModuleFetcher};
pub use lock::{LockEntry, LockMismatch, RtLock, LOCK_VERSION};
pub use precompile::{CodeCache, CodeCacheProducer, ComponentPrecompiler};
pub use resolve::{
    classify_specifier, renamed_to, DefaultResolver, ResolveError, Resolved, ResolverConfig, SpecifierKind,
    SpecifierResolver, BUILTIN_MODULES,
};
pub use rt_types::{HandlerForm, ValidatedUpload};
pub use sdk_shim::{SdkCatalog, StaticSdkCatalog};
pub use sniff::{is_wasm_component, is_wasm_core_module, media_type_of};
