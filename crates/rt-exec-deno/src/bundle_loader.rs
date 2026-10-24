//! Module loaders: the task loader serving **only** from the prepared bundle (plus the embedded
//! `std:*` modules), and the code-cache producer's loader.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use deno_core::{
    ModuleCodeString, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleLoader, ModuleSource,
    ModuleSourceCode, ModuleSpecifier, ModuleType, RequestedModuleType, ResolutionKind, SourceCodeCacheInfo,
};
use deno_error::JsErrorBox;
use rt_build::ModuleKind;
use rt_obs::Metrics;

use crate::js::rt_module_source;
use crate::prepared::DenoPrepared;

type ModuleLoaderError = JsErrorBox;

pub const MODULE_NOT_IN_BUNDLE: &str = "module_not_in_bundle";

fn not_in_bundle(specifier: &str) -> ModuleLoaderError {
    match crate::js::renamed_module_message(specifier) {
        Some(msg) => JsErrorBox::generic(msg),
        None => JsErrorBox::generic(format!("{MODULE_NOT_IN_BUNDLE}: {specifier}")),
    }
}

/// Specifier resolution shared by both loaders: absolute `file:`/`http(s):` URLs of bundle
/// modules, relative imports against them, and the `rt:`/`sdk:` built-ins. Every other scheme
/// (`node:`, `npm:`, `ext:`, `data:` ...) fails with `module_not_in_bundle`.
fn resolve_specifier(specifier: &str, referrer: &str) -> Result<ModuleSpecifier, ModuleLoaderError> {
    if specifier.starts_with("std:") || specifier.starts_with("rt:") || specifier.starts_with("sdk:") {
        return ModuleSpecifier::parse(specifier).map_err(|_| not_in_bundle(specifier));
    }
    let url = deno_core::resolve_import(specifier, referrer).map_err(|_| not_in_bundle(specifier))?;
    match url.scheme() {
        "file" | "http" | "https" => Ok(url),
        _ => Err(not_in_bundle(specifier)),
    }
}

/// FNV-1a 64: stands in for the source hash V8 code-cache consumers pass back to the loader.
fn source_hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CacheState {
    /// A blob was handed to V8: a hit unless V8 reports it back as rejected.
    Offered,
    /// No cache for this module (or no cache at all).
    Absent,
    /// The cache was produced by another engine: never offered to V8.
    Foreign,
}

/// Task loader: bundle modules + embedded `std:*`. One per isolate; re-attached per task so pooled
/// (tenant-neutral) isolates can be created before their task is known.
#[derive(Default)]
pub struct BundleLoader {
    prepared: RefCell<Option<Arc<DenoPrepared>>>,
    index: RefCell<HashMap<String, usize>>,
    cache_state: RefCell<HashMap<String, CacheState>>,
    ready: RefCell<HashSet<String>>,
    counted: RefCell<HashSet<String>>,
}

impl BundleLoader {
    pub fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    /// Bind the loader to a task's prepared bundle.
    pub fn attach(&self, prepared: Arc<DenoPrepared>) {
        let index = prepared
            .bundle
            .modules
            .iter()
            .enumerate()
            .map(|(i, m)| (m.specifier.clone(), i))
            .collect();
        *self.index.borrow_mut() = index;
        self.cache_state.borrow_mut().clear();
        self.ready.borrow_mut().clear();
        self.counted.borrow_mut().clear();
        *self.prepared.borrow_mut() = Some(prepared);
    }

    /// Count `rt_deno_code_cache_total{result}` for modules loaded since the last call.
    pub fn flush_cache_stats(&self, metrics: Option<&Metrics>) {
        let Some(metrics) = metrics else { return };
        let ready = self.ready.borrow();
        let mut counted = self.counted.borrow_mut();
        for (spec, state) in self.cache_state.borrow().iter() {
            if !counted.insert(spec.clone()) {
                continue;
            }
            let result = match state {
                CacheState::Foreign => "rejected",
                CacheState::Absent => "miss",
                CacheState::Offered if ready.contains(spec) => "rejected",
                CacheState::Offered => "hit",
            };
            metrics.deno_code_cache_total.with_label_values(&[result]).inc();
        }
    }
}

impl ModuleLoader for BundleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, ModuleLoaderError> {
        resolve_specifier(specifier, referrer)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&ModuleLoadReferrer>,
        _options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        let spec = module_specifier.as_str();
        if let Some(src) = rt_module_source(spec) {
            return ModuleLoadResponse::Sync(Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(ModuleCodeString::from(src.to_string())),
                module_specifier,
                None,
            )));
        }
        let prepared = self.prepared.borrow();
        let Some(prepared) = prepared.as_ref() else {
            return ModuleLoadResponse::Sync(Err(not_in_bundle(spec)));
        };
        let Some(m) = self
            .index
            .borrow()
            .get(spec)
            .and_then(|i| prepared.bundle.modules.get(*i))
        else {
            return ModuleLoadResponse::Sync(Err(not_in_bundle(spec)));
        };
        let (module_type, cache) = match m.kind {
            ModuleKind::Js => {
                let mut cache = SourceCodeCacheInfo {
                    hash: source_hash(&m.source),
                    data: None,
                };
                let state = match &prepared.code_cache {
                    Some(cc) if cc.engine_fp != prepared.key.engine_fp => CacheState::Foreign,
                    Some(cc) => match cc.per_module.get(spec) {
                        Some(blob) if !blob.is_empty() => {
                            cache.data = Some(Cow::Owned(blob.clone()));
                            CacheState::Offered
                        }
                        _ => CacheState::Absent,
                    },
                    None => CacheState::Absent,
                };
                self.cache_state.borrow_mut().insert(spec.to_string(), state);
                (ModuleType::JavaScript, Some(cache))
            }
            ModuleKind::Json => (ModuleType::Json, None),
            ModuleKind::Wasm => return ModuleLoadResponse::Sync(Err(not_in_bundle(spec))),
        };
        ModuleLoadResponse::Sync(Ok(ModuleSource::new(
            module_type,
            ModuleSourceCode::String(ModuleCodeString::from(m.source.clone())),
            module_specifier,
            cache,
        )))
    }

    fn code_cache_ready(
        &self,
        module_specifier: ModuleSpecifier,
        _hash: u64,
        _code_cache: &[u8],
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        self.ready.borrow_mut().insert(module_specifier.to_string());
        Box::pin(async {})
    }

    fn get_source_map(&self, file_name: &str) -> Option<Cow<'_, [u8]>> {
        let prepared = self.prepared.borrow();
        let prepared = prepared.as_ref()?;
        let i = *self.index.borrow().get(file_name)?;
        let map = prepared.bundle.modules.get(i)?.source_map.as_ref()?;
        Some(Cow::Owned(map.clone().into_bytes()))
    }
}

/// Loader used by the code-cache producer: serves the bundle and asks V8 for a cache of every
/// JS module (`data: None` makes deno_core compile and report the fresh cache).
pub struct ProducerLoader {
    pub bundle: rt_build::Bundle,
    pub produced: Rc<RefCell<HashMap<String, Vec<u8>>>>,
}

impl ModuleLoader for ProducerLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, ModuleLoaderError> {
        resolve_specifier(specifier, referrer)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&ModuleLoadReferrer>,
        options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        let spec = module_specifier.as_str();
        if let Some(src) = rt_module_source(spec) {
            return ModuleLoadResponse::Sync(Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(ModuleCodeString::from(src.to_string())),
                module_specifier,
                None,
            )));
        }
        let Some(m) = self.bundle.module(spec) else {
            return ModuleLoadResponse::Sync(Err(not_in_bundle(spec)));
        };
        let json = matches!(options.requested_module_type, RequestedModuleType::Json) || m.kind == ModuleKind::Json;
        let (module_type, cache) = if json {
            (ModuleType::Json, None)
        } else {
            (
                ModuleType::JavaScript,
                Some(SourceCodeCacheInfo {
                    hash: source_hash(&m.source),
                    data: None,
                }),
            )
        };
        ModuleLoadResponse::Sync(Ok(ModuleSource::new(
            module_type,
            ModuleSourceCode::String(ModuleCodeString::from(m.source.clone())),
            module_specifier,
            cache,
        )))
    }

    fn code_cache_ready(
        &self,
        module_specifier: ModuleSpecifier,
        _hash: u64,
        code_cache: &[u8],
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        self.produced
            .borrow_mut()
            .insert(module_specifier.to_string(), code_cache.to_vec());
        Box::pin(async {})
    }
}

/// Wraps a shared [`BundleLoader`] as the `Rc<dyn ModuleLoader>` a `JsRuntime` wants.
pub struct SharedLoader(pub Rc<BundleLoader>);

impl ModuleLoader for SharedLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, ModuleLoaderError> {
        self.0.resolve(specifier, referrer, kind)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        maybe_referrer: Option<&ModuleLoadReferrer>,
        options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        self.0.load(module_specifier, maybe_referrer, options)
    }

    fn code_cache_ready(
        &self,
        module_specifier: ModuleSpecifier,
        hash: u64,
        code_cache: &[u8],
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        self.0.code_cache_ready(module_specifier, hash, code_cache)
    }

    fn get_source_map(&self, file_name: &str) -> Option<Cow<'_, [u8]>> {
        self.0.get_source_map(file_name)
    }
}

#[cfg(test)]
mod tests {
    use rt_build::{Bundle, BundleModule, CodeCache};
    use rt_types::{Digest, HandlerForm, PreparedKey, RuntimeKind};

    use super::*;

    const FP: u64 = 7;

    fn module(spec: &str, kind: ModuleKind, source: &str) -> BundleModule {
        BundleModule {
            specifier: spec.into(),
            kind,
            source: source.into(),
            source_map: None,
        }
    }

    fn prepared(cache: Option<CodeCache>) -> Arc<DenoPrepared> {
        let mut bundle = Bundle::new("file:///main.js", HandlerForm::A, None);
        bundle
            .modules
            .push(module("file:///main.js", ModuleKind::Js, "export default () => 1;"));
        bundle.modules.push(module("file:///data.json", ModuleKind::Json, "{}"));
        bundle.modules.push(module("file:///lib.wasm", ModuleKind::Wasm, ""));
        let mut with_map = module("file:///mapped.js", ModuleKind::Js, "export {};");
        with_map.source_map = Some("{\"version\":3}".into());
        bundle.modules.push(with_map);
        Arc::new(DenoPrepared {
            key: PreparedKey {
                digest: Digest::of(b"x"),
                kind: RuntimeKind::Deno,
                engine_fp: FP,
            },
            bundle,
            code_cache: cache,
            handler_form: HandlerForm::A,
            accounted_bytes: 0,
            reads_env: Default::default(),
        })
    }

    fn load(loader: &impl ModuleLoader, spec: &str) -> Result<ModuleSource, ModuleLoaderError> {
        let url = ModuleSpecifier::parse(spec).expect("specifier");
        let options = ModuleLoadOptions {
            is_dynamic_import: false,
            is_synchronous: false,
            requested_module_type: RequestedModuleType::None,
        };
        let ModuleLoadResponse::Sync(r) = loader.load(&url, None, options) else {
            panic!("the loaders answer synchronously")
        };
        r
    }

    fn message(r: Result<ModuleSource, ModuleLoaderError>) -> String {
        r.expect_err("an error").to_string()
    }

    #[test]
    fn a_missing_module_names_itself_and_a_renamed_one_names_its_new_home() {
        assert_eq!(
            not_in_bundle("file:///nope.js").to_string(),
            "module_not_in_bundle: file:///nope.js"
        );
        assert_eq!(not_in_bundle("rt:kv").to_string(), "`rt:kv` was renamed to `std:kv`");
    }

    #[test]
    fn the_loader_serves_only_the_attached_bundle_and_never_a_wasm_module() {
        let loader = BundleLoader::new();
        assert!(
            message(load(&*loader, "file:///main.js")).contains("module_not_in_bundle"),
            "nothing attached yet"
        );
        assert!(load(&*loader, "std:kv").is_ok(), "the embedded modules need no bundle");

        loader.attach(prepared(None));
        assert!(load(&*loader, "file:///main.js").is_ok());
        assert!(load(&*loader, "file:///data.json").is_ok());
        assert!(message(load(&*loader, "file:///lib.wasm")).contains("module_not_in_bundle"));
        assert!(message(load(&*loader, "file:///other.js")).contains("module_not_in_bundle"));
        assert_eq!(
            loader.get_source_map("file:///mapped.js").as_deref(),
            Some(b"{\"version\":3}".as_slice())
        );
        assert!(
            loader.get_source_map("file:///main.js").is_none(),
            "a module without a map has none"
        );
        assert!(loader.get_source_map("file:///other.js").is_none());
    }

    #[test]
    fn the_cache_counter_says_hit_miss_or_rejected_once_per_module() {
        let metrics = Metrics::new(rt_obs::MetricsOptions::default()).expect("metrics");
        let count = |r: &str| metrics.deno_code_cache_total.with_label_values(&[r]).get();
        let cache = |fp, blob: Vec<u8>| {
            let mut c = CodeCache {
                engine_fp: fp,
                per_module: Default::default(),
            };
            c.per_module.insert("file:///main.js".into(), blob);
            c
        };

        // A cache for the module: offered, a hit unless V8 hands it back as ready (= rejected).
        let loader = BundleLoader::new();
        loader.attach(prepared(Some(cache(FP, vec![1, 2, 3]))));
        load(&*loader, "file:///main.js").expect("load");
        loader.flush_cache_stats(Some(&metrics));
        loader.flush_cache_stats(Some(&metrics)); // nothing is counted twice
        assert_eq!((count("hit"), count("miss"), count("rejected")), (1, 0, 0));

        let loader = BundleLoader::new();
        loader.attach(prepared(Some(cache(FP, vec![1, 2, 3]))));
        load(&*loader, "file:///main.js").expect("load");
        drop(loader.code_cache_ready(ModuleSpecifier::parse("file:///main.js").expect("spec"), 0, &[]));
        loader.flush_cache_stats(Some(&metrics));
        assert_eq!(
            (count("hit"), count("rejected")),
            (1, 1),
            "V8 produced a new cache: the offered one was refused"
        );

        // No blob for the module, an empty one, no cache at all: misses.
        for cc in [
            cache(FP, vec![]),
            CodeCache {
                engine_fp: FP,
                per_module: Default::default(),
            },
        ] {
            let loader = BundleLoader::new();
            loader.attach(prepared(Some(cc)));
            load(&*loader, "file:///main.js").expect("load");
            loader.flush_cache_stats(Some(&metrics));
        }
        assert_eq!(count("miss"), 2);

        // A cache of another engine is never offered.
        let loader = BundleLoader::new();
        loader.attach(prepared(Some(cache(FP + 1, vec![1]))));
        load(&*loader, "file:///main.js").expect("load");
        loader.flush_cache_stats(Some(&metrics));
        assert_eq!(count("rejected"), 2);
        loader.flush_cache_stats(None); // no metrics, nothing to do
    }

    #[test]
    fn the_producer_loader_serves_the_bundle_and_refuses_what_it_does_not_hold() {
        let p = prepared(None);
        let loader = ProducerLoader {
            bundle: p.bundle.clone(),
            produced: Default::default(),
        };
        assert!(load(&loader, "file:///main.js").is_ok());
        assert!(load(&loader, "std:kv").is_ok());
        assert!(message(load(&loader, "file:///other.js")).contains("module_not_in_bundle"));
        drop(loader.code_cache_ready(ModuleSpecifier::parse("file:///main.js").expect("spec"), 0, &[9, 9]));
        assert_eq!(
            loader.produced.borrow().get("file:///main.js").map(Vec::as_slice),
            Some([9u8, 9].as_slice())
        );
    }

    #[test]
    fn only_bundle_urls_and_the_embedded_schemes_resolve() {
        for ok in ["std:kv", "rt:kv", "sdk:acme", "./lib.js", "https://cdn.example/x.js"] {
            assert!(resolve_specifier(ok, "file:///main.js").is_ok(), "{ok}");
        }
        for bad in ["node:fs", "npm:zod", "data:text/javascript,1", "ext:core/mod.js"] {
            assert!(resolve_specifier(bad, "file:///main.js").is_err(), "{bad}");
        }
    }
}
