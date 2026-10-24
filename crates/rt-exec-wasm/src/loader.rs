//! Glue between the executor, `rt-cache` and `rt-build`: T2 → T1 loading and `.cwasm`
//! precompilation.

use std::sync::Arc;
use std::time::Duration;

use rt_build::ComponentPrecompiler;
use rt_cache::{LoadError, Loader, T2Error, T2Store};
use rt_types::{BuildFailReason, BuildFailure, PreparedKey, TaskSpec};
use wasmtime::component::Component;
use wasmtime::Engine;

use crate::prepared::{build_proxy_pre, WasmPrepared};

/// T2 file name of a WASM entry.
pub const T2_CWASM: &str = "component.cwasm";

/// `rt-cache` loader: verify the MAC, then `Component::deserialize_file` (mmap; `unsafe`
/// because precompiled code is trusted — which is exactly why the MAC is checked first,
/// MAC-verified) and link `InstancePre`.
pub struct WasmLoader {
    engine: Engine,
    t2: Arc<T2Store>,
    build_spec: Arc<dyn Fn(&PreparedKey) -> TaskSpec + Send + Sync>,
}

impl WasmLoader {
    pub fn new(
        engine: Engine,
        t2: Arc<T2Store>,
        build_spec: Arc<dyn Fn(&PreparedKey) -> TaskSpec + Send + Sync>,
    ) -> Self {
        Self { engine, t2, build_spec }
    }
}

impl Loader for WasmLoader {
    type Value = WasmPrepared;

    fn load_t2(&self, key: &PreparedKey) -> Result<Option<(WasmPrepared, u64)>, LoadError> {
        let path = match self.t2.verify_path(key, T2_CWASM) {
            Ok(Some(path)) => path,
            Ok(None) => return Ok(None),
            Err(T2Error::Tampered) => return Err(LoadError::Tampered),
            Err(T2Error::Io(e)) => return Err(LoadError::Io(e)),
        };
        let bytes = std::fs::metadata(&path)?.len();
        // SAFETY: the file's MAC was verified just above, so it is a `.cwasm` this node
        // produced with this engine configuration; nothing uploaded or fetched is ever loaded here.
        let component = unsafe { Component::deserialize_file(&self.engine, &path) }
            .map_err(|e| LoadError::Corrupt(format!("component deserialize failed: {e}")))?;
        let pre = build_proxy_pre(&self.engine, &component)
            .map_err(|e| LoadError::Corrupt(format!("component link failed: {e}")))?;
        let prepared = WasmPrepared::new(*key, component, pre, bytes)
            .map_err(|e| LoadError::Corrupt(format!("component link failed: {e}")))?;
        Ok(Some((prepared, bytes)))
    }

    fn build_spec(&self, key: &PreparedKey) -> TaskSpec {
        (self.build_spec)(key)
    }

    fn reload_cost_hint(&self, key: &PreparedKey) -> Option<Duration> {
        let _ = key;
        None
    }
}

/// `Engine::precompile_component`, injected into `rt-build`.
pub struct WasmPrecompiler {
    engine: Engine,
    engine_fp: u64,
}

impl WasmPrecompiler {
    pub fn new(engine: Engine, engine_fp: u64) -> Self {
        Self { engine, engine_fp }
    }
}

impl ComponentPrecompiler for WasmPrecompiler {
    fn precompile(&self, wasm: &[u8]) -> Result<Vec<u8>, BuildFailure> {
        let bytes = self
            .engine
            .precompile_component(wasm)
            .map_err(|e| BuildFailure::new(BuildFailReason::Internal, format!("component precompile failed: {e}")))?;
        // The pooling allocator's per-instance limits (one memory per instance, ...) are checked when the
        // artifact is loaded, not when it is compiled. Load it here so a component the node could never
        // instantiate fails the build with a reason, instead of every task of the version failing later.
        // SAFETY: `bytes` were produced by this engine just above; deserialising them only checks them
        // against the engine, it runs no guest code.
        unsafe { Component::deserialize(&self.engine, &bytes) }
            .map_err(|e| BuildFailure::new(BuildFailReason::Internal, format!("component precompile failed: {e}")))?;
        Ok(bytes)
    }

    fn engine_fp(&self) -> u64 {
        self.engine_fp
    }
}
