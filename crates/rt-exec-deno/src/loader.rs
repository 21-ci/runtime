//! Glue between the executor, `rt-cache` and `rt-build`: T2 → T1 loading, and the V8 code cache
//! producer injected into the builder.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use deno_core::{JsRuntime, ModuleSpecifier, PollEventLoopOptions, RuntimeOptions};
use rt_build::{Bundle, CodeCache, CodeCacheProducer, ModuleKind};
use rt_cache::{LoadError, Loader, T2Error, T2Store};
use rt_types::{BuildFailReason, BuildFailure, PreparedKey, TaskSpec};

use crate::bundle_loader::ProducerLoader;
use crate::config::DenoExecutorConfig;
use crate::prepared::{init_v8_once, DenoPrepared};
use crate::snapshot::{base_runtime_options, seal};

/// T2 file names of a Deno entry.
pub const T2_BUNDLE: &str = "bundle";
pub const T2_CODE_CACHE: &str = "code_cache.bin";

/// `rt-cache` loader for Deno entries: reads `bundle` + `code_cache.bin` from T2 (MAC-verified),
/// parses the bundle, and falls back to compile when V8 rejects the code cache (DNO-10).
pub struct DenoLoader {
    _t2: Arc<T2Store>,
    /// Builds the `Build` task spec for a key (needs the service ref the cache does not know).
    _build_spec: Arc<dyn Fn(&PreparedKey) -> TaskSpec + Send + Sync>,
}

impl DenoLoader {
    pub fn new(t2: Arc<T2Store>, build_spec: Arc<dyn Fn(&PreparedKey) -> TaskSpec + Send + Sync>) -> Self {
        Self {
            _t2: t2,
            _build_spec: build_spec,
        }
    }
}

impl Loader for DenoLoader {
    type Value = DenoPrepared;

    fn load_t2(&self, key: &PreparedKey) -> Result<Option<(DenoPrepared, u64)>, LoadError> {
        let tamper = |e: T2Error| match e {
            T2Error::Tampered => LoadError::Tampered,
            T2Error::Io(e) => LoadError::Io(e),
        };
        let Some(bundle_bytes) = self._t2.read_verified(key, T2_BUNDLE).map_err(tamper)? else {
            return Ok(None);
        };
        let bundle = Bundle::from_bytes(&bundle_bytes).map_err(|e| LoadError::Corrupt(e.to_string()))?;
        let mut accounted = bundle_bytes.len() as u64;
        // A missing or unparsable code cache only costs a compile; a tampered one is an error.
        let code_cache = match self._t2.read_verified(key, T2_CODE_CACHE).map_err(tamper)? {
            Some(b) => {
                accounted += b.len() as u64;
                CodeCache::from_bytes(&b).ok()
            }
            None => None,
        };
        let handler_form = bundle.handler_form;
        Ok(Some((
            DenoPrepared {
                key: *key,
                bundle,
                code_cache,
                handler_form,
                accounted_bytes: accounted,
                reads_env: Default::default(),
            },
            accounted,
        )))
    }

    fn build_spec(&self, key: &PreparedKey) -> TaskSpec {
        (self._build_spec)(key)
    }

    fn reload_cost_hint(&self, _key: &PreparedKey) -> Option<Duration> {
        None
    }
}

/// Produces V8 code caches in a throwaway isolate created from the base snapshot.
pub struct V8CodeCacheProducer {
    engine_fp: u64,
}

impl V8CodeCacheProducer {
    pub fn new(engine_fp: u64) -> Self {
        Self { engine_fp }
    }
}

/// A private tokio runtime that is dropped without blocking: `produce` runs inside the builder's
/// async task, where dropping a `Runtime` normally (it waits for its blocking pool) panics.
struct PrivateRuntime(Option<tokio::runtime::Runtime>);

impl Drop for PrivateRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Minimal executor: the futures here (module compilation) never wait on I/O, and a nested
/// `tokio` `block_on` would panic when the build runs on a runtime thread.
fn block_on<F: Future>(fut: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut fut = Box::pin(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::park_timeout(Duration::from_millis(20)),
        }
    }
}

// LCOV_EXCL_START: only the failures of V8 itself (no snapshot, no isolate, no code cache for a compiled module) call it
fn internal(msg: impl Into<String>) -> BuildFailure {
    BuildFailure::new(BuildFailReason::Internal, msg)
}
// LCOV_EXCL_STOP

impl CodeCacheProducer for V8CodeCacheProducer {
    fn produce(&self, bundle: &Bundle) -> Result<CodeCache, BuildFailure> {
        init_v8_once(&DenoExecutorConfig::default());
        // `JsRuntime` needs a tokio context (delayed V8 tasks); enter a private one.
        let tokio_rt = PrivateRuntime(Some(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| internal(format!("cannot start the compile runtime: {e}")))?,
        ));
        let _guard = tokio_rt.0.as_ref().map(tokio::runtime::Runtime::enter);

        let produced: Rc<RefCell<HashMap<String, Vec<u8>>>> = Rc::default();
        let loader = ProducerLoader {
            bundle: bundle.clone(),
            produced: produced.clone(),
        };
        let options = RuntimeOptions {
            module_loader: Some(Rc::new(loader)),
            ..base_runtime_options().map_err(|e| internal(format!("no base snapshot: {e}")))?
        };
        let mut runtime =
            JsRuntime::try_new(options).map_err(|e| internal(format!("cannot create the compile isolate: {e}")))?;
        seal(&mut runtime).map_err(internal)?;

        // Compile (load + instantiate), never evaluate.
        let js: Vec<&str> = bundle
            .modules
            .iter()
            .filter(|m| m.kind == ModuleKind::Js)
            .map(|m| m.specifier.as_str())
            .collect();
        for spec in &js {
            let url =
                ModuleSpecifier::parse(spec).map_err(|e| internal(format!("bad module specifier {spec}: {e}")))?;
            block_on(runtime.load_side_es_module(&url))
                .map_err(|e| BuildFailure::new(BuildFailReason::Syntax, format!("{spec}: {e}")))?;
        }
        // Cache-ready callbacks are delivered by the event loop.
        block_on(runtime.run_event_loop(PollEventLoopOptions::default()))
            .map_err(|e| internal(format!("compile event loop: {e}")))?;

        let mut per_module = std::collections::BTreeMap::new();
        for spec in js {
            match produced.borrow().get(spec) {
                Some(blob) if !blob.is_empty() => {
                    per_module.insert(spec.to_string(), blob.clone());
                }
                _ => return Err(internal(format!("V8 produced no code cache for {spec}"))), // LCOV_EXCL_LINE: V8 caches every module it compiled
            }
        }
        Ok(CodeCache {
            engine_fp: self.engine_fp,
            per_module,
        })
    }

    fn engine_fp(&self) -> u64 {
        self.engine_fp
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use super::*;

    /// Pending once, waking itself from another thread the way a timer would.
    struct PendOnce(bool);

    impl Future for PendOnce {
        type Output = u32;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
            if self.0 {
                return Poll::Ready(7);
            }
            self.0 = true;
            let waker = cx.waker().clone();
            std::thread::spawn(move || waker.wake());
            Poll::Pending
        }
    }

    #[test]
    fn block_on_parks_until_the_future_wakes_it() {
        assert_eq!(block_on(PendOnce(false)), 7);
    }

    #[test]
    fn block_on_polls_a_future_that_never_waits_once() {
        assert_eq!(block_on(async { 3 }), 3);
    }
}
