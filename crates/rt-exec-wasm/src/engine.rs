//! The shared `wasmtime::Engine`.
//!
//! One engine for the whole process: component model, epoch interruption (time slice +
//! kill), optional fuel, the pooling allocator **sized from the memory budget** (never a fixed
//! 8192 — WSM-09), threads/shared-memory disabled (no in-guest timers via racing threads).
//!
//! Pool dimensions (WSM-17): `slots` component instances / memories, `slots` async stacks
//! (every running task holds one slot and at most one stack; wasmtime's default is a fixed 1000),
//! and per profile how much stays warm after release: `max_unused_warm_slots` (slots kept affine
//! to their module, so the next instance of that module skips the memory-image remap) and
//! `linear_memory_keep_resident` (bytes zeroed in place instead of decommitted).

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use wasmtime::{Config, Engine, InstanceAllocationStrategy, OptLevel, PoolingAllocationConfig};

/// Upper bound on pool slots regardless of budget.
pub const MAX_POOL_SLOTS: u64 = 10_000;

/// Core instances / tables one pool slot may hold: a component is several core instances (the
/// guest module plus `wit-component` shim and fixup modules) and each module brings its own
/// table, but only the guest owns a linear memory, so memories alone bound the concurrency.
const CORE_PER_SLOT: u32 = 8;
/// Elements one table may hold (funcref tables of JS/Python components need tens of thousands).
pub const MAX_TABLE_ELEMENTS: usize = 100_000;

/// Epoch ticker period (1 ms).
pub const EPOCH_TICK: Duration = Duration::from_millis(1);

/// Pooling allocator dimensions derived from the budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolSizing {
    /// `total_memories = wasm_budget / max_memory_size`, clamped to `1..=MAX_POOL_SLOTS`.
    pub slots: u32,
    /// Plan-maximum linear memory per instance, bytes.
    pub max_memory_bytes: usize,
    /// Bytes of linear memory kept resident on slot release
    /// (`wasm.linear_memory_keep_resident`: 0 / 1 MiB / 8 MiB by profile).
    pub keep_resident: usize,
    /// Released slots kept affine to their module (`wasm.max_unused_warm_slots`: 0 / 100 / 1000
    /// by profile), at most `slots`.
    pub max_unused_warm_slots: u32,
}

/// wasmtime's own `max_unused_warm_slots` default, used until a profile value is applied.
const DEFAULT_WARM_SLOTS: u32 = 100;

impl PoolSizing {
    pub fn from_budget(wasm_budget_bytes: u64, max_memory_bytes: u64, keep_resident: u64) -> Self {
        let per = max_memory_bytes.max(1);
        let slots = (wasm_budget_bytes / per).clamp(1, MAX_POOL_SLOTS) as u32;
        Self {
            slots,
            max_memory_bytes: max_memory_bytes as usize,
            keep_resident: keep_resident as usize,
            max_unused_warm_slots: DEFAULT_WARM_SLOTS.min(slots),
        }
    }

    /// Apply the `wasm.max_unused_warm_slots` knob (capped at the slot count).
    pub fn with_warm_slots(mut self, n: u32) -> Self {
        self.max_unused_warm_slots = n.min(self.slots);
        self
    }

    /// Async (fiber) stacks: one per slot.
    pub fn total_stacks(&self) -> u32 {
        self.slots
    }
}

#[derive(Clone, Debug)]
pub struct WasmEngineConfig {
    pub pool: PoolSizing,
    /// `wasm.fuel`: deterministic instruction-count backstop next to epoch limits.
    pub fuel: bool,
}

/// Report the pool size on the `rt_wasm_pool_slots` gauge (WSM-09). Called once at startup, right
/// after the engine is built from `sizing`.
pub fn publish_pool_slots(metrics: &rt_obs::Metrics, sizing: &PoolSizing) {
    metrics.wasm_pool_slots.set(i64::from(sizing.slots));
}

/// Create the engine.
pub fn build_engine(cfg: &WasmEngineConfig) -> wasmtime::Result<Engine> {
    let mut c = Config::new();
    c.wasm_component_model(true);
    c.cranelift_opt_level(OptLevel::Speed);
    c.epoch_interruption(true);
    c.consume_fuel(cfg.fuel);
    c.wasm_threads(false);

    let mut pool = PoolingAllocationConfig::default();
    pool.total_component_instances(cfg.pool.slots);
    pool.total_core_instances(cfg.pool.slots.saturating_mul(CORE_PER_SLOT));
    pool.total_memories(cfg.pool.slots);
    pool.total_tables(cfg.pool.slots.saturating_mul(CORE_PER_SLOT));
    // SECURITY.md "WASM sandbox": a table holds at most this many elements (WLOAD-005).
    pool.table_elements(MAX_TABLE_ELEMENTS);
    pool.max_memory_size(cfg.pool.max_memory_bytes);
    pool.linear_memory_keep_resident(cfg.pool.keep_resident);
    pool.max_unused_warm_slots(cfg.pool.max_unused_warm_slots.min(cfg.pool.slots));
    pool.total_stacks(cfg.pool.total_stacks());
    c.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));

    Engine::new(&c)
}

/// FNV-1a as a `Hasher` (stable across runs and Rust versions, unlike `DefaultHasher`).
struct Fnv(u64);

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

/// Fingerprint of the compiled-code compatibility of `engine` (wasmtime version + settings).
/// Part of every `PreparedKey`; a runtime upgrade changes it and rebuilds lazily from T3.
pub fn engine_fingerprint(engine: &Engine) -> u64 {
    let mut h = Fnv(0xcbf2_9ce4_8422_2325);
    engine.precompile_compatibility_hash().hash(&mut h);
    h.finish()
}

/// Background thread incrementing the engine epoch every [`EPOCH_TICK`].
pub struct EpochTicker {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl EpochTicker {
    pub fn spawn(engine: Engine, tick: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = std::thread::Builder::new()
            .name("rt-wasm-epoch".into())
            .spawn(move || {
                while !flag.load(Ordering::Relaxed) {
                    std::thread::sleep(tick);
                    engine.increment_epoch();
                }
            })
            .expect("spawn epoch ticker thread");
        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
