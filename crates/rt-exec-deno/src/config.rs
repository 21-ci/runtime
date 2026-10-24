//! Deno executor settings (docs/config.md `[deno]`).

use std::sync::Arc;
use std::time::Duration;

use rt_config::ClockMode;
use rt_sched::WarmTargets;

/// V8 flags always applied in addition to `deno.v8_flags`.
pub const BASE_V8_FLAGS: &[&str] = &["--single-threaded-gc"];

/// Heap growth step requested from the governor by the near-heap-limit callback.
pub const HEAP_GROW_STEP_MB: u32 = 8;

#[derive(Clone, Debug)]
pub struct DenoExecutorConfig {
    /// Initial per-task heap reservation (`deno.isolate_base_mb`, 4 MB).
    pub isolate_base_mb: u32,
    /// V8 platform worker threads (`deno.v8_platform_threads`, 1) — V8 defaults to ~one per core
    /// and would compete with the Ps (RP-01).
    pub v8_platform_threads: u32,
    /// Finished isolates queued for idle-time disposal before disposing inline
    /// (`deno.dispose_backlog`, 8).
    pub dispose_backlog: u32,
    pub v8_flags: Vec<String>,
    /// Pre-created isolates per P (`deno.isolate_pool_per_p`: 0 / 1 / 4 by profile).
    pub isolate_pool_per_p: u32,
    /// `security.clock`.
    pub clock: ClockMode,
    /// Backstop for `WebAssembly.Memory` (B5.1): no single wasm memory may grow past this, applied
    /// as V8's process-wide `--wasm-max-mem-pages` when V8 is initialized. The largest
    /// `limits_max.memory_mb` of the configured plans: no task may use more anyway. The exact
    /// per-task limit is enforced by sampling (`memory.rs`). 0 = no flag.
    pub wasm_max_memory_mb: u32,
    /// Warm reuse (`deno.reuse_isolates`, D-A4): evaluated isolates kept idle per service version
    /// on this Machine when the scheduler publishes no warm target for it ([`WarmTargets`]).
    pub reuse_max_idle: u32,
    /// Idle evaluated isolates kept on this Machine across all services (the oldest goes first).
    pub reuse_max_idle_total: u32,
    /// A kept isolate unused for this long is disposed, except the warm floor: as many isolates of a
    /// service as the scheduler's published target stay however long they are idle (D-B2).
    pub reuse_idle_ttl: Duration,
    /// Tasks one isolate may serve before it is replaced by a fresh one.
    pub reuse_max_uses: u32,
    /// Warm-capacity targets published by the scheduler (D-A3/D-A4): sizes the kept set per
    /// service. `None` = [`DenoExecutorConfig::reuse_max_idle`] for every service.
    pub warm_targets: Option<Arc<WarmTargets>>,
}

impl Default for DenoExecutorConfig {
    fn default() -> Self {
        Self {
            isolate_base_mb: 4,
            v8_platform_threads: 1,
            dispose_backlog: 8,
            v8_flags: BASE_V8_FLAGS.iter().map(|s| s.to_string()).collect(),
            isolate_pool_per_p: 1,
            clock: ClockMode::Frozen,
            wasm_max_memory_mb: rt_types::PlanLimitsMax::default().memory_mb,
            reuse_max_idle: 4,
            reuse_max_idle_total: 16,
            reuse_idle_ttl: Duration::from_secs(60),
            reuse_max_uses: 1000,
            warm_targets: None,
        }
    }
}

impl DenoExecutorConfig {
    pub fn from_config(cfg: &rt_config::Config) -> Self {
        let knobs = cfg.node_knobs();
        Self {
            isolate_base_mb: cfg.deno.isolate_base_mb,
            v8_platform_threads: cfg.deno.v8_platform_threads,
            dispose_backlog: cfg.deno.dispose_backlog,
            v8_flags: cfg.deno.v8_flags.clone(),
            isolate_pool_per_p: knobs.deno_isolate_pool_per_p.value,
            clock: cfg.security.clock,
            wasm_max_memory_mb: cfg
                .plans
                .values()
                .map(|p| p.limits_max.memory_mb)
                .max()
                .unwrap_or(rt_types::PlanLimitsMax::default().memory_mb),
            reuse_max_idle: cfg.deno.reuse_max_idle,
            reuse_max_idle_total: cfg.deno.reuse_max_idle_total,
            reuse_idle_ttl: cfg.deno.reuse_idle_ttl,
            reuse_max_uses: cfg.deno.reuse_max_uses.max(1),
            warm_targets: None,
        }
    }

    /// Size the kept warm sets from the scheduler's published targets (D-A3/D-A4).
    pub fn with_warm_targets(mut self, targets: Arc<WarmTargets>) -> Self {
        self.warm_targets = Some(targets);
        self
    }

    /// V8 flags derived from this config that are not part of the engine fingerprint (they do not
    /// change compiled code): `--wasm-max-mem-pages` (64 KiB pages).
    pub fn derived_v8_flags(&self) -> Vec<String> {
        if self.wasm_max_memory_mb == 0 {
            return Vec::new();
        }
        vec![format!(
            "--wasm-max-mem-pages={}",
            u64::from(self.wasm_max_memory_mb) * 16
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wasm_memory_backstop_is_the_largest_plan_memory_in_64k_pages() {
        assert_eq!(
            DenoExecutorConfig::default().derived_v8_flags(),
            vec!["--wasm-max-mem-pages=2048".to_string()]
        );

        let mut cfg = rt_config::Config::default();
        for (name, mb) in [("small", 64), ("big", 512)] {
            let mut plan = rt_types::Plan::default();
            plan.limits_max.memory_mb = mb;
            cfg.plans.insert(name.to_string(), plan);
        }
        let d = DenoExecutorConfig::from_config(&cfg);
        assert_eq!(d.wasm_max_memory_mb, 512);
        assert_eq!(d.derived_v8_flags(), vec!["--wasm-max-mem-pages=8192".to_string()]);
        assert!(
            !d.v8_flags.iter().any(|f| f.contains("wasm-max-mem")),
            "not part of the engine fingerprint"
        );

        let off = DenoExecutorConfig {
            wasm_max_memory_mb: 0,
            ..DenoExecutorConfig::default()
        };
        assert!(off.derived_v8_flags().is_empty());
    }

    #[test]
    fn the_reuse_limits_come_from_the_deno_section() {
        let d = DenoExecutorConfig::from_config(&rt_config::Config::default());
        let e = DenoExecutorConfig::default();
        assert_eq!(
            (
                d.reuse_max_idle,
                d.reuse_max_idle_total,
                d.reuse_idle_ttl,
                d.reuse_max_uses
            ),
            (
                e.reuse_max_idle,
                e.reuse_max_idle_total,
                e.reuse_idle_ttl,
                e.reuse_max_uses
            ),
            "rt.toml defaults and the executor's agree"
        );
        let mut cfg = rt_config::Config::default();
        cfg.deno.reuse_max_idle = 2;
        cfg.deno.reuse_max_idle_total = 5;
        cfg.deno.reuse_idle_ttl = Duration::from_secs(9);
        cfg.deno.reuse_max_uses = 0;
        let d = DenoExecutorConfig::from_config(&cfg);
        assert_eq!(
            (d.reuse_max_idle, d.reuse_max_idle_total, d.reuse_idle_ttl),
            (2, 5, Duration::from_secs(9))
        );
        assert_eq!(d.reuse_max_uses, 1, "at least the task that evaluated the module");
    }
}
