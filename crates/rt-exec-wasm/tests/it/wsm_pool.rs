//! WSM-09: pool sizing derives from the memory budget (never a fixed 8192), startup succeeds,
//! `rt_wasm_pool_slots` reports it. Plus the engine's disabled proposals (SECURITY.md).

use rt_exec_wasm::{
    build_engine, engine_fingerprint, publish_pool_slots, PoolSizing, WasmEngineConfig, MAX_POOL_SLOTS,
};
use rt_obs::{Metrics, MetricsOptions};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

fn cfg(sizing: PoolSizing) -> WasmEngineConfig {
    WasmEngineConfig {
        pool: sizing,
        fuel: false,
    }
}

#[test]
fn ac_wsm_09_container_2g_pool_slots_derive_from_the_budget() {
    // `--memory=2g`: the budget is 85 % of the container limit (MEM-08: 1,740 MB)
    let budget = rt_mem::budget_from_limit(2 * GIB);
    assert_eq!(budget / MIB, 1740);

    let sizing = PoolSizing::from_budget(budget, 128 * MIB, 0);
    // total_memories = wasm_budget / max_memory_size
    assert_eq!(u64::from(sizing.slots), budget / (128 * MIB));
    assert_eq!(sizing.slots, 13);
    assert_eq!(sizing.max_memory_bytes as u64, 128 * MIB);
    assert_ne!(sizing.slots, 8192, "the pool must never use a fixed 8192 slots");
}

#[test]
fn ac_wsm_09_slots_scale_with_budget_and_plan_maximum() {
    let slots = |budget: u64, max_mem: u64| PoolSizing::from_budget(budget, max_mem, 0).slots;
    assert_eq!(slots(GIB, 128 * MIB), 8);
    assert_eq!(slots(4 * GIB, 128 * MIB), 32);
    assert_eq!(slots(4 * GIB, 64 * MIB), 64, "a smaller plan maximum fits more slots");
    assert_eq!(slots(4 * GIB, 256 * MIB), 16);
    // never zero, never above the cap
    assert_eq!(slots(MIB, 128 * MIB), 1);
    assert_eq!(slots(0, 128 * MIB), 1);
    assert_eq!(u64::from(slots(1 << 50, MIB)), MAX_POOL_SLOTS);
}

#[test]
fn ac_wsm_09_keep_resident_comes_from_the_profile() {
    for keep in [0, MIB, 8 * MIB] {
        assert_eq!(PoolSizing::from_budget(GIB, 128 * MIB, keep).keep_resident as u64, keep);
    }
}

#[test]
fn ac_wsm_09_startup_succeeds_and_the_gauge_reports_the_slot_count() {
    let sizing = PoolSizing::from_budget(rt_mem::budget_from_limit(2 * GIB), 128 * MIB, MIB);
    let _engine = build_engine(&cfg(sizing)).expect("engine startup with a budget-derived pool");

    let metrics = Metrics::new(MetricsOptions::default()).expect("register metrics");
    publish_pool_slots(&metrics, &sizing);
    assert_eq!(metrics.wasm_pool_slots.get(), i64::from(sizing.slots));
    let text = metrics.render();
    let expected = format!("rt_wasm_pool_slots {}", sizing.slots);
    assert!(
        text.lines().any(|l| l.trim() == expected),
        "`{expected}` missing from the scrape:\n{text}"
    );
}

#[test]
fn wsm_engine_rejects_shared_memory_and_threads() {
    // Disabled proposals: no threads / shared memory (no in-guest timers via racing threads)
    let engine = build_engine(&cfg(PoolSizing::from_budget(GIB, 128 * MIB, 0))).expect("engine");
    let shared = wasmtime::Module::new(&engine, "(module (memory 1 1 shared))");
    assert!(shared.is_err(), "a module with shared memory must not compile");
    let plain = wasmtime::Module::new(&engine, "(module (memory 1 1))");
    assert!(plain.is_ok(), "{:?}", plain.err());
}

#[test]
fn wsm_engine_fingerprint_is_stable_for_equal_config() {
    let a = build_engine(&cfg(PoolSizing::from_budget(GIB, 128 * MIB, 0))).expect("engine a");
    let b = build_engine(&cfg(PoolSizing::from_budget(GIB, 128 * MIB, 0))).expect("engine b");
    assert_eq!(
        engine_fingerprint(&a),
        engine_fingerprint(&b),
        "PreparedKey.engine_fp must be reproducible"
    );
}

// ---------------------------------------------------------------------------------------------
// WSM-17: stacks, warm slots and resident memory per profile
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_wsm_17_async_stacks_follow_the_slot_count_not_the_wasmtime_default() {
    // Every running task holds one pool slot and at most one async stack, so the stack pool is
    // sized like the slot pool: not wasmtime's fixed 1000 (too many for a small box, too few
    // for a big one).
    let small = PoolSizing::from_budget(GIB, 128 * MIB, 0);
    assert_eq!((small.slots, small.total_stacks()), (8, 8));
    let big = PoolSizing::from_budget(1 << 50, MIB, 0);
    assert_eq!(u64::from(big.total_stacks()), MAX_POOL_SLOTS);
    build_engine(&cfg(small)).expect("engine with a slot-sized stack pool");
}

#[test]
fn ac_wsm_17_warm_slots_come_from_the_profile_and_are_capped_by_the_pool() {
    use rt_config::profile::node_knobs;
    use rt_types::Profile;
    let budget = 2 * GIB; // 128 slots of 16 MiB
    for (p, want) in [
        (Profile::MaxDensity, 0),
        (Profile::Balanced, 100),
        (Profile::MinLatency, 128),
    ] {
        let k = node_knobs(p);
        let sizing = PoolSizing::from_budget(budget, 16 * MIB, k.wasm_linear_memory_keep_resident)
            .with_warm_slots(k.wasm_max_unused_warm_slots);
        assert_eq!(sizing.max_unused_warm_slots, want, "{p}: min(profile value, slots)");
        build_engine(&cfg(sizing)).unwrap_or_else(|e| panic!("{p}: engine: {e:#}"));
    }
    // the profiles keep more warm and resident in the latency direction
    let (d, l) = (node_knobs(Profile::MaxDensity), node_knobs(Profile::MinLatency));
    assert!(d.wasm_max_unused_warm_slots < l.wasm_max_unused_warm_slots);
    assert!(d.wasm_linear_memory_keep_resident < l.wasm_linear_memory_keep_resident);
}

/// A core module with one funcref table of `min` elements, encoded by hand.
fn module_with_table(min: u32) -> Vec<u8> {
    let mut leb = Vec::new();
    let mut v = min;
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            leb.push(byte);
            break;
        }
        leb.push(byte | 0x80);
    }
    // table section: 1 table, funcref, limits without a maximum, min.
    let mut section = vec![0x01, 0x70, 0x00];
    section.extend(&leb);
    let mut m = b"\0asm\x01\0\0\0".to_vec();
    m.push(0x04);
    m.push(section.len() as u8);
    m.extend(section);
    m
}

/// WLOAD-005 (SECURITY.md "WASM sandbox"): tables are capped at 100,000 elements: a module
/// declaring exactly that many instantiates, one more is refused.
#[test]
fn wload_005_tables_are_capped_at_100_000_elements() {
    let engine = build_engine(&cfg(PoolSizing::from_budget(GIB, 128 * MIB, 0))).expect("engine");
    let instantiate = |n: u32| {
        let module = wasmtime::Module::new(&engine, module_with_table(n))?;
        let mut store = wasmtime::Store::new(&engine, ());
        wasmtime::Instance::new(&mut store, &module, &[]).map(|_| ())
    };
    instantiate(100_000).expect("exactly the cap");
    assert!(instantiate(100_001).is_err(), "over the cap");
}
