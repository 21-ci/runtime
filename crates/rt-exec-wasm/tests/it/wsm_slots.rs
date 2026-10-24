//! WSM-19: idle instances and pool slots. Idle (reusable or warm) instances hold pooling-allocator
//! slots, which running tasks need too:
//! - one process-wide budget (`IdleSlots`, shared by every Machine's executor) bounds them;
//!   when it is full, a new idle instance replaces the least recently used one of any Machine;
//! - a task whose instantiation finds the pool exhausted evicts idle instances (least recently
//!   used first, any Machine) and retries; any other instantiation failure evicts nothing.

use crate::common;

use std::time::Duration;

use common::{prepared, prepared_on, small_engine, Req, Rig};
use rt_exec_wasm::IdleSlots;
use rt_types::KillReason;

#[tokio::test]
async fn ac_wsm_19_the_idle_budget_is_shared_by_all_machines_and_evicts_lru() {
    let slots = IdleSlots::new(1);
    let mut a = Rig::new().idle_slots(slots.clone());
    let mut b = Rig::new().idle_slots(slots.clone());
    let counter = prepared("counter");

    assert_eq!(a.run(&counter, Req::get("/").reuse().service(1)).await.text(), "1");
    assert_eq!((slots.held(), a.exec.idle_instances()), (1, 1));
    // Machine B's idle instance replaces A's, which was idle longer.
    assert_eq!(b.run(&counter, Req::get("/").reuse().service(2)).await.text(), "1");
    assert_eq!(
        (slots.held(), a.exec.idle_instances(), b.exec.idle_instances()),
        (1, 0, 1)
    );
    assert_eq!(b.run(&counter, Req::get("/").reuse().service(2)).await.text(), "2");
    assert_eq!(
        a.run(&counter, Req::get("/").reuse().service(1)).await.text(),
        "1",
        "A's instance was evicted"
    );
}

#[tokio::test]
async fn ac_wsm_19_pool_exhaustion_evicts_another_machines_idle_instances_and_retries() {
    let (engine, _ticker) = small_engine(2);
    let counter = prepared_on(&engine, "counter");
    let slots = IdleSlots::new(2);
    let mut a = Rig::on_engine(engine.clone(), 512, Duration::from_millis(1)).idle_slots(slots.clone());
    let mut b = Rig::on_engine(engine.clone(), 512, Duration::from_millis(1)).idle_slots(slots.clone());

    // B's two idle instances fill the whole pool.
    assert_eq!(b.run(&counter, Req::get("/").reuse().service(1)).await.text(), "1");
    assert_eq!(b.run(&counter, Req::get("/").reuse().service(2)).await.text(), "1");
    assert_eq!(slots.held(), 2);

    // A has no idle instance of its own: it must take a slot from B, the least recently used.
    assert_eq!(a.run(&counter, Req::get("/").service(3)).await.text(), "1");
    assert_eq!(b.exec.idle_instances(), 1);
    assert_eq!(
        b.run(&counter, Req::get("/").reuse().service(2)).await.text(),
        "2",
        "the more recent one stays"
    );
    assert_eq!(
        b.run(&counter, Req::get("/").reuse().service(1)).await.text(),
        "1",
        "the older one was evicted"
    );
}

#[tokio::test]
async fn ac_wsm_19_other_instantiation_failures_evict_nothing() {
    let slots = IdleSlots::new(4);
    let mut a = Rig::new().idle_slots(slots.clone());
    let mut b = Rig::new().idle_slots(slots.clone());
    let counter = prepared("counter");
    b.run(&counter, Req::get("/").reuse().tenant(2)).await.text();
    assert_eq!(slots.held(), 1);
    // A task whose memory limit cannot even hold the initial memory fails to instantiate.
    let killed = a.run(&counter, Req::get("/").memory_mb(0)).await.killed();
    assert_eq!(killed, KillReason::Oom);
    assert_eq!(
        (slots.held(), b.exec.idle_instances()),
        (1, 1),
        "another tenant's idle instance was flushed"
    );
}

#[test]
fn ac_wsm_19_the_node_budget_is_half_the_pool() {
    let sizing = rt_exec_wasm::PoolSizing::from_budget(32 * 128 * 1024 * 1024, 128 * 1024 * 1024, 0);
    assert_eq!(IdleSlots::for_pool(&sizing).cap(), 16);
    let tiny = rt_exec_wasm::PoolSizing::from_budget(3 * 128 * 1024 * 1024, 128 * 1024 * 1024, 0);
    assert_eq!(
        IdleSlots::for_pool(&tiny).cap(),
        1,
        "a 3-slot pool keeps two slots for running tasks"
    );
}
