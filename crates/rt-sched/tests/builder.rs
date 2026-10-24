//! `SchedulerBuilder` validation and configuration defaults.

mod common;

use std::sync::Arc;
use std::time::Duration;

use rt_sched::{
    KindRange, LiveSchedConfig, SchedConfig, SchedError, Scheduler, SchedulerBuilder, SimClock, SimScheduler,
    UnitSource,
};
use rt_testkit::{MockDenoExecutor, MockWasmExecutor, ScriptBook, TestGovernor, VirtualClock};
use rt_types::TaskKind;

fn builder(cfg: SchedConfig) -> (SchedulerBuilder, Arc<VirtualClock>) {
    let clock = Arc::new(VirtualClock::new());
    (Scheduler::builder(cfg, clock.clone(), TestGovernor::with_mb(64)), clock)
}

fn expect_err(r: Result<SimScheduler, SchedError>) -> SchedError {
    match r {
        Ok(_) => panic!("the builder accepted an invalid configuration"),
        Err(e) => e,
    }
}

#[test]
fn building_without_executors_is_rejected() {
    let (b, clock) = builder(SchedConfig::default());
    let e = expect_err(b.build_sim(clock as Arc<dyn SimClock>));
    assert!(matches!(e, SchedError::NoExecutors), "{e:?}");
    assert_eq!(e.to_string(), "no executor registered");
}

#[test]
fn registering_a_kind_twice_is_rejected() {
    let (b, clock) = builder(SchedConfig::default());
    let b = b
        .executor(|| MockDenoExecutor::new(ScriptBook::new()), Arc::new(UnitSource), 0)
        .executor(|| MockDenoExecutor::new(ScriptBook::new()), Arc::new(UnitSource), 0);
    let e = expect_err(b.build_sim(clock as Arc<dyn SimClock>));
    assert!(
        matches!(
            e,
            SchedError::DuplicateKind(TaskKind::Exec(rt_types::RuntimeKind::Deno))
        ),
        "{e:?}"
    );
    assert!(e.to_string().contains("deno"), "{e}");
}

#[test]
fn zero_processors_is_rejected() {
    let (b, clock) = builder(SchedConfig {
        processors: 0,
        ..SchedConfig::default()
    });
    let b = b.executor(|| MockWasmExecutor::new(ScriptBook::new()), Arc::new(UnitSource), 0);
    let e = expect_err(b.build_sim(clock as Arc<dyn SimClock>));
    match e {
        SchedError::InvalidConfig(msg) => assert!(msg.contains("processors"), "{msg}"),
        other => panic!("expected InvalidConfig, got {other:?}"),
    }
}

#[test]
fn the_default_config_is_the_balanced_profile_with_four_processors() {
    let c = SchedConfig::default();
    assert_eq!(c.processors, 4);
    assert_eq!(c.deno, KindRange { min: 1, max: 0 });
    assert_eq!(c.wasm, KindRange { min: 1, max: 0 });
    assert_eq!(c.build_processors, 1);
    assert_eq!(c.spare_machines, 2);
    assert_eq!(c.lrq_capacity, 256);
    assert_eq!(c.max_inflight, 10_000);
    assert_eq!(c.grq_fairness_tick, 61);
    assert!((c.rebalance_up_threshold - 2.0).abs() < 1e-9);
    assert_eq!(c.rebalance_cooldown, Duration::from_secs(1));
    assert_eq!(c.live, LiveSchedConfig::default());
}
