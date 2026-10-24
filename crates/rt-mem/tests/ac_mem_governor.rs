//! rt-mem acceptance tests: MEM-01 (budget + RAII incl. panic unwind), MEM-02 (two
//! growers, exactly one wins), MEM-08 (cgroup `memory.max` -> 85 % budget), the pressure ladder and
//! its subscriptions, purge epochs, IO buffer pools.
//!
//! `Governor` and `IoBufferPools` are `todo!()` in the skeleton: tests that construct them are
//! expected to fail until implemented. `Reservation` and the budget functions are real and their
//! tests already pass.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use rt_mem::{
    budget_from_limit, detect_budget, detect_budget_in, parse_cgroup_limit, Budget, BudgetSource, Governor,
    GovernorConfig, IoBufferPools, MemCategory, MemError, MemoryGovernor, Owner, Pressure, PurgeLevel, Reservation,
    ReservationBackend, SizeClass, BUDGET_FRACTION_PERCENT,
};
use rt_types::{ServiceId, TaskId, TenantId};

const MIB: u64 = 1024 * 1024;

fn gov(budget_mib: u64) -> Arc<Governor> {
    Governor::new(GovernorConfig::balanced(budget_mib * MIB))
}

fn committed(g: &Governor) -> u64 {
    g.stats().committed_bytes
}

fn in_category(g: &Governor, cat: MemCategory) -> u64 {
    g.stats().by_category.get(&cat).copied().unwrap_or(0)
}

fn reserve(g: &Governor, cat: MemCategory, mib: u64) -> Reservation {
    g.reserve(cat, mib * MIB, Owner::NONE)
        .unwrap_or_else(|e| panic!("reserve {mib} MiB of {cat}: {e}"))
}

// ---------------------------------------------------------------------------------------------
// Reservation (RAII) against a counting backend: real code, runs today
// ---------------------------------------------------------------------------------------------

/// Backend with a hard cap that records every release call.
struct Counting {
    cap: u64,
    used: AtomicU64,
    release_calls: AtomicU64,
}

impl Counting {
    fn new(cap: u64, used: u64) -> Arc<Self> {
        Arc::new(Self {
            cap,
            used: AtomicU64::new(used),
            release_calls: AtomicU64::new(0),
        })
    }
    fn used(&self) -> u64 {
        self.used.load(Ordering::SeqCst)
    }
    fn releases(&self) -> u64 {
        self.release_calls.load(Ordering::SeqCst)
    }
}

impl ReservationBackend for Counting {
    fn try_grow(&self, _cat: MemCategory, by: u64) -> bool {
        let mut cur = self.used.load(Ordering::SeqCst);
        loop {
            match cur.checked_add(by) {
                Some(next) if next <= self.cap => {
                    match self
                        .used
                        .compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst)
                    {
                        Ok(_) => return true,
                        Err(actual) => cur = actual,
                    }
                }
                _ => return false,
            }
        }
    }
    fn release(&self, _cat: MemCategory, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::SeqCst);
        self.release_calls.fetch_add(1, Ordering::SeqCst);
    }
}

fn charged(backend: &Arc<Counting>, bytes: u64) -> Reservation {
    Reservation::new(backend.clone(), MemCategory::DenoHeap, Owner::NONE, bytes)
}

#[test]
fn dropping_a_reservation_releases_exactly_its_bytes_once() {
    let b = Counting::new(100, 60);
    let r = charged(&b, 60);
    assert_eq!(r.bytes(), 60);
    drop(r);
    assert_eq!((b.used(), b.releases()), (0, 1));
}

#[test]
fn moving_a_reservation_does_not_double_release() {
    let b = Counting::new(100, 40);
    let r = charged(&b, 40);
    let moved = r; // move, not copy
    let boxed = Box::new(moved);
    drop(boxed);
    assert_eq!((b.used(), b.releases()), (0, 1));
}

#[test]
fn try_grow_charges_the_backend_and_a_refusal_changes_nothing() {
    let b = Counting::new(100, 60);
    let mut r = charged(&b, 60);
    assert!(r.try_grow(30));
    assert_eq!((r.bytes(), b.used()), (90, 90));
    assert!(!r.try_grow(11), "90 + 11 exceeds the cap of 100");
    assert_eq!(
        (r.bytes(), b.used()),
        (90, 90),
        "a refused grow must not change the reservation or the backend"
    );
    assert!(r.try_grow(10));
    assert_eq!((r.bytes(), b.used()), (100, 100));
    assert!(!r.try_grow(u64::MAX), "overflow must be refused, not wrapped");
    assert_eq!(r.bytes(), 100);
    drop(r);
    assert_eq!(b.used(), 0);
}

#[test]
fn shrink_returns_bytes_immediately_and_is_clamped_to_what_is_held() {
    let b = Counting::new(100, 50);
    let mut r = charged(&b, 50);
    r.shrink(20);
    assert_eq!((r.bytes(), b.used()), (30, 30));
    r.shrink(0);
    assert_eq!(b.releases(), 1, "shrinking by zero must not call the backend");
    r.shrink(1_000);
    assert_eq!(
        (r.bytes(), b.used()),
        (0, 0),
        "over-shrink is clamped to the held amount"
    );
    drop(r);
    assert_eq!(b.used(), 0);
    assert_eq!(b.releases(), 2, "an emptied reservation releases nothing more on drop");
}

#[test]
fn shrink_then_drop_releases_the_total_exactly_once() {
    let b = Counting::new(100, 70);
    let mut r = charged(&b, 70);
    r.shrink(25);
    drop(r);
    assert_eq!(b.used(), 0, "25 on shrink + 45 on drop");
}

#[test]
fn a_reservation_is_released_when_the_holder_panics() {
    let b = Counting::new(100, 60);
    let backend = b.clone();
    let result = catch_unwind(AssertUnwindSafe(move || {
        let _held = charged(&backend, 60);
        panic!("guest trapped while holding memory");
    }));
    assert!(result.is_err());
    assert_eq!((b.used(), b.releases()), (0, 1), "unwinding must run the drop");
}

#[test]
fn reservations_report_category_owner_and_detached_ones_always_grow() {
    let owner = Owner::task(TaskId(7));
    let b = Counting::new(10, 5);
    let r = Reservation::new(b.clone(), MemCategory::T1Cache, owner, 5);
    assert_eq!(r.category(), MemCategory::T1Cache);
    assert_eq!(r.owner(), owner);
    assert_eq!(r.owner().task, Some(TaskId(7)));

    let svc = Owner::service(TenantId::new(), ServiceId::new());
    assert!(svc.tenant.is_some() && svc.service.is_some() && svc.task.is_none());

    let mut d = Reservation::detached(MemCategory::Other, 3);
    assert_eq!(d.bytes(), 3);
    assert!(
        d.try_grow(u64::MAX / 4),
        "a detached reservation has no budget to refuse it"
    );
    d.shrink(1);
    drop(d); // must not panic without a backend
}

// ---------------------------------------------------------------------------------------------
// MEM-01: budget + RAII on the real governor
// ---------------------------------------------------------------------------------------------

#[test]
fn mem_01_reserve_beyond_the_budget_fails_and_dropping_releases() {
    let g = gov(100);
    let sixty = reserve(&g, MemCategory::DenoHeap, 60);
    assert_eq!(committed(&g), 60 * MIB);

    let err = g
        .reserve(MemCategory::WasmLinear, 50 * MIB, Owner::NONE)
        .expect_err("60 + 50 > 100 MiB must be refused");
    assert_eq!(
        err,
        MemError::OverBudget {
            requested: 50 * MIB,
            available: 40 * MIB
        }
    );
    assert_eq!(
        committed(&g),
        60 * MIB,
        "a refused reservation must not leave a residue"
    );
    assert_eq!(in_category(&g, MemCategory::WasmLinear), 0);

    drop(sixty);
    assert_eq!(committed(&g), 0);
    let fifty = reserve(&g, MemCategory::WasmLinear, 50);
    assert_eq!(fifty.bytes(), 50 * MIB);
}

#[test]
fn mem_01_a_panic_while_holding_a_reservation_releases_it() {
    let g = gov(100);
    let g2 = g.clone();
    let result = catch_unwind(AssertUnwindSafe(move || {
        let _held = g2.reserve(MemCategory::DenoHeap, 60 * MIB, Owner::NONE).expect("fits");
        assert_eq!(g2.stats().committed_bytes, 60 * MIB);
        panic!("task panicked while holding 60 MiB");
    }));
    assert!(result.is_err(), "the closure must have panicked");
    assert_eq!(committed(&g), 0, "unwinding must release the reservation");
    assert_eq!(g.stats().live_reservations, 0);
    // the whole budget is usable again
    let again = reserve(&g, MemCategory::DenoHeap, 90);
    assert_eq!(again.bytes(), 90 * MIB);
}

#[test]
fn reservations_account_per_category_and_in_total() {
    let g = gov(1000);
    let a = reserve(&g, MemCategory::DenoHeap, 10);
    let b = reserve(&g, MemCategory::WasmLinear, 20);
    let c = reserve(&g, MemCategory::T1Cache, 5);
    let d = reserve(&g, MemCategory::DenoHeap, 7);
    let s = g.stats();
    assert_eq!(s.budget_bytes, 1000 * MIB);
    assert_eq!(s.committed_bytes, 42 * MIB);
    assert_eq!(s.by_category.get(&MemCategory::DenoHeap), Some(&(17 * MIB)));
    assert_eq!(s.by_category.get(&MemCategory::WasmLinear), Some(&(20 * MIB)));
    assert_eq!(s.by_category.get(&MemCategory::T1Cache), Some(&(5 * MIB)));
    assert_eq!(
        s.by_category.values().sum::<u64>(),
        s.committed_bytes,
        "categories partition the total"
    );
    assert_eq!(s.live_reservations, 4);
    drop(b);
    drop(d);
    let s = g.stats();
    assert_eq!(s.committed_bytes, 15 * MIB);
    assert_eq!(in_category(&g, MemCategory::DenoHeap), 10 * MIB);
    assert_eq!(in_category(&g, MemCategory::WasmLinear), 0);
    assert_eq!(s.live_reservations, 2);
    drop((a, c));
    assert_eq!((committed(&g), g.stats().live_reservations), (0, 0));
}

#[test]
fn growing_and_shrinking_a_governed_reservation_moves_the_governor_accounting() {
    let g = gov(100);
    let mut r = reserve(&g, MemCategory::WasmLinear, 10);
    assert!(r.try_grow(60 * MIB));
    assert_eq!((r.bytes(), committed(&g)), (70 * MIB, 70 * MIB));
    assert!(!r.try_grow(40 * MIB), "70 + 40 > 100 MiB");
    assert_eq!(
        (r.bytes(), committed(&g)),
        (70 * MIB, 70 * MIB),
        "a refused grow changes nothing"
    );
    r.shrink(50 * MIB);
    assert_eq!((r.bytes(), committed(&g)), (20 * MIB, 20 * MIB));
    assert_eq!(in_category(&g, MemCategory::WasmLinear), 20 * MIB);
    drop(r);
    assert_eq!(committed(&g), 0);
}

#[test]
fn reservation_records_its_category_and_owner() {
    let g = gov(100);
    let owner = Owner::task(TaskId(99));
    let r = g.reserve(MemCategory::SqlCache, MIB, owner).unwrap();
    assert_eq!(r.category(), MemCategory::SqlCache);
    assert_eq!(r.owner(), owner);
    assert_eq!(r.bytes(), MIB);
}

#[test]
fn zero_and_absurd_requests_are_handled_without_corrupting_the_accounting() {
    let g = gov(100);
    let _held = reserve(&g, MemCategory::DenoHeap, 60);
    let zero = g
        .reserve(MemCategory::Other, 0, Owner::NONE)
        .expect("zero bytes always fit");
    assert_eq!(zero.bytes(), 0);
    for huge in [u64::MAX, u64::MAX - 10, u64::MAX / 2 + 1] {
        assert!(
            matches!(g.reserve(MemCategory::Other, huge, Owner::NONE), Err(MemError::OverBudget { requested, .. }) if requested == huge),
            "{huge} must be refused as over budget (no wrap-around)"
        );
    }
    assert_eq!(committed(&g), 60 * MIB);
}

#[test]
fn budget_accessors_agree_with_the_configuration() {
    let g = Governor::new(GovernorConfig::balanced(123 * MIB));
    assert_eq!(g.budget_bytes(), 123 * MIB);
    assert_eq!(g.stats().budget_bytes, 123 * MIB);
    assert_eq!(g.pressure(), Pressure::Normal);
    assert_eq!(g.stats().pressure, Pressure::Normal);
    assert_eq!(g.stats().committed_bytes, 0);
}

// ---------------------------------------------------------------------------------------------
// MEM-02: two growers race, exactly one wins
// ---------------------------------------------------------------------------------------------

#[test]
fn mem_02_two_tasks_reserving_120_mib_of_a_200_mib_budget_exactly_one_wins() {
    for round in 0..300 {
        let g = gov(200);
        let barrier = Barrier::new(2);
        let (deno, wasm) = thread::scope(|s| {
            let d = s.spawn(|| {
                barrier.wait();
                g.reserve(MemCategory::DenoHeap, 120 * MIB, Owner::task(TaskId(1)))
            });
            let w = s.spawn(|| {
                barrier.wait();
                g.reserve(MemCategory::WasmLinear, 120 * MIB, Owner::task(TaskId(2)))
            });
            (d.join().unwrap(), w.join().unwrap())
        });
        let winners = u32::from(deno.is_ok()) + u32::from(wasm.is_ok());
        assert_eq!(
            winners, 1,
            "round {round}: exactly one of two 120 MiB reservations fits a 200 MiB budget"
        );
        let loser = if deno.is_err() {
            deno.as_ref().err()
        } else {
            wasm.as_ref().err()
        };
        assert!(
            matches!(loser, Some(MemError::OverBudget { requested, .. }) if *requested == 120 * MIB),
            "round {round}: the loser is refused as over budget, got {loser:?}"
        );
        assert_eq!(
            committed(&g),
            120 * MIB,
            "round {round}: the loser must leave no residue"
        );
        drop((deno, wasm));
        assert_eq!(committed(&g), 0);
    }
}

#[test]
fn mem_02_two_tasks_growing_an_existing_reservation_exactly_one_wins() {
    for round in 0..300 {
        let g = gov(200);
        let barrier = Barrier::new(2);
        let (a_ok, b_ok, ra, rb) = thread::scope(|s| {
            let mut ra = reserve(&g, MemCategory::DenoHeap, 10);
            let mut rb = reserve(&g, MemCategory::WasmLinear, 10);
            let ha = s.spawn(|| {
                barrier.wait();
                let ok = ra.try_grow(110 * MIB);
                (ok, ra)
            });
            let hb = s.spawn(|| {
                barrier.wait();
                let ok = rb.try_grow(110 * MIB);
                (ok, rb)
            });
            let (a_ok, ra) = ha.join().unwrap();
            let (b_ok, rb) = hb.join().unwrap();
            (a_ok, b_ok, ra, rb)
        });
        assert!(
            a_ok ^ b_ok,
            "round {round}: exactly one grow to 120 MiB can fit; got {a_ok} / {b_ok}"
        );
        assert_eq!(committed(&g), 130 * MIB, "round {round}: 120 MiB winner + 10 MiB loser");
        let (win, lose) = if a_ok { (&ra, &rb) } else { (&rb, &ra) };
        assert_eq!((win.bytes(), lose.bytes()), (120 * MIB, 10 * MIB));
    }
}

#[test]
fn mem_02_once_the_winner_is_gone_the_loser_can_retry() {
    let g = gov(200);
    let winner = reserve(&g, MemCategory::DenoHeap, 120);
    assert!(g.reserve(MemCategory::WasmLinear, 120 * MIB, Owner::NONE).is_err());
    drop(winner); // e.g. the winner was killed with `oom`
    assert!(g.reserve(MemCategory::WasmLinear, 120 * MIB, Owner::NONE).is_ok());
}

#[test]
fn concurrent_reserve_grow_shrink_drop_never_exceeds_the_budget_and_balances_to_zero() {
    const BUDGET_MIB: u64 = 64;
    let g = gov(BUDGET_MIB);
    let stop = std::sync::atomic::AtomicBool::new(false);
    let max_seen = AtomicU64::new(0);
    thread::scope(|s| {
        let sampler = s.spawn(|| {
            while !stop.load(Ordering::Relaxed) {
                max_seen.fetch_max(g.stats().committed_bytes, Ordering::Relaxed);
            }
        });
        let workers: Vec<_> = (0..8u64)
            .map(|t| {
                let g = &g;
                s.spawn(move || {
                    let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ t;
                    let mut held: Vec<Reservation> = Vec::new();
                    for _ in 0..3_000 {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        match x % 4 {
                            0 | 1 => {
                                if let Ok(r) = g.reserve(MemCategory::DenoHeap, (1 + x % 4) * MIB, Owner::NONE) {
                                    held.push(r);
                                }
                            }
                            2 => {
                                if let Some(r) = held.last_mut() {
                                    let _ = r.try_grow(MIB);
                                }
                            }
                            _ => {
                                if held.len() > 3 {
                                    let mut r = held.remove(0);
                                    r.shrink(MIB / 2);
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        sampler.join().unwrap();
    });
    assert!(
        max_seen.load(Ordering::Relaxed) <= BUDGET_MIB * MIB,
        "committed exceeded the budget"
    );
    assert_eq!(committed(&g), 0, "everything was dropped, nothing may remain committed");
    assert_eq!(g.stats().live_reservations, 0);
}

// ---------------------------------------------------------------------------------------------
// Pressure ladder
// ---------------------------------------------------------------------------------------------

fn at_percent(g: &Governor, percent: u64) -> Reservation {
    // budget 1000 MiB -> 10 MiB per percent
    g.reserve(MemCategory::T1Cache, percent * 10 * MIB, Owner::NONE)
        .unwrap_or_else(|e| panic!("{percent}%: {e}"))
}

#[test]
fn pressure_follows_the_watermarks_up_and_down() {
    let g = gov(1000); // high 0.80, critical 0.92
    assert_eq!(g.tick(), Pressure::Normal);

    let mut r = at_percent(&g, 79);
    assert_eq!(g.tick(), Pressure::Normal, "79 % is below high");
    assert!(r.try_grow(20 * MIB)); // 81 %
    assert_eq!(g.tick(), Pressure::High, "81 % is above high");
    assert_eq!((g.pressure(), g.stats().pressure), (Pressure::High, Pressure::High));

    assert!(r.try_grow(100 * MIB)); // 91 %
    assert_eq!(g.tick(), Pressure::High, "91 % is still below critical");
    assert!(r.try_grow(20 * MIB)); // 93 %
    assert_eq!(g.tick(), Pressure::Critical, "93 % is above critical");
    assert_eq!(g.stats().pressure, Pressure::Critical);
}

#[test]
fn leaving_high_needs_no_hold() {
    let g = gov(1000);
    let r = at_percent(&g, 85);
    assert_eq!(g.tick(), Pressure::High);
    drop(r);
    assert_eq!(g.tick(), Pressure::Normal);
}

#[test]
fn critical_is_left_only_after_usage_stayed_below_high_for_the_hold() {
    let hold = Duration::from_millis(300);
    let g = Governor::new(GovernorConfig {
        critical_exit_hold: hold,
        ..GovernorConfig::balanced(1000 * MIB)
    });
    let r = at_percent(&g, 95);
    assert_eq!(g.tick(), Pressure::Critical);

    drop(r); // usage 0 %, far below high
    assert_eq!(
        g.tick(),
        Pressure::Critical,
        "must stay Critical until the hold elapsed"
    );
    thread::sleep(hold + Duration::from_millis(150));
    assert_eq!(
        g.tick(),
        Pressure::Normal,
        "below high for longer than the hold -> Normal"
    );
}

#[test]
fn critical_exit_hold_restarts_when_usage_climbs_back_above_high() {
    // Critical is left only after usage was *below high* for the whole hold.
    let hold = Duration::from_millis(600);
    let g = Governor::new(GovernorConfig {
        critical_exit_hold: hold,
        ..GovernorConfig::balanced(1000 * MIB)
    });
    let r = at_percent(&g, 95);
    assert_eq!(g.tick(), Pressure::Critical);
    drop(r);

    thread::sleep(Duration::from_millis(350)); // below high for 350 ms < hold
    assert_eq!(g.tick(), Pressure::Critical);

    let blip = at_percent(&g, 85); // above high again (below critical): the clock must restart
    assert_eq!(
        g.tick(),
        Pressure::Critical,
        "still Critical: usage has not been below high for the hold"
    );
    drop(blip);
    assert_eq!(g.tick(), Pressure::Critical);

    thread::sleep(Duration::from_millis(350)); // 350 ms since the blip (but > 600 ms since the first drop)
    assert_eq!(
        g.tick(),
        Pressure::Critical,
        "the hold counts from the last time usage was above high"
    );
    thread::sleep(hold);
    assert_eq!(g.tick(), Pressure::Normal);
}

#[test]
fn set_watermarks_takes_effect_on_the_next_tick() {
    let g = gov(1000);
    let _r = at_percent(&g, 75);
    assert_eq!(g.tick(), Pressure::Normal);

    g.set_watermarks(0.90, 0.97); // max_density
    assert_eq!(g.tick(), Pressure::Normal);

    g.set_watermarks(0.70, 0.88); // min_latency: 75 % is now above high
    assert_eq!(g.tick(), Pressure::High);

    g.set_watermarks(0.90, 0.97); // back up: leaving High needs no hold
    assert_eq!(g.tick(), Pressure::Normal);

    g.set_watermarks(0.60, 0.74); // 75 % is above the new critical watermark
    assert_eq!(g.tick(), Pressure::Critical);
}

#[test]
fn critical_pressure_refuses_new_reservations_even_when_they_fit() {
    let g = gov(1000);
    let _hog = at_percent(&g, 95);
    assert_eq!(g.tick(), Pressure::Critical);
    for cat in [MemCategory::DenoHeap, MemCategory::T1Cache] {
        assert_eq!(
            g.reserve(cat, MIB, Owner::NONE).err(),
            Some(MemError::Critical),
            "{cat}: 1 MiB fits the budget but the node is critical"
        );
    }
    // over budget is still reported as such, not as pressure
    assert!(matches!(
        g.reserve(MemCategory::DenoHeap, 100 * MIB, Owner::NONE),
        Err(MemError::OverBudget { .. } | MemError::Critical)
    ));
}

// ---------------------------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn subscribers_are_notified_of_each_pressure_change() {
    let g = gov(1000);
    let mut w1 = g.subscribe();
    let mut w2 = g.subscribe();
    assert_eq!((w1.current(), w2.current()), (Pressure::Normal, Pressure::Normal));

    let high = at_percent(&g, 85);
    g.tick();
    for w in [&mut w1, &mut w2] {
        let got = tokio::time::timeout(Duration::from_secs(1), w.changed())
            .await
            .expect("notified within 1 s");
        assert_eq!(got, Some(Pressure::High));
        assert_eq!(w.current(), Pressure::High);
    }

    // a tick that changes nothing must not wake subscribers
    g.tick();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), w1.changed())
            .await
            .is_err(),
        "no spurious notification"
    );

    drop(high);
    g.tick();
    let got = tokio::time::timeout(Duration::from_secs(1), w1.changed())
        .await
        .expect("notified");
    assert_eq!(got, Some(Pressure::Normal));
}

#[tokio::test]
async fn a_subscription_ends_with_none_when_the_governor_is_gone() {
    let g = gov(100);
    let mut w = g.subscribe();
    drop(g);
    let got = tokio::time::timeout(Duration::from_secs(1), w.changed())
        .await
        .expect("must resolve once the governor is dropped");
    assert_eq!(got, None);
}

#[tokio::test]
async fn a_late_subscriber_starts_at_the_current_level() {
    let g = gov(1000);
    let _r = at_percent(&g, 85);
    g.tick();
    assert_eq!(g.subscribe().current(), Pressure::High);
}

// ---------------------------------------------------------------------------------------------
// Purge requests and RSS accounting
// ---------------------------------------------------------------------------------------------

#[test]
fn purge_requests_have_a_strictly_increasing_epoch_and_the_latest_level() {
    let g = gov(100);
    let e0 = g.purge_request().epoch;
    g.request_purge(PurgeLevel::Light);
    let r1 = g.purge_request();
    assert!(r1.epoch > e0);
    assert_eq!(r1.level, PurgeLevel::Light);
    g.request_purge(PurgeLevel::Full);
    let r2 = g.purge_request();
    assert!(r2.epoch > r1.epoch);
    assert_eq!(r2.level, PurgeLevel::Full);
    assert_eq!(g.purge_request(), r2, "reading does not advance the epoch");
}

#[test]
fn concurrent_purge_requests_never_reuse_an_epoch() {
    let g = gov(100);
    let start = g.purge_request().epoch;
    thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                for _ in 0..100 {
                    g.request_purge(PurgeLevel::Light);
                }
            });
        }
    });
    assert!(
        g.purge_request().epoch >= start + 800,
        "every request must advance the epoch"
    );
}

#[test]
fn rss_and_fragmentation_are_reported_once_observed() {
    let g = gov(1000);
    let fresh = g.stats();
    assert_eq!(fresh.rss_bytes, 0, "unknown RSS reads as 0");
    assert_eq!(fresh.fragmentation_ratio, 1.0, "unknown fragmentation reads as 1.0");

    let _r = reserve(&g, MemCategory::DenoHeap, 100);
    g.observe_rss(300 * MIB);
    let s = g.stats();
    assert_eq!(s.rss_bytes, 300 * MIB);
    assert!(
        s.fragmentation_ratio.is_finite() && s.fragmentation_ratio > 0.0,
        "{}",
        s.fragmentation_ratio
    );
    // more RSS for the same committed bytes means worse fragmentation
    g.observe_rss(600 * MIB);
    assert!(g.stats().fragmentation_ratio > s.fragmentation_ratio);
}

// ---------------------------------------------------------------------------------------------
// MEM-08: budget detection
// ---------------------------------------------------------------------------------------------

#[test]
fn a_configured_budget_too_large_for_u64_saturates() {
    let dir = tempfile::tempdir().unwrap();
    let b = detect_budget_in(dir.path(), u64::MAX / 1024, 64 * 1024 * MIB);
    assert_eq!((b.bytes, b.source), (u64::MAX, BudgetSource::Config));
}

#[test]
fn mem_08_cgroup_memory_max_of_2g_gives_a_1740_mb_budget() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("memory.max"), "2147483648\n").unwrap();
    let b = detect_budget_in(dir.path(), 0, 64 * 1024 * MIB);
    assert_eq!(b.source, BudgetSource::CgroupV2);
    let mb = b.bytes as f64 / MIB as f64;
    assert!((mb - 1740.0).abs() <= 1.0, "budget {mb} MB must be 1,740 MB (+/- 1 MB)");
    // exactly 85 % of the limit (to the byte)
    assert!(b.bytes.abs_diff(2_147_483_648u64 * 85 / 100) <= 1);
}

#[test]
fn budget_is_85_percent_of_the_limit_for_any_limit_without_overflow() {
    assert_eq!(BUDGET_FRACTION_PERCENT, 85);
    for limit in [
        0u64,
        1,
        99,
        100,
        101,
        MIB,
        1 << 30,
        2 << 30,
        17 * (1 << 30) + 12_345,
        u64::MAX / 2,
        (1u64 << 60) - 1,
        u64::MAX,
    ] {
        let want = (u128::from(limit) * 85 / 100) as u64;
        let got = budget_from_limit(limit);
        assert!(got.abs_diff(want) <= 1, "limit {limit}: {got} vs {want}");
        assert!(got <= limit);
    }
    assert_eq!(budget_from_limit(100), 85);
    assert_eq!(budget_from_limit(2_000_000_000), 1_700_000_000);
}

#[test]
fn cgroup_limit_files_parse_like_the_kernel_writes_them() {
    assert_eq!(parse_cgroup_limit("2147483648\n"), Some(2_147_483_648));
    assert_eq!(parse_cgroup_limit("  1000000  "), Some(1_000_000));
    assert_eq!(parse_cgroup_limit("0"), Some(0));
    assert_eq!(parse_cgroup_limit("max\n"), None, "v2 `max` = unlimited");
    assert_eq!(parse_cgroup_limit("max"), None);
    assert_eq!(parse_cgroup_limit(""), None);
    assert_eq!(parse_cgroup_limit("\n"), None);
    assert_eq!(parse_cgroup_limit("not-a-number"), None);
    assert_eq!(parse_cgroup_limit("-1"), None);
    assert_eq!(parse_cgroup_limit("12.5"), None);
    assert_eq!(
        parse_cgroup_limit("9223372036854771712"),
        None,
        "v1 reports unlimited as a huge page-aligned number"
    );
    assert_eq!(parse_cgroup_limit("18446744073709551615"), None);
    assert_eq!(
        parse_cgroup_limit("99999999999999999999999"),
        None,
        "overflow is not a limit"
    );
}

#[test]
fn budget_source_precedence_config_then_cgroup_v2_then_v1_then_physical_ram() {
    let ram = 8 * 1024 * MIB;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    // nothing in the cgroup root -> physical RAM
    let b = detect_budget_in(root, 0, ram);
    assert_eq!(
        b,
        Budget {
            bytes: budget_from_limit(ram),
            source: BudgetSource::PhysicalRam
        }
    );

    // v1 only
    std::fs::create_dir_all(root.join("memory")).unwrap();
    std::fs::write(root.join("memory/memory.limit_in_bytes"), "1073741824\n").unwrap();
    let b = detect_budget_in(root, 0, ram);
    assert_eq!(
        b,
        Budget {
            bytes: budget_from_limit(1 << 30),
            source: BudgetSource::CgroupV1
        }
    );

    // v2 wins over v1
    std::fs::write(root.join("memory.max"), "2147483648\n").unwrap();
    let b = detect_budget_in(root, 0, ram);
    assert_eq!(
        b,
        Budget {
            bytes: budget_from_limit(2 << 30),
            source: BudgetSource::CgroupV2
        }
    );

    // `max` in v2 falls through to v1
    std::fs::write(root.join("memory.max"), "max\n").unwrap();
    assert_eq!(detect_budget_in(root, 0, ram).source, BudgetSource::CgroupV1);

    // unlimited v1 too -> physical RAM
    std::fs::write(root.join("memory/memory.limit_in_bytes"), "9223372036854771712\n").unwrap();
    assert_eq!(detect_budget_in(root, 0, ram).source, BudgetSource::PhysicalRam);

    // explicit memory.budget_mb beats everything and is taken literally (no 85 %)
    std::fs::write(root.join("memory.max"), "2147483648\n").unwrap();
    let b = detect_budget_in(root, 512, ram);
    assert_eq!(
        b,
        Budget {
            bytes: 512 * MIB,
            source: BudgetSource::Config
        }
    );
}

#[test]
fn detect_budget_with_an_explicit_value_ignores_the_host() {
    let b = detect_budget(300);
    assert_eq!(
        b,
        Budget {
            bytes: 300 * MIB,
            source: BudgetSource::Config
        }
    );
    let auto = detect_budget(0);
    assert_ne!(auto.source, BudgetSource::Config);
    assert!(auto.bytes > 0, "auto-detection must find some memory on a real host");
}

// ---------------------------------------------------------------------------------------------
// Categories
// ---------------------------------------------------------------------------------------------

#[test]
fn memory_categories_have_dense_indexes_and_the_documented_metric_labels() {
    let docs = [
        "deno_heap",
        "wasm_linear",
        "wasm_reuse",
        "t1_cache",
        "io_buffers",
        "sql_cache",
        "logs",
        "other",
    ];
    assert_eq!(MemCategory::ALL.len(), docs.len());
    for (i, (cat, label)) in MemCategory::ALL.iter().zip(docs).enumerate() {
        assert_eq!(cat.idx(), i, "{cat}");
        assert_eq!(cat.as_str(), label);
        assert_eq!(cat.to_string(), label);
    }
}

// ---------------------------------------------------------------------------------------------
// IO buffer pools
// ---------------------------------------------------------------------------------------------

#[test]
fn size_class_selection_picks_the_smallest_class_that_fits() {
    let table = [
        (0usize, Some(SizeClass::K4)),
        (1, Some(SizeClass::K4)),
        (4096, Some(SizeClass::K4)),
        (4097, Some(SizeClass::K64)),
        (65_536, Some(SizeClass::K64)),
        (65_537, Some(SizeClass::M1)),
        (1_048_576, Some(SizeClass::M1)),
        (1_048_577, None),
        (usize::MAX, None),
    ];
    for (len, want) in table {
        assert_eq!(SizeClass::for_len(len), want, "len {len}");
    }
    assert_eq!(SizeClass::K4.bytes(), 4096);
    assert_eq!(SizeClass::K64.bytes(), 65_536);
    assert_eq!(SizeClass::M1.bytes(), 1_048_576);
}

fn pools(g: &Arc<Governor>, max_bytes: u64) -> IoBufferPools {
    let dynamic: Arc<dyn MemoryGovernor> = g.clone();
    IoBufferPools::new(dynamic, max_bytes)
}

#[test]
fn pooled_buffers_are_zeroed_large_enough_and_charged_to_io_buffers() {
    let g = gov(100);
    let p = pools(&g, 8 * MIB);
    for len in [1usize, 4096, 5000, 70_000, 1_048_576] {
        let mut b = p.get(len);
        assert!(b.len() >= len, "asked for {len}, got {}", b.len());
        assert!(!b.is_empty());
        assert!(
            b.as_mut_slice().iter().all(|&x| x == 0),
            "fresh buffer of {len} must be zeroed"
        );
        assert!(
            in_category(&g, MemCategory::IoBuffers) >= len as u64,
            "{len} bytes in use must be accounted"
        );
    }
}

#[test]
fn a_returned_buffer_is_zeroed_again_when_reused() {
    let g = gov(100);
    let p = pools(&g, 8 * MIB);
    let mut b = p.get(4096);
    b.as_mut_slice().fill(0xAB);
    drop(b);
    let mut again = p.get(4096);
    assert!(
        again.as_mut_slice().iter().all(|&x| x == 0),
        "reused buffers must not leak previous contents"
    );
}

#[test]
fn buffers_larger_than_one_mib_are_served_but_never_retained() {
    let g = gov(100);
    let p = pools(&g, 8 * MIB);
    let b = p.get(3 * 1024 * 1024);
    assert!(b.len() >= 3 * 1024 * 1024);
    assert!(in_category(&g, MemCategory::IoBuffers) >= 3 * MIB);
    drop(b);
    assert_eq!(
        in_category(&g, MemCategory::IoBuffers),
        0,
        "oversize buffers are freed, not pooled"
    );
}

#[test]
fn pool_retention_is_capped_and_trim_returns_it_to_the_governor() {
    let g = gov(100);
    let cap = 4 * MIB;
    let p = pools(&g, cap);
    let held: Vec<_> = (0..20).map(|_| p.get(1_048_576)).collect();
    assert!(in_category(&g, MemCategory::IoBuffers) >= 20 * MIB);
    drop(held);
    let retained = in_category(&g, MemCategory::IoBuffers);
    assert!(retained <= cap, "pools retained {retained} bytes, cap is {cap}");
    p.trim();
    assert_eq!(
        in_category(&g, MemCategory::IoBuffers),
        0,
        "trim (called on purge) must release everything idle"
    );
    assert_eq!(committed(&g), 0);
}

/// GOV-006: when neither a cgroup limit nor physical RAM can be read and nothing is configured,
/// the budget is 0 and the node must refuse to start rather than refuse every reservation.
#[test]
fn gov_006_an_undetectable_budget_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let b = detect_budget_in(dir.path(), 0, 0);
    assert_eq!(b.bytes, 0);
    let err = b.check().expect_err("zero budget");
    assert!(err.contains("memory.budget_mb"), "{err}");
    detect_budget_in(dir.path(), 512, 0)
        .check()
        .expect("a configured budget");
    detect_budget_in(dir.path(), 0, 8 << 30).check().expect("detected RAM");
}

/// GOV-002: exactly the budget is reservable, one byte more is not, and `u64::MAX` is refused
/// without overflow.
#[test]
fn gov_002_the_budget_is_exact_and_huge_requests_do_not_overflow() {
    let g = gov(1);
    let r = g
        .reserve(MemCategory::DenoHeap, MIB, Owner::NONE)
        .expect("exactly the budget");
    assert!(
        g.reserve(MemCategory::DenoHeap, 1, Owner::NONE).is_err(),
        "nothing left"
    );
    drop(r);
    assert!(g.reserve(MemCategory::DenoHeap, MIB + 1, Owner::NONE).is_err());
    assert!(g.reserve(MemCategory::DenoHeap, u64::MAX, Owner::NONE).is_err());
    assert_eq!(committed(&g), 0);
    g.reserve(MemCategory::DenoHeap, 1, Owner::NONE).expect("still usable");
}

/// A reservation's Debug output names its category and the bytes it holds.
#[test]
fn a_reservation_debug_output_shows_its_category_and_bytes() {
    let r = Reservation::detached(MemCategory::DenoHeap, 4096);
    let s = format!("{r:?}");
    assert!(s.contains("DenoHeap") && s.contains("4096"), "{s}");
}

/// Growing a reservation by zero bytes succeeds without charging the budget or changing the
/// reservation.
#[test]
fn growing_a_reservation_by_zero_bytes_succeeds_and_charges_nothing() {
    let g = gov(1);
    let mut r = reserve(&g, MemCategory::DenoHeap, 1);
    assert!(r.try_grow(0));
    assert_eq!(r.bytes(), MIB);
    assert_eq!(committed(&g), MIB);
}

/// MEM-02 under contention: many threads reserving against one small budget never jointly
/// overshoot it, and the total is back to zero once every grant is dropped.
#[test]
fn concurrent_reserves_against_a_tight_budget_never_overshoot_it() {
    const THREADS: usize = 16;
    const ROUNDS: usize = 20_000;
    const CHUNK: u64 = MIB / 4;
    let g = gov(4);
    let start = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let g = g.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                let mut granted = 0u64;
                for _ in 0..ROUNDS {
                    if let Ok(r) = g.reserve(MemCategory::DenoHeap, CHUNK, Owner::NONE) {
                        assert!(committed(&g) <= 4 * MIB, "budget exceeded");
                        granted += 1;
                        drop(r);
                    }
                }
                granted
            })
        })
        .collect();
    let granted: u64 = handles.into_iter().map(|h| h.join().expect("reserving thread")).sum();
    assert!(granted > 0, "some reservations fit");
    assert_eq!(committed(&g), 0, "every grant was released");
}
