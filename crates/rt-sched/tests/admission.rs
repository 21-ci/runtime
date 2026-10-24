//! Admission and backpressure (runtime and memory waits): the ordered checks at submit, capacity waits instead of rejections
//! (decision D-A3), single-flight artifact waits, memory waits, queue timeouts, node modes.
//!
//! ACs: SCH-16, SCH-17, SCH-18, SCH-26, SCH-27, SCH-28, SCH-29, SCH-31, SCH-32, SCH-33, SCH-34, SCH-37.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use rt_mem::{MemoryGovernor, Pressure};
use rt_sched::admission::{
    Admission, AdmissionLimits, AdmissionTicket, AdmitContext, AdmitRequest, Admitted, NodeSignals, SlotClass,
    QUEUED_TASK_OVERHEAD, TENANT_QUANTUM,
};
use rt_sched::trace::TraceEvent;
use rt_sched::TaskState;
use rt_testkit::{cpu, ids, io, script, MockEvent, Script, SimRuntime, SimTask, SpecBuilder};
use rt_types::{
    BuildFailReason, BuildFailure, DegradationMode, MsgId, Outcome, Payload, PubsubMessage, RejectReason, ScalingKnobs,
    ServiceKnobs, StreamBatch, StreamRecord, TaskId, TaskKind, TaskSpec, Trigger,
};

// ---------------------------------------------------------------------------------------------
// The Admission component on its own
// ---------------------------------------------------------------------------------------------

const NORMAL: AdmitContext = AdmitContext {
    mode: DegradationMode::Normal,
    t1_ready: true,
};

/// Service knobs with a fixed limit (`start = min = max = max_concurrency`).
fn knobs(max_concurrency: u32, max_queued: u32, tenant_max: u32) -> ServiceKnobs {
    let mut k = ServiceKnobs {
        max_instances: max_concurrency,
        max_queued,
        tenant_max_concurrency: tenant_max,
        ..ServiceKnobs::default()
    };
    k.scaling.start = max_concurrency;
    k.scaling.min = max_concurrency;
    k
}

fn task(tenant: u128, service: u128, k: ServiceKnobs) -> TaskSpec {
    SpecBuilder::deno().tenant(tenant).service(service).knobs(k).finish()
}

fn req(spec: &TaskSpec) -> AdmitRequest {
    AdmitRequest::from_spec(spec, None, MIB, 0)
}

fn limits(max_inflight: u32) -> AdmissionLimits {
    AdmissionLimits::resolve(max_inflight, 0, 0, 1 << 30)
}

/// Admission of numbered items: `admit` hands out 1, 2, 3, ... and keeps each item's ticket,
/// like the scheduler keeps it in the task.
struct Adm {
    a: Admission<u32>,
    next: u32,
    tickets: BTreeMap<u32, AdmissionTicket>,
}

impl Adm {
    fn new(l: AdmissionLimits) -> Self {
        Self {
            a: Admission::new(l),
            next: 0,
            tickets: BTreeMap::new(),
        }
    }

    fn admit_cx(&mut self, spec: &TaskSpec, cx: AdmitContext) -> Result<Admitted<u32>, RejectReason> {
        self.admit_req(&req(spec), cx)
    }

    /// Admit at scheduler time `now_ns`.
    fn admit_at(&mut self, spec: &TaskSpec, now_ns: u64) -> Result<Admitted<u32>, RejectReason> {
        self.admit_req(&req_at(spec, now_ns), NORMAL)
    }

    fn admit_req(&mut self, r: &AdmitRequest, cx: AdmitContext) -> Result<Admitted<u32>, RejectReason> {
        self.next += 1;
        let n = self.next;
        let mut ticket = None;
        let r = self.a.admit(r, cx, |t| {
            ticket = Some(t.clone());
            n
        });
        if let Some(t) = ticket {
            self.tickets.insert(n, t);
        }
        r.map_err(|refused| {
            assert!(refused.granted.is_empty(), "single-threaded: nothing to hand on");
            refused.reason
        })
    }

    fn admit(&mut self, spec: &TaskSpec) -> Result<Admitted<u32>, RejectReason> {
        self.admit_cx(spec, NORMAL)
    }

    /// Item `n` (running: admitted to run or granted) finishes; returns the items granted.
    fn finish(&mut self, n: u32) -> Vec<u32> {
        let mut t = self.tickets.remove(&n).expect("a ticket for every admitted item");
        t.queued = false;
        self.a.release(&t, 1, None)
    }

    /// Item `n` finishes at `now_ns` after holding its slot for `sample`.
    fn finish_with(&mut self, n: u32, now_ns: u64, sample: Option<Duration>) -> Vec<u32> {
        let mut t = self.tickets.remove(&n).expect("a ticket for every admitted item");
        t.queued = false;
        self.a.release_with(&t, now_ns, sample, sample)
    }

    /// Run one task of `spec` on the fast path and feed its estimator `sample`.
    fn learn(&mut self, spec: &TaskSpec, sample: Duration) {
        let a = self.admit(spec).unwrap();
        assert!(a.run.is_some(), "learning needs a free slot");
        assert!(self.finish_with(self.next, 1, Some(sample)).is_empty());
    }
}

fn req_at(spec: &TaskSpec, now_ns: u64) -> AdmitRequest {
    AdmitRequest::from_spec(spec, None, MIB, now_ns)
}

/// The item may run now: admitted straight to a slot, or queued and granted at once (it found
/// others waiting for other limits, then a slot was free for it).
fn runs(a: &Admitted<u32>) -> bool {
    (a.run.is_some() && !a.ticket.queued) || (a.run.is_none() && a.granted.len() == 1)
}

fn waits(a: &Admitted<u32>) -> bool {
    a.run.is_none() && a.ticket.queued && a.granted.is_empty()
}

#[test]
fn admission_counts_inflight_and_release_returns_the_slot() {
    let mut a = Adm::new(limits(100));
    assert_eq!(a.a.inflight(), 0);
    let t1 = a.admit(&task(1, 1, knobs(BIG, 0, 0))).expect("admitted");
    let t2 = a.admit(&task(1, 1, knobs(BIG, 0, 0))).expect("admitted");
    assert_eq!(a.a.inflight(), 2);
    assert!(runs(&t1) && runs(&t2));
    assert!(a.a.release(&t1.ticket, 1, None).is_empty());
    assert_eq!(a.a.inflight(), 1);
    a.a.release(&t2.ticket, 1, None);
    assert_eq!((a.a.inflight(), a.a.running(), a.a.queued()), (0, 0, (0, 0)));
}

/// D-B2: `min_instances` raises the initial limit and its floor; `max_instances` and the tenant cap
/// bound both; a floor above the cap is cut to it.
#[test]
fn instance_bounds_set_the_initial_limit_and_its_floor() {
    let bounds = |min_instances: u32, max_instances: u32, tenant_cap: u32| {
        let mut a = Adm::new(limits(100));
        let mut k = ServiceKnobs {
            min_instances,
            max_instances,
            tenant_max_concurrency: tenant_cap,
            ..ServiceKnobs::default()
        };
        k.scaling.start = 2;
        k.scaling.min = 1;
        let t = a.admit(&task(1, 1, k)).expect("admitted");
        assert!(runs(&t));
        let s = a.a.service(rt_testkit::ids::service(1)).expect("state");
        (s.limit, s.min, s.max)
    };
    assert_eq!(bounds(0, 8, 0), (2, 1, 8), "no floor: the profile's start and floor");
    assert_eq!(bounds(4, 8, 0), (4, 4, 8), "min_instances raise both");
    assert_eq!(bounds(4, 8, 3), (3, 3, 3), "the tenant cap bounds the floor too");
    assert_eq!(bounds(9, 8, 0), (8, 8, 8), "a floor above the ceiling is cut to it");
    assert_eq!(bounds(0, 1, 0), (1, 1, 1), "one instance at most");
}

#[test]
fn admission_check_1_drain_rejects_everything_even_artifacts_in_t1() {
    let mut a = Adm::new(limits(100));
    for t1_ready in [true, false] {
        let cx = AdmitContext {
            mode: DegradationMode::Drain,
            t1_ready,
        };
        assert_eq!(
            a.admit_cx(&task(1, 1, knobs(BIG, 0, 0)), cx).err(),
            Some(RejectReason::Draining)
        );
    }
    assert_eq!(a.a.inflight(), 0, "a rejected task is never counted");
}

#[test]
fn admission_check_2_shed_rejects_tasks_whose_artifact_is_not_in_t1() {
    let mut a = Adm::new(limits(100));
    let miss = AdmitContext {
        mode: DegradationMode::Shed,
        t1_ready: false,
    };
    let hit = AdmitContext {
        mode: DegradationMode::Shed,
        t1_ready: true,
    };
    assert_eq!(
        a.admit_cx(&task(1, 1, knobs(BIG, 0, 0)), miss).err(),
        Some(RejectReason::Shed)
    );
    assert!(a.admit_cx(&task(1, 1, knobs(BIG, 0, 0)), hit).is_ok());
    let normal_miss = AdmitContext {
        mode: DegradationMode::Normal,
        t1_ready: false,
    };
    assert!(
        a.admit_cx(&task(1, 1, knobs(BIG, 0, 0)), normal_miss).is_ok(),
        "in normal mode a miss waits for its artifact"
    );
}

/// D-A3: the node cap queues; a freed slot goes to the waiter.
#[test]
fn admission_check_3_global_max_inflight_queues_instead_of_rejecting() {
    // 3 run slots: 2 for requests, 1 kept for calls and builds (ADM-003).
    let mut a = Adm::new(limits(3));
    let t1 = a.admit(&task(1, 1, knobs(BIG, 0, 0))).expect("1st");
    a.admit(&task(2, 2, knobs(BIG, 0, 0))).expect("2nd");
    let t3 = a
        .admit(&task(3, 3, knobs(BIG, 0, 0)))
        .expect("the 3rd waits, it is not rejected");
    assert!(waits(&t3));
    assert_eq!((a.a.running(), a.a.queued().0, a.a.inflight()), (2, 1, 3));
    assert_eq!(
        a.a.release(&t1.ticket, 1, None),
        vec![3],
        "the freed slot goes to the waiting task"
    );
    assert_eq!((a.a.running(), a.a.queued().0), (2, 0));
}

#[test]
fn admission_set_limits_applies_live_and_starts_what_now_fits() {
    let mut a = Adm::new(limits(1));
    a.admit(&task(1, 1, knobs(BIG, 0, 0))).expect("1st");
    assert!(waits(&a.admit(&task(1, 2, knobs(BIG, 0, 0))).expect("waits")));
    // 3 slots: 2 for requests (1 kept for calls, ADM-003), so the waiter starts.
    assert_eq!(a.a.set_limits(limits(3), 5), vec![2]);
    assert_eq!(a.a.limits().max_inflight, 3);
}

/// D-A3: the plan's tenant cap queues; other tenants are unaffected.
#[test]
fn admission_check_3_tenant_cap_queues_and_counts_across_services() {
    let mut a = Adm::new(limits(100));
    let k = knobs(BIG, 0, 2);
    let t1 = a.admit(&task(1, 1, k)).expect("tenant 1, service 1");
    a.admit(&task(1, 2, k)).expect("tenant 1, service 2");
    let t3 = a.admit(&task(1, 3, k)).expect("tenant 1 at its cap: waits");
    assert!(waits(&t3));
    assert!(
        runs(&a.admit(&task(2, 4, k)).expect("another tenant")),
        "another tenant is unaffected"
    );
    for s in 10..20 {
        assert!(runs(&a.admit(&task(9, s, knobs(BIG, 0, 0))).expect("0 = unlimited")));
    }
    assert_eq!(a.a.release(&t1.ticket, 1, None), vec![3]);
}

/// D-A3: a service at its limit queues FIFO; each freed slot starts exactly one waiter.
#[test]
fn admission_check_3_service_limit_queues_fifo() {
    let mut a = Adm::new(limits(100));
    let k = knobs(2, 0, 0);
    let t1 = a.admit(&task(1, 1, k)).expect("running slot 1");
    let t2 = a.admit(&task(1, 1, k)).expect("running slot 2");
    assert!(runs(&t1) && runs(&t2));
    for _ in 0..3 {
        assert!(waits(&a.admit(&task(1, 1, k)).expect("waits")));
    }
    assert_eq!(a.a.inflight(), 5, "waiting tasks count as admitted");
    assert!(
        runs(&a.admit(&task(1, 2, k)).expect("own limits")),
        "another service of the tenant has its own limit"
    );
    let s1 = ids::service(1);
    assert_eq!(a.a.service(s1).map(|s| (s.running, s.queued, s.limit)), Some((2, 3, 2)));

    assert_eq!(a.a.release(&t1.ticket, 1, None), vec![3], "FIFO: the first waiter");
    assert_eq!(a.a.release(&t2.ticket, 1, None), vec![4]);
    assert_eq!(a.a.service(s1).map(|s| (s.running, s.queued)), Some((2, 1)));
}

/// Room to wait: the node bound, half of it per service, an explicit `scaling.max_queued`, and
/// the bytes bound; over any: `503 overloaded`.
#[test]
fn admission_check_4_queue_bounds_answer_503_overloaded() {
    let mut a = Adm::new(AdmissionLimits {
        max_inflight: 1,
        max_queued: 4,
        max_queued_bytes: 1 << 30,
    });
    a.admit(&task(1, 1, knobs(BIG, 0, 0))).expect("runs");
    assert!(waits(&a.admit(&task(1, 1, knobs(BIG, 0, 0))).unwrap()));
    assert!(waits(&a.admit(&task(1, 1, knobs(BIG, 0, 0))).unwrap()));
    let e = a
        .admit(&task(1, 1, knobs(BIG, 0, 0)))
        .expect_err("one service may hold half the node queue");
    assert_eq!(
        (e, e.http_status(), e.retry_after()),
        (RejectReason::Overloaded, 503, Some(Duration::from_secs(1)))
    );
    assert!(
        waits(&a.admit(&task(2, 2, knobs(BIG, 0, 0))).unwrap()),
        "another service still has room"
    );
    assert!(waits(&a.admit(&task(3, 3, knobs(BIG, 0, 0))).unwrap()));
    assert_eq!(
        a.admit(&task(4, 4, knobs(BIG, 0, 0))).err(),
        Some(RejectReason::Overloaded),
        "the node queue is full"
    );

    let mut a = Adm::new(limits(1));
    a.admit(&task(1, 1, knobs(BIG, 1, 0))).expect("runs");
    assert!(waits(&a.admit(&task(1, 1, knobs(BIG, 1, 0))).unwrap()));
    assert_eq!(
        a.admit(&task(1, 1, knobs(BIG, 1, 0))).err(),
        Some(RejectReason::Overloaded),
        "explicit max_queued = 1"
    );

    let mut a = Adm::new(AdmissionLimits {
        max_inflight: 1,
        max_queued: 1000,
        max_queued_bytes: 20 * 1024,
    });
    a.admit(&task(1, 1, knobs(BIG, 0, 0))).expect("runs");
    assert!(waits(&a.admit(&task(1, 1, knobs(BIG, 0, 0))).unwrap()), "4 KiB charged");
    assert!(
        waits(&a.admit(&task(1, 1, knobs(BIG, 0, 0))).unwrap()),
        "8 KiB: half of the bytes bound is 10 KiB"
    );
    assert_eq!(
        a.admit(&task(1, 1, knobs(BIG, 0, 0))).err(),
        Some(RejectReason::Overloaded),
        "12 KiB is over the service share"
    );
    assert_eq!(a.a.queued(), (2, 8 * 1024));
}

#[test]
fn a_cancelled_waiter_is_never_granted_and_its_room_is_returned() {
    let mut a = Adm::new(limits(1));
    let t1 = a.admit(&task(1, 1, knobs(BIG, 0, 0))).unwrap();
    let w2 = a.admit(&task(1, 1, knobs(BIG, 0, 0))).unwrap();
    let _w3 = a.admit(&task(1, 1, knobs(BIG, 0, 0))).unwrap();
    assert!(a.a.cancel(&w2.ticket, true, |n| *n == 2));
    assert!(!a.a.cancel(&w2.ticket, true, |n| *n == 2), "only once");
    assert_eq!(a.a.queued().0, 1);
    assert_eq!(a.a.release(&t1.ticket, 1, None), vec![3]);
    assert_eq!(a.a.queued(), (0, 0));
}

/// When slots free, tenants take turns; the services of one tenant are served by deadline key,
/// which for equal predictions is arrival order (rule 3.6.5, changed on purpose from per-service
/// round robin).
#[test]
fn freed_slots_alternate_tenants_and_serve_a_tenants_services_by_deadline() {
    let mut a = Adm::new(wide(1));
    assert!(runs(&a.admit(&task(9, 9, knobs(BIG, 0, 0))).unwrap()));
    let mut owner: BTreeMap<u32, (u128, u128)> = BTreeMap::new();
    // tenant 1 floods two services, tenant 2 one service; all wait for the single slot
    for (i, (tenant, service)) in [(1, 11), (1, 11), (1, 11), (1, 12), (1, 12), (2, 21), (2, 21), (2, 21)]
        .into_iter()
        .enumerate()
    {
        assert!(waits(
            &a.admit_at(&task(tenant, service, knobs(BIG, 0, 0)), i as u64).unwrap()
        ));
        owner.insert(a.next, (tenant, service));
    }
    let mut running = 1;
    let mut order = Vec::new();
    for _ in 0..8 {
        let granted = a.finish(running);
        assert_eq!(granted.len(), 1, "one slot, one grant");
        running = granted[0];
        order.push(owner[&running]);
    }
    assert!(a.finish(running).is_empty());
    let tenants: Vec<u128> = order.iter().map(|(t, _)| *t).collect();
    assert_eq!(tenants[..6], [1, 2, 1, 2, 1, 2], "tenants alternate: {order:?}");
    let t1_services: Vec<u128> = order.iter().filter(|(t, _)| *t == 1).map(|(_, s)| *s).collect();
    assert_eq!(
        t1_services,
        [11, 11, 11, 12, 12],
        "a tenant's services in arrival order: {order:?}"
    );
    assert_eq!((a.a.running(), a.a.queued()), (0, (0, 0)));
}

const fn nms(n: u64) -> u64 {
    n * 1_000_000
}

fn wide(max_inflight: u32) -> AdmissionLimits {
    AdmissionLimits {
        max_inflight,
        max_queued: 100,
        max_queued_bytes: 1 << 30,
    }
}

/// Runs a blocker of tenant 9 on the only slot; returns its item.
fn block(a: &mut Adm) -> u32 {
    assert!(runs(&a.admit(&task(9, 9, knobs(BIG, 0, 0))).unwrap()));
    a.next
}

/// Admit at `now_ns`, assert it waits; returns the item.
fn wait_at(a: &mut Adm, spec: &TaskSpec, now_ns: u64) -> u32 {
    assert!(waits(&a.admit_at(spec, now_ns).unwrap()));
    a.next
}

/// Fair-002: with one slot, a tenant's service predicted at 5 ms goes before its service predicted
/// at 2 s that arrived earlier.
#[test]
fn fair_002_a_tenants_short_service_goes_before_its_long_one() {
    let mut a = Adm::new(wide(1));
    let short = task(1, 11, knobs(BIG, 0, 0));
    let long = task(1, 12, knobs(BIG, 0, 0));
    a.learn(&long, Duration::from_secs(2));
    a.learn(&short, Duration::from_millis(5));
    let b = block(&mut a);
    let l = wait_at(&mut a, &long, nms(0));
    let s = wait_at(&mut a, &short, nms(1));
    assert_eq!(a.finish(b), vec![s], "the short job first");
    assert_eq!(a.finish(s), vec![l]);
    assert!(a.finish(l).is_empty());
}

/// Fair-003: a long job (p = 20 s, k = 4, Q = 5 s) gets `Q/2` of delay credit: a short arrival at
/// +2.4 s goes before it, one at +2.6 s after it.
#[test]
fn fair_003_a_long_job_is_overtaken_only_by_arrivals_within_half_the_queue_timeout() {
    let mut a = Adm::new(wide(1));
    let short = task(1, 11, knobs(BIG, 0, 0));
    let long = task(1, 12, knobs(BIG, 0, 0));
    assert_eq!(short.knobs.queue_timeout, Duration::from_secs(5));
    a.learn(&long, Duration::from_secs(20));
    a.learn(&short, Duration::from_millis(5));
    let b = block(&mut a);
    let l = wait_at(&mut a, &long, 0);
    let s1 = wait_at(&mut a, &short, nms(2400));
    let s2 = wait_at(&mut a, &short, nms(2600));
    assert_eq!(a.tickets[&l].wait.unwrap().0, nms(2500), "key = arrival + Q/2");
    assert_eq!(a.finish_with(b, nms(2410), None), vec![s1]);
    assert_eq!(a.a.aged_grants(), 0, "the short one is granted before its key");
    assert_eq!(a.finish_with(s1, nms(3000), None), vec![l]);
    assert_eq!(a.a.aged_grants(), 1, "the long one waited past its key");
    assert_eq!(a.finish_with(l, nms(3000), None), vec![s2]);
    assert!(a.finish(s2).is_empty());
}

/// Fair-004: `aging_factor = 0` serves a tenant in arrival order whatever the predictions.
#[test]
fn fair_004_aging_zero_serves_a_tenant_in_arrival_order() {
    let mut a = Adm::new(wide(1));
    a.a = Admission::new(wide(1)).with_aging(0);
    let short = task(1, 11, knobs(BIG, 0, 0));
    let long = task(1, 12, knobs(BIG, 0, 0));
    a.learn(&long, Duration::from_secs(2));
    a.learn(&short, Duration::from_millis(5));
    let b = block(&mut a);
    let l = wait_at(&mut a, &long, nms(0));
    let s = wait_at(&mut a, &short, nms(1));
    assert_eq!(a.finish(b), vec![l], "arrival order");
    assert_eq!(a.finish(l), vec![s]);
}

/// Fair-005: two always-backlogged tenants, one with 5 ms tasks and one with 500 ms tasks, get
/// equal slot time (within `TENANT_QUANTUM + p_max`), not equal task counts.
#[test]
fn fair_005_tenants_share_slot_time_not_task_count() {
    let mut a = Adm::new(wide(1));
    let specs = [task(1, 11, knobs(BIG, 0, 0)), task(2, 21, knobs(BIG, 0, 0))];
    let cost = [Duration::from_millis(5), Duration::from_millis(500)];
    a.learn(&specs[0], cost[0]);
    a.learn(&specs[1], cost[1]);
    let mut running = block(&mut a);
    let mut owner: BTreeMap<u32, usize> = BTreeMap::new();
    let mut now = nms(10);
    for _ in 0..2 {
        for (t, spec) in specs.iter().enumerate() {
            owner.insert(wait_at(&mut a, spec, now), t);
        }
    }
    let mut sample = cost[0];
    let mut slot = [Duration::ZERO; 2];
    let mut count = [0u32; 2];
    for _ in 0..1000 {
        let g = a.finish_with(running, now, Some(sample));
        assert_eq!(g.len(), 1);
        running = g[0];
        let t = owner[&running];
        sample = cost[t];
        slot[t] += sample;
        count[t] += 1;
        now += dns(sample);
        owner.insert(wait_at(&mut a, &specs[t], now), t);
        let diff = slot[0].abs_diff(slot[1]);
        assert!(
            diff <= TENANT_QUANTUM + cost[1],
            "slot time {slot:?} after {count:?} grants"
        );
    }
    assert!(
        count[0] > 50 * count[1],
        "the short tenant runs many more tasks: {count:?}"
    );
}

fn dns(d: Duration) -> u64 {
    d.as_nanos() as u64
}

/// Fair-006: a tenant that was idle while another ran comes back at most `TENANT_QUANTUM` behind:
/// it gets at most `quantum / p + 1` grants before the busy tenant's next one.
#[test]
fn fair_006_an_idle_tenant_comes_back_at_most_one_quantum_behind() {
    let mut a = Adm::new(wide(1));
    let p = Duration::from_millis(5);
    let (fa, fb) = (task(1, 11, knobs(BIG, 0, 0)), task(2, 21, knobs(BIG, 0, 0)));
    a.learn(&fa, p);
    a.learn(&fb, p);
    let mut running = block(&mut a);
    let mut owner: BTreeMap<u32, u128> = BTreeMap::new();
    let mut now = nms(1);
    for _ in 0..3 {
        owner.insert(wait_at(&mut a, &fa, now), 1);
    }
    for _ in 0..100 {
        let g = a.finish_with(running, now, Some(p));
        running = g[0];
        assert_eq!(owner[&running], 1, "only tenant 1 waits");
        now += dns(p);
        owner.insert(wait_at(&mut a, &fa, now), 1);
    }
    for _ in 0..20 {
        owner.insert(wait_at(&mut a, &fb, now), 2);
    }
    let mut run_b = 0u64;
    loop {
        let g = a.finish_with(running, now, Some(p));
        running = g[0];
        now += dns(p);
        if owner[&running] == 1 {
            break;
        }
        run_b += 1;
    }
    let bound = dns(TENANT_QUANTUM) / dns(p) + 1;
    assert!(
        (1..=bound).contains(&run_b),
        "{run_b} grants of the returning tenant, bound {bound}"
    );
}

/// Fair-007: a tenant's waiting calls go before its waiting requests, FIFO among themselves.
#[test]
fn fair_007_calls_go_before_items_of_their_tenant_in_fifo_order() {
    let mut a = Adm::new(wide(1));
    assert!(runs(&a.admit(&task(1, 10, knobs(BIG, 0, 0))).unwrap()));
    let b = a.next;
    let item = wait_at(&mut a, &task(1, 11, knobs(BIG, 0, 0)), nms(1));
    let c1 = wait_at(&mut a, &call(1, 12, knobs(BIG, 0, 0)), nms(2));
    let c2 = wait_at(&mut a, &call(1, 13, knobs(BIG, 0, 0)), nms(3));
    assert_eq!(a.tickets[&c1].class, SlotClass::Call);
    assert_eq!(a.finish(b), vec![c1]);
    assert_eq!(a.finish(c1), vec![c2]);
    assert_eq!(a.finish(c2), vec![item]);
}

/// Fair-008: a task predicted at 5 ms that held its slot for 500 ms is charged 500 ms: the other
/// tenant then gets about 100 grants of 5 ms before the mispredicted tenant's next one.
#[test]
fn fair_008_a_mispredicted_task_is_charged_its_actual_time() {
    let mut a = Adm::new(wide(1));
    let p = Duration::from_millis(5);
    let (fa, fb) = (task(1, 11, knobs(BIG, 0, 0)), task(2, 21, knobs(BIG, 0, 0)));
    a.learn(&fa, p);
    a.learn(&fb, p);
    let b = block(&mut a);
    let mut owner: BTreeMap<u32, u128> = BTreeMap::new();
    for _ in 0..3 {
        owner.insert(wait_at(&mut a, &fa, nms(1)), 1);
        owner.insert(wait_at(&mut a, &fb, nms(1)), 2);
    }
    let g = a.finish_with(b, nms(2), Some(p));
    assert_eq!(owner[&g[0]], 1, "tenant 1 listed first");
    let mut running = a.finish_with(g[0], nms(502), Some(Duration::from_millis(500)))[0];
    let mut now = nms(502);
    let mut run_b = 0;
    while owner[&running] == 2 {
        run_b += 1;
        owner.insert(wait_at(&mut a, &fb, now), 2);
        now += dns(p);
        running = a.finish_with(running, now, Some(p))[0];
    }
    assert!((95..=101).contains(&run_b), "{run_b} grants of the other tenant");
}

/// Fair-009: tenants with equal work alternate one grant each.
#[test]
fn fair_009_equal_tenants_alternate() {
    let mut a = Adm::new(wide(1));
    let mut running = block(&mut a);
    let mut owner: BTreeMap<u32, u128> = BTreeMap::new();
    for i in 0..4 {
        owner.insert(wait_at(&mut a, &task(1, 11, knobs(BIG, 0, 0)), i), 1);
        owner.insert(wait_at(&mut a, &task(2, 21, knobs(BIG, 0, 0)), i), 2);
    }
    let mut order = Vec::new();
    for _ in 0..8 {
        running = a.finish(running)[0];
        order.push(owner[&running]);
    }
    assert_eq!(order, [1, 2, 1, 2, 1, 2, 1, 2]);
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]
    /// Fair-010: within one tenant, items are granted in deadline-key order: no item is
    /// overtaken by one with a later key, and each key is `wait_key(arrival, pred, 4, Q)`.
    #[test]
    fn fair_010_prop_no_item_is_overtaken_by_a_later_deadline(
        preds in proptest::collection::vec(1u64..3_000, 3),
        arrivals in proptest::collection::vec((0usize..3, 0u64..2_000), 1..40),
    ) {
        let mut a = Adm::new(wide(1));
        let specs: Vec<TaskSpec> = (0..3).map(|i| task(1, 11 + i as u128, knobs(BIG, 0, 0))).collect();
        for (s, p) in specs.iter().zip(&preds) {
            a.learn(s, Duration::from_millis(*p));
        }
        let mut running = block(&mut a);
        let mut now = 0;
        let mut keys = BTreeMap::new();
        for (svc, gap) in &arrivals {
            now += nms(*gap);
            let n = wait_at(&mut a, &specs[*svc], now);
            let want = rt_sched::admission::wait_key(now, nms(preds[*svc]), 4, Duration::from_secs(5));
            let t = &a.tickets[&n];
            proptest::prop_assert_eq!(t.wait.unwrap().0, want);
            keys.insert(n, t.wait.unwrap());
        }
        let mut last = (0, 0);
        for _ in 0..arrivals.len() {
            running = a.finish(running)[0];
            proptest::prop_assert!(keys[&running] >= last, "granted out of key order");
            last = keys[&running];
        }
        proptest::prop_assert!(a.finish(running).is_empty());
    }
}

fn scaling(a: &Adm, service: u128) -> rt_sched::ServiceScaling {
    a.a.service_scaling(ids::service(service), 0).expect("state")
}

/// Pred-001: the estimate is the first sample, then moves 1/8 towards each new one; a new
/// version of the service starts without an estimate.
#[test]
fn pred_001_the_estimate_follows_slot_hold_time_and_resets_on_a_new_version() {
    let mut a = Adm::new(wide(4));
    let s = task(1, 11, knobs(BIG, 0, 0));
    a.learn(&s, Duration::from_millis(10));
    assert_eq!(scaling(&a, 11).pred, [Some(Duration::from_millis(10)), None]);
    a.learn(&s, Duration::from_millis(18));
    assert_eq!(scaling(&a, 11).pred[0], Some(Duration::from_millis(11)));
    let mut r = s.service;
    r.version = rt_types::VersionNo(r.version.0 + 1);
    let v2 = SpecBuilder::deno().service_ref(r).knobs(knobs(BIG, 0, 0)).finish();
    let x = a.admit(&v2).unwrap();
    assert!(x.run.is_some());
    assert_eq!(scaling(&a, 11).pred, [None, None], "a new version resets");
    // Builds and samples-less releases never move it.
    a.finish_with(a.next, 1, None);
    let b = a.admit(&build_of(1, 11)).unwrap();
    assert!(b.run.is_some());
    a.finish_with(a.next, 1, Some(Duration::from_secs(1)));
    assert_eq!(scaling(&a, 11).pred, [None, None]);
}

/// Pred-002: a wall kill feeds the wall limit as the sample; a queued task's prediction is never
/// above its wall limit.
#[test]
fn pred_002_a_wall_kill_feeds_the_wall_limit() {
    let mut a = Adm::new(wide(1));
    let s = SpecBuilder::deno()
        .tenant(1)
        .service(11)
        .knobs(knobs(BIG, 0, 0))
        .wall_limit(100)
        .finish();
    a.learn(&s, Duration::from_millis(100));
    assert_eq!(scaling(&a, 11).pred[0], Some(Duration::from_millis(100)));
    a.learn(&s, Duration::from_secs(10));
    assert!(scaling(&a, 11).pred[0].unwrap() > Duration::from_secs(1));
    let _b = block(&mut a);
    let n = wait_at(&mut a, &s, 0);
    assert_eq!(a.tickets[&n].pred_ns, nms(100), "bounded by the wall limit");
}

/// Cancel-001: a timed-out waiter leaves the ordered queue (head or not, by its key or found by
/// search), and the next head moves up.
#[test]
fn cancel_001_a_timed_out_waiter_leaves_the_ordered_queue_and_the_next_head_moves_up() {
    let mut a = Adm::new(wide(1));
    let b = block(&mut a);
    let s = task(1, 11, knobs(BIG, 0, 0));
    let x1 = wait_at(&mut a, &s, 0);
    let x2 = wait_at(&mut a, &s, 1);
    let x3 = wait_at(&mut a, &s, 2);
    let x4 = wait_at(&mut a, &s, 3);
    let c = wait_at(&mut a, &call(1, 12, knobs(BIG, 0, 0)), 4);
    assert_eq!(a.a.waiting(ids::service(11)), 4);
    assert!(a.a.cancel(&a.tickets[&x1].clone(), true, |n| *n == x1), "the head");
    assert!(a.a.cancel(&a.tickets[&x3].clone(), false, |n| *n == x3), "not the head");
    let mut keyless = a.tickets[&x4].clone();
    keyless.wait = None;
    assert!(a.a.cancel(&keyless, false, |n| *n == x4), "found without its key");
    assert!(!a.a.cancel(&keyless, false, |n| *n == x4), "only once");
    let mut keyless_call = a.tickets[&c].clone();
    keyless_call.wait = None;
    assert!(
        a.a.cancel(&keyless_call, false, |n| *n == c),
        "a call found without its key"
    );
    assert!(!a.a.cancel(&a.tickets[&c].clone(), false, |n| *n == c));
    assert_eq!(a.a.queued().0, 1);
    assert_eq!(a.a.waiting(ids::service(11)), 1);
    assert_eq!(a.finish(b), vec![x2]);
    assert_eq!(a.a.queued(), (0, 0));
    assert_eq!(a.a.waiting(ids::service(77)), 0, "no state");
}

/// Fair-011: interactive and background work of one service keep separate estimates.
#[test]
fn fair_011_lanes_keep_separate_estimates() {
    let mut a = Adm::new(wide(1));
    let http = task(1, 11, knobs(BIG, 0, 0));
    let queue = SpecBuilder::deno()
        .tenant(1)
        .service(11)
        .knobs(knobs(BIG, 0, 0))
        .trigger(Trigger::Queue {
            batch: Vec::new(),
            attempt: 1,
        })
        .finish();
    assert_eq!(req(&queue).lane, 1);
    a.learn(&http, Duration::from_millis(5));
    a.learn(&queue, Duration::from_secs(2));
    assert_eq!(
        scaling(&a, 11).pred,
        [Some(Duration::from_millis(5)), Some(Duration::from_secs(2))]
    );
    let _b = block(&mut a);
    let h = wait_at(&mut a, &http, 0);
    let q = wait_at(&mut a, &queue, 0);
    assert_eq!(a.tickets[&h].pred_ns, nms(5));
    assert_eq!(a.tickets[&q].pred_ns, nms(2000));
}

/// A pub/sub request (`reply`) is a call: its requester holds a slot.
#[test]
fn adm_pubsub_reply_takes_the_call_class() {
    let t = |reply| Trigger::Pubsub {
        subject: "a.b".into(),
        depth: 1,
        reply,
    };
    assert_eq!(SlotClass::of(TaskKind::DENO, &t(true)), SlotClass::Call);
    assert_eq!(SlotClass::of(TaskKind::DENO, &t(false)), SlotClass::Throttled);
}

/// Pub/sub and stream payload bytes count toward the queue byte bounds.
#[test]
fn adm_stream_and_pubsub_payload_bytes_count_toward_max_queued_mb() {
    let msg = |len: usize| {
        SpecBuilder::deno()
            .tenant(1)
            .service(11)
            .knobs(knobs(BIG, 0, 0))
            .trigger(Trigger::Pubsub {
                subject: "a".into(),
                depth: 1,
                reply: false,
            })
            .payload(Payload::Pubsub(PubsubMessage {
                id: MsgId::new(),
                subject: "a".into(),
                body: vec![0u8; len].into(),
                headers: Vec::new(),
                replyable: false,
                published_at_ms: 0,
            }))
            .finish()
    };
    let rec = |len: usize| StreamRecord {
        partition: 0,
        offset: 0,
        key: None,
        headers: Vec::new(),
        body: vec![0u8; len].into(),
        ts_ms: 0,
    };
    let batch = |lens: &[usize]| {
        SpecBuilder::deno()
            .tenant(1)
            .service(12)
            .knobs(knobs(BIG, 0, 0))
            .trigger(Trigger::Stream {
                topic: "t".into(),
                partition: 0,
                attempt: 1,
            })
            .payload(Payload::Stream(StreamBatch {
                topic: "t".into(),
                group: "g".into(),
                partition: 0,
                attempt: 1,
                records: lens.iter().map(|l| rec(*l)).collect(),
            }))
            .finish()
    };
    assert_eq!(req(&msg(1000)).bytes, QUEUED_TASK_OVERHEAD + 1000);
    assert_eq!(req(&batch(&[100, 200])).bytes, QUEUED_TASK_OVERHEAD + 300);
    let mut a = Adm::new(AdmissionLimits {
        max_inflight: 1,
        max_queued: 100,
        max_queued_bytes: 64 * 1024,
    });
    let _b = block(&mut a);
    assert_eq!(a.admit(&msg(40 * 1024)).unwrap_err(), RejectReason::Overloaded);
    assert_eq!(
        a.admit(&batch(&[20 * 1024, 20 * 1024])).unwrap_err(),
        RejectReason::Overloaded
    );
    assert!(waits(&a.admit(&msg(1024)).unwrap()));
    assert_eq!(a.a.queued().1, QUEUED_TASK_OVERHEAD + 1024);
}

/// A sample on a task that was granted from the wait queue when nothing else waits corrects the
/// tenant without the waitlist (no dispatch), and a correction can lower the charge.
#[test]
fn a_correction_with_nothing_waiting_and_a_shorter_actual_time() {
    let mut a = Adm::new(wide(1));
    let fa = task(1, 11, knobs(BIG, 0, 0));
    let fb = task(2, 21, knobs(BIG, 0, 0));
    a.learn(&fa, Duration::from_millis(500));
    a.learn(&fb, Duration::from_millis(5));
    let b = block(&mut a);
    let x = wait_at(&mut a, &fa, 0);
    let y = wait_at(&mut a, &fb, 0);
    assert_eq!(a.finish(b), vec![x], "tenant 1 listed first");
    // charged 500 ms, took 1 ms (corrected under the waitlist: y waits)
    assert_eq!(a.finish_with(x, 1, Some(Duration::from_millis(1))), vec![y]);
    // charged 5 ms, took 6 ms (corrected with nothing waiting)
    assert!(a.finish_with(y, 2, Some(Duration::from_millis(6))).is_empty());
    let b = block(&mut a);
    let y2 = wait_at(&mut a, &fb, nms(1));
    let z = wait_at(&mut a, &fa, nms(1));
    assert_eq!(a.finish(b), vec![z], "tenant 1 was charged only its actual time");
    assert_eq!(a.finish(z), vec![y2]);
}

/// Calls are held by the service ceiling and the tenant's call allowance (= the plan cap), not by
/// the adaptive limit; builds are held by nothing.
#[test]
fn calls_and_builds_are_not_blocked_by_the_adaptive_limit() {
    let mut a = Adm::new(limits(100));
    let mut k = knobs(4, 0, 3);
    k.scaling.start = 1;
    k.scaling.min = 1;
    let http = a.admit(&task(1, 1, k)).unwrap();
    assert!(runs(&http));
    assert!(waits(&a.admit(&task(1, 1, k)).unwrap()), "http: adaptive limit 1");
    for _ in 0..3 {
        let c = a.admit(&call(1, 1, k)).unwrap();
        assert!(runs(&c), "a call runs up to the ceiling of 4");
        assert_eq!(c.ticket.class, SlotClass::Call);
    }
    assert!(
        waits(&a.admit(&call(1, 1, k)).unwrap()),
        "the ceiling is a hard cap for calls too"
    );
    let b = a.admit(&build_of(1, 1)).unwrap();
    assert!(runs(&b) && b.ticket.class == SlotClass::Build, "builds never wait");
}

fn call(tenant: u128, service: u128, k: ServiceKnobs) -> TaskSpec {
    SpecBuilder::deno()
        .tenant(tenant)
        .service(service)
        .knobs(k)
        .trigger(Trigger::Call {
            parent: TaskId(1),
            depth: 1,
        })
        .finish()
}

/// An eager build with default knobs (`tenant_max_concurrency = 0`).
fn build_of(tenant: u128, service: u128) -> TaskSpec {
    SpecBuilder::build().tenant(tenant).service(service).finish()
}

/// Review 1: neither a build (default knobs) nor a call may lift the tenant's plan cap, whether it
/// creates the service's admission state or comes later.
#[test]
fn builds_and_calls_never_reset_the_tenant_plan_cap() {
    let mut a = Adm::new(limits(100));
    assert!(
        runs(&a.admit(&build_of(1, 1)).unwrap()),
        "an eager build creates the slot, with cap 0"
    );
    let k = knobs(BIG, 0, 2);
    assert!(runs(&a.admit(&task(1, 1, k)).unwrap()));
    assert!(runs(&a.admit(&task(1, 2, k)).unwrap()));
    assert!(
        waits(&a.admit(&task(1, 3, k)).unwrap()),
        "the plan cap of 2 holds after the build"
    );
    let unlimited = knobs(BIG, 0, 0);
    a.admit(&build_of(1, 1)).unwrap();
    a.admit(&call(1, 4, unlimited)).unwrap();
    assert!(
        waits(&a.admit(&task(1, 5, k)).unwrap()),
        "still capped after a build and a call with cap 0"
    );
    // a plan change reaches the cap with the next request (refreshed on every request)
    let raised = knobs(BIG, 0, 10);
    let r = a.admit(&task(1, 6, raised)).unwrap();
    assert_eq!(
        r.granted.len(),
        3,
        "a raised plan applies at once: both waiters and the new request run"
    );
}

/// Review 2: A (requests) calls B (`max_concurrency = 1`); the tenant is at its plan cap and more
/// A requests wait. B's queued call must get B's freed slot (it is not held by the tenant cap):
/// otherwise A's requests wait for calls that wait behind A (deadlock until timeout).
#[test]
fn a_call_waiting_behind_a_tenant_capped_request_still_gets_its_slot() {
    let mut a = Adm::new(limits(100));
    let ka = knobs(BIG, 0, 2);
    let kb = knobs(1, 0, 2);
    // two services of A, so the third request waits on the tenant cap, not on its own limit
    assert!(runs(&a.admit(&task(1, 1, ka)).unwrap()));
    assert!(runs(&a.admit(&task(1, 3, ka)).unwrap()));
    assert!(
        waits(&a.admit(&task(1, 1, ka)).unwrap()),
        "a third request: tenant at its cap"
    );
    assert_eq!(a.a.service(ids::service(1)).map(|s| s.limit), Some(2));
    assert!(runs(&a.admit(&call(1, 2, kb)).unwrap()), "A1 calls B");
    let b1 = a.next;
    assert!(
        waits(&a.admit(&call(1, 2, kb)).unwrap()),
        "A2 calls B: B is at its ceiling of 1"
    );
    let b2 = a.next;
    assert_eq!(a.finish(b1), vec![b2], "B's freed slot goes to A2's call");
}

fn reentrant(mut spec: TaskSpec) -> TaskSpec {
    spec.reentrant = true;
    spec
}

fn reply_of(tenant: u128, service: u128, k: ServiceKnobs, depth: u8) -> TaskSpec {
    SpecBuilder::deno()
        .tenant(tenant)
        .service(service)
        .knobs(k)
        .trigger(Trigger::Pubsub {
            subject: "self.ping".into(),
            depth,
            reply: true,
        })
        .finish()
}

/// A re-entrant call (submitted by a task of the same service, which waits for it holding one of
/// the service's slots) is not held by `max_instances`: at a ceiling of 1 a self-call and a
/// self-request reply run beside their parent. Requests and calls from other services still wait
/// on the ceiling.
#[test]
fn adm_reentrant_calls_run_past_max_instances_others_wait() {
    let mut a = Adm::new(limits(100));
    let k = knobs(1, 0, 0);
    let parent = a.admit(&task(1, 1, k)).unwrap();
    assert!(runs(&parent), "the requester takes the only instance");
    let p = a.next;
    assert!(
        waits(&a.admit(&task(1, 1, k)).unwrap()),
        "a request waits on the ceiling of 1"
    );
    let queued_req = a.next;
    assert!(
        waits(&a.admit(&call(1, 1, k)).unwrap()),
        "a call from another service waits on it"
    );
    let queued_call = a.next;

    let c = a.admit(&reentrant(call(1, 1, k))).unwrap();
    assert!(runs(&c), "a self-call runs beside its parent");
    let self_call = a.next;
    assert_eq!(c.ticket.class, SlotClass::Call);
    let r = a.admit(&reentrant(reply_of(1, 1, k, 2))).unwrap();
    assert!(runs(&r), "a self-request reply (nested deeper) runs too");
    let self_reply = a.next;
    assert_eq!(a.a.service(ids::service(1)).map(|s| (s.running, s.max)), Some((3, 1)));

    // the overshoot drains first: the ceiling is free again only once all three are done
    assert!(a.finish(self_reply).is_empty());
    assert!(
        a.finish(self_call).is_empty(),
        "running 1 = the ceiling: nobody is granted"
    );
    assert_eq!(a.finish(p), vec![queued_call], "the waiting call goes first");
    assert_eq!(a.finish(queued_call), vec![queued_req]);
    a.finish(queued_req);
    assert_eq!((a.a.inflight(), a.a.running(), a.a.queued()), (0, 0, (0, 0)));
}

/// The re-entrant exemption covers the service ceiling only: the flag means nothing on a request
/// (`Throttled`), and a re-entrant call is still held by the tenant's call allowance at its depth
/// and by `max_inflight`.
#[test]
fn adm_reentrant_calls_stay_within_the_tenant_allowance_and_max_inflight() {
    let mut a = Adm::new(limits(100));
    let k = knobs(1, 0, 2);
    assert!(runs(&a.admit(&task(1, 1, k)).unwrap()));
    assert!(
        waits(&a.admit(&reentrant(task(1, 1, k))).unwrap()),
        "a request is never re-entrant: it waits on the ceiling"
    );
    assert!(runs(&a.admit(&reentrant(call(1, 1, k))).unwrap()));
    assert!(
        runs(&a.admit(&reentrant(call(1, 1, k))).unwrap()),
        "two depth-1 calls: the plan cap of 2"
    );
    assert!(
        waits(&a.admit(&reentrant(call(1, 1, k))).unwrap()),
        "the third depth-1 call waits on the tenant's call allowance"
    );
    assert!(
        runs(&a.admit(&reentrant(reply_of(1, 1, k, 2))).unwrap()),
        "depth 2 has its own allowance"
    );

    let mut b = Adm::new(limits(2));
    let k = knobs(1, 0, 0);
    assert!(runs(&b.admit(&task(2, 2, k)).unwrap()));
    assert!(runs(&b.admit(&reentrant(call(2, 2, k))).unwrap()), "the call reserve");
    let blocked = b.admit(&reentrant(call(2, 2, k))).unwrap();
    assert!(waits(&blocked), "max_inflight of 2 holds a re-entrant call");
    let n = b.next;
    assert_eq!(
        b.finish(n - 1),
        vec![n],
        "granted past the ceiling once a node slot frees"
    );
}

/// Review 7: a call queued for a service that is already listed, while its tenant is out of the
/// ring (capped), is seen as soon as a node slot frees.
#[test]
fn a_waiting_call_is_granted_when_another_tenant_frees_a_node_slot() {
    let mut a = Adm::new(limits(3));
    let k = knobs(BIG, 0, 1);
    assert!(runs(&a.admit(&task(1, 1, k)).unwrap()), "tenant 1 at its cap of 1");
    assert!(
        waits(&a.admit(&task(1, 2, k)).unwrap()),
        "tenant-capped request of service 2"
    );
    assert!(
        runs(&a.admit(&task(2, 9, knobs(BIG, 0, 0))).unwrap()),
        "the requests' share is full"
    );
    let other = a.next;
    assert!(
        runs(&a.admit(&build_of(3, 8)).unwrap()),
        "a build takes the reserve: the node is full now"
    );
    let mut kc = k;
    kc.tenant_max_concurrency = 2;
    assert!(
        waits(&a.admit(&call(1, 2, kc)).unwrap()),
        "a call of service 2 waits for a node slot"
    );
    let c = a.next;
    assert_eq!(a.finish(other), vec![c], "the node slot goes to the call");
}

/// Review 3: one tenant may hold at most half of the node queue (count and bytes), however many
/// services it spreads over; an explicit `scaling.max_queued` above the node share is clamped.
#[test]
fn a_tenant_holds_at_most_half_the_node_queue_and_explicit_bounds_are_clamped() {
    let mut a = Adm::new(AdmissionLimits {
        max_inflight: 1,
        max_queued: 8,
        max_queued_bytes: 1 << 30,
    });
    assert!(runs(&a.admit(&task(9, 9, knobs(BIG, 0, 0))).unwrap()));
    for s in [1, 2] {
        for _ in 0..2 {
            assert!(waits(&a.admit(&task(1, s, knobs(BIG, 0, 0))).unwrap()));
        }
    }
    let e = a
        .admit(&task(1, 3, knobs(BIG, 0, 0)))
        .expect_err("tenant 1 holds 4 of 8");
    assert_eq!(e, RejectReason::Overloaded);
    assert!(
        waits(&a.admit(&task(2, 4, knobs(BIG, 0, 0))).unwrap()),
        "another tenant still has room"
    );

    let mut a = Adm::new(AdmissionLimits {
        max_inflight: 1,
        max_queued: 4,
        max_queued_bytes: 1 << 30,
    });
    let greedy = knobs(BIG, u32::MAX, 0);
    assert!(runs(&a.admit(&task(1, 1, greedy)).unwrap()));
    assert!(waits(&a.admit(&task(1, 1, greedy)).unwrap()));
    assert!(waits(&a.admit(&task(1, 1, greedy)).unwrap()));
    assert_eq!(
        a.admit(&task(1, 1, greedy)).err(),
        Some(RejectReason::Overloaded),
        "max_queued = u32::MAX clamps to 2"
    );

    let mut a = Adm::new(AdmissionLimits {
        max_inflight: 1,
        max_queued: 1000,
        max_queued_bytes: 20 * 1024,
    });
    assert!(runs(&a.admit(&task(9, 9, knobs(BIG, 0, 0))).unwrap()));
    assert!(waits(&a.admit(&task(1, 1, knobs(BIG, 0, 0))).unwrap()));
    assert!(
        waits(&a.admit(&task(1, 2, knobs(BIG, 0, 0))).unwrap()),
        "8 KiB of the tenant's 10 KiB"
    );
    assert_eq!(
        a.admit(&task(1, 3, knobs(BIG, 0, 0))).err(),
        Some(RejectReason::Overloaded),
        "bytes share per tenant"
    );
}

/// Review 4: calls have reserved headroom in `max_inflight` (requests may use 7/8 of it) and a
/// per-tenant allowance equal to the plan cap, so a fan-out cannot take every slot.
#[test]
fn calls_have_reserved_node_headroom_and_a_tenant_allowance() {
    let mut a = Adm::new(limits(16));
    for s in 0..14 {
        assert!(runs(&a.admit(&task(1, 100 + s, knobs(BIG, 0, 0))).unwrap()));
    }
    assert!(
        waits(&a.admit(&task(1, 200, knobs(BIG, 0, 0))).unwrap()),
        "requests stop at 14 of 16"
    );
    assert!(
        runs(&a.admit(&call(1, 300, knobs(BIG, 0, 0))).unwrap()),
        "calls use the reserve"
    );
    assert!(runs(&a.admit(&call(1, 301, knobs(BIG, 0, 0))).unwrap()));
    assert!(
        waits(&a.admit(&call(1, 302, knobs(BIG, 0, 0))).unwrap()),
        "the node bound holds for calls"
    );

    let mut a = Adm::new(limits(100));
    let k = knobs(BIG, 0, 2);
    assert!(runs(&a.admit(&task(1, 9, k)).unwrap()), "a request brings the plan cap");
    assert!(runs(&a.admit(&call(1, 1, k)).unwrap()));
    assert!(runs(&a.admit(&call(1, 2, k)).unwrap()));
    assert!(
        waits(&a.admit(&call(1, 3, k)).unwrap()),
        "at most `plan cap` calls of a tenant in flight"
    );
}

/// A waiter granted a slot at the moment its queue timeout fires: exactly one wins (the grant or
/// the cancel), and every counter returns to zero.
#[test]
fn a_grant_racing_a_queue_timeout_has_exactly_one_winner() {
    for _ in 0..200 {
        let a = Arc::new(Admission::<u32>::new(limits(1)));
        let spec = task(1, 1, knobs(BIG, 0, 0));
        let mut first = a
            .admit(&req(&spec), NORMAL, |_| 1)
            .map_err(|e| e.reason)
            .unwrap()
            .ticket;
        let mut waiter = None;
        let w = a.admit(&req(&spec), NORMAL, |t| {
            waiter = Some(t.clone());
            2
        });
        assert!(w.map_err(|e| e.reason).unwrap().run.is_none());
        let waiter = waiter.unwrap();
        let canceller = {
            let (a, waiter) = (a.clone(), waiter.clone());
            std::thread::spawn(move || a.cancel(&waiter, true, |n| *n == 2))
        };
        first.queued = false;
        let granted = a.release(&first, 1, None);
        let cancelled = canceller.join().unwrap();
        assert!(
            cancelled != (granted == vec![2]),
            "exactly one of grant ({granted:?}) and cancel ({cancelled})"
        );
        if !cancelled {
            let mut t = waiter;
            t.queued = false;
            a.release(&t, 2, None);
        }
        assert_eq!((a.running(), a.queued()), (0, (0, 0)));
    }
}

/// Forgetting an idle service (autoscaler GC) never races an admit into two states for one
/// service: its limit holds throughout.
#[test]
fn retiring_idle_service_state_never_splits_a_service() {
    let a = Arc::new(Admission::<u32>::new(limits(1000)));
    let live = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gc = {
        let (a, stop) = (a.clone(), stop.clone());
        std::thread::spawn(move || {
            let sig = NodeSignals {
                now_ns: u64::MAX / 2,
                pressure: Pressure::Normal,
                cpu_saturated: [false; 3],
            };
            while !stop.load(Ordering::Relaxed) {
                a.tick(&sig, &mut Vec::new());
            }
        })
    };
    let ids = Arc::new(AtomicU64::new(0));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let (a, live, ids) = (a.clone(), live.clone(), ids.clone());
            std::thread::spawn(move || {
                let mut k = knobs(2, 0, 0);
                k.idle_ttl = Duration::ZERO;
                let spec = task(1, 1, k);
                for _ in 0..3_000 {
                    let me = ids.fetch_add(1, Ordering::Relaxed) as u32;
                    let mut tk = None;
                    let r = a
                        .admit(&req(&spec), NORMAL, |t| {
                            tk = Some(t.clone());
                            me
                        })
                        .map_err(|e| e.reason)
                        .unwrap();
                    let mut tk = tk.unwrap();
                    // Waiting and not cancelled in time: someone granted it a slot, so it runs.
                    let running = r.run.is_some() || !a.cancel(&tk, false, |n| *n == me);
                    if running {
                        assert!(
                            live.fetch_add(1, Ordering::SeqCst) < 2,
                            "the service ran more than its limit of 2"
                        );
                        live.fetch_sub(1, Ordering::SeqCst);
                        tk.queued = false;
                        // grants for other workers' items are theirs to notice
                        let _ = a.release(&tk, 0, None);
                    }
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    gc.join().unwrap();
}

#[test]
fn admission_checks_are_applied_in_order() {
    let k = knobs(1, 0, 1);
    let mut a = Adm::new(limits(100));
    let deep = SpecBuilder::deno()
        .service(1)
        .knobs(k)
        .trigger(Trigger::Call {
            parent: TaskId(1),
            depth: 16,
        })
        .finish();
    let all_bad = AdmitContext {
        mode: DegradationMode::Drain,
        t1_ready: false,
    };
    assert_eq!(
        a.admit_cx(&deep, all_bad).err(),
        Some(RejectReason::CallDepth),
        "call depth beats everything"
    );
    a.admit(&task(1, 1, k)).expect("fills every limit");
    assert_eq!(
        a.admit_cx(&task(1, 1, k), all_bad).err(),
        Some(RejectReason::Draining),
        "drain beats shed and capacity"
    );
    let shed_miss = AdmitContext {
        mode: DegradationMode::Shed,
        t1_ready: false,
    };
    assert_eq!(a.admit_cx(&task(1, 1, k), shed_miss).err(), Some(RejectReason::Shed));
    assert!(waits(&a.admit(&task(1, 1, k)).unwrap()), "capacity: wait");
}

#[test]
fn admission_call_depth_limit_is_15() {
    let mut a = Adm::new(limits(100));
    let at = |depth| {
        SpecBuilder::deno()
            .service(1)
            .knobs(knobs(BIG, 0, 0))
            .trigger(Trigger::Call {
                parent: TaskId(1),
                depth,
            })
            .finish()
    };
    assert!(a.admit(&at(15)).is_ok(), "depth 15 is the deepest allowed call");
    assert_eq!(a.admit(&at(16)).err(), Some(RejectReason::CallDepth));
    assert_eq!(RejectReason::CallDepth.http_status(), 508);
    assert_eq!(RejectReason::CallDepth.reason(), "call_depth");
}

/// The autoscaler grows a limit that queued work keeps hitting and lets the waiters in.
#[test]
fn the_autoscaler_tick_grows_a_binding_limit_and_grants_the_waiters() {
    let mut a = Adm::new(limits(1000));
    let k = ServiceKnobs {
        max_instances: 64,
        scaling: ScalingKnobs {
            start: 2,
            min: 1,
            ..ScalingKnobs::default()
        },
        ..ServiceKnobs::default()
    };
    for _ in 0..2 {
        assert!(runs(&a.admit(&task(1, 1, k)).unwrap()));
    }
    for _ in 0..10 {
        assert!(waits(&a.admit(&task(1, 1, k)).unwrap()));
    }
    let sig = NodeSignals {
        now_ns: 100_000_000,
        pressure: Pressure::Normal,
        cpu_saturated: [false; 3],
    };
    let mut reports = Vec::new();
    let granted = a.a.tick(&sig, &mut reports);
    assert_eq!(granted.len(), 2, "slow start doubles 2 -> 4");
    let r = reports[0];
    assert_eq!((r.admission.limit, r.change.map(|c| c.0)), (4, Some(2)));
    let granted = a.a.tick(
        &NodeSignals {
            now_ns: 200_000_000,
            ..sig
        },
        &mut Vec::new(),
    );
    assert_eq!(granted.len(), 4, "4 -> 8");
    let mut reports = Vec::new();
    a.a.tick(
        &NodeSignals {
            now_ns: 300_000_000,
            pressure: Pressure::High,
            ..sig
        },
        &mut reports,
    );
    assert_eq!(reports[0].admission.limit, 6, "memory pressure High: -25 %");
    assert_eq!(
        a.a.service(ids::service(1)).map(|s| (s.running, s.queued)),
        Some((8, 4)),
        "running tasks are not preempted"
    );
}

/// Many threads admitting and releasing across services and tenants: no waiter is lost (left
/// queued with nothing running) or granted twice, and every counter returns to zero.
#[test]
fn concurrent_admits_and_releases_conserve_every_task() {
    let a = Arc::new(Admission::<u64>::new(AdmissionLimits {
        max_inflight: 8,
        max_queued: 1 << 20,
        max_queued_bytes: 1 << 40,
    }));
    let tickets = Arc::new(Mutex::new(BTreeMap::<u64, AdmissionTicket>::new()));
    let next = Arc::new(AtomicU64::new(0));
    let ran = Arc::new(Mutex::new(Vec::<u64>::new()));
    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let (a, tickets, next, ran) = (a.clone(), tickets.clone(), next.clone(), ran.clone());
            std::thread::spawn(move || {
                // Items this thread must finish: admitted to run, or granted to this thread.
                let mut held: Vec<u64> = Vec::new();
                let finish = |n: u64, held: &mut Vec<u64>| {
                    let mut tk = tickets.lock().unwrap().remove(&n).expect("ticket");
                    tk.queued = false;
                    ran.lock().unwrap().push(n);
                    held.extend(a.release(&tk, n, None));
                };
                for i in 0..2_000u64 {
                    let service = u128::from((t * 7 + i) % 5 + 1);
                    let spec = task(service % 2 + 1, service, knobs(3, 0, 5));
                    let n = next.fetch_add(1, Ordering::Relaxed);
                    let r = a
                        .admit(&req(&spec), NORMAL, |tk| {
                            tickets.lock().unwrap().insert(n, tk.clone());
                            n
                        })
                        .map_err(|e| e.reason)
                        .expect("bounds are large");
                    held.extend(r.run);
                    held.extend(r.granted);
                    if i % 2 == 0 {
                        if let Some(n) = held.pop() {
                            finish(n, &mut held);
                        }
                    }
                }
                while let Some(n) = held.pop() {
                    finish(n, &mut held);
                }
            })
        })
        .collect();
    for h in threads {
        h.join().expect("worker");
    }
    let mut ran = ran.lock().unwrap().clone();
    let total = next.load(Ordering::Relaxed);
    ran.sort_unstable();
    let len = ran.len();
    ran.dedup();
    assert_eq!(ran.len(), len, "a task was granted twice");
    assert_eq!(len as u64, total, "every task ran exactly once (none was left waiting)");
    assert_eq!((a.running(), a.queued(), a.inflight()), (0, (0, 0), 0));
}

/// Contention measurement (run with `--ignored --nocapture`): admit + release throughput with
/// 8 threads on distinct services (the hot path: atomics and sharded read locks) and on one
/// service (one contended counter).
#[test]
#[ignore = "benchmark"]
fn bench_admission_throughput() {
    for (label, distinct) in [("distinct services", true), ("one service", false)] {
        let a = Arc::new(Admission::<u64>::new(AdmissionLimits {
            max_inflight: 1 << 20,
            max_queued: 1 << 20,
            max_queued_bytes: 1 << 40,
        }));
        let n = 200_000u64;
        let start = Instant::now();
        let threads: Vec<_> = (0..8u128)
            .map(|t| {
                let a = a.clone();
                std::thread::spawn(move || {
                    let spec = task(t + 1, if distinct { t + 1 } else { 1 }, knobs(1 << 20, 0, 0));
                    let r = req(&spec);
                    for i in 0..n {
                        let x = a.admit(&r, NORMAL, |_| i).expect("admitted");
                        a.release(&x.ticket, i, None);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("worker");
        }
        let el = start.elapsed();
        println!(
            "{label}: {:.0} admit+release/s ({:?} for {} ops)",
            (8 * n) as f64 / el.as_secs_f64(),
            el,
            8 * n
        );
    }
    // The design this replaced: one global mutex over hash maps for every admit and release.
    let old = Arc::new(Mutex::new((
        0u32,
        BTreeMap::<u128, u32>::new(),
        BTreeMap::<u128, u32>::new(),
    )));
    let n = 200_000u64;
    let start = Instant::now();
    let threads: Vec<_> = (0..8u128)
        .map(|t| {
            let old = old.clone();
            std::thread::spawn(move || {
                for _ in 0..n {
                    {
                        let mut g = old.lock().unwrap();
                        g.0 += 1;
                        *g.1.entry(t).or_insert(0) += 1;
                        *g.2.entry(t).or_insert(0) += 1;
                    }
                    let mut g = old.lock().unwrap();
                    g.0 -= 1;
                    *g.1.get_mut(&t).unwrap() -= 1;
                    *g.2.get_mut(&t).unwrap() -= 1;
                }
            })
        })
        .collect();
    for t in threads {
        t.join().expect("worker");
    }
    let el = start.elapsed();
    println!(
        "global mutex (old design): {:.0} admit+release/s",
        (8 * n) as f64 / el.as_secs_f64()
    );
}

// ---------------------------------------------------------------------------------------------
// Admission through the scheduler
// ---------------------------------------------------------------------------------------------

/// Real threads: the autoscaler runs on its own thread (review 8), off sysmon, and still grows a
/// binding limit while requests wait.
#[test]
fn real_threads_autoscale_on_their_own_thread() {
    let _serial = serial();
    let real = build_real(real_cfg(2, 1));
    real.book.set_default(script![io(400)]);
    let handles: Vec<_> = (0..30)
        .map(|_| real.sched.submit(adaptive(2).finish()).expect("admitted"))
        .collect();
    let grew = wait_until(ms(3_000), || {
        real.sched
            .service_admission(ids::service(1))
            .is_some_and(|s| s.limit > 2)
    });
    assert!(grew, "limit {:?}", real.sched.service_admission(ids::service(1)));
    let outcomes = block_on(futures_util::future::join_all(handles));
    assert!(outcomes.iter().all(|o| matches!(o, Outcome::Ok(_))));
    real.shutdown();
}

/// Real threads, tight limits (service 2, tenant 3, node 4): 300 short I/O tasks over 6 services
/// and 2 tenants all complete; nothing is left waiting or counted.
#[test]
fn real_threads_run_every_waiting_task_under_tight_limits() {
    let _serial = serial();
    let real = build_real(rt_sched::SchedConfig {
        max_inflight: 4,
        max_queued: 1_000,
        ..real_cfg(2, 1)
    });
    real.book.set_default(script![io(1)]);
    let handles: Vec<_> = (0..300u128)
        .map(|i| {
            let service = i % 6 + 1;
            let spec = d(service)
                .tenant(service % 2 + 1)
                .knobs(knobs(2, 0, 3))
                .queue_timeout_ms(60_000)
                .finish();
            real.sched.submit(spec).expect("waits instead of being rejected")
        })
        .collect();
    let outcomes = block_on(futures_util::future::join_all(handles));
    assert!(
        outcomes.iter().all(|o| matches!(o, Outcome::Ok(_))),
        "every task completes"
    );
    assert_eq!(real.sched.queued(), (0, 0));
    assert_eq!(real.sched.snapshot().inflight, 0);
    real.shutdown();
}

/// D-A3: over `sched.max_inflight` a task waits for a slot and then runs (no `503 overloaded`).
#[test]
fn admission_global_max_inflight_queues_and_the_waiter_runs_later() {
    let mut sim = SimRuntime::new(fixed(1, 1).sched(|s| s.max_inflight = 3));
    sim.scripts().set_default(script![io(50)]);
    let mut tasks: Vec<SimTask> = (0..3)
        .map(|i| admitted(sim.submit_existing(d(1 + i).finish())))
        .collect();
    let mut late = admitted(sim.submit_existing(d(9).finish()));
    assert_eq!(late.state(), TaskState::New, "waiting for a run slot");
    assert_eq!(sim.snapshot().inflight, 4);
    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
    assert_ok(sim.outcome(&mut late));
    assert!(
        late.timings().queue >= ms(50),
        "it waited for a slot: {:?}",
        late.timings().queue
    );
}

/// D-A3: the plan's tenant cap queues the excess; other tenants are unaffected.
#[test]
fn admission_tenant_cap_queues_instead_of_429() {
    let mut sim = sim(1, 1);
    let k = knobs(BIG, 0, 2);
    let spec = |tenant: u128, service: u128| d(service).tenant(tenant).knobs(k).finish();
    let mut a = admitted(sim.submit(spec(1, 1), script![io(50)]));
    let mut b = admitted(sim.submit(spec(1, 2), script![io(50)]));
    let mut c = admitted(sim.submit(spec(1, 3), script![io(50)]));
    let mut other = admitted(sim.submit(spec(2, 4), script![io(50)]));
    sim.run_for(ms(10));
    assert_eq!(c.state(), TaskState::New, "tenant 1 is at its cap");
    assert_ne!(other.state(), TaskState::New, "tenant 2 is not");
    sim.run_until_idle();
    for t in [&mut a, &mut b, &mut c, &mut other] {
        assert_ok(sim.outcome(t));
    }
}

#[test]
fn admission_call_depth_16_is_rejected_with_508() {
    let mut sim = sim(1, 1);
    let spec = d(1)
        .trigger(Trigger::Call {
            parent: TaskId(999),
            depth: 16,
        })
        .finish();
    let e = rejection(sim.submit(spec, Script::noop()));
    assert_eq!(
        (e, e.http_status(), e.reason()),
        (RejectReason::CallDepth, 508, "call_depth")
    );
}

#[test]
fn set_mode_round_trips_on_the_scheduler() {
    let sim = sim(1, 1);
    assert_eq!(sim.scheduler().mode(), DegradationMode::Normal);
    sim.set_mode(DegradationMode::Shed);
    assert_eq!(sim.scheduler().mode(), DegradationMode::Shed);
    assert_eq!(sim.snapshot().mode, DegradationMode::Shed);
    sim.set_mode(DegradationMode::Drain);
    assert_eq!(sim.snapshot().mode, DegradationMode::Drain);
}

// ---------------------------------------------------------------------------------------------
// SCH-16: single-flight build, FIFO wake
// ---------------------------------------------------------------------------------------------

/// SCH-16: 100 concurrent requests to an uncompiled version -> exactly 1 build task runs; all 100
/// complete afterwards; start order is FIFO by enqueue.
#[test]
fn ac_sch_16_hundred_requests_share_one_build_and_start_fifo() {
    let mut sim = SimRuntime::new(fixed(1, 1).auto_ready(false));
    let key = key_of(&d(7).finish());

    let mut tasks: Vec<SimTask> = (0..100)
        .map(|_| admitted(sim.submit(d(7).finish(), Script::noop())))
        .collect();
    sim.run_until_idle();
    assert_eq!(
        sim.snapshot().waiting_runtime,
        100,
        "every request waits for the artifact"
    );
    assert_eq!(
        sim.source().builds_started(),
        1,
        "single-flight: one build for 100 requests"
    );
    assert_eq!(
        sim.scripts().started(),
        0,
        "no request started before the artifact exists"
    );

    // What rt-cache does on the first miss: run exactly one build task (on a build P).
    let build_spec = b(1000).trigger(Trigger::Build { key }).finish();
    let mut build = admitted(sim.submit(build_spec, script![cpu(5)]));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut build));
    assert_eq!(
        sim.snapshot().waiting_runtime,
        100,
        "still waiting until the cache signals completion"
    );

    sim.source().complete(&key);
    sim.wake_runtime_waiters(&key);
    assert_eq!(sim.snapshot().waiting_runtime, 0);
    assert_eq!(
        sim.snapshot().grq_len(TaskKind::DENO),
        100,
        "all waiters wake into GRQ[deno]"
    );
    sim.run_until_idle();

    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
    let order = sim.scripts().start_order();
    let ids: Vec<TaskId> = tasks.iter().map(|t| t.id).collect();
    let started_guest: Vec<TaskId> = order.iter().copied().filter(|t| ids.contains(t)).collect();
    assert_eq!(started_guest, ids, "start order is FIFO by enqueue");
    let build_starts = sim
        .scripts()
        .events()
        .iter()
        .filter(|e| matches!(e, MockEvent::Started { service, .. } if *service == ids::service(1000)))
        .count();
    assert_eq!(build_starts, 1, "exactly one build task ran");
    assert_eq!(sim.source().builds_started(), 1);
}

#[test]
fn ac_sch_16_a_failed_build_fails_every_waiter() {
    let mut sim = SimRuntime::new(fixed(1, 1).auto_ready(false));
    let key = key_of(&d(7).finish());
    let mut tasks: Vec<SimTask> = (0..5)
        .map(|_| admitted(sim.submit(d(7).finish(), Script::noop())))
        .collect();
    sim.run_until_idle();
    assert_eq!(sim.snapshot().waiting_runtime, 5);

    sim.source()
        .fail(&key, BuildFailure::new(BuildFailReason::Syntax, "unexpected token"));
    sim.wake_runtime_waiters(&key);
    sim.run_until_idle();
    for t in &mut tasks {
        match sim.outcome(t) {
            Some(out @ Outcome::BuildFailed(f)) => {
                assert_eq!(f.reason, BuildFailReason::Syntax);
                assert_eq!(out.reason(), Some("build_failed"));
            }
            other => panic!("expected BuildFailed, got {other:?}"),
        }
    }
    assert_eq!(sim.scripts().started(), 0, "no waiter ever started");
    assert_eq!(sim.snapshot().waiting_runtime, 0);
}

#[test]
fn ac_sch_16_waking_a_key_nobody_waits_for_is_a_noop() {
    let mut sim = SimRuntime::new(fixed(1, 1).auto_ready(false));
    let key = key_of(&d(7).finish());
    sim.wake_runtime_waiters(&key);
    sim.run_until_idle();
    assert_eq!(sim.snapshot().waiting_runtime, 0);
    assert_eq!(sim.snapshot().grq_len(TaskKind::DENO), 0);
}

// ---------------------------------------------------------------------------------------------
// SCH-17: memory waits
// ---------------------------------------------------------------------------------------------

/// Bytes the scheduler reserves for one running task (measured, so the test is independent of
/// the exact `isolate_base_mb`).
fn reservation_of_one_task() -> u64 {
    let mut sim = SimRuntime::new(fixed(1, 1).memory_mb(4096));
    let _t = admitted(sim.submit(d(1).finish(), script![io(100)]));
    sim.run_for(ms(10));
    sim.governor().committed()
}

/// SCH-17: governor headroom for 2 tasks, 5 submitted -> 2 run, 3 `WaitingMemory`; they start as
/// memory frees.
#[test]
fn ac_sch_17_two_of_five_tasks_run_the_rest_wait_for_memory_in_fifo_order() {
    let r = reservation_of_one_task();
    assert!(r >= MIB, "a running task must hold at least 1 MiB (got {r})");
    let budget_mb = (2 * r).div_ceil(MIB);

    let mut sim = SimRuntime::new(fixed(1, 1).memory_mb(budget_mb));
    sim.scripts().set_default(script![io(100)]);
    let mut tasks: Vec<SimTask> = (0..5).map(|_| admitted(sim.submit_existing(d(1).finish()))).collect();
    let ids: Vec<TaskId> = tasks.iter().map(|t| t.id).collect();

    sim.run_for(ms(50));
    assert_eq!(sim.scripts().started(), 2, "2 run");
    assert_eq!(sim.snapshot().waiting_memory, 3, "3 wait for memory");
    assert_eq!(
        sim.scripts().start_order(),
        ids[..2].to_vec(),
        "the first two submitted are the ones that run"
    );
    assert_eq!(sim.governor().committed(), 2 * r);

    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
    assert_eq!(sim.scripts().start_order(), ids, "waiters start FIFO as memory frees");
    assert_eq!(sim.governor().committed(), 0);
    assert_eq!(sim.snapshot().waiting_memory, 0);
}

/// SCH-17: any task waiting for memory longer than `queue_timeout` gets `503 queue_timeout`.
#[test]
fn ac_sch_17_memory_waiters_past_queue_timeout_are_shed() {
    let r = reservation_of_one_task();
    let budget_mb = (2 * r).div_ceil(MIB);

    let mut sim = SimRuntime::new(fixed(1, 1).memory_mb(budget_mb));
    sim.scripts().set_default(script![io(10_000)]);
    let mut tasks: Vec<SimTask> = (0..5)
        .map(|_| admitted(sim.submit_existing(d(1).queue_timeout_ms(500).finish())))
        .collect();

    sim.run_for(ms(700));
    for t in &mut tasks[..2] {
        assert!(sim.outcome(t).is_none(), "the two running tasks are unaffected");
    }
    for t in &mut tasks[2..] {
        assert_rejected(sim.outcome(t), RejectReason::QueueTimeout);
    }
    assert_eq!(sim.scripts().started(), 2, "the shed tasks never started");
    assert_eq!(sim.snapshot().waiting_memory, 0);
    assert_eq!(
        sim.governor().committed(),
        2 * r,
        "nothing was reserved for the shed tasks"
    );
}

// ---------------------------------------------------------------------------------------------
// SCH-18: per-service concurrency (D-A3: the excess waits)
// ---------------------------------------------------------------------------------------------

/// SCH-18: service `max_concurrency=2` (explicit hard cap), 4 concurrent requests -> 2 run, 2 wait
/// in the service queue and start FIFO as slots free; all 4 complete, none is rejected.
#[test]
fn ac_sch_18_two_run_two_wait_and_all_complete() {
    let mut sim = sim(1, 1);
    let spec = || d(1).max_concurrency(2, 0).finish();

    let mut t1 = admitted(sim.submit(spec(), script![io(100)]));
    let mut t2 = admitted(sim.submit_existing(spec()));
    let mut t3 = admitted(sim.submit_existing(spec()));
    let mut t4 = admitted(sim.submit_existing(spec()));
    assert_eq!(sim.snapshot().inflight, 4, "2 running + 2 waiting");
    assert_eq!(sim.scheduler().queued().0, 2);

    sim.run_for(ms(50));
    assert_eq!(sim.scripts().started(), 2, "2 run");
    assert_eq!(
        (t3.state(), t4.state()),
        (TaskState::New, TaskState::New),
        "the others wait for a slot"
    );

    sim.run_until_idle();
    for t in [&mut t1, &mut t2, &mut t3, &mut t4] {
        assert_ok(sim.outcome(t));
    }
    let events = sim.scripts().events();
    let started = |id: TaskId| {
        events
            .iter()
            .position(|e| matches!(e, MockEvent::Started { task, .. } if *task == id))
            .expect("started")
    };
    let first_finish = events
        .iter()
        .position(|e| matches!(e, MockEvent::Finished { .. }))
        .expect("someone finished");
    assert!(
        first_finish < started(t3.id),
        "a waiting task starts only after a slot freed"
    );
    assert!(started(t3.id) < started(t4.id), "FIFO");
    assert_eq!(sim.snapshot().inflight, 0);
}

/// SCH-18: an explicit `scaling.max_queued` bounds the wait: beyond it `503 overloaded` with
/// `Retry-After` (not `429`).
#[test]
fn ac_sch_18_an_explicit_max_queued_sheds_the_excess_with_503_overloaded() {
    let mut sim = sim(1, 1);
    let spec = || d(1).max_concurrency(2, 1).finish();
    let _t: Vec<SimTask> = (0..3)
        .map(|i| {
            admitted(if i == 0 {
                sim.submit(spec(), script![io(100)])
            } else {
                sim.submit_existing(spec())
            })
        })
        .collect();
    let e = rejection(sim.submit_existing(spec()));
    assert_eq!(
        (e, e.http_status(), e.reason()),
        (RejectReason::Overloaded, 503, "overloaded")
    );
    assert_eq!(e.retry_after(), Some(Duration::from_secs(1)));
}

/// SCH-18: a waiting task that gets no slot within `queue_timeout` ends `503 queue_timeout` and
/// never starts.
#[test]
fn ac_sch_18_a_waiting_task_past_its_queue_timeout_is_shed() {
    let mut sim = sim(1, 1);
    let mut busy = admitted(sim.submit(d(1).max_concurrency(1, 0).finish(), script![io(1_000)]));
    let mut late = admitted(sim.submit_existing(d(1).max_concurrency(1, 0).queue_timeout_ms(200).finish()));
    sim.run_for(ms(300));
    assert_rejected(sim.outcome(&mut late), RejectReason::QueueTimeout);
    assert_eq!(Outcome::Rejected(RejectReason::QueueTimeout).http_status(), 503);
    assert!(!sim.scripts().start_order().contains(&late.id));
    assert_eq!(sim.scheduler().queued(), (0, 0), "its place in the queue is returned");
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut busy));
}

/// Review 5: a client that goes away while its request waits frees the place at once (the task
/// is taken out of its queue and never starts).
#[test]
fn ac_sch_18_an_abandoned_waiter_leaves_the_queue_at_once() {
    let mut sim = sim(1, 1);
    let mut busy = admitted(sim.submit(d(1).max_concurrency(1, 0).finish(), script![io(500)]));
    let mut gone = admitted(sim.submit_existing(d(1).max_concurrency(1, 0).finish()));
    sim.run_for(ms(10));
    assert_eq!(sim.scheduler().queued().0, 1);
    assert!(gone.cancel_if_waiting(), "it was waiting");
    assert_eq!(sim.scheduler().queued(), (0, 0), "its room is returned at once");
    assert!(!gone.cancel_if_waiting(), "only once");
    assert!(!busy.cancel_if_waiting(), "a started task is not cancelled");
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut busy));
    assert!(matches!(sim.outcome(&mut gone), Some(Outcome::Killed(_))));
    assert!(!sim.scripts().start_order().contains(&gone.id), "it never started");
}

/// Review 5: `Retry-After` follows the queue: at least 1 s, about the time the queue needs to
/// drain at the recent completion rate, with jitter so clients do not retry in lockstep.
#[test]
fn the_retry_after_hint_follows_the_queue_and_is_jittered() {
    let mut sim = SimRuntime::new(fixed(1, 1));
    let idle = sim.scheduler().retry_after_hint();
    assert!(
        idle >= Duration::from_secs(1) && idle <= Duration::from_millis(1_500),
        "{idle:?}"
    );
    sim.scripts().set_default(script![io(100)]);
    // one at a time, 100 ms each: 10 tasks/s; 300 waiting => about 30 s
    let _tasks: Vec<SimTask> = (0..301)
        .map(|_| admitted(sim.submit_existing(d(1).max_concurrency(1, 0).queue_timeout_ms(600_000).finish())))
        .collect();
    sim.run_for(ms(1_050));
    let hints: Vec<Duration> = (0..20).map(|_| sim.scheduler().retry_after_hint()).collect();
    let (lo, hi) = (hints.iter().min().unwrap(), hints.iter().max().unwrap());
    assert!(
        *lo >= Duration::from_secs(15) && *hi <= Duration::from_secs(60),
        "{hints:?}"
    );
    assert!(lo < hi, "jittered: {hints:?}");
}

// ---------------------------------------------------------------------------------------------
// SCH-28 / SCH-29 / SCH-30: autoscaling, warm targets, memory ladder (D-A3)
// ---------------------------------------------------------------------------------------------

fn adaptive(start: u32) -> SpecBuilder {
    SpecBuilder::deno().service(1).scaling(ScalingKnobs {
        start,
        min: 1,
        ..ScalingKnobs::default()
    })
}

fn limit_of(sim: &SimRuntime, service: u128) -> u32 {
    sim.scheduler()
        .service_admission(ids::service(service))
        .expect("admission state")
        .limit
}

/// SCH-28: an I/O-bound service starting at limit 2 under 40 concurrent requests -> the limit
/// grows each autoscaler tick while tasks wait and latency stays at its baseline; memory pressure
/// `High` shrinks it; every request completes, none is rejected.
#[test]
fn ac_sch_28_the_adaptive_limit_grows_under_queued_load_and_shrinks_under_memory_pressure() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    sim.scripts().set_default(script![io(1_000)]);
    let mut tasks: Vec<SimTask> = (0..40)
        .map(|_| admitted(sim.submit_existing(adaptive(2).finish())))
        .collect();
    assert_eq!(limit_of(&sim, 1), 2);
    assert_eq!(sim.scheduler().queued().0, 38);

    sim.run_for(ms(450)); // 4 autoscaler ticks (`rebalance.interval` = 100 ms)
    let grown = limit_of(&sim, 1);
    assert!(grown >= 16, "slow start: 2 -> 4 -> 8 -> 16 -> 32, got {grown}");
    assert_eq!(
        sim.scripts().started() as u32,
        grown.min(40),
        "the waiters started as the limit grew"
    );

    // memory pressure High (>= 80 % of the budget)
    let budget = 1024 * MIB;
    let committed = sim.governor().committed();
    let hold = sim
        .governor()
        .reserve(
            rt_mem::MemCategory::Other,
            budget * 85 / 100 - committed,
            rt_mem::Owner::NONE,
        )
        .expect("reserve");
    assert_eq!(sim.governor().pressure(), Pressure::High);
    sim.run_for(ms(100));
    let shrunk = limit_of(&sim, 1);
    assert!(shrunk < grown, "High pressure shrinks the limit: {grown} -> {shrunk}");
    drop(hold);

    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
}

/// SCH-29: after load, the service's artifact has a warm target of `ceil(demand x headroom / Ps)`
/// per P (capped by the profile); it goes to 0 once the service is idle past its idle TTL.
#[test]
fn ac_sch_29_warm_targets_follow_demand_and_scale_to_zero_after_the_idle_ttl() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    sim.scripts().set_default(script![io(300)]);
    let mut k = ServiceKnobs {
        idle_ttl: Duration::from_secs(5),
        ..ServiceKnobs::default()
    };
    k.scaling = ScalingKnobs {
        start: 8,
        min: 1,
        warm_headroom_pct: 125,
        warm_max_per_proc: 4,
        ..ScalingKnobs::default()
    };
    let spec = || SpecBuilder::deno().service(1).knobs(k).finish();
    let key = rt_sched::WarmKey {
        service: ids::service(1),
        key: key_of(&spec()),
    };
    let warm = sim.scheduler().warm_targets();
    assert_eq!(warm.per_proc(&key), 0, "no demand yet");

    let mut one = admitted(sim.submit_existing(spec()));
    sim.run_for(ms(150));
    assert_eq!(warm.per_proc(&key), 1, "demand 1 x 125 % over 2 Ps: ceil(0.625) = 1");
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut one));

    let mut tasks: Vec<SimTask> = (0..8).map(|_| admitted(sim.submit_existing(spec()))).collect();
    sim.run_for(ms(150));
    assert_eq!(warm.per_proc(&key), 4, "demand 8 x 125 % / 2 Ps = 5, capped at 4 per P");
    let target = warm.snapshot()[&key];
    assert_eq!(target.service.service, ids::service(1));
    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
    sim.advance(Duration::from_secs(6));
    assert_eq!(warm.per_proc(&key), 0, "idle past the 5 s idle TTL: scale to zero");
}

// ---------------------------------------------------------------------------------------------
// SCH-31..SCH-34: instances and autoscaling (D-B2)
// ---------------------------------------------------------------------------------------------

fn scaling_of(sim: &SimRuntime, service: u128) -> rt_sched::ServiceScaling {
    sim.scheduler()
        .service_scaling(ids::service(service))
        .expect("admission state")
}

/// SCH-31: `max_instances` caps the limit under any queue (excess requests wait and still
/// complete); `min_instances` floors it under any pressure; updating the bounds moves the limit
/// into them and says why.
#[test]
fn ac_sch_31_the_limit_stays_within_min_and_max_instances() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    sim.scripts().set_default(script![io(1_000)]);
    let mut tasks: Vec<SimTask> = (0..40)
        .map(|_| admitted(sim.submit_existing(adaptive(2).instances(2, 6).queue_timeout_ms(60_000).finish())))
        .collect();
    let mut peak = 0;
    for _ in 0..40 {
        sim.run_for(ms(100));
        peak = peak.max(scaling_of(&sim, 1).admission.running);
    }
    let sc = scaling_of(&sim, 1);
    assert_eq!(sc.admission.limit, 6, "grown to the cap and no further");
    assert!(
        peak <= 6 && sc.admission.running <= 6,
        "never more than max_instances running: {peak}"
    );
    assert!(sc.admission.queued > 0, "the excess waits instead of being rejected");
    assert_eq!((sc.floor, sc.ceiling, sc.admission.min, sc.admission.max), (2, 6, 2, 6));
    assert_eq!(sc.last_change.expect("the limit moved").cause, rt_sched::Cause::Queue);
    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }

    // memory pressure Critical cuts the limit, but not below min_instances
    let budget = 1024 * MIB;
    let hold = sim
        .governor()
        .reserve(rt_mem::MemCategory::Other, budget * 97 / 100, rt_mem::Owner::NONE)
        .expect("reserve");
    assert_eq!(sim.governor().pressure(), Pressure::Critical);
    for _ in 0..30 {
        sim.run_for(ms(100));
    }
    assert_eq!(limit_of(&sim, 1), 2, "the floor holds under Critical pressure");
    drop(hold);

    // the service is updated to other bounds: the limit moves inside them (cause `bounds`)
    let mut t = admitted(sim.submit_existing(adaptive(2).instances(0, 1).queue_timeout_ms(60_000).finish()));
    let sc = scaling_of(&sim, 1);
    assert_eq!((sc.admission.limit, sc.admission.max, sc.floor), (1, 1, 0));
    let change = sc.last_change.expect("moved by the new bounds");
    assert_eq!((change.cause, change.from, change.to), (rt_sched::Cause::Bounds, 2, 1));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut t));
}

/// SCH-32: a service with `min_instances` keeps its warm target through the idle TTL, the
/// admission state outlives the minimum slot lifetime, `High` pressure keeps the floor and only
/// `Critical` releases it.
#[test]
fn ac_sch_32_min_instances_keep_the_warm_target_through_idle_and_high_pressure() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    sim.scripts().set_default(script![io(100)]);
    let mut k = ServiceKnobs {
        idle_ttl: Duration::from_secs(5),
        min_instances: 2,
        ..ServiceKnobs::default()
    };
    k.scaling = ScalingKnobs {
        start: 8,
        min: 1,
        warm_headroom_pct: 125,
        warm_max_per_proc: 4,
        ..ScalingKnobs::default()
    };
    let spec = || SpecBuilder::deno().service(1).knobs(k).finish();
    let key = rt_sched::WarmKey {
        service: ids::service(1),
        key: key_of(&spec()),
    };
    let warm = sim.scheduler().warm_targets();

    let mut one = admitted(sim.submit_existing(spec()));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut one));
    sim.advance(ms(300));
    assert_eq!(warm.per_proc(&key), 1, "2 instances over 2 Ps: one per P");
    assert_eq!(sim.scheduler().warm_instances().get(&ids::service(1)), Some(&2));

    sim.advance(Duration::from_secs(90)); // far past the idle TTL and the 60 s minimum slot life
    assert_eq!(warm.per_proc(&key), 1, "no scale to zero while min_instances > 0");
    let sc = scaling_of(&sim, 1);
    assert!(sc.idle_for > Duration::from_secs(60), "{:?}", sc.idle_for);
    assert!(sc.admission.limit >= 2 && sc.floor == 2);
    assert_eq!(sim.scheduler().warm_instances().get(&ids::service(1)), Some(&2));

    let budget = 1024 * MIB;
    let high = sim
        .governor()
        .reserve(rt_mem::MemCategory::Other, budget * 85 / 100, rt_mem::Owner::NONE)
        .expect("reserve");
    assert_eq!(sim.governor().pressure(), Pressure::High);
    sim.advance(ms(500));
    assert_eq!(
        warm.per_proc(&key),
        1,
        "High pressure halves demand-driven targets, not the floor"
    );
    let critical = sim
        .governor()
        .reserve(rt_mem::MemCategory::Other, budget * 10 / 100, rt_mem::Owner::NONE)
        .expect("reserve");
    sim.advance(ms(500));
    assert_eq!(warm.per_proc(&key), 0, "Critical releases everything");
    drop(critical);
    drop(high);
    sim.advance(ms(500));
    assert_eq!(warm.per_proc(&key), 1, "and the floor returns with the pressure");

    // the service is updated (or deleted): its floor is released at once, so an idle service with
    // no further request scales to zero and its state is forgotten
    sim.scheduler().release_floor(ids::service(1));
    sim.advance(ms(500));
    assert_eq!(warm.per_proc(&key), 0, "no floor, idle past the TTL: scale to zero");
    sim.advance(Duration::from_secs(70));
    assert!(
        sim.scheduler().service_scaling(ids::service(1)).is_none(),
        "and the admission state is retired"
    );
    // a request with the same knobs brings the floor back
    let mut again = admitted(sim.submit_existing(spec()));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut again));
    sim.advance(Duration::from_secs(70));
    assert_eq!(
        warm.per_proc(&key),
        1,
        "the next request applies the service's current knobs"
    );
}

/// SCH-37: a service with `min_instances` gets its admission state and warm floor from
/// `pin_service` before it has seen a single request (node start, version activation, config
/// change); a service without a floor is not kept, and an update that drops the floor releases it.
#[test]
fn ac_sch_37_pinning_a_service_publishes_its_floor_before_any_request() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    let mut k = ServiceKnobs {
        idle_ttl: Duration::from_secs(5),
        min_instances: 2,
        ..ServiceKnobs::default()
    };
    k.scaling = ScalingKnobs {
        start: 8,
        min: 1,
        warm_headroom_pct: 125,
        warm_max_per_proc: 4,
        ..ScalingKnobs::default()
    };
    let spec = |k: ServiceKnobs| SpecBuilder::deno().service(1).knobs(k).finish();
    let wk = |key| rt_sched::WarmKey {
        service: ids::service(1),
        key,
    };
    let key = key_of(&spec(k));
    let warm = sim.scheduler().warm_targets();
    assert!(
        sim.scheduler().service_scaling(ids::service(1)).is_none(),
        "nothing yet"
    );

    assert_eq!(
        sim.scheduler().pin_service(&spec(k)),
        Some(key),
        "the artifact to load for it"
    );
    sim.advance(ms(300));
    assert_eq!(
        warm.per_proc(&wk(key)),
        1,
        "2 instances over 2 Ps: one per P, with no request"
    );
    assert_eq!(sim.scheduler().warm_instances().get(&ids::service(1)), Some(&2));
    let sc = scaling_of(&sim, 1);
    assert_eq!((sc.floor, sc.admission.running, sc.admission.queued), (2, 0, 0));
    assert!(sc.admission.limit >= 2, "the floor bounds the limit from the start");
    assert_eq!(
        warm.snapshot()[&wk(key)].service.version,
        spec(k).service.version,
        "for the pinned version"
    );

    sim.advance(Duration::from_secs(90));
    assert_eq!(
        warm.per_proc(&wk(key)),
        1,
        "kept through the idle TTL and the minimum state lifetime"
    );
    assert!(sim.scheduler().service_scaling(ids::service(1)).is_some());

    // pinned again with another version (an activation): the target follows it
    let mut v2 = spec(k);
    v2.service.version = rt_types::VersionNo(2);
    v2.service.digest = rt_types::Digest::of(b"v2");
    let key2 = key_of(&v2);
    assert_ne!(key, key2);
    assert_eq!(sim.scheduler().pin_service(&v2), Some(key2));
    sim.advance(ms(300));
    assert_eq!(warm.per_proc(&wk(key2)), 1, "the new version is warmed");
    assert_eq!(warm.per_proc(&wk(key)), 0, "and the old one is released");

    // an update that drops the floor: pinning with min_instances = 0 releases the service
    let none = ServiceKnobs { min_instances: 0, ..k };
    assert_eq!(
        sim.scheduler().pin_service(&spec(none)),
        Some(key),
        "still says what to load, if asked"
    );
    sim.advance(ms(300));
    assert_eq!(
        warm.per_proc(&wk(key)),
        0,
        "no floor and idle past the TTL: scale to zero"
    );
    assert_eq!(warm.per_proc(&wk(key2)), 0);

    // a service without a floor that was never run is not kept at all
    let mut cold = SimRuntime::new(fixed(2, 1));
    cold.scheduler().pin_service(&spec(none));
    cold.advance(ms(300));
    assert!(
        cold.scheduler().service_scaling(ids::service(1)).is_none(),
        "nothing to keep"
    );
}

/// SCH-33: an unused limit decays slowly: one step (1/8) per cooldown, never within the cooldown of
/// a change, and never below `start`.
#[test]
fn ac_sch_33_the_limit_scales_down_slowly_with_a_cooldown() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    sim.scripts().set_default(script![io(300)]);
    let mut tasks: Vec<SimTask> = (0..60)
        .map(|_| admitted(sim.submit_existing(adaptive(2).finish())))
        .collect();
    // sample the limit every tick: (time of a change, new limit)
    let mut changes: Vec<(Duration, u32)> = Vec::new();
    let mut last = limit_of(&sim, 1);
    let mut t = Duration::ZERO;
    for _ in 0..450 {
        sim.run_for(ms(100));
        t += ms(100);
        let now = limit_of(&sim, 1);
        if now != last {
            changes.push((t, now));
            last = now;
        }
    }
    sim.run_until_idle();
    for task in &mut tasks {
        assert_ok(sim.outcome(task));
    }
    let grown = changes.iter().map(|c| c.1).max().expect("the limit grew");
    assert!(grown >= 32, "grown under the queue: {changes:?}");
    // growth is fast (one change per tick), decay slow: successive decreases are a cooldown apart
    let mut prev = (Duration::ZERO, 0);
    let mut decays = 0;
    for &(at, limit) in &changes {
        if limit < prev.1 {
            decays += 1;
            assert!(
                at - prev.0 >= rt_sched::autoscale::SCALE_DOWN_COOLDOWN - ms(150),
                "a decrease {:?} after the previous change (limit {} -> {limit})",
                at - prev.0,
                prev.1
            );
            assert!(
                prev.1 - limit <= (prev.1 / 8).max(1),
                "one 1/8 step: {} -> {limit}",
                prev.1
            );
        }
        prev = (at, limit);
    }
    assert!(decays >= 2, "the unused limit decays over the idle time: {changes:?}");
    assert!(limit_of(&sim, 1) >= 2, "never below start");
    assert!(limit_of(&sim, 1) < grown, "and it did come down");
}

/// SCH-34: a fast-rising arrival rate grows the limit before a queue forms (cause `trend`); a trickle
/// of long requests that fills the limit just as much does not.
#[test]
fn ac_sch_34_a_rising_arrival_rate_grows_the_limit_before_a_queue_forms() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    sim.scripts().set_default(script![io(20_000)]);
    // 14 of 16 slots taken within one tick: 140 requests/s against none before
    let mut tasks: Vec<SimTask> = (0..14)
        .map(|_| admitted(sim.submit_existing(adaptive(16).finish())))
        .collect();
    assert_eq!(limit_of(&sim, 1), 16);
    sim.run_for(ms(250));
    let sc = scaling_of(&sim, 1);
    assert!(sc.admission.limit > 16, "grown early: {}", sc.admission.limit);
    assert_eq!(sc.admission.queued, 0, "no request had to wait");
    assert_eq!(sim.scheduler().queued().0, 0);
    let change = sc.last_change.expect("moved");
    assert_eq!((change.cause, change.from), (rt_sched::Cause::Trend, 16));
    assert!(sc.rps > 0.0, "the arrival rate is reported: {}", sc.rps);
    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }

    // 14 long requests, one per second: the limit is as full, but the rate is a trickle
    let mut sim = SimRuntime::new(fixed(2, 1));
    sim.scripts().set_default(script![io(25_000)]);
    let mut tasks: Vec<SimTask> = Vec::new();
    for _ in 0..14 {
        tasks.push(admitted(sim.submit_existing(adaptive(16).finish())));
        sim.run_for(Duration::from_secs(1));
    }
    assert_eq!(scaling_of(&sim, 1).admission.running, 14);
    assert_eq!(limit_of(&sim, 1), 16, "a trickle is not a trend");
    assert!(scaling_of(&sim, 1).last_change.is_none());
    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
}

/// SCH-30: memory pressure `High` requests an allocator purge; `Critical` puts the node in
/// `shed` mode, and it leaves `shed` once pressure is back below `High`. A mode the operator set
/// is not undone.
#[test]
fn ac_sch_30_critical_memory_pressure_sheds_automatically_with_hysteresis() {
    let mut sim = SimRuntime::new(fixed(1, 1));
    let budget = 1024 * MIB;
    let gov = sim.governor().clone();
    let purges = || gov.purge_request().epoch;
    let p0 = purges();

    let high = gov
        .reserve(rt_mem::MemCategory::Other, budget * 85 / 100, rt_mem::Owner::NONE)
        .expect("reserve");
    sim.advance(ms(300));
    assert!(purges() > p0, "High asks the Machines to purge their allocators");
    assert_eq!(
        sim.scheduler().mode(),
        DegradationMode::Normal,
        "High alone does not shed"
    );

    let critical = gov
        .reserve(rt_mem::MemCategory::Other, budget * 10 / 100, rt_mem::Owner::NONE)
        .expect("reserve");
    sim.advance(ms(300));
    assert_eq!(
        sim.scheduler().mode(),
        DegradationMode::Shed,
        "Critical: the node sheds"
    );
    drop(critical);
    sim.advance(ms(300));
    assert_eq!(
        sim.scheduler().mode(),
        DegradationMode::Shed,
        "still at High: no flapping"
    );
    drop(high);
    sim.advance(ms(300));
    assert_eq!(
        sim.scheduler().mode(),
        DegradationMode::Normal,
        "below High: back to normal"
    );

    sim.set_mode(DegradationMode::Shed);
    sim.advance(ms(600));
    assert_eq!(
        sim.scheduler().mode(),
        DegradationMode::Shed,
        "an operator's shed stays"
    );
}

// ---------------------------------------------------------------------------------------------
// SCH-26: queue timeout
// ---------------------------------------------------------------------------------------------

/// SCH-26: a task stays `Runnable` longer than `queue_timeout` -> `503 x-rt-reason:
/// queue_timeout`; the task never started.
#[test]
fn ac_sch_26_runnable_past_queue_timeout_is_shed_and_never_started() {
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let mut t = admitted(sim.submit_to_proc(p0, d(1).queue_timeout_ms(100).finish(), Script::noop()));

    sim.advance(ms(50));
    assert!(sim.outcome(&mut t).is_none(), "50 ms < queue_timeout: still waiting");
    sim.advance(ms(100));
    assert_rejected(sim.outcome(&mut t), RejectReason::QueueTimeout);
    assert_eq!(t.state(), TaskState::Done);

    sim.run_until_idle();
    assert_eq!(sim.scripts().started(), 0, "the task never started");
    assert_eq!(
        count_events(
            &sim.trace(),
            |e| matches!(e, TraceEvent::Start { task, .. } if *task == t.id)
        ),
        0
    );
    assert_eq!(sim.snapshot().inflight, 0);
}

#[test]
fn ac_sch_26_a_task_picked_in_time_is_not_shed() {
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let mut t = admitted(sim.submit_to_proc(p0, d(1).queue_timeout_ms(100).finish(), Script::noop()));
    sim.advance(ms(50));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut t));
}

/// The realistic form: a task stuck behind a hog on its P.
#[test]
fn ac_sch_26_a_task_stuck_behind_a_hog_is_shed() {
    let mut sim = sim(1, 1);
    let p0 = sim.procs_of(TaskKind::DENO)[0];
    let mut hog = admitted(sim.submit_to_proc(p0, d(1).finish(), script![cpu(1_000)]));
    sim.step_proc(p0); // the hog occupies the only Deno P for 1 s
    let mut victim = admitted(sim.submit_to_proc(p0, d(2).queue_timeout_ms(100).finish(), Script::noop()));

    sim.run_for(ms(300));
    assert_rejected(sim.outcome(&mut victim), RejectReason::QueueTimeout);
    assert!(!sim.scripts().start_order().contains(&victim.id));
    assert!(sim.outcome(&mut hog).is_none(), "the hog is still running");
}

// ---------------------------------------------------------------------------------------------
// SCH-27: node modes
// ---------------------------------------------------------------------------------------------

/// SCH-27: in `shed`, T1 hits are served and T3 misses get `503 x-rt-reason: shed`; in `drain`
/// every new request gets `503 draining`.
#[test]
fn ac_sch_27_shed_serves_t1_hits_and_rejects_misses_drain_rejects_all() {
    let mut sim = SimRuntime::new(fixed(1, 1).auto_ready(false));
    sim.source().make_ready(key_of(&d(1).finish())); // service 1 is a T1 hit; service 2 is a miss

    sim.set_mode(DegradationMode::Shed);
    let mut served = admitted(sim.submit(d(1).finish(), Script::noop()));
    let e = rejection(sim.submit(d(2).finish(), Script::noop()));
    assert_eq!((e, e.http_status(), e.reason()), (RejectReason::Shed, 503, "shed"));
    // Builds are never shed: they are what turns misses into hits again.
    let mut build = admitted(sim.submit(b(1000).finish(), Script::noop()));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut served));
    assert_ok(sim.outcome(&mut build));
    assert_eq!(
        sim.source().builds_started(),
        0,
        "a shed request never triggers a build"
    );

    sim.set_mode(DegradationMode::Drain);
    for service in [1, 2] {
        let e = rejection(sim.submit(d(service).finish(), Script::noop()));
        assert_eq!(
            (e, e.http_status(), e.reason()),
            (RejectReason::Draining, 503, "draining")
        );
    }

    sim.set_mode(DegradationMode::Normal);
    let miss = admitted(sim.submit(d(2).finish(), Script::noop()));
    sim.run_until_idle();
    assert_eq!(
        sim.source().builds_started(),
        1,
        "back to normal: the miss starts its build"
    );
    assert_eq!(sim.snapshot().waiting_runtime, 1);
    drop(miss);
}

#[test]
fn ac_sch_27_tasks_already_admitted_survive_a_mode_change() {
    let mut sim = SimRuntime::new(fixed(1, 1));
    let mut t = admitted(sim.submit(d(1).finish(), script![io(10)]));
    sim.set_mode(DegradationMode::Drain);
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut t));
}

/// The read-only autoscaler view (`Scheduler::service_scaling` / `services_scaling`, served by
/// `GET /v1/node/scheduler` and the service view): limit, queue, smoothed queue wait, demand and
/// latency baseline of a queueing service; nothing for a service that never ran.
#[test]
fn the_autoscaler_view_reports_queueing_and_what_was_learned() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    sim.scripts().set_default(script![io(1_000)]);
    assert!(
        sim.scheduler().service_scaling(ids::service(1)).is_none(),
        "no state before the first task"
    );
    assert!(sim.scheduler().services_scaling().is_empty());

    let mut tasks: Vec<SimTask> = (0..40)
        .map(|_| admitted(sim.submit_existing(adaptive(2).finish())))
        .collect();
    sim.run_for(ms(450)); // the limit grows while tasks wait (SCH-28)
    let v = sim
        .scheduler()
        .service_scaling(ids::service(1))
        .expect("state of a running service");
    assert_eq!(v.admission.service, ids::service(1));
    assert_eq!(v.kind, TaskKind::DENO);
    assert!(
        v.admission.limit > 2 && v.admission.running == v.admission.limit.min(40),
        "{v:?}"
    );
    assert_eq!(v.admission.queued, 40 - v.admission.running, "{v:?}");
    assert!(v.queue_wait > Duration::ZERO, "tasks waited for a slot: {v:?}");
    assert!(v.demand > 0.0, "{v:?}");
    assert_eq!(v.queue_timeouts, 0);
    assert_eq!(
        sim.scheduler().services_scaling(),
        vec![v],
        "one service, the same view"
    );

    sim.run_until_idle();
    for t in &mut tasks {
        assert_ok(sim.outcome(t));
    }
    let after = sim
        .scheduler()
        .service_scaling(ids::service(1))
        .expect("kept until its idle TTL");
    assert_eq!((after.admission.running, after.admission.queued), (0, 0));
    assert!(after.latency_baseline.is_some(), "a baseline was learned: {after:?}");
}

/// Review fixes 4, 6, 7: `min_instances` 1 on 2 Ps keeps one instance node-wide (not one per P);
/// releasing a floor brings the limit it held up back to `start`; reconciliation releases floors
/// of services that no longer hold one.
#[test]
fn a_released_or_stale_floor_resets_the_limit_and_is_reconciled() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    let mut k = ServiceKnobs {
        idle_ttl: Duration::from_secs(5),
        min_instances: 1,
        ..ServiceKnobs::default()
    };
    k.scaling = ScalingKnobs {
        start: 1,
        min: 1,
        warm_headroom_pct: 125,
        warm_max_per_proc: 4,
        ..ScalingKnobs::default()
    };
    let spec = |k: ServiceKnobs| SpecBuilder::deno().service(1).knobs(k).finish();
    let key = rt_sched::WarmKey {
        service: ids::service(1),
        key: key_of(&spec(k)),
    };
    let warm = sim.scheduler().warm_targets();

    sim.scheduler().pin_service(&spec(k));
    sim.advance(ms(300));
    assert_eq!(
        sim.scheduler().warm_instances().get(&ids::service(1)),
        Some(&1),
        "exactly min_instances node-wide"
    );
    assert_eq!(warm.instances(&key, 2), 1);

    // a floor of 6 holds the limit at 6; releasing it brings the limit back to `start`
    sim.scheduler()
        .pin_service(&spec(ServiceKnobs { min_instances: 6, ..k }));
    sim.advance(ms(300));
    assert_eq!(scaling_of(&sim, 1).admission.limit, 6);
    assert!(sim.scheduler().release_floor(ids::service(1)));
    let sc = scaling_of(&sim, 1);
    assert_eq!(
        (sc.floor, sc.admission.limit),
        (0, 1),
        "limit back to start, nothing runs"
    );
    let change = sc.last_change.expect("recorded");
    assert_eq!((change.from, change.to, change.cause), (6, 1, rt_sched::Cause::Bounds));

    // a stale floor (its release was missed) is released by reconciliation, a live one is kept
    sim.scheduler().pin_service(&spec(k));
    sim.advance(ms(300));
    let live = std::collections::HashSet::from([ids::service(1)]);
    assert_eq!(
        sim.scheduler().reconcile_floors(&live),
        0,
        "a live service keeps its floor"
    );
    assert_eq!(sim.scheduler().reconcile_floors(&std::collections::HashSet::new()), 1);
    assert_eq!(scaling_of(&sim, 1).floor, 0);
    sim.advance(Duration::from_secs(90));
    assert!(
        sim.scheduler().service_scaling(ids::service(1)).is_none(),
        "and the slot is retired"
    );
}

/// Releasing a floor races a request that brings new knobs: the release only clears the floor of
/// the metadata it read (compare-and-swap), so it never writes back knobs the request replaced.
#[test]
fn releasing_a_floor_never_loses_a_concurrent_knob_update() {
    let a = Arc::new(Admission::<u32>::new(limits(10_000)));
    let id = ids::service(1);
    let spec = |ceiling: u32| {
        let mut k = knobs(ceiling, 100, 0);
        k.min_instances = 2;
        k.scaling.min = 2;
        k.scaling.start = 2;
        task(1, 1, k)
    };
    let specs = [spec(6), spec(7)];
    a.seed(&req(&specs[0]));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let releaser = {
        let (a, stop) = (a.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                a.release_floor(id, 0);
            }
        })
    };
    let mut lost = 0;
    for i in 0..200_000usize {
        let want = [6, 7][i % 2];
        a.seed(&req(&specs[i % 2]));
        let got = a.service_scaling(id, 0).expect("state").ceiling;
        if got != want {
            lost += 1;
        }
    }
    stop.store(true, Ordering::Relaxed);
    releaser.join().expect("releaser");
    assert_eq!(lost, 0, "a release wrote back knobs a request had replaced");
}

fn call_at(tenant: u128, service: u128, k: ServiceKnobs, depth: u8) -> TaskSpec {
    SpecBuilder::deno()
        .tenant(tenant)
        .service(service)
        .knobs(k)
        .trigger(Trigger::Call {
            parent: TaskId(1),
            depth,
        })
        .finish()
}

/// ADM-003: a small node (`sched.max_inflight` of 2 to 7, where 1/8 rounds down to 0) still keeps
/// one run slot for calls, so a request holding every request slot can run its own call instead
/// of waiting for itself until its queue timeout.
#[test]
fn adm_003_a_small_node_still_reserves_a_slot_for_calls() {
    for max_inflight in 2..8u32 {
        let mut a = Adm::new(limits(max_inflight));
        let mut requests = 0;
        while runs(&a.admit(&task(1, 100 + u128::from(requests), knobs(BIG, 0, 0))).unwrap()) {
            requests += 1;
        }
        assert_eq!(
            requests,
            max_inflight - 1,
            "requests leave one slot (max_inflight {max_inflight})"
        );
        assert!(
            runs(&a.admit(&call(1, 999, knobs(BIG, 0, 0))).unwrap()),
            "the call runs (max_inflight {max_inflight})"
        );
    }
}

/// ADM-004: the tenant's call allowance is per call depth: with a plan cap of 2 and two requests
/// R → A → B in flight, the depth-1 calls (R → A) do not use up the allowance of the depth-2 calls
/// (A → B), which would otherwise wait for their own parents until the queue timeout.
#[test]
fn adm_004_a_call_chain_does_not_starve_on_the_call_allowance() {
    let mut a = Adm::new(limits(100));
    let k = knobs(BIG, 0, 2);
    assert!(runs(&a.admit(&task(1, 1, k)).unwrap()), "request 1 of R");
    assert!(
        runs(&a.admit(&task(1, 1, k)).unwrap()),
        "request 2 of R: the tenant is at its cap"
    );
    assert!(runs(&a.admit(&call_at(1, 2, k, 1)).unwrap()), "R1 calls A");
    assert!(runs(&a.admit(&call_at(1, 2, k, 1)).unwrap()), "R2 calls A");
    assert!(runs(&a.admit(&call_at(1, 3, k, 2)).unwrap()), "A1 calls B");
    assert!(runs(&a.admit(&call_at(1, 3, k, 2)).unwrap()), "A2 calls B");
    assert!(
        waits(&a.admit(&call_at(1, 3, k, 2)).unwrap()),
        "still at most `plan cap` calls per depth"
    );
    let waiting = a.next;
    // A depth-2 call that finishes hands its allowance to the waiting one.
    let first_b = waiting - 2;
    assert_eq!(a.finish(first_b), vec![waiting]);
}

/// ADM-008 (pinned, "its known body size"): a waiting HTTP task is charged its exact body length;
/// a streamed body of unknown length is charged only the fixed per-task overhead.
#[test]
fn adm_008_queued_bytes_count_the_known_body_only() {
    let base = req(&SpecBuilder::deno().http_get("/").finish()).bytes;
    let mut sized = rt_types::HttpRequest::get("/");
    sized.body = rt_types::Body::from(vec![0u8; 10_000]);
    let with_body = req(&SpecBuilder::deno().payload(rt_types::Payload::Http(sized)).finish()).bytes;
    assert_eq!(with_body, base + 10_000);
    let mut streamed = rt_types::HttpRequest::get("/");
    streamed.body = rt_types::Body::from_stream(futures_util::stream::empty());
    let streamed = req(&SpecBuilder::deno().payload(rt_types::Payload::Http(streamed)).finish()).bytes;
    assert_eq!(streamed, base, "unknown length: the overhead only");
}

/// SCHED-002: the executor panicking inside a task ends that task as `guest_error`, gives back its
/// run slot and memory, and the machine goes on running the next task.
#[test]
fn sched_002_an_executor_panic_releases_the_slot_and_the_memory() {
    let mut sim = SimRuntime::new(fixed(1, 1).memory_mb(256));
    let before = sim.governor().committed();
    let mut bad = admitted(sim.submit(
        d(1).finish(),
        rt_testkit::Script::from(vec![rt_testkit::alloc(8), rt_testkit::crash()]),
    ));
    sim.run_until_idle();
    assert!(
        matches!(sim.outcome(&mut bad), Some(Outcome::GuestError)),
        "{:?}",
        sim.outcome(&mut bad)
    );
    assert_eq!(sim.scheduler().snapshot().inflight, 0, "the run slot is back");
    assert_eq!(sim.governor().committed(), before, "the memory reservation is back");
    let mut next = admitted(sim.submit(d(1).finish(), script![cpu(1)]));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut next));
}

/// A service with one instance calls itself (the child marked `reentrant` or not) while the
/// parent stays busy waiting on it longer than the queue timeout; returns the child's outcome.
fn self_call_at_one_instance(reentrant: bool) -> Outcome {
    let mut sim = SimRuntime::new(fixed(1, 1));
    let sched = sim.scheduler().clone();
    let children: Arc<Mutex<Vec<rt_sched::TaskHandle>>> = Arc::default();
    let sink = children.clone();
    sim.scripts().set_call_hook(move |parent, _slug| {
        let mut spec = d(1)
            .max_concurrency(1, 10)
            .queue_timeout_ms(500)
            .trigger(Trigger::Call { parent, depth: 1 })
            .finish();
        spec.reentrant = reentrant;
        // scripts are per service: the child runs the same script, so only the parent's call counts
        let mut kids = sink.lock().unwrap();
        if kids.is_empty() {
            if let Ok(h) = sched.submit(spec) {
                kids.push(h);
            }
        }
    });
    let mut parent = admitted(sim.submit(
        d(1).max_concurrency(1, 10).queue_timeout_ms(500).finish(),
        rt_testkit::Script::from(vec![rt_testkit::call("self"), io(2_000)]),
    ));
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut parent));
    assert_eq!(sim.scheduler().snapshot().inflight, 0, "every run slot is back");
    let mut kids = children.lock().unwrap();
    assert_eq!(kids.len(), 1);
    futures_util::FutureExt::now_or_never(&mut kids[0]).expect("the child finished")
}

/// ADM-005: a service with one instance calling itself: the child is re-entrant (its parent holds
/// the only slot and waits for it), so it runs past `max_instances` instead of waiting its queue
/// timeout.
#[test]
fn adm_005_a_self_call_at_one_instance_runs_beside_its_parent() {
    let child = self_call_at_one_instance(true);
    assert!(matches!(child, Outcome::Ok(_)), "{child:?}");
}

/// ADM-005b (pinned limitation): a nested call not marked re-entrant (an indirect cycle, A calls B
/// calls A) still needs the slot an ancestor holds: it ends `queue_timeout`; nothing hangs.
#[test]
fn adm_005b_a_nested_call_that_is_not_reentrant_still_ends_in_queue_timeout() {
    let child = self_call_at_one_instance(false);
    assert!(
        matches!(child, Outcome::Rejected(RejectReason::QueueTimeout)),
        "{child:?}"
    );
}

/// FAIR-001: a tenant flooding the node cannot starve another: its own requests past its share
/// are refused (`overloaded`) while the other tenant's are admitted and all finish.
#[test]
fn fair_001_a_noisy_tenant_cannot_starve_another() {
    let mut sim = SimRuntime::new(fixed(1, 1).sched(|s| {
        s.max_inflight = 8;
        s.max_queued = 200;
    }));
    sim.scripts().set_default(script![io(50)]);
    let mut noisy_refused = 0;
    let mut noisy = Vec::new();
    for _ in 0..2_000 {
        match sim.submit_existing(d(1).tenant(1).queue_timeout_ms(600_000).finish()) {
            Ok(t) => noisy.push(t),
            Err(_) => noisy_refused += 1,
        }
    }
    assert!(noisy_refused > 0, "the flood hits its share of the queue");
    let quiet: Vec<SimTask> = (0..20)
        .map(|_| admitted(sim.submit_existing(d(2).tenant(2).queue_timeout_ms(600_000).finish())))
        .collect();
    sim.run_until_idle();
    for mut t in quiet {
        assert_ok(sim.outcome(&mut t));
    }
}

/// Slow-path cost of the admission waitlist (run in release with `--ignored --nocapture`):
/// 10 k items wait over 1 k tenants x 2 services behind one node slot, then are released one at
/// a time (each release grants the next waiter). Prints ns/op p50/p99 for the enqueue and for
/// release+dispatch; asserts a generous mean of at most 5 us per op. Public API only, so it
/// runs unchanged before and after the fair-order change.
#[test]
#[ignore = "perf"]
fn perf_admission_slow_path_10k_waiting_over_1k_tenants() {
    const TENANTS: u128 = 1_000;
    const N: usize = 10_000;
    let a = Admission::<usize>::new(AdmissionLimits {
        max_inflight: 1,
        max_queued: 1 << 20,
        max_queued_bytes: 1 << 40,
    });
    let specs: Vec<TaskSpec> = (0..TENANTS)
        .flat_map(|t| (0..2u128).map(move |s| task(t + 1, (t + 1) * 10 + s, knobs(1 << 10, 1 << 10, 1 << 10))))
        .collect();
    let reqs: Vec<AdmitRequest> = specs.iter().map(req).collect();
    let mut tickets: Vec<Option<AdmissionTicket>> = vec![None; N];
    let mut enqueue = Vec::with_capacity(N);
    let mut running = None;
    for i in 0..N {
        let r = &reqs[i % reqs.len()];
        let mut ticket = None;
        let t0 = Instant::now();
        let x = a
            .admit(r, NORMAL, |t| {
                ticket = Some(t.clone());
                i
            })
            .expect("admitted");
        enqueue.push(t0.elapsed().as_nanos() as u64);
        tickets[i] = ticket;
        if x.run.is_some() {
            running = Some(i);
        }
    }
    assert_eq!(a.queued().0 as usize, N - 1, "all but one wait");
    let mut release = Vec::with_capacity(N);
    let mut done = 0;
    while let Some(i) = running.take() {
        let mut t = tickets[i].take().expect("ticket of the running item");
        t.queued = false;
        let t0 = Instant::now();
        let granted = a.release(&t, 0, None);
        release.push(t0.elapsed().as_nanos() as u64);
        done += 1;
        running = granted.first().copied();
    }
    assert_eq!(done, N, "every item ran");
    let report = |label: &str, v: &mut Vec<u64>| -> f64 {
        v.sort_unstable();
        let mean = v.iter().sum::<u64>() as f64 / v.len() as f64;
        println!(
            "{label}: mean {mean:.0} ns/op, p50 {} ns, p99 {} ns ({} ops)",
            v[v.len() / 2],
            v[v.len() * 99 / 100],
            v.len()
        );
        mean
    };
    let e = report("enqueue", &mut enqueue);
    let r = report("release+dispatch", &mut release);
    assert!(
        e <= 5_000.0 && r <= 5_000.0,
        "mean over 5 us/op: enqueue {e:.0}, release {r:.0}"
    );
}

// ---------------------------------------------------------------------------------------------
// Fair order in the simulation (feature 2): the estimator learns from finished tasks
// ---------------------------------------------------------------------------------------------

/// One node slot, a tenant with a short and a long service, generous queue bounds.
fn one_slot() -> SimRuntime {
    SimRuntime::new(fixed(1, 1).sched(|s| {
        s.max_inflight = 1;
        s.max_queued = 20_000;
    }))
}

/// Run one task of each spec alone so the estimator knows their run times.
fn warm_up(sim: &mut SimRuntime, specs: &[&dyn Fn() -> TaskSpec]) {
    for s in specs {
        let mut t = admitted(sim.submit_existing(s()));
        sim.run_until_idle();
        assert_ok(sim.outcome(&mut t));
    }
}

/// AC-SCH-40: under contention a tenant's short job starts before its long job that queued
/// first, once finished tasks taught the estimator their run times.
#[test]
fn ac_sch_40_short_jobs_start_first_under_contention() {
    let mut sim = one_slot();
    sim.scripts().set_for_service(ids::service(1), script![io(5)]);
    sim.scripts().set_for_service(ids::service(2), script![io(2000)]);
    sim.scripts().set_for_service(ids::service(3), script![io(100)]);
    let short = || d(1).tenant(1).queue_timeout_ms(30_000).finish();
    let long = || d(2).tenant(1).queue_timeout_ms(30_000).finish();
    warm_up(&mut sim, &[&short, &long, &short, &long]);

    let mut blocker = admitted(sim.submit_existing(d(3).tenant(1).queue_timeout_ms(30_000).finish()));
    let mut l = admitted(sim.submit_existing(long()));
    let mut s = admitted(sim.submit_existing(short()));
    sim.run_until_idle();
    for t in [&mut blocker, &mut l, &mut s] {
        assert_ok(sim.outcome(t));
    }
    let (ql, qs) = (l.timings().queue, s.timings().queue);
    assert!(qs < ql, "the short job waited {qs:?}, the long one {ql:?}");
    assert!(ql >= ms(100), "the long job waited for the short one: {ql:?}");
}

/// AC-SCH-41: a long job is overtaken only by short arrivals within half its queue timeout: it
/// starts by `submit + Q/2 + one short job` under a steady stream of short jobs, and nothing
/// times out.
#[test]
fn ac_sch_41_a_long_job_starts_within_half_the_queue_timeout_under_a_stream_of_short_jobs() {
    let q = 5_000;
    let mut sim = one_slot();
    sim.scripts().set_for_service(ids::service(1), script![io(5)]);
    sim.scripts().set_for_service(ids::service(2), script![io(2000)]);
    let short = || d(1).tenant(1).queue_timeout_ms(q).finish();
    let long = || d(2).tenant(1).queue_timeout_ms(q).finish();
    warm_up(&mut sim, &[&short, &long, &short, &long]);

    let mut shorts = Vec::new();
    for _ in 0..4 {
        shorts.push(admitted(sim.submit_existing(short())));
    }
    let mut l = admitted(sim.submit_existing(long()));
    // a short job every 5 ms for 3 s: the slot never goes idle
    for _ in 0..600 {
        shorts.push(admitted(sim.submit_existing(short())));
        sim.run_for(ms(5));
    }
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut l));
    let started = l.timings().queue;
    assert!(
        started <= ms(q / 2 + 5 + 20),
        "the long job started {started:?} after it was submitted"
    );
    assert!(
        started >= ms(q / 2 - 100),
        "short arrivals inside Q/2 went first: {started:?}"
    );
    for mut t in shorts {
        assert_ok(sim.outcome(&mut t));
    }
}

/// AC-SCH-42: two busy tenants get equal slot time, not equal task counts (5 ms vs 500 ms jobs).
#[test]
fn ac_sch_42_two_busy_tenants_get_equal_slot_time() {
    let mut sim = one_slot();
    sim.scripts().set_for_service(ids::service(1), script![io(5)]);
    sim.scripts().set_for_service(ids::service(2), script![io(500)]);
    let a = || d(1).tenant(1).queue_timeout_ms(600_000).finish();
    let b = || d(2).tenant(2).queue_timeout_ms(600_000).finish();
    warm_up(&mut sim, &[&a, &b]);
    let mut ta: Vec<SimTask> = (0..4_000).map(|_| admitted(sim.submit_existing(a()))).collect();
    let mut tb: Vec<SimTask> = (0..40).map(|_| admitted(sim.submit_existing(b()))).collect();
    sim.run_for(Duration::from_secs(10));
    let done =
        |sim: &mut SimRuntime, v: &mut Vec<SimTask>| v.iter_mut().filter_map(|t| sim.outcome(t).map(|_| ())).count();
    let slot_a = ms(5) * done(&mut sim, &mut ta) as u32;
    let slot_b = ms(500) * done(&mut sim, &mut tb) as u32;
    let diff = slot_a.abs_diff(slot_b);
    assert!(
        diff <= TENANT_QUANTUM + ms(500) + ms(100),
        "slot time A {slot_a:?} vs B {slot_b:?}"
    );
    assert!(slot_a > Duration::from_secs(3) && slot_b > Duration::from_secs(3));
    sim.run_until_idle();
}

/// AC-SCH-43: granted waiters land in the wait-by-size histogram and aged grants are counted
/// on the node registry.
#[test]
fn ac_sch_43_the_wait_by_size_histogram_and_aged_grants_are_reported() {
    use rt_obs::{Metrics, MetricsOptions};
    use rt_sched::{Scheduler, SimClock, UnitSource};
    use rt_testkit::{
        MockBuildExecutor, MockDenoExecutor, MockPreparedSource, MockWasmExecutor, ScriptBook, TestGovernor,
        VirtualClock,
    };

    let metrics = Metrics::new(MetricsOptions::default()).expect("metrics");
    let clock = Arc::new(VirtualClock::new());
    let book = ScriptBook::new();
    book.set_default(script![io(5)]);
    let source = MockPreparedSource::new();
    source.set_auto_ready(true);
    let mut cfg = fixed(1, 1).sched;
    cfg.max_inflight = 1;
    cfg.aging_factor = 0;
    cfg.max_queued = 100;
    let (b1, b2, b3) = (book.clone(), book.clone(), book.clone());
    let mut sim = Scheduler::builder(cfg, clock.clone(), TestGovernor::with_mb(4096))
        .executor(move || MockDenoExecutor::new(b1.clone()), source.clone(), 0)
        .executor(move || MockWasmExecutor::new(b2.clone()), source.clone(), 0)
        .executor(move || MockBuildExecutor::new(b3.clone()), Arc::new(UnitSource), 0)
        .metrics(metrics.clone())
        .build_sim(clock.clone() as Arc<dyn SimClock>)
        .expect("sim");
    // one task alone teaches the estimator a 5 ms run time (before that the prior is the wall limit)
    let first = sim.scheduler().submit(d(1).tenant(1).queue_timeout_ms(30_000).finish());
    sim.run_until_idle(100_000);
    drop(first);
    let handles: Vec<_> = (0..5)
        .map(|_| {
            sim.scheduler()
                .submit(d(1).tenant(1).queue_timeout_ms(30_000).finish())
                .expect("admitted")
        })
        .collect();
    sim.run_until_idle(100_000);
    sim.advance(Duration::from_secs(2));
    drop(handles);
    let text = metrics.render();
    let count = text
        .lines()
        .find(|l| l.starts_with("rt_admission_wait_by_size_seconds_count{size=\"short\"}"))
        .unwrap_or_else(|| panic!("no short series:\n{text}"));
    assert!(count.ends_with(" 4"), "four waiters were granted: {count}");
    let aged = text
        .lines()
        .find(|l| l.starts_with("rt_admission_aged_grants_total "))
        .unwrap_or_else(|| panic!("no aged grants:\n{text}"));
    assert!(
        aged.ends_with(" 4"),
        "aging 0: every grant is at or after its key: {aged}"
    );
}

/// PRED-002 end to end: a wall kill in the scheduler feeds the estimator about the wall limit
/// (a censored sample), so the service is not predicted as fast.
#[test]
fn pred_003_a_wall_kill_in_the_scheduler_feeds_the_estimator() {
    let mut sim = sim(1, 1);
    sim.scripts().set_for_service(ids::service(1), script![io(5_000)]);
    let mut t = admitted(sim.submit_existing(d(1).wall_limit(200).finish()));
    sim.run_until_idle();
    assert_killed(sim.outcome(&mut t), rt_types::KillReason::WallTimeout);
    let pred = scaling_of(&sim, 1).pred[0].expect("a sample was taken");
    assert!(pred >= ms(150) && pred <= ms(300), "{pred:?}");
}
