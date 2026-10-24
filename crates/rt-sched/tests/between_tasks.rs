//! `Executor::between_tasks` (B4.1): bounded housekeeping between two tasks, so deferred executor
//! work (Deno isolate pool refill and disposal) keeps up on a Machine that never parks under load,
//! where `on_idle` never runs.
//!
//! Real-thread scheduler, one Deno P, an executor that records what the Machine asked of it.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use parking_lot::Mutex;
use rt_sched::{
    ExecCx, Executor, Interrupt, Interrupter, Scheduler, TaskFuture, UnitSource, BETWEEN_TASKS_EVERY,
    BETWEEN_TASKS_EVERY_BUSY,
};
use rt_testkit::TestGovernor;
use rt_types::{Outcome, Reply, TaskId, TaskKind, TaskSpec};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    Start,
    Between,
    Idle,
}

type Log = Arc<Mutex<Vec<Ev>>>;

struct NoInterrupts;

impl Interrupter for NoInterrupts {
    fn interrupt(&self, _task: TaskId, _what: Interrupt) {}
}

/// Records `start` / `between_tasks` / `on_idle` in call order; every task finishes on its first poll.
struct Recording {
    log: Log,
}

impl Executor for Recording {
    const KIND: TaskKind = TaskKind::DENO;
    type Prepared = ();

    fn start(&mut self, _spec: TaskSpec, _prepared: Arc<()>, _cx: ExecCx) -> TaskFuture {
        self.log.lock().push(Ev::Start);
        Box::pin(async { Outcome::Ok(Reply::ok()) })
    }

    fn interrupter(&self) -> Arc<dyn Interrupter> {
        Arc::new(NoInterrupts)
    }

    fn on_idle(&mut self, _budget: Duration) {
        self.log.lock().push(Ev::Idle);
    }

    fn between_tasks(&mut self, _budget: Duration) {
        self.log.lock().push(Ev::Between);
    }
}

fn one_deno_p() -> (Scheduler, Log) {
    let log: Log = Arc::default();
    let l = log.clone();
    let cfg = rt_sched::SchedConfig {
        build_processors: 0,
        spare_machines: 0,
        ..real_cfg(1, 0)
    };
    let sched = Scheduler::builder(cfg, Arc::new(rt_sched::SystemClock::new()), TestGovernor::with_mb(1024))
        .executor(move || Recording { log: l.clone() }, Arc::new(UnitSource), 0)
        .build()
        .expect("real scheduler builds");
    (sched, log)
}

fn task() -> TaskSpec {
    d(1).http_get("/").finish()
}

fn starts(log: &Log) -> usize {
    log.lock().iter().filter(|e| **e == Ev::Start).count()
}

#[test]
fn a_machine_that_runs_out_of_work_housekeeps_before_it_spins_and_parks() {
    let _g = serial();
    // The executor is created with the Machine's first task, so the log starts there.
    let (sched, log) = one_deno_p();
    for i in 0..5 {
        let outcome = block_on(admitted(sched.submit(task())));
        assert!(matches!(outcome, Outcome::Ok(_)), "task {i}: {outcome:?}");
        assert!(
            wait_until(ms(2_000), || log.lock().ends_with(&[Ev::Idle])),
            "task {i}: the Machine parks again"
        );
        let events = std::mem::take(&mut *log.lock());
        // Start, then (the Machine finds nothing) between_tasks, and only then the park.
        // (A spurious wake-up before the task may add an earlier Between/Idle pair.)
        let start = events.iter().position(|e| *e == Ev::Start).expect("the task started");
        let between = events
            .iter()
            .skip(start)
            .position(|e| *e == Ev::Between)
            .map(|b| b + start);
        let idle = events.iter().rposition(|e| *e == Ev::Idle).expect("parked");
        assert!(
            between.is_some_and(|b| start < b && b < idle),
            "task {i}: expected start < between_tasks < on_idle, got {events:?}"
        );
    }
    block_on(sched.shutdown(ms(500)));
}

/// Review 10: while tasks wait in the Machine's own queue, a housekeeping unit (a Deno isolate
/// create takes milliseconds) would delay them: it then runs only every
/// `BETWEEN_TASKS_EVERY_BUSY` picks, enough to keep deferred work from starving.
#[test]
fn a_machine_that_always_finds_work_still_housekeeps_but_rarely_while_tasks_wait() {
    let _g = serial();
    let (sched, log) = one_deno_p();
    // Queue far more tasks than one housekeeping period before the Machine can drain them.
    let n = 4 * BETWEEN_TASKS_EVERY_BUSY as usize;
    let handles: Vec<_> = (0..n).map(|_| admitted(sched.submit(task()))).collect();
    for h in handles {
        assert!(matches!(block_on(h), Outcome::Ok(_)));
    }
    assert!(wait_until(ms(2_000), || starts(&log) == n));

    // However the picks interleave with submissions, no run of starts is longer than the period.
    let events = log.lock().clone();
    let mut run = 0usize;
    let mut longest = 0usize;
    for e in &events {
        match e {
            Ev::Start => {
                run += 1;
                longest = longest.max(run);
            }
            Ev::Between | Ev::Idle => run = 0,
        }
    }
    assert!(
        longest <= BETWEEN_TASKS_EVERY_BUSY as usize,
        "{longest} tasks started back to back without housekeeping (limit {BETWEEN_TASKS_EVERY_BUSY})"
    );
    assert!(
        longest > BETWEEN_TASKS_EVERY as usize,
        "with tasks waiting it does not housekeep every {BETWEEN_TASKS_EVERY} picks"
    );
    // Runs of at most the period between housekeeping calls; the call after the last run may not
    // have happened yet when the log is read.
    let between = events.iter().filter(|e| **e == Ev::Between).count();
    assert!(
        between + 1 >= n / BETWEEN_TASKS_EVERY_BUSY as usize,
        "between_tasks ran {between} times for {n} tasks"
    );
    block_on(sched.shutdown(ms(500)));
}
