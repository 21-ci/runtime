//! CPU limits use the task's own thread CPU time, never wall time (SCH-12/13): on a loaded node a
//! poll can last far longer than the CPU it burns (the thread waits for a core, or blocks), and
//! the tasks interleaved on one Machine burn CPU of their own. Real threads, one Deno P, an
//! executor whose tasks block, spin or wait as scripted.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use common::*;
use parking_lot::Mutex;
use rt_sched::{ExecCx, Executor, Interrupt, Interrupter, Scheduler, TaskFuture, UnitSource};
use rt_testkit::TestGovernor;
use rt_types::{KillReason, Outcome, Reply, TaskId, TaskKind, TaskSpec};

/// What a task of a service does in its first poll; the second poll finishes it (or reports the
/// kill the scheduler asked for meanwhile).
#[derive(Clone, Copy)]
enum Act {
    /// Block the thread without using CPU (wall time only).
    Block(Duration),
    /// Burn CPU.
    Spin(Duration),
    /// Return `Pending` and be woken from another thread after this long.
    Wait(Duration),
}

type Kills = Arc<Mutex<Vec<(TaskId, KillReason)>>>;

struct Recorder(Kills);

impl Interrupter for Recorder {
    fn interrupt(&self, task: TaskId, what: Interrupt) {
        if let Interrupt::Kill(r) = what {
            self.0.lock().push((task, r));
        }
    }
}

struct Scripted {
    acts: Arc<Mutex<Vec<(u128, Act)>>>,
    kills: Kills,
}

struct Run {
    task: TaskId,
    act: Act,
    step: u8,
    kills: Kills,
}

impl Future for Run {
    type Output = Outcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome> {
        if self.step == 0 {
            self.step = 1;
            match self.act {
                Act::Block(d) => std::thread::sleep(d),
                Act::Spin(d) => {
                    let start = rt_sched::clock::thread_cpu_time_now();
                    while rt_sched::clock::thread_cpu_time_now().saturating_sub(start) < d {
                        std::hint::spin_loop();
                    }
                }
                Act::Wait(d) => {
                    let waker = cx.waker().clone();
                    std::thread::spawn(move || {
                        std::thread::sleep(d);
                        waker.wake();
                    });
                    return Poll::Pending;
                }
            }
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let killed = self.kills.lock().iter().find(|(t, _)| *t == self.task).map(|(_, r)| *r);
        Poll::Ready(match killed {
            Some(r) => Outcome::Killed(r),
            None => Outcome::Ok(Reply::ok()),
        })
    }
}

impl Executor for Scripted {
    const KIND: TaskKind = TaskKind::DENO;
    type Prepared = ();

    fn start(&mut self, spec: TaskSpec, _prepared: Arc<()>, cx: ExecCx) -> TaskFuture {
        let service = u128::from(spec.service.service.ulid());
        let act = self
            .acts
            .lock()
            .iter()
            .find(|(s, _)| *s == service)
            .map(|(_, a)| *a)
            .expect("scripted service");
        Box::pin(Run {
            task: cx.task,
            act,
            step: 0,
            kills: self.kills.clone(),
        })
    }

    fn interrupter(&self) -> Arc<dyn Interrupter> {
        Arc::new(Recorder(self.kills.clone()))
    }

    fn on_idle(&mut self, _budget: Duration) {}
}

fn one_machine(acts: Vec<(u128, Act)>) -> Scheduler {
    let acts = Arc::new(Mutex::new(acts));
    let kills: Kills = Arc::default();
    let cfg = rt_sched::SchedConfig {
        build_processors: 0,
        spare_machines: 0,
        ..real_cfg(1, 0)
    };
    Scheduler::builder(cfg, Arc::new(rt_sched::SystemClock::new()), TestGovernor::with_mb(1024))
        .executor(
            move || Scripted {
                acts: acts.clone(),
                kills: kills.clone(),
            },
            Arc::new(UnitSource),
            0,
        )
        .build()
        .expect("real scheduler builds")
}

fn spec(service: u128, cpu_ms: u64) -> TaskSpec {
    let mut s = d(service).http_get("/").finish();
    s.limits.cpu = Duration::from_millis(cpu_ms);
    s
}

/// A poll that lasts 300 ms of wall time but uses no CPU (the thread blocks, as it would waiting
/// for a core on a saturated node) is not a CPU-limit kill, with a 50 ms CPU budget.
#[test]
fn a_long_poll_in_wall_time_is_not_charged_as_cpu() {
    let _g = serial();
    let sched = one_machine(vec![(1, Act::Block(ms(300)))]);
    let started = Instant::now();
    let out = block_on(admitted(sched.submit(spec(1, 50))));
    assert!(started.elapsed() >= ms(300));
    assert!(matches!(out, Outcome::Ok(_)), "wall time is not CPU time: {out:?}");
    block_on(sched.shutdown(ms(500)));
}

/// The control: a poll that really burns 300 ms of CPU against a 50 ms budget is killed.
#[test]
fn a_long_poll_in_cpu_time_is_a_cpu_limit_kill() {
    let _g = serial();
    let sched = one_machine(vec![(1, Act::Spin(ms(300)))]);
    let out = block_on(admitted(sched.submit(spec(1, 50))));
    assert!(matches!(out, Outcome::Killed(KillReason::CpuLimit)), "{out:?}");
    block_on(sched.shutdown(ms(500)));
}

/// CPU another task burns on the same Machine while this one waits is not charged to it.
#[test]
fn another_tasks_cpu_on_the_same_machine_is_not_charged() {
    let _g = serial();
    let sched = one_machine(vec![(1, Act::Wait(ms(400))), (2, Act::Spin(ms(250)))]);
    let waiter = admitted(sched.submit(spec(1, 50)));
    std::thread::sleep(ms(50));
    let burner = admitted(sched.submit(spec(2, 5_000)));
    let (w, b) = block_on(async { (waiter.await, burner.await) });
    assert!(matches!(b, Outcome::Ok(_)), "{b:?}");
    assert!(
        matches!(w, Outcome::Ok(_)),
        "the waiter is charged only its own polls: {w:?}"
    );
    block_on(sched.shutdown(ms(500)));
}
