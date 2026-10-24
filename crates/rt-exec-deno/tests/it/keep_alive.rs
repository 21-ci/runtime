//! Per-service `min_run_time` keep-alive of reused isolates (plan-scheduling 2.2-2.4): MRT-DNO-01..09.
//!
//! With reuse on, a cleanly finished isolate is parked until its own expiry: the service's
//! `min_run_time` after the answer, else the node's `deno.reuse_idle_ttl` (inherit). `0` disposes
//! it after the answer, unless tasks of the service wait in admission (a handoff: kept while they
//! wait) or the service is below its warm floor. The keep-alive clock starts when the driver ends
//! (body sent, `waitUntil` settled), not at the response head.

use crate::common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use rt_exec_deno::{DenoExecutorConfig, DenoPrepared};
use rt_mem::{MemCategory, Owner};
use rt_obs::Metrics;
use rt_sched::{WarmKey, WarmTarget, WarmTargets};
use rt_testkit::SpecBuilder;
use rt_types::{KillReason, Outcome, ServiceKnobs};

/// `n` counts the requests this isolate served (module state).
const SRC: &str = r#"
export default async (req: Request, ctx: { waitUntil(p: Promise<unknown>): void }) => {
  const g = globalThis as any;
  g.n = (g.n ?? 0) + 1;
  const n = g.n;
  switch (new URL(req.url).pathname) {
    case "/throw":
      throw new Error("boom");
    case "/cpu":
      for (;;) {}
    case "/stream":
      return new Response(new ReadableStream({
        async start(c) {
          for (let i = 0; i < 3; i++) {
            await new Promise((r) => setTimeout(r, 80));
            c.enqueue(new TextEncoder().encode("x"));
          }
          c.close();
        },
      }));
    case "/wait":
      ctx.waitUntil(new Promise((r) => setTimeout(r, 200)));
      break;
  }
  return Response.json({ n });
};
"#;

fn knobs(min_run_time: Option<Duration>) -> ServiceKnobs {
    ServiceKnobs {
        reuse_isolates: true,
        min_run_time,
        queue_timeout: Duration::from_secs(5),
        ..ServiceKnobs::default()
    }
}

fn svc_spec(min_run_time: Option<Duration>) -> SpecBuilder {
    spec().knobs(knobs(min_run_time))
}

fn ms(n: u64) -> Option<Duration> {
    Some(Duration::from_millis(n))
}

struct Ka {
    h: Harness,
    p: Arc<DenoPrepared>,
    targets: Arc<WarmTargets>,
    backlog: Arc<TestBacklog>,
    metrics: Arc<Metrics>,
}

async fn ka(ttl: Duration) -> Ka {
    let targets = WarmTargets::new();
    let backlog = TestBacklog::on(&targets);
    let cfg = DenoExecutorConfig {
        reuse_idle_ttl: ttl,
        ..cfg0()
    }
    .with_warm_targets(targets.clone());
    let metrics = new_metrics();
    let h = Harness::with_metrics(cfg, metrics.clone());
    let p = h.prepare(&ts_upload(SRC)).await;
    Ka {
        h,
        p,
        targets,
        backlog,
        metrics,
    }
}

impl Ka {
    async fn n(&mut self, mrt: Option<Duration>) -> u64 {
        let ran = self.h.run(svc_spec(mrt).http_get("/").finish(), &self.p).await;
        let n = read(ran.outcome).await.json()["n"].as_u64().expect("n");
        self.h.settle(Duration::from_millis(20)).await;
        n
    }

    fn kept(&self) -> usize {
        self.h.exec.warm_len()
    }

    /// Run the Machine's housekeeping after `d`.
    async fn after(&mut self, d: Duration) {
        self.h.settle(d).await;
        self.h.exec.between_tasks(Duration::from_millis(50));
    }

    fn count(&self, outcome: &str) -> f64 {
        metric(
            &self.metrics.render(),
            "rt_instance_keepalive_total",
            &format!("outcome=\"{outcome}\""),
        )
    }

    fn publish_floor(&self, per_proc: u32) {
        let service = svc_spec(None).finish().service;
        self.targets.publish(HashMap::from([(
            WarmKey {
                service: service.service,
                key: self.p.key,
            },
            WarmTarget { service, per_proc },
        )]));
    }
}

/// Unset `min_run_time` inherits the node's `reuse_idle_ttl`: reused inside it, expired after it.
#[test]
fn mrt_dno_01_unset_keeps_the_node_idle_ttl() {
    run_local(async {
        let mut k = ka(Duration::from_millis(150)).await;
        assert_eq!(k.n(None).await, 1);
        assert_eq!(k.kept(), 1);
        assert_eq!(k.n(None).await, 2, "reused inside the node idle TTL");
        k.after(Duration::from_millis(250)).await;
        assert_eq!(k.kept(), 0, "expired after the node idle TTL");
        assert_eq!(k.n(None).await, 1);
        assert_eq!(k.count("reused"), 1.0);
        assert!(k.count("kept") >= 2.0);
        assert_eq!(k.count("expired"), 1.0);
    });
}

/// A positive `min_run_time` replaces the node idle TTL for the service's isolates.
#[test]
fn mrt_dno_02_a_kept_isolate_expires_after_the_service_min_run_time() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        assert_eq!(k.n(ms(150)).await, 1);
        k.after(Duration::from_millis(30)).await;
        assert_eq!(k.kept(), 1, "inside min_run_time");
        assert_eq!(k.n(ms(150)).await, 2);
        k.after(Duration::from_millis(250)).await;
        assert_eq!(k.kept(), 0, "min_run_time passed, although the node TTL did not");
        // An expired isolate is not taken even before housekeeping runs.
        assert_eq!(k.n(ms(100)).await, 1);
        k.h.settle(Duration::from_millis(150)).await;
        assert_eq!(k.n(ms(100)).await, 1, "take refuses an expired isolate");
    });
}

/// `min_run_time = 0` with nothing waiting disposes the isolate after the answer.
#[test]
fn mrt_dno_03_zero_disposes_after_the_answer_when_nothing_waits() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        for _ in 0..3 {
            assert_eq!(k.n(ms(0)).await, 1, "the next request sees fresh module state");
            assert_eq!(k.kept(), 0);
        }
        assert_eq!(k.count("disposed"), 3.0);
    });
}

/// `min_run_time = 0` keeps the isolate while tasks of the service wait in admission, and drops
/// it at the first housekeeping that sees none.
#[test]
fn mrt_dno_04_zero_keeps_the_isolate_while_work_of_the_service_waits() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        k.backlog.set(1);
        let mut reused = 0;
        for _ in 0..5 {
            if k.n(ms(0)).await > 1 {
                reused += 1;
            }
            k.h.exec.between_tasks(Duration::from_millis(50));
        }
        assert!(reused >= 1, "reused at least once over 5 queued tasks");
        assert_eq!(k.kept(), 1, "kept while work waits");
        assert!(k.count("handoff") >= 1.0);
        k.backlog.set(0);
        k.after(Duration::ZERO).await;
        assert_eq!(k.kept(), 0, "the queue emptied: the handoff ends");
    });
}

/// The warm floor (`min_instances` target) beats `min_run_time = 0`.
#[test]
fn mrt_dno_05_zero_keeps_the_warm_floor() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        k.publish_floor(1);
        assert_eq!(k.n(ms(0)).await, 1);
        assert_eq!(k.kept(), 1, "below the floor: kept");
        k.after(Duration::from_millis(50)).await;
        assert_eq!(k.kept(), 1, "the protected isolate does not expire");
        assert_eq!(k.n(ms(0)).await, 2, "and serves the next task");
    });
}

/// A killed or failed task's isolate is never kept, whatever `min_run_time`.
#[test]
fn mrt_dno_06_a_killed_or_failed_task_is_never_kept_whatever_min_run_time() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        k.backlog.set(3);
        for mrt in [ms(60_000), ms(0), None] {
            let ran = k.h.run(svc_spec(mrt).http_get("/throw").finish(), &k.p).await;
            assert!(matches!(ran.outcome, Outcome::GuestError));
            let ran =
                k.h.run(svc_spec(mrt).cpu_limit(100).http_get("/cpu").finish(), &k.p)
                    .await;
            assert!(
                matches!(ran.outcome, Outcome::Killed(KillReason::CpuLimit)),
                "{:?}",
                ran.outcome
            );
            k.h.settle(Duration::from_millis(30)).await;
            assert_eq!(k.kept(), 0, "{mrt:?}");
        }
    });
}

/// Memory pressure `High` sheds kept isolates although their `min_run_time` has not passed.
#[test]
fn mrt_dno_07_high_pressure_sheds_isolates_inside_their_min_run_time() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        k.n(ms(60_000)).await;
        assert_eq!(k.kept(), 1);
        let high =
            k.h.gov
                .reserve(MemCategory::Other, 400 * MIB, Owner::NONE)
                .expect("reserve");
        k.h.settle(Duration::from_millis(100)).await;
        k.h.exec.on_idle(Duration::from_millis(100));
        assert_eq!(k.kept(), 0, "shed at High");
        assert_eq!(k.count("shed"), 1.0);
        drop(high);
    });
}

/// A streamed answer starts the keep-alive at the end of its body, not at its head.
#[test]
fn mrt_dno_08_a_streamed_answer_starts_the_keep_alive_at_its_end() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        let ran = k.h.run(svc_spec(ms(150)).http_get("/stream").finish(), &k.p).await;
        let got = read(ran.outcome).await; // about 240 ms of body after the head
        assert_eq!(got.text(), "xxx");
        k.after(Duration::from_millis(20)).await;
        assert_eq!(k.kept(), 1, "over 150 ms since the head, but not since the body ended");
        k.after(Duration::from_millis(200)).await;
        assert_eq!(k.kept(), 0);
    });
}

/// `ctx.waitUntil` work finishes before the keep-alive starts.
#[test]
fn mrt_dno_09_wait_until_finishes_before_the_keep_alive_starts() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        let ran = k.h.run(svc_spec(ms(150)).http_get("/wait").finish(), &k.p).await;
        assert_eq!(read(ran.outcome).await.json()["n"], 1);
        k.after(Duration::from_millis(80)).await;
        assert_eq!(k.kept(), 0, "waitUntil still pending: not parked yet");
        k.after(Duration::from_millis(180)).await; // ~260 ms after the head, ~60 ms after parking
        assert_eq!(k.kept(), 1, "the keep-alive counts from the end of waitUntil");
        k.after(Duration::from_millis(200)).await;
        assert_eq!(k.kept(), 0);
    });
}

impl Ka {
    /// The real driver's park on an idle Machine: sleep until the executor signals or its next
    /// deadline is due (never longer than `cap`), then housekeep. Returns whether it woke.
    async fn idle_park(&mut self, cap: Duration) -> bool {
        let signal = self.h.exec.wake_signal().expect("the Deno executor has a wake signal");
        let deadline = self.h.exec.next_deadline();
        let woke = tokio::select! {
            () = signal.notified() => true,
            () = async { tokio::time::sleep_until(deadline.expect("guarded").into()).await }, if deadline.is_some() => true,
            () = tokio::time::sleep(cap) => false,
        };
        self.h.exec.between_tasks(Duration::from_millis(50));
        woke
    }

    /// Park like an idle Machine until `done`, at most `within`; returns the time it took.
    async fn idle_until(&mut self, within: Duration, done: impl Fn(&Self) -> bool) -> Option<Duration> {
        let start = tokio::time::Instant::now();
        while !done(self) {
            let left = within.checked_sub(start.elapsed())?;
            if !self.idle_park(left).await && !done(self) {
                return None;
            }
        }
        Some(start.elapsed())
    }
}

/// A kept isolate expires at its `min_run_time` although no task arrives to housekeep: the idle
/// Machine wakes at the executor's next deadline.
#[test]
fn mrt_dno_10_expiry_happens_on_an_idle_machine() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        let mrt = Duration::from_millis(300);
        assert_eq!(k.n(Some(mrt)).await, 1);
        assert_eq!(k.kept(), 1);
        let deadline = k.h.exec.next_deadline().expect("a kept isolate has a deadline");
        assert!(deadline <= std::time::Instant::now() + mrt + Duration::from_millis(100));
        let took = k
            .idle_until(mrt + Duration::from_secs(1), |k| k.kept() == 0)
            .await
            .expect("expired within min_run_time + 1 s on an idle Machine");
        assert!(
            took + Duration::from_millis(20) >= mrt - Duration::from_millis(50),
            "not before it: {took:?}"
        );
        assert_eq!(k.count("expired"), 1.0);
        assert_eq!(k.h.exec.next_deadline(), None, "nothing else is due");
    });
}

/// `min_run_time = 0`: the isolate queued for disposal after the answer is disposed by the idle
/// Machine, woken by the executor's signal.
#[test]
fn mrt_dno_11_zero_disposes_on_an_idle_machine() {
    run_local(async {
        let mut k = ka(Duration::from_secs(60)).await;
        assert_eq!(k.n(ms(0)).await, 1);
        assert_eq!(k.kept(), 0);
        assert_eq!(k.count("disposed"), 1.0);
        assert_eq!(k.h.exec.dispose_backlog_len(), 1, "queued for idle-time disposal");
        k.idle_until(Duration::from_secs(1), |k| k.h.exec.dispose_backlog_len() == 0)
            .await
            .expect("disposed within 1 s on an idle Machine");
    });
}
