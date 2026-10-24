//! The warm floor of the Deno executor (decision D-B2): the scheduler
//! publishes, per service artifact, how many ready instances each Processor keeps
//! (`WarmTargets::per_proc`, never below the floor of `scaling.min_instances`). The executor keeps
//! at least that many kept isolates alive whatever `reuse_idle_ttl` says, builds them between
//! tasks when it holds fewer (reuse on), and pools blank isolates for them when reuse is off.
//! DNO-29, DNO-30.

use crate::common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use rt_exec_deno::{DenoExecutorConfig, DenoPrepared};
use rt_mem::{MemCategory, Owner};
use rt_sched::{WarmKey, WarmTarget, WarmTargets};
use rt_testkit::SpecBuilder;
use rt_types::{ServiceKnobs, ServiceRef, VersionNo};
use serde_json::Value;

/// `n` counts the requests this isolate served; `evals` counts evaluations of the module (a
/// top-level `await` bumps a counter in KV once per evaluated isolate).
const SRC: &str = r#"
import { kv } from "std:kv";
await kv.incr("evals", 1);
export default async (req: Request) => {
  const g = globalThis as any;
  g.n = (g.n ?? 0) + 1;
  const n = g.n;
  if (new URL(req.url).pathname === "/sleep") await new Promise((r) => setTimeout(r, 120));
  return Response.json({ n, evals: await kv.get("evals") });
};
"#;

fn reuse() -> SpecBuilder {
    spec().knobs(ServiceKnobs {
        reuse_isolates: true,
        ..ServiceKnobs::default()
    })
}

struct Floor {
    h: Harness,
    p: Arc<DenoPrepared>,
    service: ServiceRef,
    targets: Arc<WarmTargets>,
}

async fn floor(ttl: Duration, tune: impl FnOnce(&mut DenoExecutorConfig)) -> Floor {
    floor_of(SRC, ttl, tune).await
}

async fn floor_of(src: &str, ttl: Duration, tune: impl FnOnce(&mut DenoExecutorConfig)) -> Floor {
    let targets = WarmTargets::new();
    let mut cfg = DenoExecutorConfig {
        reuse_idle_ttl: ttl,
        ..cfg0()
    }
    .with_warm_targets(targets.clone());
    tune(&mut cfg);
    let h = Harness::new(cfg);
    let p = h.prepare(&ts_upload(src)).await;
    let service = reuse().finish().service;
    Floor { h, p, service, targets }
}

impl Floor {
    /// Publish `per_proc` for the service (0: the key vanishes from the snapshot, as when the
    /// autoscaler scales it to zero).
    fn publish(&self, per_proc: u32) {
        let targets = if per_proc == 0 {
            HashMap::new()
        } else {
            HashMap::from([(
                WarmKey {
                    service: self.service.service,
                    key: self.p.key,
                },
                WarmTarget {
                    service: self.service,
                    per_proc,
                },
            )])
        };
        self.targets.publish(targets);
    }

    fn kept(&self) -> usize {
        self.h.exec.warm_len_of(self.service, self.p.key)
    }

    /// `n` requests at once (each sleeps, so each needs its own isolate); their bodies as JSON.
    async fn burst(&mut self, spec: impl Fn() -> SpecBuilder, n: usize) -> Vec<Value> {
        let tasks: Vec<_> = (0..n)
            .map(|_| self.h.start(spec().http_get("/sleep").finish(), &self.p).1)
            .collect();
        let mut out = Vec::new();
        for outcome in futures_util::future::join_all(tasks).await {
            out.push(read(outcome).await.json());
        }
        self.h.settle(Duration::from_millis(30)).await;
        out
    }

    async fn one(&mut self, spec: SpecBuilder) -> Value {
        let ran = self.h.run(spec.http_get("/").finish(), &self.p).await;
        let v = read(ran.outcome).await.json();
        self.h.settle(Duration::from_millis(20)).await;
        v
    }

    /// Run the Machine's between-tasks hook until `done` holds (isolates are built by local tasks).
    async fn converge(&mut self, what: &str, done: impl Fn(&Floor) -> bool) {
        for _ in 0..80 {
            if done(self) {
                return;
            }
            self.h.exec.between_tasks(Duration::from_millis(50));
            self.h.settle(Duration::from_millis(25)).await;
        }
        panic!("did not converge: {what}");
    }
}

#[test]
fn ac_dno_29_kept_isolates_up_to_the_target_outlive_the_idle_ttl() {
    run_local(async {
        let mut f = floor(Duration::from_millis(60), |_| {}).await;
        f.publish(2);
        f.burst(reuse, 2).await;
        assert_eq!(f.kept(), 2);

        f.h.settle(Duration::from_millis(150)).await; // well past reuse_idle_ttl
        f.h.exec.between_tasks(Duration::from_millis(50));
        f.h.exec.on_idle(Duration::from_millis(50));
        assert_eq!(f.kept(), 2, "the published target is not expired by the idle TTL");
        // and a task still gets one: the take does not expire it either
        let v = f.one(reuse()).await;
        assert_eq!(
            v["n"], 2,
            "served by a kept isolate that had been idle past the TTL: {v}"
        );
        assert_eq!(f.kept(), 2);
    });
}

#[test]
fn ac_dno_29_without_a_target_the_idle_ttl_applies() {
    run_local(async {
        let mut f = floor(Duration::from_millis(60), |_| {}).await;
        f.burst(reuse, 2).await;
        assert_eq!(
            f.kept(),
            2,
            "no target published: the executor's own bound keeps both for now"
        );
        f.h.settle(Duration::from_millis(150)).await;
        f.h.exec.between_tasks(Duration::from_millis(50));
        assert_eq!(f.kept(), 0, "expired");
        assert_eq!(f.one(reuse()).await["n"], 1);
    });
}

#[test]
fn ac_dno_29_isolates_above_the_target_expire_and_a_falling_target_trims() {
    run_local(async {
        let mut f = floor(Duration::from_millis(60), |_| {}).await;
        f.burst(reuse, 3).await;
        assert_eq!(f.kept(), 3);
        f.publish(2);
        f.h.settle(Duration::from_millis(150)).await;
        f.h.exec.between_tasks(Duration::from_millis(50));
        assert_eq!(
            f.kept(),
            2,
            "the one above the target went (bound and TTL); the target stays"
        );

        f.publish(1);
        f.h.exec.between_tasks(Duration::from_millis(50));
        assert_eq!(f.kept(), 1, "a falling target trims at once");

        f.publish(0);
        f.h.exec.between_tasks(Duration::from_millis(50));
        assert_eq!(f.kept(), 0, "no target, idle past the TTL: gone");
    });
}

#[test]
fn ac_dno_29_pressure_high_keeps_the_target_and_critical_drops_it() {
    run_local(async {
        let mut f = floor(Duration::from_millis(60), |_| {}).await;
        f.publish(2);
        f.burst(reuse, 3).await;
        assert_eq!(f.kept(), 2, "a target of 2 bounds what a service keeps");

        // High (the harness governor has 512 MiB, high = 0.80, critical = 0.92)
        let high =
            f.h.gov
                .reserve(MemCategory::Other, 400 * MIB, Owner::NONE)
                .expect("reserve");
        f.h.settle(Duration::from_millis(100)).await;
        f.h.exec.on_idle(Duration::from_millis(100));
        assert_eq!(
            f.kept(),
            2,
            "High: the floor stays (the scheduler already cut targets to it)"
        );

        let critical =
            f.h.gov
                .reserve(MemCategory::Other, 80 * MIB, Owner::NONE)
                .expect("reserve");
        f.h.exec.on_idle(Duration::from_millis(100));
        assert_eq!(f.kept(), 0, "Critical: everything is dropped");
        drop((high, critical));
    });
}

#[test]
fn ac_dno_29_a_new_version_discards_the_kept_isolates_of_the_old_one() {
    run_local(async {
        let mut f = floor(Duration::from_millis(60), |_| {}).await;
        f.publish(2);
        f.burst(reuse, 2).await;
        assert_eq!(f.kept(), 2);
        let v2 = ServiceRef {
            version: VersionNo(2),
            ..f.service
        };
        let ran = f.h.run(reuse().service_ref(v2).http_get("/").finish(), &f.p).await;
        assert_eq!(
            read(ran.outcome).await.json()["n"],
            1,
            "the new version starts on a fresh isolate"
        );
        f.h.settle(Duration::from_millis(30)).await;
        assert_eq!(f.kept(), 0, "nothing of the old version is kept, floor or not");
    });
}

#[test]
fn ac_dno_30_below_the_target_between_tasks_pre_creates_evaluated_isolates() {
    run_local(async {
        let mut f = floor(Duration::from_secs(60), |_| {}).await;
        f.publish(2);
        let v = f.one(reuse()).await;
        assert_eq!((v["n"].as_u64(), v["evals"].as_u64()), (Some(1), Some(1)));
        assert_eq!(f.kept(), 1);

        f.converge("a second kept isolate", |f| f.kept() == 2).await;
        // the pre-created isolate evaluated the module (once) and ran no handler
        f.h.exec.between_tasks(Duration::from_millis(50));
        f.h.settle(Duration::from_millis(100)).await;
        assert_eq!(f.kept(), 2, "at the target, nothing more is built");

        // two requests at once are both served by kept isolates: no new evaluation
        let mut ns: Vec<(u64, u64)> = f
            .burst(reuse, 2)
            .await
            .iter()
            .map(|v| (v["n"].as_u64().expect("n"), v["evals"].as_u64().expect("evals")))
            .collect();
        ns.sort_unstable();
        assert_eq!(
            ns,
            vec![(1, 2), (2, 2)],
            "the pre-created isolate's first request is n = 1; evaluations stay at 2"
        );
        assert_eq!(f.kept(), 2);
    });
}

#[test]
fn ac_dno_30_pre_creation_respects_the_total_cap_and_the_reuse_knob() {
    run_local(async {
        // the executor keeps 2 idle isolates in all: a target of 4 does not build past that
        let mut f = floor(Duration::from_secs(60), |c| c.reuse_max_idle_total = 2).await;
        f.publish(4);
        f.one(reuse()).await;
        f.converge("two kept", |f| f.h.exec.warm_len() == 2).await;
        for _ in 0..5 {
            f.h.exec.between_tasks(Duration::from_millis(50));
            f.h.settle(Duration::from_millis(30)).await;
        }
        assert_eq!(f.h.exec.warm_len(), 2, "the total cap holds");

        // a service with reuse off is never pre-evaluated (a fresh isolate per task is the contract)
        let mut f = floor(Duration::from_secs(60), |_| {}).await;
        f.publish(2);
        assert_eq!(f.one(spec()).await["n"], 1);
        for _ in 0..5 {
            f.h.exec.between_tasks(Duration::from_millis(50));
            f.h.settle(Duration::from_millis(30)).await;
        }
        assert_eq!(f.kept(), 0, "no kept isolates without reuse");
    });
}

#[test]
fn ac_dno_30_without_reuse_the_floor_is_pooled_blank_isolates() {
    run_local(async {
        let mut f = floor(Duration::from_secs(60), |_| {}).await; // deno.isolate_pool_per_p = 0
        f.publish(3);
        assert_eq!(f.h.exec.pool_len(), 0);
        assert_eq!(f.one(spec()).await["n"], 1);
        f.converge("three pooled isolates", |f| f.h.exec.pool_len() == 3).await;
        for _ in 0..3 {
            f.h.exec.between_tasks(Duration::from_millis(50));
        }
        assert_eq!(f.h.exec.pool_len(), 3, "the pool is the floor, not more");

        // High keeps the floor, Critical drops it, and the pool grows back when the pressure is gone
        let high =
            f.h.gov
                .reserve(MemCategory::Other, 400 * MIB, Owner::NONE)
                .expect("reserve");
        f.h.exec.on_idle(Duration::from_millis(100));
        assert_eq!(f.h.exec.pool_len(), 3, "High: the floor stays");
        let critical =
            f.h.gov
                .reserve(MemCategory::Other, 80 * MIB, Owner::NONE)
                .expect("reserve");
        f.h.exec.on_idle(Duration::from_millis(100));
        assert_eq!(f.h.exec.pool_len(), 0, "Critical: dropped");
        drop((high, critical));
        f.converge("the pool is back", |f| f.h.exec.pool_len() == 3).await;

        // the target falls: the pool goes back to its configured size
        f.publish(0);
        f.h.exec.on_idle(Duration::from_millis(100));
        assert_eq!(f.h.exec.pool_len(), 0, "no floor, no pool");
    });
}

#[test]
fn ac_dno_30_a_warm_up_that_fails_is_not_retried_at_every_hook() {
    run_local(async {
        // The module counts its evaluations, then throws: every evaluation fails.
        const FAILING: &str = r#"
import { kv } from "std:kv";
await kv.incr("evals", 1);
throw new Error("top-level failure");
"#;
        let targets = WarmTargets::new();
        let cfg = DenoExecutorConfig {
            warm_targets: Some(targets.clone()),
            ..cfg0()
        };
        let mut h = Harness::new(cfg);
        let p = h.prepare(&ts_upload(FAILING)).await;
        let service = reuse().finish().service;
        targets.publish(HashMap::from([(
            WarmKey {
                service: service.service,
                key: p.key,
            },
            WarmTarget { service, per_proc: 2 },
        )]));
        let ran = h.run(reuse().http_get("/").finish(), &p).await;
        assert!(
            matches!(ran.outcome, rt_types::Outcome::GuestError),
            "{:?}",
            ran.outcome
        );
        for _ in 0..12 {
            h.exec.between_tasks(Duration::from_millis(50));
            h.settle(Duration::from_millis(25)).await;
        }
        use rt_store::ServiceDataStore;
        let evals = h.data.kv(svc(1)).get(b"evals").await.expect("kv").expect("counted");
        assert_eq!(
            String::from_utf8_lossy(&evals.data),
            "2",
            "the request and one warm-up evaluated the module; the failure backs the floor off"
        );
        assert_eq!(h.exec.warm_len(), 0);
    });
}

#[test]
fn ac_dno_30_warm_ups_of_one_service_run_side_by_side_and_all_end_up_kept() {
    run_local(async {
        let mut f = floor(Duration::from_secs(60), |c| {
            c.reuse_max_idle = 8;
            c.reuse_max_idle_total = 8;
        })
        .await;
        f.publish(3);
        f.one(reuse()).await; // the Machine learns the service from its first task
                              // Each hook call starts one warm-up; the second starts before the first has finished.
        f.h.exec.between_tasks(Duration::from_millis(50));
        f.h.exec.between_tasks(Duration::from_millis(50));
        f.converge("three kept isolates", |f| f.kept() == 3).await;
    });
}

#[test]
fn ac_dno_30_a_module_that_cannot_be_evaluated_without_a_task_is_not_kept_and_the_service_still_serves() {
    run_local(async {
        // `std:sqlite` needs a task: the warm-up has none, so the module fails to evaluate there.
        let src = "import { Database } from 'std:sqlite';\nconst db = new Database();\nexport default () => new Response(String(typeof db));";
        let mut f = floor_of(src, Duration::from_secs(60), |_| {}).await;
        let first = f.h.run(reuse().http_get("/").finish(), &f.p).await; // the Machine learns the service
        assert_eq!(read(first.outcome).await.text(), "object");
        f.publish(2);
        for _ in 0..3 {
            f.h.exec.between_tasks(Duration::from_millis(50));
            f.h.settle(Duration::from_millis(40)).await;
        }
        assert_eq!(
            f.kept(),
            1,
            "the warm-up failed (only the first task's isolate is kept) and is not retried at every hook"
        );
        let ran = f.h.run(reuse().http_get("/").finish(), &f.p).await;
        assert_eq!(
            read(ran.outcome).await.text(),
            "object",
            "a real task evaluates the module fine"
        );
    });
}
