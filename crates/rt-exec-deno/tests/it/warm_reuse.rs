//! Warm isolate reuse (`deno.reuse_isolates`, decision D-A4): DNO-17..21.
//!
//! With reuse on, a cleanly finished isolate keeps its evaluated module for the next task of the
//! same tenant, service, version and artifact on the same Machine. Every test drives the real
//! executor through `Executor::start` (see `common/mod.rs`).

use crate::common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use rt_mem::{MemCategory, Owner};
use rt_sched::{WarmKey, WarmTarget, WarmTargets};
use rt_testkit::SpecBuilder;
use rt_types::{KillReason, Outcome, ServiceKnobs, ServiceRef, VersionNo};
use serde_json::Value;

/// `n` counts the requests this isolate served (module state); `boot` is drawn once per evaluation.
const COUNTER: &str = r#"
const boot = crypto.randomUUID();
export default async (req: Request, ctx: { waitUntil(p: Promise<unknown>): void }) => {
  const url = new URL(req.url);
  const g = globalThis as any;
  g.n = (g.n ?? 0) + 1;
  const n = g.n;
  switch (url.pathname) {
    case "/throw":
      throw new Error("boom");
    case "/cpu":
      for (;;) {}
    case "/reject":
      Promise.reject(new Error("nobody handles this"));
      break;
    case "/timer":
      setTimeout(() => {}, 5000);
      break;
    case "/wait":
      ctx.waitUntil(Promise.reject(new Error("late failure")));
      break;
    case "/bodyerr":
      return new Response(new ReadableStream({ pull(c) { c.error(new Error("stream")); } }));
    case "/oom": {
      const keep = [];
      for (let i = 0; i < 64; i++) keep.push(new Uint8Array(16 << 20));
      return new Response(String(keep.length));
    }
    case "/sleep":
      await new Promise((r) => setTimeout(r, 150));
      break;
    case "/spin": {
      let x = 0;
      const k = Number(url.searchParams.get("k"));
      for (let i = 0; i < k; i++) x = (x + i) % 7;
      g.sink = x;
      break;
    }
    case "/alloc": {
      const b = new Uint8Array(40 << 20);
      b[1] = 1;
      break;
    }
    case "/env":
      return Response.json({ n, a: g.a ??= Deno.env.get("A") ?? null });
    case "/compile":
      WebAssembly.compile(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0])); // not awaited
      break;
    case "/hold":
      (g.held ??= []).push(new TextEncoder().encode("x".repeat(8 << 20)));
      break;
  }
  return Response.json({ n, boot, now: Date.now() });
};
"#;

fn reuse_knobs() -> ServiceKnobs {
    ServiceKnobs {
        reuse_isolates: true,
        ..ServiceKnobs::default()
    }
}

/// Service 1 / tenant 1 with reuse on.
fn reuse() -> SpecBuilder {
    spec().knobs(reuse_knobs())
}

async fn json_of(h: &mut Harness, s: SpecBuilder, path: &str, p: &Arc<rt_exec_deno::DenoPrepared>) -> Value {
    let ran = h.run(s.http_get(path).finish(), p).await;
    read(ran.outcome).await.json()
}

async fn n_of(h: &mut Harness, s: SpecBuilder, p: &Arc<rt_exec_deno::DenoPrepared>) -> u64 {
    json_of(h, s, "/", p).await["n"].as_u64().expect("n")
}

/// Let the driver finish (park or dispose) after the head went out.
async fn finish(h: &Harness) {
    h.settle(Duration::from_millis(20)).await;
}

// ---------------------------------------------------------------------------------------------
// DNO-17: module state persists between requests of one service version when reuse is on
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_17_with_reuse_module_state_persists_and_top_level_runs_once() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let mut boots = Vec::new();
        for want in 1..=3 {
            let v = json_of(&mut h, reuse(), "/", &p).await;
            assert_eq!(v["n"], want, "request {want} reuses the evaluated isolate");
            boots.push(v["boot"].as_str().expect("boot").to_string());
            finish(&h).await;
        }
        assert!(
            boots.iter().all(|b| *b == boots[0]),
            "top-level code ran once: {boots:?}"
        );
        assert_eq!(h.exec.warm_len(), 1, "one isolate kept for the service");
    });
}

#[test]
fn ac_dno_17_without_reuse_module_state_never_survives() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let mut boots = Vec::new();
        for _ in 0..3 {
            let v = json_of(&mut h, spec(), "/", &p).await;
            assert_eq!(
                v["n"], 1,
                "DNO-01 holds with reuse off (the default outside min_latency)"
            );
            boots.push(v["boot"].as_str().expect("boot").to_string());
            finish(&h).await;
        }
        assert!(
            boots[0] != boots[1] && boots[1] != boots[2],
            "DNO-02 holds with reuse off"
        );
        assert_eq!(h.exec.warm_len(), 0);
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-18: never across tenants, services or versions (even for identical code)
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_18_reuse_never_crosses_services_tenants_or_versions() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        // One artifact (same digest, same PreparedKey) deployed as several services.
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let a = || reuse(); // tenant 1, service 1, version 1
        let b = || reuse().service(2); // another service of the same tenant
        let c = || reuse().tenant(2); // the same service id under another tenant
        let v2 = || {
            let r = ServiceRef {
                version: VersionNo(2),
                ..rt_testkit::service_ref(1, 1)
            };
            reuse().service_ref(r)
        };

        assert_eq!(n_of(&mut h, a(), &p).await, 1);
        finish(&h).await;
        assert_eq!(n_of(&mut h, a(), &p).await, 2, "same service version: reused");
        finish(&h).await;
        assert_eq!(
            n_of(&mut h, b(), &p).await,
            1,
            "another service never gets service 1's isolate"
        );
        finish(&h).await;
        assert_eq!(
            n_of(&mut h, c(), &p).await,
            1,
            "another tenant never gets tenant 1's isolate"
        );
        finish(&h).await;
        assert_eq!(n_of(&mut h, a(), &p).await, 3, "service 1 still has its own");
        finish(&h).await;
        assert_eq!(h.exec.warm_len(), 3, "one kept per (tenant, service)");

        // A new version starts fresh and discards what was kept for the old one.
        assert_eq!(
            n_of(&mut h, v2(), &p).await,
            1,
            "a new version never gets the old version's isolate"
        );
        finish(&h).await;
        assert_eq!(n_of(&mut h, v2(), &p).await, 2);
        finish(&h).await;
        assert_eq!(
            n_of(&mut h, a(), &p).await,
            1,
            "version 1's isolate was discarded at the version change"
        );
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-19: a killed or failing isolate, or one with work left, is never reused
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_19_failed_killed_or_busy_isolates_are_never_reused() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(COUNTER)).await;
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1);
        finish(&h).await;

        for (path, cpu_ms, why) in [
            ("/throw", 1000, "the handler threw"),
            ("/cpu", 100, "killed at the CPU limit"),
            ("/oom", 1000, "out of memory"),
            ("/reject", 1000, "an unhandled rejection"),
            ("/timer", 1000, "a timer still pending"),
            ("/wait", 1000, "a waitUntil promise rejected"),
            ("/bodyerr", 1000, "the response body failed"),
            ("/compile", 1000, "a wasm compile still pending"),
        ] {
            // warm up a kept isolate (n = 2) so the failing request runs on a reused one
            assert_eq!(n_of(&mut h, reuse(), &p).await, 2, "before {path}");
            finish(&h).await;
            let ran = h
                .run(reuse().cpu_limit(cpu_ms).memory_mb(64).http_get(path).finish(), &p)
                .await;
            if path == "/cpu" {
                assert!(
                    matches!(ran.outcome, Outcome::Killed(KillReason::CpuLimit)),
                    "{path}: {:?}",
                    ran.outcome
                );
            }
            if path == "/oom" {
                assert!(
                    matches!(ran.outcome, Outcome::Killed(KillReason::Oom)),
                    "{path}: {:?}",
                    ran.outcome
                );
            }
            if let Outcome::Ok(reply) = ran.outcome {
                let _ = reply.body.collect(16 * MIB).await;
            }
            h.settle(Duration::from_millis(50)).await;
            assert_eq!(h.exec.warm_len(), 0, "{why}: the isolate is not kept");
            assert_eq!(
                n_of(&mut h, reuse(), &p).await,
                1,
                "{why}: the next request starts fresh"
            );
            finish(&h).await;
        }
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-20: limits apply per request on a reused isolate
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_20_cpu_budget_is_per_request_on_a_reused_isolate() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.warm_up().await;
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let k = 20_000_000u64;
        let spin = format!("/spin?k={k}");
        // measure one request's CPU with a generous budget
        let ran = h.run(reuse().cpu_limit(10_000).http_get(&spin).finish(), &p).await;
        let one = read(ran.outcome).await.timings.cpu;
        finish(&h).await;
        assert!(
            one > Duration::from_millis(5),
            "the spin must cost measurable CPU, got {one:?}"
        );
        // each request fits a budget of ~2x one request; cumulative accounting would kill the 3rd
        let budget = (one * 2).max(one + Duration::from_millis(50));
        for i in 2..=5 {
            let ran = h
                .run(
                    reuse().cpu_limit(budget.as_millis() as u64).http_get(&spin).finish(),
                    &p,
                )
                .await;
            let got = read(ran.outcome).await;
            assert_eq!(got.json()["n"], i, "request {i} ran on the kept isolate");
            assert!(
                got.timings.cpu < budget,
                "request {i}: CPU {:?} is this request's only",
                got.timings.cpu
            );
            finish(&h).await;
        }
    });
}

#[test]
fn ac_dno_20_memory_wall_and_clock_are_per_request_on_a_reused_isolate() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(COUNTER)).await;
        // 40 MiB per request against 64 MiB: fits each time, only if each request is charged alone
        for i in 1..=4 {
            let ran = h.run(reuse().memory_mb(64).http_get("/alloc").finish(), &p).await;
            assert_eq!(read(ran.outcome).await.json()["n"], i, "alloc request {i}");
            finish(&h).await;
        }
        // 150 ms of waiting per request against a 300 ms wall: the wall timer restarts per request
        for _ in 0..3 {
            let ran = h.run(reuse().wall_limit(300).http_get("/sleep").finish(), &p).await;
            assert!(matches!(ran.outcome, Outcome::Ok(_)), "sleep: {:?}", ran.outcome);
            finish(&h).await;
        }
        // the frozen clock is re-anchored per request
        let a = json_of(&mut h, reuse(), "/", &p).await;
        h.settle(Duration::from_millis(60)).await;
        let b = json_of(&mut h, reuse(), "/", &p).await;
        assert_eq!(b["n"].as_u64(), a["n"].as_u64().map(|n| n + 1), "same isolate");
        let (ta, tb) = (a["now"].as_f64().expect("now"), b["now"].as_f64().expect("now"));
        assert!(
            tb - ta >= 50.0,
            "the clock of the second request starts at its own start: {ta} -> {tb}"
        );
        // the kept isolate holds memory until it is disposed
        assert!(h.gov.committed() > 0);
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-21: env and secrets are re-checked; expiry, max uses and memory pressure discard
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_21_an_env_change_discards_isolates_that_read_env() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[("A", "one")], &[]);
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let v = json_of(&mut h, reuse(), "/env", &p).await;
        assert_eq!((v["n"].as_u64(), v["a"].as_str()), (Some(1), Some("one")));
        finish(&h).await;
        let v = json_of(&mut h, reuse(), "/env", &p).await;
        assert_eq!(
            (v["n"].as_u64(), v["a"].as_str()),
            (Some(2), Some("one")),
            "same env: reused"
        );
        finish(&h).await;
        // a rotated value (a new secrets version) must not be served from module state
        h.control.set_env(svc(1), &[("A", "two")], &[]);
        let v = json_of(&mut h, reuse(), "/env", &p).await;
        assert_eq!(
            (v["n"].as_u64(), v["a"].as_str()),
            (Some(1), Some("two")),
            "env changed: fresh isolate"
        );
    });
}

#[test]
fn dno_21_max_uses_and_idle_ttl_bound_a_kept_isolate() {
    run_local(async {
        let cfg = rt_exec_deno::DenoExecutorConfig {
            reuse_max_uses: 2,
            reuse_idle_ttl: Duration::from_millis(80),
            ..cfg0()
        };
        let mut h = Harness::new(cfg);
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let mut seen = Vec::new();
        for _ in 0..4 {
            seen.push(n_of(&mut h, reuse(), &p).await);
            finish(&h).await;
        }
        assert_eq!(seen, vec![1, 2, 1, 2], "an isolate serves at most reuse_max_uses tasks");
        assert_eq!(h.exec.warm_len(), 0, "the second use reached the limit");
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1);
        finish(&h).await;
        assert_eq!(h.exec.warm_len(), 1);
        h.settle(Duration::from_millis(120)).await;
        h.exec.between_tasks(Duration::from_millis(50));
        assert_eq!(h.exec.warm_len(), 0, "expired after reuse_idle_ttl");
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1);
    });
}

#[test]
fn dno_21_memory_pressure_sheds_kept_isolates() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(COUNTER)).await;
        n_of(&mut h, reuse(), &p).await;
        finish(&h).await;
        assert_eq!(h.exec.warm_len(), 1);
        let held = h.gov.committed();
        // push the node to `High` (the harness governor has 512 MiB, high = 0.80)
        let pressure = h
            .gov
            .reserve(MemCategory::Other, 440 * MIB, Owner::NONE)
            .expect("reserve");
        h.exec.on_idle(Duration::from_millis(500));
        assert_eq!(h.exec.warm_len(), 0, "kept isolates are shed at High");
        assert!(h.gov.committed() < held + 440 * MIB, "their memory is released");
        drop(pressure);
        assert_eq!(h.gov.committed(), 0, "nothing else held");
    });
}

#[test]
fn dno_21_warm_targets_size_the_kept_set() {
    run_local(async {
        let targets = WarmTargets::new();
        let cfg = rt_exec_deno::DenoExecutorConfig {
            warm_targets: Some(targets.clone()),
            ..cfg0()
        };
        let mut h = Harness::new(cfg);
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let service = reuse().finish().service;
        let run3 = |h: &mut Harness| {
            (0..3)
                .map(|_| h.start(reuse().http_get("/sleep").finish(), &p).1)
                .collect::<Vec<_>>()
        };
        // no target published: the executor's own bound (reuse_max_idle = 4) keeps all three
        for outcome in futures_util::future::join_all(run3(&mut h)).await {
            read(outcome).await;
        }
        h.settle(Duration::from_millis(30)).await;
        assert_eq!(h.exec.warm_len_of(service, p.key), 3);
        // a published target of 1 trims the set between tasks and bounds what is kept later
        targets.publish(HashMap::from([(
            WarmKey {
                service: service.service,
                key: p.key,
            },
            WarmTarget { service, per_proc: 1 },
        )]));
        h.exec.between_tasks(Duration::from_millis(50));
        assert_eq!(h.exec.warm_len_of(service, p.key), 1);
        for outcome in futures_util::future::join_all(run3(&mut h)).await {
            read(outcome).await;
        }
        h.settle(Duration::from_millis(30)).await;
        assert_eq!(h.exec.warm_len_of(service, p.key), 1);
    });
}

// ---------------------------------------------------------------------------------------------
// Review follow-ups: memory held in module state, warm-store entries, CPU visibility
// ---------------------------------------------------------------------------------------------

#[test]
fn memory_held_in_module_state_counts_and_a_full_isolate_is_not_kept() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(COUNTER)).await;
        // Each request keeps 8 MiB made by an op (outside the allocator) in a global.
        let mut seen = Vec::new();
        for i in 0..8 {
            let ran = h.run(reuse().memory_mb(64).http_get("/hold").finish(), &p).await;
            let v = match ran.outcome {
                Outcome::Ok(_) => read(ran.outcome).await.json(),
                other => panic!("request {i}: {other:?}"),
            };
            seen.push(v["n"].as_u64().expect("n"));
            finish(&h).await;
        }
        assert!(
            seen[1..].contains(&1),
            "an isolate holding too much is replaced: {seen:?}"
        );
        assert!(seen.iter().all(|n| *n < 6), "never kept until the task limit: {seen:?}");
    });
}

#[test]
fn the_warm_store_keeps_no_entries_for_services_with_nothing_kept() {
    run_local(async {
        let cfg = rt_exec_deno::DenoExecutorConfig {
            reuse_idle_ttl: Duration::from_millis(50),
            ..cfg0()
        };
        let mut h = Harness::new(cfg);
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let ran = h.run(reuse().http_get("/throw").finish(), &p).await;
        assert!(matches!(ran.outcome, Outcome::GuestError));
        finish(&h).await;
        assert_eq!(h.exec.warm_services(), 0, "a failed task leaves no entry behind");
        n_of(&mut h, reuse(), &p).await;
        finish(&h).await;
        assert_eq!((h.exec.warm_len(), h.exec.warm_services()), (1, 1));
        h.settle(Duration::from_millis(80)).await;
        h.exec.between_tasks(Duration::from_millis(50));
        assert_eq!(
            (h.exec.warm_len(), h.exec.warm_services()),
            (0, 0),
            "expired entries are pruned"
        );
    });
}

/// Thread CPU spent inside the task future's polls: what the scheduler's per-poll accounting
/// (`rt-sched` `Tracked`: CPU limit, billing, autoscaler load) sees.
struct CpuMeasured {
    fut: rt_sched::TaskFuture,
    cpu_ns: u64,
}

impl std::future::Future for CpuMeasured {
    type Output = (Outcome, Duration);
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let t0 = rt_exec_deno::cpu::thread_cpu_ns();
        let polled = self.fut.as_mut().poll(cx);
        let burn = rt_exec_deno::cpu::thread_cpu_ns().saturating_sub(t0);
        self.cpu_ns += burn;
        polled.map(|o| (o, Duration::from_nanos(self.cpu_ns)))
    }
}

#[test]
fn guest_cpu_runs_inside_the_task_future_and_a_streamed_tail_is_billed() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.warm_up().await;
        let spin = "let x = 0; for (let i = 0; i < 30_000_000; i++) x = (x + i) % 7;";
        let p = h
            .prepare(&ts_upload(&format!(
                "export default async (req: Request) => {{
  if (new URL(req.url).pathname === '/tail') {{
    let sent = false;
    return new Response(new ReadableStream({{ async pull(c) {{
      if (!sent) {{ sent = true; c.enqueue(new Uint8Array([1])); return; }}
      await new Promise((r) => setTimeout(r, 20)); // the head is out by now
      {spin}
      c.close();
    }} }}));
  }}
  {spin}
  return new Response(String(x));
}};
"
            )))
            .await;
        let (_, fut) = h.start(get("/"), &p);
        let (outcome, seen) = CpuMeasured { fut, cpu_ns: 0 }.await;
        let got = read(outcome).await;
        assert!(
            got.timings.cpu > Duration::from_millis(5),
            "the spin costs CPU: {:?}",
            got.timings.cpu
        );
        assert!(
            seen.as_secs_f64() >= got.timings.cpu.as_secs_f64() * 0.8,
            "the scheduler sees {seen:?} of {:?} guest CPU",
            got.timings.cpu
        );

        // After the head: the tail's CPU is charged to the tenant when the task context goes.
        let before = h.control.usage_cpu_ms.load(std::sync::atomic::Ordering::SeqCst);
        let got = read(h.run(get("/tail"), &p).await.outcome).await;
        assert_eq!(got.body.len(), 1);
        h.settle(Duration::from_millis(30)).await;
        h.exec.on_idle(Duration::from_millis(500));
        let tail = h.control.usage_cpu_ms.load(std::sync::atomic::Ordering::SeqCst) - before;
        assert!(tail >= 3, "the streamed tail's CPU is billed: {tail} ms");
    });
}

/// Security review pass 2: a guest cannot replace the task dispatcher (`globalThis.__rt`) of a
/// warm isolate, which would let it see the next task's payload or fake a clean finish.
const HIJACK: &str = "let hijacked = 0;\n\
try { globalThis.__rt = { start: () => { hijacked++; return true; } }; } catch (_) {}\n\
try { Object.defineProperty(globalThis, '__rt', { value: {} }); } catch (_) {}\n\
try { delete globalThis.__rt; } catch (_) {}\n\
export default () => Response.json({ hijacked, frozen: Object.isFrozen(globalThis.__rt) });\n";

#[test]
fn the_dispatcher_cannot_be_replaced_by_the_guest() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(HIJACK)).await;
        for _ in 0..3 {
            let v = json_of(&mut h, reuse(), "/", &p).await;
            assert_eq!(v, serde_json::json!({"hijacked": 0, "frozen": true}));
            finish(&h).await;
        }
    });
}

// ---------------------------------------------------------------------------------------------
// The bounds of the kept set
// ---------------------------------------------------------------------------------------------

#[test]
fn dno_17_with_no_room_configured_nothing_is_kept() {
    run_local(async {
        for cfg in [
            rt_exec_deno::DenoExecutorConfig {
                reuse_max_idle: 0,
                ..cfg0()
            },
            rt_exec_deno::DenoExecutorConfig {
                reuse_max_idle_total: 0,
                ..cfg0()
            },
        ] {
            let mut h = Harness::new(cfg);
            let p = h.prepare(&ts_upload(COUNTER)).await;
            assert_eq!(n_of(&mut h, reuse(), &p).await, 1);
            finish(&h).await;
            assert_eq!(n_of(&mut h, reuse(), &p).await, 1, "no isolate was kept");
            assert_eq!(h.exec.warm_len(), 0);
        }
    });
}

#[test]
fn dno_17_past_the_total_bound_the_oldest_isolate_of_any_service_goes() {
    run_local(async {
        let cfg = rt_exec_deno::DenoExecutorConfig {
            reuse_max_idle_total: 1,
            ..cfg0()
        };
        let mut h = Harness::new(cfg);
        let p = h.prepare(&ts_upload(COUNTER)).await;
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1);
        finish(&h).await;
        assert_eq!(n_of(&mut h, reuse().service(2), &p).await, 1);
        finish(&h).await;
        assert_eq!(h.exec.warm_len(), 1, "one kept in all");
        assert_eq!(n_of(&mut h, reuse().service(2), &p).await, 2, "the newer one stayed");
        finish(&h).await;
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1, "service 1's, the oldest, was shed");
    });
}

#[test]
fn dno_17_a_kept_isolate_past_its_idle_ttl_is_not_handed_out() {
    run_local(async {
        let cfg = rt_exec_deno::DenoExecutorConfig {
            reuse_idle_ttl: Duration::from_millis(40),
            ..cfg0()
        };
        let mut h = Harness::new(cfg);
        let p = h.prepare(&ts_upload(COUNTER)).await;
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1);
        finish(&h).await;
        h.settle(Duration::from_millis(120)).await; // no idle hook runs: the next task meets the stale one
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1, "expired, so a fresh isolate");
    });
}

#[test]
fn dno_18_a_task_of_a_replaced_version_that_finishes_late_is_not_kept() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let v2 = || {
            let r = ServiceRef {
                version: VersionNo(2),
                ..rt_testkit::service_ref(1, 1)
            };
            reuse().service_ref(r)
        };
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1);
        finish(&h).await;
        // A slow task of version 1 is running when version 2 serves its first request.
        let (_task, slow) = h.start(reuse().http_get("/sleep").finish(), &p);
        let slow = tokio::task::spawn_local(slow);
        h.settle(Duration::from_millis(20)).await;
        assert_eq!(n_of(&mut h, v2(), &p).await, 1);
        finish(&h).await;
        let outcome = slow.await.expect("slow task");
        read(outcome).await;
        finish(&h).await;
        assert_eq!(h.exec.warm_len(), 1, "only version 2's isolate is kept");
        assert_eq!(n_of(&mut h, v2(), &p).await, 2);
    });
}

#[test]
fn dno_20_an_isolate_that_read_env_is_not_reused_when_the_env_can_no_longer_be_checked() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(rt_testkit::ids::service(1), &[("A", "1")], &[]);
        let p = h.prepare(&ts_upload(COUNTER)).await;
        let first = json_of(&mut h, reuse(), "/env", &p).await;
        assert_eq!(first["n"], 1);
        finish(&h).await;
        let second = json_of(&mut h, reuse(), "/env", &p).await;
        assert_eq!(second["n"], 2, "the same env: the kept isolate serves");
        finish(&h).await;

        h.control.env_fails.store(true, std::sync::atomic::Ordering::SeqCst);
        let ran = h.run(reuse().http_get("/").finish(), &p).await;
        let third = read(ran.outcome).await.json();
        assert_eq!(
            third["n"], 1,
            "with the env unreadable the kept isolate is not trusted: a fresh one served"
        );
    });
}

#[test]
fn dno_19_a_request_body_the_handler_never_read_does_not_keep_the_isolate_from_being_reused() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(COUNTER)).await;
        assert_eq!(n_of(&mut h, reuse(), &p).await, 1);
        finish(&h).await;
        // The body arrives as a handle in the isolate's resource table; the handler ignores it.
        let post = reuse().payload(post_payload("/", "text/plain", "never read")).finish();
        let ran = h.run(post, &p).await;
        assert_eq!(read(ran.outcome).await.json()["n"], 2);
        finish(&h).await;
        assert_eq!(
            n_of(&mut h, reuse(), &p).await,
            3,
            "the leftover handle was closed and the isolate is reused"
        );
    });
}

#[test]
fn dno_20_an_isolate_that_grew_far_past_what_its_first_task_held_is_not_reused() {
    run_local(async {
        let mut h = Harness::new(rt_exec_deno::DenoExecutorConfig {
            reuse_max_uses: 1000,
            ..cfg0()
        });
        let p = h.prepare(&ts_upload(COUNTER)).await;
        assert_eq!(n_of(&mut h, reuse().memory_mb(512), &p).await, 1);
        finish(&h).await;
        // Each /hold keeps 8 MiB in module state; past REUSE_MAX_GROWTH the isolate is dropped.
        let mut seen = Vec::new();
        for _ in 0..5 {
            seen.push(
                json_of(&mut h, reuse().memory_mb(512), "/hold", &p).await["n"]
                    .as_u64()
                    .expect("n"),
            );
            finish(&h).await;
        }
        assert_eq!(seen.first(), Some(&2), "light growth is tolerated");
        assert!(
            seen.contains(&1),
            "the heavy isolate was dropped along the way: {seen:?}"
        );
    });
}
