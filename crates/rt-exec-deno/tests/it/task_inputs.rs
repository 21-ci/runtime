//! What a handler is handed besides the request (socket events, the caller stamp, the invoker) and
//! how a task ends when the guest does something unusual with its response or its event loop.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use rt_sched::Interrupt;
use rt_types::{
    CallerStamp, KillReason, ObjectRef, Outcome, Payload, ProjectId, SdkStamp, ServiceId, SocketEvent, SocketEventKind,
    TaskId, TenantId, Trigger, WsData,
};
use serde_json::json;

/// A durable service: every event handler records what it was given in KV, `/seen` reads it back.
const ROOM: &str = r#"
import { kv } from 'std:kv';
export default {
  async fetch(req: Request, ctx: any) {
    const url = new URL(req.url);
    if (url.pathname === '/seen') {
      const out: Record<string, unknown> = {};
      for (const k of ['message', 'binary', 'close', 'error', 'alarm']) out[k] = await kv.get(k);
      return Response.json(out);
    }
    return Response.json({ invoker: ctx.invoker ?? null, caller: ctx.caller ?? null, client: ctx.client ?? null });
  },
  async webSocketMessage(ws: any, data: any) {
    if (typeof data === 'string') await kv.set('message', { id: ws.id, object: ws.object, data });
    else await kv.set('binary', { id: ws.id, bytes: Array.from(new Uint8Array(data)) });
  },
  async webSocketClose(_ws: any, code: number, reason: string, wasClean: boolean) {
    await kv.set('close', { code, reason, wasClean });
  },
  async webSocketError(_ws: any, err: any) {
    await kv.set('error', { code: err.code, message: err.message });
  },
  async alarm(info: any) {
    await kv.set('alarm', { scheduledTime: info.scheduledTime, retryCount: info.retryCount });
  },
};
"#;

fn event_task(kind: SocketEventKind, socket: bool) -> rt_types::TaskSpec {
    let mut s = spec()
        .trigger(Trigger::Socket { object: "room".into() })
        .payload(Payload::Socket(SocketEvent {
            socket: socket.then(|| info("ws_1", "room")),
            kind,
            attempt: 2,
        }))
        .finish();
    s.object = Some(ObjectRef::named("room"));
    s
}

#[test]
fn socket_events_reach_the_handlers_with_their_data_and_the_tasks_commit_ends_with_a_200() {
    run_local(async {
        let hub = Arc::new(RecordingHub::default());
        let mut h = Harness::with_deps(cfg0(), |d| d.sockets = hub.clone());
        let p = h.prepare(&ts_upload(ROOM)).await;
        let events = [
            (SocketEventKind::Message(WsData::Text("hello".into())), true),
            (SocketEventKind::Message(WsData::Binary(vec![1, 2, 3].into())), true),
            (
                SocketEventKind::Close {
                    code: 1001,
                    reason: "going away".into(),
                    was_clean: true,
                },
                true,
            ),
            (
                SocketEventKind::Error {
                    code: "boom".into(),
                    message: "it broke".into(),
                },
                true,
            ),
            (
                SocketEventKind::Alarm {
                    scheduled_ms: 5000,
                    retry: 1,
                },
                false,
            ),
        ];
        for (kind, with_socket) in events {
            let ran = h.run(event_task(kind, with_socket), &p).await;
            assert_eq!(read(ran.outcome).await.status, 200);
        }
        let (seen, _) = h.get("/seen", &p).await;
        assert_eq!(
            seen.json(),
            json!({
                "message": { "id": "ws_1", "object": "room", "data": "hello" },
                "binary": { "id": "ws_1", "bytes": [1, 2, 3] },
                "close": { "code": 1001, "reason": "going away", "wasClean": true },
                "error": { "code": "boom", "message": "it broke" },
                "alarm": { "scheduledTime": 5000, "retryCount": 1 },
            })
        );
    });
}

#[test]
fn ctx_carries_the_invoker_header_and_the_callers_stamp() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(ROOM)).await;

        let mut req = rt_types::HttpRequest::get("/");
        req.headers.insert("x-rt-invoker", "key_abc".parse().expect("header"));
        let (got, _) = {
            let ran = h.run(spec().payload(Payload::Http(req)).finish(), &p).await;
            (read(ran.outcome).await, ran.task)
        };
        assert_eq!(got.json()["invoker"], "key_abc");

        let stamp = CallerStamp {
            service: ServiceId::from_u128(42),
            tenant: TenantId::from_u128(7),
            slug: Some("the-caller".into()),
            project: Some(ProjectId::from_u128(9)),
            environment: Some("production".into()),
            plan: Some("pro".into()),
            sdk: Some(SdkStamp {
                name: "acme".into(),
                version: "1.0.0".into(),
            }),
        };
        let mut call = spec()
            .trigger(Trigger::Call {
                parent: TaskId(1),
                depth: 1,
            })
            .http_get("/")
            .finish();
        call.caller = Some(stamp.clone());
        let ran = h.run(call, &p).await;
        let caller = read(ran.outcome).await.json()["caller"].clone();
        assert_eq!(caller["service"]["slug"], "the-caller");
        assert_eq!(caller["tenant"], stamp.tenant.to_string());
        assert_eq!(caller["plan"], "pro");
        assert_eq!(caller["sdk"], json!({ "name": "acme", "version": "1.0.0" }));
        assert_eq!(
            caller["project"],
            stamp.project.map(|p| p.to_string()).unwrap_or_default()
        );
        assert_eq!(caller["environment"], "production");
    });
}

// ---------------------------------------------------------------------------------------------
// How a task ends
// ---------------------------------------------------------------------------------------------

async fn outcome_of(h: &mut Harness, code: &str, tweak: impl FnOnce(&mut rt_types::TaskSpec)) -> Outcome {
    let p = h.prepare(&ts_upload(code)).await;
    let mut s = get("/");
    tweak(&mut s);
    h.run(s, &p).await.outcome
}

#[test]
fn a_response_that_is_a_network_error_fails_the_task() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let out = outcome_of(&mut h, "export default () => Response.error();", |_| {}).await;
        assert!(matches!(out, Outcome::GuestError), "{out:?}");
    });
}

#[test]
fn a_response_whose_body_was_already_read_ends_in_a_failed_body_after_the_head() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let out = outcome_of(
            &mut h,
            "export default async () => { const r = new Response('x'); await r.text(); return r; };",
            |_| {},
        )
        .await;
        let Outcome::Ok(reply) = out else {
            panic!("the head leaves before the body is read: {out:?}")
        };
        let err = reply.body.collect(1 << 20).await.expect_err("the body fails");
        assert!(matches!(err, rt_types::BodyError::Aborted(_)), "{err:?}");
        assert!(
            h.log_records().iter().any(|r| r.msg.contains("TypeError")),
            "{:?}",
            h.log_records()
        );
    });
}

#[test]
fn a_streamed_response_past_max_response_stream_bytes_is_cut() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let code = "export default () => new Response(new ReadableStream({ start(c) { for (let i = 0; i < 4; i++) c.enqueue(new Uint8Array(8)); c.close(); } }));";
        let out = outcome_of(&mut h, code, |s| s.limits.max_response_stream_bytes = 16).await;
        let Outcome::Ok(reply) = out else { panic!("{out:?}") };
        let err = reply
            .body
            .collect(1 << 20)
            .await
            .expect_err("the stream is cut at the limit");
        assert!(matches!(err, rt_types::BodyError::Aborted(_)), "{err:?}");
        assert!(
            h.log_records().iter().any(|r| r.msg.contains("response_too_large")),
            "{:?}",
            h.log_records()
        );
    });
}

#[test]
fn an_error_thrown_by_a_timer_while_the_handler_waits_fails_the_task() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let code = "export default async () => { setTimeout(() => { throw new Error('late boom'); }, 5); await new Promise(() => {}); };";
        let out = outcome_of(&mut h, code, |_| {}).await;
        assert!(matches!(out, Outcome::GuestError), "{out:?}");
        assert!(
            h.log_records().iter().any(|r| r.msg.contains("late boom")),
            "{:?}",
            h.log_records()
        );
    });
}

#[test]
fn a_handler_that_can_never_settle_fails_the_task_instead_of_hanging() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let out = outcome_of(&mut h, "export default () => new Promise(() => {});", |_| {}).await;
        assert!(matches!(out, Outcome::GuestError), "{out:?}");
        assert!(
            h.log_records().iter().any(|r| r.msg.contains("never settled")),
            "{:?}",
            h.log_records()
        );
    });
}

#[test]
fn a_task_that_is_waiting_on_a_timer_is_killed_at_its_wall_deadline() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let out = outcome_of(
            &mut h,
            "export default async () => { setInterval(() => {}, 10); await new Promise(() => {}); };",
            |s| s.limits.wall = Duration::from_millis(80),
        )
        .await;
        assert!(matches!(out, Outcome::Killed(KillReason::WallTimeout)), "{out:?}");
    });
}

#[test]
fn the_interrupter_of_the_executor_kills_a_running_task_and_ignores_a_yield() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h
            .prepare(&ts_upload("export default async () => { for (;;) {} };"))
            .await;
        let (task, fut) = h.start(get("/"), &p);
        let interrupter = h.exec.interrupter();
        let killer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            interrupter.interrupt(task, Interrupt::Yield);
            interrupter.interrupt(task, Interrupt::Kill(KillReason::Shutdown));
        });
        let out = tokio::time::timeout(Duration::from_secs(20), fut)
            .await
            .expect("the kill ends the task");
        killer.join().expect("killer thread");
        assert!(matches!(out, Outcome::Killed(KillReason::Shutdown)), "{out:?}");
    });
}

#[test]
fn a_task_asked_to_be_killed_while_parked_on_io_ends_without_waiting_for_the_io() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload("export default async () => { await new Promise((r) => setTimeout(r, 60000)); return new Response('x'); };")).await;
        let (task, fut) = h.start(get("/"), &p);
        let interrupter = h.exec.interrupter();
        let killer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            interrupter.interrupt(task, Interrupt::Kill(KillReason::Shutdown));
        });
        let out = tokio::time::timeout(Duration::from_secs(20), fut)
            .await
            .expect("woken by the kill");
        killer.join().expect("killer thread");
        assert!(matches!(out, Outcome::Killed(KillReason::Shutdown)), "{out:?}");
    });
}

#[test]
fn a_handler_that_never_awaits_a_host_call_answers_from_its_first_turn() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let out = outcome_of(
            &mut h,
            "export default () => new Response(null, { status: 204 });",
            |_| {},
        )
        .await;
        let Outcome::Ok(reply) = out else { panic!("{out:?}") };
        assert_eq!(reply.status.as_u16(), 204);
    });
}

#[test]
fn an_uncaught_buffer_refusal_in_a_timer_is_oom_not_a_guest_error() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let code = "export default async () => { setTimeout(() => { new ArrayBuffer(512 * 1024 * 1024); }, 1); await new Promise(() => {}); };";
        let out = outcome_of(&mut h, code, |s| s.limits.memory_mb = 64).await;
        assert!(matches!(out, Outcome::Killed(KillReason::Oom)), "{out:?}");
        assert!(
            h.log_records().iter().any(|r| r.msg.contains("over the memory limit")),
            "{:?}",
            h.log_records()
        );
    });
}

#[test]
fn a_request_target_with_a_scheme_reaches_the_handler_as_given() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h
            .prepare(&ts_upload("export default (req: Request) => new Response(req.url);"))
            .await;
        let mut req = rt_types::HttpRequest::get("http://proxy.example/forwarded?x=1");
        req.headers.insert("host", "ignored.example".parse().expect("header"));
        let ran = h.run(spec().payload(Payload::Http(req)).finish(), &p).await;
        assert_eq!(read(ran.outcome).await.text(), "http://proxy.example/forwarded?x=1");
    });
}

#[test]
fn console_debug_reaches_the_service_log_at_debug_level() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let out = outcome_of(
            &mut h,
            "export default () => { console.debug('detail'); return new Response('x'); };",
            |_| {},
        )
        .await;
        assert!(matches!(out, Outcome::Ok(_)));
        let logs = h.log_records();
        assert!(
            logs.iter()
                .any(|r| r.msg.contains("detail") && r.level == rt_obs::LogLevel::Debug),
            "{logs:?}"
        );
    });
}

#[test]
fn a_synchronous_env_read_gives_up_at_the_tasks_deadline_and_says_env_unavailable() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        *h.control.env_thread_delay.lock().expect("lock") = Some(Duration::from_millis(600));
        let code = "export default () => { try { return Response.json({ a: (globalThis as any)['De' + 'no'].env.get('A') }); } catch (e) { return Response.json({ error: String(e) }); } };";
        let started = std::time::Instant::now();
        let out = outcome_of(&mut h, code, |s| s.limits.wall = Duration::from_millis(120)).await;
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "the read stopped at the deadline, not at the store's pace"
        );
        let Outcome::Ok(reply) = out else { panic!("{out:?}") };
        let body = reply.body.collect(1 << 20).await.expect("body");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("json"),
            json!({ "error": "Error: env_unavailable" })
        );
    });
}
