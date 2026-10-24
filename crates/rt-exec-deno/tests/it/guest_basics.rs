//! Guest contract basics: DNO-01, DNO-02,
//! DNO-09, DNO-12, plus Form B handlers and multi-file bundles.
//!
//! Every test builds its fixture through `DefaultBuilder` and runs it via `Executor::start`
//! (see `common/mod.rs`).

use crate::common;

use common::*;
use rt_types::{HttpRequest, MsgId, Outcome, Payload, QueueMessage, ScheduleId, ScheduledEvent, Trigger};
use serde_json::json;

fn looks_like_uuid(s: &str) -> bool {
    s.len() == 36 && s.matches('-').count() == 4 && s.chars().all(|c| c == '-' || c.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------------------------
// DNO-01 / DNO-02: one fresh isolate per task
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_01_global_state_never_survives_a_task() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("global_counter").await;
        for i in 0..3 {
            let (got, _) = h.get("/", &p).await;
            assert_eq!(got.status, 200, "request {i}");
            assert_eq!(
                got.text(),
                "1",
                "request {i}: globalThis.x must start from nothing in every task"
            );
        }
    });
}

#[test]
fn ac_dno_02_top_level_code_runs_once_per_task() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("toplevel_uuid").await;
        let (a, _) = h.get("/", &p).await;
        let (b, _) = h.get("/", &p).await;
        assert!(looks_like_uuid(&a.text()), "got {:?}", a.text());
        assert!(looks_like_uuid(&b.text()), "got {:?}", b.text());
        assert_ne!(
            a.text(),
            b.text(),
            "top-level `crypto.randomUUID()` must be re-evaluated per task"
        );
    });
}

#[test]
fn dno_hello_returns_the_handler_response() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("hello").await;
        let (got, _) = h.get("/anything?x=1", &p).await;
        assert_eq!(got.status, 200);
        assert_eq!(got.text(), "hello");
    });
}

#[test]
fn dno_tasks_of_different_services_do_not_share_state() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("global_counter").await;
        for svc_n in [1u128, 2, 1, 3] {
            let ran = h.run(spec().service(svc_n).http_get("/").finish(), &p).await;
            assert_eq!(read(ran.outcome).await.text(), "1");
        }
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-09 guest errors
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_09_throwing_guest_is_a_guest_error_and_the_log_has_stack_and_task_id() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("throw").await;
        let ran = h.run(get("/"), &p).await;

        // `Outcome::GuestError` maps to `500 x-rt-reason: guest_error` with a generic body: the
        // outcome carries no message at all, so "boom" cannot reach the client.
        assert!(matches!(ran.outcome, Outcome::GuestError), "got {:?}", ran.outcome);
        assert_eq!(ran.outcome.http_status(), 500);
        assert_eq!(ran.outcome.reason(), Some("guest_error"));

        h.settle(std::time::Duration::from_millis(30)).await;
        let recs = h.log_records();
        let rec = recs
            .iter()
            .find(|r| r.msg.contains("boom"))
            .unwrap_or_else(|| panic!("the error message goes to the service log, got {recs:?}"));
        assert!(
            rec.msg.contains("    at "),
            "log record carries the stack: {:?}",
            rec.msg
        );
        assert_eq!(rec.task_id, Some(ran.task), "log record is tagged with the task id");
        assert_eq!(rec.level, rt_obs::LogLevel::Error);
    });
}

#[test]
fn dno_next_task_after_a_guest_error_runs_normally() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let bad = h.prepare_fixture("throw").await;
        let good = h.prepare_fixture("hello").await;
        assert!(matches!(h.run(get("/"), &bad).await.outcome, Outcome::GuestError));
        let (got, _) = h.get("/", &good).await;
        assert_eq!(got.text(), "hello");
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-12 non-HTTP triggers on Form A handlers
// ---------------------------------------------------------------------------------------------

fn scheduled_spec(trigger: Trigger, id: ScheduleId, payload: serde_json::Value) -> rt_types::TaskSpec {
    spec()
        .trigger(trigger)
        .payload(Payload::Schedule(ScheduledEvent {
            schedule_id: id,
            scheduled_time_ms: 1_700_000_000_000,
            payload,
        }))
        .finish()
}

#[test]
fn ac_dno_12_form_a_receives_interval_as_a_synthetic_post() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("form_a_scheduled").await;
        let id = ScheduleId::from_u128(42);
        let ran = h
            .run(
                scheduled_spec(Trigger::Interval { id }, id, json!({"k": "v", "n": [1, 2]})),
                &p,
            )
            .await;

        let got = read(ran.outcome).await;
        assert_eq!(got.status, 200);
        let v = got.json();
        assert_eq!(v["method"], "POST");
        assert_eq!(v["path"], "/__rt/scheduled");
        assert_eq!(v["trigger"], "interval", "x-rt-trigger header");
        assert!(v["contentType"].as_str().unwrap_or("").contains("application/json"));
        assert_eq!(v["body"]["scheduleId"], id.to_string());
        assert_eq!(v["body"]["scheduledTime"], 1_700_000_000_000i64);
        assert_eq!(v["body"]["payload"], json!({"k": "v", "n": [1, 2]}));
    });
}

#[test]
fn ac_dno_12_cron_trigger_header_says_cron() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("form_a_scheduled").await;
        let id = ScheduleId::from_u128(43);
        let ran = h.run(scheduled_spec(Trigger::Cron { id }, id, json!(null)), &p).await;
        let v = read(ran.outcome).await.json();
        assert_eq!(v["path"], "/__rt/scheduled");
        assert_eq!(v["trigger"], "cron");
    });
}

#[test]
fn dno_form_a_receives_queue_batches_as_a_synthetic_post() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("form_a_scheduled").await;
        let (m1, m2) = (MsgId::from_u128(1), MsgId::from_u128(2));
        let spec = spec()
            .trigger(Trigger::Queue {
                batch: vec![m1, m2],
                attempt: 2,
            })
            .payload(Payload::Queue(vec![
                QueueMessage {
                    id: m1,
                    body: bytes::Bytes::from_static(br#"{"n":1}"#),
                    attempt: 2,
                },
                QueueMessage {
                    id: m2,
                    body: bytes::Bytes::from_static(br#"{"n":2}"#),
                    attempt: 1,
                },
            ]))
            .finish();
        let got = read(h.run(spec, &p).await.outcome).await;
        assert_eq!(got.status, 200, "a 2xx answer acks the whole batch");
        let v = got.json();
        assert_eq!(v["method"], "POST");
        assert_eq!(v["path"], "/__rt/queue");
        assert_eq!(v["trigger"], "queue");
        let msgs = v["body"]["messages"].as_array().expect("messages");
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["id"], m1.to_string());
        assert_eq!(msgs[0]["body"], json!({"n": 1}));
        assert_eq!(msgs[0]["attempt"], 2);
        assert_eq!(msgs[1]["body"], json!({"n": 2}));
    });
}

#[test]
fn dno_form_a_still_serves_plain_http() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("form_a_scheduled").await;
        let spec = spec()
            .payload(post_payload("/hook?x=1", "application/json", r#"{"a":1}"#))
            .finish();
        let v = read(h.run(spec, &p).await.outcome).await.json();
        assert_eq!(v["method"], "POST");
        assert_eq!(v["path"], "/hook");
        assert_eq!(
            v["trigger"],
            serde_json::Value::Null,
            "plain HTTP carries no x-rt-trigger"
        );
        assert_eq!(v["body"], json!({"a": 1}));
    });
}

// ---------------------------------------------------------------------------------------------
// Form B
// ---------------------------------------------------------------------------------------------

fn handler_log(h: &Harness, handler: &str) -> serde_json::Value {
    h.log_records()
        .iter()
        .filter_map(|r| serde_json::from_str::<serde_json::Value>(&r.msg).ok())
        .find(|v| v["handler"] == handler)
        .unwrap_or_else(|| panic!("no `{handler}` log line in {:?}", h.log_records()))
}

#[test]
fn dno_form_b_fetch_sees_ctx_trigger_http() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("form_b").await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.text(), "b:http");
    });
}

#[test]
fn dno_form_b_scheduled_handler_gets_the_event_directly() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("form_b").await;
        let id = ScheduleId::from_u128(7);
        let ran = h
            .run(scheduled_spec(Trigger::Interval { id }, id, json!({"p": 1})), &p)
            .await;
        let got = read(ran.outcome).await;
        assert!(
            (200..300).contains(&got.status),
            "void handler => 2xx, got {}",
            got.status
        );
        h.settle(std::time::Duration::from_millis(30)).await;

        let line = handler_log(&h, "scheduled");
        assert_eq!(line["trigger"], "interval");
        assert_eq!(line["event"]["scheduleId"], id.to_string());
        assert_eq!(line["event"]["scheduledTime"], 1_700_000_000_000i64);
        assert_eq!(line["event"]["payload"], json!({"p": 1}));
    });
}

#[test]
fn dno_form_b_queue_handler_gets_a_batch_and_acks() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("form_b").await;
        let m1 = MsgId::from_u128(11);
        let spec = spec()
            .trigger(Trigger::Queue {
                batch: vec![m1],
                attempt: 1,
            })
            .payload(Payload::Queue(vec![QueueMessage {
                id: m1,
                body: bytes::Bytes::from_static(br#"{"job":"x"}"#),
                attempt: 1,
            }]))
            .finish();
        let got = read(h.run(spec, &p).await.outcome).await;
        assert!(
            (200..300).contains(&got.status),
            "all messages acked => 2xx, got {}",
            got.status
        );
        h.settle(std::time::Duration::from_millis(30)).await;

        let line = handler_log(&h, "queue");
        assert_eq!(line["trigger"], "queue");
        assert_eq!(line["messages"][0]["id"], m1.to_string());
        assert_eq!(line["messages"][0]["body"], json!({"job": "x"}));
        assert_eq!(line["messages"][0]["attempt"], 1);
    });
}

// ---------------------------------------------------------------------------------------------
// Bundle-only module loading
// ---------------------------------------------------------------------------------------------

#[test]
fn dno_multi_file_bundle_runs_with_json_alias_and_dynamic_import() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare_fixture("multi_file").await;
        let (got, _) = h.get("/?name=ada", &p).await;
        assert_eq!(got.status, 200);
        let v = got.json();
        assert_eq!(v["message"], "HELLO, ADA");
        assert_eq!(v["lazy"], "lazy-loaded");
        let (dflt, _) = h.get("/", &p).await;
        assert_eq!(dflt.json()["message"], "HELLO, WORLD", "default comes from data.json");
    });
}

#[test]
fn dno_request_url_headers_and_body_reach_the_guest() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let upload = ts_upload(
            "export default async (req: Request) => Response.json({ m: req.method, p: new URL(req.url).pathname, \
             q: new URL(req.url).searchParams.get('a'), h: req.headers.get('x-test'), b: await req.text() });\n",
        );
        let p = h.prepare(&upload).await;
        let mut req = HttpRequest::get("/x/y?a=1");
        req.method = http::Method::PUT;
        req.headers.insert("x-test", "yes".parse().expect("value"));
        req.body = rt_types::Body::from_bytes(bytes::Bytes::from_static(b"payload"));
        let got = read(h.run(spec().payload(Payload::Http(req)).finish(), &p).await.outcome).await;
        assert_eq!(
            got.json(),
            json!({"m": "PUT", "p": "/x/y", "q": "1", "h": "yes", "b": "payload"})
        );
    });
}

#[test]
fn dno_response_status_and_headers_are_the_guests() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let upload = ts_upload(
            "export default () => new Response('teapot', { status: 418, headers: { 'x-custom': 'v', 'content-type': 'text/plain' } });\n",
        );
        let p = h.prepare(&upload).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 418);
        assert_eq!(got.headers.get("x-custom").and_then(|v| v.to_str().ok()), Some("v"));
        assert_eq!(got.text(), "teapot");
    });
}

#[test]
fn dno_ctx_service_carries_the_slug_of_the_task_spec() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let upload = ts_upload(
            "export default (_req: Request, ctx: any) => Response.json({ slug: ctx.service.slug, \
             version: ctx.service.version, id: typeof ctx.service.id });\n",
        );
        let p = h.prepare(&upload).await;
        // `Specs::spec` (rt-server) fills `TaskSpec.slug` for every trigger; the guest sees it as is.
        let mut s = get("/");
        s.slug = "orders-api".into();
        let got = read(h.run(s, &p).await.outcome).await;
        assert_eq!(got.json()["slug"], json!("orders-api"));
        assert_eq!(got.json()["id"], json!("string"));

        let id = ScheduleId::from_u128(7);
        let mut q = scheduled_spec(Trigger::Interval { id }, id, json!({}));
        q.slug = "orders-api".into();
        let got = read(h.run(q, &p).await.outcome).await;
        assert_eq!(
            got.json()["slug"],
            json!("orders-api"),
            "non-HTTP triggers carry it too"
        );
    });
}
