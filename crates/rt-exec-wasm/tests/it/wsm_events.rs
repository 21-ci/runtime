//! `std:host/pubsub` and `std:host/streams` from a component (the `events` fixture), over fake
//! buses: results map field for field, the host bounds answer the Deno SDK's codes, a detached
//! handle or a node without the event bus is `unavailable`, and the synthetic deliveries
//! (`POST /__rt/pubsub`, `POST /__rt/stream`) reach the component's handler.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rt_exec_wasm::host_wit::wit::std::host::{pubsub, streams};
use rt_exec_wasm::{FrozenClock, GovernorLimiter, GuardedHttpHooks, GuestLogs, WasmState};
use rt_host::calls::TaskCtx;
use rt_host::{
    AppendEntry, AppendedOut, HostError, PublishOpts, PublishOut, PubsubBus, ReadOpts, ReadPage, ReadStart, Reply,
    RequestOpts, StreamBus, TopicInfo,
};
use rt_mem::{MemCategory, MemoryGovernor as _, Owner};
use rt_types::events::{PubsubMessage, StreamBatch, StreamRecord};
use rt_types::{MsgId, Trigger};
use serde_json::{json, Value};
use wasmtime::component::ResourceTable;
use wasmtime_wasi_http::WasiHttpCtx;

use crate::common::{prepared, Delivery, Req, Rig, MIB};

// ---- fake buses --------------------------------------------------------------------------------

/// One bus call: `(subject, body, headers, timeout_ms)`.
type Seen = (String, Vec<u8>, Vec<(String, String)>, Option<u128>);

#[derive(Default)]
struct FakePubsub {
    /// `(subject, body, headers, timeout_ms)` of each call.
    seen: Mutex<Vec<Seen>>,
}

#[async_trait]
impl PubsubBus for FakePubsub {
    async fn publish(&self, _: &TaskCtx, subject: &str, body: Bytes, o: PublishOpts) -> Result<PublishOut, HostError> {
        self.seen.lock().push((subject.into(), body.to_vec(), o.headers, None));
        Ok(PublishOut {
            receivers: 2,
            dropped: 1,
        })
    }

    async fn request(&self, _: &TaskCtx, subject: &str, body: Bytes, o: RequestOpts) -> Result<Reply, HostError> {
        let t = o.timeout.map(|t| t.as_millis());
        self.seen.lock().push((subject.into(), body.to_vec(), o.headers, t));
        match subject {
            "nobody" => Err(HostError::new("no_responders", "nothing subscribes to that subject")),
            "busy" => Err(HostError::new("rate_limited", "slow down")),
            _ => Ok(Reply {
                content_type: Some("text/plain".into()),
                body: Bytes::from(format!("re:{}", String::from_utf8_lossy(&body))),
            }),
        }
    }
}

/// One-partition topics in memory; `missing` does not exist, `full` refuses appends.
#[derive(Default)]
struct FakeStreams {
    topics: Mutex<HashMap<String, Vec<StreamRecord>>>,
}

#[async_trait]
impl StreamBus for FakeStreams {
    async fn append(&self, _: &TaskCtx, topic: &str, entries: Vec<AppendEntry>) -> Result<Vec<AppendedOut>, HostError> {
        match topic {
            "missing" => return Err(HostError::new("not_found", "no such topic")),
            "full" => return Err(HostError::new("stream_full", "the tenant's streams are full")),
            _ => {}
        }
        let mut topics = self.topics.lock();
        let log = topics.entry(topic.to_string()).or_default();
        let mut out = Vec::new();
        for e in entries {
            if let Some(d) = &e.dedup_id {
                if let Some(r) = log
                    .iter()
                    .find(|r| r.headers.iter().any(|(k, v)| k == "dedup" && v == d))
                {
                    out.push(AppendedOut {
                        partition: 0,
                        offset: r.offset,
                        duplicate: true,
                    });
                    continue;
                }
            }
            let offset = log.len() as u64;
            let mut headers = e.headers;
            if let Some(d) = e.dedup_id {
                headers.push(("dedup".into(), d));
            }
            log.push(StreamRecord {
                partition: 0,
                offset,
                key: e.key,
                headers,
                body: e.body,
                ts_ms: 1000 + offset as i64,
            });
            out.push(AppendedOut {
                partition: 0,
                offset,
                duplicate: false,
            });
        }
        Ok(out)
    }

    async fn read(&self, _: &TaskCtx, topic: &str, o: ReadOpts) -> Result<ReadPage, HostError> {
        if o.partition != Some(0) {
            return Err(HostError::new("validation_failed", "partition is out of range"));
        }
        let topics = self.topics.lock();
        let log = topics
            .get(topic)
            .ok_or_else(|| HostError::new("not_found", "no such topic"))?;
        let end = log.len() as u64;
        let from = match o.start {
            ReadStart::Earliest => 0,
            ReadStart::Latest => end,
            ReadStart::Offset(n) => n,
            ReadStart::TimeMs(t) => log.iter().find(|r| r.ts_ms >= t).map_or(end, |r| r.offset),
        };
        let records: Vec<StreamRecord> = log
            .iter()
            .filter(|r| r.offset >= from)
            .take(o.limit as usize)
            .cloned()
            .collect();
        let next = records.last().map_or(from, |r| r.offset + 1);
        Ok(ReadPage { records, next, end })
    }

    async fn info(&self, _: &TaskCtx, topic: &str) -> Result<TopicInfo, HostError> {
        let topics = self.topics.lock();
        let log = topics
            .get(topic)
            .ok_or_else(|| HostError::new("not_found", "no such topic"))?;
        Ok(TopicInfo {
            name: topic.into(),
            partitions: vec![(0, 0, log.len() as u64)],
            retention_hours: 24,
            max_mb: 64,
            bytes: log.iter().map(|r| r.body.len() as u64).sum(),
        })
    }
}

struct Events {
    rig: Rig,
    pubsub: Arc<FakePubsub>,
}

fn events_rig() -> Events {
    let pubsub = Arc::new(FakePubsub::default());
    let rig = Rig::new()
        .pubsub(pubsub.clone())
        .streams(Arc::new(FakeStreams::default()));
    Events { rig, pubsub }
}

async fn call(rig: &mut Rig, req: Req) -> Value {
    let p = prepared("events");
    let text = rig.run(&p, req).await.text();
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

async fn get(rig: &mut Rig, path: &str) -> Value {
    call(rig, Req::get(path)).await
}

fn code(v: &Value) -> &str {
    v["error"]["code"]
        .as_str()
        .unwrap_or_else(|| panic!("expected an error, got {v}"))
}

// ---- tests -------------------------------------------------------------------------------------

/// `publish` returns the bus's `{receivers, dropped}` and passes subject, body and headers
/// through; `request` returns the reply's content type and body, with the asked timeout
/// (clamped by the handle) handed to the bus.
#[tokio::test]
async fn ac_wsm_evt_01_publish_and_request() {
    let Events { mut rig, pubsub } = events_rig();
    let v = get(&mut rig, "/publish?subject=orders.created&body=hi&h=trace:abc").await;
    assert_eq!(v, json!({"ok": {"receivers": 2, "dropped": 1}}));

    let v = get(&mut rig, "/request?subject=price.get&body=x&timeout_ms=1500").await;
    assert_eq!(v, json!({"ok": {"content_type": "text/plain", "body": "re:x"}}));
    let v = get(&mut rig, "/request?subject=price.get&body=y").await;
    assert_eq!(v["ok"]["body"], "re:y");

    let seen = pubsub.seen.lock();
    assert_eq!(
        (seen[0].0.as_str(), seen[0].1.as_slice(), seen[0].2.clone()),
        (
            "orders.created",
            &b"hi"[..],
            vec![("trace".to_string(), "abc".to_string())]
        )
    );
    assert_eq!(seen[1].3, Some(1500), "the asked timeout reaches the bus");
    let default = seen[2].3.expect("a resolved default timeout");
    assert!(
        default > 0 && default <= 5000,
        "default timeout is 5 s at most (the task deadline may clamp it): {default}"
    );
}

/// Append returns each record's position (a repeated dedup id is a duplicate of the first),
/// read pages from earliest / an offset / a time / latest, info reports the partitions.
#[tokio::test]
async fn ac_wsm_evt_02_append_read_info() {
    let Events { mut rig, .. } = events_rig();
    let v = get(&mut rig, "/append?topic=orders&key=k1&body=a&n=2").await;
    assert_eq!(
        v,
        json!({"ok": [
            {"partition": 0, "offset": 0, "duplicate": false},
            {"partition": 0, "offset": 1, "duplicate": false},
        ]})
    );
    let v = call(&mut rig, Req::put("/append?topic=orders&dedup=d1", b"b")).await;
    assert_eq!(v["ok"][0], json!({"partition": 0, "offset": 2, "duplicate": false}));
    let v = get(&mut rig, "/append?topic=orders&dedup=d1&body=again").await;
    assert_eq!(v["ok"][0], json!({"partition": 0, "offset": 2, "duplicate": true}));

    let v = get(&mut rig, "/read?topic=orders&partition=0&from=earliest&limit=2").await;
    assert_eq!(
        v,
        json!({"ok": {"entries": [
            {"partition": 0, "offset": 0, "key": "k1", "body": "a", "timestamp": 1000},
            {"partition": 0, "offset": 1, "key": "k1", "body": "a", "timestamp": 1001},
        ], "next": 2, "end": 3}})
    );
    let v = get(&mut rig, "/read?topic=orders&partition=0&from=2").await;
    assert_eq!(
        (v["ok"]["entries"][0]["body"].clone(), v["ok"]["next"].clone()),
        (json!("b"), json!(3))
    );
    let v = get(&mut rig, "/read?topic=orders&from=t1001").await;
    assert_eq!(v["ok"]["entries"][0]["offset"], 1);
    let v = get(&mut rig, "/read?topic=orders&from=latest").await;
    assert_eq!(v, json!({"ok": {"entries": [], "next": 3, "end": 3}}));

    let v = get(&mut rig, "/info?topic=orders").await;
    assert_eq!(
        v,
        json!({"ok": {"name": "orders", "partitions": [[0, 0, 3]], "retention_hours": 24, "max_mb": 64, "bytes": 3}})
    );
}

/// The host bounds give the codes of the Deno SDK (validation_failed, too_large, call_depth) and
/// bus refusals keep their stable codes (not_found, stream_full, no_responders, rate_limited).
#[tokio::test]
async fn ac_wsm_evt_03_error_codes_match_deno() {
    let Events { mut rig, pubsub } = events_rig();
    let cases: &[(&str, &str)] = &[
        ("/publish?subject=a.*", "validation_failed"),
        ("/publish?subject=_rt.x", "validation_failed"),
        ("/publish?subject=a&h=x-rt-trace:1", "validation_failed"),
        ("/request?subject=a.*.b", "validation_failed"),
        ("/publish?subject=a..b", "validation_failed"),
        ("/request?subject=nobody", "no_responders"),
        ("/request?subject=busy", "rate_limited"),
        ("/append?topic=Bad", "validation_failed"),
        ("/append?topic=orders&n=0", "validation_failed"),
        ("/append?topic=orders&n=101", "too_large"),
        ("/append?topic=missing", "not_found"),
        ("/append?topic=full", "stream_full"),
        ("/read?topic=orders&limit=0", "validation_failed"),
        ("/read?topic=orders&limit=101", "validation_failed"),
        ("/read?topic=orders&from=t-5", "validation_failed"),
        ("/read?topic=orders&partition=3", "validation_failed"),
        ("/read?topic=nope", "not_found"),
        ("/info?topic=nope", "not_found"),
        ("/info?topic=tpc_x!", "validation_failed"),
    ];
    for (path, want) in cases {
        let v = get(&mut rig, path).await;
        assert_eq!(code(&v), *want, "{path}: {v}");
        assert!(!v["error"]["message"].as_str().unwrap_or_default().is_empty(), "{path}");
    }
    let big = vec![b'x'; 1024 * 1024 + 1];
    let v = call(&mut rig, Req::put("/publish?subject=a", &big)).await;
    assert_eq!(code(&v), "too_large");
    let v = call(&mut rig, Req::put("/append?topic=orders", &big)).await;
    assert_eq!(code(&v), "too_large");

    // A task already MAX_CALL_DEPTH (15) hops deep may not publish (loop guard).
    let deep = Req {
        trigger: Some(Trigger::Call {
            parent: rt_types::TaskId(1),
            depth: 15,
        }),
        ..Req::get("/publish?subject=a")
    };
    let v = call(&mut rig, deep).await;
    assert_eq!(code(&v), "call_depth");

    let calls = pubsub.seen.lock().len();
    assert_eq!(calls, 2, "only `nobody` and `busy` reached the bus");
}

fn detached_state(rig: &Rig) -> WasmState {
    let host = rig.exec.host().detached_handle(rt_testkit::service_ref(1, 1));
    let reservation = rig
        .governor
        .reserve(MemCategory::WasmReuse, MIB, Owner::NONE)
        .expect("reserve");
    WasmState {
        wasi_ctx: wasmtime_wasi::WasiCtxBuilder::new().build(),
        http_ctx: WasiHttpCtx::new(),
        table: ResourceTable::new(),
        limiter: GovernorLimiter::new(reservation, usize::MAX),
        http_hooks: GuardedHttpHooks { host: host.clone() },
        host,
        clock: FrozenClock::new(),
        logs: GuestLogs::new(),
    }
}

/// Start code outside a task (a detached handle) gets `unavailable` from every pubsub / streams
/// import even with buses attached, and so does a task on a node without the event bus.
#[tokio::test]
async fn ac_wsm_evt_04_detached_is_unavailable() {
    let Events { rig, pubsub } = events_rig();
    let mut st = detached_state(&rig);
    let r = pubsub::Host::publish(&mut st, "a".into(), b"x".to_vec(), vec![]).await;
    assert_eq!(r.expect_err("publish").code, "unavailable");
    let r = pubsub::Host::request(&mut st, "a".into(), vec![], vec![], Some(10)).await;
    assert_eq!(r.expect_err("request").code, "unavailable");
    let entry = streams::NewEntry {
        body: b"x".to_vec(),
        key: None,
        headers: vec![],
        dedup_id: None,
    };
    let r = streams::Host::append(&mut st, "t".into(), vec![entry]).await;
    assert_eq!(r.expect_err("append").code, "unavailable");
    let opts = streams::ReadOptions {
        partition: 0,
        start: streams::Position::Earliest,
        limit: 1,
    };
    let r = streams::Host::read(&mut st, "t".into(), opts).await;
    assert_eq!(r.expect_err("read").code, "unavailable");
    let r = streams::Host::info(&mut st, "t".into()).await;
    assert_eq!(r.expect_err("info").code, "unavailable");
    assert!(pubsub.seen.lock().is_empty());

    let mut bare = Rig::new();
    for path in [
        "/publish?subject=a",
        "/request?subject=a",
        "/append?topic=t",
        "/read?topic=t",
        "/info?topic=t",
    ] {
        let v = get(&mut bare, path).await;
        assert_eq!(code(&v), "unavailable", "{path}");
    }
}

/// A pub/sub delivery reaches the handler as `POST /__rt/pubsub` with the message JSON; the
/// response body is what a requester gets back.
#[tokio::test]
async fn ac_wsm_evt_05_pubsub_delivery_reply_body() {
    let mut rig = Rig::new();
    let msg = PubsubMessage {
        id: MsgId::from_u128(7),
        subject: "price.get".into(),
        body: Bytes::from_static(b"{\"sku\":1}"),
        headers: vec![],
        replyable: true,
        published_at_ms: 5,
    };
    let req = Req::delivery(
        Trigger::Pubsub {
            subject: "price.get".into(),
            depth: 1,
            reply: true,
        },
        Delivery::Pubsub(msg),
    );
    let reply = rig.run(&prepared("events"), req).await.reply();
    assert_eq!(reply.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&reply.body).expect("json"),
        json!({"reply": "price.get"})
    );
}

fn batch(first_body: &'static [u8]) -> Req {
    let rec = |offset: u64, body: &'static [u8]| StreamRecord {
        partition: 0,
        offset,
        key: None,
        headers: vec![],
        body: Bytes::from_static(body),
        ts_ms: 1,
    };
    Req::delivery(
        Trigger::Stream {
            topic: "orders".into(),
            partition: 0,
            attempt: 1,
        },
        Delivery::Stream(StreamBatch {
            topic: "orders".into(),
            group: "default".into(),
            partition: 0,
            attempt: 1,
            records: vec![rec(3, first_body), rec(4, b"{}")],
        }),
    )
}

/// A stream batch reaches the handler as `POST /__rt/stream`; the handler's
/// `x-rt-stream-fail-at` header comes back on the reply for the consumer engine to settle on.
#[tokio::test]
async fn ac_wsm_evt_06_stream_delivery_fail_at_header() {
    let mut rig = Rig::new();
    let p = prepared("events");
    let reply = rig.run(&p, batch(b"{\"fail_at\":4}")).await.reply();
    assert_eq!(reply.status, 200);
    assert_eq!(reply.headers["x-rt-stream-fail-at"], "4");

    let reply = rig.run(&p, batch(b"{\"n\":1}")).await.reply();
    assert_eq!((reply.status, reply.text()), (200, "ok".to_string()));
    assert!(reply.headers.get("x-rt-stream-fail-at").is_none());
}
