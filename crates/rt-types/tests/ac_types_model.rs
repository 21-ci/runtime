//! rt-types shared model behavior: digests, task limits per trigger,
//! `TaskSpec` construction, kind/profile/scope wire forms, bodies.

use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use rt_types::{
    body_channel, scope_admits, Body, BodyError, BuildReport, CompileMode, ConfigStream, ContractTarget, ControlPlane,
    CpError, Digest, DigestError, EnvBundle, Etag, HttpRequest, KeyId, KeySecret, Limits, MsgId, NewSchedule,
    NodeEvent, ObjectRef, Payload, Plan, PreparedKey, Profile, ProjectId, RouteSnapshot, RuntimeKind, ScalingConfig,
    ScheduleId, ScheduleKind, Scope, Scopes, SecretsVersion, Service, ServiceId, ServiceKnobs, ServiceRef, TaskId,
    TaskKind, TaskSpec, TenantId, TraceCtx, Trigger, TriggerKind, TriggerSwitches, UploadValidator, UsageBatch,
    ValidatedUpload, ValidationError, VersionNo, WsServiceConfig, MAX_CALL_DEPTH,
};
use rt_types::{DegradationMode, GrqLens, ProcId, ProcSnapshot, ProcStatus, SchedSnapshot};
use std::sync::Arc;

// ---------------------------------------------------------------------------------------------
// Digest
// ---------------------------------------------------------------------------------------------

#[test]
fn digest_of_matches_known_sha256_vectors() {
    assert_eq!(
        Digest::of(b"").to_string(),
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        Digest::of(b"abc").to_string(),
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        Digest::of(b"abc").hex(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn digest_string_round_trip_and_serde() {
    let d = Digest::of(b"hello world");
    let s = d.to_string();
    assert_eq!(s.parse::<Digest>().unwrap(), d);
    assert_eq!(Digest::from_hex(&d.hex()).unwrap(), d);
    assert_eq!(serde_json::to_string(&d).unwrap(), format!("\"{s}\""));
    assert_eq!(serde_json::from_str::<Digest>(&format!("\"{s}\"")).unwrap(), d);
    assert_eq!(format!("{d:?}"), s);
    assert_ne!(Digest::of(b"a"), Digest::of(b"b"));
}

#[test]
fn digest_rejects_malformed_input() {
    let good = Digest::of(b"x").hex();
    for bad in [
        String::new(),
        good.clone(),                         // missing `sha256:` prefix
        format!("sha1:{good}"),               // wrong algorithm
        format!("sha256:{}", &good[..63]),    // too short
        format!("sha256:{good}0"),            // too long
        format!("sha256:{}", "g".repeat(64)), // not hex
        format!("SHA256:{good}"),             // prefix is case sensitive
    ] {
        assert!(
            matches!(bad.parse::<Digest>(), Err(DigestError::Malformed(_))),
            "`{bad}` must be rejected"
        );
    }
    assert!(matches!(Digest::from_hex("zz"), Err(DigestError::Malformed(_))));
    assert!(serde_json::from_str::<Digest>("\"nope\"").is_err());
    // upper-case hex is still hex
    assert_eq!(Digest::from_hex(&good.to_uppercase()).unwrap(), Digest::of(b"x"));
}

// ---------------------------------------------------------------------------------------------
// Limits per trigger (table, `balanced` defaults)
// ---------------------------------------------------------------------------------------------

fn key() -> PreparedKey {
    PreparedKey {
        digest: Digest::of(b"code"),
        kind: RuntimeKind::Deno,
        engine_fp: 7,
    }
}

#[test]
fn http_limits_match_the_documented_defaults() {
    let l = Limits::defaults_for(&Trigger::Http);
    assert_eq!(l, Limits::default());
    assert_eq!(l.wall, Duration::from_secs(30));
    assert_eq!(l.cpu, Duration::from_millis(1_000));
    assert_eq!(l.memory_mb, 128);
    assert_eq!(l.max_request_body_bytes, 10 * 1024 * 1024);
    assert_eq!(l.max_response_buffered_bytes, 10 * 1024 * 1024);
    assert_eq!(l.max_response_stream_bytes, 100 * 1024 * 1024);
    assert_eq!(l.max_fetches, 50);
    assert_eq!(l.max_concurrent_fetches, 6);
    assert_eq!(l.max_log_lines, 1_000);
}

#[test]
fn trigger_limit_table_wall_cpu_fetches_body() {
    // (trigger, wall s, cpu ms, memory MB, request body bytes, fetches)  -- docs/handlers.md "Limits"
    let sched = ScheduleId::new();
    let rows: Vec<(Trigger, u64, u64, u32, u64, u32)> = vec![
        (Trigger::Http, 30, 1_000, 128, 10 * 1024 * 1024, 50),
        (Trigger::Interval { id: sched }, 300, 10_000, 128, 10 * 1024 * 1024, 200),
        (Trigger::Cron { id: sched }, 300, 10_000, 128, 10 * 1024 * 1024, 200),
        (
            Trigger::Queue {
                batch: vec![MsgId::new()],
                attempt: 1,
            },
            300,
            10_000,
            128,
            1024 * 1024,
            200,
        ),
        (
            Trigger::Call {
                parent: TaskId(1),
                depth: 2,
            },
            30,
            1_000,
            128,
            10 * 1024 * 1024,
            50,
        ),
    ];
    for (trigger, wall_s, cpu_ms, mem, body, fetches) in rows {
        let l = Limits::defaults_for(&trigger);
        let name = trigger.label();
        assert_eq!(l.wall, Duration::from_secs(wall_s), "{name} wall");
        assert_eq!(l.cpu, Duration::from_millis(cpu_ms), "{name} cpu");
        assert_eq!(l.memory_mb, mem, "{name} memory");
        assert_eq!(l.max_request_body_bytes, body, "{name} request body");
        assert_eq!(l.max_fetches, fetches, "{name} fetches");
        assert_eq!(l.max_concurrent_fetches, 6, "{name} concurrent fetches");
    }
}

#[test]
fn build_trigger_has_its_own_budget() {
    let l = Limits::defaults_for(&Trigger::Build { key: key() });
    assert_eq!(l.wall, Duration::from_secs(120));
    assert_eq!(l.cpu, Duration::from_secs(120));
    assert_eq!(l.memory_mb, 256);
}

#[test]
fn call_depth_is_reported_by_the_trigger() {
    assert_eq!(Trigger::Http.call_depth(), 0);
    assert_eq!(
        Trigger::Call {
            parent: TaskId(9),
            depth: 4
        }
        .call_depth(),
        4
    );
    assert_eq!(MAX_CALL_DEPTH, 15);
}

#[test]
fn trigger_labels_are_the_metric_and_log_vocabulary() {
    let s = ScheduleId::new();
    let cases: Vec<(Trigger, &str)> = vec![
        (Trigger::Http, "http"),
        (Trigger::Interval { id: s }, "interval"),
        (Trigger::Cron { id: s }, "cron"),
        (
            Trigger::Queue {
                batch: vec![],
                attempt: 1,
            },
            "queue",
        ),
        (
            Trigger::Call {
                parent: TaskId(1),
                depth: 1,
            },
            "call",
        ),
        (Trigger::Build { key: key() }, "build"),
        (Trigger::Scheduled { id: "once-1".into() }, "schedule"),
        (Trigger::Socket { object: "room".into() }, "websocket"),
        (Trigger::Alarm { object: "room".into() }, "alarm"),
    ];
    for (t, l) in cases {
        assert_eq!(t.label(), l);
    }
}

#[test]
fn service_knob_defaults_are_the_balanced_profile() {
    let k = ServiceKnobs::default();
    assert_eq!(k.queue_timeout, Duration::from_secs(5));
    assert_eq!(k.idle_ttl, Duration::from_secs(600));
    assert_eq!(k.compile, CompileMode::Lazy);
    assert!(!k.preload && !k.reuse_instances && !k.keep_resident);
}

/// D-B2: `scaling.min_instances` / `max_instances` are the instance bounds; `max_concurrency` is
/// the pre-D-B2 name of `max_instances` and is still accepted on input, never written.
#[test]
fn scaling_config_instance_bounds_and_the_legacy_alias() {
    use rt_types::ScalingConfig;
    let c: ScalingConfig = serde_json::from_value(serde_json::json!({"min_instances": 2, "max_instances": 8})).unwrap();
    assert_eq!((c.min_instances, c.max_instances), (Some(2), Some(8)));
    let legacy: ScalingConfig = serde_json::from_value(serde_json::json!({"max_concurrency": 5})).unwrap();
    assert_eq!(legacy.max_instances, Some(5), "the old name keeps working on input");
    let out = serde_json::to_value(&legacy).unwrap();
    assert_eq!(out["max_instances"], 5);
    assert!(
        out.get("max_concurrency").is_none(),
        "only the new name is written: {out}"
    );
    assert!(
        out.get("min_instances").is_some_and(serde_json::Value::is_null),
        "unset bounds are null: {out}"
    );
    assert!(
        serde_json::from_value::<ScalingConfig>(serde_json::json!({"max_concurrency": 5, "max_instances": 6})).is_err(),
        "both names at once are ambiguous"
    );
}

#[test]
fn service_knobs_default_to_zero_min_and_the_balanced_ceiling() {
    let k = ServiceKnobs::default();
    assert_eq!((k.min_instances, k.max_instances), (0, 1024));
    assert_eq!(
        rt_types::Plan::default().max_instances,
        4,
        "the built-in free plan allows 4 instances per service"
    );
}

// ---------------------------------------------------------------------------------------------
// TaskSpec / PreparedKey
// ---------------------------------------------------------------------------------------------

fn service_ref() -> ServiceRef {
    ServiceRef {
        tenant: TenantId::new(),
        service: ServiceId::new(),
        version: VersionNo(3),
        digest: Digest::of(b"src"),
    }
}

#[test]
fn http_task_spec_wires_kind_trigger_payload_and_limits() {
    let sr = service_ref();
    let spec = TaskSpec::http(RuntimeKind::Wasm, sr, HttpRequest::get("/hello?x=1"));
    assert_eq!(spec.kind, TaskKind::WASM);
    assert_eq!(spec.trigger, Trigger::Http);
    assert_eq!(spec.limits, Limits::defaults_for(&Trigger::Http));
    match &spec.payload {
        Payload::Http(req) => {
            assert_eq!(req.method, http::Method::GET);
            assert_eq!(req.uri.path(), "/hello");
            assert_eq!(req.uri.query(), Some("x=1"));
        }
        other => panic!("expected an http payload, got {other:?}"),
    }
    assert!(spec.trace.is_none());
}

#[test]
fn prepared_key_binds_digest_runtime_kind_and_engine_fingerprint() {
    let sr = service_ref();
    let spec = TaskSpec::http(RuntimeKind::Deno, sr, HttpRequest::get("/"));
    let k = spec.prepared_key(42).expect("exec tasks need a prepared artifact");
    assert_eq!(
        k,
        PreparedKey {
            digest: sr.digest,
            kind: RuntimeKind::Deno,
            engine_fp: 42
        }
    );
    assert_ne!(spec.prepared_key(43), Some(k), "engine fingerprint is part of the key");

    let build = TaskSpec::build(sr, k);
    assert_eq!(build.kind, TaskKind::Build);
    assert_eq!(build.prepared_key(42), None, "builds do not read T1");
    assert_eq!(build.limits, Limits::defaults_for(&Trigger::Build { key: k }));
}

#[test]
fn prepared_key_hash64_is_stable_and_sensitive_to_every_field() {
    let k = key();
    assert_eq!(k.hash64(), k.hash64());
    // Pinned FNV-1a value (32 zero digest bytes, kind 0, engine_fp 0): persisted frequency
    // sketches depend on this hash staying stable across releases.
    let zero = PreparedKey {
        digest: Digest([0; 32]),
        kind: RuntimeKind::Deno,
        engine_fp: 0,
    };
    assert_eq!(zero.hash64(), 0x230a_d4fd_729e_c93f);
    let variants = [
        PreparedKey {
            digest: Digest::of(b"other"),
            ..k
        },
        PreparedKey {
            kind: RuntimeKind::Wasm,
            ..k
        },
        PreparedKey { engine_fp: 8, ..k },
    ];
    for v in variants {
        assert_ne!(v.hash64(), k.hash64(), "{v:?} collides with {k:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// Wire forms
// ---------------------------------------------------------------------------------------------

#[test]
fn profile_names_round_trip_through_str_and_serde() {
    for (p, s) in [
        (Profile::MaxDensity, "max_density"),
        (Profile::Balanced, "balanced"),
        (Profile::MinLatency, "min_latency"),
    ] {
        assert_eq!(p.to_string(), s);
        assert_eq!(s.parse::<Profile>().unwrap(), p);
        assert_eq!(serde_json::to_string(&p).unwrap(), format!("\"{s}\""));
        assert_eq!(serde_json::from_str::<Profile>(&format!("\"{s}\"")).unwrap(), p);
    }
    assert_eq!(Profile::default(), Profile::Balanced);
    assert_eq!(Profile::ALL.len(), 3);
    assert!("Balanced".parse::<Profile>().is_err());
    assert!("max-density".parse::<Profile>().is_err());
    assert!(serde_json::from_str::<Profile>("\"fast\"").is_err());
}

#[test]
fn task_kind_indexes_are_dense_and_strings_round_trip() {
    let mut idx: Vec<usize> = TaskKind::ALL.iter().map(|k| k.idx()).collect();
    idx.sort_unstable();
    assert_eq!(idx, vec![0, 1, 2]);
    assert_eq!(TaskKind::ALL.len(), TaskKind::COUNT);
    for (k, s) in [
        (TaskKind::DENO, "deno"),
        (TaskKind::WASM, "wasm"),
        (TaskKind::Build, "build"),
    ] {
        assert_eq!(k.to_string(), s);
        assert_eq!(s.parse::<TaskKind>().unwrap(), k);
        assert_eq!(serde_json::to_string(&k).unwrap(), format!("\"{s}\""));
        assert_eq!(serde_json::from_str::<TaskKind>(&format!("\"{s}\"")).unwrap(), k);
    }
    assert!("python".parse::<TaskKind>().is_err());
    assert_eq!("deno".parse::<RuntimeKind>().unwrap(), RuntimeKind::Deno);
    assert!(
        "build".parse::<RuntimeKind>().is_err(),
        "build is a task kind, not a runtime"
    );
}

#[test]
fn scopes_use_the_documented_names_on_the_wire() {
    // Scopes (docs/api.md)
    let documented = [
        "services:read",
        "services:write",
        "deploy",
        "invoke",
        "logs:read",
        "env:write",
        "secrets:write",
        "queue:write",
        "data:admin",
        "keys:write",
        "sockets:write",
    ];
    assert_eq!(Scope::ALL.len(), documented.len());
    for name in documented {
        let scope: Scope = name.parse().unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(scope.to_string(), name);
        assert_eq!(serde_json::to_string(&scope).unwrap(), format!("\"{name}\""));
        assert_eq!(serde_json::from_str::<Scope>(&format!("\"{name}\"")).unwrap(), scope);
    }
    assert!("services".parse::<Scope>().is_err());
    assert!("Deploy".parse::<Scope>().is_err());
    assert!(serde_json::from_str::<Scope>("\"root\"").is_err());
}

#[test]
fn scope_sets_serialize_as_sorted_arrays_and_check_membership() {
    let s: Scopes = [Scope::Invoke, Scope::Deploy].into_iter().collect();
    assert!(s.contains(Scope::Invoke) && s.contains(Scope::Deploy));
    assert!(!s.contains(Scope::DataAdmin));
    let json = serde_json::to_string(&s).unwrap();
    let back: Scopes = serde_json::from_str(&json).unwrap();
    assert_eq!(back, s);
    assert!(
        json.starts_with('[') && json.contains("\"deploy\"") && json.contains("\"invoke\""),
        "{json}"
    );
    assert_eq!(Scopes::all().iter().count(), 11);
    assert!(!Scopes::default().contains(Scope::Invoke));
    assert!(serde_json::from_str::<Scopes>("[\"invoke\",\"bogus\"]").is_err());
}

#[test]
fn etag_header_forms() {
    assert_eq!(Etag(7).to_header(), "\"7\"");
    for (h, want) in [
        ("\"7\"", Some(Etag(7))),
        ("W/\"7\"", Some(Etag(7))),
        ("7", Some(Etag(7))),
        ("  \"12\"  ", Some(Etag(12))),
        ("\"x\"", None),
        ("", None),
        ("\"-1\"", None),
    ] {
        assert_eq!(Etag::parse_header(h), want, "`{h}`");
    }
}

#[test]
fn control_plane_key_id_bare_form_round_trips() {
    // `rtk_<bare key id>_<secret>`: the embedded form is the bare ULID of the `key_...` id.
    let id = KeyId::new();
    assert_eq!(KeyId::from_bare(&id.bare()).unwrap(), id);
    assert_eq!(id.to_string(), format!("key_{}", id.bare()));
}

// ---------------------------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn buffered_body_collects_and_knows_its_length() {
    let b = Body::from("hello");
    assert_eq!(b.exact_len(), Some(5));
    assert_eq!(b.collect(1024).await.unwrap(), Bytes::from_static(b"hello"));
    assert_eq!(Body::empty().exact_len(), Some(0));
    assert!(Body::empty().collect(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn collect_enforces_the_limit_exactly() {
    assert_eq!(Body::from("12345").collect(5).await.unwrap().len(), 5);
    assert_eq!(
        Body::from("123456").collect(5).await,
        Err(BodyError::TooLarge { limit: 5 })
    );
    // limit applies across chunks
    let chunks = futures_util::stream::iter(vec![Ok(Bytes::from_static(b"abc")), Ok(Bytes::from_static(b"def"))]);
    assert_eq!(
        Body::from_stream(chunks).collect(5).await,
        Err(BodyError::TooLarge { limit: 5 })
    );
}

#[tokio::test]
async fn body_channel_delivers_in_order_and_propagates_aborts() {
    let (tx, body) = body_channel(2);
    let producer = tokio::spawn(async move {
        tx.send(Bytes::from_static(b"one ")).await.unwrap();
        tx.send(Bytes::from_static(b"two")).await.unwrap();
        tx.abort(BodyError::Aborted("client went away".into())).await;
    });
    let mut stream = body.into_stream();
    assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from_static(b"one "));
    assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from_static(b"two"));
    assert_eq!(
        stream.next().await.unwrap(),
        Err(BodyError::Aborted("client went away".into()))
    );
    producer.await.unwrap();
}

#[tokio::test]
async fn body_channel_applies_backpressure_and_detects_a_dropped_consumer() {
    let (tx, body) = body_channel(1);
    tx.send(Bytes::from_static(b"a")).await.unwrap();
    // channel is full: the next send must not complete until the consumer reads
    let blocked = tokio::time::timeout(Duration::from_millis(50), tx.send(Bytes::from_static(b"b"))).await;
    assert!(blocked.is_err(), "send into a full channel must wait (backpressure)");
    assert!(!tx.is_closed());
    drop(body);
    assert!(tx.is_closed());
    assert!(matches!(
        tx.send(Bytes::from_static(b"c")).await,
        Err(BodyError::Aborted(_))
    ));
}

#[tokio::test]
async fn empty_bytes_make_an_empty_body_with_a_known_zero_length() {
    let b = Body::from_bytes(Bytes::new());
    assert_eq!(b.exact_len(), Some(0));
    assert!(b.collect(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn sized_stream_reports_the_declared_length_and_yields_its_chunks() {
    let chunks = futures_util::stream::iter(vec![Ok(Bytes::from_static(b"ab")), Ok(Bytes::from_static(b"cd"))]);
    let b = Body::from_stream_sized(chunks, 4);
    assert_eq!(b.exact_len(), Some(4));
    assert_eq!(b.collect(4).await.unwrap(), Bytes::from_static(b"abcd"));
}

#[tokio::test]
async fn default_body_is_empty_with_zero_length() {
    let b = Body::default();
    assert_eq!(b.exact_len(), Some(0));
    assert!(b.collect(0).await.unwrap().is_empty());
}

#[test]
fn debug_shows_the_exact_length_when_known() {
    assert!(format!("{:?}", Body::from("abc")).contains("exact_len: Some(3)"));
    let open = Body::from_stream(futures_util::stream::empty::<Result<Bytes, BodyError>>());
    assert!(format!("{:?}", open).contains("exact_len: None"));
}

#[tokio::test]
async fn body_is_a_stream_of_its_chunks() {
    let mut body = Body::from("hi");
    assert_eq!(body.next().await.unwrap().unwrap(), Bytes::from_static(b"hi"));
    assert!(body.next().await.is_none());
}

#[tokio::test]
async fn conversions_from_bytes_string_and_vec_produce_the_same_body() {
    assert_eq!(
        Body::from(Bytes::from_static(b"x")).collect(8).await.unwrap(),
        Bytes::from_static(b"x")
    );
    assert_eq!(
        Body::from(String::from("yz")).collect(8).await.unwrap(),
        Bytes::from_static(b"yz")
    );
    assert_eq!(
        Body::from(vec![1u8, 2]).collect(8).await.unwrap(),
        Bytes::from_static(&[1, 2])
    );
    assert_eq!(Body::from(String::new()).exact_len(), Some(0));
}

// ---------------------------------------------------------------------------------------------
// Tenant, service and node model helpers
// ---------------------------------------------------------------------------------------------

#[test]
fn key_secret_is_redacted_in_debug_and_exposed_only_on_request() {
    let secret = KeySecret::new("s3cret-value".to_string());
    assert_eq!(secret.expose(), "s3cret-value");
    assert_eq!(format!("{secret:?}"), "KeySecret(<redacted>)");
}

#[test]
fn scope_admits_checks_project_and_environment_lists_when_given() {
    let p = ProjectId::from_u128(1);
    let q = ProjectId::from_u128(2);
    assert!(scope_admits(None, None, p, "prod"));
    assert!(scope_admits(Some(&[p][..]), Some(&["prod".to_string()][..]), p, "prod"));
    assert!(!scope_admits(Some(&[q][..]), None, p, "prod"));
    assert!(!scope_admits(None, Some(&["staging".to_string()][..]), p, "prod"));
}

#[test]
fn scaling_rejects_a_min_above_a_positive_max_only() {
    let cfg = |min: Option<u32>, max: Option<u32>| ScalingConfig {
        min_instances: min,
        max_instances: max,
        ..Default::default()
    };
    let err = cfg(Some(4), Some(2)).validate().unwrap_err();
    assert!(
        err.contains("`scaling.min_instances` (4) is greater than `scaling.max_instances` (2)"),
        "{err}"
    );
    assert!(cfg(Some(2), Some(2)).validate().is_ok());
    assert!(cfg(Some(4), None).validate().is_ok());
    assert!(
        cfg(Some(4), Some(0)).validate().is_ok(),
        "max 0 is the plan default: nothing to contradict"
    );
}

#[test]
fn trigger_switches_default_on_and_each_can_be_turned_off() {
    let all = [
        TriggerKind::Http,
        TriggerKind::Interval,
        TriggerKind::Cron,
        TriggerKind::Queue,
        TriggerKind::Call,
        TriggerKind::Websocket,
    ];
    assert!(all.iter().all(|t| TriggerSwitches::default().enabled(*t)));
    let off = TriggerSwitches {
        cron: false,
        ..TriggerSwitches::default()
    };
    assert_eq!(
        all.iter().map(|t| off.enabled(*t)).collect::<Vec<_>>(),
        vec![true, true, false, true, true, true]
    );
    assert!(!TriggerSwitches {
        websocket: false,
        ..Default::default()
    }
    .enabled(TriggerKind::Websocket));
}

#[test]
fn websocket_warm_defaults_on_and_origins_match_exactly_ignoring_case_and_slash() {
    let open = WsServiceConfig::default();
    assert!(open.warm());
    assert!(open.origin_allowed(None) && open.origin_allowed(Some("https://anything.example")));
    let cfg = WsServiceConfig {
        allowed_origins: vec!["https://app.example".into()],
        warm: Some(false),
        ..Default::default()
    };
    assert!(!cfg.warm());
    assert!(cfg.origin_allowed(Some("https://app.example")));
    assert!(cfg.origin_allowed(Some("HTTPS://APP.EXAMPLE/")));
    assert!(!cfg.origin_allowed(Some("https://evil.example")) && !cfg.origin_allowed(None));
}

#[test]
fn schedule_wire_defaults_fill_in_timezone_payload_and_enabled() {
    let cron: ScheduleKind = serde_json::from_str(r#"{"kind":"cron","cron":"*/5 * * * *"}"#).unwrap();
    assert_eq!(
        cron,
        ScheduleKind::Cron {
            expr: "*/5 * * * *".into(),
            tz: "UTC".into()
        }
    );
    let new: NewSchedule = serde_json::from_str(r#"{"kind":"interval","every_seconds":60}"#).unwrap();
    assert!(new.enabled);
    assert_eq!(new.payload, serde_json::Value::Null);
}

#[test]
fn node_events_start_untagged_with_a_fresh_id() {
    let a = NodeEvent::new("deploy", 1_700);
    let b = NodeEvent::new("deploy", 1_700);
    assert_eq!((a.kind.as_str(), a.ts_ms), ("deploy", 1_700));
    assert!(a.tenant.is_none() && a.service.is_none());
    assert_eq!(a.data, serde_json::Value::Null);
    assert_ne!(a.id, b.id);
}

#[test]
fn env_bundle_debug_names_fields_without_values_and_zeroize_clears_them() {
    let mut bundle = EnvBundle {
        env: vec![("LOG_LEVEL".into(), "plain-value".into())],
        secrets: vec![("TOKEN".into(), "secret-value".into())],
    };
    let shown = format!("{bundle:?}");
    assert!(shown.contains("env_names") && shown.contains("secret_names"));
    assert!(shown.contains("LOG_LEVEL") && shown.contains("TOKEN"));
    assert!(!shown.contains("plain-value") && !shown.contains("secret-value"));
    zeroize::Zeroize::zeroize(&mut bundle);
    assert!(bundle.env.is_empty() && bundle.secrets.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Wire names, the system plan, task builders and the trait default methods
// ---------------------------------------------------------------------------------------------

#[test]
fn runtime_and_trigger_kinds_print_their_wire_names() {
    assert_eq!(
        (RuntimeKind::Deno.as_str(), RuntimeKind::Wasm.to_string()),
        ("deno", "wasm".to_string())
    );
    let names: Vec<&str> = TriggerKind::ALL.iter().map(|t| t.as_str()).collect();
    assert_eq!(
        names,
        [
            "http",
            "interval",
            "cron",
            "queue",
            "call",
            "websocket",
            "pubsub",
            "stream"
        ]
    );
}

#[test]
fn the_system_plan_admits_every_trigger_and_sets_no_tenant_rate_caps() {
    let sys = Plan::system();
    assert!(TriggerKind::ALL.iter().all(|t| sys.allows_trigger(*t)));
    assert_eq!(
        (sys.max_services, sys.max_concurrency, sys.rps, sys.storage_mb),
        (1_000, 10_000, 0, 10_240)
    );
    assert_eq!(sys.max_queue_depth, 100_000);
}

#[test]
fn task_spec_builders_set_each_field_they_name() {
    let spec = TaskSpec::new(TaskKind::Exec(RuntimeKind::Deno), service_ref(), Trigger::Http)
        .with_limits(Limits {
            wall: Duration::from_secs(7),
            ..Limits::defaults_for(&Trigger::Http)
        })
        .with_knobs(ServiceKnobs::default())
        .with_payload(Payload::Http(HttpRequest::get("/hello")))
        .with_trace(TraceCtx {
            traceparent: "00-abc-def-01".into(),
            sampled: true,
        })
        .with_object(ObjectRef::named("room"))
        .with_slug("hello");
    assert_eq!(spec.limits.wall, Duration::from_secs(7));
    assert_eq!(spec.knobs, ServiceKnobs::default());
    assert!(matches!(spec.payload, Payload::Http(_)));
    assert!(spec
        .trace
        .as_ref()
        .is_some_and(|t| t.sampled && t.traceparent.starts_with("00-")));
    assert_eq!(spec.object.as_ref().map(|o| &*o.name), Some("room"));
    assert_eq!(&*spec.slug, "hello");
}

#[test]
fn sched_snapshot_counts_processors_per_kind_and_reads_each_queue_length() {
    let proc = |n: u128, kind: TaskKind| ProcSnapshot {
        id: ProcId(n as u32),
        kind,
        status: ProcStatus::Idle,
        lrq_len: 0,
        runnext: None,
        schedtick: 0,
        machine: None,
        busy_ns: 0,
        steals_in: 0,
        steals_out: 0,
        preemptions: 0,
        rekinds: 0,
    };
    let deno = TaskKind::Exec(RuntimeKind::Deno);
    let wasm = TaskKind::Exec(RuntimeKind::Wasm);
    let snap = SchedSnapshot {
        procs: vec![proc(1, deno), proc(2, deno), proc(3, wasm)],
        ..Default::default()
    };
    assert_eq!(
        (snap.procs_of(deno), snap.procs_of(wasm), snap.procs_of(TaskKind::Build)),
        (2, 1, 0)
    );
    let lens = GrqLens {
        deno: 4,
        wasm: 1,
        build: 9,
    };
    assert_eq!((lens.get(deno), lens.get(wasm), lens.get(TaskKind::Build)), (4, 1, 9));
    let fresh = SchedSnapshot::default();
    assert_eq!(
        (fresh.mode, fresh.procs.len(), fresh.tasks_submitted),
        (DegradationMode::Normal, 0, 0)
    );
}

/// A control plane that only answers the one call under test; the rest are never reached.
struct Plane;

#[async_trait::async_trait]
impl ControlPlane for Plane {
    fn routes(&self) -> Arc<RouteSnapshot> {
        Arc::new(RouteSnapshot::default())
    }
    async fn subscribe_config(&self) -> ConfigStream {
        Box::pin(futures_util::stream::empty())
    }
    async fn service(&self, _id: ServiceId) -> Result<Option<Service>, CpError> {
        Ok(None)
    }
    async fn env_for(
        &self,
        _service: ServiceId,
        _version: SecretsVersion,
    ) -> Result<zeroize::Zeroizing<EnvBundle>, CpError> {
        Ok(zeroize::Zeroizing::new(EnvBundle {
            env: vec![("LEVEL".into(), "info".into())],
            secrets: vec![],
        }))
    }
    async fn report_build(&self, _key: &PreparedKey, _report: BuildReport) -> Result<(), CpError> {
        Ok(())
    }
    fn usage(&self, _batch: UsageBatch) {}
    fn event(&self, _ev: NodeEvent) {}
}

#[tokio::test]
async fn env_shared_hands_out_the_planes_bundle_behind_an_arc() {
    let shared = Plane.env_shared(ServiceId::new(), SecretsVersion(1)).await.unwrap();
    assert_eq!(shared.env, vec![("LEVEL".to_string(), "info".to_string())]);
}

/// A validator that does not expose an RPC surface.
struct NoRpc;

impl UploadValidator for NoRpc {
    fn validate(
        &self,
        _kind: RuntimeKind,
        _content_type: &str,
        _body: &Bytes,
    ) -> Result<ValidatedUpload, ValidationError> {
        Err(ValidationError::InvalidArtifact("not checked here".into()))
    }
}

#[test]
fn a_validator_without_an_rpc_surface_refuses_the_contract_request() {
    let target = ContractTarget::Service { slug: "hello".into() };
    let err = NoRpc
        .rpc_contract("application/javascript", &Bytes::new(), &target)
        .unwrap_err();
    assert_eq!(err, "RPC contracts are not available on this node");
}
