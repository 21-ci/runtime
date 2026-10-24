//! rt-types event contracts: subject/topic/group grammars, the matcher, partitioning, the
//! `events` / `async` service config blocks, event triggers and plan defaults.

use std::sync::Arc;
use std::time::Duration;

use rt_types::events::{
    admission_lane, headers_over_limits, invocation_aad, partition_for, subject_matches, valid_group_name,
    valid_topic_name, validate_headers, validate_pattern, validate_subject, AsyncInvokeConfig, EventsConfig,
    OnExhausted, StartPosition, StreamConsumer, Subscription, ASYNC_MAX_ATTEMPTS_DEFAULT, EVENT_HEADERS_MAX,
};
use rt_types::{
    InvocationId, Limits, MsgId, Plan, ScheduleId, ServiceConfig, ServiceId, ServicePatch, TaskId, TopicId, Trigger,
    TriggerKind, TriggerSwitches,
};

fn h(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// Subjects are `.`-separated tokens of `[A-Za-z0-9_-]`, at most 16 tokens and 256 bytes.
#[test]
fn ac_evt_01_subject_grammar_accepts_tokens_and_rejects_empty_or_long_tokens() {
    for ok in ["orders", "orders.eu-1.created", "A_b.C-d", &vec!["t"; 16].join(".")] {
        assert_eq!(validate_subject(ok), Ok(()), "{ok}");
    }
    for bad in ["", ".", "a.", ".a", "a..b", "a b", "a/b", "é"] {
        assert!(validate_subject(bad).is_err(), "{bad}");
    }
    assert!(validate_subject(&vec!["t"; 17].join("."))
        .unwrap_err()
        .contains("16 tokens"));
    assert!(validate_subject(&"x".repeat(257)).unwrap_err().contains("256 bytes"));
    assert_eq!(validate_subject(&"x".repeat(256)), Ok(()));
}

/// `*` matches exactly one token, `>` one or more trailing tokens.
#[test]
fn ac_evt_02_pattern_star_matches_one_token_and_gt_matches_the_rest() {
    assert!(subject_matches("orders.*.created", "orders.eu.created"));
    assert!(!subject_matches("orders.*.created", "orders.eu.x.created"));
    assert!(!subject_matches("orders.*", "orders"));
    assert!(subject_matches("orders.>", "orders.eu"));
    assert!(subject_matches("orders.>", "orders.eu.created.v2"));
    assert!(!subject_matches("orders.>", "orders"));
    assert!(subject_matches("orders.eu", "orders.eu"));
    assert!(!subject_matches("orders.eu", "orders.us"));
    assert!(subject_matches("*", "x") && !subject_matches("*", "x.y"));
}

/// `>` is only valid as the last token of a pattern; published subjects carry no wildcards.
#[test]
fn ac_evt_03_gt_is_only_allowed_last_and_published_subjects_have_no_wildcards() {
    assert_eq!(validate_pattern("a.*.>"), Ok(()));
    assert!(validate_pattern("a.>.b").unwrap_err().contains("last"));
    assert!(validate_pattern("a.b*").is_err());
    assert!(validate_subject("a.*").unwrap_err().contains("wildcard"));
    assert!(validate_subject("a.>").unwrap_err().contains("wildcard"));
}

/// Subjects and patterns whose first token is `_rt` are reserved for the runtime.
#[test]
fn ac_evt_04_rt_prefixed_subjects_are_reserved() {
    assert!(validate_subject("_rt.x").unwrap_err().contains("reserved"));
    assert!(validate_pattern("_rt.>").unwrap_err().contains("reserved"));
    assert_eq!(validate_subject("x._rt"), Ok(()));
    assert_eq!(validate_subject("_rtx"), Ok(()));
}

/// Topic names `[a-z0-9][a-z0-9._-]{0,62}`, group names `[a-z0-9][a-z0-9_-]{0,62}`.
#[test]
fn ac_evt_05_topic_and_group_names_follow_the_grammar_at_63_chars() {
    assert!(valid_topic_name("orders") && valid_topic_name("0rders.v1_x-y"));
    assert!(valid_topic_name(&"a".repeat(63)) && !valid_topic_name(&"a".repeat(64)));
    for bad in ["", "Orders", "-a", ".a", "a b", "a/b"] {
        assert!(!valid_topic_name(bad), "{bad}");
    }
    assert!(valid_group_name("billing") && valid_group_name("g_1-2"));
    assert!(valid_group_name(&"g".repeat(63)) && !valid_group_name(&"g".repeat(64)));
    for bad in ["", "a.b", "_a", "Billing"] {
        assert!(!valid_group_name(bad), "{bad}");
    }
}

/// Headers: no `x-rt-` names, no CR/LF/NUL in values, at most 32 entries and 16 KiB.
#[test]
fn ac_evt_06_headers_reject_x_rt_names_crlf_and_more_than_32() {
    assert_eq!(
        validate_headers(&h(&[("content-type", "text/plain"), ("x-id", "1")])),
        Ok(())
    );
    assert!(validate_headers(&h(&[("X-RT-Trigger", "x")]))
        .unwrap_err()
        .contains("reserved"));
    for v in ["a\r\nb", "a\nb", "a\0b"] {
        assert!(validate_headers(&h(&[("k", v)])).is_err());
    }
    assert!(validate_headers(&h(&[("bad name", "v")])).is_err());
    assert!(validate_headers(&h(&[("", "v")])).is_err());
    assert!(validate_headers(&h(&[("k", &"v".repeat(4097))])).is_err());
    let many: Vec<(String, String)> = (0..=EVENT_HEADERS_MAX).map(|i| (format!("h{i}"), "v".into())).collect();
    assert!(validate_headers(&many).unwrap_err().contains("too many"));
    assert!(headers_over_limits(&many));
    let big: Vec<(String, String)> = (0..5).map(|i| (format!("h{i}"), "v".repeat(4000))).collect();
    assert!(validate_headers(&big).is_err() && headers_over_limits(&big));
    assert!(!headers_over_limits(&many[..EVENT_HEADERS_MAX]));
}

/// Partitioning is FNV-1a 64 of the key modulo the partition count: stable and in range.
#[test]
fn ac_evt_07_partition_for_is_stable_fnv1a_and_in_range() {
    assert_eq!(partition_for("order-1", 4), 1);
    assert_eq!(partition_for("user:42", 4), 2);
    assert_eq!(partition_for("a", 4), 0);
    assert_eq!(partition_for("anything", 1), 0);
    assert_eq!(partition_for("anything", 0), 0);
    for i in 0..200 {
        assert!(partition_for(&format!("k{i}"), 16) < 16);
    }
}

/// `events` defaults per the spec; out-of-bound values fail naming the field.
#[test]
fn ac_evt_08_events_config_defaults_and_bounds_name_the_field() {
    let cfg: EventsConfig =
        serde_json::from_str(r#"{"subscribe":[{"subject":"a.*"}],"consume":[{"topic":"orders"}]}"#).unwrap();
    let c = &cfg.consume[0];
    assert_eq!(
        (c.group.as_str(), c.start, c.batch_size, c.max_attempts, c.on_exhausted),
        ("default", StartPosition::Latest, 100, 10, OnExhausted::Dlq)
    );
    assert_eq!(c, &StreamConsumer::new("orders"));
    assert_eq!(cfg.subscribe[0].group, None);
    assert_eq!(cfg.validate(), Ok(()));
    assert!(serde_json::from_str::<EventsConfig>(r#"{"consume":[{"topic":"t","max_wait_ms":0}]}"#).is_err());
    assert!(serde_json::from_str::<EventsConfig>(r#"{"nope":[]}"#).is_err());

    let err = |cfg: EventsConfig| cfg.validate().unwrap_err();
    let sub = |s: &str, g: Option<&str>| Subscription {
        subject: s.into(),
        group: g.map(Into::into),
    };
    let one_sub = |s| EventsConfig {
        subscribe: vec![sub("ok", None), s],
        ..Default::default()
    };
    assert!(err(one_sub(sub("a.>.b", None))).starts_with("`events.subscribe[1].subject`"));
    assert!(err(one_sub(sub("a", Some("Bad")))).starts_with("`events.subscribe[1].group`"));
    let too_many = EventsConfig {
        subscribe: (0..33).map(|_| sub("a", None)).collect(),
        ..Default::default()
    };
    assert!(err(too_many).starts_with("`events.subscribe`"));
    let cons = |f: &dyn Fn(&mut StreamConsumer)| {
        let mut c = StreamConsumer::new("t");
        f(&mut c);
        EventsConfig {
            consume: vec![c],
            ..Default::default()
        }
    };
    assert!(err(cons(&|c| c.topic = "tpc_x".into())).starts_with("`events.consume[0].topic`"));
    assert!(err(cons(&|c| c.group = "a.b".into())).starts_with("`events.consume[0].group`"));
    assert!(err(cons(&|c| c.batch_size = 0)).starts_with("`events.consume[0].batch_size`"));
    assert!(err(cons(&|c| c.batch_size = 1001)).starts_with("`events.consume[0].batch_size`"));
    assert_eq!(cons(&|c| c.batch_size = 1000).validate(), Ok(()));
    assert!(err(cons(&|c| c.max_attempts = 0)).starts_with("`events.consume[0].max_attempts`"));
    assert!(err(cons(&|c| c.max_attempts = 101)).starts_with("`events.consume[0].max_attempts`"));
    let dup = EventsConfig {
        consume: vec![StreamConsumer::new("t"), StreamConsumer::new("t")],
        ..Default::default()
    };
    assert!(err(dup).starts_with("`events.consume[1]`"));
    let mut other_group = StreamConsumer::new("t");
    other_group.group = "g2".into();
    let mut nine = vec![StreamConsumer::new("t"), other_group];
    assert_eq!(
        EventsConfig {
            consume: nine.clone(),
            ..Default::default()
        }
        .validate(),
        Ok(())
    );
    nine.extend((0..7).map(|i| StreamConsumer::new(format!("t{i}"))));
    assert!(err(EventsConfig {
        consume: nine,
        ..Default::default()
    })
    .starts_with("`events.consume`"));
}

/// `async` defaults (3 attempts, 3600 s) and bounds (1..=10, 60..=86400).
#[test]
fn ac_evt_09_async_config_defaults_and_bounds() {
    let d = AsyncInvokeConfig::default();
    assert_eq!(d.max_attempts(), ASYNC_MAX_ATTEMPTS_DEFAULT);
    assert_eq!(d.max_event_age(), Duration::from_secs(3600));
    assert_eq!(d.validate(), Ok(()));
    let c = |a: Option<u16>, s: Option<u32>| AsyncInvokeConfig {
        enabled: Some(true),
        max_attempts: a,
        max_event_age_s: s,
    };
    assert_eq!(c(Some(10), Some(60)).validate(), Ok(()));
    assert_eq!(c(Some(1), Some(86_400)).validate(), Ok(()));
    assert!(c(Some(0), None)
        .validate()
        .unwrap_err()
        .starts_with("`async.max_attempts`"));
    assert!(c(Some(11), None)
        .validate()
        .unwrap_err()
        .starts_with("`async.max_attempts`"));
    assert!(c(None, Some(59))
        .validate()
        .unwrap_err()
        .starts_with("`async.max_event_age_s`"));
    assert!(c(None, Some(86_401))
        .validate()
        .unwrap_err()
        .starts_with("`async.max_event_age_s`"));
    assert_eq!(c(Some(5), Some(120)).max_attempts(), 5);
    assert_eq!(c(Some(5), Some(120)).max_event_age(), Duration::from_secs(120));
    assert!(serde_json::from_str::<AsyncInvokeConfig>(r#"{"bogus":1}"#).is_err());
}

/// The new blocks round-trip under `events` / `async`, are left out while unset, and old JSON
/// without them still parses.
#[test]
fn ac_evt_10_service_config_round_trips_and_omits_unset_blocks() {
    let plain = serde_json::to_value(ServiceConfig::default()).unwrap();
    assert!(plain.get("events").is_none() && plain.get("async").is_none());
    let old: ServiceConfig = serde_json::from_str(r#"{"limits":{"wall_ms":1000}}"#).unwrap();
    assert!(old.events.is_unset() && old.async_invoke.is_unset());

    let cfg: ServiceConfig = serde_json::from_str(
        r#"{"events":{"subscribe":[{"subject":"a.>","group":"g"}]},"async":{"enabled":true,"max_attempts":2}}"#,
    )
    .unwrap();
    assert!(cfg.async_invoke.enabled());
    assert_eq!(cfg.events.subscribe[0].group.as_deref(), Some("g"));
    let v = serde_json::to_value(&cfg).unwrap();
    assert_eq!(v["async"], serde_json::json!({"enabled": true, "max_attempts": 2}));
    assert_eq!(serde_json::from_value::<ServiceConfig>(v).unwrap(), cfg);

    let patch: ServicePatch = serde_json::from_str(r#"{"async":{"enabled":false},"events":{}}"#).unwrap();
    assert_eq!(patch.async_invoke.map(|a| a.enabled()), Some(false));
    assert!(patch.events.unwrap().is_unset());
}

/// A pub/sub delivery's call depth is its `depth`; labels are `pubsub` / `stream`; stream tasks
/// get the queue limits and pub/sub the HTTP ones.
#[test]
fn ac_evt_11_pubsub_depth_is_the_call_depth_and_labels_are_pubsub_and_stream() {
    let ps = Trigger::Pubsub {
        subject: Arc::from("a.b"),
        depth: 3,
        reply: false,
    };
    let st = Trigger::Stream {
        topic: Arc::from("orders"),
        partition: 2,
        attempt: 1,
    };
    assert_eq!((ps.label(), ps.call_depth()), ("pubsub", 3));
    assert_eq!((st.label(), st.call_depth()), ("stream", 0));
    assert_eq!(Limits::defaults_for(&ps), Limits::default());
    assert_eq!(
        Limits::defaults_for(&st),
        Limits::defaults_for(&Trigger::Queue {
            batch: vec![MsgId::new()],
            attempt: 1
        })
    );
    assert_eq!(TopicId::PREFIX, "tpc");
    assert_eq!(InvocationId::PREFIX, "inv");
    let id = InvocationId::from_u128(7);
    assert_eq!(id.to_string().parse::<InvocationId>().unwrap(), id);
    assert!(id.to_string().starts_with("inv_"));
}

/// Plan defaults for the event limits: free and system values.
#[test]
fn ac_evt_12_plan_defaults_and_system_plan_values() {
    let free = Plan::default();
    assert_eq!(
        (
            free.max_async_pending,
            free.max_topics,
            free.max_topic_partitions,
            free.max_stream_mb,
            free.max_stream_retention_hours
        ),
        (10_000, 10, 4, 100, 72)
    );
    let sys = Plan::system();
    assert_eq!(
        (
            sys.max_async_pending,
            sys.max_topics,
            sys.max_topic_partitions,
            sys.max_stream_mb,
            sys.max_stream_retention_hours
        ),
        (100_000, 1_000, 16, 10_240, 720)
    );
    let parsed: Plan = serde_json::from_str(r#"{"max_topics":3}"#).unwrap();
    assert_eq!((parsed.max_topics, parsed.max_stream_mb), (3, 100));
}

/// `pubsub` and `stream` trigger kinds round-trip by name, are allowed by the free plan and on
/// by default in the service switches.
#[test]
fn ac_evt_13_trigger_kinds_pubsub_and_stream_round_trip_and_are_in_the_free_plan() {
    for (k, s) in [(TriggerKind::Pubsub, "pubsub"), (TriggerKind::Stream, "stream")] {
        assert_eq!(k.as_str(), s);
        assert_eq!(serde_json::to_value(k).unwrap(), serde_json::json!(s));
        assert_eq!(serde_json::from_value::<TriggerKind>(serde_json::json!(s)).unwrap(), k);
        assert!(Plan::default().allows_trigger(k));
        assert!(TriggerSwitches::default().enabled(k));
        let all_off = TriggerSwitches {
            http: false,
            interval: false,
            cron: false,
            queue: false,
            call: false,
            websocket: false,
        };
        assert!(all_off.enabled(k));
    }
    assert_eq!(TriggerKind::ALL.len(), 8);
    assert!(!Plan::default().allows_trigger(TriggerKind::Cron));
}

/// A request delivery (`reply: true`) keeps the depth it was given.
#[test]
fn ac_evt_14_pubsub_reply_trigger_keeps_depth() {
    let t = Trigger::Pubsub {
        subject: Arc::from("pricing.quote"),
        depth: 15,
        reply: true,
    };
    assert_eq!(t.call_depth(), 15);
    assert!(matches!(t, Trigger::Pubsub { reply: true, .. }));
    assert_eq!(t.label(), "pubsub");
}

/// Queue, stream and schedule tasks share the background lane (1); the rest lane 0.
#[test]
fn ac_evt_15_admission_lane_is_background_for_queue_stream_and_schedules() {
    let sch = ScheduleId::new();
    let background = [
        Trigger::Queue {
            batch: vec![],
            attempt: 1,
        },
        Trigger::Stream {
            topic: Arc::from("t"),
            partition: 0,
            attempt: 1,
        },
        Trigger::Interval { id: sch },
        Trigger::Cron { id: sch },
        Trigger::Scheduled { id: "x".into() },
    ];
    for t in &background {
        assert_eq!(admission_lane(t), 1, "{}", t.label());
    }
    let foreground = [
        Trigger::Http,
        Trigger::Call {
            parent: TaskId(1),
            depth: 1,
        },
        Trigger::Pubsub {
            subject: Arc::from("a"),
            depth: 1,
            reply: false,
        },
        Trigger::Socket { object: Arc::from("o") },
        Trigger::Alarm { object: Arc::from("o") },
    ];
    for t in &foreground {
        assert_eq!(admission_lane(t), 0, "{}", t.label());
    }
}

/// The sealing AAD binds service, invocation and part, and has a fixed layout.
#[test]
fn ac_evt_16_invocation_aad_differs_per_service_id_and_part() {
    let (s1, s2) = (ServiceId::from_u128(1), ServiceId::from_u128(2));
    let (i1, i2) = (InvocationId::from_u128(1), InvocationId::from_u128(2));
    let base = invocation_aad(s1, i1, b'h');
    assert_eq!(base, format!("rt-async-invocation\0{s1}\0{i1}\0h").into_bytes());
    assert_ne!(base, invocation_aad(s2, i1, b'h'));
    assert_ne!(base, invocation_aad(s1, i2, b'h'));
    assert_ne!(base, invocation_aad(s1, i1, b'b'));
}

/// A topic name may not start with the topic id prefix `tpc_`.
#[test]
fn ac_evt_17_topic_names_cannot_start_with_the_id_prefix() {
    assert!(!valid_topic_name("tpc_orders"));
    assert!(!valid_topic_name(&TopicId::new().to_string().to_lowercase()));
    assert!(valid_topic_name("tpc-orders") && valid_topic_name("tpcorders"));
}

/// Async invocation is off unless `async.enabled` is true.
#[test]
fn ac_evt_18_async_is_disabled_unless_enabled() {
    assert!(!AsyncInvokeConfig::default().enabled());
    assert!(!ServiceConfig::default().async_invoke.enabled());
    let on: AsyncInvokeConfig = serde_json::from_str(r#"{"enabled":true}"#).unwrap();
    assert!(on.enabled());
    let off: AsyncInvokeConfig = serde_json::from_str(r#"{"enabled":false}"#).unwrap();
    assert!(!off.enabled() && !off.is_unset());
}
