//! Edge cases of the control API found by the edge-case catalogue (scratchpad edge-cases.md,
//! section 1): the reserved system tenant, plan quotas under concurrency and on PATCH, credential
//! placement.

use reqwest::Method;
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

const SYS: &str = "ten_00000000000000000000000SYS";

node_test! {
/// TEN-005: the operator cannot mint keys of the system tenant through the self-service route
/// (SECURITY.md "Operator SDKs": the system tenant cannot be given keys, `403 reserved`).
async fn ten_005_no_keys_for_the_system_tenant_through_v1_keys() {
    let node = start_node().await;
    let r = call(root_for(&node, SYS, Method::POST, "/v1/keys"), Some(json!({"scopes": ["services:write", "deploy"]}))).await;
    assert_eq!((r.s(), r.code()), (403, "reserved".to_string()), "{}", r.text);
    let keys = call(root(&node, Method::GET, &format!("/v1/tenants/{SYS}/keys")), None).await;
    assert_eq!(keys.json, json!([]), "no key was created: {}", keys.text);
    node.shutdown().await;
}
}

node_test! {
/// TEN-006: the system tenant is managed through `/v1/sdks` only: acting as it through
/// `x-rt-tenant`, the operator may read but never create or change services, projects, env,
/// secrets or schedules.
async fn ten_006_the_system_tenant_is_read_only_through_x_rt_tenant() {
    let node = start_node().await;
    let writes = [
        (Method::POST, "/v1/services", json!({"slug": "sys-evil", "runtime": "deno"})),
        (Method::POST, "/v1/projects", json!({"slug": "evil"})),
    ];
    for (m, path, body) in writes {
        let r = call(root_for(&node, SYS, m.clone(), path), Some(body)).await;
        assert_eq!((r.s(), r.code()), (403, "reserved".to_string()), "{m} {path}: {}", r.text);
    }
    let r = call(root_for(&node, SYS, Method::GET, "/v1/services"), None).await;
    assert_eq!(r.s(), 200, "reads stay open to the operator: {}", r.text);
    assert!(r.items().is_empty(), "nothing was created: {}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// QUOTA-002: the plan's `allowed_triggers` applies when a schedule is changed, not only when it is
/// created (docs/api.md "Schedules and queue"): the free plan has no `cron`, so PATCHing an
/// interval schedule into a cron one is `403 trigger_not_allowed`.
async fn quota_002_patching_a_schedule_into_cron_needs_cron_in_the_plan() {
    let node = start_node().await;
    let t = new_tenant(&node, "free").await;
    let svc = create_service(&node, &t.key, &uniq("q002"), "deno").await;
    let created = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "interval", "every_seconds": 60})).await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let id = created.json["id"].as_str().expect("schedule id").to_string();
    let r = patch(&node, &t.key, &format!("/v1/services/{svc}/schedules/{id}"), json!({"kind": "cron", "cron": "* * * * *"})).await;
    assert_eq!((r.s(), r.code()), (403, "trigger_not_allowed".to_string()), "{}", r.text);
    let list = get(&node, &t.key, &format!("/v1/services/{svc}/schedules")).await;
    assert_eq!(list.json[0]["kind"], json!("interval"), "unchanged: {}", list.text);

    // A service whose own `interval` trigger is off cannot get one by PATCH either.
    let pro = new_tenant(&node, "pro").await;
    let svc2 = create_service(&node, &pro.key, &uniq("q002b"), "deno").await;
    let c = post(&node, &pro.key, &format!("/v1/services/{svc2}/schedules"), json!({"kind": "cron", "cron": "0 * * * *"})).await;
    assert_eq!(c.s(), 201, "{}", c.text);
    let sch = c.json["id"].as_str().expect("id").to_string();
    let off = patch(&node, &pro.key, &format!("/v1/services/{svc2}"), json!({"triggers": {"interval": false}})).await;
    assert_eq!(off.s(), 200, "{}", off.text);
    let r = patch(&node, &pro.key, &format!("/v1/services/{svc2}/schedules/{sch}"), json!({"kind": "interval", "every_seconds": 3600})).await;
    assert_eq!((r.s(), r.code()), (403, "trigger_not_allowed".to_string()), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// QUOTA-001: `max_services` holds under concurrent creates (count and insert are one
/// transaction): with 9 of 10 free-plan services, exactly one of 8 concurrent creates wins.
async fn quota_001_max_services_holds_under_concurrent_creates() {
    let node = start_node().await;
    let t = new_tenant(&node, "free").await;
    for i in 0..9 {
        create_service(&node, &t.key, &uniq(&format!("q001-{i}")), "deno").await;
    }
    let mut tasks = Vec::new();
    for i in 0..8 {
        let rb = api(&node, &t.key, Method::POST, "/v1/services").json(&json!({"slug": uniq(&format!("q001c{i}")), "runtime": "deno"}));
        tasks.push(tokio::spawn(send(rb)));
    }
    let mut created = 0;
    for task in tasks {
        let r = task.await.expect("join");
        match r.s() {
            201 => created += 1,
            403 => assert_eq!(r.code(), "quota_exceeded", "{}", r.text),
            other => panic!("unexpected {other}: {}", r.text),
        }
    }
    assert_eq!(created, 1, "exactly one more service fits the free plan");
    let all = get(&node, &t.key, "/v1/services?limit=200").await;
    assert_eq!(all.items().len(), 10, "{}", all.text);
    node.shutdown().await;
}
}

node_test! {
/// AUTH-002: the root token is a Bearer credential only (SECURITY.md "Credentials"); sent as
/// `x-rt-key` it is not the operator.
async fn auth_002_the_root_token_is_accepted_as_a_bearer_only() {
    let node = start_node().await;
    let r = send(node.http().get(node.control_url("/v1/node/status")).header("x-rt-key", node.root_token())).await;
    assert_eq!(r.s(), 401, "{}", r.text);
    let ok = call(root(&node, Method::GET, "/v1/node/status"), None).await;
    assert_eq!(ok.s(), 200, "{}", ok.text);
    node.shutdown().await;
}
}

node_test! {
/// CRON-002: an interval longer than 10 years is refused (422), so it can never overflow into a
/// negative period that fires every second; PATCH applies the same bound.
async fn cron_002_huge_intervals_are_refused_at_the_api() {
    let node = start_node().await;
    let t = new_tenant(&node, "free").await;
    let svc = create_service(&node, &t.key, &uniq("cron002"), "deno").await;
    for every in [9_300_000_000_000_000u64, u64::MAX, 316_224_001] {
        let r = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "interval", "every_seconds": every})).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{every}: {}", r.text);
    }
    let ok = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "interval", "every_seconds": 316_224_000u64})).await;
    assert_eq!(ok.s(), 201, "ten years exactly: {}", ok.text);
    let id = ok.json["id"].as_str().expect("id");
    let r = patch(&node, &t.key, &format!("/v1/services/{svc}/schedules/{id}"), json!({"kind": "interval", "every_seconds": u64::MAX})).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// CTRL-009: an `Idempotency-Key` longer than 255 bytes is refused (422) instead of being kept
/// in memory for 24 hours.
async fn ctrl_009_an_oversized_idempotency_key_is_refused() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let long = "k".repeat(256);
    let rb = api(&node, &t.key, Method::POST, "/v1/services").header("idempotency-key", long);
    let r = call(rb, Some(json!({"slug": uniq("ctrl009"), "runtime": "deno"}))).await;
    assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{}", r.text);
    let ok = api(&node, &t.key, Method::POST, "/v1/services").header("idempotency-key", "k".repeat(255));
    assert_eq!(call(ok, Some(json!({"slug": uniq("ctrl009"), "runtime": "deno"}))).await.s(), 201);
    node.shutdown().await;
}
}

node_test! {
/// INGRESS-006 / EGR-007: a malformed `ingress.ip_allow` CIDR or a non-ASCII egress host entry is
/// `422` when the service is created or changed, instead of being stored and silently matching
/// nothing.
async fn ingress_006_egr_007_bad_ip_allow_and_unicode_hosts_are_refused() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("i006"), "deno").await;
    let bad = [
        json!({"ingress": {"ip_allow": ["10.0.0.0/33"]}}),
        json!({"ingress": {"ip_allow": ["::/129"]}}),
        json!({"ingress": {"ip_allow": ["not-an-ip"]}}),
        json!({"egress": {"allowed_hosts": ["bücher.example"]}}),
        json!({"egress": {"denied_hosts": ["évil.example"]}}),
    ];
    for body in bad {
        let p = patch(&node, &t.key, &format!("/v1/services/{svc}"), body.clone()).await;
        assert_eq!((p.s(), p.code()), (422, "validation_failed".to_string()), "patch {body}: {}", p.text);
        let mut create = body.clone();
        create["slug"] = json!(uniq("i006c"));
        create["runtime"] = json!("deno");
        let c = post(&node, &t.key, "/v1/services", create).await;
        assert_eq!(c.s(), 422, "create {body}: {}", c.text);
    }
    let ok = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({
        "ingress": {"ip_allow": ["10.0.0.0/8", "2001:db8::/32", "192.0.2.7"]},
        "egress": {"allowed_hosts": ["xn--bcher-kva.example", "API.Example.com."]},
    })).await;
    assert_eq!(ok.s(), 200, "{}", ok.text);
    node.shutdown().await;
}
}

node_test! {
/// CTRL-014: a version number in a path is canonical decimal: `0`, `-1`, out of range, leading
/// zeros, `+1` and `1.0` are `404`.
async fn ctrl_014_version_numbers_in_paths_are_canonical() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = ship(&node, &t.key, &uniq("c014"), guests::HELLO).await;
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}/versions/1")).await.s(), 200);
    for v in ["0", "-1", "4294967296", "01", "+1", "1.0"] {
        let r = get(&node, &t.key, &format!("/v1/services/{svc}/versions/{v}")).await;
        assert_eq!(r.s(), 404, "version `{v}`: {}", r.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// CTRL-016: an unknown `?compile=` is `422`; `?activate=maybe` is not a yes.
async fn ctrl_016_upload_query_values() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("c016"), "deno").await;
    let r = upload_ts(&node, &t.key, &svc, guests::HELLO, "compile=fast").await;
    assert_eq!(r.s(), 422, "{}", r.text);
    let r = upload_ts(&node, &t.key, &svc, guests::HELLO, "activate=maybe").await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let view = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert!(view.json["live_version"].is_null(), "not activated: {}", view.text);
    node.shutdown().await;
}
}

node_test! {
/// CTRL-018: a rebuild is a build: it counts against `builds_per_hour` (a `deploy` key cannot
/// loop rebuilds past the plan).
async fn ctrl_018_rebuilds_count_against_the_build_quota() {
    let node = start_node_with(|c| {
        if let Some(p) = c.plans.get_mut("free") {
            p.builds_per_hour = 3;
        }
    })
    .await;
    let t = new_tenant(&node, "free").await;
    let svc = ship(&node, &t.key, &uniq("c018"), guests::HELLO).await;
    let path = format!("/v1/services/{svc}/versions/1/rebuild");
    let mut codes = Vec::new();
    for _ in 0..4 {
        let r = post(&node, &t.key, &path, json!({})).await;
        codes.push((r.s(), r.code()));
        // Wait for the rebuild to finish before the next one.
        eventually(std::time::Duration::from_secs(20), "rebuild done", || async {
            let v = get(&node, &t.key, &format!("/v1/services/{svc}/versions/1")).await;
            v.json["build_state"] != json!("building")
        })
        .await;
    }
    assert!(codes.iter().any(|(s, c)| *s == 429 || c == "build_quota" || c == "quota_exceeded"), "the hourly build quota applies: {codes:?}");

    node.shutdown().await;
}
}

node_test! {
/// CTRL-020 / CTRL-021 / SECRET-004: env holds at most 100 entries of at most 32 KiB; secrets at
/// most 100; deleting a secret that was never set is `404`; an empty secret is a valid value.
async fn ctrl_020_021_secret_004_env_and_secret_bounds() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = ship(&node, &t.key, &uniq("c020"), guests::ECHO).await;
    let env: serde_json::Map<String, serde_json::Value> = (0..101).map(|i| (format!("K{i}"), json!("v"))).collect();
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/env"), serde_json::Value::Object(env)).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/env"), json!({"BIG": "x".repeat(32 * 1024 + 1)})).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/env"), json!({"BIG": "x".repeat(32 * 1024)})).await;
    assert_eq!(r.s(), 200, "exactly 32 KiB: {}", r.text);
    for i in 0..100 {
        let r = put(&node, &t.key, &format!("/v1/services/{svc}/secrets/S{i}"), json!({"value": ""})).await;
        assert_eq!(r.s(), 204, "secret {i}: {}", r.text);
    }
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/secrets/S100"), json!({"value": "v"})).await;
    assert_eq!(r.s(), 422, "101st secret: {}", r.text);
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/secrets/S0"), json!({"value": "again"})).await;
    assert_eq!(r.s(), 204, "overwriting an existing one is fine: {}", r.text);
    let r = delete(&node, &t.key, &format!("/v1/services/{svc}/secrets/NEVER_SET")).await;
    assert_eq!(r.s(), 404, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// CTRL-030: a schedule payload of exactly 64 KiB of JSON is accepted, one byte more is `422`,
/// on create and on PATCH.
async fn ctrl_030_schedule_payload_bound_is_exact() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("c030"), "deno").await;
    // `"xxx…"` serialises to its length + 2 quote bytes.
    let fits = json!("x".repeat(64 * 1024 - 2));
    let over = json!("x".repeat(64 * 1024 - 1));
    let path = format!("/v1/services/{svc}/schedules");
    let ok = post(&node, &t.key, &path, json!({"kind": "interval", "every_seconds": 60, "payload": fits})).await;
    assert_eq!(ok.s(), 201, "{}", &ok.text[..ok.text.len().min(200)]);
    let r = post(&node, &t.key, &path, json!({"kind": "interval", "every_seconds": 60, "payload": over.clone()})).await;
    assert_eq!(r.s(), 422, "{}", &r.text[..r.text.len().min(200)]);
    let id = ok.json["id"].as_str().expect("id");
    let r = patch(&node, &t.key, &format!("{path}/{id}"), json!({"kind": "interval", "every_seconds": 60, "payload": over})).await;
    assert_eq!(r.s(), 422, "{}", &r.text[..r.text.len().min(200)]);
    node.shutdown().await;
}
}

node_test! {
/// CTRL-008 / CTRL-010: the same Idempotency-Key sent twice at once runs once (the second waits
/// and replays); the operator acting for two tenants has two key spaces.
async fn ctrl_008_010_idempotency_under_concurrency_and_per_tenant() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    for round in 0..10 {
        let slug = uniq("c008");
        let body = json!({"slug": slug, "runtime": "deno"});
        let k = format!("idem-{round}");
        let send_one = || call(api(&node, &t.key, Method::POST, "/v1/services").header("idempotency-key", k.as_str()), Some(body.clone()));
        let (a, b) = tokio::join!(send_one(), send_one());
        assert_eq!((a.s(), b.s()), (201, 201), "round {round}: {} / {}", a.text, b.text);
        assert_eq!(a.json["id"], b.json["id"], "round {round}: one service");
    }
    let u = new_tenant(&node, "pro").await;
    let mk = |tenant: &str, slug: &str| {
        call(
            root_for(&node, tenant, Method::POST, "/v1/services").header("idempotency-key", "same-key"),
            Some(json!({"slug": slug, "runtime": "deno"})),
        )
    };
    // Different bodies under one key: a shared key space would answer `422 idempotency_mismatch`.
    let a = mk(&t.id, &uniq("op-a")).await;
    let b = mk(&u.id, &uniq("op-b")).await;
    assert_eq!((a.s(), b.s()), (201, 201), "{} / {}", a.text, b.text);
    assert_ne!(a.json["id"], b.json["id"], "two tenants, two services");
    node.shutdown().await;
}
}

node_test! {
/// CHOST-003: a custom host must be a DNS name: labels of 1 to 63 characters that neither start
/// nor end with `-`, at least two labels, no IP literal; case and a trailing dot are normalised.
async fn chost_003_custom_hosts_are_dns_names() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("ch003"), "deno").await;
    let put = |h: String| call(root_for(&node, &t.id, Method::PUT, &format!("/v1/services/{svc}/hosts/{h}")), None);
    let long_label = format!("{}.example.com", "a".repeat(64));
    let too_long = format!("{}.example.com", vec!["a".repeat(60); 5].join("."));
    for bad in [
        "a..b.example.com".to_string(),
        long_label,
        "-a.example.com".into(),
        "a-.example.com".into(),
        "1.2.3.4".into(),
        "localhost".into(),
        "b%C3%BCcher.example".into(),
        too_long,
    ] {
        let r = put(bad.clone()).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{bad}: {}", r.text);
    }
    let r = put("API.Example.com.".into()).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["host"], json!("api.example.com"));
    let r = put("xn--bcher-kva.example".into()).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// CHOST-004 / CHOST-008: two tenants claiming one host at once: one wins, the other gets `409
/// host_taken`; a host held by another tenant is never re-pointed, not by the operator either.
async fn chost_004_008_one_claim_wins_and_nothing_is_re_pointed() {
    let node = start_node().await;
    for round in 0..5 {
        let a = new_tenant(&node, "pro").await;
        let b = new_tenant(&node, "pro").await;
        let sa = create_service(&node, &a.key, &uniq("cha"), "deno").await;
        let sb = create_service(&node, &b.key, &uniq("chb"), "deno").await;
        let host = format!("race{round}-{}.example.com", uniq("h"));
        let ra = call(root_for(&node, &a.id, Method::PUT, &format!("/v1/services/{sa}/hosts/{host}")), None);
        let rb = call(root_for(&node, &b.id, Method::PUT, &format!("/v1/services/{sb}/hosts/{host}")), None);
        let (ra, rb) = tokio::join!(ra, rb);
        let mut codes = [(ra.s(), ra.code()), (rb.s(), rb.code())];
        codes.sort();
        assert_eq!(codes, [(200, String::new()), (409, "host_taken".to_string())], "round {round}: {} / {}", ra.text, rb.text);
        let (loser_t, loser_s) = if ra.s() == 409 { (&a, &sa) } else { (&b, &sb) };
        let again = call(root_for(&node, &loser_t.id, Method::PUT, &format!("/v1/services/{loser_s}/hosts/{host}")), None).await;
        assert_eq!(again.s(), 409, "no takeover: {}", again.text);
    }
    node.shutdown().await;
}
}

/// Node max for `min_run_time_ms` used by the bound tests (`sched.min_run_time_max_ms`).
const MRT_NODE_MAX: u64 = 1_000;

/// A node where the node max (1 s) is below the `pro` plan cap (10 min), plus a `short` plan whose
/// cap (500 ms) is below the node max.
async fn mrt_bound_node() -> rt_testkit::NodeHarness {
    start_node_with(|c| {
        c.sched.min_run_time_max_ms = MRT_NODE_MAX;
        let mut short = pro_plan();
        short.limits_max.min_run_time_ms = 500;
        c.plans.insert("short".to_string(), short);
    })
    .await
}

node_test! {
/// mrt_api_01: `scaling.min_run_time_ms` over `min(plan cap, node max)` is refused with
/// `422 limit_exceeds_plan` (not clamped) on create and on PATCH; the service is left unchanged.
async fn mrt_api_01_min_run_time_over_the_max_is_refused_on_create_and_patch() {
    let node = mrt_bound_node().await;
    let t = new_tenant(&node, "pro").await;
    let refused = post(&node, &t.key, "/v1/services", json!({"slug": uniq("mrt01a"), "runtime": "deno", "scaling": {"min_run_time_ms": MRT_NODE_MAX + 1}})).await;
    assert_eq!((refused.s(), refused.code()), (422, "limit_exceeds_plan".to_string()), "{}", refused.text);
    assert_eq!(refused.json["error"]["details"]["field"], json!("min_run_time_ms"), "{}", refused.text);
    assert_eq!(refused.json["error"]["details"]["max"], json!(MRT_NODE_MAX), "{}", refused.text);
    assert_eq!(refused.json["error"]["details"]["requested"], json!(MRT_NODE_MAX + 1), "{}", refused.text);

    let created = post(&node, &t.key, "/v1/services", json!({"slug": uniq("mrt01b"), "runtime": "deno"})).await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let svc = created.json["id"].as_str().expect("id").to_string();
    let r = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"scaling": {"min_run_time_ms": MRT_NODE_MAX + 1}})).await;
    assert_eq!((r.s(), r.code()), (422, "limit_exceeds_plan".to_string()), "{}", r.text);
    let view = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert_eq!(view.json["scaling"]["min_run_time_ms"], Value::Null, "unchanged: {}", view.text);
    node.shutdown().await;
}
}

node_test! {
/// mrt_api_02: `0` (keep nothing after the answer) and exactly the maximum are accepted on create
/// and on PATCH, and the stored value is the one the owner asked for.
async fn mrt_api_02_zero_and_exactly_the_max_are_accepted() {
    let node = mrt_bound_node().await;
    let t = new_tenant(&node, "pro").await;
    for v in [0u64, MRT_NODE_MAX] {
        let created = post(&node, &t.key, "/v1/services", json!({"slug": uniq("mrt02"), "runtime": "deno", "scaling": {"min_run_time_ms": v}})).await;
        assert_eq!(created.s(), 201, "{v}: {}", created.text);
        assert_eq!(created.json["scaling"]["min_run_time_ms"], json!(v), "{}", created.text);
        let svc = created.json["id"].as_str().expect("id").to_string();
        let r = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"scaling": {"min_run_time_ms": v}})).await;
        assert_eq!(r.s(), 200, "patch {v}: {}", r.text);
        assert_eq!(r.json["scaling"]["min_run_time_ms"], json!(v), "{}", r.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// mrt_api_03: the bound is the smaller of the plan cap and the node max, whichever is lower. The
/// node max (1 s) bounds the `pro` plan (10 min); the `short` plan (500 ms) bounds a service below
/// the node max. Both report the effective maximum in `details.max`.
async fn mrt_api_03_the_node_max_bounds_below_the_plan() {
    let node = mrt_bound_node().await;
    let pro = new_tenant(&node, "pro").await;
    let over_node = post(&node, &pro.key, "/v1/services", json!({"slug": uniq("mrt03a"), "runtime": "deno", "scaling": {"min_run_time_ms": 60_000}})).await;
    assert_eq!((over_node.s(), over_node.code()), (422, "limit_exceeds_plan".to_string()), "{}", over_node.text);
    assert_eq!(over_node.json["error"]["details"]["max"], json!(MRT_NODE_MAX), "{}", over_node.text);

    let short = new_tenant(&node, "short").await;
    let over_plan = post(&node, &short.key, "/v1/services", json!({"slug": uniq("mrt03b"), "runtime": "deno", "scaling": {"min_run_time_ms": 501}})).await;
    assert_eq!((over_plan.s(), over_plan.code()), (422, "limit_exceeds_plan".to_string()), "{}", over_plan.text);
    assert_eq!(over_plan.json["error"]["details"]["max"], json!(500), "{}", over_plan.text);
    let at_plan = post(&node, &short.key, "/v1/services", json!({"slug": uniq("mrt03c"), "runtime": "deno", "scaling": {"min_run_time_ms": 500}})).await;
    assert_eq!(at_plan.s(), 201, "{}", at_plan.text);
    node.shutdown().await;
}
}

node_test! {
/// mrt_api_04: `min_run_time_ms` must be a non-negative integer; a fraction, a negative number or a
/// string is `422 validation_failed` and creates nothing.
async fn mrt_api_04_a_non_integer_is_validation_failed() {
    let node = mrt_bound_node().await;
    let t = new_tenant(&node, "pro").await;
    for bad in [json!(1.5), json!(-1), json!("500")] {
        let slug = uniq("mrt04");
        let r = post(&node, &t.key, "/v1/services", json!({"slug": slug, "runtime": "deno", "scaling": {"min_run_time_ms": bad}})).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{}", r.text);
    }
    let all = get(&node, &t.key, "/v1/services?limit=200").await;
    assert!(all.items().is_empty(), "nothing was created: {}", all.text);
    node.shutdown().await;
}
}
