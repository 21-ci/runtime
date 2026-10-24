//! Control API conventions (docs/api.md): API-02..14, plus smoke coverage of the rest of the route map.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use reqwest::Method;
use rt_store::{
    DataConfig, DelayedStore, LocalServiceData, NewDelayed, ServiceDataStore, SqlStatement, SqlValue, SqliteQueueStore,
};
use rt_types::ServiceId;
use serde_json::{json, Value};

use crate::common::openapi::{documented_status_for, ERROR_CODES};
use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

fn assert_envelope(r: &Resp, status: u16, code: &str) {
    assert_eq!(r.s(), status, "expected {status} {code}, got {} {}", r.s(), r.text);
    assert_eq!(r.code(), code, "error code in {}", r.text);
    assert!(
        r.json["error"]["message"].is_string(),
        "error.message is a string: {}",
        r.text
    );
    assert!(
        r.json["error"].get("details").is_some(),
        "error.details is part of the envelope: {}",
        r.text
    );
    assert!(
        r.header("content-type").unwrap_or_default().contains("json"),
        "control errors are JSON, got {:?}",
        r.header("content-type")
    );
}

// ---------------------------------------------------------------------------------------------
// API-02
// ---------------------------------------------------------------------------------------------

node_test! {
/// API-02: every control error uses `{"error": {"code", "message", "details"}}` with a documented
/// code whose HTTP status matches the table in docs/api.md.
async fn ac_api_02_control_errors_use_the_envelope_with_documented_codes() {
    // `free` is the default plan: max memory 128 MB, builds 20/h.
    let node = start_node().await;
    let t = new_tenant(&node, "free").await;
    let svc = create_service(&node, &t.key, &uniq("err"), "deno").await;
    let ro = new_tenant_scopes(&node, "free", &["services:read"]).await;

    let mut seen: Vec<(String, Resp)> = Vec::new();

    // 401 unauthorized: no credentials, wrong token.
    seen.push(("no auth".into(), send(node.http().get(node.control_url("/v1/services"))).await));
    seen.push(("bad token".into(), send(node.http().get(node.control_url("/v1/services")).bearer_auth("not-a-token")).await));
    // 403 insufficient_scope: read-only key writes; tenant key on an operator route.
    seen.push(("scope".into(), post(&node, &ro.key, "/v1/services", json!({"slug": uniq("x"), "runtime": "deno"})).await));
    seen.push(("operator route".into(), get(&node, &t.key, "/v1/tenants").await));
    // 404 not_found.
    seen.push(("unknown service".into(), get(&node, &t.key, "/v1/services/svc_01J00000000000000000000000").await));
    seen.push(("garbage id".into(), get(&node, &t.key, "/v1/services/not-an-id").await));
    seen.push(("unknown version".into(), get(&node, &t.key, &format!("/v1/services/{svc}/versions/99")).await));
    // 409 slug_taken.
    let taken = uniq("taken");
    create_service(&node, &t.key, &taken, "deno").await;
    seen.push(("slug taken".into(), post(&node, &t.key, "/v1/services", json!({"slug": taken, "runtime": "deno"})).await));
    // 412 precondition_failed.
    seen.push((
        "stale etag".into(),
        call(api(&node, &t.key, Method::PATCH, &format!("/v1/services/{svc}")).header("if-match", "\"0\""), Some(json!({"name": "n"}))).await,
    ));
    // 422: validation, limits above the plan, malformed JSON.
    seen.push(("bad slug".into(), post(&node, &t.key, "/v1/services", json!({"slug": "Bad Slug", "runtime": "deno"})).await));
    seen.push(("bad runtime".into(), post(&node, &t.key, "/v1/services", json!({"slug": uniq("rt"), "runtime": "cobol"})).await));
    seen.push((
        "limit above plan".into(),
        post(&node, &t.key, "/v1/services", json!({"slug": uniq("big"), "runtime": "deno", "limits": {"memory_mb": 4096}})).await,
    ));
    seen.push((
        "malformed json".into(),
        send(api(&node, &t.key, Method::POST, "/v1/services").header("content-type", "application/json").body("{not json")).await,
    ));
    seen.push((
        "missing field".into(),
        send(api(&node, &t.key, Method::POST, "/v1/services").header("content-type", "application/json").body("{}")).await,
    ));
    seen.push((
        "invalid artifact".into(),
        upload(&node, &t.key, &svc, "application/typescript", b"export default (( {".to_vec(), "activate=0").await,
    ));
    // 422 idempotency_mismatch is covered by API-03; schedule_too_frequent by TRG-07.

    for (what, r) in &seen {
        assert!(r.s() >= 400 && r.s() < 600, "{what}: expected an error, got {}", r.s());
        assert!(r.json["error"].is_object(), "{what}: not an error envelope: {} (status {})", r.text, r.s());
        let code = r.code();
        let documented = documented_status_for(&code);
        assert!(documented.is_some(), "{what}: `{code}` is not a documented control error code");
        assert_eq!(documented, Some(r.s()), "{what}: status {} does not match the table for `{code}`", r.s());
        assert!(r.json["error"]["message"].as_str().is_some_and(|m| !m.is_empty()), "{what}: message");
        assert!(r.json["error"].get("details").is_some(), "{what}: details");
        assert!(!r.text.contains("panicked") && !r.text.contains("RUST_BACKTRACE"), "{what}: internal text leaked: {}", r.text);
    }
    // And the expected codes, one by one.
    let by: std::collections::BTreeMap<&str, &Resp> = seen.iter().map(|(k, v)| (k.as_str(), v)).collect();
    assert_eq!(by["no auth"].code(), "unauthorized");
    assert_eq!(by["bad token"].code(), "unauthorized");
    assert_eq!(by["scope"].code(), "insufficient_scope");
    assert_eq!(by["operator route"].code(), "insufficient_scope");
    assert_eq!(by["unknown service"].code(), "not_found");
    assert_eq!(by["garbage id"].code(), "not_found");
    assert_eq!(by["unknown version"].code(), "not_found");
    assert_eq!(by["slug taken"].code(), "slug_taken");
    assert_eq!(by["stale etag"].code(), "precondition_failed");
    assert_eq!(by["bad slug"].code(), "validation_failed");
    assert_eq!(by["bad runtime"].code(), "validation_failed");
    assert_eq!(by["limit above plan"].code(), "limit_exceeds_plan");
    assert_eq!(by["malformed json"].code(), "validation_failed");
    assert_eq!(by["missing field"].code(), "validation_failed");
    assert!(
        by["invalid artifact"].code() == "validation_failed" || by["invalid artifact"].code() == "invalid_artifact",
        "syntax errors are 422: {}",
        by["invalid artifact"].text
    );
    assert!(ERROR_CODES.len() >= 18);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-03
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_03_idempotency_key_replays_the_same_response_once() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let other = new_tenant(&node, "pro").await;
    let slug = uniq("idem");
    let body = json!({"slug": slug, "runtime": "deno"});
    let send_with = |key: String, b: Value, k: String| {
        let node = &node;
        async move {
            call(api(node, &key, Method::POST, "/v1/services").header("idempotency-key", k), Some(b)).await
        }
    };

    let first = send_with(t.key.clone(), body.clone(), "k-1".into()).await;
    let second = send_with(t.key.clone(), body.clone(), "k-1".into()).await;
    assert_eq!(first.s(), 201, "{}", first.text);
    assert_eq!(second.s(), first.s(), "second call replays the first status");
    assert_eq!(second.json, first.json, "second call returns the same body (same id)");

    let list = get(&node, &t.key, &format!("/v1/services?slug={slug}")).await;
    assert_eq!(list.items().len(), 1, "one service created, not two: {}", list.text);

    // Same key, different body: 422 idempotency_mismatch (and nothing is created).
    let other_slug = uniq("idem2");
    let mismatch = send_with(t.key.clone(), json!({"slug": other_slug, "runtime": "deno"}), "k-1".into()).await;
    assert_envelope(&mismatch, 422, "idempotency_mismatch");
    let none = get(&node, &t.key, &format!("/v1/services?slug={other_slug}")).await;
    assert!(none.items().is_empty(), "the mismatching request must not create a service");

    // Keys are per principal: another tenant can reuse the same key string.
    let theirs = send_with(other.key.clone(), json!({"slug": uniq("idem3"), "runtime": "deno"}), "k-1".into()).await;
    assert_eq!(theirs.s(), 201, "{}", theirs.text);
    assert_ne!(theirs.json["id"], first.json["id"]);

    // A different key creates normally (and conflicts on the slug, since it is taken).
    let again = send_with(t.key.clone(), body, "k-2".into()).await;
    assert_envelope(&again, 409, "slug_taken");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-04
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_04_stale_if_match_is_precondition_failed_on_patch_and_live() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("etag");
    let svc = ship(&node, &t.key, &slug, &guests::versioned(1)).await;
    let v2 = upload_ts(&node, &t.key, &svc, &guests::versioned(2), "activate=0").await;
    assert_eq!(v2.s(), 201, "{}", v2.text);

    let cur = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert_eq!(cur.s(), 200);
    let etag1 = cur.header("etag").expect("ETag header on services");
    assert_eq!(cur.json["etag"].as_str(), Some(etag1.as_str()), "body etag equals the header");

    // A matching If-Match goes through and bumps the etag.
    let ok = call(
        api(&node, &t.key, Method::PATCH, &format!("/v1/services/{svc}")).header("if-match", &etag1),
        Some(json!({"name": "first"})),
    )
    .await;
    assert_eq!(ok.s(), 200, "{}", ok.text);
    let etag2 = ok.header("etag").expect("ETag on the PATCH response");
    assert_ne!(etag1, etag2, "etag bumped by the write");

    // The old etag is now stale: PATCH and PUT /live both answer 412.
    let stale_patch = call(
        api(&node, &t.key, Method::PATCH, &format!("/v1/services/{svc}")).header("if-match", &etag1),
        Some(json!({"name": "second"})),
    )
    .await;
    assert_envelope(&stale_patch, 412, "precondition_failed");
    let stale_live = call(
        api(&node, &t.key, Method::PUT, &format!("/v1/services/{svc}/live")).header("if-match", &etag1),
        Some(json!({"version": 2})),
    )
    .await;
    assert_envelope(&stale_live, 412, "precondition_failed");
    let still = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert_eq!(still.json["live_version"], json!(1), "a rejected PUT /live changes nothing");
    assert_eq!(still.json["name"], json!("first"), "a rejected PATCH changes nothing");

    // With the fresh etag the flip works.
    let live = call(
        api(&node, &t.key, Method::PUT, &format!("/v1/services/{svc}/live")).header("if-match", &etag2),
        Some(json!({"version": 2})),
    )
    .await;
    assert_eq!(live.s(), 200, "{}", live.text);
    assert_eq!(live.json["live_version"], json!(2));
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-05
// ---------------------------------------------------------------------------------------------

async fn page_through(node: &impl Node, key: &str, limit: u32, pause: Duration) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut rb = api(node, key, Method::GET, "/v1/services").query(&[("limit", limit.to_string())]);
        if let Some(c) = &cursor {
            rb = rb.query(&[("cursor", c.clone())]);
        }
        let r = send(rb).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        pages.push(
            r.items()
                .iter()
                .map(|s| s["id"].as_str().expect("id").to_string())
                .collect(),
        );
        cursor = r.json["next_cursor"].as_str().map(str::to_string);
        if cursor.is_none() {
            return pages;
        }
        assert!(pages.len() < 100, "pagination does not terminate");
        tokio::time::sleep(pause).await;
    }
}

node_test! {
async fn ac_api_05_pagination_has_no_duplicates_or_gaps_even_while_services_are_created() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;

    // 120 services, created 10 at a time.
    let mut initial: Vec<String> = Vec::new();
    for chunk in 0..12 {
        let mut handles = Vec::new();
        for i in 0..10 {
            let (client, url, key) = (node.http().clone(), node.control_url("/v1/services"), t.key.clone());
            handles.push(tokio::spawn(async move {
                let r = client
                    .post(&url)
                    .bearer_auth(&key)
                    .json(&json!({"slug": format!("page-{chunk}-{i}"), "runtime": "deno"}))
                    .send()
                    .await
                    .expect("create");
                assert_eq!(r.status().as_u16(), 201);
                r.json::<Value>().await.expect("json")["id"].as_str().expect("id").to_string()
            }));
        }
        for h in handles {
            initial.push(h.await.expect("join"));
        }
    }
    assert_eq!(initial.len(), 120);

    // Static: exactly 3 pages of 50 / 50 / 20, in a stable order, no duplicates.
    let pages = page_through(&node, &t.key, 50, Duration::ZERO).await;
    let sizes: Vec<usize> = pages.iter().map(Vec::len).collect();
    assert_eq!(sizes, vec![50, 50, 20], "120 services with limit=50");
    let flat: Vec<&String> = pages.iter().flatten().collect();
    let unique: BTreeSet<&&String> = flat.iter().collect();
    assert_eq!(unique.len(), 120, "no duplicates");
    let expected: BTreeSet<&String> = initial.iter().collect();
    assert_eq!(flat.iter().copied().collect::<BTreeSet<_>>(), expected, "no gaps");

    // While 40 more services are being created, every page walk still sees each of the
    // original 120 exactly once and never a duplicate.
    let creator = {
        let (client, url, key) = (node.http().clone(), node.control_url("/v1/services"), t.key.clone());
        tokio::spawn(async move {
            for i in 0..40 {
                let r = client
                    .post(&url)
                    .bearer_auth(&key)
                    .json(&json!({"slug": format!("late-{i}"), "runtime": "deno"}))
                    .send()
                    .await
                    .expect("create");
                assert_eq!(r.status().as_u16(), 201);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let walk = page_through(&node, &t.key, 50, Duration::from_millis(30)).await;
    creator.await.expect("creator");
    let flat: Vec<String> = walk.into_iter().flatten().collect();
    let unique: BTreeSet<&String> = flat.iter().collect();
    assert_eq!(unique.len(), flat.len(), "no duplicates while services are created concurrently");
    for id in &initial {
        assert!(unique.contains(id), "service {id} created before paging started is missing (gap)");
    }
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-06
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_06_slug_in_use_is_409_and_invalid_slugs_are_422() {
    let node = start_node().await;
    let t1 = new_tenant(&node, "pro").await;
    let t2 = new_tenant(&node, "pro").await;
    let slug = uniq("slug");

    create_service(&node, &t1.key, &slug, "deno").await;
    let dup = post(&node, &t1.key, "/v1/services", json!({"slug": slug, "runtime": "deno"})).await;
    assert_envelope(&dup, 409, "slug_taken");
    // `/s/{slug}` is one flat namespace: another tenant cannot take it either.
    let theirs = post(&node, &t2.key, "/v1/services", json!({"slug": slug, "runtime": "deno"})).await;
    assert_envelope(&theirs, 409, "slug_taken");

    let long63 = format!("a{}", "b".repeat(62));
    let long64 = format!("a{}", "b".repeat(63));
    for bad in ["A-upper", "1starts-with-digit", "a", "has_underscore", "has space", "-leading", "", long64.as_str()] {
        let r = post(&node, &t1.key, "/v1/services", json!({"slug": bad, "runtime": "deno"})).await;
        assert_envelope(&r, 422, "validation_failed");
    }
    for good in ["ab", "a-1", long63.as_str()] {
        let r = post(&node, &t1.key, "/v1/services", json!({"slug": good, "runtime": "deno"})).await;
        assert_eq!(r.s(), 201, "slug `{good}` should be valid: {}", r.text);
    }
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-07
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_07_patch_replaces_only_the_provided_blocks_and_bumps_the_etag() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("patch");
    let created = post(
        &node,
        &t.key,
        "/v1/services",
        json!({
            "slug": slug, "runtime": "deno", "access": "key",
            "limits": {"wall_ms": 10000},
            "scaling": {"max_instances": 5},
            "egress": {"allowed_hosts": ["a.example.com"], "denied_hosts": []},
            "ingress": {"allowed_methods": ["GET"]},
        }),
    )
    .await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let svc = created.json["id"].as_str().expect("id").to_string();
    let before = get(&node, &t.key, &format!("/v1/services/{svc}")).await;

    let patched = patch(
        &node,
        &t.key,
        &format!("/v1/services/{svc}"),
        json!({"egress": {"allowed_hosts": ["api.telegram.org"], "denied_hosts": ["evil.example.com"]}}),
    )
    .await;
    assert_eq!(patched.s(), 200, "{}", patched.text);
    let after = get(&node, &t.key, &format!("/v1/services/{svc}")).await;

    assert_eq!(after.json["egress"]["allowed_hosts"], json!(["api.telegram.org"]), "egress replaced");
    assert_eq!(after.json["egress"]["denied_hosts"], json!(["evil.example.com"]));
    for block in ["limits", "scaling", "access", "triggers", "ingress", "profile", "slug", "runtime"] {
        assert_eq!(after.json[block], before.json[block], "block `{block}` must be unchanged by a PATCH of `egress`");
    }
    assert_eq!(after.json["limits"]["wall_ms"], json!(10000));
    assert_eq!(after.json["scaling"]["max_instances"], json!(5));
    assert_eq!(after.json["access"], json!("key"));
    assert_ne!(after.header("etag"), before.header("etag"), "etag bumped");
    assert_ne!(after.json["etag"], before.json["etag"]);

    // A provided block replaces the whole block (worker semantics): wall_ms is gone.
    let patched = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"limits": {"cpu_ms": 500}})).await;
    assert_eq!(patched.s(), 200, "{}", patched.text);
    let again = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert_eq!(again.json["limits"]["cpu_ms"], json!(500));
    assert_ne!(again.json["limits"]["wall_ms"], json!(10000), "the old limits block was replaced, not merged");
    assert_eq!(again.json["egress"], after.json["egress"]);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// D-B2: scaling.min_instances / max_instances
// ---------------------------------------------------------------------------------------------

node_test! {
async fn the_scaling_block_validates_and_normalizes_instance_bounds() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;

    // min above max is refused on create and on patch, naming both fields
    let bad = post(&node, &t.key, "/v1/services", json!({"slug": uniq("bad"), "runtime": "deno", "scaling": {"min_instances": 5, "max_instances": 2}})).await;
    assert_eq!((bad.s(), bad.code().as_str()), (422, "validation_failed"), "{}", bad.text);
    assert!(bad.text.contains("min_instances") && bad.text.contains("max_instances"), "{}", bad.text);

    // the old name `max_concurrency` is an alias on input and is never written back
    let slug = uniq("alias");
    let created = post(&node, &t.key, "/v1/services", json!({"slug": slug, "runtime": "deno", "scaling": {"max_concurrency": 7}})).await;
    assert_eq!(created.s(), 201, "{}", created.text);
    assert_eq!(created.json["scaling"]["max_instances"], json!(7), "{}", created.text);
    assert!(created.json["scaling"].get("max_concurrency").is_none(), "{}", created.text);
    assert_eq!(created.json["scaling"]["min_instances"], Value::Null);
    let svc = created.json["id"].as_str().expect("id").to_string();
    assert_eq!(created.json["effective"]["max_instances"], json!({"value": 7, "source": "service"}), "{}", created.text);
    assert_eq!(created.json["effective"]["min_instances"], json!({"value": 0, "source": "default"}), "{}", created.text);

    let both = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"scaling": {"max_concurrency": 3, "max_instances": 4}})).await;
    assert_eq!(both.s(), 422, "both names at once are ambiguous: {}", both.text);
    let crossed = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"scaling": {"min_instances": 9, "max_instances": 4}})).await;
    assert_eq!((crossed.s(), crossed.code().as_str()), (422, "validation_failed"), "{}", crossed.text);
    let ok = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"scaling": {"min_instances": 2, "max_instances": 4}})).await;
    assert_eq!(ok.s(), 200, "{}", ok.text);
    assert_eq!(ok.json["effective"]["min_instances"], json!({"value": 2, "source": "service"}), "{}", ok.text);
    assert_eq!(ok.json["effective"]["max_instances"], json!({"value": 4, "source": "service"}), "{}", ok.text);
    node.shutdown().await;
}
}

node_test! {
/// The tenant-wide `max_warm_floor_total` is checked in the transaction that writes the service:
/// requests at once cannot together exceed it, on create or on patch.
async fn the_warm_floor_total_holds_under_concurrent_requests() {
    let node = start_node_with(|c| {
        let mut capped = pro_plan();
        capped.max_warm_floor_total = 3;
        c.plans.insert("capped".to_string(), capped);
    })
    .await;
    let t = new_tenant(&node, "capped").await;
    let creates = (0..8).map(|i| {
        let (node, key) = (&node, t.key.as_str());
        async move { post(node, key, "/v1/services", json!({"slug": uniq(&format!("fl{i}")), "runtime": "deno", "scaling": {"min_instances": 1}})).await }
    });
    let results = futures_util::future::join_all(creates).await;
    let made: Vec<String> = results.iter().filter(|r| r.s() == 201).map(|r| r.json["id"].as_str().expect("id").to_string()).collect();
    assert_eq!(made.len(), 3, "{:?}", results.iter().map(|r| (r.s(), r.text.clone())).collect::<Vec<_>>());
    for r in results.iter().filter(|r| r.s() != 201) {
        assert_envelope(r, 422, "limit_exceeds_plan");
        assert_eq!(r.json["error"]["details"]["plan_max"], json!(3), "{}", r.text);
    }

    // lower one to 0 (total 2), then two raises 1 -> 2 at once: only one fits
    let low = patch(&node, &t.key, &format!("/v1/services/{}", made[0]), json!({"scaling": {"min_instances": 0}})).await;
    assert_eq!(low.s(), 200, "{}", low.text);
    let raises = [&made[1], &made[2]].map(|id| {
        let (node, key) = (&node, t.key.as_str());
        async move { patch(node, key, &format!("/v1/services/{id}"), json!({"scaling": {"min_instances": 2}})).await }
    });
    let raised = futures_util::future::join_all(raises).await;
    let codes: Vec<u16> = raised.iter().map(|r| r.s()).collect();
    assert_eq!(codes.iter().filter(|c| **c == 200).count(), 1, "{codes:?}");
    assert_eq!(codes.iter().filter(|c| **c == 422).count(), 1, "{codes:?}");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-08
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_08_limits_above_the_plan_are_rejected_never_clamped() {
    let node = start_node().await; // `free` plan: memory ≤ 128 MB, wall ≤ 30 s, cpu ≤ 1 s
    let t = new_tenant(&node, "free").await;
    let slug = uniq("plan");

    let too_big = post(&node, &t.key, "/v1/services", json!({"slug": slug, "runtime": "deno", "limits": {"memory_mb": 512}})).await;
    assert_envelope(&too_big, 422, "limit_exceeds_plan");
    let none = get(&node, &t.key, &format!("/v1/services?slug={slug}")).await;
    assert!(none.items().is_empty(), "no service was created with a clamped value");

    let ok = post(&node, &t.key, "/v1/services", json!({"slug": slug, "runtime": "deno", "limits": {"memory_mb": 128}})).await;
    assert_eq!(ok.s(), 201, "{}", ok.text);
    let svc = ok.json["id"].as_str().expect("id").to_string();
    assert_eq!(ok.json["limits"]["memory_mb"], json!(128));

    for (name, limits) in [
        ("memory", json!({"memory_mb": 512})),
        ("wall", json!({"wall_ms": 600_000})),
        ("cpu", json!({"cpu_ms": 60_000})),
    ] {
        let r = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"limits": limits})).await;
        assert_envelope(&r, 422, "limit_exceeds_plan");
        let now = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
        assert_eq!(now.json["limits"]["memory_mb"], json!(128), "{name}: the limits are unchanged after the rejection");
    }
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-09
// ---------------------------------------------------------------------------------------------

fn files_with_content(dir: &Path, wanted: &[u8], found: &mut usize) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            files_with_content(&p, wanted, found);
        } else if std::fs::metadata(&p)
            .map(|m| m.len() as usize == wanted.len())
            .unwrap_or(false)
            && std::fs::read(&p).map(|b| b == wanted).unwrap_or(false)
        {
            *found += 1;
        }
    }
}

node_test! {
async fn ac_api_09_same_source_twice_gives_two_versions_one_digest_one_artifact() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("dedup"), "deno").await;
    let src = "export default function (): Response { return new Response(\"dedup-me-9f3a\"); }";

    let a = upload_ts(&node, &t.key, &svc, src, "activate=0").await;
    let b = upload_ts(&node, &t.key, &svc, src, "activate=0").await;
    assert_eq!(a.s(), 201, "{}", a.text);
    assert_eq!(b.s(), 201, "{}", b.text);
    assert_ne!(a.json["version"], b.json["version"], "two different version numbers");
    assert_eq!(a.json["source_digest"], b.json["source_digest"], "same source ⇒ same digest");
    let digest = a.json["source_digest"].as_str().expect("digest");
    assert!(digest.starts_with("sha256:") && digest.len() == "sha256:".len() + 64, "digest format: {digest}");
    assert_eq!(a.json["build_state"], json!("pending"));

    let list = get(&node, &t.key, &format!("/v1/services/{svc}/versions")).await;
    assert_eq!(list.items().len(), 2, "{}", list.text);

    // Content-addressed storage: exactly one stored copy of those bytes.
    let mut copies = 0;
    files_with_content(node.data_dir(), src.as_bytes(), &mut copies);
    assert_eq!(copies, 1, "one stored artifact for two versions with the same source");

    // The stored source is downloadable for both versions.
    for v in [&a, &b] {
        let n = v.json["version"].as_u64().expect("version");
        let r = send(api(&node, &t.key, Method::GET, &format!("/v1/services/{svc}/versions/{n}/source"))).await;
        assert_eq!(r.s(), 200);
        assert_eq!(r.text, src);
    }
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-10
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_10_failed_versions_cannot_go_live_but_pending_ones_can() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = ship(&node, &t.key, &uniq("live"), &guests::versioned(1)).await;

    // A version that fails to build (eager compile so the failure is recorded now).
    let bad = upload_ts(&node, &t.key, &svc, guests::MISSING_IMPORT, "activate=0&compile=eager").await;
    assert!(bad.s() == 202 || bad.s() == 201, "upload of the broken version: {} {}", bad.s(), bad.text);
    let bad_v = bad.json["version"].as_u64().expect("version");
    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(60), "the broken version to reach `failed`", || async move {
        let r = get(n, k, &format!("/v1/services/{s}/versions/{bad_v}")).await;
        r.json["build_state"] == json!("failed")
    })
    .await;
    let v = get(&node, &t.key, &format!("/v1/services/{svc}/versions/{bad_v}")).await;
    assert!(v.json["failure"].is_object(), "a failed version carries its failure: {}", v.text);

    let r = put(&node, &t.key, &format!("/v1/services/{svc}/live"), json!({"version": bad_v})).await;
    assert_envelope(&r, 409, "version_not_ready");
    let svc_now = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert_eq!(svc_now.json["live_version"], json!(1), "live is unchanged after the rejection");

    // A pending (not yet built) version is accepted.
    let pending = upload_ts(&node, &t.key, &svc, &guests::versioned(3), "activate=0").await;
    assert_eq!(pending.s(), 201, "{}", pending.text);
    assert_eq!(pending.json["build_state"], json!("pending"));
    let pv = pending.json["version"].as_u64().expect("version");
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/live"), json!({"version": pv})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["live_version"], json!(pv));

    // Unknown versions are 404.
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/live"), json!({"version": 999})).await;
    assert_eq!(r.s(), 404, "{}", r.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-11
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_11_data_export_is_a_valid_sqlite_file_with_the_same_rows() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("export");
    let svc = ship(&node, &t.key, &slug, guests::NOTES).await;
    for body in ["first note", "second note", "third note"] {
        let r = invoke_with(&node, &slug, Method::POST, "/", &[], body).await;
        assert_eq!(r.s(), 201, "{}", r.text);
    }

    let resp = api(&node, &t.key, Method::GET, &format!("/v1/services/{svc}/data/export")).send().await.expect("export");
    assert_eq!(resp.status().as_u16(), 200);
    let bytes = resp.bytes().await.expect("export body");
    assert!(bytes.starts_with(b"SQLite format 3\0"), "not a SQLite database file");

    // Reopen it with the storage layer and compare the rows.
    let sid: ServiceId = svc.parse().expect("service id");
    let tmp = tempfile::tempdir().expect("tmp");
    for dir in [svc.clone(), sid.bare()] {
        let d = tmp.path().join("services").join(dir);
        std::fs::create_dir_all(&d).expect("mkdir");
        std::fs::write(d.join("data.db"), &bytes).expect("write export");
    }
    let store = LocalServiceData::open(DataConfig::new(tmp.path())).expect("open exported data");
    let res = store
        .sql(sid)
        .execute(SqlStatement { sql: "SELECT body FROM notes ORDER BY id".into(), args: vec![] })
        .await
        .expect("query the export");
    let rows: Vec<String> = res
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Text(s) => s.clone(),
            other => panic!("unexpected value {other:?}"),
        })
        .collect();
    assert_eq!(rows, vec!["first note", "second note", "third note"]);

    // Scope data:admin is required.
    let weak = new_tenant_scopes(&node, "pro", &["services:read"]).await;
    let denied = get(&node, &weak.key, &format!("/v1/services/{svc}/data/export")).await;
    assert!(denied.s() == 403 || denied.s() == 404, "{}", denied.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-12
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_12_operator_token_with_x_rt_tenant_acts_on_behalf_of_a_tenant() {
    let node = start_node().await;
    let t1 = new_tenant(&node, "pro").await;
    let t2 = new_tenant(&node, "pro").await;
    let slug = uniq("onbehalf");

    let r = call(
        root_for(&node, &t1.id, Method::POST, "/v1/services"),
        Some(json!({"slug": slug, "runtime": "deno"})),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    assert_eq!(r.json["tenant_id"], json!(t1.id), "created under the tenant named by x-rt-tenant");
    let svc = r.json["id"].as_str().expect("id").to_string();

    // The tenant sees it; the other tenant does not.
    assert_eq!(get(&node, &t1.key, &format!("/v1/services/{svc}")).await.s(), 200);
    assert_eq!(get(&node, &t2.key, &format!("/v1/services/{svc}")).await.s(), 404);

    // A tenant key cannot use the header to cross into another tenant.
    let sneaky = call(
        api(&node, &t1.key, Method::POST, "/v1/services").header("x-rt-tenant", &t2.id),
        Some(json!({"slug": uniq("sneaky"), "runtime": "deno"})),
    )
    .await;
    if sneaky.s() == 201 {
        assert_eq!(sneaky.json["tenant_id"], json!(t1.id), "the header must be ignored for tenant keys");
    } else {
        assert!(sneaky.s() == 403 || sneaky.s() == 404 || sneaky.s() == 422, "{}", sneaky.text);
    }
    let t2_list = get(&node, &t2.key, "/v1/services").await;
    assert!(t2_list.items().iter().all(|s| s["tenant_id"] == json!(t2.id)), "t2 never sees t1's services");

    // An unknown tenant in the header is a 404 for the operator.
    let r = call(
        root_for(&node, "ten_01J00000000000000000000000", Method::POST, "/v1/services"),
        Some(json!({"slug": uniq("ghost"), "runtime": "deno"})),
    )
    .await;
    assert!(r.s() == 404 || r.s() == 422, "unknown tenant: {} {}", r.s(), r.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-13
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_13_key_secret_is_shown_once_and_never_listed_again() {
    let node = start_node().await;
    let tenant = create_tenant(&node, "pro").await;
    let r = call(
        root(&node, Method::POST, &format!("/v1/tenants/{tenant}/keys")),
        Some(json!({"scopes": ["deploy", "invoke"], "name": "ci"})),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let secret = r.json["secret"].as_str().expect("secret is returned at creation").to_string();
    let id = r.json["id"].as_str().expect("key id").to_string();
    assert!(secret.starts_with("rtk_"), "secret format rtk_<id>_<secret>: {secret}");
    assert!(secret.contains(id.trim_start_matches("key_")), "the key id is embedded in the secret");
    assert!(secret.len() > 40, "32 bytes of base62 secret: {secret}");

    let list = call(root(&node, Method::GET, &format!("/v1/tenants/{tenant}/keys")), None).await;
    assert_eq!(list.s(), 200, "{}", list.text);
    let items = list.items();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], json!(id));
    assert_eq!(items[0]["scopes"], json!(["deploy", "invoke"]));
    assert!(items[0].get("secret").is_none(), "no `secret` field when listing");
    let secret_part = secret.rsplit('_').next().expect("secret part");
    assert!(!list.text.contains(secret_part), "the secret never appears in a listing: {}", list.text);

    // The listing does not leak it through other routes either.
    let tenant_view = call(root(&node, Method::GET, &format!("/v1/tenants/{tenant}")), None).await;
    assert!(!tenant_view.text.contains(secret_part));
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-14
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_14_node_scheduler_snapshot_is_operator_only_and_complete() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("load");
    ship(&node, &t.key, &slug, guests::HELLO).await;
    for _ in 0..10 {
        assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    }

    let r = call(root(&node, Method::GET, "/v1/node/scheduler"), None).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let snap = &r.json;
    assert!(snap["procs"].as_array().is_some_and(|p| !p.is_empty()), "per-P entries: {}", r.text);
    for p in snap["procs"].as_array().expect("procs") {
        assert!(p["kind"].is_string() || p["kind"].is_object(), "P kind: {p}");
        assert!(p["lrq_len"].is_number(), "LRQ length: {p}");
        assert!(p.get("runnext").is_some(), "runnext: {p}");
    }
    for m in snap["machines"].as_array().expect("machines") {
        assert!(m["bound_count"].is_number(), "bound counts per M: {m}");
    }
    for kind in ["deno", "wasm", "build"] {
        assert!(snap["grq"][kind].is_number(), "GRQ length for {kind}: {}", snap["grq"]);
    }
    assert!(snap["spinning"].is_number() && snap["idle"].is_number(), "spinning/idle counts");
    assert!(snap["tasks_submitted"].as_u64().unwrap_or(0) >= 10, "the load shows up: {}", r.text);

    // Matches the processor count reported by /status.
    let status = call(root(&node, Method::GET, "/v1/node/status"), None).await;
    assert_eq!(status.s(), 200, "{}", status.text);
    assert_eq!(
        snap["procs"].as_array().expect("procs").len() as u64,
        status.json["processors"].as_u64().expect("processors")
            + status.json["kinds"]["build"].as_u64().unwrap_or(0),
        "snapshot P count == status.processors (+ build Ps)"
    );

    // Tenant keys get 403.
    let denied = get(&node, &t.key, "/v1/node/scheduler").await;
    assert_envelope(&denied, 403, "insufficient_scope");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// Smoke coverage of the rest of the route map
// ---------------------------------------------------------------------------------------------

node_test! {
async fn node_routes_status_config_cache() {
    let node = start_node().await;
    let st = call(root(&node, Method::GET, "/v1/node/status"), None).await;
    assert_eq!(st.s(), 200, "{}", st.text);
    assert_eq!(st.json["v8_sandbox"], json!(false), "prebuilt V8 ⇒ v8_sandbox:false (SEC-36)");
    assert_eq!(st.json["mode"], json!("normal"));
    assert!(st.json["version"].is_string());
    assert!(st.json["default_profile"].is_string());

    // Live-updatable knobs apply; restart-only keys come back as pending_restart (CFG-05).
    let r = call(root(&node, Method::PUT, "/v1/node/config"), Some(json!({"default_profile": "min_latency"}))).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(r.json["applied"].is_array() && r.json["pending_restart"].is_array(), "{}", r.text);
    let st = call(root(&node, Method::GET, "/v1/node/status"), None).await;
    assert_eq!(st.json["default_profile"], json!("min_latency"));
    let bad = call(root(&node, Method::PUT, "/v1/node/config"), Some(json!({"default_profile": "warp_speed"}))).await;
    assert_envelope(&bad, 422, "validation_failed");
    let bad_mode = call(root(&node, Method::PUT, "/v1/node/config"), Some(json!({"degradation_mode": "sideways"}))).await;
    assert_envelope(&bad_mode, 422, "validation_failed");

    let cache = call(root(&node, Method::GET, "/v1/node/cache"), None).await;
    assert_eq!(cache.s(), 200, "{}", cache.text);
    assert!(cache.json.is_object());
    node.shutdown().await;
}
}

node_test! {
async fn tenants_crud_usage_and_purge() {
    let node = start_node().await;
    let created = call(
        root(&node, Method::POST, "/v1/tenants"),
        Some(json!({"name": "acme", "plan": "pro", "overrides": {"max_services": 5}})),
    )
    .await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let id = created.json["id"].as_str().expect("id").to_string();
    assert!(id.starts_with("ten_"));
    assert_eq!(created.json["plan"], json!("pro"));

    let bad_plan = call(root(&node, Method::POST, "/v1/tenants"), Some(json!({"name": "x", "plan": "does-not-exist"}))).await;
    assert_envelope(&bad_plan, 422, "validation_failed");

    let list = call(root(&node, Method::GET, "/v1/tenants"), None).await;
    assert!(list.items().iter().any(|t| t["id"] == json!(id)));
    let one = call(root(&node, Method::GET, &format!("/v1/tenants/{id}")), None).await;
    assert_eq!(one.json["name"], json!("acme"));

    let patched = call(root(&node, Method::PATCH, &format!("/v1/tenants/{id}")), Some(json!({"name": "acme-2", "disabled": false}))).await;
    assert_eq!(patched.s(), 200, "{}", patched.text);
    assert_eq!(patched.json["name"], json!("acme-2"));

    // Override enforcement: max_services = 5.
    let (_, key) = make_key(&node, &id, &ALL_SCOPES, None).await;
    for i in 0..5 {
        create_service(&node, &key, &format!("ov-{i}"), "deno").await;
    }
    let sixth = post(&node, &key, "/v1/services", json!({"slug": "ov-5", "runtime": "deno"})).await;
    assert_envelope(&sixth, 403, "quota_exceeded");

    let usage = call(root(&node, Method::GET, &format!("/v1/tenants/{id}/usage?from=2020-01-01&to=2100-01-01")), None).await;
    assert_eq!(usage.s(), 200, "{}", usage.text);
    assert!(usage.json.is_array() || usage.json["items"].is_array(), "{}", usage.text);

    // Boolean query values: `1|true|yes|on`, any case.
    let del = call(root(&node, Method::DELETE, &format!("/v1/tenants/{id}?purge=YES")), None).await;
    assert_eq!(del.s(), 204, "{}", del.text);
    let gone = call(root(&node, Method::GET, &format!("/v1/tenants/{id}")), None).await;
    assert_envelope(&gone, 404, "not_found");
    assert_eq!(get(&node, &key, "/v1/services").await.s(), 401, "keys of a deleted tenant stop working");
    node.shutdown().await;
}
}

node_test! {
async fn service_resource_shape_and_lifecycle() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("shape");
    let svc = ship(&node, &t.key, &slug, guests::HELLO).await;

    let r = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    for f in ["id", "tenant_id", "slug", "runtime", "live_version", "etag", "limits", "scaling", "access", "triggers", "ingress", "egress", "status", "effective"] {
        assert!(r.json.get(f).is_some(), "service resource lacks `{f}`: {}", r.text);
    }
    assert_eq!(r.json["id"], json!(svc));
    assert_eq!(r.json["tenant_id"], json!(t.id));
    assert_eq!(r.json["slug"], json!(slug));
    assert_eq!(r.json["runtime"], json!("deno"));
    assert_eq!(r.json["live_version"], json!(1));
    assert_eq!(r.json["access"], json!("public"));
    for k in ["http", "interval", "cron", "queue", "call"] {
        assert!(r.json["triggers"][k].is_boolean(), "triggers.{k}");
    }
    for k in ["build_state", "tier", "inflight", "last_error"] {
        assert!(r.json["status"].get(k).is_some(), "status.{k}");
    }
    assert!(r.json["effective"].is_object() && !r.json["effective"].as_object().expect("obj").is_empty());

    // Lookup by slug (contracts C-9): a list with 0 or 1 item.
    let found = get(&node, &t.key, &format!("/v1/services?slug={slug}")).await;
    assert_eq!(found.items().len(), 1);
    assert_eq!(found.items()[0]["id"], json!(svc));
    let nothing = get(&node, &t.key, "/v1/services?slug=nope-nope").await;
    assert_eq!(nothing.items().len(), 0);

    // Versions: list, get, source, rebuild.
    let versions = get(&node, &t.key, &format!("/v1/services/{svc}/versions")).await;
    assert_eq!(versions.items().len(), 1);
    let v1 = get(&node, &t.key, &format!("/v1/services/{svc}/versions/1")).await;
    for f in ["version", "source_digest", "build_state", "created_at"] {
        assert!(v1.json.get(f).is_some(), "version lacks `{f}`: {}", v1.text);
    }
    let src = send(api(&node, &t.key, Method::GET, &format!("/v1/services/{svc}/versions/1/source"))).await;
    assert_eq!(src.text, guests::HELLO);
    let rebuild = call(api(&node, &t.key, Method::POST, &format!("/v1/services/{svc}/versions/1/rebuild")), None).await;
    assert!(rebuild.s() == 202 || rebuild.s() == 200, "{}", rebuild.text);

    // Delete (`keep_data` accepted in any boolean spelling), then it is gone.
    let del = delete(&node, &t.key, &format!("/v1/services/{svc}?keep_data=On")).await;
    assert_eq!(del.s(), 204, "{}", del.text);
    assert_envelope(&get(&node, &t.key, &format!("/v1/services/{svc}")).await, 404, "not_found");
    assert_eq!(invoke(&node, &slug, "/").await.s(), 404, "a deleted service no longer routes");
    node.shutdown().await;
}
}

node_test! {
async fn env_secrets_hosts_schedules_queue_roundtrips() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = ship(&node, &t.key, &uniq("misc"), guests::HELLO).await;

    // env: whole-map replace (contracts C-2).
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/env"), json!({"A": "1", "B": "2"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/env"), json!({"C": "3"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let env = get(&node, &t.key, &format!("/v1/services/{svc}/env")).await;
    assert_eq!(env.json, json!({"C": "3"}), "PUT replaces the whole map");

    // secrets: names only; values are write-only.
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/secrets/DB_URL"), json!({"value": "postgres://hunter2"})).await;
    assert!(r.s() == 204 || r.s() == 200, "{}", r.text);
    let names = get(&node, &t.key, &format!("/v1/services/{svc}/secrets")).await;
    assert!(names.items().iter().any(|s| s["name"] == json!("DB_URL")), "{}", names.text);
    assert!(!names.text.contains("hunter2"), "secret values are never returned");
    let r = delete(&node, &t.key, &format!("/v1/services/{svc}/secrets/DB_URL")).await;
    assert_eq!(r.s(), 204, "{}", r.text);
    assert!(get(&node, &t.key, &format!("/v1/services/{svc}/secrets")).await.items().is_empty());

    // hosts
    let r = call(api(&node, &t.key, Method::PUT, &format!("/v1/services/{svc}/hosts/app.bots.example.com")), None).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["status"], json!("pending_verification"));
    assert!(r.json["verify_token"].is_string());
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}/hosts")).await.items().len(), 1);
    let r = delete(&node, &t.key, &format!("/v1/services/{svc}/hosts/app.bots.example.com")).await;
    assert_eq!(r.s(), 204, "{}", r.text);

    // schedules (contracts C-1)
    let s = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "interval", "every_seconds": 3600, "payload": {"k": 1}})).await;
    assert_eq!(s.s(), 201, "{}", s.text);
    let sid = s.json["id"].as_str().expect("id").to_string();
    assert!(sid.starts_with("sch_"));
    assert_eq!(s.json["kind"], json!("interval"));
    assert_eq!(s.json["every_seconds"], json!(3600));
    assert_eq!(s.json["payload"], json!({"k": 1}));
    let c = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "cron", "cron": "*/5 * * * *", "timezone": "Asia/Tashkent"})).await;
    assert_eq!(c.s(), 201, "{}", c.text);
    assert_eq!(c.json["timezone"], json!("Asia/Tashkent"));
    assert!(c.json["next_due"].is_number(), "next_due is reported: {}", c.text);
    let bad_cron = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "cron", "cron": "nope", "timezone": "UTC"})).await;
    assert_envelope(&bad_cron, 422, "validation_failed");
    let bad_tz = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "cron", "cron": "* * * * *", "timezone": "Mars/Base"})).await;
    assert_envelope(&bad_tz, 422, "validation_failed");
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}/schedules")).await.items().len(), 2);
    let p = patch(&node, &t.key, &format!("/v1/services/{svc}/schedules/{sid}"), json!({"kind": "interval", "every_seconds": 7200, "enabled": false})).await;
    assert_eq!(p.s(), 200, "{}", p.text);
    assert_eq!(p.json["every_seconds"], json!(7200));
    assert_eq!(p.json["enabled"], json!(false));
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{svc}/schedules/{sid}")).await.s(), 204);
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}/schedules")).await.items().len(), 1);

    // queue + dlq endpoints (a delayed message stays queued)
    let q = post(&node, &t.key, &format!("/v1/services/{svc}/queue"), json!({"body": {"n": 1}, "delay_seconds": 3600, "dedup_id": "d-1"})).await;
    assert_eq!(q.s(), 202, "{}", q.text);
    assert!(q.json["id"].as_str().is_some_and(|i| i.starts_with("msg_")));
    let stats = get(&node, &t.key, &format!("/v1/services/{svc}/queue")).await;
    assert_eq!(stats.json["depth"], json!(1), "{}", stats.text);
    for f in ["in_flight", "oldest_age_ms"] {
        assert!(stats.json.get(f).is_some(), "queue stats lack {f}: {}", stats.text);
    }
    let too_late = post(&node, &t.key, &format!("/v1/services/{svc}/queue"), json!({"body": 1, "delay_seconds": 8 * 24 * 3600})).await;
    assert_envelope(&too_late, 422, "validation_failed");
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}/dlq")).await.s(), 200);
    let rd = call(api(&node, &t.key, Method::POST, &format!("/v1/services/{svc}/dlq/redrive")), None).await;
    assert_eq!(rd.s(), 200, "{}", rd.text);
    assert_eq!(rd.json["redriven"], json!(0));
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{svc}/dlq")).await.s(), 204);

    // observability endpoints answer with their documented shapes
    let logs = get(&node, &t.key, &format!("/v1/services/{svc}/logs")).await;
    assert_eq!(logs.s(), 200, "{}", logs.text);
    assert!(logs.json["items"].is_array());
    let stats = get(&node, &t.key, &format!("/v1/services/{svc}/stats?window=5m")).await;
    assert_eq!(stats.s(), 200, "{}", stats.text);
    let bad_window = get(&node, &t.key, &format!("/v1/services/{svc}/stats?window=3y")).await;
    assert_envelope(&bad_window, 422, "validation_failed");
    let events = get(&node, &t.key, &format!("/v1/services/{svc}/events")).await;
    assert_eq!(events.s(), 200, "{}", events.text);
    assert!(events.json["items"].is_array());
    node.shutdown().await;
}
}

node_test! {
async fn upload_validation_errors_are_422_and_create_no_version() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("val"), "deno").await;

    // BLD-01: a syntax error is rejected with file/line/col and creates no version.
    let r = upload_ts(&node, &t.key, &svc, "export default function ( {{{", "activate=1").await;
    assert_eq!(r.s(), 422, "{}", r.text);
    assert!(r.json["error"]["details"].is_array() || r.json["error"]["details"].is_object(), "diagnostics in details: {}", r.text);
    // BLD-02: forbidden specifiers.
    for src in [
        "import x from \"file:///etc/passwd\"; export default () => new Response(String(x));",
        "import { exec } from \"node:child_process\"; export default () => new Response(String(exec));",
        "import x from \"sdk:does-not-exist\"; export default () => new Response(String(x));",
    ] {
        let r = upload_ts(&node, &t.key, &svc, src, "activate=0").await;
        assert_eq!(r.s(), 422, "{src}: {}", r.text);
    }
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}/versions")).await.items().len(), 0, "no version was created");

    // BLD-17 / SEC-31: oversized sources are 413.
    let big = "x".repeat(2 * 1024 * 1024);
    let r = upload_ts(&node, &t.key, &svc, &format!("// {big}\nexport default () => new Response('x');"), "activate=0").await;
    assert_envelope(&r, 413, "too_large");
    // A runtime mismatch: a wasm upload to a deno service.
    let r = upload(&node, &t.key, &svc, "application/wasm", b"\0asm\x01\0\0\0".to_vec(), "activate=0").await;
    assert!(r.s() == 422 || r.s() == 415, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn uploads_buffered_at_once_are_capped_per_node() {
    // Security review finding 4: every upload is buffered in memory, so only a few run at once
    // and each one is charged to the memory governor before its body is read.
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("up"), "deno").await;
    let src = "export default () => new Response('x');";
    let mut held = Vec::new();
    for _ in 0..rt_api::MAX_CONCURRENT_UPLOADS {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
        tx.send(Ok(Bytes::from_static(b"// "))).await.unwrap();
        let stream = futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|c| (c, rx)) });
        let rb = api(&node, &t.key, Method::POST, &format!("/v1/services/{svc}/versions?activate=0"))
            .header("content-type", "application/typescript")
            .body(reqwest::Body::wrap_stream(stream));
        held.push((tx, tokio::spawn(async move { send(rb).await })));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let r = upload_ts(&node, &t.key, &svc, src, "activate=0").await;
    assert_envelope(&r, 429, "rate_limited");

    for (tx, task) in held {
        tx.send(Ok(Bytes::from(format!("held\n{src}")))).await.unwrap();
        drop(tx);
        let r = task.await.unwrap();
        assert!(r.s() < 300, "a held upload completes once its body ends: {}", r.text);
    }
    let r = upload_ts(&node, &t.key, &svc, src, "activate=0").await;
    assert!(r.s() < 300, "the slots are free again: {}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn one_off_schedules_are_listed_on_their_own_paged_route() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("once"), "deno").await;
    let sid: ServiceId = svc.parse().expect("service id");
    let once = format!("/v1/services/{svc}/schedules/once");

    // `std:schedule` keeps its one-off schedules in queue.db; the test writes the same rows
    // through a second handle on that file.
    let db = SqliteQueueStore::open(&node.data_dir().join("queue.db"), 5).expect("open queue.db");
    for (id, run_at_ms) in [("b-2", 1_900_000_000_200i64), ("a-1", 1_900_000_000_100), ("c-3", 1_900_000_000_300)] {
        let put = db.put(sid, NewDelayed { id: id.into(), run_at_ms, payload: Bytes::from_static(b"{}") }).await;
        assert!(put.expect("put").is_some(), "{id} stored");
    }

    // Listed by id, with the run time and no recurring fields.
    let r = get(&node, &t.key, &once).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let all = json!([
        {"id": "a-1", "kind": "once", "run_at": 1_900_000_000_100i64},
        {"id": "b-2", "kind": "once", "run_at": 1_900_000_000_200i64},
        {"id": "c-3", "kind": "once", "run_at": 1_900_000_000_300i64},
    ]);
    assert_eq!(r.json, json!({"items": all, "next_cursor": null}), "{}", r.text);

    // Pages follow the id order; a full page carries the cursor of the next one.
    let p1 = get(&node, &t.key, &format!("{once}?limit=2")).await;
    assert_eq!(p1.json["items"], json!([all[0], all[1]]), "{}", p1.text);
    assert_eq!(p1.json["next_cursor"], json!("b-2"));
    let p2 = get(&node, &t.key, &format!("{once}?limit=2&cursor=b-2")).await;
    assert_eq!(p2.json["items"], json!([all[2]]), "{}", p2.text);
    assert!(p2.json["next_cursor"].is_null(), "{}", p2.text);

    // The recurring listing keeps its own entries only.
    let s = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "interval", "every_seconds": 3600})).await;
    assert_eq!(s.s(), 201, "{}", s.text);
    let recurring = get(&node, &t.key, &format!("/v1/services/{svc}/schedules")).await.items();
    assert_eq!(recurring.len(), 1, "one-off schedules are not in the recurring listing: {recurring:?}");
    assert_eq!(recurring[0]["kind"], json!("interval"));

    let missing = get(&node, &t.key, "/v1/services/svc_missing/schedules/once").await;
    assert_envelope(&missing, 404, "not_found");
    node.shutdown().await;
}
}

node_test! {
/// mrt_api_05: the effective view shows `min_run_time_ms` with its source: `service` when the owner
/// set it, and `default` (the executor's idle TTL applies) when it is unset.
async fn mrt_api_05_effective_shows_min_run_time_and_its_source() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let created = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": uniq("mrt05"), "runtime": "deno", "scaling": {"min_run_time_ms": 5000}}),
    )
    .await;
    assert_eq!(created.s(), 201, "{}", created.text);
    assert_eq!(
        created.json["effective"]["min_run_time_ms"],
        json!({"value": 5000, "source": "service"}),
        "{}",
        created.text
    );
    let svc = created.json["id"].as_str().expect("id").to_string();

    // a PATCH replaces the scaling block: without `min_run_time_ms` the keep-alive is inherited
    let unset = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"scaling": {"max_instances": 3}})).await;
    assert_eq!(unset.s(), 200, "{}", unset.text);
    assert_eq!(
        unset.json["effective"]["min_run_time_ms"],
        json!({"value": null, "source": "default"}),
        "{}",
        unset.text
    );
    let view = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert_eq!(view.json["effective"]["min_run_time_ms"]["source"], json!("default"), "{}", view.text);
    node.shutdown().await;
}
}

node_test! {
/// fair_api_01: the service stats report the predicted slot time. After a few HTTP tasks,
/// `admission.pred_ms` is a number near the handler's run time, and `pred_background_ms` is present
/// (`null` while the service has run no background task).
async fn fair_api_01_stats_report_the_predicted_run_time() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("fair01");
    let created = post(&node, &t.key, "/v1/services", json!({"slug": slug, "runtime": "deno"})).await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let svc = created.json["id"].as_str().expect("id").to_string();
    let up = upload_ts(&node, &t.key, &svc, &guests::slow(1, 200), "activate=1").await;
    assert_eq!(up.s(), 201, "{}", up.text);
    for _ in 0..3 {
        assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    }

    let stats = get(&node, &t.key, &format!("/v1/services/{svc}/stats?window=5m")).await;
    assert_eq!(stats.s(), 200, "{}", stats.text);
    let adm = &stats.json["admission"];
    let pred = adm["pred_ms"].as_f64().unwrap_or_else(|| panic!("pred_ms is a number after samples: {}", stats.text));
    assert!(pred >= 100.0 && pred < 5_000.0, "a 200 ms handler predicts about 200 ms, got {pred}: {}", stats.text);
    assert!(adm.get("pred_background_ms").is_some(), "the field is always present: {}", stats.text);
    assert_eq!(adm["pred_background_ms"], Value::Null, "no background task ran: {}", stats.text);
    node.shutdown().await;
}
}
