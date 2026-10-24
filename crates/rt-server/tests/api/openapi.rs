//! API-01: the OpenAPI document and the contract test generated from it.

use std::collections::BTreeSet;

use reqwest::Method;
use serde_json::{json, Value};

use crate::common::openapi::{documented_status_for, response_schema, sample, validate, ROUTE_MAP};
use crate::common::*;

async fn fetch_doc(node: &impl Node) -> Value {
    let r = call(root(node, Method::GET, "/v1/openapi.json"), None).await;
    assert_eq!(r.s(), 200, "GET /v1/openapi.json: {}", r.text);
    assert!(
        r.header("content-type").unwrap_or_default().contains("json"),
        "openapi.json must be served as JSON"
    );
    r.json
}

fn collect_refs(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(m) => {
            for (k, val) in m {
                if k == "$ref" {
                    if let Some(s) = val.as_str() {
                        out.insert(s.to_string());
                    }
                } else {
                    collect_refs(val, out);
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|x| collect_refs(x, out)),
        _ => {}
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_api_01_openapi_document_is_valid_and_covers_the_route_map() {
    let node = start_node().await;
    let doc = fetch_doc(&node).await;

    let version = doc["openapi"].as_str().unwrap_or_default();
    assert!(
        version.starts_with("3.1"),
        "OpenAPI 3.1 document expected, got `{version}`"
    );
    assert!(
        doc["info"]["title"].as_str().is_some_and(|t| !t.is_empty()),
        "info.title"
    );
    assert!(
        doc["info"]["version"].as_str().is_some_and(|t| !t.is_empty()),
        "info.version"
    );

    let mut missing = Vec::new();
    for (m, p) in ROUTE_MAP {
        if !doc["paths"][*p][*m].is_object() {
            missing.push(format!("{} {p}", m.to_uppercase()));
        }
    }
    assert!(missing.is_empty());

    // Every operation documents at least one response, and every `$ref` resolves.
    for (path, item) in doc["paths"].as_object().expect("paths") {
        for (method, op) in item.as_object().expect("path item") {
            if !["get", "post", "put", "patch", "delete"].contains(&method.as_str()) {
                continue;
            }
            assert!(
                op["responses"].as_object().is_some_and(|r| !r.is_empty()),
                "{} {path} has no documented responses",
                method.to_uppercase()
            );
        }
    }
    let mut refs = BTreeSet::new();
    collect_refs(&doc, &mut refs);
    for r in &refs {
        assert!(crate::common::openapi::resolve(&doc, r).is_some(), "dangling $ref {r}");
    }

    // The error envelope is a component of its own.
    let env = &doc["components"]["schemas"]["ErrorEnvelope"];
    assert!(env.is_object(), "components.schemas.ErrorEnvelope");
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_api_01_scalar_page_is_served() {
    let node = start_node().await;
    let r = send(node.http().get(node.control_url("/docs"))).await;
    assert_eq!(r.s(), 200, "GET /docs");
    assert!(
        r.header("content-type").unwrap_or_default().contains("html"),
        "content-type: {:?}",
        r.header("content-type")
    );
    assert!(
        r.text.to_lowercase().contains("scalar"),
        "the Scalar API reference page"
    );
    node.shutdown().await;
}

/// Curated request bodies for the mutating operations (the rest is generated from the schema).
fn curated_body(method: &str, path: &str) -> Option<(&'static str, Vec<u8>)> {
    let j = |v: Value| Some(("application/json", serde_json::to_vec(&v).expect("json")));
    match (method, path) {
        ("put", "/v1/node/config") => j(json!({"degradation_mode": "normal"})),
        ("post", "/v1/tenants") => j(json!({"name": "contract-extra", "plan": "pro"})),
        ("patch", "/v1/tenants/{t}") => j(json!({"name": "renamed-by-contract"})),
        ("post", "/v1/tenants/{t}/keys") => j(json!({"scopes": ["invoke"]})),
        ("post", "/v1/keys") => j(json!({"scopes": ["invoke"]})),
        ("post", "/v1/projects") => j(json!({"slug": "contract-project", "environments": ["production", "staging"]})),
        ("patch", "/v1/projects/{p}") => j(json!({"name": "Renamed by contract"})),
        ("post", "/v1/services") => j(json!({"slug": "contract-extra", "runtime": "deno"})),
        ("patch", "/v1/services/{s}") => j(json!({"egress": {"allowed_hosts": ["example.com"], "denied_hosts": []}})),
        ("post", "/v1/services/{s}/versions") => Some(("application/typescript", guests::HELLO.as_bytes().to_vec())),
        ("put", "/v1/services/{s}/live") => j(json!({"version": 1})),
        ("put", "/v1/services/{s}/env") => j(json!({"A": "1", "B": "2"})),
        ("put", "/v1/services/{s}/secrets/{name}") => j(json!({"value": "contract-secret"})),
        ("post", "/v1/services/{s}/schedules") => {
            j(json!({"kind": "interval", "every_seconds": 60, "payload": {"a": 1}}))
        }
        ("patch", "/v1/services/{s}/schedules/{id}") => j(json!({"kind": "interval", "every_seconds": 120})),
        ("post", "/v1/services/{s}/queue") => j(json!({"body": {"x": 1}})),
        _ => None,
    }
}

/// API-01: generate requests from `/v1/openapi.json` and validate every response against the
/// schema the document declares for its status. Statuses the document does not declare must be
/// control errors in the documented envelope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_api_01_every_response_validates_against_the_openapi_schemas() {
    let node = start_node().await;
    let doc = fetch_doc(&node).await;

    // Fixtures so that path parameters point at real resources.
    let t = new_tenant(&node, "pro").await;
    let svc = ship(&node, &t.key, &uniq("contract"), guests::HELLO).await;
    let (key2_id, _key2) = make_key(&node, &t.id, &["invoke"], None).await;
    let r = put(&node, &t.key, &format!("/v1/services/{svc}/env"), json!({"SEED": "1"})).await;
    assert_eq!(r.s(), 200, "seed env: {}", r.text);
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/secrets/API_SECRET"),
        json!({"value": "seed"}),
    )
    .await;
    assert!(r.s() == 204 || r.s() == 200, "seed secret: {} {}", r.s(), r.text);
    let sched = post(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/schedules"),
        json!({"kind": "interval", "every_seconds": 3600, "payload": {}}),
    )
    .await;
    assert_eq!(sched.s(), 201, "seed schedule: {}", sched.text);
    let sched_id = sched.json["id"].as_str().expect("schedule id").to_string();
    let q = post(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/queue"),
        json!({"body": {"seed": true}, "delay_seconds": 3600}),
    )
    .await;
    assert_eq!(q.s(), 202, "seed queue message: {}", q.text);
    let h = call(
        root_for(
            &node,
            &t.id,
            Method::PUT,
            &format!("/v1/services/{svc}/hosts/contract.bots.example.com"),
        ),
        None,
    )
    .await;
    assert!(h.s() < 500, "seed host: {} {}", h.s(), h.text);

    let subst = |path: &str| {
        path.replace("{t}", &t.id)
            .replace("{s}", &svc)
            .replace("{key}", &key2_id)
            .replace("{p}", "contract-project")
            .replace("{v}", "1")
            .replace("{name}", "API_SECRET")
            .replace("{host}", "contract.bots.example.com")
            .replace("{id}", &sched_id)
    };

    // Operations: GETs first, then POST/PUT/PATCH, destructive DELETEs last (deepest paths first).
    let mut ops: Vec<(String, String, Value)> = Vec::new();
    for (path, item) in doc["paths"].as_object().expect("paths") {
        for method in ["get", "post", "put", "patch", "delete"] {
            if let Some(op) = item.get(method) {
                ops.push((method.to_string(), path.clone(), op.clone()));
            }
        }
    }
    let rank = |m: &str| match m {
        "get" => 0,
        "post" => 1,
        "put" => 2,
        "patch" => 3,
        _ => 4,
    };
    ops.sort_by_key(|(m, p, _)| {
        (
            rank(m),
            if m == "delete" {
                -(p.matches('/').count() as i64)
            } else {
                0
            },
        )
    });

    let mut failures: Vec<String> = Vec::new();
    let mut exercised = 0usize;
    let mut statuses: BTreeSet<u16> = BTreeSet::new();
    for (method, path, op) in &ops {
        // Not exercised generically: draining the node ends the test; SSE streams never finish.
        if (method == "post" && path == "/v1/node/drain") || path.ends_with("/build/logs") {
            continue;
        }
        let url = subst(path);
        let m = Method::from_bytes(method.to_uppercase().as_bytes()).expect("method");
        let mut rb = node
            .http()
            .request(m, node.control_url(&url))
            .bearer_auth(node.root_token())
            .header("x-rt-tenant", &t.id);
        let body = curated_body(method, path).or_else(|| {
            let schema = op["requestBody"]["content"]["application/json"]["schema"].clone();
            if schema.is_null() {
                None
            } else {
                Some((
                    "application/json",
                    serde_json::to_vec(&sample(&doc, &schema, 0)).expect("json"),
                ))
            }
        });
        if let Some((ct, bytes)) = body {
            rb = rb.header("content-type", ct).body(bytes);
        }
        let resp = send(rb).await;
        exercised += 1;
        statuses.insert(resp.s());
        let label = format!("{} {url} -> {}", method.to_uppercase(), resp.s());

        if resp.s() >= 500 {
            failures.push(format!("{label}: server error: {}", resp.text));
            continue;
        }
        let is_json = resp.header("content-type").unwrap_or_default().contains("json");
        match response_schema(op, resp.s()) {
            Some(Some(schema)) => {
                if !is_json {
                    failures.push(format!(
                        "{label}: the document declares a JSON body but the response is not JSON"
                    ));
                    continue;
                }
                let mut errs = Vec::new();
                validate(&doc, schema, &resp.json, "$", &mut errs);
                failures.extend(errs.into_iter().map(|e| format!("{label}: {e}")));
            }
            Some(None) => {} // documented without a JSON schema (204, binary, SSE)
            None => {
                if resp.s() >= 400 {
                    // Undocumented error status: must still be a control error envelope.
                    let mut errs = Vec::new();
                    let envelope = json!({"$ref": "#/components/schemas/ErrorEnvelope"});
                    validate(&doc, &envelope, &resp.json, "$", &mut errs);
                    failures.extend(errs.into_iter().map(|e| format!("{label}: {e}")));
                    let code = resp.code();
                    match documented_status_for(&code) {
                        Some(s) if s == resp.s() => {}
                        other => failures.push(format!(
                            "{label}: error code `{code}` is not documented for this status ({other:?})"
                        )),
                    }
                } else {
                    failures.push(format!("{label}: successful status is not declared by the document"));
                }
            }
        }
    }

    assert!(exercised >= 40, "only {exercised} operations were exercised");
    assert!(
        statuses.iter().any(|s| (200..300).contains(s)),
        "no success response was seen: {statuses:?}"
    );
    assert!(
        failures.is_empty(),
        "contract violations ({}):\n{}",
        failures.len(),
        failures.join("\n")
    );
    node.shutdown().await;
}
