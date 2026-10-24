//! A small OpenAPI 3.1 / JSON Schema subset validator and a sample-value generator, enough for the
//! API-01 contract test (no external schema crate is a dev-dependency of rt-server).
//!
//! Supported keywords: `$ref` (local), `allOf`, `oneOf`/`anyOf` (at least one alternative must
//! match), `enum`, `const`, `type` (string or array, incl. `"null"`), `nullable`, `required`,
//! `properties`, `additionalProperties` (bool or schema), `items`.

use serde_json::{json, Map, Value};

/// Every route of the route map in docs/api.md: `(method, path)`.
pub const ROUTE_MAP: &[(&str, &str)] = &[
    ("get", "/v1/node/status"),
    ("put", "/v1/node/config"),
    ("post", "/v1/node/drain"),
    ("get", "/v1/node/scheduler"),
    ("get", "/v1/node/cache"),
    ("post", "/v1/tenants"),
    ("get", "/v1/tenants"),
    ("get", "/v1/tenants/{t}"),
    ("patch", "/v1/tenants/{t}"),
    ("delete", "/v1/tenants/{t}"),
    ("get", "/v1/tenants/{t}/usage"),
    ("post", "/v1/tenants/{t}/keys"),
    ("get", "/v1/tenants/{t}/keys"),
    ("delete", "/v1/tenants/{t}/keys/{key}"),
    ("post", "/v1/keys"),
    ("get", "/v1/keys"),
    ("delete", "/v1/keys/{key}"),
    ("get", "/v1/limits"),
    ("post", "/v1/projects"),
    ("get", "/v1/projects"),
    ("get", "/v1/projects/{p}"),
    ("patch", "/v1/projects/{p}"),
    ("delete", "/v1/projects/{p}"),
    ("get", "/v1/projects/{p}/environments"),
    ("post", "/v1/services"),
    ("get", "/v1/services"),
    ("get", "/v1/services/{s}"),
    ("patch", "/v1/services/{s}"),
    ("delete", "/v1/services/{s}"),
    ("post", "/v1/services/{s}/versions"),
    ("get", "/v1/services/{s}/versions"),
    ("get", "/v1/services/{s}/versions/{v}"),
    ("get", "/v1/services/{s}/versions/{v}/source"),
    ("post", "/v1/services/{s}/versions/{v}/rebuild"),
    ("get", "/v1/services/{s}/versions/{v}/build/logs"),
    ("put", "/v1/services/{s}/live"),
    ("get", "/v1/services/{s}/env"),
    ("put", "/v1/services/{s}/env"),
    ("get", "/v1/services/{s}/secrets"),
    ("put", "/v1/services/{s}/secrets/{name}"),
    ("delete", "/v1/services/{s}/secrets/{name}"),
    ("get", "/v1/services/{s}/hosts"),
    ("put", "/v1/services/{s}/hosts/{host}"),
    ("delete", "/v1/services/{s}/hosts/{host}"),
    ("get", "/v1/services/{s}/schedules"),
    ("get", "/v1/services/{s}/schedules/once"),
    ("post", "/v1/services/{s}/schedules"),
    ("patch", "/v1/services/{s}/schedules/{id}"),
    ("delete", "/v1/services/{s}/schedules/{id}"),
    ("post", "/v1/services/{s}/queue"),
    ("get", "/v1/services/{s}/queue"),
    ("get", "/v1/services/{s}/dlq"),
    ("post", "/v1/services/{s}/dlq/redrive"),
    ("delete", "/v1/services/{s}/dlq"),
    ("get", "/v1/services/{s}/logs"),
    ("get", "/v1/services/{s}/stats"),
    ("get", "/v1/services/{s}/events"),
    ("get", "/v1/services/{s}/data/export"),
];

/// Control error codes documented in docs/api.md (plus the contract / SEC extras) with their HTTP status.
pub const ERROR_CODES: &[(&str, u16)] = &[
    ("unauthorized", 401),
    ("insufficient_scope", 403),
    ("quota_exceeded", 403),
    ("trigger_not_allowed", 403),
    ("binding_not_allowed", 403),
    ("host_not_allowed", 403),
    ("not_found", 404),
    ("slug_taken", 409),
    ("host_taken", 409),
    ("version_not_ready", 409),
    ("project_slug_taken", 409),
    ("project_not_empty", 409),
    ("environment_in_use", 409),
    ("precondition_failed", 412),
    ("too_large", 413),
    ("validation_failed", 422),
    ("invalid_artifact", 422),
    ("limit_exceeds_plan", 422),
    ("schedule_too_frequent", 422),
    ("idempotency_mismatch", 422),
    ("rate_limited", 429),
    ("build_quota", 429),
    ("queue_full", 429),
    ("control_disabled", 503),
];

pub fn documented_status_for(code: &str) -> Option<u16> {
    ERROR_CODES.iter().find(|(c, _)| *c == code).map(|(_, s)| *s)
}

pub fn resolve<'a>(doc: &'a Value, reference: &str) -> Option<&'a Value> {
    let path = reference.strip_prefix("#/")?;
    let mut cur = doc;
    for part in path.split('/') {
        cur = cur.get(part.replace("~1", "/").replace("~0", "~"))?;
    }
    Some(cur)
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn type_matches(t: &str, v: &Value) -> bool {
    match t {
        "null" => v.is_null(),
        "boolean" => v.is_boolean(),
        "string" => v.is_string(),
        "integer" => v.is_i64() || v.is_u64() || v.as_f64().is_some_and(|f| f.fract() == 0.0),
        "number" => v.is_number(),
        "array" => v.is_array(),
        "object" => v.is_object(),
        _ => true,
    }
}

/// Validate `v` against `schema` (inside `doc`); problems are appended to `errs`.
pub fn validate(doc: &Value, schema: &Value, v: &Value, at: &str, errs: &mut Vec<String>) {
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        match resolve(doc, r) {
            Some(s) => validate(doc, s, v, at, errs),
            None => errs.push(format!("{at}: unresolved reference {r}")),
        }
        return;
    }
    if let Some(all) = schema.get("allOf").and_then(Value::as_array) {
        for s in all {
            validate(doc, s, v, at, errs);
        }
    }
    for key in ["oneOf", "anyOf"] {
        if let Some(alts) = schema.get(key).and_then(Value::as_array) {
            let ok = alts.iter().any(|s| {
                let mut e = Vec::new();
                validate(doc, s, v, at, &mut e);
                e.is_empty()
            });
            if !ok {
                errs.push(format!(
                    "{at}: value matches none of the `{key}` alternatives ({})",
                    kind(v)
                ));
            }
        }
    }
    if schema.get("nullable") == Some(&Value::Bool(true)) && v.is_null() {
        return;
    }
    if let Some(e) = schema.get("enum").and_then(Value::as_array) {
        if !e.contains(v) {
            errs.push(format!("{at}: {v} is not one of {e:?}"));
        }
    }
    if let Some(c) = schema.get("const") {
        if c != v {
            errs.push(format!("{at}: expected constant {c}, got {v}"));
        }
    }
    let types: Vec<&str> = match schema.get("type") {
        Some(Value::String(s)) => vec![s.as_str()],
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };
    if !types.is_empty() && !types.iter().any(|t| type_matches(t, v)) {
        errs.push(format!("{at}: expected {types:?}, got {} ({v})", kind(v)));
        return;
    }
    match v {
        Value::Object(map) => {
            if let Some(req) = schema.get("required").and_then(Value::as_array) {
                for r in req.iter().filter_map(Value::as_str) {
                    if !map.contains_key(r) {
                        errs.push(format!("{at}: missing required property `{r}`"));
                    }
                }
            }
            let props = schema.get("properties").and_then(Value::as_object);
            for (k, val) in map {
                if let Some(ps) = props.and_then(|p| p.get(k)) {
                    validate(doc, ps, val, &format!("{at}.{k}"), errs);
                } else if let Some(ap) = schema.get("additionalProperties") {
                    match ap {
                        Value::Bool(false) => errs.push(format!("{at}: unexpected property `{k}`")),
                        Value::Object(_) => validate(doc, ap, val, &format!("{at}.{k}"), errs),
                        _ => {}
                    }
                }
            }
        }
        Value::Array(items) => {
            if let Some(is) = schema.get("items") {
                for (i, it) in items.iter().enumerate() {
                    validate(doc, is, it, &format!("{at}[{i}]"), errs);
                }
            }
        }
        _ => {}
    }
}

/// A plausible value for `schema` (required properties only).
pub fn sample(doc: &Value, schema: &Value, depth: u32) -> Value {
    if depth > 6 {
        return Value::Null;
    }
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        return resolve(doc, r)
            .map(|s| sample(doc, s, depth + 1))
            .unwrap_or(Value::Null);
    }
    if let Some(all) = schema.get("allOf").and_then(Value::as_array) {
        let mut merged = Map::new();
        for s in all {
            if let Value::Object(m) = sample(doc, s, depth + 1) {
                merged.extend(m);
            }
        }
        return Value::Object(merged);
    }
    for key in ["oneOf", "anyOf"] {
        if let Some(alts) = schema.get(key).and_then(Value::as_array) {
            let pick = alts
                .iter()
                .find(|a| a.get("type").and_then(Value::as_str) != Some("null"))
                .or(alts.first());
            return pick.map(|a| sample(doc, a, depth + 1)).unwrap_or(Value::Null);
        }
    }
    if let Some(e) = schema.get("enum").and_then(Value::as_array) {
        return e.first().cloned().unwrap_or(Value::Null);
    }
    if let Some(c) = schema.get("const") {
        return c.clone();
    }
    let ty = match schema.get("type") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .find(|t| *t != "null")
            .unwrap_or("null")
            .to_string(),
        _ => "object".to_string(),
    };
    match ty.as_str() {
        "string" => match schema.get("format").and_then(Value::as_str) {
            Some("date-time") => json!("2030-01-01T00:00:00Z"),
            Some("date") => json!("2030-01-01"),
            _ => json!("sample"),
        },
        "integer" | "number" => json!(1),
        "boolean" => json!(true),
        "array" => json!([]),
        "null" => Value::Null,
        _ => {
            let mut m = Map::new();
            let props = schema.get("properties").and_then(Value::as_object);
            if let (Some(props), Some(req)) = (props, schema.get("required").and_then(Value::as_array)) {
                for r in req.iter().filter_map(Value::as_str) {
                    if let Some(ps) = props.get(r) {
                        m.insert(r.to_string(), sample(doc, ps, depth + 1));
                    }
                }
            }
            Value::Object(m)
        }
    }
}

/// The schema documented for `status` of an operation (`responses.<status>` → `default`), if it
/// has a JSON body schema. The outer `Option` is `None` when the status itself is undocumented.
pub fn response_schema(op: &Value, status: u16) -> Option<Option<&Value>> {
    let responses = op.get("responses")?;
    let s = status.to_string();
    let range = format!("{}XX", status / 100);
    let entry = responses
        .get(&s)
        .or_else(|| responses.get(&range))
        .or_else(|| responses.get("default"))?;
    Some(
        entry
            .get("content")
            .and_then(|c| c.get("application/json"))
            .and_then(|j| j.get("schema")),
    )
}
