//! Errors and exit codes: 0 ok, 1 API (or other runtime) failure, 2 usage, 3 network, 4 busy
//! (`429` / `503` with `Retry-After`: try again later).

use serde_json::{json, Value};

pub const EXIT_FAILURE: i32 = 1;
pub const EXIT_USAGE: i32 = 2;
pub const EXIT_NETWORK: i32 = 3;
pub const EXIT_BUSY: i32 = 4;

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// The control API answered with a non-2xx status (or the CLI found nothing where the API
    /// would have answered `not_found`; then `status` is `None`).
    #[error("{code}: {message}")]
    Api {
        status: Option<u16>,
        code: String,
        message: String,
        /// Boxed: keeps `Result<T, CliError>` small.
        details: Box<Value>,
        /// `Retry-After` in seconds, when the server sent one.
        retry_after: Option<u64>,
    },
    /// Bad arguments, bad input files, bad config. Nothing was sent.
    #[error("{0}")]
    Usage(String),
    /// The server could not be reached.
    #[error("{0}")]
    Network(String),
    /// Anything else that makes the command fail (failed build, timeout, nothing to do).
    #[error("{0}")]
    Failed(String),
    /// Already reported; only the exit code is left to do.
    #[error("exit {0}")]
    Silent(i32),
}

/// `429` / `503` with a `Retry-After`: the server is busy (or rate-limiting) and says when to
/// come back.
pub fn is_busy(status: Option<u16>, retry_after: Option<u64>) -> bool {
    matches!(status, Some(429 | 503)) && retry_after.is_some()
}

/// `90` -> `90s`, `3600` -> `1h`, `5400` -> `1h 30m`.
pub fn human_secs(secs: u64) -> String {
    let (d, h, m, s) = (secs / 86_400, secs / 3600 % 24, secs / 60 % 60, secs % 60);
    let mut parts = Vec::new();
    for (n, unit) in [(d, "d"), (h, "h"), (m, "m"), (s, "s")] {
        if n > 0 {
            parts.push(format!("{n}{unit}"));
        }
    }
    if parts.is_empty() {
        return "0s".into();
    }
    parts.truncate(2);
    parts.join(" ")
}

impl CliError {
    pub fn usage(msg: impl Into<String>) -> Self {
        CliError::Usage(msg.into())
    }

    pub fn failed(msg: impl Into<String>) -> Self {
        CliError::Failed(msg.into())
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        CliError::Api {
            status: None,
            code: "not_found".into(),
            message: msg.into(),
            details: Box::new(json!({})),
            retry_after: None,
        }
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::Api {
                status, retry_after, ..
            } if is_busy(*status, *retry_after) => EXIT_BUSY,
            CliError::Api { .. } | CliError::Failed(_) => EXIT_FAILURE,
            CliError::Usage(_) => EXIT_USAGE,
            CliError::Network(_) => EXIT_NETWORK,
            CliError::Silent(c) => *c,
        }
    }

    /// `error: …` text for the terminal (nothing for [`CliError::Silent`]).
    pub fn render_text(&self) -> Option<String> {
        match self {
            CliError::Api {
                status,
                code,
                message,
                details,
                retry_after,
            } => {
                let mut s = format!("error: {code}: {message}");
                match (status, retry_after) {
                    (Some(st), Some(ra)) => s.push_str(&format!(" (HTTP {st}, retry after {})", human_secs(*ra))),
                    (Some(st), None) => s.push_str(&format!(" (HTTP {st})")),
                    (None, Some(ra)) => s.push_str(&format!(" (retry after {})", human_secs(*ra))),
                    (None, None) => {}
                }
                if has_details(details) {
                    let pretty = serde_json::to_string_pretty(details).unwrap_or_default();
                    for line in pretty.lines() {
                        s.push_str("\n  ");
                        s.push_str(line);
                    }
                }
                Some(s)
            }
            CliError::Usage(m) | CliError::Network(m) | CliError::Failed(m) => Some(format!("error: {m}")),
            CliError::Silent(_) => None,
        }
    }

    /// `{"error": {code, message, details?, status?, retry_after?}}` for `--output json`.
    pub fn render_json(&self) -> Option<Value> {
        let (code, message, details, status, retry_after): (&str, String, Option<&Value>, Option<u16>, Option<u64>) =
            match self {
                CliError::Api {
                    status,
                    code,
                    message,
                    details,
                    retry_after,
                } => (code, message.clone(), Some(&**details), *status, *retry_after),
                CliError::Usage(m) => ("usage", m.clone(), None, None, None),
                CliError::Network(m) => ("network_error", m.clone(), None, None, None),
                CliError::Failed(m) => ("failed", m.clone(), None, None, None),
                CliError::Silent(_) => return None,
            };
        let mut e = serde_json::Map::new();
        e.insert("code".into(), json!(code));
        e.insert("message".into(), json!(message));
        if let Some(d) = details.filter(|d| has_details(d)) {
            e.insert("details".into(), d.clone());
        }
        if let Some(s) = status {
            e.insert("status".into(), json!(s));
        }
        if let Some(r) = retry_after {
            e.insert("retry_after".into(), json!(r));
        }
        Some(json!({ "error": Value::Object(e) }))
    }
}

fn has_details(d: &Value) -> bool {
    match d {
        Value::Null => false,
        Value::Object(m) => !m.is_empty(),
        Value::Array(a) => !a.is_empty(),
        _ => true,
    }
}

pub type Result<T> = std::result::Result<T, CliError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn api(status: u16, retry_after: Option<u64>) -> CliError {
        CliError::Api {
            status: Some(status),
            code: "queue_timeout".into(),
            message: "busy".into(),
            details: Box::new(json!({})),
            retry_after,
        }
    }

    #[test]
    fn exit_codes() {
        assert_eq!(CliError::usage("x").exit_code(), 2);
        assert_eq!(CliError::Network("x".into()).exit_code(), 3);
        assert_eq!(CliError::failed("x").exit_code(), 1);
        assert_eq!(CliError::not_found("x").exit_code(), 1);
        assert_eq!(CliError::Silent(7).exit_code(), 7);
        assert_eq!(api(503, Some(2)).exit_code(), 4);
        assert_eq!(api(429, Some(2)).exit_code(), 4);
        assert_eq!(api(503, None).exit_code(), 1, "no Retry-After: not a busy signal");
        assert_eq!(api(409, Some(2)).exit_code(), 1);
    }

    #[test]
    fn api_error_text_has_status_and_indented_details() {
        let e = CliError::Api {
            status: Some(422),
            code: "validation_failed".into(),
            message: "bad".into(),
            details: Box::new(json!({"diagnostics": [{"file": "a.ts"}]})),
            retry_after: None,
        };
        let t = e.render_text().unwrap();
        assert!(t.starts_with("error: validation_failed: bad (HTTP 422)\n  {"), "{t}");
        assert!(t.contains("\n    \"diagnostics\""), "{t}");
    }

    #[test]
    fn retry_after_is_rendered() {
        assert_eq!(
            api(503, Some(90)).render_text().unwrap(),
            "error: queue_timeout: busy (HTTP 503, retry after 1m 30s)"
        );
        assert_eq!(api(503, Some(2)).render_json().unwrap()["error"]["retry_after"], 2);
    }

    #[test]
    fn human_durations() {
        assert_eq!(human_secs(0), "0s");
        assert_eq!(human_secs(45), "45s");
        assert_eq!(human_secs(3600), "1h");
        assert_eq!(human_secs(5400), "1h 30m");
        assert_eq!(human_secs(8 * 86_400 + 3 * 3600 + 7), "8d 3h");
    }

    #[test]
    fn empty_details_are_not_printed() {
        let e = CliError::Api {
            status: Some(404),
            code: "not_found".into(),
            message: "nf".into(),
            details: Box::new(json!({})),
            retry_after: None,
        };
        assert_eq!(e.render_text().unwrap(), "error: not_found: nf (HTTP 404)");
        let j = e.render_json().unwrap();
        assert_eq!(
            j,
            json!({"error": {"code": "not_found", "message": "nf", "status": 404}})
        );
    }
}
