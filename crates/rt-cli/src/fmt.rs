//! Output helpers: plain-text tables, `Key:   value` blocks, timestamps, JSON printing.

use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::error::{CliError, Result};

/// Write to stdout; a closed pipe (`21ci logs | head`) ends the process quietly.
pub fn emit(s: &str) {
    let mut out = std::io::stdout().lock();
    if out.write_all(s.as_bytes()).and_then(|()| out.flush()).is_err() {
        std::process::exit(0);
    }
}

pub fn emitln(s: &str) {
    let mut line = String::with_capacity(s.len() + 1);
    line.push_str(s);
    line.push('\n');
    emit(&line);
}

/// Hint or notice for humans; never part of the machine-readable output.
pub fn note(s: &str) {
    eprintln!("{s}");
}

pub fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

pub fn print_json(v: &Value) {
    emitln(&pretty(v));
}

/// kubectl-style table: columns padded to the widest cell, two spaces apart, no trailing blanks.
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let n = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for r in rows {
        for (i, c) in r.iter().enumerate().take(n) {
            widths[i] = widths[i].max(c.chars().count());
        }
    }
    let mut out = String::new();
    let mut line = |cells: Vec<&str>| {
        let mut s = String::new();
        for (i, c) in cells.iter().enumerate() {
            if i + 1 == n {
                s.push_str(c);
            } else {
                s.push_str(c);
                for _ in c.chars().count()..widths[i] + 2 {
                    s.push(' ');
                }
            }
        }
        out.push_str(s.trim_end());
        out.push('\n');
    };
    line(headers.to_vec());
    for r in rows {
        line(r.iter().map(String::as_str).collect());
    }
    out
}

/// `Label:  value` block; the labels are padded to the widest one.
pub fn kv(rows: &[(&str, String)]) -> String {
    let w = rows.iter().map(|(k, _)| k.chars().count()).max().unwrap_or(0) + 1;
    let mut out = String::new();
    for (k, v) in rows {
        let label = format!("{k}:");
        out.push_str(&format!("{label:<w$}  {v}\n"));
    }
    out
}

pub fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

fn system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64)
}

/// `2026-01-01T00:00:00Z` from epoch milliseconds.
pub fn ts_s(ms: i64) -> String {
    humantime::format_rfc3339_seconds(system_time(ms)).to_string()
}

/// `2026-01-01T00:00:00.123Z` from epoch milliseconds.
pub fn ts_ms(ms: i64) -> String {
    humantime::format_rfc3339_millis(system_time(ms)).to_string()
}

/// Epoch-millisecond field of a JSON object as text, `-` when absent or null.
pub fn ts_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_i64)
        .map(ts_s)
        .unwrap_or_else(|| "-".into())
}

pub fn str_field(v: &Value, key: &str) -> String {
    match v.get(key) {
        None | Some(Value::Null) => "-".into(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

pub fn compact(v: &Value) -> String {
    v.to_string()
}

/// Epoch milliseconds of `t`.
pub fn epoch_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// `in 6d 23h` or `5m ago`, relative to `now_ms`.
pub fn relative(ms: i64, now_ms: i64) -> String {
    let secs = |d: i64| crate::error::human_secs(u64::try_from(d / 1000).unwrap_or(0));
    if ms >= now_ms {
        format!("in {}", secs(ms - now_ms))
    } else {
        format!("{} ago", secs(now_ms - ms))
    }
}

/// A config block as `k=v k2=v2`: strings bare, everything else compact JSON. Nulls, empty
/// arrays and empty objects are left out; `-` when nothing is left.
pub fn pairs(v: Option<&Value>) -> String {
    let Some(o) = v.and_then(Value::as_object) else {
        return "-".into();
    };
    let parts: Vec<String> = o
        .iter()
        .filter(|(_, v)| match v {
            Value::Null => false,
            Value::Array(a) => !a.is_empty(),
            Value::Object(m) => !m.is_empty(),
            _ => true,
        })
        .map(|(k, v)| match v {
            Value::String(s) => format!("{k}={s}"),
            other => format!("{k}={other}"),
        })
        .collect();
    if parts.is_empty() {
        "-".into()
    } else {
        parts.join(" ")
    }
}

/// Leaves of a JSON document as `(dotted.key, value)`: objects are walked, arrays are shown as
/// compact JSON, strings bare. An empty object is a leaf (`{}`).
pub fn flatten(v: &Value) -> Vec<(String, String)> {
    fn walk(prefix: &str, v: &Value, out: &mut Vec<(String, String)>) {
        match v {
            Value::Object(o) if !o.is_empty() => {
                for (k, val) in o {
                    let key = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    walk(&key, val, out);
                }
            }
            Value::String(s) => out.push((prefix.to_string(), s.clone())),
            other => out.push((prefix.to_string(), other.to_string())),
        }
    }
    let mut out = Vec::new();
    walk("", v, &mut out);
    out
}

/// [`flatten`] as a `KEY  VALUE` table.
pub fn flat_table(v: &Value) -> String {
    let rows: Vec<Vec<String>> = flatten(v).into_iter().map(|(k, v)| vec![k, v]).collect();
    table(&["KEY", "VALUE"], &rows)
}

/// `--from` / `--to` of usage: `YYYY-MM-DD` is passed through; a duration (`7d`) counts back
/// from `now` and gives that day.
pub fn parse_day_arg(s: &str, flag: &str, now: SystemTime) -> Result<String> {
    let t = s.trim();
    let b = t.as_bytes();
    let is_day = b.len() == 10
        && b.iter().enumerate().all(|(i, c)| {
            if i == 4 || i == 7 {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        });
    if is_day {
        return Ok(t.to_string());
    }
    match parse_duration(t) {
        Ok(d) => {
            let at = now.checked_sub(d).unwrap_or(UNIX_EPOCH);
            Ok(humantime::format_rfc3339_seconds(at).to_string()[..10].to_string())
        }
        Err(_) => Err(CliError::usage(format!(
            "invalid {flag} `{s}`: use a day (2026-10-01) or a duration back from today such as 7d"
        ))),
    }
}

/// A duration given as `90`, `90s`, `5m`, `1h 30m`, … (bare numbers are seconds).
pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    let t = s.trim();
    if let Ok(secs) = t.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }
    humantime::parse_duration(t).map_err(|e| format!("invalid duration `{s}`: {e}"))
}

/// `--since` / `--until`: an RFC 3339 timestamp is passed through; a duration (`15m`) means
/// "that long before `now`".
pub fn parse_time_arg(s: &str, flag: &str, now: SystemTime) -> Result<String> {
    let t = s.trim();
    if humantime::parse_rfc3339(t).is_ok() {
        return Ok(t.to_string());
    }
    match parse_duration(t) {
        Ok(d) => {
            let at = now.checked_sub(d).unwrap_or(UNIX_EPOCH);
            Ok(humantime::format_rfc3339_seconds(at).to_string())
        }
        Err(_) => Err(CliError::usage(format!(
            "invalid {flag} `{s}`: use an RFC 3339 timestamp (2026-01-01T00:00:00Z) or a duration such as 15m, 2h, 1d"
        ))),
    }
}

/// Percent-encode one URL path segment.
pub fn enc(seg: &str) -> String {
    let mut out = String::with_capacity(seg.len());
    for b in seg.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_pads_columns_and_trims_the_end() {
        let t = table(
            &["ID", "NAME"],
            &[vec!["a".into(), "x".into()], vec!["long-id".into(), "".into()]],
        );
        assert_eq!(t, "ID       NAME\na        x\nlong-id\n");
    }

    #[test]
    fn kv_aligns_labels() {
        assert_eq!(kv(&[("ID", "1".into()), ("Slug", "s".into())]), "ID:    1\nSlug:  s\n");
    }

    #[test]
    fn timestamps() {
        assert_eq!(ts_s(1_767_225_600_000), "2026-01-01T00:00:00Z");
        assert_eq!(ts_ms(1_767_225_600_123), "2026-01-01T00:00:00.123Z");
        assert_eq!(ts_s(-5), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1m 30s").unwrap(), Duration::from_secs(90));
        assert!(parse_duration("soon").is_err());
    }

    #[test]
    fn time_args_are_passed_through_or_made_absolute() {
        let now = UNIX_EPOCH + Duration::from_secs(1_767_225_600 + 3600);
        assert_eq!(
            parse_time_arg("2026-01-01T00:00:00Z", "--since", now).unwrap(),
            "2026-01-01T00:00:00Z"
        );
        assert_eq!(parse_time_arg("1h", "--since", now).unwrap(), "2026-01-01T00:00:00Z");
        assert_eq!(parse_time_arg("30m", "--since", now).unwrap(), "2026-01-01T00:30:00Z");
        assert!(matches!(
            parse_time_arg("nope", "--since", now),
            Err(CliError::Usage(_))
        ));
    }

    #[test]
    fn relative_times() {
        assert_eq!(relative(10_000 + 90_000, 10_000), "in 1m 30s");
        assert_eq!(relative(10_000, 10_000 + 7_200_000), "2h ago");
    }

    #[test]
    fn block_pairs() {
        let v = serde_json::json!({"cpu_ms": 500, "wall_ms": null, "ip_allow": [], "name": "x", "m": ["GET"]});
        assert_eq!(pairs(Some(&v)), r#"cpu_ms=500 name=x m=["GET"]"#);
        assert_eq!(pairs(Some(&serde_json::json!({"a": null}))), "-");
        assert_eq!(pairs(None), "-");
    }

    #[test]
    fn flattening() {
        let v = serde_json::json!({"sched": {"max_inflight": 0, "kinds": ["deno"]}, "name": "n", "e": {}});
        assert_eq!(
            flatten(&v),
            vec![
                ("sched.max_inflight".to_string(), "0".to_string()),
                ("sched.kinds".to_string(), r#"["deno"]"#.to_string()),
                ("name".to_string(), "n".to_string()),
                ("e".to_string(), "{}".to_string()),
            ]
        );
    }

    #[test]
    fn day_args() {
        let now = UNIX_EPOCH + Duration::from_secs(1_767_225_600 + 3600); // 2026-01-01T01:00Z
        assert_eq!(parse_day_arg("2025-12-01", "--from", now).unwrap(), "2025-12-01");
        assert_eq!(parse_day_arg("7d", "--from", now).unwrap(), "2025-12-25");
        assert!(matches!(
            parse_day_arg("yesterday", "--from", now),
            Err(CliError::Usage(_))
        ));
    }

    #[test]
    fn path_segments_are_encoded() {
        assert_eq!(enc("API_KEY-1.x"), "API_KEY-1.x");
        assert_eq!(enc("a b/c"), "a%20b%2Fc");
    }
}
