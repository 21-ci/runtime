//! Structured log fields (`std:log`): a record carries an optional JSON object of
//! fields. Records without fields are written and read exactly as before the field existed,
//! and the caps (32 top-level keys, 8 KiB serialized) are enforced where fields enter the store.

use rt_obs::{parse_log_fields, LogFieldsError, LogKind, LogLevel, LogRecord, MAX_LOG_FIELDS, MAX_LOG_FIELDS_BYTES};
use rt_types::{ServiceId, VersionNo};
use serde_json::{json, Map, Value};

fn record(msg: &str) -> LogRecord {
    LogRecord::new(
        ServiceId::from_u128(1),
        VersionNo(1),
        LogLevel::Info,
        LogKind::Runtime,
        msg,
    )
}

#[test]
fn a_record_without_fields_has_no_fields_member_on_the_wire() {
    let rec = record("plain");
    let wire = serde_json::to_value(&rec).expect("serialize");
    assert!(wire.get("fields").is_none(), "no fields member: {wire}");

    let back: LogRecord = serde_json::from_value(wire).expect("parse");
    assert_eq!(back.fields, None);
}

#[test]
fn a_line_written_before_fields_existed_still_parses() {
    let svc = ServiceId::from_u128(1);
    let line = format!(
        r#"{{"seq":7,"ts_ms":1,"level":"warn","tenant_id":null,"service_id":"{svc}","version":2,"task_id":null,"trigger":"http","kind":"runtime","msg":"old","truncated":false}}"#
    );
    let rec: LogRecord = serde_json::from_str(&line).expect("old line parses");
    assert_eq!(rec.msg, "old");
    assert_eq!(rec.fields, None);
}

#[test]
fn a_record_with_fields_round_trips_them() {
    let mut rec = record("with fields");
    let fields: Map<String, Value> = match json!({"chat": 42, "ok": true, "tags": ["a"]}) {
        Value::Object(m) => m,
        _ => unreachable!(),
    };
    rec.fields = Some(fields.clone());
    let line = serde_json::to_string(&rec).expect("serialize");
    let back: LogRecord = serde_json::from_str(&line).expect("parse");
    assert_eq!(back.fields, Some(fields));
}

#[test]
fn fields_must_be_a_json_object() {
    for bad in ["[1,2]", "\"text\"", "null", "42", "true", "{\"a\": "] {
        assert!(matches!(parse_log_fields(bad), Err(LogFieldsError::NotObject)), "{bad}");
    }
    assert!(parse_log_fields("{}").expect("empty object is fine").is_empty());
}

#[test]
fn at_most_thirty_two_top_level_keys() {
    let at_cap: Map<String, Value> = (0..MAX_LOG_FIELDS).map(|i| (format!("k{i}"), json!(i))).collect();
    let text = serde_json::to_string(&at_cap).expect("json");
    assert_eq!(parse_log_fields(&text).expect("32 keys").len(), 32);

    let over: Map<String, Value> = (0..=MAX_LOG_FIELDS).map(|i| (format!("k{i}"), json!(i))).collect();
    let text = serde_json::to_string(&over).expect("json");
    assert!(matches!(
        parse_log_fields(&text),
        Err(LogFieldsError::TooManyKeys { .. })
    ));
}

#[test]
fn at_most_eight_kib_serialized() {
    assert_eq!(MAX_LOG_FIELDS_BYTES, 8 * 1024);
    let fits = format!("{{\"v\":\"{}\"}}", "x".repeat(MAX_LOG_FIELDS_BYTES - 8));
    assert_eq!(fits.len(), MAX_LOG_FIELDS_BYTES);
    assert!(parse_log_fields(&fits).is_ok());

    let too_big = format!("{{\"v\":\"{}\"}}", "x".repeat(MAX_LOG_FIELDS_BYTES));
    assert!(matches!(
        parse_log_fields(&too_big),
        Err(LogFieldsError::TooLarge { .. })
    ));
}
