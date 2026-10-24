//! Event contracts shared by pub/sub (`std:pubsub`), streams (`std:stream`) and async invocation:
//! hard limits, name/subject grammars, the subject matcher, the service config blocks
//! (`events`, `async`) and the payloads delivered to guests.
//!
//! Pure data and pure functions: no I/O. The limits are mirrored in `js/bootstrap.js`, the WIT
//! comments and the docs.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::ids::{InvocationId, MsgId, ProjectId, ServiceId, TenantId};
use crate::task::Trigger;

// ---------------------------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------------------------

/// Largest pub/sub body, stream record body, reply and async request body.
pub const EVENT_BODY_MAX_BYTES: usize = 1024 * 1024;
/// Headers per message.
pub const EVENT_HEADERS_MAX: usize = 32;
/// Header names + values of one message.
pub const EVENT_HEADERS_MAX_BYTES: usize = 16 * 1024;
/// One header value.
pub const EVENT_HEADER_VALUE_MAX: usize = 4096;
/// Longest subject or subscription pattern.
pub const SUBJECT_MAX_BYTES: usize = 256;
/// Most `.`-separated tokens of a subject or pattern.
pub const SUBJECT_MAX_TOKENS: usize = 16;
/// `events.subscribe` entries per service.
pub const SUBSCRIPTIONS_PER_SERVICE: usize = 32;
/// `events.consume` entries per service.
pub const CONSUMERS_PER_SERVICE: usize = 8;
/// Deliveries per publish; the rest are dropped and counted.
pub const PUBSUB_FANOUT_MAX: usize = 100;
/// Pub/sub deliveries one subscriber service may have in flight; more are dropped (`backlog`).
pub const PUBSUB_INFLIGHT_PER_SERVICE: u32 = 64;
/// `request` timeout when none is given.
pub const REQUEST_TIMEOUT_DEFAULT_MS: u64 = 5_000;
/// Longest `request` timeout.
pub const REQUEST_TIMEOUT_MAX_MS: u64 = 30_000;
/// Longest topic name.
pub const TOPIC_NAME_MAX: usize = 63;
/// Longest group name.
pub const GROUP_NAME_MAX: usize = 63;
/// Topic ids start with this; topic names may not (so `{t}` in a route is never ambiguous).
pub const TOPIC_ID_PREFIX: &str = "tpc_";
/// Longest stream record key.
pub const STREAM_KEY_MAX_BYTES: usize = 256;
/// Longest append dedup id.
pub const DEDUP_ID_MAX_BYTES: usize = 128;
/// Records per `appendBatch`.
pub const APPEND_BATCH_MAX: usize = 100;
/// Bytes per `appendBatch`.
pub const APPEND_BATCH_MAX_BYTES: usize = 4 * 1024 * 1024;
/// Records per `read` page.
pub const READ_LIMIT_MAX: usize = 100;
/// Body bytes per `read` page.
pub const READ_MAX_BYTES: usize = 1024 * 1024;
/// Records per consumer delivery batch when `batch_size` is not set.
pub const DELIVERY_BATCH_DEFAULT: u32 = 100;
/// Largest `batch_size`.
pub const DELIVERY_BATCH_MAX: u32 = 1_000;
/// Body bytes per delivery batch (a batch always holds at least one record).
pub const DELIVERY_BATCH_MAX_BYTES: usize = 1024 * 1024;
/// The same dedup id on a topic within this window returns the original offset.
pub const STREAM_DEDUP_WINDOW_MS: i64 = 2 * 60 * 1000;
/// Partitions of one topic, whatever the plan says.
pub const PARTITIONS_HARD_MAX: u32 = 16;
/// Topic retention when none is given.
pub const DEFAULT_RETENTION_HOURS: u32 = 24;
/// Dead letters kept per consumer (oldest evicted).
pub const DEAD_MAX_PER_CONSUMER: u64 = 1_000;
/// Dead letters are kept this long.
pub const DEAD_RETENTION_MS: i64 = 7 * 24 * 3600 * 1000;
/// Stream batches one service may have in flight.
pub const STREAM_INFLIGHT_PER_SERVICE: u32 = 8;
/// Default `events.consume[].max_attempts`.
pub const CONSUMER_MAX_ATTEMPTS_DEFAULT: u16 = 10;
/// Largest `events.consume[].max_attempts`.
pub const CONSUMER_MAX_ATTEMPTS_MAX: u16 = 100;
/// Default `async.max_attempts`.
pub const ASYNC_MAX_ATTEMPTS_DEFAULT: u16 = 3;
/// Largest `async.max_attempts`.
pub const ASYNC_MAX_ATTEMPTS_MAX: u16 = 10;
/// Default `async.max_event_age_s`.
pub const ASYNC_MAX_EVENT_AGE_DEFAULT_S: u32 = 3_600;
/// Smallest `async.max_event_age_s`.
pub const ASYNC_MAX_EVENT_AGE_MIN_S: u32 = 60;
/// Largest `async.max_event_age_s`.
pub const ASYNC_MAX_EVENT_AGE_MAX_S: u32 = 86_400;
/// Succeeded async records are kept this long.
pub const ASYNC_SUCCEEDED_RETENTION_MS: i64 = 24 * 3600 * 1000;
/// Failed async records are kept this long.
pub const ASYNC_FAILED_RETENTION_MS: i64 = 7 * 24 * 3600 * 1000;
/// Failed async records kept per service (oldest evicted).
pub const ASYNC_FAILED_MAX_PER_SERVICE: u64 = 1_000;
/// Async invocations in flight across the node.
pub const ASYNC_INFLIGHT_NODE: usize = 256;
/// Longest `Idempotency-Key`.
pub const IDEMPOTENCY_KEY_MAX: usize = 255;

/// Response header of a stream handler: the offset to redeliver from.
pub const STREAM_FAIL_AT_HEADER: &str = "x-rt-stream-fail-at";
/// Runtime request header of an async invocation: its id.
pub const INVOCATION_ID_HEADER: &str = "x-rt-invocation-id";
/// Runtime request header of an async invocation: this delivery, 1-based.
pub const INVOCATION_ATTEMPT_HEADER: &str = "x-rt-invocation-attempt";

// ---------------------------------------------------------------------------------------------
// Namespace, partitioning, admission lane, sealing AAD
// ---------------------------------------------------------------------------------------------

/// Where subjects and topics live: a tenant's project environment.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Namespace {
    pub tenant: TenantId,
    pub project: ProjectId,
    pub environment: String,
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.tenant, self.project, self.environment)
    }
}

/// Partition of a keyed record: FNV-1a 64 of the key modulo the partition count (stable).
pub fn partition_for(key: &str, partitions: u32) -> u32 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % u64::from(partitions.max(1))) as u32
}

/// Admission estimator lane of a trigger: `1` (background) for queue, stream and schedule
/// tasks, `0` for everything else.
pub fn admission_lane(t: &Trigger) -> u8 {
    match t {
        Trigger::Queue { .. }
        | Trigger::Stream { .. }
        | Trigger::Interval { .. }
        | Trigger::Cron { .. }
        | Trigger::Scheduled { .. } => 1,
        _ => 0,
    }
}

/// Additional authenticated data for sealing one part of a stored async request: `b'h'`
/// headers, `b'b'` body. Binds the ciphertext to the service and the invocation.
pub fn invocation_aad(service: ServiceId, id: InvocationId, part: u8) -> Vec<u8> {
    let mut v = format!("rt-async-invocation\0{service}\0{id}\0").into_bytes();
    v.push(part);
    v
}

// ---------------------------------------------------------------------------------------------
// Grammars
// ---------------------------------------------------------------------------------------------

fn token_ok(t: &str) -> bool {
    !t.is_empty() && t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn check_subject(s: &str, wildcards: bool) -> Result<(), String> {
    if s.is_empty() {
        return Err("subject is empty".into());
    }
    if s.len() > SUBJECT_MAX_BYTES {
        return Err(format!("subject is longer than {SUBJECT_MAX_BYTES} bytes"));
    }
    let tokens: Vec<&str> = s.split('.').collect();
    if tokens.len() > SUBJECT_MAX_TOKENS {
        return Err(format!("subject has more than {SUBJECT_MAX_TOKENS} tokens"));
    }
    if tokens[0] == "_rt" {
        return Err("subjects starting with `_rt` are reserved".into());
    }
    let last = tokens.len() - 1;
    for (i, t) in tokens.iter().enumerate() {
        match *t {
            "*" | ">" if !wildcards => return Err("a published subject cannot contain wildcards".into()),
            ">" if i != last => return Err("`>` must be the last token".into()),
            "*" | ">" => {}
            t if token_ok(t) => {}
            "" => return Err("subject has an empty token".into()),
            _ => return Err(format!("subject token `{t}` must be letters, digits, `_` or `-`")),
        }
    }
    Ok(())
}

/// A subject to publish to: tokens of `[A-Za-z0-9_-]`, `.`-separated, no wildcards, not `_rt…`.
pub fn validate_subject(s: &str) -> Result<(), String> {
    check_subject(s, false)
}

/// A subscription pattern: like a subject, plus `*` (one token) and `>` (the rest, last only).
pub fn validate_pattern(s: &str) -> Result<(), String> {
    check_subject(s, true)
}

/// `true` when the (valid) pattern matches the (valid) subject.
pub fn subject_matches(pattern: &str, subject: &str) -> bool {
    let mut subj = subject.split('.');
    for p in pattern.split('.') {
        match p {
            ">" => return subj.next().is_some(),
            "*" => {
                if subj.next().is_none() {
                    return false;
                }
            }
            p => {
                if subj.next() != Some(p) {
                    return false;
                }
            }
        }
    }
    subj.next().is_none()
}

fn name_ok(s: &str, max: usize, dot: bool) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= max
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_' || *c == b'-' || (dot && *c == b'.'))
}

/// Topic name: `[a-z0-9][a-z0-9._-]{0,62}`, not starting with [`TOPIC_ID_PREFIX`].
pub fn valid_topic_name(s: &str) -> bool {
    name_ok(s, TOPIC_NAME_MAX, true) && !s.starts_with(TOPIC_ID_PREFIX)
}

/// Group name: `[a-z0-9][a-z0-9_-]{0,62}`.
pub fn valid_group_name(s: &str) -> bool {
    name_ok(s, GROUP_NAME_MAX, false)
}

/// Message headers: at most [`EVENT_HEADERS_MAX`] entries and [`EVENT_HEADERS_MAX_BYTES`] in
/// total (see [`headers_over_limits`]), names are HTTP tokens not starting with `x-rt-`, values
/// at most [`EVENT_HEADER_VALUE_MAX`] bytes without CR, LF or NUL.
pub fn validate_headers(h: &[(String, String)]) -> Result<(), String> {
    if h.len() > EVENT_HEADERS_MAX {
        return Err(format!("too many headers (more than {EVENT_HEADERS_MAX})"));
    }
    if headers_bytes(h) > EVENT_HEADERS_MAX_BYTES {
        return Err(format!("headers are larger than {EVENT_HEADERS_MAX_BYTES} bytes"));
    }
    for (k, v) in h {
        let name_ok = !k.is_empty()
            && k.bytes()
                .all(|b| b.is_ascii_graphic() && !b"()<>@,;:\\\"/[]?={}".contains(&b));
        if !name_ok {
            return Err(format!("header name `{}` is not a valid token", k.escape_debug()));
        }
        if k.len() >= 5 && k[..5].eq_ignore_ascii_case("x-rt-") {
            return Err(format!("header `{k}`: `x-rt-` headers are reserved"));
        }
        if v.len() > EVENT_HEADER_VALUE_MAX {
            return Err(format!(
                "header `{k}`: value is longer than {EVENT_HEADER_VALUE_MAX} bytes"
            ));
        }
        if v.bytes().any(|b| matches!(b, b'\r' | b'\n' | 0)) {
            return Err(format!("header `{k}`: value contains CR, LF or NUL"));
        }
    }
    Ok(())
}

fn headers_bytes(h: &[(String, String)]) -> usize {
    h.iter().map(|(k, v)| k.len() + v.len()).sum()
}

/// `true` when the headers break the count or total-size limit (`too_large`, not
/// `validation_failed`).
pub fn headers_over_limits(h: &[(String, String)]) -> bool {
    h.len() > EVENT_HEADERS_MAX || headers_bytes(h) > EVENT_HEADERS_MAX_BYTES
}

// ---------------------------------------------------------------------------------------------
// Service config blocks
// ---------------------------------------------------------------------------------------------

/// `config.events`: pub/sub subscriptions and stream consumers of a service.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EventsConfig {
    pub subscribe: Vec<Subscription>,
    pub consume: Vec<StreamConsumer>,
}

impl EventsConfig {
    /// No subscription and no consumer (the block is left out of the service's JSON).
    pub fn is_unset(&self) -> bool {
        self.subscribe.is_empty() && self.consume.is_empty()
    }

    /// Bounds and grammars; the message names the field.
    pub fn validate(&self) -> Result<(), String> {
        if self.subscribe.len() > SUBSCRIPTIONS_PER_SERVICE {
            return Err(format!(
                "`events.subscribe`: at most {SUBSCRIPTIONS_PER_SERVICE} entries"
            ));
        }
        for (i, s) in self.subscribe.iter().enumerate() {
            validate_pattern(&s.subject).map_err(|e| format!("`events.subscribe[{i}].subject`: {e}"))?;
            if let Some(g) = &s.group {
                if !valid_group_name(g) {
                    return Err(format!(
                        "`events.subscribe[{i}].group`: must match [a-z0-9][a-z0-9_-]{{0,62}}"
                    ));
                }
            }
        }
        if self.consume.len() > CONSUMERS_PER_SERVICE {
            return Err(format!("`events.consume`: at most {CONSUMERS_PER_SERVICE} entries"));
        }
        for (i, c) in self.consume.iter().enumerate() {
            if !valid_topic_name(&c.topic) {
                return Err(format!(
                    "`events.consume[{i}].topic`: must match [a-z0-9][a-z0-9._-]{{0,62}} and not start with `{TOPIC_ID_PREFIX}`"
                ));
            }
            if !valid_group_name(&c.group) {
                return Err(format!(
                    "`events.consume[{i}].group`: must match [a-z0-9][a-z0-9_-]{{0,62}}"
                ));
            }
            if !(1..=DELIVERY_BATCH_MAX).contains(&c.batch_size) {
                return Err(format!(
                    "`events.consume[{i}].batch_size`: must be 1..={DELIVERY_BATCH_MAX}"
                ));
            }
            if !(1..=CONSUMER_MAX_ATTEMPTS_MAX).contains(&c.max_attempts) {
                return Err(format!(
                    "`events.consume[{i}].max_attempts`: must be 1..={CONSUMER_MAX_ATTEMPTS_MAX}"
                ));
            }
            if self.consume[..i]
                .iter()
                .any(|o| o.topic == c.topic && o.group == c.group)
            {
                return Err(format!(
                    "`events.consume[{i}]`: topic `{}` with group `{}` is already consumed",
                    c.topic, c.group
                ));
            }
        }
        Ok(())
    }
}

/// One pub/sub subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    /// Pattern ([`validate_pattern`]).
    pub subject: String,
    /// Queue group: one member service gets each message. `None` = every subscriber gets a copy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

/// Where a new consumer cursor starts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartPosition {
    Earliest,
    #[default]
    Latest,
}

/// What happens to a record that failed `max_attempts` deliveries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnExhausted {
    /// Copy it to the consumer's dead letters and move on.
    #[default]
    Dlq,
    /// Move on.
    Skip,
    /// Stop the cursor until it is reset.
    Block,
}

fn default_group() -> String {
    "default".into()
}
const fn default_batch_size() -> u32 {
    DELIVERY_BATCH_DEFAULT
}
const fn default_consumer_attempts() -> u16 {
    CONSUMER_MAX_ATTEMPTS_DEFAULT
}

/// One stream consumer (`events.consume[]`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamConsumer {
    pub topic: String,
    #[serde(default = "default_group")]
    pub group: String,
    #[serde(default)]
    pub start: StartPosition,
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    #[serde(default = "default_consumer_attempts")]
    pub max_attempts: u16,
    #[serde(default)]
    pub on_exhausted: OnExhausted,
}

impl StreamConsumer {
    /// A consumer of `topic` with every default.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            group: default_group(),
            start: StartPosition::default(),
            batch_size: DELIVERY_BATCH_DEFAULT,
            max_attempts: CONSUMER_MAX_ATTEMPTS_DEFAULT,
            on_exhausted: OnExhausted::default(),
        }
    }
}

/// `config.async`: async invocation of the service's HTTP handler.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AsyncInvokeConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_event_age_s: Option<u32>,
}

impl AsyncInvokeConfig {
    /// No knob set (the block is left out of the service's JSON).
    pub fn is_unset(&self) -> bool {
        *self == Self::default()
    }

    /// Async invocation is off unless enabled.
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }

    /// Attempts in total (function errors), default [`ASYNC_MAX_ATTEMPTS_DEFAULT`].
    pub fn max_attempts(&self) -> u16 {
        self.max_attempts.unwrap_or(ASYNC_MAX_ATTEMPTS_DEFAULT)
    }

    /// How long an invocation may stay pending, default [`ASYNC_MAX_EVENT_AGE_DEFAULT_S`].
    pub fn max_event_age(&self) -> Duration {
        Duration::from_secs(u64::from(self.max_event_age_s.unwrap_or(ASYNC_MAX_EVENT_AGE_DEFAULT_S)))
    }

    /// Bounds; the message names the field.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(n) = self.max_attempts {
            if !(1..=ASYNC_MAX_ATTEMPTS_MAX).contains(&n) {
                return Err(format!("`async.max_attempts`: must be 1..={ASYNC_MAX_ATTEMPTS_MAX}"));
            }
        }
        if let Some(s) = self.max_event_age_s {
            if !(ASYNC_MAX_EVENT_AGE_MIN_S..=ASYNC_MAX_EVENT_AGE_MAX_S).contains(&s) {
                return Err(format!(
                    "`async.max_event_age_s`: must be {ASYNC_MAX_EVENT_AGE_MIN_S}..={ASYNC_MAX_EVENT_AGE_MAX_S}"
                ));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Payloads
// ---------------------------------------------------------------------------------------------

/// A pub/sub message as delivered to a subscriber.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PubsubMessage {
    pub id: MsgId,
    /// The concrete subject published (never a wildcard).
    pub subject: String,
    pub body: Bytes,
    pub headers: Vec<(String, String)>,
    /// `true` for a `request` (the handler's answer is the reply).
    pub replyable: bool,
    pub published_at_ms: i64,
}

impl PubsubMessage {
    /// The guest-facing JSON (`PubsubMessage` of `std:pubsub`, also the `POST /__rt/pubsub` body).
    pub fn guest_json(&self) -> Value {
        json!({
            "id": self.id.to_string(),
            "subject": self.subject,
            "body": body_json(&self.body),
            "headers": headers_json(&self.headers),
            "replyable": self.replyable,
            "publishedAt": self.published_at_ms,
        })
    }
}

/// One stream record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecord {
    pub partition: u32,
    pub offset: u64,
    pub key: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub ts_ms: i64,
}

impl StreamRecord {
    /// The guest-facing JSON (`StreamRecord` of `std:stream`).
    pub fn guest_json(&self) -> Value {
        json!({
            "partition": self.partition,
            "offset": self.offset,
            "key": self.key,
            "body": body_json(&self.body),
            "headers": headers_json(&self.headers),
            "timestamp": self.ts_ms,
        })
    }
}

/// A consumer batch: contiguous records of one partition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBatch {
    pub topic: String,
    pub group: String,
    pub partition: u32,
    pub attempt: u16,
    pub records: Vec<StreamRecord>,
}

impl StreamBatch {
    /// The guest-facing JSON (`StreamBatch` of `std:stream` without `failAt`, also the
    /// `POST /__rt/stream` body).
    pub fn guest_json(&self) -> Value {
        json!({
            "topic": self.topic,
            "group": self.group,
            "partition": self.partition,
            "attempt": self.attempt,
            "messages": self.records.iter().map(StreamRecord::guest_json).collect::<Vec<_>>(),
        })
    }

    /// Body bytes of the batch.
    pub fn body_bytes(&self) -> usize {
        self.records.iter().map(|r| r.body.len()).sum()
    }
}

/// A delivered body: parsed JSON when it parses, else a lossy UTF-8 string.
pub fn body_json(b: &[u8]) -> Value {
    serde_json::from_slice(b).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(b).into_owned()))
}

fn headers_json(h: &[(String, String)]) -> Value {
    Value::Object(
        h.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect::<Map<_, _>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matcher_handles_edge_cases() {
        assert!(subject_matches("a", "a"));
        assert!(!subject_matches("a", "a.b"));
        assert!(!subject_matches("a.b", "a"));
        assert!(!subject_matches("a.>", "a"));
        assert!(subject_matches(">", "a.b.c"));
        assert!(!subject_matches("a.*", "a"));
    }

    #[test]
    fn empty_tokens_and_bad_chars_are_named() {
        assert!(validate_subject("a..b").unwrap_err().contains("empty token"));
        assert!(validate_subject("a.b$").unwrap_err().contains("letters"));
        assert!(validate_pattern("a.>.b").unwrap_err().contains("last"));
    }
}
