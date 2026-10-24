//! Event buses on the host handle: `std:pubsub` (ephemeral subjects, request/reply) and
//! `std:stream` (persistent partitioned topics).
//!
//! The buses themselves live in higher layers (the pub/sub router and the stream store in
//! rt-triggers / rt-server); this module defines what the executors call and the bounds checked
//! before a bus sees a call. Every check answers a stable guest code (`validation_failed`,
//! `too_large`); messages never carry store or driver text.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rt_types::events::{
    headers_over_limits, valid_topic_name, validate_headers, validate_subject, StreamRecord, APPEND_BATCH_MAX,
    APPEND_BATCH_MAX_BYTES, DEDUP_ID_MAX_BYTES, EVENT_BODY_MAX_BYTES, PARTITIONS_HARD_MAX, READ_LIMIT_MAX,
    STREAM_KEY_MAX_BYTES,
};

use crate::calls::TaskCtx;
use crate::facade::HostError;

/// Options of `publish`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishOpts {
    pub headers: Vec<(String, String)>,
}

/// What a publish did: deliveries handed to the scheduler and copies dropped (fan-out cap,
/// quota, backlog, refusals).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublishOut {
    pub receivers: u32,
    pub dropped: u32,
}

/// Options of `request`. The handle always passes `timeout` resolved (default, clamped).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestOpts {
    pub headers: Vec<(String, String)>,
    pub timeout: Option<Duration>,
}

/// The responder's answer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reply {
    pub content_type: Option<String>,
    pub body: Bytes,
}

/// One record to append.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AppendEntry {
    pub key: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub dedup_id: Option<String>,
}

/// Where an appended record landed (`duplicate`: an earlier record with the same dedup id).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AppendedOut {
    pub partition: u32,
    pub offset: u64,
    pub duplicate: bool,
}

/// Where a `read` starts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadStart {
    #[default]
    Earliest,
    Latest,
    Offset(u64),
    /// The first record with a timestamp at or after this unix ms.
    TimeMs(i64),
}

/// Options of `read`. `partition` `None` is allowed only for a one-partition topic: the bus
/// checks it against the topic (`validation_failed` otherwise), as it checks the range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOpts {
    pub partition: Option<u32>,
    pub start: ReadStart,
    pub limit: u32,
}

impl Default for ReadOpts {
    fn default() -> Self {
        Self {
            partition: None,
            start: ReadStart::Earliest,
            limit: READ_LIMIT_MAX as u32,
        }
    }
}

/// A page of records. `next` = the offset after the last returned, `end` = the partition's next
/// offset.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadPage {
    pub records: Vec<StreamRecord>,
    pub next: u64,
    pub end: u64,
}

/// `info(topic)`. `partitions` = `(partition, first offset, next offset)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<(u32, u64, u64)>,
    pub retention_hours: u32,
    pub max_mb: u64,
    pub bytes: u64,
}

/// The node's pub/sub router as the host sees it.
#[async_trait]
pub trait PubsubBus: Send + Sync {
    async fn publish(
        &self,
        from: &TaskCtx,
        subject: &str,
        body: Bytes,
        o: PublishOpts,
    ) -> Result<PublishOut, HostError>;
    async fn request(&self, from: &TaskCtx, subject: &str, body: Bytes, o: RequestOpts) -> Result<Reply, HostError>;
}

/// The node's stream log as the host sees it. Topics are resolved in the caller's namespace.
#[async_trait]
pub trait StreamBus: Send + Sync {
    async fn append(
        &self,
        from: &TaskCtx,
        topic: &str,
        entries: Vec<AppendEntry>,
    ) -> Result<Vec<AppendedOut>, HostError>;
    async fn read(&self, from: &TaskCtx, topic: &str, o: ReadOpts) -> Result<ReadPage, HostError>;
    async fn info(&self, from: &TaskCtx, topic: &str) -> Result<TopicInfo, HostError>;
}

fn invalid(msg: impl Into<String>) -> HostError {
    HostError::new("validation_failed", msg)
}

fn too_large(msg: impl Into<String>) -> HostError {
    HostError::new("too_large", msg)
}

fn check_body(body: &[u8]) -> Result<(), HostError> {
    if body.len() > EVENT_BODY_MAX_BYTES {
        return Err(too_large(format!("body is larger than {EVENT_BODY_MAX_BYTES} bytes")));
    }
    Ok(())
}

fn check_headers(h: &[(String, String)]) -> Result<(), HostError> {
    if headers_over_limits(h) {
        return Err(too_large("headers are over the count or size limit"));
    }
    validate_headers(h).map_err(invalid)
}

fn check_topic(topic: &str) -> Result<(), HostError> {
    if !valid_topic_name(topic) {
        return Err(invalid("topic name must match [a-z0-9][a-z0-9._-]{0,62}"));
    }
    Ok(())
}

/// Bounds of a publish or request: a concrete subject, a body of at most 1 MiB, valid headers.
pub fn check_publish(subject: &str, body: &[u8], headers: &[(String, String)]) -> Result<(), HostError> {
    validate_subject(subject).map_err(invalid)?;
    check_body(body)?;
    check_headers(headers)
}

/// Bounds of an append: 1..=100 records, at most 4 MiB of bodies in total, each record within
/// the body, header, key and dedup-id limits.
pub fn check_append(topic: &str, entries: &[AppendEntry]) -> Result<(), HostError> {
    check_topic(topic)?;
    if entries.is_empty() {
        return Err(invalid("nothing to append"));
    }
    if entries.len() > APPEND_BATCH_MAX {
        return Err(too_large(format!("more than {APPEND_BATCH_MAX} records in one append")));
    }
    let total: usize = entries.iter().map(|e| e.body.len()).sum();
    if total > APPEND_BATCH_MAX_BYTES {
        return Err(too_large(format!(
            "append is larger than {APPEND_BATCH_MAX_BYTES} bytes"
        )));
    }
    for e in entries {
        check_body(&e.body)?;
        check_headers(&e.headers)?;
        if e.key.as_ref().is_some_and(|k| k.len() > STREAM_KEY_MAX_BYTES) {
            return Err(invalid(format!("key is longer than {STREAM_KEY_MAX_BYTES} bytes")));
        }
        if let Some(d) = &e.dedup_id {
            if d.is_empty() || d.len() > DEDUP_ID_MAX_BYTES {
                return Err(invalid(format!("dedupId must be 1..={DEDUP_ID_MAX_BYTES} bytes")));
            }
        }
    }
    Ok(())
}

/// Bounds of a read: a valid topic name, `limit` 1..=100, a partition below the hard maximum.
/// Whether the partition exists (or is required) is the bus's decision.
pub fn check_read(topic: &str, o: &ReadOpts) -> Result<(), HostError> {
    check_topic(topic)?;
    if o.limit == 0 || o.limit as usize > READ_LIMIT_MAX {
        return Err(invalid(format!("limit must be 1..={READ_LIMIT_MAX}")));
    }
    if o.partition.is_some_and(|p| p >= PARTITIONS_HARD_MAX) {
        return Err(invalid("partition is out of range"));
    }
    if matches!(o.start, ReadStart::TimeMs(t) if t < 0) {
        return Err(invalid("fromTime must not be negative"));
    }
    Ok(())
}

/// Bounds of `info`: a valid topic name.
pub fn check_info(topic: &str) -> Result<(), HostError> {
    check_topic(topic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_id_must_not_be_empty() {
        let e = AppendEntry {
            dedup_id: Some(String::new()),
            ..Default::default()
        };
        assert_eq!(check_append("t", &[e]).unwrap_err().code, "validation_failed");
    }

    #[test]
    fn negative_time_and_bad_topic_are_refused() {
        let o = ReadOpts {
            start: ReadStart::TimeMs(-1),
            ..Default::default()
        };
        assert_eq!(check_read("t", &o).unwrap_err().code, "validation_failed");
        assert_eq!(check_info("Bad").unwrap_err().code, "validation_failed");
        assert!(check_info("ok.topic").is_ok());
    }
}
