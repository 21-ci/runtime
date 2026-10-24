//! Deno ops for `std:pubsub` (publish, request) and `std:stream` (append, read, info). Each op is
//! a thin call into the task's [`HostHandle`]: the bounds, the scoping (the caller's tenant and
//! namespace) and the error codes all live in `rt-host`. The JS error message is the stable code.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use deno_core::serde_v8::ToJsBuffer;
use deno_core::{op2, JsBuffer, OpState};
use deno_error::JsErrorBox;
use rt_host::{AppendEntry, HostError, HostHandle, PublishOpts, ReadOpts, ReadStart, RequestOpts};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ops::TaskOps;

fn host(state: &Rc<RefCell<OpState>>) -> Result<HostHandle, JsErrorBox> {
    state
        .borrow()
        .try_borrow::<TaskOps>()
        .map(|t| t.host.clone())
        .ok_or_else(|| JsErrorBox::new("Error", "unavailable"))
}

fn err(e: HostError) -> JsErrorBox {
    JsErrorBox::new("Error", e.code)
}

fn invalid() -> JsErrorBox {
    JsErrorBox::new("Error", "validation_failed")
}

// ---------------------------------------------------------------------------------------------
// std:pubsub
// ---------------------------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct PublishIn {
    subject: String,
    headers: Vec<(String, String)>,
}

#[derive(Serialize)]
pub struct PublishResultOut {
    receivers: u32,
    dropped: u32,
}

/// `publish(subject, body, {headers})`: resolves after routing to `{receivers, dropped}`.
#[op2]
#[serde]
pub async fn op_rt_pubsub_publish(
    state: Rc<RefCell<OpState>>,
    #[serde] o: PublishIn,
    #[buffer] body: JsBuffer,
) -> Result<PublishResultOut, JsErrorBox> {
    let h = host(&state)?;
    let out = h
        .pubsub_publish(
            &o.subject,
            Bytes::copy_from_slice(&body),
            PublishOpts { headers: o.headers },
        )
        .await
        .map_err(err)?;
    Ok(PublishResultOut {
        receivers: out.receivers,
        dropped: out.dropped,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestIn {
    subject: String,
    headers: Vec<(String, String)>,
    timeout_ms: Option<f64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplyOut {
    content_type: Option<String>,
    body: ToJsBuffer,
}

/// `request(subject, body, {headers, timeoutMs})`: the reply's content type and bytes (the JS
/// side decodes them). A timeout of `<= 0` ms is `timeout` at once.
#[op2]
#[serde]
pub async fn op_rt_pubsub_request(
    state: Rc<RefCell<OpState>>,
    #[serde] o: RequestIn,
    #[buffer] body: JsBuffer,
) -> Result<ReplyOut, JsErrorBox> {
    let h = host(&state)?;
    let timeout = match o.timeout_ms {
        Some(ms) if ms.is_nan() || ms <= 0.0 => return Err(JsErrorBox::new("Error", "timeout")),
        Some(ms) => Some(Duration::from_millis(ms.min(u64::MAX as f64) as u64)),
        None => None,
    };
    let reply = h
        .pubsub_request(
            &o.subject,
            Bytes::copy_from_slice(&body),
            RequestOpts {
                headers: o.headers,
                timeout,
            },
        )
        .await
        .map_err(err)?;
    Ok(ReplyOut {
        content_type: reply.content_type,
        body: reply.body.to_vec().into(),
    })
}

// ---------------------------------------------------------------------------------------------
// std:stream
// ---------------------------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryIn {
    key: Option<String>,
    headers: Vec<(String, String)>,
    dedup_id: Option<String>,
    len: f64,
}

#[derive(Deserialize)]
pub struct AppendIn {
    topic: String,
    entries: Vec<EntryIn>,
}

#[derive(Serialize)]
pub struct AppendedJs {
    partition: u32,
    offset: u64,
    duplicate: bool,
}

/// Split the concatenated `bodies` by the entries' lengths; the lengths must add up to the
/// buffer exactly.
fn split_entries(entries: Vec<EntryIn>, bodies: &[u8]) -> Result<Vec<AppendEntry>, JsErrorBox> {
    let mut at = 0usize;
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        if !(e.len >= 0.0 && e.len.fract() == 0.0) {
            return Err(invalid());
        }
        let end = at
            .checked_add(e.len as usize)
            .filter(|end| *end <= bodies.len())
            .ok_or_else(invalid)?;
        out.push(AppendEntry {
            key: e.key,
            headers: e.headers,
            body: Bytes::copy_from_slice(&bodies[at..end]),
            dedup_id: e.dedup_id,
        });
        at = end;
    }
    if at != bodies.len() {
        return Err(invalid());
    }
    Ok(out)
}

/// `append` / `appendBatch`: one transaction, durable when it resolves.
#[op2]
#[serde]
pub async fn op_rt_stream_append(
    state: Rc<RefCell<OpState>>,
    #[serde] a: AppendIn,
    #[buffer] bodies: JsBuffer,
) -> Result<Vec<AppendedJs>, JsErrorBox> {
    let h = host(&state)?;
    let entries = split_entries(a.entries, &bodies)?;
    let out = h.stream_append(&a.topic, entries).await.map_err(err)?;
    Ok(out
        .into_iter()
        .map(|o| AppendedJs {
            partition: o.partition,
            offset: o.offset,
            duplicate: o.duplicate,
        })
        .collect())
}

#[derive(Deserialize)]
pub struct ReadIn {
    topic: String,
    partition: Option<f64>,
    /// `earliest`, `latest`, `offset` or `time`.
    kind: String,
    value: f64,
    limit: f64,
}

fn whole(v: f64) -> Result<u64, JsErrorBox> {
    if v >= 0.0 && v.fract() == 0.0 && v <= 9_007_199_254_740_991.0 {
        Ok(v as u64)
    } else {
        Err(invalid())
    }
}

fn read_opts(r: &ReadIn) -> Result<ReadOpts, JsErrorBox> {
    let start = match r.kind.as_str() {
        "earliest" => ReadStart::Earliest,
        "latest" => ReadStart::Latest,
        "offset" => ReadStart::Offset(whole(r.value)?),
        "time" => ReadStart::TimeMs(whole(r.value)? as i64),
        _ => return Err(invalid()),
    };
    let partition = match r.partition {
        Some(p) => Some(u32::try_from(whole(p)?).map_err(|_| invalid())?),
        None => None,
    };
    let limit = u32::try_from(whole(r.limit)?).map_err(|_| invalid())?;
    Ok(ReadOpts {
        partition,
        start,
        limit,
    })
}

#[derive(Serialize)]
pub struct PageOut {
    /// Each record as `StreamRecord::guest_json` (decoded body, headers object).
    records: Vec<Value>,
    next: u64,
    end: u64,
}

/// `read(topic, opts)`: `{records, next, end}`, bodies decoded as for a delivery.
#[op2]
#[serde]
pub async fn op_rt_stream_read(state: Rc<RefCell<OpState>>, #[serde] r: ReadIn) -> Result<PageOut, JsErrorBox> {
    let h = host(&state)?;
    let o = read_opts(&r)?;
    let page = h.stream_read(&r.topic, o).await.map_err(err)?;
    Ok(PageOut {
        records: page.records.iter().map(|r| r.guest_json()).collect(),
        next: page.next,
        end: page.end,
    })
}

#[derive(Serialize)]
pub struct PartitionOut {
    partition: u32,
    first: u64,
    next: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InfoOut {
    name: String,
    partitions: Vec<PartitionOut>,
    retention_hours: u32,
    max_mb: u64,
    bytes: u64,
}

/// `info(topic)`.
#[op2]
#[serde]
pub async fn op_rt_stream_info(state: Rc<RefCell<OpState>>, #[string] topic: String) -> Result<InfoOut, JsErrorBox> {
    let h = host(&state)?;
    let i = h.stream_info(&topic).await.map_err(err)?;
    Ok(InfoOut {
        name: i.name,
        partitions: i
            .partitions
            .into_iter()
            .map(|(partition, first, next)| PartitionOut { partition, first, next })
            .collect(),
        retention_hours: i.retention_hours,
        max_mb: i.max_mb,
        bytes: i.bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(len: f64) -> EntryIn {
        EntryIn {
            key: None,
            headers: vec![],
            dedup_id: None,
            len,
        }
    }

    #[test]
    fn split_entries_needs_the_exact_buffer_length() {
        let out = split_entries(vec![entry(2.0), entry(1.0)], b"abc").unwrap();
        assert_eq!(&out[0].body[..], b"ab");
        assert_eq!(&out[1].body[..], b"c");
        assert!(split_entries(vec![entry(2.0)], b"abc").is_err());
        assert!(split_entries(vec![entry(4.0)], b"abc").is_err());
        assert!(split_entries(vec![entry(-1.0)], b"").is_err());
        assert!(split_entries(vec![entry(0.5)], b"a").is_err());
    }

    #[test]
    fn read_opts_maps_the_start_and_refuses_bad_numbers() {
        let r = |kind: &str, value: f64, partition: Option<f64>, limit: f64| ReadIn {
            topic: "t".into(),
            partition,
            kind: kind.into(),
            value,
            limit,
        };
        assert_eq!(
            read_opts(&r("offset", 7.0, Some(1.0), 5.0)).unwrap().start,
            ReadStart::Offset(7)
        );
        assert_eq!(
            read_opts(&r("time", 9.0, None, 5.0)).unwrap().start,
            ReadStart::TimeMs(9)
        );
        assert_eq!(
            read_opts(&r("latest", 0.0, None, 5.0)).unwrap().start,
            ReadStart::Latest
        );
        assert_eq!(read_opts(&r("earliest", 0.0, None, 5.0)).unwrap().partition, None);
        assert!(read_opts(&r("nope", 0.0, None, 5.0)).is_err());
        assert!(read_opts(&r("offset", -1.0, None, 5.0)).is_err());
        assert!(read_opts(&r("offset", 0.0, Some(1e12), 5.0)).is_err());
        assert!(read_opts(&r("offset", 0.0, None, 1e12)).is_err());
    }
}
