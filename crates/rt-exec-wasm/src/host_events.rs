//! WASM host imports for `std:host/pubsub` and `std:host/streams` (`/wit/rt-host.wit`), over the
//! task's [`rt_host::HostHandle`]: the same buses, bounds and codes as the Deno `std:pubsub` /
//! `std:stream` modules. A detached handle (start code outside a task) or a node without the event
//! bus answers `unavailable`. A call that waited is an I/O boundary: the frozen clock catches up.

use std::time::Duration;

use bytes::Bytes;
use rt_host::events::{AppendEntry, PublishOpts, ReadOpts, ReadStart, RequestOpts};
use rt_host::HostError;

use crate::host_wit::wit::std::host::{pubsub, streams};
use crate::state::WasmState;

fn pubsub_err(e: HostError) -> pubsub::Error {
    pubsub::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

fn streams_err(e: HostError) -> streams::Error {
    streams::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

impl pubsub::Host for WasmState {
    async fn publish(
        &mut self,
        subject: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<pubsub::PublishResult, pubsub::Error> {
        let o = PublishOpts { headers };
        let r = self
            .clock
            .io(self.host.pubsub_publish(&subject, Bytes::from(body), o))
            .await;
        let out = r.map_err(pubsub_err)?;
        Ok(pubsub::PublishResult {
            receivers: out.receivers,
            dropped: out.dropped,
        })
    }

    async fn request(
        &mut self,
        subject: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        timeout_ms: Option<u32>,
    ) -> Result<pubsub::Reply, pubsub::Error> {
        let o = RequestOpts {
            headers,
            timeout: timeout_ms.map(|ms| Duration::from_millis(u64::from(ms))),
        };
        let r = self
            .clock
            .io(self.host.pubsub_request(&subject, Bytes::from(body), o))
            .await;
        let reply = r.map_err(pubsub_err)?;
        Ok(pubsub::Reply {
            content_type: reply.content_type,
            body: reply.body.to_vec(),
        })
    }
}

fn read_start(p: streams::Position) -> ReadStart {
    match p {
        streams::Position::Earliest => ReadStart::Earliest,
        streams::Position::Latest => ReadStart::Latest,
        streams::Position::Offset(o) => ReadStart::Offset(o),
        streams::Position::TimeMs(t) => ReadStart::TimeMs(t),
    }
}

impl streams::Host for WasmState {
    async fn append(
        &mut self,
        topic: String,
        entries: Vec<streams::NewEntry>,
    ) -> Result<Vec<streams::Appended>, streams::Error> {
        let entries = entries
            .into_iter()
            .map(|e| AppendEntry {
                key: e.key,
                headers: e.headers,
                body: Bytes::from(e.body),
                dedup_id: e.dedup_id,
            })
            .collect();
        let r = self.clock.io(self.host.stream_append(&topic, entries)).await;
        Ok(r.map_err(streams_err)?
            .into_iter()
            .map(|a| streams::Appended {
                partition: a.partition,
                offset: a.offset,
                duplicate: a.duplicate,
            })
            .collect())
    }

    async fn read(&mut self, topic: String, opts: streams::ReadOptions) -> Result<streams::Page, streams::Error> {
        let o = ReadOpts {
            partition: Some(opts.partition),
            start: read_start(opts.start),
            limit: opts.limit,
        };
        let r = self.clock.io(self.host.stream_read(&topic, o)).await;
        let page = r.map_err(streams_err)?;
        Ok(streams::Page {
            entries: page
                .records
                .into_iter()
                .map(|r| streams::Entry {
                    partition: r.partition,
                    offset: r.offset,
                    key: r.key,
                    headers: r.headers,
                    body: r.body.to_vec(),
                    timestamp_ms: r.ts_ms,
                })
                .collect(),
            next: page.next,
            end: page.end,
        })
    }

    async fn info(&mut self, topic: String) -> Result<streams::TopicInfo, streams::Error> {
        let r = self.clock.io(self.host.stream_info(&topic)).await;
        let info = r.map_err(streams_err)?;
        Ok(streams::TopicInfo {
            name: info.name,
            partitions: info
                .partitions
                .into_iter()
                .map(|(partition, first, next)| streams::PartitionInfo { partition, first, next })
                .collect(),
            retention_hours: info.retention_hours,
            max_mb: info.max_mb,
            bytes: info.bytes,
        })
    }
}
