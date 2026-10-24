//! A socket hub that records what a guest asked for and answers from a fixed table, for the
//! executors' tests (`#[path]`-included from their `tests/common`).

#![allow(dead_code)]

use bytes::Bytes;
use parking_lot::Mutex;
use rt_host::{AcceptOpts, Accepted, HostError, ObjectPage, SocketHost, SocketOp, SocketPage, TaskCtx};
use rt_types::SocketInfo;

pub fn info(id: &str, object: &str) -> SocketInfo {
    SocketInfo {
        id: id.to_string(),
        object: object.to_string(),
        tags: vec!["t".to_string()],
        protocol: Some("v1".to_string()),
        created_at_ms: 10,
        last_message_at_ms: Some(20),
        auto_response_at_ms: Some(30),
        buffered_amount: 4,
        remote_ip: Some("203.0.113.5".to_string()),
    }
}

/// A socket hub that records what the guest asked for and answers from a fixed table.
#[derive(Default)]
pub struct RecordingHub {
    pub calls: Mutex<Vec<String>>,
}

impl RecordingHub {
    fn note(&self, s: String) {
        self.calls.lock().push(s);
    }

    pub fn seen(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

impl SocketHost for RecordingHub {
    fn accept(&self, _: &TaskCtx, object: &str, opts: AcceptOpts) -> Result<Accepted, HostError> {
        self.note(format!(
            "accept {object} {:?} {:?} {:?}",
            opts.tags, opts.attachment, opts.protocol
        ));
        Ok(Accepted {
            socket: "ws_1".into(),
            ticket: "ticket-1".into(),
        })
    }

    fn apply(&self, _: &TaskCtx, op: SocketOp) -> Result<u32, HostError> {
        self.note(format!("apply {op:?}"));
        Ok(3)
    }

    fn list(
        &self,
        _: &TaskCtx,
        object: Option<&str>,
        tag: Option<&str>,
        limit: usize,
        cursor: Option<&str>,
    ) -> SocketPage {
        self.note(format!("list {object:?} {tag:?} {limit} {cursor:?}"));
        (vec![info("ws_1", "room")], Some("ws_1".into()))
    }

    fn get(&self, _: &TaskCtx, socket: &str) -> Option<SocketInfo> {
        (socket == "ws_1").then(|| info("ws_1", "room"))
    }

    fn count(&self, _: &TaskCtx, _: Option<&str>, _: Option<&str>) -> u32 {
        2
    }

    fn objects(&self, _: &TaskCtx, prefix: &str, limit: usize, cursor: Option<&str>) -> ObjectPage {
        self.note(format!("objects {prefix:?} {limit} {cursor:?}"));
        (vec![("room".to_string(), 2)], Some("room".to_string()))
    }

    fn set_tags(&self, _: &TaskCtx, socket: &str, tags: Vec<String>) -> Result<(), HostError> {
        self.note(format!("set_tags {socket} {tags:?}"));
        Ok(())
    }

    fn attachment(&self, _: &TaskCtx, _: &str) -> Result<Option<Bytes>, HostError> {
        Ok(Some(Bytes::from_static(b"att")))
    }

    fn set_attachment(&self, _: &TaskCtx, socket: &str, value: Option<Bytes>) -> Result<(), HostError> {
        self.note(format!("set_attachment {socket} {value:?}"));
        Ok(())
    }

    fn set_auto_response(&self, _: &TaskCtx, object: &str, pair: Option<(String, String)>) -> Result<(), HostError> {
        self.note(format!("set_auto_response {object} {pair:?}"));
        Ok(())
    }
}
