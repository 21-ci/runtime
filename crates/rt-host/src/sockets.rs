//! `std:sockets` behind the host handle (docs/handlers.md "WebSockets").
//!
//! The node owns every socket ([`SocketHost`] is implemented by the socket hub in `rt-triggers`);
//! guests only hold socket ids. Every call is scoped to the calling task's tenant and service: a
//! socket of another service is `not_found`, exactly like a socket that is gone.
//!
//! **Output gate.** A task that acts for an object (an event, a request to a durable object)
//! buffers its sends and closes in its handle and hands them to the hub only after its storage
//! writes committed ([`crate::HostHandle::commit`], called when the handler returned). A task that
//! fails sends nothing; its event is delivered again. `immediate` sends skip the gate.

use bytes::Bytes;
use rt_types::{SocketInfo, WsData};

use crate::calls::TaskCtx;
use crate::facade::HostError;

/// What `sockets.accept(req, opts)` asks for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AcceptOpts {
    pub tags: Vec<String>,
    /// Serialized attachment (V8 structured clone from Deno, raw bytes from WASM).
    pub attachment: Option<Bytes>,
    /// The subprotocol to answer with (one the client offered).
    pub protocol: Option<String>,
}

/// An accepted (not yet connected) socket: its id, and the ticket the accepting response carries
/// to ingress (`x-rt-ws-ticket`), which binds the upgraded connection to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Accepted {
    pub socket: String,
    pub ticket: String,
}

/// One gated socket operation (see the module docs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SocketOp {
    Send {
        socket: String,
        data: WsData,
    },
    Broadcast {
        object: String,
        tag: Option<String>,
        except: Vec<String>,
        data: WsData,
    },
    Close {
        socket: String,
        code: u16,
        reason: String,
    },
    CloseAll {
        object: String,
        tag: Option<String>,
        code: u16,
        reason: String,
    },
}

/// A page of sockets: the sockets and the cursor of the next page.
pub type SocketPage = (Vec<SocketInfo>, Option<String>);

/// A page of objects with sockets: `(name, sockets)` and the cursor of the next page.
pub type ObjectPage = (Vec<(String, u32)>, Option<String>);

/// The node's sockets as one task sees them. Methods are synchronous: the hub keeps its state in
/// memory and writes go to per-socket queues.
pub trait SocketHost: Send + Sync {
    /// Reserve a socket of `object` for the upgrade the task's request offered.
    fn accept(&self, ctx: &TaskCtx, object: &str, opts: AcceptOpts) -> Result<Accepted, HostError>;
    /// Run one socket operation now. `Broadcast` / `CloseAll` return how many sockets they reached.
    fn apply(&self, ctx: &TaskCtx, op: SocketOp) -> Result<u32, HostError>;
    /// Sockets of the service, of one object when `object` is set, with `tag` when set, in id order
    /// after `cursor`.
    fn list(
        &self,
        ctx: &TaskCtx,
        object: Option<&str>,
        tag: Option<&str>,
        limit: usize,
        cursor: Option<&str>,
    ) -> SocketPage;
    fn get(&self, ctx: &TaskCtx, socket: &str) -> Option<SocketInfo>;
    /// The socket exists for this service: open, or accepted and not connected yet.
    fn exists(&self, ctx: &TaskCtx, socket: &str) -> bool {
        self.get(ctx, socket).is_some()
    }
    fn count(&self, ctx: &TaskCtx, object: Option<&str>, tag: Option<&str>) -> u32;
    /// Objects of the service that hold sockets: `(name, sockets)` in name order after `cursor`.
    fn objects(&self, ctx: &TaskCtx, prefix: &str, limit: usize, cursor: Option<&str>) -> ObjectPage;
    fn set_tags(&self, ctx: &TaskCtx, socket: &str, tags: Vec<String>) -> Result<(), HostError>;
    fn attachment(&self, ctx: &TaskCtx, socket: &str) -> Result<Option<Bytes>, HostError>;
    fn set_attachment(&self, ctx: &TaskCtx, socket: &str, value: Option<Bytes>) -> Result<(), HostError>;
    /// The object's automatic answer: a text message equal to `request` is answered with
    /// `response` by the node, without an event. `None` removes it.
    fn set_auto_response(&self, ctx: &TaskCtx, object: &str, pair: Option<(String, String)>) -> Result<(), HostError>;
    /// The object's alarm changed (committed): due at `at` (Unix ms), or removed.
    fn set_alarm(&self, ctx: &TaskCtx, object: &str, at: Option<i64>) {
        let _ = (ctx, object, at);
    }
}

/// No sockets (tests, embedders without the hub): accept is `unavailable`, every socket is gone.
pub struct NoSockets;

impl SocketHost for NoSockets {
    fn accept(&self, _: &TaskCtx, _: &str, _: AcceptOpts) -> Result<Accepted, HostError> {
        Err(HostError::new(
            "unavailable",
            "WebSockets are not available on this node",
        ))
    }
    fn apply(&self, _: &TaskCtx, op: SocketOp) -> Result<u32, HostError> {
        match op {
            SocketOp::Send { .. } | SocketOp::Close { .. } => Err(HostError::new("not_found", "no such socket")),
            _ => Ok(0),
        }
    }
    fn list(&self, _: &TaskCtx, _: Option<&str>, _: Option<&str>, _: usize, _: Option<&str>) -> SocketPage {
        (Vec::new(), None)
    }
    fn get(&self, _: &TaskCtx, _: &str) -> Option<SocketInfo> {
        None
    }
    fn count(&self, _: &TaskCtx, _: Option<&str>, _: Option<&str>) -> u32 {
        0
    }
    fn objects(&self, _: &TaskCtx, _: &str, _: usize, _: Option<&str>) -> ObjectPage {
        (Vec::new(), None)
    }
    fn set_tags(&self, _: &TaskCtx, _: &str, _: Vec<String>) -> Result<(), HostError> {
        Err(HostError::new("not_found", "no such socket"))
    }
    fn attachment(&self, _: &TaskCtx, _: &str) -> Result<Option<Bytes>, HostError> {
        Err(HostError::new("not_found", "no such socket"))
    }
    fn set_attachment(&self, _: &TaskCtx, _: &str, _: Option<Bytes>) -> Result<(), HostError> {
        Err(HostError::new("not_found", "no such socket"))
    }
    fn set_auto_response(&self, _: &TaskCtx, _: &str, _: Option<(String, String)>) -> Result<(), HostError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_types::{ServiceId, TaskId, TenantId};
    use std::time::{Duration, Instant};

    fn ctx() -> TaskCtx {
        TaskCtx {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(2),
            task: TaskId(3),
            depth: 0,
            deadline: Instant::now() + Duration::from_secs(5),
            trace: None,
            on_behalf_of: None,
            object: None,
        }
    }

    /// Without a socket hub every socket operation is answered by the node's refusals: accepting
    /// is `unavailable`, operations on any socket are `not_found`, and listings are empty.
    #[test]
    fn without_a_hub_every_socket_is_gone_and_accepting_is_unavailable() {
        let c = ctx();
        let hub = NoSockets;
        let err = hub.accept(&c, "room", AcceptOpts::default()).expect_err("no accept");
        assert_eq!(err.code, "unavailable");

        let send = SocketOp::Send {
            socket: "s".into(),
            data: WsData::Text("hi".into()),
        };
        assert_eq!(hub.apply(&c, send).expect_err("no socket").code, "not_found");
        let close = SocketOp::Close {
            socket: "s".into(),
            code: 1000,
            reason: String::new(),
        };
        assert_eq!(hub.apply(&c, close).expect_err("no socket").code, "not_found");
        let bcast = SocketOp::Broadcast {
            object: "o".into(),
            tag: None,
            except: vec![],
            data: WsData::Binary(Bytes::new()),
        };
        assert_eq!(
            hub.apply(&c, bcast).expect("a broadcast is not an error"),
            0,
            "a broadcast reaches no one"
        );
        let close_all = SocketOp::CloseAll {
            object: "o".into(),
            tag: None,
            code: 1001,
            reason: String::new(),
        };
        assert_eq!(hub.apply(&c, close_all).expect("close-all is not an error"), 0);

        let (sockets, cursor) = hub.list(&c, None, None, 10, None);
        assert!(sockets.is_empty() && cursor.is_none());
        assert!(hub.get(&c, "s").is_none());
        assert!(!hub.exists(&c, "s"), "exists defers to get");
        assert_eq!(hub.count(&c, Some("o"), None), 0);
        let (objects, cursor) = hub.objects(&c, "", 10, None);
        assert!(objects.is_empty() && cursor.is_none());
        assert_eq!(
            hub.set_tags(&c, "s", vec!["t".into()]).expect_err("no socket").code,
            "not_found"
        );
        assert_eq!(hub.attachment(&c, "s").expect_err("no socket").code, "not_found");
        assert_eq!(
            hub.set_attachment(&c, "s", None).expect_err("no socket").code,
            "not_found"
        );
        hub.set_auto_response(&c, "o", Some(("ping".into(), "pong".into())))
            .expect("an auto-response is accepted and ignored");
    }
}
