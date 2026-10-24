//! Ops behind `std:sockets` (docs/handlers.md "WebSockets"): accept, send, broadcast, close,
//! list/get/count, tags, attachments, auto-response; the event's binary message; and the task
//! commit that opens the output gate. Every call goes through the task's [`rt_host::HostHandle`],
//! which scopes it to this service and buffers sends of an object task until the commit.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use deno_core::serde_v8::ToJsBuffer;
use deno_core::{op2, JsBuffer, OpState};
use deno_error::JsErrorBox;
use rt_host::{AcceptOpts, HostError, HostHandle, SocketOp};
use rt_types::{SocketInfo, WsData};
use serde::{Deserialize, Serialize};

use crate::ops::TaskOps;

/// The binary message of the current event (`webSocketMessage` with an `ArrayBuffer`), put into
/// `OpState` by the executor before dispatch.
pub struct WsEventData(pub Bytes);

fn host(state: &OpState) -> Result<HostHandle, JsErrorBox> {
    state
        .try_borrow::<TaskOps>()
        .map(|t| t.host.clone())
        .ok_or_else(|| JsErrorBox::new("Error", "unavailable"))
}

fn err(e: HostError) -> JsErrorBox {
    JsErrorBox::new("Error", e.code)
}

fn data_of(is_text: bool, text: String, bytes: &[u8]) -> WsData {
    if is_text {
        WsData::Text(text)
    } else {
        WsData::Binary(Bytes::copy_from_slice(bytes))
    }
}

/// `SocketInfo` as guests see it (camelCase, times in Unix ms).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InfoOut {
    id: String,
    object: String,
    tags: Vec<String>,
    protocol: Option<String>,
    created_at: f64,
    last_message_at: Option<f64>,
    auto_response_at: Option<f64>,
    buffered_amount: f64,
    remote_ip: Option<String>,
}

pub fn info_out(i: SocketInfo) -> InfoOut {
    InfoOut {
        id: i.id,
        object: i.object,
        tags: i.tags,
        protocol: i.protocol,
        created_at: i.created_at_ms as f64,
        last_message_at: i.last_message_at_ms.map(|t| t as f64),
        auto_response_at: i.auto_response_at_ms.map(|t| t as f64),
        buffered_amount: i.buffered_amount as f64,
        remote_ip: i.remote_ip,
    }
}

#[derive(Serialize)]
pub struct AcceptOut {
    id: String,
    ticket: String,
    object: String,
}

/// `sockets.accept(req, { tags, attachment, protocol })` (the JS side checked the upgrade offer).
#[op2]
#[serde]
pub fn op_rt_ws_accept(
    state: &mut OpState,
    #[serde] tags: Vec<String>,
    #[string] protocol: String,
    has_attachment: bool,
    #[buffer] attachment: JsBuffer,
) -> Result<AcceptOut, JsErrorBox> {
    let h = host(state)?;
    let opts = AcceptOpts {
        tags,
        attachment: has_attachment.then(|| Bytes::copy_from_slice(&attachment)),
        protocol: (!protocol.is_empty()).then_some(protocol),
    };
    let object = h.object_name().to_string();
    let a = h.ws_accept(opts).map_err(err)?;
    Ok(AcceptOut {
        id: a.socket,
        ticket: a.ticket,
        object,
    })
}

/// `ws.send(data, { immediate })` / `sockets.send(id, data, opts)`.
#[op2]
pub fn op_rt_ws_send(
    state: &mut OpState,
    #[string] id: String,
    is_text: bool,
    #[string] text: String,
    #[buffer] bytes: JsBuffer,
    immediate: bool,
) -> Result<(), JsErrorBox> {
    host(state)?
        .ws_send(&id, data_of(is_text, text, &bytes), immediate)
        .map_err(err)
}

#[derive(Deserialize)]
pub struct Target {
    object: Option<String>,
    tag: Option<String>,
    #[serde(default)]
    except: Vec<String>,
}

/// `sockets.broadcast(data, { object, tag, except, immediate })`: how many sockets it reached.
#[op2]
pub fn op_rt_ws_broadcast(
    state: &mut OpState,
    #[serde] target: Target,
    is_text: bool,
    #[string] text: String,
    #[buffer] bytes: JsBuffer,
    immediate: bool,
) -> Result<u32, JsErrorBox> {
    let h = host(state)?;
    let object = target.object.unwrap_or_else(|| h.object_name().to_string());
    let op = SocketOp::Broadcast {
        object,
        tag: target.tag,
        except: target.except,
        data: data_of(is_text, text, &bytes),
    };
    h.ws_apply(op, immediate).map_err(err)
}

/// `ws.close(code, reason)` / `sockets.close(id, code, reason)`.
#[op2(fast)]
pub fn op_rt_ws_close(
    state: &mut OpState,
    #[string] id: String,
    #[smi] code: u32,
    #[string] reason: String,
    immediate: bool,
) -> Result<(), JsErrorBox> {
    let code = u16::try_from(code).map_err(|_| JsErrorBox::new("Error", "invalid_request"))?;
    host(state)?
        .ws_apply(
            SocketOp::Close {
                socket: id,
                code,
                reason,
            },
            immediate,
        )
        .map(|_| ())
        .map_err(err)
}

/// `sockets.closeAll({ object, tag, code, reason })`.
#[op2]
pub fn op_rt_ws_close_all(
    state: &mut OpState,
    #[serde] target: Target,
    #[smi] code: u32,
    #[string] reason: String,
    immediate: bool,
) -> Result<u32, JsErrorBox> {
    let h = host(state)?;
    let code = u16::try_from(code).map_err(|_| JsErrorBox::new("Error", "invalid_request"))?;
    let object = target.object.unwrap_or_else(|| h.object_name().to_string());
    h.ws_apply(
        SocketOp::CloseAll {
            object,
            tag: target.tag,
            code,
            reason,
        },
        immediate,
    )
    .map_err(err)
}

#[derive(Deserialize)]
pub struct ListQuery {
    object: Option<String>,
    tag: Option<String>,
    limit: Option<u32>,
    cursor: Option<String>,
    #[serde(default)]
    prefix: String,
}

#[derive(Serialize)]
pub struct SocketPageOut {
    sockets: Vec<InfoOut>,
    cursor: Option<String>,
}

/// The object a query without one addresses: the task's object, else every object.
fn query_object(h: &HostHandle, q: &ListQuery) -> Option<String> {
    q.object.clone().or_else(|| h.object().map(|o| o.name.to_string()))
}

#[op2]
#[serde]
pub fn op_rt_ws_list(state: &mut OpState, #[serde] q: ListQuery) -> Result<SocketPageOut, JsErrorBox> {
    let h = host(state)?;
    let object = query_object(&h, &q);
    let limit = q.limit.unwrap_or(100) as usize;
    let (sockets, cursor) = h
        .ws_list(object.as_deref(), q.tag.as_deref(), limit, q.cursor.as_deref())
        .map_err(err)?;
    Ok(SocketPageOut {
        sockets: sockets.into_iter().map(info_out).collect(),
        cursor,
    })
}

#[op2]
#[serde]
pub fn op_rt_ws_get(state: &mut OpState, #[string] id: String) -> Result<Option<InfoOut>, JsErrorBox> {
    Ok(host(state)?.ws_get(&id).map_err(err)?.map(info_out))
}

#[op2]
pub fn op_rt_ws_count(state: &mut OpState, #[serde] q: ListQuery) -> Result<u32, JsErrorBox> {
    let h = host(state)?;
    let object = query_object(&h, &q);
    h.ws_count(object.as_deref(), q.tag.as_deref()).map_err(err)
}

#[derive(Serialize)]
pub struct ObjectOut {
    name: String,
    sockets: u32,
}

#[derive(Serialize)]
pub struct ObjectPageOut {
    objects: Vec<ObjectOut>,
    cursor: Option<String>,
}

#[op2]
#[serde]
pub fn op_rt_ws_objects(state: &mut OpState, #[serde] q: ListQuery) -> Result<ObjectPageOut, JsErrorBox> {
    let h = host(state)?;
    let limit = q.limit.unwrap_or(100) as usize;
    let (objects, cursor) = h.ws_objects(&q.prefix, limit, q.cursor.as_deref()).map_err(err)?;
    Ok(ObjectPageOut {
        objects: objects
            .into_iter()
            .map(|(name, sockets)| ObjectOut { name, sockets })
            .collect(),
        cursor,
    })
}

#[op2]
pub fn op_rt_ws_set_tags(
    state: &mut OpState,
    #[string] id: String,
    #[serde] tags: Vec<String>,
) -> Result<(), JsErrorBox> {
    host(state)?.ws_set_tags(&id, tags).map_err(err)
}

/// The socket's attachment as serialized (`null` when it has none).
#[op2]
#[serde]
pub fn op_rt_ws_attachment(state: &mut OpState, #[string] id: String) -> Result<Option<ToJsBuffer>, JsErrorBox> {
    Ok(host(state)?.ws_attachment(&id).map_err(err)?.map(|b| b.to_vec().into()))
}

#[op2]
pub fn op_rt_ws_set_attachment(
    state: &mut OpState,
    #[string] id: String,
    has: bool,
    #[buffer] value: JsBuffer,
) -> Result<(), JsErrorBox> {
    host(state)?
        .ws_set_attachment(&id, has.then(|| Bytes::copy_from_slice(&value)))
        .map_err(err)
}

/// `sockets.setAutoResponse({ request, response } | null, { object })`.
#[op2(fast)]
pub fn op_rt_ws_auto_response(
    state: &mut OpState,
    #[string] object: String,
    has: bool,
    #[string] request: String,
    #[string] response: String,
) -> Result<(), JsErrorBox> {
    let h = host(state)?;
    let object = if object.is_empty() {
        h.object_name().to_string()
    } else {
        object
    };
    h.ws_set_auto_response(&object, has.then_some((request, response)))
        .map_err(err)
}

/// The binary message of the current event.
#[op2]
#[serde]
pub fn op_rt_ws_event_data(state: &mut OpState) -> ToJsBuffer {
    state
        .try_borrow::<WsEventData>()
        .map(|d| d.0.to_vec())
        .unwrap_or_default()
        .into()
}

/// The handler returned: commit the task's object storage and open its output gate.
#[op2]
pub async fn op_rt_task_commit(state: Rc<RefCell<OpState>>) -> Result<(), JsErrorBox> {
    let h = host(&state.borrow())?;
    h.commit().await.map_err(err)
}

// ---------------------------------------------------------------------------------------------
// Outbound WebSockets (`new WebSocket(url)`) through the egress guard
// ---------------------------------------------------------------------------------------------

/// A task's outbound WebSocket. Named `rt…`: a kept isolate closes it with the task.
pub struct WsClientRes(pub Arc<rt_host::WsClient>);

impl deno_core::Resource for WsClientRes {
    fn name(&self) -> std::borrow::Cow<'_, str> {
        "rtWsClient".into()
    }

    /// The task ended with the socket still open (a kept isolate): close it now. Dropping the
    /// resource is not enough, a pending receive of the guest still holds the client (DWARM-003).
    fn close(self: Rc<Self>) {
        let client = self.0.clone();
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                let _ = client.close(1000, "task ended").await;
            });
        }
    }
}

fn client(state: &Rc<RefCell<OpState>>, rid: deno_core::ResourceId) -> Result<Arc<rt_host::WsClient>, JsErrorBox> {
    state
        .borrow()
        .resource_table
        .get::<WsClientRes>(rid)
        .map(|r| r.0.clone())
        .map_err(|_| JsErrorBox::new("Error", "invalid_state"))
}

#[derive(Serialize)]
pub struct ConnectOut {
    rid: u32,
    protocol: String,
}

#[op2]
#[serde]
pub async fn op_rt_ws_connect(
    state: Rc<RefCell<OpState>>,
    #[string] url: String,
    #[serde] protocols: Vec<String>,
) -> Result<ConnectOut, JsErrorBox> {
    let h = host(&state.borrow())?;
    let c = h.ws_connect(&url, &protocols).await.map_err(err)?;
    let protocol = c.protocol().unwrap_or_default().to_string();
    let rid = state.borrow_mut().resource_table.add(WsClientRes(Arc::new(c)));
    Ok(ConnectOut { rid, protocol })
}

#[op2]
pub async fn op_rt_ws_client_send(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: u32,
    is_text: bool,
    #[string] text: String,
    #[buffer] bytes: JsBuffer,
) -> Result<(), JsErrorBox> {
    let c = client(&state, rid)?;
    c.send(data_of(is_text, text, &bytes)).await.map_err(err)
}

#[derive(Serialize)]
pub struct RecvOut {
    kind: &'static str,
    text: Option<String>,
    data: Option<ToJsBuffer>,
    code: u16,
    reason: String,
}

/// The next message (`kind` text / binary) or the end of the connection (`close`).
#[op2]
#[serde]
pub async fn op_rt_ws_client_recv(state: Rc<RefCell<OpState>>, #[smi] rid: u32) -> Result<RecvOut, JsErrorBox> {
    let c = client(&state, rid)?;
    let out = match c.recv().await.map_err(err)? {
        rt_host::ClientEvent::Message(WsData::Text(t)) => RecvOut {
            kind: "text",
            text: Some(t),
            data: None,
            code: 0,
            reason: String::new(),
        },
        rt_host::ClientEvent::Message(WsData::Binary(b)) => RecvOut {
            kind: "binary",
            text: None,
            data: Some(b.to_vec().into()),
            code: 0,
            reason: String::new(),
        },
        rt_host::ClientEvent::Closed { code, reason } => RecvOut {
            kind: "close",
            text: None,
            data: None,
            code,
            reason,
        },
    };
    Ok(out)
}

#[op2]
pub async fn op_rt_ws_client_close(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: u32,
    #[smi] code: u32,
    #[string] reason: String,
) -> Result<(), JsErrorBox> {
    let c = client(&state, rid)?;
    let code = u16::try_from(code).map_err(|_| JsErrorBox::new("Error", "invalid_request"))?;
    c.close(code, &reason).await.map_err(err)
}
