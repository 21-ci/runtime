//! `std:host/sockets` and `std:host/durable` (WIT) over the task's [`rt_host::HostHandle`]: the
//! same calls as the Deno `std:sockets` / `std:durable` modules. Storage values are raw
//! bytes here (`DURABLE_FORMAT_BYTES`).

use bytes::Bytes;
use rt_host::{AcceptOpts, CallError, HostError, SocketOp};
use rt_store::{DurableListOptions, DurableValue, DURABLE_FORMAT_BYTES};
use rt_types::{SocketInfo, WsData};

use std::sync::Arc;
use std::time::Duration;

use wasmtime::component::Resource;

use crate::host_wit::wit::std::host::{call, durable, sockets, ws_client};
use crate::host_wit::{call_err, call_request};
use crate::state::WasmState;

fn serr(e: HostError) -> sockets::Error {
    sockets::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

fn derr(e: HostError) -> durable::Error {
    durable::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

fn data_of(p: sockets::Payload) -> WsData {
    match p {
        sockets::Payload::Text(t) => WsData::Text(t),
        sockets::Payload::Binary(b) => WsData::Binary(Bytes::from(b)),
    }
}

/// A socket's info as the WIT record.
pub(crate) fn info_wit(i: SocketInfo) -> sockets::SocketInfo {
    sockets::SocketInfo {
        id: i.id,
        object: i.object,
        tags: i.tags,
        protocol: i.protocol,
        created_at_ms: i.created_at_ms,
        last_message_at_ms: i.last_message_at_ms,
        auto_response_at_ms: i.auto_response_at_ms,
        buffered_amount: i.buffered_amount,
        remote_ip: i.remote_ip,
    }
}

impl WasmState {
    fn object_or(&self, object: Option<String>) -> String {
        object.unwrap_or_else(|| self.host.object_name().to_string())
    }
}

impl sockets::Host for WasmState {
    async fn accept(&mut self, opts: sockets::AcceptOptions) -> Result<sockets::Accepted, sockets::Error> {
        let opts = AcceptOpts {
            tags: opts.tags,
            attachment: opts.attachment.map(Bytes::from),
            protocol: opts.protocol,
        };
        self.host
            .ws_accept(opts)
            .map(|a| sockets::Accepted {
                id: a.socket,
                ticket: a.ticket,
            })
            .map_err(serr)
    }

    async fn send(&mut self, id: String, data: sockets::Payload) -> Result<(), sockets::Error> {
        self.host.ws_send(&id, data_of(data), false).map_err(serr)
    }

    async fn broadcast(
        &mut self,
        object: Option<String>,
        tag: Option<String>,
        data: sockets::Payload,
        except: Vec<String>,
    ) -> Result<u32, sockets::Error> {
        let op = SocketOp::Broadcast {
            object: self.object_or(object),
            tag,
            except,
            data: data_of(data),
        };
        self.host.ws_apply(op, false).map_err(serr)
    }

    async fn close(&mut self, id: String, code: u16, reason: String) -> Result<(), sockets::Error> {
        self.host
            .ws_apply(
                SocketOp::Close {
                    socket: id,
                    code,
                    reason,
                },
                false,
            )
            .map(|_| ())
            .map_err(serr)
    }

    async fn close_all(
        &mut self,
        object: Option<String>,
        tag: Option<String>,
        code: u16,
        reason: String,
    ) -> Result<u32, sockets::Error> {
        let op = SocketOp::CloseAll {
            object: self.object_or(object),
            tag,
            code,
            reason,
        };
        self.host.ws_apply(op, false).map_err(serr)
    }

    async fn list(
        &mut self,
        object: Option<String>,
        tag: Option<String>,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<(Vec<sockets::SocketInfo>, Option<String>), sockets::Error> {
        let object = object.or_else(|| self.host.object().map(|o| o.name.to_string()));
        let (items, next) = self
            .host
            .ws_list(object.as_deref(), tag.as_deref(), limit as usize, cursor.as_deref())
            .map_err(serr)?;
        Ok((items.into_iter().map(info_wit).collect(), next))
    }

    async fn get(&mut self, id: String) -> Result<Option<sockets::SocketInfo>, sockets::Error> {
        Ok(self.host.ws_get(&id).map_err(serr)?.map(info_wit))
    }

    async fn set_tags(&mut self, id: String, tags: Vec<String>) -> Result<(), sockets::Error> {
        self.host.ws_set_tags(&id, tags).map_err(serr)
    }

    async fn get_attachment(&mut self, id: String) -> Result<Option<Vec<u8>>, sockets::Error> {
        Ok(self.host.ws_attachment(&id).map_err(serr)?.map(|b| b.to_vec()))
    }

    async fn set_attachment(&mut self, id: String, value: Option<Vec<u8>>) -> Result<(), sockets::Error> {
        self.host.ws_set_attachment(&id, value.map(Bytes::from)).map_err(serr)
    }

    async fn set_auto_response(
        &mut self,
        object: Option<String>,
        pair: Option<(String, String)>,
    ) -> Result<(), sockets::Error> {
        let object = self.object_or(object);
        self.host.ws_set_auto_response(&object, pair).map_err(serr)
    }
}

impl durable::Host for WasmState {
    async fn object(&mut self) -> Option<String> {
        self.host.object().map(|o| o.name.to_string())
    }

    async fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, durable::Error> {
        let host = self.host.clone();
        let r = self
            .clock
            .io(async move { host.storage_get(&[Bytes::from(key)]).await })
            .await
            .map_err(derr)?;
        Ok(r.into_iter().next().flatten().map(|v| v.data.to_vec()))
    }

    async fn put(&mut self, key: String, value: Vec<u8>) -> Result<(), durable::Error> {
        let value = DurableValue {
            format: DURABLE_FORMAT_BYTES,
            data: Bytes::from(value),
        };
        self.host.storage_put(Bytes::from(key), value).map_err(derr)
    }

    async fn delete(&mut self, key: String) -> Result<bool, durable::Error> {
        let host = self.host.clone();
        let n = self
            .clock
            .io(async move { host.storage_delete(&[Bytes::from(key)]).await })
            .await
            .map_err(derr)?;
        Ok(n > 0)
    }

    async fn list(
        &mut self,
        prefix: String,
        start: Option<String>,
        end: Option<String>,
        limit: u32,
        reverse: bool,
    ) -> Result<Vec<(String, Vec<u8>)>, durable::Error> {
        let opts = DurableListOptions {
            prefix: Bytes::from(prefix),
            start: start.map(Bytes::from),
            end: end.map(Bytes::from),
            limit: limit as usize,
            reverse,
        };
        let host = self.host.clone();
        let page = self
            .clock
            .io(async move { host.storage_list(opts).await })
            .await
            .map_err(derr)?;
        Ok(page
            .into_iter()
            .map(|(k, v)| (String::from_utf8_lossy(&k).into_owned(), v.data.to_vec()))
            .collect())
    }

    async fn delete_all(&mut self) -> Result<(), durable::Error> {
        self.host.storage_delete_all().map_err(derr)
    }

    async fn get_alarm(&mut self) -> Result<Option<i64>, durable::Error> {
        let host = self.host.clone();
        self.clock.io(async move { host.alarm_get().await }).await.map_err(derr)
    }

    async fn set_alarm(&mut self, at_ms: Option<i64>) -> Result<(), durable::Error> {
        self.host.alarm_set(at_ms).map_err(derr)
    }

    async fn fetch(
        &mut self,
        service: Option<String>,
        object: String,
        req: call::Request,
    ) -> Result<call::Response, durable::Error> {
        let to_d = |e: call::Error| durable::Error {
            code: e.code,
            message: e.message,
        };
        if self.host.is_detached() {
            return Err(to_d(call_err(
                "unavailable",
                "std:host is not available outside a task",
            )));
        }
        let request = call_request(req).map_err(to_d)?;
        let cap = self.host.limits().max_response_buffered_bytes;
        let host = self.host.clone();
        let exchange = async move {
            match host.call_object(service.as_deref(), &object, request).await {
                Ok(resp) => {
                    let (parts, body) = resp.into_parts();
                    let headers = parts
                        .headers
                        .iter()
                        .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
                        .collect();
                    match body.collect(cap).await {
                        Ok(bytes) => Ok(call::Response {
                            status: parts.status.as_u16(),
                            headers,
                            body: bytes.to_vec(),
                        }),
                        Err(_) => Err(call_err(
                            "response_too_large",
                            "the response exceeds the buffered response limit",
                        )),
                    }
                }
                Err(e @ CallError::Depth) => Ok(call::Response {
                    status: 508,
                    headers: vec![("x-rt-reason".to_string(), e.code().as_bytes().to_vec())],
                    body: Vec::new(),
                }),
                Err(e) => Err(call_err(e.code(), e.code())),
            }
        };
        self.clock.io(exchange).await.map_err(to_d)
    }
}

// ---- ws-client ---------------------------------------------------------------------------------

/// The host side of a `ws-client.connection` resource.
pub struct WsConnection {
    client: Arc<rt_host::WsClient>,
}

fn cerr(e: HostError) -> ws_client::Error {
    ws_client::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

fn gone() -> ws_client::Error {
    ws_client::Error {
        code: "invalid_state".to_string(),
        message: "no such connection".to_string(),
    }
}

impl WasmState {
    fn connection(&self, r: &Resource<WsConnection>) -> Result<Arc<rt_host::WsClient>, ws_client::Error> {
        self.table.get(r).map(|c| c.client.clone()).map_err(|_| gone())
    }
}

impl ws_client::Host for WasmState {
    async fn connect(
        &mut self,
        url: String,
        protocols: Vec<String>,
    ) -> Result<Resource<WsConnection>, ws_client::Error> {
        let host = self.host.clone();
        let client = self
            .clock
            .io(async move { host.ws_connect(&url, &protocols).await })
            .await
            .map_err(cerr)?;
        self.table
            .push(WsConnection {
                client: Arc::new(client),
            })
            .map_err(|e| ws_client::Error {
                code: "internal".to_string(),
                message: e.to_string(),
            })
    }
}

impl ws_client::HostConnection for WasmState {
    async fn protocol(&mut self, this: Resource<WsConnection>) -> Option<String> {
        self.connection(&this)
            .ok()
            .and_then(|c| c.protocol().map(str::to_string))
    }

    async fn send(&mut self, this: Resource<WsConnection>, data: sockets::Payload) -> Result<(), ws_client::Error> {
        let c = self.connection(&this)?;
        self.clock
            .io(async move { c.send(data_of(data)).await })
            .await
            .map_err(cerr)
    }

    async fn recv(
        &mut self,
        this: Resource<WsConnection>,
        timeout_ms: u32,
    ) -> Result<Option<ws_client::Event>, ws_client::Error> {
        let c = self.connection(&this)?;
        let wait = Duration::from_millis(u64::from(timeout_ms));
        let got = self
            .clock
            .io(async move { tokio::time::timeout(wait, c.recv()).await })
            .await;
        let Ok(event) = got else { return Ok(None) };
        Ok(Some(match event.map_err(cerr)? {
            rt_host::ClientEvent::Message(WsData::Text(t)) => ws_client::Event::Message(sockets::Payload::Text(t)),
            rt_host::ClientEvent::Message(WsData::Binary(b)) => {
                ws_client::Event::Message(sockets::Payload::Binary(b.to_vec()))
            }
            rt_host::ClientEvent::Closed { code, reason } => {
                ws_client::Event::Closed(ws_client::CloseFrame { code, reason })
            }
        }))
    }

    async fn close(&mut self, this: Resource<WsConnection>, code: u16, reason: String) -> Result<(), ws_client::Error> {
        let c = self.connection(&this)?;
        self.clock
            .io(async move { c.close(code, &reason).await })
            .await
            .map_err(cerr)
    }

    async fn drop(&mut self, this: Resource<WsConnection>) -> wasmtime::Result<()> {
        self.table.delete(this)?;
        Ok(())
    }
}
