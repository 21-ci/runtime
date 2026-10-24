//! Outbound WebSockets of guests: `new WebSocket(url)` in Deno and the
//! WIT `ws-client` interface. The connection is opened by the egress guard ([`EgressGuard::dial`]:
//! scheme, port and host rules, one DNS lookup, every address public, TLS for `wss:`), counts as
//! one of the task's fetches, and its received bytes count against the daily egress quota. It
//! lives as long as the task holds it: a task that ends drops it.
//!
//! [`EgressGuard::dial`]: crate::egress::EgressGuard::dial

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use rt_types::WsData;
use tokio::sync::{Mutex, OwnedSemaphorePermit};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::egress::{EgressIo, EgressUsage};
use crate::facade::HostError;

/// Largest message an outbound socket accepts from its server.
pub const MAX_CLIENT_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

type Ws = WebSocketStream<Box<dyn EgressIo>>;

/// What an outbound socket received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientEvent {
    Message(WsData),
    /// The connection ended (`1006` without a close frame).
    Closed {
        code: u16,
        reason: String,
    },
}

/// One outbound WebSocket of a task.
pub struct WsClient {
    sink: Mutex<SplitSink<Ws, Message>>,
    stream: Mutex<SplitStream<Ws>>,
    protocol: Option<String>,
    usage: Arc<EgressUsage>,
    closed: AtomicBool,
    /// One of the task's `max_concurrent_fetches` permits while the socket is open (security
    /// review finding 5): released on close, end of stream, or drop.
    slot: std::sync::Mutex<Option<OwnedSemaphorePermit>>,
}

fn err(code: &'static str, message: &str) -> HostError {
    HostError::new(code, message)
}

impl WsClient {
    /// The client handshake over a connection the egress guard opened.
    /// `slot` is the task's `max_concurrent_fetches` permit, held while the socket is open.
    pub(crate) async fn handshake(
        io: Box<dyn EgressIo>,
        url: &url::Url,
        protocols: &[String],
        usage: Arc<EgressUsage>,
        slot: Option<OwnedSemaphorePermit>,
    ) -> Result<Self, HostError> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut req = url
            .as_str()
            .into_client_request()
            .map_err(|_| err("validation_failed", "not a WebSocket URL"))?;
        if !protocols.is_empty() {
            let v = http::HeaderValue::from_str(&protocols.join(", "))
                .map_err(|_| err("validation_failed", "invalid subprotocol"))?;
            req.headers_mut().insert(http::header::SEC_WEBSOCKET_PROTOCOL, v);
        }
        let cfg = WebSocketConfig::default()
            .read_buffer_size(4096)
            .max_message_size(Some(MAX_CLIENT_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_CLIENT_MESSAGE_BYTES));
        let (ws, resp) = tokio_tungstenite::client_async_with_config(req, io, Some(cfg))
            .await
            .map_err(|_| err("egress_upstream", "the WebSocket handshake failed"))?;
        let protocol = resp
            .headers()
            .get(http::header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let (sink, stream) = ws.split();
        Ok(Self {
            sink: Mutex::new(sink),
            stream: Mutex::new(stream),
            protocol,
            usage,
            closed: AtomicBool::new(false),
            slot: std::sync::Mutex::new(slot),
        })
    }

    /// Mark the socket closed and give its `max_concurrent_fetches` slot back.
    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(mut s) = self.slot.lock() {
            s.take();
        }
    }

    /// The subprotocol the server chose.
    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    pub async fn send(&self, data: WsData) -> Result<(), HostError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(err("invalid_state", "the WebSocket is closed"));
        }
        let msg = match data {
            WsData::Text(t) => Message::text(t),
            WsData::Binary(b) => Message::binary(b),
        };
        self.sink
            .lock()
            .await
            .send(msg)
            .await
            .map_err(|_| err("egress_upstream", "the WebSocket connection failed"))
    }

    /// The next message, or the end of the connection.
    pub async fn recv(&self) -> Result<ClientEvent, HostError> {
        let mut stream = self.stream.lock().await;
        loop {
            match stream.next().await {
                Some(Ok(Message::Text(t))) => {
                    self.charge(t.len())?;
                    return Ok(ClientEvent::Message(WsData::Text(t.as_str().to_string())));
                }
                Some(Ok(Message::Binary(b))) => {
                    self.charge(b.len())?;
                    return Ok(ClientEvent::Message(WsData::Binary(b)));
                }
                Some(Ok(Message::Close(frame))) => {
                    self.mark_closed();
                    let (code, reason) = frame.map_or((1005, String::new()), |f| {
                        (u16::from(f.code), f.reason.as_str().to_string())
                    });
                    return Ok(ClientEvent::Closed { code, reason });
                }
                Some(Ok(_)) => continue,
                Some(Err(_)) | None => {
                    self.mark_closed();
                    return Ok(ClientEvent::Closed {
                        code: 1006,
                        reason: String::new(),
                    });
                }
            }
        }
    }

    /// Start the closing handshake (code 1000 or 3000-4999).
    pub async fn close(&self, code: u16, reason: &str) -> Result<(), HostError> {
        if !rt_types::valid_app_close_code(code) || reason.len() > rt_types::MAX_CLOSE_REASON_BYTES {
            return Err(err(
                "invalid_request",
                "a close code is 1000 or 3000-4999, a reason at most 123 bytes",
            ));
        }
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let frame = CloseFrame {
            code: CloseCode::from(code),
            reason: reason.to_string().into(),
        };
        let _ = self.sink.lock().await.send(Message::Close(Some(frame))).await;
        self.mark_closed();
        Ok(())
    }

    fn charge(&self, n: usize) -> Result<(), HostError> {
        if self.usage.consume(n as u64) {
            Ok(())
        } else {
            self.mark_closed();
            Err(err("egress_quota", "the daily egress quota is exhausted"))
        }
    }
}
