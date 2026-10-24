//! One socket's task: reads frames into its object's inbox, writes what guests send, answers
//! pings, enforces the message cap (1009), the rate limit (1008) and the write timeout.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rt_types::{SocketEventKind, WsData};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

use super::hub::{Entry, Out, SocketHub};

/// How long a closing socket waits for the peer's close frame.
const CLOSE_WAIT: Duration = Duration::from_secs(2);

/// Token bucket of one socket's incoming messages.
struct Bucket {
    tokens: f64,
    rate: f64,
    burst: f64,
    at: Instant,
}

impl Bucket {
    fn new(rate: u32, burst: u32) -> Self {
        let burst = f64::from(burst.max(1));
        Self {
            tokens: burst,
            rate: f64::from(rate.max(1)),
            burst,
            at: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        self.tokens = (self.tokens + now.duration_since(self.at).as_secs_f64() * self.rate).min(self.burst);
        self.at = now;
    }

    /// Take a token; `Err(wait)` until the next one when empty.
    fn take(&mut self) -> Result<(), Duration> {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - self.tokens) / self.rate))
        }
    }
}

fn close_msg(code: u16, reason: &str) -> Message {
    Message::Close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: reason.to_string().into(),
    }))
}

fn gone(code: u16, reason: &str) -> Option<SocketEventKind> {
    Some(SocketEventKind::Close {
        code,
        reason: reason.to_string(),
        was_clean: false,
    })
}

async fn drain_close<S>(ws: &mut WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _ = timeout(CLOSE_WAIT, async {
        while let Some(r) = ws.next().await {
            if r.is_err() {
                break;
            }
        }
    })
    .await;
}

pub(super) async fn run<S>(
    hub: Arc<SocketHub>,
    entry: Arc<Entry>,
    io: S,
    mut rx: mpsc::UnboundedReceiver<Out>,
    max_message: usize,
    hold: Box<dyn std::any::Any + Send>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let _hold = hold;
    let cfg = hub.config().clone();
    let ws_cfg = WebSocketConfig::default()
        .read_buffer_size(cfg.read_buffer_bytes)
        .write_buffer_size(0)
        .max_message_size(Some(max_message.max(1)))
        .max_frame_size(Some(max_message.max(1)));
    let mut ws = WebSocketStream::from_raw_socket(io, Role::Server, Some(ws_cfg)).await;
    let mut ping = tokio::time::interval_at(Instant::now() + cfg.ping_interval, cfg.ping_interval);
    let mut last_seen = Instant::now();
    let mut bucket = Bucket::new(cfg.msgs_per_sec, cfg.msgs_burst);
    let mut throttled_since: Option<Instant> = None;

    // The close code, for the metrics (also when the guest closed and no event follows).
    let mut code = 1006u16;
    let close: Option<SocketEventKind> = loop {
        tokio::select! {
            biased;
            out = rx.recv() => match out {
                Some(Out::Data(d)) => {
                    let len = d.len() as u64;
                    let msg = match d {
                        WsData::Text(t) => Message::text(t),
                        WsData::Binary(b) => Message::binary(b),
                    };
                    let written = timeout(cfg.write_timeout, ws.send(msg)).await;
                    entry.buffered.fetch_sub(len, Ordering::AcqRel);
                    hub.count_message("out");
                    if !matches!(written, Ok(Ok(()))) {
                        break gone(1006, "write failed");
                    }
                }
                Some(Out::Close { code: c, reason, by_guest }) => {
                    code = c;
                    let _ = timeout(cfg.write_timeout, ws.send(close_msg(c, &reason))).await;
                    drain_close(&mut ws).await;
                    break (!by_guest).then_some(SocketEventKind::Close { code: c, reason, was_clean: true });
                }
                None => break None,
            },
            msg = ws.next() => {
                let data = match msg {
                    Some(Ok(Message::Text(t))) => WsData::Text(t.as_str().to_string()),
                    Some(Ok(Message::Binary(b))) => WsData::Binary(b),
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                        last_seen = Instant::now();
                        continue;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let (c, reason) = frame.map_or((1005, String::new()), |f| (u16::from(f.code), f.reason.as_str().to_string()));
                        code = c;
                        // tungstenite queued the close reply: write it, then the peer hangs up.
                        let _ = timeout(cfg.write_timeout, ws.flush()).await;
                        drain_close(&mut ws).await;
                        break Some(SocketEventKind::Close { code: c, reason, was_clean: true });
                    }
                    Some(Err(WsError::Capacity(_))) => {
                        code = 1009;
                        let _ = timeout(cfg.write_timeout, ws.send(close_msg(1009, "message too big"))).await;
                        break gone(1009, "message too big");
                    }
                    Some(Err(_)) | None => break gone(1006, ""),
                };
                let now = Instant::now();
                last_seen = now;
                hub.count_message("in");
                // Rate limit: a client over its rate is read more slowly (TCP backpressure); one
                // that keeps it up is closed.
                if let Err(wait) = bucket.take() {
                    let since = *throttled_since.get_or_insert(now);
                    if now.duration_since(since) >= cfg.flood_close_after {
                        code = 1008;
                        let _ = timeout(cfg.write_timeout, ws.send(close_msg(1008, "rate limit"))).await;
                        break gone(1008, "rate limit");
                    }
                    tokio::time::sleep(wait).await;
                    let _ = bucket.take();
                } else {
                    throttled_since = None;
                }
                entry.meta.lock().last_message_at = Some(rt_host::handle::now_ms());
                if let WsData::Text(t) = &data {
                    if let Some(answer) = entry.slot.auto_response(t) {
                        hub.mark_auto_response(&entry);
                        if !matches!(timeout(cfg.write_timeout, ws.send(Message::text(answer))).await, Ok(Ok(()))) {
                            break gone(1006, "write failed");
                        }
                        continue;
                    }
                }
                hub.socket_event(&entry, SocketEventKind::Message(data)).await;
            },
            _ = ping.tick() => {
                if last_seen.elapsed() > cfg.ping_interval * 2 {
                    break gone(1006, "ping timeout");
                }
                if !matches!(timeout(cfg.write_timeout, ws.send(Message::Ping(Default::default()))).await, Ok(Ok(()))) {
                    break gone(1006, "write failed");
                }
            }
        }
    };
    hub.socket_ended(&entry, close, code);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use rt_host::{AcceptOpts, SocketHost, SocketOp, TaskCtx};
    use rt_types::{ObjectRef, RejectReason, ServiceId, SocketEvent, TaskId, TaskSpec, TenantId, WsData};
    use tokio::io::{AsyncWriteExt, DuplexStream, ReadBuf};

    /// A connection whose reads never produce anything and whose writes always fail: the peer is
    /// gone, and the first write tells the socket so.
    struct DeadWrites;

    impl tokio::io::AsyncRead for DeadWrites {
        fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl tokio::io::AsyncWrite for DeadWrites {
        fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "the peer is gone")))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Accept a socket of object `o` and bind it to a connection that cannot be written to.
    async fn connect_dead(h: &Arc<SocketHub>) -> String {
        let t = h.accept(&ctx(), "o", AcceptOpts::default()).expect("accept");
        let claim = h.claim(&t.ticket, TenantId::from_u128(1)).expect("claim");
        h.bind_io(claim, DeadWrites, None, Box::new(()));
        t.socket
    }
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    use super::super::hub::{HubConfig, SocketHub, SocketLimits, SocketTaskFactory, Submitter};
    use super::{Bucket, Role};

    struct Caps(SocketLimits);

    impl SocketTaskFactory for Caps {
        fn event_task(&self, _: ServiceId, _: ObjectRef, _: SocketEvent) -> Option<TaskSpec> {
            None
        }
        fn limits(&self, _: TenantId, _: ServiceId) -> SocketLimits {
            self.0
        }
    }

    fn hub(cfg: HubConfig, limits: SocketLimits) -> Arc<SocketHub> {
        let submit: Submitter = Arc::new(|_| Err(RejectReason::Draining));
        SocketHub::new(cfg, submit, Arc::new(Caps(limits)), tokio::runtime::Handle::current())
    }

    fn cfg() -> HubConfig {
        HubConfig {
            max_sockets: 100,
            ..HubConfig::default()
        }
    }

    fn ctx() -> TaskCtx {
        TaskCtx {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(1),
            task: TaskId(1),
            depth: 0,
            deadline: std::time::Instant::now() + Duration::from_secs(30),
            trace: None,
            on_behalf_of: None,
            object: None,
        }
    }

    /// Accept a socket of object `o` and connect a client to it over an in-memory pipe.
    async fn connect(h: &Arc<SocketHub>) -> (String, WebSocketStream<DuplexStream>) {
        connect_in(h, "o").await
    }

    /// Accept a socket of `object` and connect a client to it (the upgrade is done).
    async fn connect_in(h: &Arc<SocketHub>, object: &str) -> (String, WebSocketStream<DuplexStream>) {
        let t = h.accept(&ctx(), object, AcceptOpts::default()).expect("accept");
        let claim = h.claim(&t.ticket, TenantId::from_u128(1)).expect("claim");
        let (server, client) = tokio::io::duplex(1 << 16);
        h.bind_io(claim, server, None, Box::new(()));
        (
            t.socket,
            WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        )
    }

    /// The close code the server sent (`None` when it ended without one).
    async fn close_code(client: &mut WebSocketStream<DuplexStream>) -> Option<u16> {
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(msg) = client.next().await {
                if let Ok(Message::Close(frame)) = msg {
                    return frame.map(|f| u16::from(f.code));
                }
            }
            None
        })
        .await
        .expect("the server closes the socket in time")
    }

    #[test]
    fn an_empty_bucket_says_how_long_until_its_next_token() {
        let mut b = Bucket::new(2, 1);
        assert!(b.take().is_ok(), "the burst token is there");
        let wait = b.take().expect_err("the bucket is empty");
        assert!(
            wait > Duration::from_millis(400) && wait <= Duration::from_millis(500),
            "one token at 2/s: {wait:?}"
        );
    }

    #[tokio::test]
    async fn a_message_over_the_cap_closes_the_socket_with_1009() {
        let limits = SocketLimits {
            max_sockets: 0,
            max_per_object: 0,
            max_per_service: 0,
            max_message_bytes: 16,
        };
        let h = hub(cfg(), limits);
        let (_socket, mut client) = connect(&h).await;
        client.send(Message::text("x".repeat(64))).await.expect("send");
        assert_eq!(close_code(&mut client).await, Some(1009));
    }

    #[tokio::test]
    async fn a_client_over_its_rate_with_no_grace_is_closed_with_1008() {
        let cfg = HubConfig {
            msgs_per_sec: 1,
            msgs_burst: 1,
            flood_close_after: Duration::ZERO,
            ..cfg()
        };
        let h = hub(cfg, SocketLimits::default());
        let (_socket, mut client) = connect(&h).await;
        client.send(Message::text("first")).await.expect("send");
        client.send(Message::text("second")).await.expect("send");
        assert_eq!(close_code(&mut client).await, Some(1008));
    }

    /// A client briefly over its rate waits for a token; the socket is slowed, not closed.
    #[tokio::test]
    async fn a_client_briefly_over_its_rate_is_slowed_not_closed() {
        let cfg = HubConfig {
            msgs_per_sec: 10,
            msgs_burst: 1,
            flood_close_after: Duration::from_secs(10),
            ..cfg()
        };
        let h = hub(cfg, SocketLimits::default());
        let (socket, mut client) = connect(&h).await;
        client.send(Message::text("one")).await.expect("send");
        client.send(Message::text("two")).await.expect("send");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(h.get(&ctx(), &socket).is_some(), "the throttled client stays connected");
    }

    #[tokio::test]
    async fn a_client_that_never_answers_pings_is_dropped_after_two_intervals() {
        let cfg = HubConfig {
            ping_interval: Duration::from_millis(50),
            ..cfg()
        };
        let h = hub(cfg, SocketLimits::default());
        let (socket, _client) = connect(&h).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(h.get(&ctx(), &socket).is_none(), "the silent socket is gone");
    }

    #[tokio::test]
    async fn pings_from_the_client_keep_a_quiet_socket_alive() {
        let cfg = HubConfig {
            ping_interval: Duration::from_millis(200),
            ..cfg()
        };
        let h = hub(cfg, SocketLimits::default());
        let (socket, mut client) = connect(&h).await;
        for _ in 0..50 {
            client.send(Message::Ping(Default::default())).await.expect("ping");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(h.get(&ctx(), &socket).is_some(), "the client's pings reset the timeout");
    }

    #[tokio::test]
    async fn an_open_socket_attachment_is_its_services_to_read_and_change() {
        let h = hub(cfg(), SocketLimits::default());
        let (socket, _client) = connect(&h).await;
        let mine = ctx();
        let other = TaskCtx {
            service: ServiceId::from_u128(2),
            ..ctx()
        };
        h.set_attachment(&mine, &socket, Some(bytes::Bytes::from_static(b"state")))
            .expect("attachment");
        assert_eq!(
            h.attachment(&mine, &socket).expect("read"),
            Some(bytes::Bytes::from_static(b"state"))
        );
        h.set_attachment(&mine, &socket, None).expect("clear");
        assert_eq!(h.attachment(&mine, &socket).expect("read"), None);
        assert_eq!(
            h.attachment(&other, &socket).map_err(|e| e.code),
            Err("not_found"),
            "another service"
        );
        assert_eq!(
            h.set_attachment(&other, &socket, None).map_err(|e| e.code),
            Err("not_found")
        );
        assert_eq!(
            h.set_tags(&other, &socket, vec![]).map_err(|e| e.code),
            Err("not_found")
        );
        let big = bytes::Bytes::from(vec![0u8; rt_types::MAX_ATTACHMENT_BYTES + 1]);
        assert_eq!(
            h.set_attachment(&mine, &socket, Some(big)).map_err(|e| e.code),
            Err("too_large")
        );
    }

    #[tokio::test]
    async fn object_pages_list_the_objects_with_open_sockets_in_name_order() {
        let h = hub(cfg(), SocketLimits::default());
        let _a1 = connect_in(&h, "a").await;
        let _a2 = connect_in(&h, "a").await;
        let _b = connect_in(&h, "b").await;
        assert_eq!(h.count(&ctx(), Some("a"), None), 2);
        let s = ServiceId::from_u128(1);
        let (first, cursor) = h.objects_of(s, "", 1, None);
        assert_eq!(first, vec![("a".to_string(), 2)]);
        let (second, cursor) = h.objects_of(s, "", 1, cursor.as_deref());
        assert_eq!(second, vec![("b".to_string(), 1)]);
        assert!(cursor.is_none(), "the last page has no cursor");
    }

    /// A message that cannot be written closes the socket: the peer is gone, so it is forgotten.
    #[tokio::test]
    async fn a_failed_write_closes_the_socket_and_forgets_it() {
        let h = hub(cfg(), SocketLimits::default());
        let socket = connect_dead(&h).await;
        assert!(h.get(&ctx(), &socket).is_some(), "connected");
        h.apply(
            &ctx(),
            SocketOp::Send {
                socket: socket.clone(),
                data: WsData::Text("hi".into()),
            },
        )
        .expect("queued");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(h.get(&ctx(), &socket).is_none(), "the write failed: the socket is gone");
    }

    /// A ping that cannot be written ends the socket as well.
    #[tokio::test]
    async fn a_ping_that_cannot_be_written_closes_the_socket() {
        let cfg = HubConfig {
            ping_interval: Duration::from_millis(20),
            ..cfg()
        };
        let h = hub(cfg, SocketLimits::default());
        let socket = connect_dead(&h).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            h.get(&ctx(), &socket).is_none(),
            "the ping write failed: the socket is gone"
        );
    }

    /// An automatic answer that cannot be written ends the socket. The client's frame ("ping",
    /// masked as a client must send it) is already in the pipe when the client goes away.
    #[tokio::test]
    async fn an_auto_answer_that_cannot_be_written_closes_the_socket() {
        let h = hub(cfg(), SocketLimits::default());
        let t = h.accept(&ctx(), "o", AcceptOpts::default()).expect("accept");
        h.set_auto_response(&ctx(), "o", Some(("ping".into(), "pong".into())))
            .expect("auto response");
        let claim = h.claim(&t.ticket, TenantId::from_u128(1)).expect("claim");
        let (server, mut client) = tokio::io::duplex(1 << 16);
        // FIN + text, masked, length 4, mask key 1..4, then "ping" XOR the mask.
        let frame = [0x81, 0x84, 1, 2, 3, 4, b'p' ^ 1, b'i' ^ 2, b'n' ^ 3, b'g' ^ 4];
        client.write_all(&frame).await.expect("frame");
        drop(client);
        h.bind_io(claim, server, None, Box::new(()));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            h.get(&ctx(), &t.socket).is_none(),
            "the answer could not be written: the socket is gone"
        );
    }
}
