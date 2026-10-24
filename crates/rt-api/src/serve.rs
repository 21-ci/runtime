//! Serving helpers for the four listeners: `public`, `internal` (Unix socket or TCP),
//! `control`, `metrics`.
//!
//! The accept loop is hand-rolled on hyper so the DoS limits of SECURITY.md hold: request head ≤ 16 KB
//! and ≤ 100 headers (`431`), header read timeout, per-IP connection cap, HTTP/2 stream cap with
//! the rapid-reset guard (`GOAWAY ENHANCE_YOUR_CALM`), graceful shutdown.

use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::connect_info::ConnectInfo;
use axum::Router;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use parking_lot::Mutex;
use rt_obs::Metrics;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
use tower::ServiceExt;

/// Per-listener HTTP limits.
#[derive(Clone)]
pub struct ServeLimits {
    /// `ingress.max_conns_per_ip` (0 = unlimited). A connection over the cap is closed at
    /// accept and counted in `rt_ingress_conn_rejected_total`.
    pub max_conns_per_ip: usize,
    /// Peers exempt from `max_conns_per_ip` (`ingress.trusted_proxies`): every client behind a
    /// proxy shares the proxy's address.
    pub uncapped: rt_sec::TrustedProxies,
    /// Where rejected connections are counted.
    pub metrics: Option<Arc<Metrics>>,
    /// `ingress.backlog`: the `listen(2)` backlog to bind with ([`bind_tcp_listener`]).
    pub backlog: u32,
    /// `ingress.header_timeout`.
    pub header_timeout: Duration,
    /// `ingress.idle_timeout` (HTTP/2 keep-alive interval).
    pub idle_timeout: Duration,
    /// `ingress.h2_max_streams`.
    pub h2_max_streams: u32,
    /// Largest request head.
    pub max_header_bytes: usize,
    pub max_headers: usize,
    /// How long shutdown waits for open connections before closing them.
    pub grace: Duration,
}

impl std::fmt::Debug for ServeLimits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServeLimits")
            .field("max_conns_per_ip", &self.max_conns_per_ip)
            .field("uncapped", &self.uncapped)
            .field("backlog", &self.backlog)
            .field("header_timeout", &self.header_timeout)
            .field("idle_timeout", &self.idle_timeout)
            .field("h2_max_streams", &self.h2_max_streams)
            .field("max_header_bytes", &self.max_header_bytes)
            .field("max_headers", &self.max_headers)
            .field("grace", &self.grace)
            .finish_non_exhaustive()
    }
}

impl Default for ServeLimits {
    fn default() -> Self {
        Self {
            max_conns_per_ip: 256,
            uncapped: rt_sec::TrustedProxies::default(),
            metrics: None,
            backlog: 4096,
            header_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            h2_max_streams: 100,
            max_header_bytes: 16 * 1024,
            max_headers: 100,
            grace: Duration::from_secs(30),
        }
    }
}

struct ConnCounter {
    map: Mutex<HashMap<IpAddr, usize>>,
}

struct ConnGuard {
    counter: Arc<ConnCounter>,
    ip: IpAddr,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut m = self.counter.map.lock();
        if let Some(n) = m.get_mut(&self.ip) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                m.remove(&self.ip);
            }
        }
    }
}

/// The per-IP connection slot of the connection a request arrived on (a request extension). A
/// WebSocket keeps it after the upgrade, so an upgraded connection still counts against
/// `ingress.max_conns_per_ip` until the socket closes.
#[derive(Clone)]
pub struct ConnHold(#[allow(dead_code)] Arc<ConnGuard>);

impl ConnCounter {
    fn acquire(self: &Arc<Self>, ip: IpAddr, max: usize) -> Option<ConnGuard> {
        let mut m = self.map.lock();
        let n = m.entry(ip).or_insert(0);
        if max > 0 && *n >= max {
            return None;
        }
        *n += 1;
        Some(ConnGuard {
            counter: self.clone(),
            ip,
        })
    }
}

fn builder(limits: &ServeLimits) -> AutoBuilder<TokioExecutor> {
    let mut b = AutoBuilder::new(TokioExecutor::new());
    b.http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_timeout)
        .max_buf_size(limits.max_header_bytes)
        .max_headers(limits.max_headers)
        .keep_alive(true);
    b.http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(limits.h2_max_streams)
        .keep_alive_interval(limits.idle_timeout)
        .keep_alive_timeout(limits.idle_timeout)
        .max_pending_accept_reset_streams(Some(20))
        .max_local_error_reset_streams(Some(1024));
    b
}

fn serve_one<I>(
    b: &AutoBuilder<TokioExecutor>,
    graceful: &GracefulShutdown,
    io: I,
    router: Router,
    peer: Option<SocketAddr>,
    guard: Option<ConnGuard>,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let hold = guard.map(|g| ConnHold(Arc::new(g)));
    let svc = TowerToHyperService::new(router.map_request(move |mut req: axum::http::Request<Incoming>| {
        if let Some(addr) = peer {
            req.extensions_mut().insert(ConnectInfo(addr));
        }
        if let Some(h) = &hold {
            req.extensions_mut().insert(h.clone());
        }
        req.map(axum::body::Body::new)
    }));
    let conn = b
        .clone()
        .serve_connection_with_upgrades(TokioIo::new(io), svc)
        .into_owned();
    let watched = graceful.watch(conn);
    tokio::spawn(async move {
        let _ = watched.await;
    });
}

/// Bind a TCP listener on `addr` (`host:port`; a name is resolved and each address tried in
/// turn) with an explicit `listen(2)` backlog. `TcpListener::bind` uses 128 (mio), which resets
/// connection bursts; the kernel still caps `backlog` at `kern.ipc.somaxconn` (macOS) or
/// `net.core.somaxconn` (Linux).
pub async fn bind_tcp_listener(addr: &str, backlog: u32) -> std::io::Result<TcpListener> {
    let mut last = None;
    for a in tokio::net::lookup_host(addr).await? {
        let socket = match a {
            SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        // As `TcpListener::bind`: a restarted node can rebind while old connections linger.
        socket.set_reuseaddr(true)?;
        match socket.bind(a).and_then(|()| socket.listen(backlog)) {
            Ok(listener) => return Ok(listener),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{addr}: no address"))))
}

/// Serve `router` on a bound TCP listener until `shutdown` resolves (graceful). Connect info
/// (`SocketAddr`) is available to handlers (client IP for `ip_allow`, per-IP limits).
pub async fn serve_tcp(
    listener: TcpListener,
    router: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    serve_tcp_with(listener, router, ServeLimits::default(), shutdown).await
}

/// [`serve_tcp`] with explicit limits.
pub async fn serve_tcp_with(
    listener: TcpListener,
    router: Router,
    limits: ServeLimits,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let counter = Arc::new(ConnCounter {
        map: Mutex::new(HashMap::new()),
    });
    let graceful = GracefulShutdown::new();
    let b = builder(&limits);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let max = if limits.uncapped.is_trusted(peer.ip()) { 0 } else { limits.max_conns_per_ip };
                let Some(guard) = counter.acquire(peer.ip(), max) else {
                    // Beyond the per-IP cap: count, then close immediately.
                    if let Some(m) = &limits.metrics {
                        m.ingress_conn_rejected_total.inc();
                    }
                    drop(stream);
                    continue;
                };
                let _ = stream.set_nodelay(true);
                serve_one(&b, &graceful, stream, router.clone(), Some(peer), Some(guard));
            }
            _ = &mut shutdown => break,
        }
    }
    drop(listener);
    tokio::select! {
        _ = graceful.shutdown() => {}
        _ = tokio::time::sleep(limits.grace) => {}
    }
    Ok(())
}

/// Serve `router` on a Unix socket (the trusted internal listener). Each connection's peer
/// credentials (`SO_PEERCRED` on Linux, `getpeereid` on macOS) are checked against `peers`
/// (`listeners.internal_allowed_uids`, SEC-41) before anything is read from it; other peers are
/// disconnected without a response.
pub async fn serve_unix(
    listener: UnixListener,
    router: Router,
    peers: rt_sec::UnixPeerPolicy,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let limits = ServeLimits {
        max_conns_per_ip: 0,
        ..ServeLimits::default()
    };
    let graceful = GracefulShutdown::new();
    let b = builder(&limits);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                match stream.peer_cred() {
                    Ok(cred) if peers.allows(cred.uid()) => {}
                    Ok(cred) => {
                        tracing::warn!(uid = cred.uid(), "internal socket: peer UID not in listeners.internal_allowed_uids; disconnected");
                        drop(stream);
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "internal socket: peer credentials unavailable; disconnected");
                        drop(stream);
                        continue;
                    }
                }
                serve_one(&b, &graceful, stream, router.clone(), None, None);
            }
            _ = &mut shutdown => break,
        }
    }
    drop(listener);
    tokio::select! {
        _ = graceful.shutdown() => {}
        _ = tokio::time::sleep(limits.grace) => {}
    }
    Ok(())
}

/// `host:port` or `unix:/path` (docs/config.md `[listeners]`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListenAddr {
    Tcp(String),
    Unix(std::path::PathBuf),
}

impl ListenAddr {
    pub fn parse(s: &str) -> Self {
        match s.strip_prefix("unix:") {
            Some(p) => ListenAddr::Unix(p.into()),
            None => ListenAddr::Tcp(s.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use rt_obs::{Metrics, MetricsOptions};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// One HTTP/1.1 request on `s`; the status line of the answer, or `None` if the server closed
    /// the connection without one.
    async fn status_line(s: &mut TcpStream) -> Option<String> {
        s.write_all(b"GET / HTTP/1.1\r\nHost: t\r\n\r\n").await.ok()?;
        let mut buf = vec![0u8; 512];
        let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
            .await
            .ok()?
            .ok()?;
        let text = String::from_utf8_lossy(&buf[..n]).to_string();
        text.lines().next().filter(|l| !l.is_empty()).map(str::to_string)
    }

    async fn serve(limits: ServeLimits) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
        let listener = bind_tcp_listener("127.0.0.1:0", 64).await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let router = Router::new().route("/", get(|| async { "ok" }));
        tokio::spawn(serve_tcp_with(listener, router, limits, async move {
            let _ = rx.await;
        }));
        (addr, tx)
    }

    #[tokio::test]
    async fn a_listener_binds_a_host_name_with_the_requested_backlog() {
        let listener = bind_tcp_listener("localhost:0", 4096).await.expect("bind localhost");
        let addr = listener.local_addr().expect("addr");
        assert!(addr.ip().is_loopback(), "{addr}");
        let accepted = tokio::spawn(async move { listener.accept().await.map(|(_, peer)| peer) });
        TcpStream::connect(addr).await.expect("connect");
        assert!(accepted.await.expect("join").is_ok());
        assert!(bind_tcp_listener("not a host:1", 16).await.is_err());
    }

    #[tokio::test]
    async fn connections_over_the_per_ip_cap_are_closed_and_counted() {
        let metrics = Metrics::new(MetricsOptions::default()).expect("metrics");
        let limits = ServeLimits {
            max_conns_per_ip: 1,
            metrics: Some(metrics.clone()),
            ..ServeLimits::default()
        };
        let (addr, _stop) = serve(limits).await;
        let mut first = TcpStream::connect(addr).await.expect("connect");
        assert_eq!(status_line(&mut first).await.as_deref(), Some("HTTP/1.1 200 OK"));
        let mut second = TcpStream::connect(addr).await.expect("connect");
        assert_eq!(
            status_line(&mut second).await,
            None,
            "the second connection from the same IP is closed"
        );
        assert_eq!(metrics.ingress_conn_rejected_total.get(), 1);
        drop(first);
    }

    /// SEC-41: the internal Unix socket serves the node's own UID and closes other peers.
    #[tokio::test]
    async fn ac_sec_41_the_unix_listener_checks_the_peer_uid_before_serving() {
        async fn status_over_unix(policy: rt_sec::UnixPeerPolicy) -> Option<String> {
            let dir = tempfile::tempdir().expect("tmp");
            let path = dir.path().join("rt.sock");
            let listener = UnixListener::bind(&path).expect("bind");
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let router = Router::new().route("/", get(|| async { "ok" }));
            let server = tokio::spawn(serve_unix(listener, router, policy, async move {
                let _ = rx.await;
            }));
            let mut s = tokio::net::UnixStream::connect(&path).await.expect("connect");
            let _ = s.write_all(b"GET / HTTP/1.1\r\nHost: t\r\n\r\n").await;
            let mut buf = vec![0u8; 512];
            let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or(0);
            let _ = tx.send(());
            let _ = server.await;
            String::from_utf8_lossy(&buf[..n])
                .lines()
                .next()
                .filter(|l| !l.is_empty())
                .map(str::to_string)
        }
        let own = rt_sec::UnixPeerPolicy::own_uid();
        assert_eq!(
            status_over_unix(rt_sec::UnixPeerPolicy::node_default(&[]))
                .await
                .as_deref(),
            Some("HTTP/1.1 200 OK")
        );
        assert_eq!(
            status_over_unix(rt_sec::UnixPeerPolicy::exact(&[own.wrapping_add(1)])).await,
            None,
            "a peer whose UID is not allowed is disconnected without a response"
        );
    }

    #[tokio::test]
    async fn a_trusted_proxy_is_exempt_from_the_per_ip_cap() {
        let metrics = Metrics::new(MetricsOptions::default()).expect("metrics");
        let limits = ServeLimits {
            max_conns_per_ip: 1,
            uncapped: rt_sec::TrustedProxies::parse(&["127.0.0.1"]).expect("cidr"),
            metrics: Some(metrics.clone()),
            ..ServeLimits::default()
        };
        let (addr, _stop) = serve(limits).await;
        let mut conns = Vec::new();
        for _ in 0..3 {
            let mut c = TcpStream::connect(addr).await.expect("connect");
            assert_eq!(status_line(&mut c).await.as_deref(), Some("HTTP/1.1 200 OK"));
            conns.push(c);
        }
        assert_eq!(metrics.ingress_conn_rejected_total.get(), 0);
    }
}
