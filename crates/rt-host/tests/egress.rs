//! Egress guard tests (SECURITY.md): SEC-10 (SSRF corpus), SEC-11 (ports), SEC-12
//! (`allowed_hosts`), SEC-24 (self-denylist), SEC-30 (decompressed-size cap) plus the response-size
//! cap and the redirect limit.
//!
//! Everything runs against **local mock servers** and a **mock resolver**:
//!
//! - the resolver maps test names onto "public" documentation-range addresses
//!   (`203.0.113.0/24`, `198.51.100.0/24`: not blocked by `rt_sec::is_blocked_ip`) or onto private
//!   ones, and records how often it is asked (DNS rebinding);
//! - `GuardedEgress::with_connect_rewrite` (test seam) then dials `127.0.0.1` instead of the
//!   validated "public" address, so the real guard pipeline (scheme/port/host/IP checks, pinning,
//!   redirects, caps) runs end to end without ever whitelisting loopback;
//! - a second mock server listens on **real loopback** and must never see a connection: it is the
//!   target of every SSRF attempt that names `127.0.0.1` in some form.
//!
//! The mock servers are hand-rolled HTTP/1.1 over `tokio::net` so no extra dev-dependency is needed
//! (flate2 only builds gzip bodies).

use std::collections::HashMap;
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use parking_lot::Mutex;
use rt_host::{
    is_redirect, redirect_method, EgressBudget, EgressError, EgressGuard, EgressGuardConfig, EgressPolicy,
    GuardedEgress, Request, Resolver,
};
use rt_sec::{EgressDenylist, SelfDenylist};
use rt_types::{Body, BodyError, Limits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------------------------
// Mock HTTP/1.1 server
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct SeenRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Clone)]
enum Payload {
    /// `Content-Length` body.
    Fixed(Vec<u8>),
    /// Same bytes, `Transfer-Encoding: chunked` (no length known up front).
    Chunked(Vec<u8>),
    /// Chunked, never ends until the client goes away.
    Endless,
}

#[derive(Clone)]
struct MockReply {
    status: u16,
    headers: Vec<(String, String)>,
    payload: Payload,
}

impl MockReply {
    fn ok(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            payload: Payload::Fixed(body.into()),
        }
    }

    fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            payload: Payload::Fixed(Vec::new()),
        }
    }

    fn redirect(status: u16, location: &str) -> Self {
        Self {
            status,
            headers: vec![("Location".into(), location.into())],
            payload: Payload::Fixed(Vec::new()),
        }
    }

    fn chunked(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            payload: Payload::Chunked(body.into()),
        }
    }

    fn endless() -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            payload: Payload::Endless,
        }
    }

    fn gzip(plain: &[u8]) -> Self {
        Self {
            status: 200,
            headers: vec![("Content-Encoding".into(), "gzip".into())],
            payload: Payload::Fixed(gzip_bytes(plain)),
        }
    }
}

type Handler = dyn Fn(&SeenRequest) -> MockReply + Send + Sync;

struct MockServer {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<SeenRequest>>>,
    connections: Arc<AtomicUsize>,
    bytes_written: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

impl MockServer {
    async fn start(handler: impl Fn(&SeenRequest) -> MockReply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server on loopback");
        let addr = listener.local_addr().expect("mock server addr");
        let handler: Arc<Handler> = Arc::new(handler);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let bytes_written = Arc::new(AtomicU64::new(0));
        let task = {
            let (seen, connections, bytes_written) = (seen.clone(), connections.clone(), bytes_written.clone());
            tokio::spawn(async move {
                while let Ok((sock, _)) = listener.accept().await {
                    connections.fetch_add(1, Ordering::SeqCst);
                    let (handler, seen, bytes_written) = (handler.clone(), seen.clone(), bytes_written.clone());
                    tokio::spawn(async move {
                        let _ = serve_connection(sock, handler, seen, bytes_written).await;
                    });
                }
            })
        };
        Self {
            addr,
            seen,
            connections,
            bytes_written,
            task,
        }
    }

    /// A server that answers `200 secret` and exists only to prove nobody connects to it.
    async fn tripwire() -> Self {
        Self::start(|_| MockReply::ok("secret")).await
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Requests that were fully received.
    fn hits(&self) -> usize {
        self.seen.lock().len()
    }

    /// TCP connections accepted (a connect without a request counts).
    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn paths(&self) -> Vec<String> {
        self.seen.lock().iter().map(|r| r.path.clone()).collect()
    }

    fn request(&self, i: usize) -> SeenRequest {
        self.seen
            .lock()
            .get(i)
            .cloned()
            .unwrap_or_else(|| panic!("mock server saw no request #{i}"))
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::SeqCst)
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        404 => "Not Found",
        _ => "Status",
    }
}

async fn serve_connection(
    mut sock: TcpStream,
    handler: Arc<Handler>,
    seen: Arc<Mutex<Vec<SeenRequest>>>,
    bytes_written: Arc<AtomicU64>,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    let head_end = loop {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(p) = find(&buf, b"\r\n\r\n") {
            break p + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let headers: Vec<(String, String)> = lines
        .filter(|l| !l.is_empty())
        .filter_map(|l| {
            l.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect();
    let content_len: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[head_end..].to_vec();
    while body.len() < content_len {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }

    let req = SeenRequest {
        method,
        path,
        headers,
        body,
    };
    seen.lock().push(req.clone());
    let reply = handler(&req);

    let mut out = format!("HTTP/1.1 {} {}\r\n", reply.status, reason(reply.status));
    for (k, v) in &reply.headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("Connection: close\r\n");
    match reply.payload {
        Payload::Fixed(b) => {
            out.push_str(&format!("Content-Length: {}\r\n\r\n", b.len()));
            sock.write_all(out.as_bytes()).await?;
            sock.write_all(&b).await?;
            bytes_written.fetch_add(b.len() as u64, Ordering::SeqCst);
        }
        Payload::Chunked(b) => {
            out.push_str("Transfer-Encoding: chunked\r\n\r\n");
            sock.write_all(out.as_bytes()).await?;
            for chunk in b.chunks(16 * 1024) {
                sock.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await?;
                sock.write_all(chunk).await?;
                sock.write_all(b"\r\n").await?;
                bytes_written.fetch_add(chunk.len() as u64, Ordering::SeqCst);
            }
            sock.write_all(b"0\r\n\r\n").await?;
        }
        Payload::Endless => {
            out.push_str("Transfer-Encoding: chunked\r\n\r\n");
            sock.write_all(out.as_bytes()).await?;
            let chunk = vec![b'x'; 64 * 1024];
            loop {
                // A failing write means the guard hung up: stop producing.
                if sock
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await
                    .is_err()
                    || sock.write_all(&chunk).await.is_err()
                    || sock.write_all(b"\r\n").await.is_err()
                {
                    break;
                }
                bytes_written.fetch_add(chunk.len() as u64, Ordering::SeqCst);
            }
        }
    }
    let _ = sock.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Mock resolver
// ---------------------------------------------------------------------------------------------

fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
}

/// Resolves test names; mimics `getaddrinfo` for the legacy numeric IPv4 forms (decimal, octal,
/// hex, short) so the corpus exercises "what the OS would resolve" rather than literal parsing only.
struct MockResolver {
    table: Mutex<HashMap<String, Vec<IpAddr>>>,
    /// name -> (first answer, every later answer): the classic rebinding setup.
    #[allow(clippy::type_complexity, reason = "test-only pair of answers per name")]
    flip: Mutex<HashMap<String, (Vec<IpAddr>, Vec<IpAddr>)>>,
    calls: Mutex<Vec<String>>,
}

impl MockResolver {
    fn new() -> Arc<Self> {
        let mut t: HashMap<String, Vec<IpAddr>> = HashMap::new();
        let mut put = |name: &str, ips: Vec<IpAddr>| {
            t.insert(name.to_string(), ips);
        };
        // "public" (rewritten onto the mock server by the connect seam)
        put("public.test", vec![v4(203, 0, 113, 10)]);
        put("api.telegram.org", vec![v4(203, 0, 113, 20)]);
        put("sub.api.telegram.org", vec![v4(203, 0, 113, 21)]);
        put("notapi.telegram.org", vec![v4(203, 0, 113, 22)]);
        put("api.telegram.org.evil.test", vec![v4(203, 0, 113, 23)]);
        put("example.com", vec![v4(203, 0, 113, 30)]);
        put("other.test", vec![v4(198, 51, 100, 40)]);
        put("blocked.test", vec![v4(203, 0, 113, 60)]);
        put("a.blocked.test", vec![v4(203, 0, 113, 61)]);
        put("corp.test", vec![v4(203, 0, 113, 62)]);
        // operator-configured endpoints on public addresses (SEC-24)
        put("s3.example.test", vec![v4(198, 51, 100, 7)]);
        put("otlp.example.test", vec![v4(198, 51, 100, 8)]);
        put("api.binding.test", vec![v4(198, 51, 100, 9)]);
        // names that must be refused
        put("internal.test", vec![v4(10, 1, 2, 3)]);
        put("rebind.test", vec![v4(127, 0, 0, 1)]);
        put("mixed.test", vec![v4(203, 0, 113, 50), v4(127, 0, 0, 1)]);
        put("mixed-private-first.test", vec![v4(10, 0, 0, 9), v4(203, 0, 113, 52)]);
        put("v6private.test", vec![IpAddr::V6("fd00::1".parse().expect("v6"))]);
        put("mapped.test", vec![IpAddr::V6("::ffff:10.0.0.1".parse().expect("v6"))]);
        put("metadata.google.internal", vec![v4(169, 254, 169, 254)]);
        put("localhost", vec![v4(127, 0, 0, 1), IpAddr::V6(Ipv6Addr::LOCALHOST)]);
        put("foo.localhost", vec![v4(127, 0, 0, 1)]);
        // dual stack: an IPv6 address nobody routes, then the public IPv4 one (EGR-006)
        put(
            "dual.test",
            vec![IpAddr::V6("2001:db8::1".parse().expect("v6")), v4(203, 0, 113, 70)],
        );
        Arc::new(Self {
            table: Mutex::new(t),
            flip: Mutex::new(HashMap::new()),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn set_flip(&self, name: &str, first: Vec<IpAddr>, later: Vec<IpAddr>) {
        self.flip.lock().insert(name.to_string(), (first, later));
    }

    fn lookups(&self, name: &str) -> usize {
        let name = name.trim_end_matches('.').to_ascii_lowercase();
        self.calls.lock().iter().filter(|c| **c == name).count()
    }
}

fn parse_c_int(s: &str) -> Option<u64> {
    if let Some(h) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(h, 16).ok()
    } else if s.len() > 1 && s.starts_with('0') {
        u64::from_str_radix(&s[1..], 8).ok()
    } else {
        s.parse().ok()
    }
}

/// `inet_aton`: 1-4 numeric parts (decimal / 0octal / 0xhex); the last part fills the rest.
fn legacy_ipv4(host: &str) -> Option<Ipv4Addr> {
    let parts: Vec<&str> = host.split('.').collect();
    if parts.is_empty() || parts.len() > 4 || parts.iter().any(|p| p.is_empty()) {
        return None;
    }
    let nums: Vec<u64> = parts.iter().map(|p| parse_c_int(p)).collect::<Option<Vec<_>>>()?;
    let (last, head) = nums.split_last()?;
    if head.iter().any(|n| *n > 255) {
        return None;
    }
    let rest_bytes = 4 - head.len();
    if *last >= 1u64 << (8 * rest_bytes) {
        return None;
    }
    let mut v: u64 = *last;
    for (i, n) in head.iter().enumerate() {
        v |= n << (8 * (3 - i));
    }
    Some(Ipv4Addr::from(v as u32))
}

#[async_trait]
impl Resolver for MockResolver {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<IpAddr>> {
        let h = host.trim_end_matches('.').to_ascii_lowercase();
        self.calls.lock().push(h.clone());
        let literal = h.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = literal.parse::<IpAddr>() {
            return Ok(vec![ip]);
        }
        if let Some((first, later)) = self.flip.lock().get(&h).cloned() {
            let n = self.calls.lock().iter().filter(|c| **c == h).count();
            return Ok(if n <= 1 { first } else { later });
        }
        if let Some(ips) = self.table.lock().get(&h) {
            return Ok(ips.clone());
        }
        if let Some(ip) = legacy_ipv4(&h) {
            return Ok(vec![IpAddr::V4(ip)]);
        }
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "nxdomain"))
    }
}

/// The connect seam: documentation-range "public" addresses are served by the loopback mock.
fn rewrite_to_mock(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(a) if a.octets()[..3] == [203, 0, 113] || a.octets()[..3] == [198, 51, 100] => {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------------------------
// Rig + helpers
// ---------------------------------------------------------------------------------------------

struct Rig {
    guard: GuardedEgress,
    resolver: Arc<MockResolver>,
}

fn cfg() -> EgressGuardConfig {
    // A guard that wrongly tries to connect to an unroutable address must fail fast (and with
    // `Timeout`, not `Denied`, which is what the assertions look for).
    EgressGuardConfig {
        connect_timeout: Duration::from_millis(500),
        ..EgressGuardConfig::default()
    }
}

fn empty_denylist() -> Arc<SelfDenylist> {
    Arc::new(SelfDenylist::new(Vec::new(), Vec::new()))
}

fn rig_with(cfg: EgressGuardConfig, denylist: Arc<SelfDenylist>) -> Rig {
    let resolver = MockResolver::new();
    let guard = GuardedEgress::new(cfg, denylist)
        .with_resolver(resolver.clone())
        .with_connect_rewrite(Arc::new(rewrite_to_mock));
    Rig { guard, resolver }
}

fn rig() -> Rig {
    rig_with(cfg(), empty_denylist())
}

#[derive(Debug)]
struct Fetched {
    status: StatusCode,
    #[allow(dead_code, reason = "shown by the Debug output of failing assertions")]
    headers: HeaderMap,
    body: Bytes,
}

impl Fetched {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
}

fn get_request(url: &str) -> Request {
    http::Request::builder()
        .method("GET")
        .uri(url)
        .body(Body::empty())
        .expect("valid request")
}

fn budget() -> EgressBudget {
    EgressBudget::new(&Limits::default(), Instant::now() + Duration::from_secs(20), None)
}

/// Send `req` and read the whole response. Cap violations that surface while the body streams
/// are mapped to `EgressError::TooLarge`, so streaming and buffering guards both satisfy the tests.
async fn send(
    guard: &GuardedEgress,
    req: Request,
    policy: &EgressPolicy,
    budget: &mut EgressBudget,
) -> Result<Fetched, EgressError> {
    let sent = tokio::time::timeout(Duration::from_secs(20), guard.send(req, policy, budget))
        .await
        .expect("guard.send hung for 20 s");
    let resp = sent?;
    let (parts, body) = resp.into_parts();
    let collected = tokio::time::timeout(Duration::from_secs(20), body.collect(u64::MAX))
        .await
        .expect("reading the response body hung for 20 s");
    match collected {
        Ok(body) => Ok(Fetched {
            status: parts.status,
            headers: parts.headers,
            body,
        }),
        Err(BodyError::TooLarge { .. }) => Err(EgressError::TooLarge),
        Err(BodyError::Aborted(m)) | Err(BodyError::Io(m)) if m.contains("egress_too_large") => {
            Err(EgressError::TooLarge)
        }
        Err(other) => panic!("unexpected body error while reading the response: {other:?}"),
    }
}

async fn fetch(guard: &GuardedEgress, url: &str, policy: &EgressPolicy) -> Result<Fetched, EgressError> {
    send(guard, get_request(url), policy, &mut budget()).await
}

async fn assert_denied(guard: &GuardedEgress, url: &str, policy: &EgressPolicy) {
    match fetch(guard, url, policy).await {
        Err(EgressError::Denied) => {}
        other => panic!("{url}: expected egress_denied, got {other:?}"),
    }
}

fn public_url(srv: &MockServer, path: &str) -> String {
    format!("http://public.test:{}{}", srv.port(), path)
}

fn gzip_bytes(plain: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(plain).expect("gzip write");
    enc.finish().expect("gzip finish")
}

/// Pseudo-random text over a 4-letter alphabet: gzip ratio about 4:1 (far below the ratio limit).
fn dna(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            b"ACGT"[((x >> 33) & 3) as usize]
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// SEC-10: SSRF corpus
// ---------------------------------------------------------------------------------------------

/// Everything that names the host's own loopback / unspecified address in some form. `{p}` is the
/// port of the tripwire server that is really listening on `127.0.0.1`.
fn loopback_corpus(p: u16) -> Vec<String> {
    [
        "http://127.0.0.1:{p}/",
        "http://127.1:{p}/",
        "http://127.0.0.2:{p}/",
        "http://2130706433:{p}/",
        "http://0x7f000001:{p}/",
        "http://0x7f.0.0.1:{p}/",
        "http://0177.0.0.1:{p}/",
        "http://017700000001:{p}/",
        "http://0.0.0.0:{p}/",
        "http://0:{p}/",
        "http://[::1]:{p}/",
        "http://[::]:{p}/",
        "http://[::ffff:127.0.0.1]:{p}/",
        "http://[::ffff:7f00:1]:{p}/",
        "http://[0:0:0:0:0:ffff:127.0.0.1]:{p}/",
        "http://localhost:{p}/",
        "http://localhost.:{p}/",
        "http://LOCALHOST:{p}/",
        "http://foo.localhost:{p}/",
        "http://user:pass@127.0.0.1:{p}/",
        "http://public.test@127.0.0.1:{p}/",
        "https://127.0.0.1:{p}/",
        "http://rebind.test:{p}/",
        "http://mixed.test:{p}/",
        "http://mixed-private-first.test:{p}/",
    ]
    .iter()
    .map(|t| t.replace("{p}", &p.to_string()))
    .collect()
}

/// Private / link-local / metadata / CGNAT / multicast targets (nothing listens there).
fn private_corpus() -> Vec<&'static str> {
    vec![
        "http://169.254.169.254/latest/meta-data/",
        "http://169.254.169.254:80/",
        "http://[fd00:ec2::254]/latest/meta-data/",
        "http://metadata.google.internal/computeMetadata/v1/",
        "http://[fe80::1]/",
        "http://[fc00::1]/",
        "http://[fd12:3456:789a::1]/",
        "http://10.0.0.1/",
        "http://10.255.255.255/",
        "http://172.16.0.1/",
        "http://172.31.255.254/",
        "http://192.168.0.1/",
        "http://3232235521/",
        "http://0xc0a80001/",
        "http://0300.0250.0.1/",
        "http://100.64.0.1/",
        "http://100.127.255.255/",
        "http://224.0.0.1/",
        "http://[ff02::1]/",
        "http://255.255.255.255/",
        "http://[::ffff:10.0.0.1]/",
        "http://[::ffff:a9fe:a9fe]/",
        "http://[::ffff:192.168.1.1]/",
        "http://internal.test/",
        "http://v6private.test/",
        "http://mapped.test/",
    ]
}

#[tokio::test]
async fn ac_sec_10_01_ssrf_corpus_loopback_forms_denied_and_never_connected() {
    let tripwire = MockServer::tripwire().await;
    let r = rig();
    let corpus = loopback_corpus(tripwire.port());
    let mut parsed = 0;
    for url in &corpus {
        if url.parse::<http::Uri>().is_err() {
            // Not even a valid request target for the WASI layer; nothing reaches the guard.
            continue;
        }
        parsed += 1;
        assert_denied(&r.guard, url, &EgressPolicy::default()).await;
    }
    assert!(
        parsed >= 15,
        "corpus shrank to {parsed} parseable URLs: check the http::Uri parser"
    );
    assert_eq!(
        tripwire.connections(),
        0,
        "the guard connected to loopback for one of the corpus URLs"
    );
    assert_eq!(EgressError::Denied.code(), "egress_denied");
}

#[tokio::test]
async fn ac_sec_10_02_ssrf_corpus_private_and_metadata_targets_denied() {
    let r = rig();
    let mut checked = 0;
    for url in private_corpus() {
        if url.parse::<http::Uri>().is_err() {
            continue;
        }
        checked += 1;
        assert_denied(&r.guard, url, &EgressPolicy::default()).await;
    }
    assert!(checked >= 15, "corpus shrank to {checked} parseable URLs");
}

#[tokio::test]
async fn ac_sec_10_03_public_control_url_succeeds() {
    let srv = MockServer::start(|req| match req.path.as_str() {
        "/ok" => MockReply::ok("control-ok"),
        _ => MockReply::status(404),
    })
    .await;
    let r = rig();
    let res = fetch(&r.guard, &public_url(&srv, "/ok"), &EgressPolicy::default())
        .await
        .expect("public URL allowed");
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.text(), "control-ok");
    // IP-literal public target too (no DNS involved).
    let res = fetch(
        &r.guard,
        &format!("http://203.0.113.77:{}/ok", srv.port()),
        &EgressPolicy::default(),
    )
    .await
    .expect("public IP literal allowed");
    assert_eq!(res.text(), "control-ok");
    assert_eq!(srv.hits(), 2);
}

#[tokio::test]
async fn ac_sec_10_04_dns_rebinding_mixed_answer_denied() {
    // A name that resolves to a public AND a private address is refused as a whole: the guard
    // never "picks the first safe one".
    let tripwire = MockServer::tripwire().await;
    let r = rig();
    for name in ["mixed.test", "mixed-private-first.test"] {
        assert_denied(
            &r.guard,
            &format!("http://{name}:{}/", tripwire.port()),
            &EgressPolicy::default(),
        )
        .await;
    }
    assert_eq!(tripwire.connections(), 0);
}

#[tokio::test]
async fn ac_sec_10_05_dns_rebinding_resolves_once_per_hop() {
    // flip.test answers with a public IP the first time and 127.0.0.1 afterwards. The guard must
    // resolve exactly once per hop and connect to the address it validated.
    let srv = MockServer::start(|req| match req.path.as_str() {
        "/first" => MockReply::ok("first"),
        "/second-hop" => MockReply::redirect(302, &format!("http://flip2.test:{}/second", req.headers_port())),
        _ => MockReply::status(404),
    })
    .await;
    let r = rig();
    r.resolver
        .set_flip("flip.test", vec![v4(203, 0, 113, 90)], vec![v4(127, 0, 0, 1)]);
    r.resolver
        .set_flip("flip2.test", vec![v4(203, 0, 113, 91)], vec![v4(127, 0, 0, 1)]);

    let url = format!("http://flip.test:{}/first", srv.port());
    let res = fetch(&r.guard, &url, &EgressPolicy::default())
        .await
        .expect("first resolution is public");
    assert_eq!(res.text(), "first");
    assert_eq!(
        r.resolver.lookups("flip.test"),
        1,
        "the guard resolved the name more than once for one hop"
    );

    // flip2.test: public on hop 1, then the redirect target (same name) resolves to loopback on
    // hop 2. Every hop is validated again, so the second hop is denied.
    let url = format!("http://flip2.test:{}/second-hop", srv.port());
    match fetch(&r.guard, &url, &EgressPolicy::default()).await {
        Err(EgressError::Denied) => {}
        other => panic!("rebinding across hops must be denied, got {other:?}"),
    }
}

/// `MockServer` handlers need their own port to build absolute `Location`s; read it from `Host`.
trait SeenRequestExt {
    fn headers_port(&self) -> u16;
}

impl SeenRequestExt for SeenRequest {
    fn headers_port(&self) -> u16 {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("host"))
            .and_then(|(_, v)| v.rsplit_once(':').and_then(|(_, p)| p.parse().ok()))
            .unwrap_or(0)
    }
}

#[tokio::test]
async fn ac_sec_10_06_nxdomain_is_a_dns_error_not_a_panic() {
    let r = rig();
    match fetch(&r.guard, "http://nxdomain.test/", &EgressPolicy::default()).await {
        Err(EgressError::Dns) => {}
        other => panic!("expected egress_dns, got {other:?}"),
    }
}

#[tokio::test]
async fn ac_sec_10_07_redirect_public_to_private_denied_at_every_hop() {
    let tripwire = MockServer::tripwire().await;
    let t = tripwire.port();
    let public = MockServer::start(move |req| {
        let loc = |s: &str| MockReply::redirect(302, &s.replace("{t}", &t.to_string()));
        match req.path.as_str() {
            "/ok" => MockReply::ok("ok"),
            "/relative" => MockReply::redirect(302, "/ok"),
            "/to-metadata" => loc("http://169.254.169.254/latest/meta-data/"),
            "/to-loopback" => loc("http://127.0.0.1:{t}/secret"),
            "/to-localhost" => MockReply::redirect(301, &format!("http://localhost:{t}/secret")),
            "/to-decimal" => MockReply::redirect(307, &format!("http://2130706433:{t}/secret")),
            "/to-ipv6-mapped" => loc("http://[::ffff:127.0.0.1]:{t}/secret"),
            "/to-private-10" => loc("http://10.0.0.5/admin"),
            "/to-rebind-name" => loc("http://rebind.test:{t}/secret"),
            "/to-scheme-relative" => loc("//127.0.0.1:{t}/secret"),
            "/double" => MockReply::redirect(302, "/to-metadata"),
            _ => MockReply::status(404),
        }
    })
    .await;
    let r = rig();
    let policy = EgressPolicy::default();

    // sanity: a redirect that stays public is followed
    let ok = fetch(&r.guard, &public_url(&public, "/relative"), &policy)
        .await
        .expect("public redirect followed");
    assert_eq!(ok.text(), "ok");

    for path in [
        "/to-metadata",
        "/to-loopback",
        "/to-localhost",
        "/to-decimal",
        "/to-ipv6-mapped",
        "/to-private-10",
        "/to-rebind-name",
        "/double",
    ] {
        match fetch(&r.guard, &public_url(&public, path), &policy).await {
            Err(EgressError::Denied) => {}
            other => panic!("redirect {path}: expected egress_denied, got {other:?}"),
        }
    }
    // A protocol-relative `Location` (`//127.0.0.1:port/x`) is refused too; a guard that cannot
    // resolve it may report a protocol failure instead of `egress_denied`.
    match fetch(&r.guard, &public_url(&public, "/to-scheme-relative"), &policy).await {
        Err(EgressError::Denied | EgressError::Upstream) => {}
        other => panic!("protocol-relative redirect to loopback must be refused, got {other:?}"),
    }
    assert_eq!(
        tripwire.connections(),
        0,
        "a redirect made the guard connect to loopback"
    );
}

#[tokio::test]
async fn ac_sec_10_08_non_http_schemes_denied_direct_and_via_redirect() {
    let public = MockServer::start(|req| match req.path.as_str() {
        "/to-file" => MockReply::redirect(302, "file:///etc/passwd"),
        "/to-ftp" => MockReply::redirect(302, "ftp://public.test/x"),
        "/to-gopher" => MockReply::redirect(302, "gopher://public.test/x"),
        _ => MockReply::status(404),
    })
    .await;
    let r = rig();
    let policy = EgressPolicy::default();
    for url in [
        "file:///etc/passwd",
        "ftp://public.test/x",
        "gopher://public.test/x",
        "ws://public.test/x",
        "wss://public.test/x",
        "data:text/plain,hi",
    ] {
        if url.parse::<http::Uri>().is_err() {
            continue;
        }
        match fetch(&r.guard, url, &policy).await {
            Err(EgressError::Denied) => {}
            other => panic!("{url}: expected egress_denied, got {other:?}"),
        }
    }
    for path in ["/to-ftp", "/to-gopher"] {
        match fetch(&r.guard, &public_url(&public, path), &policy).await {
            Err(EgressError::Denied) => {}
            other => panic!("redirect {path}: expected egress_denied, got {other:?}"),
        }
    }
    // `file:///...` has no authority and may not even parse as a URI: refused either way.
    match fetch(&r.guard, &public_url(&public, "/to-file"), &policy).await {
        Err(EgressError::Denied | EgressError::Upstream) => {}
        other => panic!("redirect to file://: must be refused, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// SEC-11: port denylist
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_sec_11_01_default_config_denies_smtp_ports() {
    assert_eq!(EgressGuardConfig::default().denied_ports, vec![25, 465, 587]);
}

#[tokio::test]
async fn ac_sec_11_02_fetch_to_smtp_ports_denied() {
    let r = rig();
    let policy = EgressPolicy::default();
    for port in [25u16, 465, 587] {
        assert_denied(&r.guard, &format!("http://public.test:{port}/"), &policy).await;
        assert_denied(&r.guard, &format!("https://public.test:{port}/"), &policy).await;
        assert_denied(&r.guard, &format!("http://203.0.113.10:{port}/"), &policy).await;
    }
}

#[tokio::test]
async fn ac_sec_11_03_configured_denied_ports_and_redirect_to_denied_port() {
    let srv = MockServer::start(|req| match req.path.as_str() {
        "/to-smtp" => MockReply::redirect(302, "http://public.test:25/"),
        "/ok" => MockReply::ok("ok"),
        _ => MockReply::status(404),
    })
    .await;

    // default config: a redirect into port 25 is refused
    let r = rig();
    match fetch(&r.guard, &public_url(&srv, "/to-smtp"), &EgressPolicy::default()).await {
        Err(EgressError::Denied) => {}
        other => panic!("redirect to port 25: expected egress_denied, got {other:?}"),
    }

    // operator list: `egress.denied_ports` is honoured, the mock server's own port stays reachable
    let custom = EgressGuardConfig {
        denied_ports: vec![8080, 9999],
        ..cfg()
    };
    let r = rig_with(custom, empty_denylist());
    assert_denied(&r.guard, "http://public.test:8080/", &EgressPolicy::default()).await;
    assert_denied(&r.guard, "http://public.test:9999/", &EgressPolicy::default()).await;
    let ok = fetch(&r.guard, &public_url(&srv, "/ok"), &EgressPolicy::default())
        .await
        .expect("other ports allowed");
    assert_eq!(ok.text(), "ok");
}

// ---------------------------------------------------------------------------------------------
// SEC-12: per-service host rules
// ---------------------------------------------------------------------------------------------

fn allow(hosts: &[&str]) -> EgressPolicy {
    EgressPolicy {
        allowed_hosts: hosts.iter().map(|h| h.to_string()).collect(),
        ..EgressPolicy::default()
    }
}

#[tokio::test]
async fn ac_sec_12_01_allowed_hosts_is_an_allowlist() {
    let srv = MockServer::start(|req| match req.path.as_str() {
        "/bot" => MockReply::ok("telegram"),
        _ => MockReply::status(404),
    })
    .await;
    let r = rig();
    let policy = allow(&["api.telegram.org"]);
    let p = srv.port();

    assert_denied(&r.guard, &format!("http://example.com:{p}/bot"), &policy).await;
    assert_denied(&r.guard, &format!("http://public.test:{p}/bot"), &policy).await;
    assert_denied(&r.guard, &format!("http://203.0.113.20:{p}/bot"), &policy).await; // IP literal, not the name

    let ok = fetch(&r.guard, &format!("http://api.telegram.org:{p}/bot"), &policy)
        .await
        .expect("allowed host");
    assert_eq!(ok.text(), "telegram");
    // dot-suffix match, case-insensitive, trailing dot ignored
    let ok = fetch(&r.guard, &format!("http://sub.api.telegram.org:{p}/bot"), &policy)
        .await
        .expect("subdomain");
    assert_eq!(ok.text(), "telegram");
    let ok = fetch(&r.guard, &format!("http://API.Telegram.ORG.:{p}/bot"), &policy)
        .await
        .expect("case + trailing dot");
    assert_eq!(ok.text(), "telegram");

    // look-alikes are not matches
    assert_denied(&r.guard, &format!("http://notapi.telegram.org:{p}/bot"), &policy).await;
    assert_denied(&r.guard, &format!("http://api.telegram.org.evil.test:{p}/bot"), &policy).await;
    // an empty allowlist means "any public host"
    let any = fetch(
        &r.guard,
        &format!("http://example.com:{p}/bot"),
        &EgressPolicy::default(),
    )
    .await
    .expect("no list");
    assert_eq!(any.text(), "telegram");
}

#[tokio::test]
async fn ac_sec_12_02_allowlist_is_enforced_on_every_redirect_hop() {
    let srv = MockServer::start(|req| {
        let port = req.headers_port();
        match req.path.as_str() {
            "/to-example" => MockReply::redirect(302, &format!("http://example.com:{port}/bot")),
            "/stay" => MockReply::redirect(302, "/bot"),
            "/bot" => MockReply::ok("telegram"),
            _ => MockReply::status(404),
        }
    })
    .await;
    let r = rig();
    let policy = allow(&["api.telegram.org"]);
    let p = srv.port();
    let ok = fetch(&r.guard, &format!("http://api.telegram.org:{p}/stay"), &policy)
        .await
        .expect("same-host redirect");
    assert_eq!(ok.text(), "telegram");
    match fetch(&r.guard, &format!("http://api.telegram.org:{p}/to-example"), &policy).await {
        Err(EgressError::Denied) => {}
        other => panic!("redirect off the allowlist: expected egress_denied, got {other:?}"),
    }
    // example.com was never contacted: only the two requests to api.telegram.org's address were served
    assert_eq!(srv.paths(), vec!["/stay", "/bot", "/to-example"]);
}

#[tokio::test]
async fn ac_sec_12_03_denied_hosts_service_and_node_wide() {
    let srv = MockServer::start(|_| MockReply::ok("ok")).await;
    let p = srv.port();

    // per service
    let r = rig();
    let policy = EgressPolicy {
        denied_hosts: vec!["blocked.test".into()],
        ..EgressPolicy::default()
    };
    assert_denied(&r.guard, &format!("http://blocked.test:{p}/"), &policy).await;
    assert_denied(&r.guard, &format!("http://a.blocked.test:{p}/"), &policy).await; // dot-suffix
    fetch(&r.guard, &format!("http://example.com:{p}/"), &policy)
        .await
        .expect("other hosts unaffected");

    // denied wins over allowed
    let both = EgressPolicy {
        allowed_hosts: vec!["blocked.test".into()],
        denied_hosts: vec!["blocked.test".into()],
        default_deny: false,
    };
    assert_denied(&r.guard, &format!("http://blocked.test:{p}/"), &both).await;

    // node-wide `egress.denied_hosts`
    let node = EgressGuardConfig {
        denied_hosts: vec!["corp.test".into()],
        ..cfg()
    };
    let r = rig_with(node, empty_denylist());
    assert_denied(&r.guard, &format!("http://corp.test:{p}/"), &EgressPolicy::default()).await;
    fetch(&r.guard, &format!("http://example.com:{p}/"), &EgressPolicy::default())
        .await
        .expect("other hosts unaffected");
}

#[tokio::test]
async fn ac_sec_12_04_plan_default_deny_means_operator_allowlist_only() {
    let srv = MockServer::start(|_| MockReply::ok("ok")).await;
    let p = srv.port();
    let r = rig();
    let deny_all = EgressPolicy {
        default_deny: true,
        ..EgressPolicy::default()
    };
    assert_denied(&r.guard, &format!("http://public.test:{p}/"), &deny_all).await;
    assert_denied(&r.guard, &format!("http://example.com:{p}/"), &deny_all).await;

    let listed = EgressPolicy {
        allowed_hosts: vec!["example.com".into()],
        default_deny: true,
        ..EgressPolicy::default()
    };
    fetch(&r.guard, &format!("http://example.com:{p}/"), &listed)
        .await
        .expect("listed host allowed");
    assert_denied(&r.guard, &format!("http://public.test:{p}/"), &listed).await;
}

// ---------------------------------------------------------------------------------------------
// SEC-24: the node's own endpoints
// ---------------------------------------------------------------------------------------------

fn self_denylist() -> Arc<SelfDenylist> {
    // S3 endpoint, OTLP endpoint (host:port) and a binding base_url host, all on PUBLIC addresses.
    let deny = SelfDenylist::new(
        vec!["198.51.100.7".to_string(), "198.51.100.8:4317".to_string()],
        Vec::new(),
    );
    deny.refresh();
    deny.add_addrs([v4(198, 51, 100, 9)]);
    Arc::new(deny)
}

#[tokio::test]
async fn ac_sec_24_01_s3_otlp_and_binding_endpoints_denied() {
    let srv = MockServer::start(|_| MockReply::ok("ok")).await;
    let p = srv.port();
    let r = rig_with(cfg(), self_denylist());
    let policy = EgressPolicy::default();

    // by name (resolver maps them to the configured addresses) and by address
    for url in [
        format!("http://s3.example.test:{p}/bucket/key"),
        format!("http://otlp.example.test:{p}/v1/traces"),
        format!("http://api.binding.test:{p}/v1/calls"),
        format!("http://198.51.100.7:{p}/"),
        format!("http://198.51.100.8:{p}/"),
        format!("http://198.51.100.9:{p}/"),
    ] {
        assert_denied(&r.guard, &url, &policy).await;
    }
    assert_eq!(
        srv.connections(),
        0,
        "the guard connected to a self-denylisted endpoint"
    );

    // control: an unrelated public address is fine
    let ok = fetch(&r.guard, &format!("http://198.51.100.99:{p}/"), &policy)
        .await
        .expect("public control");
    assert_eq!(ok.text(), "ok");
}

#[tokio::test]
async fn ac_sec_24_02_denied_cidrs_from_config_are_denied() {
    let srv = MockServer::start(|_| MockReply::ok("ok")).await;
    let p = srv.port();
    let cidr: ipnet::IpNet = "203.0.113.0/24".parse().expect("cidr");
    let r = rig_with(cfg(), Arc::new(SelfDenylist::new(Vec::new(), vec![cidr])));
    assert_denied(&r.guard, &format!("http://public.test:{p}/"), &EgressPolicy::default()).await; // 203.0.113.10
    assert_denied(
        &r.guard,
        &format!("http://203.0.113.200:{p}/"),
        &EgressPolicy::default(),
    )
    .await;
    // 198.51.100.0/24 is outside the configured range
    let ok = fetch(&r.guard, &format!("http://other.test:{p}/"), &EgressPolicy::default())
        .await
        .expect("outside cidr");
    assert_eq!(ok.text(), "ok");
}

#[tokio::test]
async fn ac_sec_24_03_redirect_into_self_denylist_denied() {
    let srv = MockServer::start(|req| {
        let port = req.headers_port();
        match req.path.as_str() {
            "/to-s3" => MockReply::redirect(302, &format!("http://s3.example.test:{port}/bucket")),
            _ => MockReply::ok("ok"),
        }
    })
    .await;
    let r = rig_with(cfg(), self_denylist());
    match fetch(&r.guard, &public_url(&srv, "/to-s3"), &EgressPolicy::default()).await {
        Err(EgressError::Denied) => {}
        other => panic!("redirect to the S3 endpoint: expected egress_denied, got {other:?}"),
    }
    assert_eq!(srv.paths(), vec!["/to-s3"]);
}

#[tokio::test]
async fn ac_sec_24_04_validate_endpoint_applies_the_same_rules_to_operator_urls() {
    let r = rig_with(cfg(), self_denylist());
    // blocked classes
    for (host, port) in [
        ("127.0.0.1", 9000u16),
        ("localhost", 4317),
        ("internal.test", 80),
        ("169.254.169.254", 80),
        ("10.0.0.1", 5432),
    ] {
        match r.guard.validate_endpoint(host, port).await {
            Err(EgressError::Denied) => {}
            other => panic!("{host}:{port}: expected egress_denied, got {other:?}"),
        }
    }
    // the node's own addresses, even when public
    for (host, port) in [
        ("s3.example.test", 443u16),
        ("198.51.100.8", 4317),
        ("api.binding.test", 8000),
    ] {
        match r.guard.validate_endpoint(host, port).await {
            Err(EgressError::Denied) => {}
            other => panic!("{host}:{port}: expected egress_denied, got {other:?}"),
        }
    }
    // a public endpoint passes; an unknown name is a DNS error
    r.guard
        .validate_endpoint("public.test", 443)
        .await
        .expect("public endpoint valid");
    match r.guard.validate_endpoint("nxdomain.test", 443).await {
        Err(EgressError::Dns) => {}
        other => panic!("expected egress_dns, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// SEC-30 + response size cap
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac_sec_30_01_gzip_bomb_aborted_at_the_response_cap() {
    // compressed < cap < decompressed, ratio about 4 (far below 100): only the *decompressed* count trips.
    let plain = dna(3 * 1024 * 1024, 7);
    let body = gzip_bytes(&plain);
    assert!(
        body.len() < 1024 * 1024 && body.len() * 100 > plain.len(),
        "test data: compressed {} B",
        body.len()
    );
    let srv = MockServer::start(move |_| MockReply {
        status: 200,
        headers: vec![("Content-Encoding".into(), "gzip".into())],
        payload: Payload::Fixed(body.clone()),
    })
    .await;

    let r = rig_with(
        EgressGuardConfig {
            max_response_bytes: 1024 * 1024,
            ..cfg()
        },
        empty_denylist(),
    );
    match fetch(&r.guard, &public_url(&srv, "/"), &EgressPolicy::default()).await {
        Err(EgressError::TooLarge) => {}
        other => panic!("expected egress_too_large, got {other:?}"),
    }
    assert_eq!(EgressError::TooLarge.code(), "egress_too_large");
}

#[tokio::test]
async fn ac_sec_30_02_inflate_ratio_over_100_aborts() {
    // 8 MiB of zeros gzips to about 8 KiB (ratio ~1000). The default 100 MB cap is nowhere near
    // reached: the ratio rule alone must abort it.
    let plain = vec![0u8; 8 * 1024 * 1024];
    let srv = MockServer::start({
        let body = gzip_bytes(&plain);
        move |_| MockReply {
            status: 200,
            headers: vec![("Content-Encoding".into(), "gzip".into())],
            payload: Payload::Fixed(body.clone()),
        }
    })
    .await;
    let r = rig();
    match fetch(&r.guard, &public_url(&srv, "/"), &EgressPolicy::default()).await {
        Err(EgressError::TooLarge) => {}
        other => panic!("expected egress_too_large, got {other:?}"),
    }
}

#[tokio::test]
async fn ac_sec_30_03_ordinary_gzip_response_is_decoded() {
    let plain = dna(256 * 1024, 11);
    let expected = plain.clone();
    let srv = MockServer::start(move |_| MockReply::gzip(&plain)).await;
    let r = rig();
    let res = fetch(&r.guard, &public_url(&srv, "/"), &EgressPolicy::default())
        .await
        .expect("ratio ~4 passes");
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.body.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn egress_response_cap_content_length_chunked_and_endless() {
    const CAP: u64 = 64 * 1024;
    let srv = MockServer::start(|req| match req.path.as_str() {
        "/exact" => MockReply::ok(vec![b'a'; CAP as usize]),
        "/over" => MockReply::ok(vec![b'a'; CAP as usize + 1]),
        "/chunked-over" => MockReply::chunked(vec![b'a'; 3 * CAP as usize]),
        "/chunked-exact" => MockReply::chunked(vec![b'a'; CAP as usize]),
        "/endless" => MockReply::endless(),
        _ => MockReply::status(404),
    })
    .await;
    let r = rig_with(
        EgressGuardConfig {
            max_response_bytes: CAP,
            ..cfg()
        },
        empty_denylist(),
    );
    let policy = EgressPolicy::default();

    let exact = fetch(&r.guard, &public_url(&srv, "/exact"), &policy)
        .await
        .expect("exactly at the cap is allowed");
    assert_eq!(exact.body.len() as u64, CAP);
    let exact = fetch(&r.guard, &public_url(&srv, "/chunked-exact"), &policy)
        .await
        .expect("chunked exactly at the cap");
    assert_eq!(exact.body.len() as u64, CAP);

    for path in ["/over", "/chunked-over", "/endless"] {
        match fetch(&r.guard, &public_url(&srv, path), &policy).await {
            Err(EgressError::TooLarge) => {}
            other => panic!("{path}: expected egress_too_large, got {other:?}"),
        }
    }

    // The unbounded stream stops being read soon after the cap: the producer cannot push far past it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let wrote = srv.bytes_written();
    assert!(
        wrote < 64 * 1024 * 1024,
        "the guard kept reading an endless response: server wrote {wrote} B"
    );
}

// ---------------------------------------------------------------------------------------------
// Redirects: limit, method rewriting
// ---------------------------------------------------------------------------------------------

fn chain_server() -> impl Fn(&SeenRequest) -> MockReply + Send + Sync + 'static {
    |req| {
        if let Some(n) = req.path.strip_prefix("/chain/").and_then(|n| n.parse::<u32>().ok()) {
            return if n == 0 {
                MockReply::ok("end")
            } else {
                MockReply::redirect(302, &format!("/chain/{}", n - 1))
            };
        }
        match req.path.as_str() {
            "/loop" => MockReply::redirect(302, "/loop"),
            _ => MockReply::status(404),
        }
    }
}

#[tokio::test]
async fn egress_redirect_limit_is_five_hops() {
    let srv = MockServer::start(chain_server()).await;
    let r = rig(); // max_redirects = 5

    // 5 redirects then a 200: initial request + 5 follow-ups = 6 requests
    let ok = fetch(&r.guard, &public_url(&srv, "/chain/5"), &EgressPolicy::default())
        .await
        .expect("5 redirects followed");
    assert_eq!(ok.text(), "end");
    assert_eq!(srv.hits(), 6);

    // 6 redirects: the guard stops after following 5 and never requests /chain/0
    let before = srv.hits();
    let res = fetch(&r.guard, &public_url(&srv, "/chain/6"), &EgressPolicy::default()).await;
    assert!(res.is_err(), "6 redirects must not be followed to the end, got {res:?}");
    assert_eq!(srv.hits() - before, 6, "paths: {:?}", srv.paths());
    assert!(!srv.paths()[before..].contains(&"/chain/0".to_string()));

    // an endless loop terminates the same way
    let before = srv.hits();
    assert!(fetch(&r.guard, &public_url(&srv, "/loop"), &EgressPolicy::default())
        .await
        .is_err());
    assert_eq!(srv.hits() - before, 6);
}

#[tokio::test]
async fn egress_redirect_limit_is_configurable() {
    let srv = MockServer::start(chain_server()).await;
    let r = rig_with(
        EgressGuardConfig {
            max_redirects: 1,
            ..cfg()
        },
        empty_denylist(),
    );
    let ok = fetch(&r.guard, &public_url(&srv, "/chain/1"), &EgressPolicy::default())
        .await
        .expect("one hop allowed");
    assert_eq!(ok.text(), "end");
    let before = srv.hits();
    assert!(fetch(&r.guard, &public_url(&srv, "/chain/2"), &EgressPolicy::default())
        .await
        .is_err());
    assert_eq!(srv.hits() - before, 2);

    let none = rig_with(
        EgressGuardConfig {
            max_redirects: 0,
            ..cfg()
        },
        empty_denylist(),
    );
    let before = srv.hits();
    assert!(
        fetch(&none.guard, &public_url(&srv, "/chain/1"), &EgressPolicy::default())
            .await
            .is_err()
    );
    assert_eq!(srv.hits() - before, 1);
}

#[tokio::test]
async fn egress_redirect_method_and_body_rewriting() {
    let srv = MockServer::start(|req| match req.path.as_str() {
        "/post-301" => MockReply::redirect(301, "/whoami"),
        "/post-302" => MockReply::redirect(302, "/whoami"),
        "/post-303" => MockReply::redirect(303, "/whoami"),
        "/post-307" => MockReply::redirect(307, "/whoami"),
        "/post-308" => MockReply::redirect(308, "/whoami"),
        "/whoami" => MockReply::ok(format!("{} {}", req.method, req.body.len())),
        _ => MockReply::status(404),
    })
    .await;
    let r = rig();
    for (path, expected) in [
        ("/post-301", "GET 0"),
        ("/post-302", "GET 0"),
        ("/post-303", "GET 0"),
        ("/post-307", "POST 7"),
        ("/post-308", "POST 7"),
    ] {
        let req = http::Request::builder()
            .method("POST")
            .uri(public_url(&srv, path))
            .body(Body::from("payload"))
            .expect("request");
        let res = send(&r.guard, req, &EgressPolicy::default(), &mut budget())
            .await
            .expect(path);
        assert_eq!(res.text(), expected, "{path}");
    }
}

#[test]
fn egress_redirect_helpers() {
    for s in [301u16, 302, 303, 307, 308] {
        assert!(is_redirect(StatusCode::from_u16(s).expect("status")));
    }
    for s in [200u16, 204, 300, 304, 400, 500] {
        assert!(!is_redirect(StatusCode::from_u16(s).expect("status")), "{s}");
    }
    let post = http::Method::POST;
    assert_eq!(
        redirect_method(StatusCode::SEE_OTHER, &post),
        (http::Method::GET, false)
    );
    assert_eq!(
        redirect_method(StatusCode::TEMPORARY_REDIRECT, &post),
        (http::Method::POST, true)
    );
    assert_eq!(
        redirect_method(StatusCode::PERMANENT_REDIRECT, &post),
        (http::Method::POST, true)
    );
    assert_eq!(redirect_method(StatusCode::FOUND, &post), (http::Method::GET, false));
    assert_eq!(
        redirect_method(StatusCode::MOVED_PERMANENTLY, &http::Method::PUT),
        (http::Method::PUT, true)
    );
}

// ---------------------------------------------------------------------------------------------
// Per-task budget
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn egress_per_task_fetch_count_is_enforced() {
    let srv = MockServer::start(|_| MockReply::ok("ok")).await;
    let r = rig();
    let mut budget = budget();
    budget.fetches_left = 2;
    let policy = EgressPolicy::default();
    for _ in 0..2 {
        let res = send(&r.guard, get_request(&public_url(&srv, "/")), &policy, &mut budget).await;
        assert!(res.is_ok(), "{res:?}");
    }
    match send(&r.guard, get_request(&public_url(&srv, "/")), &policy, &mut budget).await {
        Err(EgressError::Limit) => {}
        other => panic!("third fetch: expected egress_limit, got {other:?}"),
    }
    assert_eq!(srv.hits(), 2);
}

#[tokio::test]
async fn egress_request_headers_reach_the_upstream_unchanged() {
    // The guard must not add credentials or strip what the guest set (other than hop-by-hop).
    let srv = MockServer::start(|_| MockReply::ok("ok")).await;
    let r = rig();
    let req = http::Request::builder()
        .method("GET")
        .uri(public_url(&srv, "/hdr"))
        .header("x-guest", "yes")
        .body(Body::empty())
        .expect("request");
    send(&r.guard, req, &EgressPolicy::default(), &mut budget())
        .await
        .expect("fetch");
    let seen = srv.request(0);
    assert_eq!(seen.method, "GET");
    assert!(
        seen.headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("x-guest") && v == "yes"),
        "{:?}",
        seen.headers
    );
    assert!(
        !seen
            .headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("authorization") || k.eq_ignore_ascii_case("cookie")),
        "{:?}",
        seen.headers
    );
}

// ---------------------------------------------------------------------------------------------
// Outbound WebSockets: `dial` applies the same rules as a fetch
// ---------------------------------------------------------------------------------------------

/// A WebSocket echo server on loopback (reached through the connect seam).
async fn ws_echo_server() -> u16 {
    use futures_util::{SinkExt, StreamExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(s).await else {
                    return;
                };
                while let Some(Ok(m)) = ws.next().await {
                    if m.is_text() || m.is_binary() {
                        let _ = ws.send(m).await;
                    }
                }
            });
        }
    });
    port
}

#[tokio::test]
async fn a_websocket_dial_is_checked_like_a_fetch_and_reaches_a_public_host() {
    use futures_util::{SinkExt, StreamExt};
    let rig = rig();
    let port = ws_echo_server().await;
    let open = EgressPolicy::default();
    for denied in [
        "ws://localhost:1/",
        "ws://127.0.0.1:1/",
        "ws://10.0.0.1:1/",
        "http://public.test/",
        "wss://169.254.169.254/",
    ] {
        let url = url::Url::parse(denied).expect("url");
        assert_eq!(
            rig.guard.dial(&url, &open).await.err(),
            Some(EgressError::Denied),
            "{denied}"
        );
    }
    let only_example = EgressPolicy {
        allowed_hosts: vec!["example.com".into()],
        ..EgressPolicy::default()
    };
    let url = url::Url::parse(&format!("ws://public.test:{port}/")).expect("url");
    assert_eq!(
        rig.guard.dial(&url, &only_example).await.err(),
        Some(EgressError::Denied),
        "the service's allowed_hosts"
    );

    let io = rig.guard.dial(&url, &open).await.expect("dial a public host");
    let (mut ws, _) = tokio_tungstenite::client_async(url.as_str(), io)
        .await
        .expect("handshake");
    ws.send(tokio_tungstenite::tungstenite::Message::text("hi"))
        .await
        .expect("send");
    let got = ws.next().await.expect("frame").expect("ok");
    assert_eq!(got.to_text().expect("text"), "hi");
}

/// EGR-017: an operator endpoint on a denied port (SMTP) is refused like a guest URL would be.
#[tokio::test]
async fn egr_017_validate_endpoint_applies_denied_ports() {
    let r = rig_with(cfg(), self_denylist());
    for port in [25u16, 465, 587] {
        match r.guard.validate_endpoint("public.test", port).await {
            Err(EgressError::Denied) => {}
            other => panic!("port {port}: expected egress_denied, got {other:?}"),
        }
    }
    r.guard
        .validate_endpoint("public.test", 8443)
        .await
        .expect("other ports pass");
}

/// EGR-014: once the egress quota is exhausted every later charge fails, the quota stays at 0
/// (never wraps), and every received byte is still counted, also under concurrent charges.
#[test]
fn egr_014_concurrent_charges_exhaust_the_quota_without_wrapping() {
    let usage = rt_host::EgressUsage::new(Some(1_000));
    let ok = std::sync::atomic::AtomicU64::new(0);
    std::thread::scope(|sc| {
        for _ in 0..8 {
            sc.spawn(|| {
                for _ in 0..100 {
                    if usage.consume(7) {
                        ok.fetch_add(7, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let granted = ok.load(std::sync::atomic::Ordering::Relaxed);
    assert!(granted <= 1_000, "never more than the quota is granted: {granted}");
    assert!(granted > 1_000 - 7, "the quota is used up: {granted}");
    assert_eq!(usage.quota_left(), Some(0));
    assert!(!usage.consume(1), "exhausted stays exhausted");
    assert_eq!(usage.quota_left(), Some(0));
}

/// EGR-011: a request body over `max_request_bytes` is refused before any connection; exactly
/// the cap is sent whole.
#[tokio::test]
async fn egr_011_the_request_body_cap_applies_before_connecting() {
    let srv = MockServer::start(|_| MockReply::ok("ok")).await;
    let r = rig_with(
        EgressGuardConfig {
            max_request_bytes: 4096,
            ..cfg()
        },
        empty_denylist(),
    );
    let post = |n: usize| {
        http::Request::builder()
            .method("POST")
            .uri(public_url(&srv, "/up"))
            .body(Body::from(vec![b'x'; n]))
            .expect("request")
    };
    match send(&r.guard, post(4097), &EgressPolicy::default(), &mut budget()).await {
        Err(EgressError::TooLarge) => {}
        other => panic!("expected egress_too_large, got {other:?}"),
    }
    assert_eq!(srv.connections(), 0, "refused before connecting");
    let ok = send(&r.guard, post(4096), &EgressPolicy::default(), &mut budget())
        .await
        .expect("exactly the cap");
    assert_eq!(ok.status, StatusCode::OK);
    assert_eq!(srv.request(0).body.len(), 4096);
}

/// EGR-006: a dual-stack name whose first address (IPv6) cannot be reached is fetched over its
/// IPv4 address instead of failing.
#[tokio::test]
async fn egr_006_an_unreachable_first_address_falls_back_to_the_next() {
    let srv = MockServer::start(|_| MockReply::ok("v4")).await;
    let r = rig_with(
        EgressGuardConfig {
            connect_timeout: Duration::from_secs(3),
            ..cfg()
        },
        empty_denylist(),
    );
    let res = fetch(
        &r.guard,
        &format!("http://dual.test:{}/x", srv.port()),
        &EgressPolicy::default(),
    )
    .await
    .expect("fetched over IPv4");
    assert_eq!(res.text(), "v4");
}
