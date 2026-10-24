//! Connection reuse of the egress guard (SECURITY.md, SEC-52): pinned clients are pooled per
//! `(tenant, scheme, host, port, pinned IP)`, so kept-alive connections are reused within one
//! tenant and never across tenants.
//!
//! Like `egress.rs`, names resolve to documentation-range "public" addresses through a mock
//! resolver and the connect seam dials a loopback keep-alive server instead.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rt_host::{EgressBudget, EgressGuard, EgressGuardConfig, EgressPolicy, GuardedEgress, Request, Resolver};
use rt_sec::SelfDenylist;
use rt_types::{Body, Limits, TenantId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// HTTP/1.1 server that keeps connections alive and answers every request `200 ok`.
struct KeepAliveServer {
    port: u16,
    connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl KeepAliveServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
        let port = listener.local_addr().expect("addr").port();
        let (connections, requests) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (c, r) = (connections.clone(), requests.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                c.fetch_add(1, Ordering::SeqCst);
                let r = r.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        let end = loop {
                            if let Some(p) = find(&buf, b"\r\n\r\n") {
                                break p + 4;
                            }
                            match sock.read(&mut tmp).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                            }
                        };
                        buf.drain(..end);
                        r.fetch_add(1, Ordering::SeqCst);
                        if sock
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });
        Self {
            port,
            connections,
            requests,
            task,
        }
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for KeepAliveServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `*.test` names → `203.0.113.0/24`; answers can be changed between fetches.
struct TableResolver {
    table: Mutex<HashMap<String, IpAddr>>,
}

#[async_trait]
impl Resolver for TableResolver {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<IpAddr>> {
        self.table
            .lock()
            .get(host)
            .map(|ip| vec![*ip])
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "nxdomain"))
    }
}

fn public(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(203, 0, 113, last))
}

struct Rig {
    guard: GuardedEgress,
    resolver: Arc<TableResolver>,
    server: KeepAliveServer,
}

async fn rig(cfg: EgressGuardConfig) -> Rig {
    let server = KeepAliveServer::start().await;
    let table = HashMap::from([
        ("a.test".to_string(), public(10)),
        ("b.test".into(), public(11)),
        ("c.test".into(), public(12)),
    ]);
    let resolver = Arc::new(TableResolver {
        table: Mutex::new(table),
    });
    let guard = GuardedEgress::new(cfg, Arc::new(SelfDenylist::new(Vec::new(), Vec::new())))
        .with_resolver(resolver.clone())
        .with_connect_rewrite(Arc::new(|ip: IpAddr| match ip {
            IpAddr::V4(a) if a.octets()[..3] == [203, 0, 113] => IpAddr::V4(Ipv4Addr::LOCALHOST),
            other => other,
        }));
    Rig {
        guard,
        resolver,
        server,
    }
}

impl Rig {
    /// GET `http://<host>:<port>/` as `tenant` and read the whole body (the connection goes back
    /// to the pool), then give the client a moment to park it as idle.
    async fn get(&self, host: &str, tenant: Option<u128>) {
        let req: Request = http::Request::builder()
            .uri(format!("http://{host}:{}/", self.server.port))
            .body(Body::empty())
            .expect("request");
        let mut budget = EgressBudget::new(&Limits::default(), Instant::now() + Duration::from_secs(10), None);
        if let Some(t) = tenant {
            budget = budget.for_tenant(TenantId::from_u128(t));
        }
        let resp = self
            .guard
            .send(req, &EgressPolicy::default(), &mut budget)
            .await
            .expect("fetch");
        assert_eq!(resp.status(), 200);
        let body = resp.into_body().collect(u64::MAX).await.expect("body");
        assert_eq!(&body[..], b"ok");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn ac_sec_52_one_tenant_reuses_a_connection_and_another_tenant_never_shares_it() {
    let r = rig(EgressGuardConfig::default()).await;
    r.get("a.test", Some(1)).await;
    r.get("a.test", Some(1)).await;
    assert_eq!(r.server.requests(), 2);
    assert_eq!(
        r.server.connections(),
        1,
        "the second fetch of the same tenant reuses the kept-alive connection"
    );
    assert_eq!(r.guard.pooled_clients(), 1);

    r.get("a.test", Some(2)).await;
    assert_eq!(r.server.connections(), 2, "another tenant opens its own connection");
    assert_eq!(r.guard.pooled_clients(), 2);

    r.get("a.test", None).await;
    assert_eq!(
        r.server.connections(),
        3,
        "node-owned fetches (builder) share nothing with tenants"
    );

    r.get("a.test", Some(1)).await;
    r.get("a.test", Some(2)).await;
    assert_eq!(r.server.connections(), 3, "each scope keeps reusing its own connection");
    assert_eq!(r.server.requests(), 6);
}

#[tokio::test]
async fn ac_sec_52_a_new_dns_answer_never_reuses_a_connection_pinned_to_the_old_address() {
    let r = rig(EgressGuardConfig::default()).await;
    r.get("a.test", Some(1)).await;
    r.resolver.table.lock().insert("a.test".into(), public(20));
    r.get("a.test", Some(1)).await;
    assert_eq!(
        r.guard.pooled_clients(),
        2,
        "the pinned address is part of the pool key"
    );
    assert_eq!(r.server.connections(), 2);
}

#[tokio::test]
async fn ac_sec_52_the_pool_is_bounded_and_can_be_turned_off() {
    let r = rig(EgressGuardConfig {
        max_pooled_clients: 2,
        ..EgressGuardConfig::default()
    })
    .await;
    for host in ["a.test", "b.test", "c.test"] {
        r.get(host, Some(1)).await;
    }
    assert_eq!(r.guard.pooled_clients(), 2, "least recently used client evicted");

    let off = rig(EgressGuardConfig {
        max_pooled_clients: 0,
        ..EgressGuardConfig::default()
    })
    .await;
    off.get("a.test", Some(1)).await;
    off.get("a.test", Some(1)).await;
    assert_eq!(off.guard.pooled_clients(), 0);
    assert_eq!(off.server.connections(), 2, "without a pool every fetch dials anew");
}
