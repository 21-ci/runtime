//! SSRF classification (ported from the worker's `egress_guard.rs`, SECURITY.md).
//!
//! The actual guarded HTTP client lives in `rt-host`; this module holds the pure, security
//! critical decisions so they can be tested exhaustively (SEC-10, SEC-24).

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use std::sync::Arc;

use ipnet::IpNet;
use parking_lot::{Mutex, RwLock};

/// True if `ip` must never be reachable from guest-initiated outbound HTTP: loopback, RFC 1918,
/// link-local (incl. `169.254.169.254`), CGNAT `100.64/10`, IETF protocol assignments
/// `192.0.0/24`, benchmarking `198.18/15`, reserved `240/4`, multicast, unspecified, broadcast,
/// IPv6 loopback/ULA/link-local/site-local/multicast/unspecified, Teredo `2001::/32`, and IPv6
/// forms that embed an IPv4 address (IPv4-mapped `::ffff:a.b.c.d`, 6to4 `2002:aabb:ccdd::/48`)
/// judged by their IPv4 rules.
pub fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_blocked_v4(mapped);
            }
            is_blocked_v6(v6)
        }
    }
}

fn is_blocked_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || o[0] == 0 // 0.0.0.0/8 "this network"
        || (o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000) // 100.64.0.0/10 CGNAT
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 IETF protocol assignments
        || (o[0] == 198 && (o[1] & 0b1111_1110) == 18) // 198.18.0.0/15 benchmarking
        || o[0] >= 240 // 240.0.0.0/4 reserved (incl. broadcast)
}

fn is_blocked_v6(ip: Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
        return true;
    }
    let seg = ip.segments();
    if seg[0] == 0x2002 {
        // 6to4: the relay delivers to the IPv4 address in bits 16..48.
        let v4 = Ipv4Addr::new((seg[1] >> 8) as u8, seg[1] as u8, (seg[2] >> 8) as u8, seg[2] as u8);
        return is_blocked_v4(v4);
    }
    (seg[0] & 0xfe00) == 0xfc00 // fc00::/7 ULA
        || (seg[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        || (seg[0] & 0xffc0) == 0xfec0 // fec0::/10 site-local (deprecated, still routed by some stacks)
        || (seg[0] == 0x2001 && seg[1] == 0) // 2001::/32 Teredo (tunnels to an arbitrary IPv4)
}

/// Per-service host allowlist: empty = any host; otherwise exact or dot-suffix match,
/// case-insensitive, ignoring a trailing dot.
pub fn host_allowed(host: &str, allowed_hosts: &[String]) -> bool {
    if allowed_hosts.is_empty() {
        return true;
    }
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    allowed_hosts.iter().any(|entry| {
        let entry = entry.trim_end_matches('.').to_ascii_lowercase();
        host == entry || host.ends_with(&format!(".{entry}"))
    })
}

/// Hosts the guest may never reach regardless of allowlists (`egress.denied_hosts` and
/// per-service `denied_hosts`): exact or dot-suffix match.
pub fn host_denied(host: &str, denied_hosts: &[String]) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    denied_hosts.iter().any(|entry| {
        let entry = entry.trim_end_matches('.').to_ascii_lowercase();
        host == entry || host.ends_with(&format!(".{entry}"))
    })
}

/// Addresses of everything the node itself uses (listeners, S3 endpoint, OTLP endpoint, binding
/// `base_url`s) plus `egress.denied_cidrs`. Re-resolved every 5 minutes.
pub trait EgressDenylist: Send + Sync {
    fn contains(&self, ip: IpAddr) -> bool;
    /// Re-resolve configured hosts. Blocking DNS: call from the blocking pool.
    fn refresh(&self);
}

/// Blocking name resolution used by [`SelfDenylist::refresh`] for a `host` or `host:port` entry
/// (never called for literal addresses). Injectable so tests can simulate DNS outages.
pub type HostResolver = Arc<dyn Fn(&str) -> std::io::Result<Vec<IpAddr>> + Send + Sync>;

/// The system resolver (`getaddrinfo` through `ToSocketAddrs`).
fn system_resolve(host: &str) -> std::io::Result<Vec<IpAddr>> {
    let target = if host.contains(':') {
        host.to_string()
    } else {
        format!("{host}:0")
    };
    Ok(target.to_socket_addrs()?.map(|a| a.ip()).collect())
}

/// Default [`EgressDenylist`].
pub struct SelfDenylist {
    /// `host` or `host:port` strings to resolve (`api.internal:8000`, `s3.example.com`).
    hosts: RwLock<Vec<String>>,
    cidrs: Vec<IpNet>,
    /// Literal addresses added with [`SelfDenylist::add_addrs`]; kept across refreshes.
    fixed: RwLock<Arc<Vec<IpAddr>>>,
    resolved: RwLock<Arc<Vec<IpAddr>>>,
    /// The last successful answer per host: a failed lookup keeps the previous addresses, so a
    /// DNS outage never re-opens the node's own endpoints to guests.
    last_good: Mutex<HashMap<String, Vec<IpAddr>>>,
    resolver: HostResolver,
}

/// Every address configured on this host's network interfaces (`getifaddrs`). A listener bound
/// to `0.0.0.0`/`::` is reachable on all of them, so the self-denylist must hold them all, not
/// only the wildcard address the listener reports.
pub fn local_interface_addrs() -> Vec<IpAddr> {
    let mut out = Vec::new();
    #[cfg(unix)]
    if let Ok(interfaces) = nix::ifaddrs::getifaddrs() {
        for address in interfaces.filter_map(|i| i.address) {
            if let Some(v4) = address.as_sockaddr_in() {
                out.push(IpAddr::V4(v4.ip()));
            } else if let Some(v6) = address.as_sockaddr_in6() {
                out.push(IpAddr::V6(v6.ip()));
            }
        }
    }
    out
}

/// The IPv4 address an IPv6 address delivers to, so one host cannot be reached under two
/// spellings: IPv4-mapped `::ffff:a.b.c.d`, IPv4-compatible `::a.b.c.d` (not `::` / `::1`),
/// NAT64 well-known prefix `64:ff9b::a.b.c.d` (RFC 6052) and 6to4 `2002:aabb:ccdd::/48`. The
/// local-use NAT64 prefix `64:ff9b:1::/48` (RFC 8215) has no fixed embedding; the egress guard
/// blocks it outright.
pub fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4);
    }
    let seg = v6.segments();
    let low = || Ipv4Addr::new((seg[6] >> 8) as u8, seg[6] as u8, (seg[7] >> 8) as u8, seg[7] as u8);
    if seg[..6] == [0, 0, 0, 0, 0, 0] {
        return (seg[6] != 0).then(low); // `::` and `::1` stay IPv6
    }
    if seg[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return Some(low());
    }
    if seg[0] == 0x2002 {
        return Some(Ipv4Addr::new(
            (seg[1] >> 8) as u8,
            seg[1] as u8,
            (seg[2] >> 8) as u8,
            seg[2] as u8,
        ));
    }
    None
}

/// An IPv6 address embedding an IPv4 one ⇒ that IPv4 address (see [`embedded_ipv4`]).
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => embedded_ipv4(v6).map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

impl SelfDenylist {
    /// `hosts` are resolved on [`EgressDenylist::refresh`]; `cidrs` match statically.
    pub fn new(hosts: Vec<String>, cidrs: Vec<IpNet>) -> Self {
        Self {
            hosts: RwLock::new(hosts),
            cidrs,
            fixed: RwLock::new(Arc::new(Vec::new())),
            resolved: RwLock::new(Arc::new(Vec::new())),
            last_good: Mutex::new(HashMap::new()),
            resolver: Arc::new(system_resolve),
        }
    }

    /// Resolve names with `resolver` instead of the system resolver.
    pub fn with_resolver(mut self, resolver: HostResolver) -> Self {
        self.resolver = resolver;
        self
    }

    /// Replace the endpoint list (config reload). Takes effect on the next refresh, which also
    /// drops the addresses of hosts no longer listed.
    pub fn set_hosts(&self, hosts: Vec<String>) {
        *self.hosts.write() = hosts;
    }

    /// The configured endpoint list.
    pub fn hosts(&self) -> Vec<String> {
        self.hosts.read().clone()
    }

    /// Add literal addresses immediately (tests, listener addresses known at bind time). They
    /// survive [`EgressDenylist::refresh`].
    pub fn add_addrs(&self, addrs: impl IntoIterator<Item = IpAddr>) {
        let mut g = self.fixed.write();
        let mut v: Vec<IpAddr> = g.as_ref().clone();
        v.extend(addrs.into_iter().map(canonical));
        v.sort_unstable();
        v.dedup();
        *g = Arc::new(v);
    }
}

impl EgressDenylist for SelfDenylist {
    fn contains(&self, ip: IpAddr) -> bool {
        let canon = canonical(ip);
        self.cidrs.iter().any(|c| c.contains(&canon) || c.contains(&ip))
            || self.fixed.read().contains(&canon)
            || self.resolved.read().contains(&canon)
    }

    fn refresh(&self) {
        let hosts = self.hosts.read().clone();
        // Held across the lookups: concurrent refreshes serialize (readers never take it).
        let mut last = self.last_good.lock();
        let mut next: HashMap<String, Vec<IpAddr>> = HashMap::with_capacity(hosts.len());
        for h in hosts {
            if let Ok(ip) = h.parse::<IpAddr>() {
                next.insert(h, vec![canonical(ip)]);
                continue;
            }
            match (self.resolver)(&h) {
                Ok(ips) if !ips.is_empty() => {
                    next.insert(h, ips.into_iter().map(canonical).collect());
                }
                _ => match last.get(&h) {
                    Some(prev) => {
                        tracing::warn!(host = %h, "self-denylist: lookup failed; keeping the previous addresses");
                        let prev = prev.clone();
                        next.insert(h, prev);
                    }
                    None => tracing::debug!(host = %h, "self-denylist: lookup failed"),
                },
            }
        }
        let mut v: Vec<IpAddr> = next.values().flatten().copied().collect();
        v.sort_unstable();
        v.dedup();
        *last = next;
        *self.resolved.write() = Arc::new(v);
    }
}
