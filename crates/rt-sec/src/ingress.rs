//! Ingress rule evaluation (SECURITY.md, SEC-19, SEC-35): `ip_allow` CIDR matching
//! for IPv4 and IPv6, client-address resolution behind trusted proxies, and the peer-UID policy
//! of the internal Unix socket (SEC-41).
//!
//! Pure functions; the HTTP layer supplies the peer address, `X-Forwarded-For` and peer
//! credentials.

use std::net::IpAddr;

use ipnet::IpNet;
use thiserror::Error;

/// `::ffff:a.b.c.d` ⇒ `a.b.c.d`, so IPv4 rules apply to mapped peers.
fn canonical(ip: IpAddr) -> IpAddr {
    ip.to_canonical()
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("invalid CIDR `{0}`")]
pub struct CidrParseError(pub String);

/// Parse `1.2.3.0/24`, `2001:db8::/32`, or a bare address (host route).
pub fn parse_cidr(s: &str) -> Result<IpNet, CidrParseError> {
    let t = s.trim();
    if let Ok(net) = t.parse::<IpNet>() {
        return Ok(net);
    }
    t.parse::<IpAddr>()
        .map(IpNet::from)
        .map_err(|_| CidrParseError(s.to_string()))
}

/// A service's `ingress.ip_allow` list. Empty = everyone is allowed; otherwise only addresses
/// inside one of the CIDRs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IpAllowList {
    nets: Vec<IpNet>,
    /// A rule list was configured (even if no entry parsed): everything else is denied.
    restrictive: bool,
}

impl IpAllowList {
    /// Parse every entry; the first malformed one is an error (reject at config time).
    pub fn parse<S: AsRef<str>>(rules: &[S]) -> Result<Self, CidrParseError> {
        let nets = rules
            .iter()
            .map(|r| parse_cidr(r.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            nets,
            restrictive: !rules.is_empty(),
        })
    }

    /// Lenient variant for stored config: malformed entries are dropped. A non-empty rule list
    /// whose entries are all malformed therefore matches nobody (fails closed).
    pub fn parse_lossy<S: AsRef<str>>(rules: &[S]) -> Self {
        let nets = rules.iter().filter_map(|r| parse_cidr(r.as_ref()).ok()).collect();
        Self {
            nets,
            restrictive: !rules.is_empty(),
        }
    }

    /// No rules configured (everyone allowed).
    pub fn is_unrestricted(&self) -> bool {
        !self.restrictive
    }

    pub fn allows(&self, ip: IpAddr) -> bool {
        if !self.restrictive {
            return true;
        }
        let canon = canonical(ip);
        self.nets.iter().any(|n| n.contains(&canon) || n.contains(&ip))
    }
}

/// Convenience: is `ip` allowed by the raw `ip_allow` strings? Malformed entries never match
/// (fail closed when the list is non-empty).
pub fn ip_allowed<S: AsRef<str>>(ip: IpAddr, rules: &[S]) -> bool {
    if rules.is_empty() {
        return true;
    }
    let canon = canonical(ip);
    rules
        .iter()
        .filter_map(|r| parse_cidr(r.as_ref()).ok())
        .any(|n| n.contains(&canon) || n.contains(&ip))
}

/// `ingress.trusted_proxies`: CIDRs whose `X-Forwarded-For` is believed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrustedProxies {
    nets: Vec<IpNet>,
}

impl TrustedProxies {
    pub fn parse<S: AsRef<str>>(rules: &[S]) -> Result<Self, CidrParseError> {
        Ok(Self {
            nets: rules.iter().map(|r| parse_cidr(r.as_ref())).collect::<Result<_, _>>()?,
        })
    }

    pub fn is_trusted(&self, ip: IpAddr) -> bool {
        let canon = canonical(ip);
        self.nets.iter().any(|n| n.contains(&canon) || n.contains(&ip))
    }

    /// The effective client address. Without a trusted peer the peer itself is the client and
    /// `X-Forwarded-For` is ignored (spoofing it must not satisfy `ip_allow`). With a trusted
    /// peer, the header chain is walked right to left, skipping trusted proxies; the first
    /// untrusted (or unparsable ⇒ stop) entry is the client.
    pub fn client_ip(&self, peer: IpAddr, forwarded_for: &[&str]) -> IpAddr {
        let peer = canonical(peer);
        if !self.is_trusted(peer) {
            return peer;
        }
        let hops: Vec<&str> = forwarded_for
            .iter()
            .flat_map(|v| v.split(','))
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .collect();
        let mut client = peer;
        for hop in hops.iter().rev() {
            match parse_hop(hop) {
                Some(ip) => {
                    client = canonical(ip);
                    if !self.is_trusted(client) {
                        break;
                    }
                }
                None => break,
            }
        }
        client
    }
}

/// One `X-Forwarded-For` hop: an address, `a.b.c.d:port`, `[v6]` or `[v6]:port` (proxies that
/// add the client's port write these forms, INGRESS-003). Anything else is `None`.
fn parse_hop(hop: &str) -> Option<IpAddr> {
    if let Ok(ip) = hop.parse::<IpAddr>() {
        return Some(ip);
    }
    if let Some(rest) = hop.strip_prefix('[') {
        let (addr, tail) = rest.split_once(']')?;
        let port_ok = tail.is_empty()
            || tail
                .strip_prefix(':')
                .is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
        return if port_ok {
            addr.parse::<std::net::Ipv6Addr>().ok().map(IpAddr::V6)
        } else {
            None
        };
    }
    hop.parse::<std::net::SocketAddrV4>().ok().map(|a| IpAddr::V4(*a.ip()))
}

/// Which peers may use the internal Unix socket (`listeners.internal_allowed_uids`, SEC-41).
///
/// The internal listener trusts its callers (no invoke access check, `x-rt-invoker` honored), so
/// the socket checks the connecting process's credentials (`SO_PEERCRED` on Linux, `getpeereid`
/// on macOS) before serving it. The node's own effective UID is always admitted; any other UID —
/// root included — only when listed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnixPeerPolicy {
    uids: Vec<u32>,
}

impl UnixPeerPolicy {
    /// The node's own UID plus `extra` (`listeners.internal_allowed_uids`).
    pub fn node_default(extra: &[u32]) -> Self {
        let own = Self::own_uid();
        let mut uids = vec![own];
        uids.extend(extra.iter().copied().filter(|u| *u != own));
        Self { uids }
    }

    /// Exactly `uids` (tests, embedders).
    pub fn exact(uids: &[u32]) -> Self {
        Self { uids: uids.to_vec() }
    }

    /// Effective UID of this process.
    pub fn own_uid() -> u32 {
        // SAFETY: `geteuid` has no preconditions and cannot fail.
        unsafe { libc::geteuid() }
    }

    pub fn allows(&self, uid: u32) -> bool {
        self.uids.contains(&uid)
    }

    /// The admitted UIDs.
    pub fn uids(&self) -> &[u32] {
        &self.uids
    }
}
