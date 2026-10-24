//! Fixed-window rate limits. The counters live in `std:host/kv`: `rl/ip/<client>/<window start>`
//! per client address (checked before the token) and `rl/sub/<sub>/<window start>` per verified
//! subject. `kv::incr` adds 1 atomically and returns the new count, so concurrent requests never
//! share a count and the limit is exact within a window. The TTL is set when the increment creates
//! the key, so old windows expire on their own.
//!
//! A fixed window still admits up to twice the limit across a window boundary (the end of one
//! window plus the start of the next). The service's `ingress.rate_limit` is the global cap.

use std::net::IpAddr;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Window {
    pub start: u64,
    pub reset_in: u64,
}

pub fn window(now: u64, size: u64) -> Window {
    let size = size.max(1);
    let start = now - now % size;
    Window {
        start,
        reset_in: start + size - now,
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Decision {
    pub allowed: bool,
    pub limit: u64,
    pub remaining: u64,
    pub reset_in: u64,
}

/// `count` = what `kv::incr` returned: the requests in this window, this one included.
pub fn decide(count: u64, limit: u64, w: Window) -> Decision {
    Decision {
        allowed: count <= limit,
        limit,
        remaining: limit.saturating_sub(count),
        reset_in: w.reset_in,
    }
}

pub fn sub_key(sub: &str, w: Window) -> Vec<u8> {
    format!("rl/sub/{sub}/{}", w.start).into_bytes()
}

pub fn ip_key(ip: IpAddr, w: Window) -> Vec<u8> {
    format!("rl/ip/{}/{}", client_bucket(ip), w.start).into_bytes()
}

/// What a per-address limit counts: an IPv4 address, or the /64 of an IPv6 address (a subscriber
/// usually holds a whole /64, so counting single IPv6 addresses would be trivial to dodge). An
/// IPv4-mapped IPv6 address counts as its IPv4 address.
pub fn client_bucket(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => {
                let s = v6.segments();
                format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows() {
        assert_eq!(
            window(125, 60),
            Window {
                start: 120,
                reset_in: 55
            }
        );
        assert_eq!(
            window(120, 60),
            Window {
                start: 120,
                reset_in: 60
            }
        );
        assert_eq!(window(7, 0), Window { start: 7, reset_in: 1 });
    }

    #[test]
    fn decisions() {
        let w = window(125, 60);
        assert_eq!(
            decide(1, 3, w),
            Decision {
                allowed: true,
                limit: 3,
                remaining: 2,
                reset_in: 55
            }
        );
        assert_eq!(decide(3, 3, w).remaining, 0);
        assert!(decide(3, 3, w).allowed, "the limit-th request passes");
        assert!(!decide(4, 3, w).allowed, "one more does not");
        assert_eq!(
            decide(40, 3, w).remaining,
            0,
            "denied requests count too, remaining stays 0"
        );
        assert_eq!(sub_key("alice", w), b"rl/sub/alice/120".to_vec());
    }

    #[test]
    fn client_buckets() {
        let w = window(125, 60);
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        assert_eq!(ip_key(ip("203.0.113.7"), w), b"rl/ip/203.0.113.7/120".to_vec());
        // every address of one IPv6 /64 shares a counter
        assert_eq!(client_bucket(ip("2001:db8:1:2::1")), "2001:db8:1:2::/64");
        assert_eq!(
            client_bucket(ip("2001:db8:1:2:aaaa:bbbb:cccc:dddd")),
            client_bucket(ip("2001:db8:1:2::1"))
        );
        assert_ne!(
            client_bucket(ip("2001:db8:1:3::1")),
            client_bucket(ip("2001:db8:1:2::1"))
        );
        assert_eq!(client_bucket(ip("::ffff:198.51.100.4")), "198.51.100.4");
        assert_eq!(client_bucket(ip("::1")), "0:0:0:0::/64");
    }
}
