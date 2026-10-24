//! Review handoff B5.2 (SECURITY.md SEC-39..42): key-auth failures on the public listener are
//! limited like the control listener's (one shared per-client window), the limiter keys on the
//! real client behind trusted proxies (`ClientIp`), the internal Unix socket admits only allowed
//! peer UIDs, and a debug build without a root token refuses a non-loopback control address.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use rt_sec::{
    check_open_control_bind, check_root_token, control_open_without_token, limiter_key, AuthError, AuthFailureLimiter,
    Authenticator, ClientIp, DefaultAuthenticator, KeyHasher, KeyLookup, KeySpec, MasterKey, Principal, UnixPeerPolicy,
    WallClock,
};
use rt_types::{ApiKey, KeyId, Listener, Scope, TenantId, UnixMs};

const ROOT: &str = "root-token-0123456789abcdef-ABCDEF";
const NOW: UnixMs = 1_800_000_000_000;

#[derive(Default)]
struct Store {
    keys: Mutex<Vec<ApiKey>>,
}

impl KeyLookup for Store {
    fn find(&self, id: KeyId) -> Option<ApiKey> {
        self.keys.lock().unwrap().iter().find(|k| k.id == id).cloned()
    }
}

struct Clock(AtomicI64);

impl WallClock for Clock {
    fn now_ms(&self) -> UnixMs {
        self.0.load(Ordering::SeqCst)
    }
}

struct Fixture {
    auth: DefaultAuthenticator,
    clock: Arc<Clock>,
    valid_key: String,
}

fn fixture() -> Fixture {
    let store = Arc::new(Store::default());
    let clock = Arc::new(Clock(AtomicI64::new(NOW)));
    let hasher = KeyHasher::new(&MasterKey::from_bytes(std::array::from_fn(|i| 0x40 + i as u8)));
    let spec = KeySpec {
        name: None,
        scopes: [Scope::Invoke].into_iter().collect(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
    };
    let (secret, new) = hasher.issue(spec);
    store.keys.lock().unwrap().push(ApiKey {
        id: new.id,
        tenant_id: TenantId::new(),
        name: None,
        scopes: new.scopes,
        project_ids: None,
        environments: None,
        expires_at_ms: None,
        created_at_ms: NOW,
        last_used_at_ms: None,
        revoked: false,
        parent_key_id: None,
        hmac: new.hmac,
    });
    let auth = DefaultAuthenticator::new(Some(ROOT.to_string()), hasher, store, clock.clone());
    Fixture {
        auth,
        clock,
        valid_key: secret.expose().to_string(),
    }
}

/// A request from socket peer `peer` whose real client (after the trusted-proxy walk) is
/// `client` (when given), carrying `headers`.
fn req(peer: &str, client: Option<&str>, headers: &[(&str, &str)]) -> http::request::Parts {
    let mut b = http::Request::builder().uri("/s/svc/");
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let (mut parts, ()) = b.body(()).unwrap().into_parts();
    let peer: SocketAddr = format!("{peer}:4000").parse().unwrap();
    parts.extensions.insert(peer);
    if let Some(c) = client {
        parts.extensions.insert(ClientIp(c.parse::<IpAddr>().unwrap()));
    }
    parts
}

const WRONG_KEY: &str = "rtk_01J0000000000000000000000Z_wrongwrongwrongwrongwrongwrongwr";

#[test]
fn ac_sec_39_wrong_invoke_keys_on_the_public_listener_are_rate_limited_per_client() {
    let f = fixture();
    let wrong = [("x-rt-key", WRONG_KEY)];
    for i in 0..10 {
        let p = f.auth.authenticate(&req("203.0.113.7", None, &wrong), Listener::Public);
        assert_eq!(
            p,
            Ok(Principal::Anonymous),
            "failure #{} is a plain 401 for the invoke path",
            i + 1
        );
    }
    let err = f
        .auth
        .authenticate(&req("203.0.113.7", None, &wrong), Listener::Public)
        .expect_err("11th failure");
    assert!(matches!(err, AuthError::RateLimited { .. }), "{err:?}");
    assert_eq!(err.http_status(), 429);

    // The client stays blocked, even with a valid key, until the window ends…
    let good = [("x-rt-key", f.valid_key.as_str())];
    assert!(matches!(
        f.auth.authenticate(&req("203.0.113.7", None, &good), Listener::Public),
        Err(AuthError::RateLimited { .. })
    ));
    // …while another client is unaffected.
    assert!(matches!(
        f.auth.authenticate(&req("203.0.113.8", None, &good), Listener::Public),
        Ok(Principal::Tenant { .. })
    ));
    f.clock.0.fetch_add(61_000, Ordering::SeqCst);
    assert!(matches!(
        f.auth.authenticate(&req("203.0.113.7", None, &good), Listener::Public),
        Ok(Principal::Tenant { .. })
    ));
}

#[test]
fn the_guests_own_authorization_header_never_counts_as_a_key_failure() {
    let f = fixture();
    for _ in 0..30 {
        let p = f.auth.authenticate(
            &req("203.0.113.9", None, &[("authorization", "Bearer some-app-token")]),
            Listener::Public,
        );
        assert_eq!(p, Ok(Principal::Anonymous));
    }
    let good = [("x-rt-key", f.valid_key.as_str())];
    assert!(matches!(
        f.auth.authenticate(&req("203.0.113.9", None, &good), Listener::Public),
        Ok(Principal::Tenant { .. })
    ));
}

#[test]
fn ac_sec_39_public_key_failures_never_lock_the_operator_out_of_control() {
    let f = fixture();
    // A shared address (CGNAT, an office) burns its public budget with wrong invoke keys…
    for _ in 0..12 {
        let _ = f
            .auth
            .authenticate(&req("198.51.100.4", None, &[("x-rt-key", WRONG_KEY)]), Listener::Public);
    }
    let blocked = f
        .auth
        .authenticate(&req("198.51.100.4", None, &[("x-rt-key", WRONG_KEY)]), Listener::Public);
    assert!(matches!(blocked, Err(AuthError::RateLimited { .. })));
    // …but the control budget is separate: the operator behind the same address still gets in.
    let root = format!("Bearer {ROOT}");
    let r = f.auth.authenticate(
        &req("198.51.100.4", None, &[("authorization", root.as_str())]),
        Listener::Control,
    );
    assert_eq!(r, Ok(Principal::Operator));
    for _ in 0..10 {
        let r = f.auth.authenticate(
            &req("198.51.100.4", None, &[("authorization", "Bearer wrong")]),
            Listener::Control,
        );
        assert_eq!(r, Err(AuthError::Invalid), "the control budget starts fresh");
    }
}

#[test]
fn ipv6_clients_are_counted_per_64() {
    let l = AuthFailureLimiter::new(3);
    let a: IpAddr = "2001:db8:1:2::1".parse().unwrap();
    let b: IpAddr = "2001:db8:1:2:ffff::9".parse().unwrap();
    let other: IpAddr = "2001:db8:1:3::1".parse().unwrap();
    assert_eq!(limiter_key(a), limiter_key(b), "same /64");
    assert_ne!(limiter_key(a), limiter_key(other));
    for i in 0..3 {
        // rotating addresses inside one /64 does not reset the budget
        let ip: IpAddr = format!("2001:db8:1:2::{}", i + 10).parse().unwrap();
        assert_eq!(l.record_failure(ip, NOW + i), Ok(()));
    }
    assert!(l.record_failure(b, NOW + 5).is_err());
    assert!(l.check(a, NOW + 6).is_err());
    assert_eq!(l.check(other, NOW + 6), Ok(()), "the neighbouring /64 is unaffected");
    let mapped: IpAddr = "::ffff:203.0.113.5".parse().unwrap();
    assert_eq!(limiter_key(mapped), "203.0.113.5".parse::<IpAddr>().unwrap());
}

#[test]
fn the_failure_table_is_hard_capped_and_keeps_blocked_clients() {
    let l = AuthFailureLimiter::with_capacity(2, 64);
    let attacker: IpAddr = "203.0.113.200".parse().unwrap();
    for i in 0..3 {
        let _ = l.record_failure(attacker, NOW + i);
    }
    assert!(l.check(attacker, NOW + 10).is_err());
    // Flood with 10,000 distinct addresses: the table never grows past its capacity.
    for i in 0..10_000u32 {
        let ip = IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + i));
        let _ = l.record_failure(ip, NOW + 20);
    }
    assert!(l.len() <= 64, "capped: {}", l.len());
    assert!(
        l.check(attacker, NOW + 21).is_err(),
        "eviction prefers entries that block nobody"
    );
    // Expired windows are swept incrementally (no full scan per failure) and on demand.
    l.sweep(NOW + 61_000);
    assert!(l.is_empty());
}

#[test]
fn ac_sec_40_the_limiter_keys_on_the_real_client_behind_a_trusted_proxy() {
    let f = fixture();
    let proxy = "10.0.0.1";
    let bad = [("authorization", "Bearer wrong")];
    for _ in 0..11 {
        let _ = f
            .auth
            .authenticate(&req(proxy, Some("203.0.113.50"), &bad), Listener::Control);
    }
    assert!(matches!(
        f.auth
            .authenticate(&req(proxy, Some("203.0.113.50"), &bad), Listener::Control),
        Err(AuthError::RateLimited { .. })
    ));
    // Another client behind the same proxy is not locked out.
    assert_eq!(
        f.auth
            .authenticate(&req(proxy, Some("203.0.113.51"), &bad), Listener::Control),
        Err(AuthError::Invalid)
    );
    let root = format!("Bearer {ROOT}");
    assert_eq!(
        f.auth.authenticate(
            &req(proxy, Some("203.0.113.51"), &[("authorization", root.as_str())]),
            Listener::Control
        ),
        Ok(Principal::Operator)
    );
}

#[test]
fn ac_sec_41_the_internal_socket_admits_the_nodes_own_uid_and_listed_ones_only() {
    let own = UnixPeerPolicy::own_uid();
    let p = UnixPeerPolicy::node_default(&[]);
    assert!(p.allows(own));
    assert!(!p.allows(own.wrapping_add(1)));
    if own != 0 {
        assert!(!p.allows(0), "root is not implied");
    }
    let listed = UnixPeerPolicy::node_default(&[0, 4242]);
    assert!(listed.allows(own) && listed.allows(0) && listed.allows(4242));
    assert!(
        !UnixPeerPolicy::exact(&[4242]).allows(own),
        "an exact policy names every admitted UID"
    );
}

#[test]
fn ac_sec_42_an_open_control_api_must_bind_loopback() {
    assert!(!control_open_without_token(true), "a root token closes it");
    assert_eq!(
        control_open_without_token(false),
        cfg!(debug_assertions),
        "open only in debug builds"
    );
    for ok in [
        "127.0.0.1:8081",
        "127.0.0.1:0",
        "[::1]:8081",
        "localhost:8081",
        "127.1.2.3:9",
    ] {
        assert!(check_open_control_bind(ok).is_ok(), "{ok}");
    }
    for bad in [
        "0.0.0.0:8081",
        "[::]:8081",
        "10.0.0.5:8081",
        "example.com:8081",
        "",
        "8081",
    ] {
        let err = check_open_control_bind(bad).expect_err(bad);
        assert!(err.contains("RT_ROOT_TOKEN"), "the error says how to fix it: {err}");
    }
}

/// The `.env.example` placeholder is never a root token, and a token reachable from outside the
/// host must be long enough not to be guessed (security review finding 2).
#[test]
fn the_placeholder_or_a_short_root_token_is_refused() {
    let long = "a".repeat(32);
    assert!(check_root_token(&long, "0.0.0.0:8081").is_ok());
    assert!(
        check_root_token("short", "127.0.0.1:8081").is_ok(),
        "short tokens stay fine on loopback (tests, dev)"
    );
    for (token, addr) in [
        (
            "change-me-8f3a1c5e97d2406b1ae0f7c3d95b28e41067a9cd3b5f82e1704ac6d9e3f05b72",
            "127.0.0.1:8081",
        ),
        ("CHANGE-ME-please", "127.0.0.1:8081"),
        ("change-me", "0.0.0.0:8081"),
        ("thirty-one-characters-long-tokn", "0.0.0.0:8081"),
        ("short", "[::]:8081"),
    ] {
        let err = check_root_token(token, addr).expect_err(token);
        assert!(
            err.contains("RT_ROOT_TOKEN") && err.contains("openssl rand -hex 32"),
            "{err}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Edge cases (edge-cases.md §1.7 ingress)
// ---------------------------------------------------------------------------------------------

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

/// INGRESS-003: proxies that write `ip:port` or a bracketed IPv6 (`[v6]`, `[v6]:port`) into
/// `X-Forwarded-For` still name their client; a hop that is no address at all stops the walk at
/// the last parsed hop (never the garbage itself).
#[test]
fn ingress_003_forwarded_hops_with_ports_or_brackets_are_parsed() {
    let proxies = rt_sec::TrustedProxies::parse(&["10.0.0.0/8"]).unwrap();
    let peer = ip("10.0.0.1");
    assert_eq!(proxies.client_ip(peer, &["1.2.3.4:5678"]), ip("1.2.3.4"));
    assert_eq!(proxies.client_ip(peer, &["[2001:db8::1]"]), ip("2001:db8::1"));
    assert_eq!(proxies.client_ip(peer, &["[2001:db8::1]:443"]), ip("2001:db8::1"));
    assert_eq!(
        proxies.client_ip(peer, &["7.7.7.7, 10.0.0.9:80"]),
        ip("7.7.7.7"),
        "a trusted hop with a port is skipped"
    );
    assert_eq!(
        proxies.client_ip(peer, &["7.7.7.7, unknown"]),
        peer,
        "garbage stops the walk"
    );
    assert_eq!(proxies.client_ip(peer, &["7.7.7.7, 10.0.0.9, nope"]), peer);
    assert_eq!(
        proxies.client_ip(peer, &["7.7.7.7, nope, 10.0.0.9"]),
        ip("10.0.0.9"),
        "the last parsed hop"
    );
}

/// INGRESS-004: when every hop is a trusted proxy the left-most one is the client.
#[test]
fn ingress_004_an_all_trusted_chain_yields_the_left_most_hop() {
    let proxies = rt_sec::TrustedProxies::parse(&["10.0.0.0/8"]).unwrap();
    assert_eq!(
        proxies.client_ip(ip("10.0.0.1"), &["10.1.1.1, 10.2.2.2"]),
        ip("10.1.1.1")
    );
}

/// INGRESS-005: several `X-Forwarded-For` lines are one chain in order; a line the attacker sent
/// before the proxy's own line cannot replace the right-most untrusted hop.
#[test]
fn ingress_005_an_attackers_own_forwarded_line_cannot_win() {
    let proxies = rt_sec::TrustedProxies::parse(&["10.0.0.0/8"]).unwrap();
    let client = proxies.client_ip(ip("10.0.0.1"), &["6.6.6.6", "8.8.4.4"]);
    assert_eq!(client, ip("8.8.4.4"));
    let client = proxies.client_ip(ip("203.0.113.5"), &["6.6.6.6"]);
    assert_eq!(client, ip("203.0.113.5"), "an untrusted peer's header is ignored");
}

/// INGRESS-006: CIDRs with an out-of-range prefix are malformed.
#[test]
fn ingress_006_out_of_range_prefixes_are_malformed() {
    for bad in ["10.0.0.0/33", "::/129", "10.0.0.0/-1", "10.0.0/8", "1.2.3.4/"] {
        assert!(rt_sec::parse_cidr(bad).is_err(), "{bad}");
        assert!(rt_sec::IpAllowList::parse(&[bad]).is_err(), "{bad}");
    }
    assert!(rt_sec::parse_cidr("10.0.0.0/32").is_ok() && rt_sec::parse_cidr("::/128").is_ok());
}
