//! SSRF classification (SECURITY.md, SEC-10, SEC-11, SEC-12, SEC-24, SEC-35): which
//! addresses and hosts guest-initiated outbound HTTP may never reach.

use std::net::IpAddr;

use ipnet::IpNet;
use rt_sec::{host_allowed, host_denied, is_blocked_ip, EgressDenylist, SelfDenylist};

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap_or_else(|e| panic!("bad test address {s}: {e}"))
}

fn net(s: &str) -> IpNet {
    s.parse().unwrap_or_else(|e| panic!("bad test cidr {s}: {e}"))
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

// ---------------------------------------------------------------------------------------------
// is_blocked_ip corpus (SEC-10)
// ---------------------------------------------------------------------------------------------

/// (address, why it must be blocked)
const BLOCKED: &[(&str, &str)] = &[
    // loopback 127.0.0.0/8
    ("127.0.0.1", "loopback"),
    ("127.0.0.2", "loopback"),
    ("127.1.2.3", "loopback"),
    ("127.255.255.254", "loopback upper end"),
    // RFC 1918
    ("10.0.0.0", "10/8 first"),
    ("10.0.0.1", "10/8"),
    ("10.255.255.255", "10/8 last"),
    ("172.16.0.0", "172.16/12 first"),
    ("172.16.0.1", "172.16/12"),
    ("172.20.10.5", "172.16/12 middle"),
    ("172.31.255.255", "172.16/12 last"),
    ("192.168.0.0", "192.168/16 first"),
    ("192.168.1.1", "192.168/16"),
    ("192.168.255.255", "192.168/16 last"),
    // link-local incl. cloud metadata
    ("169.254.0.0", "link-local first"),
    ("169.254.169.254", "cloud metadata endpoint"),
    ("169.254.170.2", "ECS task metadata"),
    ("169.254.255.255", "link-local last"),
    // CGNAT 100.64.0.0/10
    ("100.64.0.0", "CGNAT first"),
    ("100.64.0.1", "CGNAT"),
    ("100.100.100.200", "Alibaba metadata (CGNAT)"),
    ("100.127.255.255", "CGNAT last"),
    // unspecified / this-network / broadcast / multicast
    ("0.0.0.0", "unspecified"),
    ("0.0.0.1", "0/8 this network"),
    ("0.255.255.255", "0/8 last"),
    ("255.255.255.255", "broadcast"),
    ("224.0.0.1", "multicast"),
    ("224.0.0.251", "mDNS"),
    ("239.255.255.250", "SSDP multicast"),
    // IPv6
    ("::1", "v6 loopback"),
    ("::", "v6 unspecified"),
    ("fc00::", "ULA first"),
    ("fc00::1", "ULA"),
    ("fd00::1", "ULA"),
    ("fd12:3456:789a::1", "ULA"),
    ("fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ULA last"),
    ("fe80::1", "link-local"),
    ("fe80::", "link-local first"),
    ("febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "link-local last (fe80::/10)"),
    ("ff00::1", "multicast"),
    ("ff02::1", "all nodes"),
    ("ff0e::1", "global multicast"),
    // IPv4-mapped IPv6 (`[::ffff:127.0.0.1]`) is judged by its IPv4 rules
    ("::ffff:127.0.0.1", "mapped loopback"),
    ("::ffff:10.0.0.1", "mapped 10/8"),
    ("::ffff:172.16.5.5", "mapped 172.16/12"),
    ("::ffff:192.168.1.1", "mapped 192.168/16"),
    ("::ffff:169.254.169.254", "mapped metadata"),
    ("::ffff:100.64.0.1", "mapped CGNAT"),
    ("::ffff:0.0.0.0", "mapped unspecified"),
    ("::ffff:224.0.0.1", "mapped multicast"),
    ("::ffff:7f00:1", "mapped loopback in hex notation"),
];

const ALLOWED: &[(&str, &str)] = &[
    ("8.8.8.8", "public DNS"),
    ("1.1.1.1", "public DNS"),
    ("93.184.216.34", "public"),
    ("140.82.112.3", "public"),
    // neighbours just outside the blocked ranges
    ("9.255.255.255", "just below 10/8"),
    ("11.0.0.0", "just above 10/8"),
    ("126.255.255.255", "just below 127/8"),
    ("128.0.0.1", "just above 127/8"),
    ("172.15.255.255", "just below 172.16/12"),
    ("172.32.0.0", "just above 172.16/12"),
    ("192.167.255.255", "just below 192.168/16"),
    ("192.169.0.0", "just above 192.168/16"),
    ("169.253.255.255", "just below link-local"),
    ("169.255.0.0", "just above link-local"),
    ("100.63.255.255", "just below CGNAT"),
    ("100.128.0.0", "just above CGNAT"),
    ("223.255.255.255", "just below multicast"),
    ("1.0.0.0", "first non-zero /8"),
    // IPv6
    ("2001:4860:4860::8888", "public DNS v6"),
    ("2606:4700:4700::1111", "public DNS v6"),
    ("2a00:1450:4001:81b::200e", "public v6"),
    ("fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "just below ULA"),
    ("fe00::1", "outside fe80::/10 and fc00::/7"),
    (
        "::ffff:8.8.8.8",
        "mapped public address is judged as the public IPv4 it is",
    ),
    ("::ffff:93.184.216.34", "mapped public"),
];

#[test]
fn sec_10_every_address_class_in_the_corpus_is_blocked() {
    let missed: Vec<String> = BLOCKED
        .iter()
        .filter(|(a, _)| !is_blocked_ip(ip(a)))
        .map(|(a, why)| format!("{a} ({why})"))
        .collect();
    assert!(
        missed.is_empty(),
        "these must be blocked but are reachable:\n  {}",
        missed.join("\n  ")
    );
}

#[test]
fn public_addresses_and_range_neighbours_are_not_blocked() {
    let wrongly_blocked: Vec<String> = ALLOWED
        .iter()
        .filter(|(a, _)| is_blocked_ip(ip(a)))
        .map(|(a, why)| format!("{a} ({why})"))
        .collect();
    assert!(
        wrongly_blocked.is_empty(),
        "these are public but classified as blocked:\n  {}",
        wrongly_blocked.join("\n  ")
    );
}

#[test]
fn the_classifier_is_exact_on_every_range_boundary() {
    // For each blocked IPv4 block: first and last address blocked, address before/after allowed.
    let blocks: [(&str, &str, &str, &str); 5] = [
        ("10.0.0.0/8", "10.0.0.0", "10.255.255.255", "RFC1918 10/8"),
        ("172.16.0.0/12", "172.16.0.0", "172.31.255.255", "RFC1918 172.16/12"),
        ("192.168.0.0/16", "192.168.0.0", "192.168.255.255", "RFC1918 192.168/16"),
        ("169.254.0.0/16", "169.254.0.0", "169.254.255.255", "link-local"),
        ("100.64.0.0/10", "100.64.0.0", "100.127.255.255", "CGNAT"),
    ];
    for (cidr, first, last, name) in blocks {
        let n = net(cidr);
        assert!(
            is_blocked_ip(ip(first)) && is_blocked_ip(ip(last)),
            "{name}: ends of {cidr}"
        );
        let (IpAddr::V4(f), IpAddr::V4(l)) = (ip(first), ip(last)) else {
            unreachable!()
        };
        let before = IpAddr::V4((u32::from(f) - 1).into());
        let after = IpAddr::V4((u32::from(l) + 1).into());
        assert!(!n.contains(&before) && !n.contains(&after));
        assert!(!is_blocked_ip(before), "{name}: {before} is just outside {cidr}");
        assert!(!is_blocked_ip(after), "{name}: {after} is just outside {cidr}");
    }
}

#[test]
fn every_address_in_whole_blocked_slash_16s_and_slash_24s_is_blocked() {
    // exhaustive sweep of the dense ranges guests are most likely to probe
    for third in 0..=255u8 {
        for fourth in [0u8, 1, 127, 254, 255] {
            assert!(is_blocked_ip(ip(&format!("10.1.{third}.{fourth}"))));
            assert!(is_blocked_ip(ip(&format!("192.168.{third}.{fourth}"))));
            assert!(is_blocked_ip(ip(&format!("169.254.{third}.{fourth}"))));
            assert!(is_blocked_ip(ip(&format!("127.0.{third}.{fourth}"))));
            assert!(is_blocked_ip(ip(&format!(
                "172.{}.{third}.{fourth}",
                16 + (third % 16)
            ))));
            assert!(is_blocked_ip(ip(&format!(
                "100.{}.{third}.{fourth}",
                64 + (third % 64)
            ))));
        }
    }
}

#[test]
fn every_ipv4_mapped_address_is_judged_exactly_like_its_ipv4_form() {
    for v4 in [
        "127.0.0.1",
        "10.9.8.7",
        "172.16.0.1",
        "172.32.0.1",
        "192.168.0.1",
        "169.254.169.254",
        "100.64.1.1",
        "100.128.1.1",
        "8.8.8.8",
        "93.184.216.34",
        "0.0.0.0",
        "224.0.0.1",
        "255.255.255.255",
    ] {
        let mapped = ip(&format!("::ffff:{v4}"));
        assert_eq!(is_blocked_ip(mapped), is_blocked_ip(ip(v4)), "{v4} vs ::ffff:{v4}");
    }
}

#[test]
fn ipv6_prefixes_are_classified_by_prefix_not_by_a_single_address() {
    for seg in [0xfc00u16, 0xfc01, 0xfd00, 0xfdab, 0xfdff] {
        assert!(
            is_blocked_ip(ip(&format!("{seg:x}::1234"))),
            "{seg:x}:: is ULA (fc00::/7)"
        );
    }
    for seg in [0xfe80u16, 0xfe81, 0xfe9f, 0xfeb0, 0xfebf] {
        assert!(
            is_blocked_ip(ip(&format!("{seg:x}::1"))),
            "{seg:x}:: is link-local (fe80::/10)"
        );
    }
    for seg in [0xff00u16, 0xff01, 0xff02, 0xff05, 0xff0e, 0xffff] {
        assert!(
            is_blocked_ip(ip(&format!("{seg:x}::1"))),
            "{seg:x}:: is multicast (ff00::/8)"
        );
    }
    for seg in [0x2400u16, 0x2606, 0x2a00, 0x2c0f, 0x3fff] {
        assert!(
            !is_blocked_ip(ip(&format!("{seg:x}::1"))),
            "{seg:x}:: is global unicast"
        );
    }
    // 2001::/32 is Teredo (a tunnel to an arbitrary IPv4); the rest of 2001::/16 is global unicast.
    assert!(is_blocked_ip(ip("2001::1")), "Teredo");
    assert!(
        !is_blocked_ip(ip("2001:4860::1")),
        "global unicast outside Teredo's /32"
    );
    // 6to4 (2002::/16) is judged by the IPv4 address it embeds: `2002::1` embeds 0.0.0.0.
    assert!(is_blocked_ip(ip("2002::1")), "6to4 of 0.0.0.0");
    assert!(
        !is_blocked_ip(ip("2002:5db8:d822::1")),
        "6to4 of the public 93.184.216.34"
    );
}

#[test]
fn non_canonical_literal_forms_are_not_addresses_so_callers_must_resolve_them() {
    // SEC-10 lists decimal/octal/hex IPs: Rust's parser refuses them, so a guest cannot smuggle
    // `http://2130706433/` past the classifier as an `IpAddr`; the HTTP client must reject (or
    // normalize) such hosts before classification.
    for s in [
        "2130706433",
        "0177.0.0.1",
        "0x7f.0.0.1",
        "127.1",
        "0x7f000001",
        "017700000001",
        "127.0.0.1.",
        "127.0.0.1:80",
        "[::1]",
        "::1%lo0",
        "localhost",
        "",
    ] {
        assert!(s.parse::<IpAddr>().is_err(), "`{s}` unexpectedly parses as an IpAddr");
    }
    // the canonical dotted-quad of those literals is blocked
    assert!(is_blocked_ip(ip("127.0.0.1")));
}

// ---------------------------------------------------------------------------------------------
// Host allow / deny lists (SEC-12, SEC-10 `localhost.`)
// ---------------------------------------------------------------------------------------------

#[test]
fn sec_12_allowed_hosts_exact_and_dot_suffix_matches_only() {
    let allow = strings(&["api.telegram.org"]);
    for ok in [
        "api.telegram.org",
        "API.Telegram.ORG",
        "api.telegram.org.",
        "x.api.telegram.org",
        "a.b.api.telegram.org",
    ] {
        assert!(host_allowed(ok, &allow), "{ok} must be allowed");
    }
    for denied in [
        "example.com",
        "telegram.org",
        "evilapi.telegram.org",
        "api.telegram.org.evil.com",
        "api.telegram.orgx",
        "xapi.telegram.org",
        "api-telegram.org",
        "",
        "api.telegram.org@evil.com",
    ] {
        assert!(
            !host_allowed(denied, &allow),
            "{denied} must not be allowed by [api.telegram.org]"
        );
    }
}

#[test]
fn an_empty_allowlist_means_any_host_and_entries_normalize() {
    assert!(host_allowed("example.com", &[]));
    assert!(host_allowed("anything.at.all", &[]));
    let allow = strings(&["Example.COM.", "api.other.io"]);
    assert!(host_allowed("example.com", &allow));
    assert!(host_allowed("www.example.com", &allow));
    assert!(host_allowed("api.other.io", &allow));
    assert!(!host_allowed("other.io", &allow));
    assert!(!host_allowed("notexample.com", &allow));
}

#[test]
fn denied_hosts_win_by_exact_or_dot_suffix_match() {
    let denied = strings(&["localhost", "internal.corp"]);
    for h in [
        "localhost",
        "LOCALHOST",
        "localhost.",
        "foo.localhost",
        "a.b.internal.corp",
        "internal.corp.",
    ] {
        assert!(host_denied(h, &denied), "{h} must be denied");
    }
    for h in [
        "notlocalhost",
        "localhost.example.com",
        "internal.corporation",
        "corp",
        "example.com",
        "",
    ] {
        assert!(!host_denied(h, &denied), "{h} must not be denied");
    }
    assert!(!host_denied("anything", &[]), "nothing is denied by an empty list");
}

#[test]
fn sec_10_the_trailing_dot_localhost_corpus_entry_is_denied() {
    // `localhost.` resolves to loopback; as a name it must be caught by the default denylist.
    assert!(host_denied("localhost.", &strings(&["localhost"])));
    assert!(host_denied("LocalHost.", &strings(&["localhost."])));
}

// ---------------------------------------------------------------------------------------------
// Self denylist: addresses of the node's own endpoints + egress.denied_cidrs (SEC-24, SEC-35)
// ---------------------------------------------------------------------------------------------

#[test]
fn sec_24_node_endpoints_on_public_addresses_are_denied_even_though_they_are_public() {
    // S3 endpoint / OTLP endpoint / binding base_url on public IPs: the classifier alone lets them through.
    let s3 = ip("203.0.113.9");
    let otlp = ip("198.51.100.7");
    assert!(!is_blocked_ip(s3) && !is_blocked_ip(otlp));

    let deny = SelfDenylist::new(vec![], vec![]);
    assert!(!deny.contains(s3));
    deny.add_addrs([s3, otlp]);
    assert!(deny.contains(s3) && deny.contains(otlp));
    assert!(!deny.contains(ip("203.0.113.10")), "only the exact addresses");
    assert!(!deny.contains(ip("8.8.8.8")));
}

#[test]
fn refresh_resolves_literal_hosts_and_host_port_pairs() {
    let deny = SelfDenylist::new(
        strings(&["203.0.113.50", "198.51.100.60:8000", "[2001:db8::5]:443", "2001:db8::6"]),
        vec![],
    );
    assert!(
        !deny.contains(ip("203.0.113.50")),
        "nothing is denied before the first refresh"
    );
    deny.refresh();
    for a in ["203.0.113.50", "198.51.100.60", "2001:db8::5", "2001:db8::6"] {
        assert!(deny.contains(ip(a)), "{a} must be denied after refresh");
    }
    assert!(!deny.contains(ip("198.51.100.61")));
}

#[test]
fn refresh_resolves_names_such_as_localhost() {
    let deny = SelfDenylist::new(strings(&["localhost"]), vec![]);
    deny.refresh();
    assert!(
        deny.contains(ip("127.0.0.1")) || deny.contains(ip("::1")),
        "`localhost` must resolve into the denylist"
    );
}

#[test]
fn an_unresolvable_host_does_not_poison_the_rest_of_the_list() {
    let deny = SelfDenylist::new(strings(&["this-host-does-not-exist.invalid", "203.0.113.77"]), vec![]);
    deny.refresh();
    assert!(deny.contains(ip("203.0.113.77")));
}

#[test]
fn denied_cidrs_match_statically_without_refresh() {
    let deny = SelfDenylist::new(vec![], vec![net("203.0.113.0/24"), net("198.51.100.128/25")]);
    assert!(deny.contains(ip("203.0.113.0")) && deny.contains(ip("203.0.113.255")) && deny.contains(ip("203.0.113.9")));
    assert!(!deny.contains(ip("203.0.112.255")) && !deny.contains(ip("203.0.114.0")));
    assert!(deny.contains(ip("198.51.100.128")) && deny.contains(ip("198.51.100.255")));
    assert!(!deny.contains(ip("198.51.100.127")), "/25 boundary");
}

#[test]
fn reserved_ranges_and_ipv4_tunnelling_ipv6_forms_are_blocked() {
    for (a, why) in [
        ("198.18.0.0", "benchmarking 198.18/15 first"),
        ("198.19.255.255", "benchmarking 198.18/15 last"),
        ("240.0.0.1", "reserved 240/4"),
        ("254.1.2.3", "reserved 240/4"),
        ("192.0.0.170", "IETF protocol assignments 192.0.0/24 (NAT64 discovery)"),
        ("2002:c0a8:0101::1", "6to4 of 192.168.1.1"),
        ("2002:7f00:0001::1", "6to4 of 127.0.0.1"),
        ("2002:a9fe:a9fe::1", "6to4 of 169.254.169.254"),
        ("2001:0:4136:e378:8000:63bf:3fff:fdd2", "Teredo"),
        ("fec0::1", "site-local"),
        ("feff:ffff::1", "site-local last /10"),
    ] {
        assert!(is_blocked_ip(ip(a)), "{a} ({why}) must be blocked");
    }
    for (a, why) in [
        ("198.17.255.255", "just below 198.18/15"),
        ("198.20.0.0", "just above 198.18/15"),
        ("192.0.1.1", "just above 192.0.0/24"),
        ("2002:0808:0808::1", "6to4 of the public 8.8.8.8"),
        (
            "2001:4860:4860::8888",
            "public v6 under 2001::/16 but outside Teredo's /32",
        ),
    ] {
        assert!(!is_blocked_ip(ip(a)), "{a} ({why}) is public");
    }
}

#[test]
fn listener_addresses_survive_a_refresh() {
    let deny = SelfDenylist::new(strings(&["203.0.113.50"]), vec![]);
    deny.add_addrs([ip("198.51.100.9")]);
    deny.refresh();
    assert!(
        deny.contains(ip("198.51.100.9")),
        "add_addrs entries are not replaced by a refresh"
    );
    assert!(deny.contains(ip("203.0.113.50")));
}

#[test]
fn local_interface_addresses_include_loopback() {
    let addrs = rt_sec::local_interface_addrs();
    assert!(
        addrs.iter().any(|a| a.is_loopback()),
        "getifaddrs must report the loopback interface: {addrs:?}"
    );
}

#[test]
fn sec_35_ipv6_cidrs_include_exactly_their_prefix() {
    let deny = SelfDenylist::new(vec![], vec![net("2001:db8::/32")]);
    assert!(deny.contains(ip("2001:db8::1")), "inside 2001:db8::/32");
    assert!(
        deny.contains(ip("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff")),
        "last address of the /32"
    );
    assert!(deny.contains(ip("2001:db8::")), "first address");
    assert!(!deny.contains(ip("2001:db9::1")), "just outside");
    assert!(
        !deny.contains(ip("2001:db7:ffff:ffff:ffff:ffff:ffff:ffff")),
        "just below"
    );
    assert!(!deny.contains(ip("2001:4860:4860::8888")));
    assert!(
        !deny.contains(ip("203.0.113.9")),
        "a v4 address is never inside a v6 cidr"
    );
}

#[test]
fn ipv6_cidr_prefix_lengths_are_honoured() {
    let cases = [
        ("fd00:1234::/16", "fd00:ffff::1", true),
        ("fd00:1234::/16", "fd01::1", false),
        ("2001:db8:abcd::/48", "2001:db8:abcd:ffff::1", true),
        ("2001:db8:abcd::/48", "2001:db8:abce::1", false),
        ("2001:db8::1/128", "2001:db8::1", true),
        ("2001:db8::1/128", "2001:db8::2", false),
        ("::/0", "2606:4700::1", true),
        ("0.0.0.0/0", "8.8.8.8", true),
    ];
    for (cidr, addr, inside) in cases {
        let deny = SelfDenylist::new(vec![], vec![net(cidr)]);
        assert_eq!(deny.contains(ip(addr)), inside, "{addr} in {cidr}");
    }
}

#[test]
fn a_denied_v4_cidr_also_denies_the_ipv4_mapped_form_of_its_addresses() {
    // The classifier treats `::ffff:a.b.c.d` as a.b.c.d; the self denylist must not be bypassable the
    // same way (connecting to [::ffff:203.0.113.9] reaches the same host).
    let deny = SelfDenylist::new(vec![], vec![net("203.0.113.0/24")]);
    assert!(deny.contains(ip("203.0.113.9")));
    assert!(
        deny.contains(ip("::ffff:203.0.113.9")),
        "IPv4-mapped form must be judged by the IPv4 rules"
    );
    assert!(!deny.contains(ip("::ffff:8.8.8.8")));
    let literal = SelfDenylist::new(vec![], vec![]);
    literal.add_addrs([ip("203.0.113.9")]);
    assert!(literal.contains(ip("::ffff:203.0.113.9")), "same for literal addresses");
}

#[test]
fn self_denylist_can_be_used_through_the_trait_object() {
    let deny: Box<dyn EgressDenylist> =
        Box::new(SelfDenylist::new(strings(&["203.0.113.5"]), vec![net("2001:db8::/32")]));
    deny.refresh();
    assert!(deny.contains(ip("203.0.113.5")) && deny.contains(ip("2001:db8::7")));
}

// ---------------------------------------------------------------------------------------------
// SEC-51: the self-denylist follows the configured endpoints and survives DNS failures
// ---------------------------------------------------------------------------------------------

type Answers = std::sync::Arc<parking_lot::Mutex<std::collections::HashMap<String, Vec<IpAddr>>>>;

/// A resolver whose answers the test changes between refreshes.
fn scripted_resolver() -> (Answers, rt_sec::HostResolver) {
    let table = std::sync::Arc::new(parking_lot::Mutex::new(
        std::collections::HashMap::<String, Vec<IpAddr>>::new(),
    ));
    let t = table.clone();
    let resolver: rt_sec::HostResolver = std::sync::Arc::new(move |host: &str| {
        t.lock()
            .get(host)
            .cloned()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "nxdomain"))
    });
    (table, resolver)
}

#[test]
fn ac_sec_51_a_failed_lookup_keeps_the_previous_addresses_of_that_host() {
    let (table, resolver) = scripted_resolver();
    table
        .lock()
        .insert("s3.example.test:443".into(), vec![ip("203.0.113.9")]);
    table
        .lock()
        .insert("otlp.example.test:4317".into(), vec![ip("198.51.100.7")]);
    let deny =
        SelfDenylist::new(strings(&["s3.example.test:443", "otlp.example.test:4317"]), vec![]).with_resolver(resolver);
    deny.refresh();
    assert!(deny.contains(ip("203.0.113.9")) && deny.contains(ip("198.51.100.7")));

    // DNS outage for one name: its last known addresses stay denied, the other host is re-resolved.
    table.lock().remove("s3.example.test:443");
    table
        .lock()
        .insert("otlp.example.test:4317".into(), vec![ip("198.51.100.8")]);
    deny.refresh();
    assert!(
        deny.contains(ip("203.0.113.9")),
        "a lookup failure must not open the node's own endpoint"
    );
    assert!(
        deny.contains(ip("198.51.100.8")),
        "a successful lookup replaces the addresses of that host"
    );
    assert!(
        !deny.contains(ip("198.51.100.7")),
        "the stale answer of a re-resolved host is dropped"
    );
}

#[test]
fn ac_sec_51_set_hosts_replaces_the_endpoint_list_on_the_next_refresh() {
    let deny = SelfDenylist::new(strings(&["203.0.113.50"]), vec![]);
    deny.refresh();
    assert!(deny.contains(ip("203.0.113.50")));
    // A config reload drops one binding and adds another.
    deny.set_hosts(strings(&["198.51.100.61:8443"]));
    deny.refresh();
    assert!(
        deny.contains(ip("198.51.100.61")),
        "the new endpoint is denied after the refresh"
    );
    assert!(
        !deny.contains(ip("203.0.113.50")),
        "a removed endpoint is no longer denied"
    );
    assert_eq!(deny.hosts(), strings(&["198.51.100.61:8443"]));
}

#[test]
fn concurrent_readers_and_refreshers_never_observe_a_torn_list() {
    let deny = std::sync::Arc::new(SelfDenylist::new(strings(&["203.0.113.5"]), vec![]));
    deny.refresh();
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                for _ in 0..2_000 {
                    assert!(
                        deny.contains(ip("203.0.113.5")),
                        "refresh must swap the list atomically"
                    );
                }
            });
        }
        s.spawn(|| {
            for _ in 0..200 {
                deny.refresh();
            }
        });
    });
}

#[test]
fn sec_24_ipv6_forms_embedding_a_denied_ipv4_are_denied_too() {
    // A node endpoint on a public IPv4 and a denied IPv4 CIDR: every IPv6 spelling that delivers
    // to that IPv4 address (mapped, IPv4-compatible, NAT64 well-known prefix, 6to4) must match.
    let deny = SelfDenylist::new(vec![], vec![net("198.51.100.0/24")]);
    deny.add_addrs([ip("203.0.113.9")]);
    for form in [
        "203.0.113.9",
        "::ffff:203.0.113.9",
        "::203.0.113.9",
        "64:ff9b::203.0.113.9",
        "64:ff9b::cb00:7109",
        "2002:cb00:7109::1",
        "2002:cb00:7109:ffff::42",
    ] {
        assert!(deny.contains(ip(form)), "{form} reaches the denied 203.0.113.9");
    }
    for form in [
        "198.51.100.7",
        "::198.51.100.7",
        "64:ff9b::198.51.100.7",
        "2002:c633:6407::1",
    ] {
        assert!(deny.contains(ip(form)), "{form} is inside the denied 198.51.100.0/24");
    }
    for other in [
        "203.0.113.10",
        "64:ff9b::203.0.113.10",
        "2002:cb00:710a::1",
        "2001:db8::203.0.113.9",
        "::1",
        "::",
    ] {
        assert!(!deny.contains(ip(other)), "{other} must not match");
    }
    // a denylist entry written in an embedding form matches the plain IPv4 address too
    let by_v6 = SelfDenylist::new(vec![], vec![]);
    by_v6.add_addrs([ip("64:ff9b::203.0.113.50")]);
    assert!(by_v6.contains(ip("203.0.113.50")));
}
