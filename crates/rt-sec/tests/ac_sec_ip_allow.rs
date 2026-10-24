//! `ingress.ip_allow` rule matching (SEC-19): an empty list admits everyone, a non-empty list
//! admits only addresses inside its CIDRs, and malformed entries fail closed.

use std::net::IpAddr;

use rt_sec::{ip_allowed, parse_cidr, IpAllowList};

fn ip(s: &str) -> IpAddr {
    s.parse().expect("test address")
}

#[test]
fn an_empty_rule_list_admits_everyone_and_is_unrestricted() {
    let none: [&str; 0] = [];
    assert!(ip_allowed(ip("203.0.113.9"), &none));
    let list = IpAllowList::parse(&none).expect("empty is valid");
    assert!(list.is_unrestricted());
    assert!(list.allows(ip("2001:db8::1")));
}

#[test]
fn a_rule_list_admits_only_addresses_inside_its_cidrs() {
    let rules = ["10.0.0.0/8", "2001:db8::/32"];
    let list = IpAllowList::parse(&rules).expect("valid rules");
    assert!(!list.is_unrestricted());
    assert!(list.allows(ip("10.20.30.40")));
    assert!(list.allows(ip("2001:db8:1::5")));
    assert!(!list.allows(ip("11.0.0.1")));
    assert!(!list.allows(ip("2001:dead::1")));
    assert!(ip_allowed(ip("10.1.1.1"), &rules));
    assert!(!ip_allowed(ip("192.168.1.1"), &rules));
}

#[test]
fn a_bare_address_is_a_host_route_and_v4_mapped_peers_match_v4_rules() {
    let list = IpAllowList::parse(&["192.0.2.7", "198.51.100.0/24"]).expect("valid");
    assert!(list.allows(ip("192.0.2.7")));
    assert!(!list.allows(ip("192.0.2.8")));
    // An IPv4-mapped IPv6 peer is canonicalised to its IPv4 form before matching.
    assert!(list.allows(ip("::ffff:198.51.100.77")));
    assert!(!list.allows(ip("::ffff:203.0.113.1")));
}

#[test]
fn strict_parsing_rejects_the_first_malformed_entry() {
    assert!(IpAllowList::parse(&["10.0.0.0/8", "not-a-cidr"]).is_err());
    assert!(IpAllowList::parse(&["10.0.0.0/33"]).is_err());
    assert!(parse_cidr("2001:db8::/129").is_err());
    assert!(
        parse_cidr("  172.16.0.0/12 ").is_ok(),
        "surrounding whitespace is trimmed"
    );
}

#[test]
fn lossy_parsing_drops_malformed_entries_and_fails_closed_when_all_are_bad() {
    let mixed = IpAllowList::parse_lossy(&["bogus", "10.0.0.0/8"]);
    assert!(mixed.allows(ip("10.9.9.9")));
    assert!(!mixed.allows(ip("8.8.8.8")));

    // Every entry is malformed: the list was configured, so it is restrictive and matches nobody.
    let all_bad = IpAllowList::parse_lossy(&["bogus", "999.1.1.1"]);
    assert!(!all_bad.is_unrestricted());
    assert!(!all_bad.allows(ip("10.9.9.9")));
    assert!(!ip_allowed(ip("10.9.9.9"), &["bogus"]));
}
