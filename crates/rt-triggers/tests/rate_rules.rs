//! Per-path ingress rate rules: first match, either path form, per-IP cells, caps, overflow.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use proptest::prelude::*;
use rt_sched::{Clock, SimClock};
use rt_testkit::VirtualClock;
use rt_triggers::rate_rules::{RateCaps, RateRuleLimiter, RateVerdict};
use rt_types::{RateBy, RateRule, ServiceId};

const CAPS: RateCaps = RateCaps {
    global: 1000,
    per_service: 100,
};

fn rule(path: &str, by: RateBy, rps: u32, burst: u32) -> RateRule {
    RateRule {
        path: path.into(),
        methods: vec![],
        rps,
        burst,
        by,
    }
}

fn ip(s: &str) -> Option<IpAddr> {
    Some(s.parse().unwrap())
}

struct Fx {
    clock: Arc<VirtualClock>,
    dyn_clock: Arc<dyn Clock>,
    limiter: RateRuleLimiter,
}

fn fx() -> Fx {
    let clock = Arc::new(VirtualClock::new());
    Fx {
        dyn_clock: clock.clone() as Arc<dyn Clock>,
        clock,
        limiter: RateRuleLimiter::default(),
    }
}

impl Fx {
    fn check(
        &self,
        svc: ServiceId,
        rules: &[RateRule],
        method: &str,
        path: &str,
        client: Option<IpAddr>,
        caps: RateCaps,
    ) -> RateVerdict {
        self.limiter
            .check(&self.dyn_clock, svc, rules, method, path, client, || caps)
    }
}

fn limited(v: RateVerdict) -> bool {
    matches!(v, RateVerdict::Limited { .. })
}

/// Rule of 1 rps, burst 1: the first request passes, the second shows whether it hit the rule.
fn hits_rule(pattern: &str, path: &str) -> bool {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule(pattern, RateBy::Service, 1, 1)];
    let first = f.check(svc, &rules, "GET", path, ip("1.1.1.1"), CAPS);
    assert_eq!(first, RateVerdict::Allow, "{path}");
    limited(f.check(svc, &rules, "GET", path, ip("1.1.1.1"), CAPS))
}

#[test]
fn no_form_of_the_path_escapes_a_rule() {
    for path in [
        "/api/login",
        "/api/%6cogin",
        "/%61pi/login",
        "/api/%256cogin",
        "/x/../api/login",
        "/x/%2e%2e/api/login",
        "/x/%2E%2E/api/login",
        "/./api/login",
        "/api/./login",
        "/api/login/",
        "//api//login",
        "///api/login",
        "/API/LOGIN",
        "/Api/Login/",
        "/%41PI/login",
        "/x\\..\\api\\login",
        "/api%2flogin",
        "/api%5Clogin",
    ] {
        assert!(hits_rule("/api/login", path), "{path} escaped the rule");
    }
}

#[test]
fn the_raw_form_also_matches() {
    // Resolved, this leaves `/x`; a rule on `/x/**` still applies, as the guest may read the raw path.
    assert!(hits_rule("/x/**", "/x/../api"));
}

#[test]
fn other_paths_are_not_limited() {
    assert!(!hits_rule("/api/login", "/api/logout"));
    assert!(!hits_rule("/api/login", "/api/login/x"));
}

#[test]
fn malformed_paths_fail_closed_only_with_rules() {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule("/a", RateBy::Service, 100, 100)];
    for bad in ["/a%zz", "/a%", "/a%00", "/%0a"] {
        assert_eq!(
            f.check(svc, &rules, "GET", bad, ip("1.1.1.1"), CAPS),
            RateVerdict::Malformed
        );
    }
}

#[test]
fn a_stored_rule_that_does_not_compile_limits_everything() {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule("no-slash", RateBy::Service, 1, 1)];
    assert_eq!(
        f.check(svc, &rules, "GET", "/any", ip("1.1.1.1"), CAPS),
        RateVerdict::Allow
    );
    assert!(limited(f.check(svc, &rules, "GET", "/other", ip("1.1.1.1"), CAPS)));
}

#[test]
fn a_zero_rate_refuses_rather_than_divides() {
    let f = fx();
    let svc = ServiceId::new();
    for by in [RateBy::Ip, RateBy::Service] {
        let rules = [rule("/a", by, 0, 0)];
        assert!(limited(f.check(svc, &rules, "GET", "/a", ip("1.1.1.1"), CAPS)));
    }
}

#[test]
fn first_match_wins_and_methods_filter_in_any_case() {
    let f = fx();
    let svc = ServiceId::new();
    let mut post = rule("/a", RateBy::Service, 1, 1);
    post.methods = vec!["POST".into()];
    let rules = [post, rule("/**", RateBy::Service, 1000, 1000)];
    for _ in 0..5 {
        assert_eq!(f.check(svc, &rules, "GET", "/a", None, CAPS), RateVerdict::Allow);
    }
    assert_eq!(f.check(svc, &rules, "POST", "/a", None, CAPS), RateVerdict::Allow);
    match f.check(svc, &rules, "post", "/a", None, CAPS) {
        RateVerdict::Limited { rule, retry_after } => {
            assert_eq!(rule, 0);
            assert!(retry_after > Duration::ZERO && retry_after <= Duration::from_secs(1));
        }
        v => panic!("{v:?}"),
    }
}

#[test]
fn by_ip_is_independent_per_client_and_refills() {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule("/a", RateBy::Ip, 2, 3)];
    for _ in 0..3 {
        assert_eq!(
            f.check(svc, &rules, "GET", "/a", ip("1.1.1.1"), CAPS),
            RateVerdict::Allow
        );
    }
    assert!(limited(f.check(svc, &rules, "GET", "/a", ip("1.1.1.1"), CAPS)));
    assert_eq!(
        f.check(svc, &rules, "GET", "/a", ip("2.2.2.2"), CAPS),
        RateVerdict::Allow
    );
    f.clock.advance(Duration::from_millis(500));
    assert_eq!(
        f.check(svc, &rules, "GET", "/a", ip("1.1.1.1"), CAPS),
        RateVerdict::Allow
    );
    assert!(limited(f.check(svc, &rules, "GET", "/a", ip("1.1.1.1"), CAPS)));
}

#[test]
fn ipv6_is_keyed_on_the_64_and_mapped_v4_is_v4() {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule("/a", RateBy::Ip, 1, 1)];
    f.check(svc, &rules, "GET", "/a", ip("2001:db8:1:2::1"), CAPS);
    assert!(limited(f.check(
        svc,
        &rules,
        "GET",
        "/a",
        ip("2001:db8:1:2:ffff::9"),
        CAPS
    )));
    assert_eq!(
        f.check(svc, &rules, "GET", "/a", ip("2001:db8:1:3::1"), CAPS),
        RateVerdict::Allow
    );
    f.check(svc, &rules, "GET", "/a", ip("9.9.9.9"), CAPS);
    assert!(limited(f.check(svc, &rules, "GET", "/a", ip("::ffff:9.9.9.9"), CAPS)));
}

#[test]
fn no_client_address_shares_the_overflow_cell() {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule("/a", RateBy::Ip, 1, 1)];
    assert_eq!(f.check(svc, &rules, "GET", "/a", None, CAPS), RateVerdict::Allow);
    assert!(limited(f.check(svc, &rules, "GET", "/a", None, CAPS)));
    assert_eq!(f.limiter.keys(), 0);
}

#[test]
fn over_the_cap_new_clients_share_overflow_and_active_cells_stay() {
    let f = fx();
    let svc = ServiceId::new();
    let caps = RateCaps {
        global: 1000,
        per_service: 2,
    };
    let rules = [rule("/a", RateBy::Ip, 1, 1)];
    f.check(svc, &rules, "GET", "/a", ip("1.0.0.1"), caps);
    f.check(svc, &rules, "GET", "/a", ip("1.0.0.2"), caps);
    assert_eq!(f.limiter.keys(), 2);
    // Two strangers: one overflow cell between them.
    assert_eq!(
        f.check(svc, &rules, "GET", "/a", ip("1.0.0.3"), caps),
        RateVerdict::Allow
    );
    assert!(limited(f.check(svc, &rules, "GET", "/a", ip("1.0.0.4"), caps)));
    assert_eq!(f.limiter.keys(), 2);
    // No eviction hands an attacker a fresh burst: the first client is still spent.
    assert!(limited(f.check(svc, &rules, "GET", "/a", ip("1.0.0.1"), caps)));
}

#[test]
fn one_service_churn_cannot_exhaust_the_shared_limiter() {
    let f = fx();
    let (noisy, quiet) = (ServiceId::new(), ServiceId::new());
    let caps = RateCaps {
        global: 800,
        per_service: 100,
    };
    let rules = [rule("/a", RateBy::Ip, 1, 1)];
    for i in 0..20_000u32 {
        let addr = IpAddr::V4(i.into());
        f.check(noisy, &rules, "GET", "/a", Some(addr), caps);
    }
    assert_eq!(f.limiter.keys(), 100, "the noisy service stops at its sub-cap");
    // Every client of the other service still gets its own cell, not a shared overflow.
    for i in 0..100u32 {
        let addr = IpAddr::V4((i + 1_000_000).into());
        assert_eq!(
            f.check(quiet, &rules, "GET", "/a", Some(addr), caps),
            RateVerdict::Allow,
            "client {i}"
        );
    }
    assert_eq!(f.limiter.keys(), 200);
}

#[test]
fn the_global_cap_holds() {
    let f = fx();
    let caps = RateCaps {
        global: 150,
        per_service: 100,
    };
    let rules = [rule("/a", RateBy::Ip, 1, 1)];
    for s in 0..3 {
        let svc = ServiceId::new();
        for i in 0..200u32 {
            f.check(svc, &rules, "GET", "/a", Some(IpAddr::V4((s * 1000 + i).into())), caps);
        }
    }
    assert_eq!(f.limiter.keys(), 150);
}

#[test]
fn memory_is_bounded_under_a_million_ips() {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule("/a", RateBy::Ip, 1000, 1000)];
    let caps = RateCaps {
        global: 10_000,
        per_service: 5_000,
    };
    for i in 0..1_000_000u32 {
        f.check(svc, &rules, "GET", "/a", Some(IpAddr::V4(i.into())), caps);
    }
    assert_eq!(f.limiter.keys(), 5_000);
}

#[test]
fn idle_cells_are_pruned_and_spent_ones_kept() {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule("/a", RateBy::Ip, 1, 1)];
    f.check(svc, &rules, "GET", "/a", ip("1.1.1.1"), CAPS);
    f.limiter.prune(&f.dyn_clock);
    assert_eq!(f.limiter.keys(), 1, "recent cell stays");
    f.clock.advance(Duration::from_secs(11 * 60));
    f.limiter.prune(&f.dyn_clock);
    assert_eq!(f.limiter.keys(), 0);
    // A cell in debt past the idle window is not forgiven by a prune.
    let slow = [rule("/a", RateBy::Ip, 1, 3000)];
    let svc2 = ServiceId::new();
    for _ in 0..3000 {
        f.check(svc2, &slow, "GET", "/a", ip("3.3.3.3"), CAPS);
    }
    f.clock.advance(Duration::from_secs(11 * 60));
    f.limiter.prune(&f.dyn_clock);
    assert_eq!(f.limiter.keys(), 1);
}

#[test]
fn limits_survive_a_refresh_and_unrelated_rule_changes_but_not_their_own() {
    let f = fx();
    let svc = ServiceId::new();
    let a = rule("/a", RateBy::Ip, 1, 1);
    let b = rule("/b", RateBy::Service, 1, 1);
    let first = [a.clone(), b.clone()];
    f.check(svc, &first, "GET", "/a", ip("1.1.1.1"), CAPS);
    f.check(svc, &first, "GET", "/b", None, CAPS);
    // Same rules, freshly cloned, as a route refresh after a deploy gives them.
    f.limiter.retain_services(|s| *s == svc);
    let again = first.clone();
    assert!(limited(f.check(svc, &again, "GET", "/a", ip("1.1.1.1"), CAPS)));
    assert!(limited(f.check(svc, &again, "GET", "/b", None, CAPS)));
    // A reordered list with one new rule keeps both.
    let reordered = [rule("/c", RateBy::Ip, 1, 1), b.clone(), a.clone()];
    assert!(limited(f.check(svc, &reordered, "GET", "/a", ip("1.1.1.1"), CAPS)));
    assert!(limited(f.check(svc, &reordered, "GET", "/b", None, CAPS)));
    // A changed rule applies live and starts afresh.
    let changed = [rule("/a", RateBy::Ip, 5, 5), b];
    assert_eq!(
        f.check(svc, &changed, "GET", "/a", ip("1.1.1.1"), CAPS),
        RateVerdict::Allow
    );
    assert_eq!(f.limiter.keys(), 1, "the old rule's cell is released");
}

#[test]
fn a_deleted_service_releases_its_keys() {
    let f = fx();
    let svc = ServiceId::new();
    let rules = [rule("/a", RateBy::Ip, 1, 1)];
    f.check(svc, &rules, "GET", "/a", ip("1.1.1.1"), CAPS);
    f.limiter.retain_services(|_| false);
    assert_eq!(f.limiter.keys(), 0);
    assert_eq!(f.limiter.services(), 0);
}

#[test]
fn concurrent_checks_do_not_over_admit() {
    let f = Arc::new(fx());
    let svc = ServiceId::new();
    let rules = Arc::new([rule("/a", RateBy::Ip, 1, 100)]);
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (f, rules) = (f.clone(), rules.clone());
            std::thread::spawn(move || {
                (0..1000)
                    .filter(|_| f.check(svc, &*rules, "GET", "/a", ip("1.1.1.1"), CAPS) == RateVerdict::Allow)
                    .count()
            })
        })
        .collect();
    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total, 100);
}

proptest! {
    #[test]
    fn encoded_and_dotted_forms_of_a_path_all_hit(
        segs in prop::collection::vec("[a-z]{1,6}", 1..4),
        mask in any::<u64>(),
        junk in "[a-z]{1,4}",
        slash in any::<bool>(),
        dots in any::<bool>(),
    ) {
        let plain = format!("/{}", segs.join("/"));
        // Percent-encode some letters, by the mask.
        let mut encoded = String::new();
        for (i, c) in plain.chars().enumerate() {
            if c != '/' && mask >> (i % 64) & 1 == 1 {
                encoded.push_str(&format!("%{:02X}", c as u32));
            } else {
                encoded.push(c);
            }
        }
        if dots {
            encoded = format!("/{junk}/%2e%2e{encoded}");
        }
        if slash {
            encoded.push('/');
        }
        prop_assert!(hits_rule(&plain, &encoded), "{} via {}", plain, encoded);
    }
}

/// `make test-perf`: ns per check. Run with `--release`.
#[test]
#[ignore = "perf"]
fn perf_ns_per_check() {
    const N: u32 = 2_000_000;
    let f = fx();
    let svc = ServiceId::new();
    let caps = RateCaps {
        global: 2_000_000,
        per_service: 2_000_000,
    };
    let report = |name: &str, rules: &[RateRule], path: &str, client: Option<IpAddr>| {
        let t = Instant::now();
        for _ in 0..N {
            std::hint::black_box(f.check(svc, rules, "POST", path, client, caps));
        }
        println!("{name}: {} ns/check", t.elapsed().as_nanos() / u128::from(N));
    };
    let high = 1_000_000;
    let many: Vec<RateRule> = (0..31)
        .map(|i| rule(&format!("/other{i}/**"), RateBy::Ip, high, high))
        .chain([rule("/api/login", RateBy::Ip, high, high)])
        .collect();
    report("1 by-ip rule, hit", &many[31..], "/api/login", ip("203.0.113.9"));
    report(
        "1 by-service rule, hit",
        &[rule("/api/login", RateBy::Service, high, high)],
        "/api/login",
        ip("203.0.113.9"),
    );
    report("32 rules, last hit by-ip", &many, "/api/login", ip("203.0.113.9"));
    report("32 rules, no match", &many, "/nothing/here", ip("203.0.113.9"));
    report(
        "1 rule, encoded path (allocates)",
        &many[31..],
        "/x/../api/%6cogin/",
        ip("203.0.113.9"),
    );

    // Contention on one rule from 8 threads, distinct clients.
    let shared = Arc::new(fx());
    let rules = Arc::new(vec![rule("/api/login", RateBy::Ip, high, high)]);
    let t = Instant::now();
    let handles: Vec<_> = (0..8u32)
        .map(|th| {
            let (f, rules) = (shared.clone(), rules.clone());
            std::thread::spawn(move || {
                for i in 0..N / 8 {
                    let addr = IpAddr::V4((th * 1000 + i % 1000).into());
                    std::hint::black_box(f.check(svc, &rules, "POST", "/api/login", Some(addr), caps));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    println!(
        "8 threads, 8000 clients: {} ns/check (wall, per check over all threads)",
        t.elapsed().as_nanos() / u128::from(N)
    );

    // Bytes per cell.
    let before = rss_kb();
    let cells = fx();
    let rules = [rule("/a", RateBy::Ip, 1, 1)];
    for i in 0..1_000_000u32 {
        cells.check(svc, &rules, "GET", "/a", Some(IpAddr::V4(i.into())), caps);
    }
    println!(
        "{} cells: about {} bytes/cell (RSS delta)",
        cells.limiter.keys(),
        (rss_kb() - before) * 1024 / cells.limiter.keys() as u64
    );
}

fn rss_kb() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0)
}
