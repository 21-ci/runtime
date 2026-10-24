//! Route table: TRG-01..03 plus the resolution rules around them.
//!
//! `RouteResolver::resolve(listener, host, path)` takes the path **without** the query string;
//! the query is carried by the ingress next to it, so TRG-01's `GET /s/a/x?y=1` is
//! `resolve(Public, "…", "/x")` here and the handler path stays `/x`.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use common::{entry, snapshot};
use proptest::prelude::*;
use rt_triggers::{normalize_host, RouteMatch, RouteResolver, RouteTable};
use rt_types::{Access, Listener};

fn table_with(entries: Vec<rt_types::RouteEntry>) -> Arc<RouteTable> {
    let t = RouteTable::new(None);
    t.swap(&snapshot(1, entries));
    t
}

#[test]
fn ac_trg_01_path_prefix_strips_slug_and_reports_base_path() {
    let t = table_with(vec![entry(1, 1, "a", 1)]);
    let r = t
        .resolve(Listener::Public, "node.example.com", "/s/a/x")
        .expect("route");
    assert_eq!(r.matched, RouteMatch::PathPrefix);
    assert_eq!(r.path, "/x");
    assert_eq!(r.base_path, "/s/a");
    assert_eq!(r.entry.slug, "a");
}

#[test]
fn ac_trg_01_nested_path_and_trailing_slash() {
    let t = table_with(vec![entry(1, 1, "a", 1)]);
    let r = t.resolve(Listener::Public, "h", "/s/a/deep/er/path").expect("route");
    assert_eq!(r.path, "/deep/er/path");
    assert_eq!(r.base_path, "/s/a");

    // `/s/a` and `/s/a/` both reach the handler as `/`.
    for p in ["/s/a", "/s/a/"] {
        let r = t
            .resolve(Listener::Public, "h", p)
            .unwrap_or_else(|| panic!("route for {p}"));
        assert_eq!(r.path, "/", "path {p}");
        assert_eq!(r.base_path, "/s/a", "path {p}");
    }
}

#[test]
fn ac_trg_01_unknown_slug_and_foreign_paths_do_not_resolve() {
    let t = table_with(vec![entry(1, 1, "a", 1)]);
    assert!(t.resolve(Listener::Public, "h", "/s/b/x").is_none());
    assert!(t.resolve(Listener::Public, "h", "/x").is_none());
    assert!(t.resolve(Listener::Public, "h", "/").is_none());
    assert!(t.resolve(Listener::Public, "h", "/v1/services").is_none());
    // The prefix must be a whole segment: `/s/ab` is not slug `a`.
    assert!(t.resolve(Listener::Public, "h", "/s/ab/x").is_none());
    // Slug lookup is case-insensitive in the table (slugs are lowercase by validation).
    assert!(t.resolve(Listener::Public, "h", "/s/A/x").is_some());
}

#[test]
fn ac_trg_02_custom_host_routes_to_service_with_original_path() {
    let mut e = entry(1, 1, "a", 1);
    e.hosts = vec!["bot.example.com".to_string()];
    let t = table_with(vec![e, entry(1, 2, "other", 1)]);

    let r = t
        .resolve(Listener::Public, "bot.example.com", "/webhook/telegram")
        .expect("route");
    assert_eq!(r.matched, RouteMatch::CustomHost);
    assert_eq!(r.entry.slug, "a");
    assert_eq!(r.path, "/webhook/telegram", "path is passed through unchanged");
    assert_eq!(r.base_path, "", "host routes carry no base path");

    // Host matching ignores case and a `:port` suffix.
    let r = t.resolve(Listener::Public, "BOT.Example.COM:8080", "/").expect("route");
    assert_eq!(r.entry.slug, "a");

    // An unrelated host with a plain path does not resolve; with /s/{slug} it still does.
    assert!(t.resolve(Listener::Public, "evil.example.com", "/webhook").is_none());
    let r = t
        .resolve(Listener::Public, "evil.example.com", "/s/other/x")
        .expect("path route");
    assert_eq!(r.entry.slug, "other");
}

#[test]
fn ac_trg_02_custom_host_wins_over_path_prefix() {
    let mut a = entry(1, 1, "a", 1);
    a.hosts = vec!["bot.example.com".to_string()];
    let t = table_with(vec![a, entry(1, 2, "b", 1)]);
    // First match wins: the custom host decides, the /s/b prefix is just part of the path.
    let r = t.resolve(Listener::Public, "bot.example.com", "/s/b/x").expect("route");
    assert_eq!(r.entry.slug, "a");
    assert_eq!(r.path, "/s/b/x");
}

#[test]
fn routing_base_domain_maps_subdomain_to_slug() {
    let t = RouteTable::new(Some("fn.example.com".to_string()));
    t.swap(&snapshot(1, vec![entry(1, 1, "a", 1), entry(1, 2, "b", 1)]));

    let r = t.resolve(Listener::Public, "a.fn.example.com", "/hook").expect("route");
    assert_eq!(r.matched, RouteMatch::BaseDomain);
    assert_eq!(r.entry.slug, "a");
    assert_eq!(r.path, "/hook");
    assert_eq!(r.base_path, "");

    // Port, case.
    let r = t.resolve(Listener::Public, "B.FN.example.com:443", "/").expect("route");
    assert_eq!(r.entry.slug, "b");

    // The bare base domain, unknown slugs and nested labels do not resolve by host.
    assert!(t.resolve(Listener::Public, "fn.example.com", "/hook").is_none());
    assert!(t.resolve(Listener::Public, "zzz.fn.example.com", "/hook").is_none());
    assert!(t.resolve(Listener::Public, "x.a.fn.example.com", "/hook").is_none());
    // Path routing still works on the base domain's host.
    assert!(t.resolve(Listener::Public, "fn.example.com", "/s/a/x").is_some());
}

#[test]
fn routing_base_domain_is_off_when_unset_or_empty() {
    for base in [None, Some(String::new())] {
        let t = RouteTable::new(base);
        t.swap(&snapshot(1, vec![entry(1, 1, "a", 1)]));
        assert!(t.resolve(Listener::Public, "a.fn.example.com", "/x").is_none());
    }
}

#[test]
fn routing_precedence_custom_host_then_base_domain_then_path() {
    let t = RouteTable::new(Some("fn.example.com".to_string()));
    let mut hosted = entry(1, 1, "hosted", 1);
    // A (verified) custom host that is *also* shaped like a base-domain name wins: rule 1 first.
    hosted.hosts = vec!["other.fn.example.com".to_string()];
    t.swap(&snapshot(1, vec![hosted, entry(1, 2, "other", 1)]));
    let r = t
        .resolve(Listener::Public, "other.fn.example.com", "/p")
        .expect("route");
    assert_eq!(r.matched, RouteMatch::CustomHost);
    assert_eq!(r.entry.slug, "hosted");
}

#[test]
fn access_internal_is_404_on_public_and_routable_on_internal() {
    let mut e = entry(1, 1, "secret", 1);
    e.access = Access::Internal;
    let t = table_with(vec![e, entry(1, 2, "open", 1)]);

    assert!(
        t.resolve(Listener::Public, "h", "/s/secret/x").is_none(),
        "SEC-08: public listener must not see it"
    );
    let r = t
        .resolve(Listener::Internal, "h", "/s/secret/x")
        .expect("internal listener routes it");
    assert_eq!(r.entry.slug, "secret");
    assert!(t.resolve(Listener::Public, "h", "/s/open/x").is_some());
    assert!(t.resolve(Listener::Internal, "h", "/s/open/x").is_some());
}

#[test]
fn access_key_services_still_resolve_on_public_listener() {
    // `key` access is checked by the invoke layer after routing; the table must not hide it.
    let mut e = entry(1, 1, "keyed", 1);
    e.access = Access::Key;
    let t = table_with(vec![e]);
    assert!(t.resolve(Listener::Public, "h", "/s/keyed/").is_some());
}

#[test]
fn swap_replaces_the_whole_table_and_reports_generation() {
    let t = RouteTable::new(None);
    assert!(t.is_empty());
    assert_eq!(t.len(), 0);
    assert_eq!(t.generation(), 0);

    t.swap(&snapshot(5, vec![entry(1, 1, "a", 1), entry(1, 2, "b", 1)]));
    assert_eq!(t.generation(), 5);
    assert_eq!(t.len(), 2);
    assert!(!t.is_empty());
    assert_eq!(t.by_slug("a").expect("a").version.0, 1);
    assert!(t.by_slug("zz").is_none());

    t.swap(&snapshot(6, vec![entry(1, 1, "a", 2)]));
    assert_eq!(t.generation(), 6);
    assert_eq!(t.len(), 1);
    assert_eq!(t.by_slug("a").expect("a").version.0, 2);
    assert!(t.by_slug("b").is_none(), "removed services disappear with the swap");
    assert!(t.resolve(Listener::Public, "h", "/s/b/x").is_none());
}

#[test]
fn route_obtained_before_a_swap_stays_valid() {
    let t = table_with(vec![entry(1, 1, "a", 1)]);
    let before = t.resolve(Listener::Public, "h", "/s/a/x").expect("route");
    t.swap(&snapshot(2, vec![entry(1, 1, "a", 2)]));
    // In-flight requests keep the entry they resolved (they finish on the old version).
    assert_eq!(before.entry.version.0, 1);
    let after = t.resolve(Listener::Public, "h", "/s/a/x").expect("route");
    assert_eq!(after.entry.version.0, 2);
}

#[test]
fn normalize_host_lowercases_and_strips_port() {
    assert_eq!(normalize_host("Bot.Example.COM"), "bot.example.com");
    assert_eq!(normalize_host("bot.example.com:8080"), "bot.example.com");
    assert_eq!(normalize_host("  bot.example.com:443 "), "bot.example.com");
    assert_eq!(normalize_host("localhost"), "localhost");
    // A non-numeric suffix after ':' is not a port.
    assert_eq!(normalize_host("a:b"), "a:b");
}

/// TRG-03: a deploy / live flip while requests are flowing never produces a missing route.
/// Readers resolve in a tight loop on several threads while the writer swaps snapshots that
/// alternate the live version (and add/remove unrelated services); not one resolve may return
/// `None`, and the generation a reader observes never goes backwards.
#[test]
fn ac_trg_03_live_flip_under_load_has_zero_missing_routes() {
    let t = RouteTable::new(None);
    t.swap(&snapshot(1, vec![entry(1, 1, "hot", 1)]));

    let stop = Arc::new(AtomicBool::new(false));
    let missing = Arc::new(AtomicU64::new(0));
    let resolved = Arc::new(AtomicU64::new(0));
    let regress = Arc::new(AtomicU64::new(0));

    std::thread::scope(|s| {
        for _ in 0..4 {
            let (t, stop, missing, resolved, regress) = (
                t.clone(),
                stop.clone(),
                missing.clone(),
                resolved.clone(),
                regress.clone(),
            );
            s.spawn(move || {
                let mut last_gen = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    match t.resolve(Listener::Public, "h", "/s/hot/x") {
                        Some(r) => {
                            assert!(matches!(r.entry.version.0, 1 | 2));
                            assert_eq!(r.path, "/x");
                            resolved.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            missing.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    let g = t.generation();
                    if g < last_gen {
                        regress.fetch_add(1, Ordering::Relaxed);
                    }
                    last_gen = g;
                }
            });
        }

        // Writer: 5,000 flips of the live version; every few swaps adds/removes another service.
        for i in 2..5_002u64 {
            let version = if i % 2 == 0 { 2 } else { 1 };
            let mut entries = vec![entry(1, 1, "hot", version)];
            if i % 3 == 0 {
                entries.push(entry(1, 2, "noise", 1));
            }
            t.swap(&snapshot(i, entries));
        }
        stop.store(true, Ordering::Relaxed);
    });

    assert_eq!(missing.load(Ordering::Relaxed), 0, "0 x 404 during the flip");
    assert_eq!(
        regress.load(Ordering::Relaxed),
        0,
        "generation never goes backwards for a reader"
    );
    assert!(resolved.load(Ordering::Relaxed) > 0, "readers did run");
}

proptest! {
    /// Any slug in the table resolves through `/s/{slug}{rest}`; the handler sees `rest`
    /// (or `/` when empty) and the base path is `/s/{slug}`.
    #[test]
    fn path_routing_roundtrip(slug in "[a-z][a-z0-9-]{1,30}", rest in "(/[a-z0-9._~-]{0,12}){0,5}") {
        let t = table_with(vec![entry(1, 1, &slug, 1)]);
        let path = format!("/s/{slug}{rest}");
        let r = t.resolve(Listener::Public, "h", &path).expect("route");
        let expected = if rest.is_empty() { "/".to_string() } else { rest.clone() };
        prop_assert_eq!(r.path, expected);
        prop_assert_eq!(r.base_path, format!("/s/{slug}"));
        prop_assert_eq!(r.matched, RouteMatch::PathPrefix);
    }

    /// Host normalization is idempotent and never leaves uppercase letters, a numeric port or a
    /// trailing dot (ROUTE-003: `api.example.com.` is `api.example.com`).
    #[test]
    fn normalize_host_idempotent(host in "[A-Za-z][A-Za-z0-9.-]{0,30}", port in proptest::option::of(1u16..=65535)) {
        let raw = match port { Some(p) => format!("{host}:{p}"), None => host.clone() };
        let once = normalize_host(&raw);
        prop_assert_eq!(normalize_host(&once), once.clone());
        prop_assert_eq!(&once, &host.to_ascii_lowercase().trim_end_matches('.').to_string());
    }
}

/// INV-004: a custom host wins over a `/s/<slug>` path: the request goes to the host's service
/// with the whole path and no base path, never to the other tenant's `victim`.
#[test]
fn inv_004_a_custom_host_wins_over_a_slug_path() {
    let mut mine = entry(1, 1, "mine", 1);
    mine.hosts = vec!["app.example.com".to_string()];
    let t = table_with(vec![mine, entry(2, 2, "victim", 1)]);
    let r = t
        .resolve(Listener::Public, "app.example.com", "/s/victim/x")
        .expect("route");
    assert_eq!(r.matched, RouteMatch::CustomHost);
    assert_eq!(r.entry.slug, "mine");
    assert_eq!((r.path.as_str(), r.base_path.as_str()), ("/s/victim/x", ""));
    let r = t
        .resolve(Listener::Public, "node.example.com", "/s/victim/x")
        .expect("route");
    assert_eq!((r.entry.slug.as_str(), r.base_path.as_str()), ("victim", "/s/victim"));
}

/// ROUTE-002 / ROUTE-003: a Host in fully-qualified form (`api.example.com.`) reaches the custom
/// host stored without the dot; an empty port is dropped; IPv6 literals and odd colons stay as
/// they are (they never name a custom host).
#[test]
fn route_002_003_host_normalisation_edges() {
    assert_eq!(normalize_host("API.Example.com."), "api.example.com");
    assert_eq!(normalize_host("api.example.com.:8443"), "api.example.com");
    assert_eq!(normalize_host("example.com:"), "example.com");
    assert_eq!(normalize_host("[::1]:8080"), "[::1]:8080");
    assert_eq!(normalize_host("a:b:c"), "a:b:c");
    let mut e = entry(1, 1, "a", 1);
    e.hosts = vec!["api.example.com".to_string()];
    let t = table_with(vec![e]);
    let r = t
        .resolve(Listener::Public, "api.example.com.", "/x")
        .expect("the FQDN form routes");
    assert_eq!(r.entry.slug, "a");
}
