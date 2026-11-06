//! The R4 e2e allowance in `egress.rs` exists only under `cfg(debug_assertions)` (SECURITY.md).

/// A release build cannot compile an ungated use (the module is empty there), and every use in the
/// sources is gated line by line.
#[test]
fn every_use_is_gated_by_debug_assertions() {
    let sources = [
        ("egress.rs", include_str!("../src/egress.rs")),
        ("lib.rs", include_str!("../src/lib.rs")),
        ("archive_fetch.rs", include_str!("../../rt-server/src/archive_fetch.rs")),
        ("wiring.rs", include_str!("../../rt-server/src/wiring.rs")),
    ];
    let mut uses = 0;
    for (name, src) in sources {
        let lines: Vec<&str> = src.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            // The assignment is inside the gated module.
            if !line.contains("local_test_origin")
                || line.trim_start().starts_with("//")
                || line.contains("cfg.local_test_origin =")
            {
                continue;
            }
            let attr = lines[..i]
                .iter()
                .rev()
                .find(|l| !l.trim().is_empty())
                .map_or("", |l| l.trim());
            let gated = attr == "#[cfg(debug_assertions)]"
                || (attr == "#[cfg(not(debug_assertions))]" && line.contains("mod local_test_origin {}"));
            assert!(gated, "{name}:{}: ungated use: {line}", i + 1);
            uses += 1;
        }
    }
    assert!(uses >= 6, "the scan saw the allowance: {uses}");
}

#[cfg(debug_assertions)]
mod debug {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use rt_host::{
        local_test_origin, EgressBudget, EgressError, EgressGuard, EgressGuardConfig, EgressPolicy, GuardedEgress,
    };
    use rt_sec::SelfDenylist;
    use rt_types::{Body, Limits};

    async fn send(guard: &GuardedEgress, url: &str) -> EgressError {
        let req = http::Request::builder().uri(url).body(Body::empty()).unwrap();
        let mut budget = EgressBudget::new(&Limits::default(), Instant::now() + Duration::from_secs(5), None);
        match guard.send(req, &EgressPolicy::default(), &mut budget).await {
            Ok(_) => panic!("nothing listens here"),
            Err(e) => e,
        }
    }

    fn guard(origin: Option<&str>) -> GuardedEgress {
        let cfg = EgressGuardConfig {
            local_test_origin: origin.map(str::to_string),
            ..EgressGuardConfig::default()
        };
        GuardedEgress::new(cfg, Arc::new(SelfDenylist::new(Vec::new(), Vec::new())))
    }

    #[tokio::test]
    async fn only_the_exact_host_and_port_pass_the_address_check() {
        // Port 9 of loopback: closed, so a request the guard lets through fails upstream.
        let g = guard(Some("127.0.0.1:9"));
        assert_eq!(
            send(&g, "https://127.0.0.1:9/").await.code(),
            EgressError::Upstream.code()
        );
        assert_eq!(
            send(&g, "https://127.0.0.1:10/").await.code(),
            EgressError::Denied.code()
        );
        assert_eq!(
            send(&g, "https://127.0.0.2:9/").await.code(),
            EgressError::Denied.code()
        );
        assert_eq!(
            send(&g, "https://localhost:9/").await.code(),
            EgressError::Denied.code()
        );
    }

    #[tokio::test]
    async fn without_the_allowance_loopback_stays_refused() {
        assert_eq!(
            send(&guard(None), "https://127.0.0.1:9/").await.code(),
            EgressError::Denied.code()
        );
    }

    #[test]
    fn the_environment_needs_both_variables() {
        let base = EgressGuardConfig::default;
        let none = local_test_origin::apply_from(base(), |_| None);
        assert!(none.local_test_origin.is_none() && none.extra_root_pem.is_none());
        let origin_only = local_test_origin::apply_from(base(), |n| {
            (n == local_test_origin::ORIGIN_ENV).then(|| "127.0.0.1:9".to_string())
        });
        assert!(origin_only.local_test_origin.is_none());
    }
}
