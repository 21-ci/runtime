//! `ingress.rate_rules` through the real invoke router: placement, client IP, listeners.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body as AxBody;
use axum::extract::ConnectInfo;
use http::{Method, Request, StatusCode};
use rt_types::{
    Access, Digest, IngressRules, Listener, RateBy, RateRule, RouteEntry, RouteSnapshot, RuntimeKind, ServiceId,
    TriggerSwitches, VersionNo,
};
use serde_json::json;
use tower::ServiceExt;

use crate::test_support::{node, service, tenant, TestNode};

const PROXY: &str = "10.0.0.1:9000";
const STRANGER: &str = "198.51.100.7:9000";

async fn routed(n: &TestNode, ingress: IngressRules) -> ServiceId {
    let t = tenant(n, "acme").await;
    let svc = service(n, t, "app", json!({})).await;
    let entry = RouteEntry {
        tenant: t,
        service: svc,
        slug: "app".into(),
        runtime: RuntimeKind::Deno,
        version: VersionNo::default(),
        digest: Digest::of(b"x"),
        access: Access::Public,
        triggers: TriggerSwitches::default(),
        ingress,
        profile: None,
        hosts: vec![],
        project: rt_types::ProjectId::new(),
        environment: rt_types::DEFAULT_ENVIRONMENT.into(),
        call: Default::default(),
        isolated: false,
    };
    n.state.routes.swap(&RouteSnapshot {
        generation: 1,
        entries: vec![entry],
        sdk: Default::default(),
    });
    svc
}

fn login_rule(by: RateBy) -> IngressRules {
    IngressRules {
        rate_rules: vec![RateRule {
            path: "/api/login".into(),
            methods: vec!["POST".into()],
            rps: 1,
            burst: 1,
            by,
        }],
        ..Default::default()
    }
}

async fn hit(n: &TestNode, listener: Listener, path: &str, peer: &str, xff: Option<&str>) -> StatusCode {
    let mut req = Request::builder().method(Method::POST).uri(format!("/s/app{path}"));
    if let Some(x) = xff {
        req = req.header("x-forwarded-for", x);
    }
    let mut req = req.body(AxBody::empty()).unwrap();
    req.extensions_mut()
        .insert(ConnectInfo(peer.parse::<SocketAddr>().unwrap()));
    crate::invoke_router(n.state.clone(), listener)
        .oneshot(req)
        .await
        .unwrap()
        .status()
}

fn trust_proxy(n: &TestNode) {
    let mut cfg = (**n.state.config.load()).clone();
    cfg.ingress.trusted_proxies = vec!["10.0.0.0/8".into()];
    n.state.config.store(Arc::new(cfg));
}

const LIMITED: StatusCode = StatusCode::TOO_MANY_REQUESTS;

#[tokio::test]
async fn a_rule_limits_its_path_and_not_others_and_sets_retry_after() {
    let n = node().await;
    routed(&n, login_rule(RateBy::Ip)).await;
    assert_ne!(hit(&n, Listener::Public, "/api/login", STRANGER, None).await, LIMITED);
    assert_eq!(hit(&n, Listener::Public, "/api/login", STRANGER, None).await, LIMITED);
    assert_eq!(
        hit(&n, Listener::Public, "/x/../api/%6Cogin/", STRANGER, None).await,
        LIMITED
    );
    assert_ne!(hit(&n, Listener::Public, "/api/other", STRANGER, None).await, LIMITED);
    let text = n
        .state
        .metrics
        .ingress_rate_limited_total
        .with_label_values(&["0"])
        .get();
    assert_eq!(text, 2);
}

#[tokio::test]
async fn the_internal_listener_is_exempt() {
    let n = node().await;
    routed(&n, login_rule(RateBy::Service)).await;
    for _ in 0..3 {
        assert_ne!(hit(&n, Listener::Internal, "/api/login", STRANGER, None).await, LIMITED);
    }
}

#[tokio::test]
async fn a_malformed_path_is_refused_only_on_a_service_with_rules() {
    let n = node().await;
    routed(&n, login_rule(RateBy::Ip)).await;
    assert_eq!(
        hit(&n, Listener::Public, "/api/%zz", STRANGER, None).await,
        StatusCode::BAD_REQUEST
    );
    let n = node().await;
    routed(&n, IngressRules::default()).await;
    assert_ne!(
        hit(&n, Listener::Public, "/api/%zz", STRANGER, None).await,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn forwarded_for_from_an_untrusted_peer_cannot_mint_clients() {
    let n = node().await;
    trust_proxy(&n);
    routed(&n, login_rule(RateBy::Ip)).await;
    assert_ne!(
        hit(&n, Listener::Public, "/api/login", STRANGER, Some("1.1.1.1")).await,
        LIMITED
    );
    assert_eq!(
        hit(&n, Listener::Public, "/api/login", STRANGER, Some("2.2.2.2")).await,
        LIMITED
    );
}

#[tokio::test]
async fn behind_a_trusted_proxy_only_the_hop_the_proxy_wrote_counts() {
    let n = node().await;
    trust_proxy(&n);
    routed(&n, login_rule(RateBy::Ip)).await;
    assert_ne!(
        hit(&n, Listener::Public, "/api/login", PROXY, Some("7.7.7.7")).await,
        LIMITED
    );
    // The client prepends junk: the proxy's own hop on the right still decides.
    assert_eq!(
        hit(&n, Listener::Public, "/api/login", PROXY, Some("8.8.8.8, 7.7.7.7")).await,
        LIMITED
    );
    assert_ne!(
        hit(&n, Listener::Public, "/api/login", PROXY, Some("7.7.7.8")).await,
        LIMITED
    );
}

#[tokio::test]
async fn a_malformed_forwarded_hop_cannot_mint_clients() {
    let n = node().await;
    trust_proxy(&n);
    routed(&n, login_rule(RateBy::Ip)).await;
    assert_ne!(
        hit(&n, Listener::Public, "/api/login", PROXY, Some("not-an-ip-1")).await,
        LIMITED
    );
    assert_eq!(
        hit(&n, Listener::Public, "/api/login", PROXY, Some("not-an-ip-2")).await,
        LIMITED
    );
}

#[tokio::test]
async fn limits_survive_a_route_refresh_and_changed_rules_apply_live() {
    let n = node().await;
    let svc = routed(&n, login_rule(RateBy::Ip)).await;
    hit(&n, Listener::Public, "/api/login", STRANGER, None).await;
    n.state.ingress.retain_services(|s| *s == svc);
    let table = n.state.routes.by_service(svc).unwrap();
    n.state.routes.swap(&RouteSnapshot {
        generation: 2,
        entries: vec![(*table).clone()],
        sdk: Default::default(),
    });
    assert_eq!(hit(&n, Listener::Public, "/api/login", STRANGER, None).await, LIMITED);
    let mut changed = (*table).clone();
    changed.ingress.rate_rules[0].burst = 5;
    n.state.routes.swap(&RouteSnapshot {
        generation: 3,
        entries: vec![changed],
        sdk: Default::default(),
    });
    assert_ne!(hit(&n, Listener::Public, "/api/login", STRANGER, None).await, LIMITED);
}

#[tokio::test]
async fn patch_validates_rules_and_warns_without_a_trusted_proxy() {
    let n = node().await;
    let t = tenant(&n, "acme").await;
    let svc = service(&n, t, "app", json!({})).await;
    let uri = format!("/v1/services/{svc}");
    let rule = json!({"path": "/api/login", "methods": ["POST"], "rps": 5, "burst": 10, "by": "ip"});
    let (status, _, out) = n
        .call(
            Method::PATCH,
            &uri,
            Some(t),
            Some(json!({"ingress": {"rate_rules": [rule]}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["ingress_warnings"][0].as_str().unwrap().contains("trusted_proxies"));

    trust_proxy(&n);
    let (_, _, out) = n
        .call(
            Method::PATCH,
            &uri,
            Some(t),
            Some(json!({"ingress": {"rate_rules": [rule]}})),
        )
        .await;
    assert!(out.get("ingress_warnings").is_none(), "{out}");

    for bad in [
        json!({"path": "api", "rps": 1}),
        json!({"path": "/a", "rps": 0}),
        json!({"path": "/a", "rps": 1, "by": "planet"}),
        json!({"path": "/a", "rps": 1, "extra": 1}),
    ] {
        let (status, _, out) = n
            .call(
                Method::PATCH,
                &uri,
                Some(t),
                Some(json!({"ingress": {"rate_rules": [bad]}})),
            )
            .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    }
    let many = vec![json!({"path": "/a", "rps": 1}); 33];
    let (status, _, _) = n
        .call(
            Method::PATCH,
            &uri,
            Some(t),
            Some(json!({"ingress": {"rate_rules": many}})),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}
