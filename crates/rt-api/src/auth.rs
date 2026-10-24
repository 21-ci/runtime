//! Authentication extractor for the control API.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;
use rt_sec::Principal;
use rt_store::TenantScope;
use rt_types::{Listener, TenantId};

use crate::error::ApiError;
use crate::state::ApiState;

/// The authenticated caller of a control request.
///
/// - `Authorization: Bearer <root token | rtk_… key>` (or `x-rt-key`) is checked by the
///   injected `Authenticator`; failures are rate-limited per IP (SEC-02).
/// - Release builds without a root token answer `503 control_disabled` (SEC-01).
/// - `x-rt-tenant: ten_…` lets the operator act on behalf of a tenant (API-12).
#[derive(Clone, Debug)]
pub struct Auth {
    pub principal: Principal,
    /// What the caller may see: everything (operator) or one tenant.
    pub scope: TenantScope,
}

impl Auth {
    /// `403 insufficient_scope` unless the caller holds `scope` (operator always does).
    pub fn require_scope(&self, scope: rt_types::Scope) -> Result<(), ApiError> {
        if self.principal.has_scope(scope) {
            Ok(())
        } else {
            Err(ApiError::new("insufficient_scope", format!("missing scope `{scope}`")))
        }
    }

    /// Only the operator may call node/tenant management routes.
    pub fn require_operator(&self) -> Result<(), ApiError> {
        match self.principal {
            Principal::Operator => Ok(()),
            _ => Err(ApiError::new("insufficient_scope", "operator token required")),
        }
    }

    /// The tenant a write acts on: the key's tenant, or the operator's `x-rt-tenant`.
    pub fn tenant(&self) -> Result<TenantId, ApiError> {
        match self.scope {
            TenantScope::Tenant(t) => Ok(t),
            TenantScope::Operator => Err(ApiError::invalid("x-rt-tenant header is required for this operation")),
        }
    }

    /// Identity used for per-principal limits (idempotency keys, SSE caps).
    pub fn identity(&self) -> String {
        match &self.principal {
            Principal::Tenant { key, .. } => key.to_string(),
            Principal::Operator => match self.scope {
                TenantScope::Tenant(t) => format!("operator:{t}"),
                TenantScope::Operator => "operator".to_string(),
            },
            Principal::Internal { .. } => "internal".to_string(),
            Principal::Anonymous => "anonymous".to_string(),
        }
    }

    pub fn actor(&self) -> rt_sec::Actor {
        match &self.principal {
            Principal::Tenant { key, tenant, .. } => rt_sec::Actor::Key {
                key_id: key.to_string(),
                tenant: *tenant,
            },
            Principal::Operator => rt_sec::Actor::Operator,
            _ => rt_sec::Actor::System,
        }
    }
}

/// Expose the client to the authenticator so failed authentications are rate-limited per client
/// (SEC-02, SEC-39, SEC-40): the peer (`SocketAddr` and `IpAddr` extensions) and the real client
/// behind `ingress.trusted_proxies` ([`rt_sec::ClientIp`], what the limiter keys on).
pub(crate) fn seed_peer(parts: &mut Parts, state: &ApiState) {
    if let Some(ConnectInfo(addr)) = parts.extensions.get::<ConnectInfo<SocketAddr>>().cloned() {
        if parts.extensions.get::<SocketAddr>().is_none() {
            parts.extensions.insert(addr);
        }
        if parts.extensions.get::<std::net::IpAddr>().is_none() {
            parts.extensions.insert(addr.ip());
        }
        // Only a trusted proxy's `X-Forwarded-For` counts; for any other peer the client is the
        // peer itself, which the limiter already falls back to.
        if parts.extensions.get::<rt_sec::ClientIp>().is_none() {
            let proxies = state.ingress.trusted_proxies(&state.config.load());
            if proxies.is_trusted(addr.ip()) {
                let forwarded: Vec<&str> = parts
                    .headers
                    .get_all("x-forwarded-for")
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .collect();
                let client = proxies.client_ip(addr.ip(), &forwarded);
                parts.extensions.insert(rt_sec::ClientIp(client));
            }
        }
    }
}

impl FromRequestParts<ApiState> for Auth {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &ApiState) -> Result<Self, Self::Rejection> {
        seed_peer(parts, state);
        let principal = state.auth.authenticate(parts, Listener::Control)?;
        let scope = match &principal {
            Principal::Operator => match parts.headers.get("x-rt-tenant").and_then(|v| v.to_str().ok()) {
                Some(h) if !h.trim().is_empty() => {
                    let id: TenantId = h.trim().parse().map_err(|_| ApiError::not_found())?;
                    // The system tenant is managed through `/v1/sdks`: acting as it, the operator
                    // may read, never create or change anything (keys, services, projects, …).
                    if rt_types::is_system_tenant(id)
                        && !matches!(parts.method, axum::http::Method::GET | axum::http::Method::HEAD)
                    {
                        return Err(ApiError::new(
                            "reserved",
                            "the system tenant is managed through /v1/sdks",
                        ));
                    }
                    match state.meta.get_tenant(id).await? {
                        Some(_) => TenantScope::Tenant(id),
                        None => return Err(ApiError::not_found()),
                    }
                }
                _ => TenantScope::Operator,
            },
            Principal::Tenant { tenant, .. } => TenantScope::Tenant(*tenant),
            Principal::Internal { .. } | Principal::Anonymous => {
                return Err(ApiError::new("unauthorized", "unauthorized"))
            }
        };
        Ok(Auth { principal, scope })
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::sync::Arc;

    use axum::extract::ConnectInfo;
    use axum::http::{Method, Request, StatusCode};
    use rt_sec::{Actor, AuthError, Authenticator};

    use super::*;
    use crate::test_support::{self, TestNode};

    /// Accepts every request as the one principal it was built with.
    struct Fixed(Principal);

    impl Authenticator for Fixed {
        fn authenticate(&self, _: &Parts, _: Listener) -> Result<Principal, AuthError> {
            Ok(self.0.clone())
        }
    }

    async fn node_as(principal: Principal) -> TestNode {
        let mut node = test_support::node().await;
        node.state.auth = Arc::new(Fixed(principal));
        node
    }

    fn parts(peer: Option<&str>, xff: &[&str]) -> Parts {
        let mut b = Request::builder().method(Method::GET).uri("/v1/tenants");
        for v in xff {
            b = b.header("x-forwarded-for", *v);
        }
        let mut req = b.body(()).expect("request");
        if let Some(ip) = peer {
            let addr: SocketAddr = format!("{ip}:4000").parse().expect("addr");
            req.extensions_mut().insert(ConnectInfo(addr));
        }
        req.into_parts().0
    }

    /// Callers that are neither the operator nor a key have their own identity and system actor.
    #[test]
    fn internal_and_anonymous_callers_are_named_and_act_as_the_system() {
        let internal = Auth {
            principal: Principal::Internal { invoker: None },
            scope: TenantScope::Operator,
        };
        assert_eq!(internal.identity(), "internal");
        assert_eq!(internal.actor(), Actor::System);
        let anon = Auth {
            principal: Principal::Anonymous,
            scope: TenantScope::Operator,
        };
        assert_eq!(anon.identity(), "anonymous");
        assert_eq!(anon.actor(), Actor::System);
    }

    /// A principal that is neither the operator nor a key cannot reach the control API.
    #[tokio::test]
    async fn an_anonymous_or_internal_principal_is_refused_by_the_control_api() {
        for principal in [
            Principal::Anonymous,
            Principal::Internal {
                invoker: Some("svc".into()),
            },
        ] {
            let node = node_as(principal).await;
            let (status, _, body) = node.call(Method::GET, "/v1/tenants", None, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
            assert_eq!(body["error"]["code"], "unauthorized");
        }
    }

    /// Without a peer address there is nothing to seed: the request keeps no client IP and no
    /// forwarded address is trusted.
    #[tokio::test]
    async fn a_request_without_a_peer_address_gets_no_client_ip() {
        let node = test_support::node().await;
        let mut p = parts(None, &["203.0.113.7"]);
        seed_peer(&mut p, &node.state);
        assert!(p.extensions.get::<SocketAddr>().is_none());
        assert!(p.extensions.get::<IpAddr>().is_none());
        assert!(p.extensions.get::<rt_sec::ClientIp>().is_none());
    }

    /// The peer is recorded once; a client IP already set by an outer layer is kept as it is.
    #[tokio::test]
    async fn seed_peer_records_the_peer_and_keeps_an_existing_client_ip() {
        let node = test_support::node().await;
        let mut p = parts(Some("198.51.100.9"), &[]);
        seed_peer(&mut p, &node.state);
        assert_eq!(
            p.extensions.get::<IpAddr>().copied(),
            Some("198.51.100.9".parse().expect("ip"))
        );
        assert!(
            p.extensions.get::<rt_sec::ClientIp>().is_none(),
            "an untrusted peer is its own client"
        );

        let mut outer = parts(Some("198.51.100.9"), &[]);
        outer
            .extensions
            .insert(rt_sec::ClientIp("192.0.2.1".parse().expect("ip")));
        seed_peer(&mut outer, &node.state);
        assert_eq!(
            outer.extensions.get::<rt_sec::ClientIp>().map(|c| c.0),
            Some("192.0.2.1".parse().expect("ip"))
        );
    }

    /// Behind a trusted proxy the client is the address the proxy forwarded.
    #[tokio::test]
    async fn a_trusted_proxy_hands_the_limiter_the_forwarded_client() {
        let node = test_support::node().await;
        let mut cfg = rt_config::Config::default();
        cfg.ingress.trusted_proxies = vec!["10.0.0.0/8".into()];
        node.state.config.store(Arc::new(cfg));
        let mut p = parts(Some("10.0.0.5"), &["203.0.113.7"]);
        seed_peer(&mut p, &node.state);
        assert_eq!(
            p.extensions.get::<rt_sec::ClientIp>().map(|c| c.0),
            Some("203.0.113.7".parse().expect("ip"))
        );
    }
}
