//! rt-sec authentication, scopes, failure limiting, quotas, audit shapes and hardening
//! (SECURITY.md: SEC-01..09, SEC-14, SEC-15, SEC-20, SEC-33, SEC-34).
//!
//! `DefaultAuthenticator::authenticate`, `invalidate_key`, `AuthFailureLimiter` and
//! `LocalQuotaEnforcer` are `todo!()` in the skeleton: tests that call them are expected to fail until
//! implemented. `Principal`, the error mappings, audit types and hardening are real.
#![allow(clippy::manual_range_contains, clippy::assertions_on_constants)] // lint-only allowance: test style, not behavior

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use rt_sec::{
    harden_process, is_dumpable, Actor, AuditEvent, AuditSink, AuthError, AuthFailureLimiter, Authenticator,
    DefaultAuthenticator, HardeningConfig, KeyHasher, KeyLookup, KeySpec, LocalQuotaEnforcer, MasterKey, PlanSource,
    Principal, QuotaEnforcer, QuotaError, QuotaOp, TracingAuditSink, WallClock,
};
use rt_types::{
    ApiKey, KeyId, Listener, Plan, ProjectId, Scope, Scopes, ServiceId, TenantId, TriggerKind, UnixMs, UsageDelta,
};

const ROOT: &str = "root-token-0123456789abcdef-ABCDEF";
const NOW: UnixMs = 1_800_000_000_000;
const MIB: i64 = 1024 * 1024;

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Store {
    keys: Mutex<HashMap<KeyId, ApiKey>>,
}

impl KeyLookup for Store {
    fn find(&self, id: KeyId) -> Option<ApiKey> {
        self.keys.lock().unwrap().get(&id).cloned()
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
    store: Arc<Store>,
    clock: Arc<Clock>,
    hasher: KeyHasher,
}

fn master() -> MasterKey {
    MasterKey::from_bytes(std::array::from_fn(|i| 0x40 + i as u8))
}

fn fixture(root: Option<&str>) -> Fixture {
    let store = Arc::new(Store::default());
    let clock = Arc::new(Clock(AtomicI64::new(NOW)));
    let hasher = KeyHasher::new(&master());
    let auth = DefaultAuthenticator::new(root.map(str::to_string), hasher.clone(), store.clone(), clock.clone());
    Fixture {
        auth,
        store,
        clock,
        hasher,
    }
}

struct Issued {
    plaintext: String,
    id: KeyId,
    tenant: TenantId,
}

impl Fixture {
    fn issue(&self, scopes: &[Scope], projects: Option<Vec<ProjectId>>, expires_at_ms: Option<UnixMs>) -> Issued {
        let tenant = TenantId::new();
        let spec = KeySpec {
            name: Some("test".into()),
            scopes: scopes.iter().copied().collect(),
            project_ids: projects,
            environments: None,
            expires_at_ms,
        };
        let (secret, new) = self.hasher.issue(spec);
        let key = ApiKey {
            id: new.id,
            tenant_id: tenant,
            name: new.name,
            scopes: new.scopes,
            project_ids: new.project_ids,
            environments: new.environments,
            expires_at_ms: new.expires_at_ms,
            created_at_ms: NOW,
            last_used_at_ms: None,
            revoked: false,
            parent_key_id: None,
            hmac: new.hmac,
        };
        self.store.keys.lock().unwrap().insert(key.id, key);
        Issued {
            plaintext: secret.expose().to_string(),
            id: new.id,
            tenant,
        }
    }

    fn revoke(&self, id: KeyId) {
        self.store
            .keys
            .lock()
            .unwrap()
            .get_mut(&id)
            .expect("key exists")
            .revoked = true;
    }

    fn call(&self, headers: &[(&str, &str)], listener: Listener) -> Result<Principal, AuthError> {
        let mut b = http::Request::builder().uri("/v1/services");
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        let (parts, ()) = b.body(()).unwrap().into_parts();
        self.auth.authenticate(&parts, listener)
    }

    fn bearer(&self, token: &str, listener: Listener) -> Result<Principal, AuthError> {
        let header = format!("Bearer {token}");
        self.call(&[("authorization", header.as_str())], listener)
    }
}

// ---------------------------------------------------------------------------------------------
// Principal / scopes (real code)
// ---------------------------------------------------------------------------------------------

fn tenant_principal(scopes: &[Scope], projects: Option<Vec<ProjectId>>) -> Principal {
    Principal::Tenant {
        tenant: TenantId::new(),
        key: KeyId::new(),
        scopes: scopes.iter().copied().collect(),
        project_ids: projects,
        environments: None,
    }
}

fn environment_principal(environments: Option<Vec<String>>) -> Principal {
    Principal::Tenant {
        tenant: TenantId::new(),
        key: KeyId::new(),
        scopes: [Scope::ServicesRead].into_iter().collect(),
        project_ids: None,
        environments,
    }
}

#[test]
fn sec_03_a_key_without_the_deploy_scope_cannot_deploy() {
    let p = tenant_principal(&[Scope::ServicesRead, Scope::LogsRead], None);
    assert!(p.has_scope(Scope::ServicesRead) && p.has_scope(Scope::LogsRead));
    assert!(!p.has_scope(Scope::Deploy), "SEC-03: 403 insufficient_scope");
    for s in Scope::ALL {
        let expected = matches!(s, Scope::ServicesRead | Scope::LogsRead);
        assert_eq!(p.has_scope(s), expected, "{s}");
    }
}

#[test]
fn the_operator_holds_every_scope_and_other_principals_hold_none() {
    for s in Scope::ALL {
        assert!(Principal::Operator.has_scope(s), "operator lacks {s}");
        assert!(!Principal::Anonymous.has_scope(s), "anonymous has {s}");
        assert!(
            !Principal::Internal {
                invoker: Some("svc".into())
            }
            .has_scope(s),
            "internal has {s}"
        );
    }
    let all = Principal::Tenant {
        tenant: TenantId::new(),
        key: KeyId::new(),
        scopes: Scopes::all(),
        project_ids: None,
        environments: None,
    };
    assert!(Scope::ALL.iter().all(|s| all.has_scope(*s)));
    let none = tenant_principal(&[], None);
    assert!(Scope::ALL.iter().all(|s| !none.has_scope(*s)));
}

#[test]
fn sec_04_a_key_limited_to_project_x_cannot_touch_project_y() {
    let (x, y) = (ProjectId::new(), ProjectId::new());
    let limited = tenant_principal(&[Scope::ServicesRead], Some(vec![x]));
    assert!(limited.can_touch(x, "production"));
    assert!(
        !limited.can_touch(y, "production"),
        "SEC-04: same tenant, other project -> 404"
    );
    let unrestricted = tenant_principal(&[Scope::ServicesRead], None);
    assert!(unrestricted.can_touch(x, "production") && unrestricted.can_touch(y, "production"));
    let empty_list = tenant_principal(&[Scope::ServicesRead], Some(vec![]));
    assert!(
        !empty_list.can_touch(x, "production"),
        "an explicit empty list allows nothing"
    );
    assert!(Principal::Operator.can_touch(y, "production"));
    assert!(!Principal::Anonymous.can_touch(x, "production"));
    assert!(!Principal::Internal { invoker: None }.can_touch(x, "production"));
}

#[test]
fn an_environment_limited_key_touches_only_those_environments_in_any_project() {
    let p = ProjectId::new();
    let staging = environment_principal(Some(vec!["staging".into()]));
    assert!(staging.can_touch(p, "staging"));
    assert!(
        !staging.can_touch(p, "production"),
        "a key limited to staging cannot touch production"
    );
    let none = environment_principal(Some(vec![]));
    assert!(
        !none.can_touch(p, "staging"),
        "an explicit empty environment list allows nothing"
    );
    assert!(environment_principal(None).can_touch(p, "production"));
}

#[test]
fn only_tenant_principals_are_bound_to_a_tenant() {
    let t = TenantId::new();
    let p = Principal::Tenant {
        tenant: t,
        key: KeyId::new(),
        scopes: Scopes::default(),
        project_ids: None,
        environments: None,
    };
    assert_eq!(p.tenant(), Some(t));
    assert_eq!(Principal::Operator.tenant(), None);
    assert_eq!(Principal::Anonymous.tenant(), None);
    assert_eq!(Principal::Internal { invoker: None }.tenant(), None);
}

#[test]
fn auth_errors_map_to_status_and_control_codes() {
    let table: [(AuthError, u16, &str); 5] = [
        (AuthError::Missing, 401, "unauthorized"),
        (AuthError::Invalid, 401, "unauthorized"),
        (
            AuthError::RateLimited {
                retry_after: Duration::from_secs(30),
            },
            429,
            "rate_limited",
        ),
        (AuthError::ControlDisabled, 503, "control_disabled"),
        (
            AuthError::RateLimited {
                retry_after: Duration::ZERO,
            },
            429,
            "rate_limited",
        ),
    ];
    for (e, status, code) in table {
        assert_eq!((e.http_status(), e.code()), (status, code), "{e:?}");
    }
    // missing and invalid are indistinguishable on the wire so key existence is not revealed
    assert_eq!(AuthError::Missing.code(), AuthError::Invalid.code());
    assert_eq!(AuthError::Missing.http_status(), AuthError::Invalid.http_status());
}

// ---------------------------------------------------------------------------------------------
// Operator root token
// ---------------------------------------------------------------------------------------------

#[test]
fn the_root_token_authenticates_the_operator_on_the_control_listener() {
    let f = fixture(Some(ROOT));
    assert_eq!(f.bearer(ROOT, Listener::Control), Ok(Principal::Operator));
}

#[test]
fn a_wrong_root_token_is_invalid_and_no_credentials_is_missing() {
    let f = fixture(Some(ROOT));
    assert_eq!(f.bearer("wrong-token", Listener::Control), Err(AuthError::Invalid));
    assert_eq!(f.call(&[], Listener::Control), Err(AuthError::Missing));
}

#[test]
fn near_miss_root_tokens_are_all_rejected() {
    // constant-time comparison must still compare length and every byte
    let f = fixture(Some(ROOT));
    for len in 1..ROOT.len() {
        assert_eq!(
            f.bearer(&ROOT[..len], Listener::Control),
            Err(AuthError::Invalid),
            "prefix of length {len}"
        );
    }
    for extra in ["x", "0123456789"] {
        assert_eq!(
            f.bearer(&format!("{ROOT}{extra}"), Listener::Control),
            Err(AuthError::Invalid),
            "suffix `{extra}`"
        );
    }
    let bytes = ROOT.as_bytes();
    for i in 0..bytes.len() {
        let mut b = bytes.to_vec();
        b[i] = if b[i] == b'#' { b'$' } else { b'#' }; // stays printable ASCII (valid in a header)
        let near = String::from_utf8(b).unwrap();
        assert_eq!(
            f.bearer(&near, Listener::Control),
            Err(AuthError::Invalid),
            "byte {i} changed"
        );
    }
    assert_eq!(
        f.bearer(&ROOT.to_uppercase(), Listener::Control),
        Err(AuthError::Invalid)
    );
}

#[test]
fn credentials_in_other_schemes_or_shapes_do_not_authenticate() {
    let f = fixture(Some(ROOT));
    for h in [
        format!("Basic {ROOT}"),
        format!("Token {ROOT}"),
        ROOT.to_string(),
        "Bearer".to_string(),
        "Bearer ".to_string(),
        format!("Bearer  {ROOT} extra"),
    ] {
        let r = f.call(&[("authorization", h.as_str())], Listener::Control);
        assert!(
            matches!(r, Err(AuthError::Missing | AuthError::Invalid)),
            "`{h}` must not authenticate, got {r:?}"
        );
    }
}

#[test]
fn sec_01_without_a_root_token_control_routes_are_disabled_in_release_builds() {
    let f = fixture(None);
    let r = f.call(&[], Listener::Control);
    if cfg!(debug_assertions) {
        // Debug builds stay open
        assert!(
            matches!(r, Ok(Principal::Operator) | Err(AuthError::ControlDisabled)),
            "{r:?}"
        );
    } else {
        assert_eq!(r, Err(AuthError::ControlDisabled));
    }
    // an arbitrary bearer token never turns into a tenant principal
    assert!(!matches!(
        f.bearer("anything", Listener::Control),
        Ok(Principal::Tenant { .. })
    ));
}

/// AUTH-013: an open debug control API (no root token) still refuses requests a browser makes for
/// another site (an `Origin` that is not loopback) or that arrive under a non-loopback `Host`
/// (DNS rebinding): only local tools get the operator without credentials.
#[test]
fn auth_013_an_open_control_api_serves_local_callers_only() {
    if !cfg!(debug_assertions) {
        return; // release builds disable the control API without a root token (sec_01)
    }
    let f = fixture(None);
    assert_eq!(f.call(&[], Listener::Control), Ok(Principal::Operator), "a local tool");
    for ok in [
        vec![("host", "127.0.0.1:8081")],
        vec![("host", "localhost:8081"), ("origin", "http://localhost:3000")],
        vec![("host", "[::1]:8081"), ("origin", "http://127.0.0.1")],
    ] {
        assert_eq!(f.call(&ok, Listener::Control), Ok(Principal::Operator), "{ok:?}");
    }
    for bad in [
        vec![("origin", "https://evil.example")],
        vec![("host", "127.0.0.1:8081"), ("origin", "null")],
        vec![("host", "evil.example:8081")],
        vec![
            ("host", "rebind.evil.example"),
            ("origin", "http://rebind.evil.example"),
        ],
    ] {
        assert_eq!(f.call(&bad, Listener::Control), Err(AuthError::Missing), "{bad:?}");
    }
}

/// INV-009: the authenticator recognises its root token (constant time) so the invoke path can
/// strip it from a request before a guest sees it.
#[test]
fn inv_009_the_root_token_is_recognised_for_stripping() {
    let f = fixture(Some(ROOT));
    assert!(f.auth.is_root_token(ROOT));
    assert!(!f.auth.is_root_token("not-the-root-token"));
    assert!(!f.auth.is_root_token(""));
    let open = fixture(None);
    assert!(!open.auth.is_root_token(ROOT), "no root token configured");
}

#[test]
fn sec_01_invoke_still_works_without_a_root_token() {
    let f = fixture(None);
    assert_eq!(f.call(&[], Listener::Public), Ok(Principal::Anonymous));
    assert_eq!(
        f.call(&[], Listener::Internal),
        Ok(Principal::Internal { invoker: None })
    );
}

// ---------------------------------------------------------------------------------------------
// Tenant keys
// ---------------------------------------------------------------------------------------------

#[test]
fn a_valid_tenant_key_authenticates_as_its_tenant_with_its_scopes_and_services() {
    let f = fixture(Some(ROOT));
    let svc = ProjectId::new();
    let k = f.issue(&[Scope::Deploy, Scope::Invoke], Some(vec![svc]), None);
    let expected = Principal::Tenant {
        tenant: k.tenant,
        key: k.id,
        scopes: [Scope::Deploy, Scope::Invoke].into_iter().collect(),
        project_ids: Some(vec![svc]),
        environments: None,
    };
    assert_eq!(f.bearer(&k.plaintext, Listener::Control), Ok(expected.clone()));
    assert_eq!(
        f.bearer(&k.plaintext, Listener::Public),
        Ok(expected.clone()),
        "invoke access = key on the public listener"
    );
    assert_eq!(
        f.call(&[("x-rt-key", k.plaintext.as_str())], Listener::Public),
        Ok(expected),
        "`x-rt-key` header form"
    );
}

#[test]
fn an_unrestricted_key_has_no_project_list() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::ServicesRead], None, None);
    match f.bearer(&k.plaintext, Listener::Control) {
        Ok(Principal::Tenant { project_ids: None, .. }) => {}
        other => panic!("expected an unrestricted tenant principal, got {other:?}"),
    }
}

#[test]
fn every_way_a_key_can_be_wrong_yields_the_same_invalid_error() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, None);
    let revoked = f.issue(&[Scope::Deploy], None, None);
    f.revoke(revoked.id);
    let expired = f.issue(&[Scope::Deploy], None, Some(NOW - 1));

    // right id, wrong secret
    let other_secret = rt_sec::generate_secret();
    let wrong_secret = rt_sec::format_key(k.id, &other_secret);
    // well-formed key for an id the store has never seen
    let unknown = rt_sec::format_key(KeyId::new(), &rt_sec::generate_secret());
    // another key's secret under this key's id
    let borrowed = rt_sec::format_key(k.id, rt_sec::parse_key(&revoked.plaintext).unwrap().secret);
    let malformed = [
        "rtk_garbage",
        "rtk_",
        "rtk",
        "rtk__",
        "rtk_01ARZ3NDEKTSV4RRFFQ69G5FAV_short",
    ];

    let mut results = vec![
        f.bearer(&wrong_secret, Listener::Control),
        f.bearer(&unknown, Listener::Control),
        f.bearer(&borrowed, Listener::Control),
        f.bearer(&revoked.plaintext, Listener::Control),
        f.bearer(&expired.plaintext, Listener::Control),
    ];
    results.extend(malformed.iter().map(|m| f.bearer(m, Listener::Control)));
    for (i, r) in results.iter().enumerate() {
        assert_eq!(
            r,
            &Err(AuthError::Invalid),
            "case {i}: existence and failure reason must not be revealed"
        );
    }
    // sanity: the real key still works
    assert!(matches!(
        f.bearer(&k.plaintext, Listener::Control),
        Ok(Principal::Tenant { .. })
    ));
}

#[test]
fn changing_any_single_character_of_a_valid_key_makes_it_invalid() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, None);
    let prefix_len = "rtk_".len() + 26 + 1;
    for i in prefix_len..k.plaintext.len() {
        let mut b = k.plaintext.clone().into_bytes();
        b[i] = if b[i] == b'0' { b'1' } else { b'0' };
        let bad = String::from_utf8(b).unwrap();
        assert_eq!(
            f.bearer(&bad, Listener::Control),
            Err(AuthError::Invalid),
            "secret char {i}"
        );
    }
}

#[test]
fn a_tenant_key_is_never_mistaken_for_the_operator() {
    let f = fixture(Some(ROOT));
    let k = f.issue(
        &[
            Scope::ServicesRead,
            Scope::ServicesWrite,
            Scope::Deploy,
            Scope::DataAdmin,
        ],
        None,
        None,
    );
    let p = f.bearer(&k.plaintext, Listener::Control).unwrap();
    assert_ne!(p, Principal::Operator);
    assert_eq!(p.tenant(), Some(k.tenant));
    assert!(!p.has_scope(Scope::SecretsWrite), "scopes are exactly the key's scopes");
}

#[test]
fn sec_06_a_revoked_key_stops_working_immediately_after_invalidation() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, None);
    assert!(f.bearer(&k.plaintext, Listener::Control).is_ok(), "warms the key cache");
    f.revoke(k.id);
    f.auth.invalidate_key(k.id);
    assert_eq!(f.bearer(&k.plaintext, Listener::Control), Err(AuthError::Invalid));
    // other keys are unaffected
    let other = f.issue(&[Scope::Deploy], None, None);
    assert!(f.bearer(&other.plaintext, Listener::Control).is_ok());
    f.auth.invalidate_key(other.id);
    assert!(
        f.bearer(&other.plaintext, Listener::Control).is_ok(),
        "invalidating a live key only forces a reload"
    );
}

#[test]
fn keys_expire_against_the_wall_clock() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, Some(NOW + 1_000));
    assert!(f.bearer(&k.plaintext, Listener::Control).is_ok());
    f.clock.0.store(NOW + 999, Ordering::SeqCst);
    assert!(f.bearer(&k.plaintext, Listener::Control).is_ok(), "1 ms before expiry");
    f.clock.0.store(NOW + 1_001, Ordering::SeqCst);
    assert_eq!(
        f.bearer(&k.plaintext, Listener::Control),
        Err(AuthError::Invalid),
        "after expiry (cached or not)"
    );
    // a key without expiry never expires
    let forever = f.issue(&[Scope::Deploy], None, None);
    f.clock.0.store(NOW + 10 * 365 * 86_400_000, Ordering::SeqCst);
    assert!(f.bearer(&forever.plaintext, Listener::Control).is_ok());
}

// ---------------------------------------------------------------------------------------------
// Listeners
// ---------------------------------------------------------------------------------------------

#[test]
fn the_internal_listener_trusts_the_peer_and_honors_x_rt_invoker() {
    let f = fixture(Some(ROOT));
    assert_eq!(
        f.call(&[], Listener::Internal),
        Ok(Principal::Internal { invoker: None })
    );
    assert_eq!(
        f.call(&[("x-rt-invoker", "billing-service")], Listener::Internal),
        Ok(Principal::Internal {
            invoker: Some("billing-service".into())
        })
    );
}

#[test]
fn sec_27_x_rt_invoker_is_never_honored_on_the_public_or_control_listener() {
    let f = fixture(Some(ROOT));
    let spoofed = f.call(&[("x-rt-invoker", "billing-service")], Listener::Public);
    assert_eq!(
        spoofed,
        Ok(Principal::Anonymous),
        "a public request cannot claim to be internal"
    );
    let on_control = f.call(&[("x-rt-invoker", "billing-service")], Listener::Control);
    assert_eq!(
        on_control,
        Err(AuthError::Missing),
        "control needs credentials, an invoker header is not one"
    );
    let k = f.issue(&[Scope::Invoke], None, None);
    let bearer = format!("Bearer {}", k.plaintext);
    let p = f
        .call(
            &[("authorization", bearer.as_str()), ("x-rt-invoker", "x")],
            Listener::Public,
        )
        .unwrap();
    assert!(matches!(p, Principal::Tenant { .. }), "key wins, header ignored: {p:?}");
}

#[test]
fn the_metrics_and_public_listeners_accept_anonymous_peers() {
    let f = fixture(Some(ROOT));
    assert_eq!(f.call(&[], Listener::Metrics), Ok(Principal::Anonymous));
    assert_eq!(f.call(&[], Listener::Public), Ok(Principal::Anonymous));
}

// ---------------------------------------------------------------------------------------------
// Failed-auth limiter (SEC-02)
// ---------------------------------------------------------------------------------------------

fn addr(s: &str) -> IpAddr {
    s.parse().unwrap()
}

#[test]
fn sec_02_ten_failures_per_minute_per_ip_then_blocked() {
    let l = AuthFailureLimiter::new(10);
    let ip = addr("203.0.113.7");
    let t0: UnixMs = NOW;
    for i in 0..10 {
        assert_eq!(
            l.record_failure(ip, t0 + i),
            Ok(()),
            "failure #{} is still answered with 401",
            i + 1
        );
    }
    let retry = l
        .record_failure(ip, t0 + 10)
        .expect_err("the 11th failure in a minute is rate limited");
    assert!(
        retry > Duration::ZERO && retry <= Duration::from_secs(60),
        "retry-after {retry:?}"
    );
    assert!(l.check(ip, t0 + 11).is_err(), "the IP stays blocked");
    assert!(l.check(ip, t0 + 30_000).is_err(), "still blocked 30 s later");
}

#[test]
fn the_failure_limit_is_per_ip() {
    let l = AuthFailureLimiter::new(10);
    let (a, b) = (addr("203.0.113.7"), addr("203.0.113.8"));
    for i in 0..11 {
        let _ = l.record_failure(a, NOW + i);
    }
    assert!(l.check(a, NOW + 20).is_err());
    assert_eq!(l.check(b, NOW + 20), Ok(()), "another IP is unaffected");
    assert_eq!(l.record_failure(b, NOW + 21), Ok(()));
    let v6 = addr("2001:db8::1");
    assert_eq!(l.check(v6, NOW + 22), Ok(()), "IPv6 addresses are tracked separately");
}

#[test]
fn an_ip_that_never_failed_is_not_blocked() {
    let l = AuthFailureLimiter::new(10);
    assert_eq!(l.check(addr("198.51.100.1"), NOW), Ok(()));
}

#[test]
fn the_block_expires_after_the_window() {
    let l = AuthFailureLimiter::new(3);
    let ip = addr("203.0.113.7");
    for i in 0..4 {
        let _ = l.record_failure(ip, NOW + i);
    }
    assert!(l.check(ip, NOW + 5).is_err());
    assert_eq!(l.check(ip, NOW + 61_000), Ok(()), "a minute later the IP may try again");
    assert_eq!(l.record_failure(ip, NOW + 61_001), Ok(()), "and gets a fresh allowance");
}

#[test]
fn the_limit_is_configurable() {
    for max in [1u32, 3, 25] {
        let l = AuthFailureLimiter::new(max);
        let ip = addr("203.0.113.9");
        for i in 0..max {
            assert_eq!(
                l.record_failure(ip, NOW + i64::from(i)),
                Ok(()),
                "max={max} failure {i}"
            );
        }
        assert!(
            l.record_failure(ip, NOW + 1_000).is_err(),
            "max={max}: failure #{} must be limited",
            max + 1
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Quotas
// ---------------------------------------------------------------------------------------------

struct Plans {
    default: RwLock<Plan>,
    per_tenant: RwLock<HashMap<TenantId, Plan>>,
}

impl Plans {
    fn new(default: Plan) -> Arc<Self> {
        Arc::new(Self {
            default: RwLock::new(default),
            per_tenant: RwLock::new(HashMap::new()),
        })
    }
    fn set_default(&self, p: Plan) {
        *self.default.write().unwrap() = p;
    }
    fn set_for(&self, t: TenantId, p: Plan) {
        self.per_tenant.write().unwrap().insert(t, p);
    }
}

impl PlanSource for Plans {
    fn plan_for(&self, tenant: TenantId) -> Plan {
        self.per_tenant
            .read()
            .unwrap()
            .get(&tenant)
            .cloned()
            .unwrap_or_else(|| self.default.read().unwrap().clone())
    }
}

fn enforcer(plan: Plan) -> (LocalQuotaEnforcer, Arc<Plans>) {
    let plans = Plans::new(plan);
    (LocalQuotaEnforcer::new(plans.clone()), plans)
}

fn exceeded(r: Result<(), QuotaError>) -> QuotaError {
    r.expect_err("the operation must be refused")
}

#[test]
fn sec_14_the_eleventh_service_is_refused_with_quota_exceeded() {
    let (q, _) = enforcer(Plan {
        max_services: 10,
        ..Plan::default()
    });
    let t = TenantId::new();
    for current in 0..10 {
        assert_eq!(
            q.check(t, QuotaOp::CreateService { current }),
            Ok(()),
            "creating service #{}",
            current + 1
        );
    }
    for current in [10, 11, 1_000] {
        let e = exceeded(q.check(t, QuotaOp::CreateService { current }));
        assert!(
            matches!(&e, QuotaError::Exceeded { what } if what.contains("service")),
            "{e:?}"
        );
        assert_eq!((e.http_status(), e.code()), (403, "quota_exceeded"));
    }
}

#[test]
fn schedules_are_limited_in_count_and_frequency() {
    let (q, _) = enforcer(Plan {
        max_schedules: 3,
        min_schedule_seconds: 60,
        ..Plan::default()
    });
    let t = TenantId::new();
    assert_eq!(
        q.check(
            t,
            QuotaOp::CreateSchedule {
                current: 2,
                every_seconds: 60
            }
        ),
        Ok(())
    );
    assert_eq!(
        q.check(
            t,
            QuotaOp::CreateSchedule {
                current: 0,
                every_seconds: 86_400
            }
        ),
        Ok(())
    );

    let e = exceeded(q.check(
        t,
        QuotaOp::CreateSchedule {
            current: 3,
            every_seconds: 3_600,
        },
    ));
    assert!(
        matches!(&e, QuotaError::Exceeded { what } if what.contains("schedule")),
        "{e:?}"
    );
    assert_eq!((e.http_status(), e.code()), (403, "quota_exceeded"));

    let e = exceeded(q.check(
        t,
        QuotaOp::CreateSchedule {
            current: 0,
            every_seconds: 59,
        },
    ));
    assert_eq!(e, QuotaError::ScheduleTooFrequent { min_seconds: 60 });
    assert_eq!((e.http_status(), e.code()), (422, "schedule_too_frequent"));
}

#[test]
fn sec_09_a_plan_without_cron_refuses_cron_schedules() {
    let (q, _) = enforcer(Plan::default()); // free: http, interval, queue, call
    let t = TenantId::new();
    for ok in [
        TriggerKind::Http,
        TriggerKind::Interval,
        TriggerKind::Queue,
        TriggerKind::Call,
    ] {
        assert_eq!(q.check(t, QuotaOp::UseTrigger(ok)), Ok(()), "{ok:?}");
    }
    let e = exceeded(q.check(t, QuotaOp::UseTrigger(TriggerKind::Cron)));
    assert_eq!(e, QuotaError::TriggerNotAllowed);
    assert_eq!((e.http_status(), e.code()), (403, "trigger_not_allowed"));

    let with_cron = Plan {
        allowed_triggers: vec![TriggerKind::Http, TriggerKind::Cron],
        ..Plan::default()
    };
    let (q, _) = enforcer(with_cron);
    assert_eq!(q.check(t, QuotaOp::UseTrigger(TriggerKind::Cron)), Ok(()));
    assert!(
        q.check(t, QuotaOp::UseTrigger(TriggerKind::Queue)).is_err(),
        "an explicit list is exhaustive"
    );
}

#[test]
fn sec_15_the_101st_invocation_of_the_day_is_refused_with_retry_after_midnight() {
    let (q, _) = enforcer(Plan {
        invocations_per_day: 100,
        rps: 1_000_000,
        ..Plan::default()
    });
    let t = TenantId::new();
    for i in 0..100 {
        assert_eq!(q.check(t, QuotaOp::Invoke), Ok(()), "invocation #{}", i + 1);
    }
    let e = exceeded(q.check(t, QuotaOp::Invoke));
    match &e {
        QuotaError::InvocationsPerDay { retry_after } => {
            assert!(
                *retry_after > Duration::ZERO && *retry_after <= Duration::from_secs(24 * 3600),
                "until UTC midnight: {retry_after:?}"
            );
        }
        other => panic!("expected InvocationsPerDay, got {other:?}"),
    }
    assert_eq!((e.http_status(), e.code()), (429, "quota_exceeded"));
    // another tenant has its own counter
    assert_eq!(q.check(TenantId::new(), QuotaOp::Invoke), Ok(()));
}

#[test]
fn the_rps_bucket_limits_bursts_and_refills() {
    let (q, _) = enforcer(Plan {
        rps: 10,
        invocations_per_day: 10_000_000,
        ..Plan::default()
    });
    let t = TenantId::new();
    let mut ok = 0;
    let mut limited = None;
    for _ in 0..1_000 {
        match q.check(t, QuotaOp::Invoke) {
            Ok(()) => ok += 1,
            Err(e) => limited = Some(e),
        }
    }
    assert!(
        ok >= 1 && ok <= 30,
        "a 10 rps bucket let {ok} immediate requests through"
    );
    match limited {
        Some(QuotaError::RateLimited { retry_after }) => {
            assert!(
                retry_after > Duration::ZERO && retry_after <= Duration::from_secs(2),
                "{retry_after:?}"
            );
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
    // other tenants have their own bucket
    assert_eq!(q.check(TenantId::new(), QuotaOp::Invoke), Ok(()));
    // tokens come back
    std::thread::sleep(Duration::from_millis(1_200));
    assert_eq!(q.check(t, QuotaOp::Invoke), Ok(()), "the bucket refills over time");
}

#[test]
fn rate_limited_maps_to_429_rate_limited() {
    let e = QuotaError::RateLimited {
        retry_after: Duration::from_millis(100),
    };
    assert_eq!((e.http_status(), e.code()), (429, "rate_limited"));
}

#[test]
fn sec_34_the_21st_build_in_an_hour_is_refused() {
    let (q, _) = enforcer(Plan {
        builds_per_hour: 20,
        ..Plan::default()
    });
    let t = TenantId::new();
    for i in 0..20 {
        assert_eq!(q.check(t, QuotaOp::Build), Ok(()), "build #{}", i + 1);
    }
    let e = exceeded(q.check(t, QuotaOp::Build));
    assert_eq!(e, QuotaError::BuildQuota);
    assert_eq!((e.http_status(), e.code()), (429, "build_quota"));
    assert_eq!(q.check(TenantId::new(), QuotaOp::Build), Ok(()), "per tenant");
}

#[test]
fn the_queue_depth_limit_refuses_enqueues_at_the_limit() {
    let (q, _) = enforcer(Plan {
        max_queue_depth: 10_000,
        ..Plan::default()
    });
    let t = TenantId::new();
    assert_eq!(q.check(t, QuotaOp::Enqueue { depth: 0 }), Ok(()));
    assert_eq!(q.check(t, QuotaOp::Enqueue { depth: 9_999 }), Ok(()));
    let e = exceeded(q.check(t, QuotaOp::Enqueue { depth: 10_000 }));
    assert_eq!(e, QuotaError::QueueFull);
    assert_eq!((e.http_status(), e.code()), (429, "queue_full"));
}

#[test]
fn sec_16_egress_beyond_the_daily_allowance_is_refused() {
    let (q, _) = enforcer(Plan {
        egress_mb_per_day: 1,
        ..Plan::default()
    });
    let t = TenantId::new();
    let svc = ServiceId::new();
    assert_eq!(q.check(t, QuotaOp::Egress { bytes: 100_000 }), Ok(()));
    let e = exceeded(q.check(t, QuotaOp::Egress { bytes: 2 * 1024 * 1024 }));
    assert_eq!(e, QuotaError::EgressQuota);
    assert_eq!(e.code(), "egress_quota");

    // usage accumulates through `record`
    q.record(
        t,
        svc,
        UsageDelta {
            egress_bytes: 800_000,
            ..UsageDelta::default()
        },
    );
    assert_eq!(
        exceeded(q.check(t, QuotaOp::Egress { bytes: 400_000 })),
        QuotaError::EgressQuota,
        "800 kB used + 400 kB > 1 MB"
    );
    assert_eq!(
        q.check(TenantId::new(), QuotaOp::Egress { bytes: 400_000 }),
        Ok(()),
        "other tenants have their own allowance"
    );
}

#[test]
fn storage_growth_is_checked_against_the_plan() {
    let (q, _) = enforcer(Plan {
        storage_mb: 100,
        ..Plan::default()
    });
    let t = TenantId::new();
    let svc = ServiceId::new();
    assert_eq!(
        q.check(
            t,
            QuotaOp::Storage {
                delta_bytes: MIB as u64
            }
        ),
        Ok(())
    );
    let e = exceeded(q.check(
        t,
        QuotaOp::Storage {
            delta_bytes: 200 * MIB as u64,
        },
    ));
    assert_eq!(e, QuotaError::StorageQuota);
    assert_eq!(e.code(), "storage_quota");

    q.record(
        t,
        svc,
        UsageDelta {
            storage_bytes: 99 * MIB,
            ..UsageDelta::default()
        },
    );
    assert_eq!(
        exceeded(q.check(
            t,
            QuotaOp::Storage {
                delta_bytes: 2 * MIB as u64
            }
        )),
        QuotaError::StorageQuota
    );
    q.record(
        t,
        svc,
        UsageDelta {
            storage_bytes: -50 * MIB,
            ..UsageDelta::default()
        },
    );
    assert_eq!(
        q.check(
            t,
            QuotaOp::Storage {
                delta_bytes: 2 * MIB as u64
            }
        ),
        Ok(()),
        "freed space is available again"
    );
}

#[test]
fn plans_are_resolved_per_tenant_and_re_read_on_every_check() {
    let (q, plans) = enforcer(Plan {
        max_services: 2,
        ..Plan::default()
    });
    let (small, big) = (TenantId::new(), TenantId::new());
    plans.set_for(
        big,
        Plan {
            max_services: 1_000,
            ..Plan::default()
        },
    );
    assert!(q.check(small, QuotaOp::CreateService { current: 2 }).is_err());
    assert_eq!(q.check(big, QuotaOp::CreateService { current: 500 }), Ok(()));

    // SIGHUP reloads plans: the enforcer follows the PlanSource without being rebuilt
    plans.set_default(Plan {
        max_services: 5,
        ..Plan::default()
    });
    assert_eq!(q.check(small, QuotaOp::CreateService { current: 4 }), Ok(()));
    plans.set_default(Plan {
        max_services: 1,
        ..Plan::default()
    });
    assert!(q.check(small, QuotaOp::CreateService { current: 1 }).is_err());
}

#[test]
fn quota_errors_map_to_the_documented_status_and_code() {
    let secs = Duration::from_secs(5);
    let table: Vec<(QuotaError, u16, &str)> = vec![
        (QuotaError::Exceeded { what: "services" }, 403, "quota_exceeded"),
        (QuotaError::TriggerNotAllowed, 403, "trigger_not_allowed"),
        (
            QuotaError::ScheduleTooFrequent { min_seconds: 60 },
            422,
            "schedule_too_frequent",
        ),
        (QuotaError::RateLimited { retry_after: secs }, 429, "rate_limited"),
        (
            QuotaError::InvocationsPerDay { retry_after: secs },
            429,
            "quota_exceeded",
        ),
        (QuotaError::BuildQuota, 429, "build_quota"),
        (QuotaError::QueueFull, 429, "queue_full"),
        (QuotaError::StorageQuota, 413, "storage_quota"),
        (QuotaError::EgressQuota, 413, "egress_quota"),
    ];
    for (e, status, code) in table {
        assert_eq!((e.http_status(), e.code()), (status, code), "{e:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// Audit (SEC-20)
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Collect(Mutex<Vec<AuditEvent>>);

impl AuditSink for Collect {
    fn emit(&self, ev: AuditEvent) {
        self.0.lock().unwrap().push(ev);
    }
}

#[test]
fn audit_actors_serialize_with_a_kind_tag() {
    let t = TenantId::from_u128(1);
    assert_eq!(
        serde_json::to_value(Actor::Operator).unwrap(),
        serde_json::json!({"kind": "operator"})
    );
    assert_eq!(
        serde_json::to_value(Actor::System).unwrap(),
        serde_json::json!({"kind": "system"})
    );
    assert_eq!(
        serde_json::to_value(Actor::Key {
            key_id: "key_x".into(),
            tenant: t
        })
        .unwrap(),
        serde_json::json!({"kind": "key", "key_id": "key_x", "tenant": "ten_00000000000000000000000001"})
    );
}

#[test]
fn audit_events_round_trip_through_json_and_flow_through_a_sink() {
    let ev = AuditEvent {
        ts_ms: NOW,
        actor: Actor::Key {
            key_id: KeyId::from_u128(9).to_string(),
            tenant: TenantId::from_u128(1),
        },
        tenant: Some(TenantId::from_u128(1)),
        service: Some(ServiceId::from_u128(2)),
        action: "key.created".into(),
        data: serde_json::json!({"name": "ci", "scopes": ["deploy"]}),
    };
    let back: AuditEvent = serde_json::from_str(&serde_json::to_string(&ev).unwrap()).unwrap();
    assert_eq!(back, ev);

    let collected = Arc::new(Collect::default());
    let sink: Arc<dyn AuditSink> = collected.clone();
    sink.emit(ev.clone());
    assert_eq!(collected.0.lock().unwrap().as_slice(), std::slice::from_ref(&ev));
    TracingAuditSink.emit(ev); // writes a tracing line; must not panic without a subscriber
}

// ---------------------------------------------------------------------------------------------
// Hardening (SEC-33)
// ---------------------------------------------------------------------------------------------

#[test]
fn sec_33_hardening_makes_the_process_non_dumpable_and_disables_core_files() {
    harden_process(&HardeningConfig::default()).expect("hardening");
    match is_dumpable() {
        Some(dumpable) => {
            assert!(cfg!(target_os = "linux"), "only Linux reports dumpability");
            assert!(!dumpable, "PR_SET_DUMPABLE must be 0 after hardening");
        }
        None => assert!(!cfg!(target_os = "linux"), "on Linux the dump state is always readable"),
    }
    #[cfg(target_os = "linux")]
    {
        let mut lim = libc::rlimit {
            rlim_cur: 1,
            rlim_max: 1,
        };
        // SAFETY: getrlimit writes into the valid rlimit we pass.
        let rc = unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut lim) };
        assert_eq!(rc, 0);
        assert_eq!((lim.rlim_cur, lim.rlim_max), (0, 0), "RLIMIT_CORE must be 0");
    }
    // idempotent
    harden_process(&HardeningConfig::default()).expect("second call");
}

// ---------------------------------------------------------------------------------------------
// Key cache: fill/revoke race, tenant invalidation, bounded eviction
// ---------------------------------------------------------------------------------------------

/// A lookup that, while returning a (now stale) copy, lets a revocation land mid-fill.
#[derive(Default)]
struct RacyStore {
    inner: Store,
    auth: Mutex<Option<std::sync::Weak<DefaultAuthenticator>>>,
    race_once: std::sync::atomic::AtomicBool,
}

impl KeyLookup for RacyStore {
    fn find(&self, id: KeyId) -> Option<ApiKey> {
        let stale = self.inner.find(id);
        if self.race_once.swap(false, Ordering::SeqCst) {
            self.inner.keys.lock().unwrap().get_mut(&id).unwrap().revoked = true;
            let auth = self.auth.lock().unwrap().as_ref().and_then(|w| w.upgrade()).unwrap();
            auth.invalidate_key(id);
        }
        stale
    }
}

fn racy_fixture() -> (Arc<DefaultAuthenticator>, Arc<RacyStore>, KeyHasher) {
    let store = Arc::new(RacyStore::default());
    let hasher = KeyHasher::new(&master());
    let auth = Arc::new(DefaultAuthenticator::new(
        Some(ROOT.into()),
        hasher.clone(),
        store.clone(),
        Arc::new(Clock(AtomicI64::new(NOW))),
    ));
    *store.auth.lock().unwrap() = Some(Arc::downgrade(&auth));
    (auth, store, hasher)
}

fn put_key(store: &Store, hasher: &KeyHasher, tenant: TenantId) -> (String, KeyId) {
    let (secret, new) = hasher.issue(KeySpec {
        name: None,
        scopes: [Scope::Deploy].into_iter().collect(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
    });
    let key = ApiKey {
        id: new.id,
        tenant_id: tenant,
        name: new.name,
        scopes: new.scopes,
        project_ids: new.project_ids,
        environments: new.environments,
        expires_at_ms: None,
        created_at_ms: NOW,
        last_used_at_ms: None,
        revoked: false,
        parent_key_id: None,
        hmac: new.hmac,
    };
    store.keys.lock().unwrap().insert(key.id, key);
    (secret.expose().to_string(), new.id)
}

fn bearer_on(auth: &DefaultAuthenticator, token: &str) -> Result<Principal, AuthError> {
    let (parts, ()) = http::Request::builder()
        .header("authorization", format!("Bearer {token}"))
        .body(())
        .unwrap()
        .into_parts();
    auth.authenticate(&parts, Listener::Control)
}

#[test]
fn sec_06_a_revocation_racing_a_cache_fill_does_not_recache_the_stale_key() {
    let (auth, store, hasher) = racy_fixture();
    let (token, _) = put_key(&store.inner, &hasher, TenantId::new());
    store.race_once.store(true, Ordering::SeqCst);
    // The in-flight request saw the key before revocation; that one answer may succeed.
    let _ = bearer_on(&auth, &token);
    assert_eq!(auth.cached_len(), 0, "the stale fill must not stay cached");
    assert_eq!(
        bearer_on(&auth, &token),
        Err(AuthError::Invalid),
        "the next request sees the revocation"
    );
}

#[test]
fn invalidate_tenant_drops_only_that_tenants_cached_keys() {
    let f = fixture(Some(ROOT));
    let a = f.issue(&[Scope::Deploy], None, None);
    let b = f.issue(&[Scope::Deploy], None, None);
    assert!(f.bearer(&a.plaintext, Listener::Control).is_ok());
    assert!(f.bearer(&b.plaintext, Listener::Control).is_ok());
    assert_eq!(f.auth.cached_len(), 2);
    // tenant A disabled: the source stops serving its keys and the cache is told
    f.store.keys.lock().unwrap().remove(&a.id);
    f.auth.invalidate_tenant(a.tenant);
    assert_eq!(f.auth.cached_len(), 1);
    assert_eq!(f.bearer(&a.plaintext, Listener::Control), Err(AuthError::Invalid));
    assert!(f.bearer(&b.plaintext, Listener::Control).is_ok());
}

#[test]
fn the_key_cache_stays_bounded_under_many_distinct_keys() {
    let f = fixture(Some(ROOT));
    let tenant = TenantId::new();
    let tokens: Vec<String> = (0..4200).map(|_| put_key(&f.store, &f.hasher, tenant).0).collect();
    for t in &tokens {
        assert!(f.bearer(t, Listener::Control).is_ok());
    }
    assert!(f.auth.cached_len() <= 4096, "cache size {}", f.auth.cached_len());
}

// ---------------------------------------------------------------------------------------------
// Edge cases (edge-cases.md §1.1, §1.2)
// ---------------------------------------------------------------------------------------------

/// AUTH-003: the `Bearer` scheme is case-insensitive and any whitespace may separate it from the
/// token.
#[test]
fn auth_003_the_bearer_scheme_ignores_case_and_extra_whitespace() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, None);
    for h in [
        format!("bEaReR    {ROOT}"),
        format!("Bearer\t{ROOT}"),
        format!("  BEARER {ROOT}  "),
    ] {
        assert_eq!(
            f.call(&[("authorization", h.as_str())], Listener::Control),
            Ok(Principal::Operator),
            "`{h}`"
        );
    }
    let h = format!("bearer \t {}", k.plaintext);
    assert!(matches!(
        f.call(&[("authorization", h.as_str())], Listener::Control),
        Ok(Principal::Tenant { .. })
    ));
}

/// AUTH-004: an empty, scheme-only, foreign-scheme or non-UTF-8 `Authorization` never
/// authenticates; a header that is not text is ignored (`Missing`), never a panic.
#[test]
fn auth_004_empty_scheme_only_and_binary_authorization_headers_do_not_authenticate() {
    let f = fixture(Some(ROOT));
    for h in ["", "Bearer", "Bearer   ", "Basic dXNlcjpwYXNz"] {
        let r = f.call(&[("authorization", h)], Listener::Control);
        assert!(
            matches!(r, Err(AuthError::Missing | AuthError::Invalid)),
            "`{h}`: {r:?}"
        );
    }
    let mut b = http::Request::builder().uri("/v1/services");
    b = b.header(
        "authorization",
        http::HeaderValue::from_bytes(b"Bearer \xff\xfe").unwrap(),
    );
    let (parts, ()) = b.body(()).unwrap().into_parts();
    assert_eq!(f.auth.authenticate(&parts, Listener::Control), Err(AuthError::Missing));
}

/// AUTH-005: an app's own bearer next to a valid `x-rt-key` authenticates through the key, and
/// such requests never count as failures (the limiter only records requests where every
/// credential failed).
#[test]
fn auth_005_a_foreign_bearer_beside_a_valid_x_rt_key_is_not_a_failure() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, None);
    let peer = addr("203.0.113.7");
    for i in 0..25 {
        let mut b = http::Request::builder()
            .uri("/v1/services")
            .header("authorization", "Bearer app-token-xyz")
            .header("x-rt-key", k.plaintext.as_str());
        b = b.extension(peer);
        let (parts, ()) = b.body(()).unwrap().into_parts();
        let r = f.auth.authenticate(&parts, Listener::Control);
        assert!(matches!(r, Ok(Principal::Tenant { .. })), "request {i}: {r:?}");
    }
}

/// AUTH-006: credentials are tried bearer first: the root token as bearer wins over a tenant key
/// in `x-rt-key`.
#[test]
fn auth_006_a_root_bearer_wins_over_a_tenant_x_rt_key() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, None);
    let bearer = format!("Bearer {ROOT}");
    let r = f.call(
        &[("authorization", bearer.as_str()), ("x-rt-key", k.plaintext.as_str())],
        Listener::Control,
    );
    assert_eq!(r, Ok(Principal::Operator));
    let bearer = format!("Bearer {}", k.plaintext);
    let r = f.call(
        &[("authorization", bearer.as_str()), ("x-rt-key", ROOT)],
        Listener::Control,
    );
    assert!(
        matches!(r, Ok(Principal::Tenant { .. })),
        "the root token in x-rt-key never makes the operator: {r:?}"
    );
}

/// AUTH-007: a configured root token with leading or trailing whitespace could never match (the
/// header token is trimmed), so startup refuses it with a message instead of locking the operator
/// out silently.
#[test]
fn auth_007_a_root_token_with_surrounding_whitespace_is_refused_at_startup() {
    let good = "0123456789abcdef0123456789abcdef";
    assert!(rt_sec::check_root_token(good, "0.0.0.0:8081").is_ok());
    for bad in [
        format!("{good} "),
        format!(" {good}"),
        format!("{good}\t"),
        format!("{good}\r"),
    ] {
        let err = rt_sec::check_root_token(&bad, "127.0.0.1:8081").expect_err(&bad);
        assert!(err.contains("whitespace"), "{err}");
    }
}

/// AUTH-010: an open (debug, tokenless) control API may bind only a loopback `host:port`; a Unix
/// socket, a bare host or a host name is refused (decided: a socket is reachable by every local
/// user the file mode allows, so it is not treated as loopback).
#[test]
fn auth_010_an_open_control_api_refuses_unix_sockets_and_bare_hosts() {
    for bad in [
        "unix:/tmp/c.sock",
        "myhost:8081",
        "127.0.0.1",
        "localhost",
        "localhost:",
        "localhost:80a",
    ] {
        assert!(rt_sec::check_open_control_bind(bad).is_err(), "{bad}");
    }
}

/// AUTH-012: in open debug mode only a request without credentials is the operator; a wrong
/// credential is `Invalid` and counts as a failure.
#[test]
fn auth_012_open_mode_does_not_turn_a_wrong_credential_into_the_operator() {
    if !cfg!(debug_assertions) {
        return;
    }
    let f = fixture(None);
    let peer = addr("127.0.0.1");
    let mut last = None;
    for _ in 0..11 {
        let b = http::Request::builder()
            .uri("/v1/services")
            .header("authorization", "Bearer garbage")
            .extension(peer);
        let (parts, ()) = b.body(()).unwrap().into_parts();
        last = Some(f.auth.authenticate(&parts, Listener::Control));
        assert!(!matches!(last, Some(Ok(_))), "{last:?}");
    }
    assert!(
        matches!(last, Some(Err(AuthError::RateLimited { .. }))),
        "failures are counted: {last:?}"
    );
}

/// KEY-003: ULIDs are case-insensitive, so a key presented with a lower-case id authenticates as
/// the same key (one identity: same `KeyId`), and a wrong secret still fails.
#[test]
fn key_003_a_lower_case_key_id_is_the_same_key() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, None);
    let (_, rest) = k.plaintext.split_once('_').unwrap();
    let (id, secret) = rest.split_once('_').unwrap();
    let lower = format!("rtk_{}_{secret}", id.to_ascii_lowercase());
    match f.bearer(&lower, Listener::Control) {
        Ok(Principal::Tenant { key, tenant, .. }) => {
            assert_eq!(key, k.id);
            assert_eq!(tenant, k.tenant);
        }
        other => panic!("{other:?}"),
    }
    let wrong = format!("rtk_{}_{}", id.to_ascii_lowercase(), "A".repeat(secret.len()));
    assert_eq!(f.bearer(&wrong, Listener::Control), Err(AuthError::Invalid));
}

/// KEY-007 / AUTH-023: expiry is checked on every request (not cached): valid 1 ms before
/// `expires_at_ms`, invalid at exactly `expires_at_ms`, even for a cached key.
#[test]
fn key_007_a_key_is_invalid_at_exactly_its_expiry_instant_even_when_cached() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, Some(NOW + 500));
    assert!(f.bearer(&k.plaintext, Listener::Control).is_ok(), "warms the cache");
    f.clock.0.store(NOW + 499, Ordering::SeqCst);
    assert!(f.bearer(&k.plaintext, Listener::Control).is_ok(), "1 ms before");
    f.clock.0.store(NOW + 500, Ordering::SeqCst);
    assert_eq!(
        f.bearer(&k.plaintext, Listener::Control),
        Err(AuthError::Invalid),
        "at the instant"
    );
}

/// AUTH-022: a stored scope change reaches a cached key within the 1 s cache TTL, without an
/// explicit invalidation.
#[test]
fn auth_022_a_stored_scope_change_is_seen_after_the_cache_ttl() {
    let f = fixture(Some(ROOT));
    let k = f.issue(&[Scope::Deploy], None, None);
    let scopes = |r: Result<Principal, AuthError>| match r {
        Ok(Principal::Tenant { scopes, .. }) => scopes,
        other => panic!("{other:?}"),
    };
    assert!(scopes(f.bearer(&k.plaintext, Listener::Control)).contains(Scope::Deploy));
    f.store.keys.lock().unwrap().get_mut(&k.id).unwrap().scopes = [Scope::ServicesRead].into_iter().collect();
    std::thread::sleep(Duration::from_millis(1_100));
    let now = scopes(f.bearer(&k.plaintext, Listener::Control));
    assert!(
        now.contains(Scope::ServicesRead) && !now.contains(Scope::Deploy),
        "{now:?}"
    );
}

/// QUOTA-009: a request the daily invocation quota refuses takes no rate token: once the day's
/// budget is spent, refusals do not drain the per-second bucket either.
#[test]
fn quota_009_a_day_quota_refusal_takes_no_rate_token() {
    let (q, _) = enforcer(Plan {
        rps: 1,
        invocations_per_day: 1,
        ..Plan::default()
    });
    let t = TenantId::new();
    assert_eq!(q.check(t, QuotaOp::Invoke), Ok(()));
    std::thread::sleep(Duration::from_millis(1_100));
    // The bucket is full again (1 token); the day is spent: every refusal is the day quota, never
    // the rate limit, however many arrive in the same second.
    for i in 0..5 {
        let e = exceeded(q.check(t, QuotaOp::Invoke));
        assert!(matches!(e, QuotaError::InvocationsPerDay { .. }), "refusal {i}: {e:?}");
    }
}

/// QUOTA-011: a wall clock stepping back across midnight (NTP) does not reset today's counters.
#[test]
fn quota_011_a_clock_stepping_back_over_midnight_keeps_the_count() {
    let clock = Arc::new(Clock(AtomicI64::new(NOW - NOW % 86_400_000 + 1_000)));
    let plans = Plans::new(Plan {
        rps: 0,
        invocations_per_day: 2,
        ..Plan::default()
    });
    let q = LocalQuotaEnforcer::with_clock(plans, clock.clone());
    let t = TenantId::new();
    assert_eq!(q.check(t, QuotaOp::Invoke), Ok(()));
    assert_eq!(q.check(t, QuotaOp::Invoke), Ok(()));
    clock.0.fetch_sub(5_000, Ordering::SeqCst); // back into yesterday
    assert!(matches!(
        exceeded(q.check(t, QuotaOp::Invoke)),
        QuotaError::InvocationsPerDay { .. }
    ));
    clock.0.fetch_add(86_400_000 + 10_000, Ordering::SeqCst); // tomorrow
    assert_eq!(q.check(t, QuotaOp::Invoke), Ok(()), "a real new day resets");
}

/// LIMITER-002: while an IP is blocked even the correct root token from it is `RateLimited`.
#[test]
fn limiter_002_a_blocked_ip_is_refused_even_with_the_right_token() {
    let f = fixture(Some(ROOT));
    let peer = addr("203.0.113.70");
    let req = |token: &str| {
        let b = http::Request::builder()
            .uri("/v1/services")
            .header("authorization", format!("Bearer {token}"))
            .extension(peer);
        let (parts, ()) = b.body(()).unwrap().into_parts();
        f.auth.authenticate(&parts, Listener::Control)
    };
    for _ in 0..11 {
        let _ = req("wrong");
    }
    assert!(matches!(req(ROOT), Err(AuthError::RateLimited { .. })));
    f.clock.0.fetch_add(61_000, Ordering::SeqCst);
    assert_eq!(req(ROOT), Ok(Principal::Operator), "after the window");
}

/// LIMITER-003: the window opens at the first failure and closes exactly 60 000 ms later.
#[test]
fn limiter_003_the_window_closes_at_exactly_one_minute() {
    let l = AuthFailureLimiter::new(10);
    let ip = addr("203.0.113.71");
    for i in 0..10 {
        assert_eq!(l.record_failure(ip, NOW + i), Ok(()));
    }
    assert!(
        l.record_failure(ip, NOW + 59_999).is_err(),
        "the 11th inside the window"
    );
    let l = AuthFailureLimiter::new(10);
    for i in 0..10 {
        assert_eq!(l.record_failure(ip, NOW + i), Ok(()));
    }
    assert_eq!(l.record_failure(ip, NOW + 60_000), Ok(()), "a new window at +60 000 ms");
}

/// LIMITER-010: failures from one IP recorded from many threads at once admit exactly `max`.
#[test]
fn limiter_010_concurrent_failures_never_admit_more_than_max() {
    let l = Arc::new(AuthFailureLimiter::new(10));
    let ip = addr("203.0.113.72");
    let admitted = std::sync::atomic::AtomicU32::new(0);
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                for _ in 0..50 {
                    if l.record_failure(ip, NOW).is_ok() {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    assert_eq!(admitted.load(Ordering::Relaxed), 10);
}
