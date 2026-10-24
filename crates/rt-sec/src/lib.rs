//! rt-sec (L2): authentication, API keys, scopes, secrets crypto, artifact MAC, quotas, audit,
//! isolation, SSRF classification and process hardening (SECURITY.md).
//!
//! Extension points (`Authenticator`, `SecretsBackend`, `AuditSink`, `IsolationBackend`) are
//! injected by `rt-server` through [`Plugins`]; the core binary uses the defaults here.

// Request-path hygiene (security review step 5c): no panicking unwraps outside tests.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]
#![warn(clippy::await_holding_lock, clippy::mem_forget, clippy::undocumented_unsafe_blocks)]

pub mod audit;
pub mod auth;
pub mod delegate;
pub mod egress;
pub mod harden;
pub mod ingress;
pub mod isolation;
pub mod keys;
pub mod mac;
pub mod master;
pub mod quota;
pub mod secrets;

use std::sync::Arc;

pub use audit::{Actor, AuditEvent, AuditSink, TracingAuditSink};
pub use auth::{
    check_open_control_bind, check_root_token, control_open_without_token, limiter_key, AuthError, AuthFailureLimiter,
    Authenticator, ClientIp, DefaultAuthenticator, KeyLookup, Principal, SystemClock, WallClock, LIMITER_CAPACITY,
    MIN_ROOT_TOKEN_LEN,
};
pub use delegate::{delegate, Escalation, Grant};
pub use egress::{
    embedded_ipv4, host_allowed, host_denied, is_blocked_ip, local_interface_addrs, EgressDenylist, HostResolver,
    SelfDenylist,
};
pub use harden::{harden_process, is_dumpable, HardenError, HardeningConfig};
pub use ingress::{ip_allowed, parse_cidr, CidrParseError, IpAllowList, TrustedProxies, UnixPeerPolicy};
pub use isolation::{IsolationBackend, IsolationDomain, LocalIsolation};
pub use keys::{
    format_key, generate_secret, parse_key, KeyHasher, KeyParseError, KeySpec, ParsedKey, KEY_PREFIX, SECRET_LEN,
};
pub use mac::{ArtifactMac, HkdfArtifactMac, Mac};
pub use master::{key_file_mode_problem, purpose, MasterKey, MasterKeyError};
pub use quota::{LocalQuotaEnforcer, PlanSource, QuotaEnforcer, QuotaError, QuotaOp, QuotaUsage};
pub use rt_types::{Scope, Scopes};
pub use secrets::{
    secret_aad, LocalSecrets, SealKeyId, Sealed, SecretError, SecretsBackend, SEALED_V1, SEAL_KEY_ID_LEN,
};

/// Injectable implementations of the extension points.
#[derive(Clone)]
pub struct Plugins {
    pub authenticator: Arc<dyn Authenticator>,
    pub secrets: Arc<dyn SecretsBackend>,
    pub audit: Arc<dyn AuditSink>,
    pub isolation: Arc<dyn IsolationBackend>,
}
