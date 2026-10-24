//! Operator SDKs as runtime services: the system tenant that owns them,
//! releases, channels, grants, and the grant resolution every SDK call goes through.
//!
//! An SDK is a service of the system tenant `sys`; a release is one of its versions, tagged with a
//! semver. Tenants never get SDK code: their build sees the contract only, and each call is bound
//! at run time to the release the caller's grants select. Grants attach to a scope (node, plan,
//! tenant, project, environment) and are inherited downwards, where a lower level can only narrow:
//! a deny anywhere in the chain wins, every `version_req` of the chain must hold, and the most
//! specific `channel` picks the release (default `stable`).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::digest::Digest;
use crate::ids::{ProjectId, ServiceId, TenantId, VersionNo};
use crate::kinds::RuntimeKind;
use crate::model::UnixMs;

/// The system tenant's id, `ten_00000000000000000000000SYS`: it owns the operator SDK services.
/// It cannot be created, changed or deleted through the API, and no key belongs to it.
pub fn system_tenant() -> TenantId {
    TenantId::from_u128(SYSTEM_TENANT_U128)
}

/// `ten_00000000000000000000000SYS` as a number (Crockford base32 `SYS`).
pub const SYSTEM_TENANT_U128: u128 = 26_585;
/// The system tenant's name and plan.
pub const SYSTEM_TENANT_NAME: &str = "sys";
pub const SYSTEM_PLAN: &str = "sys";
/// The channel a grant without one follows.
pub const DEFAULT_CHANNEL: &str = "stable";
/// Slugs with this prefix belong to the system tenant.
pub const SYSTEM_SLUG_PREFIX: &str = "sys-";

pub fn is_system_tenant(t: TenantId) -> bool {
    t == system_tenant()
}

/// An SDK name: `^[a-z][a-z0-9_]{1,31}$` (it is also the `std:sdk` export name).
pub fn valid_sdk_name(s: &str) -> bool {
    let b = s.as_bytes();
    (2..=32).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

/// A channel name: `^[a-z][a-z0-9-]{0,31}$`.
pub fn valid_channel(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=32).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// The slug of the system service of SDK `name` (`acme_pay` -> `sys-sdk-acme-pay`).
pub fn sdk_service_slug(name: &str) -> String {
    format!("{SYSTEM_SLUG_PREFIX}sdk-{}", name.replace('_', "-"))
}

/// `1.4.2` (semver, no build metadata).
pub fn parse_version(s: &str) -> Result<semver::Version, String> {
    let v = semver::Version::parse(s).map_err(|e| format!("`{s}` is not a semver version: {e}"))?;
    if !v.build.is_empty() {
        return Err(format!("`{s}`: build metadata is not allowed"));
    }
    Ok(v)
}

/// `^1`, `~1.4`, `>=1.2, <2`, `1.4.2` (an exact version means `=1.4.2`).
pub fn parse_version_req(s: &str) -> Result<semver::VersionReq, String> {
    let s = s.trim();
    let req = if s.chars().next().is_some_and(|c| c.is_ascii_digit()) && semver::Version::parse(s).is_ok() {
        format!("={s}")
    } else {
        s.to_string()
    };
    semver::VersionReq::parse(&req).map_err(|e| format!("`{s}` is not a version requirement: {e}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SdkState {
    Active,
    /// `DELETE /v1/sdks/{name}`: calls answer `sdk_unavailable`; publishing again restores it.
    Removed,
}

impl SdkState {
    pub fn as_str(self) -> &'static str {
        match self {
            SdkState::Active => "active",
            SdkState::Removed => "removed",
        }
    }
}

/// One SDK (the name tenants import).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sdk {
    pub name: String,
    /// Its service in the system tenant.
    pub service_id: ServiceId,
    pub description: Option<String>,
    pub state: SdkState,
    pub created_at_ms: UnixMs,
    pub removed_at_ms: Option<UnixMs>,
}

/// One release: a version of the SDK's service, tagged with a semver.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdkRelease {
    pub sdk: String,
    pub version: String,
    pub version_no: VersionNo,
    /// Top-level names of its contract (`std:sdk` / `sdk:<name>` shims export them).
    pub members: Vec<String>,
    /// The published `.d.ts` (empty in route snapshots).
    #[serde(default)]
    pub contract: String,
    pub created_at_ms: UnixMs,
}

/// A data for publishing a release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewSdkRelease {
    pub sdk: String,
    pub version: String,
    pub version_no: VersionNo,
    pub members: Vec<String>,
    pub contract: String,
}

/// A named pointer to a release (`stable`, `canary`, …); rollback moves it back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdkChannel {
    pub sdk: String,
    pub channel: String,
    pub version: String,
    /// The version it pointed to before the last move (`21ci sdk rollback`).
    pub previous: Option<String>,
    pub updated_at_ms: UnixMs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GrantEffect {
    Allow,
    Deny,
}

impl GrantEffect {
    pub fn as_str(self) -> &'static str {
        match self {
            GrantEffect::Allow => "allow",
            GrantEffect::Deny => "deny",
        }
    }
}

impl FromStr for GrantEffect {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "allow" => Ok(GrantEffect::Allow),
            "deny" => Ok(GrantEffect::Deny),
            other => Err(format!("unknown effect `{other}` (allow|deny)")),
        }
    }
}

/// Where a grant attaches. Written as `node`, `plan:<name>`, `tenant:ten_…`, `project:prj_…`,
/// `environment:prj_…/<env>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum GrantScope {
    Node,
    Plan(String),
    Tenant(TenantId),
    Project(ProjectId),
    Environment(ProjectId, String),
}

impl GrantScope {
    /// `(scope_kind, scope_id)` as stored.
    pub fn parts(&self) -> (&'static str, String) {
        match self {
            GrantScope::Node => ("node", String::new()),
            GrantScope::Plan(p) => ("plan", p.clone()),
            GrantScope::Tenant(t) => ("tenant", t.to_string()),
            GrantScope::Project(p) => ("project", p.to_string()),
            GrantScope::Environment(p, e) => ("environment", format!("{p}/{e}")),
        }
    }

    pub fn from_parts(kind: &str, id: &str) -> Result<Self, String> {
        let bad = || format!("invalid {kind} scope `{id}`");
        match kind {
            "node" if id.is_empty() => Ok(GrantScope::Node),
            "plan" if !id.is_empty() && id.len() <= 64 => Ok(GrantScope::Plan(id.to_string())),
            "tenant" => id.parse().map(GrantScope::Tenant).map_err(|_| bad()),
            "project" => id.parse().map(GrantScope::Project).map_err(|_| bad()),
            "environment" => {
                let (p, e) = id.split_once('/').ok_or_else(bad)?;
                if !crate::model::valid_environment(e) {
                    return Err(bad());
                }
                Ok(GrantScope::Environment(p.parse().map_err(|_| bad())?, e.to_string()))
            }
            other => Err(format!(
                "unknown scope `{other}` (node|plan|tenant|project|environment)"
            )),
        }
    }

    /// The level in the chain, from the top (node = 0).
    pub fn level(&self) -> u8 {
        match self {
            GrantScope::Node => 0,
            GrantScope::Plan(_) => 1,
            GrantScope::Tenant(_) => 2,
            GrantScope::Project(_) => 3,
            GrantScope::Environment(..) => 4,
        }
    }
}

impl fmt::Display for GrantScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.parts() {
            ("node", _) => f.write_str("node"),
            (kind, id) => write!(f, "{kind}:{id}"),
        }
    }
}

impl FromStr for GrantScope {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.split_once(':') {
            None => GrantScope::from_parts(s, ""),
            Some((kind, id)) => GrantScope::from_parts(kind, id),
        }
    }
}

impl Serialize for GrantScope {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for GrantScope {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// One grant: what a scope may use of one SDK (or of every SDK, `sdk = "*"`). A field left
/// `None` inherits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdkGrant {
    pub scope: GrantScope,
    /// An SDK name or `*`.
    pub sdk: String,
    pub effect: Option<GrantEffect>,
    pub version_req: Option<String>,
    pub channel: Option<String>,
    /// Who wrote it (audit).
    pub created_by: String,
    pub updated_at_ms: UnixMs,
}

/// A release as the data plane needs it: what to run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdkTarget {
    pub sdk: String,
    pub version: String,
    pub service: ServiceId,
    pub version_no: VersionNo,
    pub digest: Digest,
    pub runtime: RuntimeKind,
    pub members: Vec<String>,
}

/// Every SDK fact the node needs in memory: part of the route snapshot, so a change reaches every
/// node with the routes (no tenant service is rebuilt or redeployed).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdkSnapshot {
    pub sdks: Vec<Sdk>,
    pub releases: Vec<SdkTarget>,
    pub channels: Vec<SdkChannel>,
    pub grants: Vec<SdkGrant>,
}

/// Who calls an SDK, for grant resolution.
#[derive(Clone, Copy, Debug)]
pub struct SdkCaller<'a> {
    pub tenant: TenantId,
    pub plan: &'a str,
    pub project: ProjectId,
    pub environment: &'a str,
}

/// Why a call is not bound to a release. Codes are stable (`x-rt-reason`, `SdkError.code`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SdkDenial {
    /// No such SDK (`404 sdk_not_found`).
    NotFound,
    /// The SDK was removed (`410 sdk_unavailable`).
    Removed,
    /// The grants do not allow it (`403 sdk_not_granted`).
    NotGranted,
    /// No release satisfies the grants (`503 sdk_version_unavailable`).
    VersionUnavailable,
}

impl SdkDenial {
    pub fn code(&self) -> &'static str {
        match self {
            SdkDenial::NotFound => "sdk_not_found",
            SdkDenial::Removed => "sdk_unavailable",
            SdkDenial::NotGranted => "sdk_not_granted",
            SdkDenial::VersionUnavailable => "sdk_version_unavailable",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            SdkDenial::NotFound => 404,
            SdkDenial::Removed => 410,
            SdkDenial::NotGranted => 403,
            SdkDenial::VersionUnavailable => 503,
        }
    }

    pub fn message(&self, sdk: &str) -> String {
        match self {
            SdkDenial::NotFound => format!("there is no SDK `{sdk}`"),
            SdkDenial::Removed => format!("the SDK `{sdk}` was removed"),
            SdkDenial::NotGranted => format!("the SDK `{sdk}` is not granted to this service"),
            SdkDenial::VersionUnavailable => format!("no release of the SDK `{sdk}` satisfies the grants"),
        }
    }
}

/// The merged grant chain of one caller for one SDK.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct EffectiveGrant {
    pub allowed: bool,
    /// The deny's scope, when one applies.
    pub denied_by: Option<String>,
    pub version_reqs: Vec<String>,
    pub channel: String,
    /// The scopes that contributed, from the top.
    pub chain: Vec<String>,
}

impl SdkSnapshot {
    pub fn sdk(&self, name: &str) -> Option<&Sdk> {
        self.sdks.iter().find(|s| s.name == name)
    }

    pub fn channel(&self, sdk: &str, channel: &str) -> Option<&SdkChannel> {
        self.channels.iter().find(|c| c.sdk == sdk && c.channel == channel)
    }

    /// The release a channel (`stable` by default) points to.
    pub fn channel_release(&self, sdk: &str, channel: &str) -> Option<&SdkTarget> {
        let c = self.channel(sdk, channel)?;
        self.releases.iter().find(|r| r.sdk == sdk && r.version == c.version)
    }

    /// The grant chain of `caller` for `sdk`, merged from the node down.
    pub fn effective_grant(&self, caller: &SdkCaller<'_>, sdk: &str) -> EffectiveGrant {
        let chain = [
            GrantScope::Node,
            GrantScope::Plan(caller.plan.to_string()),
            GrantScope::Tenant(caller.tenant),
            GrantScope::Project(caller.project),
            GrantScope::Environment(caller.project, caller.environment.to_string()),
        ];
        let mut out = EffectiveGrant {
            channel: DEFAULT_CHANNEL.to_string(),
            ..EffectiveGrant::default()
        };
        let mut denied = false;
        for scope in &chain {
            // At one level a grant for the SDK by name comes before the `*` one.
            for name in [sdk, "*"] {
                let Some(g) = self.grants.iter().find(|g| g.scope == *scope && g.sdk == name) else {
                    continue;
                };
                out.chain.push(format!("{scope} {name}"));
                match g.effect {
                    Some(GrantEffect::Deny) => {
                        denied = true;
                        out.denied_by.get_or_insert_with(|| scope.to_string());
                    }
                    Some(GrantEffect::Allow) => out.allowed = true,
                    None => {}
                }
                if let Some(r) = &g.version_req {
                    out.version_reqs.push(r.clone());
                }
                if let Some(c) = &g.channel {
                    out.channel = c.clone();
                }
            }
        }
        out.allowed &= !denied;
        out
    }

    /// The release a call of `caller` to `sdk` runs.
    pub fn resolve(&self, caller: &SdkCaller<'_>, sdk: &str) -> Result<&SdkTarget, SdkDenial> {
        let s = self.sdk(sdk).ok_or(SdkDenial::NotFound)?;
        if s.state == SdkState::Removed {
            return Err(SdkDenial::Removed);
        }
        let grant = self.effective_grant(caller, sdk);
        if !grant.allowed {
            return Err(SdkDenial::NotGranted);
        }
        let reqs: Vec<semver::VersionReq> = grant
            .version_reqs
            .iter()
            .filter_map(|r| parse_version_req(r).ok())
            .collect();
        let fits = |r: &SdkTarget| parse_version(&r.version).is_ok_and(|v| reqs.iter().all(|q| q.matches(&v)));
        // The grant's channel, else `stable`; when its release does not satisfy the requirements,
        // the newest release that does (an old major keeps running for grants pinned to it).
        let by_channel = self
            .channel_release(sdk, &grant.channel)
            .or_else(|| self.channel_release(sdk, DEFAULT_CHANNEL));
        if let Some(r) = by_channel.filter(|r| fits(r)) {
            return Ok(r);
        }
        self.releases
            .iter()
            .filter(|r| r.sdk == sdk && fits(r))
            .filter_map(|r| parse_version(&r.version).ok().map(|v| (v, r)))
            .max_by(|a, b| a.0.cmp(&b.0))
            .map(|(_, r)| r)
            .ok_or(SdkDenial::VersionUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(sdk: &str, version: &str, n: u32) -> SdkTarget {
        SdkTarget {
            sdk: sdk.into(),
            version: version.into(),
            service: ServiceId::from_u128(100),
            version_no: VersionNo(n),
            digest: Digest::of(version.as_bytes()),
            runtime: RuntimeKind::Deno,
            members: vec!["ping".into()],
        }
    }

    fn grant(scope: GrantScope, sdk: &str, effect: Option<GrantEffect>) -> SdkGrant {
        SdkGrant {
            scope,
            sdk: sdk.into(),
            effect,
            version_req: None,
            channel: None,
            created_by: "root".into(),
            updated_at_ms: 0,
        }
    }

    fn snapshot() -> SdkSnapshot {
        SdkSnapshot {
            sdks: vec![Sdk {
                name: "acme".into(),
                service_id: ServiceId::from_u128(100),
                description: None,
                state: SdkState::Active,
                created_at_ms: 0,
                removed_at_ms: None,
            }],
            releases: vec![
                release("acme", "1.0.0", 1),
                release("acme", "1.1.0", 2),
                release("acme", "2.0.0", 3),
            ],
            channels: vec![
                SdkChannel {
                    sdk: "acme".into(),
                    channel: "stable".into(),
                    version: "1.0.0".into(),
                    previous: None,
                    updated_at_ms: 0,
                },
                SdkChannel {
                    sdk: "acme".into(),
                    channel: "canary".into(),
                    version: "1.1.0".into(),
                    previous: None,
                    updated_at_ms: 0,
                },
            ],
            grants: vec![grant(GrantScope::Node, "acme", Some(GrantEffect::Allow))],
        }
    }

    const P: u128 = 7;

    fn caller(env: &str) -> SdkCaller<'_> {
        SdkCaller {
            tenant: TenantId::from_u128(1),
            plan: "pro",
            project: ProjectId::from_u128(P),
            environment: env,
        }
    }

    #[test]
    fn the_system_tenant_and_names() {
        assert_eq!(system_tenant().to_string(), "ten_00000000000000000000000SYS");
        assert!(valid_sdk_name("acme") && valid_sdk_name("acme_pay2"));
        assert!(
            !valid_sdk_name("a") && !valid_sdk_name("Acme") && !valid_sdk_name("acme-pay") && !valid_sdk_name("1x")
        );
        assert_eq!(sdk_service_slug("acme_pay"), "sys-sdk-acme-pay");
        assert!(parse_version("1.2.3").is_ok() && parse_version("1.2").is_err() && parse_version("1.0.0+b1").is_err());
        assert!(parse_version_req("1.2.3")
            .unwrap()
            .matches(&parse_version("1.2.3").unwrap()));
        assert!(!parse_version_req("1.2.3")
            .unwrap()
            .matches(&parse_version("1.2.4").unwrap()));
        assert!(parse_version_req("^1")
            .unwrap()
            .matches(&parse_version("1.9.0").unwrap()));
    }

    #[test]
    fn scopes_round_trip_as_text() {
        let p = ProjectId::from_u128(P);
        for s in [
            GrantScope::Node,
            GrantScope::Plan("pro".into()),
            GrantScope::Tenant(TenantId::from_u128(1)),
            GrantScope::Project(p),
            GrantScope::Environment(p, "staging".into()),
        ] {
            assert_eq!(s.to_string().parse::<GrantScope>().unwrap(), s);
        }
        assert!("environment:prj_x/staging".parse::<GrantScope>().is_err());
        assert!("galaxy".parse::<GrantScope>().is_err());
    }

    /// SDK-01: a node allow and `stable` -> the stable release.
    #[test]
    fn sdk_01_a_node_grant_binds_the_stable_release() {
        let s = snapshot();
        assert_eq!(s.resolve(&caller("production"), "acme").unwrap().version, "1.0.0");
        assert_eq!(
            s.resolve(&caller("production"), "nope").unwrap_err(),
            SdkDenial::NotFound
        );
    }

    /// SDK-04: an environment grant with `channel = canary` -> the canary release there only.
    #[test]
    fn sdk_04_an_environment_follows_its_channel() {
        let mut s = snapshot();
        let mut g = grant(
            GrantScope::Environment(ProjectId::from_u128(P), "staging".into()),
            "acme",
            None,
        );
        g.channel = Some("canary".into());
        s.grants.push(g);
        assert_eq!(s.resolve(&caller("staging"), "acme").unwrap().version, "1.1.0");
        assert_eq!(s.resolve(&caller("production"), "acme").unwrap().version, "1.0.0");
        // A channel that does not exist falls back to `stable`.
        s.grants.last_mut().unwrap().channel = Some("beta".into());
        assert_eq!(s.resolve(&caller("staging"), "acme").unwrap().version, "1.0.0");
    }

    /// SDK-05: a lower level can only narrow: a deny above wins over an allow below, and a deny
    /// below removes an allow above.
    #[test]
    fn sdk_05_a_lower_level_only_narrows() {
        let mut s = snapshot();
        s.grants
            .push(grant(GrantScope::Plan("pro".into()), "acme", Some(GrantEffect::Deny)));
        s.grants.push(grant(
            GrantScope::Tenant(TenantId::from_u128(1)),
            "acme",
            Some(GrantEffect::Allow),
        ));
        assert_eq!(
            s.resolve(&caller("production"), "acme").unwrap_err(),
            SdkDenial::NotGranted
        );
        let g = s.effective_grant(&caller("production"), "acme");
        assert_eq!(g.denied_by.as_deref(), Some("plan:pro"));

        let mut s = snapshot();
        s.grants.push(grant(
            GrantScope::Project(ProjectId::from_u128(P)),
            "*",
            Some(GrantEffect::Deny),
        ));
        assert_eq!(
            s.resolve(&caller("production"), "acme").unwrap_err(),
            SdkDenial::NotGranted
        );

        // Nothing granted anywhere: not granted.
        let mut s = snapshot();
        s.grants.clear();
        assert_eq!(
            s.resolve(&caller("production"), "acme").unwrap_err(),
            SdkDenial::NotGranted
        );
        // `*` at the node grants every SDK.
        s.grants.push(grant(GrantScope::Node, "*", Some(GrantEffect::Allow)));
        assert!(s.resolve(&caller("production"), "acme").is_ok());
    }

    /// Major versions: every requirement of the chain must hold; a channel release that does not
    /// satisfy them falls back to the newest release that does.
    #[test]
    fn version_requirements_pin_the_major() {
        let mut s = snapshot();
        s.channels[0].version = "2.0.0".into();
        assert_eq!(
            s.resolve(&caller("production"), "acme").unwrap().version,
            "2.0.0",
            "no requirement: stable"
        );
        s.grants[0].version_req = Some("^1".into());
        assert_eq!(
            s.resolve(&caller("production"), "acme").unwrap().version,
            "1.1.0",
            "pinned to ^1: newest 1.x"
        );
        let mut tenant = grant(GrantScope::Tenant(TenantId::from_u128(1)), "acme", None);
        tenant.version_req = Some("~1.0".into());
        s.grants.push(tenant);
        assert_eq!(
            s.resolve(&caller("production"), "acme").unwrap().version,
            "1.0.0",
            "both requirements hold"
        );
        s.grants.last_mut().unwrap().version_req = Some("^3".into());
        assert_eq!(
            s.resolve(&caller("production"), "acme").unwrap_err(),
            SdkDenial::VersionUnavailable
        );
    }

    /// SDK-08: a removed SDK answers `sdk_unavailable`.
    #[test]
    fn sdk_08_a_removed_sdk_is_unavailable() {
        let mut s = snapshot();
        s.sdks[0].state = SdkState::Removed;
        let e = s.resolve(&caller("production"), "acme").unwrap_err();
        assert_eq!((e.code(), e.http_status()), ("sdk_unavailable", 410));
    }

    #[test]
    fn system_tenant_test_and_channel_grammar() {
        assert!(is_system_tenant(system_tenant()));
        assert!(!is_system_tenant(TenantId::from_u128(1)));
        assert!(valid_channel("stable") && valid_channel("a-1") && valid_channel(&"c".repeat(32)));
        assert!(!valid_channel("") && !valid_channel("1a") && !valid_channel("A") && !valid_channel("a_b"));
        assert!(!valid_channel(&"c".repeat(33)));
    }

    #[test]
    fn state_and_effect_names_match_their_wire_form() {
        assert_eq!(
            (SdkState::Active.as_str(), SdkState::Removed.as_str()),
            ("active", "removed")
        );
        assert_eq!(
            (GrantEffect::Allow.as_str(), GrantEffect::Deny.as_str()),
            ("allow", "deny")
        );
        assert_eq!("allow".parse::<GrantEffect>(), Ok(GrantEffect::Allow));
        assert_eq!("deny".parse::<GrantEffect>(), Ok(GrantEffect::Deny));
        assert!("maybe".parse::<GrantEffect>().unwrap_err().contains("maybe"));
    }

    #[test]
    fn grant_scope_levels_and_serde_text_form() {
        let p = ProjectId::from_u128(P);
        let scopes = [
            GrantScope::Node,
            GrantScope::Plan("pro".into()),
            GrantScope::Tenant(TenantId::from_u128(1)),
            GrantScope::Project(p),
            GrantScope::Environment(p, "staging".into()),
        ];
        assert_eq!(
            scopes.iter().map(GrantScope::level).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        let env = GrantScope::Environment(p, "staging".into());
        let json = serde_json::to_string(&env).unwrap();
        assert_eq!(json, format!("\"environment:{p}/staging\""));
        assert_eq!(serde_json::from_str::<GrantScope>(&json).unwrap(), env);
        assert!(serde_json::from_str::<GrantScope>("\"galaxy:x\"").is_err());
        assert!(format!("environment:{p}/Bad Env").parse::<GrantScope>().is_err());
    }

    #[test]
    fn denials_carry_their_code_status_and_message() {
        let cases = [
            (SdkDenial::NotFound, "sdk_not_found", 404),
            (SdkDenial::Removed, "sdk_unavailable", 410),
            (SdkDenial::NotGranted, "sdk_not_granted", 403),
            (SdkDenial::VersionUnavailable, "sdk_version_unavailable", 503),
        ];
        for (d, code, status) in cases {
            assert_eq!((d.code(), d.http_status()), (code, status));
            assert!(d.message("acme").contains("acme"));
        }
        assert_eq!(SdkDenial::Removed.message("acme"), "the SDK `acme` was removed");
    }
}
