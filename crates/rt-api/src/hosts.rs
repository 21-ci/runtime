//! Custom-host claim maintenance (SECURITY.md, SEC-32, SEC-43, SEC-44).
//!
//! A tenant's `PUT /v1/services/{s}/hosts/{host}` creates a `pending_verification` claim with a
//! `verify_token`. Every `hosts.verify_interval` the node:
//! 1. deletes pending claims older than `hosts.pending_ttl` (they stop blocking other services);
//! 2. with `hosts.verify_dns = true`, looks up the TXT records of `_rt-verify.<host>` for every
//!    remaining claim; a record equal to the claim's token flips it to `active`, and the route
//!    table is refreshed. With `verify_dns = false` no lookup is made (operator approval only).
//!
//! DNS goes through [`TxtResolver`] so tests use a fake; the node uses [`HickoryTxtResolver`].

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use rt_config::HostsConfig;
use rt_sec::{Actor, AuditEvent};
use rt_store::{HostRelease, MetaStore, StoreError, TenantScope};
use rt_types::{HostMapping, HostStatus, TenantId, UnixMs};
use serde_json::json;

use crate::control::refresh_routes;
use crate::state::ApiState;

/// The label in front of the host whose TXT record carries the verify token.
pub const VERIFY_LABEL: &str = "_rt-verify";

/// TXT lookups running at the same time during one sweep.
const LOOKUP_CONCURRENCY: usize = 8;

/// DNS TXT lookups.
#[async_trait]
pub trait TxtResolver: Send + Sync {
    /// Every TXT record of `name`, each as one string (its character-strings concatenated). No
    /// record (or no such name) is `Ok(vec![])`; `Err` is a transport or server failure.
    async fn txt(&self, name: &str) -> Result<Vec<String>, String>;
}

/// [`TxtResolver`] over `hickory-resolver` with the system's resolver configuration
/// (`/etc/resolv.conf`), or the library defaults when that cannot be read.
pub struct HickoryTxtResolver {
    resolver: Option<hickory_resolver::TokioResolver>,
}

impl HickoryTxtResolver {
    pub fn new() -> Self {
        let built = match hickory_resolver::TokioResolver::builder_tokio() {
            Ok(b) => b.build(),
            Err(e) => {
                tracing::warn!(error = %e, "system DNS configuration unreadable; host verification uses the resolver defaults");
                hickory_resolver::Resolver::builder_with_config(
                    hickory_resolver::config::ResolverConfig::default(),
                    hickory_resolver::net::runtime::TokioRuntimeProvider::default(),
                )
                .build()
            }
        };
        let resolver = built
            .map_err(|e| tracing::warn!(error = %e, "no DNS resolver: host verification lookups fail until restart"))
            .ok();
        Self { resolver }
    }
}

impl Default for HickoryTxtResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TxtResolver for HickoryTxtResolver {
    async fn txt(&self, name: &str) -> Result<Vec<String>, String> {
        let Some(resolver) = &self.resolver else {
            return Err("no DNS resolver".to_string());
        };
        match resolver.txt_lookup(name).await {
            Ok(found) => Ok(found
                .answers()
                .iter()
                .filter_map(|r| match &r.data {
                    hickory_resolver::proto::rr::RData::TXT(t) => Some(
                        t.txt_data
                            .iter()
                            .map(|part| String::from_utf8_lossy(part))
                            .collect::<String>(),
                    ),
                    _ => None,
                })
                .collect()),
            Err(e) if e.is_no_records_found() => Ok(Vec::new()),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// `_rt-verify.<host>.` (absolute: no search domains are tried).
pub fn verify_name(host: &str) -> String {
    format!("{VERIFY_LABEL}.{}.", host.trim_end_matches('.'))
}

fn ttl_ms(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX)
}

/// When a pending claim expires.
pub fn expires_at_ms(h: &HostMapping, pending_ttl: Duration) -> UnixMs {
    h.created_at_ms.saturating_add(ttl_ms(pending_ttl))
}

/// A pending claim older than `pending_ttl` at `now_ms` (active mappings never expire).
pub fn expired(h: &HostMapping, pending_ttl: Duration, now_ms: UnixMs) -> bool {
    h.status == HostStatus::PendingVerification && h.created_at_ms < now_ms.saturating_sub(ttl_ms(pending_ttl))
}

/// `start + ttl`.
pub fn expires_at(start_ms: UnixMs, ttl: Duration) -> UnixMs {
    start_ms.saturating_add(ttl_ms(ttl))
}

/// A released claim still counts for its tenant for one TTL after it ended: a new claim of the
/// host by that tenant continues it (or is refused once the old claim's own TTL is over).
pub fn still_cooling(r: &HostRelease, pending_ttl: Duration, now_ms: UnixMs) -> bool {
    expires_at(r.released_at_ms, pending_ttl) > now_ms
}

/// Remember that `tenant`'s pending claim `h` ended at `now_ms`.
pub async fn remember_release(
    meta: &dyn MetaStore,
    h: &HostMapping,
    tenant: TenantId,
    now_ms: UnixMs,
) -> Result<(), StoreError> {
    meta.record_host_release(HostRelease {
        host: h.host.clone(),
        tenant,
        created_at_ms: h.created_at_ms,
        verify_token: h.verify_token.clone(),
        released_at_ms: now_ms,
    })
    .await
}

/// Delete the pending claims older than `pending_ttl` (conditional in SQL: only
/// `pending_verification` rows created before the cutoff, never a mapping approved meanwhile),
/// remember each as a release of its tenant, and forget releases that no longer matter.
pub async fn expire_claims(
    meta: &dyn MetaStore,
    pending_ttl: Duration,
    now_ms: UnixMs,
) -> Result<Vec<HostMapping>, StoreError> {
    let cutoff = now_ms.saturating_sub(ttl_ms(pending_ttl));
    let expired = meta.expire_pending_hosts(cutoff).await?;
    for h in &expired {
        if let Some(svc) = meta.get_service(TenantScope::Operator, h.service_id).await? {
            remember_release(meta, h, svc.tenant_id, now_ms).await?;
        }
    }
    meta.prune_host_releases(cutoff).await?;
    Ok(expired)
}

/// What one sweep changed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostSweep {
    /// Pending claims deleted because they outlived `hosts.pending_ttl`.
    pub expired: Vec<HostMapping>,
    /// Claims whose TXT record matched; now active.
    pub activated: Vec<HostMapping>,
    /// Lookups that failed (server or transport errors); those claims are retried next time.
    pub lookup_errors: usize,
}

/// Does `_rt-verify.<host>` publish the claim's token?
async fn token_published(resolver: &dyn TxtResolver, h: &HostMapping) -> Result<bool, String> {
    let Some(token) = h.verify_token.as_deref().filter(|t| !t.is_empty()) else {
        return Ok(false);
    };
    let records = resolver.txt(&verify_name(&h.host)).await?;
    Ok(records.iter().any(|r| r.trim() == token))
}

/// One pass over the pending claims at `now_ms`: expire stale ones, then (with
/// `hosts.verify_dns`) activate the ones whose TXT record matches. Reads `meta.db` once when
/// nothing is pending.
pub async fn sweep_host_claims(
    meta: &dyn MetaStore,
    hosts: &HostsConfig,
    resolver: &dyn TxtResolver,
    now_ms: UnixMs,
) -> Result<HostSweep, StoreError> {
    let mut out = HostSweep::default();
    let pending = meta.pending_hosts().await?;
    if pending.is_empty() {
        return Ok(out);
    }
    if pending.iter().any(|h| expired(h, hosts.pending_ttl, now_ms)) {
        out.expired = expire_claims(meta, hosts.pending_ttl, now_ms).await?;
    }
    if !hosts.verify_dns {
        return Ok(out);
    }
    let live: Vec<HostMapping> = pending
        .into_iter()
        .filter(|h| !expired(h, hosts.pending_ttl, now_ms))
        .collect();
    let checked: Vec<(HostMapping, Result<bool, String>)> = stream::iter(live)
        .map(|h| async move {
            let r = token_published(resolver, &h).await;
            (h, r)
        })
        .buffer_unordered(LOOKUP_CONCURRENCY)
        .collect()
        .await;
    for (h, r) in checked {
        match r {
            Ok(true) => {
                let token = h.verify_token.clone().unwrap_or_default();
                if meta.activate_host(&h.host, h.service_id, &token).await? {
                    out.activated.push(h);
                }
            }
            Ok(false) => {}
            Err(e) => {
                tracing::debug!(host = %h.host, error = %e, "custom host TXT lookup failed");
                out.lookup_errors += 1;
            }
        }
    }
    Ok(out)
}

/// The node's claim maintenance loop: a sweep right away, then every `hosts.verify_interval`
/// (re-read each time, so `SIGHUP` applies). Activations refresh the route table; expiries and
/// activations are audited (`host.expired`, `host.verified`).
pub async fn run_host_claims(state: ApiState, resolver: Arc<dyn TxtResolver>) {
    loop {
        let cfg = state.config.load_full();
        match sweep_host_claims(
            state.meta.as_ref(),
            &cfg.hosts,
            resolver.as_ref(),
            state.clock.unix_ms(),
        )
        .await
        {
            Ok(sweep) => {
                for (h, action) in sweep
                    .expired
                    .iter()
                    .map(|h| (h, "host.expired"))
                    .chain(sweep.activated.iter().map(|h| (h, "host.verified")))
                {
                    let tenant = state.service_table.load().get(&h.service_id).map(|s| s.tenant_id);
                    state.audit.emit(AuditEvent {
                        ts_ms: state.clock.unix_ms(),
                        actor: Actor::System,
                        tenant,
                        service: Some(h.service_id),
                        action: action.to_string(),
                        data: json!({"host": h.host}),
                    });
                }
                if !sweep.activated.is_empty() {
                    if let Err(e) = refresh_routes(&state).await {
                        tracing::warn!(error = %e.message, "route refresh after host verification failed");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "custom host maintenance failed"),
        }
        tokio::time::sleep(cfg.hosts.verify_interval.max(Duration::from_secs(1))).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;
    use rt_store::SqliteMetaStore;
    use rt_types::{NewService, NewTenant, RuntimeKind, ServiceId};

    const DAY_MS: i64 = 86_400_000;

    /// TXT records by name; counts lookups. A name mapped to `Err` fails.
    #[derive(Default)]
    struct FakeDns {
        records: Mutex<HashMap<String, Result<Vec<String>, String>>>,
        lookups: AtomicUsize,
    }

    impl FakeDns {
        fn publish(&self, host: &str, values: &[&str]) {
            self.records
                .lock()
                .insert(verify_name(host), Ok(values.iter().map(|v| v.to_string()).collect()));
        }
    }

    #[async_trait]
    impl TxtResolver for FakeDns {
        async fn txt(&self, name: &str) -> Result<Vec<String>, String> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            self.records.lock().get(name).cloned().unwrap_or_else(|| Ok(Vec::new()))
        }
    }

    fn cfg(verify_dns: bool) -> HostsConfig {
        HostsConfig {
            verify_dns,
            ..HostsConfig::default()
        }
    }

    async fn setup() -> (Arc<SqliteMetaStore>, ServiceId, ServiceId) {
        let m = SqliteMetaStore::open_in_memory().expect("meta");
        let t = m
            .create_tenant(NewTenant {
                name: "t".into(),
                plan: "free".into(),
                overrides: None,
            })
            .await
            .expect("tenant");
        let mut ids = Vec::new();
        for slug in ["shop", "blog"] {
            let ns = NewService {
                slug: slug.into(),
                runtime: RuntimeKind::Wasm,
                name: None,
                profile: None,
                environment: rt_types::default_environment(),
                project: rt_types::default_project(),
                config: Default::default(),
            };
            ids.push(
                m.create_service(TenantScope::Tenant(t.id), t.id, ns)
                    .await
                    .expect("service")
                    .id,
            );
        }
        (m, ids[0], ids[1])
    }

    fn claim(host: &str, svc: ServiceId, token: &str, created_at_ms: i64) -> HostMapping {
        HostMapping {
            host: host.into(),
            service_id: svc,
            status: HostStatus::PendingVerification,
            verify_token: Some(token.into()),
            created_at_ms,
        }
    }

    #[test]
    fn the_verify_name_is_absolute_and_expiry_follows_the_ttl() {
        assert_eq!(verify_name("shop.example.com"), "_rt-verify.shop.example.com.");
        assert_eq!(verify_name("shop.example.com."), "_rt-verify.shop.example.com.");
        let ttl = Duration::from_secs(7 * 86_400);
        let h = claim("a.example.com", ServiceId::from_u128(1), "t", 1_000);
        assert_eq!(expires_at_ms(&h, ttl), 1_000 + 7 * DAY_MS);
        assert!(!expired(&h, ttl, 1_000 + 7 * DAY_MS));
        assert!(expired(&h, ttl, 1_001 + 7 * DAY_MS));
        let active = HostMapping {
            status: HostStatus::Active,
            ..h
        };
        assert!(!expired(&active, ttl, i64::MAX), "active mappings never expire");
    }

    #[tokio::test]
    async fn ac_sec_44_a_matching_txt_record_activates_the_claim() {
        let (m, shop, blog) = setup().await;
        let now = 10 * DAY_MS;
        m.put_host(claim("shop.example.com", shop, "tok-shop", now - DAY_MS))
            .await
            .expect("claim");
        m.put_host(claim("blog.example.com", blog, "tok-blog", now - DAY_MS))
            .await
            .expect("claim");
        let dns = FakeDns::default();
        dns.publish("shop.example.com", &["unrelated", " tok-shop "]);
        dns.publish("blog.example.com", &["wrong-token"]);

        let sweep = sweep_host_claims(m.as_ref(), &cfg(true), &dns, now)
            .await
            .expect("sweep");
        assert_eq!(
            sweep.activated.iter().map(|h| h.host.as_str()).collect::<Vec<_>>(),
            vec!["shop.example.com"]
        );
        assert!(sweep.expired.is_empty());
        assert_eq!(dns.lookups.load(Ordering::SeqCst), 2);
        let shop_host = m.find_host("shop.example.com").await.expect("find").expect("exists");
        assert_eq!((shop_host.status, shop_host.verify_token), (HostStatus::Active, None));
        assert_eq!(
            m.find_host("blog.example.com")
                .await
                .expect("find")
                .expect("exists")
                .status,
            HostStatus::PendingVerification,
            "a wrong token keeps the claim pending"
        );

        // A failing lookup is retried later and changes nothing.
        dns.records
            .lock()
            .insert(verify_name("blog.example.com"), Err("SERVFAIL".into()));
        let sweep = sweep_host_claims(m.as_ref(), &cfg(true), &dns, now)
            .await
            .expect("sweep");
        assert_eq!((sweep.activated.len(), sweep.lookup_errors), (0, 1));
    }

    #[tokio::test]
    async fn ac_sec_44_no_lookups_when_dns_verification_is_off() {
        let (m, shop, _) = setup().await;
        m.put_host(claim("shop.example.com", shop, "tok", DAY_MS))
            .await
            .expect("claim");
        let dns = FakeDns::default();
        dns.publish("shop.example.com", &["tok"]);
        let sweep = sweep_host_claims(m.as_ref(), &cfg(false), &dns, 2 * DAY_MS)
            .await
            .expect("sweep");
        assert_eq!(sweep, HostSweep::default());
        assert_eq!(
            dns.lookups.load(Ordering::SeqCst),
            0,
            "verify_dns = false makes no DNS query"
        );
        assert_eq!(
            m.find_host("shop.example.com")
                .await
                .expect("find")
                .expect("exists")
                .status,
            HostStatus::PendingVerification
        );
    }

    #[tokio::test]
    async fn ac_sec_43_stale_pending_claims_expire_and_are_not_looked_up() {
        let (m, shop, blog) = setup().await;
        let now = 30 * DAY_MS;
        m.put_host(claim("old.example.com", shop, "tok", now - 8 * DAY_MS))
            .await
            .expect("stale claim");
        m.put_host(claim("new.example.com", shop, "tok2", now - DAY_MS))
            .await
            .expect("fresh claim");
        let dns = FakeDns::default();
        dns.publish("old.example.com", &["tok"]);

        let sweep = sweep_host_claims(m.as_ref(), &cfg(true), &dns, now)
            .await
            .expect("sweep");
        assert_eq!(
            sweep.expired.iter().map(|h| h.host.as_str()).collect::<Vec<_>>(),
            vec!["old.example.com"]
        );
        assert!(sweep.activated.is_empty(), "an expired claim is never activated");
        assert_eq!(
            dns.lookups.load(Ordering::SeqCst),
            1,
            "only the fresh claim is looked up"
        );
        assert_eq!(m.find_host("old.example.com").await.expect("find"), None);
        m.put_host(claim("old.example.com", blog, "tok3", 0))
            .await
            .expect("the freed host can be claimed by another service");

        // Nothing pending: one read, no lookups.
        let empty = SqliteMetaStore::open_in_memory().expect("meta");
        let sweep = sweep_host_claims(empty.as_ref(), &cfg(true), &dns, now)
            .await
            .expect("sweep");
        assert_eq!(sweep, HostSweep::default());
    }

    #[tokio::test]
    async fn expiry_never_deletes_an_approved_mapping_and_remembers_the_tenants_release() {
        let (m, shop, blog) = setup().await;
        let now = 30 * DAY_MS;
        let ttl = Duration::from_secs(7 * 86_400);
        // Pending and stale…
        m.put_host(claim("old.example.com", shop, "tok", now - 8 * DAY_MS))
            .await
            .expect("stale claim");
        // …but approved by the operator just before the cleanup runs.
        let approved = HostMapping {
            status: HostStatus::Active,
            verify_token: None,
            ..claim("old.example.com", shop, "", now - 8 * DAY_MS)
        };
        m.put_host(approved).await.expect("approve");
        m.put_host(claim("stale.example.com", blog, "tok-b", now - 8 * DAY_MS))
            .await
            .expect("stale claim");

        let gone = expire_claims(m.as_ref(), ttl, now).await.expect("expire");
        assert_eq!(
            gone.iter().map(|h| h.host.as_str()).collect::<Vec<_>>(),
            vec!["stale.example.com"]
        );
        assert_eq!(
            m.find_host("old.example.com")
                .await
                .expect("find")
                .expect("kept")
                .status,
            HostStatus::Active
        );

        let tenant = m
            .get_service(TenantScope::Operator, blog)
            .await
            .expect("get")
            .expect("svc")
            .tenant_id;
        let r = m
            .host_release("stale.example.com", tenant)
            .await
            .expect("release")
            .expect("remembered");
        assert_eq!(
            (r.created_at_ms, r.verify_token.as_deref(), r.released_at_ms),
            (now - 8 * DAY_MS, Some("tok-b"), now)
        );
        assert!(still_cooling(&r, ttl, now + DAY_MS) && !still_cooling(&r, ttl, now + 7 * DAY_MS));
    }
}
