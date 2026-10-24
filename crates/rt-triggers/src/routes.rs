//! Route table. An immutable index swapped atomically on deploy / `live` flips
//! through `ArcSwap`, so a flip under load never produces a missing route (TRG-03).
//!
//! Routing, first match wins:
//! 1. `Host` equals a **verified** custom host mapped to a service — path passed through;
//! 2. `Host` = `{slug}.{routing.base_domain}` when set — path passed through;
//! 3. path `/s/{slug}/{rest}` — the handler sees `/{rest}`; the original prefix is reported in
//!    `x-rt-base-path`.
//!
//! `access = internal` services do not resolve on the public listener (`404`, SEC-08).

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use rt_types::{
    Access, Listener, ProjectId, RouteEntry, RouteSnapshot, SdkCaller, SdkDenial, SdkSnapshot, SdkTarget, ServiceId,
    TenantId,
};

/// How a request was matched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteMatch {
    CustomHost,
    BaseDomain,
    PathPrefix,
}

/// A resolved route.
#[derive(Clone, Debug)]
pub struct Route {
    pub entry: Arc<RouteEntry>,
    pub matched: RouteMatch,
    /// Path as the handler sees it (`/x` for `/s/a/x`; unchanged for host routes).
    pub path: String,
    /// Value of `x-rt-base-path` (`/s/a` for path routes, empty otherwise).
    pub base_path: String,
}

pub trait RouteResolver: Send + Sync {
    /// `host` may include a port; `path` excludes the query string.
    fn resolve(&self, listener: Listener, host: &str, path: &str) -> Option<Route>;
}

#[derive(Default)]
struct Index {
    generation: u64,
    by_host: HashMap<String, Arc<RouteEntry>>,
    by_slug: HashMap<String, Arc<RouteEntry>>,
    by_service: HashMap<ServiceId, Arc<RouteEntry>>,
    /// Operator SDKs, their releases, channels and grants.
    sdk: Arc<SdkSnapshot>,
    /// Grant resolutions of this generation, per caller scope and SDK (a new snapshot starts
    /// empty, so a grant or channel change applies to the next call).
    sdk_bound: DashMap<SdkKey, Result<SdkTarget, SdkDenial>>,
}

/// `(tenant, plan, project, environment, sdk)`.
type SdkKey = (TenantId, String, ProjectId, String, String);

/// Resolutions kept per generation at most; past it the cache starts over.
const SDK_CACHE_MAX: usize = 10_000;

impl Index {
    fn build(snap: &RouteSnapshot) -> Self {
        let mut idx = Index {
            generation: snap.generation,
            sdk: Arc::new(snap.sdk.clone()),
            ..Default::default()
        };
        for e in &snap.entries {
            let e = Arc::new(e.clone());
            idx.by_slug.insert(e.slug.to_ascii_lowercase(), e.clone());
            idx.by_service.insert(e.service, e.clone());
            for h in &e.hosts {
                idx.by_host.insert(h.to_ascii_lowercase(), e.clone());
            }
        }
        idx
    }
}

/// Lowercase `host`, strip a `:port` suffix and a trailing dot (IPv6 literals in brackets are left
/// alone).
pub fn normalize_host(host: &str) -> String {
    let h = host.trim().to_ascii_lowercase();
    if h.starts_with('[') {
        return h;
    }
    let name = match h.rsplit_once(':') {
        Some((name, port)) if port.chars().all(|c| c.is_ascii_digit()) => name.to_string(),
        _ => h,
    };
    // The fully-qualified form `api.example.com.` names the same host (ROUTE-003).
    match name.trim_end_matches('.') {
        "" => name,
        n => n.to_string(),
    }
}

/// The atomically swapped route index.
pub struct RouteTable {
    base_domain: Option<String>,
    index: ArcSwap<Index>,
}

impl RouteTable {
    /// `base_domain` = `routing.base_domain` (empty / `None` disables rule 2).
    pub fn new(base_domain: Option<String>) -> Arc<Self> {
        let base_domain = base_domain
            .filter(|d| !d.is_empty())
            .map(|d| d.trim_matches('.').to_ascii_lowercase());
        Arc::new(Self {
            base_domain,
            index: ArcSwap::from_pointee(Index::default()),
        })
    }

    /// Replace the whole table (readers keep using the old one until they next load).
    pub fn swap(&self, snapshot: &RouteSnapshot) {
        self.index.store(Arc::new(Index::build(snapshot)));
    }

    /// Generation of the table currently served.
    pub fn generation(&self) -> u64 {
        self.index.load().generation
    }

    pub fn len(&self) -> usize {
        self.index.load().by_slug.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A service by slug (service-to-service calls, queue enqueue).
    pub fn by_slug(&self, slug: &str) -> Option<Arc<RouteEntry>> {
        self.index.load().by_slug.get(&slug.to_ascii_lowercase()).cloned()
    }

    /// A service by id (the caller of a service-to-service call, D-A1).
    pub fn by_service(&self, service: ServiceId) -> Option<Arc<RouteEntry>> {
        self.index.load().by_service.get(&service).cloned()
    }

    /// The operator SDKs of the table currently served.
    pub fn sdk(&self) -> Arc<SdkSnapshot> {
        self.index.load().sdk.clone()
    }

    /// The release a call of `caller` to the SDK `name` runs (grant resolution, cached for the
    /// generation).
    pub fn sdk_resolve(&self, caller: &SdkCaller<'_>, name: &str) -> Result<SdkTarget, SdkDenial> {
        let idx = self.index.load();
        let key = (
            caller.tenant,
            caller.plan.to_string(),
            caller.project,
            caller.environment.to_string(),
            name.to_string(),
        );
        if let Some(hit) = idx.sdk_bound.get(&key) {
            return hit.clone();
        }
        let bound = idx.sdk.resolve(caller, name).cloned();
        if idx.sdk_bound.len() >= SDK_CACHE_MAX {
            idx.sdk_bound.clear();
        }
        idx.sdk_bound.insert(key, bound.clone());
        bound
    }
}

impl RouteResolver for RouteTable {
    fn resolve(&self, listener: Listener, host: &str, path: &str) -> Option<Route> {
        let idx = self.index.load();
        let host = normalize_host(host);

        let found = if let Some(e) = idx.by_host.get(&host) {
            Some(Route {
                entry: e.clone(),
                matched: RouteMatch::CustomHost,
                path: path.to_string(),
                base_path: String::new(),
            })
        } else if let Some(slug) = self
            .base_domain
            .as_ref()
            .and_then(|b| host.strip_suffix(b.as_str()))
            .and_then(|p| p.strip_suffix('.'))
            .filter(|s| !s.is_empty() && !s.contains('.'))
        {
            idx.by_slug.get(slug).map(|e| Route {
                entry: e.clone(),
                matched: RouteMatch::BaseDomain,
                path: path.to_string(),
                base_path: String::new(),
            })
        } else if let Some(rest) = path.strip_prefix("/s/") {
            let (slug, tail) = match rest.find('/') {
                Some(i) => (&rest[..i], &rest[i..]),
                None => (rest, "/"),
            };
            idx.by_slug.get(&slug.to_ascii_lowercase()).map(|e| Route {
                entry: e.clone(),
                matched: RouteMatch::PathPrefix,
                path: tail.to_string(),
                base_path: format!("/s/{slug}"),
            })
        } else {
            None
        };

        found.filter(|r| !(listener == Listener::Public && r.entry.access == Access::Internal))
    }
}
