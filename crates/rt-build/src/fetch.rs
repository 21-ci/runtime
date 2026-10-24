//! Module fetching for builds (stage 3): egress-guarded, deduplicated in a shared
//! in-memory deps cache keyed by URL → sha256 (LRU, bounded by bytes), checked against `rt.lock`.
//! Durability comes from the vendor store (the artifact store), not from this cache.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use rt_host::{EgressBudget, EgressError, EgressGuard, EgressPolicy};
use rt_store::ArtifactStore;
use rt_types::{Body, Digest, Limits};
use thiserror::Error;
use url::Url;

#[derive(Clone, Debug)]
pub struct Fetched {
    pub url: Url,
    pub bytes: Bytes,
    pub digest: Digest,
    pub content_type: Option<String>,
    /// Served from the shared deps cache (no network, BLD-05).
    pub from_cache: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FetchError {
    /// Egress guard or `build.allowed_hosts` refused it.
    #[error("fetch of {0} denied")]
    Denied(String),
    #[error("{0} not found")]
    NotFound(String),
    #[error("fetch of {0} timed out")]
    Timeout(String),
    /// Single module or total per-version dependency size cap (`build.max_deps_mb`).
    #[error("dependency too large")]
    TooLarge,
    /// Bytes differ from the locked/expected digest (`lock_mismatch`).
    #[error("digest mismatch for {url}")]
    DigestMismatch {
        url: String,
        expected: Digest,
        actual: Digest,
    },
    #[error("upstream error fetching {0}")]
    Upstream(String),
}

#[async_trait]
pub trait ModuleFetcher: Send + Sync {
    /// Fetch `url`. With `expected = Some(d)` the content must hash to `d` (taken from `rt.lock`).
    async fn fetch(&self, url: &Url, expected: Option<Digest>) -> Result<Fetched, FetchError>;

    /// Cap on the bytes of all remote modules of one build, cached or not (`build.max_deps_mb`);
    /// the build fails `too_large` past it. `None`: no total cap.
    fn max_total_bytes(&self) -> Option<u64> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct FetcherConfig {
    /// `build.allowed_hosts`.
    pub allowed_hosts: Vec<String>,
    /// `build.fetch_timeout` (30 s per module).
    pub timeout: Duration,
    /// Per-module cap in bytes.
    pub max_module_bytes: u64,
    /// Total of all remote modules of one build/version (`build.max_deps_mb`), enforced by the
    /// build ([`ModuleFetcher::max_total_bytes`]); one module is also capped by it.
    pub max_total_bytes: u64,
}

/// Default byte bound of the in-memory deps cache ([`GuardedFetcher::with_cache_capacity`]).
pub const DEFAULT_DEPS_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// URL -> (bytes, sha256, content type). Content-addressed by the digest it records.
type CacheEntry = (Bytes, Digest, Option<String>);

/// LRU of fetched modules bounded by their total bytes.
struct DepsCache {
    entries: HashMap<String, (CacheEntry, u64)>,
    /// Use tick -> URL, oldest first.
    order: BTreeMap<u64, String>,
    tick: u64,
    bytes: u64,
    capacity: u64,
}

impl DepsCache {
    fn new(capacity: u64) -> Self {
        Self {
            entries: HashMap::new(),
            order: BTreeMap::new(),
            tick: 0,
            bytes: 0,
            capacity,
        }
    }

    fn get(&mut self, url: &str) -> Option<CacheEntry> {
        self.tick += 1;
        let tick = self.tick;
        let (entry, used) = self.entries.get_mut(url)?;
        self.order.remove(used);
        *used = tick;
        self.order.insert(tick, url.to_string());
        Some(entry.clone())
    }

    fn remove(&mut self, url: &str) {
        if let Some((entry, used)) = self.entries.remove(url) {
            self.order.remove(&used);
            self.bytes -= entry.0.len() as u64;
        }
    }

    fn insert(&mut self, url: &str, entry: CacheEntry) {
        self.remove(url);
        let size = entry.0.len() as u64;
        if size > self.capacity {
            return;
        }
        while self.bytes + size > self.capacity {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some((e, _)) = self.entries.remove(&oldest) {
                self.bytes -= e.0.len() as u64;
            }
        }
        self.tick += 1;
        self.order.insert(self.tick, url.to_string());
        self.entries.insert(url.to_string(), (entry, self.tick));
        self.bytes += size;
    }
}

/// Fetches through the shared [`EgressGuard`] (builder policy: `build.allowed_hosts`), caching
/// results in the shared deps cache so a second service importing the same dependency makes no
/// network call.
pub struct GuardedFetcher {
    egress: Arc<dyn EgressGuard>,
    cfg: FetcherConfig,
    cache: Mutex<DepsCache>,
    /// Durable, content-addressed copy of every fetched module (the artifact store): a rebuild
    /// with `rt.lock` takes locked modules from here, so it reproduces the same code even when
    /// the CDN now serves different bytes or is unreachable.
    vendor: Option<Arc<dyn ArtifactStore>>,
}

impl GuardedFetcher {
    pub fn new(egress: Arc<dyn EgressGuard>, cfg: FetcherConfig) -> Self {
        Self {
            egress,
            cfg,
            cache: Mutex::new(DepsCache::new(DEFAULT_DEPS_CACHE_BYTES)),
            vendor: None,
        }
    }

    /// Keep fetched modules in `store` and serve locked digests from it.
    pub fn with_vendor_store(mut self, store: Arc<dyn ArtifactStore>) -> Self {
        self.vendor = Some(store);
        self
    }

    /// Bound the in-memory deps cache to `bytes` (default [`DEFAULT_DEPS_CACHE_BYTES`]); the least
    /// recently used modules go first, and a module larger than the bound is not cached.
    pub fn with_cache_capacity(self, bytes: u64) -> Self {
        *self.lock_cache() = DepsCache::new(bytes);
        self
    }

    /// Bytes currently held by the deps cache.
    pub fn cache_bytes(&self) -> u64 {
        self.lock_cache().bytes
    }

    fn lock_cache(&self) -> std::sync::MutexGuard<'_, DepsCache> {
        self.cache.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn host_allowed(&self, url: &Url) -> bool {
        let Some(host) = url.host_str() else { return false };
        let host = host.trim_end_matches('.');
        self.cfg
            .allowed_hosts
            .iter()
            .any(|h| h.trim_end_matches('.').eq_ignore_ascii_case(host))
    }

    /// The shared deps cache entry for `url`. A URL with a query string may carry a credential
    /// (`?token=…`), so it is never answered from the cache another build filled.
    fn cached(&self, url: &Url) -> Option<CacheEntry> {
        if !shareable(url) {
            return None;
        }
        self.lock_cache().get(url.as_str())
    }
}

/// Whether a fetched module may be shared through the deps cache (FETCH-004). A query string may be
/// a credential (`?token=…`), so only the one the resolver itself adds to npm/jsr URLs
/// (`?target=es2022`, `resolve.rs` `cdn_url`) is shared.
fn shareable(url: &Url) -> bool {
    url.query().is_none_or(|q| q == "target=es2022")
}

fn check_expected(url: &Url, expected: Option<Digest>, actual: Digest) -> Result<(), FetchError> {
    match expected {
        Some(e) if e != actual => Err(FetchError::DigestMismatch {
            url: url.to_string(),
            expected: e,
            actual,
        }),
        _ => Ok(()),
    }
}

#[async_trait]
impl ModuleFetcher for GuardedFetcher {
    async fn fetch(&self, url: &Url, expected: Option<Digest>) -> Result<Fetched, FetchError> {
        // The shared deps cache never bypasses the lock.
        if let Some((bytes, digest, content_type)) = self.cached(url) {
            // With a vendor store a different locked digest may still be on disk (the URL served
            // other bytes to another build since); without one the mismatch is final.
            if expected.is_none_or(|e| e == digest) || self.vendor.is_none() {
                check_expected(url, expected, digest)?;
                // Re-vendor (a put of present bytes only touches them): the lock this build writes
                // finds the module on disk, and GC's grace period restarts while the build runs.
                if let Some(store) = &self.vendor {
                    let _ = store.put_vendored(bytes.clone(), url.as_str()).await;
                }
                return Ok(Fetched {
                    url: url.clone(),
                    bytes,
                    digest,
                    content_type,
                    from_cache: true,
                });
            }
        }
        // A locked module vendored from this URL by an earlier build: the store re-hashes it on
        // read. Only one vendored *from this URL*: the store also holds every tenant's uploads,
        // and a lock must not read those by digest (FETCH-003).
        if let (Some(store), Some(want)) = (&self.vendor, expected) {
            let vendored = store.vendored_from(&want, url.as_str()).await.unwrap_or(false);
            if let Some(bytes) = if vendored { store.get(&want).await.ok() } else { None } {
                // In use by this build: keep it out of GC for the grace period.
                let _ = store.touch(&want).await;
                return Ok(Fetched {
                    url: url.clone(),
                    bytes,
                    digest: want,
                    content_type: None,
                    from_cache: true,
                });
            }
        }
        if !self.host_allowed(url) {
            return Err(FetchError::Denied(url.to_string()));
        }

        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(url.as_str())
            .header(http::header::ACCEPT, "*/*")
            .body(Body::empty())
            .map_err(|_| FetchError::Denied(url.to_string()))?; // LCOV_EXCL_LINE: a serialized `Url` is always a valid `http::Uri`
        let policy = EgressPolicy::for_build(self.cfg.allowed_hosts.clone());
        let deadline = Instant::now() + self.cfg.timeout;
        let mut budget = EgressBudget::new(&Limits::default(), deadline, None);

        let response =
            match tokio::time::timeout(self.cfg.timeout, self.egress.send(request, &policy, &mut budget)).await {
                Err(_) => return Err(FetchError::Timeout(url.to_string())),
                Ok(Err(EgressError::Denied)) => return Err(FetchError::Denied(url.to_string())),
                Ok(Err(EgressError::TooLarge)) => return Err(FetchError::TooLarge),
                Ok(Err(EgressError::Timeout)) => return Err(FetchError::Timeout(url.to_string())),
                Ok(Err(_)) => return Err(FetchError::Upstream(url.to_string())),
                Ok(Ok(r)) => r,
            };

        let status = response.status();
        if status == http::StatusCode::NOT_FOUND || status == http::StatusCode::GONE {
            return Err(FetchError::NotFound(url.to_string()));
        }
        if !status.is_success() {
            return Err(FetchError::Upstream(url.to_string()));
        }
        let content_type = response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let limit = self.cfg.max_module_bytes.min(self.cfg.max_total_bytes);
        let bytes = match tokio::time::timeout(self.cfg.timeout, response.into_body().collect(limit)).await {
            Err(_) => return Err(FetchError::Timeout(url.to_string())),
            Ok(Err(rt_types::BodyError::TooLarge { .. })) => return Err(FetchError::TooLarge),
            Ok(Err(_)) => return Err(FetchError::Upstream(url.to_string())),
            Ok(Ok(b)) => b,
        };
        let digest = Digest::of(&bytes);
        check_expected(url, expected, digest)?;
        if let Some(store) = &self.vendor {
            if let Err(e) = store.put_vendored(bytes.clone(), url.as_str()).await {
                tracing::warn!(%url, error = %e, "could not vendor a fetched module (rebuilds will refetch it)");
            }
        }
        if shareable(url) {
            self.lock_cache()
                .insert(url.as_str(), (bytes.clone(), digest, content_type.clone()));
        }
        Ok(Fetched {
            url: url.clone(),
            bytes,
            digest,
            content_type,
            from_cache: false,
        })
    }

    fn max_total_bytes(&self) -> Option<u64> {
        Some(self.cfg.max_total_bytes)
    }
}
