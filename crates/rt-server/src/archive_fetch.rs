//! [`ArchiveFetcher`] over the egress guard: every hop checked for scheme, port, host rules and
//! address, the name resolved once and pinned, caller headers gone at the first cross-origin hop.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use rt_api::{ArchiveFetchError, ArchiveFetchFailure, ArchiveFetcher, ArchiveRequest, FetchedArchive, MAX_UPLOAD_BODY};
use rt_host::{EgressBudget, EgressError, EgressGuard, EgressGuardConfig, EgressPolicy, EgressUsage};
use rt_types::{Body, BodyError};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;

/// The whole fetch, redirects and body included.
const FETCH_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_REDIRECTS: usize = 3;

/// The node's guard settings, tightened for archives. Connections are not pooled: a fetch is rare
/// and carries credentials.
pub fn archive_guard_config(base: &EgressGuardConfig) -> EgressGuardConfig {
    let cfg = EgressGuardConfig {
        max_redirects: MAX_REDIRECTS,
        max_response_bytes: MAX_UPLOAD_BODY as u64,
        allow_https_downgrade: false,
        max_pooled_clients: 0,
        require_https: true,
        drop_headers_cross_origin: true,
        ..base.clone()
    };
    #[cfg(debug_assertions)]
    let cfg = rt_host::local_test_origin::apply(cfg);
    cfg
}

pub struct GuardedArchiveFetcher {
    guard: Arc<dyn EgressGuard>,
    /// Where the unlinked temporary files go (the data directory: the disk the node manages).
    tmp_dir: PathBuf,
}

impl GuardedArchiveFetcher {
    pub fn new(guard: Arc<dyn EgressGuard>, tmp_dir: PathBuf) -> Self {
        Self { guard, tmp_dir }
    }

    async fn run(&self, req: ArchiveRequest, usage: Arc<EgressUsage>) -> Result<FetchedArchive, ArchiveFetchError> {
        let failed = ArchiveFetchError::Failed { status: None };
        let policy = EgressPolicy::from_rules(&req.rules, req.default_deny);
        let mut builder = http::Request::builder().method(http::Method::GET).uri(&req.url);
        for (name, value) in &req.headers {
            builder = builder.header(name, value);
        }
        // An address `http` will not take cannot be fetched; the guard never saw it.
        let request = builder.body(Body::empty()).map_err(|_| ArchiveFetchError::Denied)?;
        let mut budget = EgressBudget::shared(
            1,
            Arc::new(Semaphore::new(1)),
            usage.clone(),
            Instant::now() + FETCH_TIMEOUT,
        )
        .for_tenant(req.tenant);
        let response = self
            .guard
            .send(request, &policy, &mut budget)
            .await
            .map_err(|e| match e {
                EgressError::Denied | EgressError::InsecureRedirect => ArchiveFetchError::Denied,
                EgressError::TooLarge => ArchiveFetchError::TooLarge,
                EgressError::Quota => ArchiveFetchError::Quota,
                EgressError::Limit | EgressError::Timeout | EgressError::Dns | EgressError::Upstream => failed,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(ArchiveFetchError::Failed {
                status: Some(status.as_u16()),
            });
        }
        let dir = self.tmp_dir.clone();
        let file = tokio::task::spawn_blocking(move || tempfile::tempfile_in(dir))
            .await
            .map_err(|_| failed)?
            .map_err(|_| failed)?;
        let mut file = tokio::fs::File::from_std(file);
        let mut len = 0u64;
        let mut stream = response.into_body().into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| match e {
                BodyError::TooLarge { .. } => ArchiveFetchError::TooLarge,
                _ if usage.quota_left() == Some(0) => ArchiveFetchError::Quota,
                _ => failed,
            })?;
            len = len.saturating_add(chunk.len() as u64);
            if len > MAX_UPLOAD_BODY as u64 {
                return Err(ArchiveFetchError::TooLarge);
            }
            file.write_all(&chunk).await.map_err(|_| failed)?;
        }
        file.flush().await.map_err(|_| failed)?;
        file.rewind().await.map_err(|_| failed)?;
        Ok(FetchedArchive {
            file,
            len,
            received: usage.received(),
        })
    }
}

#[async_trait]
impl ArchiveFetcher for GuardedArchiveFetcher {
    async fn fetch(&self, req: ArchiveRequest) -> Result<FetchedArchive, ArchiveFetchFailure> {
        let usage = EgressUsage::new(req.quota_left);
        let error = match tokio::time::timeout(FETCH_TIMEOUT, self.run(req, usage.clone())).await {
            Ok(Ok(done)) => return Ok(done),
            Ok(Err(e)) => e,
            Err(_) => ArchiveFetchError::Failed { status: None },
        };
        Err(ArchiveFetchFailure {
            error,
            received: usage.received(),
        })
    }
}
