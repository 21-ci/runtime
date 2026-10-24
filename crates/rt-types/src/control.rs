//! `ControlPlane`: everything an executor / host layer learns about tenants and services,
//! and the sink for what it reports back.
//!
//! MVP: single process (`--role=all`), so the implementation is an in-proc adapter over
//! `MetaStore` + `SecretsBackend` living in `rt-server`. The trait keeps the later
//! supervisor/executor split a transport change.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use thiserror::Error;
use zeroize::Zeroizing;

use crate::ids::{SecretsVersion, ServiceId};
use crate::model::{EnvBundle, NodeEvent, RouteSnapshot, Schedule, Service, UsageBatch};
use crate::outcome::BuildFailure;
use crate::plan::Plan;
use crate::task::PreparedKey;

/// Protocol/version of the control-plane contract. A mismatch between peers refuses start.
pub const CONTROL_PLANE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CpError {
    #[error("not found")]
    NotFound,
    /// e.g. executor secret fetch anomaly threshold (`secrets.max_services_per_min`).
    #[error("rate limited")]
    RateLimited,
    #[error("control plane unavailable")]
    Unavailable,
    #[error("internal control-plane error: {0}")]
    Internal(String),
}

/// Complete config at one point in time.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConfigSnapshot {
    pub routes: Arc<RouteSnapshot>,
    pub services: Vec<Service>,
    pub schedules: Vec<Schedule>,
    /// Effective plans by name (config + SIGHUP reloads).
    pub plans: BTreeMap<String, Plan>,
}

/// Incremental change after the initial snapshot.
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigDelta {
    Routes(Arc<RouteSnapshot>),
    ServiceUpserted(Box<Service>),
    ServiceRemoved(ServiceId),
    ScheduleUpserted(Box<Schedule>),
    ScheduleRemoved(crate::ids::ScheduleId),
    Plans(BTreeMap<String, Plan>),
}

/// First item is always `Snapshot`, later items are `Delta`s.
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigEvent {
    Snapshot(Arc<ConfigSnapshot>),
    Delta(ConfigDelta),
}

pub type ConfigStream = BoxStream<'static, ConfigEvent>;

/// Result of a build, reported back so the control plane can set the version's build state.
#[derive(Clone, Debug, PartialEq)]
pub struct BuildReport {
    pub key: PreparedKey,
    /// `Ok(())` or the failure.
    pub result: Result<(), BuildFailure>,
    pub duration_ms: u64,
    /// Digest of the produced `rt.lock` (deno), if any.
    pub lock_digest: Option<crate::digest::Digest>,
    /// Digest of the produced bundle (deno), recorded on the version for audit.
    pub bundle_digest: Option<crate::digest::Digest>,
}

#[async_trait]
pub trait ControlPlane: Send + Sync + 'static {
    /// Current route snapshot (lock-free read; also delivered through `subscribe_config`).
    fn routes(&self) -> Arc<RouteSnapshot>;

    /// Snapshot first, then deltas: routes, services, schedules, plans.
    async fn subscribe_config(&self) -> ConfigStream;

    /// A service record (any tenant; callers are trusted in-process code).
    async fn service(&self, id: ServiceId) -> Result<Option<Service>, CpError>;

    /// Env + decrypted secrets for `service`. Rate-limited per `secrets.max_services_per_min`.
    /// `version` is the caller's cached generation; the plane may answer from its own cache.
    async fn env_for(&self, service: ServiceId, version: SecretsVersion) -> Result<Zeroizing<EnvBundle>, CpError>;

    /// [`ControlPlane::env_for`] as a shared bundle: a plane that caches bundles answers without a
    /// round-trip and every task of the service shares one copy. A cached bundle older than
    /// `version` is refreshed. The default fetches a fresh bundle per call.
    async fn env_shared(
        &self,
        service: ServiceId,
        version: SecretsVersion,
    ) -> Result<Arc<Zeroizing<EnvBundle>>, CpError> {
        self.env_for(service, version).await.map(Arc::new)
    }

    /// Record a finished (or failed) build for `key`.
    async fn report_build(&self, key: &PreparedKey, report: BuildReport) -> Result<(), CpError>;

    /// Fire-and-forget usage; flushed every 5 s by the implementation.
    fn usage(&self, batch: UsageBatch);

    /// Fire-and-forget node/audit event.
    fn event(&self, ev: NodeEvent);
}
