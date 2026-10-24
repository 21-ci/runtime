//! Isolation domains (extension point). Core: everything runs in one executor.

use rt_types::TenantId;

/// Identifies the executor process/shard a tenant's tasks run in. Core has exactly one (`0`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct IsolationDomain(pub u32);

pub trait IsolationBackend: Send + Sync {
    fn domain_for(&self, tenant: TenantId) -> IsolationDomain;
}

/// Core backend: one local domain.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalIsolation;

impl IsolationBackend for LocalIsolation {
    fn domain_for(&self, _tenant: TenantId) -> IsolationDomain {
        IsolationDomain(0)
    }
}
