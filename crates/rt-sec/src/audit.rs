//! Audit events (SEC-20): deploy, rollback, key create/delete, secret write, tenant change.

use rt_types::{ServiceId, TenantId, UnixMs};
use serde::{Deserialize, Serialize};

/// Who performed an audited action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Actor {
    Operator,
    Key { key_id: String, tenant: TenantId },
    System,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub ts_ms: UnixMs,
    pub actor: Actor,
    pub tenant: Option<TenantId>,
    pub service: Option<ServiceId>,
    /// `deploy`, `rollback`, `key.created`, `key.deleted`, `secret.written`, `tenant.updated`, …
    pub action: String,
    /// Free-form target / details. Never contains secret values.
    pub data: serde_json::Value,
}

/// Extension point (paid: durable, signed, exportable audit log). Core: an `events` row + a log line.
pub trait AuditSink: Send + Sync {
    fn emit(&self, ev: AuditEvent);
}

/// Sink that only writes a `tracing` line.
#[derive(Clone, Copy, Debug, Default)]
pub struct TracingAuditSink;

impl AuditSink for TracingAuditSink {
    fn emit(&self, ev: AuditEvent) {
        tracing::info!(target: "rt::audit", action = %ev.action, tenant = ?ev.tenant, service = ?ev.service, "audit");
    }
}
