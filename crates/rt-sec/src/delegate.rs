//! Self-service tokens (decision D-B4, SECURITY.md): what a key may hand to a key it
//! creates.
//!
//! A new key never gets more than its creator holds:
//!
//! - **scopes**: a subset of the creator's scopes;
//! - **projects**: a subset of the creator's project restriction. A creator restricted to some
//!   projects cannot create an unrestricted key: asking for no restriction gives the creator's own;
//! - **environments**: a subset of the creator's environment restriction, by the same rule;
//! - **expiry**: not later than the creator's own. A creator that expires cannot create a key that
//!   does not: asking for no expiry gives the creator's own.
//!
//! The operator (and a key without restrictions) holds [`Grant::unrestricted`].

use rt_types::{ProjectId, Scope, Scopes, UnixMs};
use thiserror::Error;

/// What a key may do: the scopes it holds, the projects and environments it is limited to (`None`
/// = all) and when it stops working (`None` = never).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub scopes: Scopes,
    pub project_ids: Option<Vec<ProjectId>>,
    pub environments: Option<Vec<String>>,
    pub expires_at_ms: Option<UnixMs>,
}

impl Grant {
    /// Every scope, every project and environment, no expiry: the operator acting for a tenant.
    pub fn unrestricted() -> Self {
        Self {
            scopes: Scopes::all(),
            project_ids: None,
            environments: None,
            expires_at_ms: None,
        }
    }
}

/// Why a requested key exceeds its creator.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum Escalation {
    #[error("scope `{0}` is not held by the key that creates the new key")]
    Scope(Scope),
    #[error("project {0} is outside the project restriction of the key that creates the new key")]
    Project(ProjectId),
    #[error("environment `{0}` is outside the environment restriction of the key that creates the new key")]
    Environment(String),
    #[error("`expires_at` is later than the expiry of the key that creates the new key")]
    Expiry,
}

/// The grant of a new key: `requested`, checked against `parent` and with omitted restrictions
/// inherited from it. `Err` names the first thing that exceeds the creator.
pub fn delegate(parent: &Grant, requested: &Grant) -> Result<Grant, Escalation> {
    if let Some(missing) = requested.scopes.iter().find(|s| !parent.scopes.contains(*s)) {
        return Err(Escalation::Scope(missing));
    }
    let project_ids = match (&parent.project_ids, &requested.project_ids) {
        (None, asked) => asked.clone(),
        (Some(allowed), None) => Some(allowed.clone()),
        (Some(allowed), Some(asked)) => {
            if let Some(outside) = asked.iter().find(|p| !allowed.contains(p)) {
                return Err(Escalation::Project(*outside));
            }
            Some(asked.clone())
        }
    };
    let environments = match (&parent.environments, &requested.environments) {
        (None, asked) => asked.clone(),
        (Some(allowed), None) => Some(allowed.clone()),
        (Some(allowed), Some(asked)) => {
            if let Some(outside) = asked.iter().find(|e| !allowed.contains(e)) {
                return Err(Escalation::Environment(outside.clone()));
            }
            Some(asked.clone())
        }
    };
    let expires_at_ms = match (parent.expires_at_ms, requested.expires_at_ms) {
        (None, asked) => asked,
        (Some(own), None) => Some(own),
        (Some(own), Some(asked)) if asked <= own => Some(asked),
        (Some(_), Some(_)) => return Err(Escalation::Expiry),
    };
    Ok(Grant {
        scopes: requested.scopes.clone(),
        project_ids,
        environments,
        expires_at_ms,
    })
}
