//! Projects (decision D-B3): a named group of services of one tenant, with its own list of
//! environments. Every service belongs to one project and to one of that project's environments.
//! Environment labels are project-local: `env:<name>` in a call rule (D-A1) means the environment
//! `<name>` of the target's own project, so another project's label with the same name never
//! matches.

use serde::{Deserialize, Serialize};

use crate::ids::{ProjectId, TenantId};
use crate::model::{valid_environment, valid_service_slug, UnixMs, DEFAULT_ENVIRONMENT};

/// The slug of the project every tenant has from the start, and of every service created without
/// one.
pub const DEFAULT_PROJECT: &str = "default";

/// Most projects one tenant may own.
pub const MAX_PROJECTS_PER_TENANT: usize = 100;

/// Most environments one project may list.
pub const MAX_PROJECT_ENVIRONMENTS: usize = 20;

/// Longest project display name.
pub const MAX_PROJECT_NAME_LEN: usize = 100;

/// `default`: the project of services created without one.
pub fn default_project() -> String {
    DEFAULT_PROJECT.to_string()
}

/// Environments of a project created without any: `["production"]`.
pub fn default_project_environments() -> Vec<String> {
    vec![DEFAULT_ENVIRONMENT.to_string()]
}

/// A project slug: `^[a-z][a-z0-9-]{1,62}$` (the same shape as a service slug). Immutable.
pub fn valid_project_slug(s: &str) -> bool {
    valid_service_slug(s)
}

/// The id of a tenant's `default` project: the tenant's ULID under the `prj_` prefix. Derived so
/// the schema migration can create the project of every existing tenant in plain SQL.
pub fn default_project_id(tenant: TenantId) -> ProjectId {
    ProjectId::from_ulid(tenant.ulid())
}

/// A list of environments for a project: at least one, at most [`MAX_PROJECT_ENVIRONMENTS`], each a
/// valid label, none twice.
pub fn check_project_environments(envs: &[String]) -> Result<(), String> {
    if envs.is_empty() {
        return Err("`environments` must list at least one environment".into());
    }
    if envs.len() > MAX_PROJECT_ENVIRONMENTS {
        return Err(format!(
            "`environments` holds at most {MAX_PROJECT_ENVIRONMENTS} entries"
        ));
    }
    for (i, e) in envs.iter().enumerate() {
        if !valid_environment(e) {
            return Err(format!("environment `{e}` must match ^[a-z][a-z0-9-]{{0,31}}$"));
        }
        if envs[..i].contains(e) {
            return Err(format!("environment `{e}` is listed twice"));
        }
    }
    Ok(())
}

/// A project of a tenant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub tenant_id: TenantId,
    /// Unique per tenant, immutable.
    pub slug: String,
    pub name: String,
    /// The environments a service of the project may be in; never empty.
    pub environments: Vec<String>,
    pub created_at_ms: UnixMs,
    pub updated_at_ms: UnixMs,
}

/// Input to `MetaStore::create_project`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewProject {
    pub slug: String,
    /// Display name (default: the slug).
    #[serde(default)]
    pub name: Option<String>,
    /// Default: `["production"]`.
    #[serde(default = "default_project_environments")]
    pub environments: Vec<String>,
}

impl NewProject {
    pub fn new(slug: impl Into<String>) -> Self {
        Self {
            slug: slug.into(),
            name: None,
            environments: default_project_environments(),
        }
    }
}

/// Partial update of a project: each present field replaces that field.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectPatch {
    pub name: Option<String>,
    /// The new complete list. Dropping an environment that a service is in is a conflict
    /// (`environment_in_use`).
    pub environments: Option<Vec<String>>,
}
