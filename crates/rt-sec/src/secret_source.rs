//! External secret sources (R15). The path a source reads is built here, from the authenticated
//! tenant and the service's project and environment, and never from a tenant's input alone: the
//! node's token could read anything, and a tenant chooses the relative path (confused deputy).

use async_trait::async_trait;
use zeroize::Zeroizing;

/// Why a source gave no value. Carries no text from the source: tenants see these codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceError {
    /// The source cannot be reached, or refuses the node.
    Unavailable,
    /// No such secret or field, or the value is unusable.
    NotFound,
}

impl SourceError {
    pub fn code(self) -> &'static str {
        match self {
            Self::Unavailable => "secret_source_unavailable",
            Self::NotFound => "secret_source_not_found",
        }
    }
}

/// One value read from a source.
pub struct SourceValue {
    pub value: Zeroizing<String>,
    pub version: Option<i64>,
}

#[async_trait]
pub trait SecretSource: Send + Sync {
    /// Read `field` at `full_path`, as [`full_path`] built it.
    async fn read(&self, full_path: &str, field: &str) -> Result<SourceValue, SourceError>;
}

const FORBIDDEN: [char; 6] = ['%', '\\', '?', '#', ' ', '\0'];

fn clean_segments(path: &str) -> bool {
    !path.chars().any(|c| FORBIDDEN.contains(&c) || c.is_control())
        && path.split('/').all(|seg| !seg.is_empty() && seg != "." && seg != "..")
}

/// Whether a tenant's relative `path` can stay under its prefix.
pub fn valid_ref_path(path: &str) -> bool {
    !path.is_empty() && path.len() <= 512 && clean_segments(path)
}

/// Whether a template names all three variables, so a prefix can never span tenants.
pub fn valid_template(template: &str) -> bool {
    ["{tenant}", "{project}", "{environment}"]
        .iter()
        .all(|v| template.contains(v))
}

/// The path to read, or `None` when the relative path, or a value of a variable, could leave the
/// prefix.
pub fn full_path(template: &str, tenant: &str, project: &str, environment: &str, path: &str) -> Option<String> {
    if !valid_ref_path(path)
        || [tenant, project, environment]
            .iter()
            .any(|v| v.contains('/') || !clean_segments(v))
    {
        return None;
    }
    let prefix = template
        .replace("{tenant}", tenant)
        .replace("{project}", project)
        .replace("{environment}", environment);
    Some(format!(
        "{}{path}",
        if prefix.ends_with('/') { prefix } else { prefix + "/" }
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: &str = "rt/{tenant}/{project}/{environment}/";

    #[test]
    fn the_prefix_comes_from_the_service_and_the_path_cannot_leave_it() {
        assert_eq!(
            full_path(T, "a", "p", "prod", "app/db").as_deref(),
            Some("rt/a/p/prod/app/db")
        );
        for bad in [
            "",
            "..",
            "../b/p/prod/db",
            "app/../../x",
            "a//b",
            "/abs",
            "a/",
            "%2e%2e/x",
            "a%2fb",
            "a\\b",
            "a?b",
            "a#b",
            "a b",
            "a\nb",
            ".",
            "./a",
        ] {
            assert_eq!(full_path(T, "a", "p", "prod", bad), None, "{bad:?}");
        }
        assert_eq!(full_path(T, "a", "p", "pr/od", "x"), None);
        assert_eq!(full_path(T, "a", "..", "prod", "x"), None);
    }

    #[test]
    fn a_template_needs_all_three_variables() {
        assert!(valid_template(T));
        assert!(!valid_template("rt/{tenant}/{project}/"));
        assert!(!valid_template("rt/"));
    }

    #[test]
    fn codes_are_stable() {
        assert_eq!(SourceError::Unavailable.code(), "secret_source_unavailable");
        assert_eq!(SourceError::NotFound.code(), "secret_source_not_found");
    }
}
