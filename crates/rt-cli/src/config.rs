//! `~/.config/21ci/config.toml`: named contexts (control API, invoke listener, token) and the
//! rules that turn flags, environment and the current context into [`Settings`].
//!
//! Precedence, highest first: environment (`RT_API`, `RT_INVOKE`, `RT_KEY` > `RT_ROOT_TOKEN`,
//! `RT_TENANT` / `--as-tenant`), the selected context (`--context`, `RT_CONTEXT`, else the
//! current one), built-in defaults (the local dev node).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{CliError, Result};

pub const DEFAULT_API: &str = "http://127.0.0.1:8081";
pub const DEFAULT_INVOKE: &str = "http://127.0.0.1:8080";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Context {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invoke: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// `ten_…` sent as `x-rt-tenant` (operator token acting for a tenant).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_context: Option<String>,
    #[serde(default)]
    pub contexts: BTreeMap<String, Context>,
}

/// Everything a command needs to talk to a node.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    pub context: Option<String>,
    pub api: String,
    pub invoke: String,
    pub token: Option<String>,
    pub tenant: Option<String>,
}

/// `--config`, else `$XDG_CONFIG_HOME/21ci/config.toml`, else `~/.config/21ci/config.toml`.
pub fn config_path(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(p) = explicit {
        return Ok(p.to_path_buf());
    }
    if let Some(x) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(x).join("21ci").join("config.toml"));
    }
    let home =
        std::env::home_dir().ok_or_else(|| CliError::usage("cannot find the home directory; pass --config <file>"))?;
    Ok(home.join(".config").join("21ci").join("config.toml"))
}

impl ConfigFile {
    pub fn load(path: &Path) -> Result<ConfigFile> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                toml::from_str(&text).map_err(|e| CliError::usage(format!("cannot parse {}: {e}", path.display())))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ConfigFile::default()),
            Err(e) => Err(CliError::failed(format!("cannot read {}: {e}", path.display()))),
        }
    }

    /// Writes the file with mode 0600 (it holds tokens).
    pub fn save(&self, path: &Path) -> Result<()> {
        let text =
            toml::to_string_pretty(self).map_err(|e| CliError::failed(format!("cannot serialize the config: {e}")))?;
        let io = |e: std::io::Error| CliError::failed(format!("cannot write {}: {e}", path.display()));
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(io)?;
        }
        write_private(path, text.as_bytes()).map_err(io)
    }

    /// Settings for this run. `env` is the environment lookup (injectable for tests).
    pub fn resolve(
        &self,
        context_flag: Option<&str>,
        as_tenant: Option<&str>,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Settings> {
        let name = context_flag
            .map(str::to_string)
            .or_else(|| self.current_context.clone());
        let ctx = match &name {
            Some(n) => Some(
                self.contexts
                    .get(n)
                    .ok_or_else(|| CliError::usage(format!("context \"{n}\" not found")))?,
            ),
            None => None,
        };
        let get = |k: &str| env(k).filter(|v| !v.is_empty());
        let api = get("RT_API")
            .or_else(|| ctx.and_then(|c| c.api.clone()))
            .unwrap_or_else(|| DEFAULT_API.to_string());
        let invoke = get("RT_INVOKE")
            .or_else(|| ctx.and_then(|c| c.invoke.clone()))
            .unwrap_or_else(|| DEFAULT_INVOKE.to_string());
        let token = get("RT_KEY")
            .or_else(|| get("RT_ROOT_TOKEN"))
            .or_else(|| ctx.and_then(|c| c.token.clone()));
        let tenant = as_tenant
            .map(str::to_string)
            .filter(|v| !v.is_empty())
            .or_else(|| ctx.and_then(|c| c.tenant.clone()));
        Ok(Settings {
            context: name,
            api: api.trim_end_matches('/').to_string(),
            invoke: invoke.trim_end_matches('/').to_string(),
            token,
            tenant,
        })
    }
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    // an existing file keeps its old mode through `mode()`: tighten it
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    f.write_all(bytes)
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ConfigFile {
        let mut c = ConfigFile::default();
        c.contexts.insert(
            "a".into(),
            Context {
                api: Some("http://a/".into()),
                invoke: Some("http://ia".into()),
                token: Some("tok-a".into()),
                tenant: None,
            },
        );
        c.contexts.insert(
            "b".into(),
            Context {
                api: Some("http://b".into()),
                ..Context::default()
            },
        );
        c.current_context = Some("a".into());
        c
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn current_context_supplies_the_values_and_trailing_slashes_go() {
        let s = cfg().resolve(None, None, &no_env).unwrap();
        assert_eq!(s.api, "http://a");
        assert_eq!(s.invoke, "http://ia");
        assert_eq!(s.token.as_deref(), Some("tok-a"));
        assert_eq!(s.context.as_deref(), Some("a"));
    }

    #[test]
    fn flag_beats_current_and_defaults_fill_the_gaps() {
        let s = cfg().resolve(Some("b"), None, &no_env).unwrap();
        assert_eq!(s.api, "http://b");
        assert_eq!(s.invoke, DEFAULT_INVOKE);
        assert_eq!(s.token, None);
    }

    #[test]
    fn env_beats_context_and_empty_env_is_unset() {
        let env = |k: &str| match k {
            "RT_API" => Some("http://env/".to_string()),
            "RT_ROOT_TOKEN" => Some("root".to_string()),
            "RT_KEY" => Some(String::new()),
            _ => None,
        };
        let s = cfg().resolve(None, None, &env).unwrap();
        assert_eq!(s.api, "http://env");
        assert_eq!(s.token.as_deref(), Some("root"));
        let env2 = |k: &str| (k == "RT_KEY").then(|| "key".to_string());
        assert_eq!(cfg().resolve(None, None, &env2).unwrap().token.as_deref(), Some("key"));
    }

    #[test]
    fn nothing_configured_means_the_local_dev_node() {
        let s = ConfigFile::default().resolve(None, None, &no_env).unwrap();
        assert_eq!((s.api.as_str(), s.invoke.as_str()), (DEFAULT_API, DEFAULT_INVOKE));
        assert_eq!(s.context, None);
    }

    #[test]
    fn unknown_context_is_a_usage_error() {
        assert!(matches!(
            cfg().resolve(Some("zzz"), None, &no_env),
            Err(CliError::Usage(_))
        ));
    }

    #[test]
    fn round_trips_through_toml() {
        let text = toml::to_string_pretty(&cfg()).unwrap();
        assert!(text.contains("current_context = \"a\""), "{text}");
        assert!(text.contains("[contexts.a]"), "{text}");
        let back: ConfigFile = toml::from_str(&text).unwrap();
        assert_eq!(back, cfg());
    }
}
