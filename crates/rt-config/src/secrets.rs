//! Node secrets from env / files (docs/config.md source 4). Never read from `rt.toml`.

use crate::{ConfigError, NodeSecrets, Secret, SecurityConfig};

fn lookup<'a>(env: &'a [(String, String)], name: &str) -> Option<&'a str> {
    // the last occurrence wins, like a shell re-export
    env.iter().rev().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

fn read_secret_file(name: &'static str, path: &str) -> Result<String, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Secret {
        name,
        reason: format!("cannot read `{path}`: {e}"),
    })?;
    let text = text.trim_end_matches(['\n', '\r']).to_string();
    if text.trim().is_empty() {
        return Err(ConfigError::Secret {
            name,
            reason: format!("`{path}` is empty"),
        });
    }
    Ok(text)
}

/// `NAME` or `NAME_FILE` (not both). An empty `NAME` counts as unset.
fn from_var_or_file(
    env: &[(String, String)],
    var: &'static str,
    file_var: &'static str,
) -> Result<Option<String>, ConfigError> {
    let direct = lookup(env, var).filter(|v| !v.is_empty());
    let file = lookup(env, file_var).filter(|v| !v.is_empty());
    match (direct, file) {
        (Some(_), Some(_)) => Err(ConfigError::Secret {
            name: var,
            reason: format!("both {var} and {file_var} are set; use one"),
        }),
        (Some(v), None) => Ok(Some(v.to_string())),
        (None, Some(path)) => read_secret_file(file_var, path).map(Some),
        (None, None) => Ok(None),
    }
}

fn check_master_key(key: &str) -> Result<(), ConfigError> {
    check_key_hex("RT_MASTER_KEY", key)
}

fn check_key_hex(name: &'static str, key: &str) -> Result<(), ConfigError> {
    if key.len() != 64 {
        return Err(ConfigError::Secret {
            name,
            reason: format!("must be 32 bytes as 64 hex characters (got {} characters)", key.len()),
        });
    }
    if !key.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ConfigError::Secret {
            name,
            reason: "must be hex (0-9, a-f)".into(),
        });
    }
    Ok(())
}

impl NodeSecrets {
    /// Read `RT_ROOT_TOKEN[_FILE]` and `RT_MASTER_KEY[_FILE]` (plus `security.master_key_file`).
    pub fn from_env(env: &[(String, String)], security: &SecurityConfig) -> Result<NodeSecrets, ConfigError> {
        let root_token = from_var_or_file(env, "RT_ROOT_TOKEN", "RT_ROOT_TOKEN_FILE")?.map(Secret::new);

        let mut master = from_var_or_file(env, "RT_MASTER_KEY", "RT_MASTER_KEY_FILE")?;
        if master.is_none() && !security.master_key_file.is_empty() {
            master = Some(
                read_secret_file("RT_MASTER_KEY", &security.master_key_file).map_err(|e| match e {
                    ConfigError::Secret { reason, .. } => ConfigError::Secret {
                        name: "RT_MASTER_KEY",
                        reason: format!("security.master_key_file: {reason}"),
                    },
                    other => other,
                })?,
            );
        }
        let master_key = match master {
            Some(k) => {
                let k = k.trim().to_string();
                check_master_key(&k)?;
                Some(Secret::new(k))
            }
            None => None,
        };
        let master_key_previous = match from_var_or_file(env, "RT_MASTER_KEY_PREVIOUS", "RT_MASTER_KEY_PREVIOUS_FILE")?
        {
            Some(k) => {
                let k = k.trim().to_string();
                check_key_hex("RT_MASTER_KEY_PREVIOUS", &k)?;
                Some(Secret::new(k))
            }
            None => None,
        };
        Ok(NodeSecrets {
            root_token,
            master_key,
            master_key_previous,
        })
    }
}
