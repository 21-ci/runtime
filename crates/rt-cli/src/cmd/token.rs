//! `21ci token create | list | revoke`: your own API keys (decision D-B4).
//!
//! The operator's `21ci key …` manages the keys of any tenant. These commands call `/v1/keys`,
//! which acts on the tenant of the key you authenticate with (scope `keys:write`): a new key gets
//! at most your scopes, your project and environment restriction and your lifetime, and the server refuses
//! anything more.

use std::time::SystemTime;

use serde_json::json;

use super::key::{show_created, show_list, show_revoked};
use super::App;
use crate::cli::TokenCmd;
use crate::error::{CliError, Result};
use crate::fmt;

pub fn run(app: &App, cmd: &TokenCmd) -> Result<()> {
    match cmd {
        TokenCmd::Create {
            scopes,
            name,
            projects,
            environments,
            expires_at,
            expires_in,
        } => {
            let mut body = json!({"scopes": scopes});
            if let Some(n) = name {
                body["name"] = json!(n);
            }
            if !projects.is_empty() {
                body["project_ids"] = json!(projects);
            }
            if !environments.is_empty() {
                body["environments"] = json!(environments);
            }
            if let Some(at) = expires_at {
                if humantime::parse_rfc3339(at).is_err() {
                    return Err(CliError::usage(format!(
                        "invalid --expires-at `{at}`: use an RFC 3339 timestamp such as 2027-01-01T00:00:00Z"
                    )));
                }
                body["expires_at"] = json!(at);
            }
            if let Some(d) = expires_in {
                let at = SystemTime::now()
                    .checked_add(*d)
                    .ok_or_else(|| CliError::usage("--expires-in is too far in the future"))?;
                body["expires_at"] = json!(humantime::format_rfc3339_seconds(at).to_string());
            }
            let k = app.client()?.post("/v1/keys", &body)?;
            show_created(app, &k);
        }
        TokenCmd::List => {
            let keys = app.client()?.get("/v1/keys", &[])?;
            show_list(app, &keys);
        }
        TokenCmd::Revoke { key } => {
            app.client()?.delete(&format!("/v1/keys/{}", fmt::enc(key)), &[])?;
            show_revoked(app, key);
        }
    }
    Ok(())
}
