//! `21ci config set-context | use-context | get-contexts | current`.

use std::path::Path;

use serde_json::json;

use crate::cli::ConfigCmd;
use crate::config::{config_path, ConfigFile, Context};
use crate::error::{CliError, Result};
use crate::fmt;

pub fn run(
    cmd: &ConfigCmd,
    json: bool,
    explicit_path: Option<&Path>,
    context_flag: Option<&str>,
    as_tenant: Option<&str>,
) -> Result<()> {
    let path = config_path(explicit_path)?;
    let mut file = ConfigFile::load(&path)?;
    match cmd {
        ConfigCmd::SetContext {
            name,
            api,
            invoke,
            token,
            tenant,
        } => {
            if name.trim().is_empty() {
                return Err(CliError::usage("the context name must not be empty"));
            }
            let existing = file.contexts.contains_key(name);
            if !existing && api.is_none() {
                return Err(CliError::usage(format!(
                    "a new context needs --api <url> (context \"{name}\")"
                )));
            }
            let ctx = file.contexts.entry(name.clone()).or_insert_with(Context::default);
            if let Some(v) = api {
                ctx.api = Some(v.trim_end_matches('/').to_string());
            }
            if let Some(v) = invoke {
                ctx.invoke = Some(v.trim_end_matches('/').to_string());
            }
            if let Some(v) = token {
                ctx.token = Some(v.clone());
            }
            if let Some(v) = tenant {
                ctx.tenant = Some(v.clone());
            }
            if file.current_context.is_none() {
                file.current_context = Some(name.clone());
            }
            file.save(&path)?;
            if json {
                fmt::print_json(&context_json(name, &file));
            } else {
                fmt::emitln(&format!("Context \"{name}\" set."));
            }
        }
        ConfigCmd::UseContext { name } => {
            if !file.contexts.contains_key(name) {
                return Err(CliError::usage(format!("context \"{name}\" not found")));
            }
            file.current_context = Some(name.clone());
            file.save(&path)?;
            if json {
                fmt::print_json(&json!({"current": name}));
            } else {
                fmt::emitln(&format!("Switched to context \"{name}\"."));
            }
        }
        ConfigCmd::GetContexts => {
            if json {
                let contexts: Vec<_> = file.contexts.keys().map(|n| context_json(n, &file)).collect();
                fmt::print_json(&json!({"current": file.current_context, "contexts": contexts}));
            } else if file.contexts.is_empty() {
                fmt::note("No contexts. Create one with: 21ci config set-context <name> --api <url> --token <token>");
            } else {
                let rows: Vec<Vec<String>> = file
                    .contexts
                    .iter()
                    .map(|(n, c)| {
                        vec![
                            if file.current_context.as_deref() == Some(n) {
                                "*".into()
                            } else {
                                String::new()
                            },
                            n.clone(),
                            c.api.clone().unwrap_or_else(|| "-".into()),
                            c.invoke.clone().unwrap_or_else(|| "-".into()),
                            c.tenant.clone().unwrap_or_else(|| "-".into()),
                            if c.token.is_some() { "yes".into() } else { "no".into() },
                        ]
                    })
                    .collect();
                fmt::emit(&fmt::table(
                    &["CURRENT", "NAME", "API", "INVOKE", "TENANT", "TOKEN"],
                    &rows,
                ));
            }
        }
        ConfigCmd::Current => {
            let env = |k: &str| std::env::var(k).ok();
            let settings = file.resolve(context_flag, as_tenant, &env)?;
            if json {
                fmt::print_json(&json!({
                    "context": settings.context,
                    "api": settings.api,
                    "invoke": settings.invoke,
                    "tenant": settings.tenant,
                    "token_set": settings.token.is_some(),
                }));
            } else {
                match settings.context {
                    Some(name) => fmt::emitln(&name),
                    None => {
                        return Err(CliError::usage(
                            "no current context; create one with: 21ci config set-context <name> --api <url>",
                        ))
                    }
                }
            }
        }
    }
    Ok(())
}

/// A context as JSON; the token itself is never printed.
fn context_json(name: &str, file: &ConfigFile) -> serde_json::Value {
    let c = file.contexts.get(name).cloned().unwrap_or_default();
    json!({"name": name, "api": c.api, "invoke": c.invoke, "tenant": c.tenant, "token": c.token.is_some()})
}
