//! `21ci branch`: branch services, and the lookup behind `push --branch`.

use std::time::SystemTime;

use serde_json::{json, Value};

use super::{cell, fetch_page, lookup_slug, print_page, resolve, App};
use crate::cli::BranchCmd;
use crate::client::Client;
use crate::error::{CliError, Result};
use crate::fmt;

/// `^[a-z][a-z0-9-]{0,23}$` without `--`. Also keeps a name safe in a request path.
fn valid_name(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && s.len() <= 24
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !s.contains("--")
}

pub fn check_name(s: &str) -> Result<()> {
    if valid_name(s) {
        return Ok(());
    }
    Err(CliError::usage(format!(
        "invalid branch name \"{s}\": must match ^[a-z][a-z0-9-]{{0,23}}$, without `--`"
    )))
}

/// The service of branch `name` of the service with slug `parent`. Only a real branch of that
/// parent: a service merely named `<parent>-br-<name>` must not receive the upload.
pub fn resolve_branch(c: &Client, parent: &str, name: &str) -> Result<(String, String)> {
    check_name(name)?;
    let slug = format!("{parent}-br-{name}");
    if !crate::artifact::valid_slug(&slug) {
        return Err(CliError::usage(format!(
            "the branch's slug \"{slug}\" is longer than 63 characters"
        )));
    }
    let parent_view =
        lookup_slug(c, parent)?.ok_or_else(|| CliError::not_found(format!("service \"{parent}\" not found")))?;
    let view = lookup_slug(c, &slug)?.ok_or_else(|| {
        CliError::not_found(format!(
            "branch \"{name}\" of \"{parent}\" not found (create it with: 21ci branch create {parent} --name {name})"
        ))
    })?;
    let scratch = view.get("scratch").unwrap_or(&Value::Null);
    let ours = scratch.get("kind").and_then(Value::as_str) == Some("branch")
        && scratch.get("of").is_some_and(|of| Some(of) == parent_view.get("id"));
    if !ours {
        return Err(CliError::failed(format!(
            "service \"{slug}\" is not a branch of \"{parent}\""
        )));
    }
    let id = view
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::failed("the server returned a service without an id"))?;
    Ok((id.to_string(), slug))
}

pub fn run(app: &App, cmd: &BranchCmd) -> Result<()> {
    match cmd {
        BranchCmd::Create {
            svc,
            name,
            copy_data,
            code_of_parent,
            schedules,
            events_of_parent,
            environment,
            ttl,
            env_names,
            no_egress,
        } => {
            check_name(name)?;
            let expires_at = ttl.map(|d| fmt::epoch_ms(SystemTime::now() + d));
            let mut body = json!({"name": name});
            if *copy_data {
                body["copy_data"] = json!(true);
            }
            if *code_of_parent {
                body["code_of_parent"] = json!(true);
            }
            if *schedules {
                body["schedules"] = json!(true);
            }
            if *events_of_parent {
                body["events_of_parent"] = json!(true);
            }
            if let Some(e) = environment {
                body["environment"] = json!(e);
            }
            if let Some(at) = expires_at {
                body["expires_at"] = json!(at);
            }
            if *env_names {
                body["env_names_from_parent"] = json!(true);
            }
            if *no_egress {
                body["egress_of_parent"] = json!(false);
            }
            let c = app.client()?;
            let parent = resolve(&c, svc)?;
            let view = c.post(&parent.path("/branches"), &body)?;
            if app.json {
                fmt::print_json(&view);
                return Ok(());
            }
            let slug = cell(&view, "slug");
            fmt::emitln(&format!(
                "Branch {name} of {} created: {slug} ({}).",
                parent.label,
                cell(&view, "id")
            ));
            fmt::emitln(&format!("Invoke: {}/s/{slug}/", app.settings.invoke));
            let parent_ref = parent.slug.as_deref().unwrap_or(svc);
            fmt::emitln(&format!(
                "Next:   21ci push --service {parent_ref} --branch {name} <path>"
            ));
        }
        BranchCmd::List { svc, page } => {
            let c = app.client()?;
            let parent = resolve(&c, svc)?;
            let p = fetch_page(&c, &parent.path("/branches"), page)?;
            print_page(
                app,
                &p,
                "branches",
                &["NAME", "SLUG", "ID", "LIVE", "ENV", "EXPIRES", "DATA"],
                |v| {
                    let scratch = v.get("scratch").unwrap_or(&Value::Null);
                    let copied = scratch.get("copied_data").and_then(Value::as_bool) == Some(true);
                    vec![
                        cell(scratch, "name"),
                        cell(v, "slug"),
                        cell(v, "id"),
                        cell(v, "live_version"),
                        cell(v, "environment"),
                        fmt::ts_field(scratch, "expires_at"),
                        if copied { "copied" } else { "empty" }.to_string(),
                    ]
                },
            );
        }
        BranchCmd::Delete { svc, name } => {
            check_name(name)?;
            let c = app.client()?;
            let parent = resolve(&c, svc)?;
            c.delete(&parent.path(&format!("/branches/{name}")), &[])?;
            if app.json {
                fmt::print_json(&json!({"service": parent.id, "deleted": name}));
            } else {
                fmt::emitln(&format!("Branch {name} of {} deleted, with its data.", parent.label));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_names() {
        for good in ["a", "one", "a-1", &"a".repeat(24)] {
            assert!(valid_name(good), "{good}");
        }
        for bad in ["", "1a", "A", "a--b", "a/b", "a.b", &"a".repeat(25)] {
            assert!(!valid_name(bad), "{bad}");
        }
    }
}
