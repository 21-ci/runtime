//! `21ci project create | list | get | update | delete` (decision D-B3).
//!
//! A project groups the services of a tenant and lists the environments they may be in. `--project`
//! of `21ci service create` / `list` names one.

use serde_json::{json, Value};

use super::{cell, fetch_page, print_page, App};
use crate::artifact::valid_slug;
use crate::cli::ProjectCmd;
use crate::error::{CliError, Result};
use crate::fmt;

/// `^[a-z][a-z0-9-]{0,31}$`, as the server checks an environment label.
pub fn valid_environment(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 32
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// At least one, at most 20, each a valid label, none twice.
fn check_environments(envs: &[String]) -> Result<()> {
    if envs.is_empty() {
        return Err(CliError::usage("a project needs at least one environment"));
    }
    if envs.len() > 20 {
        return Err(CliError::usage("a project holds at most 20 environments"));
    }
    for (i, e) in envs.iter().enumerate() {
        if !valid_environment(e) {
            return Err(CliError::usage(format!(
                "invalid environment \"{e}\": must match ^[a-z][a-z0-9-]{{0,31}}$"
            )));
        }
        if envs[..i].contains(e) {
            return Err(CliError::usage(format!("environment \"{e}\" is listed twice")));
        }
    }
    Ok(())
}

fn check_name(name: &str) -> Result<()> {
    let n = name.trim().chars().count();
    if (1..=100).contains(&n) {
        Ok(())
    } else {
        Err(CliError::usage("--name must be 1 to 100 characters"))
    }
}

fn path(project: &str) -> String {
    format!("/v1/projects/{}", fmt::enc(project))
}

fn envs_of(p: &Value) -> Vec<String> {
    p.get("environments")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default()
}

fn show(app: &App, p: &Value) {
    if app.json {
        fmt::print_json(p);
        return;
    }
    fmt::emit(&fmt::kv(&[
        ("ID", cell(p, "id")),
        ("Slug", cell(p, "slug")),
        ("Name", cell(p, "name")),
        ("Environments", envs_of(p).join(", ")),
        ("Created", fmt::ts_field(p, "created_at")),
    ]));
}

pub fn run(app: &App, cmd: &ProjectCmd) -> Result<()> {
    match cmd {
        ProjectCmd::Create {
            slug,
            name,
            environments,
        } => {
            if !valid_slug(slug) {
                return Err(CliError::usage(format!(
                    "invalid slug \"{slug}\": must match ^[a-z][a-z0-9-]{{1,62}}$"
                )));
            }
            let mut body = json!({"slug": slug});
            if let Some(n) = name {
                check_name(n)?;
                body["name"] = json!(n);
            }
            if !environments.is_empty() {
                check_environments(environments)?;
                body["environments"] = json!(environments);
            }
            let p = app.client()?.post("/v1/projects", &body)?;
            show(app, &p);
        }
        ProjectCmd::List(l) => {
            let page = fetch_page(&app.client()?, "/v1/projects", l)?;
            print_page(app, &page, "projects", &["ID", "SLUG", "NAME", "ENVIRONMENTS"], |p| {
                vec![cell(p, "id"), cell(p, "slug"), cell(p, "name"), envs_of(p).join(",")]
            });
        }
        ProjectCmd::Get { project } => {
            let p = app.client()?.get(&path(project), &[])?;
            show(app, &p);
        }
        ProjectCmd::Update {
            project,
            name,
            environments,
            add_env,
            remove_env,
        } => {
            let mut body = json!({});
            if let Some(n) = name {
                check_name(n)?;
                body["name"] = json!(n);
            }
            for e in add_env.iter().chain(remove_env) {
                if !valid_environment(e) {
                    return Err(CliError::usage(format!(
                        "invalid environment \"{e}\": must match ^[a-z][a-z0-9-]{{0,31}}$"
                    )));
                }
            }
            if !environments.is_empty() {
                check_environments(environments)?;
                body["environments"] = json!(environments);
            }
            if body.as_object().is_some_and(|o| o.is_empty()) && add_env.is_empty() && remove_env.is_empty() {
                return Err(CliError::usage(
                    "nothing to change: give --name, --env, --add-env or --remove-env",
                ));
            }
            let c = app.client()?;
            if !add_env.is_empty() || !remove_env.is_empty() {
                let mut envs = envs_of(&c.get(&format!("{}/environments", path(project)), &[])?);
                for e in add_env {
                    if !envs.contains(e) {
                        envs.push(e.clone());
                    }
                }
                envs.retain(|e| !remove_env.contains(e));
                check_environments(&envs)?;
                body["environments"] = json!(envs);
            }
            let p = c.patch(&path(project), &body)?;
            show(app, &p);
        }
        ProjectCmd::Delete { project } => {
            app.client()?.delete(&path(project), &[])?;
            if app.json {
                fmt::print_json(&json!({"deleted": project}));
            } else {
                fmt::emitln(&format!("Project {project} deleted."));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_lists_follow_the_servers_rules() {
        let s = |v: &[&str]| v.iter().map(|e| (*e).to_string()).collect::<Vec<_>>();
        assert!(check_environments(&s(&["production", "pr-42"])).is_ok());
        assert!(check_environments(&s(&[])).is_err());
        assert!(check_environments(&s(&["Prod"])).is_err());
        assert!(check_environments(&s(&["a", "a"])).is_err());
        let many: Vec<String> = (0..21).map(|i| format!("e{i}")).collect();
        assert!(check_environments(&many).is_err());
        assert!(valid_environment("a") && !valid_environment("") && !valid_environment(&"a".repeat(33)));
    }
}
