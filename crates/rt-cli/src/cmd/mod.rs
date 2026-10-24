//! One module per command group. Shared plumbing lives here: the run context, service
//! reference resolution (`<slug>` or `svc_…`), pagination.

use std::io::{IsTerminal, Write};
use std::path::Path;

use serde_json::{json, Value};

use crate::cli::ListArgs;
use crate::client::Client;
use crate::config::Settings;
use crate::error::{CliError, Result};
use crate::fmt;

pub mod config;
pub mod data;
pub mod doctor;
pub mod env;
pub mod events;
pub mod host;
pub mod init;
pub mod invoke;
pub mod key;
pub mod limits;
pub mod logs;
pub mod mcp;
pub mod misc;
pub mod node;
pub mod plan;
pub mod project;
pub mod push;
pub mod queue;
pub mod schedule;
pub mod sdk;
pub mod secret;
pub mod service;
pub mod sockets;
pub mod stats;
pub mod tenant;
pub mod token;
pub mod versions;

/// What every networked command needs.
pub struct App {
    pub settings: Settings,
    /// `--output json` / `--json`
    pub json: bool,
    /// `--retry N` / `RT_RETRY`
    pub retry: u32,
}

impl App {
    pub fn client(&self) -> Result<Client> {
        Client::new(&self.settings, self.retry)
    }
}

/// A downloaded file: written to `--out`, or to stdout when it is not a terminal (or the bytes
/// are text). `what` names it in messages.
pub fn save_download(app: &App, bytes: &[u8], out: Option<&Path>, what: &str) -> Result<()> {
    match out {
        Some(path) => {
            std::fs::write(path, bytes)
                .map_err(|e| CliError::failed(format!("cannot write {}: {e}", path.display())))?;
            if app.json {
                fmt::print_json(&json!({"path": path.display().to_string(), "bytes": bytes.len()}));
            } else {
                fmt::emitln(&format!("Wrote {what} to {} ({} bytes).", path.display(), bytes.len()));
            }
            Ok(())
        }
        None => {
            if std::io::stdout().is_terminal() && std::str::from_utf8(bytes).is_err() {
                return Err(CliError::usage(format!(
                    "{what} is binary: pass --out <file> or redirect stdout"
                )));
            }
            let mut o = std::io::stdout().lock();
            if o.write_all(bytes).and_then(|()| o.flush()).is_err() {
                std::process::exit(0);
            }
            Ok(())
        }
    }
}

/// A service named on the command line, resolved to its id.
pub struct Svc {
    pub id: String,
    /// `hello (svc_1)` when given a slug, `svc_1` when given an id: used in messages.
    pub label: String,
    /// Known when the service was given as a slug.
    pub slug: Option<String>,
    /// The service view, when the lookup already produced it.
    pub view: Option<Value>,
    /// Value for `If-Match`: the `ETag` header when we fetched by id, else the `etag` field.
    pub etag: Option<String>,
}

impl Svc {
    /// `/v1/services/{id}{suffix}`
    pub fn path(&self, suffix: &str) -> String {
        format!("/v1/services/{}{suffix}", self.id)
    }

    /// The service view; fetches it when only the id is known.
    pub fn view(&mut self, c: &Client) -> Result<&Value> {
        if self.view.is_none() {
            let r = c.send_json(c.req(reqwest::Method::GET, &self.path("")))?;
            self.etag = r
                .headers
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .or_else(|| r.body.get("etag").and_then(Value::as_str).map(str::to_string));
            if let Some(s) = r.body.get("slug").and_then(Value::as_str) {
                self.slug = Some(s.to_string());
            }
            self.view = Some(r.body);
        }
        Ok(self.view.as_ref().expect("set above"))
    }
}

/// `GET /v1/services?slug=<slug>` (contracts C-9): the service view, if there is one.
pub fn lookup_slug(c: &Client, slug: &str) -> Result<Option<Value>> {
    let page = c.get("/v1/services", &[("slug", slug.to_string())])?;
    Ok(page
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| {
            items
                .iter()
                .find(|s| s.get("slug").and_then(Value::as_str) == Some(slug))
        })
        .cloned())
}

/// Resolve `<slug>` (one lookup) or `svc_…` (taken as is).
pub fn resolve(c: &Client, r: &str) -> Result<Svc> {
    if r.starts_with("svc_") {
        return Ok(Svc {
            id: r.to_string(),
            label: r.to_string(),
            slug: None,
            view: None,
            etag: None,
        });
    }
    if !crate::artifact::valid_slug(r) {
        return Err(CliError::usage(format!(
            "invalid service \"{r}\": expected a slug (^[a-z][a-z0-9-]{{1,62}}$) or a svc_… id"
        )));
    }
    match lookup_slug(c, r)? {
        Some(v) => {
            let id = v
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| CliError::failed("the server returned a service without an id"))?
                .to_string();
            let etag = v.get("etag").and_then(Value::as_str).map(str::to_string);
            Ok(Svc {
                label: format!("{r} ({id})"),
                id,
                slug: Some(r.to_string()),
                view: Some(v),
                etag,
            })
        }
        None => Err(CliError::not_found(format!("service \"{r}\" not found"))),
    }
}

pub struct Page {
    pub items: Vec<Value>,
    pub next_cursor: Option<String>,
    /// The body to print for `--output json`.
    pub raw: Value,
}

/// `GET path?limit&cursor` once, or every page with `--all`.
pub fn fetch_page(c: &Client, path: &str, p: &ListArgs) -> Result<Page> {
    fetch_page_with(c, path, p, &[])
}

/// [`fetch_page`] with more query parameters (filters) on every request.
pub fn fetch_page_with(c: &Client, path: &str, p: &ListArgs, filter: &[(&str, String)]) -> Result<Page> {
    let mut query: Vec<(&str, String)> = filter.to_vec();
    if let Some(l) = p.limit {
        query.push(("limit", l.to_string()));
    }
    let mut cursor = p.cursor.clone();
    let mut all_items: Vec<Value> = Vec::new();
    loop {
        let mut q = query.clone();
        if let Some(cur) = &cursor {
            q.push(("cursor", cur.clone()));
        }
        let body = c.get(path, &q)?;
        let items = body.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
        let next = body.get("next_cursor").and_then(Value::as_str).map(str::to_string);
        if !p.all {
            return Ok(Page {
                items,
                next_cursor: next,
                raw: body,
            });
        }
        all_items.extend(items);
        match next {
            Some(n) => cursor = Some(n),
            None => {
                let raw = json!({"items": all_items.clone(), "next_cursor": null});
                return Ok(Page {
                    items: all_items,
                    next_cursor: None,
                    raw,
                });
            }
        }
    }
}

/// Table of a page (or the "nothing" notice), the JSON of it, and the "more results" hint.
pub fn print_page(app: &App, page: &Page, what: &str, headers: &[&str], row: impl Fn(&Value) -> Vec<String>) {
    if app.json {
        fmt::print_json(&page.raw);
        return;
    }
    if page.items.is_empty() {
        fmt::note(&format!("No {what} found."));
    } else {
        let rows: Vec<Vec<String>> = page.items.iter().map(row).collect();
        fmt::emit(&fmt::table(headers, &rows));
    }
    if let Some(n) = &page.next_cursor {
        fmt::note(&format!("(more results: --cursor {n}, or --all)"));
    }
}

pub fn parse_json_arg(flag: &str, text: &str) -> Result<Value> {
    serde_json::from_str(text).map_err(|e| CliError::usage(format!("{flag} must be valid JSON: {e}")))
}

/// `KEY=VALUE` (the value may contain `=`).
pub fn split_pair(s: &str) -> Result<(&str, &str)> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k, v)),
        _ => Err(CliError::usage(format!("expected KEY=VALUE, got `{s}`"))),
    }
}

/// A JSON field as a table cell: strings as is, numbers and booleans as text, `-` for null or absent.
pub fn cell(v: &Value, key: &str) -> String {
    fmt::str_field(v, key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs() {
        assert_eq!(split_pair("A=1").unwrap(), ("A", "1"));
        assert_eq!(split_pair("A=b=c").unwrap(), ("A", "b=c"));
        assert_eq!(split_pair("A=").unwrap(), ("A", ""));
        assert!(split_pair("A").is_err());
        assert!(split_pair("=x").is_err());
    }
}
