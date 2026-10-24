//! `21ci env get | set | unset`. The API replaces the whole map on `PUT`, so `set` and `unset`
//! read the current map, change it and write it back.

use std::collections::BTreeMap;

use serde_json::Value;

use super::{resolve, split_pair, App};
use crate::cli::EnvCmd;
use crate::client::Client;
use crate::error::Result;
use crate::fmt;

fn to_map(v: &Value) -> BTreeMap<String, String> {
    v.as_object()
        .map(|o| {
            o.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn put(c: &Client, path: &str, map: &BTreeMap<String, String>) -> Result<Value> {
    c.put(path, &serde_json::to_value(map).expect("a string map is JSON"))
}

pub fn run(app: &App, cmd: &EnvCmd) -> Result<()> {
    match cmd {
        EnvCmd::Get(a) => {
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            let body = c.get(&svc.path("/env"), &[])?;
            if app.json {
                fmt::print_json(&body);
                return Ok(());
            }
            let map = to_map(&body);
            if map.is_empty() {
                fmt::note("No variables set.");
            } else {
                let rows: Vec<Vec<String>> = map.into_iter().map(|(k, v)| vec![k, v]).collect();
                fmt::emit(&fmt::table(&["KEY", "VALUE"], &rows));
            }
        }
        EnvCmd::Set { svc, pairs } => {
            let parsed = pairs.iter().map(|p| split_pair(p)).collect::<Result<Vec<_>>>()?;
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            let path = svc.path("/env");
            let mut map = to_map(&c.get(&path, &[])?);
            for (k, v) in &parsed {
                map.insert((*k).to_string(), (*v).to_string());
            }
            let saved = put(&c, &path, &map)?;
            if app.json {
                fmt::print_json(&saved);
            } else {
                fmt::emitln(&format!(
                    "Set {} on {}.",
                    fmt::plural(parsed.len(), "variable", "variables"),
                    svc.label
                ));
            }
        }
        EnvCmd::Unset { svc, keys } => {
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            let path = svc.path("/env");
            let mut map = to_map(&c.get(&path, &[])?);
            for k in keys {
                map.remove(k);
            }
            let saved = put(&c, &path, &map)?;
            if app.json {
                fmt::print_json(&saved);
            } else {
                fmt::emitln(&format!(
                    "Unset {} on {}.",
                    fmt::plural(keys.len(), "variable", "variables"),
                    svc.label
                ));
            }
        }
    }
    Ok(())
}
