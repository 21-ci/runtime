//! Config loading: TOML text → table, strict unknown-key check (with key paths), `RT__A__B`
//! environment overlay, typed deserialization, and mapping of serde errors back to key paths.
//!
//! Everything works on a `toml::Table` first so errors can name the exact dotted key
//! (`unknown key sched.procesors`, `invalid value for sched.processors: …`).

use std::collections::BTreeMap;
use std::sync::OnceLock;

use toml::{Table, Value};

use crate::sections::BindingConfig;
use crate::{Config, ConfigError, ENV_PREFIX};

/// Expected structure of the config tree, derived from the serialized defaults.
enum Shape {
    Table(BTreeMap<String, Shape>),
    Leaf,
    /// `[plans.<name>]`: arbitrary names, each with the inner shape.
    Named(Box<Shape>),
    /// `[[binding]]`: array of tables with the inner shape.
    Rows(Box<Shape>),
    /// Free-form map (`binding.inject_headers`).
    Open,
}

fn shape_of(v: &serde_json::Value) -> Shape {
    match v {
        serde_json::Value::Object(m) if m.is_empty() => Shape::Open,
        serde_json::Value::Object(m) => Shape::Table(m.iter().map(|(k, v)| (k.clone(), shape_of(v))).collect()),
        _ => Shape::Leaf,
    }
}

fn json_of<T: serde::Serialize>(v: &T) -> serde_json::Value {
    serde_json::to_value(v).expect("config structs always serialize to JSON")
}

fn root_shape() -> &'static Shape {
    static SHAPE: OnceLock<Shape> = OnceLock::new();
    SHAPE.get_or_init(|| {
        let mut root = shape_of(&json_of(&Config::default()));
        if let Shape::Table(children) = &mut root {
            children.insert(
                "plans".into(),
                Shape::Named(Box::new(shape_of(&json_of(&rt_types::Plan::default())))),
            );
            children.insert(
                "binding".into(),
                Shape::Rows(Box::new(shape_of(&json_of(&BindingConfig::default())))),
            );
        }
        root
    })
}

fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

/// Sections that were removed, refused with what replaces them.
fn check_removed(t: &Table) -> Result<(), ConfigError> {
    if t.contains_key("sdk") {
        return Err(ConfigError::Removed {
            key: "[[sdk]]".into(),
            hint: "operator SDKs are no longer bundled into tenant services or preloaded into the base snapshot \
                   (`preload = true` never reached the guest at run time). Publish the SDK as a runtime service \
                   with `21ci sdk publish` and grant it with `21ci sdk grants set`",
        });
    }
    Ok(())
}

/// First unknown key in `v` (document order is alphabetical), as a dotted path.
fn check_keys(shape: &Shape, v: &Value, path: &str) -> Result<(), ConfigError> {
    match (shape, v) {
        (Shape::Table(children), Value::Table(t)) => {
            for (k, x) in t {
                let p = join(path, k);
                match children.get(k) {
                    Some(s) => check_keys(s, x, &p)?,
                    None => return Err(ConfigError::UnknownKey(p)),
                }
            }
        }
        (Shape::Named(inner), Value::Table(t)) => {
            for (k, x) in t {
                check_keys(inner, x, &join(path, k))?;
            }
        }
        (Shape::Rows(inner), Value::Array(rows)) => {
            for (i, row) in rows.iter().enumerate() {
                check_keys(inner, row, &format!("{path}[{i}]"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Check that an env override path names a real key (reporting the full dotted path).
fn check_env_path(segs: &[String], var: &str) -> Result<(), ConfigError> {
    let mut cur = root_shape();
    for seg in segs {
        cur = match cur {
            Shape::Table(c) => match c.get(seg) {
                Some(s) => s,
                None => return Err(ConfigError::UnknownKey(segs.join("."))),
            },
            Shape::Named(inner) => inner,
            Shape::Open => return Ok(()),
            Shape::Rows(_) => return Err(rows_error(var)),
            Shape::Leaf => return Err(ConfigError::UnknownKey(segs.join("."))),
        };
    }
    if matches!(cur, Shape::Rows(_)) {
        return Err(rows_error(var));
    }
    Ok(())
}

fn rows_error(var: &str) -> ConfigError {
    ConfigError::Env {
        var: var.to_string(),
        reason: "array tables ([[binding]]) cannot be set from the environment".into(),
    }
}

#[derive(Clone)]
enum Seg {
    Key(String),
    Idx(usize),
}

fn path_string(path: &[Seg]) -> String {
    let mut out = String::new();
    for seg in path {
        match seg {
            Seg::Key(k) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(k);
            }
            Seg::Idx(i) => out.push_str(&format!("[{i}]")),
        }
    }
    out
}

/// Nest `leaf` under `path` (`Idx` wraps in a one-element array).
fn wrap(path: &[Seg], leaf: Value) -> Value {
    path.iter().rev().fold(leaf, |acc, seg| match seg {
        Seg::Key(k) => {
            let mut t = Table::new();
            t.insert(k.clone(), acc);
            Value::Table(t)
        }
        Seg::Idx(_) => Value::Array(vec![acc]),
    })
}

/// Does a document consisting of just `leaf` at `path` deserialize into a `Config`?
fn leaf_check(path: &[Seg], leaf: &Value) -> Result<(), String> {
    wrap(path, leaf.clone())
        .try_into::<Config>()
        .map(|_| ())
        .map_err(|e| e.message().to_string())
}

fn collect_leaves(v: &Value, path: &mut Vec<Seg>, out: &mut Vec<(Vec<Seg>, Value)>) {
    match v {
        Value::Table(t) => {
            for (k, x) in t {
                path.push(Seg::Key(k.clone()));
                collect_leaves(x, path, out);
                path.pop();
            }
        }
        Value::Array(a) if !a.is_empty() && a.iter().all(Value::is_table) => {
            for (i, x) in a.iter().enumerate() {
                path.push(Seg::Idx(i));
                collect_leaves(x, path, out);
                path.pop();
            }
        }
        _ => out.push((path.clone(), v.clone())),
    }
}

/// Deserialization of the whole table failed: find the offending key by testing every leaf alone.
fn locate_invalid(table: &Table, whole: &toml::de::Error) -> ConfigError {
    let mut leaves = Vec::new();
    collect_leaves(&Value::Table(table.clone()), &mut Vec::new(), &mut leaves);
    for (path, leaf) in &leaves {
        if let Err(reason) = leaf_check(path, leaf) {
            return ConfigError::InvalidValue {
                key: path_string(path),
                reason,
            };
        }
    }
    ConfigError::Parse(whole.to_string())
}

/// Candidate TOML interpretations of an env string, most specific first.
fn env_candidates(raw: &str) -> Vec<Value> {
    let t = raw.trim();
    let mut out = Vec::new();
    if let Ok(i) = t.parse::<i64>() {
        out.push(Value::Integer(i));
    }
    if let Ok(f) = t.parse::<f64>() {
        if f.is_finite() {
            out.push(Value::Float(f));
        }
    }
    match t {
        "true" => out.push(Value::Boolean(true)),
        "false" => out.push(Value::Boolean(false)),
        _ => {}
    }
    out.push(Value::String(raw.to_string()));
    if t.starts_with('[') {
        if let Ok(tbl) = toml::from_str::<Table>(&format!("v = {t}")) {
            if let Some(v) = tbl.get("v") {
                out.push(v.clone());
            }
        }
    }
    let parts: Vec<&str> = t.split(',').map(str::trim).filter(|p| !p.is_empty()).collect();
    if !parts.is_empty() {
        let ints: Result<Vec<i64>, _> = parts.iter().map(|p| p.parse::<i64>()).collect();
        if let Ok(ints) = ints {
            out.push(Value::Array(ints.into_iter().map(Value::Integer).collect()));
        }
        out.push(Value::Array(
            parts.iter().map(|p| Value::String((*p).to_string())).collect(),
        ));
    }
    out
}

/// One `RT__A__B=value` override.
struct EnvOverride {
    var: String,
    segs: Vec<String>,
    raw: String,
}

fn env_overrides(env: &[(String, String)]) -> Result<Vec<EnvOverride>, ConfigError> {
    let mut out: Vec<EnvOverride> = Vec::new();
    for (name, value) in env {
        let Some(rest) = name.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        let segs: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
        if segs.iter().any(String::is_empty) {
            return Err(ConfigError::Env {
                var: name.clone(),
                reason: "empty key segment (expected RT__SECTION__KEY)".into(),
            });
        }
        // a later duplicate of the same key wins
        out.retain(|o| o.segs != segs);
        out.push(EnvOverride {
            var: name.clone(),
            segs,
            raw: value.clone(),
        });
    }
    Ok(out)
}

fn set_path(root: &mut Table, segs: &[String], v: Value, var: &str) -> Result<(), ConfigError> {
    let Some((last, parents)) = segs.split_last() else {
        return Err(ConfigError::Env {
            var: var.to_string(),
            reason: "empty key".into(),
        });
    };
    let mut cur = root;
    for s in parents {
        let entry = cur.entry(s.clone()).or_insert_with(|| Value::Table(Table::new()));
        match entry {
            Value::Table(t) => cur = t,
            _ => {
                return Err(ConfigError::Env {
                    var: var.to_string(),
                    reason: format!("`{s}` is not a table in the config file"),
                });
            }
        }
    }
    cur.insert(last.clone(), v);
    Ok(())
}

fn apply_env(table: &mut Table, env: &[(String, String)]) -> Result<Vec<String>, ConfigError> {
    let mut keys = Vec::new();
    for o in env_overrides(env)? {
        check_env_path(&o.segs, &o.var)?;
        let path: Vec<Seg> = o.segs.iter().cloned().map(Seg::Key).collect();
        let key = o.segs.join(".");
        let mut chosen = None;
        let mut reason = String::new();
        for cand in env_candidates(&o.raw) {
            match leaf_check(&path, &cand) {
                Ok(()) => {
                    chosen = Some(cand);
                    break;
                }
                // the plain-string attempt gives the most readable message
                Err(e) if matches!(cand, Value::String(_)) => reason = e,
                Err(e) if reason.is_empty() => reason = e,
                Err(_) => {}
            }
        }
        let Some(value) = chosen else {
            return Err(ConfigError::InvalidValue {
                key,
                reason: format!("{} (from {})", reason, o.var),
            });
        };
        set_path(table, &o.segs, value, &o.var)?;
        keys.push(key);
    }
    Ok(keys)
}

/// Parse `text`, overlay `env`, check keys/values/validation. Returns the config and the dotted
/// keys that came from the environment.
pub(crate) fn build(text: &str, env: &[(String, String)]) -> Result<(Config, Vec<String>), ConfigError> {
    let mut table: Table = toml::from_str(text).map_err(|e| ConfigError::Parse(e.to_string()))?;
    check_removed(&table)?;
    check_keys(root_shape(), &Value::Table(table.clone()), "")?;
    let env_keys = apply_env(&mut table, env)?;
    let mut cfg: Config = match Value::Table(table.clone()).try_into::<Config>() {
        Ok(c) => c,
        Err(e) => return Err(locate_invalid(&table, &e)),
    };
    // a `free` plan always exists (SECURITY.md)
    cfg.plans.entry("free".to_string()).or_default();
    cfg.validate()?;
    Ok((cfg, env_keys))
}
