//! What `push` uploads. Same rules as `sample/scripts/deploy.sh`:
//!
//! | input | Content-Type | runtime |
//! |---|---|---|
//! | `*.ts` | `application/typescript` | deno |
//! | `*.js`, `*.mjs` | `text/javascript` | deno |
//! | `*.wasm` | `application/wasm` | wasm |
//! | `*.zip`, `*.tar` | `application/zip`, `application/x-tar` | deno |
//! | dir with `rt.json` | `application/json` bundle `{entry, files, imports?}` | deno |
//! | dir without `rt.json` | one source: that file as-is; several: bundle with a guessed entry | deno |

use std::fs;
use std::path::Path;

use serde_json::{Map, Value};

use crate::error::{CliError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Runtime {
    Deno,
    Wasm,
}

impl Runtime {
    pub fn as_str(self) -> &'static str {
        match self {
            Runtime::Deno => "deno",
            Runtime::Wasm => "wasm",
        }
    }
}

#[derive(Debug)]
pub struct Artifact {
    pub content_type: &'static str,
    pub runtime: Runtime,
    pub bytes: Vec<u8>,
    /// Service slug derived from the file or directory name.
    pub default_slug: String,
}

const SOURCE_EXTS: [&str; 7] = ["ts", "tsx", "mts", "js", "jsx", "mjs", "json"];
const ENTRY_GUESSES: [&str; 5] = ["main.ts", "main.js", "index.ts", "index.js", "mod.ts"];

pub fn valid_slug(s: &str) -> bool {
    let b = s.as_bytes();
    (2..=63).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// Lower-case, `_`/`.` to `-`, other characters to `-`, collapse and trim dashes
/// (the same transformation as deploy.sh).
pub fn slugify(s: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for c in s.chars() {
        let c = c.to_ascii_lowercase();
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            if dash && !out.is_empty() {
                out.push('-');
            }
            dash = false;
            out.push(c);
        } else {
            dash = true;
        }
    }
    out
}

fn single_file_kind(name: &str) -> Option<(&'static str, Runtime)> {
    let ext = name.rsplit_once('.')?.1;
    Some(match ext {
        "ts" => ("application/typescript", Runtime::Deno),
        "js" | "mjs" => ("text/javascript", Runtime::Deno),
        "wasm" => ("application/wasm", Runtime::Wasm),
        "zip" => ("application/zip", Runtime::Deno),
        "tar" => ("application/x-tar", Runtime::Deno),
        _ => return None,
    })
}

pub fn build(path: &Path) -> Result<Artifact> {
    let meta = fs::metadata(path).map_err(|e| CliError::usage(format!("{}: {e}", path.display())))?;
    if meta.is_dir() {
        build_dir(path)
    } else {
        build_file(path)
    }
}

fn build_file(path: &Path) -> Result<Artifact> {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let (content_type, runtime) = single_file_kind(&name).ok_or_else(|| {
        CliError::usage(format!(
            "unsupported file type: {} (use .ts, .js, .mjs, .wasm, .zip, .tar or a directory)",
            path.display()
        ))
    })?;
    let bytes = fs::read(path).map_err(|e| CliError::usage(format!("{}: {e}", path.display())))?;
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    Ok(Artifact {
        content_type,
        runtime,
        bytes,
        default_slug: slugify(&stem),
    })
}

fn collect(dir: &Path, rel: &str, out: &mut Vec<String>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let kind = entry.file_type()?;
        let rel_path = if rel.is_empty() {
            name.clone()
        } else {
            format!("{rel}/{name}")
        };
        if kind.is_symlink() {
            continue;
        }
        if kind.is_dir() {
            if name != "node_modules" {
                collect(&entry.path(), &rel_path, out)?;
            }
        } else if kind.is_file()
            && name != "rt.json"
            && name.rsplit_once('.').is_some_and(|(_, ext)| SOURCE_EXTS.contains(&ext))
        {
            out.push(rel_path);
        }
    }
    Ok(())
}

fn build_dir(dir: &Path) -> Result<Artifact> {
    let canon = fs::canonicalize(dir).map_err(|e| CliError::usage(format!("{}: {e}", dir.display())))?;
    let default_slug = slugify(
        &canon
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
    );

    // rt.json: {entry?, imports?}
    let rt_path = dir.join("rt.json");
    let (mut entry, mut imports, has_rt) = (None::<String>, Value::Null, rt_path.is_file());
    if has_rt {
        let text = fs::read_to_string(&rt_path).map_err(|e| CliError::usage(format!("{}: {e}", rt_path.display())))?;
        let v: Value = serde_json::from_str(&text)
            .map_err(|e| CliError::usage(format!("{} is not valid JSON: {e}", rt_path.display())))?;
        let obj = v
            .as_object()
            .ok_or_else(|| CliError::usage(format!("{} is not a JSON object", rt_path.display())))?;
        entry = obj
            .get("entry")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        imports = obj.get("imports").cloned().unwrap_or(Value::Null);
    }

    let mut files = Vec::new();
    collect(dir, "", &mut files).map_err(|e| CliError::usage(format!("{}: {e}", dir.display())))?;
    files.sort();
    if files.is_empty() {
        return Err(CliError::usage(format!("no source files found in {}", dir.display())));
    }

    // no rt.json and a lone source file: a plain single-file upload
    if !has_rt && files.len() == 1 {
        let only = dir.join(&files[0]);
        if let Some((content_type, runtime)) = single_file_kind(&files[0]) {
            let bytes = fs::read(&only).map_err(|e| CliError::usage(format!("{}: {e}", only.display())))?;
            return Ok(Artifact {
                content_type,
                runtime,
                bytes,
                default_slug,
            });
        }
    }

    let entry = match entry {
        Some(e) => e,
        None => ENTRY_GUESSES
            .iter()
            .find(|g| files.iter().any(|f| f == *g))
            .map(|g| g.to_string())
            .ok_or_else(|| {
                CliError::usage(format!(
                    "cannot guess the entry point in {}; add rt.json with {{\"entry\": \"...\"}}",
                    dir.display()
                ))
            })?,
    };
    if !dir.join(&entry).is_file() {
        return Err(CliError::usage(format!(
            "entry '{entry}' not found in {}",
            dir.display()
        )));
    }

    let mut map = Map::new();
    for rel in &files {
        let p = dir.join(rel);
        let text = fs::read_to_string(&p)
            .map_err(|e| CliError::usage(format!("{}: {e} (sources must be UTF-8 text)", p.display())))?;
        map.insert(rel.clone(), Value::String(text));
    }
    let mut bundle = Map::new();
    bundle.insert("entry".into(), Value::String(entry));
    bundle.insert("files".into(), Value::Object(map));
    if !imports.is_null() {
        bundle.insert("imports".into(), imports);
    }
    let bytes = serde_json::to_vec(&Value::Object(bundle))
        .map_err(|e| CliError::failed(format!("cannot encode the bundle: {e}")))?;
    Ok(Artifact {
        content_type: "application/json",
        runtime: Runtime::Deno,
        bytes,
        default_slug,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_rules() {
        assert!(valid_slug("hello"));
        assert!(valid_slug("a1"));
        assert!(valid_slug("my-fn-2"));
        assert!(!valid_slug("a"));
        assert!(!valid_slug("1abc"));
        assert!(!valid_slug("Hello"));
        assert!(!valid_slug("under_score"));
        assert!(!valid_slug(&"a".repeat(64)));
        assert!(valid_slug(&"a".repeat(63)));
    }

    #[test]
    fn slugify_matches_deploy_sh() {
        assert_eq!(slugify("hello"), "hello");
        assert_eq!(slugify("My_Cool App"), "my-cool-app");
        assert_eq!(slugify("a.b_c"), "a-b-c");
        assert_eq!(slugify("--x--y--"), "x-y");
        assert_eq!(slugify("sample-hello.v2"), "sample-hello-v2");
        assert_eq!(slugify("Ünï"), "n");
    }

    #[test]
    fn file_kinds() {
        assert_eq!(single_file_kind("a.ts").unwrap().0, "application/typescript");
        assert_eq!(single_file_kind("a.mjs").unwrap().0, "text/javascript");
        assert_eq!(single_file_kind("a.wasm").unwrap(), ("application/wasm", Runtime::Wasm));
        assert_eq!(single_file_kind("a.zip").unwrap().0, "application/zip");
        assert!(single_file_kind("a.txt").is_none());
        assert!(single_file_kind("Makefile").is_none());
    }
}
