//! Synchronous deploy-time validation: no network, no disk.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use deno_ast::MediaType;
use rt_types::{Diagnostic, Digest, HandlerForm, ValidatedUpload};
use url::Url;
use wasmparser::{Parser, Payload, Validator, WasmFeatures};

use crate::analyze::{self, DepRef};
use crate::builder::{SourceFile, Upload, UploadLimits, Validated, WasmInfo};
use crate::extract::{self, diag, VErr};
use crate::resolve::{builtin_alias, classify_specifier, renamed_to, SpecifierKind, BUILTIN_MODULES};
use crate::wasm_policy::{is_import_allowed, is_incoming_handler, renamed_import, REQUIRED_EXPORT};

/// Which source files are ES modules the build can transpile and bundle.
pub(crate) fn is_module_media(m: MediaType) -> bool {
    matches!(
        m,
        MediaType::JavaScript
            | MediaType::Jsx
            | MediaType::Mjs
            | MediaType::TypeScript
            | MediaType::Mts
            | MediaType::Tsx
    )
}

/// Does emitting this media type need the TS/JSX transpiler?
pub(crate) fn needs_transpile(m: MediaType) -> bool {
    matches!(
        m,
        MediaType::Jsx | MediaType::TypeScript | MediaType::Mts | MediaType::Tsx
    )
}

/// `file:///{path}` for a normalized archive path.
pub(crate) fn file_url(path: &str) -> Url {
    Url::from_file_path(Path::new("/").join(path)).unwrap_or_else(|()| Url::parse("file:///").expect("static url"))
}

/// Archive path of a `file:///…` URL.
pub(crate) fn url_path(u: &Url) -> Option<String> {
    let p = u.to_file_path().ok()?;
    Some(p.to_str()?.trim_start_matches('/').to_string())
}

/// `rt.json` import map lookup: exact key first, then the longest `prefix/` key.
pub(crate) fn map_specifier(map: &BTreeMap<String, String>, spec: &str) -> Option<String> {
    if let Some(t) = map.get(spec) {
        return Some(t.clone());
    }
    map.iter()
        .filter(|(k, _)| k.ends_with('/') && spec.starts_with(k.as_str()))
        .max_by_key(|(k, _)| k.len())
        .map(|(k, t)| format!("{t}{}", &spec[k.len()..]))
}

struct Checker<'a> {
    import_map: &'a BTreeMap<String, String>,
    sdks: &'a BTreeSet<String>,
}

impl Checker<'_> {
    /// `Some(message)` when the import is not allowed.
    fn check(&self, from: &str, spec: &str) -> Option<String> {
        let kind = classify_specifier(spec);
        if kind == SpecifierKind::Relative {
            return self.check_relative(Some(from), spec);
        }
        match map_specifier(self.import_map, spec) {
            Some(target) => {
                let tk = classify_specifier(&target);
                if tk == SpecifierKind::Relative {
                    return self
                        .check_relative(None, &target)
                        .map(|m| format!("import `{spec}` (mapped to `{target}`): {m}"));
                }
                self.check_kind(tk, &target)
                    .map(|m| format!("import `{spec}` (mapped to `{target}`): {m}"))
            }
            None => self.check_kind(kind, spec),
        }
    }

    fn check_kind(&self, kind: SpecifierKind, spec: &str) -> Option<String> {
        match kind {
            SpecifierKind::Relative => None, // LCOV_EXCL_LINE: `check` routes relative specifiers to `check_relative` first
            SpecifierKind::Builtin => {
                if let Some(to) = renamed_to(spec) {
                    Some(format!("`{spec}` was renamed to `{to}`"))
                } else if BUILTIN_MODULES.contains(&builtin_alias(spec).unwrap_or(spec)) {
                    None
                } else {
                    Some(format!("unknown runtime module `{spec}`"))
                }
            }
            SpecifierKind::Sdk => {
                if self.sdks.contains(spec) {
                    None
                } else {
                    Some(format!("unknown sdk `{spec}`"))
                }
            }
            SpecifierKind::Npm | SpecifierKind::Jsr | SpecifierKind::Remote => None,
            SpecifierKind::File => Some(format!("`file:` imports are not allowed: `{spec}`")),
            SpecifierKind::Node => Some(format!("`node:` imports are not allowed: `{spec}`")),
            SpecifierKind::Bare => Some(format!("bare specifier `{spec}` needs an `imports` entry in rt.json")),
        }
    }

    fn check_relative(&self, from: Option<&str>, spec: &str) -> Option<String> {
        let base = match from {
            Some(f) => file_url(f),
            None => Url::parse("file:///").ok()?,
        };
        let target = match base.join(spec) {
            Ok(u) if u.scheme() == "file" && u.query().is_none() && u.fragment().is_none() => u,
            _ => return Some(format!("invalid relative import `{spec}`")),
        };
        // Whether the target file exists is a *build* failure (`failed(resolve)`, graph stage), not
        // a deploy-time one: the synchronous checks have no relative-import check
        // and E2E-05 deploys an import of a missing module lazily (201), then fails on first use.
        url_path(&target)
            .is_none()
            .then(|| format!("invalid relative import `{spec}`"))
    }
}

pub(crate) fn deno(limits: &UploadLimits, sdks: &BTreeSet<String>, upload: &Upload) -> Result<Validated, VErr> {
    let ex = extract::extract(Some(&upload.content_type), &upload.body, limits)?;
    let checker = Checker {
        import_map: &ex.import_map,
        sdks,
    };

    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut imports: Vec<(String, String, SpecifierKind)> = Vec::new();
    let mut handler_form = HandlerForm::Unknown;

    for (path, content) in &ex.files {
        let media = analyze_media(path);
        if media == MediaType::Json {
            if let Err(e) = serde_json::from_slice::<serde_json::Value>(content) {
                diags.push(diag(
                    path,
                    e.line() as u32,
                    e.column() as u32,
                    format!("invalid JSON: {e}"),
                ));
            }
            continue;
        }
        if !is_module_media(media) {
            continue;
        }
        let Ok(text) = std::str::from_utf8(content) else {
            diags.push(diag(path, 1, 1, "source is not valid UTF-8"));
            continue;
        };
        match analyze::parse(&file_url(path), path, text, media) {
            Err(d) => diags.push(d),
            Ok(parsed) => {
                if *path == ex.entry {
                    handler_form = parsed.handler_form;
                }
                for DepRef { spec, line, col, .. } in &parsed.deps {
                    imports.push((path.clone(), spec.clone(), classify_specifier(spec)));
                    if let Some(msg) = checker.check(path, spec) {
                        let message = if msg.contains(spec.as_str()) {
                            msg
                        } else {
                            format!("import `{spec}`: {msg}")
                        };
                        diags.push(diag(path, *line, *col, message));
                    }
                }
            }
        }
    }
    if !diags.is_empty() {
        return Err(VErr::Invalid(diags));
    }

    let size = upload.body.len() as u64;
    Ok(Validated {
        upload: ValidatedUpload {
            source_digest: Digest::of(&upload.body),
            entry: Some(ex.entry.clone()),
            handler_form,
            size,
            exports: Vec::new(),
        },
        files: ex
            .files
            .iter()
            .map(|(p, c)| SourceFile {
                path: p.clone(),
                content: c.clone(),
            })
            .collect(),
        imports,
        import_map: ex.import_map.clone(),
        wasm: None,
    })
}

fn analyze_media(path: &str) -> MediaType {
    crate::sniff::media_type_of(path)
}

pub(crate) fn wasm(limits: &UploadLimits, upload: &Upload) -> Result<Validated, VErr> {
    let bytes: &[u8] = &upload.body;
    let size = bytes.len() as u64;
    if size > limits.max_wasm {
        return Err(VErr::TooLarge(format!("component exceeds {} bytes", limits.max_wasm)));
    }
    let info = check_component(bytes)?;
    Ok(Validated {
        upload: ValidatedUpload {
            source_digest: Digest::of(bytes),
            entry: None,
            handler_form: HandlerForm::Unknown,
            size,
            exports: info.exports.clone(),
        },
        files: Vec::new(),
        imports: Vec::new(),
        import_map: BTreeMap::new(),
        wasm: Some(info),
    })
}

/// Validate a component against the import policy (contracts C-7).
pub(crate) fn check_component(bytes: &[u8]) -> Result<WasmInfo, VErr> {
    let bad = |m: String| VErr::InvalidArtifact(m);
    if crate::sniff::is_wasm_core_module(bytes) {
        return Err(bad(
            "not a WebAssembly component (a core module was uploaded)".to_string()
        ));
    }
    if !crate::sniff::is_wasm_component(bytes) {
        return Err(bad("not a WebAssembly component".to_string()));
    }

    // No threads / shared memory (no in-guest timers via racing threads).
    let mut features = WasmFeatures::default();
    features.remove(WasmFeatures::THREADS);
    features.remove(WasmFeatures::SHARED_EVERYTHING_THREADS);
    Validator::new_with_features(features)
        .validate_all(bytes)
        .map_err(|e| {
            bad(format!(
                "invalid component (threads and shared memory are not allowed): {e}"
            ))
        })?;

    let mut imports: Vec<String> = Vec::new();
    let mut exports: Vec<String> = Vec::new();
    let mut depth = 0usize;
    for payload in Parser::new(0).parse_all(bytes) {
        let payload = payload.map_err(|e| bad(format!("malformed component: {e}")))?;
        match payload {
            Payload::ModuleSection { .. } | Payload::ComponentSection { .. } => depth += 1,
            Payload::End(_) => depth = depth.saturating_sub(1),
            Payload::ComponentImportSection(r) if depth == 0 => {
                for i in r {
                    let i = i.map_err(|e| bad(format!("malformed component imports: {e}")))?;
                    imports.push(i.name.0.to_string());
                }
            }
            Payload::ComponentExportSection(r) if depth == 0 => {
                for e in r {
                    let e = e.map_err(|e| bad(format!("malformed component exports: {e}")))?;
                    exports.push(e.name.0.to_string());
                }
            }
            _ => {}
        }
    }

    for name in &imports {
        if !is_import_allowed(name) {
            return Err(bad(
                renamed_import(name).unwrap_or_else(|| format!("import `{name}` is not allowed"))
            ));
        }
    }
    if !exports.iter().any(|e| is_incoming_handler(e)) {
        return Err(bad(format!(
            "component does not export `{REQUIRED_EXPORT}` (any 0.2.x)"
        )));
    }
    Ok(WasmInfo {
        is_component: true,
        exports_incoming_handler: true,
        imports,
        exports,
    })
}
