//! Deno upload extraction: single file, `application/json` `{entry, files, imports}`, zip and tar
//! (optionally gzipped) archives. Pure: no disk, no network. Archive safety lives here (SEC-31): nothing is ever extracted to disk, entries are read into
//! memory under hard limits and every path is normalized or rejected.

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use bytes::Bytes;
use rt_types::{ArchiveShape, Diagnostic};
use serde::Deserialize;

use crate::builder::UploadLimits;

/// Why validation failed, before it is mapped to `Vec<Diagnostic>` or `ValidationError`.
#[derive(Debug)]
pub(crate) enum VErr {
    /// `422 validation_failed`.
    Invalid(Vec<Diagnostic>),
    /// `422 invalid_artifact`.
    InvalidArtifact(String),
    /// `413 too_large`.
    TooLarge(String),
}

impl VErr {
    pub(crate) fn one(file: &str, line: u32, col: u32, message: impl Into<String>) -> Self {
        VErr::Invalid(vec![diag(file, line, col, message)])
    }

    /// Flatten into diagnostics (the `Builder::validate` shape).
    pub(crate) fn into_diagnostics(self) -> Vec<Diagnostic> {
        match self {
            VErr::Invalid(d) => d,
            VErr::InvalidArtifact(r) => vec![diag("", 1, 1, format!("invalid_artifact: {r}"))],
            VErr::TooLarge(r) => vec![diag("", 1, 1, format!("too_large: {r}"))],
        }
    }
}

pub(crate) fn diag(file: &str, line: u32, col: u32, message: impl Into<String>) -> Diagnostic {
    Diagnostic {
        file: file.to_string(),
        line,
        col,
        message: message.into(),
    }
}

/// A validated, normalized Deno source tree.
#[derive(Debug)]
pub(crate) struct Extracted {
    /// Normalized path -> content (`rt.json` is consumed, not listed).
    pub files: BTreeMap<String, Bytes>,
    pub entry: String,
    pub import_map: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct RtJson {
    entry: Option<String>,
    imports: Option<BTreeMap<String, String>>,
}

#[derive(Deserialize)]
struct JsonDeploy {
    entry: Option<String>,
    files: BTreeMap<String, String>,
    imports: Option<BTreeMap<String, String>>,
}

enum Shape {
    Single(&'static str),
    Zip,
    Tar,
    Json,
}

const DEFAULT_ENTRIES: &[&str] = &["main.ts", "main.tsx", "main.js", "main.jsx", "main.mjs"];

fn shape_of(content_type: Option<&str>, body: &[u8]) -> Result<Shape, VErr> {
    let Some(ct) = content_type else {
        return Ok(sniff_shape(body));
    };
    let ct = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    match ct.as_str() {
        "application/typescript" | "text/typescript" | "application/x-typescript" | "text/x-typescript" => {
            Ok(Shape::Single("main.ts"))
        }
        "text/javascript" | "application/javascript" | "application/x-javascript" | "text/x-javascript" => {
            Ok(Shape::Single("main.js"))
        }
        "application/zip" | "application/x-zip-compressed" => Ok(Shape::Zip),
        "application/x-tar" | "application/gzip" | "application/x-gzip" => Ok(Shape::Tar),
        "application/json" => Ok(Shape::Json),
        other => Err(VErr::one(
            "",
            1,
            1,
            format!("unsupported content type `{other}` for a deno upload"),
        )),
    }
}

/// Content sniffing for builds, where only the stored bytes are known.
fn sniff_shape(body: &[u8]) -> Shape {
    if body.starts_with(b"PK\x03\x04") || body.starts_with(b"PK\x05\x06") {
        return Shape::Zip;
    }
    if body.starts_with(&[0x1f, 0x8b]) || (body.len() >= 262 && &body[257..262] == b"ustar") {
        return Shape::Tar;
    }
    if body.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'{') {
        if let Ok(serde_json::Value::Object(o)) = serde_json::from_slice::<serde_json::Value>(body) {
            if o.contains_key("files") {
                return Shape::Json;
            }
        }
    }
    // A single source file whose type is unknown is built as TypeScript, a superset of JavaScript;
    // a parse error is reported by the build itself.
    Shape::Single("main.ts")
}

/// Normalize an entry path, or say why it is unsafe.
pub(crate) fn normalize_path(raw: &str) -> Result<String, &'static str> {
    if raw.is_empty() {
        return Err("empty path");
    }
    if raw.contains('\0') {
        return Err("path contains a NUL byte");
    }
    if raw.contains('\\') {
        return Err("path contains a backslash");
    }
    if raw.starts_with('/') {
        return Err("absolute path");
    }
    let b = raw.as_bytes();
    if b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' {
        return Err("absolute path");
    }
    let mut parts: Vec<&str> = Vec::new();
    for c in raw.split('/') {
        match c {
            "" | "." => {}
            ".." => return Err("path traversal (`..`)"),
            c => parts.push(c),
        }
    }
    if parts.is_empty() {
        return Err("empty path");
    }
    Ok(parts.join("/"))
}

/// Raw entries collected from a container, before limits and `rt.json` handling.
struct Collector<'a> {
    limits: &'a UploadLimits,
    /// Keep only this part of the container (`from-url`); the rest still counts toward the limits.
    scope: Option<&'a Scope>,
    entries: BTreeMap<String, Bytes>,
    total: u64,
}

/// The part of an archive that is the source.
struct Scope {
    strip: usize,
    subdir: Option<String>,
}

impl Scope {
    /// Where the normalized `path` lands, or `None` when it is outside.
    fn place(&self, path: &str) -> Option<String> {
        let mut rest = path;
        for _ in 0..self.strip {
            rest = rest.split_once('/')?.1;
        }
        match &self.subdir {
            Some(dir) => rest.strip_prefix(dir.as_str())?.strip_prefix('/').map(str::to_string),
            None => Some(rest.to_string()),
        }
    }
}

impl<'a> Collector<'a> {
    fn new(limits: &'a UploadLimits, scope: Option<&'a Scope>) -> Self {
        Self {
            limits,
            scope,
            entries: BTreeMap::new(),
            total: 0,
        }
    }

    /// A safe path that the scope leaves out: links and special files there do not matter.
    fn out_of_scope(&self, raw_name: &str) -> bool {
        match (self.scope, normalize_path(raw_name)) {
            (Some(scope), Ok(path)) => scope.place(&path).is_none(),
            _ => false,
        }
    }

    fn add(&mut self, raw_name: &str, mut content: impl Read) -> Result<(), VErr> {
        let path = normalize_path(raw_name)
            .map_err(|why| VErr::one(raw_name, 1, 1, format!("unsafe archive entry `{raw_name}`: {why}")))?;
        let path = match self.scope {
            Some(scope) => match scope.place(&path) {
                Some(placed) => placed,
                None => return self.skip(&mut content, raw_name),
            },
            None => path,
        };
        if self.entries.len() >= self.limits.max_files {
            return Err(VErr::TooLarge(format!("more than {} files", self.limits.max_files)));
        }
        let mut buf = Vec::new();
        content
            .by_ref()
            .take(self.limits.max_file + 1)
            .read_to_end(&mut buf)
            .map_err(|e| VErr::one(raw_name, 1, 1, format!("cannot read archive entry `{raw_name}`: {e}")))?;
        if buf.len() as u64 > self.limits.max_file {
            return Err(VErr::TooLarge(format!(
                "`{raw_name}` exceeds {} bytes",
                self.limits.max_file
            )));
        }
        self.total += buf.len() as u64;
        if self.total > self.limits.max_archive_total {
            return Err(VErr::TooLarge(format!(
                "archive exceeds {} bytes",
                self.limits.max_archive_total
            )));
        }
        if self.entries.insert(path.clone(), Bytes::from(buf)).is_some() {
            return Err(VErr::one(raw_name, 1, 1, format!("duplicate archive entry `{path}`")));
        }
        Ok(())
    }

    /// Count an entry the scope leaves out, without keeping it: a bomb outside `subdir` is a bomb.
    fn skip(&mut self, content: &mut impl Read, raw_name: &str) -> Result<(), VErr> {
        let room = self.limits.max_archive_total.saturating_sub(self.total);
        let read = std::io::copy(&mut content.by_ref().take(room + 1), &mut std::io::sink())
            .map_err(|e| VErr::one(raw_name, 1, 1, format!("cannot read archive entry `{raw_name}`: {e}")))?;
        self.total += read;
        if self.total > self.limits.max_archive_total {
            return Err(VErr::TooLarge(format!(
                "archive exceeds {} bytes",
                self.limits.max_archive_total
            )));
        }
        Ok(())
    }
}

fn read_zip<'a>(body: &[u8], limits: &'a UploadLimits, scope: Option<&'a Scope>) -> Result<Collector<'a>, VErr> {
    let mut archive = zip::ZipArchive::new(Cursor::new(body))
        .map_err(|e| VErr::one("", 1, 1, format!("invalid zip archive: {e}")))?;
    // A narrowed archive may hold more than it keeps, within reason.
    let listed = if scope.is_some() {
        limits.max_files.saturating_mul(SCOPED_LISTING_FACTOR)
    } else {
        limits.max_files + 1
    };
    if archive.len() > listed {
        return Err(VErr::TooLarge(format!("more than {} files", limits.max_files)));
    }
    let mut c = Collector::new(limits, scope);
    for i in 0..archive.len() {
        let mut f = archive
            .by_index(i)
            .map_err(|e| VErr::one("", 1, 1, format!("invalid zip entry #{i}: {e}")))?;
        let name = f.name().to_string();
        if f.is_dir() || name.ends_with('/') {
            continue;
        }
        if f.is_symlink() && c.out_of_scope(&name) {
            continue;
        }
        if f.is_symlink() {
            return Err(VErr::one(
                &name,
                1,
                1,
                format!("`{name}` is a symbolic link; links are not allowed"),
            ));
        }
        c.add(&name, &mut f)?;
    }
    Ok(c)
}

fn read_tar<'a>(body: &[u8], limits: &'a UploadLimits, scope: Option<&'a Scope>) -> Result<Collector<'a>, VErr> {
    let bad = |e: std::io::Error| VErr::one("", 1, 1, format!("invalid tar archive: {e}"));
    // Gzip-compressed tars are inflated under the same total cap.
    let inflated;
    let raw: &[u8] = if body.starts_with(&[0x1f, 0x8b]) {
        let cap = limits.max_archive_total + 512 * (limits.max_files as u64 + 8);
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(body)
            .take(cap + 1)
            .read_to_end(&mut out)
            .map_err(bad)?;
        // Cut here, the tar could end cleanly at a block boundary and lose its later entries.
        if out.len() as u64 > cap {
            return Err(VErr::TooLarge(format!(
                "archive exceeds {} bytes",
                limits.max_archive_total
            )));
        }
        inflated = out;
        &inflated
    } else {
        body
    };
    let mut archive = tar::Archive::new(Cursor::new(raw));
    let mut c = Collector::new(limits, scope);
    for entry in archive.entries().map_err(bad)? {
        let mut entry = entry.map_err(bad)?;
        let name = String::from_utf8_lossy(&entry.path_bytes()).to_string();
        let kind = entry.header().entry_type();
        if kind.is_dir() || kind.is_pax_global_extensions() || kind.is_pax_local_extensions() {
            continue;
        }
        if !(kind.is_file() || kind.is_contiguous()) && c.out_of_scope(&name) {
            continue;
        }
        if kind.is_symlink() || kind.is_hard_link() {
            return Err(VErr::one(
                &name,
                1,
                1,
                format!("`{name}` is a link; links are not allowed"),
            ));
        }
        if !(kind.is_file() || kind.is_contiguous()) {
            return Err(VErr::one(&name, 1, 1, format!("`{name}` is not a regular file")));
        }
        c.add(&name, &mut entry)?;
    }
    Ok(c)
}

/// Extract and validate a Deno upload. `content_type = None` sniffs the shape (used by builds,
/// which only have the stored bytes).
pub(crate) fn extract(content_type: Option<&str>, body: &[u8], limits: &UploadLimits) -> Result<Extracted, VErr> {
    let mut import_map: BTreeMap<String, String> = BTreeMap::new();
    let mut declared_entry: Option<(String, &'static str)> = None; // (entry, where it was declared)
    let mut files: BTreeMap<String, Bytes>;

    match shape_of(content_type, body)? {
        Shape::Single(name) => {
            if body.len() as u64 > limits.max_single_file {
                return Err(VErr::TooLarge(format!(
                    "source exceeds {} bytes",
                    limits.max_single_file
                )));
            }
            files = BTreeMap::new();
            files.insert(name.to_string(), Bytes::copy_from_slice(body));
            return Ok(Extracted {
                files,
                entry: name.to_string(),
                import_map,
            });
        }
        Shape::Zip => files = ratio_checked(read_zip(body, limits, None)?, body, limits)?,
        Shape::Tar => files = ratio_checked(read_tar(body, limits, None)?, body, limits)?,
        Shape::Json => {
            let parsed: JsonDeploy = serde_json::from_slice(body).map_err(|e| {
                VErr::one(
                    "",
                    e.line() as u32,
                    e.column() as u32,
                    format!("invalid json deploy payload: {e}"),
                )
            })?;
            let mut c = Collector::new(limits, None);
            for (name, content) in &parsed.files {
                c.add(name, content.as_bytes())?;
            }
            files = c.entries;
            if let Some(imports) = parsed.imports {
                import_map = imports;
            }
            if let Some(e) = parsed.entry {
                declared_entry = Some((e, "entry"));
            }
        }
    }

    if let Some(raw) = files.remove("rt.json") {
        let rt: RtJson = serde_json::from_slice(&raw).map_err(|e| {
            VErr::one(
                "rt.json",
                e.line() as u32,
                e.column() as u32,
                format!("invalid rt.json: {e}"),
            )
        })?;
        if let Some(imports) = rt.imports {
            import_map.extend(imports);
        }
        if let Some(e) = rt.entry {
            declared_entry = Some((e, "rt.json"));
        }
    }

    let entry = match declared_entry {
        Some((raw, from)) => {
            let path = normalize_path(raw.trim_start_matches("./"))
                .map_err(|why| VErr::one(from, 1, 1, format!("invalid entry `{raw}`: {why}")))?;
            if !files.contains_key(&path) {
                return Err(VErr::one(
                    from,
                    1,
                    1,
                    format!("entry `{path}` does not exist in the upload"),
                ));
            }
            path
        }
        None => DEFAULT_ENTRIES
            .iter()
            .find(|c| files.contains_key(**c))
            .map(|c| (*c).to_string())
            .ok_or_else(|| {
                VErr::one(
                    "",
                    1,
                    1,
                    "no entry module: expected main.ts (or set `entry` in rt.json)",
                )
            })?,
    };
    Ok(Extracted {
        files,
        entry,
        import_map,
    })
}

/// Files a narrowed zip may list, as a multiple of what it may keep.
const SCOPED_LISTING_FACTOR: usize = 100;

/// A zip or (gzipped) tar narrowed to `shape`, as a canonical tar: sorted, no timestamps, so the
/// same tree has the same digest. Entries are normalized before anything is stripped, so `a/../..`
/// is refused rather than stripped into something that looks safe.
pub(crate) fn reshape(body: &[u8], shape: &ArchiveShape, limits: &UploadLimits) -> Result<Bytes, VErr> {
    let subdir = match shape.subdir.as_deref().map(|d| d.trim_end_matches('/')) {
        None | Some("") | Some(".") => None,
        Some(d) => Some(normalize_path(d).map_err(|why| VErr::one("", 1, 1, format!("invalid `subdir`: {why}")))?),
    };
    let scope = Scope {
        strip: shape.strip_components as usize,
        subdir,
    };
    let collected = match sniff_shape(body) {
        Shape::Zip => read_zip(body, limits, Some(&scope))?,
        Shape::Tar => read_tar(body, limits, Some(&scope))?,
        Shape::Json | Shape::Single(_) => {
            return Err(VErr::one("", 1, 1, "the URL did not return a zip or tar archive"));
        }
    };
    let mut files = ratio_checked(collected, body, limits)?;
    if files.is_empty() {
        return Err(VErr::one(
            "",
            1,
            1,
            "nothing in the archive matches `strip_components` and `subdir`",
        ));
    }
    if let Some(entry) = &shape.entry {
        let mut rt = match files.get("rt.json") {
            Some(raw) => serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(raw).map_err(|e| {
                VErr::one(
                    "rt.json",
                    e.line() as u32,
                    e.column() as u32,
                    format!("invalid rt.json: {e}"),
                )
            })?,
            None => serde_json::Map::new(),
        };
        rt.insert("entry".to_string(), serde_json::Value::String(entry.clone()));
        let text = serde_json::to_vec(&rt).map_err(|e| VErr::one("rt.json", 1, 1, e.to_string()))?;
        files.insert("rt.json".to_string(), Bytes::from(text));
    }
    let mut out = tar::Builder::new(Vec::new());
    for (path, content) in &files {
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_entry_type(tar::EntryType::Regular);
        out.append_data(&mut header, path, content.as_ref())
            .map_err(|e| VErr::one(path, 1, 1, format!("cannot pack `{path}`: {e}")))?;
    }
    let packed = out
        .into_inner()
        .map_err(|e| VErr::one("", 1, 1, format!("cannot pack the archive: {e}")))?;
    Ok(Bytes::from(packed))
}

fn ratio_checked(c: Collector<'_>, body: &[u8], limits: &UploadLimits) -> Result<BTreeMap<String, Bytes>, VErr> {
    let compressed = (body.len() as u64).max(1);
    if c.total > compressed.saturating_mul(limits.max_inflate_ratio) && c.total > 64 * 1024 {
        return Err(VErr::TooLarge(format!(
            "decompressed size {} is more than {}x the archive size",
            c.total, limits.max_inflate_ratio
        )));
    }
    Ok(c.entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_single_file_without_a_recorded_type_is_built_as_typescript() {
        let bodies: [&[u8]; 4] = [
            b"export default (req: Request) => new Response('ts');",
            b"export default () => new Response('plain');",
            b"export default ((",
            &[0xff, 0xfe],
        ];
        for body in bodies {
            assert!(matches!(sniff_shape(body), Shape::Single("main.ts")), "{body:?}");
        }
    }

    #[test]
    fn stored_bytes_that_are_a_json_deploy_payload_are_sniffed_as_one() {
        let body = br#"  {"entry": "main.ts", "files": {"main.ts": "export default () => new Response('x');"}}"#;
        let ex = extract(None, body, &UploadLimits::default()).unwrap();
        assert_eq!(ex.entry, "main.ts");
        assert!(ex.files.contains_key("main.ts"));
    }

    #[test]
    fn stored_bytes_that_start_like_json_but_are_not_one_are_a_source_file() {
        let ex = extract(None, b"{ not json", &UploadLimits::default()).unwrap();
        assert_eq!(ex.entry, "main.ts");
    }

    #[test]
    fn stored_json_without_a_files_map_is_a_single_source_file() {
        let ex = extract(None, br#"{"answer": 42}"#, &UploadLimits::default()).unwrap();
        assert_eq!(ex.entry, "main.ts");
        assert_eq!(ex.files.len(), 1);
    }

    #[test]
    fn normalizing_a_path_keeps_its_parts_and_names_what_is_unsafe() {
        assert_eq!(normalize_path("./lib//a.ts").unwrap(), "lib/a.ts");
        assert_eq!(normalize_path("").unwrap_err(), "empty path");
        assert_eq!(normalize_path("a\0b.ts").unwrap_err(), "path contains a NUL byte");
        assert_eq!(normalize_path("a\\b.ts").unwrap_err(), "path contains a backslash");
        assert_eq!(normalize_path("C:x.ts").unwrap_err(), "absolute path");
        assert_eq!(normalize_path("./").unwrap_err(), "empty path");
    }

    #[test]
    fn an_upload_too_large_is_reported_as_a_diagnostic_naming_the_limit() {
        let d = VErr::TooLarge("source exceeds 10 bytes".into()).into_diagnostics();
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].message, "too_large: source exceeds 10 bytes");
    }
}
