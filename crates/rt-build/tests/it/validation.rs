//! Synchronous deploy-time validation: BLD-01, BLD-02,
//! BLD-03, BLD-17, SEC-31, plus handler-form detection, entry rules, import classification and the
//! WASM component checks (WSM-08, SEC-23 deploy half).
//!
//! Validation is pure: no network, no disk. Two entry points are exercised:
//! `Builder::validate` (rich `Validated` / `Vec<Diagnostic>`) and the `UploadValidator` seam
//! `rt-api` uses (`ValidationError::{Invalid, InvalidArtifact, TooLarge}` = 422 / 422 / 413).

use crate::common;

use std::time::{Duration, Instant};

use common::wasm_fixtures as wasm;
use common::*;
use rt_build::{Builder, DefaultBuilder, SpecifierKind, Upload};
use rt_types::{Diagnostic, HandlerForm, RuntimeKind, ValidatedUpload, ValidationError};

/// The `UploadValidator` seam (fully qualified: `DefaultBuilder` implements two `validate`s).
fn deploy(b: &DefaultBuilder, up: &Upload, kind: RuntimeKind) -> Result<ValidatedUpload, ValidationError> {
    rt_types::UploadValidator::validate(b, kind, &up.content_type, &up.body)
}

fn deno(b: &DefaultBuilder, up: &Upload) -> Result<rt_build::Validated, Vec<Diagnostic>> {
    Builder::validate(b, RuntimeKind::Deno, up)
}

fn diag_mentions(diags: &[Diagnostic], needle: &str) -> bool {
    diags
        .iter()
        .any(|d| d.message.contains(needle) || d.file.contains(needle))
}

// ---------------------------------------------------------------------------------------------
// BLD-01 syntax errors
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_bld_01_syntax_error_422_with_file_line_col() {
    let b = default_builder();
    let up = fixture_upload("syntax_error");

    let diags = deno(&b, &up).expect_err("a syntax error must be rejected at deploy");
    assert!(!diags.is_empty());
    let d = &diags[0];
    assert!(
        d.file.ends_with("main.ts"),
        "diagnostic names the file, got {:?}",
        d.file
    );
    assert_eq!(d.line, 5, "fixture keeps the error on line 5");
    assert!(
        d.col >= 1 && d.col <= 20,
        "column is 1-based and on that line, got {}",
        d.col
    );
    assert!(!d.message.is_empty());

    // Through the API seam it is a 422 `validation_failed` carrying the same diagnostics.
    match deploy(&b, &up, RuntimeKind::Deno) {
        Err(ValidationError::Invalid(ds)) => {
            assert_eq!(ds[0].line, 5);
            assert!(ds[0].file.ends_with("main.ts"));
        }
        other => panic!("expected ValidationError::Invalid, got {other:?}"),
    }
}

#[test]
fn bld_01_syntax_error_in_archive_member_names_that_file() {
    let b = default_builder();
    let up = zip_upload(&[
        (
            "main.ts",
            b"import { x } from \"./lib/bad.ts\";\nexport default () => new Response(String(x));\n",
        ),
        ("lib/bad.ts", b"export const x = {;\n"),
    ]);
    let diags = deno(&b, &up).expect_err("syntax error in a library file");
    assert!(
        diags.iter().any(|d| d.file == "lib/bad.ts" && d.line == 1),
        "got {diags:?}"
    );
}

#[test]
fn bld_01_javascript_syntax_error_is_reported_too() {
    let b = default_builder();
    let up = js_upload("export default function () { return new Response(\"x\" }\n");
    let diags = deno(&b, &up).expect_err("JS syntax error");
    assert!(diags[0].file.ends_with("main.js"));
    assert_eq!(diags[0].line, 1);
}

#[test]
fn bld_01_valid_typescript_features_are_accepted() {
    // Transpile-only: types, enums, generics and satisfies must parse.
    let src = r#"
        enum Color { Red, Green }
        interface Shape<T> { kind: T; area(): number }
        const s = { kind: "sq", area: () => 4 } satisfies Shape<string>;
        export default async function (req: Request): Promise<Response> {
            return new Response(String(s.area() + Color.Green + (req.method as string).length));
        }
    "#;
    deno(&default_builder(), &ts_upload(src)).expect("valid TypeScript");
}

// ---------------------------------------------------------------------------------------------
// BLD-02 forbidden specifiers
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_bld_02_forbidden_specifiers_422_naming_each() {
    let b = default_builder();
    let up = fixture_upload("bad_specifiers");
    let diags = deno(&b, &up).expect_err("file:, node: and unknown sdk: imports are rejected");

    for spec in ["file:///etc/passwd", "node:child_process", "sdk:x"] {
        assert!(diag_mentions(&diags, spec), "422 must name `{spec}`, got {diags:?}");
    }
    assert!(
        !diag_mentions(&diags, "std:kv"),
        "`std:kv` is a valid built-in and must not be reported"
    );

    // Positions point at the import lines of the fixture (4, 5, 6).
    let line_of = |needle: &str| diags.iter().find(|d| d.message.contains(needle)).map(|d| d.line);
    assert_eq!(line_of("file:///etc/passwd"), Some(4));
    assert_eq!(line_of("node:child_process"), Some(5));
    assert_eq!(line_of("sdk:x"), Some(6));

    assert!(matches!(
        deploy(&b, &up, RuntimeKind::Deno),
        Err(ValidationError::Invalid(_))
    ));
}

#[test]
fn bld_02_each_forbidden_specifier_is_rejected_on_its_own() {
    let b = default_builder();
    for spec in [
        "file:///etc/passwd",
        "node:child_process",
        "node:fs",
        "sdk:x",
        "std:nope",
    ] {
        let src = format!("import m from \"{spec}\";\nexport default () => new Response(String(m));\n");
        let diags = deno(&b, &ts_upload(&src)).expect_err(spec);
        assert!(diag_mentions(&diags, spec), "`{spec}` not named in {diags:?}");
    }
}

#[test]
fn bld_02_renamed_rt_module_names_its_replacement() {
    let b = default_builder();
    let src = "import { kv } from \"rt:kv\";\nexport default () => new Response(String(kv));\n";
    let diags = deno(&b, &ts_upload(src)).expect_err("rt:kv");
    assert!(diag_mentions(&diags, "`rt:kv` was renamed to `std:kv`"), "{diags:?}");
}

#[test]
fn bld_02_export_from_is_a_static_import_too() {
    let b = default_builder();
    for src in [
        "export * from \"file:///etc/passwd\";\nexport default () => new Response('x');\n",
        "export { a } from \"node:fs\";\nexport default () => new Response('x');\n",
    ] {
        assert!(
            deno(&b, &ts_upload(src)).is_err(),
            "re-export must be classified like an import: {src}"
        );
    }
}

#[test]
fn bld_02_a_published_sdk_specifier_is_accepted() {
    let b = default_builder().with_sdk_catalog(sdk_catalog(&[("telegram", &["send"])]));
    let ok = ts_upload("import { send } from \"sdk:telegram\";\nexport default () => new Response(String(send));\n");
    deno(&b, &ok).expect("configured sdk:telegram");
    let unknown = ts_upload("import { send } from \"sdk:other\";\nexport default () => new Response(String(send));\n");
    assert!(deno(&b, &unknown).is_err(), "only published SDKs pass");
    let std =
        ts_upload("import { telegram } from \"std:sdk\";\nexport default () => new Response(String(telegram));\n");
    deno(&b, &std).expect("std:sdk is a runtime module");
}

#[test]
fn bld_02_bare_specifier_needs_an_import_map() {
    let b = default_builder();
    let bare = ts_upload("import _ from \"lodash\";\nexport default () => new Response(String(_));\n");
    let diags = deno(&b, &bare).expect_err("bare specifier without a map");
    assert!(diag_mentions(&diags, "lodash"));

    let mapped = json_upload(
        "main.ts",
        &[(
            "main.ts",
            "import _ from \"lodash\";\nexport default () => new Response(String(_));\n",
        )],
        Some(&[("lodash", "npm:lodash@4")]),
    );
    deno(&b, &mapped).expect("rt.json imports map the bare specifier");
}

#[test]
fn relative_import_of_a_missing_file_passes_validation_and_fails_at_build() {
    // The synchronous checks do not resolve relative imports; E2E-05 / API-10 deploy such
    // a version lazily (201) and it ends in `failed(resolve)` at build time (graph stage).
    let b = default_builder();
    let up = zip_upload(&[(
        "main.ts",
        b"import \"./missing.ts\";\nexport default () => new Response('x');\n",
    )]);
    deno(&b, &up).expect("a missing relative import is a build-time failure, not a deploy-time one");
}

#[test]
fn validate_classifies_every_static_import() {
    let b = default_builder();
    let files: &[(&str, &[u8])] = &[
        (
            "main.ts",
            b"import { a } from \"./a.ts\";\nimport { kv } from \"std:kv\";\nimport { z } from \"npm:zod@3\";\n\
              import { join } from \"jsr:@std/path@1\";\nimport { y } from \"https://esm.sh/foo.js\";\n\
              export { c } from \"./a.ts\";\nexport default () => new Response(String([a, kv, z, join, y, c]));\n",
        ),
        ("a.ts", b"export const a = 1;\nexport const c = 2;\n"),
    ];
    let v = deno(&b, &zip_upload(files)).expect("valid archive");

    let has = |spec: &str, kind: SpecifierKind| {
        v.imports
            .iter()
            .any(|(f, s, k)| f == "main.ts" && s == spec && *k == kind)
    };
    assert!(has("./a.ts", SpecifierKind::Relative));
    assert!(has("std:kv", SpecifierKind::Builtin));
    assert!(has("npm:zod@3", SpecifierKind::Npm));
    assert!(has("jsr:@std/path@1", SpecifierKind::Jsr));
    assert!(has("https://esm.sh/foo.js", SpecifierKind::Remote));
}

// ---------------------------------------------------------------------------------------------
// Entry, handler form, deploy shapes
// ---------------------------------------------------------------------------------------------

#[test]
fn validate_single_file_sets_entry_and_digest() {
    let b = default_builder();
    let src = fixture_source("hello");
    let ts = deno(&b, &ts_upload(&src)).expect("hello");
    assert_eq!(ts.upload.entry.as_deref(), Some("main.ts"));
    assert_eq!(ts.upload.handler_form, HandlerForm::A);
    assert_eq!(ts.upload.size, src.len() as u64);
    assert_eq!(ts.files.len(), 1);
    assert_eq!(ts.files[0].path, "main.ts");
    assert_eq!(
        ts.upload.source_digest,
        rt_types::Digest::of(src.as_bytes()),
        "single-file source is stored as-is (content-addressed in T3)"
    );

    let js = deno(&b, &js_upload("export default () => new Response('x');\n")).expect("js");
    assert_eq!(js.upload.entry.as_deref(), Some("main.js"));
}

#[test]
fn validate_is_content_addressed_and_deterministic() {
    let b = default_builder();
    let up = fixture_upload("multi_file");
    let a = deno(&b, &up).expect("multi_file");
    let c = deno(&b, &up).expect("multi_file again");
    assert_eq!(a.upload.source_digest, c.upload.source_digest);
    let other = deno(&b, &ts_upload(&fixture_source("hello"))).expect("hello");
    assert_ne!(a.upload.source_digest, other.upload.source_digest);
}

#[test]
fn validate_detects_handler_form_statically() {
    let b = default_builder();
    let form = |src: &str| deno(&b, &ts_upload(src)).expect("valid").upload.handler_form;

    assert_eq!(
        form("export default function (req: Request) { return new Response('a'); }\n"),
        HandlerForm::A
    );
    assert_eq!(
        form("export default async (req: Request) => new Response('a');\n"),
        HandlerForm::A
    );
    assert_eq!(
        form("export default { fetch(req: Request) { return new Response('b'); } };\n"),
        HandlerForm::B
    );
    assert_eq!(
        form("export default { scheduled() {}, queue() {} };\n"),
        HandlerForm::B,
        "an object literal with scheduled/queue is Form B"
    );
    assert_eq!(
        form("function make() { return () => new Response('x'); }\nexport default make();\n"),
        HandlerForm::Unknown,
        "ambiguous default exports are detected at runtime"
    );

    // The real fixtures agree.
    assert_eq!(
        deno(&b, &fixture_upload("hello")).unwrap().upload.handler_form,
        HandlerForm::A
    );
    assert_eq!(
        deno(&b, &fixture_upload("form_a_scheduled"))
            .unwrap()
            .upload
            .handler_form,
        HandlerForm::A
    );
    assert_eq!(
        deno(&b, &fixture_upload("form_b")).unwrap().upload.handler_form,
        HandlerForm::B
    );
}

#[test]
fn validate_zip_tar_and_json_shapes_agree() {
    let b = default_builder();
    let zip = deno(&b, &fixture_upload("multi_file")).expect("zip");
    let tar = deno(&b, &fixture_tar_upload("multi_file")).expect("tar");

    for v in [&zip, &tar] {
        assert_eq!(v.upload.entry.as_deref(), Some("main.ts"));
        assert_eq!(v.import_map.get("greet").map(String::as_str), Some("./lib/greet.ts"));
        let paths: Vec<&str> = v.files.iter().map(|f| f.path.as_str()).collect();
        for p in ["main.ts", "lib/greet.ts", "lib/util.ts", "lib/lazy.ts"] {
            assert!(paths.contains(&p), "{p} missing from {paths:?}");
        }
    }

    let json = json_upload(
        "src/app.ts",
        &[
            (
                "src/app.ts",
                "import { n } from \"./n.ts\";\nexport default () => new Response(String(n));\n",
            ),
            ("src/n.ts", "export const n = 1;\n"),
        ],
        Some(&[("n", "./src/n.ts")]),
    );
    let v = deno(&b, &json).expect("json deploy");
    assert_eq!(v.upload.entry.as_deref(), Some("src/app.ts"));
    assert_eq!(v.import_map.get("n").map(String::as_str), Some("./src/n.ts"));
}

#[test]
fn validate_entry_must_exist() {
    let b = default_builder();
    let json = json_upload(
        "main.ts",
        &[("other.ts", "export default () => new Response('x');\n")],
        None,
    );
    let diags = deno(&b, &json).expect_err("entry missing from json deploy");
    assert!(diag_mentions(&diags, "main.ts"));

    let rt = br#"{"entry": "app.ts"}"#;
    let zip = zip_upload(&[
        ("main.ts", b"export default () => new Response('x');\n"),
        ("rt.json", rt),
    ]);
    let diags = deno(&b, &zip).expect_err("rt.json entry missing from archive");
    assert!(diag_mentions(&diags, "app.ts"));
}

#[test]
fn validate_rt_json_must_be_valid() {
    let b = default_builder();
    let zip = zip_upload(&[
        ("main.ts", b"export default () => new Response('x');\n"),
        ("rt.json", b"{ not json"),
    ]);
    let diags = deno(&b, &zip).expect_err("malformed rt.json");
    assert!(diags.iter().any(|d| d.file == "rt.json"), "{diags:?}");
}

#[test]
fn validate_archive_default_entry_is_main() {
    let b = default_builder();
    let zip = zip_upload(&[("main.ts", b"export default () => new Response('x');\n")]);
    assert_eq!(
        deno(&b, &zip).expect("no rt.json").upload.entry.as_deref(),
        Some("main.ts")
    );
}

// ---------------------------------------------------------------------------------------------
// BLD-03 1 MB, no network
// ---------------------------------------------------------------------------------------------

#[tokio::test]
#[cfg_attr(debug_assertions, ignore = "300 ms budget is a release SLO")]
async fn ac_bld_03_one_mb_source_validates_in_300ms_without_network() {
    // A live MockCdn proves nothing is contacted while validating.
    let cdn = cdn_with_zod().await;
    let src = big_valid_ts(1_000_000);
    assert!(src.len() <= 1024 * 1024, "stay within the 1 MB single-file limit");
    let up = ts_upload(&src);
    let b = default_builder();

    let t0 = Instant::now();
    let v = deploy(&b, &up, RuntimeKind::Deno).expect("1 MB valid source");
    let took = t0.elapsed();

    assert!(
        took <= Duration::from_millis(300),
        "validation took {took:?}, budget is 300 ms (run optimized)"
    );
    assert_eq!(v.size, src.len() as u64);
    assert_eq!(cdn.total_hits(), 0, "validation must not touch the network");
}

// ---------------------------------------------------------------------------------------------
// BLD-17 limits -> 413 too_large
// ---------------------------------------------------------------------------------------------

fn tiny_ts_files(n: usize) -> Vec<(String, Vec<u8>)> {
    let mut v = vec![(
        "main.ts".to_string(),
        b"export default () => new Response('x');\n".to_vec(),
    )];
    for i in 1..n {
        v.push((format!("m{i}.ts"), format!("export const v{i} = {i};\n").into_bytes()));
    }
    v
}

fn zip_of(files: &[(String, Vec<u8>)]) -> Upload {
    let refs: Vec<(&str, &[u8])> = files.iter().map(|(n, c)| (n.as_str(), c.as_slice())).collect();
    zip_upload(&refs)
}

fn assert_too_large(r: Result<ValidatedUpload, ValidationError>, what: &str) {
    match r {
        Err(ValidationError::TooLarge(_)) => {}
        other => panic!("{what}: expected 413 TooLarge, got {other:?}"),
    }
}

#[test]
fn ac_bld_17_more_than_1000_files_is_too_large() {
    let b = default_builder();
    assert_too_large(
        deploy(&b, &zip_of(&tiny_ts_files(1001)), RuntimeKind::Deno),
        "1001 files",
    );
    let entries: Vec<TarEntry> = tiny_ts_files(1001).iter().map(|(n, c)| TarEntry::file(n, c)).collect();
    assert_too_large(deploy(&b, &tar_upload(&entries), RuntimeKind::Deno), "1001 files (tar)");
}

#[test]
fn bld_17_exactly_1000_files_is_accepted() {
    let b = default_builder();
    deploy(&b, &zip_of(&tiny_ts_files(1000)), RuntimeKind::Deno).expect("1000 files is within the limit");
}

#[test]
fn ac_bld_17_more_than_10_mb_total_is_too_large() {
    let b = default_builder();
    let mut files = tiny_ts_files(1);
    for i in 0..11u64 {
        files.push((format!("data/blob{i}.json"), random_json(1_000_000, 1 + i)));
    }
    // 11 files x 1 MB (each within the 1 MB per-file limit) = 11 MB > 10 MB.
    assert_too_large(deploy(&b, &zip_of(&files), RuntimeKind::Deno), "11 MB archive");
    let entries: Vec<TarEntry> = files.iter().map(|(n, c)| TarEntry::file(n, c)).collect();
    assert_too_large(
        deploy(&b, &tar_upload(&entries), RuntimeKind::Deno),
        "11 MB archive (tar)",
    );
}

#[test]
fn bld_17_one_file_over_1_mb_is_too_large() {
    let b = default_builder();
    let mut files = tiny_ts_files(1);
    files.push(("data/big.json".into(), random_json(1024 * 1024 + 1, 7)));
    assert_too_large(deploy(&b, &zip_of(&files), RuntimeKind::Deno), "file over 1 MB");
}

#[test]
fn bld_17_single_file_over_1_mb_is_too_large() {
    let b = default_builder();
    let src = big_valid_ts(1024 * 1024 + 1024);
    assert_too_large(deploy(&b, &ts_upload(&src), RuntimeKind::Deno), "single file over 1 MB");
}

#[test]
fn bld_17_wasm_over_50_mb_is_too_large() {
    let b = default_builder();
    let mut body = vec![0u8; 50 * 1024 * 1024 + 1];
    body[..8].copy_from_slice(&wasm::OK_COMPONENT[..8]);
    let up = upload("application/wasm", body);
    assert_too_large(deploy(&b, &up, RuntimeKind::Wasm), "wasm over 50 MB");
}

// ---------------------------------------------------------------------------------------------
// SEC-31 archive safety
// ---------------------------------------------------------------------------------------------

fn assert_invalid_naming(r: Result<ValidatedUpload, ValidationError>, needle: &str, what: &str) {
    match r {
        Err(ValidationError::Invalid(ds)) => {
            assert!(!ds.is_empty(), "{what}: empty diagnostics");
            assert!(
                ds.iter().any(|d| d.message.contains(needle) || d.file.contains(needle)),
                "{what}: diagnostics should name `{needle}`, got {ds:?}"
            );
        }
        other => panic!("{what}: expected 422 Invalid, got {other:?}"),
    }
}

const OK_MAIN: &[u8] = b"export default () => new Response('x');\n";

#[test]
fn ac_sec_31_path_traversal_entries_are_rejected() {
    let b = default_builder();
    let zip = zip_upload(&[("main.ts", OK_MAIN), ("../rt_sec31_escape_marker.ts", OK_MAIN)]);
    assert_invalid_naming(
        deploy(&b, &zip, RuntimeKind::Deno),
        "rt_sec31_escape_marker.ts",
        "zip ../",
    );
    let nested = zip_upload(&[("main.ts", OK_MAIN), ("lib/../../rt_sec31_escape_marker.ts", OK_MAIN)]);
    assert!(deploy(&b, &nested, RuntimeKind::Deno).is_err(), "nested .. escape");

    let tar = tar_upload(&[
        TarEntry::file("main.ts", OK_MAIN),
        TarEntry::file("../rt_sec31_escape_marker.ts", OK_MAIN),
    ]);
    assert_invalid_naming(
        deploy(&b, &tar, RuntimeKind::Deno),
        "rt_sec31_escape_marker.ts",
        "tar ../",
    );
}

#[test]
fn ac_sec_31_absolute_paths_are_rejected() {
    let b = default_builder();
    let zip = zip_upload(&[("main.ts", OK_MAIN), ("/tmp/rt_sec31_abs_marker.ts", OK_MAIN)]);
    assert_invalid_naming(
        deploy(&b, &zip, RuntimeKind::Deno),
        "rt_sec31_abs_marker.ts",
        "zip absolute",
    );
    let tar = tar_upload(&[
        TarEntry::file("main.ts", OK_MAIN),
        TarEntry::file("/etc/rt_sec31_abs_marker", OK_MAIN),
    ]);
    assert_invalid_naming(
        deploy(&b, &tar, RuntimeKind::Deno),
        "rt_sec31_abs_marker",
        "tar absolute",
    );
}

#[test]
fn ac_sec_31_links_are_rejected() {
    let b = default_builder();
    assert_invalid_naming(
        deploy(&b, &zip_with_symlink("link.ts", "/etc/passwd"), RuntimeKind::Deno),
        "link.ts",
        "zip symlink",
    );
    let sym = tar_upload(&[
        TarEntry::file("main.ts", OK_MAIN),
        TarEntry::symlink("link.ts", "/etc/passwd"),
    ]);
    assert_invalid_naming(deploy(&b, &sym, RuntimeKind::Deno), "link.ts", "tar symlink");
    let hard = tar_upload(&[
        TarEntry::file("main.ts", OK_MAIN),
        TarEntry::hardlink("hard.ts", "main.ts"),
    ]);
    assert_invalid_naming(deploy(&b, &hard, RuntimeKind::Deno), "hard.ts", "tar hardlink");
}

#[test]
fn ac_sec_31_zip_bomb_is_rejected_by_size_or_ratio() {
    let b = default_builder();
    // 12 x 1 MB of zeros: ~12 KB compressed, 12 MB inflated (> 10 MB, ratio ~1000).
    let r = deploy(&b, &zip_bomb(12, 1_000_000), RuntimeKind::Deno);
    assert!(
        matches!(r, Err(ValidationError::TooLarge(_)) | Err(ValidationError::Invalid(_))),
        "got {r:?}"
    );
}

#[test]
fn ac_sec_31_inflate_ratio_over_100_is_rejected_even_when_small() {
    let b = default_builder();
    // One 900 KB file of zeros: well under every size limit, but ratio ~1000 > 100.
    let r = deploy(&b, &zip_bomb(1, 900_000), RuntimeKind::Deno);
    assert!(
        matches!(r, Err(ValidationError::TooLarge(_)) | Err(ValidationError::Invalid(_))),
        "got {r:?}"
    );
}

#[test]
fn sec_31_rejected_archives_write_nothing_outside_the_artifact_store() {
    // `validate` has no disk access at all; make sure the escape targets do not appear anywhere
    // an extractor might have put them.
    let b = default_builder();
    let attacks = [
        zip_upload(&[("main.ts", OK_MAIN), ("../rt_sec31_escape_marker.ts", OK_MAIN)]),
        zip_upload(&[("main.ts", OK_MAIN), ("/tmp/rt_sec31_abs_marker.ts", OK_MAIN)]),
        tar_upload(&[
            TarEntry::file("main.ts", OK_MAIN),
            TarEntry::file("../rt_sec31_escape_marker.ts", OK_MAIN),
        ]),
    ];
    for a in &attacks {
        let _ = deploy(&b, a, RuntimeKind::Deno);
    }
    let tmp = std::env::temp_dir();
    let mut dirs = vec![tmp.clone(), std::env::current_dir().expect("cwd")];
    if let Some(p) = tmp.parent() {
        dirs.push(p.to_path_buf());
    }
    if let Some(p) = std::env::current_dir().expect("cwd").parent() {
        dirs.push(p.to_path_buf());
    }
    for d in dirs {
        for marker in ["rt_sec31_escape_marker.ts", "rt_sec31_abs_marker.ts"] {
            assert!(!d.join(marker).exists(), "{} was written", d.join(marker).display());
        }
    }
    assert!(!std::path::Path::new("/tmp/rt_sec31_abs_marker.ts").exists());
}

// ---------------------------------------------------------------------------------------------
// WASM component validation (WSM-08; SEC-23 deploy half)
// ---------------------------------------------------------------------------------------------

fn wasm_upload(bytes: &[u8]) -> Upload {
    upload("application/wasm", bytes.to_vec())
}

fn wasm_deploy(bytes: &[u8]) -> Result<ValidatedUpload, ValidationError> {
    deploy(&default_builder(), &wasm_upload(bytes), RuntimeKind::Wasm)
}

#[test]
fn bld_wasm_valid_component_is_accepted_with_facts() {
    let b = default_builder();
    let v = Builder::validate(&b, RuntimeKind::Wasm, &wasm_upload(wasm::OK_COMPONENT)).expect("valid component");
    let info = v.wasm.expect("wasm facts");
    assert!(info.is_component);
    assert!(info.exports_incoming_handler);
    assert!(info.imports.is_empty());
    assert_eq!(v.upload.size, wasm::OK_COMPONENT.len() as u64);
    assert_eq!(v.upload.source_digest, rt_types::Digest::of(wasm::OK_COMPONENT));
}

#[test]
fn bld_wasm_linkable_imports_and_any_wasi_02_minor_are_accepted() {
    let b = default_builder();
    let v = Builder::validate(&b, RuntimeKind::Wasm, &wasm_upload(wasm::OK_COMPONENT_WITH_IMPORTS))
        .expect("wasi:clocks, std:host and wasi:sockets (denied at runtime) are linkable; handler 0.2.6");
    let imports = v.wasm.expect("facts").imports;
    for want in [
        "wasi:clocks/monotonic-clock@0.2.3",
        "std:host/kv@0.1.0",
        "wasi:sockets/tcp@0.2.3",
    ] {
        assert!(imports.iter().any(|i| i == want), "{want} missing from {imports:?}");
    }
}

#[test]
fn ac_wsm_08_component_without_incoming_handler_is_invalid_artifact() {
    match wasm_deploy(wasm::COMPONENT_WITHOUT_HANDLER) {
        Err(ValidationError::InvalidArtifact(reason)) => {
            assert!(
                reason.contains("incoming-handler"),
                "reason should say what is missing: {reason}"
            );
        }
        other => panic!("expected InvalidArtifact, got {other:?}"),
    }
}

#[test]
fn ac_wsm_08_core_module_is_invalid_artifact() {
    match wasm_deploy(wasm::CORE_MODULE) {
        Err(ValidationError::InvalidArtifact(reason)) => assert!(reason.contains("component"), "{reason}"),
        other => panic!("expected InvalidArtifact, got {other:?}"),
    }
}

#[test]
fn bld_wasm_garbage_is_invalid_artifact() {
    assert!(matches!(
        wasm_deploy(b"definitely not wasm"),
        Err(ValidationError::InvalidArtifact(_))
    ));
    assert!(matches!(wasm_deploy(&[]), Err(ValidationError::InvalidArtifact(_))));
}

#[test]
fn bld_wasm_unknown_namespace_import_is_invalid_artifact() {
    match wasm_deploy(wasm::COMPONENT_BAD_IMPORT) {
        Err(ValidationError::InvalidArtifact(reason)) => assert!(reason.contains("evil:pkg/thing"), "{reason}"),
        other => panic!("expected InvalidArtifact, got {other:?}"),
    }
}

#[test]
fn bld_wasm_pre_0_2_wasi_import_is_invalid_artifact() {
    assert!(matches!(
        wasm_deploy(wasm::COMPONENT_WASI_01),
        Err(ValidationError::InvalidArtifact(_))
    ));
}

#[test]
fn ac_sec_23_component_using_the_threads_proposal_is_rejected_at_deploy() {
    match wasm_deploy(wasm::COMPONENT_SHARED_MEMORY) {
        Err(ValidationError::InvalidArtifact(reason)) => {
            assert!(!reason.is_empty());
        }
        other => panic!("shared memory must be 422 invalid_artifact, got {other:?}"),
    }
    let diags = Builder::validate(
        &default_builder(),
        RuntimeKind::Wasm,
        &wasm_upload(wasm::COMPONENT_SHARED_MEMORY),
    )
    .expect_err("Builder::validate reports it too");
    assert!(!diags.is_empty());
}

#[test]
fn a_relative_import_with_a_query_or_fragment_is_invalid() {
    let b = default_builder();
    for spec in ["./a.ts?v=1", "./a.ts#frag"] {
        let up = ts_upload(&format!(
            "import x from '{spec}'; export default () => new Response(String(x));"
        ));
        let diags = deno(&b, &up).expect_err("a relative import with a query or fragment");
        assert!(diag_mentions(&diags, "invalid relative import"), "{diags:?}");
    }
}

#[test]
fn a_non_module_file_in_an_archive_is_not_parsed() {
    let up = zip_upload(&[
        ("main.ts", b"export default () => new Response('x');"),
        ("readme.txt", b"not code"),
    ]);
    assert!(
        deno(&default_builder(), &up).is_ok(),
        "only modules and JSON are checked"
    );
}

#[test]
fn an_invalid_json_file_in_an_archive_is_reported() {
    let up = zip_upload(&[
        ("main.ts", b"export default () => new Response('x');"),
        ("data.json", b"{"),
    ]);
    let diags = deno(&default_builder(), &up).expect_err("bad JSON");
    assert!(diag_mentions(&diags, "invalid JSON"), "{diags:?}");
}

#[test]
fn a_non_utf8_module_in_an_archive_is_reported() {
    let up = zip_upload(&[
        ("main.ts", b"export default () => new Response('x');"),
        ("lib.ts", &[0xff, 0xfe]),
    ]);
    let diags = deno(&default_builder(), &up).expect_err("not UTF-8");
    assert!(diag_mentions(&diags, "not valid UTF-8"), "{diags:?}");
}

#[test]
fn the_upload_validator_renders_the_rpc_surface_for_a_service_and_an_sdk() {
    let b = default_builder();
    let body = bytes::Bytes::from_static(
        b"export const rpc = { async add(args: { a: number }): Promise<number> { return args.a; } };",
    );
    let service = rt_types::UploadValidator::rpc_contract(
        &b,
        "application/typescript",
        &body,
        &rt_types::ContractTarget::Service { slug: "billing".into() },
    )
    .expect("a service surface");
    assert_eq!(service.members, vec!["add".to_string()]);
    assert!(service.dts.contains("BillingRpc"), "{}", service.dts);

    let sdk = rt_types::UploadValidator::rpc_contract(
        &b,
        "application/typescript",
        &body,
        &rt_types::ContractTarget::Sdk {
            name: "acme".into(),
            version: "1.0.0".into(),
        },
    )
    .expect("an sdk surface");
    assert!(sdk.dts.contains("sdk:acme"), "{}", sdk.dts);
}
