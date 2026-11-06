//! Archive and JSON-deploy edge cases of the upload extractor: unsupported content types, unsafe
//! entry names, directory and special archive members, gzip tars, duplicate names, and the file
//! count and size limits. Each case is an accepted upload or a named rejection.

use crate::common;

use std::io::{Cursor, Write};

use bytes::Bytes;
use common::*;
use rt_build::{Builder, Upload};
use rt_types::{Diagnostic, RuntimeKind, UploadValidator, ValidationError};

const MAIN: &[u8] = b"export default () => new Response('x');\n";

fn rejected(up: &Upload) -> Vec<Diagnostic> {
    match Builder::validate(&default_builder(), RuntimeKind::Deno, up) {
        Err(d) => d,
        Ok(_) => panic!("expected the upload to be rejected"),
    }
}

fn accepted(up: &Upload) {
    if let Err(d) = Builder::validate(&default_builder(), RuntimeKind::Deno, up) {
        panic!("expected the upload to pass validation, got {d:?}");
    }
}

/// A plain (non-PAX) tar with the given members, each `(name, type, data)`.
fn gnu_tar(members: &[(&str, tar::EntryType, &[u8])]) -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    for (name, kind, data) in members {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(*kind);
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        b.append_data(&mut h, name, *data).expect("tar member");
    }
    b.into_inner().expect("tar finish")
}

#[test]
fn a_content_type_that_is_no_deploy_format_is_rejected_by_name() {
    let d = rejected(&upload("image/png", Bytes::from_static(MAIN)));
    assert!(d[0].message.contains("unsupported content type `image/png`"), "{d:?}");
}

#[test]
fn a_zip_directory_entry_is_skipped_and_its_files_are_kept() {
    let mut zw = zip::ZipWriter::new(Cursor::new(Vec::new()));
    zw.add_directory("lib/", zip::write::SimpleFileOptions::default())
        .expect("dir");
    zw.start_file("main.ts", zip::write::SimpleFileOptions::default())
        .expect("start");
    zw.write_all(MAIN).expect("write");
    let body = zw.finish().expect("finish").into_inner();
    accepted(&upload("application/zip", Bytes::from(body)));
}

#[test]
fn a_gzipped_tar_is_inflated_and_validated_like_a_tar() {
    let tar = gnu_tar(&[("main.ts", tar::EntryType::Regular, MAIN)]);
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(&tar).expect("gzip");
    accepted(&upload(
        "application/gzip",
        Bytes::from(gz.finish().expect("gzip finish")),
    ));
}

#[test]
fn a_tar_that_names_one_path_twice_is_rejected_as_a_duplicate() {
    let body = gnu_tar(&[
        ("main.ts", tar::EntryType::Regular, MAIN),
        ("main.ts", tar::EntryType::Regular, b"x"),
    ]);
    let d = rejected(&upload("application/x-tar", Bytes::from(body)));
    assert!(
        d.iter()
            .any(|x| x.message.contains("duplicate archive entry `main.ts`")),
        "{d:?}"
    );
}

#[test]
fn a_tar_device_entry_is_not_a_regular_file() {
    let body = gnu_tar(&[
        ("main.ts", tar::EntryType::Regular, MAIN),
        ("dev", tar::EntryType::Char, b""),
    ]);
    let d = rejected(&upload("application/x-tar", Bytes::from(body)));
    assert!(
        d.iter().any(|x| x.message.contains("`dev` is not a regular file")),
        "{d:?}"
    );
}

#[test]
fn a_tar_global_pax_header_is_not_an_upload_member() {
    let body = gnu_tar(&[
        ("pax_global_header", tar::EntryType::XGlobalHeader, b"30 mtime=1\n"),
        ("main.ts", tar::EntryType::Regular, MAIN),
    ]);
    accepted(&upload("application/x-tar", Bytes::from(body)));
}

#[test]
fn a_zip_with_more_entries_than_the_file_limit_is_too_large() {
    let names: Vec<String> = (0..1002).map(|i| format!("f{i}.json")).collect();
    let files: Vec<(&str, &[u8])> = names.iter().map(|n| (n.as_str(), &b"{}"[..])).collect();
    let up = upload("application/zip", Bytes::from(zip_bytes(&files)));
    let r = UploadValidator::validate(&default_builder(), RuntimeKind::Deno, &up.content_type, &up.body);
    assert!(matches!(r, Err(ValidationError::TooLarge(_))), "{r:?}");
}

#[test]
fn an_upload_over_the_single_file_limit_is_reported_as_too_large() {
    let up = upload("application/typescript", Bytes::from(vec![b' '; 1024 * 1024 + 1]));
    let d = rejected(&up);
    assert!(d[0].message.starts_with("too_large: source exceeds"), "{d:?}");
}

#[test]
fn a_json_deploy_with_an_unsafe_file_name_is_rejected_with_the_reason() {
    let main = std::str::from_utf8(MAIN).expect("utf8");
    for (name, reason) in [
        ("", "empty path"),
        ("a\u{0}b.ts", "NUL byte"),
        ("a\\b.ts", "backslash"),
        ("C:x.ts", "absolute path"),
        ("/abs.ts", "absolute path"),
        ("./", "empty path"),
        ("lib/../x.ts", "path traversal"),
    ] {
        let up = json_upload("main.ts", &[("main.ts", main), (name, "x")], None);
        let d = rejected(&up);
        assert!(
            d.iter().any(|x| x.message.contains(reason)),
            "{name:?} should fail as {reason}: {d:?}"
        );
    }
}

// ---- from-url: an archive narrowed to its source ------------------------------------------------

fn shape(strip: u32, subdir: Option<&str>) -> rt_types::ArchiveShape {
    rt_types::ArchiveShape {
        strip_components: strip,
        subdir: subdir.map(str::to_string),
        entry: None,
    }
}

fn gz(raw: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(raw).unwrap();
    e.finish().unwrap()
}

fn repo() -> Vec<u8> {
    gnu_tar(&[
        ("repo-abc/README.md", tar::EntryType::Regular, b"readme"),
        ("repo-abc/docs/link", tar::EntryType::Symlink, b""),
        ("repo-abc/apps/x/main.ts", tar::EntryType::Regular, MAIN),
        (
            "repo-abc/apps/x/lib/a.ts",
            tar::EntryType::Regular,
            b"export const a = 1;\n",
        ),
        ("repo-abc/apps/xy/other.ts", tar::EntryType::Regular, b"x"),
    ])
}

fn names_in(tar_bytes: &[u8]) -> Vec<String> {
    tar::Archive::new(Cursor::new(tar_bytes))
        .entries()
        .unwrap()
        .map(|e| e.unwrap().path().unwrap().to_string_lossy().to_string())
        .collect()
}

#[test]
fn an_archive_is_narrowed_to_its_subdir_and_validates_as_an_upload() {
    let b = default_builder();
    let out = b
        .reshape_archive(&Bytes::from(gz(&repo())), &shape(1, Some("apps/x/")))
        .unwrap();
    // `apps/xy` is not `apps/x`, and a link outside the subdir does not matter.
    assert_eq!(names_in(&out), vec!["lib/a.ts", "main.ts"]);
    UploadValidator::validate(&b, RuntimeKind::Deno, "application/x-tar", &out).unwrap();
}

#[test]
fn the_same_tree_has_the_same_digest_whatever_the_archive_around_it() {
    let b = default_builder();
    let one = b
        .reshape_archive(&Bytes::from(gz(&repo())), &shape(1, Some("apps/x")))
        .unwrap();
    let two = b
        .reshape_archive(&Bytes::from(repo()), &shape(1, Some("apps/x")))
        .unwrap();
    assert_eq!(one, two);
}

#[test]
fn the_entry_argument_is_what_rt_json_would_say() {
    let b = default_builder();
    let raw = gnu_tar(&[
        ("r/app.ts", tar::EntryType::Regular, MAIN),
        ("r/rt.json", tar::EntryType::Regular, br#"{"imports":{"a":"./app.ts"}}"#),
    ]);
    let s = rt_types::ArchiveShape {
        entry: Some("app.ts".into()),
        ..shape(1, None)
    };
    let out = b.reshape_archive(&Bytes::from(raw), &s).unwrap();
    let v = UploadValidator::validate(&b, RuntimeKind::Deno, "application/x-tar", &out).unwrap();
    assert_eq!(v.entry.as_deref(), Some("app.ts"));
}

#[test]
fn an_entry_that_escapes_is_refused_before_anything_is_stripped() {
    let b = default_builder();
    let mut raw = tar::Builder::new(Vec::new());
    let mut h = tar::Header::new_gnu();
    h.as_gnu_mut().unwrap().name[..16].copy_from_slice(b"top/../../evil.t");
    h.set_size(1);
    h.set_mode(0o644);
    h.set_cksum();
    raw.append(&h, &b"x"[..]).unwrap();
    let e = b
        .reshape_archive(&Bytes::from(raw.into_inner().unwrap()), &shape(1, None))
        .unwrap_err();
    assert!(matches!(e, ValidationError::Invalid(_)), "{e:?}");
}

#[test]
fn a_link_inside_the_subdir_is_refused() {
    let b = default_builder();
    let raw = gnu_tar(&[
        ("r/main.ts", tar::EntryType::Regular, MAIN),
        ("r/passwd", tar::EntryType::Symlink, b""),
    ]);
    let e = b.reshape_archive(&Bytes::from(raw), &shape(1, None)).unwrap_err();
    assert!(matches!(e, ValidationError::Invalid(_)), "{e:?}");
}

#[test]
fn a_bomb_outside_the_subdir_still_counts() {
    let b = default_builder();
    let zeros = vec![0u8; 11 * 1024 * 1024];
    let raw = gnu_tar(&[
        ("r/big.bin", tar::EntryType::Regular, &zeros),
        ("r/a/main.ts", tar::EntryType::Regular, MAIN),
    ]);
    let e = b
        .reshape_archive(&Bytes::from(gz(&raw)), &shape(1, Some("a")))
        .unwrap_err();
    assert!(matches!(e, ValidationError::TooLarge(_)), "{e:?}");
    let small = gz(&vec![0u8; 512 * 40_000]);
    let e = b.reshape_archive(&Bytes::from(small), &shape(0, None)).unwrap_err();
    assert!(matches!(e, ValidationError::TooLarge(_)), "{e:?}");
}

#[test]
fn what_is_no_archive_or_matches_nothing_is_refused() {
    let b = default_builder();
    let e = b
        .reshape_archive(&Bytes::from_static(b"export default 1"), &shape(0, None))
        .unwrap_err();
    assert!(matches!(e, ValidationError::Invalid(_)), "{e:?}");
    let e = b
        .reshape_archive(&Bytes::from(repo()), &shape(1, Some("nope")))
        .unwrap_err();
    assert!(matches!(e, ValidationError::Invalid(_)), "{e:?}");
    let e = b
        .reshape_archive(&Bytes::from(repo()), &shape(1, Some("../x")))
        .unwrap_err();
    assert!(matches!(e, ValidationError::Invalid(_)), "{e:?}");
}
