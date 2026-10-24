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
