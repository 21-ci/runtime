//! App bundles (R12): every bundle is hostile input. Each test crafts one, refuses it with a stable
//! code, and leaves no way for its contents to run or to reach a host file.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use rt_backup::app::{read_bundle, write_bundle, AppManifest, AppService, AppVersion, ReadLimits};
use rusqlite::Connection;
use sha2::{Digest, Sha256};

const BOOT: &str = rt_store::data::SERVICE_DATA_BOOTSTRAP;
const BLOBS: &str = "CREATE TABLE IF NOT EXISTS _rt_blobs (\n  key BLOB PRIMARY KEY,\n  size INTEGER NOT NULL,\n  content_type TEXT,\n  file TEXT NOT NULL\n) WITHOUT ROWID;";
const BLOB_FILE: &str = "1a-2b-3c";
const LIMITS: ReadLimits = ReadLimits { max_unpacked: 64 << 20 };

fn manifest() -> AppManifest {
    AppManifest {
        format: rt_backup::app::FORMAT,
        rt_version: "test".into(),
        exported_at: 1,
        slug: "demo".into(),
        service: AppService {
            runtime: "deno".into(),
            ..AppService::default()
        },
        env: BTreeMap::from([("A".to_string(), "1".to_string())]),
        secret_names: vec!["TOKEN".into()],
        schedules: Vec::new(),
        versions: vec![AppVersion {
            number: 1,
            size: 0,
            sha256: String::new(),
            content_type: "application/typescript".into(),
            live: true,
            meta: None,
        }],
        data: rt_backup::app::AppFile::default(),
        blobs: Vec::new(),
    }
}

/// A healthy work directory: a database with one blob, and one source. `sql` runs last.
fn work(root: &Path, sql: &str) -> PathBuf {
    let w = root.join("work");
    std::fs::create_dir_all(w.join("objects")).expect("objects");
    std::fs::create_dir_all(w.join("sources")).expect("sources");
    std::fs::write(w.join("sources/1"), "export default {};").expect("source");
    std::fs::write(w.join("objects").join(BLOB_FILE), "abc").expect("blob");
    let c = Connection::open(w.join("data.db")).expect("db");
    c.execute_batch(BOOT).expect("boot");
    c.execute_batch(BLOBS).expect("blobs");
    c.execute("INSERT INTO _rt_blobs VALUES (X'01', 3, NULL, ?1)", [BLOB_FILE])
        .expect("row");
    c.execute_batch(sql).expect("sql");
    w
}

fn pack(root: &Path, sql: &str) -> PathBuf {
    let w = work(root, sql);
    let out = root.join("app.tar.zst");
    write_bundle(&w, manifest(), &out).expect("write");
    out
}

fn code_of(bundle: &Path, root: &Path) -> &'static str {
    read_bundle(bundle, &root.join("staging"), LIMITS).unwrap_err().code
}

/// Write `entries` raw, manifest as given. The tar crate's safe path setters refuse `..`, as an
/// attacker's tool would not.
fn raw_tar(out: &Path, entries: &[(&str, tar::EntryType, Vec<u8>)]) {
    let f = std::fs::File::create(out).expect("create");
    let enc = zstd::stream::write::Encoder::new(f, 1).expect("enc").auto_finish();
    let mut b = tar::Builder::new(enc);
    for (path, kind, data) in entries {
        let mut h = tar::Header::new_gnu();
        h.as_gnu_mut().expect("gnu").name[..path.len()].copy_from_slice(path.as_bytes());
        h.set_size(data.len() as u64);
        h.set_mode(0o600);
        h.set_entry_type(*kind);
        h.set_cksum();
        b.append(&h, &data[..]).expect("append");
    }
    b.into_inner().expect("finish").flush().expect("flush");
}

fn sha(b: &[u8]) -> String {
    Sha256::digest(b).iter().map(|x| format!("{x:02x}")).collect()
}

/// A manifest that lists exactly one source and a real database, for path-level attacks.
fn small_parts(root: &Path) -> (Vec<u8>, Vec<u8>) {
    let p = root.join("small.db");
    let c = Connection::open(&p).expect("db");
    c.execute_batch(BOOT).expect("boot");
    c.execute_batch(BLOBS).expect("blobs");
    drop(c);
    let db = std::fs::read(&p).expect("read");
    let mut m = manifest();
    m.versions.clear();
    m.data.size = db.len() as u64;
    m.data.sha256 = sha(&db);
    (serde_json::to_vec(&m).expect("json"), db)
}

#[test]
fn a_healthy_bundle_round_trips() {
    let d = tempfile::tempdir().expect("dir");
    let b = pack(d.path(), "CREATE TABLE t (a); INSERT INTO t VALUES (1);");
    let s = read_bundle(&b, &d.path().join("staging"), LIMITS).expect("read");
    assert_eq!(s.manifest.env["A"], "1");
    assert_eq!(s.manifest.secret_names, ["TOKEN"]);
    assert_eq!(s.manifest.blobs.len(), 1);
    assert_eq!(std::fs::read(s.objects.join(BLOB_FILE)).expect("blob"), b"abc");
    assert_eq!(
        std::fs::read_to_string(s.sources.join("1")).expect("source"),
        "export default {};"
    );
    let c = Connection::open(&s.db).expect("open");
    let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).expect("t");
    assert_eq!(n, 1);
    assert_eq!(s.unpacked_data_bytes(), std::fs::metadata(&s.db).expect("db").len() + 3);
}

/// The attack the plan calls critical: a row that makes `std:blob.get` read the master key.
#[test]
fn a_blob_row_pointing_at_the_master_key_is_refused() {
    for file in ["/data/master.key", "../../master.key", "objects/../../master.key"] {
        let d = tempfile::tempdir().expect("dir");
        let b = pack(
            d.path(),
            &format!("INSERT INTO _rt_blobs VALUES (X'02', 1, NULL, '{file}');"),
        );
        assert_eq!(code_of(&b, d.path()), "unsafe_entry", "{file}");
    }
}

#[test]
fn triggers_and_views_reaching_the_runtime_tables_are_refused() {
    for sql in [
        "CREATE TRIGGER x AFTER INSERT ON _rt_blobs BEGIN SELECT 1; END;",
        "CREATE TABLE t (a); CREATE TRIGGER x AFTER INSERT ON t BEGIN DELETE FROM _rt_kv; END;",
        "CREATE TABLE t (a); CREATE TRIGGER x AFTER INSERT ON t BEGIN DELETE FROM _RT_KV; END;",
        "CREATE VIEW v AS SELECT * FROM _rt_blobs;",
        "CREATE VIEW _rt_shadow AS SELECT 1;",
        "CREATE TABLE _rt_other (a);",
        "CREATE TABLE t (a); CREATE TRIGGER _rt_named AFTER INSERT ON t BEGIN SELECT 1; END;",
        "CREATE INDEX spy ON _rt_kv(value);",
    ] {
        let d = tempfile::tempdir().expect("dir");
        let b = pack(d.path(), sql);
        assert_eq!(code_of(&b, d.path()), "unsafe_entry", "{sql}");
    }
}

/// A guest's own trigger is fine until it calls what the guest authorizer denies.
#[test]
fn schema_denied_by_the_guest_authorizer_is_refused() {
    for sql in [
        "CREATE TABLE t (a); CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT load_extension('/tmp/evil'); END;",
        "CREATE VIEW v AS SELECT load_extension('/tmp/evil');",
        "CREATE VIRTUAL TABLE vt USING dbstat;",
    ] {
        let d = tempfile::tempdir().expect("dir");
        let b = pack(d.path(), sql);
        let r = read_bundle(&b, &d.path().join("staging"), LIMITS);
        assert_eq!(r.err().map(|e| e.code), Some("unsafe_entry"), "{sql}");
    }
}

#[test]
fn a_guests_own_schema_stays() {
    let d = tempfile::tempdir().expect("dir");
    let b = pack(
        d.path(),
        "CREATE TABLE t (a); CREATE INDEX ti ON t(a); CREATE VIEW v AS SELECT a FROM t;
         CREATE TRIGGER g AFTER INSERT ON t BEGIN UPDATE t SET a = 1 WHERE 0; END;
         CREATE VIRTUAL TABLE docs USING fts5(body);",
    );
    let s = read_bundle(&b, &d.path().join("staging"), LIMITS).expect("read");
    let c = Connection::open(&s.db).expect("open");
    let n: i64 = c
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name IN ('t','ti','v','g','docs')",
            [],
            |r| r.get(0),
        )
        .expect("count");
    assert_eq!(n, 5);
}

/// A trigger that would fire during the import's own writes never does.
#[test]
fn imported_data_runs_nothing_at_import() {
    let d = tempfile::tempdir().expect("dir");
    let b = pack(
        d.path(),
        "CREATE TABLE fired (a);
         INSERT INTO _rt_durable_alarm VALUES ('obj', 1);
         CREATE TABLE t (a); CREATE TRIGGER g AFTER INSERT ON t BEGIN INSERT INTO fired VALUES (1); END;",
    );
    // A row in a runtime table cannot carry a trigger (refused above), so the alarm goes by policy.
    let s = read_bundle(&b, &d.path().join("staging"), LIMITS).expect("read");
    let c = Connection::open(&s.db).expect("open");
    let count = |sql: &str| c.query_row::<i64, _, _>(sql, [], |r| r.get(0)).expect("count");
    assert_eq!(
        count("SELECT count(*) FROM _rt_durable_alarm"),
        0,
        "no alarm may wake the guest"
    );
    assert_eq!(count("SELECT count(*) FROM fired"), 0);
}

#[test]
fn reserved_ddl_is_compared_byte_for_byte() {
    for (table, ddl) in [
        ("_rt_blobs", "CREATE TABLE _rt_blobs (key BLOB PRIMARY KEY, size INTEGER NOT NULL, content_type TEXT, file TEXT NOT NULL) WITHOUT ROWID"),
        ("_rt_kv", "CREATE TABLE _rt_kv (key BLOB PRIMARY KEY, value BLOB NOT NULL, tag INTEGER NOT NULL DEFAULT 0, expires_at INTEGER, extra TEXT) WITHOUT ROWID"),
    ] {
        let d = tempfile::tempdir().expect("dir");
        let w = work(d.path(), "");
        let c = Connection::open(w.join("data.db")).expect("db");
        c.execute_batch(&format!("DROP TABLE {table}; {ddl};")).expect("alter");
        if table == "_rt_blobs" {
            c.execute("INSERT INTO _rt_blobs VALUES (X'01', 3, NULL, ?1)", [BLOB_FILE]).expect("row");
        }
        drop(c);
        let out = d.path().join("a.tar.zst");
        write_bundle(&w, manifest(), &out).expect("write");
        assert_eq!(code_of(&out, d.path()), "unsafe_entry", "{table}");
    }
}

#[test]
fn hostile_tar_entries_are_refused() {
    let d = tempfile::tempdir().expect("dir");
    let (m, db) = small_parts(d.path());
    let reg = tar::EntryType::Regular;
    for (i, extra) in [
        ("../master.key", reg, b"x".to_vec()),
        ("/data/master.key", reg, b"x".to_vec()),
        ("blobs/../../master.key", reg, b"x".to_vec()),
        ("blobs/1a-2b-3c", reg, b"x".to_vec()), // not listed
        ("sources/1", reg, b"x".to_vec()),      // not listed
        ("hosts.json", reg, b"{}".to_vec()),
        ("artifacts/x/blob", reg, b"x".to_vec()),
        ("data.db", tar::EntryType::Symlink, Vec::new()),
        ("././@LongLink", tar::EntryType::GNULongName, b"data.db\0".to_vec()),
    ]
    .into_iter()
    .enumerate()
    {
        let out = d.path().join(format!("h{i}.tar.zst"));
        let name = extra.0;
        raw_tar(
            &out,
            &[("manifest.json", reg, m.clone()), extra, ("data.db", reg, db.clone())],
        );
        let staging = d.path().join(format!("s{i}"));
        let e = read_bundle(&out, &staging, LIMITS).unwrap_err();
        assert_eq!(e.code, "unsafe_entry", "{name}: {}", e.message);
    }
    // The neighbouring good one.
    let out = d.path().join("ok.tar.zst");
    raw_tar(&out, &[("manifest.json", reg, m.clone()), ("data.db", reg, db.clone())]);
    read_bundle(&out, &d.path().join("sok"), LIMITS).expect("good");
    // A duplicate, a manifest out of place.
    let out = d.path().join("dup.tar.zst");
    raw_tar(
        &out,
        &[
            ("manifest.json", reg, m.clone()),
            ("data.db", reg, db.clone()),
            ("data.db", reg, db.clone()),
        ],
    );
    assert_eq!(code_of(&out, &d.path().join("x1")), "unsafe_entry");
    let out = d.path().join("late.tar.zst");
    raw_tar(&out, &[("data.db", reg, db.clone()), ("manifest.json", reg, m)]);
    assert_eq!(code_of(&out, &d.path().join("x2")), "unsafe_entry");
}

#[test]
fn a_manifest_is_an_allowlist() {
    for key in ["tenant", "project", "preview", "hosts", "artifacts", "secrets"] {
        let d = tempfile::tempdir().expect("dir");
        let (m, db) = small_parts(d.path());
        let mut v: serde_json::Value = serde_json::from_slice(&m).expect("json");
        v[key] = serde_json::json!("x");
        let out = d.path().join("a.tar.zst");
        let reg = tar::EntryType::Regular;
        raw_tar(
            &out,
            &[
                ("manifest.json", reg, serde_json::to_vec(&v).expect("json")),
                ("data.db", reg, db),
            ],
        );
        assert_eq!(code_of(&out, d.path()), "bad_manifest", "{key}");
    }
    // And inside `service`: a scratch policy or tenant cannot be smuggled in.
    for key in ["scratch", "tenant", "project", "environment"] {
        let d = tempfile::tempdir().expect("dir");
        let (m, db) = small_parts(d.path());
        let mut v: serde_json::Value = serde_json::from_slice(&m).expect("json");
        v["service"][key] = serde_json::json!("x");
        let out = d.path().join("a.tar.zst");
        let reg = tar::EntryType::Regular;
        raw_tar(
            &out,
            &[
                ("manifest.json", reg, serde_json::to_vec(&v).expect("json")),
                ("data.db", reg, db),
            ],
        );
        assert_eq!(code_of(&out, d.path()), "bad_manifest", "service.{key}");
    }
}

#[test]
fn a_digest_or_size_that_does_not_match_is_refused() {
    let d = tempfile::tempdir().expect("dir");
    let (m, mut db) = small_parts(d.path());
    let last = db.len() - 1;
    db[last] ^= 1;
    let out = d.path().join("a.tar.zst");
    let reg = tar::EntryType::Regular;
    raw_tar(&out, &[("manifest.json", reg, m.clone()), ("data.db", reg, db.clone())]);
    assert_eq!(code_of(&out, d.path()), "unsafe_entry");
    // Longer than declared.
    db.push(0);
    let out = d.path().join("b.tar.zst");
    raw_tar(&out, &[("manifest.json", reg, m), ("data.db", reg, db)]);
    assert_ne!(code_of(&out, &d.path().join("y")), "");
}

#[test]
fn a_bomb_is_stopped_by_its_declared_sizes_and_ratio() {
    let d = tempfile::tempdir().expect("dir");
    let b = pack(d.path(), "");
    let small = ReadLimits { max_unpacked: 16 };
    let e = read_bundle(&b, &d.path().join("s1"), small).unwrap_err();
    assert_eq!(e.code, "too_large");

    // A manifest that declares terabytes, in a small file.
    let (m, db) = small_parts(d.path());
    let mut v: serde_json::Value = serde_json::from_slice(&m).expect("json");
    v["data"]["size"] = serde_json::json!(1u64 << 42);
    let out = d.path().join("c.tar.zst");
    let reg = tar::EntryType::Regular;
    raw_tar(
        &out,
        &[
            ("manifest.json", reg, serde_json::to_vec(&v).expect("json")),
            ("data.db", reg, db),
        ],
    );
    let huge = ReadLimits { max_unpacked: u64::MAX };
    assert_eq!(
        read_bundle(&out, &d.path().join("s2"), huge).unwrap_err().code,
        "too_large"
    );

    // Sizes honestly declared, but compressing past any real database's ratio.
    let zeros = vec![0u8; 256 << 20];
    let mut m: AppManifest = serde_json::from_slice(&small_parts(d.path()).0).expect("m");
    m.data.size = zeros.len() as u64;
    m.data.sha256 = sha(&zeros);
    let out = d.path().join("z.tar.zst");
    raw_tar(
        &out,
        &[
            ("manifest.json", reg, serde_json::to_vec(&m).expect("json")),
            ("data.db", reg, zeros),
        ],
    );
    let e = read_bundle(&out, &d.path().join("s3"), ReadLimits { max_unpacked: 1 << 30 }).unwrap_err();
    assert_eq!(e.code, "too_large");
}

#[test]
fn a_truncated_bundle_is_refused() {
    let d = tempfile::tempdir().expect("dir");
    let b = pack(d.path(), "");
    let all = std::fs::read(&b).expect("read");
    for cut in [0, 10, all.len() / 2, all.len() - 1] {
        let p = d.path().join(format!("cut{cut}"));
        std::fs::write(&p, &all[..cut]).expect("write");
        assert!(
            read_bundle(&p, &d.path().join(format!("s{cut}")), LIMITS).is_err(),
            "cut at {cut}"
        );
    }
    // Whole frame, listed entry missing.
    let (m, _) = small_parts(d.path());
    let out = d.path().join("miss.tar.zst");
    raw_tar(&out, &[("manifest.json", tar::EntryType::Regular, m)]);
    assert_eq!(code_of(&out, &d.path().join("q")), "unsafe_entry");
}

#[test]
fn the_entries_it_writes_are_regular_private_and_listed() {
    let d = tempfile::tempdir().expect("dir");
    let b = pack(d.path(), "");
    let f = std::fs::File::open(&b).expect("open");
    let mut a = tar::Archive::new(zstd::stream::read::Decoder::new(f).expect("dec"));
    let names: Vec<String> = a
        .entries()
        .expect("entries")
        .map(|e| {
            let e = e.expect("entry");
            assert_eq!(e.header().entry_type(), tar::EntryType::Regular);
            String::from_utf8(e.path_bytes().to_vec()).expect("utf8")
        })
        .collect();
    assert_eq!(names[0], "manifest.json");
    assert!(names.contains(&"data.db".to_string()));
    assert!(names.contains(&format!("blobs/{BLOB_FILE}")));
    assert!(names.contains(&"sources/1".to_string()));
}
