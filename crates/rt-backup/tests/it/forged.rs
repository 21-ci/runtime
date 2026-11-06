//! Hostile archives: each is refused with its exact code, and nothing lands in the data directory.

use std::io::Read;
use std::path::{Path, PathBuf};

use rt_backup::archive::{sign_manifest, FileEntry, Manifest, MAC, MANIFEST};
use rt_backup::restore::{restore_archive, verify_archive, Auth, RestoreOptions};
use rt_sec::MasterKey;
use sha2::{Digest as _, Sha256};

use crate::common::*;

struct Rig {
    node: Node,
    tmp: tempfile::TempDir,
    good: PathBuf,
    entries: Vec<RawEntry>,
}

impl Rig {
    async fn new() -> Self {
        let node = Node::seeded().await;
        let tmp = tempfile::tempdir().expect("dir");
        let good = node.backup_to(tmp.path());
        let entries = read_entries(&good);
        Self {
            node,
            tmp,
            good,
            entries,
        }
    }

    fn file(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }

    /// Verify and restore both refuse `archive` with `want`, and the target stays empty.
    fn refused(&self, archive: &Path, want: &str) {
        assert_eq!(code(self.node.verify(archive)), want, "verify");
        let to = tempfile::tempdir().expect("dir");
        assert_eq!(code(self.node.restore(archive, to.path())), want, "restore");
        assert!(is_empty(to.path()), "restore left files behind");
    }

    fn write(&self, name: &str, entries: &[RawEntry]) -> PathBuf {
        let p = self.file(name);
        write_entries(&p, entries);
        p
    }
}

fn entry(path: &str, body: &[u8]) -> RawEntry {
    RawEntry {
        path: path.into(),
        body: body.to_vec(),
        kind: tar::EntryType::Regular,
        link: None,
    }
}

fn sha(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

/// The rewritten copy of a good archive still passes: the tools below change only what they say.
#[tokio::test(flavor = "multi_thread")]
async fn the_rewriter_itself_keeps_a_good_archive_good() {
    let r = Rig::new().await;
    let same = r.write("same.tar", &r.entries);
    r.node.verify(&same).expect("verify");
}

/// S2: any change to the manifest breaks the MAC.
#[tokio::test(flavor = "multi_thread")]
async fn a_tampered_manifest_fails_the_mac() {
    let r = Rig::new().await;
    let mut e = r.entries.clone();
    let i = e.iter().position(|x| x.path == MANIFEST).expect("manifest");
    let text = String::from_utf8(e[i].body.clone()).expect("utf8");
    e[i].body = text
        .replacen("\"rt_version\":\"test\"", "\"rt_version\":\"evil\"", 1)
        .into_bytes();
    r.refused(&r.write("t.tar", &e), "bad_mac");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tampered_mac_fails_the_mac() {
    let r = Rig::new().await;
    let mut e = r.entries.clone();
    let i = e.iter().position(|x| x.path == MAC).expect("mac");
    e[i].body = "00".repeat(32).into_bytes();
    r.refused(&r.write("t.tar", &e), "bad_mac");
    e[i].body = b"not hex".to_vec();
    r.refused(&r.write("t2.tar", &e), "bad_mac");
}

/// Anyone with bucket write access can build a self-consistent archive; only the master key signs it.
#[tokio::test(flavor = "multi_thread")]
async fn a_forged_archive_with_consistent_digests_fails_without_the_key() {
    let r = Rig::new().await;
    let mut e = r.entries.clone();
    resign(&mut e, &MasterKey::generate(), |m| m.rt_version = "forged".into());
    r.refused(&r.write("forged.tar", &e), "bad_mac");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_mac_is_refused_unless_no_auth_is_asked_for() {
    let r = Rig::new().await;
    let e: Vec<_> = r.entries.iter().filter(|x| x.path != MAC).cloned().collect();
    let p = r.write("nomac.tar", &e);
    r.refused(&p, "truncated");
    verify_archive(&p, &Auth::None).expect("--no-auth reads it");
}

/// The manifest is authentic, an entry's bytes are not.
#[tokio::test(flavor = "multi_thread")]
async fn a_changed_entry_fails_its_digest() {
    let r = Rig::new().await;
    let mut e = r.entries.clone();
    let i = e.iter().position(|x| x.path.starts_with("blobs/")).expect("blob");
    e[i].body[0] ^= 1;
    r.refused(&r.write("t.tar", &e), "digest_mismatch");
    let mut e = r.entries.clone();
    let i = e.iter().position(|x| x.path == "meta.db.zst").expect("db");
    let last = e[i].body.len() - 1;
    e[i].body[last] ^= 1;
    r.refused(&r.write("t2.tar", &e), "digest_mismatch");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_archive_cut_anywhere_is_refused() {
    let r = Rig::new().await;
    let bytes = std::fs::read(&r.good).expect("read");
    let manifest_at = r.entries.iter().map(|e| e.body.len()).sum::<usize>();
    for cut in [0, 100, 512, bytes.len() / 2, manifest_at, bytes.len() - 2048] {
        let p = r.file("cut.tar");
        std::fs::write(&p, &bytes[..cut]).expect("write");
        let c = code(r.node.verify(&p));
        assert_eq!(c, "truncated", "cut at {cut}");
        let to = tempfile::tempdir().expect("dir");
        assert_eq!(code(r.node.restore(&p, to.path())), "truncated");
        assert!(is_empty(to.path()));
    }
}

/// Path traversal in the tar, and through the manifest, even when properly signed.
#[tokio::test(flavor = "multi_thread")]
async fn traversal_entries_are_refused() {
    let r = Rig::new().await;
    for evil in [
        "../evil",
        "../../master.key",
        "/etc/cron.d/evil",
        "services/../../evil",
        "master.key",
        "cache/evil",
    ] {
        let mut e = r.entries.clone();
        e.insert(0, entry(evil, b"x"));
        r.refused(&r.write("t.tar", &e), "unsafe_entry");
        // Listed in a manifest signed with the real key: the shape is still refused.
        let mut e = r.entries.clone();
        e.insert(0, entry(evil, b"x"));
        resign(&mut e, &r.node.master, |m| {
            m.files.push(FileEntry {
                path: evil.into(),
                size: 1,
                sha256: sha(b"x"),
                raw_size: None,
            })
        });
        r.refused(&r.write("t2.tar", &e), "unsafe_entry");
    }
    assert!(!r.tmp.path().join("evil").exists());
    assert!(!Path::new("/tmp/evil").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn symlink_hardlink_and_special_entries_are_refused() {
    let r = Rig::new().await;
    for kind in [
        tar::EntryType::Symlink,
        tar::EntryType::Link,
        tar::EntryType::Fifo,
        tar::EntryType::Char,
        tar::EntryType::Directory,
    ] {
        // A link at a path of a legitimate shape, and a write through it after.
        let svc = r.node.service;
        let mut e = r.entries.clone();
        e.insert(
            0,
            RawEntry {
                path: format!("blobs/{svc}/objects/aa-bb-cc"),
                body: Vec::new(),
                kind,
                link: Some("/etc/passwd".into()),
            },
        );
        r.refused(&r.write("t.tar", &e), "unsafe_entry");
    }
}

/// The master key file is the thing to steal: a signed archive's row must still not reach it.
#[tokio::test(flavor = "multi_thread")]
async fn a_blob_row_pointing_at_the_master_key_is_refused_on_restore() {
    let r = Rig::new().await;
    let svc = r.node.service;
    for file in ["/data/master.key", "../../../master.key", "a/b-1-2"] {
        let mut e = r.entries.clone();
        let db_path = format!("services/{svc}/data.db.zst");
        let i = e.iter().position(|x| x.path == db_path).expect("db");
        // Rewrite the service database with the crafted row.
        let work = tempfile::tempdir().expect("dir");
        let raw = work.path().join("d.db");
        let mut plain = Vec::new();
        zstd::stream::read::Decoder::new(&e[i].body[..])
            .expect("zstd")
            .read_to_end(&mut plain)
            .expect("decode");
        std::fs::write(&raw, &plain).expect("write");
        {
            let c = rusqlite::Connection::open(&raw).expect("open");
            c.execute("UPDATE _rt_blobs SET file = ?1", [file]).expect("update");
        }
        let plain = std::fs::read(&raw).expect("read");
        let packed = zstd::encode_all(&plain[..], 3).expect("encode");
        e[i].body = packed.clone();
        resign(&mut e, &r.node.master, |m: &mut Manifest| {
            let f = m.files.iter_mut().find(|f| f.path == db_path).expect("listed");
            f.size = packed.len() as u64;
            f.sha256 = sha(&packed);
            f.raw_size = Some(plain.len() as u64);
        });
        let p = r.write("t.tar", &e);
        // verify checks bytes; the row check is restore's: nothing reaches disk either way.
        let to = tempfile::tempdir().expect("dir");
        assert_eq!(code(r.node.restore(&p, to.path())), "unsafe_entry", "{file}");
        assert!(is_empty(to.path()));
    }
}

/// A frame that decompresses past what the manifest declares stops there.
#[tokio::test(flavor = "multi_thread")]
async fn a_decompression_bomb_stops_at_the_declared_size() {
    let r = Rig::new().await;
    let bomb = zstd::encode_all(&vec![0u8; 8 << 20][..], 19).expect("encode");
    let mut e = r.entries.clone();
    let i = e.iter().position(|x| x.path == "queue.db.zst").expect("queue");
    e[i].body = bomb.clone();
    resign(&mut e, &r.node.master, |m| {
        let f = m.files.iter_mut().find(|f| f.path == "queue.db.zst").expect("listed");
        f.size = bomb.len() as u64;
        f.sha256 = sha(&bomb);
        f.raw_size = Some(4096);
    });
    r.refused(&r.write("t.tar", &e), "too_large");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declared_size_past_the_disk_is_refused_before_writing() {
    let r = Rig::new().await;
    let mut e = r.entries.clone();
    resign(&mut e, &r.node.master, |m| {
        let f = m.files.iter_mut().find(|f| f.path == "queue.db.zst").expect("listed");
        f.raw_size = Some(u64::MAX / 2);
    });
    let p = r.write("t.tar", &e);
    let to = tempfile::tempdir().expect("dir");
    assert_eq!(code(r.node.restore(&p, to.path())), "insufficient_storage");
    assert!(is_empty(to.path()));
}

#[tokio::test(flavor = "multi_thread")]
async fn entries_the_manifest_does_not_list_or_lists_twice_are_refused() {
    let r = Rig::new().await;
    let svc = r.node.service;
    // A blob file of a legal shape, not in the manifest.
    let mut e = r.entries.clone();
    e.insert(0, entry(&format!("blobs/{svc}/objects/aa-bb-cc"), b"x"));
    r.refused(&r.write("t.tar", &e), "unsafe_entry");
    // The same entry twice.
    let mut e = r.entries.clone();
    let dup = e[0].clone();
    e.insert(0, dup);
    r.refused(&r.write("t2.tar", &e), "unsafe_entry");
    // The manifest lists one path twice.
    let mut e = r.entries.clone();
    resign(&mut e, &r.node.master, |m| {
        let f = m.files[0].clone();
        m.files.push(f);
    });
    r.refused(&r.write("t3.tar", &e), "unsafe_entry");
    // The manifest lists a file that is not there.
    let mut e = r.entries.clone();
    e.remove(0);
    r.refused(&r.write("t4.tar", &e), "truncated");
}

#[tokio::test(flavor = "multi_thread")]
async fn entries_after_the_trailer_and_a_doubled_manifest_are_refused() {
    let r = Rig::new().await;
    let mut e = r.entries.clone();
    e.push(entry("meta.db.zst", b"x"));
    r.refused(&r.write("t.tar", &e), "unsafe_entry");
    let mut e = r.entries.clone();
    let m = e.iter().find(|x| x.path == MANIFEST).expect("m").clone();
    e.insert(0, m);
    r.refused(&r.write("t2.tar", &e), "unsafe_entry");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_format_or_a_newer_schema_is_refused() {
    let r = Rig::new().await;
    let mut e = r.entries.clone();
    resign(&mut e, &r.node.master, |m| m.format = 99);
    r.refused(&r.write("t.tar", &e), "unsupported_format");

    // meta.db newer than this binary.
    let mut e = r.entries.clone();
    let i = e.iter().position(|x| x.path == "meta.db.zst").expect("db");
    let work = tempfile::tempdir().expect("dir");
    let raw = work.path().join("m.db");
    let mut plain = Vec::new();
    zstd::stream::read::Decoder::new(&e[i].body[..])
        .expect("zstd")
        .read_to_end(&mut plain)
        .expect("decode");
    std::fs::write(&raw, &plain).expect("write");
    rusqlite::Connection::open(&raw)
        .expect("open")
        .execute_batch("PRAGMA user_version = 9999")
        .expect("pragma");
    let plain = std::fs::read(&raw).expect("read");
    let packed = zstd::encode_all(&plain[..], 3).expect("encode");
    e[i].body = packed.clone();
    resign(&mut e, &r.node.master, |m| {
        let f = m.files.iter_mut().find(|f| f.path == "meta.db.zst").expect("listed");
        f.size = packed.len() as u64;
        f.sha256 = sha(&packed);
        f.raw_size = Some(plain.len() as u64);
    });
    let p = r.write("t2.tar", &e);
    let to = tempfile::tempdir().expect("dir");
    assert_eq!(code(r.node.restore(&p, to.path())), "schema_too_new");
    assert!(is_empty(to.path()));
}

/// `--no-auth` skips only the MAC; the shape checks still hold.
#[tokio::test(flavor = "multi_thread")]
async fn no_auth_still_applies_the_structural_checks() {
    let r = Rig::new().await;
    let mut e = r.entries.clone();
    e.insert(0, entry("../evil", b"x"));
    let p = r.write("t.tar", &e);
    let to = tempfile::tempdir().expect("dir");
    let res = restore_archive(
        &p,
        to.path(),
        &RestoreOptions {
            auth: Auth::None,
            revoke_keys_before: None,
            secrets: None,
            now_ms: 1,
        },
    );
    assert_eq!(code(res), "unsafe_entry");
    assert!(is_empty(to.path()));
}

/// A MAC under the previous master key still counts, during a rotation.
#[tokio::test(flavor = "multi_thread")]
async fn a_mac_under_a_listed_previous_key_passes() {
    let r = Rig::new().await;
    let keys = [MasterKey::generate(), r.node.master.clone()];
    verify_archive(&r.good, &Auth::Keys(&keys)).expect("previous key");
    let tag = sign_manifest(&r.node.master, b"x");
    assert_eq!(tag.len(), 64);
}
