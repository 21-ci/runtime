//! T2 on disk (SEC-21 at unit level): MAC-verified reads, tamper -> reject +
//! delete + rebuild, plus CCH-09 disk watermark GC through the cache.
//!
//! Layout under test: `cache/<engine-fp hex>/<digest hex>/<file>` with a `<file>.mac` sidecar, mode
//! 0600, written atomically.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use parking_lot::Mutex;
use rt_cache::{EvictReason, Lookup, PreparedSource, T2Error, T2Store, TamperSink};
use rt_types::{PreparedKey, RuntimeKind, TaskId};

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<(PreparedKey, String)>>,
}

impl RecordingSink {
    fn events(&self) -> Vec<(PreparedKey, String)> {
        self.events.lock().clone()
    }
}

impl TamperSink for RecordingSink {
    fn tampered(&self, key: &PreparedKey, file: &str) {
        self.events.lock().push((*key, file.to_string()));
    }
}

fn store(root: &std::path::Path) -> (Arc<T2Store>, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    (t2_store(root, 9, sink.clone()), sink)
}

fn mac_path(t2: &T2Store, key: &PreparedKey, file: &str) -> PathBuf {
    let mut p = t2.path_for(key, file).into_os_string();
    p.push(".mac");
    PathBuf::from(p)
}

fn flip_first_byte(path: &std::path::Path) {
    let mut bytes = std::fs::read(path).expect("read file to tamper");
    bytes[0] ^= 0x01;
    std::fs::write(path, bytes).expect("write tampered file");
}

// ---- plain behaviour ---------------------------------------------------------------------------

#[test]
fn ac_t2_01_write_then_read_verified_returns_the_exact_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let k = key(1);
    let content: Vec<u8> = (0..=255u8).cycle().take(100_000).collect();

    t2.write(&k, "component.cwasm", &content).expect("write");
    let got = t2.read_verified(&k, "component.cwasm").expect("read").expect("present");
    assert_eq!(got.as_ref(), content.as_slice());
    assert!(sink.events().is_empty(), "a genuine entry raises no tamper event");

    // Several files per entry coexist (bundle + code cache).
    t2.write(&k, "code_cache.bin", b"cc").expect("write second file");
    assert_eq!(
        t2.read_verified(&k, "code_cache.bin")
            .expect("read")
            .expect("present")
            .as_ref(),
        b"cc"
    );
    assert_eq!(
        t2.read_verified(&k, "component.cwasm")
            .expect("read")
            .expect("present")
            .as_ref(),
        content.as_slice()
    );
}

#[test]
fn ac_t2_02_absent_entries_read_as_none_without_side_effects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    assert_eq!(t2.read_verified(&key(1), "bundle").expect("read"), None);
    assert_eq!(t2.verify_path(&key(1), "bundle").expect("verify"), None);
    t2.write(&key(2), "bundle", b"x").expect("write");
    assert_eq!(
        t2.read_verified(&key(1), "bundle").expect("read"),
        None,
        "other keys do not collide"
    );
    assert_eq!(
        t2.read_verified(&key(2), "other-file").expect("read"),
        None,
        "other file names do not collide"
    );
    assert!(sink.events().is_empty());
}

#[test]
fn ac_t2_03_layout_is_fp_dir_digest_dir_file_with_a_mac_sidecar() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, _) = store(dir.path());
    let k = key(1);
    t2.write(&k, "bundle", b"layout").expect("write");

    // SEC-55: layout v2 lives under `cache/v2/` (was `cache/<fp>/…` directly).
    let entry = dir
        .path()
        .join("v2")
        .join(format!("{:016x}", k.engine_fp))
        .join(k.digest.hex());
    assert_eq!(t2.entry_dir(&k), entry);
    assert_eq!(t2.path_for(&k, "bundle"), entry.join("bundle"));
    assert_eq!(t2.root(), dir.path());
    assert!(entry.join("bundle").is_file());
    let mac = std::fs::metadata(mac_path(&t2, &k, "bundle")).expect("`<file>.mac` sidecar must exist");
    assert!(
        mac.len() >= 32,
        "the sidecar holds a 32-byte HMAC-SHA256 tag, got {} bytes",
        mac.len()
    );
}

#[test]
fn ac_t2_04_files_are_private_and_writes_leave_no_temp_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, _) = store(dir.path());
    let k = key(1);
    t2.write(&k, "bundle", b"private").expect("write");

    for p in [t2.path_for(&k, "bundle"), mac_path(&t2, &k, "bundle")] {
        let mode = std::fs::metadata(&p).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "{p:?} must be mode 0600");
    }
    let mut names: Vec<String> = std::fs::read_dir(t2.entry_dir(&k))
        .expect("read entry dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["bundle", "bundle.mac"],
        "atomic writes (tmp + rename) must not leave temp files"
    );
}

#[test]
fn ac_t2_05_overwrite_replaces_content_and_mac_together() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let k = key(1);
    t2.write(&k, "bundle", b"version one").expect("write 1");
    t2.write(&k, "bundle", b"version two, longer").expect("write 2");
    assert_eq!(
        t2.read_verified(&k, "bundle").expect("read").expect("present").as_ref(),
        b"version two, longer"
    );
    assert!(
        sink.events().is_empty(),
        "the replaced MAC matches the replaced content"
    );
}

#[test]
fn ac_t2_06_total_bytes_list_and_remove() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, _) = store(dir.path());
    assert_eq!(t2.total_bytes().expect("empty total"), 0);
    assert!(t2.list().expect("empty list").is_empty());

    let keys = [key(1), key(2), key(3)];
    for (k, len) in keys.iter().zip([1000usize, 2000, 3000]) {
        t2.write(k, "bundle", &vec![7u8; len]).expect("write");
    }
    let total = t2.total_bytes().expect("total");
    assert!(
        (6000..=6000 + 3 * 512).contains(&total),
        "payload 6000 B + small MAC sidecars, got {total}"
    );

    let mut listed = t2.list().expect("list");
    listed.sort_by_key(|e| e.bytes);
    assert_eq!(listed.len(), 3);
    for (e, (k, len)) in listed.iter().zip(keys.iter().zip([1000u64, 2000, 3000])) {
        assert_eq!((e.engine_fp, e.digest), (k.engine_fp, k.digest));
        assert!(
            e.bytes >= len && e.bytes <= len + 512,
            "entry bytes {} for payload {len}",
            e.bytes
        );
    }

    t2.remove(&keys[1]).expect("remove");
    assert_eq!(t2.read_verified(&keys[1], "bundle").expect("read"), None);
    assert!(
        !t2.path_for(&keys[1], "bundle").exists() && !mac_path(&t2, &keys[1], "bundle").exists(),
        "all files of the entry are gone"
    );
    assert_eq!(t2.list().expect("list").len(), 2);
    assert!(t2.total_bytes().expect("total") < total - 2000 + 1);
    t2.remove(&keys[0]).expect("remove");
    t2.remove(&keys[2]).expect("remove");
    assert_eq!(t2.total_bytes().expect("total"), 0);
}

// ---- SEC-21: tamper -> reject ----------------------------------------------------------------------

#[test]
fn ac_sec_21_a_flipped_payload_byte_is_rejected_deleted_and_reported() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let k = key(1);
    t2.write(&k, "bundle", b"genuine compiled code").expect("write");
    flip_first_byte(&t2.path_for(&k, "bundle"));

    let err = t2
        .read_verified(&k, "bundle")
        .expect_err("tampered bytes must not be returned");
    assert!(matches!(err, T2Error::Tampered), "got {err:?}");

    assert!(
        !t2.entry_dir(&k).exists() || t2.list().expect("list").is_empty(),
        "the bad entry is deleted"
    );
    assert!(!t2.path_for(&k, "bundle").exists());
    let events = sink.events();
    assert_eq!(events.len(), 1, "exactly one tamper event: {events:?}");
    assert_eq!(events[0].0, k);
    assert!(
        events[0].1.ends_with("bundle"),
        "event names the file, got {:?}",
        events[0].1
    );

    // Afterwards the entry is simply absent (it will be rebuilt from T3).
    assert_eq!(t2.read_verified(&k, "bundle").expect("read"), None);
    assert_eq!(sink.events().len(), 1, "an absent entry is not a tamper");
}

#[test]
fn ac_sec_21_truncation_and_appended_bytes_are_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    for (n, name) in [(1u64, "truncated"), (2, "appended")] {
        let k = key(n);
        t2.write(&k, "bundle", b"0123456789abcdef").expect("write");
        let p = t2.path_for(&k, "bundle");
        let mut bytes = std::fs::read(&p).expect("read");
        if name == "truncated" {
            bytes.truncate(8);
        } else {
            bytes.extend_from_slice(b"EVIL");
        }
        std::fs::write(&p, bytes).expect("write tampered");
        assert!(
            matches!(t2.read_verified(&k, "bundle"), Err(T2Error::Tampered)),
            "{name} file must fail verification"
        );
    }
    assert_eq!(sink.events().len(), 2);
}

#[test]
fn ac_sec_21_a_flipped_mac_byte_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let k = key(1);
    t2.write(&k, "bundle", b"payload").expect("write");
    flip_first_byte(&mac_path(&t2, &k, "bundle"));
    assert!(matches!(t2.read_verified(&k, "bundle"), Err(T2Error::Tampered)));
    assert_eq!(sink.events().len(), 1);
    assert_eq!(t2.read_verified(&k, "bundle").expect("read"), None);
}

#[test]
fn ac_sec_21_a_missing_mac_file_is_a_failure_not_a_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let k = key(1);
    t2.write(&k, "bundle", b"payload").expect("write");
    std::fs::remove_file(mac_path(&t2, &k, "bundle")).expect("delete the MAC");

    assert!(
        matches!(t2.read_verified(&k, "bundle"), Err(T2Error::Tampered)),
        "unsigned artifacts are never loaded"
    );
    assert_eq!(sink.events().len(), 1);
    assert!(!t2.path_for(&k, "bundle").exists(), "the unsigned file is deleted");
}

#[test]
fn ac_sec_21_a_validly_signed_file_moved_to_another_key_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let (victim, source) = (key(1), key(2));
    t2.write(&victim, "bundle", b"victim code").expect("write victim");
    t2.write(&source, "bundle", b"attacker code").expect("write source");

    // Copy the attacker's bytes *and* its (valid for its own key) MAC over the victim's files.
    std::fs::copy(t2.path_for(&source, "bundle"), t2.path_for(&victim, "bundle")).expect("copy bytes");
    std::fs::copy(mac_path(&t2, &source, "bundle"), mac_path(&t2, &victim, "bundle")).expect("copy mac");

    assert!(
        matches!(t2.read_verified(&victim, "bundle"), Err(T2Error::Tampered)),
        "the MAC is bound to the PreparedKey"
    );
    // The genuine entry for the source key still verifies.
    assert_eq!(
        t2.read_verified(&source, "bundle")
            .expect("read")
            .expect("present")
            .as_ref(),
        b"attacker code"
    );
    assert_eq!(sink.events().len(), 1);
    assert_eq!(sink.events()[0].0, victim);
}

#[test]
fn ac_sec_21_the_mac_is_bound_to_engine_fingerprint_and_runtime_kind() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, _) = store(dir.path());
    let a = key(1);
    t2.write(&a, "bundle", b"engine A").expect("write");

    // Same bytes and MAC under another engine fingerprint (different directory): rejected.
    let other_engine = PreparedKey {
        engine_fp: a.engine_fp + 1,
        ..a
    };
    std::fs::create_dir_all(t2.entry_dir(&other_engine)).expect("mkdir");
    std::fs::copy(t2.path_for(&a, "bundle"), t2.path_for(&other_engine, "bundle")).expect("copy bytes");
    std::fs::copy(mac_path(&t2, &a, "bundle"), mac_path(&t2, &other_engine, "bundle")).expect("copy mac");
    let res = t2.read_verified(&other_engine, "bundle");
    assert!(
        matches!(res, Err(T2Error::Tampered)),
        "engine fp: a MAC for another key must not verify, got {res:?}"
    );
    assert_eq!(
        t2.read_verified(&a, "bundle").expect("read").expect("present").as_ref(),
        b"engine A",
        "the genuine entry is unaffected"
    );

    // The runtime kind is not part of the path: the very same files read under the other kind must fail.
    let other_kind = PreparedKey {
        kind: RuntimeKind::Wasm,
        ..a
    };
    let res = t2.read_verified(&other_kind, "bundle");
    assert!(
        matches!(res, Err(T2Error::Tampered)),
        "runtime kind: a MAC for another key must not verify, got {res:?}"
    );
}

#[test]
fn ac_sec_21_entries_signed_under_another_master_key_are_rejected() {
    // Master-key rotation invalidates T2 (lazy rebuild).
    let dir = tempfile::tempdir().expect("tempdir");
    let old = t2_store(dir.path(), 1, Arc::new(rt_cache::NoTamperSink));
    let k = key(1);
    old.write(&k, "bundle", b"signed with the old key").expect("write");
    assert!(old.read_verified(&k, "bundle").expect("read").is_some());

    let sink = Arc::new(RecordingSink::default());
    let rotated = t2_store(dir.path(), 2, sink.clone());
    assert!(matches!(rotated.read_verified(&k, "bundle"), Err(T2Error::Tampered)));
    assert_eq!(sink.events().len(), 1);
}

#[test]
fn ac_sec_21_verify_path_returns_the_path_only_after_verification() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let (good, bad) = (key(1), key(2));
    t2.write(&good, "component.cwasm", b"mmap me").expect("write good");
    t2.write(&bad, "component.cwasm", b"mmap me too").expect("write bad");
    flip_first_byte(&t2.path_for(&bad, "component.cwasm"));

    assert_eq!(
        t2.verify_path(&good, "component.cwasm").expect("verify"),
        Some(t2.path_for(&good, "component.cwasm"))
    );
    let err = t2
        .verify_path(&bad, "component.cwasm")
        .expect_err("tampered file must not be handed to mmap");
    assert!(matches!(err, T2Error::Tampered), "got {err:?}");
    assert!(
        !t2.path_for(&bad, "component.cwasm").exists(),
        "the tampered file is deleted"
    );
    assert_eq!(sink.events().len(), 1);
}

// ---- SEC-55: file-name binding and layout migration -------------------------------------------------

#[test]
fn ac_sec_55_a_file_renamed_inside_an_entry_fails_verification() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let k = key(1);
    t2.write(&k, "bundle", b"the bundle").expect("write bundle");
    t2.write(&k, "code_cache.bin", b"the code cache").expect("write cache");

    // Swap a validly signed file (and its sidecar) into another name of the same entry.
    std::fs::copy(t2.path_for(&k, "bundle"), t2.path_for(&k, "code_cache.bin")).expect("copy bytes");
    std::fs::copy(mac_path(&t2, &k, "bundle"), mac_path(&t2, &k, "code_cache.bin")).expect("copy mac");
    assert!(
        matches!(t2.read_verified(&k, "code_cache.bin"), Err(T2Error::Tampered)),
        "the tag is bound to the file name"
    );
    assert_eq!(sink.events().len(), 1);
    assert_eq!(sink.events()[0].1, "code_cache.bin");

    // A file renamed to a name that was never written is rejected the same way.
    let k2 = key(2);
    t2.write(&k2, "bundle", b"bundle two").expect("write");
    std::fs::rename(t2.path_for(&k2, "bundle"), t2.path_for(&k2, "snapshot.bin")).expect("rename");
    std::fs::rename(mac_path(&t2, &k2, "bundle"), mac_path(&t2, &k2, "snapshot.bin")).expect("rename mac");
    assert!(matches!(t2.read_verified(&k2, "snapshot.bin"), Err(T2Error::Tampered)));
    assert_eq!(sink.events().len(), 2);
}

/// A legacy-layout entry (`cache/<fp>/<digest>/<file>` + `.mac`) as an older node left it.
fn legacy_entry(root: &std::path::Path, k: &PreparedKey, file: &str, bytes: &[u8]) -> PathBuf {
    let dir = root.join(format!("{:016x}", k.engine_fp)).join(k.digest.hex());
    std::fs::create_dir_all(&dir).expect("mkdir legacy");
    std::fs::write(dir.join(file), bytes).expect("legacy file");
    std::fs::write(dir.join(format!("{file}.mac")), [7u8; 32]).expect("legacy mac");
    root.join(format!("{:016x}", k.engine_fp))
}

#[test]
fn ac_sec_55_legacy_directories_are_removed_at_startup_without_tamper_events() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (legacy_a, legacy_b) = (
        key(1),
        PreparedKey {
            engine_fp: 0xBEEF,
            ..key(2)
        },
    );
    let a_dir = legacy_entry(dir.path(), &legacy_a, "bundle", b"old bundle");
    let b_dir = legacy_entry(dir.path(), &legacy_b, "component.cwasm", b"old cwasm");
    std::fs::write(dir.path().join("README"), b"not ours").expect("unrelated file");

    let (t2, sink) = store(dir.path());
    let report = t2.prepare().expect("prepare");
    assert_eq!(
        report,
        rt_cache::T2Prepared {
            legacy_dirs_removed: 2,
            stale_key_dirs_removed: 0
        }
    );
    assert!(
        !a_dir.exists() && !b_dir.exists(),
        "legacy engine-fingerprint directories are gone"
    );
    assert!(
        dir.path().join("README").exists(),
        "only legacy T2 directories are touched"
    );
    assert!(
        sink.events().is_empty(),
        "a legacy entry is stale, not tampered: {:?}",
        sink.events()
    );

    // The legacy entry is simply absent now (rebuilt from T3); it was never verified.
    assert_eq!(t2.read_verified(&legacy_a, "bundle").expect("read"), None);
    assert!(sink.events().is_empty());
    let fresh = key(3);
    t2.write(&fresh, "bundle", b"v2 bundle").expect("write v2");
    assert_eq!(t2.list().expect("list").len(), 1);

    // Running it again (next restart) keeps the v2 entries of the same key.
    assert_eq!(t2.prepare().expect("prepare again"), rt_cache::T2Prepared::default());
    assert_eq!(
        t2.read_verified(&fresh, "bundle")
            .expect("read")
            .expect("present")
            .as_ref(),
        b"v2 bundle"
    );
}

#[test]
fn ac_sec_55_prepare_drops_entries_of_another_mac_key_quietly_and_keeps_its_own() {
    // Master-key rotation: entries signed under the old key are dropped at startup instead of
    // each raising a tamper event on first read.
    let dir = tempfile::tempdir().expect("tempdir");
    let old = t2_store(dir.path(), 1, Arc::new(rt_cache::NoTamperSink));
    old.prepare().expect("prepare old");
    let k = key(1);
    old.write(&k, "bundle", b"signed with the old key").expect("write");
    assert_eq!(old.prepare().expect("same key").stale_key_dirs_removed, 0);
    assert!(old.read_verified(&k, "bundle").expect("read").is_some());

    let sink = Arc::new(RecordingSink::default());
    let rotated = t2_store(dir.path(), 2, sink.clone());
    assert_eq!(rotated.prepare().expect("prepare rotated").stale_key_dirs_removed, 1);
    assert_eq!(
        rotated.read_verified(&k, "bundle").expect("read"),
        None,
        "absent, not tampered"
    );
    assert!(sink.events().is_empty());
    rotated.write(&k, "bundle", b"rebuilt").expect("rebuild");
    assert_eq!(
        rotated
            .read_verified(&k, "bundle")
            .expect("read")
            .expect("present")
            .as_ref(),
        b"rebuilt"
    );
}

// ---- SEC-21 through the cache: reject + reload -------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_sec_21_cache_never_serves_tampered_bytes_and_rebuilds_the_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, sink) = store(dir.path());
    let k = key(1);
    t2.write(&k, BUNDLE, b"genuine-v1").expect("write");
    flip_first_byte(&t2.path_for(&k, BUNDLE));

    let r = rig(
        FakeLoader::with_disk(t2.clone(), Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );

    // The tampered artifact is never turned into a T1 value: the task waits for a rebuild instead.
    assert!(matches!(r.cache.lookup(&k, TaskId(1)), Lookup::Pending));
    let t2c = t2.clone();
    eventually("the tampered T2 entry to be deleted", move || {
        !t2c.path_for(&k, BUNDLE).exists()
    })
    .await;
    assert_eq!(r.loader.created(), 0, "no value may be built from tampered bytes");
    assert!(!r.cache.is_ready(&k));
    assert!(!r.cache.contains(&k));
    assert_eq!(sink.events().len(), 1, "security.artifact_tamper is reported once");

    // The rebuild from T3 writes a fresh, signed T2 entry; completion promotes it to T1.
    t2.write(&k, BUNDLE, b"genuine-v2").expect("rebuild writes T2");
    r.cache.on_build_done(&k, Ok(()));
    let v = get(&r.cache, &k, 2).await;
    assert_eq!(v.payload.as_ref(), b"genuine-v2", "only the rebuilt bytes are served");
    assert_eq!(r.loader.created(), 1);
    assert_eq!(sink.events().len(), 1, "no further tamper events");
}

// ---- CCH-09: disk watermark GC ------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_09_below_the_low_watermark_gc_removes_lowest_score_t2_entries_until_above_high() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, _) = store(dir.path());
    let keys: Vec<PreparedKey> = (0..10).map(key).collect();
    for (i, k) in keys.iter().enumerate() {
        t2.write(k, BUNDLE, &[i as u8; 100]).expect("write T2 entry");
    }
    let mut cfg = settings(1_000_000, Duration::from_secs(600));
    cfg.disk_low_free = 1000;
    cfg.disk_high_free = 1500;
    let r = rig_with(FakeLoader::with_disk(t2.clone(), Duration::ZERO), cfg, 100);
    r.cache.attach_t2(t2.clone());

    // Keys 0..3 are hot (high frequency), 3..10 cold. Key 9 is in use by a running task.
    for k in &keys {
        request_then_wait_ready(&r.cache, k).await;
    }
    for k in &keys[0..3] {
        for _ in 0..10 {
            hit(&r.cache, k);
        }
    }
    let in_use = hit(&r.cache, &keys[9]);
    assert_eq!(r.cache.stats().t2_entries, 10);

    // Plenty of free space (>= low watermark): nothing happens.
    assert_eq!(r.cache.gc_disk(1200).evicted, 0);
    assert_eq!(t2.list().expect("list").len(), 10);

    // Free space 950 < low (1000): delete lowest-score entries until free > high (1500), i.e. free
    // more than 550 bytes -> 5 or 6 entries of ~100 bytes each.
    let report = r.cache.gc_disk(950);
    assert!(
        report.bytes_freed > 550,
        "must free enough to get above the high watermark, freed {}",
        report.bytes_freed
    );
    assert!((5..=7).contains(&report.evicted), "removed {} entries", report.evicted);

    let remaining = t2.list().expect("list");
    assert_eq!(remaining.len() as u32, 10 - report.evicted);
    assert_eq!(r.cache.stats().t2_entries as usize, remaining.len());
    for k in &keys[0..3] {
        assert!(
            t2.read_verified(k, BUNDLE).expect("read").is_some(),
            "hot entry {k:?} must survive GC"
        );
    }
    assert!(
        t2.read_verified(&keys[9], BUNDLE).expect("read").is_some(),
        "an entry in use is kept"
    );
    drop(in_use);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_09_disk_gc_counts_its_evictions_with_reason_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, _) = store(dir.path());
    let k = key(1);
    t2.write(&k, BUNDLE, &[1u8; 100]).expect("write");
    let mut cfg = settings(1_000_000, Duration::from_secs(600));
    cfg.disk_low_free = 1000;
    cfg.disk_high_free = 1500;
    let r = rig_with(FakeLoader::with_disk(t2.clone(), Duration::ZERO), cfg, 100);
    r.cache.attach_t2(t2.clone());
    request_then_wait_ready(&r.cache, &k).await;

    let report = r.cache.gc_disk(10);
    assert_eq!(report.evicted, 1, "the only (idle) entry is the lowest-score one");
    assert_eq!(
        r.cache.eviction_count(EvictReason::Disk),
        1,
        "T2 GC evictions are counted with reason `disk`"
    );
    assert_eq!(r.cache.eviction_count(EvictReason::Pressure), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_gc_keeps_the_t2_entry_of_a_build_in_flight() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (t2, _) = store(dir.path());
    let mut cfg = settings(1_000_000, Duration::from_secs(600));
    cfg.disk_low_free = 1000;
    cfg.disk_high_free = 1500;
    let r = rig_with(FakeLoader::with_disk(t2.clone(), Duration::ZERO), cfg, 100);
    r.cache.attach_t2(t2.clone());
    let k = key(1);
    // Nothing in T2: the request starts a build (no scheduler bound, so it stays in flight).
    assert!(matches!(r.cache.lookup(&k, TaskId(1)), Lookup::Pending));
    eventually("the build to be requested", || r.loader.build_specs() == 1).await;
    // The build wrote its artifact and has not promoted it yet; the disk runs low meanwhile.
    t2.write(&k, BUNDLE, &[1u8; 100]).expect("write the built artifact");
    let report = r.cache.gc_disk(10);
    assert_eq!(report.evicted, 0, "an in-flight build's T2 entry is kept");
    assert!(t2.read_verified(&k, BUNDLE).expect("read").is_some());
}
