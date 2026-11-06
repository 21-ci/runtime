//! `_rt_blobs.file` is not trusted: a crafted row can neither read nor unlink a host file. A restore
//! or import hands this table to someone else's pen (`Path::join` with an absolute name replaces
//! the base). SECURITY.md "Backups".

use crate::common;

use bytes::Bytes;
use common::*;
use rt_store::{blob_file_name_ok, ServiceDataStore};
use rt_types::{Body, ServiceId};

const MAX: u64 = 1 << 30;

fn craft(root: &std::path::Path, s: ServiceId, file: &str) {
    let c = rusqlite::Connection::open(root.join("services").join(s.to_string()).join("data.db")).expect("open");
    c.execute("UPDATE _rt_blobs SET file = ?1", [file]).expect("update");
}

#[test]
fn only_generated_names_pass() {
    assert!(blob_file_name_ok("1a2b-17f00d-0"));
    for bad in [
        "",
        "/data/master.key",
        "../../../master.key",
        "master.key",
        "a/b-1-2",
        "1a-2b",
        "1a-2b-3c-4d",
        "1A-2B-3C",
        "..-..-..",
        "1a-2b-3c\n",
        &"a".repeat(200),
    ] {
        assert!(!blob_file_name_ok(bad), "{bad:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crafted_row_cannot_read_or_unlink_a_host_file() {
    let dir = tempfile::tempdir().expect("dir");
    let victim = dir.path().join("master.key");
    std::fs::write(&victim, "the key").expect("write");
    let store = data(dir.path());
    let s = svc(1);
    let blob = store.blob(s);
    for key in ["read", "del", "over"] {
        blob.put(key, Body::from_bytes(Bytes::from_static(b"x")), None, MAX)
            .await
            .expect("put");
    }
    drop(blob);
    let abs = victim.to_string_lossy().into_owned();
    for file in [abs.as_str(), "../../../master.key"] {
        craft(dir.path(), s, file);
        let blob = store.blob(s);
        assert!(blob.get("read").await.is_err(), "read through {file}");
        blob.put("over", Body::from_bytes(Bytes::from_static(b"y")), None, MAX)
            .await
            .expect("an overwrite leaves the old name alone");
        assert!(victim.exists(), "overwrite through {file}");
        craft(dir.path(), s, file);
        store.blob(s).delete("del").await.expect("delete");
        assert!(victim.exists(), "delete through {file}");
        store
            .blob(s)
            .put("del", Body::from_bytes(Bytes::from_static(b"x")), None, MAX)
            .await
            .expect("put");
    }
    assert_eq!(std::fs::read(&victim).expect("read"), b"the key");
}

/// `copy_data` of a branch walks the same rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_crafted_row_cannot_be_copied_out() {
    let dir = tempfile::tempdir().expect("dir");
    let victim = dir.path().join("master.key");
    std::fs::write(&victim, "the key").expect("write");
    let store = data(dir.path());
    let (from, to) = (svc(1), svc(2));
    store
        .blob(from)
        .put("k", Body::from_bytes(Bytes::from_static(b"x")), None, MAX)
        .await
        .expect("put");
    craft(dir.path(), from, &victim.to_string_lossy());
    assert!(store.sql(from).copy_into(to).await.is_err());
    assert!(victim.exists());
}
