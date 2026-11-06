//! `/v1/node/backups*` on a real node, and the restore drill: back up a live node, restore into a
//! new data directory, start a node on it and use it. SECURITY.md "Backups".

use std::path::PathBuf;
use std::time::Duration;

use reqwest::Method;
use rt_backup::restore::{restore_archive, verify_archive, Auth, RestoreOptions};
use rt_sec::MasterKey;
use serde_json::{json, Value};

use crate::common::*;

/// Keeps a value in KV and a blob, and a row in SQL; reads them back.
const DRILL: &str = r#"
import { kv } from "std:kv";
export default {
  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/write") {
      await kv.set("drill", "kept");
      return new Response("written");
    }
    return new Response(String(await kv.get("drill")));
  },
};
"#;

fn on(c: &mut rt_config::Config) {
    c.backup.enabled = true;
}

async fn wait_ok(node: &impl Node, id: &str) -> Value {
    let path = format!("/v1/node/backups/{id}");
    for _ in 0..1200 {
        let r = call(root(node, Method::GET, &path), None).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        match r.json["state"].as_str() {
            Some("ok") => return r.json,
            Some("failed") => panic!("backup failed: {}", r.text),
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("backup did not finish");
}

async fn back_up(node: &impl Node) -> String {
    let r = call(root(node, Method::POST, "/v1/node/backups"), None).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    assert_eq!(r.json["state"], json!("running"));
    let id = r.json["id"].as_str().expect("id").to_string();
    wait_ok(node, &id).await;
    id
}

/// Off by default, and says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backups_are_off_until_enabled() {
    let node = start_node().await;
    let r = call(root(&node, Method::POST, "/v1/node/backups"), None).await;
    assert_eq!((r.s(), r.code().as_str()), (503, "backup_disabled"));
    let r = call(root(&node, Method::GET, "/v1/node/backups/status"), None).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["enabled"], json!(false));
    assert_eq!(r.json["next_at"], Value::Null);
    assert_eq!(r.json["destination"]["kind"], json!("local"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenant_key_reaches_none_of_it() {
    let node = start_node_with(on).await;
    let t = new_tenant(&node, "pro").await;
    let id = back_up(&node).await;
    for (m, p) in [
        (Method::POST, "/v1/node/backups".to_string()),
        (Method::GET, "/v1/node/backups".to_string()),
        (Method::GET, "/v1/node/backups/status".to_string()),
        (Method::GET, format!("/v1/node/backups/{id}")),
        (Method::GET, format!("/v1/node/backups/{id}/archive")),
        (Method::DELETE, format!("/v1/node/backups/{id}")),
    ] {
        let r = call(node.http().request(m, node.control_url(&p)).bearer_auth(&t.key), None).await;
        assert_eq!((r.s(), r.code().as_str()), (403, "insufficient_scope"), "{p}");
    }
    // Still there.
    assert_eq!(
        call(root(&node, Method::GET, &format!("/v1/node/backups/{id}")), None)
            .await
            .s(),
        200
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_routes_list_stream_with_range_and_delete() {
    let node = start_node_with(on).await;
    let id = back_up(&node).await;

    let r = call(root(&node, Method::GET, "/v1/node/backups"), None).await;
    assert_eq!(r.json["items"][0]["id"], json!(id));
    assert_eq!(r.json["items"][0]["trigger"], json!("api"));
    assert!(r.json["items"][0]["verified_at"].is_i64());
    assert!(
        r.json["items"][0].get("local_path").is_none(),
        "no host path in the API"
    );

    let r = call(root(&node, Method::GET, "/v1/node/backups/status"), None).await;
    assert_eq!(r.json["last_ok_id"], json!(id));
    assert!(r.json["next_at"].is_i64());

    let whole = root(&node, Method::GET, &format!("/v1/node/backups/{id}/archive"))
        .send()
        .await
        .expect("send");
    assert_eq!(whole.status(), 200);
    let etag = whole
        .headers()
        .get("etag")
        .expect("etag")
        .to_str()
        .expect("str")
        .to_string();
    let bytes = whole.bytes().await.expect("bytes");
    assert!(bytes.len() > 512);
    let part = root(&node, Method::GET, &format!("/v1/node/backups/{id}/archive"))
        .header("range", "bytes=512-1023")
        .header("if-range", etag)
        .send()
        .await
        .expect("send");
    assert_eq!(part.status(), 206);
    assert_eq!(&part.bytes().await.expect("bytes")[..], &bytes[512..1024]);

    let r = call(root(&node, Method::DELETE, &format!("/v1/node/backups/{id}")), None).await;
    assert_eq!(r.s(), 204);
    let r = call(
        root(&node, Method::GET, &format!("/v1/node/backups/{id}/archive")),
        None,
    )
    .await;
    assert_eq!((r.s(), r.code().as_str()), (404, "not_found"));
    let r = call(root(&node, Method::DELETE, "/v1/node/backups/bkp_nope"), None).await;
    assert_eq!((r.s(), r.code().as_str()), (404, "not_found"));
    let r = call(root(&node, Method::GET, "/v1/node/backups/bkp_nope"), None).await;
    assert_eq!((r.s(), r.code().as_str()), (404, "not_found"));
}

/// The console binding: explicit paths only. Download and delete are out of its reach, including
/// by the `{id}` of its paths. Guards the list the operations docs give.
#[test]
fn the_console_binding_paths_do_not_reach_download_or_delete() {
    let paths: Vec<String> = rt_api::backups::CONSOLE_BINDING_PATHS
        .iter()
        .map(|p| (*p).to_string())
        .collect();
    assert!(rt_host::ConfigBindings::path_allowed(&paths, "/v1/node/backups"));
    assert!(rt_host::ConfigBindings::path_allowed(&paths, "/v1/node/backups/status"));
    assert!(paths.iter().all(|p| !p.contains('*') && !p.contains('?')));
    let id = "bkp_01ARZ3NDEKTSV4RRFFQ69G5FAV";
    assert!(!rt_host::ConfigBindings::path_allowed(
        &paths,
        &format!("/v1/node/backups/{id}/archive")
    ));
    assert!(!rt_host::ConfigBindings::path_allowed(
        &paths,
        &format!("/v1/node/backups/{id}")
    ));
    // What the old glob would have let through: the reason for the list.
    let glob = vec!["/v1/node/*".to_string()];
    assert!(rt_host::ConfigBindings::path_allowed(
        &glob,
        &format!("/v1/node/backups/{id}/archive")
    ));
}

/// A restored `ok` row whose file is not on this node: `410`.
fn archive_in(dir: &std::path::Path) -> PathBuf {
    std::fs::read_dir(dir.join("backups"))
        .expect("backups dir")
        .map(|e| e.expect("entry").path())
        .find(|p| p.extension().is_some_and(|x| x == "tar"))
        .expect("an archive")
}

/// The acceptance scenario: seed a service, back it up, restore to a new directory, start a node on
/// it: same data, same key, same live version, the `node.restored` event, and no old archive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_drill_on_a_real_node() {
    let mut node = Restartable::boot(on).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("drill");
    let svc = ship(&node, &t.key, &slug, DRILL).await;
    let r = invoke(&node, &slug, "/write").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "written"));
    let secret = call(
        node.http()
            .put(node.control_url(&format!("/v1/services/{svc}/secrets/TOKEN")))
            .bearer_auth(&t.key),
        Some(json!({"value": "s3cret"})),
    )
    .await;
    assert!(secret.s() < 300, "{}", secret.text);

    let id = back_up(&node).await;
    let archive = archive_in(node.data_dir());
    let master_hex = node.secrets.master_key.as_ref().map(|k| k.expose().to_string());
    let master = match master_hex {
        Some(h) => MasterKey::from_hex(&h).expect("hex"),
        None => MasterKey::from_hex(
            std::fs::read_to_string(node.data_dir().join("master.key"))
                .expect("key")
                .trim(),
        )
        .expect("hex"),
    };
    let keys = [master.clone()];
    verify_archive(&archive, &Auth::Keys(&keys)).expect("verify");
    // The master key file is not in the archive.
    let listing = std::fs::read(&archive).expect("read");
    assert!(!listing.windows(10).any(|w| w == b"master.key"));

    node.stop().await;
    let target = tempfile::tempdir().expect("dir");
    let report = restore_archive(
        &archive,
        target.path(),
        &RestoreOptions {
            auth: Auth::Keys(&keys),
            revoke_keys_before: None,
            secrets: None,
            now_ms: chrono::Utc::now().timestamp_millis(),
        },
    )
    .expect("restore");
    assert_eq!(report.id, id);
    assert!(report.active_keys >= 1);
    // The key comes from outside, as an operator would supply it.
    if node.secrets.master_key.is_none() {
        std::fs::copy(node.data_dir().join("master.key"), target.path().join("master.key")).expect("key");
    }

    node.config.node.data_dir = target.path().to_string_lossy().into_owned();
    node.start().await;

    let r = invoke(&node, &slug, "/").await;
    assert_eq!(
        (r.s(), r.text.as_str()),
        (200, "kept"),
        "the KV value and the live version are back"
    );
    let r = call(
        node.http()
            .get(node.control_url(&format!("/v1/services/{svc}")))
            .bearer_auth(&t.key),
        None,
    )
    .await;
    assert_eq!(r.s(), 200, "the tenant key works under the same master key: {}", r.text);
    let r = call(root(&node, Method::GET, "/v1/node/backups/status"), None).await;
    assert!(r.json["last_restored_at"].is_i64(), "{}", r.text);
    // The row came along, the file did not.
    let r = call(
        root(&node, Method::GET, &format!("/v1/node/backups/{id}/archive")),
        None,
    )
    .await;
    assert_eq!(r.s(), 410, "{}", r.text);
    assert_eq!(r.code(), "archive_gone");
    node.stop().await;
}

/// `--revoke-keys-before` takes effect on the restored node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_revoked_at_restore_stops_working() {
    let mut node = Restartable::boot(on).await;
    let t = new_tenant(&node, "pro").await;
    back_up(&node).await;
    let archive = archive_in(node.data_dir());
    node.stop().await;
    let target = tempfile::tempdir().expect("dir");
    restore_archive(
        &archive,
        target.path(),
        &RestoreOptions {
            auth: Auth::None,
            revoke_keys_before: Some(chrono::Utc::now().timestamp_millis()),
            secrets: None,
            now_ms: chrono::Utc::now().timestamp_millis(),
        },
    )
    .expect("restore");
    node.config.node.data_dir = target.path().to_string_lossy().into_owned();
    node.start().await;
    let r = call(node.http().get(node.control_url("/v1/me")).bearer_auth(&t.key), None).await;
    assert_eq!(r.s(), 401, "{}", r.text);
    node.stop().await;
}

/// `rt backup verify|restore` as the binary runs them: exit codes, and a refusal leaves no data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_offline_commands_answer_with_exit_codes() {
    use rt_server::backup::run_offline;
    use rt_server::BackupCommand::{Restore, Verify};

    let mut node = Restartable::boot(on).await;
    new_tenant(&node, "pro").await;
    back_up(&node).await;
    let archive = archive_in(node.data_dir());
    node.stop().await;
    let (config, secrets) = (node.config.clone(), node.secrets.clone());
    let verify = |archive: PathBuf, no_auth| Verify {
        archive,
        no_auth,
        age_identity: None,
    };
    assert_eq!(run_offline(&verify(archive.clone(), false), &config, &secrets), 0);

    let dir = tempfile::tempdir().expect("dir");
    let garbage = dir.path().join("garbage.tar");
    std::fs::write(&garbage, b"not an archive").expect("write");
    assert_eq!(run_offline(&verify(garbage, true), &config, &secrets), 1);

    let restore = |data_dir: PathBuf, revoke: Option<&str>| Restore {
        archive: archive.clone(),
        data_dir,
        no_auth: false,
        revoke_keys_before: revoke.map(str::to_string),
        age_identity: None,
    };
    let busy = dir.path().join("busy");
    std::fs::create_dir_all(&busy).expect("dir");
    std::fs::write(busy.join("keep"), "x").expect("write");
    assert_eq!(run_offline(&restore(busy.clone(), None), &config, &secrets), 1);
    assert!(!busy.join("meta.db").exists());
    let target = dir.path().join("new");
    assert_eq!(
        run_offline(&restore(target.clone(), Some("soon")), &config, &secrets),
        1
    );
    assert!(!target.join("meta.db").exists());
    assert_eq!(
        run_offline(
            &restore(target.clone(), Some("2020-01-01T00:00:00Z")),
            &config,
            &secrets
        ),
        0
    );
    assert!(target.join("meta.db").exists());
}
