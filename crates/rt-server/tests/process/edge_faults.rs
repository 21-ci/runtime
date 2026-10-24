//! Crash and filesystem edge cases with the real `rt` binary (scratchpad edge-cases.md): a
//! `kill -9` in the middle of `rt secrets reseal`, and internal socket paths that are not plain
//! files.

use std::process::{Command, Stdio};
use std::time::Duration;

use rt_sec::{secret_aad, LocalSecrets, MasterKey, Sealed, SecretsBackend};
use rt_store::{MetaStore, SqliteMetaStore, TenantScope};
use rt_types::{NewService, NewTenant, RuntimeKind, ServiceId, TenantId};

fn hexed(b: u8) -> String {
    hex::encode([b; 32])
}

fn backend(b: u8) -> LocalSecrets {
    LocalSecrets::new(&MasterKey::from_bytes([b; 32]))
}

async fn populate(dir: &std::path::Path, services: usize, per_service: usize) -> (TenantId, Vec<ServiceId>) {
    let meta = SqliteMetaStore::open(&dir.join("meta.db")).expect("meta.db");
    let tenant = meta
        .create_tenant(NewTenant {
            name: "t".into(),
            plan: "free".into(),
            overrides: None,
        })
        .await
        .expect("tenant");
    let old = backend(1);
    let mut ids = Vec::new();
    for i in 0..services {
        let new = NewService {
            slug: format!("s{i}"),
            runtime: RuntimeKind::Wasm,
            name: None,
            profile: None,
            environment: rt_types::default_environment(),
            project: rt_types::default_project(),
            config: Default::default(),
        };
        let svc = meta
            .create_service(TenantScope::Operator, tenant.id, new)
            .await
            .expect("service")
            .id;
        for j in 0..per_service {
            let name = format!("S{j}");
            let sealed = old.seal(&secret_aad(tenant.id, svc, &name), format!("v-{i}-{j}").as_bytes());
            meta.put_secret(svc, &name, sealed.to_bytes()).await.expect("put");
        }
        ids.push(svc);
    }
    (tenant.id, ids)
}

fn reseal_cmd(cfg: &std::path::Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_rt"));
    c.args(["secrets", "reseal", "--config"])
        .arg(cfg)
        .env("RT_ROOT_TOKEN", "x")
        .env("RT_MASTER_KEY", hexed(2))
        .env("RT_MASTER_KEY_PREVIOUS", hexed(1));
    c
}

/// RESEAL-002: `kill -9` in the middle of a reseal leaves every value readable (with the current
/// or the previous key); a rerun finishes the job and nothing is lost.
#[tokio::test]
async fn reseal_002_a_reseal_killed_half_way_loses_nothing() {
    let dir = tempfile::tempdir().expect("tmp");
    let (tenant, services) = populate(dir.path(), 40, 50).await;
    let cfg = dir.path().join("rt.toml");
    std::fs::write(&cfg, format!("[node]\ndata_dir = \"{}\"\n", dir.path().display())).expect("config");

    for delay_ms in [5u64, 40, 120] {
        let mut child = reseal_cmd(&cfg)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn");
        std::thread::sleep(Duration::from_millis(delay_ms));
        let _ = Command::new("kill").arg("-9").arg(child.id().to_string()).status();
        let _ = child.wait();
        // Mid-way: every value still opens with one of the two keys.
        check(dir.path(), tenant, &services, false).await;
    }
    let out = reseal_cmd(&cfg).output().expect("rerun");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    check(dir.path(), tenant, &services, true).await;
}

async fn check(dir: &std::path::Path, tenant: TenantId, services: &[ServiceId], all_current: bool) {
    let meta = SqliteMetaStore::open(&dir.join("meta.db")).expect("meta.db");
    let (new, old) = (backend(2), backend(1));
    for (i, svc) in services.iter().enumerate() {
        let stored = meta.get_sealed_secrets(*svc).await.expect("secrets");
        assert_eq!(stored.len(), 50, "no secret lost");
        for (name, bytes) in stored {
            let sealed = Sealed::from_bytes(&bytes).expect("a whole sealed value");
            let aad = secret_aad(tenant, *svc, &name);
            let plain = new
                .open(&aad, &sealed)
                .or_else(|_| old.open(&aad, &sealed))
                .expect("opens with a configured key");
            let j: usize = name[1..].parse().expect("index");
            assert_eq!(&plain[..], format!("v-{i}-{j}").as_bytes());
            if all_current {
                assert!(
                    new.is_current(&sealed),
                    "{name} of service {i} is under the current key"
                );
            }
        }
    }
}

/// LSN-003: an internal Unix socket path that is an existing directory, or a symlink to a file
/// elsewhere, never makes the node touch anything outside the path: a directory fails startup,
/// and a symlink's target is left as it was.
#[tokio::test]
async fn lsn_003_odd_internal_socket_paths_touch_nothing_else() {
    let dir = tempfile::tempdir().expect("tmp");
    let start = |internal: &std::path::Path, data: &std::path::Path| {
        let cfg = data.join("rt.toml");
        std::fs::write(
            &cfg,
            format!(
                "[node]\ndata_dir = \"{}\"\n[listeners]\npublic = \"127.0.0.1:0\"\ncontrol = \"127.0.0.1:0\"\nmetrics = \"127.0.0.1:0\"\ninternal = \"unix:{}\"\n",
                data.display(),
                internal.display()
            ),
        )
        .expect("config");
        let mut child = Command::new(env!("CARGO_BIN_EXE_rt"))
            .args(["serve", "--config"])
            .arg(&cfg)
            .env("RT_ROOT_TOKEN", "x".repeat(40))
            .env("RT_MASTER_KEY", hexed(3))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn rt");
        // Give it time to fail or to come up, then stop it. A failing start returns as soon as the
        // process exits; the bound only matters for an instrumented build, which starts slowly.
        for _ in 0..600 {
            if let Ok(Some(status)) = child.try_wait() {
                return Some(status.success());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = child.kill();
        let _ = child.wait();
        None
    };

    // An existing directory at the socket path: startup fails, the directory and its content stay.
    let data1 = dir.path().join("d1");
    std::fs::create_dir_all(&data1).expect("dir");
    let sock_dir = dir.path().join("sockdir");
    std::fs::create_dir_all(&sock_dir).expect("dir");
    std::fs::write(sock_dir.join("keep.txt"), "keep").expect("file");
    let r = start(&sock_dir, &data1);
    assert_eq!(r, Some(false), "a directory at the socket path fails startup");
    assert_eq!(
        std::fs::read_to_string(sock_dir.join("keep.txt")).expect("kept"),
        "keep"
    );

    // A symlink to a file elsewhere: the target file is never modified or removed.
    let data2 = dir.path().join("d2");
    std::fs::create_dir_all(&data2).expect("dir");
    let victim = dir.path().join("victim.txt");
    std::fs::write(&victim, "precious").expect("victim");
    let link = dir.path().join("rt.sock");
    std::os::unix::fs::symlink(&victim, &link).expect("symlink");
    let _ = start(&link, &data2);
    assert_eq!(
        std::fs::read_to_string(&victim).expect("the target still exists"),
        "precious"
    );
}
