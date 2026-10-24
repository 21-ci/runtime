//! The real `rt` binary as a child process.

use std::process::Command;
use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::json;

use crate::common::*;

/// ARC-03: SIGTERM while requests are in flight. New requests get `503 x-rt-reason: draining`,
/// the in-flight ones finish within the grace period, and the process exits with status 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_arc_03_sigterm_drains_in_flight_requests_and_exits_zero() {
    let mut node = ProcNode::spawn(true, "info", |_| {});
    node.wait_ready(Duration::from_secs(90)).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("drain");
    ship(&node, &t.key, &slug, &guests::slow(1, 3_000)).await;
    let warm = invoke(&node, &slug, "/").await;
    assert_eq!((warm.s(), warm.text.as_str()), (200, "v1"), "{}", warm.text);

    let url = node.public_url(&format!("/s/{slug}/"));
    let inflight: Vec<_> = (0..5)
        .map(|_| tokio::spawn(fetch(node.http().clone(), url.clone())))
        .collect();
    tokio::time::sleep(Duration::from_millis(600)).await; // all five are inside the handler

    node.sigterm();

    // While draining, new requests are refused with a documented reason.
    let mut saw_draining = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Ok(r) = node.http().get(&url).send().await {
            if r.status().as_u16() == 503
                && r.headers().get("x-rt-reason").and_then(|v| v.to_str().ok()) == Some("draining")
            {
                saw_draining = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        saw_draining,
        "new requests during the drain must get 503 x-rt-reason: draining\n{}",
        node.logs()
    );

    // In-flight requests complete normally.
    for (i, h) in inflight.into_iter().enumerate() {
        let (status, body, version, _) = h.await.expect("join");
        assert_eq!(
            (status, body.as_str(), version),
            (200, "v1", Some(1)),
            "in-flight request {i} must finish"
        );
    }

    let status = node
        .wait_exit(Duration::from_secs(40))
        .await
        .expect("the process exits after draining");
    assert!(
        status.success(),
        "exit status after SIGTERM must be 0, got {status}\n{}",
        node.logs()
    );
}

/// SIGHUP re-reads the plans from the config file and the node keeps running (without a
/// handler, SIGHUP's default action terminates the process).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sighup_reloads_plans_and_keeps_the_node_running() {
    let mut node = ProcNode::spawn(true, "info", |_| {});
    node.wait_ready(Duration::from_secs(90)).await;
    let t = new_tenant(&node, "pro").await;
    let svc = |n: &str| json!({"slug": uniq(n), "runtime": "deno"});
    assert_eq!(post(&node, &t.key, "/v1/services", svc("hup-a")).await.s(), 201);

    // Lower the plan's service quota in the file, then signal.
    let path = node.dir.path().join("rt.toml");
    let mut cfg = rt_config::Config::from_toml(&std::fs::read_to_string(&path).expect("read rt.toml"), &[])
        .expect("parse rt.toml");
    cfg.plans.get_mut("pro").expect("pro plan").max_services = 1;
    std::fs::write(&path, toml::to_string(&cfg).expect("serialize")).expect("write rt.toml");
    let sent = Command::new("kill")
        .arg("-HUP")
        .arg(node.child.id().to_string())
        .status()
        .expect("kill -HUP");
    assert!(sent.success());

    eventually(Duration::from_secs(10), "the reloaded plan applies", || async {
        post(&node, &t.key, "/v1/services", svc("hup-b")).await.s() == 403
    })
    .await;
    assert!(
        node.child.try_wait().expect("try_wait").is_none(),
        "SIGHUP must not stop the node\n{}",
        node.logs()
    );
    assert_eq!(
        send(node.http().get(format!("http://{}/readyz", node.metrics)))
            .await
            .s(),
        200
    );

    node.sigterm();
    let status = node
        .wait_exit(Duration::from_secs(40))
        .await
        .expect("exits after SIGTERM");
    assert!(status.success(), "{status}\n{}", node.logs());
}

/// A SIGHUP whose file no longer has a plan that tenants use is refused: those tenants would
/// silently fall back to `free`. The running plans stay in effect and the log says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sighup_refuses_a_file_that_drops_a_plan_tenants_still_use() {
    let mut node = ProcNode::spawn(true, "info", |_| {});
    node.wait_ready(Duration::from_secs(90)).await;
    let t = new_tenant(&node, "pro").await;
    let id = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": uniq("hup-c"), "runtime": "wasm"}),
    )
    .await;
    let id = id.json["id"].as_str().expect("id").to_string();

    let path = node.dir.path().join("rt.toml");
    let mut cfg = rt_config::Config::from_toml(&std::fs::read_to_string(&path).expect("read rt.toml"), &[])
        .expect("parse rt.toml");
    assert!(cfg.plans.remove("pro").is_some());
    std::fs::write(&path, toml::to_string(&cfg).expect("serialize")).expect("write rt.toml");
    let sent = Command::new("kill")
        .arg("-HUP")
        .arg(node.child.id().to_string())
        .status()
        .expect("kill -HUP");
    assert!(sent.success());

    eventually(Duration::from_secs(10), "the refusal is logged", || async {
        node.logs().contains("plans still used by tenants are missing")
    })
    .await;
    // `pro` still applies (`free` allows no custom hosts).
    let r = send(api(
        &node,
        &t.key,
        Method::PUT,
        &format!("/v1/services/{id}/hosts/{}.bots.example.com", uniq("h")),
    ))
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(
        node.child.try_wait().expect("try_wait").is_none(),
        "SIGHUP must not stop the node\n{}",
        node.logs()
    );
    node.sigterm();
    let _ = node.wait_exit(Duration::from_secs(40)).await;
}

/// SEC-01: no `RT_ROOT_TOKEN`. Release builds answer every control call with
/// `503 control_disabled`; debug builds stay open (SECURITY.md). Invocation keeps working and the
/// node logs a startup warning in both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sec_01_control_api_without_a_root_token() {
    let mut node = ProcNode::spawn(false, "info", |_| {});
    node.wait_ready(Duration::from_secs(90)).await;

    for (m, path) in [
        (Method::GET, "/v1/tenants"),
        (Method::GET, "/v1/services"),
        (Method::GET, "/v1/node/status"),
    ] {
        let r = send(node.http().request(m.clone(), node.control_url(path))).await;
        if cfg!(debug_assertions) {
            assert!(
                r.s() == 200 || r.s() == 404,
                "debug builds without a root token stay open: {m} {path} -> {} {}",
                r.s(),
                r.text
            );
        } else {
            assert_eq!(r.s(), 503, "{m} {path}: {}", r.text);
            assert_eq!(r.code(), "control_disabled");
        }
    }
    // Even a (made-up) bearer token does not open the control API in a release build.
    let r = send(node.http().get(node.control_url("/v1/tenants")).bearer_auth("anything")).await;
    if !cfg!(debug_assertions) {
        assert_eq!(r.s(), 503, "{}", r.text);
        assert_eq!(r.code(), "control_disabled");
    }

    // Invoke is independent of the control API: an unknown slug is a plain 404.
    let r = send(node.http().get(node.public_url("/s/nobody/"))).await;
    assert_eq!(r.s(), 404, "{}", r.text);
    assert_eq!(r.header("x-rt-reason").as_deref(), Some("not_found"));
    let r = send(node.http().get(node.metrics_url("/healthz"))).await;
    assert_eq!(r.s(), 200);

    // A startup warning names the problem.
    let logs = node.logs().to_lowercase();
    assert!(
        logs.contains("warn") && logs.contains("root"),
        "expected a startup warning about the missing root token in the logs:\n{}",
        node.logs()
    );
    assert!(
        node.child.try_wait().expect("try_wait").is_none(),
        "the node keeps running"
    );
}

/// SEC-17: a canary secret (and the node's own root token / master key) never appears in the
/// runtime's debug logs, in `/metrics`, or in any API response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sec_17_canary_secret_never_reaches_logs_metrics_or_api_responses() {
    let canary = "canary-5f3a9c71e2b84d06";
    let env_canary = "env-canary-0b7d44aa91ce";
    let mut node = ProcNode::spawn(true, "debug,rt_sched=debug", |c| c.logs.stdout = true);
    node.wait_ready(Duration::from_secs(90)).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("canary");
    let svc = ship(&node, &t.key, &slug, guests::SECRET_READER).await;

    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/secrets/CANARY_SECRET"),
        json!({"value": canary}),
    )
    .await;
    assert!(r.s() == 200 || r.s() == 204, "{}", r.text);
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/env"),
        json!({"VISIBLE": env_canary}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    // A rejected write must not echo the value either.
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/secrets/bad name!"),
        json!({"value": canary}),
    )
    .await;
    assert!(r.s() >= 400, "{}", r.text);

    let r = invoke(&node, &slug, "/").await;
    assert_eq!(
        (r.s(), r.text.as_str()),
        (200, "len=23"),
        "the guest can read its secret: {}",
        r.text
    );
    // Make some failures too: guest error paths log a lot.
    let _ = invoke(&node, &slug, "/does/not/exist?x=1").await;

    // Collect every API response we can think of.
    let mut bodies: Vec<(String, String)> = Vec::new();
    for path in [
        "/v1/services".to_string(),
        format!("/v1/services/{svc}"),
        format!("/v1/services/{svc}/versions"),
        format!("/v1/services/{svc}/versions/1"),
        format!("/v1/services/{svc}/secrets"),
        format!("/v1/services/{svc}/events"),
        format!("/v1/services/{svc}/logs?limit=500"),
        format!("/v1/services/{svc}/stats?window=5m"),
        format!("/v1/tenants/{}/keys", t.id),
        format!("/v1/tenants/{}", t.id),
        format!("/v1/tenants/{}/usage", t.id),
        "/v1/node/status".to_string(),
        "/v1/node/scheduler".to_string(),
        "/v1/node/cache".to_string(),
    ] {
        let r = call(root(&node, Method::GET, &path), None).await;
        bodies.push((path, r.text));
    }
    let metrics = send(node.http().get(node.metrics_url("/metrics"))).await.text;
    bodies.push(("/metrics".into(), metrics));
    tokio::time::sleep(Duration::from_millis(1_500)).await; // let log writers flush

    for (path, body) in &bodies {
        assert!(!body.contains(canary), "the secret value leaked through GET {path}");
    }
    // The env value is plain config (readable through the API) but must stay out of the logs.
    let logs = node.logs();
    for (what, needle) in [
        ("secret", canary),
        ("env value", env_canary),
        ("root token", node.root.as_deref().unwrap_or("-no-root-")),
        ("master key", node.master_key_hex.as_str()),
    ] {
        assert!(!logs.contains(needle), "the {what} appears in the runtime logs");
    }
    // The key's own secret never shows up in logs either.
    let key_secret = t.key.rsplit('_').next().expect("secret part");
    assert!(!logs.contains(key_secret), "tenant key secret in the logs");
    assert!(
        logs.len() > 200,
        "debug logging produced output ({} bytes), so the check above means something",
        logs.len()
    );
    node.child.kill().ok();
}

/// CFG-01 through the binary: `rt --print-config` with only `RT_ROOT_TOKEN` prints the resolved
/// `balanced` configuration and exits 0 without starting a node.
#[test]
fn cli_print_config_prints_resolved_defaults_and_exits_zero() {
    let out = Command::new(env!("CARGO_BIN_EXE_rt"))
        .arg("--print-config")
        .env_remove("RT_MASTER_KEY")
        .env("RT_ROOT_TOKEN", "print-config-test")
        .output()
        .expect("run rt --print-config");
    assert!(
        out.status.success(),
        "exit status {:?}: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("[sched]"), "{text}");
    assert!(text.contains("balanced"), "default profile is balanced: {text}");
    assert!(!text.contains("print-config-test"), "secrets are never printed");
}

/// An invalid configuration fails startup with the key path in the message (CFG-02 via the binary).
#[test]
fn cli_unknown_config_key_fails_startup_naming_the_key() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = dir.path().join("rt.toml");
    std::fs::write(&cfg, "[sched]\nprocesors = 2\n").expect("write config");
    let out = Command::new(env!("CARGO_BIN_EXE_rt"))
        .arg("--print-config")
        .arg("--config")
        .arg(&cfg)
        .env("RT_ROOT_TOKEN", "x")
        .output()
        .expect("run rt");
    assert!(!out.status.success(), "an unknown key must fail");
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(all.contains("sched.procesors"), "the error names the key path: {all}");
}

/// B0 regression (handoff C.3): Deno `hello` at c=16 for 3 s while every core runs a busy loop,
/// then one request to a second service must be served, not `503 queue_timeout`. Before the B0
/// fix (a leaked spinner count) such a request stalled within 1–8 rounds. Stress test, run it
/// after scheduler changes:
/// `cargo test -p rt-server --test process -- --ignored scheduler_stall --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress: saturates every core for about a minute"]
async fn scheduler_stall_regression_under_cpu_hogs_and_concurrent_load() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let mut node = ProcNode::spawn(true, "warn", |c| c.ingress.max_conns_per_ip = 4096);
    node.wait_ready(Duration::from_secs(90)).await;
    let t = new_tenant(&node, "pro").await;
    let hello = uniq("hello");
    let other = uniq("other");
    ship(&node, &t.key, &hello, guests::HELLO).await;
    ship(&node, &t.key, &other, &guests::versioned(2)).await;
    assert_eq!(invoke(&node, &hello, "/").await.s(), 200);
    assert_eq!(invoke(&node, &other, "/").await.s(), 200);

    let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
    let hello_url = node.public_url(&format!("/s/{hello}/?name=x"));
    let other_url = node.public_url(&format!("/s/{other}/"));
    for round in 1..=14 {
        let stop = Arc::new(AtomicBool::new(false));
        let hogs: Vec<_> = (0..cores)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::hint::spin_loop();
                    }
                })
            })
            .collect();
        let until = Instant::now() + Duration::from_secs(3);
        let load: Vec<_> = (0..16)
            .map(|_| {
                let (client, url) = (node.http().clone(), hello_url.clone());
                tokio::spawn(async move {
                    let mut statuses = Vec::new();
                    while Instant::now() < until {
                        if let Ok(r) = client.get(&url).send().await {
                            statuses.push(r.status().as_u16());
                            let _ = r.bytes().await;
                        }
                    }
                    statuses
                })
            })
            .collect();
        let mut statuses = Vec::new();
        for l in load {
            statuses.extend(l.await.expect("load task"));
        }
        stop.store(true, Ordering::Relaxed);
        for h in hogs {
            h.join().expect("hog thread");
        }
        assert!(
            statuses.iter().all(|s| *s == 200 || *s == 503),
            "round {round}: unexpected statuses under load"
        );

        let probe = tokio::time::timeout(Duration::from_secs(8), node.http().get(&other_url).send()).await;
        let status = match probe {
            Ok(Ok(r)) => r.status().as_u16(),
            _ => 0,
        };
        if status != 200 {
            let snap = send(root(&node, Method::GET, "/v1/node/scheduler")).await;
            panic!(
                "STALL in round {round} (status {status}): scheduler {}\n{}",
                snap.text,
                node.logs()
            );
        }
        eprintln!("round {round}: {} requests under hogs, probe ok", statuses.len());
    }
    node.sigterm();
    let _ = node.wait_exit(Duration::from_secs(40)).await;
}

/// `rt openapi` prints the control API document without a configuration, a token or a data dir.
#[test]
fn cli_openapi_prints_the_document_without_any_configuration() {
    let out = Command::new(env!("CARGO_BIN_EXE_rt"))
        .arg("openapi")
        .output()
        .expect("run rt openapi");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let doc: serde_json::Value = serde_json::from_slice(&out.stdout).expect("a JSON document");
    assert!(
        doc["openapi"].as_str().is_some_and(|v| v.starts_with("3.")),
        "{}",
        doc["openapi"]
    );
    assert!(doc["paths"]["/v1/services"].is_object());
}

/// `rt secrets reseal`: a secret no configured key opens is named on stderr (never its value) and the
/// command exits non-zero; the secrets it could open were rewritten all the same.
#[tokio::test]
async fn cli_secrets_reseal_names_what_it_could_not_open_and_exits_non_zero() {
    use rt_sec::{secret_aad, LocalSecrets, MasterKey, SecretsBackend};
    use rt_store::{MetaStore, SqliteMetaStore, TenantScope};
    use rt_types::{NewService, NewTenant, RuntimeKind};

    let dir = tempfile::tempdir().expect("tmp");
    let hexed = |b: u8| hex::encode([b; 32]);
    let meta = SqliteMetaStore::open(&dir.path().join("meta.db")).expect("meta.db");
    let tenant = meta
        .create_tenant(NewTenant {
            name: "t".into(),
            plan: "free".into(),
            overrides: None,
        })
        .await
        .expect("tenant");
    let new = NewService {
        slug: "s".into(),
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
    let seal = |key: u8, name: &str, value: &str| {
        LocalSecrets::new(&MasterKey::from_bytes([key; 32]))
            .seal(&secret_aad(tenant.id, svc, name), value.as_bytes())
            .to_bytes()
    };
    meta.put_secret(svc, "OLD", seal(1, "OLD", "recoverable"))
        .await
        .expect("old");
    meta.put_secret(svc, "LOST", seal(9, "LOST", "unrecoverable-canary"))
        .await
        .expect("lost");
    drop(meta);

    let cfg = dir.path().join("rt.toml");
    std::fs::write(&cfg, format!("[node]\ndata_dir = \"{}\"\n", dir.path().display())).expect("config");
    let out = Command::new(env!("CARGO_BIN_EXE_rt"))
        .args(["secrets", "reseal", "--config"])
        .arg(&cfg)
        .env("RT_ROOT_TOKEN", "x")
        .env("RT_MASTER_KEY", hexed(2))
        .env("RT_MASTER_KEY_PREVIOUS", hexed(1))
        .output()
        .expect("run rt secrets reseal");
    let (stdout, stderr) = (
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        !out.status.success(),
        "a secret nobody opens fails the command: {stdout}{stderr}"
    );
    assert!(
        stdout.contains("resealed: 1") && stdout.contains("failed: 1"),
        "{stdout}"
    );
    assert!(stderr.contains("could not open secret LOST"), "{stderr}");
    assert!(!format!("{stdout}{stderr}").contains("unrecoverable-canary"));
}

/// `RT_SCHEDTRACE=<ms>` prints a scheduler snapshot line to stderr at that interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_schedtrace_prints_scheduler_lines_to_stderr() {
    let mut node = ProcNode::spawn_with_env(true, "info", |_| {}, &[("RT_SCHEDTRACE", "100")]);
    node.wait_ready(Duration::from_secs(90)).await;
    eventually(Duration::from_secs(10), "a schedtrace line", || async {
        node.stderr().lines().any(|l| l.starts_with("SCHED "))
    })
    .await;
    node.sigterm();
    let status = node
        .wait_exit(Duration::from_secs(40))
        .await
        .expect("exits after SIGTERM");
    assert!(status.success(), "{status}");
}
