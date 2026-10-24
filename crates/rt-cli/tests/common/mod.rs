//! Shared harness for the `21ci` integration tests: a mock control API (httpmock), an isolated
//! config directory and helpers that run the real binary (assert_cmd) and normalise its output.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use httpmock::prelude::*;
use serde_json::{json, Value};
use tempfile::TempDir;

/// Token every test run uses unless it says otherwise.
pub const TOKEN: &str = "rtk_test_secret";
/// 2026-01-01T00:00:00Z in epoch milliseconds.
pub const T0_MS: i64 = 1_767_225_600_000;
pub const T0: &str = "2026-01-01T00:00:00Z";

pub struct Out {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl Out {
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.stdout).unwrap_or_else(|e| panic!("stdout is not JSON ({e}):\n{}", self.stdout))
    }
    /// stdout lines with whitespace runs collapsed (column padding is not part of the contract).
    pub fn lines(&self) -> Vec<String> {
        norm(&self.stdout)
    }
    #[track_caller]
    pub fn ok(self) -> Self {
        assert_eq!(
            self.code, 0,
            "expected exit 0\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        self
    }
    #[track_caller]
    pub fn fails_with(self, code: i32) -> Self {
        assert_eq!(
            self.code, code,
            "expected exit {code}\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        self
    }
}

pub fn norm(s: &str) -> Vec<String> {
    s.lines()
        .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|l| !l.is_empty())
        .collect()
}

pub struct Ctx {
    pub server: MockServer,
    pub cfg: TempDir,
    /// When set, `RT_API`/`RT_INVOKE`/`RT_KEY` are NOT injected (config/context tests).
    bare: bool,
    pub invoke_base: String,
}

impl Ctx {
    /// Mock control API; the binary is pointed at it through `RT_API` and authenticates with [`TOKEN`].
    pub fn new() -> Self {
        Ctx {
            server: MockServer::start(),
            cfg: TempDir::new().unwrap(),
            bare: false,
            invoke_base: "http://invoke.example".into(),
        }
    }

    /// No `RT_*` environment: everything must come from the config file.
    pub fn bare() -> Self {
        Ctx {
            bare: true,
            ..Ctx::new()
        }
    }

    pub fn base(&self) -> String {
        self.server.base_url()
    }

    pub fn config_path(&self) -> PathBuf {
        self.cfg.path().join("21ci").join("config.toml")
    }

    pub fn cmd(&self) -> Command {
        let mut c = Command::cargo_bin("21ci").unwrap();
        c.env_clear();
        c.env("HOME", self.cfg.path());
        c.env("XDG_CONFIG_HOME", self.cfg.path());
        c.env("NO_PROXY", "*").env("no_proxy", "*");
        c.env("PATH", std::env::var("PATH").unwrap_or_default());
        // Under scripts/coverage.sh the instrumented binary writes its profile here; without it the CLI's runs are lost.
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            c.env("LLVM_PROFILE_FILE", profile);
        }
        if !self.bare {
            c.env("RT_API", self.base());
            c.env("RT_INVOKE", &self.invoke_base);
            c.env("RT_KEY", TOKEN);
        }
        c
    }

    /// The same environment as [`Ctx::cmd`], as a `std` command: for the tests that choose stdin or
    /// stdout themselves (assert_cmd always pipes stdin).
    pub fn raw_cmd(&self) -> std::process::Command {
        let mut c = std::process::Command::new(assert_cmd::cargo::cargo_bin("21ci"));
        c.env_clear()
            .env("HOME", self.cfg.path())
            .env("XDG_CONFIG_HOME", self.cfg.path())
            .env("NO_PROXY", "*")
            .env("no_proxy", "*")
            .env("PATH", std::env::var("PATH").unwrap_or_default());
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            c.env("LLVM_PROFILE_FILE", profile);
        }
        if !self.bare {
            c.env("RT_API", self.base())
                .env("RT_INVOKE", &self.invoke_base)
                .env("RT_KEY", TOKEN);
        }
        c
    }

    pub fn run(&self, args: &[&str]) -> Out {
        self.run_in(args, None, None)
    }

    pub fn run_stdin(&self, args: &[&str], stdin: &str) -> Out {
        self.run_in(args, None, Some(stdin))
    }

    pub fn run_in(&self, args: &[&str], cwd: Option<&Path>, stdin: Option<&str>) -> Out {
        let mut c = self.cmd();
        c.args(args);
        if let Some(d) = cwd {
            c.current_dir(d);
        }
        // never inherit a terminal stdin: commands that read it must see EOF, not hang
        c.write_stdin(stdin.unwrap_or(""));
        let o = c.output().expect("spawn 21ci");
        Out {
            code: o.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
        }
    }

    // ---- mock helpers ----------------------------------------------------------------------------

    /// `GET /v1/services?slug=<slug>` answering with one service.
    pub fn lookup(&self, slug: &str, id: &str) -> httpmock::Mock<'_> {
        let body = json!({"items": [service_json(id, slug)], "next_cursor": null});
        self.server.mock(|when, then| {
            when.method(GET)
                .path("/v1/services")
                .query_param("slug", slug)
                .header("authorization", format!("Bearer {TOKEN}"));
            then.status(200).json_body(body);
        })
    }

    /// `GET /v1/services?slug=<slug>` answering with no service.
    pub fn lookup_empty(&self, slug: &str) -> httpmock::Mock<'_> {
        self.server.mock(|when, then| {
            when.method(GET).path("/v1/services").query_param("slug", slug);
            then.status(200).json_body(json!({"items": [], "next_cursor": null}));
        })
    }
}

pub fn service_json(id: &str, slug: &str) -> Value {
    service_json_with(id, slug, Some(3), "\"7\"")
}

pub fn service_json_with(id: &str, slug: &str, live: Option<u32>, etag: &str) -> Value {
    json!({
        "id": id, "tenant_id": "ten_1", "slug": slug, "runtime": "deno", "name": slug,
        "profile": null, "project": "default", "environment": "production", "call": {"allowed_callers": []},
        "live_version": live, "etag": etag,
        "limits": {}, "scaling": {}, "access": "public", "triggers": {}, "ingress": {}, "egress": {},
        "status": {"build_state": "ready", "tier": "T1", "inflight": 0, "last_error": null},
        "effective": {}
    })
}

pub fn version_json(n: u32, state: &str) -> Value {
    json!({
        "version": n, "source_digest": format!("sha256:{}", "ab".repeat(32)),
        "lock_digest": null, "build_state": state, "failure": null, "created_at": T0_MS
    })
}

pub fn err_json(code: &str, message: &str) -> Value {
    json!({"error": {"code": code, "message": message, "details": {}}})
}

/// Write `files` (relative path, content) below `root`.
pub fn write_tree(root: &Path, files: &[(&str, &str)]) {
    for (rel, content) in files {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, content).unwrap();
    }
}
