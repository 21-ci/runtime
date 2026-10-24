//! `21ci init` templates. The sources are the repository samples (`sample/…`), embedded at
//! compile time; READMEs and build scripts are written for a standalone project (they mention
//! `21ci push`, not the repo's `deploy.sh`).

use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Template {
    /// Deno handler `export default (req, ctx)` (sample/deno/hello).
    #[value(name = "deno")]
    Deno,
    /// Deno handler with `std:kv` state (sample/deno/kv-counter).
    #[value(name = "deno-kv")]
    DenoKv,
    /// Rust `wasi:http` component (sample/wasm/rust-hello).
    #[value(name = "wasm-rust")]
    WasmRust,
    /// JavaScript component built with `jco componentize` (sample/wasm/js-componentize).
    #[value(name = "wasm-js")]
    WasmJs,
}

impl Template {
    pub fn as_str(self) -> &'static str {
        match self {
            Template::Deno => "deno",
            Template::DenoKv => "deno-kv",
            Template::WasmRust => "wasm-rust",
            Template::WasmJs => "wasm-js",
        }
    }
}

pub struct File {
    pub path: String,
    pub content: String,
    pub executable: bool,
}

fn file(path: &str, content: impl Into<String>) -> File {
    File {
        path: path.to_string(),
        content: content.into(),
        executable: false,
    }
}

fn script(path: &str, content: impl Into<String>) -> File {
    File {
        path: path.to_string(),
        content: content.into(),
        executable: true,
    }
}

/// Files of `template` for a project called `name` (a valid service slug).
pub fn files(template: Template, name: &str) -> Vec<File> {
    let mut v = match template {
        Template::Deno => vec![
            file("main.ts", include_str!("../../../sample/deno/hello/main.ts")),
            file(
                "README.md",
                deno_readme(
                    name,
                    "Form A handler (`export default async function (req, ctx)`).",
                    "'/x?name=Ada'",
                ),
            ),
        ],
        Template::DenoKv => vec![
            file("main.ts", include_str!("../../../sample/deno/kv-counter/main.ts")),
            file(
                "README.md",
                deno_readme(
                    name,
                    "A counter that keeps its state in `std:kv` (get / set with TTL / delete / list).",
                    "'/?key=visits&action=increment'",
                ),
            ),
        ],
        Template::WasmRust => wasm_rust(name),
        Template::WasmJs => wasm_js(name),
    };
    v.sort_by(|a, b| a.path.cmp(&b.path));
    v
}

fn deno_readme(name: &str, what: &str, invoke_path: &str) -> String {
    format!(
        "# {name}\n\n{what}\n\n## Deploy\n\n```bash\n21ci push . --service {name} --create\n```\n\n\
         `--create` makes the service the first time; later pushes upload a new version and make it live.\n\n\
         ## Invoke\n\n```bash\n21ci invoke {name} {invoke_path}\n```\n\n\
         ## Next\n\n```bash\n21ci logs {name} -f              # follow the logs\n\
         21ci versions {name}             # list versions\n21ci rollback {name}             # back to the previous version\n```\n\n\
         The handler contract is in the runtime repository: docs/handlers.md.\n"
    )
}

fn wasm_rust(name: &str) -> Vec<File> {
    let krate = format!("{name}-wasm");
    let lib = krate.replace('-', "_");
    let cargo = include_str!("../../../sample/wasm/rust-hello/Cargo.toml")
        .replace("name = \"rust-hello-wasm\"", &format!("name = \"{krate}\""));
    let out = format!("target/wasm32-wasip2/release/{lib}.wasm");
    let build = format!(
        "#!/usr/bin/env bash\n# Build the Rust WASI P2 `wasi:http` component.\nset -euo pipefail\n\ncd \"$(dirname \"${{BASH_SOURCE[0]}}\")\"\n\n\
         command -v cargo >/dev/null || {{ echo \"error: cargo not found (install Rust: https://rustup.rs)\" >&2; exit 1; }}\n\n\
         if command -v rustup >/dev/null && ! rustup target list --installed 2>/dev/null | grep -qx wasm32-wasip2; then\n  rustup target add wasm32-wasip2\nfi\n\n\
         cargo build --release --target wasm32-wasip2\n\nOUT=\"{out}\"\n\
         [[ -f \"$OUT\" ]] || {{ echo \"error: $OUT was not produced\" >&2; exit 1; }}\n\
         echo \"Built $OUT ($(wc -c <\"$OUT\" | tr -d ' ') bytes)\"\n\n\
         # The runtime rejects (422 invalid_artifact) a component without wasi:http/incoming-handler\n\
         # or with imports outside wasi:http, wasi:io, wasi:clocks, wasi:random,\n\
         # wasi:cli/{{environment,stdout,stderr}} and std:host/*. With wasm-tools installed:\n\
         #   wasm-tools component wit \"$OUT\"\n\n\
         echo\necho \"Deploy with:\"\necho \"  21ci push $OUT --service {name} --create\"\n"
    );
    vec![
        file("Cargo.toml", cargo),
        file("src/lib.rs", include_str!("../../../sample/wasm/rust-hello/src/lib.rs")),
        script("build.sh", build),
        file(
            "README.md",
            format!(
                "# {name}\n\nA WASI Preview 2 HTTP component written in Rust, for a `wasm` service.\n\n\
                 ## Build\n\n```bash\n./build.sh\n```\n\nProduces `{out}`.\n\n\
                 ## Deploy\n\n```bash\n21ci push {out} --service {name} --create\n```\n\n\
                 ## Invoke\n\n```bash\n21ci invoke {name} '/?name=Ferris'\n```\n\n\
                 The component exports `wasi:http/incoming-handler`; the guest sees `/{{rest}}?{{query}}` (the runtime strips `/s/{name}`).\n\
                 Keep the import set inside what the runtime allows (see the header of `src/lib.rs`).\n"
            ),
        ),
    ]
}

fn wasm_js(name: &str) -> Vec<File> {
    let package =
        include_str!("../../../sample/wasm/js-componentize/package.json").replace("js-componentize-example", name);
    let build = format!(
        "#!/usr/bin/env bash\n# Build src/app.js into a WASI P2 `wasi:http` component with `jco componentize`.\nset -euo pipefail\n\n\
         cd \"$(dirname \"${{BASH_SOURCE[0]}}\")\"\n\n\
         command -v node >/dev/null || {{ echo \"error: node not found (need Node.js >= 20)\" >&2; exit 1; }}\n\
         command -v npm  >/dev/null || {{ echo \"error: npm not found\" >&2; exit 1; }}\n\n\
         if [[ ! -d node_modules ]]; then\n  echo \"Installing dependencies (npm install)...\"\n  npm install --no-audit --no-fund\nfi\n\n\
         mkdir -p dist\n\n\
         # --disable stdio: the stdio feature would import wasi:cli/stdin and wasi:cli/terminal-*, which the\n\
         # runtime does not allow (trade-off: console.log output is not captured).\n\
         npx --no-install jco componentize src/app.js \\\n  --wit wit \\\n  --world-name hello \\\n  --disable stdio \\\n  --out dist/app.wasm\n\n\
         OUT=\"dist/app.wasm\"\n[[ -f \"$OUT\" ]] || {{ echo \"error: $OUT was not produced\" >&2; exit 1; }}\n\
         echo \"Built $OUT ($(wc -c <\"$OUT\" | tr -d ' ') bytes)\"\n\n\
         echo\necho \"Deploy with:\"\necho \"  21ci push $OUT --service {name} --create\"\n"
    );
    vec![
        file("src/app.js", include_str!("../../../sample/wasm/js-componentize/src/app.js")),
        file("wit/world.wit", include_str!("../../../sample/wasm/js-componentize/wit/world.wit")),
        file("wit/deps/wasi-clocks/package.wit", include_str!("../../../sample/wasm/js-componentize/wit/deps/wasi-clocks/package.wit")),
        file("wit/deps/wasi-http/package.wit", include_str!("../../../sample/wasm/js-componentize/wit/deps/wasi-http/package.wit")),
        file("wit/deps/wasi-io/package.wit", include_str!("../../../sample/wasm/js-componentize/wit/deps/wasi-io/package.wit")),
        file("package.json", package),
        script("build.sh", build),
        file(
            "README.md",
            format!(
                "# {name}\n\nA JavaScript handler compiled to a WASI Preview 2 HTTP component with\n\
                 [`jco componentize`](https://github.com/bytecodealliance/jco), for a `wasm` service.\n\n\
                 ## Build\n\n```bash\n./build.sh        # needs Node.js >= 20\n```\n\nProduces `dist/app.wasm`.\n\n\
                 ## Deploy\n\n```bash\n21ci push dist/app.wasm --service {name} --create\n```\n\n\
                 ## Invoke\n\n```bash\n21ci invoke {name} '/?name=Ada'\n```\n\n\
                 This is not the Deno contract: the handler listens for the service-worker style `fetch` event; there is no `ctx`,\n\
                 no `std:*` imports and no `Deno.*`.\n"
            ),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_template_has_files_and_none_mentions_deploy_sh() {
        for t in [Template::Deno, Template::DenoKv, Template::WasmRust, Template::WasmJs] {
            let fs = files(t, "demo");
            assert!(!fs.is_empty());
            for f in &fs {
                assert!(!f.path.starts_with('/') && !f.path.contains(".."), "{}", f.path);
                if f.path != "src/lib.rs" && !f.path.ends_with(".wit") {
                    assert!(!f.content.contains("deploy.sh"), "{}: {}", t.as_str(), f.path);
                }
            }
            assert!(fs.iter().any(|f| f.path == "README.md"));
        }
    }

    #[test]
    fn files_are_sorted() {
        let fs = files(Template::WasmJs, "demo");
        let paths: Vec<_> = fs.iter().map(|f| f.path.as_str()).collect();
        let mut sorted = paths.clone();
        sorted.sort();
        assert_eq!(paths, sorted);
    }
}
