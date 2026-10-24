//! `21ci init <dir> --template …`

use std::path::Path;

use serde_json::json;

use crate::artifact::{slugify, valid_slug};
use crate::cli::InitArgs;
use crate::error::{CliError, Result};
use crate::fmt;
use crate::template;

fn is_empty_dir(dir: &Path) -> std::io::Result<bool> {
    Ok(std::fs::read_dir(dir)?.next().is_none())
}

pub fn run(json: bool, a: &InitArgs) -> Result<()> {
    // the project name doubles as the service slug in the README and the build script
    let name = match &a.name {
        Some(n) => n.clone(),
        None => {
            let abs = std::path::absolute(&a.dir).map_err(|e| CliError::usage(format!("{}: {e}", a.dir.display())))?;
            slugify(
                &abs.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            )
        }
    };
    if !valid_slug(&name) {
        return Err(CliError::usage(format!(
            "\"{name}\" is not a valid service slug (^[a-z][a-z0-9-]{{1,62}}$); pass one with --name <slug>"
        )));
    }

    if a.dir.exists() {
        if !a.dir.is_dir() {
            return Err(CliError::usage(format!(
                "{} exists and is not a directory",
                a.dir.display()
            )));
        }
        let empty = is_empty_dir(&a.dir).map_err(|e| CliError::usage(format!("{}: {e}", a.dir.display())))?;
        if !empty && !a.force {
            return Err(CliError::usage(format!(
                "{} is not empty (use --force to write into it anyway)",
                a.dir.display()
            )));
        }
    }

    let files = template::files(a.template, &name);
    let io = |p: &Path, e: std::io::Error| CliError::failed(format!("{}: {e}", p.display()));
    for f in &files {
        let target = a.dir.join(&f.path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io(parent, e))?;
        } // LCOV_EXCL_LINE: a template path joined to the target directory always has a parent
        std::fs::write(&target, &f.content).map_err(|e| io(&target, e))?;
        #[cfg(unix)]
        if f.executable {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).map_err(|e| io(&target, e))?;
        }
    }

    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    if json {
        fmt::print_json(
            &json!({"template": a.template.as_str(), "name": name, "dir": a.dir.display().to_string(), "files": paths}),
        );
        return Ok(());
    }
    fmt::emitln(&format!(
        "Created {} (template {})",
        a.dir.display(),
        a.template.as_str()
    ));
    for p in &paths {
        fmt::emitln(&format!("  {p}"));
    }
    let push = match a.template {
        template::Template::Deno | template::Template::DenoKv => format!("21ci push . --service {name} --create"),
        template::Template::WasmRust => format!(
            "./build.sh && 21ci push target/wasm32-wasip2/release/{}_wasm.wasm --service {name} --create",
            name.replace('-', "_")
        ),
        template::Template::WasmJs => format!("./build.sh && 21ci push dist/app.wasm --service {name} --create"),
    };
    fmt::emitln(&format!("Next: cd {} && {push}", a.dir.display()));
    Ok(())
}
