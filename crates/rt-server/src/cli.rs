//! Command line of the `rt` binary.
//!
//! ```text
//! rt [serve] [--config rt.toml] [--role all]     run the node (default command)
//! rt --print-config [--config rt.toml]           print the fully resolved configuration and exit 0
//! rt openapi                                     print the OpenAPI JSON of the control API and exit 0
//! rt secrets reseal [--config rt.toml]           re-seal every stored secret under RT_MASTER_KEY
//! ```
//!
//! Environment overlay `RT__SECTION__KEY=value`, secrets `RT_ROOT_TOKEN` / `RT_MASTER_KEY` /
//! `RT_MASTER_KEY_PREVIOUS` (and `*_FILE`) are read by `rt-config`, not here.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

/// Process role. The MVP runs everything in one process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Role {
    /// Supervisor + executor in one process.
    All,
}

#[derive(Debug, Parser)]
#[command(name = "rt", version, about = "21 Cloud Functions Runtime")]
pub struct Cli {
    /// Path to `rt.toml` (default: `$RT_DATA/rt.toml` if it exists).
    #[arg(long, global = true, env = "RT_CONFIG")]
    pub config: Option<PathBuf>,

    /// Which role this process plays. Only `all` exists in the MVP.
    #[arg(long, global = true, value_enum, default_value_t = Role::All)]
    pub role: Role,

    /// Print the fully resolved configuration (profile defaults filled in) and exit.
    #[arg(long)]
    pub print_config: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Clone, Copy, Subcommand)]
pub enum Command {
    /// Run the node (default).
    Serve,
    /// Print the OpenAPI 3.1 document of the control API (JSON) to stdout and exit.
    Openapi,
    /// Maintenance of the sealed tenant secrets in `meta.db`.
    Secrets {
        #[command(subcommand)]
        action: SecretsCommand,
    },
}

/// `rt secrets …`
#[derive(Debug, Clone, Copy, Subcommand)]
pub enum SecretsCommand {
    /// Rewrite every stored secret under the current master key (`RT_MASTER_KEY`), opening old
    /// values with `RT_MASTER_KEY_PREVIOUS` (key rotation, docs/deploy.md). Exits 1 if a value
    /// opens under neither key.
    Reseal,
}

impl Cli {
    /// The effective command (`serve` when none is given).
    pub fn command(&self) -> Command {
        self.command.unwrap_or(Command::Serve)
    }
}
