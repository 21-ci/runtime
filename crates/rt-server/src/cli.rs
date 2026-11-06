//! Command line of the `rt` binary.
//!
//! ```text
//! rt [serve] [--config rt.toml] [--role all]     run the node (default command)
//! rt --print-config [--config rt.toml]           print the fully resolved configuration and exit 0
//! rt openapi                                     print the OpenAPI JSON of the control API and exit 0
//! rt secrets reseal [--config rt.toml]           re-seal every stored secret under RT_MASTER_KEY
//! rt telemetry show [--config rt.toml]           print the next anonymous report; sends nothing
//! rt backup verify <archive> [--no-auth]         check an archive, offline
//! rt backup restore <archive> --data-dir <empty> restore an archive, offline
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

#[derive(Debug, Clone, Subcommand)]
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
    /// The anonymous daily report.
    Telemetry {
        #[command(subcommand)]
        action: TelemetryCommand,
    },
    /// Offline work on backup archives. The node need not run; the archive is not trusted.
    Backup {
        #[command(subcommand)]
        action: BackupCommand,
    },
}

/// `rt telemetry …`
#[derive(Debug, Clone, Subcommand)]
pub enum TelemetryCommand {
    /// Print the next report and whether it is sent. Sends nothing, creates no id.
    Show,
}

/// `rt backup …`
#[derive(Debug, Clone, Subcommand)]
pub enum BackupCommand {
    /// Check an archive: its MAC, every entry's digest and shape. Writes nothing.
    Verify {
        archive: PathBuf,
        /// Skip the MAC: for an archive whose master key is lost. Says nothing about who wrote it.
        #[arg(long)]
        no_auth: bool,
        /// `age` identity file, for an encrypted archive.
        #[arg(long)]
        age_identity: Option<PathBuf>,
    },
    /// Restore an archive into an empty data directory.
    Restore {
        archive: PathBuf,
        /// The new data directory; must be empty or absent.
        #[arg(long)]
        data_dir: PathBuf,
        /// Skip the MAC: for an archive whose master key is lost. Says nothing about who wrote it.
        #[arg(long)]
        no_auth: bool,
        /// Revoke API keys created before this time (RFC 3339 or Unix ms), and keys they minted.
        #[arg(long)]
        revoke_keys_before: Option<String>,
        /// `age` identity file, for an encrypted archive.
        #[arg(long)]
        age_identity: Option<PathBuf>,
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
        self.command.clone().unwrap_or(Command::Serve)
    }
}
