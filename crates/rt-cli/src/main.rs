//! `21ci`: command line client for the control API of the 21 Cloud Functions Runtime.
//!
//! Exit codes: 0 ok, 1 API error (or a failed command), 2 usage error, 3 network error,
//! 4 busy (`429`/`503` with `Retry-After`).

mod artifact;
mod cli;
mod client;
mod cmd;
mod config;
mod error;
mod fmt;
mod sse;
mod template;

use clap::Parser;

use cli::{Cli, Command, Output};
use cmd::App;
use config::ConfigFile;
use error::Result;

fn main() {
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        // prints help/version to stdout (exit 0) and usage errors to stderr (exit 2)
        Err(e) => e.exit(),
    };
    let json = cli.json || cli.output == Output::Json;
    if let Err(e) = run(&cli, json) {
        let text = if json {
            e.render_json().map(|v| v.to_string())
        } else {
            e.render_text()
        };
        if let Some(t) = text {
            eprintln!("{t}");
        }
        std::process::exit(e.exit_code());
    }
}

fn run(cli: &Cli, json: bool) -> Result<()> {
    // commands that work without a node
    match &cli.command {
        Command::Config { cmd } => {
            return cmd::config::run(
                cmd,
                json,
                cli.config.as_deref(),
                cli.context.as_deref(),
                cli.as_tenant.as_deref(),
            )
        }
        Command::Init(a) => return cmd::init::run(json, a),
        Command::Completion(a) => return cmd::misc::completion(a),
        _ => {}
    }

    let path = config::config_path(cli.config.as_deref())?;
    let settings = ConfigFile::load(&path)?.resolve(cli.context.as_deref(), cli.as_tenant.as_deref(), &|k| {
        std::env::var(k).ok()
    })?;
    let app = App {
        settings,
        json,
        retry: cli.retry,
    };

    match &cli.command {
        Command::Whoami => cmd::plan::whoami(&app),
        Command::Limits => cmd::limits::run(&app),
        Command::Plan { cmd } => cmd::plan::run(&app, cmd),
        Command::Tenant { cmd } => cmd::tenant::run(&app, cmd),
        Command::Key { cmd } => cmd::key::run(&app, cmd),
        Command::Token { cmd } => cmd::token::run(&app, cmd),
        Command::Project { cmd } => cmd::project::run(&app, cmd),
        Command::Mcp(_) => cmd::mcp::run(&app),
        Command::Service { cmd } => cmd::service::run(&app, cmd),
        Command::Push(a) => cmd::push::run(&app, a),
        Command::Versions(a) => cmd::versions::list(&app, a),
        Command::Rollback(a) => cmd::versions::rollback(&app, a),
        Command::Rebuild(a) => cmd::versions::rebuild(&app, a),
        Command::BuildLogs(a) => cmd::versions::build_logs(&app, a),
        Command::Source(a) => cmd::versions::source(&app, a),
        Command::Types(a) => cmd::sdk::types(&app, a),
        Command::Sdk { cmd } => cmd::sdk::run(&app, cmd),
        Command::Env { cmd } => cmd::env::run(&app, cmd),
        Command::Secret { cmd } => cmd::secret::run(&app, cmd),
        Command::Host { cmd } => cmd::host::run(&app, cmd),
        Command::Data { cmd } => cmd::data::run(&app, cmd),
        Command::Schedule { cmd } => cmd::schedule::run(&app, cmd),
        Command::Sockets { cmd } => cmd::sockets::run(&app, cmd),
        Command::Objects { cmd } => cmd::sockets::objects(&app, cmd),
        Command::Queue { cmd } => cmd::queue::run(&app, cmd),
        Command::Topic { cmd } => cmd::topic::run(&app, cmd),
        Command::Consumer { cmd } => cmd::topic::consumer(&app, cmd),
        Command::Pubsub { cmd } => cmd::topic::pubsub(&app, cmd),
        Command::Invocation { cmd } => cmd::invocation::run(&app, cmd),
        Command::Logs(a) => cmd::logs::run(&app, a),
        Command::Stats(a) => cmd::stats::run(&app, a),
        Command::Events(a) => cmd::events::run(&app, a),
        Command::Invoke(a) => cmd::invoke::run(&app, a),
        Command::Node { cmd } => cmd::node::run(&app, cmd),
        Command::Openapi(a) => cmd::misc::openapi(&app, a),
        Command::Docs => cmd::misc::docs(&app),
        Command::Doctor => cmd::doctor::run(&app, &path),
        Command::Version(a) => cmd::misc::version(&app, a),
        Command::Config { .. } | Command::Init(_) | Command::Completion(_) => unreachable!("handled above"),
    }
}
