//! `rt`: the 21 Cloud Functions Runtime binary.

use clap::Parser;
use mimalloc::MiMalloc;
use rt_config::{Config, LoadOptions};
use rt_server::{start, Cli, Command, NodeSignal, NodeSignals, RunningServer};

// One allocator for the whole process: per-thread heaps fit per-Machine execution and give the
// governor a purge hook.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // The OpenAPI document needs neither configuration nor tracing.
    if let Some(Command::Openapi) = cli.command {
        println!("{}", rt_api::openapi_json_string());
        return Ok(());
    }

    rt_obs::init_tracing(&rt_obs::TracingOptions::default())?;

    let opts = LoadOptions {
        file: cli.config.clone(),
        env: Vec::new(),
    }
    .with_process_env();
    let loaded = Config::load(&opts)?;

    if cli.print_config {
        println!("{}", loaded.config.print()?);
        return Ok(());
    }

    match cli.command() {
        Command::Serve => {
            // Install the handlers before the node is up: an early SIGHUP must not kill it.
            let mut signals = NodeSignals::new();
            let server = start(loaded.config, loaded.secrets).await?;
            tracing::info!(addrs = ?server.addrs(), "rt is up");
            match signals.as_mut() {
                Some(signals) => loop {
                    match signals.next().await {
                        NodeSignal::Stop => break,
                        NodeSignal::Reload => match server.reload_config(&opts).await {
                            Ok(()) => {}
                            Err(e @ rt_server::ServerError::ReloadPartial(_)) => {
                                tracing::error!(error = %e, "SIGHUP: reload incomplete");
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "SIGHUP: reload failed; the running configuration is unchanged");
                            }
                        },
                    }
                },
                None => RunningServer::wait_for_signal().await,
            }
            tracing::info!("shutting down");
            server.shutdown().await;
        }
        Command::Openapi => unreachable!("handled above"),
        Command::Secrets {
            action: rt_server::SecretsCommand::Reseal,
        } => {
            let report = rt_server::reseal::run_reseal(&loaded.config, &loaded.secrets).await?;
            println!("{}", report.summary());
            for (service, name) in &report.failed {
                eprintln!("could not open secret {name} of service {service}: neither RT_MASTER_KEY nor RT_MASTER_KEY_PREVIOUS opens it");
            }
            if !report.failed.is_empty() {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
