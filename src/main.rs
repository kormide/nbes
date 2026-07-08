use anyhow::Result;
use args::Args;
use clap::Parser;
use log::{LevelFilter, info};
use nbes::Config;
use tokio::signal;

mod args;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .format_target(false)
        .parse_env("NBES_LOG")
        .init();

    info!("starting nbes {}", env!("CARGO_PKG_VERSION"),);
    info!(
        "listening on {}",
        args.socket
            .as_ref()
            .map(|s| format!("unix:{}", s.display()))
            .unwrap_or(args.listen.to_string())
    );

    let config = Config {
        bes_backends: args.bes_backends.into_iter().map(|b| b.into()).collect(),
        listen: args.listen,
        socket: args.socket,
    };

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to listed for ctrl-c signal");
        info!("received ctrl-c");
    };

    nbes::run(config, ctrl_c).await?;

    Ok(())
}
