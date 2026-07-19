use anyhow::Result;
use args::Args;
use clap::Parser;
use log::{LevelFilter, info};
use nbes::{Binding, Config, ServerTlsConfig};
use tokio::signal;

use crate::args::ServerTlsArgs;

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
        listen: args
            .socket
            .map(|socket_path| Binding::UnixDomainSocket(socket_path))
            .unwrap_or_else(|| Binding::SocketAddr(args.listen)),
        server_tls_config: match args.server_tls_config {
            ServerTlsArgs {
                server_tls_certificate: Some(certificate),
                server_tls_private_key: Some(private_key),
            } => Some(ServerTlsConfig {
                certificate,
                private_key,
            }),
            _ => None,
        },
        tls_certificates: args.tls_certificate,
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
