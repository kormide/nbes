use anyhow::Result;
use args::Args;
use build_proto::google::devtools::build::v1::publish_build_event_server::PublishBuildEventServer;
use clap::Parser;
use std::env;
use tonic::transport::Server;

use crate::backend::BesBackend;
use crate::nbes::NBesService;

mod args;
mod backend;
mod client;
mod nbes;

#[tokio::main]
async fn main() -> Result<()> {
    const DEFAULT_PORT: u16 = 9000;

    let args = Args::parse();

    let port = args
        .port
        .or_else(|| {
            env::var("NBES_PORT").ok().and_then(|port| {
                port.parse::<u16>()
                    .inspect_err(|_| {
                        eprintln!("failed to parse NBES_PORT env var, defaulting to {DEFAULT_PORT}")
                    })
                    .ok()
            })
        })
        .unwrap_or(DEFAULT_PORT);

    let address = format!("0.0.0.0:{}", port)
        .parse()
        .expect("failed to parse socket address");

    eprintln!("starting bes server on {address}");

    let bes_backends: Vec<BesBackend> = args.bes_backends.into_iter().map(|b| b.into()).collect();

    let mut nbes_service = NBesService::new();

    if bes_backends.is_empty() {
        eprintln!("no bes backends configured; bes events will be swallowed by a black hole");
    }

    for backend in bes_backends {
        eprintln!(
            "configured backend {} -> {}",
            backend.name, backend.endpoint
        );
        nbes_service.add_backend(backend)?;
    }

    Server::builder()
        // TODO: properties to potentially configure
        // .concurrency_limit_per_connection(100)
        // .load_shed(true)
        // .max_concurrent_streams(Some(1000))
        .add_service(PublishBuildEventServer::new(nbes_service))
        .serve(address)
        .await?;

    Ok(())
}
