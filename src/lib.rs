use anyhow::Result;
use log::{info, warn};
use std::{net::SocketAddr, path::PathBuf};

use crate::{
    forwarding::{BesBackend, BesForwardingService},
    server::GrpcBesServer,
};

pub mod forwarding;
mod server;

pub struct Config {
    pub bes_backends: Vec<BesBackend>,
    pub listen: SocketAddr,
    pub socket: Option<PathBuf>,
}

pub async fn run(config: Config, shutdown_signal: impl Future<Output = ()>) -> Result<()> {
    let mut nbes_service = BesForwardingService::new();

    if config.bes_backends.is_empty() {
        warn!("no bes backends configured; bes events will be swallowed by a black hole");
    }

    for backend in config.bes_backends {
        info!(
            "configured backend {} -> {}",
            backend.name, backend.endpoint
        );
        nbes_service.add_backend(backend)?;
    }

    let server = if let Some(socket_path) = config.socket {
        GrpcBesServer::unix_domain_socket(socket_path)
    } else {
        GrpcBesServer::listen(config.listen)
    };

    server
        .bes_service(nbes_service)
        .serve(shutdown_signal)
        .await?;

    info!("shutting down");

    Ok(())
}
