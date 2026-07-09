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
    pub listen: Listen,
}

pub enum Listen {
    SocketAddr(SocketAddr),
    UnixDomainSocket(PathBuf),
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

    GrpcBesServer::listen(config.listen)
        .bes_service(nbes_service)
        .serve(shutdown_signal)
        .await?;

    info!("shutting down");

    Ok(())
}
