use anyhow::Result;
use log::{info, warn};
use std::{net::SocketAddr, path::PathBuf};
use url::Url;

use crate::{
    forwarding::{BesBackend, BesForwardingService},
    server::GrpcBesServer,
};

pub mod forwarding;
pub mod server;

pub struct Config {
    pub bes_backends: Vec<BesBackend>,
    pub listen: Binding,
}

#[derive(Clone)]
pub enum Binding {
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

impl Into<Url> for &Binding {
    fn into(self) -> Url {
        match self {
            Binding::SocketAddr(address) => {
                Url::parse(&address.to_string()).expect("failed to parse url from socket address")
            }
            Binding::UnixDomainSocket(socket_path) => {
                Url::parse(&format!("unix:{}", socket_path.display()))
                    .expect("failed to parse url from unix domain socket path")
            }
        }
    }
}
