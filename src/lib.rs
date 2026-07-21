use anyhow::Result;
use log::{info, warn};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
};
use url::Url;

use crate::{
    forwarding::{BesBackend, BesForwardingService},
    server::GrpcBesServer,
};

pub mod forwarding;
pub mod server;

#[derive(Default)]
pub struct Config {
    pub bes_backends: Vec<BesBackend>,
    pub listen: Binding,
    pub server_tls_config: Option<ServerTlsConfig>,
    pub tls_certificates: Vec<PathBuf>,
}

pub struct ServerTlsConfig {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Clone)]
pub enum Binding {
    SocketAddr(SocketAddr),
    UnixDomainSocket(PathBuf),
}

pub async fn run(config: Config, shutdown_signal: impl Future<Output = ()>) -> Result<()> {
    let mut nbes_service = BesForwardingService::new();
    for tls_certificate in config.tls_certificates {
        nbes_service.add_tls_trusted_cert(tls_certificate);
    }

    if config.bes_backends.is_empty() {
        warn!("no bes backends configured; bes events will be swallowed by a black hole");
    }

    for backend in config.bes_backends {
        info!(
            "configured backend {} -> {}{}",
            backend.name,
            backend.endpoint,
            if backend.asynchronous { " (async)" } else { "" }
        );
        nbes_service.add_backend(backend)?;
    }

    let mut server = GrpcBesServer::listen(config.listen);

    if let Some(tls_config) = config.server_tls_config {
        server = server.tls_config(&tls_config.certificate, &tls_config.private_key)?;
    }

    server
        .bes_service(nbes_service)
        .serve(shutdown_signal)
        .await?;

    info!("shutting down");

    Ok(())
}

impl Default for Binding {
    fn default() -> Self {
        Binding::SocketAddr(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(0, 0, 0, 0),
            9000,
        )))
    }
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
