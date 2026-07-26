use anyhow::Result;
use log::{info, warn};
use std::{
    fmt::Display,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    str::FromStr,
    time::Duration,
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
    pub concurrency_limit_per_connection: Option<usize>,
    pub load_shed_requests: bool,
    pub max_concurrent_streams: Option<u32>,
    pub max_connection_age: Option<Duration>,
    pub max_connection_age_grace: Option<Duration>,
}

pub struct ServerTlsConfig {
    pub certificate: PathBuf,
    pub key: PathBuf,
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
            if backend.r#async { " (async)" } else { "" }
        );
        nbes_service.add_backend(backend)?;
    }

    let mut server = GrpcBesServer::listen(config.listen);

    if let Some(tls_config) = config.server_tls_config {
        server = server.tls_config(
            &tls_config.certificate,
            &tls_config.key,
            Vec::default(),
            false,
        )?;
    }

    if let Some(concurrency_limit_per_connection) = config.concurrency_limit_per_connection {
        server = server.concurrency_limit_per_connection(concurrency_limit_per_connection);
    }

    if config.load_shed_requests {
        server = server.load_shed_requests(true);
    }

    if let Some(max_concurrent_streams) = config.max_concurrent_streams {
        server = server.max_concurrent_streams(max_concurrent_streams);
    }

    if let Some(max_connection_age) = config.max_connection_age {
        server = server.max_connection_age(max_connection_age);
    }

    if let Some(max_connection_age_grace) = config.max_connection_age_grace {
        server = server.max_connection_age_grace(max_connection_age_grace);
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

impl Display for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Binding::SocketAddr(socket_addr) => {
                write!(f, "{socket_addr}")
            }
            Binding::UnixDomainSocket(socket_path) => {
                write!(f, "unix:{}", socket_path.display())
            }
        }
    }
}

impl FromStr for Binding {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(if s.starts_with("unix:/") {
            let socket_path = Url::parse(s)?
                .to_file_path()
                .map_err(|_| anyhow::anyhow!("invalid unix domain socket path"))?;

            Binding::UnixDomainSocket(socket_path)
        } else {
            let address: SocketAddrV4 = s.parse()?;

            Binding::SocketAddr(SocketAddr::V4(address))
        })
    }
}
