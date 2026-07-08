use anyhow::{Context, Result};
use build_proto::google::devtools::build::v1::publish_build_event_server::{
    PublishBuildEvent, PublishBuildEventServer,
};
use std::{fs, net::SocketAddr, path::PathBuf};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Server, server::Router};

pub struct GrpcBesServer {
    address: Option<SocketAddr>,
    unix_domain_socket: Option<PathBuf>,
    router: Router,
}

pub struct GrpcBesServerConfig {
    address: Option<SocketAddr>,
    unix_domain_socket: Option<PathBuf>,
    server: Server,
}

impl GrpcBesServer {
    pub fn listen(address: SocketAddr) -> GrpcBesServerConfig {
        GrpcBesServerConfig {
            address: Some(address),
            unix_domain_socket: None,
            server: Server::builder(),
        }
    }

    pub fn unix_domain_socket(path: PathBuf) -> GrpcBesServerConfig {
        GrpcBesServerConfig {
            address: None,
            unix_domain_socket: Some(path),
            server: Server::builder(),
        }
    }

    pub async fn serve(self, shutdown_signal: impl Future<Output = ()>) -> Result<()> {
        if let Some(socket_path) = self.unix_domain_socket {
            if socket_path.exists() {
                fs::remove_file(&socket_path).context("failed to remove existing socket")?;
            }
            let socket_listener = UnixListener::bind(socket_path)?;
            let socket_stream = UnixListenerStream::new(socket_listener);
            self.router
                .serve_with_incoming_shutdown(socket_stream, shutdown_signal)
                .await?;
        } else if let Some(address) = self.address {
            self.router
                .serve_with_shutdown(address, shutdown_signal)
                .await?;
        } else {
            anyhow::bail!("expected an address or a unix domain socket to be configured")
        }

        Ok(())
    }
}

impl GrpcBesServerConfig {
    // TODO: properties to potentially configure
    // .concurrency_limit_per_connection(100)
    // .load_shed(true)
    // .max_concurrent_streams(Some(1000))
    // .add_service(PublishBuildEventServer::new(nbes_service));

    pub fn bes_service(mut self, service: impl PublishBuildEvent) -> GrpcBesServer {
        GrpcBesServer {
            address: self.address,
            unix_domain_socket: self.unix_domain_socket,
            router: self
                .server
                .add_service(PublishBuildEventServer::new(service)),
        }
    }
}
