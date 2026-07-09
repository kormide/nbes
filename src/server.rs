use anyhow::{Context, Result};
use build_proto::google::devtools::build::v1::publish_build_event_server::{
    PublishBuildEvent, PublishBuildEventServer,
};
use std::fs;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Server, server::Router};

use crate::Binding;

pub struct GrpcBesServer {
    binding: Binding,
    router: Router,
}

pub struct GrpcBesServerConfig {
    binding: Binding,
    server: Server,
}

impl GrpcBesServer {
    pub fn listen(binding: Binding) -> GrpcBesServerConfig {
        GrpcBesServerConfig {
            binding,
            server: Server::builder(),
        }
    }

    pub async fn serve(self, shutdown_signal: impl Future<Output = ()>) -> Result<()> {
        match self.binding {
            Binding::SocketAddr(address) => {
                self.router
                    .serve_with_shutdown(address, shutdown_signal)
                    .await?;
            }
            Binding::UnixDomainSocket(socket_path) => {
                if socket_path.exists() {
                    fs::remove_file(&socket_path).context("failed to remove existing socket")?;
                }
                let socket_listener = UnixListener::bind(socket_path)?;
                let socket_stream = UnixListenerStream::new(socket_listener);
                self.router
                    .serve_with_incoming_shutdown(socket_stream, shutdown_signal)
                    .await?;
            }
        }

        Ok(())
    }

    pub fn binding(&self) -> &Binding {
        &self.binding
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
            binding: self.binding,
            router: self
                .server
                .add_service(PublishBuildEventServer::new(service)),
        }
    }
}
