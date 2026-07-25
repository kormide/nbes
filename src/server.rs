use anyhow::{Context, Result};
use build_proto::google::devtools::build::v1::publish_build_event_server::{
    PublishBuildEvent, PublishBuildEventServer,
};
use std::{fs, path::Path};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig, server::Router};

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

    pub fn tls_config(
        mut self,
        certificate_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
        client_certificate_paths: Vec<Box<dyn AsRef<Path>>>,
        require_client_auth: bool,
    ) -> Result<Self> {
        let certificate =
            fs::read_to_string(certificate_path).context("failed to read tls certificate")?;
        let key =
            fs::read_to_string(key_path).context("failed to read tls private key")?;
        let mut tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(certificate, key))
            .client_auth_optional(require_client_auth);
        for client_certificate_path in client_certificate_paths {
            let client_certificate = fs::read_to_string(client_certificate_path.as_ref())
                .context("failed to read tls client certificate")?;
            let client_certificate = Certificate::from_pem(&client_certificate);
            tls_config = tls_config.client_ca_root(client_certificate);
        }
        self.server = self
            .server
            .tls_config(tls_config)
            .context("failed to configure server tls")?;
        Ok(self)
    }

    pub fn bes_service(mut self, service: impl PublishBuildEvent) -> GrpcBesServer {
        GrpcBesServer {
            binding: self.binding,
            router: self
                .server
                .add_service(PublishBuildEventServer::new(service)),
        }
    }
}
