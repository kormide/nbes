use anyhow::{Context, Result};
use build_proto::google::devtools::build::v1::publish_build_event_client::PublishBuildEventClient;
use std::str::FromStr;
use tonic::transport::{Channel, Endpoint};
use url::Url;

use crate::client::Client;

pub trait Backend {
    type Client: Client;

    fn name(&self) -> &str;
    fn endpoint(&self) -> &Url;
    fn create_client(&self) -> Result<Self::Client>;
}

pub struct BesBackend {
    pub name: String,
    pub endpoint: Url,
}

impl Backend for BesBackend {
    type Client = PublishBuildEventClient<Channel>;

    fn name(&self) -> &str {
        &self.name
    }

    fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Create up a client for a gRPC channel to the bes backend that does not
    /// connect until first use.
    fn create_client(&self) -> Result<PublishBuildEventClient<Channel>> {
        Ok(PublishBuildEventClient::new(
            Endpoint::from_str(self.endpoint.as_str())
                .context(format!(
                    "failed to parse endpoint for backend {}",
                    self.name
                ))?
                // TODO: properties to potentially configure
                // .connect_timeout(Duration::from_secs(10))
                // .tcp_keepalive(tcp_keepalive)
                // .tcp_keepalive_interval(tcp_keepalive_interval)
                // .tcp_keepalive_retries(tcp_keepalive_retries)
                // .concurrency_limit(limit)
                // .rate_limit(limit, duration)
                // .http2_keep_alive_interval(interval)
                // .keep_alive_while_idle(enabled)
                // .keep_alive_timeout(duration)
                .connect_lazy(),
        ))
    }
}

impl BesBackend {
    // pub fn lazy_connect(&mut self) -> Result<()> {
    //     Ok(match self.client {
    //         Some(_) => {}
    //         None => {
    //             let channel = Endpoint::from_str(self.endpoint.as_str())
    //                 .context(format!(
    //                     "failed to parse endpoint for backend {}",
    //                     self.name
    //                 ))?
    //                 // TODO: properties to potentially configure
    //                 // .connect_timeout(Duration::from_secs(10))
    //                 // .tcp_keepalive(tcp_keepalive)
    //                 // .tcp_keepalive_interval(tcp_keepalive_interval)
    //                 // .tcp_keepalive_retries(tcp_keepalive_retries)
    //                 // .concurrency_limit(limit)
    //                 // .rate_limit(limit, duration)
    //                 // .http2_keep_alive_interval(interval)
    //                 // .keep_alive_while_idle(enabled)
    //                 // .keep_alive_timeout(duration)
    //                 .connect_lazy();

    //             self.client.replace(PublishBuildEventClient::new(channel));
    //         }
    //     })
    // }
}
