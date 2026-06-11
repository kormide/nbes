use anyhow::{Context, Result};
use args::Args;
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use clap::Parser;
use futures::{Stream, stream::unfold};
use std::{env, pin::Pin, str::FromStr};
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Channel, Endpoint, Server},
};
use url::Url;

mod args;

struct NBesService {
    clients: Vec<PublishBuildEventClient<Channel>>,
}

struct BesBackend {
    name: String,
    endpoint: Url,
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

impl BesBackend {
    /// Open a gRPC channel and create a client for the bes backend.
    pub async fn connect(&self) -> Result<PublishBuildEventClient<Channel>> {
        let channel = Endpoint::from_str(self.endpoint.as_str())
            .context(format!(
                "failed to parse endpoint for backend {}",
                self.name
            ))?
            .connect()
            .await
            .context(format!("falied to connect to backend {}", self.name))?;

        Ok(PublishBuildEventClient::new(channel))
    }
}

impl NBesService {
    pub fn new() -> Self {
        Self {
            clients: Vec::default(),
        }
    }

    pub fn add_client(&mut self, client: PublishBuildEventClient<Channel>) {
        self.clients.push(client);
    }
}

#[tonic::async_trait]
impl PublishBuildEvent for NBesService {
    type PublishBuildToolEventStreamStream = PublishBuildToolEventStreamStream;

    async fn publish_build_tool_event_stream(
        &self,
        request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<PublishBuildToolEventStreamStream>, Status> {
        struct State {
            incoming: Streaming<PublishBuildToolEventStreamRequest>,
        }

        let state = State {
            incoming: request.into_inner(),
        };

        Ok(Response::new(Box::pin(unfold(
            state,
            |mut state| async move {
                let msg = state
                    .incoming
                    .message()
                    .await
                    .expect("failed to receive message");

                if let Some(PublishBuildToolEventStreamRequest {
                    ordered_build_event:
                        Some(OrderedBuildEvent {
                            stream_id,
                            sequence_number,
                            ..
                        }),
                    ..
                }) = msg
                {
                    return Some((
                        Ok(PublishBuildToolEventStreamResponse {
                            stream_id,
                            sequence_number,
                        }),
                        state,
                    ));
                }

                None
            },
        ))))
    }

    async fn publish_lifecycle_event(
        &self,
        request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        for client in &self.clients {
            client
                .clone() // cloning client is cheap
                .publish_lifecycle_event(Request::new(request.clone()))
                .await?;
        }

        Ok(Response::new(()))
    }
}

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

    for backend in &bes_backends {
        eprintln!(
            "configured backend {} -> {}",
            backend.name, backend.endpoint
        );
    }

    for backend in bes_backends {
        let client = backend.connect().await?;

        eprintln!("connected to backend {}", backend.name,);

        nbes_service.add_client(client);
    }

    Server::builder()
        .add_service(PublishBuildEventServer::new(nbes_service))
        .serve(address)
        .await?;

    Ok(())
}
