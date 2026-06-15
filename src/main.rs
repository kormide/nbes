use anyhow::{Context, Result};
use args::Args;
use build_proto::google::devtools::build::v1::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use clap::Parser;
use futures::{Stream, stream::unfold};
use futures::{
    future::join_all,
    stream::{self, StreamExt},
};
use std::{env, pin::Pin, str::FromStr};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
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
    /// Set up a client for a gRPC channel to the bes backend that does not
    /// connect until first use.
    pub fn lazy_connect(&self) -> Result<PublishBuildEventClient<Channel>> {
        let channel = Endpoint::from_str(self.endpoint.as_str())
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
            .connect_lazy();

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
            /// Incoming request stream from Bazel
            incoming_requests: Streaming<PublishBuildToolEventStreamRequest>,
            /// Incoming response streams from the bes backends
            incoming_responses: Vec<Streaming<PublishBuildToolEventStreamResponse>>,
        }

        // Open streams to each of the bes backends. Incoming requests are sent
        // via a broadcast channel. Each backend turns its receiver into a stream
        // to forward requests to the backend.
        let (tx, _) = broadcast::channel::<PublishBuildToolEventStreamRequest>(256);
        let incoming_responses = stream::iter(self.clients.iter())
            .then(|client| async {
                let receiver = tx.subscribe();
                let outbound_requests = BroadcastStream::new(receiver).map(|request| {
                    // BroadcastStream wraps the request in a Result in case it fails to
                    // receive from the sender. But publish_build_tool_event_stream() takes
                    // a stream of requests, so we must unwrap it. This should "never" occur,
                    // so just panic.
                    request.expect("failed to receive request from broadcast channel")
                });
                client
                    .clone() // cloning clients is cheap
                    .publish_build_tool_event_stream(Request::new(outbound_requests))
                    .await
                    .expect("failed to initiate stream")
                    .into_inner()
            })
            .collect()
            .await;

        let state = State {
            incoming_requests: request.into_inner(),
            incoming_responses,
        };

        Ok(Response::new(Box::pin(unfold(state, move |mut state| {
            let tx = tx.clone();
            async move {
                // Receive a request from the Bazel client
                let request = state
                    .incoming_requests
                    .message()
                    .await
                    .expect("failed to receive message");

                if let Some(request) = request {
                    // Forward the request to all backends via the broadcast channel
                    if !state.incoming_responses.is_empty() {
                        tx.send(request.clone()).expect("failed to send message");
                    }

                    // Wait for a response from each backend
                    let responses: Vec<_> =
                        join_all(state.incoming_responses.iter_mut().map(|r| r.message()))
                            .await
                            .into_iter()
                            .map(|r| r.expect("failed to receive message from backend"))
                            .collect();

                    // Validate the responses
                    for response in responses {
                        match response {
                            Some(_) => {
                                // TODO: validate received expected response
                            }
                            None => {
                                todo!("unexpected end of stream")
                            }
                        };
                    }

                    // Send a single response back
                    let build_event = request.ordered_build_event.expect("TODO");
                    return Some((
                        Ok(PublishBuildToolEventStreamResponse {
                            stream_id: build_event.stream_id,
                            sequence_number: build_event.sequence_number,
                        }),
                        state,
                    ));
                }
                None
            }
        }))))
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
        nbes_service.add_client(backend.lazy_connect()?);
    }

    Server::builder()
        // TODO: properties to potentially configure
        // .concurrency_limit_per_connection(100)
        // .load_shed(true)
        // .max_concurrent_streams(Some(1000))
        .add_service(PublishBuildEventServer::new(nbes_service))
        .serve(address)
        .await?;

    Ok(())
}
