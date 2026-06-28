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
use futures::{
    future::join_all,
    stream::{self, StreamExt},
};
use std::sync::Arc;
use std::{fs, path::Path, pin::Pin, str::FromStr};
use tokio::{net::UnixListener, sync::broadcast};
use tokio_stream::wrappers::{BroadcastStream, UnixListenerStream};
use tonic::{
    Request, Response, Status, Streaming,
    metadata::{KeyAndValueRef, MetadataMap},
    transport::{Channel, ClientTlsConfig, Endpoint, Server},
};
use url::Url;

mod args;

struct NBesService {
    backends: Vec<BesBackend>,
}

struct BesBackend {
    name: String,
    endpoint: Url,
    client: Option<PublishBuildEventClient<Channel>>,
}

type PublishBuildToolEventStreamStream =
    Pin<Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send>>;

impl BesBackend {
    /// Set up a client for a gRPC channel to the bes backend that does not
    /// connect until first use.
    pub fn lazy_connect(&mut self) -> Result<()> {
        Ok(match self.client {
            Some(_) => {}
            None => {
                // Tonic doesn't appear to like "grpcs" as a scheme. Swap it with
                // https for the client connection to avoid FRAME_SIZE_ERROR errors.
                let mut endpoint = self.endpoint.clone();
                if endpoint.scheme() == "grpcs" {
                    // Cannot call `set_scheme` to change form grpcs to https
                    // https://docs.rs/url/latest/url/struct.Url.html#method.set_scheme
                    endpoint = Url::parse(&format!(
                        "https://{}:{}",
                        endpoint.host_str().expect("bes endpoint is missing host"),
                        endpoint.port().expect("best endpoint is missing port")
                    ))?;
                }

                let channel = Endpoint::from_str(endpoint.as_str())
                    .context(format!(
                        "failed to parse endpoint for backend {}",
                        self.name
                    ))?
                    .tls_config(ClientTlsConfig::new().with_native_roots())?
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

                self.client.replace(PublishBuildEventClient::new(channel));
            }
        })
    }
}

impl NBesService {
    pub fn new() -> Self {
        Self {
            backends: Vec::default(),
        }
    }

    pub fn add_backend(&mut self, mut backend: BesBackend) -> Result<()> {
        backend.lazy_connect()?;
        self.backends.push(backend);
        Ok(())
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
            outbound_responses: Vec<(String, Streaming<PublishBuildToolEventStreamResponse>)>,
            num_backends: usize,
            clients: Vec<(String, PublishBuildEventClient<Channel>)>,
        }

        let (metadata, _, incoming_requests) = request.into_parts();

        // Open streams to each of the bes backends. Incoming requests are sent
        // via a broadcast channel. Each backend turns its receiver into a stream
        // to forward requests to the backend.
        let (tx, _) = broadcast::channel::<PublishBuildToolEventStreamRequest>(256);

        let clients = self
            .backends
            .iter()
            .map(|backend| {
                (
                    backend.name.clone(),
                    backend
                        .client
                        .as_ref()
                        .expect("expected client to exist")
                        .clone(),
                )
            })
            .collect();

        let state = State {
            incoming_requests,
            outbound_responses: Vec::new(),
            num_backends: self.backends.len(),
            clients,
        };

        Ok(Response::new(Box::pin(unfold(state, move |mut state| {
            let tx = tx.clone();
            let metadata = metadata.clone();
            async move {
                // Receive a request from the Bazel client
                let request = state
                    .incoming_requests
                    .message()
                    .await
                    .expect("failed to receive message");

                if let Some(request) = request {
                    let PublishBuildToolEventStreamRequest {
                        ordered_build_event:
                            Some(OrderedBuildEvent {
                                stream_id: Some(ref stream_id),
                                sequence_number,
                                ..
                            }),
                        ..
                    } = request
                    else {
                        return Some((
                            Err(Status::invalid_argument(
                                "ordered_build_event field(s) are missing",
                            )),
                            state,
                        ));
                    };

                    let mut receiver_streams: Vec<
                        Pin<Box<dyn Stream<Item = PublishBuildToolEventStreamRequest> + Send>>,
                    > = Vec::new();

                    if state.num_backends > 0 && state.outbound_responses.is_empty() {
                        for _ in 0..state.num_backends {
                            let receiver = tx.subscribe();
                            let stream = BroadcastStream::new(receiver).map(|request| {
                                // BroadcastStream wraps the request in a Result in case it fails to
                                // receive from the sender. But publish_build_tool_event_stream() takes
                                // a stream of requests, so we must unwrap it. This should "never" occur,
                                // so just panic.
                                request.expect("failed to receive request from broadcast channel")
                            });
                            receiver_streams.push(stream.boxed());
                        }
                    }

                    // Forward the request to all backends via the broadcast channel
                    if state.num_backends > 0 {
                        tx.send(request.clone()).expect("failed to send message");
                    }

                    if state.num_backends > 0 && state.outbound_responses.is_empty() {
                        let foobar = stream::iter(state.clients.clone().into_iter())
                            .then(|(name, mut client)| {
                                let receiver_stream = receiver_streams
                                    .pop()
                                    .expect("failed to pop receiver stream");
                                let metadata = metadata.clone();
                                async move {
                                    let mut request = Request::new(receiver_stream);
                                    copy_request_metadata(&metadata, &mut request);

                                    (
                                        name.clone(),
                                        client
                                            .publish_build_tool_event_stream(request)
                                            .await
                                            .expect("")
                                            .into_inner(),
                                    )
                                }
                            })
                            .collect::<Vec<_>>()
                            .await;

                        // for foo in foobar {
                        //     foo.1.await.expect("").into_inner();
                        // }
                        // https://github.com/dtolnay/async-trait/issues/212
                        //
                        // state.outbound_responses =
                    }

                    // Wait for a response from each backend
                    let responses: Vec<_> =
                        join_all(state.outbound_responses.iter_mut().map(|r| r.1.message()))
                            .await
                            .into_iter()
                            .map(|r| r.expect("failed to receive message from backend"))
                            .collect();

                    // Validate the responses
                    for (i, response) in responses.iter().enumerate() {
                        let backend_name = &state.outbound_responses[i].0;
                        match response {
                            Some(response) => {
                                let PublishBuildToolEventStreamResponse {
                                    stream_id: Some(response_stream_id),
                                    sequence_number: response_sequence_number,
                                } = response
                                else {
                                    return Some((
                                        Err(Status::internal(format!(
                                            "response from bes backend {backend_name} is missing stream_id",
                                        ))),
                                        state,
                                    ));
                                };

                                // if response_stream_id != stream_id
                                //     || *response_sequence_number != sequence_number
                                // {
                                //     eprintln!(
                                //         "warning: bes backend {} responded with unexpected stream id/sequence (expected={:?}/{}, actual = {:?}/{})",
                                //         backend_name,
                                //         stream_id,
                                //         sequence_number,
                                //         response_stream_id,
                                //         response_sequence_number
                                //     );
                                //
                                //     return Some((
                                //         Err(Status::internal(format!(
                                //             "bes backend {backend_name} responded with unexpected stream/sequence",
                                //         ))),
                                //         state,
                                //     ));
                                // }

                                return None;
                            }
                            None => {
                                // eprintln!(
                                //     "bes backend {backend_name} unexpectedly ended stream {stream_id:?}"
                                // );
                                // End the stream for all backends. Consider making this more fault
                                // tolerant and continue the other streams?
                                return None;
                            }
                        };
                    }

                    // Send a single response back
                    return Some((
                        Ok(PublishBuildToolEventStreamResponse {
                            // stream_id: Some(stream_id.clone()),
                            stream_id: None,
                            sequence_number: sequence_number,
                        }),
                        state,
                    ));
                }

                // The client closed the request stream. End the response stream.
                None
            }
        }))))
    }

    async fn publish_lifecycle_event(
        &self,
        request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        let (metadata, _, message) = request.into_parts();

        for backend in &self.backends {
            let mut outbound_request = Request::new(message.clone());
            copy_request_metadata(&metadata, &mut outbound_request);
            backend
                .client
                .as_ref()
                .unwrap()
                .clone() // cloning client is cheap
                .publish_lifecycle_event(outbound_request)
                .await?;
        }

        Ok(Response::new(()))
    }
}

fn copy_request_metadata<T>(metadata: &MetadataMap, to_request: &mut Request<T>) {
    for header in metadata.iter() {
        match header {
            KeyAndValueRef::Ascii(key, value) => {
                to_request.metadata_mut().append(key, value.clone());
            }
            KeyAndValueRef::Binary(key, value) => {
                to_request.metadata_mut().append_bin(key, value.clone());
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    eprintln!(
        "starting bes server on {}",
        args.socket
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or(args.listen.to_string())
    );

    let bes_backends: Vec<BesBackend> = args.bes_backends.into_iter().map(|b| b.into()).collect();

    let mut nbes_service = NBesService::new();

    if bes_backends.is_empty() {
        eprintln!("no bes backends configured; bes events will be swallowed by a black hole");
    }

    for backend in bes_backends {
        eprintln!(
            "configured backend {} -> {}",
            backend.name, backend.endpoint
        );
        nbes_service.add_backend(backend)?;
    }

    let router = Server::builder()
        // TODO: properties to potentially configure
        // .concurrency_limit_per_connection(100)
        // .load_shed(true)
        // .max_concurrent_streams(Some(1000))
        .add_service(PublishBuildEventServer::new(nbes_service));

    if let Some(socket_url) = args.socket {
        let socket_path = Path::new(socket_url.path());
        if socket_path.exists() {
            fs::remove_file(socket_path).context("failed to remove existing socket")?;
        }
        let socket_listener = UnixListener::bind(socket_path)?;
        let socket_stream = UnixListenerStream::new(socket_listener);
        router.serve_with_incoming(socket_stream).await?
    } else {
        router.serve(args.listen).await?;
    };

    Ok(())
}
