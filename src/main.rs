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
use std::{fs, path::Path, pin::Pin, str::FromStr};
use tokio::{net::UnixListener, sync::mpsc};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
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

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

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
        struct BackendHandle {
            name: String,
            request_tx: mpsc::Sender<PublishBuildToolEventStreamRequest>,
            response_rx: mpsc::Receiver<PublishBuildToolEventStreamResponse>,
        }

        struct State {
            /// Incoming request stream from Bazel
            incoming_requests: Streaming<PublishBuildToolEventStreamRequest>,
            backends: Vec<BackendHandle>,
        }

        let (metadata, _, mut incoming_requests) = request.into_parts();

        eprintln!("waiting for first request");

        // Wait for the first request before initiating connections to BES backends.
        let Some(first_request) = incoming_requests
            .message()
            .await
            .expect("failed to receive first message")
        else {
            return Ok(Response::new(Box::pin(stream::empty())));
        };

        eprintln!("received first request");

        // Validate the first request.
        let (first_stream_id, first_sequence_number) = match &first_request {
            PublishBuildToolEventStreamRequest {
                ordered_build_event:
                    Some(OrderedBuildEvent {
                        stream_id: Some(stream_id),
                        sequence_number,
                        ..
                    }),
                ..
            } => (stream_id.clone(), *sequence_number),
            _ => {
                return Err(Status::invalid_argument(
                    "ordered_build_event field(s) are missing",
                ));
            }
        };

        // For each backend, pre-load the first request into an mpsc channel and
        // spawn a task that drives the gRPC stream. The spawned task calls
        // publish_build_tool_event_stream and forwards responses back via a
        // channel. Running in a spawned task ensures the tokio runtime can drive
        // both the outbound request stream and the wait for response headers
        // concurrently, breaking the deadlock where BuildBuddy won't send
        // response headers until it receives the first message.
        let mut backends: Vec<BackendHandle> = Vec::new();
        for backend in &self.backends {
            let (req_tx, req_rx) = mpsc::channel::<PublishBuildToolEventStreamRequest>(256);
            let (resp_tx, resp_rx) = mpsc::channel::<PublishBuildToolEventStreamResponse>(256);

            // Pre-load the first request so it's in the channel before the
            // spawned task opens the gRPC stream to the backend.
            req_tx
                .send(first_request.clone())
                .await
                .expect("failed to pre-load first request");

            let mut client = backend.client.as_ref().unwrap().clone();
            let backend_name = backend.name.clone();
            let cloned_metadata = metadata.clone();

            tokio::spawn(async move {
                let outbound = ReceiverStream::new(req_rx);
                let mut req = Request::new(outbound);
                copy_request_metadata(&cloned_metadata, &mut req);

                eprintln!("before client call");

                let mut response_stream = client
                    .publish_build_tool_event_stream(req)
                    .await
                    .expect("failed to initiate stream")
                    .into_inner();

                eprintln!("after client call");

                loop {
                    match response_stream.message().await {
                        Ok(Some(resp)) => {
                            eprintln!("received response!");
                            if resp_tx.send(resp).await.is_err() {
                                eprintln!("failed to send response");
                                break;
                            }
                        }
                        Ok(None) | Err(_) => {
                            eprintln!("none or err");
                            break;
                        }
                    }
                }
            });

            backends.push(BackendHandle {
                name: backend.name.clone(),
                request_tx: req_tx,
                response_rx: resp_rx,
            });
        }

        eprintln!("waiting for first backend responses");

        // Wait for responses to the first request from all backends.
        let first_responses: Vec<_> =
            join_all(backends.iter_mut().map(|b| b.response_rx.recv())).await;

        eprintln!("received first backend response");

        // Validate the first responses.
        for (i, response) in first_responses.iter().enumerate() {
            let backend_name = &backends[i].name;
            match response {
                Some(response) => {
                    let PublishBuildToolEventStreamResponse {
                        stream_id: Some(response_stream_id),
                        sequence_number: response_sequence_number,
                    } = response
                    else {
                        return Err(Status::internal(format!(
                            "response from bes backend {backend_name} is missing stream_id",
                        )));
                    };

                    if response_stream_id != &first_stream_id
                        || *response_sequence_number != first_sequence_number
                    {
                        eprintln!(
                            "warning: bes backend {} responded with unexpected stream id/sequence (expected={:?}/{}, actual = {:?}/{})",
                            backend_name,
                            first_stream_id,
                            first_sequence_number,
                            response_stream_id,
                            response_sequence_number
                        );
                        return Err(Status::internal(format!(
                            "bes backend {backend_name} responded with unexpected stream/sequence",
                        )));
                    }
                }
                None => {
                    eprintln!(
                        "bes backend {backend_name} unexpectedly ended stream {:?}",
                        first_stream_id
                    );
                    return Ok(Response::new(Box::pin(stream::empty())));
                }
            };
        }

        let first_response = Ok(PublishBuildToolEventStreamResponse {
            stream_id: Some(first_stream_id),
            sequence_number: first_sequence_number,
        });

        let state = State {
            incoming_requests,
            backends,
        };

        Ok(Response::new(Box::pin(
            stream::once(async move { first_response }).chain(unfold(
                state,
                |mut state| async move {
                    // Receive a request from the Bazel client
                    let request = state
                        .incoming_requests
                        .message()
                        .await
                        .expect("failed to receive message");

                    eprintln!("incoming request from bazel");

                    if let Some(request) = request {
                        let (stream_id, sequence_number) = match &request {
                            PublishBuildToolEventStreamRequest {
                                ordered_build_event:
                                    Some(OrderedBuildEvent {
                                        stream_id: Some(stream_id),
                                        sequence_number,
                                        ..
                                    }),
                                ..
                            } => (stream_id.clone(), *sequence_number),
                            _ => {
                                return Some((
                                    Err(Status::invalid_argument(
                                        "ordered_build_event field(s) are missing",
                                    )),
                                    state,
                                ));
                            }
                        };

                        // Forward the request to all backends
                        eprintln!("sending to backends");
                        for backend in &state.backends {
                            backend
                                .request_tx
                                .send(request.clone())
                                .await
                                .expect("failed to send message");
                        }

                        // Wait for a response from each backend
                        let responses: Vec<_> =
                            join_all(state.backends.iter_mut().map(|b| b.response_rx.recv()))
                                .await;

                        eprintln!("backends responded");

                        // Validate the responses
                        for (i, response) in responses.iter().enumerate() {
                            let backend_name = &state.backends[i].name;
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

                                    if response_stream_id != &stream_id
                                        || *response_sequence_number != sequence_number
                                    {
                                        eprintln!(
                                            "warning: bes backend {} responded with unexpected stream id/sequence (expected={:?}/{}, actual = {:?}/{})",
                                            backend_name,
                                            stream_id,
                                            sequence_number,
                                            response_stream_id,
                                            response_sequence_number
                                        );

                                        return Some((
                                            Err(Status::internal(format!(
                                                "bes backend {backend_name} responded with unexpected stream/sequence",
                                            ))),
                                            state,
                                        ));
                                    }
                                }
                                None => {
                                    eprintln!(
                                        "bes backend {backend_name} unexpectedly ended stream {:?}",
                                        stream_id
                                    );
                                    // End the stream for all backends. Consider making this more fault
                                    // tolerant and continue the other streams?
                                    return None;
                                }
                            };
                        }

                        // Send a single response back
                        return Some((
                            Ok(PublishBuildToolEventStreamResponse {
                                stream_id: Some(stream_id),
                                sequence_number,
                            }),
                            state,
                        ));
                    }

                    // The client closed the request stream. End the response stream.
                    None
                },
            )),
        )))
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

    eprintln!("starting server");

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
