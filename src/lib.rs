use anyhow::{Context, Result};
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest, StreamId,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use futures::{
    Stream,
    future::join_all,
    stream::{StreamExt, unfold},
};
use hyper_util::rt::TokioIo;
use log::{error, info, warn};
use std::{fs, net::SocketAddr, pin::Pin, str::FromStr};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{
        broadcast::{self},
        mpsc::{self},
    },
};
use tokio_stream::wrappers::{
    BroadcastStream, UnixListenerStream, errors::BroadcastStreamRecvError,
};
use tonic::{
    Request, Response, Status, Streaming,
    metadata::{KeyAndValueRef, MetadataMap},
    transport::{Channel, ClientTlsConfig, Endpoint, Server, Uri},
};
use tower::service_fn;
use url::Url;

pub struct Config {
    pub bes_backends: Vec<BesBackend>,
    pub listen: SocketAddr,
    pub socket: Option<Url>,
}

struct NBesService {
    backends: Vec<BesBackend>,
}

pub struct BesBackend {
    pub name: String,
    pub endpoint: Url,
    pub client: Option<PublishBuildEventClient<Channel>>,
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

impl BesBackend {
    /// Set up a client for a gRPC channel to the bes backend that does not
    /// connect until first use.
    pub fn lazy_connect(&mut self) -> Result<()> {
        if self.client.is_some() {
            return Ok(());
        }

        let use_tls = ["grpcs", "https"].contains(&self.endpoint.scheme());
        let use_uds = self.endpoint.scheme() == "unix";

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
        };

        let mut channel = if use_uds {
            // This url is ignored when connecting via a uds
            Endpoint::try_from("http://[::]:50051")?
        } else {
            Endpoint::from_str(endpoint.as_str()).context(format!(
                "failed to parse endpoint for backend {}",
                self.name
            ))?
        };

        if use_tls {
            channel = channel.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }

        // TODO: properties to potentially configure
        // let channel = channel
        // .connect_timeout(Duration::from_secs(10))
        // .tcp_keepalive(tcp_keepalive)
        // .tcp_keepalive_interval(tcp_keepalive_interval)
        // .tcp_keepalive_retries(tcp_keepalive_retries)
        // .concurrency_limit(limit)
        // .rate_limit(limit, duration)
        // .http2_keep_alive_interval(interval)
        // .keep_alive_while_idle(enabled)
        // .keep_alive_timeout(duration)

        let channel = if use_uds {
            let uds_path = endpoint.to_file_path().map_err(|_| {
                anyhow::anyhow!("failed to convert url {} to file path", endpoint.as_str())
            })?;
            channel.connect_with_connector_lazy(service_fn(move |_: Uri| {
                let uds_path = uds_path.clone();
                async move {
                    Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                }
            }))
        } else {
            channel.connect_lazy()
        };

        self.client.replace(PublishBuildEventClient::new(channel));
        Ok(())
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

    fn validate_build_event_ack_response(
        backend_name: &str,
        stream_id: &StreamId,
        seq: i64,
        response_stream_id: &StreamId,
        response_seq: i64,
    ) -> Result<(), Status> {
        if response_stream_id != stream_id {
            error!(
                "[invocation_id={}] bes backend {backend_name} responded with unexpected stream id {:?}",
                stream_id.invocation_id, stream_id
            );
            return Err(Status::internal(format!(
                "bes backend {backend_name} responded with unexpected stream id",
            )));
        }

        if response_seq != seq {
            error!(
                "[invocation_id={}] bes backend {backend_name} responded with unexpected sequence, expected {seq} but found {response_seq}",
                stream_id.invocation_id,
            );
            return Err(Status::internal(format!(
                "bes backend {backend_name} responded with unexpected sequence",
            )));
        }

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
            /// Response streams from the bes backends
            incoming_responses: Vec<(String, Streaming<PublishBuildToolEventStreamResponse>)>,
            /// Receiver to handle requests serially
            request_rx: mpsc::Receiver<PublishBuildToolEventStreamRequest>,
            /// Identifier of the build stream
            stream_id: Option<StreamId>,
        }

        impl State {
            fn iid(&self) -> &str {
                self.stream_id
                    .as_ref()
                    .map(|sid| sid.invocation_id.as_str())
                    .unwrap_or("unknown")
            }
        }

        let (metadata, _, mut incoming_request_stream) = request.into_parts();

        // Broadcast channel to send incoming requests to each backend's receiver stream
        // immediately. Transmitting the events is orthogonal to handling them because some
        // backends (e.g., Buildbuddy) will wait for the request stream to complete before
        // returning any response acks.
        let (be_request_tx, _) = broadcast::channel::<PublishBuildToolEventStreamRequest>(256);

        let mut be_request_rx: Vec<_> = self
            .backends
            .iter()
            .map(|_| be_request_tx.subscribe())
            .collect();

        // This channel lets us handle the request/response logic serially. Don't reuse the
        // broadcast channel to receive events because the slower handling may cause the
        // broadcast channel's buffer to fill up and drop events before they can be forwarded.
        let (request_tx, request_rx) = mpsc::channel::<PublishBuildToolEventStreamRequest>(10000);

        tokio::spawn({
            let has_backends = !self.backends.is_empty();
            async move {
                loop {
                    let request = incoming_request_stream
                        .message()
                        .await
                        .context(format!("failed to receive event from client stream"))?;

                    match request {
                        Some(request) => {
                            if has_backends {
                                // broadcast to all backends
                                be_request_tx
                                    .send(request.clone())
                                    .context("failed to broadcast request to backend receivers")?;
                            }
                            // send to the request/response handler
                            request_tx
                                .send(request)
                                .await
                                .context("failed to send request to handling receiver")?;
                        }
                        None => break,
                    };
                }
                Ok::<(), anyhow::Error>(())
                // be_request_tx drops here, ending the backend request streams
            }
        });

        // Create the response streams for each bes backend
        let mut incoming_responses: Vec<_> = Vec::new();
        for backend in &self.backends {
            let be_request_rx = be_request_rx.pop().unwrap();
            let metadata = metadata.clone();
            let outbound_requests = BroadcastStream::new(be_request_rx).map(|request| {
                match request {
                    Ok(request) => request,
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                        // This shouldn't happen, but panic if it does. Panicing will kill
                        // the tokio task handling the request, not the entire program.
                        panic!(
                            "error: request broadcast stream lagged and skipped {skipped} requests"
                        );
                    }
                }
            });

            let mut request = Request::new(outbound_requests);
            copy_request_metadata(&metadata, &mut request);

            incoming_responses.push((
                backend.name.clone(),
                backend
                    .client
                    .as_ref()
                    .unwrap()
                    .clone() // cloning clients is cheap
                    .publish_build_tool_event_stream(request)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "failed to initiate event stream with backend {}: {e}",
                            backend.name
                        )
                    })
                    .map(|response| response.into_inner())?,
            ));
        }

        let state = State {
            incoming_responses,
            request_rx,
            stream_id: None,
        };

        Ok(Response::new(Box::pin(unfold(state, |mut state| {
            async move {
                // Receive the next request from the bes client
                let request = match state.request_rx.recv().await {
                    Some(request) => request,
                    // The client closed the request stream, end the respoinse stream
                    None => {
                        info!(
                            "[invocation_id={}] completed build tool event stream",
                            state.iid()
                        );
                        return None;
                    }
                };

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

                if sequence_number == 1 {
                    state.stream_id.replace(stream_id.clone());
                    info!(
                        "[invocation_id={}] started build tool event stream",
                        state.iid()
                    );
                }

                // Wait for a corresponding response from each bes backend
                let responses =
                    join_all(state.incoming_responses.iter_mut().map(|r| r.1.message())).await;

                for (i, response) in responses.iter().enumerate() {
                    let backend_name = &state.incoming_responses[i].0;
                    match response {
                        Ok(response) => {
                            match response {
                                Some(response) => {
                                    let PublishBuildToolEventStreamResponse {
                                        stream_id: Some(response_stream_id),
                                        sequence_number: response_seq,
                                    } = response
                                    else {
                                        return Some((
                                            Err(Status::internal(format!(
                                                "response from bes backend {backend_name} is missing stream_id",
                                            ))),
                                            state,
                                        ));
                                    };

                                    if let Err(status) = Self::validate_build_event_ack_response(
                                        &backend_name,
                                        stream_id,
                                        sequence_number,
                                        response_stream_id,
                                        *response_seq,
                                    ) {
                                        return Some((Err(status), state));
                                    }
                                }
                                None => {
                                    error!(
                                        "[invocation_id={}] bes backend {backend_name} unexpectedly ended stream on sequence {sequence_number}",
                                        state.iid()
                                    );
                                    // End the stream for all backends. Consider making this more fault
                                    // tolerant and continue the other streams?
                                    return None;
                                }
                            };
                        }
                        Err(status) => {
                            error!(
                                "[invocation_id={}] failed to receive build event from backend {backend_name}: {status}",
                                state.iid()
                            );
                            return Some((Err(status.clone()), state));
                        }
                    };
                }

                return Some((
                    Ok(PublishBuildToolEventStreamResponse {
                        stream_id: Some(stream_id.clone()),
                        sequence_number: sequence_number,
                    }),
                    state,
                ));
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

pub async fn run(config: Config) -> Result<()> {
    let mut nbes_service = NBesService::new();

    if config.bes_backends.is_empty() {
        warn!("no bes backends configured; bes events will be swallowed by a black hole");
    }

    for backend in config.bes_backends {
        info!(
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

    if let Some(socket_url) = config.socket {
        let socket_path = socket_url.to_file_path().map_err(|_| {
            anyhow::anyhow!("failed to convert url {} to file path", socket_url.as_str())
        })?;
        if socket_path.exists() {
            fs::remove_file(&socket_path).context("failed to remove existing socket")?;
        }
        let socket_listener = UnixListener::bind(socket_path)?;
        let socket_stream = UnixListenerStream::new(socket_listener);
        router.serve_with_incoming(socket_stream).await?
    } else {
        router.serve(config.listen).await?;
    };

    Ok(())
}
