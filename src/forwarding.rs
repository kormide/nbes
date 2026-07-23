use anyhow::{Context, Result};
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest, StreamId, publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::PublishBuildEvent,
};
use futures::{
    Stream,
    future::join_all,
    stream::{StreamExt, unfold},
};
use hyper_util::rt::TokioIo;
use log::{error, info};
use std::{
    fs,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
};
use tokio::{
    net::UnixStream,
    sync::{
        broadcast::{self},
        mpsc::{self},
    },
};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tonic::{
    Request, Response, Status, Streaming,
    metadata::{KeyAndValueRef, MetadataMap},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Uri},
};
use tower::service_fn;
use url::Url;

pub struct BesForwardingService {
    backends: Vec<BesBackend>,
    tls_certificates: Vec<PathBuf>,
}
pub struct BesBackend {
    pub name: String,
    pub endpoint: Url,
    pub remote_headers: MetadataMap,
    pub asynchronous: bool,
    client: Option<PublishBuildEventClient<Channel>>,
    uds_tls_uri: Option<Url>,
    tls_client_identity: Option<TlsClientKeyPair>,
}

struct TlsClientKeyPair {
    certificate: PathBuf,
    private_key: PathBuf,
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

impl BesBackend {
    pub fn new(name: String, endpoint: Url, remote_headers: MetadataMap) -> Self {
        Self {
            name,
            endpoint,
            remote_headers,
            asynchronous: false,
            client: None,
            uds_tls_uri: None,
            tls_client_identity: None,
        }
    }

    /// Set up a client for a gRPC channel to the bes backend that does not
    /// connect until first use.
    pub fn lazy_connect(&mut self, tls_certificates: Vec<&Path>) -> Result<()> {
        if self.client.is_some() {
            return Ok(());
        }

        let use_tls =
            ["grpcs", "https"].contains(&self.endpoint.scheme()) || self.uds_tls_uri.is_some();
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
            if let Some(endpoint) = self.uds_tls_uri.as_ref() {
                // Domain needs to be set to match certificate if connecting with
                // tls over a unix domain docket
                Endpoint::try_from(endpoint.to_string())?
            } else {
                // This url is ignored when connecting via a uds
                Endpoint::try_from("http://[::]:50051")?
            }
        } else {
            Endpoint::from_str(endpoint.as_str()).context(format!(
                "failed to parse endpoint for backend {}",
                self.name
            ))?
        };

        if use_tls {
            let mut client_tls = ClientTlsConfig::new().with_native_roots();
            // Add trusted server certificates
            for tls_certificate in tls_certificates {
                let cert_pem = fs::read_to_string(tls_certificate)
                    .context("failed to read tls certificate")?;
                client_tls = client_tls.ca_certificate(Certificate::from_pem(cert_pem));
            }
            // Add client tls cert/key for mTLS
            if let Some(tls_client_identity) = self.tls_client_identity.as_ref() {
                let cert_pem = fs::read_to_string(&tls_client_identity.certificate)
                    .context("failed to read tls client certificate")?;
                let key_pem = fs::read_to_string(&tls_client_identity.private_key)
                    .context("failed to read tls client key")?;
                client_tls = client_tls.identity(Identity::from_pem(&cert_pem, &key_pem));
            }
            channel = channel.tls_config(client_tls)?;
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

    pub fn set_async(&mut self, asynchronous: bool) {
        self.asynchronous = asynchronous;
    }

    pub fn set_client_tls_identity(&mut self, certificate: PathBuf, private_key: PathBuf) {
        self.tls_client_identity.replace(TlsClientKeyPair {
            certificate,
            private_key,
        });
    }

    pub fn use_uds_tls_uri(&mut self, endpoint: Url) {
        self.uds_tls_uri.replace(endpoint);
    }
}

impl BesForwardingService {
    pub fn new() -> Self {
        Self {
            backends: Vec::default(),
            tls_certificates: Vec::default(),
        }
    }

    pub fn add_backend(&mut self, mut backend: BesBackend) -> Result<()> {
        backend.lazy_connect(self.tls_certificates.iter().map(|p| p.as_path()).collect())?;
        self.backends.push(backend);
        Ok(())
    }

    pub fn add_tls_trusted_cert(&mut self, tls_certificate: PathBuf) {
        self.tls_certificates.push(tls_certificate);
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
impl PublishBuildEvent for BesForwardingService {
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
            /// Last sequence number processed
            last_seq: i64,
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
            copy_request_metadata(&backend.remote_headers, &mut request);

            if backend.asynchronous {
                let mut client = backend.client.as_ref().unwrap().clone();
                let backend_name = backend.name.clone();
                tokio::spawn(async move {
                    let response = client
                        .publish_build_tool_event_stream(request)
                        .await
                        .inspect_err(|e| {
                            error!(
                                "failed to initiate event stream with backend {}: {e}",
                                backend_name
                            )
                        })?;

                    let mut response_stream = response.into_inner();

                    let mut stream_id: Option<StreamId> = None;
                    loop {
                        match response_stream.message().await {
                            Ok(response) => match response {
                                Some(response) => {
                                    let PublishBuildToolEventStreamResponse {
                                        stream_id: Some(response_stream_id),
                                        ..
                                    } = response
                                    else {
                                        error!("");
                                        break;
                                    };

                                    stream_id.replace(response_stream_id);
                                }
                                None => {
                                    match stream_id {
                                        Some(stream_id) => {
                                            info!(
                                                "[invocation_id={}] asynchronously uploaded build tool event stream to backend {}",
                                                stream_id.invocation_id, backend_name
                                            );
                                        }
                                        None => {
                                            error!(
                                                "asynchronous build tool event stream unexpectedly closed by backend {}",
                                                backend_name
                                            );
                                        }
                                    }
                                    break;
                                }
                            },
                            Err(status) => {
                                match stream_id {
                                    Some(stream_id) => {
                                        error!(
                                            "[invocation_id={}] failed to upload asynchronous event stream to backend {}: {}",
                                            stream_id.invocation_id,
                                            backend_name,
                                            status.message()
                                        );
                                    }
                                    None => {
                                        error!(
                                            "asynchronous build tool event stream failed on first response from backend {}",
                                            backend_name
                                        );
                                    }
                                }
                                info!("finished asynchronous upload to backend {}", backend_name);
                                break;
                            }
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                });
            } else {
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
        }

        let state = State {
            incoming_responses,
            request_rx,
            stream_id: None,
            last_seq: 0,
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

                if state.stream_id.is_none() {
                    state.stream_id.replace(stream_id.clone());
                    state.last_seq = sequence_number;
                    info!(
                        "[invocation_id={}] started build tool event stream at seq {sequence_number}",
                        state.iid()
                    );
                } else {
                    if Some(stream_id) != state.stream_id.as_ref() {
                        error!(
                            "[invocation_id={}] received inconsistent stream id from client",
                            state.iid()
                        );
                        return Some((
                            Err(Status::invalid_argument(
                                "received inconsistent stream id from client",
                            )),
                            state,
                        ));
                    }
                    let next_seq = state.last_seq + 1;
                    if sequence_number != state.last_seq + 1 {
                        error!(
                            "[invocation_id={}] received seq {sequence_number} from client but expected {next_seq}",
                            state.iid()
                        );
                        return Some((
                            Err(Status::invalid_argument(format!(
                                "expected seq {next_seq} but received {sequence_number}"
                            ))),
                            state,
                        ));
                    }
                    state.last_seq = next_seq;
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
                            return Some((
                                Err(Status::with_details_and_metadata(
                                    status.code(),
                                    format!(
                                        "{} failed event stream request: {}",
                                        backend_name,
                                        status.message()
                                    ),
                                    status.details().iter().cloned().collect(),
                                    status.metadata().clone(),
                                )),
                                state,
                            ));
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

        // TODO: if `check_preceding_lifecycle_events_present` is set, a BES
        // backend is supposed to wait to have received a corresponding parent
        // event before processing. However, it's unclear what's meant by "before
        // processing". Should an error be returned? Should the call block until
        // the parent is received?
        //
        // The changesets where this flag was added don't provide additional context.
        // https://github.com/googleapis/googleapis/commit/a877d3d3a0fcf5f02d083796710a804583586012
        // https://github.com/bazelbuild/bazel/commit/14b5c41c29423866cd3f2ee3f7b69ff48241bd34
        //
        // Buildbuddy doesn't appear to do anything and also just forwards to proxies and acks.
        // https://github.com/buildbuddy-io/buildbuddy/blob/cc2b155e70e9fd6666dfb8bfeebd7118893e5b51/server/build_event_protocol/build_event_server/build_event_server.go#L51

        for backend in &self.backends {
            let mut outbound_request = Request::new(message.clone());
            copy_request_metadata(&metadata, &mut outbound_request);
            copy_request_metadata(&backend.remote_headers, &mut outbound_request);
            backend
                .client
                .as_ref()
                .unwrap()
                .clone() // cloning client is cheap
                .publish_lifecycle_event(outbound_request)
                .await
                .map_err(|status| {
                    Status::with_details_and_metadata(
                        status.code(),
                        format!(
                            "{} failed lifecycle request: {}",
                            backend.name,
                            status.message()
                        ),
                        status.details().iter().cloned().collect(),
                        status.metadata().clone(),
                    )
                })?;
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
