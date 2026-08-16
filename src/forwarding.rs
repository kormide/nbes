use anyhow::{Context, Result};
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest, StreamId, publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::PublishBuildEvent,
};
use futures::{Stream, future::join_all, stream::unfold};
use hyper_util::rt::TokioIo;
use log::{error, info};
use rand::{RngExt, distr::Alphabetic};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    time::Duration,
};
use tokio::{
    net::UnixStream,
    sync::{
        mpsc::{self},
        watch::{self},
    },
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status, Streaming,
    metadata::{AsciiMetadataKey, KeyAndValueRef, MetadataMap, MetadataValue},
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
    pub remote_header_files: HashMap<AsciiMetadataKey, PathBuf>,
    pub r#async: bool,
    client: Option<PublishBuildEventClient<Channel>>,
    uds_tls_uri: Option<Url>,
    tls_client_identity: Option<TlsClientKeyPair>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    request_buffer_size: usize,
}

struct TlsClientKeyPair {
    certificate: PathBuf,
    key: PathBuf,
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

pub struct BesBackendBuilder {
    name: Option<String>,
    endpoint: Url,
    remote_headers: MetadataMap,
    remote_header_files: HashMap<AsciiMetadataKey, PathBuf>,
    r#async: bool,
    tls_client_identity: Option<TlsClientKeyPair>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    request_buffer_size: usize,
}

/// Current state of a the build tool event response stream
struct BuildToolResponseStreamState {
    /// Response streams from the bes backends
    incoming_responses: Vec<(String, Streaming<PublishBuildToolEventStreamResponse>)>,
    /// Identifier of the build stream
    stream_id: Option<StreamId>,
    /// Next request sequence to process
    next_seq: i64,
    /// The currently known latest sequence since requests are processed concurrently
    latest_seq: i64,
    /// An error a the latest known sequence, if any
    error_at_latest: Option<Status>,
    /// Whether the request stream has already completed
    request_stream_completed: bool,
    /// A channel used to watch the latest status of the request stream processing
    request_watch_rx: watch::Receiver<BuildToolRequestStreamState>,
    /// Whether we controlled the ending of a stream, either by finishing it or returning
    /// an error. If this is false then the grpc stream unexpectedly ended, for example,
    /// if the client sends a RST_STREAM.
    controlled_exit: bool,
}

/// Current state of the build tool event request stream
#[derive(Debug)]
struct BuildToolRequestStreamState {
    /// The stream id, only available after the first message is received.
    stream_id: Option<StreamId>,
    /// The starting sequence number of the stream
    starting_sequence: Option<i64>,
    /// Lastest request sequence number processed
    latest_sequence: i64,
    /// Error status to return as a response for the latest_sequence.
    /// A Some value indicates that request processing has halted and the
    /// latest sequence number will no longer advance.
    error: Option<Status>,
}

impl BesBackend {
    pub fn builder(endpoint: impl Into<String>) -> Result<BesBackendBuilder> {
        BesBackendBuilder::new(endpoint.into())
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
                let key_pem = fs::read_to_string(&tls_client_identity.key)
                    .context("failed to read tls client key")?;
                client_tls = client_tls.identity(Identity::from_pem(&cert_pem, &key_pem));
            }
            channel = channel.tls_config(client_tls)?;
        }

        if let Some(timeout) = self.connect_timeout {
            channel = channel.connect_timeout(timeout);
        }

        if let Some(timeout) = self.request_timeout {
            channel = channel.timeout(timeout);
        }

        // TODO: properties to potentially configure
        // let channel = channel
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

    pub fn set_async(&mut self, r#async: bool) {
        self.r#async = r#async;
    }

    pub fn set_client_tls_identity(&mut self, certificate: PathBuf, key: PathBuf) {
        self.tls_client_identity
            .replace(TlsClientKeyPair { certificate, key });
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

    /// Process each incoming event, fanning it out to each backend and communicating
    /// the current state over a watch stream that the response stream observes.
    async fn process_request_stream(
        mut incoming_request_stream: Streaming<PublishBuildToolEventStreamRequest>,
        request_watch_tx: watch::Sender<BuildToolRequestStreamState>,
        be_request_txs: Vec<mpsc::Sender<PublishBuildToolEventStreamRequest>>,
    ) -> Result<()> {
        let mut seq = 0;
        let mut original_stream_id: Option<StreamId> = None;
        let mut starting_seq: Option<i64> = None;
        loop {
            let request = incoming_request_stream
                .message()
                .await
                .context(format!("failed to receive event from client stream"))?;

            match request {
                Some(request) => {
                    let original_request = request.clone();

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
                        request_watch_tx
                            .send(BuildToolRequestStreamState {
                                stream_id: None,
                                starting_sequence: None,
                                latest_sequence: seq,
                                error: Some(Status::internal(
                                    "ordered_build_event field(s) are missing",
                                )),
                            })
                            .context("failed to send request stream status to response handler")
                            .inspect_err(|e| error!("{e}"))?;
                        break;
                    };

                    if original_stream_id.is_none() {
                        // Render the stream started message here rather than below in the
                        // response handling, which is blocked from starting if the bes
                        // backend consumes the full stream before sending responses.
                        info!(
                            "[invocation_id={}] started build tool event stream at seq {}",
                            stream_id.invocation_id, sequence_number,
                        );
                        original_stream_id.replace(stream_id.clone());
                        starting_seq.replace(sequence_number);
                        seq = sequence_number;
                    } else {
                        if Some(stream_id) != original_stream_id.as_ref() {
                            error!(
                                "[invocation_id={}] received inconsistent stream id from client",
                                stream_id.invocation_id
                            );
                            request_watch_tx
                                .send(BuildToolRequestStreamState {
                                    stream_id: original_stream_id.clone(),
                                    starting_sequence: starting_seq.clone(),
                                    latest_sequence: sequence_number,
                                    error: Some(Status::invalid_argument(
                                        "received inconsistent stream id from client",
                                    )),
                                })
                                .context("failed to send request stream status to response handler")
                                .inspect_err(|e| error!("{e}"))?;
                            break;
                        }

                        let next_seq = seq + 1;
                        if sequence_number != next_seq {
                            error!(
                                "[invocation_id={}] received seq {sequence_number} from client but expected {next_seq}",
                                stream_id.invocation_id,
                            );
                            request_watch_tx
                                .send(BuildToolRequestStreamState {
                                    stream_id: original_stream_id.clone(),
                                    starting_sequence: starting_seq.clone(),
                                    // indicate error occurrent at what should be the next sequance
                                    latest_sequence: seq + 1,
                                    error: Some(Status::invalid_argument(format!(
                                        "expected seq {next_seq} but received {sequence_number}"
                                    ))),
                                })
                                .context("failed to send request stream status to response handler")
                                .inspect_err(|e| error!("{e}"))?;
                            break;
                        }
                        seq = next_seq;
                    }

                    // Send the request to all backends
                    join_all(
                        be_request_txs
                            .iter()
                            .map(|be_request_tx| be_request_tx.send(original_request.clone())),
                    )
                    .await
                    .into_iter()
                    .collect::<Result<Vec<()>, _>>()
                    .context("backend receiver stream unexpectedly closed")?;

                    request_watch_tx
                        .send(BuildToolRequestStreamState {
                            stream_id: Some(stream_id.clone()),
                            starting_sequence: starting_seq.clone(),
                            latest_sequence: sequence_number,
                            error: None,
                        })
                        .context("failed to send request stream status to response handler")
                        .inspect_err(|e| error!("{e}"))?;
                }
                None => {
                    break;
                }
            };
        }

        Ok::<(), anyhow::Error>(())
        // be_request_tx drops here, ending the backend request streams
    }

    /// Asyncronously process responses for a built tool event stream, detached from the
    /// stream returned to the client.
    async fn process_response_stream_async(
        mut client: PublishBuildEventClient<Channel>,
        request: Request<ReceiverStream<PublishBuildToolEventStreamRequest>>,
        backend_name: String,
    ) -> Result<()> {
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
    }

    /// Process the response in the in the build tool event stream.
    async fn process_response_stream(
        mut state: BuildToolResponseStreamState,
    ) -> Option<(
        Result<PublishBuildToolEventStreamResponse, Status>,
        BuildToolResponseStreamState,
    )> {
        // Only query the watch channel if there's something we need:
        // - we don't have a stream id yet
        // - we have sent responses for requests up to the current latest sequence
        // Otherwise, continue to process responses since we have enough info for now.
        while !state.request_stream_completed
            && (state.stream_id.is_none() || state.next_seq > state.latest_seq)
        {
            // watch sender has closed -> request stream ended
            if let Err(_) = state.request_watch_rx.changed().await {
                state.request_stream_completed = true;
            }
            let request_stream_status = state.request_watch_rx.borrow_and_update();
            state.latest_seq = request_stream_status.latest_sequence;
            state.error_at_latest = request_stream_status.error.clone();

            if state.stream_id.is_none() {
                state.stream_id.replace(
                    request_stream_status
                        .stream_id
                        .clone()
                        .expect("expected stream_id to exist"),
                );
                state.next_seq = request_stream_status
                    .starting_sequence
                    .clone()
                    .expect("expected starting seq to exist");
            }
        }

        if state.error_at_latest.is_some() && state.next_seq >= state.latest_seq {
            // The request stream failed at the latest sequence and we've processed
            // responses up to that sequence. End the response stream with the failure
            // it returned.
            state.mark_controlled_exit();
            return Some((Err(state.error_at_latest.take().unwrap()), state));
        }

        if state.request_stream_completed && state.next_seq > state.latest_seq {
            info!(
                "[invocation_id={}] completed build tool event stream",
                state.iid()
            );
            state.mark_controlled_exit();
            return None;
        }

        // Wait for a corresponding response from each bes backend
        let responses = join_all(state.incoming_responses.iter_mut().map(|r| r.1.message())).await;

        for (i, response) in responses.iter().enumerate() {
            match response {
                Ok(response) => {
                    match response {
                        Some(response) => {
                            let PublishBuildToolEventStreamResponse {
                                stream_id: Some(response_stream_id),
                                sequence_number: response_seq,
                            } = response
                            else {
                                state.mark_controlled_exit();
                                return Some((
                                    Err(Status::internal(format!(
                                        "response from bes backend {} is missing stream_id",
                                        state.incoming_responses[i].0
                                    ))),
                                    state,
                                ));
                            };

                            if let Err(status) = Self::validate_build_event_ack_response(
                                &state.incoming_responses[i].0,
                                state.stream_id.as_ref().unwrap(),
                                state.next_seq,
                                response_stream_id,
                                *response_seq,
                            ) {
                                state.mark_controlled_exit();
                                return Some((Err(status), state));
                            }
                        }
                        None => {
                            error!(
                                "[invocation_id={}] bes backend {} unexpectedly ended stream on sequence {}",
                                state.iid(),
                                state.incoming_responses[i].0,
                                state.next_seq,
                            );
                            state.mark_controlled_exit();
                            // End the stream for all backends. Consider making this more fault
                            // tolerant and continue the other streams?
                            return None;
                        }
                    };
                }
                Err(status) => {
                    error!(
                        "[invocation_id={}] failed to receive build event from backend {}: {status}",
                        state.iid(),
                        state.incoming_responses[i].0,
                    );
                    state.mark_controlled_exit();
                    return Some((
                        Err(Status::with_details_and_metadata(
                            status.code(),
                            format!(
                                "{} failed event stream request: {}",
                                state.incoming_responses[i].0,
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

        let sequence_number = state.next_seq;
        state.next_seq += 1;

        return Some((
            Ok(PublishBuildToolEventStreamResponse {
                stream_id: state.stream_id.clone(),
                sequence_number: sequence_number,
            }),
            state,
        ));
    }

    fn load_remote_header_files(
        &self,
        remote_header_files: &HashMap<AsciiMetadataKey, PathBuf>,
        backend_name: &str,
    ) -> Result<MetadataMap, Status> {
        let mut metadata = MetadataMap::new();
        for (key, path) in remote_header_files {
            match fs::read_to_string(path) {
                Ok(value) => match MetadataValue::from_str(value.trim_end()) {
                    Ok(mut value) => {
                        value.set_sensitive(true);
                        metadata.insert(key, value);
                    }
                    Err(_) => {
                        let error_msg = format!(
                            "remote header from file {} for backend {} has invalid value",
                            path.display(),
                            backend_name,
                        );
                        error!("{error_msg}");
                        return Err(Status::internal(error_msg));
                    }
                },
                Err(e) => {
                    let error_msg = format!(
                        "failed to load remote header file {} for backend {}: {e}",
                        path.display(),
                        backend_name,
                    );
                    error!("{error_msg}");
                    return Err(Status::internal(error_msg));
                }
            }
        }
        Ok(metadata)
    }
}

#[tonic::async_trait]
impl PublishBuildEvent for BesForwardingService {
    type PublishBuildToolEventStreamStream = PublishBuildToolEventStreamStream;

    async fn publish_build_tool_event_stream(
        &self,
        request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<PublishBuildToolEventStreamStream>, Status> {
        let (metadata, _, incoming_request_stream) = request.into_parts();

        // Send each incoming request to the receiver stream that feeds the outgoing request
        // stream for each backend. Sending the requests must be done asynchronously from
        // processing responses because some bes backends, e.g., Buildbuddy, will wait for
        // all requests to be received before sending any responses. The channel buffer size
        // adds back pressure. If we receive requests too quickly for backends to process,
        // the buffers will be filled until one of the backends starts blocking.
        let (be_request_txs, mut be_request_rxs): (Vec<_>, Vec<_>) = self
            .backends
            .iter()
            .map(|b| mpsc::channel::<PublishBuildToolEventStreamRequest>(b.request_buffer_size))
            .unzip();

        // Watch channel that contains the latest status of the request stream. This is an
        // optimization over buffering requests in a channel to send for response processing,
        // which can cause en entire bes stream to sit buffered in memory if the backend chooses
        // to not send responses before processing the full stream. To send response ACKs, we only
        // need to know to what sequence the request stream has progressed.
        let (request_watch_tx, request_watch_rx) = watch::channel(BuildToolRequestStreamState {
            stream_id: None,
            starting_sequence: None,
            latest_sequence: 0,
            error: None,
        });

        tokio::spawn(Self::process_request_stream(
            incoming_request_stream,
            request_watch_tx,
            be_request_txs,
        ));

        // Create the response streams for each bes backend
        let mut incoming_responses: Vec<_> = Vec::new();
        for backend in &self.backends {
            let be_request_rx = be_request_rxs.pop().unwrap();
            let metadata = metadata.clone();
            let outbound_requests = ReceiverStream::new(be_request_rx);
            let mut request = Request::new(outbound_requests);

            let remote_header_files =
                self.load_remote_header_files(&backend.remote_header_files, &backend.name)?;

            copy_request_metadata(&metadata, &mut request);
            copy_request_metadata(&backend.remote_headers, &mut request);
            copy_request_metadata(&remote_header_files, &mut request);

            let backend_name = backend.name.clone();

            if backend.r#async {
                // Detach response processing, not returned to the client
                let client = backend.client.as_ref().unwrap().clone();
                tokio::spawn(Self::process_response_stream_async(
                    client,
                    request,
                    backend_name,
                ));
            } else {
                // Store the response streams for synchronous response-by-response handling below
                incoming_responses.push((
                    backend_name,
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

        let state = BuildToolResponseStreamState {
            incoming_responses,
            stream_id: None,
            next_seq: 0,
            latest_seq: 0,
            error_at_latest: None,
            request_stream_completed: false,
            request_watch_rx,
            controlled_exit: false,
        };

        Ok(Response::new(Box::pin(unfold(
            state,
            Self::process_response_stream,
        ))))
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

            let remote_header_files =
                self.load_remote_header_files(&backend.remote_header_files, &backend.name)?;

            copy_request_metadata(&metadata, &mut outbound_request);
            copy_request_metadata(&backend.remote_headers, &mut outbound_request);
            copy_request_metadata(&remote_header_files, &mut outbound_request);

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

impl BuildToolResponseStreamState {
    /// Get the invocation ID (if known), or "unknown"
    fn iid(&self) -> &str {
        self.stream_id
            .as_ref()
            .map(|sid| sid.invocation_id.as_str())
            .unwrap_or("unknown")
    }

    /// Indicate that the repsonse stream is ending on our terms by
    /// explicitly ending the stream or returning an error.
    fn mark_controlled_exit(&mut self) {
        self.controlled_exit = true;
    }
}

impl Drop for BuildToolResponseStreamState {
    /// Log an error if the stream was cancelled unexpectedly
    fn drop(&mut self) {
        if !self.controlled_exit {
            error!(
                "[invocation_id={}] build tool event stream was cancelled",
                self.iid()
            );
        }
    }
}

impl BesBackendBuilder {
    fn new(endpoint: String) -> Result<Self> {
        Ok(Self {
            name: None,
            endpoint: Self::parse_endpoint(endpoint)?,
            remote_headers: MetadataMap::new(),
            remote_header_files: HashMap::new(),
            r#async: false,
            tls_client_identity: None,
            connect_timeout: None,
            request_timeout: None,
            request_buffer_size: 500,
        })
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name.replace(name.into());
        self
    }

    pub fn remote_headers(mut self, remote_headers: MetadataMap) -> Self {
        self.remote_headers = remote_headers;
        self
    }

    pub fn remote_header_files(
        mut self,
        remote_header_files: HashMap<AsciiMetadataKey, PathBuf>,
    ) -> Self {
        self.remote_header_files = remote_header_files;
        self
    }

    pub fn r#async(mut self, r#async: bool) -> Self {
        self.r#async = r#async;
        self
    }

    pub fn tls_client_identity(mut self, certificate: PathBuf, key: PathBuf) -> Self {
        self.tls_client_identity
            .replace(TlsClientKeyPair { certificate, key });
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout.replace(timeout);
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout.replace(timeout);
        self
    }

    pub fn request_buffer_size(mut self, size: usize) -> Self {
        self.request_buffer_size = size;
        self
    }

    pub fn build(self) -> BesBackend {
        let name = self.name.unwrap_or_else(|| {
            rand::rng()
                .sample_iter(&Alphabetic)
                .take(8)
                .map(char::from)
                .map(|c| c.to_ascii_lowercase())
                .collect::<String>()
        });
        BesBackend {
            name,
            endpoint: self.endpoint,
            remote_headers: self.remote_headers,
            remote_header_files: self.remote_header_files,
            r#async: self.r#async,
            client: None,
            uds_tls_uri: None,
            tls_client_identity: None,
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            request_buffer_size: self.request_buffer_size,
        }
    }

    fn parse_endpoint(mut endpoint: String) -> Result<Url> {
        // A missing scheme is not a valid Url, so prepend the
        // default before parsing
        if !endpoint.contains("://") && !endpoint.contains("unix:/") {
            endpoint.insert_str(0, "grpcs://");
        }

        Ok(endpoint
            .parse()
            .map_err(|_| anyhow::anyhow!("failed to parse backend endpoint {endpoint}"))
            .and_then(|mut url: Url| {
                if !["grpc", "grpcs", "unix"].contains(&url.scheme()) {
                    anyhow::bail!("backend endpoint {endpoint} has invalid scheme");
                }
                if url.scheme() != "unix" {
                    if url.host().is_none() {
                        anyhow::bail!("backend endpoint {endpoint} is missing host");
                    }
                    if url.port().is_none() {
                        url.set_port(if url.scheme() == "grpcs" {
                            Some(443)
                        } else {
                            Some(80)
                        })
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "failed to set default port on bes backend url {endpoint}"
                            )
                        })?;
                    }
                }

                Ok(url)
            })?)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bes_backend_url_endpoint() {
        let result = BesBackend::builder("grpc://127.0.0.1:3000");

        assert!(result.is_ok());
    }

    #[test]
    fn test_bes_backend_url_endpoint_defaults_scheme_to_grpcs_and_port_443() {
        let result = BesBackend::builder("127.0.0.1");

        assert!(result.is_ok());
        let backend = result.unwrap().build();
        assert_eq!("grpcs", backend.endpoint.scheme());
        assert_eq!(443, backend.endpoint.port().unwrap());
    }

    #[test]
    fn test_bes_backend_url_endpoint_defaults_port_for_grpc() {
        let result = BesBackend::builder("grpc://127.0.0.1");

        assert!(result.is_ok());
        let backend = result.unwrap().build();
        assert_eq!(80, backend.endpoint.port().unwrap());
    }

    #[test]
    fn test_bes_backend_url_endpoint_defaults_port_for_grpcs() {
        let result = BesBackend::builder("grpcs://127.0.0.1");

        assert!(result.is_ok());
        let backend = result.unwrap().build();
        assert_eq!(443, backend.endpoint.port().unwrap());
    }

    #[test]
    fn test_bes_backend_url_endpoint_invalid_scheme() {
        let result = BesBackend::builder("http://127.0.0.1");

        assert!(result.is_err());
    }

    #[test]
    fn test_bes_backend_invalid_endpoint() {
        let result = BesBackend::builder("foobar::21q34");

        assert!(result.is_err());
    }

    #[test]
    fn test_bes_backend_unix_domain_socket() {
        let result = BesBackend::builder("unix:/tmp/foobar");

        assert!(result.is_ok());
        let backend = result.unwrap().build();
        assert_eq!("unix", backend.endpoint.scheme());
        let result = backend.endpoint.to_file_path();
        assert!(result.is_ok());
        let path = result.unwrap();
        assert_eq!("/tmp/foobar", path.to_string_lossy());
    }

    #[test]
    fn test_bes_backend_unix_domain_socket_missing_scheme() {
        let result = BesBackend::builder("/tmp/foobar");

        assert!(result.is_err());
    }

    #[test]
    fn test_bes_backend_no_name_generates_random_name() {
        let b1 = BesBackend::builder("grpcs://127.0.0.1:3000")
            .unwrap()
            .build();
        let b2 = BesBackend::builder("grpcs://127.0.0.1:3000")
            .unwrap()
            .build();

        assert!(b1.name.len() == 8);
        assert!(b2.name.len() == 8);
        assert_ne!(b1.name, b2.name);
    }

    #[test]
    fn test_bes_backend_uses_provided_name() {
        let backend = BesBackend::builder("grpcs://127.0.0.1:3000")
            .unwrap()
            .name("foobar")
            .build();

        assert_eq!("foobar", backend.name);
    }
}
