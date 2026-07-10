use anyhow::Result;
use build_proto::google::devtools::build::v1::{
    BuildEvent, BuildStatus, OrderedBuildEvent, PublishBuildToolEventStreamRequest,
    PublishBuildToolEventStreamResponse, PublishLifecycleEventRequest, StreamId,
    build_event::{
        BuildEnqueued, BuildFinished, Event, InvocationAttemptFinished, InvocationAttemptStarted,
    },
    build_status,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::PublishBuildEvent,
    publish_lifecycle_event_request::ServiceLevel,
    stream_id::BuildComponent,
};
use futures::{Stream, stream::unfold};
use hyper_util::rt::TokioIo;
use nbes::Binding;
use nbes::{Config, forwarding::BesBackend, server::GrpcBesServer};
use prost_types::Timestamp;
use std::{pin::Pin, sync::Arc};
use tempfile::NamedTempFile;
use tokio::{
    net::UnixStream,
    sync::{Mutex, oneshot},
    task::JoinHandle,
};
use tonic::{
    Request, Response, Status, Streaming,
    metadata::MetadataValue,
    transport::{Endpoint, Uri},
};
use tonic::{metadata::MetadataMap, transport::Channel};
use tower::service_fn;
use url::Url;

#[tokio::test]
pub async fn test_blackhole_acks_build_tool_events() -> Result<()> {
    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: Vec::default(),
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let request_stream = futures::stream::iter([
        build_tool_event(1),
        build_tool_event(2),
        build_tool_event(3),
        build_tool_event(4),
        build_tool_event(5),
    ]);

    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    let mut expected_seq = 1;
    while let Some(response) = response_stream.message().await? {
        assert_eq!(expected_seq, response.sequence_number);
        expected_seq += 1;
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;

    Ok(())
}

#[tokio::test]
pub async fn test_blackhole_acks_lifecycle_events() -> Result<()> {
    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: Vec::default(),
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    for lifecycle_event in standard_lifecycle_events() {
        client
            .publish_lifecycle_event(Request::new(lifecycle_event))
            .await?;
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;

    Ok(())
}

#[tokio::test]
pub async fn test_forward_responds_in_sequence() -> Result<()> {
    let server_uds = NamedTempFile::new()?;
    let mock_server =
        MockBesServer::spawn(Binding::UnixDomainSocket(server_uds.path().to_path_buf())).await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let request_stream = futures::stream::iter([
        build_tool_event(1),
        build_tool_event(2),
        build_tool_event(3),
        build_tool_event(4),
        build_tool_event(5),
    ]);

    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    let mut expected_seq = 1;
    while let Some(response) = response_stream.message().await? {
        assert_eq!(expected_seq, response.sequence_number);
        expected_seq += 1;
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    mock_server.shutdown().await;

    Ok(())
}

#[tokio::test]
pub async fn test_forward_acks_lifecycle_events() -> Result<()> {
    let server_uds = NamedTempFile::new()?;
    let mock_server =
        MockBesServer::spawn(Binding::UnixDomainSocket(server_uds.path().to_path_buf())).await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    for lifecycle_event in standard_lifecycle_events() {
        client
            .publish_lifecycle_event(Request::new(lifecycle_event))
            .await?;
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    mock_server.shutdown().await;

    Ok(())
}

#[tokio::test]
pub async fn test_forwards_headers() -> Result<()> {
    let server_uds = NamedTempFile::new()?;
    let mock_server =
        MockBesServer::spawn(Binding::UnixDomainSocket(server_uds.path().to_path_buf())).await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let request_stream = futures::stream::iter([build_tool_event(1)]);

    let mut request = Request::new(request_stream);

    // Add a "foo: bar" header to the request
    request
        .metadata_mut()
        .append("foo", MetadataValue::from_static("bar"));
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    while let Some(_) = response_stream.message().await? {}

    {
        // Header was included in the forwarded request
        let requests = mock_server.build_tool_event_stream_requests.lock().await;
        assert_eq!(1, requests.len());
        let header = requests[0].metadata.get("foo");
        assert!(header.is_some());
        assert_eq!(header.unwrap(), "bar");
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    mock_server.shutdown().await;

    Ok(())
}

/// Spawn nbes in a task and provide a oneshot channel to shut it down
async fn spawn_nbes(config: Config) -> (oneshot::Sender<()>, JoinHandle<Result<()>>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = tokio::spawn(nbes::run(config, async {
        shutdown_rx.await.unwrap();
    }));

    // Yield to allow the server to listen before we make
    // a connection.
    tokio::task::yield_now().await;

    (shutdown_tx, handle)
}

/// Create a bes client that connects to a locally running server
async fn connect_client_local(server_binding: Binding) -> Result<PublishBuildEventClient<Channel>> {
    Ok(match server_binding {
        Binding::SocketAddr(_address) => {
            todo!()
        }
        Binding::UnixDomainSocket(socket_path) => {
            PublishBuildEventClient::new(
                Endpoint::try_from("http://[::]:50051")? // unused when connecting on uds
                    .connect_with_connector(service_fn(move |_: Uri| {
                        let socket = socket_path.clone();
                        async move {
                            Ok::<_, std::io::Error>(TokioIo::new(
                                UnixStream::connect(socket).await?,
                            ))
                        }
                    }))
                    .await?,
            )
        }
    })
}

/// A BES server that can be used to record requests and mock responses for testing.
struct MockBesServer {
    handle: JoinHandle<Result<()>>,
    shutdown_tx: oneshot::Sender<()>,
    url: Url,
    pub build_tool_event_stream_requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

/// Implements the bes server proto contract
struct MockBesService {
    build_tool_event_stream_requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

#[derive(Debug)]
struct RecordedRequest {
    metadata: MetadataMap,
}

impl MockBesServer {
    pub async fn spawn(listen: Binding) -> MockBesServer {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let url: Url = (&listen).into();

        let build_tool_event_stream_requests = Arc::new(Mutex::new(Vec::default()));
        let server = GrpcBesServer::listen(listen).bes_service(MockBesService {
            build_tool_event_stream_requests: build_tool_event_stream_requests.clone(),
        });
        let handle = tokio::spawn(async move {
            server
                .serve(async move {
                    shutdown_rx.await.unwrap();
                })
                .await
        });

        MockBesServer {
            handle,
            shutdown_tx,
            url,
            build_tool_event_stream_requests,
        }
    }

    pub async fn shutdown(self) {
        self.shutdown_tx.send(()).unwrap();
        self.handle.await.unwrap().unwrap();
    }

    pub fn to_bes_backend(&self) -> BesBackend {
        BesBackend::new(String::from("mock_server"), self.url.clone())
    }
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

#[tonic::async_trait]
impl PublishBuildEvent for MockBesService {
    type PublishBuildToolEventStreamStream = PublishBuildToolEventStreamStream;

    async fn publish_build_tool_event_stream(
        &self,
        request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<PublishBuildToolEventStreamStream>, Status> {
        struct State {
            request_stream: Streaming<PublishBuildToolEventStreamRequest>,
        }

        let (metadata, _, request_stream) = request.into_parts();

        let recorded_request = RecordedRequest { metadata };
        self.build_tool_event_stream_requests
            .lock()
            .await
            .push(recorded_request);

        let state = State { request_stream };

        Ok(Response::new(Box::pin(unfold(state, |mut state| async {
            match state.request_stream.message().await {
                Ok(request) => match request {
                    Some(request) => {
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

                        Some((
                            Ok(PublishBuildToolEventStreamResponse {
                                stream_id: Some(stream_id.clone()),
                                sequence_number: sequence_number,
                            }),
                            state,
                        ))
                    }
                    None => None,
                },
                Err(_) => None,
            }
        }))))
    }

    async fn publish_lifecycle_event(
        &self,
        _request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}

fn build_tool_event(seq: i64) -> PublishBuildToolEventStreamRequest {
    PublishBuildToolEventStreamRequest {
        ordered_build_event: Some(OrderedBuildEvent {
            stream_id: Some(StreamId {
                build_id: String::from(""),
                invocation_id: String::from(""),
                ..Default::default()
            }),
            sequence_number: seq,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn standard_lifecycle_events() -> Vec<PublishLifecycleEventRequest> {
    vec![
        build_enqueued_lifecycle_event(),
        invocation_attempt_started_lifecycle_event(),
        invocation_attempt_finished_lifecycle_event(),
        build_finished_lifecycle_event(),
    ]
}

fn lifecycle_request(ordered_build_event: OrderedBuildEvent) -> PublishLifecycleEventRequest {
    PublishLifecycleEventRequest {
        service_level: ServiceLevel::Interactive.into(),
        build_event: Some(ordered_build_event),
        stream_timeout: None,
        notification_keywords: vec![
            String::from("protocol_name=BEP"),
            String::from("command_name=build"),
        ],
        project_id: String::default(),
        check_preceding_lifecycle_events_present: false,
    }
}

fn build_enqueued_lifecycle_event() -> PublishLifecycleEventRequest {
    lifecycle_request(OrderedBuildEvent {
        stream_id: Some(StreamId {
            build_id: String::from("b3bcf30e-1513-4c0a-b340-0964bfb83707"),
            invocation_id: String::default(),
            component: BuildComponent::Controller.into(),
        }),
        sequence_number: 1,
        event: Some(BuildEvent {
            event_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            event: Some(Event::BuildEnqueued(BuildEnqueued { details: None })),
        }),
    })
}

fn invocation_attempt_started_lifecycle_event() -> PublishLifecycleEventRequest {
    lifecycle_request(OrderedBuildEvent {
        stream_id: Some(StreamId {
            build_id: String::from("b3bcf30e-1513-4c0a-b340-0964bfb83707"),
            invocation_id: String::from("7e971239-65b5-4a14-9e54-967b4552a486"),
            component: BuildComponent::Controller.into(),
        }),
        sequence_number: 1,
        event: Some(BuildEvent {
            event_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            event: Some(Event::InvocationAttemptStarted(InvocationAttemptStarted {
                attempt_number: 1,
                details: None,
            })),
        }),
    })
}

fn invocation_attempt_finished_lifecycle_event() -> PublishLifecycleEventRequest {
    lifecycle_request(OrderedBuildEvent {
        stream_id: Some(StreamId {
            build_id: String::from("b3bcf30e-1513-4c0a-b340-0964bfb83707"),
            invocation_id: String::from("7e971239-65b5-4a14-9e54-967b4552a486"),
            component: BuildComponent::Controller.into(),
        }),
        sequence_number: 2,
        event: Some(BuildEvent {
            event_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            event: Some(Event::InvocationAttemptFinished(
                InvocationAttemptFinished {
                    invocation_status: Some(BuildStatus {
                        result: build_status::Result::CommandSucceeded.into(),
                        final_invocation_id: String::default(),
                        build_tool_exit_code: None,
                        error_message: String::default(),
                        details: None,
                    }),
                    details: None,
                },
            )),
        }),
    })
}

fn build_finished_lifecycle_event() -> PublishLifecycleEventRequest {
    lifecycle_request(OrderedBuildEvent {
        stream_id: Some(StreamId {
            build_id: String::from("b3bcf30e-1513-4c0a-b340-0964bfb83707"),
            invocation_id: String::default(),
            component: BuildComponent::Controller.into(),
        }),
        sequence_number: 2,
        event: Some(BuildEvent {
            event_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            event: Some(Event::BuildFinished(BuildFinished {
                status: Some(BuildStatus {
                    result: build_status::Result::CommandSucceeded.into(),
                    final_invocation_id: String::default(),
                    build_tool_exit_code: None,
                    error_message: String::default(),
                    details: None,
                }),
                details: None,
            })),
        }),
    })
}
