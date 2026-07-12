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
use std::{collections::VecDeque, pin::Pin, sync::Arc};
use tempfile::NamedTempFile;
use tokio::{
    net::UnixStream,
    sync::{Mutex, oneshot},
    task::JoinHandle,
};
use tonic::{
    Code, Request, Response, Status, Streaming,
    metadata::MetadataValue,
    transport::{Endpoint, Uri},
};
use tonic::{metadata::MetadataMap, transport::Channel};
use tower::service_fn;
use url::Url;
use uuid::Uuid;

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

    let stream_id = build_tool_event_stream_id();
    let request_stream = futures::stream::iter([
        build_tool_event(&stream_id, 1),
        build_tool_event(&stream_id, 2),
        build_tool_event(&stream_id, 3),
        build_tool_event(&stream_id, 4),
        build_tool_event(&stream_id, 5),
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
pub async fn test_build_tool_event_client_sends_inconsistent_stream_id() -> Result<()> {
    // This tests a misbehaving client, which we don't expect Bazel to be, but test it anyway

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: Vec::default(),
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_tool_event_stream_id();
    let other_stream_id = build_tool_event_stream_id();
    let request_stream = futures::stream::iter([
        build_tool_event(&stream_id, 1),
        build_tool_event(&other_stream_id, 2),
    ]);

    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    response_stream.message().await?;
    let r2 = response_stream.message().await;

    assert!(r2.is_err());
    let status = r2.unwrap_err();
    assert_eq!(Code::Internal, status.code());
    assert_eq!(
        "received inconsistent stream id from client",
        status.message()
    );

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;

    Ok(())
}

#[tokio::test]
pub async fn test_forward_responds_in_sequence() -> Result<()> {
    let server_uds = NamedTempFile::new()?;
    let mock_server = MockBesServer::spawn(
        String::from("mock_server"),
        Binding::UnixDomainSocket(server_uds.path().to_path_buf()),
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_tool_event_stream_id();
    let request_stream = futures::stream::iter([
        build_tool_event(&stream_id, 1),
        build_tool_event(&stream_id, 2),
        build_tool_event(&stream_id, 3),
        build_tool_event(&stream_id, 4),
        build_tool_event(&stream_id, 5),
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
    let mock_server = MockBesServer::spawn(
        String::from("mock_server"),
        Binding::UnixDomainSocket(server_uds.path().to_path_buf()),
    )
    .await;

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
    let mock_server = MockBesServer::spawn(
        String::from("mock_server"),
        Binding::UnixDomainSocket(server_uds.path().to_path_buf()),
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_tool_event_stream_id();
    let request_stream = futures::stream::iter([build_tool_event(&stream_id, 1)]);

    let mut request = Request::new(request_stream);

    // Add a "foo: bar" header to the request
    request
        .metadata_mut()
        .append("foo", MetadataValue::from_static("bar"));
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    while let Some(_) = response_stream.message().await? {}

    mock_server
        .assert(|mock| {
            let requests = &mock.build_tool_event_stream_requests;
            assert_eq!(1, requests.len());
            let header = requests[0].metadata.get("foo");
            assert!(header.is_some());
            assert_eq!(header.unwrap(), "bar");
        })
        .await;

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    mock_server.shutdown().await;

    Ok(())
}

#[tokio::test]
pub async fn test_lifecycle_request_fails_when_one_backend_fails() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;
    let b2_uds = NamedTempFile::new()?;
    let b2 = MockBesServer::spawn(
        String::from("b2"),
        Binding::UnixDomainSocket(b2_uds.path().to_path_buf()),
    )
    .await;

    b2.fail_lifecycle_events().await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend(), b2.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_lifecycle_event_stream_id();
    let response = client
        .publish_lifecycle_event(Request::new(build_enqueued_lifecycle_event(&stream_id)))
        .await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(Code::Internal, status.code());
    assert_eq!("b2 failed lifecycle request: oops", status.message());

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    b1.shutdown().await;
    b2.shutdown().await;

    Ok(())
}

#[tokio::test]
pub async fn test_event_stream_request_fails_when_one_backend_fails() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;
    let stream_id = build_tool_event_stream_id();
    b1.mock_event_stream_responses([
        Ok(PublishBuildToolEventStreamResponse {
            stream_id: Some(stream_id.clone()),
            sequence_number: 1,
        }),
        Ok(PublishBuildToolEventStreamResponse {
            stream_id: Some(stream_id.clone()),
            sequence_number: 2,
        }),
    ])
    .await;
    let b2_uds = NamedTempFile::new()?;
    let b2 = MockBesServer::spawn(
        String::from("b2"),
        Binding::UnixDomainSocket(b2_uds.path().to_path_buf()),
    )
    .await;
    b2.mock_event_stream_responses([
        Ok(PublishBuildToolEventStreamResponse {
            stream_id: Some(stream_id.clone()),
            sequence_number: 1,
        }),
        Err(Status::internal("oops")),
    ])
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend(), b2.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let request_stream = futures::stream::iter([
        build_tool_event(&stream_id, 1),
        build_tool_event(&stream_id, 2), // b2 fails
    ]);

    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    response_stream.message().await?; // r1
    let r2 = response_stream.message().await;

    assert!(r2.is_err());
    let status = r2.unwrap_err();
    assert_eq!(Code::Internal, status.code());
    assert_eq!("b2 failed event stream request: oops", status.message());

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    b1.shutdown().await;
    b2.shutdown().await;

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
    name: String,
    handle: JoinHandle<Result<()>>,
    shutdown_tx: oneshot::Sender<()>,
    url: Url,
    mock: Arc<Mutex<MockData>>,
}

/// Implements the bes server proto contract
struct MockBesService {
    mock: Arc<Mutex<MockData>>,
}

struct MockData {
    fail_lifecycle_events: bool,
    ack_all_build_stream_requests: bool,
    build_tool_event_stream_requests: Vec<RecordedRequest>,
    build_tool_event_stream_responses:
        VecDeque<Result<PublishBuildToolEventStreamResponse, Status>>,
}

#[derive(Debug)]
struct RecordedRequest {
    metadata: MetadataMap,
}

impl MockBesServer {
    pub async fn spawn(name: String, listen: Binding) -> MockBesServer {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let url: Url = (&listen).into();

        let mock = Arc::new(Mutex::new(MockData {
            fail_lifecycle_events: false,
            ack_all_build_stream_requests: true,
            build_tool_event_stream_requests: Vec::default(),
            build_tool_event_stream_responses: VecDeque::default(),
        }));

        let server =
            GrpcBesServer::listen(listen).bes_service(MockBesService { mock: mock.clone() });
        let handle = tokio::spawn(async move {
            server
                .serve(async move {
                    shutdown_rx.await.unwrap();
                })
                .await
        });

        MockBesServer {
            name,
            handle,
            shutdown_tx,
            url,
            mock,
        }
    }

    /// Run assertions on the current mock data
    pub async fn assert(&self, assert: impl Fn(&MockData) -> ()) {
        let mock = self.mock.lock().await;
        assert(&mock);
    }

    pub async fn mock_event_stream_responses(
        &self,
        responses: impl IntoIterator<Item = Result<PublishBuildToolEventStreamResponse, Status>>,
    ) {
        let mut mock = self.mock.lock().await;

        mock.ack_all_build_stream_requests = false;
        mock.build_tool_event_stream_responses.extend(responses);
    }

    pub async fn fail_lifecycle_events(&self) {
        self.mock.lock().await.fail_lifecycle_events = true;
    }

    pub async fn shutdown(self) {
        self.shutdown_tx.send(()).unwrap();
        self.handle.await.unwrap().unwrap();
    }

    pub fn to_bes_backend(&self) -> BesBackend {
        BesBackend::new(self.name.clone(), self.url.clone())
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
            mock: Arc<Mutex<MockData>>,
        }

        let (metadata, _, request_stream) = request.into_parts();

        let recorded_request = RecordedRequest { metadata };
        self.mock
            .lock()
            .await
            .build_tool_event_stream_requests
            .push(recorded_request);

        let state = State {
            request_stream,
            mock: self.mock.clone(),
        };

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

                        if state.mock.lock().await.ack_all_build_stream_requests {
                            Some((
                                Ok(PublishBuildToolEventStreamResponse {
                                    stream_id: Some(stream_id.clone()),
                                    sequence_number: sequence_number,
                                }),
                                state,
                            ))
                        } else {
                            {
                                state
                                    .mock
                                    .lock()
                                    .await
                                    .build_tool_event_stream_responses
                                    .pop_front()
                            }
                            .map(|response| (response, state))
                        }
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
        if self.mock.lock().await.fail_lifecycle_events {
            Err(Status::internal("oops"))
        } else {
            Ok(Response::new(()))
        }
    }
}

fn build_tool_event_stream_id() -> StreamId {
    StreamId {
        build_id: String::from(Uuid::new_v4()),
        invocation_id: String::from(Uuid::new_v4()),
        component: BuildComponent::Tool.into(),
    }
}

fn build_lifecycle_event_stream_id() -> StreamId {
    StreamId {
        build_id: Uuid::new_v4().to_string(),
        // build lifecycle events don't have an invocation id
        invocation_id: String::from(""),
        component: BuildComponent::Controller.into(),
    }
}

fn invocation_lifecycle_event_stream_id(build_id: &str) -> StreamId {
    StreamId {
        build_id: build_id.to_string(),
        invocation_id: Uuid::new_v4().to_string(),
        component: BuildComponent::Controller.into(),
    }
}

fn build_tool_event(stream_id: &StreamId, seq: i64) -> PublishBuildToolEventStreamRequest {
    PublishBuildToolEventStreamRequest {
        ordered_build_event: Some(OrderedBuildEvent {
            stream_id: Some(stream_id.clone()),
            sequence_number: seq,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn standard_lifecycle_events() -> Vec<PublishLifecycleEventRequest> {
    let build_event_stream_id = build_lifecycle_event_stream_id();
    let invocation_event_stream_id =
        invocation_lifecycle_event_stream_id(&build_event_stream_id.build_id);

    vec![
        build_enqueued_lifecycle_event(&build_event_stream_id),
        invocation_attempt_started_lifecycle_event(&invocation_event_stream_id),
        invocation_attempt_finished_lifecycle_event(&invocation_event_stream_id),
        build_finished_lifecycle_event(&build_event_stream_id),
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

fn build_enqueued_lifecycle_event(stream_id: &StreamId) -> PublishLifecycleEventRequest {
    lifecycle_request(OrderedBuildEvent {
        stream_id: Some(stream_id.clone()),
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

fn invocation_attempt_started_lifecycle_event(
    stream_id: &StreamId,
) -> PublishLifecycleEventRequest {
    lifecycle_request(OrderedBuildEvent {
        stream_id: Some(stream_id.clone()),
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

fn invocation_attempt_finished_lifecycle_event(
    stream_id: &StreamId,
) -> PublishLifecycleEventRequest {
    lifecycle_request(OrderedBuildEvent {
        stream_id: Some(stream_id.clone()),
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

fn build_finished_lifecycle_event(stream_id: &StreamId) -> PublishLifecycleEventRequest {
    lifecycle_request(OrderedBuildEvent {
        stream_id: Some(stream_id.clone()),
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
