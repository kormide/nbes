#![allow(unused)]

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
use tokio::{
    net::UnixStream,
    sync::{Mutex, oneshot},
    task::JoinHandle,
};
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Endpoint, Uri},
};
use tonic::{metadata::MetadataMap, transport::Channel};
use tower::service_fn;
use url::Url;
use uuid::Uuid;

/// Spawn nbes in a task and provide a oneshot channel to shut it down
// pub async fn spawn_nbes(config: Config) -> (oneshot::Sender<()>, JoinHandle<Result<()>>) {
pub async fn spawn_nbes(config: Config) -> impl Future<Output = ()> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = tokio::spawn(nbes::run(config, async {
        shutdown_rx.await.unwrap();
    }));

    // Yield to allow the server to listen before we make
    // a connection.
    tokio::task::yield_now().await;

    async move {
        shutdown_tx.send(());
        handle.await.unwrap().unwrap();
    }
}

/// Create a bes client that connects to a locally running server
pub async fn connect_client_local(
    server_binding: Binding,
) -> Result<PublishBuildEventClient<Channel>> {
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
pub struct MockBesServer {
    name: String,
    handle: JoinHandle<Result<()>>,
    shutdown_tx: oneshot::Sender<()>,
    url: Url,
    mock: Arc<Mutex<MockData>>,
}

/// Implements the bes server proto contract
pub struct MockBesService {
    mock: Arc<Mutex<MockData>>,
}

pub struct MockData {
    fail_lifecycle_events: bool,
    ack_all_build_stream_requests: bool,
    build_tool_event_stream_responses:
        VecDeque<Result<PublishBuildToolEventStreamResponse, Status>>,
    pub build_tool_event_stream_requests: Vec<RecordedRequest>,
    pub lifecycle_requests: Vec<RecordedRequest>,
}

#[derive(Debug)]
pub struct RecordedRequest {
    pub metadata: MetadataMap,
}

impl MockBesServer {
    pub async fn spawn(name: String, listen: Binding) -> MockBesServer {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let url: Url = (&listen).into();

        let mock = Arc::new(Mutex::new(MockData {
            fail_lifecycle_events: false,
            ack_all_build_stream_requests: true,
            build_tool_event_stream_responses: VecDeque::default(),
            build_tool_event_stream_requests: Vec::default(),
            lifecycle_requests: Vec::default(),
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
        request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        let (metadata, _, _) = request.into_parts();

        let recorded_request = RecordedRequest { metadata };
        self.mock
            .lock()
            .await
            .lifecycle_requests
            .push(recorded_request);

        if self.mock.lock().await.fail_lifecycle_events {
            Err(Status::internal("oops"))
        } else {
            Ok(Response::new(()))
        }
    }
}

pub fn build_tool_event_stream_id() -> StreamId {
    StreamId {
        build_id: String::from(Uuid::new_v4()),
        invocation_id: String::from(Uuid::new_v4()),
        component: BuildComponent::Tool.into(),
    }
}

pub fn build_lifecycle_event_stream_id() -> StreamId {
    StreamId {
        build_id: Uuid::new_v4().to_string(),
        // build lifecycle events don't have an invocation id
        invocation_id: String::from(""),
        component: BuildComponent::Controller.into(),
    }
}

pub fn invocation_lifecycle_event_stream_id(build_id: &str) -> StreamId {
    StreamId {
        build_id: build_id.to_string(),
        invocation_id: Uuid::new_v4().to_string(),
        component: BuildComponent::Controller.into(),
    }
}

pub fn build_tool_event(stream_id: &StreamId, seq: i64) -> PublishBuildToolEventStreamRequest {
    PublishBuildToolEventStreamRequest {
        ordered_build_event: Some(OrderedBuildEvent {
            stream_id: Some(stream_id.clone()),
            sequence_number: seq,
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn standard_lifecycle_events() -> Vec<PublishLifecycleEventRequest> {
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

pub fn lifecycle_request(ordered_build_event: OrderedBuildEvent) -> PublishLifecycleEventRequest {
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

pub fn build_enqueued_lifecycle_event(stream_id: &StreamId) -> PublishLifecycleEventRequest {
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

pub fn invocation_attempt_started_lifecycle_event(
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

pub fn invocation_attempt_finished_lifecycle_event(
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

pub fn build_finished_lifecycle_event(stream_id: &StreamId) -> PublishLifecycleEventRequest {
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
