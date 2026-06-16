use anyhow::Result;
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest, publish_build_event_server::PublishBuildEvent,
};
use futures::StreamExt;
use futures::{
    Stream,
    future::join_all,
    stream::{self, unfold},
};
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{
    Request, Response, Status, Streaming,
    metadata::{KeyAndValueRef, MetadataMap},
};

use crate::backend::Backend;
use crate::client::Client;

pub struct NBesService<B: Backend + 'static> {
    clients: HashMap<String, B::Client>,
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

impl<B: Backend> NBesService<B> {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    pub fn add_backend(&mut self, backend: B) -> Result<()> {
        let client = backend.create_client()?;
        self.clients.insert(backend.name().to_string(), client);
        Ok(())
    }
}

#[tonic::async_trait]
impl<B: Backend> PublishBuildEvent for NBesService<B>
where
    B::Client: Send + Sync + Clone,
{
    type PublishBuildToolEventStreamStream = PublishBuildToolEventStreamStream;

    async fn publish_build_tool_event_stream(
        &self,
        request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<PublishBuildToolEventStreamStream>, Status> {
        struct State {
            /// Incoming request stream from Bazel
            incoming_requests: Streaming<PublishBuildToolEventStreamRequest>,
            /// Incoming response streams from the bes backends
            incoming_responses: Vec<(String, Streaming<PublishBuildToolEventStreamResponse>)>,
        }

        let (metadata, _, incoming_requests) = request.into_parts();

        // Open streams to each of the bes backends. Incoming requests are sent
        // via a broadcast channel. Each backend turns its receiver into a stream
        // to forward requests to the backend.
        let (tx, _) = broadcast::channel::<PublishBuildToolEventStreamRequest>(256);
        let incoming_responses = stream::iter(self.clients.iter())
            .then(|backend| async {
                let receiver = tx.subscribe();
                let outbound_requests = BroadcastStream::new(receiver).map(|request| {
                    // BroadcastStream wraps the request in a Result in case it fails to
                    // receive from the sender. But publish_build_tool_event_stream() takes
                    // a stream of requests, so we must unwrap it. This should "never" occur,
                    // so just panic.
                    request.expect("failed to receive request from broadcast channel")
                });

                let mut request = Request::new(outbound_requests);
                copy_request_metadata(&metadata, &mut request);

                (
                    backend.0.clone(),
                    backend
                        .1
                        .clone() // cloning clients is cheap
                        .publish_build_tool_event_stream(request)
                        .await
                        .expect("failed to initiate stream")
                        .into_inner(),
                )
            })
            .collect()
            .await;

        let state = State {
            incoming_requests,
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

                eprintln!("{:#?}", request);

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

                    // Forward the request to all backends via the broadcast channel
                    if !state.incoming_responses.is_empty() {
                        tx.send(request.clone()).expect("failed to send message");
                    }

                    // Wait for a response from each backend
                    let responses: Vec<_> =
                        join_all(state.incoming_responses.iter_mut().map(|r| r.1.message()))
                            .await
                            .into_iter()
                            .map(|r| r.expect("failed to receive message from backend"))
                            .collect();

                    // Validate the responses
                    for (i, response) in responses.iter().enumerate() {
                        let backend_name = &state.incoming_responses[i].0;
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

                                if response_stream_id != stream_id
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
                                    "bes backend {backend_name} unexpectedly ended stream {stream_id:?}"
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
                            stream_id: Some(stream_id.clone()),
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

        for backend in self.clients.iter() {
            let mut outbound_request = Request::new(message.clone());
            copy_request_metadata(&metadata, &mut outbound_request);
            backend
                .1
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

#[cfg(test)]
mod tests {
    use std::env;

    use crate::backend::BesBackend;

    use super::*;
    use build_proto::google::devtools::build::v1::{
        BuildEvent, StreamId,
        build_event::{
            BuildEnqueued,
            Event::{self, BazelEvent},
        },
        publish_build_event_client::PublishBuildEventClient,
        publish_build_event_server::PublishBuildEventServer,
        publish_lifecycle_event_request::ServiceLevel,
        stream_id::BuildComponent,
    };
    use hyper_util::rt::TokioIo;
    use prost_types::{Any, Timestamp};
    use rand::distr::{Alphanumeric, SampleString};
    use tempfile::TempPath;
    use tokio::{
        net::{UnixListener, UnixStream},
        sync::oneshot,
    };
    use tokio_stream::{
        StreamExt,
        wrappers::{ReceiverStream, UnixListenerStream},
    };
    use tonic::transport::Uri;
    use tonic::transport::{Channel, Endpoint, Server};
    use tower::service_fn;

    #[tokio::test]
    pub async fn receives_lifecycle_events() {
        let service = NBesService::<BesBackend>::new();
        let event = build_enqueued_lifecycle_event();

        let request = Request::new(event);
        let response = service.publish_lifecycle_event(request).await;

        assert!(response.is_ok());
    }

    #[tokio::test]
    pub async fn preserves_request_header_on_lifecycle_event() {
        let service = NBesService::<BesBackend>::new();
        let event = build_enqueued_lifecycle_event();

        let request = Request::new(event);
        // request.metadata_mut().append("foo", "bar");

        let response = service.publish_lifecycle_event(request).await;

        assert!(response.is_ok());
    }

    #[tokio::test]
    pub async fn foo_stream() {
        let requests = vec![bazel_event()];

        let backend = MockBackend {};
        let mut service = NBesService::new();
        // service.add_backend(backend).unwrap();

        let (mut client, cleanup) = setup(service);

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let rs = ReceiverStream::new(rx);
        let request = Request::new(rs);

        eprintln!("A");
        let mut response = client
            .publish_build_tool_event_stream(request)
            .await
            .expect("foobar")
            .into_inner();

        eprintln!("B");
        tx.send(requests[0].clone()).await.unwrap();

        response.try_next().await.expect("foobar");
        drop(tx);

        eprintln!("C");
        // shutdown_server_tx.send(()).unwrap();

        // let result = handle.await;
        cleanup.await;

        // assert!(result.is_ok());
        // assert!(result.unwrap().is_ok());

        // service.publish_build_tool_event_stream(request);
    }

    /// Setup a tonic server and client to test streaming RPCs on the NBesService.
    ///
    /// This may seem heavy, but testing service methods with streaming RPCs
    /// directly requires tricky setup involing the Streaming struct and prost
    /// codec which I wasn't able to figure out. There is an OSS crate to
    /// support this (https://crates.io/crates/tonic-mock), but it doesn't
    /// support tonic 0.14 and appears to be unmaintained.
    ///
    /// Instead, test the service methods via a call from the client reaching
    /// out to a real tonic server. Set up a lightweight unix domain socket
    /// to facilitate communication between the server and client.
    ///
    /// NB: Non-streaming RPCs can still easily test the service method directly
    /// without the need for this setup.
    ///
    /// Returns the client and a future to await to stop the server and cleanup.
    fn setup(
        service: NBesService<MockBackend>,
    ) -> (PublishBuildEventClient<Channel>, impl Future<Output = ()>) {
        let (signal_tx, signal_rx) = oneshot::channel::<()>();
        // Create a random path to a temporary domain socket file that will be deleted
        // when the variable goes out scope. This file cannot exist under Bazel's
        // TEST_TMPDIR because the sandboxed path is too long for unix domain sockets.
        let uds_path = TempPath::try_from_path(env::temp_dir().join(format!(
            ".{}",
            Alphanumeric.sample_string(&mut rand::rng(), 16)
        )))
        .expect("failed to create path to unix domain socket");

        let uds = UnixListener::bind(&uds_path).expect("failed to bind unix domain socket");
        let uds_stream = UnixListenerStream::new(uds);

        // Spawn a tonic server listening on the domain socket
        let handle = tokio::spawn(async {
            Server::builder()
                .add_service(PublishBuildEventServer::new(service))
                .serve_with_incoming_shutdown(uds_stream, async {
                    signal_rx.await.ok();
                })
                .await
                .expect("failed to start tonic server for test");
        });

        // Set up a client to talk to the server
        let channel = Endpoint::try_from("http://[::]:50051") // Url is ignored, but required
            .unwrap()
            .connect_with_connector_lazy({
                let uds_path = uds_path.to_path_buf();
                service_fn(move |_: Uri| {
                    let uds_path = uds_path.clone();
                    async move {
                        let stream = UnixStream::connect(&uds_path)
                            .await
                            .expect("failed to connect to domain socket stream");
                        Ok::<_, std::io::Error>(TokioIo::new(stream))
                    }
                })
            });
        let client = PublishBuildEventClient::new(channel);

        (client, async move {
            // When awaited this future cleans up by stopping
            // the server and deleting the domain socket
            signal_tx.send(()).unwrap();
            let _ = handle.await;
            drop(uds_path)
        })
    }

    fn build_enqueued_lifecycle_event() -> PublishLifecycleEventRequest {
        PublishLifecycleEventRequest {
            service_level: ServiceLevel::Interactive as i32,
            build_event: Some(OrderedBuildEvent {
                stream_id: Some(StreamId {
                    build_id: String::from("4e350e2c-c29b-4d33-9aab-c7f5c5a47dc3"),
                    invocation_id: String::from(""),
                    component: BuildComponent::Controller as i32,
                }),
                sequence_number: 1,
                event: Some(BuildEvent {
                    event_time: Some(Timestamp {
                        seconds: 1781564945,
                        nanos: 145000000,
                    }),
                    event: Some(Event::BuildEnqueued(BuildEnqueued { details: None })),
                }),
            }),
            stream_timeout: None,
            notification_keywords: vec![
                String::from("protocol_name=BEP"),
                String::from("command_name=build"),
            ],
            project_id: String::from(""),
            check_preceding_lifecycle_events_present: false,
        }
    }

    fn bazel_event() -> PublishBuildToolEventStreamRequest {
        PublishBuildToolEventStreamRequest {
            ordered_build_event: Some(OrderedBuildEvent {
                stream_id: Some(StreamId {
                    build_id: String::from("4e350e2c-c29b-4d33-9aab-c7f5c5a47dc3"),
                    invocation_id: String::from("fa57617a-a7d8-405d-a19c-c44cfade02df"),
                    component: BuildComponent::Tool as i32,
                }),
                sequence_number: 1,
                event: Some(BuildEvent {
                    event_time: Some(Timestamp {
                        seconds: 1781566027,
                        nanos: 104000000,
                    }),
                    event: Some(BazelEvent(Any {
                        type_url: String::from("type.googleapis.com/build_event_stream.BuildEvent"),
                        value: vec![
                            10, 4, 18, 2, 8, 25, 18, 4, 18, 2, 8, 26, 18, 6, 106, 4, 10, 2, 50, 50,
                            26, 0,
                        ],
                    })),
                }),
            }),
            notification_keywords: Vec::default(),
            project_id: String::from(""),
            check_preceding_lifecycle_events_present: false,
        }
    }

    struct MockBackend {}

    #[derive(Clone)]
    struct MockClient {}

    #[tonic::async_trait]
    impl Client for MockClient {
        async fn publish_build_tool_event_stream(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = PublishBuildToolEventStreamRequest>
            + Send,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<PublishBuildToolEventStreamResponse>>,
            tonic::Status,
        > {
            todo!()
        }

        async fn publish_lifecycle_event(
            &mut self,
            request: impl tonic::IntoRequest<PublishLifecycleEventRequest> + Send,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            todo!()
        }
    }

    impl Backend for MockBackend {
        type Client = MockClient;

        fn name(&self) -> &str {
            todo!()
        }

        fn endpoint(&self) -> &url::Url {
            todo!()
        }

        fn create_client(&self) -> Result<Self::Client> {
            todo!()
        }
    }
}
