use anyhow::Result;
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest, StreamId, publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::PublishBuildEvent,
};
use futures::{Stream, stream::unfold};
use hyper_util::rt::TokioIo;
use nbes::{Config, forwarding::BesBackend, server::GrpcBesServer};
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use tempfile::NamedTempFile;
use tokio::{net::UnixStream, sync::oneshot, task::JoinHandle};
use tonic::transport::Channel;
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Endpoint, Uri},
};
use tower::service_fn;
use url::Url;

#[tokio::test]
pub async fn test_forward_to_single_backend_responds_in_sequence() -> Result<()> {
    let server_socket = NamedTempFile::new()?;
    let mock_server = MockBesServer::spawn_on_socket(server_socket.path()).await;

    let nbes_socket = NamedTempFile::new()?;
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: "0.0.0.0:9000".parse()?,
        socket: Some(nbes_socket.path().to_path_buf()),
    };

    let shutdown_tx = spawn_nbes(config).await;

    let mut client = connect_client_socket(nbes_socket.path()).await?;

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

    shutdown_tx.send(()).unwrap();
    mock_server.shutdown().await;

    Ok(())
}

async fn spawn_nbes(config: Config) -> oneshot::Sender<()> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let _ = nbes::run(config, async {
            shutdown_rx.await.unwrap();
        })
        .await;
    });

    // Yield to allow the server to listen before we make
    // a connection.
    tokio::task::yield_now().await;

    shutdown_tx
}

async fn connect_client_socket(socket: &Path) -> Result<PublishBuildEventClient<Channel>> {
    let socket = socket.to_path_buf();
    Ok(
        PublishBuildEventClient::new(
            Endpoint::try_from("http://[::]:50051")?
                .connect_with_connector(service_fn(move |_: Uri| {
                    let socket = socket.to_path_buf();
                    async move {
                        Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(socket).await?))
                    }
                }))
                .await?,
        ),
    )
}

struct MockBesServer {
    handle: JoinHandle<Result<()>>,
    shutdown_tx: oneshot::Sender<()>,
    socket: PathBuf,
}

struct MockBesService {}

impl MockBesServer {
    pub async fn spawn_on_socket(path: &Path) -> MockBesServer {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let socket = path.to_path_buf();
        let server =
            GrpcBesServer::unix_domain_socket(socket.clone()).bes_service(MockBesService {});
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
            socket,
        }
    }

    pub async fn shutdown(self) {
        self.shutdown_tx.send(()).unwrap();
        self.handle.await.unwrap().unwrap();
    }

    pub fn to_bes_backend(&self) -> BesBackend {
        BesBackend::new(
            String::from("mock_server"),
            Url::parse(&format!("unix:{}", self.socket.display())).unwrap(),
        )
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

        let (_, _, request_stream) = request.into_parts();

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
