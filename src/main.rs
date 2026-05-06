// use std::net::{SocketAddr, SocketAddrV4};

use std::pin::Pin;

use futures::{Stream, stream::unfold};

use tonic::{
    Request, Response, Status, Streaming,
    transport::{Error, Server},
};
// use build_event_stream_rust_proto::java::com::google::devtools::build::lib::buildeventstream::proto::{}
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};

struct BuildEventService {}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

#[tonic::async_trait]
impl PublishBuildEvent for BuildEventService {
    type PublishBuildToolEventStreamStream = PublishBuildToolEventStreamStream;

    async fn publish_build_tool_event_stream(
        &self,
        request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<PublishBuildToolEventStreamStream>, Status> {
        struct State {
            incoming: Streaming<PublishBuildToolEventStreamRequest>,
        }

        let state = State {
            incoming: request.into_inner(),
        };

        Ok(Response::new(Box::pin(unfold(
            state,
            |mut state| async move {
                let msg = state
                    .incoming
                    .message()
                    .await
                    .expect("failed to receive message");

                if let Some(PublishBuildToolEventStreamRequest {
                    ordered_build_event:
                        Some(OrderedBuildEvent {
                            stream_id,
                            sequence_number,
                            ..
                        }),
                    ..
                }) = msg
                {
                    return Some((
                        Ok(PublishBuildToolEventStreamResponse {
                            stream_id,
                            sequence_number,
                        }),
                        state,
                    ));
                }

                None
            },
        ))))
    }

    async fn publish_lifecycle_event(
        &self,
        _request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let address = "0.0.0.0:6000"
        .parse()
        .expect("failed to parse socket address");

    eprintln!("starting grpc server on {address}");

    Server::builder()
        .add_service(PublishBuildEventServer::new(BuildEventService {}))
        .serve(address)
        .await
}
