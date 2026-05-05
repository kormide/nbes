// use std::net::{SocketAddr, SocketAddrV4};

use std::pin::Pin;

use tonic::{
    Request, Response, Status, Streaming,
    transport::{Error, Server},
};
// use build_event_stream_rust_proto::java::com::google::devtools::build::lib::buildeventstream::proto::{}
use build_proto::google::devtools::build::v1::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};

use futures_core::Stream;

struct BuildEventService {}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

#[tonic::async_trait]
impl PublishBuildEvent for BuildEventService {
    type PublishBuildToolEventStreamStream = PublishBuildToolEventStreamStream;

    async fn publish_build_tool_event_stream(
        &self,
        _request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<PublishBuildToolEventStreamStream>, Status> {
        todo!()
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
