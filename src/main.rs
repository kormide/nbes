use args::Args;
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use clap::Parser;
use futures::{Stream, stream::unfold};
use std::{env, pin::Pin};
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Error, Server},
};

mod args;

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
    const DEFAULT_PORT: u16 = 9000;

    let args = Args::parse();

    let port = args
        .port
        .or_else(|| {
            env::var("NBES_PORT").ok().and_then(|port| {
                port.parse::<u16>()
                    .inspect_err(|_| {
                        eprintln!("failed to parse NBES_PORT env var, defaulting to {DEFAULT_PORT}")
                    })
                    .ok()
            })
        })
        .unwrap_or(DEFAULT_PORT);

    let address = format!("0.0.0.0:{}", port)
        .parse()
        .expect("failed to parse socket address");

    eprintln!("starting bes server on {address}");

    Server::builder()
        .add_service(PublishBuildEventServer::new(BuildEventService {}))
        .serve(address)
        .await
}
