use args::Args;
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use clap::Parser;
use futures::{Stream, stream::unfold};
use std::{env, pin::Pin, str::FromStr};
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Endpoint, Error, Server},
};
use url::Url;

mod args;

struct NBesService {
    backends: Vec<BesBackend>,
}

struct BesBackend {
    name: String,
    endpoint: Url,
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

impl NBesService {
    pub fn new() -> Self {
        Self {
            backends: Vec::default(),
        }
    }

    pub fn add_backend(&mut self, bes_backend: BesBackend) {
        self.backends.push(bes_backend);
    }
}

#[tonic::async_trait]
impl PublishBuildEvent for NBesService {
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
        request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        for backend in &self.backends {
            let channel = Endpoint::from_str(backend.endpoint.as_str())
                .map_err(|e| Status::internal(e.to_string()))?
                .connect()
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;

            let mut client = PublishBuildEventClient::new(channel);
            client
                .publish_lifecycle_event(Request::new(request.clone()))
                .await?;
        }

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

    let bes_backends = args
        .bes_backends
        .into_iter()
        .map(|b| Into::<BesBackend>::into(b));

    let mut nbes_service = NBesService::new();

    for backend in bes_backends {
        eprintln!(
            "configured backend {} -> {}",
            backend.name, backend.endpoint
        );
        nbes_service.add_backend(backend);
    }

    if nbes_service.backends.is_empty() {
        eprintln!("no bes backends configured; bes events will be swallowed by a black hole");
    }

    Server::builder()
        .add_service(PublishBuildEventServer::new(nbes_service))
        .serve(address)
        .await
}
