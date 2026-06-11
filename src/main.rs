use args::Args;
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use clap::Parser;
use futures::{Stream, stream::unfold};
use rustls::crypto::CryptoProvider;
use std::{env, pin::Pin, str::FromStr, time::Duration};
use tonic::{
    Request, Response, Status, Streaming,
    codec::CompressionEncoding,
    metadata::{KeyRef, MetadataValue},
    transport::{Channel, ClientTlsConfig, Endpoint, Error, Server},
};
use url::Url;

mod args;

struct NBesService {
    backends: Vec<BesBackend>,
    channels: Vec<Channel>,
    clients: Vec<PublishBuildEventClient<Channel>>,
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
            channels: Vec::default(),
            clients: Vec::default(),
        }
    }

    pub fn add_backend(&mut self, bes_backend: BesBackend) {
        self.backends.push(bes_backend);
    }

    pub async fn connect(&mut self) -> Result<(), Status> {
        for backend in self.backends.iter() {
            eprintln!("{}", backend.endpoint);
            let channel = Endpoint::from_str(backend.endpoint.as_str())
                .map_err(|e| Status::internal(e.to_string()))?
                .tls_config(ClientTlsConfig::default())
                .map_err(|e| Status::internal(e.to_string()))?
                .tcp_keepalive(Some(Duration::from_secs(1)))
                .connect()
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;

            eprintln!("{:#?}", channel);

            self.channels.push(channel.clone());
            let client = PublishBuildEventClient::new(channel)
                .max_decoding_message_size(1024 * 1024 * 100)
                .max_encoding_message_size(1024 * 1024 * 100)
                .send_compressed(CompressionEncoding::Gzip);
            self.clients.push(client);
        }

        Ok(())
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

        let mut state = State {
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
                    eprintln!("{sequence_number}");
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
        let headers = request.metadata().clone();
        eprintln!("*** {request:#?}");
        let request = request.into_inner();
        for i in 0..self.backends.len() {
            eprintln!("backend: {}", self.backends[i].endpoint);

            //let mut client = PublishBuildEventClient::new(self.channels[i].clone())
            //   .max_decoding_message_size(1024 * 1024 * 16);
            //eprintln!("{request:#?}");
            let mut request = Request::new(PublishLifecycleEventRequest { ..request.clone() });
            request.metadata_mut().insert(
                "x-buildbuddy-api-key",
                MetadataValue::from_static("af825193-d232-40a5-a9b0-7efecdd288"),
            );
            for key in headers.keys() {
                match key {
                    KeyRef::Ascii(key) => {
                        request
                            .metadata_mut()
                            .append(key, headers.get(key).expect("").clone());
                    }
                    KeyRef::Binary(key) => {
                        request
                            .metadata_mut()
                            .append_bin(key, headers.get_bin(key).expect("").clone());
                    }
                }
            }
            eprintln!("{request:#?}");
            self.clients[i]
                .clone()
                .publish_lifecycle_event(request)
                .await
                .inspect_err(|e| eprintln!("{e}"))?;
        }

        Ok(Response::new(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    const DEFAULT_PORT: u16 = 9000;

    CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider())
        .expect("failed to install crypto provider");

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

    nbes_service
        .connect()
        .await
        .expect("failed to connect to bes backends");

    eprintln!("connected to {} bes backends", nbes_service.backends.len());

    Server::builder()
        .add_service(PublishBuildEventServer::new(nbes_service))
        .serve(address)
        .await
}
