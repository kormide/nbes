use anyhow::{Context, Result};
use args::Args;
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use clap::Parser;
use futures::Stream;
use futures::stream::{self, StreamExt};
use std::{fs, path::Path, pin::Pin, str::FromStr};
use tokio::{
    net::UnixListener,
    sync::broadcast::{self, Sender},
};
use tokio_stream::wrappers::{BroadcastStream, UnixListenerStream};
use tonic::IntoStreamingRequest;
use tonic::{
    Request, Response, Status, Streaming,
    metadata::{KeyAndValueRef, MetadataMap},
    transport::{Channel, ClientTlsConfig, Endpoint, Server},
};
use url::Url;

mod args;

struct NBesService {
    backends: Vec<BesBackend>,
}

#[derive(Clone)]
struct BesBackend {
    name: String,
    endpoint: Url,
    client: Option<PublishBuildEventClient<Channel>>,
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

impl BesBackend {
    /// Set up a client for a gRPC channel to the bes backend that does not
    /// connect until first use.
    pub fn lazy_connect(&mut self) -> Result<()> {
        Ok(match self.client {
            Some(_) => {}
            None => {
                // Tonic doesn't appear to like "grpcs" as a scheme. Swap it with
                // https for the client connection to avoid FRAME_SIZE_ERROR errors.
                let mut endpoint = self.endpoint.clone();
                if endpoint.scheme() == "grpcs" {
                    // Cannot call `set_scheme` to change form grpcs to https
                    // https://docs.rs/url/latest/url/struct.Url.html#method.set_scheme
                    endpoint = Url::parse(&format!(
                        "https://{}:{}",
                        endpoint.host_str().expect("bes endpoint is missing host"),
                        endpoint.port().expect("best endpoint is missing port")
                    ))?;
                }

                let channel = Endpoint::from_str(endpoint.as_str())
                    .context(format!(
                        "failed to parse endpoint for backend {}",
                        self.name
                    ))?
                    .tls_config(ClientTlsConfig::new().with_native_roots())?
                    // TODO: properties to potentially configure
                    // .connect_timeout(Duration::from_secs(10))
                    // .tcp_keepalive(tcp_keepalive)
                    // .tcp_keepalive_interval(tcp_keepalive_interval)
                    // .tcp_keepalive_retries(tcp_keepalive_retries)
                    // .concurrency_limit(limit)
                    // .rate_limit(limit, duration)
                    // .http2_keep_alive_interval(interval)
                    // .keep_alive_while_idle(enabled)
                    // .keep_alive_timeout(duration)
                    .connect_lazy();

                self.client.replace(PublishBuildEventClient::new(channel));
            }
        })
    }
}

impl NBesService {
    pub fn new() -> Self {
        Self {
            backends: Vec::default(),
        }
    }

    pub fn add_backend(&mut self, mut backend: BesBackend) -> Result<()> {
        backend.lazy_connect()?;
        self.backends.push(backend);
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
        // struct State {
        //     /// Incoming request stream from Bazel
        //     incoming_requests: Streaming<PublishBuildToolEventStreamRequest>,
        //     /// Incoming response streams from the bes backends
        //     incoming_responses: Vec<(String, Streaming<PublishBuildToolEventStreamResponse>)>,
        // }

        let (metadata, _, incoming_requests) = request.into_parts();

        // // Open streams to each of the bes backends. Incoming requests are sent
        // // via a broadcast channel. Each backend turns its receiver into a stream
        // // to forward requests to the backend.
        // let (tx, _) = broadcast::channel::<PublishBuildToolEventStreamRequest>(256);
        // let incoming_responses = stream::iter(self.backends.iter())
        //     .then(|backend| async {
        //         let receiver = tx.subscribe();
        //         let outbound_requests = BroadcastStream::new(receiver).map(|request| {
        //             // BroadcastStream wraps the request in a Result in case it fails to
        //             // receive from the sender. But publish_build_tool_event_stream() takes
        //             // a stream of requests, so we must unwrap it. This should "never" occur,
        //             // so just panic.
        //             request.expect("failed to receive request from broadcast channel")
        //         });

        //         let mut request = Request::new(outbound_requests);
        //         copy_request_metadata(&metadata, &mut request);

        //         (
        //             backend.name.clone(),
        //             backend
        //                 .client
        //                 .as_ref()
        //                 .unwrap()
        //                 .clone() // cloning clients is cheap
        //                 .publish_build_tool_event_stream(request)
        //                 .await
        //                 .expect("failed to initiate stream")
        //                 .into_inner(),
        //         )
        //     })
        //     .collect()
        //     .await;

        // let state = State {
        //     incoming_requests,
        //     incoming_responses,
        // };

        struct State {
            backends: Vec<BesBackend>,
            incoming_requests: Streaming<PublishBuildToolEventStreamRequest>,
            outbound_request_tx: Sender<PublishBuildToolEventStreamRequest>,
            outbound_responses: Vec<Streaming<PublishBuildToolEventStreamResponse>>,
            outbound_requests: Vec<
                Pin<Box<dyn Stream<Item = PublishBuildToolEventStreamRequest> + Send + 'static>>,
            >,
            clients: Vec<PublishBuildEventClient<Channel>>,
        }

        let (outbound_request_tx, _) = broadcast::channel::<PublishBuildToolEventStreamRequest>(1);

        let state = State {
            backends: self.backends.clone(),
            incoming_requests,
            outbound_request_tx,
            outbound_responses: Vec::new(),
            outbound_requests: Vec::new(),
            clients: Vec::new(),
        };

        Ok(Response::new(Box::pin(stream::unfold(
            state,
            |mut state| async move {
                let request = state
                    .incoming_requests
                    .message()
                    .await
                    .expect("failed to receive request");

                match request {
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

                        // Only create outbound request streams after the first message has been
                        // received. The tonic client has a bug where making a streaming RPC call
                        // will hang unless a message is available on the stream.
                        if state.outbound_requests.is_empty() {
                            for backend in state.backends.clone() {
                                let rx = state.outbound_request_tx.subscribe();
                                let initial_request = request.clone();
                                let initial_stream = stream::once(async move { initial_request });
                                let request_stream = BroadcastStream::new(rx).map(|request| {
                                    // BroadcastStream wraps the request in a Result in case it fails to
                                    // receive from the sender. But publish_build_tool_event_stream() takes
                                    // a stream of requests, so we must unwrap it. This should "never" occur,
                                    // so just panic.
                                    request
                                        .expect("failed to receive request from broadcast channel")
                                });

                                let stream = initial_stream.chain(request_stream);

                                state.outbound_requests.push(stream.boxed());
                            }
                        }

                        // Broadcast the request to each backend
                        if !state.backends.is_empty() {
                            state
                                .outbound_request_tx
                                .send(request.clone())
                                .expect("failed to broadcast request to backends");
                        }

                        if state.clients.is_empty() {
                            for backend in state.backends.iter() {
                                state.clients.push(
                                    backend
                                        .client
                                        .as_ref()
                                        .expect("failed to get client")
                                        .clone(),
                                );
                            }
                        }

                        if !state.outbound_requests.is_empty() {
                            for (i, outbound_request) in
                                state.outbound_requests.drain(..).into_iter().enumerate()
                            {
                                let request = outbound_request.into_streaming_request();
                                let future = state.clients[i]
                                    .publish_build_tool_event_stream(request)
                                    .await;
                                //     .expect("failed to start stream with backend")
                                //     .into_inner();
                            }
                        }

                        // if state.outbound_responses.is_empty() {
                        //     state.outbound_responses = stream::iter(self.backends.iter())
                        //         .then(|backend| async {
                        //             let request = state
                        //                 .outbound_requests
                        //                 .pop()
                        //                 .expect("failed to pop outbound request stream")
                        //                 .into_streaming_request();
                        //
                        //             let client = backend.client.as_ref().unwrap().clone();
                        //
                        //             state.clients.push(client);
                        //
                        //             let outbound_response_stream = state
                        //                 .clients
                        //                 .pop()
                        //                 .expect("failed to get client")
                        //                 .publish_build_tool_event_stream(request)
                        //                 .await
                        //                 .expect("failed to start stream with backend")
                        //                 .into_inner();
                        //
                        //             outbound_response_stream
                        //         })
                        //         .collect()
                        //         .await;
                        // }

                        return Some((
                            Ok(PublishBuildToolEventStreamResponse {
                                stream_id: Some(stream_id.clone()),
                                sequence_number: sequence_number,
                            }),
                            state,
                        ));
                    }
                    None => None,
                }
            },
        ))))

        // Ok(Response::new(Box::pin(unfold(state, move |mut state| {
        //     let tx = tx.clone();
        //     async move {
        //         // Receive a request from the Bazel client
        //         let request = state
        //             .incoming_requests
        //             .message()
        //             .await
        //             .expect("failed to receive message");

        //         if let Some(request) = request {
        //             let PublishBuildToolEventStreamRequest {
        //                 ordered_build_event:
        //                     Some(OrderedBuildEvent {
        //                         stream_id: Some(ref stream_id),
        //                         sequence_number,
        //                         ..
        //                     }),
        //                 ..
        //             } = request
        //             else {
        //                 return Some((
        //                     Err(Status::invalid_argument(
        //                         "ordered_build_event field(s) are missing",
        //                     )),
        //                     state,
        //                 ));
        //             };

        //             // Forward the request to all backends via the broadcast channel
        //             if !state.incoming_responses.is_empty() {
        //                 tx.send(request.clone()).expect("failed to send message");
        //             }

        //             // Wait for a response from each backend
        //             let responses: Vec<_> =
        //                 join_all(state.incoming_responses.iter_mut().map(|r| r.1.message()))
        //                     .await
        //                     .into_iter()
        //                     .map(|r| r.expect("failed to receive message from backend"))
        //                     .collect();

        //             // Validate the responses
        //             for (i, response) in responses.iter().enumerate() {
        //                 let backend_name = &state.incoming_responses[i].0;
        //                 match response {
        //                     Some(response) => {
        //                         let PublishBuildToolEventStreamResponse {
        //                             stream_id: Some(response_stream_id),
        //                             sequence_number: response_sequence_number,
        //                         } = response
        //                         else {
        //                             return Some((
        //                                 Err(Status::internal(format!(
        //                                     "response from bes backend {backend_name} is missing stream_id",
        //                                 ))),
        //                                 state,
        //                             ));
        //                         };

        //                         if response_stream_id != stream_id
        //                             || *response_sequence_number != sequence_number
        //                         {
        //                             eprintln!(
        //                                 "warning: bes backend {} responded with unexpected stream id/sequence (expected={:?}/{}, actual = {:?}/{})",
        //                                 backend_name,
        //                                 stream_id,
        //                                 sequence_number,
        //                                 response_stream_id,
        //                                 response_sequence_number
        //                             );

        //                             return Some((
        //                                 Err(Status::internal(format!(
        //                                     "bes backend {backend_name} responded with unexpected stream/sequence",
        //                                 ))),
        //                                 state,
        //                             ));
        //                         }
        //                     }
        //                     None => {
        //                         eprintln!(
        //                             "bes backend {backend_name} unexpectedly ended stream {stream_id:?}"
        //                         );
        //                         // End the stream for all backends. Consider making this more fault
        //                         // tolerant and continue the other streams?
        //                         return None;
        //                     }
        //                 };
        //             }

        //             // Send a single response back
        //             return Some((
        //                 Ok(PublishBuildToolEventStreamResponse {
        //                     stream_id: Some(stream_id.clone()),
        //                     sequence_number: sequence_number,
        //                 }),
        //                 state,
        //             ));
        //         }

        //         // The client closed the request stream. End the response stream.
        //         None
        //     }
        // }))))
    }

    async fn publish_lifecycle_event(
        &self,
        request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        let (metadata, _, message) = request.into_parts();

        for backend in &self.backends {
            let mut outbound_request = Request::new(message.clone());
            copy_request_metadata(&metadata, &mut outbound_request);
            backend
                .client
                .as_ref()
                .unwrap()
                .clone() // cloning client is cheap
                .publish_lifecycle_event(outbound_request)
                .await?;
        }

        Ok(Response::new(()))
    }
}

// struct ForwardedRequestStream {
//     initial_request: PublishBuildToolEventStreamRequest,
// }
//
// impl ForwardedRequestStream {
//     pub fn new(initial_request: PublishBuildToolEventStreamRequest) -> Self {
//         Self {
//             initial_request,
//         }
//     }
// }
//
// impl Stream for ForwardedRequestStream {
//     type Item = PublishBuildToolEventStreamRequest;
//
//     fn poll_next(
//         self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         todo!()
//     }
// }

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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    eprintln!(
        "starting bes server on {}",
        args.socket
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or(args.listen.to_string())
    );

    let bes_backends: Vec<BesBackend> = args.bes_backends.into_iter().map(|b| b.into()).collect();

    let mut nbes_service = NBesService::new();

    if bes_backends.is_empty() {
        eprintln!("no bes backends configured; bes events will be swallowed by a black hole");
    }

    for backend in bes_backends {
        eprintln!(
            "configured backend {} -> {}",
            backend.name, backend.endpoint
        );
        nbes_service.add_backend(backend)?;
    }

    let router = Server::builder()
        // TODO: properties to potentially configure
        // .concurrency_limit_per_connection(100)
        // .load_shed(true)
        // .max_concurrent_streams(Some(1000))
        .add_service(PublishBuildEventServer::new(nbes_service));

    if let Some(socket_url) = args.socket {
        let socket_path = Path::new(socket_url.path());
        if socket_path.exists() {
            fs::remove_file(socket_path).context("failed to remove existing socket")?;
        }
        let socket_listener = UnixListener::bind(socket_path)?;
        let socket_stream = UnixListenerStream::new(socket_listener);
        router.serve_with_incoming(socket_stream).await?
    } else {
        router.serve(args.listen).await?;
    };

    Ok(())
}
