use anyhow::{Context, Result};
use args::Args;
use build_proto::google::devtools::build::v1::{
    OrderedBuildEvent, PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use clap::Parser;
use futures::{Stream, stream::unfold};
use futures::{
    future::join_all,
    stream::{self, StreamExt},
};
use std::{fs, marker::PhantomData, path::Path, pin::Pin, str::FromStr, task::Poll};
use tokio::{
    net::UnixListener,
    sync::{
        broadcast,
        oneshot::{self, Receiver, error::TryRecvError},
    },
};
use tokio_stream::wrappers::{BroadcastStream, UnixListenerStream};
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
        let incoming_responses = stream::iter(self.backends.iter())
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
                    backend.name.clone(),
                    backend
                        .client
                        .as_ref()
                        .unwrap()
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

        let (first_request_tx, first_request_rx) = oneshot::channel::<()>();

        Ok(Response::new(Box::pin(ResponseStream {
            rx: first_request_rx,
            clients: self
                .backends
                .iter()
                .map(|backend| {
                    backend
                        .client
                        .as_ref()
                        .expect("expected client to exist")
                        .clone()
                })
                .collect(),
            response_streams: Vec::new(),
            request_tx: tx.clone(),
            create_response_stream: move |client: &mut PublishBuildEventClient<Channel>| {
                let receiver = tx.subscribe();
                let foo = async {
                    let outbound_requests = BroadcastStream::new(receiver).map(|request| {
                        // BroadcastStream wraps the request in a Result in case it fails to
                        // receive from the sender. But publish_build_tool_event_stream() takes
                        // a stream of requests, so we must unwrap it. This should "never" occur,
                        // so just panic.
                        request.expect("failed to receive request from broadcast channel")
                    });
                    let request = Request::new(outbound_requests);
                    client
                        .publish_build_tool_event_stream(request)
                        .await
                        .expect("foobar")
                        .into_inner()
                };
                Box::new(foo)
            },
        })))

        // Ok(Response::new(Box::pin(unfold(state, move |mut state| {
        //     let tx = tx.clone();
        //     async move {
        //         // Receive a request from the Bazel client
        //         let request = state
        //             .incoming_requests
        //             .message()
        //             .await
        //             .expect("failed to receive message");
        //
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
        //
        //             // Forward the request to all backends via the broadcast channel
        //             if !state.incoming_responses.is_empty() {
        //                 tx.send(request.clone()).expect("failed to send message");
        //             }
        //
        //             // Wait for a response from each backend
        //             let responses: Vec<_> =
        //                 join_all(state.incoming_responses.iter_mut().map(|r| r.1.message()))
        //                     .await
        //                     .into_iter()
        //                     .map(|r| r.expect("failed to receive message from backend"))
        //                     .collect();
        //
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
        //
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
        //
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
        //
        //             // Send a single response back
        //             return Some((
        //                 Ok(PublishBuildToolEventStreamResponse {
        //                     stream_id: Some(stream_id.clone()),
        //                     sequence_number: sequence_number,
        //                 }),
        //                 state,
        //             ));
        //         }
        //
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

struct ResponseStream<F> {
    rx: Receiver<()>,
    clients: Vec<PublishBuildEventClient<Channel>>,
    response_streams: Vec<
        Pin<
            Box<
                dyn Future<
                        Output = Result<
                            Response<Streaming<PublishBuildToolEventStreamResponse>>,
                            Status,
                        >,
                    > + Send,
            >,
        >,
    >,

    // response_streams:
    //     Vec<Box<dyn Future<Output = Streaming<PublishBuildToolEventStreamResponse>> + Send>>,
    // response_streams: Vec<Streaming<PublishBuildToolEventStreamResponse>>,
    request_tx: broadcast::Sender<PublishBuildToolEventStreamRequest>,
    create_response_stream: F,
}

impl<F> Stream for ResponseStream<F>
where
    F: for<'a> FnMut(
        &'a mut PublishBuildEventClient<Channel>,
    ) -> Pin<
        Box<dyn Future<Output = Streaming<PublishBuildToolEventStreamResponse>> + 'a>,
    >,
{
    type Item = Result<PublishBuildToolEventStreamResponse, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.as_mut().rx.try_recv() {
            Ok(_) => {
                let request_tx = self.request_tx.clone();
                for client in &mut self.clients {
                    let receiver = request_tx.subscribe();
                    let outbound_requests = BroadcastStream::new(receiver).map(|request| {
                        // BroadcastStream wraps the request in a Result in case it fails to
                        // receive from the sender. But publish_build_tool_event_stream() takes
                        // a stream of requests, so we must unwrap it. This should "never" occur,
                        // so just panic.
                        request.expect("failed to receive request from broadcast channel")
                    });
                    let request = Request::new(outbound_requests);
                    let response_stream = client.publish_build_tool_event_stream(request);
                    // let response_stream_fut =
                    //     (self.as_mut().create_response_stream)(client.clone());
                    self.response_streams.push(Box::pin(response_stream));
                }
                todo!()
            }
            Err(TryRecvError::Empty) => Poll::Pending,
            Err(TryRecvError::Closed) => Poll::Pending,
        }
        //     if self.response_streams.is_empty() {
        //         unsafe {
        //             let foo = self.get_unchecked_mut();
        //             match foo.rx.try_recv() {
        //                 Ok(_) => {
        //                     for client in &foo.clients {
        //                         let receiver = self.request_tx.subscribe();
        //                         let outbound_requests = BroadcastStream::new(receiver).map(|request| {
        //                             // BroadcastStream wraps the request in a Result in case it fails to
        //                             // receive from the sender. But publish_build_tool_event_stream() takes
        //                             // a stream of requests, so we must unwrap it. This should "never" occur,
        //                             // so just panic.
        //                             request.expect("failed to receive request from broadcast channel")
        //                         });
        //                         let request = Request::new(outbound_requests);
        //                         let response_stream = client.publish_build_tool_event_stream(request);
        //                         let response_stream_fut =
        //                             (self.as_mut().create_response_stream)(client.clone());
        //                         // self.response_streams.push(Box::new(response_stream_fut));
        //                     }
        //                     todo!()
        //                 }
        //                 Err(TryRecvError::Empty) => Poll::Pending,
        //                 Err(TryRecvError::Closed) => Poll::Pending,
        //             }
        //         }
        //     } else {
        //         Poll::Pending
        //     }
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
