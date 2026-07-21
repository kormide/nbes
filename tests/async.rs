use anyhow::Result;
use build_proto::google::devtools::build::v1::PublishBuildToolEventStreamResponse;
use futures::join;
use nbes::Binding;
use nbes::Config;
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use crate::common::MockBesServer;
use crate::common::{
    build_tool_event, build_tool_event_stream_id, connect_client_local, spawn_nbes,
};

mod common;

#[tokio::test]
pub async fn test_async_backend_does_not_block_on_responses() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;
    let b1_backend = b1.to_bes_backend();

    let b2_uds = NamedTempFile::new()?;
    let b2 = MockBesServer::spawn(
        String::from("b2"),
        Binding::UnixDomainSocket(b2_uds.path().to_path_buf()),
    )
    .await;

    // mock b2's response stream and delay sending back responses
    // to show that nbes doens't block on it
    let (b2_response_tx, b2_response_rx) = mpsc::channel(1);
    let b2_response_stream = ReceiverStream::new(b2_response_rx);
    b2.mock_response_stream(Box::pin(b2_response_stream)).await;

    let mut b2_backend = b2.to_bes_backend();
    b2_backend.set_async(true); // handle b2 responses asynchronously

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1_backend, b2_backend],
        listen: nbes_binding.clone(),
        ..Default::default()
    };

    let shutdown_nbes = spawn_nbes(config).await;
    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_tool_event_stream_id();
    let request_stream = futures::stream::iter([
        build_tool_event(&stream_id, 1),
        build_tool_event(&stream_id, 2),
        build_tool_event(&stream_id, 3),
        build_tool_event(&stream_id, 4),
        build_tool_event(&stream_id, 5),
    ]);

    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    b2_response_tx
        .send(Ok(PublishBuildToolEventStreamResponse {
            sequence_number: 1,
            stream_id: Some(stream_id.clone()),
        }))
        .await?;

    // receive all responses from nbes without blocking
    while let Some(_) = response_stream.message().await? {}

    drop(b2_response_tx);

    join!(shutdown_nbes, b1.shutdown(), b2.shutdown());

    Ok(())
}
