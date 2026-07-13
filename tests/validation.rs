use anyhow::Result;
use build_proto::google::devtools::build::v1::PublishBuildToolEventStreamResponse;
use nbes::Binding;
use nbes::Config;
use tempfile::NamedTempFile;
use tonic::{Code, Request};

use crate::common::{
    MockBesServer, build_tool_event, build_tool_event_stream_id, connect_client_local, spawn_nbes,
};

mod common;

#[tokio::test]
pub async fn test_stream_client_sends_inconsistent_stream_id() -> Result<()> {
    // This tests a misbehaving client, which we don't expect Bazel to be, but test it anyway

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: Vec::default(),
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_tool_event_stream_id();
    let other_stream_id = build_tool_event_stream_id();
    let request_stream = futures::stream::iter([
        build_tool_event(&stream_id, 1),
        build_tool_event(&other_stream_id, 2),
    ]);

    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    response_stream.message().await?;
    let r2 = response_stream.message().await;

    assert!(r2.is_err());
    let status = r2.unwrap_err();
    assert_eq!(Code::Internal, status.code());
    assert_eq!(
        "received inconsistent stream id from client",
        status.message()
    );

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;

    Ok(())
}

#[tokio::test]
pub async fn test_stream_backend_responds_with_wrong_stream_id() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;
    let stream_id = build_tool_event_stream_id();
    let wrong_stream_id = build_tool_event_stream_id();
    b1.mock_event_stream_responses([Ok(PublishBuildToolEventStreamResponse {
        stream_id: Some(wrong_stream_id),
        sequence_number: 1,
    })])
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let request_stream = futures::stream::iter([build_tool_event(&stream_id, 1)]);

    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    let r1 = response_stream.message().await;
    assert!(r1.is_err());
    let status = r1.unwrap_err();
    assert_eq!(Code::Internal, status.code());
    assert_eq!(
        "bes backend b1 responded with unexpected stream id",
        status.message()
    );

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    b1.shutdown().await;

    Ok(())
}
