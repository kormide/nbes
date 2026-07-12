use anyhow::Result;
use build_proto::google::devtools::build::v1::PublishBuildToolEventStreamResponse;
use nbes::Binding;
use nbes::Config;
use tempfile::NamedTempFile;
use tonic::{Code, Request, Status, metadata::MetadataValue};

use crate::common::{
    MockBesServer, build_enqueued_lifecycle_event, build_lifecycle_event_stream_id,
    build_tool_event, build_tool_event_stream_id, connect_client_local, spawn_nbes,
    standard_lifecycle_events,
};

mod common;

#[tokio::test]
pub async fn test_blackhole_acks_build_tool_events() -> Result<()> {
    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: Vec::default(),
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

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

    let mut expected_seq = 1;
    while let Some(response) = response_stream.message().await? {
        assert_eq!(expected_seq, response.sequence_number);
        expected_seq += 1;
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;

    Ok(())
}

#[tokio::test]
pub async fn test_blackhole_acks_lifecycle_events() -> Result<()> {
    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: Vec::default(),
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    for lifecycle_event in standard_lifecycle_events() {
        client
            .publish_lifecycle_event(Request::new(lifecycle_event))
            .await?;
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;

    Ok(())
}

#[tokio::test]
pub async fn test_build_tool_event_client_sends_inconsistent_stream_id() -> Result<()> {
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
pub async fn test_build_tool_event_backend_responds_with_wrong_stream_id() -> Result<()> {
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

#[tokio::test]
pub async fn test_forward_responds_in_sequence() -> Result<()> {
    let server_uds = NamedTempFile::new()?;
    let mock_server = MockBesServer::spawn(
        String::from("mock_server"),
        Binding::UnixDomainSocket(server_uds.path().to_path_buf()),
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

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

    let mut expected_seq = 1;
    while let Some(response) = response_stream.message().await? {
        assert_eq!(expected_seq, response.sequence_number);
        expected_seq += 1;
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    mock_server.shutdown().await;

    Ok(())
}

#[tokio::test]
pub async fn test_forward_acks_lifecycle_events() -> Result<()> {
    let server_uds = NamedTempFile::new()?;
    let mock_server = MockBesServer::spawn(
        String::from("mock_server"),
        Binding::UnixDomainSocket(server_uds.path().to_path_buf()),
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    for lifecycle_event in standard_lifecycle_events() {
        client
            .publish_lifecycle_event(Request::new(lifecycle_event))
            .await?;
    }

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    mock_server.shutdown().await;

    Ok(())
}

#[tokio::test]
pub async fn test_forwards_headers() -> Result<()> {
    let server_uds = NamedTempFile::new()?;
    let mock_server = MockBesServer::spawn(
        String::from("mock_server"),
        Binding::UnixDomainSocket(server_uds.path().to_path_buf()),
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![mock_server.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_tool_event_stream_id();
    let request_stream = futures::stream::iter([build_tool_event(&stream_id, 1)]);

    let mut request = Request::new(request_stream);

    // Add a "foo: bar" header to the request
    request
        .metadata_mut()
        .append("foo", MetadataValue::from_static("bar"));
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    while let Some(_) = response_stream.message().await? {}

    mock_server
        .assert(|mock| {
            let requests = &mock.build_tool_event_stream_requests;
            assert_eq!(1, requests.len());
            let header = requests[0].metadata.get("foo");
            assert!(header.is_some());
            assert_eq!(header.unwrap(), "bar");
        })
        .await;

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    mock_server.shutdown().await;

    Ok(())
}

#[tokio::test]
pub async fn test_lifecycle_request_fails_when_one_backend_fails() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;
    let b2_uds = NamedTempFile::new()?;
    let b2 = MockBesServer::spawn(
        String::from("b2"),
        Binding::UnixDomainSocket(b2_uds.path().to_path_buf()),
    )
    .await;

    b2.fail_lifecycle_events().await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend(), b2.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_lifecycle_event_stream_id();
    let response = client
        .publish_lifecycle_event(Request::new(build_enqueued_lifecycle_event(&stream_id)))
        .await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(Code::Internal, status.code());
    assert_eq!("b2 failed lifecycle request: oops", status.message());

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    b1.shutdown().await;
    b2.shutdown().await;

    Ok(())
}

#[tokio::test]
pub async fn test_event_stream_request_fails_when_one_backend_fails() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;
    let stream_id = build_tool_event_stream_id();
    b1.mock_event_stream_responses([
        Ok(PublishBuildToolEventStreamResponse {
            stream_id: Some(stream_id.clone()),
            sequence_number: 1,
        }),
        Ok(PublishBuildToolEventStreamResponse {
            stream_id: Some(stream_id.clone()),
            sequence_number: 2,
        }),
    ])
    .await;
    let b2_uds = NamedTempFile::new()?;
    let b2 = MockBesServer::spawn(
        String::from("b2"),
        Binding::UnixDomainSocket(b2_uds.path().to_path_buf()),
    )
    .await;
    b2.mock_event_stream_responses([
        Ok(PublishBuildToolEventStreamResponse {
            stream_id: Some(stream_id.clone()),
            sequence_number: 1,
        }),
        Err(Status::internal("oops")),
    ])
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend(), b2.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let (shutdown_nbes, nbes_handle) = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let request_stream = futures::stream::iter([
        build_tool_event(&stream_id, 1),
        build_tool_event(&stream_id, 2), // b2 fails
    ]);

    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    response_stream.message().await?; // r1
    let r2 = response_stream.message().await;

    assert!(r2.is_err());
    let status = r2.unwrap_err();
    assert_eq!(Code::Internal, status.code());
    assert_eq!("b2 failed event stream request: oops", status.message());

    shutdown_nbes.send(()).unwrap();
    nbes_handle.await??;
    b1.shutdown().await;
    b2.shutdown().await;

    Ok(())
}
