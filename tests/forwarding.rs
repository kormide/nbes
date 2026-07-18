use std::task::Poll;

use anyhow::Result;
use build_proto::google::devtools::build::v1::PublishBuildToolEventStreamResponse;
use futures::join;
use nbes::Binding;
use nbes::Config;
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio::task::yield_now;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Status};

use crate::common::{
    MockBesServer, build_enqueued_lifecycle_event, build_lifecycle_event_stream_id,
    build_tool_event, build_tool_event_stream_id, connect_client_local, spawn_nbes,
    standard_lifecycle_events,
};

mod common;

#[tokio::test]
pub async fn test_stream_responds_in_sequence() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend()],
        listen: nbes_binding.clone(),
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

    let mut expected_seq = 1;
    while let Some(response) = response_stream.message().await? {
        assert_eq!(expected_seq, response.sequence_number);
        expected_seq += 1;
    }

    join!(shutdown_nbes, b1.shutdown());

    Ok(())
}

#[tokio::test]
pub async fn test_acks_lifecycle_events() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let shutdown_nbes = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    for lifecycle_event in standard_lifecycle_events() {
        client
            .publish_lifecycle_event(Request::new(lifecycle_event))
            .await?;
    }

    join!(shutdown_nbes, b1.shutdown());

    Ok(())
}

#[tokio::test]
pub async fn test_lifecycle_fails_when_one_backend_fails() -> Result<()> {
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

    let shutdown_nbes = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_lifecycle_event_stream_id();
    let response = client
        .publish_lifecycle_event(Request::new(build_enqueued_lifecycle_event(&stream_id)))
        .await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(Code::Internal, status.code());
    assert_eq!("b2 failed lifecycle request: oops", status.message());

    join!(shutdown_nbes, b1.shutdown(), b2.shutdown());

    Ok(())
}

#[tokio::test]
pub async fn test_stream_request_fails_when_one_backend_fails() -> Result<()> {
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

    let shutdown_nbes = spawn_nbes(config).await;

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

    join!(shutdown_nbes, b1.shutdown(), b2.shutdown());

    Ok(())
}

#[tokio::test]
pub async fn test_request_stream_asynchronously_forwards_requests() -> Result<()> {
    // Some BES backends (e.g., Buildbuddy) may way for the entire request stream
    // to be sent before it beings sending responses. This test ensures that the
    // entire request stream is forwarded asynchronously rather sending requests and
    // receiving responses in lockstep.

    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;

    let (tx, rx) = mpsc::channel(1);
    let response_stream = ReceiverStream::new(rx);
    b1.preprocess_events().await;
    b1.mock_response_stream(Box::pin(response_stream)).await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend()],
        listen: nbes_binding.clone(),
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

    // Yield to show that we have requests a change to be processed, whether
    // or not it's actually needed
    yield_now().await;

    // A response has not been received yet
    let mut r1 = Box::pin(response_stream.message());
    let Poll::Pending = futures::poll!(&mut r1) else {
        panic!("expected repsonse to not be ready");
    };

    // But all of the requests have been processed
    b1.assert(|data| {
        assert_eq!(5, data.processed_events);
    })
    .await;

    // Send the first response back
    tx.send(Ok(PublishBuildToolEventStreamResponse {
        sequence_number: 1,
        stream_id: Some(stream_id.clone()),
    }))
    .await?;

    // Receive the first response
    let r1 = loop {
        match futures::poll!(&mut r1) {
            Poll::Pending => {
                yield_now().await;
            }
            Poll::Ready(response) => break response,
        }
    }?
    .unwrap();

    assert_eq!(1, r1.sequence_number);

    // Asusume the rest of the responses will be received
    drop(tx);

    join!(shutdown_nbes, b1.shutdown());

    Ok(())
}

#[tokio::test]
pub async fn test_request_stream_starts_with_larger_sequence_number() -> Result<()> {
    // A request stream may begin with a non-1 sequience number if the stream previously
    // failed and the client retries from the successful event.
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend()],
        listen: nbes_binding.clone(),
    };

    let shutdown_nbes = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    let stream_id = build_tool_event_stream_id();
    let request_stream = futures::stream::iter([
        build_tool_event(&stream_id, 20),
        build_tool_event(&stream_id, 21),
    ]);
    let request = Request::new(request_stream);
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    let r1 = response_stream.message().await?.unwrap();
    assert_eq!(20, r1.sequence_number);

    let r2 = response_stream.message().await?.unwrap();
    assert_eq!(21, r2.sequence_number);

    futures::join!(shutdown_nbes, b1.shutdown());

    Ok(())
}
