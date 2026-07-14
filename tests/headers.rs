use anyhow::Result;
use futures::join;
use nbes::Binding;
use nbes::Config;
use tempfile::NamedTempFile;
use tonic::{Request, metadata::MetadataValue};

use crate::common::{
    MockBesServer, build_enqueued_lifecycle_event, build_lifecycle_event_stream_id,
    build_tool_event, build_tool_event_stream_id, connect_client_local, spawn_nbes,
};

mod common;

#[tokio::test]
pub async fn test_stream_preserves_client_headers() -> Result<()> {
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
    let request_stream = futures::stream::iter([build_tool_event(&stream_id, 1)]);

    let mut request = Request::new(request_stream);

    // Add a "foo: bar" header to the request
    request
        .metadata_mut()
        .append("foo", MetadataValue::from_static("bar"));
    let response = client.publish_build_tool_event_stream(request).await?;
    let mut response_stream = response.into_inner();

    while let Some(_) = response_stream.message().await? {}

    b1.assert(|mock| {
        let requests = &mock.build_tool_event_stream_requests;
        assert_eq!(1, requests.len());
        let header = requests[0].metadata.get("foo");
        assert!(header.is_some());
        assert_eq!(header.unwrap(), "bar");
    })
    .await;

    join!(shutdown_nbes, b1.shutdown());

    Ok(())
}

#[tokio::test]
pub async fn test_lifecycle_preserves_client_headers() -> Result<()> {
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

    let build_event_stream_id = build_lifecycle_event_stream_id();
    let mut request = Request::new(build_enqueued_lifecycle_event(&build_event_stream_id));
    // Add a "moo: cow" header to the request
    request
        .metadata_mut()
        .append("moo", MetadataValue::from_static("cow"));

    client.publish_lifecycle_event(request).await?;

    b1.assert(|mock| {
        let requests = &mock.lifecycle_requests;
        assert_eq!(1, requests.len());
        let header = requests[0].metadata.get("moo");
        assert!(header.is_some());
        assert_eq!(header.unwrap(), "cow");
    })
    .await;

    join!(shutdown_nbes, b1.shutdown());

    Ok(())
}
