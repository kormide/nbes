use anyhow::Result;
use nbes::Binding;
use nbes::Config;
use tempfile::NamedTempFile;
use tonic::Request;

use crate::common::{
    build_tool_event, build_tool_event_stream_id, connect_client_local, spawn_nbes,
    standard_lifecycle_events,
};

mod common;

#[tokio::test]
pub async fn test_blackhole_acks_stream_events() -> Result<()> {
    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: Vec::default(),
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

    let mut expected_seq = 1;
    while let Some(response) = response_stream.message().await? {
        assert_eq!(expected_seq, response.sequence_number);
        expected_seq += 1;
    }

    shutdown_nbes.await;

    Ok(())
}

#[tokio::test]
pub async fn test_blackhole_acks_lifecycle_events() -> Result<()> {
    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: Vec::default(),
        listen: nbes_binding.clone(),
        ..Default::default()
    };

    let shutdown_nbes = spawn_nbes(config).await;

    let mut client = connect_client_local(nbes_binding).await?;

    for lifecycle_event in standard_lifecycle_events() {
        client
            .publish_lifecycle_event(Request::new(lifecycle_event))
            .await?;
    }

    shutdown_nbes.await;

    Ok(())
}
