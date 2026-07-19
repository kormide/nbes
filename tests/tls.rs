use anyhow::Result;
use futures::join;
use nbes::Binding;
use nbes::Config;
use nbes::ServerTlsConfig;
use tempfile::NamedTempFile;
use tonic::Request;

use crate::common::connect_client_local_tls;
use crate::common::generate_tls_keypair;
use crate::common::{
    MockBesServer, build_enqueued_lifecycle_event, build_lifecycle_event_stream_id,
    build_tool_event, build_tool_event_stream_id, connect_client_local, spawn_nbes,
    standard_lifecycle_events,
};

mod common;

#[tokio::test]
pub async fn foobar() -> Result<()> {
    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
    )
    .await;

    let (certificate, private_key) = generate_tls_keypair(["foobes.com"]);

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        bes_backends: vec![b1.to_bes_backend()],
        listen: nbes_binding.clone(),
        server_tls_config: Some(ServerTlsConfig {
            certificate: certificate.path().to_path_buf(),
            private_key: private_key.path().to_path_buf(),
        }),
    };

    let shutdown_nbes = spawn_nbes(config).await;

    let mut client =
        connect_client_local_tls(nbes_binding, &certificate, "https://foobes.com").await?;

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
