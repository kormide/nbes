use anyhow::Result;
use futures::join;
use nbes::Binding;
use nbes::Config;
use nbes::ServerTlsConfig;
use tempfile::NamedTempFile;
use tonic::Request;
use url::Url;

use crate::common::connect_client_local;
use crate::common::connect_client_local_tls;
use crate::common::generate_tls_keypair;
use crate::common::{
    MockBesServer, build_tool_event, build_tool_event_stream_id, spawn_nbes,
    standard_lifecycle_events,
};

mod common;

#[tokio::test]
pub async fn can_connect_via_tls() -> Result<()> {
    let (certificate, private_key) = generate_tls_keypair(["foobes.com"]);

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());
    let config = Config {
        listen: nbes_binding.clone(),
        server_tls_config: Some(ServerTlsConfig {
            certificate: certificate.path().to_path_buf(),
            key: private_key.path().to_path_buf(),
        }),
        ..Default::default()
    };

    let shutdown_nbes = spawn_nbes(config).await;

    let mut client =
        connect_client_local_tls(nbes_binding, &certificate, "https://foobes.com").await?;

    for lifecycle_event in standard_lifecycle_events() {
        client
            .publish_lifecycle_event(Request::new(lifecycle_event))
            .await?;
    }

    shutdown_nbes.await;

    Ok(())
}

#[tokio::test]
pub async fn forwards_to_non_tls_backend() -> Result<()> {
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
            key: private_key.path().to_path_buf(),
        }),
        ..Default::default()
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

#[tokio::test]
pub async fn forwards_to_tls_backend() -> Result<()> {
    let (b1_certificate, b1_private_key) = generate_tls_keypair(["foobes.com"]);

    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn_tls(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
        &b1_certificate,
        &b1_private_key,
    )
    .await;

    let (certificate, private_key) = generate_tls_keypair(["barbes.com"]);

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());

    let mut b1_bes_backend = b1.to_bes_backend();
    b1_bes_backend.use_uds_tls_uri(Url::parse("https://foobes.com")?);

    let config = Config {
        bes_backends: vec![b1_bes_backend],
        listen: nbes_binding.clone(),
        server_tls_config: Some(ServerTlsConfig {
            certificate: certificate.path().to_path_buf(),
            key: private_key.path().to_path_buf(),
        }),
        tls_certificates: vec![b1_certificate.path().to_path_buf()],
    };

    let shutdown_nbes = spawn_nbes(config).await;

    let mut client =
        connect_client_local_tls(nbes_binding, &certificate, "https://barbes.com").await?;

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
pub async fn backend_requires_mtls() -> Result<()> {
    // Test client tls cert/key setup for a backend that requires mTLS

    let (b1_certificate, b1_private_key) = generate_tls_keypair(["foobes.com"]);
    let (client_certificate, client_private_key) = generate_tls_keypair(["client-id-12345"]);

    let b1_uds = NamedTempFile::new()?;
    let b1 = MockBesServer::spawn_mtls(
        String::from("b1"),
        Binding::UnixDomainSocket(b1_uds.path().to_path_buf()),
        &b1_certificate,
        &b1_private_key,
        vec![Box::new(client_certificate.path().to_path_buf())],
        true,
    )
    .await;

    let nbes_uds = NamedTempFile::new()?;
    let nbes_binding = Binding::UnixDomainSocket(nbes_uds.path().to_path_buf());

    let mut b1_bes_backend = b1.to_bes_backend();
    b1_bes_backend.use_uds_tls_uri(Url::parse("https://foobes.com")?);
    b1_bes_backend.set_client_tls_identity(
        client_certificate.path().to_path_buf(),
        client_private_key.path().to_path_buf(),
    );

    let config = Config {
        bes_backends: vec![b1_bes_backend],
        listen: nbes_binding.clone(),
        tls_certificates: vec![b1_certificate.path().to_path_buf()],
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

    join!(shutdown_nbes, b1.shutdown());

    Ok(())
}
