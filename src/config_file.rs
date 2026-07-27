use std::str::FromStr;
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};

use anyhow::{Context, Result};
use nbes::forwarding::BesBackend as RealBesBackend;
use serde::Deserialize;

#[derive(Default, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub bes_backends: Vec<BesBackend>,
    #[serde(default)]
    pub tls_certificates: Vec<PathBuf>,
}

#[derive(Default, Deserialize)]
pub struct Server {
    #[serde(default)]
    pub listen: Option<String>,
    #[serde(default)]
    pub tls: ServerTls,
    pub concurrency_limit_per_connection: Option<usize>,
    #[serde(default)]
    pub load_shed_requests: bool,
    pub max_concurrent_streams: Option<u32>,
    pub max_connection_age: Option<u64>,
    pub max_connection_age_grace: Option<u64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum BesBackend {
    Endpoint(String),
    Spec(BesBackendSpec),
}

#[derive(Deserialize)]
pub struct BesBackendSpec {
    pub name: Option<String>,
    pub endpoint: String,
    #[serde(default)]
    pub r#async: bool,
    #[serde(default)]
    pub remote_headers: HashMap<String, String>,
    pub tls_client_certificate: Option<PathBuf>,
    pub tls_client_key: Option<PathBuf>,
    pub connect_timeout: Option<u64>,
    pub request_timeout: Option<u64>,
    pub request_buffer_size: Option<usize>,
}

#[derive(Default, Deserialize)]
pub struct ServerTls {
    pub certificate: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

impl ConfigFile {
    pub fn parse(path: &Path) -> Result<Self> {
        let yaml = fs::read_to_string(path).context("failed to read config file")?;
        Self::parse_yaml(yaml)
    }

    fn parse_yaml(yaml: impl Into<String>) -> Result<Self> {
        let config_file: Self =
            serde_yaml_ng::from_str(&yaml.into()).context("failed to parse config file yaml")?;
        config_file.validate().context("invalid config file")?;

        Ok(config_file)
    }

    fn validate(&self) -> Result<()> {
        // Unique bes backend names
        let mut backend_names: HashSet<&str> = HashSet::new();
        for backend in &self.bes_backends {
            if let BesBackend::Spec(backend) = backend {
                if let Some(name) = backend.name.as_ref() {
                    if !backend_names.contains(name.as_str()) {
                        backend_names.insert(name.as_str());
                    } else {
                        anyhow::bail!("multiple bes backends have the same name {}", name);
                    }
                }
            }
        }

        for backend in &self.bes_backends {
            if let BesBackend::Spec(backend) = backend {
                if backend.tls_client_certificate.is_some() && backend.tls_client_key.is_none()
                    || backend.tls_client_certificate.is_none() && backend.tls_client_key.is_some()
                {
                    anyhow::bail!(
                        "tls_client_certificate and tls_client_key must be specified together or not at all for a backend",
                    );
                }
            }
        }
        Ok(())
    }
}

impl TryInto<RealBesBackend> for BesBackend {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<RealBesBackend, Self::Error> {
        Ok(match self {
            BesBackend::Endpoint(endpoint) => RealBesBackend::builder(endpoint)?.build(),
            BesBackend::Spec(spec) => {
                let mut backend = RealBesBackend::builder(spec.endpoint)?;
                let mut metadata = MetadataMap::new();
                for (name, value) in spec.remote_headers {
                    metadata.append(
                        MetadataKey::from_str(&name).context("invalid remote header name")?,
                        MetadataValue::from_str(&value).context("invalid remote header value")?,
                    );
                }
                backend = backend.remote_headers(metadata).r#async(spec.r#async);

                if let Some(name) = spec.name {
                    backend = backend.name(name);
                }

                if let (Some(tls_client_certificate), Some(tls_client_key)) =
                    (spec.tls_client_certificate, spec.tls_client_key)
                {
                    backend = backend.tls_client_identity(tls_client_certificate, tls_client_key);
                }

                if let Some(timeout) = spec.connect_timeout {
                    backend = backend.connect_timeout(Duration::from_secs(timeout));
                }

                if let Some(timeout) = spec.request_timeout {
                    backend = backend.request_timeout(Duration::from_secs(timeout));
                }

                if let Some(request_buffer_size) = spec.request_buffer_size {
                    backend = backend.request_buffer_size(request_buffer_size);
                }

                backend.build()
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty() {
        let result = ConfigFile::parse_yaml("");

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.server.listen.is_none());
        assert!(config.server.tls.certificate.is_none());
        assert!(config.server.tls.key.is_none());
        assert!(config.server.concurrency_limit_per_connection.is_none());
        assert_eq!(false, config.server.load_shed_requests);
        assert!(config.server.max_concurrent_streams.is_none());
        assert!(config.server.max_connection_age.is_none());
        assert!(config.server.max_connection_age_grace.is_none());
        assert!(config.bes_backends.is_empty());
        assert!(config.tls_certificates.is_empty());
    }

    #[test]
    fn test_parse_server_listen() {
        let result = ConfigFile::parse_yaml(
            r#"
server:
    listen: 0.0.0.0:3000
"#,
        );

        assert!(result.is_ok());
        let config = result.unwrap();
        let listen = config.server.listen;
        assert!(listen.is_some_and(|listen| listen == "0.0.0.0:3000"));
    }

    #[test]
    fn test_parse_server_listen_unix_domain_socket() {
        let result = ConfigFile::parse_yaml(
            r#"
server:
    listen: unix:/tmp/foobar
"#,
        );

        assert!(result.is_ok());
        let config = result.unwrap();
        let listen = config.server.listen;
        assert!(listen.is_some_and(|listen| listen == "unix:/tmp/foobar"));
    }

    #[test]
    fn test_parse_bes_backend_endpoint() {
        let result = ConfigFile::parse_yaml(
            r#"
bes_backends:
- grpc://127.0.0.1
"#,
        );

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(1, config.bes_backends.len());
        let BesBackend::Endpoint(ref endpoint) = config.bes_backends[0] else {
            panic!("expected an endpoint bes backend");
        };
        assert_eq!("grpc://127.0.0.1", endpoint);
    }

    #[test]
    fn test_parse_bes_backend_spec_only_endpoint() {
        let result = ConfigFile::parse_yaml(
            r#"
bes_backends:
- endpoint: grpc://127.0.0.1
"#,
        );

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(1, config.bes_backends.len());
        let BesBackend::Spec(ref spec) = config.bes_backends[0] else {
            panic!("expected a spec bes backend");
        };
        assert_eq!("grpc://127.0.0.1", spec.endpoint);
    }

    #[test]
    fn test_parse_bes_backend_spec_defaults_non_required_fields() {
        let result = ConfigFile::parse_yaml(
            r#"
bes_backends:
- endpoint: grpc://127.0.0.1
"#,
        );

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(1, config.bes_backends.len());
        let BesBackend::Spec(ref spec) = config.bes_backends[0] else {
            panic!("expected a spec bes backend");
        };
        assert_eq!("grpc://127.0.0.1", spec.endpoint);
        assert!(spec.name.is_none());
        assert_eq!(false, spec.r#async);
        assert!(spec.remote_headers.is_empty());
        assert!(spec.tls_client_certificate.is_none());
        assert!(spec.tls_client_certificate.is_none());
        assert!(spec.request_buffer_size.is_none());
    }

    #[test]
    fn test_parse_bes_backend_spec_missing_endpoint() {
        let result = ConfigFile::parse_yaml(
            r#"
bes_backends:
- name: foobar
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_bes_backend_spec_all_fields() {
        let result = ConfigFile::parse_yaml(
            r#"
bes_backends:
- name: foobar
  endpoint: grpc://127.0.0.1
  async: true
  remote_headers:
    foo: bar
  tls_client_certificate: /cert
  tls_client_key: /key
  connect_timeout: 5
  request_timeout: 10
  request_buffer_size: 1000
"#,
        );

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(1, config.bes_backends.len());
        let BesBackend::Spec(ref spec) = config.bes_backends[0] else {
            panic!("expected a spec bes backend");
        };
        assert_eq!("grpc://127.0.0.1", spec.endpoint);
        assert_eq!("foobar", spec.name.as_ref().unwrap());
        assert_eq!(true, spec.r#async);
        assert_eq!(1, spec.remote_headers.len());
        assert_eq!("bar", spec.remote_headers["foo"]);
        assert_eq!("/cert", spec.tls_client_certificate.as_ref().unwrap());
        assert_eq!("/key", spec.tls_client_key.as_ref().unwrap());
        assert_eq!(5, spec.connect_timeout.unwrap());
        assert_eq!(10, spec.request_timeout.unwrap());
        assert_eq!(1000, spec.request_buffer_size.unwrap());
    }

    #[test]
    fn test_parse_bes_backend_multiple() {
        let result = ConfigFile::parse_yaml(
            r#"
bes_backends:
- grpcs://foo.bar
- name: foobar
  endpoint: grpc://127.0.0.1
"#,
        );

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(2, config.bes_backends.len());
    }

    #[test]
    fn test_parse_bes_backend_multiple_same_name() {
        let result = ConfigFile::parse_yaml(
            r#"
bes_backends:
- name: foobar
  endpoint: grpc://127.0.0.1:3000
- name: foobar
  endpoint: grpc://127.0.0.1:3001
"#,
        );

        assert!(result.is_err());
    }
}
