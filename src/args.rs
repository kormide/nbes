use anyhow::Result;
use clap::Parser;
use nbes::forwarding::BesBackend;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};

/// A BES backend that forwards to other BES backends
#[derive(Default, Debug, Parser)]
#[command(version, name = "nbes", about = "A BES backend that forwards to other BES backends", long_about = None)]
pub struct Args {
    /// A BES backend to forward events to. In the simplest form, this
    /// can be a grpc endpoint. E.g.,
    ///
    /// --bes_backend=[SCHEME://]HOST[:PORT]
    ///
    /// The scheme may be grpc or grpcs. The default scheme/port is grpcs/443.
    ///
    /// To set additional properties for the backend, use the comma-separated value form:
    ///
    /// --bes_backend=name=my-bes-service,endpoint=[SCHEME://]HOST[:PORT]
    ///
    /// Multiple backends can be configured by repeating the bes_backend argument.
    ///
    /// Supported properties include:
    ///
    ///   name=[NAME]
    ///
    ///     Human-readable identifier for the bes backend, displayed in logs
    ///
    ///   endpoint=[SCHEME://]HOST[:PORT]
    ///
    ///     Endpoint of the BES backend
    ///
    ///   remote_header=[NAME]=[VALUE]
    ///
    ///     Remote headers (can repeat to add multiple)
    ///
    ///   async=[true|false]
    ///
    ///     Handle responses asynchronously instead of blocking on them to send back
    ///     to the client. If the stream fails the client won't be notified. Defaults
    ///     to blocking behaviour (async=false).
    ///
    ///   tls_client_certificate=[PATH]
    ///
    ///     File path to a TLS PEM certificate used to identify the client to the backend.
    ///     Use this when the backend requires mTLS authentication.
    ///
    ///   tls_client_key
    ///
    ///     File path to a TLS PEM private key used to identify the client to the backend.
    ///     Use this when the backend requires mTLS authentication.
    ///
    ///   connect_timeout
    ///     
    ///     Max duration in seconds to connect to the backend before timing out. Deafults to no
    ///     timeout.
    ///
    ///   request_timeout
    ///     
    ///     Max duration in seconds for requests to the backend before timing out. Defaults to
    ///     no timeout.
    ///
    ///   request_buffer_size
    ///
    ///     The maximum number of requests that can be buffered waiting to send to this backend
    ///     before adding backpressure on incoming requests. Increase this on slower backends
    ///     that are bottlenecking other backends from receiving requests, at the cost of more
    ///     memory usage. Defaults to 500.
    #[arg(short, long = "bes_backend")]
    pub bes_backends: Vec<BesBackendArg>,

    /// Socket address for the server to listen on. Defaults to 0.0.0.0:9000.
    /// Alternatively, a unix domain socket, e.g., unix:/path/to/socket
    #[arg(short, long)]
    pub listen: Option<String>,

    /// File path to the server PEM private key for TLS.
    #[arg(long = "server_tls_certificate", requires = "server_tls_key")]
    pub server_tls_certificate: Option<PathBuf>,

    /// File path to the server PEM certificate for TLS.
    #[arg(long = "server_tls_key", requires = "server_tls_certificate")]
    pub server_tls_key: Option<PathBuf>,

    /// File path to a TLS PEM certificate that is trusted to sign server certificates.
    /// Can be repeated for multiple certificates.
    #[arg(long = "tls_certificate")]
    pub tls_certificate: Vec<PathBuf>,

    /// the number of concurrent inbound requests per connection. default unset.
    /// When used in combination with --load_shed_requests, requests will be rejected with
    /// a resource exhausted error instead of buffering when the concurrency limit
    /// is reached.
    #[arg(long = "concurrency_limit_per_connection")]
    pub concurrency_limit_per_connection: Option<usize>,

    /// Reject requests when the concurrency limit is reached. See
    /// --concurrency_limit_per_connection.
    #[arg(
        long = "load_shed_requests",
        default_value_t = false,
        requires = "concurrency_limit_per_connection"
    )]
    pub load_shed_requests: bool,

    /// Limit concurrent HTTP/2 streams per connection.
    /// Sets SETTINGS_MAX_CONCURRENT_STREAMS.
    #[arg(long = "max_concurrent_streams")]
    pub max_concurrent_streams: Option<u32>,

    /// The maximum duration in seconds that a connection may exist.
    #[arg(long = "max_connection_age")]
    pub max_connection_age: Option<u64>,

    /// The maximum duration in seconds that a connection may continue to exist after a graceful shutdown
    /// period. This takes effect after the duration in --max_connection_age.
    #[arg(long = "max_connection_age_grace")]
    pub max_connection_age_grace: Option<u64>,

    /// Path to configuration file
    #[arg(short, long)]
    pub config: Option<PathBuf>,
}

#[derive(Default, Clone, Debug)]
pub struct BesBackendArg {
    name: Option<String>,
    endpoint: String,
    remote_headers: MetadataMap,
    r#async: bool,
    tls_client_certificate: Option<PathBuf>,
    tls_client_key: Option<PathBuf>,
    connect_timeout: Option<u64>,
    request_timeout: Option<u64>,
    request_buffer_size: Option<usize>,
}

impl Args {
    pub fn validate(&self) -> Result<()> {
        // Unique bes backend names
        let mut backend_names: HashSet<&str> = HashSet::new();
        for backend in &self.bes_backends {
            if let Some(name) = backend.name.as_ref() {
                if !backend_names.contains(name.as_str()) {
                    backend_names.insert(name.as_str());
                } else {
                    anyhow::bail!("multiple bes backends have the same name {}", name);
                }
            }
        }

        Ok(())
    }
}

impl FromStr for BesBackendArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let args: Vec<&str> = s.split(",").collect();
        let mut arg_map = HashMap::<&str, Vec<&str>>::new();

        if args.len() > 1 || args.len() == 1 && args[0].contains("=") {
            arg_map = args.into_iter().try_fold(arg_map, |mut arg_map, token| {
                match token.split_once("=") {
                    Some((k, v)) => {
                        arg_map.entry(k).or_insert_with(Vec::new).push(v);
                        Ok(arg_map)
                    }
                    None => anyhow::bail!("invalid endpoint"),
                }
            })?;
        };

        let endpoint = if arg_map.len() > 0 {
            arg_map
                .remove("endpoint")
                .ok_or_else(|| anyhow::anyhow!("missing endpoint"))
                .and_then(|mut endpoints| {
                    if endpoints.len() != 1 {
                        Err(anyhow::anyhow!("multiple endpoints"))
                    } else {
                        Ok(endpoints.remove(0))
                    }
                })?
        } else {
            s
        }
        .to_string();

        let mut names: Vec<String> = arg_map
            .remove("name")
            .map(|names| names.into_iter().map(String::from).collect())
            .unwrap_or_default();
        if names.len() > 1 {
            return Err(anyhow::anyhow!("multiple names"));
        }
        let name = names.pop();

        let remote_headers = arg_map
            .remove("remote_header")
            .unwrap_or_default()
            .iter()
            .map(|header| match header.split_once("=") {
                Some((k, v)) => Ok((k.to_string(), v.to_string())),
                None => Err(anyhow::anyhow!("invalid remote header {header}")),
            })
            .try_fold(MetadataMap::new(), |mut metadata, kv| {
                let (k, v) = kv?;
                metadata.append(
                    MetadataKey::from_str(&k)
                        .map_err(|_| anyhow::anyhow!("invalid remote header key {k}"))?,
                    MetadataValue::from_str(&v)
                        .map_err(|_| anyhow::anyhow!("invalid remote header value {v}"))?,
                );
                Ok::<MetadataMap, anyhow::Error>(metadata)
            })?;

        let r#async: bool = arg_map
            .remove("async")
            .or(Some(Vec::new()))
            .and_then(|values| {
                if values.len() > 1 {
                    None
                } else if values.len() == 1 {
                    Some(values[0])
                } else {
                    Some("false")
                }
            })
            .ok_or_else(|| anyhow::anyhow!("multiple values for async"))
            .and_then(|r#async| {
                r#async
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid async value"))
            })?;

        let tls_client_certificate =
            if let Some(mut tls_client_certificate) = arg_map.remove("tls_client_certificate") {
                match tls_client_certificate.len() {
                    0 => None,
                    1 => Some(
                        PathBuf::from_str(tls_client_certificate.pop().unwrap())
                            .map_err(|_| anyhow::anyhow!("invalid tls client certificate path"))?,
                    ),
                    _ => anyhow::bail!("multiple tls client certificates"),
                }
            } else {
                None
            };

        let tls_client_key = if let Some(mut tls_client_key) = arg_map.remove("tls_client_key") {
            match tls_client_key.len() {
                0 => None,
                1 => Some(
                    PathBuf::from_str(tls_client_key.pop().unwrap())
                        .map_err(|_| anyhow::anyhow!("invalid tls client key path"))?,
                ),
                _ => anyhow::bail!("multiple tls client keys"),
            }
        } else {
            None
        };

        let connect_timeout = if let Some(mut connect_timeout) = arg_map.remove("connect_timeout") {
            match connect_timeout.len() {
                0 => None,
                1 => Some(
                    u64::from_str(connect_timeout.pop().unwrap())
                        .map_err(|_| anyhow::anyhow!("invalid connect_timeout"))?,
                ),
                _ => anyhow::bail!("multiple connect timeouts"),
            }
        } else {
            None
        };

        let request_timeout = if let Some(mut request_timeout) = arg_map.remove("request_timeout") {
            match request_timeout.len() {
                0 => None,
                1 => Some(
                    u64::from_str(request_timeout.pop().unwrap())
                        .map_err(|_| anyhow::anyhow!("invalid request_timeout"))?,
                ),
                _ => anyhow::bail!("multiple request timeouts"),
            }
        } else {
            None
        };

        let request_buffer_size =
            if let Some(mut request_buffer_size) = arg_map.remove("request_buffer_size") {
                match request_buffer_size.len() {
                    0 => None,
                    1 => Some(
                        usize::from_str(request_buffer_size.pop().unwrap())
                            .map_err(|_| anyhow::anyhow!("invalid request_buffer_size"))?,
                    ),
                    _ => anyhow::bail!("multiple request buffer sizes"),
                }
            } else {
                None
            };

        Ok(BesBackendArg {
            name,
            endpoint,
            remote_headers,
            r#async,
            tls_client_certificate,
            tls_client_key,
            connect_timeout,
            request_timeout,
            request_buffer_size,
        })
    }
}

impl TryInto<BesBackend> for BesBackendArg {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<BesBackend> {
        let mut backend = BesBackend::builder(self.endpoint)?
            .remote_headers(self.remote_headers)
            .r#async(self.r#async);

        if let Some(name) = self.name {
            backend = backend.name(name)
        }

        if let (Some(certificate), Some(key)) = (self.tls_client_certificate, self.tls_client_key) {
            backend = backend.tls_client_identity(certificate, key);
        }

        if let Some(timeout) = self.connect_timeout {
            backend = backend.connect_timeout(Duration::from_secs(timeout));
        }

        if let Some(timeout) = self.request_timeout {
            backend = backend.request_timeout(Duration::from_secs(timeout));
        }

        if let Some(request_buffer_size) = self.request_buffer_size {
            backend = backend.request_buffer_size(request_buffer_size);
        }

        Ok(backend.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bes_backend_arg_as_endpoint() {
        let result = "grpc://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let backend = result.unwrap();
        assert_eq!("grpc://127.0.0.1:6000", backend.endpoint,);
    }

    #[test]
    fn parse_bes_backend_with_properties() {
        let result = "name=foobar,endpoint=grpc://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let backend = result.unwrap();
        assert_eq!("grpc://127.0.0.1:6000", backend.endpoint);
    }

    #[test]
    fn parse_bes_backend_endpoint_multiple_values_fail() {
        let result = "endpoint=grpc://127.0.0.1:3000,endpoint=grpc://127.0.0.1:3001"
            .parse::<BesBackendArg>();

        assert!(result.is_err());
    }

    #[test]
    fn parse_bes_backend_no_name() {
        let result = "endpoint=grpc://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let backend = result.unwrap();
        assert!(backend.name.is_none());
    }

    #[test]
    fn parse_bes_backend_name_multiple_values_fail() {
        let result = "name=a,name=b,endpoint=grpc://127.0.0.1:3000".parse::<BesBackendArg>();

        assert!(result.is_err());
    }

    #[test]
    fn parse_bes_backend_remote_header() {
        let result = "endpoint=grpc://127.0.0.1:3000,remote_header=x-foobar-key=12345"
            .parse::<BesBackendArg>();

        assert!(result.is_ok());
        let headers = result.unwrap().remote_headers;
        assert_eq!(1, headers.len());
        assert_eq!("12345", headers.get("x-foobar-key").unwrap());
    }

    #[test]
    fn parse_bes_backend_remote_header_multiple() {
        let result =
            "endpoint=grpc://127.0.0.1:3000,remote_header=x-foobar-key=12345,remote_header=moo=cow"
                .parse::<BesBackendArg>();

        assert!(result.is_ok());
        let headers = result.unwrap().remote_headers;
        assert_eq!(2, headers.len());
        assert_eq!("12345", headers.get("x-foobar-key").unwrap());
        assert_eq!("cow", headers.get("moo").unwrap());
    }

    #[test]
    fn parse_bes_backend_async() {
        let result = "endpoint=grpc://127.0.0.1:3000,async=true".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let backend = result.unwrap();
        assert!(backend.r#async);
    }

    #[test]
    fn parse_bes_backend_tls_client_certificate() {
        let result =
            "endpoint=grpc://127.0.0.1:3000,tls_client_certificate=/cert".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let arg = result.unwrap();
        let cert = arg.tls_client_certificate;
        assert!(cert.is_some());
        assert_eq!("/cert", cert.unwrap().to_string_lossy());
    }

    #[test]
    fn parse_bes_backend_tls_client_key() {
        let result = "endpoint=grpc://127.0.0.1:3000,tls_client_key=/key".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let arg = result.unwrap();
        let key = arg.tls_client_key;
        assert!(key.is_some());
        assert_eq!("/key", key.unwrap().to_string_lossy());
    }

    #[test]
    fn parse_bes_backend_connect_timeout() {
        let result = "endpoint=grpc://127.0.0.1:3000,connect_timeout=15".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let arg = result.unwrap();
        let connect_timeout = arg.connect_timeout;
        assert!(connect_timeout.is_some());
        assert_eq!(15, connect_timeout.unwrap());
    }

    #[test]
    fn parse_bes_backend_request_timeout() {
        let result = "endpoint=grpc://127.0.0.1:3000,request_timeout=10".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let arg = result.unwrap();
        let request_timeout = arg.request_timeout;
        assert!(request_timeout.is_some());
        assert_eq!(10, request_timeout.unwrap());
    }

    #[test]
    fn parse_bes_backend_request_buffer_size() {
        let result =
            "endpoint=grpc://127.0.0.1:3000,request_buffer_size=2000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let arg = result.unwrap();
        let request_buffer_size = arg.request_buffer_size;
        assert!(request_buffer_size.is_some());
        assert_eq!(2000, request_buffer_size.unwrap());
    }

    #[test]
    fn validate_duplicate_bes_backend_names() {
        let args = Args {
            bes_backends: vec![
                BesBackendArg {
                    name: Some("foobar".to_string()),
                    ..Default::default()
                },
                BesBackendArg {
                    name: Some("foobar".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(args.validate().is_err());
    }

    #[test]
    fn validate_different_bes_backend_names() {
        let args = Args {
            bes_backends: vec![
                BesBackendArg {
                    name: Some("foobar".to_string()),
                    ..Default::default()
                },
                BesBackendArg {
                    name: Some("moocow".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(args.validate().is_ok());
    }
}
