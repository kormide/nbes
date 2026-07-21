use clap::{Args as ClapArgs, Error, Parser, error::ErrorKind};
use nbes::forwarding::BesBackend;
use rand::{RngExt, distr::Alphabetic};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    str::FromStr,
};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use url::Url;

/// A BES backend that forwards to other BES backends
#[derive(Debug, Parser)]
#[command(version, about = "A BES backend that forwards to other BES backends", long_about = None)]
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
    /// Supported properties include:
    ///
    ///   name=[NAME]
    ///
    ///   Human-readable identifier for the bes backend, displayed in logs
    ///
    ///   endpoint=[SCHEME://]HOST[:PORT]
    ///
    ///   Endpoint of the BES backend
    ///
    ///   remote_header=[NAME]=[VALUE]
    ///
    ///   Remote headers (can repeat to add multiple)
    ///
    ///   async=[true|false]
    ///
    ///   Handle responses asynchronously instead of blocking on them to send back
    ///   to the client. If the stream fails the client won't be notified. Defaults
    ///   to blocking behaviour (async=false).
    ///
    /// Multiple backends can be configured by repeating the bes_backend argument.
    #[arg(long = "bes_backend")]
    pub bes_backends: Vec<BesBackendArg>,

    /// Socket address for the server to listen on. Defaults to 0.0.0.0:9000.
    #[arg(short, long, default_value_t = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 9000)))]
    pub listen: SocketAddr,

    /// Unix domain socket for the server to listen on. Can be used instead of --listen.
    /// E.g. --socket unix:/path/to/socket
    #[arg(short, long, conflicts_with = "listen", value_parser = socket_parser)]
    pub socket: Option<PathBuf>,

    #[command(flatten)]
    pub server_tls_config: ServerTlsArgs,

    /// File path to a TLS PEM certificate that is trusted to sign server certificates.
    /// Can be repeated for multiple certificates.
    #[arg(long)]
    pub tls_certificate: Vec<PathBuf>,
}

#[derive(ClapArgs, Debug)]
pub struct ServerTlsArgs {
    /// File path to the server PEM private key for TLS.
    #[arg(long, requires = "server_tls_private_key")]
    pub server_tls_certificate: Option<PathBuf>,
    /// File path to the server PEM certificate for TLS.
    #[arg(long, requires = "server_tls_certificate")]
    pub server_tls_private_key: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct BesBackendArg {
    name: String,
    endpoint: Url,
    remote_headers: MetadataMap,
    asynchronous: bool,
}

fn socket_parser(socket: &str) -> std::result::Result<PathBuf, String> {
    let url = Url::parse(socket).map_err(|e| format!("{e}"))?;
    if url.scheme() != "unix" {
        return Err(String::from(
            "socket has incorrect url scheme; expected to start with unix:/",
        ));
    }
    let path = url
        .to_file_path()
        .map_err(|_| String::from("socket is not a valid file path"))?;

    Ok(path)
}

impl FromStr for BesBackendArg {
    type Err = clap::Error;

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
                    None => Err(Error::new(ErrorKind::InvalidValue)),
                }
            })?;
        };

        let mut endpoint = if arg_map.len() > 0 {
            arg_map
                .remove("endpoint")
                .ok_or_else(|| Error::new(ErrorKind::MissingRequiredArgument))
                .and_then(|mut endpoints| {
                    if endpoints.len() != 1 {
                        Err(Error::new(ErrorKind::MissingRequiredArgument))
                    } else {
                        Ok(endpoints.remove(0))
                    }
                })?
        } else {
            s
        }
        .to_string();

        // A missing scheme is not a valid Url, so prepend the
        // default before parsing
        if !endpoint.contains("://") && !endpoint.contains("unix:/") {
            endpoint.insert_str(0, "grpcs://");
        }

        let endpoint = endpoint
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidValue))
            .and_then(|mut url: Url| {
                if !["grpc", "grpcs", "http", "https", "unix"].contains(&url.scheme()) {
                    return Err(Error::new(ErrorKind::InvalidValue));
                }
                if url.scheme() != "unix" {
                    if url.host().is_none() {
                        return Err(Error::new(ErrorKind::InvalidValue));
                    }
                    if url.port().is_none() {
                        url.set_port(Some(443))
                            .map_err(|_| Error::new(ErrorKind::InvalidValue))?;
                    }
                }

                Ok(url)
            })?;

        let mut names = arg_map
            .remove("name")
            .map(|names| names.into_iter().map(String::from).collect())
            .unwrap_or_else(|| {
                vec![
                    rand::rng()
                        .sample_iter(&Alphabetic)
                        .take(8)
                        .map(char::from)
                        .map(|c| c.to_ascii_lowercase())
                        .collect::<String>(),
                ]
            });
        if names.len() != 1 {
            return Err(Error::new(ErrorKind::InvalidValue));
        }
        let name = names.remove(0);

        let remote_headers = arg_map
            .remove("remote_header")
            .unwrap_or_default()
            .iter()
            .map(|header| match header.split_once("=") {
                Some((k, v)) => Ok((k.to_string(), v.to_string())),
                None => Err(Error::new(ErrorKind::InvalidValue)),
            })
            .try_fold(MetadataMap::new(), |mut metadata, kv| {
                let (k, v) = kv?;
                metadata.append(
                    MetadataKey::from_str(&k).map_err(|_| Error::new(ErrorKind::InvalidValue))?,
                    MetadataValue::from_str(&v).map_err(|_| Error::new(ErrorKind::InvalidValue))?,
                );
                Ok::<MetadataMap, Error>(metadata)
            })?;

        let asynchronous: bool = arg_map
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
            .ok_or_else(|| Error::new(ErrorKind::InvalidValue))
            .and_then(|asynchronous| {
                asynchronous
                    .parse()
                    .map_err(|_| Error::new(ErrorKind::InvalidValue))
            })?;

        Ok(BesBackendArg {
            name,
            endpoint,
            remote_headers,
            asynchronous,
        })
    }
}

impl Into<BesBackend> for BesBackendArg {
    fn into(self) -> BesBackend {
        let mut backend = BesBackend::new(self.name, self.endpoint, self.remote_headers);

        if self.asynchronous {
            backend.set_async(true);
        }

        backend
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use crate::args::BesBackendArg;

    #[test]
    fn parse_bes_backend_arg_as_endpoint() {
        let result = "grpc://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        assert_eq!(
            Url::parse("grpc://127.0.0.1:6000").unwrap(),
            result.unwrap().endpoint,
        );
    }

    #[test]
    fn parse_bes_backend_arg_as_endpoint_generates_name() {
        let result = "grpc://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        assert_eq!(result.unwrap().name.len(), 8);
    }

    #[test]
    fn parse_bes_backend() {
        let result = "name=foobar,endpoint=grpc://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let backend = result.unwrap();
        assert_eq!(
            Url::parse("grpc://127.0.0.1:6000").unwrap(),
            backend.endpoint,
        );
        assert_eq!("foobar", backend.name);
    }

    #[test]
    fn parse_bes_backend_generates_name() {
        let result = "endpoint=grpc://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        let backend = result.unwrap();
        assert_eq!(8, backend.name.len(),);
    }

    #[test]
    fn parse_bes_backend_arg_generated_name_is_random() {
        let b1 = "endpoint=grpc://127.0.0.1:6000"
            .parse::<BesBackendArg>()
            .unwrap();
        let b2 = "endpoint=grpc://127.0.0.1:6001"
            .parse::<BesBackendArg>()
            .unwrap();

        assert!(b1.name != b2.name);
    }

    #[test]
    fn parse_bes_backend_endpoint_invalid_scheme() {
        let result = "foo://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_err());
    }

    #[test]
    fn parse_bes_backend_endpoint_defaults_scheme() {
        let result = "127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        assert_eq!("grpcs", result.unwrap().endpoint.scheme());
    }

    #[test]
    fn parse_bes_backend_endpoint_defaults_port() {
        let result = "grpc://127.0.0.1".parse::<BesBackendArg>();

        assert!(result.is_ok());
        assert_eq!(443, result.unwrap().endpoint.port().unwrap());
    }

    #[test]
    fn parse_bes_backend_missing_host_fails() {
        let result = "grpc://".parse::<BesBackendArg>();

        assert!(result.is_err());
    }

    #[test]
    fn parse_bes_backend_unix_domain_socket() {
        let result = "unix:/tmp/socket".parse::<BesBackendArg>();

        assert!(result.is_ok());
        assert_eq!("unix", result.unwrap().endpoint.scheme());
    }

    #[test]
    fn parse_bes_backend_endpoint_multiple_values_fail() {
        let result = "endpoint=grpc://127.0.0.1:3000,endpoint=grpc://127.0.0.1:3001"
            .parse::<BesBackendArg>();

        assert!(result.is_err());
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
}
