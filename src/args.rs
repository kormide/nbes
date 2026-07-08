use clap::{Error, Parser, error::ErrorKind};
use nbes::forwarding::BesBackend;
use rand::{RngExt, distr::Alphabetic};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
};
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
    /// Multiple backends can be configured by repeating the argument.
    #[arg(long = "bes_backend")]
    pub bes_backends: Vec<BesBackendArg>,

    /// Socket address for the server to listen on. Defaults to 0.0.0.0:9000.
    #[arg(short, long, default_value_t = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 9000)))]
    pub listen: SocketAddr,

    /// Unix domain socket for the server to listen on. Can be used instead of --listen.
    /// E.g. --socket unix:/path/to/socket
    #[arg(short, long, conflicts_with = "listen", value_parser = socket_parser)]
    pub socket: Option<Url>,
}

#[derive(Clone, Debug)]
pub struct BesBackendArg {
    name: String,
    endpoint: Url,
}

fn socket_parser(socket: &str) -> std::result::Result<Url, String> {
    let url = Url::parse(socket).map_err(|e| format!("{e}"))?;
    if url.scheme() != "unix" {
        return Err(String::from(
            "socket has incorrect url scheme; expected to start with unix:/",
        ));
    }

    Ok(url)
}

impl FromStr for BesBackendArg {
    type Err = clap::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let args: Vec<&str> = s.split(",").collect();
        let mut arg_map = HashMap::<&str, &str>::new();

        if args.len() > 1 || args.len() == 1 && args[0].contains("=") {
            arg_map = args.into_iter().try_fold(arg_map, |mut arg_map, token| {
                let kv: Vec<&str> = token.split("=").collect();
                if kv.len() != 2 {
                    return Err(Error::new(ErrorKind::InvalidValue));
                }
                arg_map.insert(kv[0], kv[1]);
                Ok(arg_map)
            })?;
        };

        let mut endpoint = if arg_map.len() > 0 {
            arg_map
                .remove("endpoint")
                .ok_or_else(|| Error::new(ErrorKind::MissingRequiredArgument))?
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

        Ok(BesBackendArg {
            name: arg_map.remove("name").map(String::from).unwrap_or_else(|| {
                rand::rng()
                    .sample_iter(&Alphabetic)
                    .take(8)
                    .map(char::from)
                    .map(|c| c.to_ascii_lowercase())
                    .collect()
            }),
            endpoint,
        })
    }
}

impl Into<BesBackend> for BesBackendArg {
    fn into(self) -> BesBackend {
        BesBackend {
            name: self.name,
            endpoint: self.endpoint,
            client: None,
        }
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
}
