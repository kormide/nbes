use std::{collections::HashMap, str::FromStr};

use clap::{Error, Parser, error::ErrorKind};
use rand::{RngExt, distr::Alphabetic};
use url::Url;

/// A BES backend that forwards to other BES backends
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// A BES backend to forward events to. In the simplest form, this
    /// can be a grpc endpoint. E.g.,
    ///
    /// --bes_backend=grpc://<ENDPOINT>:<PORT>
    ///
    /// For additional properties, use the comma-separated value form:
    ///
    /// --bes_backend=name=my-bes-service,endpoint=grpc://<ENDPOINT>:<PORT>
    ///
    /// Multiple backends can be configured by repeating the argument.
    #[arg(long = "bes_backend")]
    bes_backend: Option<Vec<BesBackendArg>>,
}

#[derive(Clone, Debug)]
#[allow(unused)]
struct BesBackendArg {
    name: String,
    endpoint: Url,
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

        let endpoint = if arg_map.len() > 0 {
            arg_map
                .remove("endpoint")
                .ok_or_else(|| Error::new(ErrorKind::MissingRequiredArgument))?
        } else {
            s
        }
        .parse()
        .map_err(|_| Error::new(ErrorKind::InvalidValue))
        .and_then(|url: Url| {
            if !["grpc", "grpcs"].contains(&url.scheme()) {
                Err(Error::new(ErrorKind::InvalidValue))
            } else {
                Ok(url)
            }
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
    fn parse_bes_backend_arg_as_endpoint_invalid_url() {
        let result = "foobar".parse::<BesBackendArg>();

        assert!(result.is_err());
    }

    #[test]
    fn parse_bes_backend_arg_as_endpoint_invalid_scheme() {
        let result = "foo://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_err());
    }

    #[test]
    fn parse_bes_backend_arg_as_endpoint_generates_name() {
        let result = "grpc://127.0.0.1:6000".parse::<BesBackendArg>();

        assert!(result.is_ok());
        assert_eq!(result.unwrap().name.len(), 8);
    }

    #[test]
    fn parse_bes_backend_arg() {
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
    fn parse_bes_backend_arg_generates_name() {
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
}
