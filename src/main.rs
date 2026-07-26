use anyhow::{Context, Result};
use args::Args;
use clap::Parser;
use log::{LevelFilter, info, warn};
use nbes::{Config, ServerTlsConfig, forwarding::BesBackend};
use std::{collections::HashMap, time::Duration};
use tokio::signal;

use crate::config_file::ConfigFile;

mod args;
mod config_file;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .format_target(false)
        .parse_env("NBES_LOG")
        .init();

    let args = Args::parse();
    args.validate().context("invalid args")?;

    info!("starting nbes {}", env!("CARGO_PKG_VERSION"),);

    let config = build_config(args)?;

    info!("listening on {}", config.listen);

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to listed for ctrl-c signal");
        info!("received ctrl-c");
    };

    nbes::run(config, ctrl_c).await?;

    Ok(())
}

/// Build an nbes configuration from command-line args and a configuration file.
fn build_config(args: Args) -> Result<Config> {
    let config_file = match args.config {
        Some(path) => Some(ConfigFile::parse(&path)?),
        None => None,
    }
    .unwrap_or_default();

    let config_backends: Vec<BesBackend> = config_file
        .bes_backends
        .into_iter()
        .map(|b| <config_file::BesBackend as TryInto<BesBackend>>::try_into(b))
        .collect::<Result<_, _>>()?;
    let args_backends: Vec<BesBackend> = args
        .bes_backends
        .into_iter()
        .map(|b| b.try_into())
        .collect::<Result<Vec<BesBackend>>>()?;
    let bes_backends = combine_backends_from_config_and_args(config_backends, args_backends);

    let server_tls_certificate = args
        .server_tls_certificate
        .or(config_file.server.tls.certificate);
    let server_tls_key = args.server_tls_key.or(config_file.server.tls.key);

    let server_tls_config = match (server_tls_certificate, server_tls_key) {
        (Some(certificate), Some(key)) => Some(ServerTlsConfig { certificate, key }),
        (Some(_), None) => anyhow::bail!(
            "server tls certificate provided but key is missing; provide the --server_tls_key arg or server.tls.key config option"
        ),
        (None, Some(_)) => anyhow::bail!(
            "server tls key provided but certificate is missing; provide the --server_tls_certificate arg or server.tls.certificate config option"
        ),
        (None, None) => None,
    };

    let config = Config {
        bes_backends,
        listen: args
            .listen
            .or(config_file.server.listen)
            .unwrap_or_else(|| String::from("0.0.0.0:9000"))
            .parse()
            .context(
                "failed to parse server binding (bad --listen arg or server.listen config option)",
            )?,
        server_tls_config,
        tls_certificates: args
            .tls_certificate
            .into_iter()
            .chain(config_file.tls_certificates.into_iter())
            .collect(),
        concurrency_limit_per_connection: args
            .concurrency_limit_per_connection
            .or(config_file.server.concurrency_limit_per_connection),
        load_shed_requests: args.load_shed_requests || config_file.server.load_shed_requests,
        max_concurrent_streams: args
            .max_concurrent_streams
            .or(config_file.server.max_concurrent_streams),
        max_connection_age: args
            .max_connection_age
            .or(config_file.server.max_connection_age)
            .map(Duration::from_secs),
        max_connection_age_grace: args
            .max_connection_age_grace
            .or(config_file.server.max_connection_age_grace)
            .map(Duration::from_secs),
    };

    Ok(config)
}

/// Combine bes backends declared in the config file and provided as arguments.
/// Backends in args that have the same name as a config backend take priority.
fn combine_backends_from_config_and_args(
    config_backends: Vec<BesBackend>,
    args_backends: Vec<BesBackend>,
) -> Vec<BesBackend> {
    let mut backends: HashMap<String, BesBackend> = config_backends
        .into_iter()
        .map(|b| (b.name.clone(), b))
        .collect();

    for backend in args_backends {
        if let Some(existing) = backends.get_mut(&backend.name) {
            warn!(
                "bes backend {} in config file will be overridden by backend specifieid in arg with the same name",
                backend.name
            );
            *existing = backend;
        } else {
            backends.insert(backend.name.clone(), backend);
        }
    }

    backends.into_values().collect()
}
