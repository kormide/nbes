# nbes

An in-memory BES backend that forwards streams to multiple backends.

This project is a workaround for Bazel not supporting multiple `--bes_backend` args: https://github.com/bazelbuild/bazel/issues/10908.

## Features

* Run locally on the same host as Bazel or as a hosted service accepting many invocations
* Configure different auth schemes (e.g., remote headers, mTLS) per backend
* Choose which backends to block on vs. upload asynchronously
* Server can terminate TLS
* Connect to nbes over a unix domain socket (locally)

## Quick start

Download the latest nbes from the [releases](https://github.com/kormide/nbes/releases).

Start nbes. In this example, a single bes backend is declared pointing to an unauthenticated buildbuddy endpoint.

```bash
nbes --listen 0.0.0.0:9000 --bes_backend=grpcs://remote.buildbuddy.io
```

Point Bazel to nbes.

```bash
bazel build //... --bes_backend=grpc://127.0.0.1:9000
```

For all options, run `nbes --help` or see the usage documentation below.

## Usage

All arguments can be specified as command-line args or in a configuration file. If both are specified, cli args will take priority, but backend specifications will be combined.

### CLI

| Arg                                | Default      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|------------------------------------|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --listen                           | 0.0.0.0:9000 | The socket address or unix domain socket path to listen on.<br><br>E.g., `0.0.0.0:9000` or `unix:/path/to/socket`.<br><br>When using a unix domain socket, invoke Bazel with both a `--bes_proxy` and `--bes_backend` arg, The backend arg can be any value and won't be used.<br><br>E.g., `bazel build //... --bes_proxy=unix:/path/to/socket --bes_backend=grpc://127.0.0.1`.                                                              |
| --config                           | None         | Path to a configuration file. All options passed via the CLI can instead be declared in a config file.                                                                                                                                                                                                                                                                                                                                        |
| --bes_backend                      | []           | Declare a BES backend to forward to. This argument may be repeated to declare multiple backends.<br><br>In the simplest form, this is just a url, e.g.<br><br>`--bes_backend grpcs://foo.backend.org`<br><br>To set more options, use the comma-separated value form, e.g.,<br><br>`--bes_backend name=buildbuddy,endpoint=grpcs://remote.buildbuddy.io`<br><br>See the full set of [backend options](#--bes_backend-options).<br><br>Support schemes: `grpc`, `grpcs`. |
| --server_tls_certificate           | None         | File path to the server PEM private key for TLS.                                                                                                                                                                                                                                                                                                                                                                                              |
| --server_tls_key                   | None         | File path to the server PEM certificate for TLS.                                                                                                                                                                                                                                                                                                                                                                                              |
| --tls_certificate                  | []           | File path to a TLS PEM certificate that is trusted to sign server certificates. Can be repeated for multiple certificates.                                                                                                                                                                                                                                                                                                                    |
| --concurrency_limit_per_connection | Unlimited    | The number of concurrent inbound requests per connection. When used in combination with `--load_shed_requests`, requests will be rejected with a resource exhausted error instead of buffering when the concurrency limit is reached.                                                                                                                                                                                                         |
| --load_shed_requests               | false        | Reject requests when the concurrency limit is reached. See --concurrency_limit_per_connection.                                                                                                                                                                                                                                                                                                                                                |
| --max_concurrent_streams           | Unlimited    | Limit concurrent HTTP/2 streams per connection. Sets SETTINGS_MAX_CONCURRENT_STREAMS.                                                                                                                                                                                                                                                                                                                                                         |
| --max_connection_age               | Unlimited    | The maximum duration in seconds that a connection may exist.                                                                                                                                                                                                                                                                                                                                                                                  |
| --max_connection_age_grace         | Unlimited    | The maximum duration in seconds that a connection may continue to exist after a graceful shutdown period. This takes effect after the duration in --max_connection_age.                                                                                                                                                                                                                                                                       |


#### --bes_backend options

Specified as csv key-value pairs in a `--bes_backend` argument. E.g.,

```bash
nbes --bes_backend=name=foo,endpoint=grpcs://foo.backend.org,async=true
```

| Name                   | Default                        | Description                                                                                                                                                                                                                                                                       |
|------------------------|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name                   | Auto-generated if not provided | Name of the BES backend. Used to uniquely identify it, appears in logs.                                                                                                                                                                                                           |
| endpoint               | None                           | Endpoint of the backend in the form `[SCHEME://]HOST[:PORT]`. E.g.,<br><br>`endpoint=grpcs://foo.backend.org`                                                                                                                                                                     |
| remote_header          | []                             | Remote header to send to the backend. May be repeated to declare multiple headers. E.g.,<br><br>`remote_header=x-foobar-api-key=abcd1234`                                                                                                                                         |
| async                  | false                          | Handle responses asynchronously instead of blocking on them to send back to the client. If the stream fails, the client won't be notified. Defaults to blocking behaviour (async=false).                                                                                          |
| tls_client_certificate | None                           | File path to a TLS PEM certificate used to identify the client to the backend. Use this when the backend requires mTLS authentication.                                                                                                                                            |
| tls_client_key         | None                           | File path to a TLS PEM private key used to identify the client to the backend. Use this when the backend requires mTLS authentication.                                                                                                                                            |
| connect_timeout        | Unlimited                      | Max duration in seconds to connect to the backend before timing out. Deafults to no timeout.                                                                                                                                                                                      |
| request_timeout        | Unlimited                      | Max duration in seconds for requests to the backend before timing out. Defaults to no timeout.                                                                                                                                                                                    |
| request_buffer_size    | 500                            | The maximum number of requests that can be buffered waiting to send to this backend before adding back pressure on incoming requests. Increase this on slower backends that bottleneck other backends from receiving requests, at the cost of more memory usage. Defaults to 500. |

### Config file

Load options from a config file.

```bash
nbes -c config.yaml
```

<details>
  <summary><b>Config reference</b></summary>

```yaml
server:
  # Address to listen on, optionally a unix domain socket, e.g., unix:/path/to/socket
  listen: <SOCKET_ADDR>
  # TLS configuration (optional)
  tls:
    certificate: <PATH_TO_CERT>
    key: <PATH_TO_KEY>

  # Number of concurrent inbound requests per connection (optional)
  concurrency_limit_per_connection: <NUMBER>

  # Reject requests when the concurrency limit is reached (optional)
  load_shed_requests: <BOOL>

  # Limit concurrent HTTP/2 streams per connection (optional)
  max_concurrent_streams: <NUMBER>

  # The maximum duration in seconds that a connection may exist (optional)
  max_connection_age: <NUMBER>

  # The maximum duration in seconds that a connection may continue to exist after a graceful shutdown (optional)
  max_connection_age_grace: <NUMBER>

# BES backends to forward to
bes_backends:
  # Can specify an endpoint [SCHEME://]HOST[:PORT]
  - <URL>
  # Or a full backend spec

    # Identifier for besbackend
  - name: <STRING> 
    # [SCHEME://]HOST[:PORT]
    endpoint: <URL>
    # Whether to handle responses asynchronously vs block to send back to the client (optional)
    async: <BOOL>
    # Remote headers to send to the backend (optional)
    remote_headers:
      <KEY>: <VALUE>
    # TLS cert to use for mTLS auth (optional)
    tls_client_certificate: <PATH>
    # TLS key to use for mTLS auth (optional)
    tls_client_key: <PATH>
    # Max duration in seconds to connect to the backend before timing out (optional)
    connect_timeout: <NUMBER>
    # Max duration in seconds for requests to the backend before timing out (optional)
    request_timeout: <NUMBER>
    # The maximum number of requests that can be buffered waiting to send to this
    # backend before adding backpressure on incoming requests (optional)
    request_buffer_size: <NUMBER>
```
</details>


## Deployment

The nbes service is released as a binary under [releases](https://github.com/kormide/nbes/releases) and as as an image. It can be deployed several ways.

<details>
  <summary><b>systemd</b></summary>

  TODO
</details>

<details>
  <summary><b>docker</b></summary>

Create a configuration file `config.yaml`. Then run:

```bash
docker container run -v ./config.yaml:/config.yaml -p 9000:9000 ghcr.io/kormide/nbes:latest
```

  TODO
</details>

<details>
  <summary><b>docker compose</b></summary>

compose.yaml
```yaml
services:
  nbes:
    image: ghcr.io/kormide/nbes:latest
    ports:
      - "9000:9000"
    volumes:
      - <PATH_TO_YOUR_CONFIG>:/config.yaml
    restart: always
```

Then run:

```bash
docker compose up -d
```

  TODO
</details>

<details>
  <summary><b>kubernetes</b></summary>

  TODO
</details>

## Compatibility

> [!NOTE]
> Only BuildBuddy has been tested at this point because they are the only vendor with a publicly facing BES backend. If you have confirmed that another vendor works, please create a pull request to update this table. If you are a vendor and can grant me access for testing, I can add your backend.

| BES Backend | Compatible         |
|-------------|--------------------|
| BuildBuddy  | :white_check_mark: |


