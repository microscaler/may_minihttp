# may_minihttp

Mini http server that implemented on top of [may](https://github.com/Xudong-Huang/may)

This crate is ported from [tokio_minihttp](https://github.com/tokio-rs/tokio-minihttp).
But with much ease of use, you can call `MAY` block APIs directly in your service.

[![Build Status](https://github.com/Xudong-Huang/may_minihttp/workflows/CI/badge.svg)](https://github.com/Xudong-Huang/may_minihttp/actions?query=workflow%3ACI+branch%3Amaster)
[![Crate](https://img.shields.io/crates/v/may_minihttp.svg)](https://crates.io/crates/may_minihttp)

## Usage

First, add this to your `Cargo.toml`:

```toml
[dependencies]
may_minihttp = "0.1"
```

Then just simply implement your http service

```rust,no_run
extern crate may_minihttp;

use std::io;
use may_minihttp::{HttpServer, HttpService, Request, Response};

#[derive(Clone)]
struct HelloWorld;

impl HttpService for HelloWorld {
    fn call(&mut self, _req: Request, res: &mut Response) -> io::Result<()> {
        res.body("Hello, world!");
        Ok(())
    }
}

// Start the server in `main`.
fn main() {
    let server = HttpServer(HelloWorld).start("0.0.0.0:8080").unwrap();
    server.join().unwrap();
}
```

### Native HTTP/HTTPS client

Enable the `client` feature to use the coroutine-native HTTP/1.1 client. Absolute `http://` URLs
use a plain `may::net::TcpStream`; absolute `https://` URLs use rustls with the platform certificate
verifier and the ring crypto provider.

```toml
[dependencies]
may_minihttp = { version = "0.1", features = ["client"] }
```

```rust,no_run
use may_minihttp::client::HttpClient;

let mut client = HttpClient::from_url("https://identity.example.com")?;
let response = client.get("/health".parse()?)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Use `HttpClient::from_url_with_tls_config` when a private CA or mTLS client configuration is
required. `HttpClient` is the low-level, single-connection streaming API.

For requests across multiple origins, use the cloneable pooled `Client`. Its default `send` API
buffers responses up to a configured limit; `send_streaming` holds the exclusive pool lease until
EOF and discards the connection on an early drop. Pool limits, header/body limits, and deadlines are
finite; redirects are disabled unless explicitly enabled.

```rust,no_run
use may_minihttp::client::{Client, RedirectPolicy};

let client = Client::builder()
    .redirect_policy(RedirectPolicy::SameOrigin { max_hops: 5 })
    .build()?;
let response = client.get("https://identity.example.com/health")?.send()?;
assert!(response.status().is_success());
# Ok::<(), Box<dyn std::error::Error>>(())
```

The rich client also provides optional JSON helpers, bounded multipart preloading, single-use
request readers, typed error classification, operational pool counters, and injectable DNS
resolution. Multipart filesystem helpers are deliberately named `blocking_*` and must be called
outside latency-sensitive may coroutines. Automatic stale-socket retry is limited to one attempt for
idempotent requests with replayable bodies.

The normal client graph uses `may`, rustls, and the ring provider selected through rustls. It does
not include reqwest, Tokio, Hyper, or AWS-LC. Proxy discovery and HTTP/2 are not implicit.

## Performance
Tested with only one working thread on my laptop

Both with the following command to start the server.
```
$ cargo run --example=hello-world --release
```

**tokio_minihttp**
```sh
$ wrk http://127.0.0.1:8080 -d 10 -t 1 -c 200
Running 10s test @ http://127.0.0.1:8080
  1 threads and 200 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.98ms  284.06us  12.53ms   98.92%
    Req/Sec   101.64k     1.76k  103.69k    91.00%
  1011679 requests in 10.05s, 99.38MB read
Requests/sec: 100650.94
Transfer/sec:      9.89MB
```

**may_minihttp**
```sh
$ wrk http://127.0.0.1:8080 -d 10 -t 1 -c 200
Running 10s test @ http://127.0.0.1:8080
  1 threads and 200 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.70ms  812.42us  20.17ms   97.94%
    Req/Sec   117.65k     7.52k  123.40k    88.00%
  1171118 requests in 10.08s, 115.04MB read
Requests/sec: 116181.73
Transfer/sec:     11.41MB
```

## Benchmarks

One of the fastest web frameworks available according to the [TechEmpower Framework Benchmark](https://www.techempower.com/benchmarks/#section=data-r22&test=composite&hw=ph).

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.
