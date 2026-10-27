//! Client-side load harness: the same request three ways, with latency percentiles and allocations
//! per request.
//!
//! ```text
//! cargo run --release --example bench_client --features client -- \
//!     <mode> <url> [concurrency] [requests-per-worker] [header-bytes] [body-bytes]
//! ```
//!
//! Modes:
//! - `fresh` a new `HttpClient` (new TCP connection) per request; what a caller that builds a client
//!   per call pays
//! - `reuse` one `HttpClient` per worker, kept alive across its requests
//! - `pool`  one shared pooled [`Client`], `max_connections_per_origin = concurrency`
//! - `pool-hook` the same pool with an interceptor whose hooks are empty but which does not opt
//!   out (`Interceptor::ENABLED` left true): the cost of building the hook arguments, against
//!   `pool` (no interceptor)
//!
//! `header-bytes` adds an `X-Pad` header of that size (a bearer token with claims is 1-6 KiB);
//! `body-bytes > 0` makes the request a POST with a body of that size (`/echo` on bench_server).
//! Workers are may coroutines. Pair with `examples/bench_server.rs`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{self, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::{HeaderName, HeaderValue, Method, Uri};
use may_minihttp::client::{Client, HttpClient, Interceptor};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Fresh,
    Reuse,
    Pool,
    PoolHook,
}

struct Job {
    url: String,
    path: Uri,
    pad: Option<HeaderValue>,
    body: Option<Vec<u8>>,
}

impl Job {
    fn method(&self) -> Method {
        if self.body.is_some() {
            Method::POST
        } else {
            Method::GET
        }
    }

    /// One request on `client`; returns the body length.
    fn on(&self, client: &mut HttpClient) -> io::Result<usize> {
        let mut req = client.new_request(self.method(), self.path.clone());
        if let Some(pad) = &self.pad {
            req.headers_mut()
                .insert(HeaderName::from_static("x-pad"), pad.clone());
        }
        if let Some(body) = &self.body {
            req.send(body)?;
        }
        let mut rsp = client.send_request(req)?;
        let mut sink = Vec::new();
        rsp.read_to_end(&mut sink)?;
        if !rsp.status().is_success() {
            return Err(io::Error::other(format!("status {}", rsp.status())));
        }
        Ok(sink.len())
    }

    fn pooled<I: Interceptor>(&self, client: &Client<I>) -> io::Result<usize> {
        let mut builder = client.request(self.method(), &self.url)?;
        if let Some(pad) = &self.pad {
            builder = builder.header(HeaderName::from_static("x-pad"), pad.clone());
        }
        if let Some(body) = &self.body {
            builder = builder.body(body.clone());
        }
        let rsp = builder.send()?;
        if !rsp.status().is_success() {
            return Err(io::Error::other(format!("status {}", rsp.status())));
        }
        Ok(rsp.body().len())
    }
}

/// Empty hooks, still called: measures the interceptor plumbing itself.
#[derive(Clone)]
struct EmptyHooks;

impl Interceptor for EmptyHooks {}

fn arg<T: std::str::FromStr>(value: Option<String>, default: T) -> T {
    value.and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = match args.next().as_deref() {
        Some("fresh") => Mode::Fresh,
        Some("reuse") => Mode::Reuse,
        Some("pool") => Mode::Pool,
        Some("pool-hook") => Mode::PoolHook,
        _ => {
            eprintln!(
                "usage: bench_client fresh|reuse|pool|pool-hook <url> [c] [n] [header-bytes] [body-bytes]"
            );
            std::process::exit(2);
        }
    };
    let url = args
        .next()
        .unwrap_or_else(|| "http://127.0.0.1:18080/".into());
    let concurrency: usize = arg(args.next(), 16);
    let per_worker: usize = arg(args.next(), 2000);
    let header_bytes: usize = arg(args.next(), 0);
    let body_bytes: usize = arg(args.next(), 0);

    may::config().set_stack_size(0x8000);
    let parsed: Uri = url.parse().map_err(io::Error::other)?;
    let path: Uri = parsed
        .path_and_query()
        .map_or("/", |p| p.as_str())
        .parse()
        .map_err(io::Error::other)?;
    let job = Arc::new(Job {
        url: url.clone(),
        path,
        pad: (header_bytes > 0)
            .then(|| HeaderValue::from_str(&"p".repeat(header_bytes)).expect("pad")),
        body: (body_bytes > 0).then(|| vec![b'b'; body_bytes]),
    });
    let pool = Arc::new(
        Client::builder()
            .max_connections(concurrency)
            .max_connections_per_origin(concurrency)
            .request_timeout(Duration::from_secs(30))
            .build()?,
    );
    let hooked = Arc::new(pool.with_interceptor(EmptyHooks));

    // warm-up: one request per worker's worth of connections, not measured
    {
        let mut c = HttpClient::from_url(&job.url)?;
        job.on(&mut c)?;
        job.pooled(&pool)?;
    }

    let latencies = Arc::new(Mutex::new(Vec::with_capacity(concurrency * per_worker)));
    let errors = Arc::new(AtomicU64::new(0));
    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let started = Instant::now();
    let workers: Vec<_> = (0..concurrency)
        .map(|_| {
            let job = Arc::clone(&job);
            let pool = Arc::clone(&pool);
            let hooked = Arc::clone(&hooked);
            let latencies = Arc::clone(&latencies);
            let errors = Arc::clone(&errors);
            may::go!(move || {
                let mut local = Vec::with_capacity(per_worker);
                let mut kept: Option<HttpClient> = None;
                for _ in 0..per_worker {
                    let t = Instant::now();
                    let result = match mode {
                        Mode::Fresh => {
                            HttpClient::from_url(&job.url).and_then(|mut c| job.on(&mut c))
                        }
                        Mode::Reuse => {
                            if kept.is_none() {
                                kept = HttpClient::from_url(&job.url).ok();
                            }
                            match kept.as_mut() {
                                Some(c) => job.on(c).inspect_err(|_| kept = None),
                                None => Err(io::Error::other("connect")),
                            }
                        }
                        Mode::Pool => job.pooled(&pool),
                        Mode::PoolHook => job.pooled(&hooked),
                    };
                    match result {
                        Ok(_) => local.push(t.elapsed()),
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                latencies.lock().unwrap().extend(local);
            })
        })
        .collect();
    for w in workers {
        w.join().expect("worker");
    }
    let elapsed = started.elapsed();
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;

    let mut lat = latencies.lock().unwrap().clone();
    lat.sort_unstable();
    let ok = lat.len();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() {
            return 0.0;
        }
        let i = ((lat.len() as f64 - 1.0) * p).round() as usize;
        lat[i].as_secs_f64() * 1e3
    };
    let stats = pool.stats();
    println!(
        "mode={} c={} n={} hdr={} body={} ok={} err={} rps={:.0} p50={:.3}ms p90={:.3}ms p99={:.3}ms max={:.3}ms allocs/req={:.1}{}",
        match mode {
            Mode::Fresh => "fresh",
            Mode::Reuse => "reuse",
            Mode::Pool => "pool",
            Mode::PoolHook => "pool-hook",
        },
        concurrency,
        per_worker,
        header_bytes,
        body_bytes,
        ok,
        errors.load(Ordering::Relaxed),
        ok as f64 / elapsed.as_secs_f64(),
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(1.0),
        allocs as f64 / (ok.max(1)) as f64,
        if matches!(mode, Mode::Pool | Mode::PoolHook) {
            format!(" pool={stats:?}")
        } else {
            String::new()
        }
    );
    Ok(())
}
