//! Load-test target for the server: a handful of fixed routes, an allocation counter, nothing else.
//!
//! ```text
//! cargo run --release --example bench_server -- 127.0.0.1:18080 [workers]
//! oha -z 10s -c 64 http://127.0.0.1:18080/
//! curl -s http://127.0.0.1:18080/__allocs     # allocations and requests served so far
//! ```
//!
//! Routes:
//! - `GET /`        13-byte static body (no body allocation)
//! - `GET /json`    1 KiB body built per request (`body_vec`)
//! - `GET /big`     1 MiB static body
//! - `POST /echo`   echoes the request body
//! - `GET /sse`     1000 chunked-encoding chunks of 24 bytes each (an SSE-like stream)
//! - `GET /__allocs` `allocs=<n> bytes=<n> requests=<n>` since start
//!
//! It serves with 32 header slots, as BRRTRouter does. The allocation counter wraps the system
//! allocator, so `allocs` divided by `requests` between two samples is allocations per request,
//! server side.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{self, Read};
use std::sync::atomic::{AtomicU64, Ordering};

use may_minihttp::{HttpServerWithHeaders, HttpService, Request, Response};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static REQUESTS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

static BIG: [u8; 1 << 20] = [b'x'; 1 << 20];

#[derive(Clone)]
struct Bench;

impl HttpService for Bench {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        let path = req.path();
        if path == "/__allocs" {
            let body = format!(
                "allocs={} bytes={} requests={}\n",
                ALLOCS.load(Ordering::Relaxed),
                BYTES.load(Ordering::Relaxed),
                REQUESTS.load(Ordering::Relaxed)
            );
            rsp.header("Content-Type: text/plain");
            rsp.body_vec(body.into_bytes());
            return Ok(());
        }
        REQUESTS.fetch_add(1, Ordering::Relaxed);
        match (req.method(), path) {
            ("GET", "/") => {
                rsp.header("Content-Type: text/plain");
                rsp.body("Hello, World!");
            }
            ("GET", "/json") => {
                rsp.header("Content-Type: application/json");
                let mut body = Vec::with_capacity(1024);
                body.extend_from_slice(b"{\"data\":\"");
                body.resize(1022, b'a');
                body.extend_from_slice(b"\"}");
                rsp.body_vec(body);
            }
            ("GET", "/big") => {
                rsp.header("Content-Type: application/octet-stream");
                // SAFETY of 'static: BIG is a static.
                let s: &'static str = std::str::from_utf8(&BIG).expect("ascii");
                rsp.body(s);
            }
            ("GET", "/sse") => {
                let stream = req.body().into_stream();
                let mut events = rsp.begin_chunked_stream(stream)?;
                for _ in 0..1000 {
                    events.write_chunk(b"data: {\"tick\":12345}\n\n\n")?;
                }
                events.finish()?;
            }
            ("POST", "/echo") => {
                let mut body = Vec::new();
                req.body().read_to_end(&mut body)?;
                rsp.header("Content-Type: application/octet-stream");
                rsp.body_vec(body);
            }
            _ => {
                rsp.status_code(404, "Not Found");
            }
        }
        Ok(())
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:18080".to_string());
    let workers: usize = args
        .next()
        .and_then(|w| w.parse().ok())
        .unwrap_or_else(num_cpus::get);
    may::config().set_workers(workers).set_stack_size(0x8000);
    eprintln!("bench_server on {addr} with {workers} may workers");
    let server = HttpServerWithHeaders::<_, 32>(Bench)
        .start(&addr)
        .expect("bind");
    server.join().expect("server");
}
