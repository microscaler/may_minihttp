//! A connection that served a streamed (chunked / SSE) response must end when the client goes away.
//!
//! The connection loop used to decide whether to wait for the next request from a readiness check made
//! before the service was called. A streamed response runs inside `call` for as long as the stream lasts,
//! so a client that closed (or half-closed) during the stream had already signalled before the loop
//! waited, and the connection coroutine then waited forever on a socket nobody would use again: its
//! stack, buffers and file descriptor stayed allocated for the life of the process.
//!
//! Each case opens connections, finishes or abandons them as a client would, then requires that every
//! server-side connection has ended. The server's per-connection service counts itself in and out.
//!
//! Run with:
//!     cargo test --test streamed_response_close -- --test-threads=1

use may_minihttp::{HttpService, HttpServiceFactory, Request, Response};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Connections each case opens.
const CONNECTIONS: usize = 20;

struct Svc {
    live: Arc<AtomicUsize>,
}

impl Drop for Svc {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl HttpService for Svc {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        let path = req.path().to_string();
        if !path.starts_with("/stream") {
            rsp.body("ok");
            return Ok(());
        }
        // /stream?ms=N&n=K: K chunks N ms apart, then a clean end of the stream.
        let param = |key: &str, default: u64| {
            path.split(['?', '&'])
                .find_map(|p| p.strip_prefix(&format!("{key}=")))
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let (ms, chunks) = (param("ms", 20), param("n", 3));
        let stream = req.body().into_stream();
        rsp.header("Content-Type: text/event-stream");
        let mut writer = rsp.begin_chunked_stream(stream)?;
        for i in 0..chunks {
            if writer
                .write_chunk(format!("data: {i}\n\n").as_bytes())
                .is_err()
            {
                break;
            }
            may::coroutine::sleep(Duration::from_millis(ms));
        }
        let _ = writer.finish();
        Ok(())
    }
}

struct Factory {
    live: Arc<AtomicUsize>,
}

impl HttpServiceFactory for Factory {
    type Service = Svc;
    fn new_service(&self, _id: usize) -> Svc {
        self.live.fetch_add(1, Ordering::SeqCst);
        Svc {
            live: Arc::clone(&self.live),
        }
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Starts a server and returns its port and live-connection count.
fn server() -> (u16, Arc<AtomicUsize>) {
    let live = Arc::new(AtomicUsize::new(0));
    let port = free_port();
    let _handle = Factory {
        live: Arc::clone(&live),
    }
    .start(("127.0.0.1", port))
    .unwrap();
    let started = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "server did not start"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // The probe connection above counts too: let it end before a case starts.
    wait_for_no_connections(&live, "probe");
    (port, live)
}

fn request(port: u16, target: &str) -> TcpStream {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    write!(
        s,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"
    )
    .unwrap();
    s
}

/// Reads until the chunked terminator; false on EOF or timeout first.
fn read_to_end_of_stream(s: &mut TcpStream) -> bool {
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) | Err(_) => return false,
            Ok(k) => {
                got.extend_from_slice(&buf[..k]);
                if got.windows(5).any(|w| w == b"0\r\n\r\n") {
                    return true;
                }
            }
        }
    }
}

/// Every server-side connection must end within a few seconds of its client leaving.
fn wait_for_no_connections(live: &AtomicUsize, case: &str) {
    let started = Instant::now();
    while live.load(Ordering::SeqCst) != 0 {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{case}: {} server connection(s) still open after their clients left",
            live.load(Ordering::SeqCst)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_stream_that_ends_cleanly_then_the_client_closes() {
    let (port, live) = server();
    for _ in 0..CONNECTIONS {
        let mut s = request(port, "/stream?ms=10&n=3");
        assert!(
            read_to_end_of_stream(&mut s),
            "the stream ends with its terminator"
        );
        drop(s);
    }
    wait_for_no_connections(&live, "clean end");
}

#[test]
fn the_client_closes_in_the_middle_of_a_stream() {
    let (port, live) = server();
    for _ in 0..CONNECTIONS {
        let mut s = request(port, "/stream?ms=50&n=20");
        let mut buf = [0u8; 256];
        assert!(s.read(&mut buf).unwrap() > 0, "the stream started");
        drop(s);
    }
    wait_for_no_connections(&live, "client closed mid-stream");
}

#[test]
fn the_client_half_closes_during_a_stream_and_reads_to_its_end() {
    let (port, live) = server();
    for _ in 0..CONNECTIONS {
        let mut s = request(port, "/stream?ms=30&n=6");
        let mut buf = [0u8; 256];
        assert!(s.read(&mut buf).unwrap() > 0, "the stream started");
        s.shutdown(Shutdown::Write).unwrap();
        assert!(
            read_to_end_of_stream(&mut s),
            "the stream still ends with its terminator"
        );
        drop(s);
    }
    wait_for_no_connections(&live, "client half-closed mid-stream");
}

#[test]
fn a_plain_response_then_the_client_closes() {
    let (port, live) = server();
    for _ in 0..CONNECTIONS {
        let mut s = request(port, "/plain");
        let mut buf = [0u8; 256];
        assert!(s.read(&mut buf).unwrap() > 0);
        drop(s);
    }
    wait_for_no_connections(&live, "plain response");
}

#[test]
fn a_stream_then_another_request_on_the_same_connection() {
    // Keep-alive still works after a streamed response: the loop reads the next request.
    let (port, live) = server();
    let mut s = request(port, "/stream?ms=5&n=2");
    assert!(read_to_end_of_stream(&mut s));
    write!(s, "GET /plain HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
    let mut buf = [0u8; 512];
    let k = s.read(&mut buf).unwrap();
    assert!(
        String::from_utf8_lossy(&buf[..k]).starts_with("HTTP/1.1 200"),
        "second request on the connection answered"
    );
    drop(s);
    wait_for_no_connections(&live, "keep-alive after a stream");
}
