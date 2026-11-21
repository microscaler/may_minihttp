//! The ~40 ms Nagle / delayed-ACK stall (27 Oct 2026, PriceWhisperer pp: trader-bff -> Sesame `GET /identity/me`
//! p50 46-63 ms with Sesame's handler at ~5-10 ms).
//!
//! A request whose head is over the client's 4 KiB write buffer went out as two writes, as did any request with a
//! body (the head was flushed before the body was written). With Nagle on, the second write waits until the first is
//! acknowledged, and a peer that has nothing to send yet (it has not seen the whole request) delays that ACK - 40 ms on
//! Linux once the connection has left quick-ACK mode. The server here is a plain blocking std server without
//! TCP_NODELAY, like most peers; the client is the crate's, on one keep-alive connection.
//!
//! Run with:
//!     cargo test --test tcp_nodelay_stall --features client -- --nocapture

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

use http::header::{HeaderName, HeaderValue};
use http::Method;
use may_minihttp::client::HttpClient;

const WARMUP: usize = 4;
const SAMPLES: usize = 24;

/// A keep-alive server: reads each request head (and a Content-Length body), answers `ok` in one write.
fn serve() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        loop {
            // the head
            let end = loop {
                if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            };
            let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
            let len: usize = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
            while buf.len() < end + len {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            buf.drain(..end + len);
            if stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .is_err()
            {
                return;
            }
        }
    });
    (addr, handle)
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

/// Times `SAMPLES` requests on one connection after `WARMUP`; prints them and returns the median.
fn time_requests(label: &str, head_pad: usize, body: Option<&[u8]>) -> Duration {
    let (addr, server) = serve();
    let mut client = HttpClient::connect(&*addr).expect("connect");
    let pad = HeaderValue::from_str(&"a".repeat(head_pad.max(1))).unwrap();
    let mut times = Vec::with_capacity(SAMPLES);
    for i in 0..WARMUP + SAMPLES {
        let method = if body.is_some() {
            Method::POST
        } else {
            Method::GET
        };
        let mut req = client.new_request(method, "/x".parse().unwrap());
        req.headers_mut()
            .insert(HeaderName::from_static("x-pad"), pad.clone());
        let t0 = Instant::now();
        if let Some(b) = body {
            req.send(b).expect("send body");
        }
        let mut rsp = client.send_request(req).expect("request");
        let mut out = Vec::new();
        rsp.read_to_end(&mut out).expect("body");
        assert_eq!(out, b"ok");
        if i >= WARMUP {
            times.push(t0.elapsed());
        }
    }
    drop(client);
    let _ = server.join();
    let m = median(times.clone());
    let mut sorted = times;
    sorted.sort();
    eprintln!(
        "{label:<34} median {:>7.2} ms  p90 {:>7.2} ms  max {:>7.2} ms",
        m.as_secs_f64() * 1e3,
        sorted[sorted.len() * 9 / 10].as_secs_f64() * 1e3,
        sorted[sorted.len() - 1].as_secs_f64() * 1e3
    );
    m
}

/// The stall shows as a median near 40 ms; a request on loopback without it takes well under a millisecond.
const STALL: Duration = Duration::from_millis(15);

#[test]
fn small_get_head_does_not_stall() {
    let m = time_requests("GET, 0.2 KiB head (control)", 200, None);
    assert!(m < STALL, "GET with a small head: median {m:?}");
}

#[test]
fn large_get_head_does_not_stall() {
    // a bearer JWT with roles, permissions and authn claims, plus tracing headers: the head passes 4 KiB
    let m = time_requests("GET, 5 KiB head", 5 * 1024, None);
    assert!(
        m < STALL,
        "GET with a 5 KiB head: median {m:?} - the head went out in more than one write and waited on a delayed ACK"
    );
}

#[test]
fn post_with_small_body_does_not_stall() {
    let body = vec![b'{'; 600];
    let m = time_requests("POST, 1 KiB head + 0.6 KiB body", 1024, Some(&body));
    assert!(
        m < STALL,
        "POST with a small body: median {m:?} - the head and the body went out as separate writes"
    );
}
