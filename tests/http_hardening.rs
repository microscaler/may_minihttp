//! HTTP/1.1 edge cases an internet-facing server must get right: request framing (request
//! smuggling), header limits, unread bodies, chunked request bodies, and response-side safety.
//!
//! Every test talks raw bytes over a std `TcpStream`, so what is asserted is exactly what is on the
//! wire.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use may_minihttp::{HttpServer, HttpService, Request, Response};

#[derive(Clone)]
struct Service {
    seen: Arc<Mutex<Vec<String>>>,
}

impl HttpService for Service {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        let path = req.path().to_string();
        self.seen.lock().unwrap().push(path.clone());
        match path.as_str() {
            "/echo" => {
                let mut body = Vec::new();
                req.body().read_to_end(&mut body)?;
                rsp.body_vec(body);
            }
            "/many-headers" => {
                for i in 0..20 {
                    rsp.header(format!("X-H{i}: {i}"));
                }
                rsp.body("many");
            }
            "/split" => {
                rsp.header(String::from("X-Ok: 1"));
                rsp.header(String::from("X-Bad: a\r\nSet-Cookie: injected=1"));
                rsp.body("split");
            }
            "/fail" => {
                return Err(io::Error::other(
                    "secret upstream db-7.internal:5432 refused",
                ));
            }
            // "/ignore" and anything else: never touch the body
            _ => rsp.body("ignored"),
        }
        Ok(())
    }
}

struct Server {
    port: u16,
    seen: Arc<Mutex<Vec<String>>>,
}

fn server() -> &'static Server {
    static SERVER: OnceLock<Server> = OnceLock::new();
    SERVER.get_or_init(|| {
        may::config().set_stack_size(0x8000);
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handle = HttpServer(Service { seen: seen.clone() })
            .start(("127.0.0.1", port))
            .expect("start server");
        std::mem::forget(handle);
        for _ in 0..100 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Server { port, seen }
    })
}

fn connect() -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", server().port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
}

/// One parsed response off the wire.
#[derive(Debug)]
struct Wire {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Wire {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Read one Content-Length-framed response; `None` if the connection closed first.
fn read_response(stream: &mut TcpStream, pending: &mut Vec<u8>) -> Option<Wire> {
    let mut chunk = [0_u8; 4096];
    let head_end = loop {
        if let Some(i) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => pending.extend_from_slice(&chunk[..n]),
        }
    };
    let head = String::from_utf8(pending[..head_end].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers: Vec<(String, String)> = lines
        .map(|l| {
            let (k, v) = l.split_once(':').unwrap();
            (k.trim().to_string(), v.trim().to_string())
        })
        .collect();
    let len: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, v)| v.parse().unwrap());
    pending.drain(..head_end + 4);
    while pending.len() < len {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => pending.extend_from_slice(&chunk[..n]),
        }
    }
    let body = pending.drain(..len).collect();
    Some(Wire {
        status,
        headers,
        body,
    })
}

/// Send `raw` on a fresh connection; return the responses until the server closes or goes quiet,
/// and whether it closed.
fn exchange(raw: &[u8]) -> (Vec<Wire>, bool) {
    let mut stream = connect();
    stream.write_all(raw).unwrap();
    let mut pending = Vec::new();
    let mut responses = Vec::new();
    while let Some(rsp) = read_response(&mut stream, &mut pending) {
        responses.push(rsp);
    }
    // read_response returned None: EOF (closed) or timeout (still open)
    let mut probe = [0_u8; 1];
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let closed = matches!(stream.read(&mut probe), Ok(0))
        || matches!(&responses.last(), Some(r) if r.header("connection") == Some("close"));
    (responses, closed)
}

fn seen(path: &str) -> bool {
    server().seen.lock().unwrap().iter().any(|p| p == path)
}

// ---------------------------------------------------------------------------------------------
// Framing: Content-Length and Transfer-Encoding
// ---------------------------------------------------------------------------------------------

#[test]
fn content_length_and_transfer_encoding_together_is_refused() {
    // The classic CL.TE / TE.CL smuggling shape: a front end and this server could disagree about
    // where the body ends. Refused before the service runs.
    let (rsp, closed) = exchange(
        b"POST /te-cl HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n\
          5c\r\nGPOST /te-cl-smuggled HTTP/1.1\r\n\r\n0\r\n\r\n",
    );
    assert_eq!(rsp.len(), 1);
    assert_eq!(rsp[0].status, 400);
    assert_eq!(rsp[0].header("connection"), Some("close"));
    assert!(closed);
    assert!(!seen("/te-cl"));
    assert!(!seen("/te-cl-smuggled"));
}

#[test]
fn conflicting_content_lengths_are_refused() {
    let (rsp, closed) = exchange(
        b"POST /cl-cl HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\nhello!",
    );
    assert_eq!(rsp[0].status, 400);
    assert!(closed);
    assert!(!seen("/cl-cl"));
}

#[test]
fn identical_duplicate_content_lengths_are_accepted() {
    let (rsp, _) = exchange(
        b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello",
    );
    assert_eq!(rsp[0].status, 200);
    assert_eq!(rsp[0].body, b"hello");
}

#[test]
fn malformed_content_length_is_refused_not_a_panic() {
    // Each of these used to reach `str::parse().unwrap()` in the body reader.
    for value in [
        "abc",
        "-1",
        "+5",
        "1 2",
        "0x10",
        "",
        "99999999999999999999999",
    ] {
        let raw = format!("POST /bad-cl HTTP/1.1\r\nHost: x\r\nContent-Length: {value}\r\n\r\n");
        let (rsp, closed) = exchange(raw.as_bytes());
        assert_eq!(rsp.len(), 1, "Content-Length {value:?}");
        assert_eq!(rsp[0].status, 400, "Content-Length {value:?}");
        assert!(closed, "Content-Length {value:?}");
    }
    assert!(!seen("/bad-cl"));
    // and the server is still serving
    let (rsp, _) = exchange(b"GET /alive HTTP/1.1\r\nHost: x\r\n\r\n");
    assert_eq!(rsp[0].status, 200);
}

#[test]
fn transfer_encoding_must_end_in_chunked() {
    for (value, status) in [
        ("chunked, gzip", 400),
        ("identity", 400),
        ("chunked, chunked", 400),
        ("gzip, chunked", 501),
    ] {
        let raw = format!(
            "POST /te-bad HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: {value}\r\n\r\n0\r\n\r\n"
        );
        let (rsp, closed) = exchange(raw.as_bytes());
        assert_eq!(rsp[0].status, status, "Transfer-Encoding {value:?}");
        assert!(closed, "Transfer-Encoding {value:?}");
    }
    assert!(!seen("/te-bad"));
}

#[test]
fn transfer_encoding_on_http_1_0_is_refused() {
    let (rsp, closed) =
        exchange(b"POST /te-10 HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n");
    assert_eq!(rsp[0].status, 400);
    assert!(closed);
    assert!(!seen("/te-10"));
}

// ---------------------------------------------------------------------------------------------
// Chunked request bodies
// ---------------------------------------------------------------------------------------------

#[test]
fn chunked_request_body_is_decoded_and_the_connection_stays_usable() {
    let (rsp, closed) = exchange(
        b"POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n\
          5\r\nhello\r\n6;name=value\r\n world\r\n0\r\nX-Trailer: t\r\n\r\n\
          GET /after-chunked HTTP/1.1\r\nHost: x\r\n\r\n",
    );
    assert!(!closed);
    assert_eq!(rsp.len(), 2);
    assert_eq!(rsp[0].status, 200);
    assert_eq!(rsp[0].body, b"hello world");
    assert_eq!(rsp[1].body, b"ignored");
    assert!(seen("/after-chunked"));
}

#[test]
fn chunked_body_arriving_in_pieces_is_decoded() {
    let mut stream = connect();
    let parts: [&[u8]; 6] = [
        b"POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"a",
        b"\r\n0123",
        b"456789\r",
        b"\n0\r\n",
        b"\r\n",
    ];
    for part in parts {
        stream.write_all(part).unwrap();
        stream.flush().unwrap();
        std::thread::sleep(Duration::from_millis(20));
    }
    let rsp = read_response(&mut stream, &mut Vec::new()).expect("response");
    assert_eq!(rsp.status, 200);
    assert_eq!(rsp.body, b"0123456789");
}

#[test]
fn malformed_chunked_bodies_close_the_connection() {
    for body in [
        "zz\r\nhello\r\n0\r\n\r\n",              // not hex
        "fffffffffffffffffff\r\nx\r\n0\r\n\r\n", // size overflows
        "5\nhello\r\n0\r\n\r\n",                 // bare LF after size
        "5\r\nhelloXY0\r\n\r\n",                 // data not followed by CRLF
        "5 \r\nhello\r\n0\r\n\r\n",              // trailing whitespace, no extension
    ] {
        let raw = format!(
            "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n{body}\
             GET /after-bad-chunk HTTP/1.1\r\nHost: x\r\n\r\n"
        );
        let (rsp, closed) = exchange(raw.as_bytes());
        assert_eq!(
            rsp.len(),
            1,
            "body {body:?}: no request may follow a bad chunk"
        );
        assert_eq!(
            rsp[0].status, 500,
            "body {body:?}: the service saw a read error"
        );
        assert!(closed, "body {body:?}");
    }
    assert!(!seen("/after-bad-chunk"));
}

// ---------------------------------------------------------------------------------------------
// Bodies the service did not read
// ---------------------------------------------------------------------------------------------

#[test]
fn an_unread_body_is_never_parsed_as_the_next_request() {
    // Before: the service ignored the body, and its bytes were parsed as a pipelined request.
    let smuggled = b"GET /smuggled-in-body HTTP/1.1\r\nHost: x\r\n\r\n";
    let raw = [
        format!(
            "POST /ignore HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
            smuggled.len()
        )
        .as_bytes(),
        smuggled,
        b"GET /after-unread HTTP/1.1\r\nHost: x\r\n\r\n",
    ]
    .concat();
    let (rsp, closed) = exchange(&raw);
    assert!(
        !closed,
        "a fully buffered body is discarded and the connection kept"
    );
    assert_eq!(rsp.len(), 2);
    assert!(rsp.iter().all(|r| r.status == 200));
    assert!(!seen("/smuggled-in-body"));
    assert!(seen("/after-unread"));
}

#[test]
fn an_unread_chunked_body_is_discarded() {
    let smuggled = "GET /smuggled-in-chunk HTTP/1.1\r\nHost: x\r\n\r\n";
    let raw = format!(
        "POST /ignore HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n\
         {:x}\r\n{smuggled}\r\n0\r\n\r\n\
         GET /after-unread-chunked HTTP/1.1\r\nHost: x\r\n\r\n",
        smuggled.len()
    );
    let (rsp, closed) = exchange(raw.as_bytes());
    assert!(!closed);
    assert_eq!(rsp.len(), 2);
    assert!(!seen("/smuggled-in-chunk"));
    assert!(seen("/after-unread-chunked"));
}

#[test]
fn an_unread_body_still_in_flight_closes_after_the_response() {
    // 100 bytes declared, 7 sent: the server answers without waiting for the rest, then closes
    // (it cannot know where the next request would start).
    let (rsp, closed) =
        exchange(b"POST /ignore HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\npartial");
    assert_eq!(rsp.len(), 1);
    assert_eq!(rsp[0].status, 200);
    assert!(closed);
}

// ---------------------------------------------------------------------------------------------
// Head limits
// ---------------------------------------------------------------------------------------------

#[test]
fn an_endless_request_head_is_refused_with_431() {
    let mut stream = connect();
    stream
        .write_all(b"GET /endless HTTP/1.1\r\nHost: x\r\nX-Pad: ")
        .unwrap();
    let pad = vec![b'p'; 8 * 1024];
    let mut response = None;
    for _ in 0..16 {
        if stream.write_all(&pad).is_err() {
            break;
        }
    }
    let mut pending = Vec::new();
    if let Some(r) = read_response(&mut stream, &mut pending) {
        response = Some(r);
    }
    let rsp = response.expect("a 431 before the connection closes");
    assert_eq!(rsp.status, 431);
    assert!(!seen("/endless"));
}

#[test]
fn a_large_but_bounded_head_is_served() {
    // A bearer token with claims runs to several KiB; well under the 64 KiB head limit.
    let raw = format!(
        "GET /big-head HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {}\r\n\r\n",
        "t".repeat(16 * 1024)
    );
    let (rsp, _) = exchange(raw.as_bytes());
    assert_eq!(rsp[0].status, 200);
    assert!(seen("/big-head"));
}

#[test]
fn too_many_header_fields_is_refused_with_431() {
    let mut raw = String::from("GET /too-many HTTP/1.1\r\nHost: x\r\n");
    for i in 0..40 {
        raw.push_str(&format!("X-{i}: v\r\n"));
    }
    raw.push_str("\r\n");
    let (rsp, closed) = exchange(raw.as_bytes());
    assert_eq!(rsp[0].status, 431);
    assert!(closed);
    assert!(!seen("/too-many"));
}

#[test]
fn malformed_request_heads_are_refused_with_400() {
    for raw in [
        &b"GET /space name HTTP/1.1\r\nHost: x\r\n\r\n"[..],
        b"GET /obs-fold HTTP/1.1\r\nHost: x\r\nX-A: a\r\n b\r\n\r\n",
        b"GET /ws-colon HTTP/1.1\r\nHost : x\r\n\r\n",
        b"GET /v2 HTTP/2.0\r\nHost: x\r\n\r\n",
        b"X\x00Y\x01Z\r\n\r\n",
    ] {
        let (rsp, closed) = exchange(raw);
        assert_eq!(rsp.len(), 1, "{:?}", String::from_utf8_lossy(raw));
        assert_eq!(rsp[0].status, 400, "{:?}", String::from_utf8_lossy(raw));
        assert!(closed);
    }
    assert!(!seen("/obs-fold") && !seen("/ws-colon") && !seen("/v2"));
}

#[test]
fn responses_for_earlier_pipelined_requests_are_sent_before_a_refusal() {
    let (rsp, closed) = exchange(
        b"GET /pipelined-ok HTTP/1.1\r\nHost: x\r\n\r\n\
          POST /pipelined-bad HTTP/1.1\r\nContent-Length: x\r\n\r\n",
    );
    assert!(closed);
    assert_eq!(rsp.len(), 2);
    assert_eq!(rsp[0].status, 200);
    assert_eq!(rsp[1].status, 400);
}

// ---------------------------------------------------------------------------------------------
// Response side
// ---------------------------------------------------------------------------------------------

#[test]
fn more_than_sixteen_response_headers_are_all_sent() {
    // The 17th header used to index past a fixed array and panic the connection.
    let (rsp, _) = exchange(b"GET /many-headers HTTP/1.1\r\nHost: x\r\n\r\n");
    assert_eq!(rsp[0].status, 200);
    for i in 0..20 {
        assert_eq!(
            rsp[0].header(&format!("x-h{i}")),
            Some(i.to_string().as_str())
        );
    }
}

#[test]
fn a_header_value_with_crlf_is_dropped_not_split_into_the_response() {
    let (rsp, _) = exchange(b"GET /split HTTP/1.1\r\nHost: x\r\n\r\n");
    assert_eq!(rsp[0].status, 200);
    assert_eq!(rsp[0].header("x-ok"), Some("1"));
    assert_eq!(rsp[0].header("x-bad"), None);
    assert_eq!(rsp[0].header("set-cookie"), None);
    assert_eq!(rsp[0].body, b"split");
}

#[test]
fn a_service_error_does_not_leak_its_text() {
    let (rsp, _) = exchange(b"GET /fail HTTP/1.1\r\nHost: x\r\n\r\n");
    assert_eq!(rsp[0].status, 500);
    let body = String::from_utf8_lossy(&rsp[0].body);
    assert_eq!(body, "Internal Server Error");
}
