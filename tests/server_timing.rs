//! Feature `op-timing`: the server's phases land in op_duration_seconds and its connection levels
//! in op_gauge (microscaler-timing).
#![cfg(feature = "op-timing")]

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use may_minihttp::{HttpServer, HttpService, Request, Response};
use microscaler_timing::{gauge_values, snapshot, Outcome};

#[derive(Clone)]
struct Slow;

impl HttpService for Slow {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        if req.path() == "/fail" {
            return Err(io::Error::other("fail"));
        }
        may::coroutine::sleep(Duration::from_millis(30));
        rsp.body("ok");
        Ok(())
    }
}

fn count(op: &str, outcome: Outcome) -> u64 {
    snapshot()
        .into_iter()
        .filter(|s| s.component == "may_minihttp.server" && s.op == op && s.outcome == outcome)
        .map(|s| s.count())
        .sum()
}

fn read_response(stream: &mut TcpStream) -> String {
    let mut buf = [0u8; 1024];
    let mut out = Vec::new();
    while !String::from_utf8_lossy(&out).contains("\r\n\r\n")
        || !out.ends_with(b"ok") && !String::from_utf8_lossy(&out).contains(" 500 ")
    {
        let n = stream.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
fn server_phases_and_connection_levels_are_recorded() {
    may::config().set_stack_size(0x8000);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let handle = HttpServer(Slow).start(("127.0.0.1", port)).unwrap();
    std::mem::forget(handle);
    let mut stream = loop {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            break s;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    // the client waits before sending: that is accept_to_first_byte
    std::thread::sleep(Duration::from_millis(20));
    for _ in 0..2 {
        stream
            .write_all(b"GET /a HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        assert!(read_response(&mut stream).contains("200"));
    }
    stream
        .write_all(b"GET /fail HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
        .unwrap();
    let _ = read_response(&mut stream);
    // the response is on the wire before the server's write returns: give it a moment
    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(count("accept_to_first_byte", Outcome::Ok), 1);
    assert_eq!(count("read_parse", Outcome::Ok), 3);
    assert_eq!(count("service", Outcome::Ok), 2);
    assert_eq!(count("service", Outcome::Err), 1);
    assert_eq!(count("response_write", Outcome::Ok), 3);
    assert_eq!(count("request_total", Outcome::Ok), 3);
    let service = snapshot()
        .into_iter()
        .find(|s| {
            s.component == "may_minihttp.server" && s.op == "service" && s.outcome == Outcome::Ok
        })
        .unwrap();
    assert!(
        service.sum_nanos >= 60_000_000,
        "two 30 ms calls: {}",
        service.sum_nanos
    );
    let accept = snapshot()
        .into_iter()
        .find(|s| s.op == "accept_to_first_byte")
        .unwrap();
    assert!(accept.sum_nanos >= 20_000_000, "{}", accept.sum_nanos);

    let gauges = gauge_values();
    let gauge = |op: &str| {
        gauges
            .iter()
            .find(|(c, o, _)| *c == "may_minihttp.server" && *o == op)
            .map(|g| g.2)
    };
    assert_eq!(gauge("open_connections"), Some(1));
    #[cfg(target_os = "linux")]
    {
        assert_eq!(gauge("accept_backlog"), Some(0));
        assert!(gauge("accept_backlog_limit").unwrap() > 0);
    }
    drop(stream);
    for _ in 0..100 {
        if gauge_values()
            .iter()
            .any(|(c, o, v)| *c == "may_minihttp.server" && *o == "open_connections" && *v == 0)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("open_connections did not return to 0");
}
