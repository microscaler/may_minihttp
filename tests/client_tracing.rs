//! The `tracing` feature: the client's span tree, its fields, and silence when disabled.

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use may_minihttp::client::{Client, RedirectPolicy};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// Records `new <name> parent=<parent name> fields`, `record <name> fields`, `event <message>
/// parent=<span name> fields` and `close <name>`, in order.
#[derive(Clone, Default)]
struct Recorder {
    lines: Arc<Mutex<Vec<String>>>,
}

impl Recorder {
    fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    fn push(&self, line: String) {
        self.lines.lock().unwrap().push(line);
    }
}

#[derive(Default)]
struct Fields {
    message: String,
    text: String,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            let _ = write!(self.text, " {}={}", field.name(), value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            let _ = write!(self.text, " {}={:?}", field.name(), value);
        }
    }
}

impl<S> Layer<S> for Recorder
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("new span");
        let parent = span
            .parent()
            .map_or_else(|| "-".to_owned(), |parent| parent.name().to_owned());
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        self.push(format!(
            "new {} parent={}{}",
            span.name(),
            parent,
            fields.text
        ));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("recorded span");
        let mut fields = Fields::default();
        values.record(&mut fields);
        self.push(format!("record {}{}", span.name(), fields.text));
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let parent = ctx
            .event_span(event)
            .map_or_else(|| "-".to_owned(), |span| span.name().to_owned());
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.push(format!(
            "event {} parent={}{}",
            fields.message, parent, fields.text
        ));
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("closed span");
        self.push(format!("close {}", span.name()));
    }
}

fn read_head(stream: &mut impl Read) -> String {
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while !request.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        request.push(byte[0]);
    }
    String::from_utf8(request).unwrap()
}

/// Answers `count` requests on one keep-alive connection with `200 ok`.
fn server(count: usize) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        for _ in 0..count {
            let _ = read_head(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .unwrap();
            stream.flush().unwrap();
        }
    });
    (port, handle)
}

fn client() -> Client {
    Client::builder()
        .redirect_policy(RedirectPolicy::None)
        .request_timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

fn client_lines(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter(|line| !line.contains(" caller"))
        .cloned()
        .collect()
}

#[test]
fn a_request_is_a_span_tree_under_the_given_parent() {
    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let (port, server) = server(2);
    tracing::subscriber::with_default(subscriber, || {
        let caller = tracing::info_span!("caller");
        let client = client();
        for path in ["/one?secret=1", "/two"] {
            let response = client
                .get(&format!("http://127.0.0.1:{port}{path}"))
                .unwrap()
                .parent_span(&caller)
                .send()
                .unwrap();
            assert_eq!(response.body(), b"ok");
        }
    });
    server.join().unwrap();

    let lines = client_lines(&recorder.lines());
    let text = lines.join("\n");
    let request = format!(
        "new http_client_request parent=caller http.request.method=GET server.address=127.0.0.1 server.port={port} url.scheme=http"
    );
    assert_eq!(
        lines.iter().filter(|line| **line == request).count(),
        2,
        "{text}"
    );
    // The first request opens a connection, the second reuses it.
    let first: Vec<&str> = lines[..lines
        .iter()
        .position(|line| line == "close http_client_request")
        .unwrap()
        + 1]
        .iter()
        .map(String::as_str)
        .collect();
    assert_eq!(
        first,
        [
            request.as_str(),
            "event pool_checkout parent=http_client_request result=miss",
            &format!("new http_client_connect parent=http_client_request server.address=127.0.0.1 server.port={port}"),
            "close http_client_connect",
            "record http_client_request may_minihttp.connection_reused=false",
            "new http_client_write parent=http_client_request",
            "close http_client_write",
            "new http_client_wait parent=http_client_request",
            "record http_client_wait http.response.status_code=200",
            "close http_client_wait",
            "new http_client_body parent=http_client_request",
            "record http_client_body http.response.body.size=2",
            "close http_client_body",
            "record http_client_request http.response.status_code=200",
            "close http_client_request",
        ],
        "{text}"
    );
    assert!(
        text.contains("event pool_checkout parent=http_client_request result=hit"),
        "{text}"
    );
    assert!(
        text.contains("record http_client_request may_minihttp.connection_reused=true"),
        "{text}"
    );
    // Paths and query strings are never recorded.
    assert!(!text.contains("/one") && !text.contains("secret"), "{text}");
}

#[test]
fn without_an_explicit_parent_the_current_span_is_the_parent() {
    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let (port, server) = server(1);
    tracing::subscriber::with_default(subscriber, || {
        let caller = tracing::info_span!("caller");
        let _entered = caller.enter();
        client()
            .get(&format!("http://127.0.0.1:{port}/"))
            .unwrap()
            .send()
            .unwrap();
    });
    server.join().unwrap();
    assert!(recorder
        .lines()
        .iter()
        .any(|line| line.starts_with("new http_client_request parent=caller ")));
}

#[test]
fn a_streaming_request_span_ends_with_the_body_not_the_drop() {
    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let (port, server) = server(1);
    tracing::subscriber::with_default(subscriber, || {
        let mut response = client()
            .get(&format!("http://127.0.0.1:{port}/"))
            .unwrap()
            .send_streaming()
            .unwrap();
        let mut body = Vec::new();
        response.read_to_end(&mut body).unwrap();
        assert_eq!(body, b"ok");
        assert!(recorder
            .lines()
            .contains(&"close http_client_request".to_owned()));
        drop(response);
    });
    server.join().unwrap();
    let lines = recorder.lines();
    assert!(
        lines.contains(&"close http_client_body".to_owned()),
        "{lines:#?}"
    );
}

#[test]
fn a_refused_connect_records_the_error_type() {
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    tracing::subscriber::with_default(subscriber, || {
        assert!(client()
            .get(&format!("http://127.0.0.1:{port}/"))
            .unwrap()
            .send()
            .is_err());
    });
    let text = recorder.lines().join("\n");
    assert!(
        text.contains("record http_client_connect error.type="),
        "{text}"
    );
    assert!(
        text.contains("record http_client_request error.type="),
        "{text}"
    );
}

#[test]
fn nothing_is_emitted_when_the_target_is_disabled() {
    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(
        recorder
            .clone()
            .with_filter(Targets::new().with_target("may_minihttp", LevelFilter::OFF)),
    );
    let (port, server) = server(2);
    tracing::subscriber::with_default(subscriber, || {
        let client = client();
        for _ in 0..2 {
            client
                .get(&format!("http://127.0.0.1:{port}/"))
                .unwrap()
                .send()
                .unwrap();
        }
    });
    server.join().unwrap();
    assert_eq!(recorder.lines(), Vec::<String>::new());
}

#[test]
fn at_info_only_the_request_and_connect_spans_are_emitted() {
    let recorder = Recorder::default();
    let subscriber =
        tracing_subscriber::registry().with(recorder.clone().with_filter(LevelFilter::INFO));
    let (port, server) = server(1);
    tracing::subscriber::with_default(subscriber, || {
        client()
            .get(&format!("http://127.0.0.1:{port}/"))
            .unwrap()
            .send()
            .unwrap();
    });
    server.join().unwrap();
    let names: Vec<String> = recorder
        .lines()
        .iter()
        .filter(|line| line.starts_with("new ") || line.starts_with("event "))
        .map(|line| line.split(' ').nth(1).unwrap().to_owned())
        .collect();
    assert_eq!(names, ["http_client_request", "http_client_connect"]);
}

#[test]
fn a_caller_span_can_be_the_request_span() {
    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let (port, server) = server(1);
    tracing::subscriber::with_default(subscriber, || {
        let mine = tracing::info_span!(
            "my_client_span",
            "http.response.status_code" = tracing::field::Empty,
            "may_minihttp.connection_reused" = tracing::field::Empty,
        );
        client()
            .get(&format!("http://127.0.0.1:{port}/"))
            .unwrap()
            .request_span(&mine)
            .send()
            .unwrap();
    });
    server.join().unwrap();
    let lines = recorder.lines();
    let text = lines.join("\n");
    assert!(!text.contains("http_client_request"), "{text}");
    for expected in [
        "new http_client_connect parent=my_client_span",
        "new http_client_write parent=my_client_span",
        "new http_client_wait parent=my_client_span",
        "new http_client_body parent=my_client_span",
        "record my_client_span may_minihttp.connection_reused=false",
        "record my_client_span http.response.status_code=200",
    ] {
        assert!(
            lines.iter().any(|line| line.starts_with(expected)),
            "{expected}\n{text}"
        );
    }
}

#[test]
fn a_disabled_request_span_gets_no_orphan_children() {
    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let (port, server) = server(1);
    tracing::subscriber::with_default(subscriber, || {
        client()
            .get(&format!("http://127.0.0.1:{port}/"))
            .unwrap()
            .request_span(&tracing::Span::none())
            .send()
            .unwrap();
    });
    server.join().unwrap();
    assert_eq!(recorder.lines(), Vec::<String>::new());
}
