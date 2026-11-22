//! HTTP/1.1 protocol upgrade (RFC 9110 §7.8): send a request that asks to switch protocols and,
//! on `101 Switching Protocols`, hand the caller the connection itself.
//!
//! This is what a WebSocket client needs from an HTTP client: the opening handshake is an ordinary
//! HTTP/1.1 request, and everything after the 101 is another protocol on the same plain or TLS
//! stream. The HTTP client does the HTTP part (connect, TLS, request head, response head); the
//! caller's protocol library only frames bytes on the [`Upgraded`] stream.
//!
//! The client does not interpret the upgraded protocol. It checks only what HTTP itself requires:
//! the request carries `Connection: upgrade` and `Upgrade`, and a 101 response names the protocol
//! in its own `Upgrade` header. Protocol-specific checks (a WebSocket `Sec-WebSocket-Accept`, say)
//! are the caller's.

use std::fmt;
use std::io::{self, Read, Write};
use std::time::Duration;

use http::header::{CONNECTION, UPGRADE};
use http::{HeaderMap, StatusCode};

use super::shared::SharedStream;
use super::Response;

/// The outcome of [`super::HttpClient::upgrade`].
#[derive(Debug)]
pub enum Upgrade {
    /// The server switched protocols (`101`). `response` is the 101 head (no body); `stream` is
    /// the connection, now speaking the upgraded protocol.
    Switched {
        /// The `101 Switching Protocols` response head.
        response: Response,
        /// The connection after the 101.
        stream: Upgraded,
    },
    /// The server answered with a final response instead (a `401`, a `426`, a redirect...). Its
    /// body can be read as usual; the connection is not upgraded.
    Refused(Response),
}

/// A connection after `101 Switching Protocols`: raw bytes, plain or TLS, in both directions.
///
/// Bytes the server sent right behind the 101 head (a first WebSocket frame, say) were already
/// read into the client's buffer; reads return those first, then read the socket. The read and
/// write timeouts set on the client carry over and can be changed with [`Self::set_timeout`].
pub struct Upgraded {
    stream: SharedStream,
}

impl Upgraded {
    pub(crate) fn new(stream: SharedStream) -> Self {
        Self { stream }
    }

    /// Whether the connection is TLS.
    pub fn is_tls(&self) -> io::Result<bool> {
        self.stream.is_tls()
    }

    /// Set the read and write timeout of the connection (`None`: block until data or close).
    pub fn set_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_timeout(timeout)
    }
}

impl fmt::Debug for Upgraded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Upgraded")
            .field("tls", &self.stream.is_tls().unwrap_or(false))
            .finish_non_exhaustive()
    }
}

impl Read for Upgraded {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buffer)
    }
}

impl Write for Upgraded {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

/// A request asks to upgrade when it names a protocol in `Upgrade` and lists `upgrade` among the
/// `Connection` options (RFC 9110 §7.8: a server ignores an `Upgrade` without it).
pub(crate) fn check_upgrade_request(headers: &HeaderMap) -> io::Result<()> {
    if !headers.contains_key(UPGRADE) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "an upgrade request needs an Upgrade header",
        ));
    }
    if !has_token(headers, CONNECTION, "upgrade") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "an upgrade request needs `Connection: upgrade`",
        ));
    }
    Ok(())
}

/// A 101 must say which protocol the connection switched to (RFC 9110 §15.2.2).
pub(crate) fn check_switched_response(response: &Response) -> io::Result<()> {
    debug_assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    if response.headers().contains_key(UPGRADE) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "101 Switching Protocols without an Upgrade header",
        ))
    }
}

fn has_token(headers: &HeaderMap, name: http::header::HeaderName, token: &str) -> bool {
    headers.get_all(name).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|option| option.trim().eq_ignore_ascii_case(token))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::HttpClient;
    use http::{HeaderValue, Method};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::thread;

    const SWITCHED: &[u8] =
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: echo\r\nConnection: Upgrade\r\n\r\n";

    trait ReadWrite: Read + Write {}
    impl<T: Read + Write> ReadWrite for T {}

    fn read_head(stream: &mut dyn ReadWrite) -> String {
        let mut head = Vec::new();
        let mut byte = [0_u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).expect("read request head");
            head.push(byte[0]);
        }
        String::from_utf8(head).expect("request head is UTF-8")
    }

    /// Answers the upgrade with `answer`, then echoes four bytes back.
    fn serve(
        listener: TcpListener,
        tls: Option<Arc<rustls::ServerConfig>>,
        answer: &'static [u8],
    ) -> thread::JoinHandle<String> {
        thread::spawn(move || {
            let (socket, _) = listener.accept().expect("accept");
            let mut stream: Box<dyn ReadWrite> = match tls {
                Some(config) => Box::new(rustls::StreamOwned::new(
                    rustls::ServerConnection::new(config).expect("TLS server"),
                    socket,
                )),
                None => Box::new(socket),
            };
            let head = read_head(stream.as_mut());
            stream.write_all(answer).expect("write answer");
            stream.flush().expect("flush answer");
            if answer.starts_with(b"HTTP/1.1 101") {
                let mut ping = [0_u8; 4];
                stream.read_exact(&mut ping).expect("read upgraded bytes");
                stream.write_all(&ping).expect("echo");
                stream.flush().expect("flush echo");
            }
            head
        })
    }

    fn upgrade_request(client: &HttpClient) -> crate::client::Request {
        let mut request = client.new_request(Method::GET, "/stream".parse().unwrap());
        request
            .headers_mut()
            .insert(UPGRADE, HeaderValue::from_static("echo"));
        request
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("Upgrade"));
        request
    }

    fn switched(outcome: Upgrade) -> (Response, Upgraded) {
        match outcome {
            Upgrade::Switched { response, stream } => (response, stream),
            Upgrade::Refused(response) => panic!("refused: {response:?}"),
        }
    }

    fn tls_configs() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
        let certificate = cert.der().clone();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der());
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key.into())
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        (Arc::new(client), Arc::new(server))
    }

    #[test]
    fn plain_upgrade_hands_over_the_connection_with_early_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        // The 101 and the first bytes of the new protocol arrive in one write: the client has
        // buffered them by the time it returns, and the stream must still yield them first.
        let answer: &'static [u8] = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: echo\r\nConnection: Upgrade\r\n\r\nearly";
        let server = serve(listener, None, answer);

        let client = HttpClient::from_url(&format!("http://127.0.0.1:{port}/")).unwrap();
        let request = upgrade_request(&client);
        let (response, mut stream) = switched(client.upgrade(request).unwrap());
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(response.headers()[UPGRADE], "echo");
        assert!(!stream.is_tls().unwrap());

        let mut early = [0_u8; 5];
        stream.read_exact(&mut early).unwrap();
        assert_eq!(&early, b"early");
        stream.write_all(b"ping").unwrap();
        stream.flush().unwrap();
        let mut echo = [0_u8; 4];
        stream.read_exact(&mut echo).unwrap();
        assert_eq!(&echo, b"ping");

        let head = server.join().unwrap().to_ascii_lowercase();
        assert!(head.starts_with("get /stream http/1.1\r\n"), "{head}");
        assert!(head.contains("\r\nupgrade: echo\r\n"), "{head}");
        assert!(head.contains("\r\nconnection: upgrade\r\n"), "{head}");
        assert!(
            head.contains(&format!("\r\nhost: 127.0.0.1:{port}\r\n")),
            "{head}"
        );
    }

    #[test]
    fn tls_upgrade_hands_over_the_tls_stream() {
        let (client_config, server_config) = tls_configs();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = serve(listener, Some(server_config), SWITCHED);

        let client = HttpClient::from_url_with_tls_config(
            &format!("https://127.0.0.1:{port}/"),
            client_config,
        )
        .unwrap();
        let request = upgrade_request(&client);
        let (_, mut stream) = switched(client.upgrade(request).unwrap());
        assert!(stream.is_tls().unwrap());
        stream.set_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.write_all(b"tls!").unwrap();
        stream.flush().unwrap();
        let mut echo = [0_u8; 4];
        stream.read_exact(&mut echo).unwrap();
        assert_eq!(&echo, b"tls!");
        server.join().unwrap();
    }

    #[test]
    fn a_final_response_is_returned_with_its_body() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = serve(
            listener,
            None,
            b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 6\r\nConnection: close\r\n\r\nno key",
        );

        let client = HttpClient::from_url(&format!("http://127.0.0.1:{port}/")).unwrap();
        let request = upgrade_request(&client);
        let mut response = match client.upgrade(request).unwrap() {
            Upgrade::Refused(response) => response,
            Upgrade::Switched { response, .. } => panic!("switched: {response:?}"),
        };
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let mut body = String::new();
        response.read_to_string(&mut body).unwrap();
        assert_eq!(body, "no key");
        server.join().unwrap();
    }

    #[test]
    fn a_101_without_upgrade_is_invalid_data() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = serve(listener, None, b"HTTP/1.1 101 Switching Protocols\r\n\r\n");

        let client = HttpClient::from_url(&format!("http://127.0.0.1:{port}/")).unwrap();
        let request = upgrade_request(&client);
        let error = client.upgrade(request).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        drop(server);
    }

    #[test]
    fn a_request_without_upgrade_headers_is_not_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let client = HttpClient::from_url(&format!("http://127.0.0.1:{port}/")).unwrap();
        let request = client.new_request(Method::GET, "/stream".parse().unwrap());
        let error = client.upgrade(request).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let mut byte = [0_u8; 1];
        // Dropping the unsent request must not write an empty head either: the peer sees EOF.
        assert_eq!(socket.read(&mut byte).unwrap(), 0);
    }

    #[test]
    fn upgrade_request_needs_both_headers() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            check_upgrade_request(&headers).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
        assert_eq!(
            check_upgrade_request(&headers).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive, Upgrade"));
        check_upgrade_request(&headers).unwrap();
    }

    #[test]
    fn connection_options_are_matched_as_tokens() {
        let mut headers = HeaderMap::new();
        headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(CONNECTION, HeaderValue::from_static("upgraded"));
        assert!(check_upgrade_request(&headers).is_err());
        headers.append(CONNECTION, HeaderValue::from_static("UPGRADE"));
        check_upgrade_request(&headers).unwrap();
    }
}
