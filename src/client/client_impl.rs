use std::fmt;
use std::io;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use http::{header::HOST, HeaderValue, Method, Uri};
use may::net::TcpStream;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, StreamOwned};
use rustls_platform_verifier::BuilderVerifierExt;

use super::shared::{SharedStream, Transport};
use crate::client::{MultipartForm, Request, Response};

/// Coroutine HTTP/1.1 client with native HTTP and rustls-backed HTTPS transports.
pub struct HttpClient {
    conn: SharedStream,
    expect_body: bool,
    host_header: Option<HeaderValue>,
}

impl fmt::Debug for HttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tls = self.conn.is_tls().unwrap_or(false);
        formatter
            .debug_struct("HttpClient")
            .field("tls", &tls)
            .field("host_header", &self.host_header)
            .finish_non_exhaustive()
    }
}

/// On Windows, `may::net::TcpStream::connect` can return various
/// WSA error codes for connection failures, and `raw_os_error()` may
/// be `None` when the error passes through the coroutine context.
/// Remap common connection-refusal errors so the client API reports
/// `ErrorKind::ConnectionRefused` consistently.
#[cfg(windows)]
fn connect_remap(e: io::Error) -> io::Error {
    match e.raw_os_error() {
        // WSAECONNREFUSED (10061) — connection refused
        // WSAETIMEDOUT (10060) — connection timed out (no response)
        // WSAEHOSTUNREACH (10064) — host unreachable
        Some(10061) | Some(10060) | Some(10064) => {
            io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused")
        }
        _ => {
            // raw_os_error() may return None when errors pass through
            // the coroutine context; fall back to string matching
            let desc = e.to_string().to_lowercase();
            if desc.contains("refused")
                || desc.contains("timed out")
                || desc.contains("unreachable")
                || desc.contains("wsaeconnrefused")
                || desc.contains("wsaetimedout")
                || desc.contains("wsaehostunreach")
            {
                io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused")
            } else {
                e
            }
        }
    }
}

impl HttpClient {
    /// Connect to the given address.
    pub fn connect<A: ToSocketAddrs>(remote: A) -> io::Result<Self> {
        #[cfg(windows)]
        let stream = TcpStream::connect(remote).map_err(connect_remap)?;
        #[cfg(not(windows))]
        let stream = TcpStream::connect(remote)?;
        Ok(HttpClient {
            conn: SharedStream::new(Transport::Plain(stream)),
            expect_body: true,
            host_header: None,
        })
    }

    /// Connect to an absolute HTTP or HTTPS URL.
    ///
    /// HTTPS uses rustls with the operating system's certificate verifier and an explicit
    /// ring crypto provider. The URL's authority is retained as the default HTTP `Host` header;
    /// request methods still accept an origin-form path such as `/v1/items?limit=10`.
    pub fn from_url(url: &str) -> io::Result<Self> {
        let uri = Self::parse_absolute_url(url)?;
        match uri.scheme_str() {
            Some(scheme) if scheme.eq_ignore_ascii_case("http") => Self::from_uri(uri, None, None),
            Some(scheme) if scheme.eq_ignore_ascii_case("https") => {
                Self::from_uri(uri, Some(Self::platform_tls_config()?), None)
            }
            Some(scheme) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported URL scheme: {scheme}"),
            )),
            None => unreachable!("parse_absolute_url validates the scheme"),
        }
    }

    /// Connect to an absolute URL using a caller-supplied rustls client configuration.
    ///
    /// This supports private certificate authorities, mTLS, and deterministic TLS tests while
    /// preserving the same URL parsing and `Host` header behavior as [`Self::from_url`].
    pub fn from_url_with_tls_config(url: &str, tls_config: Arc<ClientConfig>) -> io::Result<Self> {
        let uri = Self::parse_absolute_url(url)?;
        Self::from_uri(uri, Some(tls_config), None)
    }

    pub(crate) fn from_url_with_options(
        url: &str,
        tls_config: Arc<ClientConfig>,
        connect_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        let uri = Self::parse_absolute_url(url)?;
        let tls = uri
            .scheme_str()
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https"))
            .then_some(tls_config);
        Self::from_uri(uri, tls, connect_timeout)
    }

    fn parse_absolute_url(url: &str) -> io::Result<Uri> {
        let uri: Uri = url.parse().map_err(|error| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("invalid URL: {error}"))
        })?;
        uri.scheme_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "URL must include http:// or https://",
            )
        })?;
        let host = uri.host().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "URL must include a host")
        })?;
        if host.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "URL must include a host",
            ));
        }
        Ok(uri)
    }

    fn from_uri(
        uri: Uri,
        tls_config: Option<Arc<ClientConfig>>,
        connect_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        let scheme = uri.scheme_str().expect("from_uri receives an absolute URL");
        let host = uri.host().expect("from_uri receives a URL with a host");
        let port = uri
            .port_u16()
            .unwrap_or(if scheme.eq_ignore_ascii_case("https") {
                443
            } else {
                80
            });
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported URL scheme: {scheme}"),
            ));
        }

        let stream = if let Some(timeout) = connect_timeout {
            let mut last_error = None;
            let mut connected = None;
            for address in (host, port).to_socket_addrs()? {
                match TcpStream::connect_timeout(&address, timeout) {
                    Ok(stream) => {
                        connected = Some(stream);
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            connected.ok_or_else(|| {
                last_error.unwrap_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::AddrNotAvailable,
                        "host resolved no addresses",
                    )
                })
            })?
        } else {
            TcpStream::connect((host, port))?
        };
        let transport = if scheme.eq_ignore_ascii_case("https") {
            let tls_config = tls_config.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "HTTPS requires a TLS client configuration",
                )
            })?;
            let server_name = ServerName::try_from(host.to_string()).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid TLS server name: {error}"),
                )
            })?;
            let connection = ClientConnection::new(tls_config, server_name)
                .map_err(|error| io::Error::other(format!("TLS setup failed: {error}")))?;
            Transport::Tls(Box::new(StreamOwned::new(connection, stream)))
        } else {
            Transport::Plain(stream)
        };

        let authority = uri.authority().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "URL must include an authority")
        })?;
        let host_header = HeaderValue::from_str(authority.as_str()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid URL authority: {error}"),
            )
        })?;

        Ok(Self {
            conn: SharedStream::new(transport),
            expect_body: true,
            host_header: Some(host_header),
        })
    }

    pub(crate) fn platform_tls_config() -> io::Result<Arc<ClientConfig>> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| io::Error::other(format!("TLS protocol setup failed: {error}")))?;
        let config = builder
            .with_platform_verifier()
            .map_err(|error| io::Error::other(format!("platform verifier failed: {error}")))?
            .with_no_client_auth();
        Ok(Arc::new(config))
    }

    /// Set read/write timeout on the underlying connection.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        let _ = self.conn.set_timeout(timeout);
        self
    }

    /// GET shortcut — sends request on drop and reads the response.
    pub fn get(&mut self, uri: Uri) -> io::Result<Response> {
        self.expect_body = true; // GET can have a body
        let req = self.new_request(Method::GET, uri);
        drop(req);
        self.get_rsp()
    }

    /// POST shortcut with body bytes.
    pub fn post<T: Buf>(&mut self, uri: Uri, mut data: T) -> io::Result<Response> {
        self.expect_body = true; // POST can have a body
        let mut req = self.new_request(Method::POST, uri);
        let body = data.copy_to_bytes(data.remaining());
        req.send(&body)?;
        drop(req);
        self.get_rsp()
    }

    /// POST a multipart/form-data body without buffering an additional encoded copy.
    pub fn post_multipart(&mut self, uri: Uri, form: &MultipartForm) -> io::Result<Response> {
        self.expect_body = true;
        let mut req = self.new_request(Method::POST, uri);
        req.send_multipart(form)?;
        self.send_request(req)
    }

    /// Serialize a value as JSON, POST it, and return the response.
    #[cfg(feature = "json")]
    pub fn post_json<T: serde::Serialize + ?Sized>(
        &mut self,
        uri: Uri,
        value: &T,
    ) -> io::Result<Response> {
        self.expect_body = true;
        let mut req = self.new_request(Method::POST, uri);
        req.send_json(value)?;
        self.send_request(req)
    }

    /// Build a request with the given method and URI.
    #[inline]
    pub fn new_request(&self, method: Method, uri: Uri) -> Request {
        let mut req = Request::new(self.conn.clone());
        // HEAD requests expect no body
        if method == Method::HEAD {
            req.expect_body(false);
        }
        *req.method_mut() = method;
        *req.uri_mut() = uri;
        if let Some(host_header) = &self.host_header {
            req.headers_mut().insert(HOST, host_header.clone());
        }
        req
    }

    /// Send a request built from this client and read the response.
    #[inline]
    pub fn send_request(&mut self, req: Request) -> io::Result<Response> {
        debug_assert!(
            self.conn.ptr_eq(req.conn()),
            "client and request must share the same connection"
        );
        self.expect_body = req.expect_body_request();
        drop(req);
        self.get_rsp()
    }

    #[inline]
    fn get_rsp(&mut self) -> io::Result<Response> {
        let reader = self.conn.clone();
        let expect_body = self.expect_body;
        self.conn.with_buffer(|stream| loop {
            match super::response::decode(stream.get_reader_buf())? {
                None => {
                    if stream.bump_read()? == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection closed before the response head",
                        ));
                    }
                }
                Some(mut response) => {
                    response.set_reader(reader.clone(), expect_body)?;
                    return Ok(response);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn serve_once(listener: TcpListener, tls_config: Option<Arc<rustls::ServerConfig>>) {
        let (socket, _) = listener.accept().expect("accept test connection");
        let mut transport: Box<dyn ReadWrite> = if let Some(config) = tls_config {
            let connection = rustls::ServerConnection::new(config).expect("create TLS server");
            Box::new(rustls::StreamOwned::new(connection, socket))
        } else {
            Box::new(socket)
        };

        let mut request = Vec::new();
        let mut byte = [0_u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            transport.read_exact(&mut byte).expect("read request");
            request.push(byte[0]);
        }
        let request = String::from_utf8(request).expect("request is UTF-8");
        assert!(request.starts_with("GET /secure?value=1 HTTP/1.1\r\n"));
        assert!(request
            .to_ascii_lowercase()
            .contains("\r\nhost: 127.0.0.1:"));

        transport
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nsecure")
            .expect("write response");
        transport.flush().expect("flush response");
    }

    trait ReadWrite: Read + Write {}
    impl<T: Read + Write> ReadWrite for T {}

    fn tls_configs() -> (Arc<ClientConfig>, Arc<rustls::ServerConfig>) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])
                .expect("generate test certificate");
        let certificate = cert.der().clone();
        let private_key = rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der());
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("server protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], private_key.into())
            .expect("server certificate");

        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).expect("trust test certificate");
        let client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("client protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        (Arc::new(client), Arc::new(server))
    }

    #[test]
    fn from_url_supports_http_and_sets_host_header() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP server");
        let port = listener.local_addr().expect("server address").port();
        let server = thread::spawn(move || serve_once(listener, None));

        let mut client = HttpClient::from_url(&format!("http://127.0.0.1:{port}/secure"))
            .expect("connect HTTP URL");
        let mut response = client
            .get("/secure?value=1".parse().expect("origin-form URI"))
            .expect("HTTP request");
        let mut body = String::new();
        response.read_to_string(&mut body).expect("read body");
        assert_eq!(body, "secure");
        server.join().expect("HTTP server thread");
    }

    #[test]
    fn from_url_supports_https_with_rustls() {
        let (client_config, server_config) = tls_configs();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTPS server");
        let port = listener.local_addr().expect("server address").port();
        let server = thread::spawn(move || serve_once(listener, Some(server_config)));

        let mut client = HttpClient::from_url_with_tls_config(
            &format!("https://127.0.0.1:{port}/secure"),
            client_config,
        )
        .expect("connect HTTPS URL");
        let mut response = client
            .get("/secure?value=1".parse().expect("origin-form URI"))
            .expect("HTTPS request");
        let mut body = String::new();
        response.read_to_string(&mut body).expect("read body");
        assert_eq!(body, "secure");
        server.join().expect("HTTPS server thread");
    }

    #[test]
    fn from_url_rejects_relative_and_unknown_schemes() {
        assert_eq!(
            HttpClient::from_url("/relative").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            HttpClient::from_url("ftp://example.com/file")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
