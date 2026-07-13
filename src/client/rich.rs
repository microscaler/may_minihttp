//! Replay-aware requests, secure redirects, and bounded connection pooling.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{self, Read};
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::header::{
    AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, LOCATION, PROXY_AUTHORIZATION,
    TRANSFER_ENCODING,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Version};
use may::sync::{Condvar, Mutex};
use rustls::ClientConfig;
use url::Url;

use super::{HttpClient, MultipartForm};

const DEFAULT_MAX_RESPONSE_BODY: usize = 8 * 1024 * 1024;

/// Policy governing whether HTTP redirects are followed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RedirectPolicy {
    /// Return 3xx responses to the caller. This is the default.
    #[default]
    None,
    /// Follow at most `max_hops` GET/HEAD redirects within the original origin.
    SameOrigin { max_hops: usize },
    /// Follow cross-origin GET/HEAD redirects, stripping credentials first.
    CrossOrigin {
        max_hops: usize,
        /// HTTPS-to-HTTP transitions remain forbidden unless this is explicitly true.
        allow_https_downgrade: bool,
    },
}

impl RedirectPolicy {
    fn max_hops(self) -> Option<usize> {
        match self {
            Self::None => None,
            Self::SameOrigin { max_hops } | Self::CrossOrigin { max_hops, .. } => Some(max_hops),
        }
    }
}

/// Builder for a cloneable, coroutine-safe HTTP client.
pub struct ClientBuilder {
    max_connections: usize,
    max_connections_per_origin: usize,
    idle_timeout: Duration,
    max_connection_lifetime: Duration,
    connect_timeout: Duration,
    io_timeout: Duration,
    request_timeout: Duration,
    max_response_body: usize,
    redirect_policy: RedirectPolicy,
    tls_config: Option<Arc<ClientConfig>>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_connections_per_origin: 8,
            idle_timeout: Duration::from_secs(90),
            max_connection_lifetime: Duration::from_secs(15 * 60),
            connect_timeout: Duration::from_secs(10),
            io_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            max_response_body: DEFAULT_MAX_RESPONSE_BODY,
            redirect_policy: RedirectPolicy::None,
            tls_config: None,
        }
    }
}

impl ClientBuilder {
    /// Create a builder with conservative finite limits and redirects disabled.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_connections(mut self, value: usize) -> Self {
        self.max_connections = value;
        self
    }

    pub fn max_connections_per_origin(mut self, value: usize) -> Self {
        self.max_connections_per_origin = value;
        self
    }

    pub fn idle_timeout(mut self, value: Duration) -> Self {
        self.idle_timeout = value;
        self
    }

    pub fn max_connection_lifetime(mut self, value: Duration) -> Self {
        self.max_connection_lifetime = value;
        self
    }

    pub fn connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    pub fn io_timeout(mut self, value: Duration) -> Self {
        self.io_timeout = value;
        self
    }

    pub fn request_timeout(mut self, value: Duration) -> Self {
        self.request_timeout = value;
        self
    }

    pub fn max_response_body(mut self, value: usize) -> Self {
        self.max_response_body = value;
        self
    }

    pub fn redirect_policy(mut self, value: RedirectPolicy) -> Self {
        self.redirect_policy = value;
        self
    }

    /// Use a custom rustls configuration for HTTPS (private CAs, mTLS, or tests).
    pub fn tls_config(mut self, value: Arc<ClientConfig>) -> Self {
        self.tls_config = Some(value);
        self
    }

    pub fn build(self) -> io::Result<Client> {
        if self.max_connections == 0 || self.max_connections_per_origin == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection limits must be greater than zero",
            ));
        }
        if self.max_connections_per_origin > self.max_connections {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "per-origin connection limit cannot exceed the global limit",
            ));
        }
        if self.max_response_body == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum response body must be greater than zero",
            ));
        }

        let tls_config = match self.tls_config {
            Some(config) => config,
            None => HttpClient::platform_tls_config()?,
        };
        let tls_profile = Arc::as_ptr(&tls_config) as usize;
        Ok(Client {
            inner: Arc::new(ClientInner {
                config: ClientConfigValues {
                    max_connections: self.max_connections,
                    max_connections_per_origin: self.max_connections_per_origin,
                    idle_timeout: self.idle_timeout,
                    max_connection_lifetime: self.max_connection_lifetime,
                    connect_timeout: self.connect_timeout,
                    io_timeout: self.io_timeout,
                    request_timeout: self.request_timeout,
                    max_response_body: self.max_response_body,
                    redirect_policy: self.redirect_policy,
                },
                tls_config,
                tls_profile,
                pool: Mutex::new(PoolState::default()),
                available: Condvar::new(),
            }),
        })
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("max_connections", &self.inner.config.max_connections)
            .field(
                "max_connections_per_origin",
                &self.inner.config.max_connections_per_origin,
            )
            .field("redirect_policy", &self.inner.config.redirect_policy)
            .finish_non_exhaustive()
    }
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub fn new() -> io::Result<Self> {
        Self::builder().build()
    }

    pub fn request(&self, method: Method, url: &str) -> io::Result<RequestBuilder> {
        let url = parse_url(url)?;
        Ok(RequestBuilder {
            client: self.clone(),
            method,
            url,
            headers: HeaderMap::new(),
            body: ReplayableBody::Empty,
            timeout: None,
        })
    }

    pub fn get(&self, url: &str) -> io::Result<RequestBuilder> {
        self.request(Method::GET, url)
    }

    pub fn post(&self, url: &str) -> io::Result<RequestBuilder> {
        self.request(Method::POST, url)
    }

    fn execute(&self, request: RequestBuilder) -> io::Result<BufferedResponse> {
        let deadline = Instant::now()
            .checked_add(request.timeout.unwrap_or(self.inner.config.request_timeout))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "request timeout overflow")
            })?;
        let mut method = request.method;
        let mut url = request.url;
        let mut headers = request.headers;
        let mut body = request.body;
        let policy = self.inner.config.redirect_policy;
        let mut visited = HashSet::new();
        visited.insert(normalized_url(&url));
        let mut hops = 0_usize;

        loop {
            let response = self.execute_once(&method, &url, &headers, &body, deadline)?;
            let Some(max_hops) = policy.max_hops() else {
                return Ok(response);
            };
            if !is_redirect(response.status) {
                return Ok(response);
            }
            let Some(location) = response.headers.get(LOCATION) else {
                return Ok(response);
            };
            if !matches!(method, Method::GET | Method::HEAD)
                && response.status != StatusCode::SEE_OTHER
            {
                // 301/302 compatibility rewriting and 307/308 body replay require separate,
                // explicit policy. The safe policy never resends a non-GET request.
                return Ok(response);
            }
            if hops >= max_hops {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP redirect hop limit exceeded",
                ));
            }
            let location = location.to_str().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid redirect Location header: {error}"),
                )
            })?;
            let target = url.join(location).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid redirect target: {error}"),
                )
            })?;
            validate_redirect(policy, &url, &target)?;
            if !visited.insert(normalized_url(&target)) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP redirect loop detected",
                ));
            }

            if !same_origin(&url, &target) {
                headers.remove(AUTHORIZATION);
                headers.remove(COOKIE);
                headers.remove(PROXY_AUTHORIZATION);
            }
            if response.status == StatusCode::SEE_OTHER && method != Method::HEAD {
                method = Method::GET;
                body = ReplayableBody::Empty;
                headers.remove(CONTENT_TYPE);
                headers.remove(CONTENT_LENGTH);
                headers.remove(TRANSFER_ENCODING);
            }
            url = target;
            hops += 1;
        }
    }

    fn execute_once(
        &self,
        method: &Method,
        url: &Url,
        headers: &HeaderMap,
        body: &ReplayableBody,
        deadline: Instant,
    ) -> io::Result<BufferedResponse> {
        let key = OriginKey::from_url(url, self.inner.tls_profile)?;
        let mut pooled = self.checkout(&key, deadline)?;
        let result = (|| {
            let initial_remaining = remaining(deadline)?;
            pooled
                .client
                .set_timeout(Some(self.inner.config.io_timeout.min(initial_remaining)));
            let target = origin_form(url)?;
            let mut request = pooled.client.new_request(method.clone(), target);
            for (name, value) in headers {
                request.headers_mut().append(name, value.clone());
            }
            let mut response = match body {
                ReplayableBody::Empty => pooled.client.send_request(request)?,
                ReplayableBody::Bytes(bytes) => {
                    request.send(bytes)?;
                    pooled.client.send_request(request)?
                }
                ReplayableBody::Multipart(form) => {
                    request.send_multipart(form)?;
                    pooled.client.send_request(request)?
                }
            };

            let status = response.status();
            let version = response.version();
            let response_headers = response.headers().clone();
            let reusable = response_is_reusable(method, status, version, &response_headers);
            let mut bytes = Vec::new();
            let limit = self.inner.config.max_response_body;
            let mut chunk = [0_u8; 8 * 1024];
            loop {
                let remaining = remaining(deadline)?;
                response.set_timeout(Some(self.inner.config.io_timeout.min(remaining)))?;
                let allowed = chunk.len().min(limit.saturating_sub(bytes.len()) + 1);
                let read = response.read(&mut chunk[..allowed])?;
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..read]);
                if bytes.len() > limit {
                    response.abandon_body();
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("HTTP response body exceeds configured {limit}-byte limit"),
                    ));
                }
            }
            drop(response);
            Ok((
                BufferedResponse {
                    status,
                    version,
                    headers: response_headers,
                    body: bytes,
                    final_url: url.clone(),
                },
                reusable,
            ))
        })();

        match result {
            Ok((response, true)) => {
                self.checkin(key, pooled);
                Ok(response)
            }
            Ok((response, false)) => {
                self.discard(key);
                Ok(response)
            }
            Err(error) => {
                self.discard(key);
                Err(error)
            }
        }
    }

    fn checkout(&self, key: &OriginKey, deadline: Instant) -> io::Result<PooledConnection> {
        loop {
            let now = Instant::now();
            let mut state = self
                .inner
                .pool
                .lock()
                .map_err(|_| io::Error::other("HTTP connection pool lock poisoned"))?;
            state.purge_expired(
                now,
                self.inner.config.idle_timeout,
                self.inner.config.max_connection_lifetime,
            );
            if let Some(connections) = state.idle.get_mut(key) {
                if let Some(connection) = connections.pop() {
                    return Ok(connection);
                }
            }
            let per_origin = state.per_origin.get(key).copied().unwrap_or(0);
            if state.total < self.inner.config.max_connections
                && per_origin < self.inner.config.max_connections_per_origin
            {
                state.total += 1;
                *state.per_origin.entry(key.clone()).or_default() += 1;
                drop(state);

                let timeout = self.inner.config.connect_timeout.min(remaining(deadline)?);
                let origin = key.connect_url();
                return match HttpClient::from_url_with_options(
                    &origin,
                    Arc::clone(&self.inner.tls_config),
                    Some(timeout),
                ) {
                    Ok(client) => Ok(PooledConnection {
                        client,
                        created: Instant::now(),
                        idle_since: Instant::now(),
                    }),
                    Err(error) => {
                        self.discard(key.clone());
                        Err(error)
                    }
                };
            }

            let wait = remaining(deadline)?;
            let (_state, timeout) = self
                .inner
                .available
                .wait_timeout(state, wait)
                .map_err(|_| io::Error::other("HTTP connection pool lock poisoned"))?;
            if timeout.timed_out() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for an HTTP connection",
                ));
            }
        }
    }

    fn checkin(&self, key: OriginKey, mut connection: PooledConnection) {
        let now = Instant::now();
        if now.duration_since(connection.created) >= self.inner.config.max_connection_lifetime {
            self.discard(key);
            return;
        }
        connection.idle_since = now;
        if let Ok(mut state) = self.inner.pool.lock() {
            state.idle.entry(key).or_default().push(connection);
            drop(state);
            self.inner.available.notify_one();
        }
    }

    fn discard(&self, key: OriginKey) {
        if let Ok(mut state) = self.inner.pool.lock() {
            state.total = state.total.saturating_sub(1);
            if let Some(count) = state.per_origin.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    state.per_origin.remove(&key);
                }
            }
            drop(state);
            self.inner.available.notify_one();
        }
    }
}

/// Replay-aware request builder. Current body variants can all be safely resent.
pub struct RequestBuilder {
    client: Client,
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: ReplayableBody,
    timeout: Option<Duration>,
}

impl RequestBuilder {
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.append(name, value);
        self
    }

    pub fn header_str(mut self, name: &str, value: &str) -> io::Result<Self> {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid request header name: {error}"),
            )
        })?;
        let value = HeaderValue::from_str(value).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid request header value: {error}"),
            )
        })?;
        self.headers.append(name, value);
        Ok(self)
    }

    pub fn body(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.body = ReplayableBody::Bytes(Arc::from(value.into()));
        self
    }

    pub fn multipart(mut self, value: MultipartForm) -> Self {
        self.body = ReplayableBody::Multipart(value);
        self
    }

    #[cfg(feature = "json")]
    pub fn json<T: serde::Serialize + ?Sized>(mut self, value: &T) -> io::Result<Self> {
        let body = serde_json::to_vec(value).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSON serialization failed: {error}"),
            )
        })?;
        self.headers
            .entry(CONTENT_TYPE)
            .or_insert(HeaderValue::from_static("application/json"));
        self.body = ReplayableBody::Bytes(Arc::from(body));
        Ok(self)
    }

    pub fn timeout(mut self, value: Duration) -> Self {
        self.timeout = Some(value);
        self
    }

    pub fn send(self) -> io::Result<BufferedResponse> {
        let client = self.client.clone();
        client.execute(self)
    }
}

#[derive(Clone)]
enum ReplayableBody {
    Empty,
    Bytes(Arc<[u8]>),
    Multipart(MultipartForm),
}

/// Fully buffered response. Buffering makes pool check-in unambiguous and redirect replay safe.
pub struct BufferedResponse {
    status: StatusCode,
    version: Version,
    headers: HeaderMap,
    body: Vec<u8>,
    final_url: Url,
}

impl BufferedResponse {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn into_body(self) -> Vec<u8> {
        self.body
    }

    pub fn final_url(&self) -> &Url {
        &self.final_url
    }

    #[cfg(feature = "json")]
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> io::Result<T> {
        serde_json::from_slice(&self.body).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSON deserialization failed: {error}"),
            )
        })
    }
}

struct ClientInner {
    config: ClientConfigValues,
    tls_config: Arc<ClientConfig>,
    tls_profile: usize,
    pool: Mutex<PoolState>,
    available: Condvar,
}

struct ClientConfigValues {
    max_connections: usize,
    max_connections_per_origin: usize,
    idle_timeout: Duration,
    max_connection_lifetime: Duration,
    connect_timeout: Duration,
    io_timeout: Duration,
    request_timeout: Duration,
    max_response_body: usize,
    redirect_policy: RedirectPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OriginKey {
    scheme: String,
    host: String,
    port: u16,
    tls_profile: usize,
}

impl OriginKey {
    fn from_url(url: &Url, tls_profile: usize) -> io::Result<Self> {
        let host = url
            .host_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "URL has no host"))?;
        let port = url.port_or_known_default().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "URL scheme has no known port")
        })?;
        Ok(Self {
            scheme: url.scheme().to_ascii_lowercase(),
            host: host.to_ascii_lowercase(),
            port,
            tls_profile: if url.scheme() == "https" {
                tls_profile
            } else {
                0
            },
        })
    }

    fn connect_url(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        format!("{}://{}:{}/", self.scheme, host, self.port)
    }
}

#[derive(Default)]
struct PoolState {
    idle: HashMap<OriginKey, Vec<PooledConnection>>,
    per_origin: HashMap<OriginKey, usize>,
    total: usize,
}

impl PoolState {
    fn purge_expired(&mut self, now: Instant, idle_timeout: Duration, lifetime: Duration) {
        let mut removed = Vec::new();
        self.idle.retain(|key, connections| {
            let before = connections.len();
            connections.retain(|connection| {
                now.duration_since(connection.idle_since) < idle_timeout
                    && now.duration_since(connection.created) < lifetime
            });
            let count = before - connections.len();
            if count > 0 {
                removed.push((key.clone(), count));
            }
            !connections.is_empty()
        });
        for (key, count) in removed {
            self.total = self.total.saturating_sub(count);
            if let Some(origin_count) = self.per_origin.get_mut(&key) {
                *origin_count = origin_count.saturating_sub(count);
                if *origin_count == 0 {
                    self.per_origin.remove(&key);
                }
            }
        }
    }
}

struct PooledConnection {
    client: HttpClient,
    created: Instant,
    idle_since: Instant,
}

fn parse_url(value: &str) -> io::Result<Url> {
    let mut url = Url::parse(value).map_err(|error| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("invalid URL: {error}"))
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "URL scheme must be http or https",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "credentials in request URLs are not supported",
        ));
    }
    url.set_fragment(None);
    Ok(url)
}

fn origin_form(url: &Url) -> io::Result<http::Uri> {
    let mut target = url.path().to_string();
    if target.is_empty() {
        target.push('/');
    }
    if let Some(query) = url.query() {
        target.push('?');
        target.push_str(query);
    }
    target.parse().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("URL cannot be represented as an HTTP request target: {error}"),
        )
    })
}

fn normalized_url(url: &Url) -> String {
    let mut normalized = url.clone();
    normalized.set_fragment(None);
    normalized.to_string()
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme().eq_ignore_ascii_case(right.scheme())
        && left.host_str().map(str::to_ascii_lowercase)
            == right.host_str().map(str::to_ascii_lowercase)
        && left.port_or_known_default() == right.port_or_known_default()
}

fn validate_redirect(policy: RedirectPolicy, source: &Url, target: &Url) -> io::Result<()> {
    if !matches!(target.scheme(), "http" | "https") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "redirect target scheme must be http or https",
        ));
    }
    let same = same_origin(source, target);
    match policy {
        RedirectPolicy::None => unreachable!(),
        RedirectPolicy::SameOrigin { .. } if !same => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "cross-origin redirect rejected by policy",
        )),
        RedirectPolicy::CrossOrigin {
            allow_https_downgrade: false,
            ..
        } if source.scheme() == "https" && target.scheme() == "http" => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "HTTPS-to-HTTP redirect rejected by policy",
        )),
        _ => Ok(()),
    }
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

fn response_is_reusable(
    method: &Method,
    status: StatusCode,
    version: Version,
    headers: &HeaderMap,
) -> bool {
    let close = header_has_token(headers, CONNECTION, "close");
    let persistent = match version {
        Version::HTTP_11 => !close,
        Version::HTTP_10 => header_has_token(headers, CONNECTION, "keep-alive"),
        _ => false,
    };
    let no_body = method == Method::HEAD
        || status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;
    let framed = no_body
        || headers.contains_key(CONTENT_LENGTH)
        || header_has_token(headers, TRANSFER_ENCODING, "chunked");
    persistent && framed
}

fn header_has_token(headers: &HeaderMap, name: HeaderName, expected: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case(expected))
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "HTTP request deadline exceeded"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    fn read_head(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut byte = [0_u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        String::from_utf8(request).unwrap()
    }

    fn test_client(policy: RedirectPolicy) -> Client {
        Client::builder()
            .redirect_policy(policy)
            .request_timeout(Duration::from_secs(2))
            .build()
            .unwrap()
    }

    #[test]
    fn low_level_client_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<HttpClient>();
    }

    #[test]
    fn pool_key_separates_scheme_port_and_tls_profile() {
        let http = OriginKey::from_url(&Url::parse("http://example.com/").unwrap(), 10).unwrap();
        let https = OriginKey::from_url(&Url::parse("https://example.com/").unwrap(), 10).unwrap();
        let other_port =
            OriginKey::from_url(&Url::parse("https://example.com:444/").unwrap(), 10).unwrap();
        let other_tls =
            OriginKey::from_url(&Url::parse("https://example.com/").unwrap(), 11).unwrap();
        assert_ne!(http, https);
        assert_ne!(https, other_port);
        assert_ne!(https, other_tls);
    }

    #[test]
    fn redirect_policy_rejects_cross_origin_and_downgrade() {
        let https = Url::parse("https://example.com/a").unwrap();
        let other = Url::parse("https://other.example/a").unwrap();
        let http = Url::parse("http://example.com/a").unwrap();
        assert!(
            validate_redirect(RedirectPolicy::SameOrigin { max_hops: 2 }, &https, &other).is_err()
        );
        assert!(validate_redirect(
            RedirectPolicy::CrossOrigin {
                max_hops: 2,
                allow_https_downgrade: false,
            },
            &https,
            &http
        )
        .is_err());
    }

    #[test]
    fn default_builder_has_redirects_disabled_and_finite_limits() {
        let builder = ClientBuilder::new();
        assert_eq!(builder.redirect_policy, RedirectPolicy::None);
        assert!(builder.max_connections > 0);
        assert!(builder.max_response_body > 0);
    }

    #[test]
    fn fully_consumed_responses_reuse_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            for expected in ["/one", "/two"] {
                let request = read_head(&mut stream);
                assert!(request.starts_with(&format!("GET {expected} HTTP/1.1\r\n")));
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .unwrap();
                stream.flush().unwrap();
            }
        });

        let client = test_client(RedirectPolicy::None);
        for path in ["one", "two"] {
            let response = client
                .get(&format!("http://127.0.0.1:{port}/{path}"))
                .unwrap()
                .send()
                .unwrap();
            assert_eq!(response.body(), b"ok");
        }
        server.join().unwrap();
    }

    #[test]
    fn pool_capacity_waits_without_opening_a_second_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            for _ in 0..2 {
                let _ = read_head(&mut stream);
                thread::sleep(Duration::from_millis(25));
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .unwrap();
                stream.flush().unwrap();
            }
        });

        let client = Client::builder()
            .max_connections(1)
            .max_connections_per_origin(1)
            .request_timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let first = client.clone();
        let second = client.clone();
        let one = may::go!(move || {
            first
                .get(&format!("http://127.0.0.1:{port}/one"))
                .unwrap()
                .send()
        });
        let two = may::go!(move || {
            second
                .get(&format!("http://127.0.0.1:{port}/two"))
                .unwrap()
                .send()
        });
        assert_eq!(one.join().unwrap().unwrap().body(), b"ok");
        assert_eq!(two.join().unwrap().unwrap().body(), b"ok");
        server.join().unwrap();
    }

    #[test]
    fn redirects_are_disabled_by_default() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_head(&mut stream);
            assert!(request.starts_with("GET /start HTTP/1.1\r\n"));
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });

        let response = test_client(RedirectPolicy::None)
            .get(&format!("http://127.0.0.1:{port}/start"))
            .unwrap()
            .send()
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        server.join().unwrap();
    }

    #[test]
    fn same_origin_redirect_resolves_relative_location() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            assert!(read_head(&mut stream).starts_with("GET /start HTTP/1.1\r\n"));
            stream
                .write_all(b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
            stream.flush().unwrap();
            assert!(read_head(&mut stream).starts_with("GET /final HTTP/1.1\r\n"));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\ndone!",
                )
                .unwrap();
        });

        let response = test_client(RedirectPolicy::SameOrigin { max_hops: 3 })
            .get(&format!("http://127.0.0.1:{port}/start"))
            .unwrap()
            .send()
            .unwrap();
        assert_eq!(response.body(), b"done!");
        assert_eq!(response.final_url().path(), "/final");
        server.join().unwrap();
    }

    #[test]
    fn cross_origin_redirect_strips_credentials() {
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        let target_port = target.local_addr().unwrap().port();
        let source = TcpListener::bind("127.0.0.1:0").unwrap();
        let source_port = source.local_addr().unwrap().port();
        let source_server = thread::spawn(move || {
            let (mut stream, _) = source.accept().unwrap();
            let _ = read_head(&mut stream);
            write!(
                stream,
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{target_port}/final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });
        let target_server = thread::spawn(move || {
            let (mut stream, _) = target.accept().unwrap();
            let request = read_head(&mut stream).to_ascii_lowercase();
            assert!(!request.contains("\r\nauthorization:"));
            assert!(!request.contains("\r\ncookie:"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });

        let response = test_client(RedirectPolicy::CrossOrigin {
            max_hops: 3,
            allow_https_downgrade: false,
        })
        .get(&format!("http://127.0.0.1:{source_port}/start"))
        .unwrap()
        .header(AUTHORIZATION, HeaderValue::from_static("Bearer secret"))
        .header(COOKIE, HeaderValue::from_static("session=secret"))
        .send()
        .unwrap();
        assert_eq!(response.body(), b"ok");
        source_server.join().unwrap();
        target_server.join().unwrap();
    }
}
