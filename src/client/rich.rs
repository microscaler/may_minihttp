//! Replay-aware requests, secure redirects, and bounded connection pooling.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{self, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::header::{
    AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HOST, LOCATION,
    PROXY_AUTHORIZATION, TRANSFER_ENCODING,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Version};
use may::sync::{Condvar, Mutex};
use rustls::ClientConfig;
use url::Url;

use super::{ClientEvent, ClientObserver, ObservedOrigin, Resolver, SystemResolver};
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
    max_response_header_bytes: usize,
    max_response_body: usize,
    redirect_policy: RedirectPolicy,
    tls_config: Option<Arc<ClientConfig>>,
    resolver: Arc<dyn Resolver>,
    observer: Option<Arc<dyn ClientObserver>>,
    sensitive_headers: HashSet<HeaderName>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        let sensitive_headers = [AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION]
            .into_iter()
            .collect();
        Self {
            max_connections: 64,
            max_connections_per_origin: 8,
            idle_timeout: Duration::from_secs(90),
            max_connection_lifetime: Duration::from_secs(15 * 60),
            connect_timeout: Duration::from_secs(10),
            io_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            max_response_header_bytes: super::response::DEFAULT_MAX_RESPONSE_HEADER_BYTES,
            max_response_body: DEFAULT_MAX_RESPONSE_BODY,
            redirect_policy: RedirectPolicy::None,
            tls_config: None,
            resolver: Arc::new(SystemResolver),
            observer: None,
            sensitive_headers,
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

    pub fn max_response_header_bytes(mut self, value: usize) -> Self {
        self.max_response_header_bytes = value;
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

    /// Inject a cached, static, or may-aware resolver.
    pub fn resolver(mut self, value: Arc<dyn Resolver>) -> Self {
        self.resolver = value;
        self
    }

    /// Observe sanitized request lifecycle events.
    pub fn observer(mut self, value: Arc<dyn ClientObserver>) -> Self {
        self.observer = Some(value);
        self
    }

    /// Mark an additional header for removal before a cross-origin redirect.
    pub fn sensitive_header(mut self, value: HeaderName) -> Self {
        self.sensitive_headers.insert(value);
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
        if self.max_response_body == 0 || self.max_response_header_bytes < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "response body limit must be non-zero and header limit at least four bytes",
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
                    max_response_header_bytes: self.max_response_header_bytes,
                    max_response_body: self.max_response_body,
                    redirect_policy: self.redirect_policy,
                    sensitive_headers: self.sensitive_headers,
                },
                tls_config,
                tls_profile,
                resolver: self.resolver,
                observer: self.observer,
                pool: Mutex::new(PoolState::default()),
                available: Condvar::new(),
                stats: ClientStatsInner::default(),
                next_request_id: AtomicU64::new(1),
            }),
        })
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

/// Monotonic operational counters for a [`Client`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientStats {
    pub connections_created: u64,
    pub connections_reused: u64,
    pub connections_discarded: u64,
    pub pool_waits: u64,
    pub stale_retries: u64,
    pub redirects_followed: u64,
}

/// Stable high-level classification for client failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientErrorKind {
    InvalidRequest,
    Dns,
    Connection,
    Tls,
    Timeout,
    Protocol,
    BodyTooLarge,
    BodyNotReplayable,
    Redirect,
    Io,
}

impl ClientErrorKind {
    fn classify(source: &io::Error) -> Self {
        let message = source.to_string().to_ascii_lowercase();
        match source.kind() {
            _ if message.contains("body is not replayable") => Self::BodyNotReplayable,
            io::ErrorKind::InvalidInput => Self::InvalidRequest,
            io::ErrorKind::AddrNotAvailable => Self::Dns,
            io::ErrorKind::TimedOut => Self::Timeout,
            io::ErrorKind::PermissionDenied => Self::Redirect,
            io::ErrorKind::InvalidData if message.contains("body exceeds") => Self::BodyTooLarge,
            io::ErrorKind::InvalidData if message.contains("redirect") => Self::Redirect,
            io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => Self::Protocol,
            io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::WriteZero => Self::Connection,
            io::ErrorKind::Other if message.contains("tls") || message.contains("certificate") => {
                Self::Tls
            }
            _ => Self::Io,
        }
    }
}

/// Classified error returned by [`RequestBuilder::send_typed`].
#[derive(Debug)]
pub struct ClientError {
    kind: ClientErrorKind,
    source: io::Error,
}

impl ClientError {
    pub fn kind(&self) -> ClientErrorKind {
        self.kind
    }

    pub fn into_io_error(self) -> io::Error {
        self.source
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.source)
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl From<io::Error> for ClientError {
    fn from(source: io::Error) -> Self {
        let kind = ClientErrorKind::classify(&source);
        Self { kind, source }
    }
}

#[derive(Default)]
struct ClientStatsInner {
    connections_created: AtomicU64,
    connections_reused: AtomicU64,
    connections_discarded: AtomicU64,
    pool_waits: AtomicU64,
    stale_retries: AtomicU64,
    redirects_followed: AtomicU64,
}

impl ClientStatsInner {
    fn snapshot(&self) -> ClientStats {
        ClientStats {
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_reused: self.connections_reused.load(Ordering::Relaxed),
            connections_discarded: self.connections_discarded.load(Ordering::Relaxed),
            pool_waits: self.pool_waits.load(Ordering::Relaxed),
            stale_retries: self.stale_retries.load(Ordering::Relaxed),
            redirects_followed: self.redirects_followed.load(Ordering::Relaxed),
        }
    }
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

#[derive(Debug, Clone, Copy)]
struct RequestTrace {
    request_id: u64,
    started: Instant,
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
            body: RequestBody::Empty,
            timeout: None,
        })
    }

    pub fn get(&self, url: &str) -> io::Result<RequestBuilder> {
        self.request(Method::GET, url)
    }

    pub fn post(&self, url: &str) -> io::Result<RequestBuilder> {
        self.request(Method::POST, url)
    }

    /// Snapshot connection-pool and redirect counters.
    pub fn stats(&self) -> ClientStats {
        self.inner.stats.snapshot()
    }

    fn execute(&self, request: RequestBuilder) -> io::Result<BufferedResponse> {
        let trace = self.begin_request(&request.method, &request.url);
        let result = self.execute_buffered(request, &trace);
        match &result {
            Ok(response) => self.inner.observe(ClientEvent::RequestCompleted {
                request_id: trace.request_id,
                status: response.status,
                total_duration: trace.started.elapsed(),
            }),
            Err(error) => self.observe_failure(&trace, error),
        }
        result
    }

    fn execute_buffered(
        &self,
        request: RequestBuilder,
        trace: &RequestTrace,
    ) -> io::Result<BufferedResponse> {
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
            let response =
                self.execute_once(&method, &url, &headers, &mut body, deadline, trace)?;
            let Some(max_hops) = policy.max_hops() else {
                return Ok(response);
            };
            if !is_redirect(response.status) {
                return Ok(response);
            }
            let Some(location) = response.headers.get(LOCATION) else {
                return Ok(response);
            };
            if matches!(
                response.status,
                StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND
            ) && !matches!(method, Method::GET | Method::HEAD)
            {
                return Ok(response);
            }
            if matches!(
                response.status,
                StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
            ) && !body.is_replayable()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "request body is not replayable across this redirect",
                ));
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
                for header in &self.inner.config.sensitive_headers {
                    headers.remove(header);
                }
            }
            self.inner.observe(ClientEvent::RedirectFollowed {
                request_id: trace.request_id,
                status: response.status,
                from: observed_url(&url),
                to: observed_url(&target),
            });
            if response.status == StatusCode::SEE_OTHER && method != Method::HEAD {
                method = Method::GET;
                body = RequestBody::Empty;
                headers.remove(CONTENT_TYPE);
                headers.remove(CONTENT_LENGTH);
                headers.remove(TRANSFER_ENCODING);
            }
            url = target;
            hops += 1;
            self.inner
                .stats
                .redirects_followed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn execute_streaming(&self, request: RequestBuilder) -> io::Result<StreamingResponse> {
        let trace = self.begin_request(&request.method, &request.url);
        let result = self.execute_streaming_inner(request, trace);
        if let Err(error) = &result {
            self.observe_failure(&trace, error);
        }
        result
    }

    fn execute_streaming_inner(
        &self,
        request: RequestBuilder,
        trace: RequestTrace,
    ) -> io::Result<StreamingResponse> {
        if self.inner.config.redirect_policy != RedirectPolicy::None {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "streaming responses require redirects to be disabled",
            ));
        }
        let deadline = Instant::now()
            .checked_add(request.timeout.unwrap_or(self.inner.config.request_timeout))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "request timeout overflow")
            })?;
        let mut body = request.body;
        self.execute_streaming_once(
            &request.method,
            &request.url,
            &request.headers,
            &mut body,
            deadline,
            &trace,
        )
    }

    fn execute_once(
        &self,
        method: &Method,
        url: &Url,
        headers: &HeaderMap,
        body: &mut RequestBody,
        deadline: Instant,
        trace: &RequestTrace,
    ) -> io::Result<BufferedResponse> {
        validate_body_method(method, body)?;
        let key = OriginKey::from_url(url, self.inner.tls_profile)?;
        let mut stale_retry_available = method_is_idempotent(method) && body.is_replayable();
        loop {
            let mut lease = self.checkout(&key, deadline, trace)?;
            let reused_idle_connection = lease.reused_idle_connection;
            let result = (|| {
                let mut response =
                    self.send_on_lease(&mut lease, method, url, headers, body, deadline)?;

                let status = response.status();
                self.inner.observe(ClientEvent::ResponseHeaders {
                    request_id: trace.request_id,
                    origin: observed_url(url),
                    status,
                    elapsed: trace.started.elapsed(),
                });
                let version = response.version();
                let response_headers = response.headers().clone();
                let reusable =
                    response_is_reusable(method, status, version, headers, &response_headers);
                let mut bytes = Vec::new();
                let limit = self.inner.config.max_response_body;
                // Keep body buffers off may's deliberately small coroutine stacks.
                let mut chunk = vec![0_u8; 8 * 1024];
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
                    lease.checkin();
                    return Ok(response);
                }
                Ok((response, false)) => {
                    drop(lease);
                    return Ok(response);
                }
                Err(error)
                    if reused_idle_connection
                        && stale_retry_available
                        && stale_connection_error(&error) =>
                {
                    stale_retry_available = false;
                    self.inner
                        .stats
                        .stale_retries
                        .fetch_add(1, Ordering::Relaxed);
                    self.inner.observe(ClientEvent::StaleConnectionRetried {
                        request_id: trace.request_id,
                        origin: observed_url(url),
                    });
                    drop(lease);
                    let _ = remaining(deadline)?;
                }
                Err(error) => {
                    drop(lease);
                    return Err(error);
                }
            }
        }
    }

    fn execute_streaming_once(
        &self,
        method: &Method,
        url: &Url,
        headers: &HeaderMap,
        body: &mut RequestBody,
        deadline: Instant,
        trace: &RequestTrace,
    ) -> io::Result<StreamingResponse> {
        validate_body_method(method, body)?;
        let key = OriginKey::from_url(url, self.inner.tls_profile)?;
        let mut stale_retry_available = method_is_idempotent(method) && body.is_replayable();
        loop {
            let mut lease = self.checkout(&key, deadline, trace)?;
            let reused_idle_connection = lease.reused_idle_connection;
            match self.send_on_lease(&mut lease, method, url, headers, body, deadline) {
                Ok(response) => {
                    let status = response.status();
                    self.inner.observe(ClientEvent::ResponseHeaders {
                        request_id: trace.request_id,
                        origin: observed_url(url),
                        status,
                        elapsed: trace.started.elapsed(),
                    });
                    let version = response.version();
                    let response_headers = response.headers().clone();
                    let reusable =
                        response_is_reusable(method, status, version, headers, &response_headers);
                    let mut streaming = StreamingResponse {
                        response: Some(response),
                        lease: Some(lease),
                        reusable,
                        deadline,
                        io_timeout: self.inner.config.io_timeout,
                        status,
                        version,
                        headers: response_headers,
                        final_url: url.clone(),
                        inner: Arc::clone(&self.inner),
                        trace: *trace,
                        terminal_observed: false,
                    };
                    if streaming
                        .response
                        .as_ref()
                        .is_some_and(super::Response::body_complete)
                    {
                        streaming.complete();
                    }
                    return Ok(streaming);
                }
                Err(error)
                    if reused_idle_connection
                        && stale_retry_available
                        && stale_connection_error(&error) =>
                {
                    stale_retry_available = false;
                    self.inner
                        .stats
                        .stale_retries
                        .fetch_add(1, Ordering::Relaxed);
                    self.inner.observe(ClientEvent::StaleConnectionRetried {
                        request_id: trace.request_id,
                        origin: observed_url(url),
                    });
                    drop(lease);
                    let _ = remaining(deadline)?;
                }
                Err(error) => {
                    drop(lease);
                    return Err(error);
                }
            }
        }
    }

    fn send_on_lease(
        &self,
        lease: &mut PoolLease,
        method: &Method,
        url: &Url,
        headers: &HeaderMap,
        body: &mut RequestBody,
        deadline: Instant,
    ) -> io::Result<super::Response> {
        if headers.contains_key(HOST) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Host is derived from the request URL and cannot be overridden",
            ));
        }
        if matches!(*method, Method::GET | Method::HEAD) && !matches!(body, RequestBody::Empty) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request bodies are not supported for GET or HEAD",
            ));
        }
        let initial_remaining = remaining(deadline)?;
        lease
            .connection_mut()
            .client
            .set_timeout(Some(self.inner.config.io_timeout.min(initial_remaining)));
        lease
            .connection_mut()
            .client
            .set_max_response_header_bytes(self.inner.config.max_response_header_bytes)?;
        let target = origin_form(url)?;
        let mut request = lease
            .connection_mut()
            .client
            .new_request(method.clone(), target);
        for (name, value) in headers {
            request.headers_mut().append(name, value.clone());
        }
        match body {
            RequestBody::Empty => lease.connection_mut().client.send_request(request),
            RequestBody::Bytes(bytes) => {
                request.send(bytes)?;
                lease.connection_mut().client.send_request(request)
            }
            RequestBody::Multipart(form) => {
                request.send_multipart(form)?;
                lease.connection_mut().client.send_request(request)
            }
            RequestBody::Reader {
                reader,
                content_length,
            } => {
                if matches!(*method, Method::GET | Method::HEAD) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "streaming request bodies are not supported for GET or HEAD",
                    ));
                }
                let mut reader = reader.take().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "request body is not replayable and was already consumed",
                    )
                })?;
                request.send_reader(&mut *reader, *content_length)?;
                lease.connection_mut().client.send_request(request)
            }
        }
    }

    fn begin_request(&self, method: &Method, url: &Url) -> RequestTrace {
        let trace = RequestTrace {
            request_id: self.inner.next_request_id.fetch_add(1, Ordering::Relaxed),
            started: Instant::now(),
        };
        self.inner.observe(ClientEvent::RequestStarted {
            request_id: trace.request_id,
            method,
            origin: observed_url(url),
        });
        trace
    }

    fn observe_failure(&self, trace: &RequestTrace, error: &io::Error) {
        self.inner.observe(ClientEvent::RequestFailed {
            request_id: trace.request_id,
            error: ClientErrorKind::classify(error),
            total_duration: trace.started.elapsed(),
        });
    }

    fn checkout(
        &self,
        key: &OriginKey,
        deadline: Instant,
        trace: &RequestTrace,
    ) -> io::Result<PoolLease> {
        loop {
            let now = Instant::now();
            let mut state = self
                .inner
                .pool
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.purge_expired(
                now,
                self.inner.config.idle_timeout,
                self.inner.config.max_connection_lifetime,
            );
            if let Some(connections) = state.idle.get_mut(key) {
                if let Some(connection) = connections.pop() {
                    self.inner
                        .stats
                        .connections_reused
                        .fetch_add(1, Ordering::Relaxed);
                    let lease = PoolLease::with_connection(
                        Arc::clone(&self.inner),
                        key.clone(),
                        connection,
                        trace.request_id,
                    );
                    drop(state);
                    self.inner.observe(ClientEvent::ConnectionReused {
                        request_id: trace.request_id,
                        origin: observed_key(key),
                    });
                    return Ok(lease);
                }
            }
            let per_origin = state.per_origin.get(key).copied().unwrap_or(0);
            if state.total < self.inner.config.max_connections
                && per_origin < self.inner.config.max_connections_per_origin
            {
                state.total += 1;
                *state.per_origin.entry(key.clone()).or_default() += 1;
                drop(state);

                let mut lease =
                    PoolLease::reserved(Arc::clone(&self.inner), key.clone(), trace.request_id);

                let connect_budget = self.inner.config.connect_timeout.min(remaining(deadline)?);
                let connect_deadline =
                    Instant::now().checked_add(connect_budget).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "connect timeout overflow")
                    })?;
                let dns_started = Instant::now();
                let resolution = self.inner.resolver.resolve_with_deadline(
                    &key.host,
                    key.port,
                    connect_deadline,
                );
                self.inner.observe(ClientEvent::DnsCompleted {
                    request_id: trace.request_id,
                    origin: observed_key(key),
                    duration: dns_started.elapsed(),
                    address_count: resolution.as_ref().map_or(0, |value| value.addresses.len()),
                    source: resolution.as_ref().ok().map(|value| value.source),
                    error: resolution.as_ref().err().map(ClientErrorKind::classify),
                });
                let addresses = resolution?.addresses;
                let timeout = connect_deadline
                    .checked_duration_since(Instant::now())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "DNS resolution exhausted the connect deadline",
                        )
                    })?;
                let origin = key.connect_url();
                let connect_started = Instant::now();
                let client = HttpClient::from_url_with_resolved_options(
                    &origin,
                    Arc::clone(&self.inner.tls_config),
                    timeout,
                    &addresses,
                );
                self.inner.observe(ClientEvent::ConnectionCompleted {
                    request_id: trace.request_id,
                    origin: observed_key(key),
                    duration: connect_started.elapsed(),
                    tls: key.scheme == "https",
                    error: client.as_ref().err().map(ClientErrorKind::classify),
                });
                let client = client?;
                lease.connection = Some(PooledConnection {
                    client,
                    created: Instant::now(),
                    idle_since: Instant::now(),
                });
                self.inner
                    .stats
                    .connections_created
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(lease);
            }

            let wait = remaining(deadline)?;
            self.inner.stats.pool_waits.fetch_add(1, Ordering::Relaxed);
            let wait_started = Instant::now();
            let (state_after_wait, timeout) = self
                .inner
                .available
                .wait_timeout(state, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            drop(state_after_wait);
            self.inner.observe(ClientEvent::PoolWaited {
                request_id: trace.request_id,
                origin: observed_key(key),
                duration: wait_started.elapsed(),
                timed_out: timeout.timed_out(),
            });
            if timeout.timed_out() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for an HTTP connection",
                ));
            }
        }
    }
}

/// Request builder whose body explicitly records whether it can be replayed.
pub struct RequestBuilder {
    client: Client,
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: RequestBody,
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
        self.body = RequestBody::Bytes(Arc::from(value.into()));
        self
    }

    pub fn multipart(mut self, value: MultipartForm) -> Self {
        self.body = RequestBody::Multipart(value);
        self
    }

    /// Attach a single-use streaming request body.
    ///
    /// The reader must itself be coroutine-safe. This body is never retried and cannot be replayed
    /// across a 307/308 redirect.
    pub fn reader(mut self, value: impl Read + Send + 'static, content_length: usize) -> Self {
        self.body = RequestBody::Reader {
            reader: Some(Box::new(value)),
            content_length,
        };
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
        self.body = RequestBody::Bytes(Arc::from(body));
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

    /// Send with a stable high-level error classification while retaining the underlying I/O error.
    pub fn send_typed(self) -> Result<BufferedResponse, ClientError> {
        self.send().map_err(ClientError::from)
    }

    /// Send without buffering the response body.
    ///
    /// The connection remains checked out until the body reaches EOF. Dropping the response before
    /// EOF discards that connection without performing blocking drain I/O. Redirect following must
    /// be disabled because a streaming body cannot safely hide redirect consumption and replay.
    pub fn send_streaming(self) -> io::Result<StreamingResponse> {
        let client = self.client.clone();
        client.execute_streaming(self)
    }

    /// Streaming variant with stable high-level error classification.
    pub fn send_streaming_typed(self) -> Result<StreamingResponse, ClientError> {
        self.send_streaming().map_err(ClientError::from)
    }
}

enum RequestBody {
    Empty,
    Bytes(Arc<[u8]>),
    Multipart(MultipartForm),
    Reader {
        reader: Option<Box<dyn Read + Send>>,
        content_length: usize,
    },
}

impl RequestBody {
    fn is_replayable(&self) -> bool {
        !matches!(self, Self::Reader { .. })
    }
}

/// Fully buffered response. Buffering makes pool check-in unambiguous and redirect replay safe.
#[derive(Debug)]
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

/// Streaming response that owns its connection-pool lease.
///
/// Reading to EOF returns a reusable HTTP/1.x connection to the pool. Any read error, request
/// deadline, or early drop discards the connection without trying to drain the body in `Drop`.
pub struct StreamingResponse {
    response: Option<super::Response>,
    lease: Option<PoolLease>,
    reusable: bool,
    deadline: Instant,
    io_timeout: Duration,
    status: StatusCode,
    version: Version,
    headers: HeaderMap,
    final_url: Url,
    inner: Arc<ClientInner>,
    trace: RequestTrace,
    terminal_observed: bool,
}

impl fmt::Debug for StreamingResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamingResponse")
            .field("status", &self.status)
            .field("version", &self.version)
            .field("headers", &self.headers)
            .field("final_url", &self.final_url)
            .field("complete", &self.response.is_none())
            .finish()
    }
}

impl StreamingResponse {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn final_url(&self) -> &Url {
        &self.final_url
    }

    fn complete(&mut self) {
        drop(self.response.take());
        if let Some(lease) = self.lease.take() {
            if self.reusable {
                lease.checkin();
            }
        }
        if !self.terminal_observed {
            self.inner.observe(ClientEvent::RequestCompleted {
                request_id: self.trace.request_id,
                status: self.status,
                total_duration: self.trace.started.elapsed(),
            });
            self.terminal_observed = true;
        }
    }

    fn discard_connection(&mut self) {
        if let Some(response) = self.response.as_mut() {
            response.abandon_body();
        }
        drop(self.response.take());
        drop(self.lease.take());
    }

    fn fail(&mut self, error: &io::Error) {
        self.discard_connection();
        if !self.terminal_observed {
            self.inner.observe(ClientEvent::RequestFailed {
                request_id: self.trace.request_id,
                error: ClientErrorKind::classify(error),
                total_duration: self.trace.started.elapsed(),
            });
            self.terminal_observed = true;
        }
    }

    fn abandon(&mut self) {
        let incomplete = self.response.is_some();
        self.discard_connection();
        if incomplete && !self.terminal_observed {
            self.inner.observe(ClientEvent::RequestAbandoned {
                request_id: self.trace.request_id,
                status: self.status,
                total_duration: self.trace.started.elapsed(),
            });
            self.terminal_observed = true;
        }
    }
}

impl Read for StreamingResponse {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let timeout = match remaining(self.deadline) {
            Ok(remaining) => self.io_timeout.min(remaining),
            Err(error) => {
                self.fail(&error);
                return Err(error);
            }
        };
        let Some(response) = self.response.as_mut() else {
            return Ok(0);
        };
        if let Err(error) = response.set_timeout(Some(timeout)) {
            self.fail(&error);
            return Err(error);
        }
        match response.read(buffer) {
            Ok(read) => {
                let complete = response.body_complete();
                if complete {
                    self.complete();
                }
                Ok(read)
            }
            Err(error) => {
                self.fail(&error);
                Err(error)
            }
        }
    }
}

impl Drop for StreamingResponse {
    fn drop(&mut self) {
        self.abandon();
    }
}

struct ClientInner {
    config: ClientConfigValues,
    tls_config: Arc<ClientConfig>,
    tls_profile: usize,
    resolver: Arc<dyn Resolver>,
    observer: Option<Arc<dyn ClientObserver>>,
    pool: Mutex<PoolState>,
    available: Condvar,
    stats: ClientStatsInner,
    next_request_id: AtomicU64,
}

impl ClientInner {
    fn observe(&self, event: ClientEvent<'_>) {
        if let Some(observer) = &self.observer {
            observer.observe(event);
        }
    }
}

struct ClientConfigValues {
    max_connections: usize,
    max_connections_per_origin: usize,
    idle_timeout: Duration,
    max_connection_lifetime: Duration,
    connect_timeout: Duration,
    io_timeout: Duration,
    request_timeout: Duration,
    max_response_header_bytes: usize,
    max_response_body: usize,
    redirect_policy: RedirectPolicy,
    sensitive_headers: HashSet<HeaderName>,
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

/// Owns one accounted pool slot. Dropping it from any path, including coroutine cancellation,
/// releases capacity unless the connection was successfully returned to the idle pool.
struct PoolLease {
    inner: Arc<ClientInner>,
    key: OriginKey,
    request_id: u64,
    connection: Option<PooledConnection>,
    accounted: bool,
    reused_idle_connection: bool,
}

impl PoolLease {
    fn reserved(inner: Arc<ClientInner>, key: OriginKey, request_id: u64) -> Self {
        Self {
            inner,
            key,
            request_id,
            connection: None,
            accounted: true,
            reused_idle_connection: false,
        }
    }

    fn with_connection(
        inner: Arc<ClientInner>,
        key: OriginKey,
        connection: PooledConnection,
        request_id: u64,
    ) -> Self {
        Self {
            inner,
            key,
            request_id,
            connection: Some(connection),
            accounted: true,
            reused_idle_connection: true,
        }
    }

    fn connection_mut(&mut self) -> &mut PooledConnection {
        self.connection
            .as_mut()
            .expect("connected pool lease must contain a connection")
    }

    fn checkin(mut self) {
        let now = Instant::now();
        if now.duration_since(self.connection_mut().created)
            >= self.inner.config.max_connection_lifetime
        {
            return;
        }
        self.connection_mut().idle_since = now;
        let connection = self
            .connection
            .take()
            .expect("connected pool lease must contain a connection");
        let mut state = self
            .inner
            .pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .idle
            .entry(self.key.clone())
            .or_default()
            .push(connection);
        self.accounted = false;
        drop(state);
        self.inner.available.notify_one();
    }
}

impl Drop for PoolLease {
    fn drop(&mut self) {
        if !self.accounted {
            return;
        }
        let discarded = self.connection.is_some();
        if discarded {
            self.inner
                .stats
                .connections_discarded
                .fetch_add(1, Ordering::Relaxed);
        }
        let mut state = self
            .inner
            .pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.total = state.total.saturating_sub(1);
        if let Some(count) = state.per_origin.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.per_origin.remove(&self.key);
            }
        }
        self.accounted = false;
        drop(state);
        self.inner.available.notify_one();
        if discarded {
            self.inner.observe(ClientEvent::ConnectionDiscarded {
                request_id: self.request_id,
                origin: observed_key(&self.key),
            });
        }
    }
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

fn observed_url(url: &Url) -> ObservedOrigin<'_> {
    ObservedOrigin {
        scheme: url.scheme(),
        host: url.host_str().unwrap_or_default(),
        port: url.port_or_known_default().unwrap_or_default(),
    }
}

fn observed_key(key: &OriginKey) -> ObservedOrigin<'_> {
    ObservedOrigin {
        scheme: &key.scheme,
        host: &key.host,
        port: key.port,
    }
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
    if !target.username().is_empty() || target.password().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "redirect target must not contain URL credentials",
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

fn method_is_idempotent(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE | Method::OPTIONS | Method::TRACE
    )
}

fn validate_body_method(method: &Method, body: &RequestBody) -> io::Result<()> {
    if matches!(*method, Method::GET | Method::HEAD) && !matches!(body, RequestBody::Empty) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "request bodies are not supported for GET or HEAD",
        ));
    }
    Ok(())
}

fn stale_connection_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::WriteZero
    )
}

fn response_is_reusable(
    method: &Method,
    status: StatusCode,
    version: Version,
    request_headers: &HeaderMap,
    headers: &HeaderMap,
) -> bool {
    if method == Method::CONNECT || status == StatusCode::SWITCHING_PROTOCOLS {
        return false;
    }
    let close = header_has_token(request_headers, CONNECTION, "close")
        || header_has_token(headers, CONNECTION, "close");
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
    use crate::client::ServiceResolver;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Mutex as StdMutex;
    use std::thread;

    struct StaticResolver(SocketAddr);

    impl Resolver for StaticResolver {
        fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
            Ok(vec![self.0])
        }
    }

    #[derive(Default)]
    struct RecordingObserver(StdMutex<Vec<String>>);

    impl RecordingObserver {
        fn events(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl ClientObserver for RecordingObserver {
        fn observe(&self, event: ClientEvent<'_>) {
            let value = match event {
                ClientEvent::RequestStarted {
                    request_id,
                    method,
                    origin,
                } => format!(
                    "start:{request_id}:{method}:{}://{}:{}",
                    origin.scheme, origin.host, origin.port
                ),
                ClientEvent::PoolWaited {
                    request_id,
                    timed_out,
                    ..
                } => format!("wait:{request_id}:{timed_out}"),
                ClientEvent::DnsCompleted {
                    request_id,
                    address_count,
                    source,
                    error,
                    ..
                } => format!("dns:{request_id}:{address_count}:{source:?}:{error:?}"),
                ClientEvent::ConnectionCompleted {
                    request_id,
                    tls,
                    error,
                    ..
                } => format!("connect:{request_id}:{tls}:{error:?}"),
                ClientEvent::ConnectionReused { request_id, .. } => {
                    format!("reuse:{request_id}")
                }
                ClientEvent::ConnectionDiscarded { request_id, .. } => {
                    format!("discard:{request_id}")
                }
                ClientEvent::ResponseHeaders {
                    request_id, status, ..
                } => format!("headers:{request_id}:{}", status.as_u16()),
                ClientEvent::RedirectFollowed {
                    request_id, status, ..
                } => format!("redirect:{request_id}:{}", status.as_u16()),
                ClientEvent::StaleConnectionRetried { request_id, .. } => {
                    format!("retry:{request_id}")
                }
                ClientEvent::RequestCompleted {
                    request_id, status, ..
                } => format!("complete:{request_id}:{}", status.as_u16()),
                ClientEvent::RequestFailed {
                    request_id, error, ..
                } => format!("failed:{request_id}:{error:?}"),
                ClientEvent::RequestAbandoned {
                    request_id, status, ..
                } => format!("abandoned:{request_id}:{}", status.as_u16()),
            };
            self.0.lock().unwrap().push(value);
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
    fn pool_expiry_uses_injected_instant_for_idle_and_lifetime_limits() {
        fn connection(created: Instant, idle_since: Instant) -> PooledConnection {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let accept = thread::spawn(move || listener.accept().unwrap());
            let client = HttpClient::connect(address).unwrap();
            let _ = accept.join().unwrap();
            PooledConnection {
                client,
                created,
                idle_since,
            }
        }

        let now = Instant::now();
        let key = OriginKey::from_url(&Url::parse("http://example.com/").unwrap(), 0).unwrap();
        let mut state = PoolState::default();
        state.total = 2;
        state.per_origin.insert(key.clone(), 2);
        state.idle.insert(
            key.clone(),
            vec![
                connection(now - Duration::from_secs(5), now - Duration::from_secs(3)),
                connection(now - Duration::from_secs(30), now - Duration::from_secs(1)),
            ],
        );

        state.purge_expired(now, Duration::from_secs(2), Duration::from_secs(20));
        assert_eq!(state.total, 0);
        assert!(!state.per_origin.contains_key(&key));
        assert!(!state.idle.contains_key(&key));
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
    fn get_body_is_rejected_before_connection_attempt() {
        let error = test_client(RedirectPolicy::None)
            .get("http://127.0.0.1:9/")
            .unwrap()
            .body(b"not allowed".to_vec())
            .send()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("GET or HEAD"));
    }

    #[test]
    fn typed_errors_preserve_source_and_classification() {
        let error = ClientError::from(io::Error::new(
            io::ErrorKind::InvalidData,
            "HTTP response body exceeds configured limit",
        ));
        assert_eq!(error.kind(), ClientErrorKind::BodyTooLarge);
        assert!(error.to_string().contains("body exceeds"));
    }

    #[test]
    fn injected_resolver_controls_connection_addresses() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_head(&mut stream).to_ascii_lowercase();
            assert!(request.contains("\r\nhost: service.invalid:"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });
        let client = Client::builder()
            .resolver(Arc::new(StaticResolver(address)))
            .build()
            .unwrap();
        let response = client
            .get(&format!("http://service.invalid:{}/", address.port()))
            .unwrap()
            .send()
            .unwrap();
        assert_eq!(response.body(), b"ok");
        server.join().unwrap();
    }

    #[test]
    fn service_resolver_preserves_logical_host_and_reports_source() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_head(&mut stream).to_ascii_lowercase();
            assert!(request.contains("\r\nhost: identity.internal:"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });
        let resolver = Arc::new(ServiceResolver::default());
        let unavailable = SocketAddr::from(([127, 0, 0, 2], address.port()));
        resolver
            .update(
                "identity.internal",
                address.port(),
                vec![unavailable, address],
            )
            .unwrap();
        let observer = Arc::new(RecordingObserver::default());
        let client = Client::builder()
            .resolver(resolver)
            .observer(observer.clone())
            .build()
            .unwrap();

        assert_eq!(
            client
                .get(&format!(
                    "http://identity.internal:{}/health",
                    address.port()
                ))
                .unwrap()
                .send()
                .unwrap()
                .body(),
            b"ok"
        );
        server.join().unwrap();
        assert!(observer
            .events()
            .iter()
            .any(|event| event == "dns:1:2:Some(ServiceRegistry):None"));
    }

    #[test]
    fn service_resolver_preserves_logical_tls_identity() {
        use rcgen::{generate_simple_self_signed, CertifiedKey};
        use rustls::pki_types::PrivatePkcs8KeyDer;
        use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned};

        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["identity.internal".to_owned()]).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server_config = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
            )
            .unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let client_config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let connection = ServerConnection::new(Arc::new(server_config)).unwrap();
            let mut tls = StreamOwned::new(connection, stream);
            let request = read_head(&mut tls).to_ascii_lowercase();
            assert!(request.contains("\r\nhost: identity.internal:"));
            tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
            tls.flush().unwrap();
        });
        let resolver = Arc::new(ServiceResolver::default());
        resolver
            .update("identity.internal", address.port(), vec![address])
            .unwrap();
        let client = Client::builder()
            .resolver(resolver)
            .tls_config(Arc::new(client_config))
            .build()
            .unwrap();

        let response = client
            .get(&format!(
                "https://identity.internal:{}/health",
                address.port()
            ))
            .unwrap()
            .send()
            .unwrap();
        assert_eq!(response.body(), b"ok");
        server.join().unwrap();
    }

    #[test]
    fn resolver_time_counts_against_connect_deadline() {
        struct SlowResolver;
        impl Resolver for SlowResolver {
            fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
                thread::sleep(Duration::from_millis(30));
                Ok(vec!["127.0.0.1:9".parse().unwrap()])
            }
        }

        let client = Client::builder()
            .resolver(Arc::new(SlowResolver))
            .connect_timeout(Duration::from_millis(5))
            .build()
            .unwrap();
        let error = client
            .get("http://slow.invalid/")
            .unwrap()
            .send()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
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
        let stats = client.stats();
        assert_eq!(stats.connections_created, 1);
        assert_eq!(stats.connections_reused, 1);
        server.join().unwrap();
    }

    #[test]
    fn observer_records_sanitized_new_and_reused_request_lifecycles() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            for _ in 0..2 {
                let _ = read_head(&mut stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .unwrap();
                stream.flush().unwrap();
            }
        });
        let observer = Arc::new(RecordingObserver::default());
        let client = Client::builder()
            .observer(observer.clone())
            .build()
            .unwrap();

        for path in ["one?token=do-not-observe", "two"] {
            assert_eq!(
                client
                    .get(&format!("http://127.0.0.1:{port}/{path}"))
                    .unwrap()
                    .header(AUTHORIZATION, HeaderValue::from_static("Bearer secret"))
                    .send()
                    .unwrap()
                    .body(),
                b"ok"
            );
        }
        server.join().unwrap();

        let events = observer.events();
        assert_eq!(events[0], format!("start:1:GET:http://127.0.0.1:{port}"));
        assert!(events
            .iter()
            .any(|event| event == "dns:1:1:Some(Resolver):None"));
        assert!(events.iter().any(|event| event == "connect:1:false:None"));
        assert!(events.iter().any(|event| event == "complete:1:200"));
        assert!(events.iter().any(|event| event == "reuse:2"));
        assert!(events.iter().any(|event| event == "complete:2:200"));
        let joined = events.join("|");
        assert!(!joined.contains("do-not-observe"));
        assert!(!joined.contains("Bearer"));
        assert!(!joined.contains("secret"));
    }

    #[test]
    fn fully_consumed_streaming_response_reuses_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            for expected in ["/stream", "/after-stream"] {
                let request = read_head(&mut stream);
                assert!(request.starts_with(&format!("GET {expected} HTTP/1.1\r\n")));
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata")
                    .unwrap();
                stream.flush().unwrap();
            }
        });

        let client = test_client(RedirectPolicy::None);
        let mut response = client
            .get(&format!("http://127.0.0.1:{port}/stream"))
            .unwrap()
            .send_streaming()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = [0_u8; 4];
        response.read_exact(&mut body).unwrap();
        assert_eq!(&body, b"data");
        drop(response);

        assert_eq!(
            client
                .get(&format!("http://127.0.0.1:{port}/after-stream"))
                .unwrap()
                .send()
                .unwrap()
                .body(),
            b"data"
        );
        let stats = client.stats();
        assert_eq!(stats.connections_created, 1);
        assert_eq!(stats.connections_reused, 1);
        server.join().unwrap();
    }

    #[test]
    fn partial_streaming_response_drop_discards_connection_without_drain() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut partial, _) = listener.accept().unwrap();
            assert!(read_head(&mut partial).starts_with("GET /partial HTTP/1.1\r\n"));
            partial
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabcdefghij")
                .unwrap();
            partial.flush().unwrap();

            let (mut replacement, _) = listener.accept().unwrap();
            assert!(read_head(&mut replacement).starts_with("GET /replacement HTTP/1.1\r\n"));
            replacement
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });

        let client = test_client(RedirectPolicy::None);
        let mut response = client
            .get(&format!("http://127.0.0.1:{port}/partial"))
            .unwrap()
            .send_streaming()
            .unwrap();
        let mut prefix = [0_u8; 2];
        response.read_exact(&mut prefix).unwrap();
        assert_eq!(&prefix, b"ab");
        drop(response);

        assert_eq!(
            client
                .get(&format!("http://127.0.0.1:{port}/replacement"))
                .unwrap()
                .send()
                .unwrap()
                .body(),
            b"ok"
        );
        let stats = client.stats();
        assert_eq!(stats.connections_created, 2);
        // One discard is the partial response; the other is the replacement's explicit close.
        assert_eq!(stats.connections_discarded, 2);
        server.join().unwrap();
    }

    #[test]
    fn observer_marks_partial_streaming_response_abandoned() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_head(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata")
                .unwrap();
        });
        let observer = Arc::new(RecordingObserver::default());
        let client = Client::builder()
            .observer(observer.clone())
            .build()
            .unwrap();
        let mut response = client
            .get(&format!("http://127.0.0.1:{port}/stream"))
            .unwrap()
            .send_streaming()
            .unwrap();
        let mut byte = [0_u8; 1];
        response.read_exact(&mut byte).unwrap();
        drop(response);
        server.join().unwrap();

        let events = observer.events();
        assert!(events.iter().any(|event| event == "discard:1"));
        assert!(events.iter().any(|event| event == "abandoned:1:200"));
    }

    #[test]
    fn observer_records_connection_failure_once() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let observer = Arc::new(RecordingObserver::default());
        let client = Client::builder()
            .resolver(Arc::new(StaticResolver(address)))
            .observer(observer.clone())
            .build()
            .unwrap();

        let error = client
            .get(&format!("http://service.invalid:{}/", address.port()))
            .unwrap()
            .send()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        let events = observer.events();
        assert!(events
            .iter()
            .any(|event| event == "connect:1:false:Some(Connection)"));
        assert_eq!(
            events
                .iter()
                .filter(|event| *event == "failed:1:Connection")
                .count(),
            1
        );
    }

    #[test]
    fn streaming_response_rejects_implicit_redirect_following() {
        let client = test_client(RedirectPolicy::SameOrigin { max_hops: 1 });
        let error = client
            .get("http://127.0.0.1:9/")
            .unwrap()
            .send_streaming()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("redirects"));
    }

    #[test]
    fn stale_idle_connection_is_replaced_once_for_get() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stale, _) = listener.accept().unwrap();
            assert!(read_head(&mut stale).starts_with("GET /first HTTP/1.1\r\n"));
            stale
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .unwrap();
            stale.flush().unwrap();
            drop(stale);

            let (mut replacement, _) = listener.accept().unwrap();
            assert!(read_head(&mut replacement).starts_with("GET /second HTTP/1.1\r\n"));
            replacement
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nfresh",
                )
                .unwrap();
        });

        let observer = Arc::new(RecordingObserver::default());
        let client = Client::builder()
            .redirect_policy(RedirectPolicy::None)
            .request_timeout(Duration::from_secs(2))
            .observer(observer.clone())
            .build()
            .unwrap();
        assert_eq!(
            client
                .get(&format!("http://127.0.0.1:{port}/first"))
                .unwrap()
                .send()
                .unwrap()
                .body(),
            b"ok"
        );
        assert_eq!(
            client
                .get(&format!("http://127.0.0.1:{port}/second"))
                .unwrap()
                .send()
                .unwrap()
                .body(),
            b"fresh"
        );
        assert_eq!(client.stats().stale_retries, 1);
        assert!(observer.events().iter().any(|event| event == "retry:2"));
        server.join().unwrap();
    }

    #[test]
    fn stale_idle_connection_does_not_retry_post() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stale, _) = listener.accept().unwrap();
            let _ = read_head(&mut stale);
            stale
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .unwrap();
            stale.flush().unwrap();
            drop(stale);

            thread::sleep(Duration::from_millis(150));
            listener.set_nonblocking(true).unwrap();
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                io::ErrorKind::WouldBlock,
                "POST unexpectedly opened a retry connection"
            );
        });

        let client = test_client(RedirectPolicy::None);
        client
            .get(&format!("http://127.0.0.1:{port}/prime"))
            .unwrap()
            .send()
            .unwrap();
        let error = client
            .post(&format!("http://127.0.0.1:{port}/must-not-retry"))
            .unwrap()
            .body(b"side effect".to_vec())
            .send()
            .unwrap_err();
        assert!(stale_connection_error(&error));
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

        let observer = Arc::new(RecordingObserver::default());
        let client = Client::builder()
            .max_connections(1)
            .max_connections_per_origin(1)
            .request_timeout(Duration::from_secs(2))
            .observer(observer.clone())
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
        assert!(observer
            .events()
            .iter()
            .any(|event| event.starts_with("wait:")));
        server.join().unwrap();
    }

    #[test]
    fn cancelled_request_releases_pool_capacity() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (blocked_tx, blocked_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (mut blocked, _) = listener.accept().unwrap();
            let _ = read_head(&mut blocked);
            blocked_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(100));
            drop(blocked);

            let (mut replacement, _) = listener.accept().unwrap();
            let request = read_head(&mut replacement);
            assert!(request.starts_with("GET /after-cancel HTTP/1.1\r\n"));
            replacement
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });

        let client = Client::builder()
            .max_connections(1)
            .max_connections_per_origin(1)
            .request_timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let blocked_client = client.clone();
        let blocked = may::go!(move || {
            blocked_client
                .get(&format!("http://127.0.0.1:{port}/blocked"))
                .unwrap()
                .send()
        });
        blocked_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        unsafe { blocked.coroutine().cancel() };
        assert!(blocked.join().is_err());

        let response = client
            .get(&format!("http://127.0.0.1:{port}/after-cancel"))
            .unwrap()
            .send()
            .unwrap();
        assert_eq!(response.body(), b"ok");
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

        let observer = Arc::new(RecordingObserver::default());
        let client = Client::builder()
            .redirect_policy(RedirectPolicy::SameOrigin { max_hops: 3 })
            .request_timeout(Duration::from_secs(2))
            .observer(observer.clone())
            .build()
            .unwrap();
        let response = client
            .get(&format!("http://127.0.0.1:{port}/start"))
            .unwrap()
            .send()
            .unwrap();
        assert_eq!(response.body(), b"done!");
        assert_eq!(response.final_url().path(), "/final");
        let events = observer.events();
        let redirect = events
            .iter()
            .position(|event| event == "redirect:1:302")
            .unwrap();
        let completed = events
            .iter()
            .position(|event| event == "complete:1:200")
            .unwrap();
        assert!(redirect < completed);
        server.join().unwrap();
    }

    #[test]
    fn temporary_redirect_replays_buffered_post_body() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            for (path, redirect) in [("/start", true), ("/final", false)] {
                let head = read_head(&mut stream);
                assert!(head.starts_with(&format!("POST {path} HTTP/1.1\r\n")));
                assert!(head.to_ascii_lowercase().contains("content-length: 4\r\n"));
                let mut body = [0_u8; 4];
                stream.read_exact(&mut body).unwrap();
                assert_eq!(&body, b"data");
                if redirect {
                    stream
                        .write_all(
                            b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /final\r\nContent-Length: 0\r\n\r\n",
                        )
                        .unwrap();
                    stream.flush().unwrap();
                } else {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )
                        .unwrap();
                }
            }
        });

        let response = test_client(RedirectPolicy::SameOrigin { max_hops: 2 })
            .post(&format!("http://127.0.0.1:{port}/start"))
            .unwrap()
            .body(b"data".to_vec())
            .send()
            .unwrap();
        assert_eq!(response.body(), b"ok");
        server.join().unwrap();
    }

    #[test]
    fn streaming_reader_is_sent_once_and_rejected_for_replay_redirect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let head = read_head(&mut stream);
            assert!(head.starts_with("POST /start HTTP/1.1\r\n"));
            assert!(head.to_ascii_lowercase().contains("content-length: 4\r\n"));
            let mut body = [0_u8; 4];
            stream.read_exact(&mut body).unwrap();
            assert_eq!(&body, b"data");
            stream
                .write_all(
                    b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /again\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });

        let error = test_client(RedirectPolicy::SameOrigin { max_hops: 2 })
            .post(&format!("http://127.0.0.1:{port}/start"))
            .unwrap()
            .reader(std::io::Cursor::new(b"data".to_vec()), 4)
            .send_typed()
            .unwrap_err();
        assert_eq!(error.kind(), ClientErrorKind::BodyNotReplayable);
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
            assert!(!request.contains("\r\nx-secret:"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });

        let response = Client::builder()
            .redirect_policy(RedirectPolicy::CrossOrigin {
                max_hops: 3,
                allow_https_downgrade: false,
            })
            .sensitive_header(HeaderName::from_static("x-secret"))
            .request_timeout(Duration::from_secs(2))
            .build()
            .unwrap()
            .get(&format!("http://127.0.0.1:{source_port}/start"))
            .unwrap()
            .header(AUTHORIZATION, HeaderValue::from_static("Bearer secret"))
            .header(COOKIE, HeaderValue::from_static("session=secret"))
            .header(
                HeaderName::from_static("x-secret"),
                HeaderValue::from_static("hidden"),
            )
            .send()
            .unwrap();
        assert_eq!(response.body(), b"ok");
        source_server.join().unwrap();
        target_server.join().unwrap();
    }
}
