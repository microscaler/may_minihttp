//! Bounded, policy-neutral request metadata injection.

use std::fmt;
use std::io;

use http::{HeaderMap, HeaderName, HeaderValue, Method};

use super::ObservedOrigin;

/// Sanitized context supplied immediately before one network attempt.
///
/// Paths, query strings, request bodies, and existing header values are deliberately absent.
/// `attempt` starts at one and increases across redirect hops and a stale-connection retry.
#[derive(Debug, Clone, Copy)]
pub struct RequestMetadataContext<'a> {
    pub request_id: u64,
    pub method: &'a Method,
    pub origin: ObservedOrigin<'a>,
    pub attempt: u32,
    pub redirect_hop: usize,
    pub stale_retry: bool,
}

/// Headers returned by a [`RequestMetadataProvider`] for one network attempt.
///
/// The custom `Debug` implementation deliberately reports counts rather than header values.
#[derive(Clone, Default)]
pub struct RequestMetadata {
    pub(crate) headers: HeaderMap,
    pub(crate) sensitive_headers: Vec<HeaderName>,
}

impl RequestMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a header for this attempt.
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.append(name, value);
        self
    }

    /// Mark a header name as credential-bearing for cross-origin redirect stripping.
    ///
    /// Built-in credential headers (`Authorization`, `Cookie`, and `Proxy-Authorization`) are
    /// always sensitive and do not need to be declared here.
    pub fn sensitive_header(mut self, name: HeaderName) -> Self {
        if !self.sensitive_headers.contains(&name) {
            self.sensitive_headers.push(name);
        }
        self
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl fmt::Debug for RequestMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestMetadata")
            .field("header_count", &self.headers.len())
            .field("sensitive_header_count", &self.sensitive_headers.len())
            .finish()
    }
}

/// Supplies policy-neutral headers immediately before an HTTP request attempt.
///
/// Implementations may read an atomically replaceable credential or trace-context snapshot, but
/// token acquisition, authorization policy, and tracing export remain application concerns. The
/// callback runs synchronously without client pool or transport locks held. It should return
/// quickly and must apply its own panic and latency policy.
///
/// ```
/// use std::sync::Arc;
/// use http::{HeaderName, HeaderValue};
/// use may_minihttp::client::{Client, RequestMetadata, RequestMetadataContext};
///
/// let provider = Arc::new(|context: RequestMetadataContext<'_>| {
///     let trace = HeaderValue::from_str(&format!(
///         "request-{}-attempt-{}",
///         context.request_id, context.attempt
///     ))
///     .expect("generated trace header is valid");
///     Ok(RequestMetadata::new().header(HeaderName::from_static("x-trace-id"), trace))
/// });
/// let _client = Client::builder()
///     .request_metadata_provider(provider)
///     .build()?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub trait RequestMetadataProvider: Send + Sync {
    fn provide(&self, context: RequestMetadataContext<'_>) -> io::Result<RequestMetadata>;
}

impl<F> RequestMetadataProvider for F
where
    F: for<'a> Fn(RequestMetadataContext<'a>) -> io::Result<RequestMetadata> + Send + Sync,
{
    fn provide(&self, context: RequestMetadataContext<'_>) -> io::Result<RequestMetadata> {
        self(context)
    }
}
