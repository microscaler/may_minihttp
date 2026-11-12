//! Per-attempt hooks on the pooled [`Client`](super::Client).
//!
//! An [`Interceptor`] sees every attempt the client makes on the wire: the first try, a retry on a
//! fresh connection after a reused one turned out to be dead, and each redirect hop. It is the
//! extension point for anything that has to happen *per attempt* rather than once per logical
//! request, such as:
//!
//! - headers whose value must be fresh on every attempt (a signature over the request line and a
//!   timestamp, a per-attempt id), including on the retry the pool makes by itself;
//! - per-attempt timing or connection-reuse accounting;
//! - recording failures that the caller never sees because a retry succeeded.
//!
//! Headers that are the same for every attempt (a static token, a trace context for the logical
//! request) are simpler to set once on the [`RequestBuilder`](super::RequestBuilder).
//!
//! The client is generic over its interceptor and defaults to [`NoInterceptor`], whose methods are
//! empty and inlined, so a client without an interceptor compiles to the same code as before. An
//! interceptor is attached with [`ClientBuilder::build_with_interceptor`](super::ClientBuilder::build_with_interceptor)
//! or [`Client::with_interceptor`](super::Client::with_interceptor); the latter shares the original
//! client's connection pool.
//!
//! Hooks run synchronously on the request's coroutine and never while the client holds its pool
//! lock. They cannot alter control flow. They should return quickly; a panic propagates to the
//! caller like any panic in the request path.

use std::borrow::Cow;
use std::io;
use std::time::{Duration, Instant};

use http::{HeaderMap, Method, StatusCode, Version};
use url::Url;

/// Hooks called for every attempt the client makes. All methods default to doing nothing.
///
/// Implementations are cloned into every [`RequestBuilder`](super::RequestBuilder), so keep them
/// cheap to clone (a unit struct, or an `Arc` around shared state).
pub trait Interceptor: Clone + Send + Sync + 'static {
    /// False only for an interceptor whose hooks all do nothing ([`NoInterceptor`]): the client
    /// then skips building the hook arguments, including reading the clock.
    const ENABLED: bool = true;

    /// Called once the attempt has a connection and before the request is written to it.
    ///
    /// Headers added here are sent on this attempt only; a retry calls this again with the
    /// request's original headers.
    #[inline(always)]
    fn before_send(&self, request: &mut OutgoingRequest<'_, '_>) {
        let _ = request;
    }

    /// Called when the response head has been read, before any of the body.
    #[inline(always)]
    fn after_response(&self, response: &ResponseHead<'_>, timing: &Timing) {
        let _ = (response, timing);
    }

    /// Called when an attempt fails after [`before_send`](Self::before_send), whether or not the
    /// client then retries it.
    #[inline(always)]
    fn on_error(&self, error: &io::Error, timing: &Timing) {
        let _ = (error, timing);
    }
}

/// The default interceptor: does nothing, costs nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoInterceptor;

impl Interceptor for NoInterceptor {
    const ENABLED: bool = false;
}

/// One attempt, as seen by [`Interceptor::before_send`].
pub struct OutgoingRequest<'a, 'h> {
    pub(super) method: &'a Method,
    pub(super) url: &'a Url,
    pub(super) headers: &'a mut Cow<'h, HeaderMap>,
    pub(super) attempt: u32,
    pub(super) redirect_hop: usize,
    pub(super) stale_retry: bool,
    pub(super) reused_connection: bool,
}

impl OutgoingRequest<'_, '_> {
    pub fn method(&self) -> &Method {
        self.method
    }

    /// The URL of this attempt (after any redirects).
    pub fn url(&self) -> &Url {
        self.url
    }

    pub fn headers(&self) -> &HeaderMap {
        self.headers
    }

    /// Headers for this attempt. The first call copies the request's headers when they were
    /// borrowed, so an interceptor that only reads pays nothing.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        self.headers.to_mut()
    }

    /// 1 for the first attempt; counts across stale-connection retries and redirect hops.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// 0 for the original URL, then 1, 2, ... for each redirect followed.
    pub fn redirect_hop(&self) -> usize {
        self.redirect_hop
    }

    /// True when this attempt replaces one that failed on a reused, stale connection.
    pub fn stale_retry(&self) -> bool {
        self.stale_retry
    }

    /// True when the attempt goes out on a pooled connection rather than a new one.
    pub fn reused_connection(&self) -> bool {
        self.reused_connection
    }
}

/// The response head, as seen by [`Interceptor::after_response`].
#[derive(Debug, Clone, Copy)]
pub struct ResponseHead<'a> {
    pub(super) status: StatusCode,
    pub(super) version: Version,
    pub(super) headers: &'a HeaderMap,
    pub(super) url: &'a Url,
}

impl ResponseHead<'_> {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn headers(&self) -> &HeaderMap {
        self.headers
    }

    pub fn url(&self) -> &Url {
        self.url
    }
}

/// When things happened during one attempt.
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    pub(super) request_started: Instant,
    pub(super) attempt_started: Instant,
    pub(super) attempt: u32,
    pub(super) reused_connection: bool,
}

impl Timing {
    /// When the logical request started (the `send` call).
    pub fn request_started(&self) -> Instant {
        self.request_started
    }

    /// When this attempt had its connection and started writing.
    pub fn attempt_started(&self) -> Instant {
        self.attempt_started
    }

    /// Time from the start of this attempt to the moment of the call: in `after_response`, the
    /// time to the response head (time to first byte); in `on_error`, the time to the failure.
    /// Read it inside the hook; a stored `Timing` keeps counting.
    pub fn attempt_elapsed(&self) -> Duration {
        self.attempt_started.elapsed()
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn reused_connection(&self) -> bool {
        self.reused_connection
    }
}
