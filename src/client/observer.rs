//! Request lifecycle observations for service-client integrations.

use std::time::Duration;

use http::{Method, StatusCode};

use super::resolver::ResolutionSource;
use super::rich::ClientErrorKind;

/// Sanitized network origin exposed to client observers.
///
/// Paths, query strings, user information, and headers are deliberately absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedOrigin<'a> {
    pub scheme: &'a str,
    pub host: &'a str,
    pub port: u16,
}

/// One synchronous observation from an HTTP request lifecycle.
///
/// Event values borrow request state only for the duration of [`ClientObserver::observe`].
/// Implementations that retain data must copy only the fields they actually need.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ClientEvent<'a> {
    RequestStarted {
        request_id: u64,
        method: &'a Method,
        origin: ObservedOrigin<'a>,
    },
    PoolWaited {
        request_id: u64,
        origin: ObservedOrigin<'a>,
        duration: Duration,
        timed_out: bool,
    },
    DnsCompleted {
        request_id: u64,
        origin: ObservedOrigin<'a>,
        duration: Duration,
        address_count: usize,
        source: Option<ResolutionSource>,
        error: Option<ClientErrorKind>,
    },
    ConnectionCompleted {
        request_id: u64,
        origin: ObservedOrigin<'a>,
        duration: Duration,
        tls: bool,
        error: Option<ClientErrorKind>,
    },
    ConnectionReused {
        request_id: u64,
        origin: ObservedOrigin<'a>,
    },
    ConnectionDiscarded {
        request_id: u64,
        origin: ObservedOrigin<'a>,
    },
    ResponseHeaders {
        request_id: u64,
        origin: ObservedOrigin<'a>,
        status: StatusCode,
        elapsed: Duration,
    },
    RedirectFollowed {
        request_id: u64,
        status: StatusCode,
        from: ObservedOrigin<'a>,
        to: ObservedOrigin<'a>,
    },
    StaleConnectionRetried {
        request_id: u64,
        origin: ObservedOrigin<'a>,
    },
    RequestCompleted {
        request_id: u64,
        status: StatusCode,
        total_duration: Duration,
    },
    RequestFailed {
        request_id: u64,
        error: ClientErrorKind,
        total_duration: Duration,
    },
    RequestCancelled {
        request_id: u64,
        total_duration: Duration,
    },
    RequestAbandoned {
        request_id: u64,
        status: StatusCode,
        total_duration: Duration,
    },
}

/// Receives request lifecycle observations.
///
/// Callbacks run synchronously and never while the client holds its pool or transport lock. They
/// cannot alter request control flow. Implementations should return quickly and must apply their
/// own panic and latency policy.
pub trait ClientObserver: Send + Sync {
    fn observe(&self, event: ClientEvent<'_>);
}
