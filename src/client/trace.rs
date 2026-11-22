//! Optional [`tracing`](https://docs.rs/tracing) instrumentation of the pooled client (feature
//! `tracing`, off by default).
//!
//! With the feature off, every item here is a zero-sized no-op and the macros expand to nothing:
//! no `tracing` dependency, no code, no fields evaluated.
//!
//! With it on, each request made through [`Client`](super::Client) gets one span, target
//! `may_minihttp::client`:
//!
//! | span (level) | when | fields |
//! |---|---|---|
//! | `http_client_request` (INFO) | the whole request, until the body is read or the response dropped | `http.request.method`, `server.address`, `server.port`, `url.scheme`, `http.response.status_code`, `error.type`, `http.request.resend_count`, `may_minihttp.connection_reused` |
//! | `http_client_connect` (INFO) | DNS and connect (TLS handshake included) of a new connection | `server.address`, `server.port`, `error.type` |
//! | `http_client_write` (DEBUG) | writing the request | |
//! | `http_client_wait` (DEBUG) | from the request written to the response head: time to first byte | `http.response.status_code` |
//! | `http_client_body` (DEBUG) | reading the response body | `http.response.body.size` |
//!
//! and DEBUG events on the request span: `pool_checkout` (`result` = `hit`, `miss` or `wait`) and
//! `pool_evict` (`reason` = `expired` or `closed`, `count`).
//!
//! Paths, query strings and headers are never recorded.
//!
//! The request span's parent is the span passed to `RequestBuilder::parent_span`, or else
//! [`tracing::Span::current()`] when the request is sent. `RequestBuilder::request_span` instead
//! hands the client a span to use *as* the request span (a caller's own client span, with its own
//! name and attributes): the client records the request fields that span declares and opens the
//! phase spans under it. When the request span is disabled, no phase spans or events are created. The client never *enters* its spans: a
//! may coroutine can yield inside the request and resume on another thread, and an entered span
//! would leak into whatever runs on the first thread meanwhile. Spans are created with an explicit
//! parent and closed when dropped, which is all a span exporter needs for the tree and the timing.

#[cfg(feature = "tracing")]
pub(crate) type Span = tracing::Span;

/// [`tracing::Span::current()`], skipped (a disabled span) when nothing records INFO spans: with
/// no subscriber, the request path does not touch the dispatcher's thread-local.
#[cfg(feature = "tracing")]
#[inline]
pub(crate) fn current_parent() -> Span {
    if tracing::level_enabled!(tracing::Level::INFO) {
        tracing::Span::current()
    } else {
        tracing::Span::none()
    }
}

/// Stand-in for `tracing::Span` with the feature off.
#[cfg(not(feature = "tracing"))]
#[derive(Clone, Debug, Default)]
pub(crate) struct Span;

#[cfg(not(feature = "tracing"))]
#[inline(always)]
pub(crate) fn current_parent() -> Span {
    Span
}

/// `tracing::span!` with the client's target and an explicit parent; a zero-sized no-op without
/// the feature (nothing in the invocation is evaluated).
#[cfg(feature = "tracing")]
macro_rules! client_span {
    ($level:ident, parent: $parent:expr, $name:literal $(, $($fields:tt)*)?) => {
        tracing::span!(
            target: "may_minihttp::client",
            parent: $parent,
            tracing::Level::$level,
            $name
            $(, $($fields)*)?
        )
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! client_span {
    ($($ignored:tt)*) => {
        $crate::client::trace::Span
    };
}

/// A span under the request's span, or none when the request's span is disabled (a child of a
/// disabled span would otherwise be exported as a root of its own).
#[cfg(feature = "tracing")]
macro_rules! phase_span {
    ($level:ident, parent: $parent:expr, $name:literal $(, $($fields:tt)*)?) => {{
        let parent: &tracing::Span = $parent;
        if parent.is_disabled() {
            tracing::Span::none()
        } else {
            tracing::span!(
                target: "may_minihttp::client",
                parent: parent,
                tracing::Level::$level,
                $name
                $(, $($fields)*)?
            )
        }
    }};
}

#[cfg(not(feature = "tracing"))]
macro_rules! phase_span {
    ($($ignored:tt)*) => {
        $crate::client::trace::Span
    };
}

/// A DEBUG event on `parent` (skipped when `parent` is disabled); nothing without the feature.
#[cfg(feature = "tracing")]
macro_rules! client_event {
    (parent: $parent:expr, $($rest:tt)*) => {{
        let parent: &tracing::Span = $parent;
        if !parent.is_disabled() {
            tracing::event!(
                target: "may_minihttp::client",
                parent: parent,
                tracing::Level::DEBUG,
                $($rest)*
            );
        }
    }};
}

#[cfg(not(feature = "tracing"))]
macro_rules! client_event {
    ($($ignored:tt)*) => {
        ()
    };
}

/// `span.record(field, value)`; nothing (the value is not evaluated) without the feature.
#[cfg(feature = "tracing")]
macro_rules! span_record {
    ($span:expr, $field:literal, $value:expr) => {{
        $span.record($field, $value);
    }};
}

#[cfg(not(feature = "tracing"))]
macro_rules! span_record {
    ($($ignored:tt)*) => {
        ()
    };
}

pub(crate) use {client_event, client_span, phase_span, span_record};
