//! Coroutine HTTP/1.1 client (drop-in replacement for `may_http::client`).
//!
//! Enabled with the `client` feature. Uses native transport on `may::net::TcpStream`
//! — no dependency on the abandoned `may_http` crate.

mod body;
mod buffer;
mod cancellation;
mod client_impl;
mod metadata;
mod multipart;
mod observer;
mod request;
mod resolver;
mod response;
mod rich;
mod shared;

pub use cancellation::CancellationToken;
pub use client_impl::HttpClient;
pub use metadata::{RequestMetadata, RequestMetadataContext, RequestMetadataProvider};
pub use multipart::MultipartForm;
pub use observer::{ClientEvent, ClientObserver, ObservedOrigin};
pub use request::Request;
pub use resolver::{
    CachingResolver, Resolution, ResolutionSource, Resolver, ResolverCacheConfig, ServiceResolver,
    ServiceResolverConfig, SystemResolver,
};
pub use response::Response;
pub use rich::{
    BufferedResponse, Client, ClientBuilder, ClientError, ClientErrorKind, ClientStats,
    RedirectPolicy, RequestBuilder, StreamingResponse,
};
