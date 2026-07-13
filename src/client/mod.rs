//! Coroutine HTTP/1.1 client (drop-in replacement for `may_http::client`).
//!
//! Enabled with the `client` feature. Uses native transport on `may::net::TcpStream`
//! — no dependency on the abandoned `may_http` crate.

mod body;
mod buffer;
mod client_impl;
mod multipart;
mod request;
mod response;
mod rich;
mod shared;

pub use client_impl::HttpClient;
pub use multipart::MultipartForm;
pub use request::Request;
pub use response::Response;
pub use rich::{BufferedResponse, Client, ClientBuilder, RedirectPolicy, RequestBuilder};
