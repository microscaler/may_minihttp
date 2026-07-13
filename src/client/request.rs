//! Outgoing HTTP/1.1 requests (client side).
use std::fmt;
use std::io::{self, Write};
use std::ops::{Deref, DerefMut};

use crate::client::body::BodyWriter;
use crate::client::shared::SharedStream;
use crate::client::MultipartForm;
use http::header::CONTENT_TYPE;
use http::{self, HeaderValue, Method};

/// Outgoing request for [`super::HttpClient`].
///
/// Derefs to `http::Request<BodyWriter>`. On drop, writes the request head and
/// flushes the body unless the handler already did so.
pub struct Request {
    raw_req: http::Request<BodyWriter>,
    writer: SharedStream,
    body_size: Option<usize>,
    expect_body: bool,
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<HTTP Request {}>", self.method())
    }
}

impl Request {
    /// Creates a new Request that can be used to write to a network stream.
    #[inline]
    pub(crate) fn new(stream: SharedStream) -> Request {
        Request {
            raw_req: http::Request::new(BodyWriter::InvalidWriter),
            writer: stream,
            body_size: None,
            expect_body: true,
        }
    }

    fn write_head_impl(&mut self) -> io::Result<()> {
        let mut writer = self.writer.clone();

        write!(
            writer,
            "{} {} {:?}\r\n",
            self.method(),
            self.uri(),
            self.version()
        )?;
        write!(writer, "User-Agent: may_minihttp\r\nAccept: */*\r\n")?;
        if !self.headers().contains_key(http::header::HOST) {
            if let Some(host) = self.uri().host() {
                write!(writer, "Host: {host}\r\n")?;
            }
        }

        for (key, value) in self.headers().iter() {
            write!(
                writer,
                "{}: {}\r\n",
                key.as_str(),
                value.to_str().unwrap_or("")
            )?;
        }

        if let Some(len) = self.body_size {
            write!(writer, "Content-Length: {}\r\n", len)?
        }

        write!(writer, "\r\n")?;
        Ok(())
    }

    fn write_head(&mut self) -> io::Result<BodyWriter> {
        let body = match *self.method() {
            Method::GET | Method::HEAD => BodyWriter::EmptyWriter(self.writer.clone()),
            Method::POST => match self.body_size {
                Some(size) => BodyWriter::SizedWriter(self.writer.clone(), size),
                None => BodyWriter::ChunkWriter(self.writer.clone()),
            },
            // DELETE / PUT / PATCH / OPTIONS etc. — sized body when Content-Length
            // is set; otherwise assume no body (no Transfer-Encoding for these methods).
            _ => match self.body_size {
                Some(size) => BodyWriter::SizedWriter(self.writer.clone(), size),
                None => BodyWriter::EmptyWriter(self.writer.clone()),
            },
        };
        self.write_head_impl()?;
        // Flush headers immediately so pipelined requests don't overwrite
        // the buffer before the server receives them. (BufferIo batches
        // writes to its internal Vec and only flushes on buffer fill-up.)
        let mut writer = self.writer.clone();
        writer.flush()?;
        Ok(body)
    }

    /// Writes the body and ends the Request.
    #[inline]
    pub fn send(&mut self, body: &[u8]) -> io::Result<()> {
        self.body_size = Some(body.len());
        self.write_all(body)
    }

    /// Stream an encoded multipart/form-data body into this request.
    ///
    /// The form computes its exact length before the request head is written, so the request uses
    /// `Content-Length` rather than chunked transfer encoding and does not allocate a second body.
    pub fn send_multipart(&mut self, form: &MultipartForm) -> io::Result<()> {
        let content_type = HeaderValue::from_str(&form.content_type()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid multipart content type: {error}"),
            )
        })?;
        self.headers_mut().insert(CONTENT_TYPE, content_type);
        self.set_content_length(form.content_length()?);
        form.write_to(self)
    }

    /// Serialize a value as JSON and write it as the request body.
    #[cfg(feature = "json")]
    pub fn send_json<T: serde::Serialize + ?Sized>(&mut self, value: &T) -> io::Result<()> {
        let body = serde_json::to_vec(value).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSON serialization failed: {error}"),
            )
        })?;
        self.headers_mut()
            .entry(CONTENT_TYPE)
            .or_insert(HeaderValue::from_static("application/json"));
        self.send(&body)
    }

    /// Set Content-Length before writing the request body (when not using [`Self::send`]).
    #[inline]
    pub fn set_content_length(&mut self, len: usize) {
        self.body_size = Some(len);
    }

    pub(super) fn conn(&self) -> &SharedStream {
        &self.writer
    }

    /// Set whether the request is expected to have a response body.
    ///
    /// HEAD requests should call this with `false` so that [`super::Response`]
    /// selects `EmptyReader` for the response body, preventing a hang.
    #[inline]
    pub fn expect_body(&mut self, val: bool) -> &mut Self {
        self.expect_body = val;
        self
    }

    pub(crate) fn expect_body_request(&self) -> bool {
        self.expect_body
    }
}

impl Deref for Request {
    type Target = http::Request<BodyWriter>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.raw_req
    }
}

impl DerefMut for Request {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw_req
    }
}

impl Write for Request {
    #[inline]
    fn write(&mut self, msg: &[u8]) -> io::Result<usize> {
        if let BodyWriter::InvalidWriter = *self.body() {
            *self.body_mut() = self.write_head()?;
        }
        self.body_mut().write(msg)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        if let BodyWriter::InvalidWriter = *self.body() {
            return Ok(());
        }
        self.body_mut().flush()
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        use std::thread;

        if thread::panicking() {
            return;
        }

        if let BodyWriter::InvalidWriter = *self.body() {
            *self.body_mut() = self
                .write_head()
                .unwrap_or_else(|_| BodyWriter::EmptyWriter(self.writer.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Read for Capture {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Ok(0)
        }
    }

    impl Write for Capture {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn capture() -> (SharedStream, Arc<Mutex<Vec<u8>>>) {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        (SharedStream::test(Capture(bytes.clone())), bytes)
    }

    fn request_with_method(method: Method, stream: SharedStream) -> Request {
        let mut req = Request::new(stream);
        *req.method_mut() = method;
        *req.uri_mut() = "/things/42".parse().unwrap();
        req
    }

    fn written(bytes: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8(bytes.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn delete_without_body_writes_head_on_drop() {
        let (stream, bytes) = capture();
        let req = request_with_method(Method::DELETE, stream.clone());
        drop(req);
        let head = written(&bytes);
        assert!(head.starts_with("DELETE /things/42"), "head was: {head}");
        assert!(!head.contains("Content-Length"), "head was: {head}");
    }

    #[test]
    fn put_with_sized_body_writes_content_length() {
        let (stream, bytes) = capture();
        let mut req = request_with_method(Method::PUT, stream.clone());
        req.send(b"{\"a\":1}").unwrap();
        drop(req);
        let head = written(&bytes);
        assert!(head.starts_with("PUT /things/42"), "head was: {head}");
        assert!(head.contains("Content-Length: 7"), "head was: {head}");
        assert!(head.ends_with("{\"a\":1}"), "head was: {head}");
    }

    #[test]
    fn patch_and_options_do_not_panic() {
        for method in [Method::PATCH, Method::OPTIONS] {
            let (stream, bytes) = capture();
            let req = request_with_method(method.clone(), stream.clone());
            drop(req);
            assert!(
                written(&bytes).starts_with(method.as_str()),
                "no head written for {method}"
            );
        }
    }

    #[test]
    fn absolute_uri_adds_host_header() {
        let (stream, bytes) = capture();
        let mut req = Request::new(stream.clone());
        *req.uri_mut() = "http://example.com/things".parse().unwrap();
        drop(req);

        assert!(written(&bytes).contains("Host: example.com\r\n"));
    }

    #[test]
    fn explicit_host_header_is_not_duplicated() {
        let (stream, bytes) = capture();
        let mut req = Request::new(stream.clone());
        *req.uri_mut() = "http://example.com/things".parse().unwrap();
        req.headers_mut().insert(
            http::header::HOST,
            http::HeaderValue::from_static("override.example"),
        );
        drop(req);

        let head = written(&bytes);
        let head_lower = head.to_ascii_lowercase();
        assert_eq!(
            head_lower.matches("\r\nhost:").count(),
            1,
            "head was: {head}"
        );
        assert!(head_lower.contains("host: override.example\r\n"));
    }

    #[cfg(feature = "json")]
    #[test]
    fn send_json_sets_content_type_and_length() {
        let (stream, bytes) = capture();
        let mut req = request_with_method(Method::POST, stream.clone());
        req.send_json(&serde_json::json!({"ok": true})).unwrap();
        drop(req);

        let head_and_body = written(&bytes);
        assert!(head_and_body.contains("content-type: application/json\r\n"));
        assert!(head_and_body.contains("Content-Length: 11\r\n"));
        assert!(head_and_body.ends_with("{\"ok\":true}"));
    }
}
