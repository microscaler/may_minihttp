//! Outgoing HTTP/1.1 requests (client side).
use std::fmt;
use std::io::{self, Read, Write};
use std::ops::{Deref, DerefMut};

use crate::client::body::BodyWriter;
use crate::client::shared::SharedStream;
use crate::client::MultipartForm;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING};
use http::{self, HeaderValue, Method};

/// Outgoing request for [`super::HttpClient`].
///
/// Derefs to `http::Request<BodyWriter>`. For compatibility, dropping a request that has never
/// attempted completion writes its empty request head. Normal callers should use [`Self::finish`]
/// or [`super::HttpClient::send_request`] so errors are observable.
pub struct Request {
    raw_req: http::Request<BodyWriter>,
    writer: SharedStream,
    body_size: Option<usize>,
    expect_body: bool,
    completion_attempted: bool,
    completed: bool,
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
            completion_attempted: false,
            completed: false,
        }
    }

    fn write_head_impl(&mut self) -> io::Result<()> {
        self.writer.ensure_request_ready()?;
        if self.headers().contains_key(TRANSFER_ENCODING)
            && (self.body_size.is_some() || self.headers().contains_key(CONTENT_LENGTH))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request must not contain both Transfer-Encoding and Content-Length",
            ));
        }
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
            if self.body_size.is_some() && key == CONTENT_LENGTH {
                continue;
            }
            writer.write_all(key.as_str().as_bytes())?;
            writer.write_all(b": ")?;
            writer.write_all(value.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }

        if let Some(len) = self.body_size {
            write!(writer, "Content-Length: {}\r\n", len)?
        } else if self.method() == Method::POST
            && !self.headers().contains_key(CONTENT_LENGTH)
            && !self.headers().contains_key(TRANSFER_ENCODING)
        {
            writer.write_all(b"Transfer-Encoding: chunked\r\n")?;
        }

        write!(writer, "\r\n")?;
        Ok(())
    }

    fn write_head(&mut self) -> io::Result<BodyWriter> {
        let chunked = parse_transfer_encoding(self.headers())?;
        if self.body_size.is_none() {
            self.body_size = parse_content_length(self.headers())?;
        }
        if matches!(*self.method(), Method::GET | Method::HEAD)
            && (self.body_size.is_some() || chunked)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request bodies are not supported for GET or HEAD",
            ));
        }
        let body = match *self.method() {
            Method::GET | Method::HEAD => BodyWriter::EmptyWriter(self.writer.clone()),
            Method::POST => match self.body_size {
                Some(size) => BodyWriter::SizedWriter(self.writer.clone(), size),
                None => BodyWriter::ChunkWriter(self.writer.clone(), false),
            },
            // DELETE / PUT / PATCH / OPTIONS etc. — sized body when Content-Length
            // is set; otherwise assume no body (no Transfer-Encoding for these methods).
            _ if chunked => BodyWriter::ChunkWriter(self.writer.clone(), false),
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
        let result = self.write_all(body);
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// Stream exactly `content_length` bytes from a caller-supplied reader.
    pub fn send_reader(
        &mut self,
        reader: &mut (impl Read + ?Sized),
        content_length: usize,
    ) -> io::Result<()> {
        self.body_size = Some(content_length);
        let mut remaining = content_length;
        // Keep streaming buffers off may's deliberately small coroutine stacks.
        let mut buffer = vec![0_u8; 8 * 1024];
        while remaining > 0 {
            let allowed = buffer.len().min(remaining);
            let read = match reader.read(&mut buffer[..allowed]) {
                Ok(0) => {
                    self.abort();
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("request reader ended {remaining} bytes before Content-Length"),
                    ));
                }
                Ok(read) => read,
                Err(error) => {
                    self.abort();
                    return Err(error);
                }
            };
            if let Err(error) = self.write_all(&buffer[..read]) {
                self.abort();
                return Err(error);
            }
            remaining -= read;
        }
        Ok(())
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
        let result = form.write_to(self);
        if result.is_err() {
            self.abort();
        }
        result
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

    /// Explicitly finish and flush the request, returning any write error.
    ///
    /// [`super::HttpClient::send_request`] calls this automatically. It is exposed for low-level
    /// users that need to separate request completion from reading the response.
    pub fn finish(&mut self) -> io::Result<()> {
        if self.completed {
            return Ok(());
        }
        self.completion_attempted = true;
        if let BodyWriter::InvalidWriter = *self.body() {
            *self.body_mut() = self.write_head()?;
        }
        self.body_mut().finish()?;
        self.completed = true;
        Ok(())
    }

    pub(crate) fn abort(&mut self) {
        self.completion_attempted = true;
        self.body_mut().abort();
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
        if self.completed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "request is already finished",
            ));
        }
        if let BodyWriter::InvalidWriter = *self.body() {
            *self.body_mut() = self.write_head()?;
        }
        self.body_mut().write(msg)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        if self.completed {
            return Ok(());
        }
        if !self.completion_attempted && matches!(*self.body(), BodyWriter::InvalidWriter) {
            return Ok(());
        }
        self.body_mut().flush()
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        use std::thread;

        if thread::panicking() || self.completion_attempted || self.completed {
            return;
        }

        if let BodyWriter::InvalidWriter = *self.body() {
            *self.body_mut() = self
                .write_head()
                .unwrap_or_else(|_| BodyWriter::EmptyWriter(self.writer.clone()));
        }
    }
}

fn parse_content_length(headers: &http::HeaderMap) -> io::Result<Option<usize>> {
    let mut parsed = None;
    for value in headers.get_all(CONTENT_LENGTH) {
        let value = value.to_str().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("malformed request Content-Length: {error}"),
            )
        })?;
        for item in value.split(',') {
            let item = item.trim();
            if item.is_empty() || !item.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "malformed request Content-Length",
                ));
            }
            let length = item.parse::<usize>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("malformed request Content-Length: {error}"),
                )
            })?;
            if parsed.is_some_and(|previous| previous != length) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "conflicting request Content-Length values",
                ));
            }
            parsed = Some(length);
        }
    }
    Ok(parsed)
}

fn parse_transfer_encoding(headers: &http::HeaderMap) -> io::Result<bool> {
    let mut codings = Vec::new();
    for value in headers.get_all(TRANSFER_ENCODING) {
        let value = value.to_str().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("malformed request Transfer-Encoding: {error}"),
            )
        })?;
        codings.extend(
            value
                .split(',')
                .map(str::trim)
                .filter(|coding| !coding.is_empty()),
        );
    }
    match codings.as_slice() {
        [] => Ok(false),
        [coding] if coding.eq_ignore_ascii_case("chunked") => Ok(true),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only a single request Transfer-Encoding: chunked is supported",
        )),
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

    #[test]
    fn raw_header_value_bytes_are_preserved() {
        let (stream, bytes) = capture();
        let mut req = Request::new(stream);
        *req.uri_mut() = "/".parse().unwrap();
        req.headers_mut().insert(
            http::header::HeaderName::from_static("x-opaque"),
            http::HeaderValue::from_bytes(&[0x80, 0x81]).unwrap(),
        );
        drop(req);
        assert!(bytes
            .lock()
            .unwrap()
            .windows(12)
            .any(|window| window == b"x-opaque: \x80\x81"));
    }

    #[test]
    fn request_rejects_transfer_encoding_with_content_length() {
        let (stream, _bytes) = capture();
        let mut req = request_with_method(Method::POST, stream);
        req.headers_mut()
            .insert(TRANSFER_ENCODING, http::HeaderValue::from_static("chunked"));
        let error = req.send(b"body").unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn caller_content_length_selects_sized_writer_and_is_canonicalized() {
        let (stream, bytes) = capture();
        let mut req = request_with_method(Method::POST, stream);
        req.headers_mut()
            .append(CONTENT_LENGTH, HeaderValue::from_static("3"));
        req.headers_mut()
            .append(CONTENT_LENGTH, HeaderValue::from_static("3"));
        req.write_all(b"abc").unwrap();
        req.finish().unwrap();
        drop(req);

        let written = written(&bytes);
        assert_eq!(written.matches("Content-Length: 3\r\n").count(), 1);
        assert!(!written.contains("Transfer-Encoding"));
        assert!(written.ends_with("\r\n\r\nabc"));
    }

    #[test]
    fn unsupported_request_transfer_coding_is_rejected_before_write() {
        let (stream, bytes) = capture();
        let mut req = request_with_method(Method::POST, stream);
        req.headers_mut()
            .insert(TRANSFER_ENCODING, HeaderValue::from_static("gzip, chunked"));
        assert_eq!(
            req.finish().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        drop(req);
        assert!(bytes.lock().unwrap().is_empty());
    }

    #[test]
    fn reader_failure_before_head_does_not_send_from_drop() {
        let (stream, bytes) = capture();
        let mut req = request_with_method(Method::POST, stream);
        let mut empty = io::empty();
        assert_eq!(
            req.send_reader(&mut empty, 3).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        drop(req);
        assert!(bytes.lock().unwrap().is_empty());
    }

    #[test]
    fn explicit_finish_rejects_short_body_without_zero_padding() {
        let (stream, bytes) = capture();
        let mut req = request_with_method(Method::PUT, stream);
        req.set_content_length(5);
        req.write_all(b"hi").unwrap();
        assert_eq!(
            req.finish().unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        drop(req);
        let written = bytes.lock().unwrap().clone();
        assert!(written.ends_with(b"hi"));
        assert!(!written.ends_with(b"hi\0\0\0"));
    }

    #[test]
    fn explicit_finish_writes_one_chunk_terminator() {
        let (stream, bytes) = capture();
        let mut req = request_with_method(Method::POST, stream);
        req.write_all(b"hi").unwrap();
        req.finish().unwrap();
        req.finish().unwrap();
        drop(req);
        let written = bytes.lock().unwrap().clone();
        assert_eq!(
            written
                .windows(5)
                .filter(|part| *part == b"0\r\n\r\n")
                .count(),
            1
        );
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
