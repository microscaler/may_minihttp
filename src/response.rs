use std::borrow::Cow;
use std::io::{self, Write};

use crate::request::MAX_HEADERS;

use bytes::BytesMut;
use may::net::TcpStream;

/// A single HTTP response header value.
///
/// This type lets callers pass either `&'static str` (the previous fast path,
/// zero allocation) or an owned string (`String` / `Box<str>` / `Cow::Owned`)
/// for headers whose value is computed per-response (for example a generated
/// request id, a dynamic `Content-Type`, or an `Accept-Post` list built from
/// OpenAPI metadata).
///
/// Owned variants are dropped when the [`Response`] is dropped, so using
/// owned values here does **not** leak memory — unlike the previous workaround
/// that required callers to `Box::leak` their formatted header strings to
/// satisfy the `&'static str` requirement on [`Response::header`].
///
/// In the common static case the `Static` variant stores the same fat pointer
/// as a `&'static str`, so there is no additional indirection on the encode
/// hot path.
#[derive(Debug)]
pub enum ResponseHeader {
    /// A header whose value has `'static` lifetime (e.g. a string literal).
    /// Matches the original `may_minihttp` behavior — no allocation, no drop.
    Static(&'static str),
    /// A header whose value is owned by this response.
    /// Freed when the response is dropped.
    Owned(Box<str>),
}

impl ResponseHeader {
    /// Borrow the header line as `&str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            ResponseHeader::Static(s) => s,
            ResponseHeader::Owned(s) => s,
        }
    }

    /// Borrow the header line as raw bytes (what `encode` writes).
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl Default for ResponseHeader {
    #[inline]
    fn default() -> Self {
        ResponseHeader::Static("")
    }
}

/// Conversion trait that lets [`Response::header`] accept either static or
/// owned string-like values without an API split.
///
/// Implementations are provided for `&'static str`, `String`, `Box<str>`, and
/// `Cow<'static, str>`. All of them are zero-cost in the `Static` case and
/// perform a single `into_boxed_str()` / no-op in the owned case.
pub trait IntoResponseHeader {
    fn into_response_header(self) -> ResponseHeader;
}

impl IntoResponseHeader for &'static str {
    #[inline]
    fn into_response_header(self) -> ResponseHeader {
        ResponseHeader::Static(self)
    }
}

impl IntoResponseHeader for String {
    #[inline]
    fn into_response_header(self) -> ResponseHeader {
        ResponseHeader::Owned(self.into_boxed_str())
    }
}

impl IntoResponseHeader for Box<str> {
    #[inline]
    fn into_response_header(self) -> ResponseHeader {
        ResponseHeader::Owned(self)
    }
}

impl IntoResponseHeader for Cow<'static, str> {
    #[inline]
    fn into_response_header(self) -> ResponseHeader {
        match self {
            Cow::Borrowed(s) => ResponseHeader::Static(s),
            Cow::Owned(s) => ResponseHeader::Owned(s.into_boxed_str()),
        }
    }
}

impl IntoResponseHeader for ResponseHeader {
    #[inline]
    fn into_response_header(self) -> ResponseHeader {
        self
    }
}

pub struct Response<'a> {
    headers: [ResponseHeader; MAX_HEADERS],
    headers_len: usize,
    status_message: StatusMessage,
    body: Body,
    rsp_buf: &'a mut BytesMut,
    /// When true, headers/body were already written to the socket (chunked/SSE).
    /// [`encode`] becomes a no-op so the connection loop does not double-write.
    streamed: bool,
}

enum Body {
    Str(&'static str),
    Vec(Vec<u8>),
    Dummy,
}

struct StatusMessage {
    code: usize,
    msg: &'static str,
}

impl<'a> Response<'a> {
    pub(crate) fn new(rsp_buf: &'a mut BytesMut) -> Response<'a> {
        Response {
            headers: std::array::from_fn(|_| ResponseHeader::Static("")),
            headers_len: 0,
            body: Body::Dummy,
            status_message: StatusMessage {
                code: 200,
                msg: "Ok",
            },
            rsp_buf,
            streamed: false,
        }
    }

    /// `true` when [`Self::begin_chunked_stream`] already wrote this response.
    #[inline]
    pub fn is_streamed(&self) -> bool {
        self.streamed
    }

    #[inline]
    pub fn status_code(&mut self, code: usize, msg: &'static str) -> &mut Self {
        self.status_message = StatusMessage { code, msg };
        self
    }

    /// Append a header line to the response.
    ///
    /// Accepts both `&'static str` (zero allocation, identical to the previous
    /// behavior) and owned strings (`String`, `Box<str>`, `Cow<'static, str>`).
    /// Owned header values are freed when the response is dropped — there is
    /// no need for callers to `Box::leak` formatted values.
    ///
    /// ```
    /// # use may_minihttp::{Response, ResponseHeader};
    /// # use bytes::BytesMut;
    /// # let mut buf = BytesMut::new();
    /// # let mut res = Response::_test_new(&mut buf);
    /// // static fast path (no allocation)
    /// res.header("Content-Type: application/json");
    ///
    /// // owned path (freed with the response)
    /// let request_id = format!("X-Request-ID: {}", "01J…");
    /// res.header(request_id);
    /// ```
    #[inline]
    pub fn header<H: IntoResponseHeader>(&mut self, header: H) -> &mut Self {
        self.headers[self.headers_len] = header.into_response_header();
        self.headers_len += 1;
        self
    }

    #[inline]
    pub fn body(&mut self, s: &'static str) {
        self.body = Body::Str(s);
    }

    #[inline]
    pub fn body_vec(&mut self, v: Vec<u8>) {
        self.body = Body::Vec(v);
    }

    /// Write status + headers with `Transfer-Encoding: chunked`, then return a
    /// writer for incremental body chunks (SSE / live flush). Marks this
    /// response as streamed so the server loop skips [`encode`].
    pub fn begin_chunked_stream<'s>(
        &mut self,
        stream: &'s mut TcpStream,
    ) -> io::Result<ChunkedBodyWriter<'s>> {
        if self.streamed {
            return Err(io::Error::other("response already streamed"));
        }
        let mut head = BytesMut::with_capacity(256);
        write_status_and_headers(self, &mut head, /*chunked=*/ true);
        stream.write_all(&head)?;
        stream.flush()?;
        self.streamed = true;
        // Clear buffered body — wire path owns the payload now.
        self.body = Body::Dummy;
        self.rsp_buf.clear();
        Ok(ChunkedBodyWriter {
            stream,
            finished: false,
        })
    }

    #[inline]
    pub fn body_mut(&mut self) -> &mut BytesMut {
        match self.body {
            Body::Dummy => {}
            Body::Str(s) => {
                self.rsp_buf.extend_from_slice(s.as_bytes());
                self.body = Body::Dummy;
            }
            Body::Vec(ref v) => {
                self.rsp_buf.extend_from_slice(v);
                self.body = Body::Dummy;
            }
        }
        self.rsp_buf
    }

    #[inline]
    fn body_len(&self) -> usize {
        match self.body {
            Body::Dummy => self.rsp_buf.len(),
            Body::Str(s) => s.len(),
            Body::Vec(ref v) => v.len(),
        }
    }

    #[inline]
    fn get_body(&mut self) -> &[u8] {
        match self.body {
            Body::Dummy => self.rsp_buf.as_ref(),
            Body::Str(s) => s.as_bytes(),
            Body::Vec(ref v) => v,
        }
    }

    /// Test-only constructor used by the doc example above. Not part of the
    /// public API; gated so downstream crates cannot accidentally rely on it.
    #[doc(hidden)]
    pub fn _test_new(rsp_buf: &'a mut BytesMut) -> Response<'a> {
        Response::new(rsp_buf)
    }
}

impl Drop for Response<'_> {
    fn drop(&mut self) {
        self.rsp_buf.clear();
    }
}

/// Writer returned by [`Response::begin_chunked_stream`].
pub struct ChunkedBodyWriter<'s> {
    stream: &'s mut TcpStream,
    finished: bool,
}

impl ChunkedBodyWriter<'_> {
    /// Write one chunk and flush (clients observe data immediately).
    pub fn write_chunk(&mut self, data: &[u8]) -> io::Result<()> {
        if self.finished {
            return Err(io::Error::other("chunked stream already finished"));
        }
        if data.is_empty() {
            return Ok(());
        }
        write!(self.stream, "{:x}\r\n", data.len())?;
        self.stream.write_all(data)?;
        self.stream.write_all(b"\r\n")?;
        self.stream.flush()?;
        Ok(())
    }

    /// Terminate the chunked body (`0\r\n\r\n`).
    pub fn finish(mut self) -> io::Result<()> {
        if !self.finished {
            self.stream.write_all(b"0\r\n\r\n")?;
            self.stream.flush()?;
            self.finished = true;
        }
        Ok(())
    }
}

impl Drop for ChunkedBodyWriter<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.stream.write_all(b"0\r\n\r\n");
            let _ = self.stream.flush();
            self.finished = true;
        }
    }
}

fn write_status_and_headers(rsp: &Response<'_>, buf: &mut BytesMut, chunked: bool) {
    if rsp.status_message.code == 200 {
        buf.extend_from_slice(b"HTTP/1.1 200 Ok\r\nServer: M\r\nDate: ");
    } else {
        buf.extend_from_slice(b"HTTP/1.1 ");
        let mut code = itoa::Buffer::new();
        buf.extend_from_slice(code.format(rsp.status_message.code).as_bytes());
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(rsp.status_message.msg.as_bytes());
        buf.extend_from_slice(b"\r\nServer: M\r\nDate: ");
    }
    crate::date::append_date(buf);
    if chunked {
        buf.extend_from_slice(b"\r\nTransfer-Encoding: chunked");
    } else {
        buf.extend_from_slice(b"\r\nContent-Length: ");
        let mut length = itoa::Buffer::new();
        buf.extend_from_slice(length.format(rsp.body_len()).as_bytes());
    }

    // SAFETY: we already have bound check when insert headers
    let headers = unsafe { rsp.headers.get_unchecked(..rsp.headers_len) };
    for h in headers {
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(h.as_bytes());
    }

    buf.extend_from_slice(b"\r\n\r\n");
}

pub(crate) fn encode(mut rsp: Response, buf: &mut BytesMut) {
    if rsp.streamed {
        return;
    }
    write_status_and_headers(&rsp, buf, /*chunked=*/ false);
    buf.extend_from_slice(rsp.get_body());
}

#[cold]
pub(crate) fn encode_error(e: io::Error, buf: &mut BytesMut) {
    error!("error in service: err = {e:?}");
    let msg_string = e.to_string();
    let msg = msg_string.as_bytes();

    buf.extend_from_slice(b"HTTP/1.1 500 Internal Server Error\r\nServer: M\r\nDate: ");
    crate::date::append_date(buf);
    buf.extend_from_slice(b"\r\nContent-Length: ");
    let mut length = itoa::Buffer::new();
    buf.extend_from_slice(length.format(msg.len()).as_bytes());

    buf.extend_from_slice(b"\r\n\r\n");
    buf.extend_from_slice(msg);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    /// A `&'static str` header takes the Static fast path and stores the
    /// exact same slice. No allocation.
    #[test]
    fn header_static_is_zero_alloc() {
        let hdr: ResponseHeader = "Content-Type: text/plain".into_response_header();
        match hdr {
            ResponseHeader::Static(s) => assert_eq!(s, "Content-Type: text/plain"),
            ResponseHeader::Owned(_) => panic!("static input must take Static variant"),
        }
    }

    /// `String` / `Box<str>` / `Cow::Owned` take the Owned variant and are
    /// freed with the response; no `Box::leak` needed by callers.
    #[test]
    fn header_owned_variants_are_accepted() {
        let s: ResponseHeader = String::from("X-Req-Id: 01J").into_response_header();
        assert!(matches!(s, ResponseHeader::Owned(_)));

        let b: ResponseHeader = Box::<str>::from("X-Foo: bar").into_response_header();
        assert!(matches!(b, ResponseHeader::Owned(_)));

        let c_borrowed: ResponseHeader =
            Cow::<'static, str>::Borrowed("Content-Type: application/json").into_response_header();
        assert!(matches!(c_borrowed, ResponseHeader::Static(_)));

        let c_owned: ResponseHeader =
            Cow::<'static, str>::Owned(String::from("X-Trace: abc")).into_response_header();
        assert!(matches!(c_owned, ResponseHeader::Owned(_)));
    }

    /// `Response::header()` accepts both static and owned headers without an
    /// API split, and `encode()` writes both correctly to the response buffer.
    #[test]
    fn encode_mixes_static_and_owned_headers() {
        let mut rsp_buf = BytesMut::new();
        let mut out = BytesMut::new();
        {
            let mut res = Response::new(&mut rsp_buf);
            res.status_code(200, "OK");
            res.header("Content-Type: application/json");
            res.header(format!("X-Request-ID: {}", "01J"));
            res.header(Cow::<'static, str>::Owned(String::from("X-Trace: abc")));
            res.body("ok");
            // `encode` consumes the response via `mut rsp: Response`
            encode(res, &mut out);
        }
        let response_str = std::str::from_utf8(&out).expect("utf8");
        assert!(response_str.starts_with("HTTP/1.1 200 Ok\r\n"));
        assert!(response_str.contains("\r\nContent-Type: application/json\r\n"));
        assert!(response_str.contains("\r\nX-Request-ID: 01J\r\n"));
        assert!(response_str.contains("\r\nX-Trace: abc\r\n"));
        assert!(response_str.ends_with("\r\n\r\nok"));
    }
}
