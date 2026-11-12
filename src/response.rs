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
    // header lines past the first MAX_HEADERS; allocated only when a response has that many
    more_headers: Vec<ResponseHeader>,
    status_message: StatusMessage,
    body: Body,
    rsp_buf: &'a mut BytesMut,
    /// Header lines added with [`Response::header_kv`], already encoded (`\r\nName: value` each),
    /// in a buffer the connection reuses.
    kv_head: &'a mut BytesMut,
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
    pub(crate) fn new(rsp_buf: &'a mut BytesMut, kv_head: &'a mut BytesMut) -> Response<'a> {
        Response {
            headers: std::array::from_fn(|_| ResponseHeader::Static("")),
            headers_len: 0,
            more_headers: Vec::new(),
            body: Body::Dummy,
            status_message: StatusMessage {
                code: 200,
                msg: "Ok",
            },
            rsp_buf,
            kv_head,
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
    ///
    /// Any number of headers may be added; the first 16 are stored inline. (There used to be a
    /// hard limit of 16: the 17th header panicked the connection.)
    ///
    /// An owned header line containing CR or LF is dropped and logged: written out, it would end
    /// the header line early and let the rest of the value inject headers or a body (response
    /// splitting). Static lines are trusted.
    #[inline]
    pub fn header<H: IntoResponseHeader>(&mut self, header: H) -> &mut Self {
        let header = header.into_response_header();
        if let ResponseHeader::Owned(line) = &header {
            if line.bytes().any(|b| b == b'\r' || b == b'\n') {
                error!("response header dropped: it contains CR or LF");
                return self;
            }
        }
        if self.headers_len < MAX_HEADERS {
            self.headers[self.headers_len] = header;
            self.headers_len += 1;
        } else {
            self.more_headers.push(header);
        }
        self
    }

    /// Append the header `name: value` without building the line as a string first.
    ///
    /// The line is encoded straight into a buffer the connection reuses, so a header whose value
    /// is computed per response costs no allocation (with [`Response::header`] it costs one: the
    /// caller's `format!`). These lines are written after the ones added with `header`, in the
    /// order added.
    ///
    /// A name that is not an HTTP token, or a value containing CR, LF or NUL, is dropped and
    /// logged (it would split the response).
    pub fn header_kv(&mut self, name: &str, value: &str) -> &mut Self {
        let name_ok = !name.is_empty() && name.bytes().all(is_token_byte);
        let value_ok = !value.bytes().any(|b| b == b'\r' || b == b'\n' || b == 0);
        if !(name_ok && value_ok) {
            error!("response header dropped: invalid name or a value containing CR, LF or NUL");
            return self;
        }
        self.kv_head.reserve(name.len() + value.len() + 4);
        self.kv_head.extend_from_slice(b"\r\n");
        self.kv_head.extend_from_slice(name.as_bytes());
        self.kv_head.extend_from_slice(b": ");
        self.kv_head.extend_from_slice(value.as_bytes());
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
            frame: Vec::new(),
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
        // test-only: the header_kv buffer is leaked (a few bytes per call)
        Response::new(rsp_buf, Box::leak(Box::new(BytesMut::new())))
    }
}

impl Drop for Response<'_> {
    fn drop(&mut self) {
        self.rsp_buf.clear();
        self.kv_head.clear();
    }
}

/// RFC 9110 `tchar`.
#[inline]
fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// Writer returned by [`Response::begin_chunked_stream`].
pub struct ChunkedBodyWriter<'s> {
    stream: &'s mut TcpStream,
    finished: bool,
    // one chunk (size line, data, CRLF) assembled here, reused across chunks
    frame: Vec<u8>,
}

/// Lower-case hex digits of `n`, most significant first, into the end of `out`; returns the
/// start index.
fn hex_digits(mut n: usize, out: &mut [u8; 2 * std::mem::size_of::<usize>()]) -> usize {
    let mut at = out.len();
    loop {
        at -= 1;
        out[at] = b"0123456789abcdef"[n & 0xf];
        n >>= 4;
        if n == 0 {
            return at;
        }
    }
}

impl ChunkedBodyWriter<'_> {
    /// Write one chunk and flush (clients observe data immediately).
    ///
    /// The size line, the data and the trailing CRLF go out in one write: with `TCP_NODELAY` each
    /// write is its own segment, and the formatted size line alone could be more than one, so one
    /// SSE event cost 3-4 syscalls and packets.
    pub fn write_chunk(&mut self, data: &[u8]) -> io::Result<()> {
        if self.finished {
            return Err(io::Error::other("chunked stream already finished"));
        }
        if data.is_empty() {
            return Ok(());
        }
        let mut size = [0_u8; 2 * std::mem::size_of::<usize>()];
        let at = hex_digits(data.len(), &mut size);
        self.frame.clear();
        self.frame.reserve(size.len() - at + data.len() + 4);
        self.frame.extend_from_slice(&size[at..]);
        self.frame.extend_from_slice(b"\r\n");
        self.frame.extend_from_slice(data);
        self.frame.extend_from_slice(b"\r\n");
        self.stream.write_all(&self.frame)?;
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
    for h in headers.iter().chain(rsp.more_headers.iter()) {
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(h.as_bytes());
    }
    buf.extend_from_slice(rsp.kv_head);

    buf.extend_from_slice(b"\r\n\r\n");
}

pub(crate) fn encode(mut rsp: Response, buf: &mut BytesMut) {
    if rsp.streamed {
        return;
    }
    write_status_and_headers(&rsp, buf, /*chunked=*/ false);
    buf.extend_from_slice(rsp.get_body());
}

/// The response to a request refused before the service saw it; the connection closes after it.
#[cold]
pub(crate) fn encode_rejection(status: u16, buf: &mut BytesMut) {
    let line: &[u8] = match status {
        400 => b"HTTP/1.1 400 Bad Request",
        413 => b"HTTP/1.1 413 Content Too Large",
        431 => b"HTTP/1.1 431 Request Header Fields Too Large",
        501 => b"HTTP/1.1 501 Not Implemented",
        _ => b"HTTP/1.1 400 Bad Request",
    };
    buf.extend_from_slice(line);
    buf.extend_from_slice(b"\r\nServer: M\r\nDate: ");
    crate::date::append_date(buf);
    buf.extend_from_slice(b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
}

/// A service error becomes a 500 whose body is a fixed phrase. The error itself is logged, not sent:
/// its text (paths, upstream addresses, driver messages) is for operators, not for whoever is on
/// the other end of an internet-facing socket.
#[cold]
pub(crate) fn encode_error(e: io::Error, buf: &mut BytesMut) {
    error!("error in service: err = {e:?}");
    let msg: &[u8] = b"Internal Server Error";

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

    #[test]
    fn header_kv_is_written_after_header_lines_and_rejects_splitting() {
        let mut rsp_buf = BytesMut::new();
        let mut kv = BytesMut::new();
        let mut out = BytesMut::new();
        {
            let mut res = Response::new(&mut rsp_buf, &mut kv);
            res.header("Content-Type: application/json");
            res.header_kv("X-Request-ID", "01J");
            res.header_kv("Set-Cookie", "a=1; Path=/");
            res.header_kv("X-Bad", "v\r\nInjected: 1");
            res.header_kv("Bad Name", "v");
            res.header_kv("", "v");
            res.body("ok");
            encode(res, &mut out);
        }
        let text = std::str::from_utf8(&out).unwrap();
        assert!(text.contains(
            "\r\nContent-Type: application/json\r\nX-Request-ID: 01J\r\nSet-Cookie: a=1; Path=/\r\n\r\nok"
        ), "{text}");
        assert!(!text.contains("Injected"));
        assert!(!text.contains("Bad Name"));
        assert!(kv.is_empty(), "the buffer is cleared for the next response");
    }

    #[test]
    fn chunk_size_line_is_lower_hex() {
        for (len, hex) in [
            (1_usize, "1"),
            (15, "f"),
            (16, "10"),
            (255, "ff"),
            (4096, "1000"),
            (usize::MAX, "ffffffffffffffff"),
        ] {
            let mut size = [0_u8; 2 * std::mem::size_of::<usize>()];
            let at = hex_digits(len, &mut size);
            assert_eq!(&size[at..], hex.as_bytes());
        }
    }

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
            let mut kv = BytesMut::new();
            let mut res = Response::new(&mut rsp_buf, &mut kv);
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
