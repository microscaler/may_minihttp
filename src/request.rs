use std::fmt;
use std::io::{self, BufRead, Read};
use std::mem::MaybeUninit;

/// Maximum header buffer size configurations.
///
/// This enum provides pre-defined buffer sizes for different use cases while
/// allowing custom sizes via the `Custom` variant.
///
/// # Examples
///
/// ```
/// use may_minihttp::MaxHeaders;
///
/// let default = MaxHeaders::Default;
/// assert_eq!(default.value(), 16);
///
/// let large = MaxHeaders::Large;
/// assert_eq!(large.value(), 64);
///
/// let custom = MaxHeaders::Custom(100);
/// assert_eq!(custom.value(), 100);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Maximum number of HTTP header lines to accept in a single request.
///
/// This enum controls how many **individual header lines** the parser will accept.
/// Each variant represents a specific count of header items (not bytes or kilobytes).
///
/// # What Counts as a Header?
///
/// Each line in the HTTP request counts as one header:
/// ```http
/// GET /api HTTP/1.1
/// Host: example.com              ← Header 1
/// User-Agent: Mozilla/5.0        ← Header 2
/// Accept: application/json       ← Header 3
/// X-Custom-Header: value         ← Header 4
/// ```
///
/// # Choosing the Right Size
///
/// | Variant | Count | Use Case | Example Environment |
/// |---------|-------|----------|---------------------|
/// | `Default` | 16 | Simple APIs, controlled environments | Direct API calls, testing |
/// | `Standard` | 32 | Most web applications | Standard web apps behind single proxy |
/// | `Large` | 64 | Complex deployments | Multiple proxies, load balancers |
/// | `XLarge` | 128 | Production infrastructure | Kubernetes, service mesh, multiple proxy layers |
///
/// # Real-World Header Counts
///
/// - **Direct browser request**: 10-15 headers
/// - **Browser + CDN**: 15-25 headers
/// - **Browser + CDN + Load Balancer**: 25-40 headers
/// - **Kubernetes (Ingress + Service Mesh)**: 40-80 headers
/// - **Full enterprise stack**: 60-100+ headers
///
/// # Memory Impact
///
/// Each header slot consumes a small amount of stack memory (typically ~24 bytes for
/// the header reference). Increasing the limit has minimal memory overhead.
///
/// # Examples
///
/// ```rust
/// use may_minihttp::MaxHeaders;
///
/// // For a simple API with direct clients
/// let simple = MaxHeaders::Default;  // 16 header items
/// assert_eq!(simple.value(), 16);
///
/// // For production Kubernetes deployment
/// let production = MaxHeaders::XLarge;  // 128 header items
/// assert_eq!(production.value(), 128);
///
/// // Custom size (clamped to 1-256 range)
/// let custom = MaxHeaders::Custom(96);  // 96 header items
/// assert_eq!(custom.value(), 96);
/// ```
#[derive(Default)]
pub enum MaxHeaders {
    /// Default: 16 header items (backwards compatible, minimal memory)
    ///
    /// Accepts up to **16 individual HTTP header lines**.
    ///
    /// **Suitable for**: Simple APIs, controlled environments, testing
    #[default]
    Default,

    /// Standard: 32 header items
    ///
    /// Accepts up to **32 individual HTTP header lines**.
    ///
    /// **Suitable for**: Most web applications, single proxy/load balancer
    Standard,

    /// Large: 64 header items
    ///
    /// Accepts up to **64 individual HTTP header lines**.
    ///
    /// **Suitable for**: Applications behind load balancers, CDN + proxy
    Large,

    /// `XLarge`: 128 header items
    ///
    /// Accepts up to **128 individual HTTP header lines**.
    ///
    /// **Suitable for**: Production services in Kubernetes, service mesh,
    /// multiple proxy layers, enterprise environments with extensive headers
    XLarge,

    /// Custom size: 1-256 header items
    ///
    /// Accepts up to the specified number of **individual HTTP header lines**.
    ///
    /// Values are automatically clamped:
    /// - Minimum: 16 (if 0 is specified)
    /// - Maximum: 256
    ///
    /// # Example
    /// ```rust
    /// use may_minihttp::MaxHeaders;
    ///
    /// let custom = MaxHeaders::Custom(96);  // 96 header items
    /// assert_eq!(custom.value(), 96);
    ///
    /// let clamped_low = MaxHeaders::Custom(0);  // Clamped to 16
    /// assert_eq!(clamped_low.value(), 16);
    ///
    /// let clamped_high = MaxHeaders::Custom(512);  // Clamped to 256
    /// assert_eq!(clamped_high.value(), 256);
    /// ```
    Custom(usize),
}

impl MaxHeaders {
    /// Get the numeric value of the max headers setting
    #[must_use]
    pub const fn value(&self) -> usize {
        match self {
            MaxHeaders::Default => 16,
            MaxHeaders::Standard => 32,
            MaxHeaders::Large => 64,
            MaxHeaders::XLarge => 128,
            MaxHeaders::Custom(n) => {
                // Clamp to reasonable range
                if *n == 0 {
                    16
                } else if *n > 256 {
                    256
                } else {
                    *n
                }
            }
        }
    }
}

/// Default maximum number of HTTP headers (backwards compatible)
pub(crate) const MAX_HEADERS: usize = MaxHeaders::Default.value();

use std::cell::Cell;

use bytes::{Buf, BufMut, BytesMut};
use may::net::TcpStream;

use crate::http_server::err;

/// Largest request head (request line and header fields) the server buffers. A client that sends
/// more without finishing the head is answered `431` and disconnected; before this limit the
/// buffer grew without bound, and the head-terminator scan made each read cost the whole buffer.
pub const MAX_REQUEST_HEAD_BYTES: usize = 64 * 1024;

/// Most request-body bytes the server discards on behalf of a handler that did not read the body,
/// so the connection can serve the next request. Past it the connection is closed instead.
const MAX_DISCARD_BYTES: usize = 1024 * 1024;

/// Longest chunk-size line (size, extensions, CRLF) accepted in a chunked request body.
const MAX_CHUNK_LINE_BYTES: usize = 4 * 1024;

/// Most trailer-section bytes accepted after the last chunk.
const MAX_TRAILER_BYTES: usize = 16 * 1024;

/// A request the server refuses before it reaches the service: answered with `status` and
/// `Connection: close`, then the connection is closed. Carried inside the `io::Error` that
/// [`decode`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RejectedRequest {
    pub status: u16,
    pub reason: &'static str,
}

impl fmt::Display for RejectedRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "request rejected with {}: {}", self.status, self.reason)
    }
}

impl std::error::Error for RejectedRequest {}

#[cold]
fn reject<T>(status: u16, reason: &'static str) -> io::Result<T> {
    err(io::Error::new(
        io::ErrorKind::InvalidData,
        RejectedRequest { status, reason },
    ))
}

/// The [`RejectedRequest`] inside an error returned by [`decode`], if it is one.
pub fn rejected_request(error: &io::Error) -> Option<RejectedRequest> {
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<RejectedRequest>())
        .copied()
}

/// How the request body is delimited (RFC 9112 section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    /// No `Content-Length` and no `Transfer-Encoding`: no body.
    Empty,
    Length(usize),
    Chunked,
}

/// Validate the framing headers. Every ambiguity a front proxy could resolve differently from this
/// server (the basis of request smuggling) is refused, not guessed:
///
/// - `Content-Length` must be digits only; several must agree;
/// - `Transfer-Encoding` with `Content-Length` is refused (RFC 9112 6.3 allows rejecting it);
/// - `Transfer-Encoding` must end in `chunked`, and `chunked` may appear only once and last;
/// - other transfer codings (gzip etc.) are not implemented: 501;
/// - `Transfer-Encoding` on an HTTP/1.0 request is faulty framing (RFC 9112 6.1).
fn framing(req: &httparse::Request<'_, '_>) -> io::Result<Framing> {
    let mut content_length: Option<usize> = None;
    let mut transfer_encoding = false;
    let mut chunked = false;
    let mut other_coding = false;
    for header in req.headers.iter() {
        if header.name.eq_ignore_ascii_case("content-length") {
            let value = parse_content_length(header.value)?;
            if content_length.is_some_and(|previous| previous != value) {
                return reject(400, "conflicting Content-Length headers");
            }
            content_length = Some(value);
        } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
            transfer_encoding = true;
            for coding in header.value.split(|&b| b == b',') {
                let coding = trim_ows(coding);
                if coding.is_empty() {
                    continue;
                }
                if chunked {
                    return reject(400, "chunked is not the final transfer coding");
                }
                if coding.eq_ignore_ascii_case(b"chunked") {
                    chunked = true;
                } else {
                    other_coding = true;
                }
            }
        }
    }
    if !transfer_encoding {
        return Ok(content_length.map_or(Framing::Empty, Framing::Length));
    }
    if req.version == Some(0) {
        return reject(400, "Transfer-Encoding in an HTTP/1.0 request");
    }
    if content_length.is_some() {
        return reject(400, "both Transfer-Encoding and Content-Length");
    }
    if !chunked {
        return reject(400, "Transfer-Encoding does not end in chunked");
    }
    if other_coding {
        return reject(501, "transfer coding not implemented");
    }
    Ok(Framing::Chunked)
}

fn trim_ows(mut value: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = value {
        value = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = value {
        value = rest;
    }
    value
}

fn parse_content_length(value: &[u8]) -> io::Result<usize> {
    let digits = trim_ows(value);
    // 19 digits always fit in a u64; anything longer is not a length this server will read.
    if digits.is_empty() || digits.len() > 19 || !digits.iter().all(u8::is_ascii_digit) {
        return reject(400, "invalid Content-Length");
    }
    let value = digits
        .iter()
        .fold(0_u64, |acc, &d| acc * 10 + u64::from(d - b'0'));
    usize::try_from(value).or_else(|_| reject(400, "invalid Content-Length"))
}

/// Where a body reader is in a chunked body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chunk {
    /// Expecting a chunk-size line.
    Size,
    /// Inside chunk data, this many bytes left.
    Data(usize),
    /// Expecting the CRLF after chunk data.
    DataEnd,
    /// After the last chunk: trailer fields until an empty line; bytes seen so far.
    Trailers(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyState {
    Length(usize),
    Chunked(Chunk),
    Done,
}

pub struct BodyReader<'buf, 'stream> {
    // bytes read from the socket and not yet consumed
    req_buf: &'buf mut BytesMut,
    state: BodyState,
    // set when the connection must close after this request's response (see `poison`)
    close: Option<&'buf Cell<bool>>,
    // discarding: never wait for the socket (see `discard_rest`)
    no_wait: bool,
    // used to read extra body bytes
    stream: &'stream mut TcpStream,
}

impl BodyReader<'_, '_> {
    fn read_more_data(&mut self) -> io::Result<usize> {
        if self.no_wait {
            return self.read_without_waiting();
        }
        crate::http_server::reserve_buf(self.req_buf);
        let read_buf: &mut [u8] = unsafe { std::mem::transmute(self.req_buf.chunk_mut()) };
        let n = self.stream.read(read_buf)?;
        unsafe { self.req_buf.advance_mut(n) };
        Ok(n)
    }

    /// The body cannot be delimited (malformed chunking, a truncated body, one too large to
    /// discard): nothing more on this connection can be trusted to start at a request boundary.
    /// Buffered bytes are dropped and the connection closes after the response.
    fn poison(&mut self) {
        self.state = BodyState::Done;
        self.req_buf.clear();
        match self.close {
            Some(close) => close.set(true),
            // Standalone `decode` callers have no close flag: stop reading so the bytes that
            // follow can never be parsed as a request.
            None => {
                let _ = self.stream.shutdown(std::net::Shutdown::Read);
            }
        }
    }

    #[cold]
    fn framing_error<T>(&mut self, msg: &'static str) -> io::Result<T> {
        self.poison();
        err(io::Error::new(io::ErrorKind::InvalidData, msg))
    }

    /// Make at least one byte available in `req_buf`. EOF before the body ends is an error.
    fn fill(&mut self) -> io::Result<()> {
        if self.req_buf.is_empty() && self.read_more_data()? == 0 {
            return self.framing_error("connection closed inside the request body");
        }
        Ok(())
    }

    /// Advance the chunked decoder until data is available or the body ends; returns the number of
    /// data bytes available in `req_buf` right now (0 means the body is complete).
    fn chunked_available(&mut self) -> io::Result<usize> {
        loop {
            let BodyState::Chunked(chunk) = self.state else {
                return Ok(0);
            };
            match chunk {
                Chunk::Data(0) => self.state = BodyState::Chunked(Chunk::DataEnd),
                Chunk::Data(remaining) => {
                    self.fill()?;
                    return Ok(remaining.min(self.req_buf.len()));
                }
                Chunk::DataEnd => {
                    while self.req_buf.len() < 2 {
                        if self.read_more_data()? == 0 {
                            return self.framing_error("connection closed inside the request body");
                        }
                    }
                    if &self.req_buf[..2] != b"\r\n" {
                        return self.framing_error("chunk data not followed by CRLF");
                    }
                    self.req_buf.advance(2);
                    self.state = BodyState::Chunked(Chunk::Size);
                }
                Chunk::Size => {
                    let line_end = self.line_end(MAX_CHUNK_LINE_BYTES)?;
                    let size = parse_chunk_size(&self.req_buf[..line_end]);
                    self.req_buf.advance(line_end + 2);
                    match size {
                        Some(0) => self.state = BodyState::Chunked(Chunk::Trailers(0)),
                        Some(size) => self.state = BodyState::Chunked(Chunk::Data(size)),
                        None => return self.framing_error("invalid chunk size line"),
                    }
                }
                Chunk::Trailers(seen) => {
                    let budget = MAX_TRAILER_BYTES.saturating_sub(seen);
                    let line_end = self.line_end(budget)?;
                    self.req_buf.advance(line_end + 2);
                    self.state = if line_end == 0 {
                        BodyState::Done
                    } else {
                        BodyState::Chunked(Chunk::Trailers(seen + line_end + 2))
                    };
                }
            }
        }
    }

    /// Offset of the CRLF that ends the line at the start of `req_buf`, reading as needed. A line
    /// longer than `limit` or ended by a bare LF poisons the connection.
    fn line_end(&mut self, limit: usize) -> io::Result<usize> {
        let mut scanned = 0;
        loop {
            if let Some(i) = self.req_buf[scanned..].iter().position(|&b| b == b'\n') {
                let lf = scanned + i;
                if lf == 0 || self.req_buf[lf - 1] != b'\r' {
                    return self.framing_error("chunked body line not ended by CRLF");
                }
                return Ok(lf - 1);
            }
            scanned = self.req_buf.len();
            if scanned > limit {
                return self.framing_error("chunked body line too long");
            }
            if self.read_more_data()? == 0 {
                return self.framing_error("connection closed inside the request body");
            }
        }
    }

    /// Bytes of body data available in `req_buf` now, reading from the socket if none are.
    fn available(&mut self) -> io::Result<usize> {
        match self.state {
            BodyState::Done => Ok(0),
            BodyState::Length(0) => {
                self.state = BodyState::Done;
                Ok(0)
            }
            BodyState::Length(remaining) => {
                self.fill()?;
                Ok(remaining.min(self.req_buf.len()))
            }
            BodyState::Chunked(_) => self.chunked_available(),
        }
    }

    fn consumed(&mut self, n: usize) {
        self.req_buf.advance(n);
        match &mut self.state {
            BodyState::Length(remaining) | BodyState::Chunked(Chunk::Data(remaining)) => {
                *remaining -= n;
            }
            _ => {}
        }
    }

    /// Discard the rest of the body so the connection can carry the next request. Only bytes that
    /// can be had without waiting are read (the client may never send a body the handler did not
    /// want, and nothing else bounds that wait); a body that is not all here, or is larger than
    /// `MAX_DISCARD_BYTES`, poisons the connection instead.
    fn discard_rest(&mut self) {
        self.no_wait = true;
        let mut discarded = 0_usize;
        loop {
            if matches!(self.state, BodyState::Length(remaining) if remaining > MAX_DISCARD_BYTES) {
                return self.poison();
            }
            match self.available() {
                Ok(0) => return,
                Ok(n) => {
                    discarded += n;
                    if discarded > MAX_DISCARD_BYTES {
                        return self.poison();
                    }
                    self.consumed(n);
                }
                Err(_) => return self.poison(),
            }
        }
    }

    /// Read whatever the socket holds right now; `WouldBlock` when nothing is there.
    #[cfg(unix)]
    fn read_without_waiting(&mut self) -> io::Result<usize> {
        crate::http_server::reserve_buf(self.req_buf);
        let read_buf: &mut [u8] = unsafe { std::mem::transmute(self.req_buf.chunk_mut()) };
        let n = self.stream.inner_mut().read(read_buf)?;
        unsafe { self.req_buf.advance_mut(n) };
        Ok(n)
    }

    #[cfg(not(unix))]
    fn read_without_waiting(&mut self) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

/// Size of a chunk-size line (`1*HEXDIG [ BWS ";" chunk-ext ]`, CRLF excluded). Extensions are
/// ignored; a size that does not fit, no digits, or anything else after the digits is `None`.
fn parse_chunk_size(line: &[u8]) -> Option<usize> {
    let digits = line.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if digits == 0 || digits > 2 * std::mem::size_of::<usize>() {
        return None;
    }
    let rest = &line[digits..];
    let after_ws = trim_ows(rest);
    if !after_ws.is_empty() && after_ws[0] != b';' {
        return None;
    }
    if after_ws.is_empty() && !rest.is_empty() {
        // trailing whitespace with no extension
        return None;
    }
    line[..digits].iter().try_fold(0_usize, |acc, &b| {
        let digit = (b as char).to_digit(16)? as usize;
        acc.checked_mul(16)?.checked_add(digit)
    })
}

impl Read for BodyReader<'_, '_> {
    // the user should control the body reading, don't exceeds the body!
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.available()?.min(buf.len());
        buf[..n].copy_from_slice(&self.req_buf[..n]);
        self.consumed(n);
        Ok(n)
    }
}

impl BufRead for BodyReader<'_, '_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        let n = self.available()?;
        Ok(&self.req_buf.chunk()[..n])
    }

    fn consume(&mut self, amt: usize) {
        let available = match self.state {
            BodyState::Length(remaining) | BodyState::Chunked(Chunk::Data(remaining)) => {
                remaining.min(self.req_buf.len())
            }
            _ => 0,
        };
        assert!(amt <= available);
        self.consumed(amt);
    }
}

impl<'buf, 'stream> BodyReader<'buf, 'stream> {
    /// Discard the rest of the request body and return the TCP stream so the service can write a
    /// streamed response (chunked / SSE) inside `call`. A body that cannot be discarded without
    /// waiting closes the connection after the response.
    pub fn into_stream(self) -> &'stream mut TcpStream {
        let mut this = std::mem::ManuallyDrop::new(self);
        this.discard_rest();
        // Safety: ManuallyDrop skips Drop; the stream borrow is moved out exactly once.
        unsafe { std::ptr::addr_of_mut!(this.stream).read() }
    }
}

impl Drop for BodyReader<'_, '_> {
    fn drop(&mut self) {
        self.discard_rest();
    }
}

// we should hold the mut ref of req_buf
// before into body, this req_buf is only for holding headers
// after into body, this req_buf is mutable to read extra body bytes
// and the headers buf can be reused
pub struct Request<'buf, 'header, 'stream> {
    req: httparse::Request<'header, 'buf>,
    req_buf: &'buf mut BytesMut,
    framing: Framing,
    close: Option<&'buf Cell<bool>>,
    stream: &'stream mut TcpStream,
}

impl<'buf, 'stream> Request<'buf, '_, 'stream> {
    pub fn method(&self) -> &str {
        self.req.method.unwrap()
    }

    pub fn path(&self) -> &str {
        self.req.path.unwrap()
    }

    pub fn version(&self) -> u8 {
        self.req.version.unwrap()
    }

    pub fn headers(&self) -> &[httparse::Header<'_>] {
        self.req.headers
    }

    /// Reader over the request body, decoded per the request's framing (`Content-Length` or
    /// chunked). Whatever the service leaves unread is discarded before the next request.
    pub fn body(self) -> BodyReader<'buf, 'stream> {
        let this = std::mem::ManuallyDrop::new(self);
        // Safety: ManuallyDrop skips Request's Drop; each field is moved out exactly once.
        let (req_buf, stream) = unsafe {
            (
                std::ptr::addr_of!(this.req_buf).read(),
                std::ptr::addr_of!(this.stream).read(),
            )
        };
        BodyReader {
            req_buf,
            state: match this.framing {
                Framing::Empty => BodyState::Done,
                Framing::Length(n) => BodyState::Length(n),
                Framing::Chunked => BodyState::Chunked(Chunk::Size),
            },
            close: this.close,
            no_wait: false,
            stream,
        }
    }

    /// Mutable access to the underlying TCP stream (response streaming / SSE).
    #[inline]
    pub fn stream_mut(&mut self) -> &mut TcpStream {
        self.stream
    }
}

impl Drop for Request<'_, '_, '_> {
    /// A service that never asked for the body: discard it (or close the connection) so its bytes
    /// are not parsed as the next request. Before this, a body the handler ignored was read as a
    /// pipelined request: a request smuggled inside it ran.
    fn drop(&mut self) {
        if self.framing == Framing::Empty {
            return;
        }
        let mut body = BodyReader {
            req_buf: &mut *self.req_buf,
            state: match self.framing {
                Framing::Empty => BodyState::Done,
                Framing::Length(n) => BodyState::Length(n),
                Framing::Chunked => BodyState::Chunked(Chunk::Size),
            },
            close: self.close,
            no_wait: true,
            stream: &mut *self.stream,
        };
        body.discard_rest();
    }
}

impl fmt::Debug for Request<'_, '_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<HTTP Request {} {}>", self.method(), self.path())
    }
}

/// Decode one request from the start of `req_buf`. `Ok(None)` means more bytes are needed.
///
/// Malformed or ambiguous requests are refused with an error carrying a [`RejectedRequest`]
/// (`400`, `431` head too large or too many headers, `501` unsupported transfer coding); the
/// server answers it and closes the connection.
pub fn decode<'header, 'buf, 'stream, const N: usize>(
    headers: &'header mut [MaybeUninit<httparse::Header<'buf>>; N],
    req_buf: &'buf mut BytesMut,
    stream: &'stream mut TcpStream,
) -> io::Result<Option<Request<'buf, 'header, 'stream>>> {
    decode_with_close(headers, req_buf, stream, None)
}

pub(crate) fn decode_with_close<'header, 'buf, 'stream, const N: usize>(
    headers: &'header mut [MaybeUninit<httparse::Header<'buf>>; N],
    req_buf: &'buf mut BytesMut,
    stream: &'stream mut TcpStream,
    close: Option<&'buf Cell<bool>>,
) -> io::Result<Option<Request<'buf, 'header, 'stream>>> {
    let mut req = httparse::Request::new(&mut []);
    // safety: don't hold the reference of req_buf
    // so we can transfer the mutable reference to Request
    let buf: &[u8] = unsafe { std::mem::transmute(req_buf.chunk()) };

    // No separate `\r\n\r\n` pre-scan: `httparse` returns `Status::Partial` on any truncated
    // head (mid-method, mid-path, mid-header, or no terminating `\r\n\r\n` yet), which maps to
    // `Ok(None)` below, and malformed bytes are a permanent 400 whatever arrives later. Only the
    // first MAX_REQUEST_HEAD_BYTES are parsed: a head that has not ended by then is refused (431)
    // rather than buffered or re-parsed without bound.
    let window = &buf[..buf.len().min(MAX_REQUEST_HEAD_BYTES)];

    let header_limit = headers.len();
    let status = match req.parse_with_uninit_headers(window, headers) {
        Ok(s) => s,
        Err(httparse::Error::TooManyHeaders) => {
            warn!(
                "request refused: more than {header_limit} header fields; consider \
                 HttpServerWithHeaders with a larger MaxHeaders"
            );
            return reject(431, "too many header fields");
        }
        Err(e) => {
            debug!("request refused: unparsable request head: {e:?}");
            return reject(400, "malformed request head");
        }
    };

    let len = match status {
        httparse::Status::Complete(amt) => amt,
        httparse::Status::Partial if buf.len() >= MAX_REQUEST_HEAD_BYTES => {
            return reject(431, "request head too large");
        }
        httparse::Status::Partial => return Ok(None),
    };
    let framing = framing(&req)?;
    req_buf.advance(len);

    Ok(Some(Request {
        req,
        req_buf,
        framing,
        close,
        stream,
    }))
}

/// Decode HTTP request with Default (16) headers
///
/// # Errors
///
/// Returns an error if:
/// - The TCP stream cannot be read
/// - The HTTP request is malformed
/// - The number of headers exceeds 16
pub fn decode_default<'header, 'buf, 'stream>(
    headers: &'header mut [MaybeUninit<httparse::Header<'buf>>; 16],
    req_buf: &'buf mut BytesMut,
    stream: &'stream mut TcpStream,
) -> io::Result<Option<Request<'buf, 'header, 'stream>>> {
    decode(headers, req_buf, stream)
}

/// Decode HTTP request with Standard (32) headers
///
/// # Errors
///
/// Returns an error if:
/// - The TCP stream cannot be read
/// - The HTTP request is malformed
/// - The number of headers exceeds 32
pub fn decode_standard<'header, 'buf, 'stream>(
    headers: &'header mut [MaybeUninit<httparse::Header<'buf>>; 32],
    req_buf: &'buf mut BytesMut,
    stream: &'stream mut TcpStream,
) -> io::Result<Option<Request<'buf, 'header, 'stream>>> {
    decode(headers, req_buf, stream)
}

/// Decode HTTP request with Large (64) headers
///
/// # Errors
///
/// Returns an error if:
/// - The TCP stream cannot be read
/// - The HTTP request is malformed
/// - The number of headers exceeds 64
pub fn decode_large<'header, 'buf, 'stream>(
    headers: &'header mut [MaybeUninit<httparse::Header<'buf>>; 64],
    req_buf: &'buf mut BytesMut,
    stream: &'stream mut TcpStream,
) -> io::Result<Option<Request<'buf, 'header, 'stream>>> {
    decode(headers, req_buf, stream)
}

/// Decode HTTP request with `XLarge` (128) headers
///
/// # Errors
///
/// Returns an error if:
/// - The TCP stream cannot be read
/// - The HTTP request is malformed
/// - The number of headers exceeds 128
pub fn decode_xlarge<'header, 'buf, 'stream>(
    headers: &'header mut [MaybeUninit<httparse::Header<'buf>>; 128],
    req_buf: &'buf mut BytesMut,
    stream: &'stream mut TcpStream,
) -> io::Result<Option<Request<'buf, 'header, 'stream>>> {
    decode(headers, req_buf, stream)
}
