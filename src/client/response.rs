use std::fmt;
use std::io::{self, Read};
use std::ops::{Deref, DerefMut};

use bytes::BytesMut;
use http::header::*;
use http::{self, HeaderMap, Version};
use httparse;

use crate::client::body::BodyReader;
use crate::client::shared::SharedStream;

pub(crate) const DEFAULT_MAX_RESPONSE_HEADER_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_HEADERS: usize = 128;

#[cfg(test)]
pub(crate) fn decode(buf: &mut BytesMut) -> io::Result<Option<Response>> {
    decode_with_limit(buf, DEFAULT_MAX_RESPONSE_HEADER_BYTES)
}

pub(crate) fn decode_with_limit(
    buf: &mut BytesMut,
    max_header_bytes: usize,
) -> io::Result<Option<Response>> {
    let header_end = buf.windows(4).position(|window| window == b"\r\n\r\n");
    match header_end {
        Some(offset) if offset + 4 > max_header_bytes => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("HTTP response headers exceed configured {max_header_bytes}-byte limit"),
            ));
        }
        None if buf.len() >= max_header_bytes => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("HTTP response headers exceed configured {max_header_bytes}-byte limit"),
            ));
        }
        _ => {}
    }

    // Parse into owned response metadata before mutating `buf`. `httparse`
    // stores header slices that borrow the input buffer, so splitting the
    // buffer while the parser is alive would violate Rust's aliasing rules.
    let (head_len, version, status_code, response_headers) = {
        // Keep the header table off the small may coroutine stack.
        let mut headers = vec![httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
        let mut parsed = httparse::Response::new(&mut headers);
        let status = parsed.parse(buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse HTTP response: {e}"),
            )
        })?;

        let head_len = match status {
            httparse::Status::Complete(amount) => amount,
            httparse::Status::Partial => return Ok(None),
        };
        let version = match parsed.version {
            Some(0) => Version::HTTP_10,
            Some(1) => Version::HTTP_11,
            Some(version) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported HTTP response version: 1.{version}"),
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP response missing version",
                ));
            }
        };
        let status_code = parsed.code.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP response missing status code",
            )
        })?;
        let response_headers = parsed
            .headers
            .iter()
            .map(|header| {
                let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid HTTP response header name: {e}"),
                    )
                })?;
                let value = HeaderValue::from_bytes(header.value).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid HTTP response header value: {e}"),
                    )
                })?;
                Ok((name, value))
            })
            .collect::<io::Result<Vec<_>>>()?;

        (head_len, version, status_code, response_headers)
    };

    // The parser and all header borrows are gone, so advancing the input is safe.
    let _ = buf.split_to(head_len);

    let mut rsp_builder = http::Response::builder();
    rsp_builder = rsp_builder.status(status_code).version(version);

    for (name, value) in response_headers {
        rsp_builder = rsp_builder.header(name, value);
    }

    rsp_builder
        .body(BodyReader::EmptyReader)
        .map(|req| Some(Response(req)))
        .map_err(|e| {
            let msg = format!("failed to build http Response: {e:?}");
            io::Error::other(msg)
        })
}

/// HTTP response from a client request.
pub struct Response(http::Response<BodyReader>);

impl Response {
    pub(crate) fn set_reader(&mut self, reader: SharedStream, expect_body: bool) -> io::Result<()> {
        let content_length = parse_content_length(self.headers())?;
        let transfer_encoding = parse_transfer_encoding(self.headers())?;
        if content_length.is_some() && transfer_encoding.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP response contains both Transfer-Encoding and Content-Length",
            ));
        }

        let status_forbids_body = self.status().is_informational()
            || self.status() == http::StatusCode::NO_CONTENT
            || self.status() == http::StatusCode::NOT_MODIFIED;
        if self.status() == http::StatusCode::NO_CONTENT && content_length.is_some_and(|n| n != 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "204 response contains a non-zero Content-Length",
            ));
        }
        if !expect_body || status_forbids_body {
            reader.mark_response_complete();
            *self.body_mut() = BodyReader::EmptyReader;
            return Ok(());
        }

        if content_length == Some(0) {
            reader.mark_response_complete();
        } else {
            reader.mark_response_pending();
        }

        let body_reader = match (content_length, transfer_encoding) {
            (Some(n), _) => BodyReader::SizedReader(reader, n),
            (None, Some(())) => BodyReader::ChunkReader(reader, None),
            (None, None) => BodyReader::EofReader(Some(reader)),
        };

        *self.body_mut() = body_reader;
        Ok(())
    }

    pub(crate) fn abandon_body(&mut self) {
        self.body_mut().abandon();
    }

    pub(crate) fn body_complete(&self) -> bool {
        self.body().is_complete()
    }

    pub(crate) fn set_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        self.body().set_timeout(timeout)
    }

    /// Deserialize the remaining response body as JSON.
    ///
    /// This consumes bytes from the streaming body. Call it at most once unless the caller has
    /// independently buffered and reconstructed the response.
    #[cfg(feature = "json")]
    pub fn json<T: serde::de::DeserializeOwned>(&mut self) -> io::Result<T> {
        serde_json::from_reader(self).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSON deserialization failed: {error}"),
            )
        })
    }
}

fn parse_content_length(headers: &HeaderMap) -> io::Result<Option<usize>> {
    let mut parsed = None;
    for value in headers.get_all(CONTENT_LENGTH) {
        let value = value.to_str().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("malformed Content-Length: {error}"),
            )
        })?;
        for item in value.split(',') {
            let item = item.trim();
            if item.is_empty() || !item.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "malformed Content-Length",
                ));
            }
            let length = item.parse::<usize>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("malformed Content-Length: {error}"),
                )
            })?;
            if parsed.is_some_and(|previous| previous != length) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "conflicting Content-Length values",
                ));
            }
            parsed = Some(length);
        }
    }
    Ok(parsed)
}

fn parse_transfer_encoding(headers: &HeaderMap) -> io::Result<Option<()>> {
    let mut codings = Vec::new();
    for value in headers.get_all(TRANSFER_ENCODING) {
        let value = value.to_str().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("malformed Transfer-Encoding: {error}"),
            )
        })?;
        codings.extend(
            value
                .split(',')
                .map(str::trim)
                .filter(|coding| !coding.is_empty()),
        );
    }
    if codings.is_empty() {
        return Ok(None);
    }
    if codings.len() != 1 || !codings[0].eq_ignore_ascii_case("chunked") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported or invalid HTTP Transfer-Encoding; only chunked is supported",
        ));
    }
    Ok(Some(()))
}

impl Deref for Response {
    type Target = http::Response<BodyReader>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Response {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Read for Response {
    #[inline]
    fn read(&mut self, msg: &mut [u8]) -> io::Result<usize> {
        self.body_mut().read(msg)
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<HTTP Response {} {:?}>", self.status(), self.version())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use std::io::{Cursor, Read, Write};

    use super::decode;

    struct FakeReader;

    impl Read for FakeReader {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Ok(0)
        }
    }

    impl Write for FakeReader {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn build_response(status: u16, headers: &[(&str, &str)], body: &str) -> String {
        let mut resp = format!("HTTP/1.1 {}\r\n", status);
        for (name, value) in headers {
            resp.push_str(&format!("{}: {}\r\n", name, value));
        }
        if !body.is_empty() {
            resp.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        resp.push_str("\r\n");
        resp.push_str(body);
        resp
    }

    #[test]
    fn test_decode_valid_200() {
        let text = build_response(200, &[("Server", "test")], "hello");
        let mut buf = BytesMut::from(text.as_bytes());
        let rsp = decode(&mut buf).unwrap().unwrap();
        assert_eq!(rsp.status().as_u16(), 200);
        assert_eq!(rsp.version(), http::Version::HTTP_11);
        assert_eq!(rsp.headers()["Server"], "test");
        assert_eq!(buf.as_ref(), b"hello");
    }

    #[test]
    fn test_decode_partial() {
        let mut buf = BytesMut::from(b"HTTP/1.1 200 OK\r\nServer: t".as_slice());
        assert!(decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn response_header_limit_is_enforced_before_body_bytes() {
        let mut oversized = BytesMut::from(
            format!("HTTP/1.1 200 OK\r\nX-Large: {}\r\n\r\n", "a".repeat(64)).as_bytes(),
        );
        let error = super::decode_with_limit(&mut oversized, 32).unwrap_err();
        assert!(error.to_string().contains("headers exceed"));

        let mut body_is_not_counted =
            BytesMut::from(b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\n".as_slice());
        body_is_not_counted.extend_from_slice(&[b'x'; 64]);
        assert!(super::decode_with_limit(&mut body_is_not_counted, 48)
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_decode_content_length() {
        let text = build_response(200, &[("Content-Length", "5")], "hello");
        let mut buf = BytesMut::from(text.as_bytes());
        let rsp = decode(&mut buf).unwrap().unwrap();
        assert!(rsp.headers().get("Content-Length").is_some());
    }

    #[test]
    fn test_decode_http10() {
        let text = "HTTP/1.0 200 OK\r\n\r\n";
        let mut buf = BytesMut::from(text.as_bytes());
        let rsp = decode(&mut buf).unwrap().unwrap();
        assert_eq!(rsp.version(), http::Version::HTTP_10);
    }

    #[test]
    fn test_decode_malformed() {
        let mut buf = BytesMut::from(b"not a response".as_slice());
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_set_reader_with_expect_body() {
        let text = build_response(200, &[("Content-Length", "5")], "");
        let mut buf = BytesMut::from(text.as_bytes());
        let mut rsp = decode(&mut buf).unwrap().unwrap();

        let reader = super::SharedStream::test(FakeReader);
        rsp.set_reader(reader, true).unwrap();

        match rsp.body() {
            super::BodyReader::SizedReader(_, ref n) => assert_eq!(*n, 5),
            _ => panic!("expected SizedReader"),
        }
    }

    #[test]
    fn test_decode_set_reader_no_body() {
        let text = build_response(200, &[] as &[(&str, &str)], "");
        let mut buf = BytesMut::from(text.as_bytes());
        let mut rsp = decode(&mut buf).unwrap().unwrap();

        let reader = super::SharedStream::test(FakeReader);
        rsp.set_reader(reader, false).unwrap();

        assert!(matches!(*rsp.body(), super::BodyReader::EmptyReader));
    }

    #[test]
    fn test_decode_set_reader_bad_cl() {
        let text = build_response(200, &[("Content-Length", "abc")], "");
        let mut buf = BytesMut::from(text.as_bytes());
        let mut rsp = decode(&mut buf).unwrap().unwrap();

        let reader = super::SharedStream::test(FakeReader);
        let err = rsp.set_reader(reader, true).unwrap_err();
        assert!(err.to_string().contains("malformed Content-Length"));
    }

    #[test]
    fn response_rejects_ambiguous_framing() {
        for headers in [
            vec![("Content-Length", "3"), ("Content-Length", "4")],
            vec![("Content-Length", "3"), ("Transfer-Encoding", "chunked")],
            vec![("Transfer-Encoding", "gzip, chunked")],
        ] {
            let text = build_response(200, &headers, "");
            let mut buf = BytesMut::from(text.as_bytes());
            let mut response = decode(&mut buf).unwrap().unwrap();
            let error = response
                .set_reader(super::SharedStream::test(FakeReader), true)
                .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn identical_repeated_content_lengths_are_accepted() {
        let text = build_response(200, &[("Content-Length", "5"), ("Content-Length", "5")], "");
        let mut buf = BytesMut::from(text.as_bytes());
        let mut response = decode(&mut buf).unwrap().unwrap();
        response
            .set_reader(super::SharedStream::test(FakeReader), true)
            .unwrap();
        assert!(matches!(
            response.body(),
            super::BodyReader::SizedReader(_, 5)
        ));
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_deserializes_streaming_body() {
        let text = build_response(200, &[("Content-Length", "11")], "");
        let mut buf = BytesMut::from(text.as_bytes());
        let mut response = decode(&mut buf).unwrap().unwrap();
        let reader = super::SharedStream::test(Cursor::new(br#"{"ok":true}"#.to_vec()));
        response.set_reader(reader, true).unwrap();

        let value: serde_json::Value = response.json().unwrap();
        assert_eq!(value, serde_json::json!({"ok": true}));
    }
}
