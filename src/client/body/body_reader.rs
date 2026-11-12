use std::fmt;
use std::io::{self, Read};

use crate::client::shared::SharedStream;

use super::BodyReader::*;

const MAX_CHUNK_LINE_BYTES: usize = 8 * 1024;
const MAX_TRAILER_BYTES: usize = 16 * 1024;

#[allow(clippy::enum_variant_names)]
pub enum BodyReader {
    SizedReader(SharedStream, usize),
    ChunkReader(SharedStream, Option<usize>),
    EofReader(Option<SharedStream>),
    EmptyReader,
}

impl fmt::Debug for BodyReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let name = match *self {
            SizedReader(..) => "SizedReader",
            ChunkReader(..) => "ChunkReader",
            EofReader(..) => "EofReader",
            EmptyReader => "EmptyReader",
        };
        write!(f, "BodyReader {}", name)
    }
}

impl Read for BodyReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use std::cmp;
        match *self {
            SizedReader(ref r, ref mut remain) => {
                let len = cmp::min(*remain, buf.len());
                if len == 0 {
                    r.mark_response_complete();
                    return Ok(0);
                }
                let mut r = r.clone();
                let n = r.read(&mut buf[0..len])?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed before Content-Length bytes were received",
                    ));
                }
                *remain -= n;
                if *remain == 0 {
                    r.mark_response_complete();
                }
                Ok(n)
            }
            ChunkReader(ref r, ref mut opt_remaining) => {
                let mut r = r.clone();
                let mut rem = match *opt_remaining {
                    Some(ref rem) => *rem,
                    // None means we don't know the size of the next chunk
                    None => read_chunk_size(&mut r)?,
                };
                trace!("Chunked read, remaining={:?}", rem);

                if rem == 0 {
                    if opt_remaining.is_none() {
                        consume_trailers(&mut r)?;
                    }

                    *opt_remaining = Some(0);
                    r.mark_response_complete();

                    trace!("end of chunked");

                    return Ok(0);
                }

                let to_read = cmp::min(rem, buf.len());
                let count = r.read(&mut buf[..to_read])?;

                if count == 0 {
                    *opt_remaining = Some(0);
                    return Err(io::Error::other("early eof"));
                }

                rem -= count;
                *opt_remaining = if rem > 0 {
                    Some(rem)
                } else {
                    eat(&mut r, b"\r\n")?;
                    None
                };
                Ok(count)
            }
            EofReader(Some(ref r)) => {
                let mut r = r.clone();
                let read = r.read(buf)?;
                if read == 0 {
                    r.mark_response_complete();
                }
                Ok(read)
            }
            EofReader(None) => Ok(0),
            EmptyReader => Ok(0),
        }
    }
}

impl BodyReader {
    pub(crate) fn is_complete(&self) -> bool {
        match self {
            Self::SizedReader(_, remaining) => *remaining == 0,
            Self::ChunkReader(_, remaining) => *remaining == Some(0),
            Self::EofReader(reader) => reader.is_none(),
            Self::EmptyReader => true,
        }
    }

    pub(crate) fn set_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        match self {
            Self::SizedReader(stream, _)
            | Self::ChunkReader(stream, _)
            | Self::EofReader(Some(stream)) => stream.set_timeout(timeout),
            Self::EofReader(None) | Self::EmptyReader => Ok(()),
        }
    }

    pub(crate) fn abandon(&mut self) {
        match self {
            Self::SizedReader(_, remaining) => *remaining = 0,
            Self::ChunkReader(_, remaining) => *remaining = Some(0),
            Self::EofReader(reader) => {
                let _ = reader.take();
            }
            Self::EmptyReader => {}
        }
    }
}

fn eat(rdr: &mut dyn Read, bytes: &[u8]) -> io::Result<()> {
    let mut buf = [0];
    for &b in bytes.iter() {
        match rdr.read(&mut buf)? {
            1 if buf[0] == b => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid characters found",
                ));
            }
        }
    }
    Ok(())
}

/// Chunked chunks start with 1*HEXDIGIT, indicating the size of the chunk.
fn read_chunk_size(rdr: &mut dyn Read) -> io::Result<usize> {
    macro_rules! byte (
        ($rdr:ident) => ({
            let mut buf = [0];
            match $rdr.read(&mut buf)? {
                1 => buf[0],
                _ => return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid chunk size line, read byte",
                )),
            }
        })
    );
    let mut size = 0;
    let mut in_ext = false;
    let mut in_chunk_size = true;
    let mut line_bytes = 0_usize;
    let mut saw_digit = false;
    loop {
        line_bytes += 1;
        if line_bytes > MAX_CHUNK_LINE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk-size line exceeds configured limit",
            ));
        }
        match byte!(rdr) {
            b @ b'0'..=b'9' if in_chunk_size => {
                saw_digit = true;
                size = checked_hex_digit(size, b - b'0')?;
            }
            b @ b'a'..=b'f' if in_chunk_size => {
                saw_digit = true;
                size = checked_hex_digit(size, b + 10 - b'a')?;
            }
            b @ b'A'..=b'F' if in_chunk_size => {
                saw_digit = true;
                size = checked_hex_digit(size, b + 10 - b'A')?;
            }
            b'\r' if saw_digit => match byte!(rdr) {
                b'\n' => break,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Invalid chunk size line, read new line",
                    ));
                }
            },
            b';' if !in_ext => {
                in_ext = true;
                in_chunk_size = false;
            }
            b'\t' | b' ' if !in_ext && !in_chunk_size => {}
            b'\t' | b' ' if in_chunk_size => in_chunk_size = false,
            ext if in_ext => {
                error!("chunk extension byte={}", ext);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid chunk size line, unknown byte",
                ));
            }
        }
    }
    trace!("chunk size={:?}", size);
    Ok(size)
}

fn checked_hex_digit(size: usize, digit: u8) -> io::Result<usize> {
    size.checked_mul(16)
        .and_then(|value| value.checked_add(digit as usize))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chunk size overflows usize"))
}

fn consume_trailers(reader: &mut dyn Read) -> io::Result<()> {
    let mut total = 0_usize;
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        if reader.read(&mut byte)? != 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed inside chunk trailers",
            ));
        }
        total += 1;
        if total > MAX_TRAILER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk trailers exceed configured limit",
            ));
        }
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            if line.len() == 2 {
                return Ok(());
            }
            if !line[..line.len() - 2].contains(&b':') {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "malformed chunk trailer field",
                ));
            }
            line.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use super::*;

    struct TestReader {
        data: Vec<u8>,
        pos: usize,
    }

    impl Read for TestReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let remaining = &self.data[self.pos..];
            let len = std::cmp::min(buf.len(), remaining.len());
            if len == 0 {
                return Ok(0);
            }
            buf[..len].copy_from_slice(&remaining[..len]);
            self.pos += len;
            Ok(len)
        }
    }

    impl Write for TestReader {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn stream(data: &[u8]) -> SharedStream {
        SharedStream::test(TestReader {
            data: data.to_vec(),
            pos: 0,
        })
    }

    // --- eat tests ---

    #[test]
    fn test_eat_valid() {
        let reader = &mut TestReader {
            data: b"\r\nhello".to_vec(),
            pos: 0,
        };
        eat(reader, b"\r\n").unwrap();
    }

    #[test]
    fn test_eat_invalid() {
        let reader = &mut TestReader {
            data: b"XXhello".to_vec(),
            pos: 0,
        };
        assert!(eat(reader, b"\r\n").is_err());
    }

    // --- read_chunk_size tests ---

    #[test]
    fn test_read_chunk_size_basic() {
        let reader = &mut TestReader {
            data: b"FF\r\n".to_vec(),
            pos: 0,
        };
        assert_eq!(read_chunk_size(reader).unwrap(), 255);
    }

    #[test]
    fn test_read_chunk_size_small() {
        let reader = &mut TestReader {
            data: b"5\r\n".to_vec(),
            pos: 0,
        };
        assert_eq!(read_chunk_size(reader).unwrap(), 5);
    }

    #[test]
    fn test_read_chunk_size_with_extension() {
        let reader = &mut TestReader {
            data: b"5;ext=val\r\n".to_vec(),
            pos: 0,
        };
        assert_eq!(read_chunk_size(reader).unwrap(), 5);
    }

    #[test]
    fn test_read_chunk_size_zero() {
        let reader = &mut TestReader {
            data: b"0\r\n\r\n".to_vec(),
            pos: 0,
        };
        assert_eq!(read_chunk_size(reader).unwrap(), 0);
    }

    #[test]
    fn test_read_chunk_size_invalid() {
        let reader = &mut TestReader {
            data: b"ZZ\r\n".to_vec(),
            pos: 0,
        };
        assert!(read_chunk_size(reader).is_err());
    }

    #[test]
    fn test_read_chunk_size_overflow_is_rejected() {
        let reader = &mut TestReader {
            data: format!("{}\r\n", "F".repeat(usize::BITS as usize / 4 + 1)).into_bytes(),
            pos: 0,
        };
        assert_eq!(
            read_chunk_size(reader).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    // --- BodyReader tests ---

    #[test]
    fn test_sized_reader_exact_bytes() {
        let reader = stream(b"hello world!");
        let mut br = BodyReader::SizedReader(reader, 12);
        let mut buf = [0u8; 12];
        assert_eq!(br.read(&mut buf).unwrap(), 12);
        assert_eq!(&buf, b"hello world!");
        let mut buf2 = [0u8; 4];
        assert_eq!(br.read(&mut buf2).unwrap(), 0);
    }

    #[test]
    fn test_sized_reader_zero_remain() {
        let reader = stream(b"nope");
        let mut br = BodyReader::SizedReader(reader, 0);
        let mut buf = [0u8; 4];
        assert_eq!(br.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn test_chunk_reader_multiple_chunks() {
        // 5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n
        let data = b"5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n";
        let reader = stream(data);
        let mut br = BodyReader::ChunkReader(reader, None);
        let mut buf = [0u8; 10];
        // First read: chunk size 5, body "hello"
        assert_eq!(br.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf[..5], b"hello");
        // Second read: chunk size 5, body "world"
        assert_eq!(br.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf[..5], b"world");
        // Exhausted
        assert_eq!(br.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn test_chunk_reader_chunk_extensions() {
        // 5;ext=val\r\nhello\r\n0\r\n\r\n
        let data = b"5;ext=val\r\nhello\r\n0\r\n\r\n";
        let reader = stream(data);
        let mut br = BodyReader::ChunkReader(reader, None);
        let mut buf = [0u8; 5];
        assert_eq!(br.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"hello");
        assert_eq!(br.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn test_chunk_reader_consumes_trailers() {
        let data = b"5\r\nhello\r\n0\r\nX-Checksum: yes\r\nX-Other: ok\r\n\r\n";
        let reader = stream(data);
        let mut br = BodyReader::ChunkReader(reader, None);
        let mut buf = [0u8; 5];
        assert_eq!(br.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"hello");
        assert_eq!(br.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn test_chunk_reader_early_eof() {
        let data = b"10\r\nhel";
        let reader = stream(data);
        let mut br = BodyReader::ChunkReader(reader, None);
        let mut buf = [0u8; 10];
        assert_eq!(br.read(&mut buf).unwrap(), 3);
        assert!(br.read(&mut buf).is_err());
    }

    #[test]
    fn test_empty_reader_always_zero() {
        let mut br = BodyReader::EmptyReader;
        let mut buf = [0u8; 4];
        assert_eq!(br.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn dropping_partial_body_does_not_drain_or_release_connection() {
        let reader = stream(b"5\r\nhello\r\n0\r\n\r\n");
        reader.mark_response_pending();
        let mut br = BodyReader::ChunkReader(reader.clone(), None);
        let mut buf = [0u8; 10];
        assert_eq!(br.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf[..5], b"hello");
        drop(br);
        assert_eq!(
            reader.ensure_request_ready().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
    }
}
