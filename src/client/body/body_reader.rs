use std::cell::RefCell;
use std::fmt;
use std::io::{self, Read};
use std::rc::Rc;

use super::BodyReader::*;

pub enum BodyReader {
    SizedReader(Rc<RefCell<dyn Read>>, usize),
    ChunkReader(Rc<RefCell<dyn Read>>, Option<usize>),
    EmptyReader,
}

impl fmt::Debug for BodyReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let name = match *self {
            SizedReader(..) => "SizedReader",
            ChunkReader(..) => "ChunkReader",
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
                    return Ok(0);
                }
                let mut r = r.borrow_mut();
                let n = r.read(&mut buf[0..len])?;
                *remain -= n;
                Ok(n)
            }
            ChunkReader(ref r, ref mut opt_remaining) => {
                let mut r = r.borrow_mut();
                let mut rem = match *opt_remaining {
                    Some(ref rem) => *rem,
                    // None means we don't know the size of the next chunk
                    None => read_chunk_size(&mut *r)?,
                };
                trace!("Chunked read, remaining={:?}", rem);

                if rem == 0 {
                    if opt_remaining.is_none() {
                        eat(&mut *r, b"\r\n")?;
                    }

                    *opt_remaining = Some(0);

                    trace!("end of chunked");

                    return Ok(0);
                }

                let to_read = cmp::min(rem, buf.len());
                let count = r.read(&mut buf[..to_read])?;

                if count == 0 {
                    *opt_remaining = Some(0);
                    return Err(io::Error::new(io::ErrorKind::Other, "early eof"));
                }

                rem -= count;
                *opt_remaining = if rem > 0 {
                    Some(rem)
                } else {
                    eat(&mut *r, b"\r\n")?;
                    None
                };
                Ok(count)
            }
            EmptyReader => Ok(0),
        }
    }
}

impl Drop for BodyReader {
    fn drop(&mut self) {
        // consume all the chunks
        let mut buf = vec![0; 4096];
        loop {
            match self.read(&mut buf) {
                Err(e) => {
                    error!("drop Reader err={}", e);
                    break;
                }
                Ok(n) => {
                    if n == 0 {
                        break;
                    }
                }
            }
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
    loop {
        match byte!(rdr) {
            b @ b'0'..=b'9' if in_chunk_size => {
                size <<= 4;
                size += (b - b'0') as usize;
            }
            b @ b'a'..=b'f' if in_chunk_size => {
                size <<= 4;
                size += (b + 10 - b'a') as usize;
            }
            b @ b'A'..=b'F' if in_chunk_size => {
                size <<= 4;
                size += (b + 10 - b'A') as usize;
            }
            b'\r' => match byte!(rdr) {
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
            b'\t' | b' ' if !in_ext & !in_chunk_size => {}
            b'\t' | b' ' if in_chunk_size => in_chunk_size = false,
            ext if in_ext => {
                error!("chunk extension byte={}", ext);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid chunk size line, unkonw byte",
                ));
            }
        }
    }
    trace!("chunk size={:?}", size);
    Ok(size)
}
