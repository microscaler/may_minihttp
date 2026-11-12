//! Coroutine-aware shared client transport.

use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use may::net::TcpStream;
use may::sync::Mutex;
use rustls::{ClientConnection, StreamOwned};

use super::buffer::BufferIo;

/// Plain or TLS transport owned by one HTTP/1.1 connection.
pub enum Transport {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
    #[cfg(test)]
    Test(Box<dyn ReadWriteSend>),
}

#[cfg(test)]
pub trait ReadWriteSend: Read + Write + Send {}
#[cfg(test)]
impl<T: Read + Write + Send> ReadWriteSend for T {}

impl Transport {
    fn set_timeout(&mut self, timeout: Option<Duration>) {
        let socket = match self {
            Self::Plain(socket) => Some(socket),
            Self::Tls(stream) => Some(&mut stream.sock),
            #[cfg(test)]
            Self::Test(_) => None,
        };
        if let Some(socket) = socket {
            // may's setters also call setsockopt(SO_RCVTIMEO / SO_SNDTIMEO): two syscalls, which
            // the pooled client paid before every request and every body read (each re-deriving
            // the timeout from the request deadline). The current value is read back from may
            // without a syscall, and a change within `TIMEOUT_TOLERANCE` is not applied.
            let current = socket.read_timeout().ok().flatten();
            if timeout_close_enough(current, timeout) {
                return;
            }
            // Coroutine sockets can report EOPNOTSUPP for standard socket options. may still
            // enforces its I/O timeout, so preserve the existing best-effort behavior.
            let _ = socket.set_read_timeout(timeout);
            let _ = socket.set_write_timeout(timeout);
        }
    }

    fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Whether an idle connection can carry another request, judged without blocking: `false` when
    /// the peer has closed it (EOF), it errored, or (plain HTTP) it holds bytes nobody asked for,
    /// such as a late `408`. A TLS connection with pending bytes is kept: they may be a session
    /// ticket rustls has not read yet. Only implemented on unix, where may sockets are
    /// non-blocking; elsewhere every connection passes and the stale-retry path covers it.
    fn idle_usable(&self) -> bool {
        #[cfg(unix)]
        {
            let (socket, tls) = match self {
                Self::Plain(socket) => (socket, false),
                Self::Tls(stream) => (&stream.sock, true),
                #[cfg(test)]
                Self::Test(_) => return true,
            };
            let mut probe = [0_u8; 1];
            match socket.inner().peek(&mut probe) {
                Ok(0) => false,
                Ok(_) => tls,
                Err(error) => error.kind() == io::ErrorKind::WouldBlock,
            }
        }
        #[cfg(not(unix))]
        {
            true
        }
    }
}

/// Largest timeout change that is not applied: 5 ms, or a tenth of the new timeout if that is
/// smaller. A deadline can be overshot (or a read cut short) by at most this much.
const TIMEOUT_TOLERANCE: Duration = Duration::from_millis(5);

fn timeout_close_enough(current: Option<Duration>, new: Option<Duration>) -> bool {
    match (current, new) {
        (None, None) => true,
        (Some(current), Some(new)) => current.abs_diff(new) <= TIMEOUT_TOLERANCE.min(new / 10),
        _ => false,
    }
}

impl Read for Transport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
            #[cfg(test)]
            Self::Test(stream) => stream.read(buffer),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
            #[cfg(test)]
            Self::Test(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
            #[cfg(test)]
            Self::Test(stream) => stream.flush(),
        }
    }
}

/// Cloneable handle to a connection's buffered transport.
///
/// `may::sync::Mutex` parks a waiting coroutine rather than an OS worker. The public `HttpClient`
/// remains non-Clone, so normal use still serializes one request/response exchange per HTTP/1.1
/// connection; cloning is restricted to its request and response body plumbing.
#[derive(Clone)]
pub struct SharedStream {
    inner: Arc<Mutex<BufferIo<Transport>>>,
    request_ready: Arc<AtomicBool>,
}

impl SharedStream {
    pub fn new(transport: Transport) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BufferIo::new(transport))),
            request_ready: Arc::new(AtomicBool::new(true)),
        }
    }

    #[cfg(test)]
    pub fn test<T: Read + Write + Send + 'static>(transport: T) -> Self {
        Self::new(Transport::Test(Box::new(transport)))
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn ensure_request_ready(&self) -> io::Result<()> {
        if self.request_ready.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "previous HTTP response body was not fully consumed",
            ))
        }
    }

    pub fn mark_response_pending(&self) {
        self.request_ready.store(false, Ordering::Release);
    }

    pub fn mark_response_complete(&self) {
        self.request_ready.store(true, Ordering::Release);
    }

    pub fn is_tls(&self) -> io::Result<bool> {
        self.with_buffer(|buffer| Ok(buffer.inner_mut().is_tls()))
    }

    /// See [`Transport::idle_usable`]; a connection with buffered, unread response bytes is not
    /// usable either.
    pub fn idle_usable(&self) -> bool {
        self.with_buffer(|buffer| {
            Ok(buffer.get_reader_buf().is_empty() && buffer.inner_mut().idle_usable())
        })
        .unwrap_or(false)
    }

    pub fn set_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.with_buffer(|buffer| {
            buffer.inner_mut().set_timeout(timeout);
            Ok(())
        })
    }

    pub fn with_buffer<T>(
        &self,
        operation: impl FnOnce(&mut BufferIo<Transport>) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut buffer = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("HTTP connection lock poisoned"))?;
        operation(&mut buffer)
    }
}

impl Read for SharedStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.with_buffer(|stream| stream.read(buffer))
    }
}

impl Write for SharedStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.with_buffer(|stream| stream.write(buffer))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.with_buffer(Write::flush)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_changes_within_tolerance_are_skipped() {
        let s = Duration::from_secs;
        let ms = Duration::from_millis;
        assert!(timeout_close_enough(None, None));
        assert!(!timeout_close_enough(None, Some(s(1))));
        assert!(!timeout_close_enough(Some(s(1)), None));
        assert!(timeout_close_enough(Some(s(30)), Some(s(30) - ms(3))));
        assert!(timeout_close_enough(Some(s(30) - ms(3)), Some(s(30))));
        assert!(!timeout_close_enough(Some(s(30)), Some(s(30) - ms(6))));
        // short timeouts: a tenth of the value
        assert!(timeout_close_enough(Some(ms(20)), Some(ms(19))));
        assert!(!timeout_close_enough(Some(ms(20)), Some(ms(17))));
    }
}
