//! Coroutine-aware shared client transport.

use std::io::{self, Read, Write};
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
            // Coroutine sockets can report EOPNOTSUPP for standard socket options. may still
            // enforces its I/O timeout, so preserve the existing best-effort behavior.
            let _ = socket.set_read_timeout(timeout);
            let _ = socket.set_write_timeout(timeout);
        }
    }

    fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
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
}

impl SharedStream {
    pub fn new(transport: Transport) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BufferIo::new(transport))),
        }
    }

    #[cfg(test)]
    pub fn test<T: Read + Write + Send + 'static>(transport: T) -> Self {
        Self::new(Transport::Test(Box::new(transport)))
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn is_tls(&self) -> io::Result<bool> {
        self.with_buffer(|buffer| Ok(buffer.inner_mut().is_tls()))
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
