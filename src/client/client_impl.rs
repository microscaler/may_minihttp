use std::cell::RefCell;
use std::io;
use std::net::ToSocketAddrs;
use std::rc::Rc;
use std::time::Duration;

use bytes::Buf;
use http::{Method, Uri};
use may::net::TcpStream;

use crate::client::buffer::BufferIo;
use crate::client::{Request, Response};

/// Coroutine HTTP/1.1 client — API-compatible with `may_http::client::HttpClient`.
#[derive(Debug)]
pub struct HttpClient {
    conn: Rc<RefCell<BufferIo<TcpStream>>>,
}

/// On Windows, `may::net::TcpStream::connect` returns
/// WSAECONNREFUSED (10061) for refused connections. Remap it so the
/// client API reports `ErrorKind::ConnectionRefused` consistently.
#[cfg(windows)]
fn connect_remap(e: io::Error) -> io::Error {
    if e.raw_os_error() == Some(10061) {
        io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused")
    } else {
        e
    }
}

impl HttpClient {
    /// Connect to the given address.
    pub fn connect<A: ToSocketAddrs>(remote: A) -> io::Result<Self> {
        #[cfg(windows)]
        let stream = TcpStream::connect(remote).map_err(connect_remap)?;
        #[cfg(not(windows))]
        let stream = TcpStream::connect(remote)?;
        let stream = BufferIo::new(stream);
        Ok(HttpClient {
            conn: Rc::new(RefCell::new(stream)),
        })
    }

    /// Set read/write timeout on the underlying connection.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        {
            let mut s = self.conn.borrow_mut();
            let s = s.inner_mut();
            s.set_read_timeout(timeout).unwrap();
            s.set_write_timeout(timeout).unwrap();
        }
        self
    }

    /// GET shortcut — sends request on drop and reads the response.
    pub fn get(&mut self, uri: Uri) -> io::Result<Response> {
        let mut req = Request::new(self.conn.clone());
        *req.uri_mut() = uri;
        drop(req);
        self.get_rsp()
    }

    /// POST shortcut with body bytes.
    pub fn post<T: Buf>(&mut self, uri: Uri, mut data: T) -> io::Result<Response> {
        let mut req = Request::new(self.conn.clone());
        *req.method_mut() = Method::POST;
        *req.uri_mut() = uri;
        let body = data.copy_to_bytes(data.remaining());
        req.send(&body)?;
        drop(req);
        self.get_rsp()
    }

    /// Build a request with the given method and URI.
    #[inline]
    pub fn new_request(&self, method: Method, uri: Uri) -> Request {
        let mut req = Request::new(self.conn.clone());
        *req.method_mut() = method;
        *req.uri_mut() = uri;
        req
    }

    /// Send a request built from this client and read the response.
    #[inline]
    pub fn send_request(&mut self, req: Request) -> io::Result<Response> {
        use std::io::Write;
        let conn: Rc<RefCell<dyn Write>> = self.conn.clone();
        assert_eq!(Rc::ptr_eq(&conn, req.conn()), true);
        drop(req);
        self.get_rsp()
    }

    #[inline]
    fn get_rsp(&mut self) -> io::Result<Response> {
        let mut stream = self.conn.borrow_mut();
        loop {
            match super::response::decode(stream.get_reader_buf())? {
                None => {
                    if stream.bump_read()? == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection breaked",
                        ));
                    }
                }
                Some(mut rsp) => {
                    rsp.set_reader(self.conn.clone());
                    return Ok(rsp);
                }
            }
        }
    }
}
