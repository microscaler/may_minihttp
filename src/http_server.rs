//! http server implementation on top of `MAY`

use std::cell::Cell;
use std::io::{self, Read, Write};
use std::mem::MaybeUninit;
use std::net::ToSocketAddrs;

use crate::request::{self, Request};
use crate::response::{self, Response};

#[cfg(unix)]
use bytes::Buf;
use bytes::{BufMut, BytesMut};
#[cfg(unix)]
use may::io::WaitIo;
use may::net::{TcpListener, TcpStream};
use may::{coroutine, go};

/// Check if an error is a normal client disconnect (not worth logging as ERROR)
#[inline]
fn is_client_disconnect(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

macro_rules! t_c {
    ($e: expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?}\nerr = {:?}", stringify!($e), err);
                continue;
            }
        }
    };
}

/// the http service trait
/// user code should supply a type that impl the `call` method for the http server
///
pub trait HttpService {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()>;
}

pub trait HttpServiceFactory: Send + Sized + 'static {
    type Service: HttpService + Send;
    // create a new http service for each connection
    fn new_service(&self, id: usize) -> Self::Service;

    /// Spawns the http service, binding to the given address
    /// return a coroutine that you can cancel it when need to stop the service
    fn start<L: ToSocketAddrs>(self, addr: L) -> io::Result<coroutine::JoinHandle<()>> {
        let listener = TcpListener::bind(addr)?;
        go!(
            coroutine::Builder::new().name("TcpServerFac".to_owned()),
            move || {
                #[cfg(unix)]
                use std::os::fd::AsRawFd;
                #[cfg(windows)]
                use std::os::windows::io::AsRawSocket;
                for stream in listener.incoming() {
                    let mut stream = t_c!(stream);
                    #[cfg(unix)]
                    let id = stream.as_raw_fd() as usize;
                    #[cfg(windows)]
                    let id = stream.as_raw_socket() as usize;
                    // Responses are written whole; Nagle would only hold a second write (a streamed
                    // chunk) for the client's delayed ACK. Not fatal if it cannot be set.
                    let _ = stream.set_nodelay(true);
                    let service = self.new_service(id);
                    let builder = may::coroutine::Builder::new().id(id);
                    go!(
                        builder,
                        move || if let Err(e) = each_connection_loop(&mut stream, service) {
                            // Only log actual errors, not normal client disconnects
                            if !is_client_disconnect(&e) {
                                error!("service err = {e:?}");
                            }
                            stream.shutdown(std::net::Shutdown::Both).ok();
                        }
                    )
                    .unwrap();
                }
            }
        )
    }
}

#[inline]
#[cold]
pub(crate) fn err<T>(e: io::Error) -> io::Result<T> {
    Err(e)
}

#[cfg(unix)]
#[inline]
fn nonblock_read(stream: &mut impl Read, req_buf: &mut BytesMut) -> io::Result<bool> {
    reserve_buf(req_buf);
    let read_buf: &mut [u8] = unsafe { std::mem::transmute(req_buf.chunk_mut()) };
    let len = read_buf.len();

    let mut read_cnt = 0;
    while read_cnt < len {
        match stream.read(unsafe { read_buf.get_unchecked_mut(read_cnt..) }) {
            Ok(0) => return err(io::Error::new(io::ErrorKind::BrokenPipe, "read closed")),
            Ok(n) => read_cnt += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return err(e),
        }
    }

    unsafe { req_buf.advance_mut(read_cnt) };
    Ok(read_cnt < len)
}

#[cfg(unix)]
#[inline]
fn nonblock_write(stream: &mut impl Write, rsp_buf: &mut BytesMut) -> io::Result<usize> {
    let write_buf = rsp_buf.chunk();
    let len = write_buf.len();
    let mut write_cnt = 0;
    while write_cnt < len {
        match stream.write(unsafe { write_buf.get_unchecked(write_cnt..) }) {
            Ok(0) => return err(io::Error::new(io::ErrorKind::BrokenPipe, "write closed")),
            Ok(n) => write_cnt += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return err(e),
        }
    }
    rsp_buf.advance(write_cnt);
    Ok(write_cnt)
}

const BUF_LEN: usize = 4096 * 8;
#[inline]
pub(crate) fn reserve_buf(buf: &mut BytesMut) {
    let rem = buf.capacity() - buf.len();
    if rem < 1024 {
        buf.reserve(BUF_LEN - rem);
    }
}

/// this is the generic type http server
/// with a type parameter that impl `HttpService` trait
///
pub struct HttpServer<T>(pub T);

/// HTTP server with configurable max headers (const generic)
///
/// Use this when you need to handle more than 16 headers.
/// Common sizes: 32 (Standard), 64 (Large), 128 (`XLarge`)
///
/// # Example
/// ```ignore
/// use may_minihttp::HttpServerWithHeaders;
/// let server = HttpServerWithHeaders::<_, 32>(my_service);
/// ```
pub struct HttpServerWithHeaders<T, const N: usize>(pub T);

#[cfg(unix)]
fn each_connection_loop<T: HttpService>(stream: &mut TcpStream, service: T) -> io::Result<()> {
    each_connection_loop_with_headers::<T, { request::MAX_HEADERS }>(stream, service)
}

#[cfg(unix)]
fn each_connection_loop_with_headers<T: HttpService, const N: usize>(
    stream: &mut TcpStream,
    mut service: T,
) -> io::Result<()> {
    let mut req_buf = BytesMut::with_capacity(BUF_LEN);
    let mut rsp_buf = BytesMut::with_capacity(BUF_LEN);
    let mut body_buf = BytesMut::with_capacity(4096);
    // set by a request whose body could not be delimited or discarded: answer it, then close
    let close = Cell::new(false);

    loop {
        let read_blocked = nonblock_read(stream.inner_mut(), &mut req_buf)?;
        let mut refused = None;

        // prepare the requests, we should make sure the request is fully read
        loop {
            let mut headers = [MaybeUninit::uninit(); N];
            let req = match request::decode_with_close(
                &mut headers,
                &mut req_buf,
                stream,
                Some(&close),
            ) {
                Ok(Some(req)) => req,
                Ok(None) => break,
                Err(e) => {
                    refused = Some(e);
                    break;
                }
            };
            reserve_buf(&mut rsp_buf);
            let mut rsp = Response::new(&mut body_buf);
            match service.call(req, &mut rsp) {
                Ok(()) => response::encode(rsp, &mut rsp_buf),
                Err(e) => {
                    eprintln!("service err = {e:?}");
                    response::encode_error(e, &mut rsp_buf);
                }
            }
            if close.get() {
                break;
            }
        }

        if let Some(e) = refused {
            return refuse(stream, &mut rsp_buf, e);
        }
        if close.get() {
            return close_after(stream, &rsp_buf);
        }

        // write out the responses
        nonblock_write(stream.inner_mut(), &mut rsp_buf)?;

        if read_blocked {
            stream.wait_io();
        }
    }
}

/// Answer a request the decoder refused (after any responses still queued for earlier pipelined
/// requests) and close the connection. Not an error worth logging: scanners and broken clients
/// send these all day.
fn refuse(stream: &mut TcpStream, rsp_buf: &mut BytesMut, error: io::Error) -> io::Result<()> {
    match request::rejected_request(&error) {
        Some(rejected) => {
            debug!("{rejected}");
            response::encode_rejection(rejected.status, rsp_buf);
            close_after(stream, rsp_buf)
        }
        None => Err(error),
    }
}

/// Write everything queued, then end the connection: no further request on it is read.
///
/// Closing a socket that still has unread input makes the kernel send RST, which can destroy the
/// response before the client reads it (a 431 for a head still being sent, a 413 for a body still
/// uploading). So after the response and a FIN, what the client is still sending is read and
/// discarded for a short, bounded time first (a "lingering close").
fn close_after(stream: &mut TcpStream, rsp_buf: &[u8]) -> io::Result<()> {
    const LINGER: std::time::Duration = std::time::Duration::from_secs(1);
    const LINGER_READ: std::time::Duration = std::time::Duration::from_millis(100);
    const LINGER_BYTES: usize = 1024 * 1024;
    stream.write_all(rsp_buf)?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let _ = stream.set_read_timeout(Some(LINGER_READ));
    let started = std::time::Instant::now();
    let mut scratch = [0_u8; 1024];
    let mut discarded = 0;
    while discarded < LINGER_BYTES && started.elapsed() < LINGER {
        match stream.read(&mut scratch) {
            Ok(0) | Err(_) => break,
            Ok(n) => discarded += n,
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn each_connection_loop<T: HttpService>(stream: &mut TcpStream, service: T) -> io::Result<()> {
    each_connection_loop_with_headers::<T, { request::MAX_HEADERS }>(stream, service)
}

#[cfg(not(unix))]
fn each_connection_loop_with_headers<T: HttpService, const N: usize>(
    stream: &mut TcpStream,
    mut service: T,
) -> io::Result<()> {
    let mut req_buf = BytesMut::with_capacity(BUF_LEN);
    let mut rsp_buf = BytesMut::with_capacity(BUF_LEN);
    let mut body_buf = BytesMut::with_capacity(BUF_LEN);
    let close = Cell::new(false);
    let mut refused = None;
    loop {
        // read the socket for requests
        reserve_buf(&mut req_buf);
        let read_buf: &mut [u8] = unsafe { std::mem::transmute(&mut *req_buf.chunk_mut()) };
        let read_cnt = stream.read(read_buf)?;
        if read_cnt == 0 {
            //connection was closed
            return err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"));
        }
        unsafe { req_buf.advance_mut(read_cnt) };

        // prepare the requests
        if read_cnt > 0 {
            loop {
                let mut headers = [MaybeUninit::uninit(); N];
                let req = match request::decode_with_close(
                    &mut headers,
                    &mut req_buf,
                    stream,
                    Some(&close),
                ) {
                    Ok(Some(req)) => req,
                    Ok(None) => break,
                    Err(e) => {
                        refused = Some(e);
                        break;
                    }
                };
                let mut rsp = Response::new(&mut body_buf);
                match service.call(req, &mut rsp) {
                    Ok(()) => response::encode(rsp, &mut rsp_buf),
                    Err(e) => {
                        eprintln!("service err = {:?}", e);
                        response::encode_error(e, &mut rsp_buf);
                    }
                }
                if close.get() {
                    break;
                }
            }
        }
        if let Some(e) = refused.take() {
            return refuse(stream, &mut rsp_buf, e);
        }
        if close.get() {
            return close_after(stream, &rsp_buf);
        }

        // send the result back to client
        stream.write_all(&rsp_buf)?;
        rsp_buf.clear();
    }
}

impl<T: HttpService + Clone + Send + Sync + 'static> HttpServer<T> {
    /// Spawns the http service, binding to the given address
    /// return a coroutine that you can cancel it when need to stop the service
    pub fn start<L: ToSocketAddrs>(self, addr: L) -> io::Result<coroutine::JoinHandle<()>> {
        let listener = TcpListener::bind(addr)?;
        let service = self.0;
        go!(
            coroutine::Builder::new().name("TcpServer".to_owned()),
            move || {
                for stream in listener.incoming() {
                    let mut stream = t_c!(stream);
                    // Responses are written whole; Nagle would only hold a second write (a streamed
                    // chunk) for the client's delayed ACK. Not fatal if it cannot be set.
                    let _ = stream.set_nodelay(true);
                    let service = service.clone();
                    go!(
                        move || if let Err(e) = each_connection_loop(&mut stream, service) {
                            // Only log actual errors, not normal client disconnects
                            if !is_client_disconnect(&e) {
                                error!("service err = {e:?}");
                            }
                            stream.shutdown(std::net::Shutdown::Both).ok();
                        }
                    );
                }
            }
        )
    }
}

impl<T: HttpService + Clone + Send + Sync + 'static, const N: usize> HttpServerWithHeaders<T, N> {
    /// Spawns the http service with custom max headers, binding to the given address
    /// return a coroutine that you can cancel it when need to stop the service
    pub fn start<L: ToSocketAddrs>(self, addr: L) -> io::Result<coroutine::JoinHandle<()>> {
        let listener = TcpListener::bind(addr)?;
        let service = self.0;
        go!(
            coroutine::Builder::new().name("TcpServer".to_owned()),
            move || {
                for stream in listener.incoming() {
                    let mut stream = t_c!(stream);
                    // Responses are written whole; Nagle would only hold a second write (a streamed
                    // chunk) for the client's delayed ACK. Not fatal if it cannot be set.
                    let _ = stream.set_nodelay(true);
                    let service = service.clone();
                    go!(move || if let Err(e) =
                        each_connection_loop_with_headers::<T, N>(&mut stream, service)
                    {
                        // Only log actual errors, not normal client disconnects
                        if !is_client_disconnect(&e) {
                            error!("service err = {e:?}");
                        }
                        stream.shutdown(std::net::Shutdown::Both).ok();
                    });
                }
            }
        )
    }
}
