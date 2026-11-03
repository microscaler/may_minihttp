//! Server-side phase timings in the shared `op_duration_seconds{component="may_minihttp.server", op,
//! outcome}` histogram (`microscaler-timing`, feature `op-timing`), and the levels beside them in
//! `op_gauge`. A host that renders that family (BRRTRouter does) shows them with no wiring.
//!
//! | op | from | to |
//! |---|---|---|
//! | `accept_to_first_byte` | the listener accepted the connection | its first request byte was read (once per connection: includes the client's send and this coroutine being scheduled) |
//! | `read_parse` | a request's first byte was read | the request was fully read and parsed |
//! | `service` | the request was handed to the service | the service returned (`err` if it failed) |
//! | `response_write` | the service returned | the response was fully written to the socket |
//! | `request_total` | a request's first byte was read | its response was fully written |
//!
//! Gauges: `open_connections`; on Linux, `accept_backlog` (connections the kernel has completed
//! but the server has not accepted yet, read from `TCP_INFO` on the listening socket at scrape
//! time) and `accept_backlog_limit` (the listen backlog).
//!
//! Without the feature every call here is an empty inline function. With it, recording is a few
//! atomic adds and an `Instant::now()`: nothing blocks the coroutine.

// The connection loop that uses ConnTiming is unix-only; elsewhere only listening() is called.
#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

#[cfg(feature = "op-timing")]
mod imp {
    use std::time::Instant;

    use microscaler_timing::{Gauge, Outcome, Site};

    static ACCEPT_TO_FIRST_BYTE: Site = Site::new("may_minihttp.server", "accept_to_first_byte");
    static READ_PARSE: Site = Site::new("may_minihttp.server", "read_parse");
    static SERVICE: Site = Site::new("may_minihttp.server", "service");
    static RESPONSE_WRITE: Site = Site::new("may_minihttp.server", "response_write");
    static REQUEST_TOTAL: Site = Site::new("may_minihttp.server", "request_total");
    static OPEN: Gauge = Gauge::new("may_minihttp.server", "open_connections");

    /// One connection's timing state; lives in the connection coroutine.
    pub(crate) struct ConnTiming {
        accepted: Instant,
        first_seen: bool,
        /// When the first byte of the request being read arrived.
        first_byte: Option<Instant>,
        /// The first byte of the request being served.
        serving: Option<Instant>,
        call_started: Option<Instant>,
        /// (first byte, service returned) of responses not yet fully written.
        pending: Vec<(Instant, Instant)>,
    }

    impl ConnTiming {
        pub(crate) fn new(accepted: Instant) -> Self {
            OPEN.add(1);
            Self {
                accepted,
                first_seen: false,
                first_byte: None,
                serving: None,
                call_started: None,
                pending: Vec::new(),
            }
        }

        /// After a read that took the buffer from `before` to `after` bytes.
        #[inline]
        pub(crate) fn read(&mut self, before: usize, after: usize) {
            if after > before && self.first_byte.is_none() {
                let now = Instant::now();
                self.first_byte = Some(now);
                if !self.first_seen {
                    self.first_seen = true;
                    ACCEPT_TO_FIRST_BYTE.record(Outcome::Ok, now - self.accepted);
                }
            }
        }

        /// A request was parsed (a pipelined request already buffered behind one counts from here).
        #[inline]
        pub(crate) fn parsed(&mut self) {
            let now = Instant::now();
            let first = self.first_byte.take().unwrap_or(now);
            READ_PARSE.record(Outcome::Ok, now - first);
            self.serving = Some(first);
            self.call_started = Some(now);
        }

        /// No complete request is buffered; `buffered` bytes of the next one are.
        #[inline]
        pub(crate) fn incomplete(&mut self, buffered: usize) {
            if buffered > 0 && self.first_byte.is_none() {
                self.first_byte = Some(Instant::now());
            }
        }

        /// The service returned.
        #[inline]
        pub(crate) fn served(&mut self, ok: bool) {
            let now = Instant::now();
            if let Some(started) = self.call_started.take() {
                SERVICE.record(if ok { Outcome::Ok } else { Outcome::Err }, now - started);
            }
            if let Some(first) = self.serving.take() {
                self.pending.push((first, now));
            }
        }

        /// After a write; `drained` when nothing is left to write.
        #[inline]
        pub(crate) fn written(&mut self, drained: bool) {
            if !drained || self.pending.is_empty() {
                return;
            }
            let now = Instant::now();
            for (first, served) in self.pending.drain(..) {
                RESPONSE_WRITE.record(Outcome::Ok, now - served);
                REQUEST_TOTAL.record(Outcome::Ok, now - first);
            }
        }
    }

    impl Drop for ConnTiming {
        fn drop(&mut self) {
            OPEN.sub(1);
        }
    }

    #[cfg(target_os = "linux")]
    mod backlog {
        use std::sync::atomic::{AtomicI32, Ordering};

        use microscaler_timing::GaugeFn;

        static LISTENER_FD: AtomicI32 = AtomicI32::new(-1);
        static BACKLOG: GaugeFn = GaugeFn::new("may_minihttp.server", "accept_backlog", backlog);
        static LIMIT: GaugeFn = GaugeFn::new("may_minihttp.server", "accept_backlog_limit", limit);

        /// `TCP_INFO` of the listening socket: for a listener, `tcpi_unacked` is the accept
        /// queue's current length and `tcpi_sacked` its limit.
        fn info() -> Option<libc::tcp_info> {
            let fd = LISTENER_FD.load(Ordering::Relaxed);
            if fd < 0 {
                return None;
            }
            // SAFETY: `tcp_info` is plain old data; getsockopt writes at most `len` bytes into it.
            let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
            // SAFETY: a valid out pointer and its length; a closed fd only makes the call fail.
            let rc = unsafe {
                libc::getsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_INFO,
                    (&mut info as *mut libc::tcp_info).cast(),
                    &mut len,
                )
            };
            (rc == 0).then_some(info)
        }

        fn backlog() -> Option<i64> {
            info().map(|i| i64::from(i.tcpi_unacked))
        }

        fn limit() -> Option<i64> {
            info().map(|i| i64::from(i.tcpi_sacked))
        }

        pub(crate) fn listening(fd: i32) {
            LISTENER_FD.store(fd, Ordering::Relaxed);
            BACKLOG.register();
            LIMIT.register();
        }
    }

    /// The server is listening on `listener`: report its accept queue.
    pub(crate) fn listening(listener: &may::net::TcpListener) {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            backlog::listening(listener.as_raw_fd());
        }
        #[cfg(not(target_os = "linux"))]
        let _ = listener;
    }
}

#[cfg(not(feature = "op-timing"))]
mod imp {
    use std::time::Instant;

    pub(crate) struct ConnTiming;

    impl ConnTiming {
        #[inline(always)]
        pub(crate) fn new(_accepted: Instant) -> Self {
            Self
        }
        #[inline(always)]
        pub(crate) fn read(&mut self, _before: usize, _after: usize) {}
        #[inline(always)]
        pub(crate) fn parsed(&mut self) {}
        #[inline(always)]
        pub(crate) fn incomplete(&mut self, _buffered: usize) {}
        #[inline(always)]
        pub(crate) fn served(&mut self, _ok: bool) {}
        #[inline(always)]
        pub(crate) fn written(&mut self, _drained: bool) {}
    }

    #[inline(always)]
    pub(crate) fn listening(_listener: &may::net::TcpListener) {}
}

pub(crate) use imp::{listening, ConnTiming};
