//! Cooperative cancellation for pooled HTTP requests.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use may::sync::{Condvar, Mutex};

/// Cloneable, reusable cancellation signal for one or more HTTP requests.
///
/// Cancellation is sticky and idempotent. Dropping a token does not cancel anything. A token that
/// has been cancelled remains cancelled and should not be reused for new work.
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

impl std::fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    state: Mutex<Running>,
    changed: Condvar,
}

/// Coroutines currently running an operation under this token. `cancel` cancels each of them
/// directly, so a cancellable operation needs one coroutine, not a watcher per request.
#[derive(Default)]
struct Running {
    next_id: u64,
    coroutines: Vec<(u64, may::coroutine::Coroutine)>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancel every request currently using this token.
    ///
    /// Returns `true` only for the call that changes the token to cancelled.
    pub fn cancel(&self) -> bool {
        let guard = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut guard = guard;
        let changed = !self.inner.cancelled.swap(true, Ordering::AcqRel);
        if changed {
            for (_, coroutine) in guard.coroutines.drain(..) {
                // SAFETY: the coroutine runs a `run_cancellable` operation, which joins it and
                // turns the cancel unwind into a cancellation error; it is still registered, so it
                // has not been joined yet.
                unsafe { coroutine.cancel() };
            }
            self.inner.changed.notify_all();
        }
        drop(guard);
        changed
    }

    /// Register a coroutine to be cancelled with this token. `None` when the token is already
    /// cancelled: the caller cancels the coroutine itself.
    pub(crate) fn register(&self, coroutine: may::coroutine::Coroutine) -> Option<u64> {
        let mut guard = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_cancelled() {
            return None;
        }
        guard.next_id = guard.next_id.wrapping_add(1);
        let id = guard.next_id;
        guard.coroutines.push((id, coroutine));
        Some(id)
    }

    pub(crate) fn deregister(&self, id: u64) {
        let mut guard = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.coroutines.retain(|(registered, _)| *registered != id);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn check(&self) -> io::Result<()> {
        if self.is_cancelled() {
            Err(cancelled_error())
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    pub(crate) fn wait(&self) {
        let mut guard = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !self.is_cancelled() {
            guard = self
                .inner
                .changed
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

#[derive(Debug)]
struct CancellationError;

impl std::fmt::Display for CancellationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HTTP request cancelled")
    }
}

impl std::error::Error for CancellationError {}

pub(crate) fn cancelled_error() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, CancellationError)
}

/// May coroutine stack for a cancellable operation, in machine words: at least 256 KiB, more if the
/// runtime's default is larger.
///
/// A cancellable request runs on a coroutine of its own (cancelling it unwinds that coroutine, not
/// the caller). It used to be spawned by `may::select!`, which always takes may's global default
/// stack: 32 KiB unless the application raised it. A pooled HTTPS request (rustls handshake,
/// response parsing, redirects) does not fit in that on Windows debug builds, where frames are
/// larger: it overflowed the stack. The floor makes the operation independent of how the host
/// application configured may.
fn cancellable_stack_words() -> usize {
    const FLOOR_BYTES: usize = 256 * 1024;
    let floor = FLOOR_BYTES / std::mem::size_of::<usize>();
    may::config().get_stack_size().max(floor)
}

/// Run `operation` so that cancelling `token` interrupts it at its next blocking point.
///
/// The operation runs on a coroutine sized by [`cancellable_stack_words`], registered with the
/// token. Cancelling the token cancels that coroutine; its unwind runs every destructor (pool
/// leases return their capacity) and is reported as [`cancelled_error`]. A result the operation
/// produced before the cancellation landed is returned as is.
pub(crate) fn run_cancellable<T>(
    token: &CancellationToken,
    operation: impl FnOnce() -> io::Result<T> + Send,
) -> io::Result<T>
where
    T: Send + 'static,
{
    token.check()?;
    let operation: Box<dyn FnOnce() -> io::Result<T> + Send + '_> = Box::new(operation);
    // SAFETY: the coroutine is joined below on every path before this function returns, so the
    // borrows the closure holds outlive it (the same contract as `may::coroutine::scope`).
    let operation: Box<dyn FnOnce() -> io::Result<T> + Send + 'static> =
        unsafe { std::mem::transmute(operation) };
    let builder = may::coroutine::Builder::new().stack_size(cancellable_stack_words());
    // SAFETY: the closure touches no coroutine-unsafe thread-local state.
    let handle = unsafe { builder.spawn(operation) }?;
    let registration = token.register(handle.coroutine().clone());
    if registration.is_none() {
        // SAFETY: joined below.
        unsafe { handle.coroutine().cancel() };
    }
    let joined = handle.join();
    if let Some(id) = registration {
        token.deregister(id);
    }
    match joined {
        Ok(result) => result,
        Err(panic) => {
            if panic
                .downcast_ref::<generator::Error>()
                .is_some_and(|error| matches!(error, generator::Error::Cancel))
            {
                Err(cancelled_error())
            } else {
                std::panic::resume_unwind(panic)
            }
        }
    }
}

pub(crate) fn is_cancelled_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<CancellationError>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_sticky_idempotent_and_clone_shared() {
        let token = CancellationToken::new();
        let clone = token.clone();
        assert!(!token.is_cancelled());
        assert!(clone.cancel());
        assert!(!token.cancel());
        assert!(token.is_cancelled());
        assert!(is_cancelled_error(&token.check().unwrap_err()));
    }

    #[test]
    fn dropping_a_token_does_not_cancel_its_clone() {
        let token = CancellationToken::new();
        let clone = token.clone();
        drop(token);
        assert!(!clone.is_cancelled());
        clone.check().unwrap();
    }

    #[test]
    fn cancellation_wakes_a_waiting_may_coroutine() {
        let token = CancellationToken::new();
        let waiter_token = token.clone();
        let waiter = may::go!(move || waiter_token.wait());
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(token.cancel());
        waiter.join().unwrap();
    }
    /// The operation must not inherit may's global default stack (32 KiB unless the application
    /// raised it): a request needs more than that on some targets. 160 KiB of locals would overflow
    /// a default-sized coroutine and abort the test process.
    #[test]
    fn cancellable_operation_runs_on_a_large_enough_stack() {
        #[inline(never)]
        fn deep() -> usize {
            let frame = [1_u8; 160 * 1024];
            std::hint::black_box(&frame)
                .iter()
                .map(|&b| b as usize)
                .sum()
        }
        let token = CancellationToken::new();
        let sum = run_cancellable(&token, || Ok(deep())).unwrap();
        assert_eq!(sum, 160 * 1024);
    }

    #[test]
    fn cancelling_interrupts_a_blocked_operation_and_borrows_stay_valid() {
        let token = CancellationToken::new();
        let canceller = token.clone();
        let local = String::from("borrowed by the operation");
        let thread = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            canceller.cancel();
        });
        let error = run_cancellable(&token, || {
            assert_eq!(local.len(), 25);
            may::coroutine::sleep(std::time::Duration::from_secs(30));
            Ok(())
        })
        .unwrap_err();
        assert!(is_cancelled_error(&error));
        thread.join().unwrap();
        assert_eq!(local.len(), 25);
    }

    #[test]
    fn already_cancelled_token_never_runs_the_operation() {
        let token = CancellationToken::new();
        token.cancel();
        let error = run_cancellable(&token, || -> io::Result<()> {
            panic!("operation must not run")
        })
        .unwrap_err();
        assert!(is_cancelled_error(&error));
    }

    #[test]
    fn a_panic_in_the_operation_is_not_mistaken_for_cancellation() {
        let token = CancellationToken::new();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_cancellable(&token, || -> io::Result<()> { panic!("boom") })
        }));
        assert!(caught.is_err());
    }
}
