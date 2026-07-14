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
    state: Mutex<()>,
    changed: Condvar,
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
        let changed = !self.inner.cancelled.swap(true, Ordering::AcqRel);
        if changed {
            self.inner.changed.notify_all();
        }
        drop(guard);
        changed
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
}
