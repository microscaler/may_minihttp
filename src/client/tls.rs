//! Atomic rustls configuration snapshots for long-running service clients.

use std::fmt;
use std::io;
use std::sync::Arc;

use rustls::ClientConfig;

/// An immutable rustls configuration and its monotonically increasing generation.
///
/// Generation zero is reserved for non-TLS pool keys. A provider must retain a generation while
/// its effective identity and trust material are unchanged, and increase it whenever they change.
#[derive(Clone)]
pub struct TlsConfigSnapshot {
    pub(crate) generation: u64,
    pub(crate) config: Arc<ClientConfig>,
}

impl TlsConfigSnapshot {
    pub fn new(generation: u64, config: Arc<ClientConfig>) -> Self {
        Self { generation, config }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

impl fmt::Debug for TlsConfigSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsConfigSnapshot")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// Request behavior when a configured TLS provider cannot load a replacement snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TlsConfigFailurePolicy {
    /// Fail the new request before DNS, connect, or request bytes. This is the default.
    #[default]
    FailRequest,
    /// Continue with the last snapshot successfully accepted by the client.
    UseLastKnownGood,
}

/// Supplies an atomic rustls identity and trust snapshot for a new logical HTTPS request.
///
/// Secret-store access, certificate issuance, and parsing private keys remain application
/// responsibilities. The callback runs synchronously without the connection-pool lock held and
/// should apply its own blocking, latency, and panic policy. The client calls it once during
/// construction and once for each logical request that first encounters an HTTPS origin.
pub trait TlsConfigProvider: Send + Sync {
    fn current(&self) -> io::Result<TlsConfigSnapshot>;
}

impl<F> TlsConfigProvider for F
where
    F: Fn() -> io::Result<TlsConfigSnapshot> + Send + Sync,
{
    fn current(&self) -> io::Result<TlsConfigSnapshot> {
        self()
    }
}
