//! Bounded resolver adapters for strict-may service communication.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use may::sync::{Condvar, Mutex};

/// Source of one address resolution result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionSource {
    /// Direct lookup by the configured resolver.
    Resolver,
    /// Positive or negative result served by [`CachingResolver`].
    Cache,
    /// Address supplied by a push-updated [`ServiceResolver`].
    ServiceRegistry,
}

/// Resolved addresses plus non-sensitive operational metadata.
#[derive(Debug, Clone)]
pub struct Resolution {
    pub addresses: Vec<SocketAddr>,
    pub source: ResolutionSource,
}

/// Hostname or service resolver used before coroutine-aware TCP connection attempts.
pub trait Resolver: Send + Sync {
    fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>>;

    fn resolve_with_metadata(&self, host: &str, port: u16) -> io::Result<Resolution> {
        self.resolve(host, port).map(|addresses| Resolution {
            addresses,
            source: ResolutionSource::Resolver,
        })
    }

    /// Resolve within the caller's connect deadline when the implementation can enforce it.
    ///
    /// The default preserves compatibility with existing resolvers. Implementations with their own
    /// wait queues should override this method so queueing cannot outlive the request budget.
    fn resolve_with_deadline(
        &self,
        host: &str,
        port: u16,
        _deadline: Instant,
    ) -> io::Result<Resolution> {
        self.resolve_with_metadata(host, port)
    }
}

/// Operating-system resolver used by default.
///
/// OS resolution may block on a cache miss. Strict deployments should inject a may-aware resolver,
/// a [`ServiceResolver`], or a [`CachingResolver`] around an application-owned resolver.
#[derive(Debug, Default)]
pub struct SystemResolver;

impl Resolver for SystemResolver {
    fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        (host, port).to_socket_addrs().map(Iterator::collect)
    }
}

/// Bounds and TTLs for [`CachingResolver`].
#[derive(Debug, Clone, Copy)]
pub struct ResolverCacheConfig {
    pub positive_ttl: Duration,
    pub negative_ttl: Duration,
    pub max_entries: usize,
    pub max_addresses_per_entry: usize,
}

impl Default for ResolverCacheConfig {
    fn default() -> Self {
        Self {
            positive_ttl: Duration::from_secs(30),
            negative_ttl: Duration::from_secs(5),
            max_entries: 1_024,
            max_addresses_per_entry: 32,
        }
    }
}

/// Bounded, single-flight resolver cache.
///
/// Waiting coroutines use a may condition variable. The cache lock is never held while the wrapped
/// resolver performs a lookup. Whether a cache miss is scheduler-safe depends on the wrapped
/// resolver; wrapping [`SystemResolver`] reduces but does not eliminate its blocking boundary.
pub struct CachingResolver {
    inner: Arc<dyn Resolver>,
    config: ResolverCacheConfig,
    state: Mutex<CacheState>,
    available: Condvar,
}

impl std::fmt::Debug for CachingResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CachingResolver")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl CachingResolver {
    pub fn new(inner: Arc<dyn Resolver>, config: ResolverCacheConfig) -> io::Result<Self> {
        validate_cache_config(config)?;
        Ok(Self {
            inner,
            config,
            state: Mutex::new(CacheState::default()),
            available: Condvar::new(),
        })
    }

    /// Remove one cached service entry.
    pub fn invalidate(&self, host: &str, port: u16) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .remove(&CacheKey::new(host, port))
            .is_some()
    }

    /// Remove all positive and negative cached entries.
    pub fn clear(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .clear();
    }

    fn resolve_internal(
        &self,
        host: &str,
        port: u16,
        injected_now: Option<Instant>,
        deadline: Option<Instant>,
    ) -> io::Result<Resolution> {
        let key = CacheKey::new(host, port);
        loop {
            let now = injected_now.unwrap_or_else(Instant::now);
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.entries.retain(|_, entry| entry.expires_at > now);
            if let Some(entry) = state.entries.get_mut(&key) {
                return entry.result();
            }
            if state.in_flight.contains(&key) {
                let state = if let Some(deadline) = deadline {
                    let wait =
                        deadline
                            .checked_duration_since(Instant::now())
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::TimedOut,
                                    "resolver cache wait exhausted the connect deadline",
                                )
                            })?;
                    let (state, timeout) = self
                        .available
                        .wait_timeout(state, wait)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if timeout.timed_out() {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "resolver cache wait exhausted the connect deadline",
                        ));
                    }
                    state
                } else {
                    self.available
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                };
                drop(state);
                continue;
            }
            if state.entries.len() + state.in_flight.len() >= self.config.max_entries {
                state.evict_earliest();
                if state.entries.len() + state.in_flight.len() >= self.config.max_entries {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "resolver cache has reached its in-flight entry limit",
                    ));
                }
            }
            state.in_flight.insert(key.clone());
            drop(state);

            let resolved = self.inner.resolve(host, port).and_then(|addresses| {
                bounded_addresses(addresses, port, self.config.max_addresses_per_entry)
            });

            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.in_flight.remove(&key);
            if state.entries.len() >= self.config.max_entries {
                state.evict_earliest();
            }
            let cached_at = injected_now.unwrap_or_else(Instant::now);
            if let Some(entry) = CacheEntry::new(&resolved, cached_at, self.config) {
                state.entries.insert(key, entry);
            }
            drop(state);
            self.available.notify_all();

            return resolved.map(|addresses| Resolution {
                addresses,
                source: ResolutionSource::Resolver,
            });
        }
    }

    #[cfg(test)]
    fn resolve_at(&self, host: &str, port: u16, now: Instant) -> io::Result<Resolution> {
        self.resolve_internal(host, port, Some(now), None)
    }
}

impl Resolver for CachingResolver {
    fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        self.resolve_with_metadata(host, port)
            .map(|resolution| resolution.addresses)
    }

    fn resolve_with_metadata(&self, host: &str, port: u16) -> io::Result<Resolution> {
        self.resolve_internal(host, port, None, None)
    }

    fn resolve_with_deadline(
        &self,
        host: &str,
        port: u16,
        deadline: Instant,
    ) -> io::Result<Resolution> {
        self.resolve_internal(host, port, None, Some(deadline))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    host: String,
    port: u16,
}

impl CacheKey {
    fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_ascii_lowercase(),
            port,
        }
    }
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    in_flight: HashSet<CacheKey>,
}

impl CacheState {
    fn evict_earliest(&mut self) {
        if let Some(key) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.expires_at)
            .map(|(key, _)| key.clone())
        {
            self.entries.remove(&key);
        }
    }
}

struct CacheEntry {
    value: CachedValue,
    expires_at: Instant,
    next_address: usize,
}

enum CachedValue {
    Addresses(Vec<SocketAddr>),
    Error(io::ErrorKind, String),
}

impl CacheEntry {
    fn new(
        resolved: &io::Result<Vec<SocketAddr>>,
        now: Instant,
        config: ResolverCacheConfig,
    ) -> Option<Self> {
        match resolved {
            Ok(addresses) => Some(Self {
                value: CachedValue::Addresses(addresses.clone()),
                expires_at: now.checked_add(config.positive_ttl)?,
                next_address: usize::from(addresses.len() > 1),
            }),
            Err(error) => Some(Self {
                value: CachedValue::Error(error.kind(), error.to_string()),
                expires_at: now.checked_add(config.negative_ttl)?,
                next_address: 0,
            }),
        }
    }

    fn result(&mut self) -> io::Result<Resolution> {
        match &self.value {
            CachedValue::Addresses(addresses) => {
                let mut rotated = addresses.clone();
                if !rotated.is_empty() {
                    rotated.rotate_left(self.next_address % addresses.len());
                    self.next_address = (self.next_address + 1) % addresses.len();
                }
                Ok(Resolution {
                    addresses: rotated,
                    source: ResolutionSource::Cache,
                })
            }
            CachedValue::Error(kind, message) => Err(io::Error::new(*kind, message.clone())),
        }
    }
}

/// Bounds for a push-updated [`ServiceResolver`].
#[derive(Debug, Clone, Copy)]
pub struct ServiceResolverConfig {
    pub max_services: usize,
    pub max_addresses_per_service: usize,
}

impl Default for ServiceResolverConfig {
    fn default() -> Self {
        Self {
            max_services: 1_024,
            max_addresses_per_service: 32,
        }
    }
}

/// Push-updated service registry resolver with no request-path DNS I/O.
#[derive(Clone)]
pub struct ServiceResolver {
    config: ServiceResolverConfig,
    entries: Arc<Mutex<HashMap<CacheKey, ServiceEntry>>>,
}

impl std::fmt::Debug for ServiceResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceResolver")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ServiceResolver {
    pub fn new(config: ServiceResolverConfig) -> io::Result<Self> {
        if config.max_services == 0 || config.max_addresses_per_service == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "service resolver limits must be greater than zero",
            ));
        }
        Ok(Self {
            config,
            entries: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Insert or atomically replace addresses for one logical service origin.
    pub fn update(&self, host: &str, port: u16, addresses: Vec<SocketAddr>) -> io::Result<()> {
        let addresses = bounded_addresses(addresses, port, self.config.max_addresses_per_service)?;
        let key = CacheKey::new(host, port);
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !entries.contains_key(&key) && entries.len() >= self.config.max_services {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "service resolver entry limit reached",
            ));
        }
        entries.insert(
            key,
            ServiceEntry {
                addresses,
                next_address: 0,
            },
        );
        Ok(())
    }

    pub fn remove(&self, host: &str, port: u16) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&CacheKey::new(host, port))
            .is_some()
    }
}

impl Default for ServiceResolver {
    fn default() -> Self {
        Self {
            config: ServiceResolverConfig::default(),
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Resolver for ServiceResolver {
    fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        self.resolve_with_metadata(host, port)
            .map(|resolution| resolution.addresses)
    }

    fn resolve_with_metadata(&self, host: &str, port: u16) -> io::Result<Resolution> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = entries.get_mut(&CacheKey::new(host, port)).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "service resolver has no addresses for origin",
            )
        })?;
        let mut addresses = entry.addresses.clone();
        let address_count = addresses.len();
        addresses.rotate_left(entry.next_address % address_count);
        entry.next_address = (entry.next_address + 1) % address_count;
        Ok(Resolution {
            addresses,
            source: ResolutionSource::ServiceRegistry,
        })
    }
}

struct ServiceEntry {
    addresses: Vec<SocketAddr>,
    next_address: usize,
}

fn validate_cache_config(config: ResolverCacheConfig) -> io::Result<()> {
    if config.positive_ttl.is_zero()
        || config.negative_ttl.is_zero()
        || config.max_entries == 0
        || config.max_addresses_per_entry == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "resolver cache TTLs and limits must be greater than zero",
        ));
    }
    let now = Instant::now();
    if now.checked_add(config.positive_ttl).is_none()
        || now.checked_add(config.negative_ttl).is_none()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "resolver cache TTL exceeds the platform instant range",
        ));
    }
    Ok(())
}

fn bounded_addresses(
    addresses: Vec<SocketAddr>,
    expected_port: u16,
    limit: usize,
) -> io::Result<Vec<SocketAddr>> {
    let mut bounded = Vec::with_capacity(addresses.len().min(limit));
    for address in addresses {
        if address.port() != expected_port {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "resolver returned an address with an unexpected port",
            ));
        }
        if !bounded.contains(&address) {
            if bounded.len() == limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "resolver returned more addresses than the configured limit",
                ));
            }
            bounded.push(address);
        }
    }
    if bounded.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "resolver returned no addresses",
        ));
    }
    Ok(bounded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    struct CountingResolver {
        calls: AtomicUsize,
        result: Vec<SocketAddr>,
    }

    impl Resolver for CountingResolver {
        fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.result.clone())
        }
    }

    struct FailingResolver(AtomicUsize);

    impl Resolver for FailingResolver {
        fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "not found"))
        }
    }

    fn address(last_octet: u8, port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, last_octet], port))
    }

    #[test]
    fn cache_rotates_addresses_and_expires_with_injected_instant() {
        let port = 8080;
        let inner = Arc::new(CountingResolver {
            calls: AtomicUsize::new(0),
            result: vec![address(1, port), address(2, port)],
        });
        let cache = CachingResolver::new(
            inner.clone(),
            ResolverCacheConfig {
                positive_ttl: Duration::from_secs(10),
                negative_ttl: Duration::from_secs(2),
                max_entries: 4,
                max_addresses_per_entry: 4,
            },
        )
        .unwrap();
        let now = Instant::now();

        let first = cache.resolve_at("SERVICE", port, now).unwrap();
        let second = cache.resolve_at("service", port, now).unwrap();
        assert_eq!(first.source, ResolutionSource::Resolver);
        assert_eq!(second.source, ResolutionSource::Cache);
        assert_eq!(first.addresses, vec![address(1, port), address(2, port)]);
        assert_eq!(second.addresses, vec![address(2, port), address(1, port)]);
        assert_eq!(inner.calls.load(Ordering::Relaxed), 1);

        let expired = cache
            .resolve_at("service", port, now + Duration::from_secs(11))
            .unwrap();
        assert_eq!(expired.source, ResolutionSource::Resolver);
        assert_eq!(inner.calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn cache_negative_result_is_bounded_and_invalidatable() {
        let inner = Arc::new(FailingResolver(AtomicUsize::new(0)));
        let cache = CachingResolver::new(inner.clone(), ResolverCacheConfig::default()).unwrap();

        for _ in 0..2 {
            assert_eq!(
                cache.resolve("missing.internal", 8080).unwrap_err().kind(),
                io::ErrorKind::AddrNotAvailable
            );
        }
        assert_eq!(inner.0.load(Ordering::Relaxed), 1);
        assert!(cache.invalidate("MISSING.INTERNAL", 8080));
        let _ = cache.resolve("missing.internal", 8080).unwrap_err();
        assert_eq!(inner.0.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn cache_coalesces_concurrent_cold_resolution() {
        struct SlowResolver {
            calls: AtomicUsize,
            address: SocketAddr,
        }

        impl Resolver for SlowResolver {
            fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(30));
                Ok(vec![self.address])
            }
        }

        let port = 8080;
        let inner = Arc::new(SlowResolver {
            calls: AtomicUsize::new(0),
            address: address(1, port),
        });
        let cache =
            Arc::new(CachingResolver::new(inner.clone(), ResolverCacheConfig::default()).unwrap());
        let first_cache = cache.clone();
        let second_cache = cache.clone();
        let first = may::go!(move || first_cache.resolve("service.internal", port));
        let second = may::go!(move || second_cache.resolve("service.internal", port));

        assert_eq!(first.join().unwrap().unwrap(), vec![address(1, port)]);
        assert_eq!(second.join().unwrap().unwrap(), vec![address(1, port)]);
        assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cache_waiter_honours_resolution_deadline() {
        struct SlowResolver {
            started: std::sync::mpsc::Sender<()>,
            address: SocketAddr,
        }

        impl Resolver for SlowResolver {
            fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
                self.started.send(()).unwrap();
                thread::sleep(Duration::from_millis(100));
                Ok(vec![self.address])
            }
        }

        let port = 8080;
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let cache = Arc::new(
            CachingResolver::new(
                Arc::new(SlowResolver {
                    started: started_tx,
                    address: address(1, port),
                }),
                ResolverCacheConfig::default(),
            )
            .unwrap(),
        );
        let cold_cache = cache.clone();
        let cold = may::go!(move || cold_cache.resolve("service.internal", port));
        started_rx.recv().unwrap();

        let error = cache
            .resolve_with_deadline(
                "service.internal",
                port,
                Instant::now() + Duration::from_millis(10),
            )
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(cold.join().unwrap().unwrap(), vec![address(1, port)]);
    }

    #[test]
    fn service_resolver_updates_removes_bounds_and_rotates() {
        let port = 8443;
        let resolver = ServiceResolver::new(ServiceResolverConfig {
            max_services: 1,
            max_addresses_per_service: 2,
        })
        .unwrap();
        resolver
            .update(
                "identity.internal",
                port,
                vec![address(1, port), address(2, port)],
            )
            .unwrap();
        assert_eq!(
            resolver.resolve("IDENTITY.INTERNAL", port).unwrap(),
            vec![address(1, port), address(2, port)]
        );
        assert_eq!(
            resolver.resolve("identity.internal", port).unwrap(),
            vec![address(2, port), address(1, port)]
        );
        assert!(resolver
            .update("other.internal", port, vec![address(3, port)])
            .is_err());
        assert!(resolver.remove("identity.internal", port));
        assert_eq!(
            resolver
                .resolve("identity.internal", port)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AddrNotAvailable
        );
    }
}
