//! Capture a pooled HTTP/1.1 baseline for CA-07.
//!
//! ```text
//! cargo run --example client_pool_audit --features client -- \
//!   http://127.0.0.1:8080/health 16 100 8
//! ```
//!
//! Arguments are URL, concurrency, requests per worker, and per-origin connection limit. The
//! endpoint should be a local or deployed Microscaler service path; this probe does not discover
//! or assume HTTP/2 support.

use std::env;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use may_minihttp::client::{Client, ClientEvent, ClientObserver};

#[derive(Default)]
struct WaitObserver {
    total_wait_micros: AtomicU64,
}

impl ClientObserver for WaitObserver {
    fn observe(&self, event: ClientEvent<'_>) {
        if let ClientEvent::PoolWaited { duration, .. } = event {
            self.total_wait_micros
                .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
        }
    }
}

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let url = args
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing URL"))?;
    let concurrency = parse_arg(args.next(), "concurrency", 16)?;
    let requests_per_worker = parse_arg(args.next(), "requests_per_worker", 100)?;
    let per_origin = parse_arg(args.next(), "per_origin_connections", 8)?;
    if concurrency == 0 || requests_per_worker == 0 || per_origin == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "concurrency, requests_per_worker, and per_origin_connections must be greater than zero",
        ));
    }

    may::config().set_stack_size(0x8000);
    let wait_observer = Arc::new(WaitObserver::default());
    let client = Arc::new(
        Client::builder()
            .max_connections(concurrency.max(per_origin))
            .max_connections_per_origin(per_origin)
            .request_timeout(Duration::from_secs(30))
            .observer(wait_observer.clone())
            .build()?,
    );
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(
        concurrency.saturating_mul(requests_per_worker),
    )));
    let errors = Arc::new(Mutex::new(0usize));
    let response_bytes = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut workers = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let client = Arc::clone(&client);
        let latencies = Arc::clone(&latencies);
        let errors = Arc::clone(&errors);
        let response_bytes = Arc::clone(&response_bytes);
        let url = url.clone();
        workers.push(may::go!(move || {
            for _ in 0..requests_per_worker {
                let request_started = Instant::now();
                let result = client.get(&url).and_then(|request| {
                    request.send().map(|response| {
                        response_bytes.fetch_add(response.body().len() as u64, Ordering::Relaxed);
                        response.status().is_success()
                    })
                });
                let elapsed = request_started.elapsed().as_micros();
                latencies
                    .lock()
                    .expect("latency mutex poisoned")
                    .push(elapsed);
                if !matches!(result, Ok(true)) {
                    *errors.lock().expect("error mutex poisoned") += 1;
                }
            }
        }));
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| io::Error::other("pool audit worker panicked"))?;
    }

    let mut latencies = latencies.lock().expect("latency mutex poisoned").clone();
    latencies.sort_unstable();
    let stats = client.stats();
    let error_count = *errors.lock().expect("error mutex poisoned");
    println!("url,{url}");
    println!("requests,{}", latencies.len());
    println!("errors,{error_count}");
    println!("response_bytes,{}", response_bytes.load(Ordering::Relaxed));
    println!("elapsed_ms,{}", started.elapsed().as_millis());
    println!("p50_us,{}", percentile(&latencies, 0.50));
    println!("p95_us,{}", percentile(&latencies, 0.95));
    println!("p99_us,{}", percentile(&latencies, 0.99));
    println!("connections_created,{}", stats.connections_created);
    println!("connections_reused,{}", stats.connections_reused);
    println!("pool_waits,{}", stats.pool_waits);
    println!(
        "pool_wait_time_us,{}",
        wait_observer.total_wait_micros.load(Ordering::Relaxed)
    );
    println!("connections_discarded,{}", stats.connections_discarded);
    Ok(())
}

fn parse_arg<T>(value: Option<String>, name: &str, default: T) -> io::Result<T>
where
    T: std::str::FromStr,
{
    value
        .map(|value| {
            value.parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid {name}: {value}"),
                )
            })
        })
        .unwrap_or(Ok(default))
}

fn percentile(samples: &[u128], percentile: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let index = ((samples.len() - 1) as f64 * percentile).round() as usize;
    samples[index]
}
