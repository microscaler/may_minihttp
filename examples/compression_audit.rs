//! Measure gzip's wire-size and wall-clock trade-off for service payloads.
//!
//! This is a discovery probe for CA-06, not production response decompression. It uses bounded,
//! deterministic fixtures by default and accepts file paths as positional arguments when captured
//! service payloads are available:
//!
//! ```text
//! cargo run --example compression_audit -- payload.json another-response.json
//! ```
//!
//! The latency columns are wall-clock measurements for the local process. They are useful for
//! comparing the same machine and fixture, but are not a substitute for a platform CPU profile.

use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::time::Instant;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

const ITERATIONS: usize = 100;

struct Fixture {
    name: String,
    payload: Vec<u8>,
}

fn main() -> io::Result<()> {
    let fixtures = load_fixtures()?;
    println!(
        "fixture,plain_bytes,gzip_bytes,wire_ratio,encode_p50_us,encode_p95_us,decode_p50_us,decode_p95_us"
    );

    for fixture in fixtures {
        let compressed = gzip(&fixture.payload)?;
        let mut encode_samples = Vec::with_capacity(ITERATIONS);
        let mut decode_samples = Vec::with_capacity(ITERATIONS);

        for _ in 0..ITERATIONS {
            let started = Instant::now();
            let encoded = gzip(&fixture.payload)?;
            encode_samples.push(started.elapsed().as_micros());
            debug_assert_eq!(encoded, compressed);

            let started = Instant::now();
            let decoded = gunzip(&compressed)?;
            decode_samples.push(started.elapsed().as_micros());
            debug_assert_eq!(decoded, fixture.payload);
        }

        encode_samples.sort_unstable();
        decode_samples.sort_unstable();
        let ratio = compressed.len() as f64 / fixture.payload.len().max(1) as f64;
        println!(
            "{},{},{},{ratio:.4},{},{},{},{}",
            fixture.name,
            fixture.payload.len(),
            compressed.len(),
            percentile(&encode_samples, 0.50),
            percentile(&encode_samples, 0.95),
            percentile(&decode_samples, 0.50),
            percentile(&decode_samples, 0.95),
        );
    }

    Ok(())
}

fn load_fixtures() -> io::Result<Vec<Fixture>> {
    let paths: Vec<_> = env::args_os().skip(1).collect();
    if !paths.is_empty() {
        return paths
            .into_iter()
            .map(|path| {
                let name = path.to_string_lossy().into_owned();
                let payload = fs::read(&path)?;
                Ok(Fixture { name, payload })
            })
            .collect();
    }

    Ok(vec![
        Fixture {
            name: "idam-user-list".into(),
            payload: json_fixture("user", 128),
        },
        Fixture {
            name: "brrtrouter-route-table".into(),
            payload: json_fixture("route", 256),
        },
        Fixture {
            name: "hauliage-bulk-records".into(),
            payload: json_fixture("record", 1024),
        },
    ])
}

fn json_fixture(kind: &str, count: usize) -> Vec<u8> {
    let mut payload = String::from("{\"items\":[");
    for index in 0..count {
        if index != 0 {
            payload.push(',');
        }
        payload.push_str(&format!(
            "{{\"kind\":\"{kind}\",\"id\":{index},\"tenant\":\"microscaler\",\"active\":true}}"
        ));
    }
    payload.push_str("]}");
    payload.into_bytes()
}

fn gzip(payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload)?;
    encoder.finish()
}

fn gunzip(payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(payload);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded)?;
    Ok(decoded)
}

fn percentile(samples: &[u128], percentile: f64) -> u128 {
    debug_assert!(!samples.is_empty());
    let index = ((samples.len() - 1) as f64 * percentile).round() as usize;
    samples[index]
}
