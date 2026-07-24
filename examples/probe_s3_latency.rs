//! Measure ranged-GET latency against a real AWS S3 endpoint.
//!
//! The disk-less read path is round-trip-bound: a selective query issues ~87
//! ranged GETs, so the cost is (requests / effective concurrency) x per-request
//! latency. Every latency figure in `docs/benchmarking.md` comes from MinIO on
//! loopback, where a request costs ~0.5 ms. This measures the real constant.
//!
//! It reads anonymously from an AWS Open Data bucket — public, free to read,
//! and used here at a few hundred small ranged GETs — because no publicly
//! available S3-compatible endpoint accepts *writes*, so a full graph cannot be
//! staged. That limits this to calibrating the per-request constant rather than
//! running the whole harness.
//!
//! ```text
//! cargo run --release --features object-store --example probe_s3_latency
//! ```
//!
//! Override with `PROBE_BUCKET`, `PROBE_REGION`, `PROBE_CONCURRENCY`, `PROBE_N`.

#[cfg(not(feature = "object-store"))]
fn main() {
    eprintln!("run with: --features object-store");
}

#[cfg(feature = "object-store")]
fn main() {
    use futures::stream::StreamExt;
    use object_store::{aws::AmazonS3Builder, path::Path as ObjectPath, GetOptions, GetRange};
    use object_store::{ObjectMeta, ObjectStore};
    use std::env;
    use std::sync::Arc;
    use std::time::Instant;

    fn env_usize(k: &str, d: usize) -> usize {
        env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
    }

    let bucket = env::var("PROBE_BUCKET").unwrap_or_else(|_| "noaa-gfs-bdp-pds".to_string());
    let region = env::var("PROBE_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let n = env_usize("PROBE_N", 60);
    let concurrency = env_usize("PROBE_CONCURRENCY", 16);
    // Matches the size of the small per-layer index structures the disk-less
    // path fetches.
    let read_len: usize = env_usize("PROBE_READ_BYTES", 8 * 1024);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let store: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name(&bucket)
                .with_region(&region)
                .with_skip_signature(true)
                .build()
                .expect("failed to build anonymous S3 client"),
        );

        // Find one object big enough to range-read repeatedly at distinct offsets.
        let mut listing = store.list(None);
        let mut target: Option<ObjectMeta> = None;
        while let Some(item) = listing.next().await {
            let meta = item.expect("list failed");
            if meta.size as usize > n * read_len {
                target = Some(meta);
                break;
            }
        }
        let target = target.expect("no object large enough found");
        println!("=== ranged-GET latency probe ===");
        println!("bucket: s3://{} ({})", bucket, region);
        println!(
            "object: {} ({:.1} MiB)",
            target.location,
            target.size as f64 / (1024.0 * 1024.0)
        );
        println!(
            "reads:  {} x {} B, concurrency {}\n",
            n, read_len, concurrency
        );

        let path: ObjectPath = target.location.clone();
        let fetch = |i: usize| {
            let store = store.clone();
            let path = path.clone();
            async move {
                let start = i * read_len;
                let opts = GetOptions {
                    range: Some(GetRange::Bounded(start..start + read_len)),
                    ..Default::default()
                };
                let t = Instant::now();
                let r = store.get_opts(&path, opts).await;
                let ok = match r {
                    Ok(res) => res.bytes().await.is_ok(),
                    Err(_) => false,
                };
                (t.elapsed(), ok)
            }
        };

        // Warm the connection pool so the first TLS handshake is not counted.
        for i in 0..4 {
            let _ = fetch(i).await;
        }

        let mut sequential: Vec<f64> = Vec::with_capacity(n);
        for i in 0..n {
            let (d, ok) = fetch(i).await;
            if ok {
                sequential.push(d.as_secs_f64() * 1000.0);
            }
        }
        sequential.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let pct = |v: &[f64], p: f64| -> f64 {
            if v.is_empty() {
                return 0.0;
            }
            v[((v.len() as f64 * p) as usize).min(v.len() - 1)]
        };

        println!("sequential (1 in flight, warm connection):");
        println!(
            "  p50 {:.1}ms  p95 {:.1}ms  p99 {:.1}ms  ({} samples)",
            pct(&sequential, 0.50),
            pct(&sequential, 0.95),
            pct(&sequential, 0.99),
            sequential.len()
        );

        let t = Instant::now();
        let results: Vec<(std::time::Duration, bool)> = futures::stream::iter(0..n)
            .map(fetch)
            .buffer_unordered(concurrency)
            .collect()
            .await;
        let wall = t.elapsed().as_secs_f64();
        let mut conc: Vec<f64> = results
            .iter()
            .filter(|(_, ok)| *ok)
            .map(|(d, _)| d.as_secs_f64() * 1000.0)
            .collect();
        conc.sort_by(|a, b| a.partial_cmp(b).unwrap());

        println!("\nconcurrent ({} in flight):", concurrency);
        println!(
            "  p50 {:.1}ms  p95 {:.1}ms   throughput {:.0} requests/s",
            pct(&conc, 0.50),
            pct(&conc, 0.95),
            conc.len() as f64 / wall
        );

        // What this implies for a disk-less query, which issues ~87 ranged GETs.
        let per_req = pct(&sequential, 0.50);
        let eff = conc.len() as f64 / wall;
        println!("\nimplied cost of one selective query (87 ranged GETs):");
        println!("  fully serial      {:.0} ms", 87.0 * per_req);
        println!(
            "  at this concurrency {:.0} ms",
            87.0 / eff.max(1e-9) * 1000.0
        );
    });
}
