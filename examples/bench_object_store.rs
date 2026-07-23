//! Backend-agnostic benchmark harness for the object-store backend.
//!
//! The same harness points at an **in-memory** bucket (default, zero setup) or
//! any **S3-compatible** store (MinIO, Cloudflare R2, AWS S3) purely via the
//! environment — so correctness and latency-distribution runs need no cloud
//! account, and only the throttling-at-scale question needs real cloud creds.
//!
//! ```text
//! # in-memory (default):
//! cargo run --release --example bench_object_store --features object-store
//!
//! # against MinIO (real S3 API, self-generated keys, no cloud account):
//! TDB_OBJECT_STORE_ENDPOINT=http://localhost:9000 \
//! TDB_OBJECT_STORE_BUCKET=bench \
//! cargo run --release --example bench_object_store --features object-store
//! ```
//!
//! To see realistic round-trip / tail latency without a cloud bill, put a
//! network emulator in front of a *remote* MinIO (or loopback, with care):
//!
//! ```text
//! # ~5 ms mean + 2 ms jitter, 0.5% loss, on the interface MinIO is reached over:
//! sudo tc qdisc add dev <iface> root netem delay 5ms 2ms distribution normal loss 0.5%
//! # ... run the harness ...
//! sudo tc qdisc del dev <iface> root netem
//! ```
//!
//! MinIO validates the API and (with netem) the latency distribution, but it
//! does **not** reproduce AWS request-rate throttling (503 SlowDown) — for that,
//! point the same env vars at real S3/R2. The harness reports request *counts*
//! per scenario precisely, which is the number that predicts throttling risk.
//!
//! Config (all optional; unset endpoint -> in-memory):
//!   TDB_OBJECT_STORE_ENDPOINT / _BUCKET / _ACCESS_KEY_ID / _SECRET_ACCESS_KEY / _REGION
//!   BENCH_DEPTH   (layer-chain depth, default 12)
//!   BENCH_REPS    (repetitions per read scenario, default 20)
//!   BENCH_WRITES  (small commits in the write scenario, default 50)

#[cfg(not(feature = "object-store"))]
fn main() {
    eprintln!("run with: --features object-store");
}

#[cfg(feature = "object-store")]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    harness::run().await;
}

#[cfg(feature = "object-store")]
mod harness {
    use std::env;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::path::Path as OsPath;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };

    use terminus_store::store::buffered::BufferedNamedGraph;
    use terminus_store::store::Store;
    use terminus_store::{open_object_store, ValueTriple};

    // ---- per-request metering ----

    #[derive(Default)]
    struct Meter {
        samples: Mutex<Vec<(&'static str, u64, u128)>>, // (op, bytes, micros)
    }
    impl Meter {
        fn record(&self, op: &'static str, bytes: u64, micros: u128) {
            self.samples.lock().unwrap().push((op, bytes, micros));
        }
        fn take(&self) -> Vec<(&'static str, u64, u128)> {
            std::mem::take(&mut *self.samples.lock().unwrap())
        }
    }

    struct MeteredStore {
        inner: Arc<dyn ObjectStore>,
        meter: Arc<Meter>,
    }
    impl std::fmt::Debug for MeteredStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MeteredStore")
        }
    }
    impl std::fmt::Display for MeteredStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MeteredStore")
        }
    }

    #[async_trait]
    impl ObjectStore for MeteredStore {
        async fn put_opts(
            &self,
            l: &OsPath,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            let bytes = p.content_length() as u64;
            let t = Instant::now();
            let r = self.inner.put_opts(l, p, o).await;
            self.meter.record("put", bytes, t.elapsed().as_micros());
            r
        }
        async fn put_multipart_opts(
            &self,
            l: &OsPath,
            o: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &OsPath, o: GetOptions) -> object_store::Result<GetResult> {
            let head = o.head;
            let range_bytes = match &o.range {
                Some(object_store::GetRange::Bounded(r)) => (r.end - r.start) as u64,
                _ => 0,
            };
            let t = Instant::now();
            let r = self.inner.get_opts(l, o).await;
            let micros = t.elapsed().as_micros();
            self.meter
                .record(if head { "head" } else { "get" }, range_bytes, micros);
            r
        }
        async fn delete(&self, l: &OsPath) -> object_store::Result<()> {
            let t = Instant::now();
            let r = self.inner.delete(l).await;
            self.meter.record("delete", 0, t.elapsed().as_micros());
            r
        }
        fn list(&self, p: Option<&OsPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.meter.record("list", 0, 0);
            self.inner.list(p)
        }
        async fn list_with_delimiter(
            &self,
            p: Option<&OsPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy(&self, f: &OsPath, t: &OsPath) -> object_store::Result<()> {
            self.inner.copy(f, t).await
        }
        async fn copy_if_not_exists(&self, f: &OsPath, t: &OsPath) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(f, t).await
        }
    }

    // ---- store construction ----

    fn base_store() -> (Arc<dyn ObjectStore>, String) {
        match env::var("TDB_OBJECT_STORE_ENDPOINT") {
            Ok(endpoint) => {
                use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
                let bucket =
                    env::var("TDB_OBJECT_STORE_BUCKET").unwrap_or_else(|_| "bench".to_string());
                let s3 = AmazonS3Builder::new()
                    .with_endpoint(&endpoint)
                    .with_bucket_name(&bucket)
                    .with_access_key_id(
                        env::var("TDB_OBJECT_STORE_ACCESS_KEY_ID")
                            .unwrap_or_else(|_| "minioadmin".to_string()),
                    )
                    .with_secret_access_key(
                        env::var("TDB_OBJECT_STORE_SECRET_ACCESS_KEY")
                            .unwrap_or_else(|_| "minioadmin".to_string()),
                    )
                    .with_region(
                        env::var("TDB_OBJECT_STORE_REGION")
                            .unwrap_or_else(|_| "us-east-1".to_string()),
                    )
                    .with_allow_http(endpoint.starts_with("http://"))
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()
                    .expect("failed to build S3 store");
                (Arc::new(s3), format!("s3 @ {}/{}", endpoint, bucket))
            }
            Err(_) => (
                Arc::new(object_store::memory::InMemory::new()),
                "in-memory".to_string(),
            ),
        }
    }

    fn env_usize(key: &str, default: usize) -> usize {
        env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    // ---- reporting ----

    fn pct(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[i]
    }

    /// One read scenario: run `op` `reps` times over a fresh cold store each time,
    /// and report wall-clock distribution + per-run request/byte breakdown.
    async fn read_scenario<F, Fut>(name: &str, reps: usize, meter: &Arc<Meter>, mut fresh: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = (Store, [u32; 5])>,
    {
        let mut wall = Vec::with_capacity(reps);
        let mut per_op: std::collections::BTreeMap<&'static str, (u64, u64)> = Default::default(); // op -> (count, bytes)
        let mut req_latencies: Vec<u128> = Vec::new();
        for _ in 0..reps {
            let (store, head) = fresh().await;
            meter.take(); // discard setup/build requests
            let t = Instant::now();
            run_op(name, &store, head).await;
            wall.push(t.elapsed().as_micros());
            for (op, bytes, micros) in meter.take() {
                let e = per_op.entry(op).or_default();
                e.0 += 1;
                e.1 += bytes;
                if op == "get" || op == "put" {
                    req_latencies.push(micros);
                }
            }
        }
        wall.sort_unstable();
        req_latencies.sort_unstable();
        let n = reps as u64;
        let reqs: u64 = per_op.values().map(|(c, _)| c).sum::<u64>() / n;
        let bytes: u64 = per_op.values().map(|(_, b)| b).sum::<u64>() / n;
        println!("\n{}", name);
        println!(
            "  wall-clock  p50 {:>8}  p95 {:>8}  p99 {:>8}",
            ms(pct(&wall, 0.50)),
            ms(pct(&wall, 0.95)),
            ms(pct(&wall, 0.99)),
        );
        println!(
            "  per run     {:>3} requests   {} transferred",
            reqs,
            human(bytes)
        );
        let ops: Vec<String> = per_op
            .iter()
            .map(|(op, (c, _))| format!("{} {}", c / n.max(1), op))
            .collect();
        println!("  request mix {}", ops.join(", "));
        if !req_latencies.is_empty() {
            println!(
                "  per-request p50 {}  p95 {}  p99 {}   ({} data requests sampled)",
                ms(pct(&req_latencies, 0.50)),
                ms(pct(&req_latencies, 0.95)),
                ms(pct(&req_latencies, 0.99)),
                req_latencies.len(),
            );
        }
    }

    async fn run_op(name: &str, store: &Store, head: [u32; 5]) {
        match name {
            "cold whole-layer read (get_layer)" => {
                let _ = store.get_layer_from_id(head).await.unwrap().unwrap();
            }
            "selective existence, present (disk-less)" => {
                let t = ValueTriple::new_string_value("s00042", "p", "o00042");
                assert!(store.selective_value_triple_exists(head, &t).await.unwrap());
            }
            "selective existence, absent (full chain walk)" => {
                // resolvable terms (s00042, p, o00043 all exist) but the triple
                // does not — so it resolves and then walks the whole chain.
                let t = ValueTriple::new_string_value("s00042", "p", "o00043");
                assert!(!store.selective_value_triple_exists(head, &t).await.unwrap());
            }
            "full scan (disk-less)" => {
                let n = store.selective_id_triples(head).await.unwrap().count();
                assert!(n > 0);
            }
            _ => unreachable!(),
        }
    }

    fn ms(micros: u128) -> String {
        format!("{:.2}ms", micros as f64 / 1000.0)
    }
    fn human(bytes: u64) -> String {
        if bytes >= 1024 * 1024 {
            format!("{:.1} MiB", bytes as f64 / 1048576.0)
        } else if bytes >= 1024 {
            format!("{:.1} KiB", bytes as f64 / 1024.0)
        } else {
            format!("{} B", bytes)
        }
    }

    pub async fn run() {
        let depth = env_usize("BENCH_DEPTH", 12);
        let reps = env_usize("BENCH_REPS", 20);
        let writes = env_usize("BENCH_WRITES", 50);
        let (raw, desc) = base_store();
        let meter = Arc::new(Meter::default());
        let prefix = format!("bench/{:016x}", rand::random::<u64>());

        println!("=== object-store benchmark ===");
        println!("backend: {}", desc);
        println!(
            "depth: {} layers   reps: {}   writes: {}   prefix: {}",
            depth, reps, writes, prefix
        );

        // Build the chain once, into the metered store (build cost is reported).
        let metered: Arc<dyn ObjectStore> = Arc::new(MeteredStore {
            inner: raw.clone(),
            meter: meter.clone(),
        });
        meter.take();
        let build_t = Instant::now();
        let head = build_chain(&metered, &prefix, depth).await;
        let build = build_t.elapsed();
        let build_reqs = meter.take().len();
        println!(
            "\nbuilt {}-layer chain in {} ({} object-store requests)",
            depth,
            ms(build.as_micros()),
            build_reqs
        );

        // Fresh cold Store per repetition (cache 0 -> disk-less, nothing resident).
        let fresh = || {
            let m = metered.clone();
            let p = prefix.clone();
            async move { (open_object_store(m, p, 0), head) }
        };

        for name in [
            "cold whole-layer read (get_layer)",
            "selective existence, present (disk-less)",
            "selective existence, absent (full chain walk)",
            "full scan (disk-less)",
        ] {
            read_scenario(name, reps, &meter, fresh).await;
        }

        write_scenario(&metered, &prefix, &meter, writes).await;
        println!("\n=== done ===");
    }

    async fn build_chain(store: &Arc<dyn ObjectStore>, prefix: &str, depth: usize) -> [u32; 5] {
        let s = open_object_store(store.clone(), prefix.to_string(), 1 << 30);
        let db = s.create("g").await.unwrap();
        // A base large enough to cross the block-lazy threshold (so a selective
        // query fetches dictionary blocks, not whole dictionaries).
        let base = env_usize("BENCH_BASE", 3000);
        let builder = s.create_base_layer().await.unwrap();
        for i in 0..base {
            builder
                .add_value_triple(ValueTriple::new_string_value(
                    &format!("s{:05}", i),
                    "p",
                    &format!("o{:05}", i),
                ))
                .unwrap();
        }
        let mut head = builder.name();
        let mut layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();
        for d in 1..depth {
            let b = layer.open_write().await.unwrap();
            for i in 0..40 {
                let k = d * 40 + i;
                b.add_value_triple(ValueTriple::new_string_value(
                    &format!("s{:05}", 200 + k),
                    "p",
                    &format!("o{:05}", 200 + k),
                ))
                .unwrap();
            }
            head = b.name();
            layer = b.commit().await.unwrap();
            db.set_head(&layer).await.unwrap();
        }
        head
    }

    /// Durable, disk-less, batched writes via a bucket-backed WAL: p50/p99 of a
    /// small commit and the object-store request cost per commit.
    async fn write_scenario(
        store: &Arc<dyn ObjectStore>,
        prefix: &str,
        meter: &Arc<Meter>,
        n: usize,
    ) {
        // dedicated graph for writing
        let s = open_object_store(store.clone(), format!("{}/w", prefix), 1 << 20);
        let db = s.create("wg").await.unwrap();
        let builder = s.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("seed", "p", "0"))
            .unwrap();
        let layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();

        let buffered = BufferedNamedGraph::open_with_object_wal(
            s.clone(),
            "wg",
            0,
            store.clone(),
            format!("{}/w/wg.wal", prefix),
        )
        .await
        .unwrap();

        meter.take();
        let mut commit_latencies = Vec::with_capacity(n);
        for i in 0..n {
            let t = Instant::now();
            buffered
                .add(ValueTriple::new_string_value(
                    &format!("w{:05}", i),
                    "p",
                    &format!("v{:05}", i),
                ))
                .await
                .unwrap();
            commit_latencies.push(t.elapsed().as_micros());
        }
        let durable_reqs = meter.take().len();
        let flush_t = Instant::now();
        buffered.flush().await.unwrap();
        let flush = flush_t.elapsed();
        let flush_reqs = meter.take().len();

        commit_latencies.sort_unstable();
        println!("\nbuffered writes (durable, disk-less WAL in bucket)");
        println!(
            "  per commit  p50 {}  p95 {}  p99 {}   ({} durable-append requests total)",
            ms(pct(&commit_latencies, 0.50)),
            ms(pct(&commit_latencies, 0.95)),
            ms(pct(&commit_latencies, 0.99)),
            durable_reqs,
        );
        println!(
            "  flush of {} commits: {} ({} requests -> 1 layer object + label CAS + WAL checkpoint)",
            n,
            ms(flush.as_micros()),
            flush_reqs,
        );
    }
}
