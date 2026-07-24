# Benchmarking the object-store backend

`examples/bench_object_store.rs` is a backend-agnostic harness. The **same binary**
points at an in-memory bucket, MinIO, Cloudflare R2, or AWS S3 purely via the
environment, and reports for each query class:

- **wall-clock** p50 / p95 / p99 (over `BENCH_REPS` cold runs),
- **requests per run** and the **request mix** (get / head / put / list) — the number that predicts request-rate throttling,
- **bytes transferred** per run,
- **per-request** p50 / p95 / p99 latency (the tail that a deep, many-request query is exposed to).

It covers a cold whole-layer read (the old path), selective existence (present and absent — disk-less), a full scan (disk-less), durable batched writes through a **bucket-backed WAL** (disk-less), and a **concurrent load test** that drives many queries in flight and reports **object-store requests/second** — the number that predicts request-rate throttling.

## The three tiers

### 1. In-memory (default) — correctness + request/byte accounting, zero setup

```sh
cargo run --release --example bench_object_store --features object-store
```

Latency is ~0 (no network), but **request counts and bytes are exact**. Use this to
see the bytes/requests trade — e.g. selective existence moves a fraction of a
whole-layer read's bytes, but still makes many small requests.

### 2. MinIO (+ netem) — the real S3 API and a realistic latency distribution, no cloud account

Run MinIO (self-generated keys, no cloud account):

```sh
docker run -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data
# create the bucket once (mc, or the console at :9001)
```

Point the harness at it:

```sh
TDB_OBJECT_STORE_ENDPOINT=http://localhost:9000 \
TDB_OBJECT_STORE_BUCKET=bench \
cargo run --release --example bench_object_store --features object-store
```

To get a **realistic round-trip / tail latency** without a cloud bill, put a network
emulator in front. Best done against MinIO on a *separate host/VM* (netem on the
NIC facing it); on a single box you can emulate on loopback, but it affects all
loopback traffic, so use it only for a quick read:

```sh
# ~5 ms mean, 2 ms normal jitter, 0.5% loss:
sudo tc qdisc add dev <iface> root netem delay 5ms 2ms distribution normal loss 0.5%
#   ... run the harness ...
sudo tc qdisc del dev <iface> root netem
```

This validates the **latency distribution** (p99, tail) that the constant-delay
unit benchmark (`bench_selective_existence_latency`) cannot.

### 3. Real S3 / R2 — the throttling question (needs credentials)

MinIO does **not** reproduce AWS request-rate throttling (`503 SlowDown`). The
disk-less path trades *fewer bytes* for *more requests*, so throttling at scale is
the one risk only real cloud can settle. Point the same env vars at S3 or R2
(R2 is cheap — no egress fees):

```sh
TDB_OBJECT_STORE_ENDPOINT=https://<accountid>.r2.cloudflarestorage.com \
TDB_OBJECT_STORE_BUCKET=bench \
TDB_OBJECT_STORE_ACCESS_KEY_ID=<token-id> \
TDB_OBJECT_STORE_SECRET_ACCESS_KEY=<token-secret> \
cargo run --release --example bench_object_store --features object-store
```

Drive it with the **concurrent load test** — `BENCH_CONCURRENCY` queries in flight —
and watch **requests/second** and **p99**. AWS S3 caps ~5,500 GET/s per prefix; the
in-memory tier already shows this path sustaining *hundreds of thousands* of
requests/s at modest concurrency, so on real S3/R2 throttling (503 SlowDown →
backoff-inflated p99, then errors if retries exhaust) will appear as you raise
concurrency. If it does, the fix is **request coalescing** (fetch contiguous
structures in one GET) — and the harness's requests/s figure is what tells you it is
needed, and by how much.

```sh
# throttling probe against R2:
TDB_OBJECT_STORE_ENDPOINT=https://<accountid>.r2.cloudflarestorage.com \
TDB_OBJECT_STORE_BUCKET=bench TDB_OBJECT_STORE_ACCESS_KEY_ID=... TDB_OBJECT_STORE_SECRET_ACCESS_KEY=... \
BENCH_CONCURRENCY=64 BENCH_LOAD=4000 \
cargo run --release --example bench_object_store --features object-store
```

> Credentials never go through this repo. Set them in your own shell/CI secret and
> run the harness yourself; the harness only reads standard `object_store` env vars.

## Measured: MinIO tier

Run on 2026-07-24 against MinIO on loopback (`--address 127.0.0.1:9377`), a
12-layer chain over a 2,000-triple base. This is a real S3 API over real HTTP
with real conditional PUT — but ~0.5 ms RTT, so treat wall-clock as a lower
bound and request counts as the transferable number.

| query class | wall-clock p50 | requests | bytes |
|---|---|---|---|
| cold whole-layer read | 223.6 ms | 522 | 35.6 KiB |
| selective existence, present | 52.7 ms | 350 | 11.1 KiB |
| selective existence, absent | 51.9 ms | 350 | 11.1 KiB |
| full scan (disk-less) | 19.9 ms | 229 | 11.8 KiB |

Buffered writes: 0.91 ms p50 per commit; a 20-commit flush takes 19.6 ms in 33
requests (one layer object, one label CAS, one WAL checkpoint).

### The throttling result

Concurrency sweep, 400 queries per run:

| in flight | queries/s | object-store req/s | query p50 |
|---|---|---|---|
| 1 | 22 | 6,660 | 44.9 ms |
| 4 | 60 | 18,188 | 63.6 ms |
| 16 | 60 | 18,080 | 264.9 ms |
| 64 | 67 | 20,352 | 911.4 ms |

Throughput saturates around 60 queries/s past concurrency 4 while latency grows
linearly — that ceiling is MinIO's, single-node on loopback, not S3's.

**The number that transfers is requests per query: ~350** for a selective
existence check on a 12-layer chain. That is a property of this code, not of the
backend, and a *single-threaded* query stream measured 6,660 requests/s — above
S3's documented ~5,500 GET/s per prefix. So the disk-less path does trade bytes
for request count, and request count is what S3 rate-limits.

**But dividing that whole-query rate by the per-prefix cap overstates the
problem, and an earlier version of this document did exactly that.** S3's limit
applies per prefix, and layer objects are already keyed
`<first-3-hex-of-name>/<name>.larch` — names are content hashes, so objects are
spread over 4,096 prefixes. A query's requests therefore land on roughly as many
prefixes as it touches layers. Measured on the 12-layer chain
(`profile_selective_request_breakdown` reports it):

| | requests | prefixes touched | busiest prefix | implied ceiling |
|---|---|---|---|---|
| coalescing off | 369 | 12 | 39 | ~141 queries/s |
| coalescing on | 24 | 12 | 2 | **~2,750 queries/s** |

So the ceiling is set by the *busiest* prefix, not the query total: roughly
**2,750 disk-less queries per second** on one graph with coalescing on, about
20× better than without it, and far better than the naive whole-query division
suggested.

Two caveats. Every query on a given graph hits that same set of layer prefixes,
so this is a per-graph ceiling; unrelated graphs use different layers and
different prefixes. And AWS partitions dynamically rather than giving each key
prefix a fixed budget, so only a real S3 run can confirm the number.

### Request coalescing — implemented

Profiling where the *requests* went (`profile_selective_request_breakdown`)
found no single structure dominating. The largest was the node dictionary's
block binary search at 32 requests; the bulk was ~20 different structures each
costing 11–13 requests, one per layer, each tiny — `NegSPAdjacencyListBits` was
11 requests for **88 bytes total**, eight bytes per request. Those are control
words and index headers.

So the fix is to fetch each layer's small structures together. Note the backend
has **three** read paths — `get_layer_structure_bytes`,
`get_layer_structure_range` and `read_layer_structure_bytes_from` — and the
third carries the logarray control-word reads: eight bytes at the end of a
structure, three per layer per query. Wiring only the first two left most of the
benefit unclaimed. Routing all three through the region cache is what took a
12-layer query from 87 requests to **24**, and a single layer to **2** (a header
probe plus one span).

Measured on MinIO at concurrency 4, toggled with
`TDB_COALESCE_MAX_STRUCTURE_BYTES`:

| | coalescing off | coalescing on | |
|---|---|---|---|
| selective existence p50 | 53.1 ms | **11.0 ms** | 4.8× faster |
| requests per query (cold) | 350 | **24** | 15× fewer |
| bytes per query | 11.1 KiB | 35.6 KiB | 3.2× **more** |
| throughput under load | 65 q/s | **2,076 q/s** | 32× |
| query p50 under load | 61.0 ms | **1.8 ms** | 34× lower |
| object-store req/s | 19,459 | **125** | 156× less pressure |

The last row is the striking one. At 2,076 queries/s the store sees 125
requests/s — about **0.06 requests per query in steady state**. Layers are
immutable, so a cached span is valid forever and warm queries barely touch the
network at all. The 24 is the *cold* cost.

Requests scale with chain depth at roughly **2 per layer**:

| chain depth | requests (cold) |
|---|---|
| 1 (rolled up) | **2** |
| 2 | 4 |
| 4 | 8 |
| 12 | 24 |

Two requests for a rolled-up graph is fewer round trips than a columnar store
typically needs for a single row group, so on this axis the disk-less path is no
longer behind.

**This is a trade, not a free win.** Coalescing fetches whole small structures
where the uncoalesced path took only the bytes it needed, so byte transfer rises
~3× — to *more* than reading the whole chain. That is the right trade against a
store that rate-limits or bills per request while bandwidth is cheap, and the
wrong one where bytes are scarce. Hence the knobs:
`TDB_COALESCE_MAX_STRUCTURE_BYTES=0` disables it, the default is 8 KiB, and
`ObjectArchiveBackend::without_coalescing()` disables it for one backend.

A whole-layer fetch was also tried — probe the object's size, pull the whole
thing when it is small — on the theory that one GET beats ten. Measured, it
changed the request count not at all (2/8/24 either way) and cost slightly more
bytes, because the coalesced span already covers nearly the whole object at this
layer shape. The measurement was kept and the code dropped.

Note this invalidates the "a selective read moves ~8% of a full layer" claim
*when coalescing is on*. With it off, that claim still holds.

**Prefix sharding is already in place** and was mistaken for future work when
this document first flagged it: `layer_key` has always keyed objects by the
first three hex characters of the layer name, which is exactly the pattern AWS
recommends for spreading load across partitions. There is nothing to add there;
the measurement above already reflects it.

The materialized path pays 522 requests once and then serves from cache; the
disk-less path pays ~350 per query. That is the real trade, and it argues for
disk-less on large-working-set / low-QPS workloads rather than as a blanket
default.

## Measured: real AWS S3 (read-only calibration)

Every latency figure above comes from MinIO on loopback, where a request costs
~0.5 ms. That understates a real deployment badly, so this calibrates the
per-request constant against genuine AWS S3.

**Why only a calibration.** There is no publicly available S3-compatible
endpoint that accepts *writes*, so a graph cannot be staged on one and the full
harness cannot run. MinIO's `play.min.io` playground would have been the
candidate, but its published credentials are now rejected
(`SignatureDoesNotMatch`) and anonymous access returns 403. What *is* available
is anonymous **reads** from AWS Open Data buckets, which is enough to measure
what dominates a disk-less query: ranged-GET latency.

`examples/probe_s3_latency.rs`, against `s3://noaa-gfs-bdp-pds` (us-east-1),
8 KiB ranged reads matching the small per-layer structures:

| in flight | per-request p50 | p95 | throughput |
|---|---|---|---|
| 1 (warm connection) | **124.9 ms** | 133.2 ms | 8 req/s |
| 8 | 127.6 ms | 335.4 ms | 54 req/s |
| 16 | 127.8 ms | 346.9 ms | 83 req/s |
| 32 | 129.9 ms | 352.9 ms | 150 req/s |
| 64 | 381.3 ms | 405.3 ms | 183 req/s |

The 125 ms is round-trip time from this host to us-east-1 — a machine on
another continent. **It is not a deployment number**; compute co-located with
its bucket sees ~0.5–2 ms. The ~180 req/s ceiling is this host's egress and
connection pool, not S3 throttling.

### What it means for the disk-less path

Query cost is `requests x RTT / effective concurrency`, and the request count is
now measured at 87 per selective query:

| deployment | RTT | implied query cost |
|---|---|---|
| compute co-located with bucket | ~1 ms | **~3 ms** at concurrency 32 |
| cross-continent (measured here) | 125 ms | ~580 ms at concurrency 32 |
| cross-continent, fully serial | 125 ms | 10.9 s |

Two conclusions, both actionable:

- **Co-locate compute with the bucket.** The disk-less path is round-trip-bound
  by construction, so cross-region deployment is not a slower configuration, it
  is a broken one. A materialized replica tolerates distance because it pays the
  round trips once; the disk-less path pays them per query.
- **Concurrency is necessary but not sufficient.** It helps sub-linearly here
  and saturates around 180 req/s, so it cannot rescue a high-RTT deployment. It
  is what makes a co-located one fast.

This also re-validates the coalescing work independently of any throttling
argument: 350 to 87 requests is a 4x cut in the term that dominates the cost.

### Still unanswered

The throttling ceiling. That needs sustained writes and load against a bucket
you control, which needs your credentials — the one thing no public endpoint
can substitute for. The question is now sharp: does a 12-layer disk-less graph
sustain roughly 550 queries/s against real S3 before 503 SlowDown appears?

## Measured: Cloudflare R2 (real, credentialed)

Run 2026-07-24 against a real R2 bucket, same 12-layer / 2,000-triple shape as
the MinIO tier. Per-request latency ~55–63 ms from this host.

**R2 accepts the ETag-conditional PUT** the label store's compare-and-swap needs
— the one thing that could have ruled the backend out, and what
`object_store`'s `LocalFileSystem` lacks. Writes, group commit, the bucket WAL
and head updates all work.

Request counts transferred exactly from MinIO: 24 for a selective existence
check at depth 12, identical to loopback. Latency did not, and that is the point
of running it.

### The serial chain walk, and fixing it

The first R2 run showed 1,663 ms for a query issuing 24 requests at ~63 ms each
— essentially **serial**. The cause was chain discovery:
`retrieve_layer_stack_names` walks parent pointers, and you cannot read layer
N−1 until layer N's parent pointer tells you its id. Twelve layers, twelve
sequential round trips, and no amount of downstream concurrency helps.

A `.stack` manifest that records the whole chain in one object already existed
and was already being written — but it was only used to warm caches, never for
discovery, so the walk still happened. Wiring it into discovery (as a validated
hint, with the authoritative walk as fallback) and probing layers concurrently:

| | serial walk | + manifest | + no existence HEADs |
|---|---|---|---|
| selective existence p50 | 1,663 ms | 407 ms | **292 ms** |
| requests per query | 24 | 97 (25 get, 72 head) | **25 (all get)** |
| throughput under load | — | 105 q/s | **143 q/s** |
| query p50 under load | — | 2.69 ms | **1.70 ms** |
| object-store req/s under load | — | 817 | **100** |

The manifest bought 4.1× at 4× the requests — the old path stopped at the first
layer holding the string, the new one probes all of them, and those probes were
mostly `layer_exists` HEADs.

Those HEADs turned out to be pure overhead. Every caller that asks whether a
layer exists goes on to read it, and the header GET that follows establishes
existence by itself; `layer_exists` now proves it by fetching the header (cached
and single-flighted) instead of by a separate HEAD. That removed all 72.

The end state is **5.7× faster than the serial walk at the same request count** —
1,663 ms to 292 ms for 24 requests versus 25. The apparent latency/cost tension
was not inherent; it was a redundant round trip.

### Rollup does nothing for the disk-less path

Read cost scales with chain depth, so rollup — which collapses a chain into one
layer without discarding the originals — looked like the largest available
lever. Measured on R2, it is not, because the disk-less path never looks at it.

The rollup itself is cheap: a 12-layer chain rolled up in 569 ms and 5 requests.
The queries afterwards were unchanged:

| | before rollup | after rollup |
|---|---|---|
| selective existence p50 | 273 ms | 240 ms |
| requests per query | 25 | **25** |

Chain discovery reads the `.stack` manifest, or walks parent pointers, and
neither consults the rollup pointer. Only the materialized `get_layer` path does.
So a rolled-up graph is still read as its original twelve layers.

This corrects earlier advice in this document and in the NamiDB comparison, which
treated rollup as the fix for deep chains. **It is the fix for the materialized
path only.**

The optimization is available and now known to be safe: ids are preserved across
rollup, verified by `disk_less_reads_agree_with_materialized_after_rollup`, which
asserts that a disk-less read of the original chain returns the same id as a
materialized read through the rollup. So the disk-less read paths could resolve
`get_rollup(head)` and read the single rolled-up layer instead of the chain,
taking a maintained graph from 25 requests to roughly 3. It has to be scoped to
read-only paths — the delta predicates need the real per-layer chain — which is
why it is not a one-line change.

One caveat the measurement also surfaced: a rolled-up layer contains the whole
dataset, so it is a much larger object. The first cold query against one showed a
24.6 s outlier at p95. Fewer, bigger layers is not uniformly cheaper.

### A measurement error worth recording

The figures of "2 requests per layer" and "24 at depth 12" that appeared in
earlier revisions of this document came from `profile_selective_request_breakdown`,
which was recording only **ranged** GETs. Manifest, rollup-pointer and label
reads are unranged and were invisible to it. Corrected, the same query costs
5 / 9 / 17 / 49 requests at depth 1 / 2 / 4 / 12 — roughly double what was
reported. (After removing the existence HEADs these are 3 / — / — / 25.) The benchmark harness always counted every request, so its numbers
were right; only the profiler's were not.

## Knobs

| Env var | Default | Meaning |
|---|---|---|
| `BENCH_DEPTH` | 12 | layer-chain depth |
| `BENCH_BASE` | 3000 | base-layer entries (keep > 512 to exercise block-lazy dictionaries) |
| `BENCH_REPS` | 20 | cold repetitions per read scenario |
| `BENCH_WRITES` | 50 | small commits in the write scenario |
| `BENCH_CONCURRENCY` | 1 | queries in flight in the load test |
| `BENCH_LOAD` | 400 | total queries in the load test |

## Reading the output

- **bytes** validate the disk-less transfer claims directly (exact on any backend).
- **requests per run** is the throttling-risk signal — a high count with small bytes
  is the block-lazy trade, and is what to watch on real S3.
- **wall-clock vs per-request p99**: on a real/emulated network, a big gap between a
  scenario's wall-clock and a single request's latency means the query is
  round-trip-bound (many requests), which is where coalescing or fewer round trips
  would help.
