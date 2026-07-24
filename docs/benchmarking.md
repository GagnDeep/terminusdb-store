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
backend. Against S3's documented ~5,500 GET/s per prefix:

> **~5,500 ÷ 350 ≈ 15 disk-less queries per second per prefix** before AWS
> starts returning 503 SlowDown.

Even a *single-threaded* query stream measured 6,660 requests/s here, already
above the per-prefix cap. This confirms, with measurements rather than
arithmetic, the risk flagged in the object-store RFC: the disk-less path trades
bytes for request count, and request count is what S3 rate-limits.

### Request coalescing — implemented

Profiling where the *requests* went (`profile_selective_request_breakdown`)
found no single structure dominating. The largest was the node dictionary's
block binary search at 32 requests; the bulk was ~20 different structures each
costing 11–13 requests, one per layer, each tiny — `NegSPAdjacencyListBits` was
11 requests for **88 bytes total**, eight bytes per request. Those are control
words and index headers.

So the fix is to fetch each layer's small structures together. Measured on MinIO
at concurrency 4, toggled with `TDB_COALESCE_MAX_STRUCTURE_BYTES`:

| | coalescing off | coalescing on | |
|---|---|---|---|
| selective existence p50 | 53.1 ms | **23.5 ms** | 2.3× faster |
| requests per query | 350 | **87** | 4.0× fewer |
| bytes per query | 11.1 KiB | 36.7 KiB | 3.3× **more** |
| throughput under load | 65 q/s | **342 q/s** | 5.3× |
| query p50 under load | 61.0 ms | **11.2 ms** | 5.5× lower |
| object-store req/s | 19,459 | **9,382** | half the pressure |

The last two rows together are the point: **5.3× the throughput at half the
request rate**. The S3 ceiling moves from ~15 to **~63 disk-less queries per
second per prefix** (5,500 ÷ 87).

**This is a trade, not a free win.** Coalescing fetches whole small structures
where the uncoalesced path took only the bytes it needed, so byte transfer rises
2.9–3.3× — to *more* than reading the whole chain. That is the right trade
against S3, where request rate is capped per prefix and bandwidth is not, and
the wrong one where bytes are scarce. Hence the knob:
`TDB_COALESCE_MAX_STRUCTURE_BYTES=0` disables it, and the default is 8 KiB.

Note this invalidates the "a selective read moves ~8% of a full layer" claim
*when coalescing is on*. With it off, that claim still holds.

Still not implemented:

- **Prefix sharding** — S3's limit is per prefix, so distributing layer objects
  across N prefixes multiplies the ceiling by roughly N. Cheap, and independent
  of coalescing; the two multiply.

The materialized path pays 522 requests once and then serves from cache; the
disk-less path pays ~350 per query. That is the real trade, and it argues for
disk-less on large-working-set / low-QPS workloads rather than as a blanket
default.

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
