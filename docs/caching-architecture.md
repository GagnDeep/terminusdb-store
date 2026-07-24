# Caching architecture

This describes how the object-store backend caches data, why the caches need no
invalidation logic, and what that means for latency, throughput, and scaling. It
is the reference for the "warm reads barely touch the object store" behaviour in
[`benchmarking.md`](benchmarking.md).

## The one principle everything rests on

**Layers are immutable and content-addressed.** A layer's name is a hash of its
contents, so an object named `abc….larch` can never change — a different content
would have a different name. Every byte range of every layer is therefore valid
*forever* once fetched.

That single fact removes the hard part of caching. There is no invalidation, no
TTL, no staleness window, no cache-coherence protocol. A cached entry is correct
by construction until it is evicted for space. Every cache below is a plain LRU
with no expiry.

**The one exception is labels.** A label (a named graph's head pointer) is the
only mutable state in the system, and it is **never cached** — every read goes
to the object store and returns the current ETag, which is the token the
compare-and-swap uses to update the head safely. Caching a label would break
that. So: immutable layer data is cached aggressively; the mutable head pointer
is always read fresh.

## Where the caches live

Everything is **in-process RAM inside the replica**. There is no shared cache
and no external cache service. In particular this is *not* a CDN: the backend
talks to the object store's authenticated S3 API, which is not served from any
edge cache. The speed of a warm read is the speed of the replica's own memory.

A store opened with [`open_object_store`](../src/store/mod.rs) stacks four layers,
each owning its own caches:

```
CachedLayerStore              materialized-layer cache, per-layer count cache
  └─ ArchiveLayerStore        (assembles layers from structures; no cache of its own)
       └─ LruArchiveBackend   whole-layer archive cache, rollup-pointer cache
            └─ ObjectArchiveBackend   header / region / manifest caches, single-flight locks
                 └─ object store (R2 / S3 / MinIO)   durable cold storage
```

A read looks for what it needs at the top and descends only on a miss, so a warm
query is answered from the first tier that has it and never reaches the object
store.

## The caches, tier by tier

Sizes are the current defaults; all are plain LRUs keyed by the immutable layer
id unless noted.

### `ObjectArchiveBackend` — the object-store leaf (`src/storage/object.rs`)

This is where disk-less reads spend their time, because it is the tier that turns
a query into ranged GETs. Its caches are what make the disk-less path — which
deliberately does *not* hold whole layers in RAM — nonetheless cheap when warm.

| cache | holds | size | purpose |
|---|---|---|---|
| `header_cache` | parsed archive header + data offset | 4096 layers | the header maps a structure name to its byte range; one probe per layer serves every structure read of it |
| `region_cache` | coalesced small-structure spans (`Vec<(range, bytes)>`) | 1024 layers | the small per-layer index structures, fetched together in one ranged GET and sliced from thereafter (see `benchmarking.md`) |
| `manifest_cache` | the `.stack` manifest (or its absence) | 4096 layers | the whole ancestor chain in one object, so chain discovery is one request instead of a parent-pointer walk |
| `header_locks`, `region_locks` | a per-layer async mutex | 4096 each | **single-flight**: when many concurrent reads first touch a cold layer, one fetches and the rest wait, instead of a thundering herd of identical GETs |

The absence of a manifest is cached too, so a layer without one is not re-probed
on every query.

Coalescing is a per-backend setting (`ObjectArchiveBackend::without_coalescing`,
or `TDB_COALESCE_MAX_STRUCTURE_BYTES`); see `benchmarking.md` for the trade it
makes (fewer requests, more bytes).

### `LruArchiveBackend` — whole-layer tier (`src/storage/archive.rs`)

| cache | holds | size | purpose |
|---|---|---|---|
| `cache` | whole layer archives (`Bytes`) | byte-bounded by the `cache_size` argument | the materialized path's working set; the disk-less path opens with `cache_size = 0`, so this tier is empty and reads fall through to ranged GETs |
| `rollup_cache` | rollup pointer (`Some(name)` / `None`) | 100 000 layers | lets a read resolve the whole chain's rollups in one parallel wave; `prefetch_rollups` warms it from a manifest |

`cache_size = 0` is what "disk-less" means: no whole layer is ever resident, and
reads are served block-by-block through the leaf tier's caches above. A non-zero
size gives a hybrid — hot layers whole in RAM, everything else block-lazy.

### `CachedLayerStore` — top tier (`src/storage/cache.rs`)

| cache | holds | size | purpose |
|---|---|---|---|
| `cache` (`LockingHashMapLayerCache`) | assembled `InternalLayer`s, via `Weak` references | unbounded, self-pruning | a fully-parsed layer ready to query; `Weak` so a layer is dropped once nothing holds it, with a periodic sweep of dead entries |
| `counts` | per-layer node / predicate / value counts | 16 384 entries | these are immutable and read on every id-resolution to compute chain offsets; caching them removes a metadata round trip per layer per query |

### Block-lazy readers — transient (`src/storage/block_lazy.rs`)

`BlockLazyStringDict`, `BlockLazyTypedDict` and `BlockLazyLogArray` each hold a
small block/word cache (256 blocks) for the duration of a single dictionary
lookup, so the `O(log n)` block touches of one binary search do not re-fetch. The
longer-lived caching across queries is the region cache above.

### Optional disk tier — `DiskSpillArchiveBackend` (`src/storage/object_cache.rs`)

`open_object_store_with_cache` inserts a local-disk cache of whole archives
between the in-memory LRU and the object store: RAM → local disk → object store.
A warm replica then serves layers from local disk without a network round trip,
and a cold replica populates that disk cache as it reads. Because layers are
immutable, disk entries never need invalidation either. This tier is opt-in and
only relevant where the replica has local disk to spare.

## A read, warm and cold

**Cold** (a freshly started replica, nothing resident). A selective query on a
rolled-up graph:

1. read the label — always fresh — to learn the head (1 request),
2. resolve the rollup pointer and the manifest — 1 request, then cached,
3. fetch the head layer's coalesced index region — 1 request, then cached.

That is the measured **3 requests** cold. Every fetched span is now resident.

**Warm** (the same or a related query on the same replica). The header, region,
manifest and counts are all in RAM, so resolution and existence run entirely in
memory. The label is still read fresh, but that is a single small GET, and much
of a query class needs no new layer data at all. This is why a load test
measured thousands of queries per second while the object store saw only tens of
requests per second: **the queries never left the replica.**

## What is deliberately *not* cached

- **Labels / graph heads.** Mutable; always read fresh so the compare-and-swap
  is correct. This is the only per-query object-store request a fully warm read
  makes.
- **Anything, when correctness would require invalidation.** There is none to
  cache — the design has exactly one mutable thing, and it is the label.

## Why this shapes the deployment

- **Per-replica, independent.** Each process warms its own caches. A fresh
  replica starts cold and warms as it serves; N replicas keep N independent warm
  sets. There is no shared cache to contend on.
- **Reads scale horizontally.** Because warm reads are RAM-bound and barely
  touch the object store, throughput is limited by the replica's CPU, not by the
  bucket. Add replicas to multiply read throughput; the object store is nowhere
  near a request-rate limit.
- **Backend-agnostic.** The warm behaviour is identical on R2, S3, or MinIO,
  because the cache is the replica's own RAM and its correctness comes from
  immutability, not from any backend or CDN feature.
- **Compaction keeps it cheap.** Read cost is ~2 object-store requests per layer
  cold; background rollup (see `DISKLESS_STORAGE_INTEGRATION.md` in the
  TerminusDB integration, and `Store::spawn_compaction`) collapses depth so a
  maintained graph stays at a handful of requests regardless of history length.

## Files

- `src/storage/object.rs` — `ObjectArchiveBackend` (header / region / manifest
  caches, single-flight), `ObjectLabelStore` (uncached, ETag CAS).
- `src/storage/archive.rs` — `LruArchiveBackend` (whole-layer cache, rollup
  cache, prefetch).
- `src/storage/cache.rs` — `CachedLayerStore`, `LockingHashMapLayerCache`, count
  cache.
- `src/storage/block_lazy.rs` — per-lookup block/word caches.
- `src/storage/object_cache.rs` — optional `DiskSpillArchiveBackend`.
- `src/store/mod.rs` — `open_object_store` (how the tiers stack).
