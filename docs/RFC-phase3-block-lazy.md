# RFC: Phase 3 — disk-less, fetch-only-what-you-touch reads (matching NamiDB)

Status: **Design (staged). Stage 0 landed; Stages 1–3 gated on approval + measurement.**
Goal: on a **disk-less** replica (no local NVMe), read a graph while transferring
and holding in RAM only the bytes a query actually touches — the property NamiDB
gets from columnar SSTs + ranged reads. Today, without local disk, we fetch and
materialize **whole layer archives**.

This is deliberately staged so the working, tested engine is never left broken and
each stage is independently shippable, feature-gated, and measurable.

## Where we are (the two facts that shape everything)

1. **Ranged reads already work — at *structure* granularity.** The object backend
   already fetches a single structure of a layer with a bounded GET
   (`ObjectArchiveBackend::get_layer_structure_bytes`: an ~8 KiB header probe, then
   a ranged GET of just that structure). The plumbing exists end-to-end
   (`FileLoad::open_read_from` → `read_layer_structure_bytes_from`). Stage 0 (below)
   measures this: reading one structure transfers a fraction of the whole archive.
2. **But layer construction loads *everything*, synchronously.** Building an
   `InternalLayer` calls `map_all` (`src/storage/file.rs:118,202`), which `.map()`s
   **all ~48 structures** into resident `Bytes` (`base.rs:52`, `child.rs:64`), and
   the query accessors (`src/layer/internal/mod.rs:69-219`) return borrowed `&T`
   synchronously. So even the big-layer path that uses ranged reads still fetches
   and holds every structure.

## The core obstacle (why it is not a small change)

- **Sync query interface vs async fetch.** ~40 accessors are `&self`-sync and the
  iterators `.clone()` their results. `OnceCell::get_or_init` is sync — you cannot
  `.await` a fetch inside it. Fetch-on-demand therefore needs either an **async
  query path** (huge ripple) or an **async "prepare" step** that fetches what a
  query needs *before* entering sync code.
- **Whole-data-resident structures.** `SizedDict` (tdb-succinct `tfc/dict.rs:96`)
  stores `data: Bytes` whole; `from_parts` takes the whole buffer; `block_bytes`
  slices it. There is no block-source seam — adding one means **forking/extending
  tdb-succinct** (or reimplementing its block codec).
- **Latency needs batching + caching.** A dictionary `id()` lookup binary-searches
  block heads (`tfc/dict.rs:173`) — O(log n) block touches. Naive per-block S3
  fetches would be sequential round trips. NamiDB avoids this by fetching a row
  group's column pages in one ranged GET (~50–100 KB, 3–4 round trips) and caching.
  We must do the same: coalesce and cache block ranges.
- **Count residency.** `node_and_value_count`/`predicate_count`
  (`mod.rs:546`) sum dictionary `num_entries` up the whole chain, and id-remapping
  relies on them; `num_entries` needs the offsets plus the last block. Any lazy dict
  must keep offsets + a cached count resident.

## Recommended architecture

Keep the query accessors **synchronous**; add an **async prepare step** that fetches
(batched, cached) exactly what a query will touch, after which sync code runs over a
resident block cache. This mirrors NamiDB's "fetch the pages, then decode locally"
shape and avoids turning the entire query engine async.

Two new pieces:
- **`BlockSource` + a batched, cached ranged reader** (Stage 0/2): fetch arbitrary
  byte ranges of a layer structure, coalescing adjacent ranges into one GET, with an
  LRU cache keyed by `(layer, structure, block)`. This is the disk-less analogue of
  a page cache, and the thing that keeps latency sane.
- **Lazy `InternalLayer`** (Stage 1): per-structure cells filled by `prepare`, so a
  query loads only the structures it needs (existence/subject scans never touch the
  object index or wavelet tree, etc. — the query "fingerprints" are known,
  `mod.rs:257-431`).

## Staged plan

### Stage 0 — evidence + foundation (LANDED, safe, no live-path change)
- A byte-counting test proving structure-granular ranged reads already transfer a
  fraction of the whole layer (`src/storage/object.rs`,
  `ranged_structure_read_transfers_less_than_whole_layer`). This guards the ranged
  path from regressing and quantifies the starting point.
- No change to the query path; zero regression risk.

### Stage 1 — selective, chain-walking query methods (LANDED for existence + s/p/sp)
Rather than the invasive `InternalLayer`-laziness rework, Stage 1 landed as
**new `Store` methods that walk the chain using the existing per-layer selective
primitives** — no rewrite of the ~40 accessors, and the eager `get_layer` path is
untouched. Each is differential-tested against the fully-materialized layer.
- **1a — `selective_id_triple_exists`** (done): adjacency-only existence walk;
  transfers ~31% of a full read.
- **1b — `selective_value_triple_exists`** (done): resolves strings→ids via
  per-layer dictionaries + id-maps (replicating `InternalLayer`'s resolution
  exactly, incl. the `+node_dict_len` value shift and cumulative offsets),
  differential-tested over nodes, predicates, and typed values; ~78% of a full
  read. Also added a **per-layer archive-header cache** (headers are immutable),
  which cut a full `get_layer`'s transfer 268 KB → 47 KB by sharing one header
  probe across a layer's many structure reads.
- **1c — `selective_id_triples_s/_sp/_p/_o`** (done): disk-less traversal by
  subject, predicate, and object, reconciled head-first; differential-tested
  across all four directions. (`_o` needs an exact-object filter because the
  cached per-layer iterator seeks to the nearest object rather than filtering.)
- **Deferred:** the deeper `InternalLayer`-laziness rework (only needed if
  arbitrary whole-layer materialization must also shrink; the selective methods
  above cover the common disk-less query classes — existence and traversal).

**Stage 1 is complete.** Disk-less existence and traversal queries no longer
materialize whole layers.

### Stage 2 — block-lazy dictionary (biggest RAM component) — MECHANISM LANDED, no fork
Landed as a **terminus-side wrapper over tdb-succinct's public block codec**, so the
no-fork decision held — `SizedDictBlock::{parse,entry,id,num_entries}` and
`MonotonicLogArray` are all public, so no change to the external crate was needed.
- **2a — `id -> string`** (done): `BlockLazyStringDict` (`src/storage/block_lazy.rs`)
  keeps only the offset table + data-section size resident and fetches the single
  block holding an id via a ranged read (`get_layer_structure_range`). Differential-
  tested against the fully-loaded dictionary over a 500-entry, multi-block dict.
- **2b — `string -> id`** (done): a binary search over block heads
  (`get_block(mid).entry(0)`), mirroring `SizedDict::id`, touching only the O(log n)
  blocks the search visits plus the found block. Differential-tested (matches, round-
  trips, and absent strings) against the full dictionary.
- **Block cache** (done): a 256-entry LRU of raw block bytes keyed by block index, so
  the binary search and repeated lookups never re-fetch a block. This is the
  per-dictionary analogue of the coalescing block reader the design called for.
- **Payoff:** dictionaries — usually the largest resident part — are now
  fetch-on-touch in both directions, with only the offset table resident, **without
  forking tdb-succinct**.
- **2c — wire into the live selective path** (remaining): replace the whole-dictionary
  loads in `Store::selective_value_triple_exists` (`get_node_dictionary` etc.) with
  `BlockLazyStringDict` lookups. Requires plumbing `get_layer_structure_range` up
  through the `LayerStore` seam (today it lives on `ArchiveBackend`); per the
  measure-first posture, justify with a byte/RSS measurement first (Stage 1b's
  string-selective path already transfers ~78% — the dictionaries are what remains to
  shrink there).

### Stage 3 — block-lazy rank/select (hardest, likely partial)
- `BitIndex`/`AdjacencyList`/`WaveletTree` do random indexing + data-dependent
  binary search over resident buffers. Options: keep the small index logarrays
  (`blocks`/`sblocks`) resident and block-fetch only the bit payload; or a deeper
  redesign. Expect a partial win here; measure whether it is worth it after Stages
  1–2.

## Recommendation

Do **Stage 0 (done) + Stage 1** first: they deliver a real disk-less reduction for
the common query classes **without forking tdb-succinct**, and Stage 1 keeps the
query accessors synchronous via the prepare step. Then **measure** on a genuinely
disk-less object-`memory://` deployment; only if dictionary residency is still the
ceiling do we take on **Stage 2** (the fork), and **Stage 3** only if measurements
justify it. This matches the "measure-first" posture and stages the risk so the
engine stays green throughout.

## What full parity looks like vs. what we keep

At the end of Stages 1–3, a disk-less replica fetches only the structures (and,
within dictionaries, the blocks) a query touches — matching NamiDB's byte and RAM
profile for selective queries with no local disk. We retain our differentiators
throughout: immutable content-addressed layers (so every cache and range is safe by
construction), no background compaction rewrites, and a per-commit tamper-evident
audit trail.
