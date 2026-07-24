# Object-store backend: read latency, RAM, and write amortization

This branch makes the object-storage backend **purpose-built to minimize RAM and
S3 read/write latency at scale**, comparable to a cloud-native LSM engine, while
preserving the properties that make terminus-store valuable for audits:
**immutability** and **per-commit traceability**. All changes are additive and
feature-gated where they touch object code; the default path and
`DirectoryLayerStore` behaviour are unchanged and every pre-existing test stays
green.

## Results (12-layer cold read, 5 ms simulated RTT)

| Stage | Cold read |
|---|---:|
| Baseline (sequential whole-chain walk) | 380 ms |
| + persisted stack manifest (parallel archive warm) | 171 ms |
| + rollup-pointer cache (parallel rollup warm) | 96 ms |
| + release cache lock before origin fallback | **29 ms** |

**~13×**, validated end-to-end against a live MinIO. Warm (in-process object
cache) reads are ~10 µs.

## What changed

### Read latency
- **Stack manifest** (`src/storage/stack_manifest.rs`): a `.stack` sidecar object
  records a layer's full ancestor id list, written best-effort on finalize. On
  read, one GET learns the whole chain and warms every archive in one parallel
  prefetch wave before the (unchanged) discovery/build walk runs — so that walk
  hits a warm cache. Hint-only: any absence/parse/validation failure falls back
  to the authoritative parent walk, and content-addressed immutability makes a
  valid manifest permanently correct.
- **Parallel prefetch** hooks on `PersistentLayerStore`/`ArchiveBackend`,
  implemented (bounded concurrency) on the LRU and disk-spill tiers.
- **Rollup-pointer cache** on `LruArchiveBackend` + parallel `prefetch_rollups`,
  eliminating the per-ancestor sequential `.rollup.hex` probe. Safe because
  rollup layers are immutable and `set_rollup` keeps the cache consistent.
- **Lock-scope fix**: the LRU metadata methods held the cache mutex across the
  fallback origin `.await`, serializing concurrent lookups. Releasing the guard
  first makes prefetch genuinely parallel (the 96 → 29 ms step).

### RAM (larger-than-RAM)
- **mmap-backed reads**: both the disk-spill tier and the default
  `DirectoryArchiveBackend` memory-map immutable `.larch` archives via
  `Bytes::from_owner`, so resident set collapses to the pages a query touches.
  Combined with the local disk-spill tier this is a hybrid RAM+NVMe buffer pool:
  a graph larger than the in-memory cache reads from local NVMe through the OS
  page cache. Type-preserving (`Bytes` in/out) — no query-code changes.

### Read depth
- **Automatic compaction** (`src/store/compaction.rs`, `Store::spawn_compaction`):
  a background task rolls up any head whose effective stack exceeds a threshold.
  **Rollup-only, never squash** — every original layer is retained and the parent
  chain stays walkable, so history and immutability are preserved.

### Write amortization (opt-in)
- **Group commit** (`src/store/buffered.rs`): `BufferedNamedGraph` batches many
  logical commits into one physical layer (one PUT + one label CAS per batch)
  with an in-RAM `OverlayLayer` (`src/layer/overlay.rs`) providing
  read-your-writes. Property-tested to answer string-level queries identically to
  a real committed child.
- **Write-ahead log** (`src/store/wal.rs`): `open_with_wal` makes each buffered op
  durable before it is acknowledged (`DurabilityMode::PerCommitFsync` — no
  acknowledged-commit loss). Torn-tail-tolerant framing; recovery replays
  un-flushed ops onto the current head and never double-applies a
  flushed-but-not-checkpointed batch (records carry their base).

## Audit safety

Immutability is never traded: layers stay content-addressed and append-only, the
overlay/WAL are never persisted as layers, and compaction is rollup-only.
Per-commit history is preserved on the strict default path; group commit is
opt-in per label, so audit labels keep full per-commit immutable history. The
per-commit layer chain remains a tamper-evident audit trail.

## Validation

`cargo test` and `cargo test --features object-store` pass (both feature states),
`cargo fmt --check` and `cargo clippy --lib --tests` clean on new code. Four
`#[ignore]`d MinIO integration tests were run green against a live MinIO,
including a deep-chain manifest read and group commit over real S3.

> Note: `cargo clippy --all-targets` fails at the benches on stable
> (`#![feature(test)]` needs nightly) — pre-existing and unrelated; verification
> runs clippy over `--lib --tests`.
