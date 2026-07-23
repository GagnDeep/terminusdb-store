# RFC: Object-storage backend for terminus-store

Status: **Draft (Milestone 0 — reconnaissance)**
Author: object-store backend work
Target crate: `terminus-store` (this repo), v0.21.5, edition 2018
Feature gate: `object-store` (default **off**)

## TL;DR (read this first)

Three findings dominate the design and one of them shrinks the work
substantially:

1. **Reads are NOT mmap-backed.** Every read path — old and new — pulls a whole
   file/archive into a heap `Bytes` via `read_exact`/`read_to_end`. There is *no*
   `mmap`/`memmap` anywhere in this crate or in `tdb-succinct`. The Milestone 0
   go/no-go question is answered **GO**: fetching whole layer archives over the
   network is exactly how the local backend already behaves against the page
   cache. (Evidence in §5.)

2. **The extension point is not `LayerStore` and not even `PersistentLayerStore`.**
   Since the v0.20 single-archive format, the modern store is
   `ArchiveLayerStore<M, D>`, already generic over two small backend traits:
   - `ArchiveBackend` (D) — 4 methods, moves layer *bytes*;
   - `ArchiveMetadataBackend` (M) — 8 methods, layer existence/size/parent/rollup.

   `DirectoryArchiveBackend` implements both against the filesystem.
   **An object store is a third implementation of these two traits — nothing
   else in the layer read/write path needs to change.** `LayerStore` and
   `PersistentLayerStore` are obtained *for free* via existing blanket impls.
   This is a much smaller and safer surface than "write a third `LayerStore`."

3. **The only POSIX dependency that actually matters is `fs2` advisory `flock`**
   in `src/storage/locking.rs`, used by the *label* store (and rollup writes) for
   compare-and-swap atomicity. Object storage has no flock; it has conditional
   PUT (ETag / version preconditions). Milestone 3 is where the real design risk
   lives, exactly as the prompt says.

Consequence for the milestone plan: Milestones 1–2 become "implement
`ObjectArchiveBackend: ArchiveBackend + ArchiveMetadataBackend`", Milestone 3 is a
new `ObjectLabelStore` using conditional PUT, Milestone 4 reuses the *existing*
`LruArchiveBackend` byte-cache plus the existing `LockingHashMapLayerCache`
object-cache and adds an optional disk-spill tier.

---

## 1. Core trait definitions (exact signatures + locations)

### 1.1 `FileLoad` / `FileStore` / `SyncableFile`

These are **re-exported** by this crate from the external `tdb-succinct` crate:
`src/storage/file.rs:6` does `pub use tdb_succinct::storage::{... FileLoad, FileStore, SyncableFile ...}`.
Definitions live in `tdb-succinct-0.1.2/src/storage/types.rs`:

```rust
// tdb-succinct: src/storage/types.rs:8
#[async_trait]
pub trait SyncableFile: AsyncWrite + Unpin + Send {
    async fn sync_all(self) -> io::Result<()>;
}

// :13
#[async_trait]
pub trait FileStore: Clone + Send + Sync {
    type Write: SyncableFile;
    async fn open_write(&self) -> io::Result<Self::Write>;
}

// :19
#[async_trait]
pub trait FileLoad: Clone + Send + Sync {
    type Read: AsyncRead + Unpin + Send;

    async fn exists(&self) -> io::Result<bool>;
    async fn size(&self) -> io::Result<usize>;
    async fn open_read(&self) -> io::Result<Self::Read> {   // provided
        self.open_read_from(0).await
    }
    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read>;
    async fn map(&self) -> io::Result<Bytes>;

    async fn map_if_exists(&self) -> io::Result<Option<Bytes>> {  // provided
        match self.exists().await? {
            false => Ok(None),
            true => Ok(Some(self.map().await?)),
        }
    }
}
```

Note `map() -> Bytes`: the contract is *materialize the whole file as owned
bytes*. There is no random-access / slice API in the trait itself.

### 1.2 `LayerStore` and `PersistentLayerStore`

`src/storage/layer.rs:73` — `LayerStore` is the big object-safe trait
(`Arc<dyn LayerStore>` is used everywhere via `CachedLayerStore`). ~45 methods:
`layers`, `get_layer_with_cache`, `get_layer_parent_name`, the dictionaries/counts
getters, `create_base_layer`, `create_child_layer_with_cache`, the rollup/squash
family, `triple_*` iterators, `retrieve_layer_stack_names[_upto]`, and the default
`layer_changes` walker. Signatures use `[u32; 5]` as the layer name (a 20-byte
content hash) throughout.

`src/storage/layer.rs:356` — `PersistentLayerStore`, the *filesystem-shaped*
sub-trait:

```rust
#[async_trait]
pub trait PersistentLayerStore: 'static + Send + Sync + Clone {
    type File: FileLoad + FileStore + Clone;
    async fn directories(&self) -> io::Result<Vec<[u32; 5]>>;
    async fn create_named_directory(&self, id: [u32; 5]) -> io::Result<[u32; 5]>;
    async fn create_directory(&self) -> io::Result<[u32; 5]> { /* random name */ }
    async fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool>;
    async fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File>;
    async fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool>;
    async fn finalize(&self, _directory: [u32; 5]) -> io::Result<()> { Ok(()) }
    // + many provided helpers: base_layer_files/child_layer_files build the
    //   ~48 per-structure File handles, read/write_parent_file, read/write_rollup_file, …
}
```

**Blanket impl** at `src/storage/layer.rs:1426`:

```rust
impl<F: 'static + FileLoad + FileStore + Clone, T: 'static + PersistentLayerStore<File = F>>
    LayerStore for T { ... }
```

So *anything* implementing `PersistentLayerStore` is a `LayerStore`. And
`ArchiveLayerStore<M, D>` (below) implements `PersistentLayerStore`. We inherit
both without writing a line of `LayerStore`.

### 1.3 `LabelStore`

`src/storage/label.rs:38`:

```rust
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Label { pub name: String, pub layer: Option<[u32; 5]>, pub version: u64 }
impl Label {
    pub fn with_updated_layer(&self, layer: Option<[u32; 5]>) -> Label { /* version+1 */ }
}

#[async_trait]
pub trait LabelStore: Send + Sync {
    async fn labels(&self) -> io::Result<Vec<Label>>;
    async fn create_label(&self, name: &str) -> io::Result<Label>;
    async fn get_label(&self, name: &str) -> io::Result<Option<Label>>;
    async fn set_label_option(&self, label: &Label, layer: Option<[u32; 5]>)
        -> io::Result<Option<Label>>;                 // Ok(None) == CAS lost
    async fn delete_label(&self, name: &str) -> io::Result<bool>;
    // provided: set_label / clear_label
}
```

The `version: u64` field is the crate's *own* optimistic-concurrency token:
`set_label_option` returns `Ok(None)` when the caller's `label` no longer matches
the stored one. This maps cleanly onto object-store conditional PUT (§3).

### 1.4 `LayerCache`

`src/storage/cache.rs:10`:

```rust
pub trait LayerCache: 'static + Send + Sync {
    fn get_layer_from_cache(&self, name: [u32; 5]) -> Option<Arc<InternalLayer>>;
    fn cache_layer(&self, layer: Arc<InternalLayer>);
    fn invalidate(&self, name: [u32; 5]);
}
```

This caches **deserialized `InternalLayer` objects**, not bytes. Implementations:
`NoCache` and `LockingHashMapLayerCache` (a `HashMap<[u32;5], Weak<InternalLayer>>`).
`CachedLayerStore` (`cache.rs:91`) wraps any `Arc<dyn LayerStore>` + `Arc<dyn LayerCache>`.
This is orthogonal to, and sits *above*, the byte-level `LruArchiveBackend` cache
(§4).

---

## 2. How the (modern) directory backend locates / reads / writes a layer

Two layer-store implementations coexist:

| Constructor (`src/store/mod.rs`) | Layer store | Format |
|---|---|---|
| `open_directory_store` (:952) | `DirectoryLayerStore` | **legacy**: one dir per layer, ~48 files |
| `open_archive_store` (:919) / `open_raw_archive_store` (:939) | `ArchiveLayerStore<M,D>` | **v0.20+**: one `.larch` archive file per layer |
| `open_memory_store` (:907) | `MemoryLayerStore` | in-RAM |

The prompt's "one layer = one object" premise matches the **archive** path, so
that is the model to mirror. (`DirectoryLayerStore` at `src/storage/directory.rs:21`
is the legacy per-file format; we leave it untouched.)

### 2.1 The archive backend traits (`src/storage/archive.rs`)

```rust
// :43
#[async_trait]
pub trait ArchiveBackend: Clone + Send + Sync {
    type Read: AsyncRead + Unpin + Send;
    async fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes>;
    async fn get_layer_structure_bytes(&self, id: [u32; 5], file_type: LayerFileEnum)
        -> io::Result<Option<Bytes>>;
    async fn store_layer_file(&self, id: [u32; 5], bytes: Bytes) -> io::Result<()>;
    async fn read_layer_structure_bytes_from(&self, id: [u32; 5],
        file_type: LayerFileEnum, read_from: usize) -> io::Result<Self::Read>;
}

// :61
#[async_trait]
pub trait ArchiveMetadataBackend: Clone + Send + Sync {
    async fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>>;
    async fn layer_exists(&self, id: [u32; 5]) -> io::Result<bool>;
    async fn layer_size(&self, id: [u32; 5]) -> io::Result<u64>;
    async fn layer_file_exists(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<bool>;
    async fn get_layer_structure_size(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<usize>;
    async fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>>;
    async fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()>;
    async fn get_parent(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>>;
}
```

`ArchiveLayerStore<M, D>` (`archive.rs:1328`) holds `metadata_backend: M`,
`data_backend: D`, and an in-process `construction` map
(`HashMap<[u32;5], HashMap<LayerFileEnum, ConstructionFile>>`) for layers being
built. Its `PersistentLayerStore for ArchiveLayerStore` impl (`archive.rs:1361`)
is where everything wires together.

### 2.2 Locate + open + read

- **Locate.** A layer name `[u32;5]` → 40-hex string (`name_to_string`,
  `layer.rs:1387`). `DirectoryArchiveBackend::path_for_layer` (`archive.rs:106`)
  is `<root>/<first 3 hex>/<40hex>.larch`. The 3-char prefix directory
  (`PREFIX_DIR_SIZE = 3`) shards the layers. For object storage this becomes an
  object **key**, e.g. `<prefix>/<3hex>/<40hex>.larch` (prefix sharding optional —
  object stores don't need it, but keeping it makes buckets diff-able against a
  local mirror).
- **Open a whole layer.** `get_layer_bytes` (`archive.rs:128`) opens the file,
  reads `metadata.size()` bytes with `read_to_end` into a `Vec<u8>` →
  `Bytes`. **Whole archive into heap.**
- **Open one structure.** The `.larch` archive begins with an
  `ArchiveFilePresenceHeader` (`u64` bitfield of which of the ~48 `LayerFileEnum`
  structures are present) + a `MonotonicLogArray` of cumulative offsets
  (`ArchiveHeader`, `archive.rs:876`). `get_layer_structure_bytes` (`archive.rs:146`)
  parses the header, computes the `Range` for the requested `LayerFileEnum`
  (`header.range_for`), seeks, and `read_exact`s just that slice. This is a
  *within-file* random read — it is NOT mmap, it is `seek`+`read_exact`.
- **`get_file` dispatch.** `ArchiveLayerStore::get_file` (`archive.rs:1412`)
  returns an `ArchiveLayerHandle` enum: `Construction` (in the build map),
  `Persistent(PersistentFileSlice)` (points at `(metadata, data, layer_id,
  file_type)` and lazily fetches on `map()`/`open_read`), or `Rollup`
  (special-cased, its own tiny file). `PersistentFileSlice::map` (`archive.rs:1067`)
  just calls `data_backend.get_layer_structure_bytes`.

### 2.3 Write + make visible

- Building a layer creates `ConstructionFile`s (in-RAM `BytesMut`, `archive.rs:654`)
  via `create_named_directory` (`archive.rs:1383`, inserts into `construction`) and
  `get_file`.
- `finalize(directory)` (`archive.rs:1468`) collects the finalized construction
  files, sorts by `LayerFileEnum`, builds the presence header + offset logarray,
  concatenates all structure bytes into one `data_buf`, and calls
  `data_backend.store_layer_file(directory, data_buf)` — **one archive, one
  write.**
- `DirectoryArchiveBackend::store_layer_file` (`archive.rs:167`): `create_dir_all`,
  open with `create(true).write(true)`, write the bytes, `flush` + `sync_all`,
  then on unix `sync_all` the *directory fd* to persist the dirent. **There is no
  temp-file-plus-rename**; because the name is a content hash the write is
  effectively idempotent and the file only becomes listable once created.

  → For object storage the equivalent is a single `put_opts(key, bytes,
  PutMode::Create)`. "Already exists" (412/`AlreadyExists`) is success, not error
  (idempotent content-addressed write; Milestone 2).

### 2.4 Walking the parent chain (read)

- Per-layer parent is stored *inside* the archive as the `LayerFileEnum::Parent`
  structure (40 hex bytes). `get_parent` (`archive.rs:342`) reads that slice.
- `LayerStore::get_layer_parent_name` → (via blanket impl) `layer_parent` →
  `read_parent_file`; `ArchiveLayerStore` overrides `layer_parent` (`archive.rs:1506`)
  to call `metadata_backend.get_parent`.
- The stack walk itself: `get_layer_with_cache` (`layer.rs:1433`) loops
  `layer_has_rollup`/`read_rollup_file`/`layer_parent`, fetching each ancestor
  until it hits a cached one. The `walk_backwards_from_disk!` macro (`layer.rs:36`)
  and `retrieve_layer_stack_names` do the same. **Each ancestor is one
  `get_parent` round-trip** → over a network this is N sequential GETs for a
  depth-N stack (see Risks §8). Currently sequential; ancestor prefetch is a
  noted future optimization, out of scope this pass.

---

## 3. Labels: storage, mutation, atomicity

Modern stores pair the archive layer store with **`DirectoryLabelStore`**
(`src/storage/directory.rs:118`), *not* an archive-specific label store.

- **Storage.** One file `<root>/<name>.label` per label, two lines: `version\n` +
  `40-hex-layer-or-empty\n` (`get_label_from_data`, `directory.rs:128`).
- **List / read.** `labels()` = `read_dir` + parse each `*.label`. `get_label` =
  read one file.
- **Create.** `create_label` (`directory.rs:222`) writes `"0\n\n"` iff the file
  does not exist; existing → `InvalidInput` "database already exists".
- **Mutate (the CAS).** `set_label_option` (`directory.rs:258`):
  1. `new_label = label.with_updated_layer(layer)` (version bumps by 1);
  2. open the file **exclusive-locked** (`ExclusiveLockedFile`, `fs2` `flock`);
  3. re-read stored label; if `retrieved_label == *label` (caller's snapshot still
     current) → truncate + write new contents + `sync_all` → `Ok(Some(new_label))`;
  4. else → `Ok(None)` (**CAS lost**, caller retries).
- **Delete.** `remove_file`, unlocked by design (`directory.rs:286` comment: a
  racing read/write is indistinguishable from reorder).

**Atomicity guarantee relied upon:** advisory `flock` (`fs2`) around the
read-compare-write, on a single shared filesystem. `CachedDirectoryLabelStore`
(`directory.rs:317`) is a lock-free in-memory variant valid only when a single
process owns the files.

**This is the one guarantee object storage cannot provide the same way.** Replace
`flock`-guarded RMW with **conditional PUT keyed on the object's version/ETag**:
read `(Label, ETag)`, compute `new_label`, `put_opts(key, new, PutMode::Update(ETag))`.
`412 Precondition Failed` → return `Ok(None)` (lost CAS), matching directory
semantics. `create_label` → `PutMode::Create`. Details in Milestone 3 below.

---

## 4. Caching tiers (what already exists vs. what Milestone 4 adds)

Two independent caches already exist:

1. **Object cache** — `LockingHashMapLayerCache` (`LayerCache`), caches
   `Arc<InternalLayer>` by weak ref. Wired by `CachedLayerStore`. **Reused as-is.**
2. **Byte cache** — `LruArchiveBackend<M, D>` (`archive.rs:356`): an LRU of whole
   layer archives (`Bytes`) with a MiB budget, plus single-flight de-dup of
   concurrent fetches (the `Resolving`/`Resolved` `CacheEntry`). It *wraps* an
   inner `M`+`D` and itself implements both `ArchiveBackend` and
   `ArchiveMetadataBackend`. `open_archive_store` already stacks it over
   `DirectoryArchiveBackend`.

   → **Milestone 4 reuses `LruArchiveBackend` unchanged** by stacking it over the
   new object backend, and adds an *optional local-disk spill* tier
   (keyed by `<40hex>.larch`) as a second `ArchiveBackend` behind the object one,
   plus a `tracing` span / counters for hit-rate and bytes-fetched. Immutability
   means entries are eviction-only, never invalidated.

---

## 5. The mmap question (Milestone 0 Q5) — decisive

**Reads are fully materialized into heap memory; nothing is mmap-backed.**

- `grep -rn 'mmap|memmap|Mmap' src/` → **no matches**. `Cargo.toml` has no
  memmap dependency.
- Legacy `FileBackedStore::map` (`tdb-succinct/src/storage/file.rs:62`): allocates
  a `BytesMut` of `size`, `read_exact`s the whole file, `freeze()`s. Heap, not
  mmap.
- Archive `get_layer_bytes` (`archive.rs:128`): `read_to_end` into `Vec` → `Bytes`.
- Every succinct structure is built from an owned `Bytes` handed in by `map()`;
  they slice/refcount that buffer but never touch a file descriptor afterward.

Therefore fetching an entire layer archive with a single object GET and handing
the resulting `Bytes` to the exact same construction code is *behaviorally
identical* to the local backend (which already copies the whole thing off the
page cache). **GO.** The one nuance — `get_layer_structure_bytes` currently does a
within-archive `seek`+`read_exact` to avoid materializing the whole archive for a
single structure — is an optimization we can honor over object storage with a
ranged GET, but even the naive "GET whole archive, slice in memory" is correct and
is precisely what `LruArchiveBackend` already does when a layer fits its budget
(`archive.rs:529`).

---

## 6. Exhaustive inventory of POSIX / filesystem assumptions

Scope = everything that must be re-expressed (or deliberately bypassed) for object
storage. Grep-verified.

| Assumption | Where | Object-store treatment |
|---|---|---|
| **Advisory `flock`** (`fs2`) | `src/storage/locking.rs` (`lock_exclusive`/`lock_shared`/`try_lock_*`); used by `DirectoryLabelStore` CAS and `DirectoryArchiveBackend::{get,set}_rollup` (`archive.rs:304,327`) | **Replaced** by conditional PUT / ETag CAS (labels) and PutMode::Create idempotency (layers). Rollup writes become conditional puts of a small `<40hex>.rollup.hex` object. |
| **Directory listing** `read_dir` | `directory.rs` (`directories`, `labels`), `archive.rs:225` (`get_layer_names`) | Object **LIST** with prefix + suffix filter (`.larch` / `.label`). |
| **`create_dir_all` / dir-fd `sync_all`** | `archive.rs:171,192`; `directory.rs:59,105`; unix dirent durability | **No-op** — object stores have no directories; PUT is atomic + durable on ack. |
| **`metadata().size()` / `MetadataExt`** | `archive.rs:18-20,138,262` | Object **HEAD** → `ObjectMeta.size`. |
| **`metadata()` existence check** | `archive.rs:245,267`; `directory.rs:70,92` | HEAD → NotFound handling. |
| **`sync_all` / `flush` on write** | `archive.rs:183`; `directory.rs`; parent/rollup writers | Implicit — PUT is durable on 200/OK. |
| **`fs::rename` / temp+persist** | *none found* (`grep rename` → 0 hits) | N/A. Content-addressed writes never rename. |
| **`mmap`** | *none* | N/A (see §5). |
| **Path manipulation** (`PathBuf`, `push`, prefix-dir) | `archive.rs:106-122`; `directory.rs` | String **key** construction; `/` joined. |
| **`tempfile` for merges** | `LayerStore::merge_base_layer(temp_dir: &Path)` (`layer.rs:224`); `open_archive_store` uses filesystem temp | Merge still uses a local temp dir for scratch — **out of scope**, works regardless of backend (it produces bytes that are then `store_layer_file`'d). Documented, not changed. |
| **`std::fs`/`tokio::fs`** direct calls | `archive.rs`, `directory.rs`, `memory.rs`, `new_memory.rs`, `pack.rs`, `locking.rs`, `layer.rs` | Only the `archive.rs`+`directory.rs` occurrences are in the storage backends we replace; the rest are legacy/memory/pack paths untouched by this feature. |

The load-bearing item is row 1 (flock → conditional PUT). Everything else is a
mechanical LIST/HEAD/GET/PUT translation.

---

## 7. Proposed module layout & public API

All new code under `#[cfg(feature = "object-store")]`, default off.

```
src/storage/
  object.rs        # new: ObjectArchiveBackend (ArchiveBackend + ArchiveMetadataBackend)
                   #      ObjectLabelStore (LabelStore, conditional-PUT CAS)
                   #      ObjectStoreConfig / URI parsing / error types
  object_cache.rs  # new (Milestone 4): optional local-disk spill ArchiveBackend +
                   #      tracing spans / hit-rate + bytes-fetched counters
```

`Cargo.toml`:

```toml
[dependencies]
object_store = { version = "0.11", optional = true, features = ["aws", "gcp", "azure"] }
# (in-memory + local backends are always in object_store; no extra feature needed)

[features]
object-store = ["dep:object_store"]
```

(Only `object_store` is added, per the working rules. Exact minor version pinned
once `cargo` resolves it; `aws`/`gcp`/`azure` features enable the cloud stores,
R2/MinIO reached via the S3 store's endpoint override.)

Public API (mirrors the existing `open_*` functions in `src/store/mod.rs`):

```rust
#[cfg(feature = "object-store")]
pub fn open_object_store(url: &str, options: /* endpoint/creds */, cache_size: usize)
    -> io::Result<Store>;
// builds: ObjectArchiveBackend --wrapped in--> LruArchiveBackend
//         --> ArchiveLayerStore --> CachedLayerStore(LockingHashMapLayerCache)
//         + ObjectLabelStore
```

Internally the layer store is literally
`ArchiveLayerStore::new(lru.clone(), lru)` where `lru` wraps
`ObjectArchiveBackend` — **identical shape to `open_archive_store`**, only the
leaf backend and the label store differ.

`ObjectArchiveBackend` holds an `Arc<dyn object_store::ObjectStore>` + a key
prefix + a small runtime handle for the LIST/HEAD/GET/PUT translations in §6.
Ranged reads (`get_layer_structure_bytes`, `read_layer_structure_bytes_from`) use
`ObjectStore::get_opts` with a byte `Range` after fetching/parsing the archive
header (or fall back to whole-object GET + in-memory slice; both correct).

### URI grammar (Milestone 5, documented now)

`object_store::parse_url_opts` handles `s3://bucket/prefix`, `gs://…`,
`az://…`, `file:///…`, `memory://`. Endpoint override (R2/MinIO/LocalStack) and
credentials pass through `object_store`'s option map
(`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
`AWS_ALLOW_HTTP=true` for http endpoints). No hand-rolled S3 client.

---

## 8. Known risks (tracked, per prompt)

- **Layer-stack depth → N sequential GETs.** The parent walk (`get_parent` per
  ancestor, §2.4) is sequential today. Over a network, latency compounds with
  history depth. *Opportunity noted* at `get_layer_with_cache`
  (`layer.rs:1433`) and `retrieve_layer_stack_names`: ancestor names could be
  prefetched in parallel once known. **Not built this pass** (no rollup policy
  change). The existing rollup/squash machinery is the intended mitigation and is
  unaffected.
- **Write amplification / RAM.** Layer construction already buffers the whole
  archive in RAM (`ConstructionFile` `BytesMut`, then one `data_buf`); the object
  backend PUTs that same buffer. No change to the memory profile.
- **This does not make the DB bigger than RAM.** Working set is still fully
  materialized and expanded in memory on read (§5). The wins are durability, cheap
  storage, stateless replicas, and trivial backup — **not** unbounded scale. Docs
  and commit messages will not claim otherwise.

---

## 9. Verification posture (all milestones)

`cargo test`, `cargo clippy --all-targets`, `cargo fmt --check` must pass with the
feature **both on and off** (`--features object-store` and default). Milestones
1–4 develop against `object_store`'s `memory://` and `file://` backends → **no
network**. Milestone 5 adds `#[ignore]`d MinIO/LocalStack integration tests gated
by an env var (no such optional-test pattern exists in the repo yet — this
introduces one) and a docker-compose service for CI/local.

---

## 10. Deviation from the prompt's framing (flagged, per instructions)

The prompt says "Implement `ObjectLayerStore` satisfying `LayerStore`". The code
says the right seam is one level lower: implement `ArchiveBackend` +
`ArchiveMetadataBackend` and reuse `ArchiveLayerStore`/`LruArchiveBackend`/the
blanket `LayerStore` impl. This is strictly less code, touches no existing
behavior, and keeps `ObjectLayerStore` as a thin type alias
(`ArchiveLayerStore<LruArchiveBackend<ObjectArchiveBackend, …>, …>`) if a named
type is wanted. Every prompt premise (§ "Why this is plausible") checks out:
trait-abstracted ✓, single-archive-per-layer ✓, content-addressed/immutable ✓,
tokio/async ✓. Recommend proceeding on the trait-pair seam.
```
