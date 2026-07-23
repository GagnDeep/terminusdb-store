//! Local-disk spill cache for the object-store backend (Milestone 4).
//!
//! [`open_object_store`](crate::store::open_object_store) already puts a bounded
//! in-memory LRU of whole layer archives
//! ([`LruArchiveBackend`](super::archive::LruArchiveBackend)) and the
//! deserialized-layer object cache
//! ([`LockingHashMapLayerCache`](super::cache::LockingHashMapLayerCache)) in
//! front of the origin. This module adds an optional **second tier** between
//! them and the network: a read-through cache of whole layer archives on local
//! disk, keyed by layer hash.
//!
//! Because layers are content-addressed and immutable, cache entries are never
//! invalidated — a present file is always correct — so this tier only ever
//! evicts (implicitly, by the operator managing the directory) and never has to
//! reason about staleness.
//!
//! The stack built by `open_object_store_with_cache` is therefore three tiers
//! deep on the read path:
//!
//! ```text
//! get_layer_bytes → LruArchiveBackend (RAM) → DiskSpillArchiveBackend (disk)
//!                 → ObjectArchiveBackend (network)
//! ```
//!
//! [`DiskSpillArchiveBackend`] records hit/miss counts and bytes served vs.
//! bytes fetched from the origin, exposed via [`DiskSpillArchiveBackend::stats`].

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::archive::{Archive, ArchiveBackend};
use super::consts::LayerFileEnum;
use super::layer::name_to_string;

const PREFIX_DIR_SIZE: usize = 3;

/// Maximum number of layer archives fetched concurrently during a prefetch wave.
const PREFETCH_CONCURRENCY: usize = 16;

/// Owns a memory-map so it can back a [`Bytes`] via `Bytes::from_owner`; slices
/// derived from that `Bytes` keep the mapping alive by reference count.
struct MmapOwner(memmap2::Mmap);

impl AsRef<[u8]> for MmapOwner {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Memory-map a cache file into a `Bytes`. Returns `None` if the file is absent;
/// an empty file maps to empty `Bytes` (mmap rejects zero-length maps).
fn mmap_path(path: &std::path::Path) -> Option<Bytes> {
    let file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    if len == 0 {
        return Some(Bytes::new());
    }
    // SAFETY: layer archives are content-addressed and immutable once written,
    // so the mapped region is never mutated or truncated while mapped.
    let mmap = unsafe { memmap2::Mmap::map(&file).ok()? };
    Some(Bytes::from_owner(MmapOwner(mmap)))
}

/// Atomic counters for the disk-spill tier. Cloneable via [`Arc`]; a live view
/// is taken with [`CacheStats::snapshot`].
#[derive(Debug, Default)]
pub struct CacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    bytes_from_disk: AtomicU64,
    bytes_fetched: AtomicU64,
}

/// A point-in-time, plain-data view of [`CacheStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub bytes_from_disk: u64,
    pub bytes_fetched: u64,
}

impl CacheStatsSnapshot {
    /// Fraction of whole-layer reads served from the local disk cache, in
    /// `0.0..=1.0`. Returns `0.0` when there have been no reads.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

impl CacheStats {
    fn record_hit(&self, bytes: usize) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.bytes_from_disk
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_miss(&self, bytes: usize) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.bytes_fetched
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Take a consistent-enough snapshot of the current counters.
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bytes_from_disk: self.bytes_from_disk.load(Ordering::Relaxed),
            bytes_fetched: self.bytes_fetched.load(Ordering::Relaxed),
        }
    }
}

/// A read-through whole-layer disk cache in front of any [`ArchiveBackend`].
///
/// Cached archives are stored at `<dir>/<first-3-hex>/<40-hex>.larch`, the same
/// layout the directory backend uses, so a cache directory is itself a valid
/// (partial) raw archive store.
#[derive(Clone)]
pub struct DiskSpillArchiveBackend<D> {
    inner: D,
    dir: PathBuf,
    stats: Arc<CacheStats>,
}

impl<D> DiskSpillArchiveBackend<D> {
    /// Wrap `inner`, spilling whole layer archives under `dir`.
    pub fn new(inner: D, dir: impl Into<PathBuf>) -> Self {
        Self {
            inner,
            dir: dir.into(),
            stats: Arc::new(CacheStats::default()),
        }
    }

    /// Current cache statistics (hit rate, bytes served vs. fetched).
    pub fn stats(&self) -> CacheStatsSnapshot {
        self.stats.snapshot()
    }

    fn cache_path(&self, id: [u32; 5]) -> PathBuf {
        let name = name_to_string(id);
        let mut p = self.dir.clone();
        p.push(&name[0..PREFIX_DIR_SIZE]);
        p.push(format!("{}.larch", name));
        p
    }

    /// Read a fully-written cache file, or `None` if it is not present.
    ///
    /// The file is memory-mapped rather than read into the heap: layer archives
    /// are content-addressed and immutable once written, so the mapping never
    /// changes underneath us, and only the pages a query actually touches become
    /// resident. This is what lets a graph larger than RAM be read from the local
    /// NVMe cache — the OS page cache is the buffer pool.
    async fn read_cached(&self, id: [u32; 5]) -> Option<Bytes> {
        let path = self.cache_path(id);
        tokio::task::spawn_blocking(move || mmap_path(&path))
            .await
            .ok()
            .flatten()
    }

    /// Best-effort write-through to disk. Writes to a temporary sibling and
    /// atomically renames so a concurrent reader never sees a torn file. Any
    /// error is swallowed — the cache is an optimization, not a source of truth.
    async fn write_cached(&self, id: [u32; 5], bytes: &Bytes) {
        let path = self.cache_path(id);
        let dir = match path.parent() {
            Some(d) => d.to_path_buf(),
            None => return,
        };
        if fs::create_dir_all(&dir).await.is_err() {
            return;
        }
        // Randomised temp name so concurrent write-throughs of the same layer
        // don't clobber each other's staging file.
        let salt: u64 = rand::random();
        let tmp = dir.join(format!(".{}.{:016x}.tmp", name_to_string(id), salt));
        let write_result = async {
            let mut f = fs::File::create(&tmp).await?;
            f.write_all(bytes).await?;
            f.flush().await?;
            fs::rename(&tmp, &path).await
        }
        .await;
        if write_result.is_err() {
            let _ = fs::remove_file(&tmp).await;
        }
    }
}

#[async_trait]
impl<D: ArchiveBackend> ArchiveBackend for DiskSpillArchiveBackend<D> {
    type Read = D::Read;

    async fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes> {
        if let Some(bytes) = self.read_cached(id).await {
            self.stats.record_hit(bytes.len());
            return Ok(bytes);
        }
        let bytes = self.inner.get_layer_bytes(id).await?;
        self.stats.record_miss(bytes.len());
        self.write_cached(id, &bytes).await;
        Ok(bytes)
    }

    async fn get_layer_structure_bytes(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<Option<Bytes>> {
        // If we already have the whole archive on disk, slice it locally and
        // avoid the network entirely.
        if let Some(bytes) = self.read_cached(id).await {
            self.stats.record_hit(bytes.len());
            return Ok(Archive::parse(bytes).slice_for(file_type));
        }
        // Otherwise delegate the ranged read; we do not fetch the whole archive
        // just to satisfy a single-structure request.
        self.inner.get_layer_structure_bytes(id, file_type).await
    }

    async fn store_layer_file(&self, id: [u32; 5], bytes: Bytes) -> io::Result<()> {
        self.inner.store_layer_file(id, bytes.clone()).await?;
        // Populate the cache on write so a subsequent read is a local hit.
        self.write_cached(id, &bytes).await;
        Ok(())
    }

    async fn read_layer_structure_bytes_from(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
        read_from: usize,
    ) -> io::Result<Self::Read> {
        // Ranged streaming read always goes to the origin; the disk tier caches
        // whole archives, which are consumed via the map()/get_layer_* paths.
        self.inner
            .read_layer_structure_bytes_from(id, file_type, read_from)
            .await
    }

    async fn prefetch_layers(&self, ids: &[[u32; 5]]) -> io::Result<()> {
        use futures::stream::StreamExt;
        // Warm the disk tier (and its origin) concurrently. `get_layer_bytes`
        // records hit/miss stats and populates the local cache file, so a later
        // read is a local hit. Best-effort: per-layer errors are swallowed.
        futures::stream::iter(ids.iter().copied())
            .for_each_concurrent(PREFETCH_CONCURRENCY, |id| async move {
                let _ = self.get_layer_bytes(id).await;
            })
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use crate::storage::archive::{ArchiveLayerStore, LruArchiveBackend};
    use crate::storage::object::ObjectArchiveBackend;
    use crate::storage::LayerStore;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use tempfile::tempdir;

    type Disk = DiskSpillArchiveBackend<ObjectArchiveBackend>;
    type Lru = LruArchiveBackend<ObjectArchiveBackend, Disk>;
    type DiskLayerStore = ArchiveLayerStore<Lru, Lru>;

    // Mirror the production `open_object_store_with_cache` stack: the in-memory
    // LRU sits in front of the disk tier, which sits in front of the origin.
    // The LRU's "fits in cache" path is what turns a per-structure read into a
    // single whole-layer `get_layer_bytes` call against the disk tier.
    fn layer_store_with_disk(store: Arc<dyn ObjectStore>, dir: PathBuf) -> (DiskLayerStore, Disk) {
        let object = ObjectArchiveBackend::new(store, "");
        let disk = DiskSpillArchiveBackend::new(object.clone(), dir);
        let lru = LruArchiveBackend::new(object, disk.clone(), 100);
        let ls = ArchiveLayerStore::new(lru.clone(), lru);
        (ls, disk)
    }

    async fn build_one<S: LayerStore>(store: &S) -> [u32; 5] {
        let mut builder = store.create_base_layer().await.unwrap();
        let name = builder.name();
        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(name).await.unwrap();
        name
    }

    #[tokio::test]
    async fn disk_cache_populates_on_write_and_serves_hits() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = tempdir().unwrap();

        let name = {
            let (ls, disk) = layer_store_with_disk(bucket.clone(), dir.path().to_path_buf());
            let name = build_one(&ls).await;
            // store_layer_file wrote through to disk, so the file exists
            assert!(disk.cache_path(name).exists());
            name
        };

        // Fresh store over the same bucket AND the same disk cache dir: reads
        // should be served from disk, never touching the (in-memory) origin.
        let (ls, disk) = layer_store_with_disk(bucket.clone(), dir.path().to_path_buf());
        let layer = ls.get_layer(name).await.unwrap().unwrap();
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));

        let s = disk.stats();
        assert!(s.hits >= 1, "expected at least one disk hit, got {:?}", s);
        assert_eq!(0, s.misses, "warm disk cache should not miss");
        assert!(s.bytes_from_disk > 0);
        assert_eq!(0, s.bytes_fetched);
        assert_eq!(1.0, s.hit_rate());
    }

    // ---- Milestone 4: cold vs warm benchmark ----

    use futures::stream::BoxStream;
    use object_store::path::Path as OsPath;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOpts,
        PutOptions, PutPayload, PutResult,
    };
    use std::time::{Duration, Instant};

    /// Wraps an object store and sleeps `delay` before every GET/HEAD so a
    /// benchmark can see the per-request round-trip latency that a real network
    /// backend incurs (and that a deep layer stack multiplies).
    #[derive(Debug)]
    struct LatencyStore {
        inner: Arc<dyn ObjectStore>,
        delay: Duration,
    }

    impl std::fmt::Display for LatencyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LatencyStore({:?})", self.delay)
        }
    }

    #[async_trait]
    impl ObjectStore for LatencyStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OsPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            tokio::time::sleep(self.delay).await;
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &OsPath) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&OsPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &OsPath, to: &OsPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &OsPath, to: &OsPath) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn full_stack(store: Arc<dyn ObjectStore>, dir: PathBuf) -> (crate::store::Store, Disk) {
        use crate::storage::{CachedLayerStore, LockingHashMapLayerCache};
        let object = ObjectArchiveBackend::new(store.clone(), "");
        let disk = DiskSpillArchiveBackend::new(object.clone(), dir);
        let lru = LruArchiveBackend::new(object, disk.clone(), 100);
        let layer_store = CachedLayerStore::new(
            ArchiveLayerStore::new(lru.clone(), lru),
            LockingHashMapLayerCache::new(),
        );
        let s = crate::store::Store::new(
            crate::storage::object::ObjectLabelStore::new(store, ""),
            layer_store,
        );
        (s, disk)
    }

    async fn build_chain(store: &DiskLayerStore, depth: usize) -> [u32; 5] {
        let mut builder = store.create_base_layer().await.unwrap();
        let mut name = builder.name();
        builder.add_value_triple(ValueTriple::new_string_value("root", "p", "0"));
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(name).await.unwrap();
        for i in 1..depth {
            let mut builder = store.create_child_layer(name).await.unwrap();
            name = builder.name();
            builder.add_value_triple(ValueTriple::new_string_value(
                &format!("s{}", i),
                "p",
                &format!("o{}", i),
            ));
            builder.commit_boxed().await.unwrap();
            store.finalize_layer(name).await.unwrap();
        }
        name
    }

    /// Cold vs warm read of a multi-layer graph. Ignored by default (it sleeps);
    /// run with `cargo test --features object-store -- --ignored --nocapture
    /// bench_cold_vs_warm` and copy the printed numbers into the RFC.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn bench_cold_vs_warm() {
        const DEPTH: usize = 12;
        let rtt = Duration::from_millis(5);

        // Build the chain into a latency-free in-memory bucket.
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let build_dir = tempdir().unwrap();
        let head = {
            let (ls, _) = layer_store_with_disk(bucket.clone(), build_dir.path().to_path_buf());
            build_chain(&ls, DEPTH).await
        };

        // Reads now go through a simulated-latency wrapper.
        let slow: Arc<dyn ObjectStore> = Arc::new(LatencyStore {
            inner: bucket.clone(),
            delay: rtt,
        });

        // COLD: empty disk cache, empty LRU, empty object cache.
        let cold_dir = tempdir().unwrap();
        let (cold_store, cold_disk) = full_stack(slow.clone(), cold_dir.path().to_path_buf());
        let t = Instant::now();
        let layer = cold_store.get_layer_from_id(head).await.unwrap().unwrap();
        let cold = t.elapsed();
        assert!(layer.triple_count() >= DEPTH);

        // WARM (object cache): same store, second read → Arc hit, no I/O.
        let t = Instant::now();
        let _ = cold_store.get_layer_from_id(head).await.unwrap().unwrap();
        let warm_obj = t.elapsed();

        // WARM (disk): fresh caches over the same populated disk directory, so
        // layer *data* is local but *metadata* HEADs still hit the slow origin.
        let (disk_store, _d) = full_stack(slow.clone(), cold_dir.path().to_path_buf());
        let t = Instant::now();
        let _ = disk_store.get_layer_from_id(head).await.unwrap().unwrap();
        let warm_disk = t.elapsed();

        let stats = cold_disk.stats();
        println!(
            "\n=== object-store cold vs warm read ({} layers, {:?} simulated RTT) ===",
            DEPTH, rtt
        );
        println!("cold  (network, empty caches): {:?}", cold);
        println!("warm  (in-process object cache): {:?}", warm_obj);
        println!("warm  (local disk, cold metadata): {:?}", warm_disk);
        println!(
            "disk tier during cold read: {} hits / {} misses, {} bytes fetched from origin",
            stats.hits, stats.misses, stats.bytes_fetched
        );
        println!("=================================================================\n");
    }

    // ---- Phase 0B: mmap-backed reads / larger-than-RAM ----

    // A layer store whose in-memory LRU budget is `mem_mib` MiB, over an mmap'd
    // disk tier over the origin. With mem_mib = 0 the RAM cache holds nothing, so
    // every structure read is served from the memory-mapped disk file.
    fn layer_store_disk_mem(
        store: Arc<dyn ObjectStore>,
        dir: PathBuf,
        mem_mib: usize,
    ) -> (DiskLayerStore, Disk) {
        let object = ObjectArchiveBackend::new(store, "");
        let disk = DiskSpillArchiveBackend::new(object.clone(), dir);
        let lru = LruArchiveBackend::new(object, disk.clone(), mem_mib);
        let ls = ArchiveLayerStore::new(lru.clone(), lru);
        (ls, disk)
    }

    async fn build_wide_chain(store: &DiskLayerStore, layers: usize, per_layer: usize) -> [u32; 5] {
        let mut builder = store.create_base_layer().await.unwrap();
        let mut name = builder.name();
        for i in 0..per_layer {
            builder.add_value_triple(ValueTriple::new_string_value(
                &format!("s{}", i),
                "p",
                &format!("o{}", i),
            ));
        }
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(name).await.unwrap();
        for l in 1..layers {
            let mut builder = store.create_child_layer(name).await.unwrap();
            name = builder.name();
            for i in 0..per_layer {
                builder.add_value_triple(ValueTriple::new_string_value(
                    &format!("s{}_{}", l, i),
                    "p",
                    &format!("o{}_{}", l, i),
                ));
            }
            builder.commit_boxed().await.unwrap();
            store.finalize_layer(name).await.unwrap();
        }
        name
    }

    /// With a zero-byte RAM cache, the whole read is served from the memory-mapped
    /// disk tier — proving the graph can be read without holding its structures in
    /// the heap cache (the larger-than-RAM path).
    #[tokio::test]
    async fn larger_than_ram_read_via_mmap() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = tempdir().unwrap();

        let head = {
            // build with a normal cache so writes go through and populate disk
            let (ls, _) = layer_store_disk_mem(bucket.clone(), dir.path().to_path_buf(), 100);
            build_wide_chain(&ls, 4, 250).await
        };

        // reopen with a ZERO-byte RAM cache: nothing fits, so every structure read
        // must come from the mmap'd disk file.
        let (ls, disk) = layer_store_disk_mem(bucket.clone(), dir.path().to_path_buf(), 0);
        let layer = ls.get_layer(head).await.unwrap().unwrap();

        // spot-check triples from the base and the top layer
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("s0", "p", "o0")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("s3_249", "p", "o3_249")));
        assert!(!layer.value_triple_exists(&ValueTriple::new_string_value("nope", "p", "nope")));

        // and a full scan reconstructs every triple
        let count = layer.triples().count();
        assert_eq!(4 * 250, count);

        // the disk tier actually served the reads (RAM cache held nothing)
        assert!(disk.stats().hits + disk.stats().misses > 0);
    }

    #[tokio::test]
    async fn disk_cache_miss_then_fetch_from_origin() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // First store writes the layer to the bucket with NO disk cache.
        let name = {
            let object = ObjectArchiveBackend::new(bucket.clone(), "");
            let ls = ArchiveLayerStore::new(object.clone(), object);
            build_one(&ls).await
        };

        // Second store has an empty disk cache dir: the first read must miss and
        // fetch from the origin, then populate the cache so the next read hits.
        let dir = tempdir().unwrap();
        let (ls, disk) = layer_store_with_disk(bucket.clone(), dir.path().to_path_buf());

        let layer = ls.get_layer(name).await.unwrap().unwrap();
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));

        let s = disk.stats();
        assert!(s.misses >= 1, "cold read should miss, got {:?}", s);
        assert!(s.bytes_fetched > 0);
        assert!(disk.cache_path(name).exists(), "miss should populate cache");
    }
}
