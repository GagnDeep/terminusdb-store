//! Block-lazy string dictionary reader (Phase 3, Stage 2a).
//!
//! A layer's dictionary is stored as an offset table plus a data section of
//! fixed-size (8-entry) front-coded blocks. Loading a whole dictionary — the
//! largest part of a layer — is what keeps the disk-less selective read path
//! from being a big win.
//!
//! [`BlockLazyStringDict`] keeps only the small offset table resident and
//! fetches an individual data block on demand via a ranged read, decoding it
//! with tdb-succinct's public block codec. An `id -> string` lookup therefore
//! transfers just one block (~a few hundred bytes) instead of the whole
//! dictionary. It is **async** (each block is a fetch), so it is used from the
//! already-async selective read path rather than the synchronous `Layer`
//! accessors.
//!
//! Both directions are block-lazy: `id -> string` ([`entry`]/[`get_string`])
//! fetches the single block holding the id, and `string -> id` ([`id`]/
//! [`id_of_string`]) binary-searches block heads, touching only the O(log n)
//! blocks the search visits. Fetched blocks are cached (keyed by block index)
//! so the binary search and repeated lookups never re-fetch a block.
//!
//! [`entry`]: BlockLazyStringDict::entry
//! [`get_string`]: BlockLazyStringDict::get_string
//! [`id`]: BlockLazyStringDict::id
//! [`id_of_string`]: BlockLazyStringDict::id_of_string

use std::io;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use tdb_succinct::block::{IdLookupResult, SizedDictBlock};
use tdb_succinct::{MonotonicLogArray, SizedDictEntry, TypedDictEntry};

use super::archive::{ArchiveBackend, ArchiveMetadataBackend};
use super::consts::LayerFileEnum;

const BLOCK_SIZE: usize = 8;

/// The minimal, object-safe read seam a block-lazy dictionary needs: read a
/// whole small structure (the offset table), report a structure's byte size,
/// and read an arbitrary byte range of a structure (one block). Blanket-
/// implemented for any backend that is both an [`ArchiveBackend`] and an
/// [`ArchiveMetadataBackend`], and also implemented by `ArchiveLayerStore` so a
/// `dyn LayerStore` can hand one out without exposing its backend type.
#[async_trait]
pub trait BlockSource: Send + Sync {
    async fn structure_bytes(
        &self,
        layer: [u32; 5],
        file: LayerFileEnum,
    ) -> io::Result<Option<Bytes>>;
    async fn structure_size(&self, layer: [u32; 5], file: LayerFileEnum) -> io::Result<usize>;
    async fn structure_range(
        &self,
        layer: [u32; 5],
        file: LayerFileEnum,
        range: Range<usize>,
    ) -> io::Result<Bytes>;
}

#[async_trait]
impl<T: ArchiveBackend + ArchiveMetadataBackend> BlockSource for T {
    async fn structure_bytes(
        &self,
        layer: [u32; 5],
        file: LayerFileEnum,
    ) -> io::Result<Option<Bytes>> {
        self.get_layer_structure_bytes(layer, file).await
    }
    async fn structure_size(&self, layer: [u32; 5], file: LayerFileEnum) -> io::Result<usize> {
        self.get_layer_structure_size(layer, file).await
    }
    async fn structure_range(
        &self,
        layer: [u32; 5],
        file: LayerFileEnum,
        range: Range<usize>,
    ) -> io::Result<Bytes> {
        self.get_layer_structure_range(layer, file, range).await
    }
}

/// A dictionary reader that fetches one data block per lookup instead of the
/// whole dictionary, over any [`BlockSource`] (e.g. the object-store backend).
pub struct BlockLazyStringDict {
    source: Arc<dyn BlockSource>,
    layer: [u32; 5],
    blocks_file: LayerFileEnum,
    /// Resident offset table: `offsets.entry(i)` is the byte offset in the data
    /// section where block `i + 1` begins. Has `num_blocks - 1` entries.
    offsets: Option<MonotonicLogArray>,
    /// Total byte length of the data (blocks) structure.
    data_len: usize,
    /// LRU cache of already-fetched block bytes, keyed by block index, so the
    /// `id()` binary search (and repeated lookups) never re-fetches a block.
    block_cache: Mutex<LruCache<usize, Bytes>>,
}

impl BlockLazyStringDict {
    /// Open a block-lazy view over the dictionary whose offset table and data
    /// section are the given layer structures. Loads only the (small) offset
    /// table and the data-section size — not the data itself.
    pub async fn open(
        source: Arc<dyn BlockSource>,
        layer: [u32; 5],
        offsets_file: LayerFileEnum,
        blocks_file: LayerFileEnum,
    ) -> io::Result<Self> {
        let offsets = match source.structure_bytes(layer, offsets_file).await? {
            Some(b) if !b.is_empty() => Some(MonotonicLogArray::parse(b).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("offsets: {:?}", e))
            })?),
            _ => None,
        };
        let data_len = source.structure_size(layer, blocks_file).await.unwrap_or(0);
        Ok(Self {
            source,
            layer,
            blocks_file,
            offsets,
            data_len,
            block_cache: Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap())),
        })
    }

    fn num_blocks(&self) -> usize {
        if self.data_len == 0 {
            0
        } else {
            self.offsets.as_ref().map_or(1, |o| o.len() + 1)
        }
    }

    fn block_range(&self, block_index: usize) -> (usize, usize) {
        let offsets = self.offsets.as_ref();
        let start = if block_index == 0 {
            0
        } else {
            offsets.map_or(0, |o| o.entry(block_index - 1) as usize)
        };
        let end = if block_index + 1 < self.num_blocks() {
            offsets.map_or(self.data_len, |o| o.entry(block_index) as usize)
        } else {
            self.data_len
        };
        (start, end)
    }

    /// Fetch and parse one data block via a ranged read, caching the raw block
    /// bytes so a repeat fetch is served from memory (parse itself is cheap).
    async fn get_block(&self, block_index: usize) -> io::Result<SizedDictBlock> {
        let cached = self.block_cache.lock().unwrap().get(&block_index).cloned();
        let mut bytes: Bytes = match cached {
            Some(b) => b,
            None => {
                let (start, end) = self.block_range(block_index);
                let fetched = self
                    .source
                    .structure_range(self.layer, self.blocks_file, start..end)
                    .await?;
                self.block_cache
                    .lock()
                    .unwrap()
                    .put(block_index, fetched.clone());
                fetched
            }
        };
        SizedDictBlock::parse(&mut bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("block: {:?}", e)))
    }

    /// The dictionary entry for `id` (1-based), fetching only its block.
    pub async fn entry(&self, id: u64) -> io::Result<Option<SizedDictEntry>> {
        if id == 0 {
            return Ok(None);
        }
        let idx = (id - 1) as usize;
        let block_index = idx / BLOCK_SIZE;
        let within = idx % BLOCK_SIZE;
        if block_index >= self.num_blocks() {
            return Ok(None);
        }
        let block = self.get_block(block_index).await?;
        if within >= block.num_entries() as usize {
            return Ok(None);
        }
        Ok(Some(block.entry(within)))
    }

    /// The id of a dictionary entry given its raw bytes, fetching only the
    /// blocks the binary search touches (mirrors `SizedDict::id`).
    pub async fn id(&self, slice: &[u8]) -> io::Result<IdLookupResult> {
        let num_blocks = self.num_blocks();
        if num_blocks == 0 {
            return Ok(IdLookupResult::NotFound);
        }
        let mut min = 0usize;
        let mut max = num_blocks - 1; // = offsets.len()
        while min <= max {
            let mid = (min + max) / 2;
            let head = self.get_block(mid).await?.entry(0).to_bytes();
            match slice.cmp(&head[..]) {
                std::cmp::Ordering::Less => {
                    if mid == 0 {
                        return Ok(IdLookupResult::NotFound);
                    }
                    max = mid - 1;
                }
                std::cmp::Ordering::Greater => min = mid + 1,
                std::cmp::Ordering::Equal => {
                    return Ok(IdLookupResult::Found((mid * BLOCK_SIZE + 1) as u64))
                }
            }
        }
        let found = max;
        let block = self.get_block(found).await?;
        let offset = (found * BLOCK_SIZE) as u64 + 1;
        Ok(block.id(slice).offset(offset).default(offset - 1))
    }

    /// The id of a string in this (string) dictionary, or `None` if absent.
    pub async fn id_of_string(&self, s: &str) -> io::Result<Option<u64>> {
        Ok(self.id(s.as_bytes()).await?.into_option())
    }

    /// The string for `id` (for a string dictionary), fetching only its block.
    pub async fn get_string(&self, id: u64) -> io::Result<Option<String>> {
        Ok(self
            .entry(id)
            .await?
            .map(|e| String::from_utf8_lossy(&e.to_bytes()).into_owned()))
    }
}

/// A block-lazy reader for a layer's **typed value** dictionary (`TypedDict`).
///
/// A value dictionary groups its entries into per-datatype segments over one
/// shared block-data structure. This keeps the three small index logarrays
/// resident (`types_present`, `type_offsets`, `block_offsets`) and the per-type
/// id offsets, then fetches only the O(log n) blocks a lookup touches — mirroring
/// `TypedDict::id_slice` (find the datatype's segment, binary-search its blocks).
///
/// Only the `value -> id` direction is implemented (all the selective existence
/// path needs); the reverse (`id -> value`) still uses the whole dictionary.
///
/// A datatype segment's block `k` is exactly the global block `seg_start + k`,
/// so block byte ranges are computed from the global `block_offsets` and the
/// data length — the same shape as [`BlockLazyStringDict`].
pub struct BlockLazyTypedDict {
    source: Arc<dyn BlockSource>,
    layer: [u32; 5],
    blocks_file: LayerFileEnum,
    types_present: MonotonicLogArray,
    type_offsets: MonotonicLogArray,
    block_offsets: MonotonicLogArray,
    /// Cumulative id offset for the start of each datatype segment (mirrors
    /// `TypedDict::type_id_offsets`); `type_id_offsets[i-1]` is segment `i`'s.
    type_id_offsets: Vec<u64>,
    /// Length of the block data, excluding the 8 trailing bytes `TypedDict`
    /// strips (`data.slice(..len - 8)`).
    data_len: usize,
    block_cache: Mutex<LruCache<usize, Bytes>>,
}

impl BlockLazyTypedDict {
    /// Open a block-lazy view over a value dictionary from its four structures.
    /// Loads only the three small index logarrays (and, for multi-datatype
    /// dictionaries, one control byte per datatype boundary) — never the blocks.
    pub async fn open(
        source: Arc<dyn BlockSource>,
        layer: [u32; 5],
        types_present_file: LayerFileEnum,
        type_offsets_file: LayerFileEnum,
        block_offsets_file: LayerFileEnum,
        blocks_file: LayerFileEnum,
    ) -> io::Result<Self> {
        async fn logarray(
            source: &Arc<dyn BlockSource>,
            layer: [u32; 5],
            file: LayerFileEnum,
        ) -> io::Result<MonotonicLogArray> {
            // Value-dict index structures are always present (an empty logarray
            // is still an 8-byte control word), so absence is an error rather
            // than a silently-empty array. `open` is only called on non-empty
            // value dictionaries (guarded by the entry count at the call site).
            let bytes = source
                .structure_bytes(layer, file)
                .await?
                .filter(|b| !b.is_empty())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "value dict structure missing")
                })?;
            MonotonicLogArray::parse(bytes).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("logarray: {:?}", e))
            })
        }
        let types_present = logarray(&source, layer, types_present_file).await?;
        let type_offsets = logarray(&source, layer, type_offsets_file).await?;
        let block_offsets = logarray(&source, layer, block_offsets_file).await?;
        let raw_len = source.structure_size(layer, blocks_file).await.unwrap_or(0);
        // `TypedDict` stores its block data as `data.slice(..len - 8)`.
        let data_len = raw_len.saturating_sub(8);

        // Compute per-segment id offsets exactly as `TypedDict::from_parts`:
        // each needs the control byte at the datatype boundary in the data.
        let mut type_id_offsets = Vec::new();
        if !types_present.is_empty() {
            let mut tally: u64 = 0;
            for type_offset in type_offsets.iter() {
                let boundary = if type_offset == 0 {
                    0
                } else {
                    block_offsets.entry(type_offset as usize - 1) as usize
                };
                let cw = source
                    .structure_range(layer, blocks_file, boundary..boundary + 1)
                    .await?;
                let last_block_len = tdb_succinct::block::parse_block_control_records(cw[0]);
                let gap = BLOCK_SIZE as u8 - last_block_len;
                tally += gap as u64;
                type_id_offsets.push((type_offset + 1) * 8 - tally);
            }
        }

        Ok(Self {
            source,
            layer,
            blocks_file,
            types_present,
            type_offsets,
            block_offsets,
            type_id_offsets,
            data_len,
            block_cache: Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap())),
        })
    }

    /// Byte range of global block `g` in the data structure.
    fn global_block_range(&self, g: usize) -> (usize, usize) {
        let start = if g == 0 {
            0
        } else {
            self.block_offsets.entry(g - 1) as usize
        };
        let end = if g < self.block_offsets.len() {
            self.block_offsets.entry(g) as usize
        } else {
            self.data_len
        };
        (start, end)
    }

    async fn get_block(&self, g: usize) -> io::Result<SizedDictBlock> {
        let cached = self.block_cache.lock().unwrap().get(&g).cloned();
        let mut bytes: Bytes = match cached {
            Some(b) => b,
            None => {
                let (start, end) = self.global_block_range(g);
                let fetched = self
                    .source
                    .structure_range(self.layer, self.blocks_file, start..end)
                    .await?;
                self.block_cache.lock().unwrap().put(g, fetched.clone());
                fetched
            }
        };
        SizedDictBlock::parse(&mut bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("block: {:?}", e)))
    }

    /// The datatype segment for type index `i`: its first global block index,
    /// its block count, and its id offset (mirrors `TypedDict::inner_type_segment`).
    fn segment(&self, i: usize) -> (usize, usize, u64) {
        let (type_offset, id_offset) = if i == 0 {
            (0usize, 0u64)
        } else {
            (
                self.type_offsets.entry(i - 1) as usize,
                self.type_id_offsets[i - 1],
            )
        };
        let len = if i == self.types_present.len() - 1 {
            if i == 0 {
                self.block_offsets.len() - type_offset
            } else {
                self.block_offsets.len() - type_offset - 1
            }
        } else {
            let next_offset = self.type_offsets.entry(i) as usize;
            if i == 0 {
                next_offset - type_offset
            } else {
                next_offset - type_offset - 1
            }
        };
        let seg_start = if i == 0 { 0 } else { type_offset + 1 };
        (seg_start, len + 1, id_offset)
    }

    /// The value-dictionary-local id of a typed entry, or `None` if absent —
    /// the same value as `TypedDict::id_entry(..).into_option()`.
    pub async fn id_of_entry(&self, entry: &TypedDictEntry) -> io::Result<Option<u64>> {
        let dt = entry.datatype();
        let i = match self.types_present.index_of(dt as u64) {
            Some(i) => i,
            None => return Ok(None),
        };
        let (seg_start, num_blocks, id_offset) = self.segment(i);
        if num_blocks == 0 {
            return Ok(None);
        }
        let slice = entry.to_bytes();
        let slice = &slice[..];

        // Binary search over the segment's blocks (mirrors `SizedDict::id`),
        // block `k` = global block `seg_start + k`.
        let mut min = 0usize;
        let mut max = num_blocks - 1;
        let seg_result: IdLookupResult = loop {
            if min > max {
                let found = max;
                let block = self.get_block(seg_start + found).await?;
                let offset = (found * BLOCK_SIZE) as u64 + 1;
                break block.id(slice).offset(offset).default(offset - 1);
            }
            let mid = (min + max) / 2;
            let head = self.get_block(seg_start + mid).await?.entry(0).to_bytes();
            match slice.cmp(&head[..]) {
                std::cmp::Ordering::Less => {
                    if mid == 0 {
                        break IdLookupResult::NotFound;
                    }
                    max = mid - 1;
                }
                std::cmp::Ordering::Greater => min = mid + 1,
                std::cmp::Ordering::Equal => {
                    break IdLookupResult::Found((mid * BLOCK_SIZE + 1) as u64)
                }
            }
        };
        // Compose with the segment's id offset, as `TypedDict::id_slice` does.
        Ok(seg_result.offset(id_offset).into_option())
    }

    /// The datatype segment a value-dictionary-local id belongs to (mirrors
    /// `TypedDict::type_index_for_id`).
    fn type_index_for_id(&self, id: u64) -> usize {
        for (ix, offset) in self.type_id_offsets.iter().enumerate() {
            if *offset > id - 1 {
                return ix;
            }
        }
        self.type_id_offsets.len()
    }

    /// The typed value for a value-dictionary-local `id`, fetching only the block
    /// that holds it — the same value as `TypedDict::entry(id)`. `None` if out of
    /// range.
    pub async fn entry(&self, id: u64) -> io::Result<Option<TypedDictEntry>> {
        if id == 0 || self.types_present.is_empty() {
            return Ok(None);
        }
        let type_index = self.type_index_for_id(id);
        if type_index >= self.types_present.len() {
            return Ok(None);
        }
        let (seg_start, num_blocks, id_offset) = self.segment(type_index);
        if id <= id_offset {
            return Ok(None);
        }
        // 1-based position within the segment, then block + offset within block.
        let p = (id - id_offset) as usize;
        let block_in_seg = (p - 1) / BLOCK_SIZE;
        let pos_in_block = (p - 1) % BLOCK_SIZE;
        if block_in_seg >= num_blocks {
            return Ok(None);
        }
        let block = self.get_block(seg_start + block_in_seg).await?;
        if pos_in_block >= block.num_entries() as usize {
            return Ok(None);
        }
        let dt = <tdb_succinct::Datatype as num_traits::FromPrimitive>::from_u64(
            self.types_present.entry(type_index),
        )
        .expect("value dictionary has an unknown datatype discriminant");
        Ok(Some(TypedDictEntry::new(dt, block.entry(pos_in_block))))
    }
}

/// A block-lazy reader for a `LogArray` — random access to a single bit-packed
/// element without loading the whole array. An adjacency list's `nums` array is
/// the largest structure a selective existence walk touches, yet each lookup
/// reads only a handful of elements; this fetches just the one or two 64-bit
/// words each element spans (via a ranged read), keeping only the control word
/// (length + bit width) resident.
pub struct BlockLazyLogArray {
    source: Arc<dyn BlockSource>,
    layer: [u32; 5],
    file: LayerFileEnum,
    len: u64,
    width: u8,
    /// Byte length of the structure (data words followed by the 8-byte control
    /// word).
    data_len: usize,
    /// Cache of fetched 64-bit words keyed by their byte offset, so a scan over
    /// consecutive elements re-reads a shared word at most once.
    word_cache: Mutex<LruCache<usize, [u8; 8]>>,
}

impl BlockLazyLogArray {
    /// Open a block-lazy view over a `LogArray` structure, reading only its
    /// 8-byte control word (which encodes length and element bit width).
    pub async fn open(
        source: Arc<dyn BlockSource>,
        layer: [u32; 5],
        file: LayerFileEnum,
    ) -> io::Result<Self> {
        let data_len = source.structure_size(layer, file).await.unwrap_or(0);
        let (len, width) = if data_len >= 8 {
            let cw = source
                .structure_range(layer, file, data_len - 8..data_len)
                .await?;
            tdb_succinct::parse_control_word(&cw[..])
        } else {
            (0, 0)
        };
        Ok(Self {
            source,
            layer,
            file,
            len,
            width,
            data_len,
            word_cache: Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap())),
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The 64-bit big-endian word at `byte_index` (zero-padded if the structure
    /// ends early, which a valid element never relies on), via a cached ranged
    /// read.
    async fn word_at(&self, byte_index: usize) -> io::Result<u64> {
        if let Some(w) = self.word_cache.lock().unwrap().get(&byte_index) {
            return Ok(u64::from_be_bytes(*w));
        }
        let end = (byte_index + 8).min(self.data_len);
        let mut w = [0u8; 8];
        if byte_index < end {
            let bytes = self
                .source
                .structure_range(self.layer, self.file, byte_index..end)
                .await?;
            let n = bytes.len().min(8);
            w[..n].copy_from_slice(&bytes[..n]);
        }
        self.word_cache.lock().unwrap().put(byte_index, w);
        Ok(u64::from_be_bytes(w))
    }

    /// The element at `index` — the exact decoding `LogArray::entry` performs,
    /// but fetching only the word(s) it spans.
    pub async fn entry(&self, index: usize) -> io::Result<u64> {
        if self.width == 0 {
            return Ok(0);
        }
        let bit_index = (self.width as usize) * index;
        let byte_index = (bit_index >> 6) << 3;
        let offset = (bit_index & 0b11_1111) as u8;
        let leading_zeros = 64 - self.width;
        let first_word = self.word_at(byte_index).await?;
        if offset + self.width <= 64 {
            return Ok(first_word << offset >> leading_zeros);
        }
        let second_word = self.word_at(byte_index + 8).await?;
        let first_width = 64 - offset;
        let second_width = self.width - first_width;
        let first_part = first_word << offset >> offset << second_width;
        let second_part = second_word >> (64 - second_width);
        Ok(first_part | second_part)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use crate::storage::archive::ArchiveLayerStore;
    use crate::storage::consts::LayerFileEnum;
    use crate::storage::object::ObjectArchiveBackend;
    use crate::storage::LayerStore;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn block_lazy_string_dict_matches_full_dictionary() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectArchiveBackend::new(bucket.clone(), "");
        let store = ArchiveLayerStore::new(backend.clone(), backend.clone());

        // A base layer with many distinct node strings (several dict blocks).
        let mut builder = store.create_base_layer().await.unwrap();
        let name = builder.name();
        for i in 0..500 {
            builder.add_value_triple(ValueTriple::new_node(
                &format!("node{:04}", i),
                "p",
                &format!("node{:04}", (i + 1) % 500),
            ));
        }
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(name).await.unwrap();

        // Oracle: the fully-loaded node dictionary.
        let full = store.get_node_dictionary(name).await.unwrap().unwrap();
        let n = full.num_entries() as u64;
        assert!(n > 8, "expected several blocks");

        let lazy = BlockLazyStringDict::open(
            Arc::new(backend) as Arc<dyn BlockSource>,
            name,
            LayerFileEnum::NodeDictionaryOffsets,
            LayerFileEnum::NodeDictionaryBlocks,
        )
        .await
        .unwrap();

        // Every id resolves to the same string, one block fetched at a time.
        for id in 1..=n {
            assert_eq!(
                full.get(id as usize),
                lazy.get_string(id).await.unwrap(),
                "id {}",
                id
            );
        }
        // Out-of-range ids yield None.
        assert_eq!(None, lazy.get_string(0).await.unwrap());
        assert_eq!(None, lazy.get_string(n + 1).await.unwrap());

        // string -> id matches the full dictionary, and round-trips.
        for i in [0usize, 1, 7, 8, 42, 100, 255, 256, 499] {
            let s = format!("node{:04}", i);
            let sr: &str = &s;
            let expected = full.id(&sr).into_option();
            let got = lazy.id_of_string(&s).await.unwrap();
            assert_eq!(expected, got, "id_of({})", s);
            if let Some(id) = got {
                assert_eq!(Some(s.clone()), lazy.get_string(id).await.unwrap());
            }
        }
        // absent strings resolve to None, matching the full dictionary.
        for s in ["node9999", "aaa", "zzzzzz"] {
            let sr: &str = s;
            assert_eq!(
                full.id(&sr).into_option(),
                lazy.id_of_string(s).await.unwrap(),
                "absent {}",
                s
            );
        }

        // A single lookup fetches one block, a small fraction of the whole
        // dictionary data — the disk-less win.
        let (s, e) = lazy.block_range(0);
        assert!(
            (e - s) * 8 < lazy.data_len,
            "one block ({} bytes) should be a small fraction of the dict data ({} bytes)",
            e - s,
            lazy.data_len
        );
    }

    #[tokio::test]
    async fn block_lazy_typed_dict_matches_full_dictionary() {
        use tdb_succinct::TdbDataType;

        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectArchiveBackend::new(bucket.clone(), "");
        let store = ArchiveLayerStore::new(backend.clone(), backend.clone());

        // A base layer whose value dictionary spans several datatypes, each with
        // enough entries to fill multiple blocks (so segments and block-lazy
        // binary search are genuinely exercised).
        let mut builder = store.create_base_layer().await.unwrap();
        let name = builder.name();
        for i in 0..400 {
            // string values
            builder.add_value_triple(ValueTriple::new_string_value(
                &format!("s{:04}", i),
                "p",
                &format!("val{:04}", i),
            ));
            // i32 typed values
            builder.add_value_triple(ValueTriple::new_value(
                &format!("s{:04}", i),
                "n",
                <i32 as TdbDataType>::make_entry(&(i as i32)),
            ));
            // f64 typed values
            builder.add_value_triple(ValueTriple::new_value(
                &format!("s{:04}", i),
                "f",
                <f64 as TdbDataType>::make_entry(&(i as f64 * 1.5)),
            ));
        }
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(name).await.unwrap();

        // Oracle: the fully-loaded typed value dictionary.
        let full = store.get_value_dictionary(name).await.unwrap().unwrap();

        let lazy = BlockLazyTypedDict::open(
            Arc::new(backend) as Arc<dyn BlockSource>,
            name,
            LayerFileEnum::ValueDictionaryTypesPresent,
            LayerFileEnum::ValueDictionaryTypeOffsets,
            LayerFileEnum::ValueDictionaryOffsets,
            LayerFileEnum::ValueDictionaryBlocks,
        )
        .await
        .unwrap();

        // Present entries across all three datatypes resolve to the same id.
        let mut entries: Vec<TypedDictEntry> = Vec::new();
        for i in [0usize, 1, 7, 8, 42, 100, 255, 256, 399] {
            entries.push(<i32 as TdbDataType>::make_entry(&(i as i32)));
            entries.push(<f64 as TdbDataType>::make_entry(&(i as f64 * 1.5)));
            entries.push(<String as TdbDataType>::make_entry(&format!("val{:04}", i)));
        }
        for e in &entries {
            assert_eq!(
                full.id_entry(e).into_option(),
                lazy.id_of_entry(e).await.unwrap(),
                "id_of_entry mismatch for {:?}",
                e.datatype()
            );
        }
        // Absent entries (including a datatype-present-but-value-absent case and
        // a value of a datatype not in the dictionary) resolve to None, matching.
        let absents = [
            <i32 as TdbDataType>::make_entry(&999_999),
            <f64 as TdbDataType>::make_entry(&-1.0),
            <String as TdbDataType>::make_entry(&"nope".to_string()),
            <bool as TdbDataType>::make_entry(&true),
        ];
        for e in &absents {
            assert_eq!(
                full.id_entry(e).into_option(),
                lazy.id_of_entry(e).await.unwrap(),
                "absent id_of_entry mismatch for {:?}",
                e.datatype()
            );
        }

        // Reverse direction: every id resolves to the same typed value as the
        // full dictionary, one block fetched at a time.
        let n = full.num_entries() as u64;
        assert!(n > 24, "expected several entries across datatypes");
        for id in 1..=n {
            assert_eq!(
                full.entry(id as usize),
                lazy.entry(id).await.unwrap(),
                "entry {}",
                id
            );
        }
        assert_eq!(None, lazy.entry(0).await.unwrap());
        assert_eq!(None, lazy.entry(n + 1).await.unwrap());
    }

    #[tokio::test]
    async fn block_lazy_logarray_matches_full_logarray() {
        use tdb_succinct::LogArray;

        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectArchiveBackend::new(bucket.clone(), "");
        let store = ArchiveLayerStore::new(backend.clone(), backend.clone());

        // A base layer whose sp_o adjacency `nums` LogArray has many entries of
        // varying magnitude (ids up to a few thousand -> multi-word spans).
        let mut builder = store.create_base_layer().await.unwrap();
        let name = builder.name();
        for i in 0..1000 {
            for j in 0..3 {
                builder.add_value_triple(ValueTriple::new_node(
                    &format!("s{:04}", i),
                    &format!("p{}", j),
                    &format!("s{:04}", (i * 7 + j) % 1000),
                ));
            }
        }
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(name).await.unwrap();

        // Oracle: the fully-parsed nums LogArray.
        let bytes = backend
            .get_layer_structure_bytes(name, LayerFileEnum::PosSpOAdjacencyListNums)
            .await
            .unwrap()
            .unwrap();
        let full = LogArray::parse(bytes).unwrap();
        let n = full.len();
        assert!(n > 100, "expected a sizeable nums array");

        let lazy = BlockLazyLogArray::open(
            Arc::new(backend) as Arc<dyn BlockSource>,
            name,
            LayerFileEnum::PosSpOAdjacencyListNums,
        )
        .await
        .unwrap();
        assert_eq!(n as u64, lazy.len());

        for i in 0..n {
            assert_eq!(full.entry(i), lazy.entry(i).await.unwrap(), "entry {}", i);
        }
    }
}
