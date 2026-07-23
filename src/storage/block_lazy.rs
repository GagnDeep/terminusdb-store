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
//! This is the read (`id -> string`) direction. The `string -> id` direction
//! (a binary search over block heads) is a further increment.

use std::io;

use bytes::Bytes;
use tdb_succinct::block::{IdLookupResult, SizedDictBlock};
use tdb_succinct::{MonotonicLogArray, SizedDictEntry};

use super::archive::{ArchiveBackend, ArchiveMetadataBackend};
use super::consts::LayerFileEnum;

const BLOCK_SIZE: usize = 8;

/// A dictionary reader that fetches one data block per lookup instead of the
/// whole dictionary. `B` is any backend that can read structure byte-ranges and
/// report structure sizes (e.g. the object-store backend).
pub struct BlockLazyStringDict<B> {
    backend: B,
    layer: [u32; 5],
    blocks_file: LayerFileEnum,
    /// Resident offset table: `offsets.entry(i)` is the byte offset in the data
    /// section where block `i + 1` begins. Has `num_blocks - 1` entries.
    offsets: Option<MonotonicLogArray>,
    /// Total byte length of the data (blocks) structure.
    data_len: usize,
}

impl<B: ArchiveBackend + ArchiveMetadataBackend> BlockLazyStringDict<B> {
    /// Open a block-lazy view over the dictionary whose offset table and data
    /// section are the given layer structures. Loads only the (small) offset
    /// table and the data-section size — not the data itself.
    pub async fn open(
        backend: B,
        layer: [u32; 5],
        offsets_file: LayerFileEnum,
        blocks_file: LayerFileEnum,
    ) -> io::Result<Self> {
        let offsets = match backend
            .get_layer_structure_bytes(layer, offsets_file)
            .await?
        {
            Some(b) if !b.is_empty() => Some(MonotonicLogArray::parse(b).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("offsets: {:?}", e))
            })?),
            _ => None,
        };
        let data_len = backend
            .get_layer_structure_size(layer, blocks_file)
            .await
            .unwrap_or(0);
        Ok(Self {
            backend,
            layer,
            blocks_file,
            offsets,
            data_len,
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

    /// Fetch and parse one data block via a ranged read.
    async fn get_block(&self, block_index: usize) -> io::Result<SizedDictBlock> {
        let (start, end) = self.block_range(block_index);
        let mut bytes: Bytes = self
            .backend
            .get_layer_structure_range(self.layer, self.blocks_file, start..end)
            .await?;
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
            backend,
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
}
