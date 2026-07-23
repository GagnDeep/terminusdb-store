//! Persisted layer-stack manifest (Phase 1 of the object-store latency work).
//!
//! A cold read of a graph must know every ancestor layer id before it can fetch
//! the archives. Discovering that list by walking `parent.hex` pointers is
//! inherently sequential — one round trip per ancestor — which dominates cold
//! read latency over object storage.
//!
//! A [`StackManifest`] is an optional sidecar object, written next to a layer
//! when it is finalized, that records the full ordered ancestor id list
//! (head first, base last) in a single blob. Reading it in one GET lets the
//! store warm the whole chain's archives in parallel before the normal
//! (now cache-hitting) walk runs.
//!
//! It is a **hint only**: the authoritative source of truth remains each layer's
//! `parent.hex` / archive `Parent` structure. Any absence, parse failure, or
//! validation mismatch silently degrades to the sequential walk. Because layers
//! are content-addressed and immutable, a manifest that is valid for a given
//! head is valid forever.

use bytes::{Buf, BufMut, Bytes, BytesMut};

const MAGIC: &[u8; 4] = b"TSTK";
const VERSION: u8 = 1;

/// The ordered ancestor chain of a layer: `layers[0]` is the head (the layer the
/// manifest is keyed by), `layers[last]` is the base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackManifest {
    pub layers: Vec<[u32; 5]>,
}

fn put_name(buf: &mut BytesMut, name: [u32; 5]) {
    for n in name {
        buf.put_u32(n);
    }
}

fn get_name(buf: &mut Bytes) -> [u32; 5] {
    let mut name = [0u32; 5];
    for slot in name.iter_mut() {
        *slot = buf.get_u32();
    }
    name
}

impl StackManifest {
    pub fn new(layers: Vec<[u32; 5]>) -> Self {
        Self { layers }
    }

    /// Serialize to `MAGIC | VERSION | count:u32 | count × id[20 bytes big-endian]`.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4 + 1 + 4 + self.layers.len() * 20);
        buf.put_slice(MAGIC);
        buf.put_u8(VERSION);
        buf.put_u32(self.layers.len() as u32);
        for &name in &self.layers {
            put_name(&mut buf, name);
        }
        buf.freeze()
    }

    /// Parse a manifest, returning `None` on any inconsistency (wrong magic or
    /// version, truncated buffer, empty chain). Callers treat `None` as "no
    /// manifest" and fall back to the authoritative walk.
    pub fn decode(mut bytes: Bytes) -> Option<Self> {
        if bytes.len() < 9 || &bytes[0..4] != MAGIC {
            return None;
        }
        bytes.advance(4);
        if bytes.get_u8() != VERSION {
            return None;
        }
        let count = bytes.get_u32() as usize;
        if count == 0 || bytes.len() != count * 20 {
            return None;
        }
        let mut layers = Vec::with_capacity(count);
        for _ in 0..count {
            layers.push(get_name(&mut bytes));
        }
        Some(Self { layers })
    }

    /// Validate that this manifest is the one for `head` (its first entry).
    pub fn is_for(&self, head: [u32; 5]) -> bool {
        self.layers.first() == Some(&head)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let m = StackManifest::new(vec![[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [0, 0, 0, 0, 0]]);
        let decoded = StackManifest::decode(m.encode()).unwrap();
        assert_eq!(m, decoded);
        assert!(decoded.is_for([1, 2, 3, 4, 5]));
        assert!(!decoded.is_for([6, 7, 8, 9, 10]));
    }

    #[test]
    fn rejects_garbage() {
        assert!(StackManifest::decode(Bytes::from_static(b"nope")).is_none());
        assert!(StackManifest::decode(Bytes::new()).is_none());
        // right magic, wrong length
        let mut b = BytesMut::new();
        b.put_slice(MAGIC);
        b.put_u8(VERSION);
        b.put_u32(2); // claims 2 entries
        b.put_slice(&[0u8; 20]); // but only 1 present
        assert!(StackManifest::decode(b.freeze()).is_none());
    }

    #[test]
    fn rejects_empty_chain() {
        let mut b = BytesMut::new();
        b.put_slice(MAGIC);
        b.put_u8(VERSION);
        b.put_u32(0);
        assert!(StackManifest::decode(b.freeze()).is_none());
    }
}
