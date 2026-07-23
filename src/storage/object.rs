//! Object-storage backend for terminus-store.
//!
//! This module provides a third storage backend (next to the directory and
//! memory backends) that keeps a store inside an S3-compatible object store.
//! It is built on the Apache Arrow [`object_store`] crate, so the same code
//! works against S3, GCS, Azure, a local filesystem, or an in-memory store.
//!
//! # Design
//!
//! Since the v0.20 archive format, a layer is a single `*.larch` archive file.
//! The modern layer store, [`ArchiveLayerStore`](super::archive::ArchiveLayerStore),
//! is already generic over two small traits that move layer bytes and layer
//! metadata around:
//!
//! - [`ArchiveBackend`](super::archive::ArchiveBackend)
//! - [`ArchiveMetadataBackend`](super::archive::ArchiveMetadataBackend)
//!
//! [`ObjectArchiveBackend`] implements both against an object store; one layer
//! maps to one object, keyed by its content hash. We therefore reuse the
//! existing [`ArchiveLayerStore`], the existing byte-level
//! [`LruArchiveBackend`](super::archive::LruArchiveBackend), and the blanket
//! `LayerStore` impl unchanged.
//!
//! Labels — the only mutable state — are handled by [`ObjectLabelStore`], which
//! replaces the directory backend's `flock`-guarded read-modify-write with a
//! conditional PUT (ETag / version compare-and-swap).
//!
//! Whole layer archives are fetched into memory; nothing is memory-mapped. This
//! backend provides durability, cheap storage, stateless replicas, and trivial
//! backup. It does **not** make the database larger than RAM: the working set is
//! still fully materialized and expanded in memory on read.

use std::io::{self, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;
use tokio::io::AsyncRead;

use object_store::path::Path as ObjectPath;
use object_store::{
    Error as OsError, GetOptions, GetRange, ObjectStore, PutMode, PutOptions, UpdateVersion,
};

use super::archive::{Archive, ArchiveBackend, ArchiveHeader, ArchiveMetadataBackend};
use super::consts::LayerFileEnum;
use super::label::{Label, LabelStore};
use super::layer::{name_to_string, string_to_name};
use super::stack_manifest::StackManifest;

/// Errors surfaced by the object-store backend that are not naturally an
/// [`io::Error`] on their own. They are wrapped in an [`io::Error`] (and are
/// therefore downcastable) so the trait signatures stay unchanged.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    /// A label compare-and-swap kept losing to concurrent writers until the
    /// retry budget was exhausted.
    #[error("label '{label}' update conflict: still contended after {retries} retries")]
    LabelConflict { label: String, retries: usize },
}

impl ObjectStoreError {
    fn into_io(self) -> io::Error {
        io::Error::other(self)
    }
}

/// How many times a label compare-and-swap will re-read and retry on a
/// precondition failure before surfacing an [`ObjectStoreError::LabelConflict`].
const LABEL_CAS_MAX_RETRIES: usize = 16;

/// Upper bound of an archive header (file-presence `u64` + offsets logarray).
/// The offsets logarray has at most one entry per [`LayerFileEnum`] variant
/// (~48), so the real header is a few hundred bytes; 8 KiB is a generous,
/// single-round-trip probe that is guaranteed to contain the whole header.
const HEADER_PROBE_BYTES: usize = 8192;

fn os_err_to_io(e: OsError) -> io::Error {
    let kind = match &e {
        OsError::NotFound { .. } => ErrorKind::NotFound,
        OsError::AlreadyExists { .. } => ErrorKind::AlreadyExists,
        _ => ErrorKind::Other,
    };
    io::Error::new(kind, e)
}

/// A minimal `AsyncRead` over an owned [`Bytes`] buffer, used as the `Read`
/// associated type for ranged structure reads.
pub struct BytesReader(Bytes);

impl AsyncRead for BytesReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let bytes = &mut self.get_mut().0;
        let n = std::cmp::min(buf.remaining(), bytes.len());
        let chunk = bytes.split_to(n);
        buf.put_slice(chunk.as_ref());
        std::task::Poll::Ready(Ok(()))
    }
}

/// Object-store implementation of both archive backend traits.
///
/// One layer archive is stored as a single object at
/// `<prefix>/<first-3-hex>/<40-hex>.larch`, mirroring the directory backend's
/// on-disk layout so a bucket and a local mirror are byte-for-byte comparable.
/// Rollups live in a sibling `<...>.rollup.hex` object.
#[derive(Clone)]
pub struct ObjectArchiveBackend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectArchiveBackend {
    /// Build a backend over an existing object store, placing all keys under
    /// `prefix` (which may be empty).
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        // normalize: no trailing slash, we add separators explicitly
        while prefix.ends_with('/') {
            prefix.pop();
        }
        Self { store, prefix }
    }

    fn key(&self, suffix: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(suffix)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, suffix))
        }
    }

    fn layer_key(&self, id: [u32; 5]) -> ObjectPath {
        let s = name_to_string(id);
        self.key(&format!("{}/{}.larch", &s[0..3], s))
    }

    fn rollup_key(&self, id: [u32; 5]) -> ObjectPath {
        let s = name_to_string(id);
        self.key(&format!("{}/{}.rollup.hex", &s[0..3], s))
    }

    fn stack_key(&self, id: [u32; 5]) -> ObjectPath {
        let s = name_to_string(id);
        self.key(&format!("{}/{}.stack", &s[0..3], s))
    }

    /// Walk parent pointers from `id` (inclusive) to the base, returning the
    /// ordered chain head-first. Used only to seed a manifest when an ancestor
    /// has none; once ancestors have manifests this is never called.
    async fn walk_parents_inclusive(&self, id: [u32; 5]) -> io::Result<Vec<[u32; 5]>> {
        let mut chain = vec![id];
        let mut current = id;
        while let Some(parent) = self.get_parent(current).await? {
            chain.push(parent);
            current = parent;
        }
        Ok(chain)
    }

    fn list_prefix(&self) -> Option<ObjectPath> {
        if self.prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(self.prefix.clone()))
        }
    }

    /// Fetch and parse the archive header of a layer, returning the header plus
    /// the absolute byte offset at which the data section begins.
    async fn layer_header(&self, id: [u32; 5]) -> io::Result<(ArchiveHeader, usize)> {
        let path = self.layer_key(id);
        let opts = GetOptions {
            range: Some(GetRange::Bounded(0..HEADER_PROBE_BYTES)),
            ..Default::default()
        };
        let result = self
            .store
            .get_opts(&path, opts)
            .await
            .map_err(os_err_to_io)?;
        let probe = result.bytes().await.map_err(os_err_to_io)?;
        let probe_len = probe.len();
        let (header, remainder) = ArchiveHeader::parse(probe.clone());
        let data_start = probe_len - remainder.len();
        Ok((header, data_start))
    }
}

#[async_trait]
impl ArchiveBackend for ObjectArchiveBackend {
    type Read = BytesReader;

    async fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes> {
        let path = self.layer_key(id);
        let result = self.store.get(&path).await.map_err(os_err_to_io)?;
        result.bytes().await.map_err(os_err_to_io)
    }

    async fn get_layer_structure_bytes(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<Option<Bytes>> {
        let (header, data_start) = self.layer_header(id).await?;
        match header.range_for(file_type) {
            None => Ok(None),
            Some(range) => {
                let abs = (data_start + range.start)..(data_start + range.end);
                if abs.start == abs.end {
                    // zero-length structure: object_store rejects empty ranges
                    return Ok(Some(Bytes::new()));
                }
                let path = self.layer_key(id);
                let opts = GetOptions {
                    range: Some(GetRange::Bounded(abs)),
                    ..Default::default()
                };
                let result = self
                    .store
                    .get_opts(&path, opts)
                    .await
                    .map_err(os_err_to_io)?;
                Ok(Some(result.bytes().await.map_err(os_err_to_io)?))
            }
        }
    }

    async fn store_layer_file(&self, id: [u32; 5], bytes: Bytes) -> io::Result<()> {
        let path = self.layer_key(id);
        let opts = PutOptions::from(PutMode::Create);
        match self.store.put_opts(&path, bytes.into(), opts).await {
            Ok(_) => Ok(()),
            // Layer names are content hashes: an existing object has identical
            // content, so a create conflict is success, not an error.
            Err(OsError::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(os_err_to_io(e)),
        }
    }

    async fn read_layer_structure_bytes_from(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
        read_from: usize,
    ) -> io::Result<Self::Read> {
        let (header, data_start) = self.layer_header(id).await?;
        let range = header
            .range_for(file_type)
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "slice not found in archive"))?;
        let start = data_start + range.start + read_from;
        let end = data_start + range.end;
        if start >= end {
            return Ok(BytesReader(Bytes::new()));
        }
        let path = self.layer_key(id);
        let opts = GetOptions {
            range: Some(GetRange::Bounded(start..end)),
            ..Default::default()
        };
        let result = self
            .store
            .get_opts(&path, opts)
            .await
            .map_err(os_err_to_io)?;
        Ok(BytesReader(result.bytes().await.map_err(os_err_to_io)?))
    }
}

#[async_trait]
impl ArchiveMetadataBackend for ObjectArchiveBackend {
    async fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>> {
        let mut stream = self.store.list(self.list_prefix().as_ref());
        let mut result = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(os_err_to_io)?;
            if let Some(filename) = meta.location.filename() {
                if let Some(stem) = filename.strip_suffix(".larch") {
                    result.push(string_to_name(stem)?);
                }
            }
        }
        Ok(result)
    }

    async fn layer_exists(&self, id: [u32; 5]) -> io::Result<bool> {
        let path = self.layer_key(id);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(OsError::NotFound { .. }) => Ok(false),
            Err(e) => Err(os_err_to_io(e)),
        }
    }

    async fn layer_size(&self, id: [u32; 5]) -> io::Result<u64> {
        let path = self.layer_key(id);
        let meta = self.store.head(&path).await.map_err(os_err_to_io)?;
        Ok(meta.size as u64)
    }

    async fn layer_file_exists(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<bool> {
        let (header, _) = self.layer_header(id).await?;
        Ok(header.range_for(file_type).is_some())
    }

    async fn get_layer_structure_size(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<usize> {
        let (header, _) = self.layer_header(id).await?;
        header
            .size_of(file_type)
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "slice not found in archive"))
    }

    async fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        let path = self.rollup_key(id);
        let result = match self.store.get(&path).await {
            Ok(r) => r,
            Err(OsError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(os_err_to_io(e)),
        };
        let data = result.bytes().await.map_err(os_err_to_io)?;
        let s = String::from_utf8_lossy(&data);
        // Format: "1\n<40-hex>\n" — a version line then the rollup id.
        let name = s.lines().nth(1).ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "rollup object missing second line")
        })?;
        Ok(Some(string_to_name(name)?))
    }

    async fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()> {
        let path = self.rollup_key(id);
        let contents = format!("1\n{}\n", name_to_string(rollup));
        // Rollup objects are written atomically; no lock needed. Overwrite is
        // fine — a rollup only ever improves and readers see whole objects.
        self.store
            .put(&path, Bytes::from(contents).into())
            .await
            .map_err(os_err_to_io)?;
        Ok(())
    }

    async fn get_parent(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        if let Some(parent_bytes) = self
            .get_layer_structure_bytes(id, LayerFileEnum::Parent)
            .await?
        {
            let parent_string = std::str::from_utf8(&parent_bytes[..40])
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
            Ok(Some(string_to_name(parent_string)?))
        } else {
            Ok(None)
        }
    }

    async fn get_stack_manifest(&self, id: [u32; 5]) -> io::Result<Option<Bytes>> {
        let path = self.stack_key(id);
        match self.store.get(&path).await {
            Ok(r) => Ok(Some(r.bytes().await.map_err(os_err_to_io)?)),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(os_err_to_io(e)),
        }
    }

    async fn set_stack_manifest(&self, id: [u32; 5], bytes: Bytes) -> io::Result<()> {
        // Overwrite is safe: the manifest is keyed by an immutable,
        // content-addressed head, so it is written once for a unique layer.
        self.store
            .put(&self.stack_key(id), bytes.into())
            .await
            .map_err(os_err_to_io)?;
        Ok(())
    }

    async fn on_layer_finalized(&self, id: [u32; 5]) -> io::Result<()> {
        // Build this layer's manifest = [id] ++ parent's chain. If the parent
        // already has a manifest (the common case for freshly built chains) this
        // is O(1); otherwise we seed by walking parents once.
        let chain = match self.get_parent(id).await? {
            None => vec![id],
            Some(parent) => {
                let mut chain = vec![id];
                match self.get_stack_manifest(parent).await? {
                    Some(bytes) => match StackManifest::decode(bytes) {
                        Some(m) if m.is_for(parent) => chain.extend(m.layers),
                        _ => chain.extend(self.walk_parents_inclusive(parent).await?),
                    },
                    None => chain.extend(self.walk_parents_inclusive(parent).await?),
                }
                chain
            }
        };
        self.set_stack_manifest(id, StackManifest::new(chain).encode())
            .await
    }
}

// ---------------------------------------------------------------------------
// Label store
// ---------------------------------------------------------------------------

fn label_from_data(name: String, data: &[u8]) -> io::Result<Label> {
    let s = String::from_utf8_lossy(data);
    let lines: Vec<&str> = s.lines().collect();
    if lines.len() != 2 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "expected label object to have two lines. contents were ({:?})",
                lines
            ),
        ));
    }

    let version = lines[0].parse::<u64>().map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "expected first line of label object to be a number but it was {}",
                lines[0]
            ),
        )
    })?;

    let layer = if lines[1].is_empty() {
        None
    } else {
        Some(string_to_name(lines[1])?)
    };

    Ok(Label {
        name,
        layer,
        version,
    })
}

fn label_to_contents(label: &Label) -> Bytes {
    let s = match label.layer {
        None => format!("{}\n\n", label.version),
        Some(layer) => format!("{}\n{}\n", label.version, name_to_string(layer)),
    };
    Bytes::from(s)
}

/// Compare-and-swap label store backed by an object store.
///
/// Each label `foo` is a single object `<prefix>/foo.label` holding the same
/// two-line `version\nlayer` format the directory backend uses. Mutations use a
/// conditional PUT keyed on the object's ETag/version: this replaces the
/// directory backend's `flock`, so multiple stateless processes sharing one
/// bucket coordinate purely through the store.
#[derive(Clone)]
pub struct ObjectLabelStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectLabelStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        while prefix.ends_with('/') {
            prefix.pop();
        }
        Self { store, prefix }
    }

    fn label_key(&self, name: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!("{}.label", name))
        } else {
            ObjectPath::from(format!("{}/{}.label", self.prefix, name))
        }
    }

    fn list_prefix(&self) -> Option<ObjectPath> {
        if self.prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(self.prefix.clone()))
        }
    }

    /// Read a label together with the store version needed to condition a
    /// later update on it.
    async fn get_label_versioned(&self, name: &str) -> io::Result<Option<(Label, UpdateVersion)>> {
        let path = self.label_key(name);
        let result = match self.store.get(&path).await {
            Ok(r) => r,
            Err(OsError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(os_err_to_io(e)),
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let data = result.bytes().await.map_err(os_err_to_io)?;
        let label = label_from_data(name.to_owned(), &data)?;
        Ok(Some((label, version)))
    }
}

#[async_trait]
impl LabelStore for ObjectLabelStore {
    async fn labels(&self) -> io::Result<Vec<Label>> {
        let mut stream = self.store.list(self.list_prefix().as_ref());
        let mut result = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(os_err_to_io)?;
            if let Some(filename) = meta.location.filename() {
                if let Some(stem) = filename.strip_suffix(".label") {
                    let name = stem.to_string();
                    let data = self
                        .store
                        .get(&meta.location)
                        .await
                        .map_err(os_err_to_io)?
                        .bytes()
                        .await
                        .map_err(os_err_to_io)?;
                    result.push(label_from_data(name, &data)?);
                }
            }
        }
        Ok(result)
    }

    async fn create_label(&self, name: &str) -> io::Result<Label> {
        let path = self.label_key(name);
        let contents = Bytes::from_static(b"0\n\n");
        let opts = PutOptions::from(PutMode::Create);
        match self.store.put_opts(&path, contents.into(), opts).await {
            Ok(_) => Ok(Label::new_empty(name)),
            Err(OsError::AlreadyExists { .. }) => Err(io::Error::new(
                ErrorKind::InvalidInput,
                "database already exists",
            )),
            Err(e) => Err(os_err_to_io(e)),
        }
    }

    async fn get_label(&self, name: &str) -> io::Result<Option<Label>> {
        Ok(self
            .get_label_versioned(name)
            .await?
            .map(|(label, _)| label))
    }

    async fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);
        let contents = label_to_contents(&new_label);

        let mut backoff = Duration::from_millis(1);
        for _ in 0..LABEL_CAS_MAX_RETRIES {
            let (current, version) = match self.get_label_versioned(&label.name).await? {
                Some(pair) => pair,
                None => return Err(io::Error::new(ErrorKind::NotFound, "label not found")),
            };

            // The caller's snapshot must still be the current stored label —
            // this mirrors the directory backend's equality check and yields
            // Ok(None) on a genuine lost update.
            if current != *label {
                return Ok(None);
            }

            let opts = PutOptions::from(PutMode::Update(version));
            match self
                .store
                .put_opts(&self.label_key(&label.name), contents.clone().into(), opts)
                .await
            {
                Ok(_) => return Ok(Some(new_label)),
                // Someone else wrote between our read and our conditional PUT.
                // Re-read: usually the stored label now differs from the
                // caller's snapshot and we return Ok(None); otherwise retry.
                Err(OsError::Precondition { .. }) => {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(100));
                    continue;
                }
                Err(e) => return Err(os_err_to_io(e)),
            }
        }

        Err(ObjectStoreError::LabelConflict {
            label: label.name.clone(),
            retries: LABEL_CAS_MAX_RETRIES,
        }
        .into_io())
    }

    async fn delete_label(&self, name: &str) -> io::Result<bool> {
        let path = self.label_key(name);
        // Check existence first so we can report whether we actually deleted
        // something, matching the directory backend's bool return.
        let existed = match self.store.head(&path).await {
            Ok(_) => true,
            Err(OsError::NotFound { .. }) => false,
            Err(e) => return Err(os_err_to_io(e)),
        };
        match self.store.delete(&path).await {
            Ok(()) => Ok(existed),
            Err(OsError::NotFound { .. }) => Ok(false),
            Err(e) => Err(os_err_to_io(e)),
        }
    }
}

/// Unused helper retained for symmetry with the whole-archive read path;
/// parses an already-fetched archive and returns a structure slice.
#[allow(dead_code)]
fn slice_structure(bytes: Bytes, file_type: LayerFileEnum) -> Option<Bytes> {
    Archive::parse(bytes).slice_for(file_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use crate::storage::archive::ArchiveLayerStore;
    use crate::storage::LayerStore;
    use futures::future::join_all;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    fn mem() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    type ObjLayerStore = ArchiveLayerStore<ObjectArchiveBackend, ObjectArchiveBackend>;

    fn object_layer_store(store: Arc<dyn ObjectStore>) -> ObjLayerStore {
        let backend = ObjectArchiveBackend::new(store, "");
        ArchiveLayerStore::new(backend.clone(), backend)
    }

    // ---- Milestone 1: read path, ported from the directory test suite ----

    #[tokio::test]
    async fn create_layers_from_object_store() {
        let store = object_layer_store(mem());

        let layer = async {
            let mut builder = store.create_base_layer().await?;
            let base_name = builder.name();

            builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
            builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
            builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

            builder.commit_boxed().await?;

            let mut builder = store.create_child_layer(base_name).await?;
            let child_name = builder.name();

            builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
            builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

            builder.commit_boxed().await?;

            store.get_layer(child_name).await
        }
        .await
        .unwrap()
        .unwrap();

        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
        assert!(layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn rollup_and_retrieve_base() {
        let store = Arc::new(object_layer_store(mem()));

        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));
        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        let _rolled_id = store.clone().rollup(unrolled_layer).await.unwrap();
        let rolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(rolled_layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn rollup_and_retrieve_child() {
        let store = Arc::new(object_layer_store(mem()));

        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));
        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(child_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_node("cow", "likes", "pig"));
        builder.add_value_triple(ValueTriple::new_node("cow", "hates", "pig"));
        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        let _rolled_id = store
            .clone()
            .rollup_upto(unrolled_layer, base_name)
            .await
            .unwrap();
        let rolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(rolled_layer.value_triple_exists(&ValueTriple::new_node("cow", "hates", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("cow", "likes", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn nonexistent_layer_is_none() {
        let store = object_layer_store(mem());
        assert!(store.get_layer([1, 2, 3, 4, 5]).await.unwrap().is_none());
    }

    // ---- Milestone 2: write path — commit a chain, drop, reopen, read back ----

    // Persist a committed layer to the backing store. `commit_boxed` only fills
    // the in-process construction map; `finalize_layer` is what assembles the
    // archive and PUTs the object (this is exactly what the high-level `Store`
    // API does after committing a builder).
    async fn commit_chain<S: LayerStore>(store: &S) -> [u32; 5] {
        let mut builder = store.create_base_layer().await.unwrap();
        let base = builder.name();
        builder.add_value_triple(ValueTriple::new_string_value("a", "p", "1"));
        builder.add_value_triple(ValueTriple::new_string_value("b", "p", "2"));
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(base).await.unwrap();

        let mut builder = store.create_child_layer(base).await.unwrap();
        let mid = builder.name();
        builder.add_value_triple(ValueTriple::new_string_value("c", "p", "3"));
        builder.remove_value_triple(ValueTriple::new_string_value("a", "p", "1"));
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(mid).await.unwrap();

        let mut builder = store.create_child_layer(mid).await.unwrap();
        let head = builder.name();
        builder.add_value_triple(ValueTriple::new_node("c", "likes", "b"));
        builder.commit_boxed().await.unwrap();
        store.finalize_layer(head).await.unwrap();

        head
    }

    fn assert_graph(layer: &InternalLayer) {
        assert!(!layer.value_triple_exists(&ValueTriple::new_string_value("a", "p", "1")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("b", "p", "2")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("c", "p", "3")));
        assert!(layer.value_triple_exists(&ValueTriple::new_node("c", "likes", "b")));
    }

    #[tokio::test]
    async fn commit_chain_reopen_from_same_bucket_in_memory() {
        let bucket = mem();

        let head = {
            let store = object_layer_store(bucket.clone());
            commit_chain(&store).await
            // store dropped here
        };

        // reopen from the same bucket with a brand new store instance
        let reopened = object_layer_store(bucket.clone());
        let layer = reopened.get_layer(head).await.unwrap().unwrap();
        assert_graph(&layer);

        // the full ancestor chain is walkable
        let stack = reopened.retrieve_layer_stack_names(head).await.unwrap();
        assert_eq!(3, stack.len());
    }

    #[tokio::test]
    async fn commit_chain_reopen_from_same_bucket_local_fs() {
        let dir = tempdir().unwrap();
        let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        let head = {
            let store = object_layer_store(fs.clone());
            commit_chain(&store).await
        };

        // A truly independent store object over the same on-disk bucket.
        let fs2 = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let reopened = object_layer_store(fs2 as Arc<dyn ObjectStore>);
        let layer = reopened.get_layer(head).await.unwrap().unwrap();
        assert_graph(&layer);
    }

    // ---- Milestone 3: labels & compare-and-swap coordination ----
    //
    // Note: these use InMemory because conditional `PutMode::Update` is required
    // and LocalFileSystem returns NotImplemented for it. S3/GCS/Azure/R2/MinIO
    // all support it.

    #[tokio::test]
    async fn object_create_and_retrieve_equal_label() {
        let store = ObjectLabelStore::new(mem(), "");
        let stored = store.create_label("foo").await.unwrap();
        let retrieved = store.get_label("foo").await.unwrap();
        assert_eq!(None, stored.layer);
        assert_eq!(stored, retrieved.unwrap());
    }

    #[tokio::test]
    async fn object_update_label_succeeds() {
        let store = ObjectLabelStore::new(mem(), "");
        let stored = store.create_label("foo").await.unwrap();
        store.set_label(&stored, [6, 7, 8, 9, 10]).await.unwrap();
        let retrieved = store.get_label("foo").await.unwrap().unwrap();
        assert_eq!(Some([6, 7, 8, 9, 10]), retrieved.layer);
        assert_eq!(1, retrieved.version);
    }

    #[tokio::test]
    async fn object_update_label_twice_from_same_label_object_fails() {
        let store = ObjectLabelStore::new(mem(), "");
        let stored1 = store.create_label("foo").await.unwrap();
        let stored2 = store.set_label(&stored1, [6, 7, 8, 9, 10]).await.unwrap();
        let stored3 = store.set_label(&stored1, [10, 9, 8, 7, 6]).await.unwrap();
        assert!(stored2.is_some());
        assert!(stored3.is_none());
    }

    #[tokio::test]
    async fn object_create_label_twice_errors() {
        let store = ObjectLabelStore::new(mem(), "");
        store.create_label("foo").await.unwrap();
        let result = store.create_label("foo").await;
        assert!(result.is_err());
        assert_eq!(io::ErrorKind::InvalidInput, result.err().unwrap().kind());
    }

    #[tokio::test]
    async fn object_create_and_delete_label() {
        let store = ObjectLabelStore::new(mem(), "");
        store.create_label("foo").await.unwrap();
        assert!(store.get_label("foo").await.unwrap().is_some());
        assert!(store.delete_label("foo").await.unwrap());
        assert!(store.get_label("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn object_delete_nonexistent_label() {
        let store = ObjectLabelStore::new(mem(), "");
        assert!(!store.delete_label("foo").await.unwrap());
    }

    #[tokio::test]
    async fn object_labels_lists_all() {
        let store = ObjectLabelStore::new(mem(), "");
        store.create_label("foo").await.unwrap();
        store.create_label("bar").await.unwrap();
        let mut names: Vec<String> = store
            .labels()
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.name)
            .collect();
        names.sort();
        assert_eq!(vec!["bar".to_string(), "foo".to_string()], names);
    }

    /// N tasks race to advance the same label from an identical snapshot, for
    /// several rounds. Every round must have exactly one winner (no lost
    /// updates), the version must advance by exactly one (no torn reads), and no
    /// task may observe a malformed label.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_label_advance_single_winner_per_round() {
        const N: u32 = 8;
        const ROUNDS: u64 = 6;

        let store = Arc::new(ObjectLabelStore::new(mem(), ""));
        store.create_label("race").await.unwrap();

        for round in 0..ROUNDS {
            // Every racer starts from the same freshly-read snapshot.
            let snapshot = store.get_label("race").await.unwrap().unwrap();
            assert_eq!(round, snapshot.version);

            let mut handles = Vec::new();
            for i in 0..N {
                let store = store.clone();
                let snapshot = snapshot.clone();
                let layer = [round as u32, i, 0, 0, 0];
                handles.push(tokio::spawn(async move {
                    store.set_label(&snapshot, layer).await
                }));
            }

            let results = join_all(handles).await;
            let mut winners = 0;
            for r in results {
                match r.unwrap() {
                    Ok(Some(new)) => {
                        winners += 1;
                        assert_eq!(round + 1, new.version);
                    }
                    Ok(None) => {}
                    Err(e) => panic!("unexpected error racing label: {}", e),
                }
            }

            assert_eq!(1, winners, "expected exactly one winner in round {round}");

            let after = store.get_label("race").await.unwrap().unwrap();
            assert_eq!(
                round + 1,
                after.version,
                "version must advance by exactly one"
            );
        }
    }

    // ---- Milestone 5: integration tests against real object storage ----
    //
    // These are #[ignore]d and only run when pointed at a live S3-compatible
    // endpoint (MinIO/LocalStack/R2/S3) via environment variables. Bring up the
    // provided MinIO with `docker compose -f docker-compose.minio.yml up -d`,
    // then:
    //
    //   TDB_OBJECT_STORE_ENDPOINT=http://localhost:9100 \
    //   TDB_OBJECT_STORE_BUCKET=terminusdb \
    //   TDB_OBJECT_STORE_ACCESS_KEY_ID=minioadmin \
    //   TDB_OBJECT_STORE_SECRET_ACCESS_KEY=minioadmin \
    //   cargo test --features object-store -- --ignored --nocapture minio_
    //
    // Each test uses a unique key prefix so runs don't collide and cleanup is
    // unnecessary.
    use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
    use std::env;

    fn s3_from_env() -> Arc<dyn ObjectStore> {
        let endpoint = env::var("TDB_OBJECT_STORE_ENDPOINT")
            .expect("set TDB_OBJECT_STORE_ENDPOINT to run the MinIO integration tests");
        let bucket = env::var("TDB_OBJECT_STORE_BUCKET")
            .expect("set TDB_OBJECT_STORE_BUCKET to run the MinIO integration tests");
        let access =
            env::var("TDB_OBJECT_STORE_ACCESS_KEY_ID").unwrap_or_else(|_| "minioadmin".to_string());
        let secret = env::var("TDB_OBJECT_STORE_SECRET_ACCESS_KEY")
            .unwrap_or_else(|_| "minioadmin".to_string());
        let region =
            env::var("TDB_OBJECT_STORE_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let s3 = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_access_key_id(access)
            .with_secret_access_key(secret)
            .with_region(region)
            .with_allow_http(true) // MinIO over http; drop for https R2/S3
            // MinIO and R2 support conditional PUT via If-Match/If-None-Match,
            // which is what the label compare-and-swap relies on.
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build S3 store from environment");
        Arc::new(s3)
    }

    fn unique_prefix(tag: &str) -> String {
        format!("it/{}/{:016x}", tag, rand::random::<u64>())
    }

    /// Full end-to-end round-trip against a real object store using the public
    /// `Store` API: create a database, commit a two-layer chain, set the head,
    /// then reopen the store from the same bucket+prefix and read it back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn minio_end_to_end_roundtrip() {
        use crate::store::open_object_store;

        let s3 = s3_from_env();
        let prefix = unique_prefix("roundtrip");

        {
            let store = open_object_store(s3.clone(), prefix.clone(), 100);
            let db = store.create("graph").await.unwrap();
            assert!(db.head().await.unwrap().is_none());

            let builder = store.create_base_layer().await.unwrap();
            builder
                .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
                .unwrap();
            let l1 = builder.commit().await.unwrap();
            assert!(db.set_head(&l1).await.unwrap());

            let builder = l1.open_write().await.unwrap();
            builder
                .add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"))
                .unwrap();
            let l2 = builder.commit().await.unwrap();
            assert!(db.set_head(&l2).await.unwrap());
            // store dropped here — nothing kept in process
        }

        // Reopen a brand-new store over the same bucket and prefix.
        let store = open_object_store(s3.clone(), prefix.clone(), 100);
        let db = store
            .open("graph")
            .await
            .unwrap()
            .expect("database must exist after reopening from the bucket");
        let head = db.head().await.unwrap().unwrap();
        assert!(head.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(head.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
    }

    /// Compare-and-swap against a real object store: two racers advancing the
    /// same label from an identical snapshot yield exactly one winner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn minio_label_cas_single_winner() {
        let store = Arc::new(ObjectLabelStore::new(s3_from_env(), unique_prefix("cas")));
        store.create_label("race").await.unwrap();
        let snapshot = store.get_label("race").await.unwrap().unwrap();

        let a = {
            let store = store.clone();
            let snapshot = snapshot.clone();
            tokio::spawn(async move { store.set_label(&snapshot, [1, 1, 1, 1, 1]).await })
        };
        let b = {
            let store = store.clone();
            let snapshot = snapshot.clone();
            tokio::spawn(async move { store.set_label(&snapshot, [2, 2, 2, 2, 2]).await })
        };

        let ra = a.await.unwrap().unwrap();
        let rb = b.await.unwrap().unwrap();
        let winners = [ra.is_some(), rb.is_some()].iter().filter(|x| **x).count();
        assert_eq!(1, winners, "exactly one racer must win the CAS");
        assert_eq!(1, store.get_label("race").await.unwrap().unwrap().version);
    }
}
