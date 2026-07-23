//! High-level API for working with terminus-store.
//!
//! It is expected that most users of this library will work exclusively with the types contained in this module.
pub mod buffered;
pub mod compaction;
pub mod sync;
pub mod wal;

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::layer::{IdTriple, Layer, LayerBuilder, LayerCounts, ObjectType, ValueTriple};
use crate::storage::archive::{ArchiveLayerStore, DirectoryArchiveBackend, LruArchiveBackend};
use crate::storage::directory::{DirectoryLabelStore, DirectoryLayerStore};
use crate::storage::memory::{MemoryLabelStore, MemoryLayerStore};
use crate::storage::{CachedLayerStore, LabelStore, LayerStore, LockingHashMapLayerCache};
use tdb_succinct::TypedDictEntry;

use std::io;

use async_trait::async_trait;
use rayon::prelude::*;

/// A store, storing a set of layers and database labels pointing to these layers.
#[derive(Clone)]
pub struct Store {
    label_store: Arc<dyn LabelStore>,
    layer_store: Arc<dyn LayerStore>,
}

/// A wrapper over a SimpleLayerBuilder, providing a thread-safe sharable interface.
///
/// The SimpleLayerBuilder requires one to have a mutable reference to
/// the underlying LayerBuilder, and on commit it will be
/// consumed. This builder only requires an immutable reference, and
/// uses a futures-aware read-write lock to synchronize access to it
/// between threads. Also, rather than consuming itself on commit,
/// this wrapper will simply mark itself as having committed,
/// returning errors on further calls.
#[derive(Clone)]
pub struct StoreLayerBuilder {
    parent: Option<Arc<dyn Layer>>,
    builder: Arc<RwLock<Option<Box<dyn LayerBuilder>>>>,
    name: [u32; 5],
    store: Store,
}

impl StoreLayerBuilder {
    async fn new(store: Store) -> io::Result<Self> {
        let builder = store.layer_store.create_base_layer().await?;

        Ok(Self {
            parent: builder.parent(),
            name: builder.name(),
            builder: Arc::new(RwLock::new(Some(builder))),
            store,
        })
    }

    fn wrap(builder: Box<dyn LayerBuilder>, store: Store) -> Self {
        StoreLayerBuilder {
            parent: builder.parent(),
            name: builder.name(),
            builder: Arc::new(RwLock::new(Some(builder))),
            store,
        }
    }

    pub fn with_builder<R, F: FnOnce(&mut Box<dyn LayerBuilder>) -> R>(
        &self,
        f: F,
    ) -> Result<R, io::Error> {
        let mut builder = self
            .builder
            .write()
            .expect("rwlock write should always succeed");
        match (*builder).as_mut() {
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "builder has already been committed",
            )),
            Some(builder) => Ok(f(builder)),
        }
    }

    /// Returns the name of the layer being built.
    pub fn name(&self) -> [u32; 5] {
        self.name
    }

    /// Returns the parent layer this builder is building on top of, if any.
    ///
    /// If there's no parent, this returns None.
    pub fn parent(&self) -> Option<Arc<dyn Layer>> {
        self.parent.clone()
    }

    /// Add a string triple.
    pub fn add_value_triple(&self, triple: ValueTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.add_value_triple(triple))
    }

    /// Add an id triple.
    pub fn add_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.add_id_triple(triple))
    }

    /// Remove a string triple.
    pub fn remove_value_triple(&self, triple: ValueTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.remove_value_triple(triple))
    }

    /// Remove an id triple.
    pub fn remove_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.remove_id_triple(triple))
    }

    /// Returns true if this layer has been committed, and false otherwise.
    pub fn committed(&self) -> bool {
        self.builder
            .read()
            .expect("rwlock write should always succeed")
            .is_none()
    }

    /// Commit the layer to storage without loading the resulting layer.
    pub async fn commit_no_load(&self) -> io::Result<()> {
        let mut builder = None;
        {
            let mut guard = self
                .builder
                .write()
                .expect("rwlock write should always succeed");

            // Setting the builder to None ensures that committed() detects we already committed (or tried to do so anyway)
            std::mem::swap(&mut builder, &mut guard);
        }

        match builder {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "builder has already been committed",
                ))
            }
            Some(builder) => {
                let id = builder.name();
                builder.commit_boxed().await?;
                self.store.layer_store.finalize_layer(id).await
            }
        }
    }

    /// Commit the layer to storage.
    pub async fn commit(&self) -> io::Result<StoreLayer> {
        let name = self.name;
        self.commit_no_load().await?;

        let layer = self.store.layer_store.get_layer(name).await?;
        Ok(StoreLayer::wrap(
            layer.expect("layer that was just created was not found in store"),
            self.store.clone(),
        ))
    }

    /// Apply all triples added and removed by a layer to this builder.
    ///
    /// This is a way to 'cherry-pick' a layer on top of another
    /// layer, without caring about its history.
    pub async fn apply_delta(&self, delta: &StoreLayer) -> Result<(), io::Error> {
        // create a child builder and use it directly
        // first check what dictionary entries we don't know about, add those
        let triple_additions = delta.triple_additions().await?;
        let triple_removals = delta.triple_removals().await?;
        rayon::join(
            move || {
                triple_additions.par_bridge().for_each(|t| {
                    delta
                        .id_triple_to_string(&t)
                        .map(|st| self.add_value_triple(st));
                });
            },
            move || {
                triple_removals.par_bridge().for_each(|t| {
                    delta
                        .id_triple_to_string(&t)
                        .map(|st| self.remove_value_triple(st));
                })
            },
        );

        Ok(())
    }

    /// Apply the changes required to change our parent layer into the given layer.
    pub fn apply_diff(&self, other: &StoreLayer) -> Result<(), io::Error> {
        // create a child builder and use it directly
        // first check what dictionary entries we don't know about, add those
        rayon::join(
            || {
                if let Some(this) = self.parent() {
                    this.triples().par_bridge().for_each(|t| {
                        if let Some(st) = this.id_triple_to_string(&t) {
                            if !other.value_triple_exists(&st) {
                                self.remove_value_triple(st).unwrap()
                            }
                        }
                    })
                };
            },
            || {
                other.triples().par_bridge().for_each(|t| {
                    if let Some(st) = other.id_triple_to_string(&t) {
                        if let Some(this) = self.parent() {
                            if !this.value_triple_exists(&st) {
                                self.add_value_triple(st).unwrap()
                            }
                        } else {
                            self.add_value_triple(st).unwrap()
                        };
                    }
                })
            },
        );

        Ok(())
    }
}

/// A layer that keeps track of the store it came out of, allowing the creation of a layer builder on top of this layer.
///
/// This type of layer supports querying what was added and what was
/// removed in this layer. This can not be done in general, because
/// the layer that has been loaded may not be the layer that was
/// originally built. This happens whenever a rollup is done. A rollup
/// will create a new layer that bundles the changes of various
/// layers. It allows for more efficient querying, but loses the
/// ability to do these delta queries directly. In order to support
/// them anyway, the StoreLayer will dynamically load in the relevant
/// files to perform the requested addition or removal query method.
#[derive(Clone)]
pub struct StoreLayer {
    // TODO this Arc here is not great
    layer: Arc<dyn Layer>,
    store: Store,
}

impl StoreLayer {
    fn wrap(layer: Arc<dyn Layer>, store: Store) -> Self {
        StoreLayer { layer, store }
    }

    /// Create a layer builder based on this layer.
    pub async fn open_write(&self) -> io::Result<StoreLayerBuilder> {
        let layer = self
            .store
            .layer_store
            .create_child_layer(self.layer.name())
            .await?;

        Ok(StoreLayerBuilder::wrap(layer, self.store.clone()))
    }

    /// Returns the parent of this layer, if any, or None if this layer has no parent.
    pub async fn parent(&self) -> io::Result<Option<StoreLayer>> {
        let parent_name = self.layer.parent_name();

        match parent_name {
            None => Ok(None),
            Some(parent_name) => match self.store.layer_store.get_layer(parent_name).await? {
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "parent layer not found even though it should exist",
                )),
                Some(layer) => Ok(Some(StoreLayer::wrap(layer, self.store.clone()))),
            },
        }
    }

    pub async fn squash_upto(&self, upto: &StoreLayer) -> io::Result<StoreLayer> {
        let layer_opt = self.store.layer_store.get_layer(self.name()).await?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "layer not found"))?;
        let name = self
            .store
            .layer_store
            .squash_upto(layer, upto.name())
            .await?;
        Ok(self
            .store
            .get_layer_from_id(name)
            .await?
            .expect("layer that was just created doesn't exist"))
    }

    /// Create a new base layer consisting of all triples in this layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Squash is one approach of
    /// accomplishing this. Rollup is another. Squash is the better
    /// option if you do not care for history, as it throws away all
    /// data that you no longer need.
    pub async fn squash(&self) -> io::Result<StoreLayer> {
        let layer_opt = self.store.layer_store.get_layer(self.name()).await?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "layer not found"))?;
        let name = self.store.layer_store.squash(layer).await?;
        Ok(self
            .store
            .get_layer_from_id(name)
            .await?
            .expect("layer that was just created doesn't exist"))
    }

    /// Create a new rollup layer which rolls up all triples in this layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    pub async fn rollup(&self) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name()).await?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "layer not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.rollup(layer).await?;
        Ok(())
    }

    /// Create a new rollup layer which rolls up all triples in this layer, as well as all ancestors up to (but not including) the given ancestor.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    pub async fn rollup_upto(&self, upto: &StoreLayer) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name()).await?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "label not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.rollup_upto(layer, upto.name()).await?;
        Ok(())
    }

    /// Like rollup_upto, rolls up upto the given layer. However, if
    /// this layer is a rollup layer, this will roll up upto that
    /// rollup.
    pub async fn imprecise_rollup_upto(&self, upto: &StoreLayer) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name()).await?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "label not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.imprecise_rollup_upto(layer, upto.name()).await?;
        Ok(())
    }

    /// Returns a future that yields true if this triple has been added in this layer, or false if it doesn't.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_addition_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        self.store
            .layer_store
            .triple_addition_exists(self.layer.name(), subject, predicate, object)
            .await
    }

    /// Returns a future that yields true if this triple has been removed in this layer, or false if it doesn't.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_removal_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        self.store
            .layer_store
            .triple_removal_exists(self.layer.name(), subject, predicate, object)
            .await
    }

    /// Returns a future that yields an iterator over all layer additions.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_additions(&self) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        let result = self
            .store
            .layer_store
            .triple_additions(self.layer.name())
            .await?;

        Ok(Box::new(result) as Box<dyn Iterator<Item = _> + Send>)
    }

    /// Returns a future that yields an iterator over all layer removals.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_removals(&self) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        let result = self
            .store
            .layer_store
            .triple_removals(self.layer.name())
            .await?;

        Ok(Box::new(result) as Box<dyn Iterator<Item = _> + Send>)
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular subject.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_additions_s(
        &self,
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_additions_s(self.layer.name(), subject)
            .await
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular subject.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_removals_s(
        &self,
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_removals_s(self.layer.name(), subject)
            .await
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular subject and predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_additions_sp(self.layer.name(), subject, predicate)
            .await
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular subject and predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_removals_sp(self.layer.name(), subject, predicate)
            .await
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_additions_p(
        &self,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_additions_p(self.layer.name(), predicate)
            .await
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_removals_p(
        &self,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_removals_p(self.layer.name(), predicate)
            .await
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular object.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_additions_o(
        &self,
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_additions_o(self.layer.name(), object)
            .await
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular object.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_removals_o(
        &self,
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_removals_o(self.layer.name(), object)
            .await
    }

    /// Returns a future that yields the amount of triples that this layer adds.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_layer_addition_count(&self) -> io::Result<usize> {
        self.store
            .layer_store
            .triple_layer_addition_count(self.layer.name())
            .await
    }

    /// Returns a future that yields the amount of triples that this layer removes.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub async fn triple_layer_removal_count(&self) -> io::Result<usize> {
        self.store
            .layer_store
            .triple_layer_removal_count(self.layer.name())
            .await
    }

    /// Returns a future that yields a vector of layer stack names describing the history of this layer, starting from the base layer up to and including the name of this layer itself.
    pub async fn retrieve_layer_stack_names(&self) -> io::Result<Vec<[u32; 5]>> {
        self.store
            .layer_store
            .retrieve_layer_stack_names(self.name())
            .await
    }
}

impl PartialEq for StoreLayer {
    #[allow(clippy::vtable_address_comparisons)]
    fn eq(&self, other: &StoreLayer) -> bool {
        Arc::ptr_eq(&self.layer, &other.layer)
    }
}

impl Eq for StoreLayer {}

#[async_trait]
impl Layer for StoreLayer {
    fn name(&self) -> [u32; 5] {
        self.layer.name()
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        self.layer.parent_name()
    }

    fn node_and_value_count(&self) -> usize {
        self.layer.node_and_value_count()
    }

    fn predicate_count(&self) -> usize {
        self.layer.predicate_count()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        self.layer.subject_id(subject)
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.layer.predicate_id(predicate)
    }

    fn object_node_id(&self, object: &str) -> Option<u64> {
        self.layer.object_node_id(object)
    }

    fn object_value_id(&self, object: &TypedDictEntry) -> Option<u64> {
        self.layer.object_value_id(object)
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        self.layer.id_subject(id)
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        self.layer.id_predicate(id)
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        self.layer.id_object(id)
    }

    fn id_object_is_node(&self, id: u64) -> Option<bool> {
        self.layer.id_object_is_node(id)
    }

    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        self.layer.triple_exists(subject, predicate, object)
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples()
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_s(subject)
    }

    fn triples_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_sp(subject, predicate)
    }

    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_p(predicate)
    }

    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_o(object)
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_addition_count(&self) -> usize {
        self.layer.triple_addition_count()
    }

    fn triple_removal_count(&self) -> usize {
        self.layer.triple_removal_count()
    }

    fn all_counts(&self) -> LayerCounts {
        self.layer.all_counts()
    }

    fn single_triple_sp(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        self.layer.single_triple_sp(subject, predicate)
    }
}

/// A named graph in terminus-store.
///
/// Named graphs in terminus-store are basically just a label pointing
/// to a layer. Opening a read transaction to a named graph is just
/// getting hold of the layer it points at, as layers are
/// read-only. Writing to a named graph is just making it point to a
/// new layer.
#[derive(Clone)]
pub struct NamedGraph {
    label: String,
    store: Store,
}

impl NamedGraph {
    fn new(label: String, store: Store) -> Self {
        NamedGraph { label, store }
    }

    /// Returns the label name itself.
    pub fn name(&self) -> &str {
        &self.label
    }

    /// Returns the layer this database points at, as well as the label version.
    pub async fn head_version(&self) -> io::Result<(Option<StoreLayer>, u64)> {
        let new_label = self.store.label_store.get_label(&self.label).await?;

        match new_label {
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "database not found",
            )),
            Some(new_label) => {
                let layer = match new_label.layer {
                    None => None,
                    Some(layer) => {
                        let layer = self.store.layer_store.get_layer(layer).await?;
                        match layer {
                            None => {
                                return Err(io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "layer not found even though it is pointed at by a label",
                                ))
                            }
                            Some(layer) => Some(StoreLayer::wrap(layer, self.store.clone())),
                        }
                    }
                };
                Ok((layer, new_label.version))
            }
        }
    }

    /// Returns the layer this database points at.
    pub async fn head(&self) -> io::Result<Option<StoreLayer>> {
        Ok(self.head_version().await?.0)
    }

    /// Set the database label to the given layer if it is a valid ancestor, returning false otherwise.
    pub async fn set_head(&self, layer: &StoreLayer) -> io::Result<bool> {
        let layer_name = layer.name();
        let label = self.store.label_store.get_label(&self.label).await?;
        if label.is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "label not found"));
        }
        let label = label.unwrap();

        let set_is_ok = match label.layer {
            None => true,
            Some(retrieved_layer_name) => {
                self.store
                    .layer_store
                    .layer_is_ancestor_of(layer_name, retrieved_layer_name)
                    .await?
            }
        };

        if set_is_ok {
            Ok(self
                .store
                .label_store
                .set_label(&label, layer_name)
                .await?
                .is_some())
        } else {
            Ok(false)
        }
    }

    /// Set the database label to the given layer, even if it is not a valid ancestor.
    pub async fn force_set_head(&self, layer: &StoreLayer) -> io::Result<()> {
        let layer_name = layer.name();

        // We are stomping on the label but `set_label` expects us to
        // know about the current label, which may have been updated
        // concurrently.
        // So keep looping until an update was succesful or an error
        // was encountered.
        loop {
            let label = self.store.label_store.get_label(&self.label).await?;
            match label {
                None => return Err(io::Error::new(io::ErrorKind::NotFound, "label not found")),
                Some(label) => {
                    if self
                        .store
                        .label_store
                        .set_label(&label, layer_name)
                        .await?
                        .is_some()
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Set the database label to the given layer, even if it is not a valid ancestor. Also checks given version, and if it doesn't match, the update won't happen and false will be returned.
    pub async fn force_set_head_version(
        &self,
        layer: &StoreLayer,
        version: u64,
    ) -> io::Result<bool> {
        let layer_name = layer.name();
        let label = self.store.label_store.get_label(&self.label).await?;
        match label {
            None => Err(io::Error::new(io::ErrorKind::NotFound, "label not found")),
            Some(label) => {
                if label.version != version {
                    Ok(false)
                } else {
                    Ok(self
                        .store
                        .label_store
                        .set_label(&label, layer_name)
                        .await?
                        .is_some())
                }
            }
        }
    }

    pub async fn delete(&self) -> io::Result<()> {
        self.store.delete(&self.label).await.map(|_| ())
    }
}

/// Reconcile per-layer `(additions, removals)` given head-first into the set of
/// id-triples that exist: the newest layer that mentions a triple decides it
/// (an addition includes it, a removal excludes it). Result is sorted.
fn reconcile_layered(per_layer_head_first: Vec<(Vec<IdTriple>, Vec<IdTriple>)>) -> Vec<IdTriple> {
    use std::collections::HashSet;
    let mut result: HashSet<IdTriple> = HashSet::new();
    let mut seen: HashSet<IdTriple> = HashSet::new();
    for (adds, removes) in per_layer_head_first {
        for t in adds {
            if seen.insert(t) {
                result.insert(t);
            }
        }
        for t in removes {
            seen.insert(t);
        }
    }
    let mut v: Vec<IdTriple> = result.into_iter().collect();
    v.sort();
    v
}

impl Store {
    /// Create a new store from the given label and layer store.
    pub fn new<Labels: 'static + LabelStore, Layers: 'static + LayerStore>(
        label_store: Labels,
        layer_store: Layers,
    ) -> Store {
        Store {
            label_store: Arc::new(label_store),
            layer_store: Arc::new(layer_store),
        }
    }

    /// Create a new database with the given name.
    ///
    /// If the database already exists, this will return an error.
    pub async fn create(&self, label: &str) -> io::Result<NamedGraph> {
        let label = self.label_store.create_label(label).await?;
        Ok(NamedGraph::new(label.name, self.clone()))
    }

    /// Open an existing database with the given name, or None if it does not exist.
    pub async fn open(&self, label: &str) -> io::Result<Option<NamedGraph>> {
        let label = self.label_store.get_label(label).await?;
        Ok(label.map(|label| NamedGraph::new(label.name, self.clone())))
    }

    /// Delete an existing database with the given name. Returns true if this database was deleted
    /// and false otherwise.
    pub async fn delete(&self, label: &str) -> io::Result<bool> {
        self.label_store.delete_label(label).await
    }

    /// Return list of names of all existing databases.
    pub async fn labels(&self) -> io::Result<Vec<String>> {
        let labels = self.label_store.labels().await?;
        Ok(labels.iter().map(|label| label.name.to_string()).collect())
    }

    /// Retrieve a layer with the given name from the layer store this Store was initialized with.
    pub async fn get_layer_from_id(&self, layer: [u32; 5]) -> io::Result<Option<StoreLayer>> {
        let layer = self.layer_store.get_layer(layer).await?;
        Ok(layer.map(|layer| StoreLayer::wrap(layer, self.clone())))
    }

    /// Check whether an id-level triple exists in the graph headed by `head`,
    /// loading only the adjacency structures of each layer rather than
    /// materializing whole layers. This is the low-memory / disk-less read path:
    /// it never fetches dictionaries, object indexes, or wavelet trees, so on a
    /// disk-less replica it transfers and holds far fewer bytes than a full
    /// `get_layer`.
    ///
    /// The ids must already be resolved in `head`'s numbering (e.g. via a layer's
    /// `value_triple_to_id`). Correct regardless of rollups: it consults the
    /// authoritative parent chain, whose per-layer additions/removals are the
    /// ground truth a rollup is only derived from. (Phase 3, Stage 1a; string
    /// resolution via selective dictionary loading is a later increment.)
    pub async fn selective_id_triple_exists(
        &self,
        head: [u32; 5],
        triple: IdTriple,
    ) -> io::Result<bool> {
        // `retrieve_layer_stack_names` returns the chain base-first; walk it
        // head-first so the newest layer that mentions the triple wins.
        let chain = self.layer_store.retrieve_layer_stack_names(head).await?;
        for &layer in chain.iter().rev() {
            if self
                .layer_store
                .triple_addition_exists(layer, triple.subject, triple.predicate, triple.object)
                .await?
            {
                return Ok(true);
            }
            if self
                .layer_store
                .triple_removal_exists(layer, triple.subject, triple.predicate, triple.object)
                .await?
            {
                return Ok(false);
            }
        }
        Ok(false)
    }

    /// Like [`selective_id_triple_exists`](Self::selective_id_triple_exists) but
    /// for a *string* triple: resolves the subject/predicate/object to ids by
    /// loading only the dictionaries and id-maps of the layers in the chain
    /// (never the adjacency-only structures are enough for the final existence
    /// walk). Nothing else is materialized, so on a disk-less replica this
    /// fetches only dictionaries + id-maps + adjacency — not object indexes or
    /// wavelet trees. Returns `false` if any string is absent from the graph.
    ///
    /// Mirrors `InternalLayer`'s resolution exactly (per-layer dict lookup, the
    /// layer's id-map `inner_to_outer`, the `+ node_dict_len` shift for values,
    /// and the cumulative parent count as the global offset), verified against
    /// the fully-materialized layer by a differential test. (Phase 3, Stage 1b.)
    pub async fn selective_value_triple_exists(
        &self,
        head: [u32; 5],
        triple: &ValueTriple,
    ) -> io::Result<bool> {
        use tdb_succinct::TypedDictEntry;

        // chain is base-first; compute the cumulative node+value and predicate
        // counts *below* each layer (the global-id offset for entries it owns).
        let chain = self.layer_store.retrieve_layer_stack_names(head).await?;
        let ls = &self.layer_store;
        let mut off_nv = Vec::with_capacity(chain.len());
        let mut off_pred = Vec::with_capacity(chain.len());
        let (mut cum_nv, mut cum_pred) = (0u64, 0u64);
        for &layer in &chain {
            off_nv.push(cum_nv);
            off_pred.push(cum_pred);
            cum_nv += ls.get_node_count(layer).await?.unwrap_or(0)
                + ls.get_value_count(layer).await?.unwrap_or(0);
            cum_pred += ls.get_predicate_count(layer).await?.unwrap_or(0);
        }

        // Resolve a node string (used for subjects and node objects).
        async fn resolve_node(
            ls: &Arc<dyn LayerStore>,
            chain: &[[u32; 5]],
            off_nv: &[u64],
            s: &str,
        ) -> io::Result<Option<u64>> {
            for i in (0..chain.len()).rev() {
                if let Some(dict) = ls.get_node_dictionary(chain[i]).await? {
                    if let Some(local) = dict.id(&s).into_option() {
                        let outer = match ls.get_node_value_idmap(chain[i]).await? {
                            Some(m) => m.inner_to_outer(local),
                            None => local,
                        };
                        return Ok(Some(outer + off_nv[i]));
                    }
                }
            }
            Ok(None)
        }

        let subject = match resolve_node(ls, &chain, &off_nv, &triple.subject).await? {
            Some(id) => id,
            None => return Ok(false),
        };

        // Resolve the predicate.
        let predicate = {
            let mut found = None;
            for i in (0..chain.len()).rev() {
                if let Some(dict) = ls.get_predicate_dictionary(chain[i]).await? {
                    let p: &str = &triple.predicate;
                    if let Some(local) = dict.id(&p).into_option() {
                        let outer = match ls.get_predicate_idmap(chain[i]).await? {
                            Some(m) => m.inner_to_outer(local),
                            None => local,
                        };
                        found = Some(outer + off_pred[i]);
                        break;
                    }
                }
            }
            match found {
                Some(id) => id,
                None => return Ok(false),
            }
        };

        // Resolve the object (node or typed value).
        let object = match &triple.object {
            ObjectType::Node(n) => match resolve_node(ls, &chain, &off_nv, n).await? {
                Some(id) => id,
                None => return Ok(false),
            },
            ObjectType::Value(v) => {
                let v: &TypedDictEntry = v;
                let mut found = None;
                for i in (0..chain.len()).rev() {
                    if let Some(vdict) = ls.get_value_dictionary(chain[i]).await? {
                        if let Some(local) = vdict.id_entry(v).into_option() {
                            // values live above this layer's nodes in the id-map's
                            // input space, hence the `+ node_dict_len` shift.
                            let node_len = ls.get_node_count(chain[i]).await?.unwrap_or(0);
                            let combined = local + node_len;
                            let outer = match ls.get_node_value_idmap(chain[i]).await? {
                                Some(m) => m.inner_to_outer(combined),
                                None => combined,
                            };
                            found = Some(outer + off_nv[i]);
                            break;
                        }
                    }
                }
                match found {
                    Some(id) => id,
                    None => return Ok(false),
                }
            }
        };

        self.selective_id_triple_exists(head, IdTriple::new(subject, predicate, object))
            .await
    }

    /// All id-triples with subject `subject` in the graph headed by `head`,
    /// loading only the adjacency structures of each layer (the disk-less
    /// traversal path). See [`selective_id_triple_exists`](Self::selective_id_triple_exists)
    /// for the correctness rationale. (Phase 3, Stage 1c.)
    pub async fn selective_id_triples_s(
        &self,
        head: [u32; 5],
        subject: u64,
    ) -> io::Result<Vec<IdTriple>> {
        let chain = self.layer_store.retrieve_layer_stack_names(head).await?;
        let mut per_layer = Vec::with_capacity(chain.len());
        for &layer in chain.iter().rev() {
            let adds = self.layer_store.triple_additions_s(layer, subject).await?;
            let removes = self.layer_store.triple_removals_s(layer, subject).await?;
            per_layer.push((adds.collect(), removes.collect()));
        }
        Ok(reconcile_layered(per_layer))
    }

    /// All id-triples with subject `subject` and predicate `predicate`.
    pub async fn selective_id_triples_sp(
        &self,
        head: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> io::Result<Vec<IdTriple>> {
        let chain = self.layer_store.retrieve_layer_stack_names(head).await?;
        let mut per_layer = Vec::with_capacity(chain.len());
        for &layer in chain.iter().rev() {
            let adds = self
                .layer_store
                .triple_additions_sp(layer, subject, predicate)
                .await?;
            let removes = self
                .layer_store
                .triple_removals_sp(layer, subject, predicate)
                .await?;
            per_layer.push((adds.collect(), removes.collect()));
        }
        Ok(reconcile_layered(per_layer))
    }

    /// All id-triples with predicate `predicate`.
    pub async fn selective_id_triples_p(
        &self,
        head: [u32; 5],
        predicate: u64,
    ) -> io::Result<Vec<IdTriple>> {
        let chain = self.layer_store.retrieve_layer_stack_names(head).await?;
        let mut per_layer = Vec::with_capacity(chain.len());
        for &layer in chain.iter().rev() {
            let adds = self
                .layer_store
                .triple_additions_p(layer, predicate)
                .await?;
            let removes = self.layer_store.triple_removals_p(layer, predicate).await?;
            per_layer.push((adds.collect(), removes.collect()));
        }
        Ok(reconcile_layered(per_layer))
    }

    /// All id-triples with object `object`.
    ///
    /// The per-layer object iterator seeks to the *nearest* object when the
    /// queried one is absent from a layer, and the cached path does not apply
    /// the exact-match filter the file path does, so we filter to the exact
    /// object here before reconciling.
    pub async fn selective_id_triples_o(
        &self,
        head: [u32; 5],
        object: u64,
    ) -> io::Result<Vec<IdTriple>> {
        let chain = self.layer_store.retrieve_layer_stack_names(head).await?;
        let mut per_layer = Vec::with_capacity(chain.len());
        for &layer in chain.iter().rev() {
            let adds: Vec<IdTriple> = self
                .layer_store
                .triple_additions_o(layer, object)
                .await?
                .filter(|t| t.object == object)
                .collect();
            let removes: Vec<IdTriple> = self
                .layer_store
                .triple_removals_o(layer, object)
                .await?
                .filter(|t| t.object == object)
                .collect();
            per_layer.push((adds, removes));
        }
        Ok(reconcile_layered(per_layer))
    }

    /// Spawn a background task that keeps read depth bounded: every `interval`
    /// it rolls up (non-destructively) any label head whose effective layer
    /// stack exceeds `max_depth`. Returns the task handle; abort it to stop.
    ///
    /// Rollup-only, never squash — every original layer is retained and the
    /// content-addressed parent chain stays walkable, so immutability and the
    /// per-commit audit trail are preserved.
    pub fn spawn_compaction(
        &self,
        max_depth: usize,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        compaction::CompactionManager::new(self.clone(), max_depth).spawn(interval)
    }

    /// Create a base layer builder, unattached to any database label.
    ///
    /// After having committed it, use `set_head` on a `NamedGraph` to attach it.
    pub async fn create_base_layer(&self) -> io::Result<StoreLayerBuilder> {
        StoreLayerBuilder::new(self.clone()).await
    }

    pub async fn merge_base_layers(
        &self,
        layers: &[[u32; 5]],
        temp_dir: &Path,
    ) -> io::Result<[u32; 5]> {
        self.layer_store.merge_base_layer(layers, temp_dir).await
    }

    /// Export the given layers by creating a pack, a Vec<u8> that can later be used with `import_layers` on a different store.
    pub async fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        self.layer_store.export_layers(layer_ids).await
    }

    /// Import the specified layers from the given pack, a byte slice that was previously generated with `export_layers`, on another store, and possibly even another machine).
    ///
    /// After this operation, the specified layers will be retrievable
    /// from this store, provided they existed in the pack. specified
    /// layers that are not in the pack are silently ignored.
    pub async fn import_layers<'a>(
        &'a self,
        pack: &'a [u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        self.layer_store.import_layers(pack, layer_ids).await
    }
}

/// Open a store that is entirely in memory.
///
/// This is useful for testing purposes, or if the database is only going to be used for caching purposes.
pub fn open_memory_store() -> Store {
    Store::new(
        MemoryLabelStore::new(),
        CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new()),
    )
}

/// Open a store that stores its data in the given directory as archive files.
///
/// cache_size specifies in megabytes how large the LRU cache should
/// be. Loaded layers will stick around in the LRU cache to speed up
/// subsequent loads.
pub fn open_archive_store<P: Into<PathBuf>>(path: P, cache_size: usize) -> Store {
    let p = path.into();
    let directory_archive_backend = DirectoryArchiveBackend::new(p.clone());
    let archive_backend = LruArchiveBackend::new(
        directory_archive_backend.clone(),
        directory_archive_backend,
        cache_size,
    );
    Store::new(
        DirectoryLabelStore::new(p),
        CachedLayerStore::new(
            ArchiveLayerStore::new(archive_backend.clone(), archive_backend),
            LockingHashMapLayerCache::new(),
        ),
    )
}

/// Open a store that stores its data in the given directory as archive files.
///
/// This version doesn't use lru caching.
pub fn open_raw_archive_store<P: Into<PathBuf>>(path: P) -> Store {
    let p = path.into();
    let archive_backend = DirectoryArchiveBackend::new(p.clone());
    Store::new(
        DirectoryLabelStore::new(p),
        CachedLayerStore::new(
            ArchiveLayerStore::new(archive_backend.clone(), archive_backend),
            LockingHashMapLayerCache::new(),
        ),
    )
}

/// Open a store that stores its data in the given directory.
pub fn open_directory_store<P: Into<PathBuf>>(path: P) -> Store {
    let p = path.into();
    Store::new(
        DirectoryLabelStore::new(p.clone()),
        CachedLayerStore::new(DirectoryLayerStore::new(p), LockingHashMapLayerCache::new()),
    )
}

/// Open a store backed by an S3-compatible object store.
///
/// Layers are stored as single archive objects and labels as compare-and-swap
/// objects, both under `prefix` in the given [`object_store::ObjectStore`]. The
/// bucket is the source of truth and the sole coordinator between replicas.
///
/// `cache_size` specifies, in megabytes, how large the in-memory LRU cache of
/// whole layer archives should be. Because layers are immutable, cached entries
/// are only ever evicted, never invalidated.
///
/// Develop and test against `object_store::memory::InMemory` or
/// `object_store::local::LocalFileSystem` for a network-free store; point it at
/// an `AmazonS3Builder`-built store (with an endpoint override for R2/MinIO) for
/// real object storage.
#[cfg(feature = "object-store")]
pub fn open_object_store(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: impl Into<String>,
    cache_size: usize,
) -> Store {
    use crate::storage::object::{ObjectArchiveBackend, ObjectLabelStore};
    let prefix = prefix.into();
    let object_backend = ObjectArchiveBackend::new(store.clone(), prefix.clone());
    let archive_backend =
        LruArchiveBackend::new(object_backend.clone(), object_backend, cache_size);
    Store::new(
        ObjectLabelStore::new(store, prefix),
        CachedLayerStore::new(
            ArchiveLayerStore::new(archive_backend.clone(), archive_backend),
            LockingHashMapLayerCache::new(),
        ),
    )
}

/// Like [`open_object_store`], but inserts a local-disk spill cache of whole
/// layer archives between the in-memory LRU and the network.
///
/// This gives a three-tier read path — bounded in-memory LRU → local disk →
/// object store — so a warm replica serves layers from local disk without a
/// round-trip, and a cold replica populates that disk cache as it reads. Because
/// layers are immutable, disk entries never need invalidation; manage the
/// directory's size out of band. The returned [`Store`] carries a handle to the
/// disk tier's [`CacheStats`](crate::storage::object_cache::CacheStatsSnapshot)
/// via the returned [`DiskSpillArchiveBackend`] for metrics.
#[cfg(feature = "object-store")]
pub fn open_object_store_with_cache(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: impl Into<String>,
    mem_cache_size: usize,
    disk_cache_dir: PathBuf,
) -> (
    Store,
    crate::storage::object_cache::DiskSpillArchiveBackend<
        crate::storage::object::ObjectArchiveBackend,
    >,
) {
    use crate::storage::object::{ObjectArchiveBackend, ObjectLabelStore};
    use crate::storage::object_cache::DiskSpillArchiveBackend;
    let prefix = prefix.into();
    let object_backend = ObjectArchiveBackend::new(store.clone(), prefix.clone());
    let disk_backend = DiskSpillArchiveBackend::new(object_backend.clone(), disk_cache_dir);
    // metadata from the origin; data flows in-memory LRU -> disk -> origin.
    let archive_backend =
        LruArchiveBackend::new(object_backend, disk_backend.clone(), mem_cache_size);
    let store = Store::new(
        ObjectLabelStore::new(store, prefix),
        CachedLayerStore::new(
            ArchiveLayerStore::new(archive_backend.clone(), archive_backend),
            LockingHashMapLayerCache::new(),
        ),
    );
    (store, disk_backend)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_and_manipulate_database(store: Store) {
        let database = store.create("foodb").await.unwrap();

        let head = database.head().await.unwrap();
        assert!(head.is_none());

        let mut builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().await.unwrap();
        assert!(database.set_head(&layer).await.unwrap());

        builder = layer.open_write().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"))
            .unwrap();

        let layer2 = builder.commit().await.unwrap();
        assert!(database.set_head(&layer2).await.unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().await.unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
    }

    #[tokio::test]
    async fn create_and_manipulate_memory_database() {
        let store = open_memory_store();

        create_and_manipulate_database(store).await;
    }

    #[tokio::test]
    async fn create_and_manipulate_directory_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());

        create_and_manipulate_database(store).await;
    }

    #[tokio::test]
    async fn create_and_manipulate_archive_database() {
        // Exercises the archive (.larch) path with the mmap-backed
        // DirectoryArchiveBackend + LRU.
        let dir = tempdir().unwrap();
        let store = open_archive_store(dir.path(), 100);

        create_and_manipulate_database(store).await;
    }

    #[tokio::test]
    async fn archive_database_reopens_from_disk() {
        // A fresh store over the same directory reads layers back via mmap.
        let dir = tempdir().unwrap();
        let name = {
            let store = open_archive_store(dir.path(), 100);
            let db = store.create("g").await.unwrap();
            let builder = store.create_base_layer().await.unwrap();
            builder
                .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
                .unwrap();
            let layer = builder.commit().await.unwrap();
            db.set_head(&layer).await.unwrap();
            layer.name()
        };
        let store = open_archive_store(dir.path(), 100);
        let layer = store.get_layer_from_id(name).await.unwrap().unwrap();
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn selective_id_triple_exists_matches_full_layer() {
        // Build a chain that exercises add, remove-of-a-base-triple, and re-add.
        let store = open_memory_store();
        let db = store.create("g").await.unwrap();

        let vs = |s, p, o| ValueTriple::new_string_value(s, p, o);
        let vn = |s, p, o| ValueTriple::new_node(s, p, o);

        let builder = store.create_base_layer().await.unwrap();
        builder.add_value_triple(vs("a", "p", "1")).unwrap();
        builder.add_value_triple(vs("b", "p", "2")).unwrap();
        builder.add_value_triple(vn("a", "links", "b")).unwrap();
        let mut layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();

        let builder = layer.open_write().await.unwrap();
        builder.add_value_triple(vs("c", "p", "3")).unwrap();
        builder.remove_value_triple(vs("a", "p", "1")).unwrap(); // remove a base triple
        layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();

        let builder = layer.open_write().await.unwrap();
        builder.add_value_triple(vs("a", "p", "1")).unwrap(); // re-add it
        layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();
        let head = layer.name();

        // Ground truth via the fully-materialized layer.
        let full = store.get_layer_from_id(head).await.unwrap().unwrap();

        // A mix of present and absent (but resolvable) triples.
        let candidates = [
            vs("a", "p", "1"),     // removed then re-added -> exists
            vs("b", "p", "2"),     // base -> exists
            vs("c", "p", "3"),     // added -> exists
            vn("a", "links", "b"), // node -> exists
            vs("a", "p", "2"),     // strings all exist, triple does not -> absent
            vs("b", "p", "3"),     // absent
        ];
        for cand in &candidates {
            let expected = full.value_triple_exists(cand);
            let idt = full
                .value_triple_to_id(cand)
                .expect("all strings in these candidates exist in the dictionary");
            let got = store.selective_id_triple_exists(head, idt).await.unwrap();
            assert_eq!(expected, got, "mismatch for {:?}", cand);
        }
    }

    #[tokio::test]
    async fn selective_value_triple_exists_matches_full_layer() {
        use tdb_succinct::TdbDataType;

        let store = open_memory_store();
        let db = store.create("g").await.unwrap();

        let vs = |s: &str, p: &str, o: &str| ValueTriple::new_string_value(s, p, o);
        let vn = |s: &str, p: &str, o: &str| ValueTriple::new_node(s, p, o);
        let vv = |s: &str, p: &str, i: i32| -> ValueTriple {
            ValueTriple::new_value(s, p, <i32 as TdbDataType>::make_entry(&i))
        };

        // base: nodes, string values, and typed (i32) values
        let builder = store.create_base_layer().await.unwrap();
        for i in 0..40 {
            builder
                .add_value_triple(vs(&format!("n{}", i), "p", &format!("str{}", i)))
                .unwrap();
            builder
                .add_value_triple(vn(&format!("n{}", i), "rel", &format!("n{}", (i + 1) % 40)))
                .unwrap();
            builder
                .add_value_triple(vv(&format!("n{}", i), "age", i))
                .unwrap();
        }
        let mut layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();

        // child: add more, remove some (nodes and typed values)
        let builder = layer.open_write().await.unwrap();
        for i in 40..60 {
            builder
                .add_value_triple(vs(&format!("n{}", i), "p", &format!("str{}", i)))
                .unwrap();
            builder
                .add_value_triple(vv(&format!("n{}", i), "age", i))
                .unwrap();
        }
        for i in 0..10 {
            builder
                .remove_value_triple(vv(&format!("n{}", i), "age", i))
                .unwrap();
            builder
                .remove_value_triple(vn(&format!("n{}", i), "rel", &format!("n{}", (i + 1) % 40)))
                .unwrap();
        }
        layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();

        // child2: re-add a removed one, add a fresh string
        let builder = layer.open_write().await.unwrap();
        builder.add_value_triple(vv("n0", "age", 0)).unwrap();
        builder.add_value_triple(vs("z", "zz", "zzz")).unwrap();
        layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();
        let head = layer.name();

        let full = store.get_layer_from_id(head).await.unwrap().unwrap();

        // Candidates: present + absent across all three object kinds.
        let mut candidates: Vec<ValueTriple> = Vec::new();
        for i in 0..65 {
            candidates.push(vs(&format!("n{}", i), "p", &format!("str{}", i)));
            candidates.push(vn(&format!("n{}", i), "rel", &format!("n{}", (i + 1) % 40)));
            candidates.push(vv(&format!("n{}", i), "age", i));
        }
        candidates.push(vs("n0", "p", "str1")); // resolvable, absent triple
        candidates.push(vv("n5", "age", 999)); // absent typed value
        candidates.push(vs("absent", "p", "x")); // absent subject
        candidates.push(vn("n0", "rel", "n1")); // removed -> absent
        candidates.push(vv("n0", "age", 0)); // removed then re-added -> present
        candidates.push(vs("z", "zz", "zzz")); // present (child2)

        for cand in &candidates {
            let expected = full.value_triple_exists(cand);
            let got = store
                .selective_value_triple_exists(head, cand)
                .await
                .unwrap();
            assert_eq!(
                expected, got,
                "mismatch for {:?} (expected {})",
                cand, expected
            );
        }
    }

    #[tokio::test]
    async fn selective_id_triples_iterators_match_full_layer() {
        use std::collections::HashSet;

        let store = open_memory_store();
        let db = store.create("g").await.unwrap();
        let vn = |s: &str, p: &str, o: &str| ValueTriple::new_node(s, p, o);

        // base
        let builder = store.create_base_layer().await.unwrap();
        for i in 0..30 {
            for p in 0..3 {
                builder
                    .add_value_triple(vn(
                        &format!("s{}", i),
                        &format!("p{}", p),
                        &format!("s{}", (i + p + 1) % 30),
                    ))
                    .unwrap();
            }
        }
        let mut layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();

        // child: add + remove
        let builder = layer.open_write().await.unwrap();
        for i in 30..40 {
            builder
                .add_value_triple(vn(&format!("s{}", i), "p0", "s0"))
                .unwrap();
        }
        for i in 0..10 {
            builder
                .remove_value_triple(vn(&format!("s{}", i), "p1", &format!("s{}", (i + 2) % 30)))
                .unwrap();
        }
        layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();

        // child2: re-add one removed
        let builder = layer.open_write().await.unwrap();
        builder.add_value_triple(vn("s0", "p1", "s2")).unwrap();
        layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();
        let head = layer.name();

        let full = store.get_layer_from_id(head).await.unwrap().unwrap();
        let all: Vec<IdTriple> = full.triples().collect();
        let subjects: HashSet<u64> = all.iter().map(|t| t.subject).collect();
        let predicates: HashSet<u64> = all.iter().map(|t| t.predicate).collect();
        let objects: HashSet<u64> = all.iter().map(|t| t.object).collect();

        let sorted = |it: Box<dyn Iterator<Item = IdTriple> + Send>| {
            let mut v: Vec<IdTriple> = it.collect();
            v.sort();
            v.dedup();
            v
        };

        for &s in &subjects {
            assert_eq!(
                sorted(full.triples_s(s)),
                store.selective_id_triples_s(head, s).await.unwrap(),
                "triples_s({})",
                s
            );
        }
        for &p in &predicates {
            assert_eq!(
                sorted(full.triples_p(p)),
                store.selective_id_triples_p(head, p).await.unwrap(),
                "triples_p({})",
                p
            );
        }
        for &o in &objects {
            assert_eq!(
                sorted(full.triples_o(o)),
                store.selective_id_triples_o(head, o).await.unwrap(),
                "triples_o({})",
                o
            );
        }
        for t in all.iter().take(25) {
            assert_eq!(
                sorted(full.triples_sp(t.subject, t.predicate)),
                store
                    .selective_id_triples_sp(head, t.subject, t.predicate)
                    .await
                    .unwrap(),
                "triples_sp({},{})",
                t.subject,
                t.predicate
            );
        }
    }

    #[tokio::test]
    async fn selective_value_triple_exists_matches_full_layer_randomized() {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        use tdb_succinct::TdbDataType;

        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let store = open_memory_store();
        let db = store.create("g").await.unwrap();

        // Build a random multi-layer graph, remembering every triple ever added
        // so we can use them (plus random absent ones) as candidates.
        let mut candidates: Vec<ValueTriple> = Vec::new();
        let mk = |kind: u8, a: u32, b: u32, rng: &mut StdRng| -> ValueTriple {
            let s = format!("s{}", a % 40);
            let p = format!("p{}", b % 6);
            match kind % 3 {
                0 => ValueTriple::new_node(&s, &p, &format!("s{}", rng.gen_range(0..40))),
                1 => ValueTriple::new_string_value(&s, &p, &format!("v{}", rng.gen_range(0..50))),
                _ => ValueTriple::new_value(
                    &s,
                    &p,
                    <i32 as TdbDataType>::make_entry(&(rng.gen_range(0..100) as i32)),
                ),
            }
        };

        let mut head = None;
        for _layer in 0..5 {
            let builder = match head {
                None => store.create_base_layer().await.unwrap(),
                Some(h) => store
                    .get_layer_from_id(h)
                    .await
                    .unwrap()
                    .unwrap()
                    .open_write()
                    .await
                    .unwrap(),
            };
            for _ in 0..30 {
                let t = mk(rng.gen(), rng.gen(), rng.gen(), &mut rng);
                builder.add_value_triple(t.clone()).unwrap();
                candidates.push(t);
            }
            // remove some previously-added triples
            for _ in 0..8 {
                if let Some(t) = candidates.get(rng.gen_range(0..candidates.len())).cloned() {
                    builder.remove_value_triple(t).unwrap();
                }
            }
            let layer = builder.commit().await.unwrap();
            db.set_head(&layer).await.unwrap();
            head = Some(layer.name());
        }
        let head = head.unwrap();
        let full = store.get_layer_from_id(head).await.unwrap().unwrap();

        // add some certainly-absent candidates
        for _ in 0..30 {
            candidates.push(ValueTriple::new_string_value(
                &format!("s{}", rng.gen_range(0..40)),
                &format!("p{}", rng.gen_range(0..6)),
                &format!("absent{}", rng.gen_range(0..1000)),
            ));
        }

        for cand in &candidates {
            let expected = full.value_triple_exists(cand);
            let got = store
                .selective_value_triple_exists(head, cand)
                .await
                .unwrap();
            assert_eq!(
                expected, got,
                "mismatch for {:?} (expected {})",
                cand, expected
            );
        }
    }

    #[tokio::test]
    async fn spawn_compaction_bounds_depth_in_background() {
        let store = open_memory_store();
        let db = store.create("g").await.unwrap();

        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "p", "1"))
            .unwrap();
        let mut layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();
        for i in 1..5 {
            let builder = layer.open_write().await.unwrap();
            builder
                .add_value_triple(ValueTriple::new_string_value(&format!("k{}", i), "p", "v"))
                .unwrap();
            layer = builder.commit().await.unwrap();
            db.set_head(&layer).await.unwrap();
        }
        let head_name = layer.name();

        let handle = store.spawn_compaction(2, std::time::Duration::from_millis(20));

        // wait (bounded) for a background tick to roll the deep head up
        let mut rolled = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let internal = store
                .layer_store
                .get_layer(head_name)
                .await
                .unwrap()
                .unwrap();
            if internal.is_rollup() {
                rolled = true;
                break;
            }
        }
        handle.abort();
        assert!(
            rolled,
            "background compaction should have rolled up the deep head"
        );
    }

    #[tokio::test]
    async fn create_layer_and_retrieve_it_by_id() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().await.unwrap();

        let id = layer.name();

        let layer2 = store.get_layer_from_id(id).await.unwrap().unwrap();

        assert!(layer2.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn commit_builder_makes_builder_committed() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();

        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        assert!(!builder.committed());

        builder.commit_no_load().await.unwrap();

        assert!(builder.committed());
    }

    #[tokio::test]
    async fn hard_reset() {
        let store = open_memory_store();
        let database = store.create("foodb").await.unwrap();

        let builder1 = store.create_base_layer().await.unwrap();
        builder1
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        let layer1 = builder1.commit().await.unwrap();

        assert!(database.set_head(&layer1).await.unwrap());

        let builder2 = store.create_base_layer().await.unwrap();
        builder2
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();

        let layer2 = builder2.commit().await.unwrap();

        database.force_set_head(&layer2).await.unwrap();

        let new_layer = database.head().await.unwrap().unwrap();

        assert!(
            new_layer.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack"))
        );
        assert!(
            !new_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }

    #[tokio::test]
    async fn create_two_layers_and_squash() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        let layer = builder.commit().await.unwrap();

        let builder2 = layer.open_write().await.unwrap();

        builder2
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();

        builder2
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "sniff"))
            .unwrap();

        builder2
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        builder2
            .remove_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        builder2
            .add_value_triple(ValueTriple::new_node("bunny", "likes", "cow"))
            .unwrap();

        builder2
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();

        let layer2 = builder2.commit().await.unwrap();

        let new = layer2.squash().await.unwrap();
        let triples: Vec<_> = new
            .triples()
            .map(|t| new.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_string_value("dog", "says", "woof"),
            ],
            triples
        );

        assert!(new.parent().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_three_layers_and_squash_last_two() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        let base_layer = builder.commit().await.unwrap();

        let builder = base_layer.open_write().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("bunny", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("duck", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();

        let intermediate_layer = builder.commit().await.unwrap();
        let builder = intermediate_layer.open_write().await.unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "sniff"))
            .unwrap();
        let final_layer = builder.commit().await.unwrap();

        let squashed_layer = final_layer.squash_upto(&base_layer).await.unwrap();
        assert_eq!(squashed_layer.parent_name().unwrap(), base_layer.name());
        let additions: Vec<_> = squashed_layer
            .triple_additions()
            .await
            .unwrap()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
            ],
            additions
        );
        let removals: Vec<_> = squashed_layer
            .triple_removals()
            .await
            .unwrap()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "hates", "duck"),
                ValueTriple::new_string_value("cow", "says", "quack"),
            ],
            removals
        );

        let all_triples: Vec<_> = squashed_layer
            .triples()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("cow", "likes", "horse"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
            ],
            all_triples
        );
    }

    #[tokio::test]
    async fn create_three_layers_and_squash_all_after_rollup() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        let base_layer = builder.commit().await.unwrap();

        let builder = base_layer.open_write().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("bunny", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("duck", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();

        let intermediate_layer = builder.commit().await.unwrap();
        let builder = intermediate_layer.open_write().await.unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "sniff"))
            .unwrap();
        let final_layer = builder.commit().await.unwrap();
        final_layer.rollup_upto(&base_layer).await.unwrap();
        let final_rolled_layer = store
            .get_layer_from_id(final_layer.name())
            .await
            .unwrap()
            .unwrap();

        let squashed_layer = final_rolled_layer.squash().await.unwrap();
        assert!(squashed_layer.parent_name().is_none());

        let all_triples: Vec<_> = squashed_layer
            .triples()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("cow", "likes", "horse"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
            ],
            all_triples
        );
    }

    #[tokio::test]
    async fn create_three_layers_and_squash_last_two_after_rollup() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        let base_layer = builder.commit().await.unwrap();

        let builder = base_layer.open_write().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("bunny", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("duck", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();

        let intermediate_layer = builder.commit().await.unwrap();
        let builder = intermediate_layer.open_write().await.unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "sniff"))
            .unwrap();
        let final_layer = builder.commit().await.unwrap();
        final_layer.rollup_upto(&base_layer).await.unwrap();
        let final_rolled_layer = store
            .get_layer_from_id(final_layer.name())
            .await
            .unwrap()
            .unwrap();

        let squashed_layer = final_rolled_layer.squash_upto(&base_layer).await.unwrap();
        assert_eq!(squashed_layer.parent_name().unwrap(), base_layer.name());
        let additions: Vec<_> = squashed_layer
            .triple_additions()
            .await
            .unwrap()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
            ],
            additions
        );
        let removals: Vec<_> = squashed_layer
            .triple_removals()
            .await
            .unwrap()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "hates", "duck"),
                ValueTriple::new_string_value("cow", "says", "quack"),
            ],
            removals
        );

        let all_triples: Vec<_> = squashed_layer
            .triples()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("cow", "likes", "horse"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
            ],
            all_triples
        );
    }

    #[tokio::test]
    async fn squash_and_forget_dict_entries() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("a", "b", "anode"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "b", "astring"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("a", "c", "anothernode"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "c", "anotherstring"))
            .unwrap();

        let base_layer = builder.commit().await.unwrap();

        let builder = base_layer.open_write().await.unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("a", "c", "anothernode"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("a", "c", "anotherstring"))
            .unwrap();
        let child_layer = builder.commit().await.unwrap();

        let squashed = child_layer.squash().await.unwrap();
        // annoyingly we need to get the internal layer version, so lets re-retrieve
        let squashed = store
            .layer_store
            .get_layer(squashed.name())
            .await
            .unwrap()
            .unwrap();
        let nodes: Vec<_> = squashed
            .node_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"a" as &[u8], b"anode"], nodes);
        let preds: Vec<_> = squashed
            .predicate_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"b" as &[u8]], preds);
        let vals: Vec<_> = squashed
            .value_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"astring" as &[u8]], vals);

        let all_triples: Vec<_> = squashed
            .triples()
            .map(|t| squashed.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("a", "b", "anode"),
                ValueTriple::new_string_value("a", "b", "astring"),
            ],
            all_triples
        );
    }

    #[tokio::test]
    async fn squash_upto_and_forget_dict_entries() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("foo", "bar", "baz"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("baz", "bar", "quux"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("foo", "baz", "hai"))
            .unwrap();
        let base_layer = builder.commit().await.unwrap();
        let builder = base_layer.open_write().await.unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("foo", "baz", "hai"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("a", "b", "anode"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "b", "astring"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("a", "c", "anothernode"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "c", "anotherstring"))
            .unwrap();

        let child_layer1 = builder.commit().await.unwrap();

        let builder = child_layer1.open_write().await.unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("foo", "bar", "baz"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("a", "c", "anothernode"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("a", "c", "anotherstring"))
            .unwrap();
        let child_layer2 = builder.commit().await.unwrap();

        let squashed = child_layer2.squash_upto(&base_layer).await.unwrap();
        // annoyingly we need to get the internal layer version, so lets re-retrieve
        let squashed = store
            .layer_store
            .get_layer(squashed.name())
            .await
            .unwrap()
            .unwrap();
        let nodes: Vec<_> = squashed
            .node_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"a" as &[u8], b"anode"], nodes);
        let preds: Vec<_> = squashed
            .predicate_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"b" as &[u8]], preds);
        let vals: Vec<_> = squashed
            .value_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"astring" as &[u8]], vals);

        let all_triple_additions: Vec<_> = squashed
            .internal_triple_additions()
            .map(|t| squashed.id_triple_to_string(&t).unwrap())
            .collect();
        let all_triple_removals: Vec<_> = squashed
            .internal_triple_removals()
            .map(|t| squashed.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("a", "b", "anode"),
                ValueTriple::new_string_value("a", "b", "astring"),
            ],
            all_triple_additions
        );
        assert_eq!(
            vec![
                ValueTriple::new_node("foo", "bar", "baz"),
                ValueTriple::new_string_value("foo", "baz", "hai"),
            ],
            all_triple_removals
        );
    }

    #[tokio::test]
    async fn apply_a_base_delta() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();

        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().await.unwrap();

        let builder2 = layer.open_write().await.unwrap();

        builder2
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();

        let layer2 = builder2.commit().await.unwrap();

        let delta_builder_1 = store.create_base_layer().await.unwrap();

        delta_builder_1
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        delta_builder_1
            .add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();

        let delta_1 = delta_builder_1.commit().await.unwrap();

        let delta_builder_2 = delta_1.open_write().await.unwrap();

        delta_builder_2
            .add_value_triple(ValueTriple::new_string_value("crow", "says", "caw"))
            .unwrap();
        delta_builder_2
            .remove_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();

        let delta = delta_builder_2.commit().await.unwrap();

        let rebase_builder = layer2.open_write().await.unwrap();

        let _ = rebase_builder.apply_delta(&delta).await.unwrap();

        let rebase_layer = rebase_builder.commit().await.unwrap();

        assert!(
            rebase_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rebase_layer.value_triple_exists(&ValueTriple::new_string_value("crow", "says", "caw"))
        );
        assert!(
            rebase_layer.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof"))
        );
        assert!(!rebase_layer
            .value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow")));
    }

    async fn cached_layer_name_does_not_change_after_rollup(store: Store) {
        let builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();
        let x = builder.commit().await.unwrap();
        let builder = x.open_write().await.unwrap();
        let child_name = builder.name();
        builder.commit().await.unwrap();

        let unrolled_layer = store.get_layer_from_id(child_name).await.unwrap().unwrap();
        let unrolled_name = unrolled_layer.name();
        let unrolled_parent_name = unrolled_layer.parent_name().unwrap();
        assert_eq!(child_name, unrolled_name);
        assert_eq!(base_name, unrolled_parent_name);

        unrolled_layer.rollup().await.unwrap();
        let rolled_layer = store.get_layer_from_id(child_name).await.unwrap().unwrap();
        let rolled_name = rolled_layer.name();
        let rolled_parent_name = rolled_layer.parent_name().unwrap();
        assert_eq!(child_name, rolled_name);
        assert_eq!(base_name, rolled_parent_name);

        rolled_layer.rollup().await.unwrap();
        let rolled_layer2 = store.get_layer_from_id(child_name).await.unwrap().unwrap();
        let rolled_name2 = rolled_layer2.name();
        let rolled_parent_name2 = rolled_layer2.parent_name().unwrap();
        assert_eq!(child_name, rolled_name2);
        assert_eq!(base_name, rolled_parent_name2);
    }

    #[tokio::test]
    async fn mem_cached_layer_name_does_not_change_after_rollup() {
        let store = open_memory_store();

        cached_layer_name_does_not_change_after_rollup(store).await
    }

    #[tokio::test]
    async fn dir_cached_layer_name_does_not_change_after_rollup() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());

        cached_layer_name_does_not_change_after_rollup(store).await
    }

    async fn cached_layer_name_does_not_change_after_rollup_upto(store: Store) {
        let builder = store.create_base_layer().await.unwrap();
        let _base_name = builder.name();
        let base_layer = builder.commit().await.unwrap();
        let builder = base_layer.open_write().await.unwrap();
        let child_name = builder.name();
        let x = builder.commit().await.unwrap();
        let builder = x.open_write().await.unwrap();
        let child_name2 = builder.name();
        builder.commit().await.unwrap();

        let unrolled_layer = store.get_layer_from_id(child_name2).await.unwrap().unwrap();
        let unrolled_name = unrolled_layer.name();
        let unrolled_parent_name = unrolled_layer.parent_name().unwrap();
        assert_eq!(child_name2, unrolled_name);
        assert_eq!(child_name, unrolled_parent_name);

        unrolled_layer.rollup_upto(&base_layer).await.unwrap();
        let rolled_layer = store.get_layer_from_id(child_name2).await.unwrap().unwrap();
        let rolled_name = rolled_layer.name();
        let rolled_parent_name = rolled_layer.parent_name().unwrap();
        assert_eq!(child_name2, rolled_name);
        assert_eq!(child_name, rolled_parent_name);

        rolled_layer.rollup_upto(&base_layer).await.unwrap();
        let rolled_layer2 = store.get_layer_from_id(child_name2).await.unwrap().unwrap();
        let rolled_name2 = rolled_layer2.name();
        let rolled_parent_name2 = rolled_layer2.parent_name().unwrap();
        assert_eq!(child_name2, rolled_name2);
        assert_eq!(child_name, rolled_parent_name2);
    }

    #[tokio::test]
    async fn mem_cached_layer_name_does_not_change_after_rollup_upto() {
        let store = open_memory_store();
        cached_layer_name_does_not_change_after_rollup_upto(store).await
    }

    #[tokio::test]
    async fn dir_cached_layer_name_does_not_change_after_rollup_upto() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        cached_layer_name_does_not_change_after_rollup_upto(store).await
    }

    #[tokio::test]
    async fn force_update_with_matching_0_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").await.unwrap();
        let (layer, version) = graph.head_version().await.unwrap();
        assert!(layer.is_none());
        assert_eq!(0, version);

        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();

        assert!(graph.force_set_head_version(&layer, 0).await.unwrap());
    }

    #[tokio::test]
    async fn force_update_with_mismatching_0_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").await.unwrap();
        let (layer, version) = graph.head_version().await.unwrap();
        assert!(layer.is_none());
        assert_eq!(0, version);

        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();

        assert!(!graph.force_set_head_version(&layer, 3).await.unwrap());
    }

    #[tokio::test]
    async fn force_update_with_matching_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").await.unwrap();

        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();
        assert!(graph.set_head(&layer).await.unwrap());

        let (_, version) = graph.head_version().await.unwrap();
        assert_eq!(1, version);

        let builder2 = store.create_base_layer().await.unwrap();
        let layer2 = builder2.commit().await.unwrap();

        assert!(graph.force_set_head_version(&layer2, 1).await.unwrap());
    }

    #[tokio::test]
    async fn force_update_with_mismatched_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").await.unwrap();

        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();
        assert!(graph.set_head(&layer).await.unwrap());

        let (_, version) = graph.head_version().await.unwrap();
        assert_eq!(1, version);

        let builder2 = store.create_base_layer().await.unwrap();
        let layer2 = builder2.commit().await.unwrap();

        assert!(!graph.force_set_head_version(&layer2, 0).await.unwrap());
    }

    #[tokio::test]
    async fn delete_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let _ = store.create("foo").await.unwrap();
        assert!(store.delete("foo").await.unwrap());
        assert!(store.open("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        assert!(!store.delete("foo").await.unwrap());
    }

    #[tokio::test]
    async fn delete_graph() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").await.unwrap();
        assert!(store.open("foo").await.unwrap().is_some());
        graph.delete().await.unwrap();
        assert!(store.open("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recreate_graph() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").await.unwrap();
        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();
        graph.set_head(&layer).await.unwrap();
        assert!(graph.head().await.unwrap().is_some());
        graph.delete().await.unwrap();
        store.create("foo").await.unwrap();
        assert!(graph.head().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_databases() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        assert!(store.labels().await.unwrap().is_empty());
        let _ = store.create("foo").await.unwrap();
        let one = vec!["foo".to_string()];
        assert_eq!(store.labels().await.unwrap(), one);
        let _ = store.create("bar").await.unwrap();
        let two = vec!["bar".to_string(), "foo".to_string()];
        let mut left = store.labels().await.unwrap();
        left.sort();
        assert_eq!(left, two);
    }
}
