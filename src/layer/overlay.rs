//! In-memory overlay layer for group commit (Phase 2).
//!
//! An [`OverlayLayer`] presents a base layer plus a set of un-flushed value-level
//! commits as a single queryable [`Layer`], entirely in RAM and without touching
//! storage. It lets a buffering writer read its own un-committed changes (the
//! LSM-memtable analog) while many logical commits are batched into one physical
//! layer at flush time.
//!
//! It is never persisted. On flush the accumulated value-level operations are
//! replayed, in order, into a real layer via the normal builder path, so the
//! provisional ids the overlay assigns never escape into a stored layer.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tdb_succinct::TypedDictEntry;

use crate::layer::{IdTriple, Layer, LayerCounts, ObjectType, ValueTriple};

/// A value-level operation buffered on top of the base layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverlayOp {
    Add(ValueTriple),
    Remove(ValueTriple),
}

/// A base layer plus un-flushed value-level commits, queryable as one `Layer`.
#[derive(Clone)]
pub struct OverlayLayer {
    name: [u32; 5],
    parent: Arc<dyn Layer>,

    // provisional dictionaries for entries not present in the parent
    nodes_values: HashMap<ObjectType, u64>,
    id_to_node_value: HashMap<u64, ObjectType>,
    predicates: HashMap<String, u64>,
    id_to_predicate: HashMap<u64, String>,
    nv_count: u64,
    pred_count: u64,
    new_node_count: usize,
    new_value_count: usize,
    new_predicate_count: usize,

    // resolved id-level view (with parent reconciliation applied)
    additions: HashSet<IdTriple>,
    removals: HashSet<IdTriple>,

    /// Ordered value-level ops, for WAL persistence and flush replay.
    ops: Vec<OverlayOp>,
}

impl OverlayLayer {
    /// Create an empty overlay on top of `parent`.
    pub fn new(parent: Arc<dyn Layer>) -> Self {
        let nv_count = parent.node_and_value_count() as u64;
        let pred_count = parent.predicate_count() as u64;
        Self {
            name: rand::random(),
            parent,
            nodes_values: HashMap::new(),
            id_to_node_value: HashMap::new(),
            predicates: HashMap::new(),
            id_to_predicate: HashMap::new(),
            nv_count,
            pred_count,
            new_node_count: 0,
            new_value_count: 0,
            new_predicate_count: 0,
            additions: HashSet::new(),
            removals: HashSet::new(),
            ops: Vec::new(),
        }
    }

    /// The number of buffered value-level operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// The ordered value-level operations, for replay at flush time.
    pub fn ops(&self) -> &[OverlayOp] {
        &self.ops
    }

    fn parent_has(&self, t: IdTriple) -> bool {
        self.parent.triple_exists(t.subject, t.predicate, t.object)
    }

    fn alloc_node_value(&mut self, entry: ObjectType, is_value: bool) -> u64 {
        if let Some(id) = self.nodes_values.get(&entry) {
            return *id;
        }
        self.nv_count += 1;
        if is_value {
            self.new_value_count += 1;
        } else {
            self.new_node_count += 1;
        }
        let id = self.nv_count;
        self.nodes_values.insert(entry.clone(), id);
        self.id_to_node_value.insert(id, entry);
        id
    }

    fn alloc_predicate(&mut self, predicate: &str) -> u64 {
        if let Some(id) = self.predicates.get(predicate) {
            return *id;
        }
        self.pred_count += 1;
        self.new_predicate_count += 1;
        let id = self.pred_count;
        self.predicates.insert(predicate.to_string(), id);
        self.id_to_predicate.insert(id, predicate.to_string());
        id
    }

    /// Resolve (allocating provisional ids where needed) a value triple to ids,
    /// mirroring `SimpleLayerBuilder::calculate_triple`.
    fn calculate_triple(&mut self, triple: &ValueTriple) -> IdTriple {
        let subject_id = self.parent.subject_id(&triple.subject).unwrap_or_else(|| {
            self.alloc_node_value(ObjectType::Node(triple.subject.clone()), false)
        });
        let predicate_id = self
            .parent
            .predicate_id(&triple.predicate)
            .unwrap_or_else(|| self.alloc_predicate(&triple.predicate));
        let object_id = match &triple.object {
            ObjectType::Node(n) => self
                .parent
                .object_node_id(n)
                .unwrap_or_else(|| self.alloc_node_value(ObjectType::Node(n.clone()), false)),
            ObjectType::Value(v) => self
                .parent
                .object_value_id(v)
                .unwrap_or_else(|| self.alloc_node_value(ObjectType::Value(v.clone()), true)),
        };
        IdTriple::new(subject_id, predicate_id, object_id)
    }

    /// Apply a value-level addition, with the same no-op reconciliation the
    /// builder performs (adding an existing triple is a no-op; re-adding a
    /// removed triple cancels the removal).
    pub fn add(&mut self, triple: ValueTriple) {
        let id = self.calculate_triple(&triple);
        self.ops.push(OverlayOp::Add(triple));
        if self.removals.remove(&id) {
            // was pending-removed; re-adding restores it
        } else if !self.parent_has(id) {
            self.additions.insert(id);
        }
        // else: already present in the parent -> no-op
    }

    /// Apply a value-level removal, with builder-matching reconciliation.
    pub fn remove(&mut self, triple: ValueTriple) {
        let id = self.calculate_triple(&triple);
        self.ops.push(OverlayOp::Remove(triple));
        if self.additions.remove(&id) {
            // was a pending addition; removing cancels it
        } else if self.parent_has(id) {
            self.removals.insert(id);
        }
        // else: not present -> no-op
    }

    fn sorted_additions(&self) -> Vec<IdTriple> {
        let mut v: Vec<IdTriple> = self.additions.iter().copied().collect();
        v.sort();
        v
    }

    /// Buffered additions whose object value lies in `[low, high)`, in ascending
    /// value order.
    ///
    /// The bound test is done on the value rather than on the object id: an
    /// addition may carry a *provisional* id that the overlay minted for a value
    /// the parent does not have, and those ids are allocated in insertion order,
    /// so they say nothing about where the value sorts.
    fn additions_in_value_range(
        &self,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
    ) -> Vec<IdTriple> {
        let mut matched: Vec<(TypedDictEntry, IdTriple)> = self
            .additions
            .iter()
            .filter_map(|t| match self.id_object(t.object) {
                Some(ObjectType::Value(v))
                    if v.datatype() == low.datatype() && v >= *low && v < *high =>
                {
                    Some((v, *t))
                }
                _ => None,
            })
            .collect();
        matched.sort();
        matched.into_iter().map(|(_, t)| t).collect()
    }
}

impl Layer for OverlayLayer {
    fn name(&self) -> [u32; 5] {
        self.name
    }
    fn parent_name(&self) -> Option<[u32; 5]> {
        Some(self.parent.name())
    }

    fn node_and_value_count(&self) -> usize {
        self.nv_count as usize
    }
    fn predicate_count(&self) -> usize {
        self.pred_count as usize
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        self.parent.subject_id(subject).or_else(|| {
            self.nodes_values
                .get(&ObjectType::Node(subject.to_string()))
                .copied()
        })
    }
    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.parent
            .predicate_id(predicate)
            .or_else(|| self.predicates.get(predicate).copied())
    }
    fn object_node_id(&self, object: &str) -> Option<u64> {
        self.parent.object_node_id(object).or_else(|| {
            self.nodes_values
                .get(&ObjectType::Node(object.to_string()))
                .copied()
        })
    }
    fn object_value_id(&self, object: &TypedDictEntry) -> Option<u64> {
        self.parent.object_value_id(object).or_else(|| {
            self.nodes_values
                .get(&ObjectType::Value(object.clone()))
                .copied()
        })
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        if id <= self.parent.node_and_value_count() as u64 {
            self.parent.id_subject(id)
        } else {
            match self.id_to_node_value.get(&id) {
                Some(ObjectType::Node(n)) => Some(n.clone()),
                _ => None,
            }
        }
    }
    fn id_predicate(&self, id: u64) -> Option<String> {
        if id <= self.parent.predicate_count() as u64 {
            self.parent.id_predicate(id)
        } else {
            self.id_to_predicate.get(&id).cloned()
        }
    }
    fn id_object(&self, id: u64) -> Option<ObjectType> {
        if id <= self.parent.node_and_value_count() as u64 {
            self.parent.id_object(id)
        } else {
            self.id_to_node_value.get(&id).cloned()
        }
    }
    fn id_object_is_node(&self, id: u64) -> Option<bool> {
        if id <= self.parent.node_and_value_count() as u64 {
            self.parent.id_object_is_node(id)
        } else {
            match self.id_to_node_value.get(&id) {
                Some(ObjectType::Node(_)) => Some(true),
                Some(ObjectType::Value(_)) => Some(false),
                None => None,
            }
        }
    }

    fn all_counts(&self) -> LayerCounts {
        let pc = self.parent.all_counts();
        LayerCounts {
            node_count: pc.node_count + self.new_node_count,
            predicate_count: pc.predicate_count + self.new_predicate_count,
            value_count: pc.value_count + self.new_value_count,
        }
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        let t = IdTriple::new(subject, predicate, object);
        if self.additions.contains(&t) {
            return true;
        }
        self.parent.triple_exists(subject, predicate, object) && !self.removals.contains(&t)
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let removals = self.removals.clone();
        let additions = self.sorted_additions();
        Box::new(
            self.parent
                .triples()
                .filter(move |t| !removals.contains(t))
                .chain(additions),
        )
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let removals = self.removals.clone();
        let additions: Vec<IdTriple> = self
            .sorted_additions()
            .into_iter()
            .filter(|t| t.subject == subject)
            .collect();
        Box::new(
            self.parent
                .triples_s(subject)
                .filter(move |t| !removals.contains(t))
                .chain(additions),
        )
    }

    fn triples_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let removals = self.removals.clone();
        let additions: Vec<IdTriple> = self
            .sorted_additions()
            .into_iter()
            .filter(|t| t.subject == subject && t.predicate == predicate)
            .collect();
        Box::new(
            self.parent
                .triples_sp(subject, predicate)
                .filter(move |t| !removals.contains(t))
                .chain(additions),
        )
    }

    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let removals = self.removals.clone();
        let additions: Vec<IdTriple> = self
            .sorted_additions()
            .into_iter()
            .filter(|t| t.predicate == predicate)
            .collect();
        Box::new(
            self.parent
                .triples_p(predicate)
                .filter(move |t| !removals.contains(t))
                .chain(additions),
        )
    }

    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let removals = self.removals.clone();
        let additions: Vec<IdTriple> = self
            .sorted_additions()
            .into_iter()
            .filter(|t| t.object == object)
            .collect();
        Box::new(
            self.parent
                .triples_o(object)
                .filter(move |t| !removals.contains(t))
                .chain(additions),
        )
    }

    fn triples_value_range(
        &self,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let removals = self.removals.clone();
        let additions = self.additions_in_value_range(low, high);
        Box::new(
            self.parent
                .triples_value_range(low, high)
                .filter(move |t| !removals.contains(t))
                .chain(additions),
        )
    }

    fn triples_value_range_rev(
        &self,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let removals = self.removals.clone();
        let mut additions = self.additions_in_value_range(low, high);
        additions.reverse();
        Box::new(
            self.parent
                .triples_value_range_rev(low, high)
                .filter(move |t| !removals.contains(t))
                .chain(additions),
        )
    }

    fn triple_addition_count(&self) -> usize {
        self.parent.triple_addition_count() + self.additions.len()
    }
    fn triple_removal_count(&self) -> usize {
        self.parent.triple_removal_count() + self.removals.len()
    }

    fn single_triple_sp(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        self.triples_sp(subject, predicate).next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryLayerStore;
    use crate::storage::{CachedLayerStore, LayerStore, LockingHashMapLayerCache};
    use tdb_succinct::TdbDataType;

    // Build a real committed child from `ops` on top of `base`, for comparison.
    async fn real_child(
        store: &CachedLayerStore,
        base: [u32; 5],
        ops: &[OverlayOp],
    ) -> Arc<crate::layer::InternalLayer> {
        let mut builder = store.create_child_layer(base).await.unwrap();
        let name = builder.name();
        for op in ops {
            match op {
                OverlayOp::Add(t) => builder.add_value_triple(t.clone()),
                OverlayOp::Remove(t) => builder.remove_value_triple(t.clone()),
            }
        }
        builder.commit_boxed().await.unwrap();
        store.get_layer(name).await.unwrap().unwrap()
    }

    fn vt(s: &str, p: &str, o: &str) -> ValueTriple {
        ValueTriple::new_string_value(s, p, o)
    }
    fn vn(s: &str, p: &str, o: &str) -> ValueTriple {
        ValueTriple::new_node(s, p, o)
    }

    // Assert the overlay and a real child answer string-level queries identically.
    fn assert_equivalent(overlay: &OverlayLayer, real: &dyn Layer) {
        // triple sets (compared as strings, since ids differ between the two)
        let mut o: Vec<ValueTriple> = overlay
            .triples()
            .map(|t| overlay.id_triple_to_string(&t).unwrap())
            .collect();
        let mut r: Vec<ValueTriple> = real
            .triples()
            .map(|t| real.id_triple_to_string(&t).unwrap())
            .collect();
        o.sort();
        r.sort();
        assert_eq!(r, o, "triple sets differ");
        assert_eq!(
            real.triple_count(),
            overlay.triple_count(),
            "triple_count differs"
        );
        // spot-check existence both ways
        for t in &r {
            assert!(overlay.value_triple_exists(t), "overlay missing {:?}", t);
        }

        // Value-range queries must agree as sets. Compared as strings because ids
        // differ between the two, and sorted because the overlay appends its
        // buffered additions after the parent's results rather than merging them
        // in value order.
        let low = String::make_entry(&"");
        let high = String::make_entry(&"zzzzzzzz");
        for rev in [false, true] {
            let mut o: Vec<ValueTriple> = if rev {
                overlay.triples_value_range_rev(&low, &high)
            } else {
                overlay.triples_value_range(&low, &high)
            }
            .map(|t| overlay.id_triple_to_string(&t).unwrap())
            .collect();
            let mut r: Vec<ValueTriple> = if rev {
                real.triples_value_range_rev(&low, &high)
            } else {
                real.triples_value_range(&low, &high)
            }
            .map(|t| real.id_triple_to_string(&t).unwrap())
            .collect();
            o.sort();
            r.sort();
            assert!(!r.is_empty(), "value-range check is vacuous (rev={})", rev);
            assert_eq!(r, o, "value-range results differ (rev={rev})");
        }
    }

    async fn base_layer() -> (CachedLayerStore, [u32; 5]) {
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = store.create_base_layer().await.unwrap();
        let name = builder.name();
        builder.add_value_triple(vt("cow", "says", "moo"));
        builder.add_value_triple(vt("pig", "says", "oink"));
        builder.add_value_triple(vn("cow", "likes", "pig"));
        builder.commit_boxed().await.unwrap();
        (store, name)
    }

    #[tokio::test]
    async fn overlay_matches_real_child_adds_and_removes() {
        let (store, base) = base_layer().await;
        let base_layer = store.get_layer(base).await.unwrap().unwrap();

        let ops = vec![
            OverlayOp::Add(vt("duck", "says", "quack")),
            OverlayOp::Remove(vt("duck", "says", "quack")), // cancels the add
            OverlayOp::Add(vn("cow", "hates", "duck")),
            OverlayOp::Remove(vt("pig", "says", "oink")), // removes a base triple
            OverlayOp::Add(vt("pig", "says", "oink")),    // re-adds it
            OverlayOp::Remove(vn("cow", "likes", "pig")), // removes a base triple
            OverlayOp::Add(vt("hen", "says", "cluck")),
        ];

        let mut overlay = OverlayLayer::new(base_layer);
        for op in &ops {
            match op {
                OverlayOp::Add(t) => overlay.add(t.clone()),
                OverlayOp::Remove(t) => overlay.remove(t.clone()),
            }
        }

        let real = real_child(&store, base, &ops).await;
        assert_equivalent(&overlay, &*real);

        // concrete expectations
        assert!(overlay.value_triple_exists(&vt("cow", "says", "moo")));
        assert!(overlay.value_triple_exists(&vt("pig", "says", "oink"))); // removed then re-added
        assert!(overlay.value_triple_exists(&vn("cow", "hates", "duck")));
        assert!(overlay.value_triple_exists(&vt("hen", "says", "cluck")));
        assert!(!overlay.value_triple_exists(&vt("duck", "says", "quack"))); // add then remove
        assert!(!overlay.value_triple_exists(&vn("cow", "likes", "pig"))); // removed
    }

    #[tokio::test]
    async fn overlay_empty_equals_base() {
        let (store, base) = base_layer().await;
        let base_layer = store.get_layer(base).await.unwrap().unwrap();
        let overlay = OverlayLayer::new(base_layer.clone());
        assert!(overlay.is_empty());
        assert_eq!(base_layer.triple_count(), overlay.triple_count());
        assert!(overlay.value_triple_exists(&vt("cow", "says", "moo")));
    }
}
