//! Automatic, non-destructive read-depth compaction (Phase 0C).
//!
//! A long commit history is a deep layer chain, and reading it means loading
//! many layers. Rollup collapses a chain into a single flattened stand-in whose
//! `parent_name` jumps over the collapsed range, so a subsequent read loads O(1)
//! layers instead of walking the whole history.
//!
//! This manager rolls a label's head up whenever its *effective* stack depth
//! (rollups counted as one) exceeds a threshold. It uses **rollup only, never
//! squash**: rollup is non-destructive — every original layer is retained and the
//! content-addressed parent chain stays fully walkable — so immutability and the
//! per-commit audit trail are preserved. This is the audit-safe compaction
//! policy chosen for this engine.

use std::io;
use std::time::Duration;

use super::Store;

/// Rolls up label heads in the background to keep read depth bounded.
#[derive(Clone)]
pub struct CompactionManager {
    store: Store,
    max_depth: usize,
}

impl CompactionManager {
    /// Create a manager that rolls up any head whose effective layer stack
    /// exceeds `max_depth`.
    pub fn new(store: Store, max_depth: usize) -> Self {
        Self { store, max_depth }
    }

    /// Roll up the head of `label` if its effective stack exceeds `max_depth`.
    /// Returns whether a rollup was performed.
    pub async fn maybe_compact_label(&self, label: &str) -> io::Result<bool> {
        let head = match self.store.label_store.get_label(label).await? {
            Some(l) => match l.layer {
                Some(name) => name,
                None => return Ok(false),
            },
            None => return Ok(false),
        };
        self.maybe_compact_layer(head).await
    }

    /// Roll up `head` if its effective stack (rollups counted as one) exceeds
    /// `max_depth`. Non-destructive; never squashes.
    pub async fn maybe_compact_layer(&self, head: [u32; 5]) -> io::Result<bool> {
        // Loading the head is required to roll it up anyway; its
        // `layer_stack_size()` is the effective depth, which already accounts for
        // any existing rollup (so a rolled-up head reads as shallow and won't be
        // re-rolled every commit).
        let layer = match self.store.layer_store.get_layer(head).await? {
            Some(l) => l,
            None => return Ok(false),
        };
        if layer.layer_stack_size() <= self.max_depth {
            return Ok(false);
        }
        // Non-destructive: registers a rollup pointer; originals are retained and
        // the label is untouched (no CAS needed). `CachedLayerStore::register_rollup`
        // invalidates its cache entry so the next read sees the rollup.
        self.store.layer_store.clone().rollup(layer).await?;
        Ok(true)
    }

    /// Roll up every label head that exceeds `max_depth`. Returns the number
    /// compacted.
    pub async fn compact_all(&self) -> io::Result<usize> {
        let mut compacted = 0;
        for label in self.store.label_store.labels().await? {
            if self.maybe_compact_label(&label.name).await? {
                compacted += 1;
            }
        }
        Ok(compacted)
    }

    /// Spawn a background task that compacts all labels every `interval`. The
    /// returned handle can be aborted to stop it.
    pub fn spawn(self, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                // Best-effort: a transient failure should not kill the loop.
                let _ = self.compact_all().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use crate::store::open_memory_store;

    #[tokio::test]
    async fn auto_rollup_bounds_depth_and_is_nondestructive() {
        let store = open_memory_store();
        let db = store.create("g").await.unwrap();

        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        let mut layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();
        let base_name = layer.name();

        for i in 1..5 {
            let builder = layer.open_write().await.unwrap();
            builder
                .add_value_triple(ValueTriple::new_string_value(
                    &format!("k{}", i),
                    "p",
                    &format!("v{}", i),
                ))
                .unwrap();
            layer = builder.commit().await.unwrap();
            db.set_head(&layer).await.unwrap();
        }
        let head_name = layer.name();

        let mgr = CompactionManager::new(store.clone(), 2);

        // effective depth is 5 (> 2) -> a rollup happens
        assert!(mgr.maybe_compact_label("g").await.unwrap());

        // the head now loads as a shallow rollup
        let internal = store
            .layer_store
            .get_layer(head_name)
            .await
            .unwrap()
            .unwrap();
        assert!(internal.is_rollup());
        assert!(internal.layer_stack_size() <= 2);

        // contents are fully preserved across the rollup
        assert!(internal.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(internal.value_triple_exists(&ValueTriple::new_string_value("k4", "p", "v4")));

        // non-destructive: the original base layer is still retrievable
        assert!(store
            .layer_store
            .get_layer(base_name)
            .await
            .unwrap()
            .is_some());

        // a second pass is a no-op (effective depth is now small)
        assert!(!mgr.maybe_compact_label("g").await.unwrap());
    }
}
