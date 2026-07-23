//! Opt-in group commit (Phase 2).
//!
//! [`BufferedNamedGraph`] batches many logical commits into one physical layer.
//! Adds/removes accumulate in an in-RAM [`OverlayLayer`] so the writer reads its
//! own un-flushed changes (the LSM-memtable analog); a flush replays the whole
//! batch, in order, into a single layer via the normal builder path — one
//! `.larch` object and one label compare-and-swap per batch instead of per
//! commit. Over object storage this cuts write amplification and label
//! contention by the batch factor.
//!
//! **Durability contract.** A buffered commit is durable once [`flush`] returns
//! (or an auto-flush fires on the op threshold). Between flushes, buffered ops
//! live only in RAM — like a database transaction that has not yet committed. If
//! you need per-commit durability, use the default unbuffered `NamedGraph` path
//! (which fsyncs every commit), or flush after each commit. Buffering is
//! therefore opt-in and audit labels should stay on the strict default. (A
//! persistent write-ahead log giving per-commit durability *on buffered labels*
//! is a planned enhancement.)
//!
//! **Concurrency.** A buffered graph assumes a single logical writer for its
//! label (matching the label compare-and-swap model). If a foreign writer
//! advances the label between flushes, the flush's CAS fails and a typed
//! conflict error is surfaced.
//!
//! [`flush`]: BufferedNamedGraph::flush

use std::io;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::layer::overlay::{OverlayLayer, OverlayOp};
use crate::layer::{Layer, ValueTriple};

use super::Store;

#[derive(Debug, thiserror::Error)]
pub enum BufferedError {
    #[error("label '{0}' does not exist")]
    NoSuchLabel(String),
    #[error("label '{0}' has no head layer to buffer on top of")]
    NoHead(String),
    #[error("label '{0}' was advanced by another writer during flush")]
    Conflict(String),
}

impl From<BufferedError> for io::Error {
    fn from(e: BufferedError) -> io::Error {
        io::Error::other(e)
    }
}

struct Pending {
    /// The last flushed persisted head, and the overlay built on top of it.
    overlay: OverlayLayer,
    base_name: [u32; 5],
}

/// A group-commit wrapper around a single label.
pub struct BufferedNamedGraph {
    store: Store,
    label: String,
    max_ops: usize,
    pending: Mutex<Pending>,
}

impl BufferedNamedGraph {
    /// Open group-commit buffering for an existing label that already has a
    /// head. `max_ops` triggers an automatic flush once that many value-level
    /// operations have accumulated (0 disables auto-flush).
    pub async fn open(store: Store, label: &str, max_ops: usize) -> io::Result<Self> {
        let head_name = store
            .label_store
            .get_label(label)
            .await?
            .ok_or_else(|| BufferedError::NoSuchLabel(label.to_string()))?
            .layer
            .ok_or_else(|| BufferedError::NoHead(label.to_string()))?;
        let base = store
            .layer_store
            .get_layer(head_name)
            .await?
            .ok_or_else(|| BufferedError::NoHead(label.to_string()))?;
        let overlay = OverlayLayer::new(base as Arc<dyn Layer>);
        Ok(Self {
            store,
            label: label.to_string(),
            max_ops,
            pending: Mutex::new(Pending {
                overlay,
                base_name: head_name,
            }),
        })
    }

    /// Buffer a triple addition. Auto-flushes if the op threshold is reached.
    pub async fn add(&self, triple: ValueTriple) -> io::Result<()> {
        let mut p = self.pending.lock().await;
        p.overlay.add(triple);
        if self.max_ops != 0 && p.overlay.len() >= self.max_ops {
            self.flush_locked(&mut p).await?;
        }
        Ok(())
    }

    /// Buffer a triple removal. Auto-flushes if the op threshold is reached.
    pub async fn remove(&self, triple: ValueTriple) -> io::Result<()> {
        let mut p = self.pending.lock().await;
        p.overlay.remove(triple);
        if self.max_ops != 0 && p.overlay.len() >= self.max_ops {
            self.flush_locked(&mut p).await?;
        }
        Ok(())
    }

    /// A read snapshot that includes all un-flushed commits (read-your-writes).
    pub async fn head(&self) -> Arc<dyn Layer> {
        let p = self.pending.lock().await;
        Arc::new(p.overlay.clone())
    }

    /// The number of buffered, un-flushed value-level operations.
    pub async fn pending_ops(&self) -> usize {
        self.pending.lock().await.overlay.len()
    }

    /// Flush all buffered commits into a single new layer and advance the label.
    /// Returns the new head layer id, or `None` if nothing was buffered.
    pub async fn flush(&self) -> io::Result<Option<[u32; 5]>> {
        let mut p = self.pending.lock().await;
        self.flush_locked(&mut p).await
    }

    async fn flush_locked(&self, p: &mut Pending) -> io::Result<Option<[u32; 5]>> {
        if p.overlay.is_empty() {
            return Ok(None);
        }

        // Replay the whole batch, in order, into one real child layer.
        let base = self
            .store
            .get_layer_from_id(p.base_name)
            .await?
            .ok_or_else(|| BufferedError::NoHead(self.label.clone()))?;
        let builder = base.open_write().await?;
        for op in p.overlay.ops() {
            match op {
                OverlayOp::Add(t) => builder.add_value_triple(t.clone())?,
                OverlayOp::Remove(t) => builder.remove_value_triple(t.clone())?,
            }
        }
        let new_layer = builder.commit().await?;

        // Advance the label with the existing CAS; a foreign writer that moved
        // the label makes this fail.
        let graph = self
            .store
            .open(&self.label)
            .await?
            .ok_or_else(|| BufferedError::NoSuchLabel(self.label.clone()))?;
        if !graph.set_head(&new_layer).await? {
            return Err(BufferedError::Conflict(self.label.clone()).into());
        }

        // Reset the overlay onto the freshly flushed head.
        let new_name = new_layer.name();
        let base_layer = self
            .store
            .layer_store
            .get_layer(new_name)
            .await?
            .ok_or_else(|| BufferedError::NoHead(self.label.clone()))?;
        p.overlay = OverlayLayer::new(base_layer as Arc<dyn Layer>);
        p.base_name = new_name;

        Ok(Some(new_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use crate::store::open_memory_store;

    fn vt(s: &str, p: &str, o: &str) -> ValueTriple {
        ValueTriple::new_string_value(s, p, o)
    }

    async fn store_with_base() -> (Store, String) {
        let store = open_memory_store();
        let db = store.create("g").await.unwrap();
        let builder = store.create_base_layer().await.unwrap();
        builder.add_value_triple(vt("cow", "says", "moo")).unwrap();
        let layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();
        (store, "g".to_string())
    }

    #[tokio::test]
    async fn group_commit_reads_own_writes_then_flushes_one_layer() {
        let (store, label) = store_with_base().await;
        let buffered = BufferedNamedGraph::open(store.clone(), &label, 0)
            .await
            .unwrap();

        // buffer several commits
        for i in 0..10 {
            buffered
                .add(vt(&format!("s{}", i), "p", &format!("o{}", i)))
                .await
                .unwrap();
        }
        assert_eq!(10, buffered.pending_ops().await);

        // read-your-writes: the overlay head sees un-flushed commits
        let head = buffered.head().await;
        assert!(head.value_triple_exists(&vt("s7", "p", "o7")));
        assert!(head.value_triple_exists(&vt("cow", "says", "moo")));

        // the persisted label has NOT advanced yet (still the base head)
        let base_depth = store
            .layer_store
            .retrieve_layer_stack_names(
                store
                    .open(&label)
                    .await
                    .unwrap()
                    .unwrap()
                    .head()
                    .await
                    .unwrap()
                    .unwrap()
                    .name(),
            )
            .await
            .unwrap()
            .len();
        assert_eq!(1, base_depth);

        // flush: 10 commits collapse into exactly ONE new layer
        let new_head = buffered.flush().await.unwrap().unwrap();
        let depth = store
            .layer_store
            .retrieve_layer_stack_names(new_head)
            .await
            .unwrap()
            .len();
        assert_eq!(2, depth, "10 commits must flush to a single new layer");

        // reopen from the store: all buffered triples are persisted
        let persisted = store.get_layer_from_id(new_head).await.unwrap().unwrap();
        for i in 0..10 {
            assert!(persisted.value_triple_exists(&vt(
                &format!("s{}", i),
                "p",
                &format!("o{}", i)
            )));
        }
        assert!(persisted.value_triple_exists(&vt("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn auto_flush_on_threshold() {
        let (store, label) = store_with_base().await;
        let buffered = BufferedNamedGraph::open(store.clone(), &label, 4)
            .await
            .unwrap();

        // 4 ops trigger an auto-flush, leaving the buffer empty
        for i in 0..4 {
            buffered
                .add(vt(&format!("s{}", i), "p", "o"))
                .await
                .unwrap();
        }
        assert_eq!(0, buffered.pending_ops().await);

        // the label advanced and holds the data
        let head = store
            .open(&label)
            .await
            .unwrap()
            .unwrap()
            .head()
            .await
            .unwrap()
            .unwrap();
        assert!(head.value_triple_exists(&vt("s3", "p", "o")));
    }

    #[tokio::test]
    async fn flush_empty_is_noop() {
        let (store, label) = store_with_base().await;
        let buffered = BufferedNamedGraph::open(store.clone(), &label, 0)
            .await
            .unwrap();
        assert!(buffered.flush().await.unwrap().is_none());
    }
}
