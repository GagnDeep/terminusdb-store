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
//! **Durability contract.** Opened via [`open`](BufferedNamedGraph::open), a
//! buffered commit is durable once [`flush`] returns (or an auto-flush fires) —
//! between flushes the ops live only in RAM, like a transaction that has not yet
//! committed. Opened via [`open_with_wal`](BufferedNamedGraph::open_with_wal),
//! each op is written to a node-local write-ahead log before it is acknowledged,
//! so with [`DurabilityMode::PerCommitFsync`] no acknowledged commit is lost on a
//! crash: recovery replays the log's un-flushed ops onto the current head.
//! Buffering is opt-in; audit labels stay on the strict default unbuffered path.
//!
//! **Concurrency.** A buffered graph assumes a single logical writer for its
//! label (matching the label compare-and-swap model). If a foreign writer
//! advances the label between flushes, the flush's CAS fails and a typed
//! conflict error is surfaced.
//!
//! [`flush`]: BufferedNamedGraph::flush

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::layer::overlay::{OverlayLayer, OverlayOp};
use crate::layer::{Layer, ValueTriple};

use super::wal::{DurabilityMode, FileWal, Wal, WalRecord};
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
    wal: Option<Arc<dyn Wal>>,
    pending: Mutex<Pending>,
}

impl BufferedNamedGraph {
    /// Open group-commit buffering for an existing label that already has a
    /// head. `max_ops` triggers an automatic flush once that many value-level
    /// operations have accumulated (0 disables auto-flush).
    ///
    /// No write-ahead log: buffered commits are durable on flush only. For
    /// per-commit durability use [`open_with_wal`](Self::open_with_wal).
    pub async fn open(store: Store, label: &str, max_ops: usize) -> io::Result<Self> {
        Self::open_inner(store, label, max_ops, None).await
    }

    /// Open with a write-ahead log at `wal_path`, giving per-commit durability
    /// per `durability`. On open, the log is replayed and any un-flushed commits
    /// are recovered onto the current head (already-flushed records are skipped
    /// by base, so a batch that was flushed but not checkpointed is never
    /// double-applied).
    pub async fn open_with_wal(
        store: Store,
        label: &str,
        max_ops: usize,
        wal_path: impl Into<PathBuf>,
        durability: DurabilityMode,
    ) -> io::Result<Self> {
        let wal: Arc<dyn Wal> = Arc::new(FileWal::open(wal_path, durability).await?);
        Self::open_inner(store, label, max_ops, Some(wal)).await
    }

    /// Like [`open_with_wal`](Self::open_with_wal) but the log lives in an object
    /// store under `wal_prefix` instead of on local disk, so durable group commit
    /// needs no local disk. Each buffered op is one small object (a successful
    /// PUT is the durability point); recovery replays and reconciles exactly as
    /// with the file log.
    #[cfg(feature = "object-store")]
    pub async fn open_with_object_wal(
        store: Store,
        label: &str,
        max_ops: usize,
        object_store: std::sync::Arc<dyn object_store::ObjectStore>,
        wal_prefix: impl Into<String>,
    ) -> io::Result<Self> {
        let wal: Arc<dyn Wal> =
            Arc::new(super::wal::ObjectWal::open(object_store, wal_prefix).await?);
        Self::open_inner(store, label, max_ops, Some(wal)).await
    }

    async fn open_inner(
        store: Store,
        label: &str,
        max_ops: usize,
        wal: Option<Arc<dyn Wal>>,
    ) -> io::Result<Self> {
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
        let mut overlay = OverlayLayer::new(base as Arc<dyn Layer>);

        // Crash recovery: replay the WAL, keep only records for the current head
        // (others were already flushed), rewrite the log to just those, and
        // re-apply them to the overlay.
        if let Some(wal) = &wal {
            let surviving: Vec<WalRecord> = wal
                .replay()
                .await?
                .into_iter()
                .filter(|r| r.base == head_name)
                .collect();
            wal.checkpoint().await?;
            for record in &surviving {
                wal.append(record).await?;
                match &record.op {
                    OverlayOp::Add(t) => overlay.add(t.clone()),
                    OverlayOp::Remove(t) => overlay.remove(t.clone()),
                }
            }
        }

        Ok(Self {
            store,
            label: label.to_string(),
            max_ops,
            wal,
            pending: Mutex::new(Pending {
                overlay,
                base_name: head_name,
            }),
        })
    }

    /// Buffer a triple addition. Auto-flushes if the op threshold is reached.
    pub async fn add(&self, triple: ValueTriple) -> io::Result<()> {
        let mut p = self.pending.lock().await;
        // durable before ack
        if let Some(wal) = &self.wal {
            wal.append(&WalRecord {
                base: p.base_name,
                op: OverlayOp::Add(triple.clone()),
            })
            .await?;
        }
        p.overlay.add(triple);
        if self.max_ops != 0 && p.overlay.len() >= self.max_ops {
            self.flush_locked(&mut p).await?;
        }
        Ok(())
    }

    /// Buffer a triple removal. Auto-flushes if the op threshold is reached.
    pub async fn remove(&self, triple: ValueTriple) -> io::Result<()> {
        let mut p = self.pending.lock().await;
        if let Some(wal) = &self.wal {
            wal.append(&WalRecord {
                base: p.base_name,
                op: OverlayOp::Remove(triple.clone()),
            })
            .await?;
        }
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

        // The batch is now persisted and the label advanced, so the log can be
        // discarded. (A crash between set_head and here leaves the old records,
        // which recovery skips by base and rewrites away.)
        if let Some(wal) = &self.wal {
            wal.checkpoint().await?;
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

    #[tokio::test]
    async fn wal_recovers_unflushed_commits_without_double_apply() {
        use crate::store::wal::DurabilityMode::PerCommitFsync;
        use tempfile::tempdir;

        let (store, label) = store_with_base().await;
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("g.wal");

        // buffer two commits, then "crash" (drop without flushing)
        {
            let b = BufferedNamedGraph::open_with_wal(
                store.clone(),
                &label,
                0,
                &wal_path,
                PerCommitFsync,
            )
            .await
            .unwrap();
            b.add(vt("x", "p", "1")).await.unwrap();
            b.add(vt("y", "p", "2")).await.unwrap();
            // the persisted label has NOT advanced
        }

        // reopen: the un-flushed commits are recovered from the WAL
        let b =
            BufferedNamedGraph::open_with_wal(store.clone(), &label, 0, &wal_path, PerCommitFsync)
                .await
                .unwrap();
        assert_eq!(2, b.pending_ops().await);
        let head = b.head().await;
        assert!(head.value_triple_exists(&vt("x", "p", "1")));
        assert!(head.value_triple_exists(&vt("y", "p", "2")));

        // flush persists them and checkpoints the WAL
        b.flush().await.unwrap().unwrap();
        let persisted = store
            .open(&label)
            .await
            .unwrap()
            .unwrap()
            .head()
            .await
            .unwrap()
            .unwrap();
        assert!(persisted.value_triple_exists(&vt("x", "p", "1")));
        assert!(persisted.value_triple_exists(&vt("y", "p", "2")));

        // reopening a third time recovers nothing — no double-apply of the
        // already-flushed batch
        let b3 =
            BufferedNamedGraph::open_with_wal(store.clone(), &label, 0, &wal_path, PerCommitFsync)
                .await
                .unwrap();
        assert_eq!(0, b3.pending_ops().await);
    }

    // Same durable-group-commit crash recovery, but with the WAL in the bucket
    // (no local disk anywhere) — the graph and its log share one object store.
    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn object_wal_recovers_unflushed_commits_disk_less() {
        use crate::store::open_object_store;

        let bucket: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = open_object_store(bucket.clone(), "", 1 << 30);
        let db = store.create("g").await.unwrap();
        let builder = store.create_base_layer().await.unwrap();
        builder.add_value_triple(vt("cow", "says", "moo")).unwrap();
        let layer = builder.commit().await.unwrap();
        db.set_head(&layer).await.unwrap();
        let label = "g".to_string();

        // buffer two commits, then "crash" (drop without flushing). The WAL lives
        // in the bucket under "g.wal", so nothing is on local disk.
        {
            let b = BufferedNamedGraph::open_with_object_wal(
                store.clone(),
                &label,
                0,
                bucket.clone(),
                "g.wal",
            )
            .await
            .unwrap();
            b.add(vt("x", "p", "1")).await.unwrap();
            b.add(vt("y", "p", "2")).await.unwrap();
        }

        // reopen: the un-flushed commits are recovered from the bucket-backed WAL
        let b = BufferedNamedGraph::open_with_object_wal(
            store.clone(),
            &label,
            0,
            bucket.clone(),
            "g.wal",
        )
        .await
        .unwrap();
        assert_eq!(2, b.pending_ops().await);
        let head = b.head().await;
        assert!(head.value_triple_exists(&vt("x", "p", "1")));
        assert!(head.value_triple_exists(&vt("y", "p", "2")));

        // flush persists them and checkpoints the WAL
        b.flush().await.unwrap().unwrap();
        let persisted = store
            .open(&label)
            .await
            .unwrap()
            .unwrap()
            .head()
            .await
            .unwrap()
            .unwrap();
        assert!(persisted.value_triple_exists(&vt("x", "p", "1")));
        assert!(persisted.value_triple_exists(&vt("y", "p", "2")));

        // reopening again recovers nothing — no double-apply
        let b3 = BufferedNamedGraph::open_with_object_wal(
            store.clone(),
            &label,
            0,
            bucket.clone(),
            "g.wal",
        )
        .await
        .unwrap();
        assert_eq!(0, b3.pending_ops().await);
    }
}
