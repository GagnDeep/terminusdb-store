//! Write-ahead log for group commit (Phase 2 durability).
//!
//! [`BufferedNamedGraph`](super::buffered::BufferedNamedGraph) accumulates
//! commits in RAM and flushes them as one layer. Without a log, an un-flushed
//! batch is lost on a crash. A [`Wal`] makes each buffered op durable *before*
//! it is acknowledged, so a crash loses at most what the [`DurabilityMode`]
//! allows, and recovery replays the surviving ops onto the current head.
//!
//! Each record carries the base layer it applied to, so recovery can tell
//! already-flushed ops (whose base is no longer the head) from un-flushed ones
//! and never double-applies a batch that was flushed but not yet checkpointed.
//!
//! The log is a node-local durability aid; over object storage the bucket
//! remains the source of truth and the sole cross-node coordinator.

use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use num_traits::FromPrimitive;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use tdb_succinct::{Datatype, SizedDictEntry, TypedDictEntry};

use crate::layer::overlay::OverlayOp;
use crate::layer::{ObjectType, ValueTriple};

/// How aggressively buffered commits are made durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityMode {
    /// fsync the log after every appended op — no data loss on crash, at the
    /// cost of one small sequential fsync per op (still far cheaper than
    /// building a whole layer + label CAS per commit).
    PerCommitFsync,
    /// Append without fsync — fastest, but a crash may lose the un-fsynced tail.
    /// The OS still flushes eventually; use when some tail loss is acceptable.
    NoSync,
}

/// One logged value-level operation, tagged with the base head it applied to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub base: [u32; 5],
    pub op: OverlayOp,
}

const SENTINEL: u8 = 0xA5;

// ---- codec ----

fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_u32(s.len() as u32);
    buf.put_slice(s.as_bytes());
}

fn put_value_triple(buf: &mut BytesMut, t: &ValueTriple) {
    put_str(buf, &t.subject);
    put_str(buf, &t.predicate);
    match &t.object {
        ObjectType::Node(n) => {
            buf.put_u8(0);
            put_str(buf, n);
        }
        ObjectType::Value(v) => {
            buf.put_u8(1);
            buf.put_u8(v.datatype() as u8);
            let bytes = v.to_bytes();
            buf.put_u32(bytes.len() as u32);
            buf.put_slice(&bytes);
        }
    }
}

fn encode_payload(record: &WalRecord) -> BytesMut {
    let mut buf = BytesMut::new();
    for n in record.base {
        buf.put_u32(n);
    }
    let (tag, triple) = match &record.op {
        OverlayOp::Add(t) => (0u8, t),
        OverlayOp::Remove(t) => (1u8, t),
    };
    buf.put_u8(tag);
    put_value_triple(&mut buf, triple);
    buf
}

/// Frame = `len:u32 | payload | SENTINEL:u8`. The sentinel + length let replay
/// detect a torn tail from a partial write.
fn encode_frame(record: &WalRecord) -> Bytes {
    let payload = encode_payload(record);
    let mut framed = BytesMut::with_capacity(4 + payload.len() + 1);
    framed.put_u32(payload.len() as u32);
    framed.extend_from_slice(&payload);
    framed.put_u8(SENTINEL);
    framed.freeze()
}

fn take(buf: &mut &[u8], n: usize) -> Option<Bytes> {
    if buf.len() < n {
        return None;
    }
    let (head, rest) = buf.split_at(n);
    let out = Bytes::copy_from_slice(head);
    *buf = rest;
    Some(out)
}

fn get_str(buf: &mut &[u8]) -> Option<String> {
    if buf.len() < 4 {
        return None;
    }
    let len = (&buf[..4]).get_u32() as usize;
    *buf = &buf[4..];
    let bytes = take(buf, len)?;
    String::from_utf8(bytes.to_vec()).ok()
}

fn get_value_triple(buf: &mut &[u8]) -> Option<ValueTriple> {
    let subject = get_str(buf)?;
    let predicate = get_str(buf)?;
    let obj_tag = take(buf, 1)?[0];
    let object = match obj_tag {
        0 => ObjectType::Node(get_str(buf)?),
        1 => {
            let dt = Datatype::from_u8(take(buf, 1)?[0])?;
            if buf.len() < 4 {
                return None;
            }
            let len = (&buf[..4]).get_u32() as usize;
            *buf = &buf[4..];
            let bytes = take(buf, len)?;
            ObjectType::Value(TypedDictEntry::new(dt, SizedDictEntry::from(bytes)))
        }
        _ => return None,
    };
    Some(ValueTriple {
        subject,
        predicate,
        object,
    })
}

fn decode_payload(mut payload: &[u8]) -> Option<WalRecord> {
    let buf = &mut payload;
    let mut base = [0u32; 5];
    for slot in base.iter_mut() {
        if buf.len() < 4 {
            return None;
        }
        *slot = (&buf[..4]).get_u32();
        *buf = &buf[4..];
    }
    let tag = take(buf, 1)?[0];
    let triple = get_value_triple(buf)?;
    let op = match tag {
        0 => OverlayOp::Add(triple),
        1 => OverlayOp::Remove(triple),
        _ => return None,
    };
    Some(WalRecord { base, op })
}

/// Parse as many whole frames as are intact, stopping at the first torn/short
/// frame (a partial trailing write from a crash).
fn decode_all(mut data: &[u8]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    loop {
        if data.len() < 4 {
            break;
        }
        let len = (&data[..4]).get_u32() as usize;
        // need len bytes payload + 1 sentinel after the 4-byte length
        if data.len() < 4 + len + 1 {
            break; // torn tail
        }
        let payload = &data[4..4 + len];
        let sentinel = data[4 + len];
        if sentinel != SENTINEL {
            break; // torn / corrupt frame boundary
        }
        match decode_payload(payload) {
            Some(record) => out.push(record),
            None => break,
        }
        data = &data[4 + len + 1..];
    }
    out
}

// ---- the trait + a file-backed implementation ----

#[async_trait]
pub trait Wal: Send + Sync {
    async fn append(&self, record: &WalRecord) -> io::Result<()>;
    async fn replay(&self) -> io::Result<Vec<WalRecord>>;
    /// Discard all logged records (called after a successful flush).
    async fn checkpoint(&self) -> io::Result<()>;
}

/// Append-only, torn-tail-tolerant file WAL.
pub struct FileWal {
    path: PathBuf,
    durability: DurabilityMode,
    file: Mutex<tokio::fs::File>,
}

impl FileWal {
    pub async fn open(path: impl Into<PathBuf>, durability: DurabilityMode) -> io::Result<Self> {
        let path = path.into();
        let file = Self::open_append(&path).await?;
        Ok(Self {
            path,
            durability,
            file: Mutex::new(file),
        })
    }

    async fn open_append(path: &Path) -> io::Result<tokio::fs::File> {
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
    }
}

#[async_trait]
impl Wal for FileWal {
    async fn append(&self, record: &WalRecord) -> io::Result<()> {
        let frame = encode_frame(record);
        let mut file = self.file.lock().await;
        file.write_all(&frame).await?;
        file.flush().await?;
        if self.durability == DurabilityMode::PerCommitFsync {
            file.sync_all().await?;
        }
        Ok(())
    }

    async fn replay(&self) -> io::Result<Vec<WalRecord>> {
        let mut data = Vec::new();
        match tokio::fs::File::open(&self.path).await {
            Ok(mut f) => {
                f.read_to_end(&mut data).await?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        }
        Ok(decode_all(&data))
    }

    async fn checkpoint(&self) -> io::Result<()> {
        let mut file = self.file.lock().await;
        file.sync_all().await?;
        // Truncate to empty and reopen the append handle at offset 0.
        tokio::fs::File::create(&self.path)
            .await?
            .sync_all()
            .await?;
        *file = Self::open_append(&self.path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn add(s: &str, p: &str, o: &str) -> WalRecord {
        WalRecord {
            base: [1, 2, 3, 4, 5],
            op: OverlayOp::Add(ValueTriple::new_string_value(s, p, o)),
        }
    }
    fn remove_node(s: &str, p: &str, o: &str) -> WalRecord {
        WalRecord {
            base: [9, 9, 9, 9, 9],
            op: OverlayOp::Remove(ValueTriple::new_node(s, p, o)),
        }
    }

    #[test]
    fn codec_round_trips_string_node_and_typed_values() {
        use tdb_succinct::TdbDataType;
        let records = vec![
            add("cow", "says", "moo"),
            remove_node("cow", "likes", "pig"),
            WalRecord {
                base: [7, 7, 7, 7, 7],
                op: OverlayOp::Add(ValueTriple::new_value("n", "age", i32::make_entry(&42i32))),
            },
        ];
        let mut buf = BytesMut::new();
        for r in &records {
            buf.extend_from_slice(&encode_frame(r));
        }
        assert_eq!(records, decode_all(&buf));
    }

    #[tokio::test]
    async fn append_replay_and_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = FileWal::open(&path, DurabilityMode::PerCommitFsync)
            .await
            .unwrap();

        wal.append(&add("a", "p", "1")).await.unwrap();
        wal.append(&add("b", "p", "2")).await.unwrap();
        assert_eq!(2, wal.replay().await.unwrap().len());

        // reopen: records survive
        drop(wal);
        let wal = FileWal::open(&path, DurabilityMode::PerCommitFsync)
            .await
            .unwrap();
        assert_eq!(2, wal.replay().await.unwrap().len());

        // checkpoint clears the log
        wal.checkpoint().await.unwrap();
        assert_eq!(0, wal.replay().await.unwrap().len());

        // and it is usable again after checkpoint
        wal.append(&add("c", "p", "3")).await.unwrap();
        assert_eq!(1, wal.replay().await.unwrap().len());
    }

    #[tokio::test]
    async fn torn_tail_is_tolerated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal = FileWal::open(&path, DurabilityMode::PerCommitFsync)
                .await
                .unwrap();
            wal.append(&add("a", "p", "1")).await.unwrap();
            wal.append(&add("b", "p", "2")).await.unwrap();
        }
        // simulate a crash mid-write: append a partial (truncated) frame
        let mut bytes = tokio::fs::read(&path).await.unwrap();
        bytes.extend_from_slice(&[0, 0, 0, 50, 1, 2, 3]); // claims 50-byte payload, only 3 present
        tokio::fs::write(&path, &bytes).await.unwrap();

        let wal = FileWal::open(&path, DurabilityMode::NoSync).await.unwrap();
        // the two intact records are recovered; the torn tail is dropped
        assert_eq!(2, wal.replay().await.unwrap().len());
    }
}
