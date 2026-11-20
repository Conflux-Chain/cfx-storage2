//! Chain-level snapshot sync of an LVMT commit (gate 3), skeleton.
//!
//! A sender exports the state of a commit `N` as a manifest plus chunks; a
//! receiver imports them. Everything crosses the network as opaque bytes:
//! conflux-rust only sees `Vec<u8>` and reads three scalars of the manifest
//! through [`ManifestHeader::from_manifest_bytes`]. Every encoding starts with
//! [`SNAPSHOT_FORMAT_VERSION`]; bytes of another version are refused with
//! [`SnapshotSyncError::UnsupportedFormatVersion`].
//!
//! - Sender: [`LvmtStoreReader::load_manifest`], then
//!   [`LvmtStoreReader::load_chunk`] by chunk id, in whatever order the receiver
//!   asks for them.
//! - Receiver: [`LvmtStorage::begin_import`], [`ImportSession::apply_chunk`] for
//!   each chunk (anchor chunks before the data chunks that use them, otherwise
//!   [`Applied::Deferred`]), then [`ImportSession::finalize`].
//!
//! Skeleton status: the formats, the call sequence and the bookkeeping of the
//! import session are in place. Not yet: the anchor pass over all items of `N`
//! (the manifest has no anchor commits or MMR nodes yet, so there are no anchor
//! chunks), the contents of data chunks (empty), chunk sizing by
//! `max_chunk_bytes` (data chunks are fixed `w0` ranges), verification, and
//! writing the imported state in `finalize`.

use ethereum_types::H256;

use crate::{
    backends::{
        serde::{Decode, Encode},
        DatabaseTrait, HistoricalTableName, PendingTableName,
    },
    errors::{Result, SnapshotSyncError},
    middlewares::CommitID,
    StorageError,
};

use super::{
    example::LvmtStorage,
    proof::StateRootParts,
    state_root::StateRoot,
    storage::LvmtStoreReader,
    types::{AmtId, AuthChangeKey, AuthChangeNode, CurvePointWithVersion, LvmtValue},
};

/// Version tag at the start of every encoded manifest, chunk id and chunk.
pub const SNAPSHOT_FORMAT_VERSION: u8 = 1;

/// Number of values of `w0`, the first 16-bit word of a key digest.
const W0_COUNT: u32 = 1 << 16;

/// Skeleton only: the `w0` span of every data chunk, until chunks are sized by
/// their contents.
const SKELETON_DATA_CHUNK_W0_SPAN: u32 = 1 << 12;

const KIND_MANIFEST: u8 = 0;
const KIND_CHUNK_ID: u8 = 1;
const KIND_CHUNK: u8 = 2;

const TAG_ANCHOR: u8 = 0;
const TAG_DATA: u8 = 1;

/// Identifies one chunk of the snapshot of a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ChunkId {
    /// The used auth-change nodes of anchor commit `commit_index` (an index into
    /// [`Manifest::anchor_commits`]); a large tree is split into parts.
    Anchor { commit_index: u32, part: u32 },
    /// The items whose key digest starts with a `w0` in `[w0_start, w0_end)`,
    /// `w0_end <= 65536`.
    Data { w0_start: u16, w0_end: u32 },
}

/// The scalars of a manifest that the consensus layer of conflux-rust checks
/// before trusting the rest, readable without decoding the whole manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestHeader {
    pub commit_id: CommitID,
    pub height: u64,
    pub state_root: StateRoot,
}

/// Everything the receiver verifies first; every chunk is verified against it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub header: ManifestHeader,
    /// Root commitment, auth-change root and hroot of `N`.
    pub state_root_parts: StateRootParts,
    /// MMR peaks of `N`'s parent: bag to `N`'s hroot, and continue the MMR after
    /// the import.
    pub parent_mmr_peaks: Vec<H256>,
    /// The versions of the 65536 depth-1 AMTs at `N`, the leaves of the root AMT.
    pub root_leaf_versions: Vec<u64>,
    /// The commits whose auth-change trees authenticate the items current at
    /// `N`: id, height and state root parts.
    pub anchor_commits: Vec<(CommitID, u64, StateRootParts)>,
    /// The MMR nodes before `N` the anchor commits need, by `(level, index)`.
    pub mmr_nodes: Vec<((u32, u64), H256)>,
    /// Anchor chunks first, then data chunks.
    pub chunks: Vec<ChunkId>,
}

/// One chunk of the snapshot of a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotChunk {
    /// The nodes of the auth-change tree of anchor commit `commit_index` used by
    /// the items current at `N` (not the whole tree). Internal nodes carry their
    /// ticks, which are not hashed.
    Anchor {
        commit_index: u32,
        part: u32,
        nodes: Vec<(AuthChangeKey, AuthChangeNode)>,
    },
    /// The items of a `w0` range, each with the index of its anchor commit.
    Data {
        w0_start: u16,
        w0_end: u32,
        flat_kv_pairs: Vec<(Box<[u8]>, LvmtValue, u32)>,
        amt_versions: Vec<(AmtId, u64, u32)>,
    },
}

/// Outcome of [`ImportSession::apply_chunk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    Accepted,
    /// The chunk is a data chunk whose anchor chunks have not all been applied.
    /// Nothing is kept: hand the same bytes in again later.
    Deferred,
    /// The chunk had already been applied (e.g. a retried request); ignored.
    AlreadyApplied,
}

impl ChunkId {
    fn data_ranges() -> impl Iterator<Item = Self> {
        (0..W0_COUNT)
            .step_by(SKELETON_DATA_CHUNK_W0_SPAN as usize)
            .map(|start| ChunkId::Data {
                w0_start: start as u16,
                w0_end: (start + SKELETON_DATA_CHUNK_W0_SPAN).min(W0_COUNT),
            })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = Writer::new(KIND_CHUNK_ID);
        self.write(&mut w);
        w.0
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes, KIND_CHUNK_ID)?;
        let id = Self::read(&mut r)?;
        r.finish()?;
        Ok(id)
    }

    fn write(&self, w: &mut Writer) {
        match *self {
            ChunkId::Anchor { commit_index, part } => {
                w.u8(TAG_ANCHOR);
                w.u32(commit_index);
                w.u32(part);
            }
            ChunkId::Data { w0_start, w0_end } => {
                w.u8(TAG_DATA);
                w.u16(w0_start);
                w.u32(w0_end);
            }
        }
    }

    fn read(r: &mut Reader) -> Result<Self> {
        match r.u8()? {
            TAG_ANCHOR => Ok(ChunkId::Anchor {
                commit_index: r.u32()?,
                part: r.u32()?,
            }),
            TAG_DATA => {
                let w0_start = r.u16()?;
                let w0_end = r.u32()?;
                if w0_end <= w0_start as u32 || w0_end > W0_COUNT {
                    return Err(malformed("data chunk range"));
                }
                Ok(ChunkId::Data { w0_start, w0_end })
            }
            _ => Err(malformed("chunk id tag")),
        }
    }
}

impl ManifestHeader {
    /// Reads the header of an encoded manifest without decoding the rest.
    pub fn from_manifest_bytes(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes, KIND_MANIFEST)?;
        Self::read(&mut r)
    }

    fn write(&self, w: &mut Writer) {
        w.h256(&self.commit_id);
        w.u64(self.height);
        w.h256(&self.state_root);
    }

    fn read(r: &mut Reader) -> Result<Self> {
        Ok(Self {
            commit_id: r.h256()?,
            height: r.u64()?,
            state_root: r.h256()?,
        })
    }
}

impl Manifest {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = Writer::new(KIND_MANIFEST);
        self.header.write(&mut w);
        write_parts(&mut w, &self.state_root_parts);
        w.seq(&self.parent_mmr_peaks, |w, h| w.h256(h));
        w.seq(&self.root_leaf_versions, |w, v| w.u64(*v));
        w.seq(&self.anchor_commits, |w, (id, height, parts)| {
            w.h256(id);
            w.u64(*height);
            write_parts(w, parts);
        });
        w.seq(&self.mmr_nodes, |w, ((level, index), hash)| {
            w.u32(*level);
            w.u64(*index);
            w.h256(hash);
        });
        w.seq(&self.chunks, |w, id| id.write(w));
        w.0
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes, KIND_MANIFEST)?;
        let manifest = Self {
            header: ManifestHeader::read(&mut r)?,
            state_root_parts: read_parts(&mut r)?,
            parent_mmr_peaks: r.seq(|r| r.h256())?,
            root_leaf_versions: r.seq(|r| r.u64())?,
            anchor_commits: r.seq(|r| Ok((r.h256()?, r.u64()?, read_parts(r)?)))?,
            mmr_nodes: r.seq(|r| Ok(((r.u32()?, r.u64()?), r.h256()?)))?,
            chunks: r.seq(ChunkId::read)?,
        };
        r.finish()?;
        Ok(manifest)
    }

    /// The encoded ids of all chunks, anchor chunks first: what the receiver
    /// schedules requests by.
    pub fn chunk_id_bytes(&self) -> Vec<Vec<u8>> {
        self.chunks.iter().map(ChunkId::to_bytes).collect()
    }
}

impl SnapshotChunk {
    pub fn id(&self) -> ChunkId {
        match *self {
            SnapshotChunk::Anchor {
                commit_index, part, ..
            } => ChunkId::Anchor { commit_index, part },
            SnapshotChunk::Data {
                w0_start, w0_end, ..
            } => ChunkId::Data { w0_start, w0_end },
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = Writer::new(KIND_CHUNK);
        self.id().write(&mut w);
        match self {
            SnapshotChunk::Anchor { nodes, .. } => {
                w.seq(nodes, |w, (key, node)| {
                    w.bytes(&key.encode());
                    w.bytes(&node.encode());
                });
            }
            SnapshotChunk::Data {
                flat_kv_pairs,
                amt_versions,
                ..
            } => {
                w.seq(flat_kv_pairs, |w, (key, value, anchor)| {
                    w.bytes(key);
                    w.bytes(&value.encode());
                    w.u32(*anchor);
                });
                w.seq(amt_versions, |w, (amt_id, version, anchor)| {
                    w.bytes(&amt_id.encode());
                    w.u64(*version);
                    w.u32(*anchor);
                });
            }
        }
        w.0
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes, KIND_CHUNK)?;
        let chunk = match ChunkId::read(&mut r)? {
            ChunkId::Anchor { commit_index, part } => SnapshotChunk::Anchor {
                commit_index,
                part,
                nodes: r.seq(|r| {
                    let key = decode::<AuthChangeKey>(r.bytes()?, "auth-change key")?;
                    let node = decode::<AuthChangeNode>(r.bytes()?, "auth-change node")?;
                    Ok((key, node))
                })?,
            },
            ChunkId::Data { w0_start, w0_end } => SnapshotChunk::Data {
                w0_start,
                w0_end,
                flat_kv_pairs: r.seq(|r| {
                    let key: Box<[u8]> = r.bytes()?.into();
                    let value = decode::<LvmtValue>(r.bytes()?, "lvmt value")?;
                    Ok((key, value, r.u32()?))
                })?,
                amt_versions: r.seq(|r| {
                    let raw = r.bytes()?;
                    // At most 16 levels of 16-bit words.
                    if raw.len() > 32 {
                        return Err(malformed("amt id"));
                    }
                    let amt_id = decode::<AmtId>(raw, "amt id")?;
                    Ok((amt_id, r.u64()?, r.u32()?))
                })?,
            },
        };
        r.finish()?;
        Ok(chunk)
    }

    /// The anchor commits the items of a data chunk refer to.
    fn anchor_indices(&self) -> impl Iterator<Item = u32> + '_ {
        let (kv, amt): (&[_], &[_]) = match self {
            SnapshotChunk::Anchor { .. } => (&[], &[]),
            SnapshotChunk::Data {
                flat_kv_pairs,
                amt_versions,
                ..
            } => (flat_kv_pairs, amt_versions),
        };
        kv.iter()
            .map(|(_, _, i)| *i)
            .chain(amt.iter().map(|(_, _, i)| *i))
    }
}

// Sender
impl<P: DatabaseTrait<PendingTableName>> LvmtStoreReader<'_, '_, P> {
    /// The encoded manifest of the snapshot of `commit`, pending or historical.
    ///
    /// Fails with [`SnapshotSyncError::CommitNotFound`] if `commit` is not in the
    /// store (check [`Self::query_commit_existence`] before offering it), and
    /// with [`SnapshotSyncError::ManifestTooLarge`] above `max_manifest_bytes`.
    /// `max_chunk_bytes` is meant to size the chunks; the skeleton does not use
    /// it yet.
    pub fn load_manifest(
        &self,
        commit: CommitID,
        max_chunk_bytes: usize,
        max_manifest_bytes: usize,
    ) -> Result<Vec<u8>> {
        let _ = max_chunk_bytes;
        self.check_snapshot_commit(commit)?;
        let height = self.commit_height(commit)?;
        let state_root_parts = self.state_root_parts(commit)?;
        let parent_mmr_peaks = if height == 0 {
            vec![]
        } else {
            let parent = self
                .get_key_value_reader()
                .get_ancestor_at_height(&commit, height - 1)?;
            self.mmr_entry(parent)?.peaks
        };
        let manifest = Manifest {
            header: ManifestHeader {
                commit_id: commit,
                height,
                state_root: state_root_parts.state_root(),
            },
            state_root_parts,
            parent_mmr_peaks,
            root_leaf_versions: self.get_root_leaf_versions(commit)?,
            anchor_commits: vec![],
            mmr_nodes: vec![],
            chunks: ChunkId::data_ranges().collect(),
        };
        let bytes = manifest.to_bytes();
        if bytes.len() > max_manifest_bytes {
            return Err(SnapshotSyncError::ManifestTooLarge {
                size: bytes.len(),
                max: max_manifest_bytes,
            }
            .into());
        }
        Ok(bytes)
    }

    /// The encoded chunk `chunk_id` (as listed in the manifest) of the snapshot of
    /// `commit`, at most `max_bytes` long.
    pub fn load_chunk(
        &self,
        commit: CommitID,
        chunk_id: &[u8],
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        self.check_snapshot_commit(commit)?;
        let chunk = match ChunkId::from_bytes(chunk_id)? {
            // The skeleton lists no anchor commits.
            ChunkId::Anchor { .. } => return Err(SnapshotSyncError::UnknownChunk.into()),
            id @ ChunkId::Data { w0_start, w0_end } => {
                if !ChunkId::data_ranges().any(|planned| planned == id) {
                    return Err(SnapshotSyncError::UnknownChunk.into());
                }
                SnapshotChunk::Data {
                    w0_start,
                    w0_end,
                    flat_kv_pairs: vec![],
                    amt_versions: vec![],
                }
            }
        };
        let bytes = chunk.to_bytes();
        if bytes.len() > max_bytes {
            return Err(SnapshotSyncError::ChunkTooLarge {
                size: bytes.len(),
                max: max_bytes,
            }
            .into());
        }
        Ok(bytes)
    }

    fn check_snapshot_commit(&self, commit: CommitID) -> Result<()> {
        if !self.query_commit_existence(&commit)? {
            return Err(SnapshotSyncError::CommitNotFound(commit).into());
        }
        Ok(())
    }
}

// Receiver
impl<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> LvmtStorage<D, P> {
    /// Starts importing the snapshot described by an encoded manifest.
    ///
    /// Skeleton: the manifest is decoded, not verified.
    pub fn begin_import(&self, manifest: &[u8]) -> Result<ImportSession> {
        let manifest = Manifest::from_bytes(manifest)?;
        let anchor_parts = {
            let mut parts = vec![0u32; manifest.anchor_commits.len()];
            for id in &manifest.chunks {
                if let ChunkId::Anchor { commit_index, .. } = *id {
                    let slot = parts
                        .get_mut(commit_index as usize)
                        .ok_or(malformed("anchor chunk of an unknown commit"))?;
                    *slot += 1;
                }
            }
            parts
        };
        let mut sorted = manifest.chunks.clone();
        sorted.sort();
        if sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err(malformed("duplicate chunk id"));
        }
        Ok(ImportSession {
            applied: vec![false; manifest.chunks.len()],
            anchor_parts_missing: anchor_parts,
            manifest,
        })
    }
}

/// An import in progress, see [`LvmtStorage::begin_import`].
pub struct ImportSession {
    manifest: Manifest,
    /// `applied[i]`: whether `manifest.chunks[i]` has been applied.
    applied: Vec<bool>,
    /// Per anchor commit, the number of its anchor chunks not applied yet.
    anchor_parts_missing: Vec<u32>,
}

impl ImportSession {
    pub fn header(&self) -> ManifestHeader {
        self.manifest.header
    }

    /// Applies one encoded chunk.
    ///
    /// Skeleton: the chunk is decoded and checked against the manifest, not
    /// verified, and its contents are not kept.
    pub fn apply_chunk(&mut self, chunk: &[u8]) -> Result<Applied> {
        let chunk = SnapshotChunk::from_bytes(chunk)?;
        let id = chunk.id();
        let position = self
            .manifest
            .chunks
            .iter()
            .position(|listed| *listed == id)
            .ok_or(SnapshotSyncError::UnknownChunk)?;
        if self.applied[position] {
            return Ok(Applied::AlreadyApplied);
        }
        // A bad index is an error even when some anchor chunks are still missing.
        let mut deferred = false;
        for index in chunk.anchor_indices() {
            match self.anchor_parts_missing.get(index as usize) {
                None => return Err(malformed("anchor index out of range")),
                Some(&missing) => deferred |= missing > 0,
            }
        }
        if deferred {
            return Ok(Applied::Deferred);
        }
        if let ChunkId::Anchor { commit_index, .. } = id {
            self.anchor_parts_missing[commit_index as usize] -= 1;
        }
        self.applied[position] = true;
        Ok(Applied::Accepted)
    }

    /// The number of chunks of the manifest not applied yet.
    pub fn missing_chunks(&self) -> usize {
        self.applied.iter().filter(|a| !**a).count()
    }

    /// Finishes the import once every chunk has been applied, returning the id of
    /// the imported commit.
    ///
    /// Skeleton: nothing is written to `storage` yet.
    pub fn finalize<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>>(
        self,
        storage: &mut LvmtStorage<D, P>,
    ) -> Result<CommitID> {
        let _ = storage;
        let missing = self.missing_chunks();
        if missing > 0 {
            return Err(SnapshotSyncError::Incomplete(missing).into());
        }
        Ok(self.manifest.header.commit_id)
    }
}

fn malformed(what: &'static str) -> StorageError {
    SnapshotSyncError::Malformed(what).into()
}

fn decode<T: Decode + Clone + ToOwned<Owned = T>>(bytes: &[u8], what: &'static str) -> Result<T> {
    Ok(T::decode(bytes).map_err(|_| malformed(what))?.into_owned())
}

fn write_parts(w: &mut Writer, parts: &StateRootParts) {
    w.bytes(&parts.root_commitment.encode());
    w.h256(&parts.auth_change_root);
    w.h256(&parts.hroot);
}

fn read_parts(r: &mut Reader) -> Result<StateRootParts> {
    Ok(StateRootParts {
        root_commitment: decode::<CurvePointWithVersion>(r.bytes()?, "root commitment")?,
        auth_change_root: r.h256()?,
        hroot: r.h256()?,
    })
}

/// Little-endian writer of the snapshot formats: version tag, kind, then fields;
/// sequences and byte strings are prefixed by a `u32` length.
struct Writer(Vec<u8>);

impl Writer {
    fn new(kind: u8) -> Self {
        Self(vec![SNAPSHOT_FORMAT_VERSION, kind])
    }
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn h256(&mut self, v: &H256) {
        self.0.extend_from_slice(v.as_bytes());
    }
    fn bytes(&mut self, v: &[u8]) {
        self.u32(u32::try_from(v.len()).expect("field shorter than 4 GiB"));
        self.0.extend_from_slice(v);
    }
    fn seq<T>(&mut self, items: &[T], mut write: impl FnMut(&mut Self, &T)) {
        self.u32(u32::try_from(items.len()).expect("fewer than 2^32 items"));
        for item in items {
            write(self, item);
        }
    }
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], kind: u8) -> Result<Self> {
        let mut r = Self(bytes);
        let version = r.u8()?;
        if version != SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotSyncError::UnsupportedFormatVersion(version).into());
        }
        if r.u8()? != kind {
            return Err(malformed("kind"));
        }
        Ok(r)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(malformed("truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn h256(&mut self) -> Result<H256> {
        Ok(H256::from_slice(self.take(32)?))
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }
    fn seq<T>(&mut self, mut read: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let len = self.u32()? as usize;
        // Every item takes at least one byte: don't trust `len` for the allocation.
        let mut items = Vec::with_capacity(len.min(self.0.len()));
        for _ in 0..len {
            items.push(read(self)?);
        }
        Ok(items)
    }
    fn finish(self) -> Result<()> {
        if !self.0.is_empty() {
            return Err(malformed("trailing bytes"));
        }
        Ok(())
    }
}
