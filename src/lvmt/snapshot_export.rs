//! Sender side of the snapshot sync: the export plan of a commit `N` and the
//! chunks it serves. Formats in [`super::snapshot_sync`].
//!
//! [`LvmtStoreReader::prepare_snapshot`] goes once over every item of `N` (each
//! key, through the slot allocations of the depth-1 subtrees in `w0` order, and
//! each non-root AMT) and looks up the commit that last wrote it: its anchor
//! commit. From that it fixes
//!
//! - the anchor commits, ordered by height (the items of a data chunk refer to
//!   them by index), with their state root parts, and the MMR nodes before `N`
//!   that prove their state roots in the `hroot` of `N`;
//! - per anchor commit, the auth-change nodes on the paths of the items it
//!   anchors, in preorder, cut into parts of at most `max_chunk_bytes`; a part is
//!   served with the path from the root to its first node, so every part is
//!   closed under ancestors and checks against the auth-change root alone;
//! - the data chunks: runs of whole `w0` values (a depth-1 subtree is never
//!   split) of at most `max_chunk_bytes`, unless a single `w0` is larger.
//!
//! The plan depends only on the state of `N` and `max_chunk_bytes`, so every peer
//! with the same budget plans the same chunk ids and serves the same bytes.
//! While building it, the item hashes of every anchor commit are held in memory
//! (32 bytes per item); the plan itself keeps one `AuthChangeKey` per used node.

use std::collections::{BTreeMap, HashMap, HashSet};

use ethereum_types::H256;

use crate::{
    backends::{serde::Encode, DatabaseTrait, PendingTableName},
    errors::{Result, SnapshotSyncError},
    middlewares::CommitID,
    StorageError,
};

use super::{
    auth_changes::{amt_change_hash, key_value_hash},
    mmr::MmrProof,
    snapshot_sync::{ChunkId, Manifest, ManifestHeader, SnapshotChunk, W0_COUNT},
    storage::LvmtStoreReader,
    types::{AmtId, AuthChangeKey, AuthChangeNode, LvmtValue},
};

/// `w0` values read per range query in the planning pass, to bound the memory
/// of one range read.
const PLAN_W0_BLOCK: u32 = 256;

/// Bytes of a chunk besides its items: version, kind, chunk id, item counts.
const CHUNK_OVERHEAD: u64 = 2 + 9 + 8;

fn kv_item_len(key: &[u8], value: &LvmtValue) -> u64 {
    (4 + key.len() + 4 + value.encode().len() + 4) as u64
}

fn amt_item_len(amt_id: &AmtId) -> u64 {
    (4 + 2 * amt_id.len() + 8 + 4) as u64
}

fn node_item_len(node: &AuthChangeNode) -> u64 {
    (4 + 4 + 4 + node.encode().len()) as u64
}

fn w0_bounds(w0_start: u32, w0_end: u32) -> (AmtId, Option<AmtId>) {
    let lower = AmtId::root().child(w0_start as u16);
    let upper = (w0_end < W0_COUNT).then(|| AmtId::root().child(w0_end as u16));
    (lower, upper)
}

fn inconsistent(what: &str) -> StorageError {
    StorageError::CorruptedHistoryIndices(format!("snapshot export: {what}"))
}

/// The export of the snapshot of one commit, see [`LvmtStoreReader::prepare_snapshot`].
/// It owns its data: keep it (e.g. per `N`) while serving `N`.
pub struct SnapshotExport {
    manifest: Manifest,
    manifest_bytes: Vec<u8>,
    /// Anchor commit → its index in `manifest.anchor_commits`.
    anchor_index: HashMap<CommitID, u32>,
    /// `anchor_parts[commit_index][part]`: the nodes of the part, in preorder,
    /// without the path to the first one.
    anchor_parts: Vec<Vec<Vec<AuthChangeKey>>>,
    data_chunks: HashSet<(u16, u32)>,
}

impl<P: DatabaseTrait<PendingTableName>> LvmtStoreReader<'_, '_, P> {
    /// Plans the snapshot of `commit` (pending or historical) with chunks of at
    /// most `max_chunk_bytes` (the same value on every peer). Reads every item
    /// of `commit` once.
    ///
    /// Fails with [`SnapshotSyncError::CommitNotFound`] if `commit` is not in the
    /// store; check [`Self::query_commit_existence`] before offering it.
    pub fn prepare_snapshot(
        &self,
        commit: CommitID,
        max_chunk_bytes: usize,
    ) -> Result<SnapshotExport> {
        self.check_snapshot_commit(commit)?;
        let budget = max_chunk_bytes as u64;
        let height = self.commit_height(commit)?;
        let state_root_parts = self.state_root_parts(commit)?;
        let parent_mmr_peaks = if height == 0 {
            vec![]
        } else {
            let parent = self
                .key_value_reader()
                .get_ancestor_at_height(&commit, height - 1)?;
            self.mmr_entry(parent)?.peaks
        };

        // The pass over all items.
        let mut w0_bytes = vec![0u64; W0_COUNT as usize];
        let mut items_by_commit: HashMap<CommitID, Vec<H256>> = HashMap::new();
        let slot_view = self
            .slot_alloc_reader()
            .get_versioned_store(&commit, false)?;
        let amt_view = self.amt_node_reader().get_versioned_store(&commit, false)?;
        for block in (0..W0_COUNT).step_by(PLAN_W0_BLOCK as usize) {
            let (lower, upper) = w0_bounds(block, block + PLAN_W0_BLOCK);
            for (node_id, info) in slot_view.iter_range(lower, upper.clone())? {
                for key in info.keys() {
                    let (c, value) = self
                        .get_key_last_modified(commit, key)?
                        .ok_or_else(|| inconsistent("an allocated key has no value"))?;
                    w0_bytes[node_id[0] as usize] += kv_item_len(key, &value);
                    items_by_commit
                        .entry(c)
                        .or_default()
                        .push(key_value_hash(key, &value));
                }
            }
            for (amt_id, _) in amt_view.iter_range(lower, upper)? {
                let (c, point) = self
                    .get_amt_last_modified(commit, &amt_id)?
                    .ok_or_else(|| inconsistent("an AMT has no commitment"))?;
                w0_bytes[amt_id[0] as usize] += amt_item_len(&amt_id);
                items_by_commit
                    .entry(c)
                    .or_default()
                    .push(amt_change_hash(&amt_id, &point));
            }
        }
        drop(slot_view);
        drop(amt_view);

        // Anchor commits, by height.
        let mut anchors = items_by_commit
            .keys()
            .map(|c| Ok((self.commit_height(*c)?, *c)))
            .collect::<Result<Vec<_>>>()?;
        anchors.sort();
        let mut anchor_commits = Vec::with_capacity(anchors.len());
        let mut anchor_index = HashMap::with_capacity(anchors.len());
        let mut anchor_parts = Vec::with_capacity(anchors.len());
        let mut mmr_nodes = BTreeMap::new();
        for (index, (c_height, c)) in anchors.into_iter().enumerate() {
            let items = items_by_commit.remove(&c).expect("an anchor commit");
            anchor_parts.push(self.plan_anchor_parts(c, &items, budget)?);
            if c != commit {
                let proof = self.prove_hroot_leaf(commit, c)?;
                let positions = MmrProof::sibling_positions(proof.leaf_count, proof.leaf_index)
                    .expect("an anchor commit is a strict ancestor of N");
                mmr_nodes.extend(positions.into_iter().zip(proof.siblings));
            }
            anchor_commits.push((c, c_height, self.state_root_parts(c)?));
            anchor_index.insert(c, index as u32);
        }

        // Chunk ids: anchor chunks, then data chunks.
        let mut chunks = vec![];
        for (commit_index, parts) in anchor_parts.iter().enumerate() {
            for part in 0..parts.len() {
                chunks.push(ChunkId::Anchor {
                    commit_index: commit_index as u32,
                    part: part as u32,
                });
            }
        }
        let mut data_chunks = HashSet::new();
        for (w0_start, w0_end) in plan_data_ranges(&w0_bytes, budget) {
            chunks.push(ChunkId::Data { w0_start, w0_end });
            data_chunks.insert((w0_start, w0_end));
        }

        let manifest = Manifest {
            header: ManifestHeader {
                commit_id: commit,
                height,
                state_root: state_root_parts.state_root(),
            },
            state_root_parts,
            parent_mmr_peaks,
            root_leaf_versions: self.get_root_leaf_versions(commit)?,
            anchor_commits,
            mmr_nodes: mmr_nodes.into_iter().collect(),
            chunks,
        };
        Ok(SnapshotExport {
            manifest_bytes: manifest.to_bytes(),
            manifest,
            anchor_index,
            anchor_parts,
            data_chunks,
        })
    }

    /// The nodes of the auth-change tree of `c` on the paths of `items`, in
    /// preorder, cut into parts.
    fn plan_anchor_parts(
        &self,
        c: CommitID,
        items: &[H256],
        budget: u64,
    ) -> Result<Vec<Vec<AuthChangeKey>>> {
        let mut nodes: HashMap<AuthChangeKey, AuthChangeNode> = HashMap::new();
        for item in items {
            let mut key = AuthChangeKey::root();
            loop {
                if !nodes.contains_key(&key) {
                    let node = self
                        .get_auth_change_node(c, &key)?
                        .ok_or_else(|| inconsistent("a missing auth-change node"))?;
                    nodes.insert(key, node);
                }
                let node = &nodes[&key];
                if node.is_leaf() {
                    if !node.hashes().contains(item) {
                        return Err(inconsistent("an item missing from its anchor tree"));
                    }
                    break;
                }
                key = key.child(
                    node.child_for(item)
                        .ok_or_else(|| inconsistent("an item missing from its anchor tree"))?,
                );
            }
        }

        // Preorder over the used nodes.
        let mut preorder = Vec::with_capacity(nodes.len());
        let mut stack = vec![AuthChangeKey::root()];
        while let Some(key) = stack.pop() {
            let node = &nodes[&key];
            if !node.is_leaf() {
                stack.extend(
                    (0..node.hashes().len())
                        .rev()
                        .map(|i| key.child(i))
                        .filter(|child| nodes.contains_key(child)),
                );
            }
            preorder.push(key);
        }

        let size = |key: &AuthChangeKey| node_item_len(&nodes[key]);
        let path_size = |key: &AuthChangeKey| {
            let mut total = 0;
            let mut k = *key;
            while let Some((parent, _)) = k.parent() {
                total += size(&parent);
                k = parent;
            }
            total
        };
        let mut parts: Vec<Vec<AuthChangeKey>> = vec![];
        let mut current: Vec<AuthChangeKey> = vec![];
        let mut current_bytes = 0;
        for key in preorder {
            let node_bytes = size(&key);
            if !current.is_empty() && current_bytes + node_bytes > budget {
                parts.push(std::mem::take(&mut current));
            }
            if current.is_empty() {
                current_bytes = CHUNK_OVERHEAD + path_size(&key);
            }
            current.push(key);
            current_bytes += node_bytes;
        }
        if !current.is_empty() {
            parts.push(current);
        }
        Ok(parts)
    }

    pub(super) fn check_snapshot_commit(&self, commit: CommitID) -> Result<()> {
        if !self.query_commit_existence(&commit)? {
            return Err(SnapshotSyncError::CommitNotFound(commit).into());
        }
        Ok(())
    }
}

/// Runs of whole `w0` values of at most `budget` bytes, unless one `w0` alone
/// is larger. `w0` values without items join the run they fall in.
fn plan_data_ranges(w0_bytes: &[u64], budget: u64) -> Vec<(u16, u32)> {
    let mut ranges = vec![];
    let mut start = 0u32;
    let mut bytes = CHUNK_OVERHEAD;
    for (w0, &b) in w0_bytes.iter().enumerate() {
        let w0 = w0 as u32;
        if w0 > start && bytes + b > budget {
            ranges.push((start as u16, w0));
            start = w0;
            bytes = CHUNK_OVERHEAD;
        }
        bytes += b;
    }
    ranges.push((start as u16, W0_COUNT));
    ranges
}

impl SnapshotExport {
    pub fn header(&self) -> ManifestHeader {
        self.manifest.header
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The encoded manifest; [`SnapshotSyncError::ManifestTooLarge`] above
    /// `max_manifest_bytes`.
    pub fn manifest_bytes(&self, max_manifest_bytes: usize) -> Result<Vec<u8>> {
        if self.manifest_bytes.len() > max_manifest_bytes {
            return Err(SnapshotSyncError::ManifestTooLarge {
                size: self.manifest_bytes.len(),
                max: max_manifest_bytes,
            }
            .into());
        }
        Ok(self.manifest_bytes.clone())
    }

    /// The encoded chunk `chunk_id` (as listed in the manifest), read through
    /// `reader` (a reader of the store the export was prepared on), at most
    /// `max_bytes` long.
    pub fn load_chunk<P: DatabaseTrait<PendingTableName>>(
        &self,
        reader: &LvmtStoreReader<'_, '_, P>,
        chunk_id: &[u8],
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        let commit = self.manifest.header.commit_id;
        reader.check_snapshot_commit(commit)?;
        let chunk = match ChunkId::from_bytes(chunk_id)? {
            ChunkId::Anchor { commit_index, part } => {
                let keys = self
                    .anchor_parts
                    .get(commit_index as usize)
                    .and_then(|parts| parts.get(part as usize))
                    .ok_or(SnapshotSyncError::UnknownChunk)?;
                let c = self.manifest.anchor_commits[commit_index as usize].0;
                let mut path = vec![];
                let mut k = keys[0];
                while let Some((parent, _)) = k.parent() {
                    path.push(parent);
                    k = parent;
                }
                let nodes = path
                    .into_iter()
                    .rev()
                    .chain(keys.iter().copied())
                    .map(|key| {
                        let node = reader
                            .get_auth_change_node(c, &key)?
                            .ok_or_else(|| inconsistent("a missing auth-change node"))?;
                        Ok((key, node))
                    })
                    .collect::<Result<_>>()?;
                SnapshotChunk::Anchor {
                    commit_index,
                    part,
                    nodes,
                }
            }
            ChunkId::Data { w0_start, w0_end } => {
                if !self.data_chunks.contains(&(w0_start, w0_end)) {
                    return Err(SnapshotSyncError::UnknownChunk.into());
                }
                self.load_data_chunk(reader, w0_start, w0_end)?
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

    fn load_data_chunk<P: DatabaseTrait<PendingTableName>>(
        &self,
        reader: &LvmtStoreReader<'_, '_, P>,
        w0_start: u16,
        w0_end: u32,
    ) -> Result<SnapshotChunk> {
        let commit = self.manifest.header.commit_id;
        let anchor_of = |c: CommitID| {
            self.anchor_index
                .get(&c)
                .copied()
                .ok_or_else(|| inconsistent("an item anchored outside the plan"))
        };
        let (lower, upper) = w0_bounds(w0_start as u32, w0_end);
        let mut flat_kv_pairs = vec![];
        let slot_view = reader
            .slot_alloc_reader()
            .get_versioned_store(&commit, false)?;
        for (_, info) in slot_view.iter_range(lower, upper.clone())? {
            for key in info.keys() {
                let (c, value) = reader
                    .get_key_last_modified(commit, key)?
                    .ok_or_else(|| inconsistent("an allocated key has no value"))?;
                flat_kv_pairs.push((key.clone(), value, anchor_of(c)?));
            }
        }
        let mut amt_versions = vec![];
        let amt_view = reader
            .amt_node_reader()
            .get_versioned_store(&commit, false)?;
        for (amt_id, point) in amt_view.iter_range(lower, upper)? {
            let (c, _) = reader
                .get_amt_last_modified(commit, &amt_id)?
                .ok_or_else(|| inconsistent("an AMT has no commitment"))?;
            amt_versions.push((amt_id, point.version, anchor_of(c)?));
        }
        Ok(SnapshotChunk::Data {
            w0_start,
            w0_end,
            flat_kv_pairs,
            amt_versions,
        })
    }
}
