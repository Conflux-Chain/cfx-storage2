//! The history accumulator `hroot`: a Merkle Mountain Range over the state roots
//! of a commit's ancestors.
//!
//! Leaf `h` of the MMR of commit `N` is the state root of `N`'s ancestor at
//! height `h`; `hroot_N` accumulates the leaves `0..height(N)` (not `N` itself, as
//! `hroot_N` is part of `state_root_N`). The MMR is defined per chain: each
//! commit appends its own state root to its parent's MMR, so forks need no
//! special handling.
//!
//! Node `(l, j)` covers leaves `[j·2^l, (j+1)·2^l)` and is created when leaf
//! `(j+1)·2^l − 1` is appended. Every commit stores, in [`MmrTable`], the nodes it
//! created (levels `0..=t`) and the peaks after its append. A node is found from
//! a descendant by locating the ancestor at the height of the leaf that created
//! it.
//!
//! Hashes (blake2s, with domain tags disjoint from the auth-change tree and the
//! state root):
//! - leaf: `blake2s(4 ‖ state_root)`;
//! - inner node: `blake2s(5 ‖ left ‖ right)`;
//! - root ("bagging"): `blake2s(3 ‖ leaf_count as u64 BE ‖ peak_1 ‖ … ‖ peak_k)`,
//!   peaks from the highest (leftmost) to the lowest, or [`EMPTY_HROOT`] without
//!   leaves. The leaf count fixes the peak layout a proof is checked against.

use std::borrow::Cow;

use blake2::{Blake2s, Digest};
use ethereum_types::H256;

use crate::{
    backends::{
        serde::{Decode, Encode},
        PendingTableName, TableSchema,
    },
    errors::{DecResult, DecodeError},
    middlewares::CommitID,
};

use super::state_root::StateRoot;

const MMR_BAG_FLAG: u8 = 3;
const MMR_LEAF_FLAG: u8 = 4;
const MMR_NODE_FLAG: u8 = 5;

/// `hroot` of a commit without ancestors.
pub const EMPTY_HROOT: H256 = H256::zero();

pub fn mmr_leaf_hash(state_root: &StateRoot) -> H256 {
    let mut hasher = Blake2s::new();
    hasher.update([MMR_LEAF_FLAG]);
    hasher.update(state_root.as_bytes());
    H256(hasher.finalize().into())
}

fn mmr_node_hash(left: &H256, right: &H256) -> H256 {
    let mut hasher = Blake2s::new();
    hasher.update([MMR_NODE_FLAG]);
    hasher.update(left.as_bytes());
    hasher.update(right.as_bytes());
    H256(hasher.finalize().into())
}

/// The root of an MMR with `leaf_count` leaves and the given peaks.
pub fn bag_peaks(leaf_count: u64, peaks: &[H256]) -> H256 {
    if leaf_count == 0 {
        return EMPTY_HROOT;
    }
    let mut hasher = Blake2s::new();
    hasher.update([MMR_BAG_FLAG]);
    hasher.update(leaf_count.to_be_bytes());
    for peak in peaks {
        hasher.update(peak.as_bytes());
    }
    H256(hasher.finalize().into())
}

/// The peak of an MMR with `leaf_count` leaves that contains `leaf_index`:
/// `(position among the peaks, level, index of its first leaf)`.
pub(super) fn locate_peak(leaf_count: u64, leaf_index: u64) -> Option<(usize, u32, u64)> {
    if leaf_index >= leaf_count {
        return None;
    }
    let mut start = 0u64;
    let mut position = 0;
    for level in (0..u64::BITS).rev() {
        if leaf_count & (1 << level) == 0 {
            continue;
        }
        if leaf_index < start + (1 << level) {
            return Some((position, level, start));
        }
        start += 1 << level;
        position += 1;
    }
    unreachable!("leaf_index < leaf_count")
}

/// The height of the leaf whose append creates node `(level, index)`.
pub(super) fn creating_leaf(level: u32, index: u64) -> u64 {
    ((index + 1) << level) - 1
}

/// What one commit adds to its chain's MMR.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MmrEntry {
    /// The nodes created by appending this commit's leaf, `created[l]` at level `l`
    /// (`created[0]` is the leaf hash).
    pub created: Vec<H256>,
    /// The peaks after the append, from the highest to the lowest.
    pub peaks: Vec<H256>,
}

impl MmrEntry {
    /// Appends `state_root` as leaf `leaf_index` to the MMR whose peaks are
    /// `parent_peaks` (the MMR of leaves `0..leaf_index`).
    pub fn append(parent_peaks: &[H256], leaf_index: u64, state_root: &StateRoot) -> Self {
        debug_assert_eq!(parent_peaks.len(), leaf_index.count_ones() as usize);
        let mut peaks = parent_peaks.to_vec();
        let mut node = mmr_leaf_hash(state_root);
        let mut created = vec![node];
        let mut n = leaf_index;
        while n & 1 == 1 {
            let left = peaks.pop().expect("one peak per set bit");
            node = mmr_node_hash(&left, &node);
            created.push(node);
            n >>= 1;
        }
        peaks.push(node);
        Self { created, peaks }
    }
}

impl Encode for MmrEntry {
    fn encode(&self) -> Cow<[u8]> {
        let mut out = Vec::with_capacity(1 + 32 * (self.created.len() + self.peaks.len()));
        out.push(self.created.len() as u8);
        for h in self.created.iter().chain(&self.peaks) {
            out.extend_from_slice(h.as_bytes());
        }
        Cow::Owned(out)
    }
}

impl Decode for MmrEntry {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        let (&n_created, rest) = input.split_first().ok_or(DecodeError::IncorrectLength)?;
        if rest.len() % 32 != 0 || rest.len() / 32 < n_created as usize {
            return Err(DecodeError::IncorrectLength);
        }
        let mut hashes = rest.chunks_exact(32).map(H256::from_slice);
        let created = hashes.by_ref().take(n_created as usize).collect();
        let peaks = hashes.collect();
        Ok(Cow::Owned(Self { created, peaks }))
    }
}

/// `commit id → MmrEntry`, in the pending db next to the state root, never pruned.
#[derive(Clone, Copy)]
pub struct MmrTable;

impl TableSchema for MmrTable {
    type TableName = PendingTableName;
    const NAME: PendingTableName = PendingTableName::Mmr;

    type Key = CommitID;
    type Value = MmrEntry;
}

/// Proof that a state root is leaf `leaf_index` of an `hroot` over `leaf_count`
/// leaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MmrProof {
    pub leaf_count: u64,
    pub leaf_index: u64,
    /// Siblings on the path from the leaf up to its peak, lowest first.
    pub siblings: Vec<H256>,
    /// All peaks of the MMR, from the highest to the lowest.
    pub peaks: Vec<H256>,
}

impl MmrProof {
    /// Whether `state_root` is leaf `leaf_index` of `hroot`.
    pub fn verify(&self, hroot: &H256, state_root: &StateRoot) -> bool {
        let Some((position, level, start)) = locate_peak(self.leaf_count, self.leaf_index) else {
            return false;
        };
        if self.siblings.len() != level as usize
            || self.peaks.len() != self.leaf_count.count_ones() as usize
        {
            return false;
        }
        let offset = self.leaf_index - start;
        let mut node = mmr_leaf_hash(state_root);
        for (l, sibling) in self.siblings.iter().enumerate() {
            node = if (offset >> l) & 1 == 0 {
                mmr_node_hash(&node, sibling)
            } else {
                mmr_node_hash(sibling, &node)
            };
        }
        self.peaks[position] == node && bag_peaks(self.leaf_count, &self.peaks) == *hroot
    }

    /// The nodes `(level, index)` the proof's siblings are, lowest first.
    pub(super) fn sibling_positions(leaf_count: u64, leaf_index: u64) -> Option<Vec<(u32, u64)>> {
        let (_, level, _) = locate_peak(leaf_count, leaf_index)?;
        Some((0..level).map(|l| (l, (leaf_index >> l) ^ 1)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(i: u64) -> H256 {
        H256::from_low_u64_be(i + 1)
    }

    /// Reference root: build every level explicitly.
    fn naive_root(leaves: &[H256]) -> H256 {
        let n = leaves.len() as u64;
        let mut peaks = vec![];
        let mut start = 0usize;
        for level in (0..u64::BITS).rev() {
            if n & (1 << level) == 0 {
                continue;
            }
            let mut layer: Vec<H256> = leaves[start..start + (1 << level)]
                .iter()
                .map(mmr_leaf_hash)
                .collect();
            while layer.len() > 1 {
                layer = layer
                    .chunks(2)
                    .map(|p| mmr_node_hash(&p[0], &p[1]))
                    .collect();
            }
            peaks.push(layer[0]);
            start += 1 << level;
        }
        bag_peaks(n, &peaks)
    }

    #[test]
    fn test_mmr_append_and_prove() {
        let leaves: Vec<H256> = (0..70).map(leaf).collect();
        let mut entries: Vec<MmrEntry> = vec![];
        for (i, l) in leaves.iter().enumerate() {
            let parent_peaks = entries.last().map_or(&[][..], |e| &e.peaks[..]);
            let entry = MmrEntry::append(parent_peaks, i as u64, l);
            assert_eq!(
                bag_peaks(i as u64 + 1, &entry.peaks),
                naive_root(&leaves[..=i])
            );
            let decoded = MmrEntry::decode(&entry.encode()).unwrap().into_owned();
            assert_eq!(decoded, entry);
            entries.push(entry);
        }

        for n in 1..=70u64 {
            let hroot = bag_peaks(n, &entries[n as usize - 1].peaks);
            for c in 0..n {
                let siblings = MmrProof::sibling_positions(n, c)
                    .unwrap()
                    .into_iter()
                    .map(|(l, j)| entries[creating_leaf(l, j) as usize].created[l as usize])
                    .collect();
                let proof = MmrProof {
                    leaf_count: n,
                    leaf_index: c,
                    siblings,
                    peaks: entries[n as usize - 1].peaks.clone(),
                };
                assert!(
                    proof.verify(&hroot, &leaves[c as usize]),
                    "n = {n}, c = {c}"
                );
                assert!(!proof.verify(&hroot, &leaf(1000)));
                let mut wrong = proof.clone();
                wrong.leaf_index = (c + 1) % n;
                assert!(n == 1 || !wrong.verify(&hroot, &leaves[c as usize]));
                let mut wrong = proof.clone();
                wrong.leaf_count = n + 1;
                assert!(!wrong.verify(&hroot, &leaves[c as usize]));
            }
        }
        assert_eq!(bag_peaks(0, &[]), EMPTY_HROOT);
    }
}
