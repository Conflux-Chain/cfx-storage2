//! Checkpoints of the root AMT leaf values in the historical part.
//!
//! The leaf values of the root AMT are the versions of the depth-1 AMTs (slot 5
//! of each root leaf, see [`AmtChangeManager::compute_amt_changes`]). Reading all
//! 65536 of them at an old historical commit takes one historical point lookup
//! each, which is slow. Instead, every `checkpoint_interval` historical commits the
//! whole vector is stored in [`RootLeafCheckpointTable`], written in the same
//! historical write batch as the confirmed commits. The versions at a commit near
//! a checkpoint are recovered by counting the depth-1 AMT changes in between: a
//! version goes up by exactly one per changing commit, in either direction.
//!
//! A missing checkpoint (e.g. after the interval is changed) only makes the read
//! slower: the reader falls back to the historical tip or to direct lookups.
//!
//! [`AmtChangeManager::compute_amt_changes`]: super::amt_change_manager::AmtChangeManager::compute_amt_changes

use std::borrow::Cow;

use crate::{
    backends::{
        serde::{Decode, Encode},
        HistoricalTableName, TableSchema,
    },
    errors::{DecResult, DecodeError},
    middlewares::HistoryNumber,
};

/// Number of leaves of the root AMT.
pub const ROOT_LEAF_COUNT: usize = 1 << 16;

/// Knobs of the historical proof reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProofConfig {
    /// `K`: how many commits below the historical tip proofs are served for.
    /// Checkpoints older than that (plus one interval) are pruned at confirmation.
    pub history_depth: u64,
    /// `S`: a checkpoint is stored at every historical commit whose height is a
    /// multiple of this. Must be positive.
    pub checkpoint_interval: u64,
}

impl Default for ProofConfig {
    fn default() -> Self {
        Self {
            history_depth: 2000,
            checkpoint_interval: 100,
        }
    }
}

impl ProofConfig {
    pub(super) fn is_checkpoint(&self, history_number: HistoryNumber) -> bool {
        history_number % self.checkpoint_interval == 0
    }

    /// Checkpoints below this history number are no longer needed once the
    /// historical tip is `tip`.
    pub(super) fn prune_below(&self, tip: HistoryNumber) -> HistoryNumber {
        tip.saturating_sub(self.history_depth + self.checkpoint_interval)
    }
}

/// The root AMT leaf versions at one commit, entry `i` for leaf `i`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootLeafVersions(pub Vec<u64>);

impl Encode for RootLeafVersions {
    fn encode(&self) -> Cow<[u8]> {
        Cow::Owned(self.0.iter().flat_map(|v| v.to_le_bytes()).collect())
    }
}

impl Decode for RootLeafVersions {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() != ROOT_LEAF_COUNT * 8 {
            return Err(DecodeError::IncorrectLength);
        }
        Ok(Cow::Owned(RootLeafVersions(
            input
                .chunks_exact(8)
                .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                .collect(),
        )))
    }
}

/// `history number → root AMT leaf versions at that commit`.
#[derive(Clone, Copy)]
pub struct RootLeafCheckpointTable;

impl TableSchema for RootLeafCheckpointTable {
    type TableName = HistoricalTableName;
    const NAME: HistoricalTableName = HistoricalTableName::RootLeafCheckpoint;

    type Key = HistoryNumber;
    type Value = RootLeafVersions;
}
