use blake2::{Blake2s, Digest};
use ethereum_types::H256;

use crate::{
    backends::{serde::Encode, PendingTableName, TableSchema},
    middlewares::CommitID,
};

use super::types::CurvePointWithVersion;

pub type StateRoot = H256;

/// Domain tag of the state root hash. Disjoint from the leaf tags of the
/// auth-change tree (`0` for key-value changes, `1` for AMT changes).
const STATE_ROOT_FLAG: u8 = 2;

/// The auth-change root of a commit without any change (empty tree).
pub const EMPTY_AUTH_CHANGE_ROOT: H256 = H256::zero();

/// `state_root = blake2s(STATE_ROOT_FLAG ‖ encode(root AMT commitment) ‖ auth-change root)`.
///
/// The root AMT commitment is excluded from the auth-change tree, so it enters
/// the state root here directly. The auth-change root is the hash of the root
/// node of this commit's auth-change tree, or [`EMPTY_AUTH_CHANGE_ROOT`] if the
/// commit has no change.
pub fn compute_state_root(
    root_commitment: &CurvePointWithVersion,
    auth_change_root: &H256,
) -> StateRoot {
    let mut hasher = Blake2s::new();
    hasher.update([STATE_ROOT_FLAG]);
    hasher.update(root_commitment.encode().as_ref());
    hasher.update(auth_change_root.as_bytes());
    H256(hasher.finalize().into())
}
#[derive(Clone, Copy)]
pub struct StateRootTable;
impl TableSchema for StateRootTable {
    type TableName = PendingTableName;
    const NAME: PendingTableName = PendingTableName::StateRoot;

    type Key = CommitID;
    type Value = StateRoot;
}
