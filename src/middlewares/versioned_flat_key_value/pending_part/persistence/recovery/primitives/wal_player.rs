use std::collections::HashMap;

use crate::middlewares::versioned_flat_key_value::pending_part::persistence::format::WalTable;

use super::{
    ModificationId, PendingKeyValueSchema, RecoverMap, RecoveryError, Result, SnapshotId,
    TableRead, Tree, WalKey, WalKeySpecificPart, WalValue,
};

/// Represents a single, fully-assembled modification operation to be applied to the Tree.
/// This structure is built by `collect_modification_info` from one or more `WalKey`/`WalValue` pairs.
#[derive(Debug)]
pub enum OneModificationForTree<S: PendingKeyValueSchema> {
    /// Represents an `add_root` or `add_non_root_node` operation.
    AddNode {
        commit_id: S::CommitId,
        /// `None` for a root node, `Some` for a non-root node.
        parent_commit_id: Option<S::CommitId>,
        /// The collected key-value modifications for this node.
        modifications: RecoverMap<S>,
        /// The number of `MapValue` records we expect to find, for validation.
        expected_map_value_count: u64,
    },
    /// Represents a `change_root` operation.
    ChangeRoot { commit_id: S::CommitId },
    /// Represents a `make_pivot` operation.
    MakePivot { commit_id: S::CommitId },
    /// Represents a `discard` operation.
    Discard { commit_id: S::CommitId },
}

/// The status of a single WAL modification replay attempt.
pub(super) enum ReplayStatus {
    /// A modification was successfully found and applied to the tree.
    Applied,
    /// No more modifications were found for the current snapshot_id.
    NoMoreModifications,
    /// A `change_root` operation was encountered, signaling the end of this snapshot's valid WAL.
    /// This represents an incomplete transaction that needs to be rolled back.
    ChangeRootEncountered,
}

/// Attempts to replay all WAL records corresponding to a single modification_id.
///
/// Returns a [`ReplayStatus`] indicating the outcome:
/// - `Applied`: A modification was successfully found and applied.
/// - `NoMoreModifications`: No records for the given modification_id were found.
/// - `ChangeRootEncountered`: A `change_root` operation was found and NOT applied.
pub(super) fn replay_one_modification<S: PendingKeyValueSchema, P: TableRead<WalTable<S>>>(
    wal_view: &P,
    tree: &mut Tree<S>,
    snapshot_id: SnapshotId,
    modification_id: ModificationId,
) -> Result<ReplayStatus> {
    let mut this_modification: Option<OneModificationForTree<S>> = None;
    let wal_seek_key = WalKey::seek_key_for_snap_mod_id(snapshot_id, modification_id);

    // Iterate over WAL records for the current modification_id.
    for wal_item in wal_view.iter(&wal_seek_key.key)? {
        let (wal_key_cow, wal_value_cow) = wal_item?;
        let wal_key = wal_key_cow.into_owned();

        // If the WAL record does not belong to the current modification being processed, stop.
        if wal_key.snapshot_id != snapshot_id || wal_key.modification_id != modification_id {
            break;
        }

        // We can consume the item now.
        let wal_value = wal_value_cow.into_owned();
        collect_modification_info(&mut this_modification, wal_key, wal_value)?;
    }

    if let Some(modification) = this_modification {
        // A modification was successfully constructed. Now, decide what to do with it.
        match modification {
            // For a ChangeRoot operation, we do NOT apply it.
            // Instead, we signal that an incomplete transaction was found.
            OneModificationForTree::ChangeRoot { .. } => Ok(ReplayStatus::ChangeRootEncountered),
            // For all other valid operations, apply them to the tree.
            _ => {
                check_modification_info(&modification)?;
                apply_wal_to_tree(tree, modification)?;
                Ok(ReplayStatus::Applied) // Indicates one modification was successfully processed.
            }
        }
    } else {
        // If `this_modification` is None, it means there are no records for the current modification_id.
        Ok(ReplayStatus::NoMoreModifications) // Indicates that there are no more modifications to process.
    }
}

/// Checks if a fully collected modification is valid.
/// For `AddNode`, it ensures the number of map values matches the metadata's count.
/// For other types, it's a no-op as they are self-contained.
fn check_modification_info<S: PendingKeyValueSchema>(
    modification: &OneModificationForTree<S>,
) -> Result<()> {
    match modification {
        OneModificationForTree::AddNode {
            modifications,
            expected_map_value_count,
            ..
        } => {
            let got = modifications.len() as u64;
            if got != *expected_map_value_count {
                Err(RecoveryError::WalMapValueCountMismatch {
                    expected: *expected_map_value_count,
                    got,
                })?
            } else {
                Ok(())
            }
        }
        // Other modification types are built from a single WAL record,
        // so no cross-record validation is needed.
        _ => Ok(()),
    }
}

/// Collects individual WAL records into a single logical `OneModificationForTree`.
/// This function acts as a state machine, starting a new modification or adding to an existing one.
fn collect_modification_info<S: PendingKeyValueSchema>(
    modification: &mut Option<OneModificationForTree<S>>,
    wal_key: WalKey<S>,
    wal_value: WalValue<S>,
) -> Result<()> {
    match modification {
        // If no modification is in progress, this WAL record must be a "Meta" record that starts one.
        None => {
            match (wal_key.operation_specific_parts, wal_value) {
                // Start of an AddNode operation
                (
                    WalKeySpecificPart::AddNodeMeta,
                    WalValue::MetaValue {
                        commit_id,
                        maybe_parent_cid,
                        map_value_count,
                    },
                ) => {
                    *modification = Some(OneModificationForTree::AddNode {
                        commit_id,
                        parent_commit_id: maybe_parent_cid,
                        modifications: HashMap::new(),
                        expected_map_value_count: map_value_count,
                    });
                    Ok(())
                }
                // A ChangeRoot operation (self-contained)
                (WalKeySpecificPart::ChangeRootMeta, WalValue::MetaValue { commit_id, .. }) => {
                    *modification = Some(OneModificationForTree::ChangeRoot { commit_id });
                    Ok(())
                }
                // A MakePivot operation (self-contained)
                (WalKeySpecificPart::MakePivotMeta, WalValue::MetaValue { commit_id, .. }) => {
                    *modification = Some(OneModificationForTree::MakePivot { commit_id });
                    Ok(())
                }
                // A Discard operation (self-contained)
                (WalKeySpecificPart::DiscardMeta, WalValue::MetaValue { commit_id, .. }) => {
                    *modification = Some(OneModificationForTree::Discard { commit_id });
                    Ok(())
                }
                // Any other combination is an error when starting a new modification.
                _ => Err(RecoveryError::UnexpectedWalRecord)?,
            }
        }
        // If we are already building an AddNode modification, we expect to see MapValue records.
        Some(OneModificationForTree::AddNode { modifications, .. }) => {
            match (wal_key.operation_specific_parts, wal_value) {
                (WalKeySpecificPart::AddNodeMapKey(key), WalValue::MapValue(record)) => {
                    modifications.insert(key, record);
                    Ok(())
                }
                // Receiving another type of record while building an AddNode is an error.
                _ => Err(RecoveryError::InconsistentWalRecord(
                    "Expected AddNodeMapKey and MapValue while building a node",
                ))?,
            }
        }
        // If we are in any other state, receiving more records for the same modification ID is an error,
        // because ChangeRoot, MakePivot, and Discard are single-record operations.
        Some(_) => Err(RecoveryError::InconsistentWalRecord(
            "Received multiple WAL records for a single-record operation type",
        ))?,
    }
}

/// Applies the fully validated `OneModificationForTree` to the `Tree` instance.
/// It matches on the modification type and calls the corresponding `Tree` method.
fn apply_wal_to_tree<S: PendingKeyValueSchema>(
    tree: &mut Tree<S>,
    modification: OneModificationForTree<S>,
) -> Result<()> {
    match modification {
        OneModificationForTree::AddNode {
            commit_id,
            parent_commit_id,
            modifications,
            ..
        } => {
            match parent_commit_id {
                // If there's no parent, it's a new root.
                None => {
                    tree.add_root(commit_id, modifications)?;
                }
                // If there is a parent, it's a non-root node.
                Some(parent_cid) => {
                    tree.add_non_root_node(commit_id, parent_cid, modifications)?;
                }
            }
        }
        OneModificationForTree::ChangeRoot { commit_id } => {
            tree.change_root(commit_id)?;
        }
        OneModificationForTree::MakePivot { commit_id } => {
            tree.make_pivot(commit_id)?;
        }
        OneModificationForTree::Discard { commit_id } => {
            tree.discard(commit_id)?;
        }
    }
    Ok(())
}
