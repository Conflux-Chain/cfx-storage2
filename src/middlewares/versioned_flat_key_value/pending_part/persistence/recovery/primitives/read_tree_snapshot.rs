use std::collections::HashMap;
use thiserror::Error;

use super::{
    PendingKeyValueSchema, RecoverMap, Result, SnapshotId, SnapshotKey, SnapshotMapValue,
    SnapshotNodeDataType, SnapshotRecordType, SnapshotValue, SnapshotsTable, TableItem, TableIter,
    Tree, TreeSnapshot, TreeSnapshotNode,
};

/// The `iter` is exactly after the snapshot Meta record.
/// The `snapshot_root_height` is obtained from the snapshot Meta key.
/// The `parent_of_root`, `snapshot_id`, and `nodes_count_hint` are obtained from the snapshot Meta value.
#[allow(clippy::type_complexity)]
pub fn read_tree_from_snapshot_iter<'b, S: PendingKeyValueSchema>(
    snapshot_root_height: u64,
    parent_of_root: Option<S::CommitId>,
    snapshot_id: SnapshotId,
    nodes_count_hint: u64,
    iter: &mut TableIter<'_, 'b, SnapshotsTable<S>>,
) -> Result<(Tree<S>, Option<TableItem<'b, SnapshotsTable<S>>>)> {
    // 1) Aggregate each node in order: first see a NodeMeta, followed by its NodeMap records
    let mut nodes: Vec<TreeSnapshotNode<S>> =
        Vec::with_capacity(usize::try_from(nodes_count_hint).unwrap_or_default());

    // Tracks the node currently being built
    struct BuildingNode<S: PendingKeyValueSchema> {
        key_height: u64,
        key_commit: S::CommitId,
        parent_commit_id: Option<S::CommitId>,
        modifications: RecoverMap<S>,
        expected_mods: Option<u64>, // From NodeMeta.modifications_count
    }

    let mut current: Option<BuildingNode<S>> = None;
    let mut maybe_last_key: Option<SnapshotKey<S>> = None;
    let mut maybe_next_item = None;

    for item_res in iter {
        let (k_cow, v_cow) = item_res?;
        let k = k_cow.as_ref();

        // The root height must be consistent
        if k.snapshot_root_height != snapshot_root_height {
            if k.snapshot_root_height < snapshot_root_height {
                Err(SnapshotReadError::SnapshotOutOfOrder)?
            }

            maybe_next_item = Some((k_cow, v_cow));
            break;
        }

        // Validate the overall order: ascending by (node_height, node_commit_id, NodeMeta/NodeMap)
        if let Some(last_key) = maybe_last_key {
            if k < &last_key {
                Err(SnapshotReadError::NodeRecordOutOfOrder)?
            }
        }
        maybe_last_key = Some(k.clone());

        let SnapshotRecordType::Map(part) = &k.record_type else {
            Err(SnapshotReadError::UnexpectedMeta)?
        };

        let node_height = part.node_height;
        let node_commit_id = part.node_commit_id;

        match &part.node_data_type {
            // Start a new node: must encounter a NodeMeta first, and its value must exist
            SnapshotNodeDataType::NodeMeta => {
                // If there is an unfinished node, finalize it and push it to nodes
                if let Some(bn) = current.take() {
                    // Validate the count (if an expected value is provided)
                    if let Some(exp) = bn.expected_mods {
                        if exp != bn.modifications.len() as u64 {
                            Err(SnapshotReadError::ModificationCountMismatch)?
                        }
                    } else {
                        Err(SnapshotReadError::ModificationCountIsAbsent)?
                    }

                    nodes.push(TreeSnapshotNode {
                        node_height: bn.key_height,
                        node_commit_id: bn.key_commit,
                        node_parent_commit_id: bn.parent_commit_id,
                        modifications: bn.modifications,
                    });
                }

                // Parse the value of the current NodeMeta
                let SnapshotValue::MapValue(SnapshotMapValue::NodeMeta {
                    parent_commit_id,
                    modifications_count,
                }) = v_cow.as_ref()
                else {
                    Err(SnapshotReadError::WrongValueForNodeMeta)?
                };

                // Start a new builder
                current = Some(BuildingNode::<S> {
                    key_height: node_height,
                    key_commit: node_commit_id,
                    parent_commit_id: *parent_commit_id,
                    modifications: HashMap::with_capacity(
                        usize::try_from(*modifications_count).unwrap_or(0),
                    ),
                    expected_mods: Some(*modifications_count),
                });
            }

            // Collect NodeMap: a corresponding NodeMeta must already be present for the same node
            SnapshotNodeDataType::NodeMap { key: node_mod_key } => {
                let Some(bn) = current.as_mut() else {
                    Err(SnapshotReadError::NodeMapWithoutNodeMeta)?
                };
                if bn.key_height != node_height || bn.key_commit != node_commit_id {
                    Err(SnapshotReadError::InterleavedNodes)?
                }

                let SnapshotValue::MapValue(SnapshotMapValue::NodeMap(node_mod_rec)) =
                    v_cow.as_ref()
                else {
                    Err(SnapshotReadError::WrongValueForNodeMap)?
                };

                bn.modifications
                    .insert(node_mod_key.clone(), node_mod_rec.clone());
            }
        }
    }

    // 2) Finalize the last node at the end
    if let Some(bn) = current.take() {
        if let Some(exp) = bn.expected_mods {
            if exp != bn.modifications.len() as u64 {
                Err(SnapshotReadError::ModificationCountMismatch)?
            }
        } else {
            Err(SnapshotReadError::ModificationCountIsAbsent)?
        }
        nodes.push(TreeSnapshotNode {
            node_height: bn.key_height,
            node_commit_id: bn.key_commit,
            node_parent_commit_id: bn.parent_commit_id,
            modifications: bn.modifications,
        });
    }

    // 3) Validate against the nodes_count from the global Meta
    if nodes_count_hint != nodes.len() as u64 {
        Err(SnapshotReadError::NodesCountMismatch)?
    }

    // 4) Construct the TreeSnapshot and restore the Tree
    let snapshot = TreeSnapshot {
        parent_of_root,
        height_of_root: snapshot_root_height,
        nodes,
    };

    Ok((Tree::from_snapshot(snapshot)?, maybe_next_item))
}

/// Errors that can occur while reading a tree from a snapshot.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotReadError {
    #[error("Unexpected Meta record")]
    UnexpectedMeta,
    #[error("Mixed snapshot root heights detected in records")]
    MixedRootHeights,
    #[error("Snapshot records are out of order")]
    SnapshotOutOfOrder,
    #[error("Snapshot node records are out of order")]
    NodeRecordOutOfOrder,
    #[error("NodeMap encountered without a preceding NodeMeta")]
    NodeMapWithoutNodeMeta,
    #[error("Node records are interleaved")]
    InterleavedNodes,
    #[error("Incorrect value variant for NodeMeta")]
    WrongValueForNodeMeta,
    #[error("Incorrect value variant for NodeMap")]
    WrongValueForNodeMap,
    #[error("Tombstone found on a NodeMeta record")]
    TombstoneOnNodeMeta,
    #[error("Tombstone found on a NodeMap record")]
    TombstoneOnNodeMap,
    #[error("Node modification count mismatch")]
    ModificationCountMismatch,
    #[error("Node modification count is absent")]
    ModificationCountIsAbsent,
    #[error("Total nodes count mismatch")]
    NodesCountMismatch,
}
