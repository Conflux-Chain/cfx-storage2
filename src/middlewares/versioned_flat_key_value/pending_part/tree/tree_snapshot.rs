use crate::middlewares::{
    versioned_flat_key_value::pending_part::pending_schema::{PendingKeyValueSchema, RecoverMap},
    PendingError,
};

use super::{PendResult, SlabIndex, Tree};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeSnapshot<S: PendingKeyValueSchema> {
    pub parent_of_root: Option<S::CommitId>,
    pub height_of_root: u64,
    // Nodes are sorted by (height, commit_id) in increasing order.
    pub nodes: Vec<TreeSnapshotNode<S>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeSnapshotNode<S: PendingKeyValueSchema> {
    pub node_height: u64,
    pub node_commit_id: S::CommitId,
    /// Only consider nodes in tree. I.e., for the root, `node_parent_commit_id` is None, rather than the `parent_of_root`.
    pub node_parent_commit_id: Option<S::CommitId>,
    pub modifications: RecoverMap<S>,
}

impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn export_snapshot(&self) -> TreeSnapshot<S> {
        // SlabIndex -> CommitId
        let mut rev: HashMap<SlabIndex, S::CommitId> = HashMap::with_capacity(self.index_map.len());
        for (cid, idx) in self.index_map.iter() {
            rev.insert(*idx, *cid);
        }

        let mut nodes_sorted_by_height = BTreeMap::new();
        for (node_index, _) in rev.iter() {
            let node = self.nodes.get(*node_index).unwrap();
            let height = node.get_height();
            let commit_id = node.get_commit_id();
            nodes_sorted_by_height.insert((height, commit_id), node);
        }

        let mut nodes = Vec::with_capacity(self.nodes.len());
        for ((height, commit_id), node) in nodes_sorted_by_height.into_iter() {
            let parent_commit_id = node.get_parent().and_then(|p| rev.get(&p).cloned());

            nodes.push(TreeSnapshotNode {
                node_height: height,
                node_commit_id: commit_id,
                node_parent_commit_id: parent_commit_id,
                modifications: node.get_modifications().clone(),
            });
        }

        TreeSnapshot {
            parent_of_root: self.parent_of_root,
            height_of_root: self.height_of_root,
            nodes,
        }
    }

    pub fn from_snapshot(snapshot: TreeSnapshot<S>) -> PendResult<Self> {
        let TreeSnapshot {
            parent_of_root,
            height_of_root,
            nodes,
        } = snapshot;

        let mut tree = Tree::<S>::new(parent_of_root, height_of_root);
        for node in nodes {
            match node.node_height.cmp(&height_of_root) {
                std::cmp::Ordering::Greater => {
                    if let Some(parent_commit_id) = node.node_parent_commit_id {
                        tree.add_non_root_node(
                            node.node_commit_id,
                            parent_commit_id,
                            node.modifications,
                        )?;
                    } else {
                        return Err(PendingError::InconsistentSnapshotState);
                    }
                }
                std::cmp::Ordering::Equal => {
                    tree.add_root(node.node_commit_id, node.modifications)?
                }
                std::cmp::Ordering::Less => return Err(PendingError::InconsistentSnapshotState),
            }
        }

        Ok(tree)
    }
}
