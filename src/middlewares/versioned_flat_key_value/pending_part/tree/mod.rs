//! Implementation of basic methods for [`Tree`]

mod add_node;
mod change_root;
mod checkout;
mod commands;
mod node;

pub type SlabIndex = usize;

use std::collections::HashMap;

use slab::Slab;

use self::node::TreeNode;

use super::pending_schema::{PendingKeyValueSchema, Result as PendResult};
use super::PendingError;
use crate::types::ValueEntry;

/// Represents the tree in the pending part (see [`super::VersionedMap`])
/// of the underlying database (see [`super::super::VersionedStore`]).
///
/// # Fields:
/// - Information used to connect with the historical part:
///   - `parent_of_root`: The latest node in the historical part (i.e., the parent of the pending root).
///     `None` means that the historical part is empty.
///   - `height_of_root`: The height of the root (referring to the height of the pending root in the entire underlying database).
/// - `nodes`: A collection (using a [`Slab`]) of all nodes ([`TreeNode`]) stored in this tree.
///   - Each node represents a commit and contains information about its changes relative to its parent.
///     Each change also includes where the last modification of this change's key occurred.
/// - `index_map`: A mapping from each node's `CommitId` to its [`SlabIndex`] in the collection of nodes.
///   Every `CommitId` in the `index_map` corresponds exactly to an existing node in the tree.
///
/// # Usage:
/// [`Tree`] contains all the information about the pending part of the database. It tracks all nodes (commits)
/// and their relationships, and can be used to implement all necessary operations on the pending part.
/// However, while the [`Tree`] provides full access to the pending part, certain operations like switching
/// between commits or querying specific snapshots may not be efficient when using only the [`Tree`].
pub(super) struct Tree<S: PendingKeyValueSchema> {
    parent_of_root: Option<S::CommitId>,
    height_of_root: usize,

    nodes: Slab<TreeNode<S>>,

    index_map: HashMap<S::CommitId, SlabIndex>,
}

impl<S: PendingKeyValueSchema> Tree<S> {
    /// Creates an empty tree for the pending part of the database.
    /// The parameters `parent_of_root` and `height_of_root` are determined by the underlying database.
    /// For their meanings, see the documentation on [`Tree`].
    pub fn new(parent_of_root: Option<S::CommitId>, height_of_root: usize) -> Self {
        Tree {
            parent_of_root,
            height_of_root,
            nodes: Slab::new(),
            index_map: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn check_consistency(&self, height_of_root: usize) -> bool {
        if self.height_of_root != height_of_root {
            return false;
        };

        if self.nodes.len() != self.index_map.len() {
            return false;
        };

        for (commit_id, slab_index) in self.index_map.iter() {
            let node = self.get_node_by_slab_index(*slab_index);

            if node.get_commit_id() != *commit_id {
                return false;
            };

            if node.get_height() == height_of_root {
                if node.get_parent().is_some() {
                    return false;
                };
            } else if let Some(parent) = self.get_parent_node(node) {
                if node.get_height() != parent.get_height() + 1 {
                    return false;
                }
                if !parent.get_children().contains(slab_index) {
                    return false;
                }
            } else {
                return false;
            }

            for child in node.get_children() {
                let child_node = self.get_node_by_slab_index(*child);
                if child_node.get_parent() != Some(*slab_index) {
                    return false;
                };
            }
        }

        // todo: modifications

        true
    }

    /// Returns the latest `CommitId` in the historical part. If `None`, the historical part is empty.
    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.parent_of_root
    }

    /// Checks if the tree contains a node with the given `commit_id`.
    pub(super) fn contains_commit_id(&self, commit_id: &S::CommitId) -> bool {
        self.index_map.contains_key(commit_id)
    }

    /// Retrieves the `SlabIndex` corresponding to the given `commit_id`.
    ///
    /// Returns an error if the `commit_id` is not in the tree.
    fn get_slab_index_by_commit_id(&self, commit_id: S::CommitId) -> PendResult<SlabIndex, S> {
        let slab_index = *self
            .index_map
            .get(&commit_id)
            .ok_or(PendingError::CommitIDNotFound(commit_id))?;
        Ok(slab_index)
    }

    /// Retrieves a reference to a node in the tree by its `SlabIndex`.
    fn get_node_by_slab_index(&self, slab_index: SlabIndex) -> &TreeNode<S> {
        &self.nodes[slab_index]
    }

    /// Retrieves a mutable reference to a node in the tree by its `SlabIndex`.
    fn get_node_mut_by_slab_index(&mut self, slab_index: SlabIndex) -> &mut TreeNode<S> {
        &mut self.nodes[slab_index]
    }

    /// Retrieves a reference to a node in the tree by its corresponding `commit_id`.
    ///
    /// Returns an error if the `commit_id` is not in the tree.
    fn get_node_by_commit_id(&self, commit_id: S::CommitId) -> PendResult<&TreeNode<S>, S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;
        Ok(self.get_node_by_slab_index(slab_index))
    }

    /// Checks whether the tree has a root node.
    ///
    /// Note that if the tree has a node, then the tree has a root.
    fn has_root(&self) -> bool {
        !self.index_map.is_empty()
    }

    /// Retrieves a reference to the parent node of a given node.
    ///
    /// Returns `None` if `node` is the root.
    fn get_parent_node(&self, node: &TreeNode<S>) -> Option<&TreeNode<S>> {
        node.get_parent()
            .map(|p_slab_index| self.get_node_by_slab_index(p_slab_index))
    }

    /// Retrieves a modification for a given `key` at a given `commit_id`, specifically the change made in this node
    /// relative to its parent, not any prior modifications.
    ///
    /// # Returns
    /// - `Ok(Some)` if there is a modification.
    /// - `Ok(None)` if no modification exists for the `key`.
    /// - an error if the `commit_id` is not in the tree.
    fn get_modification_by_commit_id(
        &self,
        commit_id: S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<ValueEntry<S::Value>>, S> {
        let node = self.get_node_by_commit_id(commit_id)?;
        Ok(node.get_modified_value(key))
    }

    /// Collects all nodes within a subtree starting from `subroot_slab_index` (included).
    fn bfs_subtree(&self, subroot_slab_index: SlabIndex) -> Vec<SlabIndex> {
        let mut slab_indices = vec![subroot_slab_index];
        let mut head = 0;
        while head < slab_indices.len() {
            let node = self.get_node_by_slab_index(slab_indices[head]);

            for &child_index in node.get_children() {
                slab_indices.push(child_index);
            }

            head += 1;
        }

        slab_indices
    }

    /// Detaches a node from the tree by its `SlabIndex`.
    ///
    /// # Notes:
    /// This function does not modify the relationships between this node and its parent or children.
    /// Specifically:
    /// - The parent node's list of children is not updated.
    /// - The child nodes' references to their parent are not changed.
    /// These relationship updates are expected to be handled elsewhere in the code.
    fn detach_node(&mut self, idx: SlabIndex) {
        self.index_map
            .remove(&self.nodes.remove(idx).get_commit_id());
    }
}
