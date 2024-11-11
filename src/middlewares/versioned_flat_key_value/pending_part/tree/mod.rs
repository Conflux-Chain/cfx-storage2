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

/// The `Tree` structure represents the tree of nodes in the pending part of the underlying database (see `VersionedStore`).
///
/// # Overview:
/// The pending part is in-memory and allows forking, where each node represents a snapshot of a key-value store,
/// and each edge represents the changes between parent and child nodes. The root of this tree is unique
/// and its parent is always the latest node in the historical part. If the historical part is empty,
/// the parent of the pending root is `None`.
///
/// The `Tree` maintains:
/// - Information used to connect with the historical part:
///   - The parent of the pending root.
///   - The height of the root (referring to the height of the pending root in the entire underlying database,
///     in order to obtain a continuously increasing `HistoryNumber` when moving it into the historical part in the future).
/// - A collection (using a `Slab`) of all nodes (`TreeNode<S>`) stored in this tree.
///   - The `CommitId` of each node.
///   - The `SlabIndex` of each node's parent and children nodes.
///   - The changes made by each node relative to its parent.
///   - Information required to trace back to the most recent modification in the pending part of each key in these changes.
/// - Efficient lookup of nodes by their `CommitId`.
///
/// # Usage:
/// The `Tree` structure contains all the information about the pending part of the database. It tracks all nodes (commits)
/// and their relationships, and can be used to implement all necessary operations on the pending part. However, while the `Tree`
/// provides full access to the pending part, certain operations like switching between commits or querying specific snapshots
/// may not be efficient when using only the `Tree`.
///
/// The `VersionedMap` structure is built on top of the `Tree` and maintains a `CurrentMap`, which provides an optimized way
/// to quickly switch between different commits and perform efficient queries. By maintaining a separate `CurrentMap`,
/// `VersionedMap` can accelerate operations that would otherwise require traversing or recalculating parts of the tree.
pub struct Tree<S: PendingKeyValueSchema> {
    /// The `CommitId` of the parent node for the root of this tree.
    /// This refers to the latest node in the historical part or is set to `None` if there is no historical data.
    parent_of_root: Option<S::CommitId>,

    /// The height of the root node in relation to the entire underlying database.
    /// This height is used when moving nodes from pending to historical parts, ensuring continuous history numbering.
    height_of_root: usize,

    /// A collection (using a `Slab`) of all nodes (`TreeNode<S>`) stored in this tree.
    /// Each node represents a commit and contains information about its changes relative to its parent.
    nodes: Slab<TreeNode<S>>,

    /// A mapping from each node's `CommitId` to its index in the slab (`nodes`).
    /// This allows for efficient lookup of nodes by their unique commit identifiers.
    index_map: HashMap<S::CommitId, SlabIndex>,
}

// basic methods
impl<S: PendingKeyValueSchema> Tree<S> {
    /// Creates a new `Tree` instance, initializing the tree in the pending part of the database.
    ///
    /// # Parameters:
    /// - `parent_of_root`: The `CommitId` of the parent of the pending root (i.e., the latest node in the historical part).
    ///   If the historical part is empty, this should be `None`.
    /// - `height_of_root`: The height of the pending root in the entire underlying database. This is used to ensure that when
    ///   moving the pending root into the historical part, it will receive a continuously increasing `HistoryNumber`.
    ///
    /// # Returns:
    /// A new `Tree` instance with an empty tree.
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

    /// Returns the `CommitId` of the parent of the pending root.
    ///
    /// # Returns:
    /// An `Option<CommitId>` representing the parent of the pending root.
    /// If the historical part is empty, this will return `None`.
    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.parent_of_root
    }

    /// Checks if the tree contains a node with the given `commit_id`.
    ///
    /// This function looks up the `commit_id` in the tree's `index_map` to determine if a node
    /// corresponding to the given `commit_id` exists in the tree.
    ///
    /// # Parameters:
    /// - `commit_id`: A reference to the `CommitId` to check for in the tree.
    ///
    /// # Returns:
    /// - `true` if a node with the given `commit_id` exists in the tree.
    /// - `false` otherwise.
    pub(super) fn contains_commit_id(&self, commit_id: &S::CommitId) -> bool {
        self.index_map.contains_key(commit_id)
    }

    /// Retrieves the `SlabIndex` corresponding to the given `commit_id`.
    ///
    /// This function looks up the `commit_id` in the tree's `index_map` and returns the associated
    /// `SlabIndex`. If the `commit_id` is not found, it returns a `PendingError::CommitIDNotFound`.
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitId` to look up in the tree.
    ///
    /// # Returns:
    /// - A `PendResult` containing the `SlabIndex` if found, or an error if not found.
    fn get_slab_index_by_commit_id(&self, commit_id: S::CommitId) -> PendResult<SlabIndex, S> {
        let slab_index = *self
            .index_map
            .get(&commit_id)
            .ok_or(PendingError::CommitIDNotFound(commit_id))?;
        Ok(slab_index)
    }

    /// Retrieves a reference to a node in the tree by its `SlabIndex`.
    ///
    /// # Parameters:
    /// - `slab_index`: The index of the node in the tree's slab.
    ///
    /// # Returns:
    /// - A reference to the node (`TreeNode<S>`) at the given index.
    fn get_node_by_slab_index(&self, slab_index: SlabIndex) -> &TreeNode<S> {
        &self.nodes[slab_index]
    }

    /// Retrieves a mutable reference to a node in the tree by its `SlabIndex`.
    ///
    /// # Parameters:
    /// - `slab_index`: The index of the node in the tree's slab.
    ///
    /// # Returns:
    /// - A mutable reference to the node (`TreeNode<S>`) at the given index.
    fn get_node_mut_by_slab_index(&mut self, slab_index: SlabIndex) -> &mut TreeNode<S> {
        &mut self.nodes[slab_index]
    }

    /// Retrieves a reference to a node in the tree by its corresponding `commit_id`.
    ///
    /// This function first looks up the `SlabIndex` for the given `commit_id`, then retrieves
    /// and returns a reference to the corresponding node (`TreeNode<S>`).
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitId` of the node to retrieve.
    ///
    /// # Returns:
    /// - A `PendResult` containing a reference to the node if found, or an error if not found.
    fn get_node_by_commit_id(&self, commit_id: S::CommitId) -> PendResult<&TreeNode<S>, S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;
        Ok(self.get_node_by_slab_index(slab_index))
    }

    /// Checks whether the tree has a root node.
    ///
    /// This function checks if there is any root node in the tree by determining if the `index_map` is empty or not.
    /// If the `index_map` is not empty, it means there is at least one node, which implies that there is a root node.
    /// If the `index_map` is empty, then there is no root node.
    ///
    /// # Returns:
    /// - `true` if there is a root node.
    /// - `false` otherwise.
    fn has_root(&self) -> bool {
        !self.index_map.is_empty()
    }

    /// Retrieves a reference to the parent node of a given node.
    ///
    /// # Parameters:
    /// - `node`: A reference to a node (`TreeNode<S>`) whose parent is being queried.
    ///
    /// # Returns:
    /// - An optional reference to the parent node.
    ///   - If `node` is the root, i.e., no parent exists, returns `None`.
    ///   - If `node` is not the root, i.e., its parent exists, returns `Some(parent_node)`.
    fn get_parent_node(&self, node: &TreeNode<S>) -> Option<&TreeNode<S>> {
        node.get_parent()
            .map(|p_slab_index| self.get_node_by_slab_index(p_slab_index))
    }

    /// Retrieves a modification for a specific key at a given commit.
    ///
    /// This function looks up a modification (if any) for a specific key in
    /// a particular commit. It first retrieves the node corresponding to
    /// the provided commit ID and then checks for any modifications related
    /// to that key within that commit.
    ///
    /// # Parameters:
    /// - `commit_id`: The ID of the commit where modifications are being queried.
    /// - `key`: The key whose modification is being queried.
    ///
    /// # Returns:
    /// - A result containing an option with either some modification (`ValueEntry`)
    ///   or none if no modification exists for that key. If no such commit exists,
    ///   an error is returned.
    fn get_modification_by_commit_id(
        &self,
        commit_id: S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<ValueEntry<S::Value>>, S> {
        let node = self.get_node_by_commit_id(commit_id)?;
        Ok(node.get_modified_value(key))
    }

    /// Performs breadth-first search (BFS) on all nodes within a subtree starting from subroot (including subroot).
    ///
    /// Parameters:
    /// - `subroot_slab_index`: The `SlabIndex` of subtree root from which traversal begins.
    ///
    /// Returns:
    /// - A vector containing `SlabIndex` of all nodes visited during BFS traversal.
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

    /// Detaches (removes) a node from the tree by its `SlabIndex`.
    ///
    /// # Parameters:
    /// - `idx`: The `SlabIndex` of the node to remove.
    ///
    /// # Notes:
    /// This function removes a node from the tree's internal `nodes` collection and
    /// deletes its corresponding entry in the `index_map`.
    /// However, it does not modify the relationships between this node and its parent or children.
    /// Specifically:
    /// - The parent node's list of children is not updated.
    /// - The child nodes' references to their parent are not changed.
    /// These relationship updates are expected to be handled elsewhere in the code.
    fn detach_node(&mut self, idx: SlabIndex) {
        self.index_map
            .remove(&self.nodes.remove(idx).get_commit_id());
    }
}
