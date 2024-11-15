//! Implementation of [`Tree`] to support the `add_node` function in [`super::super::super::VersionedMap`]

use crate::middlewares::{
    versioned_flat_key_value::pending_part::pending_schema::{
        ChangeWithRecoverMap, PendingKeyValueSchema, Result as PendResult,
    },
    PendingError,
};

use super::{node::TreeNode, Tree};

impl<S: PendingKeyValueSchema> Tree<S> {
    /// Adds the root to the tree.
    ///
    /// # Parameters:
    /// - `modifications`: The changes from the parent node to the new node.
    ///   Each change is recorded as a (`Key`, [`super::super::pending_schema::ChangeWithRecoverRecord`]) pair.
    ///
    /// # Notes:
    /// When creating a new [`TreeNode`], the height of this node in the underlying database should be provided.
    /// The root's height is already stored in the tree when the tree is initialized, so it can be retrieved directly.
    pub fn add_root(
        &mut self,
        commit_id: S::CommitId,
        modifications: ChangeWithRecoverMap<S>,
    ) -> PendResult<(), S> {
        // return error if there is root
        if self.has_root() {
            return Err(PendingError::MultipleRootsNotAllowed);
        }
        // PendingError::CommitIdAlreadyExists(_) cannot happend because no root <=> no node

        // new root
        let root = TreeNode::new_root(commit_id, modifications, self.height_of_root);

        // add root to tree
        let slab_index = self.nodes.insert(root);
        self.index_map.insert(commit_id, slab_index);

        Ok(())
    }

    /// Adds a non-root node to the tree.
    ///
    /// # Parameters:
    /// - `modifications`: The changes from the parent node to the new node.
    ///   Each change is recorded as a (`Key`, [`super::super::pending_schema::ChangeWithRecoverRecord`]) pair.
    ///
    /// # Notes:
    /// When creating a new [`TreeNode`], the height of this node in the underlying database should be provided.
    /// The new node's height is calculated as its parent node's height plus 1.
    pub fn add_non_root_node(
        &mut self,
        commit_id: S::CommitId,
        parent_commit_id: S::CommitId,
        modifications: ChangeWithRecoverMap<S>,
    ) -> PendResult<(), S> {
        // return error if parent_commit_id does not exist
        let parent_slab_index = self.get_slab_index_by_commit_id(parent_commit_id)?;

        // return error if commit_id exists
        if self.contains_commit_id(&commit_id) {
            return Err(PendingError::CommitIdAlreadyExists(commit_id));
        }

        // new node
        let parent_height = self.get_node_by_slab_index(parent_slab_index).get_height();
        let node = TreeNode::new_non_root_node(
            commit_id,
            parent_slab_index,
            parent_height + 1,
            modifications,
        );

        // add node to tree
        let slab_index = self.nodes.insert(node);
        self.index_map.insert(commit_id, slab_index);
        self.nodes[parent_slab_index].insert_child(slab_index);

        Ok(())
    }
}
