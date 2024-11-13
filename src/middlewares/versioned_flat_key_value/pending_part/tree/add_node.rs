use crate::middlewares::{
    versioned_flat_key_value::pending_part::pending_schema::{
        PendingKeyValueSchema, RecoverMap, Result as PendResult,
    },
    PendingError,
};

use super::{node::TreeNode, Tree};

// methods to support VersionedMap::add_node()
impl<S: PendingKeyValueSchema> Tree<S> {
    /// Adds a root to the tree.
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitID` of the node being added.
    /// - `modifications`: A `RecoverMap = BTreeMap<Key, RecoverRecord>` representing the changes from the parent node to the new node.
    ///   Each change is recorded as a (`Key`, `RecoverRecord`) pair.
    ///   `RecoverRecord` includes two fields:
    ///   - `value`: an enum type `ValueEntry` representing this modification:
    ///     - `ValueEntry::Deleted` represents deletion.
    ///     - `ValueEntry::Value(value)` represents a specific value.
    ///   - `last_commit_id`: an `Option<CommitId>` recording where the last modification before this modification occurred:
    ///     - `None`: no modification before this modification in the tree.
    ///     - `Some(last_cid)`: the last modification occurred at `last_cid`.
    ///     For the `add_root` function, each `last_commit_id` should be set to `None`.
    ///
    /// # Notes:
    /// When creating a new `TreeNode`, the height of this node in the underlying database should be provided.
    /// The root's height is already stored in the tree when it is initialized, so it can be retrieved directly.
    ///
    /// # Returns:
    /// A `Result` that is empty if successful, or returns an error if the operation fails.
    /// Failure can occur due to:
    /// - the tree already has a root.
    pub fn add_root(
        &mut self,
        commit_id: S::CommitId,
        modifications: RecoverMap<S>,
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

    pub fn add_non_root_node(
        &mut self,
        commit_id: S::CommitId,
        parent_commit_id: S::CommitId,
        modifications: RecoverMap<S>,
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
