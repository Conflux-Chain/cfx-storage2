//! Methods in `Tree`: used in the pending part to support the logic
//! for implementing the `KeyValueStoreManager` trait for `VersionedStore`.

use crate::{
    middlewares::versioned_flat_key_value::pending_part::pending_schema::{
        ChangeWithRecoverRecord, PendingKeyValueSchema, Result as PendResult,
    },
    traits::{IsCompleted, NeedNext},
    types::ValueEntry,
};

use super::Tree;

impl<S: PendingKeyValueSchema> Tree<S> {
    /// Queries the modification history of a specified `Key` in the tree.
    /// Starts from the given `CommitID` and iterates changes upward to the root (including the given `CommitID` and the root).
    ///
    /// # Parameters:
    /// - `accept`: `impl FnMut(&CommitID, &T::Key, Option<&T::Value>) -> NeedNext`
    ///   Receives a change, including the `CommitID` where the change occurred, the `Key` that was changed, and an `Option<Value>`
    ///   (None means the key was deleted in this change).
    ///   Returns whether to continue iterating.
    /// - `commit_id`: The `CommitID` of the node to start iterating upward.
    /// - `key`: The `Key` to query.
    ///
    /// # Algorithm:
    /// 1. Traverse up the `Tree` from the given `commit_id`, searching for the most recent modification of the `key`.
    /// 2. Since each node stores information required to trace back to the most recent modification of each key in the changes
    ///    that this node made relative to its parent, we can directly jump from one modification of the `key` to the previous one,
    ///    continuing until there are no more modifications in the tree.
    ///
    ///    Note: The node containing the previous modification of the `key` may have already been removed from the tree.
    ///    However, the node where the current modification occurred still stores a `last_commit_id` with a value of `Some`,
    ///    which points to that previous modification. This is done to ensure efficient operation of the `change_root` function
    ///    (i.e., in `change_root`, nodes that remain do not have their `last_commit_id` set to `None` even if the previous node was removed).
    ///
    /// # Returns:
    /// A `Result` containing an `IsCompleted` (i.e., a boolean indicating whether the iteration is completed) if successful,
    /// or an error if the operation fails. Failures include:
    /// - The `commit_id` does not exist in the tree.
    pub fn iter_historical_changes(
        &self,
        mut accept: impl FnMut(&S::CommitId, &S::Key, Option<&S::Value>) -> NeedNext,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<IsCompleted, S> {
        let mut node_option = Some(self.get_node_by_commit_id(*commit_id)?);
        let mut old_commit_id = None;
        while let Some(node) = node_option {
            if let Some(ChangeWithRecoverRecord {
                value,
                last_commit_id,
            }) = node.get_recover_record(key)
            {
                let need_next = accept(&node.get_commit_id(), key, value.as_opt_ref());
                if !need_next {
                    return Ok(false);
                }
                old_commit_id = *last_commit_id;
                break;
            }
            node_option = self.get_parent_node(node);
        }

        while let Some(old_cid) = old_commit_id {
            if !self.contains_commit_id(&old_cid) {
                break;
            }
            let node = self.get_node_by_commit_id(old_cid).unwrap();
            let ChangeWithRecoverRecord {
                value,
                last_commit_id,
            } = node.get_recover_record(key).unwrap();
            let need_next = accept(&node.get_commit_id(), key, value.as_opt_ref());
            if !need_next {
                return Ok(false);
            }
            old_commit_id = *last_commit_id;
        }

        Ok(true)
    }

    /// Queries the most recent modification from the given `commit_id` of the given `key`.
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitID` to query.
    /// - `key`: The `Key` to query.
    ///
    /// # Algorithm:
    /// Traverse up the `Tree` from the given `commit_id`, searching for the most recent modification of the `key`.
    ///
    /// # Returns:
    /// A `Result` containing the changed value if successful, otherwise returns an error if the operation fails.
    /// - The changed value is of type `Option<ValueEntry<Value>>`:
    ///   - None: there is no modification of the `key` in the tree
    ///     (i.e., the value of `key` at `commit_id` is exactly the value of `key` at the parent of the pending root);
    ///   - Some(ValueEntry::Deleted): in the snapshot of `commit_id`, `key` is deleted;
    ///   - Some(ValueEntry::Value(value)): in the snapshot of `commit_id`, `key`'s value is value.
    /// - Failures include:
    ///   - The `commit_id` does not exist in the tree.
    pub fn get_versioned_key(
        &self,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<ValueEntry<S::Value>>, S> {
        let mut node_option = Some(self.get_node_by_commit_id(*commit_id)?);
        while let Some(node) = node_option {
            if let Some(value) = node.get_modified_value(key) {
                return Ok(Some(value));
            }
            node_option = self.get_parent_node(node);
        }
        Ok(None)
    }

    pub fn discard(&mut self, commit_id: S::CommitId) -> PendResult<(), S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;
        if let Some(parent_of_discard) = self.get_node_by_slab_index(slab_index).get_parent() {
            let parent_node = self.get_node_by_slab_index(parent_of_discard);
            let mut to_remove = Vec::new();
            for child in parent_node.get_children() {
                if *child != slab_index {
                    to_remove.append(&mut self.bfs_subtree(*child));
                }
            }
            for idx in to_remove {
                self.detach_node(idx);
            }

            let parent_node = self.get_node_mut_by_slab_index(parent_of_discard);
            parent_node.remove_child_except(&slab_index);
        } // else // root is already the unique child of its parent, so do nothing

        Ok(())
    }
}
