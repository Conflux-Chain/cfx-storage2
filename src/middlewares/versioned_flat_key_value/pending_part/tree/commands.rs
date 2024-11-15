//! Implementation of [`Tree`] to support [`crate::traits::KeyValueStoreManager`] for [`super::super::super::VersionedStore`]

use crate::{
    middlewares::versioned_flat_key_value::pending_part::pending_schema::{
        ChangeWithRecoverRecord, PendingKeyValueSchema, Result as PendResult,
    },
    traits::{IsCompleted, NeedNext},
    types::ValueEntry,
};

use super::Tree;

impl<S: PendingKeyValueSchema> Tree<S> {
    /// Queries the modification history of a given `Key` in the tree.
    /// Starts from the given `CommitID` and iterates changes upward to the root (including the given `CommitID` and the root).
    ///
    /// # Parameters:
    /// - `accept`: A function that receives a change and returns whether to continue.
    ///
    /// # Returns:
    /// A `Result` with a boolean ([`IsCompleted`]) indicating whether the iteration is completed, or an error if:
    /// - The `commit_id` does not exist in the tree.
    ///
    /// # Algorithm:
    /// 1. Traverse up the tree from the given `commit_id`, searching for the most recent modification of the `key`.
    /// 2. Since each change stored in a node includes where the last modification of this change's key occurred,
    ///    we can directly jump from one modification of the `key` to the previous one,
    ///    continuing until there are no more modifications in the tree.
    ///
    ///    Note: The node that the previous modification occurred may have already been moved from the tree to
    ///    the historical part. When encountering such node, the iteration in the tree is also completed.
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
            // `old_cid` may have been removed from the tree, so this check is necessary
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

    /// Queries the most recent (i.e., further from the root) modification
    /// from the given `commit_id` (included) of the given `key`.
    ///
    /// # Algorithm:
    /// Traverse up the tree from the given `commit_id`, searching for the most recent modification of the `key`.
    ///
    /// Returns the changed value if successful:
    /// - `None`: no change of `key` occurred from `commit_id` (included) to the root (included);
    /// - `Some(ValueEntry::Deleted)`: `key` is deleted;
    /// - `Some(ValueEntry::Value(value))`: `key`'s value is set to `value`.
    ///
    /// Returns an error if:
    /// - `commit_id` does not exist in the tree.
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
