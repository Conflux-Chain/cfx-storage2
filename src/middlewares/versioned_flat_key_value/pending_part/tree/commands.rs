use crate::{
    middlewares::versioned_flat_key_value::pending_part::pending_schema::{
        PendingKeyValueSchema, Result as PendResult,
    },
    traits::{IsCompleted, NeedNext},
    types::ValueEntry,
};

use super::Tree;

// Internal Tree methods
// supporting helper methods in VersionedMap for
// implementing `KeyValueStoreManager` for `VersionedStore`.
impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn iter_historical_changes(
        &self,
        mut accept: impl FnMut(&S::CommitId, &S::Key, Option<&S::Value>) -> NeedNext,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<IsCompleted, S> {
        let mut node_option = Some(self.get_node_by_commit_id(*commit_id)?);
        while let Some(node) = node_option {
            // TODO: faster historical changes iteration
            if let Some(value) = node.get_modified_value(key) {
                let need_next = accept(&node.get_commit_id(), key, value.as_opt_ref());
                if !need_next {
                    return Ok(false);
                }
            }
            node_option = self.get_parent_node(node);
        }
        Ok(true)
    }

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
