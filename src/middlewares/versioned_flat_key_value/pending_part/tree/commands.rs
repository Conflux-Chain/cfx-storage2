use crate::{
    middlewares::versioned_flat_key_value::pending_part::pending_schema::{
        PendingKeyValueSchema, RecoverRecord, Result as PendResult,
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
        let mut old_commit_id = None;
        while let Some(node) = node_option {
            if let Some(RecoverRecord {
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
            let RecoverRecord {
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
