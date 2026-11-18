use crate::{
    middlewares::versioned_flat_key_value::pending_part::pending_schema::{
        KeyValueMap, PendingKeyValueSchema, RecoverRecord, Result as PendResult,
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
    ) -> PendResult<IsCompleted> {
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

    /// The value as of `commit_id` of every key accepted by `filter` that is modified
    /// on the pending path from the root to `commit_id` (the closest modification
    /// wins). Keys not modified on that path are absent.
    ///
    /// Visits each node of the path once and does not touch the checked-out
    /// current state.
    pub fn get_path_changes(
        &self,
        commit_id: &S::CommitId,
        filter: impl Fn(&S::Key) -> bool,
    ) -> PendResult<KeyValueMap<S>> {
        let mut changes = KeyValueMap::<S>::new();
        let mut node_option = Some(self.get_node_by_commit_id(*commit_id)?);
        while let Some(node) = node_option {
            for (key, RecoverRecord { value, .. }) in node.get_modifications() {
                if filter(key) && !changes.contains_key(key) {
                    changes.insert(key.clone(), value.clone());
                }
            }
            node_option = self.get_parent_node(node);
        }
        Ok(changes)
    }

    /// The value of `key` as of `commit_id` together with the commit that wrote it,
    /// i.e. the closest ancestor-or-self of `commit_id` that modified `key`. `None`
    /// means `key` is not modified anywhere on the pending path from the root to
    /// `commit_id`.
    ///
    /// Visits each node of that path once.
    pub fn get_versioned_key_with_commit(
        &self,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<(S::CommitId, ValueEntry<S::Value>)>> {
        let mut node_option = Some(self.get_node_by_commit_id(*commit_id)?);
        while let Some(node) = node_option {
            if let Some(value) = node.get_modified_value(key) {
                return Ok(Some((node.get_commit_id(), value)));
            }
            node_option = self.get_parent_node(node);
        }
        Ok(None)
    }

    /// Like [`Self::get_versioned_key_with_commit`], but uses a snapshot of the
    /// checked-out state at another commit `current`: `current_record` is its entry
    /// for `key`, i.e. the closest ancestor-or-self of `current` that modified `key`
    /// and the value written there.
    ///
    /// Let L be the lowest common ancestor of `commit_id` and `current`. A
    /// modification of `key` between L (exclusive) and `commit_id` is the answer.
    /// Otherwise the answer is the value as of L, which lies on the path of
    /// `current`: it is `current_record` or an earlier modification on that path,
    /// reached through the `last_commit_id` links of `key`'s records.
    ///
    /// So the cost is the distance between the two commits plus the number of
    /// modifications of `key` between L and `current`, instead of the distance from
    /// `commit_id` to the root. When the search for L has taken as many steps as
    /// that plain walk would, it falls back to the plain walk, so it never takes
    /// more than about twice as long.
    pub fn get_versioned_key_with_current(
        &self,
        commit_id: &S::CommitId,
        key: &S::Key,
        current: S::CommitId,
        current_record: Option<&(S::CommitId, ValueEntry<S::Value>)>,
    ) -> PendResult<Option<(S::CommitId, ValueEntry<S::Value>)>> {
        let mut node = self.get_node_by_commit_id(*commit_id)?;
        let Ok(mut current_node) = self.get_node_by_commit_id(current) else {
            return self.get_versioned_key_with_commit(commit_id, key);
        };
        let mut budget = node.get_height() - self.height_of_root;

        // Bring `current_node` down to the height of `node`.
        while current_node.get_height() > node.get_height() {
            if budget == 0 {
                return self.get_versioned_key_with_commit(commit_id, key);
            }
            budget -= 1;
            current_node = self.get_parent_node(current_node).unwrap();
        }
        // Walk up from `node` until it meets the path of `current`.
        while node.get_commit_id() != current_node.get_commit_id() {
            if let Some(value) = node.get_modified_value(key) {
                return Ok(Some((node.get_commit_id(), value)));
            }
            if budget == 0 {
                return self.get_versioned_key_with_commit(commit_id, key);
            }
            budget -= 1;
            // Above `current_node` means it has a parent, as the tree has one root.
            let at_same_height = node.get_height() == current_node.get_height();
            node = self.get_parent_node(node).unwrap();
            if at_same_height {
                current_node = self.get_parent_node(current_node).unwrap();
            }
        }

        // `node` is L. Find the last modification of `key` at or above it on the
        // path of `current`.
        let lca_height = node.get_height();
        let Some((mut modified_at, value)) = current_record.cloned() else {
            // Not modified anywhere on the path of `current`.
            return Ok(None);
        };
        let mut value = Some(value);
        loop {
            // A commit missing from the tree has been moved to the historical part,
            // and so have all earlier ones.
            let Ok(modifier) = self.get_node_by_commit_id(modified_at) else {
                return Ok(None);
            };
            if modifier.get_height() <= lca_height {
                let value = value.unwrap_or_else(|| {
                    modifier
                        .get_modified_value(key)
                        .expect("a commit linked as modifying a key modifies it")
                });
                return Ok(Some((modified_at, value)));
            }
            let record = modifier
                .get_recover_record(key)
                .expect("a commit linked as modifying a key modifies it");
            match record.last_commit_id {
                Some(previous) => {
                    modified_at = previous;
                    value = None;
                }
                None => return Ok(None),
            }
        }
    }

    /// The number of keys modified by the commit `commit_id`, for benches.
    #[cfg(test)]
    pub fn modification_count(&self, commit_id: S::CommitId) -> PendResult<usize> {
        Ok(self.get_node_by_commit_id(commit_id)?.get_modifications().len())
    }

    /// This function discards the siblings of the node at `commit_id`.
    /// If there is at least one node discarded, return `Ok(true)`; otherwise, return `Ok(false)`.
    pub(super) fn discard_inner(&mut self, commit_id: S::CommitId) -> PendResult<bool> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;
        if let Some(parent_of_discard) = self.get_node_by_slab_index(slab_index).get_parent() {
            let parent_node = self.get_node_by_slab_index(parent_of_discard);
            let mut to_remove = Vec::new();
            for child in parent_node.get_children() {
                if *child != slab_index {
                    to_remove.append(&mut self.bfs_subtree(*child));
                }
            }

            if to_remove.is_empty() {
                return Ok(false);
            }

            for idx in to_remove {
                self.detach_node(idx);
            }
            let parent_node = self.get_node_mut_by_slab_index(parent_of_discard);
            parent_node.remove_child_except(&slab_index);

            Ok(true)
        } else {
            // root is already the unique child of its parent, so do nothing
            Ok(false)
        }
    }

    pub fn discard(&mut self, commit_id: S::CommitId) -> PendResult<bool> {
        self.discard_inner(commit_id)
    }
}
