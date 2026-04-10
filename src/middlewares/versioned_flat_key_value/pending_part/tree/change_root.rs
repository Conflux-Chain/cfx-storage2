use std::collections::VecDeque;

use nonempty::NonEmpty;

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::{
    ConfirmedPathInfo, KeyValueMap, PendingKeyValueSchema, Result as PendResult,
};

use super::{SlabIndex, Tree};

// methods to support VersionedMap::change_root()
impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn get_ancestor_commit_at_height(
        &self,
        ancestor_height: u64,
        commit_id: S::CommitId,
    ) -> PendResult<S::CommitId> {
        let node = self.get_node_by_commit_id(commit_id)?;
        let height = node.get_height();
        if (ancestor_height < self.height_of_root) || (ancestor_height > height) {
            return Err(crate::middlewares::PendingError::InvalidAncestorHeight);
        }

        let mut target_node = node;
        while let Some(parent_slab_index) = target_node.get_parent() {
            target_node = self.get_node_by_slab_index(parent_slab_index);
            if target_node.get_height() == ancestor_height {
                return Ok(target_node.get_commit_id());
            }
        }

        Err(crate::middlewares::PendingError::InvalidAncestorHeight)
    }

    /// Changes the root of the pending tree to the given `commit_id`.
    ///
    /// This function communicates its outcome to the caller via its return value,
    /// clearly distinguishing between a meaningful state change, a no-op, and an error:
    ///
    /// - `Ok(Some(path_info))`: The root was successfully changed. This represents a valid
    ///   state transition that satisfies the core invariant: the new root's height is
    ///   strictly greater than the old one's. The caller should use the returned
    ///   `path_info` to persist these changes (e.g., writing to a database).
    ///
    /// - `Ok(None)`: The given `commit_id` was already the current root. This is treated as a
    ///   successful "no-operation" (no-op). Because the height-increase invariant is not met,
    ///   no change is performed. The caller should interpret this as a signal to safely
    ///   skip any follow-up work.
    ///
    /// - `Err(...)`: An error occurred, for instance, if the `commit_id` was not found.
    pub fn change_root(
        &mut self,
        commit_id: S::CommitId,
    ) -> PendResult<Option<ConfirmedPathInfo<S>>> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;

        // old_root..=new_root's parent
        let to_commit = self.find_path(slab_index);

        // IMPORTANT: Do not persist this operation before this return statement.
        // If the path is empty, the new root is the same as the old root.
        // This is a valid no-op scenario. Return `Ok(None)` to explicitly signal to the
        // caller that no state has changed and no further work is necessary.
        if to_commit.is_empty() {
            return Ok(None);
        }

        let old_root_height = self.height_of_root;

        for (ancester, _) in to_commit.iter() {
            self.discard_inner(*ancester)?;
        }
        self.discard_inner(commit_id)?;

        for (ancester, _) in to_commit.iter() {
            self.detach_node(self.get_slab_index_by_commit_id(*ancester).unwrap())
        }

        // set new_root as root
        let new_root = self.get_node_mut_by_slab_index(slab_index);
        new_root.set_as_root();
        self.height_of_root = new_root.get_height();
        let last = to_commit
            .last()
            .expect("Logic error: to_commit should be non-empty here.");
        self.parent_of_root = Some(last.0);

        // Invariant: A real root change must strictly increase the height.
        assert!(old_root_height < self.height_of_root);

        // self.logger
        //     .log_change(&self.parent_of_root, self.height_of_root)?;

        // height of old_root
        let start_height_to_commit = self.height_of_root - to_commit.len() as u64;
        assert_eq!(start_height_to_commit, old_root_height);
        let (to_commit_ids, to_commit_maps) = to_commit.into_iter().unzip();

        Ok(Some(ConfirmedPathInfo {
            start_height: start_height_to_commit,
            commit_ids: NonEmpty::from_vec(to_commit_ids)
                .expect("Logic error: to_commit_ids should be non-empty here."),
            key_value_maps: NonEmpty::from_vec(to_commit_maps)
                .expect("Logic error: to_commit_maps should be non-empty here."),
        }))
    }

    /// This function discards the siblings of the nodes from the root (excluded) to `commit_id` (included).
    /// If there is at least one node discarded, return `Ok(true)`; otherwise, return `Ok(false)`.
    pub fn make_pivot(&mut self, commit_id: S::CommitId) -> PendResult<bool> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;

        // old_root..=new_root's parent
        let to_check_children = self.find_path_ids_only(slab_index);

        let mut has_discarded_nodes = false;

        if let Some(last) = to_check_children.last() {
            for ancester in to_check_children.iter() {
                if self.discard_inner(*ancester)? {
                    has_discarded_nodes = true;
                };
            }
            if self.discard_inner(commit_id)? {
                has_discarded_nodes = true;
            };
        }

        Ok(has_discarded_nodes)
    }

    // excluding target
    fn find_path(&self, target_slab_index: SlabIndex) -> Vec<(S::CommitId, KeyValueMap<S>)> {
        let mut target_node = self.get_node_by_slab_index(target_slab_index);
        let mut path = VecDeque::new();
        while let Some(parent_slab_index) = target_node.get_parent() {
            target_node = self.get_node_by_slab_index(parent_slab_index);
            path.push_front((target_node.get_commit_id(), target_node.get_updates()));
        }
        path.into()
    }

    // excluding target, returns only commit IDs without cloning KeyValueMaps
    fn find_path_ids_only(&self, target_slab_index: SlabIndex) -> Vec<S::CommitId> {
        let mut target_node = self.get_node_by_slab_index(target_slab_index);
        let mut path = VecDeque::new();
        while let Some(parent_slab_index) = target_node.get_parent() {
            target_node = self.get_node_by_slab_index(parent_slab_index);
            path.push_front(target_node.get_commit_id());
        }
        path.into()
    }
}
