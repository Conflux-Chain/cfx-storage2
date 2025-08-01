use std::collections::VecDeque;

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
    ) -> PendResult<S::CommitId, S> {
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

    pub fn change_root(&mut self, commit_id: S::CommitId) -> PendResult<ConfirmedPathInfo<S>, S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;

        // old_root..=new_root's parent
        let to_commit = self.find_path(slab_index);

        if let Some(last) = to_commit.last() {
            for (ancester, _) in to_commit.iter() {
                self.discard(*ancester)?;
            }
            self.discard(commit_id)?;

            for (ancester, _) in to_commit.iter() {
                self.detach_node(self.get_slab_index_by_commit_id(*ancester).unwrap())
            }

            // set new_root as root
            let new_root = self.get_node_mut_by_slab_index(slab_index);
            new_root.set_as_root();
            self.height_of_root = new_root.get_height();
            self.parent_of_root = Some(last.0);
        }

        // height of old_root
        let start_height_to_commit = self.height_of_root - to_commit.len() as u64;
        let (to_commit_ids, to_commit_maps) = to_commit.into_iter().unzip();

        Ok(ConfirmedPathInfo {
            start_height: start_height_to_commit,
            commit_ids: to_commit_ids,
            key_value_maps: to_commit_maps,
        })
    }

    /// This function discards the siblings of the nodes from the root (excluded) to `commit_id` (included).
    /// If there is at least one node discarded, return `Ok(true)`; otherwise, return `Ok(false)`.
    pub fn make_pivot(&mut self, commit_id: S::CommitId) -> PendResult<bool, S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;

        // old_root..=new_root's parent
        let to_check_children = self.find_path(slab_index);

        let mut has_discarded_nodes = false;

        if let Some(last) = to_check_children.last() {
            for (ancester, _) in to_check_children.iter() {
                if self.discard(*ancester)? {
                    has_discarded_nodes = true;
                };
            }
            if self.discard(commit_id)? {
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
}
