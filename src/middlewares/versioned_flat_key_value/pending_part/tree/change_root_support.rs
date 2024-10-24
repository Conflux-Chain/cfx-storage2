use std::collections::{BTreeSet, VecDeque};

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::{
    KeyValueMap, PendingKeyValueSchema, Result as PendResult,
};

use super::{SlabIndex, Tree};

// methods to support VersionedMap::change_root()
impl<S: PendingKeyValueSchema> Tree<S> {
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

    // todo: test
    #[allow(clippy::type_complexity)]
    pub fn change_root(
        &mut self,
        commit_id: S::CommitId,
    ) -> PendResult<(usize, Vec<(S::CommitId, KeyValueMap<S>)>), S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;

        // old_root..=new_root's parent
        let to_commit = self.find_path(slab_index);

        // early return if new_root == old_root
        if to_commit.is_empty() {
            assert_eq!(self.height_of_root, self.nodes[slab_index].get_height());
            return Ok((self.height_of_root, to_commit));
        }

        // subtree of new_root
        let to_maintain_vec = self.bfs_subtree(slab_index);
        let to_maintain = BTreeSet::from_iter(to_maintain_vec);

        // remove: tree - subtree of new_root
        let mut to_remove = Vec::new();
        for (idx, _) in self.nodes.iter() {
            if !to_maintain.contains(&idx) {
                to_remove.push(idx);
            }
        }
        for idx in to_remove {
            self.index_map
                .remove(&self.nodes.remove(idx).get_commit_id());
        }

        // set new_root as root
        let new_root = self.get_mut_node_by_slab_index(slab_index);
        new_root.set_as_root();
        self.height_of_root = new_root.get_height();

        // (height of old_root, old_root..=new_root's parent)
        Ok((self.height_of_root - to_commit.len(), to_commit))
    }
}
