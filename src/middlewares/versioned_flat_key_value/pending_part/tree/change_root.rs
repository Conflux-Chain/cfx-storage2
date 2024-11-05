use std::collections::{BTreeSet, VecDeque};

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::{
    KeyValueMap, PendingKeyValueSchema, Result as PendResult,
};

use super::{SlabIndex, Tree};

// methods to support VersionedMap::change_root()
impl<S: PendingKeyValueSchema> Tree<S> {
    #[allow(clippy::type_complexity)]
    pub fn change_root(
        &mut self,
        commit_id: S::CommitId,
    ) -> PendResult<(usize, Vec<(S::CommitId, KeyValueMap<S>)>), S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;

        // old_root..=new_root's parent
        let to_commit = self.find_path(slab_index);

        let parent_of_new_root = if let Some(last) = to_commit.last() {
            last.0
        } else {
            // early return if new_root == old_root

            assert_eq!(self.height_of_root, self.nodes[slab_index].get_height());
            return Ok((self.height_of_root, to_commit));
        };

        for idx in self.find_remove_node_index(slab_index) {
            self.detach_node(idx);
        }

        // set new_root as root
        let new_root = self.get_node_mut_by_slab_index(slab_index);
        new_root.set_as_root();
        self.height_of_root = new_root.get_height();
        self.parent_of_root = Some(parent_of_new_root);

        // (height of old_root, old_root..=new_root's parent)
        Ok((self.height_of_root - to_commit.len(), to_commit))
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

    fn find_remove_node_index(&self, retain_subtree_root: usize) -> impl Iterator<Item = usize> {
        // subtree of new_root
        let to_maintain_vec = self.bfs_subtree(retain_subtree_root);
        let to_maintain = BTreeSet::from_iter(to_maintain_vec);

        // remove: tree - subtree of new_root
        let mut to_remove = Vec::new();
        for (idx, _) in self.nodes.iter() {
            if !to_maintain.contains(&idx) {
                to_remove.push(idx);
            }
        }
        to_remove.into_iter()
    }
}
