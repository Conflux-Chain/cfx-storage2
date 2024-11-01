mod add_node_commands;
mod change_root_commands;
mod checkout_commands;
mod key_value_store_manager_commands;
mod node;

pub type SlabIndex = usize;

use std::collections::HashMap;

use slab::Slab;

use self::node::TreeNode;

use super::pending_schema::{PendingKeyValueSchema, Result as PendResult};
use super::PendingError;

pub struct Tree<S: PendingKeyValueSchema> {
    parent_of_root: Option<S::CommitId>,
    height_of_root: usize,
    nodes: Slab<TreeNode<S>>,
    index_map: HashMap<S::CommitId, SlabIndex>,
}

// basic methods
impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn new(parent_of_root: Option<S::CommitId>, height_of_root: usize) -> Self {
        Tree {
            parent_of_root,
            height_of_root,
            nodes: Slab::new(),
            index_map: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn check_consistency(&self, height_of_root: usize) -> bool {
        if self.height_of_root != height_of_root {
            return false;
        };

        if self.nodes.len() != self.index_map.len() {
            return false;
        };

        for (commit_id, slab_index) in self.index_map.iter() {
            let node = self.get_node_by_slab_index(*slab_index);

            if node.get_commit_id() != *commit_id {
                return false;
            };

            if node.get_height() == height_of_root {
                if node.get_parent().is_some() {
                    return false;
                };
            } else if let Some(parent) = self.get_parent_node(node) {
                if node.get_height() != parent.get_height() + 1 {
                    return false;
                }
                if !parent.get_children().contains(slab_index) {
                    return false;
                }
            } else {
                return false;
            }

            for child in node.get_children() {
                let child_node = self.get_node_by_slab_index(*child);
                if child_node.get_parent() != Some(*slab_index) {
                    return false;
                };
            }
        }

        // todo: modifications

        true
    }

    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.parent_of_root
    }

    pub(super) fn contains_commit_id(&self, commit_id: &S::CommitId) -> bool {
        self.index_map.contains_key(commit_id)
    }

    fn get_slab_index_by_commit_id(&self, commit_id: S::CommitId) -> PendResult<SlabIndex, S> {
        let slab_index = *self
            .index_map
            .get(&commit_id)
            .ok_or(PendingError::CommitIDNotFound(commit_id))?;
        Ok(slab_index)
    }

    fn get_node_by_slab_index(&self, slab_index: SlabIndex) -> &TreeNode<S> {
        &self.nodes[slab_index]
    }

    fn get_mut_node_by_slab_index(&mut self, slab_index: SlabIndex) -> &mut TreeNode<S> {
        &mut self.nodes[slab_index]
    }

    fn get_node_by_commit_id(&self, commit_id: S::CommitId) -> PendResult<&TreeNode<S>, S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;
        Ok(self.get_node_by_slab_index(slab_index))
    }

    fn has_root(&self) -> bool {
        !self.index_map.is_empty()
    }

    fn get_parent_node(&self, node: &TreeNode<S>) -> Option<&TreeNode<S>> {
        node.get_parent()
            .map(|p_slab_index| self.get_node_by_slab_index(p_slab_index))
    }

    fn get_by_commit_id(
        &self,
        commit_id: S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<Option<S::Value>>, S> {
        let node = self.get_node_by_commit_id(commit_id)?;
        Ok(node.get_modified_value(key))
    }

    // including subroot
    fn bfs_subtree(&self, subroot_slab_index: SlabIndex) -> Vec<SlabIndex> {
        let mut slab_indices = vec![subroot_slab_index];
        let mut head = 0;
        while head < slab_indices.len() {
            let node = self.get_node_by_slab_index(slab_indices[head]);

            for &child_index in node.get_children() {
                slab_indices.push(child_index);
            }

            head += 1;
        }

        slab_indices
    }

    fn detach_node(&mut self, idx: SlabIndex) {
        self.index_map
            .remove(&self.nodes.remove(idx).get_commit_id());
    }
}
