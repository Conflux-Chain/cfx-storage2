use std::collections::{BTreeSet, HashMap, HashSet};

use slab::Slab;

use super::pending_schema::{Commits, Modifications, PendingKeyValueSchema, RollComm};
use super::PendingError;

type SlabIndex = usize;

pub struct TreeNode<S: PendingKeyValueSchema> {
    parent: Option<SlabIndex>,
    children: BTreeSet<SlabIndex>,

    // todo: test lazy height
    // height will not be changed even when root is changed
    // height is only used for lca
    height: usize,

    commit_id: S::CommitId,
    // before current node, the old value of this key is modified by which commit_id,
    // if none, this key is absent before current node
    // here must use CommitID instead of SlabIndex (which may be reused, see slab doc)
    modifications: Modifications<S>,
}

pub struct Tree<S: PendingKeyValueSchema> {
    nodes: Slab<TreeNode<S>>,
    index_map: HashMap<S::CommitId, SlabIndex>,
}

impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn new() -> Self {
        Tree {
            nodes: Slab::new(),
            index_map: HashMap::new(),
        }
    }

    fn contains_commit_id(&self, commit_id: &S::CommitId) -> bool {
        self.index_map.contains_key(commit_id)
    }

    fn get_slab_index_by_commit_id(
        &self,
        commit_id: S::CommitId,
    ) -> Result<SlabIndex, PendingError<S::CommitId>> {
        let slab_index = *self
            .index_map
            .get(&commit_id)
            .ok_or(PendingError::CommitIDNotFound(commit_id))?;
        Ok(slab_index)
    }

    fn get_node_by_slab_index(&self, slab_index: SlabIndex) -> &TreeNode<S> {
        &self.nodes[slab_index]
    }

    fn get_node_by_commit_id(
        &self,
        commit_id: S::CommitId,
    ) -> Result<&TreeNode<S>, PendingError<S::CommitId>> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;
        Ok(self.get_node_by_slab_index(slab_index))
    }

    fn has_root(&self) -> bool {
        !self.index_map.is_empty()
    }

    pub fn get_parent_commit_id(&self, node: &TreeNode<S>) -> Option<S::CommitId> {
        node.parent
            .map(|p_slab_index| self.nodes[p_slab_index].commit_id)
    }

    fn get_parent_node(&self, node: &TreeNode<S>) -> Option<&TreeNode<S>> {
        node.parent
            .map(|p_slab_index| self.get_node_by_slab_index(p_slab_index))
    }
}

impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn add_node(
        &mut self,
        commit_id: S::CommitId,
        parent_commit_id: Option<S::CommitId>,
        modifications: Modifications<S>,
    ) -> Result<(), PendingError<S::CommitId>> {
        // return error if Some(parent_commit_id) but parent_commit_id does not exist
        let (parent_slab_index, parent_height) = if let Some(parent_commit_id) = parent_commit_id {
            let p_slab_index = self.get_slab_index_by_commit_id(parent_commit_id)?;
            let p_height = self.get_node_by_slab_index(p_slab_index).height;
            (Some(p_slab_index), p_height)
        } else {
            // return error if want to add root but there has been a root
            if self.has_root() {
                return Err(PendingError::MultipleRootsNotAllowed);
            }
            (None, 0)
        };
        // return error if commit_id exists
        if self.index_map.contains_key(&commit_id) {
            return Err(PendingError::CommitIdAlreadyExists(commit_id));
        }
        let node = TreeNode::new(commit_id, parent_slab_index, parent_height, modifications);

        let slab_index = self.nodes.insert(node);
        self.index_map.insert(commit_id, slab_index);
        if let Some(parent_slab_index) = parent_slab_index {
            self.nodes[parent_slab_index].children.insert(slab_index);
        }
        Ok(())
    }
}

impl<S: PendingKeyValueSchema> Tree<S> {
    fn bfs_subtree(&self, subroot_slab_index: SlabIndex) -> Vec<SlabIndex> {
        let mut slab_indices = vec![subroot_slab_index];
        let mut head = 0;
        while head < slab_indices.len() {
            let node = self.get_node_by_slab_index(slab_indices[head]);

            for &child_index in &node.children {
                slab_indices.push(child_index);
            }

            head += 1;
        }

        slab_indices
    }

    fn find_path_nodes(
        &self,
        target_slab_index: SlabIndex,
    ) -> (Vec<S::CommitId>, HashSet<SlabIndex>) {
        let mut target_node = self.get_node_by_slab_index(target_slab_index);
        let mut path = Vec::new();
        let mut set = HashSet::new();
        while let Some(parent_slab_index) = target_node.parent {
            set.insert(parent_slab_index);
            target_node = self.get_node_by_slab_index(parent_slab_index);
            path.push(target_node.commit_id);
        }
        (path, set)
    }

    // todo: test
    #[allow(clippy::type_complexity)]
    pub fn change_root(
        &mut self,
        commit_id: S::CommitId,
    ) -> Result<(Vec<S::CommitId>, Vec<S::CommitId>), PendingError<S::CommitId>> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;

        // (root)..=(new_root's parent)
        let (to_commit_rev, to_commit_set) = self.find_path_nodes(slab_index);

        // subtree of new_root
        let to_maintain_vec = self.bfs_subtree(slab_index);
        let to_maintain = BTreeSet::from_iter(to_maintain_vec);

        // tree - subtree of new_root - (root)..-(new_root's parent)
        let mut to_remove_indices = Vec::new();
        for (idx, _) in self.nodes.iter() {
            if !to_maintain.contains(&idx) && !to_commit_set.contains(&idx) {
                to_remove_indices.push(idx);
            }
        }
        let mut to_remove = Vec::new();
        for idx in to_remove_indices.into_iter() {
            let to_remove_node = self.nodes.remove(idx);
            self.index_map.remove(&to_remove_node.commit_id);
            to_remove.push(to_remove_node.commit_id);
        }

        // set new_root's parent as None
        self.nodes[slab_index].parent = None;

        Ok((to_commit_rev, to_remove))
    }
}

impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn find_path(
        &self,
        target_commit_id: S::CommitId,
    ) -> Result<Commits<S>, PendingError<S::CommitId>> {
        let mut target_node = self.get_node_by_commit_id(target_commit_id)?;
        let mut commits_rev = HashMap::new();
        target_node.export_commit_data(&mut commits_rev);
        while let Some(parent_slab_index) = target_node.parent {
            target_node = self.get_node_by_slab_index(parent_slab_index);
            target_node.export_commit_data(&mut commits_rev);
        }
        Ok(commits_rev)
    }

    // correctness based on single root
    pub(super) fn lca(
        &self,
        current_commit_id: S::CommitId,
        target_commit_id: S::CommitId,
    ) -> Result<RollComm<S>, PendingError<S::CommitId>> {
        let mut current_node = self.get_node_by_commit_id(current_commit_id).unwrap();
        let mut target_node = self.get_node_by_commit_id(target_commit_id)?;
        let mut rollbacks = HashMap::new();
        let mut commits_rev = HashMap::new();
        while current_node.height > target_node.height {
            current_node.export_rollback_data(&mut rollbacks);
            current_node = self.get_parent_node(current_node).unwrap();
        }
        while target_node.height > current_node.height {
            target_node.export_commit_data(&mut commits_rev);
            target_node = self.get_parent_node(target_node).unwrap();
        }
        while current_node.commit_id != target_node.commit_id {
            current_node.export_rollback_data(&mut rollbacks);
            current_node = self.get_parent_node(current_node).unwrap();
            target_node.export_commit_data(&mut commits_rev);
            target_node = self.get_parent_node(target_node).unwrap();
        }
        // check rollbacks' old_commit_id because TreeNodes are deleted
        // in a lazy way with respect to TreeNodes.modifications
        // todo: test this lazy method
        for (_, old_commit_id_option) in rollbacks.iter_mut() {
            if let Some(ref old_commit_id) = old_commit_id_option {
                if !self.contains_commit_id(old_commit_id) {
                    *old_commit_id_option = None;
                }
            }
        }
        // rollbacks or commits_rev may be empty,
        // they contain current and target (if they are not lca), respectively,
        // but they do not contain lca
        Ok((rollbacks, commits_rev))
    }
}

impl<S: PendingKeyValueSchema> TreeNode<S> {
    pub fn new(
        commit_id: S::CommitId,
        parent: Option<SlabIndex>,
        parent_height: usize,
        modifications: Modifications<S>,
    ) -> Self {
        Self {
            height: parent_height + 1,
            commit_id,
            parent,
            children: BTreeSet::new(),
            modifications,
        }
    }

    pub fn get_commit_id(&self) -> S::CommitId {
        self.commit_id
    }

    pub fn get_modifications(
        &self,
    ) -> impl Iterator<Item = &(S::Key, Option<S::Value>, Option<S::CommitId>)> {
        self.modifications.iter()
    }

    pub fn export_rollback_data(&self, rollbacks: &mut HashMap<S::Key, Option<S::CommitId>>) {
        for (key, _, old_commit_id) in self.get_modifications() {
            rollbacks.insert(key.clone(), *old_commit_id);
        }
    }

    pub fn export_commit_data(&self, commits_rev: &mut Commits<S>) {
        let commit_id = self.commit_id;
        for (key, value, _) in self.get_modifications() {
            commits_rev
                .entry(key.clone())
                .or_insert_with(|| (commit_id, value.clone()));
        }
    }
}
