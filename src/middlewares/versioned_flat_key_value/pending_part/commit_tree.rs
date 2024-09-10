use std::collections::{BTreeSet, HashMap, HashSet};
use std::{fmt::Debug, hash::Hash};

use slab::Slab;

use super::PendingError;

type SlabIndex = usize;

pub struct TreeNode<Key: Eq + Hash + Clone, CommitId: Debug + Eq + Hash + Copy, Value: Clone> {
    parent: Option<SlabIndex>,
    children: BTreeSet<SlabIndex>,

    // todo: test lazy height
    // height will not be changed even when root is changed
    // height is only used for lca
    height: usize,

    commit_id: CommitId,
    // before current node, the old value of this key is modified by which commit_id,
    // if none, this key is absent before current node
    // here must use CommitID instead of SlabIndex (which may be reused, see slab doc)
    modifications: Vec<(Key, Option<Value>, Option<CommitId>)>,
}

pub struct Tree<Key: Eq + Hash + Clone, CommitId: Debug + Eq + Hash + Copy, Value: Clone> {
    nodes: Slab<TreeNode<Key, CommitId, Value>>,
    index_map: HashMap<CommitId, SlabIndex>,
}

impl<Key: Eq + Hash + Clone, CommitId: Debug + Eq + Hash + Copy, Value: Clone>
    Tree<Key, CommitId, Value>
{
    pub fn new() -> Self {
        Tree {
            nodes: Slab::new(),
            index_map: HashMap::new(),
        }
    }

    fn contains_commit_id(&self, commit_id: &CommitId) -> bool {
        self.index_map.contains_key(commit_id)
    }

    fn commit_id_to_slab_index(
        &self,
        commit_id: CommitId,
    ) -> Result<SlabIndex, PendingError<CommitId>> {
        let slab_index = *self
            .index_map
            .get(&commit_id)
            .ok_or_else(|| PendingError::CommitIDNotFound(commit_id))?;
        Ok(slab_index)
    }

    fn slab_index_to_node(&self, slab_index: SlabIndex) -> &TreeNode<Key, CommitId, Value> {
        &self.nodes[slab_index]
    }

    fn has_root(&self) -> bool {
        !self.index_map.is_empty()
    }

    pub fn get_parent_commit_id(&self, node: &TreeNode<Key, CommitId, Value>) -> Option<CommitId> {
        node.parent
            .and_then(|p_slab_index| Some(self.nodes[p_slab_index].commit_id))
    }
}

impl<Key: Eq + Hash + Clone, CommitId: Debug + Eq + Hash + Copy, Value: Clone>
    Tree<Key, CommitId, Value>
{
    pub fn add_node(
        &mut self,
        commit_id: CommitId,
        parent_commit_id: Option<CommitId>,
        modifications: Vec<(Key, Option<Value>, Option<CommitId>)>,
    ) -> Result<(), PendingError<CommitId>> {
        // return error if Some(parent_commit_id) but parent_commit_id does not exist
        let (parent_slab_index, parent_height) = if let Some(parent_commit_id) = parent_commit_id {
            let p_slab_index = self.commit_id_to_slab_index(parent_commit_id)?;
            let p_height = self.slab_index_to_node(p_slab_index).height;
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

impl<Key: Eq + Hash + Clone, CommitId: Debug + Eq + Hash + Copy, Value: Clone>
    Tree<Key, CommitId, Value>
{
    fn bfs_subtree(&self, subroot_slab_index: SlabIndex) -> Vec<SlabIndex> {
        let mut slab_indices = vec![subroot_slab_index];
        let mut head = 0;
        while head < slab_indices.len() {
            let node = self.slab_index_to_node(slab_indices[head]);

            for &child_index in &node.children {
                slab_indices.push(child_index);
            }

            head += 1;
        }

        slab_indices
    }

    fn find_path_nodes(&self, target_slab_index: SlabIndex) -> (Vec<CommitId>, HashSet<SlabIndex>) {
        let mut target_node = self.slab_index_to_node(target_slab_index);
        let mut path = Vec::new();
        let mut set = HashSet::new();
        while target_node.parent.is_some() {
            let slab_index = target_node.parent.unwrap();
            set.insert(slab_index);
            target_node = self.slab_index_to_node(slab_index);
            path.push(target_node.commit_id);
        }
        (path, set)
    }

    // todo: test
    pub fn change_root(
        &mut self,
        commit_id: CommitId,
    ) -> Result<(Vec<CommitId>, Vec<CommitId>), PendingError<CommitId>> {
        let slab_index = self.commit_id_to_slab_index(commit_id)?;

        // (root)..=(new_root's parent)
        let (to_commit_rev, to_commit_set) = self.find_path_nodes(slab_index);

        // subtree of new_root
        let to_maintain_vec = self.bfs_subtree(slab_index);
        let to_maintain = BTreeSet::from_iter(to_maintain_vec.into_iter());

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

impl<Key: Eq + Hash + Clone, CommitId: Debug + Eq + Hash + Copy, Value: Clone>
    Tree<Key, CommitId, Value>
{
    pub fn find_path(
        &self,
        target_commit_id: CommitId,
    ) -> Result<HashMap<Key, (CommitId, Option<Value>)>, PendingError<CommitId>> {
        let target_slab_index = self.commit_id_to_slab_index(target_commit_id)?;
        let mut target_node = self.slab_index_to_node(target_slab_index);
        let mut commits_rev = HashMap::new();
        loop {
            target_node.target_up(&mut commits_rev);
            if target_node.parent.is_none() {
                break;
            }
            target_node = self.slab_index_to_node(target_node.parent.unwrap());
        }
        Ok(commits_rev)
    }

    // correctness based on single root
    pub fn lca(
        &self,
        current_commit_id: CommitId,
        target_commit_id: CommitId,
    ) -> Result<
        (
            HashMap<Key, Option<CommitId>>,
            HashMap<Key, (CommitId, Option<Value>)>,
        ),
        PendingError<CommitId>,
    > {
        let current_slab_index = self.commit_id_to_slab_index(current_commit_id).unwrap();
        let target_slab_index = self.commit_id_to_slab_index(target_commit_id)?;
        let mut current_node = self.slab_index_to_node(current_slab_index);
        let mut target_node = self.slab_index_to_node(target_slab_index);
        let mut rollbacks = HashMap::new();
        let mut commits_rev = HashMap::new();
        while current_node.height > target_node.height {
            current_node.current_up(&mut rollbacks);
            current_node = self.slab_index_to_node(current_node.parent.unwrap());
        }
        while target_node.height > current_node.height {
            target_node.target_up(&mut commits_rev);
            target_node = self.slab_index_to_node(target_node.parent.unwrap());
        }
        while current_node.commit_id != target_node.commit_id {
            current_node.current_up(&mut rollbacks);
            current_node = self.slab_index_to_node(current_node.parent.unwrap());
            target_node.target_up(&mut commits_rev);
            target_node = self.slab_index_to_node(target_node.parent.unwrap());
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

impl<Key: Eq + Hash + Clone, CommitId: Debug + Eq + Hash + Copy, Value: Clone>
    TreeNode<Key, CommitId, Value>
{
    pub fn new(
        commit_id: CommitId,
        parent: Option<SlabIndex>,
        parent_height: usize,
        modifications: Vec<(Key, Option<Value>, Option<CommitId>)>,
    ) -> Self {
        Self {
            height: parent_height + 1,
            commit_id,
            parent,
            children: BTreeSet::new(),
            modifications,
        }
    }

    pub fn get_commit_id(&self) -> CommitId {
        self.commit_id
    }

    pub fn get_modifications(
        &self,
    ) -> impl Iterator<Item = &(Key, Option<Value>, Option<CommitId>)> {
        self.modifications.iter()
    }

    pub fn current_up(&self, rollbacks: &mut HashMap<Key, Option<CommitId>>) {
        for (key, _, old_commit_id) in self.get_modifications() {
            rollbacks.insert(key.clone(), *old_commit_id);
        }
    }

    pub fn target_up(&self, commits_rev: &mut HashMap<Key, (CommitId, Option<Value>)>) {
        let commit_id = self.commit_id;
        for (key, value, _) in self.get_modifications() {
            commits_rev
                .entry(key.clone())
                .or_insert_with(|| (commit_id, value.clone()));
        }
    }
}
