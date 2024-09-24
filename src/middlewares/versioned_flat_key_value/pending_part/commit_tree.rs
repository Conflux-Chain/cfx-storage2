use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use slab::Slab;

use super::pending_schema::{
    ApplyMap, ApplyRecord, KeyValueMap, PendingKeyValueSchema, RecoverMap, RecoverRecord,
    Result as PendResult,
};
use super::PendingError;

type SlabIndex = usize;

pub struct TreeNode<S: PendingKeyValueSchema> {
    parent: Option<SlabIndex>,
    children: BTreeSet<SlabIndex>,

    // todo: test lazy height
    // height will not be changed even when root is changed
    // height is only used for lca in Tree.collect_rollback_and_apply_ops()
    height: usize,

    commit_id: S::CommitId,
    // before current node, the old value of this key is modified by which commit_id,
    // if none, this key is absent before current node
    // here must use CommitID instead of SlabIndex (which may be reused, see slab doc)
    modifications: RecoverMap<S>,
}

pub struct Tree<S: PendingKeyValueSchema> {
    height_of_root: usize,
    nodes: Slab<TreeNode<S>>,
    index_map: HashMap<S::CommitId, SlabIndex>,
}

impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn new(height_of_root: usize) -> Self {
        Tree {
            height_of_root,
            nodes: Slab::new(),
            index_map: HashMap::new(),
        }
    }

    fn contains_commit_id(&self, commit_id: &S::CommitId) -> bool {
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

    fn get_node_by_commit_id(&self, commit_id: S::CommitId) -> PendResult<&TreeNode<S>, S> {
        let slab_index = self.get_slab_index_by_commit_id(commit_id)?;
        Ok(self.get_node_by_slab_index(slab_index))
    }

    fn has_root(&self) -> bool {
        !self.index_map.is_empty()
    }

    fn get_parent_node(&self, node: &TreeNode<S>) -> Option<&TreeNode<S>> {
        node.parent
            .map(|p_slab_index| self.get_node_by_slab_index(p_slab_index))
    }

    fn get_by_commit_id(
        &self,
        commit_id: S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<Option<S::Value>>, S> {
        let node = self.get_node_by_commit_id(commit_id)?;
        Ok(node.modifications.get(key).map(|v| v.value.clone()))
    }
}

impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn add_root(
        &mut self,
        commit_id: S::CommitId,
        modifications: RecoverMap<S>,
    ) -> PendResult<(), S> {
        // return error if there is root
        if self.has_root() {
            return Err(PendingError::MultipleRootsNotAllowed);
        }
        // PendingError::CommitIdAlreadyExists(_) cannot happend because no root indicates no node

        // new root
        let root = TreeNode::new_root(commit_id, modifications, self.height_of_root);

        // add root to tree
        let slab_index = self.nodes.insert(root);
        self.index_map.insert(commit_id, slab_index);

        Ok(())
    }

    pub fn add_non_root_node(
        &mut self,
        commit_id: S::CommitId,
        parent_commit_id: S::CommitId,
        modifications: RecoverMap<S>,
    ) -> PendResult<(), S> {
        // return error if parent_commit_id does not exist
        let parent_slab_index = self.get_slab_index_by_commit_id(parent_commit_id)?;

        // return error if commit_id exists
        if self.contains_commit_id(&commit_id) {
            return Err(PendingError::CommitIdAlreadyExists(commit_id));
        }

        // new node
        let node = TreeNode::new(
            commit_id,
            parent_slab_index,
            self.get_node_by_slab_index(parent_slab_index).height + 1,
            modifications,
        );

        // add node to tree
        let slab_index = self.nodes.insert(node);
        self.index_map.insert(commit_id, slab_index);
        self.nodes[parent_slab_index].children.insert(slab_index);

        Ok(())
    }
}

impl<S: PendingKeyValueSchema> Tree<S> {
    // including subroot
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

    // excluding target
    fn find_path(&self, target_slab_index: SlabIndex) -> Vec<(S::CommitId, KeyValueMap<S>)> {
        let mut target_node = self.get_node_by_slab_index(target_slab_index);
        let mut path = VecDeque::new();
        while let Some(parent_slab_index) = target_node.parent {
            target_node = self.get_node_by_slab_index(parent_slab_index);
            path.push_front((target_node.commit_id, target_node.get_updates()));
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

        // root..=new_root's parent
        let to_commit = self.find_path(slab_index);

        // early return if new_root == root
        if to_commit.is_empty() {
            return Ok((self.nodes[slab_index].height - to_commit.len(), to_commit));
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
            self.index_map.remove(&self.nodes.remove(idx).commit_id);
        }

        // set new_root's parent as None
        self.nodes[slab_index].parent = None;

        // (height of new_root's parent, root..=new_root's parent)
        Ok((self.nodes[slab_index].height - to_commit.len(), to_commit))
    }
}

impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn get_apply_map_from_root_included(
        &self,
        target_commit_id: S::CommitId,
    ) -> PendResult<ApplyMap<S>, S> {
        let mut target_node = self.get_node_by_commit_id(target_commit_id)?;
        let mut commits_rev = BTreeMap::new();
        target_node.export_commit_data(&mut commits_rev);
        while let Some(parent_slab_index) = target_node.parent {
            target_node = self.get_node_by_slab_index(parent_slab_index);
            target_node.export_commit_data(&mut commits_rev);
        }
        Ok(commits_rev)
    }

    // correctness based on single root
    #[allow(clippy::type_complexity)]
    pub(super) fn collect_rollback_and_apply_ops(
        &self,
        current_commit_id: S::CommitId,
        target_commit_id: S::CommitId,
    ) -> PendResult<(BTreeMap<S::Key, Option<ApplyRecord<S>>>, ApplyMap<S>), S> {
        let mut current_node = self.get_node_by_commit_id(current_commit_id).unwrap();
        let mut target_node = self.get_node_by_commit_id(target_commit_id)?;
        let mut rollbacks = BTreeMap::new();
        let mut commits_rev = BTreeMap::new();
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

        let rollbacks_with_value: BTreeMap<_, _> = rollbacks
            .into_iter()
            .map(|(k, old_cid_opt)| match old_cid_opt {
                None => (k, None),
                Some(rollback_cid) => {
                    let rollback_value = self.get_by_commit_id(rollback_cid, &k).unwrap().unwrap();
                    (
                        k,
                        Some(ApplyRecord::<S> {
                            value: rollback_value,
                            commit_id: rollback_cid,
                        }),
                    )
                }
            })
            .collect();

        // rollbacks or commits_rev may be empty,
        // they contain current and target (if they are not lca), respectively,
        // but they do not contain lca
        Ok((rollbacks_with_value, commits_rev))
    }
}

impl<S: PendingKeyValueSchema> TreeNode<S> {
    fn new_root(commit_id: S::CommitId, modifications: RecoverMap<S>, height: usize) -> Self {
        Self {
            height,
            commit_id,
            parent: None,
            children: BTreeSet::new(),
            modifications,
        }
    }

    fn new(
        commit_id: S::CommitId,
        parent: SlabIndex,
        height: usize,
        modifications: RecoverMap<S>,
    ) -> Self {
        Self {
            height,
            commit_id,
            parent: Some(parent),
            children: BTreeSet::new(),
            modifications,
        }
    }

    fn get_updates(&self) -> KeyValueMap<S> {
        self.modifications
            .iter()
            .map(|(k, RecoverRecord { value, .. })| (k.clone(), value.clone()))
            .collect()
    }

    fn export_rollback_data(&self, rollbacks: &mut BTreeMap<S::Key, Option<S::CommitId>>) {
        for (key, RecoverRecord { last_commit_id, .. }) in self.modifications.iter() {
            rollbacks.insert(key.clone(), *last_commit_id);
        }
    }

    fn export_commit_data(&self, commits_rev: &mut ApplyMap<S>) {
        let commit_id = self.commit_id;
        for (key, RecoverRecord { value, .. }) in self.modifications.iter() {
            commits_rev
                .entry(key.clone())
                .or_insert_with(|| ApplyRecord {
                    commit_id,
                    value: value.clone(),
                });
        }
    }
}
