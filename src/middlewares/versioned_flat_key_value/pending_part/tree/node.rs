use std::collections::BTreeSet;

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::{
    ApplyMap, ApplyRecord, KeyValueMap, LastCommitIdMap, PendingKeyValueSchema, RecoverMap,
    RecoverRecord,
};
use crate::types::ValueEntry;

use super::SlabIndex;

pub(super) struct TreeNode<S: PendingKeyValueSchema> {
    parent: Option<SlabIndex>,
    children: BTreeSet<SlabIndex>,

    // todo: test lazy height
    // height will not be changed even when root is changed
    height: usize,

    commit_id: S::CommitId,
    // before current node, the old value of this key is modified by which commit_id,
    // if none, this key is absent before current node
    // here must use CommitID instead of SlabIndex (which may be reused, see slab doc)
    modifications: RecoverMap<S>,
}

impl<S: PendingKeyValueSchema> TreeNode<S> {
    pub fn new_root(commit_id: S::CommitId, modifications: RecoverMap<S>, height: usize) -> Self {
        Self {
            height,
            commit_id,
            parent: None,
            children: BTreeSet::new(),
            modifications,
        }
    }

    pub fn new_non_root_node(
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

    pub fn get_parent(&self) -> Option<SlabIndex> {
        self.parent
    }

    pub fn set_as_root(&mut self) {
        self.parent = None;
    }

    pub fn get_children(&self) -> &BTreeSet<SlabIndex> {
        &self.children
    }

    pub fn insert_child(&mut self, new_child: SlabIndex) {
        self.children.insert(new_child);
    }

    pub fn remove_child_except(&mut self, child_to_remove: &SlabIndex) {
        self.children = BTreeSet::from([*child_to_remove]);
    }

    pub fn get_height(&self) -> usize {
        self.height
    }

    pub fn get_commit_id(&self) -> S::CommitId {
        self.commit_id
    }

    pub fn get_modified_value(&self, key: &S::Key) -> Option<ValueEntry<S::Value>> {
        self.modifications.get(key).map(|v| v.value.clone())
    }

    pub fn get_updates(&self) -> KeyValueMap<S> {
        self.modifications
            .iter()
            .map(|(k, RecoverRecord { value, .. })| (k.clone(), value.clone()))
            .collect()
    }

    pub fn export_rollback_data<const OVERRIDE: bool>(&self, rollbacks: &mut LastCommitIdMap<S>) {
        for (key, RecoverRecord { last_commit_id, .. }) in self.modifications.iter() {
            if OVERRIDE {
                rollbacks.insert(key.clone(), *last_commit_id);
            } else {
                rollbacks.entry(key.clone()).or_insert(*last_commit_id);
            }
        }
    }

    pub fn export_commit_data<const OVERRIDE: bool>(&self, commits: &mut ApplyMap<S>) {
        let commit_id = self.commit_id;
        for (key, RecoverRecord { value, .. }) in self.modifications.iter() {
            let new_record = || ApplyRecord {
                commit_id,
                value: value.clone(),
            };
            if OVERRIDE {
                commits.insert(key.clone(), new_record());
            } else {
                commits.entry(key.clone()).or_insert_with(new_record);
            }
        }
    }
}
