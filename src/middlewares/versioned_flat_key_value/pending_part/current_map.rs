use std::{collections::BTreeMap, ops::Deref};

use super::{
    pending_schema::{ApplyMap, ApplyRecord, PendingKeyValueSchema},
    tree::Tree,
};

/// A `CurrentMap` stores a `CommitId` and its corresponding *relative snapshot*.
/// A *relative snapshot* refers to all the changes (including both the key-value pair,
/// and the `CommitId` that indicates where each change was most recently modified）
/// in the snapshot corresponding to this `CommitId` relative to the snapshot of the parent of the pending root.
pub(super) struct CurrentMap<S: PendingKeyValueSchema> {
    /// `commit_id`: the `CommitId` that `CurrentMap` corresponds to
    commit_id: S::CommitId,
    /// `map`: *relative snapshot* at `commit_id`
    map: BTreeMap<S::Key, ApplyRecord<S>>,
}

impl<S: PendingKeyValueSchema> Deref for CurrentMap<S> {
    type Target = BTreeMap<S::Key, ApplyRecord<S>>;

    /// Deref `CurrentMap` as its `map` field
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<S: PendingKeyValueSchema> CurrentMap<S> {
    pub fn new(commit_id: S::CommitId, applys: ApplyMap<S>) -> Self {
        let mut current_map = Self {
            map: BTreeMap::new(),
            commit_id,
        };
        current_map.apply(applys);
        current_map
    }

    /// Returns the `CommitId` of this `CurrentMap`.
    pub fn get_commit_id(&self) -> S::CommitId {
        self.commit_id
    }

    pub fn switch_to_commit(
        &mut self,
        rollbacks: BTreeMap<S::Key, Option<ApplyRecord<S>>>,
        applys: ApplyMap<S>,
        commit_id: S::CommitId,
    ) {
        self.rollback(rollbacks);
        self.apply(applys);
        self.commit_id = commit_id;
    }

    pub fn adjust_for_new_root(&mut self, tree: &Tree<S>) {
        self.map
            .retain(|_, ApplyRecord { commit_id, .. }| tree.contains_commit_id(commit_id));
    }

    fn rollback(&mut self, rollbacks: BTreeMap<S::Key, Option<ApplyRecord<S>>>) {
        for (key, to_rollback) in rollbacks.into_iter() {
            match to_rollback {
                None => {
                    self.map.remove(&key);
                }
                Some(to_rollback_record) => {
                    self.map.insert(key, to_rollback_record);
                }
            }
        }
    }

    fn apply(&mut self, applys: ApplyMap<S>) {
        for (key, apply) in applys.into_iter() {
            self.map.insert(key, apply);
        }
    }
}
