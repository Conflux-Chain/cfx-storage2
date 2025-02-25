use std::{collections::HashMap, ops::Deref};

use super::{
    pending_schema::{ApplyMap, ApplyRecord, PendingKeyValueSchema},
    tree::Tree,
};

pub(super) struct CurrentMap<S: PendingKeyValueSchema> {
    map: HashMap<S::Key, ApplyRecord<S>>,
    commit_id: S::CommitId,
}

impl<S: PendingKeyValueSchema> Deref for CurrentMap<S> {
    type Target = HashMap<S::Key, ApplyRecord<S>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<S: PendingKeyValueSchema> CurrentMap<S> {
    pub fn new(commit_id: S::CommitId) -> Self {
        Self {
            map: HashMap::new(),
            commit_id,
        }
    }

    pub fn get_commit_id(&self) -> S::CommitId {
        self.commit_id
    }

    pub fn set_commit_id(&mut self, commit_id: S::CommitId) {
        self.commit_id = commit_id;
    }

    pub fn rollback(&mut self, rollbacks: HashMap<S::Key, Option<ApplyRecord<S>>>) {
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

    pub fn apply(&mut self, applys: ApplyMap<S>) {
        for (key, apply) in applys.into_iter() {
            self.map.insert(key, apply);
        }
    }

    pub fn update_rerooted(&mut self, tree: &Tree<S>) {
        self.map
            .retain(|_, ApplyRecord { commit_id, .. }| tree.contains_commit_id(commit_id));
    }
}
