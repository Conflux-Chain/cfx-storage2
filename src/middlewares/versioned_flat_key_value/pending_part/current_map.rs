use std::{collections::BTreeMap, ops::Deref};

use super::pending_schema::{ApplyMap, ApplyRecord, PendingKeyValueSchema};

pub(super) struct CurrentMap<S: PendingKeyValueSchema> {
    map: BTreeMap<S::Key, ApplyRecord<S>>,
    commit_id: S::CommitId,
}

impl<S: PendingKeyValueSchema> Deref for CurrentMap<S> {
    type Target = BTreeMap<S::Key, ApplyRecord<S>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<S: PendingKeyValueSchema> CurrentMap<S> {
    pub fn new(commit_id: S::CommitId) -> Self {
        Self {
            map: BTreeMap::new(),
            commit_id,
        }
    }

    pub fn get_commit_id(&self) -> S::CommitId {
        self.commit_id
    }

    pub fn set_commit_id(&mut self, commit_id: S::CommitId) {
        self.commit_id = commit_id;
    }

    pub fn rollback(&mut self, rollbacks: BTreeMap<S::Key, Option<ApplyRecord<S>>>) {
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
}
