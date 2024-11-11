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
    /// Creates a fully initialized `CurrentMap` with the given `commit_id`.
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitId` corresponding to this `CurrentMap`.
    /// - `applys`: The changes to apply for the new state.
    ///
    /// # Returns:
    /// A fully initialized `CurrentMap` with both its `commit_id` and its relative snapshot (`map`) properly set.
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

    /// Switches from the current state to a new state associated with the given `commit_id`.
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitId` of the new state.
    /// - `rollbacks`: The changes to roll back from the current state.
    /// - `applys`: The changes to apply for the new state.
    ///
    /// # Notes:
    /// `rollbacks` should be before `applys`.
    pub fn switch_to_commit(
        &mut self,
        commit_id: S::CommitId,
        rollbacks: BTreeMap<S::Key, Option<ApplyRecord<S>>>,
        applys: ApplyMap<S>,
    ) {
        self.rollback(rollbacks);
        self.apply(applys);
        self.commit_id = commit_id;
    }

    /// Updates the current state in response to the change of the tree's root.
    ///
    /// This function retains only those entries in `CurrentMap` whose `commit_id`s are present in the provided `tree`.
    ///
    /// # Parameters:
    /// - `tree`: The `Tree` that contains the node corresponding to this `CurrentMap`.
    ///   The provided `tree` should be the one that this `CurrentMap` is associated with.
    pub fn adjust_for_new_root(&mut self, tree: &Tree<S>) {
        self.map
            .retain(|_, ApplyRecord { commit_id, .. }| tree.contains_commit_id(commit_id));
    }

    /// Rolls back changes in the current state based on the provided rollbacks.
    ///
    /// This function updates the `CurrentMap` by removing or restoring entries based on the
    /// specified rollbacks. If a key is associated with `None`, it is removed from the map.
    /// If a key is associated with a specific `ApplyRecord`, that record is restored in the map.
    ///
    /// # Parameters:
    /// - `rollbacks`: A mapping of keys to their corresponding rollback records.
    ///   - If a key's value is `None`, it indicates that the key should be removed from the map.
    ///   - If a key's value is `Some(ApplyRecord)`, it indicates that this record should be restored.
    ///
    /// # Notes:
    /// This function is private and should be used in conjunction with other functions and statements
    /// to ensure that `self.map` and `self.commit_id` remain consistent.
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

    /// Applies changes to the current state based on the provided updates.
    ///
    /// This function updates the `CurrentMap` by inserting the specified key-value pairs
    /// into the map.
    ///
    /// # Parameters:
    /// - `applys`: A mapping of keys to their corresponding `ApplyRecord` values.
    ///   Each entry represents a change to be applied to the current state.
    ///
    /// # Notes:
    /// This function is private and should be used in conjunction with other functions and statements
    /// to ensure that `self.map` and `self.commit_id` remain consistent.
    fn apply(&mut self, applys: ApplyMap<S>) {
        for (key, apply) in applys.into_iter() {
            self.map.insert(key, apply);
        }
    }
}
