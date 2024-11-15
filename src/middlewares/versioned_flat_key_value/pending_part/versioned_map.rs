//! Implementation of [`VersionedMap`]

use std::collections::BTreeMap;

use crate::traits::{IsCompleted, NeedNext};
use crate::types::ValueEntry;

use super::{
    current_map::CurrentMap,
    pending_schema::{
        ChangeWithRecoverRecord, KeyValueMap, PendingKeyValueSchema, Result as PendResult,
    },
    tree::Tree,
    PendingError,
};

use parking_lot::RwLock;

/// The pending part of the underlying database (see [`super::super::VersionedStore`]).
///
/// # Overview:
/// The pending part is a tree, where each node represents a snapshot of a key-value store,
/// and each edge represents the changes between parent and child nodes. The root of this tree is unique,
/// and its parent is always the latest node in the historical part. If the historical part is empty,
/// the parent of the pending root is `None`.
///
/// # Implementation Details:
/// [`VersionedMap`] supports efficient queries, as well as adding or discarding nodes in the pending part.
/// It is built on top of [`Tree`] and leverages a [`CurrentMap`] to optimize operations.
/// A [`CurrentMap`] stores a `CommitId` and its corresponding *relative snapshot*,
/// which represents all changes relative to the snapshot of the parent of the pending root.
/// While the [`Tree`] tracks all nodes (commits) and their relationships, [`VersionedMap`] provides a more efficient
/// interface for switching between commits and querying snapshots.
/// More specifically, by propagating updates downwards or tracing upwards through the `Tree`,
/// one `CurrentMap` can be efficiently derived from another.
///
/// # Future Optimizations:
/// Since [`CurrentMap`] and [`Tree`] are decoupled, in principle, multiple `CurrentMap`s with multiple branches
/// could be maintained to accelerate queries. However, currently only one `CurrentMap` is maintained at a time.
pub struct VersionedMap<S: PendingKeyValueSchema> {
    tree: Tree<S>,
    /// `None`: no `CurrentMap` has been computed yet or the corresponding node has been pruned.
    current: RwLock<Option<CurrentMap<S>>>,
}

impl<S: PendingKeyValueSchema> VersionedMap<S> {
    /// Initializes the pending part of the database, with an empty `tree` and a no computed `current`.
    ///
    /// # Parameters:
    /// - `parent_of_root`: The latest `CommitId` in the historical part. If `None`, the historical part is empty.
    /// - `height_of_root`: The height of the pending root in the entire underlying database.
    pub fn new(parent_of_root: Option<S::CommitId>, height_of_root: usize) -> Self {
        VersionedMap {
            tree: Tree::new(parent_of_root, height_of_root),
            current: RwLock::new(None),
        }
    }

    #[cfg(test)]
    pub fn check_consistency(&self, height_of_root: usize) -> bool {
        if self.tree.check_consistency(height_of_root) {
            // todo: check current
            true
        } else {
            false
        }
    }

    /// Returns the latest `CommitId` in the historical part. If `None`, the historical part is empty.
    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.tree.get_parent_of_root()
    }
}

// add_node
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    /// Adds a node to the pending part.
    /// Distinguishes whether the added node is the root or a non-root, and calls the corresponding function.
    pub fn add_node(
        &mut self,
        updates: impl IntoIterator<Item = (S::Key, Option<S::Value>)>,
        commit_id: S::CommitId,
        parent_commit_id: Option<S::CommitId>,
    ) -> PendResult<(), S> {
        let updates = updates
            .into_iter()
            .map(|(key, value)| (key, ValueEntry::from_option(value)));
        if self.get_parent_of_root() == parent_commit_id {
            self.add_root(updates, commit_id)
        } else if let Some(parent_commit_id) = parent_commit_id {
            self.add_non_root_node(updates, commit_id, parent_commit_id)
        } else {
            Err(PendingError::NonRootNodeShouldHaveParent)
        }
    }

    /// Adds the pending root.
    ///
    /// # Notes:
    /// When adding the root, since there are no nodes before the root in the pending part,
    /// the `last_commit_id` showing the last modification in the pending tree is set to `None`.
    ///
    /// Returns an error if:
    /// - the pending root already exists.
    fn add_root(
        &mut self,
        updates: impl Iterator<Item = (S::Key, ValueEntry<S::Value>)>,
        commit_id: S::CommitId,
    ) -> PendResult<(), S> {
        let enact_update = |(key, value)| {
            (
                key,
                ChangeWithRecoverRecord::<S> {
                    value,
                    last_commit_id: None,
                },
            )
        };

        let modifications = updates.map(enact_update).collect();
        self.tree.add_root(commit_id, modifications)?;

        Ok(())
    }

    /// Adds a non-root node to the pending part.
    ///
    /// # Notes:
    /// When adding the root, to calculate the `last_commit_id` showing the last modification in the pending tree,
    /// obtaining the [`CurrentMap`] at `parent_commit_id` is necessary.
    ///
    /// Returns an error if:
    /// - `parent_commit_id` does not exist in the pending part.
    /// - `commit_id` already exists in the pending part.
    fn add_non_root_node(
        &mut self,
        updates: impl Iterator<Item = (S::Key, ValueEntry<S::Value>)>,
        commit_id: S::CommitId,
        parent_commit_id: S::CommitId,
    ) -> PendResult<(), S> {
        // let parent to be self.current
        // this step is necessary for computing modifications' last_commit_id
        let mut guard = self.current.write();
        self.tree.checkout_current(parent_commit_id, &mut guard)?;

        // add node to tree
        let current = guard.as_ref().unwrap();
        let mut modifications = BTreeMap::new();
        for (key, value) in updates {
            let last_commit_id = current.get(&key).map(|s| s.commit_id);
            modifications.insert(
                key,
                ChangeWithRecoverRecord {
                    value,
                    last_commit_id,
                },
            );
        }
        self.tree
            .add_non_root_node(commit_id, parent_commit_id, modifications)?;

        Ok(())
    }
}

// change_root
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    #[allow(clippy::type_complexity)]
    // old_root..=new_root's parent: (commit_id, key_value_map)
    pub fn change_root(
        &mut self,
        commit_id: S::CommitId,
    ) -> PendResult<(usize, Vec<(S::CommitId, KeyValueMap<S>)>), S> {
        // to_commit: old_root..=new_root's parent
        let (start_height_to_commit, to_commit) = self.tree.change_root(commit_id)?;

        if let Some(parent_of_new_root) = to_commit.last() {
            // clear current is necessary
            // because apply_commit_id in current.map may be removed from pending part
            self.clear_removed_current();
            if let Some(current) = self.current.get_mut() {
                current.adjust_for_new_root(&self.tree);
            }
        }

        Ok((start_height_to_commit, to_commit))
    }
}

// Helper methods in pending part to support
// impl KeyValueStoreManager for VersionedStore
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    /// Queries the modification history of a given `Key` in the pending part.
    /// Starts from the given `CommitID` and iterates changes backward in the pending part.
    ///
    /// # Parameters:
    /// - `accept`: A function that receives a change and returns whether to continue.
    ///
    /// # Returns:
    /// A `Result` with a boolean ([`IsCompleted`]) indicating whether the iteration is completed, or an error if:
    /// - The `commit_id` does not exist in the pending part.
    ///
    /// # Notes:
    /// All information required is in `self.tree`. No read or write on `self.current`.
    pub fn iter_historical_changes(
        &self,
        mut accept: impl FnMut(&S::CommitId, &S::Key, Option<&S::Value>) -> NeedNext,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<IsCompleted, S> {
        self.tree
            .iter_historical_changes(&mut accept, commit_id, key)
    }

    /// Queries the change of the given `Key` in the snapshot at the given `CommitId` in the pending part
    /// relative to the snapshot of the parent of the pending root.
    ///
    /// Returns the changed value if successful:
    /// - `None`: no change of `key` compared to the parent of the pending root;
    /// - `Some(ValueEntry::Deleted)`: `key` is deleted;
    /// - `Some(ValueEntry::Value(value))`: `key`'s value is set to `value`.
    ///
    /// Returns an error if:
    /// - `commit_id` does not exist in the pending part.
    ///
    /// # Notes:
    /// All information required is in `self.tree`. No read or write on `self.current`.
    pub fn get_versioned_key(
        &self,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<ValueEntry<S::Value>>, S> {
        self.tree.get_versioned_key(commit_id, key)
    }

    // alternative method of self.get_versioned_key(),
    // but it invokes self.checkout_current(),
    // thus is only suitable for frequent commit_id
    #[cfg(test)]
    pub fn get_versioned_key_with_checkout(
        &self,
        commit_id: S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<ValueEntry<S::Value>>, S> {
        // let query node to be self.current
        let mut guard = self.current.write();
        self.tree.checkout_current(commit_id, &mut guard)?;

        let current = guard.as_ref().unwrap();
        Ok(current.get(key).map(|c| c.value.clone()))
    }

    pub fn discard(&mut self, commit_id: S::CommitId) -> PendResult<(), S> {
        self.tree.discard(commit_id)?;

        self.clear_removed_current();

        Ok(())
    }

    fn clear_removed_current(&mut self) {
        let current = self.current.get_mut();

        let obsoleted_commit_id =
            |c: &CurrentMap<S>| !self.tree.contains_commit_id(&c.get_commit_id());

        if current.as_ref().map_or(false, obsoleted_commit_id) {
            *current = None;
        }
    }

    /// Querys the changes of key-value pairs in the snapshot at a given `CommitID` relative to the parent of the pending root.
    ///
    /// Returns an error if:
    /// - The `commit_id` does not exist in the pending part.
    ///
    /// # Notes:
    /// Need to obtain the [`CurrentMap`] at `commit_id`.
    ///
    /// # ATTENTION:
    /// - `checkout_current` & read current should be guarded together.
    pub fn get_versioned_store(&self, commit_id: S::CommitId) -> PendResult<KeyValueMap<S>, S> {
        // let query node to be self.current
        let mut guard = self.current.write();
        self.tree.checkout_current(commit_id, &mut guard)?;

        let current = guard.as_ref().unwrap();
        Ok(current
            .iter()
            .map(|(k, apply_record)| (k.clone(), apply_record.value.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backends::VersionedKVName,
        middlewares::versioned_flat_key_value::{
            pending_part::pending_schema::PendingKeyValueConfig,
            table_schema::VersionedKeyValueSchema,
        },
    };

    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use rand_distr::{Distribution, Uniform};

    pub type CommitId = u64;

    #[derive(Clone, Copy)]
    struct TestSchema;

    impl VersionedKeyValueSchema for TestSchema {
        const NAME: crate::backends::VersionedKVName = VersionedKVName::FlatKV;
        type Key = u64;
        type Value = u64;
    }

    type TestPendingConfig = PendingKeyValueConfig<TestSchema, CommitId>;

    fn random_key_value(rng: &mut StdRng) -> (u64, Option<u64>) {
        // use a small key range to achieve more key conflicts
        let key_range = Uniform::from(0..10);
        let key: u64 = key_range.sample(rng);

        let value: Option<u64> = if rng.gen_range(0..2) == 0 {
            Some(rng.gen::<u64>())
        } else {
            None
        };

        (key, value)
    }

    fn generate_random_tree(
        num_nodes: usize,
        rng: &mut StdRng,
    ) -> (Tree<TestPendingConfig>, VersionedMap<TestPendingConfig>) {
        let mut forward_only_tree = Tree::new(None, 0);
        let mut versioned_map = VersionedMap::new(None, 0);

        for i in 1..=num_nodes as CommitId {
            let parent_commit_id = if i == 1 {
                None
            } else {
                Some(rng.gen_range(1..i))
            };
            let mut updates = BTreeMap::new();
            for _ in 0..5 {
                let (key, value) = random_key_value(rng);
                updates.insert(key, value);
            }
            let updates_none = updates
                .iter()
                .map(|(key, value)| {
                    (
                        *key,
                        ChangeWithRecoverRecord {
                            value: ValueEntry::from_option(*value),
                            last_commit_id: None,
                        },
                    )
                })
                .collect();

            if let Some(parent_commit_id) = parent_commit_id {
                forward_only_tree
                    .add_non_root_node(i, parent_commit_id, updates_none)
                    .unwrap();
            } else {
                forward_only_tree.add_root(i, updates_none).unwrap();
            }
            versioned_map
                .add_node(updates, i, parent_commit_id)
                .unwrap();
        }
        (forward_only_tree, versioned_map)
    }

    #[test]
    fn test_get_versioned_key() {
        let num_nodes = 30;
        let num_query = 100;

        let seed: [u8; 32] = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ];
        let mut rng = StdRng::from_seed(seed);

        let (forward_only_tree, versioned_map) = generate_random_tree(num_nodes, &mut rng);
        for _ in 0..num_query {
            let commit_id = rng.gen_range(1..=num_nodes) as CommitId;
            for ikey in 0..10 {
                let key: u64 = ikey;
                let versioned_value = versioned_map.get_versioned_key(&commit_id, &key).unwrap();
                let versioned_value_with_checkout = versioned_map
                    .get_versioned_key_with_checkout(commit_id, &key)
                    .unwrap();
                let apply_map = forward_only_tree
                    .get_apply_map_from_root_included_for_test(commit_id)
                    .unwrap();
                let answer = apply_map.get(&key).map(|a| a.value.clone());
                assert_eq!(versioned_value, answer);
                assert_eq!(versioned_value_with_checkout, answer);
            }
        }
    }

    #[test]
    fn test_multiple_roots_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map = VersionedMap::<TestPendingConfig>::new(None, 0);

        forward_only_tree.add_root(0, BTreeMap::new()).unwrap();
        versioned_map.add_node(BTreeMap::new(), 0, None).unwrap();

        assert_eq!(
            forward_only_tree.add_root(1, BTreeMap::new()),
            Err(PendingError::MultipleRootsNotAllowed)
        );
        assert_eq!(
            versioned_map.add_node(BTreeMap::new(), 1, None),
            Err(PendingError::MultipleRootsNotAllowed)
        );
    }

    #[test]
    fn test_commit_id_not_found_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map = VersionedMap::<TestPendingConfig>::new(None, 0);

        assert_eq!(
            forward_only_tree.add_non_root_node(1, 0, BTreeMap::new()),
            Err(PendingError::CommitIDNotFound(0))
        );
        assert_eq!(
            versioned_map.add_node(BTreeMap::new(), 1, Some(0)),
            Err(PendingError::CommitIDNotFound(0))
        );
    }

    #[test]
    fn test_commit_id_already_exists_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map = VersionedMap::<TestPendingConfig>::new(None, 0);

        forward_only_tree.add_root(0, BTreeMap::new()).unwrap();
        versioned_map.add_node(BTreeMap::new(), 0, None).unwrap();

        assert_eq!(
            forward_only_tree.add_non_root_node(0, 0, BTreeMap::new()),
            Err(PendingError::CommitIdAlreadyExists(0))
        );
        assert_eq!(
            versioned_map.add_node(BTreeMap::new(), 0, Some(0)),
            Err(PendingError::CommitIdAlreadyExists(0))
        );
    }
}
