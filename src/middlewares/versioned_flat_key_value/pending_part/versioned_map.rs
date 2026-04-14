use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Bound;

use crate::backends::{DatabaseTrait, PendingTableName};
use crate::traits::{IsCompleted, NeedNext};
use crate::types::ValueEntry;

use super::pending_schema::{ConfirmedPathInfo, KeyValueMap};
use super::tree_with_tracker::TreeWithTracker;
use super::{
    current_map::CurrentMap,
    pending_schema::{PendingKeyValueSchema, RecoverRecord, Result as PendResult},
    PendingError,
};

use parking_lot::RwLock;

/// A versioned, in-memory key-value map with a persistent backend.
///
/// It represents the "pending" part, where modifications are held in memory (`tree`)
/// and simultaneously logged to a database for fault tolerance.
pub struct VersionedMap<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>> {
    /// A marker for the database trait `P`.
    ///
    /// This `VersionedMap` does not directly hold a database handle. Instead,
    /// database operations are performed via a `WriteSchema` passed into the modification methods.
    /// This marker ensures type safety by associating the `VersionedMap` with a specific
    /// database implementation `P` without needing to store an actual instance.
    db_marker: PhantomData<P>,

    /// The core in-memory data structure representing the versioned state of the pending part.
    ///
    /// It is bundled with a `PersistenceTracker` to correctly sequence its modifications
    /// for the recovery log. In the event of a crash, its state can be rebuilt to the last
    /// consistent state recorded in the database by replaying the logged operations.
    tree_with_tracker: TreeWithTracker<S>,

    /// A cache holding the key-value map for a specific, "current" version of the tree.
    ///
    /// This is purely an optimization for accelerating read operations. The data it holds
    /// is entirely derivable from the `tree_with_tracker`. Consequently, it is not involved
    /// in the persistence logic and is ignored during state recovery.
    current: RwLock<Option<CurrentMap<S>>>,
}

impl<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>> VersionedMap<S, P> {
    /// Creates a new `VersionedMap` from a pre-initialized in-memory state.
    ///
    /// This constructor assumes the caller has already prepared the storage and derived the correct
    /// initial in-memory state. It simply assembles the `VersionedMap` instance from these components.
    ///
    /// The provided `initial_state` typically originates from one of the following scenarios:
    /// 1. A state successfully recovered by replaying logs from an existing database.
    /// 2. A fresh, clean state created after a failed recovery attempt necessitated a database reset.
    /// 3. An empty state corresponding to a newly created database.
    ///
    /// **Important**: It is the caller's responsibility to ensure that the `initial_state` is
    /// perfectly consistent with the state of the database that will be used for persistence.
    /// The `initial_state` must be the exact result of applying all operations currently
    /// stored in the persistent backend.
    ///
    /// This means the database must not contain any log records that are not already accounted for
    /// in `initial_state`. For instance, no record can exist with a sequence identifier greater than
    /// or equal to the `next_modification_id` that `initial_state.tracker` is poised to generate
    /// for the current snapshot. Violating this invariant will corrupt the log sequence and lead to
    /// data inconsistency.
    ///
    /// # Arguments
    ///
    /// * `initial_state`: The initial in-memory tree and its corresponding persistence tracker,
    ///   representing a consistent state.
    pub fn from_initialized_state(initial_state: TreeWithTracker<S>) -> Self {
        VersionedMap {
            db_marker: PhantomData,
            tree_with_tracker: initial_state,
            current: RwLock::new(None),
        }
    }

    #[cfg(any(test, fuzzing))]
    pub fn check_consistency(&self, height_of_root: u64) -> bool {
        if self
            .tree_with_tracker
            .tree
            .check_consistency(height_of_root)
        {
            // todo: check current
            true
        } else {
            false
        }
    }

    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.tree_with_tracker.tree.get_parent_of_root()
    }

    pub fn get_height_of_root(&self) -> u64 {
        self.tree_with_tracker.tree.get_height_of_root()
    }
}

// add_node
impl<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>> VersionedMap<S, P> {
    pub fn add_node(
        &mut self,
        updates: impl IntoIterator<Item = (S::Key, Option<S::Value>)>,
        commit_id: S::CommitId,
        parent_commit_id: Option<S::CommitId>,
        write_schema: &P::WriteSchema,
    ) -> PendResult<()> {
        let updates = updates
            .into_iter()
            .map(|(key, value)| (key, ValueEntry::from_option(value)));
        if self.get_parent_of_root() == parent_commit_id {
            self.add_root(updates, commit_id, write_schema)
        } else if let Some(parent_commit_id) = parent_commit_id {
            self.add_non_root_node(updates, commit_id, parent_commit_id, write_schema)
        } else {
            Err(PendingError::NonRootNodeShouldHaveParent)
        }
    }

    fn add_root(
        &mut self,
        updates: impl Iterator<Item = (S::Key, ValueEntry<S::Value>)>,
        commit_id: S::CommitId,
        write_schema: &P::WriteSchema,
    ) -> PendResult<()> {
        let enact_update = |(key, value)| {
            (
                key,
                RecoverRecord::<S> {
                    value,
                    last_commit_id: None,
                },
            )
        };

        let modifications = updates.map(enact_update).collect();
        self.tree_with_tracker
            .add_root::<P>(commit_id, modifications, write_schema)?;

        Ok(())
    }

    fn add_non_root_node(
        &mut self,
        updates: impl Iterator<Item = (S::Key, ValueEntry<S::Value>)>,
        commit_id: S::CommitId,
        parent_commit_id: S::CommitId,
        write_schema: &P::WriteSchema,
    ) -> PendResult<()> {
        // let parent to be self.current
        // this step is necessary for computing modifications' last_commit_id
        let mut guard = self.current.write();
        self.tree_with_tracker
            .tree
            .checkout_current(parent_commit_id, &mut guard)?;

        // add node to tree
        let current = guard.as_ref().unwrap();
        let mut modifications = HashMap::new();
        for (key, value) in updates {
            let last_commit_id = current.get(&key).map(|s| s.commit_id);
            modifications.insert(
                key,
                RecoverRecord {
                    value,
                    last_commit_id,
                },
            );
        }
        self.tree_with_tracker.add_non_root_node::<P>(
            commit_id,
            parent_commit_id,
            modifications,
            write_schema,
        )?;

        Ok(())
    }
}

// change_root
impl<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>> VersionedMap<S, P> {
    pub fn get_ancestor_commit_at_height(
        &self,
        ancestor_height: u64,
        commit_id: S::CommitId,
    ) -> PendResult<S::CommitId> {
        self.tree_with_tracker
            .tree
            .get_ancestor_commit_at_height(ancestor_height, commit_id)
    }

    pub fn change_root(
        &mut self,
        commit_id: S::CommitId,
        write_schema: &P::WriteSchema,
    ) -> PendResult<Option<ConfirmedPathInfo<S>>> {
        let confirm_path_info = self
            .tree_with_tracker
            .change_root::<P>(commit_id, write_schema)?;

        if confirm_path_info.is_some() {
            // clear current is necessary
            // because apply_commit_id in current.map may be removed from pending part

            // Take a single write lock and do both operations under it
            let mut guard = self.current.write();
            self.clear_removed_current_with_guard(&mut guard);
            if let Some(current) = guard.as_mut() {
                current.update_rerooted(&self.tree_with_tracker.tree);
            }
        }

        Ok(confirm_path_info)
    }

    #[cfg(any(test, fuzzing))]
    pub fn change_root_without_persistence(
        &mut self,
        commit_id: S::CommitId,
    ) -> PendResult<Option<ConfirmedPathInfo<S>>> {
        let confirm_path_info = self
            .tree_with_tracker
            .change_root_without_persistence(commit_id)?;

        if confirm_path_info.is_some() {
            // clear current is necessary
            // because apply_commit_id in current.map may be removed from pending part

            // Take a single write lock and do both operations under it
            let mut guard = self.current.write();
            self.clear_removed_current_with_guard(&mut guard);
            if let Some(current) = guard.as_mut() {
                current.update_rerooted(&self.tree_with_tracker.tree);
            }
        }

        Ok(confirm_path_info)
    }

    /// This function discards the siblings of the nodes from the root (excluded) to `commit_id` (included).
    /// If there is at least one node discarded, return `Ok(true)`; otherwise, return `Ok(false)`.
    pub fn make_pivot(
        &mut self,
        commit_id: S::CommitId,
        write_schema: &P::WriteSchema,
    ) -> PendResult<bool> {
        let has_discarded_nodes = self
            .tree_with_tracker
            .make_pivot::<P>(commit_id, write_schema)?;

        if has_discarded_nodes {
            // clear current is necessary
            // because apply_commit_id in current.map may be removed from pending part
            let mut guard = self.current.write();
            self.clear_removed_current_with_guard(&mut guard);
        }

        Ok(has_discarded_nodes)
    }
}

// Helper methods in pending part to support
// impl KeyValueStoreManager for VersionedStore
impl<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>> VersionedMap<S, P> {
    pub fn iter_historical_changes(
        &self,
        mut accept: impl FnMut(&S::CommitId, &S::Key, Option<&S::Value>) -> NeedNext,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<IsCompleted> {
        self.tree_with_tracker
            .tree
            .iter_historical_changes(&mut accept, commit_id, key)
    }

    // None: pending_part not know
    // Some(None): pending_part know that this key has been deleted
    // Some(Some(value)): pending_part know this key's value
    pub fn get_versioned_key(
        &self,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<ValueEntry<S::Value>>> {
        let guard = self.current.read();

        let ancestor_cache = if let Some(current) = guard.as_ref() {
            if current.get_commit_id() == *commit_id {
                return Ok(current.get(key).map(|c| c.value.clone()));
            }
            let cached_value = current.get(key).map(|c| c.value.clone());
            Some((current.get_commit_id(), cached_value))
        } else {
            None
        };
        drop(guard);

        self.tree_with_tracker
            .tree
            .get_versioned_key(commit_id, key, ancestor_cache.as_ref())
    }

    // alternative method of self.get_versioned_key(),
    // but it invokes self.checkout_current(),
    // thus is only suitable for frequent commit_id
    #[cfg(test)]
    pub fn get_versioned_key_with_checkout(
        &self,
        commit_id: S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<ValueEntry<S::Value>>> {
        // let query node to be self.current
        let mut guard = self.current.write();
        self.tree_with_tracker
            .tree
            .checkout_current(commit_id, &mut guard)?;

        // Safety of unwrap: guard is set to be Some if it was None in self.tree.checkout_current.
        let current = guard.as_ref().unwrap();
        Ok(current.get(key).map(|c| c.value.clone()))
    }

    pub fn discard(
        &mut self,
        commit_id: S::CommitId,
        write_schema: &P::WriteSchema,
    ) -> PendResult<()> {
        let has_discarded_nodes = self
            .tree_with_tracker
            .discard::<P>(commit_id, write_schema)?;

        if has_discarded_nodes {
            let mut guard = self.current.write();
            self.clear_removed_current_with_guard(&mut guard);
        }

        Ok(())
    }

    fn clear_removed_current_with_guard<'a>(
        &'a self,
        guard: &mut parking_lot::RwLockWriteGuard<'a, Option<CurrentMap<S>>>,
    ) {
        let obsoleted_commit_id = |c: &CurrentMap<S>| {
            !self
                .tree_with_tracker
                .tree
                .contains_commit_id(&c.get_commit_id())
        };

        if guard.as_ref().is_some_and(obsoleted_commit_id) {
            **guard = None;
        }
    }

    pub fn contains_commit_id(&self, commit_id: &S::CommitId) -> bool {
        self.tree_with_tracker.tree.contains_commit_id(commit_id)
    }

    pub fn checkout_current(&self, commit_id: S::CommitId) -> PendResult<()> {
        // let query node to be self.current
        let mut guard = self.current.write();
        self.tree_with_tracker
            .tree
            .checkout_current(commit_id, &mut guard)?;

        Ok(())
    }

    pub fn get_versioned_store(&self, commit_id: S::CommitId) -> PendResult<KeyValueMap<S>> {
        // let query node to be self.current
        let mut guard = self.current.write();
        self.tree_with_tracker
            .tree
            .checkout_current(commit_id, &mut guard)?;

        let current = guard.as_ref().unwrap();
        Ok(current
            .iter()
            .map(|(k, apply_record)| (k.clone(), apply_record.value.clone()))
            .collect())
    }
}

impl<S, P: DatabaseTrait<PendingTableName>> VersionedMap<S, P>
where
    S: PendingKeyValueSchema,
    S::Key: AsRef<[u8]>,
{
    pub fn get_versioned_store_range(
        &self,
        commit_id: S::CommitId,
        lower_bound_incl: S::Key,
        upper_bound_excl: Option<S::Key>,
    ) -> PendResult<KeyValueMap<S>> {
        // let query node to be self.current
        let mut guard = self.current.write();
        self.tree_with_tracker
            .tree
            .checkout_current(commit_id, &mut guard)?;

        let current = guard.as_ref().unwrap();
        let mut result = HashMap::new();
        let start_bound = Bound::Included(lower_bound_incl);
        let end_bound = match &upper_bound_excl {
            Some(upper) => Bound::Excluded(upper.clone()),
            None => Bound::Unbounded,
        };
        for (key, apply_record) in current.range((start_bound, end_bound)) {
            result.insert(key.clone(), apply_record.value.clone());
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{primitives_initialize_empty_schema, primitives_verify_schema_is_empty};
    use std::sync::Arc;

    use crate::{
        backends::{DatabaseTrait, VersionedKVName, WrappedInMemoryDb},
        middlewares::versioned_flat_key_value::{
            pending_part::{pending_schema::PendingKeyValueConfig, tree::Tree},
            table_schema::VersionedKeyValueSchema,
        },
    };

    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use rand_distr::{Distribution, Uniform};

    pub type CommitId = u64;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

    fn initialize_empty_pending_db() -> (
        Arc<WrappedInMemoryDb<PendingTableName>>,
        TreeWithTracker<TestPendingConfig>,
    ) {
        let pending_db = Arc::new(WrappedInMemoryDb::empty());
        primitives_verify_schema_is_empty::<TestPendingConfig, WrappedInMemoryDb<PendingTableName>>(&pending_db).unwrap();
        let write_schema = WrappedInMemoryDb::write_schema();
        let tree_with_tracker =
            primitives_initialize_empty_schema::<TestPendingConfig>(&write_schema, None, 0);
        (pending_db, tree_with_tracker)
    }

    fn generate_random_tree(
        num_nodes: usize,
        rng: &mut StdRng,
    ) -> (
        Tree<TestPendingConfig>,
        VersionedMap<TestPendingConfig, WrappedInMemoryDb<PendingTableName>>,
    ) {
        let (db, tree_with_tracker) = initialize_empty_pending_db();

        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map = VersionedMap::from_initialized_state(tree_with_tracker);

        let write_schema = WrappedInMemoryDb::write_schema();
        for i in 1..=num_nodes as CommitId {
            let parent_commit_id = if i == 1 {
                None
            } else {
                Some(rng.gen_range(1..i))
            };
            let mut updates = HashMap::new();
            for _ in 0..5 {
                let (key, value) = random_key_value(rng);
                updates.insert(key, value);
            }
            let updates_none = updates
                .iter()
                .map(|(key, value)| {
                    (
                        *key,
                        RecoverRecord {
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
                .add_node(updates, i, parent_commit_id, &write_schema)
                .unwrap();
        }

        db.commit(write_schema).unwrap();

        (forward_only_tree, versioned_map)
    }

    #[test]
    fn test_export_and_from_snapshot_roundtrip() {
        let num_nodes = 30;

        let seed: [u8; 32] = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ];
        let mut rng = StdRng::from_seed(seed);

        let (forward_only_tree, _) = generate_random_tree(num_nodes, &mut rng);

        let tree_snapshot = forward_only_tree.export_snapshot();
        let tree_from_tree_snapshot = Tree::from_snapshot(tree_snapshot.clone()).unwrap();
        let tree_snapshot_second_hand = tree_from_tree_snapshot.export_snapshot();
        assert_eq!(tree_snapshot, tree_snapshot_second_hand);
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
                let answer = apply_map.get(&key).map(|a| a.value);
                assert_eq!(versioned_value, answer);
                assert_eq!(versioned_value_with_checkout, answer);
            }
        }
    }

    #[test]
    fn test_multiple_roots_err() {
        let (db, tree_with_tracker) = initialize_empty_pending_db();

        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map: VersionedMap<_, WrappedInMemoryDb<PendingTableName>> =
            VersionedMap::from_initialized_state(tree_with_tracker);

        forward_only_tree.add_root(0, HashMap::new()).unwrap();

        let write_schema = WrappedInMemoryDb::write_schema();
        versioned_map
            .add_node(HashMap::new(), 0, None, &write_schema)
            .unwrap();
        db.commit(write_schema).unwrap();

        let write_schema = WrappedInMemoryDb::write_schema();
        assert_eq!(
            forward_only_tree.add_root(1, HashMap::new()),
            Err(PendingError::MultipleRootsNotAllowed)
        );
        assert_eq!(
            versioned_map.add_node(HashMap::new(), 1, None, &write_schema),
            Err(PendingError::MultipleRootsNotAllowed)
        );
    }

    #[test]
    fn test_commit_id_not_found_err() {
        let (db, tree_with_tracker) = initialize_empty_pending_db();

        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map: VersionedMap<_, WrappedInMemoryDb<PendingTableName>> =
            VersionedMap::from_initialized_state(tree_with_tracker);

        assert_eq!(
            forward_only_tree.add_non_root_node(1, 0, HashMap::new()),
            Err(PendingError::CommitIDNotFound(format!("{:?}", 0)))
        );

        let write_schema = WrappedInMemoryDb::write_schema();
        assert_eq!(
            versioned_map.add_node(HashMap::new(), 1, Some(0), &write_schema),
            Err(PendingError::CommitIDNotFound(format!("{:?}", 0)))
        );
    }

    // Helper: build a VersionedMap, add nodes, optionally checkout a commit.
    // Returns the VersionedMap ready for get_versioned_key queries.
    fn build_versioned_map(
        nodes: Vec<(CommitId, Option<CommitId>, Vec<(u64, Option<u64>)>)>,
    ) -> VersionedMap<TestPendingConfig, WrappedInMemoryDb<PendingTableName>> {
        let (db, tree_with_tracker) = initialize_empty_pending_db();
        let mut versioned_map = VersionedMap::from_initialized_state(tree_with_tracker);
        let write_schema = WrappedInMemoryDb::write_schema();
        for (commit_id, parent, updates) in nodes {
            let updates: HashMap<u64, Option<u64>> = updates.into_iter().collect();
            versioned_map
                .add_node(updates, commit_id, parent, &write_schema)
                .unwrap();
        }
        db.commit(write_schema).unwrap();
        versioned_map
    }

    // Tree structure used in ancestor_cache tests:
    //
    //       1 (root)    key=1 => Some(100)
    //      / \
    //     2   3         commit 2: key=2 => Some(200), key=3 => deleted
    //     |             commit 3: key=1 => Some(300)
    //     4
    //     |             commit 4: key=4 => Some(400)
    //     5             commit 5: (no modifications)
    //
    fn build_ancestor_cache_test_tree(
    ) -> VersionedMap<TestPendingConfig, WrappedInMemoryDb<PendingTableName>> {
        build_versioned_map(vec![
            (1, None, vec![(1, Some(100))]),
            (2, Some(1), vec![(2, Some(200)), (3, None)]), // key=3 deleted
            (3, Some(1), vec![(1, Some(300))]),
            (4, Some(2), vec![(4, Some(400))]),
            (5, Some(4), vec![]),
        ])
    }

    #[test]
    fn test_ancestor_cache_key_modified_between_current_and_query() {
        // CurrentMap at commit 2, query commit 5.
        // Commit 2 is ancestor of 5 (path: 5->4->2).
        // key=4 is modified at commit 4 (between current and query).
        // Should find key=4 in the tree traversal before reaching CurrentMap.
        let vm = build_ancestor_cache_test_tree();
        vm.checkout_current(2).unwrap();

        let result = vm.get_versioned_key(&5, &4).unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(Some(400))));
    }

    #[test]
    fn test_ancestor_cache_key_in_current_not_modified_after() {
        // CurrentMap at commit 2, query commit 5.
        // key=2 was set at commit 2, never modified in commits 4 or 5.
        // Should stop at CurrentMap and return Some(Some(200)).
        let vm = build_ancestor_cache_test_tree();
        vm.checkout_current(2).unwrap();

        let result = vm.get_versioned_key(&5, &2).unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(Some(200))));
    }

    #[test]
    fn test_ancestor_cache_key_deleted_in_current() {
        // CurrentMap at commit 2, query commit 5.
        // key=3 was deleted at commit 2, never modified in commits 4 or 5.
        // Should stop at CurrentMap and return Some(None) — pending part knows it's deleted.
        let vm = build_ancestor_cache_test_tree();
        vm.checkout_current(2).unwrap();

        let result = vm.get_versioned_key(&5, &3).unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(None)));
    }

    #[test]
    fn test_ancestor_cache_key_not_in_current() {
        // CurrentMap at commit 2, query commit 5.
        // key=99 was never modified in any commit.
        // Should stop at CurrentMap and return None — pending part doesn't know.
        let vm = build_ancestor_cache_test_tree();
        vm.checkout_current(2).unwrap();

        let result = vm.get_versioned_key(&5, &99).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_ancestor_cache_current_on_different_branch() {
        // CurrentMap at commit 3 (branch), query commit 5 (path: 5->4->2->1).
        // Commit 3 is NOT an ancestor of commit 5.
        // Should fall through to full traversal without using the cache.
        // key=1 was set at commit 1, so it should be found at root.
        let vm = build_ancestor_cache_test_tree();
        vm.checkout_current(3).unwrap();

        let result = vm.get_versioned_key(&5, &1).unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(Some(100))));

        // key=2 was set at commit 2 (on the path to root), should be found.
        let result = vm.get_versioned_key(&5, &2).unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(Some(200))));

        // key=99 never modified anywhere, should return None.
        let result = vm.get_versioned_key(&5, &99).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_ancestor_cache_no_current_map() {
        // No checkout — CurrentMap is None.
        // Should fall through to full traversal.
        let vm = build_ancestor_cache_test_tree();

        let result = vm.get_versioned_key(&5, &4).unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(Some(400))));

        let result = vm.get_versioned_key(&5, &99).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_ancestor_cache_current_equals_query() {
        // CurrentMap at commit 5, query commit 5.
        // Should hit the existing fast path (exact match), not the ancestor path.
        let vm = build_ancestor_cache_test_tree();
        vm.checkout_current(5).unwrap();

        // key=4 was set at commit 4, visible at commit 5.
        let result = vm.get_versioned_key(&5, &4).unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(Some(400))));

        // key=99 never modified.
        let result = vm.get_versioned_key(&5, &99).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_ancestor_cache_current_is_root() {
        // CurrentMap at commit 1 (root), query commit 5.
        // Root is ancestor of everything.
        // key=1 was set at root, never modified on path 2->4->5.
        // Should stop at CurrentMap (root) and return the cached value.
        let vm = build_ancestor_cache_test_tree();
        vm.checkout_current(1).unwrap();

        let result = vm.get_versioned_key(&5, &1).unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(Some(100))));

        // key=99 not in CurrentMap => None (pending part doesn't know).
        let result = vm.get_versioned_key(&5, &99).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_ancestor_cache_consistency_with_full_traversal() {
        // Comprehensive: for every (current_commit, query_commit, key) combination,
        // verify the ancestor_cache optimization returns the same result
        // as a fresh query without any CurrentMap.
        let num_nodes = 30;
        let seed: [u8; 32] = [
            42, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
            23, 24, 25, 26, 27, 28, 29, 30, 31,
        ];
        let mut rng = StdRng::from_seed(seed);

        let (_, versioned_map) = generate_random_tree(num_nodes, &mut rng);

        // Collect baseline results without CurrentMap
        let mut baseline = HashMap::new();
        for commit_id in 1..=num_nodes as CommitId {
            for key in 0..10u64 {
                let result = versioned_map.get_versioned_key(&commit_id, &key).unwrap();
                baseline.insert((commit_id, key), result);
            }
        }

        // Now checkout various commits and verify results still match
        for current_commit in 1..=num_nodes as CommitId {
            versioned_map.checkout_current(current_commit).unwrap();
            for query_commit in 1..=num_nodes as CommitId {
                for key in 0..10u64 {
                    let result = versioned_map
                        .get_versioned_key(&query_commit, &key)
                        .unwrap();
                    assert_eq!(
                        result,
                        baseline[&(query_commit, key)],
                        "mismatch: current={}, query={}, key={}",
                        current_commit,
                        query_commit,
                        key
                    );
                }
            }
        }
    }

    #[test]
    fn test_ancestor_cache_safe_after_current_checked_out() {
        // Simulates the scenario where another thread checks out CurrentMap
        // between the cache copy and the tree traversal.
        //
        // Timeline:
        //   1. Thread A: reads CurrentMap at commit 2, copies cache (commit_id=2, key=2 => Some(200))
        //   2. Thread B: checks out CurrentMap to commit 3 (different branch)
        //   3. Thread A: uses the stale cache to query Tree::get_versioned_key(commit=5, key=2)
        //
        // The stale cache must still produce the correct result, because the cached
        // data is a snapshot of an immutable commit state.

        let vm = build_ancestor_cache_test_tree();

        // Step 1: checkout to commit 2, snapshot the cache as get_versioned_key would
        vm.checkout_current(2).unwrap();
        let stale_cache = {
            let guard = vm.current.read();
            let current = guard.as_ref().unwrap();
            assert_eq!(current.get_commit_id(), 2);
            let cached_value = current.get(&2u64).map(|c| c.value.clone());
            (current.get_commit_id(), cached_value)
        };

        // Step 2: another thread checks out CurrentMap to commit 3
        vm.checkout_current(3).unwrap();
        // Verify CurrentMap has indeed moved away
        assert_eq!(vm.current.read().as_ref().unwrap().get_commit_id(), 3);

        // Step 3: use the stale cache (from commit 2) directly on the tree
        // key=2 was set at commit 2, which is on the path 5->4->2->1.
        // The stale cache should correctly short-circuit at commit 2.
        let result = vm
            .tree_with_tracker
            .tree
            .get_versioned_key(&5, &2, Some(&stale_cache))
            .unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(Some(200))));

        // Also test the three-level return semantics with stale cache:

        // key=3 deleted at commit 2 => Some(None)
        let stale_cache_deleted = {
            vm.checkout_current(2).unwrap();
            let guard = vm.current.read();
            let current = guard.as_ref().unwrap();
            let cached_value = current.get(&3u64).map(|c| c.value.clone());
            (current.get_commit_id(), cached_value)
        };
        vm.checkout_current(3).unwrap(); // move CurrentMap away again
        let result = vm
            .tree_with_tracker
            .tree
            .get_versioned_key(&5, &3, Some(&stale_cache_deleted))
            .unwrap();
        assert_eq!(result, Some(ValueEntry::from_option(None)));

        // key=99 never modified => None (pending part doesn't know)
        let stale_cache_unknown = {
            vm.checkout_current(2).unwrap();
            let guard = vm.current.read();
            let current = guard.as_ref().unwrap();
            let cached_value = current.get(&99u64).map(|c| c.value.clone());
            (current.get_commit_id(), cached_value)
        };
        vm.checkout_current(3).unwrap();
        let result = vm
            .tree_with_tracker
            .tree
            .get_versioned_key(&5, &99, Some(&stale_cache_unknown))
            .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_commit_id_already_exists_err() {
        let (db, tree_with_tracker) = initialize_empty_pending_db();

        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map: VersionedMap<_, WrappedInMemoryDb<PendingTableName>> =
            VersionedMap::from_initialized_state(tree_with_tracker);

        forward_only_tree.add_root(0, HashMap::new()).unwrap();

        let write_schema = WrappedInMemoryDb::write_schema();
        versioned_map
            .add_node(HashMap::new(), 0, None, &write_schema)
            .unwrap();
        db.commit(write_schema).unwrap();

        assert_eq!(
            forward_only_tree.add_non_root_node(0, 0, HashMap::new()),
            Err(PendingError::CommitIdAlreadyExists(format!("{:?}", 0)))
        );

        let write_schema = WrappedInMemoryDb::write_schema();
        assert_eq!(
            versioned_map.add_node(HashMap::new(), 0, Some(0), &write_schema),
            Err(PendingError::CommitIdAlreadyExists(format!("{:?}", 0)))
        );
    }
}
