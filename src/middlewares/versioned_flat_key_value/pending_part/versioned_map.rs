use std::collections::HashMap;

use crate::traits::{IsCompleted, NeedNext};
use crate::types::ValueEntry;

use super::pending_schema::ConfirmedPathInfo;
use super::{
    current_map::CurrentMap,
    pending_schema::{KeyValueMap, PendingKeyValueSchema, RecoverRecord, Result as PendResult},
    tree::Tree,
    PendingError,
};

use parking_lot::RwLock;

pub struct VersionedMap<S: PendingKeyValueSchema> {
    tree: Tree<S>,
    current: RwLock<Option<CurrentMap<S>>>,
}

impl<S: PendingKeyValueSchema> VersionedMap<S> {
    pub fn new(parent_of_root: Option<S::CommitId>, height_of_root: usize) -> Self {
        VersionedMap {
            tree: Tree::new(parent_of_root, height_of_root),
            current: RwLock::new(None),
        }
    }

    pub fn new_empty() -> Self {
        Self::new(None, 0)
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

    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.tree.get_parent_of_root()
    }
}

// add_node
impl<S: PendingKeyValueSchema> VersionedMap<S> {
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

    fn add_root(
        &mut self,
        updates: impl Iterator<Item = (S::Key, ValueEntry<S::Value>)>,
        commit_id: S::CommitId,
    ) -> PendResult<(), S> {
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
        self.tree.add_root(commit_id, modifications)?;

        Ok(())
    }

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
        self.tree
            .add_non_root_node(commit_id, parent_commit_id, modifications)?;

        Ok(())
    }
}

// change_root
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    pub fn change_root(&mut self, commit_id: S::CommitId) -> PendResult<ConfirmedPathInfo<S>, S> {
        let confirm_path_info = self.tree.change_root(commit_id)?;

        if confirm_path_info.commit_ids.last().is_some() {
            // clear current is necessary
            // because apply_commit_id in current.map may be removed from pending part
            self.clear_removed_current();
            if let Some(current) = self.current.get_mut() {
                current.update_rerooted(&self.tree);
            }
        }

        Ok(confirm_path_info)
    }

    pub fn remove_root(&mut self, commit_id: S::CommitId) -> PendResult<ConfirmedPathInfo<S>, S> {
        let confirm_path_info = self.tree.remove_root(commit_id)?;

        if confirm_path_info.commit_ids.last().is_some() {
            // clear current is necessary
            // because apply_commit_id in current.map may be removed from pending part
            self.clear_removed_current();
            if let Some(current) = self.current.get_mut() {
                current.update_rerooted(&self.tree);
            }
        }

        Ok(confirm_path_info)
    }
}

// Helper methods in pending part to support
// impl KeyValueStoreManager for VersionedStore
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    pub fn iter_historical_changes(
        &self,
        mut accept: impl FnMut(&S::CommitId, &S::Key, Option<&S::Value>) -> NeedNext,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<IsCompleted, S> {
        self.tree
            .iter_historical_changes(&mut accept, commit_id, key)
    }

    // None: pending_part not know
    // Some(None): pending_part know that this key has been deleted
    // Some(Some(value)): pending_part know this key's value
    pub fn get_versioned_key(
        &self,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<ValueEntry<S::Value>>, S> {
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

        self.tree
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
    ) -> PendResult<Option<ValueEntry<S::Value>>, S> {
        // let query node to be self.current
        let mut guard = self.current.write();
        self.tree.checkout_current(commit_id, &mut guard)?;

        // Safety of unwrap: guard is set to be Some if it was None in self.tree.checkout_current.
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

    pub fn contains_commit_id(&self, commit_id: &S::CommitId) -> bool {
        self.tree.contains_commit_id(commit_id)
    }

    pub fn checkout_current(&self, commit_id: S::CommitId) -> PendResult<(), S> {
        // let query node to be self.current
        let mut guard = self.current.write();
        self.tree.checkout_current(commit_id, &mut guard)?;

        Ok(())
    }

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
                let answer = apply_map.get(&key).map(|a| a.value);
                assert_eq!(versioned_value, answer);
                assert_eq!(versioned_value_with_checkout, answer);
            }
        }
    }

    #[test]
    fn test_multiple_roots_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map = VersionedMap::<TestPendingConfig>::new(None, 0);

        forward_only_tree.add_root(0, HashMap::new()).unwrap();
        versioned_map.add_node(HashMap::new(), 0, None).unwrap();

        assert_eq!(
            forward_only_tree.add_root(1, HashMap::new()),
            Err(PendingError::MultipleRootsNotAllowed)
        );
        assert_eq!(
            versioned_map.add_node(HashMap::new(), 1, None),
            Err(PendingError::MultipleRootsNotAllowed)
        );
    }

    #[test]
    fn test_commit_id_not_found_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map = VersionedMap::<TestPendingConfig>::new(None, 0);

        assert_eq!(
            forward_only_tree.add_non_root_node(1, 0, HashMap::new()),
            Err(PendingError::CommitIDNotFound(0))
        );
        assert_eq!(
            versioned_map.add_node(HashMap::new(), 1, Some(0)),
            Err(PendingError::CommitIDNotFound(0))
        );
    }

    // Helper: build a VersionedMap, add nodes, optionally checkout a commit.
    // Returns the VersionedMap ready for get_versioned_key queries.
    fn build_versioned_map(
        nodes: Vec<(CommitId, Option<CommitId>, Vec<(u64, Option<u64>)>)>,
    ) -> VersionedMap<TestPendingConfig> {
        let mut versioned_map = VersionedMap::new(None, 0);
        for (commit_id, parent, updates) in nodes {
            let updates: HashMap<u64, Option<u64>> = updates.into_iter().collect();
            versioned_map.add_node(updates, commit_id, parent).unwrap();
        }
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
    fn build_ancestor_cache_test_tree() -> VersionedMap<TestPendingConfig> {
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
            .tree
            .get_versioned_key(&5, &99, Some(&stale_cache_unknown))
            .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_commit_id_already_exists_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new(None, 0);
        let mut versioned_map = VersionedMap::<TestPendingConfig>::new(None, 0);

        forward_only_tree.add_root(0, HashMap::new()).unwrap();
        versioned_map.add_node(HashMap::new(), 0, None).unwrap();

        assert_eq!(
            forward_only_tree.add_non_root_node(0, 0, HashMap::new()),
            Err(PendingError::CommitIdAlreadyExists(0))
        );
        assert_eq!(
            versioned_map.add_node(HashMap::new(), 0, Some(0)),
            Err(PendingError::CommitIdAlreadyExists(0))
        );
    }
}
