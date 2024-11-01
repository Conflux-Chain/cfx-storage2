use std::collections::BTreeMap;

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

    fn get_current_commit_id(&self) -> Option<S::CommitId> {
        self.current.read().as_ref().map(|c| c.get_commit_id())
    }
}

// checkout
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    fn checkout_current(&self, target_commit_id: S::CommitId) -> PendResult<(), S> {
        let mut current_option = self.current.write();
        self.tree
            .checkout_current(target_commit_id, &mut current_option)
    }
}

// add_node
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    pub fn add_node(
        &mut self,
        updates: KeyValueMap<S>,
        commit_id: S::CommitId,
        parent_commit_id: Option<S::CommitId>,
    ) -> PendResult<(), S> {
        if self.get_parent_of_root() == parent_commit_id {
            self.add_root(updates, commit_id)
        } else if let Some(parent_commit_id) = parent_commit_id {
            self.add_non_root_node(updates, commit_id, parent_commit_id)
        } else {
            Err(PendingError::NonRootNodeShouldHaveParent)
        }
    }

    fn add_root(&mut self, updates: KeyValueMap<S>, commit_id: S::CommitId) -> PendResult<(), S> {
        // add root to tree
        let modifications = updates
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    RecoverRecord::<S> {
                        value,
                        last_commit_id: None,
                    },
                )
            })
            .collect();
        self.tree.add_root(commit_id, modifications)?;

        Ok(())
    }

    fn add_non_root_node(
        &mut self,
        updates: KeyValueMap<S>,
        commit_id: S::CommitId,
        parent_commit_id: S::CommitId,
    ) -> PendResult<(), S> {
        // let parent to be self.current
        // this step is necessary for computing modifications' last_commit_id
        self.checkout_current(parent_commit_id)?;

        // add node to tree
        let current_read = self.current.read();
        let current = current_read.as_ref().unwrap();
        let mut modifications = BTreeMap::new();
        for (key, value) in updates.into_iter() {
            let last_commit_id = current.get_map().get(&key).map(|s| s.commit_id);
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
            *self.current.write() = None;
        }

        Ok((start_height_to_commit, to_commit))
    }
}

// Helper methods in pending part to support
// impl KeyValueStoreManager for VersionedStore
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    pub fn iter_historical_changes<'a>(
        &'a self,
        commit_id: &S::CommitId,
        key: &'a S::Key,
    ) -> PendResult<impl 'a + Iterator<Item = (S::CommitId, &S::Key, Option<S::Value>)>, S> {
        self.tree.iter_historical_changes(commit_id, key)
    }

    // None: pending_part not know
    // Some(None): pending_part know that this key has been deleted
    // Some(Some(value)): pending_part know this key's value
    pub fn get_versioned_key(
        &self,
        commit_id: &S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<Option<S::Value>>, S> {
        self.tree.get_versioned_key(commit_id, key)
    }

    // alternative method of self.get_versioned_key(),
    // but it invokes self.checkout_current(),
    // thus is only suitable for frequent commit_id
    pub fn get_versioned_key_with_checkout(
        &self,
        commit_id: S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<Option<S::Value>>, S> {
        // let query node to be self.current
        self.checkout_current(commit_id)?;

        // query
        Ok(self
            .current
            .read()
            .as_ref()
            .unwrap()
            .get_map()
            .get(key)
            .map(|c| c.value.clone()))
    }

    pub fn discard(&mut self, commit_id: S::CommitId) -> PendResult<(), S> {
        self.tree.discard(commit_id)?;
        if let Some(current_commit_id) = self.get_current_commit_id() {
            if !self.tree.contains_commit_id(&current_commit_id) {
                *self.current.write() = None;
            }
        }
        Ok(())
    }

    pub fn get_versioned_store(&self, commit_id: S::CommitId) -> PendResult<KeyValueMap<S>, S> {
        // let query node to be self.current
        self.checkout_current(commit_id)?;
        let current_read = self.current.read();
        let map: BTreeMap<_, _> = current_read
            .as_ref()
            .unwrap()
            .get_map()
            .iter()
            .map(|(k, apply_record)| (k.clone(), apply_record.value.clone()))
            .collect();
        Ok(map)
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
                        RecoverRecord {
                            value: *value,
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
