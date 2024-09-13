use std::collections::{BTreeMap, HashMap};

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::RecoverRecord;

use super::{
    commit_tree::Tree,
    pending_schema::{
        ApplyMap, ApplyRecord, KeyValueMap, PendingKeyValueSchema, Result as PendResult,
    },
    PendingError,
};

pub struct VersionedHashMap<S: PendingKeyValueSchema> {
    parent_of_root: Option<S::CommitId>,
    tree: Tree<S>,
    history: HashMap<S::CommitId, KeyValueMap<S>>,
    current: Option<CurrentMap<S>>,
}

pub struct CurrentMap<S: PendingKeyValueSchema> {
    pub map: BTreeMap<S::Key, ApplyRecord<S>>,
    pub commit_id: S::CommitId,
}

impl<S: PendingKeyValueSchema> CurrentMap<S> {
    pub fn new(commit_id: S::CommitId) -> Self {
        Self {
            map: BTreeMap::new(),
            commit_id,
        }
    }

    fn rollback(
        &mut self,
        rollbacks: BTreeMap<S::Key, Option<S::CommitId>>,
        history: &HashMap<S::CommitId, KeyValueMap<S>>,
    ) {
        for (key, to_commit_id) in rollbacks.into_iter() {
            match to_commit_id {
                None => {
                    self.map.remove(&key);
                }
                Some(commit_id) => {
                    let value = history.get(&commit_id).unwrap().get(&key).unwrap().clone();
                    self.map.insert(key, ApplyRecord { commit_id, value });
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

impl<S: PendingKeyValueSchema> VersionedHashMap<S> {
    pub fn new(parent_of_root: Option<S::CommitId>) -> Self {
        VersionedHashMap {
            parent_of_root,
            history: HashMap::new(),
            tree: Tree::new(),
            current: None,
        }
    }
}

impl<S: PendingKeyValueSchema> VersionedHashMap<S> {
    #[allow(clippy::type_complexity)]
    pub fn change_root(
        &mut self,
        commit_id: S::CommitId,
    ) -> PendResult<Vec<(S::CommitId, Option<KeyValueMap<S>>)>, S> {
        let (to_commit_rev, to_remove) = self.tree.change_root(commit_id)?;
        if to_commit_rev.is_empty() {
            assert!(to_remove.is_empty());
            return Ok(Vec::new());
        }
        self.parent_of_root = Some(to_commit_rev[0]);
        self.current = None;
        for to_remove_one in to_remove.into_iter() {
            self.history.remove(&to_remove_one);
        }
        let mut to_commit = Vec::new();
        for to_commit_one in to_commit_rev.into_iter().rev() {
            to_commit.push((to_commit_one, self.history.remove(&to_commit_one)));
        }
        Ok(to_commit)
    }
}

impl<S: PendingKeyValueSchema> VersionedHashMap<S> {
    pub fn add_node(
        &mut self,
        updates: KeyValueMap<S>,
        commit_id: S::CommitId,
        parent_commit_id: Option<S::CommitId>,
    ) -> PendResult<(), S> {
        if self.history.contains_key(&commit_id) {
            return Err(PendingError::CommitIdAlreadyExists(commit_id));
        }

        if self.get_parent_of_root() == parent_commit_id {
            self.add_root(updates, commit_id)
        } else if let Some(parent_commit_id) = parent_commit_id {
            self.add_non_root_node(updates, commit_id, parent_commit_id)
        } else {
            Err(PendingError::NonRootNodeHasNoParentError)
        }
    }

    fn add_root(&mut self, updates: KeyValueMap<S>, commit_id: S::CommitId) -> PendResult<(), S> {
        if self.current.is_some() || !self.history.is_empty() {
            return Err(PendingError::MultipleRootsNotAllowed);
        }

        // add root to tree
        let modifications = updates
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    RecoverRecord::<S> {
                        value: v.clone(),
                        last_commit_id: None,
                    },
                )
            })
            .collect();
        self.tree.add_root(commit_id, modifications)?;

        // add root to history
        self.history.insert(commit_id, updates);

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
        let current = self.current.as_ref().unwrap();
        let mut modifications = BTreeMap::new();
        for (key, value) in updates.iter() {
            let last_commit_id = current.map.get(key).map(|s| s.commit_id);
            modifications.insert(
                key.clone(),
                RecoverRecord {
                    value: value.clone(),
                    last_commit_id,
                },
            );
        }
        self.tree
            .add_non_root_node(commit_id, parent_commit_id, modifications)?;

        // add node to history
        self.history.insert(commit_id, updates);

        Ok(())
    }

    // None: pending_part not know
    // Some(None): pending_part know that this key has been deleted
    // Some(Some(value)): pending_part know this key's value
    pub fn query(
        &mut self,
        commit_id: S::CommitId,
        key: &S::Key,
    ) -> PendResult<Option<Option<S::Value>>, S> {
        // let query node to be self.current
        self.checkout_current(commit_id)?;

        // query
        Ok(self
            .current
            .as_ref()
            .unwrap()
            .map
            .get(key)
            .map(|c| c.value.clone()))
    }

    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.parent_of_root
    }
}

impl<S: PendingKeyValueSchema> VersionedHashMap<S> {
    fn checkout_current(&mut self, target_commit_id: S::CommitId) -> PendResult<(), S> {
        if let Some(ref mut current) = self.current {
            let (rollbacks, applys) = self
                .tree
                .collect_rollback_and_apply_ops(current.commit_id, target_commit_id)?;
            current.rollback(rollbacks, &self.history);
            current.apply(applys);
            current.commit_id = target_commit_id;
        } else {
            let mut current = CurrentMap::<S>::new(target_commit_id);
            let applys = self.tree.get_apply_map_from_root(target_commit_id)?;
            current.apply(applys);
            self.current = Some(current)
        };

        assert_eq!(self.current.as_ref().unwrap().commit_id, target_commit_id);

        Ok(())
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
    ) -> (Tree<TestPendingConfig>, VersionedHashMap<TestPendingConfig>) {
        let mut forward_only_tree = Tree::new();
        let mut versioned_hash_map = VersionedHashMap::new(None);

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
            versioned_hash_map
                .add_node(updates, i, parent_commit_id)
                .unwrap();
        }
        (forward_only_tree, versioned_hash_map)
    }

    #[test]
    fn test_query() {
        let num_nodes = 100;

        let seed: [u8; 32] = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ];
        let mut rng = StdRng::from_seed(seed);

        let (forward_only_tree, mut versioned_hash_map) = generate_random_tree(num_nodes, &mut rng);
        for _ in 0..100 {
            let commit_id = rng.gen_range(1..=num_nodes) as CommitId;
            for ikey in 0..10 {
                let key: u64 = ikey;
                let result = versioned_hash_map.query(commit_id, &key).unwrap();
                let current = forward_only_tree
                    .get_apply_map_from_root(commit_id)
                    .unwrap();
                let answer = current.get(&key).map(|a| a.value.clone());
                assert_eq!(result, answer);
            }
        }
    }

    #[test]
    fn test_multiple_roots_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new();
        let mut versioned_hash_map = VersionedHashMap::<TestPendingConfig>::new(None);

        forward_only_tree.add_root(0, BTreeMap::new()).unwrap();
        versioned_hash_map
            .add_node(BTreeMap::new(), 0, None)
            .unwrap();

        assert_eq!(
            forward_only_tree.add_root(1, BTreeMap::new()),
            Err(PendingError::MultipleRootsNotAllowed)
        );
        assert_eq!(
            versioned_hash_map.add_node(BTreeMap::new(), 1, None),
            Err(PendingError::MultipleRootsNotAllowed)
        );
    }

    #[test]
    fn test_commit_id_not_found_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new();
        let mut versioned_hash_map = VersionedHashMap::<TestPendingConfig>::new(None);

        assert_eq!(
            forward_only_tree.add_non_root_node(1, 0, BTreeMap::new()),
            Err(PendingError::CommitIDNotFound(0))
        );
        assert_eq!(
            versioned_hash_map.add_node(BTreeMap::new(), 1, Some(0)),
            Err(PendingError::CommitIDNotFound(0))
        );
    }

    #[test]
    fn test_commit_id_already_exists_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new();
        let mut versioned_hash_map = VersionedHashMap::<TestPendingConfig>::new(None);

        forward_only_tree.add_root(0, BTreeMap::new()).unwrap();
        versioned_hash_map
            .add_node(BTreeMap::new(), 0, None)
            .unwrap();

        assert_eq!(
            forward_only_tree.add_non_root_node(0, 0, BTreeMap::new()),
            Err(PendingError::CommitIdAlreadyExists(0))
        );
        assert_eq!(
            versioned_hash_map.add_node(BTreeMap::new(), 0, Some(0)),
            Err(PendingError::CommitIdAlreadyExists(0))
        );
    }
}
