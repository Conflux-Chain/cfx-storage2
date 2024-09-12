use std::collections::{BTreeMap, HashMap};

use super::{
    commit_tree::Tree,
    pending_schema::{CIdOptValue, OptValue, PendResult, PendingKeyValueSchema, ToCommit},
    PendingError,
};

pub struct VersionedHashMap<S: PendingKeyValueSchema> {
    parent_of_root: Option<S::CommitId>,

    history: HashMap<S::CommitId, HashMap<S::Key, OptValue<S>>>,
    tree: Tree<S>,

    current: BTreeMap<S::Key, CIdOptValue<S>>,
    current_node: Option<S::CommitId>,
}

impl<S: PendingKeyValueSchema> VersionedHashMap<S> {
    pub fn new(parent_of_root: Option<S::CommitId>) -> Self {
        VersionedHashMap {
            parent_of_root,
            current: BTreeMap::new(),
            history: HashMap::new(),
            tree: Tree::new(),
            current_node: None,
        }
    }
}

impl<S: PendingKeyValueSchema> VersionedHashMap<S> {
    pub fn change_root(&mut self, commit_id: S::CommitId) -> PendResult<ToCommit<S>, S> {
        let (to_commit_rev, to_remove) = self.tree.change_root(commit_id)?;
        if to_commit_rev.is_empty() {
            assert!(to_remove.is_empty());
            return Ok(Vec::new());
        }
        self.parent_of_root = Some(to_commit_rev[0]);
        self.current.clear();
        self.current_node = None;
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
        updates: BTreeMap<S::Key, Option<S::Value>>,
        commit_id: S::CommitId,
        parent_commit_id: Option<S::CommitId>,
    ) -> PendResult<(), S> {
        if self.history.contains_key(&commit_id) {
            return Err(PendingError::CommitIdAlreadyExists(commit_id));
        }
        // let parent to be self.current
        self.walk_to_node(parent_commit_id)?;
        assert_eq!(parent_commit_id, self.current_node);
        // add node
        let mut modifications = Vec::new();
        let mut inner_map = HashMap::new();
        for (key, value) in updates.into_iter() {
            inner_map.insert(key.clone(), value.clone());
            let old_commit_id = if let Some((old_commit_id, _)) =
                self.current.insert(key.clone(), (commit_id, value.clone()))
            {
                Some(old_commit_id)
            } else {
                None
            };
            modifications.push((key, value, old_commit_id));
        }
        self.history.insert(commit_id, inner_map);
        self.tree
            .add_node(commit_id, parent_commit_id, modifications)?;
        self.current_node = Some(commit_id);
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
        // let queried node to be self.current
        self.walk_to_node_unchecked(commit_id)?;
        assert_eq!(Some(commit_id), self.current_node);
        // query
        let value = self.current.get(key).map(|(_, value)| value.clone());
        Ok(value)
    }

    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.parent_of_root
    }
}

impl<S: PendingKeyValueSchema> VersionedHashMap<S> {
    fn walk_to_node(&mut self, target_commit_id: Option<S::CommitId>) -> PendResult<(), S> {
        if let Some(target_commit_id) = target_commit_id {
            self.walk_to_node_unchecked(target_commit_id)?;
        } else {
            self.current = BTreeMap::new();
            self.current_node = None;
        }
        Ok(())
    }
    fn walk_to_node_unchecked(&mut self, target_commit_id: S::CommitId) -> PendResult<(), S> {
        let (rollbacks, commits_rev) = if let Some(current_commit_id) = self.current_node {
            self.tree.lca(current_commit_id, target_commit_id)?
        } else {
            (HashMap::new(), self.tree.find_path(target_commit_id)?)
        };
        self.rollback_without_node_update(rollbacks);
        self.commit_without_node_update(commits_rev);
        self.current_node = Some(target_commit_id);
        Ok(())
    }

    fn commit_without_node_update(&mut self, commits_rev: HashMap<S::Key, CIdOptValue<S>>) {
        for (key, (commit_id, value)) in commits_rev.into_iter() {
            self.current.insert(key, (commit_id, value));
        }
    }

    fn rollback_without_node_update(&mut self, rollbacks: HashMap<S::Key, Option<S::CommitId>>) {
        for (key, old_commit_id) in rollbacks.into_iter() {
            match old_commit_id {
                None => {
                    self.current.remove(&key);
                }
                Some(old_commit_id) => {
                    let value = self.history.get(&old_commit_id).unwrap().get(&key).unwrap();
                    self.current.insert(key, (old_commit_id, value.clone()));
                }
            }
        }
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
                .map(|(key, value)| (key.clone(), value.clone(), None))
                .collect();

            forward_only_tree
                .add_node(i, parent_commit_id, updates_none)
                .unwrap();
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
                let current = forward_only_tree.find_path(commit_id).unwrap();
                let answer = current.get(&key).and_then(|(_, value)| Some(value.clone()));
                assert_eq!(result, answer);
            }
        }
    }

    #[test]
    fn test_multiple_roots_err() {
        let mut forward_only_tree = Tree::<TestPendingConfig>::new();
        let mut versioned_hash_map = VersionedHashMap::<TestPendingConfig>::new(None);

        forward_only_tree.add_node(0, None, Vec::new()).unwrap();
        versioned_hash_map
            .add_node(BTreeMap::new(), 0, None)
            .unwrap();

        assert_eq!(
            forward_only_tree.add_node(1, None, Vec::new()),
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
            forward_only_tree.add_node(1, Some(0), Vec::new()),
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

        forward_only_tree.add_node(0, None, Vec::new()).unwrap();
        versioned_hash_map
            .add_node(BTreeMap::new(), 0, None)
            .unwrap();

        assert_eq!(
            forward_only_tree.add_node(0, Some(0), Vec::new()),
            Err(PendingError::CommitIdAlreadyExists(0))
        );
        assert_eq!(
            versioned_hash_map.add_node(BTreeMap::new(), 0, Some(0)),
            Err(PendingError::CommitIdAlreadyExists(0))
        );
    }
}
