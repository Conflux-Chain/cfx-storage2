use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
};

use crate::{commit_tree::Tree, TreeError};

pub struct VersionedHashMap<
    Key: Eq + Hash + Clone + Ord,
    CommitId: Debug + Eq + Hash + Copy,
    Value: Clone,
> {
    history: HashMap<(Key, CommitId), Option<Value>>,
    tree: Tree<Key, CommitId, Value>,

    current: BTreeMap<Key, (CommitId, Option<Value>)>,
    current_node: Option<CommitId>,
}

impl<Key: Eq + Hash + Clone + Ord, CommitId: Debug + Eq + Hash + Copy, Value: Clone>
    VersionedHashMap<Key, CommitId, Value>
{
    pub fn new() -> Self {
        VersionedHashMap {
            current: BTreeMap::new(),
            history: HashMap::new(),
            tree: Tree::new(),
            current_node: None,
        }
    }
}

impl<Key: Eq + Hash + Clone + Ord, CommitId: Debug + Eq + Hash + Copy, Value: Clone>
    VersionedHashMap<Key, CommitId, Value>
{
    pub fn add_node(
        &mut self,
        updates: Vec<(Key, Option<Value>)>,
        commit_id: CommitId,
        parent_commit_id: Option<CommitId>,
    ) -> Result<(), TreeError<CommitId>> {
        // let parent to be self.current
        self.walk_to_node(parent_commit_id)?;
        assert_eq!(parent_commit_id, self.current_node);
        // add node
        let mut modifications = Vec::new();
        for (key, value) in updates.into_iter() {
            self.history.insert((key.clone(), commit_id), value.clone());
            let old_commit_id = {
                if let Some((old_commit_id, _)) =
                    self.current.insert(key.clone(), (commit_id, value.clone()))
                {
                    Some(old_commit_id)
                } else {
                    None
                }
            };
            modifications.push((key, value, old_commit_id));
        }
        self.tree
            .add_node(commit_id, parent_commit_id, modifications)?;
        Ok(())
    }

    pub fn query(
        &mut self,
        commit_id: CommitId,
        key: &Key,
    ) -> Result<Option<Value>, TreeError<CommitId>> {
        // let queried node to be self.current
        self.walk_to_node_unchecked(commit_id)?;
        assert_eq!(Some(commit_id), self.current_node);
        // query
        let value = self
            .current
            .get(key)
            .and_then(|(_, value)| Some(value.clone()));
        if let Some(Some(value_inner)) = value {
            Ok(Some(value_inner))
        } else {
            Ok(None)
        }
    }

    pub fn query_range<'a>(
        &'a mut self,
        commit_id: CommitId,
        key: Key,
    ) -> Result<Box<dyn Iterator<Item = (&Key, &Value)> + 'a>, TreeError<CommitId>> {
        // let queried node to be self.current
        self.walk_to_node_unchecked(commit_id)?;
        assert_eq!(Some(commit_id), self.current_node);
        // query
        let range = self
            .current
            .range(key..)
            .filter_map(|(key, (_, opt_value))| {
                if let Some(value) = opt_value {
                    Some((key, value))
                } else {
                    None
                }
            });
        Ok(Box::new(range))
    }
}

impl<Key: Eq + Hash + Clone + Ord, CommitId: Debug + Eq + Hash + Copy, Value: Clone>
    VersionedHashMap<Key, CommitId, Value>
{
    fn walk_to_node(
        &mut self,
        target_commit_id: Option<CommitId>,
    ) -> Result<(), TreeError<CommitId>> {
        if target_commit_id.is_none() {
            self.current = BTreeMap::new();
            self.current_node = None;
        } else {
            self.walk_to_node_unchecked(target_commit_id.unwrap())?;
        }
        Ok(())
    }
    fn walk_to_node_unchecked(
        &mut self,
        target_commit_id: CommitId,
    ) -> Result<(), TreeError<CommitId>> {
        let (rollbacks, commits_rev) = if self.current_node.is_none() {
            let commits_rev = self.tree.find_path(target_commit_id)?;
            (HashMap::new(), commits_rev)
        } else {
            self.tree
                .lca(self.current_node.unwrap(), target_commit_id)?
        };
        self.rollback_without_node_update(rollbacks);
        self.commit_without_node_update(commits_rev);
        self.current_node = Some(target_commit_id);
        Ok(())
    }

    fn commit_without_node_update(&mut self, commits_rev: HashMap<Key, (CommitId, Option<Value>)>) {
        for (key, (commit_id, value)) in commits_rev.into_iter() {
            self.current.insert(key, (commit_id, value));
        }
    }

    fn rollback_without_node_update(&mut self, rollbacks: HashMap<Key, Option<CommitId>>) {
        for (key, old_commit_id) in rollbacks.into_iter() {
            match old_commit_id {
                None => {
                    self.current.remove(&key);
                }
                Some(old_commit_id) => {
                    if let Some(value) = self.history.get(&(key.clone(), old_commit_id)) {
                        self.current
                            .insert(key.clone(), (old_commit_id, value.clone()));
                    } else {
                        unreachable!(
                            "A modification recorded in a rollbacked commit is absent in history"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::ThreadRng, Rng};
    use rand_distr::{Distribution, Uniform};

    pub type Key = Vec<u8>;
    pub type Value = Vec<u8>;
    pub type CommitId = u64;

    fn random_key_value(rng: &mut ThreadRng) -> (Key, Option<Value>) {
        // use a small key range to achieve more key conflicts
        let key_range = Uniform::from(0..10);
        let key: Key = vec![key_range.sample(rng)];

        let value: Option<Value> = if rng.gen_range(0..2) == 0 {
            Some(vec![rng.gen::<u8>()])
        } else {
            None
        };

        (key, value)
    }

    fn generate_random_tree(
        num_nodes: usize,
        rng: &mut ThreadRng,
    ) -> (
        Tree<Key, CommitId, Value>,
        VersionedHashMap<Key, CommitId, Value>,
    ) {
        let mut forward_only_tree = Tree::new();
        let mut versioned_hash_map = VersionedHashMap::new();

        for i in 1..=num_nodes as CommitId {
            let parent_commit_id = if i == 1 {
                None
            } else {
                Some(rng.gen_range(1..i))
            };
            let mut updates = Vec::new();
            for _ in 0..5 {
                updates.push(random_key_value(rng));
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
        let num_nodes = 10;
        let mut rng = rand::thread_rng();
        let (forward_only_tree, mut versioned_hash_map) = generate_random_tree(num_nodes, &mut rng);
        for _ in 0..100 {
            let commit_id = rng.gen_range(1..=num_nodes) as CommitId;
            for ikey in 0..10 {
                let key: Key = ikey.to_string().into_bytes();
                let result = versioned_hash_map.query(commit_id, &key).unwrap();
                let current = forward_only_tree.find_path(commit_id).unwrap();
                let answer = current.get(&key).and_then(|(_, value)| {
                    if value.is_some() {
                        Some(value.clone().unwrap())
                    } else {
                        None
                    }
                });
                assert_eq!(result, answer);
            }
        }
    }

    #[test]
    fn test_multiple_roots_err() {
        let mut forward_only_tree = Tree::<Key, CommitId, Value>::new();
        let mut versioned_hash_map = VersionedHashMap::<Key, CommitId, Value>::new();

        forward_only_tree.add_node(0, None, Vec::new()).unwrap();
        versioned_hash_map.add_node(Vec::new(), 0, None).unwrap();

        assert_eq!(
            forward_only_tree.add_node(1, None, Vec::new()),
            Err(TreeError::MultipleRootsNotAllowed)
        );
        assert_eq!(
            versioned_hash_map.add_node(Vec::new(), 1, None),
            Err(TreeError::MultipleRootsNotAllowed)
        );
    }

    #[test]
    fn test_commit_id_not_found_err() {
        let mut forward_only_tree = Tree::<Key, CommitId, Value>::new();
        let mut versioned_hash_map = VersionedHashMap::<Key, CommitId, Value>::new();

        assert_eq!(
            forward_only_tree.add_node(1, Some(0), Vec::new()),
            Err(TreeError::CommitIDNotFound(0))
        );
        assert_eq!(
            versioned_hash_map.add_node(Vec::new(), 1, Some(0)),
            Err(TreeError::CommitIDNotFound(0))
        );
    }

    #[test]
    fn test_commit_id_already_exists_err() {
        let mut forward_only_tree = Tree::<Key, CommitId, Value>::new();
        let mut versioned_hash_map = VersionedHashMap::<Key, CommitId, Value>::new();

        forward_only_tree.add_node(0, None, Vec::new()).unwrap();
        versioned_hash_map.add_node(Vec::new(), 0, None).unwrap();

        assert_eq!(
            forward_only_tree.add_node(0, Some(0), Vec::new()),
            Err(TreeError::CommitIdAlreadyExists(0))
        );
        assert_eq!(
            versioned_hash_map.add_node(Vec::new(), 0, Some(0)),
            Err(TreeError::CommitIdAlreadyExists(0))
        );
    }
}
