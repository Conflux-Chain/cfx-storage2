use std::collections::BTreeMap;

use crate::traits::{IsCompleted, NeedNext};
use crate::types::ValueEntry;

use super::{
    current_map::CurrentMap,
    pending_schema::{KeyValueMap, PendingKeyValueSchema, RecoverRecord, Result as PendResult},
    tree::Tree,
    PendingError,
};

use parking_lot::RwLock;

/// The `VersionedMap` structure implements the pending part of the underlying database (see `VersionedStore`).
///
/// # Overview:
/// The pending part is in-memory and allows forking, where each node represents a snapshot of a key-value store,
/// and each edge represents the changes between parent and child nodes. The root of this tree is unique
/// and its parent is always the latest node in the historical part. If the historical part is empty,
/// the parent of the pending root is `None`.
///
/// # Implementation Details:
/// `VersionedMap` supports efficient queries, as well as adding or discarding nodes in the pending part.
///
/// - Tree:
///   The `Tree` maintains both:
///   - Information used to connect with the historical part:
///     - The parent of the pending root.
///     - The height of the root (referring to the height of the pending root in the entire underlying database,
///       in order to obtain a continuously increasing `HistoryNumber` when moving it into the historical part in the future).
///   - Information used to support queries in the pending part:
///     - The changes made by each node relative to its parent.
///     - Information required to trace back to the most recent modification in the pending part of each key in these changes.
///
/// - CurrentMap:
///   A `CurrentMap` stores a `CommitId` and its corresponding *relative snapshot*.
///   A *relative snapshot* refers to all the changes (including both the key-value pair,
///   and the `CommitId` that indicates where each change was most recently modified）
///   in the snapshot corresponding to this `CommitId` relative to the snapshot of the parent of the pending root.
///
/// - Efficient Querying:
///   By propagating updates downwards or tracing upwards through the `Tree`, one `CurrentMap` can be efficiently derived from another.
///   Since `CurrentMap` and `Tree` are decoupled, in principle, multiple `CurrentMap`s with multiple branches
///   could be maintained to accelerate queries. However, currently only one `CurrentMap` is maintained at a time.
///
/// - Handling of `None`:
///   If `CurrentMap` is `None`, it indicates that no `CurrentMap` has been computed yet
///   or that the node corresponding to this `CurrentMap` has been pruned.
///
/// # Fields:
/// - `tree`: Maintains the `Tree`.
/// - `current`: Holds an optional reference to a `CurrentMap`.
pub struct VersionedMap<S: PendingKeyValueSchema> {
    tree: Tree<S>,
    current: RwLock<Option<CurrentMap<S>>>,
}

impl<S: PendingKeyValueSchema> VersionedMap<S> {
    /// Creates a new `VersionedMap` instance, initializing the pending part of the database.
    ///
    /// # Parameters:
    /// - `parent_of_root`: The `CommitId` of the parent of the pending root (i.e., the latest node in the historical part).
    ///   If the historical part is empty, this should be `None`.
    /// - `height_of_root`: The height of the pending root in the entire underlying database. This is used to ensure that when
    ///   moving the pending root into the historical part, it will receive a continuously increasing `HistoryNumber`.
    ///
    /// # Returns:
    /// A new `VersionedMap` instance with an empty tree and no computed `CurrentMap`.
    ///
    /// # Notes:
    /// The `current` field is initialized as `None`, indicating that no `CurrentMap` has been computed yet.
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

    /// Returns the `CommitId` of the parent of the pending root.
    ///
    /// # Returns:
    /// An `Option<CommitId>` representing the parent of the pending root.
    /// If the historical part is empty, this will return `None`.
    pub fn get_parent_of_root(&self) -> Option<S::CommitId> {
        self.tree.get_parent_of_root()
    }
}

// add_node
impl<S: PendingKeyValueSchema> VersionedMap<S> {
    /// Adds a node to the pending part.
    ///
    /// # Parameters:
    /// - `parent_commit_id`: Specifies the `CommitID` of the parent node for the node being added.
    ///   If the node being added is the first node in the underlying database, then `parent_commit_id` should be set to `None`.
    /// - `commit_id`: The `CommitID` of the node being added.
    /// - `updates`: A `impl IntoIterator<Item = (Key, Option<Value>)>` representing the changes from the parent node to the new node.
    ///   Here, a pair `(key, None)` indicates the deletion of the key.
    ///
    /// # Notes:
    /// The changes passed in from outside of `VersionedMap` are represented using the `Option<Value>` type
    /// (i.e., `None` indicates deletion, and `Some` represents a specific value).
    /// When stored inside `VersionedMap`, they are converted to the `ValueEntry` type
    /// (i.e., `ValueEntry::Deleted` represents deletion, and `ValueEntry::Value(value)` represents a specific value).
    /// The purpose of this conversion is to assign a specific type for this particular meaning of `Option`,
    /// to avoid confusion with other uses of `Option`.
    ///
    /// # Returns:
    /// A `Result` that is empty if successful, or returns an error if the operation fails.
    /// Failure can occur under several circumstances:
    /// - `parent_commit_id` equals to the parent of the pending root, which indicates that this invocation is to add a pending root,
    ///   but the pending root already exists.
    /// - `parent_commit_id` is different from the parent of the pending root,
    ///   which indicates that this invocation is to add a pending non-root node
    ///   and `parent_commit_id` must already exist in the pending part, but
    ///   - `parent_commit_id` is `None`;
    ///   - or `parent_commit_id` does not exist in the pending part yet;
    ///   - or `commit_id` is already in the pending part.
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

    /// Adds a pending root.
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitID` of the node being added.
    /// - `updates`: A `impl Iterator<Item = (S::Key, ValueEntry<S::Value>)>` representing the changes from the parent node to the new node.
    ///   `ValueEntry::Deleted` represents deletion, and `ValueEntry::Value(value)` represents a specific value.
    ///
    /// # Notes:
    /// Information required to trace back to the most recent modification in the pending part of each key in these changes
    /// is stored in the `last_commit_id` field of `RecoverRecord`. When adding a root, since there are no nodes
    /// before the root in the pending part, the `last_commit_id` for all keys is set to `None`.
    ///
    /// # Returns:
    /// A `Result` that is empty if successful, or returns an error if the operation fails.
    /// Failure can occur due to:
    /// - the pending root already exists.
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

    /// Adds a non-root node to the pending part.
    ///
    /// # Parameters:
    /// - `parent_commit_id`: Specifies the `CommitID` of the parent node for the node being added.
    /// - `commit_id`: The `CommitID` of the node being added.
    /// - `updates`: A `impl Iterator<Item = (S::Key, ValueEntry<S::Value>)>` representing the changes from the parent node to the new node.
    ///   `ValueEntry::Deleted` represents deletion, and `ValueEntry::Value(value)` represents a specific value.
    ///
    /// # Notes:
    /// Information required to trace back to the most recent modification in the pending part of each key in these changes
    /// is stored in the `last_commit_id` field of `RecoverRecord`. When adding a non-root node,
    /// the `last_commit_id` for all keys requires extra computation:
    /// 1. First, obtain the `CurrentMap` at `parent_commit_id`. Then the `CurrentMap` stores all the changes
    ///    (including the `CommitId` that indicates where each change was most recently modified) in the snapshot
    ///    corresponding to `parent_commit_id` relative to the snapshot of the parent of the pending root.
    /// 2. For each change from the parent node to the new node, query the `CurrentMap` at `parent_commit_id` to get `last_commit_id`:
    ///    - If found, use the result (of type `Option<CommitId>`) directly as `last_commit_id`.
    ///    - If not found, it means that this key has not been modified in any of the ancestors of `commit_id` within the pending part,
    ///      so set `last_commit_id` to `None`.
    ///
    /// # Returns:
    /// A `Result` that is empty if successful, or returns an error if the operation fails.
    /// Failure can occur due to:
    /// - `parent_commit_id` does not exist in the pending part yet;
    /// - or `commit_id` is already in the pending part.
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
    /// Queries the modification history of a specified `Key` in the pending part.
    /// Starts from the given `CommitID` and iterates changes backward in the pending part.
    ///
    /// # Parameters:
    /// - `accept`: `impl FnMut(&CommitID, &T::Key, Option<&T::Value>) -> NeedNext`
    ///   Receives a change, including the `CommitID` where the change occurred, the `Key` that was changed, and an `Option<Value>`
    ///   (None means the key was deleted in this change).
    ///   Returns whether to continue iterating backward.
    /// - `commit_id`: The `CommitID` of the snapshot to start iterating backward.
    /// - `key`: The `Key` to query.
    ///
    /// # Returns:
    /// A `Result` containing an `IsCompleted` (i.e., a boolean indicating whether the iteration is completed) if successful,
    /// or an error if the operation fails. Failures include:
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
    /// # Parameters:
    /// - `commit_id`: The `CommitID` to query.
    /// - `key`: The `Key` to query.
    ///
    /// # Returns:
    /// A `Result` containing the changed value if successful, otherwise returns an error if the operation fails.
    /// - The changed value is of type `Option<ValueEntry<Value>>`:
    ///   - None: the value of `key` at `commit_id` is exactly the value of `key` at the parent of the pending root;
    ///   - Some(ValueEntry::Deleted): in the snapshot of `commit_id`, `key` is deleted;
    ///   - Some(ValueEntry::Value(value)): in the snapshot of `commit_id`, `key`'s value is value.
    /// - Failures include:
    ///   - The `commit_id` does not exist in the pending part.
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
    /// # Parameters:
    /// - `commit_id`: The `CommitID` to query.
    ///
    /// # Returns:
    /// A `Result` containing the changes in the type `BTreeMap<Key, ValueEntry<Value>>` if successful,
    /// or an error if the operation fails. Failures include:
    /// - The `commit_id` does not exist in the pending part.
    ///
    /// # Notes:
    /// Need to obtain the `CurrentMap` at `commit_id`. Then the `CurrentMap` stores all the changes (including the key-value pairs)
    /// in the snapshot corresponding to `commit_id` relative to the snapshot of the parent of the pending root.
    /// The key-value pairs are in the type of (`Key`, `ValueEntry(Value)`), where
    /// - `ValueEntry::Deleted` means that the key is deleted;
    /// - `ValueEntry::Value(value)` means that the key is set to be of value.
    ///
    /// # Attentions:
    /// - checkout_current & read current should be guarded together
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
