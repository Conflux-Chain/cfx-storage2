use ethereum_types::H256;

use super::{
    pending_part::pending_schema::PendingKeyValueConfig, table_schema::VersionedKeyValueSchema,
    VersionedStore,
};
use crate::{
    backends::{
        impls::kvdb_rocksdb::open_database, DatabaseTrait, InMemoryDatabase, VersionedKVName,
    },
    errors::Result,
    middlewares::{
        versioned_flat_key_value::{
            confirm_series_to_history, confirmed_pending_to_history, pending_part::VersionedMap,
        },
        CommitID, PendingError,
    },
    traits::{IsCompleted, KeyValueStoreManager, KeyValueStoreRead, NeedNext},
    StorageError,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaChaRng,
};

impl<'cache, 'db, T: VersionedKeyValueSchema> VersionedStore<'cache, 'db, T> {
    #[cfg(test)]
    pub fn check_consistency(&self) -> Result<()> {
        if self.check_consistency_inner().is_err() {
            Err(StorageError::ConsistencyCheckFailure)
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn check_consistency_inner(&self) -> Result<()> {
        use crate::middlewares::commit_id_schema::{
            height_to_history_number, history_number_to_height,
        };

        if let Some(parent) = self.pending_part.get_parent_of_root() {
            let parent_history_number =
                if let Some(parent_history_number) = self.commit_id_table.get(&parent)? {
                    parent_history_number.into_owned()
                } else {
                    return Err(StorageError::ConsistencyCheckFailure);
                };

            let mut history_number = parent_history_number;
            let min_history_number = height_to_history_number(0);
            while history_number >= min_history_number {
                let commit_id =
                    if let Some(commit_id) = self.history_number_table.get(&history_number)? {
                        commit_id.into_owned()
                    } else {
                        return Err(StorageError::ConsistencyCheckFailure);
                    };
                let check_history_number =
                    if let Some(check_history_number) = self.commit_id_table.get(&commit_id)? {
                        check_history_number.into_owned()
                    } else {
                        return Err(StorageError::ConsistencyCheckFailure);
                    };
                if history_number != check_history_number {
                    return Err(StorageError::ConsistencyCheckFailure);
                };
                history_number -= 1;
            }

            let parent_history_number_plus_one = parent_history_number + 1;
            if self
                .history_number_table
                .iter(&parent_history_number_plus_one)?
                .next()
                .is_some()
            {
                return Err(StorageError::ConsistencyCheckFailure);
            }

            if self.commit_id_table.iter_from_start()?.count()
                != self.history_number_table.iter_from_start()?.count()
            {
                return Err(StorageError::ConsistencyCheckFailure);
            }

            if !self
                .pending_part
                .check_consistency(history_number_to_height(parent_history_number + 1))
            {
                return Err(StorageError::ConsistencyCheckFailure);
            }
            // todo: history_index_table, change_table
        } else if self.commit_id_table.iter_from_start()?.next().is_some()
            || self
                .history_number_table
                .iter_from_start()?
                .next()
                .is_some()
            || self.history_index_table.iter_from_start()?.next().is_some()
            || self
                .change_history_table
                .iter_from_start()?
                .next()
                .is_some()
        {
            return Err(StorageError::ConsistencyCheckFailure);
        }

        Ok(())
    }
}

type MockStore<T> = BTreeMap<
    <T as VersionedKeyValueSchema>::Key,
    (Option<<T as VersionedKeyValueSchema>::Value>, bool),
>;

#[derive(PartialEq, Debug)]
pub struct MockOneStore<K: Ord, V: Clone> {
    map: BTreeMap<K, V>,
}

impl<K: Ord + Clone, V: Clone> MockOneStore<K, V> {
    pub fn from_mock_map(map: &BTreeMap<K, (Option<V>, bool)>) -> Self {
        let inner_map = map
            .iter()
            .filter_map(|(k, (opt_v, _))| opt_v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect();
        MockOneStore { map: inner_map }
    }

    pub fn get_keys(&self) -> Vec<K> {
        self.map.keys().cloned().collect()
    }
}

impl<K: 'static + Ord, V: 'static + Clone> KeyValueStoreRead<K, V> for MockOneStore<K, V> {
    fn get(&self, key: &K) -> Result<Option<V>> {
        Ok(self.map.get(key).cloned())
    }
}

#[derive(Debug)]
struct MockVersionedStore<T: VersionedKeyValueSchema> {
    pending: MockTree<T>,
    history: HashMap<CommitID, (Option<CommitID>, MockStore<T>)>,
}

#[derive(Debug)]
struct MockTree<T: VersionedKeyValueSchema> {
    tree: HashMap<CommitID, MockNode<T>>,
    parent_of_root: Option<CommitID>,
}

#[derive(Debug, Clone)]
struct MockNode<T: VersionedKeyValueSchema> {
    commit_id: CommitID,
    parent: Option<CommitID>,
    children: HashSet<CommitID>,
    store: MockStore<T>,
}

impl<T: VersionedKeyValueSchema> KeyValueStoreManager<T::Key, T::Value, CommitID>
    for MockVersionedStore<T>
{
    type Store = MockOneStore<T::Key, T::Value>;

    fn get_versioned_store(&self, commit: &CommitID) -> Result<Self::Store> {
        if let Some(pending_res) = self.pending.tree.get(commit) {
            Ok(MockOneStore::from_mock_map(&pending_res.store))
        } else if let Some((_, history_res)) = self.history.get(commit) {
            Ok(MockOneStore::from_mock_map(history_res))
        } else {
            Err(StorageError::CommitIDNotFound)
        }
    }

    fn iter_historical_changes(
        &self,
        mut accept: impl FnMut(&CommitID, &T::Key, Option<&T::Value>) -> NeedNext,
        commit_id: &CommitID,
        key: &T::Key,
    ) -> Result<IsCompleted> {
        let mut current_node = self.pending.tree.get(commit_id);
        while let Some(node) = current_node {
            if let Some((value, true)) = node.store.get(key) {
                if !accept(&node.commit_id, key, value.as_ref()) {
                    return Ok(false);
                }
            }
            current_node = node.parent.map(|p| self.pending.tree.get(&p).unwrap());
        }

        let history_commit_id = if self.pending.tree.contains_key(commit_id) {
            if let Some(parent_of_pending) = self.pending.parent_of_root {
                parent_of_pending
            } else {
                assert!(self.history.is_empty());
                return Ok(true);
            }
        } else {
            *commit_id
        };

        if !self.history.contains_key(&history_commit_id) {
            return Err(StorageError::CommitIDNotFound);
        }

        let mut current_cid = Some(history_commit_id);
        while let Some(cid) = current_cid {
            let (parent_cid, store) = self.history.get(&cid).unwrap();
            if let Some((value, true)) = store.get(key) {
                if !accept(&cid, key, value.as_ref()) {
                    return Ok(false);
                }
            }
            current_cid = *parent_cid;
        }

        Ok(true)
    }

    fn discard(&mut self, commit: CommitID) -> Result<()> {
        if self.history.contains_key(&commit) {
            return Ok(());
        }

        if self.pending.tree.contains_key(&commit) {
            if let Some(parent) = self.pending.tree.get(&commit).unwrap().parent {
                let mut to_remove = VecDeque::new();

                assert!(self
                    .pending
                    .tree
                    .get(&parent)
                    .unwrap()
                    .children
                    .contains(&commit));
                for child in self.pending.tree.get(&parent).unwrap().children.iter() {
                    if *child != commit {
                        to_remove.push_back(*child);
                    }
                }

                while !to_remove.is_empty() {
                    let remove_this = to_remove.pop_front().unwrap();
                    let remove_this_node = self.pending.tree.remove(&remove_this).unwrap();
                    for child in remove_this_node.children.iter() {
                        to_remove.push_back(*child);
                    }
                }

                self.pending.tree.get_mut(&parent).unwrap().children = HashSet::from([commit]);
            }

            Ok(())
        } else {
            Err(StorageError::PendingError(PendingError::CommitIDNotFound(
                commit,
            )))
        }
    }

    fn get_versioned_key(&self, commit: &CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        self.get_versioned_store(commit)?.get(key)
    }
}

fn update_last_store_to_store<T: VersionedKeyValueSchema>(
    last_store: &MockStore<T>,
    updates: BTreeMap<T::Key, Option<T::Value>>,
) -> MockStore<T> {
    let mut store: BTreeMap<_, _> = last_store
        .iter()
        .map(|(k, (opt_v, _))| (k.clone(), (opt_v.clone(), false)))
        .collect();
    for (k, opt_v) in updates.into_iter() {
        store.insert(k, (opt_v, true));
    }
    store
}

#[derive(Clone)]
pub struct UniqueVec<T> {
    items: Vec<T>,
    set: HashSet<T>,
}

impl<T: Eq + std::hash::Hash + Clone> UniqueVec<T> {
    pub fn new() -> Self {
        UniqueVec {
            items: Vec::new(),
            set: HashSet::new(),
        }
    }

    pub fn push(&mut self, item: T) -> bool {
        if self.set.insert(item.clone()) {
            self.items.push(item);
            true
        } else {
            false
        }
    }

    pub fn contains(&self, item: &T) -> bool {
        self.set.contains(item)
    }

    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn into_vec(self) -> Vec<T> {
        self.items
    }
}

impl<T: VersionedKeyValueSchema> MockVersionedStore<T> {
    pub fn build(
        history_cids: UniqueVec<CommitID>,
        history_updates: Vec<BTreeMap<T::Key, Option<T::Value>>>,
    ) -> Self {
        assert_eq!(history_cids.len(), history_updates.len());
        let mut history: HashMap<_, _> = Default::default();
        let mut last_store = Default::default();
        let mut last_commit_id = None;
        for (commit_id, updates) in history_cids.items.iter().zip(history_updates.into_iter()) {
            let store = update_last_store_to_store::<T>(&last_store, updates);
            history.insert(*commit_id, (last_commit_id, store.clone()));
            last_store = store;
            last_commit_id = Some(*commit_id);
        }
        MockVersionedStore::<T>::new_unchecked(last_commit_id, history)
    }

    pub fn check_consistency(&self) {
        if let Some(parent) = self.get_parent_of_root() {
            assert!(self.history.contains_key(&parent));
            let mut num_history = 1;
            let mut commit_id = parent;
            while let (Some(parent_commit_id), _) = self.history.get(&commit_id).unwrap() {
                num_history += 1;
                commit_id = *parent_commit_id;
            }
            assert_eq!(num_history, self.history.len());

            let root = self.get_pending_root();
            if let Some(root) = root.last() {
                assert!(self.pending.tree.get(root).unwrap().parent.is_none());
            }

            assert_eq!(
                self.get_history().len() + self.get_pending().len(),
                self.get_commit_ids().len()
            )
        } else {
            assert!(self.history.is_empty());
        }
    }

    fn new_unchecked(
        parent_of_pending: Option<CommitID>,
        history: HashMap<CommitID, (Option<CommitID>, MockStore<T>)>,
    ) -> Self {
        let mock_versioned_store = Self {
            pending: MockTree {
                tree: Default::default(),
                parent_of_root: parent_of_pending,
            },
            history,
        };
        mock_versioned_store.check_consistency();
        mock_versioned_store
    }

    pub fn get_pending_root(&self) -> Vec<CommitID> {
        let pending_root: Vec<_> = self
            .pending
            .tree
            .iter()
            .filter(|(_, node)| node.parent.is_none())
            .map(|(cid, _)| *cid)
            .collect();
        if self.pending.tree.is_empty() {
            assert_eq!(pending_root.len(), 0);
        } else {
            assert_eq!(pending_root.len(), 1);
        }
        pending_root
    }

    pub fn get_pending_non_root(&self) -> Vec<CommitID> {
        let mut pending_non_root: Vec<_> = self
            .pending
            .tree
            .iter()
            .filter(|(_, node)| node.parent.is_some())
            .map(|(cid, _)| *cid)
            .collect();
        if self.pending.tree.is_empty() {
            assert_eq!(pending_non_root.len(), 0);
        } else {
            assert_eq!(pending_non_root.len() + 1, self.pending.tree.len());
        }
        pending_non_root.sort();
        pending_non_root
    }

    pub fn get_pending(&self) -> Vec<CommitID> {
        let mut pending: Vec<_> = self.pending.tree.keys().cloned().collect();
        pending.sort();
        pending
    }

    pub fn get_parent_of_root(&self) -> Option<CommitID> {
        self.pending.parent_of_root
    }

    pub fn get_history(&self) -> Vec<CommitID> {
        let mut history: Vec<_> = self.history.keys().cloned().collect();
        history.sort();
        history
    }

    pub fn get_history_but_parent_of_root(&self) -> Vec<CommitID> {
        let mut history: HashSet<_> = self.history.keys().cloned().collect();
        if let Some(parent_of_root) = self.pending.parent_of_root {
            history.remove(&parent_of_root);
        }
        let mut history: Vec<_> = history.into_iter().collect();
        history.sort();
        history
    }

    pub fn get_commit_ids(&self) -> BTreeSet<CommitID> {
        self.history
            .keys()
            .cloned()
            .chain(self.pending.tree.keys().cloned())
            .collect()
    }

    fn get_keys_on_path(&self, commit: &CommitID) -> Vec<T::Key> {
        let mut keys: Vec<_> = if let Some(pending_res) = self.pending.tree.get(commit) {
            pending_res.store.keys().cloned().collect()
        } else if let Some((_, history_res)) = self.history.get(commit) {
            history_res.keys().cloned().collect()
        } else {
            Vec::new()
        };
        keys.sort();
        keys
    }

    pub fn add_to_pending_part(
        &mut self,
        parent_commit: Option<CommitID>,
        commit: CommitID,
        updates: BTreeMap<T::Key, Option<T::Value>>,
    ) -> Result<()> {
        if self.history.contains_key(&commit) {
            return Err(StorageError::CommitIdAlreadyExistsInHistory);
        }

        if parent_commit == self.pending.parent_of_root {
            if !self.pending.tree.is_empty() {
                return Err(StorageError::PendingError(
                    PendingError::MultipleRootsNotAllowed,
                ));
            }

            let default_store = Default::default();
            let last_store = if let Some(parent_commit_id) = parent_commit {
                let (_, history_store) = self.history.get(&parent_commit_id).unwrap();
                history_store
            } else {
                &default_store
            };
            let store = update_last_store_to_store::<T>(last_store, updates);

            let root = MockNode {
                commit_id: commit,
                parent: None,
                children: Default::default(),
                store,
            };
            self.pending.tree.insert(commit, root);

            Ok(())
        } else if let Some(parent_commit_id) = parent_commit {
            if !self.pending.tree.contains_key(&parent_commit_id) {
                return Err(StorageError::PendingError(PendingError::CommitIDNotFound(
                    parent_commit_id,
                )));
            }
            if self.pending.tree.contains_key(&commit) {
                return Err(StorageError::PendingError(
                    PendingError::CommitIdAlreadyExists(commit),
                ));
            }

            let last_store = &self.pending.tree.get(&parent_commit_id).unwrap().store;
            let store = update_last_store_to_store::<T>(last_store, updates);

            let node = MockNode {
                commit_id: commit,
                parent: parent_commit,
                children: Default::default(),
                store,
            };
            self.pending.tree.insert(commit, node);
            self.pending
                .tree
                .get_mut(&parent_commit_id)
                .unwrap()
                .children
                .insert(commit);

            Ok(())
        } else {
            Err(StorageError::PendingError(
                PendingError::NonRootNodeShouldHaveParent,
            ))
        }
    }

    pub fn confirmed_pending_to_history(&mut self, new_root_commit_id: CommitID) -> Result<()> {
        if !self.pending.tree.contains_key(&new_root_commit_id) {
            return Err(StorageError::PendingError(PendingError::CommitIDNotFound(
                new_root_commit_id,
            )));
        }

        let mut parent_cid = self
            .pending
            .tree
            .get(&new_root_commit_id)
            .cloned()
            .unwrap()
            .parent;
        let mut commit_id = new_root_commit_id;
        while let Some(parent_commit_id) = parent_cid {
            self.discard(commit_id).unwrap();

            let parent_node = self.pending.tree.remove(&parent_commit_id).unwrap();

            let grandparent = if let Some(grandparent) = parent_node.parent {
                Some(grandparent)
            } else {
                self.pending.parent_of_root
            };
            self.history
                .insert(parent_commit_id, (grandparent, parent_node.store));

            commit_id = parent_commit_id;
            parent_cid = parent_node.parent;
        }

        let old_root_commit_id = commit_id;
        if old_root_commit_id != new_root_commit_id {
            self.pending.parent_of_root =
                self.pending.tree.get(&new_root_commit_id).unwrap().parent;
            self.pending
                .tree
                .get_mut(&new_root_commit_id)
                .unwrap()
                .parent = None;
        }

        self.check_consistency();

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct TestSchema;

impl VersionedKeyValueSchema for TestSchema {
    const NAME: VersionedKVName = VersionedKVName::FlatKV;
    type Key = u64;
    type Value = u64;
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
enum Operation {
    GetVersionedStore,
    IterHisoricalChanges,
    Discard,
    GetVersionedKey,
    AddToPendingPart,
    ConfirmedPendingToHistory,
}

#[derive(Clone, PartialEq, Debug)]
enum CommitIDType {
    History,
    PendingRoot,
    PendingNonRoot,
    Novel,
}

#[derive(Clone, PartialEq, Debug)]
enum ParentCommitType {
    Pending,
    ParentOfPendingRoot,
    NoneButInvalid,
    HistoryButInvalid,
    Novel,
}

#[derive(Clone)]
enum KeyType {
    Exist,
    Novel,
}

pub fn get_rng_for_test() -> ChaChaRng {
    ChaChaRng::from_seed([123; 32])
}

fn gen_opt_value(rng: &mut ChaChaRng) -> Option<u64> {
    let value_is_none = (rng.next_u64() % 3) == 0;
    if value_is_none {
        None
    } else {
        Some(rng.next_u64())
    }
}

fn select_vec_element<T: Clone>(rng: &mut ChaChaRng, vec: &[T]) -> T {
    assert!(!vec.is_empty());
    let num_elements = vec.len();
    vec[rng.next_u64() as usize % num_elements].clone()
}

pub fn gen_updates(
    rng: &mut ChaChaRng,
    previous_keys: &BTreeSet<u64>,
    num_gen_new_keys: usize,
    num_gen_previous_keys: usize,
    all_keys: &mut BTreeSet<u64>,
) -> BTreeMap<u64, Option<u64>> {
    // gen previous keys (i.e., replace), allow redundant keys and adopt the newest value for the same key
    let mut updates: BTreeMap<_, _> = if !previous_keys.is_empty() {
        let previous_keys_vec: Vec<_> = previous_keys.iter().cloned().collect();
        (0..num_gen_previous_keys)
            .map(|_| {
                (
                    select_vec_element(rng, &previous_keys_vec),
                    gen_opt_value(rng),
                )
            })
            .collect()
    } else {
        Default::default()
    };

    // gen new keys (i.e., insert), do not allow repeated keys
    let mut new_keys = BTreeSet::new();
    while new_keys.len() < num_gen_new_keys {
        let key = rng.next_u64();
        if previous_keys.contains(&key) || new_keys.contains(&key) {
            continue;
        }
        new_keys.insert(key);
        updates.insert(key, gen_opt_value(rng));
    }

    for key in updates.keys() {
        all_keys.insert(*key);
    }

    updates
}

#[allow(clippy::type_complexity)]
fn gen_init(
    db: &mut impl DatabaseTrait,
    num_history: usize,
    rng: &mut ChaChaRng,
    max_num_new_keys: usize,
    max_num_previous_keys: usize,
    all_keys: &mut BTreeSet<u64>,
) -> (
    UniqueVec<CommitID>,
    Vec<BTreeMap<u64, Option<u64>>>,
    VersionedMap<PendingKeyValueConfig<TestSchema, CommitID>>,
) {
    let mut history_cids = UniqueVec::new();
    for _ in 0..num_history << 4 {
        if history_cids.len() < num_history {
            history_cids.push(gen_random_commit_id(rng));
        } else {
            break;
        }
    }
    assert_eq!(history_cids.len(), num_history);

    assert!(all_keys.is_empty());
    let mut history_updates = Vec::new();
    for _ in 0..num_history {
        let num_new_keys = (rng.next_u64() as usize % max_num_new_keys) + 1;
        let num_previous_keys = (rng.next_u64() as usize % max_num_previous_keys) + 1;
        let previous_keys = all_keys.clone();
        history_updates.push(gen_updates(
            rng,
            &previous_keys,
            num_new_keys,
            num_previous_keys,
            all_keys,
        ));
    }

    let pending_part = VersionedMap::new(history_cids.items().last().copied(), history_cids.len());

    confirm_series_to_history::<_, TestSchema>(
        db,
        0,
        history_cids
            .clone()
            .into_vec()
            .into_iter()
            .zip(history_updates.clone())
            .collect(),
        true,
    )
    .unwrap();

    (history_cids, history_updates, pending_part)
}

fn gen_novel_u64(rng: &mut ChaChaRng, previous: &BTreeSet<u64>) -> u64 {
    for _ in 0..1 << 4 {
        let novel = rng.next_u64();
        if !previous.contains(&novel) {
            return novel;
        }
    }

    panic!("Failed to generate a novel u64 after {} attempts", 1 << 4)
}

pub fn gen_random_commit_id(rng: &mut ChaChaRng) -> CommitID {
    let mut bytes = [0u8; 32];
    for i in 0..4 {
        let num = rng.next_u64().to_ne_bytes();
        bytes[i * 8..(i + 1) * 8].copy_from_slice(&num);
    }
    H256::from_slice(&bytes)
}

fn gen_key(rng: &mut ChaChaRng, existing_keys: Vec<u64>) -> (KeyType, u64) {
    let key_types = if existing_keys.is_empty() {
        vec![KeyType::Novel]
    } else {
        vec![KeyType::Novel, KeyType::Exist]
    };
    let key_type = select_vec_element(rng, &key_types);
    match key_type {
        KeyType::Exist => (key_type, select_vec_element(rng, &existing_keys)),
        KeyType::Novel => {
            let previous = BTreeSet::from_iter(existing_keys);
            (key_type, gen_novel_u64(rng, &previous))
        }
    }
}

struct VersionedStoreProxy<'a, 'b, 'c, 'cache, 'db, T: VersionedKeyValueSchema> {
    mock_store: &'a mut MockVersionedStore<T>,
    real_store: &'b mut VersionedStore<'cache, 'db, T>,
    all_keys: &'c mut BTreeSet<T::Key>,
}

impl<'a, 'b, 'c, 'cache, 'db, T: VersionedKeyValueSchema<Key = u64, Value = u64>>
    VersionedStoreProxy<'a, 'b, 'c, 'cache, 'db, T>
where
    T::Value: PartialEq,
{
    fn new(
        mock_store: &'a mut MockVersionedStore<T>,
        real_store: &'b mut VersionedStore<'cache, 'db, T>,
        all_keys: &'c mut BTreeSet<T::Key>,
    ) -> Self {
        Self {
            mock_store,
            real_store,
            all_keys,
        }
    }

    fn gen_novel_commit_id(&self, rng: &mut ChaChaRng) -> CommitID {
        let previous = self.mock_store.get_commit_ids();
        for _ in 0..1 << 4 {
            let novel = gen_random_commit_id(rng);
            if !previous.contains(&novel) {
                return novel;
            }
        }

        panic!(
            "Failed to generate a novel commit ID after {} attempts",
            1 << 4
        )
    }

    fn gen_commit_id(&self, rng: &mut ChaChaRng) -> (CommitIDType, CommitID) {
        let mut commit_id_types = vec![CommitIDType::Novel];
        if !self.mock_store.history.is_empty() {
            commit_id_types.push(CommitIDType::History);
        }
        if !self.mock_store.pending.tree.is_empty() {
            commit_id_types.push(CommitIDType::PendingRoot);
            if self.mock_store.pending.tree.len() > 1 {
                commit_id_types.push(CommitIDType::PendingNonRoot);
            }
        }
        let commit_id_type = select_vec_element(rng, &commit_id_types);
        let selected_range = match commit_id_type {
            CommitIDType::History => self.mock_store.get_history(),
            CommitIDType::PendingRoot => self.mock_store.get_pending_root(),
            CommitIDType::PendingNonRoot => self.mock_store.get_pending_non_root(),
            CommitIDType::Novel => {
                return (commit_id_type, self.gen_novel_commit_id(rng));
            }
        };
        (commit_id_type, select_vec_element(rng, &selected_range))
    }

    fn gen_parent_commit(
        &self,
        rng: &mut ChaChaRng,
        pending_only: bool,
    ) -> (ParentCommitType, Option<CommitID>) {
        let parent_types = if pending_only {
            assert!(!self.mock_store.pending.tree.is_empty());
            vec![ParentCommitType::Pending]
        } else {
            let mut parent_types = vec![
                ParentCommitType::ParentOfPendingRoot,
                ParentCommitType::Novel,
            ];
            if self.mock_store.get_parent_of_root().is_some() {
                parent_types.push(ParentCommitType::NoneButInvalid)
            }
            if !self.mock_store.pending.tree.is_empty() {
                parent_types.push(ParentCommitType::Pending)
            }
            if self.mock_store.history.len() > 1 {
                parent_types.push(ParentCommitType::HistoryButInvalid)
            }
            parent_types
        };
        let parent_type = select_vec_element(rng, &parent_types);
        let selected_range = match parent_type {
            ParentCommitType::Pending => self.mock_store.get_pending(),
            ParentCommitType::ParentOfPendingRoot => {
                return (parent_type, self.mock_store.get_parent_of_root())
            }
            ParentCommitType::NoneButInvalid => return (parent_type, None),
            ParentCommitType::HistoryButInvalid => self.mock_store.get_history_but_parent_of_root(),
            ParentCommitType::Novel => {
                return (parent_type, Some(self.gen_novel_commit_id(rng)));
            }
        };
        (parent_type, Some(select_vec_element(rng, &selected_range)))
    }

    fn get_previous_keys(&self, parent_commit: Option<CommitID>) -> BTreeSet<u64> {
        if let Some(parent_cid) = parent_commit {
            self.mock_store
                .get_keys_on_path(&parent_cid)
                .into_iter()
                .collect()
        } else {
            Default::default()
        }
    }

    fn init_pending_part(
        &mut self,
        num_pending: usize,
        rng: &mut ChaChaRng,
        num_gen_new_keys: usize,
        num_gen_previous_keys: usize,
    ) {
        if num_pending > 0 {
            // gen root
            let pending_root = self.gen_novel_commit_id(rng);
            let previous_keys = self.get_previous_keys(self.mock_store.pending.parent_of_root);
            let updates = gen_updates(
                rng,
                &previous_keys,
                num_gen_new_keys,
                num_gen_previous_keys,
                self.all_keys,
            );

            // add root
            let parent_of_root = self.mock_store.pending.parent_of_root;
            self.mock_store
                .add_to_pending_part(parent_of_root, pending_root, updates.clone())
                .unwrap();
            self.real_store
                .add_to_pending_part(parent_of_root, pending_root, updates)
                .unwrap();

            // add non_root nodes
            for _ in 0..num_pending - 1 {
                let commit_id = self.gen_novel_commit_id(rng);
                let (parent_commit_type, parent_commit) = self.gen_parent_commit(rng, true);
                assert_eq!(parent_commit_type, ParentCommitType::Pending);
                assert!(parent_commit.is_some());
                let previous_keys = self.get_previous_keys(parent_commit);
                let updates = gen_updates(
                    rng,
                    &previous_keys,
                    num_gen_new_keys,
                    num_gen_previous_keys,
                    self.all_keys,
                );

                self.mock_store
                    .add_to_pending_part(parent_commit, commit_id, updates.clone())
                    .unwrap();
                self.real_store
                    .add_to_pending_part(parent_commit, commit_id, updates)
                    .unwrap();
            }
        }

        self.mock_store.check_consistency();
        self.real_store.check_consistency().unwrap();
    }
}

impl<'a, 'b, 'c, 'cache, 'db, T: VersionedKeyValueSchema<Key = u64, Value = u64>>
    VersionedStoreProxy<'a, 'b, 'c, 'cache, 'db, T>
{
    fn get_versioned_store(
        &self,
        rng: &mut ChaChaRng,
        commit_id_type: CommitIDType,
        commit: &CommitID,
    ) -> bool {
        let mock_res = self.mock_store.get_versioned_store(commit);
        let real_res = self.real_store.get_versioned_store(commit);

        match commit_id_type {
            CommitIDType::Novel => {
                assert_eq!(mock_res, Err(StorageError::CommitIDNotFound));
                match real_res {
                    Err(err) => assert_eq!(err, StorageError::CommitIDNotFound),
                    _ => panic!("real is ok but mock is err"),
                }
                false
            }
            _ => {
                let mock_res = mock_res.unwrap();
                let real_res = real_res.unwrap();
                for key in self.all_keys.iter() {
                    assert_eq!(mock_res.get(key), real_res.get(key));
                }
                for _ in 0..10 {
                    let key = gen_novel_u64(rng, self.all_keys);
                }
                true
            }
        }
    }

    fn iter_historical_changes(
        &self,
        rng: &mut ChaChaRng,
        commit_id_type: CommitIDType,
        commit_id: &CommitID,
    ) -> bool {
        let keys_on_path = self.mock_store.get_keys_on_path(commit_id);
        let (key_type, key) = gen_key(rng, keys_on_path);

        let mut mock_collected = Vec::new();
        let mock_accept = |cid: &CommitID, k: &T::Key, v: Option<&T::Value>| -> NeedNext {
            mock_collected.push((*cid, *k, v.copied()));
            true
        };
        let mock_res = self
            .mock_store
            .iter_historical_changes(mock_accept, commit_id, &key);

        let mut real_collected = Vec::new();
        let real_accept = |cid: &CommitID, k: &T::Key, v: Option<&T::Value>| -> NeedNext {
            real_collected.push((*cid, *k, v.copied()));
            true
        };
        let real_res = self
            .real_store
            .iter_historical_changes(real_accept, commit_id, &key);

        match (mock_res, real_res) {
            (Err(mock_err), Err(real_err)) => {
                assert_eq!(mock_err, real_err);

                assert_eq!(commit_id_type, CommitIDType::Novel);
                assert_eq!(mock_err, StorageError::CommitIDNotFound);

                false
            }
            (Ok(true), Ok(true)) => {
                assert_eq!(mock_collected, real_collected);

                assert_ne!(commit_id_type, CommitIDType::Novel);
                match key_type {
                    KeyType::Exist => assert!(!mock_collected.is_empty()),
                    KeyType::Novel => assert!(mock_collected.is_empty()),
                }

                true
            }
            _ => panic!(),
        }
    }

    fn get_versioned_key(
        &self,
        rng: &mut ChaChaRng,
        commit_id_type: CommitIDType,
        commit: &CommitID,
    ) -> bool {
        let mock_one_store = self.mock_store.get_versioned_store(commit);
        let mock_keys = if let Ok(ref mock_store) = mock_one_store {
            mock_store.get_keys()
        } else {
            Default::default()
        };
        let (key_type, key) = gen_key(rng, mock_keys);

        let mock_res = self.mock_store.get_versioned_key(commit, &key);
        let real_res = self.real_store.get_versioned_key(commit, &key);

        assert_eq!(mock_res, real_res);

        match (commit_id_type, key_type) {
            (CommitIDType::Novel, _) => {
                assert_eq!(mock_res, Err(StorageError::CommitIDNotFound))
            }
            (_, KeyType::Exist) => assert_eq!(
                mock_res.unwrap().unwrap(),
                mock_one_store.unwrap().get(&key).unwrap().unwrap()
            ),
            (_, KeyType::Novel) => {
                assert!(mock_res.unwrap().is_none());
                assert!(mock_one_store.unwrap().get(&key).unwrap().is_none());
            }
        };

        real_res.is_ok()
    }

    fn discard(&mut self, commit_id_type: CommitIDType, commit: CommitID) -> bool {
        let mock_res = self.mock_store.discard(commit);
        let real_res = self.real_store.discard(commit);

        assert_eq!(mock_res, real_res);

        match commit_id_type {
            CommitIDType::Novel => assert_eq!(
                mock_res,
                Err(StorageError::PendingError(PendingError::CommitIDNotFound(
                    commit
                )))
            ),
            _ => assert!(mock_res.is_ok()),
        };

        self.mock_store.check_consistency();
        self.real_store.check_consistency().unwrap();

        real_res.is_ok()
    }

    fn add_to_pending_part(
        &mut self,
        rng: &mut ChaChaRng,
        commit_id_type: CommitIDType,
        commit: CommitID,
        num_gen_new_keys: usize,
        num_gen_previous_keys: usize,
    ) -> bool {
        let has_root_before_add = !self.mock_store.pending.tree.is_empty();
        let (parent_commit_type, parent_commit) = self.gen_parent_commit(rng, false);
        let previous_keys = self.get_previous_keys(parent_commit);
        let updates = gen_updates(
            rng,
            &previous_keys,
            num_gen_new_keys,
            num_gen_previous_keys,
            self.all_keys,
        );

        let mock_res = self
            .mock_store
            .add_to_pending_part(parent_commit, commit, updates.clone());
        let real_res = self
            .real_store
            .add_to_pending_part(parent_commit, commit, updates);

        assert_eq!(mock_res, real_res);

        match (parent_commit_type, commit_id_type.clone()) {
            (_, CommitIDType::History) => assert_eq!(
                mock_res.unwrap_err(),
                StorageError::CommitIdAlreadyExistsInHistory
            ),
            (ParentCommitType::NoneButInvalid, _) => assert_eq!(
                mock_res.unwrap_err(),
                StorageError::PendingError(PendingError::NonRootNodeShouldHaveParent)
            ),
            (ParentCommitType::ParentOfPendingRoot, _) => {
                if has_root_before_add {
                    assert_eq!(
                        mock_res.unwrap_err(),
                        StorageError::PendingError(PendingError::MultipleRootsNotAllowed)
                    );
                } else {
                    assert_eq!(commit_id_type, CommitIDType::Novel);
                    assert!(mock_res.is_ok());
                }
            }
            (ParentCommitType::HistoryButInvalid, _) | (ParentCommitType::Novel, _) => {
                assert_eq!(
                    mock_res.unwrap_err(),
                    StorageError::PendingError(PendingError::CommitIDNotFound(
                        parent_commit.unwrap()
                    ))
                )
            }
            (ParentCommitType::Pending, CommitIDType::PendingRoot)
            | (ParentCommitType::Pending, CommitIDType::PendingNonRoot) => assert_eq!(
                mock_res.unwrap_err(),
                StorageError::PendingError(PendingError::CommitIdAlreadyExists(commit))
            ),
            (ParentCommitType::Pending, CommitIDType::Novel) => assert!(mock_res.is_ok()),
        };

        real_res.is_ok()
    }
}

fn test_versioned_store(
    db: &mut impl DatabaseTrait,
    num_history: usize,
    num_pending: usize,
    num_operations: usize,
) {
    let mut rng = get_rng_for_test();
    let num_gen_new_keys = 10;
    let num_gen_previous_keys = 10;

    let mut all_keys = BTreeSet::new();

    // init history part
    let (history_cids, history_updates, mut pending_part) = gen_init(
        db,
        num_history,
        &mut rng,
        num_gen_new_keys,
        num_gen_previous_keys,
        &mut all_keys,
    );

    let mut mock_versioned_store =
        MockVersionedStore::build(history_cids.clone(), history_updates.clone());

    let mut real_versioned_store = VersionedStore::new(db, &mut pending_part).unwrap();
    real_versioned_store.check_consistency().unwrap();

    let mut versioned_store_proxy = VersionedStoreProxy::new(
        &mut mock_versioned_store,
        &mut real_versioned_store,
        &mut all_keys,
    );

    // init pending part
    versioned_store_proxy.init_pending_part(
        num_pending,
        &mut rng,
        num_gen_new_keys,
        num_gen_previous_keys,
    );

    let operations = vec![
        Operation::GetVersionedStore,
        Operation::GetVersionedStore,
        Operation::GetVersionedStore,
        Operation::IterHisoricalChanges,
        Operation::Discard,
        Operation::GetVersionedKey,
        Operation::AddToPendingPart,
        Operation::AddToPendingPart,
        Operation::AddToPendingPart,
        Operation::AddToPendingPart,
        Operation::ConfirmedPendingToHistory,
    ];

    let mut operations_analyses = HashMap::new();
    for _ in 0..num_operations {
        let operation = select_vec_element(&mut rng, &operations);
        let (commit_id_type, commit_id) = versioned_store_proxy.gen_commit_id(&mut rng);

        let this_operation_is_ok = match operation {
            Operation::GetVersionedStore => {
                versioned_store_proxy.get_versioned_store(&mut rng, commit_id_type, &commit_id)
            }
            Operation::GetVersionedKey => {
                versioned_store_proxy.get_versioned_key(&mut rng, commit_id_type, &commit_id)
            }
            Operation::IterHisoricalChanges => {
                versioned_store_proxy.iter_historical_changes(&mut rng, commit_id_type, &commit_id)
            }
            Operation::Discard => versioned_store_proxy.discard(commit_id_type, commit_id),
            Operation::AddToPendingPart => versioned_store_proxy.add_to_pending_part(
                &mut rng,
                commit_id_type,
                commit_id,
                num_gen_new_keys,
                num_gen_previous_keys,
            ),
            Operation::ConfirmedPendingToHistory => {
                let mock_res = mock_versioned_store.confirmed_pending_to_history(commit_id);

                drop(real_versioned_store);
                let real_res = confirmed_pending_to_history(db, &mut pending_part, commit_id, true);
                real_versioned_store = VersionedStore::new(db, &mut pending_part).unwrap();
                real_versioned_store.check_consistency().unwrap();

                versioned_store_proxy = VersionedStoreProxy::new(
                    &mut mock_versioned_store,
                    &mut real_versioned_store,
                    &mut all_keys,
                );

                assert_eq!(mock_res, real_res);

                match commit_id_type {
                    CommitIDType::PendingRoot | CommitIDType::PendingNonRoot => {
                        assert!(mock_res.is_ok());
                    }
                    _ => assert_eq!(
                        mock_res.unwrap_err(),
                        StorageError::PendingError(PendingError::CommitIDNotFound(commit_id))
                    ),
                };

                real_res.is_ok()
            }
        };
        *operations_analyses
            .entry((operation, this_operation_is_ok))
            .or_insert(0) += 1;
    }

    println!("operations_analyses");

    let operations_set = BTreeSet::from([
        Operation::GetVersionedStore,
        Operation::IterHisoricalChanges,
        Operation::Discard,
        Operation::GetVersionedKey,
        Operation::AddToPendingPart,
        Operation::ConfirmedPendingToHistory,
    ]);

    print!("{:<20}", "");
    for op in &operations_set {
        let str = match op {
            Operation::IterHisoricalChanges => "IterChanges",
            Operation::GetVersionedStore => "GetStore",
            Operation::AddToPendingPart => "*Add",
            Operation::Discard => "*Discard",
            Operation::GetVersionedKey => "GetKey",
            Operation::ConfirmedPendingToHistory => "*Confirm",
        };
        print!("{:>15}", str);
    }
    println!();

    for &flag in &[true, false] {
        print!("{:<20}", flag);
        for op in &operations_set {
            let count = operations_analyses.get(&(op.clone(), flag)).unwrap_or(&0);
            print!("{:>15}", count);
        }
        println!();
    }
}

pub fn empty_rocksdb(db_path: &str) -> Result<kvdb_rocksdb::Database> {
    use crate::backends::TableName;

    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path).unwrap();
    }
    std::fs::create_dir_all(db_path).unwrap();

    open_database(TableName::max_index() + 1, db_path)
}

#[test]
fn tests_versioned_store_inmemory() {
    let mut db = InMemoryDatabase::empty();
    test_versioned_store(&mut db, 2, 10, 1000);
}

#[test]
fn tests_versioned_store_rocksdb() {
    let db_path = "__test_versioned_store";

    let mut db = empty_rocksdb(db_path).unwrap();
    test_versioned_store(&mut db, 2, 10, 1000);

    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path).unwrap();
    }
}
