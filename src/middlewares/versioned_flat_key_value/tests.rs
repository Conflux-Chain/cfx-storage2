use ethereum_types::H256;

use super::{key_value_store_manager_impl::OneStore, table_schema::VersionedKeyValueSchema};
use crate::{
    backends::VersionedKVName,
    errors::Result,
    middlewares::{CommitID, PendingError},
    traits::{KeyValueStore, KeyValueStoreManager},
    StorageError,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaChaRng,
};

type MockOneStore<T> = BTreeMap<
    <T as VersionedKeyValueSchema>::Key,
    (Option<<T as VersionedKeyValueSchema>::Value>, bool),
>;

#[derive(Debug)]
struct MockVersionedStore<T: VersionedKeyValueSchema> {
    pending: MockTree<T>,
    history: HashMap<CommitID, (Option<CommitID>, MockOneStore<T>)>,
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
    store: MockOneStore<T>,
}

impl<T: VersionedKeyValueSchema> KeyValueStoreManager<T::Key, T::Value, CommitID>
    for MockVersionedStore<T>
{
    type Store = OneStore<T::Key, T::Value, CommitID>;

    fn get_versioned_store(&self, commit: &CommitID) -> Result<Self::Store> {
        if let Some(pending_res) = self.pending.tree.get(commit) {
            Ok(OneStore::from_mock_map(&pending_res.store))
        } else {
            if let Some((_, history_res)) = self.history.get(commit) {
                Ok(OneStore::from_mock_map(history_res))
            } else {
                Err(StorageError::CommitIDNotFound)
            }
        }
    }

    fn iter_historical_changes<'a>(
        &'a self,
        commit_id: &CommitID,
        key: &'a T::Key,
    ) -> Result<Box<dyn 'a + Iterator<Item = (CommitID, &T::Key, Option<T::Value>)>>> {
        let mut res = Vec::new();

        let mut current_node = self.pending.tree.get(commit_id);
        while let Some(node) = current_node {
            if let Some((value, true)) = node.store.get(key) {
                res.push((node.commit_id, key, value.clone()));
            }
            current_node = node.parent.map(|p| self.pending.tree.get(&p).unwrap());
        }

        let history_commit_id = if self.pending.tree.contains_key(commit_id) {
            if let Some(parent_of_pending) = self.pending.parent_of_root {
                parent_of_pending
            } else {
                assert!(self.history.is_empty());
                return Ok(Box::new(res.into_iter()));
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
                res.push((cid, key, value.clone()));
            }
            current_cid = parent_cid.clone();
        }

        Ok(Box::new(res.into_iter()))
    }

    fn discard(&mut self, commit: CommitID) -> Result<()> {
        if let Some(node) = self.pending.tree.remove(&commit) {
            if let Some(parent) = node.parent {
                let mut removed_but_children = VecDeque::new();
                removed_but_children.push_back(node);
                while !removed_but_children.is_empty() {
                    let removed_this_children = removed_but_children.pop_front().unwrap();
                    for child in removed_this_children.children.iter() {
                        removed_but_children.push_back(self.pending.tree.remove(child).unwrap());
                    }
                }
                assert_eq!(
                    self.pending
                        .tree
                        .get_mut(&parent)
                        .unwrap()
                        .children
                        .remove(&commit),
                    true
                );
                Ok(())
            } else {
                self.pending.tree.insert(commit, node);
                Err(StorageError::PendingError(
                    PendingError::RootShouldNotBeDiscarded,
                ))
            }
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
    last_store: &MockOneStore<T>,
    updates: BTreeMap<T::Key, Option<T::Value>>,
) -> MockOneStore<T> {
    let mut store: BTreeMap<_, _> = last_store
        .iter()
        .map(|(k, (opt_v, _))| (k.clone(), (opt_v.clone(), false)))
        .collect();
    for (k, opt_v) in updates.into_iter() {
        store.insert(k, (opt_v, true));
    }
    store
}

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
}

impl<T: VersionedKeyValueSchema> MockVersionedStore<T> {
    pub fn build(
        history_cids: UniqueVec<CommitID>,
        history_stores: Vec<BTreeMap<T::Key, Option<T::Value>>>,
    ) -> Self {
        assert_eq!(history_cids.len(), history_stores.len());
        let mut history: HashMap<_, _> = Default::default();
        let mut last_store = Default::default();
        let mut last_commit_id = None;
        for (commit_id, updates) in history_cids.items.iter().zip(history_stores.into_iter()) {
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
        history: HashMap<CommitID, (Option<CommitID>, MockOneStore<T>)>,
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
        let pending_non_root: Vec<_> = self
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
        pending_non_root
    }

    pub fn get_pending(&self) -> Vec<CommitID> {
        self.pending.tree.keys().cloned().into_iter().collect()
    }

    pub fn get_parent_of_root(&self) -> Option<CommitID> {
        self.pending.parent_of_root
    }

    pub fn get_history(&self) -> Vec<CommitID> {
        self.history.keys().cloned().into_iter().collect()
    }

    pub fn get_history_but_parent_of_root(&self) -> Vec<CommitID> {
        let mut history: BTreeSet<_> = self.history.keys().cloned().collect();
        if let Some(parent_of_root) = self.pending.parent_of_root {
            history.remove(&parent_of_root);
        }
        history.into_iter().collect()
    }

    pub fn get_commit_ids(&self) -> BTreeSet<CommitID> {
        self.history
            .keys()
            .cloned()
            .into_iter()
            .chain(self.pending.tree.keys().cloned().into_iter())
            .collect()
    }

    fn get_keys_on_path(&self, commit: &CommitID) -> Vec<T::Key> {
        if let Some(pending_res) = self.pending.tree.get(commit) {
            pending_res.store.keys().cloned().into_iter().collect()
        } else {
            if let Some((_, history_res)) = self.history.get(commit) {
                history_res.keys().cloned().into_iter().collect()
            } else {
                Vec::new()
            }
        }
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
        } else {
            if let Some(parent_commit_id) = parent_commit {
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
            let mut siblings = self
                .pending
                .tree
                .get(&parent_commit_id)
                .unwrap()
                .children
                .clone();
            assert_eq!(siblings.remove(&commit_id), true);
            for sibling in siblings.into_iter() {
                self.discard(sibling).unwrap();
            }

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

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
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

fn get_rng_for_test() -> ChaChaRng {
    ChaChaRng::from_seed([123; 32])
}

fn gen_opt_value(rng: &mut ChaChaRng) -> Option<u64> {
    let value_is_none = (rng.next_u32() % 3) == 0;
    if value_is_none {
        None
    } else {
        Some(rng.next_u64())
    }
}

fn select_vec_element<T: Clone>(rng: &mut ChaChaRng, vec: &[T]) -> T {
    assert!(!vec.is_empty());
    let num_elements = vec.len();
    vec[rng.next_u32() as usize % num_elements].clone()
}

fn gen_updates(
    rng: &mut ChaChaRng,
    previous_keys: &mut BTreeSet<u64>,
    num_gen_new_keys: usize,
    num_gen_previous_keys: usize,
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
    previous_keys.append(&mut new_keys);

    updates
}

fn gen_init(
    num_history: usize,
    rng: &mut ChaChaRng,
    max_num_new_keys: usize,
    max_num_previous_keys: usize,
) -> MockVersionedStore<TestSchema> {
    let mut history_cids = UniqueVec::new();
    for _ in 0..num_history << 4 {
        if history_cids.len() < num_history {
            history_cids.push(H256::random());
        } else {
            break;
        }
    }
    assert_eq!(history_cids.len(), num_history);

    let mut history_stores = Vec::new();
    let mut previous_keys = BTreeSet::new();
    for _ in 0..num_history {
        let num_new_keys = (rng.next_u32() as usize % max_num_new_keys) + 1;
        let num_previous_keys = (rng.next_u32() as usize % max_num_previous_keys) + 1;
        history_stores.push(gen_updates(
            rng,
            &mut previous_keys,
            num_new_keys,
            num_previous_keys,
        ));
    }

    MockVersionedStore::build(history_cids, history_stores)
}

fn gen_novel_commit_id(rng: &mut ChaChaRng, previous: &BTreeSet<CommitID>) -> CommitID {
    for _ in 0..1 << 4 {
        let novel = H256::random();
        if !previous.contains(&novel) {
            return novel;
        }
    }
    panic!()
}

fn gen_novel_u64(rng: &mut ChaChaRng, previous: &BTreeSet<u64>) -> u64 {
    for _ in 0..1 << 4 {
        let novel = rng.next_u64();
        if !previous.contains(&novel) {
            return novel;
        }
    }
    panic!()
}

fn gen_commit_id(
    rng: &mut ChaChaRng,
    mock_versioned_store: &MockVersionedStore<TestSchema>,
) -> (CommitIDType, CommitID) {
    let mut commit_id_types = vec![CommitIDType::Novel];
    if !mock_versioned_store.history.is_empty() {
        commit_id_types.push(CommitIDType::History);
    }
    if !mock_versioned_store.pending.tree.is_empty() {
        commit_id_types.push(CommitIDType::PendingRoot);
        if mock_versioned_store.pending.tree.len() > 1 {
            commit_id_types.push(CommitIDType::PendingNonRoot);
        }
    }
    let commit_id_type = select_vec_element(rng, &commit_id_types);
    let selected_range = match commit_id_type {
        CommitIDType::History => mock_versioned_store.get_history(),
        CommitIDType::PendingRoot => mock_versioned_store.get_pending_root(),
        CommitIDType::PendingNonRoot => mock_versioned_store.get_pending_non_root(),
        CommitIDType::Novel => {
            let previous = mock_versioned_store.get_commit_ids();
            return (commit_id_type, gen_novel_commit_id(rng, &previous));
        }
    };
    (commit_id_type, select_vec_element(rng, &selected_range))
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

fn gen_parent_commit(
    rng: &mut ChaChaRng,
    mock_versioned_store: &MockVersionedStore<TestSchema>,
    pending_only: bool,
) -> (ParentCommitType, Option<CommitID>) {
    let parent_types = if pending_only {
        assert!(!mock_versioned_store.pending.tree.is_empty());
        vec![ParentCommitType::Pending]
    } else {
        let mut parent_types = vec![
            ParentCommitType::ParentOfPendingRoot,
            ParentCommitType::Novel,
        ];
        if mock_versioned_store.get_parent_of_root().is_some() {
            parent_types.push(ParentCommitType::NoneButInvalid)
        }
        if !mock_versioned_store.pending.tree.is_empty() {
            parent_types.push(ParentCommitType::Pending)
        }
        if mock_versioned_store.history.len() > 1 {
            parent_types.push(ParentCommitType::HistoryButInvalid)
        }
        parent_types
    };
    let parent_type = select_vec_element(rng, &parent_types);
    let selected_range = match parent_type {
        ParentCommitType::Pending => mock_versioned_store.get_pending(),
        ParentCommitType::ParentOfPendingRoot => {
            return (parent_type, mock_versioned_store.get_parent_of_root())
        }
        ParentCommitType::NoneButInvalid => return (parent_type, None),
        ParentCommitType::HistoryButInvalid => {
            mock_versioned_store.get_history_but_parent_of_root()
        }
        ParentCommitType::Novel => {
            let previous = mock_versioned_store.get_commit_ids();
            return (parent_type, Some(gen_novel_commit_id(rng, &previous)));
        }
    };
    (parent_type, Some(select_vec_element(rng, &selected_range)))
}

fn get_previous_keys(
    parent_commit: Option<CommitID>,
    mock_versioned_store: &MockVersionedStore<TestSchema>,
) -> BTreeSet<u64> {
    if let Some(parent_cid) = parent_commit {
        mock_versioned_store
            .get_keys_on_path(&parent_cid)
            .into_iter()
            .collect()
    } else {
        Default::default()
    }
}

fn test_versioned_store(num_history: usize, num_pending: usize, num_operations: usize) {
    let mut rng = get_rng_for_test();
    let num_gen_new_keys = 10;
    let num_gen_previous_keys = 10;

    // init history part
    let mut mock_versioned_store = gen_init(
        num_history,
        &mut rng,
        num_gen_new_keys,
        num_gen_previous_keys,
    );

    // init pending part
    if num_pending > 0 {
        // add root
        let previous_cids = mock_versioned_store.get_history().into_iter().collect();
        let pending_root = gen_novel_commit_id(&mut rng, &previous_cids);
        let mut previous_keys = get_previous_keys(
            mock_versioned_store.pending.parent_of_root,
            &mock_versioned_store,
        );
        let updates = gen_updates(
            &mut rng,
            &mut previous_keys,
            num_gen_new_keys,
            num_gen_previous_keys,
        );
        mock_versioned_store
            .add_to_pending_part(
                mock_versioned_store.pending.parent_of_root,
                pending_root,
                updates,
            )
            .unwrap();

        // add non_root nodes
        for _ in 0..num_pending - 1 {
            let previous_cids = mock_versioned_store.get_commit_ids().into_iter().collect();
            let commit_id = gen_novel_commit_id(&mut rng, &previous_cids);
            let (parent_commit_type, parent_commit) =
                gen_parent_commit(&mut rng, &mock_versioned_store, true);
            assert_eq!(parent_commit_type, ParentCommitType::Pending);
            assert!(parent_commit.is_some());
            let mut previous_keys = get_previous_keys(parent_commit, &mock_versioned_store);
            let updates = gen_updates(
                &mut rng,
                &mut previous_keys,
                num_gen_new_keys,
                num_gen_previous_keys,
            );
            mock_versioned_store
                .add_to_pending_part(parent_commit, commit_id, updates)
                .unwrap();
        }
    }

    mock_versioned_store.check_consistency();

    let operations = vec![
        Operation::GetVersionedStore,
        Operation::IterHisoricalChanges,
        Operation::Discard,
        Operation::GetVersionedKey,
        Operation::AddToPendingPart,
        Operation::ConfirmedPendingToHistory,
    ];

    let mut operations_analyses = HashMap::new();
    for _ in 0..num_operations {
        let operation = select_vec_element(&mut rng, &operations);
        let (commit_id_type, commit_id) = gen_commit_id(&mut rng, &mock_versioned_store);

        let this_operation_is_ok = match operation {
            Operation::GetVersionedStore => {
                let mock_res = mock_versioned_store.get_versioned_store(&commit_id);
                match commit_id_type {
                    CommitIDType::Novel => {
                        assert_eq!(mock_res, Err(StorageError::CommitIDNotFound))
                    }
                    _ => assert!(mock_res.is_ok()),
                };
                mock_res.is_ok()
            }
            Operation::GetVersionedKey => {
                let mock_store = mock_versioned_store.get_versioned_store(&commit_id);
                let mock_keys = if let Ok(ref mock_store) = mock_store {
                    mock_store.get_keys()
                } else {
                    Default::default()
                };
                let (key_type, key) = gen_key(&mut rng, mock_keys);
                let mock_res = mock_versioned_store.get_versioned_key(&commit_id, &key);
                let mock_res_is_ok = mock_res.is_ok();
                match (commit_id_type, key_type) {
                    (CommitIDType::Novel, _) => {
                        assert_eq!(mock_res, Err(StorageError::CommitIDNotFound))
                    }
                    (_, KeyType::Exist) => assert_eq!(
                        mock_res.unwrap().unwrap(),
                        mock_store.unwrap().get(&key).unwrap().unwrap()
                    ),
                    (_, KeyType::Novel) => {
                        assert!(mock_res.unwrap().is_none());
                        assert!(mock_store.unwrap().get(&key).unwrap().is_none());
                    }
                };
                mock_res_is_ok
            }
            Operation::IterHisoricalChanges => {
                let keys_on_path = mock_versioned_store.get_keys_on_path(&commit_id);
                let (key_type, key) = gen_key(&mut rng, keys_on_path);
                let mock_res = mock_versioned_store.iter_historical_changes(&commit_id, &key);
                let mock_res_is_ok = mock_res.is_ok();
                match (commit_id_type, key_type) {
                    (CommitIDType::Novel, _) => {
                        if let Err(err) = mock_res {
                            assert_eq!(err, StorageError::CommitIDNotFound)
                        } else {
                            panic!()
                        }
                    }
                    (_, KeyType::Exist) => assert!(mock_res.unwrap().count() > 0),
                    (_, KeyType::Novel) => assert_eq!(mock_res.unwrap().count(), 0),
                };
                mock_res_is_ok
            }
            Operation::Discard => {
                let mock_res = mock_versioned_store.discard(commit_id);
                match commit_id_type {
                    CommitIDType::PendingNonRoot => assert!(mock_res.is_ok()),
                    CommitIDType::PendingRoot => assert_eq!(
                        mock_res,
                        Err(StorageError::PendingError(
                            PendingError::RootShouldNotBeDiscarded
                        ))
                    ),
                    _ => assert_eq!(
                        mock_res,
                        Err(StorageError::PendingError(PendingError::CommitIDNotFound(
                            commit_id
                        )))
                    ),
                };
                mock_res.is_ok()
            }
            Operation::AddToPendingPart => {
                let has_root_before_add = !mock_versioned_store.pending.tree.is_empty();
                let (parent_commit_type, parent_commit) =
                    gen_parent_commit(&mut rng, &mock_versioned_store, false);
                let mut previous_keys = get_previous_keys(parent_commit, &mock_versioned_store);
                let updates = gen_updates(
                    &mut rng,
                    &mut previous_keys,
                    num_gen_new_keys,
                    num_gen_previous_keys,
                );
                let mock_res =
                    mock_versioned_store.add_to_pending_part(parent_commit, commit_id, updates);
                let mock_res_is_ok = mock_res.is_ok();
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
                        StorageError::PendingError(PendingError::CommitIdAlreadyExists(commit_id))
                    ),
                    (ParentCommitType::Pending, CommitIDType::Novel) => assert!(mock_res.is_ok()),
                };
                mock_res_is_ok
            }
            Operation::ConfirmedPendingToHistory => {
                let mock_res = mock_versioned_store.confirmed_pending_to_history(commit_id);
                let mock_res_is_ok = mock_res.is_ok();
                match commit_id_type {
                    CommitIDType::PendingRoot | CommitIDType::PendingNonRoot => {
                        assert!(mock_res.is_ok());
                    }
                    _ => assert_eq!(
                        mock_res.unwrap_err(),
                        StorageError::PendingError(PendingError::CommitIDNotFound(commit_id))
                    ),
                };
                mock_res_is_ok
            }
        };
        *operations_analyses
            .entry((operation, this_operation_is_ok))
            .or_insert(0) += 1;
    }

    dbg!("operations_analyses");

    print!("{:<20}", "");
    for op in &operations {
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
        for op in &operations {
            let count = operations_analyses.get(&(op.clone(), flag)).unwrap_or(&0);
            print!("{:>15}", count);
        }
        println!();
    }
}

#[test]
fn tests_versioned_store() {
    test_versioned_store(20, 100, 1000);
}
