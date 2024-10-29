use std::{collections::BTreeMap, marker::PhantomData};

use crate::{
    errors::Result,
    middlewares::CommitID,
    traits::{KeyValueStore, KeyValueStoreBulksTrait, KeyValueStoreManager},
    StorageError,
};

use super::{table_schema::VersionedKeyValueSchema, HistoryIndexKey, PendingError, VersionedStore};

#[cfg_attr(test, derive(PartialEq, Debug))]
pub struct OneStore<K: Ord, V: Clone, C> {
    map: BTreeMap<K, V>,
    _marker: PhantomData<C>,
}

impl<K: Ord, V: Clone, C> OneStore<K, V, C> {
    pub fn from_map(map: BTreeMap<K, V>) -> Self {
        OneStore {
            map,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
impl<K: Ord + Clone, V: Clone, C> OneStore<K, V, C> {
    pub fn from_mock_map(map: &BTreeMap<K, (Option<V>, bool)>) -> Self {
        let inner_map = map
            .iter()
            .filter_map(|(k, (opt_v, _))| opt_v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect();
        Self::from_map(inner_map)
    }

    pub fn get_keys(&self) -> Vec<K> {
        self.map.keys().cloned().into_iter().collect()
    }
}

impl<K: 'static + Ord, V: 'static + Clone, C: 'static> KeyValueStore<K, V, C>
    for OneStore<K, V, C>
{
    fn get(&self, key: &K) -> Result<Option<V>> {
        Ok(self.map.get(key).cloned())
    }

    fn iter<'a>(&'a self, key: &K) -> Result<impl 'a + Iterator<Item = (&K, &V)>> {
        Ok(self.map.range(key..))
    }

    fn commit(self, commit: C, changes: impl Iterator<Item = (K, V)>) {
        todo!()
    }
}

// Trait methods implementation
impl<'db, T: VersionedKeyValueSchema> KeyValueStoreManager<T::Key, T::Value, CommitID>
    for VersionedStore<'db, T>
{
    type Store = OneStore<T::Key, T::Value, CommitID>;
    fn get_versioned_store(&self, commit: &CommitID) -> Result<Self::Store> {
        let pending_res = self.pending_part.get_versioned_store(*commit);
        match pending_res {
            Ok(pending_map) => {
                let mut history_res =
                    if let Some(history_commit) = self.pending_part.get_parent_of_root() {
                        self.get_versioned_store_history_part(&history_commit)?
                    } else {
                        BTreeMap::new()
                    };
                for (k, v) in pending_map.into_iter() {
                    if let Some(value) = v {
                        history_res.insert(k, value);
                    } else {
                        history_res.remove(&k);
                    }
                }
                Ok(OneStore::from_map(history_res))
            }
            Err(PendingError::CommitIDNotFound(target_commit_id)) => {
                assert_eq!(target_commit_id, *commit);
                Ok(OneStore::from_map(
                    self.get_versioned_store_history_part(commit)?,
                ))
            }
            Err(other_err) => Err(StorageError::PendingError(other_err)),
        }
    }

    #[allow(clippy::type_complexity)]
    fn iter_historical_changes<'a>(
        &'a self,
        commit_id: &CommitID,
        key: &'a T::Key,
    ) -> Result<Box<dyn 'a + Iterator<Item = (CommitID, &T::Key, Option<T::Value>)>>> {
        let pending_res = self.pending_part.iter_historical_changes(commit_id, key);
        match pending_res {
            Ok(pending_iter) => {
                if let Some(history_commit) = self.pending_part.get_parent_of_root() {
                    let history_iter =
                        self.iter_historical_changes_history_part(&history_commit, key)?;
                    Ok(Box::new(pending_iter.chain(history_iter)))
                } else {
                    Ok(Box::new(pending_iter))
                }
            }
            Err(PendingError::CommitIDNotFound(target_commit)) => {
                assert_eq!(target_commit, *commit_id);
                let history_iter =
                    self.iter_historical_changes_history_part(&target_commit, key)?;
                Ok(Box::new(history_iter))
            }
            Err(other_err) => Err(StorageError::PendingError(other_err)),
        }
    }

    fn discard(&mut self, commit: CommitID) -> Result<()> {
        Ok(self.pending_part.discard(commit)?)
    }

    fn get_versioned_key(&self, commit: &CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        // let pending_res = self.pending_part.get_versioned_key_with_checkout(commit, key); // this will checkout_current
        let pending_res = self.pending_part.get_versioned_key(commit, key);
        let history_commit = match pending_res {
            Ok(Some(value)) => {
                return Ok(value);
            }
            Ok(None) => {
                if let Some(commit) = self.pending_part.get_parent_of_root() {
                    commit
                } else {
                    return Ok(None);
                }
            }
            Err(PendingError::CommitIDNotFound(target_commit)) => {
                assert_eq!(target_commit, *commit);
                target_commit
            }
            Err(other_err) => {
                return Err(StorageError::PendingError(other_err));
            }
        };

        let history_number = self.get_history_number_by_commit_id(history_commit)?;
        self.get_historical_part(history_number, key)
    }
}

// Helper methods used in trait implementations
impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
    fn iter_historical_changes_history_part<'a>(
        &'a self,
        commit_id: &CommitID,
        key: &'a T::Key,
    ) -> Result<impl 'a + Iterator<Item = (CommitID, &T::Key, Option<T::Value>)>> {
        let query_number = self.get_history_number_by_commit_id(*commit_id)?;

        let mut num_items = 0;
        let range_query_key = HistoryIndexKey(key.clone(), query_number);
        for item in self.history_index_table.iter(&range_query_key)? {
            let (k_with_history_number, indices) = item?;
            let HistoryIndexKey(k, history_number) = k_with_history_number.as_ref();
            if k != key {
                break;
            } else {
                let found_version_number = indices.as_ref().last(*history_number);
                let found_value = self
                    .change_history_table
                    .get_versioned_key(&found_version_number, key)?;
                let found_commit_id = self.history_number_table.get(&found_version_number)?;
                if found_commit_id.is_none() {
                    return Err(StorageError::VersionNotFound);
                }
                num_items += 1;
            }
        }

        let history_iter = self
            .history_index_table
            .iter(&range_query_key)?
            .take(num_items)
            .map(move |item| {
                let (k_with_history_number, indices) =
                    item.expect("previous for + take() should truncate before err");
                let HistoryIndexKey(k, history_number) = k_with_history_number.as_ref();
                assert_eq!(k, key);
                let found_version_number = indices.as_ref().last(*history_number);
                let found_value = self
                    .change_history_table
                    .get_versioned_key(&found_version_number, key)
                    .expect("previous for + take() should truncate before err");
                let found_commit_id = self
                    .history_number_table
                    .get(&found_version_number)
                    .expect("previous for + take() should truncate before err");
                (found_commit_id.unwrap().into_owned(), key, found_value)
            });
        Ok(history_iter)
    }

    fn get_versioned_store_history_part(
        &self,
        commit_id: &CommitID,
    ) -> Result<BTreeMap<T::Key, T::Value>> {
        let query_number = self.get_history_number_by_commit_id(*commit_id)?;
        let mut key_opt = todo!();
        let mut map = BTreeMap::new();
        while let Some(key) = key_opt {
            let value = self.get_historical_part(query_number, &key)?;
            if let Some(value) = value {
                map.insert(key.clone(), value);
            }
            key_opt = self.find_larger_historical_key(&key)?;
        }
        Ok(map)
    }
}
