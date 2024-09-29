mod pending_part;
mod serde;
mod table_schema;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::marker::PhantomData;

use parking_lot::RwLock;
pub use pending_part::PendingError;

use self::pending_part::pending_schema::PendingKeyValueConfig;
use self::table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema};
use pending_part::VersionedHashMap;

use super::commit_id_schema::HistoryNumberSchema;
use super::ChangeKey;
use super::CommitIDSchema;
use crate::backends::{TableReader, WriteSchemaTrait};
use crate::errors::Result;
use crate::middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks};
use crate::traits::{KeyValueStore, KeyValueStoreBulksTrait, KeyValueStoreManager};
use crate::StorageError;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct HistoryIndexKey<K: Clone>(K, HistoryNumber);

pub type HistoryChangeKey<K> = ChangeKey<HistoryNumber, K>;

#[derive(Clone, Debug)]
pub struct HistoryIndices;
impl HistoryIndices {
    fn last(&self, offset: HistoryNumber) -> HistoryNumber {
        offset
    }
}

const MIN_HISTORY_NUMBER_MINUS_ONE: HistoryNumber = 0;

pub struct VersionedStore<'db, T: VersionedKeyValueSchema> {
    pending_part: &'db mut VersionedHashMap<PendingKeyValueConfig<T, CommitID>>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    history_number_table: TableReader<'db, HistoryNumberSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
    history_min_key: RwLock<Option<T::Key>>,
}

impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
    fn get_history_number_by_commit_id(&self, commit: CommitID) -> Result<HistoryNumber> {
        if let Some(value) = self.commit_id_table.get(&commit)? {
            Ok(value.into_owned())
        } else {
            Err(StorageError::CommitIDNotFound)
        }
    }

    pub fn add_to_pending_part(
        &mut self,
        parent_commit: Option<CommitID>,
        commit: CommitID,
        updates: BTreeMap<T::Key, Option<T::Value>>,
    ) -> Result<()> {
        Ok(self.pending_part.add_node(updates, commit, parent_commit)?)
    }

    pub fn get_historical_part(
        &self,
        query_version_number: HistoryNumber,
        key: &T::Key,
    ) -> Result<Option<T::Value>> {
        let range_query_key = HistoryIndexKey(key.clone(), query_version_number);

        let found_version_number = match self.history_index_table.iter(&range_query_key)?.next() {
            None => {
                return Ok(None);
            }
            Some(Err(e)) => {
                return Err(e.into());
            }
            Some(Ok((k, _))) if &k.as_ref().0 != key => {
                return Ok(None);
            }
            Some(Ok((k, indices))) => {
                let HistoryIndexKey(_, history_number) = k.as_ref();
                // let offset = target_history_number - history_number;
                indices.as_ref().last(*history_number)
            }
        };

        self.change_history_table
            .get_versioned_key(&found_version_number, key)
    }

    pub fn find_larger_key(&self, key: &T::Key) -> Result<Option<T::Key>> {
        // todo: here correct?
        let range_query_key = HistoryIndexKey(key.clone(), MIN_HISTORY_NUMBER_MINUS_ONE);

        match self.history_index_table.iter(&range_query_key)?.next() {
            None => Ok(None),
            Some(Err(e)) => Err(e.into()),
            Some(Ok((k, _))) if &k.as_ref().0 > key => Ok(Some(k.as_ref().0.to_owned())),
            _ => unreachable!(
                "iter((key, MIN_HISTORY_NUMBER_MINUS_ONE)) should not result in k <= key"
            ),
        }
    }
}

fn need_update_min_key<K: Ord>(original_min: Option<&K>, challenge_min: &K) -> bool {
    if let Some(original_min) = original_min {
        original_min > challenge_min
    } else {
        true
    }
}

impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
    fn confirmed_pending_to_history(
        &mut self,
        new_root_commit_id: CommitID,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()> {
        // old root..=new root's parent
        let (start_height, confirmed_ids_maps) =
            self.pending_part.change_root(new_root_commit_id)?;

        let mut confirmed_min_key = None;

        for (delta_height, (confirmed_commit_id, updates)) in
            confirmed_ids_maps.into_iter().enumerate()
        {
            let height = (start_height + delta_height) as u64;
            let history_number = height + 1;

            assert!(self.commit_id_table.get(&confirmed_commit_id)?.is_none());

            let commit_id_table_op = (
                Cow::Owned(confirmed_commit_id),
                Some(Cow::Owned(history_number)),
            );
            write_schema.write::<CommitIDSchema>(commit_id_table_op);

            let history_number_table_op = (
                Cow::Owned(history_number),
                Some(Cow::Owned(confirmed_commit_id)),
            );
            write_schema.write::<HistoryNumberSchema>(history_number_table_op);

            let history_indices_table_op = updates.keys().map(|key| {
                (
                    Cow::Owned(HistoryIndexKey(key.clone(), history_number)),
                    Some(Cow::Owned(HistoryIndices)),
                )
            });
            write_schema.write_batch::<HistoryIndicesTable<T>>(history_indices_table_op);

            if let Some(this_min_k) = updates.keys().min().cloned() {
                let update_min_k = need_update_min_key(confirmed_min_key.as_ref(), &this_min_k);
                if update_min_k {
                    confirmed_min_key = Some(this_min_k);
                }
            }

            self.change_history_table
                .commit(history_number, updates.into_iter(), write_schema)?;
        }

        if let Some(confirmed_min_k) = confirmed_min_key {
            let previous_min_key = self.history_min_key.read().clone();
            let update_min_k = need_update_min_key(previous_min_key.as_ref(), &confirmed_min_k);
            if update_min_k {
                *self.history_min_key.write() = Some(confirmed_min_k);
            }
        }

        Ok(())
    }
}

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
        let mut key_opt = self.history_min_key.read().as_ref().cloned();
        let mut map = BTreeMap::new();
        while let Some(key) = key_opt {
            let value = self.get_historical_part(query_number, &key)?;
            if let Some(value) = value {
                map.insert(key.clone(), value);
            }
            key_opt = self.find_larger_key(&key)?;
        }
        Ok(map)
    }
}

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

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreManager<T::Key, T::Value, CommitID>
    for VersionedStore<'db, T>
{
    type Store = OneStore<T::Key, T::Value, CommitID>;
    fn get_versioned_store(&self, commit: &CommitID) -> Result<Self::Store> {
        let pending_res = self.pending_part.get_versioned_store(*commit);
        match pending_res {
            Ok(pending_map) => Ok(OneStore::from_map(pending_map)),
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
        // let pending_res = self.pending_part.get_versioned_key(commit, key); // this will checkout_current
        let pending_res = self.pending_part.get_versioned_key(commit, key); // this does not checkout_current
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
