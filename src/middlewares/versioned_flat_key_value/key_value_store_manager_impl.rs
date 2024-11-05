use std::{borrow::Borrow, collections::BTreeMap};

use crate::{
    backends::TableReader,
    errors::Result,
    middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks},
    traits::{
        IsCompleted, KeyValueStoreBulksTrait, KeyValueStoreManager, KeyValueStoreRead, NeedNext,
    },
    StorageError,
};

use super::{
    get_versioned_key,
    table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema},
    HistoryIndexKey, PendingError, VersionedStore,
};

pub struct OneStore<'db, T: VersionedKeyValueSchema> {
    updates: Option<BTreeMap<T::Key, Option<T::Value>>>,
    history: Option<OneStoreHistory<'db, T>>,
}

pub struct OneStoreHistory<'db, T: VersionedKeyValueSchema> {
    history_number: HistoryNumber,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value> for OneStore<'db, T> {
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        if let Some(updates) = &self.updates {
            if let Some(opt_v) = updates.get(key) {
                return Ok(opt_v.clone());
            }
        }

        if let Some(history) = &self.history {
            get_versioned_key(
                history.history_number,
                key,
                &history.history_index_table,
                &history.change_history_table,
            )
        } else {
            Ok(None)
        }
    }
}

// impl<'db, C: 'static, T: VersionedKeyValueSchema> KeyValueStoreIterable<T::Key, T::Value>
//     for OneStore<'db, C, T>
// {
//     fn iter<'a>(&'a self, key: &T::Key) -> Result<impl 'a + Iterator<Item = (&T::Key, &T::Value)>> {
//         todo!()
//     }
// }

// Trait methods implementation
impl<'cache, 'db, T: VersionedKeyValueSchema> KeyValueStoreManager<T::Key, T::Value, CommitID>
    for VersionedStore<'cache, 'db, T>
{
    type Store = OneStore<'db, T>;
    fn get_versioned_store(&self, commit: &CommitID) -> Result<Self::Store> {
        let pending_res = self.pending_part.get_versioned_store(*commit);
        match pending_res {
            Ok(pending_map) => {
                let history = if let Some(history_commit) = self.pending_part.get_parent_of_root() {
                    Some(OneStoreHistory {
                        history_number: self.get_history_number_by_commit_id(history_commit)?,
                        history_index_table: self.history_index_table.clone(),
                        change_history_table: self.change_history_table.clone(),
                    })
                } else {
                    None
                };
                Ok(OneStore {
                    updates: Some(pending_map),
                    history,
                })
            }
            Err(PendingError::CommitIDNotFound(target_commit_id)) => {
                assert_eq!(target_commit_id, *commit);
                let history = OneStoreHistory {
                    history_number: self.get_history_number_by_commit_id(*commit)?,
                    history_index_table: self.history_index_table.clone(),
                    change_history_table: self.change_history_table.clone(),
                };
                Ok(OneStore {
                    updates: None,
                    history: Some(history),
                })
            }
            Err(other_err) => Err(StorageError::PendingError(other_err)),
        }
    }

    fn iter_historical_changes(
        &self,
        mut accept: impl FnMut(&CommitID, &T::Key, Option<&T::Value>) -> NeedNext,
        commit_id: &CommitID,
        key: &T::Key,
    ) -> Result<IsCompleted> {
        let pending_res = self
            .pending_part
            .iter_historical_changes(&mut accept, commit_id, key);
        match pending_res {
            Ok(false) => Ok(false),
            Ok(true) => {
                if let Some(history_commit) = self.pending_part.get_parent_of_root() {
                    self.iter_historical_changes_history_part(&mut accept, &history_commit, key)
                } else {
                    Ok(true)
                }
            }
            Err(PendingError::CommitIDNotFound(target_commit)) => {
                assert_eq!(target_commit, *commit_id);
                self.iter_historical_changes_history_part(&mut accept, &target_commit, key)
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
impl<'cache, 'db, T: VersionedKeyValueSchema> VersionedStore<'cache, 'db, T> {
    fn iter_historical_changes_history_part(
        &self,
        mut accept: impl FnMut(&CommitID, &T::Key, Option<&T::Value>) -> NeedNext,
        commit_id: &CommitID,
        key: &T::Key,
    ) -> Result<IsCompleted> {
        let query_number = self.get_history_number_by_commit_id(*commit_id)?;

        let range_query_key = HistoryIndexKey(key.clone(), query_number);
        for item in self.history_index_table.iter(&range_query_key)? {
            let (k_with_history_number, indices) = item?;
            let HistoryIndexKey(k, history_number) = k_with_history_number.as_ref();
            if k != key {
                return Ok(true);
            } else {
                let found_version_number = indices.as_ref().last(*history_number);
                let found_value = self
                    .change_history_table
                    .get_versioned_key(&found_version_number, key)?;
                let found_commit_id = self.history_number_table.get(&found_version_number)?;
                if found_commit_id.is_none() {
                    return Err(StorageError::VersionNotFound);
                }
                if !accept(found_commit_id.unwrap().borrow(), key, found_value.as_ref()) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    // fn get_versioned_store_history_part(
    //     &self,
    //     commit_id: &CommitID,
    // ) -> Result<BTreeMap<T::Key, T::Value>> {
    //     let query_number = self.get_history_number_by_commit_id(*commit_id)?;
    //     let mut key_opt = todo!();
    //     let mut map = BTreeMap::new();
    //     while let Some(key) = key_opt {
    //         let value = self.get_historical_part(query_number, &key)?;
    //         if let Some(value) = value {
    //             map.insert(key.clone(), value);
    //         }
    //         key_opt = self.find_larger_historical_key(&key)?;
    //     }
    //     Ok(map)
    // }
}
