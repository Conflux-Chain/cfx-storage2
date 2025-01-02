use std::{borrow::Borrow, collections::BTreeMap};

use crate::{
    backends::TableReader,
    errors::Result,
    middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks},
    traits::{
        IsCompleted, KeyValueStoreBulksTrait, KeyValueStoreManager, KeyValueStoreRead, NeedNext,
    },
    types::ValueEntry,
    StorageError,
};

use super::{
    get_versioned_key,
    table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema},
    HistoryIndexKey, PendingError, VersionedStore,
};

pub struct SnapshotView<'db, T: VersionedKeyValueSchema> {
    pending_updates: Option<BTreeMap<T::Key, ValueEntry<T::Value>>>,
    history: Option<SnapshotHistorical<'db, T>>,
}

#[cfg(test)]
const MIN_HISTORY_NUMBER_MINUS_ONE: u64 = 0;

impl<'db, T: VersionedKeyValueSchema> SnapshotView<'db, T> {
    #[cfg(test)]
    fn iter_history(&self) -> Result<BTreeMap<T::Key, ValueEntry<T::Value>>> {
        if let Some(ref history) = self.history {
            let (key_with_history_number, _) =
                match history.history_index_table.iter_from_start()?.next() {
                    Some(item) => item.unwrap(),
                    None => return Ok(BTreeMap::new()),
                };

            let HistoryIndexKey(mut key, mut history_number) =
                key_with_history_number.as_ref().clone();

            let mut history_map = BTreeMap::new();

            loop {
                let found_version_number = if history_number <= history.history_number {
                    history_number
                } else {
                    let range_query_key = HistoryIndexKey(key.clone(), history.history_number);
                    match history.history_index_table.iter(&range_query_key)?.next() {
                        None => break,
                        Some(Err(e)) => return Err(e.into()),
                        Some(Ok((next_key_with_history_number, _)))
                            if next_key_with_history_number.as_ref().0 != key =>
                        {
                            HistoryIndexKey(key, history_number) =
                                next_key_with_history_number.as_ref().clone();
                            continue;
                        }
                        Some(Ok((k, indices))) => {
                            let HistoryIndexKey(_, found_history_number) = k.as_ref();
                            indices.as_ref().last(*found_history_number)
                        }
                    }
                };

                let value = history
                    .change_history_table
                    .get_versioned_key(&found_version_number, &key)?;

                history_map.insert(key.clone(), ValueEntry::from_option(value));

                let next_range_query_key =
                    HistoryIndexKey(key.clone(), MIN_HISTORY_NUMBER_MINUS_ONE);
                match history
                    .history_index_table
                    .iter(&next_range_query_key)?
                    .next()
                {
                    None => break,
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok((next_key_with_history_number, _))) => {
                        HistoryIndexKey(key, history_number) =
                            next_key_with_history_number.as_ref().clone();
                    }
                }
            }

            Ok(history_map)
        } else {
            Ok(BTreeMap::new())
        }
    }

    #[cfg(test)]
    pub fn iter(&self) -> Result<impl Iterator<Item = (T::Key, ValueEntry<T::Value>)>> {
        let mut map = self.iter_history()?;

        if let Some(ref pending_map) = self.pending_updates {
            for (k, v) in pending_map {
                map.insert(k.clone(), v.clone());
            }
        }

        Ok(map.into_iter())
    }
}

pub struct SnapshotHistorical<'db, T: VersionedKeyValueSchema> {
    history_number: HistoryNumber,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value> for SnapshotView<'db, T> {
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        if let Some(opt_v) = self.pending_updates.as_ref().and_then(|u| u.get(key)) {
            return Ok(opt_v.to_option());
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

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value>
    for Option<SnapshotView<'db, T>>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        if let Some(view) = self {
            view.get(key)
        } else {
            Ok(None)
        }
    }
}

impl<'cache, 'db, T: VersionedKeyValueSchema> KeyValueStoreManager<T::Key, T::Value, CommitID>
    for VersionedStore<'cache, 'db, T>
{
    type Store = SnapshotView<'db, T>;
    fn get_versioned_store(&self, commit: &CommitID) -> Result<Self::Store> {
        let pending_res = self.pending_part.get_versioned_store(*commit);
        match pending_res {
            Ok(pending_map) => {
                let history = if let Some(history_commit) = self.pending_part.get_parent_of_root() {
                    Some(SnapshotHistorical {
                        history_number: self.get_history_number_by_commit_id(history_commit)?,
                        history_index_table: self.history_index_table.clone(),
                        change_history_table: self.change_history_table.clone(),
                    })
                } else {
                    None
                };
                Ok(SnapshotView {
                    pending_updates: Some(pending_map),
                    history,
                })
            }
            Err(PendingError::CommitIDNotFound(target_commit_id)) => {
                assert_eq!(target_commit_id, *commit);
                let history = SnapshotHistorical {
                    history_number: self.get_history_number_by_commit_id(*commit)?,
                    history_index_table: self.history_index_table.clone(),
                    change_history_table: self.change_history_table.clone(),
                };
                Ok(SnapshotView {
                    pending_updates: None,
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
        if self.commit_id_table.get(&commit)?.is_some() {
            return Ok(());
        }

        Ok(self.pending_part.discard(commit)?)
    }

    fn get_versioned_key(&self, commit: &CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        // let pending_res = self.pending_part.get_versioned_key_with_checkout(commit, key); // this will checkout_current
        let pending_res = self.pending_part.get_versioned_key(commit, key);
        let history_commit = match pending_res {
            Ok(Some(value)) => {
                return Ok(value.into_option());
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
                break;
            }

            let found_version_number = indices.as_ref().last(*history_number);
            let found_value = self
                .change_history_table
                .get_versioned_key(&found_version_number, key)?;
            let found_commit_id = self.history_number_table.get(&found_version_number)?;
            if let Some(found_commit_id) = found_commit_id {
                let need_next = accept(found_commit_id.borrow(), key, found_value.as_ref());
                if !need_next {
                    return Ok(false);
                }
            } else {
                return Err(StorageError::VersionNotFound);
            }
        }
        Ok(true)
    }
}
