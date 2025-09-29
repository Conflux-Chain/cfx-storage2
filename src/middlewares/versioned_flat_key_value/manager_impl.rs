use std::borrow::Borrow;

use crate::{
    backends::{DatabaseTrait, PendingTableName, TableReader},
    errors::Result,
    middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks},
    traits::{
        IsCompleted, KeyValueStoreBulksTrait, KeyValueStoreIterable, KeyValueStoreManager,
        KeyValueStoreRead, NeedNext,
    },
    StorageError,
};

use super::{
    get_versioned_key_latest, get_versioned_key_previous,
    history_indices::HistoryIndices,
    iter_history, iter_history_range,
    pending_part::{pending_schema::PendingKeyValueConfig, VersionedMap},
    table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema},
    HistoryIndexKey, PendingError, VersionedStore,
};

use crate::types::ValueEntry;
use std::collections::BTreeMap;

/// Enum explicitly distinguishing between view types
pub enum SnapshotView<'a, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>> {
    /// Pending view can be any pending version
    Pending(PendingSnapshot<'a, 'db, T, P>),
    /// Historical view can be any historical version
    Historical(HistoricalSnapshot<'db, T>),
}

/// Pending view contains unconfirmed updates combined with the latest historical snapshot (if the latest historical snapshot exists)
pub struct PendingSnapshot<'a, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
{
    pending: PendingUpdates<'a, T, P>,
    latest: Option<LatestHistoricalSnapshot<'db, T>>,
}

struct PendingUpdates<'a, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>> {
    commit_id: CommitID,
    inner: &'a VersionedMap<PendingKeyValueConfig<T, CommitID>, P>,
}

/// Explicitly distinguishes historical snapshot types (Latest/Previous) with different algorithms
pub enum HistoricalSnapshot<'db, T: VersionedKeyValueSchema> {
    Latest(LatestHistoricalSnapshot<'db, T>),
    Previous(PreviousHistoricalSnapshot<'db, T>),
}

/// Historical view for the latest historical version
pub struct LatestHistoricalSnapshot<'db, T: VersionedKeyValueSchema> {
    history_number: HistoryNumber,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
}

/// Historical view for previous (i.e., not the latest) historical versions
pub struct PreviousHistoricalSnapshot<'db, T: VersionedKeyValueSchema> {
    history_number: HistoryNumber,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'a, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
    KeyValueStoreIterable<T::Key, T::Value> for SnapshotView<'a, 'db, T, P>
{
    fn iter(&self) -> Result<impl Iterator<Item = (T::Key, T::Value)>> {
        let map = match self {
            SnapshotView::Pending(pending_snapshot) => {
                let mut map = if let Some(latest_history_snapshot) = &pending_snapshot.latest {
                    iter_history(
                        latest_history_snapshot.history_number,
                        &latest_history_snapshot.history_index_table,
                        None,
                    )?
                } else {
                    BTreeMap::new()
                };

                let pending_updates = &pending_snapshot.pending;
                let pending_map = pending_updates
                    .inner
                    .get_versioned_store(pending_updates.commit_id)?;
                for (k, v) in pending_map {
                    match v {
                        ValueEntry::Value(value_not_deleted) => {
                            map.insert(k.clone(), value_not_deleted.clone())
                        }
                        ValueEntry::Deleted => map.remove(&k),
                    };
                }

                map
            }
            SnapshotView::Historical(historical_snapshot) => match historical_snapshot {
                HistoricalSnapshot::Latest(latest_history_snapshot) => iter_history(
                    latest_history_snapshot.history_number,
                    &latest_history_snapshot.history_index_table,
                    None,
                )?,
                HistoricalSnapshot::Previous(previous_history_snapshot) => iter_history(
                    previous_history_snapshot.history_number,
                    &previous_history_snapshot.history_index_table,
                    Some(&previous_history_snapshot.change_history_table),
                )?,
            },
        };

        Ok(map.into_iter())
    }
}

impl<'a, 'db, T, P: DatabaseTrait<PendingTableName>> SnapshotView<'a, 'db, T, P>
where
    T: VersionedKeyValueSchema,
    T::Key: AsRef<[u8]>,
{
    pub fn iter_range(
        &self,
        lower_bound_incl: T::Key,
        upper_bound_excl: Option<T::Key>,
    ) -> Result<impl Iterator<Item = (T::Key, T::Value)>> {
        let res_map = match self {
            SnapshotView::Pending(pending_snapshot) => {
                let mut map = if let Some(latest_history_snapshot) = &pending_snapshot.latest {
                    iter_history_range(
                        latest_history_snapshot.history_number,
                        &latest_history_snapshot.history_index_table,
                        None,
                        lower_bound_incl.clone(),
                        upper_bound_excl.clone(),
                    )?
                } else {
                    BTreeMap::new()
                };

                let pending_updates = &pending_snapshot.pending;
                let pending_map = pending_updates.inner.get_versioned_store_range(
                    pending_updates.commit_id,
                    lower_bound_incl.clone(),
                    upper_bound_excl.clone(),
                )?;
                for (k, v) in pending_map {
                    match v {
                        ValueEntry::Value(value_not_deleted) => {
                            map.insert(k.clone(), value_not_deleted.clone())
                        }
                        ValueEntry::Deleted => map.remove(&k),
                    };
                }

                map
            }
            SnapshotView::Historical(historical_snapshot) => match historical_snapshot {
                HistoricalSnapshot::Latest(latest_history_snapshot) => iter_history_range(
                    latest_history_snapshot.history_number,
                    &latest_history_snapshot.history_index_table,
                    None,
                    lower_bound_incl.clone(),
                    upper_bound_excl.clone(),
                )?,
                HistoricalSnapshot::Previous(previous_history_snapshot) => iter_history_range(
                    previous_history_snapshot.history_number,
                    &previous_history_snapshot.history_index_table,
                    Some(&previous_history_snapshot.change_history_table),
                    lower_bound_incl.clone(),
                    upper_bound_excl.clone(),
                )?,
            },
        };

        Ok(res_map.into_iter())
    }
}

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value>
    for LatestHistoricalSnapshot<'db, T>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        get_versioned_key_latest(self.history_number, key, &self.history_index_table)
    }
}

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value>
    for PreviousHistoricalSnapshot<'db, T>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        get_versioned_key_previous(
            self.history_number,
            key,
            &self.history_index_table,
            &self.change_history_table,
        )
    }
}

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value>
    for HistoricalSnapshot<'db, T>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        match self {
            HistoricalSnapshot::Latest(latest_historical_snapshot) => {
                latest_historical_snapshot.get(key)
            }
            HistoricalSnapshot::Previous(previous_historical_snapshot) => {
                previous_historical_snapshot.get(key)
            }
        }
    }
}

impl<'a, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
    KeyValueStoreRead<T::Key, T::Value> for PendingSnapshot<'a, 'db, T, P>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        let pending_optv = self
            .pending
            .inner
            .get_versioned_key(&self.pending.commit_id, key)?;

        if let Some(pending_v) = pending_optv {
            Ok(pending_v.into_option())
        } else if let Some(latest) = &self.latest {
            latest.get(key)
        } else {
            Ok(None)
        }
    }
}

impl<'a, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
    KeyValueStoreRead<T::Key, T::Value> for SnapshotView<'a, 'db, T, P>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        match self {
            SnapshotView::Pending(pending_snapshot) => pending_snapshot.get(key),
            SnapshotView::Historical(historical_snapshot) => historical_snapshot.get(key),
        }
    }
}

impl<'a, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
    KeyValueStoreRead<T::Key, T::Value> for Option<SnapshotView<'a, 'db, T, P>>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        if let Some(view) = self {
            view.get(key)
        } else {
            Ok(None)
        }
    }
}

impl<'cache, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
    KeyValueStoreManager<T::Key, T::Value, CommitID, P> for VersionedStore<'cache, 'db, T, P>
{
    type Store<'a> = SnapshotView<'a, 'db, T, P> where Self: 'a;
    fn get_versioned_store<'s>(&'s self, commit: &CommitID) -> Result<Self::Store<'s>> {
        if self.pending_part.contains_commit_id(commit) {
            let latest_history: Option<LatestHistoricalSnapshot<'_, T>> =
                if let Some(history_commit) = self.pending_part.get_parent_of_root() {
                    Some(LatestHistoricalSnapshot {
                        history_number: self.get_history_number_by_commit_id(history_commit)?,
                        history_index_table: self.history_index_table.clone(),
                    })
                } else {
                    None
                };

            // TODO: checkout_current or not?
            self.pending_part.checkout_current(*commit)?;

            Ok(SnapshotView::Pending(PendingSnapshot {
                pending: PendingUpdates {
                    commit_id: *commit,
                    inner: &*self.pending_part,
                },
                latest: latest_history,
            }))
        } else {
            let history_number = self.get_history_number_by_commit_id(*commit)?;
            let latest_history_commit = self.pending_part.get_parent_of_root().expect("The parent of pending root should exists when there is at least one commit in the historical part.");

            if commit == &latest_history_commit {
                Ok(SnapshotView::Historical(HistoricalSnapshot::Latest(
                    LatestHistoricalSnapshot {
                        history_number,
                        history_index_table: self.history_index_table.clone(),
                    },
                )))
            } else {
                Ok(SnapshotView::Historical(HistoricalSnapshot::Previous(
                    PreviousHistoricalSnapshot {
                        history_number,
                        history_index_table: self.history_index_table.clone(),
                        change_history_table: self.change_history_table.clone(),
                    },
                )))
            }
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
                assert_eq!(target_commit, format!("{:?}", commit_id));
                self.iter_historical_changes_history_part(&mut accept, commit_id, key)
            }
            Err(other_err) => Err(StorageError::PendingError(other_err)),
        }
    }

    fn discard(&mut self, commit: CommitID, write_schema: &P::WriteSchema) -> Result<()> {
        if self.commit_id_table.get(&commit)?.is_some() {
            return Ok(());
        }

        Ok(self.pending_part.discard(commit, write_schema)?)
    }

    fn get_versioned_key(&self, commit: &CommitID, key: &T::Key) -> Result<Option<T::Value>> {
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
                assert_eq!(target_commit, format!("{:?}", commit));
                *commit
            }
            Err(other_err) => {
                return Err(StorageError::PendingError(other_err));
            }
        };

        let history_number = self.get_history_number_by_commit_id(history_commit)?;
        let latest_history_commit = self.pending_part.get_parent_of_root().expect("The parent of pending root should exists when there is at least one commit in the historical part.");

        self.get_historical_part(history_number, key, latest_history_commit == history_commit)
    }
}

// Helper methods used in trait implementations
impl<'cache, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
    VersionedStore<'cache, 'db, T, P>
{
    fn iter_historical_changes_one_range(
        &self,
        mut accept: impl FnMut(&CommitID, &T::Key, Option<&T::Value>) -> NeedNext,
        maybe_version_number: Option<HistoryNumber>,
        key: &T::Key,
        history_indices: HistoryIndices<T::Value>,
        end_version_number: HistoryNumber,
    ) -> Result<(IsCompleted, Option<HistoryNumber>)> {
        let version_numbers = if let Some(version_number) = maybe_version_number {
            history_indices.collect_versions_le(version_number, end_version_number)?
        } else {
            let mut all_version_numbers =
                history_indices.collect_versions_le(end_version_number, end_version_number)?;

            match all_version_numbers.pop() {
                None => {
                    return Err(StorageError::CorruptedHistoryIndices(
                        ("A record's all_version_numbers should contain at least one element.")
                            .to_string(),
                    ))
                }
                Some(largest_version_number) => {
                    if largest_version_number != end_version_number {
                        return Err(StorageError::CorruptedHistoryIndices(format!("A record's all_version_numbers's largest_version_number should be the end_version_number {} instead of {}", end_version_number, largest_version_number)));
                    }
                }
            }

            all_version_numbers
        };

        let start_version_number = version_numbers.first().cloned();

        for found_version_number in version_numbers.into_iter().rev() {
            let found_value = self
                .change_history_table
                .get_versioned_key(&found_version_number, key)?;
            let found_commit_id = self.history_number_table.get(&found_version_number)?;

            if let Some(found_commit_id) = found_commit_id {
                let need_next = accept(found_commit_id.borrow(), key, found_value.as_ref());
                if !need_next {
                    return Ok((false, None));
                }
            } else {
                return Err(StorageError::VersionNotFound);
            }
        }

        Ok((true, start_version_number))
    }

    fn iter_historical_changes_history_part(
        &self,
        mut accept: impl FnMut(&CommitID, &T::Key, Option<&T::Value>) -> NeedNext,
        commit_id: &CommitID,
        key: &T::Key,
    ) -> Result<IsCompleted> {
        let query_number = self.get_history_number_by_commit_id(*commit_id)?;

        let range_query_key = HistoryIndexKey(key.clone(), query_number);
        let mut prev_end_version_number =
            match self.history_index_table.iter(&range_query_key)?.next() {
                None => return Ok(true),
                Some(item) => {
                    let (history_index_k, history_indices) = item?;
                    let HistoryIndexKey(k, end_version_number) = history_index_k.as_ref().clone();
                    if &k != key {
                        return Ok(true);
                    }

                    let (this_range_is_completed, maybe_start_version_number) = self
                        .iter_historical_changes_one_range(
                            &mut accept,
                            Some(query_number),
                            key,
                            history_indices.into_owned(),
                            end_version_number,
                        )?;
                    if !this_range_is_completed {
                        return Ok(false);
                    }
                    if let Some(start_version_number) = maybe_start_version_number {
                        start_version_number
                    } else {
                        return Ok(true);
                    }
                }
            };

        loop {
            let this_end_version_number = prev_end_version_number;

            let history_index_k = HistoryIndexKey(key.clone(), this_end_version_number);
            prev_end_version_number = match self.history_index_table.get(&history_index_k)? {
                None => return Ok(true),
                Some(history_indices) => {
                    let (this_range_is_completed, maybe_start_version_number) = self
                        .iter_historical_changes_one_range(
                            &mut accept,
                            None,
                            key,
                            history_indices.into_owned(),
                            this_end_version_number,
                        )?;
                    if !this_range_is_completed {
                        return Ok(false);
                    }
                    if let Some(start_version_number) = maybe_start_version_number {
                        start_version_number
                    } else {
                        return Ok(true);
                    }
                }
            };

            assert!(prev_end_version_number < this_end_version_number);
        }
    }
}
