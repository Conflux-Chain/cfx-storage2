use std::borrow::Borrow;

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
    pending_part::{pending_schema::PendingKeyValueConfig, VersionedMap},
    table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema},
    HistoryIndexKey, PendingError, VersionedStore,
};

#[cfg(test)]
use crate::types::ValueEntry;
#[cfg(test)]
use std::collections::BTreeMap;

/// Enum explicitly distinguishing between view types
pub enum SnapshotView<'cache, 'db, T: VersionedKeyValueSchema> {
    /// Pending view can be any pending version
    Pending(PendingSnapshot<'cache, 'db, T>),
    /// Historical view can be any historical version
    Historical(HistoricalSnapshot<'db, T>),
}

/// Pending view contains unconfirmed updates combined with the latest historical snapshot (if the latest historical snapshot exists)
pub struct PendingSnapshot<'cache, 'db, T: VersionedKeyValueSchema> {
    pending: PendingUpdates<'cache, T>,
    latest: Option<HistoricalSnapshot<'db, T>>,
}

struct PendingUpdates<'cache, T: VersionedKeyValueSchema> {
    commit_id: CommitID,
    inner: &'cache VersionedMap<PendingKeyValueConfig<T, CommitID>>,
}

/// Historical view for any historical version (naive design: a single code path).
pub struct HistoricalSnapshot<'db, T: VersionedKeyValueSchema> {
    history_number: HistoryNumber,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'cache, 'db, T: VersionedKeyValueSchema> SnapshotView<'cache, 'db, T> {
    #[cfg(test)]
    pub fn iter(&self) -> Result<impl Iterator<Item = (T::Key, ValueEntry<T::Value>)>> {
        use super::iter_history;

        let map = match self {
            SnapshotView::Pending(pending_snapshot) => {
                let mut map = if let Some(latest_history_snapshot) = &pending_snapshot.latest {
                    iter_history(
                        latest_history_snapshot.history_number,
                        &latest_history_snapshot.history_index_table,
                        &latest_history_snapshot.change_history_table,
                    )?
                } else {
                    BTreeMap::new()
                };

                let pending_updates = &pending_snapshot.pending;
                let pending_map = pending_updates
                    .inner
                    .get_versioned_store(pending_updates.commit_id)?;
                for (k, v) in pending_map {
                    map.insert(k.clone(), v.clone());
                }

                map
            }
            SnapshotView::Historical(historical_snapshot) => iter_history(
                historical_snapshot.history_number,
                &historical_snapshot.history_index_table,
                &historical_snapshot.change_history_table,
            )?,
        };

        Ok(map.into_iter())
    }
}

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value>
    for HistoricalSnapshot<'db, T>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        get_versioned_key(
            self.history_number,
            key,
            &self.history_index_table,
            &self.change_history_table,
        )
    }
}

impl<'cache, 'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value>
    for PendingSnapshot<'cache, 'db, T>
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

impl<'cache, 'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value>
    for SnapshotView<'cache, 'db, T>
{
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        match self {
            SnapshotView::Pending(pending_snapshot) => pending_snapshot.get(key),
            SnapshotView::Historical(historical_snapshot) => historical_snapshot.get(key),
        }
    }
}

impl<'cache, 'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value>
    for Option<SnapshotView<'cache, 'db, T>>
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
    type Store<'a> = SnapshotView<'a, 'db, T> where Self: 'a;
    fn get_versioned_store<'a>(
        &'a self,
        commit: &CommitID,
        checkout_current: bool,
    ) -> Result<Self::Store<'a>> {
        if self.pending_part.contains_commit_id(commit) {
            let latest_history =
                if let Some(history_commit) = self.pending_part.get_parent_of_root() {
                    Some(HistoricalSnapshot {
                        history_number: self.get_history_number_by_commit_id(history_commit)?,
                        history_index_table: self.history_index_table.clone(),
                        change_history_table: self.change_history_table.clone(),
                    })
                } else {
                    None
                };

            if checkout_current {
                self.pending_part.checkout_current(*commit)?;
            }

            Ok(SnapshotView::Pending(PendingSnapshot {
                pending: PendingUpdates {
                    commit_id: *commit,
                    inner: &*self.pending_part,
                },
                latest: latest_history,
            }))
        } else {
            let history_number = self.get_history_number_by_commit_id(*commit)?;

            Ok(SnapshotView::Historical(HistoricalSnapshot {
                history_number,
                history_index_table: self.history_index_table.clone(),
                change_history_table: self.change_history_table.clone(),
            }))
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

        let mut range_query_key = HistoryIndexKey(key.clone(), query_number);
        loop {
            let next = self.history_index_table.iter(&range_query_key)?.next();
            let found_version_number = match next {
                None => return Ok(true),
                Some(Err(e)) => return Err(e.into()),
                Some(Ok((k, _))) if &k.as_ref().0 != key => return Ok(true),
                Some(Ok((k, _))) => {
                    let HistoryIndexKey(_, v) = k.as_ref();
                    *v
                }
            };

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

            if found_version_number == 0 {
                return Ok(true);
            }
            range_query_key = HistoryIndexKey(key.clone(), found_version_number - 1);
        }
    }
}
