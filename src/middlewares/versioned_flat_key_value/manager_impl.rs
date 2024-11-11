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

/// Snapshot of a key-value store (see `VersionedStore`).
/// Since the key-value store can be very large, the snapshot consists of two parts:
/// - A base snapshot (`history`), which represents the snapshot of the key-value store at a given `HistoryNumber`.
///   If `history` is `None`, it indicates that the historical part of the underlying database is empty,
///   meaning that the base snapshot contains no keys.
/// - Changes relative to the base snapshot (`pending_updates`), which represent modifications made after the base snapshot.
///
/// This snapshot can be in one of two cases:
/// - in the pending part of the underlying database:
///    - `pending_updates` is `Some`, representing changes relative to the base snapshot.
///    - The base snapshot (`history`) refers to the snapshot at the parent of the pending root (i.e., the latest node in the historical part).
/// - in the historical part of the underlying database:
///    - `pending_updates` is `None`, as there are no changes relative to the base snapshot.
///    - The base snapshot (`history`) refers to this snapshot itself.
///
/// Currently, usage and testing only cover cases where `history` and `pending_updates` are not both `None`.
pub struct SnapshotView<'db, T: VersionedKeyValueSchema> {
    pending_updates: Option<BTreeMap<T::Key, ValueEntry<T::Value>>>,
    history: Option<SnapshotHistorical<'db, T>>,
}

/// Snapshot of a key-value store in the historical part (see `VersionedStore`).
/// This struct represents the snapshot of the key-value store at a specific point in history, identified by `history_number`.
/// Since the key-value store can be very large, the snapshot holds an immutable reference to the underlying historical part.
/// More specifically, it provides access to both `history_index_table` and `change_history_table`,
/// which work together to enable efficient querying of the snapshot.
pub struct SnapshotHistorical<'db, T: VersionedKeyValueSchema> {
    /// The `HistoryNumber` that uniquely identifies this snapshot within the historical part of the underlying database.
    history_number: HistoryNumber,

    /// Immutable reference to the underlying historical part, including:
    /// - A mapping table from `(key, start_history_number)` to `history_number_indices`.
    ///   Currently, `history_number_indices` are an empty struct (i.e., `()`), and each `history_number` is its own `start_history_number`.
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    /// - A mapping table from `(history_number, key)` to the corresponding value.
    /// In mapping tables, the value is wrapped in an `Option` type, where `None` indicates a deletion.
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'db, T: VersionedKeyValueSchema> KeyValueStoreRead<T::Key, T::Value> for SnapshotView<'db, T> {
    /// Queries the value of a specified `Key` in this snapshot.
    ///
    /// # Parameters:
    /// - `key`: The `Key` to query.
    ///
    /// # Returns:
    /// A `Result` containing the queried value (of type `Option<Value>`,
    /// where `None` indicates that the key is not present in the key-value store at this snapshot) if successful,
    /// otherwise returns an error if the operation fails. Failures include:
    /// - An error occurs while reading data from the historical part of the underlying database.
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

impl<'cache, 'db, T: VersionedKeyValueSchema> KeyValueStoreManager<T::Key, T::Value, CommitID>
    for VersionedStore<'cache, 'db, T>
{
    /// Type for representing a snapshot of the key-value store.
    type Store = SnapshotView<'db, T>;

    /// Querys the snapshot of the key-value store at a specified `CommitID`.
    ///
    /// # Parameters:
    /// - `commit`: The `CommitID` of the snapshot to query.
    ///
    /// # Returns:
    /// A `Result` containing a snapshot of the key-value store at the given `CommitID` if successful,
    /// or an error if the operation fails. Failures include:
    /// - The `commit` does not exist in the underlying database.
    /// - There is a mismatch between the pending part and the historical part of the underlying database.
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

    /// Queries the modification history of a specified `Key`. Starts from the given `CommitID` and iterates changes backward.
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
    /// - The `commit_id` does not exist in the underlying database.
    /// - There is a mismatch between the pending part and the historical part of the underlying database.
    /// - An error occurs while reading data from the historical part of the underlying database.
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

    /// Queries the value of a specified `Key` in the snapshot of the key-value store at a specified `CommitID`.
    ///
    /// # Parameters:
    /// - `commit`: The `CommitID` of the snapshot to query.
    /// - `key`: The `Key` to query.
    ///
    /// # Returns:
    /// A `Result` containing the queried value (of type `Option<Value>`,
    /// where `None` indicates that the key is not present in the key-value store at this snapshot) if successful,
    /// otherwise returns an error if the operation fails. Failures include:
    /// - The `commit` does not exist in the underlying database.
    /// - There is a mismatch between the pending part and the historical part of the underlying database.
    /// - An error occurs while reading data from the historical part of the underlying database.
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
    /// Queries the modification history of a specified `Key`. Starts from the given `CommitID` in the historical part and iterates changes backward.
    /// This is a helper method for iter_historical_changes(), concentrating on the historical part.
    ///
    /// # Parameters:
    /// - `accept`: `impl FnMut(&CommitID, &T::Key, Option<&T::Value>) -> NeedNext`
    ///   Receives a change, including the `CommitID` where the change occurred, the `Key` that was changed, and an `Option<Value>`
    ///   (None means the key was deleted in this change).
    ///   Returns whether to continue iterating backward.
    /// - `commit_id`: The `CommitID` of the snapshot to start iterating backward. Should in the historical part.
    /// - `key`: The `Key` to query.
    ///
    /// # Returns:
    /// A `Result` containing an `IsCompleted` (i.e., a boolean indicating whether the iteration is completed) if successful,
    /// or an error if the operation fails. Failures include:
    /// - The `commit_id` does not exist in the historical part of the underlying database.
    /// - An error occurs while reading data from the historical part of the underlying database.
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