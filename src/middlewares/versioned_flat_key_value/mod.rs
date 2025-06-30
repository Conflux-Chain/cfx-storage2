mod history_indices;
mod history_indices_cache;
mod manager_impl;
mod pending_part;
mod serde;
pub mod table_schema;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub use history_indices::PushError;
pub use manager_impl::SnapshotView;
use parking_lot::Mutex;
pub use pending_part::PendingError;

#[cfg(test)]
pub use tests::{empty_rocksdb, gen_random_commit_id, gen_updates, get_rng_for_test};

use self::history_indices::LATEST;
use self::history_indices_cache::HistoryIndexCache;
use self::pending_part::pending_schema::PendingKeyValueConfig;
use self::table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema};
use pending_part::VersionedMap;

use super::commit_id_schema::HistoryNumberSchema;
use super::ChangeKey;
use super::CommitIDSchema;
use crate::backends::{DatabaseTrait, TableRead, TableReader, WriteSchemaTrait};
use crate::errors::Result;
use crate::middlewares::commit_id_schema::height_to_history_number;
use crate::middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks};
use crate::traits::KeyValueStoreBulksTrait;
use crate::StorageError;

pub type VersionedStoreCache<Schema> = VersionedMap<PendingKeyValueConfig<Schema, CommitID>>;

use crate::types::ValueEntry;

/// Key for accessing version history records in storage.
///
/// Consists of two components:
/// 1. The database `key` being tracked
/// 2. A version specifier which is either:
///    - `LATEST` for the latest (mutable) record
///    - An `end_version_number` for a previous (immutable) record
///
/// Used in conjunction with [`history_indices::HistoryIndices`] to maintain version history through chained records.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct HistoryIndexKey<K: Clone>(K, HistoryNumber);

pub type HistoryChangeKey<K> = ChangeKey<HistoryNumber, K>;

pub struct VersionedStore<'db, T: VersionedKeyValueSchema> {
    pending_part: Arc<Mutex<VersionedMap<PendingKeyValueConfig<T, CommitID>>>>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    history_number_table: TableReader<'db, HistoryNumberSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
    pub fn checkout_current(&mut self, commit: CommitID) -> Result<()> {
        let pending_guard = self.pending_part.lock();
        Ok(pending_guard.checkout_current(commit)?)
    }

    pub fn into_pending_part(self) -> Arc<Mutex<VersionedMap<PendingKeyValueConfig<T, CommitID>>>> {
        self.pending_part
    }

    pub fn new<D: DatabaseTrait>(
        db: Arc<D>,
        pending_part: Arc<Mutex<VersionedMap<PendingKeyValueConfig<T, CommitID>>>>,
    ) -> Result<Self> {
        let history_index_table = Arc::new(db.view::<HistoryIndicesTable<T>>()?);
        let commit_id_table = Arc::new(db.view::<CommitIDSchema>()?);
        let history_number_table = Arc::new(db.view::<HistoryNumberSchema>()?);
        let change_history_table =
            KeyValueStoreBulks::new(Arc::new(db.view::<HistoryChangeTable<T>>()?));

        let versioned_store = VersionedStore {
            pending_part,
            history_index_table,
            commit_id_table,
            history_number_table,
            change_history_table,
        };

        Ok(versioned_store)
    }

    pub fn add_to_pending_part(
        &mut self,
        parent_commit: Option<CommitID>,
        commit: CommitID,
        updates: HashMap<T::Key, Option<T::Value>>,
    ) -> Result<()> {
        if self.commit_id_table.get(&commit)?.is_some() {
            return Err(StorageError::CommitIdAlreadyExistsInHistory);
        }

        let mut pending_part = self.pending_part.lock();
        Ok(pending_part.add_node(updates, commit, parent_commit)?)
    }

    fn get_history_number_by_commit_id(&self, commit: CommitID) -> Result<HistoryNumber> {
        if let Some(value) = self.commit_id_table.get(&commit)? {
            Ok(value.into_owned())
        } else {
            Err(StorageError::CommitIDNotFound)
        }
    }

    fn get_historical_part(
        &self,
        query_version_number: HistoryNumber,
        key: &T::Key,
        is_latest: bool,
    ) -> Result<Option<T::Value>> {
        if is_latest {
            get_versioned_key_latest(query_version_number, key, &self.history_index_table)
        } else {
            get_versioned_key_previous(
                query_version_number,
                key,
                &self.history_index_table,
                &self.change_history_table,
            )
        }
    }
}

/// Returns the value for the key at the maximum version_number such that
/// version_number <= the latest_version_number in the historical part.
/// If there is no such version_number, returns `None`.
// # Algorithm Explanation
// - If the lastest record exists, since the latest record has at least one
//   version_number, and all recorded version_numbers are <= latest_version_number,
//   we can always obtain the value from the lastest record.
// - If the latest record does not exists, then there is no record for this key.
// Therefore, there is no need to view previous records.
fn get_versioned_key_latest<T: VersionedKeyValueSchema>(
    latest_version_number: HistoryNumber,
    key: &T::Key,
    history_index_table: &TableReader<'_, HistoryIndicesTable<T>>,
) -> Result<Option<T::Value>> {
    let range_query_key = HistoryIndexKey(key.clone(), LATEST);
    match history_index_table.get(&range_query_key)? {
        Some(history_indices) => history_indices
            .into_owned()
            .get_latest_value(latest_version_number),
        None => Ok(None),
    }
}

/// Returns the value for the key at the maximum version_number such that
/// version_number <= the query_version_number in the historical part.
/// If there is no such version_number, returns `None`.
// # Algorithm Explanation
// Find the record with the minimum version specifier such that the version specifier >= query_version_number.
// Since LATEST >= query_version_number, if no such record, there is no record for this key.
// If there is such record,
// - If there is a previous_record before this record, then the previous_version_specifier < query_version_number,
//   and previous_version_specifier is the start_version_number of this record.
//   I.e., the start_version_number of this record < query_version_number <= the version specifier of this record.
//   Thus, we can always obtain the value from this record.
// - If there is no previous_record before this record, version_number can only be within this record or non-existing.
// Therefore, there is no need to view previous records before this record.
fn get_versioned_key_previous<'db, T: VersionedKeyValueSchema>(
    query_version_number: HistoryNumber,
    key: &T::Key,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: &KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
) -> Result<Option<T::Value>> {
    let range_query_key = HistoryIndexKey(key.clone(), query_version_number);

    match history_index_table.iter(&range_query_key)?.next() {
        None => Ok(None),
        Some(Err(e)) => Err(e.into()),
        Some(Ok((k, _))) if &k.as_ref().0 != key => Ok(None),
        Some(Ok((k, indices))) => {
            let HistoryIndexKey(_, end_history_number) = k.as_ref();
            if let Some(found_version_number) = indices
                .as_ref()
                .last_le(query_version_number, *end_history_number)?
            {
                change_history_table.get_versioned_key(&found_version_number, key)
            } else {
                Ok(None)
            }
        }
    }
}

fn iter_history_prefix<'db, T>(
    query_version_number: HistoryNumber,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    maybe_change_history_table: Option<&KeyValueStoreBulks<'db, HistoryChangeTable<T>>>,
    key_prefix: T::Key,
) -> Result<BTreeMap<T::Key, ValueEntry<T::Value>>>
where
    T: VersionedKeyValueSchema,
    T::Key: AsRef<[u8]>,
{
    let range_query_key = HistoryIndexKey(key_prefix.clone(), 0);
    let (history_index_key, _) = match history_index_table.iter(&range_query_key)?.next() {
        Some(item) => item.unwrap(),
        None => return Ok(BTreeMap::new()),
    };
    let HistoryIndexKey(mut key, _) = history_index_key.as_ref().clone();

    let mut history_map = BTreeMap::new();

    loop {
        // check key's prefix
        if !key.as_ref().starts_with(key_prefix.as_ref()) {
            break;
        }

        let value = if let Some(change_history_table) = maybe_change_history_table {
            get_versioned_key_previous(
                query_version_number,
                &key,
                history_index_table,
                change_history_table,
            )?
        } else {
            get_versioned_key_latest(query_version_number, &key, history_index_table)?
        };

        history_map.insert(key.clone(), ValueEntry::from_option(value));

        let range_query_key = HistoryIndexKey(key.clone(), LATEST);
        let mut find_next_key_iter = history_index_table.iter(&range_query_key)?;

        let this_key = match find_next_key_iter.next().transpose()? {
            Some((this_historical_index_key, _)) => this_historical_index_key.as_ref().0.clone(),
            None => break,
        };
        assert_eq!(this_key, key, "The latest record of a key should exist when there is at least one record of that key.");

        let next_key = match find_next_key_iter.next().transpose()? {
            Some((next_historical_index_key, _)) => next_historical_index_key.0.clone(),
            None => break,
        };
        assert_ne!(next_key, key, "Iterator should have moved to a different key after processing the lastest record for the current key.");

        key = next_key;
    }

    Ok(history_map)
}

fn iter_history<'db, T: VersionedKeyValueSchema>(
    query_version_number: HistoryNumber,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    maybe_change_history_table: Option<&KeyValueStoreBulks<'db, HistoryChangeTable<T>>>,
) -> Result<BTreeMap<T::Key, ValueEntry<T::Value>>> {
    let (history_index_key, _) = match history_index_table.iter_from_start()?.next() {
        Some(item) => item.unwrap(),
        None => return Ok(BTreeMap::new()),
    };

    let HistoryIndexKey(mut key, _) = history_index_key.as_ref().clone();

    let mut history_map = BTreeMap::new();

    loop {
        let value = if let Some(change_history_table) = maybe_change_history_table {
            get_versioned_key_previous(
                query_version_number,
                &key,
                history_index_table,
                change_history_table,
            )?
        } else {
            get_versioned_key_latest(query_version_number, &key, history_index_table)?
        };

        history_map.insert(key.clone(), ValueEntry::from_option(value));

        let range_query_key = HistoryIndexKey(key.clone(), LATEST);
        let mut find_next_key_iter = history_index_table.iter(&range_query_key)?;

        let this_key = match find_next_key_iter.next().transpose()? {
            Some((this_historical_index_key, _)) => this_historical_index_key.as_ref().0.clone(),
            None => break,
        };
        assert_eq!(this_key, key, "The latest record of a key should exist when there is at least one record of that key.");

        let next_key = match find_next_key_iter.next().transpose()? {
            Some((next_historical_index_key, _)) => next_historical_index_key.0.clone(),
            None => break,
        };
        assert_ne!(next_key, key, "Iterator should have moved to a different key after processing the lastest record for the current key.");

        key = next_key;
    }

    Ok(history_map)
}

pub fn confirmed_pending_to_history<D: DatabaseTrait, T: VersionedKeyValueSchema>(
    db: Arc<D>,
    pending_part: Arc<Mutex<VersionedMap<PendingKeyValueConfig<T, CommitID>>>>,
    new_root_commit_id: CommitID,
    write_schema: &D::WriteSchema,
) -> Result<()> {
    let mut pending_part_guard = pending_part.lock();
    let confirmed_path = pending_part_guard.change_root(new_root_commit_id)?;

    confirm_ids_to_history::<D>(
        db.clone(),
        confirmed_path.start_height,
        &confirmed_path.commit_ids,
        write_schema,
    )?;

    confirm_maps_to_history::<D, T>(
        db.clone(),
        confirmed_path.start_height,
        confirmed_path.key_value_maps,
        write_schema,
    )?;

    Ok(())
}

pub fn confirm_maps_to_history<D: DatabaseTrait, T: VersionedKeyValueSchema>(
    db: Arc<D>,
    to_confirm_start_height: usize,
    to_confirm_maps: Vec<HashMap<T::Key, impl Into<Option<T::Value>>>>,
    write_schema: &D::WriteSchema,
) -> Result<()> {
    let history_index_table = db.view::<HistoryIndicesTable<T>>()?;
    let change_history_table =
        KeyValueStoreBulks::new(Arc::new(db.view::<HistoryChangeTable<T>>()?));

    let mut history_index_cache = HistoryIndexCache::new();
    for (delta_height, updates) in to_confirm_maps.into_iter().enumerate() {
        let height = to_confirm_start_height + delta_height;
        let history_number = height_to_history_number(height);

        let commit_data = updates
            .into_iter()
            .map(|(k, v)| {
                let value = v.into();
                history_index_cache.insert(
                    k.clone(),
                    value.clone(),
                    history_number,
                    &history_index_table,
                )?;
                Ok((k, value))
            })
            .collect::<Result<Vec<_>>>()?;

        change_history_table.commit(history_number, commit_data.into_iter(), &write_schema)?;
    }

    let history_indices_table_op = history_index_cache.into_write_batch();
    write_schema.write_batch::<HistoryIndicesTable<T>>(history_indices_table_op.into_iter());

    Ok(())
}

pub fn confirm_ids_to_history<D: DatabaseTrait>(
    db: Arc<D>,
    to_confirm_start_height: usize,
    to_confirm_ids: &[CommitID],
    write_schema: &D::WriteSchema,
) -> Result<()> {
    let commit_id_table = db.view::<CommitIDSchema>()?;
    let history_number_table = db.view::<HistoryNumberSchema>()?;

    for (delta_height, confirmed_commit_id) in to_confirm_ids.iter().enumerate() {
        let height = to_confirm_start_height + delta_height;
        let history_number = height_to_history_number(height);

        if commit_id_table.get(confirmed_commit_id)?.is_some()
            || history_number_table.get(&history_number)?.is_some()
        {
            return Err(StorageError::ConsistencyCheckFailure);
        }

        let commit_id_table_op = (
            Cow::Owned(*confirmed_commit_id),
            Some(Cow::Owned(history_number)),
        );
        write_schema.write::<CommitIDSchema>(commit_id_table_op);

        let history_number_table_op = (
            Cow::Owned(history_number),
            Some(Cow::Owned(*confirmed_commit_id)),
        );
        write_schema.write::<HistoryNumberSchema>(history_number_table_op);
    }

    Ok(())
}
